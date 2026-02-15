# NoSQL ODM

A lightweight, schema-first ODM for NoSQL-style data stores.

## Features

- Versioned schemas with forward migrations
- Index-based queries, `where` queries, and cursor pagination
- Lazy migration on reads
- Ignore-first migration behavior for non-migratable documents
- Dynamic index names (multi-tenant style index partitions)
- Explicit bulk operations (`batchGet`, `batchSet`, `batchDelete`)
- Pluggable query engine interface (memory, SQLite, IndexedDB, DynamoDB, Cassandra, Redis, MongoDB, and Firestore adapters included)

## Package Intent

`nosql-odm` is built around one core split:

- `Model`/`Store` layer: schema validation, type safety, and user-facing API ergonomics.
- `QueryEngine` layer: low-level storage behavior (atomicity, race handling, batching, locking, and performance details).

In other words, the ODM acts as a type-safe proxy over the engine contract. Adapter authors implement correctness/performance details at the engine boundary.

## Installation

```bash
npm install nosql-odm zod
```

For SQLite support:

```bash
npm install better-sqlite3
```

For DynamoDB support:

```bash
npm install @aws-sdk/lib-dynamodb
```

For Cassandra support:

```bash
npm install cassandra-driver
```

For Redis support:

```bash
npm install redis
```

For MongoDB support:

```bash
npm install mongodb
```

For Firestore support:

```bash
npm install @google-cloud/firestore
```

## Quick Start

The examples below use `zod`, which implements the Standard Schema interface used by this package.

```ts
import { z } from "zod";
import { model, createStore } from "nosql-odm";
import { memoryEngine } from "nosql-odm/engines/memory";

const User = model("user")
  .schema(
    1,
    z.object({
      id: z.string(),
      name: z.string(),
      email: z.email(),
    }),
  )
  .schema(
    2,
    z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      email: z.email(),
      role: z.enum(["admin", "member", "guest"]),
    }),
    {
      migrate(old) {
        const [firstName, ...rest] = old.name.split(" ");

        return {
          id: old.id,
          firstName: firstName ?? "",
          lastName: rest.join(" ") || "",
          email: old.email,
          role: "member" as const,
        };
      },
    },
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .index({ name: "byRole", value: (user) => `${user.role}#${user.lastName}` })
  .build();

const store = createStore(memoryEngine(), [User]);
```

## Typical Usage Flow

Most projects follow this sequence:

1. Define model schema versions and indexes.
2. Create a store with one engine and one or more models.
3. Use model APIs (`create`, `findByKey`, `query`, `update`, `delete`, `batch*`) in app code.
4. Let lazy migration handle stale docs on reads, or run `migrateAll()` for explicit upgrades.

## SQLite Engine (`better-sqlite3`)

```ts
import Database from "better-sqlite3";
import { sqliteEngine } from "nosql-odm/engines/sqlite";

const db = new Database("./data/app.sqlite");
const engine = sqliteEngine({ database: db });

const store = createStore(engine, [User]);
```

The SQLite engine requires an explicit database instance (`{ database }`), runs required internal table migrations on startup, and uses normalized index tables for efficient filtered queries, sorting, and cursor pagination.

Note: this adapter is typed against the `better-sqlite3` `Database` API. If you pass another compatible SQLite database object (for example via a cast), that path is currently untyped/untested.

## Postgres Engine (`pg`)

```ts
import { Pool } from "pg";
import { postgresEngine } from "nosql-odm/engines/postgres";

const pool = new Pool({
  host: "127.0.0.1",
  port: 5432,
  user: "postgres",
  password: "postgres",
  database: "app",
});

const engine = postgresEngine({
  client: pool,
  schema: "public",
});

const store = createStore(engine, [User]);
```

The Postgres adapter creates internal tables on first use:

- `<schema>.nosql_odm_documents`
- `<schema>.nosql_odm_index_entries`
- `<schema>.nosql_odm_migration_locks`
- `<schema>.nosql_odm_migration_checkpoints`

You can override table names with:
`documentsTable`, `indexesTable`, `migrationLocksTable`, and `migrationCheckpointsTable`.

## IndexedDB Engine (Browser / Worker)

```ts
import { indexedDbEngine } from "nosql-odm/engines/indexeddb";

const engine = indexedDbEngine({ databaseName: "my-app-db" });
const store = createStore(engine, [User]);
```

In non-browser environments (for example tests), pass a factory explicitly:

```ts
import { indexedDB as fakeIndexedDB } from "fake-indexeddb";
import { indexedDbEngine } from "nosql-odm/engines/indexeddb";

const engine = indexedDbEngine({
  databaseName: "test-db",
  factory: fakeIndexedDB as any,
});
```

## DynamoDB Engine (AWS SDK DocumentClient)

```ts
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { dynamoDbEngine } from "nosql-odm/engines/dynamodb";

const baseClient = new DynamoDBClient({ region: "us-east-1" });
const client = DynamoDBDocumentClient.from(baseClient);

const engine = dynamoDbEngine({
  client,
  tableName: "nosql_odm",
  // Optional overrides (defaults shown):
  // hashKeyName: "pk",
  // sortKeyName: "sk",
  // migrationOutdatedIndexName: "migration_outdated_idx",
  // migrationSyncIndexName: "migration_sync_idx",
});

const store = createStore(engine, [User]);
```

The DynamoDB adapter expects a table with:

- Partition key `pk` (string) by default, or your configured `hashKeyName`
- Sort key `sk` (string) by default, or your configured `sortKeyName`
- Global secondary index `migration_outdated_idx`
  - Partition key `migrationOutdatedPk` (string)
  - Sort key `migrationOutdatedSk` (string)
- Global secondary index `migration_sync_idx`
  - Partition key `migrationSyncPk` (string)
  - Sort key `migrationSyncSk` (number)

If you configure custom metadata index names, use those names instead of the defaults above.

It stores both model documents and migration metadata in that table using prefixed keys.

`migration_outdated_idx` is used to page only migration-eligible documents (version-behind and/or index-signature mismatch).
`migration_sync_idx` is used to find metadata rows that are stale for the current migration target version.

## Cassandra Engine (`cassandra-driver`)

```ts
import { Client } from "cassandra-driver";
import { cassandraEngine } from "nosql-odm/engines/cassandra";

const client = new Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  protocolOptions: { port: 9042 },
});

const engine = cassandraEngine({
  client,
  keyspace: "app",
});

const store = createStore(engine, [User]);
```

The Cassandra adapter creates its internal tables on first use:

- `<keyspace>.nosql_odm_documents`
- `<keyspace>.nosql_odm_metadata`

You can override table names with `documentsTable` / `metadataTable`.

## Redis Engine (`redis`)

```ts
import { createClient } from "redis";
import { redisEngine } from "nosql-odm/engines/redis";

const client = createClient({
  url: "redis://127.0.0.1:6379",
});

await client.connect();

const engine = redisEngine({
  client,
  keyPrefix: "nosql_odm",
});

const store = createStore(engine, [User]);
```

The Redis adapter stores document/index state in per-document hashes and keeps
collection ordering in Redis sorted sets. Migration lock/checkpoint state is
stored under dedicated metadata keys in the same prefix namespace.

## MongoDB Engine (`mongodb`)

```ts
import { MongoClient } from "mongodb";
import { mongoDbEngine } from "nosql-odm/engines/mongodb";

const client = new MongoClient("mongodb://127.0.0.1:27017");
await client.connect();

const engine = mongoDbEngine({
  database: client.db("app"),
});

const store = createStore(engine, [User]);
```

The MongoDB adapter stores model documents in one collection and migration run state in another:

- `nosql_odm_documents`
- `nosql_odm_metadata`

Document migration metadata used for outdated-page queries is co-located with each document row in
`nosql_odm_documents` (for atomic document + metadata updates).
`nosql_odm_metadata` stores migration lock/checkpoint/sequence state.

You can override collection names with `documentsCollection` / `metadataCollection`.

## MySQL Engine (`mysql2`)

```ts
import { createPool } from "mysql2/promise";
import { mySqlEngine } from "nosql-odm/engines/mysql";

const pool = createPool({
  host: "127.0.0.1",
  port: 3306,
  user: "root",
  password: "root",
  database: "app",
});

const engine = mySqlEngine({
  client: pool,
  database: "app",
});

const store = createStore(engine, [User]);
```

The MySQL adapter creates internal tables on first use:

- `<database>.nosql_odm_documents`
- `<database>.nosql_odm_index_entries`
- `<database>.nosql_odm_migration_locks`
- `<database>.nosql_odm_migration_checkpoints`

You can override table names with:
`documentsTable`, `indexesTable`, `migrationLocksTable`, and `migrationCheckpointsTable`.

## Firestore Engine (`@google-cloud/firestore`)

```ts
import { Firestore } from "@google-cloud/firestore";
import { firestoreEngine } from "nosql-odm/engines/firestore";

process.env.FIRESTORE_EMULATOR_HOST = "127.0.0.1:8080"; // for local emulator usage

const database = new Firestore({
  projectId: "app-local",
});

const engine = firestoreEngine({
  database,
});

const store = createStore(engine, [User]);
```

The Firestore adapter stores model documents and migration metadata in two collections:

- `nosql_odm_documents`
- `nosql_odm_metadata`

You can override collection names with `documentsCollection` / `metadataCollection`.

## Local Engine Services (Docker Compose)

For engine test suites with runtime dependencies, this repo uses named Docker Compose services.

Start DynamoDB Local:

```bash
bun run services:up:dynamodb
```

Stop it:

```bash
bun run services:down:dynamodb
```

The DynamoDB Local endpoint is `http://localhost:8000`.
The DynamoDB engine integration suite uses this service directly (no fake client):

```bash
bun run test:integration:dynamodb
```

Start Cassandra:

```bash
bun run services:up:cassandra
```

Stop it:

```bash
bun run services:down:cassandra
```

The Cassandra engine integration suite uses this service directly:

```bash
bun run test:integration:cassandra
```

Start Redis:

```bash
bun run services:up:redis
```

Stop it:

```bash
bun run services:down:redis
```

The Redis engine integration suite uses this service directly:

```bash
bun run test:integration:redis
```

Start MongoDB:

```bash
bun run services:up:mongodb
```

Stop it:

```bash
bun run services:down:mongodb
```

The MongoDB engine integration suite uses this service directly:

```bash
bun run test:integration:mongodb
```

Start Firestore emulator:

```bash
bun run services:up:firestore
```

Stop it:

```bash
bun run services:down:firestore
```

The Firestore engine integration suite uses this service directly:

```bash
bun run test:integration:firestore
```

Start MySQL:

````bash
bun run services:up:mysql
Start Postgres:

```bash
bun run services:up:postgres
````

Stop it:

```bash
bun run services:down:mysql
```

The MySQL engine integration suite uses this service directly:

```bash
bun run test:integration:mysql
bun run services:down:postgres
```

The Postgres engine integration suite uses this service directly:

```bash
bun run test:integration:postgres
```

Test runner modes:

```bash
bun run test                                 # unit tests only (./tests/unit/*.test.ts)
bun run test:integration:dynamodb            # only DynamoDB integration
bun run test:integration:cassandra           # only Cassandra integration
bun run test:integration:redis               # only Redis integration
bun run test:integration:mongodb             # only MongoDB integration
bun run test:integration:firestore           # only Firestore integration
bun run test:integration:mysql               # only MySQL integration
bun run test:integration:postgres            # only Postgres integration
```

The DynamoDB integration suite creates and deletes its own test table automatically.

## CRUD

`create` requires an explicit key:

```ts
await store.user.create("u1", {
  id: "u1",
  firstName: "Sam",
  lastName: "Laycock",
  email: "sam@example.com",
  role: "member",
});

const user = await store.user.findByKey("u1");

await store.user.update("u1", { role: "admin" });
await store.user.delete("u1");
```

`batchSet` also requires explicit keys:

```ts
await store.user.batchSet([
  {
    key: "u2",
    data: {
      id: "u2",
      firstName: "Jane",
      lastName: "Doe",
      email: "jane@example.com",
      role: "member",
    },
  },
  {
    key: "u3",
    data: {
      id: "u3",
      firstName: "Alex",
      lastName: "Smith",
      email: "alex@example.com",
      role: "guest",
    },
  },
]);

await store.user.batchDelete(["u2", "u3"]);
```

## Querying

The query API is explicit by design: choose an index, provide a filter, then paginate with cursors.

```ts
const page1 = await store.user.query({
  index: "byRole",
  filter: { value: { $begins: "member#" } },
  sort: "asc",
  limit: 20,
});

const page2 = await store.user.query({
  index: "byRole",
  filter: { value: { $begins: "member#" } },
  cursor: page1.cursor ?? undefined,
});
```

`query()` returns:

- `documents`: typed model documents
- `cursor`: `string | null` for keyset pagination

Field-based `where` shorthand is also supported (requires an index on that field):

```ts
const exact = await store.user.query({
  where: { email: "sam@example.com" },
});

const range = await store.user.query({
  where: { id: { $gte: "u1000" } },
  limit: 50,
});
```

You can scan all documents by omitting both `index`/`filter` and `where`:

```ts
const allUsers = await store.user.query({});
```

`where` rules:

- Must contain exactly one field.
- Cannot be combined with `index`/`filter`.
- Works with indexes whose `value` is a field name string (for function-based indexes, use `index` + `filter`).

Supported filter operators:

- Equality: direct value or `$eq`
- Comparison: `$gt`, `$gte`, `$lt`, `$lte`
- Prefix: `$begins`
- Range: `$between: [low, high]`

## Indexing Guide

Indexes are precomputed string keys. Good index design is the difference between simple queries and painful query logic.

### Static and composite indexes

```ts
const User = model("user")
  .schema(
    1,
    z.object({
      id: z.string(),
      email: z.email(),
      role: z.enum(["admin", "member", "guest"]),
      lastName: z.string(),
      createdAt: z.string(), // ISO-8601 timestamp
    }),
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .index({ name: "byCreatedAt", value: "createdAt" })
  .index({ name: "byRoleLastName", value: (u) => `${u.role}#${u.lastName.toLowerCase()}` })
  .build();
```

Composite indexes are ideal when your common queries include both partitioning and ordering:

```ts
const members = await store.user.query({
  index: "byRoleLastName",
  filter: { value: { $begins: "member#" } },
  sort: "asc",
  limit: 25,
});
```

### Lexicographic ordering rules

All index values are strings. Sort/range comparisons are lexicographic.

If you need numeric ordering, encode numeric values so lexicographic order matches numeric order:

```ts
const Product = model("product")
  .schema(
    1,
    z.object({
      id: z.string(),
      priceCents: z.number().int(),
    }),
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byPrice", value: (p) => String(p.priceCents).padStart(12, "0") })
  .build();
```

For dates/times, prefer sortable ISO-8601 strings (`2026-02-13T19:00:00.000Z`).

### Dynamic index names

Use the overload `index(key, definition)` when the index name itself is data-driven (for example tenant partitions):

```ts
const Resource = model("resource")
  .schema(
    1,
    z.object({
      id: z.string(),
      tenant: z.string(),
      userId: z.string(),
    }),
  )
  .index({ name: "primary", value: "id" })
  .index("tenantUser_v1", {
    name: (r) => `${r.tenant}#user`,
    value: (r) => `${r.tenant}#${r.userId}`,
  })
  .build();
```

Query dynamic indexes by resolved name:

```ts
await store.resource.query({
  index: "acme#user",
  filter: { value: { $begins: "acme#" } },
});
```

Notes:

- Dynamic-name indexes are queried with `index` + `filter` (not `where`).
- Use a stable naming convention (`<partition>#<kind>`) so callers can derive names safely.

### Index evolution across schema versions

Indexes are attached to the latest schema shape. When adding a schema version, re-declare indexes:

```ts
const User = model("user")
  .schema(1, z.object({ id: z.string(), email: z.email() }))
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .schema(
    2,
    z.object({
      id: z.string(),
      email: z.email(),
      role: z.enum(["admin", "member", "guest"]),
    }),
    {
      migrate(old) {
        return { ...old, role: "member" as const };
      },
    },
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .index({ name: "byRole", value: "role" })
  .build();
```

## Migrations

Migrations are designed around resumable, lock-coordinated runs, so they work in both:

- horizontally scaled app fleets where many nodes may attempt to start migration at boot
- durable workflows/job runners that process migration in ordered pages and recover after failures

### 1. Version chain and projection rules

Define schemas as an ordered chain. Each step migrates from the immediately previous version:

```ts
const User = model("user")
  .schema(
    1,
    z.object({
      id: z.string(),
      name: z.string(),
      email: z.email(),
    }),
  )
  .schema(
    2,
    z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      email: z.email(),
    }),
    {
      migrate(old) {
        const [firstName, ...rest] = old.name.split(" ");
        return {
          id: old.id,
          firstName: firstName ?? "",
          lastName: rest.join(" ") || "",
          email: old.email,
        };
      },
    },
  )
  .schema(
    3,
    z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      email: z.email(),
      role: z.enum(["admin", "member", "guest"]),
    }),
    {
      migrate(old) {
        return { ...old, role: "member" as const };
      },
    },
  )
  .index({ name: "primary", value: "id" })
  .index({ name: "byEmail", value: "email" })
  .build();
```

Migration is stepwise (`v1 -> v2 -> v3`), never a single jump. A document is considered outdated when:

- its stored version is behind the latest model version, or
- its stored index-name list does not match the current model index names

That means `migrateAll()` also functions as a reindex backfill.

### 2. Read-path migration modes

Default mode is `lazy`:

```ts
const User = model("user", { migration: "lazy" }) // default
  .schema(/* ... */)
  .build();
```

Modes:

- `lazy`: read APIs (`findByKey`, `query`, `batchGet`) project stale docs to latest and persist the write-back.
- `readonly`: read APIs project in-memory but do not persist migration write-backs.
- `eager`: same read behavior as `readonly`; use explicit migration jobs (`migrateAll` / paged APIs) for persistence.

### 3. Migration scopes and conflict rules

There are two run scopes:

- model scope: `store.user.*migration*()` operates only on `user`
- store scope: `store.*migration*()` operates on all registered models in deterministic order

Scope conflict protection is built in:

- if a store-scope run is active, starting a model-scope run for any covered model throws `MigrationScopeConflictError`
- if a model-scope run is active for a model, starting a store-scope run that includes that model throws `MigrationScopeConflictError`

This prevents overlapping runs that would duplicate work or fight over checkpoints.

### 4. Migration APIs

Model-level APIs:

- `await store.user.getOrCreateMigration(options?)`
- `await store.user.migrateNextPage(options?)`
- `await store.user.getMigrationProgress()`
- `await store.user.migrateAll(options?)`
- `await store.user.getMigrationStatus()`

Store-level APIs:

- `await store.getOrCreateMigration(options?)`
- `await store.migrateNextPage(options?)`
- `await store.getMigrationProgress()`
- `await store.migrateAll(options?)`

Options:

- `lockTtlMs?: number`

`lockTtlMs` enables stale lock recovery. Engines should treat a lock as stale when `Date.now() - lock.acquiredAt >= lockTtlMs`, then allow takeover.

### 5. `getOrCreateMigration()` semantics

`getOrCreateMigration()` is safe to call from many workers:

- if no run exists, it creates one and returns progress
- if a run already exists, it returns existing progress (resume semantics)
- if another worker holds the lock during this call, it still attempts to return existing progress

Use this to initialize orchestration state without needing a separate "create if missing" primitive.

### 6. `migrateNextPage()` semantics (durable workflow primitive)

`migrateNextPage()` processes at most one page and returns:

- `status: "busy"`: another worker currently holds the migration lock
- `status: "processed"`: one page was processed and more work remains
- `status: "completed"`: the run is finished and cleared

Returned shape:

- `model`: model processed in this step (`null` for some completed/no-op paths)
- `migrated`: number of documents successfully moved/reindexed in this page
- `skipped`: number of documents skipped in this page
- `skipReasons`: optional per-reason counts for skipped docs
- `completed`: whether the run finished in this call
- `hasMore`: whether more work remains
- `progress`: current run progress snapshot, or `null` when completed

Important workflow properties:

- page boundaries are checkpointed, so retries resume from saved cursor boundaries
- crashes mid-page can re-scan that page on retry (already-migrated docs are naturally filtered as up-to-date)
- lock is released in `finally`, including error paths
- when an engine supports conditional batch writes, docs changed concurrently after page fetch are skipped with `concurrent_write` instead of being overwritten

### 7. Full-run helpers: `migrateAll()`

Model scope:

```ts
const result = await store.user.migrateAll({ lockTtlMs: 30_000 });
console.log(result.model, result.status, result.migrated, result.skipped, result.skipReasons);
```

Store scope:

```ts
const results = await store.migrateAll({ lockTtlMs: 30_000 });
for (const r of results) {
  console.log(r.model, r.status, r.migrated, r.skipped, r.skipReasons);
}
```

`migrateAll()` is a loop over `migrateNextPage()`. If it receives `status: "busy"` mid-run, it throws `MigrationAlreadyRunningError`.

### 8. Progress and status introspection

`getMigrationProgress()` returns durable run progress (if a run exists):

```ts
const progress = await store.getMigrationProgress();
```

Progress includes:

- `id`: run ID
- `scope`: `"model" | "store"`
- `models`: included model names
- `modelIndex`: current model position for store-scope runs
- `cursor`: current page cursor for current model
- `startedAt`, `updatedAt`
- `running`
- `totals`: aggregate migrated/skipped
- `progressByModel[model]`: `{ migrated, skipped, pages, skipReasons }`

`getMigrationStatus()` (model API only) returns raw engine migration status for that collection lock/checkpoint:

```ts
const status = await store.user.getMigrationStatus();
```

Use `getMigrationProgress()` for user-facing run progress. Use `getMigrationStatus()` for low-level lock/checkpoint inspection.

### 9. Skip behavior and reasons

Migration is intentionally ignore-first per document:

- bad/incompatible docs do not fail the whole run
- skipped documents are counted and surfaced in `skipReasons`

Current reason keys:

- `invalid_version`
- `ahead_of_latest`
- `unknown_source_version`
- `version_compare_error`
- `migration_error`
- `validation_error`
- `concurrent_write`

Read-path handling for skipped docs:

- `findByKey()` returns `null`
- `query()` and `batchGet()` omit them

### 10. Event hooks

Pass hooks via `createStore(..., { migrationHooks })`:

```ts
const store = createStore(engine, [User], {
  migrationHooks: {
    onMigrationCreated: ({ progress }) => {
      console.log("created", progress.id);
    },
    onDocumentMigrated: ({ runId, model, key }) => {
      console.log("migrated", runId, model, key);
    },
    onDocumentSkipped: ({ runId, model, key, reason, error }) => {
      console.warn("skipped", runId, model, key, reason, error);
    },
    onMigrationCompleted: ({ progress }) => {
      console.log("completed", progress.id, progress.totals);
    },
    onMigrationFailed: ({ runId, error, progress }) => {
      console.error("failed", runId, error, progress);
    },
  },
});
```

Available hooks:

- `onMigrationCreated`
- `onMigrationResumed`
- `onPageClaimed`
- `onDocumentMigrated`
- `onDocumentSkipped`
- `onPageCommitted`
- `onMigrationCompleted`
- `onMigrationFailed`

Hook errors are intentionally swallowed so observability code cannot break migration execution.

### 11. Migrator selection and overriding

Migrator resolution order:

1. `createStore(..., { migrator })` (highest priority)
2. `engine.migrator`
3. none -> migration APIs throw `MissingMigratorError`

If you pass `migrationHooks` and do not pass a `migrator`, the store uses a `DefaultMigrator` with those hooks.
If you pass a custom `migrator`, do not also pass `migrationHooks` — `createStore` throws when both are provided.

### 12. Implementing a custom migrator

Implement the `Migrator` interface:

```ts
import type {
  Migrator,
  MigrationRunOptions,
  MigrationNextPageResult,
  MigrationRunProgress,
} from "nosql-odm";

type MigrationScope = Parameters<Migrator["getOrCreateRun"]>[0];
type MigrationContexts = Parameters<Migrator["getOrCreateRun"]>[1];
type MigrationGetOrCreateResult = Awaited<ReturnType<Migrator["getOrCreateRun"]>>;

class MyMigrator implements Migrator {
  async getOrCreateRun(
    scope: MigrationScope,
    contexts: MigrationContexts,
    options?: MigrationRunOptions,
  ): Promise<MigrationGetOrCreateResult> {
    // create-or-resume run metadata in your own durable store
    // enforce scope overlap rules
    // return current progress
    throw new Error("not implemented");
  }

  async migrateNextPage(
    scope: MigrationScope,
    contexts: MigrationContexts,
    options?: MigrationRunOptions,
  ): Promise<MigrationNextPageResult> {
    // claim lock/work unit, read one page, call context.project(),
    // persist with context.persist(), update checkpoints, return page result
    throw new Error("not implemented");
  }

  async getProgress(scope: MigrationScope): Promise<MigrationRunProgress | null> {
    // return progress snapshot or null if no run exists
    throw new Error("not implemented");
  }
}
```

Practical recommendations for custom migrators:

- keep run metadata durable (run id, scope, model order, model index, cursor, per-model totals)
- use short critical sections around lock acquisition and checkpoint writes
- keep page processing idempotent (safe to replay after crash)
- checkpoint only after page commit
- treat hook/telemetry side effects as non-critical
- normalize model order deterministically for store-scope runs

### 13. Engine requirements for migration support

Every engine must provide:

- `migration.acquireLock(collection, { ttl? })`
- `migration.releaseLock(lock)`
- `migration.getOutdated(collection, criteria, cursor?)`

For `DefaultMigrator` support, engines must also provide:

- `migration.saveCheckpoint(lock, cursor)`
- `migration.loadCheckpoint(collection)`
- `migration.clearCheckpoint(collection)`

Optional but recommended:

- `migration.getStatus(collection)` for lock/checkpoint inspection

`getOutdated()` must return docs that are version-behind and/or missing expected indexes, plus a cursor for pagination.
Engines may run internal metadata maintenance before page selection (for example, syncing per-document migration metadata to the current target criteria), but returned page documents should still be migration-eligible only.

For robust concurrent migration safety, engines should also support:

- returning a per-document optimistic write token in `getOutdated()` results (`KeyedDocument.writeToken`)
- honoring `expectedWriteToken` in `batchSetWithResult` so stale migration writes are reported as conflicts, not persisted

### 14. Recommended production patterns

Boot-time horizontally scaled workers:

```ts
await store.getOrCreateMigration({ lockTtlMs: 30_000 });
while (true) {
  const page = await store.migrateNextPage({ lockTtlMs: 30_000 });
  if (page.status === "busy") break;
  if (page.completed) break;
}
```

Durable workflow/orchestrator tick:

```ts
await store.getOrCreateMigration();
const page = await store.migrateNextPage();

if (page.status === "busy") {
  // re-enqueue with backoff
} else if (!page.completed) {
  // enqueue next chunk
} else {
  // done
}
```

This gives strict page-by-page advancement, resumability, and safe multi-worker coordination.

## Custom Metadata Fields

```ts
const User = model("user", {
  versionField: "_v",
  indexesField: "_idx",
})
  .schema(/* version 1 */)
  .schema(/* version 2 */)
  .build();
```

You can also customize version parsing/comparison (version values may be `string` or `number`). This directly affects outdated detection and migration comparisons:

```ts
const User = model("user", {
  parseVersion(raw) {
    if (typeof raw === "string" || typeof raw === "number") return raw;
    return null;
  },
  compareVersions(a, b) {
    const toNum = (v: string | number) =>
      typeof v === "number" ? v : Number(String(v).replace(/^release-/i, ""));
    return toNum(a) - toNum(b);
  },
});
```

Example for release tags:

```ts
const User = model("user", {
  versionField: "release",
  parseVersion(raw) {
    return typeof raw === "string" ? raw : null;
  },
  compareVersions(a, b) {
    const toNum = (v: string | number) =>
      typeof v === "number" ? v : Number(String(v).replace(/^release-/i, ""));
    return toNum(a) - toNum(b);
  },
});
```

## Engine Contract (Adapter Authors)

Responsibility boundary:

- The ODM (`Model`/`Store`) handles:
  - Schema validation and migration projection
  - Query ergonomics (`where` shorthand, index-name resolution)
  - Error translation for user-facing API errors
- The `QueryEngine` handles:
  - Atomic write behavior (`create`/`update` existence semantics)
  - Concurrency/race correctness
  - Batch operation behavior/performance
  - Locking/checkpoint persistence for migrations

Required methods:

- `get`, `create`, `put`, `update`, `delete`, `query`
- Migration: `acquireLock`, `releaseLock`, `getOutdated`

Required batch methods:

- `batchGet`, `batchSet`, `batchDelete`

Required for default `Migrator` support:

- Migration checkpoints: `saveCheckpoint`, `loadCheckpoint`, `clearCheckpoint`
- Migration status: `getStatus`

Engine migrator:

- Expose `engine.migrator` (built-ins do this automatically).
- If an engine does not expose a migrator and none is passed via `createStore(..., { migrator })`,
  migration methods throw `MissingMigratorError`.

Behavioral requirements:

- `create` must be atomic and throw `EngineDocumentAlreadyExistsError` when the key already exists.
- `update` must throw `EngineDocumentNotFoundError` when the key does not exist.
- `batchGet`, `batchSet`, and `batchDelete` are required (the ODM does not fall back to per-item `get`/`put`/`delete`).
- `getOutdated` criteria can receive custom `parseVersion` / `compareVersions` callbacks.

See `dist/index.d.ts` and `dist/engines/memory.d.ts` for full signatures.

## Notes

- `create` and `batchSet` require explicit string keys (the `key` parameter is the storage key, distinct from any `id` field in your data).
- `create` throws if the key already exists (`DocumentAlreadyExistsError`), based on the adapter's atomic `engine.create` behavior.
- `batchGet`, `batchSet`, and `batchDelete` are required adapter methods.
- Query comparisons are lexicographic (index values are stored as strings).
- Adding a new schema version resets builder indexes; re-add indexes for the latest shape.
- `create`, `batchSet`, and `update` validate against the latest schema.
- The `unique` index flag is metadata only in this package; uniqueness enforcement is adapter-specific.

## License

MIT
