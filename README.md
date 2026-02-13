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
});

const store = createStore(engine, [User]);
```

The DynamoDB adapter expects a table with:

- Partition key `pk` (string)
- Sort key `sk` (string)

It stores both model documents and migration metadata in that table using prefixed keys.

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

The MongoDB adapter stores model documents and migration metadata in two collections:

- `nosql_odm_documents`
- `nosql_odm_metadata`

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
bun run test:integration                     # all integration suites
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

### Defining version chains

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
  .build();
```

Migrations run stepwise (`v1 -> v2 -> v3`), not as one jump.

### Migration modes

Default mode is `lazy`:

```ts
const User = model("user", { migration: "lazy" }) // default
  .schema(/* ... */)
  .build();
```

Modes:

- `lazy`: read paths (`findByKey`, `query`, `batchGet`) migrate stale docs and write back.
- `readonly`: read paths migrate in memory but do not write back.
- `eager`: same read behavior as `readonly`; use `migrateAll()` for persisted upgrades.

### Running migrations explicitly

One model:

```ts
const result = await store.user.migrateAll();

if (result.status === "completed") {
  console.log(result.migrated, result.skipped, result.skipReasons);
}
```

All models in a store:

```ts
const results = await store.migrateAll();

for (const result of results) {
  if (result.status === "completed") {
    console.log(result.model, result.migrated, result.skipped);
  } else {
    // typically already running for a locked model
    console.log(result.model, result.status, result.reason);
  }
}
```

Optional lock TTL in ms for stale-lock recovery:

```ts
await store.user.migrateAll({ lockTtlMs: 30_000 });
await store.migrateAll({ lockTtlMs: 30_000 });
```

`migrateAll()` returns a summary including:

- `migrated`: documents migrated to newer schema versions
- `skipped`: documents ignored because they could not be safely projected to the latest schema
- `skipReasons`: per-reason skip counts (for documents the engine returned to the migrator)

Typical `skipReasons` keys:

- `unknown_source_version`
- `migration_error`
- `validation_error`
- `invalid_version`
- `ahead_of_latest`
- `version_compare_error`

### Ignore-first policy

- Documents with versions ahead of the latest schema are ignored.
- Documents with invalid/unrecognized versions are ignored.
- Migration function errors and post-migration validation failures are ignored per-document.
- `findByKey` returns `null` for ignored documents.
- `query` and `batchGet` omit ignored documents from results.

### Migration observability

Inspect migration lock/checkpoint state per model:

```ts
const status = await store.user.getMigrationStatus();

if (status) {
  console.log(status.lock.id, status.lock.acquiredAt, status.cursor);
}
```

### Recommended migration rollout patterns

1. `lazy` first, `migrateAll` later:
   Deploy code with new schemas in `lazy` mode, let hot data self-heal on reads, then run `migrateAll()` to backfill.
2. `readonly` for safety checks:
   Use `readonly` if you want migrated reads without persistence while validating new schema behavior.
3. `eager` for controlled maintenance windows:
   Use `eager` + scheduled `migrateAll()` when writes during read paths should stay minimal/predictable.

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

Optional methods:

- Migration checkpoints: `saveCheckpoint`, `loadCheckpoint`, `clearCheckpoint`
- Migration status: `getStatus`

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
