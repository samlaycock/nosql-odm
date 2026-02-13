# NoSQL ODM

A lightweight, schema-first ODM for NoSQL-style data stores.

## Features

- Versioned schemas with forward migrations
- Index-based queries, `where` queries, and cursor pagination
- Lazy migration on reads
- Ignore-first migration behavior for non-migratable documents
- Dynamic index names (multi-tenant style index partitions)
- Explicit bulk operations (`batchGet`, `batchSet`, `batchDelete`)
- Pluggable query engine interface (memory, SQLite, IndexedDB, DynamoDB, and Cassandra adapters included)

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

Test runner modes:

```bash
bun run test                                 # unit tests only (./tests/unit/*.test.ts)
bun run test:integration                     # all integration suites
bun run test:integration:dynamodb            # only DynamoDB integration
bun run test:integration:cassandra           # only Cassandra integration
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

Field-based `where` shorthand is also supported (requires an index on that field):

```ts
const results = await store.user.query({
  where: { email: "sam@example.com" },
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

## Dynamic Index Names

Use the overload `index(key, definition)` when the index name is computed per document:

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

Query dynamic indexes using the resolved name:

```ts
await store.resource.query({
  index: "acme#user",
  filter: { value: { $begins: "acme#" } },
});
```

## Migrations

Default migration mode is `lazy` (migrate stale docs on read and write them back).

```ts
const User = model("user", { migration: "lazy" }) // default
  .schema(/* ... */)
  .build();
```

Run explicit full migrations:

```ts
await store.user.migrateAll(); // one model
await store.migrateAll(); // every model in the store

// Optional: lock TTL in ms for stale-lock recovery
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

Migration modes:

- `lazy`: read paths (`findByKey`, `query`, `batchGet`) migrate stale docs and write back.
- `readonly`: read paths migrate in memory but do not write back.
- `eager`: same read behavior as `readonly`; use `migrateAll()` for persisted upgrades.

Ignore-first policy:

- Documents with versions ahead of the latest schema are ignored.
- Documents with invalid/unrecognized versions are ignored.
- Migration function errors and post-migration validation failures are ignored per-document.
- `findByKey` returns `null` for ignored documents.
- `query` and `batchGet` omit ignored documents from results.

You can inspect migration lock/checkpoint state per model:

```ts
const status = await store.user.getMigrationStatus();
```

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

You can also customize version parsing/comparison (version values may be `string` or `number`):

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
