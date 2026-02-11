# NoSQL ODM

A lightweight, schema-first ODM for NoSQL-style data stores.

## Features

- Versioned schemas with forward migrations
- Index-based queries, `where` queries, and cursor pagination
- Lazy migration on reads
- Ignore-first migration behavior for non-migratable documents
- Dynamic index names (multi-tenant style index partitions)
- Explicit bulk operations (`batchGet`, `batchSet`, `batchDelete`)
- Pluggable query engine interface (memory engine adapter included)

## Installation

```bash
npm install nosql-odm zod
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
      email: z.string().email(),
    }),
  )
  .schema(
    2,
    z.object({
      id: z.string(),
      firstName: z.string(),
      lastName: z.string(),
      email: z.string().email(),
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

Required methods:

- `get`, `put`, `delete`, `query`
- Migration: `acquireLock`, `releaseLock`, `getOutdated`

Optional methods:

- `batchGet`, `batchSet`, `batchDelete`
- Migration checkpoints: `saveCheckpoint`, `loadCheckpoint`, `clearCheckpoint`
- Migration status: `getStatus`
- `getOutdated` criteria can receive custom `parseVersion` / `compareVersions` callbacks.

See `dist/index.d.ts` and `dist/engines/memory.d.ts` for full signatures.

## Notes

- `create` and `batchSet` require explicit string keys (the `key` parameter is the storage key, distinct from any `id` field in your data).
- `create` throws if the key already exists (`DocumentAlreadyExistsError`).
- `batchGet` requires engine support for `batchGet`; `batchSet`/`batchDelete` fall back to per-item writes/deletes if adapter methods are not implemented.
- Query comparisons are lexicographic (index values are stored as strings).
- Adding a new schema version resets builder indexes; re-add indexes for the latest shape.
- `create`, `batchSet`, and `update` validate against the latest schema.
- The `unique` index flag is metadata only in this package; uniqueness enforcement is adapter-specific.

## License

MIT
