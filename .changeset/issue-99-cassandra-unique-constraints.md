---
"nosql-odm": minor
---

Downgrade the Cassandra engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Cassandra consumers that declare `unique: true` indexes must now opt into store-managed guards with `createStore(..., { allowStoreManagedUniqueConstraints: true })` until adapter-side enforcement exists. Update the README capability matrix, add a unit regression for the capability flag, and add Cassandra integration coverage showing that store-managed duplicate create, update, and `batchSet()` writes are still rejected.
