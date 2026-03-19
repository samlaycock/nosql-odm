---
"nosql-odm": patch
---

Downgrade the Cassandra engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Update the README capability matrix, add a unit regression for the capability flag, and add Cassandra integration coverage showing that `allowStoreManagedUniqueConstraints` still rejects duplicate create, update, and `batchSet()` writes.
