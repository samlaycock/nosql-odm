---
"nosql-odm": patch
---

Downgrade the DynamoDB engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Document the capability matrix change, add a unit regression for the capability flag, and add DynamoDB integration coverage showing that `allowStoreManagedUniqueConstraints` still rejects duplicate create, update, and `batchSet()` writes.
