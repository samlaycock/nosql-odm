---
"nosql-odm": patch
---

Downgrade the MongoDB engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Document the required `allowStoreManagedUniqueConstraints` opt-in for MongoDB unique indexes, add a unit regression check for the capability flag, and add MongoDB integration coverage for store-managed duplicate create, update, and `batchSet()` rejections.
