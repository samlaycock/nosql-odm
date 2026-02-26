---
"nosql-odm": minor
---

Add an explicit `allowStoreManagedUniqueConstraints` `createStore()` option to allow models with unique indexes when an engine declares `capabilities.uniqueConstraints = "none"`.

When enabled, the store accepts those models and relies on the existing store-managed lock + uniqueness pre-check guard path instead of requiring engine-level atomic unique constraints.
