---
"nosql-odm": patch
---

Add adapter-managed unique index ownership to the Redis engine so `create()`, `put()`, `update()`, `batchSet()`, and `batchSetWithResult()` reject duplicate values for `unique: true` indexes with `EngineUniqueConstraintError`. Redis writes now persist unique-index ownership alongside document state, release ownership on delete, preserve existing ownership when `uniqueIndexes` are omitted, and enable the shared Redis integration conformance coverage for unique-constraint enforcement.
