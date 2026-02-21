---
"nosql-odm": patch
---

Parallelize unique-index pre-check queries during unique-guarded writes and add bounded concurrency via `createStore(..., { uniqueConstraintPrecheck: { concurrency } })`.

This reduces serialized pre-check round trips for large `batchSet` operations while preserving uniqueness correctness and allowing callers to cap backend load.
