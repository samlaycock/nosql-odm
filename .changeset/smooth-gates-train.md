---
"nosql-odm": patch
---

Optimize SQLite `batchGet()` and `batchGetWithMetadata()` by fetching requested keys through batched `IN (...)` queries instead of issuing one lookup per key.

The SQLite adapter now preserves request order and duplicates while reducing database round-trips for multi-key reads.
