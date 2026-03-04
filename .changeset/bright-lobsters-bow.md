---
"nosql-odm": patch
---

Use MongoDB `bulkWrite` for `batchSet` and `batchDelete`, and for unconditional writes in `batchSetWithResult`, reducing per-item round trips on large batches.

Reserve `createdAt` sequence values in a single metadata increment per batch to preserve monotonic ordering without per-item sequence fetches.

Add MongoDB batch regression tests covering large-batch `bulkWrite` paths and conditional write conflict behavior.
