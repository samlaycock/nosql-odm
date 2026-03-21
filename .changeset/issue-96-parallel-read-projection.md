---
"nosql-odm": patch
---

Parallelize lazy read-path projection and validation for `query()` and `batchGet()` with a shared bounded-concurrency helper. This preserves input ordering and lazy writeback behavior while reducing latency for larger result sets and keeping the migrator on the same shared concurrency utility.
