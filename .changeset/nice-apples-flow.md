---
"nosql-odm": patch
---

Optimize store-managed unique precheck conflict detection by querying with `limit: 1` instead of `limit: 10`, since these probes only need to detect existence.

Add a regression unit test that verifies unique precheck queries use existence-only limits during `batchSet()`.
