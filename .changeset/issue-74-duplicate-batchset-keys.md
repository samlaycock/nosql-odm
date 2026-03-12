---
"nosql-odm": minor
---

Reject duplicate document keys inside a single store `batchSet()` call before any engine write occurs, and surface a dedicated error that reports the conflicting keys and their batch positions.
