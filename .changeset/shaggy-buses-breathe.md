---
"nosql-odm": patch
---

Reduce repeated deep traversal on write paths by letting engines opt into store-level document preparation.

Reuse a shared JSON-compatible document preparation helper across store writes and the memory, IndexedDB, DynamoDB, Firestore, MongoDB, SQLite, MySQL, Postgres, Redis, and Cassandra adapters so clone/serialization work only happens once per write.

Add regression coverage that verifies engine-provided write preparation is used for `create()`, `update()`, and `batchSet()` while preserving the existing JSON-compatibility errors.
