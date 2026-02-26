---
"nosql-odm": minor
---

Adopt opaque, query-bound query cursors for in-memory paginated adapters (memory, IndexedDB, DynamoDB, Firestore, Redis, and Cassandra) so continuation stays stable across duplicate sort values and cursor-row deletions.

Query cursors are now opaque tokens rather than raw document keys, and invalid or mismatched cursors are rejected explicitly instead of silently restarting from the beginning.
