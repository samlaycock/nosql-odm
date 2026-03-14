---
"nosql-odm": patch
---

Add a shared query-engine conformance suite across adapters, publish per-engine conformance status from the full integration matrix, and fix sqlite/indexeddb engine-level unique constraint enforcement to satisfy the shared parity checks. For SQLite upgrades from schema v2, backfill any obvious singleton unique-index owners from historical index entries, noting that this reconstruction is heuristic because v2 did not persist unique/non-unique index metadata separately.
