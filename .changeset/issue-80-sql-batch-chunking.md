---
"nosql-odm": patch
---

Add configurable chunking for SQL-engine `batchGet()` and `batchSet()` operations so large MySQL, Postgres, and SQLite batches avoid oversized requests and reduce memory spikes while preserving transactional write behavior. The SQL integration suite now exercises the chunked paths directly with explicit chunk-size overrides.
