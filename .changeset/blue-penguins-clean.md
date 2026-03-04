---
"nosql-odm": patch
---

Reduce MongoDB full-collection fallback scans by pushing down additional index-filter combinations, including mixed `$between` and range predicates.

Add `rejectUnsupportedQueries` to `mongoDbEngine(...)` so unsupported indexed filters can fail fast instead of silently scanning collections.

Add an `onQueryFallbackScan` hook for observing when MongoDB query methods use in-memory scan fallback.
