---
"nosql-odm": minor
---

Add an optional `probeUnique()` engine hook so store-managed unique guards can batch unique prechecks by index instead of issuing one query per value. The store now falls back to the existing per-value query behavior when an engine does not implement the hook, and the memory engine exposes the new batched probe contract.
