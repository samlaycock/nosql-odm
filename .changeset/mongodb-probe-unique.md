---
"nosql-odm": patch
---

Implement MongoDB `probeUnique()` so store-managed unique prechecks can batch
index ownership lookups instead of issuing one query per value.
