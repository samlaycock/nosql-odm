---
"nosql-odm": patch
---

Queue IndexedDB batch and index-query document reads before awaiting results to reduce per-key read latency while preserving duplicate keys and request order.
