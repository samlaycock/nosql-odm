---
"nosql-odm": patch
---

Replace DynamoDB index-filter partition scans with query-index lookups when an
index-backed filter can be pushed down, while preserving scan fallback when the
secondary lookup index is unavailable.
