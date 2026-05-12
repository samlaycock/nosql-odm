---
"nosql-odm": patch
---

Reduce non-SQL adapter write contention by replacing per-collection createdAt sequence reservations with time-sortable local allocation while preserving deterministic cursor tie-breaking.
