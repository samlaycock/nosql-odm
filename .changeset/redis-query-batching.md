---
"nosql-odm": patch
---

Push Redis indexed queries down into Redis lex-sorted index sets and batch
document reads through a single script call, while preserving scan fallback for
query shapes that cannot be pushed down safely.
