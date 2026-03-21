---
"nosql-odm": minor
---

Add composite `where` query support for static indexes that declare `fields` metadata. Multi-field `where` clauses now resolve deterministically onto matching composite indexes, preserve exact-equality semantics, and return clearer errors when no matching metadata exists or when the query shape cannot be resolved safely.
