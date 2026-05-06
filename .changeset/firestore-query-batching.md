---
"nosql-odm": patch
---

Push expressible Firestore queries down into server-side ordering and cursor
pagination, and batch `batchGet` reads through `getAll` while preserving local
fallbacks for query shapes Firestore cannot represent safely.
