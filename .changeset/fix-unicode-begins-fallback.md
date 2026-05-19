---
"nosql-odm": patch
---

Fix Unicode correctness for `$begins` queries in Firestore, IndexedDB, and Redis by avoiding sentinel-based prefix range pushdown when the backend cannot express a safe prefix predicate.
