---
"nosql-odm": patch
---

Add and export `DocumentNotFoundError` for `store.update()` missing-document paths, including concurrent delete handling when the engine reports not found.
