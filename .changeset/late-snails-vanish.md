---
"nosql-odm": patch
---

Gate the publish workflow behind quality checks, unit tests, and selected in-process integration smoke tests (memory, indexeddb, sqlite).

The release job now depends on these prerequisite jobs via `needs`, and the workflow includes inline comments documenting the required checks.
