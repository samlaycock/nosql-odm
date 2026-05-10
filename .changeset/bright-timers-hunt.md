---
"nosql-odm": patch
---

Add optimistic write-token support to the IndexedDB adapter so stale conditional writes are reported as conflicts instead of overwriting newer documents.
