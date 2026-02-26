---
"nosql-odm": patch
---

Prevent `store.update()` from silently overwriting concurrent writes when the engine exposes optimistic write tokens.

`update()` now reads via `getWithMetadata()` when available and performs a conditional single-document write using `batchSetWithResult()`. When the conditional write conflicts, the store throws a new `ConcurrentWriteError` instead of silently clobbering the newer document state.
