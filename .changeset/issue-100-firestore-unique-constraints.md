---
"nosql-odm": patch
---

Add adapter-managed unique index ownership to the Firestore engine so `create()`, `put()`, `update()`, `delete()`, `batchSet()`, `batchSetWithResult()`, and `batchDelete()` keep unique-index claims in sync and reject duplicate values with `EngineUniqueConstraintError`. The engine now persists document-level `uniqueIndexes` metadata, preserves existing ownership when `uniqueIndexes` is omitted on subsequent writes, and enables the shared Firestore integration conformance coverage for atomic unique-constraint enforcement.
