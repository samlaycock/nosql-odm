---
"nosql-odm": patch
---

Add adapter-managed unique index ownership to the MySQL engine so `create()`, `put()`, `update()`, `batchSet()`, and `batchSetWithResult()` reject duplicate values for `unique: true` indexes with `EngineUniqueConstraintError`. The engine now provisions a dedicated unique-index ownership table during schema bootstrap, preserves custom internal table naming for that table, and enables the shared MySQL integration conformance coverage for unique-constraint enforcement.
