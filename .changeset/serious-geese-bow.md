---
"nosql-odm": patch
---

Extend atomic unique-index enforcement coverage across all engine integrations by declaring explicit unique-constraint capabilities on the remaining adapters and adding shared integration tests for duplicate create/update/batchSet behavior.

Add a store-level unique guard that uses engine-backed locks and index checks to enforce unique index values consistently across create, update, batchSet, lazy writeback, and migration persistence paths.
