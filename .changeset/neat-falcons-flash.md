---
"nosql-odm": patch
---

Expand adapter integration workflow `pull_request.paths` filters to include shared integration helper files.

This ensures CI integration workflows run when `tests/integration/helpers.ts` or `tests/integration/migration-suite.ts` changes.
