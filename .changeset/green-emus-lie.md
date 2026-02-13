---
"nosql-odm": minor
---

Add a new MySQL engine via `nosql-odm/engines/mysql` backed by `mysql2`.

This release introduces first-class MySQL support with:
- Full `QueryEngine` contract coverage for create/get/update/delete, `batchGet`, and paginated queries.
- Migration primitives (locks, checkpoints, and outdated migration discovery).
- Support for custom internal table names used for metadata and migration state.

Also includes:
- Integration test coverage for MySQL parity with the other engines.
- Docker Compose service wiring and test scripts for local MySQL integration runs.
- Updated package exports and documentation for MySQL setup/usage.

If you use the MySQL engine, install `mysql2` in your app:

```bash
bun add mysql2
```
