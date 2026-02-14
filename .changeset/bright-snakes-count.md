---
"nosql-odm": minor
---

Overhaul migration orchestration around a new `Migrator` abstraction with run upsert, page-by-page execution, scope-aware conflict checks, and durable progress tracking.

### Added

- `DefaultMigrator` and `Migrator` interfaces for custom migration workflows.
- Model and store migration APIs for paged workflows:
  - `getOrCreateMigration()`
  - `migrateNextPage()`
  - `getMigrationProgress()`
- `MissingMigratorError` and `MigrationScopeConflictError`.
- Built-in engines now expose `engine.migrator` by default.

### Changed

- `migrateAll()` now executes through migrator runs/pages and enforces scope conflicts between model-level and store-level migrations.
- Store-wide migrations fail fast when scope is already covered by another active migration run.
