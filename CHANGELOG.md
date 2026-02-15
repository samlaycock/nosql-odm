# nosql-odm

## 0.6.0

### Minor Changes

- 990e9e2: Overhaul migration orchestration around a new `Migrator` abstraction with run upsert, page-by-page execution, scope-aware conflict checks, and durable progress tracking.

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

## 0.5.0

### Minor Changes

- 1d256bf: Add a new Postgres engine adapter using `pg`.
  - Introduce `postgresEngine` with full `QueryEngine` support for CRUD, query filters/sorting/pagination, batch operations, and migration lock/checkpoint flows.
  - Add package export `nosql-odm/engines/postgres`.
  - Add Postgres integration coverage and local Docker service/test scripts.
  - Document Postgres engine setup and integration testing workflow in the README.

- 755a3de: Add a new MySQL engine via `nosql-odm/engines/mysql` backed by `mysql2`.

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
