# nosql-odm

## 0.7.0

### Minor Changes

- 43fd641: Improve migration throughput and consistency across all engines by adding adaptive paging hints, reducing redundant metadata sync work, and tightening migrator execution behavior.

  ### Added

  - Optional migration criteria hints for engines:
    - `pageSizeHint`
    - `skipMetadataSyncHint`
  - Adaptive per-model migration page sizing persisted in run state.
  - Bounded parallel projection in the default migrator for page processing.
  - Optional batched migration hook:
    - `onDocumentsPersisted({ persistedKeys, conflictedKeys, ... })`

  ### Changed

  - Engines now honor dynamic page-size hints for outdated-document paging.
  - Engines with metadata sync paths can skip redundant sync on continuation pages.
  - Store-scope conflict checks in the default migrator now resolve model scope activity concurrently.
  - Migration run-state parsing now validates persisted page-size hints.

  ### Tooling / Docs

  - Removed the aggregate `test:integration` npm script because it is not reliable in constrained shared-runner environments.
  - README test runner examples now point to per-engine integration scripts only.

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
