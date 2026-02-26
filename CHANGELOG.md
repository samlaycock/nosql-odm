# nosql-odm

## 0.8.0

### Minor Changes

- 8b93a49: Parallelize unique-index pre-check queries during unique-guarded writes and add bounded concurrency via `createStore(..., { uniqueConstraintPrecheck: { concurrency } })`.

  This reduces serialized pre-check round trips for large `batchSet` operations while preserving uniqueness correctness and allowing callers to cap backend load.

- da18cfd: Adopt opaque, query-bound query cursors for in-memory paginated adapters (memory, IndexedDB, DynamoDB, Firestore, Redis, and Cassandra) so continuation stays stable across duplicate sort values and cursor-row deletions.

  Query cursors are now opaque tokens rather than raw document keys, and invalid or mismatched cursors are rejected explicitly instead of silently restarting from the beginning.

- 51b77a5: Add configurable unique-constraint lock behavior in `createStore()` via
  `uniqueConstraintLock` options (`ttlMs`, `maxAttempts`, `retryDelayMs`, and
  `heartbeatIntervalMs`) so high-latency workloads can tune lock acquisition and
  long-running writes can renew locks safely.
- ac428d3: Add `encodeNumericIndexValue()` as a first-class helper for building numeric
  index values and numeric query bounds that preserve numeric ordering under the
  library's lexicographic string comparisons. Includes tests and README guidance
  for usage and reindexing existing data.

### Patch Changes

- d750904: Add a nightly GitHub Actions full integration matrix that runs every adapter test regardless of PR path filters, with manual dispatch support for on-demand validation.

  Document the CI coverage model in the README so contributors understand the fast PR path versus the scheduled full-matrix safety net.

- 94d59fe: Reduce non-SQL adapter query materialization for indexed queries by pushing
  filtering into Firestore and DynamoDB queries and adding a MongoDB native query
  path that pushes filter, sort, and cursor/limit pagination when the filter
  shape is supported.
- 16935bd: Expand adapter integration workflow `pull_request.paths` filters to include shared integration helper files.

  This ensures CI integration workflows run when `tests/integration/helpers.ts` or `tests/integration/migration-suite.ts` changes.

- f1495f8: Add and export `DocumentNotFoundError` for `store.update()` missing-document paths, including concurrent delete handling when the engine reports not found.
- e329120: Protect lazy migration writebacks from clobbering concurrent updates by propagating optimistic write tokens through store read paths and using conditional `batchSetWithResult` writes (including conflict skip hooks).
- 7e869d6: Fix optional field index resolution to skip missing values instead of storing the literal strings `"undefined"` or `"null"`.

  Field-based indexes now omit entries when the indexed field is `undefined` or `null`, which prevents false query matches and avoids unique index conflicts for documents where optional indexed fields are not present.

  Includes regression tests covering:

  - querying an optional index with `"undefined"`
  - optional unique indexes allowing multiple documents with missing values

## 0.7.1

### Patch Changes

- 29affc9: Update CI quality and test workflows to install dependencies with `bun install --frozen-lockfile` for deterministic dependency resolution.
- 14f54a1: Validate model index definitions at build time to reject duplicate static index names, duplicate dynamic index storage keys, and collisions between static index names and dynamic storage keys with clear runtime errors.

  Add unit tests covering duplicate detection and error messages for both static and dynamic index definitions.

- 3152fb8: Add an `oxfmt` import-sorting configuration and apply consistent import ordering across source and test files.

  Also refresh formatter/linter and related dependency versions in `package.json` and `bun.lock` so formatting behavior is deterministic in CI and local development.

- 44fab13: Expand README documentation for migration error behavior (`migrationErrors`), including concise tolerant-vs-strict examples, public API option defaults, and user-facing error type references.

  Document uniqueness guarantees with an engine capability matrix and runtime/error semantics for unique indexes.

  Refresh development dependencies and lockfile entries, including AWS DynamoDB SDK packages and formatting/linting toolchain updates.

- 6ca9ba0: Add projection-skip observability hooks to `createStore` so ignored projection failures in read/query paths can be observed with model, key, reason, and operation context.

  Emit projection skip events for `findByKey`, `query`, `batchGet`, and update fallback paths when migration error mode is `ignore`, while preserving strict-mode throwing behavior.

- 784f00c: Gate the publish workflow behind quality checks, unit tests, and selected in-process integration smoke tests (memory, indexeddb, sqlite).

  The release job now depends on these prerequisite jobs via `needs`, and the workflow includes inline comments documenting the required checks.

- b1109bc: Detect resolved index-name collisions at runtime when building index key maps, and throw deterministic errors that include the model name and conflicting index identifiers.

  Add unit tests for static+dynamic and dynamic+dynamic resolved name collision scenarios.

- 89f6111: Extend atomic unique-index enforcement coverage across all engine integrations by declaring explicit unique-constraint capabilities on the remaining adapters and adding shared integration tests for duplicate create/update/batchSet behavior.

  Add a store-level unique guard that uses engine-backed locks and index checks to enforce unique index values consistently across create, update, batchSet, lazy writeback, and migration persistence paths.

- ab0df1c: Mark engine adapter peer dependencies as optional via `peerDependenciesMeta` so consumers only need to install the database drivers they actually use.

  Clarify installation requirements in the README by explicitly noting that adapter peers are optional and documenting `mysql2` and `pg` install commands.

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
