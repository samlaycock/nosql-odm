# nosql-odm

## 0.11.0

### Minor Changes

- 5bb44fc: Require MongoDB query fallback collection scans to be explicitly enabled with
  `allowFallbackCollectionScans`, while keeping `rejectUnsupportedQueries` as an
  optional stricter guard when scan fallback is enabled.

### Patch Changes

- dab0ceb: Strengthen shared query cursor conformance coverage for opaque payloads,
  query-bound reuse checks, and deleted-row resume behavior across adapters.
- 3aba1ff: Fix MongoDB query pagination to use opaque, query-bound cursors across native and fallback execution paths.
- 9390427: Implement MongoDB `probeUnique()` so store-managed unique prechecks can batch
  index ownership lookups instead of issuing one query per value.

## 0.10.0

### Minor Changes

- 5100fd3: Add an optional `probeUnique()` engine hook so store-managed unique guards can batch unique prechecks by index instead of issuing one query per value. The store now falls back to the existing per-value query behavior when an engine does not implement the hook, and the memory engine exposes the new batched probe contract.
- 31208f8: Add stable `code` values to the exported model, store, migrator, and engine error classes, and document the public error-code reference table in the README.
- c21cc5c: Reject duplicate document keys inside a single store `batchSet()` call before any engine write occurs, and surface a dedicated error that reports the conflicting keys and their batch positions.
- 36ceaa0: Add `store.<model>.batchGetOrdered(keys)` to preserve requested key order, retain duplicate positions, and include `null` placeholders for missing or skipped documents without changing the existing `batchGet()` contract.
- 638de2c: Add composite `where` query support for static indexes that declare `fields` metadata. Multi-field `where` clauses now resolve deterministically onto matching composite indexes, preserve exact-equality semantics, and return clearer errors when no matching metadata exists or when the query shape cannot be resolved safely.
- 4d36baf: Downgrade the Cassandra engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Cassandra consumers that declare `unique: true` indexes must now opt into store-managed guards with `createStore(..., { allowStoreManagedUniqueConstraints: true })` until adapter-side enforcement exists. Update the README capability matrix, add a unit regression for the capability flag, and add Cassandra integration coverage showing that store-managed duplicate create, update, and `batchSet()` writes are still rejected.

### Patch Changes

- 93634cf: Add adapter-managed unique index ownership to the Firestore engine so `create()`, `put()`, `update()`, `delete()`, `batchSet()`, `batchSetWithResult()`, and `batchDelete()` keep unique-index claims in sync and reject duplicate values with `EngineUniqueConstraintError`. The engine now persists document-level `uniqueIndexes` metadata, preserves existing ownership when `uniqueIndexes` is omitted on subsequent writes, and enables the shared Firestore integration conformance coverage for atomic unique-constraint enforcement.
- 38744fc: Add public `parseSemverVersion` and `compareSemverVersions` helpers for semver-style schema version fields so migrations and outdated-document detection no longer require custom parser/comparator boilerplate. The helpers validate semver input, honor prerelease precedence, ignore build metadata for ordering, and bridge semver strings to the existing numeric schema chain by major version.
- 4d1fc76: Add a shared query-engine conformance suite across adapters, publish per-engine conformance status from the full integration matrix, and fix sqlite/indexeddb engine-level unique constraint enforcement to satisfy the shared parity checks. For SQLite upgrades from schema v2, backfill any obvious singleton unique-index owners from historical index entries, noting that this reconstruction is heuristic because v2 did not persist unique/non-unique index metadata separately.
- 8ab8014: Add configurable chunking for SQL-engine `batchGet()` and `batchSet()` operations so large MySQL, Postgres, and SQLite batches avoid oversized requests and reduce memory spikes while preserving transactional write behavior. The SQL integration suite now exercises the chunked paths directly with explicit chunk-size overrides.
- 78ef67b: Expand migration telemetry so paged migration results, durable progress snapshots, and migration hook payloads expose per-page duration, records-per-second throughput, writeback failure counts, and rolling skip-reason histograms. The example app and README now show how to log and chart the new telemetry fields for migration observability.
- e86fee0: Downgrade the MongoDB engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Document the required `allowStoreManagedUniqueConstraints` opt-in for MongoDB unique indexes, add a unit regression check for the capability flag, and add MongoDB integration coverage for store-managed duplicate create, update, and `batchSet()` rejections.
- 5e28ad2: Downgrade the DynamoDB engine's advertised unique-constraint capability from `atomic` to `none` so stores no longer trust unsupported adapter-side uniqueness guarantees. Document the capability matrix change, add a unit regression for the capability flag, and add DynamoDB integration coverage showing that `allowStoreManagedUniqueConstraints` still rejects duplicate create, update, and `batchSet()` writes.
- 094b129: Add adapter-managed unique index ownership to the Redis engine so `create()`, `put()`, `update()`, `batchSet()`, and `batchSetWithResult()` reject duplicate values for `unique: true` indexes with `EngineUniqueConstraintError`. Redis writes now persist unique-index ownership alongside document state, release ownership on delete, preserve existing ownership when `uniqueIndexes` are omitted, and enable the shared Redis integration conformance coverage for unique-constraint enforcement.
- 29d6993: Add adapter-managed unique index ownership to the MySQL engine so `create()`, `put()`, `update()`, `batchSet()`, and `batchSetWithResult()` reject duplicate values for `unique: true` indexes with `EngineUniqueConstraintError`. The engine now provisions a dedicated unique-index ownership table during schema bootstrap, preserves custom internal table naming for that table, and enables the shared MySQL integration conformance coverage for unique-constraint enforcement.
- 1995a85: Add adapter-managed unique index ownership to the Postgres engine so `create()`, `put()`, `update()`, `batchSet()`, and `batchSetWithResult()` reject duplicate values for `unique: true` indexes with `EngineUniqueConstraintError`. The engine now provisions a dedicated unique-index ownership table during schema bootstrap, preserves custom internal table naming for that table, and enables the shared Postgres integration conformance coverage for unique-constraint enforcement.
- 2a2e4ef: Parallelize lazy read-path projection and validation for `query()` and `batchGet()` with a shared bounded-concurrency helper. This preserves input ordering and lazy writeback behavior while reducing latency for larger result sets and keeping the migrator on the same shared concurrency utility.
- ad250fc: Reject model builds that define schema fields or static index field references using reserved migration metadata fields such as `__v` and `__indexes`, including custom `versionField` and `indexesField` overrides. This prevents store writes from silently overwriting user data and adds regression coverage for both default and custom metadata field names.
- 0ee8ce3: Validate `batchSet()` payloads concurrently before invoking the engine while preserving duplicate-key fast-fail behavior, input-ordered results, and deterministic validation error selection. This reduces avoidable latency for large batches and async schema validators, and adds regression coverage for concurrent validation.
- 3c48ff0: Reject stale query cursors after model version or index metadata changes by salting cursor signatures with model query-shape metadata.

  Version query cursor payloads explicitly so unsupported legacy cursor formats fail fast, and add regression coverage for model/version drift and index-metadata drift across paginated queries.

## 0.9.0

### Minor Changes

- 071191f: Add an explicit `allowStoreManagedUniqueConstraints` `createStore()` option to allow models with unique indexes when an engine declares `capabilities.uniqueConstraints = "none"`.

  When enabled, the store accepts those models and relies on the existing store-managed lock + uniqueness pre-check guard path instead of requiring engine-level atomic unique constraints.

### Patch Changes

- 2927e09: Reduce MongoDB full-collection fallback scans by pushing down additional index-filter combinations, including mixed `$between` and range predicates.

  Add `rejectUnsupportedQueries` to `mongoDbEngine(...)` so unsupported indexed filters can fail fast instead of silently scanning collections.

  Add an `onQueryFallbackScan` hook for observing when MongoDB query methods use in-memory scan fallback.

- b7b8182: Use MongoDB `bulkWrite` for `batchSet` and `batchDelete`, and for unconditional writes in `batchSetWithResult`, reducing per-item round trips on large batches.

  Reserve `createdAt` sequence values in a single metadata increment per batch to preserve monotonic ordering without per-item sequence fetches.

  Add MongoDB batch regression tests covering large-batch `bulkWrite` paths and conditional write conflict behavior.

- 2891701: Prevent `store.update()` from silently overwriting concurrent writes when the engine exposes optimistic write tokens.

  `update()` now reads via `getWithMetadata()` when available and performs a conditional single-document write using `batchSetWithResult()`. When the conditional write conflicts, the store throws a new `ConcurrentWriteError` instead of silently clobbering the newer document state.

- 480be86: Optimize store-managed unique precheck conflict detection by querying with `limit: 1` instead of `limit: 10`, since these probes only need to detect existence.

  Add a regression unit test that verifies unique precheck queries use existence-only limits during `batchSet()`.

- 1e94ccc: Reduce repeated deep traversal on write paths by letting engines opt into store-level document preparation.

  Reuse a shared JSON-compatible document preparation helper across store writes and the memory, IndexedDB, DynamoDB, Firestore, MongoDB, SQLite, MySQL, Postgres, Redis, and Cassandra adapters so clone/serialization work only happens once per write.

  Add regression coverage that verifies engine-provided write preparation is used for `create()`, `update()`, and `batchSet()` while preserving the existing JSON-compatibility errors.

- e7ffe31: Optimize SQLite `batchGet()` and `batchGetWithMetadata()` by fetching requested keys through batched `IN (...)` queries instead of issuing one lookup per key.

  The SQLite adapter now preserves request order and duplicates while reducing database round-trips for multi-key reads.

- edb5d89: Skip store-managed unique-constraint lock/precheck guards by default when an engine reports atomic unique enforcement.

  `createStore(..., { allowStoreManagedUniqueConstraints: true })` now also acts as an explicit compatibility/debug override to re-enable the store-managed guard path on atomic-capable engines.

- 7a0fb3d: Fix SQL engine query pagination cursors (MySQL, Postgres, SQLite) to use opaque, query-bound cursors that remain stable when the previous page's last row is deleted between requests.

  This aligns SQL pagination behavior with the shared cursor helpers used by other adapters and adds regression coverage for deleted-cursor-row continuation.

- 22fb55e: Prevent `store.query()` from silently falling back to a full collection scan when `index` is provided without `filter`.

  The store now validates `index` and `filter` as a required pair before resolving query params, and throws a clear error for malformed queries instead of passing them through to engine scan paths.

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
