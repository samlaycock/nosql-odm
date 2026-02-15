---
"nosql-odm": minor
---

Improve migration throughput and consistency across all engines by adding adaptive paging hints, reducing redundant metadata sync work, and tightening migrator execution behavior.

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
