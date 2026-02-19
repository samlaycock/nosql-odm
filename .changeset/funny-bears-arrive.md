---
"nosql-odm": patch
---

Add an `oxfmt` import-sorting configuration and apply consistent import ordering across source and test files.

Also refresh formatter/linter and related dependency versions in `package.json` and `bun.lock` so formatting behavior is deterministic in CI and local development.
