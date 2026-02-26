---
"nosql-odm": patch
---

Prevent `store.query()` from silently falling back to a full collection scan when `index` is provided without `filter`.

The store now validates `index` and `filter` as a required pair before resolving query params, and throws a clear error for malformed queries instead of passing them through to engine scan paths.
