---
"nosql-odm": patch
---

Add projection-skip observability hooks to `createStore` so ignored projection failures in read/query paths can be observed with model, key, reason, and operation context.

Emit projection skip events for `findByKey`, `query`, `batchGet`, and update fallback paths when migration error mode is `ignore`, while preserving strict-mode throwing behavior.
