---
"nosql-odm": minor
---

Add `store.<model>.batchGetOrdered(keys)` to preserve requested key order, retain duplicate positions, and include `null` placeholders for missing or skipped documents without changing the existing `batchGet()` contract.
