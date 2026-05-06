---
"nosql-odm": minor
---

Add `queryIndexes` option to `mongoDbEngine` to provision compound MongoDB
indexes for named index fields used by native query plans. When provided, two
compound indexes are created per name — ascending and descending on
`indexes.<name>` — so that sorted, paginated, and filtered queries can use an
efficient index plan rather than relying on a collection scan.
