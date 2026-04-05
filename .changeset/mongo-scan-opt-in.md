---
"nosql-odm": minor
---

Require MongoDB query fallback collection scans to be explicitly enabled with
`allowFallbackCollectionScans`, while keeping `rejectUnsupportedQueries` as an
optional stricter guard when scan fallback is enabled.
