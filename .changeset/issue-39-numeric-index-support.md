---
"nosql-odm": minor
---

Add `encodeNumericIndexValue()` as a first-class helper for building numeric
index values and numeric query bounds that preserve numeric ordering under the
library's lexicographic string comparisons. Includes tests and README guidance
for usage and reindexing existing data.
