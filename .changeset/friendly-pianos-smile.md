---
"nosql-odm": patch
---

Validate model index definitions at build time to reject duplicate static index names and duplicate dynamic index storage keys with clear runtime errors.

Add unit tests covering duplicate detection and error messages for both static and dynamic index definitions.
