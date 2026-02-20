---
"nosql-odm": patch
---

Validate model index definitions at build time to reject duplicate static index names, duplicate dynamic index storage keys, and collisions between static index names and dynamic storage keys with clear runtime errors.

Add unit tests covering duplicate detection and error messages for both static and dynamic index definitions.
