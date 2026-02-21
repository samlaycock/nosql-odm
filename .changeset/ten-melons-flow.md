---
"nosql-odm": patch
---

Protect lazy migration writebacks from clobbering concurrent updates by propagating optimistic write tokens through store read paths and using conditional `batchSetWithResult` writes (including conflict skip hooks).
