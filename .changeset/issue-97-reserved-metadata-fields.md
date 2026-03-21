---
"nosql-odm": patch
---

Reject model builds that define schema fields or static index field references using reserved migration metadata fields such as `__v` and `__indexes`, including custom `versionField` and `indexesField` overrides. This prevents store writes from silently overwriting user data and adds regression coverage for both default and custom metadata field names.
