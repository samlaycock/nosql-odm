---
"nosql-odm": patch
---

Fix optional field index resolution to skip missing values instead of storing the literal strings `"undefined"` or `"null"`.

Field-based indexes now omit entries when the indexed field is `undefined` or `null`, which prevents false query matches and avoids unique index conflicts for documents where optional indexed fields are not present.

Includes regression tests covering:

- querying an optional index with `"undefined"`
- optional unique indexes allowing multiple documents with missing values
