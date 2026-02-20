---
"nosql-odm": patch
---

Detect resolved index-name collisions at runtime when building index key maps, and throw deterministic errors that include the model name and conflicting index identifiers.

Add unit tests for static+dynamic and dynamic+dynamic resolved name collision scenarios.
