---
"nosql-odm": patch
---

Mark engine adapter peer dependencies as optional via `peerDependenciesMeta` so consumers only need to install the database drivers they actually use.

Clarify installation requirements in the README by explicitly noting that adapter peers are optional and documenting `mysql2` and `pg` install commands.
