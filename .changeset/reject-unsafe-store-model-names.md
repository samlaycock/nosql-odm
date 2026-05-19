---
"nosql-odm": patch
---

Reject store model names that collide with unsafe object properties such as `__proto__`, `prototype`, and `constructor`.
