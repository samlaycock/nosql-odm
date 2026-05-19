---
"nosql-odm": patch
---

Encode numeric field-backed index values automatically so sorting and range filters preserve numeric ordering for values such as negatives, decimals, `2`, and `10`.
