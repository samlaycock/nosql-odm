---
"nosql-odm": patch
---

Reduce non-SQL adapter query materialization for indexed queries by pushing
filtering into Firestore and DynamoDB queries and adding a MongoDB native query
path that pushes filter, sort, and cursor/limit pagination when the filter
shape is supported.
