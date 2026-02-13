---
"nosql-odm": minor
---

Add a new Postgres engine adapter using `pg`.

- Introduce `postgresEngine` with full `QueryEngine` support for CRUD, query filters/sorting/pagination, batch operations, and migration lock/checkpoint flows.
- Add package export `nosql-odm/engines/postgres`.
- Add Postgres integration coverage and local Docker service/test scripts.
- Document Postgres engine setup and integration testing workflow in the README.
