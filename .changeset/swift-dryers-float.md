---
"nosql-odm": patch
---

Fix SQL engine query pagination cursors (MySQL, Postgres, SQLite) to use opaque, query-bound cursors that remain stable when the previous page's last row is deleted between requests.

This aligns SQL pagination behavior with the shared cursor helpers used by other adapters and adds regression coverage for deleted-cursor-row continuation.
