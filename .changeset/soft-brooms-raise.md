"nosql-odm": patch
---

Reject stale query cursors after model version or index metadata changes by salting cursor signatures with model query-shape metadata.

Version query cursor payloads explicitly so unsupported legacy cursor formats fail fast, and add regression coverage for model/version drift and index-metadata drift across paginated queries.
