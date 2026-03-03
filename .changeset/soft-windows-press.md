---
"nosql-odm": patch
---

Skip store-managed unique-constraint lock/precheck guards by default when an engine reports atomic unique enforcement.

`createStore(..., { allowStoreManagedUniqueConstraints: true })` now also acts as an explicit compatibility/debug override to re-enable the store-managed guard path on atomic-capable engines.
