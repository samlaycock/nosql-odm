---
"nosql-odm": minor
---

Add configurable unique-constraint lock behavior in `createStore()` via
`uniqueConstraintLock` options (`ttlMs`, `maxAttempts`, `retryDelayMs`, and
`heartbeatIntervalMs`) so high-latency workloads can tune lock acquisition and
long-running writes can renew locks safely.
