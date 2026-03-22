---
"nosql-odm": patch
---

Validate `batchSet()` payloads concurrently before invoking the engine while preserving duplicate-key fast-fail behavior, input-ordered results, and deterministic validation error selection. This reduces avoidable latency for large batches and async schema validators, and adds regression coverage for concurrent validation.
