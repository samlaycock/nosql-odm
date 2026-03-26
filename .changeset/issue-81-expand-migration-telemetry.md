---
"nosql-odm": patch
---

Expand migration telemetry so paged migration results, durable progress snapshots, and migration hook payloads expose per-page duration, records-per-second throughput, writeback failure counts, and rolling skip-reason histograms. The example app and README now show how to log and chart the new telemetry fields for migration observability.
