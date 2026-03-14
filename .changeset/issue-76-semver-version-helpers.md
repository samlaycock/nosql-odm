---
"nosql-odm": patch
---

Add public `parseSemverVersion` and `compareSemverVersions` helpers for semver-style schema version fields so migrations and outdated-document detection no longer require custom parser/comparator boilerplate. The helpers validate semver input, honor prerelease precedence, ignore build metadata for ordering, and bridge semver strings to the existing numeric schema chain by major version.
