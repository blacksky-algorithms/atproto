---
'@atproto/bsky': patch
---

Restore upstream's filenames and contents for the `add-mute-scope` and `add-op-thread-reply` migrations, and re-key them in the migration index so they still sort after migrations already applied. Migrations execute in sorted-key order, and the export key is independent of the filename, so the ordering can be corrected without diverging from upstream on the migration files themselves. Adds a test asserting the migration index stays internally consistent: every file exported exactly once, keys unique and sorted.
