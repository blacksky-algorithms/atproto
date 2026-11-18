---
'@atproto/bsky': patch
---

Store `post.langs` and `post.tags` as `varchar[]`. The migration converts existing `jsonb` columns in place, which rewrites the `post` table, and skips columns that are already arrays.
