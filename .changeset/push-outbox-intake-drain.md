---
'@atproto/bsky': patch
---

The notification push outbox no longer grows without bound: permanently rejected pushes are dropped instead of retried, delivered/expired/suppressed rows are deleted, and the stale-processing sweep is index-backed.
