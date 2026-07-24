---
'@atproto/bsky': patch
---

Stop auto-honoring the deprecated per-actor `priorityNotifications` flag in notification listing and unread count. `listNotifications`/`getUnreadCount` now default `priority` to `false` and apply follows-only filtering only when a client explicitly requests it, so a stale stored flag can no longer silently hide notifications from non-followed accounts (BA-271).
