---
'@atproto/bsky': patch
---

Enforce thread mutes and follows-only notification preferences across the read and push paths. `listNotifications` now hides notifications whose subject sits in a thread the viewer muted (covering rows written before the mute, and rows written by indexers that skip write-time filtering) and notifications from non-followed accounts for reasons set to `include: 'follows'`. The notification push bridge suppresses the same rows before handing them to courier. Also adds an idempotent migration restoring `thread_mute.createdAt` on deployments whose hand-provisioned schema lacks it, which made every `muteThread` call fail with a 502.
