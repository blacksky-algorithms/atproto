---
'@atproto/bsky': patch
---

Honor mute scopes when suppressing push notifications. The push bridge treated any `mute` row for a (recipient, author) pair as a full mute, but a scoped mute (`onlyReposts` / `onlyQuoteposts`) restricts the mute to that content rather than muting the account, so a scoped mute silently suppressed every push notification from that account while `listNotifications` continued to show them. Suppression now applies only to unscoped mutes, matching `getRelationships` and `getActorMutesActor`.
