---
'@atproto/bsky': patch
---

Close cross-space leaks found in pre-rollout review of space-backed community feeds. The space-keyed gate now derives a post's space from its URI when the row is absent or filtered, so a moderation-flagged space post can no longer fall through to the community-wide membership check. Space-post notifications resolve existence against `community_post` instead of the record store, so they are listed and counted again, and reply/quote notifications for them go to the real author rather than to the space's DID. Thread assembly drops gated ancestors and descendants instead of emitting an empty item that still carries their URI, and `submitPost` refuses a reply or quote that names a space, which is what let a thread span two access domains. Also restores the moderation-flag filter on the merged timeline query and on push snippet text, stops minting CDN URLs for space-post blobs that no CDN can fetch, and no longer logs submitted post text.
