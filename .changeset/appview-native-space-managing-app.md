---
'@atproto/bsky': patch
---

Fix native-space access checks: discover a space's managing app from a pinned `COMMUNITY_SPACE_MANAGING_APP` service identity instead of the retired credential-mint + `getSpace` discovery path (which now 404s), and drop the appview's space-credential minting. Access is still decided by the managing app's `community.blacksky.space.checkAccess`, gated to the `community.blacksky.feed` space type and failing closed on an absent or malformed pin, an unmanaged space type, or an unreachable managing app.
