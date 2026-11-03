---
'@atproto/bsky': minor
---

Add an optional delivery bucket for private space images. When `COMMUNITY_MEDIA_DELIVERY_ENABLED` is set, space image URLs are presigned against the delivery bucket under `spaces/{authority}/{skey}/{did}/{cid}` with separate read-only credentials; when unset, the existing PDS-bucket presign is unchanged.
