---
'@atproto/bsky': patch
---

Stop sending the notification reason as the push collapse key. Pushes for the same kind of interaction no longer replace each other on the device, so several likes, reposts, or follows arriving together each produce their own push.
