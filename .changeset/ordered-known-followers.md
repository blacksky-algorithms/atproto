---
'@atproto/bsky': patch
---

Order the known-followers dataplane query deterministically. The batched self-JOIN behind `getFollowsFollowing` had no `ORDER BY`, so the planner was free to emit rows in any order on identical data. Because callers truncate the resulting list to the first few entries, this decided *which* known followers a viewer sees, not merely their sequence. Results are now ordered by the viewer's follow cursor, which is served by the existing `follow_creator_cursor_idx`.
