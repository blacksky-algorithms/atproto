import type { ServiceImpl } from '@connectrpc/connect'
import type { Service } from '../../../proto/bsky_connect.js'
import type { Database } from '../db/index.js'
import { communityPostFromRow } from './community-util.js'

export const HOT_WINDOW_DAYS = 7
export const HOT_GRAVITY = 1.8
export const HOT_BASELINE = 0.3
export const HOT_QUOTE_WEIGHT = 2
export const HOT_REPLY_WEIGHT = 3
export const HOT_MIN_LIKES = 1
export const HOT_MIN_INTERACTIONS = 2

const CURSOR_SEPARATOR = '::'

export type HotCursor = {
  anchor: string
  score: string
  cid: string
}

export const formatHotCursor = ({ anchor, score, cid }: HotCursor): string =>
  [anchor, score, cid].join(CURSOR_SEPARATOR)

export const parseHotCursor = (
  cursor: string | undefined,
): HotCursor | null => {
  if (!cursor) return null
  const parts = cursor.split(CURSOR_SEPARATOR)
  if (parts.length !== 3) return null
  const [anchor, score, cid] = parts
  if (Number.isNaN(Date.parse(anchor))) return null
  if (!/^\d+$/.test(score)) return null
  if (!cid) return null
  return { anchor, score, cid }
}

export const hotWindowStart = (anchor: string): string =>
  new Date(
    Date.parse(anchor) - HOT_WINDOW_DAYS * 24 * 60 * 60 * 1000,
  ).toISOString()

// Score = (baseline + engagement) / (age_hours + 2) ^ gravity, the Hacker
// News ranking. Everything is measured as of the anchor carried in the cursor
// so that later pages rank against the same snapshot as the first one. Posts
// under the like and interaction floors never rank, however fresh they are.
const HOT_QUERY = `
  WITH window_posts AS (
    SELECT * FROM community_post
    WHERE space_uri IS NULL AND moderation_flagged_at IS NULL
      AND "sortAt" > $2::text AND "sortAt" <= $1::text
  ), replies AS (
    SELECT "replyParent" AS uri, count(*)::int AS n
    FROM window_posts WHERE "replyParent" IS NOT NULL GROUP BY 1
  ), quotes AS (
    SELECT COALESCE(embed->'record'->>'uri', embed->'record'->'record'->>'uri') AS uri,
           count(*)::int AS n
    FROM window_posts WHERE embed IS NOT NULL GROUP BY 1
  ), scored AS (
    SELECT p.*,
      round(1000000 * (${HOT_BASELINE} + COALESCE(pa."likeCount", 0)
          + ${HOT_QUOTE_WEIGHT} * COALESCE(q.n, 0)
          + ${HOT_REPLY_WEIGHT} * COALESCE(r.n, 0))
        / power(EXTRACT(epoch FROM ($1::text::timestamptz - p."indexedAt"::timestamptz)) / 3600 + 2, ${HOT_GRAVITY}))::bigint AS score
    FROM window_posts p
    LEFT JOIN post_agg pa ON pa.uri = p.uri
    LEFT JOIN replies r ON r.uri = p.uri
    LEFT JOIN quotes q ON q.uri = p.uri
    WHERE p."replyParent" IS NULL
      AND COALESCE(pa."likeCount", 0) >= ${HOT_MIN_LIKES}
      AND COALESCE(pa."likeCount", 0) + COALESCE(q.n, 0) + COALESCE(r.n, 0) >= ${HOT_MIN_INTERACTIONS}
  )
  SELECT * FROM scored
  WHERE $3::bigint IS NULL OR score < $3::bigint OR (score = $3::bigint AND cid < $4::text)
  ORDER BY score DESC, cid DESC
  LIMIT $5
`

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getCommunityHotTimeline(req) {
    const { limit } = req
    const cursor = parseHotCursor(req.cursor)
    const anchor = cursor?.anchor ?? new Date().toISOString()
    const res = await db.pool.query(HOT_QUERY, [
      anchor,
      hotWindowStart(anchor),
      cursor?.score ?? null,
      cursor?.cid ?? '',
      limit + 1,
    ])
    const rows = res.rows
    let nextCursor = ''
    if (rows.length > limit) {
      rows.pop()
      const last = rows[rows.length - 1]
      nextCursor = formatHotCursor({
        anchor,
        score: String(last.score),
        cid: last.cid,
      })
    }
    return {
      posts: rows.map(communityPostFromRow),
      cursor: nextCursor,
    }
  },
})
