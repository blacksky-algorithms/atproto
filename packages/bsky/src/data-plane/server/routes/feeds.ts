import type { ServiceImpl } from '@connectrpc/connect'
import { type SqlBool, sql } from 'kysely'
import type { Service } from '../../../proto/bsky_connect.js'
import { FeedType } from '../../../proto/bsky_pb.js'
import type { Database } from '../db/index.js'
import { TimeCidKeyset, paginate } from '../db/pagination.js'
import {
  type CommunityPostRow,
  communityPostFromRow,
} from './community-util.js'

type SortableRow = { sortAt: string; cid: string }

const bySortAtCidDesc = (a: SortableRow, b: SortableRow) => {
  if (a.sortAt > b.sortAt) return -1
  if (a.sortAt < b.sortAt) return 1
  return a.cid > b.cid ? -1 : 1
}

const STUB_POST_LIKE = '%/community.blacksky.feed.post/%'
const isStubPostUri = (uri: string) =>
  uri.includes('/community.blacksky.feed.post/')

const MEDIA_EMBED_TYPES = ['app.bsky.embed.images', 'app.bsky.embed.gallery']
const VIDEO_EMBED_TYPES = [
  'app.bsky.embed.video',
  'community.blacksky.embed.video',
]

const embedTypeFilter = (types: string[]) =>
  sql<SqlBool>`("embed"->>'$type' = any(${sql.val(types)}::text[]) OR "embed"->'media'->>'$type' = any(${sql.val(types)}::text[]))`

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getAuthorFeed(req) {
    const { actorDid, limit, cursor, feedType } = req
    const { ref } = db.db.dynamic

    // defaults to posts, reposts, and replies
    let builder = db.db
      .selectFrom('feed_item')
      .innerJoin('post', 'post.uri', 'feed_item.postUri')
      .selectAll('feed_item')
      .where('originatorDid', '=', actorDid)

    if (feedType === FeedType.POSTS_WITH_MEDIA) {
      builder = builder
        // only your own posts
        .where('type', '=', 'post')
        // only posts with media
        .where((eb) =>
          eb.or([
            eb.exists(
              eb
                .selectFrom('post_embed_image')
                .select('post_embed_image.postUri')
                .whereRef('post_embed_image.postUri', '=', 'feed_item.postUri'),
            ),
            eb.exists(
              eb
                .selectFrom('post_embed_gallery_image')
                .select('post_embed_gallery_image.postUri')
                .whereRef(
                  'post_embed_gallery_image.postUri',
                  '=',
                  'feed_item.postUri',
                ),
            ),
          ]),
        )
    } else if (feedType === FeedType.POSTS_WITH_VIDEO) {
      builder = builder
        // only your own posts
        .where('type', '=', 'post')
        // only posts with video
        .where(({ eb, exists }) =>
          exists(
            eb
              .selectFrom('post_embed_video')
              .select('post_embed_video.postUri')
              .whereRef('post_embed_video.postUri', '=', 'feed_item.postUri'),
          ),
        )
    } else if (feedType === FeedType.POSTS_NO_REPLIES) {
      builder = builder.where((eb) =>
        eb.or([eb('post.replyParent', 'is', null), eb('type', '=', 'repost')]),
      )
    } else if (feedType === FeedType.POSTS_AND_AUTHOR_THREADS) {
      builder = builder.where((eb) =>
        eb.or([
          eb('type', '=', 'repost'),
          eb('post.replyParent', 'is', null),
          eb('post.replyRoot', 'like', `at://${actorDid}/%`),
        ]),
      )
    }

    const keyset = new TimeCidKeyset(
      ref('feed_item.sortAt'),
      ref('feed_item.cid'),
    )

    builder = paginate(builder, {
      limit,
      cursor,
      keyset,
    })

    const feedItems = await builder.execute()

    if (!req.includeCommunityPosts) {
      return {
        items: feedItems.map(feedItemFromRow),
        cursor: keyset.packFromResult(feedItems),
      }
    }

    const [communityRows, communityReposts] = await Promise.all([
      getCommunityAuthorRows(db, { actorDid, limit, cursor, feedType }),
      getCommunityRepostEntries(db, { actorDid, limit, cursor, feedType }),
    ])
    const merged = mergeWithCommunityRows(
      [
        ...feedItems.map((row) => ({
          sortAt: row.sortAt,
          cid: row.cid,
          item: feedItemFromRow(row),
        })),
        ...communityReposts.entries,
      ],
      communityRows,
      limit,
      communityReposts.rows,
    )

    return {
      items: merged.entries.map((m) => m.item),
      cursor: keyset.packFromResult(merged.entries),
      communityPosts: merged.communityPosts,
    }
  },

  async getTimeline(req) {
    const { actorDid, limit, cursor } = req
    const { ref } = db.db.dynamic

    const keyset = new TimeCidKeyset(
      ref('feed_item.sortAt'),
      ref('feed_item.cid'),
    )

    // Parse cursor for the LATERAL query
    const cursorValues = keyset.unpack(cursor)

    // Use LATERAL JOIN to force PostgreSQL to use feed_item_originator_cursor_idx
    // per followed DID, instead of scanning the entire feed_item table backwards.
    // This is O(follows * limit) index lookups instead of O(feed_item rows).
    const cursorClause = cursorValues
      ? sql`AND ("sortAt", "cid") < (${cursorValues.primary}, ${cursorValues.secondary})`
      : sql``
    // A repost of a community post is a feed_item like any other; a
    // non-member's page must not carry it, or lose a slot to it.
    const stubClause = req.includeCommunityPosts
      ? sql``
      : sql`AND "feed_item"."postUri" NOT LIKE ${STUB_POST_LIKE}`

    const followRes = await sql<{
      uri: string
      cid: string
      type: string
      postUri: string
      originatorDid: string
      sortAt: string
    }>`
      SELECT fi.* FROM (
        SELECT "subjectDid" FROM "follow" WHERE "creator" = ${actorDid}
      ) AS followed
      CROSS JOIN LATERAL (
        SELECT * FROM "feed_item"
        WHERE "feed_item"."originatorDid" = followed."subjectDid"
          ${cursorClause}
          ${stubClause}
        ORDER BY "feed_item"."sortAt" DESC, "feed_item"."cid" DESC
        LIMIT ${limit}
      ) AS fi
      ORDER BY fi."sortAt" DESC, fi."cid" DESC
      LIMIT ${limit}
    `.execute(db.db)

    // Self-posts query uses the originator index directly
    let selfQb = db.db
      .selectFrom('feed_item')
      .where('feed_item.originatorDid', '=', actorDid)
      .$if(!req.includeCommunityPosts, (qb) =>
        qb.where('feed_item.postUri', 'not like', STUB_POST_LIKE),
      )
      .selectAll('feed_item')

    selfQb = paginate(selfQb, {
      limit: Math.min(limit, 10),
      cursor,
      keyset,
      tryIndex: true,
    })

    const selfRes = await selfQb.execute()

    const feedItems = [...followRes.rows, ...selfRes]
      .sort(bySortAtCidDesc)
      .slice(0, limit)

    if (!req.includeCommunityPosts) {
      return {
        items: feedItems.map(feedItemFromRow),
        cursor: keyset.packFromResult(feedItems),
      }
    }

    const [communityRows, subjectRows] = await Promise.all([
      getCommunityTimelineRows(db, { actorDid, limit, cursorClause }),
      getCommunitySubjectRows(
        db,
        feedItems
          .filter((row) => row.type === 'repost' && isStubPostUri(row.postUri))
          .map((row) => row.postUri),
      ),
    ])
    const merged = mergeWithCommunityRows(
      feedItems.map((row) => ({
        sortAt: row.sortAt,
        cid: row.cid,
        item: feedItemFromRow(row),
      })),
      communityRows,
      limit,
      subjectRows,
    )

    return {
      items: merged.entries.map((m) => m.item),
      cursor: keyset.packFromResult(merged.entries),
      communityPosts: merged.communityPosts,
    }
  },

  async getListFeed(req) {
    const { listUri, cursor, limit } = req
    const { ref } = db.db.dynamic

    const keyset = new TimeCidKeyset(ref('post.sortAt'), ref('post.cid'))
    const cursorValues = keyset.unpack(cursor)

    // Use LATERAL JOIN to force PostgreSQL to use post_creator_cursor_idx
    // per list member, instead of scanning the entire post table backwards.
    const cursorClause = cursorValues
      ? sql`AND ("sortAt", "cid") < (${cursorValues.primary}, ${cursorValues.secondary})`
      : sql``

    const res = await sql<{
      uri: string
      cid: string
      sortAt: string
    }>`
      SELECT p."uri", p."cid", p."sortAt" FROM (
        SELECT "subjectDid" FROM "list_item" WHERE "listUri" = ${listUri}
      ) AS member
      CROSS JOIN LATERAL (
        SELECT * FROM "post"
        WHERE "post"."creator" = member."subjectDid"
          ${cursorClause}
        ORDER BY "post"."sortAt" DESC, "post"."cid" DESC
        LIMIT ${limit}
      ) AS p
      ORDER BY p."sortAt" DESC, p."cid" DESC
      LIMIT ${limit}
    `.execute(db.db)

    return {
      items: res.rows.map((item) => ({ uri: item.uri, cid: item.cid })),
      cursor: keyset.packFromResult(res.rows),
    }
  },
})

// @NOTE does not support additional fields in the protos specific to author feeds
// and timelines. at the time of writing, hydration/view implementations do not rely on them.
const feedItemFromRow = (row: { postUri: string; uri: string }) => {
  return {
    uri: row.postUri,
    repost: row.uri === row.postUri ? undefined : row.uri,
  }
}

type CommunityQueryRow = CommunityPostRow & SortableRow & { uri: string }

type MergeEntry = SortableRow & { item: { uri: string; repost?: string } }

// Interleave community rows into an already-sorted standard skeleton by
// (sortAt DESC, cid DESC), slicing to limit. Community rows for surviving
// entries ride along so the caller never refetches them. `subjectRows` are
// the community posts that standard repost entries point at: they travel
// with the page but are never entries of their own, or the original post
// would be injected into a feed that only carried its repost.
const mergeWithCommunityRows = (
  standardEntries: MergeEntry[],
  communityRows: CommunityQueryRow[],
  limit: number,
  subjectRows: CommunityQueryRow[] = [],
) => {
  const communityByUri = new Map(
    [...communityRows, ...subjectRows].map((row) => [row.uri, row]),
  )
  const entries = [
    ...standardEntries,
    ...communityRows.map((row) => ({
      sortAt: row.sortAt,
      cid: row.cid,
      item: { uri: row.uri },
    })),
  ]
    .sort(bySortAtCidDesc)
    .slice(0, limit)
  const communityPosts = entries
    .map((m) => communityByUri.get(m.item.uri))
    .filter((row) => row !== undefined)
    .map((row) => communityPostFromRow(row))
  return { entries, communityPosts }
}

// Community posts authored by DIDs the actor follows, plus the actor's own,
// keyset-bounded to match the standard timeline pagination.
const getCommunityTimelineRows = async (
  db: Database,
  opts: {
    actorDid: string
    limit: number
    cursorClause: ReturnType<typeof sql>
  },
) => {
  const { actorDid, limit, cursorClause } = opts
  const res = await sql<CommunityQueryRow>`
    SELECT cp.* FROM (
      SELECT "subjectDid" FROM "follow" WHERE "creator" = ${actorDid}
      UNION SELECT ${actorDid}
    ) AS member
    CROSS JOIN LATERAL (
      SELECT * FROM "community_post"
      WHERE "community_post"."creator" = member."subjectDid"
        AND "community_post"."space_uri" IS NULL
        AND "community_post"."moderation_flagged_at" IS NULL
        ${cursorClause}
      ORDER BY "community_post"."sortAt" DESC, "community_post"."cid" DESC
      LIMIT ${limit}
    ) AS cp
    ORDER BY cp."sortAt" DESC, cp."cid" DESC
    LIMIT ${limit}
  `.execute(db.db)
  return res.rows
}

// The actor's community posts, filtered per the author-feed type and
// keyset-bounded to match the standard author-feed pagination.
const getCommunityAuthorRows = async (
  db: Database,
  opts: {
    actorDid: string
    limit: number
    cursor?: string
    feedType: FeedType
  },
): Promise<CommunityQueryRow[]> => {
  const { actorDid, limit, cursor, feedType } = opts
  const { ref } = db.db.dynamic

  let builder = db.db
    .selectFrom('community_post')
    .selectAll()
    .where('creator', '=', actorDid)
    .where('space_uri', 'is', null)
    .where('moderation_flagged_at', 'is', null)

  if (feedType === FeedType.POSTS_WITH_MEDIA) {
    builder = builder.where(embedTypeFilter(MEDIA_EMBED_TYPES))
  } else if (feedType === FeedType.POSTS_WITH_VIDEO) {
    builder = builder.where(embedTypeFilter(VIDEO_EMBED_TYPES))
  } else if (feedType === FeedType.POSTS_NO_REPLIES) {
    builder = builder.where('replyParent', 'is', null)
  } else if (feedType === FeedType.POSTS_AND_AUTHOR_THREADS) {
    builder = builder.where((eb) =>
      eb.or([
        eb('replyParent', 'is', null),
        eb('replyRoot', 'like', `at://${actorDid}/%`),
      ]),
    )
  }

  const keyset = new TimeCidKeyset(
    ref('community_post.sortAt'),
    ref('community_post.cid'),
  )
  builder = paginate(builder, { limit, cursor, keyset, tryIndex: true })

  const rows = await builder.execute()
  return rows as unknown as CommunityQueryRow[]
}

// The community posts that repost feed items on a page point at. Only the
// legacy Blacksky tier can be reposted, so rows in a space never qualify.
const getCommunitySubjectRows = async (
  db: Database,
  uris: string[],
): Promise<CommunityQueryRow[]> => {
  if (uris.length === 0) return []
  const rows = await db.db
    .selectFrom('community_post')
    .selectAll()
    .where('uri', 'in', [...new Set(uris)])
    .where('space_uri', 'is', null)
    .where('moderation_flagged_at', 'is', null)
    .execute()
  return rows as unknown as CommunityQueryRow[]
}

// The actor's reposts of community posts, as merge entries keyed by the
// repost (its sortAt and cid drive the shared keyset cursor) and carrying
// the subject row. Read from `repost` rather than `feed_item` so the
// (creator, subject) index narrows the scan to this creator's reposts, and
// joined to the subject so deleted and flagged posts never consume the limit.
// The repost columns are aliased because community_post shares `uri`, `cid`
// and `sortAt` and node-postgres resolves duplicate names positionally.
const getCommunityRepostEntries = async (
  db: Database,
  opts: {
    actorDid: string
    limit: number
    cursor?: string
    feedType: FeedType
  },
): Promise<{ entries: MergeEntry[]; rows: CommunityQueryRow[] }> => {
  const { actorDid, limit, cursor, feedType } = opts
  if (
    feedType === FeedType.POSTS_WITH_MEDIA ||
    feedType === FeedType.POSTS_WITH_VIDEO
  ) {
    return { entries: [], rows: [] }
  }
  const { ref } = db.db.dynamic
  const keyset = new TimeCidKeyset(ref('rp.sortAt'), ref('rp.cid'))
  const cursorValues = keyset.unpack(cursor)
  const cursorClause = cursorValues
    ? sql`AND (rp."sortAt", rp.cid) < (${cursorValues.primary}, ${cursorValues.secondary})`
    : sql``
  const res = await sql<
    CommunityQueryRow & {
      repostUri: string
      repostCid: string
      repostSortAt: string
    }
  >`
    SELECT rp.uri AS "repostUri", rp.cid AS "repostCid", rp."sortAt" AS "repostSortAt", cp.*
    FROM repost rp
    JOIN community_post cp
      ON cp.uri = rp.subject
      AND cp.space_uri IS NULL
      AND cp.moderation_flagged_at IS NULL
    WHERE rp.creator = ${actorDid}
      AND rp.subject LIKE ${STUB_POST_LIKE}
      ${cursorClause}
    ORDER BY rp."sortAt" DESC, rp.cid DESC
    LIMIT ${limit}
  `.execute(db.db)
  return {
    entries: res.rows.map((row) => ({
      sortAt: row.repostSortAt,
      cid: row.repostCid,
      item: { uri: row.uri, repost: row.repostUri },
    })),
    rows: res.rows,
  }
}
