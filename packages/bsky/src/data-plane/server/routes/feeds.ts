import { sql } from 'kysely'
import { ServiceImpl } from '@connectrpc/connect'
import { Service } from '../../../proto/bsky_connect'
import { FeedType } from '../../../proto/bsky_pb'
import { Database } from '../db'
import { TimeCidKeyset, paginate } from '../db/pagination'

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
        .whereExists((qb) =>
          qb
            .selectFrom('post_embed_image')
            .select('post_embed_image.postUri')
            .whereRef('post_embed_image.postUri', '=', 'feed_item.postUri'),
        )
    } else if (feedType === FeedType.POSTS_WITH_VIDEO) {
      builder = builder
        // only your own posts
        .where('type', '=', 'post')
        // only posts with video
        .whereExists((qb) =>
          qb
            .selectFrom('post_embed_video')
            .select('post_embed_video.postUri')
            .whereRef('post_embed_video.postUri', '=', 'feed_item.postUri'),
        )
    } else if (feedType === FeedType.POSTS_NO_REPLIES) {
      builder = builder.where((qb) =>
        qb.where('post.replyParent', 'is', null).orWhere('type', '=', 'repost'),
      )
    } else if (feedType === FeedType.POSTS_AND_AUTHOR_THREADS) {
      builder = builder.where((qb) =>
        qb
          .where('type', '=', 'repost')
          .orWhere('post.replyParent', 'is', null)
          .orWhere('post.replyRoot', 'like', `at://${actorDid}/%`),
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

    return {
      items: feedItems.map(feedItemFromRow),
      cursor: keyset.packFromResult(feedItems),
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
      .selectAll('feed_item')

    selfQb = paginate(selfQb, {
      limit: Math.min(limit, 10),
      cursor,
      keyset,
      tryIndex: true,
    })

    const selfRes = await selfQb.execute()

    const feedItems = [...followRes.rows, ...selfRes]
      .sort((a, b) => {
        if (a.sortAt > b.sortAt) return -1
        if (a.sortAt < b.sortAt) return 1
        return a.cid > b.cid ? -1 : 1
      })
      .slice(0, limit)

    return {
      items: feedItems.map(feedItemFromRow),
      cursor: keyset.packFromResult(feedItems),
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
