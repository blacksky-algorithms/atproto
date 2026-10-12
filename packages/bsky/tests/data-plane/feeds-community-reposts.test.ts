import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Database } from '../../src/data-plane/server/db/index.js'
import feedRoutes from '../../src/data-plane/server/routes/feeds.js'
import { FeedType } from '../../src/proto/bsky_pb.js'

/**
 * Reposts of Blacksky-only posts are ordinary `feed_item` / `repost` rows
 * whose subject lives in `community_post`. Members get the subject row along
 * with the page; non-members never see the item and never lose a slot to it.
 */

const VIEWER = 'did:plc:viewer'
const REPOSTER = 'did:plc:reposter'
const AUTHOR = 'did:plc:author'
const STUB = `at://${AUTHOR}/community.blacksky.feed.post/stub`
const FLAGGED = `at://${AUTHOR}/community.blacksky.feed.post/flagged`
const GONE = `at://${AUTHOR}/community.blacksky.feed.post/gone`
const PUBLIC_POST = `at://${REPOSTER}/app.bsky.feed.post/pub`

describe('data plane feeds with reposts of community posts', () => {
  let db: Database

  beforeAll(async () => {
    const url = process.env.DB_TEST_POSTGRES_URL || process.env.DB_POSTGRES_URL
    if (!url) {
      throw new Error('Missing DB_TEST_POSTGRES_URL or DB_POSTGRES_URL')
    }
    db = new Database({ url, schema: 'bsky_feeds_community_reposts' })
    await db.migrateToLatestOrThrow()
  })

  afterAll(async () => {
    await db.close()
  })

  afterEach(async () => {
    await db.db.deleteFrom('feed_item').execute()
    await db.db.deleteFrom('repost').execute()
    await db.db.deleteFrom('community_post').execute()
    await db.db.deleteFrom('follow').execute()
    await db.db.deleteFrom('post').execute()
  })

  async function insertStub(uri: string, createdAt: string, flagged = false) {
    await db.db
      .insertInto('community_post')
      .values({
        uri,
        cid: `bafy${uri.split('/').pop()}`,
        rkey: uri.split('/').pop() as string,
        creator: AUTHOR,
        text: 'community only post',
        moderation_flagged_at: flagged ? createdAt : null,
        createdAt,
        indexedAt: createdAt,
      })
      .execute()
  }

  async function insertRepost(
    rkey: string,
    subject: string,
    createdAt: string,
  ) {
    const uri = `at://${REPOSTER}/app.bsky.feed.repost/${rkey}`
    const cid = `bafyrepost${rkey}`
    await db.db
      .insertInto('repost')
      .values({
        uri,
        cid,
        creator: REPOSTER,
        subject,
        subjectCid: 'bafysubject',
        createdAt,
        indexedAt: createdAt,
      })
      .execute()
    await db.db
      .insertInto('feed_item')
      .values({
        uri,
        cid,
        type: 'repost',
        postUri: subject,
        originatorDid: REPOSTER,
        sortAt: createdAt,
      })
      .execute()
    return { uri, cid }
  }

  async function insertPublicPost(createdAt: string) {
    await db.db
      .insertInto('post')
      .values({
        uri: PUBLIC_POST,
        cid: 'bafypublic',
        creator: REPOSTER,
        text: 'public',
        createdAt,
        indexedAt: createdAt,
      })
      .execute()
    await db.db
      .insertInto('feed_item')
      .values({
        uri: PUBLIC_POST,
        cid: 'bafypublic',
        type: 'post',
        postUri: PUBLIC_POST,
        originatorDid: REPOSTER,
        sortAt: createdAt,
      })
      .execute()
  }

  async function follow() {
    await db.db
      .insertInto('follow')
      .values({
        uri: `at://${VIEWER}/app.bsky.graph.follow/f`,
        cid: 'bafyfollow',
        creator: VIEWER,
        subjectDid: REPOSTER,
        createdAt: '2026-09-01T00:00:00.000Z',
        indexedAt: '2026-09-01T00:00:00.000Z',
      })
      .execute()
  }

  const routes = () => feedRoutes(db)

  it('withholds community reposts from a non-member and fills the page', async () => {
    await follow()
    await insertStub(STUB, '2026-10-01T00:00:00.000Z')
    await insertRepost('r1', STUB, '2026-10-02T00:00:00.000Z')
    await insertPublicPost('2026-10-01T12:00:00.000Z')

    const res = await routes().getTimeline!(
      {
        actorDid: VIEWER,
        limit: 1,
        cursor: '',
        includeCommunityPosts: false,
      } as any,
      {} as any,
    )

    expect(res.items).toEqual([{ uri: PUBLIC_POST, repost: undefined }])
    expect(res.communityPosts ?? []).toEqual([])
  })

  it('gives a member the repost with its subject row, author unfollowed', async () => {
    await follow()
    await insertStub(STUB, '2026-10-01T00:00:00.000Z')
    const repost = await insertRepost('r1', STUB, '2026-10-02T00:00:00.000Z')

    const res = await routes().getTimeline!(
      {
        actorDid: VIEWER,
        limit: 10,
        cursor: '',
        includeCommunityPosts: true,
      } as any,
      {} as any,
    )

    expect(res.items).toEqual([{ uri: STUB, repost: repost.uri }])
    expect(res.communityPosts.map((row) => row.uri)).toEqual([STUB])
  })

  it('omits the subject row of a flagged or deleted post', async () => {
    await follow()
    await insertStub(FLAGGED, '2026-10-01T00:00:00.000Z', true)
    await insertRepost('r1', FLAGGED, '2026-10-02T00:00:00.000Z')
    await insertRepost('r2', GONE, '2026-10-03T00:00:00.000Z')

    const res = await routes().getTimeline!(
      {
        actorDid: VIEWER,
        limit: 10,
        cursor: '',
        includeCommunityPosts: true,
      } as any,
      {} as any,
    )

    expect(res.items.map((item) => item.uri)).toEqual([GONE, FLAGGED])
    expect(res.communityPosts).toEqual([])
  })

  it('lists a member profile repost keyed by the repost, skipping ineligible subjects', async () => {
    await insertStub(STUB, '2026-09-01T00:00:00.000Z')
    await insertStub(FLAGGED, '2026-09-01T00:00:00.000Z', true)
    const valid = await insertRepost('valid', STUB, '2026-10-01T00:00:00.000Z')
    await insertRepost('flagged', FLAGGED, '2026-10-02T00:00:00.000Z')
    await insertRepost('gone', GONE, '2026-10-03T00:00:00.000Z')

    const res = await routes().getAuthorFeed!(
      {
        actorDid: REPOSTER,
        limit: 1,
        cursor: '',
        feedType: FeedType.POSTS_NO_REPLIES,
        includeCommunityPosts: true,
      } as any,
      {} as any,
    )

    expect(res.items).toEqual([{ uri: STUB, repost: valid.uri }])
    expect(res.communityPosts.map((row) => row.uri)).toEqual([STUB])
    // the cursor is the repost's position, not the older post's
    expect(res.cursor).toBe(
      `${new Date('2026-10-01T00:00:00.000Z').getTime()}__${valid.cid}`,
    )

    const next = await routes().getAuthorFeed!(
      {
        actorDid: REPOSTER,
        limit: 1,
        cursor: res.cursor,
        feedType: FeedType.POSTS_NO_REPLIES,
        includeCommunityPosts: true,
      } as any,
      {} as any,
    )
    expect(next.items).toEqual([])
  })

  it('keeps profile reposts off the media and video feeds and away from non-members', async () => {
    await insertStub(STUB, '2026-09-01T00:00:00.000Z')
    await insertRepost('valid', STUB, '2026-10-01T00:00:00.000Z')

    for (const feedType of [
      FeedType.POSTS_WITH_MEDIA,
      FeedType.POSTS_WITH_VIDEO,
    ]) {
      const res = await routes().getAuthorFeed!(
        {
          actorDid: REPOSTER,
          limit: 10,
          cursor: '',
          feedType,
          includeCommunityPosts: true,
        } as any,
        {} as any,
      )
      expect(res.items).toEqual([])
    }
    const nonMember = await routes().getAuthorFeed!(
      {
        actorDid: REPOSTER,
        limit: 10,
        cursor: '',
        feedType: FeedType.POSTS_NO_REPLIES,
        includeCommunityPosts: false,
      } as any,
      {} as any,
    )
    expect(nonMember.items).toEqual([])
  })
})
