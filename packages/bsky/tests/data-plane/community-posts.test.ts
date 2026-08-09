import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { TestNetwork } from '@atproto/dev-env'
import { FeedType } from '../../src/proto/bsky_pb.js'
import communityRoutes from '../../src/data-plane/server/routes/community.js'
import feedRoutes from '../../src/data-plane/server/routes/feeds.js'
import { Database } from '../../src/index.js'

describe('community post tenant discriminator', () => {
  let network: TestNetwork
  let db: Database

  beforeAll(async () => {
    network = await TestNetwork.create({ dbPostgresSchema: 'bsky_community' })
    db = network.bsky.db
  })

  beforeEach(async () => {
    await db.pool.query('DELETE FROM community_post')
    // The config-resolution test inserts a `record` row; without clearing it
    // the suite passes on a fresh volume and fails on every run after.
    await db.pool.query(
      `DELETE FROM record WHERE uri = 'at://did:plc:tenant/community.blacksky.feed.config/private'`,
    )
  })

  afterAll(async () => {
    await network?.close()
  })

  const insertPost = async (uri: string, spaceUri: string | null) => {
    await db.pool.query(
      `INSERT INTO community_post
        (uri, cid, rkey, creator, text, "createdAt", "indexedAt", space_uri)
       VALUES ($1, 'bafytest', $2::varchar, 'did:plc:alice', $2::text, $3, $3, $4)`,
      [uri, uri.split('/').at(-1), new Date().toISOString(), spaceUri],
    )
  }

  it('keeps tenant rows out of Blacksky timeline and actor feeds', async () => {
    const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
    const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    await insertPost(legacyUri, null)
    await insertPost(
      tenantUri,
      'at://did:plc:tenant/space/community.blacksky.feed/private',
    )

    const routes = communityRoutes(db, undefined) as any
    const timeline = await routes.getCommunityTimeline({
      limit: 10,
      cursor: '',
    })
    const byActor = await routes.getCommunityFeedByActor({
      actorDid: 'did:plc:alice',
      limit: 10,
      cursor: '',
    })
    const feeds = feedRoutes(db) as any
    const mergedTimeline = await feeds.getTimeline({
      actorDid: 'did:plc:alice',
      limit: 10,
      cursor: '',
      includeCommunityPosts: true,
    })
    const mergedAuthorFeed = await feeds.getAuthorFeed({
      actorDid: 'did:plc:alice',
      limit: 10,
      cursor: '',
      feedType: FeedType.UNSPECIFIED,
      includeCommunityPosts: true,
    })

    expect(timeline.posts.map((post: { uri: string }) => post.uri)).toEqual([
      legacyUri,
    ])
    expect(byActor.posts.map((post: { uri: string }) => post.uri)).toEqual([
      legacyUri,
    ])
    expect(
      mergedTimeline.communityPosts.map((post: { uri: string }) => post.uri),
    ).toEqual([legacyUri])
    expect(
      mergedTimeline.items.map((item: { uri: string }) => item.uri),
    ).toEqual([legacyUri])
    expect(
      mergedAuthorFeed.communityPosts.map((post: { uri: string }) => post.uri),
    ).toEqual([legacyUri])
    expect(
      mergedAuthorFeed.items.map((item: { uri: string }) => item.uri),
    ).toEqual([legacyUri])
  })

  it('returns the discriminator on per-post reads', async () => {
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const uri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    await insertPost(uri, spaceUri)

    const routes = communityRoutes(db, undefined) as any
    const result = await routes.getCommunityPost({ uri })
    const batch = await routes.getCommunityPosts({ uris: [uri] })

    expect(result.post.spaceUri).toBe(spaceUri)
    expect(batch.posts[0].spaceUri).toBe(spaceUri)
  })

  it('resolves the feed config record at the generator rkey', async () => {
    const feedUri = 'at://did:plc:tenant/app.bsky.feed.generator/private'
    const config = {
      $type: 'community.blacksky.feed.config',
      contentType: 'communityRecord',
      visibility: 'gated',
      authorization: { serviceDid: 'did:web:feeds.example.com' },
      group: 'at://did:plc:tenant/community.blacksky.group/private',
      createdAt: new Date().toISOString(),
    }
    await db.db
      .insertInto('record')
      .values({
        uri: 'at://did:plc:tenant/community.blacksky.feed.config/private',
        cid: 'bafyconfig',
        did: 'did:plc:tenant',
        json: JSON.stringify(config),
        indexedAt: new Date().toISOString(),
      })
      .execute()

    const routes = communityRoutes(db, undefined) as any
    await expect(routes.getCommunityFeedConfig({ feedUri })).resolves.toEqual({
      configJson: JSON.stringify(config),
    })
    await expect(
      routes.getCommunityFeedConfig({
        feedUri: 'at://did:plc:tenant/app.bsky.feed.post/private',
      }),
    ).resolves.toEqual({ configJson: '' })
  })

  it('stores NULL for legacy writes and a space URI for space writes', async () => {
    const routes = communityRoutes(db, undefined) as any
    const createdAt = new Date().toISOString()
    const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
    const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'

    await routes.submitCommunityPost({
      uri: legacyUri,
      rkey: 'legacy',
      creator: 'did:plc:alice',
      text: 'legacy',
      createdAt,
    })
    await routes.submitCommunityPost({
      uri: tenantUri,
      rkey: 'tenant',
      creator: 'did:plc:alice',
      text: 'tenant',
      createdAt,
      spaceUri,
    })

    const rows = await db.db
      .selectFrom('community_post')
      .select(['uri', 'space_uri'])
      .orderBy('uri')
      .execute()

    expect(rows).toEqual([
      { uri: legacyUri, space_uri: null },
      { uri: tenantUri, space_uri: spaceUri },
    ])
  })

  it.each([
    {
      name: 'legacy to tenant',
      initialFeed: undefined,
      submittedFeed:
        'at://did:plc:tenant/space/community.blacksky.feed/private',
    },
    {
      name: 'tenant to legacy',
      initialFeed: 'at://did:plc:tenant/space/community.blacksky.feed/private',
      submittedFeed: undefined,
    },
    {
      name: 'between tenants',
      initialFeed: 'at://did:plc:tenant/space/community.blacksky.feed/private',
      submittedFeed:
        'at://did:plc:other-tenant/app.bsky.feed.generator/private',
    },
  ])('rejects a $name resubmission', async ({ initialFeed, submittedFeed }) => {
    const routes = communityRoutes(db, undefined) as any
    const createdAt = new Date().toISOString()
    const uri = 'at://did:plc:alice/community.blacksky.feed.post/boundary'
    await routes.submitCommunityPost({
      uri,
      rkey: 'boundary',
      creator: 'did:plc:alice',
      text: 'original',
      createdAt,
      spaceUri: initialFeed,
    })

    const result = await routes.submitCommunityPost({
      uri,
      rkey: 'boundary',
      creator: 'did:plc:alice',
      text: 'replacement',
      createdAt,
      spaceUri: submittedFeed,
    })

    expect(result.rejected).toBe('FeedMismatch')
    await expect(
      db.db
        .selectFrom('community_post')
        .select(['text', 'space_uri'])
        .where('uri', '=', uri)
        .executeTakeFirstOrThrow(),
    ).resolves.toEqual({ text: 'original', space_uri: initialFeed ?? null })
  })

  it('allows idempotent resubmission within the stored feed', async () => {
    const routes = communityRoutes(db, undefined) as any
    const createdAt = new Date().toISOString()
    const uri = 'at://did:plc:alice/community.blacksky.feed.post/idempotent'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    await routes.submitCommunityPost({
      uri,
      rkey: 'idempotent',
      creator: 'did:plc:alice',
      text: 'original',
      createdAt,
      spaceUri,
    })

    const result = await routes.submitCommunityPost({
      uri,
      rkey: 'idempotent',
      creator: 'did:plc:alice',
      text: 'replacement',
      createdAt,
      spaceUri,
    })

    expect(result.rejected).toBeUndefined()
    await expect(
      db.db
        .selectFrom('community_post')
        .select(['text', 'space_uri'])
        .where('uri', '=', uri)
        .executeTakeFirstOrThrow(),
    ).resolves.toEqual({ text: 'replacement', space_uri: spaceUri })
  })

  describe('moderation flags', () => {
    const ROOT = 'at://did:plc:alice/community.blacksky.feed.post/root'
    const FLAGGED = 'at://did:plc:alice/community.blacksky.feed.post/flagged'
    const KEPT = 'at://did:plc:alice/community.blacksky.feed.post/kept'

    const insert = async (
      uri: string,
      opts: {
        replyRoot?: string
        replyParent?: string
        quotes?: string
        flagged?: boolean
      } = {},
    ) => {
      const now = new Date().toISOString()
      await db.pool.query(
        `INSERT INTO community_post
          (uri, cid, rkey, creator, text, "createdAt", "indexedAt",
           "replyRoot", "replyParent", embed, moderation_flagged_at)
         VALUES ($1, 'bafytest', $2, 'did:plc:alice', 'text', $3, $3,
                 $4, $5, $6::jsonb, $7)`,
        [
          uri,
          uri.split('/').at(-1),
          now,
          opts.replyRoot ?? null,
          opts.replyParent ?? null,
          opts.quotes ? JSON.stringify({ record: { uri: opts.quotes } }) : null,
          opts.flagged ? now : null,
        ],
      )
    }

    const routes = () => communityRoutes(db, undefined) as any
    const uris = (res: { posts: Array<{ uri: string }> }) =>
      res.posts.map((post) => post.uri)

    it('hides a flagged post from every read path that returns content', async () => {
      await insert(KEPT)
      await insert(FLAGGED, { flagged: true })

      const r = routes()
      expect(
        uris(await r.getCommunityTimeline({ limit: 10, cursor: '' })),
      ).toEqual([KEPT])
      expect(
        uris(
          await r.getCommunityFeedByActor({
            actorDid: 'did:plc:alice',
            limit: 10,
            cursor: '',
          }),
        ),
      ).toEqual([KEPT])
      expect((await r.getCommunityPost({ uri: FLAGGED })).post).toBeUndefined()
      expect(
        uris(await r.getCommunityPosts({ uris: [KEPT, FLAGGED] })),
      ).toEqual([KEPT])
    })

    it('hides a flagged reply from the thread and its counts', async () => {
      await insert(ROOT)
      await insert(KEPT, { replyRoot: ROOT, replyParent: ROOT })
      await insert(FLAGGED, {
        replyRoot: ROOT,
        replyParent: ROOT,
        flagged: true,
      })

      const r = routes()
      expect(
        uris(
          await r.getCommunityPostReplies({
            parentUri: ROOT,
            limit: 10,
            cursor: '',
          }),
        ),
      ).toEqual([KEPT])
      expect(await r.getCommunityPostReplyCount({ uri: ROOT })).toEqual({
        count: 1,
      })
    })

    it('hides a flagged quote from the quote list and its count', async () => {
      await insert(ROOT)
      await insert(KEPT, { quotes: ROOT })
      await insert(FLAGGED, { quotes: ROOT, flagged: true })

      const r = routes()
      expect(
        uris(
          await r.getCommunityPostQuotes({ uri: ROOT, limit: 10, cursor: '' }),
        ),
      ).toEqual([KEPT])
      expect(await r.getCommunityPostQuoteCount({ uri: ROOT })).toEqual({
        count: 1,
      })
    })

    it('keeps the row so a reversal is a local flip, not a re-fetch', async () => {
      await insert(FLAGGED, { flagged: true })
      const r = routes()
      expect((await r.getCommunityPost({ uri: FLAGGED })).post).toBeUndefined()
      // The author's copy is untouched: the row is still there to un-flag.
      expect(await r.communityPostExists({ uri: FLAGGED })).toEqual({
        exists: true,
      })

      await db.pool.query(
        'UPDATE community_post SET moderation_flagged_at = NULL WHERE uri = $1',
        [FLAGGED],
      )
      expect((await r.getCommunityPost({ uri: FLAGGED })).post).toBeDefined()
    })
  })
})
