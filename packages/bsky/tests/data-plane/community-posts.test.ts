import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { TestNetwork } from '@atproto/dev-env'
import { FeedType } from '../../src/proto/bsky_pb.js'
import communityRoutes from '../../src/data-plane/server/routes/community.js'
import feedRoutes from '../../src/data-plane/server/routes/feeds.js'
import likeRoutes from '../../src/data-plane/server/routes/likes.js'
import notificationRoutes from '../../src/data-plane/server/routes/notifs.js'
import { Database } from '../../src/index.js'

describe('community post tenant discriminator', () => {
  let network: TestNetwork
  let db: Database

  beforeAll(async () => {
    network = await TestNetwork.create({ dbPostgresSchema: 'bsky_community' })
    db = network.bsky.db
    // Production carries this index from out-of-band DDL (created CONCURRENTLY
    // so a boot-time migration never locks the live notification table); the
    // projected-notification ON CONFLICT depends on it, so the test schema
    // needs it too.
    await db.pool.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS notification_did_record_uri_reason_unique
       ON notification (did, "recordUri", reason)`,
    )
  })

  beforeEach(async () => {
    await db.pool.query('DELETE FROM community_post')
    await db.pool.query('DELETE FROM "like"')
    // The config-resolution test inserts a `record` row; without clearing it
    // the suite passes on a fresh volume and fails on every run after.
    await db.pool.query(
      `DELETE FROM record WHERE uri = 'at://did:plc:tenant/community.blacksky.feed.config/private'`,
    )
  })

  afterAll(async () => {
    await network?.close()
  })

  const insertPost = async (
    uri: string,
    spaceUri: string | null,
    timestamp = new Date().toISOString(),
    cid = 'bafytest',
  ) => {
    await db.pool.query(
      `INSERT INTO community_post
        (uri, cid, rkey, creator, text, "createdAt", "indexedAt", space_uri)
       VALUES ($1, $2, $3::varchar, 'did:plc:alice', $3::text, $4, $4, $5)`,
      [uri, cid, uri.split('/').at(-1), timestamp, spaceUri],
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

  it('requires a resolved space decision on per-post reads', async () => {
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const uri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    await insertPost(uri, spaceUri)

    const routes = communityRoutes(db, undefined) as any
    const result = await routes.getCommunityPost({ uri })
    const batch = await routes.getCommunityPosts({ uris: [uri] })
    const allowed = await routes.getCommunityPost({
      uri,
      allowedSpaceUris: [spaceUri],
    })
    const allowedBatch = await routes.getCommunityPosts({
      uris: [uri],
      allowedSpaceUris: [spaceUri],
    })

    expect(result.post).toBeUndefined()
    expect(batch.posts).toEqual([])
    expect(allowed.post.spaceUri).toBe(spaceUri)
    expect(allowedBatch.posts[0].spaceUri).toBe(spaceUri)
  })

  it('returns no space row from any community content RPC without a decision', async () => {
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const root = `${spaceUri}/did:plc:alice/app.bsky.feed.post/root`
    const child = `${spaceUri}/did:plc:alice/app.bsky.feed.post/child`
    await insertPost(root, spaceUri)
    await insertPost(child, spaceUri)
    await db.pool.query(
      `UPDATE community_post
       SET "replyRoot" = $1, "replyParent" = $1,
           embed = $2::jsonb
       WHERE uri = $3`,
      [root, JSON.stringify({ record: { uri: root } }), child],
    )
    const routes = communityRoutes(db, undefined) as any
    const reads = [
      (await routes.getCommunityPost({ uri: root })).post ? [root] : [],
      (await routes.getCommunityPosts({ uris: [root, child] })).posts.map(
        (post: { uri: string }) => post.uri,
      ),
      (
        await routes.getCommunityPostReplies({
          parentUri: root,
          limit: 10,
          cursor: '',
        })
      ).posts.map((post: { uri: string }) => post.uri),
      (
        await routes.getCommunityPostQuotes({
          uri: root,
          limit: 10,
          cursor: '',
        })
      ).posts.map((post: { uri: string }) => post.uri),
      (
        await routes.getCommunityFeedByActor({
          actorDid: 'did:plc:alice',
          limit: 10,
          cursor: '',
        })
      ).posts.map((post: { uri: string }) => post.uri),
      (await routes.getCommunityTimeline({ limit: 10, cursor: '' })).posts.map(
        (post: { uri: string }) => post.uri,
      ),
    ]

    expect(reads).toEqual([[], [], [], [], [], []])
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

  it('excludes space rows from public reply and quote aggregates', async () => {
    const root = 'at://did:plc:alice/community.blacksky.feed.post/root'
    const publicReply = 'at://did:plc:bob/community.blacksky.feed.post/public'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const spaceReply = `${spaceUri}/did:plc:bob/app.bsky.feed.post/private`
    await insertPost(root, null)
    await insertPost(publicReply, null)
    await insertPost(spaceReply, spaceUri)
    await db.pool.query(
      `UPDATE community_post
       SET "replyRoot" = $1, "replyParent" = $1,
           embed = $2::jsonb
       WHERE uri IN ($3, $4)`,
      [
        root,
        JSON.stringify({ record: { uri: root } }),
        publicReply,
        spaceReply,
      ],
    )
    const routes = communityRoutes(db, undefined) as any

    await expect(
      routes.getCommunityPostReplyCount({ uri: root }),
    ).resolves.toEqual({
      count: 1,
    })
    await expect(
      routes.getCommunityPostQuoteCount({ uri: root }),
    ).resolves.toEqual({
      count: 1,
    })
    const quotes = await routes.getCommunityPostQuotes({
      uri: root,
      limit: 10,
      cursor: '',
    })
    expect(quotes.posts.map((post: { uri: string }) => post.uri)).toEqual([
      publicReply,
    ])
  })

  it('keeps projected space likes out of every public like reader', async () => {
    const subject = 'at://did:plc:alice/community.blacksky.feed.post/root'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const routes = communityRoutes(db, undefined) as any
    await routes.projectCommunityRecord({
      collection: 'app.bsky.feed.like',
      operation: 'create',
      uri: `${spaceUri}/did:plc:bob/app.bsky.feed.like/private`,
      cid: 'bafylike',
      author: 'did:plc:bob',
      spaceUri,
      recordJson: JSON.stringify({
        subject: { uri: subject, cid: 'bafysubject' },
        createdAt: new Date().toISOString(),
      }),
    })

    await expect(
      routes.getCommunityPostLikeCount({ uri: subject }),
    ).resolves.toEqual({
      count: 0,
    })
    const publicLikes = likeRoutes(db) as any
    await expect(
      publicLikes.getActorLikes({
        actorDid: 'did:plc:bob',
        limit: 10,
        cursor: '',
      }),
    ).resolves.toMatchObject({ likes: [] })
    await expect(
      publicLikes.getLikesBySubjectSorted({
        subject: { uri: subject },
        limit: 10,
        cursor: '',
      }),
    ).resolves.toMatchObject({ uris: [] })
  })

  it('reads space likes with exact-space filtering and a stable tie-breaker', async () => {
    const subject =
      'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:alice/app.bsky.feed.post/root'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const otherSpaceUri =
      'at://did:plc:tenant/space/community.blacksky.feed/other'
    const timestamp = '2026-08-18T00:00:00.000Z'
    const route = likeRoutes(db) as any

    const insertLike = async (
      rkey: string,
      rowSpaceUri: string | null,
      creator: string,
    ) => {
      await db.pool.query(
        `INSERT INTO "like"
          (uri, cid, creator, subject, "subjectCid", "createdAt", "indexedAt", space_uri)
         VALUES ($1, $2, $3, $4, 'bafysubject', $5, $5, $6)`,
        [
          `${rowSpaceUri ?? 'at://did:plc:public/app.bsky.feed.like'}/${rkey}`,
          `bafylike${rkey}`,
          creator,
          subject,
          timestamp,
          rowSpaceUri,
        ],
      )
    }

    await insertLike('one', spaceUri, 'did:plc:bob')
    await insertLike('two', spaceUri, 'did:plc:carol')
    await insertLike('other', otherSpaceUri, 'did:plc:dan')
    await insertLike('public', null, 'did:plc:eve')

    const first = await route.getSpacePostLikes({
      subject: { uri: subject },
      limit: 1,
      cursor: '',
    })
    const second = await route.getSpacePostLikes({
      subject: { uri: subject },
      limit: 1,
      cursor: first.cursor,
    })
    const third = await route.getSpacePostLikes({
      subject: { uri: subject },
      limit: 1,
      cursor: second.cursor,
    })
    expect(first.likes).toHaveLength(1)
    expect(second.likes).toHaveLength(1)
    expect(third.likes).toHaveLength(0)
    expect(
      [...first.likes, ...second.likes, ...third.likes].map(
        (like: any) => like.creator,
      ),
    ).toEqual(expect.arrayContaining(['did:plc:bob', 'did:plc:carol']))
    expect(
      [...first.likes, ...second.likes, ...third.likes].map(
        (like: any) => like.creator,
      ),
    ).not.toEqual(expect.arrayContaining(['did:plc:dan', 'did:plc:eve']))
  })

  it('reads space quotes with exact-space filtering and stable pagination', async () => {
    const subject =
      'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:alice/app.bsky.feed.post/root'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const otherSpaceUri =
      'at://did:plc:tenant/space/community.blacksky.feed/other'
    const timestamp = '2026-08-18T00:00:00.000Z'
    const route = communityRoutes(db, undefined) as any
    const quoteRows = [
      ['one', spaceUri],
      ['two', spaceUri],
      ['other', otherSpaceUri],
      ['public', null],
    ] as const

    for (const [rkey, rowSpaceUri] of quoteRows) {
      const uri = rowSpaceUri
        ? `${rowSpaceUri}/did:plc:quoting/app.bsky.feed.post/${rkey}`
        : `at://did:plc:quoting/community.blacksky.feed.post/${rkey}`
      await insertPost(uri, rowSpaceUri, timestamp, `bafyquote${rkey}`)
      await db.pool.query(
        `UPDATE community_post SET embed = $1::jsonb WHERE uri = $2`,
        [JSON.stringify({ record: { uri: subject, cid: 'bafysubject' } }), uri],
      )
    }

    const first = await route.getSpacePostQuotes({
      subject: { uri: subject },
      limit: 1,
      cursor: '',
    })
    const second = await route.getSpacePostQuotes({
      subject: { uri: subject },
      limit: 1,
      cursor: first.cursor,
    })
    expect(first.posts).toHaveLength(1)
    expect(second.posts).toHaveLength(1)
    expect(second.cursor).toBeUndefined()
    expect(
      [...first.posts, ...second.posts].map((post: any) => post.spaceUri),
    ).toEqual([spaceUri, spaceUri])
  })

  it('keeps legacy readers on their existing public-only behavior', async () => {
    const legacySubject =
      'at://did:plc:alice/community.blacksky.feed.post/legacy-subject'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const routes = communityRoutes(db, undefined) as any
    const likes = likeRoutes(db) as any

    await db.pool.query(
      `INSERT INTO "like"
        (uri, cid, creator, subject, "subjectCid", "createdAt", "indexedAt", space_uri)
       VALUES
        ('at://did:plc:bob/app.bsky.feed.like/public', 'bafypublic', 'did:plc:bob', $1, 'bafysubject', now()::text, now()::text, NULL),
        ('${spaceUri}/did:plc:carol/app.bsky.feed.like/private', 'bafyprivate', 'did:plc:carol', $1, 'bafysubject', now()::text, now()::text, '${spaceUri}')`,
      [legacySubject],
    )
    await insertPost(
      'at://did:plc:quoting/community.blacksky.feed.post/public-quote',
      null,
    )
    await insertPost(
      `${spaceUri}/did:plc:quoting/app.bsky.feed.post/private-quote`,
      spaceUri,
    )
    await db.pool.query(
      `UPDATE community_post SET embed = $1::jsonb
       WHERE uri IN ($2, $3)`,
      [
        JSON.stringify({ record: { uri: legacySubject } }),
        'at://did:plc:quoting/community.blacksky.feed.post/public-quote',
        `${spaceUri}/did:plc:quoting/app.bsky.feed.post/private-quote`,
      ],
    )

    await expect(
      likes.getLikesBySubjectSorted({
        subject: { uri: legacySubject },
        limit: 10,
        cursor: '',
      }),
    ).resolves.toMatchObject({
      uris: ['at://did:plc:bob/app.bsky.feed.like/public'],
    })
    await expect(
      routes.getCommunityPostQuotes({
        uri: legacySubject,
        limit: 10,
        cursor: '',
      }),
    ).resolves.toMatchObject({
      posts: [
        {
          uri: 'at://did:plc:quoting/community.blacksky.feed.post/public-quote',
        },
      ],
    })
  })

  it('writes projected notifications only for allowed recipients', async () => {
    const alice = 'did:plc:alice'
    const bob = 'did:plc:bob'
    const mallory = 'did:plc:mallory'
    const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const root = `${spaceUri}/${alice}/app.bsky.feed.post/root`
    const reply = `${spaceUri}/${bob}/app.bsky.feed.post/reply`
    const routes = communityRoutes(db, undefined) as any

    await routes.projectCommunityRecord({
      collection: 'app.bsky.feed.post',
      operation: 'create',
      uri: reply,
      cid: 'bafyreply',
      author: bob,
      spaceUri,
      revision: '1',
      actionUri: '',
      allowedNotificationDids: [alice],
      recordJson: JSON.stringify({
        text: 'reply with mentions',
        createdAt: new Date().toISOString(),
        reply: {
          root: { uri: root, cid: 'bafyroot' },
          parent: { uri: root, cid: 'bafyroot' },
        },
        facets: [
          {
            index: { byteStart: 0, byteEnd: 7 },
            features: [
              { $type: 'app.bsky.richtext.facet#mention', did: mallory },
            ],
          },
        ],
      }),
    })

    const notifications = await db.db
      .selectFrom('notification')
      .select(['did', 'reason', 'recordUri'])
      .where('recordUri', '=', reply)
      .execute()
    expect(notifications).toEqual([
      { did: alice, reason: 'reply', recordUri: reply },
    ])
  })

  it('recounts unread space notifications from the current decisions', async () => {
    const viewer = 'did:plc:notification-viewer'
    const firstSpace = 'at://did:plc:tenant/space/community.blacksky.feed/first'
    const secondSpace =
      'at://did:plc:tenant/space/community.blacksky.feed/second'
    const firstPost = `${firstSpace}/did:plc:alice/app.bsky.feed.post/first`
    const secondPost = `${secondSpace}/did:plc:alice/app.bsky.feed.post/second`
    const now = new Date().toISOString()
    await db.db
      .insertInto('actor')
      .values({
        did: viewer,
        handle: null,
        indexedAt: now,
        takedownRef: null,
        upstreamStatus: null,
        ageAssuranceStatus: null,
        ageAssuranceLastInitiatedAt: null,
        ageAssuranceAccess: null,
        ageAssuranceCountryCode: null,
        ageAssuranceRegionCode: null,
      })
      .onConflict((conflict) => conflict.doNothing())
      .execute()
    await insertPost(firstPost, firstSpace)
    await insertPost(secondPost, secondSpace)
    await db.db
      .insertInto('notification')
      .values([
        {
          did: viewer,
          recordUri: firstPost,
          recordCid: 'bafyfirst',
          author: 'did:plc:alice',
          reason: 'mention',
          reasonSubject: null,
          sortAt: now,
        },
        {
          did: viewer,
          recordUri: secondPost,
          recordCid: 'bafysecond',
          author: 'did:plc:alice',
          reason: 'mention',
          reasonSubject: null,
          sortAt: now,
        },
      ])
      .execute()
    const routes = notificationRoutes(db) as any

    await expect(
      routes.getUnreadNotificationCount({
        actorDid: viewer,
        priority: false,
        allowedSpaceUris: [firstSpace, secondSpace],
      }),
    ).resolves.toEqual({ count: 2 })
    await expect(
      routes.getUnreadNotificationCount({
        actorDid: viewer,
        priority: false,
        allowedSpaceUris: [firstSpace],
      }),
    ).resolves.toEqual({ count: 1 })
    await expect(
      routes.getUnreadNotificationCount({
        actorDid: viewer,
        priority: false,
        allowedSpaceUris: [],
      }),
    ).resolves.toEqual({ count: 0 })
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

    it('applies a projected flag and unflag only inside the asserted space', async () => {
      const spaceUri =
        'at://did:plc:tenant/space/community.blacksky.feed/private'
      const otherSpace =
        'at://did:plc:tenant/space/community.blacksky.feed/other'
      const uri = `${spaceUri}/did:plc:alice/app.bsky.feed.post/target`
      const actionUri =
        'at://did:plc:tenant/community.blacksky.moderation.action/3kact'
      await insertPost(uri, spaceUri)

      const r = routes()
      const moderation = (operation: string, space: string) =>
        r.projectCommunityRecord({
          spaceUri: space,
          author: 'did:plc:alice',
          uri: actionUri,
          cid: 'bafytest',
          revision: '3krev',
          operation,
          collection: 'community.blacksky.moderation.action',
          recordJson: JSON.stringify({ subject: { uri } }),
          actionUri,
          allowedNotificationDids: [],
        })
      const flaggedAt = async () =>
        (
          await db.pool.query(
            'SELECT moderation_flagged_at FROM community_post WHERE uri = $1',
            [uri],
          )
        ).rows[0].moderation_flagged_at

      // A flag asserting the wrong space touches nothing.
      await moderation('flag', otherSpace)
      expect(await flaggedAt()).toBeNull()

      await moderation('flag', spaceUri)
      expect(await flaggedAt()).not.toBeNull()

      // An unflag asserting the wrong space cannot lift it.
      await moderation('unflag', otherSpace)
      expect(await flaggedAt()).not.toBeNull()

      await moderation('unflag', spaceUri)
      expect(await flaggedAt()).toBeNull()
    })
  })
})
