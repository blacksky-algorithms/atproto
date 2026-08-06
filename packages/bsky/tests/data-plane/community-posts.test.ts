import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { TestNetwork } from '@atproto/dev-env'
import communityRoutes from '../../src/data-plane/server/routes/community.js'
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
  })

  afterAll(async () => {
    await network?.close()
  })

  const insertPost = async (uri: string, feedUri: string | null) => {
    await db.pool.query(
      `INSERT INTO community_post
        (uri, cid, rkey, creator, text, "createdAt", "indexedAt", feed_uri)
       VALUES ($1, 'bafytest', $2::varchar, 'did:plc:alice', $2::text, $3, $3, $4)`,
      [uri, uri.split('/').at(-1), new Date().toISOString(), feedUri],
    )
  }

  it('keeps tenant rows out of Blacksky timeline and actor feeds', async () => {
    const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
    const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    await insertPost(legacyUri, null)
    await insertPost(
      tenantUri,
      'at://did:plc:tenant/app.bsky.feed.generator/private',
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

    expect(timeline.posts.map((post: { uri: string }) => post.uri)).toEqual([
      legacyUri,
    ])
    expect(byActor.posts.map((post: { uri: string }) => post.uri)).toEqual([
      legacyUri,
    ])
  })

  it('returns the discriminator on per-post reads', async () => {
    const feedUri = 'at://did:plc:tenant/app.bsky.feed.generator/private'
    const uri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    await insertPost(uri, feedUri)

    const routes = communityRoutes(db, undefined) as any
    const result = await routes.getCommunityPost({ uri })
    const batch = await routes.getCommunityPosts({ uris: [uri] })

    expect(result.post.feedUri).toBe(feedUri)
    expect(batch.posts[0].feedUri).toBe(feedUri)
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

  it('stores NULL for legacy writes and a feed URI for tenant writes', async () => {
    const routes = communityRoutes(db, undefined) as any
    const createdAt = new Date().toISOString()
    const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
    const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
    const feedUri = 'at://did:plc:tenant/app.bsky.feed.generator/private'

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
      feedUri,
    })

    const rows = await db.db
      .selectFrom('community_post')
      .select(['uri', 'feed_uri'])
      .orderBy('uri')
      .execute()

    expect(rows).toEqual([
      { uri: legacyUri, feed_uri: null },
      { uri: tenantUri, feed_uri: feedUri },
    ])
  })
})
