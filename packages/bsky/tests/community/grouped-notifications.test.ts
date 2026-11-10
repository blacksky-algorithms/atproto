import { once } from 'node:events'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { Timestamp } from '@bufbuild/protobuf'
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from 'vitest'
import {
  RecordRef,
  type SeedClient,
  TestNetwork,
  usersSeed,
} from '@atproto/dev-env'
import type { DidString } from '@atproto/lex'
import { NOTIFICATION_REASON } from '../../src/api/app/bsky/notification/constants.js'
import { app, community } from '../../src/lexicons/index.js'
import {
  GetNotificationsV2Response,
  Notification,
} from '../../src/proto/bsky_pb.js'

/**
 * The grouped (Notifications v2) read through the community.blacksky variant:
 * the authorized union of public notifications and the spaces the viewer may
 * read, with space posts carried as spacePostView and authors derived from the
 * record rather than the space authority.
 */

const MANAGING_APP_DID = 'did:web:feeds.example.com'
const MANAGING_APP = `${MANAGING_APP_DID}#bsky_fg`
const TENANT = 'did:plc:tenant'
const CHECK_ACCESS = 'community.blacksky.space.checkAccess'

const standard = app.bsky.notification.getGroupedNotifications
const communityDefs = community.blacksky.notification.getGroupedNotifications

const spaceUri = (name: string) =>
  `at://${TENANT}/space/community.blacksky.feed/${name}`
const spacePostUri = (space: string, author: string, rkey: string) =>
  `${space}/${author}/app.bsky.feed.post/${rkey}`

describe('community grouped notifications', () => {
  let network: TestNetwork
  let sc: SeedClient
  let alice: DidString
  let bob: DidString
  let carol: DidString
  let liker: DidString
  let allowedSpaces: Set<string>
  let spaceIndex = 0
  // The managing app the gate asks for access decisions. A loopback server,
  // because the appview's own fetch is outside this test's module graph.
  let managingApp: http.Server

  beforeAll(async () => {
    network = await TestNetwork.create({
      dbPostgresSchema: 'bsky_community_grouped_notifications',
    })
    sc = network.getSeedClient()
    await usersSeed(sc)
    await sc.createAccount('liker', {
      email: 'liker@test.com',
      handle: 'liker.test',
      password: 'liker-pass',
    })
    alice = sc.dids.alice
    bob = sc.dids.bob
    carol = sc.dids.carol
    liker = sc.dids.liker
    await sc.follow(carol, alice)
    await network.processAll()

    managingApp = http.createServer((req, res) => {
      const url = new URL(req.url ?? '/', 'http://localhost')
      if (url.pathname !== `/xrpc/${CHECK_ACCESS}`) {
        res.writeHead(404).end()
        return
      }
      const space = url.searchParams.get('space') ?? ''
      res
        .writeHead(200, { 'content-type': 'application/json' })
        .end(JSON.stringify({ allowed: allowedSpaces.has(space) }))
    })
    managingApp.listen(0)
    await once(managingApp, 'listening')
    const { port } = managingApp.address() as AddressInfo

    vi.stubEnv('COMMUNITY_SPACE_MANAGING_APP', MANAGING_APP)
    const resolver = network.bsky.ctx.idResolver.did
    const resolve = resolver.resolve.bind(resolver)
    vi.spyOn(resolver, 'resolve').mockImplementation(async (did, ...rest) =>
      did === MANAGING_APP_DID
        ? ({
            id: MANAGING_APP_DID,
            service: [
              {
                id: '#bsky_fg',
                type: 'BskyFeedGenerator',
                serviceEndpoint: `http://localhost:${port}`,
              },
            ],
          } as never)
        : resolve(did, ...rest),
    )
  })

  afterAll(async () => {
    vi.unstubAllEnvs()
    managingApp?.close()
    await network?.close()
  })

  afterEach(async () => {
    await network.bsky.db.db.deleteFrom('community_post').execute()
  })

  // Access decisions are cached per space and viewer, so each case gets its
  // own spaces.
  const freshSpaces = () => {
    const allowed = spaceUri(`allowed-${spaceIndex}`)
    const denied = spaceUri(`denied-${spaceIndex}`)
    spaceIndex++
    allowedSpaces = new Set([allowed])
    return { allowed, denied }
  }

  const insertSpacePost = async (
    uri: string,
    space: string,
    author: DidString,
    text: string,
  ) => {
    const now = new Date().toISOString()
    await network.bsky.db.db
      .insertInto('community_post')
      .values({
        uri,
        cid: 'bafyreie5sviu7eeu3vf6n3lrlmqef2vdlwmgzd6zpolov56agdxtshdpda',
        rkey: uri.split('/').at(-1)!,
        creator: author,
        text,
        space_uri: space,
        createdAt: now,
        indexedAt: now,
      })
      .execute()
  }

  const raw = (
    uri: string,
    reason: string,
    reasonSubject: string | undefined,
    minutesAgo: number,
  ) =>
    new Notification({
      recipientDid: carol,
      uri,
      reason,
      ...(reasonSubject ? { reasonSubject } : {}),
      timestamp: Timestamp.fromDate(new Date(Date.now() - minutesAgo * 60_000)),
    })

  const mockRaw = (notifications: Notification[]) =>
    vi
      .spyOn(network.bsky.ctx.hydrator.dataplane, 'getNotificationsV2')
      .mockResolvedValue(new GetNotificationsV2Response({ notifications }))

  const readCommunity = async (viewer: DidString = carol) =>
    network.bsky
      .getClient()
      .call(
        communityDefs,
        {},
        { headers: await network.serviceHeaders(viewer, communityDefs.$lxm) },
      )

  const readStandard = async (viewer: DidString = carol) =>
    network.bsky
      .getClient()
      .call(
        standard,
        {},
        { headers: await network.serviceHeaders(viewer, standard.$lxm) },
      )

  it('returns the authorized union with space posts as spacePostView, and keeps the standard read public', async () => {
    const { allowed, denied } = freshSpaces()
    const pub = await sc.post(carol, 'public post')
    const like = await sc.like(bob, pub.ref)
    await network.processAll()
    const allowedPost = spacePostUri(allowed, alice, 'mention-allowed')
    const deniedPost = spacePostUri(denied, alice, 'mention-denied')
    await insertSpacePost(allowedPost, allowed, alice, 'private mention')
    await insertSpacePost(deniedPost, denied, alice, 'hidden mention')
    using _notifications = mockRaw([
      raw(deniedPost, NOTIFICATION_REASON.MENTION, undefined, 1),
      raw(allowedPost, NOTIFICATION_REASON.MENTION, undefined, 2),
      raw(like.toString(), NOTIFICATION_REASON.LIKE, pub.ref.uriStr, 3),
    ])

    const union = await readCommunity()
    expect(union.groups).toHaveLength(2)
    const [mention, likeGroup] = union.groups
    expect(mention.kind).toMatchObject({
      $type: `${communityDefs.$lxm}#mentionNotification`,
      post: allowedPost,
    })
    expect(likeGroup.kind).toMatchObject({
      $type: `${communityDefs.$lxm}#likeGroup`,
      post: pub.ref.uriStr,
    })
    expect(JSON.stringify(union)).not.toContain(deniedPost)
    expect(union.relatedViews).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          $type: 'community.blacksky.feed.defs#spacePostView',
          uri: allowedPost,
          author: expect.objectContaining({ did: alice }),
        }),
        expect.objectContaining({
          $type: app.bsky.feed.defs.postView.$type,
          uri: pub.ref.uriStr,
        }),
        expect.objectContaining({ did: bob }),
      ]),
    )

    const publicOnly = await readStandard()
    expect(publicOnly.groups).toHaveLength(1)
    expect(publicOnly.groups[0].kind).toMatchObject({
      $type: `${standard.$lxm}#likeGroup`,
      post: pub.ref.uriStr,
    })
    expect(JSON.stringify(publicOnly)).not.toContain('/space/')
  })

  it('judges blocks and mutes by the author of a space record, not its tenant authority', async () => {
    const { allowed } = freshSpaces()
    const post = spacePostUri(allowed, alice, 'mention-author')
    await insertSpacePost(post, allowed, alice, 'mention from alice')
    using _notifications = mockRaw([
      raw(post, NOTIFICATION_REASON.MENTION, undefined, 1),
    ])

    expect((await readCommunity()).groups).toHaveLength(1)

    await sc.block(carol, alice)
    await network.processAll()
    expect((await readCommunity()).groups).toHaveLength(0)
    await sc.unblock(carol, alice)
    await network.processAll()
    expect((await readCommunity()).groups).toHaveLength(1)

    const agent = network.bsky.getAgent()
    const muteHeaders = {
      encoding: 'application/json' as const,
      headers: await network.serviceHeaders(carol, 'app.bsky.graph.muteActor'),
    }
    await agent.app.bsky.graph.muteActor({ actor: alice }, muteHeaders)
    expect((await readCommunity()).groups).toHaveLength(0)
    await agent.app.bsky.graph.unmuteActor(
      { actor: alice },
      {
        ...muteHeaders,
        headers: await network.serviceHeaders(
          carol,
          'app.bsky.graph.unmuteActor',
        ),
      },
    )
    expect((await readCommunity()).groups).toHaveLength(1)
  })

  it('drops a notification whose thread the viewer muted after it arrived', async () => {
    const root = await sc.post(carol, 'thread root')
    const like = await sc.like(bob, root.ref)
    const reply = await sc.reply(alice, root.ref, root.ref, 'reply in thread')
    await network.processAll()
    using _notifications = mockRaw([
      raw(like.toString(), NOTIFICATION_REASON.LIKE, root.ref.uriStr, 1),
      raw(reply.ref.uriStr, NOTIFICATION_REASON.REPLY, root.ref.uriStr, 2),
    ])
    expect((await readCommunity()).groups).toHaveLength(2)

    const agent = network.bsky.getAgent()
    await agent.app.bsky.graph.muteThread(
      { root: root.ref.uriStr },
      {
        encoding: 'application/json',
        headers: await network.serviceHeaders(
          carol,
          'app.bsky.graph.muteThread',
        ),
      },
    )
    expect((await readCommunity()).groups).toHaveLength(0)
    expect((await readStandard()).groups).toHaveLength(0)

    await agent.app.bsky.graph.unmuteThread(
      { root: root.ref.uriStr },
      {
        encoding: 'application/json',
        headers: await network.serviceHeaders(
          carol,
          'app.bsky.graph.unmuteThread',
        ),
      },
    )
    expect((await readCommunity()).groups).toHaveLength(2)
  })

  it('keeps a like-via-repost whose subject is a legacy community post', async () => {
    const stubUri = `at://${carol}/community.blacksky.feed.post/stub`
    const stubCid =
      'bafyreie5sviu7eeu3vf6n3lrlmqef2vdlwmgzd6zpolov56agdxtshdpda'
    const now = new Date().toISOString()
    await network.bsky.db.db
      .insertInto('community_post')
      .values({
        uri: stubUri,
        cid: stubCid,
        rkey: 'stub',
        creator: carol,
        text: 'community only post',
        createdAt: now,
        indexedAt: now,
      })
      .execute()
    const stub = new RecordRef(stubUri, stubCid)
    const repost = await sc.repost(bob, stub)
    const like = await sc.like(liker, stub)
    await network.processAll()
    using _notifications = mockRaw([
      new Notification({
        recipientDid: bob,
        uri: like.toString(),
        reason: NOTIFICATION_REASON.LIKE_VIA_REPOST,
        reasonSubject: repost.uriStr,
        timestamp: Timestamp.fromDate(new Date()),
      }),
    ])

    const union = await readCommunity(bob)
    expect(union.groups).toHaveLength(1)
    expect(union.groups[0].kind).toMatchObject({
      $type: `${communityDefs.$lxm}#likeViaRepostGroup`,
      post: stubUri,
      viaRepost: repost.uriStr,
    })
  })

  it('judges a via-repost notification by its own preference and the reposted thread', async () => {
    const post = await sc.post(carol, 'reposted by bob')
    const repost = await sc.repost(bob, post.ref)
    const like = await sc.like(liker, post.ref)
    await network.processAll()
    using _notifications = mockRaw([
      new Notification({
        recipientDid: bob,
        uri: like.toString(),
        reason: NOTIFICATION_REASON.LIKE_VIA_REPOST,
        reasonSubject: repost.uriStr,
        timestamp: Timestamp.fromDate(new Date()),
      }),
    ])
    const agent = network.bsky.getAgent()
    const putPrefs = async (
      prefs: Parameters<typeof agent.app.bsky.notification.putPreferencesV2>[0],
    ) => {
      await agent.app.bsky.notification.putPreferencesV2(prefs, {
        encoding: 'application/json',
        headers: await network.serviceHeaders(
          bob,
          'app.bsky.notification.putPreferencesV2',
        ),
      })
      await network.processAll()
    }

    expect((await readCommunity(bob)).groups).toHaveLength(1)

    await putPrefs({ follow: { include: 'all', list: false, push: true } })
    expect((await readCommunity(bob)).groups).toHaveLength(1)

    await putPrefs({
      follow: { include: 'all', list: true, push: true },
      likeViaRepost: { include: 'all', list: false, push: true },
    })
    expect((await readCommunity(bob)).groups).toHaveLength(0)

    await putPrefs({
      likeViaRepost: { include: 'all', list: true, push: true },
    })
    expect((await readCommunity(bob)).groups).toHaveLength(1)

    const muteHeaders = async (lxm: string) => ({
      encoding: 'application/json' as const,
      headers: await network.serviceHeaders(bob, lxm),
    })
    await agent.app.bsky.graph.muteThread(
      { root: post.ref.uriStr },
      await muteHeaders('app.bsky.graph.muteThread'),
    )
    expect((await readCommunity(bob)).groups).toHaveLength(0)
    await agent.app.bsky.graph.unmuteThread(
      { root: post.ref.uriStr },
      await muteHeaders('app.bsky.graph.unmuteThread'),
    )
    expect((await readCommunity(bob)).groups).toHaveLength(1)
  })

  it('honors per-reason list and follows-only preferences', async () => {
    const post = await sc.post(carol, 'preference subject')
    const like = await sc.like(liker, post.ref)
    const reply = await sc.reply(liker, post.ref, post.ref, 'reply from liker')
    const followBack = await sc.follow(alice, carol)
    await network.processAll()
    using _notifications = mockRaw([
      raw(like.toString(), NOTIFICATION_REASON.LIKE, post.ref.uriStr, 1),
      raw(reply.ref.uriStr, NOTIFICATION_REASON.REPLY, post.ref.uriStr, 2),
      raw(followBack.uriStr, NOTIFICATION_REASON.FOLLOW_BACK, undefined, 3),
      raw(
        `at://${alice}/app.bsky.actor.profile/self`,
        NOTIFICATION_REASON.CONTACT_MATCH,
        undefined,
        4,
      ),
    ])
    const agent = network.bsky.getAgent()
    const putPrefs = async (
      prefs: Parameters<typeof agent.app.bsky.notification.putPreferencesV2>[0],
    ) => {
      await agent.app.bsky.notification.putPreferencesV2(prefs, {
        encoding: 'application/json',
        headers: await network.serviceHeaders(
          carol,
          'app.bsky.notification.putPreferencesV2',
        ),
      })
      await network.processAll()
    }

    const followBackKind = `${communityDefs.$lxm}#followBackNotification`
    // contact matches have no preference of their own and always list
    const contactMatchKind = `${communityDefs.$lxm}#contactMatchNotification`
    expect((await readCommunity()).groups).toHaveLength(4)

    // carol does not follow liker; a follow-back is by definition a follow
    await putPrefs({ like: { include: 'follows', list: true, push: true } })
    expect(
      (await readCommunity()).groups.map((group) => group.kind.$type),
    ).toEqual([
      `${communityDefs.$lxm}#replyNotification`,
      followBackKind,
      contactMatchKind,
    ])

    await putPrefs({
      like: { include: 'all', list: true, push: true },
      reply: { include: 'all', list: false, push: true },
      follow: { include: 'all', list: false, push: true },
    })
    expect(
      (await readCommunity()).groups.map((group) => group.kind.$type),
    ).toEqual([`${communityDefs.$lxm}#likeGroup`, contactMatchKind])

    await putPrefs({
      like: { include: 'all', list: true, push: true },
      reply: { include: 'all', list: true, push: true },
      follow: { include: 'all', list: true, push: true },
    })
    expect((await readCommunity()).groups).toHaveLength(4)
  })
})
