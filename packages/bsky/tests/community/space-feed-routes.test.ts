import { beforeEach, describe, expect, it, vi } from 'vitest'
import getCommunityTimelineRoute from '../../src/api/community/blacksky/feed/getCommunityTimeline.js'
import getSpaceFeedRoute from '../../src/api/community/blacksky/feed/getSpaceFeed.js'
import { skeletonFromFeedGen } from '../../src/api/app/bsky/feed/getFeed.js'
import { resetSpaceCredentials } from '../../src/api/community/blacksky/space-credential.js'
import { clearTenantGateCaches } from '../../src/api/community/blacksky/tenant-gate.js'

const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const FEED = 'at://did:plc:tenant/app.bsky.feed.generator/private'
const PUBLIC_FEED = 'at://did:plc:tenant/app.bsky.feed.generator/public'
const VIEWER = 'did:plc:viewer'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

const AUTHORITY = 'did:plc:tenant'
const MANAGING_APP_DID = 'did:web:feeds.example.com'

const config = (space?: string) =>
  JSON.stringify({
    $type: 'community.blacksky.feed.config',
    contentType: 'communityRecord',
    visibility: 'gated',
    authorization: { serviceDid: MANAGING_APP_DID },
    group: 'at://did:plc:tenant/app.bsky.graph.list/members',
    createdAt: '2026-08-18T00:00:00.000Z',
    ...(space ? { space } : {}),
  })

const row = (rkey: string) => ({
  uri: `${SPACE}/did:plc:alice/app.bsky.feed.post/${rkey}`,
  cid: CID,
  creator: 'did:plc:alice',
  text: 'private',
  createdAt: '2026-08-18T00:00:00.000Z',
  indexedAt: '2026-08-18T00:00:00.000Z',
  spaceUri: SPACE,
})

/**
 * The delegated access check is a live HTTP call to the space host and then to
 * the managing app. Counting the fetches is the point of one of these tests:
 * the per-request decision must not fan out per post.
 */
const mockNetwork = (allowed: boolean) => {
  const calls: string[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: URL | string) => {
      const url = String(input)
      calls.push(url)
      if (url.includes('/admin/mintCredential')) {
        const payload = Buffer.from(
          JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 7200 }),
        ).toString('base64url')
        return new Response(
          JSON.stringify({ credential: `hdr.${payload}.sig` }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }
      if (url.includes('com.atproto.space.getSpace')) {
        return new Response(
          JSON.stringify({
            config: { managingApp: `${MANAGING_APP_DID}#bsky_fg` },
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }
      if (url.includes('community.blacksky.space.checkAccess')) {
        return new Response(JSON.stringify({ allowed }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        })
      }
      throw new Error(`unexpected fetch: ${url}`)
    }),
  )
  return calls
}

const makeCtx = (posts: any[], space = SPACE) => ({
  cfg: { serverDid: 'did:web:appview.test' },
  signingKey: {
    did: () => 'did:key:zQ3s',
    jwtAlg: 'ES256K',
    sign: async () => new Uint8Array(64),
  },
  idResolver: {
    did: {
      resolve: async (did: string) => ({
        id: did,
        service: [
          {
            id: `#${did === AUTHORITY ? 'atproto_pds' : 'bsky_fg'}`,
            type: 'x',
            serviceEndpoint:
              did === AUTHORITY
                ? 'https://pds.example.com'
                : 'https://feeds.example.com',
          },
        ],
      }),
    },
  },
  reqLabelers: () => ({ dids: [], redact: new Set<string>() }),
  hydrator: {
    createContext: async (v: any) => v,
    hydrateProfilesBasic: async () => ({}),
    label: { getLabelsForSubjects: async () => ({ getBySubject: () => [] }) },
  },
  views: {
    profileBasic: () => ({ did: 'did:plc:alice', handle: 'alice.test' }),
    imgUriBuilder: { getPresetUri: () => '' },
    videoUriBuilder: { playlist: () => '', thumbnail: () => '' },
  },
  dataplane: {
    getCommunityFeedConfig: vi.fn(async ({ feedUri }: any) => ({
      configJson: feedUri === PUBLIC_FEED ? config() : config(space),
    })),
    getCommunityFeedBySpace: vi.fn(async () => ({ posts, cursor: '' })),
    getCommunityPost: vi.fn(async () => ({ post: undefined })),
    getCommunityPostReplyCount: async () => ({ count: 0 }),
    getCommunityPostLikeCount: async () => ({ count: 0 }),
    getCommunityPostQuoteCount: async () => ({ count: 0 }),
    getCommunityPostViewerLike: async () => ({ likeUri: '' }),
    checkCommunityMembership: async () => ({ isMember: false }),
  },
  authVerifier: { standard: {} },
})

const registerHandler = (ctx: any) => {
  let captured: any
  getSpaceFeedRoute(
    { add: (_lex: unknown, cfg: any) => (captured = cfg) } as any,
    ctx,
  )
  return (params: any, viewer: string | null = VIEWER) =>
    captured.handler({
      params,
      auth: { credentials: { iss: viewer } },
      req: { headers: {} },
    })
}

describe('getSpaceFeed', () => {
  beforeEach(() => {
    clearTenantGateCaches()
    resetSpaceCredentials()
    vi.stubEnv('COMMUNITY_SPACE_MINT_TOKEN', 'test-mint-token')
    vi.unstubAllGlobals()
  })

  it('serves a member a page, deciding access once for the whole page', async () => {
    const calls = mockNetwork(true)
    const ctx = makeCtx([row('a'), row('b'), row('c')])
    const res = await registerHandler(ctx)({ feed: FEED, limit: 30 })

    expect(res.body.feed).toHaveLength(3)
    expect(res.body.feed[0].$type).toBeUndefined()
    expect(res.body.feed[0].post.$type).toBe(
      'community.blacksky.feed.defs#spacePostView',
    )
    // One getSpace + one checkAccess, not one pair per post. Three posts on a
    // 5s-timeout delegated check is the difference between a page and a stall.
    expect(calls.filter((c) => c.includes('checkAccess'))).toHaveLength(1)
  })

  it('refuses a non-member with an error, never an empty page', async () => {
    mockNetwork(false)
    const ctx = makeCtx([row('a')])
    await expect(
      registerHandler(ctx)({ feed: FEED, limit: 30 }),
    ).rejects.toMatchObject({ customErrorName: 'MembershipRequired' })
    // No rows, no cursor, and no existence signal: the list was never queried.
    expect(ctx.dataplane.getCommunityFeedBySpace).not.toHaveBeenCalled()
  })

  it('refuses a signed-out viewer without reaching the space host', async () => {
    const calls = mockNetwork(true)
    const ctx = makeCtx([row('a')])
    await expect(
      registerHandler(ctx)({ feed: FEED, limit: 30 }, null),
    ).rejects.toMatchObject({ customErrorName: 'MembershipRequired' })
    expect(calls).toHaveLength(0)
  })

  it('sends a feed with no space back to the standard route', async () => {
    mockNetwork(true)
    const ctx = makeCtx([])
    await expect(
      registerHandler(ctx)({ feed: PUBLIC_FEED, limit: 30 }),
    ).rejects.toMatchObject({ customErrorName: 'NotSpaceBacked' })
  })

  it('fails closed when the space host is unreachable', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new Error('connection refused')
      }),
    )
    const ctx = makeCtx([row('a')])
    await expect(
      registerHandler(ctx)({ feed: FEED, limit: 30 }),
    ).rejects.toMatchObject({ customErrorName: 'MembershipRequired' })
  })
})

describe('app.bsky.feed.getFeed', () => {
  beforeEach(() => {
    clearTenantGateCaches()
    vi.unstubAllGlobals()
  })

  it('refuses a space-backed feed before it calls the feed generator', async () => {
    const calls = mockNetwork(true)
    const ctx = makeCtx([])
    ;(ctx as any).hydrator.feed = {
      getFeedGens: vi.fn(async () => {
        throw new Error('the feed generator must not be reached')
      }),
    }

    await expect(
      skeletonFromFeedGen(ctx as any, { feed: FEED, headers: {} } as any),
    ).rejects.toMatchObject({ customErrorName: 'SpaceBackedFeed' })
    expect((ctx as any).hydrator.feed.getFeedGens).not.toHaveBeenCalled()
    expect(calls).toHaveLength(0)
  })

  it('leaves an ordinary custom feed on the standard path', async () => {
    mockNetwork(true)
    const ctx = makeCtx([])
    ;(ctx as any).hydrator.feed = {
      getFeedGens: vi.fn(async () => new Map()),
    }

    // Reaches the generator lookup and fails there, not at the space refusal.
    await expect(
      skeletonFromFeedGen(
        ctx as any,
        { feed: PUBLIC_FEED, headers: {} } as any,
      ),
    ).rejects.toThrow('could not find feed')
  })
})

describe('getCommunityTimeline legacy compatibility', () => {
  beforeEach(() => {
    clearTenantGateCaches()
    resetSpaceCredentials()
    vi.stubEnv('COMMUNITY_SPACE_MINT_TOKEN', 'test-mint-token')
    vi.unstubAllGlobals()
  })

  const legacyRow = () => ({
    uri: 'at://did:plc:alice/community.blacksky.feed.post/3klegacy',
    cid: CID,
    creator: 'did:plc:alice',
    text: 'legacy',
    createdAt: '2026-08-18T00:00:00.000Z',
    indexedAt: '2026-08-18T00:00:00.000Z',
    spaceUri: '',
  })

  const registerTimeline = (ctx: any) => {
    let captured: any
    getCommunityTimelineRoute(
      { add: (_lex: unknown, cfg: any) => (captured = cfg) } as any,
      ctx,
    )
    return (params: any, viewer: string | null = VIEWER) =>
      captured.handler({
        params,
        auth: { credentials: { iss: viewer } },
        req: { headers: {} },
      })
  }

  it('keeps legacy posts as postView and retags only space posts', async () => {
    // A client released before spaces guards on the standard $type
    // (AppBskyFeedDefs.isPostView); a retagged legacy post would stop
    // rendering there. Space posts carry the space view types.
    mockNetwork(true)
    const ctx: any = makeCtx([])
    ctx.dataplane.checkCommunityMembership = async () => ({ isMember: true })
    ctx.dataplane.getCommunityTimeline = async () => ({
      posts: [legacyRow(), row('a')],
      cursor: '',
    })
    const res = await registerTimeline(ctx)({ limit: 30 })

    expect(res.body.feed).toHaveLength(2)
    expect(res.body.feed[0].post.$type).toBe('app.bsky.feed.defs#postView')
    expect(res.body.feed[1].post.$type).toBe(
      'community.blacksky.feed.defs#spacePostView',
    )
  })
})
