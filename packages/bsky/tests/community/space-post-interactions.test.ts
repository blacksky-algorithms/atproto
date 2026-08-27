import { beforeEach, describe, expect, it, vi } from 'vitest'
import getLikes from '../../src/api/app/bsky/feed/getLikes.js'
import getQuotes from '../../src/api/app/bsky/feed/getQuotes.js'
import getSpacePostLikes from '../../src/api/community/blacksky/feed/getSpacePostLikes.js'
import getSpacePostQuotes from '../../src/api/community/blacksky/feed/getSpacePostQuotes.js'
import * as GetSpacePostLikes from '../../src/lexicons/community/blacksky/feed/getSpacePostLikes.defs.js'
import * as GetSpacePostQuotes from '../../src/lexicons/community/blacksky/feed/getSpacePostQuotes.defs.js'
import * as CommunityFeedDefs from '../../src/lexicons/community/blacksky/feed/defs.defs.js'
import { clearTenantGateCaches } from '../../src/api/community/blacksky/tenant-gate.js'
import { resetSpaceCredentials } from '../../src/api/community/blacksky/space-credential.js'

const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const OTHER_SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/other'
const POST = `${SPACE}/did:plc:alice/app.bsky.feed.post/root`
const LEGACY_POST = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
const VIEWER = 'did:plc:viewer'
const MANAGING_APP_DID = 'did:web:feeds.example.com'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })

const allowSpace = (allowed: boolean) => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: URL | string) => {
      const url = String(input)
      if (url.includes('/admin/mintCredential')) {
        const payload = Buffer.from(
          JSON.stringify({ exp: Math.floor(Date.now() / 1000) + 7200 }),
        ).toString('base64url')
        return json({ credential: `hdr.${payload}.sig` })
      }
      if (url.includes('com.atproto.space.getSpace')) {
        return json({
          config: { managingApp: `${MANAGING_APP_DID}#bsky_fg` },
        })
      }
      if (url.includes('community.blacksky.space.checkAccess')) {
        return json({ allowed })
      }
      throw new Error(`unexpected fetch: ${url}`)
    }),
  )
}

const makeCtx = (dataplane: Record<string, unknown>) => ({
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
            id: `#${did === 'did:plc:tenant' ? 'atproto_pds' : 'bsky_fg'}`,
            type: 'x',
            serviceEndpoint:
              did === 'did:plc:tenant'
                ? 'https://pds.example.com'
                : 'https://feeds.example.com',
          },
        ],
      }),
    },
  },
  reqLabelers: () => ({ dids: [], redact: new Set<string>() }),
  hydrator: {
    createContext: async (value: unknown) => value,
    hydrateProfiles: vi.fn(async () => ({})),
    hydrateBidirectionalBlocks: vi.fn(async () => new Map()),
    hydrateProfilesBasic: vi.fn(async () => ({})),
    label: {
      getLabelsForSubjects: vi.fn(async () => ({ getBySubject: () => [] })),
    },
  },
  views: {
    profile: (did: string) => ({
      did,
      handle: `${did.split(':').at(-1)}.test`,
    }),
    viewerBlockExists: () => false,
    profileBasic: (did: string) => ({
      did,
      handle: `${did.split(':').at(-1)}.test`,
      labels: [],
    }),
    imgUriBuilder: { getPresetUri: () => '' },
    videoUriBuilder: { playlist: () => '', thumbnail: () => '' },
  },
  dataplane,
  authVerifier: { standard: {} },
})

const register = (route: any, ctx: any) => {
  let config: any
  route(
    { add: (_lex: unknown, value: unknown) => (config = value) } as any,
    ctx,
  )
  return config.handler
}

const request = (handler: any, params: Record<string, unknown>) =>
  handler({
    params,
    auth: { credentials: { iss: VIEWER } },
    req: { headers: {} },
  })

describe('space post likes and quotes', () => {
  beforeEach(() => {
    clearTenantGateCaches()
    resetSpaceCredentials()
    vi.stubEnv('COMMUNITY_SPACE_MINT_TOKEN', 'test-mint-token')
    vi.unstubAllGlobals()
  })

  it('validates custom output shapes and keeps space URIs out of standard views', () => {
    const like = {
      actor: { did: 'did:plc:bob', handle: 'bob.test' },
      createdAt: '2026-08-18T00:00:00.000Z',
      indexedAt: '2026-08-18T00:00:01.000Z',
    }
    expect(
      GetSpacePostLikes.$output.schema?.safeParse({
        uri: POST,
        likes: [like],
      }).success,
    ).toBe(true)
    expect(
      GetSpacePostQuotes.$output.schema?.safeParse({
        uri: POST,
        posts: [],
      }).success,
    ).toBe(true)
    expect(
      CommunityFeedDefs.spacePostView.safeParse({
        $type: 'community.blacksky.feed.defs#spacePostView',
        uri: POST,
        cid: CID,
        author: { did: 'did:plc:bob', handle: 'bob.test' },
        record: { text: 'private' },
        indexedAt: '2026-08-18T00:00:00.000Z',
      }).success,
    ).toBe(true)
  })

  it('rejects non-space URIs before data-plane access', async () => {
    const getSpacePostLikesCall = vi.fn()
    const ctx = makeCtx({ getSpacePostLikes: getSpacePostLikesCall })
    const handler = register(getSpacePostLikes, ctx)

    await expect(
      request(handler, { uri: LEGACY_POST, limit: 10 }),
    ).rejects.toMatchObject({
      customErrorName: 'InvalidRequest',
    })
    expect(getSpacePostLikesCall).not.toHaveBeenCalled()
  })

  it('authorizes before data-plane access and returns actor timestamps', async () => {
    allowSpace(true)
    const getSpacePostLikesCall = vi.fn(async () => ({
      likes: [
        {
          uri: `${SPACE}/did:plc:bob/app.bsky.feed.like/one`,
          creator: 'did:plc:bob',
          createdAt: '2026-08-18T00:00:00.000Z',
          indexedAt: '2026-08-18T00:00:02.000Z',
        },
      ],
      cursor: '',
    }))
    const ctx = makeCtx({ getSpacePostLikes: getSpacePostLikesCall })
    const handler = register(getSpacePostLikes, ctx)

    const result = await request(handler, { uri: POST, limit: 10 })
    expect(getSpacePostLikesCall).toHaveBeenCalledWith({
      subject: { uri: POST, cid: undefined },
      limit: 10,
      cursor: undefined,
    })
    expect(result.body.likes).toEqual([
      {
        actor: { did: 'did:plc:bob', handle: 'bob.test' },
        createdAt: '2026-08-18T00:00:00.000Z',
        indexedAt: '2026-08-18T00:00:02.000Z',
      },
    ])
  })

  it('does not call the likes data plane for an unauthorized viewer', async () => {
    allowSpace(false)
    const getSpacePostLikesCall = vi.fn()
    const ctx = makeCtx({ getSpacePostLikes: getSpacePostLikesCall })
    const handler = register(getSpacePostLikes, ctx)

    await expect(request(handler, { uri: POST })).rejects.toMatchObject({
      customErrorName: 'MembershipRequired',
    })
    expect(getSpacePostLikesCall).not.toHaveBeenCalled()
  })

  it('omits likes by actors blocked by the post author or viewer', async () => {
    allowSpace(true)
    const ctx = makeCtx({
      getSpacePostLikes: vi.fn(async () => ({
        likes: [
          {
            uri: 'like-bob',
            creator: 'did:plc:bob',
            createdAt: '2026-08-18T00:00:00Z',
            indexedAt: '2026-08-18T00:00:00Z',
          },
          {
            uri: 'like-carol',
            creator: 'did:plc:carol',
            createdAt: '2026-08-18T00:00:01Z',
            indexedAt: '2026-08-18T00:00:01Z',
          },
        ],
        cursor: '',
      })),
    })
    ctx.hydrator.hydrateBidirectionalBlocks = vi.fn(
      async () =>
        new Map([['did:plc:alice', new Map([['did:plc:bob', true]])]]),
    )
    ;(ctx as any).views.viewerBlockExists = (did: string) =>
      did === 'did:plc:carol'
    const handler = register(getSpacePostLikes, ctx)

    await expect(request(handler, { uri: POST })).resolves.toMatchObject({
      body: { likes: [] },
    })
  })

  it('returns only same-space quotes as custom post views and applies block/mute filters', async () => {
    allowSpace(true)
    const sameSpace = (creator: string, rkey: string) => ({
      uri: `${SPACE}/${creator}/app.bsky.feed.post/${rkey}`,
      cid: CID,
      creator,
      text: 'quoted',
      createdAt: '2026-08-18T00:00:00.000Z',
      indexedAt: '2026-08-18T00:00:00.000Z',
      spaceUri: SPACE,
    })
    const getSpacePostQuotesCall = vi.fn(async () => ({
      posts: [
        sameSpace('did:plc:bob', 'allowed'),
        { ...sameSpace('did:plc:carol', 'other-space'), spaceUri: OTHER_SPACE },
        sameSpace('did:plc:carol', 'blocked'),
        sameSpace('did:plc:dan', 'muted'),
      ],
      cursor: '',
    }))
    const ctx = makeCtx({ getSpacePostQuotes: getSpacePostQuotesCall })
    ctx.views.profileBasic = (did: string) => ({
      did,
      handle: `${did.split(':').at(-1)}.test`,
      labels: [],
      ...(did === 'did:plc:carol' ? { viewer: { blocking: 'block' } } : {}),
      ...(did === 'did:plc:dan' ? { viewer: { muted: true } } : {}),
    })
    ctx.dataplane.getCommunityPostReplyCount = vi.fn(async () => ({ count: 0 }))
    ctx.dataplane.getCommunityPostLikeCount = vi.fn(async () => ({ count: 0 }))
    ctx.dataplane.getCommunityPostQuoteCount = vi.fn(async () => ({ count: 0 }))
    ctx.dataplane.getCommunityPostViewerLike = vi.fn(async () => ({
      likeUri: '',
    }))
    const handler = register(getSpacePostQuotes as any, ctx)

    const result = await request(handler, { uri: POST })
    expect(getSpacePostQuotesCall).toHaveBeenCalledOnce()
    expect(result.body.posts).toHaveLength(1)
    expect(result.body.posts[0].$type).toBe(
      'community.blacksky.feed.defs#spacePostView',
    )
    expect(
      GetSpacePostQuotes.$output.schema?.safeParse(result.body).success,
    ).toBe(true)
  })

  it('does not call the quotes data plane for an unauthorized viewer', async () => {
    allowSpace(false)
    const getSpacePostQuotesCall = vi.fn()
    const ctx = makeCtx({ getSpacePostQuotes: getSpacePostQuotesCall })
    const handler = register(getSpacePostQuotes as any, ctx)

    await expect(request(handler, { uri: POST })).rejects.toMatchObject({
      customErrorName: 'MembershipRequired',
    })
    expect(getSpacePostQuotesCall).not.toHaveBeenCalled()
  })

  it('rejects non-space quote URIs before data-plane access', async () => {
    const getSpacePostQuotesCall = vi.fn()
    const ctx = makeCtx({ getSpacePostQuotes: getSpacePostQuotesCall })
    const handler = register(getSpacePostQuotes as any, ctx)

    await expect(request(handler, { uri: LEGACY_POST })).rejects.toMatchObject({
      customErrorName: 'InvalidRequest',
    })
    expect(getSpacePostQuotesCall).not.toHaveBeenCalled()
  })

  it('keeps standard likes and quotes on their legacy community routes', async () => {
    const legacyLikeUri =
      'at://did:plc:bob/app.bsky.feed.like/legacy-community-like'
    const legacyQuotes = vi.fn(async () => ({ posts: [], cursor: '' }))
    const legacyLikes = vi.fn(async () => ({
      uris: [legacyLikeUri],
      cursor: '',
    }))
    const ctx = makeCtx({
      checkCommunityMembership: vi.fn(async () => ({ isMember: true })),
      getCommunityPostQuotes: legacyQuotes,
    }) as any
    ctx.authVerifier.parseCreds = () => ({
      viewer: VIEWER,
      includeTakedowns: false,
      skipViewerBlocks: false,
    })
    ctx.hydrator.dataplane = { getLikesBySubjectSorted: legacyLikes }
    ctx.hydrator.hydrateLikes = vi.fn(async () => ({
      likes: new Map([
        [
          legacyLikeUri,
          {
            record: { createdAt: '2026-08-18T00:00:00.000Z' },
            sortedAt: new Date('2026-08-18T00:00:01.000Z'),
          },
        ],
      ]),
    }))

    const likesResult = await request(register(getLikes, ctx), {
      uri: LEGACY_POST,
    })
    const quotesResult = await request(register(getQuotes, ctx), {
      uri: LEGACY_POST,
    })

    expect(likesResult.body.likes[0].actor.did).toBe('did:plc:bob')
    expect(legacyLikes).toHaveBeenCalledWith({
      subject: { uri: LEGACY_POST, cid: undefined },
      cursor: undefined,
      limit: undefined,
    })
    expect(quotesResult.body.posts).toEqual([])
    expect(legacyQuotes).toHaveBeenCalledWith({
      uri: LEGACY_POST,
      limit: undefined,
      cursor: undefined,
    })
  })
})
