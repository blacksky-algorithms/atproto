import { beforeEach, describe, expect, it, vi } from 'vitest'

import getCommunityPostRoute from '../../src/api/community/blacksky/feed/getCommunityPost.js'
import { resetSpaceCredentials } from '../../src/api/community/blacksky/space-credential.js'
import { clearTenantGateCaches } from '../../src/api/community/blacksky/tenant-gate.js'

const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const POST = `${SPACE}/did:plc:alice/app.bsky.feed.post/3kpost`
const VIEWER = 'did:plc:viewer'
const MANAGING_APP_DID = 'did:web:feeds.example.com'

const post = () => ({
  uri: POST,
  cid: 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu',
  creator: 'did:plc:alice',
  text: 'private',
  createdAt: '2026-08-18T00:00:00.000Z',
  indexedAt: '2026-08-18T00:00:00.000Z',
  spaceUri: SPACE,
})

const mockNetwork = () => {
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
        return new Response(JSON.stringify({ allowed: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        })
      }
      throw new Error(`unexpected fetch: ${url}`)
    }),
  )
  return calls
}

const makeCtx = () => ({
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
    hydrateProfilesBasic: async () => ({}),
    label: { getLabelsForSubjects: async () => ({ getBySubject: () => [] }) },
  },
  views: {
    profileBasic: () => ({ did: 'did:plc:alice', handle: 'alice.test' }),
    imgUriBuilder: { getPresetUri: () => '' },
    videoUriBuilder: { playlist: () => '', thumbnail: () => '' },
  },
  dataplane: {
    getCommunityPost: vi.fn(async () => ({ post: post() })),
    getCommunityPostReplyCount: async () => ({ count: 0 }),
    getCommunityPostLikeCount: async () => ({ count: 0 }),
    getCommunityPostQuoteCount: async () => ({ count: 0 }),
    getCommunityPostViewerLike: async () => ({ likeUri: '' }),
    checkCommunityMembership: vi.fn(async () => ({ isMember: false })),
  },
  authVerifier: { standard: {} },
})

const registerHandler = (ctx: any) => {
  let captured: any
  getCommunityPostRoute(
    { add: (_lex: unknown, config: any) => (captured = config) } as any,
    ctx,
  )
  return () =>
    captured.handler({
      params: { uri: POST },
      auth: { credentials: { iss: VIEWER } },
      req: { headers: {} },
    })
}

describe('getCommunityPost', () => {
  beforeEach(() => {
    clearTenantGateCaches()
    resetSpaceCredentials()
    vi.stubEnv('COMMUNITY_SPACE_MINT_TOKEN', 'test-mint-token')
    vi.unstubAllGlobals()
  })

  it('authorizes a space parent through the space gate, not legacy membership', async () => {
    const calls = mockNetwork()
    const ctx = makeCtx()

    const res = await registerHandler(ctx)()

    expect(res.body.post.$type).toBe(
      'community.blacksky.feed.defs#spacePostView',
    )
    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
    expect(ctx.dataplane.getCommunityPost).toHaveBeenCalledWith({
      uri: POST,
      allowedSpaceUris: [SPACE],
    })
    expect(calls.filter((url) => url.includes('checkAccess'))).toHaveLength(1)
  })
})
