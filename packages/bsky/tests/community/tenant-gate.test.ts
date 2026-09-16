import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Secp256k1Keypair } from '@atproto/crypto'
import { assertCommunityMembershipForUris } from '../../src/api/community/blacksky/membership-guard.js'
import {
  canContributeToSpace,
  canViewCommunityPost,
  canViewSpace,
  clearTenantGateCaches,
} from '../../src/api/community/blacksky/tenant-gate.js'

const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const postUri = `${spaceUri}/did:plc:alice/app.bsky.feed.post/3kpost`
const stubPostUri = 'at://did:plc:alice/community.blacksky.feed.post/3kstub'
const viewer = 'did:plc:viewer'
const managingAppDid = 'did:web:feeds.example.com'
const managingApp = `${managingAppDid}#bsky_fg`
const managingAppUrl = 'https://feeds.example.com'

const CHECK_ACCESS = 'community.blacksky.space.checkAccess'
const MINT_PATH = '/admin/mintCredential'
const GET_SPACE = 'com.atproto.space.getSpace'

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })

const callsTo = (fetchMock: any, needle: string) =>
  fetchMock.mock.calls.filter(([url]: any) => String(url).includes(needle))

const claimsOf = (init: any) => {
  const token = init.headers.authorization.slice('Bearer '.length)
  return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString())
}

describe('community post tenant gate', () => {
  let keypair: Secp256k1Keypair
  let ctx: any

  /**
   * Answers `checkAccess` on the configured managing app, so a test only has to
   * say what the managing app decides. Access discovery is config, not network:
   * a well-behaved run touches nothing but this endpoint.
   */
  const stubNetwork = (opts: {
    allowed?: boolean
    checkAccess?: () => Response | Promise<Response>
  }) => {
    const fetchMock = vi.fn(async (url: any, _init?: RequestInit) => {
      const href = String(url)
      if (href.includes(CHECK_ACCESS)) {
        return opts.checkAccess
          ? await opts.checkAccess()
          : json({ allowed: opts.allowed ?? true })
      }
      throw new Error(`unexpected fetch: ${href}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    return fetchMock
  }

  beforeEach(async () => {
    clearTenantGateCaches()
    vi.stubEnv('COMMUNITY_SPACE_MANAGING_APP', managingApp)
    keypair = await Secp256k1Keypair.create()
    ctx = {
      cfg: { serverDid: 'did:web:api.blacksky.community' },
      signingKey: keypair,
      dataplane: {
        checkCommunityMembership: vi.fn(),
        getCommunityPosts: vi.fn(),
        getCommunityFeedConfig: vi.fn(),
      },
      idResolver: {
        did: {
          // The managing app names its own endpoint. Nothing else is resolved:
          // who decides is configured, not discovered from the space.
          resolve: vi.fn(async (did: string) => {
            if (did === managingAppDid) {
              return {
                id: managingAppDid,
                service: [
                  {
                    id: `${managingAppDid}#bsky_fg`,
                    type: 'BskyFeedGenerator',
                    serviceEndpoint: managingAppUrl,
                  },
                ],
              }
            }
            return null
          }),
        },
      },
    }
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    vi.unstubAllEnvs()
    delete process.env.COMMUNITY_POSTS_ENABLED
  })

  it('keeps the legacy membership branch unchanged', async () => {
    ctx.dataplane.checkCommunityMembership
      .mockResolvedValueOnce({ isMember: true })
      .mockResolvedValueOnce({ isMember: false })

    await expect(
      canViewCommunityPost(ctx, { uri: stubPostUri }, viewer),
    ).resolves.toBe(true)
    await expect(
      canViewCommunityPost(ctx, { uri: stubPostUri }, viewer),
    ).resolves.toBe(false)
  })

  it('lets an authorized member view a space, denies a non-member', async () => {
    let allowed = true
    stubNetwork({ checkAccess: () => json({ allowed }) })

    await expect(canViewSpace(ctx, spaceUri, viewer)).resolves.toBe(true)

    clearTenantGateCaches()
    allowed = false
    await expect(canViewSpace(ctx, spaceUri, viewer)).resolves.toBe(false)
  })

  it('denies an anonymous viewer without any network call', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(canViewSpace(ctx, spaceUri, null)).resolves.toBe(false)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, null),
    ).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('gates a space post on its space even when the row is missing', async () => {
    // The data plane filters moderation-flagged rows out of getCommunityPosts,
    // so the guard sees no row for one. Deriving the space from the uri keeps a
    // flagged space post from falling through to community-wide membership.
    stubNetwork({ allowed: false })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri }, viewer),
    ).resolves.toBe(false)
    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('asks the configured managing app, minting nothing and reading no space', async () => {
    const fetchMock = stubNetwork({ allowed: true })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(true)

    // The retired credential-mint / getSpace discovery must be gone: a serving
    // access check touches the managing app and nothing else.
    expect(callsTo(fetchMock, MINT_PATH)).toHaveLength(0)
    expect(callsTo(fetchMock, GET_SPACE)).toHaveLength(0)

    const [checkUrl, checkInit] = callsTo(fetchMock, CHECK_ACCESS)[0]
    // The decision is per (space, viewer, permission): no feed, and no post,
    // so an interleaved read of N posts in one space costs one check.
    expect(String(checkUrl)).toBe(
      `${managingAppUrl}/xrpc/${CHECK_ACCESS}?space=${encodeURIComponent(spaceUri)}&did=${encodeURIComponent(viewer)}&permission=view`,
    )
    expect(String(checkUrl)).not.toContain('feed=')
    expect(String(checkUrl)).not.toContain('post=')
    expect(claimsOf(checkInit)).toMatchObject({
      iss: 'did:web:api.blacksky.community',
      aud: managingApp,
      lxm: CHECK_ACCESS,
    })
  })

  it('distinguishes contribution denial from a retryable access outage', async () => {
    let fetchMock = stubNetwork({ allowed: false })
    await expect(canContributeToSpace(ctx, spaceUri, viewer)).resolves.toBe(
      false,
    )
    expect(String(callsTo(fetchMock, CHECK_ACCESS)[0][0])).toContain(
      'permission=contribute',
    )

    clearTenantGateCaches()
    fetchMock = stubNetwork({
      checkAccess: () => new Response('', { status: 503 }),
    })
    await expect(canContributeToSpace(ctx, spaceUri, viewer)).rejects.toThrow(
      'access check unavailable',
    )
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['the managing app denies', { allowed: false }],
    [
      'the managing app errors',
      { checkAccess: () => new Response('', { status: 503 }) },
    ],
  ])('fails closed when %s', async (_name, opts: any) => {
    stubNetwork(opts)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(false)
  })

  it('fails closed when the managing app endpoint is unresolvable', async () => {
    ctx.idResolver.did.resolve = vi.fn().mockResolvedValue({ service: [] })
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it.each([
    ['no managing app is configured', ''],
    ['the configured managing app has no service fragment', managingAppDid],
  ])('fails closed when %s, before any network call', async (_name, pin) => {
    vi.stubEnv('COMMUNITY_SPACE_MANAGING_APP', pin)
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(canViewSpace(ctx, spaceUri, viewer)).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('fails closed on an unmanaged space type without any network call', async () => {
    // A recognised type is necessary but never sufficient: a space type this
    // appview does not manage has no configured decider.
    const otherType = 'at://did:plc:tenant/space/com.example.other/private'
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(canViewSpace(ctx, otherType, viewer)).resolves.toBe(false)
    await expect(
      canViewCommunityPost(
        ctx,
        { uri: `${otherType}/did:plc:alice/app.bsky.feed.post/3k` },
        viewer,
      ),
    ).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('fails closed on a malformed space uri without any network call', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(
      canViewCommunityPost(
        ctx,
        { uri: postUri, spaceUri: 'at://did:plc:tenant/app.bsky.feed.post/3k' },
        viewer,
      ),
    ).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('caches the decision for sixty seconds', async () => {
    let now = 1_000
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    let allowed = true
    const fetchMock = stubNetwork({ checkAccess: () => json({ allowed }) })

    const post = { uri: postUri, spaceUri }
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    now += 59_999
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    // The access decision expires and is asked again.
    now += 2
    allowed = false
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('dispatches URI guards by the stored space discriminator', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: postUri, spaceUri }],
    })
    stubNetwork({ allowed: true })

    await expect(
      assertCommunityMembershipForUris(ctx, viewer, [postUri]),
    ).resolves.toEqual([spaceUri])
    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('preserves the legacy URI guard error', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: stubPostUri, spaceUri: '' }],
    })
    ctx.dataplane.checkCommunityMembership.mockResolvedValue({
      isMember: false,
    })

    await expect(
      assertCommunityMembershipForUris(ctx, viewer, [stubPostUri]),
    ).rejects.toMatchObject({
      message: 'Must be a Blacksky community member',
      error: 'MembershipRequired',
    })
  })
})

describe('space-backed feeds', () => {
  const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
  const authorityDid = 'did:plc:tenant'

  it('recognises a feed as space-backed only for a real space uri', async () => {
    const { isSpaceBackedFeed } =
      await import('../../src/api/community/blacksky/tenant-gate.js')
    const config = (space?: string) =>
      ({
        $type: 'community.blacksky.feed.config',
        authorization: { serviceDid: authorityDid },
        ...(space === undefined ? {} : { space }),
      }) as any

    expect(isSpaceBackedFeed(config(spaceUri))).toBe(true)
    expect(isSpaceBackedFeed(config())).toBe(false)
    expect(isSpaceBackedFeed(null)).toBe(false)
    expect(isSpaceBackedFeed(undefined)).toBe(false)
    // A value in the field that is not a space URI does not flip the feed over.
    expect(isSpaceBackedFeed(config(''))).toBe(false)
    expect(
      isSpaceBackedFeed(config('at://did:plc:tenant/app.bsky.feed.post/3k')),
    ).toBe(false)
    expect(isSpaceBackedFeed(config('nonsense'))).toBe(false)
  })
})

describe('guard recognition of space content', () => {
  const space = 'at://did:plc:tenant/space/community.blacksky.feed/private'

  it('treats any space record as community content', async () => {
    const { isCommunityUri } =
      await import('../../src/api/community/blacksky/membership-guard.js')
    // Posts and likes alike: the guard keys off the URI shape, not the
    // collection, so a new collection in a space is gated the day it appears.
    expect(isCommunityUri(`${space}/did:plc:a/app.bsky.feed.post/3k`)).toBe(
      true,
    )
    expect(isCommunityUri(`${space}/did:plc:a/app.bsky.feed.like/3k`)).toBe(
      true,
    )
    // The original stub collection still counts.
    expect(
      isCommunityUri('at://did:plc:a/community.blacksky.feed.post/3k'),
    ).toBe(true)
    // Ordinary public content does not.
    expect(isCommunityUri('at://did:plc:a/app.bsky.feed.post/3k')).toBe(false)
    // Nor does the space itself, which addresses no record.
    expect(isCommunityUri(space)).toBe(false)
    expect(isCommunityUri(undefined)).toBe(false)
  })
})
