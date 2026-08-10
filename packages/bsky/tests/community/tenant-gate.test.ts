import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Secp256k1Keypair } from '@atproto/crypto'
import { assertCommunityMembershipForUris } from '../../src/api/community/blacksky/membership-guard.js'
import {
  canViewCommunityPost,
  clearTenantGateCaches,
} from '../../src/api/community/blacksky/tenant-gate.js'

const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const postUri = `${spaceUri}/did:plc:alice/app.bsky.feed.post/3kpost`
const viewer = 'did:plc:viewer'
const authorityDid = 'did:plc:tenant'
const managingAppDid = 'did:web:feeds.example.com'
const managingApp = `${managingAppDid}#bsky_fg`
const spaceHost = 'https://pds.example.com'
const managingAppUrl = 'https://feeds.example.com'

const GET_SPACE = 'com.atproto.space.getSpace'
const CHECK_ACCESS = 'community.blacksky.space.checkAccess'

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })

const claimsOf = (init: any) => {
  const token = init.headers.authorization.slice('Bearer '.length)
  return JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString())
}

describe('community post tenant gate', () => {
  let keypair: Secp256k1Keypair
  let ctx: any

  /**
   * Answers `getSpace` on the space host and `checkAccess` on the managing
   * app, so a test only has to say what the managing app decides.
   */
  const stubNetwork = (opts: {
    allowed?: boolean
    getSpace?: () => Response | Promise<Response>
    checkAccess?: () => Response | Promise<Response>
  }) => {
    const fetchMock = vi.fn(async (url: any) => {
      const href = String(url)
      if (href.includes(GET_SPACE)) {
        return opts.getSpace
          ? await opts.getSpace()
          : json({
              space: spaceUri,
              config: {
                $type: 'com.atproto.simplespace.defs#config',
                policy: 'managing-app',
                managingApp,
              },
            })
      }
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
          // The space's authority names the space host; the managing app names
          // its own endpoint. Nothing here involves a feed.
          resolve: vi.fn(async (did: string) => {
            if (did === authorityDid) {
              return {
                id: authorityDid,
                service: [
                  {
                    id: `${authorityDid}#atproto_space_host`,
                    type: 'AtprotoPersonalDataServer',
                    serviceEndpoint: spaceHost,
                  },
                ],
              }
            }
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
    delete process.env.COMMUNITY_POSTS_ENABLED
  })

  it('keeps the legacy membership branch unchanged', async () => {
    ctx.dataplane.checkCommunityMembership
      .mockResolvedValueOnce({ isMember: true })
      .mockResolvedValueOnce({ isMember: false })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri }, viewer),
    ).resolves.toBe(true)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri }, viewer),
    ).resolves.toBe(false)
  })

  it('falls back to the pds endpoint when no space host is declared', async () => {
    // `#atproto_space_host` is optional; a community whose space host sits
    // behind its own PDS edge has no reason to publish it. Requiring it would
    // fail closed against every ordinary community.
    ctx.idResolver.did.resolve = vi.fn(async (did: string) => {
      if (did === authorityDid) {
        return {
          id: authorityDid,
          service: [
            {
              id: `${authorityDid}#atproto_pds`,
              type: 'AtprotoPersonalDataServer',
              serviceEndpoint: spaceHost,
            },
          ],
        }
      }
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
    })
    const fetchMock = stubNetwork({ allowed: true })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(true)
    expect(String(fetchMock.mock.calls[0][0])).toBe(
      `${spaceHost}/xrpc/${GET_SPACE}?space=${encodeURIComponent(spaceUri)}`,
    )
  })

  it('prefers the dedicated space host entry over the pds', async () => {
    // Both present: the dedicated entry wins, so an authority can point space
    // traffic at a distinct host.
    const dedicated = 'https://spaces.example.com'
    ctx.idResolver.did.resolve = vi.fn(async (did: string) => {
      if (did === authorityDid) {
        return {
          id: authorityDid,
          service: [
            {
              id: `${authorityDid}#atproto_pds`,
              type: 'AtprotoPersonalDataServer',
              serviceEndpoint: spaceHost,
            },
            {
              id: `${authorityDid}#atproto_space_host`,
              type: 'AtprotoSpaceHost',
              serviceEndpoint: dedicated,
            },
          ],
        }
      }
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
    })
    const fetchMock = stubNetwork({ allowed: true })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(true)
    expect(String(fetchMock.mock.calls[0][0])).toBe(
      `${dedicated}/xrpc/${GET_SPACE}?space=${encodeURIComponent(spaceUri)}`,
    )
  })

  it('asks the space who decides, then asks that app', async () => {
    const fetchMock = stubNetwork({ allowed: true })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(true)

    const [spaceUrl, spaceInit] = fetchMock.mock.calls[0]
    expect(String(spaceUrl)).toBe(
      `${spaceHost}/xrpc/${GET_SPACE}?space=${encodeURIComponent(spaceUri)}`,
    )
    expect(claimsOf(spaceInit)).toMatchObject({
      iss: 'did:web:api.blacksky.community',
      aud: authorityDid,
      lxm: GET_SPACE,
    })

    const [checkUrl, checkInit] = fetchMock.mock.calls[1]
    // The decision is per (space, viewer, permission): no feed, and no post,
    // so an interleaved read of N posts in one space costs one check.
    expect(String(checkUrl)).toBe(
      `${managingAppUrl}/xrpc/${CHECK_ACCESS}?space=${encodeURIComponent(spaceUri)}&did=${encodeURIComponent(viewer)}&permission=canView`,
    )
    expect(String(checkUrl)).not.toContain('feed=')
    expect(String(checkUrl)).not.toContain('post=')
    expect(claimsOf(checkInit)).toMatchObject({
      iss: 'did:web:api.blacksky.community',
      aud: managingAppDid,
      lxm: CHECK_ACCESS,
    })
  })

  it.each([
    ['the managing app denies', { allowed: false }],
    [
      'the managing app errors',
      { checkAccess: () => new Response('', { status: 503 }) },
    ],
    [
      'the space host errors',
      { getSpace: () => new Response('', { status: 503 }) },
    ],
    [
      'the space config names no managing app',
      {
        getSpace: () => json({ space: spaceUri, config: { policy: 'public' } }),
      },
    ],
  ])('fails closed when %s', async (_name, opts: any) => {
    stubNetwork(opts)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(false)
  })

  it('fails closed when the space host is unreachable or unresolvable', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
    ).resolves.toBe(false)

    clearTenantGateCaches()
    // An authority whose document declares no space host.
    ctx.idResolver.did.resolve = vi.fn().mockResolvedValue({ service: [] })
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, spaceUri }, viewer),
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

  it('caches the decision for sixty seconds and the managing app for longer', async () => {
    let now = 1_000
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    let allowed = true
    const fetchMock = stubNetwork({ checkAccess: () => json({ allowed }) })

    const post = { uri: postUri, spaceUri }
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2) // getSpace + checkAccess

    now += 59_999
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2)

    // The access decision expires; who decides does not.
    now += 2
    allowed = false
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(3)
    expect(
      fetchMock.mock.calls.filter(([url]: any) =>
        String(url).includes(GET_SPACE),
      ),
    ).toHaveLength(1)
  })

  it('dispatches URI guards by the stored space discriminator', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: postUri, spaceUri }],
    })
    stubNetwork({ allowed: true })

    await expect(
      assertCommunityMembershipForUris(ctx, viewer, [postUri]),
    ).resolves.toBeUndefined()
    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('preserves the legacy URI guard error', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: postUri, spaceUri: '' }],
    })
    ctx.dataplane.checkCommunityMembership.mockResolvedValue({
      isMember: false,
    })

    await expect(
      assertCommunityMembershipForUris(ctx, viewer, [postUri]),
    ).rejects.toMatchObject({
      message: 'Must be a Blacksky community member',
      error: 'MembershipRequired',
    })
  })
})

describe('space-backed feeds', () => {
  const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'

  it('recognises a feed as space-backed only for a real space uri', async () => {
    const { isSpaceBackedFeed } = await import(
      '../../src/api/community/blacksky/tenant-gate.js'
    )
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
    const { isCommunityUri } = await import(
      '../../src/api/community/blacksky/membership-guard.js'
    )
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
