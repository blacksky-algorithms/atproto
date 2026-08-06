import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Secp256k1Keypair } from '@atproto/crypto'
import { assertCommunityMembershipForUris } from '../../src/api/community/blacksky/membership-guard.js'
import {
  canViewCommunityPost,
  checkCommunityFeedPermission,
  clearTenantGateCaches,
} from '../../src/api/community/blacksky/tenant-gate.js'

const feedUri = 'at://did:plc:tenant/app.bsky.feed.generator/private'
const postUri = 'at://did:plc:alice/community.blacksky.feed.post/private-post'
const viewer = 'did:plc:viewer'
const authorityDid = 'did:web:feeds.example.com'

describe('community post tenant gate', () => {
  let keypair: Secp256k1Keypair
  let ctx: any

  beforeEach(async () => {
    clearTenantGateCaches()
    keypair = await Secp256k1Keypair.create()
    ctx = {
      cfg: { serverDid: 'did:web:api.blacksky.community' },
      signingKey: keypair,
      dataplane: {
        checkCommunityMembership: vi.fn(),
        getCommunityPosts: vi.fn(),
        getCommunityFeedConfig: vi.fn().mockResolvedValue({
          configJson: JSON.stringify({
            $type: 'community.blacksky.feed.config',
            authorization: {
              serviceDid: authorityDid,
              method: 'community.blacksky.feed.checkUserAccess',
            },
          }),
        }),
      },
      idResolver: {
        did: {
          resolve: vi.fn().mockResolvedValue({
            id: authorityDid,
            service: [
              {
                id: `${authorityDid}#bsky_fg`,
                type: 'BskyFeedGenerator',
                serviceEndpoint: 'https://feeds.example.com',
              },
            ],
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
    expect(ctx.dataplane.getCommunityFeedConfig).not.toHaveBeenCalled()
  })

  it('authenticates the delegated post-level check', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ allowed: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, feedUri }, viewer),
    ).resolves.toBe(true)

    const [url, init] = fetchMock.mock.calls[0]
    expect(String(url)).toBe(
      `https://feeds.example.com/xrpc/community.blacksky.feed.checkUserAccess?feed=${encodeURIComponent(feedUri)}&user=${encodeURIComponent(viewer)}&permission=canView&post=${encodeURIComponent(postUri)}`,
    )
    const token = init.headers.authorization.slice('Bearer '.length)
    const claims = JSON.parse(
      Buffer.from(token.split('.')[1], 'base64url').toString(),
    )
    expect(claims).toMatchObject({
      iss: 'did:web:api.blacksky.community',
      aud: authorityDid,
      lxm: 'community.blacksky.feed.checkUserAccess',
    })
  })

  it('checks any feed permission without a post admission constraint', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ allowed: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(
      checkCommunityFeedPermission(ctx, feedUri, viewer, 'canPost'),
    ).resolves.toBe(true)

    expect(String(fetchMock.mock.calls[0][0])).toBe(
      `https://feeds.example.com/xrpc/community.blacksky.feed.checkUserAccess?feed=${encodeURIComponent(feedUri)}&user=${encodeURIComponent(viewer)}&permission=canPost`,
    )
  })

  it.each([
    ['explicit denial', new Response(JSON.stringify({ allowed: false }))],
    ['authority error', new Response('', { status: 503 })],
  ])('fails closed on %s', async (_name, response) => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(response))
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, feedUri }, viewer),
    ).resolves.toBe(false)
  })

  it('fails closed when the authority is down or config is absent', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('down')))
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, feedUri }, viewer),
    ).resolves.toBe(false)

    clearTenantGateCaches()
    ctx.dataplane.getCommunityFeedConfig.mockResolvedValue({ configJson: '' })
    await expect(
      canViewCommunityPost(ctx, { uri: postUri, feedUri }, viewer),
    ).resolves.toBe(false)
  })

  it('caches delegated decisions for sixty seconds', async () => {
    let now = 1_000
    vi.spyOn(Date, 'now').mockImplementation(() => now)
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ allowed: true })))
      .mockResolvedValueOnce(new Response(JSON.stringify({ allowed: false })))
    vi.stubGlobal('fetch', fetchMock)

    const post = { uri: postUri, feedUri }
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    now += 59_999
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(1)

    now += 2
    await expect(canViewCommunityPost(ctx, post, viewer)).resolves.toBe(false)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('dispatches URI guards by the stored feed discriminator', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: postUri, feedUri }],
    })
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValue(new Response(JSON.stringify({ allowed: true }))),
    )

    await expect(
      assertCommunityMembershipForUris(ctx, viewer, [postUri]),
    ).resolves.toBeUndefined()
    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('preserves the legacy URI guard error', async () => {
    ctx.dataplane.getCommunityPosts.mockResolvedValue({
      posts: [{ uri: postUri, feedUri: '' }],
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
