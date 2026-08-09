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
            contentType: 'communityRecord',
            visibility: 'gated',
            authorization: {
              serviceDid: authorityDid,
              method: 'community.blacksky.feed.checkUserAccess',
            },
            group: 'at://did:plc:tenant/community.blacksky.group/private',
            createdAt: '2026-08-06T12:00:00.000Z',
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

  it('authenticates the delegated feed-level check', async () => {
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
    // The decision is per (feed, viewer, permission). The post is deliberately
    // absent: per-post exclusion is retired in favour of moderation flags.
    expect(String(url)).toBe(
      `https://feeds.example.com/xrpc/community.blacksky.feed.checkUserAccess?feed=${encodeURIComponent(feedUri)}&user=${encodeURIComponent(viewer)}&permission=canView`,
    )
    expect(String(url)).not.toContain('post=')
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

  it.each([
    [
      'required fields are missing',
      {
        $type: 'community.blacksky.feed.config',
        authorization: { serviceDid: authorityDid },
      },
    ],
    [
      'the group is not an AT URI',
      {
        $type: 'community.blacksky.feed.config',
        contentType: 'communityRecord',
        visibility: 'gated',
        authorization: { serviceDid: authorityDid },
        group: 'not-an-at-uri',
        createdAt: '2026-08-06T12:00:00.000Z',
      },
    ],
    [
      'the authorization service is not a DID',
      {
        $type: 'community.blacksky.feed.config',
        contentType: 'communityRecord',
        visibility: 'gated',
        authorization: { serviceDid: 'feeds.example.com' },
        group: 'at://did:plc:tenant/community.blacksky.group/private',
        createdAt: '2026-08-06T12:00:00.000Z',
      },
    ],
    [
      'the timestamp is invalid',
      {
        $type: 'community.blacksky.feed.config',
        contentType: 'communityRecord',
        visibility: 'gated',
        authorization: { serviceDid: authorityDid },
        group: 'at://did:plc:tenant/community.blacksky.group/private',
        createdAt: 'yesterday',
      },
    ],
    [
      'the config is not a gated community-record config',
      {
        $type: 'community.blacksky.feed.config',
        contentType: 'publicRecord',
        visibility: 'public',
        authorization: { serviceDid: authorityDid },
        group: 'at://did:plc:tenant/community.blacksky.group/private',
        createdAt: '2026-08-06T12:00:00.000Z',
      },
    ],
  ])('fails closed when %s', async (_name, config) => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    ctx.dataplane.getCommunityFeedConfig.mockResolvedValue({
      configJson: JSON.stringify(config),
    })

    await expect(
      canViewCommunityPost(ctx, { uri: postUri, feedUri }, viewer),
    ).resolves.toBe(false)
    expect(fetchMock).not.toHaveBeenCalled()
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
