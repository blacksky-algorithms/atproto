import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Secp256k1Keypair } from '@atproto/crypto'
import { authorizeCommunityPostSubmission } from '../../src/api/community/blacksky/feed/submitPost.js'
import { clearTenantGateCaches } from '../../src/api/community/blacksky/tenant-gate.js'

const feed = 'at://did:plc:tenant/app.bsky.feed.generator/private'
const root = 'at://did:plc:root/community.blacksky.feed.post/root'
const requester = 'did:plc:requester'
const authorityDid = 'did:web:feeds.example.com'

describe('tenant community post submission', () => {
  let ctx: any

  beforeEach(async () => {
    clearTenantGateCaches()
    ctx = {
      cfg: { serverDid: 'did:web:api.blacksky.community' },
      signingKey: await Secp256k1Keypair.create(),
      dataplane: {
        checkCommunityMembership: vi.fn().mockResolvedValue({ isMember: true }),
        getCommunityPost: vi.fn().mockResolvedValue({ post: undefined }),
        getCommunityFeedConfig: vi.fn().mockResolvedValue({
          configJson: JSON.stringify({
            $type: 'community.blacksky.feed.config',
            contentType: 'communityRecord',
            visibility: 'gated',
            contentStore: 'did:web:api.blacksky.community',
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
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ allowed: true }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      ),
    )
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('preserves the legacy membership branch when feed is omitted', async () => {
    await expect(
      authorizeCommunityPostSubmission(ctx, requester, undefined, undefined),
    ).resolves.toBeUndefined()

    expect(ctx.dataplane.checkCommunityMembership).toHaveBeenCalledWith({
      did: requester,
    })
    expect(ctx.dataplane.getCommunityFeedConfig).not.toHaveBeenCalled()
    expect(fetch).not.toHaveBeenCalled()
  })

  it('rejects a tenant submission without a valid config', async () => {
    ctx.dataplane.getCommunityFeedConfig.mockResolvedValue({ configJson: '' })

    await expect(
      authorizeCommunityPostSubmission(ctx, requester, feed, undefined),
    ).rejects.toMatchObject({ error: 'InvalidFeed' })

    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
    expect(fetch).not.toHaveBeenCalled()
  })

  it('fails closed when canPost is denied', async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify({ allowed: false }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    await expect(
      authorizeCommunityPostSubmission(ctx, requester, feed, undefined),
    ).rejects.toMatchObject({ error: 'PermissionRequired' })

    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('authorizes and returns the tenant feed discriminator', async () => {
    await expect(
      authorizeCommunityPostSubmission(ctx, requester, feed, undefined),
    ).resolves.toBe(feed)

    expect(String(vi.mocked(fetch).mock.calls[0][0])).toContain(
      'permission=canPost',
    )
  })

  it('derives a reply feed from the root row', async () => {
    ctx.dataplane.getCommunityPost.mockResolvedValue({
      post: { uri: root, feedUri: feed },
    })

    await expect(
      authorizeCommunityPostSubmission(ctx, requester, undefined, {
        root: { uri: root },
        parent: { uri: root },
      }),
    ).resolves.toBe(feed)

    expect(ctx.dataplane.checkCommunityMembership).not.toHaveBeenCalled()
  })

  it('rejects a reply feed that differs from the root row', async () => {
    ctx.dataplane.getCommunityPost.mockResolvedValue({
      post: { uri: root, feedUri: feed },
    })

    await expect(
      authorizeCommunityPostSubmission(
        ctx,
        requester,
        'at://did:plc:other/app.bsky.feed.generator/private',
        {
          root: { uri: root },
          parent: { uri: root },
        },
      ),
    ).rejects.toMatchObject({ error: 'InvalidReply' })

    expect(fetch).not.toHaveBeenCalled()
  })
})
