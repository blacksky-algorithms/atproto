import { beforeEach, describe, expect, it, vi } from 'vitest'
import { buildCommunityPostView } from '../../src/api/community/blacksky/views/communityPostView.js'

vi.mock('../../src/api/community/blacksky/tenant-gate.js', () => ({
  canViewCommunityPost: vi.fn().mockResolvedValue(true),
}))

const tenantFeed =
  'at://did:plc:tenant/app.bsky.feed.generator/private-community'

describe(buildCommunityPostView, () => {
  let ctx: any

  beforeEach(() => {
    ctx = {
      hydrator: {
        hydrateProfilesBasic: vi.fn().mockResolvedValue({}),
        label: {
          getLabelsForSubjects: vi.fn().mockResolvedValue({
            getBySubject: vi.fn().mockReturnValue([]),
          }),
        },
      },
      views: {
        profileBasic: vi.fn().mockReturnValue({
          did: 'did:plc:author',
          handle: 'author.test',
          labels: [],
        }),
        imgUriBuilder: {},
        videoUriBuilder: {},
      },
      dataplane: {
        getCommunityPostReplyCount: vi.fn().mockResolvedValue({ count: 0 }),
        getCommunityPostLikeCount: vi.fn().mockResolvedValue({ count: 0 }),
        getCommunityPostQuoteCount: vi.fn().mockResolvedValue({ count: 0 }),
        getCommunityPostViewerLike: vi.fn().mockResolvedValue({ likeUri: '' }),
      },
    }
  })

  const row = (feedUri?: string) => ({
    uri: 'at://did:plc:author/community.blacksky.feed.post/3m2post',
    cid: 'bafyrecordcid',
    creator: 'did:plc:author',
    text: 'hello',
    createdAt: '2026-08-06T12:00:00.000Z',
    indexedAt: '2026-08-06T12:00:01.000Z',
    feedUri,
  })

  it('exposes immutable feed identity for tenant posts', async () => {
    await expect(
      buildCommunityPostView(ctx, {}, row(tenantFeed)),
    ).resolves.toMatchObject({ communityFeed: tenantFeed })
  })

  it('does not change the legacy Blacksky post view shape', async () => {
    const view = await buildCommunityPostView(ctx, {}, row())

    expect(view).not.toHaveProperty('communityFeed')
  })
})
