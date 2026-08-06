import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  hydration,
  noBlocksOrMutes,
  presentation,
} from '../../src/api/app/bsky/feed/getFeed.js'

const { presentCommunityFeedItem } = vi.hoisted(() => ({
  presentCommunityFeedItem: vi.fn(),
}))

vi.mock(
  '../../src/api/community/blacksky/feed/mergedCommunityItems.js',
  () => ({ presentCommunityFeedItem }),
)

const standardUri = 'at://did:plc:alice/app.bsky.feed.post/standard'
const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'
const missingUri = 'at://did:plc:alice/community.blacksky.feed.post/missing'

describe('getFeed community hydration', () => {
  let ctx: any

  beforeEach(() => {
    presentCommunityFeedItem.mockReset()
    presentCommunityFeedItem.mockImplementation(async (_ctx, _hydrate, row) =>
      row.uri === tenantUri ? { post: { uri: row.uri } } : undefined,
    )
    ctx = {
      hydrator: {
        hydrateFeedItems: vi.fn().mockResolvedValue({}),
      },
      dataplane: {
        getCommunityPosts: vi.fn().mockResolvedValue({
          posts: [{ uri: tenantUri, feedUri: 'at://feed' }],
        }),
      },
      views: {
        feedItemBlocksAndMutes: vi.fn().mockReturnValue({}),
        feedViewPost: vi.fn((item) => ({ post: { uri: item.post.uri } })),
      },
    }
  })

  const skeleton = () =>
    ({
      items: [
        { post: { uri: tenantUri }, feedContext: 'tenant-context' },
        { post: { uri: standardUri }, feedContext: 'standard-context' },
        { post: { uri: missingUri }, feedContext: 'missing-context' },
      ],
      reqId: 'request-id',
      passthrough: {},
    }) as any

  it('preserves mixed skeleton order and drops unavailable community rows', async () => {
    const state = skeleton()
    const hydrated = await hydration({
      ctx,
      params: { hydrateCtx: { viewer: 'did:plc:viewer' } },
      skeleton: state,
    } as any)

    expect(ctx.hydrator.hydrateFeedItems).toHaveBeenCalledWith(
      [{ post: { uri: standardUri }, feedContext: 'standard-context' }],
      { viewer: 'did:plc:viewer' },
    )
    expect(ctx.dataplane.getCommunityPosts).toHaveBeenCalledWith({
      uris: [tenantUri, missingUri],
    })

    noBlocksOrMutes({ ctx, skeleton: state, hydration: hydrated } as any)
    const result = presentation({
      ctx,
      skeleton: state,
      hydration: hydrated,
    } as any)

    expect(result.feed).toEqual([
      {
        post: { uri: tenantUri },
        feedContext: 'tenant-context',
        reqId: 'request-id',
      },
      {
        post: { uri: standardUri },
        feedContext: 'standard-context',
        reqId: 'request-id',
      },
    ])
    expect(ctx.views.feedItemBlocksAndMutes).toHaveBeenCalledOnce()
  })

  it('keeps the standard-only hydration and presentation path unchanged', async () => {
    const state = {
      ...skeleton(),
      items: [{ post: { uri: standardUri }, feedContext: 'standard-context' }],
    }
    const hydrated = await hydration({
      ctx,
      params: { hydrateCtx: { viewer: 'did:plc:viewer' } },
      skeleton: state,
    } as any)
    noBlocksOrMutes({ ctx, skeleton: state, hydration: hydrated } as any)
    const result = presentation({
      ctx,
      skeleton: state,
      hydration: hydrated,
    } as any)

    expect(ctx.dataplane.getCommunityPosts).not.toHaveBeenCalled()
    expect(presentCommunityFeedItem).not.toHaveBeenCalled()
    expect(result.feed).toEqual([
      {
        post: { uri: standardUri },
        feedContext: 'standard-context',
        reqId: 'request-id',
      },
    ])
  })
})
