import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  hydration,
  noBlocksOrMutes,
  presentation,
  skeleton,
} from '../../src/api/app/bsky/feed/getTimeline.js'

const { presentCommunityFeedItem } = vi.hoisted(() => ({
  presentCommunityFeedItem: vi.fn(),
}))

vi.mock(
  '../../src/api/community/blacksky/feed/mergedCommunityItems.js',
  async (importOriginal) => ({
    ...(await importOriginal<Record<string, unknown>>()),
    presentCommunityFeedItem,
  }),
)

const VIEWER = 'did:plc:viewer'
const MEMBER = 'did:plc:member'
const STUB = 'at://did:plc:alice/community.blacksky.feed.post/stub'
const REPOST = `at://${MEMBER}/app.bsky.feed.repost/r1`
const PUBLIC = 'at://did:plc:bob/app.bsky.feed.post/pub'

const repostRecord = (subject: string) => ({
  cid: 'bafyrepostcid',
  record: { subject: { uri: subject, cid: 'bafystubcid' } },
  sortedAt: new Date('2026-10-01T00:00:00.000Z'),
  indexedAt: new Date('2026-10-01T00:00:00.000Z'),
})

describe('getTimeline with reposts of community posts', () => {
  let ctx: any
  let repostsByUri: Map<string, ReturnType<typeof repostRecord>>
  let viewerRelationships: Map<string, Record<string, unknown>>

  beforeEach(() => {
    presentCommunityFeedItem.mockReset()
    presentCommunityFeedItem.mockImplementation(
      async (_ctx, _hydrate, row, _viewer, opts) => ({
        post: { uri: row.uri },
        ...(opts?.reason ? { reason: opts.reason } : {}),
      }),
    )
    repostsByUri = new Map([[REPOST, repostRecord(STUB)]])
    viewerRelationships = new Map()
    ctx = {
      hydrator: {
        hydrateFeedItems: vi.fn().mockResolvedValue({}),
        hydrateReposts: vi.fn(async (uris: string[]) => ({
          reposts: new Map(uris.map((uri) => [uri, repostsByUri.get(uri)])),
          profileViewers: viewerRelationships,
        })),
      },
      views: {
        feedItemBlocksAndMutes: vi.fn().mockReturnValue({}),
        feedViewPost: vi.fn((item) => ({ post: { uri: item.post.uri } })),
        viewerBlockExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.blocking,
        viewerMuteExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.muted,
        viewerRepostMuteExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.mutedOnlyReposts,
        reasonRepost: vi.fn((uri: string, repost: any) => ({
          $type: 'app.bsky.feed.defs#reasonRepost',
          by: { did: MEMBER, handle: 'member.test' },
          uri,
          cid: repost.cid,
          indexedAt: '2026-10-01T00:00:00.000Z',
        })),
      },
      dataplane: {
        getTimeline: vi.fn(),
        checkCommunityMembership: vi.fn().mockResolvedValue({ isMember: true }),
      },
    }
  })

  const run = async (
    items: Array<{ uri: string; repost?: string }>,
    communityPosts: Array<{ uri: string }>,
    isCommunityMember = true,
  ) => {
    ctx.dataplane.getTimeline.mockResolvedValue({
      items,
      communityPosts,
      cursor: '',
    })
    const params = {
      limit: 30,
      hydrateCtx: { viewer: VIEWER },
      isCommunityMember,
    }
    const state = await skeleton({ ctx, params } as any)
    const hydrated = await hydration({ ctx, params, skeleton: state } as any)
    noBlocksOrMutes({ ctx, skeleton: state, hydration: hydrated } as any)
    return presentation({ ctx, skeleton: state, hydration: hydrated } as any)
  }

  it('attributes a reposted community post to the reposter', async () => {
    const result = await run([{ uri: STUB, repost: REPOST }], [{ uri: STUB }])

    expect(result.feed).toEqual([
      {
        post: { uri: STUB },
        reason: expect.objectContaining({
          $type: 'app.bsky.feed.defs#reasonRepost',
          uri: REPOST,
          cid: 'bafyrepostcid',
          by: { did: MEMBER, handle: 'member.test' },
        }),
      },
    ])
    expect(ctx.hydrator.hydrateReposts).toHaveBeenCalledWith([REPOST], {
      viewer: VIEWER,
    })
    expect(presentCommunityFeedItem).toHaveBeenCalledWith(
      ctx,
      { viewer: VIEWER },
      { uri: STUB },
      VIEWER,
      { reason: expect.objectContaining({ uri: REPOST }) },
    )
  })

  it('drops a repost whose record points at another post', async () => {
    repostsByUri.set(REPOST, repostRecord(PUBLIC))

    const result = await run([{ uri: STUB, repost: REPOST }], [{ uri: STUB }])

    expect(result.feed).toEqual([])
    expect(presentCommunityFeedItem).not.toHaveBeenCalled()
  })

  it('drops a repost whose record is gone', async () => {
    repostsByUri.delete(REPOST)

    const result = await run([{ uri: STUB, repost: REPOST }], [{ uri: STUB }])

    expect(result.feed).toEqual([])
  })

  it('drops a repost whose reposter cannot be shown', async () => {
    ctx.views.reasonRepost.mockReturnValue(undefined)

    const result = await run([{ uri: STUB, repost: REPOST }], [{ uri: STUB }])

    expect(result.feed).toEqual([])
  })

  it.each([
    ['blocked', { blocking: true }],
    ['muted', { muted: true }],
    ['repost-muted', { mutedOnlyReposts: true }],
  ])('drops a repost by a %s reposter', async (_label, relationship) => {
    viewerRelationships.set(MEMBER, relationship)

    const result = await run([{ uri: STUB, repost: REPOST }], [{ uri: STUB }])

    expect(result.feed).toEqual([])
  })

  it('keeps the original post and its repost as two items', async () => {
    const result = await run(
      [{ uri: STUB, repost: REPOST }, { uri: STUB }, { uri: PUBLIC }],
      [{ uri: STUB }],
    )

    expect(result.feed).toEqual([
      { post: { uri: STUB }, reason: expect.objectContaining({ uri: REPOST }) },
      { post: { uri: STUB } },
      { post: { uri: PUBLIC } },
    ])
    expect(presentCommunityFeedItem).toHaveBeenCalledTimes(2)
  })

  it('drops a repost whose subject row the data plane withheld', async () => {
    const result = await run([{ uri: STUB, repost: REPOST }], [])

    expect(result.feed).toEqual([])
    expect(ctx.hydrator.hydrateReposts).not.toHaveBeenCalled()
  })

  it('withholds community items from a non-member before hydration', async () => {
    const result = await run(
      [{ uri: STUB, repost: REPOST }, { uri: PUBLIC }],
      [],
      false,
    )

    expect(ctx.dataplane.getTimeline).toHaveBeenCalledWith(
      expect.objectContaining({ includeCommunityPosts: false }),
    )
    expect(ctx.hydrator.hydrateFeedItems).toHaveBeenCalledWith(
      [{ post: { uri: PUBLIC, cid: undefined }, repost: undefined }],
      { viewer: VIEWER },
    )
    expect(ctx.hydrator.hydrateReposts).not.toHaveBeenCalled()
    expect(result.feed).toEqual([{ post: { uri: PUBLIC } }])
  })
})
