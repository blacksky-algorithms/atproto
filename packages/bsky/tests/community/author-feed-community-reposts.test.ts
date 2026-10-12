import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  hydration,
  noBlocksOrMutedReposts,
  presentation,
} from '../../src/api/app/bsky/feed/getAuthorFeed.js'

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
const OWNER = 'did:plc:owner'
const STUB = 'at://did:plc:alice/community.blacksky.feed.post/stub'
const OTHER_STUB = 'at://did:plc:carol/community.blacksky.feed.post/other'
const REPOST = `at://${OWNER}/app.bsky.feed.repost/r1`
const OTHER_REPOST = `at://${OWNER}/app.bsky.feed.repost/r2`

const repostRecord = (subject: string) => ({
  cid: 'bafyrepostcid',
  record: { subject: { uri: subject, cid: 'bafystubcid' } },
  sortedAt: new Date('2026-10-01T00:00:00.000Z'),
  indexedAt: new Date('2026-10-01T00:00:00.000Z'),
})

describe('getAuthorFeed with reposts of community posts', () => {
  let ctx: any
  let ownerRelationship: Record<string, unknown>

  beforeEach(() => {
    presentCommunityFeedItem.mockReset()
    presentCommunityFeedItem.mockImplementation(
      async (_ctx, _hydrate, row, _viewer, opts) => ({
        post: { uri: row.uri },
        ...(opts?.reason ? { reason: opts.reason } : {}),
      }),
    )
    ownerRelationship = {}
    ctx = {
      hydrator: {
        hydrateFeedItems: vi.fn().mockResolvedValue({}),
        hydrateProfileViewers: vi.fn(async () => ({
          profileViewers: new Map([[OWNER, ownerRelationship]]),
        })),
        hydrateReposts: vi.fn(async (uris: string[]) => ({
          reposts: new Map([
            [REPOST, repostRecord(STUB)],
            [OTHER_REPOST, repostRecord(OTHER_STUB)],
          ]),
          profileViewers: new Map([[OWNER, ownerRelationship]]),
          requested: uris,
        })),
      },
      views: {
        feedItemBlocksAndMutes: vi.fn().mockReturnValue({}),
        feedViewPost: vi.fn((item) => ({ post: { uri: item.post.uri } })),
        blockingByList: () => false,
        blockedByList: () => false,
        viewerBlockExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.blocking,
        viewerMuteExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.muted,
        viewerRepostMuteExists: (did: string, state: any) =>
          !!state.profileViewers?.get(did)?.mutedOnlyReposts,
        reasonRepost: vi.fn((uri: string, repost: any) => ({
          $type: 'app.bsky.feed.defs#reasonRepost',
          by: { did: OWNER, handle: 'owner.test' },
          uri,
          cid: repost.cid,
          indexedAt: '2026-10-01T00:00:00.000Z',
        })),
      },
    }
  })

  const run = async (
    items: Array<{ post: { uri: string }; repost?: { uri: string } }>,
    rows: Array<{ uri: string }> = [{ uri: STUB }, { uri: OTHER_STUB }],
  ) => {
    const state = {
      actor: { did: OWNER },
      filter: 'posts_with_replies',
      items,
      communityRows: new Map(rows.map((row) => [row.uri, row])),
    }
    const params = { hydrateCtx: { viewer: VIEWER }, isCommunityMember: true }
    const hydrated = await hydration({ ctx, params, skeleton: state } as any)
    noBlocksOrMutedReposts({ ctx, skeleton: state, hydration: hydrated } as any)
    return presentation({ ctx, skeleton: state, hydration: hydrated } as any)
  }

  it('attributes the repost to the profile owner', async () => {
    const result = await run([{ post: { uri: STUB }, repost: { uri: REPOST } }])

    expect(result.feed).toEqual([
      {
        post: { uri: STUB },
        reason: expect.objectContaining({
          uri: REPOST,
          by: { did: OWNER, handle: 'owner.test' },
        }),
      },
    ])
    expect(presentCommunityFeedItem).toHaveBeenCalledWith(
      ctx,
      { viewer: VIEWER },
      { uri: STUB },
      VIEWER,
      {
        reason: expect.objectContaining({ uri: REPOST }),
        keepMutedAuthor: false,
      },
    )
  })

  it.each([
    ['muted', { muted: true }],
    ['repost-muted', { mutedOnlyReposts: true }],
  ])(
    'keeps the repost when the viewer has %s the owner',
    async (_label, relationship) => {
      Object.assign(ownerRelationship, relationship)

      const result = await run([
        { post: { uri: STUB }, repost: { uri: REPOST } },
      ])

      expect(result.feed).toHaveLength(1)
      expect(presentCommunityFeedItem).toHaveBeenCalledWith(
        ctx,
        { viewer: VIEWER },
        { uri: STUB },
        VIEWER,
        {
          reason: expect.objectContaining({ uri: REPOST }),
          keepMutedAuthor: 'muted' in relationship,
        },
      )
    },
  )

  it("does not relax the author mute for the owner's own community posts", async () => {
    Object.assign(ownerRelationship, { muted: true })

    await run([{ post: { uri: STUB } }])

    expect(presentCommunityFeedItem).toHaveBeenCalledWith(
      ctx,
      { viewer: VIEWER },
      { uri: STUB },
      VIEWER,
      { reason: undefined, keepMutedAuthor: false },
    )
  })

  it('fails the whole request when the viewer blocks the owner', async () => {
    Object.assign(ownerRelationship, { blocking: true })

    await expect(
      run([{ post: { uri: STUB }, repost: { uri: REPOST } }]),
    ).rejects.toMatchObject({ customErrorName: 'BlockedActor' })
  })

  it('drops only the item whose original author is blocked', async () => {
    presentCommunityFeedItem.mockImplementation(
      async (_ctx, _hydrate, row, _viewer, opts) =>
        row.uri === OTHER_STUB
          ? undefined
          : { post: { uri: row.uri }, reason: opts?.reason },
    )

    const result = await run([
      { post: { uri: STUB }, repost: { uri: REPOST } },
      { post: { uri: OTHER_STUB }, repost: { uri: OTHER_REPOST } },
    ])

    expect(result.feed.map((item: any) => item.post.uri)).toEqual([STUB])
  })

  it('drops a repost whose record names another post', async () => {
    ctx.hydrator.hydrateReposts.mockResolvedValue({
      reposts: new Map([[REPOST, repostRecord(OTHER_STUB)]]),
      profileViewers: new Map(),
    })

    const result = await run([{ post: { uri: STUB }, repost: { uri: REPOST } }])

    expect(result.feed).toEqual([])
  })
})
