import { beforeEach, describe, expect, it, vi } from 'vitest'
import { buildCommunityThread } from '../../src/api/community/blacksky/feed/communityThread.js'
import { presentCommunityFeedItem } from '../../src/api/community/blacksky/feed/mergedCommunityItems.js'
import { buildCommunityPostView } from '../../src/api/community/blacksky/views/communityPostView.js'
import { Views } from '../../src/views/index.js'

vi.mock('../../src/api/community/blacksky/tenant-gate.js', () => ({
  canViewCommunityPost: vi.fn().mockResolvedValue(true),
}))

vi.mock('../../src/api/community/blacksky/membership-guard.js', () => ({
  assertCommunityMembershipForUris: vi.fn().mockResolvedValue([]),
  isCommunityUri: (uri?: string) =>
    !!uri && uri.includes('community.blacksky.feed.post'),
}))

const AUTHOR = 'did:plc:author'
const POST = `at://${AUTHOR}/community.blacksky.feed.post/3m2post`
const QUOTER = `at://${AUTHOR}/community.blacksky.feed.post/3m2quote`
const SPACE_POST =
  'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:author/app.bsky.feed.post/3kpost'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'
const CREATED_AT = '2026-08-06T12:00:00.000Z'
const LABELER_LABEL = {
  src: 'did:plc:labeler',
  uri: POST,
  cid: CID,
  val: 'misogynoir',
  cts: '2026-08-07T00:00:00.000Z',
}

const selfLabelsJson = (val: string) =>
  JSON.stringify({
    $type: 'com.atproto.label.defs#selfLabels',
    values: [{ val }],
  })

const row = (overrides: Record<string, unknown> = {}) => ({
  uri: POST,
  cid: CID,
  creator: AUTHOR,
  text: 'hello',
  createdAt: CREATED_AT,
  indexedAt: '2026-08-06T12:00:01.000Z',
  labels: selfLabelsJson('porn'),
  ...overrides,
})

const selfLabel = (uri: string, val = 'porn') => ({
  src: AUTHOR,
  uri,
  cid: CID,
  val,
  cts: CREATED_AT,
})

const makeCtx = (rows: Record<string, unknown>[]) => {
  const byUri = new Map(rows.map((r) => [r.uri as string, r]))
  return {
    cfg: {},
    hydrator: {
      hydrateProfilesBasic: vi.fn().mockResolvedValue({}),
      label: {
        getLabelsForSubjects: vi.fn().mockResolvedValue({
          getBySubject: (uri: string) => (uri === POST ? [LABELER_LABEL] : []),
        }),
      },
    },
    views: {
      profileBasic: vi.fn().mockReturnValue({
        did: AUTHOR,
        handle: 'author.test',
        labels: [],
      }),
      selfLabels: Views.prototype.selfLabels,
      imgUriBuilder: {},
      videoUriBuilder: {},
    },
    dataplane: {
      getCommunityPost: vi.fn(async ({ uri }: { uri: string }) => ({
        post: byUri.get(uri),
      })),
      getCommunityPostReplies: vi.fn().mockResolvedValue({ posts: [] }),
      checkCommunityReplyAllowed: vi.fn().mockResolvedValue({ allowed: true }),
      getCommunityPostReplyCount: vi.fn().mockResolvedValue({ count: 0 }),
      getCommunityPostLikeCount: vi.fn().mockResolvedValue({ count: 0 }),
      getCommunityPostQuoteCount: vi.fn().mockResolvedValue({ count: 0 }),
      getCommunityPostViewerLike: vi.fn().mockResolvedValue({ likeUri: '' }),
    },
  }
}

describe('community post self-labels', () => {
  let ctx: any

  beforeEach(() => {
    ctx = makeCtx([row()])
  })

  it('hydrates stored self-labels onto the record and the view', async () => {
    const view = await buildCommunityPostView(ctx, {}, row())

    expect(view?.record).toMatchObject({
      labels: {
        $type: 'com.atproto.label.defs#selfLabels',
        values: [{ val: 'porn' }],
      },
    })
    expect(view?.labels).toEqual([LABELER_LABEL, selfLabel(POST)])
  })

  it('attributes a space record self-label to the author, not the tenant', async () => {
    const view = await buildCommunityPostView(
      ctx,
      {},
      row({
        uri: SPACE_POST,
        spaceUri: 'at://did:plc:tenant/space/community.blacksky.feed/private',
      }),
    )

    expect(view?.labels).toEqual([selfLabel(SPACE_POST)])
  })

  it('leaves an unlabeled post unchanged', async () => {
    const view = await buildCommunityPostView(ctx, {}, row({ labels: null }))

    expect(view?.record).not.toHaveProperty('labels')
    expect(view?.labels).toEqual([LABELER_LABEL])
  })

  it('carries self-labels through a quote embed', async () => {
    const quoter = row({
      uri: QUOTER,
      labels: null,
      embed: JSON.stringify({
        $type: 'app.bsky.embed.record',
        record: { uri: POST, cid: CID },
      }),
    })
    ctx = makeCtx([row(), quoter])

    const view = await buildCommunityPostView(ctx, {}, quoter)

    expect(view?.embed).toMatchObject({
      record: {
        value: { labels: { values: [{ val: 'porn' }] } },
        labels: [LABELER_LABEL, selfLabel(POST)],
      },
    })
  })

  it('carries self-labels through a feed item', async () => {
    const item = await presentCommunityFeedItem(ctx, {}, row() as any)

    expect(item?.post).toMatchObject({
      labels: [LABELER_LABEL, selfLabel(POST)],
    })
  })

  it('carries self-labels through a thread anchor', async () => {
    const res = await buildCommunityThread(
      ctx,
      { labelers: { dids: [], redact: new Set() } } as any,
      { anchor: POST, above: false, below: 1, branchingFactor: 1 },
      'did:plc:viewer',
    )

    expect((res.body as any).thread[0]).toMatchObject({
      uri: POST,
      value: { post: { labels: [LABELER_LABEL, selfLabel(POST)] } },
    })
  })
})
