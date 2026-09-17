import { beforeEach, describe, expect, it, vi } from 'vitest'
import applyLabelRoute from './applyLabel.js'

const BSKY_POST = 'at://did:plc:alice/app.bsky.feed.post/3kpost'
const COMMUNITY_POST = 'at://did:plc:alice/community.blacksky.feed.post/3kpost'
const SPACE_POST =
  'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:alice/app.bsky.feed.post/3kpost'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'
const MOD = 'did:plc:mod'

const mockOzone = () => {
  const bodies: any[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: unknown, init: RequestInit) => {
      bodies.push(JSON.parse(String(init.body)))
      return new Response(JSON.stringify({ id: 7 }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }),
  )
  return bodies
}

const makeCtx = (opts: { bskyCid?: string; communityPost?: unknown } = {}) => ({
  peerModConfig: { ozoneUrl: 'https://ozone.test', ozoneAuth: 'Basic x' },
  authVerifier: { standard: {} },
  dataplane: {
    getActorBadges: vi.fn(async () => ({ badges: ['peer-moderator'] })),
    getPostRecords: vi.fn(async () => ({
      records: [{ cid: opts.bskyCid ?? '' }],
    })),
    getCommunityPost: vi.fn(async () => ({ post: opts.communityPost })),
    recordPeerModLabel: vi.fn(async () => ({})),
  },
})

const registerHandler = (ctx: any) => {
  let captured: any
  applyLabelRoute(
    { add: (_lex: unknown, config: any) => (captured = config) } as any,
    ctx,
  )
  return (body: Record<string, unknown>) =>
    captured.handler({ input: { body }, auth: { credentials: { iss: MOD } } })
}

describe('applyLabel', () => {
  beforeEach(() => {
    vi.unstubAllGlobals()
  })

  it('labels an indexed app.bsky post with the supplied cid', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx({ bskyCid: CID })

    const res = await registerHandler(ctx)({
      subjectUri: BSKY_POST,
      subjectCid: CID,
      val: 'spam',
    })

    expect(res.body).toEqual({ val: 'spam', subjectUri: BSKY_POST })
    expect(ctx.dataplane.getPostRecords).toHaveBeenCalledWith({
      uris: [BSKY_POST],
    })
    expect(bodies[0]).toMatchObject({
      event: { createLabelVals: ['spam'], negateLabelVals: [] },
      subject: { uri: BSKY_POST, cid: CID },
      createdBy: MOD,
    })
    expect(ctx.dataplane.recordPeerModLabel).toHaveBeenCalledWith({
      subjectUri: BSKY_POST,
      subjectCid: CID,
      val: 'spam',
      peerModDid: MOD,
      ozoneEventId: '7',
    })
  })

  it('still labels a community post', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx({ communityPost: { cid: CID } })

    await registerHandler(ctx)({
      subjectUri: COMMUNITY_POST,
      subjectCid: CID,
      val: 'spam',
    })

    expect(ctx.dataplane.getCommunityPost).toHaveBeenCalledWith({
      uri: COMMUNITY_POST,
    })
    expect(bodies[0].subject).toEqual({
      $type: 'com.atproto.repo.strongRef',
      uri: COMMUNITY_POST,
      cid: CID,
    })
  })

  it('rejects an app.bsky post the appview has not indexed', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx()

    await expect(
      registerHandler(ctx)({
        subjectUri: BSKY_POST,
        subjectCid: CID,
        val: 'spam',
      }),
    ).rejects.toThrow('Subject post not found')
    expect(bodies).toHaveLength(0)
    expect(ctx.dataplane.recordPeerModLabel).not.toHaveBeenCalled()
  })

  it('rejects a permissioned-space record', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx({ bskyCid: CID })

    await expect(
      registerHandler(ctx)({
        subjectUri: SPACE_POST,
        subjectCid: CID,
        val: 'spam',
      }),
    ).rejects.toThrow('Permissioned-space records cannot be label subjects')
    expect(ctx.dataplane.getPostRecords).not.toHaveBeenCalled()
    expect(bodies).toHaveLength(0)
  })

  it('rejects a caller without the peer-moderator badge', async () => {
    mockOzone()
    const ctx = makeCtx({ bskyCid: CID })
    ctx.dataplane.getActorBadges.mockResolvedValue({ badges: [] })

    await expect(
      registerHandler(ctx)({
        subjectUri: BSKY_POST,
        subjectCid: CID,
        val: 'spam',
      }),
    ).rejects.toThrow('Caller is not a peer-moderator')
  })
})
