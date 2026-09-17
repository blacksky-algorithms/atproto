import { beforeEach, describe, expect, it, vi } from 'vitest'
import removeLabelRoute from './removeLabel.js'

const BSKY_POST = 'at://did:plc:alice/app.bsky.feed.post/3kpost'
const LABELED_CID =
  'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'
const MOD = 'did:plc:mod'

const mockOzone = () => {
  const bodies: any[] = []
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: unknown, init: RequestInit) => {
      bodies.push(JSON.parse(String(init.body)))
      return new Response(JSON.stringify({ id: 9 }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    }),
  )
  return bodies
}

const makeCtx = (labels: Array<{ val: string; subjectCid: string }>) => ({
  peerModConfig: { ozoneUrl: 'https://ozone.test', ozoneAuth: 'Basic x' },
  authVerifier: { standard: {} },
  dataplane: {
    getActorBadges: vi.fn(async () => ({ badges: ['peer-moderator'] })),
    getPeerModLabelsForSubject: vi.fn(async () => ({
      vals: labels.map((l) => l.val),
      labels,
    })),
    getPostRecords: vi.fn(),
    getCommunityPost: vi.fn(),
    negatePeerModLabel: vi.fn(async () => ({ found: true })),
  },
})

const registerHandler = (ctx: any) => {
  let captured: any
  removeLabelRoute(
    { add: (_lex: unknown, config: any) => (captured = config) } as any,
    ctx,
  )
  return (body: Record<string, unknown>) =>
    captured.handler({ input: { body }, auth: { credentials: { iss: MOD } } })
}

describe('removeLabel', () => {
  beforeEach(() => {
    vi.unstubAllGlobals()
  })

  it('negates against the cid that was labeled, without reading the post', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx([{ val: 'spam', subjectCid: LABELED_CID }])

    const res = await registerHandler(ctx)({
      subjectUri: BSKY_POST,
      val: 'spam',
    })

    expect(res.body).toEqual({ val: 'spam', subjectUri: BSKY_POST })
    expect(bodies[0]).toMatchObject({
      event: { createLabelVals: [], negateLabelVals: ['spam'] },
      subject: { uri: BSKY_POST, cid: LABELED_CID },
    })
    expect(ctx.dataplane.getPostRecords).not.toHaveBeenCalled()
    expect(ctx.dataplane.getCommunityPost).not.toHaveBeenCalled()
    expect(ctx.dataplane.negatePeerModLabel).toHaveBeenCalledWith({
      subjectUri: BSKY_POST,
      val: 'spam',
      peerModDid: MOD,
      ozoneEventId: '9',
    })
  })

  it('refuses a label the caller did not apply', async () => {
    const bodies = mockOzone()
    const ctx = makeCtx([{ val: 'other', subjectCid: LABELED_CID }])

    await expect(
      registerHandler(ctx)({ subjectUri: BSKY_POST, val: 'spam' }),
    ).rejects.toThrow('Caller did not apply this label')
    expect(bodies).toHaveLength(0)
    expect(ctx.dataplane.negatePeerModLabel).not.toHaveBeenCalled()
  })
})
