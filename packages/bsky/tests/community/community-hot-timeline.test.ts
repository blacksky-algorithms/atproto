import { describe, expect, it, vi } from 'vitest'
import getCommunityTimelineRoute from '../../src/api/community/blacksky/feed/getCommunityTimeline.js'
import communityHot, {
  HOT_WINDOW_DAYS,
  formatHotCursor,
  hotWindowStart,
  parseHotCursor,
} from '../../src/data-plane/server/routes/community-hot.js'

const VIEWER = 'did:plc:viewer'
const ANCHOR = '2026-09-03T12:00:00.000Z'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

describe('hot cursor', () => {
  it('round-trips through format and parse', () => {
    const cursor = { anchor: ANCHOR, score: '12345', cid: CID }
    expect(parseHotCursor(formatHotCursor(cursor))).toEqual(cursor)
  })

  it('treats a missing or malformed cursor as the first page', () => {
    expect(parseHotCursor(undefined)).toBeNull()
    expect(parseHotCursor('')).toBeNull()
    expect(parseHotCursor(ANCHOR)).toBeNull()
    expect(parseHotCursor(`not-a-date::1::${CID}`)).toBeNull()
    expect(parseHotCursor(`${ANCHOR}::1.5::${CID}`)).toBeNull()
    expect(parseHotCursor(`${ANCHOR}::-1::${CID}`)).toBeNull()
    expect(parseHotCursor(`${ANCHOR}::1::`)).toBeNull()
    expect(parseHotCursor(`${ANCHOR}::1::${CID}::extra`)).toBeNull()
  })

  it('opens the window a fixed number of days before the anchor', () => {
    const start = hotWindowStart(ANCHOR)
    expect(Date.parse(ANCHOR) - Date.parse(start)).toBe(
      HOT_WINDOW_DAYS * 24 * 60 * 60 * 1000,
    )
    expect(start < ANCHOR).toBe(true)
  })
})

describe('getCommunityHotTimeline dataplane route', () => {
  const row = (cid: string, score: number) => ({
    uri: `at://did:plc:alice/community.blacksky.feed.post/${cid}`,
    cid,
    creator: 'did:plc:alice',
    text: 'hot',
    createdAt: ANCHOR,
    indexedAt: ANCHOR,
    sortAt: ANCHOR,
    score: String(score),
  })

  const makeDb = (rows: unknown[]) => {
    const query = vi.fn(async () => ({ rows: [...rows] }))
    return { db: { pool: { query } } as any, query }
  }

  it('anchors the first page at now and passes an open cursor', async () => {
    const { db, query } = makeDb([row('a', 3), row('b', 2)])
    const before = Date.now()
    const res = await (communityHot(db) as any).getCommunityHotTimeline({
      limit: 30,
      cursor: '',
    })

    const [, params] = query.mock.calls[0] as unknown as [string, unknown[]]
    const anchor = params[0] as string
    expect(Date.parse(anchor)).toBeGreaterThanOrEqual(before)
    expect(params[1]).toBe(hotWindowStart(anchor))
    expect(params[2]).toBeNull()
    expect(params[3]).toBe('')
    expect(params[4]).toBe(31)
    expect(res.posts.map((p: any) => p.cid)).toEqual(['a', 'b'])
    expect(res.cursor).toBe('')
  })

  it('keeps the anchor from the cursor and continues after the last row', async () => {
    const { db, query } = makeDb([row('c', 9), row('b', 8), row('a', 7)])
    const cursor = formatHotCursor({ anchor: ANCHOR, score: '10', cid: 'd' })
    const res = await (communityHot(db) as any).getCommunityHotTimeline({
      limit: 2,
      cursor,
    })

    const [, params] = query.mock.calls[0] as unknown as [string, unknown[]]
    expect(params[0]).toBe(ANCHOR)
    expect(params[2]).toBe('10')
    expect(params[3]).toBe('d')
    expect(params[4]).toBe(3)
    expect(res.posts.map((p: any) => p.cid)).toEqual(['c', 'b'])
    expect(res.cursor).toBe(
      formatHotCursor({ anchor: ANCHOR, score: '8', cid: 'b' }),
    )
  })
})

describe('getCommunityTimeline sort param', () => {
  const makeCtx = () => ({
    reqLabelers: () => ({ dids: [], redact: new Set<string>() }),
    hydrator: { createContext: async (v: any) => v },
    views: {},
    dataplane: {
      checkCommunityMembership: vi.fn(async () => ({ isMember: true })),
      getCommunityTimeline: vi.fn(async () => ({
        posts: [],
        cursor: 'recent-cursor',
      })),
      getCommunityHotTimeline: vi.fn(async () => ({
        posts: [],
        cursor: 'hot-cursor',
      })),
    },
    authVerifier: { standard: {} },
  })

  const registerHandler = (ctx: any) => {
    let captured: any
    getCommunityTimelineRoute(
      { add: (_lex: unknown, cfg: any) => (captured = cfg) } as any,
      ctx,
    )
    return (params: any) =>
      captured.handler({
        params,
        auth: { credentials: { iss: VIEWER } },
        req: { headers: {} },
      })
  }

  it('serves the chronological timeline by default', async () => {
    const ctx = makeCtx()
    const res = await registerHandler(ctx)({ limit: 30, cursor: 'c1' })

    expect(ctx.dataplane.getCommunityTimeline).toHaveBeenCalledWith({
      limit: 30,
      cursor: 'c1',
    })
    expect(ctx.dataplane.getCommunityHotTimeline).not.toHaveBeenCalled()
    expect(res.body.cursor).toBe('recent-cursor')
  })

  it('treats an explicit recent sort like the default', async () => {
    const ctx = makeCtx()
    await registerHandler(ctx)({ limit: 30, sort: 'recent' })

    expect(ctx.dataplane.getCommunityTimeline).toHaveBeenCalledTimes(1)
    expect(ctx.dataplane.getCommunityHotTimeline).not.toHaveBeenCalled()
  })

  it('routes the hot sort to the ranked timeline with its cursor', async () => {
    const ctx = makeCtx()
    const res = await registerHandler(ctx)({
      limit: 30,
      sort: 'hot',
      cursor: 'h1',
    })

    expect(ctx.dataplane.getCommunityHotTimeline).toHaveBeenCalledWith({
      limit: 30,
      cursor: 'h1',
    })
    expect(ctx.dataplane.getCommunityTimeline).not.toHaveBeenCalled()
    expect(res.body.cursor).toBe('hot-cursor')
  })
})
