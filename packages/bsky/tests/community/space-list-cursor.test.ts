import { describe, expect, it, vi } from 'vitest'
import communityRoutes from '../../src/data-plane/server/routes/community.js'

/**
 * The private list's pagination.
 *
 * The other community listers use a bare `"sortAt" < $n` string cursor with no
 * tiebreak. A space projects a batch of records with the same `sortAt` often
 * enough that such a cursor drops or repeats rows at every page boundary, and
 * the symptom — a post that never appears, or appears twice — reads as an
 * indexing bug rather than a cursor bug. So this list uses `(sortAt, cid)`.
 */

const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

const routes = (rows: any[]) => {
  const query = vi.fn(async () => ({ rows: [...rows] }))
  const impl = communityRoutes({ pool: { query } } as any, undefined)
  return { impl, query }
}

const row = (rkey: string, sortAt: string, cid = CID) => ({
  uri: `${SPACE}/did:plc:alice/app.bsky.feed.post/${rkey}`,
  cid,
  creator: 'did:plc:alice',
  text: 't',
  createdAt: sortAt,
  indexedAt: sortAt,
  sortAt,
  space_uri: SPACE,
})

describe('getCommunityFeedBySpace', () => {
  it('orders by the keyset and binds the space', async () => {
    const { impl, query } = routes([row('a', '2026-08-18T00:00:02.000Z')])
    await impl.getCommunityFeedBySpace!(
      { spaceUri: SPACE, limit: 2, cursor: '' } as any,
      {} as any,
    )
    const [sql, params] = query.mock.calls[0] as any
    expect(sql).toContain('WHERE space_uri = $1')
    expect(sql).toContain('moderation_flagged_at IS NULL')
    expect(sql).toContain('ORDER BY "sortAt" DESC, cid DESC')
    expect(params[0]).toBe(SPACE)
  })

  it('emits a cursor carrying the cid tiebreak and consumes it as a tuple', async () => {
    const sortAt = '2026-08-18T00:00:00.000Z'
    const rows = [row('a', sortAt, 'bafycidb'), row('b', sortAt, 'bafycida')]
    const { impl, query } = routes(rows)

    const first = await impl.getCommunityFeedBySpace!(
      { spaceUri: SPACE, limit: 1, cursor: '' } as any,
      {} as any,
    )
    expect(first.cursor).toBe(`${sortAt}::bafycidb`)

    await impl.getCommunityFeedBySpace!(
      { spaceUri: SPACE, limit: 1, cursor: first.cursor } as any,
      {} as any,
    )
    const [sql, params] = query.mock.calls[1] as any
    // A row-value comparison, not two independent predicates: at an equal
    // sortAt it advances on cid instead of re-reading or skipping the page.
    expect(sql).toContain('("sortAt", cid) < ($3, $4)')
    expect(params[2]).toBe(sortAt)
    expect(params[3]).toBe('bafycidb')
  })

  it('rejects an unparseable cursor rather than silently restarting', async () => {
    const { impl } = routes([])
    await expect(
      impl.getCommunityFeedBySpace!(
        { spaceUri: SPACE, limit: 1, cursor: 'garbage' } as any,
        {} as any,
      ),
    ).rejects.toThrow('invalid cursor')
  })

  it('returns nothing when no space is named', async () => {
    const { impl, query } = routes([row('a', '2026-08-18T00:00:00.000Z')])
    const res = await impl.getCommunityFeedBySpace!(
      { spaceUri: '', limit: 10, cursor: '' } as any,
      {} as any,
    )
    expect(res.posts).toEqual([])
    expect(query).not.toHaveBeenCalled()
  })
})
