import { describe, expect, it, vi } from 'vitest'
import peerModLabelRoutes from '../../src/data-plane/server/routes/peer-mod-label.js'

describe('getPeerModLabelsForSubject', () => {
  it('returns each owned label with the cid it was applied to', async () => {
    const query = vi.fn(async (_sql: string, _params: unknown[]) => ({
      rows: [
        { val: 'spam', subjectCid: 'bafyone' },
        { val: 'porn', subjectCid: 'bafytwo' },
      ],
    }))
    const routes = peerModLabelRoutes({ pool: { query } } as any)

    const res = await routes.getPeerModLabelsForSubject!(
      {
        subjectUri: 'at://did:plc:alice/app.bsky.feed.post/3k',
        peerModDid: 'did:plc:mod',
      } as any,
      {} as any,
    )

    expect(res).toEqual({
      vals: ['spam', 'porn'],
      labels: [
        { val: 'spam', subjectCid: 'bafyone' },
        { val: 'porn', subjectCid: 'bafytwo' },
      ],
    })
    expect(query.mock.calls[0][1]).toEqual([
      'at://did:plc:alice/app.bsky.feed.post/3k',
      'did:plc:mod',
    ])
  })
})
