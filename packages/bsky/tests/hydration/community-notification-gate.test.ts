import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Hydrator } from '../../src/hydration/hydrator.js'

const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
const tenantUri = 'at://did:plc:alice/community.blacksky.feed.post/tenant'

describe('community notification hydration gate', () => {
  let dataplane: any
  let hydrator: Hydrator

  beforeEach(() => {
    dataplane = {
      getCommunityPost: vi.fn(async ({ uri }) => ({
        post: {
          uri,
          cid: 'bafytest',
          creator: 'did:plc:alice',
          text: uri === legacyUri ? 'legacy' : 'tenant',
          createdAt: new Date().toISOString(),
          indexedAt: new Date().toISOString(),
          feedUri:
            uri === tenantUri
              ? 'at://did:plc:tenant/app.bsky.feed.generator/private'
              : '',
        },
      })),
    }
    hydrator = new Hydrator(dataplane, [], {
      debugFieldAllowedDids: new Set(),
      featureGatesClient: {} as any,
    })
  })

  it('leaves legacy rows untouched and includes allowed tenant rows', async () => {
    const gate = vi.fn().mockResolvedValue(true)
    hydrator.setCommunityPostGate(gate)

    const result = await (hydrator as any).fetchCommunityPostsForNotifs(
      [legacyUri, tenantUri],
      'did:plc:viewer',
    )

    expect([...result.keys()]).toEqual([legacyUri, tenantUri])
    expect(gate).toHaveBeenCalledOnce()
    expect(gate).toHaveBeenCalledWith(
      expect.objectContaining({ uri: tenantUri }),
      'did:plc:viewer',
    )
  })

  it('drops tenant rows on denial, missing gate, or gate failure', async () => {
    for (const gate of [
      vi.fn().mockResolvedValue(false),
      vi.fn().mockRejectedValue(new Error('down')),
      undefined,
    ]) {
      const instance = new Hydrator(dataplane, [], {
        debugFieldAllowedDids: new Set(),
        featureGatesClient: {} as any,
      })
      if (gate) instance.setCommunityPostGate(gate)
      const result = await (instance as any).fetchCommunityPostsForNotifs(
        [legacyUri, tenantUri],
        'did:plc:viewer',
      )
      expect([...result.keys()]).toEqual([legacyUri])
    }
  })
})
