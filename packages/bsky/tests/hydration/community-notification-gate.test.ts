import { beforeEach, describe, expect, it, vi } from 'vitest'
import { Hydrator } from '../../src/hydration/hydrator.js'

const legacyUri = 'at://did:plc:alice/community.blacksky.feed.post/legacy'
const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const tenantUri = `${spaceUri}/did:plc:alice/app.bsky.feed.post/tenant`

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
          spaceUri: uri === tenantUri ? spaceUri : '',
        },
      })),
    }
    hydrator = new Hydrator(dataplane, [], {
      debugFieldAllowedDids: new Set(),
      featureGatesClient: {} as any,
    })
  })

  it('carries the resolved space decision into notification hydration', async () => {
    const result = await (hydrator as any).fetchCommunityPostsForNotifs([
      legacyUri,
      tenantUri,
    ])

    expect([...result.keys()]).toEqual([legacyUri, tenantUri])
    expect(dataplane.getCommunityPost).toHaveBeenCalledWith(
      expect.objectContaining({ uri: legacyUri, allowedSpaceUris: [] }),
    )
    expect(dataplane.getCommunityPost).toHaveBeenCalledWith(
      expect.objectContaining({
        uri: tenantUri,
        allowedSpaceUris: [spaceUri],
      }),
    )
  })
})
