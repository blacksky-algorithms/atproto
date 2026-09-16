import { describe, expect, it, vi } from 'vitest'
import {
  signedSpaceMediaUrl,
  spaceMediaExpiry,
  signSpaceMedia,
  verifySpaceMedia,
} from './space-media-signing.js'
import { presignSpaceBlob } from './space-media-presign.js'
import { buildCommunityEmbedView } from './views/communityPostView.js'

describe('space media signing', () => {
  it('rounds expiry to a stable future window', () => {
    expect(spaceMediaExpiry(100)).toBe(spaceMediaExpiry(100 + 1000))
    expect(spaceMediaExpiry(100)).toBeGreaterThan(100)
  })

  it('rejects tampering with every signed field and expiry', () => {
    const space = 'at://did:example:space/space/type/key'
    const did = 'did:example:author'
    const cid = 'bafyblob'
    const exp = 200
    const sig = signSpaceMedia(space, did, cid, exp, 'secret')!
    expect(verifySpaceMedia(space, did, cid, exp, sig, 100, 'secret')).toBe(
      true,
    )
    expect(
      verifySpaceMedia(`${space}x`, did, cid, exp, sig, 100, 'secret'),
    ).toBe(false)
    expect(
      verifySpaceMedia(space, `${did}x`, cid, exp, sig, 100, 'secret'),
    ).toBe(false)
    expect(
      verifySpaceMedia(space, did, `${cid}x`, exp, sig, 100, 'secret'),
    ).toBe(false)
    expect(verifySpaceMedia(space, did, cid, exp + 1, sig, 100, 'secret')).toBe(
      false,
    )
    expect(
      verifySpaceMedia(space, did, cid, exp, `${sig}x`, 100, 'secret'),
    ).toBe(false)
    expect(verifySpaceMedia(space, did, cid, 100, sig, 100, 'secret')).toBe(
      false,
    )
  })

  it('does not mint without a signing key and enforces the size cap', () => {
    vi.stubEnv('COMMUNITY_MEDIA_SIGNING_SECRET', '')
    expect(
      signedSpaceMediaUrl(
        'https://app.example',
        'at://did:example:s/space/t/k',
        'did:example:a',
        'bafy',
        1,
      ),
    ).toBeNull()
    vi.stubEnv('COMMUNITY_MEDIA_SIGNING_SECRET', 'secret')
    expect(
      signedSpaceMediaUrl(
        'https://app.example',
        'at://did:example:s/space/t/k',
        'did:example:a',
        'bafy',
        21,
        100,
        { maxBytes: 20 },
      ),
    ).toBeNull()
  })

  it('presigns deterministically within a window and changes across windows and keys', async () => {
    const cfg = {
      communityMediaSigningSecret: 'secret',
      communityMediaSigningWindowSeconds: 100,
      communityMediaMaxImageBytes: 20,
      communityMediaBucketEndpoint: 'https://objects.example',
      communityMediaBucketRegion: 'us-east-1',
      communityMediaBucketName: 'blobs',
      communityMediaBucketAccessKeyId: 'access',
      communityMediaBucketSecretAccessKey: 'secret',
    } as any
    const first = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy1' },
      101,
    )
    const sameWindow = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy1' },
      199,
    )
    const nextWindow = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy1' },
      201,
    )
    const differentCid = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy2' },
      101,
    )
    const differentDid = await presignSpaceBlob(
      cfg,
      'did:example:b',
      { cid: 'bafy1' },
      101,
    )
    expect(first).toBe(sameWindow)
    expect(first).not.toBe(nextWindow)
    expect(first).not.toBe(differentCid)
    expect(first).not.toBe(differentDid)
    expect(first).toContain('blocks/did%3Aexample%3Aa/bafy1')
  })

  it('hydrates only referenced space media with signed delivery URLs', async () => {
    const space = 'at://did:example:space/space/type/key'
    const cfg = {
      communityMediaSigningSecret: 'secret',
      communityMediaSigningWindowSeconds: 100,
      communityMediaMaxImageBytes: 20,
      communityMediaBucketEndpoint: 'https://objects.example',
      communityMediaBucketRegion: 'us-east-1',
      communityMediaBucketName: 'blobs',
      communityMediaBucketAccessKeyId: 'access',
      communityMediaBucketSecretAccessKey: 'secret',
    } as any
    const builders = {
      imgUriBuilder: {
        getPresetUri: (_preset: string, did: string, cid: string) =>
          `public/${did}/${cid}`,
      },
      videoUriBuilder: {
        playlist: ({ did, cid }: { did: string; cid: string }) =>
          `https://video.example/${did}/${cid}/playlist.m3u8`,
        thumbnail: ({ did, cid }: { did: string; cid: string }) =>
          `https://video.example/${did}/${cid}/thumbnail.jpg`,
      },
    }
    const image = await buildCommunityEmbedView(
      builders,
      'did:example:author',
      {
        $type: 'app.bsky.embed.images',
        images: [{ image: { ref: { $link: 'bafyimage' }, size: 10 }, alt: '' }],
      },
      space,
      cfg,
    )
    const video = await buildCommunityEmbedView(
      builders,
      'did:example:author',
      {
        $type: 'app.bsky.embed.video',
        video: { ref: { $link: 'bafyvideo' }, size: 10 },
      },
      space,
      cfg,
    )
    expect(image?.images[0].thumb).toBe(image?.images[0].fullsize)
    expect(video?.playlist).toContain(`space=${encodeURIComponent(space)}`)
    expect(
      await buildCommunityEmbedView(
        builders,
        'did:example:author',
        {
          $type: 'app.bsky.embed.images',
          images: [{ image: { ref: { $link: 'bafyoversized' }, size: 21 } }],
        },
        space,
        cfg,
      ),
    ).toBeUndefined()
    expect(
      await buildCommunityEmbedView(
        builders,
        'did:example:author',
        {
          $type: 'app.bsky.embed.video',
          video: { ref: { $link: 'bafyvideo' }, size: 10 },
        },
        space,
      ),
    ).toBeUndefined()
    expect(
      await buildCommunityEmbedView(builders, 'did:example:author', {
        $type: 'app.bsky.embed.video',
        video: { ref: { $link: 'bafyvideo' }, size: 10 },
      }),
    ).toEqual({
      $type: 'app.bsky.embed.video#view',
      cid: 'bafyvideo',
      playlist:
        'https://video.example/did:example:author/bafyvideo/playlist.m3u8',
      thumbnail:
        'https://video.example/did:example:author/bafyvideo/thumbnail.jpg',
      alt: undefined,
      aspectRatio: undefined,
      presentation: undefined,
    })
  })
})
