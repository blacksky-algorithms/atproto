import { describe, expect, it } from 'vitest'
import { parseCommunityMediaPdsBuckets } from '../../../config.js'
import { presignSpaceBlob, spaceMediaAuthorPds } from './space-media-presign.js'
import {
  signSpaceMedia,
  spaceMediaExpiry,
  verifySpaceMedia,
} from './space-media-signing.js'
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

  it('does not sign without a key', () => {
    expect(
      signSpaceMedia(
        'at://did:example:s/space/t/k',
        'did:example:a',
        'bafy',
        200,
        '',
      ),
    ).toBeNull()
  })

  it('matches the verifier test vector exactly', () => {
    expect(
      signSpaceMedia(
        'at://did:plc:spacehost123/space/feed/3kspace',
        'did:plc:author456',
        'bafkreicrossrepovector',
        1758067200,
        'cross-repo-test-key',
      ),
    ).toBe('n81rog6m1xwSPJxE_7rDqQOtsc5y5kRqgjs99W78US8')
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
      undefined,
      101,
    )
    const sameWindow = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy1' },
      undefined,
      199,
    )
    const nextWindow = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy1' },
      undefined,
      201,
    )
    const differentCid = await presignSpaceBlob(
      cfg,
      'did:example:a',
      { cid: 'bafy2' },
      undefined,
      101,
    )
    const differentDid = await presignSpaceBlob(
      cfg,
      'did:example:b',
      { cid: 'bafy1' },
      undefined,
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
    } as unknown as Parameters<typeof buildCommunityEmbedView>[0]
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
        video: { ref: { $link: 'bafyvideo' }, size: 50_000_000 },
      },
      space,
      cfg,
    )
    const images = image?.images as Array<Record<string, unknown>>
    expect(images[0].thumb).toBe(images[0].fullsize)
    expect(video?.playlist).toContain(`space=${encodeURIComponent(space)}`)
    expect(video?.thumbnail).toContain('sig=')
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

  describe('per-PDS buckets', () => {
    const nds = 'https://newdisabledsouth.community'
    const cfg = {
      communityMediaSigningSecret: 'secret',
      communityMediaSigningWindowSeconds: 100,
      communityMediaMaxImageBytes: 20,
      communityMediaBucketEndpoint: 'https://objects.example',
      communityMediaBucketRegion: 'us-east-1',
      communityMediaBucketName: 'blobs',
      communityMediaBucketAccessKeyId: 'access',
      communityMediaBucketSecretAccessKey: 'secret',
      communityMediaPdsBuckets: parseCommunityMediaPdsBuckets(
        JSON.stringify([
          {
            pds: `${nds}/`,
            endpoint: 'https://nyc3.digitaloceanspaces.com',
            region: 'nyc3',
            bucket: 'nds-blobs',
            accessKeyId: 'NDSACCESSKEY',
            secretAccessKey: 'nds-secret',
          },
        ]),
      ),
    } as any

    it('presigns a mapped PDS author into that bucket with its creds', async () => {
      const url = new URL(
        (await presignSpaceBlob(
          cfg,
          'did:example:a',
          { cid: 'bafy1' },
          nds,
          101,
        ))!,
      )
      expect(url.host).toBe('nyc3.digitaloceanspaces.com')
      expect(url.pathname).toBe('/nds-blobs/blocks/did%3Aexample%3Aa/bafy1')
      expect(url.searchParams.get('X-Amz-Credential')).toMatch(
        /^NDSACCESSKEY\/\d+\/nyc3\//,
      )
      expect(
        await presignSpaceBlob(
          cfg,
          'did:example:a',
          { cid: 'bafy1' },
          nds,
          199,
        ),
      ).toBe(url.toString())
    })

    it('falls back to the default bucket for unmapped or unknown PDSes', async () => {
      for (const pds of ['https://blacksky.app', undefined]) {
        const url = new URL(
          (await presignSpaceBlob(
            cfg,
            'did:example:a',
            { cid: 'bafy1' },
            pds,
            101,
          ))!,
        )
        expect(url.host).toBe('objects.example')
        expect(url.pathname).toBe('/blobs/blocks/did%3Aexample%3Aa/bafy1')
        expect(url.searchParams.get('X-Amz-Credential')).toMatch(/^access\//)
      }
    })

    it('applies an optional key prefix', async () => {
      const prefixed = {
        ...cfg,
        communityMediaPdsBuckets: [
          { ...cfg.communityMediaPdsBuckets[0], keyPrefix: 'pds/' },
        ],
      }
      const url = new URL(
        (await presignSpaceBlob(
          prefixed,
          'did:example:a',
          { cid: 'bafy1' },
          nds,
          101,
        ))!,
      )
      expect(url.pathname).toBe('/nds-blobs/pds/blocks/did%3Aexample%3Aa/bafy1')
    })

    it('resolves the author PDS origin only when buckets are mapped', async () => {
      let calls = 0
      const dataplane = {
        getIdentityByDid: async () => {
          calls++
          return {
            services: Buffer.from(
              JSON.stringify({
                atproto_pds: {
                  Type: 'AtprotoPersonalDataServer',
                  URL: `${nds}/`,
                },
              }),
            ),
          }
        },
      } as any
      expect(await spaceMediaAuthorPds(cfg, dataplane, 'did:example:a')).toBe(
        nds,
      )
      expect(
        await spaceMediaAuthorPds(
          { ...cfg, communityMediaPdsBuckets: [] },
          dataplane,
          'did:example:a',
        ),
      ).toBeUndefined()
      expect(calls).toBe(1)
      const failing = {
        getIdentityByDid: async () => {
          throw new Error('not found')
        },
      } as any
      expect(
        await spaceMediaAuthorPds(cfg, failing, 'did:example:a'),
      ).toBeUndefined()
    })

    it('rejects malformed config at load', () => {
      expect(parseCommunityMediaPdsBuckets(undefined)).toEqual([])
      expect(() => parseCommunityMediaPdsBuckets('{')).toThrow()
      expect(() => parseCommunityMediaPdsBuckets('{}')).toThrow(/JSON array/)
      expect(() =>
        parseCommunityMediaPdsBuckets(JSON.stringify([{ pds: nds }])),
      ).toThrow(/endpoint is required/)
      const entry = {
        pds: nds,
        endpoint: 'https://e',
        region: 'r',
        bucket: 'b',
        accessKeyId: 'a',
        secretAccessKey: 's',
      }
      expect(() =>
        parseCommunityMediaPdsBuckets(
          JSON.stringify([{ ...entry, pds: 'nope' }]),
        ),
      ).toThrow()
      expect(() =>
        parseCommunityMediaPdsBuckets(
          JSON.stringify([entry, { ...entry, pds: `${nds}/` }]),
        ),
      ).toThrow(/duplicate/)
      expect(() =>
        parseCommunityMediaPdsBuckets(
          JSON.stringify([{ ...entry, keyPrefix: 1 }]),
        ),
      ).toThrow(/keyPrefix/)
    })
  })
})
