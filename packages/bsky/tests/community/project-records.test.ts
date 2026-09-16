import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { Secp256k1Keypair } from '@atproto/crypto'
import { projectRecordsHandler } from '../../src/api/community/blacksky/space/projectRecords.js'
import { clearTenantGateCaches } from '../../src/api/community/blacksky/tenant-gate.js'

const spaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const otherSpaceUri = 'at://did:plc:tenant/space/community.blacksky.feed/other'
const author = 'did:plc:alice'
const mentioned = 'did:plc:bob'
const postUri = `${spaceUri}/${author}/app.bsky.feed.post/3kpost`
const managingAppDid = 'did:web:feeds.example.com'
const managingApp = `${managingAppDid}#bsky_fg`
const managingAppUrl = 'https://feeds.example.com'
const projectorDid = 'did:web:daemon.example.com'

const CHECK_ACCESS = 'community.blacksky.space.checkAccess'

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  })

describe('space projection ingress', () => {
  let keypair: Secp256k1Keypair
  let ctx: any

  /**
   * Answers per-permission `checkAccess` on the configured managing app, so a
   * test only has to say what each decision is.
   */
  const stubNetwork = (opts: {
    contribute?: () => Response | Promise<Response>
    view?: () => Response | Promise<Response>
  }) => {
    const fetchMock = vi.fn(async (url: any) => {
      const href = String(url)
      if (href.includes(CHECK_ACCESS)) {
        const permission = new URL(href).searchParams.get('permission')
        if (permission === 'contribute') {
          return opts.contribute
            ? await opts.contribute()
            : json({ allowed: true })
        }
        return opts.view ? await opts.view() : json({ allowed: true })
      }
      throw new Error(`unexpected fetch: ${href}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    return fetchMock
  }

  beforeEach(async () => {
    clearTenantGateCaches()
    vi.stubEnv('SPACE_PROJECTOR_ISSUERS', projectorDid)
    vi.stubEnv('COMMUNITY_SPACE_MANAGING_APP', managingApp)
    keypair = await Secp256k1Keypair.create()
    ctx = {
      cfg: { serverDid: 'did:web:api.blacksky.community' },
      signingKey: keypair,
      dataplane: {
        projectCommunityRecord: vi.fn().mockResolvedValue({ rejected: '' }),
        checkCommunityMembership: vi.fn(),
      },
      idResolver: {
        did: {
          resolve: vi.fn(async (did: string) => {
            if (did === managingAppDid) {
              return {
                id: managingAppDid,
                service: [
                  {
                    id: `${managingAppDid}#bsky_fg`,
                    type: 'BskyFeedGenerator',
                    serviceEndpoint: managingAppUrl,
                  },
                ],
              }
            }
            return null
          }),
        },
      },
    }
  })

  afterEach(() => {
    vi.unstubAllEnvs()
    vi.restoreAllMocks()
  })

  const op = (overrides: Record<string, unknown> = {}) => ({
    space: spaceUri,
    author,
    uri: postUri,
    cid: 'bafyreiaaaa',
    revision: '3krev',
    operation: 'create',
    collection: 'app.bsky.feed.post',
    record: {
      $type: 'app.bsky.feed.post',
      text: 'a projected post',
      createdAt: '2026-08-17T00:00:00Z',
    },
    ...overrides,
  })

  const project = (ops: unknown[], iss = projectorDid) =>
    projectRecordsHandler(ctx)({
      input: { body: { ops } },
      auth: { credentials: { iss } },
    } as any)

  it('refuses an untrusted issuer before touching the data plane', async () => {
    stubNetwork({})
    await expect(
      project([op()], 'did:web:intruder.example.com'),
    ).rejects.toThrow('untrusted projection issuer')
    expect(ctx.dataplane.projectCommunityRecord).not.toHaveBeenCalled()
  })

  it.each([
    ['a uri outside the asserted space', op({ space: otherSpaceUri })],
    ['an author who did not write the record', op({ author: 'did:plc:carol' })],
    [
      'a collection that does not match the uri',
      op({ collection: 'app.bsky.feed.like' }),
    ],
    ['an unparseable uri', op({ uri: 'at://not-a-space-record' })],
  ])('refuses %s and writes no row', async (_name, forged) => {
    stubNetwork({})
    await expect(project([forged])).rejects.toThrow(
      'projection does not name a record in its asserted space',
    )
    expect(ctx.dataplane.projectCommunityRecord).not.toHaveBeenCalled()
  })

  it('refuses a post whose author is not admitted to the space', async () => {
    stubNetwork({ contribute: () => json({ allowed: false }) })
    await expect(project([op()])).rejects.toThrow(
      'author is not admitted to this space',
    )
    expect(ctx.dataplane.projectCommunityRecord).not.toHaveBeenCalled()
  })

  it('surfaces an admission outage as an error, not a drop', async () => {
    stubNetwork({ contribute: () => new Response('', { status: 503 }) })
    await expect(project([op()])).rejects.toThrow('access check unavailable')
    expect(ctx.dataplane.projectCommunityRecord).not.toHaveBeenCalled()
  })

  it('surfaces a notification-gate outage as retryable instead of materializing without notifying', async () => {
    stubNetwork({ view: () => new Response('', { status: 503 }) })
    const mention = op({
      record: {
        $type: 'app.bsky.feed.post',
        text: 'hello @bob',
        createdAt: '2026-08-17T00:00:00Z',
        facets: [
          {
            features: [
              { $type: 'app.bsky.richtext.facet#mention', did: mentioned },
            ],
          },
        ],
      },
    })
    await expect(project([mention])).rejects.toThrow(
      'notification gate unavailable',
    )
    expect(ctx.dataplane.projectCommunityRecord).not.toHaveBeenCalled()
  })

  it('materializes a blob-bearing embed intact', async () => {
    stubNetwork({})
    const image = op({
      record: {
        $type: 'app.bsky.feed.post',
        text: 'with media',
        createdAt: '2026-08-17T00:00:00Z',
        embed: {
          $type: 'app.bsky.embed.images',
          images: [
            {
              alt: '',
              image: {
                $type: 'blob',
                ref: { $link: 'bafyreiblob' },
                mimeType: 'image/png',
                size: 100,
              },
            },
          ],
        },
      },
    })
    await expect(project([image])).resolves.toBeTruthy()
    expect(ctx.dataplane.projectCommunityRecord).toHaveBeenCalledOnce()
    const stored = JSON.parse(
      ctx.dataplane.projectCommunityRecord.mock.calls[0][0].recordJson,
    )
    expect(stored.text).toBe('with media')
    expect(stored.embed.$type).toBe('app.bsky.embed.images')
    expect(stored.embed.images[0].image.ref.$link).toBe('bafyreiblob')
  })

  it('keeps both halves of a quote wrapped with media', async () => {
    stubNetwork({})
    const quotedUri = `${spaceUri}/${mentioned}/app.bsky.feed.post/3kquoted`
    const quote = op({
      record: {
        $type: 'app.bsky.feed.post',
        text: 'quoting with media',
        createdAt: '2026-08-17T00:00:00Z',
        embed: {
          $type: 'app.bsky.embed.recordWithMedia',
          record: {
            $type: 'app.bsky.embed.record',
            record: { uri: quotedUri, cid: 'bafyreiquoted' },
          },
          media: {
            $type: 'app.bsky.embed.images',
            images: [
              {
                alt: '',
                image: {
                  $type: 'blob',
                  ref: { $link: 'bafyreiblob' },
                  mimeType: 'image/png',
                  size: 100,
                },
              },
            ],
          },
        },
      },
    })
    await expect(project([quote])).resolves.toBeTruthy()
    const call = ctx.dataplane.projectCommunityRecord.mock.calls[0][0]
    const stored = JSON.parse(call.recordJson)
    expect(stored.embed.$type).toBe('app.bsky.embed.recordWithMedia')
    expect(stored.embed.record.record.uri).toBe(quotedUri)
    expect(stored.embed.media.images[0].image.ref.$link).toBe('bafyreiblob')
    // The quoted author is still a notification candidate.
    expect(call.allowedNotificationDids).toEqual([mentioned])
  })

  it('leaves a blob-free record untouched and projects it', async () => {
    stubNetwork({})
    await expect(project([op()])).resolves.toBeTruthy()
    const call = ctx.dataplane.projectCommunityRecord.mock.calls[0][0]
    expect(JSON.parse(call.recordJson)).toMatchObject({
      text: 'a projected post',
    })
    expect(call.spaceUri).toBe(spaceUri)
  })
})
