import { beforeEach, describe, expect, it, vi } from 'vitest'
import registerGetEventRsvps from '../../src/api/community/blacksky/calendar/getEventRsvps.js'
import { canViewEvent } from '../../src/api/community/blacksky/calendar/views.js'

const HOST = 'did:plc:host'
const EVENT = `at://${HOST}/community.lexicon.calendar.event/3kevent`

const recordBytes = (value: unknown) =>
  new TextEncoder().encode(JSON.stringify(value))

const eventRecord = {
  $type: 'community.lexicon.calendar.event',
  name: 'Block party',
  createdAt: '2026-09-01T00:00:00.000Z',
}

type Setup = {
  entry?: Record<string, unknown>
  labelTakedown?: boolean
  hostLabels?: string[]
  hostMissing?: boolean
  blocked?: boolean
}

const makeCtx = (setup: Setup = {}) => {
  const entry =
    'entry' in setup
      ? setup.entry
      : { cid: 'bafyevent', takenDown: false, record: recordBytes(eventRecord) }
  return {
    dataplane: {
      getCalendarEventRecords: vi.fn().mockResolvedValue({ records: [entry] }),
      getCalendarEventRsvps: vi
        .fn()
        .mockResolvedValue({ rsvps: [], cursor: '', mutedOrBlockedCount: 0 }),
    },
    hydrator: {
      label: {
        getLabelsForSubjects: vi.fn().mockResolvedValue({
          get: () => ({ isTakendown: !!setup.labelTakedown }),
          getBySubject: () => [],
        }),
      },
      hydrateProfilesBasic: vi.fn().mockResolvedValue({}),
      createContext: vi.fn(),
    },
    views: {
      profileBasic: vi.fn(() =>
        setup.hostMissing
          ? undefined
          : {
              did: HOST,
              handle: 'host.test',
              labels: (setup.hostLabels ?? []).map((val) => ({ val })),
            },
      ),
      viewerBlockExists: vi.fn(() => !!setup.blocked),
    },
  }
}

const hydrateCtx = (viewer: string | null, includeTakedowns = false) =>
  ({ viewer, includeTakedowns, labelers: { dids: [] } }) as any

describe('canViewEvent', () => {
  it('allows a normal event, checking the host rather than the viewer', async () => {
    const ctx = makeCtx()
    const hctx = hydrateCtx('did:plc:v')
    expect(await canViewEvent(ctx as any, hctx, EVENT)).toBe(true)
    expect(ctx.hydrator.hydrateProfilesBasic).toHaveBeenCalledWith(
      [HOST],
      hctx,
    )
    expect(ctx.views.profileBasic).toHaveBeenCalledWith(HOST, {})
    expect(ctx.views.viewerBlockExists).toHaveBeenCalledWith(HOST, {})
  })

  it.each<[string, Setup]>([
    ['unknown events', { entry: undefined }],
    ['events without a cid', { entry: { cid: '', record: new Uint8Array() } }],
    [
      'taken-down records',
      {
        entry: {
          cid: 'bafy',
          takenDown: true,
          record: recordBytes(eventRecord),
        },
      },
    ],
    ['events with a takedown label', { labelTakedown: true }],
    [
      'records that are not events',
      {
        entry: {
          cid: 'bafy',
          record: recordBytes({ $type: 'app.bsky.feed.post', text: 'x' }),
        },
      },
    ],
    ['hosts that cannot be hydrated', { hostMissing: true }],
    ['a block between viewer and host', { blocked: true }],
  ])('hides %s', async (_name, setup) => {
    expect(
      await canViewEvent(makeCtx(setup) as any, hydrateCtx('did:plc:v'), EVENT),
    ).toBe(false)
  })

  it('shows taken-down events to moderators', async () => {
    const ctx = makeCtx({ labelTakedown: true })
    expect(await canViewEvent(ctx as any, hydrateCtx(null, true), EVENT)).toBe(
      true,
    )
  })

  it('hides hosts with !no-unauthenticated from logged-out viewers only', async () => {
    const ctx = makeCtx({ hostLabels: ['!no-unauthenticated'] })
    expect(await canViewEvent(ctx as any, hydrateCtx(null), EVENT)).toBe(false)
    expect(await canViewEvent(ctx as any, hydrateCtx('did:plc:v'), EVENT)).toBe(
      true,
    )
  })

  it('rejects URIs outside the event collection without a lookup', async () => {
    const ctx = makeCtx()
    const post = `at://${HOST}/app.bsky.feed.post/3kpost`
    expect(await canViewEvent(ctx as any, hydrateCtx('did:plc:v'), post)).toBe(
      false,
    )
    expect(ctx.dataplane.getCalendarEventRecords).not.toHaveBeenCalled()
  })
})

describe('getEventRsvps', () => {
  let ctx: ReturnType<typeof makeCtx> & Record<string, any>
  let handler: (input: any) => Promise<any>

  const register = (setup: Setup, viewer: string | null) => {
    ctx = makeCtx(setup) as any
    ctx.authVerifier = {
      standardOptional: {},
      parseCreds: () => ({ viewer, includeTakedowns: false }),
    }
    ctx.reqLabelers = () => ({ dids: [] })
    ctx.hydrator.createContext.mockResolvedValue(hydrateCtx(viewer))
    registerGetEventRsvps(
      {
        add: (_method: unknown, cfg: { handler: typeof handler }) => {
          handler = cfg.handler
        },
      } as any,
      ctx as any,
    )
  }

  beforeEach(() => vi.clearAllMocks())

  it('lists nothing for an event the viewer cannot see', async () => {
    register({ blocked: true }, 'did:plc:v')
    const res = await handler({ auth: {}, params: { event: EVENT }, req: {} })
    expect(res.body).toEqual({ event: EVENT, rsvps: [] })
    expect(res.headers).toHaveProperty('Atproto-Content-Labelers')
    expect(ctx.dataplane.getCalendarEventRsvps).not.toHaveBeenCalled()
  })

  it('lists nothing in the muted-or-blocked section for anonymous callers', async () => {
    register({}, null)
    const res = await handler({
      auth: {},
      params: { event: EVENT, section: 'mutedOrBlocked' },
      req: {},
    })
    expect(res.body.rsvps).toEqual([])
    expect(ctx.dataplane.getCalendarEventRsvps).not.toHaveBeenCalled()
  })

  it('queries RSVPs for a visible event', async () => {
    register({}, 'did:plc:v')
    await handler({ auth: {}, params: { event: EVENT }, req: {} })
    expect(ctx.dataplane.getCalendarEventRsvps).toHaveBeenCalledWith(
      expect.objectContaining({ eventUri: EVENT, viewerDid: 'did:plc:v' }),
    )
  })
})
