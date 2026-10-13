import { Timestamp } from '@bufbuild/protobuf'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { Database } from '../../src/data-plane/server/db/index.js'
import calendarRoutes, {
  RSVP_GOING,
  RSVP_INTERESTED,
} from '../../src/data-plane/server/routes/calendar.js'

const RSVP_NOTGOING = 'community.lexicon.calendar.rsvp#notgoing'
const VIEWER = 'did:plc:viewer'
const HOST = 'did:plc:host'
const EVENT = `at://${HOST}/community.lexicon.calendar.event/3kevent`
const OTHER_EVENT = `at://${HOST}/community.lexicon.calendar.event/3kother`

describe('calendar dataplane routes', () => {
  let db: Database
  let routes: any
  let seq = 0

  beforeAll(async () => {
    const url = process.env.DB_TEST_POSTGRES_URL || process.env.DB_POSTGRES_URL
    if (!url) {
      throw new Error('Missing DB_TEST_POSTGRES_URL or DB_POSTGRES_URL')
    }
    db = new Database({ url, schema: 'bsky_calendar' })
    await db.migrateToLatestOrThrow()
    routes = calendarRoutes(db)
  })

  afterAll(async () => {
    await db.close()
  })

  afterEach(async () => {
    for (const table of [
      'calendar_rsvp',
      'calendar_event',
      'follow',
      'mute',
      'actor_block',
      'list_item',
      'list_mute',
      'list_block',
      'actor',
    ] as const) {
      await db.db.deleteFrom(table).execute()
    }
  })

  const ts = (n: number) =>
    new Date(Date.UTC(2026, 8, 1, 0, 0, n)).toISOString()

  const rsvp = async (
    creator: string,
    status: string,
    subject = EVENT,
    sortAt = ts(++seq),
  ) => {
    await db.db
      .insertInto('calendar_rsvp')
      .values({
        creator,
        subject,
        uri: `at://${creator}/community.lexicon.calendar.rsvp/${seq}${creator.slice(-4)}`,
        cid: 'bafyrsvp',
        subjectCid: null,
        status,
        indexedAt: sortAt,
        sortAt,
      })
      .execute()
  }

  const event = async (uri: string, creator: string, startsAt: string) => {
    await db.db
      .insertInto('calendar_event')
      .values({
        uri,
        cid: 'bafyevent',
        creator,
        startsAt,
        status: null,
        indexedAt: ts(0),
      })
      .execute()
  }

  const follow = async (creator: string, subjectDid: string) => {
    await db.db
      .insertInto('follow')
      .values({
        uri: `at://${creator}/app.bsky.graph.follow/${subjectDid.slice(-6)}`,
        cid: 'bafyfollow',
        creator,
        subjectDid,
        via: null,
        viaCid: null,
        createdAt: ts(0),
        indexedAt: ts(0),
        takedownRef: null,
      })
      .execute()
  }

  const block = async (creator: string, subjectDid: string) => {
    await db.db
      .insertInto('actor_block')
      .values({
        uri: `at://${creator}/app.bsky.graph.block/${subjectDid.slice(-6)}`,
        cid: 'bafyblock',
        creator,
        subjectDid,
        createdAt: ts(0),
        indexedAt: ts(0),
        takedownRef: null,
      })
      .execute()
  }

  const mute = async (subjectDid: string, onlyReposts = false) => {
    await db.db
      .insertInto('mute')
      .values({
        mutedByDid: VIEWER,
        subjectDid,
        createdAt: ts(0),
        onlyReposts,
        onlyQuoteposts: false,
      })
      .execute()
  }

  const listRsvps = (req: Record<string, unknown>) =>
    routes.getCalendarEventRsvps({
      eventUri: EVENT,
      statuses: [],
      viewerDid: VIEWER,
      mutedOrBlocked: false,
      includeMutedOrBlockedCount: true,
      limit: 50,
      cursor: '',
      ...req,
    })

  it('counts going and interested per event and ignores notgoing', async () => {
    await rsvp('did:plc:a', RSVP_GOING)
    await rsvp('did:plc:b', RSVP_GOING)
    await rsvp('did:plc:c', RSVP_INTERESTED)
    await rsvp('did:plc:d', RSVP_NOTGOING)
    await rsvp('did:plc:e', RSVP_GOING, OTHER_EVENT)

    const res = await routes.getCalendarEventCounts({
      uris: [
        EVENT,
        OTHER_EVENT,
        'at://did:plc:x/community.lexicon.calendar.event/none',
      ],
    })
    expect(res.going).toEqual([2, 1, 0])
    expect(res.interested).toEqual([1, 0, 0])
  })

  it('returns the viewer rsvp and up to the limit of followed goers, newest first', async () => {
    await rsvp(VIEWER, RSVP_INTERESTED)
    for (const did of ['did:plc:f1', 'did:plc:f2', 'did:plc:f3']) {
      await follow(VIEWER, did)
      await rsvp(did, RSVP_GOING)
    }
    await follow(VIEWER, 'did:plc:f4')
    await rsvp('did:plc:f4', RSVP_INTERESTED)
    await rsvp('did:plc:stranger', RSVP_GOING)

    const res = await routes.getCalendarEventViewerStates({
      viewerDid: VIEWER,
      uris: [EVENT, OTHER_EVENT],
      knownGoingLimit: 2,
    })
    expect(res.states[0].rsvpStatus).toBe(RSVP_INTERESTED)
    expect(res.states[0].rsvpUri).toContain(VIEWER)
    expect(res.states[0].knownGoingDids).toEqual(['did:plc:f3', 'did:plc:f2'])
    expect(res.states[1]).toEqual({
      rsvpUri: '',
      rsvpStatus: '',
      knownGoingDids: [],
    })
  })

  it('leaves muted, blocked and blocking followees out of known goers', async () => {
    for (const did of ['did:plc:ok', 'did:plc:muted', 'did:plc:blocker']) {
      await follow(VIEWER, did)
      await rsvp(did, RSVP_GOING)
    }
    await mute('did:plc:muted')
    await block('did:plc:blocker', VIEWER)

    const res = await routes.getCalendarEventViewerStates({
      viewerDid: VIEWER,
      uris: [EVENT],
      knownGoingLimit: 4,
    })
    expect(res.states[0].knownGoingDids).toEqual(['did:plc:ok'])
  })

  it('splits rsvps into main and muted-or-blocked sections and hides accounts that block the viewer', async () => {
    await rsvp('did:plc:plain', RSVP_GOING)
    await rsvp('did:plc:muted', RSVP_GOING)
    await rsvp('did:plc:repostmuted', RSVP_GOING)
    await rsvp('did:plc:blocked', RSVP_INTERESTED)
    await rsvp('did:plc:listmuted', RSVP_GOING)
    await rsvp('did:plc:blocker', RSVP_GOING)
    await rsvp('did:plc:gone', RSVP_GOING)
    await mute('did:plc:muted')
    await mute('did:plc:repostmuted', true)
    await block(VIEWER, 'did:plc:blocked')
    await block('did:plc:blocker', VIEWER)
    const list = `at://${VIEWER}/app.bsky.graph.list/mutes`
    await db.db
      .insertInto('list_mute')
      .values({ listUri: list, mutedByDid: VIEWER, createdAt: ts(0) })
      .execute()
    await db.db
      .insertInto('list_item')
      .values({
        uri: `at://${VIEWER}/app.bsky.graph.listitem/1`,
        cid: 'bafyitem',
        creator: VIEWER,
        subjectDid: 'did:plc:listmuted',
        listUri: list,
        createdAt: ts(0),
        indexedAt: ts(0),
      })
      .execute()
    await db.db
      .insertInto('actor')
      .values({
        did: 'did:plc:gone',
        indexedAt: ts(0),
        takedownRef: 'BSKY-TAKEDOWN',
      } as any)
      .execute()

    const main = await listRsvps({})
    expect(main.rsvps.map((r: any) => r.creator).sort()).toEqual([
      'did:plc:plain',
      'did:plc:repostmuted',
    ])
    expect(main.mutedOrBlockedCount).toBe(3)

    const hidden = await listRsvps({ mutedOrBlocked: true })
    expect(hidden.rsvps.map((r: any) => r.creator).sort()).toEqual([
      'did:plc:blocked',
      'did:plc:listmuted',
      'did:plc:muted',
    ])

    const anonymous = await listRsvps({ viewerDid: '' })
    expect(anonymous.rsvps).toHaveLength(6)
  })

  it('filters by status and pages newest first without gaps or repeats', async () => {
    for (let i = 0; i < 5; i++) await rsvp(`did:plc:p${i}`, RSVP_GOING)
    await rsvp('did:plc:i', RSVP_INTERESTED)

    const seen: string[] = []
    let cursor = ''
    do {
      const page = await listRsvps({ statuses: [RSVP_GOING], limit: 2, cursor })
      seen.push(...page.rsvps.map((r: any) => r.creator))
      cursor = page.cursor
    } while (cursor)
    expect(seen).toEqual([
      'did:plc:p4',
      'did:plc:p3',
      'did:plc:p2',
      'did:plc:p1',
      'did:plc:p0',
    ])
  })

  it('lists hosted and rsvpd events, upcoming ascending and past descending', async () => {
    const now = new Date('2026-09-15T00:00:00.000Z')
    const at = (d: string) => new Date(d).toISOString()
    const hostedSoon = `at://${VIEWER}/community.lexicon.calendar.event/soon`
    const goingLater = `at://${HOST}/community.lexicon.calendar.event/later`
    const interestedPast = `at://${HOST}/community.lexicon.calendar.event/past`
    const notGoing = `at://${HOST}/community.lexicon.calendar.event/nope`
    const unrelated = `at://${HOST}/community.lexicon.calendar.event/other`
    await event(hostedSoon, VIEWER, at('2026-09-16'))
    await event(goingLater, HOST, at('2026-09-20'))
    await event(interestedPast, HOST, at('2026-09-01'))
    await event(notGoing, HOST, at('2026-09-18'))
    await event(unrelated, HOST, at('2026-09-17'))
    await rsvp(VIEWER, RSVP_GOING, goingLater)
    await rsvp(VIEWER, RSVP_INTERESTED, interestedPast)
    await rsvp(VIEWER, RSVP_NOTGOING, notGoing)
    await rsvp(VIEWER, RSVP_GOING, hostedSoon)

    const req = (past: boolean, cursor = '', limit = 50) =>
      routes.getMyCalendarEvents({
        viewerDid: VIEWER,
        past,
        now: Timestamp.fromDate(now),
        limit,
        cursor,
      })

    const first = await req(false, '', 1)
    expect(first.uris).toEqual([hostedSoon])
    const second = await req(false, first.cursor, 1)
    expect(second.uris).toEqual([goingLater])
    expect((await req(true)).uris).toEqual([interestedPast])
  })
})
