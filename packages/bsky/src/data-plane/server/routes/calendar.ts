import { Timestamp } from '@bufbuild/protobuf'
import type { ServiceImpl } from '@connectrpc/connect'
import { type RawBuilder, sql } from 'kysely'
import { keyBy } from '@atproto/common'
import { community } from '../../../lexicons/index.js'
import type { Service } from '../../../proto/bsky_connect.js'
import type { Database } from '../db/index.js'
import { TimeCidKeyset, paginate } from '../db/pagination.js'
import { getRecords } from './records.js'

export const RSVP_GOING = 'community.lexicon.calendar.rsvp#going'
export const RSVP_INTERESTED = 'community.lexicon.calendar.rsvp#interested'
const AFFIRMATIVE = [RSVP_GOING, RSVP_INTERESTED]

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  getCalendarEventRecords: getRecords(db, community.lexicon.calendar.event),

  async getCalendarEventCounts(req) {
    if (!req.uris.length) return { going: [], interested: [] }
    const rows = await db.db
      .selectFrom('calendar_rsvp')
      .where('subject', 'in', req.uris)
      .where('status', 'in', AFFIRMATIVE)
      .select([
        'subject',
        sql<number>`count(*) filter (where status = ${RSVP_GOING})`.as('going'),
        sql<number>`count(*) filter (where status = ${RSVP_INTERESTED})`.as(
          'interested',
        ),
      ])
      .groupBy('subject')
      .execute()
    const bySubject = keyBy(rows, 'subject')
    return {
      going: req.uris.map((uri) => Number(bySubject.get(uri)?.going ?? 0)),
      interested: req.uris.map((uri) =>
        Number(bySubject.get(uri)?.interested ?? 0),
      ),
    }
  },

  async getCalendarEventViewerStates(req) {
    const { viewerDid, uris } = req
    if (!viewerDid || !uris.length) return { states: [] }
    const knownLimit = Math.max(0, Math.min(req.knownGoingLimit, 10))
    const [own, known] = await Promise.all([
      db.db
        .selectFrom('calendar_rsvp')
        .where('creator', '=', viewerDid)
        .where('subject', 'in', uris)
        .select(['subject', 'uri', 'status'])
        .execute(),
      knownLimit > 0
        ? db.db
            .selectFrom('calendar_rsvp as r')
            .innerJoin('follow', (join) =>
              join
                .onRef('follow.subjectDid', '=', 'r.creator')
                .on('follow.creator', '=', viewerDid),
            )
            .where('r.subject', 'in', uris)
            .where('r.status', '=', RSVP_GOING)
            .where(notTakenDown(sql.ref('r.creator')))
            .where(
              sql<boolean>`not (${mutedOrBlocking(viewerDid, sql.ref('r.creator'))})`,
            )
            .where(
              sql<boolean>`not (${blockedBy(viewerDid, sql.ref('r.creator'))})`,
            )
            .select([
              'r.subject',
              'r.creator',
              sql<number>`row_number() over (partition by r.subject order by r."sortAt" desc, r.uri desc)`.as(
                'rank',
              ),
            ])
            .execute()
        : Promise.resolve([]),
    ])
    const ownBySubject = keyBy(own, 'subject')
    const knownBySubject = new Map<string, string[]>()
    for (const row of known) {
      if (Number(row.rank) > knownLimit) continue
      const dids = knownBySubject.get(row.subject) ?? []
      dids.push(row.creator)
      knownBySubject.set(row.subject, dids)
    }
    return {
      states: uris.map((uri) => ({
        rsvpUri: ownBySubject.get(uri)?.uri ?? '',
        rsvpStatus: ownBySubject.get(uri)?.status ?? '',
        knownGoingDids: knownBySubject.get(uri) ?? [],
      })),
    }
  },

  async getCalendarEventRsvps(req) {
    const { eventUri, viewerDid, limit, cursor } = req
    const statuses = req.statuses.length ? req.statuses : AFFIRMATIVE
    const creator = sql.ref('r.creator')
    let builder = db.db
      .selectFrom('calendar_rsvp as r')
      .where('r.subject', '=', eventUri)
      .where('r.status', 'in', statuses)
      .where(notTakenDown(creator))
      .select(['r.uri', 'r.creator', 'r.status', 'r.indexedAt', 'r.sortAt'])
    if (viewerDid) {
      builder = builder.where(
        sql<boolean>`not (${blockedBy(viewerDid, creator)})`,
      )
      builder = req.mutedOrBlocked
        ? builder.where(mutedOrBlocking(viewerDid, creator))
        : builder.where(
            sql<boolean>`not (${mutedOrBlocking(viewerDid, creator)})`,
          )
    }
    const keyset = new SortAtUriKeyset(sql.ref('r.sortAt'), sql.ref('r.uri'))
    builder = paginate(builder, { limit, cursor, keyset })
    const countMutedOrBlocked =
      viewerDid &&
      !req.mutedOrBlocked &&
      req.includeMutedOrBlockedCount &&
      !cursor
    const [rows, count] = await Promise.all([
      builder.execute(),
      countMutedOrBlocked
        ? db.db
            .selectFrom('calendar_rsvp as r')
            .where('r.subject', '=', eventUri)
            .where('r.status', 'in', statuses)
            .where(notTakenDown(creator))
            .where(sql<boolean>`not (${blockedBy(viewerDid, creator)})`)
            .where(mutedOrBlocking(viewerDid, creator))
            .select(sql<number>`count(*)`.as('count'))
            .executeTakeFirst()
        : Promise.resolve(undefined),
    ])
    return {
      rsvps: rows.map((row) => ({
        uri: row.uri,
        creator: row.creator,
        status: row.status,
        indexedAt: Timestamp.fromDate(new Date(row.indexedAt)),
      })),
      cursor: keyset.packFromResult(rows),
      mutedOrBlockedCount: Number(count?.count ?? 0),
    }
  },

  async getMyCalendarEvents(req) {
    const { viewerDid, past, limit, cursor } = req
    const now = (req.now?.toDate() ?? new Date()).toISOString()
    const hosted = db.db
      .selectFrom('calendar_event as e')
      .where('e.creator', '=', viewerDid)
      .select(['e.uri', 'e.startsAt'])
    const attending = db.db
      .selectFrom('calendar_rsvp as r')
      .innerJoin('calendar_event as e', 'e.uri', 'r.subject')
      .where('r.creator', '=', viewerDid)
      .where('r.status', 'in', AFFIRMATIVE)
      .select(['e.uri', 'e.startsAt'])
    let builder = db.db
      .selectFrom(hosted.union(attending).as('mine'))
      .where('mine.startsAt', 'is not', null)
      .where('mine.startsAt', past ? '<' : '>=', now)
      .select(['mine.uri', 'mine.startsAt'])
    const keyset = new StartsAtUriKeyset(
      sql.ref('mine.startsAt'),
      sql.ref('mine.uri'),
    )
    builder = paginate(builder, {
      limit,
      cursor,
      keyset,
      direction: past ? 'desc' : 'asc',
    })
    const rows = await builder.execute()
    return {
      uris: rows.map((row) => row.uri),
      cursor: keyset.packFromResult(rows),
    }
  },
})

const notTakenDown = (did: RawBuilder<unknown>) =>
  sql<boolean>`not exists (select 1 from actor a where a.did = ${did} and a."takedownRef" is not null)`

// Full mutes only: reposts-only / quotes-only mutes are not account mutes.
const mutedOrBlocking = (viewer: string, did: RawBuilder<unknown>) =>
  sql<boolean>`(
    exists (select 1 from mute m where m."mutedByDid" = ${viewer} and m."subjectDid" = ${did}
            and not m."onlyReposts" and not m."onlyQuoteposts")
    or exists (select 1 from list_mute lm join list_item li on li."listUri" = lm."listUri"
               where lm."mutedByDid" = ${viewer} and li."subjectDid" = ${did})
    or exists (select 1 from actor_block b where b.creator = ${viewer} and b."subjectDid" = ${did})
    or exists (select 1 from list_block lb join list_item li on li."listUri" = lb."subjectUri"
               where lb.creator = ${viewer} and li."subjectDid" = ${did})
  )`

const blockedBy = (viewer: string, did: RawBuilder<unknown>) =>
  sql<boolean>`(
    exists (select 1 from actor_block b where b.creator = ${did} and b."subjectDid" = ${viewer})
    or exists (select 1 from list_block lb join list_item li on li."listUri" = lb."subjectUri"
               where lb.creator = ${did} and li."subjectDid" = ${viewer})
  )`

class SortAtUriKeyset extends TimeCidKeyset<{ sortAt: string; uri: string }> {
  labelResult(result: { sortAt: string; uri: string }) {
    return { primary: result.sortAt, secondary: result.uri }
  }
}

class StartsAtUriKeyset extends TimeCidKeyset<{
  startsAt: string | null
  uri: string
}> {
  labelResult(result: { startsAt: string | null; uri: string }) {
    return { primary: result.startsAt ?? '', secondary: result.uri }
  }
}
