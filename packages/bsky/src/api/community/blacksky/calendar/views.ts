import { dedupeStrs } from '@atproto/common'
import {
  type AtUriString,
  type DatetimeString,
  type DidString,
  getBlobCidString,
  lexParseJsonBytes,
} from '@atproto/lex'
import { AtUri } from '@atproto/syntax'
import type { AppContext } from '../../../../context.js'
import type {
  HydrateCtx,
  HydrationState,
} from '../../../../hydration/hydrator.js'
import type { Labels } from '../../../../hydration/label.js'
import { parseDate } from '../../../../hydration/util.js'
import type { community } from '../../../../lexicons/index.js'
import type { Record as RecordEntry } from '../../../../proto/bsky_pb.js'

type EventView = community.blacksky.calendar.defs.EventView
type RsvpView = community.blacksky.calendar.defs.RsvpView

const EVENT_COLLECTION = 'community.lexicon.calendar.event'
const KNOWN_GOING_LIMIT = 4

// Events from other apps often carry fields or location shapes outside the
// published schema, so only the identifying fields are checked here and the
// record is passed through as stored.
const parseEventRecord = (
  bytes: Uint8Array,
): Record<string, unknown> | undefined => {
  if (!bytes.byteLength) return undefined
  const value = lexParseJsonBytes(bytes, { strict: false })
  if (!value || typeof value !== 'object' || Array.isArray(value)) return
  const record = value as Record<string, unknown>
  if (record.$type !== EVENT_COLLECTION) return
  if (typeof record.name !== 'string') return
  return record
}

const thumbCid = (record: Record<string, unknown>): string | undefined => {
  const media = record.media
  if (!Array.isArray(media)) return
  for (const item of media) {
    if (!item || typeof item !== 'object') continue
    const { role, content } = item as { role?: unknown; content?: unknown }
    if (role !== 'thumbnail' || !content) continue
    try {
      return getBlobCidString(content as Parameters<typeof getBlobCidString>[0])
    } catch {
      return
    }
  }
}

/**
 * The one rule for whether a viewer may see an event, shared by getEvents and
 * the endpoints that list things about an event.
 */
function visibleEvent(
  ctx: AppContext,
  hydrateCtx: HydrateCtx,
  uri: AtUriString,
  entry: RecordEntry | undefined,
  labels: Labels,
  state: HydrationState,
) {
  if (!entry?.cid) return
  if (entry.takenDown && !hydrateCtx.includeTakedowns) return
  if (labels.get(uri)?.isTakendown && !hydrateCtx.includeTakedowns) return
  const record = parseEventRecord(entry.record)
  if (!record) return
  const creatorDid = new AtUri(uri).did as DidString
  const creator = ctx.views.profileBasic(creatorDid, state)
  if (!creator) return
  if (ctx.views.viewerBlockExists(creatorDid, state)) return
  if (
    !hydrateCtx.viewer &&
    creator.labels?.some((label) => label.val === '!no-unauthenticated')
  ) {
    return
  }
  return { record, creator, creatorDid }
}

export async function canViewEvent(
  ctx: AppContext,
  hydrateCtx: HydrateCtx,
  uri: AtUriString,
): Promise<boolean> {
  if (new AtUri(uri).collection !== EVENT_COLLECTION) return false
  const [records, labels, state] = await Promise.all([
    ctx.dataplane.getCalendarEventRecords({ uris: [uri] }),
    ctx.hydrator.label.getLabelsForSubjects([uri], hydrateCtx.labelers),
    ctx.hydrator.hydrateProfilesBasic(
      [new AtUri(uri).did as DidString],
      hydrateCtx,
    ),
  ])
  return !!visibleEvent(ctx, hydrateCtx, uri, records.records[0], labels, state)
}

export async function buildEventViews(
  ctx: AppContext,
  hydrateCtx: HydrateCtx,
  requested: AtUriString[],
): Promise<EventView[]> {
  const uris = dedupeStrs(requested).filter(
    (uri) => new AtUri(uri).collection === EVENT_COLLECTION,
  ) as AtUriString[]
  if (!uris.length) return []
  const viewer = hydrateCtx.viewer
  const [records, counts, viewerStates, labels] = await Promise.all([
    ctx.dataplane.getCalendarEventRecords({ uris }),
    ctx.dataplane.getCalendarEventCounts({ uris }),
    viewer
      ? ctx.dataplane.getCalendarEventViewerStates({
          viewerDid: viewer,
          uris,
          knownGoingLimit: KNOWN_GOING_LIMIT,
        })
      : undefined,
    ctx.hydrator.label.getLabelsForSubjects(uris, hydrateCtx.labelers),
  ])

  const dids = new Set<DidString>()
  uris.forEach((uri, i) => {
    dids.add(new AtUri(uri).did as DidString)
    for (const did of viewerStates?.states[i]?.knownGoingDids ?? []) {
      dids.add(did as DidString)
    }
  })
  const state = await ctx.hydrator.hydrateProfilesBasic([...dids], hydrateCtx)

  const views: EventView[] = []
  uris.forEach((uri, i) => {
    const entry = records.records[i]
    const visible = visibleEvent(ctx, hydrateCtx, uri, entry, labels, state)
    if (!visible || !entry.cid) return
    const { record, creator, creatorDid } = visible
    const cid = thumbCid(record)
    const viewerState = viewerStates?.states[i]
    const knownGoing = (viewerState?.knownGoingDids ?? []).flatMap((did) => {
      const profile = ctx.views.profileBasic(did as DidString, state)
      return profile ? [profile] : []
    })
    views.push({
      uri,
      cid: entry.cid,
      creator,
      record,
      thumb: cid
        ? ctx.views.imgUriBuilder.getPresetUri(
            'feed_thumbnail',
            creatorDid,
            cid,
          )
        : undefined,
      goingCount: counts.going[i] ?? 0,
      interestedCount: counts.interested[i] ?? 0,
      knownGoing: viewer ? knownGoing : undefined,
      viewer: viewer
        ? {
            rsvp: viewerState?.rsvpUri
              ? {
                  uri: viewerState.rsvpUri as AtUriString,
                  status: viewerState.rsvpStatus,
                }
              : undefined,
          }
        : undefined,
      labels: labels.getBySubject(uri),
      indexedAt: (
        parseDate(entry.indexedAt) ?? new Date(0)
      ).toISOString() as DatetimeString,
    } as EventView)
  })
  return views
}

export async function buildRsvpViews(
  ctx: AppContext,
  hydrateCtx: HydrateCtx,
  items: { uri: string; creator: string; status: string; indexedAt?: Date }[],
): Promise<RsvpView[]> {
  const state = await ctx.hydrator.hydrateProfilesBasic(
    dedupeStrs(items.map((item) => item.creator)) as DidString[],
    hydrateCtx,
  )
  return items.flatMap((item) => {
    const actor = ctx.views.profileBasic(item.creator as DidString, state)
    if (!actor) return []
    return [
      {
        uri: item.uri as AtUriString,
        status: item.status,
        actor,
        indexedAt: (
          item.indexedAt ?? new Date(0)
        ).toISOString() as DatetimeString,
      } as RsvpView,
    ]
  })
}
