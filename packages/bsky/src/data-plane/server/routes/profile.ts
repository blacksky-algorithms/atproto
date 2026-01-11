import { Timestamp } from '@bufbuild/protobuf'
import { ServiceImpl } from '@connectrpc/connect'
import { Selectable, sql } from 'kysely'
import {
  AppBskyNotificationDeclaration,
  ChatBskyActorDeclaration,
} from '@atproto/api'
import { keyBy } from '@atproto/common'
import { parseRecordBytes } from '../../../hydration/util'
import { Service } from '../../../proto/bsky_connect'
import { VerificationMeta } from '../../../proto/bsky_pb'
import { ActorCache, CachedActor } from '../cache'
import { Database } from '../db'
import { Verification } from '../db/tables/verification'
import { getRecords } from './records'

type VerifiedBy = {
  [handle: string]: Pick<
    VerificationMeta,
    'rkey' | 'handle' | 'displayName' | 'sortedAt'
  >
}

// Helper function to fetch actors from database
async function fetchActorsFromDb(
  db: Database,
  dids: string[],
  returnAgeAssuranceForDids: string[] = [],
) {
  if (dids.length === 0) {
    return []
  }
  const profileUris = dids.map(
    (did) => `at://${did}/app.bsky.actor.profile/self`,
  )
  const statusUris = dids.map(
    (did) => `at://${did}/app.bsky.actor.status/self`,
  )
  const chatDeclarationUris = dids.map(
    (did) => `at://${did}/chat.bsky.actor.declaration/self`,
  )
  const notifDeclarationUris = dids.map(
    (did) => `at://${did}/app.bsky.notification.declaration/self`,
  )
  const { ref } = db.db.dynamic
  const [
    handlesRes,
    verificationsReceived,
    profiles,
    statuses,
    chatDeclarations,
    notifDeclarations,
  ] = await Promise.all([
    db.db
      .selectFrom('actor')
      .leftJoin('actor_state', 'actor_state.did', 'actor.did')
      .where('actor.did', 'in', dids)
      .selectAll('actor')
      .select('actor_state.priorityNotifs')
      .select([
        db.db
          .selectFrom('labeler')
          .whereRef('creator', '=', ref('actor.did'))
          .select(sql<true>`${true}`.as('val'))
          .as('isLabeler'),
      ])
      .execute(),
    db.db
      .selectFrom('verification')
      .selectAll('verification')
      .innerJoin('actor', 'actor.did', 'verification.creator')
      .where('verification.subject', 'in', dids)
      .where('actor.trustedVerifier', '=', true)
      .orderBy('sortedAt', 'asc')
      .execute(),
    getRecords(db)({ uris: profileUris }),
    getRecords(db)({ uris: statusUris }),
    getRecords(db)({ uris: chatDeclarationUris }),
    getRecords(db)({ uris: notifDeclarationUris }),
  ])

  const verificationsBySubjectDid = verificationsReceived.reduce(
    (acc, cur) => {
      const list = acc.get(cur.subject) ?? []
      list.push(cur)
      acc.set(cur.subject, list)
      return acc
    },
    new Map<string, Selectable<Verification>[]>(),
  )

  const byDid = keyBy(handlesRes, 'did')
  const ageAssuranceForDids = new Set(returnAgeAssuranceForDids)

  return dids.map((did, i) => {
    const row = byDid.get(did)

    const status = statuses.records[i]

    const chatDeclaration = parseRecordBytes<ChatBskyActorDeclaration.Record>(
      chatDeclarations.records[i].record,
    )

    const verifications = verificationsBySubjectDid.get(did) ?? []
    const verifiedBy: VerifiedBy = verifications.reduce((acc, cur) => {
      acc[cur.creator] = {
        rkey: cur.rkey,
        handle: cur.handle,
        displayName: cur.displayName,
        sortedAt: Timestamp.fromDate(new Date(cur.sortedAt)),
      }
      return acc
    }, {} as VerifiedBy)

    const activitySubscription = () => {
      const record = parseRecordBytes<AppBskyNotificationDeclaration.Record>(
        notifDeclarations.records[i].record,
      )

      // The dataplane is responsible for setting the default of "followers" (default according to the lexicon).
      const defaultVal = 'followers'

      if (typeof record?.allowSubscriptions !== 'string') {
        return defaultVal
      }

      switch (record.allowSubscriptions) {
        case 'followers':
        case 'mutuals':
        case 'none':
          return record.allowSubscriptions
        default:
          return defaultVal
      }
    }

    const ageAssuranceStatus = () => {
      if (!ageAssuranceForDids.has(did)) {
        return undefined
      }

      const assuranceStatus = row?.ageAssuranceStatus ?? 'unknown'
      let access = row?.ageAssuranceAccess
      if (!access || access === 'unknown') {
        if (assuranceStatus === 'assured') {
          access = 'full'
        } else if (assuranceStatus === 'blocked') {
          access = 'none'
        } else {
          access = 'unknown'
        }
      }

      return {
        lastInitiatedAt: row?.ageAssuranceLastInitiatedAt
          ? Timestamp.fromDate(new Date(row?.ageAssuranceLastInitiatedAt))
          : undefined,
        status: assuranceStatus,
        access,
      }
    }

    return {
      exists: !!row,
      handle: row?.handle ?? undefined,
      profile: profiles.records[i],
      takenDown: !!row?.takedownRef,
      takedownRef: row?.takedownRef || undefined,
      tombstonedAt: undefined, // in current implementation, tombstoned actors are deleted
      labeler: row?.isLabeler ?? false,
      allowIncomingChatsFrom:
        typeof chatDeclaration?.['allowIncoming'] === 'string'
          ? chatDeclaration['allowIncoming']
          : undefined,
      upstreamStatus: row?.upstreamStatus ?? '',
      createdAt: profiles.records[i].createdAt, // @NOTE profile creation date not trusted in production
      priorityNotifications: row?.priorityNotifs ?? false,
      trustedVerifier: row?.trustedVerifier ?? false,
      verifiedBy,
      statusRecord: status,
      tags: [],
      profileTags: [],
      allowActivitySubscriptionsFrom: activitySubscription(),
      ageAssuranceStatus: ageAssuranceStatus(),
    }
  })
}

export default (
  db: Database,
  actorCache?: ActorCache,
): Partial<ServiceImpl<typeof Service>> => ({
  async getActors(req) {
    const { dids, skipCacheForDids = [], returnAgeAssuranceForDids = [] } = req
    if (dids.length === 0) {
      return { actors: [] }
    }

    // If no cache, fetch all from DB
    if (!actorCache) {
      const actors = await fetchActorsFromDb(db, dids, returnAgeAssuranceForDids)
      return { actors }
    }

    // Split DIDs into cached and uncached
    const skipCacheSet = new Set(skipCacheForDids)
    const cacheableDids = dids.filter((did) => !skipCacheSet.has(did))
    const uncacheableDids = dids.filter((did) => skipCacheSet.has(did))

    // Get cached actors
    const cached = await actorCache.getMany(cacheableDids)
    const cacheMisses = cacheableDids.filter((did) => !cached.has(did))

    // Fetch all DIDs that need DB lookup
    const toFetch = [...cacheMisses, ...uncacheableDids]
    const fetched = await fetchActorsFromDb(db, toFetch, returnAgeAssuranceForDids)

    // Build map of fetched actors
    const fetchedMap = new Map<string, CachedActor>()
    toFetch.forEach((did, i) => {
      fetchedMap.set(did, fetched[i] as unknown as CachedActor)
    })

    // Cache the misses (but not the explicitly skipped ones)
    if (cacheMisses.length > 0) {
      const toCache = new Map<string, CachedActor>()
      cacheMisses.forEach((did) => {
        const actor = fetchedMap.get(did)
        if (actor) {
          toCache.set(did, actor)
        }
      })
      // Don't await - cache in background
      actorCache.setMany(toCache).catch(() => {
        // Ignore cache errors
      })
    }

    // Merge results in original order
    const actors = dids.map((did) => {
      const fromCache = cached.get(did)
      if (fromCache) {
        return fromCache
      }
      return fetchedMap.get(did) ?? {
        exists: false,
        takenDown: false,
        labeler: false,
        upstreamStatus: '',
        priorityNotifications: false,
        trustedVerifier: false,
        verifiedBy: {},
        tags: [],
        profileTags: [],
        allowActivitySubscriptionsFrom: 'followers',
      }
    })

    return { actors }
  },

  // @TODO handle req.lookupUnidirectional w/ networked handle resolution
  async getDidsByHandles(req) {
    const { handles } = req
    if (handles.length === 0) {
      return { dids: [] }
    }
    const res = await db.db
      .selectFrom('actor')
      .where('handle', 'in', handles)
      .selectAll()
      .execute()
    const byHandle = keyBy(res, 'handle')
    const dids = handles.map((handle) => byHandle.get(handle)?.did ?? '')
    return { dids }
  },

  async updateActorUpstreamStatus(req) {
    const { actorDid, upstreamStatus } = req
    await db.db
      .updateTable('actor')
      .set({ upstreamStatus })
      .where('did', '=', actorDid)
      .execute()
  },
})
