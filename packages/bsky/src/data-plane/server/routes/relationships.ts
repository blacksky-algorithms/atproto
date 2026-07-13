import { ServiceImpl } from '@connectrpc/connect'
import { sql } from 'kysely'
import { keyBy } from '@atproto/common'
import { Service } from '../../../proto/bsky_connect'
import { RelationshipCache, CachedRelationship } from '../cache'
import { Database } from '../db'
import { valuesList } from '../db/util'

// List-membership checks are resolved via these bounded lookups rather than
// scalar subqueries joining list_item by subject: actors can appear in
// hundreds of thousands of list_items, and the planner driving from that
// side stalls the query for tens of seconds.
const getListBlocksByCreator = async (
  db: Database,
  creators: string[],
): Promise<Map<string, string[]>> => {
  const byCreator = new Map<string, string[]>()
  if (creators.length === 0) return byCreator
  const rows = await db.db
    .selectFrom('list_block')
    .where('creator', 'in', creators)
    .select(['creator', 'subjectUri'])
    .execute()
  for (const row of rows) {
    const uris = byCreator.get(row.creator) ?? []
    uris.push(row.subjectUri)
    byCreator.set(row.creator, uris)
  }
  return byCreator
}

const getListMemberships = async (
  db: Database,
  listUris: string[],
  subjectDids: string[],
): Promise<Set<string>> => {
  if (listUris.length === 0 || subjectDids.length === 0) return new Set()
  const rows = await db.db
    .selectFrom('list_item')
    .where('listUri', 'in', [...new Set(listUris)])
    .where('subjectDid', 'in', [...new Set(subjectDids)])
    .select(['listUri', 'subjectDid'])
    .execute()
  return new Set(rows.map((row) => membershipKey(row.listUri, row.subjectDid)))
}

const membershipKey = (listUri: string, did: string) => `${listUri}\n${did}`

const firstListedIn = (
  listUris: string[],
  did: string,
  memberships: Set<string>,
): string => {
  return listUris.find((uri) => memberships.has(membershipKey(uri, did))) ?? ''
}

export default (
  db: Database,
  cache?: RelationshipCache,
): Partial<ServiceImpl<typeof Service>> => ({
  async getRelationships(req) {
    const { actorDid, targetDids } = req
    if (targetDids.length === 0) {
      return { relationships: [] }
    }

    // Check cache first
    let cached = new Map<string, CachedRelationship | null>()
    let needFetch = targetDids
    if (cache && actorDid) {
      cached = await cache.getMany(actorDid, targetDids)
      needFetch = targetDids.filter((did) => !cached.has(did))
    }

    // Fetch uncached from DB. Scalar subqueries use .limit(1) to avoid
    // "more than one row returned by a subquery" on duplicate rows.
    let byDid = new Map<string, Record<string, unknown>>()
    if (needFetch.length > 0) {
      const { ref } = db.db.dynamic
      const [muteListRows, blockListsByCreator, res] = await Promise.all([
        actorDid
          ? db.db
              .selectFrom('list_mute')
              .where('mutedByDid', '=', actorDid)
              .select('listUri')
              .execute()
          : [],
        getListBlocksByCreator(
          db,
          actorDid ? [actorDid, ...needFetch] : needFetch,
        ),
        db.db
          .selectFrom('actor')
          .where('did', 'in', needFetch)
          .select([
            'actor.did',
            db.db
              .selectFrom('mute')
              .where('mute.mutedByDid', '=', actorDid)
              .whereRef('mute.subjectDid', '=', ref('actor.did'))
              .select(sql<true>`${true}`.as('val'))
              .as('muted'),
            db.db
              .selectFrom('actor_block')
              .where('actor_block.creator', '=', actorDid)
              .whereRef('actor_block.subjectDid', '=', ref('actor.did'))
              .select('uri')
              .limit(1)
              .as('blocking'),
            db.db
              .selectFrom('actor_block')
              .where('actor_block.subjectDid', '=', actorDid)
              .whereRef('actor_block.creator', '=', ref('actor.did'))
              .select('uri')
              .limit(1)
              .as('blockedBy'),
            db.db
              .selectFrom('follow')
              .where('follow.creator', '=', actorDid)
              .whereRef('follow.subjectDid', '=', ref('actor.did'))
              .select('uri')
              .limit(1)
              .as('following'),
            db.db
              .selectFrom('follow')
              .where('follow.subjectDid', '=', actorDid)
              .whereRef('follow.creator', '=', ref('actor.did'))
              .select('uri')
              .limit(1)
              .as('followedBy'),
          ])
          .execute(),
      ])

      const viewerMuteLists = muteListRows.map((row) => row.listUri)
      const viewerBlockLists = actorDid
        ? (blockListsByCreator.get(actorDid) ?? [])
        : []
      const targetBlockLists = needFetch.flatMap(
        (did) => blockListsByCreator.get(did) ?? [],
      )
      const [targetMemberships, viewerMemberships] = await Promise.all([
        getListMemberships(
          db,
          [...viewerMuteLists, ...viewerBlockLists],
          needFetch,
        ),
        getListMemberships(db, targetBlockLists, actorDid ? [actorDid] : []),
      ])

      byDid = keyBy(res, 'did')
      for (const did of needFetch) {
        const row = byDid.get(did)
        if (!row) continue
        row.mutedByList = firstListedIn(viewerMuteLists, did, targetMemberships)
        row.blockingByList = firstListedIn(
          viewerBlockLists,
          did,
          targetMemberships,
        )
        row.blockedByList = firstListedIn(
          blockListsByCreator.get(did) ?? [],
          actorDid,
          viewerMemberships,
        )
      }

      // Cache the fetched results
      if (cache && actorDid) {
        const toCache = new Map<string, CachedRelationship>()
        for (const did of needFetch) {
          const row = byDid.get(did)
          toCache.set(did, {
            muted: !!(row?.muted),
            mutedByList: (row?.mutedByList as string) ?? '',
            blockedBy: (row?.blockedBy as string) ?? '',
            blocking: (row?.blocking as string) ?? '',
            blockedByList: (row?.blockedByList as string) ?? '',
            blockingByList: (row?.blockingByList as string) ?? '',
            following: (row?.following as string) ?? '',
            followedBy: (row?.followedBy as string) ?? '',
          })
        }
        await cache.setMany(actorDid, toCache).catch(() => {})
      }
    }

    // Build response from cache + DB
    const relationships = targetDids.map((did) => {
      const fromCache = cached.get(did)
      if (fromCache) return fromCache
      const row = byDid.get(did)
      return {
        muted: !!(row?.muted),
        mutedByList: (row?.mutedByList as string) ?? '',
        blockedBy: (row?.blockedBy as string) ?? '',
        blocking: (row?.blocking as string) ?? '',
        blockedByList: (row?.blockedByList as string) ?? '',
        blockingByList: (row?.blockingByList as string) ?? '',
        following: (row?.following as string) ?? '',
        followedBy: (row?.followedBy as string) ?? '',
      }
    })
    return { relationships }
  },

  async getBlockExistence(req) {
    const { pairs } = req
    if (pairs.length === 0) {
      return { exists: [], blocks: [] }
    }
    const { ref } = db.db.dynamic
    const sourceRef = ref('pair.source')
    const targetRef = ref('pair.target')
    const values = valuesList(pairs.map((p) => sql`${p.a}, ${p.b}`))
    const pairDids = [...new Set(pairs.flatMap((p) => [p.a, p.b]))]
    const [blockListsByCreator, res] = await Promise.all([
      getListBlocksByCreator(db, pairDids),
      db.db
        .selectFrom(values.as(sql`pair (source, target)`))
        .select([
          sql<string>`${sourceRef}`.as('source'),
          sql<string>`${targetRef}`.as('target'),
          (eb) =>
            eb
              .selectFrom('actor_block')
              .whereRef('actor_block.creator', '=', sourceRef)
              .whereRef('actor_block.subjectDid', '=', targetRef)
              .select('uri')
              .limit(1)
              .as('blocking'),
          (eb) =>
            eb
              .selectFrom('actor_block')
              .whereRef('actor_block.creator', '=', targetRef)
              .whereRef('actor_block.subjectDid', '=', sourceRef)
              .select('uri')
              .limit(1)
              .as('blockedBy'),
        ])
        .execute(),
    ])
    const allBlockLists = pairDids.flatMap(
      (did) => blockListsByCreator.get(did) ?? [],
    )
    const memberships = await getListMemberships(db, allBlockLists, pairDids)
    const withListBlocks = res.map((cur) => ({
      ...cur,
      blockingByList: firstListedIn(
        blockListsByCreator.get(cur.source) ?? [],
        cur.target,
        memberships,
      ),
      blockedByList: firstListedIn(
        blockListsByCreator.get(cur.target) ?? [],
        cur.source,
        memberships,
      ),
    }))
    const getKey = (a, b) => [a, b].sort().join(',')
    const lookup = withListBlocks.reduce((acc, cur) => {
      const key = getKey(cur.source, cur.target)
      return acc.set(key, cur)
    }, new Map<string, (typeof withListBlocks)[0]>())
    return {
      exists: pairs.map((pair) => {
        const item = lookup.get(getKey(pair.a, pair.b))
        if (!item) return false
        return !!(
          item.blocking ||
          item.blockedBy ||
          item.blockingByList ||
          item.blockedByList
        )
      }),
      blocks: pairs.map((pair) => {
        const item = lookup.get(getKey(pair.a, pair.b))
        if (!item) return {}
        return {
          blockedBy: item.blockedBy || undefined,
          blocking: item.blocking || undefined,
          blockedByList: item.blockedByList || undefined,
          blockingByList: item.blockingByList || undefined,
        }
      }),
    }
  },
})
