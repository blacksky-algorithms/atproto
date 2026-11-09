import type { ServiceImpl } from '@connectrpc/connect'
import { keyBy } from '@atproto/common'
import type { Service } from '../../../proto/bsky_connect.js'
import {
  FollowsFollowing,
  SampledFollowsFollowing,
} from '../../../proto/bsky_pb.js'
import type { Database } from '../db/index.js'
import { TimeCidKeyset, paginate } from '../db/pagination.js'
import { countAll } from '../db/util.js'

const RSKY_GRAPH_URL = process.env.RSKY_GRAPH_URL || ''
const RSKY_GRAPH_TIMEOUT_MS = Number(process.env.RSKY_GRAPH_TIMEOUT_MS || 80)

type RskyGraphFollowsFollowing = {
  results: Array<{ targetDid: string; dids: string[] }>
}

export async function fetchKnownFollowersFromRskyGraph(
  viewerDid: string,
  subjectDids: string[],
  baseUrl: string = RSKY_GRAPH_URL,
  timeoutMs: number = RSKY_GRAPH_TIMEOUT_MS,
): Promise<Map<string, string[]> | null> {
  if (!baseUrl) return null
  try {
    const url = new URL('/v1/follows-following', baseUrl)
    url.searchParams.set('viewer', viewerDid)
    url.searchParams.set('targets', subjectDids.join(','))
    const resp = await fetch(url, {
      signal: AbortSignal.timeout(timeoutMs),
    })
    if (!resp.ok) return null
    const json = (await resp.json()) as RskyGraphFollowsFollowing
    const byTarget = new Map<string, string[]>()
    for (const r of json.results || []) {
      byTarget.set(r.targetDid, r.dids || [])
    }
    return byTarget
  } catch {
    return null
  }
}

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getActorFollowsActors(req) {
    const { actorDid, targetDids } = req
    if (targetDids.length < 1) {
      return { uris: [] }
    }
    const res = await db.db
      .selectFrom('follow')
      .where('follow.creator', '=', actorDid)
      .where('follow.subjectDid', 'in', targetDids)
      .selectAll()
      .execute()
    const bySubject = keyBy(res, 'subjectDid')
    const uris = targetDids.map((did) => bySubject.get(did)?.uri ?? '')
    return {
      uris,
    }
  },
  async getFollowers(req) {
    const { actorDid, limit, cursor } = req
    const { ref } = db.db.dynamic
    let followersReq = db.db
      .selectFrom('follow')
      .where('follow.subjectDid', '=', actorDid)
      .innerJoin('actor as creator', 'creator.did', 'follow.creator')
      .selectAll('creator')
      .select([
        'follow.uri as uri',
        'follow.cid as cid',
        'follow.creator as creatorDid',
        'follow.subjectDid as subjectDid',
        'follow.sortAt as sortAt',
      ])

    const keyset = new TimeCidKeyset(ref('follow.sortAt'), ref('follow.cid'))
    followersReq = paginate(followersReq, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })

    const page = keyset.page(await followersReq.execute(), limit)
    return {
      followers: page.items.map((f) => ({
        uri: f.uri,
        actorDid: f.creatorDid,
        subjectDid: f.subjectDid,
      })),
      cursor: page.cursor,
    }
  },
  async getFollows(req) {
    const { actorDid, limit, cursor } = req
    const { ref } = db.db.dynamic

    let followsReq = db.db
      .selectFrom('follow')
      .where('follow.creator', '=', actorDid)
      .innerJoin('actor as subject', 'subject.did', 'follow.subjectDid')
      .selectAll('subject')
      .select([
        'follow.uri as uri',
        'follow.cid as cid',
        'follow.creator as creatorDid',
        'follow.subjectDid as subjectDid',
        'follow.sortAt as sortAt',
      ])

    const keyset = new TimeCidKeyset(ref('follow.sortAt'), ref('follow.cid'))
    followsReq = paginate(followsReq, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })

    const page = keyset.page(await followsReq.execute(), limit)

    return {
      follows: page.items.map((f) => ({
        uri: f.uri,
        actorDid: f.creatorDid,
        subjectDid: f.subjectDid,
      })),
      cursor: page.cursor,
    }
  },

  /**
   * Return known followers of a given actor.
   *
   * Example:
   *   - Alice follows Bob
   *   - Bob follows Dan
   *
   *   If Alice (the viewer) looks at Dan's profile (the subject), she should see that Bob follows Dan
   */
  async getFollowsFollowing(req) {
    const { actorDid: viewerDid, targetDids: subjectDids, limit, cursor } = req
    if (!subjectDids.length) {
      return { results: [] }
    }
    const fromRskyGraph = cursor
      ? null
      : await fetchKnownFollowersFromRskyGraph(viewerDid, subjectDids)
    const results = await Promise.all(
      subjectDids.map(async (subjectDid) => {
        const graphDids = fromRskyGraph?.get(subjectDid)
        if (graphDids && (!limit || graphDids.length <= limit)) {
          return new FollowsFollowing({
            targetDid: subjectDid,
            dids: graphDids,
          })
        }
        const { dids, cursor: nextCursor } = await getKnownFollowers(
          db,
          viewerDid,
          subjectDid,
          limit,
          cursor,
        )
        return new FollowsFollowing({
          targetDid: subjectDid,
          dids,
          cursor: nextCursor,
        })
      }),
    )
    return { results }
  },
  async sampleFollowsFollowing(req) {
    const { actorDid: viewerDid, targetDids: subjectDids, limit } = req
    if (!subjectDids.length) {
      return { results: [] }
    }
    const byTarget =
      (await fetchKnownFollowersFromRskyGraph(viewerDid, subjectDids)) ??
      (await getKnownFollowersBatch(db, viewerDid, subjectDids))
    return {
      results: subjectDids.map((did) => {
        const dids = byTarget.get(did) ?? []
        return new SampledFollowsFollowing({
          targetDid: did,
          dids: limit ? dids.slice(0, limit) : dids,
          totalKnown: dids.length,
        })
      }),
    }
  },
})

const getKnownFollowers = async (
  db: Database,
  viewerDid: string,
  subjectDid: string,
  limit?: number,
  cursor?: string,
) => {
  const followsReq = db.db
    .selectFrom('follow')
    .where('follow.creator', '=', viewerDid)
    .where(
      'follow.subjectDid',
      'in',
      db.db
        .selectFrom('follow')
        .where('follow.subjectDid', '=', subjectDid)
        .select(['creator']),
    )
    .select(['subjectDid', 'sortAt', 'cid'])

  const totalReq = followsReq
    .clearSelect()
    .select(countAll.as('count'))
    .executeTakeFirst()
  let pageReq = followsReq
  let pageLimit: number | undefined
  if (limit && limit > 0) {
    pageLimit = limit
    const { ref } = db.db.dynamic
    const keyset = new TimeCidKeyset(ref('follow.sortAt'), ref('follow.cid'))
    pageReq = paginate(pageReq, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })
  }

  const [rows, totalRow] = await Promise.all([pageReq.execute(), totalReq])
  const keyset = new TimeCidKeyset<{ sortAt: string; cid: string }>(
    db.db.dynamic.ref('follow.sortAt'),
    db.db.dynamic.ref('follow.cid'),
  )
  const page =
    pageLimit === undefined ? { items: rows } : keyset.page(rows, pageLimit)
  return {
    dids: page.items.map((row) => row.subjectDid),
    total: totalRow?.count ?? 0,
    cursor: page.cursor,
  }
}

// Single self-JOIN for every subject at once. Ordering by the viewer's follow
// cursor, newest first like the paginated query above, is served by
// follow_creator_cursor_idx (creator, sortAt, cid) and keeps the sampled
// known-followers list stable between runs.
const getKnownFollowersBatch = async (
  db: Database,
  viewerDid: string,
  subjectDids: string[],
): Promise<Map<string, string[]>> => {
  const rows = await db.db
    .selectFrom('follow as viewer_follow')
    .innerJoin('follow as target_follower', (join) =>
      join
        .onRef('target_follower.creator', '=', 'viewer_follow.subjectDid')
        .on('target_follower.subjectDid', 'in', subjectDids),
    )
    .where('viewer_follow.creator', '=', viewerDid)
    .select([
      'target_follower.subjectDid as targetDid',
      'viewer_follow.subjectDid as mutualDid',
    ])
    .orderBy('viewer_follow.sortAt', 'desc')
    .orderBy('viewer_follow.cid', 'desc')
    .execute()
  const byTarget = new Map<string, string[]>()
  for (const row of rows) {
    const list = byTarget.get(row.targetDid) ?? []
    list.push(row.mutualDid)
    byTarget.set(row.targetDid, list)
  }
  return byTarget
}
