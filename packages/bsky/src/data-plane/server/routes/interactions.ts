import { ServiceImpl } from '@connectrpc/connect'
import { DAY, keyBy } from '@atproto/common'
import { Service } from '../../../proto/bsky_connect'
import { CachedInteraction, InteractionCache } from '../cache'
import { Database } from '../db'
import { countAll } from '../db/util'

// Helper function to fetch interactions from database
async function fetchInteractionsFromDb(db: Database, uris: string[]) {
  if (uris.length === 0) {
    return new Map<string, CachedInteraction>()
  }
  const res = await db.db
    .selectFrom('post_agg')
    .where('uri', 'in', uris)
    .selectAll()
    .execute()
  const byUri = keyBy(res, 'uri')
  const result = new Map<string, CachedInteraction>()
  for (const uri of uris) {
    const row = byUri.get(uri)
    result.set(uri, {
      likeCount: row?.likeCount ?? 0,
      replyCount: row?.replyCount ?? 0,
      repostCount: row?.repostCount ?? 0,
      quoteCount: row?.quoteCount ?? 0,
      bookmarkCount: row?.bookmarkCount ?? 0,
    })
  }
  return result
}

export default (
  db: Database,
  interactionCache?: InteractionCache,
): Partial<ServiceImpl<typeof Service>> => ({
  async getInteractionCounts(req) {
    const uris = req.refs.map((ref) => ref.uri)
    if (uris.length === 0) {
      return { likes: [], replies: [], reposts: [], quotes: [], bookmarks: [] }
    }

    // If no cache, fetch all from DB
    if (!interactionCache) {
      const interactions = await fetchInteractionsFromDb(db, uris)
      return {
        likes: uris.map((uri) => interactions.get(uri)?.likeCount ?? 0),
        replies: uris.map((uri) => interactions.get(uri)?.replyCount ?? 0),
        reposts: uris.map((uri) => interactions.get(uri)?.repostCount ?? 0),
        quotes: uris.map((uri) => interactions.get(uri)?.quoteCount ?? 0),
        bookmarks: uris.map((uri) => interactions.get(uri)?.bookmarkCount ?? 0),
      }
    }

    // Check cache first
    const cached = await interactionCache.getMany(uris)
    const cacheMisses = uris.filter((uri) => !cached.has(uri))

    // Fetch cache misses from DB
    const fetched = await fetchInteractionsFromDb(db, cacheMisses)

    // Cache the misses in background
    if (cacheMisses.length > 0) {
      interactionCache.setMany(fetched).catch(() => {
        // Ignore cache errors
      })
    }

    // Merge results
    const merged = new Map<string, CachedInteraction>()
    for (const uri of uris) {
      const fromCache = cached.get(uri)
      if (fromCache) {
        merged.set(uri, fromCache)
      } else {
        merged.set(uri, fetched.get(uri) ?? {
          likeCount: 0,
          replyCount: 0,
          repostCount: 0,
          quoteCount: 0,
          bookmarkCount: 0,
        })
      }
    }

    return {
      likes: uris.map((uri) => merged.get(uri)?.likeCount ?? 0),
      replies: uris.map((uri) => merged.get(uri)?.replyCount ?? 0),
      reposts: uris.map((uri) => merged.get(uri)?.repostCount ?? 0),
      quotes: uris.map((uri) => merged.get(uri)?.quoteCount ?? 0),
      bookmarks: uris.map((uri) => merged.get(uri)?.bookmarkCount ?? 0),
    }
  },
  async getCountsForUsers(req) {
    if (req.dids.length === 0) {
      return {}
    }
    const { ref } = db.db.dynamic
    const res = await db.db
      .selectFrom('profile_agg')
      .where('did', 'in', req.dids)
      .selectAll('profile_agg')
      .select([
        db.db
          .selectFrom('feed_generator')
          .whereRef('creator', '=', ref('profile_agg.did'))
          .select(countAll.as('val'))
          .as('feedGensCount'),
        db.db
          .selectFrom('list')
          .whereRef('creator', '=', ref('profile_agg.did'))
          .select(countAll.as('val'))
          .as('listsCount'),
        db.db
          .selectFrom('starter_pack')
          .whereRef('creator', '=', ref('profile_agg.did'))
          .select(countAll.as('val'))
          .as('starterPacksCount'),
      ])
      .execute()
    const byDid = keyBy(res, 'did')
    return {
      followers: req.dids.map((uri) => byDid.get(uri)?.followersCount ?? 0),
      following: req.dids.map((uri) => byDid.get(uri)?.followsCount ?? 0),
      posts: req.dids.map((uri) => byDid.get(uri)?.postsCount ?? 0),
      lists: req.dids.map((uri) => byDid.get(uri)?.listsCount ?? 0),
      feeds: req.dids.map((uri) => byDid.get(uri)?.feedGensCount ?? 0),
      starterPacks: req.dids.map(
        (uri) => byDid.get(uri)?.starterPacksCount ?? 0,
      ),
    }
  },
  async getStarterPackCounts(req) {
    const weekAgo = new Date(Date.now() - 7 * DAY)
    const uris = req.refs.map((ref) => ref.uri)
    if (uris.length === 0) {
      return { joinedAllTime: [], joinedWeek: [] }
    }
    const countsAllTime = await db.db
      .selectFrom('profile')
      .where('joinedViaStarterPackUri', 'in', uris)
      .select(['joinedViaStarterPackUri as uri', countAll.as('count')])
      .groupBy('joinedViaStarterPackUri')
      .execute()
    const countsWeek = await db.db
      .selectFrom('profile')
      .where('joinedViaStarterPackUri', 'in', uris)
      .where('createdAt', '>', weekAgo.toISOString())
      .select(['joinedViaStarterPackUri as uri', countAll.as('count')])
      .groupBy('joinedViaStarterPackUri')
      .execute()
    const countsWeekByUri = countsWeek.reduce((cur, item) => {
      if (!item.uri) return cur
      return cur.set(item.uri, item.count)
    }, new Map<string, number>())
    const countsAllTimeByUri = countsAllTime.reduce((cur, item) => {
      if (!item.uri) return cur
      return cur.set(item.uri, item.count)
    }, new Map<string, number>())
    return {
      joinedWeek: uris.map((uri) => countsWeekByUri.get(uri) ?? 0),
      joinedAllTime: uris.map((uri) => countsAllTimeByUri.get(uri) ?? 0),
    }
  },
  async getListCounts(req) {
    const uris = req.refs.map((ref) => ref.uri)
    if (uris.length === 0) {
      return { listItems: [] }
    }
    const countsListItems = await db.db
      .selectFrom('list_item')
      .where('listUri', 'in', uris)
      .select(['listUri as uri', countAll.as('count')])
      .groupBy('listUri')
      .execute()
    const countsByUri = keyBy(countsListItems, 'uri')
    return {
      listItems: uris.map((uri) => countsByUri.get(uri)?.count ?? 0),
    }
  },
})
