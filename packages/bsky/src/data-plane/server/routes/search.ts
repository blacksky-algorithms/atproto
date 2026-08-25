import type { ServiceImpl } from '@connectrpc/connect'
import type { Service } from '../../../proto/bsky_connect.js'
import type { Database } from '../db/index.js'
import {
  IndexedAtDidKeyset,
  TimeCidKeyset,
  paginate,
} from '../db/pagination.js'
import { parsePostSearchQuery } from '../util.js'

// When a search service is configured, proxy queries to its skeleton
// endpoints (ranked, index-backed) and keep the SQL below as the fallback:
// LIKE scans over multi-TB tables cannot be allowed to serve production search.
const searchUrl =
  process.env.BSKY_SEARCH_URL || process.env.BSKY_SEARCH_ENDPOINT || undefined

const palomarGet = async (
  path: string,
  params: Record<string, string | number | undefined>,
): Promise<Record<string, unknown> | undefined> => {
  if (!searchUrl) return undefined
  const url = new URL(`/xrpc/${path}`, searchUrl)
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== '') url.searchParams.set(k, String(v))
  }
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) })
    if (!res.ok) return undefined
    return (await res.json()) as Record<string, unknown>
  } catch {
    return undefined
  }
}

export default (db: Database): Partial<ServiceImpl<typeof Service>> => {
  const searchActorsImpl = async (req: {
    term: string
    limit: number
    cursor?: string
    typeahead?: boolean
  }) => {
    const { term, limit, cursor } = req
    const fromPalomar = await palomarGet(
      'app.bsky.unspecced.searchActorsSkeleton',
      {
        q: term,
        limit,
        cursor,
        typeahead: req.typeahead ? 'true' : undefined,
      },
    )
    if (fromPalomar && Array.isArray(fromPalomar['actors'])) {
      const actors = fromPalomar['actors'] as { did?: string }[]
      return {
        dids: actors.flatMap((a) => (a.did ? [a.did] : [])),
        cursor:
          typeof fromPalomar['cursor'] === 'string'
            ? fromPalomar['cursor']
            : undefined,
      }
    }
    const { ref } = db.db.dynamic
    let builder = db.db
      .selectFrom('actor')
      .where('actor.handle', 'like', `%${cleanQuery(term)}%`)
      .selectAll()

    const keyset = new IndexedAtDidKeyset(
      ref('actor.indexedAt'),
      ref('actor.did'),
    )
    builder = paginate(builder, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })

    const res = await builder.execute()

    return {
      dids: res.map((row) => row.did),
      cursor: keyset.packFromResult(res),
    }
  }

  const searchPostsImpl = async (req: {
    term: string
    limit: number
    cursor?: string
  }) => {
    const { term, limit, cursor } = req
    const fromPalomar = await palomarGet(
      'app.bsky.unspecced.searchPostsSkeleton',
      { q: term, limit, cursor },
    )
    if (fromPalomar && Array.isArray(fromPalomar['posts'])) {
      const posts = fromPalomar['posts'] as { uri?: string }[]
      return {
        uris: posts.flatMap((p) => (p.uri ? [p.uri] : [])),
        cursor:
          typeof fromPalomar['cursor'] === 'string'
            ? fromPalomar['cursor']
            : undefined,
      }
    }
    const { q, author } = parsePostSearchQuery(term)

    let authorDid = author
    if (author && !author?.startsWith('did:')) {
      const res = await db.db
        .selectFrom('actor')
        .where('handle', '=', author)
        .selectAll()
        .executeTakeFirst()
      authorDid = res?.did
    }

    const { ref } = db.db.dynamic
    let builder = db.db
      .selectFrom('post')
      .where('post.text', 'like', `%${q}%`)
      .selectAll()

    if (authorDid) {
      builder = builder.where('post.creator', '=', authorDid)
    }

    const keyset = new TimeCidKeyset(ref('post.sortAt'), ref('post.cid'))
    builder = paginate(builder, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })

    const res = await builder.execute()
    return {
      uris: res.map((row) => row.uri),
      cursor: keyset.packFromResult(res),
    }
  }

  const searchStarterPacksImpl = async (req: {
    term: string
    limit: number
    cursor?: string
  }) => {
    const { term, limit, cursor } = req
    const { ref } = db.db.dynamic
    let builder = db.db
      .selectFrom('starter_pack')
      .where('starter_pack.name', 'ilike', `%${term}%`)
      .selectAll()

    const keyset = new TimeCidKeyset(
      ref('starter_pack.sortAt'),
      ref('starter_pack.cid'),
    )

    builder = paginate(builder, {
      limit,
      cursor,
      keyset,
      tryIndex: true,
    })

    const res = await builder.execute()

    return {
      uris: res.map((row) => row.uri),
      cursor: keyset.packFromResult(res),
    }
  }

  return {
    // @TODO actor search endpoints still fall back to search service
    searchActors: searchActorsImpl,

    // @TODO post search endpoint still falls back to search service
    searchPosts: searchPostsImpl,

    searchStarterPacks: searchStarterPacksImpl,

    // V2 endpoints reuse the V1 SQL for dev env and reshape the response.
    async searchActorsV2(req) {
      const { dids, cursor } = await searchActorsImpl({
        term: req.params?.query ?? '',
        limit: req.params?.limit ?? 25,
        cursor: req.params?.cursor,
      })
      return {
        actors: dids.map((did) => ({ did, score: 0 })),
        pageInfo: { cursor: cursor ?? '', hitsTotal: 0n },
      }
    },

    async searchActorsTypeahead(req) {
      const { dids } = await searchActorsImpl({
        term: req.params?.query ?? '',
        limit: req.params?.limit || 10,
        typeahead: true,
      })
      return {
        actors: dids.map((did) => ({ did, score: 0 })),
      }
    },

    async searchPostsV2(req) {
      const author = req.filters?.authors?.[0]
      const baseQuery = req.params?.query ?? ''
      const term = author ? `${baseQuery} from:${author}` : baseQuery
      const { uris, cursor } = await searchPostsImpl({
        term,
        limit: req.params?.limit ?? 25,
        cursor: req.params?.cursor,
      })
      return {
        posts: uris.map((uri) => ({ uri, score: 0 })),
        pageInfo: { cursor: cursor ?? '', hitsTotal: 0n },
        detectedQueryLanguages: [],
      }
    },

    async searchStarterPacksV2(req) {
      const { uris, cursor } = await searchStarterPacksImpl({
        term: req.params?.query ?? '',
        limit: req.params?.limit ?? 25,
        cursor: req.params?.cursor,
      })
      return {
        starterPacks: uris.map((uri) => ({ uri, score: 0 })),
        pageInfo: { cursor: cursor ?? '', hitsTotal: 0n },
      }
    },
  }
}

// Remove leading @ in case a handle is input that way
const cleanQuery = (query: string) => query.trim().replace(/^@/g, '')
