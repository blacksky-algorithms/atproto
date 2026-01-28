import { Timestamp } from '@bufbuild/protobuf'
import { ServiceImpl } from '@connectrpc/connect'
import * as ui8 from 'uint8arrays'
import { keyBy } from '@atproto/common'
import { AtUri } from '@atproto/syntax'
import { ids } from '../../../lexicon/lexicons'
import { Service } from '../../../proto/bsky_connect'
import { PostRecordMeta, Record } from '../../../proto/bsky_pb'
import { CachedPostMeta, CachedRecord, PostMetaCache, RecordCache } from '../cache'
import { Database } from '../db'

type DbRow = {
  uri: string
  cid: string | null
  did: string | null
  json: string | null
  indexedAt: string | null
  takedownRef: string | null
  tags: string[] | null
  rev: string | null
}

function rowToRecord(uri: string, row: DbRow | undefined): Record {
  // Handle null, undefined, and empty string json values
  // Sanitize control characters that break JSON.parse - in JSON strings,
  // newlines/tabs/returns must be escaped (\n \t \r), not literal bytes
  let json = row?.json && row.json.length > 0 ? row.json : JSON.stringify(null)
  // Replace literal newlines/tabs/returns with their escaped forms
  json = json.replace(/\t/g, '\\t').replace(/\n/g, '\\n').replace(/\r/g, '\\r')
  // Remove other control characters (0x00-0x1f except the ones we just escaped)
  // eslint-disable-next-line no-control-regex
  json = json.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f]/g, '')
  // Also remove JSON-escaped control characters like \u0000
  json = json.replace(/\\u00[01][0-9a-fA-F]/g, '')

  // Safely parse JSON - if it still fails, return an empty record
  let parsed: { [key: string]: unknown } | null = null
  try {
    parsed = JSON.parse(json)
  } catch (err) {
    console.error(`[dataplane] rowToRecord JSON parse error for uri=${uri}:`, err)
    // Return empty record for malformed JSON
    json = JSON.stringify(null)
  }

  const createdAtRaw = new Date((parsed?.['createdAt'] as string | undefined) ?? '')
  const createdAt = !isNaN(createdAtRaw.getTime())
    ? Timestamp.fromDate(createdAtRaw)
    : undefined
  const indexedAt = row?.indexedAt
    ? Timestamp.fromDate(new Date(row.indexedAt))
    : undefined
  const recordBytes = ui8.fromString(json, 'utf8')
  return new Record({
    record: recordBytes,
    cid: row?.cid ?? undefined,
    createdAt,
    indexedAt,
    sortedAt: compositeTime(createdAt, indexedAt),
    takenDown: !!row?.takedownRef,
    takedownRef: row?.takedownRef ?? undefined,
    tags: row?.tags ?? undefined,
  })
}

function cachedToRow(cached: CachedRecord): DbRow {
  return {
    uri: cached.uri,
    cid: cached.cid,
    did: cached.did,
    json: cached.json,
    indexedAt: cached.indexedAt,
    takedownRef: cached.takedownRef,
    tags: cached.tags,
    rev: cached.rev,
  }
}

function rowToCached(row: DbRow): CachedRecord {
  return {
    uri: row.uri,
    cid: row.cid,
    did: row.did,
    json: row.json,
    indexedAt: row.indexedAt,
    takedownRef: row.takedownRef,
    tags: row.tags,
    rev: row.rev,
  }
}

export default (
  db: Database,
  recordCache?: RecordCache,
  postMetaCache?: PostMetaCache,
): Partial<ServiceImpl<typeof Service>> => ({
  getBlockRecords: getRecords(db, ids.AppBskyGraphBlock, recordCache),
  getFeedGeneratorRecords: getRecords(db, ids.AppBskyFeedGenerator, recordCache),
  getFollowRecords: getRecords(db, ids.AppBskyGraphFollow, recordCache),
  getLikeRecords: getRecords(db, ids.AppBskyFeedLike, recordCache),
  getListBlockRecords: getRecords(db, ids.AppBskyGraphListblock, recordCache),
  getListItemRecords: getRecords(db, ids.AppBskyGraphListitem, recordCache),
  getListRecords: getRecords(db, ids.AppBskyGraphList, recordCache),
  getPostRecords: getPostRecords(db, recordCache, postMetaCache),
  getProfileRecords: getRecords(db, ids.AppBskyActorProfile, recordCache),
  getRepostRecords: getRecords(db, ids.AppBskyFeedRepost, recordCache),
  getThreadGateRecords: getRecords(db, ids.AppBskyFeedThreadgate, recordCache),
  getPostgateRecords: getRecords(db, ids.AppBskyFeedPostgate, recordCache),
  getLabelerRecords: getRecords(db, ids.AppBskyLabelerService, recordCache),
  getActorChatDeclarationRecords: getRecords(
    db,
    ids.ChatBskyActorDeclaration,
    recordCache,
  ),
  getNotificationDeclarationRecords: getRecords(
    db,
    ids.AppBskyNotificationDeclaration,
    recordCache,
  ),
  getStarterPackRecords: getRecords(db, ids.AppBskyGraphStarterpack, recordCache),
  getVerificationRecords: getRecords(db, ids.AppBskyGraphVerification, recordCache),
  getStatusRecords: getRecords(db, ids.AppBskyActorStatus, recordCache),
})

export const getRecords =
  (db: Database, collection?: string, recordCache?: RecordCache) =>
  async (req: { uris: string[] }): Promise<{ records: Record[] }> => {
    try {
      console.log(`[dataplane] getRecords START collection=${collection} uris=${req.uris.length}`)
      const validUris = collection
        ? req.uris.filter((uri) => new AtUri(uri).collection === collection)
        : req.uris

      if (validUris.length === 0) {
        console.log(`[dataplane] getRecords no valid URIs`)
        return { records: req.uris.map((uri) => rowToRecord(uri, undefined)) }
      }

      // If no cache, fetch all from DB
      if (!recordCache) {
        console.log(`[dataplane] getRecords no cache, fetching from DB`)
        const res = await db.db
          .selectFrom('record')
          .selectAll()
          .where('uri', 'in', validUris)
          .execute()
        const byUri = keyBy(res, 'uri')
        const records: Record[] = req.uris.map((uri) =>
          rowToRecord(uri, byUri.get(uri) as DbRow | undefined),
        )
        console.log(`[dataplane] getRecords DONE (no cache) records=${records.length}`)
        return { records }
      }

      // Check cache first
      console.log(`[dataplane] getRecords checking cache for ${validUris.length} URIs`)
      const cached = await recordCache.getMany(validUris)
      const cacheMisses = validUris.filter((uri) => !cached.has(uri))
      console.log(`[dataplane] getRecords cache hits=${cached.size} misses=${cacheMisses.length}`)

      // Fetch cache misses from DB
      let fetched = new Map<string, DbRow>()
      if (cacheMisses.length > 0) {
        console.log(`[dataplane] getRecords fetching ${cacheMisses.length} from DB`)
        const res = await db.db
          .selectFrom('record')
          .selectAll()
          .where('uri', 'in', cacheMisses)
          .execute()
        fetched = new Map(res.map((row) => [row.uri, row as DbRow]))
        console.log(`[dataplane] getRecords fetched ${fetched.size} from DB`)

        // Cache the fetched rows in background
        const toCache = new Map<string, CachedRecord>()
        for (const [uri, row] of fetched) {
          toCache.set(uri, rowToCached(row))
        }
        recordCache.setMany(toCache).catch((err) => {
          console.error(`[dataplane] getRecords cache set error:`, err)
        })
      }

      // Merge results and build records
      const records: Record[] = req.uris.map((uri) => {
        const fromCache = cached.get(uri)
        if (fromCache) {
          return rowToRecord(uri, cachedToRow(fromCache))
        }
        return rowToRecord(uri, fetched.get(uri))
      })

      console.log(`[dataplane] getRecords DONE records=${records.length}`)
      return { records }
    } catch (err) {
      console.error(`[dataplane] getRecords ERROR collection=${collection}:`, err)
      throw err
    }
  }

export const getPostRecords = (
  db: Database,
  recordCache?: RecordCache,
  postMetaCache?: PostMetaCache,
) => {
  const getBaseRecords = getRecords(db, ids.AppBskyFeedPost, recordCache)
  return async (req: {
    uris: string[]
  }): Promise<{ records: Record[]; meta: PostRecordMeta[] }> => {
    if (req.uris.length === 0) {
      return { records: [], meta: [] }
    }

    // If no cache, fetch all from DB
    if (!postMetaCache) {
      const [{ records }, details] = await Promise.all([
        getBaseRecords(req),
        db.db
          .selectFrom('post')
          .where('uri', 'in', req.uris)
          .select([
            'uri',
            'violatesThreadGate',
            'violatesEmbeddingRules',
            'hasThreadGate',
            'hasPostGate',
          ])
          .execute(),
      ])
      const byKey = keyBy(details, 'uri')
      const meta = req.uris.map((uri) => {
        return new PostRecordMeta({
          violatesThreadGate: !!byKey.get(uri)?.violatesThreadGate,
          violatesEmbeddingRules: !!byKey.get(uri)?.violatesEmbeddingRules,
          hasThreadGate: !!byKey.get(uri)?.hasThreadGate,
          hasPostGate: !!byKey.get(uri)?.hasPostGate,
        })
      })
      return { records, meta }
    }

    // Check cache first
    const cached = await postMetaCache.getMany(req.uris)
    const cacheMisses = req.uris.filter((uri) => !cached.has(uri))

    // Fetch records and cache misses in parallel
    const [{ records }, details] = await Promise.all([
      getBaseRecords(req),
      cacheMisses.length > 0
        ? db.db
            .selectFrom('post')
            .where('uri', 'in', cacheMisses)
            .select([
              'uri',
              'violatesThreadGate',
              'violatesEmbeddingRules',
              'hasThreadGate',
              'hasPostGate',
            ])
            .execute()
        : [],
    ])
    const byKey = keyBy(details, 'uri')

    // Cache the fetched post meta in background
    if (cacheMisses.length > 0) {
      const toCache = new Map<string, CachedPostMeta>()
      for (const uri of cacheMisses) {
        const row = byKey.get(uri)
        toCache.set(uri, {
          violatesThreadGate: !!row?.violatesThreadGate,
          violatesEmbeddingRules: !!row?.violatesEmbeddingRules,
          hasThreadGate: !!row?.hasThreadGate,
          hasPostGate: !!row?.hasPostGate,
        })
      }
      postMetaCache.setMany(toCache).catch(() => {
        // Ignore cache errors
      })
    }

    // Build meta from cache and fetched data
    const meta = req.uris.map((uri) => {
      const fromCache = cached.get(uri)
      if (fromCache) {
        return new PostRecordMeta({
          violatesThreadGate: fromCache.violatesThreadGate,
          violatesEmbeddingRules: fromCache.violatesEmbeddingRules,
          hasThreadGate: fromCache.hasThreadGate,
          hasPostGate: fromCache.hasPostGate,
        })
      }
      const row = byKey.get(uri)
      return new PostRecordMeta({
        violatesThreadGate: !!row?.violatesThreadGate,
        violatesEmbeddingRules: !!row?.violatesEmbeddingRules,
        hasThreadGate: !!row?.hasThreadGate,
        hasPostGate: !!row?.hasPostGate,
      })
    })
    return { records, meta }
  }
}

const compositeTime = (
  ts1: Timestamp | undefined,
  ts2: Timestamp | undefined,
) => {
  if (!ts1) return ts2
  if (!ts2) return ts1
  return ts1.toDate() < ts2.toDate() ? ts1 : ts2
}
