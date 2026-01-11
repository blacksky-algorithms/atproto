import { Timestamp } from '@bufbuild/protobuf'
import { ServiceImpl } from '@connectrpc/connect'
import * as ui8 from 'uint8arrays'
import { keyBy } from '@atproto/common'
import { AtUri } from '@atproto/syntax'
import { ids } from '../../../lexicon/lexicons'
import { Service } from '../../../proto/bsky_connect'
import { PostRecordMeta, Record } from '../../../proto/bsky_pb'
import { CachedRecord, RecordCache } from '../cache'
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
  const json = row?.json ?? JSON.stringify(null)
  const createdAtRaw = new Date(JSON.parse(json)?.['createdAt'])
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
): Partial<ServiceImpl<typeof Service>> => ({
  getBlockRecords: getRecords(db, ids.AppBskyGraphBlock, recordCache),
  getFeedGeneratorRecords: getRecords(db, ids.AppBskyFeedGenerator, recordCache),
  getFollowRecords: getRecords(db, ids.AppBskyGraphFollow, recordCache),
  getLikeRecords: getRecords(db, ids.AppBskyFeedLike, recordCache),
  getListBlockRecords: getRecords(db, ids.AppBskyGraphListblock, recordCache),
  getListItemRecords: getRecords(db, ids.AppBskyGraphListitem, recordCache),
  getListRecords: getRecords(db, ids.AppBskyGraphList, recordCache),
  getPostRecords: getPostRecords(db, recordCache),
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
    const validUris = collection
      ? req.uris.filter((uri) => new AtUri(uri).collection === collection)
      : req.uris

    if (validUris.length === 0) {
      return { records: req.uris.map((uri) => rowToRecord(uri, undefined)) }
    }

    // If no cache, fetch all from DB
    if (!recordCache) {
      const res = await db.db
        .selectFrom('record')
        .selectAll()
        .where('uri', 'in', validUris)
        .execute()
      const byUri = keyBy(res, 'uri')
      const records: Record[] = req.uris.map((uri) =>
        rowToRecord(uri, byUri.get(uri) as DbRow | undefined),
      )
      return { records }
    }

    // Check cache first
    const cached = await recordCache.getMany(validUris)
    const cacheMisses = validUris.filter((uri) => !cached.has(uri))

    // Fetch cache misses from DB
    let fetched = new Map<string, DbRow>()
    if (cacheMisses.length > 0) {
      const res = await db.db
        .selectFrom('record')
        .selectAll()
        .where('uri', 'in', cacheMisses)
        .execute()
      fetched = new Map(res.map((row) => [row.uri, row as DbRow]))

      // Cache the fetched rows in background
      const toCache = new Map<string, CachedRecord>()
      for (const [uri, row] of fetched) {
        toCache.set(uri, rowToCached(row))
      }
      recordCache.setMany(toCache).catch(() => {
        // Ignore cache errors
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

    return { records }
  }

export const getPostRecords = (db: Database, recordCache?: RecordCache) => {
  const getBaseRecords = getRecords(db, ids.AppBskyFeedPost, recordCache)
  return async (req: {
    uris: string[]
  }): Promise<{ records: Record[]; meta: PostRecordMeta[] }> => {
    const [{ records }, details] = await Promise.all([
      getBaseRecords(req),
      req.uris.length
        ? await db.db
            .selectFrom('post')
            .where('uri', 'in', req.uris)
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
}

const compositeTime = (
  ts1: Timestamp | undefined,
  ts2: Timestamp | undefined,
) => {
  if (!ts1) return ts2
  if (!ts2) return ts1
  return ts1.toDate() < ts2.toDate() ? ts1 : ts2
}
