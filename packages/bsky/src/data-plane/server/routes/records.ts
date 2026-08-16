import { Timestamp } from '@bufbuild/protobuf'
import { ServiceImpl } from '@connectrpc/connect'
import * as ui8 from 'uint8arrays'
import { keyBy } from '@atproto/common'
import { l } from '@atproto/lex'
import { AtUri } from '@atproto/syntax'
import { app, chat, com } from '../../../lexicons/index.js'
import { dataplaneLogger } from '../../../logger.js'
import { Service } from '../../../proto/bsky_connect.js'
import { PostRecordMeta, Record } from '../../../proto/bsky_pb.js'
import { Database } from '../db/index.js'

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  getBlockRecords: getBlockRecordsSynthesized(db),
  getFeedGeneratorRecords: getRecords(db, app.bsky.feed.generator),
  getFollowRecords: getFollowRecordsSynthesized(db),
  getLikeRecords: getLikeRecordsSynthesized(db),
  getListBlockRecords: getRecords(db, app.bsky.graph.listblock),
  getListItemRecords: getRecords(db, app.bsky.graph.listitem),
  getListRecords: getRecords(db, app.bsky.graph.list),
  getPostRecords: getPostRecords(db),
  getProfileRecords: getRecords(db, app.bsky.actor.profile),
  getRepostRecords: getRepostRecordsSynthesized(db),
  getThreadGateRecords: getRecords(db, app.bsky.feed.threadgate),
  getPostgateRecords: getRecords(db, app.bsky.feed.postgate),
  getLabelerRecords: getRecords(db, app.bsky.labeler.service),
  getActorChatDeclarationRecords: getRecords(db, chat.bsky.actor.declaration),
  getNotificationDeclarationRecords: getRecords(
    db,
    app.bsky.notification.declaration,
  ),
  getGermDeclarationRecords: getRecords(db, com.germnetwork.declaration),
  getStarterPackRecords: getRecords(db, app.bsky.graph.starterpack),
  getVerificationRecords: getRecords(db, app.bsky.graph.verification),
  getStatusRecords: getRecords(db, app.bsky.actor.status),
})

// invalid json rows exist in the wild and must hydrate as missing records
// rather than throw
const parseRecordJson = (uri: string, json: string): any => {
  try {
    return JSON.parse(json)
  } catch (err) {
    dataplaneLogger.error({ err, uri }, 'invalid record json in db')
    return null
  }
}

export const getRecords = (db: Database, ns?: l.Main<l.RecordSchema>) => {
  const collection = ns ? l.getMain(ns).$type : undefined

  return async (req: { uris: string[] }): Promise<{ records: Record[] }> => {
    const validUris = collection
      ? req.uris.filter((uri) => new AtUri(uri).collection === collection)
      : req.uris
    const res = validUris.length
      ? await db.db
          .selectFrom('record')
          .selectAll()
          .where('uri', 'in', validUris)
          .execute()
      : []
    const byUri = keyBy(res, 'uri')
    const records: Record[] = req.uris.map((uri) => {
      const row = byUri.get(uri)
      const parsed = row ? parseRecordJson(uri, row.json) : null
      if (!row || parsed === null) {
        return new Record({
          record: ui8.fromString(
            JSON.stringify(null),
            'utf8',
          ) as Uint8Array<ArrayBuffer>,
        })
      }
      const createdAtRaw = new Date(parsed?.['createdAt'])
      const createdAt = !isNaN(createdAtRaw.getTime())
        ? Timestamp.fromDate(createdAtRaw)
        : undefined
      const indexedAt = row.indexedAt
        ? Timestamp.fromDate(new Date(row.indexedAt))
        : undefined
      const recordBytes = ui8.fromString(row.json, 'utf8')
      return new Record({
        record: recordBytes as Uint8Array<ArrayBuffer>,
        cid: row.cid,
        createdAt,
        indexedAt,
        sortedAt: compositeTime(createdAt, indexedAt),
        takenDown: !!row.takedownRef,
        takedownRef: row.takedownRef ?? undefined,
        tags: row.tags ?? undefined,
      })
    })
    return { records }
  }
}

// Like/follow/repost/block records are synthesized from the typed tables
// instead of read from the record store: their content is fully reconstructible
// (subject, createdAt, via attribution), which lets the record table drop those
// rows. Raw createdAt byte-fidelity and unknown extra fields are not preserved,
// so the returned value is not re-hashable to `cid` (which stays the original
// commit cid from index time).

type SynthesizedRow = {
  uri: string
  cid: string
  createdAt: string
  indexedAt: string
  takedownRef: string | null
  value: { [k: string]: unknown }
}

const synthesizedRecords = (
  collection: string,
  fetchRows: (uris: string[]) => Promise<SynthesizedRow[]>,
) => {
  return async (req: { uris: string[] }): Promise<{ records: Record[] }> => {
    const validUris = req.uris.filter(
      (uri) => new AtUri(uri).collection === collection,
    )
    const rows = validUris.length ? await fetchRows(validUris) : []
    const byUri = keyBy(rows, 'uri')
    const records: Record[] = req.uris.map((uri) => {
      const row = byUri.get(uri)
      if (!row) {
        return new Record({
          record: ui8.fromString(
            JSON.stringify(null),
            'utf8',
          ) as Uint8Array<ArrayBuffer>,
        })
      }
      const createdAtRaw = new Date(row.createdAt)
      const createdAt = !isNaN(createdAtRaw.getTime())
        ? Timestamp.fromDate(createdAtRaw)
        : undefined
      const indexedAt = row.indexedAt
        ? Timestamp.fromDate(new Date(row.indexedAt))
        : undefined
      return new Record({
        record: ui8.fromString(
          JSON.stringify(row.value),
          'utf8',
        ) as Uint8Array<ArrayBuffer>,
        cid: row.cid,
        createdAt,
        indexedAt,
        sortedAt: compositeTime(createdAt, indexedAt),
        takenDown: !!row.takedownRef,
        takedownRef: row.takedownRef ?? undefined,
      })
    })
    return { records }
  }
}

const viaRef = (via: string | null, viaCid: string | null) =>
  via ? { via: viaCid ? { uri: via, cid: viaCid } : { uri: via } } : {}

export const getLikeRecordsSynthesized = (db: Database) =>
  synthesizedRecords('app.bsky.feed.like', async (uris) => {
    const rows = await db.db
      .selectFrom('like')
      .select([
        'uri',
        'cid',
        'subject',
        'subjectCid',
        'via',
        'viaCid',
        'createdAt',
        'indexedAt',
        'takedownRef',
      ])
      .where('uri', 'in', uris)
      .where('space_uri', 'is', null)
      .execute()
    return rows.map((r) => ({
      uri: r.uri,
      cid: r.cid,
      createdAt: r.createdAt,
      indexedAt: r.indexedAt,
      takedownRef: r.takedownRef,
      value: {
        $type: 'app.bsky.feed.like',
        subject: { uri: r.subject, cid: r.subjectCid },
        createdAt: r.createdAt,
        ...viaRef(r.via, r.viaCid),
      },
    }))
  })

export const getRepostRecordsSynthesized = (db: Database) =>
  synthesizedRecords('app.bsky.feed.repost', async (uris) => {
    const rows = await db.db
      .selectFrom('repost')
      .select([
        'uri',
        'cid',
        'subject',
        'subjectCid',
        'via',
        'viaCid',
        'createdAt',
        'indexedAt',
        'takedownRef',
      ])
      .where('uri', 'in', uris)
      .execute()
    return rows.map((r) => ({
      uri: r.uri,
      cid: r.cid,
      createdAt: r.createdAt,
      indexedAt: r.indexedAt,
      takedownRef: r.takedownRef,
      value: {
        $type: 'app.bsky.feed.repost',
        subject: { uri: r.subject, cid: r.subjectCid },
        createdAt: r.createdAt,
        ...viaRef(r.via, r.viaCid),
      },
    }))
  })

export const getFollowRecordsSynthesized = (db: Database) =>
  synthesizedRecords('app.bsky.graph.follow', async (uris) => {
    const rows = await db.db
      .selectFrom('follow')
      .select([
        'uri',
        'cid',
        'subjectDid',
        'via',
        'viaCid',
        'createdAt',
        'indexedAt',
        'takedownRef',
      ])
      .where('uri', 'in', uris)
      .execute()
    return rows.map((r) => ({
      uri: r.uri,
      cid: r.cid,
      createdAt: r.createdAt,
      indexedAt: r.indexedAt,
      takedownRef: r.takedownRef,
      value: {
        $type: 'app.bsky.graph.follow',
        subject: r.subjectDid,
        createdAt: r.createdAt,
        ...viaRef(r.via, r.viaCid),
      },
    }))
  })

export const getBlockRecordsSynthesized = (db: Database) =>
  synthesizedRecords('app.bsky.graph.block', async (uris) => {
    const rows = await db.db
      .selectFrom('actor_block')
      .select([
        'uri',
        'cid',
        'subjectDid',
        'createdAt',
        'indexedAt',
        'takedownRef',
      ])
      .where('uri', 'in', uris)
      .execute()
    return rows.map((r) => ({
      uri: r.uri,
      cid: r.cid,
      createdAt: r.createdAt,
      indexedAt: r.indexedAt,
      takedownRef: r.takedownRef,
      value: {
        $type: 'app.bsky.graph.block',
        subject: r.subjectDid,
        createdAt: r.createdAt,
      },
    }))
  })

export const getPostRecords = (db: Database) => {
  const getBaseRecords = getRecords(db, app.bsky.feed.post)
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
