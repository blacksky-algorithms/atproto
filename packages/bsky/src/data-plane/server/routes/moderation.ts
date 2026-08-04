import { ServiceImpl } from '@connectrpc/connect'
import { AtUri } from '@atproto/syntax'
import { Service } from '../../../proto/bsky_connect.js'
import { Database } from '../db/index.js'

// Takedown state for like/follow/repost/block lives on the typed tables
// (their record rows are being dropped); everything else stays on record.
const takedownTableForUri = (uri: string) => {
  switch (new AtUri(uri).collection) {
    case 'app.bsky.feed.like':
      return 'like' as const
    case 'app.bsky.feed.repost':
      return 'repost' as const
    case 'app.bsky.graph.follow':
      return 'follow' as const
    case 'app.bsky.graph.block':
      return 'actor_block' as const
    default:
      return 'record' as const
  }
}

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getActorTakedown(req) {
    const { did } = req
    const res = await db.db
      .selectFrom('actor')
      .where('did', '=', did)
      .select('takedownRef')
      .executeTakeFirst()
    return {
      takenDown: !!res?.takedownRef,
      takedownRef: res?.takedownRef || undefined,
    }
  },

  async getBlobTakedown(req) {
    const { did, cid } = req
    const res = await db.db
      .selectFrom('blob_takedown')
      .where('did', '=', did)
      .where('cid', '=', cid)
      .select('takedownRef')
      .executeTakeFirst()
    return {
      takenDown: !!res,
      takedownRef: res?.takedownRef || undefined,
    }
  },

  async getRecordTakedown(req) {
    const { recordUri } = req
    const res = await db.db
      .selectFrom(takedownTableForUri(recordUri))
      .where('uri', '=', recordUri)
      .select('takedownRef')
      .executeTakeFirst()
    return {
      takenDown: !!res?.takedownRef,
      takedownRef: res?.takedownRef || undefined,
    }
  },

  async takedownActor(req) {
    const { did, ref } = req
    await db.db
      .updateTable('actor')
      .set({ takedownRef: ref || 'TAKEDOWN' })
      .where('did', '=', did)
      .execute()
  },

  async takedownBlob(req) {
    const { did, cid, ref } = req
    await db.db
      .insertInto('blob_takedown')
      .values({
        did,
        cid,
        takedownRef: ref || 'TAKEDOWN',
      })
      .execute()
  },

  async takedownRecord(req) {
    const { recordUri, ref } = req
    await db.db
      .updateTable(takedownTableForUri(recordUri))
      .set({ takedownRef: ref || 'TAKEDOWN' })
      .where('uri', '=', recordUri)
      .execute()
  },

  async untakedownActor(req) {
    const { did } = req
    await db.db
      .updateTable('actor')
      .set({ takedownRef: null })
      .where('did', '=', did)
      .execute()
  },

  async untakedownBlob(req) {
    const { did, cid } = req
    await db.db
      .deleteFrom('blob_takedown')
      .where('did', '=', did)
      .where('cid', '=', cid)
      .executeTakeFirst()
  },

  async untakedownRecord(req) {
    const { recordUri } = req
    await db.db
      .updateTable(takedownTableForUri(recordUri))
      .set({ takedownRef: null })
      .where('uri', '=', recordUri)
      .execute()
  },
})
