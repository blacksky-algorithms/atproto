import { Pool } from 'pg'
import { ServiceImpl } from '@connectrpc/connect'
import { cidForCbor, cborEncode } from '@atproto/common'
import { Service } from '../../../proto/bsky_connect'
import { Database } from '../db'

interface CacheEntry {
  value: boolean
  expiresAt: number
}

const CACHE_TTL_MS = 5 * 60 * 1000
const CACHE_MAX_SIZE = 100_000

export default (
  db: Database,
  membershipPool: Pool | undefined,
): Partial<ServiceImpl<typeof Service>> => {
  const membershipCache = new Map<string, CacheEntry>()

  return {
    async checkCommunityMembership(req) {
      const { did } = req
      if (!membershipPool) {
        return { isMember: false }
      }

      const now = Date.now()
      const cached = membershipCache.get(did)
      if (cached && cached.expiresAt > now) {
        return { isMember: cached.value }
      }

      const res = await membershipPool.query(
        `SELECT 1 FROM membership WHERE did = $1 AND list = 'blacksky' AND included = true`,
        [did],
      )
      const isMember = res.rowCount !== null && res.rowCount > 0

      if (membershipCache.size >= CACHE_MAX_SIZE) {
        const firstKey = membershipCache.keys().next().value
        if (firstKey !== undefined) {
          membershipCache.delete(firstKey)
        }
      }
      membershipCache.set(did, { value: isMember, expiresAt: now + CACHE_TTL_MS })

      return { isMember }
    },

    async getCommunityPost(req) {
      const { uri } = req
      const row = await db.db
        .selectFrom('community_post')
        .selectAll()
        .where('uri', '=', uri)
        .executeTakeFirst()

      if (!row) {
        return { post: undefined }
      }

      return {
        post: {
          uri: row.uri,
          cid: row.cid,
          rkey: row.rkey,
          creator: row.creator,
          text: row.text,
          facets: row.facets ?? '',
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: row.embed ?? '',
          langs: row.langs ?? '',
          labels: row.labels ?? '',
          tags: row.tags ?? '',
          createdAt: row.createdAt,
          indexedAt: row.indexedAt,
          sortAt: row.sortAt,
        },
      }
    },

    async getCommunityFeedByActor(req) {
      const { actorDid, limit, cursor } = req
      const params: unknown[] = [actorDid, limit + 1]
      let query = `SELECT * FROM community_post WHERE creator = $1`
      if (cursor) {
        query += ` AND "sortAt" < $3`
        params.push(cursor)
      }
      query += ` ORDER BY "sortAt" DESC LIMIT $2`

      const res = await db.pool.query(query, params)
      const rows = res.rows
      let nextCursor = ''
      if (rows.length > limit) {
        rows.pop()
        nextCursor = rows[rows.length - 1]?.sortAt ?? ''
      }

      return {
        posts: rows.map((row: Record<string, string | null>) => ({
          uri: row.uri ?? '',
          cid: row.cid ?? '',
          rkey: row.rkey ?? '',
          creator: row.creator ?? '',
          text: row.text ?? '',
          facets: row.facets ?? '',
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: row.embed ?? '',
          langs: row.langs ?? '',
          labels: row.labels ?? '',
          tags: row.tags ?? '',
          createdAt: row.createdAt ?? '',
          indexedAt: row.indexedAt ?? '',
          sortAt: row.sortAt ?? '',
        })),
        cursor: nextCursor,
      }
    },

    async submitCommunityPost(req) {
      // Build the record object matching AT Protocol post schema
      const record: Record<string, unknown> = {
        $type: 'community.blacksky.feed.post',
        text: req.text,
        createdAt: req.createdAt,
      }
      if (req.facets) {
        record.facets = JSON.parse(req.facets)
      }
      if (req.langs) {
        record.langs = req.langs.split(',')
      }
      if (req.embed) {
        record.embed = JSON.parse(req.embed)
      }
      if (req.replyRoot && req.replyParent) {
        record.reply = {
          root: { uri: req.replyRoot, cid: req.replyRootCid || '' },
          parent: {
            uri: req.replyParent,
            cid: req.replyParentCid || req.replyRootCid || '',
          },
        }
      }

      // Compute CID from CBOR-encoded record
      const cid = await cidForCbor(record)
      const cidStr = cid.toString()

      const now = new Date().toISOString()

      await db.pool.query(
        `INSERT INTO community_post (
          uri, cid, rkey, creator, text, facets,
          "replyRoot", "replyRootCid", "replyParent", "replyParentCid",
          embed, langs, labels, tags, "createdAt", "indexedAt"
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (uri) DO UPDATE SET
          text = EXCLUDED.text,
          facets = EXCLUDED.facets,
          embed = EXCLUDED.embed,
          cid = EXCLUDED.cid`,
        [
          req.uri,
          cidStr,
          req.rkey,
          req.creator,
          req.text,
          req.facets || null,
          req.replyRoot || null,
          req.replyRootCid || null,
          req.replyParent || null,
          req.replyParentCid || null,
          req.embed || null,
          req.langs || null,
          req.labels || null,
          req.tags || null,
          req.createdAt,
          now,
        ],
      )

      return { contentHash: cidStr }
    },

    async deleteCommunityPost(req) {
      const { uri, requesterDid } = req
      const res = await db.pool.query(
        `DELETE FROM community_post WHERE uri = $1 AND creator = $2`,
        [uri, requesterDid],
      )
      return { deleted: res.rowCount !== null && res.rowCount > 0 }
    },

    async communityPostExists(req) {
      const { uri } = req
      const res = await db.pool.query(
        `SELECT 1 FROM community_post WHERE uri = $1`,
        [uri],
      )
      return { exists: res.rowCount !== null && res.rowCount > 0 }
    },
  }
}
