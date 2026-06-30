import pg from 'pg'
import { ServiceImpl } from '@connectrpc/connect'
import * as dcbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'
import { Service } from '../../../proto/bsky_connect.js'
import { Database } from '../db/index.js'

function inflateForHashing(v: unknown): unknown {
  if (v === null || typeof v !== 'object') return v
  if (Array.isArray(v)) return v.map(inflateForHashing)
  const obj = v as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1) {
    if (keys[0] === '$link' && typeof obj.$link === 'string') {
      return CID.parse(obj.$link)
    }
    if (keys[0] === '/' && typeof obj['/'] === 'string') {
      return CID.parse(obj['/'] as string)
    }
  }
  const out: Record<string, unknown> = {}
  for (const k of keys) {
    const val = obj[k]
    if (val === undefined) continue
    out[k] = inflateForHashing(val)
  }
  return out
}

async function computeRecordCid(canonical: unknown): Promise<string> {
  const encoded = dcbor.encode(canonical)
  const digest = await sha256.digest(encoded)
  return CID.createV1(0x71, digest).toString()
}

interface CacheEntry {
  value: boolean
  expiresAt: number
}

const CACHE_TTL_MS = 5 * 60 * 1000
const CACHE_MAX_SIZE = 100_000

// pg returns jsonb columns parsed; proto fields are typed `string`.
const jsonbToProtoString = (v: unknown): string =>
  v == null ? '' : typeof v === 'string' ? v : JSON.stringify(v)

export default (
  db: Database,
  membershipPool: pg.Pool | undefined,
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
        `SELECT 1 FROM membership WHERE did = $1 AND list = 'blacksky-beta' AND included = true`,
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
          facets: jsonbToProtoString(row.facets),
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: jsonbToProtoString(row.embed),
          langs: row.langs ?? '',
          labels: jsonbToProtoString(row.labels),
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
          facets: jsonbToProtoString(row.facets),
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: jsonbToProtoString(row.embed),
          langs: row.langs ?? '',
          labels: jsonbToProtoString(row.labels),
          tags: row.tags ?? '',
          createdAt: row.createdAt ?? '',
          indexedAt: row.indexedAt ?? '',
          sortAt: row.sortAt ?? '',
        })),
        cursor: nextCursor,
      }
    },

    async submitCommunityPost(req) {
      console.log('[dataplane] submitCommunityPost START', {
        uri: req.uri,
        rkey: req.rkey,
        creator: req.creator,
        text: req.text?.substring(0, 50),
      })

      try {
        const record: Record<string, unknown> = {
          $type: 'community.blacksky.feed.post',
          text: req.text,
          createdAt: req.createdAt,
        }
        if (req.facets) {
          const facets = JSON.parse(req.facets)
          if (Array.isArray(facets) && facets.length > 0) {
            record.facets = inflateForHashing(facets)
          }
        }
        if (req.langs) {
          const langs = req.langs.split(',').filter(Boolean)
          if (langs.length > 0) record.langs = langs
        }
        if (req.embed) {
          record.embed = inflateForHashing(JSON.parse(req.embed))
        }
        if (req.replyRoot && req.replyParent) {
          record.reply = {
            root: { uri: req.replyRoot, cid: req.replyRootCid },
            parent: { uri: req.replyParent, cid: req.replyParentCid },
          }
        }

        const cidStr = await computeRecordCid(record)
        const cidVerified = req.expectedCid
          ? cidStr === req.expectedCid
          : false

        if (req.expectedCid && !cidVerified) {
          console.warn('[dataplane] submitCommunityPost CID mismatch', {
            uri: req.uri,
            expected: req.expectedCid,
            computed: cidStr,
          })
          return { cid: cidStr, cidVerified: false }
        }

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

        try {
          await writeCommunityNotifications(db, {
            uri: req.uri,
            cid: cidStr,
            creator: req.creator,
            facets: req.facets,
            embed: req.embed,
            replyParent: req.replyParent,
            createdAt: req.createdAt,
          })
        } catch (notifErr) {
          console.warn('[dataplane] community notification write failed:', notifErr)
        }

        return { cid: cidStr, cidVerified }
      } catch (err) {
        console.error('[dataplane] submitCommunityPost ERROR:', err)
        throw err
      }
    },

    async deleteCommunityPost(req) {
      const { uri, requesterDid } = req
      const res = await db.pool.query(
        `DELETE FROM community_post WHERE uri = $1 AND creator = $2`,
        [uri, requesterDid],
      )
      if (res.rowCount && res.rowCount > 0) {
        await db.pool.query(
          `DELETE FROM notification WHERE "recordUri" = $1`,
          [uri],
        )
      }
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

    async getCommunityPostReplies(req) {
      const { parentUri, limit, cursor } = req
      const params: unknown[] = [parentUri, limit + 1]
      // parentUri is the THREAD ROOT URI; returns every descendant for tree assembly.
      let query = `SELECT * FROM community_post WHERE "replyRoot" = $1`
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
          facets: jsonbToProtoString(row.facets),
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: jsonbToProtoString(row.embed),
          langs: row.langs ?? '',
          labels: jsonbToProtoString(row.labels),
          tags: row.tags ?? '',
          createdAt: row.createdAt ?? '',
          indexedAt: row.indexedAt ?? '',
          sortAt: row.sortAt ?? '',
        })),
        cursor: nextCursor,
      }
    },

    async getCommunityPostReplyCount(req) {
      const { uri } = req
      const res = await db.pool.query(
        `SELECT COUNT(*) as count FROM community_post WHERE "replyParent" = $1`,
        [uri],
      )
      const count = parseInt(res.rows[0]?.count ?? '0', 10)
      return { count }
    },

    async getCommunityPostLikeCount(req) {
      const { uri } = req
      const res = await db.pool.query(
        `SELECT COUNT(*) as count FROM "like" WHERE subject = $1`,
        [uri],
      )
      const count = parseInt(res.rows[0]?.count ?? '0', 10)
      return { count }
    },

    async getCommunityPostViewerLike(req) {
      const { subjectUri, viewerDid } = req
      if (!viewerDid) return { likeUri: '' }
      const res = await db.pool.query(
        `SELECT uri FROM "like" WHERE subject = $1 AND creator = $2 LIMIT 1`,
        [subjectUri, viewerDid],
      )
      return { likeUri: res.rows[0]?.uri ?? '' }
    },

    async getCommunityTimeline(req) {
      const { limit, cursor } = req
      const params: unknown[] = [limit + 1]
      let query = `SELECT * FROM community_post WHERE TRUE`
      if (cursor) {
        query += ` AND "sortAt" < $2`
        params.push(cursor)
      }
      query += ` ORDER BY "sortAt" DESC LIMIT $1`

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
          facets: jsonbToProtoString(row.facets),
          replyRoot: row.replyRoot ?? '',
          replyRootCid: row.replyRootCid ?? '',
          replyParent: row.replyParent ?? '',
          replyParentCid: row.replyParentCid ?? '',
          embed: jsonbToProtoString(row.embed),
          langs: row.langs ?? '',
          labels: jsonbToProtoString(row.labels),
          tags: row.tags ?? '',
          createdAt: row.createdAt ?? '',
          indexedAt: row.indexedAt ?? '',
          sortAt: row.sortAt ?? '',
        })),
        cursor: nextCursor,
      }
    },
  }
}

const didFromAtUri = (uri: string | undefined): string | null => {
  const m = uri?.match(/^at:\/\/([^/]+)/)
  return m ? m[1] : null
}

async function writeCommunityNotifications(
  db: Database,
  args: {
    uri: string
    cid: string
    creator: string
    facets: string | null | undefined
    embed: string | null | undefined
    replyParent: string | null | undefined
    createdAt: string
  },
): Promise<void> {
  const { uri, cid, creator, facets, embed, replyParent, createdAt } = args
  const targets: Array<{
    did: string
    reason: 'reply' | 'mention' | 'quote'
    reasonSubject: string
  }> = []

  const replyParentAuthor = replyParent ? didFromAtUri(replyParent) : null
  if (replyParent && replyParentAuthor && replyParentAuthor !== creator) {
    targets.push({
      did: replyParentAuthor,
      reason: 'reply',
      reasonSubject: replyParent,
    })
  }

  if (facets) {
    try {
      const parsed = JSON.parse(facets)
      const mentioned = new Set<string>()
      for (const f of Array.isArray(parsed) ? parsed : []) {
        for (const feat of f?.features ?? []) {
          if (
            feat?.$type === 'app.bsky.richtext.facet#mention' &&
            typeof feat.did === 'string' &&
            feat.did !== creator &&
            feat.did !== replyParentAuthor
          ) {
            mentioned.add(feat.did)
          }
        }
      }
      for (const did of mentioned) {
        targets.push({ did, reason: 'mention', reasonSubject: uri })
      }
    } catch {}
  }

  if (embed) {
    try {
      const parsed = JSON.parse(embed)
      const quotedUri =
        parsed?.$type === 'app.bsky.embed.record'
          ? parsed.record?.uri
          : parsed?.$type === 'app.bsky.embed.recordWithMedia'
            ? parsed.record?.record?.uri
            : undefined
      const quotedAuthor = quotedUri ? didFromAtUri(quotedUri) : null
      if (quotedUri && quotedAuthor && quotedAuthor !== creator) {
        targets.push({
          did: quotedAuthor,
          reason: 'quote',
          reasonSubject: quotedUri,
        })
      }
    } catch {}
  }

  for (const t of targets) {
    await db.pool.query(
      `INSERT INTO notification (did, author, "recordUri", "recordCid", reason, "reasonSubject", "sortAt")
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (did, "recordUri", reason) DO NOTHING`,
      [t.did, creator, uri, cid, t.reason, t.reasonSubject, createdAt],
    )
  }
}
