import type { ServiceImpl } from '@connectrpc/connect'
import * as dcbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import { sha256 } from 'multiformats/hashes/sha2'
import type pg from 'pg'
import { AtUri } from '@atproto/syntax'
import type { Service } from '../../../proto/bsky_connect.js'
import type { Database } from '../db/index.js'
import {
  isSpaceRecordUri,
  spaceOfRecordUri,
  spaceRecordAuthor,
} from '../../../api/community/blacksky/space-uri.js'
import { communityPostFromRow } from './community-util.js'

function parseJson(input: string): any | undefined {
  try {
    return JSON.parse(input)
  } catch {
    return undefined
  }
}

function extractQuotedCommunityUri(embedJson: string): string | null {
  try {
    const embed = JSON.parse(embedJson)
    const uri = embed?.record?.uri ?? embed?.record?.record?.uri ?? undefined
    return typeof uri === 'string' &&
      uri.includes('/community.blacksky.feed.post/')
      ? uri
      : null
  } catch {
    return null
  }
}

// True when a direct or list-based block exists in either direction.
async function blockExistsBetween(
  db: Database,
  a: string,
  b: string,
): Promise<boolean> {
  const direct = await db.pool.query(
    `SELECT 1 FROM actor_block
     WHERE (creator = $1 AND "subjectDid" = $2)
        OR (creator = $2 AND "subjectDid" = $1)
     LIMIT 1`,
    [a, b],
  )
  if (direct.rowCount && direct.rowCount > 0) return true
  const viaList = await db.pool.query(
    `SELECT 1 FROM list_block lb
     JOIN list_item li ON li."listUri" = lb."subjectUri"
     WHERE (lb.creator = $1 AND li."subjectDid" = $2)
        OR (lb.creator = $2 AND li."subjectDid" = $1)
     LIMIT 1`,
    [a, b],
  )
  return (viaList.rowCount ?? 0) > 0
}

async function threadgatePermitsReply(
  db: Database,
  opts: {
    rules: unknown
    rootCreator: string
    rootFacets: string | null
    replier: string
  },
): Promise<boolean> {
  const rules = Array.isArray(opts.rules) ? opts.rules : []
  // Empty allow array means nobody may reply.
  if (rules.length === 0) return false
  for (const rule of rules as Array<{ $type?: string; list?: string }>) {
    const t = rule?.$type ?? ''
    if (t.endsWith('#mentionRule')) {
      const facets = opts.rootFacets ? parseJson(opts.rootFacets) : []
      const mentioned = (Array.isArray(facets) ? facets : []).some((f: any) =>
        (f?.features ?? []).some(
          (feat: any) =>
            feat?.$type === 'app.bsky.richtext.facet#mention' &&
            feat?.did === opts.replier,
        ),
      )
      if (mentioned) return true
    } else if (t.endsWith('#followingRule')) {
      const res = await db.pool.query(
        `SELECT 1 FROM follow WHERE creator = $1 AND "subjectDid" = $2 LIMIT 1`,
        [opts.rootCreator, opts.replier],
      )
      if (res.rowCount && res.rowCount > 0) return true
    } else if (t.endsWith('#followerRule')) {
      const res = await db.pool.query(
        `SELECT 1 FROM follow WHERE creator = $1 AND "subjectDid" = $2 LIMIT 1`,
        [opts.replier, opts.rootCreator],
      )
      if (res.rowCount && res.rowCount > 0) return true
    } else if (t.endsWith('#listRule') && rule.list) {
      const res = await db.pool.query(
        `SELECT 1 FROM list_item WHERE "listUri" = $1 AND "subjectDid" = $2 LIMIT 1`,
        [rule.list, opts.replier],
      )
      if (res.rowCount && res.rowCount > 0) return true
    }
  }
  return false
}

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

const MEMBERSHIP_LIST = process.env.COMMUNITY_MEMBERSHIP_LIST ?? 'blacksky'

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
        `SELECT 1 FROM membership WHERE did = $1 AND list = $2 AND included = true`,
        [did, MEMBERSHIP_LIST],
      )
      const isMember = res.rowCount !== null && res.rowCount > 0

      if (membershipCache.size >= CACHE_MAX_SIZE) {
        const firstKey = membershipCache.keys().next().value
        if (firstKey !== undefined) {
          membershipCache.delete(firstKey)
        }
      }
      membershipCache.set(did, {
        value: isMember,
        expiresAt: now + CACHE_TTL_MS,
      })

      return { isMember }
    },

    async getCommunityFeedConfig(req) {
      let feed: AtUri
      try {
        feed = new AtUri(req.feedUri)
      } catch {
        return { configJson: '' }
      }
      if (feed.collection !== 'app.bsky.feed.generator' || !feed.rkey) {
        return { configJson: '' }
      }
      const configUri = `at://${feed.did}/community.blacksky.feed.config/${feed.rkey}`
      const row = await db.db
        .selectFrom('record')
        .select(['json', 'takedownRef'])
        .where('uri', '=', configUri)
        .executeTakeFirst()
      return {
        configJson: row && !row.takedownRef ? row.json : '',
      }
    },

    async getCommunityPost(req) {
      const { uri } = req
      const allowedSpaceUris = req.allowedSpaceUris ?? []
      let query = db.db
        .selectFrom('community_post')
        .selectAll()
        .where('uri', '=', uri)
        .where('moderation_flagged_at', 'is', null)
      query = allowedSpaceUris.length
        ? query.where((eb) =>
            eb.or([
              eb('space_uri', 'is', null),
              eb('space_uri', 'in', allowedSpaceUris),
            ]),
          )
        : query.where('space_uri', 'is', null)
      const row = await query.executeTakeFirst()

      if (!row) {
        return { post: undefined }
      }

      return {
        post: communityPostFromRow(row),
      }
    },

    async getCommunityPosts(req) {
      if (req.uris.length === 0) {
        return { posts: [] }
      }
      let query = db.db
        .selectFrom('community_post')
        .selectAll()
        .where('uri', 'in', req.uris)
        .where('moderation_flagged_at', 'is', null)
      const allowedSpaceUris = req.allowedSpaceUris ?? []
      query = allowedSpaceUris.length
        ? query.where((eb) =>
            eb.or([
              eb('space_uri', 'is', null),
              eb('space_uri', 'in', allowedSpaceUris),
            ]),
          )
        : query.where('space_uri', 'is', null)
      const rows = await query.execute()

      return {
        posts: rows.map(communityPostFromRow),
      }
    },

    async getCommunityFeedByActor(req) {
      const { actorDid, limit, cursor } = req
      const params: unknown[] = [actorDid, limit + 1]
      let query = `SELECT * FROM community_post
        WHERE creator = $1 AND space_uri IS NULL AND ${UNFLAGGED}`
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
        posts: rows.map(communityPostFromRow),
        cursor: nextCursor,
      }
    },

    async submitCommunityPost(req) {
      console.log('[dataplane] submitCommunityPost START', {
        uri: req.uri,
        rkey: req.rkey,
        creator: req.creator,
      })

      try {
        // Merged-feed cursors compare sortAt strings; a non-canonical
        // createdAt would mis-order against ISO keyset bounds. Rejecting
        // (rather than rewriting) preserves the client-computed CID.
        const createdAtDate = new Date(req.createdAt)
        if (
          isNaN(createdAtDate.getTime()) ||
          createdAtDate.toISOString() !== req.createdAt
        ) {
          return { cid: '', cidVerified: false, rejected: 'InvalidCreatedAt' }
        }

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
        const cidVerified = req.expectedCid ? cidStr === req.expectedCid : false

        if (req.expectedCid && !cidVerified) {
          console.warn('[dataplane] submitCommunityPost CID mismatch', {
            uri: req.uri,
            expected: req.expectedCid,
            computed: cidStr,
          })
          return { cid: cidStr, cidVerified: false }
        }

        // Threadgate: the root post's allow rules gate replies. A block in
        // either direction between the replier and the root or parent author
        // severs the interaction entirely, matching app.bsky reply semantics.
        if (req.replyRoot) {
          const rootRes = await db.pool.query(
            `SELECT creator, facets, "threadgateAllow" FROM community_post WHERE uri = $1`,
            [req.replyRoot],
          )
          const root = rootRes.rows[0]
          const ancestorAuthors = new Set<string>()
          if (root?.creator) ancestorAuthors.add(root.creator)
          if (req.replyParent && req.replyParent !== req.replyRoot) {
            const parentRes = await db.pool.query(
              `SELECT creator FROM community_post WHERE uri = $1`,
              [req.replyParent],
            )
            if (parentRes.rows[0]?.creator) {
              ancestorAuthors.add(parentRes.rows[0].creator)
            }
          }
          ancestorAuthors.delete(req.creator)
          for (const ancestor of ancestorAuthors) {
            if (await blockExistsBetween(db, ancestor, req.creator)) {
              return { cid: cidStr, cidVerified, rejected: 'BlockedFromReply' }
            }
          }
          if (root && root.threadgateAllow != null) {
            const allowed = await threadgatePermitsReply(db, {
              rules: root.threadgateAllow,
              rootCreator: root.creator,
              rootFacets: root.facets,
              replier: req.creator,
            })
            if (!allowed && req.creator !== root.creator) {
              return { cid: cidStr, cidVerified, rejected: 'ReplyNotAllowed' }
            }
          }
        }

        // Postgate: a quoted community post may disable embedding.
        if (req.embed) {
          const quotedUri = extractQuotedCommunityUri(req.embed)
          if (quotedUri) {
            const gateRes = await db.pool.query(
              `SELECT creator, "embeddingRules" FROM community_post WHERE uri = $1`,
              [quotedUri],
            )
            const quoted = gateRes.rows[0]
            const rules = quoted?.embeddingRules
            const disabled =
              Array.isArray(rules) &&
              rules.some(
                (r: any) =>
                  r?.$type === 'community.blacksky.feed.postgate#disableRule',
              )
            if (disabled && req.creator !== quoted.creator) {
              return { cid: cidStr, cidVerified, rejected: 'EmbeddingDisabled' }
            }
          }
        }

        const now = new Date().toISOString()
        const writeRes = await db.pool.query(
          `INSERT INTO community_post (
            uri, cid, rkey, creator, text, facets,
            "replyRoot", "replyRootCid", "replyParent", "replyParentCid",
            embed, langs, labels, tags, "threadgateAllow", "embeddingRules", "createdAt", "indexedAt", space_uri, projection_revision
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
          ON CONFLICT (uri) DO UPDATE SET
            text = EXCLUDED.text,
            facets = EXCLUDED.facets,
            embed = EXCLUDED.embed,
            cid = EXCLUDED.cid
          WHERE community_post.space_uri IS NOT DISTINCT FROM EXCLUDED.space_uri`,
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
            req.threadgateAllow || null,
            req.embeddingRules || null,
            req.createdAt,
            now,
            req.spaceUri || null,
            req.projectionRevision || null,
          ],
        )
        if (writeRes.rowCount === 0) {
          return { cid: cidStr, cidVerified, rejected: 'FeedMismatch' }
        }

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
          console.warn(
            '[dataplane] community notification write failed:',
            notifErr,
          )
        }

        return { cid: cidStr, cidVerified }
      } catch (err) {
        console.error('[dataplane] submitCommunityPost ERROR:', err)
        throw err
      }
    },

    async projectCommunityRecord(req) {
      if (req.collection === 'app.bsky.feed.post') {
        if (req.operation === 'delete') {
          await db.pool.query(
            'DELETE FROM community_post WHERE uri = $1 AND space_uri = $2',
            [req.uri, req.spaceUri],
          )
          return { rejected: '' }
        }
        const record = parseJson(req.recordJson)
        if (
          req.operation !== 'create' ||
          !record?.createdAt ||
          typeof record.text !== 'string'
        )
          return { rejected: 'InvalidRecord' }
        const rkey = req.uri.split('/').at(-1) ?? ''
        const facets = record.facets ? JSON.stringify(record.facets) : null
        const embed = record.embed ? JSON.stringify(record.embed) : null
        const replyRoot = record.reply?.root?.uri ?? null
        const replyParent = record.reply?.parent?.uri ?? null
        await db.pool.query(
          'INSERT INTO community_post (uri,cid,rkey,creator,text,facets,"replyRoot","replyRootCid","replyParent","replyParentCid",embed,langs,labels,tags,"createdAt","indexedAt",space_uri,projection_revision) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) ON CONFLICT (uri) DO UPDATE SET cid=EXCLUDED.cid,text=EXCLUDED.text,facets=EXCLUDED.facets,"replyRoot"=EXCLUDED."replyRoot","replyRootCid"=EXCLUDED."replyRootCid","replyParent"=EXCLUDED."replyParent","replyParentCid"=EXCLUDED."replyParentCid",embed=EXCLUDED.embed,langs=EXCLUDED.langs,labels=EXCLUDED.labels,tags=EXCLUDED.tags,projection_revision=EXCLUDED.projection_revision WHERE community_post.space_uri = EXCLUDED.space_uri',
          [
            req.uri,
            req.cid,
            rkey,
            req.author,
            record.text,
            facets,
            replyRoot,
            record.reply?.root?.cid ?? null,
            replyParent,
            record.reply?.parent?.cid ?? null,
            embed,
            Array.isArray(record.langs) ? record.langs.join(',') : null,
            record.labels ? JSON.stringify(record.labels) : null,
            Array.isArray(record.tags) ? record.tags.join(',') : null,
            record.createdAt,
            new Date().toISOString(),
            req.spaceUri,
            req.revision,
          ],
        )
        await writeCommunityNotifications(db, {
          uri: req.uri,
          cid: req.cid,
          creator: req.author,
          facets,
          embed,
          replyParent,
          createdAt: record.createdAt,
          allowedRecipients: new Set(req.allowedNotificationDids),
        })
        return { rejected: '' }
      }
      if (req.collection === 'app.bsky.feed.like') {
        if (req.operation === 'delete') {
          await db.pool.query(
            'DELETE FROM "like" WHERE uri = $1 AND space_uri = $2',
            [req.uri, req.spaceUri],
          )
        } else if (req.operation === 'create') {
          const record = parseJson(req.recordJson)
          const subject = record?.subject?.uri
          if (!subject) return { rejected: 'InvalidRecord' }
          await db.pool.query(
            'INSERT INTO "like" (uri, cid, creator, subject, "subjectCid", "createdAt", "indexedAt", space_uri) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (uri) DO NOTHING',
            [
              req.uri,
              req.cid,
              req.author,
              subject,
              record.subject?.cid ?? '',
              record.createdAt ?? new Date().toISOString(),
              new Date().toISOString(),
              req.spaceUri,
            ],
          )
        }
        return { rejected: '' }
      }
      if (req.collection === 'community.blacksky.moderation.action') {
        if (req.operation === 'flag') {
          const record = parseJson(req.recordJson)
          const subject = record?.subject?.uri
          if (!subject) return { rejected: 'InvalidRecord' }
          await db.pool.query(
            'UPDATE community_post SET moderation_flagged_at = now()::text, moderation_flagged_by = $2 WHERE uri = $1 AND space_uri = $3',
            [subject, req.actionUri || req.uri, req.spaceUri],
          )
        } else if (req.operation === 'unflag') {
          await db.pool.query(
            'UPDATE community_post SET moderation_flagged_at = NULL, moderation_flagged_by = NULL WHERE moderation_flagged_by = $1 AND space_uri = $2',
            [req.actionUri, req.spaceUri],
          )
        }
        return { rejected: '' }
      }
      return { rejected: 'UnknownCollection' }
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

    async getCommunityPostReplies(req) {
      const { parentUri, limit, cursor } = req
      const allowedSpaceUris = req.allowedSpaceUris ?? []
      const parentSpace = spaceOfRecordUri(parentUri)
      const params: unknown[] = [parentUri, limit + 1]
      const spaceParameter = parentSpace ? params.push(parentSpace) : 0
      const allowedParameter = params.push(allowedSpaceUris)
      // parentUri is the THREAD ROOT URI; returns every descendant for tree assembly.
      let query = `SELECT * FROM community_post
        WHERE "replyRoot" = $1 AND ${UNFLAGGED}
          AND ${sameSpaceClause(parentSpace, spaceParameter)}
          AND (space_uri IS NULL OR space_uri = ANY($${allowedParameter}::text[]))`
      if (cursor) {
        query += ` AND "sortAt" < $${params.push(cursor)}`
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
        posts: rows.map(communityPostFromRow),
        cursor: nextCursor,
      }
    },

    async getCommunityPostReplyCount(req) {
      const { uri } = req
      const space = spaceOfRecordUri(uri)
      const res = await db.pool.query(
        `SELECT COUNT(*) as count FROM community_post
         WHERE "replyParent" = $1 AND ${UNFLAGGED} AND ${sameSpaceClause(space)}`,
        space ? [uri, space] : [uri],
      )
      const count = parseInt(res.rows[0]?.count ?? '0', 10)
      return { count }
    },

    async getCommunityPostLikeCount(req) {
      const { uri } = req
      const space = spaceOfRecordUri(uri)
      const res = await db.pool.query(
        `SELECT COUNT(*) as count FROM "like"
         WHERE subject = $1 AND ${sameLikeSpaceClause(space, 2)}`,
        space ? [uri, space] : [uri],
      )
      const count = parseInt(res.rows[0]?.count ?? '0', 10)
      return { count }
    },

    async getCommunityPostQuoteCount(req) {
      const { uri } = req
      const space = spaceOfRecordUri(uri)
      const res = await db.pool.query(
        `SELECT COUNT(*) as count FROM community_post
         WHERE (embed->'record'->>'uri' = $1
            OR embed->'record'->'record'->>'uri' = $1)
           AND ${UNFLAGGED} AND ${sameSpaceClause(space)}`,
        space ? [uri, space] : [uri],
      )
      const count = parseInt(res.rows[0]?.count ?? '0', 10)
      return { count }
    },

    async getCommunityPostQuotes(req) {
      const { uri, limit, cursor } = req
      const allowedSpaceUris = req.allowedSpaceUris ?? []
      const space = spaceOfRecordUri(uri)
      const params: unknown[] = [uri, limit + 1]
      const spaceParameter = space ? params.push(space) : 0
      const allowedParameter = params.push(allowedSpaceUris)
      let query = `SELECT * FROM community_post
        WHERE (embed->'record'->>'uri' = $1
           OR embed->'record'->'record'->>'uri' = $1)
          AND ${UNFLAGGED} AND ${sameSpaceClause(space, spaceParameter)}
          AND (space_uri IS NULL OR space_uri = ANY($${allowedParameter}::text[]))`
      if (cursor) {
        query += ` AND "sortAt" < $${params.push(cursor)}`
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
        posts: rows.map(communityPostFromRow),
        cursor: nextCursor,
      }
    },

    async getSpacePostQuotes(req) {
      const { subject, limit, cursor } = req
      const space = subject?.uri ? spaceOfRecordUri(subject.uri) : null
      if (!subject?.uri || !isSpaceRecordUri(subject.uri) || !space) {
        return { posts: [] }
      }

      const params: unknown[] = [subject.uri, space, limit + 1]
      let query = `SELECT * FROM community_post
        WHERE (embed->'record'->>'uri' = $1
           OR embed->'record'->'record'->>'uri' = $1)
          AND ${UNFLAGGED}
          AND space_uri = $2`
      if (subject.cid) {
        const cidParameter = params.push(subject.cid)
        query += ` AND (embed->'record'->>'cid' = $${cidParameter}
           OR embed->'record'->'record'->>'cid' = $${cidParameter})`
      }
      const keyset = parseKeysetCursor(cursor)
      if (cursor && !keyset) {
        throw new Error('invalid cursor')
      }
      if (keyset) {
        const sortAtParameter = params.push(keyset.sortAt)
        const cidParameter = params.push(keyset.cid)
        query += ` AND ("sortAt", cid) < ($${sortAtParameter}, $${cidParameter})`
      }
      query += ` ORDER BY "sortAt" DESC, cid DESC LIMIT $3`

      const res = await db.pool.query(query, params)
      const rows = res.rows
      if (rows.length <= limit) {
        return { posts: rows.map(communityPostFromRow) }
      }
      rows.pop()
      const last = rows.at(-1)
      return {
        posts: rows.map(communityPostFromRow),
        cursor: last ? `${last.sortAt}::${last.cid}` : '',
      }
    },

    async checkCommunityReplyAllowed(req) {
      const { rootUri, viewerDid } = req
      if (!viewerDid) return { allowed: false }
      const rootRes = await db.pool.query(
        `SELECT creator, facets, "threadgateAllow" FROM community_post WHERE uri = $1`,
        [rootUri],
      )
      const root = rootRes.rows[0]
      if (!root) return { allowed: true }
      if (root.creator === viewerDid) return { allowed: true }
      if (await blockExistsBetween(db, root.creator, viewerDid)) {
        return { allowed: false }
      }
      if (root.threadgateAllow == null) return { allowed: true }
      const allowed = await threadgatePermitsReply(db, {
        rules: root.threadgateAllow,
        rootCreator: root.creator,
        rootFacets: root.facets,
        replier: viewerDid,
      })
      return { allowed }
    },

    async getCommunityPostViewerLike(req) {
      const { subjectUri, viewerDid } = req
      if (!viewerDid) return { likeUri: '' }
      const space = spaceOfRecordUri(subjectUri)
      const res = await db.pool.query(
        `SELECT uri FROM "like" WHERE subject = $1 AND creator = $2
         AND ${sameLikeSpaceClause(space, 3)} LIMIT 1`,
        space ? [subjectUri, viewerDid, space] : [subjectUri, viewerDid],
      )
      return { likeUri: res.rows[0]?.uri ?? '' }
    },

    async getCommunityTimeline(req) {
      const { limit, cursor } = req
      const params: unknown[] = [limit + 1]
      // Replies are included so the client can assemble Following-style
      // thread slices; its tuners collapse threads and drop orphans.
      let query = `SELECT * FROM community_post
        WHERE space_uri IS NULL AND ${UNFLAGGED}`
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
        posts: rows.map(communityPostFromRow),
        cursor: nextCursor,
      }
    },

    async getCommunityFeedBySpace(req) {
      const { spaceUri, limit, cursor } = req
      // Never widen to "space_uri IS NULL OR ...": a caller with no space is
      // asking for private content it has not named, and this route is the
      // only one that returns space rows in bulk.
      if (!spaceUri) {
        return { posts: [], cursor: '' }
      }
      const params: unknown[] = [spaceUri, limit + 1]
      let query = `SELECT * FROM community_post
        WHERE space_uri = $1 AND ${UNFLAGGED}`
      const keyset = parseKeysetCursor(cursor)
      if (cursor && !keyset) {
        // An unparseable cursor is a client error, not "start from the top":
        // silently restarting would loop a paginating client forever.
        throw new Error('invalid cursor')
      }
      if (keyset) {
        query += ` AND ("sortAt", cid) < ($3, $4)`
        params.push(keyset.sortAt, keyset.cid)
      }
      query += ` ORDER BY "sortAt" DESC, cid DESC LIMIT $2`

      const res = await db.pool.query(query, params)
      const rows = res.rows
      let nextCursor = ''
      if (rows.length > limit) {
        rows.pop()
        const last = rows[rows.length - 1]
        nextCursor = last ? `${last.sortAt}::${last.cid}` : ''
      }

      return {
        posts: rows.map(communityPostFromRow),
        cursor: nextCursor,
      }
    },
  }
}

/**
 * `<sortAt>::<cid>`. The cid tiebreak is load-bearing: `sortAt` is a timestamp
 * string and a space projects several records in the same millisecond often
 * enough that a bare `"sortAt" < $n` cursor drops or repeats rows at the page
 * boundary.
 */
const parseKeysetCursor = (
  cursor: string | undefined,
): { sortAt: string; cid: string } | null => {
  if (!cursor) return null
  const sep = cursor.lastIndexOf('::')
  if (sep <= 0) return null
  const sortAt = cursor.slice(0, sep)
  const cid = cursor.slice(sep + 2)
  if (!sortAt || !cid) return null
  return { sortAt, cid }
}

/**
 * Moderated posts are flagged rather than deleted, so every read path that
 * returns content or counts it excludes them. Authorization lookups and the
 * author's own delete deliberately still see the row.
 */
const UNFLAGGED = 'moderation_flagged_at IS NULL'

/**
 * Interaction magnitudes are as space-scoped as the content is: a count that
 * spans spaces (or spans a space and the public tier) reports how much of a
 * private space's activity exists to someone outside it. `$2` is the space
 * when there is one.
 */
const sameSpaceClause = (space: string | null, parameter = 2): string =>
  space ? `space_uri = $${parameter}` : 'space_uri IS NULL'

const sameLikeSpaceClause = (
  space: string | null,
  parameter: number,
): string => (space ? `space_uri = $${parameter}` : 'space_uri IS NULL')

const didFromAtUri = (uri: string | undefined): string | null => {
  const m = uri?.match(/^at:\/\/([^/]+)/)
  return m ? m[1] : null
}

/**
 * A space record URI's authority is the space DID, not a person, so the
 * authority is only the author for ordinary at-uris.
 */
const authorOfUri = (uri: string | undefined): string | null =>
  spaceRecordAuthor(uri) ?? didFromAtUri(uri)

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
    allowedRecipients?: Set<string>
  },
): Promise<void> {
  const {
    uri,
    cid,
    creator,
    facets,
    embed,
    replyParent,
    createdAt,
    allowedRecipients,
  } = args
  const targets: Array<{
    did: string
    reason: 'reply' | 'mention' | 'quote'
    reasonSubject: string
  }> = []

  const replyParentAuthor = replyParent ? authorOfUri(replyParent) : null
  if (replyParent && replyParentAuthor && replyParentAuthor !== creator) {
    targets.push({
      did: replyParentAuthor,
      reason: 'reply',
      reasonSubject: replyParent,
    })
  }

  if (facets) {
    const parsed = parseJson(facets)
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
  }

  if (embed) {
    const parsed = parseJson(embed)
    const quotedUri =
      parsed?.$type === 'app.bsky.embed.record'
        ? parsed.record?.uri
        : parsed?.$type === 'app.bsky.embed.recordWithMedia'
          ? parsed.record?.record?.uri
          : undefined
    const quotedAuthor = quotedUri ? authorOfUri(quotedUri) : null
    if (quotedUri && quotedAuthor && quotedAuthor !== creator) {
      targets.push({
        did: quotedAuthor,
        reason: 'quote',
        reasonSubject: quotedUri,
      })
    }
  }

  for (const t of targets) {
    if (allowedRecipients && !allowedRecipients.has(t.did)) continue
    await db.pool.query(
      `INSERT INTO notification (did, author, "recordUri", "recordCid", reason, "reasonSubject", "sortAt")
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (did, "recordUri", reason) DO NOTHING`,
      [t.did, creator, uri, cid, t.reason, t.reasonSubject, createdAt],
    )
  }
}
