import { ServiceImpl } from '@connectrpc/connect'
import { Service } from '../../../proto/bsky_connect.js'
import { Database } from '../db/index.js'

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async recordPeerModLabel(req) {
    const now = new Date().toISOString()
    await db.pool.query(
      `INSERT INTO peer_mod_label (
         "subjectUri", "subjectCid", "val", "peerModDid", "ozoneEventId", "createdAt"
       ) VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT ("subjectUri", "val") WHERE "negatedAt" IS NULL DO NOTHING`,
      [
        req.subjectUri,
        req.subjectCid,
        req.val,
        req.peerModDid,
        req.ozoneEventId,
        now,
      ],
    )
    return { val: req.val, subjectUri: req.subjectUri }
  },

  async negatePeerModLabel(req) {
    const now = new Date().toISOString()
    const res = await db.pool.query(
      `UPDATE peer_mod_label
         SET "negatedAt" = $1,
             "negatedBy" = $2,
             "negationOzoneEventId" = $3
       WHERE "subjectUri" = $4
         AND "val" = $5
         AND "peerModDid" = $2
         AND "negatedAt" IS NULL`,
      [now, req.peerModDid, req.ozoneEventId, req.subjectUri, req.val],
    )
    return { found: (res.rowCount ?? 0) > 0 }
  },

  async getPeerModLabelsForSubject(req) {
    const res = await db.pool.query<{ val: string }>(
      `SELECT "val" FROM peer_mod_label
       WHERE "subjectUri" = $1
         AND "peerModDid" = $2
         AND "negatedAt" IS NULL`,
      [req.subjectUri, req.peerModDid],
    )
    return { vals: res.rows.map((r) => r.val) }
  },
})
