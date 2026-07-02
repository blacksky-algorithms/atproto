import { ServiceImpl } from '@connectrpc/connect'
import { Service } from '../../../proto/bsky_connect.js'
import { Database } from '../db/index.js'

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async grantBadge(req) {
    const res = await db.pool.query(
      `INSERT INTO actor_badge ("did", "badge", "issuedBy")
       VALUES ($1, $2, $3)
       ON CONFLICT ("did", "badge") WHERE "revokedAt" IS NULL DO NOTHING`,
      [req.actor, req.badge, req.issuedBy],
    )
    return { granted: (res.rowCount ?? 0) > 0 }
  },

  async revokeBadge(req) {
    const res = await db.pool.query(
      `UPDATE actor_badge
         SET "revokedAt" = now(), "revokedBy" = $1
       WHERE "did" = $2 AND "badge" = $3 AND "revokedAt" IS NULL`,
      [req.revokedBy, req.actor, req.badge],
    )
    return { found: (res.rowCount ?? 0) > 0 }
  },

  async getActorBadges(req) {
    const rows = await db.db
      .selectFrom('actor_badge')
      .select('badge')
      .where('did', '=', req.actor)
      .where('revokedAt', 'is', null)
      .orderBy('id', 'asc')
      .execute()
    return { badges: rows.map((r) => r.badge) }
  },
})
