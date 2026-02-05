import { Pool, type PoolConfig } from 'pg'
import { createHash } from 'crypto'

export interface CommunityPostRow {
  uri: string
  cid: string
  rkey: string
  creator: string
  text: string
  facets: string | null
  replyRoot: string | null
  replyRootCid: string | null
  replyParent: string | null
  replyParentCid: string | null
  embed: string | null
  langs: string | null
  labels: string | null
  tags: string | null
  createdAt: string
  indexedAt: string
  sortAt: string
}

export class CommunityDb {
  private pool: Pool

  constructor(connectionString: string, opts?: PoolConfig) {
    this.pool = new Pool({
      connectionString,
      max: 5,
      ...opts,
    })
  }

  async insertCommunityPost(params: {
    uri: string
    rkey: string
    creator: string
    text: string
    facets?: unknown
    replyRoot?: string
    replyRootCid?: string
    replyParent?: string
    replyParentCid?: string
    embed?: unknown
    langs?: string[]
    labels?: unknown
    tags?: string[]
    createdAt: string
    indexedAt: string
  }): Promise<{ contentHash: string }> {
    const contentHash = createHash('sha256')
      .update(
        JSON.stringify({
          text: params.text,
          facets: params.facets,
          reply: params.replyRoot
            ? { root: params.replyRoot, parent: params.replyParent }
            : undefined,
          embed: params.embed,
        }),
      )
      .digest('hex')

    await this.pool.query(
      `INSERT INTO community_post (
        uri, cid, rkey, creator, text, facets,
        "replyRoot", "replyRootCid", "replyParent", "replyParentCid",
        embed, langs, labels, tags, "createdAt", "indexedAt"
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
      ON CONFLICT (uri) DO UPDATE SET
        text = EXCLUDED.text,
        facets = EXCLUDED.facets,
        embed = EXCLUDED.embed`,
      [
        params.uri,
        '', // cid will be updated when stub arrives on firehose
        params.rkey,
        params.creator,
        params.text,
        params.facets ? JSON.stringify(params.facets) : null,
        params.replyRoot ?? null,
        params.replyRootCid ?? null,
        params.replyParent ?? null,
        params.replyParentCid ?? null,
        params.embed ? JSON.stringify(params.embed) : null,
        params.langs ? `{${params.langs.join(',')}}` : null,
        params.labels ? JSON.stringify(params.labels) : null,
        params.tags ? `{${params.tags.join(',')}}` : null,
        params.createdAt,
        params.indexedAt,
      ],
    )

    return { contentHash }
  }

  async getCommunityPost(uri: string): Promise<CommunityPostRow | null> {
    const res = await this.pool.query(
      `SELECT * FROM community_post WHERE uri = $1`,
      [uri],
    )
    return (res.rows[0] as CommunityPostRow) ?? null
  }

  async getCommunityPosts(
    uris: string[],
  ): Promise<Map<string, CommunityPostRow>> {
    if (uris.length === 0) return new Map()
    const res = await this.pool.query(
      `SELECT * FROM community_post WHERE uri = ANY($1)`,
      [uris],
    )
    const map = new Map<string, CommunityPostRow>()
    for (const row of res.rows as CommunityPostRow[]) {
      map.set(row.uri, row)
    }
    return map
  }

  async getCommunityFeedByActor(
    actor: string,
    limit: number,
    cursor?: string,
  ): Promise<{ posts: CommunityPostRow[]; cursor?: string }> {
    const params: unknown[] = [actor, limit + 1]
    let query = `SELECT * FROM community_post WHERE creator = $1`
    if (cursor) {
      query += ` AND "sortAt" < $3`
      params.push(cursor)
    }
    query += ` ORDER BY "sortAt" DESC LIMIT $2`

    const res = await this.pool.query(query, params)
    const rows = res.rows as CommunityPostRow[]
    let nextCursor: string | undefined
    if (rows.length > limit) {
      rows.pop()
      nextCursor = rows[rows.length - 1]?.sortAt
    }
    return { posts: rows, cursor: nextCursor }
  }

  async deleteCommunityPost(uri: string, requesterDid: string): Promise<boolean> {
    const res = await this.pool.query(
      `DELETE FROM community_post WHERE uri = $1 AND creator = $2`,
      [uri, requesterDid],
    )
    return res.rowCount !== null && res.rowCount > 0
  }

  async communityPostExists(uri: string): Promise<boolean> {
    const res = await this.pool.query(
      `SELECT 1 FROM community_post WHERE uri = $1`,
      [uri],
    )
    return res.rowCount !== null && res.rowCount > 0
  }

  async close(): Promise<void> {
    await this.pool.end()
  }
}
