import { Router, Request, Response } from 'express'
import pg from 'pg'
import { AppContext } from '../context'

const { Pool } = pg

interface ChatMessage {
  uri: string
  cid: string
  author: {
    did: string
    handle: string
    displayName?: string
    avatar?: string
  }
  text: string
  createdAt: string
}

interface StreamStatus {
  did: string
  handle: string
  live: boolean
  viewerCount?: number
  title?: string
  startedAt?: string
}

export const createRouter = (ctx: AppContext): Router => {
  const router = Router()

  const dbUrl = process.env.BLACKSKY_COMMUNITY_DB_URL
  if (!dbUrl) {
    console.warn(
      '[stream] BLACKSKY_COMMUNITY_DB_URL not set, stream endpoints disabled',
    )
    return router
  }

  const pool = new Pool({
    connectionString: dbUrl,
    max: 5,
    idleTimeoutMillis: 30000,
  })

  router.get(
    '/xrpc/community.blacksky.stream.getChat',
    async (req: Request, res: Response) => {
      try {
        const streamer = req.query.streamer as string
        if (!streamer) {
          return res
            .status(400)
            .json({ error: 'InvalidRequest', message: 'streamer is required' })
        }

        const limit = Math.min(
          Math.max(parseInt(req.query.limit as string) || 50, 1),
          100,
        )
        const cursor = req.query.cursor as string | undefined

        let query: string
        let params: (string | number)[]

        if (cursor) {
          query = `
          SELECT m.uri, m.cid, m.creator, m.streamer, m.text, m."createdAt",
                 a.handle, p."displayName", p."avatarCid"
          FROM stream_chat_message m
          LEFT JOIN actor a ON a.did = m.creator
          LEFT JOIN profile p ON p.creator = m.creator
          WHERE m.streamer = $1 AND m."createdAt" < $2
          ORDER BY m."createdAt" DESC
          LIMIT $3`
          params = [streamer, cursor, limit]
        } else {
          query = `
          SELECT m.uri, m.cid, m.creator, m.streamer, m.text, m."createdAt",
                 a.handle, p."displayName", p."avatarCid"
          FROM stream_chat_message m
          LEFT JOIN actor a ON a.did = m.creator
          LEFT JOIN profile p ON p.creator = m.creator
          WHERE m.streamer = $1
          ORDER BY m."createdAt" DESC
          LIMIT $2`
          params = [streamer, limit]
        }

        const result = await pool.query(query, params)

        const messages: ChatMessage[] = result.rows.map((row) => ({
          uri: row.uri,
          cid: row.cid,
          author: {
            did: row.creator,
            handle: row.handle || 'handle.invalid',
            displayName: row.displayName || undefined,
            avatar: row.avatarCid
              ? `https://cdn.bsky.app/img/avatar/plain/${row.creator}/${row.avatarCid}@jpeg`
              : undefined,
          },
          text: row.text,
          createdAt: row.createdAt,
        }))

        const nextCursor =
          messages.length === limit
            ? messages[messages.length - 1].createdAt
            : undefined

        return res.json({ messages, cursor: nextCursor })
      } catch (err) {
        console.error('[stream] getChat error:', err)
        return res
          .status(500)
          .json({ error: 'InternalServerError', message: 'Internal error' })
      }
    },
  )

  router.get(
    '/xrpc/community.blacksky.stream.getStreamStatus',
    async (req: Request, res: Response) => {
      try {
        let streamer = req.query.streamer as string
        if (!streamer) {
          return res
            .status(400)
            .json({ error: 'InvalidRequest', message: 'streamer is required' })
        }

        // Resolve handle to DID if needed
        let did = streamer
        let handle = ''
        if (!streamer.startsWith('did:')) {
          const handleResult = await pool.query(
            'SELECT did FROM actor WHERE handle = $1',
            [streamer],
          )
          if (handleResult.rows.length === 0) {
            return res
              .status(400)
              .json({ error: 'InvalidRequest', message: 'Streamer not found' })
          }
          did = handleResult.rows[0].did
          handle = streamer
        } else {
          const actorResult = await pool.query(
            'SELECT handle FROM actor WHERE did = $1',
            [did],
          )
          handle = actorResult.rows[0]?.handle || ''
        }

        // Get active livestream
        const livestreamResult = await pool.query(
          `SELECT title, "createdAt" FROM stream_livestream
           WHERE creator = $1 AND "endedAt" IS NULL
           ORDER BY "createdAt" DESC LIMIT 1`,
          [did],
        )

        // Get viewer count (summed across all servers)
        const viewerResult = await pool.query(
          'SELECT COALESCE(SUM(count), 0) as count FROM stream_viewer_count WHERE streamer = $1',
          [did],
        )

        const live = livestreamResult.rows.length > 0
        const status: StreamStatus = {
          did,
          handle,
          live,
        }

        if (live) {
          status.title = livestreamResult.rows[0].title || undefined
          status.startedAt = livestreamResult.rows[0].createdAt
          if (viewerResult.rows.length > 0) {
            status.viewerCount = viewerResult.rows[0].count
          }
        }

        return res.json(status)
      } catch (err) {
        console.error('[stream] getStreamStatus error:', err)
        return res
          .status(500)
          .json({ error: 'InternalServerError', message: 'Internal error' })
      }
    },
  )

  console.log('[stream] Streamplace endpoints registered')
  return router
}
