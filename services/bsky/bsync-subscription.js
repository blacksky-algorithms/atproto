import { BsyncSubscription, Database } from '@atproto/bsky'

const main = async () => {
  const dbUrl = process.env.BSKY_DB_POSTGRES_URL
  const dbSchema = process.env.BSKY_DB_POSTGRES_SCHEMA || 'bsky'
  const bsyncUrl = process.env.BSKY_BSYNC_URL
  const bsyncApiKey = process.env.BSKY_BSYNC_API_KEY || undefined
  const bsyncHttpVersion = process.env.BSKY_BSYNC_HTTP_VERSION || '1.1'
  const bsyncIgnoreBadTls = process.env.BSKY_BSYNC_IGNORE_BAD_TLS === 'true'
  if (!dbUrl) {
    throw new Error('BSKY_DB_POSTGRES_URL is required')
  }
  if (!bsyncUrl) {
    throw new Error('BSKY_BSYNC_URL is required')
  }
  if (bsyncHttpVersion !== '1.1' && bsyncHttpVersion !== '2') {
    throw new Error('BSKY_BSYNC_HTTP_VERSION must be "1.1" or "2"')
  }

  const db = new Database({
    url: dbUrl,
    schema: dbSchema,
    poolSize: parseInt(process.env.BSKY_DB_POOL_SIZE || '10', 10),
  })

  const sub = new BsyncSubscription({
    db,
    config: { bsyncUrl, bsyncApiKey, bsyncHttpVersion, bsyncIgnoreBadTls },
  })
  sub.start()
  console.log('bsync subscription is running against', bsyncUrl)

  const shutdown = async () => {
    console.log('Shutting down bsync subscription...')
    await sub.destroy()
    await db.close()
    process.exit(0)
  }

  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

main().catch((err) => {
  console.error('bsync subscription failed to start:', err)
  process.exit(1)
})
