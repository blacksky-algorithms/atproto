import { Database, DataPlaneServer } from '@atproto/bsky'

const main = async () => {
  const dbUrl = process.env.BSKY_DB_POSTGRES_URL
  const dbSchema = process.env.BSKY_DB_POSTGRES_SCHEMA || 'bsky'
  const port = parseInt(process.env.BSKY_DATAPLANE_PORT || '2585', 10)
  const plcUrl = process.env.BSKY_DID_PLC_URL || 'https://plc.directory'
  const membershipDbUrl = process.env.BLACKSKY_MEMBERSHIP_DB_URL || undefined
  const courierHttpVersion = process.env.BSKY_COURIER_HTTP_VERSION || '2'
  if (courierHttpVersion !== '1.1' && courierHttpVersion !== '2') {
    throw new Error('BSKY_COURIER_HTTP_VERSION must be "1.1" or "2"')
  }
  const notificationPushBridge = {
    enabled: process.env.BSKY_NOTIFICATION_PUSH_WORKER_ENABLED === 'true',
    courierUrl: process.env.BSKY_COURIER_URL || undefined,
    courierApiKey: process.env.BSKY_COURIER_API_KEY || undefined,
    courierHttpVersion,
    courierIgnoreBadTls: process.env.BSKY_COURIER_IGNORE_BAD_TLS === 'true',
    batchSize: parseInt(process.env.BSKY_NOTIFICATION_PUSH_BATCH_SIZE || '100', 10),
    batchWindowMs: parseInt(
      process.env.BSKY_NOTIFICATION_PUSH_BATCH_WINDOW_MS || '250',
      10,
    ),
    courierTimeoutMs: parseInt(
      process.env.BSKY_NOTIFICATION_PUSH_COURIER_TIMEOUT_MS || '5000',
      10,
    ),
    retryIntervalMs: parseInt(
      process.env.BSKY_NOTIFICATION_PUSH_RETRY_INTERVAL_MS || '10000',
      10,
    ),
    maxAttempts: parseInt(
      process.env.BSKY_NOTIFICATION_PUSH_MAX_ATTEMPTS || '10',
      10,
    ),
    ttlHours: parseInt(
      process.env.BSKY_NOTIFICATION_PUSH_TTL_HOURS || '24',
      10,
    ),
  }

  console.log('Starting DataPlane server...')
  console.log('Database URL:', dbUrl.replace(/:[^:@]+@/, ':****@'))
  console.log('Schema:', dbSchema)
  console.log('Port:', port)
  console.log('Membership DB:', membershipDbUrl ? 'configured' : 'disabled')
  console.log(
    'Notification push bridge:',
    notificationPushBridge.enabled ? 'enabled' : 'disabled',
  )

  const db = new Database({
    url: dbUrl,
    schema: dbSchema,
    poolSize: 50,
  })

  console.log('Running database migrations...')
  await db.migrateToLatestOrThrow()
  console.log('Migrations complete')

  const server = await DataPlaneServer.create({
    db,
    port,
    plcUrl,
    membershipDbUrl,
    notificationPushBridge,
  })
  console.log('DataPlane server listening on port', port)

  const shutdown = async () => {
    console.log('Shutting down DataPlane server...')
    await server.destroy()
    await db.close()
    process.exit(0)
  }

  process.on('SIGTERM', shutdown)
  process.on('SIGINT', shutdown)
}

main().catch((err) => {
  console.error('DataPlane server failed to start:', err)
  process.exit(1)
})
