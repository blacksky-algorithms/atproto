import events from 'node:events'
import http from 'node:http'
import pg from 'pg'
import { expressConnectMiddleware } from '@connectrpc/connect-express'
import express from 'express'
// eslint-disable-next-line import/default
import httpTerminator from 'http-terminator'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { Database, DatabaseSchema } from './db/index.js'
import {
  createNotificationPushBridge,
  NotificationPushBridge,
  NotificationPushBridgeConfig,
} from './notification-push-bridge.js'
import createRoutes from './routes/index.js'

export type { DatabaseSchema }

export { RepoSubscription } from './subscription.js'

export interface DataPlaneServerOptions {
  db: Database
  port: number
  plcUrl?: string
  membershipDbUrl?: string
  notificationPushBridge?: NotificationPushBridgeConfig
}

export class DataPlaneServer {
  private terminator: httpTerminator.HttpTerminator

  constructor(
    public server: http.Server,
    public idResolver: IdResolver,
    public membershipPool?: pg.Pool,
    public notificationPushBridge?: NotificationPushBridge,
  ) {
    this.terminator = httpTerminator.createHttpTerminator({ server })
  }

  static async create(opts: DataPlaneServerOptions) {
    const {
      db,
      port,
      plcUrl,
      membershipDbUrl,
      notificationPushBridge: notificationPushBridgeConfig,
    } = opts
    const app = express()
    const didCache = new MemoryCache()
    const idResolver = new IdResolver({ plcUrl, didCache })
    const membershipPool = membershipDbUrl
      ? new pg.Pool({ connectionString: membershipDbUrl, max: 3 })
      : undefined
    const routes = createRoutes(db, idResolver, membershipPool)
    app.use(expressConnectMiddleware({ routes }))
    const server = app.listen(port)
    await events.once(server, 'listening')
    const notificationPushBridge = notificationPushBridgeConfig
      ? createNotificationPushBridge(db, notificationPushBridgeConfig)
      : undefined
    if (notificationPushBridge) {
      await notificationPushBridge.start()
    }
    return new DataPlaneServer(
      server,
      idResolver,
      membershipPool,
      notificationPushBridge,
    )
  }

  async destroy() {
    if (this.notificationPushBridge) {
      await this.notificationPushBridge.stop()
    }
    await this.terminator.terminate()
    if (this.membershipPool) {
      await this.membershipPool.end()
    }
  }

  async [Symbol.asyncDispose]() {
    await this.destroy()
  }
}
