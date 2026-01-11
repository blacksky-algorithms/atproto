import events from 'node:events'
import http from 'node:http'
import { expressConnectMiddleware } from '@connectrpc/connect-express'
import express from 'express'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { Redis } from '../../redis'
import { Database, DatabaseSchema } from './db'
import createRoutes from './routes'

export type { DatabaseSchema }

export { RepoSubscription } from './subscription'

export interface DataPlaneServerOptions {
  db: Database
  port: number
  plcUrl?: string
  redisHost?: string
  redisPassword?: string
}

export class DataPlaneServer {
  constructor(
    public server: http.Server,
    public idResolver: IdResolver,
    public redis?: Redis,
  ) {}

  static async create(opts: DataPlaneServerOptions) {
    const { db, port, plcUrl, redisHost, redisPassword } = opts
    const app = express()
    const didCache = new MemoryCache()
    const idResolver = new IdResolver({ plcUrl, didCache })
    const redis = redisHost
      ? new Redis({ host: redisHost, password: redisPassword })
      : undefined
    const routes = createRoutes(db, idResolver, redis)
    app.use(expressConnectMiddleware({ routes }))
    const server = app.listen(port)
    await events.once(server, 'listening')
    return new DataPlaneServer(server, idResolver, redis)
  }

  async destroy() {
    await new Promise<void>((resolve, reject) => {
      this.server.close((err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
    if (this.redis) {
      await this.redis.destroy()
    }
  }
}
