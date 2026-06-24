import events from 'node:events'
import http from 'node:http'
import { Pool } from 'pg'
import { expressConnectMiddleware } from '@connectrpc/connect-express'
import express from 'express'
// eslint-disable-next-line import/default
import httpTerminator from 'http-terminator'
import { IdResolver, MemoryCache } from '@atproto/identity'
import { Database, DatabaseSchema } from './db/index.js'
import createRoutes from './routes/index.js'

export type { DatabaseSchema }

export { RepoSubscription } from './subscription.js'

export class DataPlaneServer {
  private terminator: httpTerminator.HttpTerminator

  constructor(
    public server: http.Server,
    public idResolver: IdResolver,
  ) {
    this.terminator = httpTerminator.createHttpTerminator({ server })
  }

  static async create(
    db: Database,
    port: number,
    plcUrl?: string,
    membershipDbUrl?: string,
  ) {
    const app = express()
    const didCache = new MemoryCache()
    const idResolver = new IdResolver({ plcUrl, didCache })
    const membershipPool = membershipDbUrl
      ? new Pool({ connectionString: membershipDbUrl, max: 3 })
      : undefined
    const routes = createRoutes(db, idResolver, membershipPool)
    app.use(expressConnectMiddleware({ routes }))
    const server = app.listen(port)
    await events.once(server, 'listening')
    return new DataPlaneServer(server, idResolver)
  }

  async destroy() {
    await this.terminator.terminate()
  }

  async [Symbol.asyncDispose]() {
    await this.destroy()
  }
}
