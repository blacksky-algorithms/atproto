import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { resHeaders } from '../../../util.js'
import { buildEventViews } from './views.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.calendar.getEvents, {
    auth: ctx.authVerifier.standardOptional,
    handler: async ({ auth, params, req }) => {
      const { viewer, includeTakedowns } = ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        viewer,
        labelers,
        includeTakedowns,
      })
      const events = await buildEventViews(ctx, hydrateCtx, params.uris)
      return {
        encoding: 'application/json',
        body: { events },
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
