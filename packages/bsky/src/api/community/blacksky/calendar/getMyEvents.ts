import { Timestamp } from '@bufbuild/protobuf'
import type { AtUriString } from '@atproto/lex'
import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { resHeaders } from '../../../util.js'
import { buildEventViews } from './views.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.calendar.getMyEvents, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth, params, req }) => {
      const viewer = auth.credentials.iss
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({ viewer, labelers })
      const res = await ctx.dataplane.getMyCalendarEvents({
        viewerDid: viewer,
        past: params.when === 'past',
        now: Timestamp.fromDate(new Date()),
        limit: params.limit,
        cursor: params.cursor,
      })
      const events = await buildEventViews(
        ctx,
        hydrateCtx,
        res.uris as AtUriString[],
      )
      return {
        encoding: 'application/json',
        body: { events, cursor: res.cursor || undefined },
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
