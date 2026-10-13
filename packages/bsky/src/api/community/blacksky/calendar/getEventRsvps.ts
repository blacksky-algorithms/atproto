import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { resHeaders } from '../../../util.js'
import { buildRsvpViews, canViewEvent } from './views.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.calendar.getEventRsvps, {
    auth: ctx.authVerifier.standardOptional,
    handler: async ({ auth, params, req }) => {
      const { viewer, includeTakedowns } = ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        viewer,
        labelers,
        includeTakedowns,
      })
      const mutedOrBlocked = params.section === 'mutedOrBlocked'
      const visible = await canViewEvent(ctx, hydrateCtx, params.event)
      if (!visible || (mutedOrBlocked && !viewer)) {
        return {
          encoding: 'application/json',
          body: { event: params.event, rsvps: [] },
          headers: resHeaders({ labelers: hydrateCtx.labelers }),
        }
      }
      const res = await ctx.dataplane.getCalendarEventRsvps({
        eventUri: params.event,
        statuses: params.status ? [params.status] : [],
        viewerDid: viewer ?? '',
        mutedOrBlocked,
        includeMutedOrBlockedCount: true,
        limit: params.limit,
        cursor: params.cursor,
      })
      const rsvps = await buildRsvpViews(
        ctx,
        hydrateCtx,
        res.rsvps.map((item) => ({
          uri: item.uri,
          creator: item.creator,
          status: item.status,
          indexedAt: item.indexedAt?.toDate(),
        })),
      )
      return {
        encoding: 'application/json',
        body: {
          event: params.event,
          rsvps,
          cursor: res.cursor || undefined,
          mutedOrBlockedCount:
            viewer && !mutedOrBlocked && !params.cursor
              ? res.mutedOrBlockedCount
              : undefined,
        },
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
