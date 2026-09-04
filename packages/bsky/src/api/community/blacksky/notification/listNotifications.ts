import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { runNotificationList } from '../../../app/bsky/notification/listNotifications.js'
import { resHeaders } from '../../../util.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.notification.listNotifications, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({ labelers, viewer })
      const result = await runNotificationList(
        { ...params, hydrateCtx },
        ctx,
        'authorized-union',
      )
      const body: community.blacksky.notification.listNotifications.$OutputBody =
        {
          ...result,
          notifications: result.notifications.map((notification) => ({
            ...notification,
            $type: undefined,
          })),
        }
      return {
        encoding: 'application/json',
        body,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
