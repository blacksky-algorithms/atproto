import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { runNotificationCount } from '../../../app/bsky/notification/getUnreadCount.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.notification.getUnreadCount, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth, params }) => {
      const viewer = auth.credentials.iss
      const result = await runNotificationCount(
        { ...params, viewer },
        ctx,
        'authorized-union',
      )
      return {
        encoding: 'application/json',
        body: result,
      }
    },
  })
}
