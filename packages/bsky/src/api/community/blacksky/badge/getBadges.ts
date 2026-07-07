import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.badge.getBadges, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params }) => {
      const res = await ctx.dataplane.getActorBadges({ actor: params.actor })
      return {
        encoding: 'application/json' as const,
        body: { badges: res.badges },
      }
    },
  })
}
