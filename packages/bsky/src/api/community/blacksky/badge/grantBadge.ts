import { AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.badge.grantBadge, {
    auth: ctx.authVerifier.role,
    handler: async ({ input, auth }) => {
      if (!auth.credentials.admin) {
        throw new AuthRequiredError('Must be an admin to grant badges')
      }
      const { actor, badge } = input.body
      const res = await ctx.dataplane.grantBadge({
        actor,
        badge,
        issuedBy: 'admin',
      })
      return {
        encoding: 'application/json' as const,
        body: { granted: res.granted },
      }
    },
  })
}
