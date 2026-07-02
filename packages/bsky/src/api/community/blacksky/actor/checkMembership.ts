import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.actor.checkMembership, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth }) => {
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: auth.credentials.iss,
      })
      return {
        encoding: 'application/json' as const,
        body: { isMember },
      }
    },
  })
}
