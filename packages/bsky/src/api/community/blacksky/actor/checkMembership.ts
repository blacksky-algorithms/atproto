import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { communityPostsEnabled } from '../membership-guard.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.actor.checkMembership, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth }) => {
      // Master launch switch: report non-member to hide the tab everywhere.
      if (!communityPostsEnabled()) {
        return {
          encoding: 'application/json' as const,
          body: { isMember: false },
        }
      }
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
