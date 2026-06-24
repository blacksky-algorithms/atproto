import { InvalidRequestError, AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.deletePost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const requesterDid = auth.credentials.iss

      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: requesterDid,
      })
      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }

      const { uri } = input.body
      const { deleted } = await ctx.dataplane.deleteCommunityPost({
        uri,
        requesterDid,
      })
      if (!deleted) {
        throw new InvalidRequestError(
          'Post not found or not owned by requester',
          'PostNotFound',
        )
      }

      return {
        encoding: 'application/json' as const,
        body: {},
      }
    },
  })
}
