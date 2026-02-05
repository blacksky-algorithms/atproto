import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.deletePost({
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const requesterDid = auth.credentials.iss

      // 1. Verify membership
      if (!ctx.communityMembership) {
        throw new InvalidRequestError('Community features not configured')
      }
      const isMember = await ctx.communityMembership.isMember(requesterDid)
      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }

      // 2. Delete the post (only if owned by requester)
      if (!ctx.communityDb) {
        throw new InvalidRequestError('Community database not configured')
      }

      const { uri } = input.body
      const deleted = await ctx.communityDb.deleteCommunityPost(
        uri,
        requesterDid,
      )
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
