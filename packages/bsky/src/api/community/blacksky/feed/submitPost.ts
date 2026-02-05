import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.submitPost({
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

      // 2. Validate reply cascade
      const { rkey, text, facets, reply, embed, langs, labels, tags, createdAt } =
        input.body
      if (reply) {
        const rootUri = reply.root.uri
        if (!rootUri.includes(COMMUNITY_POST_COLLECTION)) {
          throw new InvalidRequestError(
            'Replies to community posts must reference community posts',
            'InvalidReply',
          )
        }
        // Verify root post exists in our DB
        if (ctx.communityDb) {
          const exists = await ctx.communityDb.communityPostExists(rootUri)
          if (!exists) {
            throw new InvalidRequestError(
              'Reply root post not found',
              'InvalidReply',
            )
          }
        }
      }

      // 3. Build URI and store content
      const uri = `at://${requesterDid}/${COMMUNITY_POST_COLLECTION}/${rkey}`
      const now = new Date().toISOString()

      if (!ctx.communityDb) {
        throw new InvalidRequestError('Community database not configured')
      }

      const { contentHash } = await ctx.communityDb.insertCommunityPost({
        uri,
        rkey,
        creator: requesterDid,
        text,
        facets: facets ?? undefined,
        replyRoot: reply?.root.uri,
        replyRootCid: reply?.root.cid,
        replyParent: reply?.parent.uri,
        replyParentCid: reply?.parent.cid,
        embed: embed ?? undefined,
        langs: langs ?? undefined,
        labels: labels ?? undefined,
        tags: tags ?? undefined,
        createdAt,
        indexedAt: now,
      })

      return {
        encoding: 'application/json' as const,
        body: {
          uri,
          contentHash,
        },
      }
    },
  })
}
