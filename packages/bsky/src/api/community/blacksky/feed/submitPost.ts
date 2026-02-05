import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.submitPost({
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

      const {
        rkey,
        text,
        facets,
        reply,
        embed,
        langs,
        labels,
        tags,
        createdAt,
        expectedCid,
      } = input.body

      // Validate reply cascade
      if (reply) {
        const rootUri = reply.root.uri
        if (!rootUri.includes(COMMUNITY_POST_COLLECTION)) {
          throw new InvalidRequestError(
            'Replies to community posts must reference community posts',
            'InvalidReply',
          )
        }
        const { exists } = await ctx.dataplane.communityPostExists({
          uri: rootUri,
        })
        if (!exists) {
          throw new InvalidRequestError(
            'Reply root post not found',
            'InvalidReply',
          )
        }
      }

      const uri = `at://${requesterDid}/${COMMUNITY_POST_COLLECTION}/${rkey}`

      const { cid, cidVerified } = await ctx.dataplane.submitCommunityPost({
        uri,
        rkey,
        creator: requesterDid,
        text,
        facets: facets ? JSON.stringify(facets) : '',
        replyRoot: reply?.root.uri ?? '',
        replyRootCid: reply?.root.cid ?? '',
        replyParent: reply?.parent.uri ?? '',
        replyParentCid: reply?.parent.cid ?? '',
        embed: embed ? JSON.stringify(embed) : '',
        langs: langs?.join(',') ?? '',
        labels: labels ? JSON.stringify(labels) : '',
        tags: tags?.join(',') ?? '',
        createdAt,
        expectedCid: expectedCid ?? '',
      })

      // If client provided expectedCid but it didn't match, reject
      if (expectedCid && !cidVerified) {
        throw new InvalidRequestError(
          `CID mismatch: expected ${expectedCid}, computed ${cid}`,
          'CidMismatch',
        )
      }

      return {
        encoding: 'application/json' as const,
        body: {
          uri,
          cid,
        },
      }
    },
  })
}
