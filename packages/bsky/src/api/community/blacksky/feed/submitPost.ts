import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.submitPost({
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      console.log('[submitPost] START', { hasAuth: !!auth })

      const requesterDid = auth.credentials.iss
      console.log('[submitPost] requesterDid:', requesterDid)

      console.log('[submitPost] checking membership...')
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: requesterDid,
      })
      console.log('[submitPost] membership check result:', { isMember })

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

      console.log('[submitPost] input:', {
        rkey,
        text: text?.substring(0, 50),
        hasReply: !!reply,
        hasEmbed: !!embed,
        langs,
        createdAt,
        expectedCid,
      })

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
      console.log('[submitPost] generated uri:', uri)

      console.log('[submitPost] calling dataplane.submitCommunityPost...')
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
      console.log('[submitPost] dataplane result:', { cid, cidVerified })

      // If client provided expectedCid but it didn't match, reject
      if (expectedCid && !cidVerified) {
        throw new InvalidRequestError(
          `CID mismatch: expected ${expectedCid}, computed ${cid}`,
          'CidMismatch',
        )
      }

      console.log('[submitPost] SUCCESS')
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
