import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.getCommunityPost({
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth }) => {
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

      const res = await ctx.dataplane.getCommunityPost({ uri: params.uri })
      if (!res.post) {
        throw new InvalidRequestError('Post not found', 'PostNotFound')
      }

      const post = res.post
      return {
        encoding: 'application/json' as const,
        body: {
          post: {
            uri: post.uri,
            cid: post.cid || undefined,
            creator: post.creator,
            text: post.text,
            facets: post.facets ? JSON.parse(post.facets) : undefined,
            replyRoot: post.replyRoot || undefined,
            replyParent: post.replyParent || undefined,
            embed: post.embed ? JSON.parse(post.embed) : undefined,
            langs: post.langs
              ? post.langs.replace(/[{}]/g, '').split(',').filter(Boolean)
              : undefined,
            labels: post.labels ? JSON.parse(post.labels) : undefined,
            tags: post.tags
              ? post.tags.replace(/[{}]/g, '').split(',').filter(Boolean)
              : undefined,
            createdAt: post.createdAt,
            indexedAt: post.indexedAt,
          },
        },
      }
    },
  })
}
