import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { canViewCommunityPost } from '../tenant-gate.js'
import { buildCommunityPostView } from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityPost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const requesterDid = auth.credentials.iss
      const res = await ctx.dataplane.getCommunityPost({ uri: params.uri })
      if (!res.post) {
        throw new InvalidRequestError('Post not found', 'PostNotFound')
      }
      if (!(await canViewCommunityPost(ctx, res.post, requesterDid))) {
        throw new AuthRequiredError(
          res.post.feedUri
            ? 'Must have access to the community feed'
            : 'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })
      const helperCtx = ctx
      const post = await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx,
        res.post as any,
        0,
        requesterDid,
      )
      if (!post) {
        throw new AuthRequiredError(
          'Must have access to the community feed',
          'MembershipRequired',
        )
      }
      return {
        encoding: 'application/json' as const,
        body: { post } as any,
      }
    },
  })
}
