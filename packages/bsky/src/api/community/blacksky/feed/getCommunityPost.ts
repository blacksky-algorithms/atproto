import { InvalidRequestError, AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AtUriString } from '@atproto/lex'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { buildCommunityPostView } from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityPost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
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
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })
      const helperCtx = {
        hydrator: ctx.hydrator,
        views: ctx.views,
        dataplane: ctx.dataplane,
      }
      const post = await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx,
        res.post as any,
      )
      return {
        encoding: 'application/json' as const,
        body: { post } as any,
      }
    },
  })
}
