import {
  AuthRequiredError,
  InvalidRequestError,
  type Server,
} from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { assertCommunityMembershipForUris } from '../membership-guard.js'
import { toSpacePostView } from '../views/spaceViews.js'
import { buildCommunityPostView } from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityPost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const requesterDid = auth.credentials.iss
      const allowedSpaceUris = await assertCommunityMembershipForUris(
        ctx,
        requesterDid,
        [params.uri],
      )
      const res = await ctx.dataplane.getCommunityPost({
        uri: params.uri,
        allowedSpaceUris,
      })
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
        0,
        requesterDid,
        undefined,
        new Set(allowedSpaceUris),
      )
      return {
        encoding: 'application/json' as const,
        body: { post: toSpacePostView(post) } as any,
      }
    },
  })
}
