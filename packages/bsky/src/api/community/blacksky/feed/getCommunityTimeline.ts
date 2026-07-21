import { AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { communityPostsEnabled } from '../membership-guard.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'
import { buildReplyContext } from './mergedCommunityItems.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityTimeline, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const requesterDid = auth.credentials.iss
      if (!communityPostsEnabled()) {
        throw new AuthRequiredError(
          'Community posts are not available',
          'MembershipRequired',
        )
      }
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: requesterDid,
      })
      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }
      const limit = params.limit ?? 50
      const res = await ctx.dataplane.getCommunityTimeline({
        limit,
        cursor: params.cursor,
      })
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
      const hydratedPosts = await Promise.all(
        res.posts.map((post) =>
          buildCommunityPostView(helperCtx as any, hydrateCtx, post as any, 0, requesterDid),
        ),
      )
      const feed = (
        await Promise.all(
          res.posts.map(async (row: any, i: number) => {
            const post = hydratedPosts[i]
            if (isBlockedForViewer(post) || isMutedForViewer(post)) {
              return null
            }
            const reply = await buildReplyContext(
              helperCtx,
              hydrateCtx,
              row,
              requesterDid,
            )
            return reply ? { post, reply } : { post }
          }),
        )
      ).filter(Boolean)
      return {
        encoding: 'application/json' as const,
        body: { cursor: res.cursor || undefined, feed } as any,
      }
    },
  })
}

