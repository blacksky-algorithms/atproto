import { AuthRequiredError, type Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { communityPostsEnabled } from '../membership-guard.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'
import { isSpaceRecordUri } from '../space-uri.js'
import { toSpaceFeedViewPost } from '../views/spaceViews.js'
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
      const res =
        params.sort === 'hot'
          ? await ctx.dataplane.getCommunityHotTimeline({
              limit,
              cursor: params.cursor,
            })
          : await ctx.dataplane.getCommunityTimeline({
              limit,
              cursor: params.cursor,
            })
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })
      const helperCtx = ctx
      const hydratedPosts = await Promise.all(
        res.posts.map((post) =>
          buildCommunityPostView(
            helperCtx as any,
            hydrateCtx,
            post as any,
            0,
            requesterDid,
          ),
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
            const item = reply ? { post, reply } : { post }
            // The timeline serves legacy community posts and space posts side
            // by side. Only space posts are retagged to the space view types:
            // a legacy post keeps `app.bsky.feed.defs#postView` byte-for-byte
            // so clients released before spaces keep rendering it.
            return isSpaceRecordUri(row.uri) ? toSpaceFeedViewPost(item) : item
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
