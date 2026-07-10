import { AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { communityPostsEnabled } from '../membership-guard.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
} from '../views/communityPostView.js'

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
            if (isBlockedForViewer(post)) return null
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

async function buildReplyContext(
  helperCtx: any,
  hydrateCtx: any,
  row: any,
  viewerDid?: string,
) {
  const parentUri = row.replyParent || ''
  const rootUri = row.replyRoot || ''
  if (!parentUri) return undefined
  const [parentRes, rootRes] = await Promise.all([
    helperCtx.dataplane.getCommunityPost({ uri: parentUri }),
    rootUri && rootUri !== parentUri
      ? helperCtx.dataplane.getCommunityPost({ uri: rootUri })
      : Promise.resolve(null),
  ])
  if (!parentRes?.post) return undefined
  const parentView = await buildCommunityPostView(
    helperCtx,
    hydrateCtx,
    parentRes.post,
    0,
    viewerDid,
  )
  const rootView =
    rootRes?.post
      ? await buildCommunityPostView(
          helperCtx,
          hydrateCtx,
          rootRes.post,
          0,
          viewerDid,
        )
      : parentView
  // A blocked parent/root must not surface through reply context.
  if (isBlockedForViewer(parentView) || isBlockedForViewer(rootView)) {
    return undefined
  }
  return { root: rootView, parent: parentView }
}
