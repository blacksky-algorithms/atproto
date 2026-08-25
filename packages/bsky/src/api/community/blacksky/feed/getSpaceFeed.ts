import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { resHeaders } from '../../../util.js'
import { communityPostsEnabled } from '../membership-guard.js'
import {
  canViewSpace,
  getCommunityFeedConfig,
  isSpaceBackedFeed,
} from '../tenant-gate.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'
import { toSpaceFeedViewPost } from '../views/spaceViews.js'
import { buildReplyContext } from './mergedCommunityItems.js'

/**
 * The private read contract for a space-backed feed.
 *
 * `app.bsky.feed.getFeed` cannot serve this: its response declares every post
 * URI `format: at-uri`, and a permissioned-space record URI is not one. The
 * feed generator record stays a public at-uri and remains the feed's
 * identifier; only the query a client chooses for it changes.
 *
 * Access is decided once here, for the space, and threaded into the view
 * builder. The space is the tenancy key — a feed is a view over one — so a
 * single `view` decision covers every row on the page.
 */
export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getSpaceFeed, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      if (!communityPostsEnabled() || !viewer) {
        throw new AuthRequiredError(
          'Must have access to this feed',
          'MembershipRequired',
        )
      }

      const config = await getCommunityFeedConfig(ctx, params.feed)
      if (!isSpaceBackedFeed(config)) {
        // Not an authorization answer: a feed with no space is a public
        // custom feed and belongs on the standard route. Saying so is safe,
        // because the config record it is read from is itself public (D19).
        throw new InvalidRequestError(
          'This feed is not backed by a permissioned space',
          'NotSpaceBacked',
        )
      }
      const spaceUri = config!.space!

      // One fail-closed live decision for the whole page. An unreachable space
      // host or managing app denies; the managing app refuses `view` for any
      // non-active space, so lifecycle is enforced without reading its
      // provisioning rows here. Staleness is bounded by the access cache TTL.
      const allowed = await canViewSpace(ctx, spaceUri, viewer)
      if (!allowed) {
        // Never an empty successful result: that would report "no posts" for
        // an access failure and leak the difference between an empty space and
        // a closed one in the opposite direction.
        throw new AuthRequiredError(
          'Must have access to this feed',
          'MembershipRequired',
        )
      }
      const preAuthorized = new Set([spaceUri])

      let res
      try {
        res = await ctx.dataplane.getCommunityFeedBySpace({
          spaceUri,
          limit: params.limit,
          cursor: params.cursor,
        })
      } catch (err) {
        // Only the route's own cursor rejection is the client's fault; a
        // dataplane failure must surface as such, not reset pagination.
        if (err instanceof Error && /invalid cursor/i.test(err.message)) {
          throw new InvalidRequestError('Invalid cursor')
        }
        throw err
      }

      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
      })
      const feed = (
        await Promise.all(
          res.posts.map(async (row: any) => {
            const post = await buildCommunityPostView(
              ctx as any,
              hydrateCtx,
              row,
              0,
              viewer,
              undefined,
              preAuthorized,
            )
            if (!post || isBlockedForViewer(post) || isMutedForViewer(post)) {
              return null
            }
            const reply = await buildReplyContext(
              ctx,
              hydrateCtx,
              row,
              viewer,
              preAuthorized,
            )
            return toSpaceFeedViewPost(reply ? { post, reply } : { post })
          }),
        )
      ).filter(Boolean)

      return {
        encoding: 'application/json' as const,
        body: { cursor: res.cursor || undefined, feed } as any,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
