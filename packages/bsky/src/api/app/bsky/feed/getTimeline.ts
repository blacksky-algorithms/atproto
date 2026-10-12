import { mapDefined } from '@atproto/common'
import type { AtUriString } from '@atproto/syntax'
import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import type { FeedItem } from '../../../../hydration/feed.js'
import type {
  HydrateCtxWithViewer,
  HydrationState,
} from '../../../../hydration/hydrator.js'
import { parseString } from '../../../../hydration/util.js'
import { app } from '../../../../lexicons/index.js'
import { createPipeline } from '../../../../pipeline.js'
import type { CommunityPostView } from '../../../../proto/bsky_pb.js'
import type { Views } from '../../../../views/index.js'
import {
  communityRepostReason,
  feedItemKey,
  hydrateCommunityReposts,
  presentCommunityFeedItem,
  resolveCommunityMembership,
} from '../../../community/blacksky/feed/mergedCommunityItems.js'
import { isCommunityUri } from '../../../community/blacksky/membership-guard.js'
import { clearlyBadCursor, resHeaders } from '../../../util.js'

type FeedViewItem = ReturnType<Views['feedViewPost']>

export default function (server: Server, ctx: AppContext) {
  const getTimeline = createPipeline(
    skeleton,
    hydration,
    noBlocksOrMutes,
    presentation,
  )
  server.add(app.bsky.feed.getTimeline, {
    auth: ctx.authVerifier.standard,
    opts: {
      // @TODO remove after grace period has passed, behavior is non-standard.
      // temporarily added for compat w/ previous version of xrpc-server to avoid breakage of a few specified parties.
      paramsParseLoose: true,
    },
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({ labelers, viewer })
      const isCommunityMember = await resolveCommunityMembership(ctx, viewer)

      const result = await getTimeline(
        { ...params, hydrateCtx, isCommunityMember },
        ctx,
      )

      const repoRev = await ctx.hydrator.actor.getRepoRevSafe(viewer)

      return {
        encoding: 'application/json',
        body: result,
        headers: resHeaders({ labelers: hydrateCtx.labelers, repoRev }),
      }
    },
  })
}

export const skeleton = async (inputs: {
  ctx: Context
  params: Params
}): Promise<Skeleton> => {
  const { ctx, params } = inputs
  if (clearlyBadCursor(params.cursor)) {
    return { items: [] }
  }
  const res = await ctx.dataplane.getTimeline({
    actorDid: params.hydrateCtx.viewer,
    limit: params.limit,
    cursor: params.cursor,
    includeCommunityPosts: params.isCommunityMember,
  })
  const items: FeedItem[] = res.items.map((item) => ({
    post: { uri: item.uri as AtUriString, cid: item.cid || undefined },
    repost: item.repost
      ? { uri: item.repost as AtUriString, cid: item.repostCid || undefined }
      : undefined,
  }))
  return {
    // The data plane already withholds community items from non-members;
    // this keeps that true if a skeleton source ever stops doing so.
    items: params.isCommunityMember
      ? items
      : items.filter((item) => !isCommunityUri(item.post.uri)),
    communityRows: params.isCommunityMember
      ? new Map(res.communityPosts.map((row) => [row.uri, row]))
      : undefined,
    cursor: parseString(res.cursor),
  }
}

export const hydration = async (inputs: {
  ctx: Context
  params: Params
  skeleton: Skeleton
}): Promise<HydrationState> => {
  const { ctx, params, skeleton } = inputs
  const standardItems = skeleton.items.filter(
    (item) => !isCommunityUri(item.post.uri),
  )
  const [state] = await Promise.all([
    ctx.hydrator.hydrateFeedItems(standardItems, params.hydrateCtx),
    buildCommunityViews(ctx, params, skeleton),
  ])
  return state
}

// Community items are built through the community view path (presentation is
// synchronous, so views are prepared here) and spliced by position later.
// Blocked/muted authors, hidden reposters and broken replies come back
// undefined and drop.
const buildCommunityViews = async (
  ctx: Context,
  params: Params,
  skeleton: Skeleton,
) => {
  const communityItems = skeleton.items.filter((item) =>
    isCommunityUri(item.post.uri),
  )
  if (communityItems.length === 0 || !skeleton.communityRows?.size) return
  const repostState = await hydrateCommunityReposts(
    ctx,
    params.hydrateCtx,
    communityItems,
  )
  const entries = await Promise.all(
    communityItems.map(async (item) => {
      const row = skeleton.communityRows?.get(item.post.uri)
      if (!row) return undefined
      let reason
      if (item.repost) {
        const result = communityRepostReason(
          ctx,
          item,
          repostState,
          'following',
        )
        if (result === 'drop') return undefined
        reason = result
      }
      const view = await presentCommunityFeedItem(
        ctx,
        params.hydrateCtx,
        row,
        params.hydrateCtx.viewer,
        { reason },
      )
      return view ? ([feedItemKey(item), view] as const) : undefined
    }),
  )
  skeleton.communityViews = new Map(
    entries.flatMap((entry) => (entry ? [entry] : [])),
  )
}

export const noBlocksOrMutes = (inputs: {
  ctx: Context
  skeleton: Skeleton
  hydration: HydrationState
}): Skeleton => {
  const { ctx, skeleton, hydration } = inputs
  skeleton.items = skeleton.items.filter((item) => {
    const bam = ctx.views.feedItemBlocksAndMutes(item, hydration)
    return (
      !bam.authorBlocked &&
      !bam.authorMuted &&
      !bam.authorQuotepostMuted &&
      !bam.originatorBlocked &&
      !bam.originatorMuted &&
      !bam.originatorRepostMuted &&
      !bam.ancestorAuthorBlocked
    )
  })
  return skeleton
}

export const presentation = (inputs: {
  ctx: Context
  skeleton: Skeleton
  hydration: HydrationState
}) => {
  const { ctx, skeleton, hydration } = inputs
  const feed = mapDefined(skeleton.items, (item) => {
    if (isCommunityUri(item.post.uri)) {
      // Community items render only through their pre-built views; a
      // missing view means the item was dropped, never rendered publicly.
      return skeleton.communityViews?.get(feedItemKey(item)) as FeedViewItem
    }
    return ctx.views.feedViewPost(item, hydration)
  })
  return { feed, cursor: skeleton.cursor }
}

type Context = Pick<
  AppContext,
  'cfg' | 'dataplane' | 'hydrator' | 'idResolver' | 'signingKey' | 'views'
>

type Params = app.bsky.feed.getTimeline.$Params & {
  hydrateCtx: HydrateCtxWithViewer
  isCommunityMember: boolean
}

type Skeleton = {
  items: FeedItem[]
  communityRows?: Map<string, CommunityPostView>
  communityViews?: Map<string, Record<string, unknown>>
  cursor?: string
}
