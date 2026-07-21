import { mapDefined } from '@atproto/common'
import { AtUriString } from '@atproto/lex'
import { InvalidRequestError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { DataPlaneClient } from '../../../../data-plane/index.js'
import { Actor } from '../../../../hydration/actor.js'
import { FeedItem, Post } from '../../../../hydration/feed.js'
import {
  HydrateCtx,
  HydrationState,
  Hydrator,
  mergeStates,
} from '../../../../hydration/hydrator.js'
import { parseString } from '../../../../hydration/util.js'
import { app } from '../../../../lexicons/index.js'
import { createPipeline } from '../../../../pipeline.js'
import { CommunityPostView, FeedType } from '../../../../proto/bsky_pb.js'
import { safePinnedPost, uriToDid } from '../../../../util/uris.js'
import { Views } from '../../../../views/index.js'
import { isCommunityUri } from '../../../community/blacksky/membership-guard.js'
import {
  presentCommunityFeedItem,
  resolveCommunityMembership,
} from '../../../community/blacksky/feed/mergedCommunityItems.js'
import { clearlyBadCursor, resHeaders } from '../../../util.js'

type FeedViewItem = ReturnType<Views['feedViewPost']>

export default function (server: Server, ctx: AppContext) {
  const getAuthorFeed = createPipeline(
    skeleton,
    hydration,
    noBlocksOrMutedReposts,
    presentation,
  )
  server.add(app.bsky.feed.getAuthorFeed, {
    auth: ctx.authVerifier.optionalStandardOrRole,
    handler: async ({ params, auth, req }) => {
      const { viewer, includeTakedowns, skipViewerBlocks } =
        ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
        includeTakedowns,
        skipViewerBlocks,
      })
      const isCommunityMember = await resolveCommunityMembership(ctx, viewer)

      const result = await getAuthorFeed(
        { ...params, hydrateCtx, isCommunityMember },
        ctx,
      )

      const repoRev = await ctx.hydrator.actor.getRepoRevSafe(viewer)

      return {
        encoding: 'application/json',
        body: result,
        headers: resHeaders({
          repoRev,
          labelers: hydrateCtx.labelers,
        }),
      }
    },
  })
}

const FILTER_TO_FEED_TYPE = {
  posts_with_replies: undefined, // default: all posts, replies, and reposts
  posts_no_replies: FeedType.POSTS_NO_REPLIES,
  posts_with_media: FeedType.POSTS_WITH_MEDIA,
  posts_and_author_threads: FeedType.POSTS_AND_AUTHOR_THREADS,
  posts_with_video: FeedType.POSTS_WITH_VIDEO,
}

export const skeleton = async (inputs: {
  ctx: Context
  params: Params
}): Promise<Skeleton> => {
  const { ctx, params } = inputs
  const [did] = await ctx.hydrator.actor.getDids([params.actor])
  if (!did) {
    throw new InvalidRequestError('Profile not found')
  }
  const actors = await ctx.hydrator.actor.getActors([did], {
    includeTakedowns: params.hydrateCtx.includeTakedowns,
    skipCacheForDids: params.hydrateCtx.skipCacheForViewer,
  })
  const actor = actors.get(did)
  if (!actor) {
    throw new InvalidRequestError('Profile not found')
  }
  if (clearlyBadCursor(params.cursor)) {
    return { actor, filter: params.filter, items: [] }
  }

  const pinnedPost = safePinnedPost(actor.profile?.pinnedPost)
  const isFirstPageRequest = !params.cursor
  const shouldInsertPinnedPost =
    isFirstPageRequest &&
    params.includePins &&
    pinnedPost &&
    uriToDid(pinnedPost.uri) === actor.did

  const res = await ctx.dataplane.getAuthorFeed({
    actorDid: did,
    limit: params.limit,
    cursor: params.cursor,
    feedType: FILTER_TO_FEED_TYPE[params.filter],
    includeCommunityPosts: params.isCommunityMember,
  })

  let items: FeedItem[] = res.items.map((item) => ({
    post: { uri: item.uri as AtUriString, cid: item.cid || undefined },
    repost: item.repost
      ? { uri: item.repost as AtUriString, cid: item.repostCid || undefined }
      : undefined,
  }))

  if (shouldInsertPinnedPost && pinnedPost) {
    const pinnedItem = {
      post: {
        uri: pinnedPost.uri,
        cid: pinnedPost.cid,
      },
      authorPinned: true,
    }

    items = items.filter((item) => item.post.uri !== pinnedItem.post.uri)
    items.unshift(pinnedItem)
  }

  return {
    actor,
    filter: params.filter,
    items,
    communityRows: params.isCommunityMember
      ? new Map(res.communityPosts.map((row) => [row.uri, row]))
      : undefined,
    cursor: parseString(res.cursor),
  }
}

const hydration = async (inputs: {
  ctx: Context
  params: Params
  skeleton: Skeleton
}): Promise<HydrationState> => {
  const { ctx, params, skeleton } = inputs
  const standardItems = skeleton.items.filter(
    (item) => !isCommunityUri(item.post.uri),
  )
  const [feedPostState, profileViewerState] = await Promise.all([
    ctx.hydrator.hydrateFeedItems(standardItems, params.hydrateCtx),
    ctx.hydrator.hydrateProfileViewers([skeleton.actor.did], params.hydrateCtx),
    buildCommunityViews(ctx, params, skeleton),
  ])
  return mergeStates(feedPostState, profileViewerState)
}

// Community items are built through the community view path (presentation is
// synchronous, so views are prepared here) and spliced by position later.
// Blocked/muted authors and broken replies come back undefined and drop.
const buildCommunityViews = async (
  ctx: Context,
  params: Params,
  skeleton: Skeleton,
) => {
  if (!skeleton.communityRows?.size) return
  const helperCtx = {
    hydrator: ctx.hydrator,
    views: ctx.views,
    dataplane: ctx.dataplane,
  }
  const entries = await Promise.all(
    [...skeleton.communityRows.values()].map(
      async (row) =>
        [
          row.uri,
          await presentCommunityFeedItem(
            helperCtx,
            params.hydrateCtx,
            row,
            params.hydrateCtx.viewer ?? undefined,
          ),
        ] as const,
    ),
  )
  skeleton.communityViews = new Map(
    entries.flatMap(([uri, view]) => (view ? [[uri, view]] : [])),
  )
}

const noBlocksOrMutedReposts = (inputs: {
  ctx: Context
  skeleton: Skeleton
  hydration: HydrationState
}): Skeleton => {
  const { ctx, skeleton, hydration } = inputs
  const relationship = hydration.profileViewers?.get(skeleton.actor.did)
  if (
    relationship &&
    (relationship.blocking || ctx.views.blockingByList(relationship, hydration))
  ) {
    throw new InvalidRequestError(
      `Requester has blocked actor: ${skeleton.actor.did}`,
      'BlockedActor',
    )
  }
  if (
    relationship &&
    (relationship.blockedBy || ctx.views.blockedByList(relationship, hydration))
  ) {
    throw new InvalidRequestError(
      `Requester is blocked by actor: ${skeleton.actor.did}`,
      'BlockedByActor',
    )
  }

  const checkBlocksAndMutes = (item: FeedItem) => {
    const bam = ctx.views.feedItemBlocksAndMutes(item, hydration)
    return (
      !bam.authorBlocked &&
      !bam.originatorBlocked &&
      (!bam.authorMuted || bam.originatorMuted) // repost of muted content
    )
  }

  if (skeleton.filter === 'posts_and_author_threads') {
    // ensure replies are only included if the feed contains all
    // replies up to the thread root (i.e. a complete self-thread.)
    const selfThread = new SelfThreadTracker(
      skeleton.items,
      hydration,
      communityParentsFromRows(skeleton.communityRows),
    )
    skeleton.items = skeleton.items.filter((item) => {
      return (
        checkBlocksAndMutes(item) &&
        (item.repost || item.authorPinned || selfThread.ok(item.post.uri))
      )
    })
  } else {
    skeleton.items = skeleton.items.filter(checkBlocksAndMutes)
  }

  return skeleton
}

const presentation = (inputs: {
  ctx: Context
  skeleton: Skeleton
  hydration: HydrationState
}) => {
  const { ctx, skeleton, hydration } = inputs
  const feed = mapDefined(skeleton.items, (item) => {
    if (isCommunityUri(item.post.uri)) {
      // Community items render only through their pre-built views; a
      // missing view means the item was dropped, never rendered publicly.
      return skeleton.communityViews?.get(item.post.uri) as FeedViewItem
    }
    return ctx.views.feedViewPost(item, hydration)
  })
  return { feed, cursor: skeleton.cursor }
}

type Context = {
  hydrator: Hydrator
  views: Views
  dataplane: DataPlaneClient
}

type Params = app.bsky.feed.getAuthorFeed.$Params & {
  hydrateCtx: HydrateCtx
  isCommunityMember: boolean
}

type Skeleton = {
  actor: Actor
  items: FeedItem[]
  filter: app.bsky.feed.getAuthorFeed.$Params['filter']
  communityRows?: Map<string, CommunityPostView>
  communityViews?: Map<string, Record<string, unknown>>
  cursor?: string
}

// Parent linkage for community rows so SelfThreadTracker can walk community
// self-threads, which are absent from standard post hydration state.
const communityParentsFromRows = (
  rows?: Map<string, CommunityPostView>,
): Map<AtUriString, AtUriString | null> => {
  const map = new Map<AtUriString, AtUriString | null>()
  if (!rows) return map
  for (const row of rows.values()) {
    map.set(
      row.uri as AtUriString,
      row.replyParent ? (row.replyParent as AtUriString) : null,
    )
  }
  return map
}

class SelfThreadTracker {
  feedUris = new Set<AtUriString>()
  cache = new Map<AtUriString, boolean>()

  constructor(
    items: FeedItem[],
    private hydration: HydrationState,
    private communityParents: Map<AtUriString, AtUriString | null> = new Map(),
  ) {
    items.forEach((item) => {
      if (!item.repost) {
        this.feedUris.add(item.post.uri)
      }
    })
  }

  ok(uri: AtUriString, loop = new Set<AtUriString>()) {
    // if we've already checked this uri, pull from the cache
    if (this.cache.has(uri)) {
      return this.cache.get(uri) ?? false
    }
    // loop detection
    if (loop.has(uri)) {
      this.cache.set(uri, false)
      return false
    } else {
      loop.add(uri)
    }
    // cache through the result
    const result = this._ok(uri, loop)
    this.cache.set(uri, result)
    return result
  }

  private _ok(uri: AtUriString, loop: Set<AtUriString>): boolean {
    // must be in the feed to be in a self-thread
    if (!this.feedUris.has(uri)) {
      return false
    }
    // community posts live outside standard hydration; their parent
    // linkage rides along with the skeleton rows instead.
    if (this.communityParents.has(uri)) {
      const communityParent = this.communityParents.get(uri) ?? null
      if (communityParent === null) {
        return true
      }
      return this.ok(communityParent, loop)
    }
    // must be hydratable to be part of self-thread
    const post = this.hydration.posts?.get(uri)
    if (!post) {
      return false
    }
    // root posts (no parent) are trivial case of self-thread
    const parentUri = getParentUri(post)
    if (parentUri === null) {
      return true
    }
    // recurse w/ cache: this post is in a self-thread if its parent is.
    return this.ok(parentUri, loop)
  }
}

function getParentUri(post: Post) {
  return post.record.reply?.parent.uri ?? null
}
