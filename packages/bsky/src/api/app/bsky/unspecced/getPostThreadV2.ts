import { AtUri } from '@atproto/syntax'
import { ServerConfig } from '../../../../config'
import { AppContext } from '../../../../context'
import { Code, DataPlaneClient, isDataplaneError } from '../../../../data-plane'
import { HydrateCtx, Hydrator } from '../../../../hydration/hydrator'
import { Server } from '../../../../lexicon'
import {
  OutputSchema,
  QueryParams,
  ThreadItem,
} from '../../../../lexicon/types/app/bsky/unspecced/getPostThreadV2'
import {
  HydrationFnInput,
  PresentationFnInput,
  SkeletonFnInput,
  createPipeline,
  noRules,
} from '../../../../pipeline'
import { postUriToThreadgateUri } from '../../../../util/uris'
import { Views } from '../../../../views'
import { resHeaders } from '../../../util'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  const getPostThread = createPipeline(
    skeleton,
    hydration,
    noRules, // handled in presentation: 3p block-violating replies are turned to #blockedPost, viewer blocks turned to #notFoundPost.
    presentation,
  )
  server.app.bsky.unspecced.getPostThreadV2({
    auth: ctx.authVerifier.optionalStandardOrRole,
    handler: async ({ params, auth, req }) => {
      const { viewer, includeTakedowns, include3pBlocks } =
        ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
        includeTakedowns,
        include3pBlocks,
        featureGates: ctx.featureGates.checkGates(
          [ctx.featureGates.ids.ThreadsReplyRankingExplorationEnable],
          ctx.featureGates.userContext({ did: viewer }),
        ),
      })

      // Community posts live in a separate table; handle them directly
      // rather than going through the standard dataplane pipeline.
      const anchor = await ctx.hydrator.resolveUri(params.anchor)
      const anchorAtUri = new AtUri(anchor)
      if (anchorAtUri.collection === COMMUNITY_POST_COLLECTION) {
        const body = await communityThread(ctx, anchor, hydrateCtx)
        return {
          encoding: 'application/json' as const,
          body,
          headers: resHeaders({ labelers: hydrateCtx.labelers }),
        }
      }

      const result = await getPostThread({ ...params, hydrateCtx }, ctx)

      // Post-process thread to hydrate any community post parents that were
      // returned as "not found" from the standard pipeline.
      const hydratedThread = await hydrateCommunityParents(
        ctx,
        result.thread,
        hydrateCtx,
      )

      return {
        encoding: 'application/json',
        body: { ...result, thread: hydratedThread },
        headers: resHeaders({
          labelers: hydrateCtx.labelers,
        }),
      }
    },
  })
}

const skeleton = async (inputs: SkeletonFnInput<Context, Params>) => {
  const { ctx, params } = inputs
  const anchor = await ctx.hydrator.resolveUri(params.anchor)
  try {
    const res = await ctx.dataplane.getThread({
      postUri: anchor,
      above: calculateAbove(ctx, params),
      below: calculateBelow(ctx, anchor, params),
    })
    return {
      anchor,
      uris: res.uris,
    }
  } catch (err) {
    if (isDataplaneError(err, Code.NotFound)) {
      return {
        anchor,
        uris: [],
      }
    } else {
      throw err
    }
  }
}

const hydration = async (
  inputs: HydrationFnInput<Context, Params, Skeleton>,
) => {
  const { ctx, params, skeleton } = inputs
  return ctx.hydrator.hydrateThreadPosts(
    skeleton.uris.map((uri) => ({ uri })),
    params.hydrateCtx,
  )
}

const presentation = (
  inputs: PresentationFnInput<Context, Params, Skeleton>,
) => {
  const { ctx, params, skeleton, hydration } = inputs
  const { hasOtherReplies, thread } = ctx.views.threadV2(skeleton, hydration, {
    above: calculateAbove(ctx, params),
    below: calculateBelow(ctx, skeleton.anchor, params),
    branchingFactor: params.branchingFactor,
    sort: params.sort,
  })

  const rootUri =
    hydration.posts?.get(skeleton.anchor)?.record.reply?.root.uri ??
    skeleton.anchor
  const threadgate = ctx.views.threadgate(
    postUriToThreadgateUri(rootUri),
    hydration,
  )
  return { hasOtherReplies, thread, threadgate }
}

type Context = {
  dataplane: DataPlaneClient
  hydrator: Hydrator
  views: Views
  cfg: ServerConfig
}

type Params = QueryParams & { hydrateCtx: HydrateCtx }

type Skeleton = {
  anchor: string
  uris: string[]
}

const calculateAbove = (ctx: Context, params: Params) => {
  return params.above ? ctx.cfg.maxThreadParents : 0
}

const calculateBelow = (ctx: Context, anchor: string, params: Params) => {
  let maxDepth = ctx.cfg.maxThreadDepth
  if (ctx.cfg.bigThreadUris.has(anchor) && ctx.cfg.bigThreadDepth) {
    maxDepth = ctx.cfg.bigThreadDepth
  }
  return maxDepth ? Math.min(maxDepth, params.below) : params.below
}

// ---------------------------------------------------------------------------
// Community post thread
// ---------------------------------------------------------------------------

function parsePgArray(val: string | null): string[] | undefined {
  if (!val) return undefined
  return val
    .replace(/[{}]/g, '')
    .split(',')
    .filter(Boolean)
}

async function communityThread(
  ctx: AppContext,
  anchor: string,
  hydrateCtx: HydrateCtx,
): Promise<OutputSchema> {
  const notFound: OutputSchema = {
    hasOtherReplies: false,
    thread: [
      {
        uri: anchor,
        depth: 0,
        value: {
          $type: 'app.bsky.unspecced.defs#threadItemNotFound',
        },
      },
    ],
  }

  const res = await ctx.dataplane.getCommunityPost({ uri: anchor })
  if (!res.post) return notFound

  const post = res.post

  // Hydrate author profile through the standard pipeline, with fallback
  const profileState = await ctx.hydrator.hydrateProfilesBasic(
    [post.creator],
    hydrateCtx,
  )
  const author = ctx.views.profileBasic(post.creator, profileState) ?? {
    did: post.creator,
    handle: 'handle.invalid',
    labels: [],
  }

  // Build an app.bsky.feed.post-shaped record from the community row
  const facets = post.facets ? JSON.parse(post.facets) : undefined
  const embed = post.embed ? JSON.parse(post.embed) : undefined
  const langs = post.langs ? parsePgArray(post.langs) : undefined
  const record: Record<string, unknown> = {
    $type: 'app.bsky.feed.post',
    text: post.text,
    createdAt: post.createdAt,
  }
  if (facets) record.facets = facets
  if (langs) record.langs = langs
  if (embed) record.embed = embed
  if (post.replyRoot) {
    record.reply = {
      root: { uri: post.replyRoot, cid: post.replyRootCid || '' },
      parent: {
        uri: post.replyParent || post.replyRoot,
        cid: post.replyParentCid || post.replyRootCid || '',
      },
    }
  }

  const postView = {
    uri: post.uri,
    cid: post.cid || '',
    author,
    record,
    indexedAt: post.indexedAt,
    likeCount: 0,
    repostCount: 0,
    replyCount: 0,
    quoteCount: 0,
    bookmarkCount: 0,
    labels: [],
  }

  return {
    hasOtherReplies: false,
    thread: [
      {
        uri: anchor,
        depth: 0,
        value: {
          $type: 'app.bsky.unspecced.defs#threadItemPost',
          post: postView,
          opThread: true,
          moreParents: false,
          moreReplies: 0,
          hiddenByThreadgate: false,
          mutedByViewer: false,
        },
      },
    ],
  }
}

// ---------------------------------------------------------------------------
// Hydrate community post parents in thread
// ---------------------------------------------------------------------------

async function hydrateCommunityParents(
  ctx: AppContext,
  thread: ThreadItem[],
  hydrateCtx: HydrateCtx,
): Promise<ThreadItem[]> {
  // Find "not found" items with negative depth (parents) that are community posts.
  // Quick string check before parsing URI to minimize overhead for regular threads.
  const communityNotFoundParents = thread.filter(
    (item) =>
      item.depth < 0 &&
      item.value.$type === 'app.bsky.unspecced.defs#threadItemNotFound' &&
      item.uri.includes(COMMUNITY_POST_COLLECTION),
  )

  if (communityNotFoundParents.length === 0) {
    return thread
  }

  // Hydrate community post parents
  const hydratedItems = new Map<string, ThreadItem>()
  for (const item of communityNotFoundParents) {
    // Double-check with proper URI parsing
    const itemUri = new AtUri(item.uri)
    if (itemUri.collection !== COMMUNITY_POST_COLLECTION) {
      continue
    }

    // Fetch and hydrate the community post
    const res = await ctx.dataplane.getCommunityPost({ uri: item.uri })
    if (!res.post) {
      continue
    }

    const post = res.post

    // Hydrate author profile
    const profileState = await ctx.hydrator.hydrateProfilesBasic(
      [post.creator],
      hydrateCtx,
    )
    const author = ctx.views.profileBasic(post.creator, profileState) ?? {
      did: post.creator,
      handle: 'handle.invalid',
      labels: [],
    }

    // Build the record
    const facets = post.facets ? JSON.parse(post.facets) : undefined
    const embed = post.embed ? JSON.parse(post.embed) : undefined
    const langs = post.langs ? parsePgArray(post.langs) : undefined
    const record: Record<string, unknown> = {
      $type: 'app.bsky.feed.post',
      text: post.text,
      createdAt: post.createdAt,
    }
    if (facets) record.facets = facets
    if (langs) record.langs = langs
    if (embed) record.embed = embed
    if (post.replyRoot) {
      record.reply = {
        root: { uri: post.replyRoot, cid: post.replyRootCid || '' },
        parent: {
          uri: post.replyParent || post.replyRoot,
          cid: post.replyParentCid || post.replyRootCid || '',
        },
      }
    }

    const postView = {
      uri: post.uri,
      cid: post.cid || '',
      author,
      record,
      indexedAt: post.indexedAt,
      likeCount: 0,
      repostCount: 0,
      replyCount: 0,
      quoteCount: 0,
      bookmarkCount: 0,
      labels: [],
    }

    hydratedItems.set(item.uri, {
      uri: item.uri,
      depth: item.depth,
      value: {
        $type: 'app.bsky.unspecced.defs#threadItemPost',
        post: postView,
        opThread: false,
        moreParents: false,
        moreReplies: 0,
        hiddenByThreadgate: false,
        mutedByViewer: false,
      },
    })
  }

  if (hydratedItems.size === 0) {
    return thread
  }

  // Replace not found items with hydrated community posts
  return thread.map((item) => hydratedItems.get(item.uri) ?? item)
}
