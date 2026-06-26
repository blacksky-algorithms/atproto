import { AtUriString } from '@atproto/syntax'
import { Server } from '@atproto/xrpc-server'
import { ServerConfig } from '../../../../config.js'
import { AppContext } from '../../../../context.js'
import {
  Code,
  DataPlaneClient,
  isDataplaneError,
} from '../../../../data-plane/index.js'
import { HydrateCtx, Hydrator } from '../../../../hydration/hydrator.js'
import { app } from '../../../../lexicons/index.js'
import {
  HydrationFnInput,
  PresentationFnInput,
  SkeletonFnInput,
  createPipeline,
  noRules,
} from '../../../../pipeline.js'
import { postUriToThreadgateUri } from '../../../../util/uris.js'
import { Views } from '../../../../views/index.js'
import {
  buildCommunityPostView,
  isCommunityPostUri,
} from '../../../community/blacksky/views/communityPostView.js'
import { resHeaders } from '../../../util.js'

export default function (server: Server, ctx: AppContext) {
  const getPostThread = createPipeline(
    skeleton,
    hydration,
    noRules, // handled in presentation: 3p block-violating replies are turned to #blockedPost, viewer blocks turned to #notFoundPost.
    presentation,
  )
  server.add(app.bsky.unspecced.getPostThreadV2, {
    auth: ctx.authVerifier.optionalStandardOrRole,
    handler: async ({ params, auth, req }) => {
      const { viewer, includeTakedowns, include3pBlocks, skipViewerBlocks } =
        ctx.authVerifier.parseCreds(auth)
      const labelers = ctx.reqLabelers(req)
      const features = ctx.featureGatesClient.scope(
        ctx.featureGatesClient.parseUserContextFromHandler({
          viewer,
          req,
        }),
      )
      // temp
      void features.checkGate(features.Gate.AATest)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
        includeTakedowns,
        include3pBlocks,
        skipViewerBlocks,
        features,
      })

      // Community posts live in a separate dataplane table and aren't
      // reachable through the standard post/thread hydration pipeline, so
      // a community URI falls through to threadItemNotFound. Synthesize a
      // single-anchor thread item from the community post row directly.
      if (isCommunityPostUri(params.anchor)) {
        const { post } = await ctx.dataplane.getCommunityPost({
          uri: params.anchor as AtUriString,
        })
        if (!post) {
          return {
            encoding: 'application/json',
            body: {
              hasOtherReplies: false,
              thread: [
                {
                  uri: params.anchor,
                  depth: 0,
                  value: {
                    $type: 'app.bsky.unspecced.defs#threadItemNotFound',
                  },
                },
              ],
            } as any,
          }
        }
        const helperCtx = {
          hydrator: ctx.hydrator,
          views: ctx.views,
          dataplane: ctx.dataplane,
        }
        const anchorView = await buildCommunityPostView(
          helperCtx,
          hydrateCtx,
          post,
        )
        const repliesRes = await ctx.dataplane.getCommunityPostReplies({
          parentUri: params.anchor as AtUriString,
          limit: Math.min(params.below ?? 50, 200),
        })
        const descendants = repliesRes.posts ?? []
        // Assign depth by walking the replyParent chain back to the anchor.
        const uriToParent = new Map<string, string>(
          descendants.map((r: any) => [r.uri, r.replyParent || params.anchor]),
        )
        const depthFor = (uri: string): number => {
          let d = 0
          let cur = uri
          while (cur !== params.anchor && d < 20) {
            const p = uriToParent.get(cur)
            if (!p) break
            cur = p
            d++
          }
          return d
        }
        const sortedDesc = [...descendants].sort((a: any, b: any) => {
          const t = (a.createdAt ?? '').localeCompare(b.createdAt ?? '')
          return t !== 0 ? t : (a.uri ?? '').localeCompare(b.uri ?? '')
        })
        const replyViews = await Promise.all(
          sortedDesc.map(async (r: any) => ({
            view: await buildCommunityPostView(helperCtx, hydrateCtx, r),
            depth: depthFor(r.uri),
          })),
        )
        return {
          encoding: 'application/json',
          body: {
            hasOtherReplies: false,
            thread: [
              {
                uri: post.uri,
                depth: 0,
                value: {
                  $type: 'app.bsky.unspecced.defs#threadItemPost',
                  post: anchorView,
                  moreParents: false,
                  moreReplies: 0,
                  opThread: true,
                  hiddenByThreadgate: false,
                  mutedByViewer: false,
                },
              },
              ...replyViews.map(({ view, depth }) => ({
                uri: (view as any).uri,
                depth,
                value: {
                  $type: 'app.bsky.unspecced.defs#threadItemPost',
                  post: view,
                  moreParents: false,
                  moreReplies: 0,
                  opThread: false,
                  hiddenByThreadgate: false,
                  mutedByViewer: false,
                },
              })),
            ],
          } as any,
          headers: resHeaders({ labelers: hydrateCtx.labelers }),
        }
      }

      return {
        encoding: 'application/json',
        body: await getPostThread({ ...params, hydrateCtx }, ctx),
        headers: resHeaders({
          labelers: hydrateCtx.labelers,
        }),
      }
    },
  })
}

const skeleton = async (
  inputs: SkeletonFnInput<Context, Params>,
): Promise<Skeleton> => {
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
      uris: res.uris as AtUriString[],
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

type Params = app.bsky.unspecced.getPostThreadV2.$Params & {
  hydrateCtx: HydrateCtx
}

type Skeleton = {
  anchor: AtUriString
  uris: AtUriString[]
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
