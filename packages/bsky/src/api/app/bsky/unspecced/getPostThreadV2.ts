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

      // Community posts: synthesize the thread from community_post directly.
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
          0,
          viewer ?? undefined,
        )
        const threadRoot = post.replyRoot || post.uri
        const allInThreadRes = await ctx.dataplane.getCommunityPostReplies({
          parentUri: threadRoot as AtUriString,
          limit: 200,
        })
        const allInThread = allInThreadRes.posts ?? []
        const byUri = new Map<string, any>(allInThread.map((p: any) => [p.uri, p]))
        byUri.set(post.uri, post)

        const ancestorViews: Array<{ uri: string; view: unknown; depth: number }> = []
        if (params.above && post.replyParent) {
          const maxAbove = ctx.cfg.maxThreadParents ?? 80
          let parentUri: string | undefined = post.replyParent
          let depth = -1
          while (parentUri && -depth <= maxAbove) {
            let parentRow: any = byUri.get(parentUri)
            if (!parentRow) {
              const r = await ctx.dataplane.getCommunityPost({
                uri: parentUri as AtUriString,
              })
              if (!r.post) break
              parentRow = r.post
              byUri.set(parentRow.uri, parentRow)
            }
            const view = await buildCommunityPostView(
              helperCtx,
              hydrateCtx,
              parentRow,
              0,
              viewer ?? undefined,
            )
            ancestorViews.push({ uri: parentRow.uri, view, depth })
            parentUri = parentRow.replyParent || undefined
            depth -= 1
          }
        }
        ancestorViews.reverse()

        const isUnderAnchor = (uri: string): number => {
          let d = 0
          let cur: string | undefined = uri
          while (cur && cur !== params.anchor && d < 50) {
            cur = byUri.get(cur)?.replyParent
            d += 1
          }
          return cur === params.anchor ? d : -1
        }
        const descendantsWithDepth = allInThread
          .filter((p: any) => p.uri !== post.uri)
          .map((p: any) => ({ post: p, depth: isUnderAnchor(p.uri) }))
          .filter(({ depth }) => depth > 0)
          .sort((a, b) => {
            const t = (a.post.createdAt ?? '').localeCompare(
              b.post.createdAt ?? '',
            )
            return t !== 0 ? t : (a.post.uri ?? '').localeCompare(b.post.uri ?? '')
          })
        const cappedDescendants = descendantsWithDepth.slice(
          0,
          Math.min(params.below ?? 10, 200),
        )
        const descendantViews = await Promise.all(
          cappedDescendants.map(async ({ post: p, depth }) => ({
            uri: p.uri as string,
            depth,
            view: await buildCommunityPostView(
              helperCtx,
              hydrateCtx,
              p,
              0,
              viewer ?? undefined,
            ),
          })),
        )

        return {
          encoding: 'application/json',
          body: {
            hasOtherReplies: false,
            thread: [
              ...ancestorViews.map(({ uri, view, depth }) => ({
                uri,
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
              ...descendantViews.map(({ uri, view, depth }) => ({
                uri,
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
