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
import { assertCommunityMembershipForUris } from '../../../community/blacksky/membership-guard.js'
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
        await assertCommunityMembershipForUris(ctx, viewer, [params.anchor])
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
        const threadRootUri = post.replyRoot || post.uri
        const replyAllowed = viewer
          ? (
              await ctx.dataplane.checkCommunityReplyAllowed({
                rootUri: threadRootUri,
                viewerDid: viewer,
              })
            ).allowed
          : false
        const replyDisabled = !replyAllowed
        const anchorView = await buildCommunityPostView(
          helperCtx as any,
          hydrateCtx,
          post,
          0,
          viewer ?? undefined,
          replyDisabled,
        )
        const allInThreadRes = await ctx.dataplane.getCommunityPostReplies({
          parentUri: threadRootUri as AtUriString,
          limit: 200,
        })
        const allInThread = allInThreadRes.posts ?? []
        const byUri = new Map<string, any>(allInThread.map((p: any) => [p.uri, p]))
        byUri.set(post.uri, post)

        const ancestorViews: Array<{
          uri: string
          view: unknown
          depth: number
          notFound?: boolean
        }> = []
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
              if (!r.post) {
                // Deleted/unavailable ancestor: surface a placeholder like a
                // standard thread rather than silently dropping the ancestry.
                ancestorViews.push({ uri: parentUri, view: null, depth, notFound: true })
                break
              }
              parentRow = r.post
              byUri.set(parentRow.uri, parentRow)
            }
            const view = await buildCommunityPostView(
              helperCtx as any,
              hydrateCtx,
              parentRow,
              0,
              viewer ?? undefined,
              replyDisabled,
            )
            ancestorViews.push({ uri: parentRow.uri, view, depth })
            parentUri = parentRow.replyParent || undefined
            depth -= 1
          }
        }
        ancestorViews.reverse()

        // Depth-first assembly: each subtree's items are contiguous, which
        // is what the flattened threadItem contract requires.
        const childrenOf = new Map<string, any[]>()
        for (const p of allInThread) {
          if (p.uri === post.uri || !p.replyParent) continue
          const list = childrenOf.get(p.replyParent) ?? []
          list.push(p)
          childrenOf.set(p.replyParent, list)
        }
        for (const list of childrenOf.values()) {
          list.sort((a, b) => {
            const t = (a.createdAt ?? '').localeCompare(b.createdAt ?? '')
            return t !== 0 ? t : (a.uri ?? '').localeCompare(b.uri ?? '')
          })
        }
        const maxDepth = Math.min(params.below ?? 10, 50)
        const branching = Math.min(params.branchingFactor ?? 50, 50)
        const cappedDescendants: Array<{ post: any; depth: number }> = []
        const moreRepliesByUri = new Map<string, number>()
        const walk = (uri: string, depth: number) => {
          const all = childrenOf.get(uri) ?? []
          if (depth > maxDepth || cappedDescendants.length >= 200) {
            if (all.length > 0) moreRepliesByUri.set(uri, all.length)
            return
          }
          // branchingFactor caps every level except the anchor's direct replies
          const children = depth === 1 ? all : all.slice(0, branching)
          if (children.length < all.length) {
            moreRepliesByUri.set(uri, all.length - children.length)
          }
          for (const child of children) {
            if (cappedDescendants.length >= 200) {
              moreRepliesByUri.set(
                uri,
                (moreRepliesByUri.get(uri) ?? 0) + 1,
              )
              continue
            }
            cappedDescendants.push({ post: child, depth })
            walk(child.uri, depth + 1)
          }
        }
        walk(post.uri, 1)
        const descendantViews = await Promise.all(
          cappedDescendants.map(async ({ post: p, depth }) => ({
            uri: p.uri as string,
            depth,
            view: await buildCommunityPostView(
              helperCtx as any,
              hydrateCtx,
              p,
              0,
              viewer ?? undefined,
              replyDisabled,
            ),
          })),
        )

        return {
          encoding: 'application/json',
          body: {
            hasOtherReplies: false,
            thread: [
              ...ancestorViews.map(({ uri, view, depth, notFound }) =>
                notFound
                  ? {
                      uri,
                      depth,
                      value: {
                        $type: 'app.bsky.unspecced.defs#threadItemNotFound',
                      },
                    }
                  : {
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
                    },
              ),
              {
                uri: post.uri,
                depth: 0,
                value: {
                  $type: 'app.bsky.unspecced.defs#threadItemPost',
                  post: anchorView,
                  moreParents: false,
                  moreReplies: moreRepliesByUri.get(post.uri) ?? 0,
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
                  moreReplies: moreRepliesByUri.get(uri) ?? 0,
                  opThread: (byUri.get(uri)?.creator ?? '') === post.creator,
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
