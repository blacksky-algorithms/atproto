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
  buildCommunityEmbedView,
  isCommunityPostUri,
  normalizeCidJsonRefs,
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
        const profileState = await ctx.hydrator.hydrateProfilesBasic(
          [post.creator as AtUriString as any],
          hydrateCtx,
        )
        const author = ctx.views.profileBasic(
          post.creator as AtUriString as any,
          profileState,
        ) ?? {
          did: post.creator,
          handle: 'handle.invalid',
          labels: [],
        }
        const facets = post.facets
          ? normalizeCidJsonRefs(JSON.parse(post.facets))
          : undefined
        const embed = post.embed
          ? normalizeCidJsonRefs(JSON.parse(post.embed))
          : undefined
        const langs = post.langs
          ? post.langs
              .replace(/[{}]/g, '')
              .split(',')
              .filter(Boolean)
          : undefined
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
        const embedView = embed
          ? buildCommunityEmbedView(
              ctx.views.imgUriBuilder,
              post.creator,
              embed,
            )
          : undefined
        const postView = {
          uri: post.uri,
          cid: post.cid,
          author,
          record,
          embed: embedView,
          indexedAt: post.indexedAt,
          likeCount: 0,
          repostCount: 0,
          replyCount: 0,
          quoteCount: 0,
          bookmarkCount: 0,
          labels: [],
        }
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
                  post: postView,
                  moreParents: false,
                  moreReplies: 0,
                  opThread: true,
                  hiddenByThreadgate: false,
                  mutedByViewer: false,
                },
              },
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
