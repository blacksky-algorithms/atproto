import {
  XrpcInvalidResponseError,
  XrpcResponseError,
  isDidString,
  isXrpcErrorPayload,
  xrpcSafe,
} from '@atproto/lex'
import type { AtUriString, DidString } from '@atproto/syntax'
import {
  AuthRequiredError,
  InvalidRequestError,
  type Server,
  UpstreamFailureError,
  createServiceJwt,
} from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import {
  Code,
  getServiceEndpoint,
  isDataplaneError,
  unpackIdentityServices,
} from '../../../../data-plane/index.js'
import { community } from '../../../../lexicons/index.js'
import { httpLogger } from '../../../../logger.js'
import type { GetIdentityByDidResponse } from '../../../../proto/bsky_pb.js'
import { resHeaders } from '../../../util.js'
import { communityPostsEnabled } from '../membership-guard.js'
import { isSpaceRecordUri, spaceOfRecordUri } from '../space-uri.js'
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

const REQUEST_TIMEOUT_MS = 10_000
const FEED_GENERATOR_SERVICE = {
  id: 'bsky_fg',
  type: 'BskyFeedGenerator',
} as const

const getFeedGeneratorEndpoint = async (ctx: AppContext, feed: AtUriString) => {
  const found = await ctx.hydrator.feed.getFeedGens([feed], true)
  const feedDid = found.get(feed)?.record.did
  if (!feedDid || !isDidString(feedDid)) {
    throw new InvalidRequestError('could not find feed')
  }

  let identity: GetIdentityByDidResponse
  try {
    identity = await ctx.dataplane.getIdentityByDid({ did: feedDid })
  } catch (err) {
    if (isDataplaneError(err, Code.NotFound)) {
      throw new InvalidRequestError(`could not resolve identity: ${feedDid}`)
    }
    throw err
  }

  const endpoint = getServiceEndpoint(
    unpackIdentityServices(identity.services),
    FEED_GENERATOR_SERVICE,
  )
  if (!endpoint) {
    throw new InvalidRequestError(
      `invalid feed generator service details in did document: ${feedDid}`,
    )
  }

  return { endpoint, audience: feedDid }
}

const getSpaceFeedSkeleton = async (
  ctx: AppContext,
  feed: AtUriString,
  viewer: DidString,
  limit: number,
  cursor?: string,
) => {
  const { endpoint, audience } = await getFeedGeneratorEndpoint(ctx, feed)
  const serviceJwt = await createServiceJwt({
    iss: ctx.cfg.serverDid,
    aud: audience,
    lxm: community.blacksky.feed.getSpaceFeedSkeleton.$lxm,
    keypair: ctx.signingKey,
  })
  let result
  try {
    result = await xrpcSafe(
      endpoint,
      community.blacksky.feed.getSpaceFeedSkeleton,
      {
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        headers: { authorization: `Bearer ${serviceJwt}` },
        params: { feed, did: viewer, limit, cursor },
      },
    )
  } catch (err) {
    throw new UpstreamFailureError('feed unavailable', 'UpstreamFailure', {
      cause: err,
    })
  }

  if (!result.success) {
    const cause = result.reason
    if (
      cause instanceof XrpcResponseError &&
      isXrpcErrorPayload(cause.payload) &&
      cause.matchesSchemaErrors()
    ) {
      if (cause.error === 'InvalidRequest' && cause.status === 400) {
        throw new InvalidRequestError(cause.message, cause.error, { cause })
      }
      if (cause.error === 'MembershipRequired' && cause.status === 403) {
        throw new AuthRequiredError(cause.message, cause.error, { cause })
      }
    }
    if (result.reason instanceof XrpcInvalidResponseError) {
      throw new UpstreamFailureError(
        'feed provided an invalid response',
        'InvalidFeedResponse',
        { cause },
      )
    }
    throw new UpstreamFailureError('feed unavailable', 'UpstreamFailure', {
      cause,
    })
  }
  return result.body
}

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
      const viewerDid = viewer.split('#', 1)[0]
      if (!isDidString(viewerDid)) {
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
      const allowed = await canViewSpace(ctx, spaceUri, viewerDid)
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

      const skeleton = await getSpaceFeedSkeleton(
        ctx,
        params.feed,
        viewerDid,
        params.limit,
        params.cursor,
      )
      if (skeleton.feed.length > params.limit) {
        throw new UpstreamFailureError(
          'feed returned more posts than requested',
          'InvalidFeedResponse',
        )
      }

      const selectedUris: string[] = []
      const seen = new Set<string>()
      let invalidReferences = 0
      for (const item of skeleton.feed) {
        if (
          !isSpaceRecordUri(item.post) ||
          spaceOfRecordUri(item.post) !== spaceUri
        ) {
          invalidReferences++
          continue
        }
        if (!seen.has(item.post)) {
          seen.add(item.post)
          selectedUris.push(item.post)
        }
      }
      if (invalidReferences > 0) {
        httpLogger.warn(
          {
            feed: params.feed,
            spaceUri,
            invalidReferences,
            skeletonSize: skeleton.feed.length,
          },
          'space feed returned references outside the requested space',
        )
      }

      const rows = selectedUris.length
        ? await ctx.dataplane.getCommunityPosts({
            uris: selectedUris,
            allowedSpaceUris: [spaceUri],
          })
        : { posts: [] }
      const rowsByUri = new Map(
        rows.posts
          .filter(
            (row: any) =>
              selectedUris.includes(row.uri) &&
              isSpaceRecordUri(row.uri) &&
              spaceOfRecordUri(row.uri) === spaceUri,
          )
          .map((row: any) => [row.uri, row] as const),
      )
      const projectionMisses = selectedUris.length - rowsByUri.size
      if (projectionMisses > 0) {
        httpLogger.info(
          {
            feed: params.feed,
            spaceUri,
            skeletonSize: skeleton.feed.length,
            selectedReferences: selectedUris.length,
            projectionMisses,
          },
          'space feed references were not projected',
        )
      }

      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: viewerDid,
      })
      const feed = (
        await Promise.all(
          selectedUris.map(async (uri) => {
            const row = rowsByUri.get(uri)
            if (!row) return null
            const post = await buildCommunityPostView(
              ctx as any,
              hydrateCtx,
              row,
              0,
              viewerDid,
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
              viewerDid,
              preAuthorized,
            )
            return toSpaceFeedViewPost(reply ? { post, reply } : { post })
          }),
        )
      ).filter(Boolean)

      return {
        encoding: 'application/json' as const,
        body: { cursor: skeleton.cursor || undefined, feed } as any,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
