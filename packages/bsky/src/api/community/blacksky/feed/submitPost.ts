import { subsystemLogger } from '@atproto/common'
import { AtUriString, CidString } from '@atproto/lex'
import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import {
  getServiceEndpoint,
  unpackIdentityServices,
} from '../../../../data-plane/client/util.js'
import { community } from '../../../../lexicons/index.js'
import { findBlobMetadata } from '../../../../util/find-blob-refs.js'
import {
  checkCommunityFeedPermission,
  getCommunityFeedConfig,
  isSpaceBackedFeed,
} from '../tenant-gate.js'

const logger = subsystemLogger('bsky:moderation')

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

type ReplyRef = {
  root: { uri: string }
  parent: { uri: string }
}

export async function authorizeCommunityPostSubmission(
  ctx: AppContext,
  requesterDid: string,
  requestedFeed: string | undefined,
  reply: ReplyRef | undefined,
): Promise<string | undefined> {
  let feed = requestedFeed
  if (reply?.root.uri.includes(COMMUNITY_POST_COLLECTION)) {
    const { post } = await ctx.dataplane.getCommunityPost({
      uri: reply.root.uri,
    })
    if (post) {
      const rootFeed = post.feedUri || undefined
      if (requestedFeed !== undefined && requestedFeed !== rootFeed) {
        throw new InvalidRequestError(
          'Reply feed does not match the root post',
          'InvalidReply',
        )
      }
      feed = rootFeed
    }
  }

  if (!feed) {
    const { isMember } = await ctx.dataplane.checkCommunityMembership({
      did: requesterDid,
    })
    if (!isMember) {
      throw new AuthRequiredError(
        'Must be a Blacksky community member',
        'MembershipRequired',
      )
    }
    return undefined
  }

  const config = await getCommunityFeedConfig(ctx, feed)
  // A space-backed feed is written through the space host, not here; accepting
  // a submission would put private content in a public repo.
  if (isSpaceBackedFeed(config)) {
    throw new InvalidRequestError(
      'This feed is space-backed; write to the space instead',
      'InvalidFeed',
    )
  }
  if (
    config?.contentType !== 'communityRecord' ||
    config.visibility !== 'gated' ||
    config.contentStore !== ctx.cfg.serverDid
  ) {
    throw new InvalidRequestError(
      'Feed is not configured for community posts on this content store',
      'InvalidFeed',
    )
  }
  const allowed = await checkCommunityFeedPermission(
    ctx,
    feed,
    requesterDid,
    'canPost',
  )
  if (!allowed) {
    throw new AuthRequiredError(
      'Not authorized to post to this feed',
      'PermissionRequired',
    )
  }
  return feed
}

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.submitPost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const requesterDid = auth.credentials.iss

      const {
        feed,
        rkey,
        text,
        facets,
        reply,
        embed,
        langs,
        labels,
        tags,
        createdAt,
        expectedCid,
      } = input.body

      const effectiveFeed = await authorizeCommunityPostSubmission(
        ctx,
        requesterDid,
        feed,
        reply,
      )

      // Validate reply cascade
      if (reply) {
        const rootUri = reply.root.uri
        if (!rootUri.includes(COMMUNITY_POST_COLLECTION)) {
          throw new InvalidRequestError(
            'Replies to community posts must reference community posts',
            'InvalidReply',
          )
        }
        const { exists } = await ctx.dataplane.communityPostExists({
          uri: rootUri,
        })
        if (!exists) {
          throw new InvalidRequestError(
            'Reply root post not found',
            'InvalidReply',
          )
        }
      }

      const uri =
        `at://${requesterDid}/${COMMUNITY_POST_COLLECTION}/${rkey}` as AtUriString

      const { threadgateAllow, embeddingRules } = input.body as {
        threadgateAllow?: unknown[]
        embeddingRules?: unknown[]
      }
      const { cid, cidVerified, rejected } =
        await ctx.dataplane.submitCommunityPost({
          uri,
          rkey,
          creator: requesterDid,
          text,
          facets: facets ? JSON.stringify(facets) : '',
          replyRoot: reply?.root.uri ?? '',
          replyRootCid: reply?.root.cid ?? '',
          replyParent: reply?.parent.uri ?? '',
          replyParentCid: reply?.parent.cid ?? '',
          embed: embed ? JSON.stringify(embed) : '',
          langs: langs?.join(',') ?? '',
          labels: labels ? JSON.stringify(labels) : '',
          tags: tags?.join(',') ?? '',
          createdAt,
          expectedCid: expectedCid ?? '',
          feedUri: effectiveFeed ?? '',
          threadgateAllow:
            reply == null && threadgateAllow
              ? JSON.stringify(threadgateAllow)
              : '',
          embeddingRules: embeddingRules ? JSON.stringify(embeddingRules) : '',
        })
      if (rejected === 'InvalidCreatedAt') {
        throw new InvalidRequestError(
          'createdAt must be a canonical ISO-8601 timestamp',
        )
      }
      if (rejected === 'BlockedFromReply') {
        throw new InvalidRequestError(
          'Cannot reply to this thread',
          'BlockedActor',
        )
      }
      if (rejected === 'ReplyNotAllowed') {
        throw new InvalidRequestError(
          'The thread author has limited who can reply',
          'ReplyNotAllowed',
        )
      }
      if (rejected === 'EmbeddingDisabled') {
        throw new InvalidRequestError(
          'The quoted post has quotes disabled',
          'EmbeddingDisabled',
        )
      }
      if (rejected === 'FeedMismatch') {
        throw new InvalidRequestError(
          'Post feed does not match its existing authorization boundary',
          'InvalidFeed',
        )
      }
      // If client provided expectedCid but it didn't match, reject
      if (expectedCid && !cidVerified) {
        throw new InvalidRequestError(
          `CID mismatch: expected ${expectedCid}, computed ${cid}`,
          'CidMismatch',
        )
      }

      // Enqueue for moderation (fire-and-forget with internal retry)
      if (ctx.moderationClient) {
        let pdsEndpoint: string | undefined
        try {
          const identity = await ctx.dataplane.getIdentityByDid({
            did: requesterDid,
          })
          const services = unpackIdentityServices(identity.services)
          pdsEndpoint = getServiceEndpoint(services, {
            id: 'atproto_pds',
            type: 'AtprotoPersonalDataServer',
          })
        } catch (err) {
          logger.warn(
            { err, did: requesterDid },
            'failed to resolve PDS endpoint for moderation enqueue',
          )
        }

        if (pdsEndpoint) {
          const blobs = embed ? findBlobMetadata(embed) : []
          const blobCids = blobs.map((blob) => blob.cid)
          ctx.moderationClient
            .enqueue({
              did: requesterDid,
              collection: COMMUNITY_POST_COLLECTION,
              rkey,
              pdsEndpoint,
              blobCids: blobCids.length > 0 ? blobCids : undefined,
              blobs: blobs.length > 0 ? blobs : undefined,
            })
            .catch(() => {}) // error already logged inside client
        }
      }

      return {
        encoding: 'application/json' as const,
        body: { uri: uri as AtUriString, cid: cid as CidString },
      }
    },
  })
}
