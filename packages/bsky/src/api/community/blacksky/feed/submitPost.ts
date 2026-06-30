import { subsystemLogger } from '@atproto/common'
import {
  InvalidRequestError,
  AuthRequiredError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { AtUriString, CidString } from '@atproto/lex'
import {
  getServiceEndpoint,
  unpackIdentityServices,
} from '../../../../data-plane/client/util.js'
import { community } from '../../../../lexicons/index.js'
import { findBlobMetadata } from '../../../../util/find-blob-refs.js'

const logger = subsystemLogger('bsky:moderation')

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.submitPost, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const requesterDid = auth.credentials.iss

      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: requesterDid,
      })

      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }

      const {
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

      const uri = `at://${requesterDid}/${COMMUNITY_POST_COLLECTION}/${rkey}` as AtUriString

      const { cid, cidVerified } = await ctx.dataplane.submitCommunityPost({
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
      })
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
