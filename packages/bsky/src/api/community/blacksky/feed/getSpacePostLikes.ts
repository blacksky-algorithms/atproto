import {
  AuthRequiredError,
  InvalidRequestError,
  type Server,
} from '@atproto/xrpc-server'
import { type DidString, normalizeDatetimeAlways } from '@atproto/syntax'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import {
  isSpaceRecordUri,
  parseSpaceRecordUri,
  spaceUriOf,
} from '../space-uri.js'
import { canViewSpace } from '../tenant-gate.js'
import { resHeaders } from '../../../util.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getSpacePostLikes, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      const spaceRecord = parseSpaceRecordUri(params.uri)
      if (!isSpaceRecordUri(params.uri) || !spaceRecord) {
        throw new InvalidRequestError(
          'URI is not a permissioned-space record',
          'InvalidRequest',
        )
      }
      if (!(await canViewSpace(ctx, spaceUriOf(spaceRecord), viewer))) {
        throw new AuthRequiredError(
          'Must have access to this space',
          'MembershipRequired',
        )
      }

      const result = await ctx.dataplane.getSpacePostLikes({
        subject: { uri: params.uri, cid: params.cid },
        limit: params.limit,
        cursor: params.cursor,
      })
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
      })
      const likerDids = [
        ...new Set(result.likes.map((like) => like.creator as DidString)),
      ]
      const [profileState, blocks] = await Promise.all([
        ctx.hydrator.hydrateProfiles(likerDids, hydrateCtx),
        ctx.hydrator.hydrateBidirectionalBlocks(
          new Map([[spaceRecord.authorDid as DidString, likerDids]]),
          hydrateCtx,
        ),
      ])
      const authorDid = spaceRecord.authorDid as DidString
      const likes = result.likes.flatMap((like) => {
        const likerDid = like.creator as DidString
        if (
          blocks.get(authorDid)?.get(likerDid) ||
          ctx.views.viewerBlockExists(likerDid, profileState)
        ) {
          return []
        }
        const actor = ctx.views.profile(likerDid, profileState)
        if (!actor) return []
        return [
          {
            actor,
            createdAt: normalizeDatetimeAlways(like.createdAt),
            indexedAt: normalizeDatetimeAlways(like.indexedAt),
          },
        ]
      })

      return {
        encoding: 'application/json' as const,
        body: {
          uri: params.uri,
          cid: params.cid,
          cursor: result.cursor || undefined,
          likes,
        },
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
