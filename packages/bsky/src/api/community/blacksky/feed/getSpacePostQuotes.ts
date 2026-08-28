import {
  AuthRequiredError,
  InvalidRequestError,
  type Server,
} from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { resHeaders } from '../../../util.js'
import { isSpaceRecordUri, spaceOfRecordUri } from '../space-uri.js'
import { canViewSpace } from '../tenant-gate.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'
import { toSpacePostView } from '../views/spaceViews.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getSpacePostQuotes, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      if (!isSpaceRecordUri(params.uri)) {
        throw new InvalidRequestError(
          'URI is not a permissioned-space record',
          'InvalidRequest',
        )
      }
      const spaceUri = spaceOfRecordUri(params.uri)
      if (!spaceUri || !(await canViewSpace(ctx, spaceUri, viewer))) {
        throw new AuthRequiredError(
          'Must have access to this space',
          'MembershipRequired',
        )
      }

      const result = await ctx.dataplane.getSpacePostQuotes({
        subject: { uri: params.uri, cid: params.cid },
        limit: params.limit,
        cursor: params.cursor,
      })
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
      })
      const preAuthorized = new Set([spaceUri])
      const posts = (
        await Promise.all(
          result.posts.map(async (row) => {
            if (row.spaceUri !== spaceUri) return null
            const post = await buildCommunityPostView(
              ctx as any,
              hydrateCtx,
              row as any,
              0,
              viewer,
              undefined,
              preAuthorized,
            )
            if (!post || isBlockedForViewer(post) || isMutedForViewer(post)) {
              return null
            }
            return toSpacePostView(post)
          }),
        )
      ).filter(Boolean)

      return {
        encoding: 'application/json' as const,
        body: {
          uri: params.uri,
          cid: params.cid,
          cursor: result.cursor || undefined,
          posts,
        } as any,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
