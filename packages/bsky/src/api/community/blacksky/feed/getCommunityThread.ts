import { InvalidRequestError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { isSpaceRecordUri } from '../space-uri.js'
import { buildCommunityThread } from './communityThread.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityThread, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      // Space records only. A stub post is an ordinary at-uri and is served
      // by the standard thread endpoint; keeping one path per kind of content
      // leaves the Blacksky-only feed with exactly the route it already had.
      if (!isSpaceRecordUri(params.anchor)) {
        throw new InvalidRequestError(
          'Anchor is not a permissioned-space record',
          'InvalidRequest',
        )
      }
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
      })
      const { body, headers } = await buildCommunityThread(
        ctx,
        hydrateCtx,
        params,
        viewer,
      )
      return {
        encoding: 'application/json' as const,
        body: body as any,
        ...(headers ? { headers } : {}),
      }
    },
  })
}
