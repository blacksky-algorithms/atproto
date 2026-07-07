import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { hasPeerModBadge } from '../../../../peer-mod.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.getMyLabels, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth }) => {
      const callerDid = auth.credentials.iss
      if (!(await hasPeerModBadge(ctx.dataplane, callerDid))) {
        return {
          encoding: 'application/json' as const,
          body: { vals: [] },
        }
      }
      const { vals } = await ctx.dataplane.getPeerModLabelsForSubject({
        subjectUri: params.subjectUri,
        peerModDid: callerDid,
      })
      return {
        encoding: 'application/json' as const,
        body: { vals },
      }
    },
  })
}
