import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { hasPeerModBadge } from '../../../../peer-mod.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.getMyPermissions, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth }) => {
      const callerDid = auth.credentials.iss
      const isPeerMod = await hasPeerModBadge(ctx.dataplane, callerDid)
      return {
        encoding: 'application/json' as const,
        body: { isPeerMod },
      }
    },
  })
}
