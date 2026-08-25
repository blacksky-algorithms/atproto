import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { hasPeerModBadge } from '../../../../peer-mod.js'

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
