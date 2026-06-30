import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.getMyPermissions, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth }) => {
      const callerDid = auth.credentials.iss
      const isPeerMod = ctx.peerModConfig.peerModDids.has(callerDid as never)
      return {
        encoding: 'application/json' as const,
        body: { isPeerMod },
      }
    },
  })
}
