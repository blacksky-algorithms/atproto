import {
  AuthRequiredError,
  InvalidRequestError,
  type Server,
} from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import {
  PeerModNotConfiguredError,
  emitLabelEvent,
  hasPeerModBadge,
} from '../../../../peer-mod.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.removeLabel, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const callerDid = auth.credentials.iss
      if (!(await hasPeerModBadge(ctx.dataplane, callerDid))) {
        throw new AuthRequiredError(
          'Caller is not a peer-moderator',
          'PeerModRequired',
        )
      }

      const { subjectUri, val, reason } = input.body

      const { labels } = await ctx.dataplane.getPeerModLabelsForSubject({
        subjectUri,
        peerModDid: callerDid,
      })
      const owned = labels.find((label) => label.val === val)
      if (!owned) {
        throw new InvalidRequestError(
          'Caller did not apply this label',
          'LabelNotOwned',
        )
      }
      const subjectCid = owned.subjectCid

      let ozoneEventId = ''
      try {
        const ev = await emitLabelEvent(ctx.peerModConfig, {
          subjectUri,
          subjectCid,
          val,
          peerModDid: callerDid,
          comment: reason,
          negate: true,
        })
        ozoneEventId = ev.id
      } catch (err) {
        if (err instanceof PeerModNotConfiguredError) {
          throw new InvalidRequestError(
            'Peer-mod is not configured on this appview',
            'OzoneFailed',
          )
        }
        throw new InvalidRequestError(
          `Ozone emitEvent failed: ${(err as Error).message}`,
          'OzoneFailed',
        )
      }

      await ctx.dataplane.negatePeerModLabel({
        subjectUri,
        val,
        peerModDid: callerDid,
        ozoneEventId,
      })

      return {
        encoding: 'application/json' as const,
        body: { val, subjectUri },
      }
    },
  })
}
