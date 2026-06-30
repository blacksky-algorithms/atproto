import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import {
  PeerModNotConfiguredError,
  emitLabelEvent,
} from '../../../../peer-mod.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.removeLabel, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const callerDid = auth.credentials.iss
      const { peerModDids } = ctx.peerModConfig
      if (!peerModDids.has(callerDid as never)) {
        throw new AuthRequiredError(
          'Caller is not a peer-moderator',
          'PeerModRequired',
        )
      }

      const { subjectUri, val, reason } = input.body

      // Look up the existing row to read the subjectCid for the Ozone subject.
      // Also enforces caller-owned (the dataplane negation is keyed on peerModDid).
      const { vals } = await ctx.dataplane.getPeerModLabelsForSubject({
        subjectUri,
        peerModDid: callerDid,
      })
      if (!vals.includes(val)) {
        throw new InvalidRequestError(
          'Caller did not apply this label',
          'LabelNotOwned',
        )
      }

      // We don't have subjectCid stored convenient on read here, but Ozone's
      // strongRef accepts the URI alone for negations against existing labels;
      // pass the current community post CID.
      const { post } = await ctx.dataplane.getCommunityPost({ uri: subjectUri })
      const subjectCid = post?.cid ?? ''

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
