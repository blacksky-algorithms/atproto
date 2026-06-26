import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import {
  PeerModNotConfiguredError,
  emitAcknowledgeEvent,
  emitLabelEvent,
  emitReportEvent,
} from '../../../../peer-mod.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.moderation.applyLabel, {
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

      const { subjectUri, subjectCid, val, reason } = input.body
      if (!subjectUri.includes(COMMUNITY_POST_COLLECTION)) {
        throw new InvalidRequestError(
          'Subject must be a community post',
          'InvalidSubject',
        )
      }
      const { exists } = await ctx.dataplane.communityPostExists({
        uri: subjectUri,
      })
      if (!exists) {
        throw new InvalidRequestError('Subject post not found', 'InvalidSubject')
      }

      // Ozone first, DB second — a failed Ozone call leaves no orphan row.
      let ozoneEventId = ''
      try {
        const ev = await emitLabelEvent(ctx.peerModConfig, {
          subjectUri,
          subjectCid,
          val,
          peerModDid: callerDid,
          comment: reason,
          negate: false,
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

      await ctx.dataplane.recordPeerModLabel({
        subjectUri,
        subjectCid,
        val,
        peerModDid: callerDid,
        ozoneEventId,
      })

      const auditComment = reason
        ? `Peer-mod label "${val}" applied: ${reason}`
        : `Peer-mod label "${val}" applied`
      try {
        await emitReportEvent(ctx.peerModConfig, {
          subjectUri,
          subjectCid,
          peerModDid: callerDid,
          comment: auditComment,
        })
        await emitAcknowledgeEvent(ctx.peerModConfig, {
          subjectUri,
          subjectCid,
          peerModDid: callerDid,
          comment: auditComment,
        })
      } catch (err) {
        console.warn(
          '[applyLabel] audit-trail report/ack emit failed:',
          (err as Error).message,
        )
      }

      return {
        encoding: 'application/json' as const,
        body: { val, subjectUri },
      }
    },
  })
}
