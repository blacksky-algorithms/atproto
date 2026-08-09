import { InvalidRequestError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { MuteOperation_Type } from '../../../../proto/bsync_pb.js'
import { assertCommunityMembershipForUris } from '../membership-guard.js'
import { isSpaceRecordUri } from '../space-uri.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.setThreadMute, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth, input }) => {
      const { root, mute } = input.body
      const requester = auth.credentials.iss
      // Space records only: a thread rooted at an ordinary at-uri is served by
      // the standard mute methods.
      if (!isSpaceRecordUri(root)) {
        throw new InvalidRequestError(
          'Root is not a permissioned-space record',
          'InvalidRequest',
        )
      }
      await assertCommunityMembershipForUris(ctx, requester, [root])
      await ctx.bsyncClient.addMuteOperation({
        type: mute ? MuteOperation_Type.ADD : MuteOperation_Type.REMOVE,
        actorDid: requester,
        subject: root,
      })
    },
  })
}
