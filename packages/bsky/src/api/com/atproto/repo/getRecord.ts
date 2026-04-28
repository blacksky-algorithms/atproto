import { AtUri } from '@atproto/syntax'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'
import { ids } from '../../../../lexicon/lexicons'

export default function (server: Server, ctx: AppContext) {
  server.com.atproto.repo.getRecord({
    auth: ctx.authVerifier.optionalStandardOrRole,
    handler: async ({ auth, params }) => {
      const { repo, collection, rkey, cid } = params
      const { includeTakedowns } = ctx.authVerifier.parseCreds(auth)
      const [did] = await ctx.hydrator.actor.getDids([repo])
      if (!did) {
        throw new InvalidRequestError(`Could not find repo: ${repo}`)
      }

      const actors = await ctx.hydrator.actor.getActors([did], {
        includeTakedowns,
      })
      if (!actors.get(did)) {
        throw new InvalidRequestError(`Could not find repo: ${repo}`)
      }

      const uri = AtUri.make(did, collection, rkey).toString()

      // Community posts require membership check (unless admin/role access)
      if (collection === ids.CommunityBlackskyFeedPost) {
        const isAdmin = auth?.credentials?.type === 'role'
        if (!isAdmin) {
          // Check if requester is authenticated and is a member
          const viewerDid =
            auth?.credentials?.type === 'standard'
              ? auth.credentials.iss
              : undefined
          if (!viewerDid) {
            throw new InvalidRequestError(
              `Could not locate record: ${uri}`,
              'RecordNotFound',
            )
          }
          const membershipRes =
            await ctx.dataplane.checkCommunityMembership({ did: viewerDid })
          if (!membershipRes.isMember) {
            throw new InvalidRequestError(
              `Could not locate record: ${uri}`,
              'RecordNotFound',
            )
          }
        }
      }

      const result = await ctx.hydrator.getRecord(uri, includeTakedowns)

      if (!result || (cid && result.cid !== cid)) {
        throw new InvalidRequestError(
          `Could not locate record: ${uri}`,
          'RecordNotFound',
        )
      }

      return {
        encoding: 'application/json' as const,
        body: {
          uri: uri,
          cid: result.cid,
          value: result.record,
        },
      }
    },
  })
}
