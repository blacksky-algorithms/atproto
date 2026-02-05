import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'
import { CommunityPostRow } from '../../../../community/db'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.getCommunityFeed({
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth }) => {
      const requesterDid = auth.credentials.iss

      if (!ctx.communityMembership) {
        throw new InvalidRequestError('Community features not configured')
      }
      if (!ctx.communityDb) {
        throw new InvalidRequestError('Community database not configured')
      }

      const isMember = await ctx.communityMembership.isMember(requesterDid)
      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }

      // Resolve actor DID (params.actor could be a handle)
      let actorDid = params.actor
      if (!actorDid.startsWith('did:')) {
        const resolved = await ctx.idResolver.handle.resolve(actorDid)
        if (!resolved) {
          throw new InvalidRequestError('Actor not found')
        }
        actorDid = resolved
      }

      const { posts, cursor } = await ctx.communityDb.getCommunityFeedByActor(
        actorDid,
        params.limit,
        params.cursor,
      )

      return {
        encoding: 'application/json' as const,
        body: {
          cursor,
          posts: posts.map(rowToView),
        },
      }
    },
  })
}

function rowToView(row: CommunityPostRow) {
  return {
    uri: row.uri,
    cid: row.cid || undefined,
    creator: row.creator,
    text: row.text,
    facets: row.facets ? JSON.parse(row.facets) : undefined,
    replyRoot: row.replyRoot ?? undefined,
    replyParent: row.replyParent ?? undefined,
    embed: row.embed ? JSON.parse(row.embed) : undefined,
    langs: row.langs ? row.langs.replace(/[{}]/g, '').split(',').filter(Boolean) : undefined,
    labels: row.labels ? JSON.parse(row.labels) : undefined,
    tags: row.tags ? row.tags.replace(/[{}]/g, '').split(',').filter(Boolean) : undefined,
    createdAt: row.createdAt,
    indexedAt: row.indexedAt,
  }
}
