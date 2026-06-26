import { InvalidRequestError, AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AtIdentifierString } from '@atproto/lex'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { buildCommunityPostView } from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityFeed, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const requesterDid = auth.credentials.iss
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: requesterDid,
      })
      if (!isMember) {
        throw new AuthRequiredError(
          'Must be a Blacksky community member',
          'MembershipRequired',
        )
      }
      let actorDid = params.actor
      if (!actorDid.startsWith('did:')) {
        const resolved = await ctx.idResolver.handle.resolve(actorDid)
        if (!resolved) {
          throw new InvalidRequestError('Actor not found')
        }
        actorDid = resolved as AtIdentifierString
      }
      const res = await ctx.dataplane.getCommunityFeedByActor({
        actorDid,
        limit: params.limit,
        cursor: params.cursor,
      })
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })
      const helperCtx = {
        hydrator: ctx.hydrator,
        views: ctx.views,
        dataplane: ctx.dataplane,
      }
      const hydratedPosts = await Promise.all(
        res.posts.map((post) =>
          buildCommunityPostView(helperCtx as any, hydrateCtx, post as any),
        ),
      )
      return {
        encoding: 'application/json' as const,
        body: {
          cursor: res.cursor || undefined,
          feed: hydratedPosts.map((post) => ({ post })),
        } as any,
      }
    },
  })
}
