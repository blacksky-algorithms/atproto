import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.getCommunityFeed({
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth }) => {
      try {
        const requesterDid = auth.credentials.iss
        console.log('[getCommunityFeed] START requester=%s actor=%s', requesterDid, params.actor)

        const { isMember } = await ctx.dataplane.checkCommunityMembership({
          did: requesterDid,
        })
        console.log('[getCommunityFeed] membership check: isMember=%s', isMember)
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

        const res = await ctx.dataplane.getCommunityFeedByActor({
          actorDid,
          limit: params.limit,
          cursor: params.cursor,
        })
        console.log('[getCommunityFeed] feed returned %d posts', res.posts.length)

        return {
          encoding: 'application/json' as const,
          body: {
            cursor: res.cursor || undefined,
            posts: res.posts.map(postViewFromProto),
          },
        }
      } catch (err) {
        console.error('[getCommunityFeed] ERROR:', err)
        throw err
      }
    },
  })
}

function postViewFromProto(post: {
  uri: string
  cid: string
  creator: string
  text: string
  facets: string
  replyRoot: string
  replyParent: string
  embed: string
  langs: string
  labels: string
  tags: string
  createdAt: string
  indexedAt: string
}) {
  return {
    uri: post.uri,
    cid: post.cid || undefined,
    creator: post.creator,
    text: post.text,
    facets: post.facets ? JSON.parse(post.facets) : undefined,
    replyRoot: post.replyRoot || undefined,
    replyParent: post.replyParent || undefined,
    embed: post.embed ? JSON.parse(post.embed) : undefined,
    langs: post.langs
      ? post.langs.replace(/[{}]/g, '').split(',').filter(Boolean)
      : undefined,
    labels: post.labels ? JSON.parse(post.labels) : undefined,
    tags: post.tags
      ? post.tags.replace(/[{}]/g, '').split(',').filter(Boolean)
      : undefined,
    createdAt: post.createdAt,
    indexedAt: post.indexedAt,
  }
}
