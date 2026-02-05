import { InvalidRequestError, AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.getCommunityFeed({
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      try {
        const requesterDid = auth.credentials.iss
        console.log(
          '[getCommunityFeed] START requester=%s actor=%s',
          requesterDid,
          params.actor,
        )

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

        // Create hydration context for profile lookups
        const labelers = ctx.reqLabelers(req)
        const hydrateCtx = await ctx.hydrator.createContext({
          labelers,
          viewer: requesterDid,
        })

        // Hydrate posts with author profiles and reply counts
        const hydratedPosts = await Promise.all(
          res.posts.map(async (post) => {
            // Hydrate author profile
            const profileState = await ctx.hydrator.hydrateProfilesBasic(
              [post.creator],
              hydrateCtx,
            )
            const author = ctx.views.profileBasic(post.creator, profileState) ?? {
              did: post.creator,
              handle: 'handle.invalid',
              labels: [],
            }

            // Get reply count
            const replyCountRes = await ctx.dataplane.getCommunityPostReplyCount({
              uri: post.uri,
            })

            // Build the post record
            const facets = post.facets ? JSON.parse(post.facets) : undefined
            const embed = post.embed ? JSON.parse(post.embed) : undefined
            const langs = post.langs ? parsePgArray(post.langs) : undefined
            const record: Record<string, unknown> = {
              $type: 'app.bsky.feed.post',
              text: post.text,
              createdAt: post.createdAt,
            }
            if (facets) record.facets = facets
            if (langs) record.langs = langs
            if (embed) record.embed = embed
            if (post.replyRoot) {
              record.reply = {
                root: { uri: post.replyRoot, cid: post.replyRootCid || '' },
                parent: {
                  uri: post.replyParent || post.replyRoot,
                  cid: post.replyParentCid || post.replyRootCid || '',
                },
              }
            }

            return {
              uri: post.uri,
              cid: post.cid || '',
              author,
              record,
              indexedAt: post.indexedAt,
              likeCount: 0,
              repostCount: 0,
              replyCount: replyCountRes.count,
              quoteCount: 0,
              bookmarkCount: 0,
              labels: [],
            }
          }),
        )

        return {
          encoding: 'application/json' as const,
          body: {
            cursor: res.cursor || undefined,
            feed: hydratedPosts.map((post) => ({ post })),
          },
        }
      } catch (err) {
        console.error('[getCommunityFeed] ERROR:', err)
        throw err
      }
    },
  })
}

function parsePgArray(val: string | null): string[] | undefined {
  if (!val) return undefined
  return val
    .replace(/[{}]/g, '')
    .split(',')
    .filter(Boolean)
}
