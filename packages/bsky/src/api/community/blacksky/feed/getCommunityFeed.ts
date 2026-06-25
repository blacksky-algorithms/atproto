import { InvalidRequestError, AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { AtUriString, DidString, CidString, AtIdentifierString } from '@atproto/lex'
import { community } from '../../../../lexicons/index.js'
import {
  buildCommunityEmbedView,
  normalizeCidJsonRefs,
} from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityFeed, {
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
          actorDid = resolved as AtIdentifierString
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
              [(post.creator as DidString)],
              hydrateCtx,
            )
            const author = ctx.views.profileBasic((post.creator as DidString), profileState) ?? {
              did: (post.creator as DidString),
              handle: 'handle.invalid',
              labels: [],
            }

            // Get reply count
            const replyCountRes = await ctx.dataplane.getCommunityPostReplyCount({
              uri: post.uri as AtUriString,
            })

            // Build the post record; normalize `{"/":"..."}` → `{"$link":"..."}`.
            const facets = post.facets
              ? normalizeCidJsonRefs(JSON.parse(post.facets))
              : undefined
            const embed = post.embed
              ? normalizeCidJsonRefs(JSON.parse(post.embed))
              : undefined
            const langs = post.langs ? parsePgArray(post.langs) : undefined
            const record: Record<string, unknown> = {
              $type: 'app.bsky.feed.post',
              text: post.text,
              createdAt: post.createdAt,
            }
            if (facets) record.facets = facets
            if (langs) record.langs = langs
            if (embed) record.embed = embed
            const embedView = embed
              ? buildCommunityEmbedView(
                  ctx.views.imgUriBuilder,
                  post.creator as DidString,
                  embed,
                )
              : undefined
            if (post.replyRoot) {
              record.reply = {
                root: { uri: post.replyRoot as AtUriString, cid: (post.replyRootCid || '') as CidString },
                parent: {
                  uri: (post.replyParent || post.replyRoot) as AtUriString,
                  cid: (post.replyParentCid || post.replyRootCid || '') as CidString,
                },
              }
            }

            return {
              uri: post.uri as AtUriString,
              cid: (post.cid || '') as CidString,
              author,
              record,
              embed: embedView,
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
          } as any,
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
