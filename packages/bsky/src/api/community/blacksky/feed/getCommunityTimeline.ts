import { AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context'
import { Server } from '../../../../lexicon'

export default function (server: Server, ctx: AppContext) {
  server.community.blacksky.feed.getCommunityTimeline({
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

      const limit = params.limit ?? 50
      const res = await ctx.dataplane.getCommunityTimeline({
        limit,
        cursor: params.cursor,
      })

      // Create hydration context for profile lookups
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })

      // Hydrate all posts with author profiles and reply counts
      const hydratedPosts = await Promise.all(
        res.posts.map(async (post) => {
          const profileState = await ctx.hydrator.hydrateProfilesBasic(
            [post.creator],
            hydrateCtx,
          )
          const author = ctx.views.profileBasic(post.creator, profileState) ?? {
            did: post.creator,
            handle: 'handle.invalid',
            labels: [],
          }

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
