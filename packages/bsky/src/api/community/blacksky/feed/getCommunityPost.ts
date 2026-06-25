import { InvalidRequestError, AuthRequiredError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { AtUriString, DidString, CidString, AtIdentifierString } from '@atproto/lex'
import { community } from '../../../../lexicons/index.js'
import {
  buildCommunityEmbedView,
  normalizeCidJsonRefs,
} from '../views/communityPostView.js'

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.feed.getCommunityPost, {
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

      const res = await ctx.dataplane.getCommunityPost({ uri: params.uri })
      if (!res.post) {
        throw new InvalidRequestError('Post not found', 'PostNotFound')
      }

      const post = res.post

      // Create hydration context for profile lookups
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer: requesterDid,
      })

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
        encoding: 'application/json' as const,
        body: {
          post: {
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
          },
        } as any,
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
