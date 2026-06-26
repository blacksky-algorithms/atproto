// Build an `app.bsky.feed.defs#postView` for a community.blacksky.feed.post
// row. Two transformations the bare timeline/feed handlers used to skip:
//
// 1. CID JSON normalization. CommunityPostView.embed is the JSON string we
//    persisted from the appview, which (because the lex parser walked it
//    before JSON.stringify) emits `{"/":"bafkrei..."}` (multiformats CID
//    default) instead of the AT Protocol wire form `{"$link":"bafkrei..."}`.
//    Clients (and any record-validating consumer) expect $link.
//
// 2. Embed view hydration. Clients render embeds from `post.embed` (the
//    hydrated view with a resolved CDN thumb URL), not from
//    `post.record.embed` (the raw record). Without the view, the YouTube
//    card on a community post renders as nothing.

import { ImageUriBuilder } from '../../../../image/uri.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

// Walk any JSON-shaped object/array and rewrite `{"/":"..."}` (multiformats'
// default CID#toJSON) back to `{"$link":"..."}` (atproto wire form). Other
// shapes pass through unchanged.
export function normalizeCidJsonRefs(v: unknown): unknown {
  if (v === null || typeof v !== 'object') return v
  if (Array.isArray(v)) return v.map(normalizeCidJsonRefs)
  const obj = v as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1 && keys[0] === '/' && typeof obj['/'] === 'string') {
    return { $link: obj['/'] }
  }
  const out: Record<string, unknown> = {}
  for (const k of keys) {
    out[k] = normalizeCidJsonRefs(obj[k])
  }
  return out
}

type AnyEmbed = {
  $type?: string
  external?: {
    uri?: string
    title?: string
    description?: string
    thumb?: {
      $type?: string
      ref?: { $link?: string; '/'?: string } | string
      mimeType?: string
      size?: number
    }
  }
  images?: Array<{
    image?: {
      ref?: { $link?: string; '/'?: string } | string
      mimeType?: string
    }
    alt?: string
    aspectRatio?: { width: number; height: number }
  }>
}

function extractBlobCidString(ref: unknown): string | undefined {
  if (typeof ref === 'string') return ref
  if (ref && typeof ref === 'object') {
    const r = ref as Record<string, unknown>
    if (typeof r.$link === 'string') return r.$link
    if (typeof r['/'] === 'string') return r['/'] as string
  }
  return undefined
}

// Build the hydrated `post.embed` view from a parsed record embed. v1 covers
// app.bsky.embed.external (the dominant case — link cards / YouTube). Images
// and other embed types fall through to undefined for now; record-level
// rendering still works because we hand back the (normalized) record.
export function buildCommunityEmbedView(
  imgUriBuilder: ImageUriBuilder,
  did: string,
  embed: unknown,
): Record<string, unknown> | undefined {
  if (!embed || typeof embed !== 'object') return undefined
  const e = embed as AnyEmbed
  if (e.$type === 'app.bsky.embed.external' && e.external) {
    const thumbCid = e.external.thumb
      ? extractBlobCidString(e.external.thumb.ref)
      : undefined
    return {
      $type: 'app.bsky.embed.external#view',
      external: {
        uri: e.external.uri ?? '',
        title: e.external.title ?? '',
        description: e.external.description ?? '',
        thumb: thumbCid
          ? imgUriBuilder.getPresetUri('feed_thumbnail', did, thumbCid)
          : undefined,
      },
    }
  }
  if (e.$type === 'app.bsky.embed.images' && Array.isArray(e.images)) {
    return {
      $type: 'app.bsky.embed.images#view',
      images: e.images
        .map((img) => {
          const cid = extractBlobCidString(img.image?.ref)
          if (!cid) return undefined
          return {
            thumb: imgUriBuilder.getPresetUri('feed_thumbnail', did, cid),
            fullsize: imgUriBuilder.getPresetUri('feed_fullsize', did, cid),
            alt: img.alt ?? '',
            aspectRatio: img.aspectRatio,
          }
        })
        .filter(Boolean),
    }
  }
  return undefined
}

export const isCommunityPostUri = (uri: string): boolean =>
  uri.includes(`/${COMMUNITY_POST_COLLECTION}/`)

// Build a hydrated app.bsky.feed.defs#postView for a single CommunityPostView
// row off the dataplane. Used by the feed/timeline/post handlers AND the
// getPostThreadV2 community branch so the same shape is rendered everywhere.
export async function buildCommunityPostView(
  ctx: {
    hydrator: { hydrateProfilesBasic: Function }
    views: { profileBasic: Function; imgUriBuilder: ImageUriBuilder }
    dataplane: { getCommunityPostReplyCount: Function }
  },
  hydrateCtx: unknown,
  post: {
    uri: string
    cid: string
    creator: string
    text: string
    createdAt: string
    indexedAt: string
    facets?: string
    embed?: string
    langs?: string
    replyRoot?: string
    replyRootCid?: string
    replyParent?: string
    replyParentCid?: string
  },
): Promise<Record<string, unknown>> {
  const profileState = await ctx.hydrator.hydrateProfilesBasic(
    [post.creator],
    hydrateCtx,
  )
  const author = ctx.views.profileBasic(post.creator, profileState) ?? {
    did: post.creator,
    handle: 'handle.invalid',
    labels: [],
  }
  const facets = post.facets
    ? normalizeCidJsonRefs(JSON.parse(post.facets))
    : undefined
  const embed = post.embed
    ? normalizeCidJsonRefs(JSON.parse(post.embed))
    : undefined
  const langs = post.langs
    ? post.langs.replace(/[{}]/g, '').split(',').filter(Boolean)
    : undefined
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
  const embedView = embed
    ? buildCommunityEmbedView(ctx.views.imgUriBuilder, post.creator, embed)
    : undefined
  const replyCountRes = await ctx.dataplane.getCommunityPostReplyCount({
    uri: post.uri,
  })
  return {
    uri: post.uri,
    cid: post.cid,
    author,
    record,
    embed: embedView,
    indexedAt: post.indexedAt,
    likeCount: 0,
    repostCount: 0,
    replyCount: replyCountRes.count ?? 0,
    quoteCount: 0,
    bookmarkCount: 0,
    labels: [],
  }
}
