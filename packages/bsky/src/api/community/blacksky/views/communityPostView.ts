import { ImageUriBuilder } from '../../../../image/uri.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'
const BLACKSKY_LABELER_DID = 'did:plc:d2mkddsbmnrgr3domzg5qexf'

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

type BlobRef = {
  ref?: { $link?: string; '/'?: string } | string
  mimeType?: string
}

type AnyEmbed = {
  $type?: string
  external?: {
    uri?: string
    title?: string
    description?: string
    thumb?: BlobRef & { $type?: string; size?: number }
  }
  images?: Array<{
    image?: BlobRef
    alt?: string
    aspectRatio?: { width: number; height: number }
  }>
  record?:
    | { uri?: string; cid?: string }
    | { record?: { uri?: string; cid?: string } }
  video?: BlobRef
  alt?: string
  aspectRatio?: { width: number; height: number }
  presentation?: string
  items?: Array<{
    image?: BlobRef
    alt?: string
    aspectRatio?: { width: number; height: number }
  }>
  media?: AnyEmbed
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

type EmbedUriBuilders = {
  imgUriBuilder: ImageUriBuilder
  videoUriBuilder: {
    playlist: (args: { did: string; cid: string }) => string
    thumbnail: (args: { did: string; cid: string }) => string
  }
}

// Build external / images / video / gallery / recordWithMedia view (sync);
// record (quote) is handled in buildCommunityPostView so it can fetch.
export function buildCommunityEmbedView(
  builders: EmbedUriBuilders,
  did: string,
  embed: unknown,
): Record<string, unknown> | undefined {
  if (!embed || typeof embed !== 'object') return undefined
  const e = embed as AnyEmbed
  const { imgUriBuilder, videoUriBuilder } = builders
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
  if (e.$type === 'app.bsky.embed.video' && e.video) {
    const cid = extractBlobCidString(e.video.ref)
    if (!cid) return undefined
    return {
      $type: 'app.bsky.embed.video#view',
      cid,
      playlist: videoUriBuilder.playlist({ did, cid }),
      thumbnail: videoUriBuilder.thumbnail({ did, cid }),
      alt: e.alt,
      aspectRatio: e.aspectRatio,
      presentation: e.presentation,
    }
  }
  if (e.$type === 'app.bsky.embed.gallery' && Array.isArray(e.items)) {
    return {
      $type: 'app.bsky.embed.gallery#view',
      items: e.items
        .slice(0, 10)
        .map((item) => {
          const cid = extractBlobCidString(item.image?.ref)
          if (!cid) return undefined
          return {
            $type: 'app.bsky.embed.gallery#viewImage',
            thumbnail: imgUriBuilder.getPresetUri('feed_thumbnail', did, cid),
            fullsize: imgUriBuilder.getPresetUri('feed_fullsize', did, cid),
            alt: item.alt ?? '',
            aspectRatio: item.aspectRatio,
          }
        })
        .filter(Boolean),
    }
  }
  return undefined
}

export const isCommunityPostUri = (uri: string): boolean =>
  uri.includes(`/${COMMUNITY_POST_COLLECTION}/`)

type CommunityPostRow = {
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
}

type HelperCtx = {
  hydrator: {
    hydrateProfilesBasic: (...args: any[]) => any
    label: { getLabelsForSubjects: (...args: any[]) => any }
  }
  views: {
    profileBasic: (...args: any[]) => any
    imgUriBuilder: ImageUriBuilder
    videoUriBuilder: EmbedUriBuilders['videoUriBuilder']
  }
  dataplane: {
    getCommunityPost: (...args: any[]) => any
    getCommunityPostReplyCount: (...args: any[]) => any
    getCommunityPostLikeCount: (...args: any[]) => any
    getCommunityPostViewerLike: (...args: any[]) => any
  }
}

export async function buildCommunityPostView(
  ctx: HelperCtx,
  hydrateCtx: unknown,
  post: CommunityPostRow,
  depth = 0,
  viewerDid?: string,
  replyDisabled?: boolean,
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
  const builders = {
    imgUriBuilder: ctx.views.imgUriBuilder,
    videoUriBuilder: ctx.views.videoUriBuilder,
  }
  let embedView: Record<string, unknown> | undefined
  if (embed && typeof embed === 'object') {
    const eType = (embed as AnyEmbed).$type
    if (eType === 'app.bsky.embed.record') {
      embedView = await buildQuoteView(ctx, hydrateCtx, embed as AnyEmbed, depth)
    } else if (eType === 'app.bsky.embed.recordWithMedia') {
      const ewm = embed as AnyEmbed
      const recordView = await buildQuoteView(
        ctx,
        hydrateCtx,
        { $type: 'app.bsky.embed.record', record: (ewm.record as any)?.record },
        depth,
      )
      const mediaView = buildCommunityEmbedView(
        builders,
        post.creator,
        ewm.media,
      )
      if (recordView || mediaView) {
        embedView = {
          $type: 'app.bsky.embed.recordWithMedia#view',
          record: recordView,
          media: mediaView,
        }
      }
    } else {
      embedView = buildCommunityEmbedView(builders, post.creator, embed)
    }
  }
  const labelers = augmentLabelers(
    (hydrateCtx as { labelers?: unknown })?.labelers,
  )
  const [replyCountRes, likeCountRes, viewerLikeRes, labelMap] =
    await Promise.all([
      ctx.dataplane.getCommunityPostReplyCount({ uri: post.uri }),
      ctx.dataplane.getCommunityPostLikeCount({ uri: post.uri }),
      viewerDid
        ? ctx.dataplane.getCommunityPostViewerLike({
            subjectUri: post.uri,
            viewerDid,
          })
        : Promise.resolve({ likeUri: '' }),
      ctx.hydrator.label.getLabelsForSubjects([post.uri], labelers),
    ])
  const viewerState: Record<string, unknown> = {}
  if (viewerLikeRes.likeUri) viewerState.like = viewerLikeRes.likeUri
  if (replyDisabled) viewerState.replyDisabled = true
  const viewer =
    Object.keys(viewerState).length > 0 ? viewerState : undefined
  const labels = (labelMap?.getBySubject?.(post.uri) ?? []) as unknown[]
  return {
    $type: 'app.bsky.feed.defs#postView',
    uri: post.uri,
    cid: post.cid,
    author,
    record,
    embed: embedView,
    indexedAt: post.indexedAt,
    likeCount: likeCountRes.count ?? 0,
    repostCount: 0,
    replyCount: replyCountRes.count ?? 0,
    quoteCount: 0,
    bookmarkCount: 0,
    labels,
    ...(viewer ? { viewer } : {}),
  }
}

async function buildQuoteView(
  ctx: HelperCtx,
  hydrateCtx: unknown,
  embed: AnyEmbed,
  depth: number,
): Promise<Record<string, unknown>> {
  const quotedUri = (embed.record as { uri?: string } | undefined)?.uri
  const notFound = (uri: string) => ({
    $type: 'app.bsky.embed.record#view',
    record: {
      $type: 'app.bsky.embed.record#viewNotFound',
      uri,
      notFound: true,
    },
  })
  if (!quotedUri || !isCommunityPostUri(quotedUri)) {
    return notFound(quotedUri ?? '')
  }
  if (depth >= 1) {
    return notFound(quotedUri)
  }
  const { post: quoted } = await ctx.dataplane.getCommunityPost({
    uri: quotedUri,
  })
  if (!quoted) {
    return notFound(quotedUri)
  }
  const quotedView = await buildCommunityPostView(
    ctx,
    hydrateCtx,
    quoted,
    depth + 1,
  )
  return {
    $type: 'app.bsky.embed.record#view',
    record: {
      $type: 'app.bsky.embed.record#viewRecord',
      uri: quotedView.uri,
      cid: quotedView.cid,
      author: quotedView.author,
      value: quotedView.record,
      embeds: quotedView.embed ? [quotedView.embed] : undefined,
      labels: quotedView.labels,
      likeCount: quotedView.likeCount,
      replyCount: quotedView.replyCount,
      repostCount: quotedView.repostCount,
      quoteCount: quotedView.quoteCount,
      indexedAt: quotedView.indexedAt,
    },
  }
}

// Always check the Blacksky labeler on community posts, even if the request's
// atproto-accept-labelers header hadn't loaded it yet (first paint timing).
function augmentLabelers(
  labelers: unknown,
): { dids: string[]; redact: Set<string> } {
  const base = labelers as
    | { dids?: string[]; redact?: Set<string> }
    | undefined
  const dids = new Set(base?.dids ?? [])
  dids.add(BLACKSKY_LABELER_DID)
  const redact = new Set(base?.redact ?? [])
  redact.add(BLACKSKY_LABELER_DID)
  return { dids: [...dids], redact }
}
