import type { AppContext } from '../../../../context.js'
import type { ImageUriBuilder } from '../../../../image/uri.js'
import { isCommunityUri } from '../membership-guard.js'
import { presignSpaceBlob, spaceMediaConfig } from '../space-media-presign.js'
import { signSpaceMedia, spaceMediaExpiry } from '../space-media-signing.js'
import { spaceOfRecordUri } from '../space-uri.js'
import { canViewCommunityPost } from '../tenant-gate.js'

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
  size?: number
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
    { uri?: string; cid?: string } | { record?: { uri?: string; cid?: string } }
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
export async function buildCommunityEmbedView(
  builders: EmbedUriBuilders,
  did: string,
  embed: unknown,
  spaceUri?: string,
  cfg?: AppContext['cfg'],
): Promise<Record<string, unknown> | undefined> {
  if (!embed || typeof embed !== 'object') return undefined
  const e = embed as AnyEmbed
  const { imgUriBuilder, videoUriBuilder } = builders
  const mediaConfig = spaceUri && cfg ? spaceMediaConfig(cfg) : undefined
  if (spaceUri && !mediaConfig) return undefined
  const imageUrl = async (ref: BlobRef | undefined) => {
    const cid = extractBlobCidString(ref?.ref)
    if (!cid) return null
    return spaceUri && cfg
      ? presignSpaceBlob(cfg, did, { cid, size: ref?.size })
      : imgUriBuilder.getPresetUri('feed_fullsize', did, cid)
  }
  const videoUrl = (url: string, cid: string) => {
    if (!spaceUri || !mediaConfig) return url
    const exp = spaceMediaExpiry(undefined, mediaConfig.windowSeconds)
    const sig = signSpaceMedia(
      spaceUri,
      did,
      cid,
      exp,
      cfg?.communityMediaSigningSecret,
    )
    if (!sig) return null
    const signed = new URL(url)
    signed.searchParams.set('space', spaceUri)
    signed.searchParams.set('exp', String(exp))
    signed.searchParams.set('sig', sig)
    return signed.toString()
  }
  if (e.$type === 'app.bsky.embed.external' && e.external) {
    const thumbCid = e.external.thumb
      ? extractBlobCidString(e.external.thumb.ref)
      : undefined
    const thumb = thumbCid
      ? spaceUri
        ? await imageUrl(e.external.thumb)
        : imgUriBuilder.getPresetUri('feed_thumbnail', did, thumbCid)
      : undefined
    return {
      $type: 'app.bsky.embed.external#view',
      external: {
        uri: e.external.uri ?? '',
        title: e.external.title ?? '',
        description: e.external.description ?? '',
        thumb: thumb ?? undefined,
      },
    }
  }
  if (e.$type === 'app.bsky.embed.images' && Array.isArray(e.images)) {
    const images = await Promise.all(
      e.images.map(async (img) => {
        const cid = extractBlobCidString(img.image?.ref)
        const url = await imageUrl(img.image)
        if (!cid || !url) return undefined
        return {
          thumb: spaceUri
            ? url
            : imgUriBuilder.getPresetUri('feed_thumbnail', did, cid),
          fullsize: url,
          alt: img.alt ?? '',
          aspectRatio: img.aspectRatio,
        }
      }),
    )
    if (!images.some(Boolean)) return undefined
    return {
      $type: 'app.bsky.embed.images#view',
      images: images.filter(Boolean),
    }
  }
  if (
    (e.$type === 'app.bsky.embed.video' ||
      e.$type === 'community.blacksky.embed.video') &&
    e.video
  ) {
    const cid = extractBlobCidString(e.video.ref)
    if (!cid) return undefined
    const playlist = videoUrl(videoUriBuilder.playlist({ did, cid }), cid)
    const thumbnail = videoUrl(videoUriBuilder.thumbnail({ did, cid }), cid)
    if (!playlist || !thumbnail) return undefined
    return {
      $type: 'app.bsky.embed.video#view',
      cid,
      playlist,
      thumbnail,
      alt: e.alt,
      aspectRatio: e.aspectRatio,
      presentation: e.presentation,
    }
  }
  if (e.$type === 'app.bsky.embed.gallery' && Array.isArray(e.items)) {
    const items = await Promise.all(
      e.items.slice(0, 10).map(async (item) => {
        const cid = extractBlobCidString(item.image?.ref)
        const url = await imageUrl(item.image)
        if (!cid || !url) return undefined
        return {
          $type: 'app.bsky.embed.gallery#viewImage',
          thumbnail: spaceUri
            ? url
            : imgUriBuilder.getPresetUri('feed_thumbnail', did, cid),
          fullsize: url,
          alt: item.alt ?? '',
          aspectRatio: item.aspectRatio,
        }
      }),
    )
    if (!items.some(Boolean)) return undefined
    return {
      $type: 'app.bsky.embed.gallery#view',
      items: items.filter(Boolean),
    }
  }
  return undefined
}

/**
 * Community content: the stub collection, or any record inside a permissioned
 * space. Both live in `community_post` rather than `post`, so every caller
 * that routes on storage location wants both.
 */
export const isCommunityPostUri = (uri: string): boolean => isCommunityUri(uri)

// True when the built view's author has a block relationship with the viewer.
export function isBlockedForViewer(
  view: Record<string, unknown> | undefined,
): boolean {
  const viewer = (view?.author as { viewer?: Record<string, unknown> })?.viewer
  return !!(viewer?.blocking || viewer?.blockedBy)
}

// True when the viewer mutes the built view's author (directly or via list).
export function isMutedForViewer(
  view: Record<string, unknown> | undefined,
): boolean {
  const viewer = (view?.author as { viewer?: Record<string, unknown> })?.viewer
  return !!(viewer?.muted || viewer?.mutedByList)
}

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
  spaceUri?: string
}

type HelperCtx = {
  cfg: AppContext['cfg']
  signingKey: AppContext['signingKey']
  idResolver: AppContext['idResolver']
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
    checkCommunityMembership: (...args: any[]) => any
    getCommunityFeedConfig: (...args: any[]) => any
    getCommunityPost: (...args: any[]) => any
    getCommunityPostReplyCount: (...args: any[]) => any
    getCommunityPostLikeCount: (...args: any[]) => any
    getCommunityPostQuoteCount: (...args: any[]) => any
    getCommunityPostViewerLike: (...args: any[]) => any
  }
}

/**
 * Spaces this request has already been authorized to view, decided once by the
 * caller. A list endpoint asks the managing app about its one space; without
 * this every row on the page would re-ask, turning a 30-post page into 30
 * delegated network checks behind a 5s timeout. A row in another space is not
 * in the set, so it still gets its own live decision.
 */
export type PreAuthorizedSpaces = ReadonlySet<string> | undefined

const spaceIsPreAuthorized = (
  post: CommunityPostRow,
  preAuthorized: PreAuthorizedSpaces,
): boolean => {
  if (!preAuthorized?.size) return false
  const space = post.spaceUri || spaceOfRecordUri(post.uri)
  return !!space && preAuthorized.has(space)
}

export async function buildCommunityPostView(
  ctx: HelperCtx,
  hydrateCtx: unknown,
  post: CommunityPostRow,
  depth = 0,
  viewerDid?: string,
  replyDisabled?: boolean,
  preAuthorized?: PreAuthorizedSpaces,
): Promise<Record<string, unknown> | undefined> {
  if (
    !spaceIsPreAuthorized(post, preAuthorized) &&
    !(await canViewCommunityPost(ctx as AppContext, post, viewerDid))
  ) {
    return undefined
  }
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
  const mediaBlobsReachable =
    !post.spaceUri || (!!ctx.cfg && !!spaceMediaConfig(ctx.cfg))
  let embedView: Record<string, unknown> | undefined
  if (embed && typeof embed === 'object') {
    const eType = (embed as AnyEmbed).$type
    if (eType === 'app.bsky.embed.record') {
      embedView = await buildQuoteView(
        ctx,
        hydrateCtx,
        embed as AnyEmbed,
        depth,
        viewerDid,
        preAuthorized,
      )
    } else if (eType === 'app.bsky.embed.recordWithMedia') {
      const ewm = embed as AnyEmbed
      const recordView = await buildQuoteView(
        ctx,
        hydrateCtx,
        { $type: 'app.bsky.embed.record', record: (ewm.record as any)?.record },
        depth,
        viewerDid,
        preAuthorized,
      )
      const mediaView = mediaBlobsReachable
        ? await buildCommunityEmbedView(
            builders,
            post.creator,
            ewm.media,
            post.spaceUri,
            ctx.cfg,
          )
        : undefined
      if (recordView || mediaView) {
        embedView = {
          $type: 'app.bsky.embed.recordWithMedia#view',
          record: recordView,
          media: mediaView,
        }
      }
    } else if (mediaBlobsReachable) {
      embedView = await buildCommunityEmbedView(
        builders,
        post.creator,
        embed,
        post.spaceUri,
        ctx.cfg,
      )
    }
  }
  const labelers = augmentLabelers(
    (hydrateCtx as { labelers?: unknown })?.labelers,
  )
  const [replyCountRes, likeCountRes, quoteCountRes, viewerLikeRes, labelMap] =
    await Promise.all([
      ctx.dataplane.getCommunityPostReplyCount({ uri: post.uri }),
      ctx.dataplane.getCommunityPostLikeCount({ uri: post.uri }),
      ctx.dataplane.getCommunityPostQuoteCount({ uri: post.uri }),
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
  const viewer = Object.keys(viewerState).length > 0 ? viewerState : undefined
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
    quoteCount: quoteCountRes.count ?? 0,
    bookmarkCount: 0,
    labels,
    ...(post.spaceUri ? { communitySpace: post.spaceUri } : {}),
    ...(viewer ? { viewer } : {}),
  }
}

async function buildQuoteView(
  ctx: HelperCtx,
  hydrateCtx: unknown,
  embed: AnyEmbed,
  depth: number,
  viewerDid?: string,
  preAuthorized?: PreAuthorizedSpaces,
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
  const quotedSpace = spaceOfRecordUri(quotedUri)
  if (
    quotedSpace &&
    !preAuthorized?.has(quotedSpace) &&
    !(await canViewCommunityPost(
      ctx as AppContext,
      { uri: quotedUri, spaceUri: quotedSpace },
      viewerDid,
    ))
  ) {
    return notFound(quotedUri)
  }
  const { post: quoted } = await ctx.dataplane.getCommunityPost({
    uri: quotedUri,
    allowedSpaceUris: quotedSpace ? [quotedSpace] : [],
  })
  if (!quoted) {
    return notFound(quotedUri)
  }
  const quotedView = await buildCommunityPostView(
    ctx,
    hydrateCtx,
    quoted,
    depth + 1,
    viewerDid,
    undefined,
    preAuthorized,
  )
  if (!quotedView) return notFound(quotedUri)
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
function augmentLabelers(labelers: unknown): {
  dids: string[]
  redact: Set<string>
} {
  const base = labelers as { dids?: string[]; redact?: Set<string> } | undefined
  const dids = new Set(base?.dids ?? [])
  dids.add(BLACKSKY_LABELER_DID)
  const redact = new Set(base?.redact ?? [])
  redact.add(BLACKSKY_LABELER_DID)
  return { dids: [...dids], redact }
}
