import type { $Typed } from '@atproto/lex'
import type { AppContext } from '../../../../context.js'
import type { FeedItem } from '../../../../hydration/feed.js'
import type {
  HydrateCtx,
  HydrationState,
} from '../../../../hydration/hydrator.js'
import type { app } from '../../../../lexicons/index.js'
import { uriToDid } from '../../../../util/uris.js'
import { communityPostsEnabled, isCommunityUri } from '../membership-guard.js'
import { spaceOfRecordUri } from '../space-uri.js'
import {
  type PreAuthorizedSpaces,
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'

/**
 * Resolve whether the viewer may see community posts on merged surfaces.
 * Fail closed: any error, missing viewer, or disabled feature reads as
 * non-member, so standard endpoints degrade to their pre-merge behavior
 * rather than throwing or leaking.
 */
export async function resolveCommunityMembership(
  ctx: AppContext,
  viewer: string | null,
): Promise<boolean> {
  if (!communityPostsEnabled() || !viewer) return false
  try {
    const { isMember } = await ctx.dataplane.checkCommunityMembership({
      did: viewer,
    })
    return isMember
  } catch {
    return false
  }
}

/**
 * Drop community URIs from a skeleton unless the viewer is a member. The
 * authoritative gate for feed-generator skeletons and any other source that
 * can hand standard endpoints community URIs.
 */
export function filterCommunityUris<T extends { uri: string }>(
  items: T[],
  isMember: boolean,
): T[] {
  if (isMember) return items
  return items.filter((item) => !isCommunityUri(item.uri))
}

type HelperCtx = Pick<
  AppContext,
  'cfg' | 'dataplane' | 'hydrator' | 'idResolver' | 'signingKey' | 'views'
>

type CommunityRow = {
  uri: string
  replyRoot?: string
  replyParent?: string
}

/**
 * Reply context (root/parent views) for a community reply row. Blocked or
 * muted ancestors suppress the context entirely.
 */
export async function buildReplyContext(
  helperCtx: HelperCtx,
  hydrateCtx: unknown,
  row: CommunityRow,
  viewerDid?: string,
  preAuthorized?: PreAuthorizedSpaces,
) {
  const parentUri = row.replyParent || ''
  const rootUri = row.replyRoot || ''
  if (!parentUri) return undefined
  const rowSpace = spaceOfRecordUri(row.uri)
  const allowedSpaceUris = rowSpace ? [rowSpace] : []
  const [parentRes, rootRes] = await Promise.all([
    helperCtx.dataplane.getCommunityPost({ uri: parentUri, allowedSpaceUris }),
    rootUri && rootUri !== parentUri
      ? helperCtx.dataplane.getCommunityPost({
          uri: rootUri,
          allowedSpaceUris,
        })
      : Promise.resolve(null),
  ])
  if (!parentRes?.post) return undefined
  const parentView = await buildCommunityPostView(
    helperCtx as any,
    hydrateCtx as any,
    parentRes.post as any,
    0,
    viewerDid,
    undefined,
    preAuthorized,
  )
  if (!parentView) return undefined
  const rootView = rootRes?.post
    ? await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx as any,
        rootRes.post as any,
        0,
        viewerDid,
        undefined,
        preAuthorized,
      )
    : parentView
  if (!rootView) return undefined
  // Blocked or muted parent/root must not surface through reply context.
  if (
    isBlockedForViewer(parentView) ||
    isBlockedForViewer(rootView) ||
    isMutedForViewer(parentView) ||
    isMutedForViewer(rootView)
  ) {
    return undefined
  }
  return { root: rootView, parent: parentView }
}

/**
 * Build a feedViewPost-shaped item for a community row on a merged standard
 * surface (timeline, author feed, and later getFeed). Returns undefined when
 * the item must be dropped (blocked or muted author, or a reply whose
 * ancestors are unavailable). Callers pass rows from the skeleton side
 * channel; nothing is refetched for the post itself.
 */
export type CommunityFeedItemOptions = {
  reason?: $Typed<app.bsky.feed.defs.ReasonRepost>
  /**
   * The profile surface keeps a fully muted owner's repost of another muted
   * author, as the standard author feed does (`!authorMuted ||
   * originatorMuted`): the viewer chose to open that profile.
   */
  keepMutedAuthor?: boolean
}

export async function presentCommunityFeedItem(
  helperCtx: HelperCtx,
  hydrateCtx: unknown,
  row: CommunityRow,
  viewerDid?: string,
  opts?: CommunityFeedItemOptions,
): Promise<Record<string, unknown> | undefined> {
  try {
    const post = await buildCommunityPostView(
      helperCtx as any,
      hydrateCtx as any,
      row as any,
      0,
      viewerDid,
    )
    if (!post) return undefined
    if (isBlockedForViewer(post)) return undefined
    if (!opts?.keepMutedAuthor && isMutedForViewer(post)) return undefined
    const reason = opts?.reason ? { reason: opts.reason } : {}
    if (row.replyParent) {
      const reply = await buildReplyContext(
        helperCtx,
        hydrateCtx,
        row,
        viewerDid,
      )
      // A reply whose ancestors are blocked/muted/missing does not surface
      // as a bare orphan on merged surfaces.
      if (!reply) return undefined
      return { post, reply, ...reason }
    }
    return { post, ...reason }
  } catch {
    // A failed community item degrades to absence, never to a 5xx.
    return undefined
  }
}

/**
 * Community views are keyed per feed item, not per post: the same post can
 * appear on one page as the author's own item and as a follow's repost, and
 * standard timelines keep both (the client tuner dedupes).
 */
export const feedItemKey = (item: FeedItem): string =>
  `${item.repost?.uri ?? ''}|${item.post.uri}`

export type RepostSurface = 'following' | 'profile'

/**
 * Repost records plus the reposters' profiles and viewer state for the
 * community repost items on a merged page. Reposts of community posts are
 * ordinary public records, so the standard repost hydration serves them.
 */
export async function hydrateCommunityReposts(
  ctx: Pick<AppContext, 'hydrator'>,
  hydrateCtx: HydrateCtx,
  items: FeedItem[],
): Promise<HydrationState> {
  const uris = [
    ...new Set(
      items.flatMap((item) =>
        item.repost && isCommunityUri(item.post.uri) ? [item.repost.uri] : [],
      ),
    ),
  ]
  if (uris.length === 0) return {}
  return ctx.hydrator.hydrateReposts(uris, hydrateCtx)
}

/**
 * The `reasonRepost` for a community repost item, or `'drop'` when the item
 * must not render: the repost record is gone or names another post, the
 * reposter cannot be shown, or the viewer's relationship to the reposter
 * hides it on this surface. Mirrors `feedViewPost` and the originator checks
 * of `feedItemBlocksAndMutes` for standard items.
 */
export function communityRepostReason(
  ctx: Pick<AppContext, 'views'>,
  item: FeedItem,
  state: HydrationState,
  surface: RepostSurface,
): $Typed<app.bsky.feed.defs.ReasonRepost> | 'drop' {
  const repostUri = item.repost?.uri
  if (!repostUri) return 'drop'
  const repost = state.reposts?.get(repostUri)
  if (!repost || repost.record.subject.uri !== item.post.uri) return 'drop'
  const reposter = uriToDid(repostUri)
  if (ctx.views.viewerBlockExists(reposter, state)) return 'drop'
  if (
    surface === 'following' &&
    (ctx.views.viewerMuteExists(reposter, state) ||
      ctx.views.viewerRepostMuteExists(reposter, state))
  ) {
    return 'drop'
  }
  return ctx.views.reasonRepost(repostUri, repost, state) ?? 'drop'
}
