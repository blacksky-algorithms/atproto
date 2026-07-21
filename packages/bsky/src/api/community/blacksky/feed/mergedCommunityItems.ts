import { AppContext } from '../../../../context.js'
import { communityPostsEnabled, isCommunityUri } from '../membership-guard.js'
import {
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

type HelperCtx = {
  hydrator: AppContext['hydrator']
  views: AppContext['views']
  dataplane: AppContext['dataplane']
}

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
) {
  const parentUri = row.replyParent || ''
  const rootUri = row.replyRoot || ''
  if (!parentUri) return undefined
  const [parentRes, rootRes] = await Promise.all([
    helperCtx.dataplane.getCommunityPost({ uri: parentUri }),
    rootUri && rootUri !== parentUri
      ? helperCtx.dataplane.getCommunityPost({ uri: rootUri })
      : Promise.resolve(null),
  ])
  if (!parentRes?.post) return undefined
  const parentView = await buildCommunityPostView(
    helperCtx as any,
    hydrateCtx as any,
    parentRes.post as any,
    0,
    viewerDid,
  )
  const rootView = rootRes?.post
    ? await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx as any,
        rootRes.post as any,
        0,
        viewerDid,
      )
    : parentView
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
export async function presentCommunityFeedItem(
  helperCtx: HelperCtx,
  hydrateCtx: unknown,
  row: CommunityRow,
  viewerDid?: string,
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
    if (isBlockedForViewer(post) || isMutedForViewer(post)) return undefined
    if (row.replyParent) {
      const reply = await buildReplyContext(helperCtx, hydrateCtx, row, viewerDid)
      // A reply whose ancestors are blocked/muted/missing does not surface
      // as a bare orphan on merged surfaces.
      if (!reply) return undefined
      return { post, reply }
    }
    return { post }
  } catch {
    // A failed community item degrades to absence, never to a 5xx.
    return undefined
  }
}
