import { AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../context.js'
import { isSpaceRecordUri, spaceOfRecordUri } from './space-uri.js'
import { canViewCommunityPost } from './tenant-gate.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

/**
 * Master launch switch. Set COMMUNITY_POSTS_ENABLED=false on the appview to
 * hide community posts from every client (the tab and composer toggle both
 * key off checkMembership) and reject reads, with no client rebuild.
 * Defaults to enabled so an unset var keeps the current beta behavior.
 */
export const communityPostsEnabled = (): boolean =>
  process.env.COMMUNITY_POSTS_ENABLED !== 'false'

/**
 * Community content comes in two shapes: the stub collection on the original
 * feed, and any record inside a permissioned space. Both are gated, and every
 * guard keys off this one predicate, so recognising space URIs here covers
 * every call site at once rather than needing each to be widened.
 */
export const isCommunityUri = (uri?: string): boolean =>
  !!uri &&
  (uri.includes(`/${COMMUNITY_POST_COLLECTION}/`) || isSpaceRecordUri(uri))

/**
 * Every read into community-post content is gated behind authentication AND
 * community membership. Call this from any standard endpoint that can surface
 * a community post by URI (thread, likes, reposts, quotes, ...). No-ops when
 * none of the URIs are community posts, so non-community traffic is unaffected.
 */
export async function assertCommunityMembershipForUris(
  ctx: AppContext,
  viewer: string | null,
  uris: Array<string | undefined>,
): Promise<string[]> {
  if (!uris.some((u) => isCommunityUri(u))) return []
  if (!communityPostsEnabled() || !viewer) {
    throw new AuthRequiredError(
      'Must be a Blacksky community member',
      'MembershipRequired',
    )
  }
  const communityUris = [
    ...new Set(uris.filter((uri): uri is string => isCommunityUri(uri))),
  ]
  const allowed = await Promise.all(
    communityUris.map((uri) =>
      canViewCommunityPost(
        ctx,
        { uri, spaceUri: spaceOfRecordUri(uri) ?? undefined },
        viewer,
      ),
    ),
  )
  if (allowed.some((value) => !value)) {
    const hasTenantPost = communityUris.some(isSpaceRecordUri)
    throw new AuthRequiredError(
      hasTenantPost
        ? 'Must have access to the community feed'
        : 'Must be a Blacksky community member',
      'MembershipRequired',
    )
  }
  return [
    ...new Set(
      communityUris
        .map(spaceOfRecordUri)
        .filter((space): space is string => space !== null),
    ),
  ]
}
