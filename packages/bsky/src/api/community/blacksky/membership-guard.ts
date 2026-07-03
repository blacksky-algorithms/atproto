import { AuthRequiredError } from '@atproto/xrpc-server'
import { AppContext } from '../../../context.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'

export const isCommunityUri = (uri?: string): boolean =>
  !!uri && uri.includes(`/${COMMUNITY_POST_COLLECTION}/`)

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
): Promise<void> {
  if (!uris.some((u) => isCommunityUri(u))) return
  if (!viewer) {
    throw new AuthRequiredError(
      'Must be a Blacksky community member',
      'MembershipRequired',
    )
  }
  const { isMember } = await ctx.dataplane.checkCommunityMembership({
    did: viewer,
  })
  if (!isMember) {
    throw new AuthRequiredError(
      'Must be a Blacksky community member',
      'MembershipRequired',
    )
  }
}
