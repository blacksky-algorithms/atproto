import { createServiceJwt } from '@atproto/xrpc-server'
import type { AppContext } from '../../../context.js'
import { community } from '../../../lexicons/index.js'
import { isSpaceUri, parseSpaceUri, spaceOfRecordUri } from './space-uri.js'

/**
 * Our own space-keyed access question. The spec's
 * `com.atproto.simplespace.checkUserAccess` remains a spec compatibility alias;
 * every decision terminates at this space-keyed capability check.
 */
const CHECK_SPACE_ACCESS = 'community.blacksky.space.checkAccess'
/**
 * The managing app that decides access for this appview's Acorn-managed spaces,
 * as a service identifier (`did#fragment`). This is an explicit authority
 * assignment for the spaces we host, not discovery for arbitrary federated
 * spaces — so it is pinned in config and never read from a user-controlled
 * record. Absent or malformed, every space access check fails closed.
 */
const MANAGING_APP = () => process.env.COMMUNITY_SPACE_MANAGING_APP ?? ''
/**
 * The one space type this appview manages. A recognised type is necessary but
 * never sufficient: the managing app still authorises the exact space, its
 * community, policy, lifecycle and viewer. An unmanaged type has no configured
 * decider and is denied here before any network call.
 */
const SUPPORTED_SPACE_TYPE = 'community.blacksky.feed'
const CACHE_TTL_MS = () =>
  Number(process.env.COMMUNITY_ACCESS_CACHE_TTL_MS ?? '') || 60_000
const CACHE_MAX_SIZE = 100_000
const REQUEST_TIMEOUT_MS = 5_000

type CommunityPost = {
  uri: string
  spaceUri?: string
}

export type FeedPermission = 'view' | 'contribute' | 'moderate'

export type CommunityFeedConfig = community.blacksky.feed.config.Main & {
  contentType: 'communityRecord'
  visibility: 'gated'
  authorization: community.blacksky.feed.config.Authorization
  /**
   * Present when the feed is backed by a permissioned space. A plain string,
   * never `format: at-uri` — a space URI is not a valid at-uri.
   */
  space?: string
}

/**
 * Which write path a feed uses. A space-backed feed's content lives in members'
 * permissioned repos and reaches the appview through a syncer, so the appview
 * never accepts a direct submission for one.
 */
export const isSpaceBackedFeed = (
  config: CommunityFeedConfig | null | undefined,
): boolean => !!config?.space && isSpaceUri(config.space)

type CacheEntry<T> = {
  value: T
  expiresAt: number
}

const configCache = new Map<string, CacheEntry<CommunityFeedConfig | null>>()
const accessCache = new Map<string, CacheEntry<boolean>>()

const cacheGet = <T>(cache: Map<string, CacheEntry<T>>, key: string) => {
  const entry = cache.get(key)
  if (!entry) return undefined
  if (entry.expiresAt <= Date.now()) {
    cache.delete(key)
    return undefined
  }
  return entry.value
}

const cacheSet = <T>(
  cache: Map<string, CacheEntry<T>>,
  key: string,
  value: T,
  ttlMs: number = CACHE_TTL_MS(),
) => {
  if (cache.size >= CACHE_MAX_SIZE) {
    const first = cache.keys().next().value
    if (first !== undefined) cache.delete(first)
  }
  cache.set(key, { value, expiresAt: Date.now() + ttlMs })
}

const parseConfig = (raw: string): CommunityFeedConfig | null => {
  try {
    const parsed = community.blacksky.feed.config.$safeParse(JSON.parse(raw))
    if (
      !parsed.success ||
      parsed.value.contentType !== 'communityRecord' ||
      parsed.value.visibility !== 'gated' ||
      !parsed.value.authorization
    ) {
      return null
    }
    return parsed.value as CommunityFeedConfig
  } catch {
    return null
  }
}

export const getCommunityFeedConfig = async (
  ctx: AppContext,
  feedUri: string,
) => {
  const cached = cacheGet(configCache, feedUri)
  if (cached !== undefined) return cached
  const { configJson } = await ctx.dataplane.getCommunityFeedConfig({ feedUri })
  const config = configJson ? parseConfig(configJson) : null
  cacheSet(configCache, feedUri, config)
  return config
}

/** Resolve a named service (`#fragment`) from a DID document. */
const serviceEndpoint = async (
  ctx: AppContext,
  did: string,
  fragment: string,
) => {
  const doc = await ctx.idResolver.did.resolve(did)
  const service = (doc?.service ?? []).find(
    (candidate) => candidate.id.split('#').at(1) === fragment,
  )
  if (service && typeof service.serviceEndpoint === 'string') {
    return safeEndpoint(service.serviceEndpoint)
  }
  return null
}

/** Reject plain http except on loopback, and strip anything but the origin. */
const safeEndpoint = (raw: string) => {
  let endpoint: URL
  try {
    endpoint = new URL(raw)
  } catch {
    return null
  }
  const isLoopback =
    endpoint.hostname === 'localhost' ||
    endpoint.hostname === '127.0.0.1' ||
    endpoint.hostname === '[::1]'
  if (
    endpoint.protocol !== 'https:' &&
    !(endpoint.protocol === 'http:' && isLoopback)
  ) {
    return null
  }
  endpoint.username = ''
  endpoint.password = ''
  endpoint.search = ''
  endpoint.hash = ''
  return endpoint
}

const serviceJwt = (ctx: AppContext, aud: string, lxm: string) =>
  createServiceJwt({
    iss: ctx.cfg.serverDid,
    aud,
    lxm,
    keypair: ctx.signingKey,
  })

const fetchJson = async (url: URL, token: string) => {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  try {
    const response = await fetch(url, {
      headers: { authorization: `Bearer ${token}` },
      signal: controller.signal,
    })
    if (!response.ok) return null
    return (await response.json()) as Record<string, unknown>
  } finally {
    clearTimeout(timeout)
  }
}

/**
 * Which app decides access for this space.
 *
 * Not asked of the space itself: this appview serves projected content and is
 * configured with the trusted managing app for the Acorn-managed spaces it
 * hosts. Discovering it through the space host's `getSpace` needs a repo-reading
 * credential the appview should not hold for a bare authorisation question, and
 * would follow a user-controlled record. So it is pinned in config, gated to the
 * one space type we manage, and validated downstream before use. A malformed or
 * absent pin, or an unmanaged space type, yields `null` and fails closed.
 */
const managingAppForSpace = (spaceUri: string): string | null => {
  const ref = parseSpaceUri(spaceUri)
  if (!ref || ref.spaceType !== SUPPORTED_SPACE_TYPE) return null
  return MANAGING_APP() || null
}

/**
 * One access decision per (space, viewer, permission), asked of the configured
 * managing app. Fails closed: an unresolvable or unreachable managing app denies.
 */
const delegatedSpaceCheck = async (
  ctx: AppContext,
  spaceUri: string,
  user: string,
  permission: FeedPermission,
  retryUnavailable = false,
) => {
  const cacheKey = `${spaceUri}\u0000${user}\u0000${permission}`
  const cached = cacheGet(accessCache, cacheKey)
  if (cached !== undefined) return cached

  const serviceId = managingAppForSpace(spaceUri)
  if (!serviceId) {
    if (retryUnavailable) throw new Error('no managing app configured')
    return false
  }
  const [did, fragment] = serviceId.split('#')
  if (!did?.startsWith('did:') || !fragment) {
    if (retryUnavailable) throw new Error('invalid managing app')
    return false
  }
  const endpoint = await serviceEndpoint(ctx, did, fragment)
  if (!endpoint) {
    if (retryUnavailable) throw new Error('managing app unavailable')
    return false
  }

  const token = await serviceJwt(ctx, serviceId, CHECK_SPACE_ACCESS)
  const params = new URLSearchParams({ space: spaceUri, did: user, permission })
  const body = await fetchJson(
    new URL(`/xrpc/${CHECK_SPACE_ACCESS}?${params}`, endpoint),
    token,
  )
  if (!body && retryUnavailable) throw new Error('access check unavailable')
  const allowed = body?.allowed === true
  cacheSet(accessCache, cacheKey, allowed)
  return allowed
}

export async function canViewCommunityPost(
  ctx: AppContext,
  post: CommunityPost,
  viewer: string | null | undefined,
  opts?: { retryUnavailable?: boolean },
): Promise<boolean> {
  if (process.env.COMMUNITY_POSTS_ENABLED === 'false' || !viewer) return false
  const retryUnavailable = opts?.retryUnavailable === true
  try {
    /**
     * The row is not the only source of the space: callers reach here with a
     * missing row (unknown uri) or a row the data plane filtered out (a
     * moderation-flagged post), and either would otherwise downgrade a space
     * post to the community-wide membership check. The URI names its own space.
     */
    const spaceUri = post.spaceUri || spaceOfRecordUri(post.uri)
    if (!spaceUri) {
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: viewer,
      })
      return isMember
    }
    return await delegatedSpaceCheck(
      ctx,
      spaceUri,
      viewer,
      'view',
      retryUnavailable,
    )
  } catch (err) {
    if (retryUnavailable) throw err
    return false
  }
}

/**
 * The `view` decision for a whole space, with no post in hand.
 *
 * A list endpoint asks this once and then trusts it for every row on the page:
 * read access is uniform within a space (differentiated content means separate
 * spaces), so a per-post re-check would fan one decision out into N delegated
 * network calls without deciding anything new. Fails closed like every other
 * path here — an unresolvable space (never provisioned, not yet active,
 * deleted) has no managing app to ask and therefore denies.
 */
export const canViewSpace = async (
  ctx: AppContext,
  spaceUri: string,
  viewer: string | null | undefined,
): Promise<boolean> => {
  if (process.env.COMMUNITY_POSTS_ENABLED === 'false' || !viewer) return false
  if (!isSpaceUri(spaceUri)) return false
  try {
    return await delegatedSpaceCheck(ctx, spaceUri, viewer, 'view')
  } catch {
    return false
  }
}

export const canContributeToSpace = async (
  ctx: AppContext,
  spaceUri: string,
  author: string,
): Promise<boolean> => {
  return await delegatedSpaceCheck(ctx, spaceUri, author, 'contribute', true)
}

export const clearTenantGateCaches = () => {
  configCache.clear()
  accessCache.clear()
}
