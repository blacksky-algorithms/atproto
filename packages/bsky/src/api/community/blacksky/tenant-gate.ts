import { createServiceJwt } from '@atproto/xrpc-server'
import { AppContext } from '../../../context.js'
import { community } from '../../../lexicons/index.js'
import { isSpaceUri, parseSpaceUri } from './space-uri.js'

/**
 * Our own space-keyed access question. The spec's
 * `com.atproto.simplespace.checkUserAccess` is the authority's credential-mint
 * hook and answers about reads only, so it cannot carry a permission.
 */
const CHECK_SPACE_ACCESS = 'community.blacksky.space.checkAccess'
const GET_SPACE = 'com.atproto.space.getSpace'
const SPACE_HOST_SERVICE_ID = 'atproto_space_host'
/** A space's managing app is a deployment fact, not per-request state. */
const SPACE_CACHE_TTL_MS = 10 * 60_000
const CACHE_TTL_MS = 60_000
const CACHE_MAX_SIZE = 100_000
const REQUEST_TIMEOUT_MS = 5_000

type CommunityPost = {
  uri: string
  spaceUri?: string
}

export type FeedPermission =
  | 'canView'
  | 'canPost'
  | 'canRemovePost'
  | 'canRemoveMember'
  | 'canConfigure'
  | 'canModerate'

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
/** space URI -> the managing app's service identifier (`did#fragment`). */
const managingAppCache = new Map<string, CacheEntry<string | null>>()

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
  ttlMs: number = CACHE_TTL_MS,
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

/** Resolve one named service (`#fragment`) from a DID document. */
const serviceEndpoint = async (
  ctx: AppContext,
  did: string,
  fragment: string,
) => {
  const doc = await ctx.idResolver.did.resolve(did)
  const service = (doc?.service ?? []).find(
    (candidate) => candidate.id.split('#').at(1) === fragment,
  )
  if (!service || typeof service.serviceEndpoint !== 'string') return null
  return safeEndpoint(service.serviceEndpoint)
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
 * Which app decides access for this space, asked of the space itself.
 *
 * A space URI is self-describing (D13): its first segment is the authority,
 * which is the community DID (D21), whose document names the space host. The
 * host's `getSpace` then names the managing app. Nothing about the feed is
 * involved, which is the point — a feed is a view over a space, and reads are
 * gated by the space.
 */
const managingAppForSpace = async (ctx: AppContext, spaceUri: string) => {
  const cached = cacheGet(managingAppCache, spaceUri)
  if (cached !== undefined) return cached

  let managingApp: string | null = null
  const ref = parseSpaceUri(spaceUri)
  if (ref) {
    const host = await serviceEndpoint(ctx, ref.spaceDid, SPACE_HOST_SERVICE_ID)
    if (host) {
      const token = await serviceJwt(ctx, ref.spaceDid, GET_SPACE)
      const url = new URL(
        `/xrpc/${GET_SPACE}?space=${encodeURIComponent(spaceUri)}`,
        host,
      )
      const body = await fetchJson(url, token)
      const config = body?.config as { managingApp?: unknown } | undefined
      if (typeof config?.managingApp === 'string') {
        managingApp = config.managingApp
      }
    }
  }
  cacheSet(managingAppCache, spaceUri, managingApp, SPACE_CACHE_TTL_MS)
  return managingApp
}

/**
 * One access decision per (space, viewer, permission), asked of the space's
 * managing app. Fails closed: an unreachable space host or managing app denies.
 */
const delegatedSpaceCheck = async (
  ctx: AppContext,
  spaceUri: string,
  user: string,
  permission: FeedPermission,
) => {
  const cacheKey = `${spaceUri}\u0000${user}\u0000${permission}`
  const cached = cacheGet(accessCache, cacheKey)
  if (cached !== undefined) return cached

  const serviceId = await managingAppForSpace(ctx, spaceUri)
  if (!serviceId) return false
  const [did, fragment] = serviceId.split('#')
  if (!did?.startsWith('did:') || !fragment) return false
  const endpoint = await serviceEndpoint(ctx, did, fragment)
  if (!endpoint) return false

  const token = await serviceJwt(ctx, did, CHECK_SPACE_ACCESS)
  const params = new URLSearchParams({ space: spaceUri, did: user, permission })
  const body = await fetchJson(
    new URL(`/xrpc/${CHECK_SPACE_ACCESS}?${params}`, endpoint),
    token,
  )
  const allowed = body?.allowed === true
  cacheSet(accessCache, cacheKey, allowed)
  return allowed
}

export async function canViewCommunityPost(
  ctx: AppContext,
  post: CommunityPost,
  viewer: string | null | undefined,
): Promise<boolean> {
  if (process.env.COMMUNITY_POSTS_ENABLED === 'false' || !viewer) return false
  try {
    if (!post.spaceUri) {
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: viewer,
      })
      return isMember
    }
    return await delegatedSpaceCheck(ctx, post.spaceUri, viewer, 'canView')
  } catch {
    return false
  }
}

export const clearTenantGateCaches = () => {
  configCache.clear()
  accessCache.clear()
  managingAppCache.clear()
}
