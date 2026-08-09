import { createServiceJwt } from '@atproto/xrpc-server'
import { AppContext } from '../../../context.js'
import { community } from '../../../lexicons/index.js'

const CHECK_USER_ACCESS = 'community.blacksky.feed.checkUserAccess'
const CACHE_TTL_MS = 60_000
const CACHE_MAX_SIZE = 100_000
const REQUEST_TIMEOUT_MS = 5_000

type CommunityPost = {
  uri: string
  feedUri?: string
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
  contentStore: string
  authorization: community.blacksky.feed.config.Authorization
}

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
) => {
  if (cache.size >= CACHE_MAX_SIZE) {
    const first = cache.keys().next().value
    if (first !== undefined) cache.delete(first)
  }
  cache.set(key, { value, expiresAt: Date.now() + CACHE_TTL_MS })
}

const parseConfig = (raw: string): CommunityFeedConfig | null => {
  try {
    const parsed = community.blacksky.feed.config.$safeParse(JSON.parse(raw))
    if (
      !parsed.success ||
      parsed.value.contentType !== 'communityRecord' ||
      parsed.value.visibility !== 'gated' ||
      !parsed.value.contentStore ||
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

const authorityEndpoint = async (ctx: AppContext, did: string) => {
  const doc = await ctx.idResolver.did.resolve(did)
  const services = doc?.service ?? []
  const service = services.find((candidate) => {
    const id = candidate.id.split('#').at(1)
    return (
      id === 'bsky_fg' ||
      id === 'atproto_pds' ||
      candidate.type === 'BskyFeedGenerator' ||
      candidate.type === 'AtprotoPersonalDataServer'
    )
  })
  if (!service || typeof service.serviceEndpoint !== 'string') return null
  const endpoint = new URL(service.serviceEndpoint)
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

const delegatedCheck = async (
  ctx: AppContext,
  feedUri: string,
  user: string,
  permission: FeedPermission,
  postUri?: string,
) => {
  const cacheKey = `${feedUri}\u0000${user}\u0000${permission}\u0000${postUri ?? ''}`
  const cached = cacheGet(accessCache, cacheKey)
  if (cached !== undefined) return cached

  const config = await getCommunityFeedConfig(ctx, feedUri)
  const method = config?.authorization.method ?? CHECK_USER_ACCESS
  if (
    !config ||
    config.contentStore !== ctx.cfg.serverDid ||
    method !== CHECK_USER_ACCESS
  ) {
    return false
  }
  const endpoint = await authorityEndpoint(ctx, config.authorization.serviceDid)
  if (!endpoint) return false

  const token = await createServiceJwt({
    iss: ctx.cfg.serverDid,
    aud: config.authorization.serviceDid,
    lxm: method,
    keypair: ctx.signingKey,
  })
  const params = new URLSearchParams({
    feed: feedUri,
    user,
    permission,
  })
  if (postUri) params.set('post', postUri)
  const url = new URL(`/xrpc/${method}?${params}`, endpoint)
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  try {
    const response = await fetch(url, {
      headers: { authorization: `Bearer ${token}` },
      signal: controller.signal,
    })
    if (!response.ok) return false
    const body = (await response.json()) as { allowed?: unknown }
    const allowed = body.allowed === true
    cacheSet(accessCache, cacheKey, allowed)
    return allowed
  } finally {
    clearTimeout(timeout)
  }
}

export async function checkCommunityFeedPermission(
  ctx: AppContext,
  feedUri: string,
  user: string,
  permission: FeedPermission,
): Promise<boolean> {
  if (process.env.COMMUNITY_POSTS_ENABLED === 'false') return false
  try {
    return await delegatedCheck(ctx, feedUri, user, permission)
  } catch {
    return false
  }
}

export async function canViewCommunityPost(
  ctx: AppContext,
  post: CommunityPost,
  viewer: string | null | undefined,
): Promise<boolean> {
  if (process.env.COMMUNITY_POSTS_ENABLED === 'false' || !viewer) return false
  try {
    if (!post.feedUri) {
      const { isMember } = await ctx.dataplane.checkCommunityMembership({
        did: viewer,
      })
      return isMember
    }
    return await delegatedCheck(ctx, post.feedUri, viewer, 'canView', post.uri)
  } catch {
    return false
  }
}

export const clearTenantGateCaches = () => {
  configCache.clear()
  accessCache.clear()
}
