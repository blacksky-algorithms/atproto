import { Redis } from '../../../redis'

const ACTOR_CACHE_TTL = 60_000 // 60 seconds
const ACTOR_CACHE_PREFIX = 'dp:actor:'

export type CachedActor = {
  exists: boolean
  handle?: string
  profile?: {
    uri: string
    cid: string
    record?: Uint8Array
    takedownRef?: string
    createdAt?: { seconds: bigint; nanos: number }
  }
  takenDown: boolean
  takedownRef?: string
  tombstonedAt?: { seconds: bigint; nanos: number }
  labeler: boolean
  allowIncomingChatsFrom?: string
  upstreamStatus: string
  createdAt?: { seconds: bigint; nanos: number }
  priorityNotifications: boolean
  trustedVerifier: boolean
  verifiedBy: Record<
    string,
    {
      rkey?: string
      handle?: string
      displayName?: string
      sortedAt?: { seconds: bigint; nanos: number }
    }
  >
  statusRecord?: {
    uri: string
    cid: string
    record?: Uint8Array
    takedownRef?: string
    createdAt?: { seconds: bigint; nanos: number }
  }
  tags: string[]
  profileTags: string[]
  allowActivitySubscriptionsFrom: string
  ageAssuranceStatus?: {
    lastInitiatedAt?: { seconds: bigint; nanos: number }
    status: string
    access: string
  }
}

export class ActorCache {
  constructor(private redis: Redis) {}

  private keyFor(did: string): string {
    return `${ACTOR_CACHE_PREFIX}${did}`
  }

  async getMany(dids: string[]): Promise<Map<string, CachedActor | null>> {
    if (dids.length === 0) {
      return new Map()
    }
    const keys = dids.map((did) => this.keyFor(did))
    const cached = await this.redis.getMulti(keys)
    const result = new Map<string, CachedActor | null>()

    for (const did of dids) {
      const key = this.keyFor(did)
      const value = cached[key]
      if (value) {
        try {
          result.set(did, JSON.parse(value) as CachedActor)
        } catch {
          // Invalid cache entry, treat as miss
        }
      }
    }

    return result
  }

  async setMany(actors: Map<string, CachedActor>) {
    if (actors.size === 0) {
      return
    }
    const entries: Record<string, string> = {}
    for (const [did, actor] of actors) {
      entries[this.keyFor(did)] = JSON.stringify(actor)
    }
    await this.redis.setMulti(entries, ACTOR_CACHE_TTL)
  }

  async invalidate(dids: string[]) {
    for (const did of dids) {
      await this.redis.del(this.keyFor(did))
    }
  }
}
