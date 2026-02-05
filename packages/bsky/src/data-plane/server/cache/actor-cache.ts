import * as ui8 from 'uint8arrays'
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
          const parsed = JSON.parse(value) as CachedActor
          // Convert base64 strings back to Uint8Array for record fields
          // JSON.stringify converts Uint8Array to base64 string, so we need to reverse this
          if (parsed.profile?.record && typeof parsed.profile.record === 'string') {
            parsed.profile.record = ui8.fromString(parsed.profile.record, 'base64')
          }
          if (parsed.statusRecord?.record && typeof parsed.statusRecord.record === 'string') {
            parsed.statusRecord.record = ui8.fromString(parsed.statusRecord.record, 'base64')
          }
          result.set(did, parsed)
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
      // Create a copy to avoid mutating the original
      const toCache = { ...actor }
      // Convert Uint8Array to base64 string for JSON serialization
      if (toCache.profile?.record instanceof Uint8Array) {
        toCache.profile = {
          ...toCache.profile,
          record: ui8.toString(toCache.profile.record, 'base64') as unknown as Uint8Array,
        }
      }
      if (toCache.statusRecord?.record instanceof Uint8Array) {
        toCache.statusRecord = {
          ...toCache.statusRecord,
          record: ui8.toString(toCache.statusRecord.record, 'base64') as unknown as Uint8Array,
        }
      }
      entries[this.keyFor(did)] = JSON.stringify(toCache)
    }
    await this.redis.setMulti(entries, ACTOR_CACHE_TTL)
  }

  async invalidate(dids: string[]) {
    for (const did of dids) {
      await this.redis.del(this.keyFor(did))
    }
  }
}
