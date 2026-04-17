import { Redis } from '../../../redis'

const RELATIONSHIP_CACHE_TTL = 30_000 // 30 seconds
const RELATIONSHIP_CACHE_PREFIX = 'dp:rel:'

export type CachedRelationship = {
  muted: boolean
  mutedByList: string
  blockedBy: string
  blocking: string
  blockedByList: string
  blockingByList: string
  following: string
  followedBy: string
}

export class RelationshipCache {
  constructor(private redis: Redis) {}

  private keyFor(viewerDid: string, targetDid: string): string {
    return `${RELATIONSHIP_CACHE_PREFIX}${viewerDid}:${targetDid}`
  }

  async getMany(
    viewerDid: string,
    targetDids: string[],
  ): Promise<Map<string, CachedRelationship | null>> {
    if (targetDids.length === 0) {
      return new Map()
    }
    const keys = targetDids.map((did) => this.keyFor(viewerDid, did))
    const cached = await this.redis.getMulti(keys)
    const result = new Map<string, CachedRelationship | null>()

    for (const did of targetDids) {
      const key = this.keyFor(viewerDid, did)
      const value = cached[key]
      if (value) {
        try {
          result.set(did, JSON.parse(value) as CachedRelationship)
        } catch {
          // Invalid cache entry, treat as miss
        }
      }
    }

    return result
  }

  async setMany(
    viewerDid: string,
    relationships: Map<string, CachedRelationship>,
  ) {
    if (relationships.size === 0) {
      return
    }
    const entries: Record<string, string> = {}
    for (const [did, rel] of relationships) {
      entries[this.keyFor(viewerDid, did)] = JSON.stringify(rel)
    }
    await this.redis.setMulti(entries, RELATIONSHIP_CACHE_TTL)
  }
}
