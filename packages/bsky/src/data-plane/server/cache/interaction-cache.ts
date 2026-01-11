import { Redis } from '../../../redis'

const INTERACTION_CACHE_TTL = 30_000 // 30 seconds
const INTERACTION_CACHE_PREFIX = 'dp:int:'

export type CachedInteraction = {
  likeCount: number
  replyCount: number
  repostCount: number
  quoteCount: number
  bookmarkCount: number
}

export class InteractionCache {
  constructor(private redis: Redis) {}

  private keyFor(uri: string): string {
    return `${INTERACTION_CACHE_PREFIX}${uri}`
  }

  async getMany(uris: string[]): Promise<Map<string, CachedInteraction | null>> {
    if (uris.length === 0) {
      return new Map()
    }
    const keys = uris.map((uri) => this.keyFor(uri))
    const cached = await this.redis.getMulti(keys)
    const result = new Map<string, CachedInteraction | null>()

    for (const uri of uris) {
      const key = this.keyFor(uri)
      const value = cached[key]
      if (value) {
        try {
          result.set(uri, JSON.parse(value) as CachedInteraction)
        } catch {
          // Invalid cache entry, treat as miss
        }
      }
    }

    return result
  }

  async setMany(interactions: Map<string, CachedInteraction>) {
    if (interactions.size === 0) {
      return
    }
    const entries: Record<string, string> = {}
    for (const [uri, interaction] of interactions) {
      entries[this.keyFor(uri)] = JSON.stringify(interaction)
    }
    await this.redis.setMulti(entries, INTERACTION_CACHE_TTL)
  }

  async invalidate(uris: string[]) {
    for (const uri of uris) {
      await this.redis.del(this.keyFor(uri))
    }
  }
}
