import { Redis } from '../../../redis'

const POST_META_CACHE_TTL = parseInt(process.env.BSKY_POST_META_CACHE_TTL || '300000', 10)
const POST_META_CACHE_PREFIX = 'dp:pm:'

export type CachedPostMeta = {
  violatesThreadGate: boolean
  violatesEmbeddingRules: boolean
  hasThreadGate: boolean
  hasPostGate: boolean
}

export class PostMetaCache {
  constructor(private redis: Redis) {}

  private keyFor(uri: string): string {
    return `${POST_META_CACHE_PREFIX}${uri}`
  }

  async getMany(uris: string[]): Promise<Map<string, CachedPostMeta | null>> {
    if (uris.length === 0) {
      return new Map()
    }
    const keys = uris.map((uri) => this.keyFor(uri))
    const cached = await this.redis.getMulti(keys)
    const result = new Map<string, CachedPostMeta | null>()

    for (const uri of uris) {
      const key = this.keyFor(uri)
      const value = cached[key]
      if (value) {
        try {
          result.set(uri, JSON.parse(value) as CachedPostMeta)
        } catch {
          // Invalid cache entry, treat as miss
        }
      }
    }

    return result
  }

  async setMany(metas: Map<string, CachedPostMeta>) {
    if (metas.size === 0) {
      return
    }
    const entries: Record<string, string> = {}
    for (const [uri, meta] of metas) {
      entries[this.keyFor(uri)] = JSON.stringify(meta)
    }
    await this.redis.setMulti(entries, POST_META_CACHE_TTL)
  }

  async invalidate(uris: string[]) {
    for (const uri of uris) {
      await this.redis.del(this.keyFor(uri))
    }
  }
}
