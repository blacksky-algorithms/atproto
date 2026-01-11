import { Redis } from '../../../redis'

const RECORD_CACHE_TTL = 300_000 // 5 minutes
const RECORD_CACHE_PREFIX = 'dp:rec:'

export type CachedRecord = {
  record?: string // base64 encoded
  cid?: string
  createdAt?: { seconds: string; nanos: number }
  indexedAt?: { seconds: string; nanos: number }
  sortedAt?: { seconds: string; nanos: number }
  takenDown: boolean
  takedownRef?: string
  tags?: string[]
}

export class RecordCache {
  constructor(private redis: Redis) {}

  private keyFor(uri: string): string {
    return `${RECORD_CACHE_PREFIX}${uri}`
  }

  async getMany(uris: string[]): Promise<Map<string, CachedRecord | null>> {
    if (uris.length === 0) {
      return new Map()
    }
    const keys = uris.map((uri) => this.keyFor(uri))
    const cached = await this.redis.getMulti(keys)
    const result = new Map<string, CachedRecord | null>()

    for (const uri of uris) {
      const key = this.keyFor(uri)
      const value = cached[key]
      if (value) {
        try {
          result.set(uri, JSON.parse(value) as CachedRecord)
        } catch {
          // Invalid cache entry, treat as miss
        }
      }
    }

    return result
  }

  async setMany(records: Map<string, CachedRecord>) {
    if (records.size === 0) {
      return
    }
    const entries: Record<string, string> = {}
    for (const [uri, record] of records) {
      entries[this.keyFor(uri)] = JSON.stringify(record)
    }
    await this.redis.setMulti(entries, RECORD_CACHE_TTL)
  }

  async invalidate(uris: string[]) {
    for (const uri of uris) {
      await this.redis.del(this.keyFor(uri))
    }
  }
}
