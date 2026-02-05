import { Pool } from 'pg'

interface CacheEntry {
  value: boolean
  expiresAt: number
}

const CACHE_TTL_MS = 5 * 60 * 1000 // 5 minutes
const CACHE_MAX_SIZE = 100_000

export class MembershipChecker {
  private pool: Pool
  private cache: Map<string, CacheEntry> = new Map()

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 3,
    })
  }

  async isMember(did: string): Promise<boolean> {
    const now = Date.now()

    // Check cache first
    const cached = this.cache.get(did)
    if (cached && cached.expiresAt > now) {
      return cached.value
    }

    // Query database
    const res = await this.pool.query(
      `SELECT 1 FROM membership WHERE did = $1 AND included = true`,
      [did],
    )
    const isMember = res.rowCount !== null && res.rowCount > 0

    // Evict oldest if cache is too large
    if (this.cache.size >= CACHE_MAX_SIZE) {
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }

    this.cache.set(did, { value: isMember, expiresAt: now + CACHE_TTL_MS })
    return isMember
  }

  invalidate(did: string): void {
    this.cache.delete(did)
  }

  async close(): Promise<void> {
    await this.pool.end()
  }
}
