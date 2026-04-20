import { Timestamp } from '@bufbuild/protobuf'
import { Redis } from '../../../redis'

const ACTOR_CACHE_TTL = parseInt(process.env.BSKY_ACTOR_CACHE_TTL || '60000', 10)
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

// Serializable version of CachedActor where Timestamps are ISO strings
// and Uint8Arrays are base64 strings, safe for JSON round-trip
type SerializedTimestamp = string // ISO 8601
type SerializedBytes = string // base64

interface SerializedActor {
  [key: string]: unknown
  _serialized: true
}

function tsToIso(
  ts: { seconds: bigint; nanos: number } | undefined,
): SerializedTimestamp | undefined {
  if (!ts) return undefined
  const ms = Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000)
  return new Date(ms).toISOString()
}

function isoToTs(
  iso: string | undefined,
): { seconds: bigint; nanos: number } | undefined {
  if (!iso) return undefined
  return Timestamp.fromDate(new Date(iso))
}

function bytesToBase64(arr: Uint8Array | undefined): SerializedBytes | undefined {
  if (!arr) return undefined
  return Buffer.from(arr).toString('base64')
}

function base64ToBytes(b64: string | undefined): Uint8Array | undefined {
  if (!b64) return undefined
  return new Uint8Array(Buffer.from(b64, 'base64'))
}

function serializeForCache(actor: CachedActor): SerializedActor {
  const serialized: Record<string, unknown> = { ...actor, _serialized: true }

  // Top-level timestamps
  serialized.createdAt = tsToIso(actor.createdAt)
  serialized.tombstonedAt = tsToIso(actor.tombstonedAt)

  // Profile
  if (actor.profile) {
    serialized.profile = {
      ...actor.profile,
      record: bytesToBase64(actor.profile.record),
      createdAt: tsToIso(actor.profile.createdAt),
    }
  }

  // Status record
  if (actor.statusRecord) {
    serialized.statusRecord = {
      ...actor.statusRecord,
      record: bytesToBase64(actor.statusRecord.record),
      createdAt: tsToIso(actor.statusRecord.createdAt),
    }
  }

  // Verified by
  if (actor.verifiedBy) {
    const vb: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(actor.verifiedBy)) {
      vb[key] = { ...val, sortedAt: tsToIso(val.sortedAt) }
    }
    serialized.verifiedBy = vb
  }

  // Age assurance
  if (actor.ageAssuranceStatus) {
    serialized.ageAssuranceStatus = {
      ...actor.ageAssuranceStatus,
      lastInitiatedAt: tsToIso(actor.ageAssuranceStatus.lastInitiatedAt),
    }
  }

  return serialized as SerializedActor
}

function deserializeFromCache(data: Record<string, unknown>): CachedActor {
  const actor = { ...data } as Record<string, unknown>
  delete actor._serialized

  // Top-level timestamps
  actor.createdAt = isoToTs(actor.createdAt as string | undefined)
  actor.tombstonedAt = isoToTs(actor.tombstonedAt as string | undefined)

  // Profile
  const profile = actor.profile as Record<string, unknown> | undefined
  if (profile) {
    profile.record = base64ToBytes(profile.record as string | undefined)
    profile.createdAt = isoToTs(profile.createdAt as string | undefined)
  }

  // Status record
  const statusRecord = actor.statusRecord as Record<string, unknown> | undefined
  if (statusRecord) {
    statusRecord.record = base64ToBytes(statusRecord.record as string | undefined)
    statusRecord.createdAt = isoToTs(statusRecord.createdAt as string | undefined)
  }

  // Verified by
  const verifiedBy = actor.verifiedBy as Record<string, Record<string, unknown>> | undefined
  if (verifiedBy) {
    for (const val of Object.values(verifiedBy)) {
      val.sortedAt = isoToTs(val.sortedAt as string | undefined)
    }
  }

  // Age assurance
  const ageAssurance = actor.ageAssuranceStatus as Record<string, unknown> | undefined
  if (ageAssurance) {
    ageAssurance.lastInitiatedAt = isoToTs(ageAssurance.lastInitiatedAt as string | undefined)
  }

  return actor as unknown as CachedActor
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
          const parsed = JSON.parse(value)
          // Reconstruct Timestamp/Uint8Array objects from serialized form
          if (parsed._serialized) {
            result.set(did, deserializeFromCache(parsed))
          } else {
            // Legacy cache entry without serialization -- treat as miss
            // so it gets re-fetched and re-cached in the new format
          }
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
      entries[this.keyFor(did)] = JSON.stringify(serializeForCache(actor))
    }
    await this.redis.setMulti(entries, ACTOR_CACHE_TTL)
  }

  async invalidate(dids: string[]) {
    for (const did of dids) {
      await this.redis.del(this.keyFor(did))
    }
  }
}
