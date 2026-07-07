import { Generated } from 'kysely'

export const tableName = 'actor_badge'

export interface ActorBadge {
  id: Generated<number>
  did: string
  badge: string
  issuedBy: string
  createdAt: Generated<Date>
  revokedAt: Date | null
  revokedBy: string | null
}

export type PartialDB = { [tableName]: ActorBadge }
