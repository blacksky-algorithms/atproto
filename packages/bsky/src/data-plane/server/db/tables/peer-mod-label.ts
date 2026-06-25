import { Generated } from 'kysely'

export const tableName = 'peer_mod_label'

export interface PeerModLabel {
  id: Generated<number>
  subjectUri: string
  subjectCid: string
  val: string
  peerModDid: string
  ozoneEventId: string
  createdAt: string
  negatedAt: string | null
  negatedBy: string | null
  negationOzoneEventId: string | null
}

export type PartialDB = { [tableName]: PeerModLabel }
