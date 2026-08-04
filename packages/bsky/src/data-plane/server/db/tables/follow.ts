import { GeneratedAlways } from 'kysely'

export const tableName = 'follow'

export interface Follow {
  uri: string
  cid: string
  creator: string
  subjectDid: string
  via: string | null
  viaCid: string | null
  createdAt: string
  indexedAt: string
  sortAt: GeneratedAlways<string>
  takedownRef: string | null
}

export type PartialDB = { [tableName]: Follow }
