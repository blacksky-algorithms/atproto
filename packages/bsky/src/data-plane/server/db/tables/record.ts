import { ColumnType } from 'kysely'

export interface Record {
  uri: string
  cid: string
  did: string
  json: string
  rev: ColumnType<string | null, string | null | undefined, string | null>
  indexedAt: string
  takedownRef: string | null
  tags: ColumnType<string[] | null, string | undefined, string> | null
}

export const tableName = 'record'

export type PartialDB = { [tableName]: Record }
