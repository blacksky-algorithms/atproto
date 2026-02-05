import { GeneratedAlways } from 'kysely'

export const tableName = 'community_post'

export interface CommunityPost {
  uri: string
  cid: string
  rkey: string
  creator: string
  text: string
  facets: string | null
  replyRoot: string | null
  replyRootCid: string | null
  replyParent: string | null
  replyParentCid: string | null
  embed: string | null
  langs: string | null
  labels: string | null
  tags: string | null
  createdAt: string
  indexedAt: string
  sortAt: GeneratedAlways<string>
}

export type PartialDB = {
  [tableName]: CommunityPost
}
