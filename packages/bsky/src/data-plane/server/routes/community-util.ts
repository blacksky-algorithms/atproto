// pg returns jsonb columns parsed; proto fields are typed `string`.
export const jsonbToProtoString = (v: unknown): string =>
  v == null ? '' : typeof v === 'string' ? v : JSON.stringify(v)

export type CommunityPostRow = Record<string, string | null>

export const communityPostFromRow = (row: CommunityPostRow) => ({
  uri: row.uri ?? '',
  cid: row.cid ?? '',
  rkey: row.rkey ?? '',
  creator: row.creator ?? '',
  text: row.text ?? '',
  facets: jsonbToProtoString(row.facets),
  replyRoot: row.replyRoot ?? '',
  replyRootCid: row.replyRootCid ?? '',
  replyParent: row.replyParent ?? '',
  replyParentCid: row.replyParentCid ?? '',
  embed: jsonbToProtoString(row.embed),
  langs: row.langs ?? '',
  labels: jsonbToProtoString(row.labels),
  tags: row.tags ?? '',
  createdAt: row.createdAt ?? '',
  indexedAt: row.indexedAt ?? '',
  sortAt: row.sortAt ?? '',
})
