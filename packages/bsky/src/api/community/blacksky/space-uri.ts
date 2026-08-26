/**
 * Permissioned-space URIs (proposal 0016 addressing).
 *
 *   space:  at://{spaceDid}/space/{spaceType}/{skey}
 *   record: at://{spaceDid}/space/{spaceType}/{skey}/{authorDid}/{collection}/{rkey}
 *
 * These are NOT valid at-uris. `@atproto/syntax` caps an at-uri at
 * authority/collection/rkey and requires the collection to be an NSID, and
 * `space` has no dots — so both forms fail `format: at-uri` validation and
 * misparse in `AtUri`. Never hand a space URI to `AtUri`; use this instead.
 *
 * Deliberately dependency-free: the appview, the client and both syncers need
 * the same parser, and a copy that imports nothing can be vendored verbatim.
 */

/** The literal segment that marks a URI as addressing permissioned data. */
export const SPACE_MARKER = 'space'

const AT_PREFIX = 'at://'

export type SpaceRef = {
  spaceDid: string
  spaceType: string
  skey: string
}

export type SpaceRecordRef = SpaceRef & {
  authorDid: string
  collection: string
  rkey: string
}

/**
 * True when `uri` addresses permissioned data, in either form.
 *
 * A public at-uri can never collide: its second segment is a collection NSID,
 * which always contains at least one dot, and `space` contains none.
 */
export const isSpaceUri = (uri?: string | null): boolean => {
  if (!uri || !uri.startsWith(AT_PREFIX)) return false
  const parts = uri.slice(AT_PREFIX.length).split('/')
  return (parts.length === 4 || parts.length === 7) && parts[1] === SPACE_MARKER
}

/** True only for the 7-segment record form. */
export const isSpaceRecordUri = (uri?: string | null): boolean =>
  parseSpaceRecordUri(uri) !== null

const looksLikeDid = (value: string): boolean =>
  /^did:[a-z]+:[A-Za-z0-9._:%-]*[A-Za-z0-9._-]$/.test(value)

const isNonEmpty = (value: string): boolean => value.length > 0

/** Parse the 4-segment space form. Returns null for anything else. */
export const parseSpaceUri = (uri?: string | null): SpaceRef | null => {
  if (!uri || !uri.startsWith(AT_PREFIX)) return null
  const parts = uri.slice(AT_PREFIX.length).split('/')
  if (parts.length !== 4 || parts[1] !== SPACE_MARKER) return null
  const [spaceDid, , spaceType, skey] = parts
  if (!looksLikeDid(spaceDid) || !isNonEmpty(spaceType) || !isNonEmpty(skey)) {
    return null
  }
  return { spaceDid, spaceType, skey }
}

/**
 * Parse the 7-segment record form. Returns null for anything else — including
 * the 4-segment space form, a public at-uri, and near-misses at 6 or 8
 * segments.
 */
export const parseSpaceRecordUri = (
  uri?: string | null,
): SpaceRecordRef | null => {
  if (!uri || !uri.startsWith(AT_PREFIX)) return null
  const parts = uri.slice(AT_PREFIX.length).split('/')
  if (parts.length !== 7 || parts[1] !== SPACE_MARKER) return null
  const [spaceDid, , spaceType, skey, authorDid, collection, rkey] = parts
  if (
    !looksLikeDid(spaceDid) ||
    !looksLikeDid(authorDid) ||
    !isNonEmpty(spaceType) ||
    !isNonEmpty(skey) ||
    !isNonEmpty(collection) ||
    !isNonEmpty(rkey)
  ) {
    return null
  }
  return { spaceDid, spaceType, skey, authorDid, collection, rkey }
}

/** The space a record belongs to, as a URI. */
export const spaceUriOf = (ref: SpaceRef): string =>
  `${AT_PREFIX}${ref.spaceDid}/${SPACE_MARKER}/${ref.spaceType}/${ref.skey}`

/** Build a record URI inside a space. */
export const spaceRecordUri = (ref: SpaceRecordRef): string =>
  `${spaceUriOf(ref)}/${ref.authorDid}/${ref.collection}/${ref.rkey}`

/**
 * The author of a space record. Authorship comes from the URI's own
 * `authorDid` segment, never from a repo the URI was fetched from.
 */
export const spaceRecordAuthor = (uri?: string | null): string | null =>
  parseSpaceRecordUri(uri)?.authorDid ?? null

/** The collection of a space record. */
export const spaceRecordCollection = (uri?: string | null): string | null =>
  parseSpaceRecordUri(uri)?.collection ?? null

/** The space a record URI belongs to, or null if it is not one. */
export const spaceOfRecordUri = (uri?: string | null): string | null => {
  const parsed = parseSpaceRecordUri(uri)
  return parsed ? spaceUriOf(parsed) : null
}
