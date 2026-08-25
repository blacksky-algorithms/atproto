/**
 * Standard view shapes -> the private `community.blacksky.feed.defs` family.
 *
 * `buildCommunityPostView` and `buildCommunityThread` emit the standard
 * `app.bsky.*` `$type`s because merged standard surfaces (timeline, author
 * feed) consume them, and those surfaces only ever carry `space_uri IS NULL`
 * rows, whose URIs are ordinary at-uris. The private endpoints return the same
 * structures under our own `$type`s, where every URI-bearing field is a plain
 * string — a seven-segment space URI is not an at-uri, so a standard view type
 * cannot lawfully carry one.
 *
 * This is a rename plus a strip, not a rebuild: keeping one builder means the
 * gate, mutes, labels, counts and viewer state cannot drift between the public
 * and private read paths.
 */

const DEFS = 'community.blacksky.feed.defs'

/**
 * Fields the community view builder hardcodes to 0 because it cannot populate
 * them: reposts are deferred (D28) and bookmarks have no community store. The
 * private lexicon does not declare them, so they are dropped rather than
 * reported as a real zero.
 */
const UNPOPULATED = ['repostCount', 'bookmarkCount'] as const

const TYPE_MAP: Record<string, string> = {
  'app.bsky.feed.defs#postView': `${DEFS}#spacePostView`,
  'app.bsky.embed.record#view': `${DEFS}#spaceRecordView`,
  'app.bsky.embed.record#viewRecord': `${DEFS}#spaceViewRecord`,
  'app.bsky.embed.record#viewNotFound': `${DEFS}#spaceViewNotFound`,
  'app.bsky.embed.record#viewBlocked': `${DEFS}#spaceViewBlocked`,
  'app.bsky.embed.recordWithMedia#view': `${DEFS}#spaceRecordWithMediaView`,
  'app.bsky.unspecced.defs#threadItemPost': `${DEFS}#spaceThreadItemPost`,
  'app.bsky.unspecced.defs#threadItemNotFound': `${DEFS}#spaceThreadItemNotFound`,
  'app.bsky.unspecced.defs#threadItemBlocked': `${DEFS}#spaceThreadItemBlocked`,
}

type Json = Record<string, unknown>

const isPlainObject = (value: unknown): value is Json =>
  !!value && typeof value === 'object' && !Array.isArray(value)

/**
 * Rewrite every mapped `$type` in a view tree and drop the unpopulated
 * counters. Media view types (`app.bsky.embed.images#view` and friends) are
 * deliberately absent from the map: they carry blob URLs, never record URIs,
 * so the private lexicon references them directly.
 */
const convert = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(convert)
  if (!isPlainObject(value)) return value
  const out: Json = {}
  for (const [key, entry] of Object.entries(value)) {
    if (entry === undefined) continue
    if (key === '$type' && typeof entry === 'string') {
      out.$type = TYPE_MAP[entry] ?? entry
      continue
    }
    if ((UNPOPULATED as readonly string[]).includes(key)) continue
    out[key] = convert(entry)
  }
  return out
}

/** A `community.blacksky.feed.defs#spacePostView`. */
export const toSpacePostView = (view: unknown): Json => convert(view) as Json

/** A `community.blacksky.feed.defs#spaceFeedViewPost`. */
export const toSpaceFeedViewPost = (item: unknown): Json =>
  convert(item) as Json

/** A `community.blacksky.feed.defs#spaceThreadItem`. */
export const toSpaceThreadItem = (item: unknown): Json => convert(item) as Json

/** The whole `{ thread, hasOtherReplies }` body of a private thread read. */
export const toSpaceThreadBody = (body: unknown): Json => convert(body) as Json
