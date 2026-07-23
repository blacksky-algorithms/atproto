import { AtUri } from '@atproto/syntax'
import { NotificationRow } from './notification-push-bridge.js'

export type PushActor = { handle: string | null; displayName: string | null }

export type PushCopyContext = {
  actorsByDid: Map<string, PushActor>
  postTextByUri: Map<string, string>
}

export type PushCopy = { title: string; message: string }

export const GENERIC_PUSH_COPY: PushCopy = {
  title: 'Blacksky',
  message: 'You have a new notification',
}

const SNIPPET_MAX_CHARS = 128
const TITLE_MAX_CHARS = 64
const POST_COLLECTION = 'app.bsky.feed.post'
const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'
const FEED_GENERATOR_COLLECTION = 'app.bsky.feed.generator'

// Collections whose records carry post text usable as a snippet. The bridge
// relies on isCommunityPostUri() plus snippetUriForRow()'s invariant (it only
// returns uris from these collections) to partition text hydration between
// the `post` and `community_post` tables — it does not consume this set.
//
// Community-post snippets are deliberately included for every reason
// (like/repost via subject, mention/reply/quote/subscribed-post via record):
// push snippet exposure matches in-app notification hydration
// (hydrator.ts fetchCommunityPostsForNotifs), which already shows the same
// text to the same recipients without a membership gate. If that gating is
// ever tightened, this push path must be tightened with it.
const POST_TEXT_COLLECTIONS: ReadonlySet<string> = new Set([
  POST_COLLECTION,
  COMMUNITY_POST_COLLECTION,
])

// True when the at-uri points at a community-only post, whose text lives in
// the `community_post` table rather than `post`.
export function isCommunityPostUri(uri: string): boolean {
  return uriCollection(uri) === COMMUNITY_POST_COLLECTION
}

// Which at-uri (if any) supplies the post snippet for each reason. Returned
// uris are guaranteed to be in a POST_TEXT_COLLECTIONS collection.
export function snippetUriForRow(row: NotificationRow): string | undefined {
  switch (row.reason) {
    case 'mention':
    case 'reply':
    case 'quote':
    case 'subscribed-post':
      return isTextPostUri(row.recordUri) ? row.recordUri : undefined
    case 'like':
    case 'repost': {
      // Only posts have text; likes on feed generators etc. get no snippet.
      if (!row.reasonSubject) return undefined
      return isTextPostUri(row.reasonSubject) ? row.reasonSubject : undefined
    }
    default:
      return undefined
  }
}

export function composePushCopy(
  row: NotificationRow,
  ctx: PushCopyContext,
): PushCopy {
  const action = actionPhrase(row)
  if (!action) return GENERIC_PUSH_COPY
  const name = actorName(ctx.actorsByDid.get(row.author))
  const snippetUri = snippetUriForRow(row)
  const snippet = snippetUri
    ? cleanSnippet(ctx.postTextByUri.get(snippetUri))
    : undefined
  // Mirror the in-app notification feed (NotificationFeedItem.tsx): the whole
  // "<name> <action>" sentence is the title (name first, matching its
  // a11yLabel), and the subject/record post text is the secondary body line
  // (its AdditionalPostText). Reasons with no post text (follow, bare like,
  // verified, …) produce a title-only push — buildApnsPayload/buildFcmPayload
  // in the courier render a title-only notification as visible.
  return {
    title: `${name} ${action}`,
    message: snippet ?? '',
  }
}

function actionPhrase(row: NotificationRow): string | undefined {
  switch (row.reason) {
    case 'follow':
      return 'followed you'
    case 'like': {
      // Explicit feed-generator case; posts (app.bsky and community-only) and
      // anything else (other collections, malformed, absent) read as a post
      // like — the latter just get no snippet.
      const collection = subjectCollection(row)
      return collection === FEED_GENERATOR_COLLECTION
        ? 'liked your custom feed'
        : 'liked your post'
    }
    case 'like-via-repost':
      return 'liked your repost'
    case 'mention':
      return 'mentioned you'
    case 'quote':
      return 'quoted your post'
    case 'reply':
      return 'replied to your post'
    case 'repost':
      return 'reposted your post'
    case 'repost-via-repost':
      return 'reposted your repost'
    case 'starterpack-joined':
      return 'signed up with your starter pack'
    case 'subscribed-post':
      return 'posted'
    case 'verified':
      return 'verified you'
    case 'unverified':
      return 'removed their verification of you'
    default:
      return undefined
  }
}

function actorName(actor: PushActor | undefined): string {
  const displayName = actor?.displayName && sanitizeLine(actor.displayName)
  if (displayName) return truncate(displayName, TITLE_MAX_CHARS)
  if (actor?.handle) return truncate(`@${actor.handle}`, TITLE_MAX_CHARS)
  return 'Someone'
}

function cleanSnippet(text: string | undefined): string | undefined {
  if (!text) return undefined
  const cleaned = sanitizeLine(text)
  if (!cleaned) return undefined
  return truncate(cleaned, SNIPPET_MAX_CHARS)
}

// Collapse all whitespace runs (incl. newlines) to single spaces, strip
// control characters and bidi override/isolate characters (so a display name
// can't visually reverse the title), and trim. Deliberately not \p{Cf}: that
// would strip ZWJ and break composed emoji in names.
function sanitizeLine(text: string): string {
  return text
    .replace(/[\p{Cc}\u202a-\u202e\u2066-\u2069]/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text
  let end = max
  // Don't split a surrogate pair: back up if the cut lands mid-pair.
  const prev = text.charCodeAt(end - 1)
  if (prev >= 0xd800 && prev <= 0xdbff) end -= 1
  return text.slice(0, end) + '…'
}

// Collection of the reasonSubject at-uri, or undefined if absent/malformed.
function subjectCollection(row: NotificationRow): string | undefined {
  if (!row.reasonSubject) return undefined
  return uriCollection(row.reasonSubject)
}

function isTextPostUri(uri: string): boolean {
  const collection = uriCollection(uri)
  return collection !== undefined && POST_TEXT_COLLECTIONS.has(collection)
}

// Collection of an at-uri, or undefined if malformed.
function uriCollection(uri: string): string | undefined {
  try {
    return new AtUri(uri).collection
  } catch {
    return undefined
  }
}
