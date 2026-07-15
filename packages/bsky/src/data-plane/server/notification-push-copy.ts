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

// Which at-uri (if any) supplies the post snippet for each reason.
export function snippetUriForRow(row: NotificationRow): string | undefined {
  switch (row.reason) {
    case 'mention':
    case 'reply':
    case 'quote':
    case 'subscribed-post':
      return row.recordUri
    case 'like':
    case 'repost': {
      if (!row.reasonSubject) return undefined
      // Only posts have text; likes on feed generators etc. get no snippet.
      return subjectCollection(row) === POST_COLLECTION
        ? row.reasonSubject
        : undefined
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
  const title = actorName(ctx.actorsByDid.get(row.author))
  const snippetUri = snippetUriForRow(row)
  const snippet = snippetUri
    ? cleanSnippet(ctx.postTextByUri.get(snippetUri))
    : undefined
  return {
    title,
    message: snippet ? `${action}: ${snippet}` : action,
  }
}

function actionPhrase(row: NotificationRow): string | undefined {
  switch (row.reason) {
    case 'follow':
      return 'followed you'
    case 'like': {
      const collection = subjectCollection(row)
      return collection && collection !== POST_COLLECTION
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
  try {
    return new AtUri(row.reasonSubject).collection
  } catch {
    return undefined
  }
}
