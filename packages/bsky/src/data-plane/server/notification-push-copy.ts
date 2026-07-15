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
      return isPostUri(row.reasonSubject) ? row.reasonSubject : undefined
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
    case 'like':
      return row.reasonSubject && !isPostUri(row.reasonSubject)
        ? 'liked your custom feed'
        : 'liked your post'
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
// control characters, and trim.
function sanitizeLine(text: string): string {
  return text
    .replace(/\p{Cc}/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim()
}

function truncate(text: string, max: number): string {
  if (text.length <= max) return text
  return text.slice(0, max) + '…'
}

function isPostUri(uri: string): boolean {
  try {
    return new AtUri(uri).collection === 'app.bsky.feed.post'
  } catch {
    return false
  }
}
