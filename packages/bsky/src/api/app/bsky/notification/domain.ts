import { isAtUriString } from '@atproto/syntax'
import {
  parseSpaceRecordUri,
  spaceUriOf,
} from '../../../community/blacksky/space-uri.js'

export type NotificationDomain =
  { type: 'public' } | { type: 'space'; spaceUri: string } | { type: 'invalid' }

const uriDomain = (
  uri: string | null | undefined,
):
  | { type: 'public' }
  | { type: 'space'; spaceUri: string }
  | { type: 'invalid' } => {
  if (!uri) return { type: 'invalid' }
  const space = parseSpaceRecordUri(uri)
  if (space) return { type: 'space', spaceUri: spaceUriOf(space) }
  const parts = uri.startsWith('at://') ? uri.slice(5).split('/') : []
  if (parts[1] === 'space') return { type: 'invalid' }
  return isAtUriString(uri) ? { type: 'public' } : { type: 'invalid' }
}

export function classifyNotificationDomain(notification: {
  uri: string
  reasonSubject?: string
}): NotificationDomain {
  const record = uriDomain(notification.uri)
  if (record.type === 'invalid') return record
  if (!notification.reasonSubject) return record

  const subject = uriDomain(notification.reasonSubject)
  if (subject.type === 'invalid' || subject.type !== record.type) {
    return { type: 'invalid' }
  }
  if (
    record.type === 'space' &&
    subject.type === 'space' &&
    record.spaceUri !== subject.spaceUri
  ) {
    return { type: 'invalid' }
  }
  return record
}
