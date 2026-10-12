export const tableName = 'calendar_event'

export interface CalendarEvent {
  uri: string
  cid: string
  creator: string
  startsAt: string | null
  status: string | null
  indexedAt: string
}

export type PartialDB = {
  [tableName]: CalendarEvent
}
