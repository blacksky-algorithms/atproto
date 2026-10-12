export const tableName = 'calendar_rsvp'

export interface CalendarRsvp {
  creator: string
  subject: string
  uri: string
  cid: string
  subjectCid: string | null
  status: string
  indexedAt: string
  sortAt: string
}

export type PartialDB = {
  [tableName]: CalendarRsvp
}
