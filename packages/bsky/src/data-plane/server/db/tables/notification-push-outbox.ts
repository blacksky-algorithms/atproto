import { Generated } from 'kysely'

export const tableName = 'notification_push_outbox'

export interface NotificationPushOutbox {
  id: string
  notificationId: number | null
  did: string
  recordUri: string
  recordCid: string
  author: string
  reason: string
  reasonSubject: string | null
  sortAt: string
  courierNotificationId: string
  status: string
  attempts: Generated<number>
  nextAttemptAt: Generated<Date>
  expiresAt: Date
  lastError: string | null
  createdAt: Generated<Date>
  updatedAt: Generated<Date>
}

export type PartialDB = { [tableName]: NotificationPushOutbox }
