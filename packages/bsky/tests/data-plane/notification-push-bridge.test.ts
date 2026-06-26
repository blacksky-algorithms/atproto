import { lexStringify } from '@atproto/lex'
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest'
import { Namespaces } from '../../src/stash.js'
import { Database } from '../../src/data-plane/server/db/index.js'
import {
  getCourierNotificationId,
  NotificationPushBridge,
  NotificationPushBridgeConfig,
  NotificationRow,
  shouldPushForReason,
  toCourierNotification,
} from '../../src/data-plane/server/notification-push-bridge.js'

describe('notification push bridge', () => {
  let db: Database

  beforeAll(async () => {
    const url = process.env.DB_TEST_POSTGRES_URL || process.env.DB_POSTGRES_URL
    if (!url) {
      throw new Error('Missing DB_TEST_POSTGRES_URL or DB_POSTGRES_URL')
    }
    db = new Database({
      url,
      schema: 'bsky_notif_push_bridge',
    })
    await db.migrateToLatestOrThrow()
  })

  afterAll(async () => {
    await db.close()
  })

  afterEach(async () => {
    await db.db.deleteFrom('notification_push_outbox').execute()
    await db.db.deleteFrom('notification').execute()
    await db.db.deleteFrom('private_data').execute()
  })

  it('maps notification rows to deterministic courier payloads', () => {
    const row = notificationRow()
    const notif = toCourierNotification(row)

    expect(notif.id).toBe(getCourierNotificationId(row))
    expect(notif.recipientDid).toBe(row.did)
    expect(notif.title).toBe('Blacksky')
    expect(notif.message).toBe('You have a new notification')
    expect(notif.collapseKey).toBe(row.reason)
    expect(notif.additional.toJson()).toEqual({
      reason: row.reason,
      uri: row.recordUri,
      cid: row.recordCid,
      subject: row.reasonSubject,
      recipientDid: row.did,
      actorDid: row.author,
    })
  })

  it('applies reason-level push preferences', () => {
    expect(shouldPushForReason(undefined, 'like')).toBe(true)
    expect(shouldPushForReason({ like: { include: 'all', list: true, push: false } }, 'like')).toBe(false)
    expect(shouldPushForReason({ reply: { include: 'all', list: true, push: true } }, 'reply')).toBe(true)
    expect(shouldPushForReason(undefined, 'badge-granted')).toBe(false)
  })

  it('sends eligible notifications to courier without writing outbox on success', async () => {
    const row = await insertNotification()
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row.id])

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    const req = pushNotifications.mock.calls[0][0]
    expect(req.notifications).toHaveLength(1)
    expect(req.notifications[0].id).toBe(getCourierNotificationId(row))
    expect(req.notifications[0].recipientDid).toBe(row.did)

    await expect(outboxRows()).resolves.toHaveLength(0)
  })

  it('writes outbox rows when courier handoff fails', async () => {
    const row = await insertNotification()
    const pushNotifications = vi
      .fn()
      .mockRejectedValue(new Error('courier down'))
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row.id])

    const rows = await outboxRows()
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      notificationId: row.id,
      did: row.did,
      recordUri: row.recordUri,
      courierNotificationId: getCourierNotificationId(row),
      status: 'pending',
      attempts: 0,
      lastError: 'courier down',
    })
    expect(rows[0].expiresAt.getTime()).toBeGreaterThan(Date.now())
  })

  it('does not call courier or write outbox when push preference is disabled', async () => {
    await insertPreferences('did:plc:recipient', {
      like: { include: 'all', list: true, push: false },
    })
    const row = await insertNotification()
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row.id])

    expect(pushNotifications).not.toHaveBeenCalled()
    await expect(outboxRows()).resolves.toHaveLength(0)
  })

  it('treats malformed stored preferences as default preferences', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    await insertRawPreferences('did:plc:recipient', '{not-json')
    const row = await insertNotification()
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    try {
      await bridge.flushOnceForTest([row.id])
    } finally {
      consoleError.mockRestore()
    }

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    const req = pushNotifications.mock.calls[0][0]
    expect(req.notifications[0].id).toBe(getCourierNotificationId(row))
  })

  it('keeps notification ids buffered after pre-courier failures', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    const row = await insertNotification()
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)
    const filterRowsByPushPreferences = vi
      .fn()
      .mockRejectedValueOnce(new Error('pre-courier db failure'))
      .mockResolvedValueOnce([row])
    ;(bridge as any).filterRowsByPushPreferences = filterRowsByPushPreferences

    try {
      await bridge.flushOnceForTest([row.id])
      await bridge.stop()
    } finally {
      consoleError.mockRestore()
    }

    expect(pushNotifications).not.toHaveBeenCalled()
    await expect(outboxRows()).resolves.toHaveLength(0)

    await bridge.flushOnceForTest()

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    expect(filterRowsByPushPreferences).toHaveBeenCalledTimes(2)
  })

  it('does not call courier or write outbox for unknown reasons', async () => {
    const row = await insertNotification({ reason: 'badge-granted' })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row.id])

    expect(pushNotifications).not.toHaveBeenCalled()
    await expect(outboxRows()).resolves.toHaveLength(0)
  })

  it('marks retry rows sent after successful courier handoff', async () => {
    const row = await insertNotification()
    await insertOutbox(row)
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    const rows = await outboxRows()
    expect(rows[0].status).toBe('sent')
  })

  it('backs off retry rows after failed courier handoff', async () => {
    const row = await insertNotification()
    await insertOutbox(row)
    const before = await outboxRows()
    const pushNotifications = vi
      .fn()
      .mockRejectedValue(new Error('still down'))
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    const rows = await outboxRows()
    expect(rows[0].status).toBe('retryable')
    expect(rows[0].attempts).toBe(1)
    expect(rows[0].lastError).toBe('still down')
    expect(rows[0].nextAttemptAt.getTime()).toBeGreaterThanOrEqual(
      before[0].nextAttemptAt.getTime(),
    )
  })

  it('expires stale retry rows without calling courier', async () => {
    const row = await insertNotification()
    await insertOutbox(row, { expiresAt: new Date(Date.now() - 1000) })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    expect(pushNotifications).not.toHaveBeenCalled()
    const rows = await outboxRows()
    expect(rows[0].status).toBe('expired')
  })

  it('retries stale processing rows', async () => {
    const row = await insertNotification()
    await insertOutbox(row, {
      status: 'processing',
      updatedAt: new Date(Date.now() - 10 * 60 * 1000),
    })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    const rows = await outboxRows()
    expect(rows[0].status).toBe('sent')
  })

  it('does not retry fresh processing rows', async () => {
    const row = await insertNotification()
    await insertOutbox(row, {
      status: 'processing',
      updatedAt: new Date(),
    })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    expect(pushNotifications).not.toHaveBeenCalled()
    const rows = await outboxRows()
    expect(rows[0].status).toBe('processing')
  })

  it('expires expired processing rows without calling courier', async () => {
    const row = await insertNotification()
    await insertOutbox(row, {
      status: 'processing',
      expiresAt: new Date(Date.now() - 1000),
      updatedAt: new Date(Date.now() - 10 * 60 * 1000),
    })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.processRetryBatchOnceForTest()

    expect(pushNotifications).not.toHaveBeenCalled()
    const rows = await outboxRows()
    expect(rows[0].status).toBe('expired')
  })

  it('batches notification flushes into one courier call', async () => {
    const row1 = await insertNotification({ recordUri: 'at://did:plc:actor/app.bsky.feed.like/1' })
    const row2 = await insertNotification({ recordUri: 'at://did:plc:actor/app.bsky.feed.like/2' })
    const pushNotifications = vi.fn().mockResolvedValue({})
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row1.id, row2.id])

    expect(pushNotifications).toHaveBeenCalledTimes(1)
    const req = pushNotifications.mock.calls[0][0]
    expect(req.notifications).toHaveLength(2)
  })

  it('upserts outbox rows idempotently after repeated handoff failures', async () => {
    const row = await insertNotification()
    const pushNotifications = vi
      .fn()
      .mockRejectedValueOnce(new Error('first failure'))
      .mockRejectedValueOnce(new Error('second failure'))
    const bridge = createBridge(pushNotifications)

    await bridge.flushOnceForTest([row.id])
    await bridge.flushOnceForTest([row.id])

    const rows = await outboxRows()
    expect(rows).toHaveLength(1)
    expect(rows[0].lastError).toBe('second failure')
    expect(rows[0].courierNotificationId).toBe(getCourierNotificationId(row))
  })

  function createBridge(pushNotifications: ReturnType<typeof vi.fn>) {
    return new NotificationPushBridge(db, testConfig(), {
      courierClient: { pushNotifications } as any,
    })
  }

  async function insertNotification(
    overrides: Partial<NotificationRow> = {},
  ): Promise<NotificationRow> {
    return db.db
      .insertInto('notification')
      .values({
        did: 'did:plc:recipient',
        author: 'did:plc:actor',
        recordUri: 'at://did:plc:actor/app.bsky.feed.like/abc',
        recordCid: 'bafyrecordcid',
        reason: 'like',
        reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/root',
        sortAt: new Date().toISOString(),
        ...overrides,
      })
      .returningAll()
      .executeTakeFirstOrThrow()
  }

  async function insertPreferences(
    did: string,
    prefs: Record<string, unknown>,
  ) {
    await db.db
      .insertInto('private_data')
      .values({
        actorDid: did,
        namespace: Namespaces.AppBskyNotificationDefsPreferences.$type,
        key: 'self',
        payload: lexStringify({
          $type: Namespaces.AppBskyNotificationDefsPreferences.$type,
          chat: { include: 'all', push: true },
          follow: { include: 'all', list: true, push: true },
          like: { include: 'all', list: true, push: true },
          likeViaRepost: { include: 'all', list: true, push: true },
          mention: { include: 'all', list: true, push: true },
          quote: { include: 'all', list: true, push: true },
          reply: { include: 'all', list: true, push: true },
          repost: { include: 'all', list: true, push: true },
          repostViaRepost: { include: 'all', list: true, push: true },
          starterpackJoined: { list: true, push: true },
          subscribedPost: { list: true, push: true },
          unverified: { list: true, push: true },
          verified: { list: true, push: true },
          ...prefs,
        }),
        indexedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })
      .execute()
  }

  async function insertRawPreferences(did: string, payload: string) {
    await db.db
      .insertInto('private_data')
      .values({
        actorDid: did,
        namespace: Namespaces.AppBskyNotificationDefsPreferences.$type,
        key: 'self',
        payload,
        indexedAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      })
      .execute()
  }

  async function insertOutbox(
    row: NotificationRow,
    overrides: {
      expiresAt?: Date
      status?: string
      updatedAt?: Date
    } = {},
  ) {
    await db.db
      .insertInto('notification_push_outbox')
      .values({
        id: getCourierNotificationId(row),
        notificationId: row.id,
        did: row.did,
        recordUri: row.recordUri,
        recordCid: row.recordCid,
        author: row.author,
        reason: row.reason,
        reasonSubject: row.reasonSubject,
        sortAt: row.sortAt,
        courierNotificationId: getCourierNotificationId(row),
        status: overrides.status ?? 'pending',
        attempts: 0,
        nextAttemptAt: new Date(Date.now() - 1000),
        expiresAt: overrides.expiresAt ?? new Date(Date.now() + 60_000),
        updatedAt: overrides.updatedAt,
      })
      .execute()
  }

  function outboxRows() {
    return db.db
      .selectFrom('notification_push_outbox')
      .selectAll()
      .orderBy('createdAt')
      .execute()
  }
})

function notificationRow(): NotificationRow {
  return {
    id: 1,
    did: 'did:plc:recipient',
    author: 'did:plc:actor',
    recordUri: 'at://did:plc:actor/app.bsky.feed.like/abc',
    recordCid: 'bafyrecordcid',
    reason: 'like',
    reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/root',
    sortAt: new Date().toISOString(),
  }
}

function testConfig(): NotificationPushBridgeConfig {
  return {
    enabled: true,
    courierUrl: 'http://courier.test',
    courierHttpVersion: '2',
    courierIgnoreBadTls: false,
    batchSize: 100,
    batchWindowMs: 0,
    courierTimeoutMs: 100,
    retryIntervalMs: 100,
    maxAttempts: 10,
    ttlHours: 24,
  }
}
