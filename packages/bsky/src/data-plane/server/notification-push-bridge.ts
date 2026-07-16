import { createHash } from 'node:crypto'
import { Struct, Timestamp } from '@bufbuild/protobuf'
import { sql } from 'kysely'
import pg from 'pg'
import { lexParse } from '@atproto/lex'
import {
  CourierClient,
  authWithApiKey as courierAuth,
  createCourierClient,
} from '../../courier.js'
import { app } from '../../lexicons/index.js'
import { Namespaces } from '../../stash.js'
import { Database } from './db/index.js'
import {
  GENERIC_PUSH_COPY,
  PushCopy,
  PushCopyContext,
  composePushCopy,
  isCommunityPostUri,
  snippetUriForRow,
} from './notification-push-copy.js'

const CHANNEL = 'notification_push_inserted'
const DEFAULT_ENABLED_REASONS = new Set([
  'follow',
  'like',
  'like-via-repost',
  'mention',
  'quote',
  'reply',
  'repost',
  'repost-via-repost',
  'starterpack-joined',
  'subscribed-post',
  'unverified',
  'verified',
])

export type NotificationPushBridgeConfig = {
  enabled: boolean
  courierUrl?: string
  courierApiKey?: string
  courierHttpVersion: '1.1' | '2'
  courierIgnoreBadTls: boolean
  batchSize: number
  batchWindowMs: number
  courierTimeoutMs: number
  retryIntervalMs: number
  maxAttempts: number
  ttlHours: number
}

export type NotificationRow = {
  id: number
  did: string
  recordUri: string
  recordCid: string
  author: string
  reason: string
  reasonSubject: string | null
  sortAt: string
}

type OutboxRow = NotificationRow & {
  courierNotificationId: string
}

type ClaimedOutboxRow = {
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
  attempts: number
}

type NotificationPushBridgeDeps = {
  courierClient?: CourierClient
}

export class NotificationPushBridge {
  private courierClient: CourierClient
  private listener?: pg.Client
  private stopped = true
  private bufferedIds = new Set<number>()
  private flushTimer?: NodeJS.Timeout
  private retryTimer?: NodeJS.Timeout
  private activeFlush?: Promise<void>
  private activeRetry?: Promise<void>
  private processingTimeoutMs = 5 * 60 * 1000

  constructor(
    private db: Database,
    private cfg: NotificationPushBridgeConfig,
    deps: NotificationPushBridgeDeps = {},
  ) {
    if (deps.courierClient) {
      this.courierClient = deps.courierClient
      return
    }
    if (!cfg.courierUrl) {
      throw new Error('notification push bridge requires BSKY_COURIER_URL')
    }
    this.courierClient = createCourierClient({
      baseUrl: cfg.courierUrl,
      httpVersion: cfg.courierHttpVersion,
      nodeOptions: { rejectUnauthorized: !cfg.courierIgnoreBadTls },
      interceptors: cfg.courierApiKey ? [courierAuth(cfg.courierApiKey)] : [],
    })
  }

  enqueueNotificationIdForTest(id: number) {
    this.bufferedIds.add(id)
  }

  async flushOnceForTest(ids: number[] = []) {
    for (const id of ids) {
      this.bufferedIds.add(id)
    }
    const wasStopped = this.stopped
    this.stopped = false
    try {
      await this.flush()
    } finally {
      this.stopped = wasStopped
    }
  }

  async processRetryBatchOnceForTest() {
    const wasStopped = this.stopped
    this.stopped = false
    try {
      await this.processRetryBatch()
    } finally {
      this.stopped = wasStopped
    }
  }

  async start() {
    if (!this.stopped) return
    this.stopped = false
    await this.startListener()
    this.scheduleRetry(0)
  }

  async stop() {
    this.stopped = true
    if (this.flushTimer) clearTimeout(this.flushTimer)
    if (this.retryTimer) clearTimeout(this.retryTimer)
    await Promise.allSettled([this.activeFlush, this.activeRetry])
    if (this.listener) {
      await this.listener.end().catch(() => undefined)
      this.listener = undefined
    }
  }

  private async startListener() {
    const listener = new pg.Client({ connectionString: this.db.opts.url })
    await listener.connect()
    if (this.db.schema) {
      await listener.query(`set search_path to "${this.db.schema}", public`)
    }
    listener.on('notification', (msg) => this.onNotification(msg.payload))
    listener.on('error', (err) => {
      console.error('[notification-push-bridge] listener error', err)
      if (!this.stopped) {
        this.restartListener()
      }
    })
    await listener.query(`listen ${CHANNEL}`)
    this.listener = listener
  }

  private restartListener() {
    this.listener?.end().catch(() => undefined)
    this.listener = undefined
    setTimeout(() => {
      if (!this.stopped) {
        this.startListener().catch((err) => {
          console.error(
            '[notification-push-bridge] listener restart failed',
            err,
          )
          this.restartListener()
        })
      }
    }, 5000)
  }

  private onNotification(payload: string | undefined) {
    if (!payload) return
    try {
      const parsed = JSON.parse(payload)
      const id = Number(parsed.id)
      if (!Number.isSafeInteger(id)) return
      this.bufferedIds.add(id)
      if (this.bufferedIds.size >= this.cfg.batchSize) {
        this.scheduleFlush(0)
      } else {
        this.scheduleFlush(this.cfg.batchWindowMs)
      }
    } catch (err) {
      console.error('[notification-push-bridge] invalid notify payload', err)
    }
  }

  private scheduleFlush(delayMs: number) {
    if (this.flushTimer) return
    this.flushTimer = setTimeout(() => {
      this.flushTimer = undefined
      this.activeFlush = this.flush()
    }, delayMs)
  }

  private async flush() {
    if (this.stopped || this.bufferedIds.size === 0) return
    const ids = this.takeBufferedIds()
    try {
      const rows = await this.getNotificationRows(ids)
      await this.sendRows(rows)
    } catch (err) {
      this.rebufferIds(ids)
      console.error('[notification-push-bridge] flush failed', err)
    } finally {
      if (!this.stopped && this.bufferedIds.size > 0) {
        this.scheduleFlush(this.cfg.batchWindowMs)
      }
    }
  }

  private takeBufferedIds() {
    const ids: number[] = []
    for (const id of this.bufferedIds) {
      ids.push(id)
      this.bufferedIds.delete(id)
      if (ids.length >= this.cfg.batchSize) break
    }
    return ids
  }

  private rebufferIds(ids: number[]) {
    for (const id of ids) {
      this.bufferedIds.add(id)
    }
  }

  private async getNotificationRows(ids: number[]): Promise<NotificationRow[]> {
    if (ids.length === 0) return []
    return this.db.db
      .selectFrom('notification')
      .select([
        'id',
        'did',
        'recordUri',
        'recordCid',
        'author',
        'reason',
        'reasonSubject',
        'sortAt',
      ])
      .where('id', 'in', ids)
      .execute()
  }

  private async sendRows(rows: NotificationRow[]) {
    const eligible = await this.filterRowsByPushPreferences(rows)
    if (eligible.length === 0) return

    const copies = await this.composeCopyForRows(eligible)
    try {
      await withTimeout(
        this.courierClient.pushNotifications({
          notifications: eligible.map((row, i) =>
            toCourierNotification(row, copies[i]),
          ),
        }),
        this.cfg.courierTimeoutMs,
      )
    } catch (err) {
      await this.upsertOutboxRows(
        eligible.map((row) => ({
          ...row,
          courierNotificationId: getCourierNotificationId(row),
        })),
        summarizeError(err),
      )
    }
  }

  private async filterRowsByPushPreferences(
    rows: NotificationRow[],
  ): Promise<NotificationRow[]> {
    if (rows.length === 0) return []
    const dids = [...new Set(rows.map((row) => row.did))]
    const res = await this.db.db
      .selectFrom('private_data')
      .select(['actorDid', 'payload'])
      .where('actorDid', 'in', dids)
      .where(
        'namespace',
        '=',
        Namespaces.AppBskyNotificationDefsPreferences.$type,
      )
      .where('key', '=', 'self')
      .execute()
    const prefsByDid = new Map<
      string,
      Partial<app.bsky.notification.defs.Preferences> | undefined
    >()
    for (const row of res) {
      try {
        prefsByDid.set(
          row.actorDid,
          lexParse(
            row.payload,
          ) as Partial<app.bsky.notification.defs.Preferences>,
        )
      } catch (err) {
        console.error(
          '[notification-push-bridge] invalid notification preferences; using defaults',
          { did: row.actorDid, err },
        )
        prefsByDid.set(row.actorDid, undefined)
      }
    }
    return rows.filter((row) =>
      shouldPushForReason(prefsByDid.get(row.did), row.reason),
    )
  }

  // One roundtrip each for actors, profiles, posts — per send batch.
  private async hydratePushCopyContext(
    rows: NotificationRow[],
  ): Promise<PushCopyContext> {
    const authorDids = [...new Set(rows.map((row) => row.author))]
    const snippetUris = [
      ...new Set(
        rows.map(snippetUriForRow).filter((uri): uri is string => !!uri),
      ),
    ]
    // Snippet uris are all post uris; community-only post text lives in
    // `community_post` rather than `post`.
    const postUris = snippetUris.filter((uri) => !isCommunityPostUri(uri))
    const communityPostUris = snippetUris.filter(isCommunityPostUri)
    const [actors, profiles, posts, communityPosts] = await Promise.all([
      authorDids.length
        ? this.db.db
            .selectFrom('actor')
            .select(['did', 'handle'])
            .where('did', 'in', authorDids)
            .execute()
        : [],
      authorDids.length
        ? this.db.db
            .selectFrom('profile')
            .select(['creator', 'displayName'])
            .where('creator', 'in', authorDids)
            .execute()
        : [],
      postUris.length
        ? this.db.db
            .selectFrom('post')
            .select(['uri', 'text'])
            .where('uri', 'in', postUris)
            .execute()
        : [],
      communityPostUris.length
        ? this.db.db
            .selectFrom('community_post')
            .select(['uri', 'text'])
            .where('uri', 'in', communityPostUris)
            .execute()
        : [],
    ])
    const displayNameByDid = new Map(
      profiles.map((p) => [p.creator, p.displayName] as const),
    )
    return {
      actorsByDid: new Map(
        actors.map(
          (a) =>
            [
              a.did,
              {
                handle: a.handle,
                displayName: displayNameByDid.get(a.did) ?? null,
              },
            ] as const,
        ),
      ),
      postTextByUri: new Map(
        [...posts, ...communityPosts].map((p) => [p.uri, p.text] as const),
      ),
    }
  }

  // Returns copy aligned with `rows` by array index (outbox retry rows can
  // collide on notification id 0, so ids are not a safe key there).
  // Copy failures must never block delivery.
  private async composeCopyForRows(
    rows: NotificationRow[],
  ): Promise<PushCopy[]> {
    try {
      const ctx = await this.hydratePushCopyContext(rows)
      return rows.map((row) => composePushCopy(row, ctx))
    } catch (err) {
      console.error('[notification-push-bridge] copy hydration failed', err)
      return rows.map(() => GENERIC_PUSH_COPY)
    }
  }

  private async upsertOutboxRows(rows: OutboxRow[], error: string) {
    if (rows.length === 0) return
    const expiresAt = new Date(Date.now() + this.cfg.ttlHours * 60 * 60 * 1000)
    const values = rows.map((row) => ({
      id: getOutboxId(row),
      notificationId: row.id,
      did: row.did,
      recordUri: row.recordUri,
      recordCid: row.recordCid,
      author: row.author,
      reason: row.reason,
      reasonSubject: row.reasonSubject,
      sortAt: row.sortAt,
      courierNotificationId: row.courierNotificationId,
      status: 'pending',
      nextAttemptAt: new Date(),
      expiresAt,
      lastError: error,
    }))
    await this.db.db
      .insertInto('notification_push_outbox')
      .values(values)
      .onConflict((oc) =>
        oc.column('id').doUpdateSet({
          status: 'retryable',
          nextAttemptAt: new Date(),
          lastError: error,
          updatedAt: new Date(),
        }),
      )
      .execute()
  }

  private scheduleRetry(delayMs = this.cfg.retryIntervalMs) {
    if (this.retryTimer || this.stopped) return
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined
      this.activeRetry = this.processRetryBatch().finally(() => {
        this.scheduleRetry()
      })
    }, delayMs)
  }

  private async processRetryBatch() {
    if (this.stopped) return
    await this.reclaimStaleProcessingRows()
    await this.expireOutboxRows()
    const rows = await this.claimOutboxRows()
    if (rows.length === 0) return
    const notificationRows: NotificationRow[] = rows.map((row) => ({
      id: row.notificationId ?? 0,
      did: row.did,
      recordUri: row.recordUri,
      recordCid: row.recordCid,
      author: row.author,
      reason: row.reason,
      reasonSubject: row.reasonSubject,
      sortAt: row.sortAt,
    }))
    // Keyed by array index: reconstructed ids can collide on 0.
    const copies = await this.composeCopyForRows(notificationRows)
    try {
      await withTimeout(
        this.courierClient.pushNotifications({
          notifications: notificationRows.map((row, i) =>
            toCourierNotification(row, copies[i]),
          ),
        }),
        this.cfg.courierTimeoutMs,
      )
      await this.db.db
        .updateTable('notification_push_outbox')
        .set({ status: 'sent', updatedAt: new Date() })
        .where(
          'id',
          'in',
          rows.map((row) => row.id),
        )
        .execute()
    } catch (err) {
      const error = summarizeError(err)
      await Promise.all(
        rows.map((row) => {
          const attempts = row.attempts + 1
          const expired = attempts >= this.cfg.maxAttempts
          return this.db.db
            .updateTable('notification_push_outbox')
            .set({
              status: expired ? 'expired' : 'retryable',
              attempts,
              nextAttemptAt: new Date(
                Date.now() + getBackoffMs(attempts, this.cfg.retryIntervalMs),
              ),
              lastError: error,
              updatedAt: new Date(),
            })
            .where('id', '=', row.id)
            .execute()
        }),
      )
    }
  }

  private async claimOutboxRows() {
    return this.db.transaction(async (db) => {
      const rows = await sql<ClaimedOutboxRow>`
        select
          id,
          "notificationId",
          did,
          "recordUri",
          "recordCid",
          author,
          reason,
          "reasonSubject",
          "sortAt",
          "courierNotificationId",
          attempts
        from notification_push_outbox
        where status in ('pending', 'retryable')
          and "nextAttemptAt" <= now()
          and "expiresAt" > now()
        order by "nextAttemptAt" asc
        limit ${this.cfg.batchSize}
        for update skip locked
      `.execute(db.db)
      const ids = rows.rows.map((row) => row.id)
      if (ids.length > 0) {
        await db.db
          .updateTable('notification_push_outbox')
          .set({ status: 'processing', updatedAt: new Date() })
          .where('id', 'in', ids)
          .execute()
      }
      return rows.rows
    })
  }

  private async expireOutboxRows() {
    await this.db.db
      .updateTable('notification_push_outbox')
      .set({ status: 'expired', updatedAt: new Date() })
      .where('status', 'in', ['pending', 'retryable', 'processing'])
      .where('expiresAt', '<=', new Date())
      .execute()
  }

  private async reclaimStaleProcessingRows() {
    await this.db.db
      .updateTable('notification_push_outbox')
      .set({
        status: 'retryable',
        nextAttemptAt: new Date(),
        lastError: 'retry processing timeout',
        updatedAt: new Date(),
      })
      .where('status', '=', 'processing')
      .where('updatedAt', '<', new Date(Date.now() - this.processingTimeoutMs))
      .where('expiresAt', '>', new Date())
      .execute()
  }
}

export const parseNotificationPushBridgeConfigFromEnv =
  (): NotificationPushBridgeConfig => {
    const courierHttpVersion = process.env.BSKY_COURIER_HTTP_VERSION || '2'
    if (courierHttpVersion !== '1.1' && courierHttpVersion !== '2') {
      throw new Error('BSKY_COURIER_HTTP_VERSION must be "1.1" or "2"')
    }
    return {
      enabled: process.env.BSKY_NOTIFICATION_PUSH_WORKER_ENABLED === 'true',
      courierUrl: process.env.BSKY_COURIER_URL || undefined,
      courierApiKey: process.env.BSKY_COURIER_API_KEY || undefined,
      courierHttpVersion,
      courierIgnoreBadTls: process.env.BSKY_COURIER_IGNORE_BAD_TLS === 'true',
      batchSize: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_BATCH_SIZE || '100',
        10,
      ),
      batchWindowMs: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_BATCH_WINDOW_MS || '250',
        10,
      ),
      courierTimeoutMs: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_COURIER_TIMEOUT_MS || '5000',
        10,
      ),
      retryIntervalMs: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_RETRY_INTERVAL_MS || '10000',
        10,
      ),
      maxAttempts: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_MAX_ATTEMPTS || '10',
        10,
      ),
      ttlHours: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_TTL_HOURS || '24',
        10,
      ),
    }
  }

export const createNotificationPushBridge = (
  db: Database,
  cfg: NotificationPushBridgeConfig,
): NotificationPushBridge | undefined => {
  if (!cfg.enabled) return undefined
  if (!cfg.courierUrl) {
    console.warn(
      '[notification-push-bridge] disabled because BSKY_COURIER_URL is unset',
    )
    return undefined
  }
  return new NotificationPushBridge(db, cfg)
}

export function shouldPushForReason(
  prefs: Partial<app.bsky.notification.defs.Preferences> | undefined,
  reason: string,
) {
  if (!DEFAULT_ENABLED_REASONS.has(reason)) return false
  if (!prefs) return true
  switch (reason) {
    case 'follow':
      return prefs.follow?.push ?? true
    case 'like':
      return prefs.like?.push ?? true
    case 'like-via-repost':
      return prefs.likeViaRepost?.push ?? true
    case 'mention':
      return prefs.mention?.push ?? true
    case 'quote':
      return prefs.quote?.push ?? true
    case 'reply':
      return prefs.reply?.push ?? true
    case 'repost':
      return prefs.repost?.push ?? true
    case 'repost-via-repost':
      return prefs.repostViaRepost?.push ?? true
    case 'starterpack-joined':
      return prefs.starterpackJoined?.push ?? true
    case 'subscribed-post':
      return prefs.subscribedPost?.push ?? true
    case 'unverified':
      return prefs.unverified?.push ?? true
    case 'verified':
      return prefs.verified?.push ?? true
    default:
      return false
  }
}

export function toCourierNotification(
  row: NotificationRow,
  copy: PushCopy = GENERIC_PUSH_COPY,
) {
  return {
    id: getCourierNotificationId(row),
    recipientDid: row.did,
    title: copy.title,
    message: copy.message,
    collapseKey: row.reason,
    alwaysDeliver: false,
    clientControlled: false,
    timestamp: Timestamp.fromDate(new Date(row.sortAt)),
    additional: Struct.fromJson({
      reason: row.reason,
      uri: row.recordUri,
      cid: row.recordCid,
      subject: row.reasonSubject,
      recipientDid: row.did,
      actorDid: row.author,
    }),
  }
}

export function getOutboxId(row: NotificationRow) {
  return hashParts([
    row.did,
    row.recordUri,
    row.reason,
    row.reasonSubject ?? '',
  ])
}

export function getCourierNotificationId(row: NotificationRow) {
  return `appview:${getOutboxId(row)}`
}

function hashParts(parts: string[]) {
  return createHash('sha256').update(parts.join('::')).digest('hex')
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number) {
  let timer: NodeJS.Timeout | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () =>
            reject(new Error(`courier request timed out after ${timeoutMs}ms`)),
          timeoutMs,
        )
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function summarizeError(err: unknown) {
  if (err instanceof Error) return err.message.slice(0, 500)
  return String(err).slice(0, 500)
}

function getBackoffMs(attempts: number, baseMs: number) {
  const capped = Math.min(attempts, 8)
  const max = baseMs * 2 ** capped
  return Math.floor(Math.random() * max)
}
