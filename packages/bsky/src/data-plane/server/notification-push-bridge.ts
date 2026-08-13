import { createHash } from 'node:crypto'
import { Struct, Timestamp } from '@bufbuild/protobuf'
import { sql } from 'kysely'
import pg from 'pg'
import { lexParse } from '@atproto/lex'
import {
  type CourierClient,
  authWithApiKey as courierAuth,
  createCourierClient,
} from '../../courier.js'
import type { app } from '../../lexicons/index.js'
import { Namespaces } from '../../stash.js'
import type { Database } from './db/index.js'
import {
  GENERIC_PUSH_COPY,
  type PushCopy,
  type PushCopyContext,
  composePushCopy,
  isCommunityPostUri,
  snippetUriForRow,
} from './notification-push-copy.js'

const CHANNEL = 'notification_push_inserted'
// Runaway backstop for the batched maintenance sweeps: even a fully backed-up
// outbox is cleared over successive retry ticks rather than in one unbounded
// pass, so a single tick never scans the whole table.
const MAX_MAINTENANCE_BATCHES = 50
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

// COMMUNITY_POSTS_ENABLED master launch switch. Local env check because the
// canonical helper (communityPostsEnabled in
// src/api/community/blacksky/membership-guard.ts) sits in the api layer,
// which the dataplane must not import (context.ts would circle back here).
// Keep the two definitions in sync.
const communityPostsEnabled = (): boolean =>
  process.env.COMMUNITY_POSTS_ENABLED !== 'false'

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
  maintenanceBatchSize: number
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
    const prefsByDid = await this.getPushPreferencesByDid([
      ...new Set(rows.map((row) => row.did)),
    ])
    const eligible = rows.filter((row) =>
      shouldPushForReason(prefsByDid.get(row.did), row.reason),
    )
    if (eligible.length === 0) return

    const suppressed = await this.findSuppressedRows(eligible, prefsByDid)
    const visible = eligible.filter((row) => !suppressed.has(row))
    if (visible.length === 0) return

    const copies = await this.composeCopyForRows(visible)
    try {
      await withTimeout(
        this.courierClient.pushNotifications({
          notifications: visible.map((row, i) =>
            toCourierNotification(row, copies[i]),
          ),
        }),
        this.cfg.courierTimeoutMs,
      )
    } catch (err) {
      await this.upsertOutboxRows(
        visible.map((row) => ({
          ...row,
          courierNotificationId: getCourierNotificationId(row),
        })),
        summarizeError(err),
      )
    }
  }

  private async getPushPreferencesByDid(
    dids: string[],
  ): Promise<
    Map<string, Partial<app.bsky.notification.defs.Preferences> | undefined>
  > {
    const prefsByDid = new Map<
      string,
      Partial<app.bsky.notification.defs.Preferences> | undefined
    >()
    if (dids.length === 0) return prefsByDid
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
    return prefsByDid
  }

  // Parity with the appview's listNotifications pipeline, which hides a
  // notification entirely (noBlockOrMutesOrNeedsFiltering + takedown handling
  // in the notification view) rather than showing its author/content. The push
  // channel would otherwise surface a name + post snippet the in-app list
  // suppresses — worst on the outbox retry path, which re-reads content long
  // after moderation acted. Returns the subset of `rows` that must NOT push.
  //
  // Covers: bidirectional actor blocks, recipient->author mutes (unscoped
  // only — a scoped mute does not hide the notification), recipient
  // thread mutes on the subject's thread root, follows-only (`include:
  // 'follows'`) reason preferences, actor takedown / non-active upstream
  // status, and record-level takedowns (of the notif record and its subject).
  // Does NOT cover label-based takedowns or needs-review labels: those require
  // resolving labeler output, which the dataplane has no client for.
  // List-based blocks/mutes are also not covered. One roundtrip per check —
  // per send batch; every query is keyed on the batch's dids/uris.
  private async findSuppressedRows(
    rows: NotificationRow[],
    knownPrefsByDid?: Map<
      string,
      Partial<app.bsky.notification.defs.Preferences> | undefined
    >,
  ): Promise<Set<NotificationRow>> {
    const suppressed = new Set<NotificationRow>()
    if (rows.length === 0) return suppressed
    const recipients = [...new Set(rows.map((row) => row.did))]
    const authors = [...new Set(rows.map((row) => row.author))]
    const recordUris = [
      ...new Set(
        rows.flatMap((row) =>
          [row.recordUri, row.reasonSubject].filter(
            (uri): uri is string => !!uri,
          ),
        ),
      ),
    ]
    const subjectPostUris = [
      ...new Set(
        rows
          .map((row) => row.reasonSubject)
          .filter(
            (uri): uri is string =>
              !!uri && uri.includes('/app.bsky.feed.post/'),
          ),
      ),
    ]
    const [blocks, mutes, takenDownActors, takenDownRecords, prefsByDid] =
      await Promise.all([
        this.db.db
          .selectFrom('actor_block')
          .select(['creator', 'subjectDid'])
          .where((eb) =>
            eb.or([
              eb.and([
                eb('creator', 'in', authors),
                eb('subjectDid', 'in', recipients),
              ]),
              eb.and([
                eb('creator', 'in', recipients),
                eb('subjectDid', 'in', authors),
              ]),
            ]),
          )
          .execute(),
        this.db.db
          .selectFrom('mute')
          .select(['mutedByDid', 'subjectDid', 'onlyReposts', 'onlyQuoteposts'])
          .where('mutedByDid', 'in', recipients)
          .where('subjectDid', 'in', authors)
          .execute(),
        this.db.db
          .selectFrom('actor')
          .select('did')
          .where('did', 'in', authors)
          .where((eb) =>
            eb.or([
              eb('takedownRef', 'is not', null),
              eb.and([
                eb('upstreamStatus', 'is not', null),
                eb('upstreamStatus', '!=', 'active'),
              ]),
            ]),
          )
          .execute(),
        // Takedown state for like/follow/repost/block lives on the typed
        // tables (their record rows are being dropped); union both homes.
        this.db.db
          .selectFrom('record')
          .select('uri')
          .where('uri', 'in', recordUris)
          .where('takedownRef', 'is not', null)
          .unionAll(
            this.db.db
              .selectFrom('like')
              .select('uri')
              .where('uri', 'in', recordUris)
              .where('takedownRef', 'is not', null),
          )
          .unionAll(
            this.db.db
              .selectFrom('repost')
              .select('uri')
              .where('uri', 'in', recordUris)
              .where('takedownRef', 'is not', null),
          )
          .unionAll(
            this.db.db
              .selectFrom('follow')
              .select('uri')
              .where('uri', 'in', recordUris)
              .where('takedownRef', 'is not', null),
          )
          .unionAll(
            this.db.db
              .selectFrom('actor_block')
              .select('uri')
              .where('uri', 'in', recordUris)
              .where('takedownRef', 'is not', null),
          )
          .execute(),
        knownPrefsByDid ?? this.getPushPreferencesByDid(recipients),
      ])
    // Thread mutes are keyed by the subject's thread root, which needs the
    // subject post's replyRoot (the subject is its own root when not a reply).
    const rootBySubject = new Map<string, string>()
    if (subjectPostUris.length > 0) {
      const subjectPosts = await this.db.db
        .selectFrom('post')
        .select(['uri', 'replyRoot'])
        .where('uri', 'in', subjectPostUris)
        .execute()
      for (const post of subjectPosts) {
        rootBySubject.set(post.uri, post.replyRoot ?? post.uri)
      }
    }
    const subjectRootFor = (row: NotificationRow) =>
      row.reasonSubject
        ? (rootBySubject.get(row.reasonSubject) ?? row.reasonSubject)
        : undefined
    const threadRoots = [
      ...new Set(
        rows.map(subjectRootFor).filter((uri): uri is string => !!uri),
      ),
    ]
    const threadMutes =
      threadRoots.length > 0
        ? await this.db.db
            .selectFrom('thread_mute')
            .select(['mutedByDid', 'rootUri'])
            .where('mutedByDid', 'in', recipients)
            .where('rootUri', 'in', threadRoots)
            .execute()
        : []
    const threadMutePairs = new Set(
      threadMutes.map((m) => `${m.mutedByDid}:${m.rootUri}`),
    )
    const followsOnlyRows = rows.filter((row) =>
      isFollowsOnlyForReason(prefsByDid.get(row.did), row.reason),
    )
    const follows =
      followsOnlyRows.length > 0
        ? await this.db.db
            .selectFrom('follow')
            .select(['creator', 'subjectDid'])
            .where('creator', 'in', [
              ...new Set(followsOnlyRows.map((row) => row.did)),
            ])
            .where('subjectDid', 'in', [
              ...new Set(followsOnlyRows.map((row) => row.author)),
            ])
            .execute()
        : []
    const followPairs = new Set(
      follows.map((f) => `${f.creator}:${f.subjectDid}`),
    )
    // Blocks are bidirectional: a block in either direction hides the notif.
    const blockPairs = new Set<string>()
    for (const b of blocks) {
      blockPairs.add(`${b.creator}:${b.subjectDid}`)
      blockPairs.add(`${b.subjectDid}:${b.creator}`)
    }
    // A scoped mute ("only their reposts") is not a full mute: the appview
    // treats the subject as muted only when no scope is set, so suppressing
    // the push on mere row existence would hide notifications that
    // listNotifications still shows. Mirrors getRelationships/getActorMutesActor.
    const mutePairs = new Set(
      mutes
        .filter((m) => !m.onlyReposts && !m.onlyQuoteposts)
        .map((m) => `${m.mutedByDid}:${m.subjectDid}`),
    )
    const takenDownActorDids = new Set(takenDownActors.map((a) => a.did))
    const takenDownRecordUris = new Set(takenDownRecords.map((r) => r.uri))
    for (const row of rows) {
      const pair = `${row.did}:${row.author}`
      const subjectRoot = subjectRootFor(row)
      if (
        takenDownActorDids.has(row.author) ||
        takenDownRecordUris.has(row.recordUri) ||
        (row.reasonSubject && takenDownRecordUris.has(row.reasonSubject)) ||
        blockPairs.has(pair) ||
        mutePairs.has(pair) ||
        (subjectRoot && threadMutePairs.has(`${row.did}:${subjectRoot}`)) ||
        (isFollowsOnlyForReason(prefsByDid.get(row.did), row.reason) &&
          !followPairs.has(pair))
      ) {
        suppressed.add(row)
      }
    }
    return suppressed
  }

  // One roundtrip each for actors, profiles, posts, and community posts —
  // per send batch.
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
    // `community_post` rather than `post`. When the community-posts launch
    // switch is off, skip community hydration entirely — those uris then miss
    // the text map, so their pushes degrade to phrase-only copy.
    const postUris = snippetUris.filter((uri) => !isCommunityPostUri(uri))
    const communityPostUris = communityPostsEnabled()
      ? snippetUris.filter(isCommunityPostUri)
      : []
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
      // Crash isolation: a rejection here (e.g. a statement timeout on the
      // outbox) must never reach Node's unhandledRejection handler, which would
      // exit(1) and take down the entire dataplane process. Swallow and log,
      // then always reschedule so the loop self-heals on the next tick. Mirrors
      // the try/catch already guarding flush().
      this.activeRetry = this.processRetryBatch()
        .catch((err) => {
          console.error('[notification-push-bridge] retry batch failed', err)
        })
        .finally(() => {
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
    // A row can become block/mute/takedown-hidden after it was enqueued, so
    // re-check on retry too; drop those to a terminal 'suppressed' status so
    // they neither push nor get reclaimed for another attempt. notificationRows
    // is aligned with `rows` by index.
    const suppressed = await this.findSuppressedRows(notificationRows)
    const sendable = rows.filter((_, i) => !suppressed.has(notificationRows[i]))
    const suppressedIds = rows
      .filter((_, i) => suppressed.has(notificationRows[i]))
      .map((row) => row.id)
    if (suppressedIds.length > 0) {
      await this.db.db
        .updateTable('notification_push_outbox')
        .set({ status: 'suppressed', updatedAt: new Date() })
        .where('id', 'in', suppressedIds)
        .execute()
    }
    if (sendable.length === 0) return
    // Keyed by array index: reconstructed ids can collide on 0.
    const sendableRows = notificationRows.filter((row) => !suppressed.has(row))
    const copies = await this.composeCopyForRows(sendableRows)
    try {
      await withTimeout(
        this.courierClient.pushNotifications({
          notifications: sendableRows.map((row, i) =>
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
          sendable.map((row) => row.id),
        )
        .execute()
    } catch (err) {
      const error = summarizeError(err)
      await Promise.all(
        sendable.map((row) => {
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

  // Bounded maintenance sweep: rewriting every matching row in one UPDATE can
  // cross the connection statement_timeout once the outbox holds tens of
  // millions of rows — the aborted statement then rolls back, leaves dead
  // tuples, and each retry is slower (the crash-loop bloat ratchet). Instead
  // flip rows in id-keyed batches until a sweep affects fewer than a full
  // batch. Each batch changes status out of the matched set, so the loop
  // converges. Bounded by a hard iteration cap as a runaway backstop.
  private async expireOutboxRows() {
    const now = new Date()
    const limit = this.cfg.maintenanceBatchSize
    for (let i = 0; i < MAX_MAINTENANCE_BATCHES; i++) {
      if (this.stopped) return
      const res = await this.db.db
        .updateTable('notification_push_outbox')
        .set({ status: 'expired', updatedAt: new Date() })
        .where('id', 'in', (eb) =>
          eb
            .selectFrom('notification_push_outbox')
            .select('id')
            .where('status', 'in', ['pending', 'retryable', 'processing'])
            .where('expiresAt', '<=', now)
            .limit(limit),
        )
        .executeTakeFirst()
      if (Number(res.numUpdatedRows ?? 0) < limit) return
    }
  }

  private async reclaimStaleProcessingRows() {
    const staleBefore = new Date(Date.now() - this.processingTimeoutMs)
    const now = new Date()
    const limit = this.cfg.maintenanceBatchSize
    for (let i = 0; i < MAX_MAINTENANCE_BATCHES; i++) {
      if (this.stopped) return
      const res = await this.db.db
        .updateTable('notification_push_outbox')
        .set({
          status: 'retryable',
          nextAttemptAt: new Date(),
          lastError: 'retry processing timeout',
          updatedAt: new Date(),
        })
        .where('id', 'in', (eb) =>
          eb
            .selectFrom('notification_push_outbox')
            .select('id')
            .where('status', '=', 'processing')
            .where('updatedAt', '<', staleBefore)
            .where('expiresAt', '>', now)
            .limit(limit),
        )
        .executeTakeFirst()
      if (Number(res.numUpdatedRows ?? 0) < limit) return
    }
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
      maintenanceBatchSize: parseInt(
        process.env.BSKY_NOTIFICATION_PUSH_MAINTENANCE_BATCH_SIZE || '5000',
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

export function isFollowsOnlyForReason(
  prefs: Partial<app.bsky.notification.defs.Preferences> | undefined,
  reason: string,
) {
  if (!prefs) return false
  switch (reason) {
    case 'follow':
      return prefs.follow?.include === 'follows'
    case 'like':
      return prefs.like?.include === 'follows'
    case 'like-via-repost':
      return prefs.likeViaRepost?.include === 'follows'
    case 'mention':
      return prefs.mention?.include === 'follows'
    case 'quote':
      return prefs.quote?.include === 'follows'
    case 'reply':
      return prefs.reply?.include === 'follows'
    case 'repost':
      return prefs.repost?.include === 'follows'
    case 'repost-via-repost':
      return prefs.repostViaRepost?.include === 'follows'
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
