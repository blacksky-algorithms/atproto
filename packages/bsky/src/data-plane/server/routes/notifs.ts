import { Timestamp } from '@bufbuild/protobuf'
import type { ServiceImpl } from '@connectrpc/connect'
import { sql } from 'kysely'
import { keyBy } from '@atproto/common'
import { lexParse } from '@atproto/lex'
import type { app } from '../../../lexicons/index.js'
import type { Service } from '../../../proto/bsky_connect.js'
import {
  FilterableNotificationPreference,
  NotificationInclude,
  NotificationPreference,
  NotificationPreferences,
} from '../../../proto/bsky_pb.js'
import { Namespaces } from '../../../stash.js'
import type { Database } from '../db/index.js'
import { IsoSortAtKey } from '../db/pagination.js'
import { countAll, notSoftDeletedClause } from '../db/util.js'

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getNotifications(req) {
    const { actorDid, limit, cursor, priority, includeSpaceNotifications } = req
    const { ref } = db.db.dynamic
    const priorityFollowQb = db.db
      .selectFrom('follow')
      .select(sql<boolean>`true`.as('val'))
      .where('creator', '=', actorDid)
      .whereRef('subjectDid', '=', ref('notif.author'))
      .limit(1)

    let builder = db.db
      .selectFrom('notification as notif')
      .where('notif.did', '=', actorDid)
      // Subject-existence dispatches by collection: like/follow/repost/block
      // live in the typed tables (their record rows are being dropped), the
      // rest still resolve through the record store. reasonSubject is usually
      // a post uri, but via-repost reasons carry the repost uri.
      //
      // A space record uri has seven segments, so segment 4 is the literal
      // `space` rather than a collection, and the record store never holds one
      // — permissioned content only ever reaches `community_post`.
      .where(
        sql<boolean>`(
          notif."reasonSubject" IS NULL OR
          CASE split_part(notif."reasonSubject", '/', 4)
            WHEN 'space' THEN EXISTS (
              SELECT 1 FROM community_post cp
              WHERE cp.uri = notif."reasonSubject"
                AND cp.moderation_flagged_at IS NULL
            )
            WHEN 'app.bsky.feed.like' THEN EXISTS (SELECT 1 FROM "like" l WHERE l.uri = notif."reasonSubject")
            WHEN 'app.bsky.feed.repost' THEN EXISTS (SELECT 1 FROM repost rp WHERE rp.uri = notif."reasonSubject")
            WHEN 'app.bsky.graph.follow' THEN EXISTS (SELECT 1 FROM follow f WHERE f.uri = notif."reasonSubject")
            WHEN 'app.bsky.graph.block' THEN EXISTS (SELECT 1 FROM actor_block ab WHERE ab.uri = notif."reasonSubject")
            ELSE EXISTS (SELECT 1 FROM record r WHERE r.uri = notif."reasonSubject")
          END
        )`,
      )
      .$if(!includeSpaceNotifications, (qb) =>
        qb.where(
          sql<boolean>`(
            split_part(notif."recordUri", '/', 4) <> 'space' AND
            (
              notif."reasonSubject" IS NULL OR
              split_part(notif."reasonSubject", '/', 4) <> 'space'
            )
          )`,
        ),
      )
      .$if(includeSpaceNotifications, (qb) =>
        qb.where(
          sql<boolean>`(
            split_part(notif."recordUri", '/', 4) <> 'space' OR EXISTS (
              SELECT 1 FROM community_post cp
              WHERE cp.uri = notif."recordUri"
                AND cp.moderation_flagged_at IS NULL
            )
          )`,
        ),
      )
      .$if(priority, (qb) => qb.where(({ exists }) => exists(priorityFollowQb)))
      .select([
        'notif.author as authorDid',
        'notif.recordUri as uri',
        'notif.recordCid as cid',
        'notif.reason as reason',
        'notif.reasonSubject as reasonSubject',
        'notif.sortAt as sortAt',
      ])
      .select(priorityFollowQb.as('priority'))

    const key = new IsoSortAtKey(ref('notif.sortAt'))
    builder = key.paginate(builder, {
      cursor,
      limit,
    })

    const notifsRes = await builder.execute()
    const notifications = notifsRes.map((notif) => ({
      recipientDid: actorDid,
      uri: notif.uri,
      reason: notif.reason,
      reasonSubject: notif.reasonSubject ?? undefined,
      timestamp: Timestamp.fromDate(new Date(notif.sortAt)),
      priority: notif.priority === true || (notif.priority as unknown) === 't',
    }))
    return {
      notifications,
      cursor: key.packFromResult(notifsRes),
    }
  },

  async getNotificationSeen(req) {
    const { actorDid, priority } = req
    const res = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .selectAll()
      .executeTakeFirst()
    if (!res) {
      return {}
    }
    const lastSeen =
      priority && res.lastSeenPriorityNotifs
        ? res.lastSeenPriorityNotifs
        : res.lastSeenNotifs
    return {
      timestamp: Timestamp.fromDate(new Date(lastSeen)),
    }
  },

  async getUnreadNotificationCount(req) {
    const { actorDid, priority } = req
    const allowedSpaceUris = req.allowedSpaceUris ?? []
    const { ref } = db.db.dynamic
    const lastSeenRes = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .selectAll()
      .executeTakeFirst()
    const lastSeen =
      priority && lastSeenRes?.lastSeenPriorityNotifs
        ? lastSeenRes.lastSeenPriorityNotifs
        : lastSeenRes?.lastSeenNotifs

    const result = await db.db
      .selectFrom('notification')
      .select(countAll.as('count'))
      .innerJoin('actor', 'actor.did', 'notification.did')
      .leftJoin('actor_state', 'actor_state.did', 'actor.did')
      // Existence + takedown gate dispatches by collection: recordUri for
      // like/follow/repost/block notifications resolves in the typed tables
      // (their record rows are being dropped), everything else in record.
      // All arms are uri primary-key probes. The `space` arm covers seven-
      // segment permissioned uris, whose moderation flag is the community-side
      // analog of takedownRef.
      .where(
        sql<boolean>`(
          (
            split_part(notification."recordUri", '/', 4) <> 'space' AND
            (
              notification."reasonSubject" IS NULL OR
              split_part(notification."reasonSubject", '/', 4) <> 'space'
            )
          ) OR (
            split_part(notification."recordUri", '/', 4) = 'space' AND
            (
              notification."reasonSubject" IS NULL OR
              split_part(notification."reasonSubject", '/', 4) = 'space'
            ) AND EXISTS (
              SELECT 1 FROM community_post cp
              WHERE cp.uri = notification."recordUri"
                AND cp.moderation_flagged_at IS NULL
                AND cp.space_uri = ANY(${sql.val(allowedSpaceUris)}::text[])
                AND (
                  notification."reasonSubject" IS NULL OR EXISTS (
                    SELECT 1 FROM community_post subject_cp
                    WHERE subject_cp.uri = notification."reasonSubject"
                      AND subject_cp.space_uri = cp.space_uri
                      AND subject_cp.moderation_flagged_at IS NULL
                  )
                )
            )
          )
        )`,
      )
      .where(
        sql<boolean>`(
          CASE split_part(notification."recordUri", '/', 4)
            WHEN 'space' THEN true
            WHEN 'app.bsky.feed.like' THEN EXISTS (SELECT 1 FROM "like" l WHERE l.uri = notification."recordUri" AND l."takedownRef" IS NULL)
            WHEN 'app.bsky.feed.repost' THEN EXISTS (SELECT 1 FROM repost rp WHERE rp.uri = notification."recordUri" AND rp."takedownRef" IS NULL)
            WHEN 'app.bsky.graph.follow' THEN EXISTS (SELECT 1 FROM follow f WHERE f.uri = notification."recordUri" AND f."takedownRef" IS NULL)
            WHEN 'app.bsky.graph.block' THEN EXISTS (SELECT 1 FROM actor_block ab WHERE ab.uri = notification."recordUri" AND ab."takedownRef" IS NULL)
            ELSE EXISTS (SELECT 1 FROM record r WHERE r.uri = notification."recordUri" AND r."takedownRef" IS NULL)
          END
        )`,
      )
      .where(notSoftDeletedClause(ref('actor')))
      // Ensure to hit notification_did_sortat_idx, handling case where lastSeenNotifs is null.
      .where('notification.did', '=', actorDid)
      .where('notification.sortAt', '>', lastSeen ?? '')
      .$if(priority, (qb) =>
        qb.where(({ exists }) =>
          exists(
            db.db
              .selectFrom('follow')
              .select(sql<boolean>`true`.as('val'))
              .where('creator', '=', actorDid)
              .whereRef('subjectDid', '=', ref('notification.author')),
          ),
        ),
      )
      .executeTakeFirst()

    return {
      count: result?.count,
    }
  },

  async getUnreadNotificationSpaces(req) {
    const { actorDid, priority } = req
    const lastSeenRes = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .selectAll()
      .executeTakeFirst()
    const lastSeen =
      priority && lastSeenRes?.lastSeenPriorityNotifs
        ? lastSeenRes.lastSeenPriorityNotifs
        : lastSeenRes?.lastSeenNotifs
    const params: unknown[] = [actorDid, lastSeen ?? '']
    const priorityClause = priority
      ? `AND EXISTS (
          SELECT 1 FROM follow
          WHERE follow.creator = $1
            AND follow."subjectDid" = notification.author
        )`
      : ''
    const result = await db.pool.query<{
      post_uri: string
      space_uri: string
    }>(
      `SELECT DISTINCT cp.uri AS post_uri, cp.space_uri
       FROM notification
       JOIN community_post cp ON cp.uri = notification."recordUri"
       WHERE notification.did = $1
         AND notification."sortAt" > $2
         AND cp.space_uri IS NOT NULL
         AND cp.moderation_flagged_at IS NULL
         AND (
           notification."reasonSubject" IS NULL OR EXISTS (
             SELECT 1 FROM community_post subject_cp
             WHERE subject_cp.uri = notification."reasonSubject"
               AND subject_cp.space_uri = cp.space_uri
               AND subject_cp.moderation_flagged_at IS NULL
           )
         )
         ${priorityClause}`,
      params,
    )
    return {
      spaces: result.rows.map((row) => ({
        postUri: row.post_uri,
        spaceUri: row.space_uri,
      })),
    }
  },

  async updateNotificationSeen(req) {
    const { actorDid, timestamp, priority } = req
    if (!timestamp) {
      return
    }
    const timestampIso = timestamp.toDate().toISOString()
    let builder = db.db
      .updateTable('actor_state')
      .where('did', '=', actorDid)
      .returningAll()
    if (priority) {
      builder = builder.set({ lastSeenPriorityNotifs: timestampIso })
    } else {
      builder = builder.set({ lastSeenNotifs: timestampIso })
    }
    const updateRes = await builder.executeTakeFirst()
    if (updateRes) {
      return
    }
    await db.db
      .insertInto('actor_state')
      .values({
        did: actorDid,
        lastSeenNotifs: timestampIso,
        priorityNotifs: priority,
        lastSeenPriorityNotifs: priority ? timestampIso : undefined,
      })
      .onConflict((oc) => oc.doNothing())
      .executeTakeFirst()
  },

  async getNotificationPreferences(req) {
    const { dids } = req
    if (dids.length === 0) {
      return { preferences: [] }
    }

    const res = await db.db
      .selectFrom('private_data')
      .selectAll()
      .where('actorDid', 'in', dids)
      .where(
        'namespace',
        '=',
        Namespaces.AppBskyNotificationDefsPreferences.$type,
      )
      .where('key', '=', 'self')
      .execute()

    const byDid = keyBy(res, 'actorDid')
    const preferences = dids.map((did) => {
      const row = byDid.get(did)
      if (!row) {
        return {}
      }
      const p = lexParse<app.bsky.notification.defs.Preferences>(row.payload)
      return notificationPreferencesLexToProtobuf(p, row.payload)
    })

    return { preferences }
  },
})

export const notificationPreferencesLexToProtobuf = (
  p: app.bsky.notification.defs.Preferences,
  json: string,
): NotificationPreferences => {
  const lexFilterablePreferenceToProtobuf = (
    p: app.bsky.notification.defs.FilterablePreference,
  ): FilterableNotificationPreference =>
    new FilterableNotificationPreference({
      include:
        p.include === 'follows'
          ? NotificationInclude.FOLLOWS
          : NotificationInclude.ALL,
      list: { enabled: p.list ?? true },
      push: { enabled: p.push ?? true },
    })

  const lexPreferenceToProtobuf = (
    p: app.bsky.notification.defs.Preference,
  ): NotificationPreference =>
    new NotificationPreference({
      list: { enabled: p.list ?? true },
      push: { enabled: p.push ?? true },
    })

  return new NotificationPreferences({
    entry: Buffer.from(json),
    follow: lexFilterablePreferenceToProtobuf(p.follow),
    like: lexFilterablePreferenceToProtobuf(p.like),
    likeViaRepost: lexFilterablePreferenceToProtobuf(p.likeViaRepost),
    mention: lexFilterablePreferenceToProtobuf(p.mention),
    quote: lexFilterablePreferenceToProtobuf(p.quote),
    reply: lexFilterablePreferenceToProtobuf(p.reply),
    repost: lexFilterablePreferenceToProtobuf(p.repost),
    repostViaRepost: lexFilterablePreferenceToProtobuf(p.repostViaRepost),
    starterpackJoined: lexPreferenceToProtobuf(p.starterpackJoined),
    subscribedPost: lexPreferenceToProtobuf(p.subscribedPost),
    unverified: lexPreferenceToProtobuf(p.unverified),
    verified: lexPreferenceToProtobuf(p.verified),
  })
}
