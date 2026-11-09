import { Timestamp } from '@bufbuild/protobuf'
import type { ServiceImpl } from '@connectrpc/connect'
import { sql } from 'kysely'
import { keyBy } from '@atproto/common'
import { lexParse } from '@atproto/lex-json'
import { NOTIFICATION_REASON } from '../../../api/app/bsky/notification/constants.js'
import type { app } from '../../../lexicons/index.js'
import type { Service } from '../../../proto/bsky_connect.js'
import {
  FilterableNotificationPreference,
  NotificationFeed,
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
    const { actorDid, limit, cursor, includeSpaceNotifications } = req
    const { ref } = db.db.dynamic

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
      // — permissioned content only ever reaches `community_post`. A legacy
      // community stub resolves there too: its body, deletion and moderation
      // flag live in `community_post`, never in `record`. A via-repost subject
      // is only as eligible as the post that repost points at.
      .where(
        sql<boolean>`(
          notif."reasonSubject" IS NULL OR
          CASE split_part(notif."reasonSubject", '/', 4)
            WHEN 'space' THEN EXISTS (
              SELECT 1 FROM community_post cp
              WHERE cp.uri = notif."reasonSubject"
                AND cp.moderation_flagged_at IS NULL
            )
            WHEN 'community.blacksky.feed.post' THEN EXISTS (
              SELECT 1 FROM community_post cp
              WHERE cp.uri = notif."reasonSubject"
                AND cp.moderation_flagged_at IS NULL
            )
            WHEN 'app.bsky.feed.like' THEN EXISTS (SELECT 1 FROM "like" l WHERE l.uri = notif."reasonSubject")
            WHEN 'app.bsky.feed.repost' THEN EXISTS (
              SELECT 1 FROM repost rp
              WHERE rp.uri = notif."reasonSubject"
                AND (
                  rp.subject NOT LIKE '%/community.blacksky.feed.post/%' OR EXISTS (
                    SELECT 1 FROM community_post cp
                    WHERE cp.uri = rp.subject
                      AND cp.moderation_flagged_at IS NULL
                  )
                )
            )
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
      .select([
        'notif.author as authorDid',
        'notif.recordUri as uri',
        'notif.recordCid as cid',
        'notif.reason as reason',
        'notif.reasonSubject as reasonSubject',
        'notif.sortAt as sortAt',
      ])

    const key = new IsoSortAtKey(ref('notif.sortAt'))
    builder = key.paginate(builder, {
      cursor,
      limit,
    })

    const page = key.page(await builder.execute(), limit)
    const notifications = page.items.map((notif) => ({
      recipientDid: actorDid,
      uri: notif.uri,
      reason:
        notif.reason === NOTIFICATION_REASON.FOLLOW_BACK
          ? NOTIFICATION_REASON.FOLLOW
          : notif.reason,
      reasonSubject: notif.reasonSubject ?? undefined,
      timestamp: Timestamp.fromDate(new Date(notif.sortAt)),
    }))
    return {
      notifications,
      cursor: page.cursor,
    }
  },

  async getNotificationsV2(req) {
    const { actorDid, feed, limit, cursor } = req
    const { ref } = db.db.dynamic

    let builder = db.db
      .selectFrom('notification as notif')
      .where('notif.did', '=', actorDid)
      .where((eb) =>
        eb.or([
          eb('reasonSubject', 'is', null),
          eb.exists(
            db.db
              .selectFrom('record as subject')
              .selectAll()
              .whereRef('subject.uri', '=', ref('notif.reasonSubject')),
          ),
        ]),
      )
      .select([
        'notif.author as authorDid',
        'notif.recordUri as uri',
        'notif.recordCid as cid',
        'notif.reason as reason',
        'notif.reasonSubject as reasonSubject',
        'notif.sortAt as sortAt',
      ])

    switch (feed) {
      case NotificationFeed.ALL:
        break
      case NotificationFeed.PEOPLE_I_FOLLOW:
        builder = builder.where((eb) =>
          eb.exists(
            db.db
              .selectFrom('follow')
              .select('follow.uri')
              .where('follow.creator', '=', actorDid)
              .whereRef('follow.subjectDid', '=', ref('notif.author')),
          ),
        )
        break
      case NotificationFeed.CONVERSATIONS:
        builder = builder.where('notif.reason', 'in', [
          NOTIFICATION_REASON.REPLY,
          NOTIFICATION_REASON.QUOTE,
          NOTIFICATION_REASON.MENTION,
        ])
        break
      case NotificationFeed.FOLLOWERS:
        builder = builder.where('notif.reason', 'in', [
          NOTIFICATION_REASON.FOLLOW,
          NOTIFICATION_REASON.FOLLOW_BACK,
        ])
        break
      case NotificationFeed.ACTIVITY:
        builder = builder.where(
          'notif.reason',
          '=',
          NOTIFICATION_REASON.SUBSCRIBED_POST,
        )
        break
    }

    const key = new IsoSortAtKey(ref('notif.sortAt'))
    builder = key.paginate(builder, { cursor, limit })
    const page = key.page(await builder.execute(), limit)
    return {
      notifications: page.items.map((notif) => ({
        recipientDid: actorDid,
        uri: notif.uri,
        reason: notif.reason,
        reasonSubject: notif.reasonSubject ?? undefined,
        timestamp: Timestamp.fromDate(new Date(notif.sortAt)),
      })),
      cursor: page.cursor,
    }
  },

  async getNotificationSeen(req) {
    const { actorDid } = req
    const res = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .selectAll()
      .executeTakeFirst()
    if (!res) {
      return {}
    }
    return {
      timestamp: Timestamp.fromDate(new Date(res.lastSeenNotifs)),
    }
  },

  async getUnreadNotificationCount(req) {
    const { actorDid } = req
    const allowedSpaceUris = req.allowedSpaceUris ?? []
    const { ref } = db.db.dynamic
    const lastSeenRes = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .selectAll()
      .executeTakeFirst()
    const lastSeen = lastSeenRes?.lastSeenNotifs

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
      .executeTakeFirst()

    return {
      count: result?.count,
    }
  },

  async getUnreadNotificationSpaces(req) {
    const { actorDid } = req
    const lastSeenRes = await db.db
      .selectFrom('actor_state')
      .where('did', '=', actorDid)
      .select('lastSeenNotifs')
      .executeTakeFirst()
    const params: unknown[] = [actorDid, lastSeenRes?.lastSeenNotifs ?? '']
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
         )`,
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
    const { actorDid, timestamp } = req
    if (!timestamp) {
      return
    }
    const timestampIso = timestamp.toDate().toISOString()
    const { ref } = db.db.dynamic
    await db.db
      .insertInto('actor_state')
      .values({
        did: actorDid,
        lastSeenNotifs: timestampIso,
      })
      .onConflict((oc) =>
        oc.column('did').doUpdateSet({
          lastSeenNotifs: sql`greatest(${ref('actor_state.lastSeenNotifs')}, ${timestampIso})`,
        }),
      )
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
