import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import type { DataplaneNotificationReason } from '../../src/api/app/bsky/notification/constants.js'
import { Database } from '../../src/data-plane/server/db/index.js'
import notifRoutes from '../../src/data-plane/server/routes/notifs.js'
import { NotificationFeed } from '../../src/proto/bsky_pb.js'

/**
 * A legacy community post lives only in `community_post`; its notifications
 * must follow that row, not `record`. Reposts of such posts are ordinary
 * `app.bsky.feed.repost` rows, so a `repost` reason carries the stub as its
 * subject and a via-repost reason carries the repost.
 */

const RECIPIENT = 'did:plc:recipient'
const ACTOR = 'did:plc:actor'
const STUB = `at://${RECIPIENT}/community.blacksky.feed.post/comm`
const REPOST = `at://${ACTOR}/app.bsky.feed.repost/comm`
const VIA_LIKE = `at://did:plc:third/app.bsky.feed.like/via`

describe('notification listing for community-only subjects', () => {
  let db: Database

  beforeAll(async () => {
    const url = process.env.DB_TEST_POSTGRES_URL || process.env.DB_POSTGRES_URL
    if (!url) {
      throw new Error('Missing DB_TEST_POSTGRES_URL or DB_POSTGRES_URL')
    }
    db = new Database({ url, schema: 'bsky_notifs_community_subjects' })
    await db.migrateToLatestOrThrow()
  })

  afterAll(async () => {
    await db.close()
  })

  afterEach(async () => {
    await db.db.deleteFrom('notification').execute()
    await db.db.deleteFrom('repost').execute()
    await db.db.deleteFrom('community_post').execute()
  })

  const now = () => new Date().toISOString()

  async function insertStub(flagged = false) {
    await db.db
      .insertInto('community_post')
      .values({
        uri: STUB,
        cid: 'bafycommunitypostcid',
        rkey: 'comm',
        creator: RECIPIENT,
        text: 'community only post',
        moderation_flagged_at: flagged ? now() : null,
        createdAt: now(),
        indexedAt: now(),
      })
      .execute()
  }

  async function insertRepost() {
    await db.db
      .insertInto('repost')
      .values({
        uri: REPOST,
        cid: 'bafyrepostcid',
        creator: ACTOR,
        subject: STUB,
        subjectCid: 'bafycommunitypostcid',
        createdAt: now(),
        indexedAt: now(),
      })
      .execute()
  }

  async function insertNotification(
    recipient: string,
    author: string,
    recordUri: string,
    reason: DataplaneNotificationReason,
    reasonSubject: string,
  ) {
    await db.db
      .insertInto('notification')
      .values({
        did: recipient,
        author,
        recordUri,
        recordCid: 'bafyrecordcid',
        reason,
        reasonSubject,
        sortAt: now(),
      })
      .execute()
  }

  const describeNotif = (n: { reason?: string; uri?: string }) =>
    `${n.reason}:${n.uri}`

  // Both reads share the subject-eligibility rules, so every case is
  // asserted against the legacy list and the v2 (grouped) list.
  async function list(actorDid: string, includeSpaceNotifications = false) {
    const routes = notifRoutes(db)
    const req = { actorDid, limit: 10, cursor: '', includeSpaceNotifications }
    const [legacy, v2] = await Promise.all([
      routes.getNotifications!(req as any, {} as any),
      routes.getNotificationsV2!(
        { ...req, feed: NotificationFeed.ALL } as any,
        {} as any,
      ),
    ])
    const legacyItems = (legacy.notifications ?? []).map(describeNotif)
    const v2Items = (v2.notifications ?? []).map(describeNotif)
    expect(v2Items).toEqual(legacyItems)
    return legacyItems
  }

  it('lists a repost of a community-only post while the post exists', async () => {
    await insertStub()
    await insertRepost()
    await insertNotification(RECIPIENT, ACTOR, REPOST, 'repost', STUB)

    await expect(list(RECIPIENT)).resolves.toEqual([`repost:${REPOST}`])
  })

  it('hides it once the community-only post is flagged', async () => {
    await insertStub(true)
    await insertRepost()
    await insertNotification(RECIPIENT, ACTOR, REPOST, 'repost', STUB)

    await expect(list(RECIPIENT)).resolves.toEqual([])
  })

  it('hides it once the community-only post is deleted', async () => {
    await insertRepost()
    await insertNotification(RECIPIENT, ACTOR, REPOST, 'repost', STUB)

    await expect(list(RECIPIENT)).resolves.toEqual([])
  })

  it('judges a via-repost notification by the reposted community-only post', async () => {
    await insertStub()
    await insertRepost()
    await insertNotification(
      ACTOR,
      'did:plc:third',
      VIA_LIKE,
      'like-via-repost',
      REPOST,
    )

    await expect(list(ACTOR)).resolves.toEqual([`like-via-repost:${VIA_LIKE}`])

    await db.db
      .updateTable('community_post')
      .set({ moderation_flagged_at: now() })
      .where('uri', '=', STUB)
      .execute()

    await expect(list(ACTOR)).resolves.toEqual([])
  })

  it('still lists a via-repost notification whose repost names a public post', async () => {
    const publicRepost = `at://${ACTOR}/app.bsky.feed.repost/pub`
    await db.db
      .insertInto('repost')
      .values({
        uri: publicRepost,
        cid: 'bafyrepostcid2',
        creator: ACTOR,
        subject: 'at://did:plc:someone/app.bsky.feed.post/pub',
        subjectCid: 'bafypostcid',
        createdAt: now(),
        indexedAt: now(),
      })
      .execute()
    await insertNotification(
      ACTOR,
      'did:plc:third',
      VIA_LIKE,
      'like-via-repost',
      publicRepost,
    )

    await expect(list(ACTOR)).resolves.toEqual([`like-via-repost:${VIA_LIKE}`])
  })

  it('surfaces a live space record only when space notifications are requested', async () => {
    const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
    const SPACE_POST = `${SPACE}/did:plc:alice/app.bsky.feed.post/mention`
    await db.db
      .insertInto('community_post')
      .values({
        uri: SPACE_POST,
        cid: 'bafyspacepostcid',
        rkey: 'mention',
        creator: 'did:plc:alice',
        text: 'private mention',
        space_uri: SPACE,
        createdAt: now(),
        indexedAt: now(),
      })
      .execute()
    await insertNotification(
      RECIPIENT,
      'did:plc:alice',
      SPACE_POST,
      'mention',
      SPACE_POST,
    )

    await expect(list(RECIPIENT, false)).resolves.toEqual([])
    await expect(list(RECIPIENT, true)).resolves.toEqual([
      `mention:${SPACE_POST}`,
    ])

    await db.db
      .updateTable('community_post')
      .set({ moderation_flagged_at: now() })
      .where('uri', '=', SPACE_POST)
      .execute()

    await expect(list(RECIPIENT, true)).resolves.toEqual([])
  })
})
