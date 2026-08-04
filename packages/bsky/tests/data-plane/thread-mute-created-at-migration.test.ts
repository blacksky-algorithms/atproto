import { sql } from 'kysely'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { Database } from '../../src/data-plane/server/db/index.js'
import * as migration from '../../src/data-plane/server/db/migrations/20260803T000000000Z-thread-mute-created-at.js'

// The production thread_mute table was provisioned by hand without the
// createdAt column that MockBsync's insert supplies, so every muteThread
// call fails. This migration must add the column on such a database and
// no-op on a database whose schema is already correct.
describe('thread_mute createdAt migration', () => {
  let db: Database

  beforeAll(async () => {
    const url = process.env.DB_TEST_POSTGRES_URL || process.env.DB_POSTGRES_URL
    if (!url) {
      throw new Error('Missing DB_TEST_POSTGRES_URL or DB_POSTGRES_URL')
    }
    db = new Database({
      url,
      schema: 'bsky_thread_mute_created_at',
    })
    await db.migrateToLatestOrThrow()
  })

  afterAll(async () => {
    await db.close()
  })

  const hasCreatedAt = async () => {
    const res = await sql<{ column_name: string }>`
      select column_name from information_schema.columns
      where table_schema = 'bsky_thread_mute_created_at'
        and table_name = 'thread_mute'
        and column_name = 'createdAt'
    `.execute(db.db)
    return res.rows.length === 1
  }

  it('adds the column to a hand-provisioned table that lacks it', async () => {
    // simulate the drifted production schema
    await db.db.schema.alterTable('thread_mute').dropColumn('createdAt').execute()
    await expect(hasCreatedAt()).resolves.toBe(false)

    await migration.up(db.db)
    await expect(hasCreatedAt()).resolves.toBe(true)

    // the MockBsync write shape must now work
    await db.db
      .insertInto('thread_mute')
      .values({
        mutedByDid: 'did:plc:muter',
        rootUri: 'at://did:plc:author/app.bsky.feed.post/root',
        createdAt: new Date().toISOString(),
      })
      .execute()
    await db.db.deleteFrom('thread_mute').execute()
  })

  it('no-ops on a schema that already has the column', async () => {
    await expect(hasCreatedAt()).resolves.toBe(true)
    await migration.up(db.db)
    await expect(hasCreatedAt()).resolves.toBe(true)
  })
})
