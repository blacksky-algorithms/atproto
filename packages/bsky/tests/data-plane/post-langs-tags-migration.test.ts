import { type Kysely, sql } from 'kysely'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { TestNetwork } from '@atproto/dev-env'
import type { Database } from '../../src/data-plane/server/db/index.js'
import { up } from '../../src/data-plane/server/db/migrations/20261116T120000000Z-post-langs-tags-array.js'

describe('post langs and tags migration', () => {
  const converted = {
    uri: 'at://a',
    langs: ['fr', 'en-US'],
    tags: ['with "quotes", and commas'],
  }
  let network: TestNetwork
  let db: Database

  beforeAll(async () => {
    network = await TestNetwork.create({
      dbPostgresSchema: 'bsky_post_langs_tags_migration',
    })
    db = network.bsky.db
    await network.bsky.sub.stop()
  })

  afterAll(async () => network?.close())

  const columnTypes = async () => {
    const res = await sql<{ column: string; type: string }>`
      SELECT column_name AS column, udt_name AS type
      FROM information_schema.columns
      WHERE table_schema = current_schema() AND table_name = 'post'
        AND column_name IN ('langs', 'tags')
      ORDER BY column_name
    `.execute(db.db)
    return res.rows
  }

  const postRows = async () => {
    const res = await sql<{
      uri: string
      langs: string[] | null
      tags: string[] | null
    }>`SELECT uri, langs, tags FROM post ORDER BY uri`.execute(db.db)
    return res.rows
  }

  it('converts existing jsonb values and back.', async () => {
    await db.migrateToOrThrow('_20261103T000000003Z')
    expect(await columnTypes()).toEqual([
      { column: 'langs', type: 'jsonb' },
      { column: 'tags', type: 'jsonb' },
    ])

    const now = new Date().toISOString()
    const insert = (uri: string, langs: string | null, tags: string | null) =>
      sql`
        INSERT INTO post (uri, cid, creator, text, "createdAt", "indexedAt", langs, tags)
        VALUES (${uri}, 'cid', 'did:example:alice', 'text', ${now}, ${now}, ${langs}::jsonb, ${tags}::jsonb)
      `.execute(db.db)
    await insert(
      'at://a',
      '["fr","en-US"]',
      '["with \\"quotes\\", and commas"]',
    )
    await insert('at://b', null, '[]')
    await insert('at://c', '"not-an-array"', null)

    await db.migrateToLatestOrThrow()
    expect(await columnTypes()).toEqual([
      { column: 'langs', type: '_varchar' },
      { column: 'tags', type: '_varchar' },
    ])
    expect(await postRows()).toEqual([
      {
        uri: 'at://a',
        langs: ['fr', 'en-US'],
        tags: ['with "quotes", and commas'],
      },
      { uri: 'at://b', langs: null, tags: [] },
      { uri: 'at://c', langs: null, tags: null },
    ])

    await db.migrateToOrThrow('_20261103T000000003Z')
    expect(await columnTypes()).toEqual([
      { column: 'langs', type: 'jsonb' },
      { column: 'tags', type: 'jsonb' },
    ])
    expect((await postRows())[0]).toEqual(converted)
    await db.migrateToLatestOrThrow()
    expect((await postRows())[0]).toEqual(converted)
  })

  it('leaves array columns alone, without needing table ownership.', async () => {
    const before = await postRows()
    await db.db.transaction().execute(async (trx) => {
      await sql`CREATE ROLE bsky_post_langs_tags_reader NOLOGIN`.execute(trx)
      const { rows } = await sql<{ schema: string }>`
        SELECT current_schema() AS schema
      `.execute(trx)
      await sql`
        GRANT USAGE ON SCHEMA ${sql.id(rows[0].schema)}
        TO bsky_post_langs_tags_reader
      `.execute(trx)
      await sql`GRANT SELECT ON post TO bsky_post_langs_tags_reader`.execute(
        trx,
      )
      await sql`SET LOCAL ROLE bsky_post_langs_tags_reader`.execute(trx)
      await up(trx as unknown as Kysely<unknown>)
      await sql`RESET ROLE`.execute(trx)
      await sql`DROP OWNED BY bsky_post_langs_tags_reader`.execute(trx)
      await sql`DROP ROLE bsky_post_langs_tags_reader`.execute(trx)
    })
    expect(await columnTypes()).toEqual([
      { column: 'langs', type: '_varchar' },
      { column: 'tags', type: '_varchar' },
    ])
    expect(await postRows()).toEqual(before)
  })
})
