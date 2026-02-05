import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  // Community post content table - stores full content submitted via
  // community.blacksky.feed.submitPost XRPC. The PDS only holds stubs.
  await db.schema
    .createTable('community_post')
    .addColumn('uri', 'varchar', (col) => col.notNull().primaryKey())
    .addColumn('cid', 'varchar', (col) => col.notNull().defaultTo(''))
    .addColumn('rkey', 'varchar', (col) => col.notNull())
    .addColumn('creator', 'varchar', (col) => col.notNull())
    .addColumn('text', 'text', (col) => col.notNull().defaultTo(''))
    .addColumn('facets', 'jsonb')
    .addColumn('replyRoot', 'varchar')
    .addColumn('replyRootCid', 'varchar')
    .addColumn('replyParent', 'varchar')
    .addColumn('replyParentCid', 'varchar')
    .addColumn('embed', 'jsonb')
    .addColumn('langs', 'varchar')
    .addColumn('labels', 'jsonb')
    .addColumn('tags', 'varchar')
    .addColumn('createdAt', 'varchar', (col) => col.notNull())
    .addColumn('indexedAt', 'varchar', (col) => col.notNull())
    .addColumn('sortAt', 'varchar', (col) =>
      col
        .generatedAlwaysAs(sql`least("createdAt", "indexedAt")`)
        .stored()
        .notNull(),
    )
    .execute()

  // Supports getAuthorFeed for community posts
  await sql`CREATE INDEX "community_post_creator_sort_idx" ON "community_post" ("creator", "sortAt" DESC)`.execute(
    db,
  )

  // Supports timeline queries
  await sql`CREATE INDEX "community_post_sort_idx" ON "community_post" ("sortAt" DESC)`.execute(
    db,
  )

  // Supports thread hydration
  await db.schema
    .createIndex('community_post_reply_root_idx')
    .on('community_post')
    .column('replyRoot')
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('community_post').execute()
}
