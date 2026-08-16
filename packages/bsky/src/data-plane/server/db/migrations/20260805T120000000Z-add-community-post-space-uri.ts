import { Kysely, sql } from 'kysely'

// A space-backed post is discriminated by this column being set; NULL is the
// existing Blacksky community-only feed, untouched. Keyed by space rather than
// by feed because a feed is a view over a space: the same post can appear in
// several feeds, and the access decision belongs to the space.
export async function up(db: Kysely<unknown>): Promise<void> {
  const column = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'community_post'
        AND column_name = 'space_uri'
    ) AS exists
  `.execute(db)
  if (column.rows[0]?.exists !== true) {
    await db.schema
      .alterTable('community_post')
      .addColumn('space_uri', 'varchar')
      .execute()
  }
  const index = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = current_schema()
        AND indexname = 'community_post_space_uri_idx'
    ) AS exists
  `.execute(db)
  if (index.rows[0]?.exists !== true) {
    await db.schema
      .createIndex('community_post_space_uri_idx')
      .on('community_post')
      .column('space_uri')
      .execute()
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('community_post_space_uri_idx').execute()
  await db.schema.alterTable('community_post').dropColumn('space_uri').execute()
}
