import { Kysely, sql } from 'kysely'

const addColumnIfMissing = async (db: Kysely<unknown>, column: string) => {
  const result = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'community_post'
        AND column_name = ${column}
    ) AS exists
  `.execute(db)
  if (result.rows[0]?.exists !== true) {
    await sql`ALTER TABLE community_post ADD COLUMN ${sql.ref(column)} varchar`.execute(
      db,
    )
  }
}

// A moderated community post is flagged, never deleted. The author's own copy
// lives in their repo and is not ours to remove, and reversal has to be a
// local state flip rather than a re-fetch, so the row stays and the read paths
// skip it.
export async function up(db: Kysely<unknown>): Promise<void> {
  await addColumnIfMissing(db, 'moderation_flagged_at')
  await addColumnIfMissing(db, 'moderation_flagged_by')
  // Every read path filters on the flag and orders by sortAt, so index the
  // pair. Leading with the flag keeps the common (unflagged) case contiguous.
  const index = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = current_schema()
        AND indexname = 'community_post_unflagged_sort_idx'
    ) AS exists
  `.execute(db)
  if (index.rows[0]?.exists !== true) {
    await db.schema
      .createIndex('community_post_unflagged_sort_idx')
      .on('community_post')
      .columns(['moderation_flagged_at', 'sortAt'])
      .execute()
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('community_post_unflagged_sort_idx').execute()
  await db.schema
    .alterTable('community_post')
    .dropColumn('moderation_flagged_at')
    .dropColumn('moderation_flagged_by')
    .execute()
}
