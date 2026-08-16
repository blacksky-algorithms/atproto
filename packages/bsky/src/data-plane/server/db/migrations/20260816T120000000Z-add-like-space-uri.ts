import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  const column = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'like'
        AND column_name = 'space_uri'
    ) AS exists
  `.execute(db)
  if (column.rows[0]?.exists !== true) {
    await db.schema
      .alterTable('like')
      .addColumn('space_uri', 'varchar')
      .execute()
  }
  const index = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = current_schema()
        AND indexname = 'like_space_uri_creator_sort_idx'
    ) AS exists
  `.execute(db)
  if (index.rows[0]?.exists !== true) {
    await db.schema
      .createIndex('like_space_uri_creator_sort_idx')
      .on('like')
      .columns(['space_uri', 'creator', 'sortAt'])
      .execute()
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('like_space_uri_creator_sort_idx').execute()
  await db.schema.alterTable('like').dropColumn('space_uri').execute()
}
