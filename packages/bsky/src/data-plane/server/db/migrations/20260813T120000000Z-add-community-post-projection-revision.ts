import { Kysely, sql } from 'kysely'

const indexExists = async (db: Kysely<unknown>, index: string) => {
  const result = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = current_schema() AND indexname = ${index}
    ) AS exists
  `.execute(db)
  return result.rows[0]?.exists === true
}

export async function up(db: Kysely<unknown>): Promise<void> {
  const column = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'community_post'
        AND column_name = 'projection_revision'
    ) AS exists
  `.execute(db)
  if (column.rows[0]?.exists !== true) {
    await db.schema
      .alterTable('community_post')
      .addColumn('projection_revision', 'varchar')
      .execute()
  }
  if (
    !(await indexExists(db, 'community_post_space_uri_projection_revision_idx'))
  ) {
    await db.schema
      .createIndex('community_post_space_uri_projection_revision_idx')
      .on('community_post')
      .columns(['space_uri', 'uri', 'projection_revision'])
      .unique()
      .execute()
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .dropIndex('community_post_space_uri_projection_revision_idx')
    .execute()
  await db.schema
    .alterTable('community_post')
    .dropColumn('projection_revision')
    .execute()
}
