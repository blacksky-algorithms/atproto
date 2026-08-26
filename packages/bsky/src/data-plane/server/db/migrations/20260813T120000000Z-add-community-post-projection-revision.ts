import { Kysely, sql } from 'kysely'

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
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .dropColumn('projection_revision')
    .execute()
}
