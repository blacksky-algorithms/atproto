import { Kysely, sql } from 'kysely'

const indexName = 'notification_did_record_uri_reason_unique'

export async function up(db: Kysely<unknown>): Promise<void> {
  const result = await sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1 FROM pg_indexes
      WHERE schemaname = current_schema() AND indexname = ${indexName}
    ) AS exists
  `.execute(db)
  if (result.rows[0]?.exists !== true) {
    await db.schema
      .createIndex(indexName)
      .on('notification')
      .columns(['did', 'recordUri', 'reason'])
      .unique()
      .execute()
  }
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex(indexName).ifExists().execute()
}
