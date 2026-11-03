import { type Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await sql`
    create index if not exists notification_push_outbox_processing_idx
    on notification_push_outbox ("updatedAt")
    where status = 'processing'
  `.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`drop index if exists notification_push_outbox_processing_idx`.execute(
    db,
  )
}
