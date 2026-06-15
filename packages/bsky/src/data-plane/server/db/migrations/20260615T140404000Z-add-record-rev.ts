import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  // Repo commit rev: read by the appview, written by wintermute to guard stale writes.
  await sql`
    ALTER TABLE record
    ADD COLUMN IF NOT EXISTS "rev" varchar
  `.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`
    ALTER TABLE record
    DROP COLUMN IF EXISTS "rev"
  `.execute(db)
}
