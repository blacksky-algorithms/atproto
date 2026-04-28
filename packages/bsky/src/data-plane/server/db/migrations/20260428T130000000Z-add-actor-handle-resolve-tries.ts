import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  // Track failed handle-resolution attempts so the wintermute scheduler can
  // apply exponential backoff to broken/deleted DIDs that would otherwise
  // head-of-line block the resolver queue.
  //
  // PG 11+ ADD COLUMN with constant default is metadata-only -- no rewrite of
  // the actor table.
  await sql`
    ALTER TABLE actor
    ADD COLUMN IF NOT EXISTS "handleResolveTries" SMALLINT NOT NULL DEFAULT 0
  `.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`
    ALTER TABLE actor
    DROP COLUMN IF EXISTS "handleResolveTries"
  `.execute(db)
}
