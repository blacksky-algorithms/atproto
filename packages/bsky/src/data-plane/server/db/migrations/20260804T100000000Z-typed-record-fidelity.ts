import { Kysely, sql } from 'kysely'

// Idempotent: production may already carry these columns from the
// out-of-band DDL applied ahead of the record-table migration.
export async function up(db: Kysely<unknown>): Promise<void> {
  await sql`ALTER TABLE follow ADD COLUMN IF NOT EXISTS "via" varchar`.execute(
    db,
  )
  await sql`ALTER TABLE follow ADD COLUMN IF NOT EXISTS "viaCid" varchar`.execute(
    db,
  )
  await sql`ALTER TABLE "like" ADD COLUMN IF NOT EXISTS "takedownRef" varchar`.execute(
    db,
  )
  await sql`ALTER TABLE repost ADD COLUMN IF NOT EXISTS "takedownRef" varchar`.execute(
    db,
  )
  await sql`ALTER TABLE follow ADD COLUMN IF NOT EXISTS "takedownRef" varchar`.execute(
    db,
  )
  await sql`ALTER TABLE actor_block ADD COLUMN IF NOT EXISTS "takedownRef" varchar`.execute(
    db,
  )
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`ALTER TABLE actor_block DROP COLUMN IF EXISTS "takedownRef"`.execute(
    db,
  )
  await sql`ALTER TABLE follow DROP COLUMN IF EXISTS "takedownRef"`.execute(db)
  await sql`ALTER TABLE repost DROP COLUMN IF EXISTS "takedownRef"`.execute(db)
  await sql`ALTER TABLE "like" DROP COLUMN IF EXISTS "takedownRef"`.execute(db)
  await sql`ALTER TABLE follow DROP COLUMN IF EXISTS "viaCid"`.execute(db)
  await sql`ALTER TABLE follow DROP COLUMN IF EXISTS "via"`.execute(db)
}
