import { Kysely, sql } from 'kysely'

// Idempotent AND privilege-safe: production already carries these columns
// from out-of-band DDL, and ALTER TABLE demands table ownership even with
// IF NOT EXISTS, so existence is checked first via information_schema.
const addColumnIfMissing = async (
  db: Kysely<unknown>,
  table: string,
  column: string,
) => {
  const res = await sql<{ n: number }>`
    SELECT count(*)::int AS n FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = ${table} AND column_name = ${column}
  `.execute(db)
  if (res.rows[0]?.n === 0) {
    await sql`ALTER TABLE ${sql.table(table)} ADD COLUMN ${sql.ref(column)} varchar`.execute(
      db,
    )
  }
}

export async function up(db: Kysely<unknown>): Promise<void> {
  await addColumnIfMissing(db, 'follow', 'via')
  await addColumnIfMissing(db, 'follow', 'viaCid')
  await addColumnIfMissing(db, 'like', 'takedownRef')
  await addColumnIfMissing(db, 'repost', 'takedownRef')
  await addColumnIfMissing(db, 'follow', 'takedownRef')
  await addColumnIfMissing(db, 'actor_block', 'takedownRef')
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
