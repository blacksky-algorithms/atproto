import { type Kysely, sql } from 'kysely'

// Some deployments were provisioned with a thread_mute table that lacks the
// createdAt column from 20240606T171229898Z-thread-mutes (the migration log
// was seeded without running it), which makes every thread-mute insert fail.
//
// Idempotent AND privilege-safe: ALTER TABLE demands table ownership even
// with IF NOT EXISTS — Postgres checks ownership before it checks existence —
// so an unconditional statement fails with 42501 on deployments where
// thread_mute was provisioned out of band. Existence is therefore checked via
// information_schema first, and the table is only touched when the column is
// genuinely absent (the case where this deployment is the one creating it,
// and so owns the result).
export async function up(db: Kysely<unknown>): Promise<void> {
  const res = await sql<{ n: number }>`
    SELECT count(*)::int AS n FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = 'thread_mute'
      AND column_name = 'createdAt'
  `.execute(db)
  if (res.rows[0]?.n !== 0) return
  // Added with a default so existing rows backfill, then dropped so the
  // column matches the canonical schema. Both statements are safe here
  // because reaching them means the column did not exist.
  await sql`
    alter table thread_mute
    add column "createdAt" varchar not null default now()::varchar
  `.execute(db)
  await sql`
    alter table thread_mute
    alter column "createdAt" drop default
  `.execute(db)
}

export async function down(): Promise<void> {
  // No-op: the column is part of the canonical schema (see
  // 20240606T171229898Z-thread-mutes); dropping it would break inserts.
}
