import { Kysely, sql } from 'kysely'

// Some deployments were provisioned with a thread_mute table that lacks the
// createdAt column from 20240606T171229898Z-thread-mutes (the migration log
// was seeded without running it), which makes every thread-mute insert fail.
// Idempotent: no-op where the schema is already correct.
export async function up(db: Kysely<unknown>): Promise<void> {
  await sql`
    alter table thread_mute
    add column if not exists "createdAt" varchar not null default now()::varchar
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
