import { type Kysely, sql } from 'kysely'

// scope restrictions: when any is set, just the scoped content is muted;
// when none are set, the subject is fully muted
//
// Retimestamped from upstream's 20260618T200000000Z so it sorts after the
// migrations this deployment has already executed. Kysely's migrator runs in
// sorted-name order and requires executed migrations to form an exact prefix
// of that list, so inserting a new migration underneath applied ones aborts
// the whole run before anything executes.
//
// Idempotent, for the same reason as the fidelity migration: ALTER TABLE
// demands table ownership even with IF NOT EXISTS, so existence is checked
// first via information_schema.
//
// NOTE: the guard makes this migration a no-op once the columns exist; it
// cannot grant privilege. On a deployment where `mute` is not owned by the
// role running migrations, ADD COLUMN still fails with 42501 because the
// columns are genuinely absent. Such deployments must have the columns
// applied out of band by a privileged role first, after which this migration
// no-ops:
//
//   ALTER TABLE mute ADD COLUMN "onlyReposts" boolean NOT NULL DEFAULT false;
//   ALTER TABLE mute ADD COLUMN "onlyQuoteposts" boolean NOT NULL DEFAULT false;
//
// Both are fast defaults on pg11+ (catalog-only, no table rewrite).
const addBoolColumnIfMissing = async (
  db: Kysely<unknown>,
  table: string,
  column: string,
) => {
  const res = await sql<{ n: number }>`
    SELECT count(*)::int AS n FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = ${table} AND column_name = ${column}
  `.execute(db)
  if (res.rows[0]?.n === 0) {
    // NOT NULL DEFAULT false is a fast default on pg11+: no table rewrite,
    // only a brief ACCESS EXCLUSIVE lock to update the catalog.
    await sql`ALTER TABLE ${sql.table(table)} ADD COLUMN ${sql.ref(column)} boolean NOT NULL DEFAULT false`.execute(
      db,
    )
  }
}

export async function up(db: Kysely<unknown>): Promise<void> {
  await addBoolColumnIfMissing(db, 'mute', 'onlyReposts')
  await addBoolColumnIfMissing(db, 'mute', 'onlyQuoteposts')
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`ALTER TABLE mute DROP COLUMN IF EXISTS "onlyQuoteposts"`.execute(db)
  await sql`ALTER TABLE mute DROP COLUMN IF EXISTS "onlyReposts"`.execute(db)
}
