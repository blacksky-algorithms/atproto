import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .createTable('actor_badge')
    .ifNotExists()
    .addColumn('id', 'bigserial', (col) => col.primaryKey())
    .addColumn('did', 'varchar', (col) => col.notNull())
    .addColumn('badge', 'varchar', (col) => col.notNull())
    .addColumn('issuedBy', 'varchar', (col) => col.notNull())
    .addColumn('createdAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`now()`),
    )
    .addColumn('revokedAt', 'timestamptz')
    .addColumn('revokedBy', 'varchar')
    .execute()

  // One ACTIVE row per (did, badge); revoked rows stay for audit and
  // stop blocking a re-grant.
  await sql`CREATE UNIQUE INDEX IF NOT EXISTS "actor_badge_active_unique"
    ON "actor_badge" ("did", "badge")
    WHERE "revokedAt" IS NULL`.execute(db)

  await sql`CREATE INDEX IF NOT EXISTS "actor_badge_did_idx"
    ON "actor_badge" ("did")
    WHERE "revokedAt" IS NULL`.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('actor_badge').ifExists().execute()
}
