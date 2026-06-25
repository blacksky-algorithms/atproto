import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  // Peer-mod label attribution — when a community peer-moderator applies a
  // label via community.blacksky.moderation.applyLabel, we record (a) the
  // Ozone event we emitted so reconciliation can correlate, (b) which mod
  // did it so getMyLabels / removeLabel can scope to ownership, (c) the
  // negation timestamp + actor when removed (peer-mod removes their own,
  // or T&S overrides).
  await db.schema
    .createTable('peer_mod_label')
    .ifNotExists()
    .addColumn('id', 'bigserial', (col) => col.primaryKey())
    .addColumn('subjectUri', 'varchar', (col) => col.notNull())
    .addColumn('subjectCid', 'varchar', (col) => col.notNull())
    .addColumn('val', 'varchar', (col) => col.notNull())
    .addColumn('peerModDid', 'varchar', (col) => col.notNull())
    .addColumn('ozoneEventId', 'varchar', (col) => col.notNull().defaultTo(''))
    .addColumn('createdAt', 'varchar', (col) => col.notNull())
    .addColumn('negatedAt', 'varchar')
    .addColumn('negatedBy', 'varchar')
    .addColumn('negationOzoneEventId', 'varchar')
    .execute()

  // Idempotency: a given (subject, val) can have at most one ACTIVE row.
  // Negated rows stay for audit but stop blocking new applications.
  await sql`CREATE UNIQUE INDEX IF NOT EXISTS "peer_mod_label_active_unique"
    ON "peer_mod_label" ("subjectUri", "val")
    WHERE "negatedAt" IS NULL`.execute(db)

  // getMyLabels(subject, peerMod) — primary access path
  await sql`CREATE INDEX IF NOT EXISTS "peer_mod_label_subject_peer_idx"
    ON "peer_mod_label" ("subjectUri", "peerModDid")
    WHERE "negatedAt" IS NULL`.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('peer_mod_label').ifExists().execute()
}
