import type { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .createTable('calendar_event')
    .ifNotExists()
    .addColumn('uri', 'varchar', (col) => col.primaryKey())
    .addColumn('cid', 'varchar', (col) => col.notNull())
    .addColumn('creator', 'varchar', (col) => col.notNull())
    .addColumn('startsAt', 'varchar')
    .addColumn('status', 'varchar')
    .addColumn('indexedAt', 'varchar', (col) => col.notNull())
    .execute()
  await db.schema
    .createIndex('calendar_event_creator_starts_at_idx')
    .ifNotExists()
    .on('calendar_event')
    .columns(['creator', 'startsAt', 'uri'])
    .execute()
  await db.schema
    .createIndex('calendar_event_starts_at_idx')
    .ifNotExists()
    .on('calendar_event')
    .column('startsAt')
    .execute()

  await db.schema
    .createTable('calendar_rsvp')
    .ifNotExists()
    .addColumn('creator', 'varchar', (col) => col.notNull())
    .addColumn('subject', 'varchar', (col) => col.notNull())
    .addColumn('uri', 'varchar', (col) => col.notNull().unique())
    .addColumn('cid', 'varchar', (col) => col.notNull())
    .addColumn('subjectCid', 'varchar')
    .addColumn('status', 'varchar', (col) => col.notNull())
    .addColumn('indexedAt', 'varchar', (col) => col.notNull())
    .addColumn('sortAt', 'varchar', (col) => col.notNull())
    .addPrimaryKeyConstraint('calendar_rsvp_pkey', ['creator', 'subject'])
    .execute()
  await db.schema
    .createIndex('calendar_rsvp_subject_status_sort_at_idx')
    .ifNotExists()
    .on('calendar_rsvp')
    .columns(['subject', 'status', 'sortAt', 'uri'])
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('calendar_rsvp').ifExists().execute()
  await db.schema.dropTable('calendar_event').ifExists().execute()
}
