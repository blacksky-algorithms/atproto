import { Kysely, sql } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .createTable('notification_push_outbox')
    .addColumn('id', 'varchar', (col) => col.notNull().primaryKey())
    .addColumn('notificationId', 'bigint')
    .addColumn('did', 'varchar', (col) => col.notNull())
    .addColumn('recordUri', 'varchar', (col) => col.notNull())
    .addColumn('recordCid', 'varchar', (col) => col.notNull())
    .addColumn('author', 'varchar', (col) => col.notNull())
    .addColumn('reason', 'varchar', (col) => col.notNull())
    .addColumn('reasonSubject', 'varchar')
    .addColumn('sortAt', 'varchar', (col) => col.notNull())
    .addColumn('courierNotificationId', 'varchar', (col) => col.notNull())
    .addColumn('status', 'varchar', (col) =>
      col.notNull().defaultTo('pending'),
    )
    .addColumn('attempts', 'integer', (col) => col.notNull().defaultTo(0))
    .addColumn('nextAttemptAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`now()`),
    )
    .addColumn('expiresAt', 'timestamptz', (col) => col.notNull())
    .addColumn('lastError', 'varchar')
    .addColumn('createdAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`now()`),
    )
    .addColumn('updatedAt', 'timestamptz', (col) =>
      col.notNull().defaultTo(sql`now()`),
    )
    .execute()

  await sql`
    create index notification_push_outbox_due_idx
    on notification_push_outbox ("nextAttemptAt")
    where status in ('pending', 'retryable')
  `.execute(db)

  await sql`
    create index notification_push_outbox_expires_idx
    on notification_push_outbox ("expiresAt")
    where status in ('pending', 'retryable')
  `.execute(db)

  await sql`
    create or replace function notify_notification_push_insert()
    returns trigger as $$
    begin
      perform pg_notify(
        'notification_push_inserted',
        json_build_object(
          'id', new.id,
          'did', new.did,
          'recordUri', new."recordUri",
          'reason', new.reason,
          'reasonSubject', new."reasonSubject"
        )::text
      );
      return new;
    end;
    $$ language plpgsql
  `.execute(db)

  await sql`
    create trigger notification_push_insert_notify
    after insert on notification
    for each row
    execute function notify_notification_push_insert()
  `.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await sql`
    drop trigger if exists notification_push_insert_notify on notification
  `.execute(db)
  await sql`drop function if exists notify_notification_push_insert()`.execute(db)
  await db.schema.dropTable('notification_push_outbox').execute()
}
