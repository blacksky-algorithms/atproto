import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .createTable('community_post_moderation_action')
    .addColumn('actionUri', 'varchar', (col) => col.primaryKey())
    .addColumn('postUri', 'varchar', (col) => col.notNull())
    .addColumn('postCid', 'varchar', (col) => col.notNull())
    .addColumn('active', 'boolean', (col) => col.notNull().defaultTo(true))
    .execute()
  await db.schema
    .createIndex('community_post_moderation_action_active_idx')
    .on('community_post_moderation_action')
    .columns(['postUri', 'postCid', 'active'])
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropTable('community_post_moderation_action').execute()
}
