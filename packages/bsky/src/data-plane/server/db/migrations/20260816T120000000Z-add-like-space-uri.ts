import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema.alterTable('like').addColumn('space_uri', 'varchar').execute()
  await db.schema
    .createIndex('like_space_uri_creator_sort_idx')
    .on('like')
    .columns(['space_uri', 'creator', 'sortAt'])
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('like_space_uri_creator_sort_idx').execute()
  await db.schema.alterTable('like').dropColumn('space_uri').execute()
}
