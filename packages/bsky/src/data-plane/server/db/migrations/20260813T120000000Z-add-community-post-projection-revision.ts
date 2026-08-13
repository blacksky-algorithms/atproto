import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .addColumn('projection_revision', 'varchar')
    .execute()
  await db.schema
    .createIndex('community_post_space_uri_projection_revision_idx')
    .on('community_post')
    .columns(['space_uri', 'uri', 'projection_revision'])
    .unique()
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .dropIndex('community_post_space_uri_projection_revision_idx')
    .execute()
  await db.schema
    .alterTable('community_post')
    .dropColumn('projection_revision')
    .execute()
}
