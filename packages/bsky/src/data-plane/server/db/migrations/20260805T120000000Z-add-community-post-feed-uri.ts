import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .addColumn('feed_uri', 'varchar')
    .execute()
  await db.schema
    .createIndex('community_post_feed_uri_idx')
    .on('community_post')
    .column('feed_uri')
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('community_post_feed_uri_idx').execute()
  await db.schema.alterTable('community_post').dropColumn('feed_uri').execute()
}
