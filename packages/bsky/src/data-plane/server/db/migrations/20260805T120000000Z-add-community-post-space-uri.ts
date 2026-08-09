import { Kysely } from 'kysely'

// A space-backed post is discriminated by this column being set; NULL is the
// existing Blacksky community-only feed, untouched. Keyed by space rather than
// by feed because a feed is a view over a space: the same post can appear in
// several feeds, and the access decision belongs to the space.
export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .addColumn('space_uri', 'varchar')
    .execute()
  await db.schema
    .createIndex('community_post_space_uri_idx')
    .on('community_post')
    .column('space_uri')
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('community_post_space_uri_idx').execute()
  await db.schema.alterTable('community_post').dropColumn('space_uri').execute()
}
