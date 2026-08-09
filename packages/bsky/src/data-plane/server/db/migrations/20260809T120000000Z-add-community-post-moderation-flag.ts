import { Kysely } from 'kysely'

// A moderated community post is flagged, never deleted. The author's own copy
// lives in their repo and is not ours to remove, and reversal has to be a
// local state flip rather than a re-fetch, so the row stays and the read paths
// skip it.
export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .addColumn('moderation_flagged_at', 'varchar')
    .execute()
  // Every read path filters on the flag and orders by sortAt, so index the
  // pair. Leading with the flag keeps the common (unflagged) case contiguous.
  await db.schema
    .createIndex('community_post_unflagged_sort_idx')
    .on('community_post')
    .columns(['moderation_flagged_at', 'sortAt'])
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema.dropIndex('community_post_unflagged_sort_idx').execute()
  await db.schema
    .alterTable('community_post')
    .dropColumn('moderation_flagged_at')
    .execute()
}
