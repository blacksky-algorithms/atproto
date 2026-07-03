import { Kysely } from 'kysely'

export async function up(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .addColumn('threadgateAllow', 'jsonb')
    .execute()
  await db.schema
    .alterTable('community_post')
    .addColumn('embeddingRules', 'jsonb')
    .execute()
}

export async function down(db: Kysely<unknown>): Promise<void> {
  await db.schema
    .alterTable('community_post')
    .dropColumn('threadgateAllow')
    .execute()
  await db.schema
    .alterTable('community_post')
    .dropColumn('embeddingRules')
    .execute()
}
