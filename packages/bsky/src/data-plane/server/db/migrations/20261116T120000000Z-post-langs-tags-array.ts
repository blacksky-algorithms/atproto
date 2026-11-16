import { type Kysely, sql } from 'kysely'

const columns = ['langs', 'tags']

// @NOTE some deployments already carry these columns as varchar[] from
// out-of-band DDL, and ALTER TABLE demands table ownership, so the current
// type is checked first.
async function columnType(
  db: Kysely<unknown>,
  column: string,
): Promise<string | undefined> {
  const res = await sql<{ type: string }>`
    SELECT udt_name AS type FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'post' AND column_name = ${column}
  `.execute(db)
  return res.rows[0]?.type
}

export async function up(db: Kysely<unknown>): Promise<void> {
  const pending: string[] = []
  for (const column of columns) {
    if ((await columnType(db, column)) === 'jsonb') pending.push(column)
  }
  if (pending.length === 0) return

  // @NOTE a USING clause cannot contain a subquery, hence the helper function
  await sql`
    CREATE FUNCTION pg_temp.jsonb_to_varchar_array(jsonb) RETURNS varchar[]
    LANGUAGE sql IMMUTABLE AS $$
      SELECT CASE WHEN jsonb_typeof($1) = 'array'
        THEN ARRAY(SELECT jsonb_array_elements_text($1))::varchar[]
      END
    $$
  `.execute(db)
  for (const column of pending) {
    await sql`
      ALTER TABLE post ALTER COLUMN ${sql.ref(column)} TYPE varchar[]
      USING pg_temp.jsonb_to_varchar_array(${sql.ref(column)})
    `.execute(db)
  }
  await sql`DROP FUNCTION pg_temp.jsonb_to_varchar_array(jsonb)`.execute(db)
}

export async function down(db: Kysely<unknown>): Promise<void> {
  for (const column of columns) {
    if ((await columnType(db, column)) !== '_varchar') continue
    await sql`
      ALTER TABLE post ALTER COLUMN ${sql.ref(column)} TYPE jsonb
      USING to_jsonb(${sql.ref(column)})
    `.execute(db)
  }
}
