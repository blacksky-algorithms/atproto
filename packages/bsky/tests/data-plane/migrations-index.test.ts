import { readFileSync, readdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

// Guards the invariants that make fork/upstream migration merges safe.
//
// Migrations run in sorted-KEY order — the `export * as _<key>` identifier —
// which is independent of the filename. Upstream migrations can carry
// timestamps predating fork migrations that have already run, so they are
// re-keyed to sort last while their files keep upstream's names. That keeps
// the files conflict-free across merges, but it means index.ts is the single
// place where a bad merge can break every deployment's startup.
//
// The failure this is built to catch: a merge that keeps upstream's export
// line alongside the fork's re-keyed one, exporting the same file under two
// keys. The second key is unapplied, so the migration runs a second time and
// fails against objects that already exist.

const MIGRATIONS_DIR = join(
  dirname(fileURLToPath(import.meta.url)),
  '../../src/data-plane/server/db/migrations',
)

const indexSource = readFileSync(join(MIGRATIONS_DIR, 'index.ts'), 'utf8')

const exportLines = [
  ...indexSource.matchAll(
    /export \* as (_\d{8}T\d{9}Z) from '\.\/([^']+)\.js'/g,
  ),
].map(([, key, file]) => ({ key, file }))

const migrationFiles = readdirSync(MIGRATIONS_DIR)
  .filter((f) => /^\d{8}T\d{9}Z-.+\.ts$/.test(f))
  .map((f) => f.replace(/\.ts$/, ''))

describe('migrations index', () => {
  it('exports every migration file exactly once', () => {
    const referenced = exportLines.map((e) => e.file)
    const duplicates = referenced.filter((f, i) => referenced.indexOf(f) !== i)
    // A file exported under two keys runs twice — the exact breakage a
    // careless upstream merge of index.ts produces.
    expect(duplicates).toEqual([])
    expect([...referenced].sort()).toEqual([...migrationFiles].sort())
  })

  it('uses a unique key per migration', () => {
    const keys = exportLines.map((e) => e.key)
    expect(new Set(keys).size).toBe(keys.length)
  })

  it('lists exports in sorted key order', () => {
    const keys = exportLines.map((e) => e.key)
    // Migrations execute in sorted-key order regardless of listing order;
    // keeping the file sorted is what makes "keyed last" reviewable by eye.
    expect(keys).toEqual([...keys].sort())
  })

  it('parses the same number of exports as there are migration files', () => {
    // Catches an export line whose shape the regex above cannot read, which
    // would otherwise make the checks above silently vacuous.
    expect(exportLines).toHaveLength(migrationFiles.length)
    expect(migrationFiles.length).toBeGreaterThan(0)
  })
})
