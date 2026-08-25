import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

/**
 * Space rows must never reach a standard `app.bsky.*` surface.
 *
 * They cannot do so lawfully — a standard `postView.uri` is `format: at-uri`
 * and a space record URI is not one — but the enforcement is a `space_uri IS
 * NULL` predicate in four data-plane queries, and a predicate is easy to drop
 * while refactoring. Interleaving private content into Home is a real future
 * product ask (it needs its own private query using the custom view family,
 * not a relaxed stock lexicon), so this pins the exclusion until that lands.
 *
 * Asserted against the query source rather than a seeded database because the
 * failure being guarded against is textual: someone widening or deleting the
 * clause. A row-level test would only catch it if the fixture happened to hold
 * a space post.
 */

const read = (relative: string) =>
  readFileSync(
    fileURLToPath(
      new URL(`../../src/data-plane/server/${relative}`, import.meta.url),
    ),
    'utf8',
  )

describe('standard surfaces exclude space rows', () => {
  const feeds = read('routes/feeds.ts')
  const community = read('routes/community.ts')

  it('the merged timeline query filters space rows out', () => {
    expect(feeds).toContain('"community_post"."space_uri" IS NULL')
  })

  it('the merged author-feed query filters space rows out', () => {
    expect(feeds).toContain(".where('space_uri', 'is', null)")
  })

  it('the by-actor community list stays null-space only', () => {
    expect(community).toContain('WHERE creator = $1 AND space_uri IS NULL')
  })

  it('the legacy community timeline stays null-space only', () => {
    expect(community).toContain('WHERE space_uri IS NULL')
  })

  it('only the private list reads a space in bulk, and only one named space', () => {
    // Exactly one query lists a space, and it binds that space as a parameter.
    // The other space-reading queries (replies, quotes) widen only to an
    // allowlist their caller has already had authorized, never to "any space".
    const bySpace = community.match(/WHERE space_uri = \$1/g) ?? []
    expect(bySpace).toHaveLength(1)
    const widened = community.match(/space_uri IS NULL OR space_uri = /g) ?? []
    for (const _ of widened) {
      expect(community).toContain('::text[]))')
    }
  })
})
