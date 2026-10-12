import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

/**
 * A legacy community post's deletion and moderation flag live in
 * `community_post`, not `record`. The notification list and the push bridge
 * must both judge a repost (or like) of one by that row, and a via-repost
 * reason by the subject of the repost it names. Pinned against the query
 * source because the failure mode is textual: an arm quietly dropped while
 * refactoring the CASE would silently fall back to the record store.
 */

const read = (relative: string) =>
  readFileSync(
    fileURLToPath(
      new URL(`../../src/data-plane/server/${relative}`, import.meta.url),
    ),
    'utf8',
  )

describe('notification subject eligibility for community posts', () => {
  const notifs = read('routes/notifs.ts')
  const bridge = read('notification-push-bridge.ts')

  it('the list resolves a stub subject through community_post', () => {
    expect(notifs).toContain(
      'WHEN \'community.blacksky.feed.post\' THEN EXISTS (\n              SELECT 1 FROM community_post cp\n              WHERE cp.uri = notif."reasonSubject"\n                AND cp.moderation_flagged_at IS NULL',
    )
  })

  it('the list judges a via-repost subject by the repost subject', () => {
    expect(notifs).toContain(
      "rp.subject NOT LIKE '%/community.blacksky.feed.post/%' OR EXISTS (",
    )
    expect(notifs).toContain('WHERE cp.uri = rp.subject')
  })

  it('the push bridge suppresses rows whose stub subject is gone or flagged', () => {
    expect(bridge).toContain('findRowsWithIneligibleStub')
    expect(bridge).toContain('ineligibleStubRows.has(row) ||')
    expect(bridge).toContain(".where('moderation_flagged_at', 'is', null)")
  })
})
