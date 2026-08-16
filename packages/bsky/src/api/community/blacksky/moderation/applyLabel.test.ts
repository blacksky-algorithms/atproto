import { describe, expect, it } from 'vitest'
import { assertPeerModLabelSubject } from './applyLabel.js'

describe(assertPeerModLabelSubject, () => {
  it('refuses permissioned-space subjects', () => {
    expect(() =>
      assertPeerModLabelSubject(
        'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:alice/app.bsky.feed.post/3kpost',
      ),
    ).toThrow('Permissioned-space records cannot be label subjects')
  })

  it('accepts a legacy community-post subject', () => {
    expect(() =>
      assertPeerModLabelSubject(
        'at://did:plc:alice/community.blacksky.feed.post/3kpost',
      ),
    ).not.toThrow()
  })
})
