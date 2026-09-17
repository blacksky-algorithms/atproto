import { describe, expect, it, vi } from 'vitest'
import {
  assertLabelSubjectExists,
  assertPeerModLabelSubject,
} from './label-subject.js'

const COMMUNITY_POST = 'at://did:plc:alice/community.blacksky.feed.post/3kpost'
const BSKY_POST = 'at://did:plc:alice/app.bsky.feed.post/3kpost'
const SPACE_POST =
  'at://did:plc:tenant/space/community.blacksky.feed/private/did:plc:alice/app.bsky.feed.post/3kpost'

describe(assertPeerModLabelSubject, () => {
  it('refuses permissioned-space subjects', () => {
    expect(() => assertPeerModLabelSubject(SPACE_POST)).toThrow(
      'Permissioned-space records cannot be label subjects',
    )
  })

  it('accepts a legacy community-post subject', () => {
    expect(() => assertPeerModLabelSubject(COMMUNITY_POST)).not.toThrow()
  })

  it('accepts an app.bsky post subject', () => {
    expect(() => assertPeerModLabelSubject(BSKY_POST)).not.toThrow()
  })

  it('refuses other collections', () => {
    expect(() =>
      assertPeerModLabelSubject('at://did:plc:alice/app.bsky.feed.like/3k'),
    ).toThrow('Subject must be a community or app.bsky post')
  })
})

describe(assertLabelSubjectExists, () => {
  const dataplane = (opts: { communityPost?: unknown; bskyCid?: string }) => ({
    getCommunityPost: vi.fn(async () => ({ post: opts.communityPost })),
    getPostRecords: vi.fn(async () => ({
      records: [{ cid: opts.bskyCid ?? '' }],
    })),
  })

  it('resolves an indexed community post through getCommunityPost', async () => {
    const dp = dataplane({ communityPost: { cid: 'bafy' } })

    await expect(
      assertLabelSubjectExists(dp, COMMUNITY_POST),
    ).resolves.toBeUndefined()
    expect(dp.getCommunityPost).toHaveBeenCalledWith({ uri: COMMUNITY_POST })
    expect(dp.getPostRecords).not.toHaveBeenCalled()
  })

  it('resolves an indexed app.bsky post through getPostRecords', async () => {
    const dp = dataplane({ bskyCid: 'bafy' })

    await expect(
      assertLabelSubjectExists(dp, BSKY_POST),
    ).resolves.toBeUndefined()
    expect(dp.getPostRecords).toHaveBeenCalledWith({ uris: [BSKY_POST] })
    expect(dp.getCommunityPost).not.toHaveBeenCalled()
  })

  it('rejects an app.bsky post the dataplane has not indexed', async () => {
    await expect(
      assertLabelSubjectExists(dataplane({}), BSKY_POST),
    ).rejects.toThrow('Subject post not found')
  })

  it('rejects a missing community post', async () => {
    await expect(
      assertLabelSubjectExists(dataplane({}), COMMUNITY_POST),
    ).rejects.toThrow('Subject post not found')
  })
})
