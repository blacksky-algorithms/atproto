import { AtUri } from '@atproto/syntax'
import { describe, expect, it } from 'vitest'
import {
  isSpaceRecordUri,
  isSpaceUri,
  parseSpaceRecordUri,
  parseSpaceUri,
  spaceOfRecordUri,
  spaceRecordAuthor,
  spaceRecordCollection,
  spaceRecordUri,
  spaceUriOf,
} from '../../src/api/community/blacksky/space-uri'

const AUTHORITY = 'did:plc:communityauthority'
const AUTHOR = 'did:plc:member'
const SPACE = `at://${AUTHORITY}/space/community.blacksky.feed/main`
const RECORD = `${SPACE}/${AUTHOR}/app.bsky.feed.post/3kabc`

describe('space uri parser', () => {
  it('round-trips both forms', () => {
    const space = parseSpaceUri(SPACE)
    expect(space).toEqual({
      spaceDid: AUTHORITY,
      spaceType: 'community.blacksky.feed',
      skey: 'main',
    })
    expect(spaceUriOf(space!)).toBe(SPACE)

    const record = parseSpaceRecordUri(RECORD)
    expect(record).toEqual({
      spaceDid: AUTHORITY,
      spaceType: 'community.blacksky.feed',
      skey: 'main',
      authorDid: AUTHOR,
      collection: 'app.bsky.feed.post',
      rkey: '3kabc',
    })
    expect(spaceRecordUri(record!)).toBe(RECORD)
    expect(spaceOfRecordUri(RECORD)).toBe(SPACE)
  })

  it('recognises both forms and nothing else', () => {
    expect(isSpaceUri(SPACE)).toBe(true)
    expect(isSpaceUri(RECORD)).toBe(true)
    expect(isSpaceRecordUri(RECORD)).toBe(true)
    // The space form is not a record.
    expect(isSpaceRecordUri(SPACE)).toBe(false)
    expect(parseSpaceUri(RECORD)).toBeNull()
    expect(parseSpaceRecordUri(SPACE)).toBeNull()
  })

  it('rejects segment counts either side of the two valid shapes', () => {
    const six = `at://${AUTHORITY}/space/community.blacksky.feed/main/${AUTHOR}/app.bsky.feed.post`
    const eight = `${RECORD}/extra`
    const five = `at://${AUTHORITY}/space/community.blacksky.feed/main/${AUTHOR}`
    for (const uri of [six, eight, five]) {
      expect(isSpaceUri(uri)).toBe(false)
      expect(parseSpaceUri(uri)).toBeNull()
      expect(parseSpaceRecordUri(uri)).toBeNull()
    }
  })

  it('rejects a uri whose marker segment is not `space`', () => {
    const impostor = `at://${AUTHORITY}/spaces/community.blacksky.feed/main`
    expect(isSpaceUri(impostor)).toBe(false)
    expect(parseSpaceUri(impostor)).toBeNull()
  })

  it('rejects malformed dids in either did position', () => {
    expect(
      parseSpaceUri(`at://notadid/space/community.blacksky.feed/main`),
    ).toBeNull()
    expect(
      parseSpaceRecordUri(
        `${SPACE}/notadid/app.bsky.feed.post/3kabc`,
      ),
    ).toBeNull()
    expect(
      parseSpaceUri(`at://did:/space/community.blacksky.feed/main`),
    ).toBeNull()
  })

  it('rejects empty segments', () => {
    expect(parseSpaceUri(`at://${AUTHORITY}/space//main`)).toBeNull()
    expect(parseSpaceUri(`at://${AUTHORITY}/space/type/`)).toBeNull()
    expect(
      parseSpaceRecordUri(`${SPACE}/${AUTHOR}/app.bsky.feed.post/`),
    ).toBeNull()
    expect(parseSpaceRecordUri(`${SPACE}/${AUTHOR}//3kabc`)).toBeNull()
  })

  it('is not fooled by public at-uris or junk', () => {
    for (const uri of [
      `at://${AUTHOR}/app.bsky.feed.post/3kabc`,
      `at://${AUTHOR}/community.blacksky.feed.post/3kabc`,
      'https://example.com/space/a/b',
      'at://',
      '',
      undefined,
      null,
    ]) {
      expect(isSpaceUri(uri)).toBe(false)
      expect(parseSpaceUri(uri)).toBeNull()
      expect(parseSpaceRecordUri(uri)).toBeNull()
    }
  })

  it('takes the author from the uri segment, not the collection position', () => {
    expect(spaceRecordAuthor(RECORD)).toBe(AUTHOR)
    expect(spaceRecordCollection(RECORD)).toBe('app.bsky.feed.post')
    // A like written by someone else into the same space is attributed to them.
    const otherAuthor = `${SPACE}/did:plc:someoneelse/app.bsky.feed.like/3kdef`
    expect(spaceRecordAuthor(otherAuthor)).toBe('did:plc:someoneelse')
    expect(spaceRecordAuthor(SPACE)).toBeNull()
    expect(spaceRecordCollection(SPACE)).toBeNull()
    expect(spaceOfRecordUri(SPACE)).toBeNull()
  })

  it('documents why AtUri cannot be used on these', () => {
    // The reason this module exists: existing tooling does not reject a space
    // URI, it silently misparses it.
    const parsed = new AtUri(RECORD)
    expect(parsed.collection).toBe('space')
    expect(parsed.collection).not.toBe('app.bsky.feed.post')
    expect(parsed.rkey).not.toBe('3kabc')
  })
})
