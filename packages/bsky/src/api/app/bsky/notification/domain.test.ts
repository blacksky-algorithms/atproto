import { describe, expect, test } from 'vitest'
import { classifyNotificationDomain } from './domain.js'

const publicPost = 'at://did:plc:alice/app.bsky.feed.post/3kpublic'
const firstSpace = 'at://did:plc:tenant/space/community.blacksky.feed/first'
const secondSpace = 'at://did:plc:tenant/space/community.blacksky.feed/second'
const firstPost = `${firstSpace}/did:plc:alice/app.bsky.feed.post/3kfirst`
const secondPost = `${secondSpace}/did:plc:alice/app.bsky.feed.post/3ksecond`

describe(classifyNotificationDomain, () => {
  test.each([
    {
      note: 'public record without subject',
      notification: { uri: publicPost },
      expected: { type: 'public' },
    },
    {
      note: 'public record with public subject',
      notification: { uri: publicPost, reasonSubject: publicPost },
      expected: { type: 'public' },
    },
    {
      note: 'space record without subject',
      notification: { uri: firstPost },
      expected: { type: 'space', spaceUri: firstSpace },
    },
    {
      note: 'space record with same-space subject',
      notification: { uri: firstPost, reasonSubject: firstPost },
      expected: { type: 'space', spaceUri: firstSpace },
    },
    {
      note: 'public record with private subject',
      notification: { uri: publicPost, reasonSubject: firstPost },
      expected: { type: 'invalid' },
    },
    {
      note: 'private record with public subject',
      notification: { uri: firstPost, reasonSubject: publicPost },
      expected: { type: 'invalid' },
    },
    {
      note: 'cross-space pair',
      notification: { uri: firstPost, reasonSubject: secondPost },
      expected: { type: 'invalid' },
    },
    {
      note: 'malformed space record',
      notification: {
        uri: 'at://did:plc:tenant/space/community.blacksky.feed/first/broken',
      },
      expected: { type: 'invalid' },
    },
    {
      note: 'malformed ordinary uri',
      notification: { uri: 'not-a-uri' },
      expected: { type: 'invalid' },
    },
  ])('$note', ({ notification, expected }) => {
    expect(classifyNotificationDomain(notification)).toEqual(expected)
  })
})
