import { describe, expect, it } from 'vitest'
import { NotificationRow } from '../../src/data-plane/server/notification-push-bridge.js'
import {
  PushCopyContext,
  composePushCopy,
  snippetUriForRow,
} from '../../src/data-plane/server/notification-push-copy.js'

const row = (over: Partial<NotificationRow>): NotificationRow => ({
  id: 1,
  did: 'did:plc:recipient',
  recordUri: 'at://did:plc:author/app.bsky.feed.post/3abc',
  recordCid: 'bafy...cid',
  author: 'did:plc:author',
  reason: 'mention',
  reasonSubject: null,
  sortAt: '2026-07-15T00:00:00.000Z',
  ...over,
})

const ctx = (over: Partial<PushCopyContext> = {}): PushCopyContext => ({
  actorsByDid: new Map([
    [
      'did:plc:author',
      { handle: 'alice.blacksky.community', displayName: 'Alice' },
    ],
  ]),
  postTextByUri: new Map([
    [
      'at://did:plc:author/app.bsky.feed.post/3abc',
      'hey @rishi check this out',
    ],
  ]),
  ...over,
})

describe('composePushCopy', () => {
  it('composes a mention with snippet from recordUri', () => {
    expect(composePushCopy(row({ reason: 'mention' }), ctx())).toEqual({
      title: 'Alice',
      message: 'mentioned you: hey @rishi check this out',
    })
  })

  it('composes a reply with snippet from recordUri', () => {
    expect(
      composePushCopy(
        row({
          reason: 'reply',
          reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/3parent',
        }),
        ctx(),
      ).message,
    ).toBe('replied to your post: hey @rishi check this out')
  })

  it('composes a quote with snippet from recordUri', () => {
    expect(
      composePushCopy(
        row({
          reason: 'quote',
          reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/3q',
        }),
        ctx(),
      ).message,
    ).toBe('quoted your post: hey @rishi check this out')
  })

  it('composes a like with snippet from reasonSubject post', () => {
    expect(
      composePushCopy(
        row({
          reason: 'like',
          recordUri: 'at://did:plc:author/app.bsky.feed.like/3l',
          reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/3mine',
        }),
        ctx({
          postTextByUri: new Map([
            [
              'at://did:plc:recipient/app.bsky.feed.post/3mine',
              'my great post',
            ],
          ]),
        }),
      ).message,
    ).toBe('liked your post: my great post')
  })

  it('composes a like with no reasonSubject as a post like without snippet', () => {
    expect(
      composePushCopy(
        row({
          reason: 'like',
          recordUri: 'at://did:plc:author/app.bsky.feed.like/3l',
          reasonSubject: null,
        }),
        ctx(),
      ).message,
    ).toBe('liked your post')
  })

  it('composes a like with a malformed reasonSubject as a post like without snippet', () => {
    expect(
      composePushCopy(
        row({
          reason: 'like',
          recordUri: 'at://did:plc:author/app.bsky.feed.like/3l',
          reasonSubject: 'not-an-at-uri',
        }),
        ctx(),
      ).message,
    ).toBe('liked your post')
  })

  it('composes a feed-generator like without snippet', () => {
    expect(
      composePushCopy(
        row({
          reason: 'like',
          recordUri: 'at://did:plc:author/app.bsky.feed.like/3l',
          reasonSubject:
            'at://did:plc:recipient/app.bsky.feed.generator/cool-feed',
        }),
        ctx(),
      ).message,
    ).toBe('liked your custom feed')
  })

  it('composes a repost with snippet from reasonSubject post', () => {
    expect(
      composePushCopy(
        row({
          reason: 'repost',
          recordUri: 'at://did:plc:author/app.bsky.feed.repost/3r',
          reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/3mine',
        }),
        ctx({
          postTextByUri: new Map([
            [
              'at://did:plc:recipient/app.bsky.feed.post/3mine',
              'my great post',
            ],
          ]),
        }),
      ).message,
    ).toBe('reposted your post: my great post')
  })

  it.each([
    ['follow', 'followed you'],
    ['like-via-repost', 'liked your repost'],
    ['repost-via-repost', 'reposted your repost'],
    ['starterpack-joined', 'signed up with your starter pack'],
    ['verified', 'verified you'],
    ['unverified', 'removed their verification of you'],
  ])('composes %s without snippet', (reason, expected) => {
    expect(composePushCopy(row({ reason }), ctx()).message).toBe(expected)
  })

  it('composes subscribed-post with snippet', () => {
    expect(
      composePushCopy(row({ reason: 'subscribed-post' }), ctx()).message,
    ).toBe('posted: hey @rishi check this out')
  })

  it('falls back to @handle when displayName is missing', () => {
    const c = ctx({
      actorsByDid: new Map([
        [
          'did:plc:author',
          { handle: 'alice.blacksky.community', displayName: null },
        ],
      ]),
    })
    expect(composePushCopy(row({}), c).title).toBe('@alice.blacksky.community')
  })

  it('falls back to Someone when actor is unknown', () => {
    expect(
      composePushCopy(row({}), ctx({ actorsByDid: new Map() })).title,
    ).toBe('Someone')
  })

  it('drops the snippet when post text is missing or empty', () => {
    expect(
      composePushCopy(row({}), ctx({ postTextByUri: new Map() })).message,
    ).toBe('mentioned you')
    expect(
      composePushCopy(
        row({}),
        ctx({
          postTextByUri: new Map([
            ['at://did:plc:author/app.bsky.feed.post/3abc', '   '],
          ]),
        }),
      ).message,
    ).toBe('mentioned you')
  })

  it('collapses whitespace and truncates snippets to 128 chars with ellipsis', () => {
    const text = 'line one\nline two   spaced ' + 'x'.repeat(200)
    const out = composePushCopy(
      row({}),
      ctx({
        postTextByUri: new Map([
          ['at://did:plc:author/app.bsky.feed.post/3abc', text],
        ]),
      }),
    ).message
    expect(out.startsWith('mentioned you: line one line two spaced')).toBe(true)
    expect(out.endsWith('…')).toBe(true)
    expect(out.length).toBeLessThanOrEqual('mentioned you: '.length + 129)
  })

  it('does not split surrogate pairs when truncating snippets', () => {
    // The emoji's surrogate pair straddles the 128-char cut point.
    const text = 'x'.repeat(127) + '😀' + 'tail'
    const out = composePushCopy(
      row({}),
      ctx({
        postTextByUri: new Map([
          ['at://did:plc:author/app.bsky.feed.post/3abc', text],
        ]),
      }),
    ).message
    expect(out.endsWith('…')).toBe(true)
    // No lone surrogate may survive into the push copy.
    expect(/\p{Cs}/u.test(out)).toBe(false)
  })

  it('strips bidi override characters from display names', () => {
    const c = ctx({
      actorsByDid: new Map([
        ['did:plc:author', { handle: 'a.b', displayName: 'Evil\u202eName' }],
      ]),
    })
    expect(composePushCopy(row({}), c).title).toBe('Evil Name')
  })

  it('sanitizes and truncates display names', () => {
    const c = ctx({
      actorsByDid: new Map([
        [
          'did:plc:author',
          {
            handle: 'a.b',
            displayName: ' Evil\nName\u0000 ' + 'y'.repeat(100),
          },
        ],
      ]),
    })
    const title = composePushCopy(row({}), c).title
    expect(title.includes('\n')).toBe(false)
    expect(title.includes('\u0000')).toBe(false)
    expect(title.length).toBeLessThanOrEqual(65)
  })

  it('returns generic copy for unknown reasons', () => {
    expect(composePushCopy(row({ reason: 'bogus' }), ctx())).toEqual({
      title: 'Blacksky',
      message: 'You have a new notification',
    })
  })
})

describe('snippetUriForRow', () => {
  it('uses recordUri for mention, reply, quote, and subscribed-post', () => {
    for (const reason of ['mention', 'reply', 'quote', 'subscribed-post']) {
      expect(snippetUriForRow(row({ reason }))).toBe(
        'at://did:plc:author/app.bsky.feed.post/3abc',
      )
    }
  })

  it('uses reasonSubject for likes and reposts of posts', () => {
    for (const reason of ['like', 'repost']) {
      expect(
        snippetUriForRow(
          row({
            reason,
            reasonSubject: 'at://did:plc:recipient/app.bsky.feed.post/3mine',
          }),
        ),
      ).toBe('at://did:plc:recipient/app.bsky.feed.post/3mine')
    }
  })

  it('returns no uri for non-post, malformed, or missing subjects', () => {
    expect(
      snippetUriForRow(
        row({
          reason: 'like',
          reasonSubject:
            'at://did:plc:recipient/app.bsky.feed.generator/cool-feed',
        }),
      ),
    ).toBeUndefined()
    expect(
      snippetUriForRow(row({ reason: 'like', reasonSubject: 'not-an-at-uri' })),
    ).toBeUndefined()
    expect(snippetUriForRow(row({ reason: 'like' }))).toBeUndefined()
  })

  it('returns no uri for snippet-less reasons', () => {
    for (const reason of ['follow', 'verified', 'starterpack-joined']) {
      expect(snippetUriForRow(row({ reason }))).toBeUndefined()
    }
  })
})
