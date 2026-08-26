import { describe, expect, it } from 'vitest'
import * as FeedDefs from '../../src/lexicons/app/bsky/feed/defs.defs.js'
import * as CommunityFeedDefs from '../../src/lexicons/community/blacksky/feed/defs.defs.js'
import {
  toSpaceFeedViewPost,
  toSpacePostView,
  toSpaceThreadBody,
} from '../../src/api/community/blacksky/views/spaceViews.js'

/**
 * The response contract, checked against the generated lexicons rather than by
 * eye. The defect this closes was invisible for exactly one reason: appview
 * output validation is off outside debug mode, so an invalid body serialised
 * happily and only the client's validator rejected it.
 */

const SPACE = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const POST_URI = `${SPACE}/did:plc:alice/app.bsky.feed.post/3kpost`
const QUOTED_URI = `${SPACE}/did:plc:bob/app.bsky.feed.post/3kquote`
const CID = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

const author = {
  did: 'did:plc:alice',
  handle: 'alice.test',
  labels: [],
}

/** What `buildCommunityPostView` actually emits for a space row. */
const builtPostView = () => ({
  $type: 'app.bsky.feed.defs#postView',
  uri: POST_URI,
  cid: CID,
  author,
  record: {
    $type: 'app.bsky.feed.post',
    text: 'hello from a private space',
    createdAt: '2026-08-18T00:00:00.000Z',
  },
  embed: {
    $type: 'app.bsky.embed.record#view',
    record: {
      $type: 'app.bsky.embed.record#viewRecord',
      uri: QUOTED_URI,
      cid: CID,
      author,
      value: { $type: 'app.bsky.feed.post', text: 'quoted' },
      embeds: undefined,
      labels: [],
      likeCount: 0,
      replyCount: 0,
      repostCount: 0,
      quoteCount: 0,
      indexedAt: '2026-08-18T00:00:00.000Z',
    },
  },
  indexedAt: '2026-08-18T00:00:00.000Z',
  likeCount: 1,
  repostCount: 0,
  replyCount: 0,
  quoteCount: 0,
  bookmarkCount: 0,
  labels: [],
  communitySpace: SPACE,
  viewer: { like: `${SPACE}/did:plc:viewer/app.bsky.feed.like/3klike` },
})

describe('private read contract', () => {
  it('a space post is not a lawful standard postView', () => {
    // The whole reason this endpoint family exists. If this ever passes, the
    // at-uri format check has been loosened somewhere and the boundary is gone.
    const standard = FeedDefs.postView.safeParse(builtPostView())
    expect(standard.success).toBe(false)
  })

  it('the same post validates as a spacePostView', () => {
    const view = toSpacePostView(builtPostView())
    const parsed = CommunityFeedDefs.spacePostView.safeParse(view)
    expect(parsed.success).toBe(true)
    expect(view.$type).toBe('community.blacksky.feed.defs#spacePostView')
    // The real space record URI survives untouched: it is the identifier every
    // later write, like, delete and permalink needs.
    expect(view.uri).toBe(POST_URI)
  })

  it('drops the counters the builder cannot populate', () => {
    const view = toSpacePostView(builtPostView())
    expect(view.repostCount).toBeUndefined()
    expect(view.bookmarkCount).toBeUndefined()
  })

  it('converts a nested quote embed, which also carries a space URI', () => {
    const view = toSpacePostView(builtPostView()) as any
    expect(view.embed.$type).toBe(
      'community.blacksky.feed.defs#spaceRecordView',
    )
    expect(view.embed.record.$type).toBe(
      'community.blacksky.feed.defs#spaceViewRecord',
    )
    expect(view.embed.record.uri).toBe(QUOTED_URI)
  })

  it('a list row validates as a spaceFeedViewPost, reply context included', () => {
    const item = toSpaceFeedViewPost({
      post: builtPostView(),
      reply: { root: builtPostView(), parent: builtPostView() },
    })
    const parsed = CommunityFeedDefs.spaceFeedViewPost.safeParse(item)
    expect(parsed.success).toBe(true)
  })

  it('a thread item validates as a spaceThreadItem', () => {
    const body = toSpaceThreadBody({
      hasOtherReplies: false,
      thread: [
        {
          uri: POST_URI,
          depth: 0,
          value: {
            $type: 'app.bsky.unspecced.defs#threadItemPost',
            post: builtPostView(),
            moreParents: false,
            moreReplies: 0,
            opThread: true,
            hiddenByThreadgate: false,
            mutedByViewer: false,
          },
        },
        {
          uri: POST_URI,
          depth: -1,
          value: { $type: 'app.bsky.unspecced.defs#threadItemNotFound' },
        },
      ],
    }) as any
    for (const item of body.thread) {
      const parsed = CommunityFeedDefs.spaceThreadItem.safeParse(item)
      expect(parsed.success).toBe(true)
    }
  })

  it('a viewer like on a space post is a plain string, not an at-uri', () => {
    // `app.bsky.feed.defs#viewerState.like` is format:at-uri, and a like on a
    // space post is itself written into the space (D28), so it is a space URI.
    const spaceLike = {
      like: `${SPACE}/did:plc:viewer/app.bsky.feed.like/3klike`,
    }
    expect(FeedDefs.viewerState.safeParse(spaceLike).success).toBe(false)
    expect(
      CommunityFeedDefs.spaceViewerState.safeParse(spaceLike).success,
    ).toBe(true)
  })

  it('leaves an ordinary community post lawful under both', () => {
    // The legacy `space_uri IS NULL` path shares the builder and the endpoints;
    // its URIs are ordinary at-uris and must stay valid.
    const legacy = {
      ...builtPostView(),
      uri: 'at://did:plc:alice/community.blacksky.feed.post/3kstub',
      embed: undefined,
      communitySpace: undefined,
      viewer: undefined,
    }
    expect(FeedDefs.postView.safeParse(legacy).success).toBe(true)
    expect(
      CommunityFeedDefs.spacePostView.safeParse(toSpacePostView(legacy))
        .success,
    ).toBe(true)
  })
})
