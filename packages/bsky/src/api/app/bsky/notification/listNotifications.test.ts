import { Timestamp } from '@bufbuild/protobuf'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { canViewSpace } from '../../../community/blacksky/tenant-gate.js'
import { paginateNotifications } from './listNotifications.js'

vi.mock('../../../community/blacksky/tenant-gate.js', () => ({
  canViewSpace: vi.fn(),
}))

const firstSpace = 'at://did:plc:tenant/space/community.blacksky.feed/first'
const secondSpace = 'at://did:plc:tenant/space/community.blacksky.feed/second'

const notification = (uri: string, index: number) => ({
  recipientDid: 'did:plc:viewer',
  uri,
  reason: 'mention',
  reasonSubject: uri,
  timestamp: Timestamp.fromDate(new Date(Date.UTC(2026, 8, 1, 0, 0, index))),
  priority: false,
})

const publicUri = (index: number) =>
  `at://did:plc:alice/app.bsky.feed.post/public-${index}`

const spaceUri = (space: string, index: number) =>
  `${space}/did:plc:alice/app.bsky.feed.post/private-${index}`

const contextFor = (notifications: ReturnType<typeof notification>[]) => {
  const getNotifications = vi.fn(
    async (req: { cursor?: string; limit: number }) => {
      const offset = req.cursor
        ? notifications.findIndex(
            (item) => item.timestamp?.toDate().toISOString() === req.cursor,
          ) + 1
        : 0
      const page = notifications.slice(offset, offset + req.limit)
      return {
        notifications: page,
        cursor: page.at(-1)?.timestamp?.toDate().toISOString(),
      }
    },
  )
  return {
    ctx: { hydrator: { dataplane: { getNotifications } } } as any,
    getNotifications,
  }
}

describe(paginateNotifications, () => {
  beforeEach(() => {
    vi.mocked(canViewSpace).mockReset()
  })

  it('keeps mixed notifications ordered and owns the last consumed cursor', async () => {
    const rows = [
      notification(publicUri(0), 0),
      notification(spaceUri(secondSpace, 1), 1),
      notification(spaceUri(firstSpace, 2), 2),
      notification(publicUri(3), 3),
      notification(publicUri(4), 4),
    ]
    const { ctx, getNotifications } = contextFor(rows)
    vi.mocked(canViewSpace).mockImplementation(async (_ctx, space) =>
      Promise.resolve(space === firstSpace),
    )

    const first = await paginateNotifications({
      ctx,
      priority: false,
      limit: 3,
      viewer: 'did:plc:viewer',
      mode: 'authorized-union',
    })
    expect(first.notifications.map((item) => item.uri)).toEqual([
      rows[0].uri,
      rows[2].uri,
      rows[3].uri,
    ])
    expect(first.cursor).toBe(rows[3].timestamp?.toDate().toISOString())
    expect(getNotifications.mock.calls[0][0]).toMatchObject({
      includeSpaceNotifications: true,
    })

    const second = await paginateNotifications({
      ctx,
      priority: false,
      cursor: first.cursor,
      limit: 3,
      viewer: 'did:plc:viewer',
      mode: 'authorized-union',
    })
    expect(second.notifications.map((item) => item.uri)).toEqual([rows[4].uri])
  })

  it('returns an advancing empty page when the denied scan reaches its cap', async () => {
    const rows = Array.from({ length: 1_001 }, (_, index) =>
      index < 1_000
        ? notification(spaceUri(firstSpace, index), index)
        : notification(publicUri(index), index),
    )
    const { ctx } = contextFor(rows)
    vi.mocked(canViewSpace).mockResolvedValue(false)

    const denied = await paginateNotifications({
      ctx,
      priority: false,
      limit: 1,
      viewer: 'did:plc:viewer',
      mode: 'authorized-union',
    })
    expect(denied.notifications).toEqual([])
    expect(denied.cursor).toBe(rows[999].timestamp?.toDate().toISOString())

    const continued = await paginateNotifications({
      ctx,
      priority: false,
      cursor: denied.cursor,
      limit: 1,
      viewer: 'did:plc:viewer',
      mode: 'authorized-union',
    })
    expect(continued.notifications.map((item) => item.uri)).toEqual([
      rows[1_000].uri,
    ])
  })

  it('does not authorize spaces in public-only mode', async () => {
    const { ctx, getNotifications } = contextFor([
      notification(publicUri(0), 0),
    ])
    const result = await paginateNotifications({
      ctx,
      priority: false,
      limit: 1,
      viewer: 'did:plc:viewer',
      mode: 'public-only',
    })
    expect(result.notifications).toHaveLength(1)
    expect(canViewSpace).not.toHaveBeenCalled()
    expect(getNotifications.mock.calls[0][0]).toMatchObject({
      includeSpaceNotifications: false,
    })
  })
})
