import { beforeEach, describe, expect, it, vi } from 'vitest'
import { canViewSpace } from '../../../community/blacksky/tenant-gate.js'
import { runNotificationCount } from './getUnreadCount.js'

vi.mock('../../../community/blacksky/tenant-gate.js', () => ({
  canViewSpace: vi.fn(),
}))

const firstSpace = 'at://did:plc:tenant/space/community.blacksky.feed/first'
const secondSpace = 'at://did:plc:tenant/space/community.blacksky.feed/second'
const viewer = 'did:plc:viewer'

const context = () => {
  const getUnreadNotificationSpaces = vi.fn(async () => ({
    spaces: [
      { postUri: `${firstSpace}/post/one`, spaceUri: firstSpace },
      { postUri: `${firstSpace}/post/two`, spaceUri: firstSpace },
      { postUri: `${secondSpace}/post/three`, spaceUri: secondSpace },
    ],
  }))
  const getUnreadNotificationCount = vi.fn(
    async ({ allowedSpaceUris }: { allowedSpaceUris: string[] }) => ({
      count: 2 + allowedSpaceUris.length,
    }),
  )
  return {
    ctx: {
      hydrator: {
        dataplane: {
          getUnreadNotificationSpaces,
          getUnreadNotificationCount,
        },
      },
    } as any,
    getUnreadNotificationSpaces,
    getUnreadNotificationCount,
  }
}

describe(runNotificationCount, () => {
  beforeEach(() => {
    vi.mocked(canViewSpace).mockReset()
  })

  it('deduplicates candidates and counts public plus authorized spaces', async () => {
    const { ctx, getUnreadNotificationCount } = context()
    vi.mocked(canViewSpace).mockImplementation(async (_ctx, spaceUri) =>
      Promise.resolve(spaceUri === firstSpace),
    )

    await expect(
      runNotificationCount({ viewer }, ctx, 'authorized-union'),
    ).resolves.toEqual({ count: 3 })
    expect(canViewSpace).toHaveBeenCalledTimes(2)
    expect(canViewSpace).toHaveBeenCalledWith(ctx, firstSpace, viewer)
    expect(canViewSpace).toHaveBeenCalledWith(ctx, secondSpace, viewer)
    expect(getUnreadNotificationCount).toHaveBeenCalledWith({
      actorDid: viewer,
      priority: false,
      allowedSpaceUris: [firstSpace],
    })
  })

  it('fails closed when one space authorization throws', async () => {
    const { ctx, getUnreadNotificationCount } = context()
    vi.mocked(canViewSpace).mockImplementation(async (_ctx, spaceUri) => {
      if (spaceUri === firstSpace) throw new Error('authorization unavailable')
      return true
    })

    await expect(
      runNotificationCount({ viewer }, ctx, 'authorized-union'),
    ).resolves.toEqual({ count: 3 })
    expect(getUnreadNotificationCount).toHaveBeenCalledWith({
      actorDid: viewer,
      priority: false,
      allowedSpaceUris: [secondSpace],
    })
  })

  it('makes no candidate or authorization calls in public-only mode', async () => {
    const { ctx, getUnreadNotificationSpaces, getUnreadNotificationCount } =
      context()

    await expect(
      runNotificationCount({ viewer, priority: true }, ctx, 'public-only'),
    ).resolves.toEqual({ count: 2 })
    expect(getUnreadNotificationSpaces).not.toHaveBeenCalled()
    expect(canViewSpace).not.toHaveBeenCalled()
    expect(getUnreadNotificationCount).toHaveBeenCalledWith({
      actorDid: viewer,
      priority: true,
      allowedSpaceUris: [],
    })
  })
})
