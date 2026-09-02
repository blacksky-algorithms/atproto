import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { runNotificationList } from '../../src/api/app/bsky/notification/listNotifications.js'
import registerCommunityNotificationList from '../../src/api/community/blacksky/notification/listNotifications.js'
import * as StandardUnreadCount from '../../src/lexicons/app/bsky/notification/getUnreadCount.js'
import * as StandardNotifications from '../../src/lexicons/app/bsky/notification/listNotifications.js'
import * as CommunityUnreadCount from '../../src/lexicons/community/blacksky/notification/getUnreadCount.js'
import * as CommunityNotifications from '../../src/lexicons/community/blacksky/notification/listNotifications.js'

vi.mock('../../src/api/app/bsky/notification/listNotifications.js', () => ({
  runNotificationList: vi.fn(),
}))

const space = 'at://did:plc:tenant/space/community.blacksky.feed/private'
const privateUri = `${space}/did:plc:alice/app.bsky.feed.post/3kprivate`
const cid = 'bafyreiacsg6vsw7ppwbnowzsdgstulhrwftirtcnvkcbnfgvhwjrnzfmsu'

const privateNotification = {
  uri: privateUri,
  cid,
  author: { did: 'did:plc:alice', handle: 'alice.test' },
  reason: 'mention',
  reasonSubject: privateUri,
  record: {
    $type: 'app.bsky.feed.post',
    text: 'private',
    createdAt: '2026-09-01T00:00:00.000Z',
  },
  isRead: false,
  indexedAt: '2026-09-01T00:00:00.000Z',
}

const pipelinePrivateNotification = {
  ...privateNotification,
  $type: 'app.bsky.notification.listNotifications#notification' as const,
}

const publicNotification = {
  ...privateNotification,
  uri: 'at://did:plc:alice/app.bsky.feed.post/3kpublic',
  reasonSubject: 'at://did:plc:alice/app.bsky.feed.post/3kpublic',
}

const completeBody = (notification: typeof privateNotification) => ({
  cursor: '2026-08-31T23:59:59.000Z',
  notifications: [notification],
  priority: true,
  seenAt: '2026-09-01T00:00:00.000Z',
})

const readLexicon = (path: string) =>
  JSON.parse(
    readFileSync(fileURLToPath(new URL(path, import.meta.url)), 'utf8'),
  )

describe('private notification contract', () => {
  beforeEach(() => {
    vi.mocked(runNotificationList).mockReset()
  })

  it('accepts a complete private response only on the Blacksky schema', () => {
    const body = completeBody(privateNotification)

    expect(CommunityNotifications.$output.schema.$safeParse(body).success).toBe(
      true,
    )
    expect(StandardNotifications.$output.schema.$safeParse(body).success).toBe(
      false,
    )
  })

  it('keeps complete public responses valid on both schemas', () => {
    const body = completeBody(publicNotification)

    expect(StandardNotifications.$output.schema.$safeParse(body).success).toBe(
      true,
    )
    expect(CommunityNotifications.$output.schema.$safeParse(body).success).toBe(
      true,
    )
  })

  it('serializes the custom route without the standard notification discriminator', async () => {
    let route: any
    const server = {
      add: vi.fn((_schema, config) => {
        route = config
      }),
    }
    const ctx = {
      authVerifier: { standard: vi.fn() },
      reqLabelers: vi.fn(() => undefined),
      hydrator: {
        createContext: vi.fn(async () => ({ labelers: undefined })),
      },
    } as any
    vi.mocked(runNotificationList).mockResolvedValue(
      completeBody(pipelinePrivateNotification) as any,
    )
    registerCommunityNotificationList(server as any, ctx)

    const response = await route.handler({
      params: { limit: 50 },
      auth: { credentials: { iss: 'did:plc:viewer' } },
      req: {},
    })
    const wireBody = JSON.parse(JSON.stringify(response.body))

    expect(runNotificationList).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 50 }),
      ctx,
      'authorized-union',
    )
    expect(wireBody).toMatchObject({
      cursor: '2026-08-31T23:59:59.000Z',
      priority: true,
      seenAt: '2026-09-01T00:00:00.000Z',
    })
    expect(wireBody.notifications[0]).not.toHaveProperty('$type')
    expect(
      CommunityNotifications.$output.schema.$safeParse(wireBody).success,
    ).toBe(true)
    expect(
      StandardNotifications.$output.schema.$safeParse(wireBody).success,
    ).toBe(false)
  })

  it('mirrors standard list and unread-count query parameter contracts', () => {
    const standardList = readLexicon(
      '../../../../lexicons/app/bsky/notification/listNotifications.json',
    )
    const communityList = readLexicon(
      '../../../../lexicons/community/blacksky/notification/listNotifications.json',
    )
    const standardCount = readLexicon(
      '../../../../lexicons/app/bsky/notification/getUnreadCount.json',
    )
    const communityCount = readLexicon(
      '../../../../lexicons/community/blacksky/notification/getUnreadCount.json',
    )

    expect(communityList.defs.main.parameters).toEqual(
      standardList.defs.main.parameters,
    )
    expect(communityCount.defs.main.parameters).toEqual(
      standardCount.defs.main.parameters,
    )

    for (const params of [
      StandardNotifications.$params,
      CommunityNotifications.$params,
    ]) {
      expect(params.$safeParse({ limit: 0 }).success).toBe(false)
      expect(params.$safeParse({ limit: 101 }).success).toBe(false)
      expect(
        params.$safeParse({
          reasons: ['mention'],
          limit: 100,
          priority: true,
          cursor: 'cursor',
          seenAt: '2026-09-01T00:00:00.000Z',
        }).success,
      ).toBe(true)
    }
    for (const params of [
      StandardUnreadCount.$params,
      CommunityUnreadCount.$params,
    ]) {
      expect(
        params.$safeParse({
          priority: true,
          seenAt: '2026-09-01T00:00:00.000Z',
        }).success,
      ).toBe(true)
      expect(params.$safeParse({ seenAt: 'not-a-datetime' }).success).toBe(
        false,
      )
    }
  })
})
