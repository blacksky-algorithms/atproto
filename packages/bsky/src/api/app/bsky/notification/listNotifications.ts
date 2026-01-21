import { mapDefined } from '@atproto/common'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { ServerConfig } from '../../../../config'
import { AppContext } from '../../../../context'
import { HydrateCtx, Hydrator } from '../../../../hydration/hydrator'
import { Server } from '../../../../lexicon'
import { isRecord as isPostRecord } from '../../../../lexicon/types/app/bsky/feed/post'
import { QueryParams } from '../../../../lexicon/types/app/bsky/notification/listNotifications'
import {
  HydrationFnInput,
  PresentationFnInput,
  RulesFnInput,
  SkeletonFnInput,
  createPipeline,
} from '../../../../pipeline'
import { Notification } from '../../../../proto/bsky_pb'
import { uriToDid as didFromUri } from '../../../../util/uris'
import { Views } from '../../../../views'
import { resHeaders } from '../../../util'
import { protobufToLex } from './util'

// Timing helper
const timing = (label: string, startTime: number) => {
  const elapsed = Date.now() - startTime
  console.log(`[listNotifications] ${label}: ${elapsed}ms`)
  return elapsed
}

// Build enabled reasons array from user preferences
const getEnabledReasonsFromPreferences = async (
  ctx: AppContext,
  actorDid: string,
): Promise<string[] | undefined> => {
  try {
    const res = await ctx.dataplane.getNotificationPreferences({
      dids: [actorDid],
    })
    if (res.preferences.length !== 1) {
      // No preferences set, return undefined to show all notifications
      return undefined
    }
    const prefs = protobufToLex(res.preferences[0])
    const enabledReasons: string[] = []

    // Map preference keys to notification reasons
    // A reason is enabled if the corresponding preference has list: true
    if (prefs.like?.list || prefs.likeViaRepost?.list) {
      enabledReasons.push('like')
    }
    if (prefs.repost?.list || prefs.repostViaRepost?.list) {
      enabledReasons.push('repost')
    }
    if (prefs.follow?.list) {
      enabledReasons.push('follow')
    }
    if (prefs.reply?.list) {
      enabledReasons.push('reply')
    }
    if (prefs.quote?.list) {
      enabledReasons.push('quote')
    }
    if (prefs.mention?.list) {
      enabledReasons.push('mention')
    }
    if (prefs.starterpackJoined?.list) {
      enabledReasons.push('starterpack-joined')
    }
    if (prefs.verified?.list) {
      enabledReasons.push('verified')
    }
    if (prefs.unverified?.list) {
      enabledReasons.push('unverified')
    }
    if (prefs.subscribedPost?.list) {
      enabledReasons.push('subscribed-post')
    }

    // If all reasons are enabled, return undefined to skip filtering
    // This is an optimization to avoid unnecessary filtering
    const allReasons = [
      'like', 'repost', 'follow', 'reply', 'quote', 'mention',
      'starterpack-joined', 'verified', 'unverified', 'subscribed-post'
    ]
    if (enabledReasons.length === allReasons.length) {
      return undefined
    }

    return enabledReasons.length > 0 ? enabledReasons : undefined
  } catch (err) {
    // If we can't fetch preferences, show all notifications
    console.error('[listNotifications] Failed to fetch preferences:', err)
    return undefined
  }
}

export default function (server: Server, ctx: AppContext) {
  const listNotifications = createPipeline(
    skeleton,
    hydration,
    noBlockOrMutesOrNeedsFiltering,
    presentation,
  )
  server.app.bsky.notification.listNotifications({
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const requestStart = Date.now()
      console.log(`[listNotifications] START viewer=${auth.credentials.iss} limit=${params.limit} priority=${params.priority}`)

      try {
        const viewer = auth.credentials.iss
        const labelers = ctx.reqLabelers(req)
        const hydrateCtx = await ctx.hydrator.createContext({ labelers, viewer })
        timing('createContext', requestStart)

        // If reasons not explicitly provided, apply user's notification preferences
        let effectiveReasons = params.reasons
        if (!effectiveReasons) {
          effectiveReasons = await getEnabledReasonsFromPreferences(ctx, viewer)
          if (effectiveReasons) {
            console.log(`[listNotifications] Applied preferences filter: ${effectiveReasons.join(',')}`)
          }
        }

        const result = await listNotifications(
          { ...params, reasons: effectiveReasons, hydrateCtx: hydrateCtx.copy({ viewer }) },
          ctx,
        )
        const totalTime = timing('TOTAL', requestStart)
        console.log(`[listNotifications] END total=${totalTime}ms items=${result.notifications?.length || 0}`)

        return {
          encoding: 'application/json',
          body: result,
          headers: resHeaders({ labelers: hydrateCtx.labelers }),
        }
      } catch (err) {
        const totalTime = timing('ERROR', requestStart)
        console.error(`[listNotifications] FAILED after ${totalTime}ms:`, err)
        throw err
      }
    },
  })
}

const paginateNotifications = async (opts: {
  ctx: Context
  priority: boolean
  reasons?: string[]
  cursor?: string
  limit: number
  viewer: string
}) => {
  const { ctx, priority, reasons, limit, viewer } = opts

  // if not filtering, then just pass through the response from dataplane
  if (!reasons) {
    const res = await ctx.hydrator.dataplane.getNotifications({
      actorDid: viewer,
      priority,
      cursor: opts.cursor,
      limit,
    })
    return {
      notifications: res.notifications,
      cursor: res.cursor,
    }
  }

  let nextCursor: string | undefined = opts.cursor
  let toReturn: Notification[] = []
  const maxAttempts = 10
  const attemptSize = Math.ceil(limit / 2)
  for (let i = 0; i < maxAttempts; i++) {
    const res = await ctx.hydrator.dataplane.getNotifications({
      actorDid: viewer,
      priority,
      cursor: nextCursor,
      limit,
    })
    const filtered = res.notifications.filter((notif) =>
      reasons.includes(notif.reason),
    )
    toReturn = [...toReturn, ...filtered]
    nextCursor = res.cursor ?? undefined
    if (toReturn.length >= attemptSize || !nextCursor) {
      break
    }
  }
  return {
    notifications: toReturn,
    cursor: nextCursor,
  }
}

/**
 * Applies a configurable delay to the datetime string of a cursor,
 * effectively allowing for a delay on listing the notifications.
 * This is useful to allow time for services to process notifications
 * before they are listed to the user.
 */
export const delayCursor = (
  cursorStr: string | undefined,
  delayMs: number,
): string => {
  const nowMinusDelay = Date.now() - delayMs
  if (cursorStr === undefined) return new Date(nowMinusDelay).toISOString()
  const cursor = new Date(cursorStr).getTime()
  if (isNaN(cursor)) return cursorStr
  return new Date(Math.min(cursor, nowMinusDelay)).toISOString()
}

const skeleton = async (
  input: SkeletonFnInput<Context, Params>,
): Promise<SkeletonState> => {
  const skeletonStart = Date.now()
  const { params, ctx } = input
  console.log(`[listNotifications] skeleton START viewer=${params.hydrateCtx.viewer}`)

  if (params.seenAt) {
    throw new InvalidRequestError('The seenAt parameter is unsupported')
  }

  const originalCursor = params.cursor
  const delayedCursor = delayCursor(
    originalCursor,
    ctx.cfg.notificationsDelayMs,
  )
  const viewer = params.hydrateCtx.viewer

  let t = Date.now()
  const priority = params.priority ?? (await getPriority(ctx, viewer))
  console.log(`[listNotifications] skeleton.getPriority: ${Date.now() - t}ms`)

  t = Date.now()
  const [res, lastSeenRes] = await Promise.all([
    paginateNotifications({
      ctx,
      priority,
      reasons: params.reasons,
      cursor: delayedCursor,
      limit: params.limit,
      viewer,
    }),
    ctx.hydrator.dataplane.getNotificationSeen({
      actorDid: viewer,
      priority,
    }),
  ])
  console.log(`[listNotifications] skeleton.getNotifications+Seen: ${Date.now() - t}ms notifs=${res.notifications.length}`)

  // @NOTE for the first page of results if there's no last-seen time, consider top notification unread
  // rather than all notifications. bit of a hack to be more graceful when seen times are out of sync.
  let lastSeenDate = lastSeenRes.timestamp?.toDate()
  if (!lastSeenDate && !originalCursor) {
    lastSeenDate = res.notifications.at(0)?.timestamp?.toDate()
  }
  console.log(`[listNotifications] skeleton TOTAL: ${Date.now() - skeletonStart}ms`)
  return {
    notifs: res.notifications,
    cursor: res.cursor || undefined,
    priority,
    lastSeenNotifs: lastSeenDate?.toISOString(),
  }
}

const hydration = async (
  input: HydrationFnInput<Context, Params, SkeletonState>,
) => {
  const { skeleton, params, ctx } = input
  return ctx.hydrator.hydrateNotifications(skeleton.notifs, params.hydrateCtx)
}

const noBlockOrMutesOrNeedsFiltering = (
  input: RulesFnInput<Context, Params, SkeletonState>,
) => {
  const { skeleton, hydration, ctx, params } = input
  skeleton.notifs = skeleton.notifs.filter((item) => {
    const did = didFromUri(item.uri)
    if (
      ctx.views.viewerBlockExists(did, hydration) ||
      ctx.views.viewerMuteExists(did, hydration)
    ) {
      return false
    }
    // Filter out hidden replies only if the viewer owns
    // the threadgate and they hid the reply.
    if (item.reason === 'reply') {
      const post = hydration.posts?.get(item.uri)
      if (post) {
        const rootPostUri = isPostRecord(post.record)
          ? post.record.reply?.root.uri
          : undefined
        const isRootPostByViewer =
          rootPostUri && didFromUri(rootPostUri) === params.hydrateCtx?.viewer
        const isHiddenByThreadgate = isRootPostByViewer
          ? ctx.views.replyIsHiddenByThreadgate(
              item.uri,
              rootPostUri,
              hydration,
            )
          : false
        if (isHiddenByThreadgate) {
          return false
        }
      }
    }
    // Filter out notifications from users that have thread hide tags and are from people they
    // are not following
    if (
      item.reason === 'reply' ||
      item.reason === 'quote' ||
      item.reason === 'mention'
    ) {
      const post = hydration.posts?.get(item.uri)
      if (post) {
        for (const [tag] of post.tags.entries()) {
          if (ctx.cfg.threadTagsHide.has(tag)) {
            if (!hydration.profileViewers?.get(did)?.following) {
              return false
            } else {
              break
            }
          }
        }
      }
    }
    // Filter out notifications from users that need review unless moots
    if (
      item.reason === 'reply' ||
      item.reason === 'quote' ||
      item.reason === 'mention' ||
      item.reason === 'like' ||
      item.reason === 'follow'
    ) {
      if (!ctx.views.viewerSeesNeedsReview({ did, uri: item.uri }, hydration)) {
        return false
      }
    }
    return true
  })
  return skeleton
}

const presentation = (
  input: PresentationFnInput<Context, Params, SkeletonState>,
) => {
  const { skeleton, hydration, ctx } = input
  const { notifs, lastSeenNotifs, cursor } = skeleton
  const notifications = mapDefined(notifs, (notif) =>
    ctx.views.notification(notif, lastSeenNotifs, hydration),
  )
  return {
    notifications,
    cursor,
    priority: skeleton.priority,
    seenAt: skeleton.lastSeenNotifs,
  }
}

type Context = {
  hydrator: Hydrator
  views: Views
  cfg: ServerConfig
}

type Params = QueryParams & {
  hydrateCtx: HydrateCtx & { viewer: string }
}

type SkeletonState = {
  notifs: Notification[]
  priority: boolean
  lastSeenNotifs?: string
  cursor?: string
}

const getPriority = async (ctx: Context, did: string) => {
  const actors = await ctx.hydrator.actor.getActors([did], {
    skipCacheForDids: [did],
  })
  return !!actors.get(did)?.priorityNotifications
}
