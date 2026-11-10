import { mapDefined } from '@atproto/common'
import type { AtUriString, DatetimeString } from '@atproto/syntax'
import { InvalidRequestError, type Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import type {
  HydrateCtxWithViewer,
  Hydrator,
} from '../../../../hydration/hydrator.js'
import { app } from '../../../../lexicons/index.js'
import {
  type HydrationFnInput,
  type PresentationFnInput,
  type RulesFnInput,
  type SkeletonFnInput,
  createPipeline,
} from '../../../../pipeline.js'
import type { Notification } from '../../../../proto/bsky_pb.js'
import { uriToAuthorDid } from '../../../../util/uris.js'
import { isPostRecordType } from '../../../../views/types.js'
import { canViewSpace } from '../../../community/blacksky/tenant-gate.js'
import { fillPage, resHeaders } from '../../../util.js'
import { classifyNotificationDomain } from './domain.js'
import {
  delayCursor,
  protobufToLex,
  shouldFilterForNeedsReview,
  shouldFilterHiddenThreadTag,
  shouldFilterReplyByThreadgate,
} from './util.js'

const ALL_NOTIFICATION_REASONS_COUNT = 12
const AUTHORIZED_UNION_SCAN_CAP = 1_000
const NOTIFICATION_BATCH_SIZE = 100

export type NotificationListMode = 'public-only' | 'authorized-union'

export type PreferenceFilters = {
  reasons?: string[]
  followsOnlyReasons: Set<string>
}

export const getPreferenceFilters = async (
  hydrator: Hydrator,
  actorDid: string,
): Promise<PreferenceFilters> => {
  try {
    const res = await hydrator.dataplane.getNotificationPreferences({
      dids: [actorDid],
    })
    if (res.preferences.length !== 1) return { followsOnlyReasons: new Set() }
    const prefs = protobufToLex(res.preferences[0])
    const enabled: string[] = []
    if (prefs.like.list) enabled.push('like')
    if (prefs.likeViaRepost.list) enabled.push('like-via-repost')
    if (prefs.repost.list) enabled.push('repost')
    if (prefs.repostViaRepost.list) enabled.push('repost-via-repost')
    if (prefs.follow.list) enabled.push('follow')
    if (prefs.reply.list) enabled.push('reply')
    if (prefs.quote.list) enabled.push('quote')
    if (prefs.mention.list) enabled.push('mention')
    if (prefs.starterpackJoined.list) enabled.push('starterpack-joined')
    if (prefs.verified.list) enabled.push('verified')
    if (prefs.unverified.list) enabled.push('unverified')
    if (prefs.subscribedPost.list) enabled.push('subscribed-post')
    // per-reason `include: 'follows'`, keyed by notification row reason
    const followsOnlyReasons = new Set<string>()
    if (prefs.follow.include === 'follows') followsOnlyReasons.add('follow')
    if (prefs.like.include === 'follows') followsOnlyReasons.add('like')
    if (prefs.likeViaRepost.include === 'follows') {
      followsOnlyReasons.add('like-via-repost')
    }
    if (prefs.mention.include === 'follows') followsOnlyReasons.add('mention')
    if (prefs.quote.include === 'follows') followsOnlyReasons.add('quote')
    if (prefs.reply.include === 'follows') followsOnlyReasons.add('reply')
    if (prefs.repost.include === 'follows') followsOnlyReasons.add('repost')
    if (prefs.repostViaRepost.include === 'follows') {
      followsOnlyReasons.add('repost-via-repost')
    }
    return {
      reasons:
        enabled.length === ALL_NOTIFICATION_REASONS_COUNT ? undefined : enabled,
      followsOnlyReasons,
    }
  } catch {
    return { followsOnlyReasons: new Set() }
  }
}

export default function (server: Server, ctx: AppContext) {
  server.add(app.bsky.notification.listNotifications, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        labelers,
        viewer,
        xrpcEndpoint: app.bsky.notification.listNotifications.$lxm,
      })
      const result = await runNotificationList(
        { ...params, hydrateCtx },
        ctx,
        'public-only',
      )
      return {
        encoding: 'application/json',
        body: result,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}

export async function runNotificationList(
  params: Omit<Params, 'mode' | 'lastSeen'>,
  ctx: AppContext,
  mode: NotificationListMode,
) {
  if (params.seenAt) {
    throw new InvalidRequestError('The seenAt parameter is unsupported')
  }
  const viewer = params.hydrateCtx.viewer
  const lastSeenRes = await ctx.hydrator.dataplane.getNotificationSeen({
    actorDid: viewer,
  })
  const lastSeen = lastSeenRes.timestamp?.toDate()

  const listNotifications = createPipeline(
    skeleton,
    hydration,
    noBlockOrMutesOrNeedsFiltering,
    presentation,
  )
  return await fillPage({
    cursor: params.cursor,
    limit: params.limit,
    fetch: ({ cursor, limit }) =>
      listNotifications({ ...params, cursor, limit, lastSeen, mode }, ctx),
    items: (r) => r.notifications,
  })
}

export async function paginateNotifications(opts: {
  ctx: Context
  reasons?: string[]
  cursor?: string
  limit: number
  viewer: string
  mode: NotificationListMode
}) {
  const { ctx, reasons, limit, viewer, mode } = opts
  if (mode === 'public-only' && !reasons) {
    const res = await ctx.hydrator.dataplane.getNotifications({
      actorDid: viewer,
      cursor: opts.cursor,
      limit,
      includeSpaceNotifications: false,
    })
    return {
      notifications: res.notifications,
      cursor: res.cursor,
    }
  }

  let nextCursor: string | undefined = opts.cursor
  const toReturn: Notification[] = []
  const access = new Map<string, Promise<boolean>>()
  let consumed = 0
  let consumedCursor: string | undefined

  while (consumed < AUTHORIZED_UNION_SCAN_CAP) {
    const batchLimit = Math.min(
      NOTIFICATION_BATCH_SIZE,
      AUTHORIZED_UNION_SCAN_CAP - consumed,
    )
    const res = await ctx.hydrator.dataplane.getNotifications({
      actorDid: viewer,
      cursor: nextCursor,
      limit: batchLimit,
      includeSpaceNotifications: mode === 'authorized-union',
    })
    if (res.notifications.length === 0) {
      return { notifications: toReturn, cursor: undefined }
    }

    const domains = res.notifications.map(classifyNotificationDomain)
    if (mode === 'authorized-union') {
      for (const domain of domains) {
        if (domain.type === 'space' && !access.has(domain.spaceUri)) {
          access.set(
            domain.spaceUri,
            canViewSpace(ctx, domain.spaceUri, viewer),
          )
        }
      }
    }

    for (const [index, notification] of res.notifications.entries()) {
      consumed++
      consumedCursor = notification.timestamp?.toDate().toISOString()
      const domain = domains[index]
      let allowed = domain.type === 'public'
      if (mode === 'authorized-union' && domain.type === 'space') {
        const decision = access.get(domain.spaceUri)
        allowed = decision ? await decision : false
      }
      if (allowed && (!reasons || reasons.includes(notification.reason))) {
        toReturn.push(notification)
      }
      if (toReturn.length >= limit) {
        return { notifications: toReturn, cursor: consumedCursor }
      }
      if (consumed >= AUTHORIZED_UNION_SCAN_CAP) break
    }

    nextCursor = res.cursor ?? undefined
    if (!nextCursor || res.notifications.length < batchLimit) {
      return { notifications: toReturn, cursor: undefined }
    }
  }
  return {
    notifications: toReturn,
    cursor: consumedCursor,
  }
}

const skeleton = async (
  input: SkeletonFnInput<Context, Params>,
): Promise<SkeletonState> => {
  const { params, ctx } = input
  const originalCursor = params.cursor
  const delayedCursor = delayCursor(
    originalCursor,
    ctx.cfg.notificationsDelayMs,
  )
  const viewer = params.hydrateCtx.viewer
  const prefFilters = await getPreferenceFilters(ctx.hydrator, viewer)
  const reasons = params.reasons ?? prefFilters.reasons
  const res = await paginateNotifications({
    ctx,
    reasons,
    cursor: delayedCursor,
    limit: params.limit,
    viewer,
    mode: params.mode,
  })
  // @NOTE for the first page of results if there's no last-seen time, consider top notification unread
  // rather than all notifications. bit of a hack to be more graceful when seen times are out of sync.
  let lastSeenDate = params.lastSeen
  if (!lastSeenDate && !originalCursor) {
    lastSeenDate = res.notifications.at(0)?.timestamp?.toDate()
  }
  return {
    notifs: res.notifications,
    cursor: res.cursor || undefined,
    followsOnlyReasons: prefFilters.followsOnlyReasons,
    lastSeenNotifs: lastSeenDate
      ? (lastSeenDate.toISOString() as DatetimeString)
      : undefined,
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
    const uri = item.uri as AtUriString
    // The author, not the uri's authority: a space record's authority is the
    // space, so blocks and mutes would be checked against the wrong actor.
    const did = uriToAuthorDid(uri)
    if (
      ctx.views.viewerBlockExists(did, hydration) ||
      ctx.views.viewerMuteExists(did, hydration)
    ) {
      return false
    }
    // Filter out notifications whose subject sits in a thread the viewer muted
    if (item.reasonSubject) {
      const subjectUri = item.reasonSubject as AtUriString
      const subjectPost = hydration.posts?.get(subjectUri)
      const rootUri =
        subjectPost && isPostRecordType(subjectPost.record)
          ? (subjectPost.record.reply?.root.uri ?? subjectUri)
          : subjectUri
      if (hydration.threadMutes?.get(rootUri)) {
        return false
      }
    }
    // Filter out notifications from non-followed accounts for reasons the
    // viewer set to follows-only (`include: 'follows'`)
    if (skeleton.followsOnlyReasons.has(item.reason)) {
      if (!hydration.profileViewers?.get(did)?.following) {
        return false
      }
    }

    if (
      shouldFilterReplyByThreadgate(
        item.reason,
        uri,
        params.hydrateCtx.viewer,
        hydration,
        ctx.views,
      )
    ) {
      return false
    }

    if (
      shouldFilterHiddenThreadTag(
        item.reason,
        uri,
        did,
        hydration,
        ctx.cfg.threadTagsHide,
      )
    ) {
      return false
    }

    if (
      shouldFilterForNeedsReview(item.reason, did, uri, hydration, ctx.views)
    ) {
      return false
    }

    return true
  })
  return skeleton
}

const presentation = (
  input: PresentationFnInput<Context, Params, SkeletonState>,
): app.bsky.notification.listNotifications.$OutputBody => {
  const { skeleton, hydration, ctx } = input
  const { notifs, lastSeenNotifs, cursor } = skeleton
  const notifications = mapDefined(notifs, (notif) =>
    ctx.views.notification(notif, lastSeenNotifs, hydration),
  )
  return {
    notifications,
    cursor,
    seenAt: skeleton.lastSeenNotifs,
  }
}

type Context = AppContext

type Params = app.bsky.notification.listNotifications.$Params & {
  hydrateCtx: HydrateCtxWithViewer
  mode: NotificationListMode
  lastSeen?: Date
}

type SkeletonState = {
  notifs: Notification[]
  followsOnlyReasons: Set<string>
  lastSeenNotifs?: DatetimeString
  cursor?: string
}
