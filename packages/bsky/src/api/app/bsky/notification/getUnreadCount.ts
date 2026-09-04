import type { DidString } from '@atproto/syntax'
import { InvalidRequestError, type Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { app } from '../../../../lexicons/index.js'
import {
  type HydrationFnInput,
  type PresentationFnInput,
  type SkeletonFnInput,
  createPipeline,
  noRules,
} from '../../../../pipeline.js'
import { canViewSpace } from '../../../community/blacksky/tenant-gate.js'

export type NotificationCountMode = 'public-only' | 'authorized-union'

export default function (server: Server, ctx: AppContext) {
  server.add(app.bsky.notification.getUnreadCount, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth, params }) => {
      const viewer = auth.credentials.iss
      const result = await runNotificationCount(
        { ...params, viewer },
        ctx,
        'public-only',
      )
      return {
        encoding: 'application/json',
        body: result,
      }
    },
  })
}

export async function runNotificationCount(
  params: Omit<Params, 'mode'>,
  ctx: AppContext,
  mode: NotificationCountMode,
) {
  const getUnreadCount = createPipeline(
    skeleton,
    hydration,
    noRules,
    presentation,
  )
  return await getUnreadCount({ ...params, mode }, ctx)
}

const skeleton = async (
  input: SkeletonFnInput<Context, Params>,
): Promise<SkeletonState> => {
  const { params, ctx } = input
  if (params.seenAt) {
    throw new InvalidRequestError('The seenAt parameter is unsupported')
  }
  // See listNotifications: the legacy `priorityNotifications` flag is deprecated
  // (no client UI to clear it). Honor priority only when explicitly requested so
  // the unread count stays consistent with the notification list (see BA-271).
  const priority = params.priority ?? false
  let allowedSpaceUris: string[] = []
  if (params.mode === 'authorized-union') {
    const candidates = await ctx.hydrator.dataplane.getUnreadNotificationSpaces(
      {
        actorDid: params.viewer,
        priority,
      },
    )
    const spaces = [...new Set(candidates.spaces.map((item) => item.spaceUri))]
    const decisions = await Promise.all(
      spaces.map(async (spaceUri) => ({
        spaceUri,
        allowed: await canViewSpace(ctx, spaceUri, params.viewer).catch(
          () => false,
        ),
      })),
    )
    allowedSpaceUris = decisions
      .filter((decision) => decision.allowed)
      .map((decision) => decision.spaceUri)
  }
  const res = await ctx.hydrator.dataplane.getUnreadNotificationCount({
    actorDid: params.viewer,
    priority,
    allowedSpaceUris,
  })
  return {
    count: res.count,
  }
}

const hydration = async (
  _input: HydrationFnInput<Context, Params, SkeletonState>,
) => {
  return {}
}

const presentation = (
  input: PresentationFnInput<Context, Params, SkeletonState>,
) => {
  const { skeleton } = input
  return { count: skeleton.count }
}

type Context = AppContext

type Params = app.bsky.notification.getUnreadCount.$Params & {
  viewer: DidString
  mode: NotificationCountMode
}

type SkeletonState = {
  count: number
}
