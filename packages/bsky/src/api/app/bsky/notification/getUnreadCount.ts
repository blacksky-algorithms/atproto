import { DidString } from '@atproto/syntax'
import { InvalidRequestError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { Hydrator } from '../../../../hydration/hydrator.js'
import { app } from '../../../../lexicons/index.js'
import {
  HydrationFnInput,
  PresentationFnInput,
  SkeletonFnInput,
  createPipeline,
  noRules,
} from '../../../../pipeline.js'
import { Views } from '../../../../views/index.js'

export default function (server: Server, ctx: AppContext) {
  const getUnreadCount = createPipeline(
    skeleton,
    hydration,
    noRules,
    presentation,
  )
  server.add(app.bsky.notification.getUnreadCount, {
    auth: ctx.authVerifier.standard,
    handler: async ({ auth, params }) => {
      const viewer = auth.credentials.iss
      const result = await getUnreadCount({ ...params, viewer }, ctx)
      return {
        encoding: 'application/json',
        body: result,
      }
    },
  })
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
  const res = await ctx.hydrator.dataplane.getUnreadNotificationCount({
    actorDid: params.viewer,
    priority,
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

type Context = {
  hydrator: Hydrator
  views: Views
}

type Params = app.bsky.notification.getUnreadCount.$Params & {
  viewer: DidString
}

type SkeletonState = {
  count: number
}
