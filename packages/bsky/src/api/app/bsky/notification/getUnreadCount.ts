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
import { canViewCommunityPost } from '../../../community/blacksky/tenant-gate.js'

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
  const candidates = await ctx.hydrator.dataplane.getUnreadNotificationSpaces({
    actorDid: params.viewer,
    priority,
  })
  const decisions = await Promise.all(
    candidates.spaces.map(async (candidate) => ({
      spaceUri: candidate.spaceUri,
      allowed: await canViewCommunityPost(
        ctx as AppContext,
        { uri: candidate.postUri, spaceUri: candidate.spaceUri },
        params.viewer,
      ),
    })),
  )
  const allowedSpaceUris = [
    ...new Set(
      decisions
        .filter((decision) => decision.allowed)
        .map((decision) => decision.spaceUri),
    ),
  ]
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
