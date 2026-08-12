import { mapDefined } from '@atproto/common'
import { DidString } from '@atproto/syntax'
import { Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import {
  HydrateCtx,
  HydrationState,
  Hydrator,
} from '../../../../hydration/hydrator.js'
import { internal } from '../../../../lexicons/index.js'
import { createPipeline, noRules } from '../../../../pipeline.js'
import { Views } from '../../../../views/index.js'

export default function (server: Server, ctx: AppContext) {
  const getProfiles = createPipeline(skeleton, hydration, noRules, presentation)
  server.add(internal.bsky.actor.getProfiles, {
    auth: ctx.authVerifier.role,
    handler: async ({ params, req }) => {
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        viewer: params.viewer ?? null,
        labelers,
      })

      const result = await getProfiles({ ...params, hydrateCtx }, ctx)

      return {
        encoding: 'application/json',
        body: result,
      }
    },
  })
}

const skeleton = async (input: {
  ctx: Context
  params: Params
}): Promise<SkeletonState> => {
  const { params } = input
  // `socialProof` is still accepted so callers do not have to change, but it no
  // longer selects dids for known-followers hydration: that hydration is gone.
  // See Hydrator.hydrateProfilesDetailed for why.
  return { dids: params.dids }
}

const hydration = async (input: {
  ctx: Context
  params: Params
  skeleton: SkeletonState
}) => {
  const { ctx, params, skeleton } = input
  return ctx.hydrator.hydrateProfilesDetailed(skeleton.dids, params.hydrateCtx)
}

const presentation = (input: {
  ctx: Context
  params: Params
  skeleton: SkeletonState
  hydration: HydrationState
}) => {
  const { ctx, skeleton, hydration } = input
  const profiles = mapDefined(skeleton.dids, (did) =>
    ctx.views.profileDetailed(did, hydration),
  )
  return { profiles }
}

type Context = {
  hydrator: Hydrator
  views: Views
}

type Params = internal.bsky.actor.getProfiles.$Params & {
  hydrateCtx: HydrateCtx
}

type SkeletonState = {
  dids: DidString[]
}
