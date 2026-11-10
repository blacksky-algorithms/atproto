import type { Server } from '@atproto/xrpc-server'
import type { AppContext } from '../../../../context.js'
import { app, community } from '../../../../lexicons/index.js'
import { runGroupedNotificationList } from '../../../app/bsky/notification/getGroupedNotifications.js'
import { resHeaders } from '../../../util.js'
import { isCommunityUri } from '../membership-guard.js'
import { spaceOfRecordUri } from '../space-uri.js'
import { buildCommunityPostView } from '../views/communityPostView.js'
import { toSpacePostView } from '../views/spaceViews.js'

const STANDARD_PREFIX = `${app.bsky.notification.getGroupedNotifications.$lxm}#`
const COMMUNITY_PREFIX = `${community.blacksky.notification.getGroupedNotifications.$lxm}#`

// The group defs are clones of the standard ones under this lexicon's own
// id, so the shared presenter's $types are renamed rather than rebuilt.
const rename = (type: string) => type.replace(STANDARD_PREFIX, COMMUNITY_PREFIX)
const toCommunityGroup = <
  T extends { $type?: string; kind: { $type: string } },
>(
  group: T,
): T => ({
  ...group,
  ...(group.$type ? { $type: rename(group.$type) } : {}),
  kind: { ...group.kind, $type: rename(group.kind.$type) },
})

// Every post uri a group can reference: the subject of a like, repost or
// reply, the replied-to parent, and the per-item posts of a multi-post group.
const communityPostUrisOf = (groups: { kind: Record<string, unknown> }[]) => {
  const uris = new Set<string>()
  const consider = (value: unknown) => {
    if (typeof value === 'string' && isCommunityUri(value)) uris.add(value)
  }
  for (const { kind } of groups) {
    consider(kind.post)
    consider(kind.parent)
    if (Array.isArray(kind.items)) {
      for (const item of kind.items) {
        if (item && typeof item === 'object') {
          consider((item as Record<string, unknown>).post)
          consider((item as Record<string, unknown>).parent)
        }
      }
    }
  }
  return [...uris]
}

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.notification.getGroupedNotifications, {
    auth: ctx.authVerifier.standard,
    handler: async ({ params, auth, req }) => {
      const viewer = auth.credentials.iss
      const features = ctx.featureGatesClient.scope(
        ctx.featureGatesClient.parseUserContextFromHandler({ viewer, req }),
      )
      const labelers = ctx.reqLabelers(req)
      const hydrateCtx = await ctx.hydrator.createContext({
        xrpcEndpoint: app.bsky.notification.getGroupedNotifications.$lxm,
        labelers,
        viewer,
        features,
      })
      const result = await runGroupedNotificationList(
        { ...params, hydrateCtx },
        ctx,
        'authorized-union',
      )
      // The pipeline already admitted these spaces for this viewer; the
      // builder's own gate answers from the same cache.
      const helperCtx = {
        hydrator: ctx.hydrator,
        views: ctx.views,
        dataplane: ctx.dataplane,
      }
      const spacePostViews = await Promise.all(
        communityPostUrisOf(result.groups).map(async (uri) => {
          const space = spaceOfRecordUri(uri)
          const res = await ctx.dataplane.getCommunityPost({
            uri,
            allowedSpaceUris: space ? [space] : [],
          })
          if (!res.post) return
          const view = await buildCommunityPostView(
            helperCtx as never,
            hydrateCtx,
            res.post as never,
            0,
            viewer,
          )
          return view ? toSpacePostView(view) : undefined
        }),
      )
      const body: community.blacksky.notification.getGroupedNotifications.$OutputBody =
        {
          ...result,
          groups: result.groups.map(toCommunityGroup) as never,
          relatedViews: [
            ...(result.relatedViews ?? []),
            ...spacePostViews.filter((view) => view !== undefined),
          ] as never,
        }
      return {
        encoding: 'application/json',
        body,
        headers: resHeaders({ labelers: hydrateCtx.labelers }),
      }
    },
  })
}
