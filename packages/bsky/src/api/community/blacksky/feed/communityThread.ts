import { AtUriString } from '@atproto/syntax'
import { AppContext } from '../../../../context.js'
import { HydrateCtx } from '../../../../hydration/hydrator.js'
import { uriToDid } from '../../../../util/uris.js'
import { resHeaders } from '../../../util.js'
import { assertCommunityMembershipForUris } from '../membership-guard.js'
import { spaceRecordAuthor } from '../space-uri.js'
import {
  buildCommunityPostView,
  isBlockedForViewer,
  isMutedForViewer,
} from '../views/communityPostView.js'

export type CommunityThreadParams = {
  anchor: string
  above?: boolean
  below?: number
  branchingFactor?: number
}

export type CommunityThreadResponse = {
  body: unknown
  headers?: Record<string, string>
}

/**
 * Synthesize a thread from `community_post` rows.
 *
 * Community content never reaches the standard thread pipeline: its rows live
 * in their own table, and the anchor may be a space URI, which is not an
 * at-uri and cannot be resolved by the hydrator. Both the standard thread
 * endpoint (for the stub collection, whose URIs are ordinary at-uris) and the
 * community thread endpoint (for anything, space URIs included) call this.
 */
export async function buildCommunityThread(
  ctx: AppContext,
  hydrateCtx: HydrateCtx,
  params: CommunityThreadParams,
  viewer: string | null,
): Promise<CommunityThreadResponse> {
  await assertCommunityMembershipForUris(ctx, viewer, [params.anchor])
  const { post } = await ctx.dataplane.getCommunityPost({
    uri: params.anchor as AtUriString,
  })
  if (!post) {
    return {
      body: {
        hasOtherReplies: false,
        thread: [
          {
            uri: params.anchor,
            depth: 0,
            value: {
              $type: 'app.bsky.unspecced.defs#threadItemNotFound',
            },
          },
        ],
      } as any,
    }
  }
  const helperCtx = ctx
  const threadRootUri = post.replyRoot || post.uri
  const replyAllowed = viewer
    ? (
        await ctx.dataplane.checkCommunityReplyAllowed({
          rootUri: threadRootUri,
          viewerDid: viewer,
        })
      ).allowed
    : false
  const replyDisabled = !replyAllowed
  const blockedItem = (uri: string, depth: number, view: any) => ({
    uri,
    depth,
    value: {
      $type: 'app.bsky.unspecced.defs#threadItemBlocked',
      author: {
        did: (view?.author as any)?.did ?? '',
        viewer: (view?.author as any)?.viewer ?? {},
      },
    },
  })
  const anchorView = await buildCommunityPostView(
    helperCtx as any,
    hydrateCtx,
    post,
    0,
    viewer ?? undefined,
    replyDisabled,
  )
  if (!anchorView) {
    return {
      body: {
        hasOtherReplies: false,
        thread: [
          {
            uri: params.anchor,
            depth: 0,
            value: {
              $type: 'app.bsky.unspecced.defs#threadItemNotFound',
            },
          },
        ],
      } as any,
      headers: resHeaders({ labelers: hydrateCtx.labelers }),
    }
  }
  if (isBlockedForViewer(anchorView)) {
    return {
      body: {
        hasOtherReplies: false,
        thread: [blockedItem(post.uri, 0, anchorView)],
      } as any,
      headers: resHeaders({ labelers: hydrateCtx.labelers }),
    }
  }
  const allInThreadRes = await ctx.dataplane.getCommunityPostReplies({
    parentUri: threadRootUri as AtUriString,
    limit: 200,
  })
  const allInThread = allInThreadRes.posts ?? []
  const byUri = new Map<string, any>(allInThread.map((p: any) => [p.uri, p]))
  byUri.set(post.uri, post)

  const ancestorViews: Array<{
    uri: string
    view: unknown
    depth: number
    notFound?: boolean
    blocked?: boolean
  }> = []
  let moreParentsAtTop = false
  if (params.above && post.replyParent) {
    const maxAbove = ctx.cfg.maxThreadParents ?? 80
    let parentUri: string | undefined = post.replyParent
    let depth = -1
    while (parentUri && -depth <= maxAbove) {
      let parentRow: any = byUri.get(parentUri)
      if (!parentRow) {
        const r = await ctx.dataplane.getCommunityPost({
          uri: parentUri as AtUriString,
        })
        if (!r.post) {
          // Deleted/unavailable ancestor: surface a placeholder like a
          // standard thread rather than silently dropping the ancestry.
          ancestorViews.push({
            uri: parentUri,
            view: null,
            depth,
            notFound: true,
          })
          break
        }
        parentRow = r.post
        byUri.set(parentRow.uri, parentRow)
      }
      const view = await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx,
        parentRow,
        0,
        viewer ?? undefined,
        replyDisabled,
      )
      if (!view) {
        // Gated ancestor (another space, or no longer viewable). Emitting a
        // placeholder would still carry its URI, and a space record URI names
        // both the space and the author, so the chain simply ends here.
        break
      }
      if (isBlockedForViewer(view)) {
        ancestorViews.push({
          uri: parentRow.uri,
          view,
          depth,
          blocked: true,
        })
        break
      }
      ancestorViews.push({ uri: parentRow.uri, view, depth })
      parentUri = parentRow.replyParent || undefined
      depth -= 1
    }
    // Chain continues beyond the window: signal it on the topmost
    // returned ancestor, matching the standard thread contract.
    moreParentsAtTop = !!parentUri
  }
  ancestorViews.reverse()

  // The OP thread is the unbroken chain of root-author self-replies
  // starting at the thread root, matching standard thread semantics.
  // Community post URIs always carry their creator as the authority,
  // and getCommunityPostReplies loads the root's full descendant set.
  // A space record uri's authority is the space, so the author comes from the
  // uri's own author segment there.
  const rootAuthor = spaceRecordAuthor(threadRootUri) ?? uriToDid(threadRootUri)
  const opChainUris = new Set<string>([threadRootUri])
  {
    const childUrisByParent = new Map<string, string[]>()
    for (const p of allInThread) {
      if (!p.replyParent) continue
      const arr = childUrisByParent.get(p.replyParent) ?? []
      arr.push(p.uri)
      childUrisByParent.set(p.replyParent, arr)
    }
    let frontier: string[] = [threadRootUri]
    while (frontier.length) {
      const next: string[] = []
      for (const u of frontier) {
        for (const childUri of childUrisByParent.get(u) ?? []) {
          const row = byUri.get(childUri)
          if (row?.creator === rootAuthor && !opChainUris.has(childUri)) {
            opChainUris.add(childUri)
            next.push(childUri)
          }
        }
      }
      frontier = next
    }
  }

  // Depth-first assembly: each subtree's items are contiguous, which
  // is what the flattened threadItem contract requires.
  const childrenOf = new Map<string, any[]>()
  for (const p of allInThread) {
    if (p.uri === post.uri || !p.replyParent) continue
    const list = childrenOf.get(p.replyParent) ?? []
    list.push(p)
    childrenOf.set(p.replyParent, list)
  }
  for (const list of childrenOf.values()) {
    list.sort((a, b) => {
      const t = (a.createdAt ?? '').localeCompare(b.createdAt ?? '')
      return t !== 0 ? t : (a.uri ?? '').localeCompare(b.uri ?? '')
    })
  }
  const maxDepth = Math.min(params.below ?? 10, 50)
  const branching = Math.min(params.branchingFactor ?? 50, 50)
  const cappedDescendants: Array<{ post: any; depth: number }> = []
  const moreRepliesByUri = new Map<string, number>()
  const walk = (uri: string, depth: number) => {
    const all = childrenOf.get(uri) ?? []
    if (depth > maxDepth || cappedDescendants.length >= 200) {
      if (all.length > 0) moreRepliesByUri.set(uri, all.length)
      return
    }
    // branchingFactor caps every level except the anchor's direct replies
    const children = depth === 1 ? all : all.slice(0, branching)
    if (children.length < all.length) {
      moreRepliesByUri.set(uri, all.length - children.length)
    }
    for (const child of children) {
      if (cappedDescendants.length >= 200) {
        moreRepliesByUri.set(uri, (moreRepliesByUri.get(uri) ?? 0) + 1)
        continue
      }
      cappedDescendants.push({ post: child, depth })
      walk(child.uri, depth + 1)
    }
  }
  walk(post.uri, 1)
  const builtDescendants = await Promise.all(
    cappedDescendants.map(async ({ post: p, depth }) => ({
      uri: p.uri as string,
      depth,
      view: await buildCommunityPostView(
        helperCtx as any,
        hydrateCtx,
        p,
        0,
        viewer ?? undefined,
        replyDisabled,
      ),
    })),
  )
  // Muted-author replies are excluded from the main tree with their
  // whole subtree, matching how the standard pipeline buckets them
  // out of thread[]. The flattened list is depth-first, so a subtree
  // is the contiguous run of strictly deeper items.
  const descendantViews: typeof builtDescendants = []
  let skipDeeperThan: number | null = null
  for (const item of builtDescendants) {
    if (skipDeeperThan !== null) {
      if (item.depth > skipDeeperThan) continue
      skipDeeperThan = null
    }
    // No view means the gate denied the row; drop it and its whole subtree
    // rather than emitting an item whose uri names the space and the author.
    if (!item.view || isMutedForViewer(item.view)) {
      skipDeeperThan = item.depth
      continue
    }
    descendantViews.push(item)
  }

  return {
    body: {
      hasOtherReplies: false,
      thread: [
        ...ancestorViews.map(({ uri, view, depth, notFound, blocked }, idx) =>
          notFound
            ? {
                uri,
                depth,
                value: {
                  $type: 'app.bsky.unspecced.defs#threadItemNotFound',
                },
              }
            : blocked
              ? blockedItem(uri, depth, view)
              : {
                  uri,
                  depth,
                  value: {
                    $type: 'app.bsky.unspecced.defs#threadItemPost',
                    post: view,
                    moreParents: idx === 0 && moreParentsAtTop,
                    moreReplies: 0,
                    opThread: opChainUris.has(uri),
                    hiddenByThreadgate: false,
                    mutedByViewer: isMutedForViewer(view as any),
                  },
                },
        ),
        {
          uri: post.uri,
          depth: 0,
          value: {
            $type: 'app.bsky.unspecced.defs#threadItemPost',
            post: anchorView,
            moreParents:
              ancestorViews.length === 0 && !!post.replyParent && !params.above,
            moreReplies: moreRepliesByUri.get(post.uri) ?? 0,
            opThread: opChainUris.has(post.uri),
            hiddenByThreadgate: false,
            mutedByViewer: isMutedForViewer(anchorView as any),
          },
        },
        ...descendantViews.map(({ uri, view, depth }) =>
          isBlockedForViewer(view as any)
            ? blockedItem(uri, depth, view)
            : {
                uri,
                depth,
                value: {
                  $type: 'app.bsky.unspecced.defs#threadItemPost',
                  post: view,
                  moreParents: false,
                  moreReplies: moreRepliesByUri.get(uri) ?? 0,
                  opThread: opChainUris.has(uri),
                  hiddenByThreadgate: false,
                  mutedByViewer: isMutedForViewer(view as any),
                },
              },
        ),
      ],
    } as any,
    headers: resHeaders({ labelers: hydrateCtx.labelers }),
  }
}
