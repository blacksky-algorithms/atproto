import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
} from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { canContributeToSpace, canViewCommunityPost } from '../tenant-gate.js'
import { parseSpaceRecordUri, parseSpaceUri, spaceUriOf } from '../space-uri.js'

const PROJECTOR_ISSUERS = () =>
  new Set(
    (process.env.SPACE_PROJECTOR_ISSUERS ?? '')
      .split(',')
      .map((did) => did.trim())
      .filter(Boolean),
  )

const notificationCandidates = (record: Record<string, any>) => {
  const dids = new Set<string>()
  const parent = record.reply?.parent?.uri
  if (typeof parent === 'string') {
    const parsed = parseSpaceRecordUri(parent)
    if (parsed) dids.add(parsed.authorDid)
  }
  for (const facet of Array.isArray(record.facets) ? record.facets : []) {
    for (const feature of Array.isArray(facet?.features)
      ? facet.features
      : []) {
      if (
        feature?.$type === 'app.bsky.richtext.facet#mention' &&
        typeof feature.did === 'string'
      ) {
        dids.add(feature.did)
      }
    }
  }
  const quoted =
    record.embed?.$type === 'app.bsky.embed.record'
      ? record.embed.record?.uri
      : record.embed?.$type === 'app.bsky.embed.recordWithMedia'
        ? record.embed.record?.record?.uri
        : undefined
  if (typeof quoted === 'string') {
    const parsed = parseSpaceRecordUri(quoted)
    if (parsed) dids.add(parsed.authorDid)
  }
  return dids
}

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.space.projectRecords, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const issuer = auth.credentials.iss
      if (!PROJECTOR_ISSUERS().has(issuer)) {
        throw new AuthRequiredError(
          'untrusted projection issuer',
          'UntrustedIssuer',
        )
      }

      for (const op of input.body.ops) {
        const space = parseSpaceUri(op.space)
        const record = parseSpaceRecordUri(op.uri)
        if (
          !space ||
          !record ||
          spaceUriOf(record) !== op.space ||
          record.authorDid !== op.author ||
          record.collection !== op.collection
        ) {
          throw new InvalidRequestError(
            'projection does not name a record in its asserted space',
            'InvalidProjection',
          )
        }
        if (
          op.collection === 'app.bsky.feed.post' &&
          !(await canContributeToSpace(ctx, op.space, op.author))
        ) {
          throw new AuthRequiredError(
            'author is not admitted to this space',
            'NotAuthorized',
          )
        }
        const allowedNotificationDids: string[] = []
        if (op.operation === 'create' && op.record) {
          const candidates = notificationCandidates(op.record)
          candidates.delete(op.author)
          for (const did of candidates) {
            if (
              await canViewCommunityPost(
                ctx,
                { uri: op.uri, spaceUri: op.space },
                did,
              )
            ) {
              allowedNotificationDids.push(did)
            }
          }
        }
        const result = await ctx.dataplane.projectCommunityRecord({
          spaceUri: op.space,
          author: op.author,
          uri: op.uri,
          cid: op.cid ?? '',
          revision: op.revision,
          operation: op.operation,
          collection: op.collection,
          recordJson: op.record ? JSON.stringify(op.record) : '',
          actionUri: op.actionUri ?? '',
          allowedNotificationDids,
        })
        if (result.rejected) {
          throw new InvalidRequestError(result.rejected, 'InvalidProjection')
        }
      }
      return { encoding: 'application/json' as const, body: {} }
    },
  })
}
