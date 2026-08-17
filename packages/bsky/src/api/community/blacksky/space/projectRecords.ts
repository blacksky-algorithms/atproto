import {
  AuthRequiredError,
  InvalidRequestError,
  Server,
  UpstreamFailureError,
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

const hasBlobRef = (value: unknown): boolean => {
  if (Array.isArray(value)) return value.some(hasBlobRef)
  if (!value || typeof value !== 'object') return false
  const record = value as Record<string, unknown>
  if (record.$type === 'blob') return true
  if (
    typeof record.mimeType === 'string' &&
    ('ref' in record || 'cid' in record)
  ) {
    return true
  }
  return Object.values(record).some(hasBlobRef)
}

/**
 * Media is off for spaces at every layer; a projection carrying a blob
 * reference loses the media, never the post. A quote wrapped with media
 * keeps its record half.
 */
const stripBlobEmbed = (record: Record<string, any>) => {
  const embed = record.embed
  if (!embed || !hasBlobRef(embed)) return record
  const inner =
    embed.$type === 'app.bsky.embed.recordWithMedia' &&
    embed.record &&
    !hasBlobRef(embed.record)
      ? embed.record
      : undefined
  const { embed: _dropped, ...rest } = record
  return inner ? { ...rest, embed: inner } : rest
}

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

type ProjectRecordsContext = {
  input: { body: community.blacksky.space.projectRecords.$InputBody }
  auth: { credentials: { iss: string } }
}

export const projectRecordsHandler =
  (ctx: AppContext) =>
  async ({ input, auth }: ProjectRecordsContext) => {
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
      const projected =
        op.operation === 'create' && op.record
          ? stripBlobEmbed(op.record)
          : op.record
      const allowedNotificationDids: string[] = []
      if (op.operation === 'create' && projected) {
        const candidates = notificationCandidates(projected)
        candidates.delete(op.author)
        for (const did of candidates) {
          let allowed: boolean
          try {
            allowed = await canViewCommunityPost(
              ctx,
              { uri: op.uri, spaceUri: op.space },
              did,
              { retryUnavailable: true },
            )
          } catch (err) {
            throw new UpstreamFailureError(
              'notification gate unavailable',
              'UpstreamFailure',
              { cause: err },
            )
          }
          if (allowed) {
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
        recordJson: projected ? JSON.stringify(projected) : '',
        actionUri: op.actionUri ?? '',
        allowedNotificationDids,
      })
      if (result.rejected) {
        throw new InvalidRequestError(result.rejected, 'InvalidProjection')
      }
    }
    return { encoding: 'application/json' as const, body: {} }
  }

export default function (server: Server, ctx: AppContext) {
  const handler = projectRecordsHandler(ctx)
  server.add(community.blacksky.space.projectRecords, {
    auth: ctx.authVerifier.standard,
    handler: ({ input, auth }) => handler({ input, auth }),
  })
}
