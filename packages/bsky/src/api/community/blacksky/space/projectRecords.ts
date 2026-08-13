import { AuthRequiredError, InvalidRequestError, Server } from '@atproto/xrpc-server'
import { AppContext } from '../../../../context.js'
import { community } from '../../../../lexicons/index.js'
import { canContributeToSpace } from '../tenant-gate.js'
import { parseSpaceRecordUri, parseSpaceUri, spaceUriOf } from '../space-uri.js'

const PROJECTOR_ISSUERS = () =>
  new Set(
    (process.env.SPACE_PROJECTOR_ISSUERS ?? '')
      .split(',')
      .map((did) => did.trim())
      .filter(Boolean),
  )

export default function (server: Server, ctx: AppContext) {
  server.add(community.blacksky.space.projectRecords, {
    auth: ctx.authVerifier.standard,
    handler: async ({ input, auth }) => {
      const issuer = auth.credentials.iss
      if (!PROJECTOR_ISSUERS().has(issuer)) {
        throw new AuthRequiredError('untrusted projection issuer', 'UntrustedIssuer')
      }

      for (const op of input.body.ops) {
        const space = parseSpaceUri(op.space)
        const record = parseSpaceRecordUri(op.uri)
        if (!space || !record || spaceUriOf(record) !== op.space || record.authorDid !== op.author || record.collection !== op.collection) {
          throw new InvalidRequestError('projection does not name a record in its asserted space', 'InvalidProjection')
        }
        if (op.collection === 'app.bsky.feed.post' && !await canContributeToSpace(ctx, op.space, op.author)) {
          throw new AuthRequiredError('author is not admitted to this space', 'NotAuthorized')
        }
        const result = await ctx.dataplane.projectCommunityRecord({
          spaceUri: op.space, author: op.author, uri: op.uri, cid: op.cid ?? '',
          revision: op.revision, operation: op.operation, collection: op.collection,
          recordJson: op.record ? JSON.stringify(op.record) : '', actionUri: op.actionUri ?? '',
        })
        if (result.rejected) {
          throw new InvalidRequestError(result.rejected, 'InvalidProjection')
        }
      }
      return { encoding: 'application/json' as const, body: {} }
    },
  })
}
