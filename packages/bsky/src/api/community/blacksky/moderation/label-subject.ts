import { AtUri } from '@atproto/syntax'
import { InvalidRequestError } from '@atproto/xrpc-server'
import { isSpaceRecordUri } from '../space-uri.js'

const COMMUNITY_POST_COLLECTION = 'community.blacksky.feed.post'
const BSKY_POST_COLLECTION = 'app.bsky.feed.post'

type SubjectDataplane = {
  getCommunityPost: (req: { uri: string }) => Promise<{ post?: unknown }>
  getPostRecords: (req: {
    uris: string[]
  }) => Promise<{ records: Array<{ cid: string }> }>
}

export function assertPeerModLabelSubject(subjectUri: string): void {
  if (isSpaceRecordUri(subjectUri)) {
    throw new InvalidRequestError(
      'Permissioned-space records cannot be label subjects',
      'InvalidSubject',
    )
  }
  const { collection } = new AtUri(subjectUri)
  if (
    collection !== COMMUNITY_POST_COLLECTION &&
    collection !== BSKY_POST_COLLECTION
  ) {
    throw new InvalidRequestError(
      'Subject must be a community or app.bsky post',
      'InvalidSubject',
    )
  }
}

export async function assertLabelSubjectExists(
  dataplane: SubjectDataplane,
  subjectUri: string,
): Promise<void> {
  assertPeerModLabelSubject(subjectUri)
  const exists =
    new AtUri(subjectUri).collection === COMMUNITY_POST_COLLECTION
      ? !!(await dataplane.getCommunityPost({ uri: subjectUri })).post
      : !!(await dataplane.getPostRecords({ uris: [subjectUri] })).records[0]
          ?.cid
  if (!exists) {
    throw new InvalidRequestError('Subject post not found', 'InvalidSubject')
  }
}
