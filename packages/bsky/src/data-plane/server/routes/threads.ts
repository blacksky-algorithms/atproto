import { ServiceImpl } from '@connectrpc/connect'
import { Service } from '../../../proto/bsky_connect'
import { Database } from '../db'
import { getAncestorsAndSelfQb, getDescendentsQb } from '../util'

export default (db: Database): Partial<ServiceImpl<typeof Service>> => ({
  async getThread(req) {
    const { postUri, above, below } = req
    // Cap total descendant URIs to prevent massive threads from timing out.
    // The presentation layer further filters/sorts these by branching factor.
    const MAX_THREAD_DESCENDANTS = 200
    const [ancestors, descendents] = await Promise.all([
      getAncestorsAndSelfQb(db.db, {
        uri: postUri,
        parentHeight: above,
      })
        .selectFrom('ancestor')
        .selectAll()
        .execute(),
      getDescendentsQb(db.db, {
        uri: postUri,
        depth: below,
      })
        .selectFrom('descendent')
        .innerJoin('post', 'post.uri', 'descendent.uri')
        .orderBy('post.sortAt', 'desc')
        .limit(MAX_THREAD_DESCENDANTS)
        .selectAll()
        .execute(),
    ])
    const uris = [
      ...ancestors.map((p) => p.uri),
      ...descendents.map((p) => p.uri),
    ]
    return { uris }
  },
})
