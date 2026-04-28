import { Pool as PgPool } from 'pg'
import { ConnectRouter } from '@connectrpc/connect'
import { IdResolver } from '@atproto/identity'
import { Service } from '../../../proto/bsky_connect'
import { Redis } from '../../../redis'
import {
  InteractionCache,
  PostMetaCache,
  RecordCache,
  RelationshipCache,
} from '../cache'
import { Database } from '../db'
import activitySubscription from './activity-subscription'
import blocks from './blocks'
import bookmarks from './bookmarks'
import community from './community'
import drafts from './drafts'
import feedGens from './feed-gens'
import feeds from './feeds'
import follows from './follows'
import identity from './identity'
import interactions from './interactions'
import labels from './labels'
import likes from './likes'
import lists from './lists'
import moderation from './moderation'
import mutes from './mutes'
import notifs from './notifs'
import profile from './profile'
import quotes from './quotes'
import records from './records'
import relationships from './relationships'
import reposts from './reposts'
import search from './search'
import sitemap from './sitemap'
import starterPacks from './starter-packs'
import suggestions from './suggestions'
import sync from './sync'
import threads from './threads'

export default (
  db: Database,
  idResolver: IdResolver,
  redis?: Redis,
  membershipPool?: PgPool,
) => {
  const interactionCache = redis ? new InteractionCache(redis) : undefined
  const recordCache = redis ? new RecordCache(redis) : undefined
  const postMetaCache = redis ? new PostMetaCache(redis) : undefined
  const relationshipCache = redis ? new RelationshipCache(redis) : undefined
  return (router: ConnectRouter) =>
    router.service(Service, {
      ...activitySubscription(db),
      ...blocks(db),
      ...bookmarks(db),
      ...community(db, membershipPool),
      ...drafts(db),
      ...feedGens(db),
      ...feeds(db),
      ...follows(db),
      ...identity(db, idResolver),
      ...interactions(db, interactionCache),
      ...labels(db),
      ...likes(db),
      ...lists(db),
      ...moderation(db),
      ...mutes(db),
      ...notifs(db),
      ...profile(db),
      ...quotes(db),
      ...records(db, recordCache, postMetaCache),
      ...relationships(db, relationshipCache),
      ...reposts(db),
      ...search(db),
      ...sitemap(),
      ...suggestions(db),
      ...sync(db),
      ...threads(db),
      ...starterPacks(db),

      async ping() {
        return {}
      },
    })
}
