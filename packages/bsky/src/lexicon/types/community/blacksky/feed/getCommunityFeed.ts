/**
 * GENERATED CODE - DO NOT MODIFY
 */
import { type ValidationResult, BlobRef } from '@atproto/lexicon'
import { CID } from 'multiformats/cid'
import { validate as _validate } from '../../../../lexicons'
import {
  type $Typed,
  is$typed as _is$typed,
  type OmitKey,
} from '../../../../util'
import type * as AppBskyFeedDefs from '../../../app/bsky/feed/defs.js'

const is$typed = _is$typed,
  validate = _validate
const id = 'community.blacksky.feed.getCommunityFeed'

export type QueryParams = {
  /** DID or handle of the actor whose community posts to fetch. */
  actor: string
  /** Number of posts to return. */
  limit: number
  /** Pagination cursor. */
  cursor?: string
}
export type InputSchema = undefined

export interface OutputSchema {
  cursor?: string
  feed: AppBskyFeedDefs.FeedViewPost[]
}

export type HandlerInput = void

export interface HandlerSuccess {
  encoding: 'application/json'
  body: OutputSchema
  headers?: { [key: string]: string }
}

export interface HandlerError {
  status: number
  message?: string
  error?: 'MembershipRequired'
}

export type HandlerOutput = HandlerError | HandlerSuccess
