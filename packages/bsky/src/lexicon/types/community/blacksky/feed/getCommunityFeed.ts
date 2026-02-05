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
import type * as AppBskyRichtextFacet from '../../../app/bsky/richtext/facet.js'
import type * as AppBskyEmbedImages from '../../../app/bsky/embed/images.js'
import type * as AppBskyEmbedVideo from '../../../app/bsky/embed/video.js'
import type * as AppBskyEmbedExternal from '../../../app/bsky/embed/external.js'
import type * as AppBskyEmbedRecord from '../../../app/bsky/embed/record.js'
import type * as AppBskyEmbedRecordWithMedia from '../../../app/bsky/embed/recordWithMedia.js'
import type * as ComAtprotoLabelDefs from '../../../com/atproto/label/defs.js'

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
  posts: CommunityPostView[]
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

export interface CommunityPostView {
  $type?: 'community.blacksky.feed.getCommunityFeed#communityPostView'
  uri: string
  cid?: string
  creator: string
  text: string
  facets?: AppBskyRichtextFacet.Main[]
  replyRoot?: string
  replyParent?: string
  embed?:
    | $Typed<AppBskyEmbedImages.View>
    | $Typed<AppBskyEmbedVideo.View>
    | $Typed<AppBskyEmbedExternal.View>
    | $Typed<AppBskyEmbedRecord.View>
    | $Typed<AppBskyEmbedRecordWithMedia.View>
    | { $type: string }
  langs?: string[]
  labels?: ComAtprotoLabelDefs.Label[]
  tags?: string[]
  createdAt: string
  indexedAt: string
}

const hashCommunityPostView = 'communityPostView'

export function isCommunityPostView<V>(v: V) {
  return is$typed(v, id, hashCommunityPostView)
}

export function validateCommunityPostView<V>(v: V) {
  return validate<CommunityPostView & V>(v, id, hashCommunityPostView)
}
