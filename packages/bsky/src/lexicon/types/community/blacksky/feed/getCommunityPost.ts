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
const id = 'community.blacksky.feed.getCommunityPost'

export type QueryParams = {
  /** AT URI of the community post. */
  uri: string
}
export type InputSchema = undefined

export interface OutputSchema {
  post: AppBskyFeedDefs.PostView
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
  error?: 'MembershipRequired' | 'PostNotFound'
}

export type HandlerOutput = HandlerError | HandlerSuccess
