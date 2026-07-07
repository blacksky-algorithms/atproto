import {
  LexValue,
  enumBlobRefs,
  getBlobCidString,
  getBlobMime,
  getBlobSize,
} from '@atproto/lex'
import type { BlobRef } from '@atproto/lex'

export interface BlobMetadata {
  cid: string
  mimeType: string
  size?: number
}

export const findBlobRefs = (val: unknown): BlobRef[] =>
  Array.from(enumBlobRefs(val as LexValue, { strict: false, allowLegacy: true }))

export const findBlobMetadata = (val: unknown): BlobMetadata[] =>
  findBlobRefs(val).map((ref) => {
    const size = getBlobSize(ref)
    return {
      cid: getBlobCidString(ref),
      mimeType: getBlobMime(ref),
      ...(size === undefined ? {} : { size }),
    }
  })
