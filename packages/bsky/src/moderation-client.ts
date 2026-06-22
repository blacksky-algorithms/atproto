import { subsystemLogger } from '@atproto/common'

const logger = subsystemLogger('bsky:moderation')

export interface ModerationEnqueueParams {
  did: string
  collection: string
  rkey: string
  pdsEndpoint: string
  blobCids?: string[]
  blobs?: ModerationBlobMetadata[]
}

export interface ModerationBlobMetadata {
  cid: string
  mimeType: string
  size?: number
}

export class ModerationClient {
  private baseUrl: string
  private apiKey: string
  private maxRetries: number
  private baseDelayMs: number

  constructor(opts: {
    baseUrl: string
    apiKey: string
    maxRetries?: number
    baseDelayMs?: number
  }) {
    this.baseUrl = opts.baseUrl
    this.apiKey = opts.apiKey
    this.maxRetries = opts.maxRetries ?? 5
    this.baseDelayMs = opts.baseDelayMs ?? 1000
  }

  async enqueue(params: ModerationEnqueueParams): Promise<void> {
    const body = {
      did: params.did,
      collection: params.collection,
      rkey: params.rkey,
      pds_endpoint: params.pdsEndpoint,
      blob_cids: params.blobCids,
      blobs: params.blobs?.map((blob) => ({
        cid: blob.cid,
        mime_type: blob.mimeType,
        ...(blob.size === undefined ? {} : { size: blob.size }),
      })),
    }

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const resp = await fetch(`${this.baseUrl}/enqueue`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Moderation-Key': this.apiKey,
          },
          body: JSON.stringify(body),
        })
        if (resp.status === 202) {
          logger.info(
            { did: params.did, rkey: params.rkey },
            'moderation enqueue succeeded',
          )
          return
        }
        throw new Error(`Unexpected status: ${resp.status}`)
      } catch (err) {
        if (attempt < this.maxRetries) {
          const delay = this.baseDelayMs * Math.pow(2, attempt)
          logger.warn(
            {
              err,
              attempt,
              maxRetries: this.maxRetries,
              delayMs: delay,
              did: params.did,
              rkey: params.rkey,
            },
            'moderation enqueue failed, retrying',
          )
          await new Promise((resolve) => setTimeout(resolve, delay))
        }
      }
    }
    logger.error(
      {
        did: params.did,
        rkey: params.rkey,
        collection: params.collection,
      },
      'CRITICAL: moderation enqueue failed after all retries - post not scanned',
    )
  }
}
