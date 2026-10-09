import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import type { ServerConfig } from '../../../config.js'
import {
  type DataPlaneClient,
  getServiceEndpoint,
  unpackIdentityServices,
} from '../../../data-plane/index.js'

export type SpaceMediaBlob = { cid: string; size?: number }

export const spaceMediaConfig = (cfg: ServerConfig) =>
  cfg.communityMediaSigningSecret &&
  cfg.communityMediaBucketEndpoint &&
  cfg.communityMediaBucketRegion &&
  cfg.communityMediaBucketName &&
  cfg.communityMediaBucketAccessKeyId &&
  cfg.communityMediaBucketSecretAccessKey
    ? {
        endpoint: cfg.communityMediaBucketEndpoint,
        region: cfg.communityMediaBucketRegion,
        bucket: cfg.communityMediaBucketName,
        accessKeyId: cfg.communityMediaBucketAccessKeyId,
        secretAccessKey: cfg.communityMediaBucketSecretAccessKey,
        windowSeconds: cfg.communityMediaSigningWindowSeconds,
        maxBytes: cfg.communityMediaMaxImageBytes,
      }
    : undefined

export const spaceMediaObjectKey = (did: string, cid: string) =>
  `blocks/${did}/${cid}`

type BucketConfig = {
  endpoint: string
  region: string
  bucket: string
  accessKeyId: string
  secretAccessKey: string
  keyPrefix?: string
}

export const spaceMediaBucketFor = (
  cfg: ServerConfig,
  pds?: string,
): BucketConfig | undefined => {
  const config = spaceMediaConfig(cfg)
  if (!config) return undefined
  return (
    (pds && cfg.communityMediaPdsBuckets?.find((b) => b.pds === pds)) || config
  )
}

export const spaceMediaAuthorPds = async (
  cfg: ServerConfig | undefined,
  dataplane: Pick<DataPlaneClient, 'getIdentityByDid'>,
  did: string,
): Promise<string | undefined> => {
  if (!cfg?.communityMediaPdsBuckets?.length) return undefined
  try {
    const identity = await dataplane.getIdentityByDid({ did })
    const endpoint = getServiceEndpoint(
      unpackIdentityServices(identity.services),
      { id: 'atproto_pds', type: 'AtprotoPersonalDataServer' },
    )
    return endpoint ? new URL(endpoint).origin : undefined
  } catch {
    return undefined
  }
}

const clients = new Map<string, S3Client>()

const clientFor = (config: BucketConfig) => {
  const key = `${config.endpoint}\n${config.region}\n${config.accessKeyId}\n${config.secretAccessKey}`
  let client = clients.get(key)
  if (!client) {
    client = new S3Client({
      endpoint: config.endpoint,
      region: config.region,
      forcePathStyle: true,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
    })
    clients.set(key, client)
  }
  return client
}

export async function presignSpaceBlob(
  cfg: ServerConfig,
  did: string,
  blob: SpaceMediaBlob,
  pds?: string,
  now = Math.floor(Date.now() / 1000),
): Promise<string | null> {
  const config = spaceMediaConfig(cfg)
  const bucket = spaceMediaBucketFor(cfg, pds)
  if (
    !config ||
    !bucket ||
    (blob.size !== undefined && blob.size > config.maxBytes)
  ) {
    return null
  }

  const windowStart =
    Math.floor(now / config.windowSeconds) * config.windowSeconds
  return getSignedUrl(
    clientFor(bucket),
    new GetObjectCommand({
      Bucket: bucket.bucket,
      Key: `${bucket.keyPrefix ?? ''}${spaceMediaObjectKey(did, blob.cid)}`,
    }),
    {
      expiresIn: 2 * config.windowSeconds,
      signingDate: new Date(windowStart * 1000),
    },
  )
}
