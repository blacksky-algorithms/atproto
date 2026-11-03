import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { AtUri } from '@atproto/syntax'
import type { ServerConfig } from '../../../config.js'

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

type BucketConfig = NonNullable<ReturnType<typeof spaceMediaConfig>>

export const spaceMediaDeliveryConfig = (cfg: ServerConfig) =>
  cfg.communityMediaDeliveryEnabled &&
  cfg.communityMediaDeliveryBucketEndpoint &&
  cfg.communityMediaDeliveryBucketRegion &&
  cfg.communityMediaDeliveryBucketName &&
  cfg.communityMediaDeliveryBucketAccessKeyId &&
  cfg.communityMediaDeliveryBucketSecretAccessKey
    ? {
        endpoint: cfg.communityMediaDeliveryBucketEndpoint,
        region: cfg.communityMediaDeliveryBucketRegion,
        bucket: cfg.communityMediaDeliveryBucketName,
        accessKeyId: cfg.communityMediaDeliveryBucketAccessKeyId,
        secretAccessKey: cfg.communityMediaDeliveryBucketSecretAccessKey,
        windowSeconds: cfg.communityMediaSigningWindowSeconds,
        maxBytes: cfg.communityMediaMaxImageBytes,
      }
    : undefined

export const spaceMediaDeliveryObjectKey = (
  space: string,
  did: string,
  cid: string,
) => {
  let uri: AtUri
  try {
    uri = new AtUri(space)
  } catch {
    return null
  }
  const parts = uri.pathname.split('/').filter(Boolean)
  if (parts[0] !== 'space' || parts.length !== 3) return null
  return `spaces/${uri.did}/${parts[1]}/${parts[2]}/${did}/${cid}`
}

let cachedClient: { config: string; client: S3Client } | undefined

const clientFor = (config: BucketConfig) => {
  const key = `${config.endpoint}\n${config.region}\n${config.accessKeyId}\n${config.secretAccessKey}`
  if (cachedClient?.config !== key) {
    cachedClient = {
      config: key,
      client: new S3Client({
        endpoint: config.endpoint,
        region: config.region,
        forcePathStyle: true,
        credentials: {
          accessKeyId: config.accessKeyId,
          secretAccessKey: config.secretAccessKey,
        },
      }),
    }
  }
  return cachedClient.client
}

export async function presignSpaceBlob(
  cfg: ServerConfig,
  did: string,
  blob: SpaceMediaBlob,
  now = Math.floor(Date.now() / 1000),
): Promise<string | null> {
  const config = spaceMediaConfig(cfg)
  if (!config || (blob.size !== undefined && blob.size > config.maxBytes)) {
    return null
  }

  const windowStart =
    Math.floor(now / config.windowSeconds) * config.windowSeconds
  const client = clientFor(config)
  return getSignedUrl(
    client,
    new GetObjectCommand({
      Bucket: config.bucket,
      Key: spaceMediaObjectKey(did, blob.cid),
    }),
    {
      expiresIn: 2 * config.windowSeconds,
      signingDate: new Date(windowStart * 1000),
    },
  )
}

export async function presignDeliverySpaceBlob(
  cfg: ServerConfig,
  space: string,
  did: string,
  blob: SpaceMediaBlob,
  now = Math.floor(Date.now() / 1000),
): Promise<string | null> {
  const config = spaceMediaDeliveryConfig(cfg)
  const key = spaceMediaDeliveryObjectKey(space, did, blob.cid)
  if (
    !config ||
    !key ||
    (blob.size !== undefined && blob.size > config.maxBytes)
  ) {
    return null
  }
  const windowStart =
    Math.floor(now / config.windowSeconds) * config.windowSeconds
  return getSignedUrl(
    clientFor(config),
    new GetObjectCommand({ Bucket: config.bucket, Key: key }),
    {
      expiresIn: 2 * config.windowSeconds,
      signingDate: new Date(windowStart * 1000),
    },
  )
}
