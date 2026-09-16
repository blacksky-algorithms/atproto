import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
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
  const client = new S3Client({
    endpoint: config.endpoint,
    region: config.region,
    forcePathStyle: true,
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey,
    },
  })
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
