import { createHmac, timingSafeEqual } from 'node:crypto'

export const SPACE_MEDIA_WINDOW_SECONDS = 6 * 60 * 60

const secret = () => process.env.COMMUNITY_MEDIA_SIGNING_SECRET ?? ''

export const spaceMediaExpiry = (
  now = Math.floor(Date.now() / 1000),
  windowSeconds = SPACE_MEDIA_WINDOW_SECONDS,
) => (Math.floor(now / windowSeconds) + 2) * windowSeconds

const payload = (space: string, did: string, cid: string, exp: number) =>
  `${space}\n${did}\n${cid}\n${exp}`

export const signSpaceMedia = (
  space: string,
  did: string,
  cid: string,
  exp: number,
  key = secret(),
) =>
  key
    ? createHmac('sha256', key)
        .update(payload(space, did, cid, exp))
        .digest('base64url')
    : null

export const verifySpaceMedia = (
  space: string,
  did: string,
  cid: string,
  exp: number,
  sig: string,
  now = Math.floor(Date.now() / 1000),
  key = secret(),
) => {
  if (!key || exp <= now) return false
  const expected = signSpaceMedia(space, did, cid, exp, key)
  if (!expected || expected.length !== sig.length) return false
  return timingSafeEqual(Buffer.from(expected), Buffer.from(sig))
}
