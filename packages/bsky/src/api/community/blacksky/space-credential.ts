import {
  createHash,
  generateKeyPairSync,
  KeyObject,
  sign as signBytes,
} from 'node:crypto'
import { createServiceJwt } from '@atproto/xrpc-server'
import { AppContext } from '../../../context.js'

/**
 * A space credential for reading a space host, minted for this process.
 *
 * `com.atproto.space.getSpace` is credential-gated (0016 §XRPC API): it takes a
 * space credential, not service auth. The appview is an authorised minter — the
 * host lists it alongside the syncer daemon — so it mints its own rather than
 * asking a user for a delegation token it has no session to produce. Presenting
 * it needs the `DPoP` scheme and a proof of possession, because a credential
 * reads every repo in its space and would otherwise be a bearer secret every
 * host it is shown to could replay against the others.
 */

const MINT_LXM = 'community.blacksky.space.mintCredential'
const MINT_PATH = '/admin/mintCredential'
const REQUEST_TIMEOUT_MS = 5_000
/** Credentials last two hours; renew early so a read never races expiry. */
const RENEW_BEFORE_MS = 15 * 60 * 1000

const mintToken = () => process.env.COMMUNITY_SPACE_MINT_TOKEN ?? ''

const b64url = (bytes: Uint8Array | ArrayBuffer) =>
  Buffer.from(bytes as ArrayBuffer).toString('base64url')

const json64 = (value: unknown) =>
  Buffer.from(JSON.stringify(value)).toString('base64url')

/**
 * One proof key per process. A credential is bound to this key's thumbprint at
 * mint time, so a key that did not outlive the credential would mint tokens
 * nothing could present.
 */
type ProofKey = {
  privateKey: KeyObject
  jwk: { kty: string; crv: string; x: string; y: string }
}
let proofKey: ProofKey | undefined
const dpopKey = (): ProofKey => {
  if (!proofKey) {
    const { publicKey, privateKey } = generateKeyPairSync('ec', {
      namedCurve: 'prime256v1',
    })
    const exported = publicKey.export({ format: 'jwk' }) as Record<
      string,
      string
    >
    proofKey = {
      privateKey,
      jwk: {
        kty: exported.kty,
        crv: exported.crv,
        x: exported.x,
        y: exported.y,
      },
    }
  }
  return proofKey
}

export const dpopProof = (
  method: string,
  url: string,
  accessToken?: string,
): string => {
  const { privateKey, jwk } = dpopKey()
  const now = Math.floor(Date.now() / 1000)
  const ath = accessToken
    ? createHash('sha256').update(accessToken).digest('base64url')
    : undefined
  const input = `${json64({ typ: 'dpop+jwt', alg: 'ES256', jwk })}.${json64({
    htm: method,
    htu: url,
    iat: now,
    jti: `${now}-${Math.random().toString(36).slice(2, 10)}`,
    ...(ath ? { ath } : {}),
  })}`
  const signature = signBytes('sha256', Buffer.from(input), {
    key: privateKey,
    dsaEncoding: 'ieee-p1363',
  })
  return `${input}.${b64url(signature)}`
}

type Held = { credential: string; expiresAt: number }
const held = new Map<string, Held>()

const mint = async (
  ctx: AppContext,
  host: string,
  spaceUri: string,
  authority: string,
): Promise<string | null> => {
  const token = mintToken()
  if (!token) return null
  const url = new URL(MINT_PATH, host)
  const proofUrl = `${url.origin}${MINT_PATH}`
  url.search = new URLSearchParams({ space: spaceUri }).toString()
  const serviceJwt = await createServiceJwt({
    iss: ctx.cfg.serverDid,
    aud: authority,
    lxm: MINT_LXM,
    keypair: ctx.signingKey,
  })
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${serviceJwt}`,
        'x-spacehost-mint-token': token,
        dpop: dpopProof('POST', proofUrl),
      },
      signal: controller.signal,
    })
    if (!response.ok) return null
    const body = (await response.json()) as { credential?: unknown }
    return typeof body.credential === 'string' ? body.credential : null
  } catch {
    return null
  } finally {
    clearTimeout(timeout)
  }
}

/** The credential to read `spaceUri` with, minting or renewing as needed. */
export const spaceCredential = async (
  ctx: AppContext,
  host: string,
  spaceUri: string,
  authority: string,
): Promise<string | null> => {
  const existing = held.get(spaceUri)
  if (existing && existing.expiresAt - RENEW_BEFORE_MS > Date.now()) {
    return existing.credential
  }
  const credential = await mint(ctx, host, spaceUri, authority)
  if (!credential) return null
  held.set(spaceUri, {
    credential,
    expiresAt: expiryOf(credential) ?? Date.now() + RENEW_BEFORE_MS,
  })
  return credential
}

const expiryOf = (jwt: string): number | null => {
  const payload = jwt.split('.')[1]
  if (!payload) return null
  try {
    const claims = JSON.parse(Buffer.from(payload, 'base64url').toString()) as {
      exp?: unknown
    }
    return typeof claims.exp === 'number' ? claims.exp * 1000 : null
  } catch {
    return null
  }
}

/** Present a credential the way a repo host requires: DPoP, never Bearer. */
export const credentialHeaders = (
  credential: string,
  method: string,
  url: string,
): Record<string, string> => ({
  authorization: `DPoP ${credential}`,
  dpop: dpopProof(method, url, credential),
})

/** Test seam: forget held credentials and the proof key. */
export const resetSpaceCredentials = () => {
  held.clear()
  proofKey = undefined
}
