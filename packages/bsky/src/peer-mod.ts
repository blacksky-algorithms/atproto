// Peer-moderator allowlist + Ozone emitEvent client.
//
// Used by community.blacksky.moderation.{applyLabel,removeLabel,getMyPermissions}.
// The allowlist is read once from PEER_MOD_DIDS at startup; restart on roster change.
// Ozone is called with admin Basic auth (OZONE_ADMIN_USER / OZONE_ADMIN_PASSWORD).

import { DidString } from '@atproto/syntax'

export interface PeerModConfig {
  peerModDids: Set<DidString>
  ozoneUrl: string | undefined
  ozoneAuth: string | undefined // pre-encoded "Basic xxx" header value
}

export function readPeerModConfig(): PeerModConfig {
  const raw = process.env.PEER_MOD_DIDS ?? ''
  const peerModDids = new Set<DidString>(
    raw
      .split(/[\s,]+/)
      .map((s) => s.trim())
      .filter((s) => s.startsWith('did:'))
      .map((s) => s as DidString),
  )
  const ozoneUrl = process.env.PEER_MOD_OZONE_URL?.replace(/\/$/, '')
  const user = process.env.PEER_MOD_OZONE_ADMIN_USER
  const password = process.env.PEER_MOD_OZONE_ADMIN_PASSWORD
  const ozoneAuth =
    user && password
      ? `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`
      : undefined
  return { peerModDids, ozoneUrl, ozoneAuth }
}

export class PeerModNotConfiguredError extends Error {
  constructor() {
    super('Peer-mod Ozone integration is not configured')
  }
}

export interface EmitEventResult {
  id: string
}

// Emit a modEventLabel on Ozone. `negate=true` means we are removing the label
// (Ozone calls this "negate"). Returns the event id so the caller can stash it
// in peer_mod_label.ozoneEventId for later reconciliation.
export async function emitLabelEvent(
  cfg: PeerModConfig,
  opts: {
    subjectUri: string
    subjectCid: string
    val: string
    peerModDid: string
    comment?: string
    negate?: boolean
  },
): Promise<EmitEventResult> {
  if (!cfg.ozoneUrl || !cfg.ozoneAuth) {
    throw new PeerModNotConfiguredError()
  }
  const body = {
    event: {
      $type: 'tools.ozone.moderation.defs#modEventLabel',
      createLabelVals: opts.negate ? [] : [opts.val],
      negateLabelVals: opts.negate ? [opts.val] : [],
      comment: opts.comment,
    },
    subject: {
      $type: 'com.atproto.repo.strongRef',
      uri: opts.subjectUri,
      cid: opts.subjectCid,
    },
    createdBy: opts.peerModDid,
  }
  const res = await fetch(
    `${cfg.ozoneUrl}/xrpc/tools.ozone.moderation.emitEvent`,
    {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: cfg.ozoneAuth,
      },
      body: JSON.stringify(body),
    },
  )
  if (!res.ok) {
    const text = await res.text().catch(() => '')
    throw new Error(`Ozone emitEvent ${res.status}: ${text.slice(0, 200)}`)
  }
  const json = (await res.json()) as { id?: number | string }
  return { id: String(json.id ?? '') }
}
