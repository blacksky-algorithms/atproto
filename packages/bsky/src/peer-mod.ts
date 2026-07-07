import { DataPlaneClient } from './data-plane/client/index.js'

export const PEER_MOD_BADGE = 'peer-moderator'

export interface PeerModConfig {
  ozoneUrl: string | undefined
  ozoneAuth: string | undefined // pre-encoded "Basic xxx" header value
}

export function readPeerModConfig(): PeerModConfig {
  const ozoneUrl = process.env.PEER_MOD_OZONE_URL?.replace(/\/$/, '')
  const user = process.env.PEER_MOD_OZONE_ADMIN_USER
  const password = process.env.PEER_MOD_OZONE_ADMIN_PASSWORD
  const ozoneAuth =
    user && password
      ? `Basic ${Buffer.from(`${user}:${password}`).toString('base64')}`
      : undefined
  return { ozoneUrl, ozoneAuth }
}

export async function hasPeerModBadge(
  dataplane: DataPlaneClient,
  did: string,
): Promise<boolean> {
  const res = await dataplane.getActorBadges({ actor: did })
  return res.badges.includes(PEER_MOD_BADGE)
}

export class PeerModNotConfiguredError extends Error {
  constructor() {
    super('Peer-mod Ozone integration is not configured')
  }
}

export interface EmitEventResult {
  id: string
}

async function emitOzoneEvent(
  cfg: PeerModConfig,
  event: Record<string, unknown>,
  subjectUri: string,
  subjectCid: string,
  peerModDid: string,
): Promise<EmitEventResult> {
  if (!cfg.ozoneUrl || !cfg.ozoneAuth) {
    throw new PeerModNotConfiguredError()
  }
  const body = {
    event,
    subject: {
      $type: 'com.atproto.repo.strongRef',
      uri: subjectUri,
      cid: subjectCid,
    },
    createdBy: peerModDid,
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

// Emit a modEventLabel on Ozone; pass negate=true to remove an existing label.
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
  return emitOzoneEvent(
    cfg,
    {
      $type: 'tools.ozone.moderation.defs#modEventLabel',
      createLabelVals: opts.negate ? [] : [opts.val],
      negateLabelVals: opts.negate ? [opts.val] : [],
      comment: opts.comment,
    },
    opts.subjectUri,
    opts.subjectCid,
    opts.peerModDid,
  )
}

// Best-effort audit-log report so the labeled subject lands in Ozone's queue.
export async function emitReportEvent(
  cfg: PeerModConfig,
  opts: {
    subjectUri: string
    subjectCid: string
    peerModDid: string
    comment?: string
    reportType?: string
  },
): Promise<EmitEventResult> {
  return emitOzoneEvent(
    cfg,
    {
      $type: 'tools.ozone.moderation.defs#modEventReport',
      reportType:
        opts.reportType ?? 'com.atproto.moderation.defs#reasonOther',
      comment: opts.comment,
    },
    opts.subjectUri,
    opts.subjectCid,
    opts.peerModDid,
  )
}

export async function emitAcknowledgeEvent(
  cfg: PeerModConfig,
  opts: {
    subjectUri: string
    subjectCid: string
    peerModDid: string
    comment?: string
  },
): Promise<EmitEventResult> {
  return emitOzoneEvent(
    cfg,
    {
      $type: 'tools.ozone.moderation.defs#modEventAcknowledge',
      comment: opts.comment,
    },
    opts.subjectUri,
    opts.subjectCid,
    opts.peerModDid,
  )
}
