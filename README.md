# Blacksky AppView

This is [Blacksky's](https://blacksky.community) fork of the [AT Protocol reference implementation](https://github.com/bluesky-social/atproto) by Bluesky Social PBC. It powers the AppView at `api.blacksky.community`.

We're publishing this for transparency and so other communities can benefit from the work. **This repository is not accepting contributions, issues, or PRs.** If you want the canonical atproto implementation, use [bluesky-social/atproto](https://github.com/bluesky-social/atproto).

## What's Different

All changes are in `packages/bsky` (appview logic), `services/bsky` (runtime config), and one custom migration. Everything else is upstream.

### Why Not the Built-in Firehose Consumer?

The upstream dataplane includes a TypeScript firehose consumer (`subscription.ts`) that indexes events directly. We replaced it with [rsky-wintermute](https://github.com/blacksky-algorithms/rsky), a Rust indexer, for several reasons:

- **Performance at scale**: The TypeScript consumer processes events sequentially. At network scale (~1,000 events/second, 18.5 billion total records), a full backfill at ~90 records/sec would take 6.5 years. Wintermute targets 10,000+ records/sec with parallel queue processing.
- **Backfill architecture**: Wintermute separates live indexing from backfill into independent queues (firehose_live, firehose_backfill, repo_backfill, labels). Live events are never blocked by backfill work.
- **Operational tooling**: Wintermute includes utilities for direct indexing of specific accounts, PLC directory bulk import, label stream replay, blob reference repair, and queue management -- all needed when bootstrapping an AppView from scratch.

The dataplane and appview from this repo still run as-is. They read from the PostgreSQL database that wintermute writes to. We just don't start the built-in firehose subscription.

### Performance & Operational Fixes

These are broadly useful to anyone self-hosting an AppView at scale.

**LATERAL JOIN query optimization** (`packages/bsky/src/data-plane/server/routes/feeds.ts`)
- `getTimeline` and `getListFeed` rewritten with PostgreSQL LATERAL JOINs to force per-user index usage instead of full table scans. Major improvement for users following thousands of accounts.

**Redis caching layer** (`packages/bsky/src/data-plane/server/cache/`)
- Actor profiles (60s TTL), records (5m), interaction counts (30s), post metadata (5m)
- Reduces database load under production traffic
- **Known issue**: The actor cache has a protobuf timestamp serialization bug where `Timestamp` objects lose their `.toDate()` method after JSON round-tripping through Redis, causing incomplete profile hydration on cache hits. We currently run with Redis caching disabled. The fix is to serialize timestamps as ISO strings on cache write and reconstruct on read.

**Notification preferences server-side enforcement** (`packages/bsky/src/api/app/bsky/notification/listNotifications.ts`)
- When the client doesn't specify `reasons`, the server applies the user's saved notification preferences. Without this, preferences are only enforced client-side and have no effect.

**Auth verifier stale signing key fix** (`packages/bsky/src/auth-verifier.ts`)
- On JWT verification retry (`forceRefresh`), bypasses the dataplane's in-memory identity cache and resolves the DID document directly from PLC directory. Fixes authentication failures after account migration where the signing key rotates but the cache holds the old key.

**JSON sanitization** (`packages/bsky/src/data-plane/server/routes/records.ts`)
- Strips null bytes (`\u0000`) and control characters from stored records before JSON parsing. These are valid per RFC 8259 but rejected by Node.js `JSON.parse()`, causing silent `rowToRecord` parse failures in the dataplane that surface as missing posts.

### Community Posts (Blacksky-specific)

Infrastructure for private community posts that live on the AppView rather than individual PDSes. Specific to how Blacksky works, but could serve as a reference for other communities.

- Custom lexicon namespace `community.blacksky.feed.*` with endpoints for submit, get, delete, timeline, and thread views
- Separate `community_post` table (migration: `20260202T120000000Z-add-community-post.ts`)
- Membership gating at the dataplane and API layer
- Integration with `getPostThreadV2` for mixed standard/community post threads
- Requires a separate membership database (`BLACKSKY_MEMBERSHIP_DB_URL`)

## Architecture

```
Bluesky Relay (bsky.network)
     |
     v
rsky-wintermute -----> PostgreSQL 17 <----- Palomar
  (Rust indexer)            |                (Go search)
  - firehose consumer       |                     |
  - backfiller              |                     v
  - label indexer           |               OpenSearch
  - direct indexer          |
                            v
                    bsky-dataplane (gRPC :2585) <--- Redis (optional)
                            |
                            v
                    bsky-appview (HTTP :2584)
                            |
                            v
                    Reverse proxy (Caddy/nginx)
```

### Component Overview

| Component | Source | Purpose |
|-----------|--------|---------|
| **rsky-wintermute** | [blacksky-algorithms/rsky](https://github.com/blacksky-algorithms/rsky) | Rust firehose indexer: consumes events, backfills repos, indexes records into PostgreSQL |
| **rsky-relay** | [blacksky-algorithms/rsky](https://github.com/blacksky-algorithms/rsky) | AT Protocol relay for receiving moderation labels from labeler services |
| **rsky-video** | [blacksky-algorithms/rsky](https://github.com/blacksky-algorithms/rsky) | Video upload service: transcodes via Bunny Stream CDN, uploads blob refs to user PDSes |
| **bsky-dataplane** | This repo (`services/bsky`) | gRPC data layer over PostgreSQL |
| **bsky-appview** | This repo (`services/bsky`) | HTTP API server for `app.bsky.*` XRPC endpoints |
| **Palomar** | [blacksky-algorithms/indigo](https://github.com/blacksky-algorithms/indigo) | Full-text search: indexes profiles and posts into OpenSearch with follower count boosting |
| **palomar-sync** | [blacksky-algorithms/rsky](https://github.com/blacksky-algorithms/rsky) | Syncs follower counts and PageRank scores from PostgreSQL to OpenSearch |

### rsky-wintermute in Detail

Wintermute is a monolithic Rust service with four parallel processing paths:

- **Ingester**: Connects to `bsky.network` firehose via WebSocket, writes events to Fjall (embedded key-value store) queues
- **Indexer**: Reads from queues, parses records, writes to PostgreSQL with `ON CONFLICT` for idempotency
- **Backfiller**: Fetches full repo CAR files from PDSes, unpacks records into the backfill queue
- **Label indexer**: Subscribes to labeler WebSocket streams, processes label create/negate events

Additional CLI tools included in the rsky repo:
- `queue_backfill` -- queue DIDs for backfill from CSV, PDS discovery, or direct DID lists
- `direct_index` -- fetch and index specific repos bypassing queues (useful for fixing individual accounts)
- `label_sync` -- replay label streams from cursor 0 to catch up on missed negations
- `plc_import` -- bulk import handle/DID mappings from PLC directory
- `palomar-sync` -- sync follower counts and PageRank to OpenSearch

### rsky-video

Video upload service for users whose PDS doesn't support Bluesky's `video.bsky.app`. Uses its own DID (`did:web:video.blacksky.community`) to authenticate to user PDSes via service auth JWTs. Flow:

1. Client gets service auth token from PDS (audience: video service DID)
2. Client uploads video bytes to rsky-video
3. rsky-video generates a CID, uploads the blob to the user's PDS
4. Video forwarded to Bunny Stream CDN for transcoding
5. On completion, client creates the post referencing the blob -- PDS validates the blob exists

### Label Handling

Moderation labels come from labeler services (e.g., Bluesky's Ozone) via WebSocket subscription. Wintermute's ingester processes labels in a dedicated `label_live` queue (low volume, separate from the main firehose). The `label_sync` tool can replay a labeler's full stream to catch up on missed negations (label removals) without reinserting labels.

## Setup

### Prerequisites

- **Node.js 18+** and **pnpm** (for building the dataplane and appview)
- **PostgreSQL 17** with the `bsky` schema
- **Redis** (optional, for caching -- see known issue above)
- **rsky-wintermute** consuming the firehose and populating the database
- **OpenSearch** (if running Palomar search)

### Database

The `bsky` schema is created by the dataplane's migrations. On first run, the dataplane will apply all migrations automatically. The only Blacksky-specific migration is `20260202T120000000Z-add-community-post.ts` (community posts table). If you don't need community posts, you can remove it.

rsky-wintermute writes to this same schema. All its INSERT statements use `ON CONFLICT` so it's safe to run wintermute and the dataplane migrations in any order.

### Build

```bash
pnpm install
pnpm build
```

### Run the Dataplane

```bash
node services/bsky/dataplane.js
```

| Variable | Required | Description |
|----------|----------|-------------|
| `DB_PRIMARY_URL` | Yes | PostgreSQL connection string with `?options=-csearch_path%3Dbsky` |
| `DB_REPLICA_URL` | No | Read replica connection string |
| `BSKY_DATAPLANE_PORT` | No | gRPC port (default 2585) |
| `BSKY_REDIS_HOST` | No | Redis host:port for caching (currently recommended to leave disabled) |
| `BLACKSKY_MEMBERSHIP_DB_URL` | No | Separate DB for community membership (Blacksky-specific) |

### Run the AppView

```bash
node services/bsky/api.js
```

| Variable | Required | Description |
|----------|----------|-------------|
| `BSKY_APPVIEW_PORT` | No | HTTP port (default 2584) |
| `BSKY_DATAPLANE_URLS` | Yes | Comma-separated dataplane gRPC URLs |
| `BSKY_DID` | Yes | The AppView's DID (e.g. `did:web:api.example.com`) |
| `BSKY_MOD_SERVICE_DID` | Yes | Ozone moderation service DID |
| `BSKY_ADMIN_PASSWORDS` | Yes | Comma-separated admin passwords for basic auth |

## Operating at Scale

### Backfill Timeline

A full-network backfill (all ~42M users, ~18.5B records) takes weeks even with wintermute's parallel processing. Expect:

- **Live indexing**: Keeps up in real-time from day one (~1,000 events/sec)
- **Full backfill**: 2-4 weeks at 10,000 records/sec depending on PDS responsiveness and network conditions
- **Partial backfill**: Hours to days for a subset of users (e.g., community members only)

During backfill, the AppView is functional but will show incomplete data for users that haven't been backfilled yet. Live events are indexed immediately regardless of backfill progress.

### Problems We Solved Getting Here

These are issues we encountered bootstrapping a full-network AppView. If you're doing the same, you'll likely hit some of these:

**COPY text format JSON corruption**: PostgreSQL's COPY text protocol treats backslash as an escape character. If your bulk loader doesn't escape backslashes in JSON strings, `\"` becomes `"` and you get silently corrupted records. The `record.json` column is type `text` (not `jsonb`), so PostgreSQL won't catch this. We found ~66,000 corrupted records and had to repair them by re-fetching from the public API.

**Null bytes in JSON**: Some AT Protocol records contain `\u0000` (null byte), which is valid JSON per RFC 8259 but rejected by Node.js `JSON.parse()`. The dataplane silently returns null for these records. Strip null bytes before writing to the database.

**Timestamp format sensitivity**: The dataplane expects timestamps with millisecond precision and `Z` suffix (`2026-01-12T19:45:23.307Z`). Nanosecond precision or timezone offset format (`+00:00`) causes subtle sorting and comparison issues.

**Notification table bloat**: Without a unique constraint on `(did, recordUri, reason)`, the notification table grows unbounded with duplicates. Ours reached 1.3 billion rows (663 GB) before we caught it. Adding `ON CONFLICT DO NOTHING` to INSERTs only helps if the unique index exists first, and creating the index requires deduplication of the existing data.

**Post embed tables**: The `post_embed_image` and `post_embed_video` tables aren't populated by default if your indexer doesn't handle them. Without these, the media filter on `getAuthorFeed` returns nothing. These need to be backfilled separately.

**Label negation ordering**: Label negation (removal) events reference the original label by source, URI, and value. If negations arrive before the original label (common during backfill), they're silently dropped. The `label_sync` tool replays the full stream to catch these.

**Fjall queue poisoning**: The Fjall embedded database (used for wintermute's queues) can enter a "poisoned" state after crashes, blocking all queue operations. The fix is to delete the queue database directory and restart -- wintermute will catch up from the relay's cursor (relays keep ~72 hours of history).

**TLS provider initialization**: Rust's `rustls` requires explicitly installing a crypto provider before any TLS connection. Without `rustls::crypto::aws_lc_rs::default_provider().install_default()` at startup, the first WebSocket connection to the firehose panics.

**Signing key rotation after account migration**: When users migrate between PDSes, their signing key changes. The dataplane caches identity data with a staleTTL of 1 hour. During that window, JWT verification fails for migrated users. The fix is to bypass the cache on verification retry and resolve directly from PLC directory.

## Resource Requirements

Based on running a full-network AppView (all ~42M users, ~18.5B records).

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| **CPU** | 16 cores | 48+ cores |
| **RAM** | 64 GB | 256 GB |
| **Storage** | 10 TB NVMe | 28+ TB NVMe (RAID) |
| **PostgreSQL** | Dedicated, same machine or low-latency | Same machine recommended |
| **Network** | Sustained 100 Mbps | 1 Gbps+ |

**Storage breakdown** (approximate, full network):

| Table group | Size |
|-------------|------|
| Posts + records | ~3.5 TB |
| Likes | ~2 TB |
| Follows | ~500 GB |
| Notifications | ~600 GB |
| Indexes | ~4 TB |
| OpenSearch (Palomar) | ~500 GB |

For a smaller community running a partial AppView (indexing only community members), requirements scale roughly linearly with indexed accounts.

## Syncing with Upstream

```bash
git remote add upstream https://github.com/bluesky-social/atproto.git
git fetch upstream
git merge upstream/main
```

Conflicts will typically be in `packages/bsky/src/data-plane/server/routes/` and `packages/bsky/src/api/`. Resolve by keeping our additions alongside upstream changes.

## License

Same as upstream: dual-licensed under MIT and Apache 2.0. See [LICENSE-MIT.txt](./LICENSE-MIT.txt) and [LICENSE-APACHE.txt](./LICENSE-APACHE.txt).
