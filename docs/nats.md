# NATS Invalidation Bus

This document describes how StudioFS uses NATS for low-latency cross-client metadata and cache invalidation.

## Why NATS

Without NATS, clients converge via metadata TTL refresh and read-triggered pulls from remote metadata.
With NATS enabled, clients react to committed metadata changes immediately instead of waiting for TTL expiry.

NATS is optional. Correctness does not depend on NATS availability.

## Delivery Modes

Delivery mode is configured by `STUDIOFS_NATS_DELIVERY`.

| Mode | Value | Behavior | Reconnect / downtime behavior |
|---|---|---|---|
| Core (default) | `core` or unset | Core NATS publish/subscribe only | Best-effort, at-most-once delivery |
| Auto | `auto` | Try JetStream first, fall back to Core NATS when JetStream is unavailable | Durable when JetStream is available; best-effort when fallback is active |
| JetStream | `jetstream` | Use JetStream only (publish ACK + durable consumer ACK) | Durable for successfully ACKed publishes with redelivery; no Core fallback |

For strongest "do not miss invalidations" behavior across reconnects and short outages, use `STUDIOFS_NATS_DELIVERY=jetstream` on all clients.

## Event Model

- Subject namespace: `fs.invalidate.<volume>.>`
- Current publish subject: `fs.invalidate.<volume>.all`
- Payload format: `v1|<sender_id>|<metadata_version_or_->`
- Legacy payload (`<sender_id>`) is still accepted by subscribers for compatibility

JetStream stream/consumer names are generated deterministically from volume/host/cache identifiers.

## Publish Path

1. A mutating metadata operation updates local Tonbo/redb state.
2. Metadata flush succeeds to remote manifest (CAS-protected write).
3. Publisher emits invalidation on `fs.invalidate.<volume>.all`:
   - `core`: publish to Core NATS.
   - `auto`: publish to JetStream (with publish ACK); on JetStream failure, fall back to Core NATS.
   - `jetstream`: publish to JetStream (with publish ACK), no fallback.

Important: no successful metadata flush means no invalidation event.
Durability starts only after JetStream publish ACK; in strict `jetstream` mode a failed publish is not replayed via Core NATS.
For low-latency propagation, configure flush cadence accordingly.

## Subscriber Path (StudioVfs)

On each invalidation message:

1. Ignore self-echo by comparing `sender_id` from payload.
2. Snapshot currently referenced object IDs (`before`).
3. Run `sync_metadata()` with retries/backoff.
4. Validate progress using metadata version token and/or changed referenced object set.
5. Snapshot referenced object IDs again (`after`).
6. Invalidate read caches and evict only dropped cached blocks:
   - dropped object IDs (`before - after`) are evicted
   - unchanged cached blocks stay hot
   - pinned blocks (pending upload) are skipped and evicted later

JetStream-specific behavior:

- Durable pull consumer with explicit ACK.
- Message is ACKed only after sync+apply succeeds.
- On sync/apply failure the client sends NAK with delay, so message is redelivered.

## Consistency Envelope

Observed propagation delay is approximately:

`metadata flush latency + NATS delivery latency + sync_metadata latency`

Typical local setup yields near-immediate Explorer/Finder updates after flush.

If NATS or JetStream is unavailable:

- mount and I/O continue from local metadata/cache
- convergence still happens via TTL refresh and read-triggered sync
- with `auto`, listener/publisher can fall back to Core NATS
- with strict `jetstream`, failed publishes are not replayed via Core fallback

## Recommended Setup

### Baseline

- Use the same `--nats` endpoint on every client mounting the same volume.
- Set `STUDIOFS_NATS_DELIVERY=jetstream` on every client for durable invalidation.
- `--nats` CLI value takes precedence over `STUDIOFS_NATS_URL`.
- Keep clients and NATS in the same low-latency network segment when possible.

### Enable JetStream

PowerShell:

```powershell
$env:STUDIOFS_NATS_DELIVERY = "jetstream"
studiofs mount mydisk Z: --nats nats://127.0.0.1:4222
```

Bash/zsh:

```bash
export STUDIOFS_NATS_DELIVERY=jetstream
studiofs mount mydisk /mnt/mydisk --nats nats://127.0.0.1:4222
```

NATS server must have JetStream enabled (for example `nats-server -js -p 4222`).

### Metadata Tuning Profiles

Tune metadata flush cadence to match desired freshness/cost tradeoff:

| Profile | `cache_ttl_secs` | `flush_interval_secs` | `flush_ops_threshold` | `flush_bytes_threshold` |
|---|---:|---:|---:|---:|
| Interactive collaboration | 15-30 | 5-10 | 8-32 | 256 KiB-1 MiB |
| Balanced default | 20-60 | 10-30 | 32-128 | 1-4 MiB |
| Write-heavy / lower metadata churn | 60-120 | 30-60 | 128-512 | 4-16 MiB |

### Invalidation Tuning (Advanced)

| Variable | Default | Purpose |
|---|---|---|
| `STUDIOFS_INVALIDATION_SYNC_MAX_ATTEMPTS` | `8` | Max retries for `sync_metadata()` after receiving invalidation |
| `STUDIOFS_INVALIDATION_SYNC_BASE_BACKOFF_MS` | `50` | Base backoff for sync retries |
| `STUDIOFS_INVALIDATION_SYNC_MAX_BACKOFF_MS` | `1000` | Max backoff for sync retries |
| `STUDIOFS_INVALIDATION_CONNECT_RETRY_BASE_MS` | `250` | Base backoff for NATS reconnect |
| `STUDIOFS_INVALIDATION_CONNECT_RETRY_MAX_MS` | `10000` | Max backoff for NATS reconnect |
| `STUDIOFS_INVALIDATION_JS_ACK_WAIT_SECS` | `30` | JetStream consumer `ack_wait` |
| `STUDIOFS_INVALIDATION_JS_NAK_DELAY_MS` | `500` | Delay before JetStream redelivery on NAK |

## Troubleshooting

### "Changes are not visible immediately"

- Verify all clients use the same `--nats` endpoint and mount the same volume.
- Verify writer completed metadata flush (event is emitted only after successful flush).
- In `jetstream` mode, verify server JetStream is enabled.

### "Events may be missed on reconnect"

- Use `STUDIOFS_NATS_DELIVERY=jetstream` instead of `core`.
- Ensure no clients are still running in `core` mode for that volume.

### "Old block still present in cache"

- If block upload is in progress, the block can be pinned temporarily.
- Eviction is deferred until upload completion for pinned objects.

### "NATS is down"

- Mount and I/O still work.
- Freshness degrades to TTL/cold-read sync behavior until bus connectivity recovers.
