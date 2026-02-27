# NATS Invalidation Bus

This document describes how StudioFS uses NATS for low-latency cross-client metadata and cache invalidation.

## Why NATS

Without NATS, clients converge via metadata TTL refresh and read-triggered pulls from remote metadata.
With NATS enabled, clients react to committed metadata changes immediately instead of waiting for TTL expiry.

NATS is optional. Correctness does not depend on NATS availability.

## Event Model

- Subject namespace: `fs.invalidate.<volume>.>`
- Currently published subject: `fs.invalidate.<volume>.all`
- Payload: sender identifier from metadata engine (used for self-echo suppression)

Each mounted client subscribes to `fs.invalidate.<volume>.>` for its volume.

## Publish Path

1. A mutating metadata operation updates local Tonbo/redb state.
2. Metadata flush succeeds to remote manifest (CAS-protected write).
3. Publisher emits invalidation message on `fs.invalidate.<volume>.all`.

Important: no successful metadata flush means no invalidation event.  
For low-latency propagation, configure flush cadence accordingly.

## Subscriber Path (StudioVfs)

On each invalidation message:

1. Ignore message if payload sender ID equals local sender ID.
2. Snapshot currently referenced object IDs (`before`).
3. Run `sync_metadata()` with short retries/backoff.
4. Snapshot referenced object IDs again (`after`).
5. Invalidate read caches:
   - `.all` -> clear inode attribute read cache
   - inode-targeted subject (reserved for future publishers) -> invalidate that inode's read cache
6. Evict only dropped cached blocks:
   - remove object IDs present in `before` but absent in `after`
   - keep unchanged object IDs in cache
   - skip pinned blocks (pending upload); eviction is deferred

This gives "refresh immediately, evict selectively" behavior:

- deleted/overwritten cached blocks are removed
- unchanged cached blocks stay hot
- newly referenced blocks are fetched on demand when read

## Consistency Envelope

Observed propagation delay is approximately:

`metadata flush latency + NATS delivery latency + sync_metadata latency`

Typical local setup yields near-immediate Explorer/Finder updates after flush.

If NATS is unavailable:

- client keeps working from local metadata/cache
- convergence falls back to TTL refresh and read-triggered sync

## Recommended Setup

### Baseline

- Use `--nats` on every client mounting the same volume.
- `--nats` value takes precedence over `STUDIOFS_NATS_URL`.
- If `--nats` is not provided, `studiofs mount` defaults to `nats://127.0.0.1:4222`.
- Keep clients and NATS in the same low-latency network segment when possible.
- Use dedicated NATS subjects per volume (already encoded by volume name).

### OS-specific setup (temporary + persistent)

#### Windows (PowerShell)

Variant 1 (one-shot command):

```powershell
studiofs mount mydisk Z: --nats nats://127.0.0.1:4222
```

Variant 2 (persistent PowerShell function in profile):

```powershell
# add to $PROFILE
function Mount-MyDisk {
  studiofs mount mydisk Z: --nats nats://127.0.0.1:4222
}
```

Then open a new terminal and run:

```powershell
Mount-MyDisk
```

#### macOS (zsh)

Variant 1 (one-shot command):

```bash
studiofs mount mydisk /Volumes/mydisk --nats nats://127.0.0.1:4222
```

Variant 2 (persistent alias in `~/.zshrc`):

```bash
echo 'alias studiofs-mount-mydisk="studiofs mount mydisk /Volumes/mydisk --nats nats://127.0.0.1:4222"' >> ~/.zshrc
source ~/.zshrc
```

Variant 3 (persistent function in `~/.zshrc`):

```bash
cat >> ~/.zshrc <<'EOF'
studiofs_mount_mydisk() {
  studiofs mount mydisk /Volumes/mydisk --nats nats://127.0.0.1:4222
}
EOF
source ~/.zshrc
```

#### Linux (bash)

Variant 1 (one-shot command):

```bash
studiofs mount mydisk /mnt/mydisk --nats nats://127.0.0.1:4222
```

Variant 2 (persistent alias in `~/.bashrc`):

```bash
echo 'alias studiofs-mount-mydisk="studiofs mount mydisk /mnt/mydisk --nats nats://127.0.0.1:4222"' >> ~/.bashrc
source ~/.bashrc
```

Variant 3 (persistent function in `~/.bashrc`):

```bash
cat >> ~/.bashrc <<'EOF'
studiofs_mount_mydisk() {
  studiofs mount mydisk /mnt/mydisk --nats nats://127.0.0.1:4222
}
EOF
source ~/.bashrc
```

### Metadata Tuning Profiles

Tune metadata flush cadence to match desired freshness/cost tradeoff:

| Profile | `cache_ttl_secs` | `flush_interval_secs` | `flush_ops_threshold` | `flush_bytes_threshold` |
|---|---:|---:|---:|---:|
| Interactive collaboration | 15-30 | 5-10 | 8-32 | 256 KiB-1 MiB |
| Balanced default | 20-60 | 10-30 | 32-128 | 1-4 MiB |
| Write-heavy / lower metadata churn | 60-120 | 30-60 | 128-512 | 4-16 MiB |

Example:

```bash
studiofs tune-metadata mydisk ^
  --cache-ttl-secs 20 ^
  --flush-interval-secs 8 ^
  --flush-ops-threshold 16 ^
  --flush-bytes-threshold 524288
```

### Operational Guidance

- Use auth/TLS in NATS deployment according to your environment policy.
- Monitor for repeated invalidation sync failures in logs.
- Avoid sharing the same metadata bucket/prefix across unrelated test runs.

## Troubleshooting

### "Changes are not visible immediately"

- Verify every client uses the same `--nats` endpoint and that it is reachable.
- Verify all clients mount the same volume name.
- Verify writer actually flushed metadata (event is emitted after successful flush).

### "Old block still present in cache"

- If block upload is in progress, the block can be pinned temporarily.
- Eviction is deferred until upload completion for pinned objects.

### "NATS is down"

- Mount and I/O still work.
- Freshness degrades to TTL/cold-read sync behavior.
