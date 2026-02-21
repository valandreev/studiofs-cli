# StudioFS

StudioFS is a cross-platform virtual filesystem that mounts S3-compatible object storage as a local disk.

It is designed for serverless/distributed deployments:
- data is stored in object storage (`<disk>/objects/...`)
- metadata is synchronized via Tonbo manifest (`<disk>/metadata/manifest.json`)
- no central metadata server is required

Supported platforms:
- Windows via WinFSP
- Linux/macOS via FUSE

## Table of Contents
- Overview
- Architecture
- Metadata Synchronization Model
- Multi-client Features
- Commands
- Environment Variables
- Quick Start
- Development

## Overview

Key capabilities:
- Immutable block-based object storage with sequential object IDs
- Local metadata cache (redb) with remote Tonbo manifest synchronization
- Batched metadata flush with CAS retries and backoff
- Read-through metadata cache with TTL
- Optional NATS invalidation bus for faster cross-client convergence
- Automatic background janitor for orphan object cleanup
- Distributed janitor lease lock to avoid concurrent janitors

Path layout is fixed by disk name:
- Data objects: `<disk-name>/objects/...`
- Metadata: `<disk-name>/metadata/manifest.json`

## Architecture

High-level components:
- `studiofs` (CLI)
- `studiofs-vfs` (core VFS orchestration)
- `studiofs-meta` (metadata engines: local redb + Tonbo sync)
- `studiofs-data` (OpenDAL S3 object I/O + object format)
- `studiofs-cache` (memory + disk cache, writeback journal)
- `studiofs-fuse` / `studiofs-winfsp` (OS bridge)

Data path:
1. app write -> VFS splits/updates blocks
2. data object persisted in S3-compatible storage
3. metadata update stored locally, then flushed to Tonbo manifest in batches

Read path:
1. metadata lookup from local cache (refresh on TTL/need)
2. block fetch from memory cache -> disk cache -> object storage
3. return file bytes

## Metadata Synchronization Model

Tonbo synchronization behavior:
- Local source of truth during runtime: redb cache
- Shared source across clients: remote manifest in metadata storage
- Flush policy:
  - periodic (default 60s)
  - early flush on thresholds (default 100 ops or 1 MiB)
  - explicit flush on shutdown
- Consistency mechanism:
  - ETag-based CAS (`If-Match`)
  - retries with backoff on conflicts
- Read coherence:
  - TTL-based refresh (default 20s)
  - cache miss fallback refresh

Discovery behavior:
- Requires only metadata endpoint/bucket/credentials + disk name
- Looks under `<disk>/metadata/manifest.json`
- Data endpoint/bucket/prefix are loaded from manifest metadata

## Multi-client Features

### Optional invalidation bus (NATS)
- Enabled when `STUDIOFS_NATS_URL` is set
- Publisher: Tonbo flush emits `fs.invalidate.<volume>.all`
- Listener: mounted clients subscribe and invalidate local read caches
- Correctness still relies on CAS + TTL even without bus

### Automatic janitor
- Runs in background after mount
- Scans objects under data prefix and removes unreferenced old objects
- Default policy:
  - interval: 30 minutes
  - grace window: 15 minutes
  - delete cap: 500 objects per cycle

### Janitor leader election
- Distributed lease lock in metadata storage:
  - lock path: `<disk>/metadata/janitor.lock`
- Uses conditional object writes (CAS semantics) to avoid concurrent janitors

## Commands

Global usage:
```bash
studiofs [--log-level <trace|debug|info|warn|error>] <command> ...
```

Available commands:
- `init`
- `discover`
- `mount`
- `status`
- `tune-metadata`
- `janitor`
- `destroy`

### init
Initialize a new volume and store data/metadata connection settings.

```bash
studiofs init <name> \
  --endpoint <data-endpoint> \
  --bucket <data-bucket> \
  --access-key <data-access-key> \
  --secret-key <data-secret-key> \
  [--region <data-region>] \
  [--metadata-same-as-data] \
  [--metadata-endpoint <metadata-endpoint>] \
  [--metadata-bucket <metadata-bucket>] \
  [--metadata-access-key <metadata-access-key>] \
  [--metadata-secret-key <metadata-secret-key>] \
  [--metadata-region <metadata-region>] \
  [--metadata-cache-ttl-secs <u64>] \
  [--metadata-flush-interval-secs <u64>] \
  [--metadata-flush-ops-threshold <u32>] \
  [--metadata-flush-bytes-threshold <u64>] \
  [--block-size <e.g. 4MB>]
```

### discover
Attach local metadata cache to an existing disk.

```bash
studiofs discover <name> \
  --endpoint <metadata-endpoint> \
  --bucket <metadata-bucket> \
  --access-key <metadata-access-key> \
  --secret-key <metadata-secret-key> \
  [--region <metadata-region>]
```

### mount
Mount initialized/discovered volume.

```bash
studiofs mount <name> <mount-point> \
  [--cache-dir <path>] \
  [--cache-size <e.g. 100GB>] \
  [--memory-limit <e.g. 1GB>] \
  [--foreground]
```

### status
Show volume settings and counters.

```bash
studiofs status <name>
```

### tune-metadata
Update metadata TTL/flush tuning for an existing volume.

```bash
studiofs tune-metadata <name> \
  [--cache-ttl-secs <u64>] \
  [--flush-interval-secs <u64>] \
  [--flush-ops-threshold <u32>] \
  [--flush-bytes-threshold <u64>]
```

### janitor
Manual janitor run (diagnostic/ops command). Background janitor still runs automatically on mount.

```bash
studiofs janitor <name> \
  [--dry-run] \
  [--loop] \
  [--interval-minutes <u64>] \
  [--skip-newer-minutes <u64>] \
  [--max-deletes <u64>]
```

### destroy
Destroy volume.

```bash
# full wipe (remote + local), with double confirmation
studiofs destroy <name>

# local detach only
studiofs destroy <name> --local
```

## Environment Variables

Runtime:
- `STUDIOFS_NATS_URL`
  - Optional NATS URL for invalidation bus
  - Example: `nats://127.0.0.1:4222`
- `STUDIOFS_JANITOR_INTERVAL_SECS`
  - Background janitor interval (default `1800`)
- `STUDIOFS_JANITOR_SKIP_NEWER_SECS`
  - Janitor grace window for new objects (default `900`)
- `STUDIOFS_JANITOR_MAX_DELETES`
  - Janitor delete cap per cycle (default `500`)
- `STUDIOFS_JANITOR_LEASE_TTL_SECS`
  - Janitor lease TTL for leader lock (default `2700`)

MinIO/Tonbo test-related variables (used by ignored tests):
- `STUDIOFS_MINIO_ENDPOINT` (default `http://127.0.0.1:9000`)
- `STUDIOFS_MINIO_ACCESS_KEY` (default `minioadmin`)
- `STUDIOFS_MINIO_SECRET_KEY` (default `minioadmin`)
- `STUDIOFS_MINIO_BUCKET` (optional; test may create temporary bucket)

## Quick Start

MinIO example:
```bash
studiofs init mydisk \
  --endpoint http://127.0.0.1:9000 \
  --bucket data-bucket \
  --access-key minioadmin \
  --secret-key minioadmin \
  --metadata-same-as-data true

studiofs mount mydisk Z:
```

With separate metadata store:
```bash
studiofs init mydisk \
  --endpoint https://s3.us-east-1.amazonaws.com \
  --bucket data-bucket \
  --access-key DATA_KEY \
  --secret-key DATA_SECRET \
  --metadata-same-as-data false \
  --metadata-endpoint http://127.0.0.1:9000 \
  --metadata-bucket meta-bucket \
  --metadata-access-key minioadmin \
  --metadata-secret-key minioadmin
```

Enable invalidation bus:
```bash
set STUDIOFS_NATS_URL=nats://127.0.0.1:4222
studiofs mount mydisk Z:
```

## Development

Build:
```bash
cargo build --release
```

Run ignored integration tests (requires MinIO/NATS as appropriate):
```bash
cargo test --release -- --ignored
```

Useful docs:
- `docs/tonbo-cutover-plan.md`
- `docs/metadata-schema.md`
