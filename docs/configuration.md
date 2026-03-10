# Configuration Reference

## Global

```bash
studiofs [--log-level <trace|debug|info|warn|error>] <command> ...
```

## Commands

### `init`

Creates local metadata DB and registers volume in PostgreSQL.

```bash
studiofs init <name> \
  --endpoint <s3-endpoint> \
  --bucket <s3-bucket> \
  --access-key <s3-access-key> \
  --secret-key <s3-secret-key> \
  --meta <postgres-server-dsn> \
  [--region <region>] \
  [--block-size <e.g. 8MB>]
```

Important:
- PostgreSQL metadata backend is required.
- StudioFS always uses database `studiofs` and will create it during `init` when permissions allow.

### `discover`

Attaches another client to an existing PG-backed volume.

```bash
studiofs discover <name> --meta <postgres-server-dsn>
```

What it does:
- reads the latest remote-visible checkpoint from PostgreSQL,
- creates local `redb` metadata DB for this client,
- stores discovered `FormatInfo` locally,
- then normal runtime sync continues through PostgreSQL delta batches.

`discover` is the bootstrap path for a new client and a manual repair tool. A
normal client restart/reopen should recover pending PG metadata convergence
without requiring `discover`.

### `mount`

```bash
studiofs mount <name> <mount-point> \
  [--cache-dir <path>] \
  [--cache-size <e.g. 100GB>] \
  [--memory-limit <e.g. 1GB>] \
  [--foreground] \
  [--detach | -d]
```

Notes:
- Cross-client metadata convergence is handled through PostgreSQL sync.
- Client-side PG batching/sync policy is controlled at mount/runtime via environment variables, not stored in the volume.
- If neither `--foreground` nor `--detach` is provided, StudioFS mounts in the foreground by default.
- `--detach` runs the mount as a background daemon.
- `--foreground` and `--detach` are mutually exclusive; providing both returns a CLI argument conflict error.

### `umount`

```bash
studiofs umount <mount-point>
```

### `status`

```bash
studiofs status <name>
```

### `janitor`

```bash
studiofs janitor <name> [--dry-run] [--loop] [--interval-minutes <u64>] [--skip-newer-minutes <u64>] [--max-deletes <u64>]
```

### `destroy`

```bash
studiofs destroy <name>
studiofs destroy <name> --local
```

## Multi-Client Workflow

Client A:
```bash
studiofs init projectfs --endpoint http://127.0.0.1:9000 --bucket studiofs-test --access-key minioadmin --secret-key minioadmin --meta postgres://studiofs:studiofs@127.0.0.1:5432
studiofs mount projectfs Z:
```

Client B:
```bash
studiofs discover projectfs --meta postgres://studiofs:studiofs@127.0.0.1:5432
studiofs mount projectfs Y:
```

## Environment Variables

### Runtime

| Variable | Default | Description |
|---|---|---|
| `STUDIOFS_JANITOR_INTERVAL_SECS` | `1800` | Background janitor interval |
| `STUDIOFS_JANITOR_SKIP_NEWER_SECS` | `900` | Janitor grace window |
| `STUDIOFS_JANITOR_MAX_DELETES` | `500` | Janitor delete cap |
| `STUDIOFS_JANITOR_LEASE_TTL_SECS` | `2700` | Janitor lease TTL |
| `STUDIOFS_PG_SYNC_POLL_SECS` | `2` | Background PG delta sync / flush loop period |
| `STUDIOFS_PG_FLUSH_INTERVAL_SECS` | `60` | Max interval before the client flushes accumulated local PG metadata events |
| `STUDIOFS_PG_FLUSH_OPS_THRESHOLD` | `500` | Client-side PG metadata flush op batch threshold |
| `STUDIOFS_PG_FLUSH_IDLE_MS` | `5000` | Idle grace before PG flush of accumulated local metadata events |
| `STUDIOFS_MAX_DIRTY_BYTES` | `12GB` | RAM budget for accepted dirty blocks before backpressure |
| `STUDIOFS_MAX_SPILL_BYTES` | `64GB` | NVMe spill budget for accepted dirty blocks |
| `STUDIOFS_MAX_DRAIN_BATCH` | `1024` | Max staged blocks drained per flusher cycle |
| `STUDIOFS_FOREGROUND_DRAIN_ASSIST` | `true` | Allows foreground writes to assist local drain when backpressured |
| `STUDIOFS_INGRESS_SEGMENT_MAX_BYTES` | `256MB` | Max size of active append-only ingress segment before rotation |
| `STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS` | `30` | Periodic ingress checkpoint cadence |
| `STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES` | `64MB` | Immediate checkpoint trigger on uncheckpointed ingress bytes |
| `STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS` | `128` | Immediate checkpoint trigger on ingress record mutations |
| `STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG` | `2` | Checkpoint trigger when active segment runs too far ahead |
| `STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS` | `4` | Checkpoint trigger when live ingress index spans too many segments |
| `STUDIOFS_FETCH_MAX_ATTEMPTS` | `8` | Foreground fetch max retry attempts |
| `STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS` | `45` | Per-attempt S3 GET timeout |
| `STUDIOFS_FETCH_RETRY_BASE_MS` | `50` | Fetch retry base backoff |
| `STUDIOFS_FETCH_RETRY_MAX_MS` | `2000` | Fetch retry max backoff |
| `STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS` | `4` | Prefetch fetch retry attempts |
| `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS` | `75` | Max wait for prefetch AIMD permit before skipping |
| `STUDIOFS_UPLOAD_MAX_ATTEMPTS` | `10` | Upload retry attempts |
| `STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS` | `120` | Per-attempt upload timeout |
| `STUDIOFS_UPLOAD_RETRY_BASE_MS` | `200` | Upload retry base backoff |
| `STUDIOFS_UPLOAD_RETRY_MAX_MS` | `5000` | Upload retry max backoff |
| `STUDIOFS_UPLOAD_DELAY_MS` | `2000` | Delay uploads for overwrite/delete coalescing |
| `STUDIOFS_UPLOAD_REQUEUE_BASE_MS` | `5000` | Base backoff for exhausted upload requeue |
| `STUDIOFS_UPLOAD_REQUEUE_MAX_MS` | `120000` | Max backoff for exhausted upload requeue |
| `STUDIOFS_UPLOAD_STATS_INTERVAL_SECS` | `10` | Upload metrics log interval |
| `STUDIOFS_SLAB_FLUSH_INTERVAL_MS` | `10` | Background slab flusher wake interval |
| `STUDIOFS_UPLOAD_CACHE_IO_SLOTS` | `32` | Upload local-cache I/O slots |
| `STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY` | `4` | Penalty multiplied by client wait penalty |
| `STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS` | `250` | User idle grace |
| `STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS` | `5` | Shutdown upload drain max wait |
| `STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS`| `10` | Metadata flush timeout upon shutdown|

### Testing

| Variable | Default | Description |
|---|---|---|
| `STUDIOFS_MINIO_ENDPOINT` | `http://127.0.0.1:9000` | Test S3 endpoint |
| `STUDIOFS_MINIO_ACCESS_KEY` | `minioadmin` | Test access key |
| `STUDIOFS_MINIO_SECRET_KEY` | `minioadmin` | Test secret key |
| `STUDIOFS_MINIO_BUCKET` | `studiofs-test` | Test bucket |
| `STUDIOFS_TEST_PG_DSN` | none | PostgreSQL server DSN for ignored PG tests; StudioFS uses DB `studiofs` |
