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
  [--metadata-cache-ttl-secs <u64>] \
  [--metadata-flush-interval-secs <u64>] \
  [--metadata-flush-ops-threshold <u32>] \
  [--metadata-flush-bytes-threshold <u64>] \
  [--block-size <e.g. 8MB>]
```

Important:
- `--metadata-*` endpoint/bucket/keys options are legacy and rejected.
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

### `mount`

```bash
studiofs mount <name> <mount-point> \
  [--cache-dir <path>] \
  [--cache-size <e.g. 100GB>] \
  [--memory-limit <e.g. 1GB>] \
  [--foreground]
```

Notes:
- No `--nats` option (removed).
- Cross-client metadata convergence is handled through PostgreSQL sync.

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
studiofs init projectfs --endpoint http://127.0.0.1:9000 --bucket studiofs-test --access-key minioadmin --secret-key minioadmin --meta "host=100.123.245.11 port=5432 user=studiofs password=studiofs sslmode=disable"
studiofs mount projectfs Z:
```

Client B:
```bash
studiofs discover projectfs --meta "host=100.123.245.11 port=5432 user=studiofs password=studiofs sslmode=disable"
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
| `STUDIOFS_PG_FLUSH_IDLE_MS` | `2000` | Idle grace before PG flush of accumulated local metadata events |
| `STUDIOFS_MAX_DIRTY_BYTES` | `12GB` | RAM budget for accepted dirty blocks before backpressure |
| `STUDIOFS_MAX_SPILL_BYTES` | `64GB` | NVMe spill budget for accepted dirty blocks |
| `STUDIOFS_MAX_DRAIN_BATCH` | `1024` | Max staged blocks drained per flusher cycle |

### Testing

| Variable | Default | Description |
|---|---|---|
| `STUDIOFS_MINIO_ENDPOINT` | `http://127.0.0.1:9000` | Test S3 endpoint |
| `STUDIOFS_MINIO_ACCESS_KEY` | `minioadmin` | Test access key |
| `STUDIOFS_MINIO_SECRET_KEY` | `minioadmin` | Test secret key |
| `STUDIOFS_MINIO_BUCKET` | `studiofs-test` | Test bucket |
| `STUDIOFS_TEST_PG_DSN` | none | PostgreSQL server DSN for ignored PG tests; StudioFS uses DB `studiofs` |
