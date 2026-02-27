# Configuration Reference

## CLI Global Options

```bash
studiofs [--log-level <trace|debug|info|warn|error>] <command> ...
```

## Commands

### `init` — Initialize Volume

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

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `name` | Yes | — | Volume name (also S3 key prefix) |
| `--endpoint` | Yes | — | Data S3 endpoint URL |
| `--bucket` | Yes | — | Data S3 bucket |
| `--access-key` | Yes | — | Data S3 access key |
| `--secret-key` | Yes | — | Data S3 secret key |
| `--region` | No | — | Data S3 region |
| `--metadata-same-as-data` | No | false | Use same S3 config for metadata |
| `--metadata-endpoint` | No | — | Metadata S3 endpoint (if different) |
| `--metadata-bucket` | No | — | Metadata S3 bucket |
| `--metadata-access-key` | No | — | Metadata S3 access key |
| `--metadata-secret-key` | No | — | Metadata S3 secret key |
| `--metadata-region` | No | — | Metadata S3 region |
| `--metadata-cache-ttl-secs` | No | 20 | Read cache TTL |
| `--metadata-flush-interval-secs` | No | 60 | Batch flush interval |
| `--metadata-flush-ops-threshold` | No | 100 | Flush after N operations |
| `--metadata-flush-bytes-threshold` | No | 1 MiB | Flush after N bytes |
| `--block-size` | No | 1 MiB | Block size |

### `discover` — Attach to Existing Volume

```bash
studiofs discover <name> \
  --endpoint <metadata-endpoint> \
  --bucket <metadata-bucket> \
  --access-key <metadata-access-key> \
  --secret-key <metadata-secret-key> \
  [--region <metadata-region>]
```

Only metadata credentials are needed. Data credentials are stored in the Tonbo manifest and loaded automatically.

### `mount` — Mount Volume

```bash
studiofs mount <name> <mount-point> \
  [--nats <nats://host:port>] \
  [--cache-dir <path>] \
  [--cache-size <e.g. 100GB>] \
  [--memory-limit <e.g. 1GB>] \
  [--foreground]
```

| Option | Default | Description |
|--------|---------|-------------|
| `mount-point` | — | Drive letter (Windows) or directory (Linux/macOS) |
| `--nats` | `nats://127.0.0.1:4222` | NATS URL for cross-client invalidation (takes precedence over env) |
| `--cache-dir` | Platform default | Root directory for L2 cache and journals |
| `--cache-size` | 50 GiB | Maximum L2 disk cache size |
| `--memory-limit` | 2 GiB | Maximum L1 memory cache size |
| `--foreground` | false | Run in foreground (don't daemonize) |

### `status` — Volume Status

```bash
studiofs status <name>
```

Shows `FormatInfo` and runtime counters: `NEXT_INODE`, `NEXT_BLOCK_ID`, `USED_SPACE`, `TOTAL_INODES`.

### `tune-metadata` — Adjust Metadata Tuning

```bash
studiofs tune-metadata <name> \
  [--cache-ttl-secs <u64>] \
  [--flush-interval-secs <u64>] \
  [--flush-ops-threshold <u32>] \
  [--flush-bytes-threshold <u64>]
```

Updates tuning parameters in the local `FormatInfo` without remounting. Takes effect on next mount.

### `janitor` — Manual Orphan Cleanup

```bash
studiofs janitor <name> \
  [--dry-run] \
  [--loop] \
  [--interval-minutes <u64>] \
  [--skip-newer-minutes <u64>] \
  [--max-deletes <u64>]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--dry-run` | false | List orphans without deleting |
| `--loop` | false | Run continuously |
| `--interval-minutes` | 30 | Interval between runs (loop mode) |
| `--skip-newer-minutes` | 15 | Grace period for recent objects |
| `--max-deletes` | 500 | Maximum deletions per cycle |

### `test` — S3 Compatibility Test

```bash
studiofs test <name>
```

Runs data suite (PUT/GET/HEAD/range-GET/DELETE at 1KB and 4MB) and metadata suite (conditional PUT with If-Match/If-None-Match).

### `destroy` — Delete Volume

```bash
studiofs destroy <name>           # full wipe (remote + local), interactive confirmation
studiofs destroy <name> --local   # local-only detach
```

## Environment Variables

### Runtime

| Variable | Default | Description |
|----------|---------|-------------|
| `STUDIOFS_JANITOR_INTERVAL_SECS` | `1800` | Background janitor interval |
| `STUDIOFS_JANITOR_SKIP_NEWER_SECS` | `900` | Janitor grace window for new objects |
| `STUDIOFS_JANITOR_MAX_DELETES` | `500` | Janitor delete cap per cycle |
| `STUDIOFS_JANITOR_LEASE_TTL_SECS` | `2700` | Janitor leader lease TTL |

### Testing

| Variable | Default | Description |
|----------|---------|-------------|
| `STUDIOFS_MINIO_ENDPOINT` | `http://127.0.0.1:9000` | Test S3 endpoint |
| `STUDIOFS_MINIO_ACCESS_KEY` | `minioadmin` | Test access key |
| `STUDIOFS_MINIO_SECRET_KEY` | `minioadmin` | Test secret key |
| `STUDIOFS_MINIO_BUCKET` | `studiofs-test` | Test bucket |
| `STUDIOFS_NATS_URL` | `nats://127.0.0.1:4222` | Test NATS server |

## Internal Defaults

### Cache Defaults

| Parameter | Value | Location |
|-----------|-------|----------|
| L1 memory cache | 2 GiB | `BlockCacheConfig::DEFAULT_L1_SIZE` |
| L2 disk cache | 50 GiB | `BlockCacheConfig::DEFAULT_L2_SIZE` |
| Disk free ratio | 10% | `BlockCacheConfig::disk_free_ratio` |
| Max dirty bytes | 8 GiB | `VfsConfig::max_dirty_bytes` |
| Dirty flush interval | 10 ms | VFS background flusher |
| Dirty flush batch size | 256 blocks | `MAX_DRAIN_BATCH` |
| Checkpoint interval | 60s | `CHECKPOINT_INTERVAL_SECS` |
| Eviction check interval | 10s | `EVICTION_INTERVAL_SECS` |
| LRU touch debounce | 10s | `DEBOUNCE_SECS` |

### Metadata Defaults

| Parameter | Value | Location |
|-----------|-------|----------|
| Cache TTL | 20s | `MetadataConfig::cache_ttl_secs` |
| Flush interval | 60s | `MetadataConfig::flush_interval_secs` |
| Flush ops threshold | 100 | `MetadataConfig::flush_ops_threshold` |
| Flush bytes threshold | 1 MiB | `MetadataConfig::flush_bytes_threshold` |
| Inode range allocation | 4,096 | `INODE_RANGE_SIZE` |
| Block ID range allocation | 65,536 | `BLOCK_RANGE_SIZE` |
| CAS retry limit | 10 | `RANGE_CAS_MAX_RETRIES` |

### Fetch Defaults

| Parameter | Value | Location |
|-----------|-------|----------|
| Max fetch attempts | 5 | `MAX_FETCH_ATTEMPTS` |
| Request timeout | 8s | `REQUEST_TIMEOUT` |
| Retry base delay | 50 ms | `RETRY_BASE_DELAY` |
| Retry max delay | 2s | `RETRY_MAX_DELAY` |
| AIMD initial window | 2 | `AimdConfig::fetch_default()` |
| AIMD max window | 32 | `AimdConfig::fetch_default()` |

### I/O Profiles

| Profile | Timeout | Concurrency | Retries |
|---------|---------|-------------|---------|
| Default | 60s | 128 | default |
| Read | 5s | 16 | 0 |
| Write | 20s | 16 | default |

## Tuning Guide

### High-Throughput Writes

```bash
studiofs mount mydisk Z: --memory-limit 4GB --cache-size 200GB
```

The larger the memory limit, the more dirty data can be buffered before backpressure kicks in. The cache size determines how much data can be retained locally before L2 eviction.

### Low-Latency Reads

For workloads with good locality:
```bash
studiofs mount mydisk Z: --memory-limit 8GB --cache-size 100GB
```

Large L1 keeps hot data in RAM. ReadAhead automatically detects sequential patterns.

### Multi-Client Consistency

For tighter consistency between clients:
```bash
studiofs tune-metadata mydisk --cache-ttl-secs 5 --flush-interval-secs 10
```

With NATS:
```bash
studiofs mount mydisk Z: --nats nats://your-nats:4222
```

### NATS Setup Guide

For detailed runtime behavior and recommended tuning profiles, see [NATS Invalidation Bus](nats.md).

Quick recommendations:

- Pass the same `--nats` endpoint on every client for the same volume.
- Keep flush cadence aligned with freshness target:
  - interactive: lower `flush_interval_secs` and thresholds
  - write-heavy: higher thresholds to reduce metadata churn
- Keep NATS close (network latency) to clients and metadata storage path.

### Large Datasets

For datasets exceeding local cache:
```bash
studiofs mount mydisk Z: --cache-size 500GB --memory-limit 2GB
```

L2 eviction automatically manages disk space. The janitor handles orphan S3 objects.
