# Configuration Reference

## Global

```bash
studiofs [--log-level <trace|debug|info|warn|error>] <command> ...
```

StudioFS stores global defaults for mount in the OS-standard config path under:

- macOS: `~/Library/Application Support/studiofs/config.toml`
- Linux: `~/.config/studiofs/config.toml`
- Windows: `%APPDATA%\studiofs\config.toml`

Values in the config file are used by `studiofs mount` when the corresponding CLI flags are omitted.
Advanced VFS tuning lives under the `[vfs]` table in `config.toml`. CLI flags
from `studiofs settings` and `STUDIOFS_*` environment variables override that
file when `studiofs mount` starts.

The `[vfs]` section currently covers:

- dirty/spill budgets and ingress segment sizing
- janitor cadence and lease settings
- read-ahead, prefetch, and directory hydration tuning
- xattr limits and cache capacities
- upload/fetch retry and timeout tuning
- QoS semaphore slot counts
- slab flush behavior and shutdown flush timeout
- materializer drain batch sizing
- publish visibility timeout before manifest commit

Priority for each setting:

1. CLI flag (`--mount-point`, `--cache-dir`, `--memory-limit`, `--cache-size`)
2. Value from `config.toml`
3. Built-in default (`memory-limit=25% of total RAM`, `cache-size=100GB`)
4. For `mount-point`, no implicit default is used; `cache-dir` still falls back to `<meta-dir>/cache` when neither CLI nor config provides a value.

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
  [--block-size <e.g. 16MB>]
```

Important:
- PostgreSQL metadata backend is required.
- StudioFS always uses database `studiofs` and will create it during `init` when permissions allow.
- `init` validates data bucket access (connect/read/write/delete where needed) and PostgreSQL connectivity before creating local metadata.
- `init` no longer performs conditional-write CAS checks (`If-Match` / `If-None-Match`) during initialization.

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
studiofs mount <name> [<mount-point>] \
  [--cache-dir <path>] \
  [--cache-size <e.g. 100GB>] \
  [--memory-limit <e.g. 1GB>] \
  [--l2-profile <default|usb>] \
  [--principal <name>] \
  [--snapshot-id <uuid>] \
  [--foreground] \
  [--detach | -d]
```

Notes:
- You can persist defaults with `studiofs settings`.
- If `<mount-point>` is omitted, `studiofs mount` uses the configured default from `settings` on non-Windows platforms.
- Cross-client metadata convergence is handled through PostgreSQL sync.
- Client-side PG batching/sync policy is controlled at mount/runtime via environment variables, not stored in the volume.
- If `<mount-point>` does not already end with `<name>`, StudioFS mounts at `<mount-point>/<name>` and creates the directory automatically.
- If neither `--foreground` nor `--detach` is provided, StudioFS mounts in the foreground by default.
- On Windows, `<mount-point>` must be a drive letter (for example `Z:`). Directory mount points are not supported.
- On Windows, StudioFS mounts drive letters through WinFSP Mount Manager syntax (`\\.\X:`). If Mount Manager access is denied, mount fails with an explicit error (run elevated or configure WinFSP `MountUseMountmgrFromFSD=1`).
- On Windows, `studiofs mount` now performs a one-time bootstrap: if `MountUseMountmgrFromFSD=1` is missing, StudioFS requests UAC elevation once to set it automatically. After that, regular mounts can run from non-elevated terminals.
- In non-interactive Windows sessions (for example CI/background services), UAC cannot be confirmed; preconfigure `MountUseMountmgrFromFSD=1` manually before running `studiofs mount`.
- `STUDIOFS_DAEMON_MOUNT_POINT` is not used for Windows mounts.
- `--detach` runs the mount as a background daemon.
- `--foreground` and `--detach` are mutually exclusive; providing both returns a CLI argument conflict error.
- `--l2-profile usb` applies conservative L2-local-cache settings intended for external USB SSDs (lower commit concurrency/queue + tighter local L2 I/O timeouts). `default` keeps high-throughput NVMe defaults.
- `--memory-limit` and `--cache-size` use the shared size parser and accept fractional values (for example `1.5GB`); both values must be greater than `0`.
- On Windows and macOS, StudioFS uses the buffered-first runtime path for every mount. Foreground `flush()` / `close()` confirm local acceptance and surface sticky upload errors deterministically, while upload and manifest publication continue asynchronously.
- `--principal` overrides `STUDIOFS_PRINCIPAL` for this mount process.
- `--profile` enables optional Pyroscope profiling for the mount runtime.
- `--profile=off` disables profiling explicitly, even if `STUDIOFS_PROFILE` is set.
- `STUDIOFS_PROFILE=pyroscope|off` controls the same toggle from the environment.
- `STUDIOFS_PYROSCOPE_URL` or `PYROSCOPE_URL` configures the Pyroscope endpoint for the mount process.
- `STUDIOFS_PYROSCOPE_APP_NAME` or `PYROSCOPE_APPLICATION_NAME` overrides the service name.
- `STUDIOFS_PYROSCOPE_SAMPLE_RATE` or `PYROSCOPE_SAMPLE_RATE` sets the CPU sampling rate in Hz.
- `STUDIOFS_PYROSCOPE_TAGS` or `PYROSCOPE_TAGS` adds comma-separated `key=value` labels.
- Default mount tags include platform, volume name, mount mode, cache mode, bridge, and `runtime=mount`.
- When no endpoint is configured, StudioFS still writes a gzipped pprof artifact under `<cache-dir>/<uuid>/profiles/`.
- `--snapshot-id` mounts the selected snapshot as a read-only historical view.
- `--endpoint <URL>` overrides the S3 endpoint URL for the current mount session.
  - Useful for mounting via a local IP address (e.g. `http://192.168.1.50:9000`) instead of the public endpoint configured during `studiofs init`.
  - This override only applies to the current process and is NOT saved to the configuration file.
  - When the endpoint is an IP address, StudioFS automatically uses path-style addressing (required for IP-based S3-compatible services like MinIO).

Windows one-time registry setup (manual alternative):

Set `MountUseMountmgrFromFSD=1` manually (run in elevated PowerShell):

```powershell
$paths = @(
  'HKLM:\SOFTWARE\WOW6432Node\WinFsp',
  'HKLM:\SOFTWARE\WinFsp'
)
foreach ($p in $paths) {
  New-Item -Path $p -Force | Out-Null
  New-ItemProperty -Path $p -Name 'MountUseMountmgrFromFSD' -PropertyType DWord -Value 1 -Force | Out-Null
}
```

Verify value:

```powershell
Get-ItemProperty -Path 'HKLM:\SOFTWARE\WOW6432Node\WinFsp' -Name 'MountUseMountmgrFromFSD' -ErrorAction SilentlyContinue
Get-ItemProperty -Path 'HKLM:\SOFTWARE\WinFsp' -Name 'MountUseMountmgrFromFSD' -ErrorAction SilentlyContinue
```

Remove value (rollback):

```powershell
Remove-ItemProperty -Path 'HKLM:\SOFTWARE\WOW6432Node\WinFsp' -Name 'MountUseMountmgrFromFSD' -ErrorAction SilentlyContinue
Remove-ItemProperty -Path 'HKLM:\SOFTWARE\WinFsp' -Name 'MountUseMountmgrFromFSD' -ErrorAction SilentlyContinue
```

### `settings`

```bash
studiofs settings [--mount-point <path>] [--cache-dir <path>] [--memory-limit <e.g. 1GB>] [--cache-size <e.g. 200GB>]
studiofs settings [--slab-flush-interval <ms>] [--disable-bg-slab-flush] [--enable-bg-slab-flush]
```

Stores global defaults in `config.toml` for future `studiofs mount` runs.

Notes:
- CLI arguments override existing config values.
- `studiofs settings --mount-point` is not supported on Windows and is ignored there.
  On Windows, specify the drive letter directly when running `studiofs mount`.
- `studiofs settings --mount-point` creates the mount directory if needed on non-Windows systems.
- `studiofs settings --cache-dir` creates the cache directory if needed.
- `studiofs settings` with no options prints current defaults and leaves the file unchanged.
- `studiofs settings --slab-flush-interval` stores the background slab flusher wake interval in the `[vfs]` section.
- `studiofs settings --disable-bg-slab-flush` disables the background slab flusher in the `[vfs]` section.
- `studiofs settings --enable-bg-slab-flush` re-enables the background slab flusher in the `[vfs]` section.
- `studiofs settings --slab-flush-interval 0` disables the background slab flusher (for environments where zero is a preferred explicit override).
- The `[vfs]` section also stores advanced runtime knobs such as upload/fetch retry windows, QoS slot counts, materializer drain batch size, and publish visibility timeout.

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

### `fsck`

Checks local metadata for namespace inconsistencies (dangling dentries,
orphan inodes, `nlink` mismatches, and pending/deferred undo state. For
PostgreSQL-backed volumes, StudioFS first syncs metadata from PostgreSQL and
then scans.

```bash
studiofs fsck <name>
studiofs fsck <name> --repair
```

Notes:
- default mode is read-only report.
- if findings exist, interactive mode asks for confirmation before repair.
- `--repair` applies repairs immediately:
  - undo replay (`recover_undo_logs`)
  - dangling dentry cleanup
  - orphan regular/symlink inode cleanup (blocks are moved to pending GC)
- orphan directories and hardlink-like orphan cases (`nlink > 1`) are reported
  and skipped for safety.
- `nlink` mismatches are currently report-only.

### `test-cache`

Runs cache benchmarks without changing regular runtime configuration.

```bash
studiofs test-cache \
  <path> \
  [--block-size <e.g. 16MB>] \
  [--read-size <e.g. 1MB>] \
  [--small-max <e.g. 10KB>] \
  [--medium-max <e.g. 16MB>] \
  [--sizes <small=60,medium=30,large=10|4KB,128KB,1MB>] \
  [--concurrency <usize>] \
  [--sequential-streams] \
  [--io-depth <usize>] \
  [--warmup-runs <u32>] \
  [--access-pattern <sequential|random|mixed>] \
  [--l1-limit <e.g. 2GB>] \
  [--l2-limit <e.g. 100GB>] \
  [--profile[=pyroscope]]
```

Defaults:
- `--block-size 16MB`
- `--read-size` defaults to `block-size`
- `--small-max 10KB`
- `--medium-max` defaults to `block-size`
- `--sizes` defaults to existing tiered profile: `100` small, `100` medium, `100` large files
- `--concurrency 1`
- `--sequential-streams false`
- `--io-depth 32`
- `--warmup-runs 0`
- `--access-pattern sequential`
- `--l1-limit 2GB`
- `--l2-limit 100GB`
- `--profile` disabled by default
- `--profile=pyroscope` enables Pyroscope sampling; `--profile` without a
  value is accepted as shorthand for the same mode

Profiling environment variables:
- `STUDIOFS_PYROSCOPE_URL` or `PYROSCOPE_URL`: endpoint base URL for Grafana
  Pyroscope
- `STUDIOFS_PYROSCOPE_APP_NAME` or `PYROSCOPE_APPLICATION_NAME`: service name
  shown in Pyroscope/Grafana
- `STUDIOFS_PYROSCOPE_SAMPLE_RATE` or `PYROSCOPE_SAMPLE_RATE`: CPU sampling
  frequency in Hz (default `100`)
- `STUDIOFS_PYROSCOPE_USERNAME` / `STUDIOFS_PYROSCOPE_PASSWORD`: optional
  basic-auth credentials
- `STUDIOFS_PYROSCOPE_TENANT_ID`: optional Grafana Cloud / multi-tenant header

Notes:
- If `<path>` is outside active StudioFS mounts, benchmark runs in `direct-l2`
  mode and treats `<path>` as cache directory.
- If `<path>` is inside an active StudioFS mount, benchmark runs in `e2e` mode
  through the already running mounted process and uses
  `<path>/.studiofs-test-cache` for temporary workload files.
- `--warmup-runs` executes write+read cycles before measured phases; warm-up cycles are excluded from reported metrics.
- Throughput in benchmark reports is phase-scoped: write-side metrics are
  computed from write-phase wall time, and read-side metrics are computed from
  read-phase wall time instead of total benchmark runtime.
- `--sizes` accepts either class percentages (`small=60,medium=30,large=10`) or an explicit list (`4KB,128KB,1MB,16MB`).
- `--sequential-streams` runs each stream over its own segment in order and emits per-stream read/write metrics in JSON and terminal output.
- In `--sequential-streams` mode, effective in-flight I/O is bounded by `min(--concurrency, --io-depth)` because each stream executes I/O serially.
- Effective in-flight queue pressure is driven by `--io-depth`; worker count is raised to at least that value.
- Output includes a human-readable summary and JSON payload suitable for cross-platform run comparison.
- JSON report includes a stable `run_id` for correlating `test-cache` output with tracing/profile hooks.
- JSON report includes `advanced_metrics`:
  - `size_class_boundaries` (`small_max_bytes`, `medium_max_bytes`)
  - `size_class_stats` with p50/p95/p99 + throughput per `small`/`medium`/`large`
  - `queue_backpressure` with L2 commit queue depth/worker/backpressure percentiles
  - direct-l2 only: `cache_levels` with L1/L2 hits, misses, bytes served, and per-level latency histograms
- `--profile=pyroscope` enables on-demand Pyroscope profiling. The run still
  completes if profiling backends or the endpoint are missing; the command emits
  warnings and continues.
- When profiling is enabled, local pprof artifacts are written beside the JSON
  report:
  - `...-cpu.pprof.gz`
  - `...-memory.pprof.gz`
- `direct-l2` output includes blocking-task pressure counters for cache hot paths (`active`, `active_peak`, `submitted/started/completed`, wait-before-exec and execution timings).
- Current built-in workload volumes are modest (`128MB` direct-l2, `64MB` e2e); for representative measurements on fast NVMe systems, pass larger block/read and concurrency settings.

### `destroy`

```bash
studiofs destroy <name>
studiofs destroy <name> --local
```

Notes:
- `destroy` removes StudioFS local metadata files for the volume.
- Custom local data cache directories are not auto-detected; remove them manually after destroy (the command prints the volume UUID for this).

### `dump`

Creates a diagnostic dump from PostgreSQL metadata for a volume.

```bash
studiofs dump <name> --path-to-dump <dump.json>
```

What it does:
- opens local metadata for the volume,
- forces metadata flush/sync (`flush_metadata` + `sync_metadata`),
- exports the dump from PostgreSQL (`volumes`, `metadata_state`, `counters`, `event_log`).

Note:
- PostgreSQL is treated as source of truth for dump export.

### `load`

Loads a dump into PostgreSQL.

```bash
studiofs load --path-to-dump <dump.json> --postgresql-adress <postgres-server-dsn>
```

Alias:
- `--postgresql-address` is supported as alias for `--postgresql-adress`.

What it does:
- restores/updates target volume record in `volumes` (matched by UUID from dump),
- replaces target volume rows in `metadata_state`, `counters`, `event_log`, `snapshots`.

### `snapshot`

Create or list immutable snapshots.

```bash
studiofs snapshot <name> create [--root-ino <ino>]
studiofs snapshot <name> list
```

Behavior:
- `create` is immediate and local-first (state starts as `pending`).
- background PG replication publishes snapshot metadata and pinned object ids.
- replicated snapshots transition to `synced` and become discoverable on peers.
- if PostgreSQL is unavailable at snapshot creation time, the snapshot remains a
  local-only administrative artifact until the owning client reconciles and
  publishes it later.
- janitor protection is cluster-wide only for snapshots that are already
  published through PostgreSQL.

## Multi-Client Workflow

Client A:
```bash
studiofs init projectfs --endpoint http://127.0.0.1:9000 --bucket studiofs-test --access-key studiofs --secret-key studiofs123 --meta postgres://studiofs:studiofs@127.0.0.1:5432
studiofs mount projectfs /mnt/studio/projectfs
```

Client B:
```bash
studiofs discover projectfs --meta postgres://studiofs:studiofs@127.0.0.1:5432
studiofs mount projectfs /mnt/studio/projectfs-b
```

## Environment Variables

### Runtime

Size-formatted values (for `*_BYTES` variables and size-like CLI flags) accept
fractional inputs such as `1.5GB`; parsing rounds down fractional bytes.
Supported suffixes are `B`, `KB`, `MB`, `GB`, `TB`, `PB`, `EB` using binary
multipliers (`1KB = 1024B`).

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
| `STUDIOFS_PRINCIPAL` | `*` | Principal identity used by VFS ACL checks (`no_access` / `ro` / `rw`) |
| `STUDIOFS_MAX_DIRTY_BYTES` | `12GB` | RAM budget for accepted dirty blocks before backpressure |
| `STUDIOFS_MAX_SPILL_BYTES` | `64GB` | NVMe spill budget for accepted dirty blocks |
| `STUDIOFS_XATTR_MAX_BYTES` | `4MB` | Maximum bytes per xattr value when writing metadata (enforced in VFS before metadata persistence) |
| `STUDIOFS_DENTRY_CACHE_CAPACITY` | `65536` | Max number of `(parent_ino, name)` dentry cache entries (positive + negative) |
| `STUDIOFS_DENTRY_CACHE_BYTES` | `64MB` | Max in-memory bytes for dentry cache entries (hard cap, SLRU eviction) |
| `STUDIOFS_PATH_SHORTCUT_CACHE_CAPACITY` | `16384` | Max number of full-path shortcut cache entries in `NamespaceResolver` |
| `STUDIOFS_PATH_SHORTCUT_CACHE_BYTES` | `16MB` | Max in-memory bytes for full-path shortcut cache entries |
| `STUDIOFS_ATTR_READ_CACHE_CAPACITY` | `65536` | Max number of inode attribute read-through cache entries |
| `STUDIOFS_ATTR_READ_CACHE_BYTES` | `32MB` | Max in-memory bytes for attribute read-through cache entries |
| `STUDIOFS_EFFECTIVE_ACL_CACHE_CAPACITY` | `65536` | Max number of cached effective ACL entries before ACL cache reset |
| `STUDIOFS_READDIR_ATTR_BATCH_CHUNK` | `256` | Max inode attrs fetched per post-`readdir` hydration chunk |
| `STUDIOFS_READDIR_ATTR_BATCH_MAX_INODES` | `4096` | Max directory children hydrated into attr cache per `readdir` call |
| `STUDIOFS_MAX_DRAIN_BATCH` | `1024` | Max staged blocks drained per flusher cycle |
| `STUDIOFS_L2_COMMIT_WORKERS` | `32` | Number of bounded L2 commit scheduler workers |
| `STUDIOFS_L2_COMMIT_QUEUE_CAPACITY` | `4096` | L2 commit queue capacity before enqueue backpressure |
| `STUDIOFS_L2_READ_TIMEOUT_MS` | `5000` | Max wait for local L2 read before treating it as cache miss (read can continue from remote fetch path) |
| `STUDIOFS_L2_WRITE_TIMEOUT_MS` | `5000` | Max wait for local L2 write/commit attempt before timeout/retry/drop behavior |
| `STUDIOFS_L2_SYNC_FD_CACHE_CAPACITY` | `4096` | Capacity of L2 sync-read open-file descriptor LRU cache (set lower on hosts with strict FD limits) |
| `STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS` | `1000` | WinFSP FileInfo metadata timeout; accepts milliseconds or `-1` / `max` for NT Cache Manager delegation |
| `STUDIOFS_WINFSP_DIR_INFO_TIMEOUT_MS` | `1000` | WinFSP DirInfo metadata timeout; accepts milliseconds or `-1` / `max` |
| `STUDIOFS_WINFSP_VOLUME_INFO_TIMEOUT_MS` | `1000` | WinFSP VolumeInfo metadata timeout; accepts milliseconds or `-1` / `max` |
| `STUDIOFS_PUBLISH_VISIBILITY_TIMEOUT_SECS` | `30` | Max time to wait for uploaded objects to become visible before publishing a manifest |
| `STUDIOFS_FOREGROUND_DRAIN_ASSIST` | `true` | Allows foreground writes to assist local drain when backpressured |
| `STUDIOFS_CACHE_SAFETY_MODE` | `fast` | Local cache durability policy: `fast` (single local payload write, best-effort startup self-heal) or `durable` (WAL-backed local accept + checkpoint recovery) |
| `STUDIOFS_INGRESS_SEGMENT_MAX_BYTES` | `256MB` | Max size of active append-only ingress segment before rotation |
| `STUDIOFS_INGRESS_CHECKPOINT_INTERVAL_SECS` | `30` | Periodic ingress checkpoint cadence |
| `STUDIOFS_INGRESS_CHECKPOINT_MIN_APPEND_BYTES` | `64MB` | Immediate checkpoint trigger on uncheckpointed ingress bytes |
| `STUDIOFS_INGRESS_CHECKPOINT_MIN_MUTATIONS` | `128` | Immediate checkpoint trigger on ingress record mutations |
| `STUDIOFS_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG` | `2` | Checkpoint trigger when active segment runs too far ahead |
| `STUDIOFS_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS` | `4` | Checkpoint trigger when live ingress index spans too many segments |
| `STUDIOFS_UPLOAD_MAX_ATTEMPTS` | `10` | Max attempts for upload retries |
| `STUDIOFS_UPLOAD_REQUEST_TIMEOUT_SECS` | `120` | Per-upload request timeout |
| `STUDIOFS_UPLOAD_RETRY_BASE_MS` | `200` | Upload retry base delay |
| `STUDIOFS_UPLOAD_RETRY_MAX_MS` | `5000` | Upload retry max delay |
| `STUDIOFS_FETCH_MAX_ATTEMPTS` | `8` | Max attempts for foreground fetch retries |
| `STUDIOFS_PREFETCH_FETCH_MAX_ATTEMPTS` | `4` | Max attempts for best-effort prefetch fetches |
| `STUDIOFS_FETCH_REQUEST_TIMEOUT_SECS` | `45` | Per-fetch request timeout |
| `STUDIOFS_FETCH_RETRY_BASE_MS` | `50` | Fetch retry base delay |
| `STUDIOFS_FETCH_RETRY_MAX_MS` | `2000` | Fetch retry max delay |
| `STUDIOFS_FETCH_PREFETCH_RESERVE` | `0` | Reserved fetch slots kept free for foreground reads; `0` uses the runtime default |
| `STUDIOFS_FETCH_PREFETCH_ACQUIRE_WAIT_MS` | `75` | Max time a prefetch waits for a fetch slot before skipping |
| `STUDIOFS_QOS_ACCEPT_SLOTS` | `64` | QoS semaphore slots for accept operations |
| `STUDIOFS_QOS_MATERIALIZE_SLOTS` | `2` | QoS semaphore slots for materialization |
| `STUDIOFS_QOS_UPLOAD_SLOTS` | `4` | QoS semaphore slots for uploads |
| `STUDIOFS_QOS_COMMIT_SLOTS` | `4` | QoS semaphore slots for commits |
| `STUDIOFS_QOS_FETCH_SLOTS` | `128` | QoS semaphore slots for fetches |
| `STUDIOFS_UPLOAD_WORKER_COUNT` | `4` | Upload worker pool size |
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
| `STUDIOFS_UPLOAD_STATS_INTERVAL_SECS` | `1` | Upload metrics log interval |
| `STUDIOFS_SLAB_FLUSH_INTERVAL_MS` | `10` | Background slab flusher wake interval (`0` disables background flusher) |
| `STUDIOFS_DISABLE_BACKGROUND_SLAB_FLUSH` | `false` | Disables the background slab flusher |
| `STUDIOFS_UPLOAD_CACHE_IO_SLOTS` | `32` | Upload local-cache I/O slots used only for the local read phase in upload workers |
| `STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY` | `4` | Per-upload slot cost when foreground reads are active; must be `<= STUDIOFS_UPLOAD_CACHE_IO_SLOTS` (invalid config fails validation before mount) |
| `STUDIOFS_TRANSIT_MEMORY_BUDGET` | `80% of memory limit` | Global transit-memory credit budget for active in-flight fetch/upload buffers |
| `STUDIOFS_UPLOAD_USER_IDLE_GRACE_MS` | `250` | User idle grace |
| `STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS` | `50` | Foreground-read P90 latency target for upload throttling |
| `STUDIOFS_UPLOAD_FG_READ_LATENCY_WINDOW_MS` | `3000` | Foreground-read speedometer window |
| `STUDIOFS_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS` | `2` | Keep last P90 for this many windows when samples pause |
| `STUDIOFS_UPLOAD_L2_LOOKUP_SPIKE_THRESHOLD_MS` | `10` | Legacy compatibility floor for `STUDIOFS_UPLOAD_FG_READ_LATENCY_TARGET_MS` |
| `STUDIOFS_SHUTDOWN_UPLOAD_DRAIN_SECS` | `5` | Max graceful upload-drain wait before forced uploader cancellation |
| `STUDIOFS_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS`| `10` | Metadata flush timeout upon shutdown|

Metadata cache observability is emitted through the existing `upload pipeline` / `upload pipeline counters` log events and includes:

- dentry cache, full-path shortcut cache, and attr read cache metrics: hits/misses/evictions, occupancy, protected-tier (hotset) occupancy, and hot-hit contribution
- ingress append throughput (`ops`, `bytes`) and latency histogram
- materialize batch metrics (batch size in blocks/bytes, batch duration histogram, concurrent batch depth/peak)
- cache L2 read/write throughput and latency histograms
- L2 commit scheduler counters (`cache_l2_commit_queue_depth`, `cache_l2_commit_queue_depth_peak`, `cache_l2_commit_workers_active`, `cache_l2_commit_workers_active_peak`, `cache_l2_commit_backpressure_events`)
- VFS synchronous read fast-path counters (`vfs_read_sync_fast_path_hits`, `vfs_read_sync_fast_path_misses`)
- queue depth/backpressure metrics (`upload_queue_depth`, `upload_queue_depth_peak`, `write_backpressure_wait_ms`, `write_backpressure_events`)
- fast-mode backpressure metrics (`fast_backpressure_wait_ms`, `fast_backpressure_events`, `fast_backpressure_stall_max_ms`)
- staged upload payload metrics (`upload_staged_payload_hits`, `upload_staged_payload_misses`, `upload_staged_payload_evictions`, `upload_staged_payload_target_bytes`, `upload_staged_payload_min_bytes`, `upload_staged_payload_max_bytes`)
- transit-memory budget metrics (`transit_memory_budget_bytes`, `transit_memory_used_bytes`, `transit_memory_available_bytes`, `transit_memory_waiting_foreground`, `transit_memory_waiting_background`)

Dentry/path shortcut logs also include negative-hit ratio. The summary `upload pipeline` line is emitted while uploads or metadata replication are active and then once more with `pending_uploads=0` and `pending_metadata_ops=0` after the pipeline fully drains. Its `pending_uploads` field reports the queued upload volume in a human-readable size (`1904MB`, `1.9GB`) instead of a block count.

Upload local-I/O tuning notes:
- upload workers first try a transient staged payload seeded by the materializer; local-cache I/O slots are only used on staging miss and are released immediately after the cache read.
- adaptive upload penalty is enforced before local-cache read permits are acquired, so throttling creates disk idle gaps before background L2 reads.
- if `STUDIOFS_UPLOAD_DELAY_MS` already delayed an op, adaptive pre-read sleep is reduced by the already waited delay for that op.
- when `STUDIOFS_UPLOAD_CACHE_IO_USER_PENALTY` is configured above `STUDIOFS_UPLOAD_CACHE_IO_SLOTS`, StudioFS fails config validation before VFS startup.
- staged upload payload sizing is internal and adaptive rather than a user-facing knob; it is anchored to upload dispatch capacity and bounded by dirty-memory budget.
- fetch transit-budget accounting includes assembled object payload plus bounded in-flight fragment headroom.
- if a single fetch request exceeds `STUDIOFS_TRANSIT_MEMORY_BUDGET`, StudioFS returns a fetch error instead of silently clamping the credit request.
- speedometer logs include `foreground_read_p90_present` and `foreground_read_p90_ms` for stable parsing even when no fresh samples exist.

Configuration validation notes:
- StudioFS resolves user input through a raw/validated configuration pipeline before creating `StudioVfs`.
- `STUDIOFS_PREFETCH_CONCURRENCY` is normalized to a minimum of `16` when an explicit lower value is supplied in raw config.
- `STUDIOFS_UPLOAD_WORKER_COUNT=0` is treated as "use runtime default", which resolves to `4` workers.

### Testing

Primary local development and infra-backed tests use RustFS as the
S3-compatible backend.

| Variable | Default | Description |
|---|---|---|
| `STUDIOFS_S3_ENDPOINT` | `http://127.0.0.1:9000` | Test S3 endpoint |
| `STUDIOFS_S3_ACCESS_KEY` | `studiofs` | Test access key |
| `STUDIOFS_S3_SECRET_KEY` | `studiofs123` | Test secret key |
| `STUDIOFS_S3_BUCKET` | `studiofs-test` | Test bucket |
| `STUDIOFS_TEST_PG_DSN` | none | PostgreSQL server DSN for ignored PG tests; StudioFS uses DB `studiofs` |
