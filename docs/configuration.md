# Configuration Reference

This page describes the current operator surface in the repository. It is not
the canonical SFS-242 rewrite contract. The rewrite source of truth lives in
[juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

## Global

```bash
studiofs [--log-level <trace|debug|info|warn|error>] <command> ...
```

StudioFS stores global defaults for mount in the OS-standard config path under:

- macOS: `~/Library/Application Support/studiofs/config.toml`
- Linux: `~/.config/studiofs/config.toml`
- Windows: `%APPDATA%\studiofs\config.toml`

Values in the config file are used by `studiofs mount` when the corresponding
CLI flags are omitted.
The operator-facing mount surface intentionally stays small. Regular operators
should primarily use:

- `cache_dir`
- `cache_size`
- `read_buffer`
- `readahead`
- `prefetch`
- `writeback_delay`

Metadata-cache defaults and most runtime heuristics remain internal.

Priority for each setting:

1. CLI flag (`--mount-point`, `--cache-dir`, `--cache-size`)
2. Value from `config.toml`
3. Built-in default (`cache-size=100GB`)
4. For `mount-point`, no implicit default is used; `cache-dir` still falls back to `<meta-dir>/cache` when neither CLI nor config provides a value.

For the public runtime IO knobs (`--read-buffer`, `--readahead`, `--prefetch`,
`--writeback-delay`) the effective precedence is:

1. CLI flag
2. Transitional `STUDIOFS_*` environment override for the same setting
3. Persisted `studiofs settings` default
4. Built-in VFS default

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
  [--read-buffer <e.g. 4GB>] \
  [--readahead <blocks>] \
  [--prefetch <count>] \
  [--writeback-delay <ms>] \
  [--principal <name>] \
  [--snapshot-id <uuid>] \
  [--foreground] \
  [--detach | -d]
```

Notes:
- You can persist defaults with `studiofs settings`.
- If `<mount-point>` is omitted, `studiofs mount` uses the configured default from `settings` on non-Windows platforms.
- Precedence for mount path knobs is: CLI args -> persisted `studiofs settings` values -> built-in defaults.
- Public runtime IO tuning starts from persisted `studiofs settings` values, then applies transitional `STUDIOFS_*` env overrides, and finally applies explicit mount flags before validation.
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
- `--cache-size` uses the shared size parser and accepts fractional values (for example `1.5GB`); the value must be greater than `0`.
- `--read-buffer` uses the same size parser and is rounded up to MiB units for VFS config.
- Local data-cache backend provisioning is automatic for the per-volume root under `<cache-dir>/<volume-uuid>` and is owned by `studiofs-cache`; the CLI only resolves cache location and budget inputs.
- Internal runtime thresholds (dirty headroom, upload local-I/O slots, transit headroom) are derived automatically from cache budget.
- On Windows and macOS, StudioFS uses the standard Local-First runtime path for every mount. Foreground `flush()` / `close()` confirm local acceptance and surface sticky upload errors deterministically, while upload and file-version publication continue asynchronously.
- Read access is tracked per handle in core VFS, so sequential media readers keep prefetch/L1 promotion behavior even when another handle on the same inode is doing random seeks.
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
studiofs settings [--mount-point <path>] [--cache-dir <path>] [--cache-size <e.g. 200GB>] [--read-buffer <e.g. 4GB>] [--readahead <blocks>] [--prefetch <count>] [--writeback-delay <ms>]
```

Stores global defaults in `config.toml` for future `studiofs mount` runs.

Notes:
- CLI arguments override existing config values.
- `studiofs settings --mount-point` is not supported on Windows and is ignored there.
  On Windows, specify the drive letter directly when running `studiofs mount`.
- `studiofs settings --mount-point` creates the mount directory if needed on non-Windows systems.
- `studiofs settings --cache-dir` creates the cache directory if needed.
- `studiofs settings` with no options prints current defaults and leaves the file unchanged.
- `studiofs settings` persists the small public runtime IO defaults without turning the full internal `settings.vfs` structure into a user-facing product surface.
- Supported transitional `STUDIOFS_*` environment variables still override persisted runtime defaults for a single mount process before explicit CLI flags are applied.
- Internal cache thresholds are derived from cache budget and are not configurable through `studiofs settings`.

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
- If `<path>` is outside active StudioFS mounts, benchmark runs in `direct-shim`
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
  - direct-shim only: `cache_levels` with L1 hits, misses, bytes served, and lookup latency histograms
- On mounts started by newer StudioFS builds, `e2e` JSON output also includes
  `buffered_read_comparison` with `baseline-read`, `during-upload-read`, and
  `post-upload-read` rows plus the pending-upload observations used to gate
  each phase.
- The terminal summary prints the same `Local-First read comparison` table
  when that mounted upload-state metadata is available.
- `--profile=pyroscope` enables on-demand Pyroscope profiling. The run still
  completes if profiling backends or the endpoint are missing; the command emits
  warnings and continues.
- When profiling is enabled, local pprof artifacts are written beside the JSON
  report:
  - `...-cpu.pprof.gz`
  - `...-memory.pprof.gz`
- `direct-shim` output includes blocking-task pressure counters for cache hot paths (`active`, `active_peak`, `submitted/started/completed`, wait-before-exec and execution timings).
- Current built-in workload volumes are modest (`128MB` direct-shim, `64MB` e2e); for representative measurements on fast NVMe systems, pass larger block/read and concurrency settings.
- For the focused `SFS-253` benchmark recipes and the surrounding Local-First
  validation matrix, see
  [docs/local-first-validation-matrix.md](./local-first-validation-matrix.md).

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

Operator stance for the rewrite:

- Normal mount UX should stay focused on local data-cache and I/O behavior.
- Metadata-cache knobs are internal runtime defaults for developers and
  diagnostics, not the primary operator-facing surface.
- There is intentionally no separate user-facing `dir-entry-cache` knob because
  namespace resolution already runs against the local metadata database.

### Public mount/runtime knobs

| Surface | Default | Description |
|---|---|---|
| `--cache-dir` | `<meta-dir>/cache` | Local data-cache root |
| `--cache-size` | `100GB` | Local data-cache budget |
| `--read-buffer` | `2048MB` | Foreground read-buffer budget |
| `--readahead` | `128` | Sequential readahead width in blocks |
| `--prefetch` | `128` | Prefetch concurrency budget |
| `--writeback-delay` | `2000ms` | Background upload coalescing delay |
| `--principal` / `STUDIOFS_PRINCIPAL` | `*` | Principal used for ACL checks |

### Transitional and expert overrides

The current code still accepts additional `STUDIOFS_*` runtime overrides through
the existing raw/validated VFS config pipeline. They are no longer documented as
the normal operator API.

Use them only when one of these is true:

- you are migrating existing scripts during the `SFS-249` cleanup window
- you are debugging or benchmarking a specific runtime heuristic
- you are working on bridge-specific tuning that belongs to `SFS-250`
- you are changing maintenance/recovery behavior that belongs to `SFS-251`

For the new public IO knobs, the current migration mapping is:

| Public flag | Transitional env override |
|---|---|
| `--read-buffer` | `STUDIOFS_READ_BUFFER_MB` |
| `--readahead` | `STUDIOFS_READAHEAD_WIDTH` |
| `--prefetch` | `STUDIOFS_PREFETCH_CONCURRENCY` |
| `--writeback-delay` | `STUDIOFS_UPLOAD_DELAY_MS` |

Important families that remain supported in code today:

- metadata-cache internals: `STUDIOFS_DENTRY_CACHE_*`,
  `STUDIOFS_PATH_SHORTCUT_CACHE_*`, `STUDIOFS_ATTR_READ_CACHE_*`,
  `STUDIOFS_READDIR_ATTR_BATCH_*`,
  `STUDIOFS_EFFECTIVE_ACL_CACHE_CAPACITY`,
  `STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS`
- read/upload/fetch internals: `STUDIOFS_FETCH_*`, `STUDIOFS_UPLOAD_*`,
  `STUDIOFS_QOS_*`, `STUDIOFS_PREFETCH_*`
- janitor/admin policy: `STUDIOFS_JANITOR_*`, `STUDIOFS_PG_*`
- bridge-specific knobs: `STUDIOFS_FUSE_*`, `STUDIOFS_WINFSP_*`

If an override is still needed regularly by ordinary operators, promote it to a
documented CLI/config flag instead of teaching the whole env matrix again.

Internal metadata-cache defaults:

- `STUDIOFS_DENTRY_CACHE_*`, `STUDIOFS_PATH_SHORTCUT_CACHE_*`,
  `STUDIOFS_ATTR_READ_CACHE_*`, `STUDIOFS_READDIR_ATTR_BATCH_*`,
  `STUDIOFS_EFFECTIVE_ACL_CACHE_CAPACITY`, and
  `STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS` are internal defaults, not
  part of the intended simple operator UX.
- Current numeric defaults live in `crates/studiofs-vfs/src/vfs.rs` as
  `DEFAULT_*` constants.
- Typed config fields and validation live in
  `crates/studiofs-vfs/src/vfs/config.rs` (`VfsConfig` and `VfsConfigRaw`).
- If these values need to move, change them as a code change with supporting
  reasoning rather than treating them as routine mount-time tuning.

Metadata cache observability is emitted through the existing `upload pipeline` / `upload pipeline counters` log events and includes:

- dentry cache, full-path shortcut cache, and attr read cache metrics: hits/misses/evictions, occupancy, protected-tier (hotset) occupancy, and hot-hit contribution
- ingress artifact counters/checkpoint stats (expected to remain at zero during normal local-cache foreground operation unless recovering older artifacts)
- local write-buffer persist batch metrics (batch size in blocks/bytes, batch duration histogram, concurrent batch depth/peak)
- cache shim read/write throughput and latency histograms
- VFS synchronous read fast-path counters (`vfs_read_sync_fast_path_hits`, `vfs_read_sync_fast_path_misses`)
- queue depth/backpressure metrics (`upload_queue_depth`, `upload_queue_depth_peak`, `write_backpressure_wait_ms`, `write_backpressure_events`)
- fast-mode backpressure metrics (`fast_backpressure_wait_ms`, `fast_backpressure_events`, `fast_backpressure_stall_max_ms`)
- upload local reread metrics (`upload_fallback_deferrals`, `upload_fallback_rereads`, `upload_fallback_reread_bytes`, `upload_fallback_reread_cache_misses`, `upload_fallback_wait_ms`, `upload_fallback_wait_max_ms`, `upload_fallback_read_ms`, `upload_fallback_read_max_ms`)
- transit-memory budget metrics (`transit_memory_budget_bytes`, `transit_memory_used_bytes`, `transit_memory_available_bytes`, `transit_memory_waiting_foreground`, `transit_memory_waiting_background`)
- foreground access-policy metrics (`dominant_access_class`, `dominant_prefetch_enabled`, `dominant_cache_promotion_on_read`, `dominant_readahead_base_width`, `dominant_readahead_max_width`, `dominant_readahead_min_prefetch`, `dominant_upload_pressure_boost`, `active_sequential_media_reads`, `active_random_interactive_reads`, `active_sequential_writers`, `active_random_writers`)

Dentry/path shortcut logs also include negative-hit ratio. The summary `upload pipeline` line is emitted while uploads or metadata replication are active and then once more with `pending_uploads=0` and `pending_metadata_ops=0` after the pipeline fully drains. Its `pending_uploads` field reports the queued upload volume in a human-readable size (`1904MB`, `1.9GB`) instead of a block count.
Local backend read/write throughput metrics are lifetime averages since the current backend instance was opened (not short-window/EMA samples).

Upload local-I/O tuning notes:
- upload workers source payload bytes from persisted local blocks (`get_for_upload()` on the local backend) after dequeue; upload correctness does not depend on transient staged buffers.
- upload local-cache I/O slots are held only around that persisted reread phase and then released before remote upload work continues.
- upload workers do not short-circuit through L1/staged payload probes; the persisted-source path is kept uniform so live uploads and startup replay share the same correctness model.
- upload local rereads use `get_for_upload()` on the local backend, which issues a best-effort post-read `DONT_NEED` advisory over the full on-disk block span (header + payload, aligned to block start) on Linux and does not refresh LRU recency.
- adaptive upload penalty is enforced before local-cache read permits are acquired, so throttling creates disk idle gaps before background L2 reads.
- per-upload foreground cache cost is derived internally from cache budget; there is no separate user-tuned penalty knob.
- if `STUDIOFS_UPLOAD_DELAY_MS` already delayed an op, adaptive pre-read sleep is reduced by the already waited delay for that op.
- fetch transit-budget accounting includes assembled object payload plus bounded in-flight fragment headroom.
- if a single fetch request exceeds the derived transit-memory budget, StudioFS returns a fetch error instead of silently clamping the credit request.
- speedometer logs include `foreground_read_p90_present`, `foreground_read_p90_ms`, `foreground_read_window_bytes`, `foreground_read_window_coverage_ms`, and `foreground_read_throughput_bytes_per_sec` for stable parsing even when upload interference is intermittent.
- upload throttling uses the same foreground-read speedometer plus the dominant access policy's `upload_pressure_boost`, so sequential media reads raise upload penalty faster than random interactive probes without adding a separate scheduler loop.
- background/emergency pressure-maintenance eviction can switch to bounded batch mode for large reclaim targets to reduce per-block lock/scheduler overhead while preserving lock release between batches.

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
