# Configuration Reference

This page describes the operator surface for the canonical mounted-IO contract
in [juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).

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

Local per-client runtime state lives under the StudioFS client root. By default
that root is `~/.studiofs`. Set `STUDIOFS_CLIENT_ROOT=<path>` to isolate a
client sandbox without changing remote volume identity, PostgreSQL metadata, or
S3 object layout. Relative values are resolved to absolute paths when the client
starts. The root contains local metadata databases, mount records, daemon logs,
investigation artifacts, and the default cache path `<client-root>/meta/<name>/cache`
when neither `--cache-dir` nor persisted `cache_dir` is set.

The operator-facing mount surface intentionally stays small. Regular operators
should primarily use:

- `cache_dir`
- `cache_size`
- `read_buffer`
- `readahead`
- `prefetch`

Metadata-cache defaults and most runtime heuristics remain internal.

Priority for each setting:

1. CLI flag (`--mount-point`, `--cache-dir`, `--cache-size`)
2. Value from `config.toml`
3. Built-in default (`cache-size=100GB`)
4. For `mount-point`, no implicit default is used; `cache-dir` still falls back to `<meta-dir>/cache` when neither CLI nor config provides a value.

For the public runtime IO knobs (`--read-buffer`, `--readahead`, `--prefetch`)
the effective precedence is:

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
  [--region <region>]
```

Important:
- PostgreSQL metadata backend is required.
- StudioFS always uses database `studiofs` and will create it during `init` when permissions allow.
- `init` validates data bucket access (connect/read/write/delete where needed) and PostgreSQL connectivity before creating local metadata.
- `init` no longer performs conditional-write CAS checks (`If-Match` / `If-None-Match`) during initialization.
- The current CLI does not expose a public block-size flag on `init`; newly
  initialized volumes report the canonical 4 MiB block size.

### Transparent encryption at rest

StudioFS encrypts remote payload objects automatically. Operators do not enable
or configure encryption through `init`, `discover`, `mount`, or `settings`.

The remote object-store boundary receives encrypted `SFEC` objects. The local
payload cache, upload stage, local metadata, PostgreSQL metadata, filenames,
file sizes, process memory, and bridge buffers remain plaintext by design. The
current implementation supports only XChaCha20-Poly1305 and does not provide
AES-256-GCM, RSA wrapping, user passphrases, KMS integration, or legacy
plaintext remote-volume compatibility.

### Transport encryption

StudioFS' transport policy is defined in
[transport-encryption-contract.md](./transport-encryption-contract.md). In
short, S3-compatible endpoints honor the operator-selected `https://` or
`http://` scheme; bare host-port S3 endpoints without a scheme are accepted only
for loopback development/test endpoints. Non-local PostgreSQL DSNs use
TLS-preferred or TLS-required transport unless the operator explicitly sets
`sslmode=disable`; that cleartext operator choice is accepted and logged at info
level. Local loopback PostgreSQL DSNs may also use `sslmode=disable` for
development/test fixtures.

Production example:

```bash
studiofs init projectfs --endpoint https://s3.example.com --bucket studiofs-prod --access-key YOUR_ACCESS_KEY --secret-key YOUR_SECRET_KEY --meta "postgresql://user:pass@db.example.com/studiofs?sslmode=require"
```

Loopback-only development/test example:

```bash
studiofs init projectfs --endpoint http://127.0.0.1:9000 --bucket studiofs-test --access-key studiofs --secret-key studiofs123 --meta "postgres://studiofs:studiofs@127.0.0.1:5432?sslmode=disable"
```

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
- If mount still fails after the registry setup, especially immediately after a WinFSP install or upgrade, refresh the WinFSP runtime state before retrying. Restart the WinFSP service stack or reboot Windows if needed.
- On Windows, WinFSP `FlushAndPurgeOnCleanup` is disabled by default. Write-capable file handles disable the WinFSP/Windows file cache at open time; read-only handles keep the WinFSP cache-manager / fast-I/O path by default through `STUDIOFS_WINFSP_FILE_INFO_TIMEOUT_MS=max`; set a bounded value such as `1000` only for targeted multi-client invalidation experiments. Set `STUDIOFS_WINFSP_FLUSH_AND_PURGE_ON_CLEANUP=1` only for targeted regression experiments.
- `STUDIOFS_DAEMON_MOUNT_POINT` is not used for Windows mounts.
- `--detach` runs the mount as a background daemon.
- `--foreground` and `--detach` are mutually exclusive; providing both returns a CLI argument conflict error.
- `--cache-size` uses the shared size parser and accepts fractional values (for example `1.5GB`); the value must be greater than `0`.
- `--read-buffer` uses the same size parser and is rounded up to MiB units for VFS config.
- Local data-cache backend provisioning is automatic for the per-volume root under `<cache-dir>/<volume-uuid>` and is owned by `studiofs-cache`; the CLI only resolves cache location and budget inputs.
- Internal runtime thresholds (dirty headroom, upload local-I/O slots, transit headroom) are derived automatically from cache budget.
- On Windows and macOS, StudioFS uses the standard Local-First runtime path for every mount. Foreground `flush()` / `close()` confirm local acceptance and surface sticky upload errors deterministically, while upload and file-version publication continue asynchronously.
- `STUDIOFS_FUSE_ASYNC_WRITE` controls the FUSE async write-reply path on Unix FUSE mounts. macOS enables it by default as the validated asyncFUSE profile; Linux keeps synchronous writes by default unless set to `1`. Set `STUDIOFS_FUSE_ASYNC_WRITE=0` (or `false` / `off`) to restore synchronous FUSE write callbacks for rollback and A/B checks. Async mode copies each FUSE write buffer into owned memory, admits writes on Tokio tasks, serializes writes per file handle before entering the VFS, and keeps `flush`, `fsync`, and `release` behind pending-write barriers.
- Read access is tracked per handle in core VFS, so sequential media readers keep prefetch/L1 promotion behavior even when another handle on the same inode is doing random seeks.
- `--principal` overrides `STUDIOFS_PRINCIPAL` for this mount process.
- `--profile` enables optional Pyroscope profiling for the mount runtime.
- `--profile=off` disables profiling explicitly, even if `STUDIOFS_PROFILE` is set.
- `STUDIOFS_PROFILE=pyroscope|off` controls the same toggle from the environment.
- `STUDIOFS_PYROSCOPE_URL` or `PYROSCOPE_URL` configures the Pyroscope endpoint for the mount process.
- `STUDIOFS_PYROSCOPE_APP_NAME` or `PYROSCOPE_APPLICATION_NAME` overrides the service name.
- `STUDIOFS_PYROSCOPE_SAMPLE_RATE` or `PYROSCOPE_SAMPLE_RATE` sets the CPU sampling rate in Hz.
- `STUDIOFS_PYROSCOPE_TAGS` or `PYROSCOPE_TAGS` adds comma-separated `key=value` labels.
- Default mount tags include platform, volume name, mount mode, bridge, and `runtime=mount`.
- When no endpoint is configured, StudioFS still writes a gzipped pprof artifact under `<cache-dir>/<uuid>/profiles/`.
- `--snapshot-id` mounts the selected snapshot as a read-only historical view.
- `--endpoint <URL>` overrides the S3 endpoint URL for the current mount session.
  - Use `https://` for TLS or `http://` to explicitly select plain HTTP transport.
  - Plain HTTP is accepted for explicit `http://` endpoint URLs, including non-loopback private-network endpoints such as `http://192.168.1.20:10000`.
  - Bare loopback host-port overrides such as `127.0.0.1:9000` are normalized to `http://127.0.0.1:9000`; non-loopback bare host-port values are rejected unless the operator writes the `http://` scheme explicitly.
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
studiofs settings [--mount-point <path>] [--cache-dir <path>] [--cache-size <e.g. 200GB>] [--read-buffer <e.g. 4GB>] [--readahead <blocks>] [--prefetch <count>]
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
  - orphan regular/symlink inode cleanup (payload objects are moved to pending GC)
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
  `local_first_read_comparison` with `baseline-read`,
  `during-upload-read`, and `post-upload-read` rows plus the pending-upload
  observations used to gate each phase.
- When the mounted runtime exposes process and write-path observability, `e2e`
  JSON output also includes `mounted_memory` with `mount_pid`,
  `rss_bytes_steady_state`, `rss_bytes_peak`, `dirty_bytes_steady_state`,
  `dirty_bytes_peak`, `staged_bytes_peak`, and `dirty_limit_bytes`.
  RSS and dirty-pressure fields are best-effort samples and may be omitted when
  StudioFS cannot observe that signal for the measured run.
- The terminal summary prints the same `Local-First read comparison` table
  when that mounted upload-state metadata is available.
- The terminal summary prints a matching `Mounted memory` table when mounted
  RSS and dirty-pressure samples are available.
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
studiofs load --path-to-dump <dump.json> --postgresql-address <postgres-server-dsn>
```

Alias:
- Legacy alias: `--postgresql-adress`.

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

Production snippets should use HTTPS S3 endpoints and TLS-capable PostgreSQL
DSNs. The following workflow is a loopback-only development/test example.

Client A:
```bash
studiofs init projectfs --endpoint http://127.0.0.1:9000 --bucket studiofs-test --access-key studiofs --secret-key studiofs123 --meta "postgres://studiofs:studiofs@127.0.0.1:5432?sslmode=disable"
studiofs mount projectfs /mnt/studio/projectfs
```

Client B:
```bash
studiofs discover projectfs --meta "postgres://studiofs:studiofs@127.0.0.1:5432?sslmode=disable"
studiofs mount projectfs /mnt/studio/projectfs-b
```

## Environment Variables

### Runtime

Size-formatted values (for `*_BYTES` variables and size-like CLI flags) accept
fractional inputs such as `1.5GB`; parsing rounds down fractional bytes.
Supported suffixes are `B`, `KB`, `MB`, `GB`, `TB`, `PB`, `EB` using binary
multipliers (`1KB = 1024B`).

Operator stance:

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
| `--read-buffer` | `300MB` | Foreground read-buffer budget |
| `--readahead` | `64` | Sequential readahead width in 4 MiB blocks |
| `--prefetch` | `16` | Background prefetch batch concurrency budget |
| `--principal` / `STUDIOFS_PRINCIPAL` | `*` | Principal used for ACL checks |
| `STUDIOFS_CLIENT_ROOT` | `~/.studiofs` | Local per-client metadata, mount records, logs, investigations, and default cache root |

For cold sequential reads, leave `--read-buffer` at `300MB` or higher unless
memory is constrained. The runtime derives the foreground fetch AIMD ceiling
from `--read-buffer` and block size, so the default 4 MiB block layout starts
with multiple foreground S3 GETs in flight. `--readahead` controls how far the
sequential detector looks ahead, while `--prefetch` controls how many
background prefetch batches may run; foreground reads still have priority
through fetch reserve slots and the transit-memory budget. The effective
prefetch reserve is capped to at most half of the current fetch AIMD window so
the default foreground reserve cannot collapse cold sequential prefetch to only
one or two active S3 GETs during ramp-up.

The writeback dirty-buffer default is `300MB` (`max_dirty_bytes`), matching the
JuiceFS default read/write buffer size. StudioFS derives the soft throttle window
and hard safety threshold from that single dirty budget. Upload backlog bytes are
reported separately and do not by themselves stall ordinary writes while local
cache headroom remains available.

`STUDIOFS_WRITER_PAGE_SIZE_BYTES` is an expert-only mount/runtime override for
the mounted writer stack's internal page geometry. Leave it unset to preserve
the existing logical-block-sized default on all platforms. Use values such as
`64KB` only for focused FUSE write-geometry validation and benchmark runs before
changing platform defaults. The override is normalized to at least `4KB`, rounded
up to a power of two, and capped at the volume block size. For the canonical
4 MiB block size this produces clean divisors; non-standard block sizes fall
back to block-sized writer pages when the normalized override does not divide
the block size cleanly.

### Expert overrides

The code accepts additional `STUDIOFS_*` runtime overrides through the
raw/validated VFS config pipeline. They are not the normal operator API.

Use them only when one of these is true:

- you are migrating existing scripts during the `SFS-249` cleanup window
- you are debugging or benchmarking a specific runtime heuristic
- you are working on bridge-specific tuning that belongs to `SFS-250`
- you are changing maintenance/recovery behavior that belongs to `SFS-251`

For the public IO knobs, the env mapping is:

| Public flag | Environment override |
|---|---|
| `--read-buffer` | `STUDIOFS_READ_BUFFER_MB` |
| `--readahead` | `STUDIOFS_READAHEAD_WIDTH` |
| `--prefetch` | `STUDIOFS_PREFETCH_CONCURRENCY` |

Important families supported in code:

- metadata-cache internals: `STUDIOFS_DENTRY_CACHE_*`,
  `STUDIOFS_PATH_SHORTCUT_CACHE_*`, `STUDIOFS_ATTR_READ_CACHE_*`,
  `STUDIOFS_READDIR_ATTR_BATCH_*`,
  `STUDIOFS_EFFECTIVE_ACL_CACHE_CAPACITY`,
  `STUDIOFS_METADATA_INVALIDATION_SYNC_INTERVAL_MS`
- read/upload/fetch internals: `STUDIOFS_FETCH_*`, `STUDIOFS_UPLOAD_*`,
  `STUDIOFS_QOS_*`, `STUDIOFS_PREFETCH_*`,
  `STUDIOFS_WRITER_PAGE_SIZE_BYTES`
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
- Numeric defaults live in `crates/studiofs-vfs/src/vfs.rs` as
  `DEFAULT_*` constants.
- Typed config fields and validation live in
  `crates/studiofs-vfs/src/vfs/config.rs` (`VfsConfig` and `VfsConfigRaw`).
- If these values need to move, change them as a code change with supporting
  reasoning rather than treating them as routine mount-time tuning.

Metadata cache observability is split between runtime log events and the mounted
write-path observability snapshot used by `studiofs status --verbose` and cache
test reports.

PostgreSQL metadata invalidation uses push-plus-poll behavior:

- `LISTEN/NOTIFY` on `studiofs_meta_vol_<volume_id>` wakes peers promptly after
  remote-visible event-log batches commit.
- `STUDIOFS_PG_SYNC_POLL_SECS` remains the fallback discovery interval when a
  notification is missed or the listener reconnects.
- `STUDIOFS_PG_CONNECT_TIMEOUT_MS` and `STUDIOFS_PG_COMMAND_TIMEOUT_MS` bound
  metadata listener reconnect and sync attempts.
- Lock lease notifications stay on `studiofs_lock_vol_<volume_id>` and are not
  part of general namespace/attr/payload invalidation.

On PostgreSQL notify wakeups, the metadata listener syncs from the event log
before waking the VFS invalidation worker. If local namespace or metadata
version tokens already advanced, the worker skips the fallback poll's remote
event-log query and only performs cache invalidation work. Timer-driven fallback
polls still always query PostgreSQL to cover missed notifications.

The existing `upload pipeline` / `upload pipeline counters` log events include:

- dentry cache, full-path shortcut cache, and attr read cache metrics: hits/misses/evictions, occupancy, protected-tier (hotset) occupancy, and hot-hit contribution
- writer/cache recovery counters and checkpoint stats
- cached-store staging batch metrics (batch size in payloads/bytes, batch duration histogram, concurrent batch depth/peak)
- cache shim read/write throughput and latency histograms
- VFS synchronous read fast-path counters (`vfs_read_sync_fast_path_hits`, `vfs_read_sync_fast_path_misses`)
- queue depth/backpressure metrics (`upload_queue_depth`, `upload_queue_depth_peak`, `write_backpressure_wait_ms`, `write_backpressure_events`)
- writer/cache backpressure metrics (`fast_backpressure_wait_ms`, `fast_backpressure_events`, `fast_backpressure_stall_max_ms`)
- upload local payload-read metrics (`upload_local_payload_reads`, `upload_local_payload_read_bytes`, `upload_fallback_deferrals`, `upload_fallback_rereads`, `upload_fallback_reread_bytes`, `upload_fallback_reread_cache_misses`, `upload_fallback_wait_ms`, `upload_fallback_wait_max_ms`, `upload_fallback_read_ms`, `upload_fallback_read_max_ms`)
- transit-memory budget metrics (`transit_memory_budget_bytes`, `transit_memory_used_bytes`, `transit_memory_available_bytes`, `transit_memory_waiting_foreground`, `transit_memory_waiting_background`)
- foreground access-policy metrics (`dominant_access_class`, `dominant_prefetch_enabled`, `dominant_cache_promotion_on_read`, `dominant_readahead_base_width`, `dominant_readahead_max_width`, `dominant_readahead_min_prefetch`, `dominant_upload_pressure_boost`, `active_sequential_media_reads`, `active_random_interactive_reads`, `active_sequential_writers`, `active_random_writers`)

The mounted write-path observability snapshot additionally reports soft/hard
write-path events (`soft_throttle_events`, `hard_events`) and dirty-pressure
fields (`total_dirty_bytes`, `dirty_peak_bytes`, `dirty_limit_bytes`,
`pending_persisted_slice_bytes`, `staged_peak_bytes`, `pending_upload_bytes`,
`cache_pressure_bytes`).

Dentry/path shortcut logs also include negative-hit ratio. The summary `upload pipeline` line is emitted while uploads or metadata replication are active and then once more with `pending_uploads=0` and `pending_metadata_ops=0` after the pipeline fully drains. Its `pending_uploads` field reports the queued upload volume in a human-readable size (`1904MB`, `1.9GB`) instead of a block count.
Local backend read/write throughput metrics are lifetime averages since the current backend instance was opened (not short-window/EMA samples).

Upload local-I/O tuning notes:
- upload workers source payload bytes from persisted local payload sources after dequeue: hot upload buffer, upload helpers, or local backend fallback.
- upload local-cache I/O slots are held only around the local payload-read phase and then released before remote upload work continues.
- upload local rereads use `get_for_upload()` on the local backend, which issues a best-effort post-read `DONT_NEED` advisory over the full on-disk block span (header + payload, aligned to block start) on Linux and does not refresh LRU recency.
- adaptive upload penalty is enforced before local-cache read permits are acquired, so throttling creates disk idle gaps before background L2 reads.
- per-upload foreground cache cost is derived internally from cache budget; there is no separate user-tuned penalty knob.
- if an upload worker already delayed an op for coalescing, adaptive pre-read sleep is reduced by the already waited delay for that op.
- fetch transit-budget accounting includes assembled object payload plus bounded in-flight fragment headroom. Single-fragment 4 MiB object reads avoid reserving extra assembled-copy headroom.
- if a single fetch request exceeds the derived transit-memory budget, StudioFS returns a fetch error instead of silently clamping the credit request.
- fetch observability reports S3 GET, object decode/checksum, assemble/copy, local cache put, and total fetch timings separately.
- speedometer logs include `foreground_read_p90_present`, `foreground_read_p90_ms`, `foreground_read_window_bytes`, `foreground_read_window_coverage_ms`, and `foreground_read_throughput_bytes_per_sec` for stable parsing even when upload interference is intermittent.
- upload throttling uses the same foreground-read speedometer plus the dominant access policy's `upload_pressure_boost`, so sequential media reads raise upload penalty faster than random interactive probes without adding a separate scheduler loop.
- background/emergency pressure-maintenance eviction can switch to bounded batch mode for large reclaim targets to reduce per-block lock/scheduler overhead while preserving lock release between batches.

Configuration validation notes:
- StudioFS resolves user input through a raw/validated configuration pipeline before creating `StudioVfs`.
- `STUDIOFS_PREFETCH_CONCURRENCY` is normalized to a minimum of `1` when an explicit lower value is supplied in raw config.
- `STUDIOFS_UPLOAD_WORKER_COUNT=0` is treated as "use runtime default", which resolves to `20` workers.

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
