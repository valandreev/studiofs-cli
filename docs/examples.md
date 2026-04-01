# Usage Examples

## 1. Create a New Virtual Volume (Init)

Initialize a volume using local RustFS and PostgreSQL for metadata:

```bash
studiofs init myvolume \
  --endpoint http://127.0.0.1:9000 \
  --bucket mybucket \
  --access-key studiofs \
  --secret-key studiofs123 \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```

## 2. Mount a Volume

### On Windows

Mount directly to a drive letter (for example `Z:`):

```bash
studiofs mount myvolume Z:
```

### On Linux / macOS

Mount to a base directory (`/mnt/studio` becomes `/mnt/studio/myvolume`):

```bash
studiofs mount myvolume /mnt/studio
```

## 3. Daemon Mode (Background)

Mount a volume in the background (terminal will not be blocked):

```bash
studiofs mount myvolume Z: -d
```

Mount with Pyroscope profiling enabled:

```bash
STUDIOFS_PROFILE=pyroscope \
STUDIOFS_PYROSCOPE_URL=http://localhost:4040 \
STUDIOFS_PYROSCOPE_TAGS=team=vfx,cache_mode=durable \
studiofs mount myvolume Z: --profile
```

Mount with explicit cache safety mode override:

```bash
STUDIOFS_CACHE_SAFETY_MODE=durable studiofs mount myvolume Z:
```

Mount with a local S3 endpoint (for low-latency access in hybrid networks):

```bash
studiofs mount myvolume Z: --endpoint http://192.168.1.50:9000
```

To unmount a volume running in the background:

```bash
studiofs umount Z:
```

## 4. Mount an Existing Volume on a New Client (Discover)

If a volume has already been initialized on another machine, you do not need to call `init`. You only need to retrieve the latest metadata:

```bash
studiofs discover myvolume \
  --meta postgres://studiofs:studiofs@127.0.0.1:5432
```
After that, you can mount the volume:
```bash
studiofs mount myvolume /mnt/shared/projects
```

On Windows, use a free drive letter (for example `Z:`) instead of a folder path.

## 5. Clean Up Old Objects (Janitor)

See which objects are no longer used (dry-run):

```bash
studiofs janitor myvolume --dry-run
```

Run actual cleanup (delete up to 500 old objects):

```bash
studiofs janitor myvolume --max-deletes 500
```

## 6. Create and List Snapshots

Create an instant local snapshot (root inode by default):

```bash
studiofs snapshot myvolume create
```

List snapshots:

```bash
studiofs snapshot myvolume list
```

## 7. Mount a Historical Snapshot as Read-Only

```bash
studiofs mount myvolume Z: --snapshot-id <snapshot-uuid>
```

## 8. Run Metadata FSCK

Generate a read-only integrity report:

```bash
studiofs fsck myvolume
```

Report includes:
- dangling dentries
- orphan inodes
- `nlink` mismatches
- pending/deferred undo state

If issues are found, StudioFS asks whether to repair them.

Apply repair immediately without interactive prompt:

```bash
studiofs fsck myvolume --repair
```

Repair mode currently:
- replays undo logs
- removes dangling dentries
- removes orphan regular/symlink inodes and schedules their blocks for GC
- leaves orphan directories / hardlink-like orphan cases and `nlink` mismatch
  entries as report-only

## 9. Benchmark Cache (`test-cache`)

Direct L2 benchmark (bypass VFS/mount):

```bash
studiofs test-cache \
  /tmp/studiofs-cache-bench \
  --block-size 16MB \
  --read-size 1MB \
  --small-max 10KB \
  --medium-max 16MB \
  --sizes small=60,medium=30,large=10 \
  --concurrency 2 \
  --sequential-streams \
  --io-depth 64 \
  --warmup-runs 2 \
  --access-pattern mixed \
  --l1-limit 2GB \
  --l2-limit 100GB \
  --profile=pyroscope
```

End-to-end benchmark through an already mounted filesystem:

```bash
studiofs test-cache \
  /mnt/studio/bench
```

Notes:
- If `<path>` is not inside an active StudioFS mount, benchmark runs in
  `direct-l2` mode and uses `<path>` as cache dir.
- If `<path>` is inside an active StudioFS mount, benchmark runs in `e2e` mode
  through that mounted process and writes temp workload files into
  `<path>/.studiofs-test-cache`.
- Terminal output is a readable benchmark summary. The full structured JSON
  report is saved in the directory where `studiofs test-cache` was started,
  using an auto-generated file name unless `--json-output <path>` is provided.
- `--sizes` accepts percentages (`small=60,medium=30,large=10`) or explicit lists (`4KB,64KB,1MB,16MB`).
- `--warmup-runs` runs prefill cycles that are not included in measured
  metrics.
- Throughput in benchmark reports is phase-scoped: write-side metrics use
  write-phase wall time, and read-side metrics use read-phase wall time (not
  total benchmark wall time).
- `--sequential-streams` runs each stream in-order and emits per-stream results
  in both terminal and JSON output.
- in `--sequential-streams` mode, effective in-flight I/O is
  `min(--concurrency, --io-depth)` because each stream executes serially.
- JSON report contains a `run_id` field to correlate benchmark output with
  tracing/profile events.
- When `--profile=pyroscope` is enabled, `studiofs test-cache` writes local
  pprof artifacts next to the JSON report. If `STUDIOFS_PYROSCOPE_URL` is set,
  the same CPU and memory profiles are also pushed to that endpoint.
- JSON report contains an `advanced_metrics` section with:
  - size-class boundaries (`small_max_bytes`, `medium_max_bytes`)
  - per-size-class throughput and latency percentiles (p50/p95/p99)
  - L2 commit queue and upload queue backpressure/depth counters
- `direct-l2` JSON output includes a `cache_pipeline` section with:
  - `l2_read` / `l2_write` throughput + latency histograms
  - `materialize_batches` batch size/duration/concurrency metrics
  - `l2_commit_scheduler` queue depth / worker activity / backpressure counters
  - `upload_queue` depth and queue counters
  - `blocking_tasks` counters for blocking-pool pressure and queue wait timing
- `direct-l2` `advanced_metrics.cache_levels` adds explicit L1/L2 hit/miss
  counters, bytes served from each level, and per-level lookup latency
  distributions.
- On mounts started by newer StudioFS builds, `e2e` JSON output also includes
  `buffered_read_comparison`, which records `baseline-read`,
  `during-upload-read`, and `post-upload-read` phases plus the pending-upload
  wait observations used to bracket them.
- The terminal summary shows the same mounted comparison as a
  `Buffered-first read comparison` table, including wait target/observation and
  pending upload counts before and after each read phase.
- `e2e` mode still keeps filesystem-level read/write reports; cache internals
  continue to be emitted through mount logs (`upload pipeline counters`).
