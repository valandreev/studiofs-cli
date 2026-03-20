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

Mount as drive base `Z:` (resulting mount point `Z:\myvolume`):

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
  --block-size 8MB \
  --read-size 1MB \
  --small-max 10KB \
  --medium-max 8MB \
  --sizes small=60,medium=30,large=10 \
  --concurrency 2 \
  --sequential-streams \
  --io-depth 64 \
  --warmup-runs 2 \
  --access-pattern mixed \
  --l1-limit 2GB \
  --l2-limit 100GB \
  --profile
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
- `--sizes` accepts percentages (`small=60,medium=30,large=10`) or explicit
  lists (`4KB,64KB,1MB,8MB`).
- `--warmup-runs` runs prefill cycles that are not included in measured
  metrics.
- `--sequential-streams` runs each stream in-order and emits per-stream results
  in both terminal and JSON output.
- in `--sequential-streams` mode, effective in-flight I/O is
  `min(--concurrency, --io-depth)` because each stream executes serially.
- JSON report contains a `run_id` field to correlate benchmark output with
  tracing/profile events.
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
- `e2e` mode keeps filesystem-level read/write reports; cache internals are
  emitted through mount logs (`upload pipeline counters`).
