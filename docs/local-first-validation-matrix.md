# Local-First Validation Matrix

This document is the focused validation and benchmark matrix for `SFS-253`
under the `SFS-242` rewrite umbrella.

Use it together with the canonical rewrite contract in
[docs/juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
The goal is to prove that StudioFS meets its own Local-First and eventual
consistency contract while borrowing performance-relevant ideas from JuiceFS.
It is not a literal 1:1 JuiceFS parity checklist.

## How To Use This Matrix

1. Run the focused automated checks for the scenario you changed.
2. Run the benchmark recipe that matches the performance-sensitive path you
   touched.
3. After the targeted matrix passes, ask an operator to run the larger manual
   validation commands in the last section.

Ignored integration tests in this matrix should be run with
`--test-threads=1` when they require S3, PostgreSQL, or real mount drivers.

## Focused Automated Scenario Matrix

Some rows mix `studiofs-vfs` integration tests from `--test vfs_test` with
library unit tests from `src/`. That is intentional: only the integration-test
commands use `--test vfs_test`; library unit-test commands run as plain
`cargo test -p studiofs-vfs <filter>`.

| Scenario | Contract focus | Primary checks | Focused commands |
|---|---|---|---|
| Append | write success immediately establishes same-mount visibility and durable local unpublished state | `append_after_create_is_locally_durable_without_flush` | `cargo test -p studiofs-vfs --test vfs_test append_after_create_is_locally_durable_without_flush -- --nocapture` |
| Overwrite | newer local unpublished data must outrank published state on the writing mount | `overwrite_of_published_file_is_locally_readable_without_flush` | `cargo test -p studiofs-vfs --test vfs_test overwrite_of_published_file_is_locally_readable_without_flush -- --nocapture` |
| Sparse write | uncovered gaps read as zeroes; sparse ranges do not corrupt committed payload | `test_sparse_write_at_high_offset`; `test_read_spanning_allocated_and_sparse_blocks` | `cargo test -p studiofs-vfs --test vfs_test test_sparse_write_at_high_offset -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test test_read_spanning_allocated_and_sparse_blocks -- --ignored --nocapture --test-threads=1` |
| Truncate | local unpublished EOF changes must be visible immediately without advancing published size early | `truncate_of_published_file_keeps_committed_size_and_pending_incarnation`; `test_truncate_shrink_then_extend_zero_fills_old_tail` | `cargo test -p studiofs-vfs --test vfs_test truncate_of_published_file_keeps_committed_size_and_pending_incarnation -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test test_truncate_shrink_then_extend_zero_fills_old_tail -- --ignored --nocapture --test-threads=1` |
| Flush | `flush` is a local-durability barrier, not a remote publication barrier | `flush_with_offline_s3_keeps_data_locally_committed_and_queued` | `cargo test -p studiofs-vfs --test vfs_test flush_with_offline_s3_keeps_data_locally_committed_and_queued -- --nocapture` |
| Fsync | `fsync` persists current local state even when remote upload is still pending | `test_fsync` | `cargo test -p studiofs-vfs --test vfs_test test_fsync -- --ignored --nocapture --test-threads=1` |
| Close / release | last-handle release owns the fallback local-durability barrier; clean close stays observational | `close_after_explicit_flush_becomes_clean_release`; `close_owns_deferred_fallback_barrier_on_last_handle_release` | `cargo test -p studiofs-vfs close_after_explicit_flush_becomes_clean_release -- --nocapture`<br>`cargo test -p studiofs-vfs close_owns_deferred_fallback_barrier_on_last_handle_release -- --nocapture` |
| Local read-your-writes | foreground reads must keep returning accepted bytes even when S3 is offline or upload is in flight | `offline_s3_does_not_break_local_read_after_write` | `cargo test -p studiofs-vfs --test vfs_test offline_s3_does_not_break_local_read_after_write -- --nocapture` |
| Eventual remote visibility | other peers must switch visibility atomically at publish time rather than block-by-block | `test_pg_publish_switch_is_atomic_across_peers` | `cargo test -p studiofs-meta test_pg_publish_switch_is_atomic_across_peers -- --ignored --nocapture --test-threads=1` |
| Cache warm / cold reads | cold local reads stay correct; repeated reads warm the fast path and remain bounded | `warm_cache_sequential_read_bounded_latency_after_write`; `committed_random_unaligned_reads_match_payload_after_publish_and_cold_reopen` | `cargo test -p studiofs-vfs --test vfs_test warm_cache_sequential_read_bounded_latency_after_write -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test committed_random_unaligned_reads_match_payload_after_publish_and_cold_reopen -- --ignored --nocapture --test-threads=1` |
| Readahead / prefetch | sequential and random readers must remain classified separately; prefetch ramps up only after the intended threshold | `handle_aware_read_policy_keeps_sequential_and_random_streams_separate`; `test_prefetch_after_threshold`; `test_random_seek_scales_back_to_base_width` | `cargo test -p studiofs-vfs --test vfs_test handle_aware_read_policy_keeps_sequential_and_random_streams_separate -- --nocapture`<br>`cargo test -p studiofs-vfs test_prefetch_after_threshold -- --nocapture`<br>`cargo test -p studiofs-vfs test_random_seek_scales_back_to_base_width -- --nocapture` |
| Writeback-heavy foreground QoS | sequential reads must stay useful while background upload is active and recover after upload drains | `foreground_read_baseline_during_and_post_upload_are_comparable`; `sequential_foreground_read_throughput_during_upload_meets_minimum` | `cargo test -p studiofs-vfs --test vfs_test foreground_read_baseline_during_and_post_upload_are_comparable -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test sequential_foreground_read_throughput_during_upload_meets_minimum -- --ignored --nocapture --test-threads=1` |

### Local Cache Exhaustion Note

The append and overwrite guarantees above apply only after a write is accepted
into the local-first path. If the local cache cannot persist more bytes,
StudioFS must fail locally before remote publication is even relevant.

Pair append/overwrite changes with this local-backend pressure guard:

```bash
cargo test -p studiofs-cache cache_v3_backend_returns_capacity_exhausted_for_non_evictable_pressure -- --nocapture
```

This is a focused local-backend capacity regression, not a full mounted
`ENOSPC` scenario. Use it as the current disk-full-style acceptance check until
the matrix grows a dedicated end-to-end local-volume exhaustion workflow.

## Platform Bridge Matrix

Run these when a change touches bridge callback sequencing, flush ownership, or
real mounted I/O behavior.

| Platform | Focus | Primary checks | Command |
|---|---|---|---|
| Unix FUSE | mounted create/write/read/readdir/delete/unmount lifecycle | `fuse_mount_write_read_unmount_smoke` | `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity fuse_mount_write_read_unmount_smoke -- --ignored --nocapture --test-threads=1` |
| Windows WinFSP | mounted create/write/read/readdir/delete/unmount lifecycle | `winfsp_mount_write_read_unmount_smoke` | `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_write_read_unmount_smoke -- --ignored --nocapture --test-threads=1` |
| Windows WinFSP sequential writer | AJA-style large sequential writer must keep cleanup observational and preserve close-side barrier ownership | `winfsp_mount_aja_style_sequential_writer_validation_smoke` | `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_aja_style_sequential_writer_validation_smoke -- --ignored --nocapture --test-threads=1` |

## Focused Benchmark Recipes

These are the benchmark surfaces that matter most for `SFS-253`. Prefer these
recipes over ad-hoc one-off runs so results stay comparable across machines.

### Sequential Write + Read Throughput

Use the existing VFS integration benchmark when you need a focused
write-then-read throughput sanity check against the current data path:

```bash
cargo test -p studiofs-vfs --test vfs_test test_sequential_write_performance -- --ignored --nocapture --test-threads=1
```

### Random Read Throughput On Published Data

Use the ignored published-state reopen test when you need a cold-reopen random
read benchmark against committed remote-visible state:

```bash
cargo test -p studiofs-vfs --test vfs_test committed_random_unaligned_reads_match_payload_after_publish_and_cold_reopen -- --ignored --nocapture --test-threads=1
```

### Cache-Cold And Cache-Warm Direct-Shim Runs

Use `studiofs test-cache` in direct-shim mode for local cache benchmarking
without a mounted filesystem:

```bash
cargo run -p studiofs -- test-cache ./target/test-cache/direct-cold \
  --access-pattern sequential \
  --block-size 16MB \
  --read-size 1MB \
  --sizes 4MB,16MB,64MB \
  --concurrency 2 \
  --io-depth 32 \
  --warmup-runs 0
```

Re-run against the same cache directory with warm-ups enabled to capture the
warm-cache path:

```bash
cargo run -p studiofs -- test-cache ./target/test-cache/direct-cold \
  --access-pattern sequential \
  --block-size 16MB \
  --read-size 1MB \
  --sizes 4MB,16MB,64MB \
  --concurrency 2 \
  --io-depth 32 \
  --warmup-runs 2
```

Use the JSON report's `advanced_metrics.cache_levels` section for L1 hit/miss
evidence.

### Mounted End-To-End Sequential / Random Runs

Run these against a mounted StudioFS path after the mount is already active:

```bash
cargo run -p studiofs -- test-cache <mounted-path> \
  --access-pattern sequential \
  --block-size 16MB \
  --read-size 1MB \
  --sizes 16MB,64MB,256MB \
  --concurrency 2 \
  --io-depth 16 \
  --warmup-runs 1
```

```bash
cargo run -p studiofs -- test-cache <mounted-path> \
  --access-pattern random \
  --block-size 16MB \
  --read-size 256KB \
  --sizes 16MB,64MB,256MB \
  --concurrency 4 \
  --io-depth 32 \
  --warmup-runs 1
```

When the mounted runtime exposes upload-state metadata, inspect the human
summary or JSON `local_first_read_comparison` section for the
`baseline-read`, `during-upload-read`, and `post-upload-read` phases. When
mounted process observability is available, inspect the `Mounted memory`
terminal table or JSON `mounted_memory` section for `rss_bytes_peak`,
`rss_bytes_steady_state`, `dirty_bytes_peak`, and `dirty_limit_bytes`.

### Small-File Random I/O / Metadata-Sensitive Run

Use this mounted recipe when you want a latency-sensitive small-file benchmark
instead of the larger throughput-oriented defaults:

```bash
cargo run -p studiofs -- test-cache <mounted-path> \
  --access-pattern random \
  --block-size 64KB \
  --read-size 4KB \
  --sizes 4KB,16KB,64KB \
  --concurrency 16 \
  --io-depth 64 \
  --warmup-runs 1
```

Prefer the mounted path for this recipe because it exercises namespace lookups,
VFS policy, cache behavior, and small-read amplification together. Use the same
shape against a direct-shim path only when isolating local cache behavior from
the mounted filesystem stack.

### Writeback-Heavy End-To-End Run

Use a mounted path and mixed access to keep upload/writeback pressure active
while still measuring read-side service quality:

```bash
cargo run -p studiofs -- test-cache <mounted-path> \
  --access-pattern mixed \
  --block-size 16MB \
  --read-size 1MB \
  --sizes 16MB,64MB,256MB \
  --concurrency 4 \
  --io-depth 32 \
  --warmup-runs 1 \
  --sequential-streams
```

For a stricter regression gate on the same concern, pair the command above with:

```bash
cargo test -p studiofs-vfs --test vfs_test sequential_foreground_read_throughput_during_upload_meets_minimum -- --ignored --nocapture --test-threads=1
```

## StudioFS Vs JuiceFS: What To Compare And What Not To Compare

Use JuiceFS as a performance and architecture reference, not as a literal
semantic oracle.

Compare directly:

- sequential write throughput
- sequential read throughput
- random read throughput
- cache-warm versus cache-cold read behavior
- foreground read degradation while background upload/writeback is active
- bridge-specific mounted sequential writer throughput on Windows when the
  workload shape is comparable

Do not treat these as parity failures:

- StudioFS same-mount accepted writes are intentionally visible immediately
  through writer state and local unpublished state.
- StudioFS `flush`, `fsync`, and last-handle `close` confirm local durability,
  not remote publication.
- StudioFS remote visibility flips at a file-version publish boundary after
  upload plus publish; it is intentionally eventual rather than close-to-open.
- StudioFS benchmark output may surface Local-First specific metadata such as
  `local_first_read_comparison`, `mounted_memory`, or cache-level counters
  that do not have a 1:1 JuiceFS label.

When a JuiceFS reference comparison is useful, reuse the same workload shape
(block size, read size, concurrency, access pattern, warm/cold setup) and
compare throughput/latency trends, not identical callback timing or visibility
semantics.

### SFS-262 Mounted Default Gate

For `SFS-262`, lock the default mounted benchmark recipe to a same-host
StudioFS versus JuiceFS comparison:

- Use default-mounted StudioFS and default-mounted JuiceFS unless the ticket is
  explicitly about a non-default tuning surface. If you override any knob,
  record every override in the benchmark notes.
- Run the mounted sequential recipe above on both filesystems with the same
  block size, read size, size list, concurrency, queue depth, warm-up count,
  and backing hardware. Use the sequential run to compare write throughput and
  sequential read throughput.
- Run the mounted random recipe above on both filesystems with the same
  workload shape. Use that run to compare mounted read throughput on the random
  path.
- Treat the throughput gate as `>= 90%` of the comparable JuiceFS result for
  each required phase.
- Treat the mounted memory gate as
  `mounted_memory.rss_bytes_peak <= min(2 GiB, juicefs_peak_rss * 1.15)`.
- Use `mounted_memory.rss_bytes_steady_state`,
  `mounted_memory.dirty_bytes_peak`, and `mounted_memory.dirty_limit_bytes` as
  diagnostics when the peak-RSS gate fails; they help distinguish dirty-state
  pressure from other runtime memory growth.
- Treat `local_first_read_comparison` and `mounted_memory` as StudioFS-only
  diagnostics. They strengthen comparability, but they are not required to map
  1:1 onto JuiceFS counters.

## Larger Manual Validation After The Targeted Matrix Passes

These are operator-run commands for broader confidence. They are intentionally
not part of the default focused loop.

### Workspace Suites

```bash
cargo test --workspace --all-targets -- --nocapture
```

```bash
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

### Mounted Bridge Smokes

Linux / macOS:

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1
```

Windows PowerShell:

```powershell
$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'
$env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'
cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1
```

### Mounted Benchmark Sweep

Run the sequential, random, and writeback-heavy `studiofs test-cache`
commands above against the same mounted volume and archive the JSON reports.

### Optional JuiceFS Reference Sweep

If the change is explicitly performance-sensitive, rerun the same sequential
and random workload shapes against a comparable JuiceFS reference mount and
record:

- block size
- read size
- concurrency
- warm/cold setup
- writeback delay or equivalent batching knobs
- observed throughput / latency
- peak RSS and steady RSS for the mounted process when available
- any semantic differences that make the comparison non-equivalent
