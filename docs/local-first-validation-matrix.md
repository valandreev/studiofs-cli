# Canonical Local-First Validation Matrix

This document is the validation entrypoint for the canonical mounted-IO
contract.

Use it together with the canonical contract in
[docs/juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
The goal is one obvious place to verify:

- JuiceFS-equivalent local writer/cache gates that do not require live
  PostgreSQL or S3
- local-offline replay and durability gates
- infra-backed distributed visibility and lock-lease gates
- platform bridge smoke checks when callback sequencing changes

This is the focused StudioFS contract matrix that should be consulted before
data-plane, semantics, or performance work.

## How To Use This Matrix

1. Start with the fast/local rows that match the contract rule you changed.
2. Add the local-offline rows when the change touches queued uploads, replay,
   or S3-unavailable behavior.
3. Add the infra-backed rows when the change touches PostgreSQL, peer
   visibility, shared lock leases, or remote apply.
4. Run the platform bridge matrix when callback sequencing or mounted I/O
   behavior changed.
5. After the targeted matrix passes, ask an operator to run the broader
   workspace commands from [docs/tests.md](./tests.md) when the change is wider
   than the focused contract surface.

Ignored tests in this matrix should be run with `--test-threads=1` when they
require live PostgreSQL, S3, or real mount drivers.

## Local Writer / Cache Contract Matrix

Some rows intentionally mix `studiofs-vfs` integration tests from
`--test vfs_test` with fast unit tests from `src/`. Integration-test commands
use `--test vfs_test`; library unit-test commands run as plain
`cargo test -p <crate> <filter>`.

| Scenario | Contract focus | Primary checks | Focused commands |
|---|---|---|---|
| Accepted write durability boundaries | Same-mount reads use active writer state before the local durability boundary; `flush`, `fsync`, last-handle release, and graceful shutdown move accepted bytes into locally durable slices/cache without making them remote-ready | `sfs312_write_is_visible_from_writer_state_before_local_durability_boundary`; `sfs312_flush_materializes_local_slice_boundary_without_remote_visibility`; `sfs312_fsync_persists_local_state_without_remote_visibility`; `sfs312_last_handle_release_drains_local_durability_boundary`; `local_first_shutdown_reports_and_recovers_memory_only_writer_state` | `cargo test -p studiofs-vfs sfs312_ -- --nocapture`<br>`cargo test -p studiofs-vfs local_first_shutdown_reports_and_recovers_memory_only_writer_state -- --nocapture` |
| Explicit `flush` and last-handle release barriers | `flush` and last-handle release freeze/finish required writer slices through cached-store staging without pulling PG/S3 into the foreground | `flush_fh_enforces_strict_local_durability_in_single_mode`; `release_fh_enforces_strict_local_durability_on_last_handle`; `test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush` | `cargo test -p studiofs-vfs flush_fh_enforces_strict_local_durability_in_single_mode -- --nocapture`<br>`cargo test -p studiofs-vfs release_fh_enforces_strict_local_durability_on_last_handle -- --nocapture`<br>`cargo test -p studiofs-vfs test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush -- --nocapture` |
| `fsync` as the strict caller-requested local crash-safe boundary | `fsync` must persist current local state without implying remote publication | `strict_local_sync_flushes_pending_group_commit_bytes`; `test_fsync_with_outcome_materializes_pending_inode_dirty_state`; `test_fsync_with_outcome_reports_writer_stack_file_data_barrier` | `cargo test -p studiofs-vfs strict_local_sync_flushes_pending_group_commit_bytes -- --nocapture`<br>`cargo test -p studiofs-vfs test_fsync_with_outcome_materializes_pending_inode_dirty_state -- --nocapture`<br>`cargo test -p studiofs-vfs test_fsync_with_outcome_reports_writer_stack_file_data_barrier -- --nocapture` |
| Known foreign hard locks | peers may open read-only, grandfathered writers may finish, and new conflicting size / namespace mutations must block | `foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open`; `existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known`; `foreign_hard_lock_blocks_size_change_setattr_without_handle`; `foreign_hard_lock_blocks_unlink_and_rename` | `cargo test -p studiofs-vfs foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open -- --nocapture`<br>`cargo test -p studiofs-vfs existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known -- --nocapture`<br>`cargo test -p studiofs-vfs foreign_hard_lock_blocks_size_change_setattr_without_handle -- --nocapture`<br>`cargo test -p studiofs-vfs foreign_hard_lock_blocks_unlink_and_rename -- --nocapture` |
| PG disconnect headroom exhaustion and read-only degradation | new mutations stop once local inode/object ID windows are exhausted, failed larger object batches do not consume smaller inode headroom, undersized prefetched windows do not block replacement reservations, and known readable state stays available | `read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open`; `next_inode_reports_read_only_headroom_once_windows_are_exhausted`; `open_with_persisted_slot_and_ranges_survives_pg_unavailable`; `sfs315_reopen_consumes_persisted_inode_and_object_headroom_without_pg`; `sfs315_failed_object_batch_preserves_smaller_inode_headroom`; `sfs315_large_object_batch_replaces_undersized_prefetched_window` | `cargo test -p studiofs-vfs read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open -- --nocapture`<br>`cargo test -p studiofs-meta next_inode_reports_read_only_headroom_once_windows_are_exhausted -- --nocapture`<br>`cargo test -p studiofs-meta open_with_persisted_slot_and_ranges_survives_pg_unavailable -- --nocapture`<br>`cargo test -p studiofs-meta sfs315_ -- --nocapture` |
| Local-only durability and replay while S3 is unavailable | accepted bytes stay readable locally, queued uploads replay after restart, and reconnect replay keeps only the live state | `flush_with_offline_s3_keeps_data_locally_committed_and_queued`; `offline_s3_does_not_break_local_read_after_write`; `recovery_after_offline_s3_preserves_local_accepted_view`; `repeated_graceful_restarts_during_upload_replay_are_idempotent`; `restart_replays_only_live_pending_extents` | `cargo test -p studiofs-vfs --test vfs_test flush_with_offline_s3_keeps_data_locally_committed_and_queued -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test offline_s3_does_not_break_local_read_after_write -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test recovery_after_offline_s3_preserves_local_accepted_view -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test repeated_graceful_restarts_during_upload_replay_are_idempotent -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test restart_replays_only_live_pending_extents -- --nocapture` |
| Startup replay diagnostics | restart recovery status identifies recovered staged bytes, rebuilt upload queue entries, stale pending-upload cancellation, memory-only bytes outside the durability boundary, and orphan cleanup | `startup_replay_summary_reports_recovered_bytes_and_queued_uploads`; `startup_replay_cancels_ready_inode_stale_pending_upload`; `startup_replay_removes_unreferenced_warm_payloads_and_stage_links`; `startup_replay_removes_orphan_dirty_payloads_and_clears_pending_evictions` | `cargo test -p studiofs-vfs startup_replay_summary_reports_recovered_bytes_and_queued_uploads --lib -- --nocapture`<br>`cargo test -p studiofs-vfs startup_replay_cancels_ready_inode_stale_pending_upload --lib -- --nocapture`<br>`cargo test -p studiofs-vfs startup_replay_removes_unreferenced_warm_payloads_and_stage_links --lib -- --nocapture`<br>`cargo test -p studiofs-vfs startup_replay_removes_orphan_dirty_payloads_and_clears_pending_evictions --lib -- --nocapture` |
| Dirty-byte pressure and reporting | writer pages, staged local slices, upload backlog bytes, hard backpressure, and cache pressure remain separately observable without making upload backlog alone stall writes | `write_path_dirty_pressure_reports_staged_bytes_and_hard_backpressure`; `upload_backlog_does_not_create_local_backpressure_after_write_commit`; `sample_mounted_memory_during_scopes_collection_to_measured_run` | `cargo test -p studiofs-vfs write_path_dirty_pressure_reports_staged_bytes_and_hard_backpressure -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test upload_backlog_does_not_create_local_backpressure_after_write_commit -- --nocapture`<br>`cargo test -p studiofs sample_mounted_memory_during_scopes_collection_to_measured_run -- --nocapture` |
| Publication boundary contract | Uploaded payload presence alone is not peer visibility; publish requires matching epoch and uploaded payload readiness; stale or failed publish attempts leave the visible base graph unchanged; metadata-only apply keeps attrs and graph aligned | `sfs314_publish_requires_all_overlay_payloads_uploaded`; `sfs314_uploaded_payload_does_not_replace_peer_visible_base_before_publish`; `sfs314_partial_publish_preserves_base_tail_and_committed_size`; `sfs314_metadata_only_apply_keeps_attrs_and_file_graph_consistent` | `cargo test -p studiofs-meta sfs314_ -- --nocapture` |
| Delete-before-publish orphan cleanup | unpublished deletes stay local, and orphaned payload is surfaced for later janitor cleanup instead of becoming peer-visible state | `zeroio_snapshot_reports_delete_before_publish_cleanup_visibility`; `zeroio_delete_before_publish_persists_tombstone_lifecycle` | `cargo test -p studiofs-vfs zeroio_snapshot_reports_delete_before_publish_cleanup_visibility -- --nocapture`<br>`cargo test -p studiofs-meta zeroio_delete_before_publish_persists_tombstone_lifecycle -- --nocapture` |

## Infra-Backed Distributed Contract Matrix

These checks require live PostgreSQL and/or S3 because they validate peer
visibility, shared lease lifecycle, or mounted end-to-end behavior.

| Scenario | Contract focus | Primary checks | Focused commands |
|---|---|---|---|
| PG-backed hard-lock lease lifecycle | PostgreSQL is the shared lock authority for grant, renewal, expiry, and NOTIFY-driven known-lock refresh | `test_pg_file_lock_lease_blocks_peer_until_release`; `test_pg_file_lock_lease_expires_for_peer`; `test_pg_file_lock_lease_renew_keeps_peer_blocked`; `test_pg_file_lock_known_state_refreshes_from_notify` | `cargo test -p studiofs-meta test_pg_file_lock_lease_blocks_peer_until_release -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_file_lock_lease_expires_for_peer -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_file_lock_lease_renew_keeps_peer_blocked -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_file_lock_known_state_refreshes_from_notify -- --ignored --nocapture --test-threads=1` |
| Peer-visible lock enforcement | a remote peer must block on the lease while the holder still owns it | `pg_backed_flock_blocks_conflicting_peer_until_unlock` | `cargo test -p studiofs-vfs --test vfs_test pg_backed_flock_blocks_conflicting_peer_until_unlock -- --ignored --nocapture --test-threads=1` |
| New-file first visibility and metadata-only publish apply | a new file stays hidden until its first complete version exists, and peers refresh attrs only after publish/apply | `remote_attr_cache_refreshes_after_nested_publish_sync`; `test_pg_nested_temp_rename_then_publish_visible_on_remote_peer` | `cargo test -p studiofs-vfs --test vfs_test remote_attr_cache_refreshes_after_nested_publish_sync -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_nested_temp_rename_then_publish_visible_on_remote_peer -- --ignored --nocapture --test-threads=1` |
| S3 payload availability does not imply visibility | upload progress alone must not switch peer visibility, and published-file updates keep the previous committed version until the next complete publish | `upload_completion_batching_does_not_change_visibility_contract`; `remote_clients_keep_previous_committed_version_during_update`; `test_pg_publish_switch_is_atomic_across_peers`; `atomic_switch_to_new_incarnation_after_full_commit` | `cargo test -p studiofs-vfs --test vfs_test upload_completion_batching_does_not_change_visibility_contract -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test remote_clients_keep_previous_committed_version_during_update -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_publish_switch_is_atomic_across_peers -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test atomic_switch_to_new_incarnation_after_full_commit -- --ignored --nocapture --test-threads=1` |
| Open-handle snapshot semantics across rename, overwrite, and delete | peers must converge to the right published inode/path view without exposing partial rename or delete states | `remote_snapshot_reads_nested_file_after_temp_rename_publish`; `remote_snapshot_switches_nested_overwrite_to_replacement_inode`; `test_pg_reopen_after_remote_rename_overwrite_recovers_without_discover`; `test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover` | `cargo test -p studiofs-vfs --test vfs_test remote_snapshot_reads_nested_file_after_temp_rename_publish -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test remote_snapshot_switches_nested_overwrite_to_replacement_inode -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_reopen_after_remote_rename_overwrite_recovers_without_discover -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover -- --ignored --nocapture --test-threads=1` |
| Known published reads while PG is unavailable | persisted local inode-window and range state must allow offline reopen without fabricating new shared truth, and already published payload objects remain readable | `test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror`; `open_with_persisted_slot_and_ranges_survives_pg_unavailable` | `cargo test -p studiofs-meta test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta open_with_persisted_slot_and_ranges_survives_pg_unavailable -- --nocapture` |

### Local Cache Exhaustion Note

The append and overwrite guarantees above apply only after a write is accepted
into the local-first path. If the local cache cannot persist more bytes,
StudioFS must fail locally before remote publication is even relevant.

Pair append/overwrite changes with this local-backend pressure guard:

```bash
cargo test -p studiofs-cache local_backend_returns_capacity_exhausted_for_non_evictable_pressure -- --nocapture
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
| Windows WinFSP sequential writer | AJA-style large sequential writer must use WinFSP cleanup flush/purge while keeping StudioFS non-delete cleanup callback handling observational | `winfsp_mount_aja_style_sequential_writer_validation_smoke` | `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_aja_style_sequential_writer_validation_smoke -- --ignored --nocapture --test-threads=1` |

## Focused Benchmark Recipes

These are supplemental benchmark surfaces for the canonical contract. They are
useful after the contract checks above stay green; they are not substitutes for
the correctness-focused rows above.

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
`during-upload-read` and `post-upload-read` phases. A `post-upload-read` phase
is valid only when upload idle was observed first; otherwise the JSON report
marks it invalid and records the timeout reason.

For SFS-299 parity reports, prefer `studiofs test <mounted-path> --report
<path>` or the equivalent `test-cache` command with `--report`. The JSON report
must include:

- `mounted_runtime`: mounted path, cache root, inferred metadata root when the
  default `<meta-dir>/cache/<uuid>` layout is visible, block/chunk size, read
  size, workload shape, OS, and bridge type.
- `writes.latency_histogram` and `reads.latency_histogram` alongside throughput
  and percentile fields.
- `mounted_observability.writer`: dirty bytes, dirty slice count, frozen slice
  count, staged bytes, and dirty limit.
- `mounted_observability.cached_store`: staging latency/throughput and local
  cache backend latency. Cache hit/miss counters are present when that signal is
  available for the measured path.
- `mounted_observability.upload`: backlog count/bytes, upload throughput,
  retry count, active uploads, and completed upload bytes.
- `mounted_observability.barriers`: flush/fsync/close/cleanup barrier counts
  and the shared flush/fsync/close latency histogram.
- `mounted_observability.post_upload_idle`: whether idle was observed or timed
  out, wait duration, pending upload counts, and the invalid-read reason.

When mounted process observability is available, inspect the `Mounted memory`
terminal table or JSON `mounted_memory`, `mounted_observability.process`, and
`mounted_portable_profile` sections for `rss_bytes_peak`,
`rss_bytes_steady_state`, `virtual_memory_bytes_peak`, CPU average/peak,
process disk IO deltas, allocator `current_bytes` / `peak_bytes`, dirty bytes,
staged bytes, and dirty limits. These fields are best-effort and may be omitted
when the measured run does not expose that signal.

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

## StudioFS Vs JuiceFS Parity Gate

Use the local JuiceFS mount as the reference for mounted data-plane IO
economics.

Compare directly:

- sequential write throughput
- sequential read throughput
- random read throughput
- cache-warm versus cache-cold read behavior
- foreground read degradation while background upload/writeback is active
- bridge-specific mounted sequential writer throughput on Windows when the
  workload shape is comparable
- foreground callback latency
- peak RSS and steady-state RSS
- CPU during sustained writeback

StudioFS-specific report fields such as `local_first_read_comparison`,
`mounted_memory`, upload backlog, and cache-level counters are required
diagnostics. They explain differences from the JuiceFS run; they do not relax
the parity gate by themselves.

Reuse the same workload shape on both filesystems: block size, read size,
concurrency, access pattern, warm/cold setup, cache disk, metadata disk, and
mount options.

### SFS-301 Final Mounted Parity Notes

For `SFS-301`, initialize a fresh StudioFS validation volume with the current
`studiofs init` CLI and record the reported block size from the command output.
The current public `init` surface does not accept a `--block-size` flag; new
volumes report the canonical 4 MiB block size.

Use `studiofs test` for the StudioFS-native mounted report. The operator test
locks the mounted parity workload to the canonical 4 MiB chunk/read shape,
streams the 8 GiB write without allocating the full file in the test process,
and emits the `mounted_write_diagnostics` gate plus `mounted_portable_profile`
for throughput, peak RSS, portable CPU/disk samples, allocator heap pressure,
known local write amplification, landing-zone use, dirty pressure, cache
pressure, pending upload bytes, and write-path backpressure:

```powershell
.\target\release\studiofs.exe test Z:\ `
  --report D:\reports\sfs-301-studiofs-test-8g-4m.json
```

For `SFS-301`, treat the StudioFS side as solved when
`mounted_write_diagnostics.write_throughput_mib_per_sec >= 1000`,
`mounted_write_diagnostics.rss_target_met == true`, and
`mounted_write_diagnostics.known_local_write_amplification <= 1.20`.
Keep a JuiceFS comparison run with the same one-file 8 GiB / 4 MiB IO shape,
same machine, same cache disk class, and WMI process CPU/RSS sampling on
Windows.

### SFS-262 Mounted Default Gate

For `SFS-262`, lock the default mounted benchmark recipe to a same-host
StudioFS versus JuiceFS comparison:

- Use mounted StudioFS and mounted JuiceFS unless the ticket is
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
  `mounted_memory.dirty_bytes_peak`, `mounted_memory.staged_bytes_peak`,
  `mounted_memory.dirty_limit_bytes`, and `mounted_portable_profile` phase /
  allocator metrics as diagnostics when the peak-RSS gate fails; they help
  distinguish hot dirty-state pressure, staged payload pressure, Rust heap
  growth, and other runtime memory growth when those samples are present.
- Treat `local_first_read_comparison` and `mounted_memory` as required
  StudioFS-side diagnostics for explaining any parity miss.

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
