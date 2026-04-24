# Canonical Local-First Validation Matrix

This document is the validation entrypoint for `SFS-270` under the
`SFS-263` production follow-up sprint.

Use it together with the canonical rewrite contract in
[docs/juicefs-local-first-rust-contract.md](./juicefs-local-first-rust-contract.md).
The goal is to lock the 2026-04-19 Fast Local Authority contract with one
obvious place to verify:

- fast/local contract gates that do not require live PostgreSQL or S3
- local-offline replay and durability gates
- infra-backed distributed visibility and lock-lease gates
- platform bridge smoke checks when callback sequencing changes

This is not a literal 1:1 JuiceFS parity checklist. It is the focused StudioFS
contract matrix that should be consulted before future rewrites, semantics
changes, or performance work.

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

## Fast / Local Contract Matrix

Some rows intentionally mix `studiofs-vfs` integration tests from
`--test vfs_test` with fast unit tests from `src/`. Integration-test commands
use `--test vfs_test`; library unit-test commands run as plain
`cargo test -p <crate> <filter>`.

| Scenario | Contract focus | Primary checks | Focused commands |
|---|---|---|---|
| Explicit `flush` and last-handle release barriers | `flush` is a local-durability barrier, and the last-handle release path owns the fallback barrier without pulling PG/S3 into the foreground | `flush_fh_enforces_strict_local_durability_in_single_mode`; `release_fh_enforces_strict_local_durability_on_last_handle`; `test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush` | `cargo test -p studiofs-vfs flush_fh_enforces_strict_local_durability_in_single_mode -- --nocapture`<br>`cargo test -p studiofs-vfs release_fh_enforces_strict_local_durability_on_last_handle -- --nocapture`<br>`cargo test -p studiofs-vfs test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush -- --nocapture` |
| `fsync` as the strict caller-requested local crash-safe boundary | `fsync` must persist current local state without implying remote publication | `strict_local_sync_flushes_pending_group_commit_bytes`; `test_fsync_with_outcome_materializes_pending_inode_dirty_state`; `test_fsync_with_outcome_reports_handle_writer_file_data_barrier` | `cargo test -p studiofs-vfs strict_local_sync_flushes_pending_group_commit_bytes -- --nocapture`<br>`cargo test -p studiofs-vfs test_fsync_with_outcome_materializes_pending_inode_dirty_state -- --nocapture`<br>`cargo test -p studiofs-vfs test_fsync_with_outcome_reports_handle_writer_file_data_barrier -- --nocapture` |
| Known foreign hard locks | peers may open read-only, grandfathered writers may finish, and new conflicting size / namespace mutations must block | `foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open`; `existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known`; `foreign_hard_lock_blocks_size_change_setattr_without_handle`; `foreign_hard_lock_blocks_unlink_and_rename` | `cargo test -p studiofs-vfs foreign_hard_lock_blocks_new_write_open_but_allows_readonly_open -- --nocapture`<br>`cargo test -p studiofs-vfs existing_writer_handle_remains_usable_after_foreign_hard_lock_becomes_known -- --nocapture`<br>`cargo test -p studiofs-vfs foreign_hard_lock_blocks_size_change_setattr_without_handle -- --nocapture`<br>`cargo test -p studiofs-vfs foreign_hard_lock_blocks_unlink_and_rename -- --nocapture` |
| PG disconnect headroom exhaustion and read-only degradation | new mutations stop once local inode windows / block headroom are exhausted, while known readable state stays available | `read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open`; `next_inode_reports_read_only_headroom_once_windows_are_exhausted`; `open_with_persisted_slot_and_ranges_survives_pg_unavailable` | `cargo test -p studiofs-vfs read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open -- --nocapture`<br>`cargo test -p studiofs-meta next_inode_reports_read_only_headroom_once_windows_are_exhausted -- --nocapture`<br>`cargo test -p studiofs-meta open_with_persisted_slot_and_ranges_survives_pg_unavailable -- --nocapture` |
| Local-only durability and replay while S3 is unavailable | accepted bytes stay readable locally, queued uploads replay after restart, and reconnect replay keeps only the live state | `flush_with_offline_s3_keeps_data_locally_committed_and_queued`; `offline_s3_does_not_break_local_read_after_write`; `recovery_after_offline_s3_preserves_local_accepted_view`; `repeated_graceful_restarts_during_upload_replay_are_idempotent`; `restart_replays_only_live_pending_extents` | `cargo test -p studiofs-vfs --test vfs_test flush_with_offline_s3_keeps_data_locally_committed_and_queued -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test offline_s3_does_not_break_local_read_after_write -- --nocapture`<br>`cargo test -p studiofs-vfs --test vfs_test recovery_after_offline_s3_preserves_local_accepted_view -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test repeated_graceful_restarts_during_upload_replay_are_idempotent -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-vfs --test vfs_test restart_replays_only_live_pending_extents -- --nocapture` |
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
| Known published reads while PG is unavailable | persisted local inode-window and block-range state must allow offline reopen without fabricating new shared truth, and already published blocks remain readable | `test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror`; `open_with_persisted_slot_and_ranges_survives_pg_unavailable` | `cargo test -p studiofs-meta test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror -- --ignored --nocapture --test-threads=1`<br>`cargo test -p studiofs-meta open_with_persisted_slot_and_ranges_survives_pg_unavailable -- --nocapture` |

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
| Windows WinFSP sequential writer | AJA-style large sequential writer must keep cleanup observational and preserve close-side barrier ownership | `winfsp_mount_aja_style_sequential_writer_validation_smoke` | `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity winfsp_mount_aja_style_sequential_writer_validation_smoke -- --ignored --nocapture --test-threads=1` |

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
`baseline-read`, `during-upload-read`, and `post-upload-read` phases. When
mounted process observability is available, inspect the `Mounted memory`
terminal table or JSON `mounted_memory` section for `rss_bytes_peak`,
`rss_bytes_steady_state`, `dirty_bytes_peak`, and `dirty_limit_bytes`. These
fields are best-effort and may be omitted when the measured run does not expose
that signal.

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
- StudioFS `fsync` always confirms local durability; explicit `flush` and
  last-handle `close` also confirm the newest local unpublished state when
  dirty state remains, not remote publication.
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
  pressure from other runtime memory growth when those samples are present.
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
