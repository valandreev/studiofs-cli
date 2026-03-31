# Tests Guide

## Test Layers

1. Fast local/unit tests (default).
2. Ignored integration tests with RustFS/S3 + PostgreSQL.

## Prerequisites

- Rust toolchain
- RustFS or another S3-compatible backend (for data integration tests)
- PostgreSQL (for metadata PG integration tests)
- Optional `.env` in repo root

Example `.env` keys:
- `STUDIOFS_S3_ENDPOINT=http://127.0.0.1:9000`
- `STUDIOFS_S3_ACCESS_KEY=studiofs`
- `STUDIOFS_S3_SECRET_KEY=studiofs123`
- `STUDIOFS_S3_BUCKET=studiofs-test`
- `STUDIOFS_TEST_PG_DSN=host=... port=5432 user=... password=... sslmode=disable`

StudioFS will use PostgreSQL database `studiofs` automatically.
Local development and infra validation should target RustFS on the
S3-compatible side unless a test explicitly checks another provider.

## Command Matrix

| Scope | Command |
|---|---|
| Full fast suite | `cargo test --workspace --all-targets -- --nocapture` |
| Full ignored integration suite | `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1` |
| PG metadata suite | `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1` |
| VFS ignored suite | `cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture --test-threads=1` |
| Buffered-first fast suite (no S3) | `cargo test -p studiofs-vfs --test vfs_test sfs121_ -- --nocapture` |
| Buffered-first infra suite (S3 required) | `cargo test -p studiofs-vfs --test vfs_test sfs121_ -- --ignored --nocapture --test-threads=1` |
| CLI surface enforcement | `cargo test -p studiofs -- --nocapture` |
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |
| Destroy command integration suite | `cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1` |
| Dump/Load recovery integration suite | `cargo test -p studiofs --test dump_load_test -- --ignored --nocapture --test-threads=1` |
| FUSE bridge unit + smoke | `cargo test -p studiofs-fuse -- --nocapture` and `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1` |
| WinFSP bridge unit + smoke | `cargo test -p studiofs-winfsp -- --nocapture` and `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1` |

## Platform-Gated Unit Tests

`studiofs-fuse` and `studiofs-winfsp` contain unit tests that are only
meaningful on the OS they target. These tests must be run on the correct
host — cross-compilation or a Linux-only CI pass does not count.

### Rules for writing platform-gated unit tests

1. **Use the narrowest `cfg` guard that reflects the real constraint.**
   - Tests that validate Linux-specific behavior (e.g., `fusermount3` semantics): `#[cfg(target_os = "linux")]`
     Prefer this over `#[cfg(not(target_os = "macos"))]` — the latter would also fire on
     FreeBSD and other Unix variants where `fusermount3` is not present and the
     behavior would be undefined.
   - Tests that validate macOS-specific mount options (`volname`, `local`,
     `foreground`): `#[cfg(target_os = "macos")]`
   - Tests that validate WinFSP behavior (drive letters, timeout parsing):
     no extra `cfg` needed — the crate root already has `#![cfg(windows)]`

2. **Do not use `#[ignore]` to skip platform unit tests.**
   Platform-gated tests that would be useless on other OSes must use `cfg`
   guards, not `#[ignore]`. CI command details per platform:
   - Linux: `cargo test -p studiofs-fuse --lib` (unit tests only).
   - macOS: `cargo test -p studiofs-fuse --lib --tests` (unit tests +
     integration tests). The `--tests` flag is required so that
     non-mount platform tests placed in `tests/` (e.g.
     `macos_resource_fork_roundtrip_uses_native_xattr_store`) are also executed.
   - Windows: `cargo test -p studiofs-winfsp --lib` (unit tests only)
   Only the relevant runner runs each step; the other platforms skip it entirely
     `macos_resource_fork_roundtrip_uses_native_xattr_store`) are also executed.
   - Windows: `cargo test -p studiofs-winfsp --lib` (unit tests only)
   Only the relevant runner runs each step; the other platforms skip it entirely
   via `cfg` guards in the test code.

3. **Non-mount platform tests may live in `tests/` or `src/`; mount-requiring
   tests belong in ignored integration tests.**
   Non-mount platform tests that are too heavy or too coupled to other crates
   for a `src/` unit-test module are allowed in `tests/` — CI picks them up via
   `--tests`. Tests that require `FUSE` or `WinFSP` kernel interaction must be
   placed in dedicated smoke-test files (e.g. `mount_sanity.rs`) and marked
   `#[ignore]` to keep them out of the standard `cargo test -p <crate>` run.

4. **Environment-variable mutation in tests (WinFSP only).**
   Tests inside `studiofs-winfsp` that call `std::env::set_var`/`remove_var`
   must hold `ENV_LOCK` (defined in the `tests` module) for the duration of the
   env mutation. In CI the crate is run with `--test-threads=1`, which
   serialises the entire binary and makes isolation explicit. If you need to
   add new env-mutating tests, ensure they acquire `ENV_LOCK` and consider
   whether `temp_env` or `serial_test` would be cleaner than a manual mutex.

### CI coverage per platform

| Crate | CI step | Runner |
|-------|---------|--------|
| `studiofs-fuse` (Linux paths) | `cargo test (studiofs-fuse — Linux paths)` | `ubuntu-latest` |
| `studiofs-fuse` (macOS paths) | `cargo test (studiofs-fuse — macOS paths)` | `macos-latest` |
| `studiofs-winfsp` | `cargo test (studiofs-winfsp)` | `windows-latest` |

Each step is separate from the workspace run so a failure directly identifies
the failing backend in the CI job log.

## Important Current Regressions

The current architecture relies on these focused regressions:
- PG delta/checkpoint model:
  - `test_pg_remote_client_reads_committed_blocks_without_local_staged_mirror`
  - `test_pg_overlay_allows_unrelated_remote_batches_to_apply`
  - `test_pg_conflicting_batch_does_not_block_later_foreign_batch`
  - `test_pg_peer_can_rename_foreign_visible_base_entry`
  - `test_pg_peer_can_unlink_foreign_visible_base_entry`
  - `test_pg_peer_can_update_foreign_visible_symlink_target`
  - `test_pg_local_xattr_overlay_masks_foreign_update_until_flush`
  - `test_pg_checkpoint_excludes_uncommitted_local_blocks`
  - `test_pg_reopen_after_publish_before_local_persist_recovers_without_discover`
  - `test_pg_reopen_after_publish_then_remote_unlink_recovers_without_discover`
  - `test_pg_reopen_after_unpublished_local_create_flushes_without_discover`
  - `test_pg_reopen_after_claimed_batch_crash_flushes_without_discover`
  - `test_pg_reopen_after_claimed_multi_event_batch_preserves_sequence_without_discover`
- local write-path correctness:
  - `test_truncate_shrink_then_extend_zero_fills_old_tail`
  - `test_truncate_to_zero_then_rewrite`
  - `test_write_at_exact_block_boundary_transition`
  - `open_unlink_keeps_data_readable_while_canceling_pending_uploads`
  - `large_file_unlink_cancels_pending_uploads`
  - `test_crash_recovery_replays_ingress_spill_before_metadata_flush`
  - `test_ingress_prefers_spill_before_backpressure`
  - `test_ingress_pressure_reports_backpressure_when_spill_is_full`
  - `test_ingress_backpressure_wait_time_accumulates_until_drain`
  - `test_ingress_multi_writer_pressure_prefers_spill_without_wait`
  - `test_ingress_multi_writer_spill_recovers_after_crash`
  - `test_crash_recovery_replays_from_segments_when_spill_files_are_missing`
  - `test_ingress_backpressure_wait_time_accumulates_until_drain`
- metadata undo-log / orphan recovery:
  - `test_recover_undo_logs_replays_deferred_delete`
  - `test_recover_undo_logs_replays_pending_and_deferred_mixed`
  - `test_recover_undo_logs_replays_deferred_from_replaying_phase`
  - `test_recover_undo_logs_replays_pending_from_replaying_phase`
  - `test_remove_undo_logs_for_marks_completed_until_cleanup`
  - `test_remove_deferred_delete_performs_post_cleanup_of_completed_undo`
  - `test_remove_pending_create_performs_post_cleanup_of_completed_undo`
  - `test_cleanup_stale_undo_logs_removes_completed_even_if_live_rows_exist`
  - `test_repeated_pending_and_deferred_cycles_do_not_accumulate_undo_logs`
- janitor runtime recovery:
  - `test_background_janitor_recovers_pending_create_intent_runtime`
  - `test_background_janitor_does_not_recover_deferred_delete_undo_runtime`
  - `test_background_janitor_recovers_only_pending_create_in_mixed_runtime`
- namespace resolver parity:
  - `namespace_resolver_full_path_matches_component_lookup_for_ignored_and_blocked_paths`
  - `namespace_resolver_windows_and_posix_match_on_same_namespace_entry`
  - `namespace_resolver_windows_parent_normalization_matches_winfsp_semantics`
- path shortcut cache:
  - `path_shortcut_cache_is_bounded_and_supports_negative_entries`
  - `path_shortcut_cache_rejects_stale_parent_generations`
  - `path_shortcut_cache_tracks_hotset_occupancy_and_hits`
  - `namespace_resolver_path_shortcut_hits_and_renames_invalidate`
  - `namespace_resolver_path_shortcut_invalidation_on_delete_subtree`
- namespace cache invalidation tokens:
  - `dentry_cache_rejects_stale_namespace_tokens_lazily`
  - `setattr_does_not_bump_namespace_generations`
- metadata hotset promotion:
  - `dentry_cache_tracks_hotset_occupancy_and_hits`
  - `attr_read_cache_is_bounded_and_tracks_hotset_hits`
- Buffered-first correctness and performance (manual S3 tests; not covered by the `sfs121_`-filtered SFS-121 suite):
  - `local_reads_through_upload_with_partial_tail_block` *(requires S3)*
  - `concurrent_reads_through_upload_finalization` *(requires S3)*
- SFS-121 buffered-first regression suite (tests prefixed `sfs121_`, covered by the “Buffered-first” commands):
  - `sfs121_buffered_large_file_read_succeeds_across_full_upload_lifecycle` *(requires S3)*
  - `sfs121_sequential_foreground_read_throughput_during_upload_meets_minimum` *(requires S3)*
  - `sfs121_foreground_read_baseline_during_and_post_upload_are_comparable` *(requires S3; compares read-alone vs during-upload vs post-upload in one run)*
  - `sfs121_warm_cache_sequential_read_bounded_latency_after_write`
  - `sfs121_mixed_workload_concurrent_readers_during_pending_upload_no_errors`
  - `sfs121_file_session_flush_ordering_is_deterministic`
- SFS-121 CLI surface enforcement (regression gate against re-introducing `--direct-io`):
  - `no_direct_io_flag_in_any_subcommand`
  - `mount_subcommand_has_no_direct_io_flag`

Run a single focused regression:

```bash
cargo test -p studiofs-meta test_pg_checkpoint_excludes_uncommitted_local_blocks -- --ignored --nocapture
cargo test -p studiofs-vfs --test vfs_test test_truncate_shrink_then_extend_zero_fills_old_tail -- --ignored --nocapture
cargo test -p studiofs-vfs --test vfs_test sfs121_foreground_read_baseline_during_and_post_upload_are_comparable -- --ignored --nocapture --test-threads=1
```

## Recommended Local Flow

```bash
cargo test --workspace --all-targets -- --nocapture
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

When a test owns an explicit Tokio runtime around `StudioVfs`, shut the VFS
down inside `block_on(...)` before calling `rt.shutdown_background()`. This
avoids teardown-only panics from background timer tasks trying to observe a
runtime that is already shutting down.

## Platform Bridge Smoke Tests

The bridge mount/unmount smoke tests are intentionally `#[ignore]` because they
require host platform support (`/dev/fuse` or WinFSP driver/runtime). Enable
them only when validating platform bridge changes:

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1
```

```powershell
$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'
$env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'
cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1
```

## Doc/Test Name Sync Check

To ensure regression names in this document do not drift from code:

```powershell
pwsh -File tools/check-doc-test-names.ps1
```

PowerShell one-liner with `.env` + proxy cleanup (recommended for infra tests):

```powershell
Set-Location E:\dev\studiofs-cli; Get-Content .env | Where-Object { $_ -match '^\s*[^#].+=' } | ForEach-Object { $k,$v = $_.Split('=',2); Set-Item -Path "Env:$k" -Value $v }; Remove-Item Env:HTTP_PROXY,Env:HTTPS_PROXY,Env:ALL_PROXY -ErrorAction SilentlyContinue; $env:NO_PROXY='localhost,127.0.0.1,::1'; cargo test --workspace --all-targets -- --nocapture; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }; cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

## Troubleshooting

### PG test failures
- Check `STUDIOFS_TEST_PG_DSN` correctness.
- Verify PostgreSQL is reachable and database `studiofs` can be created or already exists.
- Re-run single test with `--nocapture`.
- Prefer the documented `--test-threads=1` mode for ignored PG suite runs; the
  suite uses background poll/flush timing and is intentionally validated in
  single-threaded mode.

### S3 backend failures
- Validate endpoint/credentials/bucket.
- Ensure bucket permissions include create/read/write/delete.
- If process-level `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` is set, clear them for test runs.
- Keep `NO_PROXY` including `localhost,127.0.0.1,::1` (and your direct RustFS/S3 host if needed).

### Intermittent VFS crash-recovery failures
- `test_crash_recovery_replays_ingress_spill_before_metadata_flush` and
  `test_crash_recovery_replays_ingress_spill_before_remote_fetch` have shown
  non-deterministic failures during full-suite runs.
- Current tracking issue: [SFS-30](https://linear.app/valandreev/issue/SFS-30/stabilize-crash-recovery-vfs-tests-env-isolation-deterministic-startup)
- `--test-threads=1` can reduce noise for reproductions, but does not fully
  eliminate this flake pattern.

### Windows lock issues
- Close processes that hold files in `target/`.
- Re-run with `--test-threads=1`.
