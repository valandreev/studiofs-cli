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
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |
| Destroy command integration suite | `cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1` |
| Dump/Load recovery integration suite | `cargo test -p studiofs --test dump_load_test -- --ignored --nocapture --test-threads=1` |
| FUSE bridge unit + smoke | `cargo test -p studiofs-fuse -- --nocapture` and `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1` |
| WinFSP bridge unit + smoke | `cargo test -p studiofs-winfsp -- --nocapture` and `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1` |

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
  - `test_crash_recovery_replays_ingress_spill_before_metadata_flush`
  - `test_ingress_prefers_spill_before_backpressure`
  - `test_ingress_pressure_reports_backpressure_when_spill_is_full`
  - `test_ingress_backpressure_wait_time_accumulates_until_drain`
  - `test_ingress_multi_writer_pressure_prefers_spill_without_wait`
  - `test_ingress_multi_writer_spill_recovers_after_crash`
  - `test_crash_recovery_replays_from_segments_when_spill_files_are_missing`
- namespace resolver parity:
  - `namespace_resolver_full_path_matches_component_lookup_for_ignored_and_blocked_paths`
  - `namespace_resolver_windows_and_posix_match_on_same_namespace_entry`
  - `namespace_resolver_windows_parent_normalization_matches_winfsp_semantics`
- path shortcut cache:
  - `path_shortcut_cache_is_bounded_and_supports_negative_entries`
  - `path_shortcut_cache_rejects_stale_parent_generations`
  - `namespace_resolver_path_shortcut_hits_and_renames_invalidate`
  - `namespace_resolver_path_shortcut_invalidation_on_delete_subtree`
- namespace cache invalidation tokens:
  - `dentry_cache_rejects_stale_namespace_tokens_lazily`
  - `setattr_does_not_bump_namespace_generations`

Run a single focused regression:

```bash
cargo test -p studiofs-meta test_pg_checkpoint_excludes_uncommitted_local_blocks -- --ignored --nocapture
cargo test -p studiofs-vfs --test vfs_test test_truncate_shrink_then_extend_zero_fills_old_tail -- --ignored --nocapture
```

## Recommended Local Flow

```bash
cargo test --workspace --all-targets -- --nocapture
cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
```

## Platform Bridge Smoke Tests

The bridge mount/unmount smoke tests are intentionally `#[ignore]` because they
require host platform support (`/dev/fuse` or WinFSP driver/runtime). Enable
them only when validating platform bridge changes:

```bash
STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1
```

```powershell
$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'
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

### Windows lock issues
- Close processes that hold files in `target/`.
- Re-run with `--test-threads=1`.
