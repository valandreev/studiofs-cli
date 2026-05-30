# Tests Guide

## Verification Matrix

StudioFS runs Linux PR verification in RWX. GitHub Actions macOS, Windows, and
smoke workflows are manual-only and can be started from the GitHub Actions UI
when hosted signal is useful.

The authoritative matrix — what each job checks, what is not covered
automatically, and the compile/unit/smoke distinctions — lives in
[docs/contributing.md](contributing.md#verification-matrix).

For the final 2026-04-19 canonical contract validation surface, start with
[docs/local-first-validation-matrix.md](local-first-validation-matrix.md).
That document is the focused `SFS-270` entrypoint and splits fast/local,
offline/replay, infra-backed, and bridge checks before you jump to the broader
workspace commands below.

For final encryption-at-rest acceptance, use the
[SFS-357 final encrypted-at-rest acceptance gate](local-first-validation-matrix.md#sfs-357-final-encrypted-at-rest-acceptance-gate).
It collects the encrypted-object unit, integration, E2E, platform smoke, and
performance checks that should be recorded before closing the feature.

---

## Test Layers

1. Fast local/unit tests (default).
2. Ignored integration tests with RustFS/S3 + PostgreSQL.

## Contract/Core Harness

Fast contract-level checks that should not require Docker, PostgreSQL, RustFS,
FUSE, or WinFSP live in the Contract/Core harness:

- `studiofs_meta::contract_harness::ContractCoreFixture` creates isolated
  `RedbMetaEngine` fixtures, applies deterministic namespace-style operations,
  reopens the same local metadata path, and runs shared tree/metadata/object
  graph invariants with diagnostic snapshots.
- `studiofs_vfs::contract_harness::ContractVfsFixture` layers a `StudioVfs`
  fixture and local cache directory on top of the same metadata harness, then
  delegates invariant checks back to the shared runner.

Focused harness checks:

```powershell
cargo test -p studiofs-meta contract_core_harness_runs_ops_and_reopens_with_invariants -- --nocapture
cargo test -p studiofs-meta contract_core_harness -- --nocapture
cargo test -p studiofs-meta sfs314_ -- --nocapture
cargo test -p studiofs-meta sfs315_ -- --nocapture
cargo test -p studiofs-meta sfs316_ -- --nocapture
cargo test -p studiofs-vfs contract_vfs_harness_reopens_fixture_and_runs_core_invariants -- --nocapture
cargo test -p studiofs-vfs sfs312_ -- --nocapture
cargo test -p studiofs-vfs local_first_shutdown_reports_and_recovers_memory_only_writer_state -- --nocapture
```

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

### SFS-451 Transport TLS Fixtures

SFS-451 adds focused transport validation without replacing the loopback
RustFS/PostgreSQL fixtures. The optional external TLS fixtures skip cleanly when
unset and are intended for operator-provided services:

- `STUDIOFS_TEST_PG_TLS_DSN=host=... port=5432 user=... password=... sslmode=require`
- `STUDIOFS_S3_HTTPS_ENDPOINT=https://s3-compatible.example`

Loopback RustFS/PostgreSQL fixtures remain valid for local development and
CI-style ignored tests that intentionally use `http://127.0.0.1:9000` and
`sslmode=disable`.

Focused transport policy checks:

```powershell
cargo test -p studiofs-meta pg_dsn -- --nocapture
cargo test -p studiofs-meta pg_transport -- --nocapture
cargo test -p studiofs-data endpoint -- --nocapture
cargo test -p studiofs --test architecture_vocabulary_test tests_guide_covers_sfs451_transport_tls_fixtures -- --nocapture
```

Optional TLS fixture probes:

```powershell
$env:STUDIOFS_TEST_PG_TLS_DSN='host=... port=5432 user=... password=... sslmode=require'
cargo test -p studiofs-meta tls_postgres_fixture_connects_when_configured -- --ignored --nocapture

$env:STUDIOFS_S3_HTTPS_ENDPOINT='https://s3-compatible.example'
cargo test -p studiofs-data --test integration_test https_endpoint_fixture_put_get_when_configured -- --ignored --nocapture
```

### SFS-452 Final Transport Acceptance Gate

SFS-452 is the final docs and search gate for the transport track. Use focused
checks instead of the full workspace suite:

```powershell
cargo test -p studiofs --test architecture_vocabulary_test docs_cover_sfs452_final_transport_acceptance_gate -- --nocapture
cargo check -p studiofs-meta -p studiofs-data -p studiofs
rg -n -F "tokio_postgres::connect(" crates docs
rg -n "tokio_postgres::NoTls|sslmode=disable|http://" crates docs -S
```

Expected search result shape:

- production PostgreSQL call sites use `connect_studiofs_postgres` instead of
  `tokio_postgres::NoTls`;
- remaining `tokio_postgres::NoTls` call sites are local fixture setup;
- non-local S3-compatible endpoints require HTTPS;
- plain HTTP S3 is accepted only for explicit local/test loopback endpoints;
- policy-invalid non-local S3 transports are rejected before init, persisted
  mount, or endpoint override;
- `sslmode=disable` cleartext choices must be logged at info level for
  non-local hosts.

Local loopback examples remain available only as loopback-only development/test
examples.

## Command Matrix

| Scope | Command |
|---|---|
| Workspace lint inheritance guard | `cargo test -p studiofs --test architecture_vocabulary_test workspace_members_inherit_workspace_lints` |
| Focused lint/check validation | `cargo clippy -p <crate> --all-targets -- -D warnings` and `cargo check -p <crate>` |
| Full fast suite | `cargo test --workspace --all-targets -- --nocapture` |
| Full ignored integration suite | `cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1` |
| PG metadata suite | `cargo test -p studiofs-meta pg_engine::tests::test_pg_ -- --ignored --nocapture --test-threads=1` |
| VFS ignored suite | `cargo test -p studiofs-vfs --test vfs_test -- --ignored --nocapture --test-threads=1` |
| Local-first VFS fast checks (no S3) | See [Local-first VFS fast checks](#local-first-vfs-fast-checks-no-s3). |
| Buffered-first infra suite (S3 required) | `cargo test -p studiofs-vfs --test vfs_test sfs121_ -- --ignored --nocapture --test-threads=1` |
| CLI surface enforcement | `cargo test -p studiofs -- --nocapture` |
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |
| Destroy command integration suite | `cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1` |
| Dump/Load recovery integration suite | `cargo test -p studiofs --test dump_load_test -- --ignored --nocapture --test-threads=1` |
| FUSE bridge unit + smoke | `cargo test -p studiofs-fuse -- --nocapture` and `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity -- --ignored --nocapture --test-threads=1` |
| FUSE async write mounted matrix | `cargo test -p studiofs-fuse --test mount_sanity async_mounted_test_matrix_covers_correctness_and_platform_opt_in_scenarios -- --exact --nocapture` and `STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS=1 cargo test -p studiofs-fuse --test mount_sanity async_mounted -- --ignored --nocapture --test-threads=1` |
| WinFSP bridge unit + smoke | `cargo test -p studiofs-winfsp -- --nocapture` and `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1` |

### Local-first VFS fast checks (no S3)

The old `sfs121_` filter only matched comments and helper names, not test
function names. Run these exact non-ignored local-first VFS checks instead:

```bash
cargo test -p studiofs-vfs --test vfs_test upload_backlog_progresses_via_persisted_local_payload_reads -- --exact --nocapture
cargo test -p studiofs-vfs --test vfs_test warm_cache_sequential_read_bounded_latency_after_write -- --exact --nocapture
cargo test -p studiofs-vfs --test vfs_test mixed_workload_concurrent_readers_during_pending_upload_no_errors -- --exact --nocapture
cargo test -p studiofs-vfs --test vfs_test handle_aware_read_policy_keeps_sequential_and_random_streams_separate -- --exact --nocapture
cargo test -p studiofs-vfs --test vfs_test file_session_flush_ordering_is_deterministic -- --exact --nocapture
```

### Smoke test descriptions

The `mount_sanity` integration tests in each bridge crate contain two tiers:

| Test | Backend | Verifies |
|------|---------|----------|
| `fuse_session_mount_and_drop_smoke` | FUSE | Session creation and immediate drop (minimal lifecycle) |
| `fuse_mount_write_read_unmount_smoke` | FUSE | Full lifecycle: mount → write → read → readdir → delete → unmount |
| `async_mounted_test_matrix_covers_correctness_and_platform_opt_in_scenarios` | FUSE | Fast, non-mounted guard that keeps the async FUSE mounted scenario matrix aligned with SFS-468 acceptance |
| `async_mounted_*` | FUSE | Individual opt-in mounted async write scenarios for sequential readback, flush, fsync, release/reopen, multiple writers, same-handle appends, and unmount-after-write |
| `linux_async_mounted_*` | FUSE/Linux | Individual opt-in mounted checks for Linux default sync mode, injected async write mode, injected async DIO mode, and optional `fio` multi-job writes |
| `macos_async_mounted_*` | FUSE/macOS | Individual opt-in mounted macOS workloads for `dd`, optional `fio`, fsync-heavy writes, and Finder-style multi-file copy |
| `winfsp_mount_unmount_smoke` | WinFSP | Mount startup and named-event shutdown (minimal lifecycle) |
| `winfsp_mount_write_read_unmount_smoke` | WinFSP | Full lifecycle: mount → write → read → readdir → delete → unmount |
| `macos_resource_fork_roundtrip_uses_native_xattr_store` | FUSE (macOS) | Resource fork xattr round-trip (no mount required) |

The full-lifecycle tests (`*_write_read_unmount_smoke`) are the primary smoke
tests for SFS-133. They exercise file I/O through the real OS filesystem layer,
not just session creation.

The WinFSP suite also contains post-upload/finalization cases. Those require a
reachable S3 backend and return early when the resolved test endpoint is not
available, which keeps the hosted Windows smoke job aligned with the documented
dummy-endpoint contract.

See [docs/smoke-test-strategy.md](smoke-test-strategy.md) for the
complete smoke-test strategy, CI integration, and self-hosted runner contract.

### Real mounted E2E runner contract

The real E2E runner lives in the `studiofs-e2e` workspace crate and must follow
[docs/e2e-runner-contract.md](e2e-runner-contract.md). That contract fixes the
runner crate boundary, CLI shape, `target/e2e-runs/<run-id>` sandbox layout,
required artifact names, report schema, preflight gates, cleanup safety rules,
and the smoke-first scenario rollout. Real E2E runs stay outside the default
fast suite.

Initial runner commands:

```powershell
cargo run -p studiofs-e2e -- list
cargo build -p studiofs
cargo run -p studiofs-e2e -- run basic-multi-client-smoke --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run --scenario 1-8 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run --scenario 2,5,10 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-chaos-convergence --seed 360 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-lww-reconnect-chaos-convergence --seed 360 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-sparse-file-range-convergence --seed 377 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-cold-observer-long-history-convergence --seed 378 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-stale-remount-cold-discover-catchup-convergence --seed 418 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-invalidation-dropout-fallback-convergence --seed 379 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-symlink-attribute-convergence --seed 380 --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run rustfs-outage-multi-client-recovery --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-client-crash-restart-recovery --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run collision-rename-vs-unlink --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- local-first-baseline-gate --studiofs-bin target\debug\studiofs.exe --run-id-prefix sfs405-baseline --iterations 3 --seed 405 --port-range 29700-29800
cargo run -p studiofs-e2e -- clean
```

SFS-438 mounted runner lifecycle smoke for local operator validation:

```powershell
cargo build -p studiofs
cargo run -p studiofs-e2e -- run basic-multi-client-smoke --studiofs-bin target\debug\studiofs.exe
```

Do not use the full mounted scenario sweep for this local operator check. The
focused smoke is enough to validate the runner lifecycle boundary before a
broader hosted or manual mounted pass.

SFS-381 adversarial cleanup proof is a focused runner self-test layer, not a
new mounted scenario number. It validates partial setup failure cleanup, hung
supervised child timeout reporting, mounted contamination boundaries, and
primary-error preservation through infra-disabled tests and
the best-effort `artifacts/runner/adversarial-cleanup-proof.json` artifact:

```powershell
cargo test -p studiofs-e2e runner::tests::compose_partial_launch_failure_writes_adversarial_cleanup_proof -- --exact --nocapture
cargo test -p studiofs-e2e runner::tests::post_start_cleanup_report_errors_do_not_shadow_primary_error -- --exact --nocapture
cargo test -p studiofs-e2e runner::tests::mounted_child_failure_blocks_later_mounted_children -- --exact --nocapture
cargo test -p studiofs-e2e runner::tests::mounted_supervision_error_writes_report_without_post_scenario_cleanup_verifier -- --exact --nocapture
cargo test -p studiofs-e2e runner::tests::supervised_child_timeout_records_executor_cleanup_detail -- --exact --nocapture
```

`run` uses Docker Compose infrastructure by default. The stack is defined in
`docker/compose.e2e.yml` and includes PostgreSQL 18, RustFS, and a Toxiproxy
placeholder. Use `--port-range <start-end>` to allocate deterministic
project-scoped ports for concurrent local runs, or `--infra disabled` only for
runner self-tests that must not touch Docker. The basic mounted smoke scenario
uses three isolated StudioFS clients, mounts each client separately, writes a
small file through the first mount, and validates convergence through the public
mounted trees. The RustFS/S3 outage scenario stops only the RustFS compose
service, keeps accepted writes locally readable, verifies a peer does not see
the new outage checksum before recovery, and reports upload recovery time plus
final checksum convergence. The mounted client crash/restart scenario kills and
remounts one mounted client with the same `STUDIOFS_CLIENT_ROOT`; when the
writer can read recovered bytes after restart, that recovered checksum becomes
the peer-convergence oracle for both `after_local_durability_boundary` and
`before_remote_publication_completes` modes. Unrecoverable local loss is
reported separately from publication failure, peer convergence failure, and a
corrupt advertised file. On Linux, crash-style remount attempts lazy stale FUSE
mount cleanup before starting the replacement process; publication readiness is
validated through mounted-tree convergence rather than private backend
inspection. The mounted namespace collision scenarios use three isolated
clients and barrier-start conflicting operations covering
rename/rename, rename/unlink, unlink/append, overwrite/read, mkdir/rmdir/create,
and readdir/stat while mutations are in flight. The overwrite/read scenario
records repeated read loops during the overwrite. Each collision run writes `operations.ndjson`,
`namespace-collision-summary.json`, and `validation.json`.

`mounted-sparse-file-range-convergence` is a deliberate post-1-16 promotion
scenario. It uses a bounded sparse file, validates zero-filled holes,
shrink/extend truncation, chunk-boundary writes, range read probes, logical
size, and final checksums through mounted reads, and writes `operations.ndjson`,
`sparse-range-summary.json`, and `validation.json`.
`mounted-cold-observer-long-history-convergence` is the next post-1-16
promotion scenario. It lets warm mounted clients build a bounded mutation
history before a fresh `client-d` local root/cache discovers and mounts the
volume, then validates canonical paths, content checksums, tombstone behavior,
and absence of resurrected entries through mounted reads. It writes
`operations.ndjson`, `cold-observer-long-history-summary.json`,
`cold-observer-diagnostics.json`, and `validation.json`.
`mounted-stale-remount-cold-discover-catchup-convergence` is the next
post-1-16 promotion scenario. It starts `client-a`, `client-b`, and `client-c`,
unmounts `client-c` after a public baseline is visible, lets `client-a` and
`client-b` publish mixed namespace and payload history, then remounts stale
`client-c` and discovers fresh `client-d` after the history exists. Validation
uses only public mounted reads and tree convergence. It writes
`operations.ndjson`, `stale-remount-cold-discover-summary.json`,
`catchup-diagnostics.json`, and `validation.json`.
`mounted-invalidation-dropout-fallback-convergence` is the next post-1-16
promotion scenario. It suppresses metadata notify/background polling for
`client-c` through an explicit E2E-only mount environment switch, keeps
fallback sync enabled on a bounded interval, publishes create/overwrite/delete
activity from `client-a` and `client-b`, verifies the observer's stale window
for both payload updates and deleted-path visibility, and then validates final
convergence plus deleted-path absence through public mounted reads. It writes
`operations.ndjson`, `invalidation-dropout-summary.json`,
`invalidation-fallback-diagnostics.json`, and `validation.json`.
`mounted-symlink-attribute-convergence` is the next post-1-16 promotion
scenario. It uses public mounted filesystem behavior for symlink
create/read-target/rename/unlink-recreate target replacement, writes near the
symlink namespace, and checks the supported read-only file attribute. Platform
capability skips are explicit in `symlink-attribute-summary.json`; on
Windows/WinFSP, `ERROR_GEN_FAILURE` from symlink creation after the bounded
mount-readiness retry is treated as an unsupported symlink skip. Supported
behavior must converge through mounted reads and the mounted-tree validator. It
writes `operations.ndjson`, `symlink-attribute-summary.json`, and
`validation.json`.

Hosted Linux real mounted E2E is available as a manual RWX workflow:

```powershell
rwx run .rwx/e2e.yml --target e2e-smoke --wait --open
rwx run .rwx/e2e.yml --target e2e-full --wait --open
```

The smoke target runs `basic-multi-client-smoke` only. The full target runs
the `1-23` scenario selector. Both require RWX Docker support, `/dev/fuse`,
`fusermount3`, and readable FUSE configuration on the hosted Linux task. They
do not add a GitHub Actions workflow, PR trigger, or schedule. RWX uploads
`target/e2e-runs/<run-id>/` as an artifact so `report.json`, `preflight.json`,
Docker diagnostics, scenario artifacts, and client logs remain available on
failure.

Focused runner skeleton check:

```powershell
cargo test -p studiofs-e2e --test runner_contract
cargo test -p studiofs-e2e --test compose_infra
cargo test -p studiofs-e2e --test mounted_tree_validator
cargo test -p studiofs-e2e namespace_collision_records_operations_and_validates_three_client_convergence -- --exact --nocapture
cargo test -p studiofs-e2e scenario::tests::crash_restart_recovery_runs_both_modes_reuses_client_root_and_converges -- --exact --nocapture
cargo test -p studiofs-e2e --test client_controller force_kill_then_remount_reuses_same_client_root_without_graceful_umount -- --exact --nocapture
```

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
| `studiofs-fuse` (Linux paths) | `cargo-test-studiofs-fuse-linux` / `cargo-test-studiofs-fuse-rocky` | RWX `ubuntu:24.04` + `rockylinux:9` |
| `studiofs-fuse` (macOS paths) | `cargo test (studiofs-fuse — macOS paths)` | `macos-latest` |
| `studiofs-winfsp` | `cargo test (studiofs-winfsp)` | `windows-latest` |

Each step is separate from the workspace run so a failure directly identifies
the failing backend in the CI job log.

## Final Architecture Invariants

The final chunk/slice/payload-object model is directly covered by these focused
regressions:

- Read/write resolution
  - `test_contract_core_slice_graph_resolves_logical_byte_view_after_reopen`
  - `test_resolve_chunk_slice_spans_overlay_over_base_with_hole`
  - `test_lookup_regular_file_slices_boundary_spanning_write_is_split_per_chunk`
  - `offline_s3_does_not_break_local_read_after_write`
  - `sfs312_write_is_visible_from_writer_state_before_local_durability_boundary`
  - `local_first_shutdown_reports_and_recovers_memory_only_writer_state`

- Publication and visibility
  - `sfs314_publish_requires_all_overlay_payloads_uploaded`
  - `sfs314_uploaded_payload_does_not_replace_peer_visible_base_before_publish`
  - `sfs314_partial_publish_preserves_base_tail_and_committed_size`
  - `sfs314_metadata_only_apply_keeps_attrs_and_file_graph_consistent`
  - `test_publish_regular_file_overlay_to_base_if_matches_promotes_base_rows`
  - `test_publish_regular_file_overlay_to_base_keeps_sparse_tail_committed_size_below_size`
  - `background_epoch_finalizer_keeps_non_publish_ready_epoch_local_after_restart` *(requires S3 env)*
  - `sfs312_flush_materializes_local_slice_boundary_without_remote_visibility`
  - `sfs312_fsync_persists_local_state_without_remote_visibility`
  - `sfs312_last_handle_release_drains_local_durability_boundary`

- Recovery and replay
  - `test_pg_reopen_after_publish_before_local_persist_recovers_without_discover`
  - `test_pg_reopen_after_unpublished_local_create_flushes_without_discover`
  - `sfs315_reopen_consumes_persisted_inode_and_object_headroom_without_pg`
  - `sfs316_reachability_reports_active_base_overlay_and_snapshot_pins`
  - `sfs316_publish_queues_displaced_objects_and_reachability_scan_is_idempotent`
  - `sfs316_delete_before_publish_tombstone_does_not_publish_deleted_content`
  - `startup_replay_cancels_ready_inode_stale_pending_upload`
  - `graceful_shutdown_remount_preserves_newer_local_epoch_over_published_base` *(requires S3 env)*
  - `test_truncate_shrink_then_extend_zero_fills_old_tail`
  - `test_write_at_exact_block_boundary_transition`

- Namespace and metadata stability
  - `namespace_resolver_full_path_matches_component_lookup_for_ignored_and_blocked_paths`
  - `path_shortcut_cache_is_bounded_and_supports_negative_entries`
  - `dentry_cache_rejects_stale_namespace_tokens_lazily`
  - `attr_read_cache_is_bounded_and_tracks_hotset_hits`

- Metadata authority and headroom
  - `sfs315_failed_object_batch_preserves_smaller_inode_headroom`
  - `sfs315_large_object_batch_replaces_undersized_prefetched_window`
  - `read_only_metadata_authority_blocks_new_mutations_but_keeps_reads_open`

Run a single focused regression:

Each command below is copy-paste ready and includes the full crate and test
binary prefix where applicable.

```bash
cargo test -p studiofs-meta test_contract_core_slice_graph_resolves_logical_byte_view_after_reopen -- --nocapture
cargo test -p studiofs-meta test_resolve_chunk_slice_spans_overlay_over_base_with_hole -- --nocapture
cargo test -p studiofs-meta sfs314_partial_publish_preserves_base_tail_and_committed_size -- --nocapture
cargo test -p studiofs-vfs --test vfs_test offline_s3_does_not_break_local_read_after_write -- --nocapture
cargo test -p studiofs-vfs --test vfs_test background_epoch_finalizer_keeps_non_publish_ready_epoch_local_after_restart -- --ignored --nocapture --test-threads=1
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

### WinFSP Write-Through Slow-Cache Validation Plan

SFS-307 changes should be validated on a Windows host with WinFSP installed and
a deliberately slow cache directory, such as a USB SSD/HDD or a throttled local
volume. The validation workload is:

1. Mount StudioFS with the slow cache root and normal WinFSP file-cache policy.
2. Write a multi-GB file sequentially through a handle opened with
   `FILE_WRITE_THROUGH` or `FILE_NO_INTERMEDIATE_BUFFERING`.
3. In parallel, issue lightweight metadata/read probes against the mount.
4. Confirm write throughput degrades instead of hanging permanently, callback
   latency remains bounded by the local device, `write_through_calls` increases
   in `winfsp_write_path`, and ordinary read-only handles still benefit from
   read caching.
5. Repeat ordinary write/read and cold-read smoke checks after the run to verify
   the writeback and cold-read paths did not regress.

### CI smoke workflow

Smoke tests also run in the separate `.github/workflows/smoke.yml` workflow:

| Trigger | Behavior |
|---------|----------|
| `workflow_dispatch` | Manual run — all platforms |

The workflow runs independently of the main `rust.yml` pipeline and never
starts automatically. macOS smoke tests use `continue-on-error: true` because
the macFUSE system extension may not load on hosted runners without a reboot.

See [docs/smoke-test-strategy.md](smoke-test-strategy.md) for the full
strategy document covering hosted vs self-hosted responsibilities.

## Doc/Test Name Sync Check

To ensure regression names in this document do not drift from code:

Windows:

```powershell
pwsh -File tools/check-doc-test-names.ps1
```

Linux / macOS:

```bash
sh tools/check-doc-test-names.sh
```

Both launchers call the same portable checker in
`tools/check-doc-test-names.py`; the PowerShell launcher delegates to the shell
launcher when it is run under PowerShell Core on Linux or macOS.

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
- `test_crash_recovery_replays_local_committed_block_before_metadata_flush` and
  `test_crash_recovery_replays_local_committed_block_before_remote_fetch` have shown
  non-deterministic failures during full-suite runs.
- Current tracking issue: [SFS-30](https://linear.app/valandreev/issue/SFS-30/stabilize-crash-recovery-vfs-tests-env-isolation-deterministic-startup)
- `--test-threads=1` can reduce noise for reproductions, but does not fully
  eliminate this flake pattern.

### Windows lock issues
- Close processes that hold files in `target/`.
- Re-run with `--test-threads=1`.
