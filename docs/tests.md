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

## Command Matrix

| Scope | Command |
|---|---|
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

The `mount_sanity` integration tests share two lifecycle smoke tiers as the
common bridge baseline, with bridge- or platform-specific additions where
needed:

| Test | Backend | Verifies |
|------|---------|----------|
| `fuse_session_mount_and_drop_smoke` | FUSE | Session creation and immediate drop (minimal lifecycle) |
| `fuse_mount_write_read_unmount_smoke` | FUSE | Full lifecycle: mount → write → read → readdir → delete → unmount |
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
cargo run -p studiofs-e2e -- run rustfs-outage-multi-client-recovery --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-client-crash-restart-recovery --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run collision-rename-vs-unlink --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-open-handle-convergence --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-chaos-convergence --seed 360 --timeout 20m --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-fault-chaos-convergence --seed 360 --timeout 30m --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-lww-reconnect-chaos-convergence --seed 360 --timeout 30m --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- run mounted-publication-boundary-crash-matrix --timeout 30m --studiofs-bin target\debug\studiofs.exe
cargo run -p studiofs-e2e -- clean
```

`run` uses Docker Compose infrastructure by default. The stack is defined in
`docker/compose.e2e.yml` and includes PostgreSQL 18, RustFS, and a Toxiproxy
placeholder. Use `--port-range <start-end>` to allocate deterministic
project-scoped ports for concurrent local runs, or when a default host port is
occupied or Docker reports it healthy but host-side StudioFS clients cannot
reach it. Use `--infra disabled` only for runner self-tests that must not touch
Docker. The basic mounted smoke scenario uses three isolated StudioFS clients,
mounts each client separately, writes a small file through the first mount, and
validates convergence through the public mounted trees. The RustFS/S3 outage
scenario stops only the RustFS compose
service, keeps accepted writes locally readable, verifies a peer does not see
the new outage checksum before recovery, and reports upload recovery time plus
final checksum convergence. The mounted client crash/restart scenario kills and
remounts one mounted client with the same `STUDIOFS_CLIENT_ROOT`, verifies
locally durable data remains readable after restart, and validates peer
convergence for both `after_local_durability_boundary` and
`before_remote_publication_completes` modes. On Linux, crash-style remount
attempts lazy stale FUSE mount cleanup before starting the replacement process;
publication readiness is validated through mounted-tree convergence rather than
private backend inspection.

Use `--infra external-env` for production-like runs against already-running
PostgreSQL and RustFS/S3. The runner reads `STUDIOFS_TEST_PG_DSN`,
`STUDIOFS_S3_ENDPOINT`, `STUDIOFS_S3_BUCKET`, `STUDIOFS_S3_ACCESS_KEY`, and
`STUDIOFS_S3_SECRET_KEY` from the process environment or workspace `.env`, then
uses a unique StudioFS volume name per run. This mode does not start, stop, or
clean external services, so scenarios whose purpose is to stop infrastructure
services still require `--infra compose`.

For Windows WinFSP external-infra debugging, prefer one mounted scenario per
command rather than a long PowerShell loop. If a mounted filesystem syscall
hangs below the runner, a one-scenario command limits the number of live mount
processes that can be left behind and leaves a smaller artifact set to inspect.
When a sweep is needed, pass an explicit `--timeout` so the runner can write a
best-effort timeout report and cleanup summary. Then inspect the newest
`target/e2e-runs/<run-id>/events.ndjson` and per-client logs before starting the
next scenario.

The mounted namespace collision scenarios use three
isolated clients and barrier-start conflicting operations covering
rename/rename, rename/unlink, unlink/append, overwrite/read, mkdir/rmdir/create,
and readdir/stat while mutations are in flight. The overwrite/read scenario
records repeated read loops during the overwrite. Each collision run writes
shared-schema `operations.ndjson`, `namespace-collision-summary.json`, and
`validation.json`. The `mounted-chaos-convergence` scenario is the deeper
seeded product chaos layer: it composes rename/rename, rename/unlink,
unlink/append, overwrite/read, mkdir/rmdir/create, and readdir/stat mutation
families across multiple rounds against three real mounted clients, records the
same `ScenarioOperationRecord` log, and validates final mounted tree
convergence and namespace invariants. Each mounted-chaos round records planned
and started operations before launching short-lived operation helper processes,
then records accepted or rejected results after the helpers return. If a helper
does not return within the round timeout, the scenario force-terminates pending
helpers, fails with their operation labels, and still attempts mounted cleanup.
The `mounted-open-handle-convergence` scenario keeps file handles open while
another mounted client unlinks or renames the same namespace entry, then
validates that flush/fsync/release do not resurrect deleted paths and that the
renamed destination remains visible after convergence. It records the mounted
open-handle lifecycle in `operations.ndjson`, writes
`mounted-open-handle-summary.json`, and uses only public mounted-tree
validation.
The `mounted-publication-boundary-crash-matrix` scenario force-kills and
remounts a writer at flush, fsync, release/close, and graceful shutdown-drain
publication boundaries. It validates same-root local durability after remount,
opens the explicit public convergence gate by refreshing observer peers,
records peer checksums at that gate, then waits for mounted-tree convergence.
It records shared-schema `operations.ndjson`, `publication-boundary-summary.json`,
and `validation.json`.
Early peer visibility is allowed in this scenario once the writer data is
locally durable and the product has completed its normal background publication;
the gate exists to make the observer refresh explicit instead of relying on a
timing window.
The
`mounted-fault-chaos-convergence`
scenario is the next product fault-chaos layer: it mixes a mounted conflict
burst with PostgreSQL outage, client force-kill/remount, RustFS/S3 outage, and a
late cold observer client with a fresh local root/cache. It validates final
mounted tree convergence across warm clients, the restarted client, and the cold
observer. The `mounted-lww-reconnect-chaos-convergence` scenario adds an
oracle-backed LWW layer: it forces a stale writer offline, lets online peers
publish ordered writes, remounts the stale writer so it writes last, crash/restarts
that writer before final convergence, then mounts a cold observer. It fails when
clients converge to the same tree but `lww-chaos/winner.txt` does not match the
expected last-writer checksum.

Hosted Linux real mounted E2E is available as a manual RWX workflow:

```powershell
rwx run .rwx/e2e.yml --target e2e-smoke --wait --open
```

The first RWX target runs `basic-multi-client-smoke` only. It requires RWX
Docker support, `/dev/fuse`, `fusermount3`, and readable FUSE configuration on
the hosted Linux task. It does not add a GitHub Actions workflow, PR trigger, or
schedule. RWX uploads `target/e2e-runs/<run-id>/` as the `e2e-run` artifact so
`report.json`, `preflight.json`, Docker diagnostics, scenario artifacts, and
client logs remain available on failure.

Focused runner skeleton check:

```powershell
cargo test -p studiofs-e2e --test runner_contract
cargo test -p studiofs-e2e --test compose_infra
cargo test -p studiofs-e2e --test mounted_tree_validator
cargo test -p studiofs-e2e scenario::tests::mounted_chaos_records_seeded_operations_and_validates_convergence -- --exact --nocapture
cargo test -p studiofs-e2e --test runner_contract mounted_fault_chaos_disabled_run_writes_reproducible_artifacts -- --exact --nocapture
cargo test -p studiofs-e2e --test runner_contract mounted_lww_reconnect_chaos_disabled_run_writes_reproducible_artifacts -- --exact --nocapture
cargo test -p studiofs-e2e scenario::tests::lww_reconnect_oracle_detects_converged_wrong_winner -- --exact --nocapture
cargo test -p studiofs-e2e scenario::tests::publication_boundary_crash_matrix_covers_boundaries_and_blocks_premature_peer_visibility -- --exact --nocapture
cargo test -p studiofs-e2e namespace_collision_records_operations_and_validates_three_client_convergence -- --exact --nocapture
cargo test -p studiofs-e2e scenario::tests::crash_restart_recovery_runs_both_modes_reuses_client_root_and_converges -- --exact --nocapture
cargo test -p studiofs-e2e --test client_controller force_kill_then_remount_reuses_same_client_root_without_graceful_umount -- --exact --nocapture
cargo test -p studiofs-scenario --test collision -- --nocapture
cargo test -p studiofs-scenario --test crash_replay -- --nocapture
cargo test -p studiofs-scenario --test seed_corpus -- --nocapture
```

### Deterministic simulation corpus

The deterministic simulation corpus lives in `studiofs-scenario` and is wired
to RWX through `.rwx/simulation.yml`. It is pure Rust logical simulation: it
does not require Docker, PostgreSQL, RustFS, FUSE, WinFSP, mounted filesystems,
self-hosted runners, or wall-clock sleeps.
Deep corpus seeds should change the observable trace shape for collision,
crash/restart, metadata-outage, and object-store-outage groups; identical trace
counts across every deep seed usually mean the scenario has collapsed into a
single-path check.

Use this layer as the search front end, not as the final product verdict. The
current corpus exercises the deterministic model itself. A counterexample only
becomes a StudioFS product bug after it is replayed into a production-backed
layer:

1. Model search: run the `studiofs-scenario` corpus to find a failing
   interleaving and save the counterexample artifact.
2. Contract/Core replay: replay the portable `ScenarioOperationRecord` log into
   non-mounted production fixtures and validate the exported logical snapshot.
3. Mounted replay: when the operation is mount-observable, translate the same
   log into `studiofs-e2e` and validate mounted tree convergence plus scenario
   artifacts.
4. Regression promotion: keep the smallest reproducing layer as a focused test,
   and keep the original counterexample artifact replayable.

If a failing seed reproduces only in the model, fix the model, invariant, or
scenario definition first. If it reproduces in Contract/Core, treat it as a
production core bug. If Contract/Core passes but mounted replay fails, treat it
as a bridge, process lifecycle, or integration bug.

The first production-backed adapter lives on
`studiofs_vfs::contract_harness::ContractVfsFixture::replay_operation_records`.
It applies portable namespace and local file-content operations
(`create_file`, `create_directory`, `write`, `append`, `truncate`, `rename`,
`unlink`, `rmdir`, `read`, `stat`, `readdir`, `flush`, and `fsync`) through the
real VFS fixture and reports checkpoints, mounted-only operations, crash/restart
markers, and infrastructure outages as skipped records. Skipped records are not
coverage; use mounted E2E replay for those.

Mounted replay lives in `studiofs_e2e::mounted_replay`. It executes the same
`ScenarioOperationRecord` log against real mounted client paths, selecting the
target mount from `record.client` or the first client when no client is attached.
It applies mount-observable file operations, dispatches `client_crash` and
`client_restart` to the E2E client controller, dispatches PostgreSQL and RustFS
outage/recovery operations through compose service hooks, and executes
`mounted_only` records through an explicit driver hook for platform-specific
real-mount probes.

Local commands:

```powershell
cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus fast --artifact-root target\simulation-counterexamples\fast
cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus deep --artifact-root target\simulation-counterexamples\deep
cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus fast --scenario-group namespace-collisions --artifact-root target\simulation-counterexamples\namespace-collisions
cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus fast --scenario-group crash-restart-replay --seed 13 --artifact-root target\simulation-counterexamples\seed-13
```

Exploratory local seed sweep:

```powershell
$groups = @('namespace-collisions','crash-restart-replay','metadata-outage','object-store-outage')
foreach ($group in $groups) {
  foreach ($seed in 1..256) {
    cargo run -q -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus deep --scenario-group $group --seed $seed --artifact-root "target\simulation-counterexamples\explore-$group-$seed"
    if ($LASTEXITCODE -ne 0) { throw "simulation failed for group=$group seed=$seed" }
  }
}
```

Replay a saved counterexample by passing the artifact path to
`replay-counterexample`; do not use `run-corpus --seed` for exact replay:

```powershell
cargo run -p studiofs-scenario --bin studiofs-scenario -- replay-counterexample target\simulation-counterexamples\fast\namespace-collisions\seed-13-failure-0.json
```

RWX commands:

```powershell
rwx run .rwx/simulation.yml --target simulation-fast --wait --open
rwx run .rwx/simulation.yml --target simulation-deep --wait --open
```

On failure, the RWX run uploads the matching counterexample artifact folder:
`simulation-counterexamples-fast` for `simulation-fast` and
`simulation-counterexamples-deep` for `simulation-deep`. The CLI output prints
`artifact_path=<path>` for each failing seed; open the uploaded artifact with
that relative path and replay it with the artifact's `replay_command`. Replay
is local and fails if the saved scheduler trace, operation log, event trace,
violated invariant, or relevant snapshot no longer matches the rerun. Mismatch
errors include the first differing vector index or a saved/replayed length
difference to make artifact drift diagnosable.

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
