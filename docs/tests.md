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

### SFS-638 Shutdown and Upload-Idle Regressions

Run these focused regressions when changing shutdown ownership, shutdown error
replay, or the upload-idle notification protocol:

```bash
cargo test -p studiofs-cache wait_for_idle_wakes_after_last_completion -- --nocapture
cargo test -p studiofs-vfs concurrent_shutdown_callers_observe_first_error -- --nocapture
cargo test -p studiofs-vfs cancelled_shutdown_owner_can_be_retried -- --nocapture
cargo test -p studiofs-vfs shutdown_result_reports_local_payload_sync_failure -- --nocapture
cargo test -p studiofs-vfs shutdown_replay_preserves_typed_error_for_owner_and_waiter -- --nocapture
```

### SFS-642 PostgreSQL Lifecycle and Schema Regressions

Run these focused checks when changing PostgreSQL worker ownership, startup
schema validation, or foreground lock schema behavior:

```bash
cargo test -p studiofs-meta prepare_for_close_does_not_block_forever_on_slow_background_thread -- --nocapture
cargo test -p studiofs-meta prepare_for_close_serializes_lazy_worker_registration -- --nocapture
cargo test -p studiofs-meta shutdown_releases_worker_state_before_joining_workers -- --nocapture
cargo test -p studiofs-meta concurrent_shutdown_callers_wait_for_terminal_result -- --nocapture
cargo test -p studiofs-meta metadata_notify_start_skips_start_lock_when_already_registered -- --nocapture
cargo test -p studiofs-meta prepare_for_close_is_idempotent_when_no_workers_are_running -- --nocapture
cargo test -p studiofs-meta dropping_engine_signals_background_workers_without_waiting -- --nocapture
cargo test -p studiofs-meta postgres_startup_fallback_keeps_timeout_typed -- --nocapture
cargo test -p studiofs-meta test_pg_foreground_file_lock_does_not_repair_missing_schema -- --ignored --nocapture
cargo test -p studiofs-meta test_pg_lock_notify_listener_releases_engine_after_drop -- --ignored --nocapture
cargo test -p studiofs init_pg_schema_provisions_volume_encryption_metadata -- --nocapture
cargo test -p studiofs init_repairs_existing_schema_before_volume_guard -- --ignored --nocapture
```

### SFS-36 Snapshot Source Replay Regressions

These focused PostgreSQL tests require `STUDIOFS_TEST_PG_DSN` with a local
PostgreSQL instance:

```powershell
cargo test -p studiofs-meta capture_is_one_transaction_and_one_durable_job -- --ignored --nocapture
cargo test -p studiofs-meta reconstructs_exact_cut_and_excludes_later_events -- --ignored --nocapture
cargo test -p studiofs-meta captured_checkpoint_bytes_survive_serving_checkpoint_replacement -- --ignored --nocapture
cargo test -p studiofs-meta many_snapshots_do_not_grow_serving_checkpoint_or_event_payloads -- --ignored --nocapture
```

### SFS-651 Snapshot CLI contract

Run the parser and CLI-service checks for the canonical eight-form surface:

```powershell
cargo test -p studiofs --bin studiofs snapshot_parser -- --nocapture
cargo check -p studiofs -p studiofs-meta -p studiofs-vfs
```

The live resolver, PostgreSQL identity validation, Ready wait, policy CAS, and
artifact/materializer checks require configured PostgreSQL and object storage;
run those through the focused Snapshot v2 integration/E2E commands rather than
claiming them from parser-only evidence. Do not replace these commands with a
filter that reports `running 0 tests`.

### SFS-647 Secret Redaction and Error Category Regressions

Run these focused checks when changing persisted-config diagnostics, PostgreSQL
startup timeout classification, or metadata error mappings:

```powershell
cargo test -p studiofs-meta studio_pg_dsns_debug_omits_credentials_and_full_dsns -- --nocapture
cargo test -p studiofs-meta invalid_dsn_errors_omit_the_original_dsn -- --nocapture
cargo test -p studiofs-meta credential_bearing_format_debug_omits_all_secret_values -- --nocapture
cargo test -p studiofs-meta postgres_startup_fallback_keeps_timeout_typed -- --nocapture
cargo test -p studiofs-meta transport_unavailability_excludes_reachable_session_mismatch -- --nocapture
cargo test -p studiofs-vfs classify_vfs_error_preserves_repair_category -- --nocapture
cargo test -p studiofs-vfs classify_vfs_error_preserves_remote_unavailable_category -- --nocapture
cargo test -p studiofs-vfs classify_vfs_error_preserves_timeout_category -- --nocapture
cargo test -p studiofs-bridge explicit_metadata_categories_map_to_distinct_errno_codes -- --nocapture
```

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
- S3-compatible endpoints honor the operator-selected `https://` or `http://`
  scheme;
- plain HTTP S3 is accepted for explicit `http://` endpoint URLs and for bare
  loopback host-port development/test endpoints;
- malformed, credential-bearing, `memory://`, unsupported-scheme, and bare
  non-loopback S3 endpoints are rejected before init, persisted mount, or
  endpoint override;
- `sslmode=disable` cleartext choices must be logged at info level for
  non-local hosts.

Local loopback examples remain available as development/test examples.

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
| CLI surface enforcement | `cargo test -p studiofs -- --nocapture` |
| Data integration suite | `cargo test -p studiofs-data --test integration_test -- --ignored --nocapture` |
| Destroy command integration suite | `cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1` |
| Dump/Load recovery integration suite | `cargo test -p studiofs --test dump_load_test -- --ignored --nocapture --test-threads=1` |
| Linux libfuse3 mount | `cargo test -p studiofs-linux-fuse mounted_ -- --ignored --nocapture --test-threads=1` |
| macFUSE read/cache report fields | `cargo test -p studiofs-macfuse repeated_read_report_records_callback_distribution_and_cache_interpretation -- --nocapture` |
| macFUSE FSKit native mount sanity | `STUDIOFS_FSKIT_MOUNT_ROOT=/Volumes/studiofs/target/sfs-670-validation cargo test -p studiofs-macfuse --test fskit_mount_sanity fskit_ -- --ignored --nocapture --test-threads=1` |
| WinFSP bridge unit + smoke | `cargo test -p studiofs-winfsp -- --nocapture` and `$env:STUDIOFS_RUN_PLATFORM_BRIDGE_MOUNT_TESTS='1'; $env:STUDIOFS_WINFSP_TEST_MOUNT_POINT='Z:'; cargo test -p studiofs-winfsp --test mount_sanity -- --ignored --nocapture --test-threads=1` |

### Local-first VFS fast checks (no S3)

The old issue-specific prefix filter matched comments and helper names, not
test function names. Run these exact non-ignored local-first VFS checks instead:

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
| `winfsp_mount_unmount_smoke` | WinFSP | Mount startup and named-event shutdown (minimal lifecycle) |
| `winfsp_mount_write_read_unmount_smoke` | WinFSP | Full lifecycle: mount → write → read → readdir → delete → unmount |

The full-lifecycle tests exercise file I/O through the real OS filesystem
layer, not just session creation. Linux provides
`mounted_smoke_mounts_and_unmounts_cleanly`,
`mounted_regular_file_io_is_visible_across_reopen`, and
`mounted_namespace_mutations_are_visible_through_readdir`; they are ignored by
default because they require `/dev/fuse`, libfuse3, and `fusermount3`.

The WinFSP suite also contains post-upload/finalization cases. Those require a
reachable S3 backend and return early when the resolved test endpoint is not
available, which keeps the hosted Windows smoke job aligned with the documented
dummy-endpoint contract.

See [docs/smoke-test-strategy.md](smoke-test-strategy.md) for the
complete smoke-test strategy, CI integration, and self-hosted runner contract.

### macOS FSKit native mount sanity

The native FSKit scenarios live in
`crates/studiofs-macfuse/tests/fskit_mount_sanity.rs`. They are compiled only
when macOS and `MFMount.framework` are available and are marked `#[ignore]`
because they require a real host mount. Run them explicitly with a mount root
on an internal volume:

```bash
STUDIOFS_FSKIT_MOUNT_ROOT=/Volumes/studiofs/target/sfs-670-validation \
cargo test -p studiofs-macfuse --test fskit_mount_sanity fskit_ -- \
  --ignored --nocapture --test-threads=1
```

The three scenarios cover FSKit read/unmount, pre-cancelled mount cleanup, and
local hardlink unlink semantics. Each test uses a PID-scoped mountpoint and
bounded worker cleanup; a worker that remains alive after the cleanup window
fails the isolated native test process instead of being detached. The live
channel invalidator check is a non-mounted library test and proves delivery of
inode, attribute, and entry notification frames separately from native mount
evidence.

Do not use a removable `/Volumes/Storage` root as native evidence: macOS TCC
can deny `fskitd` there even when the Rust process can access the checkout.
These checks validate the macOS bridge only; they do not validate Windows or
the remaining foreign create/update/rename/unlink parity matrix.

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

Hosted Linux real mounted E2E is operator-run once the target host has
`/dev/fuse`, `fusermount3`, libfuse3, Docker, PostgreSQL, and RustFS available.
See the SFS-610 Linux Platform Release Gate in
[docs/local-first-validation-matrix.md](local-first-validation-matrix.md#sfs-610-linux-platform-release-gate).

```powershell
rwx run .rwx/e2e.yml --target e2e-smoke --wait --open
rwx run .rwx/e2e.yml --target e2e-full --wait --open
```

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

`studiofs-winfsp` contains unit tests that are only meaningful on Windows.
macOS bridge tests belong to `studiofs-macfuse`. Linux libfuse3 bridge tests
belong to `studiofs-linux-fuse`.

### Rules for writing platform-gated unit tests

1. **Use the narrowest `cfg` guard that reflects the real constraint.**
   - Tests that validate WinFSP behavior (drive letters, timeout parsing):
     no extra `cfg` needed — the crate root already has `#![cfg(windows)]`

2. **Do not use `#[ignore]` to skip platform unit tests.**
   Platform-gated tests that would be useless on other OSes must use `cfg`
   guards, not `#[ignore]`. CI command details per platform:

   - Windows: `cargo test -p studiofs-winfsp --lib` (unit tests only).

   Only the relevant runner runs each step; the other platforms skip it entirely
   via `cfg` guards in the test code.

3. **Non-mount platform tests may live in `tests/` or `src/`; mount-requiring
   tests belong in ignored integration tests.**
   Non-mount platform tests that are too heavy or too coupled to other crates
   for a `src/` unit-test module are allowed in `tests/` — CI picks them up via
   `--tests`. Tests that require `WinFSP` kernel interaction must be
   placed in dedicated smoke-test files (e.g. `mount_sanity.rs`) and marked
   `#[ignore]` to keep them out of the standard `cargo test -p <crate>` run.

4. **Environment-variable mutation in tests (WinFSP only).**
   Tests inside `studiofs-winfsp` that call `std::env::set_var`/`remove_var`
   must hold `ENV_LOCK` (defined in the `tests` module) for the duration of the
   env mutation. In CI the crate is run with `--test-threads=1`, which
   serialises the entire binary and makes isolation explicit. If you need to
   add new env-mutating tests, ensure they acquire `ENV_LOCK` and consider
   whether `temp_env` or `serial_test` would be cleaner than a manual mutex.

### Temporary test artifacts

Test-owned temporary files should live under `target/`. Prefer
`studiofs_test_support::workspace_target_tempdir(...)` because it returns a
RAII `TempDir` and removes artifacts when the test exits.

Use `workspace_target_temp_path(...)` only when the code under test must create
the artifact root itself. The test that receives that path is responsible for
explicit cleanup or for keeping a separate cleanup guard alive until the
artifact is no longer needed.

### CI coverage per platform

| Crate | CI step | Runner |
|-------|---------|--------|
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
  - `test_publish_regular_file_overlay_to_base_commits_sparse_tail_logical_size`
  - `background_epoch_finalizer_keeps_non_publish_ready_epoch_local_after_restart` *(requires S3 env)*
  - `sfs312_flush_materializes_local_slice_boundary_without_remote_visibility`
  - `sfs312_fsync_persists_local_state_without_remote_visibility`
  - `sfs312_last_handle_release_drains_local_durability_boundary`
  - `strict_flush_interrupts_deferred_materialization_linger`
  - `strict_flush_interrupts_clean_deferred_materialization_linger`
  - `strict_flush_interrupts_materializer_before_file_session_gate`
  - `pending_sparse_slice_commit_preserves_tail_during_newer_inflight_metadata`
  - `pending_sparse_slice_commit_rebases_when_same_epoch_ready_publication_wins`
  - `pending_sparse_slice_commit_preserves_larger_same_epoch_ready_eof`
  - `vfs_upload_meta_reattaches_materialized_sparse_upload_over_uncovered_tail`
  - `pending_slice_commit_returns_retryable_error_after_metadata_retry_exhaustion`

- Recovery and replay
  - `capture_is_one_transaction_and_one_durable_job`
  - `reconstructs_exact_cut_and_excludes_later_events`
  - `captured_checkpoint_bytes_survive_serving_checkpoint_replacement`
  - `many_snapshots_do_not_grow_serving_checkpoint_or_event_payloads`
  - `test_pg_reopen_after_publish_before_local_persist_recovers_without_discover`
  - `test_pg_reopen_after_unpublished_local_create_flushes_without_discover`
  - `sfs315_reopen_consumes_persisted_inode_and_object_headroom_without_pg`
  - `sfs316_reachability_reports_active_base_and_overlay`
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
  - `regular_file_overlay_batch_commits_two_inodes_in_one_transaction`
  - `regular_file_overlay_batch_rolls_back_on_middle_failure`

- Metadata authority and headroom
  - `sfs315_failed_object_batch_preserves_smaller_inode_headroom`
  - `sfs315_large_object_batch_replaces_undersized_prefetched_window`
  - `sfs623_read_only_allocator_blocks_new_mutations_but_keeps_reads_open`

- Payload-object identifier layout
  - `composite_object_keys_use_low_bits_for_shards_and_preserve_ids`

Run a single focused regression:

Each command below is copy-paste ready and includes the full crate and test
binary prefix where applicable.

```bash
cargo test -p studiofs-meta test_contract_core_slice_graph_resolves_logical_byte_view_after_reopen -- --nocapture
cargo test -p studiofs-meta test_resolve_chunk_slice_spans_overlay_over_base_with_hole -- --nocapture
cargo test -p studiofs-meta sfs314_partial_publish_preserves_base_tail_and_committed_size -- --nocapture
cargo test -p studiofs-vfs --test vfs_test offline_s3_does_not_break_local_read_after_write -- --nocapture
cargo test -p studiofs-vfs --test vfs_test background_epoch_finalizer_keeps_non_publish_ready_epoch_local_after_restart -- --ignored --nocapture --test-threads=1
cargo test -p studiofs-vfs pending_sparse_slice_commit_preserves_tail_during_newer_inflight_metadata -- --nocapture
cargo test -p studiofs-vfs pending_sparse_slice_commit_rebases_when_same_epoch_ready_publication_wins -- --nocapture
cargo test -p studiofs-vfs pending_sparse_slice_commit_preserves_larger_same_epoch_ready_eof -- --nocapture
cargo test -p studiofs-vfs strict_flush_interrupts_deferred_materialization_linger -- --nocapture
cargo test -p studiofs-vfs strict_flush_interrupts_clean_deferred_materialization_linger -- --nocapture
cargo test -p studiofs-vfs strict_flush_interrupts_materializer_before_file_session_gate -- --nocapture
cargo test -p studiofs-vfs vfs_upload_meta_reattaches_materialized_sparse_upload_over_uncovered_tail -- --nocapture
cargo test -p studiofs-vfs pending_slice_commit_returns_retryable_error_after_metadata_retry_exhaustion -- --nocapture
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
require host platform support (WinFSP driver/runtime). Enable them only when
validating platform bridge changes:

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
starts automatically.

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
