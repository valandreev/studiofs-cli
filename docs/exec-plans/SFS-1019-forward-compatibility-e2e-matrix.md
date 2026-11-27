# SFS-1019: Supported-Release Forward-Compatibility Integration and Mounted E2E Matrix

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This document is maintained in accordance with `.agent/PLANS.md`.

## Purpose / Big Picture

StudioFS 0.2.5 introduces a split version-identity and capability-negotiation model (`docs/persisted-format-compatibility.md`, ADR 0018) so that supported releases can coexist concurrently on the same PostgreSQL metadata database and S3 object store (RustFS) while unsupported format or capability changes fail closed without corrupting shared or local state.

After this change, operators and CI can run a single registered `studiofs-e2e` scenario (`mounted-forward-compat-matrix`) — or its repeatable wrapper script `tools/test_successor/run_compat_matrix.py` — that executes real compiled binaries side by side against isolated Docker Compose PostgreSQL + RustFS services with separate local `redb` databases per client:

1. The 0.2.5 baseline binary (built from the current tree) and the compiled, versioned `test-successor` binary (SFS-1131, verified via `tools/test_successor/build.py --require-complete`), exercising two-way mounted read/write/rename/delete, multi-chunk/slice replay, xattrs (native on Unix, protocol-level on Windows), dual serving checkpoint refresh (format 4 + format 5), event log pruning, cross-binary Snapshot v2 creation and read-only mount, PostgreSQL outage reconnect with pending local writes, session expiry and SFS-1017 feature cutover (`expand` / `migrate` / activation refusal / reversible `deactivate` / `activate --accept-offline-clients`), fail-closed refusal after incompatible activation, and irreversible local redb revision 7 upgrade refusal.
2. The published `v0.2.4` binary (pinned SHA-256, same release-download mechanism as SFS-1002), exercising format 6 -> 7 `studiofs upgrade` adoption by 0.2.5 and verifying that `v0.2.4` fails closed afterward without corrupting PostgreSQL, S3, or losing its local redb state.

The run publishes a deterministic JSON matrix artifact (`artifacts/scenarios/mounted-forward-compat-matrix/forward-compat-matrix.json`) recording binary SHA-256 digests, format/PostgreSQL schema versions, event/checkpoint protocol versions, declared successor changes, and per-cell / per-operation pass/fail/skipped outcomes, with an explicit `pairs` array ready for future released pairs such as 0.2.5/0.2.7 (SFS-1053).

## Progress

- [x] (2026-11-27 17:30Z) Audited SFS-1131 `test-successor` (`crates/studiofs-meta/src/test_successor.rs`, `tools/test_successor/build.py`), SFS-1002 fixture capture (`tools/fixtures/capture-published-volume-fixtures.ps1`), SFS-1017 cutover CLI (`crates/studiofs/src/cli/upgrade.rs`), and `crates/studiofs-e2e` runner/controller architecture. Confirmed all 4 required change kinds (`postgres_migration`, `event_kind`, `checkpoint_format`, `local_format`) and both rollback classes (`reversible`, `irreversible`) are already declared in `test-successor`.
- [x] (2026-11-27 17:34Z) Authored initial living ExecPlan in `docs/exec-plans/SFS-1019-forward-compatibility-e2e-matrix.md`.
- [x] (2026-11-27 17:35Z) Extended `studiofs-e2e::client` (`ClientControllerConfig::client_bin_overrides`, `MultiClientController::set_client_bin_override`, `run_cli_allow_failure`, `client_by_name`).
- [x] (2026-11-27 17:39Z) Implemented `mounted-forward-compat-matrix` scenario in `crates/studiofs-e2e/src/scenario/workloads/forward_compat_matrix.rs`, registered it in `registry.rs`, and added it to `FULL_E2E_OPT_IN_SCENARIOS` in `runner_contract.rs`.
- [x] (2026-11-27 17:40Z) Added `tools/test_successor/run_compat_matrix.py` repeatable entrypoint and updated `docs/tests.md` and `docs/persisted-format-compatibility.md`.
- [x] (2026-11-27 18:01Z) Verified `cargo fmt`, `cargo clippy -p studiofs-e2e --all-targets -- -D warnings`, `forward_compat_matrix` unit tests (3 passed), `registry::tests` (3 passed), `runner_contract` (40 passed), `client_controller` (30 passed), `tools/test_successor/build.py --require-complete` (5 declared changes verified), and live Windows/WinFSP mounted matrix run `sfs-1019-windows-v3` (8/8 cells passed, `passed: true`).
- [ ] Commit, push branch, and open ready PR against `main`.

## Surprises & Discoveries

- Observation: `crates/studiofs-meta/src/test_successor.rs` already implements and declares all 5 changes required by `tools/test_successor/build.py --require-complete`: `postgres-core-schema-2` (`postgres_migration`, irreversible), `event-set-user-xattr-v1` (`event_kind`, irreversible), `serving-checkpoint-envelope-v5` (`checkpoint_format`, reversible), `local-redb-prepare-revision-7` (`local_format`, reversible), and `local-redb-revision-7` (`local_format`, irreversible). No product-code hack or new successor mechanism is needed.
  Evidence: `crates/studiofs-meta/src/test_successor.rs:75-125`.
- Observation: Running the `v0.2.4 -> 0.2.5` adoption cell after the compiled `test-successor` had already upgraded `core` to version 2 caused 0.2.5 `studiofs upgrade` to refuse with `upgrade required: PostgreSQL schema component core is at version 2, newer than 1`. Running the `v0.2.4 -> 0.2.5` pair first on the fresh Compose database exercises the natural upgrade sequence (`v0.2.4` format 6 -> `0.2.5` format 7 `core=1` -> `test-successor` `core=2`) cleanly without needing a second database.
  Evidence: Live Windows E2E run `sfs-1019-windows-v2` vs `sfs-1019-windows-v3` (`8/8 cells passed`).
- Observation: Under StudioFS's serving-first local-first contract, an already-discovered 0.2.5 client mounting after a read-incompatible activation serves its local cache and transitions into `PgSyncLifecycle::Degraded` (stopping replay of post-activation events and blocking publication while preserving its local redb), while fresh `discover` fails closed immediately and `studiofs compatibility` reports `upgrade_required`.
  Evidence: `crates/studiofs-meta/src/pg_engine.rs:1574-1583` and live Windows E2E run `sfs-1019-windows-v3` (`cell_06_unsupported_activation_fail_closed`).
- Observation: `studiofs-winfsp` does not expose POSIX `setxattr`/`getxattr` syscalls on Windows mounts, whereas Linux/macOS FUSE mounts do.
  Evidence: `rg` over `crates/studiofs-winfsp/src/` shows only WinFSP `extended_attribute_timeout_ms` config, no EA callbacks. Native mounted `user.*` xattr convergence runs on `cfg!(unix)` and is explicitly recorded as platform-gated (`skipped`) on Windows, while `event.set_user_xattr.v1` capability activation/refusal runs on all platforms.

## Decision Log

- Decision: Extend `MultiClientController` with `client_bin_overrides: BTreeMap<String, PathBuf>` rather than building a parallel process controller.
  Rationale: Reuses all existing sandbox layout, per-client `STUDIOFS_CLIENT_ROOT`, isolated redb/cache paths, Windows drive-letter assignment, Job Object cleanup, and log capture invariants.
  Date/Author: 2026-11-27 / Implementation Agent
- Decision: Execute the `v0.2.4 -> 0.2.5` fenced-client pair first on the Compose PostgreSQL database before the `test-successor` applies its irreversible `core=2` migration.
  Rationale: Matches real-world release order (`v0.2.4` -> `0.2.5` -> successor), keeps database-wide schema components consistent, and requires no private Docker containers.
  Date/Author: 2026-11-27 / Implementation Agent
- Decision: Register `mounted-forward-compat-matrix` in `FULL_E2E_OPT_IN_SCENARIOS` in `crates/studiofs-e2e/tests/runner_contract.rs`.
  Rationale: Matches `published-volume-fixture-capture` and `janitor-live-benchmark`, which require extra binaries or opt-in invocation rather than the single-binary `linux-e2e.yml` full target.
  Date/Author: 2026-11-27 / Implementation Agent
- Decision: Structure `forward-compat-matrix.json` around a `pairs` array where each entry identifies a release pair (`0.2.5-baseline-vs-compiled-test-successor`, `0.2.4-published-vs-0.2.5-baseline-fenced`).
  Rationale: Provides a clean, append-only extension point for future released pairs such as `0.2.5/0.2.7` (SFS-1053) without changing the schema.
  Date/Author: 2026-11-27 / Implementation Agent

## Outcomes & Retrospective

All 8 matrix cells passed on Windows/WinFSP in run `sfs-1019-windows-v3`, producing `target/e2e-runs/sfs-1019-windows-v3/artifacts/scenarios/mounted-forward-compat-matrix/forward-compat-matrix.json` (`total_cells: 8, passed: 8, failed: 0, skipped: 0, passed: true`).

Environment-gated items:
- Native mounted POSIX `user.*` xattr syscalls (`mounted_xattr_two_way`) are wired for `cfg!(unix)` (Linux FUSE / macOS FUSE) and recorded as `skipped` (platform-gated) on Windows/WinFSP.
- Linux FUSE and macOS FUSE mounted runs were not executed on this Windows host; the scenario and `tools/test_successor/run_compat_matrix.py` are cross-platform and ready for those hosts.

## Context and Orientation

Key files involved in this change:

- `crates/studiofs-meta/src/test_successor.rs`: Declares the compiled test successor changes (`CHANGES`) and `SET_USER_XATTR_CAPABILITY`.
- `tools/test_successor/build.py`: Builds the baseline and `test-successor` binaries, verifies `version-identities` reports, and writes `target/test-successor/manifest.json`.
- `tools/fixtures/capture-published-volume-fixtures.ps1`: Pinned SHA-256 and `gh release download` mechanism for published `v0.2.4` binaries.
- `crates/studiofs-e2e/src/client.rs`: `ClientControllerConfig` and `MultiClientController` managing isolated E2E clients, mount points, and CLI invocations.
- `crates/studiofs-e2e/src/scenario/registry.rs`: Scenario registry (`available_scenarios` and `resolve_scenario`).
- `crates/studiofs-e2e/src/scenario/workloads.rs`: Workload module exports and shared helpers (`wait_for_convergence`, `compose_control_from_manifest`, etc.).
- `crates/studiofs-e2e/src/scenario/workloads/forward_compat_matrix.rs` (new): Implements `MountedForwardCompatMatrix` scenario and matrix artifact serialization/validation.
- `crates/studiofs-e2e/tests/runner_contract.rs`: Runner contract tests including `FULL_E2E_OPT_IN_SCENARIOS`.
- `tools/test_successor/run_compat_matrix.py` (new): Repeatable CLI/CI wrapper that runs `build.py --require-complete`, optionally obtains verified `v0.2.4`, builds `studiofs-e2e`, and runs `mounted-forward-compat-matrix`.
- `docs/tests.md` and `docs/persisted-format-compatibility.md`: Document how to run the matrix and interpret `forward-compat-matrix.json`.

## Plan of Work

1. **Extend `crates/studiofs-e2e/src/client.rs`**:
   - Add `pub client_bin_overrides: BTreeMap<String, PathBuf>` to `ClientControllerConfig` and initialize it to `BTreeMap::new()` in all constructors.
   - Add `set_client_bin_override(&mut self, client: &str, bin: PathBuf)` and `run_cli_allow_failure(&self, client: &str, action: &str, args: Vec<String>)` and `mount_command_for(&self, client: &str)` to `MultiClientController`.
   - Use `self.config.client_bin_overrides.get(client.name.as_str()).unwrap_or(&self.config.studiofs_bin)` in `MultiClientController::command`.
   - Add focused unit test verifying per-client binary override routing.

2. **Implement `crates/studiofs-e2e/src/scenario/workloads/forward_compat_matrix.rs`**:
   - Define `MOUNTED_FORWARD_COMPAT_MATRIX = "mounted-forward-compat-matrix"` and `MountedForwardCompatMatrix` implementing `Scenario`.
   - Resolve baseline binary (`config.studiofs_bin`), successor binary (`STUDIOFS_E2E_SUCCESSOR_BIN` or `STUDIOFS_E2E_SUCCESSOR_MANIFEST` / `target/test-successor/manifest.json`), and optional `v0.2.4` binary (`STUDIOFS_E2E_V024_BIN` or `target/sfs-1002-published/v0.2.4/bin/studiofs(.exe)`).
   - Verify both baseline and successor `studiofs internal version-identities` outputs and record SHA-256 digests.
   - Execute the 8 matrix cells in sequence with bounded observable-state polling (no fixed sleeps), guaranteed teardown/unmount on error, and zero secret/DSN leakage:
     - Cell 1: Additive PG migration (`core=2`) + baseline DDL refusal + concurrent baseline/successor two-way create/read/write/rename/delete + multi-chunk slice overwrite/replay + xattrs (native on Unix, platform-gated note on Windows).
     - Cell 2: Dual serving checkpoint refresh (format 4 + format 5 in `serving_checkpoint_encodings`) + event log pruning (`event_log_prune_floor > 0`) + cold baseline & cold successor discover/mount convergence.
     - Cell 3: Cross-binary Snapshot v2 (`snapshot create` -> `Ready` -> post-cut write -> `smount` read-only verification -> unmount).
     - Cell 4: PostgreSQL outage reconnect with pending local baseline write -> recovery and convergence with successor.
     - Cell 5: Session expiry and SFS-1017 activation workflow (`expand`, `migrate`, active-session activation refusal, expired-session activation refusal without `--accept-offline-clients`, reversible `deactivate` rollback + baseline remount proof, re-`migrate` and `activate --role read --accept-offline-clients`).
     - Cell 6: Fail-closed baseline refusal after read-incompatible activation (`compatibility`, `mount`, fresh `discover`) while successor remains healthy and PG/S3/local redb stay uncorrupted.
     - Cell 7: Irreversible local redb revision 7 upgrade (`upgrade --local-only` by successor) -> successor mounts cleanly, baseline fails closed on revision-7 local redb without corrupting it, successor remounts cleanly.
     - Cell 8: Fenced published `v0.2.4` client in isolated PG database `sfs_v024_fenced`: `v0.2.4` `init` + mount + write + second `v0.2.4` client discover -> `0.2.5` `studiofs upgrade` (format 6 -> 7) + mount + read/write -> `v0.2.4` attempts mount/discover and fails closed without corrupting PG, S3, or its local redb.
   - Write `forward-compat-matrix.json` in `context.scenario_dir` and validate all cells in `Scenario::validate`.
   - Add unit tests for matrix schema validation, binary identity verification, and redaction.

3. **Wire registry, contract tests, runner script, and docs**:
   - Register `MountedForwardCompatMatrix` in `crates/studiofs-e2e/src/scenario/registry.rs` and export in `crates/studiofs-e2e/src/scenario/workloads.rs`.
   - Add `mounted-forward-compat-matrix` to `FULL_E2E_OPT_IN_SCENARIOS` and add a registration test in `crates/studiofs-e2e/tests/runner_contract.rs`.
   - Create `tools/test_successor/run_compat_matrix.py` to automate `build.py --require-complete`, `v0.2.4` acquisition/verification, `studiofs-e2e` build, and scenario execution.
   - Document the matrix in `docs/tests.md` and `docs/persisted-format-compatibility.md`.

## Concrete Steps

All commands run from the repository root:

    cargo test -p studiofs-e2e --lib client::tests::
    cargo test -p studiofs-e2e --lib forward_compat_matrix::tests::
    cargo test -p studiofs-e2e --test runner_contract
    cargo clippy -p studiofs-e2e --all-targets -- -D warnings
    python tools/test_successor/build.py --require-complete
    python tools/test_successor/run_compat_matrix.py --run-id sfs-1019-windows --retain always

## Validation and Acceptance

- `cargo test -p studiofs-e2e --test runner_contract` passes with nonzero tests, confirming scenario registration and opt-in allowlist.
- `cargo test -p studiofs-e2e --lib forward_compat_matrix::tests::` passes with nonzero tests.
- `python tools/test_successor/build.py --require-complete` exits 0 and writes `target/test-successor/manifest.json` with `verified: true`.
- `python tools/test_successor/run_compat_matrix.py --run-id sfs-1019-windows --retain always` exits 0 on Windows/WinFSP, producing `target/e2e-runs/sfs-1019-windows/artifacts/scenarios/mounted-forward-compat-matrix/forward-compat-matrix.json` with `passed: true`, all 8 cells passed (with Windows native xattr syscall explicitly noted as platform-gated), binary SHA-256s recorded, and zero secrets/DSNs present.

## Idempotence and Recovery

Every run uses an isolated `target/e2e-runs/<run-id>` sandbox, a dedicated Docker Compose project with dynamic ports, and a fresh S3 bucket and PostgreSQL database(s). Failed runs retain full diagnostics under `target/e2e-runs/<run-id>/` and clean up Docker containers and mount processes via `MultiClientController` and runner supervision. Re-running with a new `--run-id` is safe and idempotent.

## Artifacts and Notes

Will be updated with exact command transcripts and matrix artifact summary after execution.

## Interfaces and Dependencies

- `crates/studiofs-e2e/src/client.rs`:
  - `ClientControllerConfig::client_bin_overrides: BTreeMap<String, PathBuf>`
  - `MultiClientController::set_client_bin_override(&mut self, client: &str, bin: PathBuf) -> Result<(), ClientControllerError>`
  - `MultiClientController::run_cli_allow_failure(&self, client: &str, action: &str, args: Vec<String>) -> Result<ClientCommandOutput, ClientControllerError>`
- `crates/studiofs-e2e/src/scenario/workloads/forward_compat_matrix.rs`:
  - `pub const MOUNTED_FORWARD_COMPAT_MATRIX: &str = "mounted-forward-compat-matrix";`
  - `pub(in crate::scenario) struct MountedForwardCompatMatrix;`
- `tools/test_successor/run_compat_matrix.py`:
  - CLI wrapper around `tools/test_successor/build.py --require-complete`, `gh release download` (v0.2.4), and `studiofs-e2e run mounted-forward-compat-matrix`.
