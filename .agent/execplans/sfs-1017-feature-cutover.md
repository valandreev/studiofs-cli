# Explicit Operator-Controlled Feature Cutover Workflow (SFS-1017)

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` from the repository root.

## Purpose / Big Picture

StudioFS 0.2.5+ separates schema expansion from breaking feature activation using per-volume protocol policies (`volume_protocol`), policy epochs, client session capability leases (`client_sessions`), publication fencing (`BUMP_METADATA_VERSION` + `event_log` trigger), and maintenance fencing (`fence_maintenance`). Previously, `volume_protocol` only had low-level `check_update` and `update_volume_protocol` primitives with no operator-facing cutover workflow, no pre-activation readiness checks across sessions, checkpoints, dirty local work, replication lag, and snapshot/GC state, and no database-wide safety check before destructive `Contract` transitions.

After this change, an operator can drive a per-volume Expand–Migrate–Activate–Contract (and pre-Contract Deactivate rollback) cutover explicitly through `studiofs upgrade <name> --feature <cap> --phase <phase>`, starting with `--dry-run` to inspect eligible and blocking client sessions, transition epochs, rollback boundaries, and readiness blockers before committing any change. Supported 0.2.5+ clients remain writable during Expand and Migrate; Activate atomically enforces required capabilities and fences stale writers; expired sessions with unknown offline local work block cutover unless explicitly accepted with `--accept-offline-clients`; and Contract is a separate explicit operation that verifies every volume in the PostgreSQL database before retiring a feature or running `MigrationPhase::Contract` DDL.

## Progress

- [x] (2026-11-27 14:53Z) Checked out `feature/sfs-1017-17-add-explicit` from `origin/main` and read `AGENTS.md`, `crates/studiofs-meta/AGENTS.md`, `.agent/PLANS.md`, `docs/persisted-format-compatibility.md`, and `docs/adr/0018-version-identities-and-capability-model.md`.
- [x] (2026-11-27 14:55Z) Audited existing `volume_protocol.rs`, `pg_engine/volume_protocol.rs`, `pg_engine/client_sessions.rs`, `pg_engine/migrations.rs`, and `crates/studiofs/src/cli/upgrade.rs`.
- [x] (2026-11-27 14:57Z) Added unit tests and backend-neutral cutover types, proposal builder, and readiness evaluator in `crates/studiofs-meta/src/volume_protocol.rs`.
- [x] (2026-11-27 15:02Z) Implemented PostgreSQL cutover preflight, atomic execution, database-wide Contract checks, and live PG integration tests in `crates/studiofs-meta/src/pg_engine/volume_protocol.rs`, `crates/studiofs-meta/src/pg_engine/migrations.rs`, and `crates/studiofs-meta/src/pg_engine/tests/volume_protocol.rs`.
- [x] (2026-11-27 14:58Z) Extended `studiofs upgrade` in `crates/studiofs/src/cli/upgrade.rs` with `--feature`, `--phase`, `--role`, `--write-requirement`, `--accept-offline-clients`, and `--expected-epoch`.
- [x] (2026-11-27 14:58Z) Updated `docs/persisted-format-compatibility.md` and `docs/local-first-validation-matrix.md`.
- [x] (2026-11-27 15:03Z) Verified unit tests, live PostgreSQL integration tests, and clippy.

## Surprises & Discoveries

- Observation: `git fetch origin` in the worktree required `BypassSandbox: true` because the shared bare/parent `.git/worktrees/.../FETCH_HEAD` path is outside the sandbox root.
  Evidence: `error: cannot open 'E:/dev/studiofs-cli/.git/worktrees/t3-051b2375-35f33931d405c611/FETCH_HEAD': Permission denied`.
- Observation: `FeatureState::may_become` in `crates/studiofs-meta/src/volume_protocol.rs` already encodes the exact lifecycle: `Available -> Compatible | Retired`, `Compatible -> Available | Required | Retired`, `Required -> Compatible`, and `Retired -> Retired`. A `Required` feature cannot jump directly to `Retired` without first being deactivated to `Compatible` (or `Available`), and `Retired` is terminal.

## Decision Log

- Decision: Extend `studiofs upgrade` (`crates/studiofs/src/cli/upgrade.rs`) with `--feature` / `--phase` flags rather than adding a new top-level CLI subcommand.
  Rationale: The issue instructions explicitly prefer extending the existing `upgrade`/compat command rather than adding a new top-level command. Adding optional flags to `UpgradeArgs` preserves 100% compatibility with existing `studiofs upgrade <name> [--dry-run] [--repair-schema]` invocations.
  Date/Author: 2026-11-27 / Gemini 4 Hydrogen
- Decision: Keep cutover state evaluation and proposal construction pure and backend-neutral in `crates/studiofs-meta/src/volume_protocol.rs`, and keep PostgreSQL locking, session/checkpoint/snapshot/multi-volume queries, and `MigrationPhase::Contract` gating in `crates/studiofs-meta/src/pg_engine/`.
  Rationale: Preserves the `domain <- ports <- application <- adapters` dependency direction and allows unit testing all state-machine and blocker rules without PostgreSQL.
  Date/Author: 2026-11-27 / Gemini 4 Hydrogen

## Outcomes & Retrospective

Implemented the explicit Expand–Migrate–Activate–Contract feature cutover workflow across `studiofs-meta` (backend-neutral domain model and PostgreSQL adapter) and `studiofs` (`studiofs upgrade --feature ... --phase ...`), with unit tests and live PostgreSQL integration tests validating dry-run blockers, atomic activation/publication fencing, database-wide Contract checks, and terminal `Retired` boundaries. End-to-end live-mount CLI cutover tests remain environment-gated on mounted FUSE/WinFSP fixtures.

## Context and Orientation

The feature cutover system spans two crates:

1. `crates/studiofs-meta`:
   - `src/volume_protocol.rs`: defines `FeatureState` (`Available`, `Compatible`, `Required`, `Retired`), `ProtocolRequirements` (`read`, `write`, `maintain`, `checkpoint`, `features`), `VolumeProtocol` (`epoch`, `write_requirement`, `requirements`), `ProtocolRejection`, and `check_update`.
   - `src/pg_engine/volume_protocol.rs`: defines `read_volume_protocol`, `update_volume_protocol` (locks `volumes FOR KEY SHARE` and `metadata_state FOR UPDATE`, checks `expected_epoch`, calls `check_update`, checks `pg_try_advisory_xact_lock` and maintenance leases when `maintain` changes, updates `volume_protocol`, `metadata_state.write_requirement`, and `volume_protocol_history`), `fence_maintenance`, and `refused_publication`.
   - `src/pg_engine/client_sessions.rs`: defines `read_client_sessions(&client, volume)` returning `Vec<ClientSession>` with `active: bool`, `capabilities: SessionCapabilities`, `policy_epoch`, and `fencing_token`.
   - `src/pg_engine/migrations.rs`: defines `MigrationPhase` (`Additive`, `Backfill`, `Contract`), `MIGRATIONS`, `migrate_registered_schema`, and `registered_schema_status`.
   - `src/pg_engine/sql.rs`: defines `BUMP_METADATA_VERSION`, which atomically checks `metadata_format_version = $2 AND write_requirement <= $4 AND publication_epoch::TEXT = current_setting('studiofs.policy_epoch', true)`.
   - `src/pg_engine/schema.rs`: defines `EVENT_PUBLICATION_FENCE_SQL` triggers that synchronize `volume_protocol.policy_epoch` into `metadata_state.publication_epoch` and fence `event_log` inserts.

2. `crates/studiofs`:
   - `src/cli/upgrade.rs`: defines `UpgradeArgs` for `studiofs upgrade <name>`. Currently runs `migrate_registered_schema`, shared format 5/6->7 adoption, and local redb/payload-index upgrade.

## Plan of Work

First, add the pure cutover model to `crates/studiofs-meta/src/volume_protocol.rs`:
- `CutoverPhase` (`Expand`, `Migrate`, `Activate`, `Deactivate`, `Contract`), `CapabilityRole` (`Read`, `Write`, `Maintain`), and `OfflineClientPolicy` (`BlockUnknownWork`, `AcceptFence`).
- `CutoverRequest` specifying the target `feature` capability string, `phase`, target `roles`, optional `write_requirement`, optional `checkpoint`, and `offline_client_policy`.
- `CutoverLocalState` carrying optional local cache observations (`unpublished_events`, `dirty_payloads`, `local_version`, `remote_head_version`) so the CLI can feed local redb/payload-index state into the readiness check.
- `CutoverBlocker` and `SessionInspection` structs/enums covering every required check: active unsupported sessions, expired sessions with unknown offline local work (blocked unless `AcceptFence`), dirty unpublished local operations, missing compatible checkpoint, active snapshot/GC work, replication catch-up lag, database-wide volume usage for `Contract`, invalid state transitions (e.g., deactivating after `Retired`), and actor capability support.
- `CutoverDryRunReport` summarizing current/next epoch, current/proposed `VolumeProtocol`, reversibility and rollback boundary text, `SessionInspection`, and `blockers`.
- `build_proposed_protocol` and `evaluate_cutover_readiness` pure functions.

Second, add PostgreSQL orchestration to `crates/studiofs-meta/src/pg_engine/volume_protocol.rs` and `crates/studiofs-meta/src/pg_engine/migrations.rs`:
- `inspect_cutover_dry_run` reads `volume_protocol`, `metadata_state`, `serving_checkpoint_encodings`, `read_client_sessions`, active `snapshot_creation_jobs` / `snapshot_cleanup_leases` / `snapshot_payload_upload_objects` delete-pending leases, and (for `Contract`) all volumes in `volumes` + `metadata_state` + `volume_protocol` + pending `MigrationPhase::Contract` migrations.
- `execute_feature_cutover` runs inside a `ReadCommitted` transaction, acquires `volumes FOR KEY SHARE` and `metadata_state FOR UPDATE` (same lock order as `update_volume_protocol` and `BUMP_METADATA_VERSION`), re-evaluates readiness under the lock, rejects if any blocker remains, and commits via `update_volume_protocol`.
- In `migrations.rs`, add `contract_migration_blockers` / `check_contract_ready` so `MigrationPhase::Contract` steps verify every volume in the database has a `volume_protocol` row and no active/incompatible blocker before executing DDL.

Third, extend `crates/studiofs/src/cli/upgrade.rs`:
- Add `--feature`, `--phase`, `--role`, `--write-requirement`, `--accept-offline-clients`, and `--expected-epoch` to `UpgradeArgs`.
- When `--feature` is provided, `--phase` is required; `--dry-run` prints the full `CutoverDryRunReport` (epochs, rollback boundary, eligible/blocking/expired sessions, and blockers) without mutating anything; without `--dry-run`, gathers local state (if local redb/cache exists) and calls `execute_feature_cutover`, printing the committed epoch and transition summary.

Fourth, update `docs/persisted-format-compatibility.md` (and `docs/local-first-validation-matrix.md` if applicable) to document the SFS-1017 explicit cutover workflow and CLI usage.

## Concrete Steps

All commands run from the worktree root `C:\Users\valen\.t3\worktrees\studiofs-cli\t3-051b2375-35f33931d405c611`.

1. Write red tests in `crates/studiofs-meta/src/volume_protocol.rs` and `crates/studiofs-meta/src/pg_engine/tests/volume_protocol.rs`.
2. Run focused unit tests: `cargo test -p studiofs-meta volume_protocol::tests`.
3. Run focused PG integration tests (loading `.env` if present): `cargo test -p studiofs-meta pg_engine::tests::volume_protocol -- --ignored`.
4. Run CLI tests: `cargo test -p studiofs cli::upgrade::tests`.
5. Run clippy, fmt, and diff check: `cargo clippy -p studiofs-meta -p studiofs --all-targets -- -D warnings`, `cargo fmt --check`, `git diff --check`.

## Validation and Acceptance

- Dry-run reports blockers when an active session lacks the required feature, when an expired session exists with unknown offline work (unless `--accept-offline-clients` / `OfflineClientPolicy::AcceptFence`), when dirty local operations or replication lag exist, when a compatible checkpoint is missing, or when snapshot/GC work is active.
- Concurrent activation and publication are serialized by `metadata_state FOR UPDATE`: a stale writer's `BUMP_METADATA_VERSION` is fenced after activation commits.
- Database-wide `Contract` is refused when any volume in the database still has the feature in `Compatible` or `Required` state or lacks a `volume_protocol` row.
- Deactivation (`Required -> Compatible`) is refused after `Retired` (`InvalidTransition`).

## Idempotence and Recovery

- `--dry-run` is read-only and never mutates PostgreSQL or local redb.
- Pre-activation (`Expand` / `Migrate`) and `Activate` (before `Contract`) are reversible via `Deactivate` when the feature itself is reversible (`FeatureState::Required -> Compatible`, `Compatible -> Available`).
- `Contract` (`Retired`) is terminal and irreversible; it is gated on explicit `--phase contract` and database-wide verification.

## Artifacts and Notes

Will be updated with test transcripts upon completion.

## Interfaces and Dependencies

In `crates/studiofs-meta/src/volume_protocol.rs`:
- `pub enum CutoverPhase { Expand, Migrate, Activate, Deactivate, Contract }`
- `pub enum CapabilityRole { Read, Write, Maintain }`
- `pub enum OfflineClientPolicy { BlockUnknownWork, AcceptFence }`
- `pub struct CutoverRequest`
- `pub struct CutoverLocalState`
- `pub enum CutoverBlocker`
- `pub struct SessionInspection`
- `pub struct CutoverDryRunReport`
- `pub fn build_proposed_protocol(...) -> Result<VolumeProtocol, ProtocolRejection>`
- `pub fn evaluate_cutover_readiness(...) -> CutoverDryRunReport`

In `crates/studiofs-meta/src/pg_engine/volume_protocol.rs`:
- `pub async fn inspect_cutover_dry_run<C: GenericClient + Sync>(...) -> MetaResult<CutoverDryRunReport>`
- `pub async fn execute_feature_cutover(...) -> MetaResult<CutoverDryRunReport>`
