# SFS-40 Daily Automatic Snapshot Policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one connected StudioFS client automatically create and materialize at most one automatic Snapshot v2 job per eligible daily policy slot, with deterministic timezone/DST behavior and fenced evaluator takeover.

**Architecture:** Keep policy values and slot calculation in `studiofs-meta`, expose evaluator lease and due-slot operations through the existing backend-agnostic Snapshot v2 ports, implement PostgreSQL as the authority, and add a tracked `SnapshotPolicyEvaluator` beside the existing `SnapshotWorker` in `studiofs-vfs`.

**Tech Stack:** Rust 2024, Tokio, `chrono`, `chrono-tz`, PostgreSQL via `tokio-postgres`, `thiserror`, `CancellationToken`, existing Snapshot v2 ports and VFS startup-worker lifecycle.

## Global Constraints

- Follow `/Volumes/Storage/dev/studiofs/.agent/PLANS.md`; keep this plan living and update progress, discoveries, decisions, and outcomes.
- Preserve dependency direction: domain <- ports <- application <- adapters/runtime/API.
- Write each test before production code, run it failing, then add the minimum implementation.
- PostgreSQL is authoritative for policy generations, slots, evaluator leases, catalog rows, jobs, and fencing.
- Use UTC when policy timezone is omitted; persist IANA timezone names; gap slots are skipped and ambiguous slots choose the earlier UTC occurrence.
- A mounted client is the scheduler owner; no mounted client means no queued automatic snapshot is promised.
- Do not add public CLI commands in SFS-40; SFS-651 owns that surface.
- Keep backend errors typed and redacted; do not expose DSNs, credentials, or raw driver text.

## Progress update — 2026-08-14

- Domain slot resolution, evaluator port values, PostgreSQL schema/lease/slot transaction, Ready completion marker, and mounted evaluator wiring are implemented in the working tree.
- Passing focused evidence: 19 `studiofs-meta` snapshot domain tests, the schema contract test, 7 policy tests with 6 correctly ignored for live PostgreSQL, 24 `studiofs-vfs` snapshot-worker tests, workspace `RUSTFLAGS='-D warnings' cargo check`, `cargo fmt --all -- --check`, and `git diff --check`.
- Remaining external evidence: live PostgreSQL transaction/takeover tests (`STUDIOFS_TEST_PG_DSN` is not configured), platform/E2E validation, and commit creation (`.git` is read-only in this checkout).

---

### Task 1: Add policy slot domain values and deterministic clock behavior

**Files:**
- Modify: `crates/studiofs-meta/src/snapshot.rs`
- Modify: `crates/studiofs-meta/src/snapshot_ports.rs`
- Test: `crates/studiofs-meta/src/snapshot.rs`
- Test: `crates/studiofs-meta/src/pg_engine/tests/snapshot_policy.rs`

**Interfaces:**
- Consumes: existing `SnapshotDailyPolicy`, `SnapshotPolicyRecord`, `SnapshotVolumeContext`, `SnapshotPortError`, and `SnapshotPortFuture`.
- Produces: validated policy evaluator identifiers, evaluator lease/evaluation values, and pure due-slot helpers used by the PostgreSQL adapter and VFS evaluator.

- [ ] **Step 1: Write the failing pure tests.** Add tests named `policy_default_timezone_is_utc`, `dst_gap_records_skipped_slot`, `dst_ambiguity_uses_earlier_utc_occurrence`, and `catch_up_claims_only_latest_eligible_slot`. Use `America/New_York` at `2026-03-08 02:30` for the skipped gap and `2026-11-01 01:30` for the earlier `05:30Z` occurrence. Assert that the second `06:30Z` occurrence cannot produce another slot.

- [ ] **Step 2: Run the exact tests and verify RED.**

  Run from `/Volumes/Storage/dev/studiofs`:

      cargo test -p studiofs-meta policy_default_timezone_is_utc -- --nocapture
      cargo test -p studiofs-meta dst_gap_records_skipped_slot -- --nocapture
      cargo test -p studiofs-meta dst_ambiguity_uses_earlier_utc_occurrence -- --nocapture
      cargo test -p studiofs-meta catch_up_claims_only_latest_eligible_slot -- --nocapture

  Expected result before implementation: the new symbols or expected slot results are missing and the focused tests fail for the intended reason.

- [ ] **Step 3: Add the minimum domain implementation.** Add the validated evaluator ID, evaluator lease, skipped-slot/evaluation outcome, and pure local-date/`HH:MM` slot resolver. Resolve a nonexistent local time to `SkippedDstGap` without shifting it; resolve an ambiguous time to the earlier UTC occurrence; return only the latest eligible slot after downtime. Keep host local timezone out of the calculation.

- [ ] **Step 4: Run the focused domain tests and verify GREEN.**

      cargo test -p studiofs-meta policy_default_timezone_is_utc -- --nocapture
      cargo test -p studiofs-meta dst_gap_records_skipped_slot -- --nocapture
      cargo test -p studiofs-meta dst_ambiguity_uses_earlier_utc_occurrence -- --nocapture
      cargo test -p studiofs-meta catch_up_claims_only_latest_eligible_slot -- --nocapture

- [ ] **Step 5: Commit the domain slice.**

      git add crates/studiofs-meta/src/snapshot.rs crates/studiofs-meta/src/snapshot_ports.rs crates/studiofs-meta/src/pg_engine/tests/snapshot_policy.rs
      git commit -m "feat: model automatic snapshot policy slots"

### Task 2: Add PostgreSQL policy slots and evaluator lease authority

**Files:**
- Modify: `crates/studiofs-meta/src/pg_engine/schema.rs`
- Modify: `crates/studiofs-meta/src/pg_engine/snapshot_policy.rs`
- Modify: `crates/studiofs-meta/src/pg_engine/snapshot_control.rs`
- Modify: `crates/studiofs-meta/src/pg_engine/local_traits.rs`
- Modify: `crates/studiofs-meta/src/pg_engine/tests/snapshot_policy.rs`
- Test: `crates/studiofs-meta/src/pg_engine/tests/snapshot_control.rs`

**Interfaces:**
- Consumes: Task 1 policy values and existing generic `GenericClient` transaction helpers.
- Produces: PostgreSQL implementations of `claim_evaluator_lease`, `renew_evaluator_lease`, `evaluate_due_slot`, and `release_evaluator_lease`.

- [ ] **Step 1: Write the failing PostgreSQL contract tests.** Add ignored tests named `slot_claim_and_job_insert_are_one_transaction`, `policy_set_is_observed_by_mounted_evaluator_after_commit`, and `evaluator_lease_takeover_after_force_kill`; also extend policy tests for one active generation, cancellation/re-enable, invalid zone, retention validation, and automatic origin. The transaction test must force an error after slot claim and before catalog/job insertion, then assert that a later attempt can claim the same slot.

- [ ] **Step 2: Run one exact ignored test and verify RED.**

      cargo test -p studiofs-meta slot_claim_and_job_insert_are_one_transaction -- --ignored --nocapture --test-threads=1

  Expected result before implementation: the evaluator lease/slot schema or port method is unavailable, or the transaction rollback assertion fails. If the configured PostgreSQL DSN is unavailable, retain the ignored test and record the infrastructure gap; continue with non-live contract tests.

- [ ] **Step 3: Add idempotent schema.** Extend the canonical schema with policy generation/slot state and the per-volume evaluator lease/fencing state. Add uniqueness for generation plus local date and local time, store `SkippedDstGap`, and add indexes for active evaluator leases and due slots. Keep manual catalog rows outside policy rotation.

- [ ] **Step 4: Implement evaluator lease CAS.** Lock the per-volume evaluator row, increment fencing on takeover, renew only for matching owner/fence, release only for matching owner/fence, and map stale or unavailable backend results to existing redacted Snapshot port errors.

- [ ] **Step 5: Implement atomic due-slot evaluation.** In one transaction lock the active policy generation, resolve the latest eligible slot, insert the unique slot or follow the existing slot, insert/reuse the automatic Creating catalog row and queued creation job, and commit. A rollback must remove every part of the claim. Update policy completion state only from the Ready publication path so transient materialization retries keep the same slot/job.

- [ ] **Step 6: Run the focused PostgreSQL and schema tests.**

      cargo test -p studiofs-meta snapshot_policy -- --ignored --nocapture --test-threads=1
      cargo test -p studiofs-meta snapshot_schema_v2_replaces_legacy_snapshot_schema -- --nocapture

- [ ] **Step 7: Commit the PostgreSQL slice.**

      git add crates/studiofs-meta/src/pg_engine/schema.rs crates/studiofs-meta/src/pg_engine/snapshot_policy.rs crates/studiofs-meta/src/pg_engine/snapshot_control.rs crates/studiofs-meta/src/pg_engine/local_traits.rs crates/studiofs-meta/src/pg_engine/tests/snapshot_policy.rs crates/studiofs-meta/src/pg_engine/tests/snapshot_control.rs
      git commit -m "feat: claim automatic snapshot policy slots transactionally"

### Task 3: Add the mounted SnapshotPolicyEvaluator lifecycle

**Files:**
- Modify: `crates/studiofs-vfs/src/snapshot_worker.rs`
- Modify: `crates/studiofs-vfs/src/vfs/bootstrap.rs`
- Modify: `crates/studiofs-vfs/src/vfs.rs`
- Modify: `crates/studiofs-vfs/src/runtime.rs`
- Test: `crates/studiofs-vfs/src/snapshot_worker.rs`
- Test: `crates/studiofs-vfs/src/vfs/tests.rs`

**Interfaces:**
- Consumes: Task 2 `SnapshotPolicyStore`, existing `SnapshotClock`, verified `SnapshotVolumeContext`, shutdown token, and `track_background_task`.
- Produces: `SnapshotPolicyEvaluator::run` and exactly one tracked evaluator for each mounted verified volume.

- [ ] **Step 1: Write the failing VFS tests.** Add `policy_set_is_observed_by_mounted_evaluator_after_commit` and `evaluator_cancellation_releases_or_expires_lease`. Use an injected clock and a fake policy store with timed state checkpoints; assert bounded observation after policy commit, no claim after cancellation, and release only when the last lease state is certain.

- [ ] **Step 2: Run the exact VFS tests and verify RED.**

      cargo test -p studiofs-vfs policy_set_is_observed_by_mounted_evaluator_after_commit -- --nocapture
      cargo test -p studiofs-vfs evaluator_cancellation_releases_or_expires_lease -- --nocapture

  Expected result before implementation: `SnapshotPolicyEvaluator` and its lifecycle seam do not exist.

- [ ] **Step 3: Implement the evaluator loop.** Add bounded lease claim/renew logic, call `evaluate_due_slot`, and select cancellation, lease-renew deadline, returned next due instant, or 30-second fallback poll. Stop before new due-slot work when renewal is uncertain. Return typed redacted evaluator errors and never detach owned tasks.

- [ ] **Step 4: Wire startup lifecycle.** Build the evaluator from the verified volume context in `bootstrap`, add `snapshot_policy_store` to the runtime seam, and start it from `spawn_startup_workers` with the existing background shutdown token and task tracking. Preserve the current SnapshotWorker behavior and startup ordering.

- [ ] **Step 5: Run the focused VFS tests and compile checks.**

      cargo test -p studiofs-vfs snapshot_worker::tests --lib -- --nocapture
      cargo test -p studiofs-vfs snapshot_worker_supervisor_restarts_after_worker_error -- --nocapture
      RUSTFLAGS='-D warnings' cargo check -p studiofs-meta -p studiofs-vfs -p studiofs

- [ ] **Step 6: Commit the evaluator slice.**

      git add crates/studiofs-vfs/src/snapshot_worker.rs crates/studiofs-vfs/src/vfs/bootstrap.rs crates/studiofs-vfs/src/vfs.rs crates/studiofs-vfs/src/runtime.rs crates/studiofs-vfs/src/vfs/tests.rs
      git commit -m "feat: run automatic snapshot policy evaluator on mounts"

### Task 4: Final verification and evidence

**Files:**
- Modify: `.agent/execplans/SFS-650.md`
- Modify: `docs/superpowers/specs/2026-08-14-sfs-40-daily-automatic-snapshot-policy-design.md`
- Modify: `docs/superpowers/plans/2026-08-14-sfs-40-daily-automatic-snapshot-policy.md`

- [ ] **Step 1: Run the complete focused validation.**

      cargo test -p studiofs-meta snapshot_policy -- --nocapture --test-threads=1
      cargo test -p studiofs-vfs snapshot_worker::tests --lib -- --nocapture
      cargo fmt --all -- --check
      RUSTFLAGS='-D warnings' cargo check --workspace
      git diff --check

- [ ] **Step 2: Inspect the final diff and status.** Confirm no public CLI changes, no raw backend error text, no fixed-sleep synchronization, and no unrelated files.

- [ ] **Step 3: Record evidence.** Update this plan and `.agent/execplans/SFS-650.md` with actual test counts, PostgreSQL/platform gaps, and the final commit IDs. Update SFS-40 in Linear in English with the implementation summary and verification evidence.

- [ ] **Step 4: Commit documentation evidence.**

      git add .agent/execplans/SFS-650.md docs/superpowers/specs/2026-08-14-sfs-40-daily-automatic-snapshot-policy-design.md docs/superpowers/plans/2026-08-14-sfs-40-daily-automatic-snapshot-policy.md
      git commit -m "docs: record SFS-40 automatic snapshot policy plan"
