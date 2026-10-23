# Make Linux hotspot churn converge in PostgreSQL order

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Concurrent append and truncate writers can publish different contents with the same content epoch (the local file generation number). Every client must serve the last complete publication in PostgreSQL batch order once local work is acknowledged. Mounted callbacks remain autonomous and use local redb metadata.

## Progress

- [x] (2026-10-13) Recorded repository `<repo-root>`, branch `bug/linux-bughunt-02`, and initial changes. Read root and metadata/VFS/E2E contracts.
- [x] Inspected retained failures `fix2-hot-1` and `fix3-hot-21` and traced foreign apply, self apply, pending restoration, and base reconciliation.
- [x] (2026-10-13) Replaced the unfaithful truncate regression. The complete later foreign publication regression fails with size 207 instead of 406; the self-ack regression already passes in both orders.
- [x] (2026-10-13) Restricted stale Ready filtering to earlier content epochs and removed all TMPDBG instrumentation, preserving the VFS eviction fix. Validation pending.
- [x] (2026-10-13) Final metadata/VFS crate tests passed serially outside sandbox: 2066 unit/integration tests and 3 doc tests. Check, Clippy, formatting, diff check and product/runner build passed.
- [x] (2026-10-13) Obtained 12 consecutive final rebuilt-binary scenario passes; user reduced acceptance to 10. Run11 was already underway at steering; run12 was scheduled while the loop was being stopped. Both finished normally.
- [x] (2026-10-13) Final diff reviewed and task-only fix commit created; memory snapshot edits and stash excluded.

## Surprises & Discoveries

The Codebase Memory CLI is installed but cannot create secure coordination under its read-only cache directory. Direct source inspection is the fallback. In `fix3-hot-21`, the 406-byte append is the later batch (21), after the 207-byte rewrite (20), yet A and C retain 207. The timestamp-based stale-ready filter can discard a later complete publication. B first applies the earlier foreign rewrite to 207 before replaying its own append. In `fix2-hot-1`, A also applies its own rewrite at v21 before receiving B's append at v22. The initial handoff's assumed batch order was incorrect. Client A's page logs establish the order, while B's pre-replay publication completion time only establishes local upload completion.

## Decision Log

- Decision: Test both batch orders through the page coordinator and pending restoration, plus a later complete foreign append carrying older timestamps, rather than forcing a protection set in the existing test.
  Rationale: The two retained runs exercise opposite orders; final serving state must follow batch order in both.
  Date/Author: 2026-10-13, Codex.
- Decision: Follow the user's crate-scoped validation request and AGENTS.md, overriding the Rust/TDD skills' workspace-wide validation suggestions.
  Rationale: Workspace-wide tests are explicitly prohibited for this task.
  Date/Author: 2026-10-13, Codex.

## Outcomes & Retrospective

The red regression confirms that stale Ready filtering discards a complete later same-epoch append. The final change preserves the VFS inode-removal confirmation, follows PG order for complete same-epoch publications, retires replay-free local completion ranges before restoring serving tables, and protects complete local Ready attrs from earlier pending epochs. All four new metadata regressions and VFS eviction coverage pass. Full crate verification passed outside sandbox (2066 unit/integration and 3 doc tests), check/Clippy/build passed, and 12 consecutive rebuilt-binary mounted scenarios converged with clean teardown. The user reduced acceptance to 10 after run11 had started; run12 also completed while stopping the loop. Earlier failures and red tests remain in target/hotspot-validation. No peer-wait behavior or runner timeout was changed. Memory snapshot edits and stash remain untouched.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/remote_apply.rs` applies ordered remote batches and acknowledges matching local event prefixes. `metadata_sync.rs` commits replay positions and then restores pending local events. `local_state.rs` tracks published and completed local journal ranges so those ranges no longer override remote state. `crates/studiofs-meta/src/redb_engine/replication.rs` updates serving attrs (file sizes and generations) and base metadata (remote-visible state). `crates/studiofs-vfs/src/vfs/workers.rs` contains the supplied eviction correction: an inode absent from a namespace traversal is only permanently marked deleted after an actual InodeNotFound result.

## Plan of Work

First replace `ready_foreign_truncate_overwrite_replaces_published_same_epoch_append` in metadata's `pg_engine/tests/remote_apply.rs` with actual projected append and rewrite event sequences, replayed in both orders. Assert literal final sizes and payload identities after acknowledgment and pending restoration. Run the regression before editing production. Use the failing boundary to remove incorrect timestamp/size ordering or repair local acknowledgment, keeping newer unpublished writer state protected. Remove every TMPDBG/sfsdbg block. Preserve the supplied VFS tests and correction.

## Concrete Steps

Work from `<repo-root>`. Run `cargo test -p studiofs-meta same_epoch --lib -- --nocapture` for initial regressions. Run `cargo test -p studiofs-meta`, `cargo test -p studiofs-vfs`, `cargo check -p studiofs-meta -p studiofs-vfs`, and `cargo clippy -p studiofs-meta -p studiofs-vfs --all-targets -- -D warnings`. Run `cargo fmt --all --check` and `git diff --check`. Build with `cargo build -p studiofs -p studiofs-e2e`. List registered scenarios with `target/debug/studiofs-e2e list`, then repeatedly run `target/debug/studiofs-e2e run --scenario mounted-hotspot-churn-convergence --studiofs-bin target/dbg-wrap/studiofs --run-id <unique-id> --retain on-failure --timeout 300s`. Keep validation command logs under target; stop and inspect any failure before continuing.

## Validation and Acceptance

The regression must fail for the observed ordering error before the correction and pass afterward. Both crate test commands must select nonzero tests and pass. At least 10 consecutive rebuilt-binary mounted scenario runs must succeed without changing peer-wait behavior. A final `fix:` commit includes code, regression tests, and this plan, excluding `.codebase-memory`.

## Idempotence and Recovery

Use fresh unique E2E run IDs to preserve failures. Do not reset the worktree, drop the stash, or stage unrelated files. Local tests require no secrets. Mounted runs use the existing runner's disposable services and cleanup.

## Artifacts and Notes

Retained evidence: `target/e2e-runs/fix3-hot-21/artifacts/scenarios/mounted-hotspot-churn-convergence/validation/diff.json` reports B=406 versus A=207. Client A's trace identifies batch 21 as B's complete 406-byte epoch-3 publication after batch 20. Client B's trace shows the earlier rewrite applied to 207 followed by its own 406-byte state.

## Interfaces and Dependencies

Reuse `DecodedReplayBatch`, `RemoteReplayApplyContext`, `RecentFlush`, existing test engines, local event projection, and regular-file publication APIs. No new dependencies or foreground remote operations are planned.

Revision: initial investigation and acceptance plan, 2026-10-13.

Revision: corrected the initial batch-order hypothesis using client A page logs, recorded the red regression and minimal fix, 2026-10-13.

Revision: recorded focused green verification, review, and sandbox-specific test rerun, 2026-10-13.

Revision: 2026-10-13. Crate tests passed serially outside sandbox (metadata: 852 unit + 3 integration; VFS: 1154 unit + 55 integration; doc tests: VFS 3). Check, Clippy and build passed. First rebuilt E2E series passed four runs then stopped on `fix4-hot-20261013-144331-05`: client B inode 1649267441669 regressed from published epoch 1/226 bytes to epoch 0/0 bytes during replay of batches 16–18. A/C retain epoch 1. This matches the handoff's separate `fix-hot-6` variant. Acceptance remains incomplete; inspect retained database copies and event replay before changing code. Temporary diagnostic test must be removed before final validation.

Revision: 2026-10-13. Diagnostic run `fix5-hot-debug-20261013-150000-04` catches the empty-state mechanism transiently on inode1649267441667: local epoch1 publication completes at15:02:08.367; pending replay at08.415 excludes range31..37 but repeats retained creation prefix28..31 (Uploading epoch1 size0, Ready epoch0 size0, AddEntry). Page11..13 then reports serving epoch0 size0 while base is epoch1 size233, with the old prefix still reserving overlay protection. Later append heals this run; slot5 has no later write in the failed run and stays empty. Replace the passing split-self-create experiment with a red PgMetaEngine regression retaining a creation gap before a published content range. The proposed root correction prevents pending replay from replacing a complete local Ready state with a Ready event from an earlier epoch. A separate base-matching exception experiment also failed, but is not the cause of the initial regression and was removed without changing that code.

Revision: 2026-10-13. User clarified intermediate divergence is expected; acceptance and regressions assert final convergence only. The final create-gap regression retains a provisional Uploading attr via the local engine before deferred create, then later publication; it failed after self apply and final pending restore with size0/epoch1/committed0/Uploading. The Uploaded-gap regression records actual upload-marker journal boundaries between published prefix19 and publication20, restores pending after self20, then applies foreign21 and restores again; it failed with237/epoch3/Updating instead of466/epoch2/Ready. The fix retires replay-free completion ranges before computing serving exclusions and filters below the newly advanced cursor, preserving rejected authority. Pending Ready replay additionally rejects an earlier epoch than a complete local Ready state. No intermediate-attr merge patch was added. All temporary diagnostics are removed; independent review found no actionable issues. Fresh full validation and rebuilt-binary acceptance remain required.

Revision: recorded final successful crate verification and started the acceptance series `fix6-hot-final-20261013-152957-01` onward, 2026-10-13.

Revision: 2026-10-13. User explicitly reduced mounted acceptance to 10 successes. The final series `fix6-hot-final-20261013-152957-01` through `-12` passed consecutively. Run11 was underway at steering, and run12 was scheduled during stop handling; both finished cleanly. All report and cleanup statuses were checked, and the loop is terminated with no active runner or containers. Task files committed as `fix: converge Linux hotspot churn after metadata replay`.

Revision: final diff is clean and reviewed; the authorized fix commit contains only the six Rust task files and this plan. Final validation logs and 12 successful mounted reports are retained under target.
