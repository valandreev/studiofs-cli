# SFS-753 review: recovery accounting and mutation safety

This living ExecPlan follows `.agent/PLANS.md`. Work starts at `179bfb4f` on PR #734 in `E:\dev\studiofs-cli`.

## Purpose / Big Picture

Failed or concurrent puts must not delete successfully indexed pending bytes. Restart cleanup must report every removed orphan, and failed intent creation must promptly reclaim uncommitted temporary bytes. Focused tests make these outcomes observable without services or a mounted filesystem.

## Progress

- [x] (2026-10-01 15:13Z) Verified the attached document as feedback against current code and traced the affected paths.
- [x] (2026-10-01 15:13Z) Observed red tests for omitted orphan counts and staged-file leaks; minimal fixes pass focused tests.
- [x] (2026-10-01 15:35Z) Reproduced indexed-file loss with publication gates disabled; ordered gates pass all single/batch retry combinations and allow unrelated IDs to progress.
- [x] (2026-10-01 15:35Z) Preserved typed I/O errors; barrier regression was red for batch indexed-payload deletion and now retains files/intent for Lightweight recovery.
- [x] (2026-10-01 15:35Z) 46 focused cache tests pass; the nine SFS-753 tests also pass under externally configured Full mode.
- [x] (2026-10-01 15:41Z) Completed check/Clippy/format/diff checks and independent production review; strengthened the acquisition probe and observed the final race test red/green again.
- [x] (2026-10-01 15:41Z) Prepared delivery commands and review disposition. Commit/push and hosted results are recorded in PR #734 and the local review response.
- [x] (2026-10-01 17:35Z) Verified eight follow-up threads. Eviction propagates index read failures and counts eligibility rechecks; rollback attempts every unique deletion barrier while retaining its first error and intent. Tests require a contended publication gate and measure both temp-file syncs. VFS classification has focused coverage; batch-only assertions and barrier counts are clarified.

## Surprises & Discoveries

The document's single-put existing-index shortcut is absent: the return skips evictable insertion under budget pressure. Restoring fetch_update would reproduce the prior hosted deprecation failure. On directory-barrier failure, the durable intent is recovery evidence; unconditional file and intent removal is unsafe for indexed payloads.

Evidence before the small fixes: `startup_reconcile_lightweight_resolves_incomplete_batch_payload_intent` reported zero instead of one removed orphan; `sfs753_failed_intent_write_removes_uncommitted_staged_payloads` found retained staged files. Public LocalPayloadBackend is shared between threads and permits same-ID/same-bytes retries. No lock covered payload rename, index commit, and index-aware rollback.

## Decision Log

Preserve payloads and intents after post-rename barrier failure, including batch writes, rather than copying unconditional batch deletion into single puts. Only authoritative index-aware rollback or startup may discard those outputs. Decision: 2026-10-01, root.

Use fixed ID mutation gates, acquired in stable order for batches, across publication, index commit, and rollback. A gate is a mutex that prevents two conflicting operations from changing the same ID concurrently. A fixed set bounds memory; different IDs that map to one gate may serialize. Keep budget eviction before acquiring publication gates to avoid waiting on a lock already held by the same thread. Recheck eviction/pruning predicates under their ID gate. Decision: 2026-10-01, root.

Map adapter FilePayloadStoreError::Io to the port error LocalPayloadBackendError::Io in runtime.rs. Preserve other variants as boxed errors. This retains ordinary I/O categories; StorageFull already survives source-chain inspection. Retain the v1 serialized phase header, remove its constant parameter, and document index-authoritative recovery. Decision: 2026-10-01, root.

## Outcomes & Retrospective

Cleanup/accounting, marker failure handling, batch barrier retention and publication synchronization have focused red/green evidence. 46 focused cache tests pass, as do check/Clippy/format/diff checks and 15 documented test-name checks. Fixed 64 gates cover lifecycle/pin changes as well as writes/deletions; eligibility is rechecked under the gate and removal clears pins before releasing it. Independent review found no production defect. A test evidence caveat was corrected with a probe at the actual gate acquisition; the final regression was red without guards and green after restoration. Delivery follows the commands below, with hosted outcomes recorded in PR #734 and the local response. Full workspace, service-backed, and native mounted acceptance are outside this request.

## Context and Orientation

`crates/studiofs-cache/src/payload_store.rs` stages bytes in temporary files, persists an intent (a small recovery marker naming temporary and final paths), then atomically renames payloads. `runtime.rs` publishes the corresponding redb index row and retires the marker. The index is the authority that distinguishes a committed payload from an orphan. `startup_recovery.rs` replays markers before ordinary cleanup. `local.rs` declares the shared backend and its immutable-ID rule: retries use identical bytes and changed content needs a fresh ID.

## Plan of Work

First retain every successfully renamed output on later store errors. Centralize staged-file cleanup in write_batch_payload_intent on failure and remove an incomplete intent temp after its file handle closes. Aggregate final-file deletions from intent replay into LocalPayloadStartupReconcileReport and include resolved-intent count in the startup warning.

Then add fixed mutation mutexes to LocalPayloadBackendRuntime and a private helper that takes ID values, maps to unique gate indices, sorts them, and acquires them in order. Hold these guards across single/batch payload commit, index upsert, and rollback; acquire them only after budget admission. Use the same gates for file/index removal, eviction, and pruning, re-reading current eligibility before deletion. Do not gate successful reads. Add test-only bounded rendezvous at the real rollback boundary and prove concurrent same-ID publication cannot lose a payload. Independently prove unrelated gate IDs can progress and batch lock order cannot invert.

Implement From<FilePayloadStoreError> for LocalPayloadBackendError in runtime.rs and use it where store errors cross the runtime boundary. Test a failed directory barrier after rename using a per-store test-only path hook; indexed pending bytes and the intent must remain, and restart must clean only unindexed outputs. Keep public API and the wire marker format unchanged.

## Concrete Steps

Run commands sequentially in E:\dev\studiofs-cli:

    cargo test -p studiofs-cache --lib sfs753_ -- --nocapture
    cargo test -p studiofs-cache --lib startup_reconcile_ -- --nocapture
    cargo test -p studiofs-cache --lib runtime::tests::local_backend_ -- --nocapture
    cargo test -p studiofs-cache --lib runtime::file_payload_store::tests::fast_mode_ -- --nocapture
    cargo test -p studiofs-cache --lib durable_mode_keeps_per_put_sync_barriers -- --nocapture
    cargo test -p studiofs-cache --lib local_payload_backend_satisfies_local_payload_backend_contract -- --nocapture
    cargo check -p studiofs-cache
    cargo clippy -p studiofs-cache --all-targets -- -D warnings
    cargo test -p studiofs-vfs --lib payload_store_io_conversion_preserves_bridge_error_kinds -- --nocapture
    cargo clippy -p studiofs-vfs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

After review, stage every intended task file, commit with a fix subject, and push the existing branch. Use gh with --repo github.com/valandreev/studiofs-cli to inspect the new Payload read regression run and update PR #734 with precise validation.

## Validation and Acceptance

Each test filter must run nonzero tests. Before synchronization, force a failed put to pause after observing an absent index row, publish the same bytes through another put, and observe missing payload after rollback. After the fix, conflicting publication waits until rollback completes and leaves both an index row and readable pending bytes. Use bounded channels/timeouts and retain/join worker threads; no sleeps are coordination proof.

Intent-write failure must leave no staged .tmp payload files and preserve prior payloads. Lightweight and Full reports must count each deleted final file once; repeat recovery must be clean. Post-rename directory-barrier failure must keep indexed bytes and a replayable marker. Final check/Clippy/format/diff commands must exit zero.

## Idempotence and Recovery

Fixtures live under target and own isolated stores. Test hooks are instance-local and excluded from production. Preserve all unrelated worktree changes. On any deletion or sync error keep the marker for startup. Never reset the checkout or change global environment in Rust tests. Full user-run command, if desired: cargo test --workspace --all-targets -- --nocapture.

## Artifacts and Notes

Keep the attached review unchanged. Record accepted and rejected findings in a local response under docs/code-reviews and in the PR description. That directory is intentionally ignored; do not force-add supplied review artifacts. The initial small regression output was:

    orphan_payload_files_removed: expected 1, actual 0
    failed intent write leaked staged payload bytes

## Interfaces and Dependencies

Use existing parking_lot Mutex, standard arrays/iterators/collections, and existing index/store APIs. Add no dependency or public configuration fields. Private mutation-gate ownership belongs to runtime.rs; payload_store.rs owns marker/file synchronization and cfg(test) directory-failure injection. Port error conversion is implemented in the adapter runtime, so dependency direction remains domain <- ports <- application <- adapters.

Revision note: 2026-10-01 15:13Z, created after confirming the shared same-ID rollback race; records small completed fixes and the bounded synchronization work still required.

Revision note: 2026-10-01 15:35Z, implemented the confirmed race and error-path fixes. A fresh read-only review found a pre-payload marker leak on intent-directory sync failure; the extended existing test reproduced it, and best-effort marker retirement now passes. Marker retention remains required after any payload rename or uncertain rollback deletion.

Revision note: 2026-10-01 15:41Z, final local checks pass. The test's pre-call thread signal was replaced by a requested-stripe try-lock probe immediately before blocking acquisition; independent review confirmed the evidence caveat is resolved. All returned store I/O errors, including sync_data, use the adapter conversion. External delivery evidence is maintained in the PR.

Revision note: 2026-10-01 17:35Z, follow-up comments have 47 focused passing cache/VFS tests and clean cache check plus cache/VFS Clippy. Red evidence: eviction silently returned 64 bytes instead of an index error; partial rollback performed zero instead of two directory barriers; the file-sync counter reported one instead of two. The simplified gate test fails explicitly with guards disabled and passes with guards restored. Live intent-directory deletion is outside the runtime contract; retain visible I/O failure rather than introducing per-put directory creation or claiming restored recovery evidence. Codebase Memory MCP returned Transport closed, so the follow-up was verified directly from source. Sixteen documented test names validate. Final commit/CI/thread outcomes are recorded in PR #734.
