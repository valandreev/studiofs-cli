# Verify remote synchronization before showing the tray flag

This ExecPlan follows `.agent/PLANS.md` and is a living document.

## Purpose / Big Picture

The blue/yellow tray flag must mean this mount's accepted changes have reached remote storage and metadata publication. A quiet user interface or empty best-effort queue is insufficient. Unknown, stale or failed diagnostics must never advertise safe shutdown. The signal is an observation, not a barrier against later application writes; unmount retains its existing shutdown barriers.

## Progress

- [x] (2026-10-18) Read SFS-866, recorded checkout E:/dev/studiofs-cli, matching feature branch and clean initial worktree; confirmed unsafe metadata threshold.
- [x] (2026-10-18) Trace metadata acknowledgement; expose exclusive journal end and durable cursor in one redb transaction with backend forwarding.
- [x] (2026-10-18) Finish VFS payload ownership/handoff proof, including owned upload/finalization/retry leases; four focused VFS tests passed and the delayed-retry shutdown ownership test passed.
- [x] (2026-10-18) Implement diagnostics transport, tray mapping and freshness/heartbeat tests; 22 tray tests passed after the confirmed red regression.
- [x] (2026-10-18) Six focused metadata tests passed for queue-independent acknowledgement, peer scope, local-only retirement, empty base, reopen and forwarding.
- [x] (2026-10-18) Build product and runner, then pass the registered two-client Windows WinFSP acceptance with PostgreSQL/S3 outages and retained evidence; cleanup passed.
- [x] (2026-10-18) Run focused Rust validation, scoped Clippy, formatting and whitespace checks; inspect the implementation and independent handoff review.
- [ ] Commit/push and update Linear with exact evidence and limitations.

## Surprises & Discoveries

The existing tray explicitly treats 1 through 9 pending metadata mutations as idle. The upload idle helper observes the queue, scheduler and dirty ingress but does not itself certify publication. Graph coverage reports metadata-changed files, so direct source reads supplement graph discovery.

The focused red regression ran one test and failed as expected: activity_mode_is_active_for_small_metadata_backlog returned Idle instead of Active. Docker is available (29.8.2) under native/elevated execution; sandbox pipe access alone is denied.

## Decision Log

Use one VFS-owned completion summary rather than teaching the tray every upload implementation detail. Missing summary fields deserialize to unknown. Preserve blue/yellow animation and the 300 ms polling interval. Do not add remote database or S3 calls to tray ticks. Scope metadata acknowledgement to local outbound work, not unrelated peer activity.

Metadata evidence uses the exclusive local event-journal end and the durable self-acknowledgement cursor, with pending/in-flight work as additional blockers. Inbound peer versions cannot certify that cursor. Tray observations older than two seconds or from the future cannot certify completion; unchanged monitor updates carry heartbeats so a stuck monitor also expires the flag.

The initial empty local journal uses exclusive end 1 with an uninitialized stored cursor 0; normalize the empty base to 1 using the same persisted pruning-frontier semantics as replay. Read both positions in one redb transaction. Require a completed Live lifecycle for a safe observation, without comparing against a moving global peer head.

Additional handoff review found that epoch finalization removes its queue ticket before creating the publication journal event, and delayed retries temporarily own no queue ticket. Retain an owned publication activity lease from before dequeue through finalization and any delayed retry until requeue. Upload workers likewise retain a generation lease through queue removal and publication scheduling. The tray renders known blocking reasons red even when overlapping work makes the completion proof unknown.

## Outcomes & Retrospective

The shared proof, diagnostics transport, tray behavior and user documentation are implemented. All 37 focused tests, Windows mounted acceptance, scoped Clippy, formatting and whitespace checks passed. Git/Linear delivery is the remaining step.

The Windows mounted acceptance now passed with rebuilt product/runner binaries. During the S3/PG pause the authoritative state was unknown with known publication/dirty/metadata blockers (mapped red by the tray). With S3 restored and PG still paused, it was syncing with metadata acknowledgement pending and no upload blocker. After PG recovery, peer content hashes matched, namespace convergence passed and repeated observations were safe. macOS/Linux native integration and full suites were not run.

## Context and Orientation

`crates/studiofs/src/tray.rs` polls mount-local diagnostics and maps snapshots to icon colors. `crates/studiofs/src/diagnostics/types.rs` defines the JSON snapshot; `live_source.rs` gathers it from `StudioVfs`. `crates/studiofs-vfs/src/vfs.rs` owns dirty writer, upload and deferred materialization state. Its metrics module defines portable observations. `crates/studiofs-meta/src/pg_engine/replication_api.rs` exposes best-effort queue counts; replication and durable journal transitions need a stronger local completion observation. `crates/studiofs-e2e` runs registered mounted scenarios using two clients and controlled service faults.

## Plan of Work

First trace accepted writes through dirty memory, persisted staging, upload scheduling and file-version publication, and trace metadata queue claims through remote commit and durable acknowledgement. Add the smallest coherent VFS summary that can prove completion, returning unknown where the proof is unavailable. Wire the summary into runtime diagnostics. Replace the tray threshold with explicit summary state and reject old/future/absent observations. Test queue and freshness edges, document the signal's limits, and exercise remote publication held and resumed with two mounted clients.

## Concrete Steps

From E:/dev/studiofs-cli use focused commands only. Start with `cargo test -p studiofs --bin studiofs tray::supported::tests:: -- --nocapture`. Run module-filtered metadata/VFS tests for the new summary and the registered E2E scenario selected during implementation. Record actual commands and results below as they become available. Finish with formatting, scoped Clippy and `git diff --check`.

## Validation and Acceptance

Counts 1, 2, 9 and one in-flight metadata mutation must be non-safe. Dirty memory, deferred/persisted payload, retries, upload scheduling and pending file-version publication must independently block safe. Missing/stale/failed telemetry must be non-safe. Only verified completion produces the flag; unrelated peer updates must not block it. Mounted evidence must retain artifacts and demonstrate peer-visible content after remote publication resumes. Report macOS/Linux integration as untested unless actually exercised.

## Idempotence and Recovery

Keep temporary evidence under target/. Preserve unrelated work and existing commits. Run only scenario-owned mounts and service fixtures. Do not change foreground durability or shutdown barriers.

## Artifacts and Notes

Initial branch: feature/sfs-866-fix-tray-safe-to-shutdown-state-show-ukrainian-flag-only. Linear issue SFS-866 was already In Progress. Initial git status was clean.

Validated commands: `cargo test -p studiofs --bin studiofs tray::supported::tests:: -- --nocapture` (22 passed); `cargo test -p studiofs-meta --lib ack_snapshot -- --nocapture` (4 passed); the same metadata command filtered by `empty_local_redb_uses_first_event_sequence` and `peer_remote_head_progress_does_not_ack_local_journal` (1 passed each). Native execution was used after Windows sandbox filesystem/pipe restrictions; these are focused checks, not full suites.

After the final handoff changes, `cargo test -p studiofs-vfs -p studiofs-e2e --lib sync_safety -- --nocapture` passed four VFS and two runner tests. The just-built VFS test executable also passed `shutdown_owns_epoch_retry_before_reopening_local_stores`, proving delayed publication retry is visible and still joined/cancelled by shutdown.

Final tray/transport verification passed 23 tray tests and `legacy_runtime_snapshot_cannot_certify_sync_completion` (one test). `cargo build -p studiofs -p studiofs-e2e` succeeded. The rebuilt runner listed the new acceptance at position 29, preserving existing numbered scenarios. Command: `target/debug/studiofs-e2e.exe run mounted-sync-safety-two-client-acceptance --studiofs-bin E:/dev/studiofs-cli/target/debug/studiofs.exe --platform windows-winfsp --run-id sfs866-sync-safety-20261018-a --retain always --timeout 600s`. Result: Passed; report at `target/e2e-runs/sfs866-sync-safety-20261018-a/report.json`, detailed snapshot/hash evidence at `artifacts/scenarios/mounted-sync-safety-two-client-acceptance/sync-safety-summary.json`. Cleanup reported passed with no errors. `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs -p studiofs-e2e --all-targets -- -D warnings`, `cargo fmt --all -- --check` and `git diff --check` passed. The only lint correction collapsed the new scenario's nested cleanup condition without changing behavior.

## Interfaces and Dependencies

Expose a serializable VFS sync safety summary with safe, syncing and unknown states, reason information and an observation generation. Diagnostics forwards it without reconstructing a proof from counters. Tray consumes the summary and transport freshness. No additional dependency is planned.
