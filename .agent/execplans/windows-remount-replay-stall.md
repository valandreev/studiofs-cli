# Fix Windows remount replay stalls after large imports

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

A Windows mount must remain readable after uploading a large repository and reopening its local metadata database. A fully drained mount should not recreate a publication backlog, and cooperative shutdown must release the metadata workers. The reported workload has roughly 60,000 files; its remount showed 39,124 pending metadata operations, followed by blocked diagnostics and a metadata-notify join timeout.

## Progress

- [x] (2026-10-10) Recorded root `<repo-root>`, branch `main`, and unrelated untracked `.claude/` directory; created a task branch.
- [x] Traced startup pending restoration, self replay, diagnostics backlog reads, and worker shutdown. Started isolated PostgreSQL using compose project `studiofs-remount-regression`.
- [x] Proved unnecessary journal rescanning (12 scanned rows instead of zero), then eliminated it while preserving pending aliases.
- [x] Copied the inactive user's local database into ignored `target/remount-investigation/`; offline inspection reproduced exactly 39,124 pending operations from 55,430 retained events (cursor 14,834; next event 70,264).
- [x] Reproduced a drained and fully caught-up split publication reopening with 190 pending operations. Added version-gated acknowledgment of the unsplit projected prefix; the same PostgreSQL regression now reopens with zero publication operations and retains an unpublished local file tail.
- [x] Verified rollback and the last-page acknowledgment boundary, plus 24 other unique focused unit tests. Metadata check, clippy, formatting and whitespace gates passed after fixing one needless borrow.
- [x] Rebuilt release binary and passed native WinFSP scenarios 21 and 8 with retained artifacts (`windows-remount-replay-fix`, 34,148 ms, cleanup passed).
- [x] Inspected final diff, stopped the owned regression PostgreSQL project and prepared only task files for the delivery commit.

## Surprises & Discoveries

`redb_engine/replication.rs::remove_stale_dentries_for_base_entry` decoded every pending local event for each reconciled inode before checking whether any stale alias existed. Self replay retained the old durable cursor until its transaction committed, so this repeated a large scan while holding the redb write transaction and PostgreSQL pending-state lock. Diagnostics snapshots also acquire pending-state, explaining why connection saturation can accompany a stalled replay. The operation-count regression failed with 12 unnecessary decoded rows and passes with zero.

The PostgreSQL producer can split one projected publication at its byte cap. Projection visibility can depend on a later file-publication event, so an individual transport batch does not necessarily match any complete local prefix. Both remote replay and the displayed publication queue could finish while the durable journal cursor remained behind. On restart the in-memory publication exclusions disappear and the journal is projected again. A 1,024-byte test cap reproduces this at 32 files, without requiring 60,000 files or a timing assertion. The baseline test was rerun after explicitly applying every remote page and still reopened with 190 pending operations.

`pending_metadata_ops=0` measures publication work; it does not independently prove that the remote replay target has been applied. The regression waits for both queue drain and a frozen remote head before claiming complete catch-up. Interrupted catch-up retains the documented at-least-once behavior.

## Decision Log

- Decision: Fix the shared reconciliation helper, with an operation-count regression rather than a short timing assertion. Rationale: both rename repair and regular-file replay call it, and the pending-alias protection must remain correct. Date/Author: 2026-10-10, Codex.
- Decision: Use only disposable volumes and local test PostgreSQL; leave the user's mounted volume and cache untouched. Rationale: the supplied log establishes the symptom without authorizing data destruction. Date/Author: 2026-10-10, Codex.
- Decision: Match the entire unsplit projection using the existing prefix matcher and record its exact local ranges with the final PostgreSQL batch version in `LocalEventProgress.published_prefixes`. Rationale: changing the event codec, removing transport bounds, or retiring the entire journal would violate existing contracts. Retire eligible ranges atomically with replay position only after the final batch applies; retain them across failed local commits and keep local-only suffixes. Date/Author: 2026-10-10, Codex.

## Outcomes & Retrospective

Both reproduced defects are fixed. Twenty-five unique focused unit tests and one service-backed split-publication close/reopen regression passed, including pending hardlinks, rejected cursor gaps, local-only replay tails, rollback and the last-page version boundary. The rebuilt release binary passed registered native WinFSP scenarios 21 (stale remount/cold discover catch-up) and 8 (collision rename versus rename); the runner and cleanup both passed. Final diff and gates passed; the isolated regression PostgreSQL project was stopped, and task files are ready for the delivery commit. The user's existing database was only copied and read through a disposable copy; the copied inspection harness was removed from source after capturing numeric evidence in `target/remount-investigation/evidence.txt`. A fresh mounted import of the full 60,000-file workload and full test suites were not run.

## Context and Orientation

`crates/studiofs-meta/src/pg_engine/replication_flush.rs` reconstructs pending publications from the local event journal. `metadata_sync.rs` applies PostgreSQL pages to redb under the canonical local-position then pending-state locks. `remote_apply.rs` acknowledges this client's own batches and reconciles serving state. `redb_engine/replication.rs` repairs namespace bindings using the existing reverse inode indexes and preserves unacknowledged local aliases. A serving binding is the filename/inode pair visible to local IO; base is its replicated version. `crates/studiofs/src/diagnostics/discovery_transport.rs` holds a connection permit while its blocking snapshot request reads metadata, so stalled metadata can consume all 16 permits.

## Plan of Work

Collect stale candidate bindings before scanning pending aliases; if none remain, return immediately. Keep journal-based protection for actual stale candidates. In `pg_engine/replication_flush.rs`, match the unsplit projected event list to the selected local journal before publishing, then associate the matching local prefix with the last committed batch version. `pg_engine/local_state.rs` combines these eligible ranges with existing completed ranges while respecting rejected barriers. `pg_engine/metadata_sync.rs` writes the resulting journal cursor in the replay transaction and retires in-memory markers only after commit. The existing matcher determines the endpoint, so unpublished tails remain replayable. No on-disk format or remote protocol changes are introduced.

The two new tests in `pg_engine/tests/replication_backlog.rs` cover bounded split-publication catch-up and reopen, including an unpublished local file, and a two-page replay with injected rollback. `docs/pg-sync-contract.md` records the version-gated acknowledgment boundary and restart semantics.

## Concrete Steps

From `<repo-root>`, run each targeted test with `cargo test -p studiofs-meta --lib <test_filter> -- --nocapture`. Filters used are `reconcile_regular_file_serving_`, `replica_serving_rename_`, `self_ack`, `restore_pending_state`, `replay_ack_and_completed_suffix_persist_in_one_redb_transaction`, and `split_publication_prefix_ack_waits_for_last_page_and_survives_rollback`. PostgreSQL filter `drained_out_of_order_file_publications_reopen_without_metadata_backlog` uses the isolated loopback service on port 25432 through `STUDIOFS_TEST_PG_DSN`; append `--ignored --test-threads=1`. Use `cargo check -p studiofs-meta`, `cargo clippy -p studiofs-meta --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`. Run Cargo sequentially. Never run full suites.

Rebuild with `cargo build --release -p studiofs`. The registry was verified using `cargo run -p studiofs-e2e -- list`. Native command: `target/debug/studiofs-e2e.exe run 21 --scenario 8 --studiofs-bin target/release/studiofs.exe --platform windows-winfsp --retain always --run-id windows-remount-replay-fix --port-range 31000-31100`. Native execution requires elevated driver/Docker access. Reports stay under `target/e2e-runs/windows-remount-replay-fix/`.

## Validation and Acceptance

The new operation-count test must fail before the correction and pass afterward. Existing stale rename, pending hardlink, and replay cursor tests must pass. Drained close/reopen must preserve file names and attributes and report zero publication operations before new writes. Native acceptance requires rebuilding the CLI binary and running a registered mounted E2E scenario with retained report artifacts. Unit checks alone do not prove that Windows IO or Ctrl+C shutdown succeeds.

## Idempotence and Recovery

All tests use temporary directories under `target/` and disposable PostgreSQL volumes. Do not reset the worktree or remove `.claude/`. Stop only the compose project created here after validation. Retain test and native diagnostics under `target/`.

## Artifacts and Notes

Baseline: `test_pg_metadata_notify_listener_start_syncs_missed_change` passed, one test, in 0.85 seconds on isolated PostgreSQL. Coverage checks reported metadata changes, so affected source was read directly; runtime.rs additionally has a parse-partial range at lines 310-311, also read directly.

Native report: `target/e2e-runs/windows-remount-replay-fix/report.json`, status `passed`, duration 34,148 ms. Scenario 21 took 13,008 ms; scenario 8 took 7,402 ms. Both used the freshly rebuilt `target/release/studiofs.exe` on `windows-winfsp`. Runner cleanup passed and retained diagnostics. No workspace-wide or full ignored suite was run.

## Interfaces and Dependencies

Keep existing metadata APIs, reverse dentry indexes, PostgreSQL protocol, and shutdown ownership contracts. Add no dependencies, settings, migrations, or publication work to foreground IO.

Plan updated after both failing regressions, focused validation, native acceptance and final diff inspection.
