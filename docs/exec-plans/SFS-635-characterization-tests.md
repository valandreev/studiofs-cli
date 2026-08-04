# Establish SFS-635 characterization tests before refactoring

This ExecPlan is a living document and must remain aligned with `.agent/PLANS.md`.
It records the shared-core and platform-specific regression evidence needed before
the Stage 1 and Stage 2 refactor issues are implemented.

## Purpose / Big Picture

StudioFS is a local-first filesystem: a write must remain recoverable locally
when S3 or PostgreSQL is unavailable, and a platform bridge must not call native
shutdown or cache-invalidation APIs after their owner is gone. This work adds a
small set of deterministic tests that make those contracts observable before
the refactor changes their implementation.

The result is visible through focused `cargo test` commands. Shared tests run
without live services; PostgreSQL/S3 and mounted bridge rows are recorded as
separate validation axes and are never counted as passing when their runner is
unavailable.

## Progress

- [x] (2026-08-04 21:00 Europe/Kyiv) Add the SFS-637 failing-before/fixing-after upload lifecycle fixture; the `--ignored` run fails at the pre-fix queue-completion assertion.
- [x] (2026-08-04 21:10 Europe/Kyiv) Add and run the UploadQueue completion-wakeup and burst consistency tests.
- [x] (2026-08-04 21:20 Europe/Kyiv) Add and run legacy MetaEvent bytes and local event-log replay fixtures. A raw PG event-table fixture is not accessible from the current private module boundary and remains a later SFS-639/PG-axis check.
- [x] (2026-08-04 21:30 Europe/Kyiv) Add and run the shared prefetch burst test; add the Windows-only WinFSP invalidation burst test.
- [x] (2026-08-04 21:45 Europe/Kyiv) Run available focused checks and record unavailable macFUSE, Linux/FUSE, WinFSP, PostgreSQL, and S3 cells.
- [x] (2026-08-04 21:50 Europe/Kyiv) Update this plan with final evidence and the Linear issue.

## Surprises & Discoveries

- The code graph was not available for this private checkout: both persistent
  and nonpersistent MCP indexing were rejected because they would send source
  contents to an unverified destination. Discovery therefore uses local `rg`
  only for this task.
- `UploadProcessor::process_single_upload` calls `complete_upload` before the
  fallible local `mark_uploaded`/cleanup path. A test that injects a local
  transition failure should expose this as a known SFS-637 failure.
- UploadQueue already documents a notify-before-count intent, but the current
  code checks `pending_count` before constructing `idle_notify.notified()`. The
  characterization test must capture wake behavior without changing timeouts.
- macFUSE invalidation-state and queue model tests already cover close gates,
  coalescing, backpressure, and idle deadlines; SFS-635 should extend the
  lifecycle evidence rather than duplicate those tests.
- The Windows-only focused command compiled the crate on macOS but ran zero
  tests because the module is guarded by `cfg(windows)`; this is an unavailable
  Windows validation cell, not a passing burst result.

## Decision Log

- Decision: Keep SFS-635 test-only except for test support state and this plan.
  Rationale: SFS-635 establishes contracts; production fixes belong to the
  blocked Stage 1/2 issues and must not be hidden in characterization work.
  Date/Author: 2026-08-04 / Codex.
- Decision: Use ignored tests only for contracts that are intentionally expected
  to fail before a later issue, with the issue ID in the ignore reason.
  Rationale: The normal focused suite must stay green while the plan retains a
  runnable proof of the pre-fix defect.
  Date/Author: 2026-08-04 / Codex.
- Decision: Use small fixtures in the existing test modules instead of adding a
  generic characterization framework.
  Rationale: The issue explicitly asks for focused tests and the repository
  already has crate-local fixtures.
  Date/Author: 2026-08-04 / Codex.

## Outcomes & Retrospective

Completed as a test-only characterization layer. The normal focused checks are
green; the ignored SFS-637 test fails at the intended pre-fix assertion. The
macFUSE model tests ran on this macOS host. Linux/FUSE mounted, Windows/WinFSP
native, PostgreSQL mixed-client, and S3/RustFS checks remain external matrix
cells and were not counted as passes.

## Context and Orientation

The shared upload queue lives in `crates/studiofs-cache/src/upload_queue.rs`.
It owns the authoritative pending-upload maps and exposes a Tokio receiver to
the VFS worker. `crates/studiofs-vfs/src/vfs/upload_lifecycle/worker.rs` consumes
that receiver, uploads payloads, updates metadata, and removes local staging.

Persisted metadata events are postcard bytes written by
`crates/studiofs-meta/src/redb_engine/txn.rs` and decoded by
`crates/studiofs-meta/src/redb_engine/replication.rs`; the codec helpers are in
`crates/studiofs-meta/src/redb_engine/layout.rs`. The PostgreSQL engine replays
the same local event log; the raw local decoder fixture is kept in
`crates/studiofs-meta/src/redb_engine/tests.rs` because the event-table
constants are private to the redb module.

Prefetch hints enter shared VFS through `crates/studiofs-vfs/src/vfs/prefetch.rs`.
The worker deduplicates `(inode, block)` keys in a second pending queue. The
Windows bridge collects invalidated inode numbers in
`crates/studiofs-winfsp/src/context/invalidation.rs` and deduplicates them when
the notification timer drains the queue. macFUSE lifetime and invalidation
model code is in `crates/studiofs-macfuse/src/low_level.rs`.

## Plan of Work

First extend the existing `FakeLocalPayloads` fixture in the upload worker so
`mark_uploaded` can return one injected error. Add an ignored test named for the
contract: after remote upload, a local transition failure must leave the
coordinator non-terminal and the operation recoverable. Run it with
`--ignored` and preserve the observed pre-fix failure; do not fix production
ordering in SFS-635.

Next add one UploadQueue test that submits an operation, lets a waiter reach
the idle notification point, completes the operation, and proves the waiter
wakes. Add one burst test that submits many distinct objects for one inode,
checks the count and both indexes, drains all operations, and checks that the
indexes return to empty.

Add a small golden fixture in the metadata layout tests for a legacy
`MetaEvent::RemoveAttr` postcard payload. Assert both decode and re-encode so a
future stable envelope can retain dual-read compatibility. Add a local replay
test in the redb engine tests that inserts the legacy bytes into the real local
event table and reads them through `read_events_range`. Leave mixed-client PG
replay for SFS-639, where the stable envelope and PG `event_type` column will
exist.

Add a prefetch worker burst test for repeated `(inode, block)` hints and a
WinFSP notification burst test for repeated inode invalidations. Both must
assert deduplication, queue drain, and the existing depth/capacity behavior.
Keep platform-gated tests in their existing platform crates and record that
macFUSE/WinFSP real mount checks and PostgreSQL/S3 checks require their native
runner or service.

## Concrete Steps

Run commands from `/Volumes/Storage/dev/studiofs`.

    cargo test -p studiofs-vfs process_upload_does_not_complete_queue_when_local_transition_fails -- --ignored --nocapture  # expected pre-fix failure
    cargo test -p studiofs-cache upload_queue -- --nocapture  # 15 passed
    cargo test -p studiofs-meta legacy_meta_event -- --nocapture  # 2 passed
    cargo test -p studiofs-vfs vfs::prefetch::tests -- --nocapture  # 8 passed
    cargo test -p studiofs-macfuse low_level::tests::session_invalidation -- --nocapture  # 8 passed
    cargo test -p studiofs-macfuse prepare_session_shutdown_closes_invalidation_before_waiting_for_idle -- --nocapture  # 1 passed
    cargo test -p studiofs-winfsp should_notify_deduplicates_a_large_invalidation_burst -- --nocapture  # 0 tests on macOS: cfg(windows)
    RUSTFLAGS='-D warnings' cargo check -p studiofs-cache -p studiofs-meta -p studiofs-vfs -p studiofs-macfuse -p studiofs-winfsp
    cargo fmt --all -- --check
    git diff --check

The first command failed at
`queue completion must follow successful local durable acceptance`, proving the
fixture catches the current ordering defect. The remaining local shared and
macFUSE commands passed, `cargo check -D warnings` passed, and formatting/diff
hygiene passed. Platform-gated commands must be run on their supported
operating system and are recorded as unavailable otherwise.

Before handoff, run `cargo fmt --all -- --check`, the focused `cargo test`
commands above, `RUSTFLAGS='-D warnings' cargo check` for changed crates where
the platform toolchain supports them, and `git diff --check`.

## Validation and Acceptance

Acceptance is behavioral. The local upload failure test demonstrates that a
failed durable transition is not reported as a terminal completion by the
current implementation and is ready to turn green under SFS-637. The queue
test demonstrates that a completion wakes a registered idle waiter and that a
burst cannot leave count/map state inconsistent. The legacy fixture demonstrates
that existing event bytes retain their meaning. Prefetch and WinFSP tests
demonstrate coalesced burst handling.

The matrix is explicit:

- Shared core: cache queue, VFS worker/prefetch, and metadata fixtures.
- macOS/macFUSE: low-level model tests plus real mount shutdown when macFUSE is installed.
- Linux/FUSE: shared lifecycle checks plus Linux mounted smoke when `/dev/fuse` is available.
- Windows/WinFSP: invalidation burst and shared lifecycle checks plus WinFSP mounted smoke on Windows.
- PostgreSQL: local replay fixtures always; mixed-client PG replay only with `STUDIOFS_TEST_PG_DSN`.
- S3: upload ordering fixtures locally; remote visibility and outage convergence only with RustFS/S3.

An unavailable matrix cell is an external validation gap, not a pass.

## Idempotence and Recovery

All test fixtures use existing `target/`-backed temporary-directory helpers.
They are safe to rerun. The ignored failure test must remain ignored until the
SFS-637 fix changes the ordering; after that issue lands, remove the ignore and
expect it to pass. Do not reset or discard unrelated branch changes.

## Artifacts and Notes

Record the focused test output and the exact unavailable-platform/service
conditions in the final Linear update and this plan. Keep only concise output
that proves the contract; do not commit generated logs or temporary databases.

## Interfaces and Dependencies

No new production trait or queue abstraction is required. Tests use the
existing `PendingUpload`, `UploadProcessor` ports, `MetaEvent` postcard helpers,
`PrefetchWorker`, and `WinfspKernelInvalidator` seams. The only expected test
support change is an injectable failure flag in `FakeLocalPayloads`.
