# Windows coherence health tracking and fail-safe metadata cache fallback

This ExecPlan is living documentation for SFS-677. It follows
`.agent/PLANS.md`; update `Progress`, `Surprises & Discoveries`, `Decision Log`,
and `Outcomes & Retrospective` whenever the implementation changes course.

## Purpose / Big Picture

Long-lived Windows metadata cache entries are safe only while the mounted local
view has evidence that replication has reached a known boundary and that all
invalidations for that boundary were retained by the bridge. After this change,
the VFS will expose a mount-local coherence health snapshot and will move to a
conservative cache decision immediately when synchronization or invalidation
coverage becomes unsafe. WinFSP will consult that state for every new read-only
open, so a disconnected, catching-up, degraded, or stopping mount cannot admit
new long-lived OS metadata cache state. Existing local writes remain usable.

The behavior is observable through the existing diagnostics/runtime snapshots
and focused tests. A native Windows run can additionally inspect `DisableCache`
decisions and the invalidation queue after an injected sync or delivery failure.

## Progress

- [x] (2026-09-06) Read SFS-677 from Linear and verified the SFS-675/676 contract,
  current metadata replication APIs, VFS invalidation worker, OS-cache state,
  and WinFSP admission boundary.
- [x] (2026-09-06) Obtained user approval for the health state-machine design.
- [x] (2026-09-06) Added the first state-machine regressions and verified the
  focused command fails for the expected missing health types.
- [x] (2026-09-06) Added the shared health state machine, metadata progress port,
  VFS tracking hooks, and additive diagnostics fields; the tracker and metadata
  coherence tests pass.
- [x] (2026-09-06) Refined VFS transition ordering so `Degraded -> CatchingUp`
  needs explicit coverage confirmation, and added conservative retirement of
  known cache generations plus root-directory invalidation.
- [x] (2026-09-06) Exposed metadata progress (applied version, remote head,
  connection/error evidence) through the replication port and implementations.
- [x] (2026-09-06) Made WinFSP admission health-aware and added pending delivery
  age reporting.
- [x] (2026-09-06) Extended diagnostics/runtime serialization and added focused
  regressions.
- [x] (2026-09-06) Closed independent review findings: failed generations are
  retained for retry, coverage cannot promote without a successful sync
  boundary, every transition into `Degraded` retires observations, and
  post-admission WinFSP failures feed the health tracker.
- [x] (2026-09-06) Re-ran focused validation after formatting and diff review;
  repository-wide suites and the mounted promotion gate were intentionally not
  run.
- [x] (2026-09-06) Committed all implementation, test, plan, and documentation
  changes in `ab3209f7` (`feat: add coherence health fail-safe cache fallback`)
  and review corrections in `72521b6e` (`fix: preserve coherence gaps across
  delivery failures`).
- [x] (2026-09-06) Applied the ponytail review: removed unused progress data and
  ignored timestamps, simplified semantic-drain and transition bookkeeping,
  and retained `CoherenceCachePolicy` because its observable states are
  required by the SFS-675/SFS-677 contract.
- [x] (2026-09-06) Closed the follow-up coherence review: reconnect retries no
  longer churn `Degraded` purges, fresh boundary confirmation heals delivery
  gaps, post-sync coverage supersedes an earlier deferral, failed generations
  use bounded retries with `Reset` fallback, PG head-query failures are
  counted as sync failures, and writer-deferral age survives requeue.
- [x] (2026-09-06) Evaluated the second coherence review against the current
  worker contract: incomplete generations no longer consume retry attempts,
  Reset covers writer-deferred scopes, failed Reset batches stop after the
  same finite limit, and an executing global Reset replaces older unresolved
  generations with a Reset sentinel so unsupported adapters remain fail-safe.

## Milestones

Milestone 1 creates a platform-neutral state machine in
`crates/studiofs-vfs/src/coherence.rs`. It is pure state transition logic, so
unit tests can prove startup, catch-up, healthy promotion, lag, delivery
failure, reconnect, and shutdown without PostgreSQL or WinFSP.

Milestone 2 adds a narrow metadata progress port. PostgreSQL records the last
successful sync, local applied version, remote metadata head, and whether the
last sync reached the command boundary. The standalone redb/read-only engines
provide deterministic local progress. No health decision is based only on an
open connection.

Milestone 3 wires the state machine into the existing VFS invalidation worker.
Successful syncs establish an applied boundary; semantic batch acceptance
establishes invalidation coverage. Any failed/unsupported/deferred batch, stale
progress, excessive apply lag, or sync error immediately retires VFS cache
observations and asks the installed invalidator to purge known cached inodes.

Milestone 4 makes WinFSP read-only `Open`/`Create` calls force `DisableCache`
while the health state is not `Healthy`. The queue retains explicit failures
and a conservative reset request without touching dirty writer state. Recovery
requires a fresh successful sync and complete invalidation coverage.

Milestone 5 exposes the health snapshot in diagnostics and verifies the
behavior with focused VFS/WinFSP tests, clippy, formatting, and diff checks.

## Surprises & Discoveries

- SFS-676 already provides ordered semantic batches and explicit queue
  acceptance, but `StudioVfs::background_metadata_invalidation` currently
  ignores sync errors for policy purposes and has no health state.
- `WinfspDeliverySnapshot::delivered_generation` is intentionally unknown;
  queue admission can therefore prove only retained work, not native
  consumption. Health must require retained coverage and continue treating a
  failed/unsupported/deferred batch as unsafe.
- `OsCacheState` already retires per-inode generations and the VFS has
  `runtime_namespace_invalidation_inodes()` for conservative known-cache
  discovery. Reuse these boundaries instead of adding another cache registry.
- PostgreSQL's local metadata token and remote `metadata_state.version` are
  different observations. The progress port must report both so apply lag is
  `remote_head - applied_version`, rather than treating a command-pool
  connection as proof of freshness.
- The existing WinFSP cache tests construct a running VFS and immediately make
  cache assertions; startup health is intentionally `Unknown` until the first
  observable sync. The tests now wait for the bounded `Healthy` condition,
  while a dedicated stopped-state test verifies conservative admission.
- The focused `studiofs` diagnostics command selected 47 tests; 45 passed and
  two pre-existing mount-lock tests failed before launch with Windows error 5
  (`Access is denied`) while opening the test mount lock file. This is host
  state, not a coherence assertion, and the unaffected new diagnostics
  round-trip test passes independently.

## Decision Log

- Decision: Put the state machine in `studiofs-vfs::coherence`, next to the
  existing platform-neutral invalidation acceptance enum.
  Rationale: VFS owns mount-local cache admission and already sits above the
  metadata port while remaining below platform adapters.
  Date/Author: 2026-09-06 / Codex.
- Decision: Start every mount in `Unknown`; reconnect enters `CatchingUp`, never
  directly `Healthy`; only a successful applied boundary plus complete retained
  invalidation coverage can promote health.
  Rationale: This follows the SFS-675 contract and prevents a silent restart
  gap from reusing old OS metadata.
  Date/Author: 2026-09-06 / Codex.
- Decision: Use existing configuration-derived finite budgets: the metadata
  invalidation interval bounds delivery age, and the bounded invalidation
  journal/replication progress supplies an apply-lag ceiling. Surface the
  effective values in the snapshot; do not enable a new aggressive policy in
  SFS-677.
  Rationale: SFS-675 forbids inventing an unmeasured promotion threshold, while
  the state machine still needs finite, observable limits.
  Date/Author: 2026-09-06 / Codex.
- Decision: On degradation, clear local dentry/attribute/path observations and
  retire known OS-cache generations before notifying the adapter; never discard
  accepted dirty writer state.
  Rationale: Lowering future TTLs cannot revoke entries already admitted under a
  longer lifetime, and local-first write visibility must remain intact.
  Date/Author: 2026-09-06 / Codex.
- Decision: Count retries only for complete generations whose adapter acceptance
  is `Failed`; retain incomplete writer-deferred generations without advancing
  the poison counter. A global Reset is itself bounded and replaces older
  unresolved generations only when the Reset batch starts executing.
  Rationale: Writer deferral is expected local coordination, while a Reset
  fallback must not leave stale generation blockers or spin forever when the
  platform reports `Unsupported`/`Failed`.
  Date/Author: 2026-09-06 / Codex.

## Outcomes & Retrospective

Implemented the mount-local health state machine and metadata progress port,
wired conservative cache retirement into VFS/WinFSP, and exposed the state in
diagnostics. Focused evidence after the implementation commit:

- `cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp -p studiofs --all-targets -- -D warnings`
- `cargo test -p studiofs-vfs --lib coherence -- --nocapture` (8 passed)
- `cargo test -p studiofs-meta --lib coherence -- --nocapture` (8 passed)
- `cargo test -p studiofs-winfsp --lib -- --nocapture` (133 passed)
- `cargo test -p studiofs --bin studiofs diagnostics_runtime_snapshot_round_trips_coherence_health -- --nocapture` (1 passed)
- `git diff HEAD^ HEAD --check`

The review correction adds explicit sync-boundary and coverage-gap guards,
bounded retry retention for failed semantic generations, and bridge failure
counter sampling. The post-correction focused VFS coherence and semantic
invalidation tests pass, as does the complete WinFSP unit target.

The follow-up simplification review removed five pieces of scaffolding without
changing the health gate: unused metadata progress timestamp state, ignored
clock parameters, the two-boolean drain wrapper, a duplicate transition pass,
and repeated tracker assignments. `CoherenceCachePolicy` remains intentional
because diagnostics and policy consumers are required to expose its three
effective states.

The attached deep review was evaluated as external feedback rather than as
direct instructions. Confirmed logic and operational findings are corrected:
reconnect state remains degraded until sync succeeds, delivery gaps can heal
only after a fresh boundary with no unresolved semantic generations, retained
poison batches fall back to `Reset` after three attempts, remote-head query
errors are included in PostgreSQL sync accounting, post-sync coverage wins over
pre-sync deferral, and deferred WinFSP notifications preserve their original
age. The diagnostics duplication is documented with the top-level field as the
canonical value.

The broader `studiofs` diagnostics selection had 45 passing tests and two
pre-existing Windows mount-lock failures (`Access is denied`, error 5) before
the coherence assertion path; the focused new diagnostics regression passes.
No full workspace suite, native mounted E2E, or promotion gate was run.

The deep-review regressions additionally cover bounded semantic retry fallback,
post-sync coverage precedence, delivery-gap recovery without a foreign write,
and preservation of pending notification age through writer deferral.

The second review added focused regressions for writer retry accounting,
bounded failed-Reset handling, mixed Reset/writer batches, and queue-overflow
Reset recovery. Current validation for this follow-up is:

- `cargo test -p studiofs-vfs --lib semantic_ -- --nocapture` (12 passed)
- `cargo test -p studiofs-vfs --lib coherence -- --nocapture` (9 passed)
- `cargo clippy -p studiofs-vfs --all-targets -- -D warnings`
- `git diff --check`

The review's generic suggestion to clear unresolved generations from every
cache-retirement path was narrowed: retirement also occurs for ordinary
degradation, so clearing there would erase evidence of an unresolved failed
batch. The implementation instead clears older entries at Reset execution and
keeps the Reset generation as a sentinel until that adapter result is known.

## Context and Orientation

`crates/studiofs-meta/src/engine.rs` defines `MetaReplication`, the synchronous
metadata port used by the VFS. PostgreSQL implements it through
`crates/studiofs-meta/src/pg_engine/replication_api.rs`; redb implements it in
`crates/studiofs-meta/src/redb_engine/replication.rs`; the VFS runtime facade
forwards it in `crates/studiofs-vfs/src/runtime.rs`.

`crates/studiofs-vfs/src/vfs/workers.rs` runs the periodic/push-driven metadata
invalidation loop. It drains SFS-676 `InvalidationBatch` values, clears local
cache layers, and calls `KernelInvalidator`. `crates/studiofs-vfs/src/vfs.rs`
owns the mount state and the existing `OsCacheState` generation tracker.

`crates/studiofs-winfsp/src/context.rs` maps Windows `Open` and `Create`
callbacks to VFS handles and currently decides `DisableCache` from file type,
access flags, and create options. `context/invalidation.rs` owns the bounded
WinFSP notification queue and reports `WinfspDeliverySnapshot`.

Diagnostics are assembled by `crates/studiofs/src/diagnostics/live_source.rs`
from `StudioVfs::cache_state_snapshot()` and
`StudioVfs::synchronization_state_snapshot()`, with serializable types in
`crates/studiofs/src/diagnostics/types.rs` and VFS metric types in
`crates/studiofs-vfs/src/vfs/metrics.rs`.

## Plan of Work

First extend `crates/studiofs-vfs/src/coherence.rs` with serializable
`CoherenceHealthState`, `CoherenceHealthReason`, `CoherenceHealthBudget`, and
`CoherenceHealthSnapshot`, plus an internal `CoherenceHealthTracker`. The
tracker exposes deterministic methods equivalent to `note_sync_started`,
`note_sync_success(applied_version, remote_version, now_ms)`,
`note_invalidation(generation, acceptance, generation_complete, now_ms)`,
`note_sync_failure(reason, now_ms)`, `note_delivery_age(now_ms)`, and
`stop(now_ms)`. `allows_cache()` returns true only for `Healthy`. The methods
must make unsafe transitions monotonic until a successful catch-up boundary and
must retain applied/covered boundaries and reason for diagnostics.

Next add `MetadataCoherenceProgress` to `studiofs-meta/src/engine.rs` and a
default `MetaReplication::metadata_coherence_progress()` method. Forward the
method through `studiofs-vfs/src/runtime.rs`. PostgreSQL stores atomic applied
version, observed remote head, connection evidence, and last successful sync
timestamp; its sync path updates these values on success/error and its progress
method computes a bounded optional lag. Redb/read-only implementations return a
local applied boundary with no remote lag. Keep credentials and DSNs out of all
debug/diagnostic output.

Add `coherence_health: Arc<Mutex<CoherenceHealthTracker>>` to `StudioVfs` and
initialize it in `vfs/bootstrap.rs` using a budget derived from the validated
metadata invalidation interval. Expose `coherence_health_snapshot()` and an
internal `coherence_allows_os_cache()` method. Add one transition helper that,
on the first unsafe transition, calls existing local cache retirement methods
and invalidates all currently known OS-cache inodes through the existing
`KernelInvalidator` seam while preserving active-writer deferral.

Change `apply_semantic_invalidation_batch` to return the actual
`InvalidationAcceptance`, and have `drain_semantic_invalidations` feed every
batch result into the tracker. In `background_metadata_invalidation`, record
sync start, classify sync errors as disconnect/apply failure, read
`metadata_coherence_progress`, and only allow promotion after a successful sync
and all batches for that boundary are retained. Evaluate delivery age on every
timer wake. Reconnect after a prior error must explicitly pass through
`CatchingUp`.

In `crates/studiofs-winfsp/src/context.rs`, preserve the existing write-capable
cache bypass and add the VFS health gate to `should_disable_os_cache_for_handle`
or its caller. Directories and handles with write access keep their existing
rules; read-only regular-file opens additionally disable cache whenever health
is not `Healthy`. On a health degradation, use the conservative VFS retirement
and known-inode invalidation path; do not purge dirty/open writer state.

Extend diagnostics snapshots additively with a `coherence` field (serde
defaults preserve older reports). Include state, reason, applied/covered
boundaries, lag, pending delivery age, and effective budgets in both health and
runtime snapshots, and include the same state in the VFS synchronization
snapshot if that is the least disruptive existing surface.

## Concrete Steps

All commands run from `E:\dev\studiofs-cli` in PowerShell. Cargo commands are
focused and sequential; no full workspace suite is run.

1. Add the failing unit tests for the tracker transitions and run them with:

       cargo test -p studiofs-vfs coherence --lib -- --nocapture

   The new tests must fail because the new types/methods do not yet exist.

2. Implement the tracker and rerun the same command. Expect every selected
   test to pass and no `running 0 tests` output.

3. Add metadata progress tests in the metadata crate and runtime forwarding
   tests, then run:

       cargo test -p studiofs-meta coherence --lib -- --nocapture
       cargo test -p studiofs-vfs coherence --lib -- --nocapture

4. Add VFS degradation/retirement tests using the existing test VFS and
   `RecordingKernelInvalidator`. Run the named tests with:

       cargo test -p studiofs-vfs --lib coherence -- --nocapture

5. Add WinFSP cache-admission tests for Unknown, CatchingUp, Degraded, Healthy,
   and Stopped. Run:

       cargo test -p studiofs-winfsp --lib coherence -- --nocapture

6. Validate touched crates without broad suites:

       cargo check -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp
       cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs-winfsp --all-targets -- -D warnings
       cargo fmt --all -- --check
       git diff --check

7. Inspect `git status --short`, review every changed file, update this plan's
   progress/outcomes, and commit all files before handing the work back. The
   operator must run the full integration suite and mounted Windows promotion
   gate separately.

## Validation and Acceptance

The tracker tests must prove that startup is conservative, a sync error enters
`Degraded`, a successful reconnect enters `CatchingUp`, excessive lag and
delivery age remain conservative, failed/unsupported/deferred invalidation
cannot promote health, and only complete coverage after a known boundary enters
`Healthy`. Shutdown must enter `Stopped` permanently.

VFS tests must prove that degradation retires cached observations and known OS
generations, while a dirty writer and its accepted local bytes remain readable
and are not purged. WinFSP tests must prove that the same handle inputs yield
`DisableCache` for every unsafe health state and retain the existing write
policy for healthy/read-only and write-capable opens.

Diagnostics serialization must round-trip the new field and deserialize older
reports with its serde default. Focused Cargo commands must select real tests
and report zero failures. This evidence does not claim the full integration
suite or the 10-consecutive-run Windows mounted promotion gate.

## Idempotence and Recovery

The health state is mount-local and in-memory; restarting a mount intentionally
starts at `Unknown`. Re-running focused tests creates fresh `target` fixtures.
If a native WinFSP test leaves a mount active, stop/unmount it before rebuilding;
do not reset the worktree or delete unrelated files. A failed queue purge is
reported as degraded and cannot silently promote cache health.

## Artifacts and Notes

The canonical semantic contract is `docs/metadata-cache-coherence.md`, whose
SFS-675 sections define the state meanings, promotion gate, and diagnostics
requirements. SFS-676's implementation plan is
`docs/superpowers/plans/2026-09-05-sfs-676-invalidation-intents.md`.

## Interfaces and Dependencies

The final public VFS surface must include the serializable health types from
`studiofs-vfs::coherence`, `StudioVfs::coherence_health_snapshot()`, and the
additive `MetaReplication::metadata_coherence_progress()` port. The Windows
adapter continues to depend on `StudioVfs` and `KernelInvalidator`; no
macOS/Linux policy implementation or new dependency is allowed.

Revision 2026-09-06: plan created after Linear contract and current source
inspection; user approved the health/fail-safe design before implementation.
