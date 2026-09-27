# Reduce macFUSE interrupt registry contention

This is a living ExecPlan governed by `.agent/PLANS.md`. Keep `Progress`,
`Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective`
current until the work is complete.

## Purpose / Big Picture

Every asynchronous macFUSE read registers a cancellation token so macFUSE can
interrupt that read. The current process-wide mutex serializes registry insert,
callback lookup, and cleanup even when requests use different keys. This work
will preserve cancellation and request teardown behavior while measuring the
current contention and replacing the shared lock only if the before/after
measurement supports it. The observable proof is focused callback lifecycle
tests, a repeatable release-mode contention benchmark, and a native mounted
read scenario when its services and runner are available.

## Progress

- [x] (2026-09-27 15:43Z) Confirm SFS-665 scope, clean issue branch, macOS host,
  installed macFUSE version, existing DashMap dependency, and the callback and
  request cleanup call paths.
- [x] (2026-09-27 15:49Z) Add callback lifecycle characterization tests and an
  opt-in release benchmark; record the original mutex baseline before changing
  it.
- [x] (2026-09-27 15:52Z) Replace the global mutex map with the existing
  `DashMap`, preserving opaque integer keys and unregister-before-remove
  ordering.
- [x] (2026-09-27 16:01Z) Run focused and full crate tests, compare the
  benchmark, run Rust checks, and pass the registered native mounted
  concurrent-read scenario.
- [x] (2026-09-27 16:01Z) Record measurements, mounted-run evidence, and the
  measurement boundary below.
- [x] (2026-09-27 16:45Z) Verify the supplied review against the code and issue.
  Add a nonzero-key wrap regression, assert race cleanup, separate ordinary
  registration/removal from forced callback lookup, and stop benchmark timing
  before thread joins.
- [x] (2026-09-27 16:45Z) Compare the base mutex, default DashMap, and
  Foldhash-backed DashMap with the corrected benchmark. The fast hasher removes
  the one-worker regression seen with default DashMap; multi-worker results
  remain workload-dependent.
- [x] (2026-09-27 16:50Z) Re-run crate tests, check, Clippy, formatting, release
  build, and the registered native scenario after the review follow-up.
- [x] (2026-09-27 17:46Z) Verify the next review: contain Rust unwinds at the
  macFUSE interrupt callback, separate worker readiness from benchmark timing,
  and record a new Foldhash-only snapshot without comparing harness variants.
- [x] (2026-09-27 18:37Z) Verify the next review: catch panics from cancellation
  and fallback logging in the real callback, assert duplicate keys in debug
  builds, and measure the span between actual worker starts and finishes.
  Record the new release benchmark results separately from previous timing
  methods.

## Surprises & Discoveries

- The registry is used only by asynchronous cancellable reads. Registration is
  in `StudioVfsAdapter::queue_read_reply`; reply, take, and drop paths all clear
  it through `Request::clear_interrupt_token`.
- The original `request_interrupted` path called `CancellationToken::cancel()`
  while holding the process-wide map mutex. The replacement clones the token
  while protected and cancels after releasing the map guard.
- The checkout is on macOS 26.6.2 with macFUSE 5.4.0. `DashMap` 6.1.0 is
  already a workspace dependency and is already used in the macFUSE adapter.
- The five-sample median registry-cycle benchmark falls from 4,266,485
  requests/s at two workers to 2,999,378 requests/s at eight workers, a 29.7%
  throughput drop. The recorded workload includes insert, callback cancel,
  and removal; it is an in-process benchmark, not mounted filesystem evidence.
- With `DashMap`, the same benchmark reaches 9,538,060 requests/s at eight
  workers (+218.0%) and 3,165,091 requests/s at one worker (-21.7%). The
  single-thread regression is recorded rather than hidden.
- Registered scenario `collision-overwrite-vs-read` passed through the native
  macFUSE backend and runner cleanup. Its report confirms concurrent reads but
  does not count interrupt callbacks; callback cancellation itself is covered
  by the low-level tests.
- The mounted scenario ran once with the mutex (9,192 ms) and twice with
  `DashMap` (8,466 ms and 9,218 ms). The ranges overlap, so the mounted runs
  validate behavior but do not establish an end-to-end performance gain.
- The E2E scenario source is excluded from the code graph, so any scenario
  selection must be checked against its source and `cargo run -p studiofs-e2e
  -- list`.
- A key of zero becomes a null `void *` when passed to macFUSE, and the callback
  intentionally treats null as absent callback data. A red regression showed
  that the atomic counter returned zero after wrapping; the generator now skips
  that value.
- The reviewed race test joined both threads but did not inspect final registry
  state. It now checks that the key is absent after every concurrent callback
  and removal pair.
- The paired two-barrier benchmark has separate register/remove and
  register/callback/remove workloads, alternates their order, warms both, and
  stops at a completion barrier before joining workers. It measures ordinary
  cancellable-read registration and cleanup separately from forced callback
  lookup, but includes startup/scheduling jitter at its first barrier.
- With that harness, the default DashMap was slower than the mutex at one
  worker. An A/B using `foldhash::fast::RandomState` improved both one-worker
  paths by about 49–51% versus default DashMap and put them 5–13% above the
  mutex. At two and four workers, hasher results varied; at eight workers,
  Foldhash remained near or above default DashMap. These are in-process
  registry measurements, not mounted throughput.
- The paired two-barrier timing excludes joins but includes readiness and
  scheduling before all workers reach its first barrier. The previous
  three-phase timing also includes barrier release and completion costs. The
  latest worker-spanned Foldhash snapshot is not compared directly with either
  historical table.
- The paired two-barrier harness passed against the base mutex, default
  DashMap, and Foldhash-backed DashMap. At eight workers the Foldhash result was
  15.7 million register/remove lifecycles per second and 13.1 million
  register/callback/remove lifecycles per second; the mutex medians were 3.9
  million and 2.9 million, respectively.
- The rebuilt Foldhash binary passed the native `collision-overwrite-vs-read`
  scenario in 8,344 ms. Setup, run, validation, runner cleanup, and mount
  cleanup passed. This single run overlaps the earlier native timings and does
  not establish an end-to-end throughput gain.
- The first sandboxed native attempt stopped at preflight because access to the
  configured OrbStack Docker socket and port checks was denied. Re-running the
  same registered scenario with permission to access those local resources
  passed; no product scenario ran during the failed preflight.
- The `extern "C"` callback catches unwinds from both cancellation and its
  fallback warning. A regression test registers a panicking task waker and a
  panicking tracing subscriber and invokes the real callback; before the fix,
  the test binary aborted at the non-unwinding ABI boundary. OOM remains outside
  `catch_unwind`.
- `std::sync::Barrier` is reusable, so the former barrier arrangement was not a
  phase-overlap race. The latest benchmark uses ready/start barriers only;
  per-worker timestamps measure actual work rather than the coordinator's
  return from the start barrier.
- The old paired table used the two-barrier timer and included worker startup
  scheduling jitter. The first three-phase Foldhash snapshot also included
  start-barrier release and completion-barrier costs. The latest benchmark
  records each worker's start/finish and spans the earliest start through the
  latest finish, so it excludes barrier release before actual work and barrier
  completion after work. Every cycle still allocates a fresh token, matching
  the one-shot cancellation lifecycle and making allocation cost part of the
  reported work.
- Skipped the suggested DashMap preallocation: the 8,192-entry constant bounds
  distinct invalidation targets, not active reads, while read admission uses a
  byte-weighted semaphore. No measured shard-growth stall justifies a guessed
  capacity.
- Added a debug assertion for duplicate registry keys. The generator does not
  reuse a nonzero key until a full `usize` wrap, but the assertion makes an
  accidental collision visible during development.
- Kept token allocation in the benchmark. Cancellation tokens are one-shot and
  production allocates one per asynchronous read; recycling cancelled tokens
  would no longer measure that lifecycle.
- `cargo tree -i foldhash` is ambiguous because the lockfile contains 0.1.5
  and 0.2.0, but the workspace dependency graph uses 0.2.0 and the reverse
  query for 0.1.5 prints no active dependency path. Kept the dependency graph
  unchanged rather than rewriting unrelated lockfile resolution.

## Decision Log

- Decision: Measure before changing the registry and use the same benchmark
  after the change.
  Rationale: The issue confirms a structural serialization point but has no
  measured performance magnitude; a before/after comparison avoids an
  unsupported improvement claim.
  Date/Author: 2026-09-27 / Codex.
- Decision: Reuse the workspace's existing `DashMap` if the baseline supports
  replacing the mutex.
  Rationale: It is already available in `studiofs-macfuse`, so no dependency or
  new registry abstraction is needed.
  Date/Author: 2026-09-27 / Codex.
- Decision: Replace the mutex map with the existing `DashMap` after the recorded
  baseline showed lower aggregate throughput at eight workers.
  Rationale: The benchmark confirms the lock contention is measurable under
  concurrent registry cycles, and the dependency is already used in this crate.
  Date/Author: 2026-09-27 / Codex.
- Decision: Preserve monotonically allocated opaque integer callback keys and
  call macFUSE to unregister before removing the associated map entry.
  Rationale: macFUSE may have copied callback data while a request is being
  replied to or dropped; entry removal must not invalidate callback state.
  Date/Author: 2026-09-27 / Codex.
- Decision: Treat the native mounted scenario as behavior evidence, not a
  performance win.
  Rationale: One mutex run and two DashMap runs overlap in scenario duration,
  and the scenario does not report an interrupt callback count.
  Date/Author: 2026-09-27 / Codex.
- Decision: Use `foldhash::fast::RandomState` as the DashMap hasher after an
  A/B measurement.
  Rationale: With identical workloads and timer boundaries, default DashMap
  was 24–31% slower than the mutex at one worker, while Foldhash was 5–13%
  faster than the mutex and roughly 49–51% faster than default DashMap. The
  keys are generated locally, so the fast hasher's reduced resistance to
  attacker-chosen collisions does not expose an untrusted-key boundary.
  Date/Author: 2026-09-27 / Codex.
- Decision: Skip zero when allocating callback keys.
  Rationale: macFUSE receives the key as `void *`; zero is null and the callback
  returns without cancellation for null callback data. A focused wrap test
  reproduced the failure, and skipping zero fixes that exact contract violation.
  Date/Author: 2026-09-27 / Codex.
- Decision: Keep both benchmark workloads and measure from the first worker's
  actual start through the last worker's finish.
  Rationale: Every cancellable read registers and removes a token, while the
  callback is only run for interrupted reads. Per-worker timestamps exclude
  barrier release before work and thread join after it while retaining
  staggered worker startup in the elapsed span. The separate forced-callback
  mode retains a stress case for lookup/cancel contention.
  Date/Author: 2026-09-27 / Codex.

- Decision: Catch Rust unwinds inside the macFUSE interrupt callback.
  Rationale: `extern "C"` is non-unwinding and a Rust panic escaping it aborts
  the process. The callback catches both cancellation and fallback logging
  panics; it does not claim to recover from aborting panics or OOM.
  Date/Author: 2026-09-27 / Codex.
- Decision: Keep fresh cancellation tokens in every synthetic request cycle.
  Rationale: A cancelled token cannot be reset, and production creates a token
  for each asynchronous read. Document the allocation cost instead of changing
  the workload into token reuse.
  Date/Author: 2026-09-27 / Codex.

## Outcomes & Retrospective

The global mutex map is replaced by a `DashMap` using
`foldhash::fast::RandomState`; callbacks clone the token before releasing the
entry guard and cancelling. Callback keys skip zero after counter wrap, and a
debug assertion detects duplicate registration. The race test verifies
registry cleanup after both threads join. The benchmark measures ordinary
registration/removal separately from forced callback lookup and spans the
earliest worker start through the latest finish. Matched mutex and earlier
Foldhash snapshots remain historical. Mounted timings overlap, so no end-to-end
gain is claimed. The runner does not report interrupt callback counts, so
cancellation frequency remains unmeasured.

## Context and Orientation

`crates/studiofs-macfuse/src/low_level.rs` owns the low-level request wrapper
and C interrupt callback. `Request::register_interrupt_token` inserts an
integer key before registering `request_interrupted` with macFUSE.
`Request::clear_interrupt_token` unregisters that callback before removing its
key. `Request::take_raw` and `Drop` both use the same cleanup method.
`crates/studiofs-macfuse/src/adapter.rs` registers the read cancellation token
from `StudioVfsAdapter::queue_read_reply`.

The registry change is limited to synchronization and hashing for those
entries. It must not alter read admission, local-first I/O, FFI callback data,
request ownership, or shutdown state. A callback lookup returns a
`CancellationToken` clone; the map guard must be gone before `cancel()` runs.
The registry uses the existing `DashMap` dependency with the workspace's
`foldhash` dependency declared directly for this crate.

## Plan of Work

The low-level tests characterize cancellation, removal, callback/removal races,
duplicate-key detection, and the non-null key invariant at counter wrap. The
ignored release benchmark has a normal register/remove mode and a forced
callback stress mode. The current method uses ready/start barriers and records
each worker's start and finish; prior paired and three-phase measurements remain
labeled historical. Registration still inserts before FFI callback setup,
callbacks clone the token before releasing the entry guard and cancelling, and
teardown still unregisters FFI before map removal.

## Concrete Steps

Run from `/Volumes/Storage/dev/studiofs`:

    cargo test -p studiofs-macfuse --lib interrupt_registry -- --nocapture
    cargo test -p studiofs-macfuse --lib next_interrupt_key_skips_zero_after_wrap -- --test-threads=1
    cargo test -p studiofs-macfuse --lib interrupt_registry_callback_and_removal_are_race_safe
    cargo test -p studiofs-macfuse --release --lib interrupt_registry_contention_benchmark -- --ignored --nocapture --test-threads=1
    cargo test -p studiofs-macfuse --lib
    cargo check -p studiofs-macfuse
    cargo build -p studiofs --release
    target/debug/studiofs-e2e run collision-overwrite-vs-read --studiofs-bin target/release/studiofs --platform macos-fuse --infra compose --retain always --run-id sfs665-foldhash-read-20260927 --seed 665
    cargo fmt --all -- --check
    cargo clippy -p studiofs-macfuse --all-targets -- -D warnings
    git diff --check

The earliest before/after numbers used a timer that included worker joins and
a callback on every cycle. Keep them labeled as historical. The current
benchmark uses ready/start barriers, worker-local start/finish timestamps, two
workloads, seven timed samples after one warm-up per workload and worker count,
and alternating workload order. Record matched mutex/default-DashMap/Foldhash
runs separately in `docs/perf/sfs-665-macfuse-interrupt-registry.md`. Use `cargo run -p
studiofs-e2e -- list` to select an existing scenario before mounted validation;
do not invent a runner scenario name.

## Validation and Acceptance

The lifecycle tests must pass with a nonzero test count and prove callback
cancellation, cancellation/log panic containment at the real callback, removal
behavior, race cleanup, duplicate-key detection in debug builds, and that
generated keys never become null at atomic wrap. The release benchmark must
measure both registration/removal and registration/callback/removal, with
matched worker counts for mutex and DashMap variants. It must exclude thread
creation, time before worker start, and joins from the timed interval. The
reported span runs from the earliest worker start through the latest worker
finish and therefore includes staggered worker startup. Report measured
throughput and percentage change; claim a reduction only where the measurement
supports it.

The native mounted result is separate from the in-process benchmark. The same
registered `collision-overwrite-vs-read` scenario passed once with the
clean-HEAD mutex binary, twice with default DashMap, and once with Foldhash.
Scenario durations were 9,192 ms, 8,466 ms, 9,218 ms, and 8,344 ms. The overlap
does not support an end-to-end performance claim. Reports are retained in the
corresponding `target/e2e-runs/sfs665-*` directories, and no mount remained
after cleanup. The callback behavior is proved by the low-level tests, and the
contention improvement by the in-process registry benchmark.

## Idempotence and Recovery

The tests and benchmark use generated integer keys and remove each inserted
entry after the callback, so repeated runs do not retain registry state. The
benchmark is opt-in and does not mount a filesystem. If a mounted run fails,
retain the runner report and diagnostics and use its cleanup command; do not
delete or force-unmount a path that the runner does not own.

## Artifacts and Notes

The performance record belongs in
`docs/perf/sfs-665-macfuse-interrupt-registry.md`. It must distinguish the
synthetic registry benchmark from mounted scenario evidence and avoid claiming
that one proves the other.

## Interfaces and Dependencies

Keep the existing private key generator and `Request` API. The registry remains
private to `low_level.rs`; it uses the workspace's `dashmap` and `foldhash`
crates. Do not add configuration, metrics, public API, or teardown state for
this optimization.

Plan update (2026-09-27 16:01Z): recorded before/after benchmark results and the
first native mounted run. Update (2026-09-27 16:11Z): added the clean-HEAD
mounted baseline and repeat optimized run; this records the overlapping native
timings and avoids claiming an end-to-end gain that the scenario did not show.
Update (2026-09-27 16:45Z): incorporated verified code review findings. Added
non-null key allocation and race cleanup assertions, corrected benchmark
coverage/timing, remeasured base mutex/default DashMap/Foldhash variants, and
recorded the fast hasher choice. Update (2026-09-27 16:50Z): the full macFUSE
library test suite passed (186 passed, one ignored benchmark), as did `cargo
check`, Clippy with warnings denied, formatting, diff check, release build, and
the registered native scenario. The retained native report is
`target/e2e-runs/sfs665-foldhash-read-20260927/report.json`.

Update (2026-09-27 17:46Z): addressed the follow-up review with callback
unwind containment and a three-phase benchmark. Preserved the previous paired
table as historical, added a Foldhash-only snapshot, aligned workload names,
and documented fresh-token allocation. No mounted scenario was rerun for that
review-only follow-up.
Validation (2026-09-27 17:52Z): the full library suite passed (187 passed, one
ignored); the focused registry tests passed (3 passed, one ignored); the
release benchmark passed (1 test); `cargo check`, Clippy with `-D warnings`,
formatting, and `git diff --check` passed.

Update (2026-09-27 18:37Z): the new regression reproduced an abort when both
the cancellation waker and warning subscriber panicked; nested catches now
contain both at the real callback, and the test passes. A duplicate-key test
failed before the debug assertion and passes with it. The timer now spans the
earliest worker start through latest worker finish, avoiding a coordinator
timestamp taken before or after barrier release. The release benchmark passed;
its new results are recorded separately from earlier harnesses. No DashMap
capacity hint or token recycling was added: neither cold-start allocation
impact nor benefit is measured, and token reuse would change production
lifecycle cost.
Validation (2026-09-27 18:39Z): the callback regression passed (1/1), the
`interrupt_` filter passed (6 passed, one ignored benchmark), and the release
benchmark passed (1/1). `cargo check -p studiofs-macfuse`, Clippy with
`-D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed. The
mounted scenario was not rerun for this callback and synthetic-benchmark review
follow-up.
