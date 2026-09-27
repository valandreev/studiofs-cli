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

## Surprises & Discoveries

- The registry is used only by asynchronous cancellable reads. Registration is
  in `StudioVfsAdapter::queue_read_reply`; reply, take, and drop paths all clear
  it through `Request::clear_interrupt_token`.
- `request_interrupted` currently calls `CancellationToken::cancel()` while
  holding the process-wide map mutex. The new lookup must clone the token while
  protected and cancel after releasing the map guard.
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

## Outcomes & Retrospective

Completed. The global mutex map is now a `DashMap`, and callback cancellation
uses a cloned token after the map guard is dropped. The low-level characterization
tests pass, the full macFUSE library test run passed 185 tests with one ignored
benchmark, `cargo check`, Clippy, formatting, and diff checks passed, and the
registered native macFUSE concurrent read/overwrite scenario passed with
cleanup for both binaries. The release benchmark improved eight-worker
throughput by 218.0%; the one-worker result regressed by 21.7%. Mounted scenario
durations were 9,192 ms for the mutex and 8,466/9,218 ms for `DashMap`, so no
mounted performance gain is established. The benchmark isolates registry
cycles, and the mounted scenario does not expose interrupt callback counts, so
no claim is made about end-to-end cancellation frequency.

## Context and Orientation

`crates/studiofs-macfuse/src/low_level.rs` owns the low-level request wrapper
and C interrupt callback. `Request::register_interrupt_token` inserts an
integer key before registering `request_interrupted` with macFUSE.
`Request::clear_interrupt_token` unregisters that callback before removing its
key. `Request::take_raw` and `Drop` both use the same cleanup method.
`crates/studiofs-macfuse/src/adapter.rs` registers the read cancellation token
from `StudioVfsAdapter::queue_read_reply`.

The registry change is limited to synchronization for those entries. It must
not alter read admission, local-first I/O, FFI callback data, request ownership,
or shutdown state. A callback lookup returns a `CancellationToken` clone; the
map guard must be gone before `cancel()` runs. No new crate dependency is
needed.

## Plan of Work

The low-level tests characterize cancellation, removal, and callback/removal
races. The ignored release benchmark recorded the mutex baseline before the
map change. The eight-worker result showed measurable contention, so the
registry now uses the existing `DashMap`. Registration still inserts before
FFI callback setup, callbacks clone the token before releasing the entry guard
and cancelling, and teardown still unregisters FFI before map removal. The same
tests and benchmark passed after the change; crate checks and the registered
native macFUSE concurrent-read scenario also passed.

## Concrete Steps

Run from `/Volumes/Storage/dev/studiofs`:

    cargo test -p studiofs-macfuse --lib interrupt_registry -- --nocapture
    cargo test -p studiofs-macfuse --release --lib interrupt_registry_contention_benchmark -- --ignored --nocapture --test-threads=1
    cargo test -p studiofs-macfuse --lib
    cargo check -p studiofs-macfuse
    cargo build -p studiofs --release
    target/debug/studiofs-e2e run collision-overwrite-vs-read --studiofs-bin target/release/studiofs --platform macos-fuse --infra compose --retain always --run-id sfs665-macos-read-20260927 --seed 665
    cargo fmt --all -- --check
    cargo clippy -p studiofs-macfuse --all-targets -- -D warnings
    git diff --check

Run the exact benchmark command once before and once after the map change. Save
the worker counts, operations per second, OS/macFUSE versions, and command in
`docs/perf/sfs-665-macfuse-interrupt-registry.md`. Use `cargo run -p
studiofs-e2e -- list` to select an existing scenario before attempting any
mounted validation; do not invent a runner scenario name.

## Validation and Acceptance

The lifecycle tests must pass with a nonzero test count and prove callback
cancellation, removal behavior, and race safety. The release benchmark must
exercise concurrent insert, callback lookup/cancel, and removal on the same
registry path before and after the change. Report measured throughput and
percentage change for matching worker counts; claim a reduction only if the
measurement supports it.

The native mounted result is separate from the in-process benchmark. The same
registered `collision-overwrite-vs-read` scenario passed with the clean-HEAD
mutex binary and twice with the `DashMap` binary. Scenario durations were
9,192 ms, 8,466 ms, and 9,218 ms; the overlap does not support an end-to-end
performance claim. Reports are retained in the three corresponding
`target/e2e-runs/sfs665-macos-read*` directories, and no mount remained after
cleanup. The callback behavior is proved by the low-level tests, and the
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
private to `low_level.rs`; the only collection dependency for the change is the
workspace's existing `dashmap` crate. Do not add configuration, metrics, public
API, or teardown state for this optimization.

Plan update (2026-09-27 16:01Z): recorded before/after benchmark results and the
first native mounted run. Update (2026-09-27 16:11Z): added the clean-HEAD
mounted baseline and repeat optimized run; this records the overlapping native
timings and avoids claiming an end-to-end gain that the scenario did not show.
