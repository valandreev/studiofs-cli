# Keep tray polling out of inode diagnostics

This living ExecPlan follows `.agent/PLANS.md`.

## Purpose / Big Picture

Tray polling must not scan every inode in the volume. Health requests should
report current runtime counters and authoritative synchronization safety;
explicit RuntimeSnapshot requests retain ZeroIO lifecycle and inode details.
Idle tray polling should back off without allowing stale status to certify
completion.

## Progress

- [x] (2026-10-25 17:12Z) Read SFS-891 and its comment; traced tray, health,
  runtime snapshot, upload counters, and sync safety. Checkout was clean on
  `feature/sfs-891-tray-polls-diagnostics-every-300-ms-and-each-health_snapshot`.
- [x] (2026-10-25 17:17Z) Added and observed a failing live-source regression,
  then verified it passes after separating Health from explicit ZeroIO details.
- [x] (2026-10-25 17:17Z) Switched tray to Health with idle backoff; added
  legacy compatibility and backoff tests. Independent review requested a
  stronger non-enumeration assertion, now using existing queue snapshot counters.
- [x] (2026-10-25 17:19Z) Passed 42 diagnostic tests, 24 tray tests, the
  live diagnostics launch/attach test, check, Clippy, formatting, and diff checks.
  Independent review has no remaining actionable findings.
- [x] (2026-10-25 17:20Z) Recorded implementation, exact validation commands,
  independent review, and the missing mounted profile in Linear comment
  `0184c89d-206e-4a9b-b388-1807e9cb9e44`; issue remains In Progress.
- [x] (2026-10-25 17:40Z) Addressed the follow-up review of `from_health`;
  observed the expanded structured-snapshot regression fail before correction,
  then passed all 42 diagnostic tests, crate check, Clippy, and hygiene checks.
- [x] (2026-10-25 18:11Z) Addressed the attached polling review: reuse synchronization fields,
  document the Health field, wake parked tray helpers during shutdown, and
  ran focused verification. Recorded all seven recommendations' dispositions.
- [x] (2026-10-25 19:30Z) Branch review against `main` found Health still
  scaling with pending uploads: `storage_stats` probed the cache index once per
  runtime-pinned object, and `pending_summary` walked the queue under its lock.
  Both now read incrementally maintained totals; the tray monitor rechecks
  shutdown before parking.

## Surprises & Discoveries

The issue's additional upload-bytes finding was only partly addressed:
`StudioVfs::upload_stats` used recorded payload lengths, but the same Health
poll reached `write_path_observability_snapshot` → `storage_stats` →
`runtime_pinned_evictable_bytes`, which opened one cache-index read transaction
per runtime-pinned object. Every queued upload holds a pin, so this repeated the
per-pending-op redb pattern from the issue comment. `pending_summary` also
walked every pending operation under the queue lock, twice per Health request.
Only legacy entries with unknown payload lengths still read the cache index.

The current environment has `perf` but no `/dev/fuse`. A real mounted write
profile with an active tray therefore cannot be claimed here.

The existing launch/attach test initially failed because the default client
root is read-only. Setting `STUDIOFS_CLIENT_ROOT` under `target/` resolves that.
The sandbox then rejected the Unix socket connection with EPERM; the same
focused command passed with approved execution outside the sandbox.

## Decision Log

Keep the expensive ZeroIO snapshot on the existing explicit RuntimeSnapshot
request. Remove it from Health rather than returning invented zero counters or
adding a background full-volume scanner. Add defaulted synchronization and
sync-safety fields to Health so the tray keeps its existing safe/unknown logic.
Older health responses must deserialize with unknown sync safety.

Review follow-up: the test-only `DiagnosticsRuntimeSnapshot::from_health`
must also transfer `health.synchronization` intact. Reconstructing it from
top-level fields discarded simulated metadata queues and nested telemetry.
The existing structured-snapshot test now supplies pending/in-flight counts
7/3, 11 open handles, and 13 foreground samples, and compares the entire nested
synchronization response. It failed against the reconstruction before the fix.

Poll every 300 ms while active, booting, or unavailable, and every 750 ms when
idle. Keep the existing two-second snapshot freshness and heartbeat expiry:
750 ms plus the one-second request timeout leaves 250 ms scheduling margin.
This replaces the initial one-second/three-second proposal and preserves the
existing expiry contract with less code. No additional configuration or dependencies.

Runtime pins now record the evictable index bytes they hide. The value is read
once when an object gains its first pin and refreshed after any index mutation
of a pinned object (`put`, `put_batch`, `mark_uploaded`, orphan pruning), all
under the payload mutation gates. Dirty/Uploading transitions cannot change it.
The upload queue keeps payload bytes, unknown-length ids, creation times, and
retry counts in `PendingTotals`, updated at every `by_object` change.
`studiofs status` keeps its one-shot RuntimeSnapshot: switching it to Health
would show default PostgreSQL sync values against older daemons.

## Outcomes & Retrospective

Health no longer invokes ZeroIO; explicit RuntimeSnapshot retains it. All 67
focused tests passed, together with `cargo check -p studiofs`,
`cargo clippy -p studiofs --all-targets -- -D warnings`,
`cargo fmt --all -- --check`, and `git diff --check`. The counter regression
was also checked by temporarily restoring a discarded ZeroIO call: it failed
with counter 4 versus 1, then passed after removing that call. No new VFS or
metadata instrumentation was needed. Mounted profiling remains unavailable;
SFS-891 stays In Progress pending that acceptance evidence.

The attached review follow-up addressed items 1, 5, and 7. Its new join/wakeup
regression failed before the correction with a bounded channel timeout, then
passed with all 25 tray tests. All 42 diagnostics tests, crate check, Clippy,
formatting, and diff checks also passed. Items 2, 3, 4, and 6 have explicit
dispositions below; no discovery retry policy or arbitrary timeout was added.

## Attached Review Disposition

Review item 1 is confirmed: `synchronization_state_snapshot` already collects
file-session, foreground-read, and coherence values. Health should collect that
snapshot once, clone its owned session/read values into the compatibility fields,
and copy coherence. The review's example needs those clones: these session/read
types own strings/vectors, so moving their fields and then the entire snapshot
would not compile. Other required sampling, including sync safety, remains intact.

Item 2 is rejected for the current lifecycle. In `tray.rs`,
`dispatch_mount_with_auto_tray` starts the mount in this same process, passes
`std::process::id()` to its monitor, and sets stop plus sends `MountFinished`
when `mount_args.dispatch()` returns. The event handler exits on `MountFinished`.
`MountRuntimeGuard::serve_foreground` in `cli/mount/session.rs` starts the diagnostics
host once, and cleanup stops it. There is no live replacement or restart of
that host inside this tray lifetime. A restarted process has a different PID,
which `find_local_mount_session` deliberately rejects; clearing discovery would
not reconnect to it. Transient requests to the existing host already retry
using the valid cached discovery. No new reconnection policy is introduced.

Item 3 is not a confirmed defect. Failing closed to Unavailable after the
two-second heartbeat limit is intentional. Health still reads pending work,
mutex-protected telemetry, and local metadata gauges; it is not exclusively
atomic loads. No measured evidence supports halving its timeout to 500 ms.
Changing constants cannot guarantee scheduling latency under CPU starvation.
Keep the existing timeout and expiry invariants.

Item 4 is cosmetic and skipped. Adding PartialEq throughout the VFS/metadata
telemetry types solely to change a test assertion widens this change across
crates without altering behavior. JSON comparison is confined to tests.

Item 5 is accepted as a one-line field description. Item 6 is stale: the
initial working tree at this review was clean, `git ls-files` includes
`docs/plans/sfs-891-tray-health.md`, and `.agent/PLANS.md` does not require an
`.agent/execplans` directory. Preserve the tracked path and existing references.

Item 7 is confirmed for polling pauses. Replace the ticker and monitor sleeps
with `thread::park_timeout`, and unpark each helper before joining it, reusing
the pattern in `MountStatusMonitor::stop_and_join`. The stop flag remains the
condition; spurious wakeups only advance another iteration. A channel-coordinated
test exercises the real join helper and cleans up its worker on failure. This
interrupts polling pauses; it does not promise to cancel an in-flight diagnostics
request before its existing timeout.

## Context and Orientation

At task entry, `crates/studiofs/src/tray.rs::current_activity_status` requested
RuntimeSnapshot every 300 ms. `diagnostics/live_source.rs::runtime_snapshot`
called `health_snapshot`, which invoked `StudioVfs::zeroio_runtime_snapshot`.
That VFS method enumerates lifecycle records, reads their payload graphs, and
sorts per-inode details. These details belong to explicit investigation requests.
The mount already owns synchronization safety independently of these details.

## Plan of Work

Change `diagnostics/types.rs::DiagnosticsHealthSnapshot` to contain synchronization
and sync safety instead of ZeroIO. Build those fields in the live source. Keep
runtime snapshots complete by requesting ZeroIO there explicitly and reusing
the health synchronization fields. Update the test source conversion.

Switch the tray to Health, keeping its status classification unchanged. Choose
the next polling delay from the observed mode, preserving existing expiry.
Update the existing tray tests and add backoff coverage.
Use `ContractVfsFixture` for a live-source regression with actual local inode
lifecycle state. The existing `pending_upload_snapshot_calls` counter must stay
unchanged across repeated Health requests and increase once for explicit
RuntimeSnapshot, which still reports the inode details. This counter is incremented
at entry to the expensive ZeroIO collection path. Update `docs/tray.md`.

## Concrete Steps

From `/home/val/dev/studiofs-cli`, run the live-source regression before and after
implementation. Run `cargo test -p studiofs --bin studiofs diagnostics::`,
`cargo test -p studiofs --bin studiofs tray::`, and the existing
`prepared_mount_launch_publishes_live_diagnostics_and_supports_attach` test.
For that last test, use
`STUDIOFS_CLIENT_ROOT="$PWD/target/sfs-891-client-root" cargo test -p studiofs --bin studiofs prepared_mount_launch_publishes_live_diagnostics_and_supports_attach -- --nocapture`.
Its local Unix socket requires permission to connect outside this sandbox.
Then run `cargo check -p studiofs` and
`cargo clippy -p studiofs --all-targets -- -D warnings`.
Use `cargo fmt --all -- --check` and `git diff --check` for final hygiene.

For the incremental totals, run
`cargo test -p studiofs-cache --lib -- sfs891 local_runtime_pins pending_summary`.
The pinned-bytes regression failed when `storage_stats` re-read pinned rows and
passes with the counter.

## Validation and Acceptance

The live-source regression must fail with the old Health collecting full ZeroIO
and pass once only explicit RuntimeSnapshot collects it. Tray
tests must preserve active, safe, unknown, stale, future-time, and heartbeat
expiry behavior, and show a longer delay only in idle mode. Legacy Health must
default sync safety to unknown. A mounted acceptance profile remains a separate
gate requiring FUSE, a GUI tray session, and the configured storage services;
use the registered studiofs-e2e runner and retain diagnostics under `target/`.

## Idempotence and Recovery

Tests use isolated temporary fixture data under `target/`. Changes affect
diagnostic requests and polling only; filesystem durability and publication
are unchanged. Preserve unrelated changes and inspect only task files.

## Artifacts and Notes

Initial Git status was clean. Linear SFS-891 is already In Progress. The plugin's
advertised `get_agent_context` returned “Tool not found”; the issue and its
comments are readable.

Final successful focused test totals were 42 diagnostics, 24 tray, and 1 launch/
attach. The initial test command used an incomplete exact filter and ran zero
tests; it was corrected and is not counted as evidence. No workspace-wide or
ignored suite was run.

## Interfaces and Dependencies

Reuse existing `VfsSynchronizationStateSnapshot`, `VfsSyncSafetySnapshot`,
`DiagnosticsDataSource`, and `LocalDiagnosticsClient::health`. Health fields
added to the wire response use serde defaults; RuntimeSnapshot keeps its schema.

Plan created after tracing the current code; no new dependencies or VFS mutation
paths are needed.

The user-supplied `AGENTS.md` requires focused validation and autonomous execution
of an action request. Those rules take precedence over the broader suite
instructions in `rust-best-practices/SKILL.md` and
`test-driven-development/SKILL.md`, and the approval handoffs in
`brainstorming/SKILL.md` and `writing-plans/SKILL.md`.

Revision after independent review: use the existing queue snapshot counter to
detect discarded full scans, and preserve the original heartbeat expiry by
choosing a 750 ms idle polling interval.

Final revision records the focused verification, mutation check, and successful
socket test rerun, distinguishing them from the unavailable native mount profile.

Review revision replaces the test-only synchronization reconstruction with a
direct transfer (nine fewer helper lines) and strengthens the existing regression.
Verification for that revision reran
`cargo test -p studiofs --bin studiofs diagnostics:: -- --nocapture` (42 passed),
`cargo check -p studiofs`, and
`cargo clippy -p studiofs --all-targets -- -D warnings`; formatting and diff
checks were rerun after formatting the new fixture values.

Attached-review revision records the disposition of all seven findings before
changing code. It preserves process ownership, protocol compatibility, and
existing freshness limits, and uses the existing park/unpark shutdown pattern.
Validation reran `cargo test -p studiofs --bin studiofs tray:: -- --nocapture`
(25 passed), `cargo test -p studiofs --bin studiofs diagnostics:: -- --nocapture`
(42 passed), `cargo check -p studiofs`,
`cargo clippy -p studiofs --all-targets -- -D warnings`,
`cargo fmt --all -- --check`, and `git diff --check`. Native tray shutdown on
Windows/macOS and the mounted performance profile remain environment-gated.
