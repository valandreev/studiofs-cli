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

## Surprises & Discoveries

The issue's additional upload-bytes finding is already addressed for normal
uploads: `StudioVfs::upload_stats` uses recorded payload lengths from
`PendingUploadSummary`. Only legacy entries with unknown lengths read the cache
index. Queue summaries and write-path metrics still scale with pending work,
which is the dirty-set bound allowed by the issue.

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

## Outcomes & Retrospective

Health no longer invokes ZeroIO; explicit RuntimeSnapshot retains it. All 67
focused tests passed, together with `cargo check -p studiofs`,
`cargo clippy -p studiofs --all-targets -- -D warnings`,
`cargo fmt --all -- --check`, and `git diff --check`. The counter regression
was also checked by temporarily restoring a discarded ZeroIO call: it failed
with counter 4 versus 1, then passed after removing that call. No new VFS or
metadata instrumentation was needed. Mounted profiling remains unavailable;
SFS-891 stays In Progress pending that acceptance evidence.

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
