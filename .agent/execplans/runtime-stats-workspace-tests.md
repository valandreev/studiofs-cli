# Make the requested workspace test commands reliable

This living ExecPlan follows `.agent/PLANS.md`. It was added when investigation
expanded from VFS tests into the CLI crate. Repository: `/home/val/dev/studiofs-cli`,
branch `main`; the working tree was initially clean.

## Purpose / Big Picture


The user reported a missing redb page metric and requested successful completion
of both full workspace test commands, including ignored integration tests. Keep
the mounted filesystem behavior unchanged and correct test expectations or
isolation where the existing implementation already follows its contract.

## Progress


- [x] Trace the page metrics from the virtual stats file into redb diagnostics.
- [x] Replace unconditional live page-metric assertions with deterministic
  available/unavailable sample export coverage; 37 runtime-stats tests passed.
- [x] Verify nine existing metadata diagnostics tests, including writer contention.
- [x] Fix legacy landing recovery's premature cleanup assertion by waiting for
  both readable bytes and retired records; the focused test passed.
- [x] Run VFS cargo check, Clippy with warnings denied, formatting, and diff checks.
- [x] Rebuild the product before native mount tests.
- [x] (2026-10-27 21:10Z) Isolate the ignored CLI missing-session-bus test from
  the host desktop bus; both real tray backend tests passed, including the child.
- [x] CLI cargo check and all-targets Clippy with warnings denied passed.
- [x] Independent read-only review found no confirmed actionable issues.
- [x] Full ignored run passed CLI, S3, Linux FUSE mount, and all 147 metadata tests.
- [x] Full ignored command exited zero, including all 135 VFS integration tests.
- [x] (2026-10-27 21:40Z) Final ordinary workspace command exited zero; all 1257
  VFS unit tests and 55 ordinary VFS integration tests passed.
- [x] Final formatting, diff inspection, and whitespace checks passed.
- [x] (2026-10-27) Second review fixes: legacy recovery test waits on the
  recovery gate instead of hot-polling; tray child test asserts `1 passed`
  and captures child output. Focused tests and Clippy passed.
- [x] (2026-10-27) After rebuilding the product, both full workspace
  commands exited zero: ignored 312 passed, ordinary 3830 passed, 0 failed.

## Surprises & Discoveries


Page gauges are deliberately optional until a successful sample. The redb engine
starts its own periodic durability thread even before VFS workers start; merely
using an unstarted VFS fixture cannot remove contention.

The first normal workspace run encountered a two-second uploader test timeout;
that test passed individually and in the next complete VFS run. That next run
exposed a separate legacy recovery test race: recovered bytes become readable
before landing records are retired. The original assertion failed after seeing
the bytes but before cleanup completed.

The first ignored run failed in
`tray::supported::tests::linux_backend_returns_error_without_a_session_bus`:
the host supplies a working desktop D-Bus session, while this test previously
required its caller to supply an invalid address. D-Bus is the Linux desktop
message bus. Other tray tests need a working bus, so a global environment
override would make their assumptions conflict.

## Decision Log


Preserve nonblocking page sampling and local durability semantics. Move optional
field export checks to fixed diagnostic samples instead of blocking production
diagnostics or synthesizing page counts.

Wait for the full observable recovery result inside the existing bounded loop;
replace its fixed coordination sleep with an executor yield.

Run the negative tray assertion in a child copy of the test binary with a
nonexistent Unix-socket bus address. Use `Command::env`, never global environment
mutation. The child detects the exact supplied address to avoid recursion.

## Context and Orientation


`crates/studiofs-vfs/src/vfs/runtime_stats_surface.rs` exports optional
`LocalStorageDiagnostics` fields. `crates/studiofs-meta/src/redb_engine/gc.rs`
samples pages using a nonblocking writer lock and caches successful samples.
`docs/metadata.md` and `docs/perf/runtime-stats-schema.md` document omission before
the first successful sample. `crates/studiofs-vfs/src/vfs/tests.rs` contains the
stats-file and legacy recovery tests. `crates/studiofs-vfs/src/vfs/sync_ops.rs`
replays legacy landing writes, then removes their records.
`crates/studiofs/src/tray.rs` contains the ignored real Linux tray backend tests.

## Plan of Work


Keep the two VFS test corrections already validated. In the CLI negative tray
test, launch the current test executable with its exact ignored test name and
an invalid `DBUS_SESSION_BUS_ADDRESS`, then assert the child exits successfully.
Inside that child, retain the existing real backend assertion. Update the ignore
reason to describe the isolated backend test. No new dependencies or production
interfaces are needed.

## Concrete Steps


Run all commands from `/home/val/dev/studiofs-cli`. The host, outside the restricted
sandbox, has the configured PostgreSQL/S3 services, `/dev/fuse`, and a non-root
caller with supplementary groups. Integration helpers load the existing `.env`;
never print its values. Retain logs under `target/goal-runtime-stats/`.

    cargo test -p studiofs --bin studiofs linux_backend_ -- --ignored --nocapture --test-threads=1
    cargo check -p studiofs
    cargo clippy -p studiofs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check
    cargo test --workspace --all-targets -- --ignored --nocapture --test-threads=1
    cargo test --workspace --all-targets -- --nocapture

## Validation and Acceptance


The negative tray test must pass while the parent retains its working desktop
bus, with one actual child test executed. Both full commands must exit zero with
nonzero executed tests and no failing target. Report any infrastructure or
platform limitations explicitly; Linux runs cannot establish native Windows or
macOS behavior.

## Idempotence and Recovery


These are test-only corrections. Preserve unrelated edits and existing commits.
Re-run failed focused tests before changing code. Do not disable tests, replace
real assertions with unconditional passes, or change production behavior merely
to accommodate the machine. Test artifacts stay under `target/`.

## Artifacts and Notes


Evidence so far: runtime stats 37 passed; metadata diagnostics 9 passed; legacy
landing recovery 1 passed; VFS check and Clippy exited zero. Complete logs for
initial failures and focused reruns are in `target/goal-runtime-stats/`.
The isolated CLI tray run passed both backend tests while retaining the working
parent desktop bus. Formatting and diff checks passed after the CLI change.
Final `target/goal-runtime-stats/workspace.log` contains 44 successful target
summaries and no failures; `target/goal-runtime-stats/ignored.log` contains 44
successful target summaries plus the successful isolated child test. Both exact
requested Cargo invocations exited zero. Earlier failures are retained in
`workspace-before.log`, `workspace-before-legacy.log`, and `ignored-before-tray.log`
in that directory.

## Interfaces and Dependencies


Use the existing Rust standard-library process API for child environment
isolation, existing Tokio yielding for bounded recovery polling, and existing
runtime stats render/parse methods. Keep filesystem public APIs unchanged.

## Outcomes & Retrospective


Both exact requested commands succeeded on Linux, including live PostgreSQL/S3
and native FUSE checks in the ignored suite. The ordinary run passed all 1257 VFS
unit tests, including the original stats test, the new optional-field test, the
corrected legacy recovery test, and the uploader cancellation test that timed out
in an early diagnostic run. No production behavior or public interfaces changed.
Native Windows/macOS behavior was not exercised on this Linux host.

Revision note: created after the ignored suite exposed a CLI environment
dependency, making this a change across two crates.
Revision note: recorded successful isolated tray tests and their documentation update.
Revision note (2026-10-27 21:40Z): recorded successful completion of both full
workspace commands and final validation; no implementation work remains.
