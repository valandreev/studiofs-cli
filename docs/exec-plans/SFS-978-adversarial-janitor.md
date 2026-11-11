# Prove standalone Janitor payload safety

This ExecPlan is a living document and must remain aligned with `.agent/PLANS.md`.

## Purpose / Big Picture

SFS-978 makes accidental deletion of a referenced, snapshot-pinned, or uploading
payload observable as a failing test. The standalone `studiofs janitor` command
must consult shared PostgreSQL ownership even when its local redb metadata is
older. Tests must also distinguish an unsuccessful DELETE response from proof
that the server did not delete anything.

## Progress

- [x] (2026-11-09) Recorded clean `main` baseline at `782e9c26`; created branch `test/sfs-978-adversarial-janitor`.
- [x] (2026-11-09) Read Linear issue, canonical IO contract, architecture, coding guide, and existing Janitor tests. Verified graph coverage against current sources.
- [x] (2026-11-09) Add deterministic writer/claim and ambiguous DELETE regressions with payload-presence assertions (2 passed).
- [x] (2026-11-09) Add five disposable Docker PostgreSQL/RustFS process tests with payload bytes and authoritative ownership checks (5 passed).
- [x] (2026-11-09) Run focused tests, Clippy, formatting and diff checks; record unavailable service/platform proof.
- [x] (2026-11-10) Review fixes: probe a free loopback port window per stack, move the tests from `include!` into real modules under `cli/janitor/tests/` so `cargo fmt` checks them, stop blocking runtime workers while waiting on CLI children, check ownership after the PG restart, and remove duplicated harness code (2 + 5 passed again).
- [x] (2026-11-11) Run the disposable selection on Windows Docker: 6 passed in 92 s (Windows 11, Docker 29.9.3; SFS-981 release gate).
- [ ] Exercise Windows console Ctrl+C delivery (only Unix SIGINT and hard kill are covered).
- [ ] Exercise CLI vs mounted background Janitor lock contention and a live mounted writer (only CLI vs CLI and seeded PG rows are covered).
- [x] (2026-11-10) PR #974 review: tie the claim-fence barrier to the advisory-lock holder blocked by the fixture's own `FOR UPDATE` backend, and re-check that fence after SIGINT instead of relying on one exit poll (3 process tests passed). `Child::kill` already returns `Ok` for an exited child on the pinned Rust 1.95.

## Surprises & Discoveries

The issue's initial description predates the current implementation. Existing
CLI tests already cover conservative age handling, out-of-order listings,
canonical legacy keys, budgets, resumable scans, cancellation, 429/503 retries,
and lock release. PostgreSQL tests already cover snapshot lifecycle/leases,
claim fencing and concurrent snapshot creation. They are useful focused proof,
but do not establish process-level payload survival on disposable PG/S3.

Codebase Memory's November 8 generation reports an outdated CLI file and no
tracked pipeline file. Direct source inspection is required for these paths.
The actual execution host reports Darwin; Windows acceptance cannot be claimed
from this host. Initially Docker's configured OrbStack socket was absent.
Starting OrbStack restored Docker 29.8.2; live tests then ran with sandbox
escalation for access to that local socket.

The existing live fixture generated a non-UUID volume identity. Real snapshot
capture rejected it. The shared test helper now generates a UUID v4, and the
previously failing concurrent snapshot scenario passed.

## Decision Log

Reuse `DockerComposeStack` from `studiofs-e2e` and `docker/compose.e2e.yml`.
Its generated project name, bounded readiness waits,
logs and project-scoped volume teardown avoid another container harness.
The harness probes a free 16-port loopback window per run, because a fixed
`--port-range` always binds its first ports and concurrent stacks would collide.
The new tests must construct credentials and endpoints from their own stack;
they must never select resources through `.env` or inherited service settings.

Keep deterministic failures in test adapters. A payload map records actual
presence, so the test can simulate a server removing bytes before its response
is lost. The production Janitor pipeline remains the system under test.

## Outcomes & Retrospective

The new suite passed five real CLI/PostgreSQL/RustFS tests on macOS. The matrix
checks current references, uploading/uploaded objects, active and expired
reconciliation ownership, creating/ready/deleting snapshots, an active deleting
snapshot mount lease, untracked payloads and legacy upload-row queue backfill.
It verifies every canonical object and fragment key (legacy key formats remain
covered by the existing unit test), the exact one-object delete cap, unchanged
durable dry-run state, final queue acknowledgements and released claim owners.

The process cases check rejection of stale metadata, PG outage/restart/retry,
hard kill while the CLI waits at the claim fence, Unix SIGINT drain, competing CLI
lock exclusion, and persisted discovery resumption with a one-entry scan budget.
Two real snapshot captures queue on the held CLI lock, complete after the CLI is
killed, and preserve pinned payload bytes after current references are removed.

Two new deterministic tests prove writer/claim interleaving and retry after a
server-applied DELETE whose response is lost. Four existing S3 transport tests
passed for retry/throttle behavior. Existing age/order and legacy/noncanonical
tests passed individually. Clippy, formatting and diff checks passed.

SFS-978 remains In Progress because Windows Docker, Windows console Ctrl+C and
a live mounted writer/Janitor were not exercised. Shared ownership rows and lock
protocols are tested without a mounted process. Real-provider semantics remain
an opt-in verification boundary.

## Context and Orientation

`crates/studiofs/src/cli/janitor.rs` owns CLI startup, metadata safety validation,
global lock lifetime, cancellation and output. Its private `pipeline.rs` runs
candidate and object-listing work, obtains a per-object deletion claim, deletes
canonical keys, and finishes that claim. A claim is a PostgreSQL ownership row
with a fencing token: a superseded owner cannot acknowledge another owner's
work. A checkpoint records a safe scan position for another invocation.

The existing test module has metadata fakes, live fixture seeding and an S3
bucket-create helper. New child modules under `cli/janitor/` reuse them rather
than adding production exports. `crates/studiofs-meta/src/pg_engine/tests/janitor.rs`
contains complementary shared-metadata regressions.

## Plan of Work

The first milestone adds `tests/adversarial.rs` as a child of the current test module, exercising a
writer activated between listing and claim and DELETE response loss followed
by replay. The second milestone adds `tests/adversarial_live.rs` with a uniquely named disposable
Compose stack, actual CLI subprocesses, shared PG protection rows and S3 byte
checks. The third milestone adds `tests/adversarial_live/process.rs` as a child of
that live module for termination, concurrent snapshot cuts and resumable process runs.
Wire the files into the existing test module and add only the existing
workspace E2E crate as a dev dependency. Document exact commands and coverage
limits in `docs/tests.md`.

## Concrete Steps

From `/Volumes/Storage/dev/studiofs`, run focused selections:

    cargo test -p studiofs --bin studiofs sfs978_ -- --nocapture
    cargo build -p studiofs --bin studiofs
    cargo test -p studiofs --bin studiofs live_cli_disposable_ -- --ignored --nocapture --test-threads=1
    cargo test -p studiofs --bin studiofs cli::janitor::tests::s3_ -- --nocapture
    cargo test -p studiofs --bin studiofs listing_timestamps_decide_object_age_conservatively_without_head -- --nocapture
    cargo test -p studiofs --bin studiofs legacy_keys_are_cleaned_and_non_canonical_keys_are_never_touched -- --nocapture
    cargo clippy -p studiofs --all-targets -- -D warnings
    cargo fmt --all -- --check
    git diff --check

The Docker selection requires Docker Compose and healthy disposable PostgreSQL
18/RustFS containers. It must fail when prerequisites are unavailable, never
silently pass or redirect to configured remote services. Do not run workspace
or full ignored suites; the user has prohibited them.

## Validation and Acceptance

The deterministic tests must preserve every protected key and leave failed
work recoverable. A retry after server-applied DELETE must treat missing keys
idempotently and acknowledge only completed work. Live process tests must read
protected S3 bytes before and after cleanup, verify PG ownership state, and
delete only eligible canonical payloads. Dry-run must retain both data and
durable queue/checkpoint/claim state. Budgets must bound confirmed deletions
and leave a later invocation able to finish.

Windows Docker execution and mounted writer behavior are separate acceptance
boundaries; seeded shared-state rows alone do not prove mounted IO or a real
writer process. Record any remaining cases explicitly rather than calling
the entire issue complete.

## Idempotence and Recovery

Every live run uses fresh Compose resources, local metadata under `target/`,
and its own object namespace. Teardown removes only the generated Compose
project and its named volumes. Preserve diagnostics under `target/` on failure.
Every child process must have a bounded wait and be killed/reaped on timeout.
No existing volume, `.env`, mounted background worker, or production schema is
modified. The fixture initializes the existing schema in its new database.

## Artifacts and Notes

Initial Docker preflight failed because
`/Users/val/.orbstack/run/docker.sock` does not exist. This is an execution
boundary, not a test pass. After starting OrbStack, the five-test Docker selection
reported `5 passed; 0 failed` in 49.19 seconds. The final expanded fragment checks
for the stale/outage test also passed separately (1/1 in 13.25 seconds). Logs and
CLI stdout/stderr remain under `target/janitor-adversarial/`; Compose resources
were torn down.

## Interfaces and Dependencies

Reuse `JanitorCycleContext`, `JanitorMetadataSource`, `JanitorObjectStore`,
`DockerComposeStack`, and the existing CLI S3 signer. No new production API,
failpoint feature, or third-party dependency is needed.

Plan created after reconciling the issue with existing focused coverage.
Updated after live execution to include process barriers, concurrent snapshot
capture, bounded checkpoint resumption, final payload/ownership checks, and
the remaining Windows/mounted acceptance boundary.
Updated 2026-11-10 after a two-axis review against `main`: harness and
documentation corrections only; remaining acceptance gates are unchecked above.
