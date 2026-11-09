# Make CLI janitor runs observable

This ExecPlan follows `.agent/PLANS.md` and remains current with implementation.

## Purpose / Big Picture

Operators need to understand whether `studiofs janitor <volume>` is waiting on metadata, a shared lock, S3, or deletion, especially during long object-store calls. The CLI will emit a stable versioned JSONL event stream as well as readable periodic text progress, summarize coverage and work at completion, and preserve the existing fail-closed deletion pipeline. A dry-run will remain read-only across metadata and object storage.

## Progress

- [x] (2026-11-09 20:00Z) Confirmed repository baseline, CLI pipeline, metadata session, dry-run checkpoint behavior, and existing logical transport counters.
- [x] (2026-11-09 21:00Z) Implemented five-second progress events, live scanned/listed/candidate/protection/age/HEAD/DELETE counts, active operation phase, JSONL cycle/final events, and typed process outcomes.
- [x] (2026-11-09 21:00Z) Documented JSONL schema, exit codes, dry-run limits, and provider observability boundaries; focused checks passed.
- [x] (2026-11-09 21:30Z) Rechecked focused behavior and local live PostgreSQL/RustFS acceptance; corrected the ignored lock-busy test and added a local-only live CLI dry-run acceptance test.

## Surprises & Discoveries

- Observation: dry-run opens redb read-only and only persists checkpoints when `!dry_run`; candidate backfill is also disabled. It still acquires and releases a PostgreSQL advisory lock and performs eligibility queries, which are coordination/read operations.
  Evidence: `JanitorMetadataSession::open`, `load_sweep_checkpoint(..., false)`, and the CLI's `!dry_run` guards.
- Observation: OpenDAL provides logical HEAD/DELETE and transport retry/throttle counters, while list page count is not exposed.
  Evidence: `docs/janitor-performance.md` documents `list_pages=unavailable` and clarifies counters can exclude retries.

## Decision Log

- Decision: report only observable counts and mark unavailable values explicitly; do not infer HTTP request counts from logical OpenDAL calls.
  Rationale: accurate diagnostics are more useful than fabricated precision.
  Date/Author: 2026-11-09 / Codex.
- Decision: keep the session-scoped advisory lock during dry-run while suppressing all durable queue/checkpoint/fence writes and remote DELETEs.
  Rationale: the lock prevents a dry-run from racing a destructive sweep and does not persist as metadata state.
  Date/Author: 2026-11-09 / Codex.
- Decision: expose configured S3 request rate and mark actual in-flight requests, queue depth, and measured throughput null because the existing transport/metadata interfaces do not expose them.
  Rationale: no false precision in operator output.
  Date/Author: 2026-11-09 / Codex.

## Outcomes & Retrospective

The standalone CLI now emits five-second human-readable progress or versioned JSONL events. JSONL distinguishes per-cycle work from one final run summary and reports process codes 0, 2, 3, 4, and 5 for complete, partial/cancelled, lock-busy, unsafe metadata, and backend-failure outcomes. Dry-run continues to read metadata and coordinate with a session advisory lock but does not persist queue/checkpoint/fence changes or issue deletes; it reports would-delete separately from confirmed deletes.

Focused progress tests passed 4/4, and `janitor_dry_run_never_sends_delete_requests` passed. The ignored live PostgreSQL lock-busy test initially exposed an outdated success assertion; it now verifies typed exit code 3 and passed against an isolated local PostgreSQL service with zero S3 requests.

Live acceptance used a new `sfs977-verify` Compose project with PostgreSQL 18 and RustFS on dedicated localhost ports and fresh named volumes. The legacy combined cancellation test passed against these services, exercising a PostgreSQL deletion claim, RustFS DELETE, claim finish, lock retention while DELETE was gated, restart behavior, and candidate acknowledgement. The new `live_cli_dry_run_keeps_eligible_candidate_and_object` ignored test uses a unique per-run prefix and a real child CLI process: with one eligible candidate and one canonical object it reports `would_delete=1`, `delete_attempted=0`, and `delete_confirmed=0`; the candidate row and checkpoint/upload/watermark state remain unchanged, and the object remains listed. The same fixture also verifies the process returns code 2 and a `partial` summary when the one-item delete budget is exhausted. The test rejects non-loopback PostgreSQL/S3 endpoints to avoid using `.env` remote services.

The actual CLI process also returned JSONL `unsafe`/exit 4 when the isolated fixture lacked `snapshot_reachability_bootstrap`; after adding the readiness row to that disposable fixture, dry-run completed. Stopping only the isolated PostgreSQL container made the CLI emit `backend_failure`/exit 5; the service was restarted and returned healthy. The first sandboxed full binary run had nine Windows `Access is denied` failures in mount-session and snapshot fixture tests. An identical elevated rerun passed: `cargo test -p studiofs --bin studiofs -- --test-threads=1` reported 521 passed, 0 failed, 5 ignored. This run preceded adding the one ignored live acceptance test; that test was separately run and passed 1/1. This is the full `studiofs` binary unit-test suite only, not every package target or the workspace. `cargo clippy -p studiofs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed after the final test change. Docker containers were stopped with their named volumes preserved.

## Context and Orientation

`crates/studiofs/src/cli/janitor.rs` opens a read-only local metadata view, gates work with a PostgreSQL advisory lock and protection checks, and runs each sweep. `crates/studiofs/src/cli/janitor/pipeline.rs` evaluates queued candidates and remote listings. It already records completed-cycle stats and exposes transport retry/throttle counters. The mounted janitor in `crates/studiofs-vfs` is outside this change.

## Plan of Work

Add a five-second reporter for the active phase and elapsed time, plus a machine-readable `--output jsonl` mode whose first field identifies schema version and event kind. Keep counters limited to completed work and represent unknown in-flight information as null. Ensure the reporter starts before the first metadata request and emits a terminal event after lock release. Preserve existing text output as the default and readable non-TTY behavior.

Inspect and, if needed, tighten the read-only metadata boundary so dry-run never writes queue rows, checkpoints, deletion fences, or object data. Add focused tests that count calls and validate text/JSONL events, including a virtual-time stalled-operation heartbeat test where the existing runtime setup allows it.

Document exact fields and limitations in `docs/janitor-performance.md`. Keep this plan updated with evidence and final validation.

## Concrete Steps

Focused checks: `cargo test -p studiofs --bin studiofs progress_ -- --nocapture`, `cargo test -p studiofs --bin studiofs janitor_dry_run_never_sends_delete_requests -- --nocapture`, and `cargo test -p studiofs --bin studiofs janitor_resumed_object_refreshes_age_and_dry_run_preserves_checkpoint -- --nocapture`. Local live checks use a disposable PostgreSQL/RustFS Compose project and explicit process environment overrides for `STUDIOFS_TEST_PG_DSN`, `STUDIOFS_S3_ENDPOINT`, `STUDIOFS_S3_BUCKET`, `STUDIOFS_S3_ACCESS_KEY`, `STUDIOFS_S3_SECRET_KEY`, and `STUDIOFS_S3_REGION`; do not load or reuse remote `.env` endpoints. Build with `cargo build -p studiofs`, then run the ignored janitor acceptance selections serially. The new dry-run process test additionally requires an isolated `STUDIOFS_CLIENT_ROOT` and fails closed for non-loopback services. The parent turn explicitly authorized `cargo test -p studiofs --bin studiofs -- --test-threads=1`; this binary suite was run, but workspace-wide suites were not.

## Validation and Acceptance

Focused tests must show that a long pending metadata/object operation emits multiple progress events, dry-run performs zero checkpoint/queue/delete writes while allowing documented reads and advisory locking, JSONL output matches the documented versioned schema, and text output remains readable without a TTY. The live acceptance test requires loopback-only PostgreSQL and S3 endpoints and uses a unique disposable object namespace. Run formatting and diff checks, and report behavior that remains environment-gated.

## Idempotence and Recovery

The change is confined to CLI presentation and its dry-run boundary. It does not modify mounted cleanup behavior or persisted schema. The live acceptance test writes one unique object and PG candidate into disposable loopback services, confirms dry-run leaves them unchanged, and removes only that test-owned object and volume row on success. Keep the Compose volumes for evidence; do not point the test at a user or remote volume.

## Artifacts and Notes

The existing pipeline's counters are logical provider operations. `list_pages` remains unavailable through OpenDAL's public `Lister` API.

## Interfaces and Dependencies

The CLI output mode should be an enum accepted by clap, and JSON events should use `serde_json` already present in the crate. No dependency is required.

## Latest verification result: unresolved duplicate dry-run accounting

The final repeated live process test `cargo test -p studiofs --bin studiofs live_cli_dry_run_keeps_eligible_candidate_and_object -- --ignored --nocapture --test-threads=1` failed: `would_delete` was 2 for one eligible queued object also present in the S3 listing (expected 1). Earlier runs passed, but they do not override this final failure. The queue stage and listing stage each reach the dry-run increment of `stats.deleted`; no deletion or queue acknowledgement removes the duplicate observation. The exact regression assertion is retained. No production correction or assertion weakening was made in this verification task. SFS-977 acceptance remains incomplete pending a bounded, correct accounting fix.

The full binary test run passed 521 tests with 5 ignored before this new ignored regression was added. Clippy, formatting and diff checks passed after the final exact-loopback URL guard. Docker containers are stopped and their isolated named volumes remain for diagnosis. No external object storage was used.

## Review fixes: dry-run accounting and progress lifecycle

A separate review against `main` confirmed the duplicate dry-run count. Dry-run now remembers the object ids it has counted as would-delete in the cycle and drops them from later candidate batches, so the listing stage no longer revisits a queued object. The set grows only when `deleted` grows, so the delete cap bounds it. The focused unit regression `dry_run_counts_queued_object_once_when_listing_revisits_it` fails without the fix and passes with it. After a rebuild, the local PostgreSQL/RustFS fixtures passed all four ignored `cli::janitor::tests` live tests, including `live_cli_dry_run_keeps_eligible_candidate_and_object`. Loop mode now reports the exit status of the latest cycle, so a partial result from an earlier cycle no longer persists. The Kilo review's `std::sync::Mutex` warning was rejected: progress locks are held only for field copies, never across an `.await`, and the heartbeat runs on a std thread.

The same review fixed these issues:

- The progress phase returns to `listing` before every listing wait, so a stalled LIST is no longer reported as the previous metadata or delete phase. The phase timer resets only when the phase actually changes.
- The heartbeat thread is joined before the final summary, so no progress event can follow it.
- A failed command prints its cause on stderr before showing the exit label.
- Buffered tracing output is flushed before `process::exit`.
- The heartbeat test no longer underflows `Instant` on hosts that have been up less than 30 minutes.

The following review findings are still open: listing-call and in-flight fields in progress events, a schema file and its validation test, Windows terminal and redirect tests, a dedicated read-only estimate mode, and sign-off on dry-run taking the global lock.
