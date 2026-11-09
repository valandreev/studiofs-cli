# Make CLI janitor runs observable

This ExecPlan follows `.agent/PLANS.md` and remains current with implementation.

## Purpose / Big Picture

Operators need to understand whether `studiofs janitor <volume>` is waiting on metadata, a shared lock, S3, or deletion, especially during long object-store calls. The CLI will emit a stable versioned JSONL event stream as well as readable periodic text progress, summarize coverage and work at completion, and preserve the existing fail-closed deletion pipeline. A dry-run will remain read-only across metadata and object storage.

## Progress

- [x] (2026-11-09 20:00Z) Confirmed repository baseline, CLI pipeline, metadata session, dry-run checkpoint behavior, and existing logical transport counters.
- [x] (2026-11-09 21:00Z) Implemented five-second progress events, live scanned/listed/candidate/protection/age/HEAD/DELETE counts, active operation phase, JSONL cycle/final events, and typed process outcomes.
- [x] (2026-11-09 21:00Z) Documented JSONL schema, exit codes, dry-run limits, and provider observability boundaries; focused checks passed.

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

Focused progress, dry-run delete suppression, and resumed dry-run checkpoint-preservation tests passed. `cargo check`, package Clippy, formatting, and diff checks passed. Service-backed exit-code behavior was not exercised against live PostgreSQL/S3; CLI mapping is covered by the typed boundary and compile/test checks.

## Context and Orientation

`crates/studiofs/src/cli/janitor.rs` opens a read-only local metadata view, gates work with a PostgreSQL advisory lock and protection checks, and runs each sweep. `crates/studiofs/src/cli/janitor/pipeline.rs` evaluates queued candidates and remote listings. It already records completed-cycle stats and exposes transport retry/throttle counters. The mounted janitor in `crates/studiofs-vfs` is outside this change.

## Plan of Work

Add a five-second reporter for the active phase and elapsed time, plus a machine-readable `--output jsonl` mode whose first field identifies schema version and event kind. Keep counters limited to completed work and represent unknown in-flight information as null. Ensure the reporter starts before the first metadata request and emits a terminal event after lock release. Preserve existing text output as the default and readable non-TTY behavior.

Inspect and, if needed, tighten the read-only metadata boundary so dry-run never writes queue rows, checkpoints, deletion fences, or object data. Add focused tests that count calls and validate text/JSONL events, including a virtual-time stalled-operation heartbeat test where the existing runtime setup allows it.

Document exact fields and limitations in `docs/janitor-performance.md`. Keep this plan updated with evidence and final validation.

## Concrete Steps

From `E:\dev\studiofs-cli`, run `cargo test -p studiofs --bin studiofs progress_ -- --nocapture`, `cargo test -p studiofs --bin studiofs janitor_dry_run_never_sends_delete_requests -- --nocapture`, `cargo test -p studiofs --bin studiofs janitor_resumed_object_refreshes_age_and_dry_run_preserves_checkpoint -- --nocapture`, `cargo check -p studiofs --bin studiofs`, `cargo clippy -p studiofs --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check`. Do not run the full test suite.

## Validation and Acceptance

Focused tests must show that a long pending metadata/object operation emits multiple progress events, dry-run performs zero checkpoint/queue/delete writes while allowing documented reads and advisory locking, JSONL output matches the documented versioned schema, and text output remains readable without a TTY. Run formatting and diff checks. Report any service-backed behavior that is not exercised.

## Idempotence and Recovery

The change is confined to CLI presentation and its dry-run boundary. It does not modify mounted cleanup behavior or persisted schema. Re-running tests uses isolated fakes and does not access a live volume.

## Artifacts and Notes

The existing pipeline's counters are logical provider operations. `list_pages` remains unavailable through OpenDAL's public `Lister` API.

## Interfaces and Dependencies

The CLI output mode should be an enum accepted by clap, and JSON events should use `serde_json` already present in the crate. No dependency is required.
