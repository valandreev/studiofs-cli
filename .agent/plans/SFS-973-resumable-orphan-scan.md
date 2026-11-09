# Add resumable CLI orphan discovery

This ExecPlan is a living document governed by `.agent/PLANS.md`.

## Purpose / Big Picture

Repeated bounded `studiofs janitor` invocations must eventually cover the full remote object namespace without starting every invocation at key zero. The command will persist a versioned scan checkpoint under the existing per-volume PostgreSQL janitor lock, resume from safe lexical boundaries where OpenDAL reports `start_after`, and use deterministic prefix subdivisions when it does not. `--full-scan` starts a complete pass at the beginning; `--reset-scan` deliberately clears saved scan state before the ordinary bounded pass.

## Progress

- [x] (2026-11-09 12:08Z) Read root/scoped contracts, janitor source, issue requirements, and current graph coverage.
- [x] (2026-11-09 12:08Z) Confirmed current OpenDAL S3 capability advertises `list_with_start_after`; the adapter has not yet exposed it.
- [x] Add repeatable checkpoint DDL and a fail-closed versioned metadata API scoped to volume/storage/listing identity.
- [x] Add safe continuation and capability-based fallback subdivision to CLI listing, including explicit restart flags.
- [x] Add focused nonzero tests for restart, scope invalidation, split/resume, and reset/full-scan semantics.
- [x] Update user documentation and finish focused checks.

## Surprises & Discoveries

- Observation: the scanner groups canonical legacy/fragment keys by the full 16-hex object ID and can stop with the current object incomplete.
  Evidence: `discover_object` and `run_one_cycle` in `crates/studiofs/src/cli/janitor/pipeline.rs` retain one current object until the next object or listing EOF.
- Observation: OpenDAL 0.55 exposes both `FutureLister::start_after` and the per-operator `list_with_start_after` capability; S3 advertises the capability.
  Evidence: installed OpenDAL source and S3 backend capability declaration; the CLI currently calls `lister_with(prefix).recursive(true)` without a cursor.
- Observation: the PostgreSQL Janitor uses a dedicated read-only/safety session and canonical DDL is centralized in `crates/studiofs-meta/src/pg_engine/schema.rs`.
  Evidence: `JanitorMetadataSession::open` and `studiofs_pg_schema_sql`; do not add implicit runtime DDL or a general migrator.

## Decision Log

- Decision: store the last listed key plus the unfinished object ID/key, never a provider continuation token.
  Rationale: start-after is stable across process restarts and token expiry; persisted partial groups are statted across all canonical keys before age decisions.
  Date/Author: 2026-11-09 / Codex.
- Decision: bind persisted state to volume UUID, endpoint, bucket, normalized prefix, strategy, and schema version; reject unknown versions and reset position for a changed supported scope.
  Rationale: a cursor from another storage namespace must never skip keys in this one, while future schema data must not be overwritten by an older binary.
  Date/Author: 2026-11-09 / Codex.
- Decision: for an adapter without start-after, split an oversized canonical prefix into deterministic hexadecimal child prefixes and persist the next traversal prefix; cap subdivision at the exact 16-hex object ID, whose legacy/fragment key set is bounded by the canonical format.
  Rationale: restarting one oversized shard would starve later keys; descending the canonical key trie makes progress without provider tokens.
  Date/Author: 2026-11-09 / Codex.
- Decision: checkpoint only after listing-produced candidates admitted for work have completed and all buffered candidates at that safe boundary have drained.
  Rationale: failures and interrupted object groups replay work instead of skipping objects.
  Date/Author: 2026-11-09 / Codex.

## Outcomes & Retrospective

Implementation includes durable partial-object continuation, conservative age refresh, prefix-trie fallback, explicit restart flags, and checkpoint schema/API. Real PostgreSQL restart/scope/version/reset coverage passed. Focused checks are recorded below; no known failing check remains.

## Context and Orientation

`crates/studiofs/src/cli/janitor.rs` owns command options, lock/protection lifecycle, cycle reporting, and metadata source adaptation. `crates/studiofs/src/cli/janitor/pipeline.rs` streams object listings, groups keys into canonical object candidates, applies budgets, and processes candidates. `crates/studiofs-data/src/janitor.rs` owns OpenDAL listing and capability details. `crates/studiofs-meta/src/janitor.rs` exposes the CLI metadata contract, while `crates/studiofs-meta/src/pg_engine/janitor.rs` uses the dedicated PostgreSQL advisory-lock connection. `crates/studiofs-meta/src/pg_engine/schema.rs` is the repeatable DDL source.

A checkpoint is one durable row that says which safe key/prefix should be visited next for one volume/storage identity. `start_after` means the provider returns keys lexically greater than that key. An unfinished canonical object group is checkpointed only alongside its object ID and canonical key. Its full canonical key set is statted again before age decisions; an unprocessed completed candidate batch rolls back the cycle checkpoint.

## Plan of Work

The implementation adds one versioned checkpoint table to the canonical repeatable DDL and typed load/save/reset methods to the janitor metadata session. The table records the schema version, volume UUID, endpoint, bucket, normalized prefix, listing strategy, and cursor/frontier. Missing schema, malformed rows, and unsupported future versions fail closed. A supported scope mismatch resets to the beginning.

Expose OpenDAL's start-after capability and a cursor-aware lister through the data adapter. The CLI strategy uses the safe processed-key cursor when available. When unavailable, it scans deterministic hexadecimal key-prefix shards; a shard that reaches the entry/time budget before EOF is replaced by its child prefixes, with the exact 16-digit object-ID prefix as the indivisible unit. The scanner tracks only processed boundaries and writes state after candidate draining. True EOF wraps to the beginning so new keys inserted behind a prior cursor are eventually rediscovered.

Add `--full-scan` and `--reset-scan` with explicit conflict/precedence behavior, include checkpoint position and partial/completed coverage in output, and document recovery and the required schema ensure step. Focused tests use fake listers/metadata to prove repeated bounded calls advance, incomplete groups replay, scope changes reset, unknown versions fail, and EOF wraps.

## Concrete Steps

From `E:\dev\studiofs-cli`, run targeted tests and checks only:

    cargo test -p studiofs --bin studiofs janitor -- --nocapture
    cargo test -p studiofs-data janitor -- --nocapture
    cargo test -p studiofs-meta --lib janitor_ -- --nocapture
    cargo fmt --all -- --check
    cargo check -p studiofs
    cargo clippy -p studiofs --all-targets -- -D warnings
    git diff --check

Do not run workspace-wide or full ignored suites. PostgreSQL/S3 integration checks require disposable services and are not proven by unit tests.

## Validation and Acceptance

Repeated bounded invocations with a multi-prefix fake listing must eventually visit every canonical object ID; a crash or scan-budget stop before an object group closes must replay it. A fully drained object batch can advance the cursor, and true EOF resets it for the next complete cycle. Unsupported listing capability must subdivide and advance instead of restarting an oversized prefix forever. A changed supported scope resets to the first key; an unknown schema version refuses checkpoint mutation. `--reset-scan` clears durable progress, while `--full-scan` starts this pass at zero and preserves progress if its own configured budget interrupts it.

## Idempotence and Recovery

Checkpoint writes occur after remote operations and processing complete. If the process stops before the write, the prior cursor remains and already completed objects can be seen again; existing deletion fencing and idempotent not-found handling make that replay safe. Scope mismatch invalidates position. Unknown schema state is never overwritten. Reset affects only scan progress, not object data or GC candidate state.

## Artifacts and Notes

Implementation and validation evidence is recorded below.

## Interfaces and Dependencies

The public metadata surface should provide typed checkpoint load/save/reset methods under `JanitorMetadataSession`. The data adapter should expose `supports_start_after()` and `list_after(prefix, Option<&str>)`. `JanitorCycleContext` should receive the current checkpoint and return the next durable boundary only after candidate work drains. Reuse OpenDAL already present in the workspace; add no dependency.

Plan revision note (2026-11-09): refined fallback from a single restartable 4-hex shard to recursive canonical-prefix subdivision after review identified starvation when a shard itself exceeds a run budget.

Validation evidence (2026-11-09):

- `cargo check -p studiofs`: passed.
- `cargo test -p studiofs --bin studiofs janitor -- --nocapture`: 37 passed, 1 ignored. New tests cover restart with a one-entry budget across fragments, capped EOF replay, and dense fallback-prefix progression.
- `cargo test -p studiofs-meta --lib pg_janitor_sweep_checkpoint_restart_scope_dry_run_and_version -- --ignored --nocapture`: 1 passed against configured PostgreSQL. Tests reopen persisted state, each scope dimension, read-only scope invalidation, reset, malformed state, and unknown versions rejecting writes/reset.
- `cargo test -p studiofs-meta --lib janitor -- --nocapture`: 1 passed, 9 ignored; the new service test was run separately above.
- First data test attempt exposed a missed public listing caller; restored `list(prefix)` and introduced `list_after(prefix, cursor)` to preserve it.
- `cargo test -p studiofs-data janitor -- --nocapture`: 1 unit passed; live-S3 integration compiled but ignored.
- `cargo clippy -p studiofs --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
- No full suites, mounted E2E, or live S3 run. Shared canonical schema deployment remains coordinated with SFS-980; no duplicate migrator was added.

Plan revision (2026-11-09): capped batches restore the prior checkpoint, including at EOF. A listing budget stop is distinguished from an incomplete processing batch so completed work can still advance. Loop restart flags apply only to the first successful cycle. Adjacent duplicate keys remain accepted; backward listing order fails closed.
