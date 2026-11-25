# Milestone 04: Write Safety And Activation

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestones 01–03 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md`, `.agent/execplans/SFS-1001-m02-pg-compat-foundation.md`, `.agent/execplans/SFS-1001-m03-data-format-evolution.md`).

## Purpose / Big Picture

After this milestone the database itself rejects writes and deletions from a binary that lacks a capability the volume requires, so safety does not depend on every client behaving well. Mounts react to compatibility decisions as part of their normal lifecycle, an operator can turn a new feature on step by step (Expand, Migrate, Activate, Contract), and a report shows which clients and volumes are ready before anything changes.

## Progress

- [ ] SFS-1014: fence incompatible event publication atomically in PostgreSQL with the publication predicate plus a trigger keyed on a session GUC.
- [ ] SFS-1015: integrate compatibility decisions into mount and write-authority lifecycle, including policy epoch revalidation after reconnect.
- [ ] SFS-1016: guard Janitor, GC, snapshot cleanup and other destructive maintenance by capability.
- [ ] SFS-1017: explicit Expand–Migrate–Activate–Contract feature cutover workflow.
- [ ] SFS-1018: operator compatibility report, preflight and migration documentation.

## Surprises & Discoveries

- Observation: only Janitor deletion claims set a capability GUC today; snapshot cleanup and event-log or checkpoint pruning set none.
  Evidence: `crates/studiofs-meta/src/pg_engine/janitor.rs:1089`.
- Observation: `studiofs_fence_legacy_deletion_claim` checks `metadata_format_version >= 6` and compares the GUC with the constant `'6'`, so a 0.2.3 or 0.2.4 Janitor passes it on a format 7 volume; only the app-side format check fences it. SFS-1016 (with SFS-1132) must make the trigger compare the GUC with the volume's current format or maintenance capability.
  Evidence: `crates/studiofs-meta/src/pg_engine/schema.rs:574`, `crates/studiofs-meta/src/pg_engine/schema.rs:575`, `crates/studiofs-meta/src/pg_engine/janitor.rs:571`.

## Decision Log

- Decision: fencing uses the existing publication predicate plus a trigger reading a transaction GUC; no separate PostgreSQL roles.
  Rationale: preflight decision of 2026-11-25; it follows the existing `studiofs_fence_legacy_deletion_claim` pattern.
  Date/Author: 2026-11-25, project preflight.

## Outcomes & Retrospective

Not started.

## Context and Orientation

A "GUC" is a PostgreSQL setting that a session or transaction sets with `set_config(name, value, is_local)`; a trigger can read it with `current_setting(name, true)`. The existing example: the Janitor runs `CLAIM_PUBLICATION_FENCE_SQL` (`crates/studiofs-meta/src/pg_engine/janitor.rs:1089`), which sets `studiofs.deletion_format = '6'`, and the trigger function `studiofs_fence_legacy_deletion_claim` (`crates/studiofs-meta/src/pg_engine/schema.rs:569`) raises an error for a deletion claim on a format 6 volume without that setting. Publication bumps `metadata_state.version` with `BUMP_METADATA_VERSION` (`crates/studiofs-meta/src/pg_engine/sql.rs:51`) only when the format matches.

"Write authority" is the state in which a mount may publish its local journal; "lifecycle" is the mount's progression from local serving through catch-up to authority, described in `docs/pg-sync-contract.md`. The Expand–Migrate–Activate–Contract steps are defined in ADR 0018. Read the "Baseline obligations" table and the "Multi-volume databases" section of `docs/persisted-format-compatibility.md`; every obligation marked missing or partial and owned here must be closed.

## Plan of Work

SFS-1014 makes every publication transaction set session and policy epoch GUCs before the version bump, and installs a trigger on the event log that rejects rows without a matching epoch or capability; the trigger body branches on per-volume state so one database can hold volumes at different policies. SFS-1015 maps evaluator decisions to lifecycle states: refused publication keeps local work and reports `UpgradeRequired`; replay refusal stops at the last applied version; reconnect revalidates the epoch before the first publication or replay batch. SFS-1016 adds a maintenance GUC to every deletion path and a guard for each. SFS-1017 adds CLI steps for the cutover, refusing Activate while incompatible sessions are live unless the operator accepts fencing, and refusing a database-wide Contract while any volume needs the object. SFS-1018 adds the report and operator documentation.

## Concrete Steps

From the repository root, with PostgreSQL available as described in `docs/tests.md`:

    cargo test -p studiofs-meta --lib pg_engine -- --ignored fence   (environment-gated)
    cargo test -p studiofs-vfs --lib lifecycle
    cargo clippy -p studiofs-meta -p studiofs-vfs -p studiofs --all-targets -- -D warnings

## Validation and Acceptance

A publication without the required GUC is rejected by PostgreSQL even when issued by raw SQL. A deletion without the maintenance GUC is rejected. A mount whose volume is activated for a capability it lacks keeps serving reads (when read-compatible), refuses publication with a typed reason, and keeps its journal. The cutover CLI refuses Activate while an incompatible session holds a live lease.

## Idempotence and Recovery

Activate and Contract are single transactions and retryable. Trigger installation is part of a registered migration from milestone 02 and needs every volume in the database to be at a policy the trigger body understands.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1014, https://linear.app/valandreev/issue/SFS-1015, https://linear.app/valandreev/issue/SFS-1016, https://linear.app/valandreev/issue/SFS-1017, https://linear.app/valandreev/issue/SFS-1018.

## Interfaces and Dependencies

No synchronous remote publication may be added to `write`, `flush`, `fsync` or release callbacks (`AGENTS.md`). The 0.2.7 adapter must provide an equivalent atomic conditional version advance and per-operation capability stamp.
