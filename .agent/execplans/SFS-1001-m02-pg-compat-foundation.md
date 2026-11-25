# Milestone 02: PostgreSQL Compatibility Foundation

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestone 01 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md`) for the identity types and the evaluator.

## Purpose / Big Picture

The shared PostgreSQL database holds the schema that every StudioFS client and maintenance tool uses. Today the schema is created by one large idempotent SQL batch, mount startup checks a fixed global list of tables, and only one component (`payload_gc`) records a version. After this milestone, schema changes are explicit, resumable migrations recorded per component; startup validation checks only what the volume's required capabilities need; each volume has a policy record with a policy epoch; and every active client and maintenance actor is visible in a session registry. An operator can then see, before changing anything, which clients would be fenced by a new feature.

## Progress

- [ ] SFS-1005: explicit, resumable PostgreSQL schema migration registry and CLI, extending `studiofs_schema_components`.
- [ ] SFS-1006: replace the global table check with capability-scoped schema validation.
- [ ] SFS-1007: persist per-volume protocol requirements (read, write, maintenance sets) and the policy epoch.
- [ ] SFS-1008: track active client sessions, capabilities and expired leases.

## Surprises & Discoveries

- Observation: the DDL batch uses `CREATE OR REPLACE FUNCTION` for trigger bodies and is re-run by `init`, `load` and `upgrade`, so an older binary would overwrite a successor's trigger function in a shared database.
  Evidence: `crates/studiofs-meta/src/pg_engine/schema.rs:569`, `crates/studiofs/src/cli/init.rs:322`, `crates/studiofs/src/cli/dump_load.rs:413`, `crates/studiofs/src/cli/upgrade.rs:43`.

## Decision Log

- Decision: extend `studiofs_schema_components` rather than add a second migration table.
  Rationale: it already has a monotonic marker that never moves backwards (SFS-980), and the Janitor already probes it.
  Date/Author: 2026-11-25, SFS-1001 audit.
- Decision: the policy record is per volume; DDL stays database-wide.
  Rationale: several volumes can share one database; activation must not affect other volumes, but schema objects are shared.
  Date/Author: 2026-11-25, SFS-1001 audit.

## Outcomes & Retrospective

Not started.

## Context and Orientation

PostgreSQL DDL lives in `studiofs_pg_schema_sql` in `crates/studiofs-meta/src/pg_engine/schema.rs` and is applied by `ensure_studiofs_pg_schema`. Mounts never run DDL; they call `validate_postgres_schema` in `crates/studiofs-meta/src/pg_engine.rs`, which checks a fixed list of tables and columns. The table `studiofs_schema_components` (component name, integer version, applied time) records `payload_gc = 2`; the insert is the last statement of the batch and only raises the version. `metadata_state` holds one row per volume including `metadata_format_version`. `bootstrap_checkpoint_leases` shows the lease pattern to reuse for sessions: expiry computed with the server's `clock_timestamp()` and a fencing token that increases on takeover (`crates/studiofs-meta/src/pg_engine/sql.rs:158`).

A "migration" here is a named, ordered step that changes the schema and records its component version in the same transaction. "Capability-scoped validation" means the mount checks only tables and columns that the volume's required capabilities declare, so extra objects added by a successor do not break an older mount. The "policy epoch" is a counter in the volume policy record that increases on every policy change; clients compare it to detect a change. A "session" is a row describing a running client or maintenance actor.

Read the "PostgreSQL schema and DDL", "Shared metadata state and publication" and "Multi-volume databases" sections of `docs/persisted-format-compatibility.md`, and the "Per-volume policy" and "Sessions" sections of ADR 0018.

## Plan of Work

SFS-1005 turns the batch into registered migrations keyed by component, keeps the existing batch as the baseline migration, and makes every DDL entry point refuse to run when any recorded component version is newer than the binary knows. SFS-1006 replaces the fixed list with a declaration per capability and has the evaluator decide whether a missing object is fatal. SFS-1007 adds the per-volume policy record and a write-requirement field checked by the publication UPDATE, separate from the read gate. SFS-1008 adds the session table with lease renewal and reporting of expired sessions.

## Concrete Steps

From the repository root, with PostgreSQL available as described in `docs/tests.md`:

    cargo test -p studiofs-meta --lib schema
    cargo test -p studiofs-meta --lib pg_engine -- --ignored schema   (environment-gated)
    cargo clippy -p studiofs-meta --all-targets -- -D warnings

## Validation and Acceptance

An older-binary simulation that runs DDL against a database with a newer component version is refused with a typed error and leaves the schema unchanged. A mount against a database containing an extra unknown table succeeds when no required capability names it. Two concurrent migration runs leave one consistent component version. Session rows appear for a mount and expire after their lease.

## Idempotence and Recovery

Every migration must be safe to re-run after a lost response: it checks the component version inside its transaction and commits the version marker together with the change. A failed migration rolls back completely.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1005, https://linear.app/valandreev/issue/SFS-1006, https://linear.app/valandreev/issue/SFS-1007, https://linear.app/valandreev/issue/SFS-1008.

## Interfaces and Dependencies

Policy and session data must be exposed to the evaluator as plain data so a Redis-compatible adapter (0.2.7) can provide the same inputs. No new PostgreSQL roles are introduced.
