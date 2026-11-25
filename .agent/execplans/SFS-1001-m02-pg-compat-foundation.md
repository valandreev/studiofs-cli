# Milestone 02: PostgreSQL Compatibility Foundation

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

This plan is maintained in accordance with `.agent/PLANS.md` at the repository root. It depends on milestone 01 (`.agent/execplans/SFS-1001-m01-contracts-baseline.md`) for the identity types and the evaluator.

## Purpose / Big Picture

The shared PostgreSQL database holds the schema that every StudioFS client and maintenance tool uses. Today the schema is created by one large idempotent SQL batch, mount startup checks a fixed global list of tables, and only one component (`payload_gc`) records a version. After this milestone, schema changes are explicit, resumable migrations recorded per component; startup validation checks only what the volume's required capabilities need; each volume has a policy record with a policy epoch; and every active client and maintenance actor is visible in a session registry. An operator can then see, before changing anything, which clients would be fenced by a new feature.

## Progress

- [x] SFS-1005: explicit, resumable PostgreSQL schema migration registry and CLI, extending `studiofs_schema_components` (2026-11-25; `pg_engine/migrations.rs`, `studiofs upgrade --dry-run`).
- [ ] SFS-1006: replace the global table check with capability-scoped schema validation.
- [x] SFS-1007: persist per-volume protocol requirements (read, write, maintenance sets) and the policy epoch (2026-11-25; `volume_protocol.rs`, `pg_engine/volume_protocol.rs`, `metadata_state.write_requirement`).
- [x] SFS-1007 review follow-up: lock the metadata row before reading the policy, reject history conflicts atomically, preserve checkpoint format and schema, and require explicit per-volume provisioning.
- [ ] SFS-1008: track active client sessions, capabilities and expired leases.

## Surprises & Discoveries

- Observation: the DDL batch uses `CREATE OR REPLACE FUNCTION` for trigger bodies and is re-run by `init`, `load` and `upgrade`, so an older binary would overwrite a successor's trigger function in a shared database.
  Evidence: `crates/studiofs-meta/src/pg_engine/schema.rs:569`, `crates/studiofs/src/cli/init.rs:322`, `crates/studiofs/src/cli/dump_load.rs:413`, `crates/studiofs/src/cli/upgrade.rs:43`.

- Observation: re-applying the baseline DDL takes `ACCESS EXCLUSIVE` and `SHARE` table locks, so an implicit re-application in `load` waited behind an open writer transaction and the load race test timed out.
  Evidence: `dump_load_rejects_accounting_row_committed_while_load_waits` failed at its second lock wait until `ensure` stopped re-applying a verified baseline.

- Observation: under READ COMMITTED, a joined policy read that waits for the metadata row lock can retain an earlier policy snapshot.
  Evidence: `volume_protocol_waiting_update_rechecks_policy_epoch_on_live_postgres` detects the second updater waiting on the first via `pg_blocking_pids`. The expected outcome is `EpochConflict` with the winner's policy and audit row intact.

- Observation: a binary that knows a retained write capability can still propose a rollback that would let 0.2.5 writers publish without that capability.
  Evidence: the focused rollback unit test rejected the same proposal only after it checked the baseline write capabilities as well as the actor.

## Decision Log

- Decision: SFS-1007 extends the `core` 1 baseline instead of registering `core` 2, and re-pins its checksum.
  Rationale: 0.2.5 is unreleased and ships the policy, so it belongs to its baseline. A `core` 2 would leave every new database above the baseline, where `--repair-schema` and baseline reconciliation never apply. Development databases recorded with the old checksum get `RepairRequired` until `studiofs upgrade` reconciles them.
  Date/Author: 2026-11-25, SFS-1007.
- Decision: the write requirement is an integer in `metadata_state` (`SHARED_WRITE_REQUIREMENT`), not a capability set; the policy record is a separate `volume_protocol` row.
  Rationale: publication already updates `metadata_state`, so `write_requirement <= $4` adds no lock or round trip (SFS-1014 preflight); comparing sets would need the policy row on every publication. `check_update` ties the two: adding a write capability must raise the integer.
  Date/Author: 2026-11-25, SFS-1007.
- Decision: `load` keeps the target's write requirement and existing policy when replacing metadata state. Creating metadata state for a volume provisions its baseline policy.
  Rationale: preserving the write requirement keeps writers fenced after a load. Schema reconciliation only creates the shared policy tables. While format 6 is current, the format value alone cannot identify an eligible volume. Tests explicitly select an existing fixture. Production legacy volumes are adopted individually by SFS-1132.
  Date/Author: 2026-11-25, SFS-1007 review.
- Decision: lock `metadata_state` before reading the policy in a separate statement. Append history without ignoring duplicate epochs.
  Rationale: the second statement takes a fresh READ COMMITTED snapshot after any wait. A history conflict aborts the update transaction. Initial policy and history use one SQL statement.
  Date/Author: 2026-11-25, SFS-1007 review.

- Decision: no entry point runs DDL against a verified schema. `init`/`load` refuse a differing baseline; `studiofs upgrade` reconciles it, and `--repair-schema` re-applies a verified baseline to restore dropped objects, both only while nothing is recorded above it.
  Rationale: implicit DDL blocked behind live writers, and an unconditional re-apply in `upgrade` deadlocked with concurrent writers (`snapshot_cli_test` failed 2/3 runs; 1/3 on `main`, which re-ran the batch too). Re-applying is unsafe once later steps exist, because the baseline's `CREATE OR REPLACE FUNCTION` would revert their function bodies.
  Date/Author: 2026-11-25, SFS-1005 review.
- Decision: `ensure_studiofs_pg_schema` takes a `Transaction`; `ensure_studiofs_pg_schema_committed` wraps callers without one.
  Rationale: the advisory lock is transaction-scoped, so an autocommit client ran the DDL and marker unlocked.
  Date/Author: 2026-11-25, SFS-1005 review.
- Decision: one `studiofs_schema_components` row per component with a SHA-256 chained over its steps, instead of a per-step history table.
  Rationale: the issue forbids a second schema-version table; the chain still detects any difference in applied history.
  Date/Author: 2026-11-25, SFS-1005.

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

For SFS-1007, run `cargo test -p studiofs-meta --lib volume_protocol` for unit coverage, then repeat with `-- --ignored` against an explicitly configured local PostgreSQL service. The two live tests cover selected provisioning, legacy neighbors, audit conflicts, write fencing and a waiting updater. Run the `baseline_checksum_is_pinned` test after editing the unreleased baseline.

SFS-1007 verification on Windows (2026-11-25) passed 94 focused metadata tests, both protocol cases and the migration registry case on PostgreSQL 18, and the CLI dump/load/discover case on PostgreSQL 18 and RustFS 1.0.4. Clippy passed for `studiofs-meta` and `studiofs`. The mounted gates remain separate.

An older-binary simulation that runs DDL against a database with a newer component version is refused with a typed error and leaves the schema unchanged. A mount against a database containing an extra unknown table succeeds when no required capability names it. Two concurrent migration runs leave one consistent component version. Session rows appear for a mount and expire after their lease.

## Idempotence and Recovery

Every migration must be safe to re-run after a lost response: it checks the component version inside its transaction and commits the version marker together with the change. A failed migration rolls back completely.

## Artifacts and Notes

Issues: https://linear.app/valandreev/issue/SFS-1005, https://linear.app/valandreev/issue/SFS-1006, https://linear.app/valandreev/issue/SFS-1007, https://linear.app/valandreev/issue/SFS-1008.

## Interfaces and Dependencies

Policy and session data must be exposed to the evaluator as plain data so a Redis-compatible adapter (0.2.7) can provide the same inputs. No new PostgreSQL roles are introduced.
