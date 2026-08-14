# SFS-40: Daily automatic snapshot policy

This design is approved for implementation on 2026-08-14. It follows the
Snapshot v2 contract in `.agent/execplans/SFS-650.md` and is intentionally
limited to automatic policy evaluation and its mounted-client lifecycle.

## Purpose

After this change, a user can store one opt-in daily policy for a volume. Any
connected StudioFS client evaluates that policy, creates at most one automatic
Snapshot v2 job for each eligible local date and scheduled time, and lets the
existing materializer retry or transfer the job after a client disappears.
No daemon, server process, or hidden restart command is introduced.

## Scope

The change includes the policy domain values, deterministic due-slot and
daylight-saving-time mapping, PostgreSQL policy-slot/evaluator-lease
authority, the mounted `SnapshotPolicyEvaluator`, and focused unit,
PostgreSQL, and VFS lifecycle tests.

The public `snapshot policy set/show/cancel` command surface remains the
responsibility of SFS-651. This ticket exposes the backend port needed by that
surface but does not add a competing CLI workflow.

## Design

`SnapshotPolicyEvaluator` is a separate application module beside the existing
`SnapshotWorker`. The existing worker drains durable creation jobs; the new
evaluator turns an active policy into a durable job. Both are started from the
existing `StudioVfs` startup-worker lifecycle and use the existing tracked
shutdown token.

The evaluator owns a PostgreSQL per-volume lease with a fencing token. It
claims or renews that lease before evaluating. A lease-renewal failure of
uncertain state fences the evaluator from creating new work; another mounted
client may take over only after expiry. The evaluator sleeps until the next
due instant, its lease-renew deadline, or a maximum 30-second fallback poll,
and always selects cancellation during sleeps and database awaits.

The due-slot transaction locks the active policy generation, computes the
latest eligible slot, records a spring-forward gap as `SkippedDstGap`, and
claims a unique generation-plus-local-date-plus-local-time slot. It inserts or
follows the corresponding `snapshot_catalog` Creating row and
`snapshot_creation_jobs` row in the same transaction. A transaction rollback
removes the slot claim and queue row together, so a later evaluator can retry.

The slot identity is the policy generation plus local calendar date and
persisted local `HH:MM`; it is not a host-local timestamp. UTC is persisted
when no timezone is supplied. A nonexistent local time is recorded once as a
skipped gap and is not shifted. An ambiguous fall-back time uses the earlier
UTC occurrence; uniqueness suppresses the second occurrence. After downtime,
the evaluator claims only the latest eligible non-skipped slot. Automatic
retention starts at `ready_at`, and manual snapshots are never selected by
policy rotation.

## Interfaces

The backend-agnostic `SnapshotPolicyStore` port gains evaluator lease claim,
renew, due-slot evaluation, and release operations. It returns typed,
redacted Snapshot v2 values and never imports PostgreSQL types. The PostgreSQL
adapter owns all SQL, row locks, unique constraints, transaction boundaries,
and error classification.

The evaluator exposes only:

    pub async fn run(
        self: Arc<Self>,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(), SnapshotPolicyEvaluatorError>;

`StudioVfs` owns one evaluator for the verified mounted volume and tracks its
task exactly like the existing snapshot worker. No generic scheduler or new
public abstraction is added.

## Testing contract

Tests are written first and observed failing. Pure policy tests cover UTC
defaulting, invalid zones, one active generation, cancellation/re-enable,
retention validation, DST gap and ambiguity, and latest-slot catch-up.
PostgreSQL tests cover atomic rollback, one slot/job under concurrent clients,
automatic origin, evaluator observation after policy commit, lease takeover,
and no orphaned claims. VFS tests cover bounded wake-up, cancellation, and
release-or-expiry behavior. Tests use injected clocks and bounded polling;
fixed sleeps are not synchronization.

## Decision log

- Decision: Use a separate mounted evaluator beside `SnapshotWorker`.
  Rationale: `SnapshotWorker` already has one responsibility—materializing
  durable jobs—so policy evaluation must not be hidden inside its queue drain.
- Decision: Make slot claim, catalog row, and creation job one PostgreSQL
  transaction. Rationale: this removes the claim/enqueue gap and makes rollback
  retryable without duplicate work.
- Decision: Persist UTC by default and use explicit DST gap/ambiguity rules.
  Rationale: host timezone differences must not produce incompatible slots.
- Decision: Keep CLI work in SFS-651. Rationale: SFS-40 needs the backend and
  mounted lifecycle; duplicating public commands would widen scope.

