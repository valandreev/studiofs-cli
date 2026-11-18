# Reopen a crashed writer while PostgreSQL is down

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`,
`Decision Log` and `Outcomes & Retrospective` current, following
`.agent/PLANS.md`.

## Purpose / Big Picture

A StudioFS client keeps its metadata in a local `redb` database and shares it
through PostgreSQL. If the client process is killed while PostgreSQL and S3
are down, the user expects to remount it right away, still offline, see every
change it had `fsync`ed, keep working, and have that work published when the
services return. Before this change the remount failed with "remote
unavailable: postgres connect failed", because an unclean redb image always
required a graph repair from a PostgreSQL checkpoint. After the change the E2E
scenario `mounted-recoverable-trash-durability` passes: the killed writer
reopens writable offline, restores a file and a directory from trash, and a
peer and a cold client later read the published result.

## Progress

- [x] (2026-11-18) Traced the failure: `open_meta_engine_with_options`
  (`crates/studiofs-meta/src/lib.rs`) sets the repair marker for every unclean
  image, and `PgMetaEngine::open_with_options` then needs PostgreSQL.
- [x] (2026-11-18) Added red tests for the unclean-image decision (compile
  failure), then the publication fence and the trust check.
- [x] (2026-11-18) Found the second blocker (the remount stayed `local_ready`,
  read-only) and added offline write authority with red/green tests.
- [x] (2026-11-18) Scenario 34 passed three times; 35-38 passed once each.
- [x] (2026-11-18) Documentation: ADR 0013, `docs/pg-sync-contract.md`,
  `docs/tests.md`, `docs/local-first-validation-matrix.md`,
  `docs/platform-bridges.md`, the recoverable-trash plan.

## Surprises & Discoveries

- Repair is needed only because a flush could publish events that lived only
  in volatile (`Durability::None`) commits. Making them durable before
  publication removes the reason; nothing else exports local graph state to
  peers (trash authority, ACL rows and object lifetimes are written in the
  same publication transaction).
- Remote replay applies an event batch and its cursor in one redb transaction
  (`write_replay_position_in_txn`), so a recovered prefix is self-consistent.
- A published batch whose local acknowledgement was lost is matched against
  the durable journal as a self batch, the same path that heals a lost commit
  response while mounted.
- Mounts open with deferred remote probes, so an offline remount stayed
  `local_ready` even with a trusted image: `Live` also required a PostgreSQL
  identity probe after every restart (SFS-706).
- The `trash-days` CLI in the scenario also leaves an unclean image; before the
  change that sent the first, online mount through the graph repair as well.
- With the fence, `mounted-recoverable-trash-s3-crash-replay` passes too: its
  restore failure came from the graph repair.

## Decision Log

- Decision: fence publication with the immediate barrier
  `RedbMetaEngine::sync_local_metadata` before the PostgreSQL transaction, and
  mark fenced images with the local-only counter
  `pgPublicationDurabilityFence`, set on every `PgMetaEngine` open.
  Rationale: one durable barrier per publication proves that published history
  is in the recovered image; the counter separates images from older binaries.
  A skip-when-nothing-pending variant was dropped: the idle worker clears the
  pending flag before its own commit, so the skip could race it. Date:
  2026-11-18.
- Decision: trust an unclean image only with the counter set and no pending
  repair marker; otherwise keep the existing repair. Rationale: a repair
  interrupted mid-replace must finish. Date: 2026-11-18.
- Decision: grant offline write authority to an established client (persisted
  client slot, no fresh-client or repair marker) only after a reachability
  failure, and end it on the first PostgreSQL answer to the identity probe.
  Rationale: this matches the contract exception for established clients and
  the state of a mounted client that loses PostgreSQL; publication and replay
  keep their identity gate. Date: 2026-11-18.
- Decision: trust applies whether or not PostgreSQL is reachable. Rationale:
  the proof does not depend on PostgreSQL, and the graph repair loses
  unpublished namespace state. Date: 2026-11-18.

## Outcomes & Retrospective

The writer remounts offline from its own durable state and publishes later.
The legacy repair path remains for unfenced and interrupted images.
Downgrading to an older binary after a crash is unsupported (ADR 0013).

## Context and Orientation

`crates/studiofs-meta/src/lib.rs` opens a volume. When a read-only probe
reports `RepairRequired`, it opens redb writably (redb recovers the last
durable commit) and calls `mark_unclean_pg_local_graph`. That function keeps
the image when `pg_engine::local_graph_survives_unclean_shutdown` is true;
otherwise it sets `LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER` and resets the
replay cursors, and `PgMetaEngine::open_with_options`
(`crates/studiofs-meta/src/pg_engine.rs`) then runs the PostgreSQL repair.

The fence is in `push_projected_event_log_batch_locked`
(`crates/studiofs-meta/src/pg_engine/metadata_io.rs`), the only path that
publishes local journal events. It calls
`RedbMetaEngine::sync_local_metadata` (`crates/studiofs-meta/src/redb_engine.rs`)
after remote format validation, so attempts that cannot reach PostgreSQL do
not pay for the barrier.

Offline write authority is the `offline_write_authority` state in
`PgMetaEngine` (`OFFLINE_AUTHORITY_INELIGIBLE`, `_ELIGIBLE`, `_GRANTED`).
`record_remote_operation_result` grants it on a reachability error;
`validate_remote_identity` clears it when PostgreSQL answers;
`refresh_pg_sync_lifecycle_for_allocator` treats a grant like a validated
identity when promoting `local_ready` to `live`.

## Plan of Work

Done as described above, in `crates/studiofs-meta` only, plus documentation.

## Concrete Steps

From the repository root:

    cargo test -p studiofs-meta --lib unclean_pg_image
    cargo test -p studiofs-meta --lib publication_requires_a_durable_local_journal
    cargo test -p studiofs-meta --lib reopened_offline
    cargo test -p studiofs-meta --lib deferred_mount_reopen
    cargo clippy -p studiofs-meta --all-targets -- -D warnings
    cargo build --release -p studiofs
    cargo run -p studiofs-e2e -- run --scenario mounted-recoverable-trash-durability --studiofs-bin target/release/studiofs

## Validation and Acceptance

The unit tests above pass and fail without the change. The E2E scenario
reports `status=Passed`. Scenarios 35-38 still pass.

## Idempotence and Recovery

All steps can be rerun. E2E runs write under `target/e2e-runs/`.

## Artifacts and Notes

Scenario 34 passing runs: `20261118T215119Z-c17776`,
`20261118T215212Z-09a1dd`, `20261118T215243Z-d91094`.

## Interfaces and Dependencies

No public API changes. New crate-internal items:
`pg_engine::LOCAL_PG_PUBLICATION_FENCE_COUNTER`,
`pg_engine::local_graph_survives_unclean_shutdown`.
