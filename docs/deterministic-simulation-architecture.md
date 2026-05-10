# Deterministic Simulation Architecture

This document defines the first StudioFS deterministic simulation boundary. The
simulator is a fast, seed-reproducible test layer for StudioFS logical clients.
The target scope is core distributed state-machine behavior. It is not a
replacement for Contract/Core tests or real mounted E2E tests.

The initial design covers Phase 3 planning for the Deterministic Simulation
Testing milestone. Follow-up implementation issues are SFS-331 through SFS-336.

## Scope And Non-Goals

In scope:

- logical multi-client filesystem operations that exercise namespace, file
  version, publication, replay, and conflict-resolution behavior;
- local-first visibility before remote publication;
- metadata replication ordering, delayed delivery, duplicate delivery, outage,
  reconnect, and idempotent apply behavior;
- object upload readiness, object-store outage, retry, reachability, and the
  separation between payload availability and peer-visible metadata;
- crash/restart replay of durable local logical state;
- invariant validation over logical snapshots from one or more clients;
- deterministic counterexample capture and replay from a seed and event trace.

Out of scope:

- FUSE callback sequencing;
- WinFSP callback sequencing;
- OS page cache behavior, platform file-handle quirks, drive letters, mount
  permissions, or stale mount cleanup;
- PostgreSQL SQL behavior, query plans, NOTIFY latency, connection pooling, or
  network sockets;
- full S3/RustFS/OpenDAL behavior, multipart upload details, bucket policy, or
  network timing;
- wall-clock sleeps, Tokio runtime fairness, real async networking, Docker, or
  RWX worker behavior;
- throughput, latency, cache hit-rate, memory pressure, or performance
  regression coverage.

The simulator should deliberately stay smaller than the product. If a behavior
needs a real mount, Docker, PostgreSQL, RustFS, FUSE, WinFSP, or platform I/O to
be meaningful, it belongs in Real E2E or bridge tests rather than in this layer.

## Simulation Boundaries

The simulator models StudioFS as logical clients connected to deterministic
shared services. It does not instantiate the production VFS, bridges, database,
or object store.

| Boundary | Simulated surface | Explicitly not simulated |
|---|---|---|
| core logic | Namespace operations, file content versions, local-first reads, durable boundary markers, publication eligibility, restart replay inputs, and conflict outcomes | Kernel callback order, OS open-handle semantics beyond the logical model, file descriptor lifetime bugs, and platform metadata |
| metadata replication | Ordered events, batch/version ordering, foreign apply, delayed/duplicate delivery, metadata outage/recovery, reconnect, and idempotent replay | PostgreSQL SQL, schema migrations, connection behavior, NOTIFY delivery timing, and real lock-table queries |
| object upload | Local-only payloads, remote object presence, delayed upload completion, upload failure/retry, object-store outage/recovery, and object reachability | S3 API semantics, OpenDAL behavior, RustFS process health, multipart upload, and network errors outside modeled faults |
| cache/replay | Durable logical local state, pending uploads, pending publications, replay after crash, and stale pending-entry cleanup | Actual cache file layout, redb persistence mechanics, disk pressure, fsync cost, or payload bytes beyond checksums/content IDs |
| real OS mount | Public mounted behavior is consumed only through operation logs and artifacts from Real E2E | Direct FUSE, WinFSP, macFUSE, OS page cache, filesystem permissions, and platform-specific path encoding |

Publication is a separate boundary from upload. A simulated object becoming
available must never imply peer visibility unless the metadata publication event
is also eligible, emitted, delivered, and applied.

## Required Abstractions

The initial implementation should introduce small modules with explicit contracts
instead of one monolithic simulator.

| Abstraction | Responsibility | Follow-up |
|---|---|---|
| operation model | Serializable logical operations shared by Real E2E and simulation: create, mkdir, write, append, truncate, rename, unlink, rmdir, read, stat, readdir, flush, fsync, close/release, crash, restart, outage, recovery, wait, and barrier | SFS-331 |
| logical state snapshot | A stable view of paths, inodes or logical node IDs, file versions, payload references, pending uploads, pending publications, and client-local visibility | SFS-332 |
| invariant validation | Product-specific checks for convergence, no lost durable writes, no unexpected resurrection, valid tree structure, canonical path ownership, object reachability, and idempotent replay | SFS-332 |
| simulated metadata bus | Scheduler-driven metadata replication with ordered delivery, outage, reconnect, delayed delivery, duplicate delivery, and traceable batch/version behavior | SFS-333 |
| simulated object store | Scheduler-driven object presence, local-only payloads, delayed upload completion, failed uploads, retries, outage/recovery, remote read failure, and reachability export | SFS-334 |
| virtual time | Logical ticks and deadlines that replace wall-clock sleeps and make outage, retry, upload, and publication timing reproducible | SFS-335 |
| deterministic scheduler | Seeded event selection, run-until-idle, step-by-step execution, invariant stop points, and trace emission for each selected interleaving | SFS-335 |
| logical clients | Per-client local state and operation execution against the metadata bus, object store, scheduler, operation model, and invariant validator | SFS-336 |

Each abstraction should own one deterministic input/output contract. Shared data
formats should use append-only JSON-compatible schemas so failing simulation
cases can be compared with Real E2E artifacts.

## Test-Layer Relationship

StudioFS should keep three distinct correctness layers:

| Layer | Owns | Does not own |
|---|---|---|
| Contract/Core | Fast local metadata, VFS, and cache contract checks using focused fixtures such as `ContractCoreFixture` and `ContractVfsFixture` | Multi-client interleaving search, mounted OS behavior, Docker infrastructure, or exhaustive schedule exploration |
| Deterministic simulation | Seeded exploration of logical multi-client interleavings across metadata replication, object upload, publication, cache/replay, restart, and invariant validation | Real FUSE/WinFSP mounts, PostgreSQL, RustFS, Docker, wall-clock timing, or performance |
| Real E2E / Multi-client | Product CLI, real mounted clients, Docker Compose infrastructure, mounted tree convergence, platform preflight, and CI/RWX artifacts | Exhaustive interleaving search or internal logical-state mutation outside public mounted surfaces |

The simulation layer should reuse Contract/Core invariants where they can be
expressed over logical state. It should reuse Real E2E operation artifacts where
the operations are portable. It should not weaken either layer by making them
depend on simulator internals.

## Phase 1/2 Reuse

Phase 1/2 Real E2E work already established runner artifacts and mounted
scenario shape. Phase 3 should mirror those surfaces rather than inventing an
unrelated language.

Reusable inputs:

- Real E2E `operations.ndjson` records should become the reference operation
  log shape or be mechanically translatable into the shared operation model.
- Scenario identifiers, client IDs, deterministic order markers, operation
  result fields, and error fields should stay portable where the simulated
  operation is meaningful without a mount.
- Mounted namespace collision scenarios define useful logical workloads for
  rename/rename, rename/unlink, unlink/append, overwrite/read,
  mkdir/rmdir/create, and readdir/stat during mutation.

Reusable invariants:

- final client convergence on the same public tree;
- no duplicate canonical paths unless hard links are explicitly modeled;
- no missing parent directories;
- no unexpected resurrection after unlink/rmdir;
- no lost data after an explicit local durability boundary;
- object reachability must match active, pending, and deleted logical states;
- replay must be idempotent after crash/restart.

Reusable artifacts:

- operation log files;
- validation summaries;
- per-client snapshots, translated from mounted tree snapshots into logical
  snapshots when possible;
- failure diagnostics that identify scenario, seed, client, operation index,
  invariant, and relevant path/object IDs.

Unsupported mounted-only operations must remain explicit. For example, a future
operation model can contain a platform probe operation for Real E2E while marking
it as unsupported by deterministic simulation.

## Counterexample Capture And Replay

Every failing simulation run should produce a compact counterexample bundle:

- simulator schema version;
- scenario name;
- random seed;
- scheduler strategy and configuration;
- complete operation log;
- selected event trace with virtual time, client ID, event type, and causal
  references;
- fault injection points such as metadata outage, object-store outage, upload
  failure, duplicate delivery, or crash;
- last successful invariant checkpoint;
- violated invariant name, message, and logical snapshot references;
- minimized replay input when shrinking is available.

Replay should require no PostgreSQL, RustFS, Docker, real mount, or wall-clock
sleep. The replay command should accept the captured seed plus trace, then force
the same event choices. When the simulator later supports randomized exploration
and shrinking, the minimized counterexample must keep the original failing seed
and a separate minimized seed or trace ID so both the found case and the reduced
case remain reproducible.

Counterexample artifacts should use forward-slash relative paths and stable JSON
or NDJSON. This keeps them compatible with existing E2E report upload and makes
future CI/RWX collection straightforward without depending on the real E2E
runner.

## Initial Module Plan

The design intentionally sequences implementation in small slices:

1. SFS-331 extracts the shared operation model and stable operation log schema.
2. SFS-332 extracts logical snapshots and invariant validation.
3. SFS-333 adds the simulated metadata bus.
4. SFS-334 adds the simulated object store and upload model.
5. SFS-335 adds virtual time and the deterministic scheduler.
6. SFS-336 combines those pieces into logical clients and the first simulated
   multi-client runtime.

Candidate crate layout:

```text
crates/studiofs-sim/
  src/
    operation.rs
    snapshot.rs
    invariant.rs
    scheduler.rs
    metadata_bus.rs
    object_store.rs
    client.rs
    trace.rs
    replay.rs
```

If creating a new crate is too much for the first implementation slice, the same
module boundaries can start under a test-support crate or an internal module.
The boundaries matter more than the initial package name.

## MVP Scenario Set

The first runtime should prove the layer with small scenarios:

1. create/write/flush/upload/publish/read convergence across two logical
   clients;
2. uploaded object exists before metadata publication and remains invisible to a
   peer;
3. metadata outage accepts local durable writes, then recovers and converges;
4. object-store outage keeps the writer's local view readable and blocks peer
   visibility until upload and publication complete;
5. crash after local durability boundary replays pending upload/publication and
   converges;
6. rename/unlink collision converges without duplicate canonical paths or
   unexpected resurrection.

These scenarios should stay byte-light. File payloads can be represented by
stable content IDs and checksums unless a specific invariant needs actual bytes.

## Design Rules

- Determinism is part of the API. Any randomness must come from the scheduler
  seed and be recorded in the trace.
- Wall-clock time is forbidden in simulation tests. Use virtual time ticks.
- Upload completion, metadata publication, and peer apply are separate events.
- Each logical client owns isolated local state. Shared mutable state belongs
  only to simulated services.
- The simulator must validate product invariants, not implementation accidents.
- Real E2E remains the authority for mounted behavior and platform integration.
- Contract/Core remains the authority for focused production component behavior.
- Any modeled behavior that intentionally differs from production must be named
  as a simulator approximation and covered by a Real E2E or Contract/Core check
  if it is correctness-sensitive.
