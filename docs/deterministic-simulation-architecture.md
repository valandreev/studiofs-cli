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
| operation model | `studiofs-scenario` serializable logical operations shared by Real E2E and simulation: create, mkdir, write, append, truncate, rename, unlink, rmdir, read, stat, readdir, flush, fsync, close/release, crash, restart, outage, recovery, wait, and barrier | SFS-331 |
| logical state snapshot | `studiofs-scenario::invariant::LogicalSnapshot` schema version 1: clients, entries, logical IDs when known, file size/checksum, symlink targets, object references, expected-absent paths, durable write expectations, object lifecycle facts, hard-link mode, and optional replay-idempotence result | SFS-332 |
| invariant validation | `studiofs-scenario::invariant::validate_logical_invariants` checks convergence, no lost durable writes, no unexpected resurrection, valid tree structure, canonical path ownership, object reachability, and idempotent replay over logical snapshots | SFS-332 |
| simulated metadata bus | `studiofs-scenario::metadata_bus::MetadataBus` models local enqueue, ordered batch/version delivery, outage, reconnect, delayed delivery, duplicate delivery, idempotent apply, and traceable scheduler-controlled delivery points | SFS-333 |
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

## Product-Finding Strategy

The deterministic simulator is useful only if it becomes a bug-finding front
end for the production filesystem, not a separate toy implementation that only
tests itself. The deep testing strategy therefore has two jobs:

1. search more interleavings than Real E2E can afford;
2. project every interesting interleaving back onto production code through a
   shared operation log and invariant oracle.

The simulator remains the fast search engine, but production components must be
plugged into the same workload in progressively heavier layers:

| Stage | Driver | System under test | Oracle | Expected bug class |
|---|---|---|---|---|
| model search | `studiofs-scenario` logical clients | simulation model only | shared logical invariants | scenario/model gaps, invariant bugs, missing fault cases |
| contract replay | `ScenarioOperationRecord` replay into Contract/Core fixtures | `studiofs-meta`, `studiofs-cache`, and `studiofs-vfs` non-mounted fixtures | same logical snapshot and durable-write invariants | publication, replay, object reachability, epoch, and namespace bugs in production core logic |
| mounted replay | same operation log translated into `studiofs-e2e` mounted clients | `studiofs-cli` through real clients, PostgreSQL, RustFS, and mounted trees | mounted tree convergence plus shared operation/invariant summaries | bridge/runtime/integration bugs missed by pure core tests |
| counterexample promotion | saved failing artifact | smallest layer that reproduces the failure | exact artifact replay plus focused regression test | permanent regression coverage for found product bugs |

This creates a one-way validation ladder. A failure first found in model search
should be minimized and replayed against Contract/Core. If it still reproduces,
the bug belongs to production core logic and should get a focused production
regression. If Contract/Core passes but mounted replay fails, the bug belongs to
the runner, bridge, process lifecycle, or integration boundary. If only the
model fails, the simulator or invariant needs correction before the result can
be treated as a StudioFS product defect.

### Deep Search Axes

Deep eventual-consistency testing should deliberately vary the axes that create
real StudioFS risk:

- publication ordering: upload completion before/after metadata publication,
  repeated publish attempts, stale epoch publication, and foreign apply racing
  with local mutation;
- local durability: write accepted before flush, flush/fsync/release boundary,
  crash before upload, crash after upload, and restart while pending work is
  queued;
- namespace collisions: rename/rename, rename/unlink, unlink/write,
  mkdir/rmdir/create, parent rename while child changes, and stale readdir/stat
  observations;
- metadata replication: delayed head-of-line batch, duplicate delivery,
  disconnect/reconnect, outage flapping, and delivery after local divergent
  state exists;
- object lifecycle: local-only payload, pending upload, failed upload retry,
  object-store outage, deleted-but-protected object, and metadata pointing at a
  missing remote object;
- replay idempotence: repeated restart loops, stale pending events after newer
  epochs, and replay after partial cleanup.

Each axis should be represented in the model search and then be mapped either
to a Contract/Core replay command or a documented mounted-only E2E scenario.
When an axis cannot be mapped to production yet, the corpus output should label
that gap instead of implying product coverage.

### Differential Replay Contract

The shared operation log is the handoff between search and product validation.
A deep run that finds a counterexample must preserve:

- scenario group, seed, scheduler trace, and selected event order;
- operation log in `ScenarioOperationRecord` format;
- fault schedule: metadata outage/recovery, object-store outage/recovery,
  upload completion/failure, duplicate delivery, and crash/restart points;
- expected logical invariants and stable-point checkpoints;
- unsupported operation markers when the operation is simulation-only or
  mounted-only.

Contract/Core replay should consume the operation log without Docker or mounts
and export a logical snapshot that can be checked by
`validate_logical_invariants`. Mounted replay should consume the same portable
operations through `studiofs-e2e`, then export mounted tree snapshots and
operation summaries. Exact replay from an artifact remains stronger than
`run-corpus --seed`: replay must read the saved artifact and fail if the trace,
operation log, invariant, or relevant snapshot drifts.

### Execution Policy

Use three execution depths:

- fast corpus: small fixed seed set for quick PR-oriented signal;
- deep corpus: stable documented seed set for normal manual/RWX exploration;
- exploratory corpus: larger local or RWX seed windows used to find new bugs,
  with the seed range and scenario group captured in the run notes.

The current implementation supports model search directly. Product-backed
Contract/Core replay and mounted replay are the next required integration steps
before deterministic simulation can be considered a full StudioFS product
correctness gate.

## Phase 1/2 Reuse

Phase 1/2 Real E2E work already established runner artifacts and mounted
scenario shape. Phase 3 should mirror those surfaces rather than inventing an
unrelated language.

Reusable inputs:

- Real E2E `operations.ndjson` records use `studiofs-scenario`
  `ScenarioOperationRecord` lines with `schema_version = 1`, monotonic
  per-file `sequence`, typed operation variants, typed outcomes, and explicit
  simulation support markers.
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

The shared invariant core lives in `studiofs-scenario` so Contract/Core tests,
the deterministic simulator, and real E2E support crates can depend on the same
JSON-compatible data model without requiring mounted filesystems. Mounted tree
snapshots map only the portable public fields into logical entries: relative
path, entry type, regular-file size, regular-file checksum, and symlink target
when present. Mounted snapshots do not provide stable logical IDs, object
lifecycle protection, local durability expectations, or replay results, so
those fields remain absent unless a scenario supplies them explicitly.

Unsupported mounted-only operations must remain explicit. Operation records keep
`simulation_support.status = "unsupported"` plus a reason when the simulator
must skip or reject the record, such as platform probe operations or mounted
callback boundaries that have no logical simulation equivalent.

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

The initial SFS-339 artifact format is a single JSON document written by
`studiofs_scenario::crash_replay::run_crash_replay_suite_with_artifacts` under
`<artifact-root>/<scenario>/<family>-seed-<seed>.json`. Schema version 1
contains:

- `scenario`, `family`, and `seed`;
- `scheduler_trace`, using the deterministic scheduler trace schema;
- `operation_log`, using the shared `ScenarioOperationRecord` schema;
- `client_trace`, using crash/restart replay trace records;
- `failure.violated_invariant`, `failure.message`, and diagnostic strings;
- `final_snapshot`, when the simulated runtime can export one;
- `replay_command`, currently
  `cargo run -p studiofs-scenario --bin studiofs-scenario -- replay-counterexample <artifact>`.

Replay is local and deterministic: `replay-counterexample` reads the artifact,
reruns the same scenario family and seed, and fails if the violated invariant,
scheduler trace, operation log, or client trace differs from the saved
counterexample. It does not start Docker, PostgreSQL, RustFS, mounted
filesystems, Tokio timing, or wall-clock sleeps.

The SFS-359 corpus artifact format extends that replay contract to non-crash
simulation corpus failures. Generic corpus counterexamples are single JSON
documents under `<artifact-root>/<corpus>/<scenario-group>/seed-<seed>-failure-<n>.json`.
Schema version 1 contains:

- `scenario`, `corpus`, `group`, optional `sub_group`, and `seed`;
- `scheduler_trace`, using the deterministic scheduler trace schema or a stable
  checkpoint trace for deterministic groups without randomized scheduling;
- `operation_log`, using the shared `ScenarioOperationRecord` schema when the
  group can map its trace to operations;
- `event_trace`, using the group-specific logical event records;
- `failure.violated_invariant` and `failure.message`;
- `final_snapshot`, when the simulated runtime can export one; object-store
  outage artifacts use the object lifecycle snapshot exported by the simulated
  object store;
- `replay_command`, always
  `cargo run -p studiofs-scenario --bin studiofs-scenario -- replay-counterexample <artifact>`.

For generic corpus artifacts, replay reruns the saved corpus, group, and seed
from the artifact path and fails if the violated invariant, message, scheduler
trace, operation log, event trace, or final snapshot differs. Running a seed with
`run-corpus --seed <n>` is only a fresh corpus run; it is not exact
counterexample replay unless it reads the saved artifact. Replay mismatch errors
include the first differing index for trace and log vectors, or the saved and
replayed lengths when one side has extra records.

## Initial Module Plan

The design intentionally sequences implementation in small slices:

1. SFS-331 extracts the shared operation model and stable operation log schema
   into `crates/studiofs-scenario`.
2. SFS-332 extracts logical snapshots and invariant validation.
3. SFS-333 adds the simulated metadata bus.
4. SFS-334 adds the simulated object store and upload model.
5. SFS-335 adds virtual time and the deterministic scheduler.
6. SFS-336 combines those pieces into logical clients and the first simulated
   multi-client runtime.

Candidate crate layout:

```text
crates/studiofs-scenario/
  src/lib.rs        shared operation records and NDJSON helpers

crates/studiofs-sim/
  src/
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

The initial SFS-333 slice starts in `studiofs-scenario::metadata_bus` so later
simulation runtime code can reuse the same serializable batch, event, and trace
types without depending on real PostgreSQL. The bus is intentionally passive:
logical clients should be connected before scenario events are enqueued, and
enqueueing a local metadata batch only records the ordered batch and queues it
for already-known foreign clients. Tests or a future deterministic scheduler
must call `deliver_next` to apply the head ready batch to a target client. A
delayed head batch blocks later batches for that client so simulated delivery
preserves strict version order. Metadata outages, client disconnects, delayed
batches, released delays, and duplicate deliveries are explicit state
transitions and trace records. Delay, release, and duplicate-injection helpers
reject unknown batch IDs instead of mutating scheduler state. This keeps
counterexample replay independent of wall-clock timing or background polling.
Long-running searches can periodically call `drain_trace` after exporting
records; pruning applied batches requires a future runtime retention contract
that knows every simulated client's replay horizon.

The initial SFS-334 slice adds `studiofs-scenario::object_store` as the matching
passive payload service. Tests stage local-only payload facts per client, create
upload tickets, and explicitly drive upload completion, injected failures,
retry ticks, object-store outage/recovery, and remote reads. A completed upload
only creates remote object availability; it does not mark the object as
metadata-published or peer-visible. The module exports object lifecycle facts in
the shared invariant schema so active local payloads, pending uploads, and
deleted remote objects can be checked without simulating S3, OpenDAL, sockets,
or wall-clock timing.

The initial SFS-335 slice adds `studiofs-scenario::scheduler` as the virtual
time driver for those passive services. Tests enqueue `ScheduledEvent` values at
explicit ticks with typed event categories for client operations, metadata
delivery, upload completion, publish attempts, crash/restart, modeled faults,
and checkpoints. `SimulationScheduler` keeps pending work grouped by virtual
tick, selects among same-tick ready events from a seed-controlled deterministic
generator, advances only by virtual ticks, and exposes both step-by-step
execution and `run_until_idle` / `run_until` modes. The `run_until` mode stops
at the first invariant failure and leaves later events pending for
counterexample capture. Scheduler trace records include the seed, step, virtual
tick, event ID, client ID, event type, ready-event count, ready-event IDs, and
selected interleaving index. Simulation tests must keep retries, outages, upload
completion, publication, and crash/restart timing on this scheduler surface
rather than using wall-clock sleeps or Tokio timing.

The initial SFS-336 slice adds `studiofs-scenario::client::SimulatedRuntime` as
the logical multi-client runtime over those passive services. The runtime owns
isolated per-client namespace state and exposes the first core operation subset:
`mkdir`, `create_file`, `write_file`, `append_file`, `truncate_file`, `rename`,
`unlink`, `rmdir`, `read_file`, `stat`, `readdir`, `flush`, upload scheduling,
metadata publication, foreign metadata delivery, metadata outage/recovery, and
client restart markers. Upload completion still only makes an object remotely
available; peer-visible reads require an explicit metadata publish and delivery
step. Runtime snapshots export `LogicalSnapshot` values with durable write
expectations and object lifecycle facts for the shared invariant validator.
Client trace records include the logical client for each client-owned operation
so failing simulation artifacts can identify the actor without real mount,
PostgreSQL, RustFS, redb, Docker, or wall-clock dependencies.

The initial SFS-337 slice adds `studiofs-scenario::collision` as the first
seeded collision-suite surface. `run_collision_suite` accepts a scenario name,
an explicit seed list, and one or more `CollisionFamily` values, then returns a
JSON-compatible report with per-family/per-seed results, invariant checkpoint
counts, scheduler-selected event traces, and replayable failure records. Each
family schedules competing logical operations through `SimulationScheduler` and
mutates simulated client state only when the scheduler selects that operation.
The first suite covers rename/rename, rename/unlink, unlink/append, overwrite
while a stale reader keeps the previous committed view, mkdir/rmdir racing with
create inside the same directory, metadata delivery racing with a local
namespace mutation, and publish racing with foreign apply. Stable checkpoints
run after the modeled conflict-resolution phase so the shared invariant
validator checks convergence, canonical path ownership, tree structure,
resurrection, durable writes, and object reachability. If a foreign apply is
selected before the corresponding publish creates a metadata batch, the rejected
apply is still recorded as part of the replayable interleaving trace.

The initial SFS-338 slice adds `studiofs-scenario::crash_replay` as the
crash/restart replay suite over the same deterministic runtime. The suite runs
explicit seed lists across crash-after-durability, crash-after-upload,
crash-during-publish, crash-during-foreign-apply, pending-local-upload restart,
repeated restart/replay, and stale pending-upload/publish families. Each run
records replay checkpoints, scheduler-selected crash/publish/apply races where
ordering matters, a replay-idempotence result, peer convergence status, and
replayable failure traces. A stale publish carries the expected logical epoch
and is rejected when a newer local epoch already exists, so old pending upload
or publish events cannot roll a peer back to older durable content. Namespace
mutations such as `mkdir`, `rename`, `unlink`, and `rmdir` update the simulated
durable namespace replay state immediately, while file payload bytes still
require the modeled local durability boundary. Directory logical IDs are stable
through rename/restart and are not derived from the current path.

The initial SFS-340 slice adds `studiofs-scenario::seed_corpus` as the
RWX-facing corpus orchestrator. The fast corpus uses stable seeds `1, 2, 3`;
the deep corpus uses stable seeds `1..=16`. Both run the named scenario groups
in deterministic order: namespace collisions, crash/restart replay, metadata
outage, and object-store outage. The corpus CLI supports running one seed, one
scenario group, the fast corpus, or the deep corpus:
`cargo run -p studiofs-scenario --bin studiofs-scenario -- run-corpus --corpus fast`.
Metadata and object-store outage groups keep the core success condition stable
while varying recovery flaps and unavailable-read probes from the seed, so deep
corpus output makes seed-controlled fault timing visible instead of repeating a
single trace shape.
Failures report the scenario group, seed, violated invariant, message, and
counterexample artifact path when the underlying suite can write one. The RWX
workflow lives in `.rwx/simulation.yml` with `simulation-fast` and
`simulation-deep` targets and uploads `target/simulation-counterexamples/`.
When a group-specific suite does not have its own replay artifact format yet,
the corpus layer writes a schema-versioned JSON artifact with the corpus,
scenario group, optional scenario-family subgroup, seed, scheduler trace,
operation log, event trace, violated invariant, message, relevant final
snapshot, and stable replay command. The
stable replay command reads the artifact path through `replay-counterexample`;
`run-corpus --seed` remains a seed rerun and does not force saved event choices.
It intentionally does not use Docker, PostgreSQL, RustFS, FUSE, WinFSP,
self-hosted runners, GitHub Actions triggers, schedules, or label-gated PR
runs.

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
7. seeded namespace/file-version collision suite runs the required collision
   families across multiple seeds and records replayable counterexample traces
   when an invariant fails.
8. seeded crash/restart replay suite runs the required replay families across
   multiple seeds, distinguishes memory-only state from locally durable state,
   keeps replay idempotent, and rejects stale upload/publish events for older
   epochs. Namespace creations, removals, and directory renames remain durable
   across restart without making unflushed file bytes durable.

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
