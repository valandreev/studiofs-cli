# Mounted Fault Chaos Convergence Design

## Goal

Add a deeper product E2E scenario that combines mounted namespace conflicts,
infrastructure outages, client crash/restart, and a cold observer client. The
scenario should find bugs where live clients appear to converge through local
cache state but the durable remote truth does not converge for restarted or
newly mounted clients.

## Existing Base

- `mounted-chaos-convergence` already executes seeded multi-client conflict
  families through real mounted paths and validates final mounted tree
  convergence.
- PostgreSQL and RustFS outage scenarios already provide compose service
  stop/start hooks and recovery validation patterns.
- `mounted-client-crash-restart-recovery` already force-kills and remounts a
  client with the same `STUDIOFS_CLIENT_ROOT`.
- Mounted tree validation already snapshots public mounted state only, writes
  per-client trees on failure, and avoids private backend inspection.
- `operations.ndjson` already uses `ScenarioOperationRecord`, so scenario
  artifacts can be replayed by simulation, contract/core, or mounted adapters
  where the operation is supported.

## Scenario Name

`mounted-fault-chaos-convergence`

Aliases:

- `fault-chaos`
- `chaos-faults`

## High-Level Shape

Use four clients against one shared remote volume:

- `client-a`, `client-b`, and `client-c` are warm mutating clients.
- `client-d` is a cold observer. It starts later with a fresh client root/cache
  and never participates in the initial conflict workload.

The scenario is seeded. The seed controls operation families, client ordering,
payload shapes, and which client is force-killed. The same seed must produce the
same planned operation log, while real timing can still expose races through
accepted/rejected outcomes and final convergence behavior.

## Workload Phases

### Phase 1: Healthy Warmup

- Mount `client-a`, `client-b`, and `client-c`.
- Create a bounded initial tree under a scenario root such as `fault-chaos/`.
- Write small and medium payload files.
- Wait for mounted tree convergence across the three warm clients.

### Phase 2: Healthy Conflict Burst

Run a short seeded conflict burst while all services are healthy:

- rename vs rename
- rename vs unlink
- unlink vs append
- overwrite/truncate vs repeated reads
- mkdir/create/rmdir on the same subtree
- readdir/stat while a peer mutates the directory

This keeps the current `mounted-chaos-convergence` coverage as the first part of
the deeper scenario.

### Phase 3: PostgreSQL Outage With Local Writes

- Stop only PostgreSQL through the compose service hook.
- Perform accepted local writes and namespace operations from warm clients.
- Validate only local-first guarantees during the outage:
  - accepted writes remain readable on the origin client;
  - peer visibility is not required while metadata is unavailable;
  - unexpected I/O errors are surfaced, not collapsed into benign absence.
- Restart PostgreSQL and wait for recovery.

### Phase 4: Crash Before Remote Publication Completes

- Select one warm mutating client from the seed.
- Force-kill it after accepted local work and the local durability boundary, but
  before the scenario waits for full remote publication.
- Continue a small amount of peer activity while the killed client is down.
- Remount the killed client using the same `STUDIOFS_CLIENT_ROOT`.

This phase targets bugs where replay restores local pending work but does not
publish or reconcile it correctly.

### Phase 5: RustFS Outage With Payload Pressure

- Stop only RustFS/S3 through the compose service hook.
- Perform payload-bearing writes that are large enough to exercise upload/cache
  behavior without making validation expensive.
- Validate local reads for accepted writes.
- Restart RustFS/S3 and wait for upload recovery.

This phase targets bugs where metadata and namespace converge but payload
availability or checksums diverge.

### Phase 6: Cold Observer Bootstrap

- Mount `client-d` with a fresh client root/cache after all faults have
  recovered.
- `client-d` performs no writes.
- Validate that the cold observer sees the same final tree and payload checksums
  as the warm clients.

The cold observer is the key new detector: if convergence only exists in warm
client caches, `client-d` should expose the durable truth mismatch.

## Validation

The final pass condition is mounted-public only:

- all four clients converge to the same public tree;
- source paths from winning rename/unlink conflicts do not resurrect;
- losing rename destination aliases do not survive after an unlink winner;
- subtree races do not leave partial directory states;
- files with accepted final payloads have matching checksums on every client;
- the restarted client and the cold observer match the warm clients.

The scenario should also preserve operation-level evidence:

- every planned operation is recorded in `operations.ndjson`;
- accepted, rejected, and skipped outcomes are explicit;
- outage and crash/restart boundaries are recorded as scheduler checkpoints or
  supported operation records where the shared schema already has a type;
- mounted-only actions remain marked as mounted-only rather than pretending to
  be portable simulation coverage.

## Artifacts

Write scenario-specific artifacts under
`artifacts/scenarios/mounted-fault-chaos-convergence/`:

- `operations.ndjson`
- `mounted-fault-chaos-summary.json`
- `validation.json`
- per-client tree snapshots and `diff.json` on convergence failure

The summary should include:

- seed
- participating clients
- cold observer client
- killed/restarted client
- outage durations
- accepted/rejected/skipped operation counts
- final convergence duration
- validation artifact path

## Acceptance Checks

- `cargo run -p studiofs-e2e -- run mounted-fault-chaos-convergence --seed 360 --studiofs-bin target\debug\studiofs.exe` runs the scenario through compose infra.
- A disabled-infra runner contract test verifies deterministic artifact
  planning without Docker or mounted clients.
- Focused unit tests verify operation planning, summary output, primary-error
  preservation when cleanup also fails, and scenario registry aliases.
- Repeated runs of a selected seed validate that warm clients, the restarted
  client, and the cold observer converge to the same tree.
- Existing focused outage, crash/restart, collision, and mounted chaos scenarios
  continue to pass.

## Non-Goals For The First Slice

- No full workspace test suite in local development.
- No private PostgreSQL, object-store, redb, or VFS inspection to decide
  scenario success.
- No unbounded stress workload or large directory fanout.
- No Toxiproxy latency/packet-loss injection yet. Add network shaping after the
  first fault-chaos scenario is stable.
- No attempt to prove a specific conflict winner unless the scenario records an
  authoritative order that makes the winner observable and replayable.
