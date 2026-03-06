# NVMe Ingress Architecture

## Goal

StudioFS should feel like a local NVMe-backed filesystem for hot-path writes and cache hits.

User-visible expectations:

- Writing should acknowledge quickly and remain responsive during sustained bursts.
- Starting a second large write should not stall behind background metadata or upload work from the first.
- Copying trees with many small files should not pause every few hundred files.
- Read-after-write on the same client should behave like a local filesystem.
- Remote PostgreSQL metadata and object storage should be downstream consumers of local accepted writes, not blockers on the foreground path.

Non-goals:

- Making remote reads as fast as local cache misses. Remote reads remain best-effort.
- Synchronous durability to S3 on every write.

## Problem With Current Model

Today the hot path is still indirectly coupled to metadata and upload pipeline progress:

1. `write()` fills `pending_blocks`.
2. Full blocks move into `dirty_mem`.
3. `background_slab_flusher` allocates object IDs, commits metadata, stages cache, and queues uploads.
4. Foreground backpressure is released only after `dirty_mem` is drained.

This creates visible pauses when:

- metadata commit is slow
- upload journal / cache local I/O is slow
- many small files create lots of tiny pipeline transitions
- a previous large write has not finished draining its local pipeline

The result is not "local NVMe feel". It is "foreground writes wait on background bookkeeping".

## Target Mental Model

We want a local ingest layer with this contract:

- Foreground write success means: data is durably accepted into local NVMe ingress storage.
- Metadata materialization and remote upload happen asynchronously after acceptance.
- Foreground reads consult local pending/staged state first, so read-after-write remains immediate.
- Backpressure is driven by bounded local resources, not by remote system latency.

This is effectively a small local write-ahead / ingest log.

## Desired Pipeline

### Stage 0: Foreground Accept

`write()` should do only the minimum required to accept data locally:

- append block payload to local NVMe ingress storage
- register lightweight in-memory mapping from `(ino, block_index)` to local staged block
- update dirty size / inode attributes in local state
- return

Foreground should not wait for:

- PostgreSQL snapshot push
- upload journal file create/rename for every block
- S3 upload scheduling/completion
- expensive metadata batch commit

### Stage 1: Local Materialization

Background worker:

- reads ingress records
- allocates `object_id`
- updates local metadata / block refs
- marks superseded blocks and deletions
- moves block into normal cache lifecycle

This stage turns "accepted data" into "filesystem-visible materialized block state".

### Stage 2: Remote Propagation

Separate worker:

- creates durable upload intent journal
- uploads object to S3
- marks upload complete
- unpins / finalizes local state

### Stage 3: Cleanup / Finalization

After metadata + upload state are consistent:

- remove ingress record
- drop temporary staged references
- GC canceled / superseded staged blocks

## Invariants

These invariants matter more than implementation detail.

1. Foreground write acknowledgment must only depend on local NVMe ingress acceptance.
2. Read-after-write on the same client must succeed even before remote upload.
3. No committed metadata may reference a block that cannot be recovered locally after crash.
4. Delete/truncate/overwrite must correctly cancel or supersede staged blocks.
5. Backpressure must come from bounded local budgets:
   - ingress bytes budget
   - RAM budget
   - number of staged operations
   - local disk full / local NVMe failure

## Crash Recovery Model

We need to recover across these states:

### A. Ingress accepted, not materialized

- block exists only in local ingress log/spool
- recovery must replay materialization

### B. Materialized locally, upload not complete

- metadata / block refs exist
- upload intent exists
- recovery must resume upload

### C. Uploaded, finalization not complete

- object may already exist remotely
- local journal cleanup may be missing
- recovery must reconcile and complete locally

### D. Superseded or deleted staged block

- block should never be reintroduced by replay
- replay must honor tombstones / supersession records

## Recommended Storage Layout

Under the local NVMe cache root:

- `ingress/segments/`
  - append-only segment files for staged block payloads
- `ingress/index/`
  - lightweight lookup/index for live staged entries
- `ingress/checkpoints/`
  - replay checkpoints / compaction state
- `upload/`
  - upload intent journal
- `blocks/`
  - normal L2 cache storage

The important part is separation:

- ingress answers "what has been accepted locally?"
- upload journal answers "what still must reach object storage?"
- metadata answers "what block is the current file state?"

## Read Path Requirements

Read priority should be:

1. pending in-memory write buffer
2. ingress/staged local block
3. L1/L2 cache
4. remote object store

Without this, read-after-write will regress during the transition to ingest-first architecture.

## Semantics of Flush / Fsync

We need explicit semantics.

Recommended:

- `flush()`
  - moves pending in-memory state into local accepted ingest state
  - does not require remote upload completion

- `fsync(datasync=true)`
  - ensures data is durably persisted locally in ingress storage
  - does not require S3 completion

- `fsync(datasync=false)`
  - ensures local data + local metadata are durable
  - still does not require S3 completion unless a stricter mode is added later

This matches the "local NVMe feel" target much better than waiting on remote propagation.

## Backpressure Model

Backpressure should be applied in this order:

1. Prefer RAM buffer for very hot writes.
2. Spill to local NVMe ingress when RAM budget is exceeded.
3. Only block foreground writes when ingress disk budget is exhausted or local pipeline is unhealthy.

Foreground should not block merely because:

- PostgreSQL is busy
- upload queue is temporarily behind
- many journal files are being written

## Minimal Migration Path

### Phase 1

Immediate practical step:

- keep existing architecture
- decouple expensive local journal I/O from foreground-adjacent drain path
- introduce spill/ingress overflow instead of hard waiting on dirty RAM budget

### Phase 2

Introduce bounded disk-backed spill queue:

- overflow of `dirty_mem` goes to local disk staging
- `background_slab_flusher` drains RAM + spill uniformly
- hard backpressure only when combined staging budget is full

### Phase 3

Promote spill queue into real ingress layer:

- writes are accepted into ingress first
- metadata materialization becomes downstream async stage
- read path checks ingress directly

### Phase 4

Add compaction / segment cleanup:

- reclaim superseded and finalized ingress entries
- checkpoint replay position
- keep restart time bounded

## Observability Needed

When this work resumes, add metrics/logging for:

- ingress bytes used
- ingress records count
- oldest ingress record age
- dirty RAM bytes
- staged-to-materialized throughput
- upload backlog count
- upload backlog age p50/p95/max
- metadata materialization latency
- time spent blocked by backpressure
- reason for backpressure

Without this, tuning will remain guesswork.

## Risks

This architecture has real cost:

- more moving parts
- more crash states
- more local disk usage
- more code around overwrite/delete/truncate races
- need for replay/compaction correctness

But this is aligned with the product goal. If StudioFS should feel local on a fast NVMe cache, then local accepted-write state must become a first-class subsystem rather than an incidental side effect of cache + upload queue.

## Decision

Long-term direction for StudioFS:

- move toward a local NVMe-backed ingress / mini-WAL architecture
- use remote metadata and object storage as asynchronous downstream systems
- preserve strong local responsiveness even under heavy write bursts

Short-term:

- remove obvious foreground stalls caused by local bookkeeping in the current pipeline
- keep changes incremental and compatible with the future ingress design
