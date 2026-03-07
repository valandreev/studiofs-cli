# Append-Only Ingress Segments

This document defines the next architectural phase after the spill-first
ingress slice from [nvme-ingress-first-slice.md](./nvme-ingress-first-slice.md).

The goal is to replace ad hoc per-block spill files with a bounded append-only
local ingest log while preserving the same user-visible contract:

- foreground writes acknowledge after local acceptance
- read-after-write remains local-first
- metadata and S3 stay downstream consumers of accepted writes

## Why Move Past Spill Files

The spill-first slice proved the acceptance model and removed the most visible
foreground stalls, but it still has structural limits:

- file-per-block spill creates too much filesystem metadata churn
- replay cost grows with spill file count
- supersession / overwrite tracking is implicit and scattered
- compaction and bounded restart time are awkward
- the acceptance layer is still split between `dirty_mem` and ad hoc spill files

Append-only segments solve these by turning local acceptance into a first-class
log with explicit replay and checkpoint semantics.

## Target Model

Foreground write acceptance should move from:

- `pending_blocks`
- `dirty_mem`
- `ingress_spill`

to:

- `pending_blocks`
- `ingress segments`
- optional hot RAM staging only as a cache/latency optimization, not as the
  ownership boundary

The ownership boundary becomes:

- once a block leaves `pending_blocks`, it is accepted by being appended to an
  ingress segment
- all later phases consume from that accepted segment state

## Storage Layout

Under the cache root:

- `ingress_spill/segments/`
  - immutable append-only segment files
- `ingress_spill/checkpoints/`
  - compact replay checkpoints
- `ingress_spill/state.json`
  - current writer head / active segment metadata
- `upload/`
  - upload intent journal
- `blocks/`
  - normal L2 block cache

Segment filenames should be monotonic and sortable, for example:

- `0000000000000001.seg`
- `0000000000000002.seg`

Checkpoint filenames should encode the highest fully incorporated segment id.

## Record Types

Each segment stores framed records. Minimum record classes:

1. `PutBlock`
   - `ino`
   - `block_index`
   - `content_epoch`
   - `logical_len`
   - payload bytes
   - acceptance timestamp / sequence

2. `InvalidateBlock`
   - marks a previously accepted staged block as superseded by overwrite,
     truncate, unlink, or rename-overwrite side effects

3. `Barrier`
   - optional cut point for checkpointing / replay safety

This phase does not need a full general WAL for every inode mutation. It only
needs enough segment state to recover accepted local data and supersession.

## In-Memory Index

At runtime, maintain an index:

- key: `(ino, block_index)`
- value:
  - segment id
  - record offset
  - `content_epoch`
  - logical length
  - live/dead state

This replaces the old ad hoc `dirty_spill` map.

Rules:

- latest accepted live record wins
- `InvalidateBlock` marks older entries dead
- reads consult the live index before block cache / remote fetch

## Read Path

Read order in the segment phase:

1. `pending_blocks`
2. live ingress index -> segment payload
3. L1/L2 cache
4. remote object store

This must preserve the same semantics already validated in the spill-first
slice:

- local read-after-write stays immediate
- remote/materialized state never overrides newer accepted local data

## Materialization Pipeline

Background materializer:

1. scans live accepted entries from the ingress index
2. allocates `object_id`
3. writes metadata / block refs
4. stages L1/L2 cache
5. creates upload intent
6. marks the record materialized

Materialization does not remove the segment record immediately. Final cleanup
happens only after:

- local metadata is durable
- upload state is durable enough to resume
- the record is either superseded or safely represented in a checkpoint

## Crash Recovery

Startup recovery:

1. load the latest checkpoint, if any
2. replay segments newer than the checkpoint
3. rebuild the live ingress index
4. resume materialization/upload workers

Required guarantees:

- an accepted block is recoverable even if metadata was never materialized
- superseded records do not reappear as live data after replay
- restart time is bounded by checkpoint interval plus unreclaimed tail segments

## Checkpoint / Compaction

Compaction goal:

- keep replay bounded
- reclaim dead/superseded/finalized records

Minimal strategy:

1. periodically write a checkpoint of the current live ingress index
2. persist the highest covered segment id
3. delete older fully covered segments whose live records are absent from the
   checkpoint

This does not need to be fancy on day one. It just needs to be correct and
bounded.

## Interaction With Existing Spill-First Slice

The migration should be incremental:

1. keep `pending_blocks`
2. replace `ingress_spill` writes with append-to-segment writes
3. replace the old `dirty_spill` runtime map with a live ingress index
4. keep current materialization/upload code paths as downstream consumers
5. only after that consider shrinking `dirty_mem` into a pure short-lived hot
   buffer

The important point is to preserve the already-hardened contract while changing
the storage representation underneath it.

Current implementation cut:

- append-only segment records now exist under `ingress_spill/segments/`
- accepted segment-backed writes emit `PutBlock` records into
  `ingress_spill/segments/`
- spill entry removal emits `InvalidateBlock` records
- a recovery parser for latest-live segment records exists and is covered by
  unit tests
- startup recovery already rebuilds live staged state from segment records
- new runtime ingress entries now read payload from the segment record as the
  primary source
- new accepted entries no longer create legacy `.spill` files; old `.spill`
  files are no longer part of normal recovery
- runtime accepted state is now explicitly represented as an ingress live index
  (`IngressLiveIndex`) instead of an ad hoc spill-file map
- the slab flusher now compares and removes live ingress entries using the
  exact segment payload slice, rather than assuming whole-file spill artifacts
- clean shutdown now persists an ingress checkpoint snapshot under
  `ingress_spill/checkpoints/`
- startup recovery can seed the live ingress index from that checkpoint and
  replay only the newer segment tail instead of rescanning the whole file
- runtime now persists `ingress_spill/state.json` as a durable writer-head hint
  for `active_segment_id` and `next_seq`
- foreground append now rotates to a new segment once the active segment hits
  `STUDIOFS_INGRESS_SEGMENT_MAX_BYTES`
- a background checkpoint worker now persists ingress recovery state on a
  periodic interval, rather than only at clean shutdown
- checkpoint policy is now dual-triggered:
  - fixed interval for eventual bounded replay
  - immediate wakeup once uncheckpointed append bytes or ingress mutations
    cross configured thresholds
- checkpoint policy also reacts to segment topology:
  - when the active writer head gets too many segments ahead of the last
    checkpoint
  - when the live ingress index still spans too many distinct segment ids
- checkpoint persist prunes older fully covered segments that are no longer
  referenced by the live ingress index
- ingress observability now includes checkpoint progress in runtime counters
  and `studiofs status`

This is intentionally not the full cutover yet. The serving path still uses the
current local ingress index shape, but that index now points at segment payload
for newly accepted entries while the segment format and invalidation semantics
stabilize. `state.json` is currently a recovery hint rather than the sole
source of truth; segment replay and checkpoints still win if the hint is stale.

## Non-Goals For This Phase

Do not combine this phase with:

- a new metadata model
- remote durability guarantees stronger than the current system
- cross-client ingest replication
- full generalized WAL for every namespace mutation

Those would expand the scope unnecessarily.

## Exit Criteria

This phase is successful when:

1. accepted local writes no longer depend on file-per-block spill files
2. replay time is bounded by checkpoints rather than raw spill file count
3. read-after-write still prefers local accepted data
4. overwrite/truncate/unlink supersession is explicit in replay state
5. background materialization/upload still reuses the current downstream model
