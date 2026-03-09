# NVMe Ingress First Slice

## Goal

Remove the most visible foreground stalls caused by draining `dirty_mem`
directly into metadata/cache/upload work, without yet rewriting the whole write
pipeline around a true append-only ingress log.

This slice is intentionally a bounded spill-first step, not the final ingress
architecture.

## Scope

Phase 1 implementation target:

- keep `pending_blocks` as the immediate per-handle write buffer
- keep `dirty_mem` as the first materialized block queue in RAM
- promote `ingress_spill` from overflow escape hatch into a first-class bounded
  local acceptance tier
- make foreground backpressure depend on:
  - combined `dirty_mem + ingress_spill` budget
  - local disk health / local filesystem errors
  - explicit queue saturation
- do not require metadata materialization, upload journal creation, or S3
  progress before foreground writers can continue

Out of scope for this slice:

- append-only segment format
- full ingest index / checkpoint subsystem
- direct read path from a new segment log
- remote durability guarantees stronger than current model

## Foreground Acceptance Point

Foreground `write()` success in this slice means:

1. bytes are recorded in `pending_blocks`
2. once a block is materialized out of `pending_blocks`, it must land in either:
   - `dirty_mem`, or
   - `ingress_spill`
3. if RAM is over budget, the foreground path may synchronously spill to local
   NVMe
4. foreground returns after local acceptance into RAM or spill

Foreground must not wait for:

- `drain_dirty_mem()` to finish a metadata batch
- upload queue journal submission
- `commit_l2_batch()`
- PostgreSQL metadata flush
- S3 upload completion

## Concrete Data Model

Keep the existing logical states:

- `pending_blocks`
- `dirty_mem`
- `ingress_spill`
- cache L2 blocks
- upload journal

But tighten the contract:

- `dirty_mem`
  - hot RAM-resident accepted blocks
- `ingress_spill`
  - accepted local overflow blocks on NVMe
- `drain_dirty_mem()`
  - must drain both RAM and spill uniformly

Required invariant:

- a block may be in RAM only, spill only, or both transiently during handoff
- but it must have exactly one current logical staged owner in VFS state

## Replay / Recovery

First-slice crash recovery requirement:

- if a block was accepted into `ingress_spill`, restart must recover it as local
  staged data before any remote fetch path is consulted

Minimum recovery design:

1. persist spill records under a dedicated spill directory
2. store enough metadata per record to recover:
   - `ino`
   - `block_index`
   - `content_epoch`
   - block length
   - staging timestamp or sequence
3. on startup:
   - scan spill directory
   - rebuild in-memory staged map
   - ignore superseded records using latest `(ino, block_index, content_epoch)`
4. only after rebuild may normal background draining resume

This slice does not need segment compaction yet.
File-per-record or file-per-block spill is acceptable if the write/read path
stays simple and crash-safe.

## Backpressure Policy

Order of operations:

1. accept into `pending_blocks`
2. materialize into `dirty_mem` while RAM is below budget
3. spill overflow into `ingress_spill`
4. block only when:
   - RAM budget is full
   - spill budget is full
   - spill directory is unhealthy or unwritable

Foreground must not block merely because:

- upload workers are behind
- PostgreSQL flush is behind
- S3 is slow
- metadata materialization latency spikes

Required tunables:

- `max_dirty_bytes`
- `max_spill_bytes`
- `STUDIOFS_SLAB_FLUSH_INTERVAL_MS` for controlling background drain cadence
- `STUDIOFS_FOREGROUND_DRAIN_ASSIST` for choosing whether foreground
  backpressure may assist local drain
- optional per-inode or per-handle cap if global fairness becomes a problem

## Read Path Contract

The current read order is preserved:

1. `pending_blocks`
2. `dirty_mem`
3. `ingress_spill`
4. cache
5. S3

This is the main reason the slice can remain incremental.
No serving-path semantic regression is acceptable here.

## Metadata Contract

This slice does not change metadata visibility semantics.

- local reads still observe staged writes immediately
- remote visibility still begins only after upload completion + metadata flush
- no PG or `studiofs-meta` schema change is required for the first ingress step

That boundary is important: ingress first-slice is an I/O-pipeline change, not a
metadata-model change.

## Observability Required

Add or confirm metrics for:

- current `dirty_mem` bytes
- current `ingress_spill` bytes
- spill records count
- oldest spill record age
- time blocked by write backpressure
- drain throughput from RAM
- drain throughput from spill
- number of writes forced to spill

Current implementation cut:

- `StudioVfs::ingress_stats()` exposes the first local snapshot needed for
  these counters
- `StudioVfs::ingress_pressure_for()` exposes the immediate acceptance decision
  (`dirty_mem` vs `ingress_spill` vs local backpressure) for a prospective
  payload
- periodic upload-pipeline logs include the current ingress snapshot so recovery
  and backpressure behaviour can be inspected without a separate metrics system
- drain accounting now distinguishes bytes/blocks drained from RAM vs
  `ingress_spill`
- non-test regressions can also slow normal drain cadence via
  `STUDIOFS_SLAB_FLUSH_INTERVAL_MS`, reducing dependence on debug-only hooks
- wait-time regressions can disable foreground drain assist through
  `STUDIOFS_FOREGROUND_DRAIN_ASSIST=0` and observe natural background release

Without these metrics, we will not be able to tell whether the slice actually
improves user-visible latency.

## Exit Criteria

This slice is done when:

1. a sustained large write no longer stalls a second foreground writer just
   because background metadata/upload work is behind
2. crash recovery restores accepted spill-backed staged blocks
3. read-after-write still prefers local staged data
4. backpressure is triggered only by bounded local resource exhaustion
5. no PostgreSQL or S3 round-trip is required for local write acknowledgment

## Next Step After This Slice

Once the spill-first slice is stable:

1. replace ad hoc spill files with append-only ingress segments
2. add replay checkpoints / compaction
3. move the acceptance boundary fully from `dirty_mem`/spill into a dedicated
   local ingest log
