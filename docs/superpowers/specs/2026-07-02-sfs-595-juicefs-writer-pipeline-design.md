# SFS-595 JuiceFS Writer Pipeline Design

## Context

SFS-595 is part of SFS-590 through SFS-598. SFS-590 establishes the new Linux
backend boundary, while SFS-598 will gate the result against JuiceFS parity.
This task owns the shared VFS writer/cache behavior that the Linux backend will
exercise.

The local JuiceFS reference at `/home/val/dev/juicefs` routes foreground writes
through `VFS.Write -> handle.writer.Write -> chunk.Writer`. Its cached store
uses `BufferSize`, `MaxUpload`, `MaxStageWrite`, staged cache files, and
background upload workers so ordinary writes accept data at local writer/cache
speed and remote upload drains later.

StudioFS already has the right base shape in `MountedWriterStack`,
`CachedStore`, deferred materialization, object-id prefetch, and upload
lifecycle workers. The design is to strengthen that path instead of adding a
second writer or acceptance journal.

## Design

Ordinary foreground writes keep copying caller bytes into `MountedWriterStack`
slice writers. Accepted bytes remain immediately readable from active writer
state. The hot path must not perform object upload, PostgreSQL publication, or
metadata replication.

Completed blocks are drained by the existing deferred materialization workers.
The worker should behave like a continuous JuiceFS writeback drain: prefetch
object IDs before the batch needs them, detach completed blocks into durable
local staging, install local overlay slices, and keep running while the open
writer continues to produce completed blocks. Partial tails stay buffered until
idle time or an explicit local barrier.

Backpressure stays bounded by existing dirty-byte and materialization knobs:
`max_dirty_bytes`, eager flush threshold/chunk bytes, `max_drain_batch`, upload
worker count, and cached-store pending upload pressure. Upload backlog alone
must not force ordinary writes into synchronous upload/publication.

Observability must expose the active tuning knobs and the pressure that matters:
writer dirty bytes, staged/pending local bytes, upload queue/backlog, drain
activity, and throttled/admission events.

## Acceptance

- Large sequential writes drain completed blocks before `flush`, `fsync`, or
  `release`.
- A hot open writer keeps a drain task alive long enough to catch the next
  completed block.
- Partial tails are drained after the idle linger window or by an explicit
  local barrier.
- Accepted bytes remain readable while staging/materialization is in progress.
- Backpressure counters and tuning knobs are visible in the write-path
  observability snapshot.
- Focused local writer/cache contract tests pass without requiring PostgreSQL or
  object storage.
