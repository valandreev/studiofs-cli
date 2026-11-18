# A Fenced Local Graph Survives An Unclean Shutdown

Status: accepted.

## Context

Ordinary local `redb` commits use `Durability::None`; an idle worker, `fsync`
and clean shutdown make them durable. After a crash, redb recovers the last
durable commit, which is a consistent prefix of local history. Before this
decision every unclean PostgreSQL-volume image set the local-graph repair
marker, reset its replay cursors and replaced its graph with a PostgreSQL
checkpoint before the mount could start. The reason was real: a flush could
publish events that only lived in non-durable commits. After a crash the image
could be older than this client's own published history, and replay treats
own batches as echoes, so the client would never learn what it had published.

That repair needs PostgreSQL. A writer that crashed while PostgreSQL was down
could not remount at all, even though every accepted, `fsync`ed change was in
its durable image. A client that restarted offline also stayed read-only until
PostgreSQL answered the identity probe, unlike a mounted client that loses
PostgreSQL and keeps its write authority.

## Decision

Publication is fenced. `push_projected_event_log_batch_locked` runs the
immediate redb barrier (`sync_local_metadata`), which makes every earlier
volatile commit durable, before it opens the PostgreSQL transaction. Every
event peers can see is therefore in the durable image, together with the
allocator state and the trash receipts that produced it. Every `PgMetaEngine` open sets the local-only counter
`pgPublicationDurabilityFence`.

On an unclean reopen, an image with that counter and no pending graph repair
keeps its own graph and cursors. Its unpublished journal is served and later
flushed like after a clean reopen. A published batch whose acknowledgement was
lost replays as a self batch and matches the durable journal, the same path
that heals a lost commit response while mounted. Only an image without the
counter (written by an older binary) or one already being repaired takes the
PostgreSQL graph repair.

An established client (persisted client slot, no fresh-client or repair
marker) that restarts while PostgreSQL is unreachable keeps local write
authority. The grant happens only after startup or a remote operation sees a
reachability failure. It ends the first time PostgreSQL answers the identity
probe, match or mismatch. Publication and replay still wait for that probe, so
a volume whose identity changed never receives these mutations. Fresh and
repairing clients stay read-only until their startup target, as before.

## Consequences

A writer killed with PostgreSQL and S3 down remounts writable from its own
durable state, and its offline work publishes when the services return
(`mounted-recoverable-trash-durability`). Same-cache crash reopens with
PostgreSQL available no longer replace the graph, so unpublished namespace
state such as trash receipts stays visible.

Each publication attempt that passes remote format validation adds one local
`fsync`. This happens on the background replication path, not in a filesystem
callback.

A binary older than this decision that runs against a volume after a newer one
can leave the counter set while publishing unfenced. Downgrading after a crash
is not supported.
