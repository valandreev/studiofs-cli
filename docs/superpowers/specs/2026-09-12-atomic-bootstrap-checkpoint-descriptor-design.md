# Atomic Bootstrap Checkpoint Descriptor

## Purpose

Fresh StudioFS clients need one PostgreSQL read that identifies the volume,
format, published serving checkpoint, and the exact event-log position from
which replay must continue. The current schema stores the checkpoint fields,
but callers read them through bespoke queries; the runtime pull also reads the
metadata head and checkpoint in separate statements. This change makes the
stored row consumable as one validated descriptor without opening
`PgMetaEngine` or its replication workers.

## Design

Keep `metadata_state` as the storage location. Its checkpoint bytes, checksum,
publication state, generation, format version, checkpoint version, and remote
head remain one PostgreSQL row. Join that row with `volumes` in one SELECT so
the volume UUID and all checkpoint fields come from the same MVCC statement.

Expose the result as `BootstrapCheckpointDescriptor` through the existing
`PgSnapshotBackend`, which already opens PostgreSQL without opening local redb
or the live replication engine. The descriptor carries the numeric volume ID,
volume UUID, serialized format information, metadata format version, remote
head version, checkpoint version, checkpoint bytes, checksum, publication
state, and publication generation. `checkpoint_version` is explicitly the
replay floor: subsequent event-log reads use `batch_version > checkpoint_version`.

The read API validates before returning: the PG format is the current format;
the volume identity is positive and non-empty; the remote head and checkpoint
version are non-negative with the checkpoint no newer than the head; the
publication is `published`; generation is positive; the checksum is exactly
SHA-256 of the bytes; the serialized format info is valid and agrees with the
volume ID/UUID; and the checkpoint bytes decode as a serving checkpoint. Invalid
or incomplete rows return a typed `MetaError` and are never imported.

Checkpoint publication remains one `INSERT ... ON CONFLICT DO UPDATE` against
`metadata_state`, so all descriptor fields become visible together and the
generation advances only for an accepted checkpoint version. No new table,
S3 artifact, migration path, coordinator, freshness policy, or atomic local DB
installation is part of this change; those belong to SFS-703, SFS-704, and
SFS-705.

## Consumers and testing

`pull_snapshot_locked` will use the descriptor API instead of its current
two-query head/checkpoint read. `discover` will use the same API for its remote
state fetch while retaining its existing local-install behavior. Pure tests
cover descriptor validation and SQL shape. An ignored PostgreSQL test covers a
real descriptor read and verifies that the replay floor, head, identity, and
checksum are read as one validated record; it is run only when the configured
test PostgreSQL service is available.

## Decisions

- Keep the existing `metadata_state` row instead of adding a descriptor table:
  the required fields already share one row and a second table would duplicate
  publication state.
- Reuse `PgSnapshotBackend` instead of adding another backend type: it already
  provides the no-local-runtime boundary required by this API.
- Return serialized `format_info` in the descriptor because fresh bootstrap
  needs it to build local metadata; its existing redacted `Debug` behavior is
  preserved by not adding secret-bearing debug output.
- Validate the complete descriptor in the metadata crate so discovery and
  runtime pull cannot diverge in their interpretation of a published row.
