# Metadata Schema

Source of truth:
- `crates/studiofs-meta/src/schema.rs`

Compatibility:
- `crates/studiofs-meta/src/types.rs` re-exports from `schema.rs`

Design reference:
- [`base-overlay-design.md`](./base-overlay-design.md)

## Backend Modes

- `MetadataBackend::RedbLocal`
- `MetadataBackend::Postgres`

## Important Inode Fields

Regular files now use two size/generation concepts:
- `size`
  - local size visible to the current client
- `committed_size`
  - remote-visible size after committed uploads
- `content_epoch`
  - monotonic content generation used to reject stale async upload completions

## Important Block Tables

Local `redb` distinguishes:
- `file_blocks`
  - staged/local-serving block refs
- `committed_file_blocks`
  - remote-visible block refs

Read serving merges them as:
- local staged block wins
- otherwise fall back to committed block

## PostgreSQL Shared Schema

The shared PostgreSQL schema uses:
- `volumes`
  - logical volumes within database `studiofs`
- `event_log`
  - append-only remote-visible metadata deltas
- `metadata_state`
  - per-volume checkpoint snapshot plus monotonic `version`

Important cursor rule:
- runtime pull follows `batch_version`, not `seq_id`

## Config Fields

- `metadata.postgres.url`
  - PostgreSQL server DSN; StudioFS rewrites it to database `studiofs`
- `metadata.postgres.volume_id`

## Runtime Defaults

- `cache_ttl_secs = 20`
- `flush_interval_secs = 60`
- `flush_ops_threshold = 100`
- `flush_bytes_threshold = 1 MiB`
