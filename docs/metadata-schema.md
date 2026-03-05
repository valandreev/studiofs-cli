# Metadata Schema

Source of truth:
- `crates/studiofs-meta/src/schema.rs`

Compatibility:
- `crates/studiofs-meta/src/types.rs` re-exports from `schema.rs`.

Current backend model:
- `MetadataBackend::RedbLocal`
- `MetadataBackend::Postgres`

Key runtime defaults:
- `cache_ttl_secs = 20`
- `flush_interval_secs = 60`
- `flush_ops_threshold = 100`
- `flush_bytes_threshold = 1 MiB`

PostgreSQL config fields:
- `metadata.postgres.url`
- `metadata.postgres.volume_id`
