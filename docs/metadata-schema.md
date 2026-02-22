# Metadata Schema

StudioFS metadata schema has a single source of truth:
- `crates/studiofs-meta/src/schema.rs`

Compatibility note:
- `crates/studiofs-meta/src/types.rs` now re-exports from `schema.rs` and must not define divergent structs.

Manifest/version note:
- `FormatInfo.version` is reserved for remote metadata manifest versioning (ETag/If-Match semantics).
- Metadata runtime defaults:
  - `cache_ttl_secs = 20`
  - `flush_interval_secs = 60`
  - `flush_ops_threshold = 100`
  - `flush_bytes_threshold = 1 MiB`
