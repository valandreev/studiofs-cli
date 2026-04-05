//! Cache v2 — single-file block cache with native allocator and LRU eviction.
//!
//! Module layout:
//!
//! | Module         | Status      | Description                                         |
//! |----------------|-------------|-----------------------------------------------------|
//! | `block_store`  | Implemented | Core `BlockStore` API (open/put/get/remove/evict/…) |
//! | `allocator`    | Implemented | `FreeList`: best-fit, coalesce, tail truncate       |
//! | `format`       | Implemented | On-disk format constants and types                  |
//! | `platform`     | Implemented | pread/pwrite plus best-effort advisory primitives   |
//! | `index`        | Implemented | `BlockIndex` + `BlockFlags` + intrusive LRU (SFS-136) |
//! | `lru`          | Implemented | `LruList` / `LruNode` intrusive doubly-linked list  |
//! | `evictor`      | Stub        | Watermark-driven background evictor (future)        |
//! | `mmap_reader`  | Implemented | Zero-copy mmap reader with generation remapping     |
//! | `metrics`      | Implemented | Runtime counters, rates, throughput, pressure stats |

pub mod allocator;
pub(crate) mod block_store;
pub mod evictor;
pub mod format;
pub mod index;
pub mod local_block_backend;
pub mod lru;
pub mod metrics;
pub mod mmap_reader;
mod persistence;
pub mod platform;
pub mod raw_block_file_backend;

pub use index::BlockFlags;
pub use local_block_backend::{
    LocalBlockBackend, LocalBlockBackendError, LocalBlockLifecycleState,
};
pub use raw_block_file_backend::{
    RawBlockFileBackend, RawBlockFileBackendConfig, RawBlockFileBackendError,
};
use uuid::Uuid;

/// Derive a stable 16-byte local backend volume UUID from a format UUID string.
///
/// Accepts plain UUID strings and falls back to deterministic UUIDv5 over the
/// provided bytes so legacy/non-UUID values still map consistently.
pub fn derive_local_backend_volume_uuid(volume_uuid: &str) -> [u8; 16] {
    Uuid::parse_str(volume_uuid)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, volume_uuid.as_bytes()))
        .into_bytes()
}
