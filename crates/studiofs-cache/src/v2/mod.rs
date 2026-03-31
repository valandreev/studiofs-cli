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
//! | `mmap_reader`  | Implemented | Lock-free mmap reader with generation remapping     |
//! | `metrics`      | Stub        | Detailed histograms and per-op counters (future)    |

pub mod allocator;
pub mod block_store;
pub mod evictor;
pub mod format;
pub mod index;
pub mod lru;
pub mod metrics;
pub mod mmap_reader;
mod persistence;
pub mod platform;

pub use block_store::{BlockStore, BlockStoreConfig, BlockStoreError};
pub use index::BlockFlags;
