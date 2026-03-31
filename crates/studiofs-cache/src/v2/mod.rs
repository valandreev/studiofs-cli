//! Cache v2 — single-file block cache with native allocator.
//!
//! Module layout:
//!
//! | Module         | Status      | Description                                    |
//! |----------------|-------------|------------------------------------------------|
//! | `block_store`  | Implemented | Core `BlockStore` API (open/put/get/remove/…)  |
//! | `allocator`    | Implemented | `FreeList`: best-fit, coalesce, tail truncate  |
//! | `format`       | Implemented | On-disk format constants and types             |
//! | `platform`     | Implemented | pread/pwrite I/O abstraction                   |
//! | `index`        | Stub        | Persistent index (SFS-136)                     |
//! | `lru`          | Stub        | Intrusive LRU list (SFS-136)                   |
//! | `evictor`      | Stub        | Eviction policy (SFS-136)                      |
//! | `mmap_reader`  | Stub        | Lock-free mmap reader (future)                 |
//! | `metrics`      | Stub        | Detailed counters (future)                     |

pub mod allocator;
pub mod block_store;
pub mod evictor;
pub mod format;
pub mod index;
pub mod lru;
pub mod metrics;
pub mod mmap_reader;
pub mod platform;

pub use block_store::{BlockStore, BlockStoreConfig, BlockStoreError};
