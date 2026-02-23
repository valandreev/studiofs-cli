//! StudioFS Cache Layer
//!
//! Two-level block cache (L1 RAM + L2 NVMe) with unified LRU tracking
//! and integrated writeback queue for asynchronous S3 uploads.
//!
//! ## Architecture (v3)
//!
//! ```text
//! L1 (RAM, 2-4 GB)  ←→  L2 (NVMe, elastic)  ←→  S3 (cold)
//!         ↕                      ↕
//!     Unified LRU Tracker + UploadQueue
//! ```
//!
//! See `docs/cache-v3-design.md` for full design rationale.

// --- New v3 modules ---
pub mod block_cache;
pub mod l1_mem;
pub mod l2_disk;
pub mod lru_tracker;
pub mod upload_queue;

// --- Legacy modules (kept for migration, will be removed) ---
pub mod disk;
pub mod memory;
pub mod slab;
pub mod writeback;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig};
pub use memory::MemoryCache;
pub use upload_queue::PendingUpload;
