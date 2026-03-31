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
//!

// --- New v3 modules ---
pub mod block_cache;
pub mod l1_mem;
pub mod l2_disk;
pub mod lru_tracker;
pub mod metrics;
mod platform_direct_io;
pub mod runtime;
pub mod upload_queue;

// --- Cache v2: single-file block store ---
pub mod v2;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig};
pub use upload_queue::PendingUpload;
