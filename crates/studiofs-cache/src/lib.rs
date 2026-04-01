//! StudioFS Cache Layer (Cache v2).
//!
//! `v2::BlockStore` is the canonical local cache implementation.
//! `block_cache` remains as a compatibility shim for upload-queue and
//! observability surfaces during migration cleanup.

pub mod block_cache;
pub mod metrics;
pub mod upload_queue;

// --- Cache v2: single-file block store ---
pub mod v2;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig, L2IoPriority};
pub use upload_queue::PendingUpload;
