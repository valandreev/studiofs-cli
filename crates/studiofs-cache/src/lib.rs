//! StudioFS Cache Layer (Cache v2).
//!
//! `v2::RawBlockFileBackend` is the active local block backend.
//! `block_cache` remains as a compatibility shim for upload-queue and
//! pending-upload tracking at the write-path boundary.

pub mod block_cache;
pub mod metrics;
pub mod upload_queue;

// --- Cache v2: single-file local block backend ---
pub mod v2;

// --- Re-exports ---
pub use block_cache::{BlockCache, BlockCacheConfig, L2IoPriority};
pub use upload_queue::PendingUpload;
