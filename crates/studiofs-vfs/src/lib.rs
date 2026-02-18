//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod vfs;
pub mod vfs_v2;

// V2 is the active implementation (block-based architecture).
// V1 (vfs module) kept for reference / format-migration tooling.
pub use vfs_v2::{StudioVfs, VfsConfig, VfsError, VfsResult};
