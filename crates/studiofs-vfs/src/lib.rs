//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod vfs;
pub mod vfs_v2;

pub use vfs::{StudioVfs, VfsConfig, VfsError};
