//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod vfs;

pub use vfs::{StudioVfs, VfsConfig, VfsError, VfsResult};
