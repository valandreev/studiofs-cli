//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod readahead;
pub mod vfs;

pub use vfs::{OpenFile, OpenFlags, StudioVfs, VfsConfig, VfsError, VfsResult};
pub use readahead::ReadAheadTracker;
