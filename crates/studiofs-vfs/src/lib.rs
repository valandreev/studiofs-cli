//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod readahead;
pub mod vfs;

pub use vfs::{LockKind, OpenFile, OpenFlags, StatFs, StudioVfs, VfsConfig, VfsError, VfsResult};
pub use studiofs_meta::types::{SetattrArgs, TimeSpec};
pub use readahead::{DirAccessTracker, ReadAheadTracker};
