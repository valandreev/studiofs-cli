//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod readahead;
pub mod vfs;

pub use readahead::{DirAccessTracker, ReadAheadTracker};
pub use studiofs_meta::types::{SetattrArgs, TimeSpec};
pub use vfs::{LockKind, OpenFile, OpenFlags, StatFs, StudioVfs, VfsConfig, VfsError, VfsResult};
