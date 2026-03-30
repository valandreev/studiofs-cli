#![forbid(unsafe_code)]
//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

pub mod access;
pub mod aimd;
pub mod fetch;
pub mod incarnation;
pub(crate) mod ingress_wal;
pub mod lock_order;
pub mod memory_envelope;
pub mod qos;
pub mod readahead;
mod runtime;
pub mod upload;
pub mod utils;
pub mod vfs;

pub use aimd::{AimdConfig, AimdController};
pub use fetch::FetchScheduler;
pub use incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
pub use lock_order::{LockOrderGuard, LockTier, debug_lock_stack, is_validation_enabled};
pub use memory_envelope::MemoryEnvelope;
pub use qos::QosController;
pub use readahead::{DirAccessTracker, ReadAheadTracker};
pub use studiofs_meta::types::{SetattrArgs, TimeSpec};
pub use upload::UploadScheduler;
pub use utils::parse_size_bytes;
pub use vfs::{
    CacheSafetyMode, KernelInvalidator, LockKind, NamespaceParent, NamespacePathStyle,
    NamespaceResolver, OpenFile, OpenFlags, StatFs, StudioVfs, VfsConfig, VfsConfigRaw,
    VfsConfigValidated, VfsError, VfsForegroundReadProtectionStats, VfsIngressLane,
    VfsIngressPressure, VfsIngressStats, VfsReadFastPathStats, VfsResult, VfsUploadStats,
};
