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
pub use qos::QosController;
pub use readahead::{DirAccessTracker, ReadAheadTracker};
pub use runtime::{
    AclMeta, JanitorMeta, ReadPathMeta, UploadMeta, VfsCommitMeta, VfsMeta, into_vfs_meta,
};
pub use studiofs_meta::types::{SetattrArgs, TimeSpec};
pub use upload::UploadScheduler;
pub use utils::parse_size_bytes;
pub use vfs::{
    CacheSafetyMode, KernelInvalidator, LockKind, NamespaceParent, NamespacePathStyle,
    NamespaceResolver, OpenFile, OpenFlags, StatFs, StudioVfs, VfsConfig, VfsConfigRaw,
    VfsConfigValidated, VfsError, VfsForegroundReadProtectionStats, VfsIngressLane,
    VfsIngressPressure, VfsIngressStats, VfsLatencyHistogramSnapshot, VfsResult, VfsUploadStats,
    VfsWritePathBackpressureStats, VfsWritePathBurstStats, VfsWritePathEagerFlushStats,
    VfsWritePathLatencyQuantilesMs, VfsWritePathLatencyQuantilesSaturated,
    VfsWritePathObservabilitySnapshot, VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats,
    VfsWritePathTuningStats, VfsWritePathWinfspCallbackStats,
    VfsWritePathWinfspForegroundFlushStats,
};
