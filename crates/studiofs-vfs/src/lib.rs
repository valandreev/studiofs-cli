//! StudioFS VFS layer.
//! Platform-agnostic virtual filesystem operations bridging metadata, data, and cache.

/// Module with VFS access primitives and metadata visibility controls.
pub mod access;
/// Module with AIMD request-shaping helpers.
pub mod aimd;
/// Module $name.
pub mod bridge;
/// Module with integration test harness abstractions for contracts.
pub mod contract_harness;
/// Module implementing fetch scheduling.
pub mod fetch;
/// Module $name.
pub mod incarnation;
mod landing_zone;
/// Module with lock ordering helpers and diagnostics.
pub mod lock_order;
/// Module $name.
pub mod qos;
/// Module for read-ahead and related access optimization.
pub mod readahead;
mod runtime;
/// Module with upload scheduler internals.
pub mod upload;
/// Module for shared helper functions.
pub mod utils;
/// Module containing the public VFS implementation.
pub mod vfs;

pub use aimd::{AimdConfig, AimdController};
pub use bridge::{BridgeAttrTimes, BridgeVfsErrorKind, classify_vfs_error, normalize_attr_times};
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
    BarrierObservabilityPort, DeferredCleanupHintPort, DeleteCleanupPort, FlushGatePort,
    HandleDrainBarrier, HandleDrainDeleteRequest, HandleDrainRequest, HandleReleasePort,
    KernelInvalidator, LocalSyncBarrierPort, LocalSyncPolicy, LockKind, NamespaceParent,
    NamespacePathStyle, NamespaceResolver, OpenFile, OpenFlags, StatFs, StudioVfs,
    VFS_RUNTIME_EVENT_CAPACITY, VfsAttrReadCacheSnapshot, VfsCacheStateSnapshot, VfsConfig,
    VfsConfigRaw, VfsConfigValidated, VfsError, VfsFetchStats, VfsForegroundReadProtectionStats,
    VfsIngressLane, VfsIngressPressure, VfsIngressStats, VfsLatencyHistogramSnapshot,
    VfsNamespaceCacheSnapshot, VfsPortableAllocatorStats, VfsResult, VfsRuntimeEvent,
    VfsSynchronizationStateSnapshot, VfsTransitMemorySnapshot, VfsUploadStats,
    VfsWritePathBackpressureStats, VfsWritePathBurstStats, VfsWritePathDirtyPressureStats,
    VfsWritePathDrainLagStats, VfsWritePathEagerFlushStats, VfsWritePathLandingZoneStats,
    VfsWritePathLatencyQuantilesMs, VfsWritePathLatencyQuantilesSaturated,
    VfsWritePathLocalCacheIoStats, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats, VfsWritePathTuningStats,
    VfsWritePathWinfspCallbackStats, VfsWritePathWinfspForegroundFlushStats,
    VfsZeroIoCacheBytesSnapshot, VfsZeroIoDrilldownEntry, VfsZeroIoHealthSignal,
    VfsZeroIoRuntimeSnapshot, VfsZeroIoShutdownSnapshot, VfsZeroIoStartupReplaySnapshot,
    is_hidden_non_macos_root_metadata_name,
};
