#![forbid(unsafe_code)]
//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

/// Access control and permission checks shared by VFS entry points.
pub mod access;
pub mod aimd;
/// Bridge integration utilities for error normalization and VFS boundary conversions.
pub mod bridge;
pub mod contract_harness;
pub mod fetch;
/// File incarnation and upload state tracking for durable VFS metadata.
pub mod incarnation;
mod landing_zone;
pub mod lock_order;
mod ordered_mutex;
/// Quality-of-service policies and controller plumbing for write-path behavior.
pub mod qos;
pub mod readahead;
mod runtime;
pub mod snapshot_worker;
pub mod upload;
pub mod utils;
pub mod vfs;

pub use aimd::{AimdConfig, AimdController};
pub use bridge::{BridgeAttrTimes, BridgeVfsErrorKind, classify_vfs_error, normalize_attr_times};
pub use fetch::FetchScheduler;
pub use incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
pub use lock_order::{LockOrderGuard, LockTier, debug_lock_stack, is_validation_enabled};
pub use ordered_mutex::OrderedMutex;
pub use qos::QosController;
pub use readahead::{DirAccessTracker, ReadAheadTracker};
pub use runtime::{
    AclMeta, JanitorMeta, ReadPathMeta, UploadMeta, VfsCommitMeta, VfsMeta, into_vfs_meta,
};
pub use studiofs_meta::types::{SetattrArgs, TimeSpec};
pub use upload::UploadScheduler;
pub use utils::parse_size_bytes;
pub use vfs::{
    DirEntryWithAttr, KernelInvalidator, LocalSyncPolicy, LockKind, NamespaceParent,
    NamespacePathStyle, NamespaceResolver, OpenFile, OpenFlags, RUNTIME_STATS_FILE_INO,
    RUNTIME_STATS_FILE_NAME, RUNTIME_STATS_FILE_NAME_STR, RUNTIME_STATS_FILE_REPORTED_SIZE,
    RUNTIME_STATS_SCHEMA_VERSION, RUNTIME_STATS_SECTIONS, ReadAheadOwner, RuntimeLatencyBucket,
    RuntimeLatencyHistogram, RuntimeScalarMetric, RuntimeStatsParseError, RuntimeStatsSection,
    RuntimeStatsSnapshot, StatFs, StudioVfs, VFS_RUNTIME_EVENT_CAPACITY, VfsAttrReadCacheSnapshot,
    VfsCachePipelineMaterializeStats, VfsCachePipelineStats, VfsCacheStateSnapshot, VfsConfig,
    VfsConfigRaw, VfsConfigValidated, VfsError, VfsFetchStats, VfsForegroundReadProtectionStats,
    VfsFuseAsyncReadSnapshot, VfsFuseAsyncWriteSnapshot, VfsFuseConnectionSnapshot,
    VfsFuseInvalidationSnapshot, VfsIngressLane, VfsIngressPressure, VfsIngressStats,
    VfsLatencyHistogramSnapshot, VfsLinuxFuseOpenCacheStats, VfsMacFuseOpenCacheDecision,
    VfsMacFuseOpenCacheStats, VfsNamespaceCacheSnapshot, VfsPortableAllocatorStats, VfsResult,
    VfsRuntimeEvent, VfsSynchronizationStateSnapshot, VfsTransitMemorySnapshot, VfsUploadStats,
    VfsWritePathBackpressureStats, VfsWritePathBurstStats, VfsWritePathDirtyPressureStats,
    VfsWritePathDrainLagStats, VfsWritePathEagerFlushStats, VfsWritePathLandingZoneStats,
    VfsWritePathLatencyQuantilesMs, VfsWritePathLatencyQuantilesSaturated,
    VfsWritePathLocalCacheIoStats, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats, VfsWritePathTuningStats,
    VfsWritePathWinfspCallbackStats, VfsWritePathWinfspForegroundFlushStats,
    VfsZeroIoCacheBytesSnapshot, VfsZeroIoDrilldownEntry, VfsZeroIoHealthSignal,
    VfsZeroIoRuntimeSnapshot, VfsZeroIoShutdownSnapshot, VfsZeroIoStartupReplaySnapshot,
    is_hidden_platform_root_metadata_name, read_runtime_stats_buffer,
};
