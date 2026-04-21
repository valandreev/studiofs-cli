#![forbid(unsafe_code)]
//! StudioFS VFS Layer
//!
//! Platform-agnostic virtual filesystem operations. Bridges metadata, data, and cache layers.

// #region debug log
pub fn emit_debug_agent_log(
    location: &'static str,
    message: &'static str,
    hypothesis_id: &'static str,
    data: serde_json::Value,
) {
    use std::io::Write as _;

    let Ok(log_path) = std::env::var("STUDIOFS_DEBUG_AGENT_LOG_PATH") else {
        return;
    };
    let session_id = std::env::var("STUDIOFS_DEBUG_AGENT_SESSION_ID").unwrap_or_default();
    let run_id =
        std::env::var("STUDIOFS_DEBUG_AGENT_RUN_ID").unwrap_or_else(|_| "run1".to_string());
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0);
    let payload = serde_json::json!({
        "sessionId": session_id,
        "runId": run_id,
        "hypothesisId": hypothesis_id,
        "location": location,
        "message": message,
        "data": data,
        "timestamp": timestamp,
    });
    let Ok(line) = serde_json::to_string(&payload) else {
        return;
    };
    let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
    else {
        return;
    };
    let _ = writeln!(file, "{line}");
}
// #endregion

pub mod access;
pub mod aimd;
pub mod bridge;
pub mod fetch;
pub mod incarnation;
pub mod lock_order;
pub mod qos;
pub mod readahead;
mod runtime;
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
    VfsConfigRaw, VfsConfigValidated, VfsError, VfsForegroundReadProtectionStats, VfsIngressLane,
    VfsIngressPressure, VfsIngressStats, VfsLatencyHistogramSnapshot, VfsNamespaceCacheSnapshot,
    VfsResult, VfsRuntimeEvent, VfsSynchronizationStateSnapshot, VfsTransitMemorySnapshot,
    VfsUploadStats, VfsWritePathBackpressureStats, VfsWritePathBurstStats,
    VfsWritePathDirtyPressureStats, VfsWritePathEagerFlushStats, VfsWritePathLatencyQuantilesMs,
    VfsWritePathLatencyQuantilesSaturated, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats, VfsWritePathTuningStats,
    VfsWritePathWinfspCallbackStats, VfsWritePathWinfspForegroundFlushStats,
    VfsZeroIoCacheBytesSnapshot, VfsZeroIoDrilldownEntry, VfsZeroIoHealthSignal,
    VfsZeroIoRuntimeSnapshot, VfsZeroIoShutdownSnapshot, VfsZeroIoStartupReplaySnapshot,
    is_hidden_non_macos_root_metadata_name,
};
