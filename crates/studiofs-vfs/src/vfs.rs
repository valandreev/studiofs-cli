//! Core VFS implementation for the current Local-First data plane.
//!
//! Key design:
//! - Files are represented as chunk/slice object graphs
//! - Payload objects are immutable and addressed by sequential IDs
//! - No deduplication = no reference counting overhead
//! - All metadata lives in redb, S3 stores only raw payload objects
//!
//! Write Path:
//! 1. Split data into logical slices
//! 2. Merge accepted bytes into local dirty state
//! 3. Persist dirty payload objects locally via the current local cache backend `put(...)`
//! 4. Apply local `sync_data()` barriers (strict for fsync / durable mode,
//!    grouped in fast mode) and persist extent metadata / attrs
//! 5. Queue committed extents for async upload + remote publish
//!
//! Read Path:
//! 1. Check local writer state for newer local bytes
//! 2. Resolve local metadata refs to committed local extents in the local backend
//!    (inline on the active runtime worker, no per-op blocking-pool hop)
//! 3. Fall back to remote fetch
//! 4. Validate object payload + checksum and assemble response

mod acl;
mod bootstrap;
mod buffer_pool;
#[path = "commit.rs"]
mod commit;
mod config;
mod deletion_cleanup;
mod file_session;
mod handle_drain_barrier;
mod handle_table;
mod ignored_entries;
mod io_ops;
#[path = "local_dirty.rs"]
mod local_dirty;
mod locking;
mod metrics;
mod misc_ops;
mod namespace;
mod namespace_ops;
mod overlay;
mod prefetch;
mod read_context;
mod read_path;
mod runtime_stats_surface;
mod startup;
mod startup_replay;
mod statfs;
mod sync_ops;
mod upload_lifecycle;
mod workers;
mod writer_stack;

/// Public VFS configuration types used by clients and tests.
pub use config::{
    ExplicitFlushOutcome, LocalSyncPolicy, ReleaseFhOutcome, VfsConfig, VfsConfigRaw,
    VfsConfigValidated,
};
/// File session primitives returned by open operations and reported by diagnostics.
pub use file_session::{OpenFile, OpenFlags, VfsFileSessionStats};
/// Public handle-drain barrier types and ports.
pub use handle_drain_barrier::{
    BarrierObservabilityPort, DeferredCleanupHintPort, DeleteCleanupPort, FlushGatePort,
    HandleDrainBarrier, HandleDrainDeleteRequest, HandleDrainRequest, HandleReleasePort,
    LocalSyncBarrierPort,
};
/// Re-exported lock kind used by the public locking API.
pub use locking::LockKind;
/// Re-exported VFS observability snapshot and metrics types.
pub use metrics::{
    RUNTIME_STATS_SCHEMA_VERSION, RUNTIME_STATS_SECTIONS, RuntimeLatencyBucket,
    RuntimeLatencyHistogram, RuntimeScalarMetric, RuntimeStatsParseError, RuntimeStatsSection,
    RuntimeStatsSnapshot, StatFs, VfsAttrReadCacheSnapshot, VfsCachePipelineMaterializeStats,
    VfsCachePipelineStats, VfsCacheStateSnapshot, VfsFetchStats, VfsForegroundReadProtectionStats,
    VfsFuseAsyncReadSnapshot, VfsFuseAsyncWriteSnapshot, VfsFuseCallbackStats, VfsFuseIoStats,
    VfsIngressLane, VfsIngressPressure, VfsIngressStats, VfsLatencyHistogramSnapshot,
    VfsMacFuseOpenCacheDecision, VfsMacFuseOpenCacheStats, VfsNamespaceCacheSnapshot,
    VfsPortableAllocatorStats, VfsReadPathStats, VfsRuntimeEvent, VfsSynchronizationStateSnapshot,
    VfsTransitMemorySnapshot, VfsUploadStats, VfsWritePathBackpressureStats,
    VfsWritePathBurstStats, VfsWritePathCadenceStats, VfsWritePathDirtyPressureStats,
    VfsWritePathDrainLagStats, VfsWritePathEagerFlushStats, VfsWritePathLandingZoneStats,
    VfsWritePathLatencyQuantilesMs, VfsWritePathLatencyQuantilesSaturated,
    VfsWritePathLocalCacheIoStats, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats, VfsWritePathTuningStats,
    VfsWritePathVfsWriteStats, VfsWritePathWinfspCallbackStats,
    VfsWritePathWinfspForegroundFlushStats, VfsWritePathWinfspWritePathStats,
    VfsZeroIoCacheBytesSnapshot, VfsZeroIoDrilldownEntry, VfsZeroIoHealthSignal,
    VfsZeroIoRuntimeSnapshot, VfsZeroIoShutdownSnapshot, VfsZeroIoStartupReplaySnapshot,
};
/// Public namespace helpers used by bridge and diagnostics integrations.
pub use namespace::{KernelInvalidator, NamespaceParent, NamespacePathStyle, NamespaceResolver};
pub use runtime_stats_surface::{
    RUNTIME_STATS_FILE_INO, RUNTIME_STATS_FILE_NAME, RUNTIME_STATS_FILE_NAME_STR,
    RUNTIME_STATS_FILE_REPORTED_SIZE, read_runtime_stats_buffer,
};

use acl::AclState;
use buffer_pool::BufferPool;
use config::{
    CACHE_RUNTIME_HUGE_BUDGET_BYTES, CACHE_RUNTIME_LARGE_BUDGET_BYTES,
    CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE, CACHE_RUNTIME_MAX_DIRTY_BYTES_LARGE,
    CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES, CACHE_RUNTIME_MAX_WRITE_EAGER_BATCH_BYTES,
    CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES, CACHE_RUNTIME_MIN_WRITE_EAGER_BATCH_BYTES,
    CACHE_RUNTIME_SMALL_BUDGET_BYTES, CacheRuntimeTuningOverrides, DeletedInodeCleanup,
    DirtyAttrPersistPolicy, FAST_MODE_SYNC_GROUP_MIN_BYTES_CEIL,
    FAST_MODE_SYNC_GROUP_MIN_BYTES_FLOOR, FsyncOutcome, HandleReleaseContext,
    LastHandleReleaseAction, LocalSyncBarrierOutcome, PAYLOAD_PUT_CONCURRENCY_MAX,
    PAYLOAD_PUT_CONCURRENCY_TARGET_BYTES, StickyErrorPolicy, UnlinkTarget,
    WRITE_EAGER_DRAIN_CHUNK_MAX_BYTES, write_path_sync_trigger,
};
use file_session::{FileSessionOperation, FileSessionState};
use handle_table::HandleTable;
use ignored_entries::IgnoredEntryRegistry;
use locking::{DistributedFlock, FileLockState};
use namespace::{DentryCacheValue, NamespaceState};
use overlay::ExtentRecordId;
use prefetch::{
    PrefetchBatch, PrefetchHintRange, PrefetchHintUpdate, PrefetchReadContext, PrefetchWorkItem,
    PrefetchWorker,
};
use read_context::{ForegroundReadScope, ForegroundReadTelemetry, ReadRequestContext};
use startup::StartupChannels;
use startup::StartupState;
use startup_replay::StartupReplayCoordinator;
use writer_stack::{MountedWriterStack, WriterStackConfig};

use crate::access::{
    AccessClass, AccessDecision, AccessPolicy, AccessTrackerConfig, HandleAccessState,
};
use crate::incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
use crate::landing_zone::LandingZone;
#[cfg(test)]
use crate::landing_zone::RecoveredLandingSegment;
use crate::qos::{QosController, TransitMemoryBudget};
use crate::readahead::{DirAccessEntry, DirAccessTracker, ReadAheadTracker, ReadStreamKey};
use crate::{LockTier, OrderedMutex};

use crate::runtime::{
    LocalCacheIoTiming, MetaExecutor, VfsMeta, cache_io_execution_model, into_vfs_meta,
    run_local_cache_io, run_local_cache_io_blocking, run_meta,
};
use crate::utils::{now_unix_millis, now_unix_secs};
use crate::vfs::local_dirty::LocalDirtyState;
use bytes::{Buf, Bytes, BytesMut};
use futures::FutureExt;
use lru::LruCache;
use opendal::options::WriteOptions;
use opendal::{EntryMode, ErrorKind, Operator};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::OpenLocalPayloadBackendError;
use studiofs_cache::PreparedLocalPayloadRuntime;
use studiofs_cache::local::{LocalPayloadBackend, LocalPayloadLifecycleState, LocalPayloadPut};
use studiofs_cache::metrics::{LatencyHistogram, LatencyHistogramSnapshot, MaterializeBatchStats};
use studiofs_cache::open_local_payload_backend_with_budget_sync_and_write_mode;
use studiofs_cache::open_prepared_local_payload_backend_with_budget_sync_and_write_mode;
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_cache::{CachedStore, CachedStoreConfig};
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, format_object_fragment_keys,
    parse_object_id_from_key,
};
use studiofs_data::store::{DataError, DataStore, DataStoreConfig, IoProfile};

use studiofs_meta::engine::MetadataAuthorityStatus;
use studiofs_meta::schema::{
    ChunkId, DEFAULT_CHUNK_SIZE, DirEntry, FormatInfo, InodeLifecycleState, PayloadRetentionClass,
    ROOT_INO, ResolvedSliceSource, SetattrArgs, SliceId, SliceOrder, SliceRange, SliceRecord,
    UploadState as MetaUploadState, chunk_id_from_offset, chunk_start_offset,
};
use studiofs_meta::types::{
    AccessLevel, FileType, Ino, InodeAttr, PayloadRef, block_index_from_offset, offset_within_block,
};
use studiofs_meta::{FileLockLease, HardLockKind, MetaError, MetaResult};
use tempfile::NamedTempFile;
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Maximum number of runtime events retained for VFS diagnostics.
pub const VFS_RUNTIME_EVENT_CAPACITY: usize = 128;
const ZEROIO_DIAGNOSTIC_DRILLDOWN_LIMIT: usize = 32;
const ZEROIO_HEALTH_SIGNAL_LIMIT: usize = 16;
const ZEROIO_REPLAY_EVENT_SAMPLE_LIMIT: usize = 16;

/// Relative directory where zero-io startup/runtime snapshots are persisted.
pub(crate) const ZEROIO_RUNTIME_STATE_DIR: &str = "runtime";
const ZEROIO_LAST_SHUTDOWN_SNAPSHOT_FILE: &str = "zeroio-last-shutdown.json";
const HARD_BACKPRESSURE_DIRTY_MULTIPLIER: usize = 2;
const AGGRESSIVE_MATERIALIZATION_DIRTY_DIVISOR: usize = 2;
const AGGRESSIVE_MATERIALIZATION_INTERVAL: Duration = Duration::from_millis(25);
const LOCAL_DIRTY_FLUSH_INTERVAL: Duration = Duration::from_millis(25);
const DEFERRED_MATERIALIZATION_LINGER_WAIT_MS: u64 = 200;
const WRITER_STACK_PREFETCHED_OBJECT_ID_RANGE_LIMIT: usize = 32;
const DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES: usize = 6;
const WRITE_ADMISSION_SAMPLE_MIN_INTERVAL_MS: u64 = 100;
const WRITE_ADMISSION_RUNWAY_TARGET_MS: u64 = 2_500;
const WRITE_ADMISSION_MAX_DELAY_MS: u64 = 10;
const WRITE_ADMISSION_EWMA_OLD_WEIGHT: u64 = 3;
const WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS: u64 = 20;
const SUSTAINED_WRITE_IDLE_RESET_MS: u64 = 2_000;
const SUSTAINED_WRITE_DELAY_START_MS: u64 = 1_500;
const SUSTAINED_WRITE_DELAY_STEP_MS: u64 = 1_500;
const SUSTAINED_WRITE_MAX_DELAY_MS: u64 = 8;
const WRITE_BUFFER_POOL_MIN_BUFFERS: usize = 4;
const WRITE_BUFFER_POOL_BUFFER_MULTIPLIER: usize = 2;

#[derive(Debug, Clone, Copy, Default)]
struct WriteAdmissionPressure {
    writer_stack_dirty_bytes: usize,
    pending_local_dirty_bytes: usize,
    pending_persisted_slice_bytes: usize,
    pending_upload_bytes: usize,
}

impl WriteAdmissionPressure {
    fn total_bytes(self) -> usize {
        self.writer_stack_dirty_bytes
            .saturating_add(self.pending_local_dirty_bytes)
            .saturating_add(self.pending_persisted_slice_bytes)
    }
}

#[derive(Debug, Clone, Copy)]
struct WriteAdmissionSample {
    now_ms: u64,
    pressure_bytes: usize,
    budget_bytes: usize,
    next_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct WriteAdmissionDecision {
    delay_ms: u64,
    ingress_bytes_per_sec: u64,
    drain_bytes_per_sec: u64,
    runway_ms: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct WriteAdmissionGovernorStats {
    ingress_bytes_per_sec: u64,
    drain_bytes_per_sec: u64,
    runway_ms: u64,
    last_delay_ms: u64,
    pressure_bytes: usize,
    budget_bytes: usize,
}

#[derive(Debug, Default)]
struct WriteAdmissionGovernor {
    last_sample_ms: u64,
    last_pressure_bytes: usize,
    accepted_since_sample: u64,
    ingress_bytes_per_sec: u64,
    drain_bytes_per_sec: u64,
    last_runway_ms: u64,
    last_delay_ms: u64,
    last_observed_pressure_bytes: usize,
    last_budget_bytes: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct SustainedWriteState {
    streak_start_ms: u64,
    last_write_ms: u64,
    last_end_offset: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct SustainedWriteAdmission {
    duration_ms: u64,
    effective_buffer_bytes: usize,
    delay_ms: u64,
    pre_drain: bool,
}

impl SustainedWriteState {
    fn observe(
        &mut self,
        now_ms: u64,
        offset: u64,
        len: usize,
        base_buffer_bytes: usize,
        drain_chunk_bytes: usize,
        dirty_bytes_after_write: usize,
    ) -> SustainedWriteAdmission {
        let end_offset = offset.saturating_add(len as u64);
        let idle_gap_ms = now_ms.saturating_sub(self.last_write_ms);
        let sequential = self.last_write_ms == 0 || offset == self.last_end_offset;
        if self.last_write_ms == 0 || !sequential || idle_gap_ms > SUSTAINED_WRITE_IDLE_RESET_MS {
            self.streak_start_ms = now_ms;
        }
        self.last_write_ms = now_ms;
        self.last_end_offset = end_offset;

        let duration_ms = now_ms.saturating_sub(self.streak_start_ms);
        let effective_buffer_bytes = sustained_write_effective_buffer_bytes(
            base_buffer_bytes,
            drain_chunk_bytes,
            duration_ms,
        );
        SustainedWriteAdmission {
            duration_ms,
            effective_buffer_bytes,
            delay_ms: sustained_write_delay_ms(duration_ms),
            pre_drain: duration_ms >= SUSTAINED_WRITE_DELAY_START_MS
                && dirty_bytes_after_write > drain_chunk_bytes,
        }
    }
}

impl WriteAdmissionGovernor {
    fn record_accepted_bytes(&mut self, bytes: usize) {
        self.accepted_since_sample = self.accepted_since_sample.saturating_add(bytes as u64);
    }

    fn plan(&mut self, sample: WriteAdmissionSample) -> WriteAdmissionDecision {
        let budget_bytes = sample.budget_bytes.max(1);
        let pressure_after_bytes = sample.pressure_bytes.saturating_add(sample.next_bytes);
        self.last_observed_pressure_bytes = sample.pressure_bytes;
        self.last_budget_bytes = budget_bytes;

        if self.last_sample_ms == 0 {
            self.last_sample_ms = sample.now_ms;
            self.last_pressure_bytes = sample.pressure_bytes;
            self.last_runway_ms = Self::runway_ms(
                pressure_after_bytes,
                budget_bytes,
                self.ingress_bytes_per_sec,
                self.drain_bytes_per_sec,
            );
            self.last_delay_ms = 0;
            return self.decision(0);
        }

        let elapsed_ms = sample.now_ms.saturating_sub(self.last_sample_ms);
        if elapsed_ms >= WRITE_ADMISSION_SAMPLE_MIN_INTERVAL_MS {
            let ingress_sample = Self::bytes_per_sec(self.accepted_since_sample, elapsed_ms);
            let drain_bytes = (self.last_pressure_bytes as u64)
                .saturating_add(self.accepted_since_sample)
                .saturating_sub(sample.pressure_bytes as u64);
            let drain_sample = Self::bytes_per_sec(drain_bytes, elapsed_ms);

            self.ingress_bytes_per_sec =
                Self::smooth_rate(self.ingress_bytes_per_sec, ingress_sample);
            self.drain_bytes_per_sec = Self::smooth_rate(self.drain_bytes_per_sec, drain_sample);
            self.accepted_since_sample = 0;
            self.last_sample_ms = sample.now_ms;
            self.last_pressure_bytes = sample.pressure_bytes;
        }

        let runway_ms = Self::runway_ms(
            pressure_after_bytes,
            budget_bytes,
            self.ingress_bytes_per_sec,
            self.drain_bytes_per_sec,
        );
        let delay_ms = self.delay_ms(sample.next_bytes, budget_bytes, runway_ms);
        self.last_runway_ms = runway_ms;
        self.last_delay_ms = delay_ms;
        self.decision(delay_ms)
    }

    fn stats(&self) -> WriteAdmissionGovernorStats {
        WriteAdmissionGovernorStats {
            ingress_bytes_per_sec: self.ingress_bytes_per_sec,
            drain_bytes_per_sec: self.drain_bytes_per_sec,
            runway_ms: if self.last_runway_ms == u64::MAX {
                0
            } else {
                self.last_runway_ms
            },
            last_delay_ms: self.last_delay_ms,
            pressure_bytes: self.last_observed_pressure_bytes,
            budget_bytes: self.last_budget_bytes,
        }
    }

    fn reset_stats(&mut self) {
        self.last_sample_ms = 0;
        self.last_pressure_bytes = 0;
        self.accepted_since_sample = 0;
        self.ingress_bytes_per_sec = 0;
        self.drain_bytes_per_sec = 0;
        self.last_runway_ms = 0;
        self.last_delay_ms = 0;
        self.last_observed_pressure_bytes = 0;
        self.last_budget_bytes = 0;
    }

    fn decision(&self, delay_ms: u64) -> WriteAdmissionDecision {
        WriteAdmissionDecision {
            delay_ms,
            ingress_bytes_per_sec: self.ingress_bytes_per_sec,
            drain_bytes_per_sec: self.drain_bytes_per_sec,
            runway_ms: self.last_runway_ms,
        }
    }

    fn delay_ms(&self, next_bytes: usize, budget_bytes: usize, runway_ms: u64) -> u64 {
        if next_bytes == 0
            || runway_ms >= WRITE_ADMISSION_RUNWAY_TARGET_MS
            || self.ingress_bytes_per_sec <= self.drain_bytes_per_sec
        {
            return 0;
        }

        let runway_allowance_bps =
            Self::bytes_per_sec(budget_bytes as u64, WRITE_ADMISSION_RUNWAY_TARGET_MS).max(1);
        let allowed_bytes_per_sec = self
            .drain_bytes_per_sec
            .saturating_add(runway_allowance_bps)
            .max(1);
        if allowed_bytes_per_sec >= self.ingress_bytes_per_sec {
            return 0;
        }

        let current_write_ms = Self::transfer_ms(next_bytes as u64, self.ingress_bytes_per_sec);
        let target_write_ms = Self::transfer_ms(next_bytes as u64, allowed_bytes_per_sec);
        let delay_ms = target_write_ms.saturating_sub(current_write_ms);
        delay_ms.clamp(1, WRITE_ADMISSION_MAX_DELAY_MS)
    }

    fn runway_ms(
        pressure_bytes: usize,
        budget_bytes: usize,
        ingress_bytes_per_sec: u64,
        drain_bytes_per_sec: u64,
    ) -> u64 {
        if ingress_bytes_per_sec <= drain_bytes_per_sec {
            return u64::MAX;
        }
        let net_bytes_per_sec = ingress_bytes_per_sec - drain_bytes_per_sec;
        let remaining_bytes = budget_bytes.saturating_sub(pressure_bytes) as u64;
        Self::transfer_ms(remaining_bytes, net_bytes_per_sec)
    }

    fn transfer_ms(bytes: u64, bytes_per_sec: u64) -> u64 {
        if bytes == 0 {
            return 0;
        }
        if bytes_per_sec == 0 {
            return u64::MAX;
        }
        let millis = (u128::from(bytes) * 1_000).div_ceil(u128::from(bytes_per_sec));
        millis.min(u128::from(u64::MAX)) as u64
    }

    fn bytes_per_sec(bytes: u64, elapsed_ms: u64) -> u64 {
        if bytes == 0 || elapsed_ms == 0 {
            return 0;
        }
        let rate = (u128::from(bytes) * 1_000).div_ceil(u128::from(elapsed_ms));
        rate.min(u128::from(u64::MAX)) as u64
    }

    fn smooth_rate(previous: u64, sample: u64) -> u64 {
        if previous == 0 {
            return sample;
        }
        let weighted = u128::from(previous)
            .saturating_mul(u128::from(WRITE_ADMISSION_EWMA_OLD_WEIGHT))
            .saturating_add(u128::from(sample));
        let divisor = u128::from(WRITE_ADMISSION_EWMA_OLD_WEIGHT.saturating_add(1));
        (weighted / divisor).min(u128::from(u64::MAX)) as u64
    }
}

fn sustained_write_effective_buffer_bytes(
    base_buffer_bytes: usize,
    drain_chunk_bytes: usize,
    _duration_ms: u64,
) -> usize {
    base_buffer_bytes.max(drain_chunk_bytes).max(1)
}

fn sustained_write_delay_ms(duration_ms: u64) -> u64 {
    if duration_ms < SUSTAINED_WRITE_DELAY_START_MS {
        return 0;
    }
    duration_ms
        .saturating_sub(SUSTAINED_WRITE_DELAY_START_MS)
        .saturating_div(SUSTAINED_WRITE_DELAY_STEP_MS)
        .saturating_add(1)
        .min(SUSTAINED_WRITE_MAX_DELAY_MS)
}

// =============================================================================
// Error Types
// =============================================================================

/// Errors returned by VFS public APIs.
#[derive(Debug, Error)]
pub enum VfsError {
    /// Metadata-layer failure.
    #[error("metadata error: {0}")]
    Meta(#[from] studiofs_meta::MetaError),

    /// Data-layer failure.
    #[error("data error: {0}")]
    Data(#[from] studiofs_data::store::DataError),

    /// I/O failure from a blocking or async operation.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Task join failure while waiting for asynchronous work.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Object-layer failure while decoding or writing payload objects.
    #[error("object error: {0}")]
    Object(#[from] studiofs_data::object::ObjectError),

    /// Requested metadata block was not found.
    #[error("block not found: inode {0}, block {1}")]
    BlockNotFound(
        #[doc = "Inode containing the missing block."] Ino,
        #[doc = "Block number that is missing."] u32,
    ),

    /// Operation would block due to conflicting lock state.
    #[error("lock conflict (EAGAIN)")]
    WouldBlock,

    /// Open/read is blocked while remote content is still uploading.
    #[error("remote content is still uploading (EAGAIN)")]
    RemoteContentPending,

    /// Rejecting an invalid filename or malformed name.
    #[error("invalid filename: {0}")]
    InvalidName(String),

    /// QoS-related rejection while reserving capacity for object work.
    #[error("qos permit failure at {stage} for object {object_id} (0x{object_id:016x}): {message}")]
    QosFailure {
        /// Stage during which QoS denied the request.
        stage: Cow<'static, str>,
        /// Numeric identifier of the object that failed QoS checks.
        object_id: u64,
        /// Human-readable QoS error message.
        message: String,
    },

    /// Upstream fetch pipeline failed.
    #[error("fetch error: {0}")]
    Fetch(#[from] crate::fetch::FetchError),

    /// Failed to detect the configured local payload backend.
    #[error("cache backend detection failed for root '{cache_root}': {source}")]
    LocalPayloadRuntimeDetection {
        /// Local cache root path to inspect.
        cache_root: String,
        /// Low-level detection error.
        #[source]
        source: std::io::Error,
    },

    /// Generic fallback VFS error.
    #[error("{0}")]
    Other(String),
}

/// Result type alias used by the VFS public API.
pub type VfsResult<T> = Result<T, VfsError>;

/// Directory entry plus the child attributes loaded with the same metadata batch.
#[derive(Debug, Clone)]
pub struct DirEntryWithAttr {
    /// Directory entry returned by the namespace index.
    pub entry: DirEntry,
    /// Child attributes when the inode still exists; `None` keeps dangling dentries visible.
    pub attr: Option<InodeAttr>,
}

#[derive(Debug, Clone, Copy)]
enum WinfspBarrierType {
    /// Barrier for WinFSP cleanup callbacks.
    Cleanup,
    /// Barrier for WinFSP flush callbacks.
    Flush,
    /// Barrier for WinFSP close callbacks.
    Close,
}

// =============================================================================
// Filename Validation
// =============================================================================

/// Validate a filename (directory entry name).
///
/// Rejects:
/// - Empty names
/// - Names longer than 255 bytes
/// - Names containing '/' (path separator)
/// - Dots-only names (`.`, `..`)
/// - Names containing Windows-invalid characters: < > : " \ | ? *
fn validate_name(name: &[u8]) -> VfsResult<()> {
    if name.is_empty() {
        return Err(VfsError::InvalidName("name is empty".into()));
    }
    if name.len() > 255 {
        return Err(VfsError::InvalidName(format!(
            "name too long ({} bytes, max 255)",
            name.len()
        )));
    }

    // Check for Unix path separator and Windows-invalid characters
    let invalid_chars = b"/<>:\"|\\?*";
    if let Some(pos) = name.iter().position(|&b| invalid_chars.contains(&b)) {
        let char_display = name[pos] as char;
        return Err(VfsError::InvalidName(format!(
            "name contains invalid character '{}'",
            char_display
        )));
    }

    if name == b"." || name == b".." {
        return Err(VfsError::InvalidName("name is '.' or '..'".into()));
    }
    Ok(())
}

fn is_unix_trash_component(name: &[u8]) -> bool {
    matches!(name, b".Trash" | b".Trashes") || name.starts_with(b".Trash-")
}

fn is_windows_recycle_bin_component(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(b"$RECYCLE.BIN")
}

fn is_probable_trash_component(name: &[u8]) -> bool {
    is_unix_trash_component(name) || is_windows_recycle_bin_component(name)
}

/// Returns true when a root metadata entry should be hidden on platforms where it is not native.
pub fn is_hidden_platform_root_metadata_name(name: &[u8]) -> bool {
    name == b".Trashes"
}

fn is_ignored_macos_metadata(name: &[u8]) -> bool {
    name.len() > 2 && name.starts_with(b"._")
}

fn is_blocked_root_macos_metadata(name: &[u8]) -> bool {
    matches!(name, b".Spotlight-V100" | b".fseventsd")
}

// =============================================================================
// Constants & Configuration
// =============================================================================

/// Attribute cache TTL — read-through attributes are valid for 1 second.
const ATTR_CACHE_TTL: Duration = Duration::from_secs(1);
const DEFAULT_JANITOR_INTERVAL: Duration = Duration::from_secs(30 * 60);
const DEFAULT_JANITOR_SKIP_NEWER_THAN: Duration = Duration::from_secs(15 * 60);
const DEFAULT_JANITOR_MAX_DELETES_PER_RUN: u64 = 500;
const DEFAULT_JANITOR_LEASE_TTL: Duration = Duration::from_secs(45 * 60);
// JuiceFS-like high-level read/write buffering defaults:
// - buffer-size: 300M
const DEFAULT_READ_BUFFER_MB: u64 = 300;
// Block-level derived defaults (used as fallback if conversion is unavailable).
const DEFAULT_PREFETCH_CONCURRENCY: usize = 16;
const DEFAULT_READAHEAD_WIDTH: u32 = 64;
const DEFAULT_READAHEAD_MIN_PREFETCH: u32 = 16;
const DEFAULT_READAHEAD_SEQ_STEP: u8 = 4;
const DEFAULT_READAHEAD_RANDOM_PENALTY: u8 = 2;
const DEFAULT_PREFETCH_FOREGROUND_RESERVE: usize = 16;
const DEFAULT_PREFETCH_MAX_PER_CYCLE: usize = 256;
const DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS: u64 = 250;
const DEFAULT_PREFETCH_CONCURRENCY_MIN: usize = 1;
const DEFAULT_DIR_PREFETCH_MAX_BATCH: usize = 0;
const DEFAULT_READDIR_ATTR_BATCH_CHUNK: usize = 256;
const DEFAULT_READDIR_ATTR_BATCH_MAX_INODES: usize = 4096;
const DEFAULT_UPLOAD_DELAY_MS: u64 = 0;
// Keep failed uploads circulating quickly so a single transient error does not
// strand the whole inode in pending-overlay mode until remount.
const DEFAULT_UPLOAD_REQUEUE_BASE_MS: u64 = 500;
const DEFAULT_UPLOAD_REQUEUE_MAX_MS: u64 = 5_000;
const DEFAULT_UPLOAD_STATS_INTERVAL_SECS: u64 = 1;
const DEFAULT_UPLOAD_CACHE_IO_SLOTS: usize = 32;
const DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES: usize = 0;
const DEFAULT_TRANSIT_MEMORY_BUDGET_RATIO_PERCENT: u64 = 80;
const DEFAULT_FETCH_PREFETCH_ACQUIRE_WAIT_MS: u64 = 75;
const DEFAULT_UPLOAD_USER_IDLE_GRACE_MS: u64 = 500;
const DEFAULT_UPLOAD_FG_READ_LATENCY_TARGET_MS: u64 = 35;
const DEFAULT_UPLOAD_FG_READ_LATENCY_WINDOW_MS: u64 = 1_500;
const DEFAULT_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS: u64 = 2;
const DEFAULT_MAX_DIRTY_BYTES: usize = 800 * 1024 * 1024;
const DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_FETCH_INITIAL_WINDOW: f64 = 0.0;
const DEFAULT_FETCH_MIN_WINDOW: f64 = 1.0;
const DEFAULT_FETCH_MAX_WINDOW: f64 = 0.0;
const DEFAULT_FETCH_INCREASE_STEP: f64 = 0.0;
const DEFAULT_FETCH_DECREASE_FACTOR: f64 = 0.85;
const DEFAULT_FETCH_DECREASE_COOLDOWN_SECS: u64 = 3;
const DEFAULT_FETCH_IDLE_RESET_SECS: u64 = 60;
const DEFAULT_UPLOAD_INITIAL_WINDOW: f64 = 20.0;
const DEFAULT_UPLOAD_MIN_WINDOW: f64 = 1.0;
const DEFAULT_UPLOAD_MAX_WINDOW: f64 = 20.0;
const DEFAULT_UPLOAD_INCREASE_STEP: f64 = 1.0;
const DEFAULT_UPLOAD_DECREASE_FACTOR: f64 = 0.85;
const DEFAULT_UPLOAD_DECREASE_COOLDOWN_SECS: u64 = 3;
const DEFAULT_UPLOAD_IDLE_RESET_SECS: u64 = 60;
const DEFAULT_SLAB_FLUSH_INTERVAL_MS: u64 = 10;
const DEFAULT_UPLOAD_WORKER_COUNT: usize = 20;
const MIN_UPLOAD_WORKER_COUNT: usize = 1;
const DEFAULT_UPLOAD_MAX_ATTEMPTS: u32 = 4;
const DEFAULT_UPLOAD_REQUEST_TIMEOUT_SECS: u64 = 15;
const DEFAULT_UPLOAD_RETRY_BASE_MS: u64 = 200;
const DEFAULT_UPLOAD_RETRY_MAX_MS: u64 = 5_000;
const DEFAULT_FETCH_MAX_ATTEMPTS: u32 = 8;
const DEFAULT_PREFETCH_FETCH_MAX_ATTEMPTS: u32 = 4;
const DEFAULT_FETCH_REQUEST_TIMEOUT_SECS: u64 = 45;
const DEFAULT_FETCH_RETRY_BASE_MS: u64 = 50;
const DEFAULT_FETCH_RETRY_MAX_MS: u64 = 2_000;
const DEFAULT_FETCH_PREFETCH_RESERVE: usize = 0;
const DEFAULT_MAX_DRAIN_BATCH: usize = 1024;
const DEFAULT_PUBLISH_VISIBILITY_TIMEOUT_SECS: u64 = 30;
const DEFAULT_QOS_ACCEPT_SLOTS: usize = 64;
const DEFAULT_QOS_MATERIALIZE_SLOTS: usize = 2;
const DEFAULT_QOS_COMMIT_SLOTS: usize = 4;
const DEFAULT_QOS_FETCH_SLOTS: usize = 200;
const UPLOAD_CACHE_IO_FOREGROUND_BASE_DIVISOR: u32 = 4;
const UPLOAD_CACHE_IO_FOREGROUND_BASE_MAX: u32 = 8;
const UPLOAD_CACHE_IO_FOREGROUND_STEP_DIVISOR: u32 = 8;
const UPLOAD_CACHE_IO_FOREGROUND_STEP_MAX: u32 = 4;
const UPLOAD_SCHEDULER_PENALTY_STEP_MS: u64 = 16;
const UPLOAD_SCHEDULER_PENALTY_MAX_MS: u64 = 240;
const UPLOAD_SCHEDULER_OVERSHOOT_MAX: u64 = 6;
const UPLOAD_SCHEDULER_PENALTY_DECAY_STEP_MS: u64 = 40;
const FOREGROUND_READ_SPEEDOMETER_SHARDS: usize = 16;
const FOREGROUND_READ_SPEEDOMETER_MAX_SAMPLES: usize = 4_096;
const FOREGROUND_READ_THROUGHPUT_MIN_COVERAGE_MS: u64 = 100;
const UPLOAD_WORKER_YIELD_EVERY_OPS: u32 = 8;
const DEFAULT_DENTRY_CACHE_CAPACITY: usize = 65_536;
const DEFAULT_DENTRY_CACHE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_PATH_SHORTCUT_CACHE_CAPACITY: usize = 16_384;
const DEFAULT_PATH_SHORTCUT_CACHE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_ATTR_READ_CACHE_CAPACITY: usize = 65_536;
const DEFAULT_ATTR_READ_CACHE_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_EFFECTIVE_ACL_CACHE_CAPACITY: usize = 65_536;
const METADATA_CACHE_PROTECTED_PERCENT: usize = 80;
const DENTRY_CACHE_ORDER_COMPACT_MULTIPLIER: usize = 2;
const DENTRY_CACHE_ORDER_COMPACT_SLACK: usize = 64;
const DENTRY_CACHE_INDEX_NODE_OVERHEAD: usize = 32;
const DEFAULT_XATTR_MAX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS: u64 = 10;
const DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS: u64 = 5 * 60 * 1000;
const DEFAULT_LOCK_LEASE_TTL: Duration = Duration::from_secs(120);
const DEFAULT_LOCK_RETRY_BASE_MS: u64 = 25;
const DEFAULT_LOCK_RETRY_MAX_MS: u64 = 500;
const CACHE_RUNTIME_MIN_DIRTY_BYTES: usize = 800 * 1024 * 1024;
const CACHE_RUNTIME_MAX_DIRTY_BYTES_BASE: usize = 800 * 1024 * 1024;
const DEBUG_PENDING_PATH_SUFFIX_ENV: &str = "STUDIOFS_DEBUG_PENDING_PATH_SUFFIX";
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TwoTierSegment {
    /// Newly inserted cache entries eligible for promotion.
    Probationary,
    /// Protected cache entries shielded from immediate eviction.
    Protected,
}

#[derive(Debug, Clone)]
struct TwoTierEntry<V, M> {
    value: V,
    meta: M,
    segment: TwoTierSegment,
    charge: usize,
    stamp: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct TwoTierMetrics {
    hits: u64,
    protected_hits: u64,
    misses: u64,
    negative_hits: u64,
    evictions: u64,
}

trait CachePolicy<K, V> {
    type Meta;
    type InsertContext;
    type LookupContext<'a>
    where
        Self: 'a;

    fn make_meta(&mut self, key: &K, value: &V, ctx: Self::InsertContext) -> Self::Meta;
    fn is_stale(&self, key: &K, value: &V, meta: &Self::Meta, ctx: Self::LookupContext<'_>)
    -> bool;
    fn estimate_entry_charge(&self, key: &K, value: &V, meta: &Self::Meta) -> usize;
    fn is_negative(&self, _value: &V) -> bool {
        false
    }
    fn on_insert(&mut self, _key: &K, _value: &V, _meta: &Self::Meta) {}
    /// Called when an existing key is updated via `TwoTierCache::put`.
    ///
    /// Note: during this callback the entry is temporarily removed from
    /// `TwoTierCache.entries` (remove/update/reinsert flow), so policy logic
    /// must not assume the key is currently present in the main map.
    fn on_update(
        &mut self,
        _key: &K,
        _old_value: &V,
        _old_meta: &Self::Meta,
        _new_value: &V,
        _new_meta: &Self::Meta,
    ) {
    }
    fn on_remove(&mut self, _key: &K, _value: &V, _meta: &Self::Meta) {}
}

#[derive(Debug)]
struct TwoTierCache<K, V, P>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
    P: CachePolicy<K, V>,
{
    entry_budget: usize,
    byte_budget: usize,
    protected_entry_budget: usize,
    protected_byte_budget: usize,
    next_stamp: u64,
    used_bytes: usize,
    protected_entries: usize,
    protected_bytes: usize,
    metrics: TwoTierMetrics,
    entries: HashMap<K, TwoTierEntry<V, P::Meta>>,
    probationary_order: VecDeque<(K, u64)>,
    protected_order: VecDeque<(K, u64)>,
    policy: P,
}

impl<K, V, P> TwoTierCache<K, V, P>
where
    K: Clone + Eq + std::hash::Hash,
    V: Clone,
    P: CachePolicy<K, V>,
{
    fn new(entry_budget: usize, byte_budget: usize, policy: P) -> Self {
        let protected_entry_budget = if entry_budget <= 1 {
            0
        } else {
            ((entry_budget * METADATA_CACHE_PROTECTED_PERCENT) / 100).clamp(1, entry_budget - 1)
        };
        let protected_byte_budget = if byte_budget <= 1 {
            0
        } else {
            ((byte_budget * METADATA_CACHE_PROTECTED_PERCENT) / 100).clamp(1, byte_budget - 1)
        };
        Self {
            entry_budget,
            byte_budget,
            protected_entry_budget,
            protected_byte_budget,
            next_stamp: 1,
            used_bytes: 0,
            protected_entries: 0,
            protected_bytes: 0,
            metrics: TwoTierMetrics::default(),
            entries: HashMap::new(),
            probationary_order: VecDeque::new(),
            protected_order: VecDeque::new(),
            policy,
        }
    }

    fn is_enabled(&self) -> bool {
        self.entry_budget > 0 && self.byte_budget > 0
    }

    fn next_stamp(&mut self) -> u64 {
        let stamp = self.next_stamp;
        self.next_stamp = self.next_stamp.saturating_add(1);
        stamp
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn used_bytes(&self) -> usize {
        self.used_bytes
    }

    fn protected_entries(&self) -> usize {
        self.protected_entries
    }

    fn protected_bytes(&self) -> usize {
        self.protected_bytes
    }

    fn metrics(&self) -> TwoTierMetrics {
        self.metrics
    }

    fn keys(&self) -> Vec<K> {
        self.entries.keys().cloned().collect()
    }

    fn policy_mut(&mut self) -> &mut P {
        &mut self.policy
    }

    fn get(&mut self, key: &K, ctx: P::LookupContext<'_>) -> Option<V> {
        if !self.is_enabled() {
            self.metrics.misses = self.metrics.misses.saturating_add(1);
            return None;
        }
        let (value, segment, stale) = match self.entries.get(key) {
            Some(entry) => (
                entry.value.clone(),
                entry.segment,
                self.policy.is_stale(key, &entry.value, &entry.meta, ctx),
            ),
            None => {
                self.metrics.misses = self.metrics.misses.saturating_add(1);
                return None;
            }
        };
        if stale {
            // Stale invalidation is a cache-miss cleanup, not an eviction.
            self.remove(key);
            self.metrics.misses = self.metrics.misses.saturating_add(1);
            return None;
        }
        self.metrics.hits = self.metrics.hits.saturating_add(1);
        if self.policy.is_negative(&value) {
            self.metrics.negative_hits = self.metrics.negative_hits.saturating_add(1);
        }
        match segment {
            TwoTierSegment::Probationary => self.promote_to_protected(key),
            TwoTierSegment::Protected => {
                self.metrics.protected_hits = self.metrics.protected_hits.saturating_add(1);
                self.touch_protected(key);
            }
        }
        Some(value)
    }

    fn put(&mut self, key: K, value: V, ctx: P::InsertContext) {
        if !self.is_enabled() {
            return;
        }
        let meta = self.policy.make_meta(&key, &value, ctx);
        let charge = self.policy.estimate_entry_charge(&key, &value, &meta);
        if charge > self.byte_budget {
            return;
        }
        let stamp = self.next_stamp();

        if let Some(mut entry) = self.entries.remove(&key) {
            // Deliberately bypass `self.remove(...)` here: this is an in-place
            // update, not a logical deletion, so policy `on_remove` must not fire.
            if entry.segment == TwoTierSegment::Protected {
                self.protected_bytes = self.protected_bytes.saturating_sub(entry.charge);
            }
            self.used_bytes = self.used_bytes.saturating_sub(entry.charge);
            self.policy
                .on_update(&key, &entry.value, &entry.meta, &value, &meta);
            entry.value = value;
            entry.meta = meta;
            entry.charge = charge;
            entry.stamp = stamp;
            self.used_bytes = self.used_bytes.saturating_add(charge);
            if entry.segment == TwoTierSegment::Protected {
                self.protected_bytes = self.protected_bytes.saturating_add(charge);
                self.protected_order.push_back((key.clone(), stamp));
            } else {
                self.probationary_order.push_back((key.clone(), stamp));
            }
            self.entries.insert(key, entry);
            self.maybe_compact_orders();
            self.enforce_limits();
            return;
        }

        self.policy.on_insert(&key, &value, &meta);
        self.entries.insert(
            key.clone(),
            TwoTierEntry {
                value,
                meta,
                segment: TwoTierSegment::Probationary,
                charge,
                stamp,
            },
        );
        self.used_bytes = self.used_bytes.saturating_add(charge);
        self.probationary_order.push_back((key, stamp));
        self.maybe_compact_orders();
        self.enforce_limits();
    }

    fn remove(&mut self, key: &K) -> bool {
        let Some(entry) = self.entries.remove(key) else {
            return false;
        };
        self.policy.on_remove(key, &entry.value, &entry.meta);
        self.used_bytes = self.used_bytes.saturating_sub(entry.charge);
        if entry.segment == TwoTierSegment::Protected {
            self.protected_entries = self.protected_entries.saturating_sub(1);
            self.protected_bytes = self.protected_bytes.saturating_sub(entry.charge);
        }
        true
    }

    fn touch_protected(&mut self, key: &K) {
        let stamp = self.next_stamp();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.stamp = stamp;
            self.protected_order.push_back((key.clone(), stamp));
        }
        self.maybe_compact_orders();
    }

    fn promote_to_protected(&mut self, key: &K) {
        let stamp = self.next_stamp();
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.segment == TwoTierSegment::Probationary {
                entry.segment = TwoTierSegment::Protected;
                self.protected_entries = self.protected_entries.saturating_add(1);
                self.protected_bytes = self.protected_bytes.saturating_add(entry.charge);
            }
            entry.stamp = stamp;
            self.protected_order.push_back((key.clone(), stamp));
        }
        self.maybe_compact_orders();
        self.enforce_protected_budget();
    }

    fn enforce_limits(&mut self) {
        self.enforce_protected_budget();
        while self.entries.len() > self.entry_budget || self.used_bytes > self.byte_budget {
            if !self.evict_one() {
                break;
            }
        }
    }

    fn enforce_protected_budget(&mut self) {
        while self.protected_entries > self.protected_entry_budget
            || self.protected_bytes > self.protected_byte_budget
        {
            if !self.demote_one_protected() {
                break;
            }
        }
    }

    fn demote_one_protected(&mut self) -> bool {
        while let Some((key, stamp)) = self.protected_order.pop_front() {
            let Some(entry) = self.entries.get(&key) else {
                continue;
            };
            if entry.stamp != stamp || entry.segment != TwoTierSegment::Protected {
                continue;
            }
            let demote_stamp = self.next_stamp();
            if let Some(entry) = self.entries.get_mut(&key) {
                entry.segment = TwoTierSegment::Probationary;
                entry.stamp = demote_stamp;
                self.protected_entries = self.protected_entries.saturating_sub(1);
                self.protected_bytes = self.protected_bytes.saturating_sub(entry.charge);
                self.probationary_order.push_back((key, demote_stamp));
                return true;
            }
        }
        false
    }

    fn evict_one(&mut self) -> bool {
        self.evict_one_from(TwoTierSegment::Probationary)
            || self.evict_one_from(TwoTierSegment::Protected)
    }

    fn maybe_compact_orders(&mut self) {
        let probationary_live = self.entries.len().saturating_sub(self.protected_entries);
        let probationary_limit = probationary_live
            .saturating_mul(DENTRY_CACHE_ORDER_COMPACT_MULTIPLIER)
            .saturating_add(DENTRY_CACHE_ORDER_COMPACT_SLACK);
        if self.probationary_order.len() > probationary_limit {
            self.compact_order(TwoTierSegment::Probationary);
        }

        let protected_limit = self
            .protected_entries
            .saturating_mul(DENTRY_CACHE_ORDER_COMPACT_MULTIPLIER)
            .saturating_add(DENTRY_CACHE_ORDER_COMPACT_SLACK);
        if self.protected_order.len() > protected_limit {
            self.compact_order(TwoTierSegment::Protected);
        }
    }

    fn compact_order(&mut self, segment: TwoTierSegment) {
        let mut live: Vec<(u64, K)> = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                (entry.segment == segment).then_some((entry.stamp, key.clone()))
            })
            .collect();
        live.sort_unstable_by_key(|(stamp, _)| *stamp);
        let order = match segment {
            TwoTierSegment::Probationary => &mut self.probationary_order,
            TwoTierSegment::Protected => &mut self.protected_order,
        };
        order.clear();
        order.extend(live.into_iter().map(|(stamp, key)| (key, stamp)));
    }

    fn evict_one_from(&mut self, segment: TwoTierSegment) -> bool {
        loop {
            let next = match segment {
                TwoTierSegment::Probationary => self.probationary_order.pop_front(),
                TwoTierSegment::Protected => self.protected_order.pop_front(),
            };
            let Some((key, stamp)) = next else {
                break;
            };
            let Some(entry) = self.entries.get(&key) else {
                continue;
            };
            if entry.stamp != stamp || entry.segment != segment {
                continue;
            }
            if self.remove(&key) {
                self.metrics.evictions = self.metrics.evictions.saturating_add(1);
                return true;
            }
        }
        false
    }

    #[cfg(test)]
    fn contains_key(&self, key: &K) -> bool {
        self.entries.contains_key(key)
    }

    #[cfg(test)]
    fn peek(&self, key: &K) -> Option<&TwoTierEntry<V, P::Meta>> {
        self.entries.get(key)
    }

    #[cfg(test)]
    fn protected_order_len(&self) -> usize {
        self.protected_order.len()
    }
}

#[derive(Debug, Clone)]
struct AttrReadCacheMeta {
    expires_at: Instant,
}

#[derive(Debug, Default)]
struct AttrReadCachePolicy;

impl CachePolicy<Ino, InodeAttr> for AttrReadCachePolicy {
    type Meta = AttrReadCacheMeta;
    type InsertContext = Instant;
    type LookupContext<'a> = Instant;

    fn make_meta(
        &mut self,
        _key: &Ino,
        _value: &InodeAttr,
        ctx: Self::InsertContext,
    ) -> Self::Meta {
        AttrReadCacheMeta { expires_at: ctx }
    }

    fn is_stale(
        &self,
        _key: &Ino,
        _value: &InodeAttr,
        meta: &Self::Meta,
        ctx: Self::LookupContext<'_>,
    ) -> bool {
        ctx >= meta.expires_at
    }

    fn estimate_entry_charge(&self, _key: &Ino, _value: &InodeAttr, _meta: &Self::Meta) -> usize {
        std::mem::size_of::<Ino>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<InodeAttr, AttrReadCacheMeta>,
            >())
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct AttrReadCacheMetrics {
    hits: u64,
    protected_hits: u64,
    misses: u64,
    evictions: u64,
    occupancy_entries: usize,
    occupancy_bytes: usize,
    protected_occupancy_entries: usize,
    protected_occupancy_bytes: usize,
    entry_budget: usize,
    byte_budget: usize,
}

impl AttrReadCacheMetrics {
    fn hot_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.protected_hits as f64 / self.hits as f64
    }
}

#[derive(Debug)]
struct AttrReadCache {
    inner: TwoTierCache<Ino, InodeAttr, AttrReadCachePolicy>,
}

impl AttrReadCache {
    fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            inner: TwoTierCache::new(entry_budget, byte_budget, AttrReadCachePolicy),
        }
    }

    #[cfg(test)]
    fn estimate_entry_charge() -> usize {
        std::mem::size_of::<Ino>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<InodeAttr, AttrReadCacheMeta>,
            >())
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD)
    }

    fn metrics(&self) -> AttrReadCacheMetrics {
        let metrics = self.inner.metrics();
        AttrReadCacheMetrics {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            evictions: metrics.evictions,
            occupancy_entries: self.inner.entry_count(),
            occupancy_bytes: self.inner.used_bytes(),
            protected_occupancy_entries: self.inner.protected_entries(),
            protected_occupancy_bytes: self.inner.protected_bytes(),
            entry_budget: self.inner.entry_budget,
            byte_budget: self.inner.byte_budget,
        }
    }

    fn get(&mut self, ino: Ino, now: Instant) -> Option<InodeAttr> {
        self.inner.get(&ino, now)
    }

    fn put(&mut self, ino: Ino, attr: InodeAttr, ttl: Duration, now: Instant) {
        let expires_at = now.checked_add(ttl).unwrap_or(now);
        self.inner.put(ino, attr, expires_at);
    }

    fn remove(&mut self, ino: Ino) -> bool {
        self.inner.remove(&ino)
    }

    fn keys(&self) -> Vec<Ino> {
        self.inner.keys()
    }

    #[cfg(test)]
    fn contains(&self, ino: Ino) -> bool {
        self.inner.contains_key(&ino)
    }
}

fn run_sync_with_timeout<T: Send + 'static>(
    timeout: Duration,
    operation: impl FnOnce() -> VfsResult<T> + Send + 'static,
) -> VfsResult<Option<T>> {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = std::thread::spawn(move || {
        let result = operation();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(timeout) {
        Ok(result) => {
            // Result received — thread has (almost certainly) finished; join to
            // guarantee its captured `Arc`s are fully dropped before we return.
            let _ = handle.join();
            result.map(Some)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            // The operation is still running.  We must NOT abandon the thread
            // because it holds `Arc<dyn VfsMeta>` (and transitively
            // `Arc<redb::Database>`).  If the process exits before that Arc
            // drops, `Database::drop()` never runs and redb is left in an
            // unclean state.  Wait with a generous secondary cap so the
            // database is properly closed even when PG network I/O is slow.
            warn!(
                timeout_ms = timeout.as_millis() as u64,
                "run_sync_with_timeout: primary timeout reached, \
                 waiting for thread to finish so redb closes cleanly"
            );
            let _ = handle.join();
            Ok(None)
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let _ = handle.join();
            Err(VfsError::Other(
                "timed operation thread disconnected".to_string(),
            ))
        }
    }
}

// =============================================================================
// Internal State Subsystems
// =============================================================================

struct UploadState {
    deleted_inodes: OrderedMutex<std::collections::HashSet<Ino>>,
    deleted_uploads_notify: tokio::sync::Notify,
    upload_cancel_tokens: OrderedMutex<std::collections::HashMap<Ino, CancellationToken>>,
    pending_evictions: OrderedMutex<std::collections::HashSet<u64>>,
    deferred_deleted_local_payloads:
        OrderedMutex<std::collections::HashMap<Ino, std::collections::HashSet<u64>>>,
    pending_epoch_finalizations: OrderedMutex<BTreeSet<(Ino, u64)>>,
    pending_epoch_finalizations_notify: tokio::sync::Notify,
    upload_sticky_errors: OrderedMutex<std::collections::HashMap<Ino, String>>,
    startup_replay_counts: OrderedMutex<HashMap<(Ino, u64), u64>>,
    upload_requeues: AtomicU64,
    upload_canceled_deleted: AtomicU64,
    upload_canceled_stale: AtomicU64,
    upload_canceled_stale_before_put: AtomicU64,
    upload_canceled_stale_after_put: AtomicU64,
    upload_canceled_stale_after_put_bytes: AtomicU64,
    upload_local_payload_reads: AtomicU64,
    upload_local_payload_read_bytes: AtomicU64,
    upload_hot_payload_reads: AtomicU64,
    upload_hot_payload_read_bytes: AtomicU64,
    upload_staged_payload_reads: AtomicU64,
    upload_staged_payload_read_bytes: AtomicU64,
    upload_fallback_deferrals: AtomicU64,
    upload_fallback_rereads: AtomicU64,
    upload_fallback_reread_bytes: AtomicU64,
    upload_fallback_reread_cache_misses: AtomicU64,
    upload_fallback_wait_ns: AtomicU64,
    upload_fallback_wait_max_ns: AtomicU64,
    upload_fallback_read_ns: AtomicU64,
    upload_fallback_read_max_ns: AtomicU64,
    /// Dynamic slot cost adjusted from foreground-read pressure on the Local-First path.
    upload_cache_io_foreground_cost: AtomicU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheRuntimeTuning {
    max_dirty_bytes: usize,
    write_eager_flush_batch_bytes: usize,
    upload_cache_io_slots: usize,
    transit_memory_budget_bytes: usize,
}

fn derive_cache_runtime_tuning(
    cache_size: u64,
    block_size: u32,
    memory_cache_budget_bytes: u64,
) -> CacheRuntimeTuning {
    let cache_size = cache_size.max(1);
    let block_size = u64::from(block_size.max(1));

    let clamp_usize = |value: u64, min: usize, max: usize| -> usize {
        value.clamp(min as u64, max as u64).min(usize::MAX as u64) as usize
    };
    // Keep foreground write buffering bounded by cache budget while capping
    // absolute memory so the same defaults remain safe on USB-attached NVMe.
    let max_dirty_cap = if cache_size >= CACHE_RUNTIME_HUGE_BUDGET_BYTES {
        CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE
    } else if cache_size >= CACHE_RUNTIME_LARGE_BUDGET_BYTES {
        CACHE_RUNTIME_MAX_DIRTY_BYTES_LARGE
    } else {
        CACHE_RUNTIME_MAX_DIRTY_BYTES_BASE
    };
    let max_dirty_bytes = clamp_usize(
        cache_size / 128,
        CACHE_RUNTIME_MIN_DIRTY_BYTES,
        max_dirty_cap,
    );

    // Batch local persistence more aggressively as cache budgets grow, but
    // never below block size and never beyond the bounded upper limit.
    let write_eager_flush_batch_bytes = clamp_usize(
        (cache_size / 2048).max(block_size),
        CACHE_RUNTIME_MIN_WRITE_EAGER_BATCH_BYTES.max(block_size as usize),
        CACHE_RUNTIME_MAX_WRITE_EAGER_BATCH_BYTES,
    );

    let upload_cache_io_slots = if cache_size <= CACHE_RUNTIME_SMALL_BUDGET_BYTES {
        16
    } else if cache_size <= CACHE_RUNTIME_LARGE_BUDGET_BYTES {
        32
    } else {
        48
    };

    // Transit memory keeps enough headroom for read/upload concurrency while
    // still bounded independently of raw cache-size growth.
    let from_cache_budget = cache_size / 96;
    let from_memory_cache_budget = if memory_cache_budget_bytes == 0 {
        0
    } else {
        ((memory_cache_budget_bytes as u128)
            .saturating_mul(DEFAULT_TRANSIT_MEMORY_BUDGET_RATIO_PERCENT as u128)
            / 100)
            .min(u64::MAX as u128) as u64
    };
    let transit_memory_budget_bytes = clamp_usize(
        from_cache_budget
            .max(from_memory_cache_budget)
            .max(block_size),
        CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES.max(block_size as usize),
        CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES,
    );

    CacheRuntimeTuning {
        max_dirty_bytes,
        write_eager_flush_batch_bytes,
        upload_cache_io_slots,
        transit_memory_budget_bytes,
    }
}

fn derive_local_backend_budget_bytes(
    cache_size: u64,
    block_size: u32,
    cache_runtime_tuning: CacheRuntimeTuning,
) -> u64 {
    cache_size.max(minimum_local_backend_budget_bytes(
        block_size,
        cache_runtime_tuning,
    ))
}

fn minimum_local_backend_budget_bytes(
    block_size: u32,
    cache_runtime_tuning: CacheRuntimeTuning,
) -> u64 {
    u64::from(block_size.max(1))
        .max(cache_runtime_tuning.max_dirty_bytes as u64)
        .max(cache_runtime_tuning.write_eager_flush_batch_bytes as u64)
}

fn resolve_cache_runtime_tuning_overrides(
    derived: CacheRuntimeTuning,
    config: &VfsConfig,
    overrides: CacheRuntimeTuningOverrides,
) -> CacheRuntimeTuning {
    CacheRuntimeTuning {
        max_dirty_bytes: if overrides.max_dirty_bytes {
            config.max_dirty_bytes
        } else {
            derived.max_dirty_bytes
        },
        write_eager_flush_batch_bytes: if overrides.write_eager_flush_batch_bytes {
            config.write_eager_flush_batch_bytes
        } else {
            derived.write_eager_flush_batch_bytes
        },
        upload_cache_io_slots: if overrides.upload_cache_io_slots {
            config.upload_cache_io_slots
        } else {
            derived.upload_cache_io_slots
        },
        transit_memory_budget_bytes: if overrides.transit_memory_budget_bytes {
            config.transit_memory_budget_bytes
        } else {
            derived.transit_memory_budget_bytes
        },
    }
}

fn derive_upload_cache_io_foreground_cost_base(upload_cache_io_slots: usize) -> u32 {
    let slots = upload_cache_io_slots.clamp(1, u32::MAX as usize) as u32;
    slots
        .div_ceil(UPLOAD_CACHE_IO_FOREGROUND_BASE_DIVISOR.max(1))
        .clamp(1, UPLOAD_CACHE_IO_FOREGROUND_BASE_MAX)
        .min(slots)
}

fn derive_write_eager_flush_drain_chunk_bytes(
    write_eager_flush_batch_bytes: usize,
    block_size: u32,
) -> usize {
    let block_size = block_size.max(1) as usize;
    let threshold = write_eager_flush_batch_bytes.max(block_size);
    let target = threshold.saturating_div(2).max(block_size);
    target.clamp(block_size, WRITE_EAGER_DRAIN_CHUNK_MAX_BYTES.min(threshold))
}

fn derive_payload_put_concurrency(block_size: u32, eager_flush_drain_chunk_bytes: usize) -> usize {
    let block_size = block_size.max(1) as usize;
    let chunk_blocks = eager_flush_drain_chunk_bytes.div_ceil(block_size).max(1);
    PAYLOAD_PUT_CONCURRENCY_TARGET_BYTES
        .div_ceil(block_size)
        .clamp(1, PAYLOAD_PUT_CONCURRENCY_MAX)
        .min(chunk_blocks)
        .max(1)
}

fn derive_fast_mode_sync_group_min_bytes(
    _write_eager_flush_batch_bytes: usize,
    write_eager_flush_drain_chunk_bytes: usize,
) -> u64 {
    let target = write_eager_flush_drain_chunk_bytes as u64;
    target.clamp(
        FAST_MODE_SYNC_GROUP_MIN_BYTES_FLOOR,
        FAST_MODE_SYNC_GROUP_MIN_BYTES_CEIL,
    )
}

fn derive_upload_cache_io_foreground_cost_step(upload_cache_io_slots: usize) -> u32 {
    let slots = upload_cache_io_slots.clamp(1, u32::MAX as usize) as u32;
    slots
        .div_ceil(UPLOAD_CACHE_IO_FOREGROUND_STEP_DIVISOR.max(1))
        .clamp(1, UPLOAD_CACHE_IO_FOREGROUND_STEP_MAX)
        .min(slots)
}

impl UploadState {
    fn new(upload_cache_io_foreground_cost: u32) -> Self {
        Self {
            deleted_inodes: OrderedMutex::new(std::collections::HashSet::new(), LockTier::Upload),
            deleted_uploads_notify: tokio::sync::Notify::new(),
            upload_cancel_tokens: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Upload,
            ),
            pending_evictions: OrderedMutex::new(
                std::collections::HashSet::new(),
                LockTier::Upload,
            ),
            deferred_deleted_local_payloads: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Upload,
            ),
            pending_epoch_finalizations: OrderedMutex::new(BTreeSet::new(), LockTier::Upload),
            pending_epoch_finalizations_notify: tokio::sync::Notify::new(),
            upload_sticky_errors: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Upload,
            ),
            startup_replay_counts: OrderedMutex::new(HashMap::new(), LockTier::Upload),
            upload_requeues: AtomicU64::new(0),
            upload_canceled_deleted: AtomicU64::new(0),
            upload_canceled_stale: AtomicU64::new(0),
            upload_canceled_stale_before_put: AtomicU64::new(0),
            upload_canceled_stale_after_put: AtomicU64::new(0),
            upload_canceled_stale_after_put_bytes: AtomicU64::new(0),
            upload_local_payload_reads: AtomicU64::new(0),
            upload_local_payload_read_bytes: AtomicU64::new(0),
            upload_hot_payload_reads: AtomicU64::new(0),
            upload_hot_payload_read_bytes: AtomicU64::new(0),
            upload_staged_payload_reads: AtomicU64::new(0),
            upload_staged_payload_read_bytes: AtomicU64::new(0),
            upload_fallback_deferrals: AtomicU64::new(0),
            upload_fallback_rereads: AtomicU64::new(0),
            upload_fallback_reread_bytes: AtomicU64::new(0),
            upload_fallback_reread_cache_misses: AtomicU64::new(0),
            upload_fallback_wait_ns: AtomicU64::new(0),
            upload_fallback_wait_max_ns: AtomicU64::new(0),
            upload_fallback_read_ns: AtomicU64::new(0),
            upload_fallback_read_max_ns: AtomicU64::new(0),
            upload_cache_io_foreground_cost: AtomicU32::new(upload_cache_io_foreground_cost),
        }
    }
}

struct IncarnationState {
    inode_incarnations: OrderedMutex<std::collections::HashMap<Ino, InodeIncarnationState>>,
    live_extent_records: OrderedMutex<std::collections::HashMap<(Ino, u32), ExtentRecordId>>,
}

impl IncarnationState {
    fn new() -> Self {
        Self {
            inode_incarnations: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Incarnation,
            ),
            live_extent_records: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::Incarnation,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ForegroundReadSample {
    timestamp_ms: u64,
    latency_ns: u64,
    bytes_read: u64,
    access_class: AccessClass,
    policy: AccessPolicy,
}

const PUBLISHED_READ_PLAN_CACHE_MAX_INODES: usize = 1_024;
const PUBLISHED_READ_PLAN_CACHE_MAX_WINDOWS_PER_INODE: usize = 4;
const PUBLISHED_READ_PLAN_CACHE_WINDOW_BLOCKS: u64 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PublishedReadPlanCacheKey {
    size: u64,
    committed_size: u64,
    content_epoch: u64,
    committed_content_epoch: u64,
    upload_state: MetaUploadState,
}

impl PublishedReadPlanCacheKey {
    fn from_attr(attr: &InodeAttr) -> Option<Self> {
        (attr.kind == FileType::RegularFile
            && attr.upload_state == MetaUploadState::Ready
            && attr.content_epoch == attr.committed_content_epoch
            && attr.size == attr.committed_size)
            .then_some(Self::from_regular_attr(attr))
    }

    fn from_locally_stable_attr(attr: &InodeAttr) -> Option<Self> {
        (attr.kind == FileType::RegularFile).then_some(Self::from_regular_attr(attr))
    }

    fn from_regular_attr(attr: &InodeAttr) -> Self {
        Self {
            size: attr.size,
            committed_size: attr.committed_size,
            content_epoch: attr.content_epoch,
            committed_content_epoch: attr.committed_content_epoch,
            upload_state: attr.upload_state,
        }
    }
}

#[derive(Debug, Clone)]
struct PublishedReadPlanCacheEntry {
    key: PublishedReadPlanCacheKey,
    start: u64,
    end: u64,
    resolved_slices: Arc<[studiofs_meta::schema::ResolvedSliceSpan]>,
}

struct ReadState {
    foreground_reads_inflight: AtomicUsize,
    last_foreground_read_ms: AtomicU64,
    last_foreground_write_ms: AtomicU64,
    foreground_idle_notify: tokio::sync::Notify,
    foreground_read_latency_shards: Vec<Mutex<VecDeque<ForegroundReadSample>>>,
    foreground_read_shard_cursor: AtomicU64,
    /// Last observed foreground P90 (ns), used as sticky fallback.
    upload_fg_read_last_p90_ns: AtomicU64,
    /// Timestamp of the last observed foreground P90.
    upload_fg_read_last_p90_at_ms: AtomicU64,
    /// Last queued block range per inode to suppress duplicate read hints.
    prefetch_last_hint: Mutex<HashMap<(Ino, u64), (u32, u32)>>,
    read_access_fallback: Mutex<HashMap<Ino, Arc<Mutex<HandleAccessState>>>>,
    write_access_fallback: Mutex<HashMap<Ino, Arc<Mutex<HandleAccessState>>>>,
    published_read_plan_cache: Mutex<LruCache<Ino, VecDeque<PublishedReadPlanCacheEntry>>>,
    active_access_classes: [AtomicU64; AccessClass::COUNT],
    access_class_transitions: [AtomicU64; AccessClass::COUNT],
    foreground_local_payload_reads: AtomicU64,
    foreground_local_payload_read_bytes: AtomicU64,
    foreground_local_payload_read_io_ns: AtomicU64,
    foreground_local_payload_read_io_max_ns: AtomicU64,
}

impl ReadState {
    fn new(shard_count: usize) -> Self {
        Self {
            foreground_reads_inflight: AtomicUsize::new(0),
            last_foreground_read_ms: AtomicU64::new(0),
            last_foreground_write_ms: AtomicU64::new(0),
            foreground_idle_notify: tokio::sync::Notify::new(),
            foreground_read_latency_shards: (0..shard_count)
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
            foreground_read_shard_cursor: AtomicU64::new(0),
            upload_fg_read_last_p90_ns: AtomicU64::new(0),
            upload_fg_read_last_p90_at_ms: AtomicU64::new(0),
            prefetch_last_hint: Mutex::new(HashMap::new()),
            read_access_fallback: Mutex::new(HashMap::new()),
            write_access_fallback: Mutex::new(HashMap::new()),
            published_read_plan_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(PUBLISHED_READ_PLAN_CACHE_MAX_INODES)
                    .expect("published read-plan cache capacity is non-zero"),
            )),
            active_access_classes: std::array::from_fn(|_| AtomicU64::new(0)),
            access_class_transitions: std::array::from_fn(|_| AtomicU64::new(0)),
            foreground_local_payload_reads: AtomicU64::new(0),
            foreground_local_payload_read_bytes: AtomicU64::new(0),
            foreground_local_payload_read_io_ns: AtomicU64::new(0),
            foreground_local_payload_read_io_max_ns: AtomicU64::new(0),
        }
    }

    fn published_read_plan_cache_get(
        &self,
        ino: Ino,
        key: PublishedReadPlanCacheKey,
        start: u64,
        end: u64,
    ) -> Option<Arc<[studiofs_meta::schema::ResolvedSliceSpan]>> {
        let mut cache = self.published_read_plan_cache.lock();
        let entries = cache.get_mut(&ino)?;
        let index = entries
            .iter()
            .position(|entry| entry.key == key && start >= entry.start && end <= entry.end)?;
        let entry = entries.remove(index)?;
        let resolved_slices = entry.resolved_slices.clone();
        entries.push_front(entry);
        Some(resolved_slices)
    }

    fn published_read_plan_cache_put(
        &self,
        ino: Ino,
        key: PublishedReadPlanCacheKey,
        start: u64,
        end: u64,
        resolved_slices: Arc<[studiofs_meta::schema::ResolvedSliceSpan]>,
    ) {
        let mut cache = self.published_read_plan_cache.lock();
        let mut entries = cache.pop(&ino).unwrap_or_default();
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.key == key && entry.start == start && entry.end == end)
        {
            entries.remove(index);
        }
        entries.push_front(PublishedReadPlanCacheEntry {
            key,
            start,
            end,
            resolved_slices,
        });
        entries.truncate(PUBLISHED_READ_PLAN_CACHE_MAX_WINDOWS_PER_INODE);
        cache.put(ino, entries);
    }

    fn fallback_access_state(&self, ino: Ino, is_write: bool) -> Arc<Mutex<HandleAccessState>> {
        let states = if is_write {
            &self.write_access_fallback
        } else {
            &self.read_access_fallback
        };
        let mut states = states.lock();
        states
            .entry(ino)
            .or_insert_with(|| {
                let flags = if is_write {
                    OpenFlags::WRONLY
                } else {
                    OpenFlags::RDONLY
                };
                let stream_id = if is_write { u64::MAX } else { 0 };
                Arc::new(Mutex::new(HandleAccessState::new(stream_id, flags)))
            })
            .clone()
    }

    fn increment_active_access_class(&self, class: AccessClass) {
        self.active_access_classes[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_active_access_class(&self, class: AccessClass) {
        self.active_access_classes[class.index()]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    fn transition_active_access_class(&self, from: AccessClass, to: AccessClass) {
        if from == to {
            return;
        }
        self.decrement_active_access_class(from);
        self.increment_active_access_class(to);
    }

    fn active_access_class_count(&self, class: AccessClass) -> u64 {
        self.active_access_classes[class.index()].load(Ordering::Relaxed)
    }

    fn record_access_class_transition(
        &self,
        previous: AccessClass,
        next: AccessClass,
        update_active_handles: bool,
    ) {
        if previous == next {
            return;
        }
        self.access_class_transitions[next.index()].fetch_add(1, Ordering::Relaxed);
        if update_active_handles {
            self.transition_active_access_class(previous, next);
        }
    }

    fn observe_read_access(
        &self,
        open_file: Option<&OpenFile>,
        ino: Ino,
        offset: u64,
        len: usize,
        tracker: AccessTrackerConfig,
    ) -> AccessDecision {
        if let Some(open_file) = open_file {
            let decision = open_file.observe_read(offset, len, tracker);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, false);
        let decision = state.lock().observe_read(offset, len, tracker);
        self.record_access_class_transition(decision.previous_class, decision.class, false);
        decision
    }

    fn observe_write_access(
        &self,
        open_file: Option<&OpenFile>,
        ino: Ino,
        offset: u64,
        len: usize,
        tracker: AccessTrackerConfig,
    ) -> AccessDecision {
        self.last_foreground_write_ms
            .store(now_unix_millis(), Ordering::Relaxed);
        if let Some(open_file) = open_file {
            let decision = open_file.observe_write(offset, len, tracker);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, true);
        let decision = state.lock().observe_write(offset, len, tracker);
        self.record_access_class_transition(decision.previous_class, decision.class, false);
        decision
    }

    fn clear_access_tracking(&self, ino: Ino) {
        self.prefetch_last_hint
            .lock()
            .retain(|(tracked_ino, _), _| *tracked_ino != ino);
        self.read_access_fallback.lock().remove(&ino);
        self.write_access_fallback.lock().remove(&ino);
    }

    fn clear_prefetch_hint(&self, ino: Ino, stream_id: u64) {
        self.prefetch_last_hint.lock().remove(&(ino, stream_id));
    }

    fn record_prefetch_hint_range(
        &self,
        ino: Ino,
        stream_id: u64,
        range: PrefetchHintRange,
    ) -> PrefetchHintUpdate {
        let mut last_hints = self.prefetch_last_hint.lock();
        match range.update_against(last_hints.get(&(ino, stream_id)).copied()) {
            PrefetchHintUpdate::Suppress => PrefetchHintUpdate::Suppress,
            PrefetchHintUpdate::Store(stored) => {
                last_hints.insert((ino, stream_id), stored.as_stored());
                PrefetchHintUpdate::Store(stored)
            }
        }
    }

    fn active_access_counts(&self) -> [u64; AccessClass::COUNT] {
        std::array::from_fn(|idx| {
            if let Some(class) = AccessClass::from_index(idx) {
                self.active_access_class_count(class)
            } else {
                0
            }
        })
    }

    fn trim_foreground_latency_samples(
        samples: &mut VecDeque<ForegroundReadSample>,
        now_ms: u64,
        window: Duration,
    ) {
        let window_ms = window.as_millis().min(u64::MAX as u128) as u64;
        let cutoff = now_ms.saturating_sub(window_ms);
        while samples
            .front()
            .is_some_and(|sample| sample.timestamp_ms < cutoff)
        {
            samples.pop_front();
        }
        while samples.len() > FOREGROUND_READ_SPEEDOMETER_MAX_SAMPLES {
            samples.pop_front();
        }
    }

    fn select_foreground_latency_shard(&self) -> usize {
        if self.foreground_read_latency_shards.is_empty() {
            return 0;
        }
        let next = self
            .foreground_read_shard_cursor
            .fetch_add(1, Ordering::Relaxed);
        (next as usize) % self.foreground_read_latency_shards.len()
    }

    fn mark_foreground_read(&self, now_ms: u64) {
        self.foreground_reads_inflight
            .fetch_add(1, Ordering::Relaxed);
        self.last_foreground_read_ms
            .store(now_ms, Ordering::Relaxed);
    }

    fn unmark_foreground_read(&self, now_ms: u64) {
        let previous = self
            .foreground_reads_inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .unwrap_or(0);
        self.last_foreground_read_ms
            .store(now_ms, Ordering::Relaxed);
        if previous == 1 {
            self.foreground_idle_notify.notify_waiters();
        }
    }

    fn foreground_read_active(&self, now_ms: u64, idle_grace: Duration) -> bool {
        if self.foreground_reads_inflight.load(Ordering::Relaxed) > 0 {
            return true;
        }
        let last = self.last_foreground_read_ms.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= idle_grace.as_millis().min(u64::MAX as u128) as u64
    }

    fn foreground_user_io_active(&self, now_ms: u64, idle_grace: Duration) -> bool {
        if self.foreground_read_active(now_ms, idle_grace) {
            return true;
        }
        let last = self.last_foreground_write_ms.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= idle_grace.as_millis().min(u64::MAX as u128) as u64
    }

    fn record_foreground_read_latency(
        &self,
        shard_idx: usize,
        telemetry: ForegroundReadTelemetry,
        now_ms: u64,
        window: Duration,
    ) {
        let Some(shard) = self.foreground_read_latency_shards.get(shard_idx) else {
            return;
        };
        let latency_ns = telemetry.elapsed.as_nanos().clamp(1, u64::MAX as u128) as u64;
        let mut samples = shard.lock();
        samples.push_back(ForegroundReadSample {
            timestamp_ms: now_ms,
            latency_ns,
            bytes_read: telemetry.bytes_read,
            access_class: telemetry.access_class,
            policy: telemetry.policy,
        });
        Self::trim_foreground_latency_samples(&mut samples, now_ms, window);
        self.upload_fg_read_last_p90_ns
            .store(latency_ns, Ordering::Release);
        self.upload_fg_read_last_p90_at_ms
            .store(now_ms, Ordering::Release);
    }

    fn foreground_speedometer_snapshot(
        &self,
        now_ms: u64,
        window: Duration,
    ) -> (Option<u64>, usize, u64, u64) {
        let mut ages = Vec::new();
        let mut total_samples = 0usize;
        let mut window_bytes = 0u64;
        let mut oldest_start_ms = u64::MAX;
        let mut newest_end_ms = 0u64;

        for shard in &self.foreground_read_latency_shards {
            let mut samples = shard.lock();
            Self::trim_foreground_latency_samples(&mut samples, now_ms, window);
            total_samples += samples.len();
            for sample in samples.iter() {
                ages.push(sample.latency_ns);
                window_bytes = window_bytes.saturating_add(sample.bytes_read);
                oldest_start_ms = oldest_start_ms.min(sample.timestamp_ms);
                newest_end_ms = newest_end_ms.max(sample.timestamp_ms);
            }
        }

        if ages.is_empty() {
            let sticky_p90 = self.upload_fg_read_last_p90_ns.load(Ordering::Acquire);
            let sticky_at = self.upload_fg_read_last_p90_at_ms.load(Ordering::Acquire);
            let sticky_window_ms = window.as_millis().min(u64::MAX as u128) as u64;
            let sticky_valid = sticky_p90 > 0
                && sticky_at > 0
                && now_ms.saturating_sub(sticky_at) <= sticky_window_ms;
            return (sticky_valid.then_some(sticky_p90), 0, 0, 0);
        }

        ages.sort_unstable();
        let p90_idx = ((ages.len() - 1).saturating_mul(90)) / 100;
        let p90_ns = Some(ages[p90_idx]);
        self.upload_fg_read_last_p90_ns
            .store(ages[p90_idx], Ordering::Release);
        self.upload_fg_read_last_p90_at_ms
            .store(now_ms, Ordering::Release);
        let window_coverage_ms = if total_samples == 0 {
            0
        } else {
            newest_end_ms.saturating_sub(oldest_start_ms).max(1)
        };
        (p90_ns, total_samples, window_bytes, window_coverage_ms)
    }

    fn foreground_access_pattern_snapshot(
        &self,
        now_ms: u64,
        window: Duration,
    ) -> Option<(AccessClass, AccessPolicy)> {
        let mut bytes_by_class = [0u64; AccessClass::COUNT];
        let mut latest_policy_by_class = [AccessPolicy::default(); AccessClass::COUNT];

        for shard in &self.foreground_read_latency_shards {
            let mut samples = shard.lock();
            Self::trim_foreground_latency_samples(&mut samples, now_ms, window);
            for sample in samples.iter() {
                let class_idx = sample.access_class.index();
                bytes_by_class[class_idx] =
                    bytes_by_class[class_idx].saturating_add(sample.bytes_read.max(1));
                latest_policy_by_class[class_idx] = sample.policy;
            }
        }

        let (dominant_idx, dominant_bytes) = bytes_by_class
            .iter()
            .copied()
            .enumerate()
            .max_by_key(|&(_, bytes)| bytes)?;
        if dominant_bytes == 0 {
            return None;
        }
        let dominant_class = AccessClass::from_index(dominant_idx)?;
        Some((dominant_class, latest_policy_by_class[dominant_idx]))
    }
}

struct ShutdownState {
    shutdown_requested: std::sync::atomic::AtomicBool,
    uploader_shutdown_requested: std::sync::atomic::AtomicBool,
    shutdown_completed: std::sync::atomic::AtomicBool,
    shutdown_notify: tokio::sync::Notify,
    uploader_shutdown_token: CancellationToken,
    slab_flusher_stopped: std::sync::atomic::AtomicBool,
    background_tasks: OrderedMutex<Vec<JoinHandle<()>>>,
    uploader_task: OrderedMutex<Option<JoinHandle<()>>>,
}

impl ShutdownState {
    fn new() -> Self {
        Self {
            shutdown_requested: std::sync::atomic::AtomicBool::new(false),
            uploader_shutdown_requested: std::sync::atomic::AtomicBool::new(false),
            shutdown_completed: std::sync::atomic::AtomicBool::new(false),
            shutdown_notify: tokio::sync::Notify::new(),
            uploader_shutdown_token: CancellationToken::new(),
            slab_flusher_stopped: std::sync::atomic::AtomicBool::new(false),
            background_tasks: OrderedMutex::new(Vec::new(), LockTier::Shutdown),
            uploader_task: OrderedMutex::new(None, LockTier::Shutdown),
        }
    }
}

struct MetricsState {
    materialize_batch_stats: MaterializeBatchStats,
    ignored_entries: OrderedMutex<IgnoredEntryRegistry>,
    next_ignored_ino: AtomicU64,
    /// Current number of items queued in the prefetch worker's pending queue.
    /// Useful for detecting backpressure under high handle-count workloads.
    prefetch_queue_depth: AtomicU64,
    flush_barrier_calls: AtomicU64,
    local_cache_io_calls: AtomicU64,
    local_cache_io_handoff_total_ns: AtomicU64,
    local_cache_io_handoff_max_ns: AtomicU64,
    local_cache_io_handoff_histogram: LatencyHistogram,
    local_cache_io_backend_total_ns: AtomicU64,
    local_cache_io_backend_max_ns: AtomicU64,
    local_cache_io_backend_histogram: LatencyHistogram,
    payload_store_sync_data_calls: AtomicU64,
    payload_store_sync_data_deferred_calls: AtomicU64,
    payload_store_sync_data_synced_bytes: AtomicU64,
    payload_store_sync_data_total_ns: AtomicU64,
    payload_store_sync_data_max_ns: AtomicU64,
    payload_store_sync_data_histogram: LatencyHistogram,
    payload_store_sync_data_queue_wait_calls: AtomicU64,
    payload_store_sync_data_queue_wait_total_ns: AtomicU64,
    payload_store_sync_data_queue_wait_max_ns: AtomicU64,
    payload_store_sync_data_queue_wait_histogram: LatencyHistogram,
    payload_store_sync_data_last_completed_ns: AtomicU64,
    payload_store_sync_data_interval_total_ns: AtomicU64,
    payload_store_sync_data_interval_max_ns: AtomicU64,
    payload_store_sync_data_interval_histogram: LatencyHistogram,
    payload_persist_calls: AtomicU64,
    payload_persist_total_ns: AtomicU64,
    payload_persist_max_ns: AtomicU64,
    payload_persist_histogram: LatencyHistogram,
    payload_burst_count: AtomicU64,
    payload_burst_total_payloads: AtomicU64,
    payload_burst_total_bytes: AtomicU64,
    payload_burst_max_payloads: AtomicU64,
    payload_burst_max_bytes: AtomicU64,
    payload_burst_last_completed_ns: AtomicU64,
    payload_burst_interval_total_ns: AtomicU64,
    payload_burst_interval_max_ns: AtomicU64,
    payload_burst_interval_histogram: LatencyHistogram,
    metadata_commit_calls: AtomicU64,
    metadata_commit_total_ns: AtomicU64,
    metadata_commit_max_ns: AtomicU64,
    metadata_commit_histogram: LatencyHistogram,
    metadata_commit_completed_calls: AtomicU64,
    metadata_commit_last_completed_ns: AtomicU64,
    metadata_commit_interval_total_ns: AtomicU64,
    metadata_commit_interval_max_ns: AtomicU64,
    metadata_commit_interval_histogram: LatencyHistogram,
    eager_flush_checks: AtomicU64,
    eager_flush_triggered: AtomicU64,
    eager_flush_skipped_empty: AtomicU64,
    eager_flush_skipped_below_threshold: AtomicU64,
    drain_lag_pacing_events: AtomicU64,
    drain_lag_pacing_total_ms: AtomicU64,
    drain_lag_pacing_max_ms: AtomicU64,
    dirty_pressure_peak_bytes: AtomicU64,
    staged_pressure_peak_bytes: AtomicU64,
    vfs_read_calls: AtomicU64,
    vfs_read_requested_bytes: AtomicU64,
    vfs_write_calls: AtomicU64,
    vfs_write_bytes: AtomicU64,
    vfs_write_record_writer_stack_state_calls: AtomicU64,
    vfs_write_record_writer_stack_state_total_ns: AtomicU64,
    vfs_write_record_writer_stack_state_max_ns: AtomicU64,
    vfs_write_record_writer_stack_state_histogram: LatencyHistogram,
    vfs_write_materialize_completed_calls: AtomicU64,
    vfs_write_materialize_completed_total_ns: AtomicU64,
    vfs_write_materialize_completed_max_ns: AtomicU64,
    vfs_write_materialize_completed_histogram: LatencyHistogram,
    vfs_write_materialize_strict_calls: AtomicU64,
    vfs_write_materialize_strict_total_ns: AtomicU64,
    vfs_write_materialize_strict_max_ns: AtomicU64,
    vfs_write_materialize_strict_histogram: LatencyHistogram,
    vfs_write_materialize_for_limits_calls: AtomicU64,
    vfs_write_materialize_for_limits_total_ns: AtomicU64,
    vfs_write_materialize_for_limits_max_ns: AtomicU64,
    vfs_write_materialize_for_limits_histogram: LatencyHistogram,
    winfsp_write_callbacks: AtomicU64,
    winfsp_write_callback_bytes: AtomicU64,
    winfsp_write_callback_total_ns: AtomicU64,
    winfsp_write_callback_max_ns: AtomicU64,
    winfsp_write_callback_histogram: LatencyHistogram,
    winfsp_write_callback_vfs_total_ns: AtomicU64,
    winfsp_write_callback_vfs_max_ns: AtomicU64,
    winfsp_write_callback_vfs_histogram: LatencyHistogram,
    winfsp_write_callback_getattr_total_ns: AtomicU64,
    winfsp_write_callback_getattr_max_ns: AtomicU64,
    winfsp_write_callback_getattr_histogram: LatencyHistogram,
    winfsp_write_callback_strict_calls: AtomicU64,
    winfsp_write_callback_strict_bytes: AtomicU64,
    winfsp_write_callback_write_through_calls: AtomicU64,
    winfsp_write_callback_write_through_bytes: AtomicU64,
    winfsp_write_callback_opportunistic_calls: AtomicU64,
    winfsp_write_callback_opportunistic_bytes: AtomicU64,
    winfsp_read_callbacks: AtomicU64,
    winfsp_read_callback_bytes: AtomicU64,
    winfsp_read_callback_total_ns: AtomicU64,
    winfsp_read_callback_max_ns: AtomicU64,
    winfsp_overwrite_callbacks: AtomicU64,
    winfsp_cleanup_callbacks: AtomicU64,
    winfsp_flush_callbacks: AtomicU64,
    winfsp_close_callbacks: AtomicU64,
    winfsp_cleanup_flush_fh_callbacks: AtomicU64,
    winfsp_os_cache_enabled_open: AtomicU64,
    winfsp_os_cache_disabled_open: AtomicU64,
    winfsp_os_cache_miss_read_callbacks: AtomicU64,
    winfsp_os_cache_miss_read_bytes: AtomicU64,
    winfsp_cleanup_barrier_calls: AtomicU64,
    winfsp_flush_barrier_calls: AtomicU64,
    winfsp_close_barrier_calls: AtomicU64,
    winfsp_cleanup_barrier_skipped_clean: AtomicU64,
    winfsp_flush_barrier_skipped_clean: AtomicU64,
    winfsp_close_barrier_skipped_clean: AtomicU64,
    fuse_read_callbacks: AtomicU64,
    fuse_read_callback_bytes: AtomicU64,
    fuse_read_callback_errors: AtomicU64,
    fuse_read_callback_total_ns: AtomicU64,
    fuse_read_callback_max_ns: AtomicU64,
    fuse_write_callbacks: AtomicU64,
    fuse_write_callback_bytes: AtomicU64,
    fuse_write_callback_errors: AtomicU64,
    fuse_write_callback_total_ns: AtomicU64,
    fuse_write_callback_max_ns: AtomicU64,
    macfuse_open_cache_read_only_keep: AtomicU64,
    macfuse_open_cache_read_only_local_stable_keep: AtomicU64,
    macfuse_open_cache_read_only_unknown_state: AtomicU64,
    macfuse_open_cache_read_only_not_regular_file: AtomicU64,
    macfuse_open_cache_read_only_local_dirty_state: AtomicU64,
    macfuse_open_cache_read_only_active_writer: AtomicU64,
    macfuse_open_cache_read_only_generation_changed: AtomicU64,
    macfuse_open_cache_read_only_local_stable_generation_changed: AtomicU64,
    macfuse_open_cache_write_capable: AtomicU64,
    macfuse_open_cache_clean_write_release_seeded: AtomicU64,
    macfuse_open_cache_local_stable_write_release_seeded: AtomicU64,
    zeroio_replay_failures: AtomicU64,
    zeroio_publish_failures: AtomicU64,
    runtime_event_sequence: AtomicU64,
    runtime_events: OrderedMutex<VecDeque<VfsRuntimeEvent>>,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            materialize_batch_stats: MaterializeBatchStats::default(),
            ignored_entries: OrderedMutex::new(IgnoredEntryRegistry::default(), LockTier::Metrics),
            next_ignored_ino: AtomicU64::new(1u64 << 63),
            prefetch_queue_depth: AtomicU64::new(0),
            flush_barrier_calls: AtomicU64::new(0),
            local_cache_io_calls: AtomicU64::new(0),
            local_cache_io_handoff_total_ns: AtomicU64::new(0),
            local_cache_io_handoff_max_ns: AtomicU64::new(0),
            local_cache_io_handoff_histogram: LatencyHistogram::default(),
            local_cache_io_backend_total_ns: AtomicU64::new(0),
            local_cache_io_backend_max_ns: AtomicU64::new(0),
            local_cache_io_backend_histogram: LatencyHistogram::default(),
            payload_store_sync_data_calls: AtomicU64::new(0),
            payload_store_sync_data_deferred_calls: AtomicU64::new(0),
            payload_store_sync_data_synced_bytes: AtomicU64::new(0),
            payload_store_sync_data_total_ns: AtomicU64::new(0),
            payload_store_sync_data_max_ns: AtomicU64::new(0),
            payload_store_sync_data_histogram: LatencyHistogram::default(),
            payload_store_sync_data_queue_wait_calls: AtomicU64::new(0),
            payload_store_sync_data_queue_wait_total_ns: AtomicU64::new(0),
            payload_store_sync_data_queue_wait_max_ns: AtomicU64::new(0),
            payload_store_sync_data_queue_wait_histogram: LatencyHistogram::default(),
            payload_store_sync_data_last_completed_ns: AtomicU64::new(0),
            payload_store_sync_data_interval_total_ns: AtomicU64::new(0),
            payload_store_sync_data_interval_max_ns: AtomicU64::new(0),
            payload_store_sync_data_interval_histogram: LatencyHistogram::default(),
            payload_persist_calls: AtomicU64::new(0),
            payload_persist_total_ns: AtomicU64::new(0),
            payload_persist_max_ns: AtomicU64::new(0),
            payload_persist_histogram: LatencyHistogram::default(),
            payload_burst_count: AtomicU64::new(0),
            payload_burst_total_payloads: AtomicU64::new(0),
            payload_burst_total_bytes: AtomicU64::new(0),
            payload_burst_max_payloads: AtomicU64::new(0),
            payload_burst_max_bytes: AtomicU64::new(0),
            payload_burst_last_completed_ns: AtomicU64::new(0),
            payload_burst_interval_total_ns: AtomicU64::new(0),
            payload_burst_interval_max_ns: AtomicU64::new(0),
            payload_burst_interval_histogram: LatencyHistogram::default(),
            metadata_commit_calls: AtomicU64::new(0),
            metadata_commit_total_ns: AtomicU64::new(0),
            metadata_commit_max_ns: AtomicU64::new(0),
            metadata_commit_histogram: LatencyHistogram::default(),
            metadata_commit_completed_calls: AtomicU64::new(0),
            metadata_commit_last_completed_ns: AtomicU64::new(0),
            metadata_commit_interval_total_ns: AtomicU64::new(0),
            metadata_commit_interval_max_ns: AtomicU64::new(0),
            metadata_commit_interval_histogram: LatencyHistogram::default(),
            eager_flush_checks: AtomicU64::new(0),
            eager_flush_triggered: AtomicU64::new(0),
            eager_flush_skipped_empty: AtomicU64::new(0),
            eager_flush_skipped_below_threshold: AtomicU64::new(0),
            drain_lag_pacing_events: AtomicU64::new(0),
            drain_lag_pacing_total_ms: AtomicU64::new(0),
            drain_lag_pacing_max_ms: AtomicU64::new(0),
            dirty_pressure_peak_bytes: AtomicU64::new(0),
            staged_pressure_peak_bytes: AtomicU64::new(0),
            vfs_read_calls: AtomicU64::new(0),
            vfs_read_requested_bytes: AtomicU64::new(0),
            vfs_write_calls: AtomicU64::new(0),
            vfs_write_bytes: AtomicU64::new(0),
            vfs_write_record_writer_stack_state_calls: AtomicU64::new(0),
            vfs_write_record_writer_stack_state_total_ns: AtomicU64::new(0),
            vfs_write_record_writer_stack_state_max_ns: AtomicU64::new(0),
            vfs_write_record_writer_stack_state_histogram: LatencyHistogram::default(),
            vfs_write_materialize_completed_calls: AtomicU64::new(0),
            vfs_write_materialize_completed_total_ns: AtomicU64::new(0),
            vfs_write_materialize_completed_max_ns: AtomicU64::new(0),
            vfs_write_materialize_completed_histogram: LatencyHistogram::default(),
            vfs_write_materialize_strict_calls: AtomicU64::new(0),
            vfs_write_materialize_strict_total_ns: AtomicU64::new(0),
            vfs_write_materialize_strict_max_ns: AtomicU64::new(0),
            vfs_write_materialize_strict_histogram: LatencyHistogram::default(),
            vfs_write_materialize_for_limits_calls: AtomicU64::new(0),
            vfs_write_materialize_for_limits_total_ns: AtomicU64::new(0),
            vfs_write_materialize_for_limits_max_ns: AtomicU64::new(0),
            vfs_write_materialize_for_limits_histogram: LatencyHistogram::default(),
            winfsp_write_callbacks: AtomicU64::new(0),
            winfsp_write_callback_bytes: AtomicU64::new(0),
            winfsp_write_callback_total_ns: AtomicU64::new(0),
            winfsp_write_callback_max_ns: AtomicU64::new(0),
            winfsp_write_callback_histogram: LatencyHistogram::default(),
            winfsp_write_callback_vfs_total_ns: AtomicU64::new(0),
            winfsp_write_callback_vfs_max_ns: AtomicU64::new(0),
            winfsp_write_callback_vfs_histogram: LatencyHistogram::default(),
            winfsp_write_callback_getattr_total_ns: AtomicU64::new(0),
            winfsp_write_callback_getattr_max_ns: AtomicU64::new(0),
            winfsp_write_callback_getattr_histogram: LatencyHistogram::default(),
            winfsp_write_callback_strict_calls: AtomicU64::new(0),
            winfsp_write_callback_strict_bytes: AtomicU64::new(0),
            winfsp_write_callback_write_through_calls: AtomicU64::new(0),
            winfsp_write_callback_write_through_bytes: AtomicU64::new(0),
            winfsp_write_callback_opportunistic_calls: AtomicU64::new(0),
            winfsp_write_callback_opportunistic_bytes: AtomicU64::new(0),
            winfsp_read_callbacks: AtomicU64::new(0),
            winfsp_read_callback_bytes: AtomicU64::new(0),
            winfsp_read_callback_total_ns: AtomicU64::new(0),
            winfsp_read_callback_max_ns: AtomicU64::new(0),
            winfsp_overwrite_callbacks: AtomicU64::new(0),
            winfsp_cleanup_callbacks: AtomicU64::new(0),
            winfsp_flush_callbacks: AtomicU64::new(0),
            winfsp_close_callbacks: AtomicU64::new(0),
            winfsp_cleanup_flush_fh_callbacks: AtomicU64::new(0),
            winfsp_os_cache_enabled_open: AtomicU64::new(0),
            winfsp_os_cache_disabled_open: AtomicU64::new(0),
            winfsp_os_cache_miss_read_callbacks: AtomicU64::new(0),
            winfsp_os_cache_miss_read_bytes: AtomicU64::new(0),
            winfsp_cleanup_barrier_calls: AtomicU64::new(0),
            winfsp_flush_barrier_calls: AtomicU64::new(0),
            winfsp_close_barrier_calls: AtomicU64::new(0),
            winfsp_cleanup_barrier_skipped_clean: AtomicU64::new(0),
            winfsp_flush_barrier_skipped_clean: AtomicU64::new(0),
            winfsp_close_barrier_skipped_clean: AtomicU64::new(0),
            fuse_read_callbacks: AtomicU64::new(0),
            fuse_read_callback_bytes: AtomicU64::new(0),
            fuse_read_callback_errors: AtomicU64::new(0),
            fuse_read_callback_total_ns: AtomicU64::new(0),
            fuse_read_callback_max_ns: AtomicU64::new(0),
            fuse_write_callbacks: AtomicU64::new(0),
            fuse_write_callback_bytes: AtomicU64::new(0),
            fuse_write_callback_errors: AtomicU64::new(0),
            fuse_write_callback_total_ns: AtomicU64::new(0),
            fuse_write_callback_max_ns: AtomicU64::new(0),
            macfuse_open_cache_read_only_keep: AtomicU64::new(0),
            macfuse_open_cache_read_only_local_stable_keep: AtomicU64::new(0),
            macfuse_open_cache_read_only_unknown_state: AtomicU64::new(0),
            macfuse_open_cache_read_only_not_regular_file: AtomicU64::new(0),
            macfuse_open_cache_read_only_local_dirty_state: AtomicU64::new(0),
            macfuse_open_cache_read_only_active_writer: AtomicU64::new(0),
            macfuse_open_cache_read_only_generation_changed: AtomicU64::new(0),
            macfuse_open_cache_read_only_local_stable_generation_changed: AtomicU64::new(0),
            macfuse_open_cache_write_capable: AtomicU64::new(0),
            macfuse_open_cache_clean_write_release_seeded: AtomicU64::new(0),
            macfuse_open_cache_local_stable_write_release_seeded: AtomicU64::new(0),
            zeroio_replay_failures: AtomicU64::new(0),
            zeroio_publish_failures: AtomicU64::new(0),
            runtime_event_sequence: AtomicU64::new(0),
            runtime_events: OrderedMutex::new(VecDeque::new(), LockTier::Metrics),
        }
    }

    fn reset_diagnostics_observability(&self) {
        macro_rules! reset_atomic {
            ($($field:ident),+ $(,)?) => {
                $(self.$field.store(0, Ordering::Relaxed);)+
            };
        }

        // Keep this reset path aligned with new MetricsState fields so the
        // diagnostics "reset counters" action remains a complete bounded reset.
        self.materialize_batch_stats.reset();
        self.prefetch_queue_depth.store(0, Ordering::Relaxed);
        reset_atomic!(
            flush_barrier_calls,
            local_cache_io_calls,
            local_cache_io_handoff_total_ns,
            local_cache_io_handoff_max_ns,
            local_cache_io_backend_total_ns,
            local_cache_io_backend_max_ns,
            payload_store_sync_data_calls,
            payload_store_sync_data_deferred_calls,
            payload_store_sync_data_synced_bytes,
            payload_store_sync_data_total_ns,
            payload_store_sync_data_max_ns,
            payload_store_sync_data_queue_wait_calls,
            payload_store_sync_data_queue_wait_total_ns,
            payload_store_sync_data_queue_wait_max_ns,
            payload_store_sync_data_last_completed_ns,
            payload_store_sync_data_interval_total_ns,
            payload_store_sync_data_interval_max_ns,
            payload_persist_calls,
            payload_persist_total_ns,
            payload_persist_max_ns,
            payload_burst_count,
            payload_burst_total_payloads,
            payload_burst_total_bytes,
            payload_burst_max_payloads,
            payload_burst_max_bytes,
            payload_burst_last_completed_ns,
            payload_burst_interval_total_ns,
            payload_burst_interval_max_ns,
            metadata_commit_calls,
            metadata_commit_total_ns,
            metadata_commit_max_ns,
            metadata_commit_completed_calls,
            metadata_commit_last_completed_ns,
            metadata_commit_interval_total_ns,
            metadata_commit_interval_max_ns,
            eager_flush_checks,
            eager_flush_triggered,
            eager_flush_skipped_empty,
            eager_flush_skipped_below_threshold,
            drain_lag_pacing_events,
            drain_lag_pacing_total_ms,
            drain_lag_pacing_max_ms,
            dirty_pressure_peak_bytes,
            staged_pressure_peak_bytes,
            vfs_read_calls,
            vfs_read_requested_bytes,
            vfs_write_calls,
            vfs_write_bytes,
            vfs_write_record_writer_stack_state_calls,
            vfs_write_record_writer_stack_state_total_ns,
            vfs_write_record_writer_stack_state_max_ns,
            vfs_write_materialize_completed_calls,
            vfs_write_materialize_completed_total_ns,
            vfs_write_materialize_completed_max_ns,
            vfs_write_materialize_strict_calls,
            vfs_write_materialize_strict_total_ns,
            vfs_write_materialize_strict_max_ns,
            vfs_write_materialize_for_limits_calls,
            vfs_write_materialize_for_limits_total_ns,
            vfs_write_materialize_for_limits_max_ns,
            winfsp_write_callbacks,
            winfsp_write_callback_bytes,
            winfsp_write_callback_strict_calls,
            winfsp_write_callback_strict_bytes,
            winfsp_write_callback_write_through_calls,
            winfsp_write_callback_write_through_bytes,
            winfsp_write_callback_opportunistic_calls,
            winfsp_write_callback_opportunistic_bytes,
            winfsp_write_callback_total_ns,
            winfsp_write_callback_max_ns,
            winfsp_write_callback_vfs_total_ns,
            winfsp_write_callback_vfs_max_ns,
            winfsp_write_callback_getattr_total_ns,
            winfsp_write_callback_getattr_max_ns,
            winfsp_read_callbacks,
            winfsp_read_callback_bytes,
            winfsp_read_callback_total_ns,
            winfsp_read_callback_max_ns,
            winfsp_overwrite_callbacks,
            winfsp_cleanup_callbacks,
            winfsp_flush_callbacks,
            winfsp_close_callbacks,
            winfsp_cleanup_flush_fh_callbacks,
            winfsp_os_cache_enabled_open,
            winfsp_os_cache_disabled_open,
            winfsp_os_cache_miss_read_callbacks,
            winfsp_os_cache_miss_read_bytes,
            winfsp_cleanup_barrier_calls,
            winfsp_flush_barrier_calls,
            winfsp_close_barrier_calls,
            winfsp_cleanup_barrier_skipped_clean,
            winfsp_flush_barrier_skipped_clean,
            winfsp_close_barrier_skipped_clean,
            fuse_read_callbacks,
            fuse_read_callback_bytes,
            fuse_read_callback_errors,
            fuse_read_callback_total_ns,
            fuse_read_callback_max_ns,
            fuse_write_callbacks,
            fuse_write_callback_bytes,
            fuse_write_callback_errors,
            fuse_write_callback_total_ns,
            fuse_write_callback_max_ns,
            macfuse_open_cache_read_only_keep,
            macfuse_open_cache_read_only_local_stable_keep,
            macfuse_open_cache_read_only_unknown_state,
            macfuse_open_cache_read_only_not_regular_file,
            macfuse_open_cache_read_only_local_dirty_state,
            macfuse_open_cache_read_only_active_writer,
            macfuse_open_cache_read_only_generation_changed,
            macfuse_open_cache_read_only_local_stable_generation_changed,
            macfuse_open_cache_write_capable,
            macfuse_open_cache_clean_write_release_seeded,
            macfuse_open_cache_local_stable_write_release_seeded,
            zeroio_replay_failures,
            zeroio_publish_failures,
            runtime_event_sequence
        );
        self.local_cache_io_handoff_histogram.reset();
        self.local_cache_io_backend_histogram.reset();
        self.payload_store_sync_data_histogram.reset();
        self.payload_store_sync_data_queue_wait_histogram.reset();
        self.payload_store_sync_data_interval_histogram.reset();
        self.payload_persist_histogram.reset();
        self.payload_burst_interval_histogram.reset();
        self.metadata_commit_histogram.reset();
        self.metadata_commit_interval_histogram.reset();
        self.vfs_write_record_writer_stack_state_histogram.reset();
        self.vfs_write_materialize_completed_histogram.reset();
        self.vfs_write_materialize_strict_histogram.reset();
        self.vfs_write_materialize_for_limits_histogram.reset();
        self.winfsp_write_callback_histogram.reset();
        self.winfsp_write_callback_vfs_histogram.reset();
        self.winfsp_write_callback_getattr_histogram.reset();
        self.runtime_events.lock().clear();
    }
}

// =============================================================================
// Internal State (composed subsystems)
// =============================================================================

impl VfsState {
    fn new(
        cached_store: Arc<CachedStore>,
        block_size: u32,
        cfg: &VfsConfig,
        upload_cache_io_foreground_cost_base: u32,
    ) -> Self {
        Self {
            upload: UploadState::new(upload_cache_io_foreground_cost_base),
            file_session: FileSessionState::new(),
            namespace: NamespaceState::new(
                cfg.dentry_cache_capacity,
                cfg.dentry_cache_bytes,
                cfg.path_shortcut_cache_capacity,
                cfg.path_shortcut_cache_bytes,
                cfg.attr_read_cache_capacity,
                cfg.attr_read_cache_bytes,
            ),
            acl: AclState::new(),
            incarnation: IncarnationState::new(),
            writer_stack: MountedWriterStack::new(
                cached_store,
                WriterStackConfig {
                    block_size,
                    chunk_size: DEFAULT_CHUNK_SIZE,
                    page_size: resolve_writer_page_size(block_size, cfg.writer_page_size),
                    dirty_byte_limit: Some(cfg.max_dirty_bytes),
                    max_buffer_bytes: Some(cfg.max_dirty_bytes),
                },
            ),
            writer_stack_prefetched_object_ids: Mutex::new(VecDeque::new()),
            local_dirty: LocalDirtyState::new(),
            read: ReadState::new(cfg.foreground_read_speedometer_shards),
            file_lock: FileLockState::new(),
            shutdown: ShutdownState::new(),
            metrics: MetricsState::new(),
            fuse_async_write_snapshot: Mutex::new(VfsFuseAsyncWriteSnapshot::default()),
            fuse_async_write_raw_snapshot: Mutex::new(VfsFuseAsyncWriteSnapshot::default()),
            fuse_async_write_reset_baseline: Mutex::new(VfsFuseAsyncWriteSnapshot::default()),
            fuse_async_read_snapshot: Mutex::new(VfsFuseAsyncReadSnapshot::default()),
            write_admission_governor: Mutex::new(WriteAdmissionGovernor::default()),
            sustained_write_streams: Mutex::new(HashMap::new()),
            deferred_materialize_inflight: DeferredInodeGate::default(),
            deferred_materialize_reschedule_needed: Mutex::new(HashSet::new()),
            deferred_release_inflight: DeferredInodeGate::default(),
        }
    }
}

fn resolve_writer_page_size(block_size: u32, writer_page_size: Option<usize>) -> usize {
    let block_size = block_size.max(1) as usize;
    writer_page_size
        .map(|size| size.min(block_size))
        .filter(|size| *size > 0 && block_size.is_multiple_of(*size))
        .unwrap_or(block_size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PrefetchedWriterStackObjectIds {
    ino: Ino,
    start: u64,
    end: u64,
}

impl PrefetchedWriterStackObjectIds {
    fn new(ino: Ino, start: u64, len: u64) -> Option<Self> {
        Some(Self {
            ino,
            start,
            end: start.checked_add(len)?,
        })
    }

    fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    fn is_empty(self) -> bool {
        self.start >= self.end
    }
}

struct VfsState {
    upload: UploadState,
    file_session: FileSessionState,
    namespace: NamespaceState,
    acl: AclState,
    incarnation: IncarnationState,
    writer_stack: MountedWriterStack,
    writer_stack_prefetched_object_ids: Mutex<VecDeque<PrefetchedWriterStackObjectIds>>,
    local_dirty: LocalDirtyState,
    read: ReadState,
    file_lock: FileLockState,
    shutdown: ShutdownState,
    metrics: MetricsState,
    fuse_async_write_snapshot: Mutex<VfsFuseAsyncWriteSnapshot>,
    fuse_async_write_raw_snapshot: Mutex<VfsFuseAsyncWriteSnapshot>,
    fuse_async_write_reset_baseline: Mutex<VfsFuseAsyncWriteSnapshot>,
    fuse_async_read_snapshot: Mutex<VfsFuseAsyncReadSnapshot>,
    write_admission_governor: Mutex<WriteAdmissionGovernor>,
    sustained_write_streams: Mutex<HashMap<Ino, SustainedWriteState>>,
    /// Tracks inodes that currently have a deferred materialization task
    /// in-flight. The gate keeps synchronous registry access scoped away from
    /// async waiting while preserving one background task per inode.
    deferred_materialize_inflight: DeferredInodeGate,
    /// Inodes that received another write while their materializer was already
    /// in-flight. The finishing task consumes this flag and schedules one more
    /// pass if dirty writer-stack bytes remain.
    deferred_materialize_reschedule_needed: Mutex<HashSet<Ino>>,
    /// Tracks last-handle release barriers scheduled by WinFSP close callbacks.
    /// Foreground flush/fsync paths wait on this before reporting success so a
    /// close-deferred metadata commit cannot race an immediate explicit sync.
    deferred_release_inflight: DeferredInodeGate,
}

#[derive(Clone, Default)]
struct DeferredInodeGate {
    entries: Arc<Mutex<HashMap<Ino, Arc<tokio::sync::Mutex<()>>>>>,
}

impl DeferredInodeGate {
    fn create_task_lock() -> (
        Arc<tokio::sync::Mutex<()>>,
        tokio::sync::OwnedMutexGuard<()>,
    ) {
        let task_lock = Arc::new(tokio::sync::Mutex::new(()));
        let guard = task_lock
            .clone()
            .try_lock_owned()
            .expect("new deferred inode gate lock should be uncontended");
        (task_lock, guard)
    }

    #[cfg(test)]
    fn try_register(
        &self,
        ino: Ino,
    ) -> Option<(DeferredInodeGateEntry, tokio::sync::OwnedMutexGuard<()>)> {
        self.try_register_or_update_occupied(ino, || {})
    }

    fn try_register_or_update_occupied<F>(
        &self,
        ino: Ino,
        on_occupied: F,
    ) -> Option<(DeferredInodeGateEntry, tokio::sync::OwnedMutexGuard<()>)>
    where
        F: FnOnce(),
    {
        use std::collections::hash_map::Entry;

        let mut entries = self.entries.lock();
        match entries.entry(ino) {
            Entry::Occupied(_) => {
                on_occupied();
                None
            }
            Entry::Vacant(entry) => {
                let (task_lock, guard) = Self::create_task_lock();
                entry.insert(Arc::clone(&task_lock));
                Some((
                    DeferredInodeGateEntry::new(self.clone(), ino, task_lock),
                    guard,
                ))
            }
        }
    }

    fn replace(&self, ino: Ino) -> (DeferredInodeGateEntry, tokio::sync::OwnedMutexGuard<()>) {
        let (task_lock, guard) = Self::create_task_lock();
        self.entries.lock().insert(ino, Arc::clone(&task_lock));
        (
            DeferredInodeGateEntry::new(self.clone(), ino, task_lock),
            guard,
        )
    }

    async fn wait(&self, ino: Ino) {
        loop {
            let task_lock = self.entries.lock().get(&ino).cloned();
            let Some(task_lock) = task_lock else {
                return;
            };
            let _guard = task_lock.lock().await;
            let current = self.entries.lock().get(&ino).cloned();
            if current
                .as_ref()
                .is_none_or(|current| Arc::ptr_eq(current, &task_lock))
            {
                return;
            }
        }
    }

    fn contains(&self, ino: Ino) -> bool {
        self.entries.lock().contains_key(&ino)
    }

    #[cfg(test)]
    fn replace_if_current(
        &self,
        current_entry: &DeferredInodeGateEntry,
    ) -> Option<(DeferredInodeGateEntry, tokio::sync::OwnedMutexGuard<()>)> {
        self.replace_if_current_or_remove(current_entry, || true)
    }

    fn replace_if_current_or_remove<F>(
        &self,
        current_entry: &DeferredInodeGateEntry,
        should_replace: F,
    ) -> Option<(DeferredInodeGateEntry, tokio::sync::OwnedMutexGuard<()>)>
    where
        F: FnOnce() -> bool,
    {
        use std::collections::hash_map::Entry;

        let mut entries = self.entries.lock();
        match entries.entry(current_entry.ino) {
            Entry::Occupied(mut entry) if Arc::ptr_eq(entry.get(), &current_entry.task_lock) => {
                if !should_replace() {
                    entry.remove();
                    return None;
                }
                let (task_lock, guard) = Self::create_task_lock();
                entry.insert(Arc::clone(&task_lock));
                Some((
                    DeferredInodeGateEntry::new(self.clone(), current_entry.ino, task_lock),
                    guard,
                ))
            }
            _ => None,
        }
    }

    fn finish(&self, ino: Ino, task_lock: &Arc<tokio::sync::Mutex<()>>) {
        use std::collections::hash_map::Entry;

        let mut entries = self.entries.lock();
        match entries.entry(ino) {
            Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), task_lock) => {
                entry.remove();
            }
            _ => {}
        }
    }
}

struct DeferredInodeGateEntry {
    gate: DeferredInodeGate,
    ino: Ino,
    task_lock: Arc<tokio::sync::Mutex<()>>,
}

impl DeferredInodeGateEntry {
    fn new(gate: DeferredInodeGate, ino: Ino, task_lock: Arc<tokio::sync::Mutex<()>>) -> Self {
        Self {
            gate,
            ino,
            task_lock,
        }
    }
}

impl Drop for DeferredInodeGateEntry {
    fn drop(&mut self) {
        self.gate.finish(self.ino, &self.task_lock);
    }
}

#[derive(Clone)]
struct JanitorLease {
    op: Operator,
    lock_path: String,
    owner_id: String,
}

fn normalize_data_prefix(format_info: &FormatInfo) -> String {
    if format_info.data_prefix.trim().is_empty() {
        format!("{}/objects/", format_info.name)
    } else if format_info.data_prefix.ends_with('/') {
        format_info.data_prefix.clone()
    } else {
        format!("{}/", format_info.data_prefix)
    }
}

fn validate_volume_block_size(block_size: u32) -> VfsResult<()> {
    if block_size == 0 {
        return Err(VfsError::Other(
            "volume block_size 0 is invalid; block_size must be non-zero".to_string(),
        ));
    }
    let cloud_logical_block_capacity = CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT;
    if (block_size as usize) > cloud_logical_block_capacity {
        return Err(VfsError::Other(format!(
            "volume block_size {} exceeds 4x4 cloud fragment capacity {} ({} fragments x {} bytes)",
            block_size, cloud_logical_block_capacity, CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE
        )));
    }
    Ok(())
}

fn hard_backpressure_threshold_bytes(max_dirty_bytes: usize) -> usize {
    max_dirty_bytes.saturating_mul(HARD_BACKPRESSURE_DIRTY_MULTIPLIER)
}

fn aggressive_materialization_threshold_bytes(max_dirty_bytes: usize) -> usize {
    max_dirty_bytes
        .saturating_div(AGGRESSIVE_MATERIALIZATION_DIRTY_DIVISOR)
        .max(1)
}

#[cfg(test)]
fn drain_lag_pacing_attribution_ms(
    waited_ms: u64,
    requested_delay_ms: u64,
    drain_lag_delay_ms: Option<u64>,
) -> Option<u64> {
    let drain_lag_delay_ms = drain_lag_delay_ms?;
    if requested_delay_ms == 0 {
        return Some(0);
    }
    let numerator = u128::from(waited_ms) * u128::from(drain_lag_delay_ms);
    let denominator = u128::from(requested_delay_ms);
    let attributed_ms = numerator.div_ceil(denominator);
    Some(attributed_ms.min(u128::from(u64::MAX)) as u64)
}

fn warn_small_block_addressability(block_size: u32) {
    let block_size_nonzero = block_size.max(1) as u128;
    const MIN_PRACTICAL_ADDRESSABLE_BYTES: u128 = 64u128 * 1024 * 1024 * 1024 * 1024;
    let max_addressable_bytes = (u32::MAX as u128).saturating_mul(block_size_nonzero);
    debug_assert!(
        max_addressable_bytes >= MIN_PRACTICAL_ADDRESSABLE_BYTES,
        "configured block_size={} yields only {} addressable bytes with u32 block indices",
        block_size_nonzero,
        max_addressable_bytes
    );
    if max_addressable_bytes < MIN_PRACTICAL_ADDRESSABLE_BYTES {
        warn!(
            block_size,
            max_addressable_bytes,
            min_recommended = MIN_PRACTICAL_ADDRESSABLE_BYTES,
            "small block_size reduces max file addressability with u32 block indices"
        );
    }
}

fn derive_buffer_blocks_default(read_buffer_mb: u64, block_size: u32) -> u32 {
    let bytes = (read_buffer_mb as u128) * 1024 * 1024;
    let blocks = (bytes / block_size.max(1) as u128)
        .max(1)
        .min(u32::MAX as u128);
    blocks as u32
}

fn derive_readahead_max_width(
    read_buffer_mb: u64,
    block_size: u32,
    readahead_base_width: u32,
) -> u32 {
    derive_buffer_blocks_default(read_buffer_mb, block_size).max(readahead_base_width.max(1))
}

fn build_fetch_aimd_config(
    readahead_max_width: u32,
    config: &VfsConfig,
) -> crate::aimd::AimdConfig {
    let fetch_max_window_default = (readahead_max_width.max(1) as f64).clamp(1.0, 4096.0);
    let fetch_initial_window_default = (fetch_max_window_default / 4.0).clamp(1.0, 64.0);
    let fetch_increase_default = if fetch_max_window_default >= 64.0 {
        2.0
    } else {
        1.0
    };
    crate::aimd::AimdConfig {
        initial_window: if config.fetch_initial_window > 0.0 {
            config.fetch_initial_window
        } else {
            fetch_initial_window_default
        },
        min_window: config.fetch_min_window,
        max_window: if config.fetch_max_window > 0.0 {
            config.fetch_max_window
        } else {
            fetch_max_window_default
        },
        increase_step: if config.fetch_increase_step > 0.0 {
            config.fetch_increase_step
        } else {
            fetch_increase_default
        },
        decrease_factor: if config.fetch_decrease_factor > 0.0 {
            config.fetch_decrease_factor
        } else {
            0.85
        },
        decrease_cooldown: Duration::from_secs(config.fetch_decrease_cooldown_secs),
        idle_reset_after: nonzero_duration_secs(config.fetch_idle_reset_secs),
    }
}

struct ZeroIoDrilldownStatusInput<'a> {
    lifecycle: &'a studiofs_meta::schema::InodeLifecycleRecord,
    pending_upload_count: usize,
    publish_ready_queued: bool,
    replayed_after_restart: bool,
    repeated_replay_count: u64,
    deferred_cleanup_object_count: usize,
    sticky_error: Option<&'a str>,
}

#[derive(Default)]
struct ZeroIoObjectView {
    object_ids: Vec<u64>,
    live_extent_count: usize,
}

fn build_upload_aimd_config(config: &VfsConfig) -> crate::aimd::AimdConfig {
    crate::aimd::AimdConfig {
        initial_window: if config.upload_initial_window > 0.0 {
            config.upload_initial_window
        } else {
            16.0
        },
        min_window: if config.upload_min_window > 0.0 {
            config.upload_min_window
        } else {
            1.0
        },
        max_window: if config.upload_max_window > 0.0 {
            config.upload_max_window
        } else {
            512.0
        },
        increase_step: if config.upload_increase_step > 0.0 {
            config.upload_increase_step
        } else {
            2.0
        },
        decrease_factor: if config.upload_decrease_factor > 0.0 {
            config.upload_decrease_factor
        } else {
            0.85
        },
        decrease_cooldown: Duration::from_secs(config.upload_decrease_cooldown_secs),
        idle_reset_after: nonzero_duration_secs(config.upload_idle_reset_secs),
    }
}

fn nonzero_duration_secs(secs: u64) -> Option<Duration> {
    (secs > 0).then_some(Duration::from_secs(secs))
}

fn panic_payload_message(panic_payload: &(dyn Any + Send)) -> Cow<'_, str> {
    if let Some(msg) = panic_payload.downcast_ref::<&str>() {
        Cow::Borrowed(*msg)
    } else if let Some(msg) = panic_payload.downcast_ref::<String>() {
        Cow::Borrowed(msg.as_str())
    } else {
        Cow::Borrowed("unknown panic payload")
    }
}

// =============================================================================
// StudioVfs Implementation
// =============================================================================

/// The core VFS - satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    /// Preferred metadata execution boundary for hot-path call sites.
    meta: MetaExecutor<dyn VfsMeta>,
    /// Data store for writes (uploads, deletes, GC).
    data_write: Arc<DataStore>,
    /// Shared local payload backend used by the foreground read path.
    local_payload_backend: Arc<dyn LocalPayloadBackend>,
    /// Shared cached-store runtime that owns local staging + pending uploads.
    cached_store: Arc<CachedStore>,
    /// Block size in bytes.
    block_size: u32,
    /// Volume data prefix for S3 object keys.
    data_prefix: String,
    /// Internal state (mutable).
    state: Arc<VfsState>,
    /// Principal used for ACL evaluation in this mounted VFS instance.
    acl_principal: String,
    /// Max number of entries in effective ACL cache.
    effective_acl_cache_capacity: usize,
    // --- Upload Control ---
    /// Adaptive upload scheduler (AIMD + retries).
    upload_scheduler: Arc<crate::upload::UploadScheduler>,
    upload_delay: Duration,
    upload_requeue_base: Duration,
    upload_requeue_max: Duration,
    upload_stats_interval: Duration,
    shutdown_metadata_flush_timeout: Duration,
    /// Shared local-I/O budget for upload path reads from cache.
    upload_cache_io_slots: Arc<Semaphore>,
    upload_cache_io_slots_max: u32,
    /// Global memory-credit budget for active in-flight I/O buffers.
    transit_memory_budget: Arc<TransitMemoryBudget>,
    qos: Arc<QosController>,
    /// Baseline slot cost per upload op while foreground reads are active.
    upload_cache_io_foreground_cost_base: u32,
    /// Cost increase step applied when foreground reads degrade under upload.
    upload_cache_io_foreground_cost_step: u32,
    /// Foreground read latency target (P90) used by the upload throttle loop.
    upload_fg_read_latency_target_ns: u64,
    /// Sliding window for foreground speedometer samples.
    upload_fg_read_latency_window: Duration,
    /// Sticky horizon for using last known foreground P90 in the throttle loop.
    upload_fg_read_latency_sticky_window: Duration,
    /// Handle-aware access tracking tuning derived from VFS read settings.
    access_tracker: AccessTrackerConfig,
    /// Grace window after the last foreground read to keep user priority.
    upload_user_idle_grace: Duration,
    /// Upload worker pool size.
    upload_worker_count: usize,
    disable_background_slab_flush: bool,
    publish_visibility_timeout: Duration,
    /// Sequential read-ahead tracker.
    readahead: Arc<ReadAheadTracker>,
    /// Directory-level prefetch tracker.
    dir_prefetch: Arc<DirAccessTracker>,
    /// Channel sender for background prefetch planning and execution.
    /// The receiver lives in a spawned task that has an `Arc<Self>`.
    prefetch_tx: tokio::sync::mpsc::UnboundedSender<PrefetchWorkItem>,
    /// One-time startup state used by `start()` to spawn background workers.
    startup: StartupState,
    /// Maximum total bytes allowed in the dirty-write memory buffer before
    /// `flush()` / `write()` apply backpressure.
    max_dirty_bytes: AtomicUsize,
    /// Minimum dirty full-block batch size before `write()` pays the local
    /// persist boundary for eager block flushes.
    write_eager_flush_batch_bytes: usize,
    /// Reuses hot-path block buffers to reduce allocator churn on repeated
    /// materialize/stage cycles.
    write_buffer_pool: BufferPool,
    /// Maximum bytes drained by one opportunistic eager-flush attempt.
    write_eager_flush_drain_chunk_bytes: usize,
    /// Bounded local payload-put concurrency for one eager drain.
    payload_put_concurrency: usize,
    /// Fast-mode group-commit minimum bytes before forcing local sync.
    local_sync_group_min_bytes: u64,
    /// Maximum bytes allowed for a single xattr value.
    max_xattr_bytes: usize,
    zeroio_runtime_state_dir: PathBuf,
    landing_zone: LandingZone,
    /// Handle table: tracks all open file descriptors.
    handle_table: HandleTable,
    /// Optional distributed lease for janitor leader election.
    janitor_lease: Option<JanitorLease>,
    janitor_interval: Duration,
    janitor_skip_newer_than: Duration,
    janitor_max_deletes_per_run: u64,
    janitor_lease_ttl: Duration,
    prefetch_concurrency: usize,
    dir_prefetch_max_batch: usize,
    readdir_attr_batch_chunk: usize,
    readdir_attr_batch_max_inodes: usize,
    prefetch_pause_on_neterr: Duration,
    /// Adaptive fetch scheduler (AIMD + singleflight).
    /// Replaces the old `FetchThrottle`, `inflight_fetches`, and VFS-retry.
    fetch_scheduler: Arc<crate::fetch::FetchScheduler>,
    /// Optional metadata-backed hard-lock coordination for cross-client flock.
    distributed_flock: Option<DistributedFlock>,
    /// Optional kernel cache invalidator (set by platform bridge after mount).
    kernel_invalidator: RwLock<Option<Arc<dyn KernelInvalidator>>>,
    write_path_backend_tag: String,
    write_path_started_at: Instant,
    runtime_stats_file_time_nanos: i64,
    known_file_lock_overrides: Mutex<HashMap<Ino, FileLockLease>>,
    #[cfg(test)]
    metadata_authority_override: Mutex<Option<MetadataAuthorityStatus>>,
    #[cfg(test)]
    fail_next_inline_dirty_attr_persist: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_size_change_setattr_before_flush: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    fail_next_size_change_truncate_after_first_removed_block: std::sync::atomic::AtomicBool,
}

fn map_cache_backend_detection_error(cache_dir: &str, source: std::io::Error) -> VfsError {
    VfsError::LocalPayloadRuntimeDetection {
        cache_root: cache_dir.to_string(),
        source,
    }
}

fn map_cache_backend_provision_error(cache_root: &str, source: std::io::Error) -> VfsError {
    VfsError::Other(format!(
        "failed to provision local payload runtime layout under '{cache_root}': {source}"
    ))
}

fn map_open_local_payload_backend_error(
    cache_dir: &str,
    source: OpenLocalPayloadBackendError,
) -> VfsError {
    match source {
        OpenLocalPayloadBackendError::DetectCacheBackend { source, .. } => {
            map_cache_backend_detection_error(cache_dir, source)
        }
        OpenLocalPayloadBackendError::ProvisionLocalPayloadLayout { cache_root, source } => {
            map_cache_backend_provision_error(&cache_root, source)
        }
        OpenLocalPayloadBackendError::OpenLocalPayloadBackend { source, .. } => VfsError::Other(
            format!("failed to open local payload runtime backend: {source}"),
        ),
    }
}

impl StudioVfs {
    // #region debug log
    fn debug_write_probe(
        &self,
        hypothesis_id: &'static str,
        location: &'static str,
        message: &'static str,
        data: serde_json::Value,
    ) {
        const DEBUG_SESSION_ID: &str = "5ee5eb";
        const DEBUG_LOG_PATH: &str =
            r"C:\Users\valen\AppData\Local\Temp\debug-agent\debug-5ee5eb.log";
        if std::env::var_os("STUDIOFS_WRITE_DEBUG").is_none() {
            return;
        }
        let payload = serde_json::json!({
            "sessionId": DEBUG_SESSION_ID,
            "runId": std::env::var("STUDIOFS_DEBUG_RUN_ID").unwrap_or_else(|_| "run1".to_string()),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": now_unix_millis(),
        });
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(DEBUG_LOG_PATH)
        {
            let _ = writeln!(file, "{payload}");
        }
    }

    fn write_debug_enabled(&self) -> bool {
        static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        *ENABLED.get_or_init(|| std::env::var_os("STUDIOFS_WRITE_DEBUG").is_some())
    }
    // #endregion debug log

    fn aggressive_materialization_should_run(&self) -> bool {
        if self.disable_background_slab_flush {
            return false;
        }
        let total_dirty_bytes = self.state.writer_stack.total_dirty_bytes();
        if total_dirty_bytes == 0 {
            return false;
        }
        total_dirty_bytes >= aggressive_materialization_threshold_bytes(self.max_dirty_bytes())
    }

    fn clone_into_write_buffer(&self, data: &[u8]) -> BytesMut {
        self.write_buffer_pool.acquire_with_bytes(data)
    }

    fn recycle_write_buffer(&self, buffer: BytesMut) {
        self.write_buffer_pool.release(buffer);
    }

    fn pending_upload_count(&self) -> usize {
        self.cached_store.pending_upload_count()
    }

    /// Snapshot all pending uploads currently tracked by the cached store.
    pub(crate) fn pending_uploads_snapshot(&self) -> Vec<PendingUpload> {
        self.cached_store.snapshot_pending_uploads()
    }

    fn attr_read_cache_snapshot(metrics: AttrReadCacheMetrics) -> VfsAttrReadCacheSnapshot {
        VfsAttrReadCacheSnapshot {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            evictions: metrics.evictions,
            hot_hit_ratio: metrics.hot_hit_ratio(),
            occupancy_entries: metrics.occupancy_entries,
            occupancy_bytes: metrics.occupancy_bytes,
            protected_occupancy_entries: metrics.protected_occupancy_entries,
            protected_occupancy_bytes: metrics.protected_occupancy_bytes,
            entry_budget: metrics.entry_budget,
            byte_budget: metrics.byte_budget,
        }
    }

    fn attr_read_cache_metrics(&self) -> AttrReadCacheMetrics {
        self.state.namespace.attr_read_cache.lock().metrics()
    }

    /// Classify upload pipeline activity into optional diagnostics event labels.
    pub(crate) fn upload_pipeline_runtime_event(
        activity_started: bool,
        became_idle: bool,
        is_active: bool,
    ) -> Option<(&'static str, &'static str)> {
        if activity_started {
            Some(("became_active", "upload pipeline active"))
        } else if became_idle {
            Some(("became_idle", "upload pipeline idle"))
        } else if is_active {
            Some(("sample", "upload pipeline sample"))
        } else {
            None
        }
    }

    /// Snapshot cache runtime state for diagnostics and API consumers.
    pub fn cache_state_snapshot(&self) -> VfsCacheStateSnapshot {
        let dentry = self.dentry_cache_metrics();
        let path_shortcut = self.path_shortcut_cache_metrics();
        let transit = self.transit_memory_budget.snapshot();

        VfsCacheStateSnapshot {
            dentry_cache: dentry.into(),
            path_shortcut_cache: path_shortcut.into(),
            attr_read_cache: Self::attr_read_cache_snapshot(self.attr_read_cache_metrics()),
            local_backend_dirty_objects: self.local_payload_backend.dirty_object_count(),
            prefetch_queue_depth: self
                .state
                .metrics
                .prefetch_queue_depth
                .load(Ordering::Relaxed),
            transit_memory: VfsTransitMemorySnapshot {
                budget_bytes: transit.budget_bytes,
                used_bytes: transit.used_bytes,
                available_bytes: transit.available_bytes,
                waiting_foreground: transit.waiting_foreground,
                waiting_background: transit.waiting_background,
            },
        }
    }

    /// Snapshot metadata replication and session synchronization state.
    pub fn synchronization_state_snapshot(&self) -> VfsSynchronizationStateSnapshot {
        let metadata_backlog = self.sync_meta().replication_backlog();
        VfsSynchronizationStateSnapshot {
            file_sessions: self.file_session_stats(),
            foreground_reads: self.foreground_read_protection_stats(),
            pending_metadata_ops: metadata_backlog.pending_ops,
            inflight_metadata_ops: metadata_backlog.inflight_ops,
        }
    }

    fn zeroio_event_u64(fields: &serde_json::Value, key: &str) -> Option<u64> {
        fields
            .get(key)
            .and_then(|value| value.as_u64())
            .or_else(|| {
                fields
                    .get(key)
                    .and_then(|value| value.as_i64())
                    .and_then(|value| u64::try_from(value).ok())
            })
    }

    fn zeroio_event_bool(fields: &serde_json::Value, key: &str) -> Option<bool> {
        fields.get(key).and_then(|value| value.as_bool())
    }

    fn zeroio_event_string(fields: &serde_json::Value, key: &str) -> Option<String> {
        fields
            .get(key)
            .and_then(|value| value.as_str())
            .map(str::to_string)
    }

    fn zeroio_last_shutdown_snapshot_path(&self) -> PathBuf {
        self.zeroio_runtime_state_dir
            .join(ZEROIO_LAST_SHUTDOWN_SNAPSHOT_FILE)
    }

    fn load_persisted_zeroio_shutdown_snapshot(&self) -> Option<VfsZeroIoShutdownSnapshot> {
        let path = self.zeroio_last_shutdown_snapshot_path();
        let bytes = std::fs::read(&path).ok()?;
        match serde_json::from_slice(&bytes) {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to parse persisted zeroio shutdown snapshot"
                );
                None
            }
        }
    }

    fn persist_zeroio_shutdown_snapshot(&self, snapshot: &VfsZeroIoShutdownSnapshot) {
        let path = self.zeroio_last_shutdown_snapshot_path();
        let Some(parent) = path.parent() else {
            return;
        };
        if let Err(error) = std::fs::create_dir_all(parent) {
            warn!(
                path = %parent.display(),
                error = %error,
                "failed to create zeroio shutdown snapshot directory"
            );
            return;
        }
        let bytes = match serde_json::to_vec(snapshot) {
            Ok(bytes) => bytes,
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to serialize zeroio shutdown snapshot"
                );
                return;
            }
        };
        let mut temp_file = match NamedTempFile::new_in(parent) {
            Ok(temp_file) => temp_file,
            Err(error) => {
                warn!(
                    path = %path.display(),
                    error = %error,
                    "failed to create temporary zeroio shutdown snapshot file"
                );
                return;
            }
        };
        if let Err(error) = temp_file.write_all(&bytes) {
            warn!(
                path = %path.display(),
                error = %error,
                "failed to write temporary zeroio shutdown snapshot file"
            );
            return;
        }
        if let Err(error) = temp_file.as_file().sync_all() {
            warn!(
                path = %path.display(),
                error = %error,
                "failed to sync temporary zeroio shutdown snapshot file"
            );
            return;
        }
        if let Err(error) = temp_file.persist(&path).map_err(|error| error.error) {
            warn!(
                path = %path.display(),
                error = %error,
                "failed to persist zeroio shutdown snapshot"
            );
        }
    }

    fn zeroio_retention_priority(retention: PayloadRetentionClass) -> u8 {
        match retention {
            PayloadRetentionClass::PublishedWarm => 0,
            PayloadRetentionClass::PinnedUnpublished => 1,
            PayloadRetentionClass::PublishReadyRetained => 2,
            PayloadRetentionClass::DeletedBeforePublish => 3,
        }
    }

    fn zeroio_status_priority(status: &str) -> u8 {
        match status {
            "stuck_upload_error" => 7,
            "repeated_replay_after_restart" => 6,
            "publish_starved_after_replay" => 5,
            "awaiting_deferred_cleanup" => 4,
            "uploading_without_queue" => 3,
            "publish_ready_queued" => 2,
            "publish_ready_waiting" | "replayed_waiting_for_upload" | "uploading" => 1,
            _ => 0,
        }
    }

    fn zeroio_object_view_for_lifecycle(
        &self,
        ino: Ino,
        lifecycle: &studiofs_meta::schema::InodeLifecycleRecord,
    ) -> ZeroIoObjectView {
        let mut object_ids = BTreeSet::new();

        if lifecycle.state == InodeLifecycleState::DeletedBeforePublish {
            object_ids.extend(lifecycle.deferred_cleanup_object_ids.iter().copied());
            return ZeroIoObjectView {
                object_ids: object_ids.into_iter().collect(),
                live_extent_count: 0,
            };
        }

        if lifecycle.state == InodeLifecycleState::Published {
            return ZeroIoObjectView::default();
        }

        let meta = self.sync_meta();
        let mut live_extent_count = 0;
        for (_, object_id, content_epoch) in self.live_extent_records_for_inode(ino) {
            if content_epoch == lifecycle.local_content_epoch {
                live_extent_count += 1;
                object_ids.insert(object_id);
            }
        }
        if !object_ids.is_empty() {
            return ZeroIoObjectView {
                object_ids: object_ids.into_iter().collect(),
                live_extent_count,
            };
        }

        if meta.has_local_regular_file_overlay(ino).unwrap_or(false) {
            if let Ok(ids) = meta.get_regular_file_overlay_object_ids(ino) {
                object_ids.extend(ids);
            }
            if !object_ids.is_empty() {
                return ZeroIoObjectView {
                    object_ids: object_ids.into_iter().collect(),
                    live_extent_count,
                };
            }
        }

        if let Ok(usage) = meta.get_regular_file_graph_object_usage(ino) {
            object_ids.extend(usage.into_iter().map(|(object_id, _)| object_id));
        }

        ZeroIoObjectView {
            object_ids: object_ids.into_iter().collect(),
            live_extent_count,
        }
    }

    fn zeroio_object_bytes(&self, object_ids: &[u64]) -> u64 {
        object_ids
            .iter()
            .filter_map(|object_id| self.local_payload_backend.object_stats(*object_id))
            .map(|stats| stats.payload_len)
            .sum()
    }

    fn zeroio_drilldown_status(input: ZeroIoDrilldownStatusInput<'_>) -> &'static str {
        if input.sticky_error.is_some() {
            "stuck_upload_error"
        } else if input.repeated_replay_count > 1 {
            "repeated_replay_after_restart"
        } else if input.lifecycle.state == InodeLifecycleState::PublishReady
            && input.replayed_after_restart
        {
            "publish_starved_after_replay"
        } else if input.deferred_cleanup_object_count > 0
            || input.lifecycle.state == InodeLifecycleState::DeletedBeforePublish
        {
            "awaiting_deferred_cleanup"
        } else if input.lifecycle.state == InodeLifecycleState::Uploading
            && input.pending_upload_count == 0
        {
            "uploading_without_queue"
        } else if input.publish_ready_queued {
            "publish_ready_queued"
        } else if input.lifecycle.state == InodeLifecycleState::PublishReady {
            "publish_ready_waiting"
        } else if input.replayed_after_restart {
            "replayed_waiting_for_upload"
        } else if input.lifecycle.state == InodeLifecycleState::Uploading {
            "uploading"
        } else {
            "healthy"
        }
    }

    /// Snapshot zero-IO startup/runtime state for diagnostics.
    pub fn zeroio_runtime_snapshot(&self) -> VfsZeroIoRuntimeSnapshot {
        let generated_unix_ms = now_unix_millis();
        let storage_stats = self.local_payload_backend.storage_stats();
        let backend_non_evictable_bytes = storage_stats
            .current_bytes
            .saturating_sub(storage_stats.evictable_bytes);
        let pending_uploads = self.pending_uploads_snapshot();
        let mut pending_uploads_by_inode: HashMap<Ino, Vec<PendingUpload>> = HashMap::new();
        for op in pending_uploads {
            pending_uploads_by_inode.entry(op.ino).or_default().push(op);
        }

        let pending_epoch_finalizations =
            self.state.upload.pending_epoch_finalizations.lock().clone();
        let sticky_errors = self.state.upload.upload_sticky_errors.lock().clone();
        let replay_counts = self.state.upload.startup_replay_counts.lock().clone();

        let mut startup_replay = None;
        let mut startup_replay_ts = 0;
        let mut last_graceful_shutdown = None;
        let mut shutdown_ts = 0;
        for event in self.recent_runtime_events(VFS_RUNTIME_EVENT_CAPACITY) {
            if event.subsystem != "zeroio" {
                continue;
            }
            match event.kind.as_str() {
                "startup_replay_summary" if event.generated_unix_ms >= startup_replay_ts => {
                    startup_replay_ts = event.generated_unix_ms;
                    startup_replay = Some(VfsZeroIoStartupReplaySnapshot {
                        generated_unix_ms: event.generated_unix_ms,
                        replayed_epochs: Self::zeroio_event_u64(&event.fields, "replayed_epochs")
                            .unwrap_or(0),
                        replayed_blocks: Self::zeroio_event_u64(&event.fields, "replayed_blocks")
                            .unwrap_or(0),
                        replayed_bytes: Self::zeroio_event_u64(&event.fields, "replayed_bytes")
                            .unwrap_or(0),
                        queued_uploads: Self::zeroio_event_u64(&event.fields, "queued_uploads")
                            .unwrap_or(0),
                        queued_publish_ready_epochs: Self::zeroio_event_u64(
                            &event.fields,
                            "queued_publish_ready_epochs",
                        )
                        .unwrap_or(0),
                        quarantined_inodes: Self::zeroio_event_u64(
                            &event.fields,
                            "quarantined_inodes",
                        )
                        .unwrap_or(0),
                        orphan_dirty_payloads_removed: Self::zeroio_event_u64(
                            &event.fields,
                            "orphan_dirty_payloads_removed",
                        )
                        .unwrap_or(0),
                        unreferenced_local_payloads_removed: Self::zeroio_event_u64(
                            &event.fields,
                            "unreferenced_local_payloads_removed",
                        )
                        .unwrap_or(0),
                        discarded_memory_only_bytes: Self::zeroio_event_u64(
                            &event.fields,
                            "discarded_memory_only_bytes",
                        )
                        .unwrap_or(0),
                        replay_failures: Self::zeroio_event_u64(&event.fields, "replay_failures")
                            .unwrap_or(0),
                    });
                }
                "shutdown_local_state" if event.generated_unix_ms >= shutdown_ts => {
                    shutdown_ts = event.generated_unix_ms;
                    last_graceful_shutdown = Some(VfsZeroIoShutdownSnapshot {
                        generated_unix_ms: event.generated_unix_ms,
                        accepted_local_dirty_bytes_before: Self::zeroio_event_u64(
                            &event.fields,
                            "accepted_local_dirty_bytes_before",
                        )
                        .unwrap_or(0),
                        accepted_local_dirty_bytes_after: Self::zeroio_event_u64(
                            &event.fields,
                            "accepted_local_dirty_bytes_after",
                        )
                        .unwrap_or(0),
                        pending_uploads_after: Self::zeroio_event_u64(
                            &event.fields,
                            "pending_uploads_after",
                        )
                        .and_then(|value| usize::try_from(value).ok())
                        .unwrap_or(0),
                        uploader_drained_before_cancel: Self::zeroio_event_bool(
                            &event.fields,
                            "uploader_drained_before_cancel",
                        )
                        .unwrap_or(false),
                        metadata_flush_outcome: Self::zeroio_event_string(
                            &event.fields,
                            "metadata_flush_outcome",
                        )
                        .unwrap_or_else(|| "unknown".to_string()),
                        local_payload_sync_ok: Self::zeroio_event_bool(
                            &event.fields,
                            "local_payload_sync_ok",
                        )
                        .unwrap_or(false),
                        recoverable_on_restart: Self::zeroio_event_bool(
                            &event.fields,
                            "recoverable_on_restart",
                        )
                        .unwrap_or(false),
                    });
                }
                _ => {}
            }
        }

        let replay_failures = self
            .state
            .metrics
            .zeroio_replay_failures
            .load(Ordering::Relaxed);
        let publish_failures = self
            .state
            .metrics
            .zeroio_publish_failures
            .load(Ordering::Relaxed);
        let persisted_shutdown = self.load_persisted_zeroio_shutdown_snapshot();
        if let Some(snapshot) = persisted_shutdown
            && last_graceful_shutdown
                .as_ref()
                .map(|current| snapshot.generated_unix_ms > current.generated_unix_ms)
                .unwrap_or(true)
        {
            last_graceful_shutdown = Some(snapshot);
        }

        let mut snapshot = VfsZeroIoRuntimeSnapshot {
            generated_unix_ms,
            replay_failures,
            publish_failures,
            cache_bytes: VfsZeroIoCacheBytesSnapshot {
                total_local_bytes: storage_stats.current_bytes,
                backend_evictable_bytes: storage_stats.evictable_bytes,
                backend_non_evictable_bytes,
                ..VfsZeroIoCacheBytesSnapshot::default()
            },
            startup_replay,
            last_graceful_shutdown,
            ..VfsZeroIoRuntimeSnapshot::default()
        };

        let meta = self.sync_meta();
        let lifecycles = match meta.list_inode_lifecycles() {
            Ok(lifecycles) => lifecycles,
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to enumerate inode lifecycle records for zeroio snapshot"
                );
                return snapshot;
            }
        };

        let mut object_retentions: HashMap<u64, PayloadRetentionClass> = HashMap::new();
        let mut health_signals = Vec::new();
        let mut drilldown = Vec::new();

        for lifecycle in lifecycles {
            let ino = lifecycle.ino;
            let object_view = self.zeroio_object_view_for_lifecycle(ino, &lifecycle);
            let lifecycle_bytes = self.zeroio_object_bytes(&object_view.object_ids);
            for object_id in &object_view.object_ids {
                let next = lifecycle.retention;
                object_retentions
                    .entry(*object_id)
                    .and_modify(|current| {
                        if Self::zeroio_retention_priority(next)
                            > Self::zeroio_retention_priority(*current)
                        {
                            *current = next;
                        }
                    })
                    .or_insert(next);
            }

            let pending_uploads = pending_uploads_by_inode.get(&ino);
            let pending_upload_count = pending_uploads.map(|uploads| uploads.len()).unwrap_or(0);
            let pending_upload_bytes = pending_uploads
                .map(|uploads| {
                    uploads
                        .iter()
                        .filter_map(|op| self.local_payload_backend.object_stats(op.object_id))
                        .map(|stats| stats.payload_len)
                        .sum()
                })
                .unwrap_or(0);
            let oldest_pending_upload_age_ms = pending_uploads
                .and_then(|uploads| {
                    uploads
                        .iter()
                        .map(|op| generated_unix_ms.saturating_sub(op.created_unix_ms))
                        .max()
                })
                .unwrap_or(0);
            let publish_ready_queued =
                pending_epoch_finalizations.contains(&(ino, lifecycle.local_content_epoch));
            let repeated_replay_count = replay_counts
                .get(&(ino, lifecycle.local_content_epoch))
                .copied()
                .unwrap_or(0);
            let replayed_after_restart = repeated_replay_count > 0;
            if replayed_after_restart {
                snapshot.replayed_after_restart_epochs =
                    snapshot.replayed_after_restart_epochs.saturating_add(1);
            }
            let deferred_cleanup_object_count = lifecycle.deferred_cleanup_object_ids.len();
            let deferred_cleanup_bytes =
                self.zeroio_object_bytes(&lifecycle.deferred_cleanup_object_ids);
            let sticky_error = sticky_errors.get(&ino).cloned();
            let status = Self::zeroio_drilldown_status(ZeroIoDrilldownStatusInput {
                lifecycle: &lifecycle,
                pending_upload_count,
                publish_ready_queued,
                replayed_after_restart,
                repeated_replay_count,
                deferred_cleanup_object_count,
                sticky_error: sticky_error.as_deref(),
            })
            .to_string();
            let live_extent_count = object_view.live_extent_count;

            match lifecycle.state {
                InodeLifecycleState::LocalOnly
                | InodeLifecycleState::Uploading
                | InodeLifecycleState::PublishReady => {
                    snapshot.unpublished_files = snapshot.unpublished_files.saturating_add(1);
                    snapshot.unpublished_epochs = snapshot.unpublished_epochs.saturating_add(1);
                    snapshot.unpublished_payload_bytes = snapshot
                        .unpublished_payload_bytes
                        .saturating_add(lifecycle_bytes);
                }
                InodeLifecycleState::Published | InodeLifecycleState::DeletedBeforePublish => {}
            }

            if lifecycle.state == InodeLifecycleState::Uploading {
                snapshot.uploading_epochs = snapshot.uploading_epochs.saturating_add(1);
            }
            if lifecycle.state == InodeLifecycleState::PublishReady {
                snapshot.publish_ready_backlog_epochs =
                    snapshot.publish_ready_backlog_epochs.saturating_add(1);
            }
            if lifecycle.state == InodeLifecycleState::DeletedBeforePublish {
                snapshot.deferred_delete_backlog_epochs =
                    snapshot.deferred_delete_backlog_epochs.saturating_add(1);
                snapshot.deferred_delete_backlog_objects = snapshot
                    .deferred_delete_backlog_objects
                    .saturating_add(deferred_cleanup_object_count);
                snapshot.deferred_delete_backlog_bytes = snapshot
                    .deferred_delete_backlog_bytes
                    .saturating_add(deferred_cleanup_bytes);
            }
            if replayed_after_restart
                && lifecycle.state != InodeLifecycleState::Published
                && lifecycle.state != InodeLifecycleState::DeletedBeforePublish
            {
                snapshot.replay_backlog_epochs = snapshot.replay_backlog_epochs.saturating_add(1);
                snapshot.replay_backlog_bytes = snapshot
                    .replay_backlog_bytes
                    .saturating_add(lifecycle_bytes);
            }

            if let Some(error) = sticky_error.clone() {
                health_signals.push(VfsZeroIoHealthSignal {
                    severity: "error".to_string(),
                    code: "upload_failure".to_string(),
                    summary: "sticky upload error is blocking unpublished progress".to_string(),
                    ino: Some(ino),
                    content_epoch: Some(lifecycle.local_content_epoch),
                    path: None,
                });
                let _ = error;
            } else if repeated_replay_count > 1 {
                health_signals.push(VfsZeroIoHealthSignal {
                    severity: "warn".to_string(),
                    code: "repeated_replay".to_string(),
                    summary: format!(
                        "epoch replayed after restart {} times without completing publish",
                        repeated_replay_count
                    ),
                    ino: Some(ino),
                    content_epoch: Some(lifecycle.local_content_epoch),
                    path: None,
                });
            } else if lifecycle.state == InodeLifecycleState::PublishReady && replayed_after_restart
            {
                health_signals.push(VfsZeroIoHealthSignal {
                    severity: "warn".to_string(),
                    code: "publish_starvation".to_string(),
                    summary: "epoch replayed after restart but is still waiting to publish"
                        .to_string(),
                    ino: Some(ino),
                    content_epoch: Some(lifecycle.local_content_epoch),
                    path: None,
                });
            } else if lifecycle.state == InodeLifecycleState::Uploading && pending_upload_count == 0
            {
                health_signals.push(VfsZeroIoHealthSignal {
                    severity: "warn".to_string(),
                    code: "upload_truth_missing".to_string(),
                    summary: "inode is marked uploading but the runtime upload queue is empty"
                        .to_string(),
                    ino: Some(ino),
                    content_epoch: Some(lifecycle.local_content_epoch),
                    path: None,
                });
            } else if lifecycle.state == InodeLifecycleState::DeletedBeforePublish
                && deferred_cleanup_object_count > 0
            {
                health_signals.push(VfsZeroIoHealthSignal {
                    severity: "info".to_string(),
                    code: "deferred_delete_backlog".to_string(),
                    summary: "deleted-before-publish payload is awaiting deferred cleanup"
                        .to_string(),
                    ino: Some(ino),
                    content_epoch: Some(lifecycle.local_content_epoch),
                    path: None,
                });
            }

            if lifecycle.state != InodeLifecycleState::Published {
                drilldown.push(VfsZeroIoDrilldownEntry {
                    ino,
                    path: None,
                    lifecycle_state: lifecycle.state,
                    retention: lifecycle.retention,
                    local_content_epoch: lifecycle.local_content_epoch,
                    published_content_epoch: lifecycle.published_content_epoch,
                    local_visible_size: lifecycle.local_visible_size,
                    published_size: lifecycle.published_size,
                    live_extent_count,
                    pending_upload_count,
                    pending_upload_bytes,
                    oldest_pending_upload_age_ms,
                    publish_ready_queued,
                    replayed_after_restart,
                    repeated_replay_count,
                    deferred_cleanup_object_count,
                    deferred_cleanup_bytes,
                    sticky_error,
                    status,
                });
            }
        }

        let classified_object_ids: HashSet<u64> = object_retentions.keys().copied().collect();
        for (&object_id, &retention) in &object_retentions {
            let Some(stats) = self.local_payload_backend.object_stats(object_id) else {
                continue;
            };
            match retention {
                PayloadRetentionClass::PinnedUnpublished => {
                    snapshot.cache_bytes.pinned_unpublished_bytes = snapshot
                        .cache_bytes
                        .pinned_unpublished_bytes
                        .saturating_add(stats.payload_len);
                }
                PayloadRetentionClass::PublishReadyRetained => {
                    snapshot.cache_bytes.publish_ready_retained_bytes = snapshot
                        .cache_bytes
                        .publish_ready_retained_bytes
                        .saturating_add(stats.payload_len);
                }
                PayloadRetentionClass::PublishedWarm => {}
                PayloadRetentionClass::DeletedBeforePublish => {
                    snapshot.cache_bytes.deleted_before_publish_bytes = snapshot
                        .cache_bytes
                        .deleted_before_publish_bytes
                        .saturating_add(stats.payload_len);
                }
            }
        }
        for object_id in self.local_payload_backend.object_ids() {
            if classified_object_ids.contains(&object_id) {
                continue;
            }
            let Some(stats) = self.local_payload_backend.object_stats(object_id) else {
                continue;
            };
            if stats.lifecycle == LocalPayloadLifecycleState::Uploaded {
                snapshot.cache_bytes.published_warm_bytes = snapshot
                    .cache_bytes
                    .published_warm_bytes
                    .saturating_add(stats.payload_len);
            }
        }

        let classified_non_evictable = snapshot
            .cache_bytes
            .pinned_unpublished_bytes
            .saturating_add(snapshot.cache_bytes.publish_ready_retained_bytes)
            .saturating_add(snapshot.cache_bytes.deleted_before_publish_bytes);
        snapshot.cache_bytes.unclassified_non_evictable_bytes =
            backend_non_evictable_bytes.saturating_sub(classified_non_evictable);

        if let Some(shutdown) = &snapshot.last_graceful_shutdown
            && !shutdown.recoverable_on_restart
        {
            health_signals.push(VfsZeroIoHealthSignal {
                severity: "error".to_string(),
                code: "shutdown_not_recoverable".to_string(),
                summary:
                    "last graceful shutdown did not fully flush accepted writes into recoverable local durable state"
                        .to_string(),
                ino: None,
                content_epoch: None,
                path: None,
            });
        }

        health_signals.sort_by(|left, right| {
            let severity_rank = |severity: &str| match severity {
                "error" => 3,
                "warn" => 2,
                "info" => 1,
                _ => 0,
            };
            severity_rank(&right.severity)
                .cmp(&severity_rank(&left.severity))
                .then_with(|| left.code.cmp(&right.code))
                .then_with(|| left.ino.cmp(&right.ino))
        });
        health_signals.truncate(ZEROIO_HEALTH_SIGNAL_LIMIT);
        for signal in &mut health_signals {
            if signal.path.is_none()
                && let Some(ino) = signal.ino
            {
                signal.path = self.best_effort_path_for_inode(ino);
            }
        }
        snapshot.health_signals = health_signals;

        drilldown.sort_by(|left, right| {
            Self::zeroio_status_priority(&right.status)
                .cmp(&Self::zeroio_status_priority(&left.status))
                .then_with(|| right.repeated_replay_count.cmp(&left.repeated_replay_count))
                .then_with(|| {
                    right
                        .oldest_pending_upload_age_ms
                        .cmp(&left.oldest_pending_upload_age_ms)
                })
                .then_with(|| right.pending_upload_bytes.cmp(&left.pending_upload_bytes))
                .then_with(|| left.ino.cmp(&right.ino))
        });
        snapshot.drilldown_truncated = drilldown
            .len()
            .saturating_sub(ZEROIO_DIAGNOSTIC_DRILLDOWN_LIMIT);
        drilldown.truncate(ZEROIO_DIAGNOSTIC_DRILLDOWN_LIMIT);
        for entry in &mut drilldown {
            entry.path = self.best_effort_path_for_inode(entry.ino);
        }
        snapshot.drilldown = drilldown;
        snapshot
    }

    /// Return the most recent runtime events, preserving tail order and bounded by `limit`.
    pub fn recent_runtime_events(&self, limit: usize) -> Vec<VfsRuntimeEvent> {
        let events = self.state.metrics.runtime_events.lock();
        let keep = limit.min(events.len());
        events
            .iter()
            .skip(events.len().saturating_sub(keep))
            .cloned()
            .collect()
    }

    /// Reset diagnostics counters and event history for fresh observability baselines.
    pub fn reset_diagnostics_observability(&self) {
        self.state.metrics.reset_diagnostics_observability();
        self.state.write_admission_governor.lock().reset_stats();
        self.state
            .local_dirty
            .hard_backpressure_events
            .store(0, Ordering::Relaxed);
        let fuse_async_write_reset_baseline =
            self.state.fuse_async_write_raw_snapshot.lock().clone();
        *self.state.fuse_async_write_reset_baseline.lock() = fuse_async_write_reset_baseline;
        *self.state.fuse_async_write_snapshot.lock() = VfsFuseAsyncWriteSnapshot::default();
        self.state.upload.startup_replay_counts.lock().clear();
        self.cached_store.reset_diagnostics_metrics();
    }

    /// Push a custom runtime event into diagnostics history.
    pub fn record_runtime_event(
        &self,
        subsystem: &str,
        kind: &str,
        title: &str,
        fields: serde_json::Value,
    ) {
        let sequence = self
            .state
            .metrics
            .runtime_event_sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let mut events = self.state.metrics.runtime_events.lock();
        if events.len() >= VFS_RUNTIME_EVENT_CAPACITY {
            events.pop_front();
        }
        events.push_back(VfsRuntimeEvent {
            sequence,
            generated_unix_ms: now_unix_millis(),
            subsystem: subsystem.to_string(),
            kind: kind.to_string(),
            title: title.to_string(),
            fields,
        });
    }

    fn replay_dirty_uploads_from_local_backend(
        &self,
        pending_uploads: &[PendingUpload],
        attrs: &[InodeAttr],
    ) {
        StartupReplayCoordinator::new(self, pending_uploads).run(attrs);
    }

    fn ensure_can_start(&self) -> VfsResult<()> {
        if self
            .state
            .shutdown
            .shutdown_requested
            .load(Ordering::Acquire)
            || self
                .state
                .shutdown
                .shutdown_completed
                .load(Ordering::Acquire)
        {
            return Err(VfsError::Other(
                "cannot start StudioVfs after shutdown has begun".to_string(),
            ));
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(VfsError::Other(
                "StudioVfs::start requires an active Tokio runtime".to_string(),
            ));
        }
        Ok(())
    }

    fn spawn_startup_workers(
        self: &Arc<Self>,
        StartupChannels {
            prefetch_rx,
            upload_rx,
        }: StartupChannels,
    ) -> VfsResult<()> {
        self.spawn_landing_zone_recovery_worker();
        self.spawn_aggressive_materialization_worker();
        self.spawn_local_dirty_flush_worker();
        self.spawn_prefetch_worker(prefetch_rx);
        self.spawn_uploader_worker(upload_rx);
        self.spawn_epoch_finalizer_worker();
        self.spawn_upload_stats_worker();
        self.spawn_fetch_warmup_task();
        self.spawn_gc_worker();
        self.spawn_metadata_invalidation_worker();
        self.spawn_janitor_worker();
        Ok(())
    }

    fn spawn_prefetch_worker(
        self: &Arc<Self>,
        prefetch_rx: tokio::sync::mpsc::UnboundedReceiver<PrefetchWorkItem>,
    ) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            let worker = PrefetchWorker::new(vfs, prefetch_rx);
            if let Err(panic_payload) = std::panic::AssertUnwindSafe(worker.run())
                .catch_unwind()
                .await
            {
                let panic_msg = panic_payload_message(&*panic_payload);
                error!("prefetch worker panicked: {}", panic_msg);
            }
        }));
    }

    fn spawn_aggressive_materialization_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_aggressive_materialization().await;
        }));
    }

    fn spawn_local_dirty_flush_worker(self: &Arc<Self>) {
        if self.disable_background_slab_flush {
            return;
        }
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_local_dirty_flush().await;
        }));
    }

    fn spawn_uploader_worker(
        self: &Arc<Self>,
        upload_rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
    ) {
        let vfs = self.clone();
        self.set_uploader_task(tokio::spawn(async move {
            vfs.background_uploader(upload_rx).await;
        }));
    }

    fn spawn_epoch_finalizer_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_epoch_finalizer().await;
        }));
    }

    fn spawn_upload_stats_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_upload_stats().await;
        }));
    }

    fn spawn_fetch_warmup_task(self: &Arc<Self>) {
        let fetch_scheduler = self.fetch_scheduler.clone();
        let timeout = Duration::from_secs(3);
        self.track_background_task(tokio::spawn(async move {
            let _ = fetch_scheduler.warmup(timeout).await;
        }));
    }

    fn spawn_gc_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_gc().await;
        }));
    }

    fn spawn_metadata_invalidation_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        let metadata_invalidation_sync_interval = self.startup.metadata_invalidation_sync_interval;
        self.track_background_task(tokio::spawn(async move {
            vfs.background_metadata_invalidation(metadata_invalidation_sync_interval)
                .await;
        }));
    }

    fn spawn_janitor_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            vfs.background_janitor().await;
        }));
    }

    /// Start background workers for this VFS instance.
    ///
    /// `new()` is side-effect free and only builds the object graph. Call
    /// `start()` once after construction to spawn uploader, prefetch, janitor,
    /// and other maintenance tasks.
    pub fn start(self: &Arc<Self>) -> VfsResult<()> {
        self.ensure_can_start()?;
        let Some(channels) = self.startup.claim_channels_for_start()? else {
            return Ok(());
        };

        let start_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.spawn_startup_workers(channels)
        })) {
            Ok(result) => result,
            Err(panic_payload) => {
                let panic_msg = panic_payload_message(&*panic_payload);
                error!(
                    "StudioVfs::start panicked during background startup: {}",
                    panic_msg
                );
                Err(VfsError::Other(format!(
                    "StudioVfs::start panicked during background startup: {panic_msg}"
                )))
            }
        };
        self.startup.finish_start(&start_result);
        start_result
    }

    #[cfg(test)]
    fn p90_ns(mut values: Vec<u64>) -> Option<u64> {
        if values.is_empty() {
            return None;
        }
        values.sort_unstable();
        let idx = ((values.len() - 1) * 90) / 100;
        values.get(idx).copied()
    }

    fn foreground_throughput_bytes_per_sec(
        speedometer_samples: usize,
        window_bytes: u64,
        window_coverage_ms: u64,
    ) -> f64 {
        if speedometer_samples == 0 || window_coverage_ms == 0 {
            0.0
        } else {
            let effective_window_coverage_ms =
                window_coverage_ms.max(FOREGROUND_READ_THROUGHPUT_MIN_COVERAGE_MS);
            window_bytes as f64 * 1000.0 / effective_window_coverage_ms as f64
        }
    }

    fn foreground_sticky_p90_latency_ns(&self) -> Option<u64> {
        let now_ms = now_unix_millis();
        let last_at = self
            .state
            .read
            .upload_fg_read_last_p90_at_ms
            .load(Ordering::Acquire);
        if last_at == 0 {
            return None;
        }
        let sticky_window_ms = self
            .upload_fg_read_latency_sticky_window
            .as_millis()
            .clamp(1, u64::MAX as u128) as u64;
        if now_ms.saturating_sub(last_at) > sticky_window_ms {
            return None;
        }
        let p90 = self
            .state
            .read
            .upload_fg_read_last_p90_ns
            .load(Ordering::Acquire);
        if p90 == 0 { None } else { Some(p90) }
    }

    fn update_upload_io_foreground_cost(&self, congested: bool, foreground_active: bool) -> u32 {
        let base_cost = self.upload_cache_io_foreground_cost_base.max(1);
        let step_cost = self.upload_cache_io_foreground_cost_step.max(1);
        let max_cost = self.upload_cache_io_slots_max.max(base_cost);
        let proactive_floor = base_cost.saturating_add(step_cost).min(max_cost);
        let current = self
            .state
            .upload
            .upload_cache_io_foreground_cost
            .load(Ordering::Relaxed);
        let next = if congested {
            current.saturating_add(step_cost).min(max_cost)
        } else if foreground_active {
            current.max(proactive_floor)
        } else if current > base_cost {
            current.saturating_sub(step_cost).max(base_cost)
        } else {
            base_cost
        };
        self.state
            .upload
            .upload_cache_io_foreground_cost
            .store(next, Ordering::Relaxed);
        next
    }

    fn adjust_upload_backpressure_from_metrics(
        &self,
        p90_latency_ns: Option<u64>,
        upload_pressure_boost: u8,
    ) {
        let p90_latency_ns = p90_latency_ns.or_else(|| self.foreground_sticky_p90_latency_ns());
        let threshold_ns = self.upload_fg_read_latency_target_ns.max(1);
        let foreground_active = self
            .state
            .read
            .foreground_user_io_active(now_unix_millis(), self.upload_user_idle_grace);
        let congested = p90_latency_ns.is_some_and(|p90| p90 > threshold_ns);
        let io_cost = self.update_upload_io_foreground_cost(congested, foreground_active);
        let pressure_boost = u64::from(upload_pressure_boost);

        let current_penalty_ms = self.upload_scheduler.adaptive_penalty_ms();
        let next_penalty_ms = if congested {
            let overshoot = p90_latency_ns
                .unwrap_or(threshold_ns)
                .saturating_div(threshold_ns)
                .clamp(1, UPLOAD_SCHEDULER_OVERSHOOT_MAX);
            let step = overshoot
                .saturating_mul(io_cost as u64)
                .saturating_mul(pressure_boost)
                .saturating_mul(UPLOAD_SCHEDULER_PENALTY_STEP_MS);
            current_penalty_ms
                .saturating_add(step)
                .min(UPLOAD_SCHEDULER_PENALTY_MAX_MS)
        } else {
            current_penalty_ms.saturating_sub(UPLOAD_SCHEDULER_PENALTY_DECAY_STEP_MS)
        };
        // #region debug log
        if current_penalty_ms != next_penalty_ms && (current_penalty_ms > 0 || next_penalty_ms > 0)
        {
            self.debug_write_probe(
                "H5",
                concat!(file!(), ":", line!()),
                "upload scheduler adaptive penalty changed",
                serde_json::json!({
                    "current_penalty_ms": current_penalty_ms,
                    "next_penalty_ms": next_penalty_ms,
                    "foreground_active": foreground_active,
                    "congested": congested,
                    "p90_latency_ns": p90_latency_ns,
                    "threshold_ns": threshold_ns,
                    "io_cost": io_cost,
                    "upload_pressure_boost": upload_pressure_boost,
                }),
            );
        }
        // #endregion debug log
        self.upload_scheduler
            .set_adaptive_penalty(Duration::from_millis(next_penalty_ms));
    }

    fn inode_has_unmaterialized_overlay_for_finalize(&self, ino: Ino) -> bool {
        if self.has_pending_local_dirty_payloads_for_inode(ino)
            || self.state.writer_stack.has_dirty_writers(ino)
            || self.state.deferred_materialize_inflight.contains(ino)
        {
            return true;
        }

        let meta = self.sync_meta();
        match meta.has_local_regular_file_overlay(ino) {
            Ok(has_overlay) => has_overlay && self.inode_has_pending_uploads(ino),
            Err(err) => {
                tracing::warn!(
                    ino,
                    error = %err,
                    "failed to probe persisted regular-file overlay; deferring finalize"
                );
                true
            }
        }
    }

    pub(super) fn inode_has_pending_uploads(&self, ino: Ino) -> bool {
        self.cached_store.has_pending_uploads_for_ino(ino)
    }

    fn inode_has_pending_uploads_newer_than(&self, ino: Ino, content_epoch: u64) -> bool {
        self.cached_store
            .has_pending_uploads_for_ino_newer_than(ino, content_epoch)
    }

    async fn wait_for_pending_uploads_idle(&self, timeout: Duration) -> bool {
        self.cached_store.wait_for_uploads_idle(timeout).await
            && self.upload_scheduler.in_flight() == 0
    }

    fn derive_committed_incarnation_from_attr(
        &self,
        attr: &InodeAttr,
    ) -> Option<CommittedIncarnation> {
        if attr.kind != FileType::RegularFile {
            return None;
        }
        Some(CommittedIncarnation {
            content_epoch: attr.committed_content_epoch,
            size: attr.committed_size,
        })
    }

    fn update_pending_incarnation_size(&self, ino: Ino, size: u64) {
        if let Some(state) = self
            .state
            .incarnation
            .inode_incarnations
            .lock()
            .get_mut(&ino)
        {
            let base_size = state
                .committed_incarnation
                .as_ref()
                .map(|committed| committed.size)
                .unwrap_or(0);
            if let Some(pending) = state.pending_incarnation.as_mut() {
                pending.size = size;
                pending.base_size = base_size;
            }
        }
    }

    fn begin_pending_incarnation(&self, ino: Ino, attr: &mut InodeAttr) {
        if attr.kind != FileType::RegularFile {
            return;
        }

        let mut states = self.state.incarnation.inode_incarnations.lock();
        let state = states.entry(ino).or_insert_with(|| InodeIncarnationState {
            committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
            pending_incarnation: None,
            upload_state: VfsUploadState::Idle,
        });
        if Self::is_fully_published_regular_file(attr)
            && state
                .pending_incarnation
                .as_ref()
                .is_some_and(|pending| pending.content_epoch <= attr.committed_content_epoch)
        {
            state.committed_incarnation = self.derive_committed_incarnation_from_attr(attr);
            state.pending_incarnation = None;
            state.upload_state = VfsUploadState::Idle;
        }

        if state.pending_incarnation.is_none() {
            let committed = state.committed_incarnation.clone().or({
                if attr.committed_size == 0 {
                    None
                } else {
                    Some(CommittedIncarnation {
                        content_epoch: attr.content_epoch,
                        size: attr.committed_size,
                    })
                }
            });
            let next_epoch = attr.content_epoch.saturating_add(1);
            state.committed_incarnation = committed.clone();
            state.pending_incarnation = Some(PendingIncarnation {
                content_epoch: next_epoch,
                size: attr.size,
                base_size: committed.as_ref().map(|base| base.size).unwrap_or(0),
            });
            state.upload_state = if committed.is_some() {
                VfsUploadState::Updating
            } else {
                VfsUploadState::Uploading
            };
            attr.content_epoch = next_epoch;
        } else {
            attr.content_epoch = state
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch)
                .unwrap_or(attr.content_epoch);
        }

        let base_size = state
            .committed_incarnation
            .as_ref()
            .map(|committed| committed.size)
            .unwrap_or(0);
        if let Some(pending) = state.pending_incarnation.as_mut() {
            pending.size = attr.size;
            pending.base_size = base_size;
        }
    }

    fn begin_size_change_incarnation(&self, ino: Ino, attr: &mut InodeAttr) {
        if attr.kind != FileType::RegularFile {
            return;
        }

        let mut states = self.state.incarnation.inode_incarnations.lock();
        let state = states.entry(ino).or_insert_with(|| InodeIncarnationState {
            committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
            pending_incarnation: None,
            upload_state: VfsUploadState::Idle,
        });

        if state.pending_incarnation.is_none() {
            drop(states);
            self.begin_pending_incarnation(ino, attr);
            return;
        }

        let committed = state.committed_incarnation.clone().or({
            if attr.committed_size == 0 {
                None
            } else {
                Some(CommittedIncarnation {
                    content_epoch: attr.committed_content_epoch,
                    size: attr.committed_size,
                })
            }
        });
        let previous_pending_epoch = state
            .pending_incarnation
            .as_ref()
            .map(|pending| pending.content_epoch)
            .unwrap_or(attr.content_epoch);
        let next_epoch = attr
            .content_epoch
            .max(previous_pending_epoch)
            .saturating_add(1);
        let base_size = committed.as_ref().map(|base| base.size).unwrap_or(0);

        state.committed_incarnation = committed.clone();
        state.pending_incarnation = Some(PendingIncarnation {
            content_epoch: next_epoch,
            size: attr.size,
            base_size,
        });
        state.upload_state = if committed.is_some() {
            VfsUploadState::Updating
        } else {
            VfsUploadState::Uploading
        };
        attr.content_epoch = next_epoch;
    }

    fn refresh_inode_incarnation_state_from_attr(
        &self,
        ino: Ino,
        attr: &InodeAttr,
    ) -> InodeIncarnationState {
        self.refresh_inode_incarnation_state_from_attr_inner(ino, attr, true)
    }

    fn refresh_inode_incarnation_state_from_attr_inner(
        &self,
        ino: Ino,
        attr: &InodeAttr,
        invalidate_kernel_on_supersede: bool,
    ) -> InodeIncarnationState {
        if self.ready_attr_supersedes_local_regular_file_state(ino, attr) {
            self.clear_superseded_local_regular_file_state(
                ino,
                attr.committed_content_epoch,
                invalidate_kernel_on_supersede,
            );
        }

        let has_pending_uploads = self.inode_has_pending_uploads(ino);
        let has_local_overlay = self.inode_has_local_overlay(ino);
        let has_live_extents = !self.live_extent_records_for_inode(ino).is_empty();
        let has_pending_epoch_finalization = self
            .state
            .upload
            .pending_epoch_finalizations
            .lock()
            .iter()
            .any(|pending| pending.0 == ino);
        if Self::is_fully_published_regular_file(attr)
            && !has_pending_uploads
            && !has_local_overlay
            && !has_live_extents
            && !has_pending_epoch_finalization
            && !self.has_pending_local_dirty_payloads_for_inode(ino)
            && !self.state.writer_stack.has_dirty_writers(ino)
            && !self.state.deferred_materialize_inflight.contains(ino)
        {
            self.remove_live_extents_for_inode(ino);
            self.remove_pending_epoch_finalizations_for_inode(ino);
            self.clear_matching_dirty_attr_if_committed(ino, attr);
            let next_state = InodeIncarnationState {
                committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
                pending_incarnation: None,
                upload_state: VfsUploadState::Idle,
            };
            self.state
                .incarnation
                .inode_incarnations
                .lock()
                .insert(ino, next_state.clone());
            return next_state;
        }

        let mut states = self.state.incarnation.inode_incarnations.lock();
        let previous = states
            .get(&ino)
            .cloned()
            .unwrap_or_else(|| InodeIncarnationState {
                committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
                pending_incarnation: None,
                upload_state: VfsUploadState::Idle,
            });

        let should_queue_finalization = Self::needs_epoch_finalization(attr);
        let retain_previous_pending =
            previous.pending_incarnation.is_some() && should_queue_finalization;

        let needs_pending = retain_previous_pending
            || has_local_overlay
            || has_pending_uploads
            || attr.size != attr.committed_size;

        let next_state = if needs_pending {
            let keep_previous_committed = retain_previous_pending
                || (previous.committed_incarnation.is_some()
                    && previous.upload_state != VfsUploadState::Idle);
            let committed = if keep_previous_committed {
                previous.committed_incarnation.clone()
            } else if attr.committed_size == 0
                && previous.committed_incarnation.is_none()
                && previous.upload_state == VfsUploadState::Uploading
            {
                None
            } else {
                self.derive_committed_incarnation_from_attr(attr)
                    .map(|mut committed| {
                        if attr.size != attr.committed_size
                            && committed.content_epoch == attr.content_epoch
                        {
                            committed.content_epoch = committed.content_epoch.saturating_sub(1);
                        }
                        committed
                    })
            };

            let pending_epoch = previous
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch)
                .unwrap_or_else(|| {
                    let min_pending_epoch = committed
                        .as_ref()
                        .map(|base| base.content_epoch.saturating_add(1))
                        .unwrap_or(0);
                    attr.content_epoch.max(min_pending_epoch)
                });

            InodeIncarnationState {
                committed_incarnation: committed.clone(),
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: pending_epoch,
                    size: attr.size,
                    base_size: committed.as_ref().map(|base| base.size).unwrap_or(0),
                }),
                upload_state: if committed.is_some() {
                    VfsUploadState::Updating
                } else {
                    VfsUploadState::Uploading
                },
            }
        } else {
            InodeIncarnationState {
                committed_incarnation: self.derive_committed_incarnation_from_attr(attr),
                pending_incarnation: None,
                upload_state: VfsUploadState::Idle,
            }
        };

        states.insert(ino, next_state.clone());
        drop(states);
        if should_queue_finalization {
            self.queue_epoch_finalization(ino, attr.content_epoch);
        }
        next_state
    }

    fn ready_attr_supersedes_local_regular_file_state(&self, ino: Ino, attr: &InodeAttr) -> bool {
        if !Self::is_fully_published_regular_file(attr) {
            return false;
        }
        let committed_epoch = attr.committed_content_epoch;
        let (dirty_attr_present, dirty_attr_epoch, dirty_attr_covers_ready_attr) = {
            let dirty_attrs = self.state.namespace.dirty_attrs.lock();
            let dirty_attr = dirty_attrs.get(&ino);
            (
                dirty_attr.is_some(),
                dirty_attr.map(|dirty_attr| dirty_attr.content_epoch),
                dirty_attr.is_some_and(|dirty_attr| {
                    dirty_attr.content_epoch > committed_epoch && dirty_attr.size >= attr.size
                }),
            )
        };
        let pending_incarnation_newer_than_committed = self
            .state
            .incarnation
            .inode_incarnations
            .lock()
            .get(&ino)
            .and_then(|state| state.pending_incarnation.as_ref())
            .is_some_and(|pending| pending.content_epoch > committed_epoch);
        let has_pending_uploads = self.inode_has_pending_uploads(ino);
        let has_newer_pending_upload =
            has_pending_uploads && self.inode_has_pending_uploads_newer_than(ino, committed_epoch);
        let has_same_epoch_pending_upload = has_pending_uploads
            && self
                .pending_uploads_for_inode(ino)
                .iter()
                .any(|(_, _, content_epoch)| *content_epoch == committed_epoch);
        if has_newer_pending_upload {
            return false;
        }
        let live_extents = self.live_extent_records_for_inode(ino);
        let has_newer_live_extent = live_extents
            .iter()
            .any(|(_, _, content_epoch)| *content_epoch > committed_epoch);
        let (has_pending_epoch_finalization, has_newer_pending_epoch_finalization) = {
            let pending_epoch_finalizations = self.state.upload.pending_epoch_finalizations.lock();
            (
                pending_epoch_finalizations
                    .iter()
                    .any(|pending| pending.0 == ino),
                pending_epoch_finalizations
                    .iter()
                    .any(|pending| pending.0 == ino && pending.1 > committed_epoch),
            )
        };
        if has_newer_pending_epoch_finalization {
            return false;
        }
        let has_pending_local_payloads = self.has_pending_local_dirty_payloads_for_inode(ino);
        let has_dirty_writers = self.state.writer_stack.has_dirty_writers(ino);
        if has_dirty_writers {
            return false;
        }
        if !dirty_attr_present && !has_pending_local_payloads && has_same_epoch_pending_upload {
            return false;
        }
        let local_state_is_newer_than_ready_attr =
            dirty_attr_epoch.is_none_or(|content_epoch| content_epoch > committed_epoch);
        if has_pending_local_payloads && local_state_is_newer_than_ready_attr {
            return false;
        }
        if dirty_attr_covers_ready_attr {
            return false;
        }
        if !dirty_attr_present
            && (has_newer_pending_upload
                || pending_incarnation_newer_than_committed
                || has_newer_live_extent
                || has_newer_pending_epoch_finalization)
        {
            return false;
        }
        has_pending_local_payloads
            || dirty_attr_present
            || has_dirty_writers
            || has_pending_uploads
            || !live_extents.is_empty()
            || has_pending_epoch_finalization
    }

    fn clear_superseded_local_regular_file_state(
        &self,
        ino: Ino,
        committed_epoch: u64,
        invalidate_kernel: bool,
    ) {
        debug!(
            ino,
            committed_epoch, "authoritative ready metadata superseded local regular-file state"
        );
        self.cancel_pending_uploads_for_inode(ino);
        self.clear_pending_payloads_for_inode(ino);
        self.state.writer_stack.clear_inode(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.remove_live_extents_for_inode(ino);
        self.remove_pending_epoch_finalizations_for_inode(ino);
        self.invalidate_attr_read_cache([ino]);
        if invalidate_kernel {
            self.invalidate_kernel_inodes([ino]);
        }
    }

    fn clear_inode_incarnation(&self, ino: Ino) {
        self.state
            .namespace
            .directory_generations
            .lock()
            .remove(&ino);
        self.state
            .incarnation
            .inode_incarnations
            .lock()
            .remove(&ino);
        self.state.upload.upload_sticky_errors.lock().remove(&ino);
        self.remove_pending_epoch_finalizations_for_inode(ino);
        self.state.file_session.gc_if_idle(ino);
    }

    fn clear_access_tracking(&self, ino: Ino) {
        self.state.read.clear_access_tracking(ino);
    }

    fn clear_access_tracking_for_stream(&self, ino: Ino, stream_id: u64) {
        self.state.read.clear_prefetch_hint(ino, stream_id);
        self.readahead
            .evict_stream(ReadStreamKey { ino, stream_id });
    }

    fn best_effort_path_for_inode(&self, ino: Ino) -> Option<String> {
        NamespaceResolver::new(self, NamespacePathStyle::Windows)
            .path_for_inode(ino)
            .or_else(|| NamespaceResolver::new(self, NamespacePathStyle::Posix).path_for_inode(ino))
    }

    fn debug_pending_target_path(&self, ino: Ino) -> Option<String> {
        let suffix = std::env::var(DEBUG_PENDING_PATH_SUFFIX_ENV).ok()?;
        let suffix = suffix.trim();
        if suffix.is_empty() {
            return None;
        }
        let normalized_suffix = suffix.replace('/', "\\").to_ascii_lowercase();
        let normalized_basename = normalized_suffix
            .trim_end_matches('\\')
            .rsplit('\\')
            .next()
            .unwrap_or(normalized_suffix.as_str());
        let path = self.best_effort_path_for_inode(ino)?;
        let normalized_path = path.replace('/', "\\").to_ascii_lowercase();
        let basename = Path::new(&normalized_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        (normalized_path.ends_with(&normalized_suffix) || basename == normalized_basename)
            .then_some(path)
    }

    fn debug_pending_inode_state(&self, ino: Ino, event: &str) {
        let Some(path) = self.debug_pending_target_path(ino) else {
            return;
        };
        let pending_uploads = self.pending_uploads_for_inode(ino);
        let pending_upload_sample: Vec<_> = pending_uploads
            .iter()
            .take(8)
            .map(|(offset, object_id, content_epoch)| {
                format!("{offset}:{object_id}@{content_epoch}")
            })
            .collect();
        let pending_epochs: Vec<_> = self
            .state
            .upload
            .pending_epoch_finalizations
            .lock()
            .iter()
            .filter_map(|(candidate, content_epoch)| (*candidate == ino).then_some(*content_epoch))
            .collect();
        let sticky_error = self
            .state
            .upload
            .upload_sticky_errors
            .lock()
            .get(&ino)
            .cloned();
        let attr = self.getattr(ino).ok();
        let (size, committed_size, content_epoch, committed_content_epoch, upload_state) = attr
            .as_ref()
            .map(|attr| {
                (
                    attr.size,
                    attr.committed_size,
                    attr.content_epoch,
                    attr.committed_content_epoch,
                    Some(format!("{:?}", attr.upload_state)),
                )
            })
            .unwrap_or((0, 0, 0, 0, None));
        info!(
            event,
            ino,
            path = %path,
            open_handles = self.open_count(ino),
            pending_upload_count = pending_uploads.len(),
            pending_upload_sample = ?pending_upload_sample,
            pending_epoch_count = pending_epochs.len(),
            pending_epochs = ?pending_epochs,
            sticky_error = ?sticky_error,
            has_local_dirty_state = self.inode_has_local_dirty_state(ino),
            has_pending_local_dirty_payloads = self.has_pending_local_dirty_payloads_for_inode(ino),
            attr_present = attr.is_some(),
            size,
            committed_size,
            content_epoch,
            committed_content_epoch,
            upload_state = ?upload_state,
            "debug pending inode state"
        );
    }

    fn mark_upload_sticky_error(&self, ino: Ino, error: &VfsError) {
        let message = error.to_string();
        let mut sticky = self.state.upload.upload_sticky_errors.lock();
        let changed = sticky
            .get(&ino)
            .map(|existing| existing != &message)
            .unwrap_or(true);
        sticky.insert(ino, message.clone());
        if changed {
            warn!(
                ino,
                error = %message,
                "recorded sticky async upload error for inode"
            );
        }
        self.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "failed",
        );
        self.debug_pending_inode_state(ino, "sticky_error_recorded");
    }

    fn clear_upload_sticky_error(&self, ino: Ino) {
        if self
            .state
            .upload
            .upload_sticky_errors
            .lock()
            .remove(&ino)
            .is_some()
        {
            info!(ino, "cleared sticky async upload error for inode");
        }
        self.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "recovered",
        );
        self.debug_pending_inode_state(ino, "sticky_error_cleared");
    }

    fn ensure_no_upload_sticky_error(&self, ino: Ino) -> VfsResult<()> {
        if let Some(message) = self
            .state
            .upload
            .upload_sticky_errors
            .lock()
            .get(&ino)
            .cloned()
        {
            return Err(VfsError::Other(format!(
                "sticky upload error on inode {ino}: {message}"
            )));
        }
        Ok(())
    }

    fn is_invalid_regular_file_graph_error(error: &VfsError) -> bool {
        matches!(
            error,
            VfsError::Meta(MetaError::NonContiguousRegularFileGraph { .. })
        )
    }

    fn needs_epoch_finalization(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state != MetaUploadState::Ready
            && (attr.content_epoch != attr.committed_content_epoch
                || attr.size != attr.committed_size)
    }

    fn is_fully_published_regular_file(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state == MetaUploadState::Ready
            && attr.content_epoch == attr.committed_content_epoch
            && attr.size == attr.committed_size
    }

    fn queue_epoch_finalization(&self, ino: Ino, content_epoch: u64) {
        let inserted = self
            .state
            .upload
            .pending_epoch_finalizations
            .lock()
            .insert((ino, content_epoch));
        if inserted {
            self.state.file_session.note_background_transition(
                ino,
                FileSessionOperation::Finalize,
                "queued",
            );
            self.state
                .upload
                .pending_epoch_finalizations_notify
                .notify_one();
            self.debug_pending_inode_state(ino, "epoch_finalization_queued");
        }
    }

    fn remove_pending_epoch_finalizations_for_inode(&self, ino: Ino) {
        let mut pending = self.state.upload.pending_epoch_finalizations.lock();
        let before = pending.len();
        pending.retain(|(candidate, _)| *candidate != ino);
        let removed = before.saturating_sub(pending.len());
        drop(pending);
        if removed > 0 {
            self.debug_pending_inode_state(ino, "epoch_finalization_cleared");
        }
    }

    fn cancel_pending_uploads_for_inode(&self, ino: Ino) {
        if !self.inode_has_pending_uploads(ino) {
            return;
        }
        let mut quarantined = HashSet::new();
        quarantined.insert(ino);
        self.cancel_pending_uploads_for_inodes(&quarantined);
    }

    fn cancel_pending_uploads_for_inodes(&self, inos: &HashSet<Ino>) {
        if inos.is_empty() {
            return;
        }

        let pending_uploads: Vec<_> = {
            let deleted_inodes = self.state.upload.deleted_inodes.lock();
            self.pending_uploads_snapshot()
                .into_iter()
                .filter(|op| inos.contains(&op.ino))
                .map(|op| {
                    let deleted = deleted_inodes.contains(&op.ino);
                    (op, deleted)
                })
                .collect()
        };
        if pending_uploads.is_empty() {
            return;
        }

        let mut deleted_cancel_count = 0;
        let mut canceled_tokens = HashSet::new();
        for (op, deleted) in &pending_uploads {
            if canceled_tokens.insert(op.ino) {
                self.cancel_upload_token_for_inode(op.ino);
            }
            if *deleted {
                deleted_cancel_count += 1;
            }
            self.cached_store.cancel_upload(op);
        }
        if deleted_cancel_count > 0 {
            // Telemetry only; upload cancellation state is carried by the queue.
            self.state
                .upload
                .upload_canceled_deleted
                .fetch_add(deleted_cancel_count, Ordering::Relaxed);
        }

        let mut pending_evictions = self.state.upload.pending_evictions.lock();
        let mut debug_inos = HashSet::new();
        for (op, _) in pending_uploads {
            pending_evictions.remove(&op.object_id);
            debug_inos.insert(op.ino);
        }
        drop(pending_evictions);
        for ino in debug_inos {
            self.debug_pending_inode_state(ino, "pending_upload_canceled");
        }
    }

    fn pop_epoch_finalization(&self) -> Option<(Ino, u64)> {
        let next = self
            .state
            .upload
            .pending_epoch_finalizations
            .lock()
            .pop_first();
        if let Some((ino, _)) = next {
            self.state.file_session.note_background_transition(
                ino,
                FileSessionOperation::Finalize,
                "dequeued",
            );
            self.debug_pending_inode_state(ino, "epoch_finalization_dequeued");
        }
        next
    }

    fn record_live_extents(&self, extents: &[(Ino, u32, u64, u64)]) {
        if extents.is_empty() {
            return;
        }

        let mut live_extents = self.state.incarnation.live_extent_records.lock();
        for (ino, block_index, object_id, content_epoch) in extents {
            live_extents.insert(
                (*ino, *block_index),
                ExtentRecordId {
                    object_id: *object_id,
                    content_epoch: *content_epoch,
                },
            );
        }
    }

    fn collect_regular_file_object_ids_for_recovery(&self, ino: Ino, size: u64) -> HashSet<u64> {
        let mut object_ids = HashSet::new();
        if size > 0
            && let Ok(spans) = self.sync_meta().lookup_regular_file_slices(ino, 0, size)
        {
            for span in spans {
                if let Some(block) = span.payload {
                    object_ids.insert(block.object_id);
                }
            }
        }

        if let Ok(ids) = self.sync_meta().get_regular_file_overlay_object_ids(ino) {
            object_ids.extend(ids);
        }

        object_ids
    }

    fn quarantine_invalid_regular_file_graph(
        &self,
        ino: Ino,
        size: u64,
        error: &VfsError,
    ) -> HashSet<u64> {
        self.quarantine_invalid_regular_file_graph_impl(ino, size, error, true)
    }

    fn quarantine_invalid_regular_file_graph_impl(
        &self,
        ino: Ino,
        size: u64,
        error: &VfsError,
        cancel_pending_uploads: bool,
    ) -> HashSet<u64> {
        self.mark_upload_sticky_error(ino, error);
        self.remove_live_extents_for_inode(ino);
        self.remove_pending_epoch_finalizations_for_inode(ino);
        if cancel_pending_uploads {
            self.cancel_pending_uploads_for_inode(ino);
        }
        self.collect_regular_file_object_ids_for_recovery(ino, size)
    }

    /// Return the current incarnation state for an inode.
    pub async fn inode_incarnation_state(&self, ino: Ino) -> VfsResult<InodeIncarnationState> {
        let attr = self.getattr_async(ino).await?;
        Ok(self.refresh_inode_incarnation_state_from_attr(ino, &attr))
    }

    fn remove_live_extent(&self, ino: Ino, block_index: u32) {
        self.state
            .incarnation
            .live_extent_records
            .lock()
            .remove(&(ino, block_index));
    }

    fn remove_live_extents_for_inode(&self, ino: Ino) {
        self.state
            .incarnation
            .live_extent_records
            .lock()
            .retain(|(candidate, _), _| *candidate != ino);
    }

    fn remove_live_extents_for_inode_epoch(&self, ino: Ino, content_epoch: u64) {
        self.state
            .incarnation
            .live_extent_records
            .lock()
            .retain(|(candidate, _), extent| {
                *candidate != ino || extent.content_epoch != content_epoch
            });
    }

    fn remote_upload_placeholder_visible(&self, _ino: Ino, _attr: &InodeAttr) -> bool {
        false
    }

    /// Return live extent records for an inode as `(block_index, object_id, content_epoch)`.
    pub fn live_extent_records_for_inode(&self, ino: Ino) -> Vec<(u32, u64, u64)> {
        let mut out: Vec<_> = self
            .state
            .incarnation
            .live_extent_records
            .lock()
            .iter()
            .filter(|((candidate, _), _)| *candidate == ino)
            .map(|((_, block_index), extent)| {
                (*block_index, extent.object_id, extent.content_epoch)
            })
            .collect();
        out.sort_by_key(|(block_index, _, _)| *block_index);
        out
    }

    /// Return pending uploads for an inode as `(offset, object_id, content_epoch)`.
    pub fn pending_uploads_for_inode(&self, ino: Ino) -> Vec<(u64, u64, u64)> {
        let mut out: Vec<_> = self
            .pending_uploads_snapshot()
            .into_iter()
            .filter(|op| op.ino == ino)
            .map(|op| (op.offset, op.object_id, op.content_epoch))
            .collect();
        out.sort_by_key(|(offset, _, _)| *offset);
        out
    }

    /// Read a single cached payload segment by object id, if present.
    pub fn read_cached_block(&self, object_id: u64) -> Option<Bytes> {
        let (cached, timing) = run_local_cache_io(|| self.local_payload_backend.get(object_id));
        self.record_local_cache_io_timing(timing);
        cached
    }

    async fn payload_store_get_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        if let Some(payload) = self.upload_hot_get(object_id) {
            return Ok(Some(payload));
        }
        let (result, timing) = run_local_cache_io(|| self.local_payload_backend.get(object_id));
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    async fn payload_store_read_into_async(
        &self,
        object_id: u64,
        data_offset: usize,
        out: &mut [u8],
    ) -> VfsResult<Option<usize>> {
        if let Some(payload) = self.upload_hot_get(object_id) {
            if data_offset >= payload.len() {
                return Ok(Some(0));
            }
            let available = payload.len() - data_offset;
            let read = available.min(out.len());
            out[..read].copy_from_slice(&payload[data_offset..data_offset + read]);
            self.record_foreground_local_payload_read(read as u64, 0);
            return Ok(Some(read));
        }
        let (result, timing) = run_local_cache_io(|| {
            self.local_payload_backend
                .read_into(object_id, data_offset, out)
        });
        self.record_local_cache_io_timing(timing);
        if let Some(read) = result {
            if read > out.len() {
                return Err(VfsError::Other(format!(
                    "local backend returned oversized read: object_id={object_id}, read={read}, out_len={}",
                    out.len()
                )));
            }
            self.record_foreground_local_payload_read(read as u64, timing.backend_io_ns);
            Ok(Some(read))
        } else {
            Ok(None)
        }
    }

    async fn payload_store_get_for_upload_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let payload_store = self.local_payload_backend.clone();
        let (result, timing) =
            run_local_cache_io_blocking(move || Ok(payload_store.get_for_upload(object_id)))
                .await?;
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    fn upload_hot_get(&self, object_id: u64) -> Option<Bytes> {
        self.cached_store.get_hot_upload(object_id)
    }

    async fn upload_stage_contains_async(&self, object_id: u64) -> VfsResult<bool> {
        let cached_store = self.cached_store.clone();
        let (result, timing) =
            run_local_cache_io_blocking(move || Ok(cached_store.has_staged_upload(object_id)))
                .await?;
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    async fn upload_stage_get_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let cached_store = self.cached_store.clone();
        let (result, timing) =
            run_local_cache_io_blocking(move || Ok(cached_store.get_staged_for_upload(object_id)))
                .await?;
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    async fn upload_stage_link_batch_async(&self, object_ids: Vec<u64>) -> VfsResult<()> {
        if object_ids.is_empty() {
            return Ok(());
        }
        let cached_store = self.cached_store.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            cached_store
                .link_upload_stage_batch(&object_ids)
                .map_err(std::io::Error::other)?;
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn payload_store_run_pressure_maintenance_async(&self) -> VfsResult<()> {
        let payload_store = self.local_payload_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.run_pressure_maintenance().map_err(|error| {
                VfsError::Other(format!("payload backend maintenance failed: {error}"))
            })
        })
        .await?;
        let _ = timing;
        Ok(())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    async fn payload_store_put_async(
        &self,
        object_id: u64,
        data: Bytes,
        evictable: bool,
    ) -> VfsResult<()> {
        let payload_store = self.local_payload_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store
                .put(object_id, data.as_ref(), evictable)
                .map_err(|error| VfsError::Other(format!("payload store put failed: {error}")))
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn payload_store_put_batch_async(&self, entries: Vec<LocalPayloadPut>) -> VfsResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let payload_store = self.local_payload_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.put_batch(&entries).map_err(|error| {
                VfsError::Other(format!("payload store batch put failed: {error}"))
            })
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    fn cache_upload_hot_batch(&self, entries: Vec<LocalPayloadPut>) {
        if entries.is_empty() {
            return;
        }
        self.cached_store.cache_hot_upload_batch(&entries);
    }

    async fn maybe_payload_store_sync_data_async(
        &self,
        persisted_bytes: u64,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if persisted_bytes > 0 {
            self.record_local_sync_unsynced_bytes(persisted_bytes);
        }
        {
            let mut local_sync_group = self.state.local_dirty.local_sync_group.lock().await;
            let pending_before = local_sync_group.pending_bytes;
            if pending_before == 0 && persisted_bytes > 0 {
                local_sync_group.last_sync_at = Instant::now();
            }
            local_sync_group.pending_bytes = local_sync_group
                .pending_bytes
                .saturating_add(persisted_bytes);

            let trigger = write_path_sync_trigger(local_sync_policy);
            if trigger.is_none() {
                if local_sync_group.pending_bytes > 0 {
                    self.state
                        .metrics
                        .payload_store_sync_data_deferred_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }

        let sync_gate_wait_started = Instant::now();
        let _sync_gate = if local_sync_policy.is_opportunistic() {
            match self.state.local_dirty.local_sync_gate.try_lock() {
                Ok(guard) => guard,
                Err(_) => {
                    self.state
                        .metrics
                        .payload_store_sync_data_deferred_calls
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }
        } else {
            self.state.local_dirty.local_sync_gate.lock().await
        };
        self.record_sync_data_queue_wait(sync_gate_wait_started.elapsed());
        let bytes_to_sync = {
            let mut local_sync_group = self.state.local_dirty.local_sync_group.lock().await;
            let trigger = write_path_sync_trigger(local_sync_policy);
            if trigger.is_none() {
                if local_sync_group.pending_bytes > 0 {
                    self.state
                        .metrics
                        .payload_store_sync_data_deferred_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }

            let bytes_to_sync = local_sync_group.pending_bytes;
            if bytes_to_sync == 0 {
                return Ok(());
            }

            local_sync_group.pending_bytes = 0;
            bytes_to_sync
        };

        match self.payload_store_sync_data_async().await {
            Ok(()) => {
                let mut local_sync_group = self.state.local_dirty.local_sync_group.lock().await;
                local_sync_group.last_sync_at = Instant::now();
                drop(local_sync_group);
                self.record_local_sync_synced_bytes(bytes_to_sync);
                self.state
                    .metrics
                    .payload_store_sync_data_synced_bytes
                    .fetch_add(bytes_to_sync, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                let mut local_sync_group = self.state.local_dirty.local_sync_group.lock().await;
                local_sync_group.pending_bytes =
                    local_sync_group.pending_bytes.saturating_add(bytes_to_sync);
                Err(error)
            }
        }
    }

    async fn payload_store_sync_data_async(&self) -> VfsResult<()> {
        let payload_store = self.local_payload_backend.clone();
        let started = Instant::now();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.sync_data().map_err(|error| {
                VfsError::Other(format!("payload backend sync_data failed: {error}"))
            })
        })
        .await?;
        let _ = timing;
        let elapsed_ns = Self::duration_ns_u64(started.elapsed());
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.payload_store_sync_data_calls,
            &metrics.payload_store_sync_data_total_ns,
            &metrics.payload_store_sync_data_max_ns,
            &metrics.payload_store_sync_data_histogram,
            elapsed_ns,
        );
        self.record_sync_data_interval();
        Ok(())
    }

    async fn payload_store_mark_uploaded_async(&self, object_id: u64) -> VfsResult<()> {
        let payload_store = self.local_payload_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.mark_uploaded(object_id);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn payload_store_remove_async(&self, object_id: u64) -> VfsResult<()> {
        let payload_store = self.local_payload_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.remove(object_id);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn payload_store_remove_batch_async(&self, object_ids: &[u64]) -> VfsResult<()> {
        if object_ids.is_empty() {
            return Ok(());
        }
        let payload_store = self.local_payload_backend.clone();
        let object_ids = object_ids.to_vec();
        let ((), timing) = run_local_cache_io_blocking(move || {
            payload_store.remove_batch(&object_ids);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn upload_stage_remove_async(&self, object_id: u64) -> VfsResult<()> {
        let cached_store = self.cached_store.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            cached_store.remove_staged_upload(object_id);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    fn upload_hot_remove(&self, object_id: u64) {
        self.cached_store.remove_hot_upload(object_id);
    }

    async fn upload_stage_remove_batch_async(&self, object_ids: &[u64]) -> VfsResult<()> {
        if object_ids.is_empty() {
            return Ok(());
        }
        let cached_store = self.cached_store.clone();
        let object_ids = object_ids.to_vec();
        let ((), timing) = run_local_cache_io_blocking(move || {
            cached_store.remove_staged_upload_batch(&object_ids);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    fn upload_hot_remove_batch(&self, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        self.cached_store.remove_hot_upload_batch(object_ids);
    }

    /// Return upload queue and throughput statistics for diagnostics.
    pub fn upload_stats(&self) -> VfsUploadStats {
        let pending_uploads = self.pending_uploads_snapshot();
        let now_ms = now_unix_millis();
        let (
            pending_upload_bytes,
            oldest_pending_upload_age_ms,
            pending_retry_count,
            max_pending_retry_count,
        ) = pending_uploads.iter().fold(
            (0u64, 0u64, 0u64, 0u32),
            |(bytes, oldest_age, retries, max_retries), op| {
                let payload_len = self
                    .local_payload_backend
                    .object_stats(op.object_id)
                    .map(|stats| stats.payload_len)
                    .unwrap_or(u64::from(self.block_size));
                (
                    bytes.saturating_add(payload_len),
                    oldest_age.max(now_ms.saturating_sub(op.created_unix_ms)),
                    retries.saturating_add(u64::from(op.retry_count)),
                    max_retries.max(op.retry_count),
                )
            },
        );
        let cache_metrics = self.cached_store.cache_pipeline_metrics();
        let completed_uploads = cache_metrics.upload_queue.completed;
        let completed_upload_bytes = cache_metrics.upload_queue.completed_bytes;
        let elapsed = self.write_path_started_at.elapsed();
        let completed_uploads_per_sec = Self::per_second(completed_uploads, elapsed);
        let completed_upload_bytes_per_sec = Self::per_second(completed_upload_bytes, elapsed);
        let upload_metrics = &self.upload_scheduler.metrics;

        VfsUploadStats {
            pending_uploads: pending_uploads.len(),
            pending_upload_bytes,
            oldest_pending_upload_age_ms,
            active_uploads: self.upload_scheduler.in_flight(),
            pending_retry_count,
            max_pending_retry_count,
            completed_uploads,
            completed_upload_bytes,
            completed_uploads_per_sec,
            completed_upload_bytes_per_sec,
            encrypted_objects_written: upload_metrics
                .encrypted_objects_written
                .load(Ordering::Relaxed),
            encrypted_bytes_written: upload_metrics
                .encrypted_bytes_written
                .load(Ordering::Relaxed),
            encrypted_object_write_failures: upload_metrics
                .encrypted_object_write_failures
                .load(Ordering::Relaxed),
            upload_requeues: self.state.upload.upload_requeues.load(Ordering::Relaxed),
            canceled_deleted: self
                .state
                .upload
                .upload_canceled_deleted
                .load(Ordering::Relaxed),
            canceled_stale: self
                .state
                .upload
                .upload_canceled_stale
                .load(Ordering::Relaxed),
            canceled_stale_before_put: self
                .state
                .upload
                .upload_canceled_stale_before_put
                .load(Ordering::Relaxed),
            canceled_stale_after_put: self
                .state
                .upload
                .upload_canceled_stale_after_put
                .load(Ordering::Relaxed),
            canceled_stale_after_put_bytes: self
                .state
                .upload
                .upload_canceled_stale_after_put_bytes
                .load(Ordering::Relaxed),
            pending_upload_snapshot_calls: cache_metrics.upload_queue.snapshot_pending_calls,
        }
    }

    /// Return file-session statistics for open handles and handles lifecycle.
    pub fn file_session_stats(&self) -> VfsFileSessionStats {
        self.state.file_session.stats()
    }

    /// Return foreground-read protection and speedometer metrics.
    pub fn foreground_read_protection_stats(&self) -> VfsForegroundReadProtectionStats {
        let (p90_latency_ns, speedometer_samples, window_bytes, window_coverage_ms) = self
            .state
            .read
            .foreground_speedometer_snapshot(now_unix_millis(), self.upload_fg_read_latency_window);
        let throughput_bytes_per_sec = Self::foreground_throughput_bytes_per_sec(
            speedometer_samples,
            window_bytes,
            window_coverage_ms,
        );
        let dominant = self.state.read.foreground_access_pattern_snapshot(
            now_unix_millis(),
            self.upload_fg_read_latency_window,
        );
        let (dominant_access_class, dominant_policy) = match dominant {
            Some((class, policy)) => (Some(class.as_str().to_string()), policy),
            None => (None, AccessPolicy::default()),
        };

        let active_counts = self.state.read.active_access_counts();

        VfsForegroundReadProtectionStats {
            p90_latency_ns,
            throughput_bytes_per_sec,
            speedometer_samples,
            window_bytes,
            window_coverage_ms,
            dominant_access_class,
            dominant_prefetch_enabled: dominant_policy.prefetch_enabled,
            dominant_cache_promotion_on_read: dominant_policy.cache_promotion_on_read,
            dominant_readahead_base_width: dominant_policy.readahead_base_width,
            dominant_readahead_max_width: dominant_policy.readahead_max_width,
            dominant_readahead_min_prefetch: dominant_policy.readahead_min_prefetch,
            dominant_upload_pressure_boost: dominant_policy.upload_pressure_boost,
            active_sequential_media_reads: active_counts[AccessClass::SequentialMediaRead.index()],
            active_random_interactive_reads: active_counts
                [AccessClass::RandomInteractiveRead.index()],
            active_sequential_writers: active_counts[AccessClass::SequentialWriter.index()],
            active_random_writers: active_counts[AccessClass::RandomWriter.index()],
            upload_local_payload_reads: self
                .state
                .upload
                .upload_local_payload_reads
                .load(Ordering::Relaxed),
            upload_local_payload_read_bytes: self
                .state
                .upload
                .upload_local_payload_read_bytes
                .load(Ordering::Relaxed),
            upload_hot_payload_reads: self
                .state
                .upload
                .upload_hot_payload_reads
                .load(Ordering::Relaxed),
            upload_hot_payload_read_bytes: self
                .state
                .upload
                .upload_hot_payload_read_bytes
                .load(Ordering::Relaxed),
            upload_staged_payload_reads: self
                .state
                .upload
                .upload_staged_payload_reads
                .load(Ordering::Relaxed),
            upload_staged_payload_read_bytes: self
                .state
                .upload
                .upload_staged_payload_read_bytes
                .load(Ordering::Relaxed),
            upload_fallback_deferrals: self
                .state
                .upload
                .upload_fallback_deferrals
                .load(Ordering::Relaxed),
            upload_fallback_rereads: self
                .state
                .upload
                .upload_fallback_rereads
                .load(Ordering::Relaxed),
            upload_fallback_reread_bytes: self
                .state
                .upload
                .upload_fallback_reread_bytes
                .load(Ordering::Relaxed),
            upload_fallback_reread_cache_misses: self
                .state
                .upload
                .upload_fallback_reread_cache_misses
                .load(Ordering::Relaxed),
            upload_fallback_wait_ns: self
                .state
                .upload
                .upload_fallback_wait_ns
                .load(Ordering::Relaxed),
            upload_fallback_wait_max_ns: self
                .state
                .upload
                .upload_fallback_wait_max_ns
                .load(Ordering::Relaxed),
            upload_fallback_read_ns: self
                .state
                .upload
                .upload_fallback_read_ns
                .load(Ordering::Relaxed),
            upload_fallback_read_max_ns: self
                .state
                .upload
                .upload_fallback_read_max_ns
                .load(Ordering::Relaxed),
            foreground_local_payload_reads: self
                .state
                .read
                .foreground_local_payload_reads
                .load(Ordering::Relaxed),
            foreground_local_payload_read_bytes: self
                .state
                .read
                .foreground_local_payload_read_bytes
                .load(Ordering::Relaxed),
            foreground_local_payload_read_io_ns: self
                .state
                .read
                .foreground_local_payload_read_io_ns
                .load(Ordering::Relaxed),
            foreground_local_payload_read_io_max_ns: self
                .state
                .read
                .foreground_local_payload_read_io_max_ns
                .load(Ordering::Relaxed),
            upload_cache_io_foreground_cost: self
                .state
                .upload
                .upload_cache_io_foreground_cost
                .load(Ordering::Relaxed),
            upload_scheduler_penalty_ms: self.upload_scheduler.adaptive_penalty_ms(),
        }
    }

    fn logical_block_len(&self, file_size: u64, block_index: u32) -> usize {
        let block_start = block_index as u64 * self.block_size as u64;
        if block_start >= file_size {
            return 0;
        }
        file_size
            .saturating_sub(block_start)
            .min(self.block_size as u64) as usize
    }

    fn encode_lease_record(owner_id: &str, lease_until_unix: i64, fencing_token: u64) -> Vec<u8> {
        format!("{owner_id}\n{lease_until_unix}\n{fencing_token}\n").into_bytes()
    }

    fn decode_lease_record(bytes: &[u8]) -> Option<(String, i64, u64)> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let owner = lines.next()?.to_string();
        let lease_until = lines.next()?.parse::<i64>().ok()?;
        let fencing = lines.next()?.parse::<u64>().ok()?;
        Some((owner, lease_until, fencing))
    }

    async fn acquire_janitor_lease(&self, lease: &JanitorLease) -> bool {
        let now = now_unix_secs();
        let lease_until = now.saturating_add(self.janitor_lease_ttl.as_secs() as i64);

        let create_payload = Self::encode_lease_record(&lease.owner_id, lease_until, 1);
        let create_opts = WriteOptions {
            if_not_exists: true,
            ..WriteOptions::default()
        };
        match lease
            .op
            .write_options(&lease.lock_path, create_payload, create_opts)
            .await
        {
            Ok(_) => {
                debug!(
                    lock_path = lease.lock_path,
                    owner_id = lease.owner_id,
                    "janitor lease acquired via create"
                );
                return true;
            }
            Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {}
            Err(err) => {
                warn!("janitor lease create failed: {}", err);
                return false;
            }
        }

        let stat = match lease.op.stat(&lease.lock_path).await {
            Ok(v) => v,
            Err(err) if err.kind() == ErrorKind::NotFound => {
                // Lock disappeared between create attempt and stat; next cycle will retry.
                return false;
            }
            Err(err) => {
                warn!("janitor lease stat failed: {}", err);
                return false;
            }
        };
        let etag = stat.etag().map(ToString::to_string);
        let current_raw = match lease.op.read(&lease.lock_path).await {
            Ok(v) => v,
            Err(err) => {
                warn!("janitor lease read failed: {}", err);
                return false;
            }
        };
        let decoded_lease = {
            let raw_chunk = current_raw.chunk();
            if raw_chunk.len() == current_raw.remaining() {
                Self::decode_lease_record(raw_chunk)
            } else {
                let current_raw = current_raw.to_bytes();
                Self::decode_lease_record(&current_raw)
            }
        };
        let (owner, current_until, fencing) = match decoded_lease {
            Some(v) => v,
            None => ("".to_string(), 0, 0),
        };

        if current_until > now && owner != lease.owner_id {
            debug!(
                lock_path = lease.lock_path,
                lock_owner = owner,
                lock_until = current_until,
                now,
                "janitor lease is held by another client"
            );
            return false;
        }

        let mut update_opts = WriteOptions::default();
        if let Some(v) = etag {
            update_opts.if_match = Some(v);
        }
        let update_payload =
            Self::encode_lease_record(&lease.owner_id, lease_until, fencing.saturating_add(1));
        match lease
            .op
            .write_options(&lease.lock_path, update_payload, update_opts)
            .await
        {
            Ok(_) => {
                debug!(
                    lock_path = lease.lock_path,
                    owner_id = lease.owner_id,
                    "janitor lease acquired/renewed via CAS"
                );
                true
            }
            Err(err) if err.kind() == ErrorKind::ConditionNotMatch => false,
            Err(err) => {
                warn!("janitor lease CAS update failed: {}", err);
                false
            }
        }
    }

    /// Obtain the synchronous metadata engine handle backing this VFS.
    ///
    /// Prefer caching this once per function (`let meta = self.sync_meta();`) if
    /// multiple synchronous metadata calls are needed.
    ///
    /// This handle should be used only where synchronous blocking is explicitly
    /// acceptable (for example, inside `run_meta` closures, sync VFS entrypoints,
    /// or guarded `block_in_place` sections).
    ///
    /// In async paths, prefer `self.meta.run(...)` so blocking work stays behind
    /// one explicit metadata boundary.
    fn sync_meta(&self) -> Arc<dyn VfsMeta> {
        self.meta.sync_view()
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> Arc<dyn VfsMeta> {
        self.sync_meta()
    }

    /// Get volume name.
    pub fn volume_name(&self) -> VfsResult<String> {
        self.meta
            .run_sync(|meta| meta.get_format_info().map(|format| format.name))
    }

    /// Get volume UUID.
    pub fn uuid(&self) -> VfsResult<String> {
        self.meta
            .run_sync(|meta| meta.get_format_info().map(|format| format.uuid))
    }

    /// Get block size.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Current dirty-buffer memory limit.
    pub fn max_dirty_bytes(&self) -> usize {
        self.max_dirty_bytes.load(Ordering::Acquire)
    }

    /// Update the dirty-buffer memory limit at runtime.
    pub fn set_max_dirty_bytes(&self, value: usize) {
        self.max_dirty_bytes.store(value, Ordering::Release);
    }

    /// Number of foreground reads currently in-flight.
    pub fn foreground_reads_inflight(&self) -> usize {
        self.state
            .read
            .foreground_reads_inflight
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    /// Test helper: wait until foreground reads complete or timeout elapses.
    pub(crate) async fn wait_for_foreground_reads_idle(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        loop {
            let now_ms = now_unix_millis();
            if !self
                .state
                .read
                .foreground_read_active(now_ms, self.upload_user_idle_grace)
            {
                return true;
            }

            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return false;
            }

            let notified = self.state.read.foreground_idle_notify.notified();
            let now_ms = now_unix_millis();
            if !self
                .state
                .read
                .foreground_read_active(now_ms, self.upload_user_idle_grace)
            {
                return true;
            }

            let wait_slice = if self.foreground_reads_inflight() > 0 {
                remaining
            } else {
                remaining.min(self.upload_user_idle_grace)
            };
            if wait_slice.is_zero() {
                return false;
            }

            let _ = tokio::time::timeout(wait_slice, notified).await;
        }
    }

    /// Shared reference to the cached-store runtime.
    pub fn cached_store(&self) -> &Arc<CachedStore> {
        &self.cached_store
    }

    /// Shared reference to the local payload backend contract.
    pub fn local_payload_backend(&self) -> &(dyn LocalPayloadBackend + 'static) {
        self.local_payload_backend.as_ref()
    }

    /// Shared reference to the fetch scheduler.
    pub fn fetch_scheduler(&self) -> &Arc<crate::fetch::FetchScheduler> {
        &self.fetch_scheduler
    }

    /// Shared reference to the transit memory budget.
    pub fn transit_memory_budget(&self) -> &Arc<crate::qos::TransitMemoryBudget> {
        &self.transit_memory_budget
    }

    /// Wait until all pending S3 uploads complete or `timeout` elapses.
    ///
    /// Returns `true` if the upload queue drained within the timeout.
    /// Useful in tests to ensure block data is available on S3 before
    /// cross-client reads.
    pub async fn wait_for_uploads_idle(&self, timeout: Duration) -> bool {
        self.wait_for_pending_uploads_idle(timeout).await
    }

    /// Snapshot accepted local-ingress state for diagnostics and backpressure
    /// observability.
    pub fn ingress_stats(&self) -> VfsIngressStats {
        let pending = self.state.local_dirty.pending_payloads.lock();
        let dirty_mem_blocks = pending.len();
        let dirty_mem_bytes = self.total_pending_local_dirty_bytes();
        drop(pending);

        VfsIngressStats {
            dirty_mem_bytes,
            dirty_mem_blocks,
            write_backpressure_wait_ms: self
                .state
                .local_dirty
                .write_backpressure_wait_ms
                .load(Ordering::Relaxed),
            write_backpressure_events: self
                .state
                .local_dirty
                .write_backpressure_events
                .load(Ordering::Relaxed),
            fast_backpressure_wait_ms: self
                .state
                .local_dirty
                .fast_backpressure_wait_ms
                .load(Ordering::Relaxed),
            fast_backpressure_events: self
                .state
                .local_dirty
                .fast_backpressure_events
                .load(Ordering::Relaxed),
            fast_backpressure_stall_max_ms: self
                .state
                .local_dirty
                .fast_backpressure_stall_max_ms
                .load(Ordering::Relaxed),
            prefetch_queue_depth: self
                .state
                .metrics
                .prefetch_queue_depth
                .load(Ordering::Relaxed),
        }
    }

    fn total_pending_write_buffer_bytes(&self) -> usize {
        self.total_pending_local_dirty_bytes()
            .saturating_add(self.state.writer_stack.total_dirty_bytes())
    }

    fn write_admission_pressure(&self) -> WriteAdmissionPressure {
        let cached_store_writer_counters = self.cached_store.snapshot_writer_counters();
        WriteAdmissionPressure {
            writer_stack_dirty_bytes: self.state.writer_stack.total_dirty_bytes(),
            pending_local_dirty_bytes: self.total_pending_local_dirty_bytes(),
            pending_persisted_slice_bytes: self.total_pending_persisted_local_dirty_slice_bytes(),
            pending_upload_bytes: usize::try_from(
                cached_store_writer_counters.pending_upload_bytes,
            )
            .unwrap_or(usize::MAX),
        }
    }

    fn local_sync_unsynced_bytes(&self) -> usize {
        usize::try_from(
            self.state
                .local_dirty
                .local_sync_unsynced_bytes
                .load(Ordering::Relaxed),
        )
        .unwrap_or(usize::MAX)
    }

    fn record_local_sync_unsynced_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.state
            .local_dirty
            .local_sync_unsynced_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_local_sync_synced_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .state
            .local_dirty
            .local_sync_unsynced_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                Some(pending.saturating_sub(bytes))
            });
    }

    fn write_admission_buffer_budget_bytes(&self) -> usize {
        self.max_dirty_bytes().max(self.block_size.max(1) as usize)
    }

    fn record_write_admission_accepted_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.state
            .write_admission_governor
            .lock()
            .record_accepted_bytes(bytes);
    }

    fn record_write_admission_wait(&self, waited: Duration) {
        let waited_ms = waited.as_millis().min(u128::from(u64::MAX)) as u64;
        if waited_ms == 0 {
            return;
        }
        self.state
            .metrics
            .drain_lag_pacing_events
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .drain_lag_pacing_total_ms
            .fetch_add(waited_ms, Ordering::Relaxed);
        self.state
            .metrics
            .drain_lag_pacing_max_ms
            .fetch_max(waited_ms, Ordering::Relaxed);
    }

    async fn maybe_throttle_write_admission(&self, next_bytes: usize, sustained_delay_ms: u64) {
        if next_bytes == 0 || self.shutdown_requested() {
            return;
        }

        let buffer_budget_bytes = self.write_admission_buffer_budget_bytes();
        let pressure = self.write_admission_pressure();
        let mut pressure_bytes = pressure.total_bytes();

        let hard_window_bytes =
            buffer_budget_bytes.saturating_mul(HARD_BACKPRESSURE_DIRTY_MULTIPLIER);
        let mut writer_stack_pressure_bytes = pressure.writer_stack_dirty_bytes;
        let mut hard_wait_started = None;
        while writer_stack_pressure_bytes > hard_window_bytes && !self.shutdown_requested() {
            hard_wait_started.get_or_insert_with(Instant::now);
            // #region debug log
            if self.write_debug_enabled() {
                self.debug_write_probe(
                    "H1",
                    concat!(file!(), ":", line!()),
                    "write admission buffer hard pacing",
                    serde_json::json!({
                        "delay_ms": WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS,
                        "writer_stack_dirty_bytes": writer_stack_pressure_bytes,
                        "pressure_bytes": pressure_bytes,
                        "pending_upload_bytes": pressure.pending_upload_bytes,
                        "buffer_budget_bytes": buffer_budget_bytes,
                        "hard_window_bytes": hard_window_bytes,
                        "next_bytes": next_bytes,
                    }),
                );
            }
            // #endregion debug log

            tokio::time::sleep(Duration::from_millis(WRITE_ADMISSION_BUFFER_HIGH_DELAY_MS)).await;
            let pressure = self.write_admission_pressure();
            pressure_bytes = pressure.total_bytes();
            writer_stack_pressure_bytes = pressure.writer_stack_dirty_bytes;
        }
        if let Some(started) = hard_wait_started {
            self.record_write_admission_wait(started.elapsed());
            self.state.write_admission_governor.lock().reset_stats();
            return;
        }

        let decision = {
            let mut governor = self.state.write_admission_governor.lock();
            governor.plan(WriteAdmissionSample {
                now_ms: now_unix_millis(),
                pressure_bytes,
                budget_bytes: buffer_budget_bytes,
                next_bytes,
            })
        };
        let delay_ms = decision.delay_ms.max(sustained_delay_ms);
        if delay_ms == 0 {
            return;
        }
        // #region debug log
        if self.write_debug_enabled() {
            self.debug_write_probe(
                "H2",
                concat!(file!(), ":", line!()),
                "write admission governor pacing",
                serde_json::json!({
                    "delay_ms": delay_ms,
                    "governor_delay_ms": decision.delay_ms,
                    "sustained_delay_ms": sustained_delay_ms,
                    "ingress_bytes_per_sec": decision.ingress_bytes_per_sec,
                    "drain_bytes_per_sec": decision.drain_bytes_per_sec,
                    "runway_ms": decision.runway_ms,
                    "pressure_bytes": pressure_bytes,
                    "pending_upload_bytes": pressure.pending_upload_bytes,
                    "budget_bytes": buffer_budget_bytes,
                    "next_bytes": next_bytes,
                }),
            );
        }
        // #endregion debug log

        if self.shutdown_requested() {
            return;
        }
        let started = Instant::now();
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        self.record_write_admission_wait(started.elapsed());
    }

    fn write_admission_governor_stats(&self) -> WriteAdmissionGovernorStats {
        self.state.write_admission_governor.lock().stats()
    }

    fn writer_stack_steady_drain_quota_bytes(&self) -> usize {
        self.write_eager_flush_drain_chunk_bytes
            .max(self.block_size as usize)
            .max(1)
    }

    fn per_second(total: u64, elapsed: Duration) -> f64 {
        let secs = elapsed.as_secs_f64();
        if secs <= f64::EPSILON {
            0.0
        } else {
            total as f64 / secs
        }
    }

    fn histogram_percentile_ns(
        snapshot: &LatencyHistogramSnapshot,
        percentile: f64,
    ) -> (u64, bool) {
        if snapshot.bucket_counts.is_empty() {
            return (0, false);
        }
        let total: u64 = snapshot.bucket_counts.iter().sum();
        if total == 0 {
            return (0, false);
        }
        let overflow_bucket_idx = snapshot.bucket_counts.len().saturating_sub(1);
        let clamped = percentile.clamp(0.0, 1.0);
        let target = ((total as f64) * clamped).ceil() as u64;
        let mut cumulative = 0u64;
        for (idx, count) in snapshot.bucket_counts.iter().copied().enumerate() {
            cumulative = cumulative.saturating_add(count);
            if cumulative >= target.max(1) {
                let value_ns = snapshot
                    .bucket_upper_bounds_ns
                    .get(idx)
                    .copied()
                    .unwrap_or_else(|| {
                        snapshot.bucket_upper_bounds_ns.last().copied().unwrap_or(0)
                    });
                return (value_ns, idx == overflow_bucket_idx);
            }
        }
        (
            snapshot.bucket_upper_bounds_ns.last().copied().unwrap_or(0),
            snapshot.bucket_counts[overflow_bucket_idx] > 0,
        )
    }

    fn duration_ns_u64(duration: Duration) -> u64 {
        duration.as_nanos().min(u64::MAX as u128) as u64
    }

    fn observe_latency_counter(
        calls: &AtomicU64,
        total_ns: &AtomicU64,
        max_ns: &AtomicU64,
        histogram: &LatencyHistogram,
        elapsed_ns: u64,
    ) {
        calls.fetch_add(1, Ordering::Relaxed);
        total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
        histogram.observe_ns(elapsed_ns);
    }

    fn build_fuse_io_stats(
        calls: u64,
        bytes: u64,
        errors: u64,
        total_ns: u64,
        max_ns: u64,
        elapsed: Duration,
    ) -> VfsFuseIoStats {
        VfsFuseIoStats {
            calls,
            bytes,
            avg_bytes: if calls == 0 {
                0.0
            } else {
                bytes as f64 / calls as f64
            },
            errors,
            total_ms: total_ns as f64 / 1_000_000.0,
            avg_ms: if calls == 0 {
                0.0
            } else {
                (total_ns as f64 / calls as f64) / 1_000_000.0
            },
            max_ms: max_ns as f64 / 1_000_000.0,
            requested_mib_per_sec: Self::per_second(bytes, elapsed) / (1024.0 * 1024.0),
        }
    }

    fn build_stage_latency_stats(
        calls: u64,
        total_ns: u64,
        max_ns: u64,
        histogram: LatencyHistogramSnapshot,
        elapsed: Duration,
    ) -> VfsWritePathStageLatencyStats {
        let (p50_ns, p50_saturated) = Self::histogram_percentile_ns(&histogram, 0.50);
        let (p95_ns, p95_saturated) = Self::histogram_percentile_ns(&histogram, 0.95);
        let (p99_ns, p99_saturated) = Self::histogram_percentile_ns(&histogram, 0.99);
        VfsWritePathStageLatencyStats {
            calls,
            calls_per_sec: Self::per_second(calls, elapsed),
            total_ms: total_ns as f64 / 1_000_000.0,
            avg_ms: if calls == 0 {
                0.0
            } else {
                (total_ns as f64 / calls as f64) / 1_000_000.0
            },
            max_ms: max_ns as f64 / 1_000_000.0,
            latency_quantiles_ms: VfsWritePathLatencyQuantilesMs {
                p50: p50_ns as f64 / 1_000_000.0,
                p95: p95_ns as f64 / 1_000_000.0,
                p99: p99_ns as f64 / 1_000_000.0,
            },
            latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated {
                p50: p50_saturated,
                p95: p95_saturated,
                p99: p99_saturated,
            },
            latency_histogram: VfsLatencyHistogramSnapshot {
                bucket_upper_bounds_ns: histogram.bucket_upper_bounds_ns.clone(),
                bucket_counts: histogram.bucket_counts.clone(),
            },
        }
    }

    fn build_cadence_stats(
        events: u64,
        interval_total_ns: u64,
        interval_max_ns: u64,
        histogram: LatencyHistogramSnapshot,
        elapsed: Duration,
    ) -> VfsWritePathCadenceStats {
        let (p50_ns, p50_saturated) = Self::histogram_percentile_ns(&histogram, 0.50);
        let (p95_ns, p95_saturated) = Self::histogram_percentile_ns(&histogram, 0.95);
        let (p99_ns, p99_saturated) = Self::histogram_percentile_ns(&histogram, 0.99);
        let completed_intervals = events.saturating_sub(1);
        VfsWritePathCadenceStats {
            events,
            events_per_sec: Self::per_second(events, elapsed),
            avg_interval_ms: if completed_intervals == 0 {
                0.0
            } else {
                (interval_total_ns as f64 / completed_intervals as f64) / 1_000_000.0
            },
            max_interval_ms: interval_max_ns as f64 / 1_000_000.0,
            interval_quantiles_ms: VfsWritePathLatencyQuantilesMs {
                p50: p50_ns as f64 / 1_000_000.0,
                p95: p95_ns as f64 / 1_000_000.0,
                p99: p99_ns as f64 / 1_000_000.0,
            },
            interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated {
                p50: p50_saturated,
                p95: p95_saturated,
                p99: p99_saturated,
            },
            interval_histogram: VfsLatencyHistogramSnapshot {
                bucket_upper_bounds_ns: histogram.bucket_upper_bounds_ns.clone(),
                bucket_counts: histogram.bucket_counts.clone(),
            },
        }
    }

    fn record_sync_data_queue_wait(&self, queue_wait: Duration) {
        let elapsed_ns = Self::duration_ns_u64(queue_wait);
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.payload_store_sync_data_queue_wait_calls,
            &metrics.payload_store_sync_data_queue_wait_total_ns,
            &metrics.payload_store_sync_data_queue_wait_max_ns,
            &metrics.payload_store_sync_data_queue_wait_histogram,
            elapsed_ns,
        );
    }

    fn record_completion_interval(
        &self,
        last_completed_ns: &AtomicU64,
        interval_total_ns: &AtomicU64,
        interval_max_ns: &AtomicU64,
        interval_histogram: &LatencyHistogram,
    ) {
        let completed_ns = Self::duration_ns_u64(self.write_path_started_at.elapsed());
        let previous_completed_ns = last_completed_ns.swap(completed_ns, Ordering::Relaxed);
        if previous_completed_ns == 0 || completed_ns <= previous_completed_ns {
            return;
        }
        let interval_ns = completed_ns.saturating_sub(previous_completed_ns);
        interval_total_ns.fetch_add(interval_ns, Ordering::Relaxed);
        interval_max_ns.fetch_max(interval_ns, Ordering::Relaxed);
        interval_histogram.observe_ns(interval_ns);
    }

    fn note_completed_cadence_event(
        &self,
        event_count: &AtomicU64,
        last_completed_ns: &AtomicU64,
        interval_total_ns: &AtomicU64,
        interval_max_ns: &AtomicU64,
        interval_histogram: &LatencyHistogram,
    ) {
        event_count.fetch_add(1, Ordering::Relaxed);
        self.record_completion_interval(
            last_completed_ns,
            interval_total_ns,
            interval_max_ns,
            interval_histogram,
        );
    }

    fn record_sync_data_interval(&self) {
        let metrics = &self.state.metrics;
        self.record_completion_interval(
            &metrics.payload_store_sync_data_last_completed_ns,
            &metrics.payload_store_sync_data_interval_total_ns,
            &metrics.payload_store_sync_data_interval_max_ns,
            &metrics.payload_store_sync_data_interval_histogram,
        );
    }

    /// Record payload-persist observability metrics for the current burst.
    pub(super) fn record_payload_persist_observability(
        &self,
        burst_payloads: usize,
        burst_bytes: u64,
        elapsed: Duration,
        completed_burst: bool,
    ) {
        let elapsed_ns = Self::duration_ns_u64(elapsed);
        let burst_payloads_u64 = u64::try_from(burst_payloads).unwrap_or(u64::MAX);
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.payload_persist_calls,
            &metrics.payload_persist_total_ns,
            &metrics.payload_persist_max_ns,
            &metrics.payload_persist_histogram,
            elapsed_ns,
        );
        if !completed_burst {
            return;
        }
        metrics
            .payload_burst_total_payloads
            .fetch_add(burst_payloads_u64, Ordering::Relaxed);
        metrics
            .payload_burst_total_bytes
            .fetch_add(burst_bytes, Ordering::Relaxed);
        metrics
            .payload_burst_max_payloads
            .fetch_max(burst_payloads_u64, Ordering::Relaxed);
        metrics
            .payload_burst_max_bytes
            .fetch_max(burst_bytes, Ordering::Relaxed);
        self.note_completed_cadence_event(
            &metrics.payload_burst_count,
            &metrics.payload_burst_last_completed_ns,
            &metrics.payload_burst_interval_total_ns,
            &metrics.payload_burst_interval_max_ns,
            &metrics.payload_burst_interval_histogram,
        );
    }

    /// Record metadata-commit observability metrics and cadence events.
    pub(super) fn record_metadata_commit_observability(
        &self,
        elapsed: Duration,
        completed_commit: bool,
    ) {
        let elapsed_ns = Self::duration_ns_u64(elapsed);
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.metadata_commit_calls,
            &metrics.metadata_commit_total_ns,
            &metrics.metadata_commit_max_ns,
            &metrics.metadata_commit_histogram,
            elapsed_ns,
        );
        if !completed_commit {
            return;
        }
        self.note_completed_cadence_event(
            &metrics.metadata_commit_completed_calls,
            &metrics.metadata_commit_last_completed_ns,
            &metrics.metadata_commit_interval_total_ns,
            &metrics.metadata_commit_interval_max_ns,
            &metrics.metadata_commit_interval_histogram,
        );
    }

    fn record_local_cache_io_timing(&self, timing: LocalCacheIoTiming) {
        self.state
            .metrics
            .local_cache_io_calls
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .local_cache_io_handoff_total_ns
            .fetch_add(timing.handoff_ns, Ordering::Relaxed);
        self.state
            .metrics
            .local_cache_io_handoff_max_ns
            .fetch_max(timing.handoff_ns, Ordering::Relaxed);
        self.state
            .metrics
            .local_cache_io_handoff_histogram
            .observe_ns(timing.handoff_ns);
        self.state
            .metrics
            .local_cache_io_backend_total_ns
            .fetch_add(timing.backend_io_ns, Ordering::Relaxed);
        self.state
            .metrics
            .local_cache_io_backend_max_ns
            .fetch_max(timing.backend_io_ns, Ordering::Relaxed);
        self.state
            .metrics
            .local_cache_io_backend_histogram
            .observe_ns(timing.backend_io_ns);
    }

    #[inline]
    fn record_foreground_local_payload_read(&self, bytes: u64, backend_io_ns: u64) {
        self.state
            .read
            .foreground_local_payload_reads
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .read
            .foreground_local_payload_read_bytes
            .fetch_add(bytes, Ordering::Relaxed);
        self.state
            .read
            .foreground_local_payload_read_io_ns
            .fetch_add(backend_io_ns, Ordering::Relaxed);
        let max_ns = &self.state.read.foreground_local_payload_read_io_max_ns;
        // This is a best-effort statistic: a stale relaxed read may only cause a
        // redundant RMW, while `fetch_max` still preserves the maximum value.
        if backend_io_ns > max_ns.load(Ordering::Relaxed) {
            max_ns.fetch_max(backend_io_ns, Ordering::Relaxed);
        }
    }

    fn write_access_requested(flags: OpenFlags, trunc: bool) -> bool {
        trunc
            || flags.contains(OpenFlags::WRONLY)
            || flags.contains(OpenFlags::RDWR)
            || flags.contains(OpenFlags::APPEND)
    }

    fn active_open_overlay_required(flags: OpenFlags, trunc: bool) -> bool {
        Self::write_access_requested(flags, trunc)
    }

    fn open_flags_allow_write(flags: OpenFlags) -> bool {
        flags.contains(OpenFlags::WRONLY)
            || flags.contains(OpenFlags::RDWR)
            || flags.contains(OpenFlags::APPEND)
    }

    fn metadata_authority_status(&self) -> MetadataAuthorityStatus {
        #[cfg(test)]
        if let Some(status) = *self.metadata_authority_override.lock() {
            return status;
        }

        self.meta.sync_view().metadata_authority_status()
    }

    /// Ensure current metadata authority allows mutating operations for an inode.
    pub(super) fn enforce_metadata_mutation_authority(&self, ino: Ino) -> VfsResult<()> {
        if self.metadata_authority_status().allows_new_mutations() {
            return Ok(());
        }
        Err(VfsError::Meta(MetaError::AccessDenied {
            ino,
            principal: "metadata-authority".to_string(),
            required: AccessLevel::ReadWrite,
            actual: AccessLevel::ReadOnly,
        }))
    }

    fn known_file_lock_lease_snapshot(&self, ino: Ino) -> Option<FileLockLease> {
        if let Some(lease) = self.known_file_lock_overrides.lock().get(&ino).cloned() {
            return Some(lease);
        }

        let _ = self.meta.sync_view().ensure_known_file_lock_monitoring();
        self.meta.sync_view().known_file_lock_lease(ino)
    }

    fn known_hard_lock_blocks_new_mutation(&self, ino: Ino) -> Option<FileLockLease> {
        let lease = self.known_file_lock_lease_snapshot(ino)?;
        let self_client_id = self.meta.sync_view().invalidation_sender_id();
        let self_exclusive_owner = self_client_id
            .as_deref()
            .is_some_and(|client_id| lease.exclusive_owner.as_deref() == Some(client_id));

        if lease.kind == HardLockKind::Exclusive && self_exclusive_owner {
            None
        } else {
            Some(lease)
        }
    }

    /// Reject write operations when a known foreign hard lock is present.
    pub(super) fn enforce_no_known_foreign_hard_lock(&self, ino: Ino) -> VfsResult<()> {
        if self.known_hard_lock_blocks_new_mutation(ino).is_some() {
            return Err(VfsError::WouldBlock);
        }
        Ok(())
    }

    /// Verify handle access allows write for a specific inode and return open-file state.
    pub(super) fn enforce_mutating_handle_access(&self, fh: u64, ino: Ino) -> VfsResult<OpenFile> {
        let denied = || {
            VfsError::Meta(MetaError::AccessDenied {
                ino,
                principal: format!("handle:{fh}"),
                required: AccessLevel::ReadWrite,
                actual: AccessLevel::ReadOnly,
            })
        };

        let open_file = self.handle_table.get(fh).ok_or_else(denied)?;
        if open_file.ino != ino {
            return Err(denied());
        }
        if Self::open_flags_allow_write(open_file.flags) {
            return Ok(open_file);
        }
        Err(denied())
    }

    pub(super) fn is_inode_not_found_error(error: &VfsError, ino: Ino) -> bool {
        matches!(error, VfsError::Meta(MetaError::InodeNotFound(missing)) if *missing == ino)
    }

    /// Verify a mutating handle can still write even if a foreign unlink removed
    /// namespace metadata after the handle was opened.
    pub(super) fn enforce_write_access_for_handle(&self, fh: u64, ino: Ino) -> VfsResult<OpenFile> {
        let open_file = self.enforce_mutating_handle_access(fh, ino)?;
        match self.enforce_access(ino, AccessLevel::ReadWrite) {
            Ok(()) => Ok(open_file),
            Err(error)
                if open_file.active_open_overlay && Self::is_inode_not_found_error(&error, ino) =>
            {
                self.handle_table.mark_orphan(ino);
                Ok(open_file)
            }
            Err(error) => Err(error),
        }
    }

    /// Test-only override for known file lock lease.
    #[doc(hidden)]
    pub fn set_known_file_lock_for_test(&self, ino: Ino, lease: Option<FileLockLease>) {
        let mut overrides = self.known_file_lock_overrides.lock();
        if let Some(lease) = lease {
            overrides.insert(ino, lease);
        } else {
            overrides.remove(&ino);
        }
    }

    #[cfg(test)]
    /// Test-only override for metadata authority status.
    #[doc(hidden)]
    pub fn set_metadata_authority_status_for_test(&self, status: Option<MetadataAuthorityStatus>) {
        *self.metadata_authority_override.lock() = status;
    }

    /// Record a WinFSP write callback metric.
    pub fn note_winfsp_write_callback(&self) {
        self.state
            .metrics
            .winfsp_write_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record WinFSP write callback observability including bytes, policy and latency.
    pub fn record_winfsp_write_callback_observability(
        &self,
        write_bytes: usize,
        local_sync_policy: LocalSyncPolicy,
        callback_elapsed: Duration,
        vfs_write_elapsed: Duration,
        post_write_getattr_elapsed: Duration,
    ) {
        fn observe_stage_latency(
            total_ns: &AtomicU64,
            max_ns: &AtomicU64,
            histogram: &LatencyHistogram,
            elapsed_ns: u64,
        ) {
            total_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
            max_ns.fetch_max(elapsed_ns, Ordering::Relaxed);
            histogram.observe_ns(elapsed_ns);
        }

        let metrics = &self.state.metrics;
        metrics
            .winfsp_write_callback_bytes
            .fetch_add(write_bytes as u64, Ordering::Relaxed);
        match local_sync_policy {
            LocalSyncPolicy::Strict => {
                metrics
                    .winfsp_write_callback_strict_calls
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .winfsp_write_callback_strict_bytes
                    .fetch_add(write_bytes as u64, Ordering::Relaxed);
            }
            LocalSyncPolicy::WriteThrough => {
                metrics
                    .winfsp_write_callback_write_through_calls
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .winfsp_write_callback_write_through_bytes
                    .fetch_add(write_bytes as u64, Ordering::Relaxed);
            }
            LocalSyncPolicy::Opportunistic => {
                metrics
                    .winfsp_write_callback_opportunistic_calls
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .winfsp_write_callback_opportunistic_bytes
                    .fetch_add(write_bytes as u64, Ordering::Relaxed);
            }
        }
        observe_stage_latency(
            &metrics.winfsp_write_callback_total_ns,
            &metrics.winfsp_write_callback_max_ns,
            &metrics.winfsp_write_callback_histogram,
            Self::duration_ns_u64(callback_elapsed),
        );
        observe_stage_latency(
            &metrics.winfsp_write_callback_vfs_total_ns,
            &metrics.winfsp_write_callback_vfs_max_ns,
            &metrics.winfsp_write_callback_vfs_histogram,
            Self::duration_ns_u64(vfs_write_elapsed),
        );
        observe_stage_latency(
            &metrics.winfsp_write_callback_getattr_total_ns,
            &metrics.winfsp_write_callback_getattr_max_ns,
            &metrics.winfsp_write_callback_getattr_histogram,
            Self::duration_ns_u64(post_write_getattr_elapsed),
        );
    }

    /// Record WinFSP read callback bytes and callback wall time.
    pub fn record_winfsp_read_callback_observability(
        &self,
        read_bytes: u32,
        callback_elapsed: Duration,
    ) {
        let metrics = &self.state.metrics;
        let elapsed_ns = Self::duration_ns_u64(callback_elapsed);
        metrics
            .winfsp_read_callbacks
            .fetch_add(1, Ordering::Relaxed);
        metrics
            .winfsp_read_callback_bytes
            .fetch_add(u64::from(read_bytes), Ordering::Relaxed);
        metrics
            .winfsp_read_callback_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        metrics
            .winfsp_read_callback_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    /// Record VFS write call counters and byte totals.
    pub(super) fn note_vfs_write_call(&self, write_bytes: usize) {
        self.state
            .metrics
            .vfs_write_calls
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .vfs_write_bytes
            .fetch_add(write_bytes as u64, Ordering::Relaxed);
    }

    /// Record VFS read call counters and requested byte totals.
    pub(super) fn note_vfs_read_call(&self, requested_bytes: usize) {
        self.state
            .metrics
            .vfs_read_calls
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .vfs_read_requested_bytes
            .fetch_add(requested_bytes as u64, Ordering::Relaxed);
    }

    /// Record FUSE read callback observability.
    pub fn record_fuse_read_callback(
        &self,
        requested_bytes: usize,
        callback_elapsed: Duration,
        errored: bool,
    ) {
        let metrics = &self.state.metrics;
        metrics.fuse_read_callbacks.fetch_add(1, Ordering::Relaxed);
        metrics
            .fuse_read_callback_bytes
            .fetch_add(requested_bytes as u64, Ordering::Relaxed);
        if errored {
            metrics
                .fuse_read_callback_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_ns = Self::duration_ns_u64(callback_elapsed);
        metrics
            .fuse_read_callback_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        metrics
            .fuse_read_callback_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    /// Record a macFUSE open-cache decision for mounted diagnostics.
    pub fn record_macfuse_open_cache_decision(&self, decision: VfsMacFuseOpenCacheDecision) {
        let metrics = &self.state.metrics;
        // Local-stable counters refine the aggregate keep/drop/seed totals. The
        // aggregate remains the canonical summary, while the subtype explains
        // which decisions happened before remote publication.
        match decision {
            VfsMacFuseOpenCacheDecision::ReadOnlyKeep => {
                metrics
                    .macfuse_open_cache_read_only_keep
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableKeep => {
                metrics
                    .macfuse_open_cache_read_only_keep
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .macfuse_open_cache_read_only_local_stable_keep
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyUnknownState => {
                metrics
                    .macfuse_open_cache_read_only_unknown_state
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyNotRegularFile => {
                metrics
                    .macfuse_open_cache_read_only_not_regular_file
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyLocalDirtyState => {
                metrics
                    .macfuse_open_cache_read_only_local_dirty_state
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyActiveWriter => {
                metrics
                    .macfuse_open_cache_read_only_active_writer
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyGenerationChanged => {
                metrics
                    .macfuse_open_cache_read_only_generation_changed
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableGenerationChanged => {
                metrics
                    .macfuse_open_cache_read_only_generation_changed
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .macfuse_open_cache_read_only_local_stable_generation_changed
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::WriteCapable => {
                metrics
                    .macfuse_open_cache_write_capable
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::CleanWriteReleaseSeeded => {
                metrics
                    .macfuse_open_cache_clean_write_release_seeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            VfsMacFuseOpenCacheDecision::LocalStableWriteReleaseSeeded => {
                metrics
                    .macfuse_open_cache_clean_write_release_seeded
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .macfuse_open_cache_local_stable_write_release_seeded
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record FUSE write callback observability.
    pub fn record_fuse_write_callback(
        &self,
        requested_bytes: usize,
        callback_elapsed: Duration,
        errored: bool,
    ) {
        let metrics = &self.state.metrics;
        metrics.fuse_write_callbacks.fetch_add(1, Ordering::Relaxed);
        metrics
            .fuse_write_callback_bytes
            .fetch_add(requested_bytes as u64, Ordering::Relaxed);
        if errored {
            metrics
                .fuse_write_callback_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_ns = Self::duration_ns_u64(callback_elapsed);
        metrics
            .fuse_write_callback_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        metrics
            .fuse_write_callback_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    /// Replace the latest async FUSE write bridge diagnostics snapshot.
    pub fn record_fuse_async_write_snapshot(&self, snapshot: VfsFuseAsyncWriteSnapshot) {
        let mut raw = self.state.fuse_async_write_raw_snapshot.lock();
        if snapshot.snapshot_generation < raw.snapshot_generation {
            return;
        }
        *raw = snapshot.clone();
        drop(raw);

        let baseline = self.state.fuse_async_write_reset_baseline.lock().clone();
        let snapshot = Self::apply_fuse_async_write_reset_baseline(snapshot, &baseline);
        let mut current = self.state.fuse_async_write_snapshot.lock();
        if snapshot.snapshot_generation >= current.snapshot_generation {
            current.snapshot_generation = snapshot.snapshot_generation;
            current.enabled = snapshot.enabled;
            current.linux_fuse_profile = snapshot.linux_fuse_profile;
            current.fuse_max_write_bytes = snapshot.fuse_max_write_bytes;
            current.fuse_max_readahead_bytes = snapshot.fuse_max_readahead_bytes;
            current.fuse_max_background = snapshot.fuse_max_background;
            current.fuse_congestion_threshold = snapshot.fuse_congestion_threshold;
            current.max_pending_bytes = snapshot.max_pending_bytes;
            current.max_pending_writes = snapshot.max_pending_writes;
            current.per_handle_pending_bytes = snapshot.per_handle_pending_bytes;
            current.pending_writes = snapshot.pending_writes;
            current.pending_bytes = snapshot.pending_bytes;
            current.peak_pending_bytes = snapshot.peak_pending_bytes;
            current.completed_writes = snapshot.completed_writes;
            current.failed_writes = snapshot.failed_writes;
            current.task_failed_writes = snapshot.task_failed_writes;
            current.admission_waits = snapshot.admission_waits;
            current.admission_wait_total_ms = snapshot.admission_wait_total_ms;
            current.barrier_waits = snapshot.barrier_waits;
            current.barrier_wait_total_ms = snapshot.barrier_wait_total_ms;
        }
    }

    /// Replace the latest async FUSE read bridge diagnostics snapshot.
    pub fn record_fuse_async_read_snapshot(&self, snapshot: VfsFuseAsyncReadSnapshot) {
        *self.state.fuse_async_read_snapshot.lock() = snapshot;
    }

    fn apply_fuse_async_write_reset_baseline(
        mut snapshot: VfsFuseAsyncWriteSnapshot,
        baseline: &VfsFuseAsyncWriteSnapshot,
    ) -> VfsFuseAsyncWriteSnapshot {
        if snapshot.snapshot_generation < baseline.snapshot_generation {
            return snapshot;
        }
        snapshot.peak_pending_bytes = snapshot
            .peak_pending_bytes
            .saturating_sub(baseline.peak_pending_bytes)
            .max(snapshot.pending_bytes);
        snapshot.completed_writes = snapshot
            .completed_writes
            .saturating_sub(baseline.completed_writes);
        snapshot.failed_writes = snapshot
            .failed_writes
            .saturating_sub(baseline.failed_writes);
        snapshot.task_failed_writes = snapshot
            .task_failed_writes
            .saturating_sub(baseline.task_failed_writes);
        snapshot.admission_waits = snapshot
            .admission_waits
            .saturating_sub(baseline.admission_waits);
        snapshot.admission_wait_total_ms = Self::subtract_fuse_async_write_baseline_ms(
            snapshot.admission_wait_total_ms,
            baseline.admission_wait_total_ms,
        );
        snapshot.barrier_waits = snapshot
            .barrier_waits
            .saturating_sub(baseline.barrier_waits);
        snapshot.barrier_wait_total_ms = Self::subtract_fuse_async_write_baseline_ms(
            snapshot.barrier_wait_total_ms,
            baseline.barrier_wait_total_ms,
        );
        snapshot
    }

    fn subtract_fuse_async_write_baseline_ms(value: f64, baseline: f64) -> f64 {
        if value.is_finite() && baseline.is_finite() {
            (value - baseline).max(0.0)
        } else {
            value
        }
    }

    /// Record write-path stage latency for recording writer stack state.
    pub(super) fn record_vfs_write_record_writer_stack_state(&self, elapsed: Duration) {
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.vfs_write_record_writer_stack_state_calls,
            &metrics.vfs_write_record_writer_stack_state_total_ns,
            &metrics.vfs_write_record_writer_stack_state_max_ns,
            &metrics.vfs_write_record_writer_stack_state_histogram,
            Self::duration_ns_u64(elapsed),
        );
    }

    /// Record write-path stage latency for completed materialization.
    pub(super) fn record_vfs_write_materialize_completed(&self, elapsed: Duration) {
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.vfs_write_materialize_completed_calls,
            &metrics.vfs_write_materialize_completed_total_ns,
            &metrics.vfs_write_materialize_completed_max_ns,
            &metrics.vfs_write_materialize_completed_histogram,
            Self::duration_ns_u64(elapsed),
        );
    }

    /// Record write-path stage latency for strict materialization.
    pub(super) fn record_vfs_write_materialize_strict(&self, elapsed: Duration) {
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.vfs_write_materialize_strict_calls,
            &metrics.vfs_write_materialize_strict_total_ns,
            &metrics.vfs_write_materialize_strict_max_ns,
            &metrics.vfs_write_materialize_strict_histogram,
            Self::duration_ns_u64(elapsed),
        );
    }

    /// Record write-path stage latency for limit-related materialization.
    pub(super) fn record_vfs_write_materialize_for_limits(&self, elapsed: Duration) {
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.vfs_write_materialize_for_limits_calls,
            &metrics.vfs_write_materialize_for_limits_total_ns,
            &metrics.vfs_write_materialize_for_limits_max_ns,
            &metrics.vfs_write_materialize_for_limits_histogram,
            Self::duration_ns_u64(elapsed),
        );
    }

    /// Record a WinFSP overwrite callback metric.
    pub fn note_winfsp_overwrite_callback(&self) {
        self.state
            .metrics
            .winfsp_overwrite_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WinFSP cleanup callback metric.
    pub fn note_winfsp_cleanup_callback(&self) {
        self.state
            .metrics
            .winfsp_cleanup_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WinFSP flush callback metric.
    pub fn note_winfsp_flush_callback(&self) {
        self.state
            .metrics
            .winfsp_flush_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WinFSP close callback metric.
    pub fn note_winfsp_close_callback(&self) {
        self.state
            .metrics
            .winfsp_close_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a WinFSP cleanup+flush file-handle callback metric.
    pub fn note_winfsp_cleanup_flush_fh_callback(&self) {
        self.state
            .metrics
            .winfsp_cleanup_flush_fh_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record whether a WinFSP open returned with Windows file cache enabled.
    pub fn note_winfsp_os_cache_open(&self, disable_os_cache: bool) {
        let counter = if disable_os_cache {
            &self.state.metrics.winfsp_os_cache_disabled_open
        } else {
            &self.state.metrics.winfsp_os_cache_enabled_open
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a read callback on a handle that allowed Windows file cache.
    pub fn note_winfsp_os_cache_miss_read(&self, bytes: u32) {
        self.state
            .metrics
            .winfsp_os_cache_miss_read_callbacks
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .winfsp_os_cache_miss_read_bytes
            .fetch_add(u64::from(bytes), Ordering::Relaxed);
    }

    fn note_winfsp_barrier_outcome(&self, barrier_type: WinfspBarrierType, flushed: bool) {
        let metrics = &self.state.metrics;
        let (calls, skipped_clean) = match barrier_type {
            WinfspBarrierType::Cleanup => (
                &metrics.winfsp_cleanup_barrier_calls,
                &metrics.winfsp_cleanup_barrier_skipped_clean,
            ),
            WinfspBarrierType::Flush => (
                &metrics.winfsp_flush_barrier_calls,
                &metrics.winfsp_flush_barrier_skipped_clean,
            ),
            WinfspBarrierType::Close => (
                &metrics.winfsp_close_barrier_calls,
                &metrics.winfsp_close_barrier_skipped_clean,
            ),
        };

        if flushed {
            calls.fetch_add(1, Ordering::Relaxed);
        } else {
            skipped_clean.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record WinFSP cleanup barrier outcome for observability counters.
    pub fn note_winfsp_cleanup_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Cleanup, flushed);
    }

    /// Record WinFSP flush barrier outcome for observability counters.
    pub fn note_winfsp_flush_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Flush, flushed);
    }

    /// Record WinFSP close barrier outcome for observability counters.
    pub fn note_winfsp_close_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Close, flushed);
    }

    /// Returns true when the inode still has local dirty state that requires a
    /// flush barrier before close/flush callback completion.
    pub fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.has_pending_local_dirty_payloads_for_inode(ino)
            || self.state.namespace.dirty_attrs.lock().contains_key(&ino)
            || self.state.writer_stack.has_dirty_writers(ino)
    }

    /// Snapshot write-path observability counters and metrics.
    pub fn write_path_observability_snapshot(&self) -> VfsWritePathObservabilitySnapshot {
        let elapsed = self.write_path_started_at.elapsed();
        macro_rules! load_metric {
            ($field:ident) => {
                self.state.metrics.$field.load(Ordering::Relaxed)
            };
        }
        macro_rules! load_metrics {
            ($($name:ident : $field:ident),+ $(,)?) => {
                $(let $name = load_metric!($field);)+
            };
        }
        load_metrics!(
            flush_barrier_calls: flush_barrier_calls,
            local_cache_io_calls: local_cache_io_calls,
            local_cache_handoff_total_ns: local_cache_io_handoff_total_ns,
            local_cache_handoff_max_ns: local_cache_io_handoff_max_ns,
            local_cache_backend_total_ns: local_cache_io_backend_total_ns,
            local_cache_backend_max_ns: local_cache_io_backend_max_ns,
            vfs_read_calls: vfs_read_calls,
            vfs_read_requested_bytes: vfs_read_requested_bytes,
            vfs_write_calls: vfs_write_calls,
            vfs_write_bytes: vfs_write_bytes,
            vfs_write_record_writer_stack_state_calls: vfs_write_record_writer_stack_state_calls,
            vfs_write_record_writer_stack_state_total_ns: vfs_write_record_writer_stack_state_total_ns,
            vfs_write_record_writer_stack_state_max_ns: vfs_write_record_writer_stack_state_max_ns,
            vfs_write_materialize_completed_calls: vfs_write_materialize_completed_calls,
            vfs_write_materialize_completed_total_ns: vfs_write_materialize_completed_total_ns,
            vfs_write_materialize_completed_max_ns: vfs_write_materialize_completed_max_ns,
            vfs_write_materialize_strict_calls: vfs_write_materialize_strict_calls,
            vfs_write_materialize_strict_total_ns: vfs_write_materialize_strict_total_ns,
            vfs_write_materialize_strict_max_ns: vfs_write_materialize_strict_max_ns,
            vfs_write_materialize_for_limits_calls: vfs_write_materialize_for_limits_calls,
            vfs_write_materialize_for_limits_total_ns: vfs_write_materialize_for_limits_total_ns,
            vfs_write_materialize_for_limits_max_ns: vfs_write_materialize_for_limits_max_ns,
            winfsp_write_callback_bytes: winfsp_write_callback_bytes,
            winfsp_write_callback_total_ns: winfsp_write_callback_total_ns,
            winfsp_write_callback_max_ns: winfsp_write_callback_max_ns,
            winfsp_write_callback_vfs_total_ns: winfsp_write_callback_vfs_total_ns,
            winfsp_write_callback_vfs_max_ns: winfsp_write_callback_vfs_max_ns,
            winfsp_write_callback_getattr_total_ns: winfsp_write_callback_getattr_total_ns,
            winfsp_write_callback_getattr_max_ns: winfsp_write_callback_getattr_max_ns,
            winfsp_read_callbacks: winfsp_read_callbacks,
            winfsp_read_callback_bytes: winfsp_read_callback_bytes,
            winfsp_read_callback_total_ns: winfsp_read_callback_total_ns,
            fuse_read_callbacks: fuse_read_callbacks,
            fuse_read_callback_bytes: fuse_read_callback_bytes,
            fuse_read_callback_errors: fuse_read_callback_errors,
            fuse_read_callback_total_ns: fuse_read_callback_total_ns,
            fuse_read_callback_max_ns: fuse_read_callback_max_ns,
            fuse_write_callbacks: fuse_write_callbacks,
            fuse_write_callback_bytes: fuse_write_callback_bytes,
            fuse_write_callback_errors: fuse_write_callback_errors,
            fuse_write_callback_total_ns: fuse_write_callback_total_ns,
            fuse_write_callback_max_ns: fuse_write_callback_max_ns,
            macfuse_open_cache_read_only_keep: macfuse_open_cache_read_only_keep,
            macfuse_open_cache_read_only_local_stable_keep:
                macfuse_open_cache_read_only_local_stable_keep,
            macfuse_open_cache_read_only_unknown_state: macfuse_open_cache_read_only_unknown_state,
            macfuse_open_cache_read_only_not_regular_file: macfuse_open_cache_read_only_not_regular_file,
            macfuse_open_cache_read_only_local_dirty_state: macfuse_open_cache_read_only_local_dirty_state,
            macfuse_open_cache_read_only_active_writer:
                macfuse_open_cache_read_only_active_writer,
            macfuse_open_cache_read_only_generation_changed: macfuse_open_cache_read_only_generation_changed,
            macfuse_open_cache_read_only_local_stable_generation_changed:
                macfuse_open_cache_read_only_local_stable_generation_changed,
            macfuse_open_cache_write_capable: macfuse_open_cache_write_capable,
            macfuse_open_cache_clean_write_release_seeded:
                macfuse_open_cache_clean_write_release_seeded,
            macfuse_open_cache_local_stable_write_release_seeded:
                macfuse_open_cache_local_stable_write_release_seeded,
            sync_calls: payload_store_sync_data_calls,
            sync_deferred_calls: payload_store_sync_data_deferred_calls,
            sync_bytes: payload_store_sync_data_synced_bytes,
            sync_total_ns: payload_store_sync_data_total_ns,
            sync_max_ns: payload_store_sync_data_max_ns,
            sync_queue_wait_calls: payload_store_sync_data_queue_wait_calls,
            sync_queue_wait_total_ns: payload_store_sync_data_queue_wait_total_ns,
            sync_queue_wait_max_ns: payload_store_sync_data_queue_wait_max_ns,
            sync_interval_total_ns: payload_store_sync_data_interval_total_ns,
            sync_interval_max_ns: payload_store_sync_data_interval_max_ns,
            payload_persist_calls: payload_persist_calls,
            payload_persist_total_ns: payload_persist_total_ns,
            payload_persist_max_ns: payload_persist_max_ns,
            payload_burst_count: payload_burst_count,
            payload_burst_total_payloads: payload_burst_total_payloads,
            payload_burst_total_bytes: payload_burst_total_bytes,
            payload_burst_max_payloads: payload_burst_max_payloads,
            payload_burst_max_bytes: payload_burst_max_bytes,
            payload_burst_interval_total_ns: payload_burst_interval_total_ns,
            payload_burst_interval_max_ns: payload_burst_interval_max_ns,
            metadata_commit_calls: metadata_commit_calls,
            metadata_commit_total_ns: metadata_commit_total_ns,
            metadata_commit_max_ns: metadata_commit_max_ns,
            metadata_commit_completed_calls: metadata_commit_completed_calls,
            metadata_commit_interval_total_ns: metadata_commit_interval_total_ns,
            metadata_commit_interval_max_ns: metadata_commit_interval_max_ns,
            eager_flush_checks: eager_flush_checks,
            eager_flush_triggered: eager_flush_triggered,
            eager_flush_skipped_empty: eager_flush_skipped_empty,
            eager_flush_skipped_below_threshold: eager_flush_skipped_below_threshold,
        );
        let local_cache_handoff_histogram = self
            .state
            .metrics
            .local_cache_io_handoff_histogram
            .snapshot();
        let local_cache_backend_histogram = self
            .state
            .metrics
            .local_cache_io_backend_histogram
            .snapshot();
        let sync_histogram = self
            .state
            .metrics
            .payload_store_sync_data_histogram
            .snapshot();
        let sync_queue_wait_histogram = self
            .state
            .metrics
            .payload_store_sync_data_queue_wait_histogram
            .snapshot();
        let sync_interval_histogram = self
            .state
            .metrics
            .payload_store_sync_data_interval_histogram
            .snapshot();
        let vfs_write_record_writer_stack_state_histogram = self
            .state
            .metrics
            .vfs_write_record_writer_stack_state_histogram
            .snapshot();
        let vfs_write_materialize_completed_histogram = self
            .state
            .metrics
            .vfs_write_materialize_completed_histogram
            .snapshot();
        let vfs_write_materialize_strict_histogram = self
            .state
            .metrics
            .vfs_write_materialize_strict_histogram
            .snapshot();
        let vfs_write_materialize_for_limits_histogram = self
            .state
            .metrics
            .vfs_write_materialize_for_limits_histogram
            .snapshot();
        let payload_persist_histogram = self.state.metrics.payload_persist_histogram.snapshot();
        let payload_burst_interval_histogram = self
            .state
            .metrics
            .payload_burst_interval_histogram
            .snapshot();
        let metadata_commit_histogram = self.state.metrics.metadata_commit_histogram.snapshot();
        let metadata_commit_interval_histogram = self
            .state
            .metrics
            .metadata_commit_interval_histogram
            .snapshot();
        let winfsp_write_callback_histogram = self
            .state
            .metrics
            .winfsp_write_callback_histogram
            .snapshot();
        let winfsp_write_callback_vfs_histogram = self
            .state
            .metrics
            .winfsp_write_callback_vfs_histogram
            .snapshot();
        let winfsp_write_callback_getattr_histogram = self
            .state
            .metrics
            .winfsp_write_callback_getattr_histogram
            .snapshot();

        let (p50_ns, p50_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.50);
        let (p95_ns, p95_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.95);
        let (p99_ns, p99_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.99);
        let (sync_queue_wait_p50_ns, sync_queue_wait_p50_saturated) =
            Self::histogram_percentile_ns(&sync_queue_wait_histogram, 0.50);
        let (sync_queue_wait_p95_ns, sync_queue_wait_p95_saturated) =
            Self::histogram_percentile_ns(&sync_queue_wait_histogram, 0.95);
        let (sync_queue_wait_p99_ns, sync_queue_wait_p99_saturated) =
            Self::histogram_percentile_ns(&sync_queue_wait_histogram, 0.99);
        let (sync_interval_p50_ns, sync_interval_p50_saturated) =
            Self::histogram_percentile_ns(&sync_interval_histogram, 0.50);
        let (sync_interval_p95_ns, sync_interval_p95_saturated) =
            Self::histogram_percentile_ns(&sync_interval_histogram, 0.95);
        let (sync_interval_p99_ns, sync_interval_p99_saturated) =
            Self::histogram_percentile_ns(&sync_interval_histogram, 0.99);
        let synced_gib = sync_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let calls_per_gib = if synced_gib > 0.0 {
            sync_calls as f64 / synced_gib
        } else {
            0.0
        };
        let sync_interval_samples: u64 = sync_interval_histogram.bucket_counts.iter().sum();
        let payload_burst_interval_samples: u64 =
            payload_burst_interval_histogram.bucket_counts.iter().sum();

        let landing_zone_stats = self.landing_zone.stats();
        let drain_lag_current_bytes = self.total_landed_write_drain_lag_bytes();
        let write_admission_stats = self.write_admission_governor_stats();
        let landing_zone_syncs = landing_zone_stats
            .data_syncs
            .saturating_add(landing_zone_stats.manifest_syncs);
        let pending_local_dirty_bytes = self.total_pending_local_dirty_bytes();
        let pending_persisted_slice_bytes = self.total_pending_persisted_local_dirty_slice_bytes();
        let (dirty_slice_count, frozen_slice_count) = self.pending_local_dirty_slice_counts();
        let writer_stack_dirty_bytes = self.state.writer_stack.total_dirty_bytes();
        let total_dirty_bytes = pending_local_dirty_bytes.saturating_add(writer_stack_dirty_bytes);
        let upload_stats = self.upload_stats();
        let cache_pipeline_metrics = self.cached_store.cache_pipeline_metrics();
        let fetch_metrics = &self.fetch_scheduler.metrics;
        let fetch_cache_hits = fetch_metrics.cache_hits.load(Ordering::Relaxed);
        let fetch_cache_hit_bytes = fetch_metrics.cache_hit_bytes.load(Ordering::Relaxed);
        let fetch_s3_fetches = fetch_metrics.s3_fetches.load(Ordering::Relaxed);
        let fetch_s3_errors = fetch_metrics.s3_errors.load(Ordering::Relaxed);
        let fetch_singleflight_joins = fetch_metrics.singleflight_joins.load(Ordering::Relaxed);
        let fetch_retries = fetch_metrics.retries.load(Ordering::Relaxed);
        let fetch_bytes_downloaded = fetch_metrics.bytes_downloaded.load(Ordering::Relaxed);
        let fetch_encrypted_objects_read =
            fetch_metrics.encrypted_objects_read.load(Ordering::Relaxed);
        let fetch_encrypted_bytes_read = fetch_metrics.encrypted_bytes_read.load(Ordering::Relaxed);
        let fetch_encrypted_object_read_failures = fetch_metrics
            .encrypted_object_read_failures
            .load(Ordering::Relaxed);
        let fetch_malformed_encrypted_objects = fetch_metrics
            .malformed_encrypted_objects
            .load(Ordering::Relaxed);
        let fetch_prefetch_skips = fetch_metrics.prefetch_skips.load(Ordering::Relaxed);
        let fetch_local_cache_io_calls = fetch_metrics.local_cache_io_calls.load(Ordering::Relaxed);
        let fetch_local_cache_handoff_total_ns = fetch_metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        let fetch_local_cache_handoff_max_ns = fetch_metrics
            .local_cache_io_handoff_max_ns
            .load(Ordering::Relaxed);
        let fetch_local_cache_backend_total_ns = fetch_metrics
            .local_cache_io_backend_total_ns
            .load(Ordering::Relaxed);
        let fetch_local_cache_backend_max_ns = fetch_metrics
            .local_cache_io_backend_max_ns
            .load(Ordering::Relaxed);
        let fetch_s3_get_calls = fetch_metrics.s3_get_calls.load(Ordering::Relaxed);
        let fetch_s3_get_total_ns = fetch_metrics.s3_get_total_ns.load(Ordering::Relaxed);
        let fetch_s3_get_max_ns = fetch_metrics.s3_get_max_ns.load(Ordering::Relaxed);
        let fetch_object_decode_calls = fetch_metrics.object_decode_calls.load(Ordering::Relaxed);
        let fetch_object_decode_total_ns =
            fetch_metrics.object_decode_total_ns.load(Ordering::Relaxed);
        let fetch_object_decode_max_ns = fetch_metrics.object_decode_max_ns.load(Ordering::Relaxed);
        let fetch_assemble_copy_calls = fetch_metrics.assemble_copy_calls.load(Ordering::Relaxed);
        let fetch_assemble_copy_total_ns =
            fetch_metrics.assemble_copy_total_ns.load(Ordering::Relaxed);
        let fetch_assemble_copy_max_ns = fetch_metrics.assemble_copy_max_ns.load(Ordering::Relaxed);
        let fetch_local_cache_put_calls =
            fetch_metrics.local_cache_put_calls.load(Ordering::Relaxed);
        let fetch_local_cache_put_total_ns = fetch_metrics
            .local_cache_put_total_ns
            .load(Ordering::Relaxed);
        let fetch_local_cache_put_max_ns =
            fetch_metrics.local_cache_put_max_ns.load(Ordering::Relaxed);
        let fetch_total_calls = fetch_metrics.fetch_total_calls.load(Ordering::Relaxed);
        let fetch_total_ns = fetch_metrics.fetch_total_ns.load(Ordering::Relaxed);
        let fetch_total_max_ns = fetch_metrics.fetch_total_max_ns.load(Ordering::Relaxed);
        let pending_upload_bytes =
            usize::try_from(upload_stats.pending_upload_bytes).unwrap_or(usize::MAX);
        let storage_stats = self.local_payload_backend.storage_stats();
        let cache_pressure_bytes = usize::try_from(
            storage_stats
                .current_bytes
                .saturating_sub(storage_stats.evictable_bytes),
        )
        .unwrap_or(usize::MAX);
        let total_accounted_bytes = total_dirty_bytes
            .saturating_add(pending_persisted_slice_bytes)
            .saturating_add(pending_upload_bytes);
        let dirty_peak_bytes = {
            let current = total_dirty_bytes as u64;
            let previous = self
                .state
                .metrics
                .dirty_pressure_peak_bytes
                .fetch_max(current, Ordering::Relaxed);
            usize::try_from(previous.max(current)).unwrap_or(usize::MAX)
        };
        let staged_peak_bytes = {
            let current = pending_persisted_slice_bytes as u64;
            let previous = self
                .state
                .metrics
                .staged_pressure_peak_bytes
                .fetch_max(current, Ordering::Relaxed);
            usize::try_from(previous.max(current)).unwrap_or(usize::MAX)
        };
        VfsWritePathObservabilitySnapshot {
            publisher_pid: std::process::id(),
            generated_unix_ms: now_unix_millis(),
            uptime_secs: elapsed.as_secs_f64(),
            pending_uploads: self.pending_upload_count(),
            backend_tag: self.write_path_backend_tag.clone(),
            flush_barrier_calls,
            flush_barrier_calls_per_sec: Self::per_second(flush_barrier_calls, elapsed),
            payload_store_sync_data: VfsWritePathSyncDataStats {
                calls: sync_calls,
                calls_per_sec: Self::per_second(sync_calls, elapsed),
                deferred_calls: sync_deferred_calls,
                synced_bytes: sync_bytes,
                synced_gib,
                calls_per_gib,
                total_ms: sync_total_ns as f64 / 1_000_000.0,
                avg_ms: if sync_calls == 0 {
                    0.0
                } else {
                    (sync_total_ns as f64 / sync_calls as f64) / 1_000_000.0
                },
                max_ms: sync_max_ns as f64 / 1_000_000.0,
                latency_quantiles_ms: VfsWritePathLatencyQuantilesMs {
                    p50: p50_ns as f64 / 1_000_000.0,
                    p95: p95_ns as f64 / 1_000_000.0,
                    p99: p99_ns as f64 / 1_000_000.0,
                },
                latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated {
                    p50: p50_saturated,
                    p95: p95_saturated,
                    p99: p99_saturated,
                },
                latency_histogram: VfsLatencyHistogramSnapshot {
                    bucket_upper_bounds_ns: sync_histogram.bucket_upper_bounds_ns.clone(),
                    bucket_counts: sync_histogram.bucket_counts.clone(),
                },
                queue_wait_total_ms: sync_queue_wait_total_ns as f64 / 1_000_000.0,
                queue_wait_avg_ms: if sync_queue_wait_calls == 0 {
                    0.0
                } else {
                    (sync_queue_wait_total_ns as f64 / sync_queue_wait_calls as f64) / 1_000_000.0
                },
                queue_wait_max_ms: sync_queue_wait_max_ns as f64 / 1_000_000.0,
                queue_wait_latency_quantiles_ms: VfsWritePathLatencyQuantilesMs {
                    p50: sync_queue_wait_p50_ns as f64 / 1_000_000.0,
                    p95: sync_queue_wait_p95_ns as f64 / 1_000_000.0,
                    p99: sync_queue_wait_p99_ns as f64 / 1_000_000.0,
                },
                queue_wait_latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated {
                    p50: sync_queue_wait_p50_saturated,
                    p95: sync_queue_wait_p95_saturated,
                    p99: sync_queue_wait_p99_saturated,
                },
                queue_wait_latency_histogram: VfsLatencyHistogramSnapshot {
                    bucket_upper_bounds_ns: sync_queue_wait_histogram
                        .bucket_upper_bounds_ns
                        .clone(),
                    bucket_counts: sync_queue_wait_histogram.bucket_counts.clone(),
                },
                barrier_interval_avg_ms: if sync_interval_samples == 0 {
                    0.0
                } else {
                    (sync_interval_total_ns as f64 / sync_interval_samples as f64) / 1_000_000.0
                },
                barrier_interval_max_ms: sync_interval_max_ns as f64 / 1_000_000.0,
                barrier_interval_quantiles_ms: VfsWritePathLatencyQuantilesMs {
                    p50: sync_interval_p50_ns as f64 / 1_000_000.0,
                    p95: sync_interval_p95_ns as f64 / 1_000_000.0,
                    p99: sync_interval_p99_ns as f64 / 1_000_000.0,
                },
                barrier_interval_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated {
                    p50: sync_interval_p50_saturated,
                    p95: sync_interval_p95_saturated,
                    p99: sync_interval_p99_saturated,
                },
                barrier_interval_histogram: VfsLatencyHistogramSnapshot {
                    bucket_upper_bounds_ns: sync_interval_histogram.bucket_upper_bounds_ns.clone(),
                    bucket_counts: sync_interval_histogram.bucket_counts.clone(),
                },
            },
            local_cache_io: VfsWritePathLocalCacheIoStats {
                execution_model: cache_io_execution_model().as_str().to_string(),
                calls: local_cache_io_calls,
                handoff_total_ms: local_cache_handoff_total_ns as f64 / 1_000_000.0,
                handoff_avg_ms: if local_cache_io_calls == 0 {
                    0.0
                } else {
                    (local_cache_handoff_total_ns as f64 / local_cache_io_calls as f64)
                        / 1_000_000.0
                },
                handoff_max_ms: local_cache_handoff_max_ns as f64 / 1_000_000.0,
                backend_io_total_ms: local_cache_backend_total_ns as f64 / 1_000_000.0,
                backend_io_avg_ms: if local_cache_io_calls == 0 {
                    0.0
                } else {
                    (local_cache_backend_total_ns as f64 / local_cache_io_calls as f64)
                        / 1_000_000.0
                },
                backend_io_max_ms: local_cache_backend_max_ns as f64 / 1_000_000.0,
                handoff_histogram: VfsLatencyHistogramSnapshot {
                    bucket_upper_bounds_ns: local_cache_handoff_histogram
                        .bucket_upper_bounds_ns
                        .clone(),
                    bucket_counts: local_cache_handoff_histogram.bucket_counts.clone(),
                },
                backend_io_histogram: VfsLatencyHistogramSnapshot {
                    bucket_upper_bounds_ns: local_cache_backend_histogram
                        .bucket_upper_bounds_ns
                        .clone(),
                    bucket_counts: local_cache_backend_histogram.bucket_counts.clone(),
                },
            },
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                read: winfsp_read_callbacks,
                read_bytes: winfsp_read_callback_bytes,
                read_total_ns: winfsp_read_callback_total_ns,
                write: self
                    .state
                    .metrics
                    .winfsp_write_callbacks
                    .load(Ordering::Relaxed),
                overwrite: self
                    .state
                    .metrics
                    .winfsp_overwrite_callbacks
                    .load(Ordering::Relaxed),
                cleanup: self
                    .state
                    .metrics
                    .winfsp_cleanup_callbacks
                    .load(Ordering::Relaxed),
                flush: self
                    .state
                    .metrics
                    .winfsp_flush_callbacks
                    .load(Ordering::Relaxed),
                close: self
                    .state
                    .metrics
                    .winfsp_close_callbacks
                    .load(Ordering::Relaxed),
                cleanup_flush_fh: self
                    .state
                    .metrics
                    .winfsp_cleanup_flush_fh_callbacks
                    .load(Ordering::Relaxed),
                os_cache_enabled_open: self
                    .state
                    .metrics
                    .winfsp_os_cache_enabled_open
                    .load(Ordering::Relaxed),
                os_cache_disabled_open: self
                    .state
                    .metrics
                    .winfsp_os_cache_disabled_open
                    .load(Ordering::Relaxed),
                os_cache_miss_read_callbacks: self
                    .state
                    .metrics
                    .winfsp_os_cache_miss_read_callbacks
                    .load(Ordering::Relaxed),
                os_cache_miss_read_bytes: self
                    .state
                    .metrics
                    .winfsp_os_cache_miss_read_bytes
                    .load(Ordering::Relaxed),
            },
            winfsp_write_path: VfsWritePathWinfspWritePathStats {
                write_bytes: winfsp_write_callback_bytes,
                avg_write_bytes: if self
                    .state
                    .metrics
                    .winfsp_write_callbacks
                    .load(Ordering::Relaxed)
                    == 0
                {
                    0.0
                } else {
                    winfsp_write_callback_bytes as f64
                        / self
                            .state
                            .metrics
                            .winfsp_write_callbacks
                            .load(Ordering::Relaxed) as f64
                },
                strict_calls: self
                    .state
                    .metrics
                    .winfsp_write_callback_strict_calls
                    .load(Ordering::Relaxed),
                strict_bytes: self
                    .state
                    .metrics
                    .winfsp_write_callback_strict_bytes
                    .load(Ordering::Relaxed),
                write_through_calls: self
                    .state
                    .metrics
                    .winfsp_write_callback_write_through_calls
                    .load(Ordering::Relaxed),
                write_through_bytes: self
                    .state
                    .metrics
                    .winfsp_write_callback_write_through_bytes
                    .load(Ordering::Relaxed),
                opportunistic_calls: self
                    .state
                    .metrics
                    .winfsp_write_callback_opportunistic_calls
                    .load(Ordering::Relaxed),
                opportunistic_bytes: self
                    .state
                    .metrics
                    .winfsp_write_callback_opportunistic_bytes
                    .load(Ordering::Relaxed),
                callback: Self::build_stage_latency_stats(
                    self.state
                        .metrics
                        .winfsp_write_callbacks
                        .load(Ordering::Relaxed),
                    winfsp_write_callback_total_ns,
                    winfsp_write_callback_max_ns,
                    winfsp_write_callback_histogram,
                    elapsed,
                ),
                vfs_write: Self::build_stage_latency_stats(
                    self.state
                        .metrics
                        .winfsp_write_callbacks
                        .load(Ordering::Relaxed),
                    winfsp_write_callback_vfs_total_ns,
                    winfsp_write_callback_vfs_max_ns,
                    winfsp_write_callback_vfs_histogram,
                    elapsed,
                ),
                post_write_getattr: Self::build_stage_latency_stats(
                    self.state
                        .metrics
                        .winfsp_write_callbacks
                        .load(Ordering::Relaxed),
                    winfsp_write_callback_getattr_total_ns,
                    winfsp_write_callback_getattr_max_ns,
                    winfsp_write_callback_getattr_histogram,
                    elapsed,
                ),
            },
            vfs_write_path: VfsWritePathVfsWriteStats {
                calls: vfs_write_calls,
                write_bytes: vfs_write_bytes,
                avg_write_bytes: if vfs_write_calls == 0 {
                    0.0
                } else {
                    vfs_write_bytes as f64 / vfs_write_calls as f64
                },
                record_writer_stack_state: Self::build_stage_latency_stats(
                    vfs_write_record_writer_stack_state_calls,
                    vfs_write_record_writer_stack_state_total_ns,
                    vfs_write_record_writer_stack_state_max_ns,
                    vfs_write_record_writer_stack_state_histogram,
                    elapsed,
                ),
                materialize_completed: Self::build_stage_latency_stats(
                    vfs_write_materialize_completed_calls,
                    vfs_write_materialize_completed_total_ns,
                    vfs_write_materialize_completed_max_ns,
                    vfs_write_materialize_completed_histogram,
                    elapsed,
                ),
                materialize_strict: Self::build_stage_latency_stats(
                    vfs_write_materialize_strict_calls,
                    vfs_write_materialize_strict_total_ns,
                    vfs_write_materialize_strict_max_ns,
                    vfs_write_materialize_strict_histogram,
                    elapsed,
                ),
                materialize_for_limits: Self::build_stage_latency_stats(
                    vfs_write_materialize_for_limits_calls,
                    vfs_write_materialize_for_limits_total_ns,
                    vfs_write_materialize_for_limits_max_ns,
                    vfs_write_materialize_for_limits_histogram,
                    elapsed,
                ),
            },
            vfs_read_path: VfsReadPathStats {
                calls: vfs_read_calls,
                requested_bytes: vfs_read_requested_bytes,
                avg_requested_bytes: if vfs_read_calls == 0 {
                    0.0
                } else {
                    vfs_read_requested_bytes as f64 / vfs_read_calls as f64
                },
                foreground_local_payload_reads: self
                    .state
                    .read
                    .foreground_local_payload_reads
                    .load(Ordering::Relaxed),
                foreground_local_payload_read_bytes: self
                    .state
                    .read
                    .foreground_local_payload_read_bytes
                    .load(Ordering::Relaxed),
            },
            fuse_callbacks: VfsFuseCallbackStats {
                read: Self::build_fuse_io_stats(
                    fuse_read_callbacks,
                    fuse_read_callback_bytes,
                    fuse_read_callback_errors,
                    fuse_read_callback_total_ns,
                    fuse_read_callback_max_ns,
                    elapsed,
                ),
                write: Self::build_fuse_io_stats(
                    fuse_write_callbacks,
                    fuse_write_callback_bytes,
                    fuse_write_callback_errors,
                    fuse_write_callback_total_ns,
                    fuse_write_callback_max_ns,
                    elapsed,
                ),
            },
            macfuse_open_cache: {
                let read_only_drop = macfuse_open_cache_read_only_unknown_state
                    .saturating_add(macfuse_open_cache_read_only_not_regular_file)
                    .saturating_add(macfuse_open_cache_read_only_local_dirty_state)
                    .saturating_add(macfuse_open_cache_read_only_active_writer)
                    .saturating_add(macfuse_open_cache_read_only_generation_changed);
                VfsMacFuseOpenCacheStats {
                    total: macfuse_open_cache_read_only_keep
                        .saturating_add(read_only_drop)
                        .saturating_add(macfuse_open_cache_write_capable),
                    read_only_keep: macfuse_open_cache_read_only_keep,
                    read_only_local_stable_keep: macfuse_open_cache_read_only_local_stable_keep,
                    read_only_drop,
                    read_only_unknown_state: macfuse_open_cache_read_only_unknown_state,
                    read_only_not_regular_file: macfuse_open_cache_read_only_not_regular_file,
                    read_only_not_fully_committed: 0,
                    read_only_local_dirty_state: macfuse_open_cache_read_only_local_dirty_state,
                    read_only_active_writer: macfuse_open_cache_read_only_active_writer,
                    read_only_generation_changed: macfuse_open_cache_read_only_generation_changed,
                    read_only_local_stable_generation_changed:
                        macfuse_open_cache_read_only_local_stable_generation_changed,
                    write_capable: macfuse_open_cache_write_capable,
                    clean_write_release_seeded: macfuse_open_cache_clean_write_release_seeded,
                    local_stable_write_release_seeded:
                        macfuse_open_cache_local_stable_write_release_seeded,
                }
            },
            fuse_async_write: self.state.fuse_async_write_snapshot.lock().clone(),
            fuse_async_read: self.state.fuse_async_read_snapshot.lock().clone(),
            landing_zone: VfsWritePathLandingZoneStats {
                accepted_writes: landing_zone_stats.accepted_writes,
                accepted_bytes: landing_zone_stats.accepted_bytes,
                accepted_mib_per_sec: Self::per_second(landing_zone_stats.accepted_bytes, elapsed)
                    / (1024.0 * 1024.0),
                recoverable_segments: landing_zone_stats.recoverable_segments,
                accept_total_ms: landing_zone_stats.accept_total_ns as f64 / 1_000_000.0,
                accept_avg_ms: if landing_zone_stats.accepted_writes == 0 {
                    0.0
                } else {
                    (landing_zone_stats.accept_total_ns as f64
                        / landing_zone_stats.accepted_writes as f64)
                        / 1_000_000.0
                },
                accept_max_ms: landing_zone_stats.accept_max_ns as f64 / 1_000_000.0,
                data_syncs: landing_zone_stats.data_syncs,
                manifest_syncs: landing_zone_stats.manifest_syncs,
                manifest_rewrites: landing_zone_stats.manifest_rewrites,
                sync_total_ms: landing_zone_stats.sync_total_ns as f64 / 1_000_000.0,
                sync_avg_ms: if landing_zone_syncs == 0 {
                    0.0
                } else {
                    (landing_zone_stats.sync_total_ns as f64 / landing_zone_syncs as f64)
                        / 1_000_000.0
                },
                sync_max_ms: landing_zone_stats.sync_max_ns as f64 / 1_000_000.0,
            },
            drain_lag: VfsWritePathDrainLagStats {
                current_bytes: drain_lag_current_bytes,
                steady_quota_bytes: self.writer_stack_steady_drain_quota_bytes(),
                soft_pacing_events: self
                    .state
                    .metrics
                    .drain_lag_pacing_events
                    .load(Ordering::Relaxed),
                soft_pacing_total_ms: self
                    .state
                    .metrics
                    .drain_lag_pacing_total_ms
                    .load(Ordering::Relaxed),
                soft_pacing_max_ms: self
                    .state
                    .metrics
                    .drain_lag_pacing_max_ms
                    .load(Ordering::Relaxed),
                admission_ingress_bytes_per_sec: write_admission_stats.ingress_bytes_per_sec,
                admission_drain_bytes_per_sec: write_admission_stats.drain_bytes_per_sec,
                admission_runway_ms: write_admission_stats.runway_ms,
                admission_last_delay_ms: write_admission_stats.last_delay_ms,
                admission_pressure_bytes: write_admission_stats.pressure_bytes,
                admission_budget_bytes: write_admission_stats.budget_bytes,
                local_sync_unsynced_bytes: self.local_sync_unsynced_bytes(),
            },
            winfsp_foreground_flushes: VfsWritePathWinfspForegroundFlushStats {
                cleanup: self
                    .state
                    .metrics
                    .winfsp_cleanup_barrier_calls
                    .load(Ordering::Relaxed),
                flush: self
                    .state
                    .metrics
                    .winfsp_flush_barrier_calls
                    .load(Ordering::Relaxed),
                close: self
                    .state
                    .metrics
                    .winfsp_close_barrier_calls
                    .load(Ordering::Relaxed),
                cleanup_skipped_clean: self
                    .state
                    .metrics
                    .winfsp_cleanup_barrier_skipped_clean
                    .load(Ordering::Relaxed),
                flush_skipped_clean: self
                    .state
                    .metrics
                    .winfsp_flush_barrier_skipped_clean
                    .load(Ordering::Relaxed),
                close_skipped_clean: self
                    .state
                    .metrics
                    .winfsp_close_barrier_skipped_clean
                    .load(Ordering::Relaxed),
            },
            write_backpressure: VfsWritePathBackpressureStats {
                events: self
                    .state
                    .local_dirty
                    .write_backpressure_events
                    .load(Ordering::Relaxed),
                wait_total_ms: self
                    .state
                    .local_dirty
                    .write_backpressure_wait_ms
                    .load(Ordering::Relaxed),
                stall_max_ms: self
                    .state
                    .local_dirty
                    .fast_backpressure_stall_max_ms
                    .load(Ordering::Relaxed),
                soft_throttle_events: self
                    .state
                    .local_dirty
                    .fast_backpressure_events
                    .load(Ordering::Relaxed),
                hard_events: self
                    .state
                    .local_dirty
                    .hard_backpressure_events
                    .load(Ordering::Relaxed),
            },
            dirty_pressure: VfsWritePathDirtyPressureStats {
                pending_local_dirty_bytes,
                pending_persisted_slice_bytes,
                dirty_slice_count,
                frozen_slice_count,
                writer_stack_dirty_bytes,
                total_dirty_bytes,
                dirty_limit_bytes: self.max_dirty_bytes(),
                pending_upload_bytes,
                cache_pressure_bytes,
                total_accounted_bytes,
                dirty_peak_bytes,
                staged_peak_bytes,
            },
            upload: upload_stats,
            fetch: VfsFetchStats {
                cache_hits: fetch_cache_hits,
                cache_hit_bytes: fetch_cache_hit_bytes,
                s3_fetches: fetch_s3_fetches,
                s3_errors: fetch_s3_errors,
                singleflight_joins: fetch_singleflight_joins,
                retries: fetch_retries,
                bytes_downloaded: fetch_bytes_downloaded,
                encrypted_objects_read: fetch_encrypted_objects_read,
                encrypted_bytes_read: fetch_encrypted_bytes_read,
                encrypted_object_read_failures: fetch_encrypted_object_read_failures,
                malformed_encrypted_objects: fetch_malformed_encrypted_objects,
                prefetch_skips: fetch_prefetch_skips,
                local_cache_io_calls: fetch_local_cache_io_calls,
                local_cache_handoff_total_ms: fetch_local_cache_handoff_total_ns as f64
                    / 1_000_000.0,
                local_cache_handoff_avg_ms: if fetch_local_cache_io_calls == 0 {
                    0.0
                } else {
                    (fetch_local_cache_handoff_total_ns as f64 / fetch_local_cache_io_calls as f64)
                        / 1_000_000.0
                },
                local_cache_handoff_max_ms: fetch_local_cache_handoff_max_ns as f64 / 1_000_000.0,
                local_cache_backend_io_total_ms: fetch_local_cache_backend_total_ns as f64
                    / 1_000_000.0,
                local_cache_backend_io_avg_ms: if fetch_local_cache_io_calls == 0 {
                    0.0
                } else {
                    (fetch_local_cache_backend_total_ns as f64 / fetch_local_cache_io_calls as f64)
                        / 1_000_000.0
                },
                local_cache_backend_io_max_ms: fetch_local_cache_backend_max_ns as f64
                    / 1_000_000.0,
                s3_get_calls: fetch_s3_get_calls,
                s3_get_total_ms: fetch_s3_get_total_ns as f64 / 1_000_000.0,
                s3_get_avg_ms: if fetch_s3_get_calls == 0 {
                    0.0
                } else {
                    (fetch_s3_get_total_ns as f64 / fetch_s3_get_calls as f64) / 1_000_000.0
                },
                s3_get_max_ms: fetch_s3_get_max_ns as f64 / 1_000_000.0,
                object_decode_calls: fetch_object_decode_calls,
                object_decode_total_ms: fetch_object_decode_total_ns as f64 / 1_000_000.0,
                object_decode_avg_ms: if fetch_object_decode_calls == 0 {
                    0.0
                } else {
                    (fetch_object_decode_total_ns as f64 / fetch_object_decode_calls as f64)
                        / 1_000_000.0
                },
                object_decode_max_ms: fetch_object_decode_max_ns as f64 / 1_000_000.0,
                assemble_copy_calls: fetch_assemble_copy_calls,
                assemble_copy_total_ms: fetch_assemble_copy_total_ns as f64 / 1_000_000.0,
                assemble_copy_avg_ms: if fetch_assemble_copy_calls == 0 {
                    0.0
                } else {
                    (fetch_assemble_copy_total_ns as f64 / fetch_assemble_copy_calls as f64)
                        / 1_000_000.0
                },
                assemble_copy_max_ms: fetch_assemble_copy_max_ns as f64 / 1_000_000.0,
                local_cache_put_calls: fetch_local_cache_put_calls,
                local_cache_put_total_ms: fetch_local_cache_put_total_ns as f64 / 1_000_000.0,
                local_cache_put_avg_ms: if fetch_local_cache_put_calls == 0 {
                    0.0
                } else {
                    (fetch_local_cache_put_total_ns as f64 / fetch_local_cache_put_calls as f64)
                        / 1_000_000.0
                },
                local_cache_put_max_ms: fetch_local_cache_put_max_ns as f64 / 1_000_000.0,
                fetch_total_calls,
                fetch_total_ms: fetch_total_ns as f64 / 1_000_000.0,
                fetch_total_avg_ms: if fetch_total_calls == 0 {
                    0.0
                } else {
                    (fetch_total_ns as f64 / fetch_total_calls as f64) / 1_000_000.0
                },
                fetch_total_max_ms: fetch_total_max_ns as f64 / 1_000_000.0,
            },
            cache_pipeline: VfsCachePipelineStats {
                materialize: VfsCachePipelineMaterializeStats {
                    batches: cache_pipeline_metrics.materialize_batches.batches,
                    payloads: cache_pipeline_metrics.materialize_batches.payloads,
                    bytes: cache_pipeline_metrics.materialize_batches.bytes,
                    inflight_batches: cache_pipeline_metrics.materialize_batches.inflight_batches,
                    inflight_batches_peak: cache_pipeline_metrics
                        .materialize_batches
                        .inflight_batches_peak,
                },
            },
            payload_burst: VfsWritePathBurstStats {
                bursts: payload_burst_count,
                bursts_per_sec: Self::per_second(payload_burst_count, elapsed),
                total_payloads: payload_burst_total_payloads,
                total_bytes: payload_burst_total_bytes,
                avg_payloads_per_burst: if payload_burst_count == 0 {
                    0.0
                } else {
                    payload_burst_total_payloads as f64 / payload_burst_count as f64
                },
                avg_bytes_per_burst: if payload_burst_count == 0 {
                    0.0
                } else {
                    payload_burst_total_bytes as f64 / payload_burst_count as f64
                },
                max_payloads_per_burst: payload_burst_max_payloads,
                max_bytes_per_burst: payload_burst_max_bytes,
                avg_interval_ms: if payload_burst_interval_samples == 0 {
                    0.0
                } else {
                    (payload_burst_interval_total_ns as f64 / payload_burst_interval_samples as f64)
                        / 1_000_000.0
                },
                max_interval_ms: payload_burst_interval_max_ns as f64 / 1_000_000.0,
            },
            payload_persist: Self::build_stage_latency_stats(
                payload_persist_calls,
                payload_persist_total_ns,
                payload_persist_max_ns,
                payload_persist_histogram,
                elapsed,
            ),
            metadata_commit: Self::build_stage_latency_stats(
                metadata_commit_calls,
                metadata_commit_total_ns,
                metadata_commit_max_ns,
                metadata_commit_histogram,
                elapsed,
            ),
            metadata_commit_cadence: Self::build_cadence_stats(
                metadata_commit_completed_calls,
                metadata_commit_interval_total_ns,
                metadata_commit_interval_max_ns,
                metadata_commit_interval_histogram,
                elapsed,
            ),
            eager_flush: VfsWritePathEagerFlushStats {
                checks: eager_flush_checks,
                triggered: eager_flush_triggered,
                skipped_empty: eager_flush_skipped_empty,
                skipped_below_threshold: eager_flush_skipped_below_threshold,
            },
            tuning: VfsWritePathTuningStats {
                eager_flush_threshold_bytes: self.write_eager_flush_batch_bytes,
                eager_flush_drain_chunk_bytes: self.write_eager_flush_drain_chunk_bytes,
                payload_put_concurrency: self.payload_put_concurrency,
                sync_group_min_bytes: self.local_sync_group_min_bytes,
            },
            allocator: None,
        }
    }

    #[cfg(test)]
    /// Test-only helper to recover persisted landing-zone segments.
    pub(crate) fn recover_landing_zone_segments(
        &self,
    ) -> std::io::Result<Vec<RecoveredLandingSegment>> {
        self.landing_zone.recover()
    }

    /// Describe whether a new locally materialized payload would still fit in
    /// the in-memory dirty buffer or would hit local backpressure.
    pub fn ingress_pressure_for(&self, next_bytes: usize) -> VfsIngressPressure {
        let dirty_mem_bytes = self.total_pending_write_buffer_bytes();
        let dirty_mem_limit = self.max_dirty_bytes.load(Ordering::Acquire);
        let lane = if dirty_mem_bytes == 0
            || dirty_mem_bytes.saturating_add(next_bytes) <= dirty_mem_limit
        {
            VfsIngressLane::DirtyMem
        } else {
            VfsIngressLane::Backpressure
        };

        VfsIngressPressure {
            next_bytes,
            dirty_mem_bytes,
            dirty_mem_limit,
            lane,
        }
    }

    // =========================================================================
    // File Handle Operations
    // =========================================================================

    /// Open a file and allocate a file handle.
    ///
    /// Called by FUSE `open()` / WinFSP `open()`.
    /// If `O_TRUNC` is requested (`trunc == true`), the file is truncated to 0.
    pub fn open_file(&self, ino: Ino, flags: OpenFlags, trunc: bool) -> VfsResult<u64> {
        let required_access = if Self::write_access_requested(flags, trunc) {
            AccessLevel::ReadWrite
        } else {
            AccessLevel::ReadOnly
        };
        self.enforce_access(ino, required_access)?;
        if Self::write_access_requested(flags, trunc) {
            self.enforce_no_known_foreign_hard_lock(ino)?;
        }
        if let Some(ignored_attr) = self.ignored_entry_attr(ino) {
            let active_open_overlay = Self::active_open_overlay_required(flags, trunc);
            let open_file =
                self.handle_table
                    .open(ino, flags, Some(ignored_attr), active_open_overlay);
            let fh = open_file.fh;
            if active_open_overlay {
                self.sync_meta().note_active_open_inode(ino);
            }
            self.state.file_session.note_open_handle(ino);
            self.state
                .read
                .increment_active_access_class(open_file.current_access_class());
            debug!(
                "open_file ignored ino={} fh={} flags={:#x}",
                ino,
                fh,
                flags.bits()
            );
            return Ok(fh);
        }

        // Verify the inode exists
        let read_context = ReadRequestContext::from_attr(self, ino, self.getattr(ino)?);
        if read_context.placeholder_visible(self, ino) {
            self.debug_pending_inode_state(ino, "open_blocked_remote_content_pending");
            return Err(VfsError::RemoteContentPending);
        }

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let active_open_overlay = Self::active_open_overlay_required(flags, trunc);
        let open_file = self.handle_table.open(
            ino,
            flags,
            Some(read_context.attr.clone()),
            active_open_overlay,
        );
        let fh = open_file.fh;
        if active_open_overlay {
            self.sync_meta().note_active_open_inode(ino);
        }
        self.state.file_session.note_open_handle(ino);
        self.state
            .read
            .increment_active_access_class(open_file.current_access_class());
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());
        self.debug_pending_inode_state(ino, "open_file");

        // Eager prefetch: when opening a regular file for reading, schedule
        // background prefetch of the first block to hide initial S3 latency.
        if read_context.attr.kind == studiofs_meta::types::FileType::RegularFile
            && read_context.attr.size > 0
            && (flags.contains(OpenFlags::RDONLY) || flags.contains(OpenFlags::RDWR))
        {
            let _ = self
                .prefetch_tx
                .send(PrefetchWorkItem::Batch(PrefetchBatch::single(
                    ino,
                    0,
                    Some(read_context.prefetch_context()),
                )));
        }

        Ok(fh)
    }
    // =========================================================================
    // Background Tasks
    // =========================================================================
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests;
