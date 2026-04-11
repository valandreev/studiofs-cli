use serde::{Deserialize, Serialize};
use std::time::Duration;
use studiofs_meta::types::Ino;

use crate::vfs::{
    DEFAULT_ATTR_READ_CACHE_BYTES, DEFAULT_ATTR_READ_CACHE_CAPACITY, DEFAULT_DENTRY_CACHE_BYTES,
    DEFAULT_DENTRY_CACHE_CAPACITY, DEFAULT_DIR_PREFETCH_MAX_BATCH,
    DEFAULT_EFFECTIVE_ACL_CACHE_CAPACITY, DEFAULT_FETCH_DECREASE_COOLDOWN_SECS,
    DEFAULT_FETCH_DECREASE_FACTOR, DEFAULT_FETCH_INCREASE_STEP, DEFAULT_FETCH_INITIAL_WINDOW,
    DEFAULT_FETCH_MAX_ATTEMPTS, DEFAULT_FETCH_MAX_WINDOW, DEFAULT_FETCH_MIN_WINDOW,
    DEFAULT_FETCH_PREFETCH_ACQUIRE_WAIT_MS, DEFAULT_FETCH_PREFETCH_RESERVE,
    DEFAULT_FETCH_REQUEST_TIMEOUT_SECS, DEFAULT_FETCH_RETRY_BASE_MS, DEFAULT_FETCH_RETRY_MAX_MS,
    DEFAULT_INGRESS_CHECKPOINT_INTERVAL_SECS, DEFAULT_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS,
    DEFAULT_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG, DEFAULT_INGRESS_CHECKPOINT_MIN_APPEND_BYTES,
    DEFAULT_INGRESS_CHECKPOINT_MIN_MUTATIONS, DEFAULT_INGRESS_SEGMENT_MAX_BYTES,
    DEFAULT_JANITOR_INTERVAL, DEFAULT_JANITOR_LEASE_TTL, DEFAULT_JANITOR_MAX_DELETES_PER_RUN,
    DEFAULT_JANITOR_SKIP_NEWER_THAN, DEFAULT_LOCK_LEASE_TTL, DEFAULT_LOCK_RETRY_BASE_MS,
    DEFAULT_LOCK_RETRY_MAX_MS, DEFAULT_MAX_DIRTY_BYTES, DEFAULT_MAX_DRAIN_BATCH,
    DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS, DEFAULT_PATH_SHORTCUT_CACHE_BYTES,
    DEFAULT_PATH_SHORTCUT_CACHE_CAPACITY, DEFAULT_PREFETCH_CONCURRENCY,
    DEFAULT_PREFETCH_CONCURRENCY_MIN, DEFAULT_PREFETCH_FETCH_MAX_ATTEMPTS,
    DEFAULT_PREFETCH_FOREGROUND_RESERVE, DEFAULT_PREFETCH_MAX_PER_CYCLE,
    DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS, DEFAULT_PUBLISH_VISIBILITY_TIMEOUT_SECS,
    DEFAULT_QOS_ACCEPT_SLOTS, DEFAULT_QOS_COMMIT_SLOTS, DEFAULT_QOS_FETCH_SLOTS,
    DEFAULT_QOS_MATERIALIZE_SLOTS, DEFAULT_QOS_UPLOAD_SLOTS, DEFAULT_READ_BUFFER_MB,
    DEFAULT_READAHEAD_MIN_PREFETCH, DEFAULT_READAHEAD_RANDOM_PENALTY, DEFAULT_READAHEAD_SEQ_STEP,
    DEFAULT_READAHEAD_WIDTH, DEFAULT_READDIR_ATTR_BATCH_CHUNK,
    DEFAULT_READDIR_ATTR_BATCH_MAX_INODES, DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS,
    DEFAULT_SLAB_FLUSH_INTERVAL_MS, DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES,
    DEFAULT_UPLOAD_CACHE_IO_SLOTS, DEFAULT_UPLOAD_DECREASE_COOLDOWN_SECS,
    DEFAULT_UPLOAD_DECREASE_FACTOR, DEFAULT_UPLOAD_DELAY_MS,
    DEFAULT_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS, DEFAULT_UPLOAD_FG_READ_LATENCY_TARGET_MS,
    DEFAULT_UPLOAD_FG_READ_LATENCY_WINDOW_MS, DEFAULT_UPLOAD_INCREASE_STEP,
    DEFAULT_UPLOAD_INITIAL_WINDOW, DEFAULT_UPLOAD_MAX_ATTEMPTS, DEFAULT_UPLOAD_MAX_WINDOW,
    DEFAULT_UPLOAD_MIN_WINDOW, DEFAULT_UPLOAD_REQUEST_TIMEOUT_SECS, DEFAULT_UPLOAD_REQUEUE_BASE_MS,
    DEFAULT_UPLOAD_REQUEUE_MAX_MS, DEFAULT_UPLOAD_RETRY_BASE_MS, DEFAULT_UPLOAD_RETRY_MAX_MS,
    DEFAULT_UPLOAD_STATS_INTERVAL_SECS, DEFAULT_UPLOAD_USER_IDLE_GRACE_MS,
    DEFAULT_UPLOAD_WORKER_COUNT, DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES, DEFAULT_XATTR_MAX_BYTES,
    FOREGROUND_READ_SPEEDOMETER_SHARDS, MIN_UPLOAD_WORKER_COUNT,
};

use super::{OpenFile, VfsError, VfsResult};

pub(super) const CACHE_RUNTIME_MAX_DIRTY_BYTES_LARGE: usize = 4 * 1024 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE: usize = 8 * 1024 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MIN_WRITE_EAGER_BATCH_BYTES: usize = 16 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MAX_WRITE_EAGER_BATCH_BYTES: usize = 128 * 1024 * 1024;
pub(super) const WRITE_EAGER_DRAIN_CHUNK_MAX_BYTES: usize = 32 * 1024 * 1024;
pub(super) const PAYLOAD_PUT_CONCURRENCY_TARGET_BYTES: usize = 8 * 1024 * 1024;
pub(super) const PAYLOAD_PUT_CONCURRENCY_MAX: usize = 32;
pub(super) const FAST_MODE_SYNC_GROUP_MIN_BYTES_FLOOR: u64 = 32 * 1024 * 1024;
pub(super) const FAST_MODE_SYNC_GROUP_MIN_BYTES_CEIL: u64 = 256 * 1024 * 1024;
pub(super) const FAST_MODE_SYNC_GROUP_MAX_DELAY_MS: u64 = 25;
pub(super) const CACHE_RUNTIME_MIN_INGRESS_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MAX_INGRESS_SEGMENT_BYTES: u64 = 512 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES: usize = 512 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES: usize = 4 * 1024 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_SMALL_BUDGET_BYTES: u64 = 128 * 1024 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_LARGE_BUDGET_BYTES: u64 = 512 * 1024 * 1024 * 1024;
pub(super) const CACHE_RUNTIME_HUGE_BUDGET_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

/// Local cache durability/safety mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum CacheSafetyMode {
    /// Prefer a single local NVMe payload write (L2 cache) and recover
    /// best-effort by self-healing stale ingress artifacts on startup.
    #[default]
    Fast,
    /// Keep a durable local accept path via append-only ingress WAL +
    /// checkpoint recovery state.
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalSyncPolicy {
    Opportunistic,
    Strict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StickyErrorPolicy {
    Ignore,
    Report,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DirtyAttrPersistPolicy {
    Ignore,
    PersistIfPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReleaseFhOutcome {
    pub closed_last_handle: bool,
    pub flushed_local_dirty_state: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ExplicitFlushOutcome {
    pub flushed_local_dirty_state: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct FsyncOutcome {
    pub(super) flushed_file_data: bool,
    pub(super) persisted_dirty_attrs: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct LocalSyncBarrierOutcome {
    pub(super) had_local_dirty_state: bool,
    pub(super) flushed_file_data: bool,
    pub(super) persisted_dirty_attrs: bool,
}

pub(super) struct HandleReleaseContext {
    pub(super) open_file: OpenFile,
    pub(super) ino: Ino,
}

pub(super) struct UnlinkTarget {
    pub(super) ino: Ino,
    pub(super) open_handles: u32,
}

pub(super) struct DeletedInodeCleanup {
    pub(super) removed_ids: Vec<u64>,
    pub(super) freed_bytes: u64,
}

pub(super) enum LastHandleReleaseAction {
    IgnoredEntry,
    OrphanCleanup,
    LocalSync {
        sticky_error_policy: StickyErrorPolicy,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LocalSyncTrigger {
    Explicit,
    DurableMode,
    BytesThreshold,
    TimeWindow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EagerFlushOutcome {
    Triggered,
    SkippedEmpty,
    SkippedBelowThreshold,
}

pub(super) fn write_path_sync_trigger(
    cache_safety_mode: CacheSafetyMode,
    local_sync_policy: LocalSyncPolicy,
    pending_bytes: u64,
    elapsed_since_last_sync: Duration,
    group_min_bytes: u64,
    group_max_delay: Duration,
) -> Option<LocalSyncTrigger> {
    if matches!(local_sync_policy, LocalSyncPolicy::Strict) {
        return Some(LocalSyncTrigger::Explicit);
    }
    if matches!(cache_safety_mode, CacheSafetyMode::Durable) {
        return Some(LocalSyncTrigger::DurableMode);
    }
    if pending_bytes >= group_min_bytes {
        return Some(LocalSyncTrigger::BytesThreshold);
    }
    if elapsed_since_last_sync >= group_max_delay {
        return Some(LocalSyncTrigger::TimeWindow);
    }
    None
}

// =============================================================================
// VFS Configuration
// =============================================================================

/// VFS configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VfsConfig {
    /// Background janitor cycle interval.
    pub janitor_interval_secs: u64,
    /// Janitor grace window for newer objects.
    pub janitor_skip_newer_secs: u64,
    /// Max janitor deletes per cycle.
    pub janitor_max_deletes: u64,
    /// Janitor lease TTL in metadata store.
    pub janitor_lease_ttl_secs: u64,
    /// Sync interval for metadata invalidation watcher.
    pub metadata_invalidation_sync_interval_ms: u64,
    /// Read buffer size used to derive fetch/prefetch defaults.
    pub read_buffer_mb: u64,
    /// Prefetch concurrency budget.
    pub prefetch_concurrency: usize,
    /// Sequential read-ahead window.
    pub readahead_width: u32,
    /// Minimum prefetch count for sequential patterns.
    pub readahead_min_prefetch: u32,
    /// Sequential step size.
    pub readahead_seq_step: u8,
    /// Random access penalty.
    pub readahead_random_penalty: u8,
    /// Directory prefetch batch size.
    pub dir_prefetch_max_batch: usize,
    /// Readdir attr hydration chunk size.
    pub readdir_attr_batch_chunk: usize,
    /// Readdir attr hydration cap.
    pub readdir_attr_batch_max_inodes: usize,
    /// Foreground reserve for prefetch scheduling.
    pub prefetch_foreground_reserve: usize,
    /// Max prefetches per cycle.
    pub prefetch_max_per_cycle: usize,
    /// Pause between prefetch retries on network errors.
    pub prefetch_pause_on_neterr_ms: u64,
    /// Lock lease TTL.
    pub lock_ttl_secs: u64,
    /// Lock retry base backoff.
    pub lock_retry_base_ms: u64,
    /// Lock retry max backoff.
    pub lock_retry_max_ms: u64,
    /// Maximum total bytes allowed in the dirty-write memory buffer.
    /// Parsed from env via `parse_size_bytes` (for example `1.5GB`).
    pub max_dirty_bytes: usize,
    /// Minimum bytes of dirty full-block data to coalesce before `write()`
    /// triggers an eager local persist batch.
    /// Parsed from env via `parse_size_bytes` (for example `32MB`).
    pub write_eager_flush_batch_bytes: usize,
    /// Maximum bytes allowed for a single xattr value.
    /// Parsed from env via `parse_size_bytes` (for example `64KB`).
    pub max_xattr_bytes: usize,
    /// Dentry cache entry budget.
    pub dentry_cache_capacity: usize,
    /// Dentry cache byte budget.
    /// Parsed from env via `parse_size_bytes` (for example `512MB`).
    pub dentry_cache_bytes: usize,
    /// Path shortcut cache entry budget.
    pub path_shortcut_cache_capacity: usize,
    /// Path shortcut cache byte budget.
    /// Parsed from env via `parse_size_bytes` (for example `256MB`).
    pub path_shortcut_cache_bytes: usize,
    /// Attr read cache entry budget.
    pub attr_read_cache_capacity: usize,
    /// Attr read cache byte budget.
    /// Parsed from env via `parse_size_bytes` (for example `128MB`).
    pub attr_read_cache_bytes: usize,
    /// Effective ACL cache entry budget.
    pub effective_acl_cache_capacity: usize,
    /// Maximum active ingress segment size.
    /// Parsed from env via `parse_size_bytes` (for example `1GB`).
    pub max_ingress_segment_bytes: u64,
    /// Local cache durability/safety policy.
    pub cache_safety_mode: CacheSafetyMode,
    /// Principal used for ACL evaluation.
    pub principal: String,
    /// Initial fetch window.
    pub fetch_initial_window: f64,
    /// Minimum fetch window.
    pub fetch_min_window: f64,
    /// Maximum fetch window.
    pub fetch_max_window: f64,
    /// Fetch AIMD increase step.
    pub fetch_increase_step: f64,
    /// Fetch AIMD decrease factor.
    pub fetch_decrease_factor: f64,
    /// Fetch AIMD cooldown.
    pub fetch_decrease_cooldown_secs: u64,
    /// Initial upload window.
    pub upload_initial_window: f64,
    /// Minimum upload window.
    pub upload_min_window: f64,
    /// Maximum upload window.
    pub upload_max_window: f64,
    /// Upload AIMD increase step.
    pub upload_increase_step: f64,
    /// Upload AIMD decrease factor.
    pub upload_decrease_factor: f64,
    /// Upload AIMD cooldown.
    pub upload_decrease_cooldown_secs: u64,
    /// Delay uploads for overwrite/delete coalescing.
    pub upload_delay_ms: u64,
    /// Base backoff for exhausted upload requeue.
    pub upload_requeue_base_ms: u64,
    /// Max backoff for exhausted upload requeue.
    pub upload_requeue_max_ms: u64,
    /// Upload metrics log interval.
    pub upload_stats_interval_secs: u64,
    /// Background slab flusher wake interval.
    pub slab_flush_interval_ms: u64,
    /// Periodic ingress checkpoint interval.
    pub ingress_checkpoint_interval_secs: u64,
    /// Immediate checkpoint trigger on uncheckpointed ingress bytes.
    pub ingress_checkpoint_min_append_bytes: u64,
    /// Immediate checkpoint trigger on ingress record mutations.
    pub ingress_checkpoint_min_mutations: u64,
    /// Checkpoint trigger when active segment runs too far ahead.
    pub ingress_checkpoint_max_segment_lag: u64,
    /// Checkpoint trigger when live ingress index spans too many segments.
    pub ingress_checkpoint_max_live_segments: u64,
    /// Upload local-cache I/O slots.
    pub upload_cache_io_slots: usize,
    /// Max active transit-memory bytes used by in-flight read/upload buffers.
    /// Parsed from env via `parse_size_bytes` (for example `2GB`).
    pub transit_memory_budget_bytes: usize,
    /// User idle grace after the last foreground read.
    pub upload_user_idle_grace_ms: u64,
    /// Foreground-read P90 latency target that triggers upload throttling.
    pub upload_fg_read_latency_target_ms: u64,
    /// Sliding window used by the foreground-read speedometer.
    pub upload_fg_read_latency_window_ms: u64,
    /// Number of speedometer windows to retain last P90 when no fresh samples arrive.
    pub upload_fg_read_latency_sticky_windows: u64,
    /// Upload worker pool size.
    pub upload_worker_count: usize,
    /// Upload max attempts.
    pub upload_max_attempts: u32,
    /// Upload request timeout.
    pub upload_request_timeout_secs: u64,
    /// Upload retry base delay.
    pub upload_retry_base_ms: u64,
    /// Upload retry max delay.
    pub upload_retry_max_ms: u64,
    /// Fetch max attempts.
    pub fetch_max_attempts: u32,
    /// Prefetch fetch max attempts.
    pub prefetch_fetch_max_attempts: u32,
    /// Fetch request timeout.
    pub fetch_request_timeout_secs: u64,
    /// Fetch retry base delay.
    pub fetch_retry_base_ms: u64,
    /// Fetch retry max delay.
    pub fetch_retry_max_ms: u64,
    /// Prefetch reserve slots.
    ///
    /// `0` keeps runtime default prefetch reservation behavior.
    pub fetch_prefetch_reserve: usize,
    /// Prefetch acquire wait.
    pub fetch_prefetch_acquire_wait_ms: u64,
    /// Materializer batch limit for each drain cycle.
    pub max_drain_batch: usize,
    /// Time to wait for remote visibility before publishing a manifest.
    pub publish_visibility_timeout_secs: u64,
    /// Metadata flush timeout during shutdown.
    pub shutdown_metadata_flush_timeout_secs: u64,
    /// QoS slots for accept operations.
    pub qos_accept_slots: usize,
    /// QoS slots for materialization.
    pub qos_materialize_slots: usize,
    /// QoS slots for uploads.
    pub qos_upload_slots: usize,
    /// QoS slots for commits.
    pub qos_commit_slots: usize,
    /// QoS slots for fetches.
    pub qos_fetch_slots: usize,
    /// Number of shards for foreground read speedometer.
    pub foreground_read_speedometer_shards: usize,
}

/// Parsed user-provided VFS configuration.
///
/// All fields are optional to preserve whether a value was explicitly set.
/// Field semantics match [`VfsConfig`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct VfsConfigRaw {
    pub janitor_interval_secs: Option<u64>,
    pub janitor_skip_newer_secs: Option<u64>,
    pub janitor_max_deletes: Option<u64>,
    pub janitor_lease_ttl_secs: Option<u64>,
    pub metadata_invalidation_sync_interval_ms: Option<u64>,
    pub read_buffer_mb: Option<u64>,
    pub prefetch_concurrency: Option<usize>,
    pub readahead_width: Option<u32>,
    pub readahead_min_prefetch: Option<u32>,
    pub readahead_seq_step: Option<u8>,
    pub readahead_random_penalty: Option<u8>,
    pub dir_prefetch_max_batch: Option<usize>,
    pub readdir_attr_batch_chunk: Option<usize>,
    pub readdir_attr_batch_max_inodes: Option<usize>,
    pub prefetch_foreground_reserve: Option<usize>,
    pub prefetch_max_per_cycle: Option<usize>,
    pub prefetch_pause_on_neterr_ms: Option<u64>,
    pub lock_ttl_secs: Option<u64>,
    pub lock_retry_base_ms: Option<u64>,
    pub lock_retry_max_ms: Option<u64>,
    pub max_dirty_bytes: Option<usize>,
    pub write_eager_flush_batch_bytes: Option<usize>,
    pub max_xattr_bytes: Option<usize>,
    pub dentry_cache_capacity: Option<usize>,
    pub dentry_cache_bytes: Option<usize>,
    pub path_shortcut_cache_capacity: Option<usize>,
    pub path_shortcut_cache_bytes: Option<usize>,
    pub attr_read_cache_capacity: Option<usize>,
    pub attr_read_cache_bytes: Option<usize>,
    pub effective_acl_cache_capacity: Option<usize>,
    pub max_ingress_segment_bytes: Option<u64>,
    pub cache_safety_mode: Option<CacheSafetyMode>,
    pub principal: Option<String>,
    pub fetch_initial_window: Option<f64>,
    pub fetch_min_window: Option<f64>,
    pub fetch_max_window: Option<f64>,
    pub fetch_increase_step: Option<f64>,
    pub fetch_decrease_factor: Option<f64>,
    pub fetch_decrease_cooldown_secs: Option<u64>,
    pub upload_initial_window: Option<f64>,
    pub upload_min_window: Option<f64>,
    pub upload_max_window: Option<f64>,
    pub upload_increase_step: Option<f64>,
    pub upload_decrease_factor: Option<f64>,
    pub upload_decrease_cooldown_secs: Option<u64>,
    pub upload_delay_ms: Option<u64>,
    pub upload_requeue_base_ms: Option<u64>,
    pub upload_requeue_max_ms: Option<u64>,
    pub upload_stats_interval_secs: Option<u64>,
    pub slab_flush_interval_ms: Option<u64>,
    pub ingress_checkpoint_interval_secs: Option<u64>,
    pub ingress_checkpoint_min_append_bytes: Option<u64>,
    pub ingress_checkpoint_min_mutations: Option<u64>,
    pub ingress_checkpoint_max_segment_lag: Option<u64>,
    pub ingress_checkpoint_max_live_segments: Option<u64>,
    pub upload_cache_io_slots: Option<usize>,
    pub transit_memory_budget_bytes: Option<usize>,
    pub upload_user_idle_grace_ms: Option<u64>,
    pub upload_fg_read_latency_target_ms: Option<u64>,
    pub upload_fg_read_latency_window_ms: Option<u64>,
    pub upload_fg_read_latency_sticky_windows: Option<u64>,
    pub upload_worker_count: Option<usize>,
    pub upload_max_attempts: Option<u32>,
    pub upload_request_timeout_secs: Option<u64>,
    pub upload_retry_base_ms: Option<u64>,
    pub upload_retry_max_ms: Option<u64>,
    pub fetch_max_attempts: Option<u32>,
    pub prefetch_fetch_max_attempts: Option<u32>,
    pub fetch_request_timeout_secs: Option<u64>,
    pub fetch_retry_base_ms: Option<u64>,
    pub fetch_retry_max_ms: Option<u64>,
    pub fetch_prefetch_reserve: Option<usize>,
    pub fetch_prefetch_acquire_wait_ms: Option<u64>,
    pub max_drain_batch: Option<usize>,
    pub publish_visibility_timeout_secs: Option<u64>,
    pub shutdown_metadata_flush_timeout_secs: Option<u64>,
    pub qos_accept_slots: Option<usize>,
    pub qos_materialize_slots: Option<usize>,
    pub qos_upload_slots: Option<usize>,
    pub qos_commit_slots: Option<usize>,
    pub qos_fetch_slots: Option<usize>,
    pub foreground_read_speedometer_shards: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct VfsConfigValidated {
    config: VfsConfig,
    cache_runtime_overrides: CacheRuntimeTuningOverrides,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CacheRuntimeTuningOverrides {
    pub(super) max_dirty_bytes: bool,
    pub(super) write_eager_flush_batch_bytes: bool,
    pub(super) max_ingress_segment_bytes: bool,
    pub(super) ingress_checkpoint_interval_secs: bool,
    pub(super) ingress_checkpoint_min_append_bytes: bool,
    pub(super) ingress_checkpoint_min_mutations: bool,
    pub(super) ingress_checkpoint_max_segment_lag: bool,
    pub(super) ingress_checkpoint_max_live_segments: bool,
    pub(super) upload_cache_io_slots: bool,
    pub(super) transit_memory_budget_bytes: bool,
}

impl VfsConfigValidated {
    pub fn into_inner(self) -> VfsConfig {
        self.config
    }

    pub(super) fn cache_runtime_overrides(&self) -> CacheRuntimeTuningOverrides {
        self.cache_runtime_overrides
    }
}

impl std::ops::Deref for VfsConfigValidated {
    type Target = VfsConfig;

    fn deref(&self) -> &Self::Target {
        &self.config
    }
}

impl AsRef<VfsConfig> for VfsConfigValidated {
    fn as_ref(&self) -> &VfsConfig {
        &self.config
    }
}

impl TryFrom<VfsConfig> for VfsConfigValidated {
    type Error = VfsError;

    fn try_from(value: VfsConfig) -> Result<Self, Self::Error> {
        normalize_vfs_config(
            value,
            CacheRuntimeTuningOverrides {
                max_dirty_bytes: true,
                write_eager_flush_batch_bytes: true,
                max_ingress_segment_bytes: true,
                ingress_checkpoint_interval_secs: true,
                ingress_checkpoint_min_append_bytes: true,
                ingress_checkpoint_min_mutations: true,
                ingress_checkpoint_max_segment_lag: true,
                ingress_checkpoint_max_live_segments: true,
                upload_cache_io_slots: true,
                transit_memory_budget_bytes: true,
            },
        )
    }
}

impl VfsConfigRaw {
    fn resolve_defaults(self) -> (VfsConfig, CacheRuntimeTuningOverrides) {
        let overrides = CacheRuntimeTuningOverrides {
            max_dirty_bytes: self.max_dirty_bytes.is_some(),
            write_eager_flush_batch_bytes: self.write_eager_flush_batch_bytes.is_some(),
            max_ingress_segment_bytes: self.max_ingress_segment_bytes.is_some(),
            ingress_checkpoint_interval_secs: self.ingress_checkpoint_interval_secs.is_some(),
            ingress_checkpoint_min_append_bytes: self.ingress_checkpoint_min_append_bytes.is_some(),
            ingress_checkpoint_min_mutations: self.ingress_checkpoint_min_mutations.is_some(),
            ingress_checkpoint_max_segment_lag: self.ingress_checkpoint_max_segment_lag.is_some(),
            ingress_checkpoint_max_live_segments: self
                .ingress_checkpoint_max_live_segments
                .is_some(),
            upload_cache_io_slots: self.upload_cache_io_slots.is_some(),
            transit_memory_budget_bytes: self.transit_memory_budget_bytes.is_some(),
        };
        let mut cfg = VfsConfig::default();
        macro_rules! overlay {
            ($($field:ident),+ $(,)?) => {
                $(if let Some(value) = self.$field { cfg.$field = value; })+
            };
        }
        overlay!(
            janitor_interval_secs,
            janitor_skip_newer_secs,
            janitor_max_deletes,
            janitor_lease_ttl_secs,
            metadata_invalidation_sync_interval_ms,
            read_buffer_mb,
            prefetch_concurrency,
            readahead_width,
            readahead_min_prefetch,
            readahead_seq_step,
            readahead_random_penalty,
            dir_prefetch_max_batch,
            readdir_attr_batch_chunk,
            readdir_attr_batch_max_inodes,
            prefetch_foreground_reserve,
            prefetch_max_per_cycle,
            prefetch_pause_on_neterr_ms,
            lock_ttl_secs,
            lock_retry_base_ms,
            lock_retry_max_ms,
            max_dirty_bytes,
            write_eager_flush_batch_bytes,
            max_xattr_bytes,
            dentry_cache_capacity,
            dentry_cache_bytes,
            path_shortcut_cache_capacity,
            path_shortcut_cache_bytes,
            attr_read_cache_capacity,
            attr_read_cache_bytes,
            effective_acl_cache_capacity,
            max_ingress_segment_bytes,
            cache_safety_mode,
            principal,
            fetch_initial_window,
            fetch_min_window,
            fetch_max_window,
            fetch_increase_step,
            fetch_decrease_factor,
            fetch_decrease_cooldown_secs,
            upload_initial_window,
            upload_min_window,
            upload_max_window,
            upload_increase_step,
            upload_decrease_factor,
            upload_decrease_cooldown_secs,
            upload_delay_ms,
            upload_requeue_base_ms,
            upload_requeue_max_ms,
            upload_stats_interval_secs,
            slab_flush_interval_ms,
            ingress_checkpoint_interval_secs,
            ingress_checkpoint_min_append_bytes,
            ingress_checkpoint_min_mutations,
            ingress_checkpoint_max_segment_lag,
            ingress_checkpoint_max_live_segments,
            upload_cache_io_slots,
            transit_memory_budget_bytes,
            upload_user_idle_grace_ms,
            upload_fg_read_latency_target_ms,
            upload_fg_read_latency_window_ms,
            upload_fg_read_latency_sticky_windows,
            upload_worker_count,
            upload_max_attempts,
            upload_request_timeout_secs,
            upload_retry_base_ms,
            upload_retry_max_ms,
            fetch_max_attempts,
            prefetch_fetch_max_attempts,
            fetch_request_timeout_secs,
            fetch_retry_base_ms,
            fetch_retry_max_ms,
            fetch_prefetch_reserve,
            fetch_prefetch_acquire_wait_ms,
            max_drain_batch,
            publish_visibility_timeout_secs,
            shutdown_metadata_flush_timeout_secs,
            qos_accept_slots,
            qos_materialize_slots,
            qos_upload_slots,
            qos_commit_slots,
            qos_fetch_slots,
            foreground_read_speedometer_shards,
        );

        (cfg, overrides)
    }

    pub fn validate(self) -> VfsResult<VfsConfigValidated> {
        let (cfg, overrides) = self.resolve_defaults();
        normalize_vfs_config(cfg, overrides)
    }
}

fn normalize_vfs_config(
    mut cfg: VfsConfig,
    cache_runtime_overrides: CacheRuntimeTuningOverrides,
) -> VfsResult<VfsConfigValidated> {
    cfg.janitor_interval_secs = cfg.janitor_interval_secs.max(1);
    cfg.janitor_max_deletes = cfg.janitor_max_deletes.max(1);
    cfg.janitor_lease_ttl_secs = cfg.janitor_lease_ttl_secs.max(1);
    cfg.metadata_invalidation_sync_interval_ms = cfg.metadata_invalidation_sync_interval_ms.max(1);
    cfg.read_buffer_mb = cfg.read_buffer_mb.max(1);
    cfg.prefetch_concurrency = cfg
        .prefetch_concurrency
        .max(DEFAULT_PREFETCH_CONCURRENCY_MIN);
    cfg.readahead_width = cfg.readahead_width.max(1);
    cfg.readahead_min_prefetch = cfg.readahead_min_prefetch.max(1);
    cfg.readahead_seq_step = cfg.readahead_seq_step.max(1);
    cfg.readahead_random_penalty = cfg.readahead_random_penalty.max(1);
    cfg.dir_prefetch_max_batch = cfg.dir_prefetch_max_batch.max(1);
    cfg.readdir_attr_batch_chunk = cfg.readdir_attr_batch_chunk.max(1);
    cfg.readdir_attr_batch_max_inodes = cfg.readdir_attr_batch_max_inodes.max(1);
    cfg.prefetch_foreground_reserve = cfg.prefetch_foreground_reserve.max(1);
    cfg.prefetch_max_per_cycle = cfg.prefetch_max_per_cycle.max(1);
    cfg.lock_ttl_secs = cfg.lock_ttl_secs.max(1);
    cfg.lock_retry_base_ms = cfg.lock_retry_base_ms.max(1);
    cfg.lock_retry_max_ms = cfg.lock_retry_max_ms.max(cfg.lock_retry_base_ms);
    cfg.max_dirty_bytes = cfg.max_dirty_bytes.max(1);
    cfg.write_eager_flush_batch_bytes = cfg.write_eager_flush_batch_bytes.max(1);
    cfg.max_xattr_bytes = cfg.max_xattr_bytes.max(1);
    cfg.dentry_cache_capacity = cfg.dentry_cache_capacity.max(1);
    cfg.dentry_cache_bytes = cfg.dentry_cache_bytes.max(1);
    cfg.path_shortcut_cache_capacity = cfg.path_shortcut_cache_capacity.max(1);
    cfg.path_shortcut_cache_bytes = cfg.path_shortcut_cache_bytes.max(1);
    cfg.attr_read_cache_capacity = cfg.attr_read_cache_capacity.max(1);
    cfg.attr_read_cache_bytes = cfg.attr_read_cache_bytes.max(1);
    cfg.effective_acl_cache_capacity = cfg.effective_acl_cache_capacity.max(1);
    cfg.max_ingress_segment_bytes = cfg.max_ingress_segment_bytes.max(1);
    cfg.fetch_min_window = cfg.fetch_min_window.max(1.0);
    cfg.fetch_decrease_cooldown_secs = cfg.fetch_decrease_cooldown_secs.max(1);
    cfg.upload_min_window = cfg.upload_min_window.max(1.0);
    cfg.upload_max_window = cfg.upload_max_window.max(cfg.upload_min_window);
    cfg.upload_increase_step = cfg.upload_increase_step.max(0.000_001);
    cfg.upload_decrease_factor = cfg.upload_decrease_factor.max(0.000_001);
    cfg.upload_decrease_cooldown_secs = cfg.upload_decrease_cooldown_secs.max(1);
    cfg.upload_requeue_base_ms = cfg.upload_requeue_base_ms.max(1);
    cfg.upload_requeue_max_ms = cfg.upload_requeue_max_ms.max(cfg.upload_requeue_base_ms);
    cfg.upload_stats_interval_secs = cfg.upload_stats_interval_secs.max(1);
    cfg.slab_flush_interval_ms = cfg.slab_flush_interval_ms.max(1);
    cfg.ingress_checkpoint_interval_secs = cfg.ingress_checkpoint_interval_secs.max(1);
    cfg.upload_cache_io_slots = cfg.upload_cache_io_slots.max(1);
    cfg.upload_fg_read_latency_target_ms = cfg.upload_fg_read_latency_target_ms.max(1);
    cfg.upload_fg_read_latency_window_ms = cfg.upload_fg_read_latency_window_ms.max(100);
    cfg.upload_fg_read_latency_sticky_windows = cfg.upload_fg_read_latency_sticky_windows.max(1);
    cfg.max_drain_batch = cfg.max_drain_batch.max(1);
    cfg.publish_visibility_timeout_secs = cfg.publish_visibility_timeout_secs.max(1);
    cfg.shutdown_metadata_flush_timeout_secs = cfg.shutdown_metadata_flush_timeout_secs.max(1);
    cfg.qos_accept_slots = cfg.qos_accept_slots.max(1);
    cfg.qos_materialize_slots = cfg.qos_materialize_slots.max(1);
    cfg.qos_upload_slots = cfg.qos_upload_slots.max(1);
    cfg.qos_commit_slots = cfg.qos_commit_slots.max(1);
    cfg.qos_fetch_slots = cfg.qos_fetch_slots.max(1);
    cfg.foreground_read_speedometer_shards = cfg.foreground_read_speedometer_shards.max(1);

    cfg.upload_worker_count = if cfg.upload_worker_count == 0 {
        DEFAULT_UPLOAD_WORKER_COUNT
    } else {
        cfg.upload_worker_count.max(MIN_UPLOAD_WORKER_COUNT)
    };

    let principal = cfg.principal.trim();
    cfg.principal = if principal.is_empty() {
        "*".to_string()
    } else {
        principal.to_string()
    };

    Ok(VfsConfigValidated {
        config: cfg,
        cache_runtime_overrides,
    })
}

impl From<VfsConfig> for VfsConfigRaw {
    fn from(cfg: VfsConfig) -> Self {
        Self {
            janitor_interval_secs: Some(cfg.janitor_interval_secs),
            janitor_skip_newer_secs: Some(cfg.janitor_skip_newer_secs),
            janitor_max_deletes: Some(cfg.janitor_max_deletes),
            janitor_lease_ttl_secs: Some(cfg.janitor_lease_ttl_secs),
            metadata_invalidation_sync_interval_ms: Some(
                cfg.metadata_invalidation_sync_interval_ms,
            ),
            read_buffer_mb: Some(cfg.read_buffer_mb),
            prefetch_concurrency: Some(cfg.prefetch_concurrency),
            readahead_width: Some(cfg.readahead_width),
            readahead_min_prefetch: Some(cfg.readahead_min_prefetch),
            readahead_seq_step: Some(cfg.readahead_seq_step),
            readahead_random_penalty: Some(cfg.readahead_random_penalty),
            dir_prefetch_max_batch: Some(cfg.dir_prefetch_max_batch),
            readdir_attr_batch_chunk: Some(cfg.readdir_attr_batch_chunk),
            readdir_attr_batch_max_inodes: Some(cfg.readdir_attr_batch_max_inodes),
            prefetch_foreground_reserve: Some(cfg.prefetch_foreground_reserve),
            prefetch_max_per_cycle: Some(cfg.prefetch_max_per_cycle),
            prefetch_pause_on_neterr_ms: Some(cfg.prefetch_pause_on_neterr_ms),
            lock_ttl_secs: Some(cfg.lock_ttl_secs),
            lock_retry_base_ms: Some(cfg.lock_retry_base_ms),
            lock_retry_max_ms: Some(cfg.lock_retry_max_ms),
            max_dirty_bytes: Some(cfg.max_dirty_bytes),
            write_eager_flush_batch_bytes: Some(cfg.write_eager_flush_batch_bytes),
            max_xattr_bytes: Some(cfg.max_xattr_bytes),
            dentry_cache_capacity: Some(cfg.dentry_cache_capacity),
            dentry_cache_bytes: Some(cfg.dentry_cache_bytes),
            path_shortcut_cache_capacity: Some(cfg.path_shortcut_cache_capacity),
            path_shortcut_cache_bytes: Some(cfg.path_shortcut_cache_bytes),
            attr_read_cache_capacity: Some(cfg.attr_read_cache_capacity),
            attr_read_cache_bytes: Some(cfg.attr_read_cache_bytes),
            effective_acl_cache_capacity: Some(cfg.effective_acl_cache_capacity),
            max_ingress_segment_bytes: Some(cfg.max_ingress_segment_bytes),
            cache_safety_mode: Some(cfg.cache_safety_mode),
            principal: Some(cfg.principal),
            fetch_initial_window: Some(cfg.fetch_initial_window),
            fetch_min_window: Some(cfg.fetch_min_window),
            fetch_max_window: Some(cfg.fetch_max_window),
            fetch_increase_step: Some(cfg.fetch_increase_step),
            fetch_decrease_factor: Some(cfg.fetch_decrease_factor),
            fetch_decrease_cooldown_secs: Some(cfg.fetch_decrease_cooldown_secs),
            upload_initial_window: Some(cfg.upload_initial_window),
            upload_min_window: Some(cfg.upload_min_window),
            upload_max_window: Some(cfg.upload_max_window),
            upload_increase_step: Some(cfg.upload_increase_step),
            upload_decrease_factor: Some(cfg.upload_decrease_factor),
            upload_decrease_cooldown_secs: Some(cfg.upload_decrease_cooldown_secs),
            upload_delay_ms: Some(cfg.upload_delay_ms),
            upload_requeue_base_ms: Some(cfg.upload_requeue_base_ms),
            upload_requeue_max_ms: Some(cfg.upload_requeue_max_ms),
            upload_stats_interval_secs: Some(cfg.upload_stats_interval_secs),
            slab_flush_interval_ms: Some(cfg.slab_flush_interval_ms),
            ingress_checkpoint_interval_secs: Some(cfg.ingress_checkpoint_interval_secs),
            ingress_checkpoint_min_append_bytes: Some(cfg.ingress_checkpoint_min_append_bytes),
            ingress_checkpoint_min_mutations: Some(cfg.ingress_checkpoint_min_mutations),
            ingress_checkpoint_max_segment_lag: Some(cfg.ingress_checkpoint_max_segment_lag),
            ingress_checkpoint_max_live_segments: Some(cfg.ingress_checkpoint_max_live_segments),
            upload_cache_io_slots: Some(cfg.upload_cache_io_slots),
            transit_memory_budget_bytes: Some(cfg.transit_memory_budget_bytes),
            upload_user_idle_grace_ms: Some(cfg.upload_user_idle_grace_ms),
            upload_fg_read_latency_target_ms: Some(cfg.upload_fg_read_latency_target_ms),
            upload_fg_read_latency_window_ms: Some(cfg.upload_fg_read_latency_window_ms),
            upload_fg_read_latency_sticky_windows: Some(cfg.upload_fg_read_latency_sticky_windows),
            upload_worker_count: Some(cfg.upload_worker_count),
            upload_max_attempts: Some(cfg.upload_max_attempts),
            upload_request_timeout_secs: Some(cfg.upload_request_timeout_secs),
            upload_retry_base_ms: Some(cfg.upload_retry_base_ms),
            upload_retry_max_ms: Some(cfg.upload_retry_max_ms),
            fetch_max_attempts: Some(cfg.fetch_max_attempts),
            prefetch_fetch_max_attempts: Some(cfg.prefetch_fetch_max_attempts),
            fetch_request_timeout_secs: Some(cfg.fetch_request_timeout_secs),
            fetch_retry_base_ms: Some(cfg.fetch_retry_base_ms),
            fetch_retry_max_ms: Some(cfg.fetch_retry_max_ms),
            fetch_prefetch_reserve: Some(cfg.fetch_prefetch_reserve),
            fetch_prefetch_acquire_wait_ms: Some(cfg.fetch_prefetch_acquire_wait_ms),
            max_drain_batch: Some(cfg.max_drain_batch),
            publish_visibility_timeout_secs: Some(cfg.publish_visibility_timeout_secs),
            shutdown_metadata_flush_timeout_secs: Some(cfg.shutdown_metadata_flush_timeout_secs),
            qos_accept_slots: Some(cfg.qos_accept_slots),
            qos_materialize_slots: Some(cfg.qos_materialize_slots),
            qos_upload_slots: Some(cfg.qos_upload_slots),
            qos_commit_slots: Some(cfg.qos_commit_slots),
            qos_fetch_slots: Some(cfg.qos_fetch_slots),
            foreground_read_speedometer_shards: Some(cfg.foreground_read_speedometer_shards),
        }
    }
}

impl Default for VfsConfig {
    fn default() -> Self {
        Self {
            janitor_interval_secs: DEFAULT_JANITOR_INTERVAL.as_secs(),
            janitor_skip_newer_secs: DEFAULT_JANITOR_SKIP_NEWER_THAN.as_secs(),
            janitor_max_deletes: DEFAULT_JANITOR_MAX_DELETES_PER_RUN,
            janitor_lease_ttl_secs: DEFAULT_JANITOR_LEASE_TTL.as_secs(),
            metadata_invalidation_sync_interval_ms: DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS,
            read_buffer_mb: DEFAULT_READ_BUFFER_MB,
            prefetch_concurrency: DEFAULT_PREFETCH_CONCURRENCY
                .max(DEFAULT_PREFETCH_CONCURRENCY_MIN),
            readahead_width: DEFAULT_READAHEAD_WIDTH,
            readahead_min_prefetch: DEFAULT_READAHEAD_MIN_PREFETCH,
            readahead_seq_step: DEFAULT_READAHEAD_SEQ_STEP,
            readahead_random_penalty: DEFAULT_READAHEAD_RANDOM_PENALTY,
            dir_prefetch_max_batch: DEFAULT_DIR_PREFETCH_MAX_BATCH,
            readdir_attr_batch_chunk: DEFAULT_READDIR_ATTR_BATCH_CHUNK,
            readdir_attr_batch_max_inodes: DEFAULT_READDIR_ATTR_BATCH_MAX_INODES,
            prefetch_foreground_reserve: DEFAULT_PREFETCH_FOREGROUND_RESERVE,
            prefetch_max_per_cycle: DEFAULT_PREFETCH_MAX_PER_CYCLE,
            prefetch_pause_on_neterr_ms: DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS,
            lock_ttl_secs: DEFAULT_LOCK_LEASE_TTL.as_secs(),
            lock_retry_base_ms: DEFAULT_LOCK_RETRY_BASE_MS,
            lock_retry_max_ms: DEFAULT_LOCK_RETRY_MAX_MS,
            max_dirty_bytes: DEFAULT_MAX_DIRTY_BYTES,
            write_eager_flush_batch_bytes: DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES,
            max_xattr_bytes: DEFAULT_XATTR_MAX_BYTES,
            dentry_cache_capacity: DEFAULT_DENTRY_CACHE_CAPACITY,
            dentry_cache_bytes: DEFAULT_DENTRY_CACHE_BYTES,
            path_shortcut_cache_capacity: DEFAULT_PATH_SHORTCUT_CACHE_CAPACITY,
            path_shortcut_cache_bytes: DEFAULT_PATH_SHORTCUT_CACHE_BYTES,
            attr_read_cache_capacity: DEFAULT_ATTR_READ_CACHE_CAPACITY,
            attr_read_cache_bytes: DEFAULT_ATTR_READ_CACHE_BYTES,
            effective_acl_cache_capacity: DEFAULT_EFFECTIVE_ACL_CACHE_CAPACITY,
            max_ingress_segment_bytes: DEFAULT_INGRESS_SEGMENT_MAX_BYTES,
            cache_safety_mode: CacheSafetyMode::Fast,
            principal: "*".to_string(),
            fetch_initial_window: DEFAULT_FETCH_INITIAL_WINDOW,
            fetch_min_window: DEFAULT_FETCH_MIN_WINDOW,
            fetch_max_window: DEFAULT_FETCH_MAX_WINDOW,
            fetch_increase_step: DEFAULT_FETCH_INCREASE_STEP,
            fetch_decrease_factor: DEFAULT_FETCH_DECREASE_FACTOR,
            fetch_decrease_cooldown_secs: DEFAULT_FETCH_DECREASE_COOLDOWN_SECS,
            upload_initial_window: DEFAULT_UPLOAD_INITIAL_WINDOW,
            upload_min_window: DEFAULT_UPLOAD_MIN_WINDOW,
            upload_max_window: DEFAULT_UPLOAD_MAX_WINDOW,
            upload_increase_step: DEFAULT_UPLOAD_INCREASE_STEP,
            upload_decrease_factor: DEFAULT_UPLOAD_DECREASE_FACTOR,
            upload_decrease_cooldown_secs: DEFAULT_UPLOAD_DECREASE_COOLDOWN_SECS,
            upload_delay_ms: DEFAULT_UPLOAD_DELAY_MS,
            upload_requeue_base_ms: DEFAULT_UPLOAD_REQUEUE_BASE_MS,
            upload_requeue_max_ms: DEFAULT_UPLOAD_REQUEUE_MAX_MS,
            upload_stats_interval_secs: DEFAULT_UPLOAD_STATS_INTERVAL_SECS,
            slab_flush_interval_ms: DEFAULT_SLAB_FLUSH_INTERVAL_MS,
            ingress_checkpoint_interval_secs: DEFAULT_INGRESS_CHECKPOINT_INTERVAL_SECS,
            ingress_checkpoint_min_append_bytes: DEFAULT_INGRESS_CHECKPOINT_MIN_APPEND_BYTES,
            ingress_checkpoint_min_mutations: DEFAULT_INGRESS_CHECKPOINT_MIN_MUTATIONS,
            ingress_checkpoint_max_segment_lag: DEFAULT_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG,
            ingress_checkpoint_max_live_segments: DEFAULT_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS,
            upload_cache_io_slots: DEFAULT_UPLOAD_CACHE_IO_SLOTS,
            transit_memory_budget_bytes: DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES,
            upload_user_idle_grace_ms: DEFAULT_UPLOAD_USER_IDLE_GRACE_MS,
            upload_fg_read_latency_target_ms: DEFAULT_UPLOAD_FG_READ_LATENCY_TARGET_MS,
            upload_fg_read_latency_window_ms: DEFAULT_UPLOAD_FG_READ_LATENCY_WINDOW_MS,
            upload_fg_read_latency_sticky_windows: DEFAULT_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS,
            upload_worker_count: DEFAULT_UPLOAD_WORKER_COUNT,
            upload_max_attempts: DEFAULT_UPLOAD_MAX_ATTEMPTS,
            upload_request_timeout_secs: DEFAULT_UPLOAD_REQUEST_TIMEOUT_SECS,
            upload_retry_base_ms: DEFAULT_UPLOAD_RETRY_BASE_MS,
            upload_retry_max_ms: DEFAULT_UPLOAD_RETRY_MAX_MS,
            fetch_max_attempts: DEFAULT_FETCH_MAX_ATTEMPTS,
            prefetch_fetch_max_attempts: DEFAULT_PREFETCH_FETCH_MAX_ATTEMPTS,
            fetch_request_timeout_secs: DEFAULT_FETCH_REQUEST_TIMEOUT_SECS,
            fetch_retry_base_ms: DEFAULT_FETCH_RETRY_BASE_MS,
            fetch_retry_max_ms: DEFAULT_FETCH_RETRY_MAX_MS,
            fetch_prefetch_reserve: DEFAULT_FETCH_PREFETCH_RESERVE,
            fetch_prefetch_acquire_wait_ms: DEFAULT_FETCH_PREFETCH_ACQUIRE_WAIT_MS,
            max_drain_batch: DEFAULT_MAX_DRAIN_BATCH,
            publish_visibility_timeout_secs: DEFAULT_PUBLISH_VISIBILITY_TIMEOUT_SECS,
            shutdown_metadata_flush_timeout_secs: DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS,
            qos_accept_slots: DEFAULT_QOS_ACCEPT_SLOTS,
            qos_materialize_slots: DEFAULT_QOS_MATERIALIZE_SLOTS,
            qos_upload_slots: DEFAULT_QOS_UPLOAD_SLOTS,
            qos_commit_slots: DEFAULT_QOS_COMMIT_SLOTS,
            qos_fetch_slots: DEFAULT_QOS_FETCH_SLOTS,
            foreground_read_speedometer_shards: FOREGROUND_READ_SPEEDOMETER_SHARDS,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vfs_config_raw_validate_applies_defaults() {
        let validated = VfsConfigRaw::default()
            .validate()
            .expect("validate defaults");
        let defaults = VfsConfig::default();

        assert_eq!(
            validated.janitor_interval_secs,
            defaults.janitor_interval_secs
        );
        assert_eq!(validated.max_dirty_bytes, defaults.max_dirty_bytes);
        assert_eq!(
            validated.write_eager_flush_batch_bytes,
            defaults.write_eager_flush_batch_bytes
        );
        assert_eq!(validated.qos_fetch_slots, defaults.qos_fetch_slots);
        assert_eq!(
            validated.upload_worker_count, 128,
            "validation should materialize upload worker default"
        );
    }

    #[test]
    fn vfs_config_raw_validate_overlays_explicit_values() {
        let validated = VfsConfigRaw {
            read_buffer_mb: Some(8192),
            upload_cache_io_slots: Some(11),
            principal: Some("dev-user".to_string()),
            ..Default::default()
        }
        .validate()
        .expect("validate explicit values");

        assert_eq!(validated.read_buffer_mb, 8192);
        assert_eq!(validated.upload_cache_io_slots, 11);
        assert_eq!(validated.principal, "dev-user");
    }

    #[test]
    fn upload_cache_io_foreground_cost_derivation_scales_with_slot_budget() {
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_base(1),
            1
        );
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_base(8),
            2
        );
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_base(32),
            8
        );
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_step(1),
            1
        );
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_step(8),
            1
        );
        assert_eq!(
            crate::vfs::derive_upload_cache_io_foreground_cost_step(32),
            4
        );
    }

    #[test]
    fn cache_runtime_tuning_scales_with_cache_budget() {
        let small = crate::vfs::derive_cache_runtime_tuning(
            64 * 1024 * 1024 * 1024,
            16 * 1024 * 1024,
            512 * 1024 * 1024,
        );
        let large = crate::vfs::derive_cache_runtime_tuning(
            2 * 1024 * 1024 * 1024 * 1024,
            16 * 1024 * 1024,
            2 * 1024 * 1024 * 1024,
        );

        assert!(large.max_dirty_bytes >= small.max_dirty_bytes);
        assert!(large.max_ingress_segment_bytes >= small.max_ingress_segment_bytes);
        assert!(
            large.ingress_checkpoint_min_append_bytes >= small.ingress_checkpoint_min_append_bytes
        );
        assert!(large.upload_cache_io_slots >= small.upload_cache_io_slots);
    }

    #[test]
    fn test_vfs_config_defaults_to_fast_cache_safety_mode() {
        assert_eq!(
            VfsConfig::default().cache_safety_mode,
            CacheSafetyMode::Fast
        );
    }

    #[test]
    fn local_sync_policy_strict_always_triggers_sync() {
        let trigger = write_path_sync_trigger(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Strict,
            1,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(trigger, Some(LocalSyncTrigger::Explicit));
    }

    #[test]
    fn local_sync_policy_durable_mode_always_triggers_sync() {
        let trigger = write_path_sync_trigger(
            CacheSafetyMode::Durable,
            LocalSyncPolicy::Opportunistic,
            1,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(trigger, Some(LocalSyncTrigger::DurableMode));
    }

    #[test]
    fn local_sync_policy_fast_mode_waits_until_threshold_or_window() {
        let bytes_trigger = write_path_sync_trigger(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            64 * 1024 * 1024,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(bytes_trigger, Some(LocalSyncTrigger::BytesThreshold));

        let window_trigger = write_path_sync_trigger(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            1024,
            Duration::from_millis(30),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(window_trigger, Some(LocalSyncTrigger::TimeWindow));

        let deferred = write_path_sync_trigger(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            1024,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(deferred, None);
    }
}
