//! Core VFS implementation — Block-based architecture.
//!
//! Key design:
//! - Files are split into fixed-size blocks (default 16MB)
//! - Each block has a sequential ID (not content hash)
//! - Blocks are immutable - never modified, only created/deleted
//! - No deduplication = no reference counting overhead
//! - All metadata in redb, S3 stores only raw data blocks
//!
//! Write Path:
//! 1. Split data into blocks
//! 2. Merge dirty bytes into the per-file `WriteBuffer`
//! 3. Persist full dirty blocks locally via the Cache 3.0 backend `put(...)`
//! 4. Apply local `sync_data()` barriers (strict for fsync / durable mode,
//!    grouped in fast mode) and persist extent metadata / attrs
//! 5. Queue committed extents for async upload + remote publish
//!
//! Read Path:
//! 1. Check the `WriteBuffer` overlay for newer local bytes
//! 2. Resolve local metadata refs to committed local extents in the local backend
//!    (inline on the active runtime worker, no per-op blocking-pool hop)
//! 3. Fall back to remote fetch
//! 4. Validate object payload + checksum and assemble response

mod acl;
mod bootstrap;
#[path = "commit.rs"]
mod commit;
mod config;
mod deletion_cleanup;
mod file_session;
mod handle_table;
mod ignored_entries;
mod io_ops;
mod locking;
mod metrics;
mod namespace;
mod namespace_ops;
mod overlay;
mod prefetch;
mod read_path;
mod startup;
mod statfs;
mod sync_ops;
mod workers;
#[path = "write_buffer.rs"]
mod write_buffer;

pub use config::{
    CacheSafetyMode, ExplicitFlushOutcome, ReleaseFhOutcome, VfsConfig, VfsConfigRaw,
    VfsConfigValidated,
};
pub use file_session::{OpenFile, OpenFlags, VfsFileSessionStats};
pub use locking::LockKind;
pub use metrics::{
    StatFs, VfsForegroundReadProtectionStats, VfsIngressLane, VfsIngressPressure, VfsIngressStats,
    VfsLatencyHistogramSnapshot, VfsUploadStats, VfsWritePathBackpressureStats,
    VfsWritePathBurstStats, VfsWritePathCadenceStats, VfsWritePathEagerFlushStats,
    VfsWritePathLatencyQuantilesMs, VfsWritePathLatencyQuantilesSaturated,
    VfsWritePathLocalCacheIoStats, VfsWritePathObservabilitySnapshot,
    VfsWritePathStageLatencyStats, VfsWritePathSyncDataStats, VfsWritePathTuningStats,
    VfsWritePathWinfspCallbackStats, VfsWritePathWinfspForegroundFlushStats,
};
pub use namespace::{KernelInvalidator, NamespaceParent, NamespacePathStyle, NamespaceResolver};

use acl::AclState;
use config::{
    CACHE_RUNTIME_HUGE_BUDGET_BYTES, CACHE_RUNTIME_LARGE_BUDGET_BYTES,
    CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE, CACHE_RUNTIME_MAX_DIRTY_BYTES_LARGE,
    CACHE_RUNTIME_MAX_INGRESS_SEGMENT_BYTES, CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES,
    CACHE_RUNTIME_MAX_WRITE_EAGER_BATCH_BYTES, CACHE_RUNTIME_MIN_INGRESS_SEGMENT_BYTES,
    CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES, CACHE_RUNTIME_MIN_WRITE_EAGER_BATCH_BYTES,
    CACHE_RUNTIME_SMALL_BUDGET_BYTES, CacheRuntimeTuningOverrides, DeletedInodeCleanup,
    DirtyAttrPersistPolicy, EagerFlushOutcome, FAST_MODE_SYNC_GROUP_MAX_DELAY_MS,
    FAST_MODE_SYNC_GROUP_MIN_BYTES_CEIL, FAST_MODE_SYNC_GROUP_MIN_BYTES_FLOOR, FsyncOutcome,
    HandleReleaseContext, LastHandleReleaseAction, LocalSyncBarrierOutcome, LocalSyncPolicy,
    PAYLOAD_PUT_CONCURRENCY_MAX, PAYLOAD_PUT_CONCURRENCY_TARGET_BYTES, StickyErrorPolicy,
    UnlinkTarget, WRITE_EAGER_DRAIN_CHUNK_MAX_BYTES, write_path_sync_trigger,
};
use file_session::{FileSessionOperation, FileSessionState};
use handle_table::HandleTable;
use ignored_entries::IgnoredEntryRegistry;
use locking::{DistributedFlock, FileLockState};
use namespace::{DentryCacheValue, NamespaceState};
use overlay::{ExtentRecordId, PendingUploadCatalog};
use prefetch::{
    PrefetchBatch, PrefetchHintRange, PrefetchHintUpdate, PrefetchReadContext, PrefetchWorkItem,
    PrefetchWorker,
};
use startup::StartupChannels;
use startup::StartupState;

use crate::access::{
    AccessClass, AccessClassifierConfig, AccessDecision, AccessPolicy, HandleAccessState,
};
use crate::incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
use crate::qos::{FairInodeScheduler, QosController, TransitMemoryBudget};
use crate::readahead::{DirAccessTracker, ReadAheadTracker, ReadStreamKey};

use crate::runtime::{
    LocalCacheIoTiming, MetaExecutor, TimeoutOrCancel, cache_io_execution_model,
    run_local_cache_io, run_local_cache_io_blocking, run_meta, run_with_timeout_or_cancel,
};
use crate::utils::{now_unix_millis, now_unix_secs};
use crate::vfs::write_buffer::WriteBufferState;
use bytes::{Buf, Bytes, BytesMut};
use futures::{FutureExt, future::join_all};
use opendal::options::WriteOptions;
use opendal::{Buffer, EntryMode, ErrorKind, Operator};
use parking_lot::{Mutex, RwLock};
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::OpenLocalBlockBackendError;
use studiofs_cache::block_cache::{BlockCache, BlockCacheConfig, CancelUploadMode, L2IoPriority};
use studiofs_cache::metrics::{LatencyHistogram, LatencyHistogramSnapshot, MaterializeBatchStats};
use studiofs_cache::open_local_block_backend_with_budget_and_sync_mode;
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_cache::v2::LocalBlockBackend;
use studiofs_cache::v3::CacheV3PayloadSyncMode;
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys,
    parse_object_id_from_key,
};
use studiofs_data::store::{DataError, DataStore, DataStoreConfig, IoProfile};

use studiofs_meta::schema::{
    DEFAULT_CHUNK_SIZE, DirEntry, FormatInfo, ROOT_INO, SetattrArgs,
    UploadState as MetaUploadState, chunk_start_offset,
};
use studiofs_meta::types::{
    AccessLevel, BlockFlags as MetaBlockFlags, BlockRef, FileType, Ino, InodeAttr,
    block_index_from_offset, offset_within_block,
};
use studiofs_meta::{AsyncMetaEngine, MetaEngine, MetaError, MetaResult, SyncMetaEngineAdapter};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::ingress_wal::{
    IngressWal, latest_ingress_checkpoint_path, load_latest_ingress_checkpoint_meta,
    reset_ingress_artifacts_for_fast_mode, scan_ingress_artifacts,
};

// =============================================================================
// Error Types
// =============================================================================

#[derive(Debug, Error)]
pub enum VfsError {
    #[error("metadata error: {0}")]
    Meta(#[from] studiofs_meta::MetaError),

    #[error("data error: {0}")]
    Data(#[from] studiofs_data::store::DataError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("object error: {0}")]
    Object(#[from] studiofs_data::object::ObjectError),

    #[error("block not found: inode {0}, block {1}")]
    BlockNotFound(Ino, u32),

    #[error("lock conflict (EAGAIN)")]
    WouldBlock,

    #[error("remote content is still uploading (EAGAIN)")]
    RemoteContentPending,

    #[error("invalid filename: {0}")]
    InvalidName(String),

    #[error("qos permit failure at {stage} for object {object_id} (0x{object_id:016x}): {message}")]
    QosFailure {
        stage: Cow<'static, str>,
        object_id: u64,
        message: String,
    },

    #[error("fetch error: {0}")]
    Fetch(#[from] crate::fetch::FetchError),

    #[error("cache backend detection failed for root '{cache_root}': {source}")]
    CacheBackendDetection {
        cache_root: String,
        #[source]
        source: std::io::Error,
    },

    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Copy)]
enum WinfspBarrierType {
    Cleanup,
    Flush,
    Close,
}

pub type VfsResult<T> = Result<T, VfsError>;

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

fn is_probable_trash_component(name: &[u8]) -> bool {
    matches!(name, b".Trash" | b".Trashes") || name.starts_with(b".Trash-")
}

fn is_ignored_macos_metadata(name: &[u8]) -> bool {
    name == b".DS_Store" || (name.len() > 2 && name.starts_with(b"._"))
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
// JuiceFS-like high-level read tuning default:
// - buffer-size: 2048M
const DEFAULT_READ_BUFFER_MB: u64 = 2_048;
// Block-level derived defaults (used as fallback if conversion is unavailable).
const DEFAULT_PREFETCH_CONCURRENCY: usize = 128;
const DEFAULT_READAHEAD_WIDTH: u32 = 128;
const DEFAULT_READAHEAD_MIN_PREFETCH: u32 = 16;
const DEFAULT_READAHEAD_SEQ_STEP: u8 = 4;
const DEFAULT_READAHEAD_RANDOM_PENALTY: u8 = 2;
const DEFAULT_PREFETCH_FOREGROUND_RESERVE: usize = 16;
const DEFAULT_PREFETCH_MAX_PER_CYCLE: usize = 256;
const DEFAULT_PREFETCH_PAUSE_ON_NETERR_MS: u64 = 250;
const DEFAULT_PREFETCH_CONCURRENCY_MIN: usize = 16;
const DEFAULT_DIR_PREFETCH_MAX_BATCH: usize = 0;
const DEFAULT_READDIR_ATTR_BATCH_CHUNK: usize = 256;
const DEFAULT_READDIR_ATTR_BATCH_MAX_INODES: usize = 4096;
const DEFAULT_UPLOAD_DELAY_MS: u64 = 2000;
const DEFAULT_UPLOAD_REQUEUE_BASE_MS: u64 = 5000;
const DEFAULT_UPLOAD_REQUEUE_MAX_MS: u64 = 120_000;
const DEFAULT_UPLOAD_STATS_INTERVAL_SECS: u64 = 1;
const DEFAULT_UPLOAD_CACHE_IO_SLOTS: usize = 32;
const DEFAULT_TRANSIT_MEMORY_BUDGET_BYTES: usize = 0;
const DEFAULT_TRANSIT_MEMORY_BUDGET_RATIO_PERCENT: u64 = 80;
const DEFAULT_FETCH_PREFETCH_ACQUIRE_WAIT_MS: u64 = 75;
const DEFAULT_UPLOAD_USER_IDLE_GRACE_MS: u64 = 500;
const DEFAULT_UPLOAD_FG_READ_LATENCY_TARGET_MS: u64 = 35;
const DEFAULT_UPLOAD_FG_READ_LATENCY_WINDOW_MS: u64 = 1_500;
const DEFAULT_UPLOAD_FG_READ_LATENCY_STICKY_WINDOWS: u64 = 2;
const DEFAULT_MAX_DIRTY_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_FETCH_INITIAL_WINDOW: f64 = 0.0;
const DEFAULT_FETCH_MIN_WINDOW: f64 = 1.0;
const DEFAULT_FETCH_MAX_WINDOW: f64 = 0.0;
const DEFAULT_FETCH_INCREASE_STEP: f64 = 0.0;
const DEFAULT_FETCH_DECREASE_FACTOR: f64 = 0.85;
const DEFAULT_FETCH_DECREASE_COOLDOWN_SECS: u64 = 3;
const DEFAULT_UPLOAD_INITIAL_WINDOW: f64 = 16.0;
const DEFAULT_UPLOAD_MIN_WINDOW: f64 = 1.0;
const DEFAULT_UPLOAD_MAX_WINDOW: f64 = 512.0;
const DEFAULT_UPLOAD_INCREASE_STEP: f64 = 2.0;
const DEFAULT_UPLOAD_DECREASE_FACTOR: f64 = 0.85;
const DEFAULT_UPLOAD_DECREASE_COOLDOWN_SECS: u64 = 3;
const DEFAULT_SLAB_FLUSH_INTERVAL_MS: u64 = 10;
const DEFAULT_UPLOAD_WORKER_COUNT: usize = 128;
const MIN_UPLOAD_WORKER_COUNT: usize = 1;
const DEFAULT_UPLOAD_MAX_ATTEMPTS: u32 = 10;
const DEFAULT_UPLOAD_REQUEST_TIMEOUT_SECS: u64 = 120;
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
const DEFAULT_QOS_UPLOAD_SLOTS: usize = 4;
const DEFAULT_QOS_COMMIT_SLOTS: usize = 4;
const DEFAULT_QOS_FETCH_SLOTS: usize = 128;
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
const DEFAULT_INGRESS_SEGMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_INGRESS_CHECKPOINT_INTERVAL_SECS: u64 = 30;
const DEFAULT_INGRESS_CHECKPOINT_MIN_APPEND_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_INGRESS_CHECKPOINT_MIN_MUTATIONS: u64 = 128;
const DEFAULT_INGRESS_CHECKPOINT_MAX_SEGMENT_LAG: u64 = 2;
const DEFAULT_INGRESS_CHECKPOINT_MAX_LIVE_SEGMENTS: u64 = 4;
const DEFAULT_XATTR_MAX_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_SHUTDOWN_METADATA_FLUSH_TIMEOUT_SECS: u64 = 10;
const DEFAULT_METADATA_INVALIDATION_SYNC_INTERVAL_MS: u64 = 500;
const DEFAULT_LOCK_LEASE_TTL: Duration = Duration::from_secs(120);
const DEFAULT_LOCK_RETRY_BASE_MS: u64 = 25;
const DEFAULT_LOCK_RETRY_MAX_MS: u64 = 500;
const CACHE_RUNTIME_MIN_DIRTY_BYTES: usize = 256 * 1024 * 1024;
const CACHE_RUNTIME_MAX_DIRTY_BYTES_BASE: usize = 2 * 1024 * 1024 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TwoTierSegment {
    Probationary,
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

    #[cfg(test)]
    fn contains(&self, ino: Ino) -> bool {
        self.inner.contains_key(&ino)
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn ingress_checkpoint_due_with_policy(
    pending_bytes: u64,
    pending_mutations: u64,
    min_append_bytes: u64,
    min_mutations: u64,
    active_segment_lag: u64,
    max_segment_lag: u64,
    live_segment_count: u64,
    max_live_segments: u64,
    timer_fired: bool,
) -> bool {
    if pending_bytes == 0 && pending_mutations == 0 {
        return false;
    }
    timer_fired
        || pending_bytes >= min_append_bytes
        || pending_mutations >= min_mutations
        || active_segment_lag >= max_segment_lag
        || live_segment_count >= max_live_segments
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
            // because it holds `Arc<dyn MetaEngine>` (and transitively
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

type IngressRecoveryInit = Option<(u64, u64, u64)>;

fn init_ingress_recovery_state(
    cache_safety_mode: CacheSafetyMode,
    ingress_wal: &IngressWal,
) -> VfsResult<IngressRecoveryInit> {
    if matches!(cache_safety_mode, CacheSafetyMode::Durable) {
        let existing_checkpoint = load_latest_ingress_checkpoint_meta(
            ingress_wal.checkpoint_dir(),
            ingress_wal.segment_dir(),
        )?;
        let initial_checkpoint_meta = existing_checkpoint.map(|checkpoint| {
            let checkpoint_path = latest_ingress_checkpoint_path(ingress_wal.checkpoint_dir());
            let checkpoint_unix_ms = std::fs::metadata(checkpoint_path)
                .ok()
                .and_then(|meta| meta.modified().ok())
                .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            (
                checkpoint_unix_ms,
                checkpoint.covered_segment_id,
                checkpoint.covered_offset,
            )
        });
        return Ok(initial_checkpoint_meta);
    }

    let (segment_count, checkpoint_count, state_present) =
        scan_ingress_artifacts(ingress_wal.spill_dir())?;
    if segment_count > 0 || checkpoint_count > 0 || state_present {
        warn!(
            mode = "fast",
            segment_count,
            checkpoint_count,
            state_present,
            "startup detected durable ingress artifacts; fast mode will discard them during self-heal"
        );
    }
    reset_ingress_artifacts_for_fast_mode(ingress_wal.spill_dir())?;
    Ok(None)
}

// =============================================================================
// Internal State Subsystems
// =============================================================================

struct IngressState {
    ingress_checkpoint_persists: AtomicU64,
    ingress_checkpoint_failures: AtomicU64,
    ingress_last_checkpoint_unix_ms: AtomicU64,
    ingress_last_checkpoint_segment_id: AtomicU64,
    ingress_last_checkpoint_offset: AtomicU64,
    ingress_checkpoint_notify: tokio::sync::Notify,
    ingress_checkpoint_stopped: std::sync::atomic::AtomicBool,
}

impl IngressState {
    fn new(initial_checkpoint_meta: Option<(u64, u64, u64)>) -> Self {
        Self {
            ingress_checkpoint_persists: AtomicU64::new(0),
            ingress_checkpoint_failures: AtomicU64::new(0),
            ingress_last_checkpoint_unix_ms: AtomicU64::new(
                initial_checkpoint_meta
                    .map(|(unix_ms, _, _)| unix_ms)
                    .unwrap_or(0),
            ),
            ingress_last_checkpoint_segment_id: AtomicU64::new(
                initial_checkpoint_meta
                    .map(|(_, segment_id, _)| segment_id)
                    .unwrap_or(0),
            ),
            ingress_last_checkpoint_offset: AtomicU64::new(
                initial_checkpoint_meta
                    .map(|(_, _, offset)| offset)
                    .unwrap_or(0),
            ),
            ingress_checkpoint_notify: tokio::sync::Notify::new(),
            ingress_checkpoint_stopped: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

struct UploadState {
    deleted_inodes: Mutex<std::collections::HashSet<Ino>>,
    upload_cancel_tokens: Mutex<std::collections::HashMap<Ino, CancellationToken>>,
    pending_evictions: Mutex<std::collections::HashSet<u64>>,
    deferred_deleted_local_blocks:
        Mutex<std::collections::HashMap<Ino, std::collections::HashSet<u64>>>,
    pending_epoch_finalizations: Mutex<BTreeSet<(Ino, u64)>>,
    pending_epoch_finalizations_notify: tokio::sync::Notify,
    upload_sticky_errors: Mutex<std::collections::HashMap<Ino, String>>,
    upload_requeues: AtomicU64,
    upload_canceled_deleted: AtomicU64,
    upload_canceled_stale: AtomicU64,
    upload_fallback_deferrals: AtomicU64,
    upload_fallback_rereads: AtomicU64,
    upload_fallback_reread_bytes: AtomicU64,
    upload_fallback_reread_cache_misses: AtomicU64,
    upload_fallback_wait_ns: AtomicU64,
    upload_fallback_wait_max_ns: AtomicU64,
    upload_fallback_read_ns: AtomicU64,
    upload_fallback_read_max_ns: AtomicU64,
    /// Dynamic slot cost adjusted from buffered-first foreground-read pressure.
    upload_cache_io_foreground_cost: AtomicU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CacheRuntimeTuning {
    max_dirty_bytes: usize,
    write_eager_flush_batch_bytes: usize,
    max_ingress_segment_bytes: u64,
    ingress_checkpoint_interval_secs: u64,
    ingress_checkpoint_min_append_bytes: u64,
    ingress_checkpoint_min_mutations: u64,
    ingress_checkpoint_max_segment_lag: u64,
    ingress_checkpoint_max_live_segments: u64,
    upload_cache_io_slots: usize,
    transit_memory_budget_bytes: usize,
}

fn derive_cache_runtime_tuning(
    cache_size: u64,
    block_size: u32,
    block_cache_l1_limit: u64,
) -> CacheRuntimeTuning {
    let cache_size = cache_size.max(1);
    let block_size = u64::from(block_size.max(1));

    let clamp_usize = |value: u64, min: usize, max: usize| -> usize {
        value.clamp(min as u64, max as u64).min(usize::MAX as u64) as usize
    };
    let clamp_u64 = |value: u64, min: u64, max: u64| -> u64 { value.clamp(min, max) };

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

    let max_ingress_segment_bytes = clamp_u64(
        cache_size / 512,
        CACHE_RUNTIME_MIN_INGRESS_SEGMENT_BYTES,
        CACHE_RUNTIME_MAX_INGRESS_SEGMENT_BYTES,
    );
    let ingress_checkpoint_interval_secs = if cache_size <= CACHE_RUNTIME_SMALL_BUDGET_BYTES {
        15
    } else if cache_size <= CACHE_RUNTIME_LARGE_BUDGET_BYTES {
        30
    } else {
        45
    };
    let ingress_checkpoint_min_append_bytes = clamp_u64(
        max_ingress_segment_bytes / 4,
        16 * 1024 * 1024,
        128 * 1024 * 1024,
    );
    let ingress_checkpoint_min_mutations =
        clamp_u64(ingress_checkpoint_min_append_bytes / block_size, 64, 4_096);
    let ingress_checkpoint_max_segment_lag = if cache_size <= CACHE_RUNTIME_SMALL_BUDGET_BYTES {
        2
    } else {
        3
    };
    let ingress_checkpoint_max_live_segments = if cache_size <= CACHE_RUNTIME_SMALL_BUDGET_BYTES {
        4
    } else {
        6
    };

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
    let from_l1_budget = if block_cache_l1_limit == 0 {
        0
    } else {
        ((block_cache_l1_limit as u128)
            .saturating_mul(DEFAULT_TRANSIT_MEMORY_BUDGET_RATIO_PERCENT as u128)
            / 100)
            .min(u64::MAX as u128) as u64
    };
    let transit_memory_budget_bytes = clamp_usize(
        from_cache_budget.max(from_l1_budget).max(block_size),
        CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES.max(block_size as usize),
        CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES,
    );

    CacheRuntimeTuning {
        max_dirty_bytes,
        write_eager_flush_batch_bytes,
        max_ingress_segment_bytes,
        ingress_checkpoint_interval_secs,
        ingress_checkpoint_min_append_bytes,
        ingress_checkpoint_min_mutations,
        ingress_checkpoint_max_segment_lag,
        ingress_checkpoint_max_live_segments,
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
        max_ingress_segment_bytes: if overrides.max_ingress_segment_bytes {
            config.max_ingress_segment_bytes
        } else {
            derived.max_ingress_segment_bytes
        },
        ingress_checkpoint_interval_secs: if overrides.ingress_checkpoint_interval_secs {
            config.ingress_checkpoint_interval_secs
        } else {
            derived.ingress_checkpoint_interval_secs
        },
        ingress_checkpoint_min_append_bytes: if overrides.ingress_checkpoint_min_append_bytes {
            config.ingress_checkpoint_min_append_bytes
        } else {
            derived.ingress_checkpoint_min_append_bytes
        },
        ingress_checkpoint_min_mutations: if overrides.ingress_checkpoint_min_mutations {
            config.ingress_checkpoint_min_mutations
        } else {
            derived.ingress_checkpoint_min_mutations
        },
        ingress_checkpoint_max_segment_lag: if overrides.ingress_checkpoint_max_segment_lag {
            config.ingress_checkpoint_max_segment_lag
        } else {
            derived.ingress_checkpoint_max_segment_lag
        },
        ingress_checkpoint_max_live_segments: if overrides.ingress_checkpoint_max_live_segments {
            config.ingress_checkpoint_max_live_segments
        } else {
            derived.ingress_checkpoint_max_live_segments
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
    write_eager_flush_batch_bytes: usize,
    write_eager_flush_drain_chunk_bytes: usize,
) -> u64 {
    let target = (write_eager_flush_drain_chunk_bytes as u64).saturating_mul(4);
    let target = target.max(write_eager_flush_batch_bytes as u64);
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
            deleted_inodes: Mutex::new(std::collections::HashSet::new()),
            upload_cancel_tokens: Mutex::new(std::collections::HashMap::new()),
            pending_evictions: Mutex::new(std::collections::HashSet::new()),
            deferred_deleted_local_blocks: Mutex::new(std::collections::HashMap::new()),
            pending_epoch_finalizations: Mutex::new(BTreeSet::new()),
            pending_epoch_finalizations_notify: tokio::sync::Notify::new(),
            upload_sticky_errors: Mutex::new(std::collections::HashMap::new()),
            upload_requeues: AtomicU64::new(0),
            upload_canceled_deleted: AtomicU64::new(0),
            upload_canceled_stale: AtomicU64::new(0),
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
    inode_incarnations: Mutex<std::collections::HashMap<Ino, InodeIncarnationState>>,
    live_extent_records: Mutex<std::collections::HashMap<(Ino, u32), ExtentRecordId>>,
}

impl IncarnationState {
    fn new() -> Self {
        Self {
            inode_incarnations: Mutex::new(std::collections::HashMap::new()),
            live_extent_records: Mutex::new(std::collections::HashMap::new()),
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

struct ReadState {
    foreground_reads_inflight: AtomicUsize,
    last_foreground_read_ms: AtomicU64,
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
    active_access_classes: [AtomicU64; AccessClass::COUNT],
    access_class_transitions: [AtomicU64; AccessClass::COUNT],
}

impl ReadState {
    fn new(shard_count: usize) -> Self {
        Self {
            foreground_reads_inflight: AtomicUsize::new(0),
            last_foreground_read_ms: AtomicU64::new(0),
            foreground_read_latency_shards: (0..shard_count)
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
            foreground_read_shard_cursor: AtomicU64::new(0),
            upload_fg_read_last_p90_ns: AtomicU64::new(0),
            upload_fg_read_last_p90_at_ms: AtomicU64::new(0),
            prefetch_last_hint: Mutex::new(HashMap::new()),
            read_access_fallback: Mutex::new(HashMap::new()),
            write_access_fallback: Mutex::new(HashMap::new()),
            active_access_classes: std::array::from_fn(|_| AtomicU64::new(0)),
            access_class_transitions: std::array::from_fn(|_| AtomicU64::new(0)),
        }
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
        classifier: AccessClassifierConfig,
    ) -> AccessDecision {
        if let Some(open_file) = open_file {
            let decision = open_file.observe_read(offset, len, classifier);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, false);
        let decision = state.lock().observe_read(offset, len, classifier);
        self.record_access_class_transition(decision.previous_class, decision.class, false);
        decision
    }

    fn observe_write_access(
        &self,
        open_file: Option<&OpenFile>,
        ino: Ino,
        offset: u64,
        len: usize,
        classifier: AccessClassifierConfig,
    ) -> AccessDecision {
        if let Some(open_file) = open_file {
            let decision = open_file.observe_write(offset, len, classifier);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, true);
        let decision = state.lock().observe_write(offset, len, classifier);
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
    ) -> bool {
        let mut last_hints = self.prefetch_last_hint.lock();
        match range.update_against(last_hints.get(&(ino, stream_id)).copied()) {
            PrefetchHintUpdate::Suppress => false,
            PrefetchHintUpdate::Store(stored) => {
                last_hints.insert((ino, stream_id), stored.as_stored());
                true
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
        self.foreground_reads_inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        self.last_foreground_read_ms
            .store(now_ms, Ordering::Relaxed);
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
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    uploader_task: Mutex<Option<JoinHandle<()>>>,
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
            background_tasks: Mutex::new(Vec::new()),
            uploader_task: Mutex::new(None),
        }
    }
}

struct MetricsState {
    materialize_batch_stats: MaterializeBatchStats,
    ignored_entries: Mutex<IgnoredEntryRegistry>,
    next_ignored_ino: AtomicU64,
    /// Current number of items queued in the prefetch worker's pending queue.
    /// Useful for detecting backpressure under high handle-count workloads.
    prefetch_queue_depth: AtomicU64,
    flush_ordered_calls: AtomicU64,
    local_cache_io_calls: AtomicU64,
    local_cache_io_handoff_total_ns: AtomicU64,
    local_cache_io_handoff_max_ns: AtomicU64,
    local_cache_io_handoff_histogram: LatencyHistogram,
    local_cache_io_backend_total_ns: AtomicU64,
    local_cache_io_backend_max_ns: AtomicU64,
    local_cache_io_backend_histogram: LatencyHistogram,
    block_store_sync_data_calls: AtomicU64,
    block_store_sync_data_deferred_calls: AtomicU64,
    block_store_sync_data_synced_bytes: AtomicU64,
    block_store_sync_data_total_ns: AtomicU64,
    block_store_sync_data_max_ns: AtomicU64,
    block_store_sync_data_histogram: LatencyHistogram,
    block_store_sync_data_queue_wait_calls: AtomicU64,
    block_store_sync_data_queue_wait_total_ns: AtomicU64,
    block_store_sync_data_queue_wait_max_ns: AtomicU64,
    block_store_sync_data_queue_wait_histogram: LatencyHistogram,
    block_store_sync_data_last_completed_ns: AtomicU64,
    block_store_sync_data_interval_total_ns: AtomicU64,
    block_store_sync_data_interval_max_ns: AtomicU64,
    block_store_sync_data_interval_histogram: LatencyHistogram,
    payload_persist_calls: AtomicU64,
    payload_persist_total_ns: AtomicU64,
    payload_persist_max_ns: AtomicU64,
    payload_persist_histogram: LatencyHistogram,
    payload_burst_count: AtomicU64,
    payload_burst_total_blocks: AtomicU64,
    payload_burst_total_bytes: AtomicU64,
    payload_burst_max_blocks: AtomicU64,
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
    winfsp_write_callbacks: AtomicU64,
    winfsp_overwrite_callbacks: AtomicU64,
    winfsp_cleanup_callbacks: AtomicU64,
    winfsp_flush_callbacks: AtomicU64,
    winfsp_close_callbacks: AtomicU64,
    winfsp_cleanup_flush_fh_callbacks: AtomicU64,
    winfsp_cleanup_barrier_calls: AtomicU64,
    winfsp_flush_barrier_calls: AtomicU64,
    winfsp_close_barrier_calls: AtomicU64,
    winfsp_cleanup_barrier_skipped_clean: AtomicU64,
    winfsp_flush_barrier_skipped_clean: AtomicU64,
    winfsp_close_barrier_skipped_clean: AtomicU64,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            materialize_batch_stats: MaterializeBatchStats::default(),
            ignored_entries: Mutex::new(IgnoredEntryRegistry::default()),
            next_ignored_ino: AtomicU64::new(1u64 << 63),
            prefetch_queue_depth: AtomicU64::new(0),
            flush_ordered_calls: AtomicU64::new(0),
            local_cache_io_calls: AtomicU64::new(0),
            local_cache_io_handoff_total_ns: AtomicU64::new(0),
            local_cache_io_handoff_max_ns: AtomicU64::new(0),
            local_cache_io_handoff_histogram: LatencyHistogram::default(),
            local_cache_io_backend_total_ns: AtomicU64::new(0),
            local_cache_io_backend_max_ns: AtomicU64::new(0),
            local_cache_io_backend_histogram: LatencyHistogram::default(),
            block_store_sync_data_calls: AtomicU64::new(0),
            block_store_sync_data_deferred_calls: AtomicU64::new(0),
            block_store_sync_data_synced_bytes: AtomicU64::new(0),
            block_store_sync_data_total_ns: AtomicU64::new(0),
            block_store_sync_data_max_ns: AtomicU64::new(0),
            block_store_sync_data_histogram: LatencyHistogram::default(),
            block_store_sync_data_queue_wait_calls: AtomicU64::new(0),
            block_store_sync_data_queue_wait_total_ns: AtomicU64::new(0),
            block_store_sync_data_queue_wait_max_ns: AtomicU64::new(0),
            block_store_sync_data_queue_wait_histogram: LatencyHistogram::default(),
            block_store_sync_data_last_completed_ns: AtomicU64::new(0),
            block_store_sync_data_interval_total_ns: AtomicU64::new(0),
            block_store_sync_data_interval_max_ns: AtomicU64::new(0),
            block_store_sync_data_interval_histogram: LatencyHistogram::default(),
            payload_persist_calls: AtomicU64::new(0),
            payload_persist_total_ns: AtomicU64::new(0),
            payload_persist_max_ns: AtomicU64::new(0),
            payload_persist_histogram: LatencyHistogram::default(),
            payload_burst_count: AtomicU64::new(0),
            payload_burst_total_blocks: AtomicU64::new(0),
            payload_burst_total_bytes: AtomicU64::new(0),
            payload_burst_max_blocks: AtomicU64::new(0),
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
            winfsp_write_callbacks: AtomicU64::new(0),
            winfsp_overwrite_callbacks: AtomicU64::new(0),
            winfsp_cleanup_callbacks: AtomicU64::new(0),
            winfsp_flush_callbacks: AtomicU64::new(0),
            winfsp_close_callbacks: AtomicU64::new(0),
            winfsp_cleanup_flush_fh_callbacks: AtomicU64::new(0),
            winfsp_cleanup_barrier_calls: AtomicU64::new(0),
            winfsp_flush_barrier_calls: AtomicU64::new(0),
            winfsp_close_barrier_calls: AtomicU64::new(0),
            winfsp_cleanup_barrier_skipped_clean: AtomicU64::new(0),
            winfsp_flush_barrier_skipped_clean: AtomicU64::new(0),
            winfsp_close_barrier_skipped_clean: AtomicU64::new(0),
        }
    }
}

// =============================================================================
// Internal State (composed subsystems)
// =============================================================================

impl VfsState {
    fn new(
        initial_checkpoint_meta: Option<(u64, u64, u64)>,
        _block_size: u32,
        cfg: &VfsConfig,
        upload_cache_io_foreground_cost_base: u32,
    ) -> Self {
        Self {
            ingress: IngressState::new(initial_checkpoint_meta),
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
            write_buffer: WriteBufferState::new(),
            read: ReadState::new(cfg.foreground_read_speedometer_shards),
            file_lock: FileLockState::new(),
            shutdown: ShutdownState::new(),
            metrics: MetricsState::new(),
        }
    }
}

struct VfsState {
    ingress: IngressState,
    upload: UploadState,
    file_session: FileSessionState,
    namespace: NamespaceState,
    acl: AclState,
    incarnation: IncarnationState,
    write_buffer: WriteBufferState,
    read: ReadState,
    file_lock: FileLockState,
    shutdown: ShutdownState,
    metrics: MetricsState,
}

#[derive(Clone)]
struct JanitorLease {
    op: Operator,
    lock_path: String,
    owner_id: String,
}

#[derive(Debug, Clone)]
struct ReadRequestContext {
    attr: InodeAttr,
    pending_content_epoch: Option<u64>,
}

impl ReadRequestContext {
    fn from_attr(vfs: &StudioVfs, ino: Ino, attr: InodeAttr) -> Self {
        let incarnation_state = vfs.refresh_inode_incarnation_state_from_attr(ino, &attr);
        Self {
            pending_content_epoch: incarnation_state
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch),
            attr,
        }
    }

    fn placeholder_visible(&self, vfs: &StudioVfs, ino: Ino) -> bool {
        vfs.remote_upload_placeholder_visible(ino, &self.attr)
    }

    fn read_len(&self, offset: u64, requested_len: usize) -> usize {
        self.attr
            .size
            .min(offset.saturating_add(requested_len as u64))
            .saturating_sub(offset) as usize
    }

    fn prefetch_context(&self) -> PrefetchReadContext {
        PrefetchReadContext {
            size: self.attr.size,
            pending_content_epoch: self.pending_content_epoch,
        }
    }
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
    let cloud_logical_block_capacity = CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT;
    if (block_size as usize) > cloud_logical_block_capacity {
        return Err(VfsError::Other(format!(
            "volume block_size {} exceeds 4x4 cloud fragment capacity {} ({} fragments x {} bytes)",
            block_size, cloud_logical_block_capacity, CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE
        )));
    }
    Ok(())
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
    (blocks as u32).max(32)
}

fn derive_readahead_max_width(
    read_buffer_mb: u64,
    block_size: u32,
    readahead_base_width: u32,
) -> u32 {
    derive_buffer_blocks_default(read_buffer_mb, block_size)
        .max(readahead_base_width.saturating_mul(8))
        .max(256)
}

fn build_fetch_aimd_config(
    readahead_max_width: u32,
    config: &VfsConfig,
) -> crate::aimd::AimdConfig {
    let fetch_max_window_default = (readahead_max_width as f64).clamp(128.0, 4096.0);
    let fetch_initial_window_default = (fetch_max_window_default / 8.0).clamp(16.0, 256.0);
    let fetch_increase_default = if fetch_max_window_default >= 1024.0 {
        4.0
    } else {
        2.0
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
    }
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
    }
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
    /// Async boundary for metadata operations.
    meta_async: Arc<dyn AsyncMetaEngine>,
    /// Preferred metadata execution boundary for new call sites.
    meta: MetaExecutor,
    /// Data store for writes (uploads, deletes, GC).
    data_write: Arc<DataStore>,
    /// Shared local block backend used by the foreground read path.
    local_block_backend: Arc<dyn LocalBlockBackend>,
    /// Upload queue and pending-upload pinning for the write path.
    block_cache: Arc<BlockCache>,
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
    /// Handle-aware access classifier tuning derived from VFS read settings.
    access_classifier: AccessClassifierConfig,
    /// Grace window after the last foreground read to keep user priority.
    upload_user_idle_grace: Duration,
    /// Upload worker pool size.
    upload_worker_count: usize,
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
    /// Maximum bytes drained by one opportunistic eager-flush attempt.
    write_eager_flush_drain_chunk_bytes: usize,
    /// Bounded local payload-put concurrency for one eager drain.
    payload_put_concurrency: usize,
    /// Configured local cache safety mode for write-path durability behavior.
    cache_safety_mode: CacheSafetyMode,
    /// Fast-mode group-commit minimum bytes before forcing local sync.
    local_sync_group_min_bytes: u64,
    /// Fast-mode group-commit maximum delay before forcing local sync.
    local_sync_group_max_delay: Duration,
    /// Maximum bytes allowed for a single xattr value.
    max_xattr_bytes: usize,
    ingress_wal: IngressWal,
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
    prefetch_foreground_reserve: usize,
    prefetch_max_per_cycle: usize,
    prefetch_pause_on_neterr: Duration,
    /// Adaptive fetch scheduler (AIMD + singleflight).
    /// Replaces the old `FetchThrottle`, `inflight_fetches`, and VFS-retry.
    fetch_scheduler: Arc<crate::fetch::FetchScheduler>,
    /// Optional serverless distributed lock manager for bimodal consistency.
    distributed_flock: Option<DistributedFlock>,
    /// Optional kernel cache invalidator (set by platform bridge after mount).
    kernel_invalidator: RwLock<Option<Arc<dyn KernelInvalidator>>>,
    write_path_backend_tag: String,
    write_path_started_at: Instant,
}

struct ForegroundReadScope<'a> {
    read_state: &'a ReadState,
    latency_window: Duration,
    started: Instant,
    shard_idx: usize,
    bytes_read: u64,
    access_class: AccessClass,
    policy: AccessPolicy,
}

impl<'a> ForegroundReadScope<'a> {
    fn new(read_state: &'a ReadState, latency_window: Duration, policy: AccessPolicy) -> Self {
        let now_ms = now_unix_millis();
        read_state.mark_foreground_read(now_ms);
        let shard_idx = read_state.select_foreground_latency_shard();
        Self {
            read_state,
            latency_window,
            started: Instant::now(),
            shard_idx,
            bytes_read: 0,
            access_class: policy.class,
            policy,
        }
    }

    fn record_bytes(&mut self, bytes_read: u64) {
        self.bytes_read = bytes_read;
    }
}

#[derive(Debug, Clone, Copy)]
struct ForegroundReadTelemetry {
    bytes_read: u64,
    elapsed: Duration,
    access_class: AccessClass,
    policy: AccessPolicy,
}

impl Drop for ForegroundReadScope<'_> {
    fn drop(&mut self) {
        let now_ms = now_unix_millis();
        self.read_state.record_foreground_read_latency(
            self.shard_idx,
            ForegroundReadTelemetry {
                bytes_read: self.bytes_read,
                elapsed: self.started.elapsed(),
                access_class: self.access_class,
                policy: self.policy,
            },
            now_ms,
            self.latency_window,
        );
        self.read_state.unmark_foreground_read(now_ms);
    }
}

struct StartupReplayCoordinator<'a> {
    vfs: &'a StudioVfs,
    pending_catalog: PendingUploadCatalog,
    dirty_set: HashSet<u64>,
    quarantined_inodes: HashSet<Ino>,
    quarantined_object_ids: HashSet<u64>,
    replay_ops: Vec<(u64, u64, u64, u64)>,
    live_extents: HashMap<(Ino, u32), ExtentRecordId>,
    pending_epoch_finalizations: Vec<(Ino, u64)>,
    restored_live_extents: usize,
}

impl<'a> StartupReplayCoordinator<'a> {
    fn new(vfs: &'a StudioVfs, pending_uploads: &[PendingUpload]) -> Self {
        Self {
            vfs,
            pending_catalog: PendingUploadCatalog::from_ops(pending_uploads.iter().cloned()),
            dirty_set: vfs
                .local_block_backend
                .dirty_object_ids()
                .into_iter()
                .collect(),
            quarantined_inodes: HashSet::new(),
            quarantined_object_ids: HashSet::new(),
            replay_ops: Vec::new(),
            live_extents: HashMap::new(),
            pending_epoch_finalizations: Vec::new(),
            restored_live_extents: 0,
        }
    }

    fn run(mut self, attrs: &[InodeAttr]) {
        for attr in attrs {
            self.process_attr(attr);
        }
        self.finish();
    }

    fn process_attr(&mut self, attr: &InodeAttr) {
        let ino = attr.ino;
        let needs_finalization = StudioVfs::needs_epoch_finalization(attr);
        if !self.needs_scan(attr) {
            return;
        }

        let blocks = match self.vfs.sync_meta().get_file_blocks(ino) {
            Ok(blocks) => blocks,
            Err(error) => {
                let error = VfsError::Meta(error);
                if StudioVfs::is_invalid_regular_file_graph_error(&error) {
                    self.handle_quarantine(ino, attr.size, &error);
                } else {
                    warn!(
                        ino,
                        error = %error,
                        "failed to enumerate file blocks during startup recovery"
                    );
                }
                return;
            }
        };

        if self.has_dirty_objects() {
            for (block_index, block_ref) in &blocks {
                if !self.take_dirty_object(block_ref.object_id) {
                    continue;
                }

                self.record_replayed_block(
                    ino,
                    *block_index,
                    block_ref.object_id,
                    attr.content_epoch,
                    self.vfs.block_size,
                );
            }

            if self.has_dirty_objects() && attr.size > 0 {
                match self
                    .vfs
                    .sync_meta()
                    .lookup_regular_file_slices(ino, 0, attr.size)
                {
                    Ok(spans) => {
                        for span in spans {
                            let Some(block) = span.block else {
                                continue;
                            };
                            if !self.take_dirty_object(block.object_id) {
                                continue;
                            }
                            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
                            let absolute_start = chunk_base.saturating_add(span.range.start as u64);
                            let block_index =
                                block_index_from_offset(absolute_start, self.vfs.block_size);
                            self.record_replayed_block(
                                ino,
                                block_index,
                                block.object_id,
                                attr.content_epoch,
                                self.vfs.block_size,
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            ino,
                            error = %error,
                            "failed to enumerate regular-file slices during startup recovery"
                        );
                    }
                }
            }
        }

        if needs_finalization {
            self.record_epoch_finalization(ino, attr.content_epoch, blocks.as_slice());
        }
    }

    fn handle_quarantine(&mut self, ino: Ino, size: u64, error: &VfsError) {
        warn!(
            ino,
            error = %error,
            "quarantining invalid regular-file graph during startup recovery"
        );
        let protected = self
            .vfs
            .quarantine_invalid_regular_file_graph_for_startup_recovery(ino, size, error);
        self.record_quarantine(ino, protected);
    }

    fn needs_scan(&self, attr: &InodeAttr) -> bool {
        !self.dirty_set.is_empty() || StudioVfs::needs_epoch_finalization(attr)
    }

    fn has_dirty_objects(&self) -> bool {
        !self.dirty_set.is_empty()
    }

    fn take_dirty_object(&mut self, object_id: u64) -> bool {
        self.dirty_set.remove(&object_id)
    }

    fn record_quarantine(&mut self, ino: Ino, protected_object_ids: HashSet<u64>) {
        self.quarantined_inodes.insert(ino);
        self.quarantined_object_ids.extend(protected_object_ids);
    }

    fn discard_quarantined_dirty_objects(&mut self) {
        if self.quarantined_object_ids.is_empty() {
            return;
        }
        self.dirty_set
            .retain(|object_id| !self.quarantined_object_ids.contains(object_id));
    }

    fn record_replayed_block(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
        block_size: u32,
    ) {
        let offset = (block_index as u64).saturating_mul(block_size as u64);
        self.replay_ops
            .push((ino, offset, object_id, content_epoch));
        self.record_live_extent(ino, block_index, object_id, content_epoch);
        self.restored_live_extents = self.restored_live_extents.saturating_add(1);
    }

    fn record_epoch_finalization(
        &mut self,
        ino: Ino,
        content_epoch: u64,
        blocks: &[(u32, BlockRef)],
    ) {
        for (block_index, block_ref) in blocks {
            self.record_live_extent(ino, *block_index, block_ref.object_id, content_epoch);
        }
        self.pending_epoch_finalizations.push((ino, content_epoch));
    }

    fn merge_pending_live_extents(&mut self) {
        let live_extents = self
            .pending_catalog
            .live_extents(self.vfs.block_size, &self.quarantined_inodes);
        for (ino, block_index, object_id, content_epoch) in live_extents {
            self.record_pending_live_extent(ino, block_index, object_id, content_epoch);
        }
    }

    fn live_extents_batch(&self) -> Vec<(Ino, u32, u64, u64)> {
        let mut extents: Vec<_> = self
            .live_extents
            .iter()
            .map(|(&(ino, block_index), extent)| {
                (ino, block_index, extent.object_id, extent.content_epoch)
            })
            .collect();
        extents.sort_by_key(|(ino, block_index, _, _)| (*ino, *block_index));
        extents
    }

    fn queued_epoch_count(&self) -> usize {
        self.pending_epoch_finalizations.len()
    }

    fn record_live_extent(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
    ) {
        let next = ExtentRecordId {
            object_id,
            content_epoch,
        };
        match self.live_extents.entry((ino, block_index)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(next);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if next.content_epoch >= entry.get().content_epoch {
                    entry.insert(next);
                }
            }
        }
    }

    fn record_pending_live_extent(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
    ) {
        let next = ExtentRecordId {
            object_id,
            content_epoch,
        };
        match self.live_extents.entry((ino, block_index)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(next);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if next.content_epoch > entry.get().content_epoch {
                    entry.insert(next);
                }
            }
        }
    }

    fn cleanup_orphan_dirty_blocks(&mut self) {
        if self.dirty_set.is_empty() {
            return;
        }
        if !self.quarantined_inodes.is_empty() {
            self.warn_orphan_dirty_blocks();
            return;
        }
        if !self.retain_non_metadata_reachable_dirty_blocks() {
            return;
        }
        self.remove_orphan_dirty_blocks();
    }

    fn retain_non_metadata_reachable_dirty_blocks(&mut self) -> bool {
        // Cache the sync handle once so the recovery error path does
        // not re-enter metadata lookup while we are already cleaning up.
        let meta = self.vfs.sync_meta();
        let reachable_object_ids = match meta.get_referenced_object_ids() {
            Ok(ids) => Some(ids),
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to enumerate metadata reachability during startup recovery; preserving dirty blocks"
                );
                None
            }
        };
        if let Some(reachable_object_ids) = reachable_object_ids {
            self.dirty_set
                .retain(|object_id| !reachable_object_ids.contains(object_id));
            true
        } else {
            self.dirty_set.clear();
            false
        }
    }

    fn remove_orphan_dirty_blocks(&mut self) {
        if self.dirty_set.is_empty() {
            return;
        }
        let orphan_count = self.dirty_set.len() as u64;
        for object_id in &self.dirty_set {
            warn!(
                object_id,
                "removing orphan dirty block without metadata reference"
            );
            self.vfs.local_block_backend.remove(*object_id);
            self.vfs
                .state
                .upload
                .pending_evictions
                .lock()
                .remove(object_id);
        }
        self.vfs
            .state
            .upload
            .upload_canceled_stale
            .fetch_add(orphan_count, Ordering::Relaxed);
        info!(
            orphan_dirty_blocks = self.dirty_set.len(),
            "startup cleanup removed orphan dirty local backend entries"
        );
    }

    fn warn_orphan_dirty_blocks(&self) {
        warn!(
            quarantined_inodes = self.quarantined_inodes.len(),
            orphan_dirty_blocks = self.dirty_set.len(),
            "startup recovery preserved dirty local backend entries because invalid graphs were quarantined"
        );
    }

    fn finish(mut self) {
        self.discard_quarantined_dirty_objects();
        self.merge_pending_live_extents();

        if !self.replay_ops.is_empty() {
            self.vfs.block_cache.submit_uploads_batch(&self.replay_ops);
            info!(
                replayed_dirty_blocks = self.replay_ops.len(),
                restored_live_extents = self.restored_live_extents,
                "replayed dirty local backend blocks into upload queue"
            );
        }

        let live_extents = self.live_extents_batch();
        if !live_extents.is_empty() {
            self.vfs.record_live_extents(&live_extents);
        }

        for (ino, content_epoch) in &self.pending_epoch_finalizations {
            self.vfs.queue_epoch_finalization(*ino, *content_epoch);
        }
        if self.queued_epoch_count() > 0 {
            info!(
                queued_epochs = self.queued_epoch_count(),
                "seeded epoch finalizer queue from metadata"
            );
        }

        self.cleanup_orphan_dirty_blocks();

        if !self.quarantined_inodes.is_empty() {
            self.vfs
                .cancel_pending_uploads_for_inodes(&self.quarantined_inodes);
        }
    }
}

fn map_cache_backend_detection_error(cache_dir: &str, source: std::io::Error) -> VfsError {
    VfsError::CacheBackendDetection {
        cache_root: cache_dir.to_string(),
        source,
    }
}

fn map_cache_backend_provision_error(cache_root: &str, source: std::io::Error) -> VfsError {
    VfsError::Other(format!(
        "failed to provision cache-v3 layout under '{cache_root}': {source}"
    ))
}

fn map_open_local_block_backend_error(
    cache_dir: &str,
    source: OpenLocalBlockBackendError,
) -> VfsError {
    match source {
        OpenLocalBlockBackendError::DetectCacheBackend { source, .. } => {
            map_cache_backend_detection_error(cache_dir, source)
        }
        OpenLocalBlockBackendError::ProvisionCacheV3Layout { cache_root, source } => {
            map_cache_backend_provision_error(&cache_root, source)
        }
        OpenLocalBlockBackendError::OpenCacheV3Backend { source, .. } => {
            VfsError::Other(format!("failed to open cache-v3 backend: {source}"))
        }
    }
}

impl StudioVfs {
    fn attr_read_cache_metrics(&self) -> AttrReadCacheMetrics {
        self.state.namespace.attr_read_cache.lock().metrics()
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
        self.block_cache.spawn_background_tasks();
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

    fn track_background_task(&self, handle: JoinHandle<()>) {
        self.state.shutdown.background_tasks.lock().push(handle);
    }

    fn set_uploader_task(&self, handle: JoinHandle<()>) {
        *self.state.shutdown.uploader_task.lock() = Some(handle);
    }

    fn shutdown_requested(&self) -> bool {
        self.state
            .shutdown
            .shutdown_requested
            .load(Ordering::Acquire)
    }

    fn uploader_shutdown_requested(&self) -> bool {
        self.state
            .shutdown
            .uploader_shutdown_requested
            .load(Ordering::Acquire)
    }

    fn uploader_cancel_token_for_inode(&self, ino: Ino) -> CancellationToken {
        if self.state.upload.deleted_inodes.lock().contains(&ino) {
            let token = CancellationToken::new();
            token.cancel();
            return token;
        }

        let mut tokens = self.state.upload.upload_cancel_tokens.lock();
        tokens
            .entry(ino)
            .or_insert_with(|| self.state.shutdown.uploader_shutdown_token.child_token())
            .clone()
    }

    fn mark_inode_deleted_for_uploads(&self, ino: Ino) {
        // Soft upload cancellation marker: stops background uploads for this inode.
        // Data for still-open unlinked files must remain locally readable until
        // the last handle is released (POSIX open-unlink semantics).
        self.state.upload.deleted_inodes.lock().insert(ino);
        if let Some(token) = self
            .state
            .upload
            .upload_cancel_tokens
            .lock()
            .get(&ino)
            .cloned()
        {
            token.cancel();
        }
    }

    fn record_deferred_deleted_local_block(&self, ino: Ino, object_id: u64) {
        self.state
            .upload
            .deferred_deleted_local_blocks
            .lock()
            .entry(ino)
            .or_default()
            .insert(object_id);
    }

    fn take_deferred_deleted_local_blocks(&self, ino: Ino) -> Vec<u64> {
        self.state
            .upload
            .deferred_deleted_local_blocks
            .lock()
            .remove(&ino)
            .map(|ids| ids.into_iter().collect())
            .unwrap_or_default()
    }

    async fn cancel_upload_for_deleted_inode(&self, op: &PendingUpload) -> VfsResult<()> {
        let has_open_handles = self.handle_table.open_count(op.ino) > 0;
        let mode = if has_open_handles {
            CancelUploadMode::KeepBlock
        } else {
            CancelUploadMode::RemoveBlock
        };
        self.block_cache.cancel_upload(op, mode);
        if has_open_handles {
            self.record_deferred_deleted_local_block(op.ino, op.object_id);
        } else {
            self.block_store_remove_async(op.object_id).await?;
        }
        self.state
            .upload
            .pending_evictions
            .lock()
            .remove(&op.object_id);
        Ok(())
    }

    async fn cancel_upload_if_inode_deleted(&self, op: &PendingUpload) -> VfsResult<bool> {
        if !self.state.upload.deleted_inodes.lock().contains(&op.ino) {
            return Ok(false);
        }
        self.state
            .upload
            .upload_canceled_deleted
            .fetch_add(1, Ordering::Relaxed);
        self.cancel_upload_for_deleted_inode(op).await?;
        self.maybe_clear_deleted_inode_marker(op.ino);
        Ok(true)
    }

    fn maybe_clear_deleted_inode_marker(&self, ino: Ino) {
        if !self.state.upload.deleted_inodes.lock().contains(&ino) {
            return;
        }
        if self.handle_table.open_count(ino) > 0 {
            return;
        }
        if self.block_cache.has_pending_uploads_for_ino(ino) {
            return;
        }
        // Only clear after inode metadata is actually gone. This keeps the
        // marker stable for open-unlink inodes until final orphan cleanup.
        if self.sync_meta().get_attr(ino).is_ok() {
            return;
        }
        self.state.upload.deleted_inodes.lock().remove(&ino);
    }

    fn maybe_release_upload_cancel_token(&self, ino: Ino) {
        let mut tokens = self.state.upload.upload_cancel_tokens.lock();
        if self.block_cache.has_pending_uploads_for_ino(ino) {
            return;
        }
        tokens.remove(&ino);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_requested()
            || self.uploader_shutdown_requested()
            || self
                .state
                .shutdown
                .shutdown_completed
                .load(Ordering::Acquire)
    }

    fn request_uploader_shutdown(&self) {
        if !self
            .state
            .shutdown
            .uploader_shutdown_requested
            .swap(true, Ordering::AcqRel)
        {
            self.state.shutdown.uploader_shutdown_token.cancel();
        }
    }

    async fn join_background_tasks(&self, timeout: Duration) {
        let handles = {
            let mut handles = self.state.shutdown.background_tasks.lock();
            std::mem::take(&mut *handles)
        };
        for mut handle in handles {
            if !handle.is_finished() {
                match tokio::time::timeout(timeout, &mut handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is_cancelled() => {}
                    Ok(Err(e)) => {
                        warn!(error = %e, "background task join failed");
                    }
                    Err(_) => {
                        handle.abort();
                        let _ = handle.await;
                        warn!(
                            timeout_ms = timeout.as_millis() as u64,
                            "background task did not stop in time, aborted"
                        );
                    }
                }
            } else if let Err(e) = handle.await
                && !e.is_cancelled()
            {
                warn!(error = %e, "background task join failed");
            }
        }
    }

    async fn join_uploader_task(&self, timeout: Duration) {
        let handle = self.state.shutdown.uploader_task.lock().take();
        let Some(mut handle) = handle else {
            return;
        };
        match tokio::time::timeout(timeout, &mut handle).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.is_cancelled() => {}
            Ok(Err(e)) => {
                warn!(error = %e, "uploader task join failed");
            }
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "uploader task did not stop in time, aborted"
                );
            }
        }
    }

    fn pending_age_stats_ms(&self) -> (u64, u64) {
        let now_ms = now_unix_millis();
        let mut ages: Vec<u64> = self
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .map(|op| now_ms.saturating_sub(op.created_unix_ms))
            .collect();
        if ages.is_empty() {
            return (0, 0);
        }
        ages.sort_unstable();
        // Safe percentile calculation: use checked arithmetic to avoid overflow
        let p95_idx = ((ages.len() - 1).saturating_mul(95)) / 100;
        let p95 = ages[p95_idx];
        let max = *ages.last().unwrap_or(&0);
        (p95, max)
    }

    fn upload_pipeline_is_idle(&self, pending_uploads: usize, ingress: &VfsIngressStats) -> bool {
        crate::upload::upload_log_is_idle(pending_uploads, self.upload_scheduler.in_flight())
            && ingress.dirty_mem_blocks == 0
            && ingress.dirty_mem_bytes == 0
            && ingress.spill_records == 0
            && ingress.spill_bytes == 0
            && ingress.checkpoint_pending_bytes == 0
            && ingress.checkpoint_pending_mutations == 0
    }

    fn upload_pipeline_summary_is_active(
        pending_uploads: usize,
        pending_metadata_ops: u32,
    ) -> bool {
        pending_uploads != 0 || (pending_metadata_ops != 0 && pending_metadata_ops >= 10)
    }

    fn format_upload_pipeline_pending_uploads(pending_upload_bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if pending_upload_bytes == 0 {
            "0".to_string()
        } else if pending_upload_bytes >= GB {
            format!("{:.1}GB", pending_upload_bytes as f64 / GB as f64)
        } else if pending_upload_bytes >= MB {
            format!("{}MB", pending_upload_bytes / MB)
        } else if pending_upload_bytes >= KB {
            format!("{}KB", pending_upload_bytes / KB)
        } else {
            format!("{pending_upload_bytes}B")
        }
    }

    fn upload_requeue_delay(&self, op: &PendingUpload) -> Duration {
        let exp = op.retry_count.min(8);
        let base = self
            .upload_requeue_base
            .saturating_mul(2u32.saturating_pow(exp));
        let capped = base.min(self.upload_requeue_max);
        let jitter_max = (capped.as_millis() as u64 / 5).max(1);
        let jitter_seed = op.object_id ^ now_unix_millis();
        let jitter_ms = jitter_seed % jitter_max;
        capped.saturating_add(Duration::from_millis(jitter_ms))
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
            .foreground_read_active(now_unix_millis(), self.upload_user_idle_grace);
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
        self.upload_scheduler
            .set_adaptive_penalty(Duration::from_millis(next_penalty_ms));
    }

    fn inode_has_unmaterialized_overlay_for_finalize(&self, ino: Ino) -> bool {
        if self
            .state
            .write_buffer
            .pending_blocks
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
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

    fn inode_has_pending_uploads(&self, ino: Ino) -> bool {
        self.block_cache.has_pending_uploads_for_ino(ino)
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

    fn refresh_inode_incarnation_state_from_attr(
        &self,
        ino: Ino,
        attr: &InodeAttr,
    ) -> InodeIncarnationState {
        let has_local_overlay = self.inode_has_local_overlay(ino);
        let has_pending_uploads = self.inode_has_pending_uploads(ino);

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
        matches!(error, VfsError::Meta(MetaError::FragmentedBlock { .. }))
    }

    fn needs_epoch_finalization(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state != MetaUploadState::Ready
            && (attr.content_epoch != attr.committed_content_epoch
                || attr.size != attr.committed_size)
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
        }
    }

    fn remove_pending_epoch_finalizations_for_inode(&self, ino: Ino) {
        self.state
            .upload
            .pending_epoch_finalizations
            .lock()
            .retain(|(candidate, _)| *candidate != ino);
    }

    fn cancel_pending_uploads_for_inode(&self, ino: Ino) {
        let mut quarantined = HashSet::new();
        quarantined.insert(ino);
        self.cancel_pending_uploads_for_inodes(&quarantined);
    }

    fn cancel_pending_uploads_for_inodes(&self, inos: &HashSet<Ino>) {
        if inos.is_empty() {
            return;
        }

        let pending_uploads: Vec<_> = self
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .filter(|op| inos.contains(&op.ino))
            .collect();
        if pending_uploads.is_empty() {
            return;
        }

        for op in &pending_uploads {
            self.block_cache
                .cancel_upload(op, CancelUploadMode::KeepBlock);
        }

        let mut pending_evictions = self.state.upload.pending_evictions.lock();
        for op in pending_uploads {
            pending_evictions.remove(&op.object_id);
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
                if let Some(block) = span.block {
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

    fn quarantine_invalid_regular_file_graph_for_startup_recovery(
        &self,
        ino: Ino,
        size: u64,
        error: &VfsError,
    ) -> HashSet<u64> {
        self.quarantine_invalid_regular_file_graph_impl(ino, size, error, false)
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

    fn remote_upload_placeholder_visible(&self, ino: Ino, attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state == MetaUploadState::Uploading
            && attr.size == 0
            && attr.committed_size == 0
            && !self.inode_has_local_overlay(ino)
            && !self.inode_has_pending_uploads(ino)
    }

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

    pub fn pending_uploads_for_inode(&self, ino: Ino) -> Vec<(u64, u64, u64)> {
        let mut out: Vec<_> = self
            .block_cache
            .snapshot_pending_uploads()
            .into_iter()
            .filter(|op| op.ino == ino)
            .map(|op| (op.offset, op.object_id, op.content_epoch))
            .collect();
        out.sort_by_key(|(offset, _, _)| *offset);
        out
    }

    pub fn read_cached_block(&self, object_id: u64) -> Option<Bytes> {
        let (cached, timing) = run_local_cache_io(|| self.local_block_backend.get(object_id));
        self.record_local_cache_io_timing(timing);
        cached
    }

    async fn block_store_get_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let block_store = self.local_block_backend.clone();
        let (result, timing) =
            run_local_cache_io_blocking(move || Ok(block_store.get(object_id))).await?;
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    async fn block_store_read_into_async(
        &self,
        object_id: u64,
        data_offset: usize,
        out: &mut [u8],
    ) -> VfsResult<Option<usize>> {
        let block_store = self.local_block_backend.clone();
        let out_len = out.len();
        let (result, timing) = run_local_cache_io_blocking(move || {
            if !block_store.contains(object_id) {
                return Ok((Vec::new(), None));
            }
            let mut scratch = vec![0_u8; out_len];
            let read = block_store.read_into(object_id, data_offset, &mut scratch);
            Ok((scratch, read))
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        let (scratch, read) = result;
        if let Some(read) = read {
            if read > out.len() {
                return Err(VfsError::Other(format!(
                    "local backend returned oversized read: object_id={object_id}, read={read}, out_len={}",
                    out.len()
                )));
            }
            if read > 0 {
                out[..read].copy_from_slice(&scratch[..read]);
            }
            Ok(Some(read))
        } else {
            Ok(None)
        }
    }

    async fn block_store_get_for_upload_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let block_store = self.local_block_backend.clone();
        let (result, timing) =
            run_local_cache_io_blocking(move || Ok(block_store.get_for_upload(object_id))).await?;
        self.record_local_cache_io_timing(timing);
        Ok(result)
    }

    async fn block_store_run_pressure_maintenance_async(&self) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store.run_pressure_maintenance().map_err(|error| {
                VfsError::Other(format!("block backend maintenance failed: {error}"))
            })
        })
        .await?;
        let _ = timing;
        Ok(())
    }

    async fn block_store_put_async(
        &self,
        object_id: u64,
        data: Bytes,
        evictable: bool,
    ) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store
                .put(object_id, data.as_ref(), evictable)
                .map_err(|error| VfsError::Other(format!("block store put failed: {error}")))
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn maybe_block_store_sync_data_async(
        &self,
        persisted_bytes: u64,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        {
            let mut local_sync_group = self.state.write_buffer.local_sync_group.lock().await;
            let pending_before = local_sync_group.pending_bytes;
            if pending_before == 0 && persisted_bytes > 0 {
                local_sync_group.last_sync_at = Instant::now();
            }
            local_sync_group.pending_bytes = local_sync_group
                .pending_bytes
                .saturating_add(persisted_bytes);

            let trigger = write_path_sync_trigger(
                self.cache_safety_mode,
                local_sync_policy,
                local_sync_group.pending_bytes,
                local_sync_group.last_sync_at.elapsed(),
                self.local_sync_group_min_bytes,
                self.local_sync_group_max_delay,
            );
            if trigger.is_none() {
                if local_sync_group.pending_bytes > 0 {
                    self.state
                        .metrics
                        .block_store_sync_data_deferred_calls
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
        }

        let sync_gate_wait_started = Instant::now();
        let _sync_gate = self.state.write_buffer.local_sync_gate.lock().await;
        self.record_sync_data_queue_wait(sync_gate_wait_started.elapsed());
        let bytes_to_sync = {
            let mut local_sync_group = self.state.write_buffer.local_sync_group.lock().await;
            let trigger = write_path_sync_trigger(
                self.cache_safety_mode,
                local_sync_policy,
                local_sync_group.pending_bytes,
                local_sync_group.last_sync_at.elapsed(),
                self.local_sync_group_min_bytes,
                self.local_sync_group_max_delay,
            );
            if trigger.is_none() {
                if local_sync_group.pending_bytes > 0 {
                    self.state
                        .metrics
                        .block_store_sync_data_deferred_calls
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

        match self.block_store_sync_data_async().await {
            Ok(()) => {
                let mut local_sync_group = self.state.write_buffer.local_sync_group.lock().await;
                local_sync_group.last_sync_at = Instant::now();
                drop(local_sync_group);
                self.state
                    .metrics
                    .block_store_sync_data_synced_bytes
                    .fetch_add(bytes_to_sync, Ordering::Relaxed);
                Ok(())
            }
            Err(error) => {
                let mut local_sync_group = self.state.write_buffer.local_sync_group.lock().await;
                local_sync_group.pending_bytes =
                    local_sync_group.pending_bytes.saturating_add(bytes_to_sync);
                Err(error)
            }
        }
    }

    async fn block_store_sync_data_async(&self) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let started = Instant::now();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store.sync_data().map_err(|error| {
                VfsError::Other(format!("block backend sync_data failed: {error}"))
            })
        })
        .await?;
        let _ = timing;
        let elapsed_ns = Self::duration_ns_u64(started.elapsed());
        let metrics = &self.state.metrics;
        Self::observe_latency_counter(
            &metrics.block_store_sync_data_calls,
            &metrics.block_store_sync_data_total_ns,
            &metrics.block_store_sync_data_max_ns,
            &metrics.block_store_sync_data_histogram,
            elapsed_ns,
        );
        self.record_sync_data_interval();
        Ok(())
    }

    async fn block_store_mark_uploaded_async(&self, object_id: u64) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store.mark_uploaded(object_id);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn block_store_remove_async(&self, object_id: u64) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store.remove(object_id);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    async fn block_store_remove_batch_async(&self, object_ids: Vec<u64>) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        let ((), timing) = run_local_cache_io_blocking(move || {
            block_store.remove_batch(&object_ids);
            Ok(())
        })
        .await?;
        self.record_local_cache_io_timing(timing);
        Ok(())
    }

    pub fn upload_stats(&self) -> VfsUploadStats {
        VfsUploadStats {
            pending_uploads: self.block_cache.pending_upload_count(),
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
        }
    }

    pub fn file_session_stats(&self) -> VfsFileSessionStats {
        self.state.file_session.stats()
    }

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
    fn sync_meta(&self) -> Arc<dyn MetaEngine> {
        self.meta.sync_view()
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> Arc<dyn MetaEngine> {
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

    /// Shared reference to the block cache.
    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.block_cache
    }

    /// Shared reference to the local block backend contract.
    pub fn local_block_backend(&self) -> &(dyn LocalBlockBackend + 'static) {
        self.local_block_backend.as_ref()
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
        self.block_cache.wait_for_uploads_idle(timeout).await
    }

    /// Snapshot accepted local-ingress state for diagnostics and backpressure
    /// observability.
    pub fn ingress_stats(&self) -> VfsIngressStats {
        let pending = self.state.write_buffer.pending_blocks.lock();
        let dirty_mem_blocks = pending.len();
        let dirty_mem_bytes = self.total_pending_write_buffer_bytes();
        drop(pending);

        let last_checkpoint_unix_ms = self
            .state
            .ingress
            .ingress_last_checkpoint_unix_ms
            .load(Ordering::Relaxed);
        let last_checkpoint_age_ms = if last_checkpoint_unix_ms == 0 {
            0
        } else {
            now_unix_millis().saturating_sub(last_checkpoint_unix_ms)
        };

        VfsIngressStats {
            dirty_mem_bytes,
            dirty_mem_blocks,
            spill_bytes: 0,
            spill_records: 0,
            oldest_spill_age_ms: 0,
            write_backpressure_wait_ms: self
                .state
                .write_buffer
                .write_backpressure_wait_ms
                .load(Ordering::Relaxed),
            write_backpressure_events: self
                .state
                .write_buffer
                .write_backpressure_events
                .load(Ordering::Relaxed),
            fast_backpressure_wait_ms: self
                .state
                .write_buffer
                .fast_backpressure_wait_ms
                .load(Ordering::Relaxed),
            fast_backpressure_events: self
                .state
                .write_buffer
                .fast_backpressure_events
                .load(Ordering::Relaxed),
            fast_backpressure_stall_max_ms: self
                .state
                .write_buffer
                .fast_backpressure_stall_max_ms
                .load(Ordering::Relaxed),
            forced_spill_blocks: 0,
            drained_mem_blocks: 0,
            drained_mem_bytes: 0,
            drained_spill_blocks: 0,
            drained_spill_bytes: 0,
            prefetch_queue_depth: self
                .state
                .metrics
                .prefetch_queue_depth
                .load(Ordering::Relaxed),
            checkpoint_persists: self
                .state
                .ingress
                .ingress_checkpoint_persists
                .load(Ordering::Relaxed),
            checkpoint_failures: self
                .state
                .ingress
                .ingress_checkpoint_failures
                .load(Ordering::Relaxed),
            last_checkpoint_age_ms,
            last_checkpoint_segment_id: self
                .state
                .ingress
                .ingress_last_checkpoint_segment_id
                .load(Ordering::Relaxed),
            last_checkpoint_offset: self
                .state
                .ingress
                .ingress_last_checkpoint_offset
                .load(Ordering::Relaxed),
            checkpoint_pending_bytes: 0,
            checkpoint_pending_mutations: 0,
            checkpoint_active_segment_lag: 0,
            checkpoint_live_segments: 0,
        }
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
            &metrics.block_store_sync_data_queue_wait_calls,
            &metrics.block_store_sync_data_queue_wait_total_ns,
            &metrics.block_store_sync_data_queue_wait_max_ns,
            &metrics.block_store_sync_data_queue_wait_histogram,
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
            &metrics.block_store_sync_data_last_completed_ns,
            &metrics.block_store_sync_data_interval_total_ns,
            &metrics.block_store_sync_data_interval_max_ns,
            &metrics.block_store_sync_data_interval_histogram,
        );
    }

    pub(super) fn note_eager_flush_outcome(&self, outcome: EagerFlushOutcome) {
        let metrics = &self.state.metrics;
        metrics.eager_flush_checks.fetch_add(1, Ordering::Relaxed);
        match outcome {
            EagerFlushOutcome::Triggered => {
                metrics
                    .eager_flush_triggered
                    .fetch_add(1, Ordering::Relaxed);
            }
            EagerFlushOutcome::SkippedEmpty => {
                metrics
                    .eager_flush_skipped_empty
                    .fetch_add(1, Ordering::Relaxed);
            }
            EagerFlushOutcome::SkippedBelowThreshold => {
                metrics
                    .eager_flush_skipped_below_threshold
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn record_payload_persist_observability(
        &self,
        burst_blocks: usize,
        burst_bytes: u64,
        elapsed: Duration,
        completed_burst: bool,
    ) {
        let elapsed_ns = Self::duration_ns_u64(elapsed);
        let burst_blocks_u64 = u64::try_from(burst_blocks).unwrap_or(u64::MAX);
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
            .payload_burst_total_blocks
            .fetch_add(burst_blocks_u64, Ordering::Relaxed);
        metrics
            .payload_burst_total_bytes
            .fetch_add(burst_bytes, Ordering::Relaxed);
        metrics
            .payload_burst_max_blocks
            .fetch_max(burst_blocks_u64, Ordering::Relaxed);
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

    pub fn note_winfsp_write_callback(&self) {
        self.state
            .metrics
            .winfsp_write_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_overwrite_callback(&self) {
        self.state
            .metrics
            .winfsp_overwrite_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_cleanup_callback(&self) {
        self.state
            .metrics
            .winfsp_cleanup_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_flush_callback(&self) {
        self.state
            .metrics
            .winfsp_flush_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_close_callback(&self) {
        self.state
            .metrics
            .winfsp_close_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_cleanup_flush_fh_callback(&self) {
        self.state
            .metrics
            .winfsp_cleanup_flush_fh_callbacks
            .fetch_add(1, Ordering::Relaxed);
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

    pub fn note_winfsp_cleanup_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Cleanup, flushed);
    }

    pub fn note_winfsp_flush_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Flush, flushed);
    }

    pub fn note_winfsp_close_barrier_outcome(&self, flushed: bool) {
        self.note_winfsp_barrier_outcome(WinfspBarrierType::Close, flushed);
    }

    /// Returns true when the inode still has local dirty state that requires a
    /// flush barrier before close/flush callback completion.
    pub fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.has_pending_write_buffer_blocks_for_inode(ino)
            || self.state.namespace.dirty_attrs.lock().contains_key(&ino)
    }

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
            flush_ordered_calls: flush_ordered_calls,
            local_cache_io_calls: local_cache_io_calls,
            local_cache_handoff_total_ns: local_cache_io_handoff_total_ns,
            local_cache_handoff_max_ns: local_cache_io_handoff_max_ns,
            local_cache_backend_total_ns: local_cache_io_backend_total_ns,
            local_cache_backend_max_ns: local_cache_io_backend_max_ns,
            sync_calls: block_store_sync_data_calls,
            sync_deferred_calls: block_store_sync_data_deferred_calls,
            sync_bytes: block_store_sync_data_synced_bytes,
            sync_total_ns: block_store_sync_data_total_ns,
            sync_max_ns: block_store_sync_data_max_ns,
            sync_queue_wait_calls: block_store_sync_data_queue_wait_calls,
            sync_queue_wait_total_ns: block_store_sync_data_queue_wait_total_ns,
            sync_queue_wait_max_ns: block_store_sync_data_queue_wait_max_ns,
            sync_interval_total_ns: block_store_sync_data_interval_total_ns,
            sync_interval_max_ns: block_store_sync_data_interval_max_ns,
            payload_persist_calls: payload_persist_calls,
            payload_persist_total_ns: payload_persist_total_ns,
            payload_persist_max_ns: payload_persist_max_ns,
            payload_burst_count: payload_burst_count,
            payload_burst_total_blocks: payload_burst_total_blocks,
            payload_burst_total_bytes: payload_burst_total_bytes,
            payload_burst_max_blocks: payload_burst_max_blocks,
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
            .block_store_sync_data_histogram
            .snapshot();
        let sync_queue_wait_histogram = self
            .state
            .metrics
            .block_store_sync_data_queue_wait_histogram
            .snapshot();
        let sync_interval_histogram = self
            .state
            .metrics
            .block_store_sync_data_interval_histogram
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

        VfsWritePathObservabilitySnapshot {
            generated_unix_ms: now_unix_millis(),
            uptime_secs: elapsed.as_secs_f64(),
            backend_tag: self.write_path_backend_tag.clone(),
            flush_ordered_calls,
            flush_ordered_calls_per_sec: Self::per_second(flush_ordered_calls, elapsed),
            block_store_sync_data: VfsWritePathSyncDataStats {
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
                    .write_buffer
                    .write_backpressure_events
                    .load(Ordering::Relaxed),
                wait_total_ms: self
                    .state
                    .write_buffer
                    .write_backpressure_wait_ms
                    .load(Ordering::Relaxed),
                stall_max_ms: self
                    .state
                    .write_buffer
                    .fast_backpressure_stall_max_ms
                    .load(Ordering::Relaxed),
            },
            payload_burst: VfsWritePathBurstStats {
                bursts: payload_burst_count,
                bursts_per_sec: Self::per_second(payload_burst_count, elapsed),
                total_blocks: payload_burst_total_blocks,
                total_bytes: payload_burst_total_bytes,
                avg_blocks_per_burst: if payload_burst_count == 0 {
                    0.0
                } else {
                    payload_burst_total_blocks as f64 / payload_burst_count as f64
                },
                avg_bytes_per_burst: if payload_burst_count == 0 {
                    0.0
                } else {
                    payload_burst_total_bytes as f64 / payload_burst_count as f64
                },
                max_blocks_per_burst: payload_burst_max_blocks,
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
                sync_group_max_delay_ms: self.local_sync_group_max_delay.as_millis() as u64,
            },
        }
    }

    /// Describe where a new locally materialized payload would be accepted
    /// right now: RAM, spill, or local backpressure.
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
            spill_bytes: 0,
            spill_limit: 0,
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
        let required_access =
            if trunc || flags.contains(OpenFlags::WRONLY) || flags.contains(OpenFlags::RDWR) {
                AccessLevel::ReadWrite
            } else {
                AccessLevel::ReadOnly
            };
        self.enforce_access(ino, required_access)?;
        if self.ignored_entry_attr(ino).is_some() {
            let open_file = self.handle_table.open(ino, flags);
            let fh = open_file.fh;
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
            return Err(VfsError::RemoteContentPending);
        }

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let open_file = self.handle_table.open(ino, flags);
        let fh = open_file.fh;
        self.state.file_session.note_open_handle(ino);
        self.state
            .read
            .increment_active_access_class(open_file.current_access_class());
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());

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
    // Extended Attributes (xattr)
    // =========================================================================

    /// Get an extended attribute value.
    pub fn get_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<Option<Vec<u8>>> {
        if let Err(err) = self.enforce_access(ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                return Ok(None);
            }
            return Err(err);
        }
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(None);
        }
        Ok(self.sync_meta().get_xattr(ino, name)?)
    }

    /// Set an extended attribute.
    pub fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> VfsResult<()> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }
        if value.len() > self.max_xattr_bytes {
            return Err(VfsError::Meta(MetaError::XattrValueTooLarge {
                actual: value.len(),
                limit: self.max_xattr_bytes,
            }));
        }
        self.sync_meta().set_xattr(ino, name, value, flags)?;
        Ok(())
    }

    /// Remove an extended attribute.
    pub fn remove_xattr(&self, ino: Ino, name: &[u8]) -> VfsResult<()> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }
        self.sync_meta().remove_xattr(ino, name)?;
        Ok(())
    }

    /// List all extended attribute names for an inode.
    pub fn list_xattrs(&self, ino: Ino) -> VfsResult<Vec<Vec<u8>>> {
        if let Err(err) = self.enforce_access(ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                return Ok(Vec::new());
            }
            return Err(err);
        }
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(Vec::new());
        }
        Ok(self.sync_meta().list_xattrs(ino)?)
    }

    fn run_sync_future<T>(future: impl std::future::Future<Output = T>) -> T {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(future))
        } else {
            tokio::runtime::Runtime::new()
                .expect("failed to create tokio runtime for synchronous VFS operation")
                .block_on(future)
        }
    }

    fn directory_has_probable_trash_ancestor(&self, dir_ino: Ino) -> VfsResult<bool> {
        let trash_roots = self.root_trash_dir_inodes()?;
        if trash_roots.is_empty() {
            return Ok(false);
        }
        let mut current = dir_ino;
        let mut hops = 0usize;
        while current != 0 && hops < 1024 {
            if trash_roots.contains(&current) {
                return Ok(true);
            }
            let attr = self.sync_meta().get_attr(current)?;
            let parent = attr.parent;
            if parent == 0 || parent == current {
                break;
            }
            if trash_roots.contains(&parent) {
                return Ok(true);
            }
            current = parent;
            hops += 1;
        }
        Ok(false)
    }

    fn root_trash_dir_inodes(&self) -> VfsResult<HashSet<Ino>> {
        let mut trash_roots = HashSet::new();
        for entry in self
            .sync_meta()
            .readdir(ROOT_INO)?
            .into_iter()
            .filter(|entry| entry.kind == FileType::Directory)
        {
            if is_probable_trash_component(&entry.name) {
                trash_roots.insert(entry.ino);
            }
        }
        Ok(trash_roots)
    }

    /// Immediately evict a single block from every cache tier.
    ///
    /// Called by `truncate_blocks()` for each block that is removed by a
    /// truncation.
    fn evict_block_from_caches(&self, ino: Ino, block_index: u32, object_id: u64) {
        self.clear_pending_block(ino, block_index);
        self.remove_live_extent(ino, block_index);

        // Committed cache + L1/L2 upload cache (defer if pinned for pending upload).
        if self.block_cache.is_pinned(object_id) {
            self.state.upload.pending_evictions.lock().insert(object_id);
            debug!(
                "evict_block: deferred pinned object_id={} ino={} block={}",
                object_id, ino, block_index
            );
        } else {
            self.block_cache.remove_sync(object_id);
            self.local_block_backend.remove(object_id);
        }
    }

    // =========================================================================
    // Background Tasks
    // =========================================================================

    /// Process a single upload operation.
    ///
    /// The cache stores raw block payloads; we package them into the STFS object
    /// format (header + payload) before uploading to S3. The upload itself is
    /// handled by `UploadScheduler` (AIMD concurrency + timeout/retry policy).
    ///
    /// If the owning inode has been deleted, the upload is cancelled immediately
    /// (no S3 write) and the block is evicted from cache.
    async fn process_upload(&self, op: PendingUpload) -> VfsResult<()> {
        struct UploadTokenCleanup<'a> {
            vfs: &'a StudioVfs,
            ino: Ino,
        }
        impl Drop for UploadTokenCleanup<'_> {
            fn drop(&mut self) {
                self.vfs.maybe_release_upload_cancel_token(self.ino);
            }
        }

        let _token_cleanup = UploadTokenCleanup {
            vfs: self,
            ino: op.ino,
        };
        let block_index = block_index_from_offset(op.offset, self.block_size);
        self.state.file_session.note_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "start",
        );
        let upload_cancel_token = self.uploader_cancel_token_for_inode(op.ino);
        debug!(
            ino = op.ino,
            object_id = op.object_id,
            offset = op.offset,
            content_epoch = op.content_epoch,
            retry_count = op.retry_count,
            "process_upload start"
        );
        if self.uploader_shutdown_requested() {
            return Ok(());
        }
        let mut waited_upload_delay_ms = 0u64;
        if self.upload_delay > Duration::from_millis(0) {
            let age_ms = now_unix_millis().saturating_sub(op.created_unix_ms);
            let delay_ms = self.upload_delay.as_millis() as u64;
            if age_ms < delay_ms {
                waited_upload_delay_ms = delay_ms - age_ms;
                tokio::select! {
                    _ = upload_cancel_token.cancelled() => {
                        if self.cancel_upload_if_inode_deleted(&op).await? {
                            debug!(ino = op.ino, object_id = op.object_id, "process_upload cancelled during upload delay");
                        }
                        return Ok(());
                    }
                    _ = tokio::time::sleep(Duration::from_millis(waited_upload_delay_ms)) => {}
                }
            }
        }
        if self.uploader_shutdown_requested() {
            return Ok(());
        }

        // Fast-path first: if inode is already deleted, avoid any penalty
        // computation/sleep and cancel upload immediately.
        if self.cancel_upload_if_inode_deleted(&op).await? {
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "process_upload canceled: inode deleted (short-circuit)"
            );
            return Ok(());
        }

        // Apply adaptive cool-down before the worker enters the local L2 read
        // phase so background uploads create real quiet windows for foreground
        // reads instead of a back-to-back disk conveyor.
        let adaptive_penalty_ms = self.upload_scheduler.adaptive_penalty_ms();
        let effective_adaptive_penalty_ms =
            adaptive_penalty_ms.saturating_sub(waited_upload_delay_ms);
        if effective_adaptive_penalty_ms > 0 {
            tokio::select! {
                _ = self.state.shutdown.uploader_shutdown_token.cancelled() => return Ok(()),
                _ = upload_cancel_token.cancelled() => {
                    if self.cancel_upload_if_inode_deleted(&op).await? {
                        debug!(
                            ino = op.ino,
                            object_id = op.object_id,
                            penalty_ms = effective_adaptive_penalty_ms,
                            raw_penalty_ms = adaptive_penalty_ms,
                            upload_delay_waited_ms = waited_upload_delay_ms,
                            "process_upload cancelled during adaptive pre-read cooldown"
                        );
                    } else {
                        debug!(
                            ino = op.ino,
                            object_id = op.object_id,
                            penalty_ms = effective_adaptive_penalty_ms,
                            raw_penalty_ms = adaptive_penalty_ms,
                            upload_delay_waited_ms = waited_upload_delay_ms,
                            "process_upload cancelled during adaptive pre-read cooldown (shutdown/token cancel)"
                        );
                    }
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_millis(effective_adaptive_penalty_ms)) => {}
            }
        }

        // Also check metadata: after a remount, `deleted_inodes` is empty but
        // the inode may have been deleted in a prior session.  If the inode no
        // longer exists in the metadata store, the upload is stale.
        let meta = self.meta_async.clone();
        let attr = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(op.ino)
        })
        .await
        .ok();
        if attr.is_none() {
            self.state
                .upload
                .upload_canceled_deleted
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                "upload cancelled: inode deleted or missing"
            );
            self.cancel_upload_for_deleted_inode(&op).await?;
            self.maybe_clear_deleted_inode_marker(op.ino);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                "process_upload canceled: inode deleted or missing"
            );
            return Ok(());
        }

        // Coalescing: if this file block has already been replaced in metadata
        // with a newer object_id, skip uploading this stale one.
        if let Some(attr) = attr.as_ref()
            && attr.content_epoch != op.content_epoch
        {
            self.state
                .upload
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                ino = op.ino,
                block_index,
                queued_epoch = op.content_epoch,
                current_epoch = attr.content_epoch,
                "upload cancelled: stale content epoch"
            );
            self.block_cache
                .cancel_upload(&op, CancelUploadMode::KeepBlock);
            self.block_store_remove_async(op.object_id).await?;
            self.state
                .upload
                .pending_evictions
                .lock()
                .remove(&op.object_id);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                queued_epoch = op.content_epoch,
                current_epoch = attr.content_epoch,
                "process_upload canceled: stale content epoch"
            );
            return Ok(());
        }
        if let Ok(Some(current)) = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await
            && current.object_id != op.object_id
        {
            self.state
                .upload
                .upload_canceled_stale
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                object_id = op.object_id,
                current_object_id = current.object_id,
                ino = op.ino,
                block_index,
                "upload cancelled: stale block superseded by newer object"
            );
            self.block_cache
                .cancel_upload(&op, CancelUploadMode::KeepBlock);
            self.block_store_remove_async(op.object_id).await?;
            self.state
                .upload
                .pending_evictions
                .lock()
                .remove(&op.object_id);
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                current_object_id = current.object_id,
                "process_upload canceled: stale superseded block"
            );
            return Ok(());
        }

        let t0 = Instant::now();
        self.local_block_backend.mark_uploading(op.object_id);
        let fragment_keys = format_object_fragment_keys(&self.data_prefix, op.object_id);
        let mut remote_object_visible = false;
        if op.retry_count > 0 {
            let checks = fragment_keys.iter().map(|key| {
                let cancel_token = upload_cancel_token.clone();
                async move {
                    let result = run_with_timeout_or_cancel(
                        &cancel_token,
                        Duration::from_secs(5),
                        self.fetch_scheduler.data_store().exists(key),
                    )
                    .await;
                    (key, result)
                }
            });
            let results = futures::future::join_all(checks).await;
            let mut all_visible = true;
            for (key, result) in results {
                match result {
                    TimeoutOrCancel::Completed(Ok(true)) => {}
                    TimeoutOrCancel::Completed(Ok(false)) => {
                        all_visible = false;
                        break;
                    }
                    TimeoutOrCancel::Completed(Err(err)) => {
                        all_visible = false;
                        debug!(
                            object_id = op.object_id,
                            key,
                            error = %err,
                            retry_count = op.retry_count,
                            "upload reconcile: remote exists() check failed, falling back to upload"
                        );
                        break;
                    }
                    TimeoutOrCancel::TimedOut => {
                        all_visible = false;
                        debug!(
                            object_id = op.object_id,
                            key,
                            retry_count = op.retry_count,
                            "upload reconcile: remote exists() check timed out, falling back to upload"
                        );
                        break;
                    }
                    // Shutdown cancellation: keep DIRTY upload intent for
                    // next startup replay by exiting before completion.
                    TimeoutOrCancel::Cancelled => {
                        self.cancel_upload_if_inode_deleted(&op).await?;
                        return Ok(());
                    }
                }
            }
            if all_visible {
                remote_object_visible = true;
                debug!(
                    object_id = op.object_id,
                    ino = op.ino,
                    block_index,
                    retry_count = op.retry_count,
                    "upload reconcile: remote object already visible, skipping re-upload"
                );
            }
        }

        // Read raw block payload from the persisted local backend when we
        // really need to upload. Queue correctness is anchored on this reread
        // path rather than transient in-memory staging.
        // Note: we intentionally do not probe L1/staged payloads first here.
        // A single persisted-source path keeps restart replay and live uploads
        // behaviorally identical, trading some extra local I/O for simpler
        // correctness reasoning.
        let raw_payload: Option<Bytes> = if remote_object_visible {
            None
        } else {
            // Hold local upload I/O slots strictly around the backend reread
            // phase. This path must continue making progress during foreground
            // activity because startup replay reconstructs pending uploads from
            // metadata + persisted local blocks.
            let upload_io_cost = if self
                .state
                .read
                .foreground_read_active(now_unix_millis(), self.upload_user_idle_grace)
            {
                self.state
                    .upload
                    .upload_cache_io_foreground_cost
                    .load(Ordering::Relaxed)
                    .max(1)
            } else {
                1
            };
            let wait_started = Instant::now();
            let upload_io_permit = self
                .upload_cache_io_slots
                .clone()
                .acquire_many_owned(upload_io_cost)
                .await
                .map_err(|e| VfsError::QosFailure {
                    stage: Cow::Borrowed("upload_cache_io_slots"),
                    object_id: op.object_id,
                    message: e.to_string(),
                })?;
            let wait_ns = wait_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
            let read_started = Instant::now();
            let read_result = self.block_store_get_for_upload_async(op.object_id).await?;
            let read_ns = read_started.elapsed().as_nanos().clamp(0, u64::MAX as u128) as u64;
            drop(upload_io_permit);
            self.state
                .upload
                .upload_fallback_wait_ns
                .fetch_add(wait_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_wait_max_ns
                .fetch_max(wait_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_read_ns
                .fetch_add(read_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_read_max_ns
                .fetch_max(read_ns, Ordering::Relaxed);
            self.state
                .upload
                .upload_fallback_rereads
                .fetch_add(1, Ordering::Relaxed);
            if let Some(payload) = &read_result {
                self.state
                    .upload
                    .upload_fallback_reread_bytes
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
            } else {
                self.state
                    .upload
                    .upload_fallback_reread_cache_misses
                    .fetch_add(1, Ordering::Relaxed);
            }
            read_result
        };

        let raw_payload = match raw_payload {
            Some(data) => data,
            None if remote_object_visible => Bytes::new(),
            None => {
                // Block data is missing from the persisted local backend. If
                // remote already has this object, we can still continue with
                // commit reconcile.
                if op.retry_count > 0 {
                    let checks = fragment_keys.iter().map(|key| {
                        let cancel_token = upload_cancel_token.clone();
                        async move {
                            let result = run_with_timeout_or_cancel(
                                &cancel_token,
                                Duration::from_secs(5),
                                self.fetch_scheduler.data_store().exists(key),
                            )
                            .await;
                            (key, result)
                        }
                    });
                    let results = futures::future::join_all(checks).await;
                    let mut all_visible = true;
                    for (_key, result) in results {
                        match result {
                            TimeoutOrCancel::Completed(Ok(true)) => {}
                            TimeoutOrCancel::Completed(Ok(false))
                            | TimeoutOrCancel::Completed(Err(_))
                            | TimeoutOrCancel::TimedOut => {
                                all_visible = false;
                                break;
                            }
                            // Shutdown cancellation: exit quietly so worker
                            // loop does not requeue/log as transient failure.
                            TimeoutOrCancel::Cancelled => {
                                self.cancel_upload_if_inode_deleted(&op).await?;
                                return Ok(());
                            }
                        }
                    }
                    if all_visible {
                        debug!(
                            object_id = op.object_id,
                            ino = op.ino,
                            block_index,
                            retry_count = op.retry_count,
                            "upload reconcile: local backend missing block but remote object exists"
                        );
                        remote_object_visible = true;
                        Bytes::new()
                    } else {
                        // If the inode was deleted between our checks
                        // (race), cancel cleanly. Otherwise this remains a
                        // genuine error and will be retried.
                        if self.state.upload.deleted_inodes.lock().contains(&op.ino) {
                            self.block_cache
                                .cancel_upload(&op, CancelUploadMode::KeepBlock);
                            self.block_store_remove_async(op.object_id).await?;
                            self.state
                                .upload
                                .pending_evictions
                                .lock()
                                .remove(&op.object_id);
                            return Ok(());
                        }
                        return Err(VfsError::Other(format!(
                            "object {} missing from local block backend",
                            op.object_id
                        )));
                    }
                } else {
                    // First attempt without a local persisted block is
                    // unexpected; retry through normal error path.
                    if self.state.upload.deleted_inodes.lock().contains(&op.ino) {
                        self.block_cache
                            .cancel_upload(&op, CancelUploadMode::KeepBlock);
                        self.block_store_remove_async(op.object_id).await?;
                        self.state
                            .upload
                            .pending_evictions
                            .lock()
                            .remove(&op.object_id);
                        return Ok(());
                    }
                    return Err(VfsError::Other(format!(
                        "object {} missing from local block backend",
                        op.object_id
                    )));
                }
            }
        };

        let mut transit_credit = if remote_object_visible {
            None
        } else {
            Some(
                self.transit_memory_budget
                    .acquire_background(raw_payload.len().max(1))
                    .await,
            )
        };

        // Re-check visibility because the reconcile branch above may flip
        // `remote_object_visible = true` when cache is missing but remote
        // already has this object.
        let object_bytes: Option<Vec<Buffer>> = if remote_object_visible {
            None
        } else {
            let mut fragments = Vec::with_capacity(CLOUD_FRAGMENT_COUNT);
            for fragment_index in 0..CLOUD_FRAGMENT_COUNT {
                let start = fragment_index * CLOUD_FRAGMENT_SIZE;
                let end = (start + CLOUD_FRAGMENT_SIZE).min(raw_payload.len());
                let fragment_payload = if start < raw_payload.len() {
                    raw_payload.slice(start..end)
                } else {
                    Bytes::new()
                };
                fragments.push(create_object_buffer(fragment_payload)?);
            }
            Some(fragments)
        };

        if object_bytes.is_none() {
            let _ = transit_credit.take();
        }

        if let Some(object_bytes) = object_bytes {
            let _upload_qos =
                self.qos
                    .acquire_upload()
                    .await
                    .map_err(|e| VfsError::QosFailure {
                        stage: Cow::Borrowed("upload_qos"),
                        object_id: op.object_id,
                        message: e.to_string(),
                    })?;
            let uploads = fragment_keys
                .iter()
                .zip(object_bytes.into_iter())
                .enumerate()
                .map(|(fragment_index, (key, payload))| {
                    let cancel_token = upload_cancel_token.clone();
                    async move {
                        self.upload_scheduler
                            .put(key, payload, &cancel_token)
                            .await
                            .map(|_| (fragment_index, key.clone()))
                            .map_err(|e| (fragment_index, key.clone(), e))
                    }
                });
            let done = join_all(uploads).await;
            let mut first_error: Option<(usize, String, crate::upload::UploadError)> = None;
            let mut saw_cancel = false;
            for result in done {
                match result {
                    Ok((fragment_index, key)) => {
                        debug!(
                            ino = op.ino,
                            object_id = op.object_id,
                            block_index,
                            fragment_index,
                            key,
                            "process_upload fragment put completed"
                        );
                    }
                    Err((_, _, crate::upload::UploadError::Cancelled)) => {
                        saw_cancel = true;
                    }
                    Err((fragment_index, key, e)) => {
                        if first_error.is_none() {
                            first_error = Some((fragment_index, key, e));
                        }
                    }
                }
            }
            // Treat cancellation as an intentional shutdown path:
            // do not mark sticky errors and do not trigger in-memory requeue.
            // Because `complete_upload` is not called, DIRTY state remains in
            // local backend for safe startup replay on next mount.
            if saw_cancel {
                self.cancel_upload_if_inode_deleted(&op).await?;
                return Ok(());
            }
            if let Some((fragment_index, key, e)) = first_error {
                return Err(VfsError::Other(format!(
                    "fragment upload failed (idx={fragment_index}, key={key}): {e}"
                )));
            }
            debug!(
                ino = op.ino,
                object_id = op.object_id,
                block_index,
                fragments = CLOUD_FRAGMENT_COUNT,
                "process_upload put completed"
            );
        }

        let current = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(op.ino, block_index)
        })
        .await?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        match current {
            Some(ref block_ref)
                if block_ref.object_id == op.object_id
                    && run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                        engine
                            .get_attr(op.ino)
                            .map(|attr| attr.content_epoch == op.content_epoch)
                    })
                    .await
                    .unwrap_or(false) =>
            {
                self.block_cache.complete_upload(&op);
                debug!(
                    ino = op.ino,
                    object_id = op.object_id,
                    block_index,
                    "process_upload complete_upload on current block"
                );
            }
            _ => {
                run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                    engine.add_pending_deletion(op.object_id, timestamp)
                })
                .await?;
                debug!(
                    object_id = op.object_id,
                    ino = op.ino,
                    block_index,
                    "upload completed for stale block, marked for deletion"
                );
                self.block_cache.complete_upload(&op);
                debug!(
                    ino = op.ino,
                    object_id = op.object_id,
                    block_index,
                    "process_upload complete_upload on stale block"
                );
            }
        }

        if let Ok(attr) = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(op.ino)
        })
        .await
        {
            self.refresh_inode_incarnation_state_from_attr(op.ino, &attr);
        } else {
            self.clear_inode_incarnation(op.ino);
        }
        self.clear_upload_sticky_error(op.ino);

        // Post-upload cleanup: if the block was marked for deferred eviction
        // (inode was deleted while the upload was in flight), remove it now.
        if self
            .state
            .upload
            .pending_evictions
            .lock()
            .remove(&op.object_id)
        {
            self.block_cache
                .remove_with_priority(op.object_id, L2IoPriority::Background)
                .await;
            self.block_store_remove_async(op.object_id).await?;
            debug!(
                object_id = op.object_id,
                "post-upload eviction: block removed (inode deleted)"
            );
        } else if let Ok(Some(current)) =
            run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                engine.get_file_block(op.ino, block_index)
            })
            .await
            && current.object_id != op.object_id
        {
            self.block_cache
                .remove_with_priority(op.object_id, L2IoPriority::Background)
                .await;
            self.block_store_remove_async(op.object_id).await?;
        } else {
            self.block_store_mark_uploaded_async(op.object_id).await?;
        }

        debug!(
            object_id = op.object_id,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "background upload complete"
        );
        self.state.file_session.note_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "complete",
        );
        Ok(())
    }

    /// Persist the cache checkpoint to disk.
    ///
    /// Called at shutdown so the LRU state survives across remounts.
    pub async fn persist_cache(&self) {
        self.block_cache.shutdown().await;
        if let Err(error) = self.local_block_backend.sync_data() {
            warn!(error = %error, "local cache backend sync failed during shutdown");
        } else {
            tracing::info!("local cache backend sync persisted");
        }
        tracing::info!("block cache checkpoint persisted");
    }

    /// Best-effort graceful shutdown: drain dirty buffers, persist final
    /// checkpoint state, cancel uploader tasks, then persist cache checkpoint.
    pub async fn shutdown(&self, upload_drain_timeout: Duration) {
        if self
            .state
            .shutdown
            .shutdown_completed
            .load(Ordering::Acquire)
        {
            return;
        }
        if self
            .state
            .shutdown
            .shutdown_requested
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let t0 = Instant::now();
        let background_join_timeout = Duration::from_secs(2);

        // Phase 0: quiesce non-uploader background work so shutdown can finish
        // the remaining accepted ingress without concurrent producers racing it.
        self.state
            .shutdown
            .slab_flusher_stopped
            .store(true, Ordering::Release);
        self.state
            .ingress
            .ingress_checkpoint_stopped
            .store(true, Ordering::Release);
        self.state.shutdown.shutdown_notify.notify_waiters();
        self.state
            .ingress
            .ingress_checkpoint_notify
            .notify_waiters();
        self.join_background_tasks(background_join_timeout).await;

        // Reset the active-access-class telemetry counters for any handles that
        // were never explicitly closed via release_fh (e.g. force-unmount).
        // The shutdown flag is already set, so no new open_file() calls can
        // increment the counters after this point.
        for class in self.handle_table.collect_open_access_classes() {
            self.state.read.decrement_active_access_class(class);
        }

        // Phase 1: flush pending WriteBuffer state inline into the local backend +
        // metadata so local durability is established before shutdown.
        let dirty_before = self.total_pending_write_buffer_bytes();
        info!(
            dirty_bytes = dirty_before,
            pending_uploads = self.block_cache.pending_upload_count(),
            "VFS shutdown: flushing pending write buffer"
        );
        self.flush_pending_blocks_for_shutdown().await;
        info!(
            pending_uploads = self.block_cache.pending_upload_count(),
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: write buffer drain complete"
        );

        if let Err(e) = reset_ingress_artifacts_for_fast_mode(self.ingress_wal.spill_dir()) {
            warn!("VFS shutdown: failed to remove ingress artifacts: {}", e);
        }

        // Phase 2: attempt graceful upload drain (bounded by user timeout),
        // then cancel uploader work so network-blocked requests are aborted.
        let pending = self.block_cache.pending_upload_count();
        let mut drained = pending == 0;
        if pending > 0 {
            if upload_drain_timeout.is_zero() {
                info!(
                    pending,
                    "VFS shutdown: skipping upload drain and cancelling uploader immediately"
                );
            } else {
                info!(
                    pending,
                    timeout_ms = upload_drain_timeout.as_millis() as u64,
                    "VFS shutdown: waiting for uploads to drain before forced cancellation"
                );
                drained = self
                    .block_cache
                    .wait_for_uploads_idle(upload_drain_timeout)
                    .await;
            }
        }

        self.request_uploader_shutdown();

        if !drained {
            let pending_ops = self.block_cache.snapshot_pending_uploads();
            let sample_object_ids: Vec<u64> =
                pending_ops.iter().take(8).map(|op| op.object_id).collect();
            info!(
                pending = self.block_cache.pending_upload_count(),
                pending_queue_ops = pending_ops.len(),
                pending_object_ids_sample = ?sample_object_ids,
                "VFS shutdown: uploader cancelled; remaining dirty uploads will replay on next startup"
            );
        }
        self.join_uploader_task(background_join_timeout).await;

        // Phase 4: persist metadata + cache checkpoint.
        info!(
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: persisting metadata"
        );
        let meta = self.sync_meta();
        let metadata_flush_timeout = self.shutdown_metadata_flush_timeout;
        match run_sync_with_timeout(metadata_flush_timeout, move || {
            meta.flush_metadata().map_err(VfsError::Meta)
        }) {
            Ok(Some(())) => {}
            Ok(None) => {
                warn!(
                    timeout_ms = metadata_flush_timeout.as_millis() as u64,
                    "VFS shutdown: metadata flush timed out, leaving remote sync for next session"
                );
            }
            Err(e) => {
                warn!("VFS shutdown: metadata flush failed: {}", e);
            }
        }
        // Stop any background metadata threads (e.g. PgMetaEngine poll) so
        // their Arc references are released before process exit, allowing
        // redb::Database::drop() to run cleanly.
        self.sync_meta().prepare_for_close();
        self.persist_cache().await;
        self.state
            .shutdown
            .shutdown_completed
            .store(true, Ordering::Release);
        info!(
            total_ms = t0.elapsed().as_millis() as u64,
            "VFS shutdown: complete"
        );
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use studiofs_meta::engine::{MetaBlocks, MetaInodes};
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("namespace-resolver-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("namespace-resolver-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    fn make_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, volume_suffix);
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn startup_replay_does_not_override_current_epoch_live_extent_with_stale_pending_upload() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-stale-pending");

        let created = vfs
            .create(1, b"startup-stale-pending.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let mut attr = meta.get_attr(ino).expect("load test attr");
        attr.size = 4096;
        attr.committed_size = 4096;
        attr.content_epoch = 5;
        attr.committed_content_epoch = 4;
        attr.upload_state = MetaUploadState::Uploading;
        meta.set_attr(&attr).expect("persist staged attr");
        meta.set_file_block(ino, 0, &BlockRef::new_partial(9001, 4096))
            .expect("persist current file block");

        let pending_uploads = vec![PendingUpload {
            ino,
            offset: 0,
            object_id: 7001,
            content_epoch: 4,
            created_unix_ms: 100,
            retry_count: 0,
        }];

        vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

        assert_eq!(
            vfs.live_extent_records_for_inode(ino),
            vec![(0, 9001, 5)],
            "startup replay must preserve the current metadata epoch instead of overwriting it with stale pending uploads"
        );
    }

    #[test]
    fn startup_replay_keeps_metadata_live_extent_when_pending_upload_has_same_epoch() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "startup-replay-same-epoch");

        let created = vfs
            .create(1, b"startup-same-epoch.bin", 0, 0, 0o644)
            .expect("create test file");
        let ino = created.ino;
        let mut attr = meta.get_attr(ino).expect("load test attr");
        attr.size = 4096;
        attr.committed_size = 4096;
        attr.content_epoch = 5;
        attr.committed_content_epoch = 4;
        attr.upload_state = MetaUploadState::Uploading;
        meta.set_attr(&attr).expect("persist staged attr");
        meta.set_file_block(ino, 0, &BlockRef::new_partial(9001, 4096))
            .expect("persist current file block");

        let pending_uploads = vec![PendingUpload {
            ino,
            offset: 0,
            object_id: 7001,
            content_epoch: 5,
            created_unix_ms: 100,
            retry_count: 0,
        }];

        vfs.replay_dirty_uploads_from_local_backend(&pending_uploads, &[attr]);

        assert_eq!(
            vfs.live_extent_records_for_inode(ino),
            vec![(0, 9001, 5)],
            "startup replay must keep the metadata-derived block when pending upload only matches the same epoch"
        );
    }

    #[test]
    fn startup_replay_removes_orphan_dirty_blocks_and_clears_pending_evictions() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "startup-replay-orphan-cleanup");
        let object_id = 991_337;

        vfs.local_block_backend()
            .put(object_id, b"orphan-dirty-block", false)
            .expect("persist orphan dirty block");
        vfs.local_block_backend()
            .sync_data()
            .expect("sync orphan dirty block");
        vfs.state.upload.pending_evictions.lock().insert(object_id);

        assert!(
            vfs.local_block_backend()
                .dirty_object_ids()
                .contains(&object_id),
            "test setup should expose orphan as dirty before replay"
        );
        assert!(
            vfs.state
                .upload
                .pending_evictions
                .lock()
                .contains(&object_id),
            "test setup should seed pending eviction before replay"
        );

        vfs.replay_dirty_uploads_from_local_backend(&[], &[]);

        assert!(
            !vfs.local_block_backend().contains(object_id),
            "startup replay should remove orphan dirty block without metadata reference"
        );
        assert!(
            !vfs.local_block_backend()
                .dirty_object_ids()
                .contains(&object_id),
            "startup replay should clear orphan dirty block from dirty index"
        );
        assert!(
            !vfs.state
                .upload
                .pending_evictions
                .lock()
                .contains(&object_id),
            "startup replay should clear pending eviction bookkeeping for removed orphan"
        );
        assert_eq!(
            vfs.state
                .upload
                .upload_canceled_stale
                .load(Ordering::Relaxed),
            1,
            "startup replay should attribute removed orphan to stale-upload cleanup"
        );
    }

    #[test]
    fn cache_backend_detection_errors_are_structured() {
        let error = std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated permission error",
        );
        let mapped = map_cache_backend_detection_error("E:\\cache-root", error);
        match mapped {
            VfsError::CacheBackendDetection { cache_root, source } => {
                assert_eq!(cache_root, "E:\\cache-root");
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn test_block_index_calculation() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(block_index_from_offset(0, block_size), 0);
        assert_eq!(block_index_from_offset(1, block_size), 0);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
    }

    #[test]
    fn cache_runtime_tuning_enforces_bounds() {
        let tiny =
            derive_cache_runtime_tuning(2 * 1024 * 1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024);
        let huge = derive_cache_runtime_tuning(
            16 * 1024 * 1024 * 1024 * 1024,
            16 * 1024 * 1024,
            8 * 1024 * 1024 * 1024,
        );

        assert!(tiny.max_dirty_bytes > 0);
        assert!(tiny.upload_cache_io_slots > 0);
        assert_eq!(tiny.max_dirty_bytes, CACHE_RUNTIME_MIN_DIRTY_BYTES);
        assert_eq!(huge.max_dirty_bytes, CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE);
        assert!(huge.max_dirty_bytes >= tiny.max_dirty_bytes);
        assert!(huge.upload_cache_io_slots >= tiny.upload_cache_io_slots);
    }

    #[test]
    fn local_backend_budget_honors_runtime_dirty_window() {
        let runtime_tuning = CacheRuntimeTuning {
            max_dirty_bytes: 1024 * 1024,
            write_eager_flush_batch_bytes: 512 * 1024,
            max_ingress_segment_bytes: 16 * 1024 * 1024,
            ingress_checkpoint_interval_secs: 15,
            ingress_checkpoint_min_append_bytes: 16 * 1024 * 1024,
            ingress_checkpoint_min_mutations: 64,
            ingress_checkpoint_max_segment_lag: 2,
            ingress_checkpoint_max_live_segments: 4,
            upload_cache_io_slots: 16,
            transit_memory_budget_bytes: 1024 * 1024,
        };

        assert_eq!(
            derive_local_backend_budget_bytes(4096, 512 * 1024, runtime_tuning),
            1024 * 1024
        );
    }

    #[test]
    fn local_backend_budget_never_drops_below_block_size() {
        let runtime_tuning = CacheRuntimeTuning {
            max_dirty_bytes: 128 * 1024,
            write_eager_flush_batch_bytes: 128 * 1024,
            max_ingress_segment_bytes: 16 * 1024 * 1024,
            ingress_checkpoint_interval_secs: 15,
            ingress_checkpoint_min_append_bytes: 16 * 1024 * 1024,
            ingress_checkpoint_min_mutations: 64,
            ingress_checkpoint_max_segment_lag: 2,
            ingress_checkpoint_max_live_segments: 4,
            upload_cache_io_slots: 16,
            transit_memory_budget_bytes: 1024 * 1024,
        };

        assert_eq!(
            derive_local_backend_budget_bytes(64 * 1024, 512 * 1024, runtime_tuning),
            512 * 1024
        );
    }

    #[test]
    fn cache_runtime_tuning_uses_internal_config_overrides_when_non_default() {
        let derived = derive_cache_runtime_tuning(16 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
        let defaults = VfsConfig::default();
        let mut config = defaults.clone();
        config.max_dirty_bytes = 1024 * 1024;
        config.write_eager_flush_batch_bytes = 1024 * 1024;
        config.upload_cache_io_slots = 7;
        let overrides = CacheRuntimeTuningOverrides {
            max_dirty_bytes: true,
            write_eager_flush_batch_bytes: true,
            upload_cache_io_slots: true,
            ..Default::default()
        };

        let resolved = resolve_cache_runtime_tuning_overrides(derived, &config, overrides);

        assert_eq!(resolved.max_dirty_bytes, 1024 * 1024);
        assert_eq!(resolved.write_eager_flush_batch_bytes, 1024 * 1024);
        assert_eq!(resolved.upload_cache_io_slots, 7);
        assert_eq!(
            resolved.ingress_checkpoint_interval_secs,
            derived.ingress_checkpoint_interval_secs
        );
    }

    #[test]
    fn cache_runtime_tuning_respects_explicit_default_values_from_raw_config() {
        let validated = VfsConfigRaw {
            max_dirty_bytes: Some(DEFAULT_MAX_DIRTY_BYTES),
            write_eager_flush_batch_bytes: Some(DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES),
            ..Default::default()
        }
        .validate()
        .expect("validate explicit default runtime tuning values");
        let derived =
            derive_cache_runtime_tuning(16 * 1024 * 1024 * 1024, 512 * 1024, 16 * 1024 * 1024);
        let resolved = resolve_cache_runtime_tuning_overrides(
            derived,
            &validated,
            validated.cache_runtime_overrides(),
        );

        assert_ne!(derived.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
        assert_eq!(resolved.max_dirty_bytes, DEFAULT_MAX_DIRTY_BYTES);
        assert_eq!(
            resolved.write_eager_flush_batch_bytes,
            DEFAULT_WRITE_EAGER_FLUSH_BATCH_BYTES
        );
    }

    #[test]
    fn vfs_config_raw_validate_clamps_and_normalizes() {
        let validated = VfsConfigRaw {
            janitor_interval_secs: Some(0),
            prefetch_concurrency: Some(0),
            slab_flush_interval_ms: Some(0),
            upload_worker_count: Some(0),
            principal: Some("   ".to_string()),
            ..Default::default()
        }
        .validate()
        .expect("validate clamping");

        assert_eq!(validated.janitor_interval_secs, 1);
        assert_eq!(
            validated.prefetch_concurrency,
            DEFAULT_PREFETCH_CONCURRENCY_MIN
        );
        assert_eq!(validated.slab_flush_interval_ms, 1);
        assert_eq!(validated.upload_worker_count, 128);
        assert_eq!(validated.principal, "*");
    }

    #[test]
    fn upload_pipeline_summary_logs_only_when_pending_work_exists() {
        assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 0));
        assert!(StudioVfs::upload_pipeline_summary_is_active(1, 0));
        assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 1)); // < 10 threshold
        assert!(StudioVfs::upload_pipeline_summary_is_active(0, 10));
    }

    #[test]
    fn upload_pipeline_summary_is_active_checks() {
        assert!(StudioVfs::upload_pipeline_summary_is_active(256, 0));
        assert!(!StudioVfs::upload_pipeline_summary_is_active(0, 9));
        assert!(StudioVfs::upload_pipeline_summary_is_active(0, 11));
    }

    #[test]
    fn upload_pipeline_summary_formats_pending_uploads() {
        assert_eq!(StudioVfs::format_upload_pipeline_pending_uploads(0), "0");
        assert_eq!(
            StudioVfs::format_upload_pipeline_pending_uploads(512),
            "512B"
        );
        assert_eq!(
            StudioVfs::format_upload_pipeline_pending_uploads(12 * 1024),
            "12KB"
        );
        assert_eq!(
            StudioVfs::format_upload_pipeline_pending_uploads(1904 * 1024 * 1024),
            "1.9GB"
        );
    }

    #[test]
    fn foreground_speedometer_p90_uses_recent_tail() {
        let p90 = StudioVfs::p90_ns(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(p90, Some(9));
    }

    #[test]
    fn foreground_speedometer_p90_handles_empty_input() {
        assert_eq!(StudioVfs::p90_ns(vec![]), None);
    }

    #[test]
    fn foreground_speedometer_trim_drops_expired_samples() {
        let mut samples = std::collections::VecDeque::from([
            ForegroundReadSample {
                timestamp_ms: 100,
                latency_ns: 10,
                bytes_read: 4_096,
                access_class: AccessClass::RandomInteractiveRead,
                policy: AccessPolicy::default(),
            },
            ForegroundReadSample {
                timestamp_ms: 200,
                latency_ns: 20,
                bytes_read: 4_096,
                access_class: AccessClass::RandomInteractiveRead,
                policy: AccessPolicy::default(),
            },
            ForegroundReadSample {
                timestamp_ms: 350,
                latency_ns: 30,
                bytes_read: 4_096,
                access_class: AccessClass::SequentialMediaRead,
                policy: AccessPolicy::from_class(
                    AccessClass::SequentialMediaRead,
                    AccessClassifierConfig {
                        block_size: 4096,
                        readahead_base_width: 8,
                        readahead_max_width: 32,
                        readahead_min_prefetch: 2,
                    },
                ),
            },
        ]);
        ReadState::trim_foreground_latency_samples(&mut samples, 400, Duration::from_millis(150));
        let kept: Vec<(u64, u64, u64)> = samples
            .into_iter()
            .map(|sample| (sample.timestamp_ms, sample.latency_ns, sample.bytes_read))
            .collect();
        assert_eq!(kept, vec![(350, 30, 4_096)]);
    }

    #[test]
    fn foreground_read_active_respects_idle_grace_after_last_read() {
        let state = ReadState::new(1);
        let grace = Duration::from_millis(50);

        state.mark_foreground_read(100);
        assert!(state.foreground_read_active(100, grace));

        state.unmark_foreground_read(120);
        assert!(
            state.foreground_read_active(169, grace),
            "idle grace should keep reads active shortly after scope exit"
        );
        assert!(
            !state.foreground_read_active(171, grace),
            "foreground read activity should expire after the configured idle grace"
        );
    }

    #[test]
    fn foreground_read_scope_records_sample_and_clears_inflight() {
        let state = ReadState::new(1);

        {
            let mut scope = ForegroundReadScope::new(
                &state,
                Duration::from_secs(1),
                AccessPolicy::from_class(
                    AccessClass::SequentialMediaRead,
                    AccessClassifierConfig {
                        block_size: 4096,
                        readahead_base_width: 8,
                        readahead_max_width: 32,
                        readahead_min_prefetch: 2,
                    },
                ),
            );
            scope.record_bytes(4096);
            assert_eq!(
                state.foreground_reads_inflight.load(Ordering::Relaxed),
                1,
                "creating a scope should mark one active foreground read"
            );
        }

        assert_eq!(
            state.foreground_reads_inflight.load(Ordering::Relaxed),
            0,
            "dropping the scope should clear the inflight foreground read"
        );

        let (p90_ns, total_samples, window_bytes, window_coverage_ms) =
            state.foreground_speedometer_snapshot(now_unix_millis(), Duration::from_secs(1));
        assert!(
            p90_ns.is_some(),
            "dropping the scope should record latency telemetry"
        );
        assert_eq!(total_samples, 1);
        assert_eq!(window_bytes, 4096);
        assert!(window_coverage_ms >= 1);
    }

    #[test]
    fn release_fh_clears_per_stream_read_tracking() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-clears-stream-state");
        let _guard = rt.enter();

        rt.block_on(async {
            let attr = vfs.create(1, b"stream-state.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
            let stream_id = vfs.get_open_file(fh).unwrap().stream_id();
            let key = ReadStreamKey { ino, stream_id };
            let policy =
                AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_classifier)
                    .readahead_policy();

            let _ = vfs.readahead.record_read_for_stream(key, 0, 0, policy);
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .insert((ino, stream_id), (0, 1));

            assert!(vfs.readahead.debug_has_stream(key));
            assert!(
                vfs.state
                    .read
                    .prefetch_last_hint
                    .lock()
                    .contains_key(&(ino, stream_id))
            );

            vfs.release_fh(fh).await.unwrap();

            assert!(!vfs.readahead.debug_has_stream(key));
            assert!(
                !vfs.state
                    .read
                    .prefetch_last_hint
                    .lock()
                    .contains_key(&(ino, stream_id))
            );
        });
    }

    #[test]
    fn fallback_read_access_tracking_does_not_increment_active_handle_counts() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "fallback-access-tracking");
        let _guard = rt.enter();

        rt.block_on(async {
            let attr = vfs
                .create(1, b"fallback-access-tracking.bin", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let block = vfs.access_classifier.block_size as usize;

            let mut last = AccessClass::RandomInteractiveRead;
            for step in 0..4u64 {
                last = vfs
                    .state
                    .read
                    .observe_read_access(
                        None,
                        ino,
                        step * block as u64,
                        block,
                        vfs.access_classifier,
                    )
                    .class;
            }

            assert_eq!(
                last,
                AccessClass::SequentialMediaRead,
                "contiguous fallback reads should still classify as sequential"
            );

            let stats = vfs.foreground_read_protection_stats();
            assert_eq!(
                stats.active_sequential_media_reads, 0,
                "fallback tracking must not pretend there is an active handle"
            );
            assert_eq!(
                stats.active_random_interactive_reads, 0,
                "fallback tracking must not perturb active read-handle counts"
            );
            assert!(
                vfs.state.read.access_class_transitions[AccessClass::SequentialMediaRead.index()]
                    .load(Ordering::Relaxed)
                    >= 1,
                "fallback tracking should still contribute access-class transition telemetry"
            );
        });
    }

    #[test]
    fn handle_read_access_tracking_promotes_active_class_counts() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "handle-access-tracking");
        let _guard = rt.enter();

        rt.block_on(async {
            let attr = vfs
                .create(1, b"handle-access-tracking.bin", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
            let block = vfs.access_classifier.block_size as usize;

            assert_eq!(
                vfs.state
                    .read
                    .active_access_class_count(AccessClass::RandomInteractiveRead),
                1,
                "new read handle should start in the random-interactive class"
            );
            assert_eq!(
                vfs.state
                    .read
                    .active_access_class_count(AccessClass::SequentialMediaRead),
                0
            );

            let mut last = AccessClass::RandomInteractiveRead;
            for step in 0..4u64 {
                let open_file = vfs.get_open_file(fh).expect("open file");
                last = vfs
                    .state
                    .read
                    .observe_read_access(
                        Some(&open_file),
                        ino,
                        step * block as u64,
                        block,
                        vfs.access_classifier,
                    )
                    .class;
            }

            assert_eq!(
                last,
                AccessClass::SequentialMediaRead,
                "contiguous handle reads should promote the handle into sequential classification"
            );
            assert_eq!(
                vfs.state
                    .read
                    .active_access_class_count(AccessClass::RandomInteractiveRead),
                0,
                "promotion should decrement the previous active class"
            );
            assert_eq!(
                vfs.state
                    .read
                    .active_access_class_count(AccessClass::SequentialMediaRead),
                1,
                "promotion should increment the new active class"
            );

            vfs.release_fh(fh).await.unwrap();
        });
    }

    #[test]
    fn clear_access_tracking_clears_fallback_states_and_hints() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "clear-access-tracking");
        let _guard = rt.enter();

        rt.block_on(async {
            let attr = vfs
                .create(1, b"clear-access-tracking.bin", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let block = vfs.access_classifier.block_size as usize;

            let _ = vfs
                .state
                .read
                .observe_read_access(None, ino, 0, block, vfs.access_classifier);
            let _ = vfs
                .state
                .read
                .observe_write_access(None, ino, 0, block, vfs.access_classifier);
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .insert((ino, 77), (4, 2));

            assert!(
                vfs.state
                    .read
                    .read_access_fallback
                    .lock()
                    .contains_key(&ino)
            );
            assert!(
                vfs.state
                    .read
                    .write_access_fallback
                    .lock()
                    .contains_key(&ino)
            );
            assert!(
                vfs.state
                    .read
                    .prefetch_last_hint
                    .lock()
                    .contains_key(&(ino, 77))
            );

            vfs.clear_access_tracking(ino);

            assert!(
                !vfs.state
                    .read
                    .read_access_fallback
                    .lock()
                    .contains_key(&ino)
            );
            assert!(
                !vfs.state
                    .read
                    .write_access_fallback
                    .lock()
                    .contains_key(&ino)
            );
            assert!(
                !vfs.state
                    .read
                    .prefetch_last_hint
                    .lock()
                    .contains_key(&(ino, 77))
            );
        });
    }

    #[test]
    fn foreground_throughput_uses_minimum_coverage_floor_for_short_windows() {
        let throughput = StudioVfs::foreground_throughput_bytes_per_sec(1, 1_048_576, 10);
        assert_eq!(throughput, 10_485_760.0);
    }

    #[test]
    fn foreground_throughput_is_zero_without_samples() {
        let throughput = StudioVfs::foreground_throughput_bytes_per_sec(0, 1_048_576, 10);
        assert_eq!(throughput, 0.0);
    }

    #[test]
    fn test_run_sync_with_timeout_returns_result_before_deadline() {
        let result = run_sync_with_timeout(Duration::from_millis(100), || Ok::<_, VfsError>(7))
            .expect("operation should succeed");
        assert_eq!(result, Some(7));
    }

    #[test]
    fn test_run_sync_with_timeout_returns_none_on_timeout() {
        let finished = Arc::new(AtomicBool::new(false));
        let finished_clone = finished.clone();
        let t0 = std::time::Instant::now();
        let result = run_sync_with_timeout(Duration::from_millis(10), move || {
            std::thread::sleep(Duration::from_millis(100));
            finished_clone.store(true, AtomicOrdering::Release);
            Ok::<_, VfsError>(())
        })
        .expect("timeout helper should not fail");

        // The primary timeout should report None (timed out).
        assert_eq!(result, None);
        // The thread is joined to ensure clean Arc release, so the operation
        // finishes by the time we return.
        assert!(
            finished.load(AtomicOrdering::Acquire),
            "thread should have been joined after timeout"
        );
        assert!(
            t0.elapsed() >= Duration::from_millis(100),
            "run_sync_with_timeout should wait for thread join"
        );
    }

    #[test]
    fn test_offset_within_block() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(offset_within_block(0, block_size), 0);
        assert_eq!(offset_within_block(100, block_size), 100);
        assert_eq!(offset_within_block(4 * 1024 * 1024, block_size), 0);
        assert_eq!(offset_within_block(4 * 1024 * 1024 + 100, block_size), 100);
    }

    #[test]
    fn test_sticky_upload_error_is_deterministic_across_sync_points() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-upload-error");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"sticky.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

            let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
            assert!(
                fsync_err.to_string().contains("sticky upload error"),
                "fsync should surface sticky upload errors"
            );
            let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
            assert!(
                fsync_err_again.to_string().contains("sticky upload error"),
                "sticky upload errors should be deterministic across fsync calls"
            );
            vfs.clear_upload_sticky_error(ino);
            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.fsync(ino, false).await.unwrap();

            vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));
            let flush_err = vfs.flush_fh(fh).await.unwrap_err();
            assert!(
                flush_err.to_string().contains("sticky upload error"),
                "flush_fh should surface sticky upload errors"
            );
            let flush_err_again = vfs.flush_fh(fh).await.unwrap_err();
            assert!(
                flush_err_again.to_string().contains("sticky upload error"),
                "sticky upload errors should be deterministic across flush_fh calls"
            );
            vfs.clear_upload_sticky_error(ino);
            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();

            vfs.release_fh(fh).await.unwrap();
        });
    }

    #[test]
    fn test_fsync_reports_sticky_error_after_persisting_current_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-fsync-order");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"sticky-fsync.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let payload = b"persist-before-sticky-report";

            vfs.write(ino, 0, payload).await.unwrap();
            vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

            let fsync_err = vfs.fsync(ino, false).await.unwrap_err();
            assert!(
                fsync_err.to_string().contains("sticky upload error"),
                "fsync should still report sticky upload errors"
            );

            let after = vfs.getattr(ino).unwrap();
            assert_eq!(
                after.size,
                payload.len() as u64,
                "fsync must persist current dirty attrs before reporting sticky EIO"
            );

            let fsync_err_again = vfs.fsync(ino, false).await.unwrap_err();
            assert!(
                fsync_err_again.to_string().contains("sticky upload error"),
                "sticky upload errors should remain visible until recovery"
            );
            vfs.clear_upload_sticky_error(ino);
            vfs.fsync(ino, false).await.unwrap();
        });
    }

    #[test]
    fn test_release_fh_reports_sticky_error_after_persisting_current_changes() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "sticky-release-order");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"sticky-release.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let payload = b"persist-before-release-sticky-report";

            vfs.write(ino, 0, payload).await.unwrap();
            vfs.mark_upload_sticky_error(ino, &VfsError::Other("simulated upload failure".into()));

            let release_err = vfs.release_fh(fh).await.unwrap_err();
            assert!(
                release_err.to_string().contains("sticky upload error"),
                "release_fh should surface sticky upload errors"
            );
            assert_eq!(
                vfs.open_count(ino),
                0,
                "release_fh should still close the handle"
            );

            let after = vfs.getattr(ino).unwrap();
            assert_eq!(
                after.size,
                payload.len() as u64,
                "release_fh must persist local dirty state before reporting sticky EIO"
            );
        });
    }

    #[test]
    fn test_release_fh_with_outcome_distinguishes_nonfinal_and_final_close() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-close-ownership");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"release-outcome.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            vfs.write(ino, 0, b"payload").await.unwrap();

            let first = vfs.release_fh_with_outcome(fh1).await.unwrap();
            assert!(
                !first.closed_last_handle,
                "closing one of multiple handles must not report last-handle ownership"
            );
            assert!(
                !first.flushed_local_dirty_state,
                "non-final close should not attribute a local dirty-state drain"
            );
            assert_eq!(vfs.open_count(ino), 1, "one handle should remain open");

            let second = vfs.release_fh_with_outcome(fh2).await.unwrap();
            assert!(
                second.closed_last_handle,
                "the final close should report last-handle ownership"
            );
            assert!(
                second.flushed_local_dirty_state,
                "the final close should report the fallback local dirty-state drain"
            );
            assert_eq!(vfs.open_count(ino), 0, "all handles should be closed");
        });
    }

    #[test]
    fn test_release_fh_with_outcome_reports_clean_skip_after_explicit_flush() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-clean-skip");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"release-outcome-clean-skip.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.flush_fh(fh).await.unwrap();

            let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
            assert!(
                outcome.closed_last_handle,
                "the only handle should still be reported as the last-handle close"
            );
            assert!(
                !outcome.flushed_local_dirty_state,
                "release should report a clean skip after explicit flush already drained local state"
            );
            assert_eq!(vfs.open_count(ino), 0, "release should still close the handle");
        });
    }

    #[test]
    fn flush_then_release_keeps_written_bytes_visible_to_reopened_reader() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-release-reopen-read");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"flush-release-read.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let payload = b"flush-close-contract";

            vfs.write(ino, 0, payload).await.unwrap();
            vfs.flush_fh(fh).await.unwrap();
            let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
            assert!(outcome.closed_last_handle);
            assert!(
                !outcome.flushed_local_dirty_state,
                "explicit flush should leave release as a clean close"
            );

            let reopened = vfs.open_file(ino, OpenFlags::RDONLY, false).unwrap();
            let bytes = vfs.read(ino, 0, payload.len() as u32).await.unwrap();
            assert_eq!(bytes.as_ref(), payload);
            vfs.release_fh(reopened).await.unwrap();
        });
    }

    #[test]
    fn test_release_fh_with_outcome_orphan_cleanup_does_not_report_flush_barrier() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-outcome-orphan-cleanup");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"release-outcome-orphan.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.unlink(1, b"release-outcome-orphan.txt").unwrap();

            let outcome = vfs.release_fh_with_outcome(fh).await.unwrap();
            assert!(
                outcome.closed_last_handle,
                "orphan cleanup should still be owned by the last-handle release"
            );
            assert!(
                !outcome.flushed_local_dirty_state,
                "orphan cleanup should not be reported as a normal release flush barrier"
            );
            assert!(
                vfs.getattr(ino).is_err(),
                "orphan cleanup should remove the inode"
            );
        });
    }

    #[test]
    fn test_release_fh_orphan_cleanup_clears_live_extents() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-live-extents");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"release-orphan-live-extents.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.flush(ino).await.unwrap();
            assert!(
                !vfs.live_extent_records_for_inode(ino).is_empty(),
                "flush should materialize live extents before orphan cleanup"
            );

            vfs.unlink(1, b"release-orphan-live-extents.txt").unwrap();
            assert!(
                !vfs.live_extent_records_for_inode(ino).is_empty(),
                "open-unlink should keep live extents available until the last handle closes"
            );

            vfs.release_fh(fh).await.unwrap();
            assert!(
                vfs.live_extent_records_for_inode(ino).is_empty(),
                "orphan cleanup should clear live extent identities on final close"
            );
        });
    }

    #[test]
    fn test_release_fh_orphan_cleanup_waits_for_final_handle_before_clearing_live_extents() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "release-orphan-final-handle-live-extents");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"release-orphan-final-handle.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            vfs.write(ino, 0, b"payload").await.unwrap();
            vfs.flush(ino).await.unwrap();
            assert!(
                !vfs.live_extent_records_for_inode(ino).is_empty(),
                "flush should materialize live extents before orphan cleanup"
            );

            vfs.unlink(1, b"release-orphan-final-handle.txt").unwrap();
            vfs.release_fh(fh1).await.unwrap();
            assert!(
                !vfs.live_extent_records_for_inode(ino).is_empty(),
                "non-final close must not clear orphan live extents"
            );

            vfs.release_fh(fh2).await.unwrap();
            assert!(
                vfs.live_extent_records_for_inode(ino).is_empty(),
                "final orphan close must clear live extents"
            );
        });
    }

    #[test]
    fn test_unlink_immediate_cleanup_clears_dirty_local_state() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "unlink-immediate-cleanup");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"unlink-immediate-cleanup.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            vfs.write(ino, 0, b"payload").await.unwrap();
            assert!(
                vfs.inode_has_local_dirty_state(ino),
                "write should leave local dirty state before unlink"
            );

            vfs.unlink(1, b"unlink-immediate-cleanup.txt").unwrap();

            assert!(
                vfs.getattr(ino).is_err(),
                "unlink should remove the inode immediately"
            );
            assert!(
                !vfs.inode_has_local_dirty_state(ino),
                "immediate unlink should clear local dirty state for the deleted inode"
            );
        });
    }

    #[test]
    fn test_rename_over_existing_target_clears_displaced_inode_dirty_state() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "rename-displaced-cleanup");
        let _guard = rt.enter();
        rt.block_on(async {
            let source = vfs.create(1, b"rename-source.txt", 0, 0, 0o644).unwrap();
            let displaced = vfs.create(1, b"rename-target.txt", 0, 0, 0o644).unwrap();

            vfs.write(source.ino, 0, b"source").await.unwrap();
            vfs.write(displaced.ino, 0, b"target").await.unwrap();
            assert!(
                vfs.inode_has_local_dirty_state(displaced.ino),
                "target should have local dirty state before it is displaced"
            );

            vfs.rename(1, b"rename-source.txt", 1, b"rename-target.txt")
                .unwrap();

            assert!(
                vfs.getattr(displaced.ino).is_err(),
                "displaced inode should be removed after rename overwrite"
            );
            assert!(
                !vfs.inode_has_local_dirty_state(displaced.ino),
                "rename overwrite should clear local dirty state for the displaced inode"
            );
        });
    }

    #[test]
    fn test_flush_fh_with_outcome_distinguishes_clean_and_dirty_barriers() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-handle-outcome");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"flush-handle-outcome.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();

            let clean = vfs.flush_fh_with_outcome(fh).await.unwrap();
            assert!(
                !clean.flushed_local_dirty_state,
                "clean handle flush should report a clean skip"
            );

            let payload = b"payload";
            vfs.write(ino, 0, payload).await.unwrap();

            let dirty = vfs.flush_fh_with_outcome(fh).await.unwrap();
            assert!(
                dirty.flushed_local_dirty_state,
                "dirty handle flush should report that it drained local dirty state"
            );

            let after = vfs.getattr(ino).unwrap();
            assert_eq!(
                after.size,
                payload.len() as u64,
                "flush_fh_with_outcome should still persist current dirty attrs"
            );

            vfs.release_fh(fh).await.unwrap();
        });
    }

    #[test]
    fn test_flush_with_outcome_distinguishes_clean_and_dirty_inode_barriers() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "flush-inode-outcome");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"flush-inode-outcome.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            let clean = vfs.flush_with_outcome(ino).await.unwrap();
            assert!(
                !clean.flushed_local_dirty_state,
                "clean inode flush should report a clean skip"
            );

            let payload = b"payload";
            vfs.write(ino, 0, payload).await.unwrap();

            let dirty = vfs.flush_with_outcome(ino).await.unwrap();
            assert!(
                dirty.flushed_local_dirty_state,
                "dirty inode flush should report that it drained local dirty state"
            );

            let after = vfs.getattr(ino).unwrap();
            assert_eq!(
                after.size,
                payload.len() as u64,
                "flush_with_outcome should still persist current dirty attrs"
            );
        });
    }

    #[test]
    fn test_fsync_with_outcome_distinguishes_clean_and_dirty_data_barriers() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "fsync-outcome-clean-data");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"fsync-outcome-clean-data.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            let clean = vfs.fsync_with_outcome(ino, false).await.unwrap();
            assert!(
                !clean.flushed_file_data,
                "clean fsync should not report a file-data barrier"
            );
            assert!(
                !clean.persisted_dirty_attrs,
                "clean fsync should not report metadata persistence work"
            );

            let payload = b"payload";
            vfs.write(ino, 0, payload).await.unwrap();

            let dirty = vfs.fsync_with_outcome(ino, false).await.unwrap();
            assert!(
                dirty.flushed_file_data,
                "dirty fsync should report draining file data"
            );
        });
    }

    #[test]
    fn test_datasync_with_outcome_treats_already_persisted_metadata_as_clean() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "datasync-outcome-persisted-metadata");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"datasync-outcome-persisted-metadata.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            vfs.setattr(
                ino,
                SetattrArgs {
                    mtime: studiofs_meta::types::TimeSpec::Now,
                    ..Default::default()
                },
            )
            .unwrap();

            let datasync = vfs.fsync_with_outcome(ino, true).await.unwrap();
            assert!(
                !datasync.flushed_file_data,
                "metadata-only datasync should not report a file-data barrier"
            );
            assert!(
                !datasync.persisted_dirty_attrs,
                "datasync should not report metadata persistence when setattr already committed it"
            );

            let full = vfs.fsync_with_outcome(ino, false).await.unwrap();
            assert!(
                !full.flushed_file_data,
                "full fsync after already-persisted metadata should stay data-clean"
            );
            assert!(
                !full.persisted_dirty_attrs,
                "full fsync should not invent metadata persistence work after setattr already committed it"
            );
        });
    }

    #[test]
    fn test_file_session_wait_reasons_and_transitions_are_observable() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-observability");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"session-observability.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let _pin = vfs.state.file_session.session(ino);

            let session_guard = vfs
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await;
            let vfs_clone = vfs.clone();
            let waiter = tokio::spawn(async move {
                let _queued = vfs_clone
                    .state
                    .file_session
                    .enter_op(ino, FileSessionOperation::Read)
                    .await;
            });

            let mid = tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    let stats = vfs.file_session_stats();
                    if stats.queued_ops >= 1 {
                        break stats;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("expected at least one queued file-session operation");
            assert!(
                mid.queued_ops >= 1,
                "expected at least one queued file-session operation"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;

            drop(session_guard);
            waiter.await.unwrap();

            let final_stats = vfs.file_session_stats();
            assert!(
                final_stats.wait_events >= 1,
                "expected queued operation wait to be tracked"
            );
            assert!(
                final_stats.transitions >= 2,
                "expected start/finish transitions to be tracked"
            );
            assert!(
                final_stats
                    .wait_reasons
                    .iter()
                    .any(|(reason, count)| reason == "read" && *count >= 1),
                "expected wait reason for read operation"
            );
        });
    }

    #[test]
    fn test_file_session_read_lock_allows_parallel_readers() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-read-parallel");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"session-read-parallel.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            let read_guard = vfs
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Read)
                .await;
            let vfs_clone = vfs.clone();
            let second_reader = tokio::spawn(async move {
                let _second = vfs_clone
                    .state
                    .file_session
                    .enter_op(ino, FileSessionOperation::Read)
                    .await;
            });

            tokio::time::timeout(Duration::from_millis(150), second_reader)
                .await
                .expect("parallel read lock acquisition should not block")
                .unwrap();
            drop(read_guard);
        });
    }

    #[test]
    fn test_file_session_open_handle_stats_track_release_lifecycle() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-open-handle-stats");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"session-open-handle-stats.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            let fh1 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let after_first = vfs.file_session_stats();
            assert_eq!(
                after_first.active_sessions, 1,
                "opening the first handle should materialize one file session"
            );
            assert_eq!(
                after_first.open_handles, 1,
                "opening the first handle should increment open-handle tracking"
            );

            let fh2 = vfs.open_file(ino, OpenFlags::RDWR, false).unwrap();
            let after_second = vfs.file_session_stats();
            assert_eq!(
                after_second.open_handles, 2,
                "opening a second handle should keep both handles observable"
            );

            vfs.release_fh(fh1).await.unwrap();
            let after_first_release = vfs.file_session_stats();
            assert_eq!(
                after_first_release.active_sessions, 1,
                "the session should stay alive until the last handle closes"
            );
            assert_eq!(
                after_first_release.open_handles, 1,
                "releasing one of two handles should decrement the handle count only once"
            );

            vfs.release_fh(fh2).await.unwrap();
            let final_stats = vfs.file_session_stats();
            assert_eq!(
                final_stats.open_handles, 0,
                "last-handle release should clear file-session open-handle tracking"
            );
            assert_eq!(
                final_stats.active_sessions, 0,
                "last-handle release should collect the idle file session"
            );
        });
    }

    #[test]
    fn test_file_session_background_complete_gc_removes_idle_session() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-background-gc");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"session-background-gc.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;

            let _pin = vfs.state.file_session.session(ino);
            assert_eq!(
                vfs.file_session_stats().active_sessions,
                1,
                "pinning a session should materialize it for the test"
            );

            drop(_pin);
            vfs.state.file_session.note_background_transition(
                ino,
                FileSessionOperation::Upload,
                "complete",
            );

            assert_eq!(
                vfs.file_session_stats().active_sessions,
                0,
                "background completion should collect an otherwise idle session"
            );
        });
    }

    #[test]
    fn test_file_session_gc_removes_idle_read_only_session() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-gc");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs.create(1, b"session-gc.txt", 0, 0, 0o644).unwrap();
            let ino = attr.ino;
            let _ = vfs.read(ino, 0, 1).await.unwrap();
            let stats = vfs.file_session_stats();
            assert_eq!(
                stats.active_sessions, 0,
                "idle read-only session should be collected"
            );
        });
    }

    #[test]
    fn test_validate_name_valid() {
        assert!(validate_name(b"hello.txt").is_ok());
        assert!(validate_name(b"a").is_ok());
        assert!(validate_name(b"...").is_ok());
        assert!(validate_name(&vec![b'x'; 255]).is_ok());
    }

    #[test]
    fn test_validate_name_empty() {
        assert!(matches!(validate_name(b""), Err(VfsError::InvalidName(_))));
    }

    #[test]
    fn test_validate_name_too_long() {
        let long = vec![b'a'; 256];
        assert!(matches!(
            validate_name(&long),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_slash() {
        assert!(matches!(
            validate_name(b"a/b"),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_dot() {
        assert!(matches!(validate_name(b"."), Err(VfsError::InvalidName(_))));
        assert!(matches!(
            validate_name(b".."),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn test_validate_name_windows_special_chars() {
        // Test each Windows-invalid character: < > : " \ | ? *
        assert!(matches!(
            validate_name(b"file<name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file>name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file:name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file\"name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file\\name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file|name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file?name"),
            Err(VfsError::InvalidName(_))
        ));
        assert!(matches!(
            validate_name(b"file*name"),
            Err(VfsError::InvalidName(_))
        ));
    }

    #[test]
    fn attr_read_cache_is_bounded_and_tracks_hotset_hits() {
        let mut cache = AttrReadCache::new(2, AttrReadCache::estimate_entry_charge() * 4);
        let now = Instant::now();
        cache.put(
            1,
            InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
            ATTR_CACHE_TTL,
            now,
        );
        cache.put(
            2,
            InodeAttr::new_file(2, ROOT_INO, 1000, 1000, 0o644),
            ATTR_CACHE_TTL,
            now,
        );

        assert!(cache.get(1, now).is_some());
        assert!(cache.get(1, now).is_some());

        cache.put(
            3,
            InodeAttr::new_file(3, ROOT_INO, 1000, 1000, 0o644),
            ATTR_CACHE_TTL,
            now,
        );
        let metrics = cache.metrics();

        assert!(metrics.occupancy_entries <= metrics.entry_budget);
        assert!(metrics.occupancy_bytes <= metrics.byte_budget);
        assert!(metrics.protected_occupancy_entries >= 1);
        assert!(metrics.protected_hits >= 1);
        assert!(metrics.hot_hit_ratio() > 0.0);
    }

    #[test]
    fn attr_read_cache_expired_entries_do_not_increment_evictions() {
        let mut cache = AttrReadCache::new(8, AttrReadCache::estimate_entry_charge() * 8);
        let now = Instant::now();
        cache.put(
            1,
            InodeAttr::new_file(1, ROOT_INO, 1000, 1000, 0o644),
            Duration::from_millis(10),
            now,
        );

        assert!(
            cache.get(1, now + Duration::from_secs(1)).is_none(),
            "expired entry should be treated as stale miss"
        );
        let metrics = cache.metrics();
        assert_eq!(metrics.evictions, 0);
        assert_eq!(metrics.misses, 1);
    }

    #[test]
    fn test_probable_trash_component_names() {
        assert!(is_probable_trash_component(b".Trash"));
        assert!(is_probable_trash_component(b".Trashes"));
        assert!(is_probable_trash_component(b".Trash-501"));
        assert!(!is_probable_trash_component(b"Trash"));
        assert!(!is_probable_trash_component(b".Trashcan"));
    }

    #[test]
    fn strict_local_sync_flushes_pending_group_commit_bytes() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "strict-local-sync-group");
        rt.block_on(async {
            let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
            group.pending_bytes = 4096;
            group.last_sync_at = Instant::now();
        });

        let calls_before = vfs
            .state
            .metrics
            .block_store_sync_data_calls
            .load(Ordering::Relaxed);
        let synced_before = vfs
            .state
            .metrics
            .block_store_sync_data_synced_bytes
            .load(Ordering::Relaxed);

        rt.block_on(async {
            vfs.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Strict)
                .await
                .expect("strict local sync");
        });

        let calls_after = vfs
            .state
            .metrics
            .block_store_sync_data_calls
            .load(Ordering::Relaxed);
        let synced_after = vfs
            .state
            .metrics
            .block_store_sync_data_synced_bytes
            .load(Ordering::Relaxed);
        assert!(
            calls_after > calls_before,
            "strict sync should force backend sync_data"
        );
        assert!(
            synced_after >= synced_before + 4096,
            "strict sync should account for pending group-commit bytes"
        );
        rt.block_on(async {
            let group = vfs.state.write_buffer.local_sync_group.lock().await;
            assert_eq!(group.pending_bytes, 0);
        });
    }

    #[test]
    fn opportunistic_local_sync_defers_when_threshold_not_reached() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "opportunistic-local-sync-group");
        rt.block_on(async {
            let mut group = vfs.state.write_buffer.local_sync_group.lock().await;
            group.pending_bytes = 1024;
            group.last_sync_at = Instant::now();
        });

        let calls_before = vfs
            .state
            .metrics
            .block_store_sync_data_calls
            .load(Ordering::Relaxed);
        let deferred_before = vfs
            .state
            .metrics
            .block_store_sync_data_deferred_calls
            .load(Ordering::Relaxed);

        rt.block_on(async {
            vfs.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Opportunistic)
                .await
                .expect("opportunistic local sync");
        });

        let calls_after = vfs
            .state
            .metrics
            .block_store_sync_data_calls
            .load(Ordering::Relaxed);
        let deferred_after = vfs
            .state
            .metrics
            .block_store_sync_data_deferred_calls
            .load(Ordering::Relaxed);
        assert_eq!(
            calls_after, calls_before,
            "opportunistic sync should not force sync_data under threshold/window"
        );
        assert!(
            deferred_after > deferred_before,
            "opportunistic defer should be tracked"
        );
    }

    #[test]
    fn write_path_observability_tracks_burst_shape_and_metadata_commit_stalls() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-bursts");
        let block_size = meta.get_format_info().expect("get format info").block_size as usize;

        rt.block_on(async {
            let attr = vfs
                .create(1, b"write-path-burst.bin", 0, 0, 0o644)
                .expect("create");
            let first = vec![0x5Au8; block_size];
            let second = vec![0xA5u8; block_size];

            vfs.write(attr.ino, 0, &first).await.expect("first write");
            vfs.flush(attr.ino).await.expect("first flush");

            tokio::time::sleep(Duration::from_millis(2)).await;

            vfs.write(attr.ino, block_size as u64, &second)
                .await
                .expect("second write");
            vfs.flush(attr.ino).await.expect("second flush");

            // Force a deterministic durability barrier so the sync metric is stable.
            vfs.fsync(attr.ino, false).await.expect("fsync barrier");
        });

        let snapshot = vfs.write_path_observability_snapshot();
        assert!(
            snapshot.payload_burst.bursts >= 2,
            "two write+flush rounds should yield at least two payload bursts"
        );
        assert!(
            snapshot.payload_burst.total_blocks >= 2,
            "burst accounting should include persisted full blocks"
        );
        assert!(
            snapshot.payload_burst.total_bytes >= (block_size as u64) * 2,
            "burst accounting should include persisted payload bytes"
        );
        assert!(
            snapshot.payload_persist.calls >= 2,
            "payload stage timing should be captured per persisted burst"
        );
        assert!(
            snapshot.metadata_commit.calls >= 2,
            "metadata commit timing should be captured per persisted burst"
        );
        assert!(
            snapshot.metadata_commit_cadence.events >= 2,
            "metadata commit cadence should be captured per completed commit"
        );
        assert!(
            snapshot
                .metadata_commit_cadence
                .interval_histogram
                .bucket_counts
                .iter()
                .copied()
                .sum::<u64>()
                >= 1,
            "metadata commit cadence should record at least one interval sample after multiple commits"
        );
        assert!(
            snapshot.metadata_commit_cadence.max_interval_ms > 0.0,
            "metadata commit cadence should expose a positive interval after multiple commits"
        );
        assert!(
            snapshot.block_store_sync_data.calls >= 1,
            "sync barrier calls should be surfaced in write-path observability"
        );
        assert_eq!(
            snapshot.eager_flush.checks,
            snapshot.eager_flush.triggered
                + snapshot.eager_flush.skipped_empty
                + snapshot.eager_flush.skipped_below_threshold,
            "eager flush checks should be partitioned by outcome"
        );
    }

    #[test]
    fn write_path_observability_reports_active_tuning_knobs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "write-path-observability-tuning");
        let block_size = meta.get_format_info().expect("get format info").block_size as usize;

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.tuning.eager_flush_threshold_bytes, vfs.write_eager_flush_batch_bytes,
            "observability should report the active eager flush threshold"
        );
        assert!(
            snapshot.tuning.eager_flush_drain_chunk_bytes > 0,
            "observability should report a non-zero eager drain chunk"
        );
        assert!(
            snapshot.tuning.eager_flush_drain_chunk_bytes
                <= snapshot.tuning.eager_flush_threshold_bytes,
            "eager drain chunk should never exceed the trigger threshold"
        );
        assert!(
            snapshot.tuning.payload_put_concurrency >= 1,
            "payload put concurrency should be explicit in observability"
        );
        assert!(
            snapshot.tuning.payload_put_concurrency
                <= snapshot
                    .tuning
                    .eager_flush_drain_chunk_bytes
                    .div_ceil(block_size)
                    .max(1),
            "payload put concurrency should be bounded by the eager drain chunk"
        );
        assert_eq!(
            snapshot.tuning.sync_group_min_bytes, vfs.local_sync_group_min_bytes,
            "observability should report the active sync-data group minimum"
        );
        assert_eq!(
            snapshot.tuning.sync_group_max_delay_ms,
            vfs.local_sync_group_max_delay.as_millis() as u64,
            "observability should report the active sync-data delay cap"
        );
    }

    #[test]
    fn winfsp_observability_snapshot_tracks_callback_and_barrier_paths() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "winfsp-observability-snapshot");

        vfs.note_winfsp_write_callback();
        vfs.note_winfsp_overwrite_callback();
        vfs.note_winfsp_cleanup_callback();
        vfs.note_winfsp_cleanup_flush_fh_callback();
        vfs.note_winfsp_flush_callback();
        vfs.note_winfsp_close_callback();
        vfs.note_winfsp_cleanup_barrier_outcome(true);
        vfs.note_winfsp_cleanup_barrier_outcome(false);
        vfs.note_winfsp_flush_barrier_outcome(true);
        vfs.note_winfsp_flush_barrier_outcome(false);
        vfs.note_winfsp_close_barrier_outcome(false);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(snapshot.winfsp_callbacks.write, 1);
        assert_eq!(snapshot.winfsp_callbacks.overwrite, 1);
        assert_eq!(snapshot.winfsp_callbacks.cleanup, 1);
        assert_eq!(snapshot.winfsp_callbacks.cleanup_flush_fh, 1);
        assert_eq!(snapshot.winfsp_callbacks.flush, 1);
        assert_eq!(snapshot.winfsp_callbacks.close, 1);
        assert_eq!(snapshot.winfsp_foreground_flushes.cleanup, 1);
        assert_eq!(snapshot.winfsp_foreground_flushes.cleanup_skipped_clean, 1);
        assert_eq!(snapshot.winfsp_foreground_flushes.flush, 1);
        assert_eq!(snapshot.winfsp_foreground_flushes.flush_skipped_clean, 1);
        assert_eq!(snapshot.winfsp_foreground_flushes.close, 0);
        assert_eq!(snapshot.winfsp_foreground_flushes.close_skipped_clean, 1);
    }

    #[test]
    fn failed_payload_persist_attempts_do_not_count_as_completed_bursts() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "write-path-observability-failed-burst");

        vfs.record_payload_persist_observability(4, 16 * 1024, Duration::from_millis(3), false);

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.payload_persist.calls, 1,
            "payload stage latency should still include failed attempts"
        );
        assert_eq!(
            snapshot.payload_burst.bursts, 0,
            "failed payload persists must not count as completed bursts"
        );
        assert_eq!(
            snapshot.payload_burst.total_blocks, 0,
            "failed payload persists must not add completed burst block counts"
        );
        assert_eq!(
            snapshot.payload_burst.total_bytes, 0,
            "failed payload persists must not add completed burst bytes"
        );
    }

    #[test]
    fn local_cache_io_stats_track_handoff_and_backend_time_separately() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-io-stats");
        let calls_before = vfs
            .state
            .metrics
            .local_cache_io_calls
            .load(Ordering::Relaxed);
        let handoff_before = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        let backend_before = vfs
            .state
            .metrics
            .local_cache_io_backend_total_ns
            .load(Ordering::Relaxed);

        rt.block_on(async {
            vfs.block_store_put_async(0xAA55, Bytes::from_static(b"payload"), false)
                .await
                .expect("block store put");
            let payload = vfs
                .block_store_get_async(0xAA55)
                .await
                .expect("block store get")
                .expect("cached payload");
            assert_eq!(payload.as_ref(), b"payload");
        });

        let calls_after = vfs
            .state
            .metrics
            .local_cache_io_calls
            .load(Ordering::Relaxed);
        let handoff_after = vfs
            .state
            .metrics
            .local_cache_io_handoff_total_ns
            .load(Ordering::Relaxed);
        let backend_after = vfs
            .state
            .metrics
            .local_cache_io_backend_total_ns
            .load(Ordering::Relaxed);

        assert!(
            calls_after >= calls_before + 2,
            "put + get should record at least two local-cache I/O calls"
        );
        assert!(
            handoff_after >= handoff_before,
            "blocking-isolated local-cache operations may accrue scheduler handoff time"
        );
        assert!(
            backend_after >= backend_before,
            "backend execution time should be tracked independently"
        );

        let snapshot = vfs.write_path_observability_snapshot();
        assert_eq!(
            snapshot.local_cache_io.execution_model,
            "inline-runtime-worker-with-blocking-isolation"
        );
    }

    #[test]
    fn local_cache_io_stats_track_direct_partial_read_into() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-stats");
        let calls_before = vfs
            .state
            .metrics
            .local_cache_io_calls
            .load(Ordering::Relaxed);
        let backend_before = vfs
            .state
            .metrics
            .local_cache_io_backend_total_ns
            .load(Ordering::Relaxed);

        rt.block_on(async {
            vfs.block_store_put_async(0xBB66, Bytes::from_static(b"payload"), false)
                .await
                .expect("block store put");
            let mut out = [0_u8; 3];
            let read = vfs
                .block_store_read_into_async(0xBB66, 2, &mut out)
                .await
                .expect("block store read_into");
            assert_eq!(read, Some(3));
            assert_eq!(&out, b"ylo");
        });

        let calls_after = vfs
            .state
            .metrics
            .local_cache_io_calls
            .load(Ordering::Relaxed);
        let backend_after = vfs
            .state
            .metrics
            .local_cache_io_backend_total_ns
            .load(Ordering::Relaxed);

        assert!(
            calls_after >= calls_before + 2,
            "put + read_into should record local-cache I/O calls"
        );
        assert!(
            backend_after >= backend_before,
            "direct read_into should contribute backend execution timing"
        );
    }

    #[test]
    fn local_cache_read_into_returns_none_for_missing_block() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "local-cache-read-into-missing");
        rt.block_on(async {
            let mut out = [0_u8; 8];
            let read = vfs
                .block_store_read_into_async(0xDEADBEEF, 0, &mut out)
                .await
                .expect("read_into should not error on cache miss");
            assert_eq!(read, None);
        });
    }

    #[test]
    fn committed_copy_rejects_short_payload() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "committed-copy-short-payload");
        let block = BlockRef::new_full(42, vfs.block_size);
        let mut dst = [0_u8; 8];
        let err = vfs
            .copy_committed_block_payload_into_range(b"abcd", &block, 0, &mut dst)
            .expect_err("short committed payload should error");
        let message = match err {
            VfsError::Other(message) => message,
            other => panic!("expected VfsError::Other, got {other}"),
        };
        assert!(
            message.contains("committed block payload too short"),
            "error message should highlight short committed payload"
        );
    }

    #[test]
    fn test_ingress_checkpoint_due_on_timer_with_pending_churn() {
        assert!(ingress_checkpoint_due_with_policy(
            0, 1, 1024, 8, 0, 2, 1, 4, true
        ));
        assert!(!ingress_checkpoint_due_with_policy(
            0, 1, 1024, 8, 0, 2, 1, 4, false
        ));
    }

    #[test]
    fn test_ingress_checkpoint_due_on_threshold_notify() {
        assert!(ingress_checkpoint_due_with_policy(
            4096, 0, 4096, 8, 0, 2, 1, 4, false
        ));
        assert!(ingress_checkpoint_due_with_policy(
            0, 8, 4096, 8, 0, 2, 1, 4, false
        ));
        assert!(!ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 0, 2, 1, 4, false
        ));
    }

    #[test]
    fn test_ingress_checkpoint_due_on_segment_lag_or_live_segment_spread() {
        assert!(ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 2, 2, 1, 4, false
        ));
        assert!(ingress_checkpoint_due_with_policy(
            1024, 1, 4096, 8, 0, 2, 4, 4, false
        ));
    }

    #[test]
    fn histogram_percentile_marks_overflow_bucket_as_saturated() {
        let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
            bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
            bucket_counts: vec![1, 1, 3],
        };
        let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.99);
        assert_eq!(value_ns, 2_000_000);
        assert!(saturated);
    }

    #[test]
    fn histogram_percentile_non_overflow_bucket_is_not_saturated() {
        let snapshot = studiofs_cache::metrics::LatencyHistogramSnapshot {
            bucket_upper_bounds_ns: vec![1_000_000, 2_000_000],
            bucket_counts: vec![1, 2, 0],
        };
        let (value_ns, saturated) = StudioVfs::histogram_percentile_ns(&snapshot, 0.95);
        assert_eq!(value_ns, 2_000_000);
        assert!(!saturated);
    }

    #[test]
    fn build_cadence_stats_averages_over_completed_intervals() {
        let histogram = studiofs_cache::metrics::LatencyHistogramSnapshot {
            bucket_upper_bounds_ns: vec![10_000_000, 20_000_000],
            bucket_counts: vec![1, 0, 0],
        };

        let stats = StudioVfs::build_cadence_stats(
            3,
            10_000_000,
            10_000_000,
            histogram,
            Duration::from_secs(1),
        );

        assert_eq!(
            stats.avg_interval_ms, 5.0,
            "cadence averages should divide by completed intervals (events - 1), not raw histogram sample count"
        );
    }
}
