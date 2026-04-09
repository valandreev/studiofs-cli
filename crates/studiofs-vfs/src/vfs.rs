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
//! 3. Persist full dirty blocks locally via the raw backend `put(...)`
//! 4. Apply local `sync_data()` barriers (strict for fsync / durable mode,
//!    grouped in fast mode) and persist extent metadata / attrs
//! 5. Queue committed extents for async upload + remote publish
//!
//! Read Path:
//! 1. Check the `WriteBuffer` overlay for newer local bytes
//! 2. Resolve local metadata refs to committed local extents in the local backend
//! 3. Fall back to remote fetch
//! 4. Validate object payload + checksum and assemble response

#[path = "commit.rs"]
mod commit;
#[path = "write_buffer.rs"]
mod write_buffer;

use crate::access::{
    AccessClass, AccessClassifierConfig, AccessDecision, AccessPolicy, HandleAccessState,
};
use crate::incarnation::{
    CommittedIncarnation, InodeIncarnationState, PendingIncarnation, VfsUploadState,
};
use crate::qos::{FairInodeScheduler, QosController, TransitMemoryBudget};
use crate::readahead::{DirAccessTracker, ReadAheadTracker, ReadStreamKey};

use crate::runtime::{TimeoutOrCancel, run_blocking, run_meta, run_with_timeout_or_cancel};
use crate::vfs::write_buffer::WriteBufferState;
use bytes::{Buf, Bytes, BytesMut};
use futures::{FutureExt, future::join_all};
use opendal::options::WriteOptions;
use opendal::{Buffer, EntryMode, ErrorKind, Operator};
use parking_lot::{Condvar, Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use studiofs_cache::block_cache::{BlockCache, BlockCacheConfig, L2IoPriority};
use studiofs_cache::metrics::{LatencyHistogram, LatencyHistogramSnapshot, MaterializeBatchStats};
use studiofs_cache::upload_queue::PendingUpload;
use studiofs_cache::v2::LocalBlockBackend;
use studiofs_cache::v3::{CacheBackendKind, cache_v3_default_gate_status};
use studiofs_cache::{
    LocalBackendSelectionMode, OpenLocalBlockBackendError,
    open_local_block_backend_with_budget_and_mode,
};
use studiofs_data::object::{
    CLOUD_FRAGMENT_COUNT, CLOUD_FRAGMENT_SIZE, create_object_buffer, format_object_fragment_keys,
    parse_object_id_from_key,
};
use studiofs_data::store::{DataError, DataStore, DataStoreConfig, IoProfile};

use studiofs_meta::schema::{
    DEFAULT_CHUNK_SIZE, DirEntry, ROOT_INO, ResolvedSliceSource, SetattrArgs,
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

/// Filesystem statistics returned by `statfs()`.
#[derive(Debug, Clone)]
pub struct StatFs {
    /// Total number of blocks (virtual).
    pub blocks: u64,
    /// Free blocks.
    pub bfree: u64,
    /// Available blocks (same as bfree for single-user).
    pub bavail: u64,
    /// Number of inodes (files + dirs).
    pub files: u64,
    /// Free inode slots.
    pub ffree: u64,
    /// Block size.
    pub bsize: u32,
    /// Maximum filename length.
    pub namelen: u32,
    /// Fragment size.
    pub frsize: u32,
}

/// Snapshot of local write-buffer / legacy-ingress state used for observability
/// and backpressure diagnostics.
#[derive(Debug, Clone, Default)]
pub struct VfsIngressStats {
    /// Accepted bytes currently buffered in RAM by the active `WriteBuffer`
    /// path (legacy field name retained for compatibility).
    pub dirty_mem_bytes: usize,
    /// Accepted block count currently buffered in RAM by the active
    /// `WriteBuffer` path (legacy field name retained for compatibility).
    pub dirty_mem_blocks: usize,
    /// Legacy ingress spill bytes currently present on local NVMe.
    /// Expected to remain zero on the Cache v2 foreground path.
    pub spill_bytes: usize,
    /// Legacy ingress spill records currently present on disk.
    /// Expected to remain zero on the Cache v2 foreground path.
    pub spill_records: usize,
    /// Age of the oldest spill record in milliseconds.
    pub oldest_spill_age_ms: u64,
    /// Cumulative milliseconds spent waiting on local backpressure in the
    /// foreground path.
    pub write_backpressure_wait_ms: u64,
    /// Number of times foreground writes had to wait due to local backpressure.
    pub write_backpressure_events: u64,
    /// Foreground wait time spent in fast-mode backpressure only.
    pub fast_backpressure_wait_ms: u64,
    /// Number of fast-mode backpressure wait events.
    pub fast_backpressure_events: u64,
    /// Maximum single fast-mode backpressure stall observed since startup.
    pub fast_backpressure_stall_max_ms: u64,
    /// Legacy counter for blocks that were forced to spill because RAM
    /// acceptance was over budget. Expected to remain zero on the Cache v2
    /// foreground path.
    pub forced_spill_blocks: u64,
    /// Total blocks drained from RAM accepted state into committed local
    /// metadata / cache state.
    pub drained_mem_blocks: u64,
    /// Total bytes drained from RAM accepted state into committed local
    /// metadata / cache state.
    pub drained_mem_bytes: u64,
    /// Total blocks drained from legacy spill accepted state into committed
    /// local metadata / cache state.
    pub drained_spill_blocks: u64,
    /// Total bytes drained from legacy spill accepted state into committed
    /// local metadata / cache state.
    pub drained_spill_bytes: u64,
    /// Current depth of the prefetch worker's pending queue.
    pub prefetch_queue_depth: u64,
    /// Successful ingress checkpoint persists since startup.
    pub checkpoint_persists: u64,
    /// Failed ingress checkpoint persist attempts since startup.
    pub checkpoint_failures: u64,
    /// Age of the last successful ingress checkpoint in milliseconds.
    pub last_checkpoint_age_ms: u64,
    /// Covered segment id of the last successful ingress checkpoint.
    pub last_checkpoint_segment_id: u64,
    /// Covered offset within the covered segment of the last successful checkpoint.
    pub last_checkpoint_offset: u64,
    /// Uncheckpointed ingress record bytes since the last successful checkpoint.
    pub checkpoint_pending_bytes: u64,
    /// Uncheckpointed ingress record mutations since the last successful checkpoint.
    pub checkpoint_pending_mutations: u64,
    /// Active-segment lag relative to the last successful checkpoint.
    pub checkpoint_active_segment_lag: u64,
    /// Number of distinct segment ids still referenced by the live ingress index.
    pub checkpoint_live_segments: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesMs {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathLatencyQuantilesSaturated {
    pub p50: bool,
    pub p95: bool,
    pub p99: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsLatencyHistogramSnapshot {
    pub bucket_upper_bounds_ns: Vec<u64>,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathSyncDataStats {
    pub calls: u64,
    pub calls_per_sec: f64,
    pub deferred_calls: u64,
    pub synced_bytes: u64,
    pub synced_gib: f64,
    pub calls_per_gib: f64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
    pub latency_quantiles_ms: VfsWritePathLatencyQuantilesMs,
    #[serde(default)]
    pub latency_quantiles_saturated: VfsWritePathLatencyQuantilesSaturated,
    pub latency_histogram: VfsLatencyHistogramSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathWinfspCallbackStats {
    pub write: u64,
    pub flush: u64,
    pub cleanup_flush_fh: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathBackpressureStats {
    pub events: u64,
    pub wait_total_ms: u64,
    pub stall_max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct VfsWritePathObservabilitySnapshot {
    pub generated_unix_ms: u64,
    pub uptime_secs: f64,
    pub backend_tag: String,
    pub flush_ordered_calls: u64,
    pub flush_ordered_calls_per_sec: f64,
    pub block_store_sync_data: VfsWritePathSyncDataStats,
    pub winfsp_callbacks: VfsWritePathWinfspCallbackStats,
    pub write_backpressure: VfsWritePathBackpressureStats,
}

/// Compatibility-shaped acceptance decision for a prospective write batch.
/// The active Cache v2 foreground path uses `DirtyMem` and `Backpressure`;
/// `Spill` is retained for legacy observability compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VfsIngressLane {
    DirtyMem,
    Spill,
    Backpressure,
}

/// Snapshot of the current ingress pressure surface for a given payload size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VfsIngressPressure {
    pub next_bytes: usize,
    pub dirty_mem_bytes: usize,
    pub dirty_mem_limit: usize,
    pub spill_bytes: usize,
    pub spill_limit: usize,
    pub lane: VfsIngressLane,
}

#[derive(Debug, Clone, Default)]
pub struct VfsUploadStats {
    pub pending_uploads: usize,
    pub upload_requeues: u64,
    pub canceled_deleted: u64,
    pub canceled_stale: u64,
}

#[derive(Debug, Clone, Default)]
pub struct VfsFileSessionStats {
    pub active_sessions: usize,
    pub open_handles: u64,
    pub queued_ops: u64,
    pub max_queue_depth: u64,
    pub wait_events: u64,
    pub wait_ms: u64,
    pub transitions: u64,
    pub wait_reasons: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Default)]
pub struct VfsForegroundReadProtectionStats {
    pub p90_latency_ns: Option<u64>,
    pub throughput_bytes_per_sec: f64,
    pub speedometer_samples: usize,
    pub window_bytes: u64,
    pub window_coverage_ms: u64,
    pub dominant_access_class: Option<String>,
    pub dominant_prefetch_enabled: bool,
    pub dominant_cache_promotion_on_read: bool,
    pub dominant_readahead_base_width: u32,
    pub dominant_readahead_max_width: u32,
    pub dominant_readahead_min_prefetch: u32,
    pub dominant_upload_pressure_boost: u8,
    pub active_sequential_media_reads: u64,
    pub active_random_interactive_reads: u64,
    pub active_sequential_writers: u64,
    pub active_random_writers: u64,
    pub upload_fallback_deferrals: u64,
    pub upload_fallback_rereads: u64,
    pub upload_fallback_reread_bytes: u64,
    pub upload_fallback_reread_cache_misses: u64,
    pub upload_fallback_wait_ns: u64,
    pub upload_fallback_wait_max_ns: u64,
    pub upload_fallback_read_ns: u64,
    pub upload_fallback_read_max_ns: u64,
    pub upload_cache_io_foreground_cost: u32,
    pub upload_scheduler_penalty_ms: u64,
}

pub type VfsResult<T> = Result<T, VfsError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NamespacePathStyle {
    Posix,
    Windows,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceParent {
    pub parent: Ino,
    pub name: Vec<u8>,
}

pub struct NamespaceResolver<'a> {
    vfs: &'a StudioVfs,
    style: NamespacePathStyle,
}

impl<'a> NamespaceResolver<'a> {
    fn new(vfs: &'a StudioVfs, style: NamespacePathStyle) -> Self {
        Self { vfs, style }
    }

    fn path_components(&self, path: &str) -> Vec<String> {
        let stripped = match self.style {
            NamespacePathStyle::Posix => path.trim_start_matches('/'),
            NamespacePathStyle::Windows => path.trim_start_matches('\\'),
        };

        let mut components: Vec<String> = Vec::new();
        let parts: Box<dyn Iterator<Item = &str>> = match self.style {
            NamespacePathStyle::Posix => Box::new(stripped.split('/')),
            NamespacePathStyle::Windows => Box::new(stripped.split('\\')),
        };

        for raw in parts {
            if raw.is_empty() || raw == "." {
                continue;
            }
            if raw == ".." {
                components.pop();
                continue;
            }
            let normalized = match self.style {
                NamespacePathStyle::Posix => raw,
                NamespacePathStyle::Windows => raw.trim_end_matches(['.', ' ']),
            };
            if !normalized.is_empty() {
                components.push(normalized.to_string());
            }
        }
        components
    }

    fn path_not_found(path: &str) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent: ROOT_INO,
            name: path.to_string(),
        })
    }

    fn component_not_found(parent: Ino, name: &[u8]) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    fn is_entry_not_found(err: &VfsError) -> bool {
        matches!(err, VfsError::Meta(MetaError::EntryNotFound { .. }))
    }

    fn resolve_components_cached(&self, components: &[String]) -> VfsResult<InodeAttr> {
        if components.is_empty() {
            return self.vfs.getattr(ROOT_INO);
        }

        let cache_key = PathShortcutCacheKey::from_components(self.style, components);
        let do_resolve = || -> VfsResult<InodeAttr> {
            let mut current = ROOT_INO;
            let mut attr = self.vfs.getattr(ROOT_INO)?;
            let mut parent_tokens = Vec::with_capacity(components.len());
            for component in components {
                parent_tokens.push(PathShortcutParentToken {
                    parent: current,
                    generation: self.vfs.directory_generation(current),
                });
                match self.lookup_component(current, component.as_bytes()) {
                    Ok(next_attr) => {
                        current = next_attr.ino;
                        attr = next_attr;
                    }
                    Err(err) if Self::is_entry_not_found(&err) => {
                        if let Some(key) = cache_key.clone() {
                            self.vfs.path_shortcut_cache_insert_negative_by_key(
                                key,
                                current,
                                component.as_bytes(),
                                parent_tokens,
                            );
                        }
                        return Err(err);
                    }
                    Err(err) => return Err(err),
                }
            }
            if let Some(key) = cache_key.clone() {
                self.vfs
                    .path_shortcut_cache_insert_positive_by_key(key, attr.ino, parent_tokens);
            }
            Ok(attr)
        };

        if let Some(key) = &cache_key
            && let Some(cached) = self.vfs.path_shortcut_cache_lookup_by_key(key)
        {
            return match cached {
                PathShortcutCacheValue::Positive(ino) => match self.vfs.getattr(ino) {
                    Ok(attr) => Ok(attr),
                    Err(VfsError::Meta(MetaError::InodeNotFound(_))) => do_resolve(),
                    Err(err) => Err(err),
                },
                PathShortcutCacheValue::Negative { parent, name } => {
                    Err(Self::component_not_found(parent, name.as_ref()))
                }
            };
        }

        do_resolve()
    }

    pub fn lookup_component(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.vfs.lookup_component_impl(parent, name)
    }

    pub fn resolve_path(&self, path: &str) -> VfsResult<InodeAttr> {
        let components = self.path_components(path);
        self.resolve_components_cached(&components)
    }

    pub fn resolve_parent(&self, path: &str) -> VfsResult<NamespaceParent> {
        let components = self.path_components(path);
        if components.is_empty() {
            return Err(Self::path_not_found(path));
        }

        let current = if components.len() == 1 {
            ROOT_INO
        } else {
            self.resolve_components_cached(&components[..components.len() - 1])?
                .ino
        };

        Ok(NamespaceParent {
            parent: current,
            name: components.last().unwrap().as_bytes().to_vec(),
        })
    }

    pub fn path_for_inode(&self, ino: Ino) -> Option<String> {
        if ino == ROOT_INO {
            return Some(match self.style {
                NamespacePathStyle::Posix => "/".to_string(),
                NamespacePathStyle::Windows => "\\".to_string(),
            });
        }

        let mut components: Vec<String> = Vec::new();
        let mut current = ino;
        for _ in 0..256 {
            let attr = self.vfs.getattr(current).ok()?;
            let parent = attr.parent;
            if parent == 0 || current == ROOT_INO {
                break;
            }

            let entries = self.vfs.readdir(parent).ok()?;
            let name = entries
                .iter()
                .find(|entry| entry.ino == current)
                .map(|entry| String::from_utf8_lossy(&entry.name).into_owned())?;
            components.push(name);
            current = parent;
        }

        if current != ROOT_INO {
            return None;
        }

        components.reverse();
        let separator = match self.style {
            NamespacePathStyle::Posix => "/",
            NamespacePathStyle::Windows => "\\",
        };
        Some(format!("{}{}", separator, components.join(separator)))
    }
}

// =============================================================================
// Kernel Cache Invalidation
// =============================================================================

/// Callback trait for notifying the platform filesystem driver (FUSE / WinFSP)
/// to drop cached metadata so that userspace immediately sees remote changes.
///
/// Implemented by platform bridges and registered on the VFS via
/// [`StudioVfs::set_kernel_invalidator`] after mount.
pub trait KernelInvalidator: Send + Sync {
    /// Invalidate cached attributes and directory listing for a single inode.
    /// For directories this forces the kernel to re-issue `readdir`.
    fn inval_inode(&self, ino: u64);
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

fn is_probable_trash_component(name: &[u8]) -> bool {
    matches!(name, b".Trash" | b".Trashes") || name.starts_with(b".Trash-")
}

fn is_ignored_macos_metadata_name(name: &[u8]) -> bool {
    name == b".DS_Store" || (name.len() > 2 && name.starts_with(b"._"))
}

fn is_blocked_root_macos_metadata_dir(name: &[u8]) -> bool {
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
const CACHE_RUNTIME_MAX_DIRTY_BYTES_LARGE: usize = 4 * 1024 * 1024 * 1024;
const CACHE_RUNTIME_MAX_DIRTY_BYTES_HUGE: usize = 8 * 1024 * 1024 * 1024;
const CACHE_RUNTIME_MIN_WRITE_EAGER_BATCH_BYTES: usize = 16 * 1024 * 1024;
const CACHE_RUNTIME_MAX_WRITE_EAGER_BATCH_BYTES: usize = 128 * 1024 * 1024;
const FAST_MODE_SYNC_GROUP_MIN_BYTES_FLOOR: u64 = 32 * 1024 * 1024;
const FAST_MODE_SYNC_GROUP_MIN_BYTES_CEIL: u64 = 256 * 1024 * 1024;
const FAST_MODE_SYNC_GROUP_MAX_DELAY_MS: u64 = 25;
const CACHE_RUNTIME_MIN_INGRESS_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const CACHE_RUNTIME_MAX_INGRESS_SEGMENT_BYTES: u64 = 512 * 1024 * 1024;
const CACHE_RUNTIME_MIN_TRANSIT_BUDGET_BYTES: usize = 512 * 1024 * 1024;
const CACHE_RUNTIME_MAX_TRANSIT_BUDGET_BYTES: usize = 4 * 1024 * 1024 * 1024;
const CACHE_RUNTIME_SMALL_BUDGET_BYTES: u64 = 128 * 1024 * 1024 * 1024;
const CACHE_RUNTIME_LARGE_BUDGET_BYTES: u64 = 512 * 1024 * 1024 * 1024;
const CACHE_RUNTIME_HUGE_BUDGET_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

#[derive(Clone)]
struct DistributedFlock {
    op: Operator,
    prefix: String,
    client_id: String,
    lease_ttl: Duration,
    retry_base_ms: u64,
    retry_max_ms: u64,
}

#[derive(Debug, Clone, Default)]
struct RemoteLockRecord {
    lease_until_unix: i64,
    exclusive_owner: Option<String>,
    shared_owners: BTreeSet<String>,
}

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
enum LocalSyncTrigger {
    Explicit,
    DurableMode,
    BytesThreshold,
    TimeWindow,
}

fn local_sync_trigger_for_write_path(
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
struct CacheRuntimeTuningOverrides {
    max_dirty_bytes: bool,
    write_eager_flush_batch_bytes: bool,
    max_ingress_segment_bytes: bool,
    ingress_checkpoint_interval_secs: bool,
    ingress_checkpoint_min_append_bytes: bool,
    ingress_checkpoint_min_mutations: bool,
    ingress_checkpoint_max_segment_lag: bool,
    ingress_checkpoint_max_live_segments: bool,
    upload_cache_io_slots: bool,
    transit_memory_budget_bytes: bool,
}

impl VfsConfigValidated {
    pub fn into_inner(self) -> VfsConfig {
        self.config
    }

    fn cache_runtime_overrides(&self) -> CacheRuntimeTuningOverrides {
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
        normalize_and_validate_config(
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
        normalize_and_validate_config(cfg, overrides)
    }
}

fn normalize_and_validate_config(
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

// =============================================================================
// Open File Handle Tracking
// =============================================================================

/// Flags associated with an open file descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags(u32);

impl OpenFlags {
    pub const RDONLY: Self = Self(0x01);
    pub const WRONLY: Self = Self(0x02);
    pub const RDWR: Self = Self(0x04);
    pub const APPEND: Self = Self(0x08);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn bits(self) -> u32 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl std::ops::BitOr for OpenFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

/// Information about a single open file descriptor.
#[derive(Clone, Debug)]
pub struct OpenFile {
    pub fh: u64,
    pub ino: Ino,
    pub flags: OpenFlags,
    access_state: Arc<Mutex<HandleAccessState>>,
}

impl OpenFile {
    fn new(fh: u64, ino: Ino, flags: OpenFlags) -> Self {
        Self {
            fh,
            ino,
            flags,
            access_state: Arc::new(Mutex::new(HandleAccessState::new(fh, flags))),
        }
    }

    fn stream_id(&self) -> u64 {
        self.access_state.lock().stream_id()
    }

    fn current_access_class(&self) -> AccessClass {
        self.access_state.lock().current_class()
    }

    pub fn access_class_name(&self) -> &'static str {
        self.current_access_class().as_str()
    }

    fn observe_read(&self, offset: u64, len: usize, cfg: AccessClassifierConfig) -> AccessDecision {
        self.access_state.lock().observe_read(offset, len, cfg)
    }

    fn observe_write(
        &self,
        offset: u64,
        len: usize,
        cfg: AccessClassifierConfig,
    ) -> AccessDecision {
        self.access_state.lock().observe_write(offset, len, cfg)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileSessionOperation {
    Read,
    Write,
    Flush,
    Fsync,
    Release,
    Materialize,
    Upload,
    Finalize,
}

impl FileSessionOperation {
    const COUNT: usize = 8;

    fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Flush => "flush",
            Self::Fsync => "fsync",
            Self::Release => "release",
            Self::Materialize => "materialize",
            Self::Upload => "upload",
            Self::Finalize => "finalize",
        }
    }

    fn read_lock(self) -> bool {
        matches!(self, Self::Read)
    }

    fn index(self) -> usize {
        match self {
            Self::Read => 0,
            Self::Write => 1,
            Self::Flush => 2,
            Self::Fsync => 3,
            Self::Release => 4,
            Self::Materialize => 5,
            Self::Upload => 6,
            Self::Finalize => 7,
        }
    }

    fn from_index(idx: usize) -> Option<Self> {
        match idx {
            0 => Some(Self::Read),
            1 => Some(Self::Write),
            2 => Some(Self::Flush),
            3 => Some(Self::Fsync),
            4 => Some(Self::Release),
            5 => Some(Self::Materialize),
            6 => Some(Self::Upload),
            7 => Some(Self::Finalize),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct FileSessionEntry {
    gate: Arc<tokio::sync::RwLock<()>>,
    open_handles: AtomicU32,
    queued_ops: AtomicU64,
    max_queue_depth: AtomicU64,
    wait_events: AtomicU64,
    wait_ms: AtomicU64,
    transitions: AtomicU64,
    last_transition_unix_ms: AtomicU64,
    last_transition_label: Mutex<String>,
}

impl FileSessionEntry {
    fn new() -> Self {
        Self {
            gate: Arc::new(tokio::sync::RwLock::new(())),
            open_handles: AtomicU32::new(0),
            queued_ops: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            wait_events: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            transitions: AtomicU64::new(0),
            last_transition_unix_ms: AtomicU64::new(0),
            last_transition_label: Mutex::new(String::new()),
        }
    }

    fn record_transition(&self, op: FileSessionOperation, phase: &'static str) {
        self.transitions.fetch_add(1, Ordering::Relaxed);
        self.last_transition_unix_ms
            .store(StudioVfs::now_unix_millis(), Ordering::Relaxed);
        *self.last_transition_label.lock() = format!("{}:{phase}", op.as_str());
    }

    fn update_max_queue_depth(&self, candidate: u64) {
        let _ =
            self.max_queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (candidate > current).then_some(candidate)
                });
    }
}

#[derive(Debug)]
struct FileSessionPermit {
    _read_gate: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    _write_gate: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
    counters: Arc<FileSessionCounters>,
    session: Arc<FileSessionEntry>,
    op: FileSessionOperation,
}

impl Drop for FileSessionPermit {
    fn drop(&mut self) {
        // Release session lock(s) before publishing "finish" so waiters can
        // observe lower tail latency on contended inodes.
        drop(self._read_gate.take());
        drop(self._write_gate.take());
        self.counters.transitions.fetch_add(1, Ordering::Relaxed);
        self.session.record_transition(self.op, "finish");
    }
}

#[derive(Debug)]
struct FileSessionCounters {
    active_sessions: AtomicU64,
    open_handles: AtomicU64,
    queued_ops: AtomicU64,
    max_queue_depth: AtomicU64,
    wait_events: AtomicU64,
    wait_ms: AtomicU64,
    transitions: AtomicU64,
    wait_reasons: [AtomicU64; FileSessionOperation::COUNT],
}

impl FileSessionCounters {
    fn new() -> Self {
        Self {
            active_sessions: AtomicU64::new(0),
            open_handles: AtomicU64::new(0),
            queued_ops: AtomicU64::new(0),
            max_queue_depth: AtomicU64::new(0),
            wait_events: AtomicU64::new(0),
            wait_ms: AtomicU64::new(0),
            transitions: AtomicU64::new(0),
            wait_reasons: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn note_wait_reason(&self, op: FileSessionOperation) {
        self.wait_reasons[op.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn note_transition(&self) {
        self.transitions.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct FileSessionState {
    sessions: Mutex<HashMap<Ino, Weak<FileSessionEntry>>>,
    counters: Arc<FileSessionCounters>,
}

impl FileSessionState {
    fn new() -> Self {
        Self {
            sessions: Mutex::new(HashMap::new()),
            counters: Arc::new(FileSessionCounters::new()),
        }
    }

    fn session(&self, ino: Ino) -> Arc<FileSessionEntry> {
        let mut sessions = self.sessions.lock();
        if let Some(existing) = sessions.get(&ino).and_then(Weak::upgrade) {
            return existing;
        }
        if sessions.get(&ino).is_some() {
            sessions.remove(&ino);
            self.counters
                .active_sessions
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }

        let session = Arc::new(FileSessionEntry::new());
        sessions.insert(ino, Arc::downgrade(&session));
        self.counters
            .active_sessions
            .fetch_add(1, Ordering::Relaxed);
        session
    }
}

#[derive(Debug, Clone)]
struct IgnoredEntry {
    attr: InodeAttr,
    parent: Ino,
    name: Vec<u8>,
}

#[derive(Debug, Default)]
struct IgnoredEntryRegistry {
    by_ino: HashMap<Ino, IgnoredEntry>,
    by_name: HashMap<(Ino, Vec<u8>), Ino>,
}

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DentryCacheKey {
    parent: Ino,
    name: Arc<[u8]>,
}

impl DentryCacheKey {
    fn new(parent: Ino, name: &[u8]) -> Self {
        Self {
            parent,
            name: Arc::from(name),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DentryCacheValue {
    Positive(Ino),
    Negative,
}

#[derive(Debug, Clone)]
struct DentryCacheMeta {
    namespace_epoch: u64,
    parent_generation: u64,
}

#[derive(Debug, Clone, Copy)]
struct NamespaceCacheToken {
    namespace_epoch: u64,
    parent_generation: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct DentryCacheMetrics {
    hits: u64,
    protected_hits: u64,
    misses: u64,
    negative_hits: u64,
    evictions: u64,
    occupancy_entries: usize,
    occupancy_bytes: usize,
    protected_occupancy_entries: usize,
    protected_occupancy_bytes: usize,
    entry_budget: usize,
    byte_budget: usize,
}

impl DentryCacheMetrics {
    fn negative_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.negative_hits as f64 / self.hits as f64
    }

    fn hot_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.protected_hits as f64 / self.hits as f64
    }
}

#[derive(Debug, Default)]
struct DentryCachePolicy {
    parent_index: HashMap<Ino, HashSet<DentryCacheKey>>,
}

impl DentryCachePolicy {
    fn estimate_entry_charge_for_name(name_len: usize) -> usize {
        std::mem::size_of::<DentryCacheKey>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<DentryCacheValue, DentryCacheMeta>,
            >())
            .saturating_add(name_len)
            // Dentry cache additionally maintains `parent_index` (parent -> key set),
            // so the per-entry index overhead is intentionally counted twice here.
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD * 2)
    }

    fn take_parent_keys(&mut self, parent: Ino) -> Option<HashSet<DentryCacheKey>> {
        self.parent_index.remove(&parent)
    }
}

impl CachePolicy<DentryCacheKey, DentryCacheValue> for DentryCachePolicy {
    type Meta = DentryCacheMeta;
    type InsertContext = NamespaceCacheToken;
    type LookupContext<'a> = NamespaceCacheToken;

    fn make_meta(
        &mut self,
        _key: &DentryCacheKey,
        _value: &DentryCacheValue,
        ctx: Self::InsertContext,
    ) -> Self::Meta {
        DentryCacheMeta {
            namespace_epoch: ctx.namespace_epoch,
            parent_generation: ctx.parent_generation,
        }
    }

    fn is_stale(
        &self,
        _key: &DentryCacheKey,
        _value: &DentryCacheValue,
        meta: &Self::Meta,
        ctx: Self::LookupContext<'_>,
    ) -> bool {
        meta.namespace_epoch != ctx.namespace_epoch
            || meta.parent_generation != ctx.parent_generation
    }

    fn estimate_entry_charge(
        &self,
        key: &DentryCacheKey,
        _value: &DentryCacheValue,
        _meta: &Self::Meta,
    ) -> usize {
        Self::estimate_entry_charge_for_name(key.name.len())
    }

    fn is_negative(&self, value: &DentryCacheValue) -> bool {
        matches!(value, DentryCacheValue::Negative)
    }

    fn on_insert(&mut self, key: &DentryCacheKey, _value: &DentryCacheValue, _meta: &Self::Meta) {
        self.parent_index
            .entry(key.parent)
            .or_default()
            .insert(key.clone());
    }

    fn on_remove(&mut self, key: &DentryCacheKey, _value: &DentryCacheValue, _meta: &Self::Meta) {
        if let Some(keys) = self.parent_index.get_mut(&key.parent) {
            keys.remove(key);
            if keys.is_empty() {
                self.parent_index.remove(&key.parent);
            }
        }
    }
}

#[derive(Debug)]
struct DentryCache {
    inner: TwoTierCache<DentryCacheKey, DentryCacheValue, DentryCachePolicy>,
}

impl DentryCache {
    fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            inner: TwoTierCache::new(entry_budget, byte_budget, DentryCachePolicy::default()),
        }
    }

    #[cfg(test)]
    fn estimate_entry_charge(name: &[u8]) -> usize {
        DentryCachePolicy::estimate_entry_charge_for_name(name.len())
    }

    fn metrics(&self) -> DentryCacheMetrics {
        let metrics = self.inner.metrics();
        DentryCacheMetrics {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            negative_hits: metrics.negative_hits,
            evictions: metrics.evictions,
            occupancy_entries: self.inner.entry_count(),
            occupancy_bytes: self.inner.used_bytes(),
            protected_occupancy_entries: self.inner.protected_entries(),
            protected_occupancy_bytes: self.inner.protected_bytes(),
            entry_budget: self.inner.entry_budget,
            byte_budget: self.inner.byte_budget,
        }
    }

    fn get(
        &mut self,
        parent: Ino,
        name: &[u8],
        namespace_epoch: u64,
        parent_generation: u64,
    ) -> Option<DentryCacheValue> {
        let key = DentryCacheKey::new(parent, name);
        self.inner.get(
            &key,
            NamespaceCacheToken {
                namespace_epoch,
                parent_generation,
            },
        )
    }

    fn put(
        &mut self,
        parent: Ino,
        name: &[u8],
        value: DentryCacheValue,
        namespace_epoch: u64,
        parent_generation: u64,
    ) {
        let key = DentryCacheKey::new(parent, name);
        self.inner.put(
            key,
            value,
            NamespaceCacheToken {
                namespace_epoch,
                parent_generation,
            },
        );
    }

    fn invalidate(&mut self, parent: Ino, name: &[u8]) {
        let key = DentryCacheKey::new(parent, name);
        self.inner.remove(&key);
    }

    fn invalidate_parent(&mut self, parent: Ino) {
        let Some(keys) = self.inner.policy_mut().take_parent_keys(parent) else {
            return;
        };
        for key in keys {
            self.inner.remove(&key);
        }
    }

    #[cfg(test)]
    fn entry_value(&self, key: &DentryCacheKey) -> Option<DentryCacheValue> {
        self.inner.peek(key).map(|entry| entry.value)
    }

    #[cfg(test)]
    fn entry_len(&self) -> usize {
        self.inner.entry_count()
    }

    #[cfg(test)]
    fn protected_order_len(&self) -> usize {
        self.inner.protected_order_len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PathShortcutCacheKey {
    style: NamespacePathStyle,
    path: Arc<[u8]>,
}

impl PathShortcutCacheKey {
    fn from_components(style: NamespacePathStyle, components: &[String]) -> Option<Self> {
        if components.is_empty() {
            return None;
        }
        let estimated = components
            .iter()
            .map(|component| component.len())
            .sum::<usize>()
            .saturating_add(components.len().saturating_sub(1));
        let mut encoded = Vec::with_capacity(estimated);
        for (idx, component) in components.iter().enumerate() {
            if idx > 0 {
                encoded.push(0);
            }
            encoded.extend_from_slice(component.as_bytes());
        }
        Some(Self {
            style,
            path: Arc::from(encoded.into_boxed_slice()),
        })
    }
}

#[derive(Debug, Clone)]
enum PathShortcutCacheValue {
    Positive(Ino),
    Negative { parent: Ino, name: Arc<[u8]> },
}

#[derive(Debug, Clone)]
struct PathShortcutParentToken {
    parent: Ino,
    generation: u64,
}

#[derive(Debug, Clone)]
struct PathShortcutCacheMeta {
    namespace_epoch: u64,
    parent_tokens: Arc<[PathShortcutParentToken]>,
}

struct PathShortcutLookupCtx<'a> {
    namespace_epoch: u64,
    generation_of: &'a mut dyn FnMut(Ino) -> u64,
}

struct PathShortcutInsertCtx {
    namespace_epoch: u64,
    parent_tokens: Arc<[PathShortcutParentToken]>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PathShortcutCacheMetrics {
    hits: u64,
    protected_hits: u64,
    misses: u64,
    negative_hits: u64,
    evictions: u64,
    occupancy_entries: usize,
    occupancy_bytes: usize,
    protected_occupancy_entries: usize,
    protected_occupancy_bytes: usize,
    entry_budget: usize,
    byte_budget: usize,
}

impl PathShortcutCacheMetrics {
    fn negative_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.negative_hits as f64 / self.hits as f64
    }

    fn hot_hit_ratio(self) -> f64 {
        if self.hits == 0 {
            return 0.0;
        }
        self.protected_hits as f64 / self.hits as f64
    }
}

#[derive(Debug)]
struct PathShortcutCachePolicy;

impl PathShortcutCachePolicy {
    fn estimate_entry_charge_for(
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        parent_tokens: &[PathShortcutParentToken],
    ) -> usize {
        let negative_name_len = match value {
            PathShortcutCacheValue::Positive(_) => 0,
            PathShortcutCacheValue::Negative { name, .. } => name.len(),
        };
        std::mem::size_of::<PathShortcutCacheKey>()
            .saturating_add(std::mem::size_of::<
                TwoTierEntry<PathShortcutCacheValue, PathShortcutCacheMeta>,
            >())
            .saturating_add(key.path.len())
            .saturating_add(negative_name_len)
            .saturating_add(
                parent_tokens
                    .len()
                    .saturating_mul(std::mem::size_of::<PathShortcutParentToken>()),
            )
            .saturating_add(DENTRY_CACHE_INDEX_NODE_OVERHEAD)
    }
}

impl Default for PathShortcutCachePolicy {
    fn default() -> Self {
        Self
    }
}

impl CachePolicy<PathShortcutCacheKey, PathShortcutCacheValue> for PathShortcutCachePolicy {
    type Meta = PathShortcutCacheMeta;
    type InsertContext = PathShortcutInsertCtx;
    type LookupContext<'a> = PathShortcutLookupCtx<'a>;

    fn make_meta(
        &mut self,
        _key: &PathShortcutCacheKey,
        _value: &PathShortcutCacheValue,
        ctx: Self::InsertContext,
    ) -> Self::Meta {
        PathShortcutCacheMeta {
            namespace_epoch: ctx.namespace_epoch,
            parent_tokens: ctx.parent_tokens,
        }
    }

    fn is_stale(
        &self,
        _key: &PathShortcutCacheKey,
        _value: &PathShortcutCacheValue,
        meta: &Self::Meta,
        ctx: Self::LookupContext<'_>,
    ) -> bool {
        meta.namespace_epoch != ctx.namespace_epoch
            || meta
                .parent_tokens
                .iter()
                .any(|token| (ctx.generation_of)(token.parent) != token.generation)
    }

    fn estimate_entry_charge(
        &self,
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        meta: &Self::Meta,
    ) -> usize {
        Self::estimate_entry_charge_for(key, value, &meta.parent_tokens)
    }

    fn is_negative(&self, value: &PathShortcutCacheValue) -> bool {
        matches!(value, PathShortcutCacheValue::Negative { .. })
    }
}

#[derive(Debug)]
struct PathShortcutCache {
    inner: TwoTierCache<PathShortcutCacheKey, PathShortcutCacheValue, PathShortcutCachePolicy>,
}

impl PathShortcutCache {
    fn new(entry_budget: usize, byte_budget: usize) -> Self {
        Self {
            inner: TwoTierCache::new(entry_budget, byte_budget, PathShortcutCachePolicy),
        }
    }

    #[cfg(test)]
    fn estimate_entry_charge(
        key: &PathShortcutCacheKey,
        value: &PathShortcutCacheValue,
        parent_tokens: &[PathShortcutParentToken],
    ) -> usize {
        PathShortcutCachePolicy::estimate_entry_charge_for(key, value, parent_tokens)
    }

    fn metrics(&self) -> PathShortcutCacheMetrics {
        let metrics = self.inner.metrics();
        PathShortcutCacheMetrics {
            hits: metrics.hits,
            protected_hits: metrics.protected_hits,
            misses: metrics.misses,
            negative_hits: metrics.negative_hits,
            evictions: metrics.evictions,
            occupancy_entries: self.inner.entry_count(),
            occupancy_bytes: self.inner.used_bytes(),
            protected_occupancy_entries: self.inner.protected_entries(),
            protected_occupancy_bytes: self.inner.protected_bytes(),
            entry_budget: self.inner.entry_budget,
            byte_budget: self.inner.byte_budget,
        }
    }

    fn get(
        &mut self,
        key: &PathShortcutCacheKey,
        namespace_epoch: u64,
        mut generation_of: impl FnMut(Ino) -> u64,
    ) -> Option<PathShortcutCacheValue> {
        self.inner.get(
            key,
            PathShortcutLookupCtx {
                namespace_epoch,
                generation_of: &mut generation_of,
            },
        )
    }

    fn put(
        &mut self,
        key: PathShortcutCacheKey,
        value: PathShortcutCacheValue,
        namespace_epoch: u64,
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        self.inner.put(
            key,
            value,
            PathShortcutInsertCtx {
                namespace_epoch,
                parent_tokens: Arc::from(parent_tokens),
            },
        );
    }

    #[cfg(test)]
    fn entry_len(&self) -> usize {
        self.inner.entry_count()
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

// =============================================================================
// File Locking (flock)
// =============================================================================

/// File-level lock kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// Shared (read) lock.
    Shared,
    /// Exclusive (write) lock.
    Exclusive,
    /// Unlock.
    Unlock,
}

/// Internal state for a file-level lock.
#[derive(Debug)]
struct FileLock {
    kind: LockKind,
    owner: u64,
    /// Number of shared locks held (only meaningful when `kind == Shared`).
    shared_count: u32,
}

/// Per-VFS handle table tracking all open file descriptors.
struct HandleTable {
    /// Monotonically increasing file handle counter.
    next_fh: AtomicU64,
    /// All open handles: fh → OpenFile.
    handles: parking_lot::RwLock<HashMap<u64, OpenFile>>,
    /// Inode → number of currently open file descriptors.
    open_counts: Mutex<HashMap<Ino, u32>>,
    /// Inodes that were unlinked while still open (deferred deletion).
    orphans: Mutex<std::collections::HashSet<Ino>>,
}

impl HandleTable {
    fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            handles: parking_lot::RwLock::new(HashMap::new()),
            open_counts: Mutex::new(HashMap::new()),
            orphans: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Allocate a new file handle and register it.
    fn open(&self, ino: Ino, flags: OpenFlags) -> OpenFile {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        let open_file = OpenFile::new(fh, ino, flags);
        self.handles.write().insert(fh, open_file.clone());
        *self.open_counts.lock().entry(ino).or_insert(0) += 1;
        open_file
    }

    /// Look up an open file handle.
    fn get(&self, fh: u64) -> Option<OpenFile> {
        self.handles.read().get(&fh).cloned()
    }

    /// Close a file handle.  Returns `(ino, remaining_open_count)`.
    fn close(&self, fh: u64) -> Option<(OpenFile, u32)> {
        let open_file = self.handles.write().remove(&fh)?;
        let mut counts = self.open_counts.lock();
        let count = counts.get_mut(&open_file.ino)?;
        *count = count.saturating_sub(1);
        let remaining = *count;
        if remaining == 0 {
            counts.remove(&open_file.ino);
        }
        Some((open_file, remaining))
    }

    /// Get the number of open handles for an inode.
    fn open_count(&self, ino: Ino) -> u32 {
        self.open_counts.lock().get(&ino).copied().unwrap_or(0)
    }

    /// Mark an inode as orphan (unlinked while open).
    fn mark_orphan(&self, ino: Ino) {
        self.orphans.lock().insert(ino);
    }

    /// Check if inode is an orphan and remove the mark. Returns true if it was.
    fn take_orphan(&self, ino: Ino) -> bool {
        self.orphans.lock().remove(&ino)
    }

    /// Return the [`AccessClass`] of every currently open file handle.
    ///
    /// Used during shutdown to drain `active_access_classes` counters for
    /// handles that were never explicitly closed via `release_fh` (e.g. the
    /// kernel unmounted the filesystem without flushing all file descriptors).
    fn collect_open_access_classes(&self) -> Vec<AccessClass> {
        self.handles
            .read()
            .values()
            .map(|f| f.current_access_class())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExtentRecordId {
    object_id: u64,
    content_epoch: u64,
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

fn derive_fast_mode_sync_group_min_bytes(write_eager_flush_batch_bytes: usize) -> u64 {
    let target = (write_eager_flush_batch_bytes as u64).saturating_mul(4);
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

struct NamespaceState {
    dentry_cache: Mutex<DentryCache>,
    path_shortcut_cache: Mutex<PathShortcutCache>,
    attr_read_cache: Mutex<AttrReadCache>,
    dirty_attrs: Mutex<std::collections::HashMap<Ino, InodeAttr>>,
    namespace_epoch: AtomicU64,
    directory_generations: Mutex<std::collections::HashMap<Ino, u64>>,
}

impl NamespaceState {
    fn new(
        dentry_cache_capacity: usize,
        dentry_cache_bytes: usize,
        path_shortcut_cache_capacity: usize,
        path_shortcut_cache_bytes: usize,
        attr_read_cache_capacity: usize,
        attr_read_cache_bytes: usize,
    ) -> Self {
        Self {
            dentry_cache: Mutex::new(DentryCache::new(dentry_cache_capacity, dentry_cache_bytes)),
            path_shortcut_cache: Mutex::new(PathShortcutCache::new(
                path_shortcut_cache_capacity,
                path_shortcut_cache_bytes,
            )),
            attr_read_cache: Mutex::new(AttrReadCache::new(
                attr_read_cache_capacity,
                attr_read_cache_bytes,
            )),
            dirty_attrs: Mutex::new(std::collections::HashMap::new()),
            namespace_epoch: AtomicU64::new(0),
            directory_generations: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

struct AclState {
    effective_acl_cache: Mutex<std::collections::HashMap<Ino, EffectiveAclCacheEntry>>,
    acl_epoch: AtomicU64,
    acl_namespace_epoch: AtomicU64,
}

impl AclState {
    fn new() -> Self {
        Self {
            effective_acl_cache: Mutex::new(std::collections::HashMap::new()),
            acl_epoch: AtomicU64::new(0),
            acl_namespace_epoch: AtomicU64::new(0),
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

#[derive(Debug, Clone)]
enum PendingOverlayBlockSource {
    Pending(Bytes),
    Extent(u64),
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
}

struct FileLockState {
    file_locks: Mutex<std::collections::HashMap<Ino, FileLock>>,
    lock_notify: tokio::sync::Notify,
}

impl FileLockState {
    fn new() -> Self {
        Self {
            file_locks: Mutex::new(std::collections::HashMap::new()),
            lock_notify: tokio::sync::Notify::new(),
        }
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
    block_store_sync_data_calls: AtomicU64,
    block_store_sync_data_deferred_calls: AtomicU64,
    block_store_sync_data_synced_bytes: AtomicU64,
    block_store_sync_data_total_ns: AtomicU64,
    block_store_sync_data_max_ns: AtomicU64,
    block_store_sync_data_histogram: LatencyHistogram,
    winfsp_write_callbacks: AtomicU64,
    winfsp_flush_callbacks: AtomicU64,
    winfsp_cleanup_flush_fh_callbacks: AtomicU64,
}

impl MetricsState {
    fn new() -> Self {
        Self {
            materialize_batch_stats: MaterializeBatchStats::default(),
            ignored_entries: Mutex::new(IgnoredEntryRegistry::default()),
            next_ignored_ino: AtomicU64::new(1u64 << 63),
            prefetch_queue_depth: AtomicU64::new(0),
            flush_ordered_calls: AtomicU64::new(0),
            block_store_sync_data_calls: AtomicU64::new(0),
            block_store_sync_data_deferred_calls: AtomicU64::new(0),
            block_store_sync_data_synced_bytes: AtomicU64::new(0),
            block_store_sync_data_total_ns: AtomicU64::new(0),
            block_store_sync_data_max_ns: AtomicU64::new(0),
            block_store_sync_data_histogram: LatencyHistogram::default(),
            winfsp_write_callbacks: AtomicU64::new(0),
            winfsp_flush_callbacks: AtomicU64::new(0),
            winfsp_cleanup_flush_fh_callbacks: AtomicU64::new(0),
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
    ) -> Self {
        Self {
            ingress: IngressState::new(initial_checkpoint_meta),
            upload: UploadState::new(derive_upload_cache_io_foreground_cost_base(
                cfg.upload_cache_io_slots,
            )),
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

#[derive(Debug, Clone, Copy)]
struct EffectiveAclCacheEntry {
    access_level: AccessLevel,
    acl_epoch: u64,
    acl_namespace_epoch: u64,
}

#[derive(Debug, Clone)]
enum PrefetchWorkItem {
    ReadHint {
        ino: Ino,
        stream_id: u64,
        first_block: u32,
        last_block: u32,
        policy: AccessPolicy,
    },
    Batch(PrefetchBatch),
}

#[derive(Debug, Clone, Copy)]
struct PrefetchReadContext {
    size: u64,
    pending_content_epoch: Option<u64>,
}

#[derive(Debug, Clone)]
struct PrefetchBatch {
    ino: Ino,
    block_indices: Vec<u32>,
    read_context: Option<PrefetchReadContext>,
}

impl PrefetchBatch {
    fn single(ino: Ino, block_idx: u32, read_context: Option<PrefetchReadContext>) -> Self {
        Self {
            ino,
            block_indices: vec![block_idx],
            read_context,
        }
    }

    fn block_count(&self) -> usize {
        self.block_indices.len()
    }

    fn is_empty(&self) -> bool {
        self.block_indices.is_empty()
    }
}

impl PrefetchBatch {
    fn new(ino: Ino, block_indices: Vec<u32>, read_context: Option<PrefetchReadContext>) -> Self {
        Self {
            ino,
            block_indices,
            read_context,
        }
    }
}

struct StartupChannels {
    prefetch_rx: tokio::sync::mpsc::UnboundedReceiver<PrefetchWorkItem>,
    upload_rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupStatus {
    Uninitialized,
    Starting,
    Running,
    Failed,
}

struct StartupLifecycle {
    status: StartupStatus,
    channels: Option<StartupChannels>,
}

struct StartupState {
    lifecycle: Mutex<StartupLifecycle>,
    lifecycle_notify: Condvar,
    metadata_invalidation_sync_interval: Duration,
}

impl StartupState {
    #[allow(clippy::too_many_arguments)]
    fn new(
        prefetch_rx: tokio::sync::mpsc::UnboundedReceiver<PrefetchWorkItem>,
        upload_rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
        metadata_invalidation_sync_interval: Duration,
    ) -> Self {
        Self {
            lifecycle: Mutex::new(StartupLifecycle {
                status: StartupStatus::Uninitialized,
                channels: Some(StartupChannels {
                    prefetch_rx,
                    upload_rx,
                }),
            }),
            lifecycle_notify: Condvar::new(),
            metadata_invalidation_sync_interval,
        }
    }
}

// =============================================================================
// StudioVfs Implementation
// =============================================================================

/// The core VFS - satisfies filesystem operations using metadata + data + cache.
pub struct StudioVfs {
    /// Async boundary for metadata operations.
    meta_async: Arc<dyn AsyncMetaEngine>,
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
    vfs: &'a StudioVfs,
    started: Instant,
    shard_idx: usize,
    bytes_read: u64,
    access_class: AccessClass,
    policy: AccessPolicy,
}

impl<'a> ForegroundReadScope<'a> {
    fn new(vfs: &'a StudioVfs, policy: AccessPolicy) -> Self {
        vfs.mark_foreground_read();
        let shard_idx = vfs.select_foreground_latency_shard();
        Self {
            vfs,
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

impl Drop for ForegroundReadScope<'_> {
    fn drop(&mut self) {
        self.vfs.record_foreground_read_latency(
            self.shard_idx,
            self.bytes_read,
            self.started.elapsed(),
            self.access_class,
            self.policy,
        );
        self.vfs.unmark_foreground_read();
    }
}

#[derive(Debug, Clone, Copy)]
struct ReadRange {
    start: u64,
    end: u64,
}

impl ReadRange {
    fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then_some(Self { start, end })
    }
}

type PendingUploadIndex = HashMap<(Ino, u64), Vec<PendingUpload>>;

#[derive(Debug, Clone)]
enum ReadPlanSource {
    Dirty { data: Bytes, block_offset: u32 },
    PendingOverlay { data: Bytes, block_offset: u32 },
    Overlay { block: BlockRef },
    Base { block: BlockRef },
    Zero,
}

#[derive(Debug, Clone)]
struct ReadPlanSpan {
    range: ReadRange,
    source: ReadPlanSource,
}

macro_rules! with_sync_meta {
    ($self:expr, |$meta:ident| $body:block) => {{
        let $meta = $self.sync_meta();
        $body
    }};
}

fn map_cache_backend_detection_error(cache_dir: &str, source: std::io::Error) -> VfsError {
    VfsError::CacheBackendDetection {
        cache_root: cache_dir.to_string(),
        source,
    }
}

fn map_open_local_block_backend_error(
    cache_dir: &str,
    source: OpenLocalBlockBackendError,
) -> VfsError {
    match source {
        OpenLocalBlockBackendError::DetectCacheBackend { source, .. } => {
            map_cache_backend_detection_error(cache_dir, source)
        }
        OpenLocalBlockBackendError::OpenLegacyRawBackend { source, .. } => {
            VfsError::Other(format!("failed to open raw block backend: {source}"))
        }
        OpenLocalBlockBackendError::OpenCacheV3Backend { source, .. } => {
            VfsError::Other(format!("failed to open cache-v3 backend: {source}"))
        }
    }
}

impl StudioVfs {
    pub fn namespace_resolver(&self, style: NamespacePathStyle) -> NamespaceResolver<'_> {
        NamespaceResolver::new(self, style)
    }

    fn ignored_entry_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.state
            .metrics
            .ignored_entries
            .lock()
            .by_ino
            .get(&ino)
            .map(|ignored| ignored.attr.clone())
    }

    fn ignored_dir_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.ignored_entry_attr(ino)
            .filter(|attr| attr.kind == FileType::Directory)
    }

    fn ignored_entry_lookup(&self, parent: Ino, name: &[u8]) -> Option<IgnoredEntry> {
        let key = (parent, name.to_vec());
        let registry = self.state.metrics.ignored_entries.lock();
        let ino = registry.by_name.get(&key).copied()?;
        registry.by_ino.get(&ino).cloned()
    }

    fn ignored_entry(&self, ino: Ino) -> Option<IgnoredEntry> {
        self.state
            .metrics
            .ignored_entries
            .lock()
            .by_ino
            .get(&ino)
            .cloned()
    }

    fn create_ignored_entry(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        kind: FileType,
    ) -> InodeAttr {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let key = (parent, name.to_vec());
        if let Some(ino) = registry.by_name.get(&key).copied()
            && let Some(existing) = registry.by_ino.get(&ino)
        {
            return existing.attr.clone();
        }

        let ino = self
            .state
            .metrics
            .next_ignored_ino
            .fetch_add(1, Ordering::Relaxed);
        let attr = match kind {
            FileType::Directory => InodeAttr::new_dir(ino, parent, uid, gid, mode & 0o7777),
            FileType::RegularFile => InodeAttr::new_file(ino, parent, uid, gid, mode & 0o7777),
            FileType::Symlink => InodeAttr::new_symlink(ino, parent, uid, gid, 0),
        };
        let ignored = IgnoredEntry {
            attr: attr.clone(),
            parent,
            name: key.1.clone(),
        };
        registry.by_name.insert(key, ino);
        registry.by_ino.insert(ino, ignored);
        attr
    }

    fn update_ignored_entry<F>(&self, ino: Ino, update: F) -> Option<InodeAttr>
    where
        F: FnOnce(&mut InodeAttr),
    {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let ignored = registry.by_ino.get_mut(&ino)?;
        update(&mut ignored.attr);
        Some(ignored.attr.clone())
    }

    fn unlink_ignored_entry(&self, parent: Ino, name: &[u8]) -> bool {
        let key = (parent, name.to_vec());
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(ino) = registry.by_name.remove(&key) else {
            return false;
        };

        if self.handle_table.open_count(ino) > 0 {
            self.handle_table.mark_orphan(ino);
        } else {
            registry.by_ino.remove(&ino);
        }
        true
    }

    fn unlink_ignored_subtree(&self, parent: Ino, name: &[u8]) -> bool {
        let key = (parent, name.to_vec());
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(root_ino) = registry.by_name.remove(&key) else {
            return false;
        };

        let mut pending = vec![root_ino];
        let mut to_remove = Vec::new();
        while let Some(current) = pending.pop() {
            to_remove.push(current);
            let children: Vec<Ino> = registry
                .by_ino
                .iter()
                .filter_map(|(ino, entry)| (entry.parent == current).then_some(*ino))
                .collect();
            pending.extend(children);
        }

        for ino in to_remove {
            if let Some(ignored) = registry.by_ino.get(&ino).cloned() {
                registry
                    .by_name
                    .remove(&(ignored.parent, ignored.name.clone()));
                if self.handle_table.open_count(ino) > 0 {
                    self.handle_table.mark_orphan(ino);
                } else {
                    registry.by_ino.remove(&ino);
                }
            }
        }
        true
    }

    fn release_ignored_entry(&self, ino: Ino) -> bool {
        let mut registry = self.state.metrics.ignored_entries.lock();
        let Some(ignored) = registry.by_ino.remove(&ino) else {
            return false;
        };
        registry.by_name.remove(&(ignored.parent, ignored.name));
        true
    }

    fn ignored_entry_not_found(parent: Ino, name: &[u8]) -> VfsError {
        VfsError::Meta(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    fn is_blocked_root_metadata_path(&self, parent: Ino, name: &[u8]) -> bool {
        parent == ROOT_INO && is_blocked_root_macos_metadata_dir(name)
    }

    fn should_ignore_regular_file_path(&self, parent: Ino, name: &[u8]) -> bool {
        is_ignored_macos_metadata_name(name)
            || self.is_blocked_root_metadata_path(parent, name)
            || self.ignored_dir_attr(parent).is_some()
    }

    fn should_ignore_directory_path(&self, parent: Ino, name: &[u8]) -> bool {
        self.is_blocked_root_metadata_path(parent, name) || self.ignored_dir_attr(parent).is_some()
    }

    fn namespace_cache_token_for_parent(&self, parent: Ino) -> NamespaceCacheToken {
        let parent_generation = self
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&parent)
            .copied()
            .unwrap_or(0);
        NamespaceCacheToken {
            namespace_epoch: self.state.namespace.namespace_epoch.load(Ordering::Acquire),
            parent_generation,
        }
    }

    fn bump_namespace_epoch(&self) {
        self.state
            .namespace
            .namespace_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if current == u64::MAX {
                    warn!("namespace_epoch saturated at u64::MAX");
                }
                Some(current.saturating_add(1))
            })
            .ok();
    }

    fn bump_directory_generation(&self, ino: Ino) {
        let mut generations = self.state.namespace.directory_generations.lock();
        let current = generations.get(&ino).copied().unwrap_or(0);
        let next = current.saturating_add(1);
        if current == u64::MAX {
            warn!(ino, "directory generation saturated at u64::MAX");
        }
        generations.insert(ino, next);
    }

    fn bump_namespace_for_parents(&self, parents: impl IntoIterator<Item = Ino>) {
        self.bump_namespace_epoch();
        let mut generations = self.state.namespace.directory_generations.lock();
        let mut seen = HashSet::new();
        for parent in parents {
            if !seen.insert(parent) {
                continue;
            }
            let current = generations.get(&parent).copied().unwrap_or(0);
            let next = current.saturating_add(1);
            if current == u64::MAX {
                warn!(parent, "directory generation saturated at u64::MAX");
            }
            generations.insert(parent, next);
        }
    }

    fn bump_acl_epoch(&self) {
        self.state
            .acl
            .acl_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    fn bump_acl_namespace_epoch(&self) {
        self.state
            .acl
            .acl_namespace_epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .ok();
    }

    fn acl_rule_for_inode(&self, ino: Ino) -> VfsResult<Option<AccessLevel>> {
        let meta = self.sync_meta();
        if let Some(level) = meta.get_acl_rule(ino, &self.acl_principal)? {
            return Ok(Some(level));
        }
        if self.acl_principal != "*" {
            return Ok(meta.get_acl_rule(ino, "*")?);
        }
        Ok(None)
    }

    fn acl_rule_for_inode_meta(
        meta: &dyn MetaEngine,
        principal: &str,
        ino: Ino,
    ) -> MetaResult<Option<AccessLevel>> {
        if let Some(level) = meta.get_acl_rule(ino, principal)? {
            return Ok(Some(level));
        }
        if principal != "*" {
            return meta.get_acl_rule(ino, "*");
        }
        Ok(None)
    }

    fn access_satisfies(actual: AccessLevel, required: AccessLevel) -> bool {
        (actual as u8) >= (required as u8)
    }

    fn effective_access_level_cached(&self, ino: Ino) -> Option<AccessLevel> {
        let acl_epoch = self.state.acl.acl_epoch.load(Ordering::Acquire);
        let acl_namespace_epoch = self.state.acl.acl_namespace_epoch.load(Ordering::Acquire);
        self.state
            .acl
            .effective_acl_cache
            .lock()
            .get(&ino)
            .copied()
            .filter(|entry| {
                entry.acl_epoch == acl_epoch && entry.acl_namespace_epoch == acl_namespace_epoch
            })
            .map(|entry| entry.access_level)
    }

    fn effective_access_level_cache_insert(&self, ino: Ino, access_level: AccessLevel) {
        let acl_epoch = self.state.acl.acl_epoch.load(Ordering::Acquire);
        let acl_namespace_epoch = self.state.acl.acl_namespace_epoch.load(Ordering::Acquire);
        let mut cache = self.state.acl.effective_acl_cache.lock();
        if cache.len() >= self.effective_acl_cache_capacity {
            cache.clear();
        }
        cache.insert(
            ino,
            EffectiveAclCacheEntry {
                access_level,
                acl_epoch,
                acl_namespace_epoch,
            },
        );
    }

    fn effective_access_level(&self, ino: Ino) -> VfsResult<AccessLevel> {
        if let Some(access_level) = self.effective_access_level_cached(ino) {
            return Ok(access_level);
        }

        let mut current = ino;
        let mut depth = 0usize;
        let access_level = loop {
            if depth >= 4096 {
                break AccessLevel::ReadWrite;
            }
            if let Some(level) = self.acl_rule_for_inode(current)? {
                break level;
            }

            let attr = self.getattr_unchecked(current)?;
            if current == ROOT_INO || attr.parent == 0 || attr.parent == current {
                break AccessLevel::ReadWrite;
            }
            current = attr.parent;
            depth += 1;
        };

        self.effective_access_level_cache_insert(ino, access_level);
        Ok(access_level)
    }

    fn effective_access_level_meta(
        meta: &dyn MetaEngine,
        principal: &str,
        ino: Ino,
    ) -> MetaResult<AccessLevel> {
        let mut current = ino;
        let mut depth = 0usize;
        loop {
            if depth >= 4096 {
                return Ok(AccessLevel::ReadWrite);
            }
            if let Some(level) = Self::acl_rule_for_inode_meta(meta, principal, current)? {
                return Ok(level);
            }
            let attr = meta.get_attr(current)?;
            if current == ROOT_INO || attr.parent == 0 || attr.parent == current {
                return Ok(AccessLevel::ReadWrite);
            }
            current = attr.parent;
            depth += 1;
        }
    }

    fn enforce_access(&self, ino: Ino, required: AccessLevel) -> VfsResult<()> {
        let actual = self.effective_access_level(ino)?;
        if Self::access_satisfies(actual, required) {
            return Ok(());
        }
        if actual == AccessLevel::NoAccess {
            return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
        }
        Err(VfsError::Meta(MetaError::AccessDenied {
            ino,
            principal: self.acl_principal.clone(),
            required,
            actual,
        }))
    }

    async fn enforce_access_async(&self, ino: Ino, required: AccessLevel) -> VfsResult<()> {
        if let Some(actual) = self.effective_access_level_cached(ino) {
            if Self::access_satisfies(actual, required) {
                return Ok(());
            }
            if actual == AccessLevel::NoAccess {
                return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
            }
            return Err(VfsError::Meta(MetaError::AccessDenied {
                ino,
                principal: self.acl_principal.clone(),
                required,
                actual,
            }));
        }

        let meta = self.meta_async.clone();
        let principal = self.acl_principal.clone();
        let actual = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            Self::effective_access_level_meta(engine.as_ref(), &principal, ino)
        })
        .await?;
        self.effective_access_level_cache_insert(ino, actual);
        if Self::access_satisfies(actual, required) {
            return Ok(());
        }
        if actual == AccessLevel::NoAccess {
            return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
        }
        Err(VfsError::Meta(MetaError::AccessDenied {
            ino,
            principal: self.acl_principal.clone(),
            required,
            actual,
        }))
    }

    pub fn set_acl_rule(
        &self,
        ino: Ino,
        principal: &str,
        access_level: AccessLevel,
    ) -> VfsResult<()> {
        let meta = self.sync_meta();
        meta.set_acl_rule(ino, principal, access_level)?;
        self.bump_acl_epoch();
        meta.request_eager_flush();
        Ok(())
    }

    pub fn remove_acl_rule(&self, ino: Ino, principal: &str) -> VfsResult<()> {
        let meta = self.sync_meta();
        meta.remove_acl_rule(ino, principal)?;
        self.bump_acl_epoch();
        meta.request_eager_flush();
        Ok(())
    }

    fn dentry_cache_lookup(&self, parent: Ino, name: &[u8]) -> Option<DentryCacheValue> {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().get(
            parent,
            name,
            token.namespace_epoch,
            token.parent_generation,
        )
    }

    fn dentry_cache_insert_positive(&self, parent: Ino, name: &[u8], child: Ino) {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().put(
            parent,
            name,
            DentryCacheValue::Positive(child),
            token.namespace_epoch,
            token.parent_generation,
        );
    }

    fn dentry_cache_insert_negative(&self, parent: Ino, name: &[u8]) {
        let token = self.namespace_cache_token_for_parent(parent);
        self.state.namespace.dentry_cache.lock().put(
            parent,
            name,
            DentryCacheValue::Negative,
            token.namespace_epoch,
            token.parent_generation,
        );
    }

    fn dentry_cache_invalidate(&self, parent: Ino, name: &[u8]) {
        self.state
            .namespace
            .dentry_cache
            .lock()
            .invalidate(parent, name);
    }

    fn dentry_cache_invalidate_parent(&self, parent: Ino) {
        self.state
            .namespace
            .dentry_cache
            .lock()
            .invalidate_parent(parent);
    }

    fn dentry_cache_metrics(&self) -> DentryCacheMetrics {
        self.state.namespace.dentry_cache.lock().metrics()
    }

    fn directory_generation(&self, ino: Ino) -> u64 {
        self.state
            .namespace
            .directory_generations
            .lock()
            .get(&ino)
            .copied()
            .unwrap_or(0)
    }

    fn path_shortcut_cache_lookup_by_key(
        &self,
        key: &PathShortcutCacheKey,
    ) -> Option<PathShortcutCacheValue> {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        let generations = self.state.namespace.directory_generations.lock();
        self.state
            .namespace
            .path_shortcut_cache
            .lock()
            .get(key, namespace_epoch, |parent| {
                generations.get(&parent).copied().unwrap_or(0)
            })
    }

    fn path_shortcut_cache_insert_positive_by_key(
        &self,
        key: PathShortcutCacheKey,
        child_ino: Ino,
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        self.state.namespace.path_shortcut_cache.lock().put(
            key,
            PathShortcutCacheValue::Positive(child_ino),
            namespace_epoch,
            parent_tokens,
        );
    }

    fn path_shortcut_cache_insert_negative_by_key(
        &self,
        key: PathShortcutCacheKey,
        parent: Ino,
        name: &[u8],
        parent_tokens: Vec<PathShortcutParentToken>,
    ) {
        let namespace_epoch = self.state.namespace.namespace_epoch.load(Ordering::Acquire);
        self.state.namespace.path_shortcut_cache.lock().put(
            key,
            PathShortcutCacheValue::Negative {
                parent,
                name: Arc::from(name),
            },
            namespace_epoch,
            parent_tokens,
        );
    }

    fn path_shortcut_cache_metrics(&self) -> PathShortcutCacheMetrics {
        self.state.namespace.path_shortcut_cache.lock().metrics()
    }

    fn attr_read_cache_metrics(&self) -> AttrReadCacheMetrics {
        self.state.namespace.attr_read_cache.lock().metrics()
    }

    fn replay_dirty_uploads_from_local_backend(
        &self,
        pending_uploads: &[PendingUpload],
        attrs: &[InodeAttr],
    ) {
        let dirty_object_ids = self.local_block_backend.dirty_object_ids();
        let mut dirty_set: HashSet<u64> = dirty_object_ids.into_iter().collect();
        let mut quarantined_inodes = HashSet::new();
        let mut quarantined_object_ids = HashSet::new();
        let mut replay_ops: Vec<(u64, u64, u64, u64)> = Vec::new();
        let mut live_extents: Vec<(Ino, u32, u64, u64)> = Vec::new();
        let mut restored_live_extents = 0usize;
        let mut queued_epochs = 0usize;

        for attr in attrs {
            let ino = attr.ino;
            let needs_finalization = Self::needs_epoch_finalization(attr);
            if dirty_set.is_empty() && !needs_finalization {
                continue;
            }

            let blocks = match self.sync_meta().get_file_blocks(ino) {
                Ok(blocks) => blocks,
                Err(e) => {
                    let error = VfsError::Meta(e);
                    if Self::is_invalid_regular_file_graph_error(&error) {
                        warn!(
                            ino,
                            error = %error,
                            "quarantining invalid regular-file graph during startup recovery"
                        );
                        quarantined_inodes.insert(ino);
                        let protected = self
                            .quarantine_invalid_regular_file_graph_for_startup_recovery(
                                ino, attr.size, &error,
                            );
                        quarantined_object_ids.extend(protected);
                    } else {
                        warn!(
                            ino,
                            error = %error,
                            "failed to enumerate file blocks during startup recovery"
                        );
                    }
                    continue;
                }
            };

            if !dirty_set.is_empty() {
                let mut attr_live_extents: Vec<(Ino, u32, u64, u64)> = Vec::new();

                for (block_index, block_ref) in &blocks {
                    if !dirty_set.remove(&block_ref.object_id) {
                        continue;
                    }

                    let offset = (*block_index as u64).saturating_mul(self.block_size as u64);
                    replay_ops.push((ino, offset, block_ref.object_id, attr.content_epoch));
                    attr_live_extents.push((
                        ino,
                        *block_index,
                        block_ref.object_id,
                        attr.content_epoch,
                    ));
                    restored_live_extents = restored_live_extents.saturating_add(1);
                }

                if !dirty_set.is_empty() && attr.size > 0 {
                    match self
                        .sync_meta()
                        .lookup_regular_file_slices(ino, 0, attr.size)
                    {
                        Ok(spans) => {
                            for span in spans {
                                let Some(block) = span.block else {
                                    continue;
                                };
                                if !dirty_set.remove(&block.object_id) {
                                    continue;
                                }
                                let chunk_base =
                                    chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
                                let absolute_start =
                                    chunk_base.saturating_add(span.range.start as u64);
                                let block_index =
                                    block_index_from_offset(absolute_start, self.block_size);
                                let offset =
                                    (block_index as u64).saturating_mul(self.block_size as u64);
                                replay_ops.push((ino, offset, block.object_id, attr.content_epoch));
                                attr_live_extents.push((
                                    ino,
                                    block_index,
                                    block.object_id,
                                    attr.content_epoch,
                                ));
                                restored_live_extents = restored_live_extents.saturating_add(1);
                            }
                        }
                        Err(e) => {
                            warn!(
                                ino,
                                error = %e,
                                "failed to enumerate regular-file slices during startup recovery"
                            );
                        }
                    }
                }

                if !attr_live_extents.is_empty() {
                    live_extents.extend(attr_live_extents);
                }
            }

            if needs_finalization {
                self.seed_live_extents_from_file_blocks(ino, attr.content_epoch, blocks.as_slice());
                self.queue_epoch_finalization(ino, attr.content_epoch);
                queued_epochs = queued_epochs.saturating_add(1);
            }
        }

        if !quarantined_object_ids.is_empty() {
            dirty_set.retain(|object_id| !quarantined_object_ids.contains(object_id));
        }

        if !replay_ops.is_empty() {
            self.block_cache.submit_uploads_batch(&replay_ops);
            info!(
                replayed_dirty_blocks = replay_ops.len(),
                restored_live_extents, "replayed dirty local backend blocks into upload queue"
            );
        }

        if !live_extents.is_empty() {
            self.record_live_extents(&live_extents);
        }

        if queued_epochs > 0 {
            info!(queued_epochs, "seeded epoch finalizer queue from metadata");
        }

        if !dirty_set.is_empty() {
            if quarantined_inodes.is_empty() {
                // Cache the sync handle once so the recovery error path does
                // not re-enter metadata lookup while we are already cleaning up.
                let meta = self.sync_meta();
                let reachable_object_ids = match meta.get_referenced_object_ids() {
                    Ok(ids) => Some(ids),
                    Err(e) => {
                        warn!(
                            error = %e,
                            "failed to enumerate metadata reachability during startup recovery; preserving dirty blocks"
                        );
                        None
                    }
                };
                if let Some(reachable_object_ids) = reachable_object_ids {
                    dirty_set.retain(|object_id| !reachable_object_ids.contains(object_id));
                } else {
                    dirty_set.clear();
                }

                if !dirty_set.is_empty() {
                    let orphan_count = dirty_set.len() as u64;
                    for object_id in &dirty_set {
                        warn!(
                            object_id,
                            "removing orphan dirty block without metadata reference"
                        );
                        self.local_block_backend.remove(*object_id);
                        self.state.upload.pending_evictions.lock().remove(object_id);
                    }
                    self.state
                        .upload
                        .upload_canceled_stale
                        .fetch_add(orphan_count, Ordering::Relaxed);
                    info!(
                        orphan_dirty_blocks = dirty_set.len(),
                        "startup cleanup removed orphan dirty local backend entries"
                    );
                }
            } else {
                warn!(
                    quarantined_inodes = quarantined_inodes.len(),
                    orphan_dirty_blocks = dirty_set.len(),
                    "startup recovery preserved dirty local backend entries because invalid graphs were quarantined"
                );
            }
        }

        self.seed_live_extents_from_pending_uploads(pending_uploads, &quarantined_inodes);
        if !quarantined_inodes.is_empty() {
            self.cancel_pending_uploads_for_inodes(&quarantined_inodes);
        }
    }

    fn seed_live_extents_from_pending_uploads(
        &self,
        pending_uploads: &[PendingUpload],
        quarantined_inodes: &HashSet<Ino>,
    ) {
        let mut live_extents = Vec::new();
        for op in pending_uploads {
            if quarantined_inodes.contains(&op.ino) {
                continue;
            }
            if op.offset % self.block_size as u64 != 0 {
                continue;
            }
            let block_index = (op.offset / self.block_size as u64) as u32;
            live_extents.push((op.ino, block_index, op.object_id, op.content_epoch));
        }
        self.record_live_extents(&live_extents);
    }

    /// Create a new VFS instance using the current local block backend contract.
    pub fn new(
        meta: Arc<dyn MetaEngine>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        block_cache_l1_limit: u64,
        config: VfsConfigValidated,
    ) -> VfsResult<Arc<Self>> {
        Self::new_with_backend_selection(
            meta,
            data_config,
            cache_dir,
            cache_size,
            block_cache_l1_limit,
            config,
            LocalBackendSelectionMode::Auto,
        )
    }

    /// Create a new VFS instance with explicit local backend selection mode.
    pub fn new_with_backend_selection(
        meta: Arc<dyn MetaEngine>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        block_cache_l1_limit: u64,
        config: VfsConfigValidated,
        local_backend_selection_mode: LocalBackendSelectionMode,
    ) -> VfsResult<Arc<Self>> {
        Self::new_impl(
            meta,
            data_config,
            cache_dir,
            cache_size,
            block_cache_l1_limit,
            config,
            local_backend_selection_mode,
        )
    }

    fn new_impl(
        meta: Arc<dyn MetaEngine>,
        data_config: &DataStoreConfig,
        cache_dir: &str,
        cache_size: u64,
        block_cache_l1_limit: u64,
        config: VfsConfigValidated,
        local_backend_selection_mode: LocalBackendSelectionMode,
    ) -> VfsResult<Arc<Self>> {
        {
            let meta_async: Arc<dyn AsyncMetaEngine> =
                Arc::new(SyncMetaEngineAdapter::new(meta.clone()));
            // Get volume info
            let format_info = meta.get_format_info()?;
            let data_prefix = if format_info.data_prefix.trim().is_empty() {
                format!("{}/objects/", format_info.name)
            } else if format_info.data_prefix.ends_with('/') {
                format_info.data_prefix.clone()
            } else {
                format!("{}/", format_info.data_prefix)
            };
            let block_size = format_info.block_size;
            let derived_cache_runtime_tuning =
                derive_cache_runtime_tuning(cache_size, block_size, block_cache_l1_limit);
            let cache_runtime_tuning = resolve_cache_runtime_tuning_overrides(
                derived_cache_runtime_tuning,
                &config,
                config.cache_runtime_overrides(),
            );
            let cloud_logical_block_capacity = CLOUD_FRAGMENT_SIZE * CLOUD_FRAGMENT_COUNT;
            if (block_size as usize) > cloud_logical_block_capacity {
                return Err(VfsError::Other(format!(
                    "volume block_size {} exceeds 4x4 cloud fragment capacity {} ({} fragments x {} bytes)",
                    block_size,
                    cloud_logical_block_capacity,
                    CLOUD_FRAGMENT_COUNT,
                    CLOUD_FRAGMENT_SIZE
                )));
            }
            let janitor_interval = Duration::from_secs(config.janitor_interval_secs);
            let janitor_skip_newer_than = Duration::from_secs(config.janitor_skip_newer_secs);
            let janitor_max_deletes_per_run = config.janitor_max_deletes;
            let janitor_lease_ttl = Duration::from_secs(config.janitor_lease_ttl_secs);
            let metadata_invalidation_sync_interval =
                Duration::from_millis(config.metadata_invalidation_sync_interval_ms);
            let read_buffer_mb = config.read_buffer_mb;
            let block_size_nonzero = block_size as u128;
            // Block indices in the read path are u32; verify practical
            // addressable range at mount-time to avoid silent undersizing when
            // very small block sizes are configured.
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
            let mb_to_blocks = |mb: u64| -> u32 {
                let bytes = (mb as u128) * 1024 * 1024;
                let blocks = (bytes / block_size_nonzero).max(1).min(u32::MAX as u128);
                blocks as u32
            };
            let buffer_blocks_default = mb_to_blocks(read_buffer_mb).max(32);
            let prefetch_concurrency = config.prefetch_concurrency;
            let readahead_base_width = config.readahead_width;
            // Keep base conservative while allowing velocity-based expansion
            // for sustained sequential readers.
            let readahead_max_width = buffer_blocks_default
                .max(readahead_base_width.saturating_mul(8))
                .max(256);
            let readahead_min_prefetch = config.readahead_min_prefetch;
            let access_classifier = AccessClassifierConfig {
                block_size,
                readahead_base_width,
                readahead_max_width,
                readahead_min_prefetch,
            };
            let readahead_seq_step = config.readahead_seq_step;
            let readahead_random_penalty = config.readahead_random_penalty;
            let dir_prefetch_max_batch = config.dir_prefetch_max_batch;
            let readdir_attr_batch_chunk = config.readdir_attr_batch_chunk;
            let readdir_attr_batch_max_inodes = config.readdir_attr_batch_max_inodes;
            let prefetch_foreground_reserve = config.prefetch_foreground_reserve;
            let prefetch_max_per_cycle = config.prefetch_max_per_cycle;
            let prefetch_pause_on_neterr =
                Duration::from_millis(config.prefetch_pause_on_neterr_ms);
            let _lock_lease_ttl = Duration::from_secs(config.lock_ttl_secs);
            let _lock_retry_base_ms = config.lock_retry_base_ms;
            let _lock_retry_max_ms = config.lock_retry_max_ms;

            let janitor_lease = None;
            let distributed_flock = None;
            let max_dirty_bytes = cache_runtime_tuning.max_dirty_bytes;
            let write_eager_flush_batch_bytes = cache_runtime_tuning.write_eager_flush_batch_bytes;
            let local_sync_group_min_bytes =
                derive_fast_mode_sync_group_min_bytes(write_eager_flush_batch_bytes);
            let local_sync_group_max_delay =
                Duration::from_millis(FAST_MODE_SYNC_GROUP_MAX_DELAY_MS);
            let max_xattr_bytes = config.max_xattr_bytes;
            let dentry_cache_capacity = config.dentry_cache_capacity;
            let dentry_cache_bytes = config.dentry_cache_bytes;
            let path_shortcut_cache_capacity = config.path_shortcut_cache_capacity;
            let path_shortcut_cache_bytes = config.path_shortcut_cache_bytes;
            let attr_read_cache_capacity = config.attr_read_cache_capacity;
            let attr_read_cache_bytes = config.attr_read_cache_bytes;
            let effective_acl_cache_capacity = config.effective_acl_cache_capacity;
            let cache_safety_mode = config.cache_safety_mode;
            let spill_dir = PathBuf::from(cache_dir).join("ingress_spill");
            let ingress_wal =
                IngressWal::new(spill_dir, cache_runtime_tuning.max_ingress_segment_bytes);
            std::fs::create_dir_all(ingress_wal.spill_dir()).map_err(VfsError::Io)?;
            std::fs::create_dir_all(ingress_wal.segment_dir()).map_err(VfsError::Io)?;
            std::fs::create_dir_all(ingress_wal.checkpoint_dir()).map_err(VfsError::Io)?;
            let acl_principal = config.principal.clone();
            let initial_checkpoint_meta =
                init_ingress_recovery_state(cache_safety_mode, &ingress_wal)?;

            let opened_local_backend = open_local_block_backend_with_budget_and_mode(
                Path::new(cache_dir),
                &format_info.uuid,
                cache_size,
                local_backend_selection_mode,
            )
            .map_err(|source| map_open_local_block_backend_error(cache_dir, source))?;
            let cache_backend_detection = opened_local_backend.detection;
            let default_gate = cache_v3_default_gate_status();
            if cache_backend_detection.has_dual_layout() {
                if default_gate.ready {
                    info!(
                        cache_root = %cache_backend_detection.layout.cache_root.display(),
                        legacy_raw = %cache_backend_detection.legacy_raw_data_path.display(),
                        cache_v3_root = %cache_backend_detection.layout.v3_root.display(),
                        "cache root contains legacy raw and cache-v3 contract state; default cutover gate passed so cache-v3 is selected"
                    );
                } else {
                    let gate_blocker = default_gate
                        .blocker
                        .map(|blocker| blocker.as_str())
                        .unwrap_or("unknown blocker");
                    info!(
                        cache_root = %cache_backend_detection.layout.cache_root.display(),
                        legacy_raw = %cache_backend_detection.legacy_raw_data_path.display(),
                        cache_v3_root = %cache_backend_detection.layout.v3_root.display(),
                        gate_blocker,
                        "cache root contains legacy raw and cache-v3 contract state; mounting with legacy raw backend while default cutover gate is blocked"
                    );
                }
            }
            let selected_backend = opened_local_backend.backend_kind;
            if matches!(selected_backend, CacheBackendKind::CacheV3) {
                if cache_backend_detection.has_dual_layout() {
                    info!(
                        cache_root = %cache_backend_detection.layout.cache_root.display(),
                        legacy_raw = %cache_backend_detection.legacy_raw_data_path.display(),
                        cache_v3_root = %cache_backend_detection.layout.v3_root.display(),
                        "cache-v3 selected as the dual-layout default after validation gate pass"
                    );
                } else {
                    warn!(
                        cache_root = %cache_backend_detection.layout.cache_root.display(),
                        layout_marker = %cache_backend_detection.layout.layout_marker.display(),
                        cache_v3_root = %cache_backend_detection.layout.v3_root.display(),
                        "cache-v3 contract selected; using file-per-block local backend under the v3 subtree"
                    );
                }
            }
            let block_store = opened_local_backend.backend;

            // Init BlockCache upload queue state for pending write replay.
            // Compatibility-only L1 limit for the BlockCache shim path.
            let cache_config = BlockCacheConfig::new(cache_dir, block_cache_l1_limit);
            let (block_cache, upload_rx) = BlockCache::open(cache_config).map_err(VfsError::Io)?;

            // Init ReadAhead tracker + prefetch channel
            let readahead = Arc::new(ReadAheadTracker::new_with_bounds(
                readahead_base_width,
                readahead_max_width,
                readahead_min_prefetch,
                readahead_seq_step,
                readahead_random_penalty,
                4,
            ));
            let dir_prefetch = Arc::new(DirAccessTracker::new(2));
            let (prefetch_tx, prefetch_rx) =
                tokio::sync::mpsc::unbounded_channel::<PrefetchWorkItem>();

            // Create a read-optimised DataStore with dedicated connection pool and
            // tighter timeouts so that background uploads can never starve reads.
            let data_read = Arc::new(
                DataStore::with_profile(data_config, IoProfile::Read).map_err(|e| {
                    VfsError::Other(format!("failed to create read data store: {e}"))
                })?,
            );

            // Create a write-optimised DataStore for uploads/deletes.
            let data_write = Arc::new(
                DataStore::with_profile(data_config, IoProfile::Write).map_err(|e| {
                    VfsError::Other(format!("failed to create write data store: {e}"))
                })?,
            );

            // Build AIMD config for the fetch scheduler from config.
            let fetch_aimd_config = {
                let fetch_max_window_default = (readahead_max_width as f64).clamp(128.0, 4096.0);
                let fetch_initial_window_default =
                    (fetch_max_window_default / 8.0).clamp(16.0, 256.0);
                let fetch_increase_default = if fetch_max_window_default >= 1024.0 {
                    4.0
                } else {
                    2.0
                };
                let initial = if config.fetch_initial_window > 0.0 {
                    config.fetch_initial_window
                } else {
                    fetch_initial_window_default
                };
                let min = config.fetch_min_window;
                let max = if config.fetch_max_window > 0.0 {
                    config.fetch_max_window
                } else {
                    fetch_max_window_default
                };
                let increase = if config.fetch_increase_step > 0.0 {
                    config.fetch_increase_step
                } else {
                    fetch_increase_default
                };
                let decrease = if config.fetch_decrease_factor > 0.0 {
                    config.fetch_decrease_factor
                } else {
                    0.85
                };
                let cooldown_secs = config.fetch_decrease_cooldown_secs;
                crate::aimd::AimdConfig {
                    initial_window: initial,
                    min_window: min,
                    max_window: max,
                    increase_step: increase,
                    decrease_factor: decrease,
                    decrease_cooldown: Duration::from_secs(cooldown_secs),
                }
            };

            let qos = Arc::new(QosController::from_config(&config));
            let transit_memory_budget_bytes = cache_runtime_tuning.transit_memory_budget_bytes;
            let transit_memory_budget = TransitMemoryBudget::new(transit_memory_budget_bytes);
            let fetch_scheduler = Arc::new(crate::fetch::FetchScheduler::new(
                data_read,
                block_store.clone(),
                fetch_aimd_config,
                prefetch_foreground_reserve,
                qos.clone(),
                transit_memory_budget.clone(),
                &config,
            ));

            // Build AIMD config for upload scheduling from config.
            let upload_aimd_config = {
                let initial = if config.upload_initial_window > 0.0 {
                    config.upload_initial_window
                } else {
                    16.0
                };
                let min = if config.upload_min_window > 0.0 {
                    config.upload_min_window
                } else {
                    1.0
                };
                let max = if config.upload_max_window > 0.0 {
                    config.upload_max_window
                } else {
                    512.0
                };
                let increase = if config.upload_increase_step > 0.0 {
                    config.upload_increase_step
                } else {
                    2.0
                };
                let decrease = if config.upload_decrease_factor > 0.0 {
                    config.upload_decrease_factor
                } else {
                    0.85
                };
                let cooldown_secs = config.upload_decrease_cooldown_secs;
                crate::aimd::AimdConfig {
                    initial_window: initial,
                    min_window: min,
                    max_window: max,
                    increase_step: increase,
                    decrease_factor: decrease,
                    decrease_cooldown: Duration::from_secs(cooldown_secs),
                }
            };
            let upload_scheduler = Arc::new(crate::upload::UploadScheduler::new(
                data_write.clone(),
                upload_aimd_config,
                &config,
            ));
            let upload_delay = Duration::from_millis(config.upload_delay_ms);
            let upload_requeue_base = Duration::from_millis(config.upload_requeue_base_ms);
            let upload_requeue_max = Duration::from_millis(config.upload_requeue_max_ms);
            let upload_stats_interval = Duration::from_secs(config.upload_stats_interval_secs);
            let shutdown_metadata_flush_timeout =
                Duration::from_secs(config.shutdown_metadata_flush_timeout_secs);
            let ingress_checkpoint_interval =
                Duration::from_secs(cache_runtime_tuning.ingress_checkpoint_interval_secs);
            let ingress_checkpoint_min_append_bytes =
                cache_runtime_tuning.ingress_checkpoint_min_append_bytes;
            let ingress_checkpoint_min_mutations =
                cache_runtime_tuning.ingress_checkpoint_min_mutations;
            let ingress_checkpoint_max_segment_lag =
                cache_runtime_tuning.ingress_checkpoint_max_segment_lag;
            let ingress_checkpoint_max_live_segments =
                cache_runtime_tuning.ingress_checkpoint_max_live_segments;
            let upload_cache_io_slots = cache_runtime_tuning.upload_cache_io_slots;
            let upload_cache_io_foreground_cost_base =
                derive_upload_cache_io_foreground_cost_base(upload_cache_io_slots);
            let upload_cache_io_foreground_cost_step =
                derive_upload_cache_io_foreground_cost_step(upload_cache_io_slots);
            let upload_user_idle_grace = Duration::from_millis(config.upload_user_idle_grace_ms);
            let upload_fg_read_latency_target_ms = config.upload_fg_read_latency_target_ms;
            let upload_fg_read_latency_window_ms = config.upload_fg_read_latency_window_ms;
            let upload_fg_read_latency_sticky_windows =
                config.upload_fg_read_latency_sticky_windows;
            let upload_fg_read_latency_sticky_window_ms = upload_fg_read_latency_window_ms
                .saturating_mul(upload_fg_read_latency_sticky_windows);
            let upload_worker_count = config.upload_worker_count;
            let publish_visibility_timeout =
                Duration::from_secs(config.publish_visibility_timeout_secs);
            let local_sync_group_max_delay_ms = local_sync_group_max_delay.as_millis() as u64;
            debug!(
                writeback = true,
                cache_size_bytes = cache_size,
                upload_delay_ms = upload_delay.as_millis() as u64,
                requeue_base_ms = upload_requeue_base.as_millis() as u64,
                requeue_max_ms = upload_requeue_max.as_millis() as u64,
                upload_stats_interval_secs = upload_stats_interval.as_secs(),
                ingress_checkpoint_interval_secs = ingress_checkpoint_interval.as_secs(),
                ingress_checkpoint_min_append_bytes,
                ingress_checkpoint_min_mutations,
                ingress_checkpoint_max_segment_lag,
                ingress_checkpoint_max_live_segments,
                upload_cache_io_slots,
                upload_cache_io_foreground_cost_base,
                upload_cache_io_foreground_cost_step,
                transit_memory_budget_bytes,
                upload_user_idle_grace_ms = upload_user_idle_grace.as_millis() as u64,
                upload_fg_read_latency_target_ms,
                upload_fg_read_latency_window_ms,
                upload_fg_read_latency_sticky_windows,
                max_dirty_bytes,
                write_eager_flush_batch_bytes,
                local_sync_group_min_bytes,
                local_sync_group_max_delay_ms,
                max_xattr_bytes,
                dentry_cache_capacity,
                dentry_cache_bytes,
                path_shortcut_cache_capacity,
                path_shortcut_cache_bytes,
                attr_read_cache_capacity,
                attr_read_cache_bytes,
                readdir_attr_batch_chunk,
                readdir_attr_batch_max_inodes,
                "upload pipeline v2 enabled"
            );

            let vfs = Arc::new(Self {
                meta_async,
                data_write,
                local_block_backend: block_store,
                block_cache,
                block_size,
                data_prefix,
                acl_principal,
                effective_acl_cache_capacity,
                state: Arc::new(VfsState::new(initial_checkpoint_meta, block_size, &config)),
                upload_scheduler,
                upload_delay,
                upload_requeue_base,
                upload_requeue_max,
                upload_stats_interval,
                shutdown_metadata_flush_timeout,
                upload_cache_io_slots: Arc::new(Semaphore::new(upload_cache_io_slots)),
                // Safe cast: upload_cache_io_slots is validated earlier to fit in u32
                upload_cache_io_slots_max: {
                    debug_assert!(upload_cache_io_slots <= u32::MAX as usize);
                    upload_cache_io_slots as u32
                },
                transit_memory_budget,
                qos,
                upload_cache_io_foreground_cost_base,
                upload_cache_io_foreground_cost_step,
                upload_fg_read_latency_target_ns: Duration::from_millis(
                    upload_fg_read_latency_target_ms,
                )
                .as_nanos()
                .clamp(1, u64::MAX as u128)
                    as u64,
                upload_fg_read_latency_window: Duration::from_millis(
                    upload_fg_read_latency_window_ms,
                ),
                upload_fg_read_latency_sticky_window: Duration::from_millis(
                    upload_fg_read_latency_sticky_window_ms,
                ),
                access_classifier,
                upload_user_idle_grace,
                upload_worker_count,
                publish_visibility_timeout,
                readahead: readahead.clone(),
                dir_prefetch,
                prefetch_tx,
                startup: StartupState::new(
                    prefetch_rx,
                    upload_rx,
                    metadata_invalidation_sync_interval,
                ),
                max_dirty_bytes: AtomicUsize::new(max_dirty_bytes),
                write_eager_flush_batch_bytes,
                cache_safety_mode,
                local_sync_group_min_bytes,
                local_sync_group_max_delay,
                max_xattr_bytes,
                ingress_wal,
                handle_table: HandleTable::new(),
                janitor_lease,
                janitor_interval,
                janitor_skip_newer_than,
                janitor_max_deletes_per_run,
                janitor_lease_ttl,
                prefetch_concurrency,
                dir_prefetch_max_batch,
                readdir_attr_batch_chunk,
                readdir_attr_batch_max_inodes,
                prefetch_foreground_reserve,
                prefetch_max_per_cycle,
                prefetch_pause_on_neterr,
                fetch_scheduler: fetch_scheduler.clone(),
                distributed_flock,
                kernel_invalidator: RwLock::new(None),
                write_path_backend_tag: selected_backend.as_str().to_string(),
                write_path_started_at: Instant::now(),
            });

            let pending_uploads = vfs.block_cache.snapshot_pending_uploads();
            let attrs = vfs.sync_meta().list_inode_attrs()?;
            vfs.replay_dirty_uploads_from_local_backend(
                pending_uploads.as_slice(),
                attrs.as_slice(),
            );

            Ok(vfs)
        }
    }

    /// Start background workers for this VFS instance.
    ///
    /// `new()` is side-effect free and only builds the object graph. Call
    /// `start()` once after construction to spawn uploader, prefetch, janitor,
    /// and other maintenance tasks.
    pub fn start(self: &Arc<Self>) -> VfsResult<()> {
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

        let StartupChannels {
            mut prefetch_rx,
            upload_rx,
        } = {
            let mut lifecycle = self.startup.lifecycle.lock();
            loop {
                match lifecycle.status {
                    StartupStatus::Running => return Ok(()),
                    StartupStatus::Failed => {
                        return Err(VfsError::Other(
                            "StudioVfs background worker startup previously failed".to_string(),
                        ));
                    }
                    StartupStatus::Starting => self.startup.lifecycle_notify.wait(&mut lifecycle),
                    StartupStatus::Uninitialized => {
                        lifecycle.status = StartupStatus::Starting;
                        match lifecycle.channels.take() {
                            Some(channels) => break channels,
                            None => {
                                lifecycle.status = StartupStatus::Failed;
                                self.startup.lifecycle_notify.notify_all();
                                return Err(VfsError::Other(
                                    "StudioVfs startup channels are unavailable".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
        };

        let start_result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> VfsResult<()> {
                self.block_cache.spawn_background_tasks();

                // Spawn prefetch worker — holds Arc<Self> so it can run the
                // resolver-backed prefetch helper.
                let vfs_for_prefetch = self.clone();
                let prefetch_limit = self.prefetch_concurrency.max(1);
                self.track_background_task(tokio::spawn(async move {
                    let worker = async {
                        let prefetch_sem = Arc::new(Semaphore::new(prefetch_limit));
                        let mut pending_queue: VecDeque<PrefetchBatch> = VecDeque::new();
                        let mut pending_set: HashSet<(Ino, u32)> = HashSet::new();
                        let mut inflight = JoinSet::new();
                        loop {
                            // Launch as many queued prefetches as permits allow without
                            // blocking the worker event loop.
                            while !pending_queue.is_empty() {
                                if vfs_for_prefetch.shutdown_requested() {
                                    pending_queue.clear();
                                    pending_set.clear();
                                    vfs_for_prefetch
                                        .state
                                        .metrics
                                        .prefetch_queue_depth
                                        .store(0, Ordering::Relaxed);
                                    break;
                                }
                                let permit = match prefetch_sem.clone().try_acquire_owned() {
                                    Ok(permit) => permit,
                                    Err(_) => break,
                                };
                                let batch = pending_queue.pop_front().expect("batch exists");
                                for block_idx in &batch.block_indices {
                                    pending_set.remove(&(batch.ino, *block_idx));
                                }
                                vfs_for_prefetch
                                    .state
                                    .metrics
                                    .prefetch_queue_depth
                                    .store(
                                        pending_queue
                                            .iter()
                                            .map(|queued| queued.block_count() as u64)
                                            .sum(),
                                        Ordering::Relaxed,
                                    );
                                let mut started_blocks = Vec::with_capacity(batch.block_count());
                                for block_idx in batch.block_indices {
                                    if vfs_for_prefetch.readahead.start_prefetch(batch.ino, block_idx)
                                    {
                                        started_blocks.push(block_idx);
                                    }
                                }
                                if started_blocks.is_empty() {
                                    continue;
                                }
                                let v = vfs_for_prefetch.clone();
                                inflight.spawn(async move {
                                    let _permit = permit;
                                    let _ = v
                                        .read_prefetch_batch(PrefetchBatch::new(
                                            batch.ino,
                                            started_blocks,
                                            batch.read_context,
                                        ))
                                        .await;
                                });
                            }

                            tokio::select! {
                                _ = vfs_for_prefetch.state.shutdown.shutdown_notify.notified() => {
                                    pending_queue.clear();
                                    pending_set.clear();
                                    vfs_for_prefetch
                                        .state
                                        .metrics
                                        .prefetch_queue_depth
                                        .store(0, Ordering::Relaxed);
                                    inflight.abort_all();
                                    while inflight.join_next().await.is_some() {}
                                    break;
                                }
                                Some(work_item) = prefetch_rx.recv() => {
                                    if vfs_for_prefetch.shutdown_requested() {
                                        continue;
                                    }
                                    let planned: Vec<PrefetchBatch> = match work_item {
                                        PrefetchWorkItem::ReadHint {
                                            ino,
                                            stream_id,
                                            first_block,
                                            last_block,
                                            policy,
                                        } => vfs_for_prefetch.plan_prefetch_batches_from_hint(
                                            ino,
                                            stream_id,
                                            first_block,
                                            last_block,
                                            policy,
                                        ),
                                        PrefetchWorkItem::Batch(batch) => vec![batch],
                                    };
                                    for mut batch in planned {
                                        let ino = batch.ino;
                                        batch
                                            .block_indices
                                            .retain(|block_idx| pending_set.insert((ino, *block_idx)));
                                        if !batch.is_empty() {
                                            pending_queue.push_back(batch);
                                        }
                                    }
                                    vfs_for_prefetch.state.metrics.prefetch_queue_depth
                                        .store(
                                            pending_queue
                                                .iter()
                                                .map(|queued| queued.block_count() as u64)
                                                .sum(),
                                            Ordering::Relaxed,
                                        );
                                }
                                joined = inflight.join_next(), if !inflight.is_empty() => {
                                    if joined.is_none() {
                                        break;
                                    }
                                }
                            }
                        }
                    };
                    if let Err(panic_payload) =
                        std::panic::AssertUnwindSafe(worker).catch_unwind().await
                    {
                        let panic_msg = if let Some(msg) = panic_payload.downcast_ref::<&str>() {
                            *msg
                        } else if let Some(msg) = panic_payload.downcast_ref::<String>() {
                            msg.as_str()
                        } else {
                            "unknown panic payload"
                        };
                        error!("prefetch worker panicked: {}", panic_msg);
                    }
                }));

                // Spawn Upload Worker
                let vfs_clone = self.clone();
                self.set_uploader_task(tokio::spawn(async move {
                    vfs_clone.background_uploader(upload_rx).await;
                }));

                let vfs_clone = self.clone();
                self.track_background_task(tokio::spawn(async move {
                    vfs_clone.background_epoch_finalizer().await;
                }));

                // Spawn upload stats logger.
                let vfs_clone = self.clone();
                self.track_background_task(tokio::spawn(async move {
                    vfs_clone.background_upload_stats().await;
                }));

                // Warm up the HTTP GET connection pool asynchronously.
                // Avoid blocking here: `Handle::block_on` from another thread can
                // deadlock when `StudioVfs::new` is called under a current-thread runtime
                // (which is exactly how several integration tests execute).
                {
                    let fs = self.fetch_scheduler.clone();
                    let timeout = Duration::from_secs(3);
                    self.track_background_task(tokio::spawn(async move {
                        let _ = fs.warmup(timeout).await;
                    }));
                }

                // Spawn GC Worker
                let vfs_clone = self.clone();
                self.track_background_task(tokio::spawn(async move {
                    vfs_clone.background_gc().await;
                }));

                // Spawn periodic metadata sync watcher that turns remote metadata
                // version changes into kernel inode invalidations for the mounted FS.
                let vfs_clone = self.clone();
                let metadata_invalidation_sync_interval =
                    self.startup.metadata_invalidation_sync_interval;
                self.track_background_task(tokio::spawn(async move {
                    vfs_clone
                        .background_metadata_invalidation(metadata_invalidation_sync_interval)
                        .await;
                }));

                // Spawn Janitor Worker (automatic orphan scan/cleanup)
                let vfs_clone = self.clone();
                self.track_background_task(tokio::spawn(async move {
                    vfs_clone.background_janitor().await;
                }));

                Ok(())
            },
        )) {
            Ok(result) => result,
            Err(panic_payload) => {
                let panic_msg = if let Some(msg) = panic_payload.downcast_ref::<&str>() {
                    *msg
                } else if let Some(msg) = panic_payload.downcast_ref::<String>() {
                    msg.as_str()
                } else {
                    "unknown panic payload"
                };
                error!(
                    "StudioVfs::start panicked during background startup: {}",
                    panic_msg
                );
                Err(VfsError::Other(format!(
                    "StudioVfs::start panicked during background startup: {panic_msg}"
                )))
            }
        };

        let mut lifecycle = self.startup.lifecycle.lock();
        lifecycle.status = if start_result.is_ok() {
            StartupStatus::Running
        } else {
            StartupStatus::Failed
        };
        self.startup.lifecycle_notify.notify_all();
        drop(lifecycle);
        start_result
    }

    fn track_background_task(&self, handle: JoinHandle<()>) {
        self.state.shutdown.background_tasks.lock().push(handle);
    }

    fn set_uploader_task(&self, handle: JoinHandle<()>) {
        *self.state.shutdown.uploader_task.lock() = Some(handle);
    }

    async fn background_epoch_finalizer(self: Arc<Self>) {
        loop {
            let Some((ino, content_epoch)) = self.pop_epoch_finalization() else {
                tokio::select! {
                    _ = self.state.shutdown.shutdown_notify.notified() => break,
                    _ = self.state.upload.pending_epoch_finalizations_notify.notified() => {}
                }
                continue;
            };

            match commit::CommitCoordinator::finalize_inode_epoch(&self, ino, content_epoch).await {
                Ok(commit::EpochFinalizeStatus::Published) => {
                    self.note_file_session_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "published",
                    );
                }
                Ok(commit::EpochFinalizeStatus::Skipped) => {
                    self.note_file_session_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "skipped",
                    );
                }
                Ok(commit::EpochFinalizeStatus::Deferred) => {
                    self.note_file_session_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "deferred",
                    );
                    let retry_delay = self.upload_requeue_base.max(Duration::from_millis(25));
                    self.schedule_epoch_finalization_retry(ino, content_epoch, retry_delay);
                }
                Err(err) => {
                    if Self::is_invalid_regular_file_graph_error(&err) {
                        let attr_size = run_meta(
                            self.meta_async.clone(),
                            move |engine: Arc<dyn MetaEngine>| engine.get_attr(ino),
                        )
                        .await
                        .ok()
                        .map(|attr| attr.size)
                        .unwrap_or(0);
                        warn!(
                            ino,
                            content_epoch,
                            error = %err,
                            "epoch finalization failed due invalid regular-file graph, quarantining inode"
                        );
                        self.quarantine_invalid_regular_file_graph(ino, attr_size, &err);
                        self.note_file_session_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "failed",
                        );
                        continue;
                    }
                    if self.shutdown_requested() {
                        break;
                    }
                    self.note_file_session_background_transition(
                        ino,
                        FileSessionOperation::Finalize,
                        "failed",
                    );
                    warn!(
                        ino,
                        content_epoch,
                        error = %err,
                        "epoch finalization failed, will retry"
                    );
                    self.schedule_epoch_finalization_retry(
                        ino,
                        content_epoch,
                        self.upload_requeue_base,
                    );
                }
            }
        }
    }

    fn schedule_epoch_finalization_retry(
        self: &Arc<Self>,
        ino: Ino,
        content_epoch: u64,
        delay: Duration,
    ) {
        let vfs = self.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = vfs.state.shutdown.shutdown_notify.notified() => {}
                _ = tokio::time::sleep(delay) => {
                    if !vfs.shutdown_requested() {
                        vfs.queue_epoch_finalization(ino, content_epoch);
                    }
                }
            }
        });
    }

    fn kernel_invalidator(&self) -> Option<Arc<dyn KernelInvalidator>> {
        self.kernel_invalidator.read().clone()
    }

    fn collect_metadata_inodes(engine: &dyn MetaEngine) -> MetaResult<HashSet<Ino>> {
        let mut seen = HashSet::from([ROOT_INO]);
        let mut stack = vec![ROOT_INO];

        while let Some(ino) = stack.pop() {
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            if attr.kind != FileType::Directory {
                continue;
            }
            let entries = match engine.readdir(ino) {
                Ok(entries) => entries,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            for entry in entries {
                if seen.insert(entry.ino) {
                    stack.push(entry.ino);
                }
            }
        }

        Ok(seen)
    }

    fn invalidate_kernel_inodes(&self, inodes: impl IntoIterator<Item = Ino>) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        let mut unique = HashSet::new();
        for ino in inodes {
            if unique.insert(ino) {
                invalidator.inval_inode(ino);
            }
        }
    }

    fn invalidate_attr_read_cache(&self, inodes: impl IntoIterator<Item = Ino>) {
        let mut cache = self.state.namespace.attr_read_cache.lock();
        for ino in inodes {
            cache.remove(ino);
        }
    }

    async fn background_metadata_invalidation(self: Arc<Self>, interval: Duration) {
        let meta = self.sync_meta();
        let mut last_namespace_token = meta.namespace_version_token();
        let mut last_metadata_token = meta.metadata_version_token();
        let mut last_snapshot: HashSet<Ino> = HashSet::new();

        if last_namespace_token.is_some() || last_metadata_token.is_some() {
            let meta = self.meta_async.clone();
            if let Ok(snapshot) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            {
                last_snapshot = snapshot;
            }
        }

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if self.kernel_invalidator().is_none() {
                continue;
            }

            let meta = self.meta_async.clone();
            let sync = run_meta(meta.clone(), move |engine: Arc<dyn MetaEngine>| {
                let before_namespace = engine.namespace_version_token();
                let before_metadata = engine.metadata_version_token();
                engine.sync_metadata()?;
                let after_namespace = engine.namespace_version_token();
                let after_metadata = engine.metadata_version_token();
                Ok((
                    before_namespace,
                    after_namespace,
                    before_metadata,
                    after_metadata,
                ))
            })
            .await;
            let Ok((
                _before_namespace,
                after_namespace_token,
                _before_metadata,
                after_metadata_token,
            )) = sync
            else {
                continue;
            };

            let namespace_changed = last_namespace_token != after_namespace_token;
            let metadata_changed = last_metadata_token != after_metadata_token;
            last_namespace_token = after_namespace_token.clone();
            last_metadata_token = after_metadata_token.clone();

            if !namespace_changed && !metadata_changed {
                continue;
            }

            let meta = self.meta_async.clone();
            let Ok(current_snapshot) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                Self::collect_metadata_inodes(engine.as_ref())
            })
            .await
            else {
                continue;
            };

            let mut to_invalidate = last_snapshot.clone();
            to_invalidate.extend(current_snapshot.iter().copied());
            self.invalidate_attr_read_cache(to_invalidate.iter().copied());
            self.invalidate_kernel_inodes(to_invalidate.iter().copied());
            if namespace_changed {
                self.bump_namespace_epoch();
            }
            if namespace_changed || metadata_changed {
                self.bump_acl_namespace_epoch();
            }
            last_snapshot = current_snapshot;
        }
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
        self.block_cache.cancel_upload(op, !has_open_handles);
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

    fn now_unix_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn now_unix_millis() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn pending_age_stats_ms(&self) -> (u64, u64) {
        let now_ms = Self::now_unix_millis();
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
        let jitter_seed = op.object_id ^ Self::now_unix_millis();
        let jitter_ms = jitter_seed % jitter_max;
        capped.saturating_add(Duration::from_millis(jitter_ms))
    }

    fn p90_ns(mut values: Vec<u64>) -> Option<u64> {
        if values.is_empty() {
            return None;
        }
        values.sort_unstable();
        let idx = ((values.len() - 1) * 90) / 100;
        values.get(idx).copied()
    }

    fn trim_foreground_latency_samples(
        samples: &mut VecDeque<ForegroundReadSample>,
        now_ms: u64,
        window: Duration,
    ) {
        let window_ms = window.as_millis().clamp(1, u64::MAX as u128) as u64;
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
        let shard_count = self.state.read.foreground_read_latency_shards.len().max(1);
        (self
            .state
            .read
            .foreground_read_shard_cursor
            .fetch_add(1, Ordering::Relaxed) as usize)
            % shard_count
    }

    fn record_foreground_read_latency(
        &self,
        shard_idx: usize,
        bytes_read: u64,
        elapsed: Duration,
        access_class: AccessClass,
        policy: AccessPolicy,
    ) {
        let now_ms = Self::now_unix_millis();
        let elapsed_ns = elapsed.as_nanos().clamp(1, u64::MAX as u128) as u64;
        let shard_count = self.state.read.foreground_read_latency_shards.len().max(1);
        let idx = shard_idx % shard_count;
        let mut samples = self.state.read.foreground_read_latency_shards[idx].lock();
        Self::trim_foreground_latency_samples(
            &mut samples,
            now_ms,
            self.upload_fg_read_latency_window,
        );
        samples.push_back(ForegroundReadSample {
            timestamp_ms: now_ms,
            latency_ns: elapsed_ns,
            bytes_read,
            access_class,
            policy,
        });
    }

    fn foreground_speedometer_snapshot(&self) -> (Option<u64>, usize, u64, u64) {
        let now_ms = Self::now_unix_millis();
        let mut samples_all = Vec::with_capacity(FOREGROUND_READ_SPEEDOMETER_MAX_SAMPLES);
        let mut total_samples = 0usize;
        let mut window_bytes = 0u64;
        let mut oldest_start_ms = u64::MAX;
        let mut newest_end_ms = 0u64;
        for shard in &self.state.read.foreground_read_latency_shards {
            let mut samples = shard.lock();
            Self::trim_foreground_latency_samples(
                &mut samples,
                now_ms,
                self.upload_fg_read_latency_window,
            );
            total_samples = total_samples.saturating_add(samples.len());
            for sample in samples.iter() {
                samples_all.push(sample.latency_ns);
                window_bytes = window_bytes.saturating_add(sample.bytes_read);
                let latency_ms = sample.latency_ns.saturating_div(1_000_000).max(1);
                oldest_start_ms =
                    oldest_start_ms.min(sample.timestamp_ms.saturating_sub(latency_ms));
                newest_end_ms = newest_end_ms.max(sample.timestamp_ms);
            }
        }
        let p90_ns = Self::p90_ns(samples_all);
        if let Some(p90) = p90_ns {
            self.state
                .read
                .upload_fg_read_last_p90_ns
                .store(p90, Ordering::Release);
            self.state
                .read
                .upload_fg_read_last_p90_at_ms
                .store(now_ms, Ordering::Release);
        }
        let window_coverage_ms = if total_samples == 0 {
            0
        } else {
            newest_end_ms.saturating_sub(oldest_start_ms).max(1)
        };
        (p90_ns, total_samples, window_bytes, window_coverage_ms)
    }

    fn foreground_access_pattern_snapshot(&self) -> Option<(AccessClass, AccessPolicy)> {
        let now_ms = Self::now_unix_millis();
        let mut bytes_by_class = [0u64; AccessClass::COUNT];
        let mut latest_policy_by_class = [AccessPolicy::default(); AccessClass::COUNT];

        for shard in &self.state.read.foreground_read_latency_shards {
            let mut samples = shard.lock();
            Self::trim_foreground_latency_samples(
                &mut samples,
                now_ms,
                self.upload_fg_read_latency_window,
            );
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
            .max_by_key(|(_, bytes)| *bytes)?;
        if dominant_bytes == 0 {
            return None;
        }
        let class = AccessClass::from_index(dominant_idx)?;
        Some((class, latest_policy_by_class[dominant_idx]))
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
        let now_ms = Self::now_unix_millis();
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
        let foreground_active = self.foreground_read_active();
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

    async fn current_content_epoch_async(&self, ino: Ino) -> VfsResult<u64> {
        if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino) {
            return Ok(attr.content_epoch);
        }
        let meta = self.meta_async.clone();
        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            Ok(engine.get_attr(ino)?.content_epoch)
        })
        .await
    }

    fn pending_content_epoch_for_inode(&self, ino: Ino) -> Option<u64> {
        self.state
            .incarnation
            .inode_incarnations
            .lock()
            .get(&ino)
            .and_then(|state| {
                state
                    .pending_incarnation
                    .as_ref()
                    .map(|pending| pending.content_epoch)
            })
    }

    fn snapshot_pending_upload_index(&self) -> PendingUploadIndex {
        let mut index = PendingUploadIndex::new();
        for op in self.block_cache.snapshot_pending_uploads() {
            index.entry((op.ino, op.offset)).or_default().push(op);
        }
        index
    }

    fn pending_live_extent_record(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadIndex>,
    ) -> Option<ExtentRecordId> {
        if let Some(extent) = self
            .state
            .incarnation
            .live_extent_records
            .lock()
            .get(&(ino, block_index))
            .copied()
        {
            if pending_content_epoch.is_none_or(|epoch| extent.content_epoch == epoch) {
                return Some(extent);
            }
        }

        let block_offset = (block_index as u64).saturating_mul(self.block_size as u64);
        let recovered = if let Some(pending_uploads) = pending_upload_index {
            pending_uploads
                .get(&(ino, block_offset))
                .and_then(|ops| {
                    ops.iter().find(|op| {
                        pending_content_epoch.is_none_or(|epoch| op.content_epoch == epoch)
                    })
                })
                .map(|op| ExtentRecordId {
                    object_id: op.object_id,
                    content_epoch: op.content_epoch,
                })
        } else {
            self.block_cache
                .snapshot_pending_uploads()
                .into_iter()
                .find(|op| {
                    op.ino == ino
                        && op.offset == block_offset
                        && pending_content_epoch.is_none_or(|epoch| op.content_epoch == epoch)
                })
                .map(|op| ExtentRecordId {
                    object_id: op.object_id,
                    content_epoch: op.content_epoch,
                })
        }?;

        self.state
            .incarnation
            .live_extent_records
            .lock()
            .insert((ino, block_index), recovered);
        Some(recovered)
    }

    fn pending_overlay_block_source(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadIndex>,
    ) -> Option<PendingOverlayBlockSource> {
        // Stage 1 — WriteBuffer: newest local bytes not yet represented by
        // committed local-backend metadata.
        if let Some(pending_block) = self
            .state
            .write_buffer
            .pending_blocks
            .lock()
            .get(&(ino, block_index))
        {
            return Some(PendingOverlayBlockSource::Pending(
                pending_block.data.clone().freeze(),
            ));
        }
        // Stage 2 — committed local cache state for the pending epoch.
        self.pending_live_extent_record(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        )
        .map(|extent| PendingOverlayBlockSource::Extent(extent.object_id))
    }

    fn read_pending_overlay_block_sync(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadIndex>,
    ) -> VfsResult<Option<Bytes>> {
        let Some(source) = self.pending_overlay_block_source(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        ) else {
            return Ok(None);
        };
        Ok(match source {
            PendingOverlayBlockSource::Pending(data) => Some(data),
            PendingOverlayBlockSource::Extent(object_id) => self.local_block_backend.get(object_id),
        })
    }

    async fn read_pending_overlay_block_async(
        &self,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        pending_upload_index: Option<&PendingUploadIndex>,
    ) -> VfsResult<Option<Bytes>> {
        let Some(source) = self.pending_overlay_block_source(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        ) else {
            return Ok(None);
        };
        match source {
            PendingOverlayBlockSource::Pending(data) => Ok(Some(data)),
            PendingOverlayBlockSource::Extent(object_id) => {
                self.block_store_get_async(object_id).await
            }
        }
    }

    fn inode_has_local_overlay(&self, ino: Ino) -> bool {
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
        if self
            .state
            .incarnation
            .live_extent_records
            .lock()
            .keys()
            .any(|(candidate, _)| *candidate == ino)
        {
            return true;
        }
        false
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
        self.maybe_gc_file_session(ino);
    }

    fn file_session_for_inode(&self, ino: Ino) -> Arc<FileSessionEntry> {
        self.state.file_session.session(ino)
    }

    fn maybe_gc_file_session(&self, ino: Ino) {
        let mut sessions = self.state.file_session.sessions.lock();
        if sessions.get(&ino).and_then(Weak::upgrade).is_none() {
            sessions.remove(&ino);
            self.state
                .file_session
                .counters
                .active_sessions
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
            debug!(ino, "file session gc removed idle session");
        }
    }

    async fn enter_file_session_op(&self, ino: Ino, op: FileSessionOperation) -> FileSessionPermit {
        let session = self.file_session_for_inode(ino);
        let counters = self.state.file_session.counters.clone();
        let queue_depth = session.queued_ops.fetch_add(1, Ordering::Relaxed) + 1;
        session.update_max_queue_depth(queue_depth);
        let global_queue_depth = counters.queued_ops.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = counters.max_queue_depth.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| (global_queue_depth > current).then_some(global_queue_depth),
        );

        let wait_started = Instant::now();
        let (read_guard, write_guard) = if op.read_lock() {
            (Some(session.gate.clone().read_owned().await), None)
        } else {
            (None, Some(session.gate.clone().write_owned().await))
        };
        session.queued_ops.fetch_sub(1, Ordering::Relaxed);
        counters
            .queued_ops
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();

        let waited_ms = wait_started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if waited_ms > 0 {
            session.wait_events.fetch_add(1, Ordering::Relaxed);
            session.wait_ms.fetch_add(waited_ms, Ordering::Relaxed);
            counters.wait_events.fetch_add(1, Ordering::Relaxed);
            counters.wait_ms.fetch_add(waited_ms, Ordering::Relaxed);
            counters.note_wait_reason(op);
        }

        counters.note_transition();
        session.record_transition(op, "start");
        FileSessionPermit {
            _read_gate: read_guard,
            _write_gate: write_guard,
            counters,
            session,
            op,
        }
    }

    fn note_file_session_open_handle(&self, ino: Ino) {
        let session = self.file_session_for_inode(ino);
        let next = session.open_handles.fetch_add(1, Ordering::Relaxed) + 1;
        self.state
            .file_session
            .counters
            .open_handles
            .fetch_add(1, Ordering::Relaxed);
        self.state.file_session.counters.note_transition();
        session.record_transition(FileSessionOperation::Read, "open");
        debug!(ino, open_handles = next, "file session open handle");
    }

    fn note_file_session_close_handle(&self, ino: Ino, remaining: u32) {
        let session = self.file_session_for_inode(ino);
        session.open_handles.store(remaining, Ordering::Relaxed);
        self.state
            .file_session
            .counters
            .open_handles
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        self.state.file_session.counters.note_transition();
        session.record_transition(FileSessionOperation::Release, "handle_close");
        debug!(ino, open_handles = remaining, "file session close handle");
        if remaining == 0 {
            self.maybe_gc_file_session(ino);
        }
    }

    fn fallback_access_state(&self, ino: Ino, is_write: bool) -> Arc<Mutex<HandleAccessState>> {
        let states = if is_write {
            &self.state.read.write_access_fallback
        } else {
            &self.state.read.read_access_fallback
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
        self.state.read.active_access_classes[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn decrement_active_access_class(&self, class: AccessClass) {
        self.state.read.active_access_classes[class.index()]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_sub(1))
            })
            .ok();
    }

    /// Atomically transition the active-handle count from `from` to `to`.
    ///
    /// Performing both operations in a single helper avoids a window where the
    /// decrement has landed but the increment has not, which could briefly make
    /// policy decisions based on misleading class counts (e.g. SequentialMedia
    /// momentarily showing 0 active handles mid-transition).
    fn transition_active_access_class(&self, from: AccessClass, to: AccessClass) {
        if from == to {
            return;
        }
        self.decrement_active_access_class(from);
        self.increment_active_access_class(to);
    }

    fn access_class_active_count(&self, class: AccessClass) -> u64 {
        self.state.read.active_access_classes[class.index()].load(Ordering::Relaxed)
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
        self.state.read.access_class_transitions[next.index()].fetch_add(1, Ordering::Relaxed);
        if update_active_handles {
            self.transition_active_access_class(previous, next);
        }
    }

    fn observe_read_access(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        len: usize,
    ) -> AccessDecision {
        if let Some(open_file) = fh.and_then(|handle| self.handle_table.get(handle)) {
            let decision = open_file.observe_read(offset, len, self.access_classifier);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, false);
        let decision = state
            .lock()
            .observe_read(offset, len, self.access_classifier);
        self.record_access_class_transition(decision.previous_class, decision.class, false);
        decision
    }

    fn observe_write_access(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        len: usize,
    ) -> AccessDecision {
        if let Some(open_file) = fh.and_then(|handle| self.handle_table.get(handle)) {
            let decision = open_file.observe_write(offset, len, self.access_classifier);
            self.record_access_class_transition(decision.previous_class, decision.class, true);
            return decision;
        }
        let state = self.fallback_access_state(ino, true);
        let decision = state
            .lock()
            .observe_write(offset, len, self.access_classifier);
        self.record_access_class_transition(decision.previous_class, decision.class, false);
        decision
    }

    fn clear_access_tracking_for_inode(&self, ino: Ino) {
        self.state
            .read
            .prefetch_last_hint
            .lock()
            .retain(|(tracked_ino, _), _| *tracked_ino != ino);
        self.state.read.read_access_fallback.lock().remove(&ino);
        self.state.read.write_access_fallback.lock().remove(&ino);
    }

    fn clear_access_tracking_for_stream(&self, ino: Ino, stream_id: u64) {
        self.state
            .read
            .prefetch_last_hint
            .lock()
            .remove(&(ino, stream_id));
        self.readahead
            .evict_stream(ReadStreamKey { ino, stream_id });
    }

    pub(crate) fn note_file_session_background_transition(
        &self,
        ino: Ino,
        op: FileSessionOperation,
        phase: &'static str,
    ) {
        self.state.file_session.counters.note_transition();
        self.file_session_for_inode(ino)
            .record_transition(op, phase);
        if matches!(phase, "complete" | "published" | "recovered" | "skipped") {
            self.maybe_gc_file_session(ino);
        }
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
        self.note_file_session_background_transition(ino, FileSessionOperation::Upload, "failed");
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
        self.note_file_session_background_transition(
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
            self.note_file_session_background_transition(
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
            self.block_cache.cancel_upload(op, false);
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
            self.note_file_session_background_transition(
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

    fn seed_live_extents_from_file_blocks(
        &self,
        ino: Ino,
        content_epoch: u64,
        blocks: &[(u32, BlockRef)],
    ) {
        let mut extents = Vec::with_capacity(blocks.len());
        for (block_index, block_ref) in blocks {
            extents.push((ino, *block_index, block_ref.object_id, content_epoch));
        }
        self.record_live_extents(&extents);
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
        self.local_block_backend.get(object_id)
    }

    async fn block_store_get_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || Ok(block_store.get(object_id))).await
    }

    async fn block_store_get_for_upload_async(&self, object_id: u64) -> VfsResult<Option<Bytes>> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || Ok(block_store.get_for_upload(object_id))).await
    }

    async fn block_store_run_pressure_maintenance_async(&self) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || {
            block_store.run_pressure_maintenance().map_err(|error| {
                VfsError::Other(format!("block backend maintenance failed: {error}"))
            })
        })
        .await
    }

    async fn block_store_put_async(
        &self,
        object_id: u64,
        data: Bytes,
        evictable: bool,
    ) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || {
            block_store
                .put(object_id, data.as_ref(), evictable)
                .map_err(|error| VfsError::Other(format!("block store put failed: {error}")))
        })
        .await
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

            let trigger = local_sync_trigger_for_write_path(
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

        let _sync_gate = self.state.write_buffer.local_sync_gate.lock().await;
        let bytes_to_sync = {
            let mut local_sync_group = self.state.write_buffer.local_sync_group.lock().await;
            let trigger = local_sync_trigger_for_write_path(
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
        let result = run_blocking(move || {
            block_store.sync_data().map_err(|error| {
                VfsError::Other(format!("block backend sync_data failed: {error}"))
            })
        })
        .await;
        let elapsed_ns = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        self.state
            .metrics
            .block_store_sync_data_calls
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .metrics
            .block_store_sync_data_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.state
            .metrics
            .block_store_sync_data_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        self.state
            .metrics
            .block_store_sync_data_histogram
            .observe_ns(elapsed_ns);
        result
    }

    async fn block_store_mark_uploaded_async(&self, object_id: u64) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || {
            block_store.mark_uploaded(object_id);
            Ok(())
        })
        .await
    }

    async fn block_store_remove_async(&self, object_id: u64) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || {
            block_store.remove(object_id);
            Ok(())
        })
        .await
    }

    async fn block_store_remove_batch_async(&self, object_ids: Vec<u64>) -> VfsResult<()> {
        let block_store = self.local_block_backend.clone();
        run_blocking(move || {
            block_store.remove_batch(&object_ids);
            Ok(())
        })
        .await
    }

    fn transition_upload_state_for_local_mutation(attr: &mut InodeAttr) {
        if attr.kind != FileType::RegularFile {
            attr.upload_state = MetaUploadState::Ready;
            return;
        }
        if attr.content_epoch == attr.committed_content_epoch && attr.size == attr.committed_size {
            attr.upload_state = MetaUploadState::Ready;
        } else if attr.committed_size > 0 || attr.committed_content_epoch > 0 {
            attr.upload_state = MetaUploadState::Updating;
        } else {
            attr.upload_state = MetaUploadState::Uploading;
        }
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
        let counters = self.state.file_session.counters.clone();
        let mut stats = VfsFileSessionStats {
            active_sessions: counters.active_sessions.load(Ordering::Relaxed) as usize,
            open_handles: counters.open_handles.load(Ordering::Relaxed),
            queued_ops: counters.queued_ops.load(Ordering::Relaxed),
            max_queue_depth: counters.max_queue_depth.load(Ordering::Relaxed),
            wait_events: counters.wait_events.load(Ordering::Relaxed),
            wait_ms: counters.wait_ms.load(Ordering::Relaxed),
            transitions: counters.transitions.load(Ordering::Relaxed),
            wait_reasons: Vec::new(),
        };
        for idx in 0..FileSessionOperation::COUNT {
            let Some(op) = FileSessionOperation::from_index(idx) else {
                continue;
            };
            let count = counters.wait_reasons[idx].load(Ordering::Relaxed);
            stats.wait_reasons.push((op.as_str().to_string(), count));
        }
        stats
    }

    pub fn foreground_read_protection_stats(&self) -> VfsForegroundReadProtectionStats {
        let (p90_latency_ns, speedometer_samples, window_bytes, window_coverage_ms) =
            self.foreground_speedometer_snapshot();
        let throughput_bytes_per_sec = Self::foreground_throughput_bytes_per_sec(
            speedometer_samples,
            window_bytes,
            window_coverage_ms,
        );
        let dominant = self.foreground_access_pattern_snapshot();
        let (dominant_access_class, dominant_policy) = match dominant {
            Some((class, policy)) => (Some(class.as_str().to_string()), policy),
            None => (None, AccessPolicy::default()),
        };

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
            active_sequential_media_reads: self
                .access_class_active_count(AccessClass::SequentialMediaRead),
            active_random_interactive_reads: self
                .access_class_active_count(AccessClass::RandomInteractiveRead),
            active_sequential_writers: self
                .access_class_active_count(AccessClass::SequentialWriter),
            active_random_writers: self.access_class_active_count(AccessClass::RandomWriter),
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

    #[inline]
    fn mark_foreground_read(&self) {
        self.state
            .read
            .foreground_reads_inflight
            .fetch_add(1, Ordering::Relaxed);
        self.state
            .read
            .last_foreground_read_ms
            .store(Self::now_unix_millis(), Ordering::Relaxed);
    }

    #[inline]
    fn unmark_foreground_read(&self) {
        self.state
            .read
            .foreground_reads_inflight
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
        self.state
            .read
            .last_foreground_read_ms
            .store(Self::now_unix_millis(), Ordering::Relaxed);
    }

    #[inline]
    fn foreground_read_active(&self) -> bool {
        if self
            .state
            .read
            .foreground_reads_inflight
            .load(Ordering::Relaxed)
            > 0
        {
            return true;
        }
        let last = self
            .state
            .read
            .last_foreground_read_ms
            .load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        Self::now_unix_millis().saturating_sub(last)
            <= self.upload_user_idle_grace.as_millis() as u64
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
        let now = Self::now_unix_secs();
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

    fn lock_record_path(cfg: &DistributedFlock, ino: Ino) -> String {
        format!("{}{}.lock", cfg.prefix, ino)
    }

    fn encode_remote_lock_record(state: &RemoteLockRecord) -> Vec<u8> {
        let exclusive = state.exclusive_owner.as_deref().unwrap_or("-");
        let shared = if state.shared_owners.is_empty() {
            "-".to_string()
        } else {
            state
                .shared_owners
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(",")
        };
        format!("{}\n{}\n{}\n", state.lease_until_unix, exclusive, shared).into_bytes()
    }

    fn decode_remote_lock_record(bytes: &[u8]) -> Option<RemoteLockRecord> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let lease_until_unix = lines.next()?.trim().parse::<i64>().ok()?;
        let exclusive_raw = lines.next().unwrap_or("-").trim();
        let shared_raw = lines.next().unwrap_or("-").trim();
        let exclusive_owner = if exclusive_raw.is_empty() || exclusive_raw == "-" {
            None
        } else {
            Some(exclusive_raw.to_string())
        };
        let mut shared_owners = BTreeSet::new();
        if !shared_raw.is_empty() && shared_raw != "-" {
            for item in shared_raw.split(',') {
                let v = item.trim();
                if !v.is_empty() {
                    shared_owners.insert(v.to_string());
                }
            }
        }
        Some(RemoteLockRecord {
            lease_until_unix,
            exclusive_owner,
            shared_owners,
        })
    }

    fn normalize_remote_lock_state(state: &mut RemoteLockRecord, now: i64) {
        if state.lease_until_unix <= now {
            state.lease_until_unix = 0;
            state.exclusive_owner = None;
            state.shared_owners.clear();
        }
    }

    async fn acquire_distributed_flock(
        &self,
        cfg: &DistributedFlock,
        ino: Ino,
        op: LockKind,
    ) -> VfsResult<bool> {
        let path = Self::lock_record_path(cfg, ino);
        let now = Self::now_unix_secs();
        let lease_until = now.saturating_add(cfg.lease_ttl.as_secs() as i64);

        let mut retries = 0u32;
        loop {
            let mut retry_later = false;
            let (mut state, etag_opt) = match cfg.op.stat(&path).await {
                Ok(meta) => {
                    let etag = meta.etag().map(ToString::to_string);
                    match cfg.op.read(&path).await {
                        Ok(raw) => {
                            let parsed = {
                                let raw_chunk = raw.chunk();
                                if raw_chunk.len() == raw.remaining() {
                                    Self::decode_remote_lock_record(raw_chunk)
                                } else {
                                    let raw = raw.to_bytes();
                                    Self::decode_remote_lock_record(&raw)
                                }
                            }
                            .unwrap_or_default();
                            (parsed, etag)
                        }
                        Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                            retry_later = true;
                            (RemoteLockRecord::default(), None)
                        }
                        Err(err) => {
                            return Err(VfsError::Other(format!("lock read failed: {err}")));
                        }
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    (RemoteLockRecord::default(), None)
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retry_later = true;
                    (RemoteLockRecord::default(), None)
                }
                Err(err) => return Err(VfsError::Other(format!("lock stat failed: {err}"))),
            };
            if retry_later {
                retries = retries.saturating_add(1);
                if retries >= 12 {
                    return Ok(false);
                }
                let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                    .min(cfg.retry_max_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                continue;
            }

            Self::normalize_remote_lock_state(&mut state, now);
            let mut next_state = state.clone();
            let client = cfg.client_id.clone();

            let allowed = match op {
                LockKind::Shared => {
                    if let Some(excl) = &state.exclusive_owner {
                        excl == &client
                    } else {
                        true
                    }
                }
                LockKind::Exclusive => {
                    let excl_ok = state
                        .exclusive_owner
                        .as_ref()
                        .map(|v| v == &client)
                        .unwrap_or(true);
                    let shared_ok = state.shared_owners.is_empty()
                        || (state.shared_owners.len() == 1
                            && state.shared_owners.contains(&client));
                    excl_ok && shared_ok
                }
                LockKind::Unlock => true,
            };

            if !allowed {
                return Ok(false);
            }

            match op {
                LockKind::Shared => {
                    next_state.exclusive_owner = None;
                    next_state.shared_owners.insert(client);
                }
                LockKind::Exclusive => {
                    next_state.exclusive_owner = Some(client);
                    next_state.shared_owners.clear();
                }
                LockKind::Unlock => {
                    if next_state.exclusive_owner.as_deref() == Some(cfg.client_id.as_str()) {
                        next_state.exclusive_owner = None;
                    }
                    next_state.shared_owners.remove(cfg.client_id.as_str());
                }
            }
            next_state.lease_until_unix = lease_until;

            let mut opts = WriteOptions::default();
            if let Some(etag) = etag_opt {
                opts.if_match = Some(etag);
            } else {
                opts.if_not_exists = true;
            }

            let payload = Self::encode_remote_lock_record(&next_state);
            match cfg.op.write_options(&path, payload, opts).await {
                Ok(_) => return Ok(true),
                Err(err) if err.kind() == ErrorKind::ConditionNotMatch => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) if err.is_temporary() || err.kind() == ErrorKind::Unexpected => {
                    retries = retries.saturating_add(1);
                    if retries >= 12 {
                        return Ok(false);
                    }
                    let backoff_ms = (cfg.retry_base_ms.saturating_mul(1u64 << retries.min(6)))
                        .min(cfg.retry_max_ms);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
                Err(err) => return Err(VfsError::Other(format!("lock write failed: {err}"))),
            }
        }
    }

    /// Register a kernel cache invalidator (called by the platform bridge
    /// after mount so metadata refreshes can invalidate kernel dentry/attribute cache).
    pub fn set_kernel_invalidator(&self, inv: Arc<dyn KernelInvalidator>) {
        *self.kernel_invalidator.write() = Some(inv);
    }

    /// Clear kernel cache invalidator registration.
    ///
    /// Used during unmount/shutdown so in-flight metadata watchers do not
    /// attempt to notify a torn-down filesystem channel.
    pub fn clear_kernel_invalidator(&self) {
        *self.kernel_invalidator.write() = None;
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
    /// In async paths, prefer `run_meta(...)` with `self.meta_async.clone()` so
    /// blocking work stays behind the explicit async boundary.
    fn sync_meta(&self) -> Arc<dyn MetaEngine> {
        self.meta_async.sync_engine()
    }

    /// Get metadata engine reference.
    pub fn meta(&self) -> Arc<dyn MetaEngine> {
        self.sync_meta()
    }

    /// Get volume name.
    pub fn volume_name(&self) -> VfsResult<String> {
        let meta = self.sync_meta();
        Ok(meta.get_format_info()?.name)
    }

    /// Get volume UUID.
    pub fn uuid(&self) -> VfsResult<String> {
        let meta = self.sync_meta();
        Ok(meta.get_format_info()?.uuid)
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

    /// Get filesystem statistics.
    ///
    /// Returns real inode count and used space from redb counters.
    /// Total capacity is virtual (1 PB) since the backing store is cloud.
    pub fn statfs(&self) -> VfsResult<StatFs> {
        use studiofs_meta::types::counters;
        let (used_inodes, used_bytes) = with_sync_meta!(self, |meta| {
            (
                meta.get_counter(counters::TOTAL_INODES)?,
                meta.get_counter(counters::USED_SPACE)?,
            )
        });
        let bsize = self.block_size as u64;

        // Virtual capacity: 1 PB
        let total_bytes: u64 = 1024 * 1024 * 1024 * 1024 * 1024; // 1 PB
        let total_blocks = total_bytes / bsize;
        let used_blocks = used_bytes / bsize;
        let free_blocks = total_blocks.saturating_sub(used_blocks);

        Ok(StatFs {
            blocks: total_blocks,
            bfree: free_blocks,
            bavail: free_blocks,
            files: used_inodes,
            ffree: u64::MAX - used_inodes,
            bsize: self.block_size,
            namelen: 255,
            frsize: self.block_size,
        })
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
            Self::now_unix_millis().saturating_sub(last_checkpoint_unix_ms)
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

    pub fn note_winfsp_write_callback(&self) {
        self.state
            .metrics
            .winfsp_write_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_flush_callback(&self) {
        self.state
            .metrics
            .winfsp_flush_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn note_winfsp_cleanup_flush_fh_callback(&self) {
        self.state
            .metrics
            .winfsp_cleanup_flush_fh_callbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns true when the inode still has local dirty state that requires a
    /// flush barrier before close/flush callback completion.
    pub fn inode_has_local_dirty_state(&self, ino: Ino) -> bool {
        self.has_pending_write_buffer_blocks_for_inode(ino)
            || self.state.namespace.dirty_attrs.lock().contains_key(&ino)
    }

    pub fn write_path_observability_snapshot(&self) -> VfsWritePathObservabilitySnapshot {
        let elapsed = self.write_path_started_at.elapsed();
        let flush_ordered_calls = self
            .state
            .metrics
            .flush_ordered_calls
            .load(Ordering::Relaxed);
        let sync_calls = self
            .state
            .metrics
            .block_store_sync_data_calls
            .load(Ordering::Relaxed);
        let sync_deferred_calls = self
            .state
            .metrics
            .block_store_sync_data_deferred_calls
            .load(Ordering::Relaxed);
        let sync_bytes = self
            .state
            .metrics
            .block_store_sync_data_synced_bytes
            .load(Ordering::Relaxed);
        let sync_total_ns = self
            .state
            .metrics
            .block_store_sync_data_total_ns
            .load(Ordering::Relaxed);
        let sync_max_ns = self
            .state
            .metrics
            .block_store_sync_data_max_ns
            .load(Ordering::Relaxed);
        let sync_histogram = self
            .state
            .metrics
            .block_store_sync_data_histogram
            .snapshot();

        let (p50_ns, p50_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.50);
        let (p95_ns, p95_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.95);
        let (p99_ns, p99_saturated) = Self::histogram_percentile_ns(&sync_histogram, 0.99);
        let synced_gib = sync_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let calls_per_gib = if synced_gib > 0.0 {
            sync_calls as f64 / synced_gib
        } else {
            0.0
        };

        VfsWritePathObservabilitySnapshot {
            generated_unix_ms: Self::now_unix_millis(),
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
            },
            winfsp_callbacks: VfsWritePathWinfspCallbackStats {
                write: self
                    .state
                    .metrics
                    .winfsp_write_callbacks
                    .load(Ordering::Relaxed),
                flush: self
                    .state
                    .metrics
                    .winfsp_flush_callbacks
                    .load(Ordering::Relaxed),
                cleanup_flush_fh: self
                    .state
                    .metrics
                    .winfsp_cleanup_flush_fh_callbacks
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
    // Attribute Operations
    // =========================================================================

    fn getattr_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.ignored_entry_attr(ino) {
            return Ok(attr);
        }

        if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino).cloned() {
            return Ok(attr);
        }

        if let Some(attr) = self
            .state
            .namespace
            .attr_read_cache
            .lock()
            .get(ino, Instant::now())
        {
            return Ok(attr);
        }

        let attr = match self.sync_meta().get_attr(ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => {
                self.clear_pending_blocks_for_inode(ino);
                self.clear_inode_incarnation(ino);
                self.state.namespace.dirty_attrs.lock().remove(&ino);
                self.state.namespace.attr_read_cache.lock().remove(ino);
                let _ = self.handle_table.take_orphan(ino);
                return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };
        self.state.namespace.attr_read_cache.lock().put(
            ino,
            attr.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        Ok(attr)
    }

    async fn getattr_async_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.ignored_entry_attr(ino) {
            return Ok(attr);
        }

        if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino) {
            return Ok(attr.clone());
        }

        if let Some(attr) = self
            .state
            .namespace
            .attr_read_cache
            .lock()
            .get(ino, Instant::now())
        {
            return Ok(attr);
        }

        let meta = self.meta_async.clone();
        let attr = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(ino)
        })
        .await
        {
            Ok(attr) => attr,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                self.clear_pending_blocks_for_inode(ino);
                self.clear_inode_incarnation(ino);
                self.state.namespace.dirty_attrs.lock().remove(&ino);
                self.state.namespace.attr_read_cache.lock().remove(ino);
                let _ = self.handle_table.take_orphan(ino);
                return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
            }
            Err(err) => return Err(err),
        };
        self.state.namespace.attr_read_cache.lock().put(
            ino,
            attr.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        Ok(attr)
    }

    /// Get file attributes.
    pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        let attr = self.getattr_unchecked(ino)?;
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        Ok(attr)
    }

    async fn getattr_async(&self, ino: Ino) -> VfsResult<InodeAttr> {
        let attr = self.getattr_async_unchecked(ino).await?;
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
        Ok(attr)
    }

    fn clear_stale_read_state_for_inode(&self, ino: Ino) {
        self.clear_pending_blocks_for_inode(ino);
        self.clear_inode_incarnation(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.state.namespace.attr_read_cache.lock().remove(ino);
        let _ = self.handle_table.take_orphan(ino);
    }

    async fn lookup_regular_file_slices_async(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> VfsResult<Vec<studiofs_meta::schema::ResolvedSliceSpan>> {
        let meta = self.meta_async.clone();
        match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.lookup_regular_file_slices(ino, offset, length)
        })
        .await
        {
            Ok(resolved) => Ok(resolved),
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                self.clear_stale_read_state_for_inode(ino);
                Err(VfsError::Meta(MetaError::InodeNotFound(ino)))
            }
            Err(err) => Err(err),
        }
    }

    async fn resolve_read_metadata_async(
        &self,
        ino: Ino,
        offset: u64,
        buffer_len: u64,
    ) -> VfsResult<(InodeAttr, Vec<studiofs_meta::schema::ResolvedSliceSpan>)> {
        if let Some(attr) = self.ignored_entry_attr(ino) {
            return Ok((attr, Vec::new()));
        }

        let dirty_attr = {
            let dirty_attrs = self.state.namespace.dirty_attrs.lock();
            dirty_attrs.get(&ino).cloned()
        };
        if let Some(attr) = dirty_attr {
            let read_len = attr.size.saturating_sub(offset).min(buffer_len) as usize;
            let resolved = if read_len == 0 {
                Vec::new()
            } else {
                self.lookup_regular_file_slices_async(ino, offset, read_len as u64)
                    .await?
            };
            return Ok((attr, resolved));
        }

        let cached_attr = {
            let mut attr_cache = self.state.namespace.attr_read_cache.lock();
            attr_cache.get(ino, Instant::now())
        };
        if let Some(attr) = cached_attr {
            let read_len = attr.size.saturating_sub(offset).min(buffer_len) as usize;
            let resolved = if read_len == 0 {
                Vec::new()
            } else {
                self.lookup_regular_file_slices_async(ino, offset, read_len as u64)
                    .await?
            };
            return Ok((attr, resolved));
        }

        let meta = self.meta_async.clone();
        let (attr, resolved) = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            let attr = engine.get_attr(ino)?;
            let read_len = attr.size.saturating_sub(offset).min(buffer_len) as usize;
            let resolved = if read_len == 0 {
                Vec::new()
            } else {
                engine.lookup_regular_file_slices(ino, offset, read_len as u64)?
            };
            Ok((attr, resolved))
        })
        .await
        {
            Ok(result) => result,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                self.clear_stale_read_state_for_inode(ino);
                return Err(VfsError::Meta(MetaError::InodeNotFound(ino)));
            }
            Err(err) => return Err(err),
        };

        self.state.namespace.attr_read_cache.lock().put(
            ino,
            attr.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        Ok((attr, resolved))
    }

    /// Set file attributes (partial update).
    ///
    /// Accepts `SetattrArgs` — any field set to `None`/`Omit` is left unchanged.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if let Some(attr) = self.update_ignored_entry(ino, |attr| {
            if let Some(new_size) = args.size {
                attr.size = new_size;
                attr.committed_size = new_size;
            }
            if let Some(mode) = args.mode {
                attr.mode = mode & 0o7777;
            }
            if let Some(uid) = args.uid {
                attr.uid = uid;
            }
            if let Some(gid) = args.gid {
                attr.gid = gid;
            }
            if let Some(ns) = args.atime.resolve() {
                attr.atime = ns;
            }
            if let Some(ns) = args.mtime.resolve() {
                attr.mtime = ns;
            }
            if let Some(ns) = args.crtime.resolve() {
                attr.crtime = ns;
            }
            if let Some(wa) = args.win_attrs {
                attr.win_attrs = wa;
            }
            attr.touch_ctime();
        }) {
            return Ok(attr);
        }

        let mut attr = self.getattr(ino)?;
        let mut changed = false;

        if let Some(new_size) = args.size
            && new_size != attr.size
        {
            self.begin_pending_incarnation(ino, &mut attr);
            if new_size < attr.size {
                // Truncate - may need to remove blocks
                self.truncate_blocks(ino, new_size, attr.size)?;
                attr.committed_size = attr.committed_size.min(new_size);
            }
            attr.size = new_size;
            self.update_pending_incarnation_size(ino, attr.size);
            changed = true;
        }

        if let Some(mode) = args.mode {
            attr.mode = mode & 0o7777;
            changed = true;
        }

        if let Some(uid) = args.uid {
            attr.uid = uid;
            changed = true;
        }

        if let Some(gid) = args.gid {
            attr.gid = gid;
            changed = true;
        }

        if let Some(ns) = args.atime.resolve() {
            attr.atime = ns;
            changed = true;
        }

        if let Some(ns) = args.mtime.resolve() {
            attr.mtime = ns;
            changed = true;
        }

        if let Some(ns) = args.crtime.resolve() {
            attr.crtime = ns;
            changed = true;
        }

        if let Some(wa) = args.win_attrs {
            attr.win_attrs = wa;
            changed = true;
        }

        if changed {
            // Always update ctime when any attribute changes
            attr.touch_ctime();

            // Store in dirty cache
            self.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, attr.clone());

            // Invalidate read-through cache since we've changed the attribute
            self.state.namespace.attr_read_cache.lock().remove(ino);

            // Commit to metadata
            self.sync_meta().set_attr(&attr)?;
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        Ok(attr)
    }

    /// Truncate blocks beyond new size.
    fn truncate_blocks(&self, ino: Ino, new_size: u64, old_size: u64) -> VfsResult<()> {
        let old_last_block = if old_size == 0 {
            0
        } else {
            block_index_from_offset(old_size - 1, self.block_size)
        };

        // Calculate the first block index to remove.
        // When new_size == 0, remove ALL blocks starting from index 0.
        // Otherwise, keep blocks 0..=new_last_block and remove the rest.
        let first_block_to_remove = if new_size == 0 {
            0
        } else {
            block_index_from_offset(new_size - 1, self.block_size) + 1
        };

        // Remove blocks beyond new size (or all blocks when new_size == 0).
        for block_idx in first_block_to_remove..=old_last_block {
            self.clear_pending_block(ino, block_idx);
            self.remove_live_extent(ino, block_idx);
            // Remove the block and get its ID for GC
            if let Some(object_id) = self.sync_meta().remove_file_block(ino, block_idx)? {
                // Mark old block for deletion (S3 cleanup via background GC)
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                self.sync_meta()
                    .add_pending_deletion(object_id, timestamp)?;
                debug!("marking block {} for deletion (truncate)", object_id);

                // Immediately evict this block from all cache tiers.
                self.evict_block_from_caches(ino, block_idx, object_id);
            }
        }

        // Update last block length if partial
        if new_size > 0 {
            let new_last_block = block_index_from_offset(new_size - 1, self.block_size);
            let last_block_offset = offset_within_block(new_size - 1, self.block_size);
            let new_last_len = last_block_offset as usize + 1;
            let trimmed_local = self.trim_local_block_staging(ino, new_last_block, new_last_len)?;
            if let Some(mut block_ref) = self.sync_meta().get_file_block(ino, new_last_block)? {
                if !trimmed_local
                    && let Some(bytes) = {
                        let pending_upload_index = self.snapshot_pending_upload_index();
                        self.load_local_block_bytes_sync(
                            ino,
                            new_last_block,
                            block_ref.object_id,
                            Some(&pending_upload_index),
                        )?
                    }
                    && bytes.len() > new_last_len
                {
                    self.install_truncated_pending_block(ino, new_last_block, bytes, new_last_len);
                }
                block_ref.length = last_block_offset + 1;
                if block_ref.length < self.block_size {
                    block_ref.flags |= MetaBlockFlags::PARTIAL_BLOCK;
                } else {
                    block_ref.flags.remove(MetaBlockFlags::PARTIAL_BLOCK);
                }
                self.sync_meta()
                    .set_file_block(ino, new_last_block, &block_ref)?;
            }
        }

        Ok(())
    }

    // =========================================================================
    // Directory Operations
    // =========================================================================

    fn heal_orphaned_dentry(&self, parent: Ino, name: &[u8], ino_hint: Option<Ino>) {
        match self.sync_meta().remove_entry(parent, name) {
            Ok(removed) => {
                debug!(
                    parent,
                    removed_ino = removed.ino,
                    "healed orphaned dentry after inode-not-found"
                );
            }
            Err(MetaError::EntryNotFound { .. }) => {
                debug!(parent, "orphaned dentry already absent during heal");
            }
            Err(err) => {
                warn!(
                    parent,
                    name = %String::from_utf8_lossy(name),
                    error = %err,
                    "failed to heal orphaned dentry after inode-not-found"
                );
            }
        }

        if let Some(ino) = ino_hint {
            self.clear_pending_blocks_for_inode(ino);
            self.clear_inode_incarnation(ino);
            self.state.namespace.dirty_attrs.lock().remove(&ino);
            self.state.namespace.attr_read_cache.lock().remove(ino);
        }

        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_negative(parent, name);
    }

    fn lookup_component_impl(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.enforce_access(parent, AccessLevel::ReadOnly)?;
        if let Some(ignored) = self.ignored_entry_lookup(parent, name) {
            return Ok(ignored.attr);
        }
        if self.is_blocked_root_metadata_path(parent, name) {
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        if self.ignored_dir_attr(parent).is_some() {
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        if let Some(cached) = self.dentry_cache_lookup(parent, name) {
            match cached {
                DentryCacheValue::Positive(child_ino) => match self.sync_meta().get_attr(child_ino)
                {
                    Ok(attr) => return Ok(attr),
                    Err(MetaError::InodeNotFound(_)) => {
                        self.dentry_cache_invalidate(parent, name);
                        self.heal_orphaned_dentry(parent, name, Some(child_ino));
                        return Err(Self::ignored_entry_not_found(parent, name));
                    }
                    Err(err) => return Err(VfsError::Meta(err)),
                },
                DentryCacheValue::Negative => {
                    return Err(Self::ignored_entry_not_found(parent, name));
                }
            }
        }

        let entry = match self.sync_meta().lookup(parent, name) {
            Ok(entry) => entry,
            Err(MetaError::EntryNotFound { .. }) => {
                self.dentry_cache_insert_negative(parent, name);
                return Err(Self::ignored_entry_not_found(parent, name));
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };
        self.dentry_cache_insert_positive(parent, name, entry.ino);
        if let Err(err) = self.enforce_access(entry.ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                self.heal_orphaned_dentry(parent, name, Some(entry.ino));
                return Err(Self::ignored_entry_not_found(parent, name));
            }
            return Err(err);
        }
        match self.getattr_unchecked(entry.ino) {
            Ok(attr) => Ok(attr),
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                self.heal_orphaned_dentry(parent, name, Some(entry.ino));
                Err(Self::ignored_entry_not_found(parent, name))
            }
            Err(err) => Err(err),
        }
    }

    /// Lookup an entry in a directory.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.namespace_resolver(NamespacePathStyle::Posix)
            .lookup_component(parent, name)
    }

    /// Read directory entries.
    pub fn readdir(&self, ino: Ino) -> VfsResult<Vec<DirEntry>> {
        let parent_effective = self.effective_access_level(ino)?;
        if parent_effective == AccessLevel::NoAccess {
            return Ok(Vec::new());
        }
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if self.ignored_dir_attr(ino).is_some() {
            return Ok(Vec::new());
        }

        let mut entries = self.sync_meta().readdir(ino)?;
        if ino == ROOT_INO {
            entries.retain(|entry| !is_blocked_root_macos_metadata_dir(&entry.name));
        }

        let mut cache_updates: Vec<(Ino, AccessLevel)> = Vec::with_capacity(entries.len());
        entries.retain(|entry| match self.acl_rule_for_inode(entry.ino) {
            Ok(Some(level)) => {
                cache_updates.push((entry.ino, level));
                level != AccessLevel::NoAccess
            }
            Ok(None) => {
                cache_updates.push((entry.ino, parent_effective));
                parent_effective != AccessLevel::NoAccess
            }
            Err(_) => false,
        });
        for (entry_ino, level) in cache_updates {
            self.effective_access_level_cache_insert(entry_ino, level);
        }

        // Feed the directory prefetch tracker with regular-file children.
        let file_children: Vec<Ino> = entries
            .iter()
            .filter(|e| e.kind == FileType::RegularFile)
            .map(|e| e.ino)
            .collect();
        if !file_children.is_empty() {
            self.dir_prefetch.on_readdir(ino, file_children);
        }
        self.schedule_readdir_attr_hydration(&entries);

        Ok(entries)
    }

    fn schedule_readdir_attr_hydration(&self, entries: &[DirEntry]) {
        if entries.is_empty()
            || self.readdir_attr_batch_chunk == 0
            || self.readdir_attr_batch_max_inodes == 0
        {
            return;
        }

        let mut inodes = Vec::with_capacity(entries.len().min(self.readdir_attr_batch_max_inodes));
        for entry in entries.iter().take(self.readdir_attr_batch_max_inodes) {
            if self.ignored_entry_attr(entry.ino).is_some() {
                continue;
            }
            inodes.push(entry.ino);
        }
        if inodes.is_empty() {
            return;
        }

        let meta = self.sync_meta();
        let state = self.state.clone();
        let chunk_size = self.readdir_attr_batch_chunk;
        let hydrate = move || {
            Self::hydrate_readdir_attr_cache_batches(meta, state, inodes, chunk_size);
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn_blocking(hydrate));
        } else {
            drop(std::thread::spawn(hydrate));
        }
    }

    fn hydrate_readdir_attr_cache_batches(
        meta: Arc<dyn MetaEngine>,
        state: Arc<VfsState>,
        inodes: Vec<Ino>,
        chunk_size: usize,
    ) {
        for chunk in inodes.chunks(chunk_size.max(1)) {
            let attrs = match meta.get_attrs_batch(chunk) {
                Ok(attrs) => attrs,
                Err(err) => {
                    debug!(
                        chunk_len = chunk.len(),
                        error = %err,
                        "readdir attr hydration batch failed"
                    );
                    continue;
                }
            };

            let now = Instant::now();
            let dirty_attrs = state.namespace.dirty_attrs.lock();
            let mut attr_cache = state.namespace.attr_read_cache.lock();
            for attr in attrs {
                if dirty_attrs.contains_key(&attr.ino) {
                    continue;
                }
                attr_cache.put(attr.ino, attr, ATTR_CACHE_TTL, now);
            }
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
            self.note_file_session_open_handle(ino);
            self.increment_active_access_class(open_file.current_access_class());
            debug!(
                "open_file ignored ino={} fh={} flags={:#x}",
                ino,
                fh,
                flags.bits()
            );
            return Ok(fh);
        }

        // Verify the inode exists
        let attr = self.getattr(ino)?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }

        // If O_TRUNC requested, truncate to 0
        if trunc {
            self.setattr(ino, SetattrArgs::size_only(0))?;
        }

        let open_file = self.handle_table.open(ino, flags);
        let fh = open_file.fh;
        self.note_file_session_open_handle(ino);
        self.increment_active_access_class(open_file.current_access_class());
        debug!("open_file ino={} fh={} flags={:#x}", ino, fh, flags.bits());

        // Eager prefetch: when opening a regular file for reading, schedule
        // background prefetch of the first block to hide initial S3 latency.
        if attr.kind == studiofs_meta::types::FileType::RegularFile
            && attr.size > 0
            && (flags.contains(OpenFlags::RDONLY) || flags.contains(OpenFlags::RDWR))
        {
            let _ = self
                .prefetch_tx
                .send(PrefetchWorkItem::Batch(PrefetchBatch::single(
                    ino,
                    0,
                    Some(self.prefetch_read_context_from_attr(ino, &attr)),
                )));
        }

        Ok(fh)
    }

    /// Release (close) a file handle.
    ///
    /// If the inode was unlinked while this handle was open (orphan) and this
    /// was the last open handle, perform the deferred deletion now.
    pub async fn release_fh(&self, fh: u64) -> VfsResult<()> {
        let Some((open_file, remaining)) = self.handle_table.close(fh) else {
            debug!("release_fh: unknown fh={}", fh);
            return Ok(());
        };
        let ino = open_file.ino;
        let stream_id = open_file.stream_id();
        self.decrement_active_access_class(open_file.current_access_class());
        debug!("release_fh fh={} ino={} remaining={}", fh, ino, remaining);
        self.note_file_session_close_handle(ino, remaining);
        self.clear_access_tracking_for_stream(ino, stream_id);
        if remaining > 0 {
            return Ok(());
        }

        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Release)
            .await;
        let result: VfsResult<()> = async {
            if self.release_ignored_entry(ino) {
                let _ = self.handle_table.take_orphan(ino);
                return Ok(());
            }

            if self.handle_table.take_orphan(ino) {
                if self.release_ignored_entry(ino) {
                    return Ok(());
                }
                // Last handle closed on an unlinked file — final cleanup
                debug!("orphan cleanup: ino={} (last fd closed)", ino);

                self.mark_inode_deleted_for_uploads(ino);

                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let meta = self.meta_async.clone();
                let (removed_ids, total_bytes) =
                    run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        let blocks = match engine.get_file_blocks(ino) {
                            Ok(blocks) => blocks,
                            Err(MetaError::InodeNotFound(_)) => Vec::new(),
                            Err(err) => return Err(err),
                        };
                        let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();

                        let removed_ids = match engine.remove_file_blocks(ino) {
                            Ok(ids) => ids,
                            Err(MetaError::InodeNotFound(_)) => Vec::new(),
                            Err(err) => return Err(err),
                        };
                        for &object_id in &removed_ids {
                            engine.add_pending_deletion(object_id, timestamp)?;
                        }

                        if total_bytes > 0 {
                            engine.decr_counter(
                                studiofs_meta::types::counters::USED_SPACE,
                                total_bytes,
                            )?;
                        }

                        match engine.remove_attr(ino) {
                            Ok(()) | Err(MetaError::InodeNotFound(_)) => {}
                            Err(err) => return Err(err),
                        }
                        let _ = engine.remove_deferred_delete(ino);
                        Ok((removed_ids, total_bytes))
                    })
                    .await?;

                if !removed_ids.is_empty() {
                    self.clear_pending_blocks_for_inode(ino);
                    self.evict_inode_from_caches(ino, &removed_ids).await;
                }
                let deferred_local_blocks = self.take_deferred_deleted_local_blocks(ino);
                if !deferred_local_blocks.is_empty() {
                    self.clear_pending_blocks_for_inode(ino);
                    self.evict_inode_from_caches(ino, &deferred_local_blocks)
                        .await;
                }
                debug!(
                    "orphan cleanup complete: ino={} removed_blocks={} deferred_local_blocks={} freed_bytes={}",
                    ino,
                    removed_ids.len(),
                    deferred_local_blocks.len(),
                    total_bytes
                );
                self.clear_inode_incarnation(ino);
                self.state.namespace.dirty_attrs.lock().remove(&ino);
                self.state.namespace.attr_read_cache.lock().remove(ino);
                self.maybe_clear_deleted_inode_marker(ino);
                return Ok(());
            }

            let writable_handle = open_file.flags.contains(OpenFlags::WRONLY)
                || open_file.flags.contains(OpenFlags::RDWR);
            let has_local_dirty_state = self.inode_has_local_dirty_state(ino);
            if has_local_dirty_state {
                self.flush_ordered(ino, LocalSyncPolicy::Opportunistic)
                    .await?;
            }
            if writable_handle || has_local_dirty_state {
                self.ensure_no_upload_sticky_error(ino)?;
            }
            Ok(())
        }
        .await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    /// Flush data for a file handle (per-fd close in POSIX).
    ///
    /// Called by FUSE `flush()`. Flushes pending writes for the inode.
    pub async fn flush_fh(&self, fh: u64) -> VfsResult<()> {
        if let Some(open_file) = self.handle_table.get(fh) {
            let ino = open_file.ino;
            let session = self
                .enter_file_session_op(ino, FileSessionOperation::Flush)
                .await;
            let result: VfsResult<()> = async {
                self.flush_ordered(ino, LocalSyncPolicy::Opportunistic)
                    .await?;
                self.ensure_no_upload_sticky_error(ino)?;
                Ok(())
            }
            .await;
            drop(session);
            self.maybe_gc_file_session(ino);
            result?;
        }
        Ok(())
    }

    /// fsync — flush data (and optionally metadata) to durable storage.
    ///
    /// 1. Flushes pending write buffer to the dirty-mem / slab pipeline.
    /// 2. If `!datasync`, also commits dirty attrs to redb.
    ///
    /// S3 upload remains async — same as LucidLink behaviour.
    pub async fn fsync(&self, ino: Ino, datasync: bool) -> VfsResult<()> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Fsync)
            .await;
        let result: VfsResult<()> = async {
            self.enforce_access(ino, AccessLevel::ReadOnly)?;
            if self.ignored_entry_attr(ino).is_some() {
                return Ok(());
            }

            // Flush pending write buffer
            self.flush_ordered(ino, LocalSyncPolicy::Strict).await?;
            self.maybe_block_store_sync_data_async(0, LocalSyncPolicy::Strict)
                .await?;

            // If full fsync (not datasync), also persist dirty attributes to redb
            let dirty_attr = if datasync {
                None
            } else {
                self.state.namespace.dirty_attrs.lock().remove(&ino)
            };
            if let Some(attr) = dirty_attr {
                let meta = self.meta_async.clone();
                run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.set_attr(&attr)
                })
                .await?;
                self.state.namespace.attr_read_cache.lock().remove(ino);
            }
            self.ensure_no_upload_sticky_error(ino)?;
            Ok(())
        }
        .await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    /// Get information about an open file handle.
    pub fn get_open_file(&self, fh: u64) -> Option<OpenFile> {
        self.handle_table.get(fh)
    }

    /// Get the number of open handles for an inode.
    pub fn open_count(&self, ino: Ino) -> u32 {
        self.handle_table.open_count(ino)
    }

    /// Read file data into `buffer` using the buffered read path.
    pub async fn read_into_fh(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_into_impl(Some(fh), ino, offset, buffer).await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    /// Write file data using the buffered write path.
    pub async fn write_fh(&self, fh: u64, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let _ = self.observe_write_access(Some(fh), ino, offset, data.len());
        self.write(ino, offset, data).await
    }

    // =========================================================================
    // File Locking (flock)
    // =========================================================================

    /// Acquire or release a file-level lock (flock semantics).
    ///
    /// - `Shared`: multiple readers allowed, blocks if exclusive lock held by another owner.
    /// - `Exclusive`: single writer, blocks if any lock held by another owner.
    /// - `Unlock`: release the lock.
    ///
    /// If `blocking` is false and the lock cannot be acquired, returns `VfsError::WouldBlock`.
    pub async fn flock(&self, ino: Ino, op: LockKind, owner: u64, blocking: bool) -> VfsResult<()> {
        loop {
            if op == LockKind::Unlock {
                let had_local_lock = {
                    let mut locks = self.state.file_lock.file_locks.lock();
                    let mut had = false;
                    if let Some(entry) = locks.get_mut(&ino) {
                        match entry.kind {
                            LockKind::Shared => {
                                had = true;
                                entry.shared_count = entry.shared_count.saturating_sub(1);
                                if entry.shared_count == 0 {
                                    locks.remove(&ino);
                                }
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                had = true;
                                locks.remove(&ino);
                            }
                            _ => {}
                        }
                    }
                    had
                };

                if had_local_lock {
                    if let Some(cfg) = &self.distributed_flock {
                        // Bimodal strong mode guarantee:
                        // flush local data + metadata before releasing distributed lock.
                        self.flush(ino).await?;
                        let meta = self.meta_async.clone();
                        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                            engine.flush_metadata()?;
                            Ok(())
                        })
                        .await?;
                        let _ = self
                            .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                            .await?;
                    }
                    self.state.file_lock.lock_notify.notify_waiters();
                }
                return Ok(());
            }

            let local_allowed = {
                let locks = self.state.file_lock.file_locks.lock();
                match locks.get(&ino) {
                    None => true,
                    Some(entry) => match op {
                        LockKind::Shared => {
                            entry.kind == LockKind::Shared
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Exclusive => {
                            entry.kind == LockKind::Unlock
                                || (entry.kind == LockKind::Exclusive && entry.owner == owner)
                        }
                        LockKind::Unlock => true,
                    },
                }
            };
            if !local_allowed {
                if !blocking {
                    return Err(VfsError::WouldBlock);
                }
                self.state.file_lock.lock_notify.notified().await;
                continue;
            }

            if let Some(cfg) = &self.distributed_flock {
                let acquired = self.acquire_distributed_flock(cfg, ino, op).await?;
                if !acquired {
                    if !blocking {
                        return Err(VfsError::WouldBlock);
                    }
                    tokio::time::sleep(Duration::from_millis(cfg.retry_base_ms)).await;
                    continue;
                }
                // Bimodal strong mode guarantee:
                // refresh local metadata view before lock is considered granted.
                let meta = self.meta_async.clone();
                run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.sync_metadata()?;
                    Ok(())
                })
                .await?;
            }

            let committed = {
                let mut locks = self.state.file_lock.file_locks.lock();
                match op {
                    LockKind::Shared => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock | LockKind::Shared => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = entry.shared_count.saturating_add(1);
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => {
                                entry.kind = LockKind::Shared;
                                entry.shared_count = 1;
                                true
                            }
                            _ => false,
                        }
                    }
                    LockKind::Exclusive => {
                        let entry = locks.entry(ino).or_insert(FileLock {
                            kind: LockKind::Unlock,
                            owner: 0,
                            shared_count: 0,
                        });
                        match entry.kind {
                            LockKind::Unlock => {
                                *entry = FileLock {
                                    kind: LockKind::Exclusive,
                                    owner,
                                    shared_count: 0,
                                };
                                true
                            }
                            LockKind::Exclusive if entry.owner == owner => true,
                            _ => false,
                        }
                    }
                    LockKind::Unlock => true,
                }
            };
            if committed {
                return Ok(());
            }

            // Local conflict appeared after remote acquisition attempt.
            // Best-effort release remote lock and retry.
            if let Some(cfg) = &self.distributed_flock {
                let _ = self
                    .acquire_distributed_flock(cfg, ino, LockKind::Unlock)
                    .await;
            }
            if !blocking {
                return Err(VfsError::WouldBlock);
            }
            self.state.file_lock.lock_notify.notified().await;
        }
    }

    // =========================================================================
    // Hardlinks
    // =========================================================================

    /// Create a hard link: add a new directory entry pointing to an existing inode.
    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr> {
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        self.enforce_access(new_parent, AccessLevel::ReadWrite)?;
        validate_name(new_name)?;
        if self.should_ignore_regular_file_path(new_parent, new_name) {
            let source_attr = self.getattr(ino)?;
            let ignored = self.create_ignored_entry(
                new_parent,
                new_name,
                source_attr.uid,
                source_attr.gid,
                source_attr.mode,
                FileType::RegularFile,
            );
            let ignored = self
                .update_ignored_entry(ignored.ino, |attr| {
                    attr.size = source_attr.size;
                    attr.committed_size = source_attr.committed_size;
                    attr.atime = source_attr.atime;
                    attr.mtime = source_attr.mtime;
                    attr.ctime = source_attr.ctime;
                    attr.crtime = source_attr.crtime;
                })
                .unwrap_or(ignored);
            self.bump_namespace_for_parents([new_parent]);
            return Ok(ignored);
        }
        let attr = self.sync_meta().link(ino, new_parent, new_name)?;
        self.bump_namespace_for_parents([new_parent]);
        self.bump_acl_namespace_epoch();
        self.dentry_cache_insert_positive(new_parent, new_name, ino);
        // Invalidate attr caches (nlink changed)
        self.state.namespace.attr_read_cache.lock().remove(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Create a regular file.
    pub fn create(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr =
                self.create_ignored_entry(parent, name, uid, gid, mode, FileType::RegularFile);
            self.bump_namespace_for_parents([parent]);
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_file(parent, name, uid, gid, mode & 0o7777)?;
        self.state.incarnation.inode_incarnations.lock().insert(
            attr.ino,
            InodeIncarnationState {
                committed_incarnation: None,
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: attr.content_epoch,
                    size: attr.size,
                    base_size: 0,
                }),
                upload_state: VfsUploadState::Uploading,
            },
        );
        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_positive(parent, name, attr.ino);
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.namespace.attr_read_cache.lock().remove(parent);
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Create a directory.
    pub fn mkdir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        if self.should_ignore_directory_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, mode, FileType::Directory);
            self.bump_namespace_for_parents([parent]);
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_dir(parent, name, uid, gid, mode & 0o7777)?;
        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_positive(parent, name, attr.ino);
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.namespace.attr_read_cache.lock().remove(parent);
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Create a symbolic link.
    pub fn symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> VfsResult<InodeAttr> {
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, 0o777, FileType::Symlink);
            self.bump_namespace_for_parents([parent]);
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_symlink(parent, name, target, uid, gid)?;
        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_positive(parent, name, attr.ino);
        self.state.namespace.attr_read_cache.lock().remove(parent);
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Read the target of a symbolic link.
    pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if matches!(self.ignored_entry_attr(ino), Some(attr) if attr.kind == FileType::Symlink) {
            return Ok(Vec::new());
        }
        Ok(self.sync_meta().get_symlink_target(ino)?)
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

    /// Remove a file.
    ///
    /// If the file has open handles, the dentry is removed and nlink
    /// decremented, but the inode and blocks are kept alive (orphan).
    /// Actual cleanup happens in `release_fh()` when the last handle closes.
    pub fn unlink(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        Self::run_sync_future(self.unlink_inner(parent, name))
    }

    async fn unlink_inner(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        if self.unlink_ignored_entry(parent, name) || self.ignored_dir_attr(parent).is_some() {
            self.bump_namespace_for_parents([parent]);
            self.dentry_cache_insert_negative(parent, name);
            return Ok(());
        }

        let meta = self.sync_meta();

        // First, look up the entry to get the ino so we can check open handles.
        let entry = meta.lookup(parent, name)?;
        let ino = entry.ino;

        let open_handles = self.handle_table.open_count(ino);

        if open_handles > 0 {
            // Open-unlink: atomically remove dentry + decrement nlink, keep inode alive.
            let attr = match meta.unlink_open_handle(parent, name) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => {
                    self.heal_orphaned_dentry(parent, name, Some(ino));
                    return Ok(());
                }
                Err(MetaError::IsADirectory(_)) => {
                    return Err(VfsError::Meta(MetaError::IsADirectory(ino)));
                }
                Err(err) => return Err(VfsError::Meta(err)),
            };
            if attr.nlink == 0 {
                // Open-unlink still removes the final namespace reference right away.
                // Cancel pending uploads immediately while deferred inode cleanup waits
                // for the last open handle to close.
                self.mark_inode_deleted_for_uploads(attr.ino);
            }

            // Mark as orphan — release_fh will do the real cleanup
            self.handle_table.mark_orphan(ino);
            debug!(
                "unlink ino={}: {} open handles, deferred cleanup (orphan)",
                ino, open_handles
            );

            // Invalidate read-through cache (name no longer resolves)
            self.state.namespace.attr_read_cache.lock().remove(ino);
        } else {
            // No open handles — immediate full cleanup via meta.unlink()
            let attr = match meta.unlink(parent, name) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => {
                    self.heal_orphaned_dentry(parent, name, Some(ino));
                    return Ok(());
                }
                Err(err) => return Err(VfsError::Meta(err)),
            };

            if attr.nlink == 0 {
                // Inode fully removed — clean up blocks, caches, pending writes.
                // Keep this call for idempotency (open-unlink path may have already marked).
                self.mark_inode_deleted_for_uploads(attr.ino);

                // Remove all blocks and mark for GC
                let removed_ids = meta.remove_file_blocks(attr.ino)?;
                info!(
                    "unlink ino={}: removed {} block refs, object_ids={:?}",
                    attr.ino,
                    removed_ids.len(),
                    removed_ids,
                );

                // Add to pending deletions (for eventual S3 deletion via background GC)
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);

                for object_id in &removed_ids {
                    meta.add_pending_deletion(*object_id, timestamp)?;
                    debug!("marking object {} for deletion (unlink)", object_id);
                }

                // Decrement USED_SPACE by the file's logical size
                if attr.size > 0 {
                    meta.decr_counter(studiofs_meta::types::counters::USED_SPACE, attr.size)?;
                }

                // Clear pending blocks buffer
                self.clear_pending_blocks_for_inode(attr.ino);

                // Immediately evict all cached blocks for this inode — no need to wait for GC.
                self.evict_inode_from_caches(attr.ino, &removed_ids).await;

                // Clear dirty attrs and read-through cache
                self.clear_inode_incarnation(attr.ino);
                self.state.namespace.dirty_attrs.lock().remove(&attr.ino);
            } else {
                info!(
                    "unlink ino={}: nlink={}, keeping inode alive (hardlink)",
                    attr.ino, attr.nlink
                );
            }

            self.state.namespace.attr_read_cache.lock().remove(attr.ino);
        }
        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_negative(parent, name);

        meta.request_eager_flush();
        Ok(())
    }

    /// Remove a directory.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        if self.unlink_ignored_subtree(parent, name) || self.ignored_dir_attr(parent).is_some() {
            self.bump_namespace_for_parents([parent]);
            self.dentry_cache_insert_negative(parent, name);
            return Ok(());
        }
        let removed = self
            .sync_meta()
            .lookup(parent, name)
            .ok()
            .map(|entry| entry.ino);
        self.sync_meta().rmdir(parent, name)?;
        self.bump_namespace_for_parents([parent]);
        if let Some(removed_ino) = removed {
            self.bump_directory_generation(removed_ino);
        }
        self.dentry_cache_insert_negative(parent, name);
        // Invalidate parent directory's read-through cache (entry count changed)
        self.state.namespace.attr_read_cache.lock().remove(parent);
        self.sync_meta().request_eager_flush();
        Ok(())
    }

    /// Rename a file or directory.
    ///
    /// Uses `rename_atomic()` for a single-transaction metadata update.
    /// If the rename displaces an existing file (overwrite), its blocks
    /// are marked for GC and evicted from all cache tiers.
    pub fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        Self::run_sync_future(self.rename_inner(parent, name, new_parent, new_name))
    }

    async fn rename_inner(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        let meta = self.sync_meta();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.enforce_access(new_parent, AccessLevel::ReadWrite)?;
        if parent == new_parent && name == new_name {
            return Ok(());
        }
        validate_name(new_name)?;
        let source_ignored = self.ignored_entry_lookup(parent, name);
        let target_is_ignored = self.should_ignore_regular_file_path(new_parent, new_name)
            || self.should_ignore_directory_path(new_parent, new_name);
        if let Some(source) = source_ignored {
            if target_is_ignored {
                let _ = self.create_ignored_entry(
                    new_parent,
                    new_name,
                    source.attr.uid,
                    source.attr.gid,
                    source.attr.mode,
                    source.attr.kind,
                );
            }
            self.unlink_ignored_subtree(parent, name);
            self.bump_namespace_for_parents([parent, new_parent]);
            self.bump_acl_namespace_epoch();
            self.dentry_cache_insert_negative(parent, name);
            return Ok(());
        }
        if target_is_ignored {
            let source_entry = meta.lookup(parent, name)?;
            let source_attr = meta.get_attr(source_entry.ino)?;
            let target_is_ignored_trash = self.ignored_dir_attr(new_parent).is_some()
                && self.ignored_directory_has_probable_trash_ancestor(new_parent);
            if target_is_ignored_trash {
                let ignored = self.create_ignored_entry(
                    new_parent,
                    new_name,
                    source_attr.uid,
                    source_attr.gid,
                    source_attr.mode,
                    source_attr.kind,
                );
                let _ = self.update_ignored_entry(ignored.ino, |attr| {
                    attr.size = source_attr.size;
                    attr.committed_size = source_attr.committed_size;
                    attr.atime = source_attr.atime;
                    attr.mtime = source_attr.mtime;
                    attr.ctime = source_attr.ctime;
                    attr.crtime = source_attr.crtime;
                });
                if source_attr.kind == FileType::RegularFile {
                    self.evict_file_cache_only(source_attr.ino).await?;
                }
            }
            self.bump_namespace_for_parents([new_parent]);
            return self.unlink_inner(parent, name).await;
        }
        let rename_result = meta.rename_atomic(parent, name, new_parent, new_name)?;
        self.bump_namespace_for_parents([parent, new_parent]);
        self.bump_acl_namespace_epoch();
        self.dentry_cache_insert_negative(parent, name);
        self.dentry_cache_insert_positive(new_parent, new_name, rename_result.moved_ino);
        let moved_attr = meta.get_attr(rename_result.moved_ino)?;
        let moved_into_trash = moved_attr.kind == FileType::RegularFile
            && self.directory_has_probable_trash_ancestor(new_parent)?;

        // If an existing entry was overwritten, schedule GC + evict caches
        if let Some(ino) = rename_result.displaced_ino {
            self.mark_inode_deleted_for_uploads(ino);

            // Compute total bytes for USED_SPACE before removing blocks
            let blocks = meta.get_file_blocks(ino)?;
            let total_bytes: u64 = blocks.iter().map(|(_, b)| b.length as u64).sum();

            let removed_ids = meta.remove_file_blocks(ino)?;
            if !removed_ids.is_empty() {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                for &object_id in &removed_ids {
                    meta.add_pending_deletion(object_id, timestamp)?;
                }
                self.clear_pending_blocks_for_inode(ino);
                self.evict_inode_from_caches(ino, &removed_ids).await;
            }

            // Decrement USED_SPACE for removed blocks
            if total_bytes > 0 {
                meta.decr_counter(studiofs_meta::types::counters::USED_SPACE, total_bytes)?;
            }

            self.state.namespace.dirty_attrs.lock().remove(&ino);
            self.state.namespace.attr_read_cache.lock().remove(ino);
            self.clear_inode_incarnation(ino);
            self.bump_directory_generation(ino);
            self.dentry_cache_invalidate_parent(ino);
        }

        if moved_into_trash {
            self.evict_file_cache_only(rename_result.moved_ino).await?;
        }

        // Invalidate source and dest parent directory caches
        self.state.namespace.attr_read_cache.lock().remove(parent);
        if parent != new_parent {
            self.state
                .namespace
                .attr_read_cache
                .lock()
                .remove(new_parent);
        }
        meta.request_eager_flush();
        Ok(())
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

    // =========================================================================
    // Cache Eviction Helpers
    // =========================================================================

    /// Immediately evict **all** blocks for `ino` from every cache tier.
    ///
    /// Called by `unlink()` so that deleted file data is no longer occupying
    /// disk / RAM space without waiting for the next background GC run.
    async fn evict_inode_from_caches(&self, ino: Ino, object_ids: &[u64]) {
        // L1.5 — dirty memory buffer: drain blocks buffered post-flush.
        self.clear_pending_blocks_for_inode(ino);

        self.remove_live_extents_for_inode(ino);

        // Committed cache + L1/L2 upload cache: remove each object.
        // Blocks with a pending upload are pinned — schedule deferred eviction
        // so they are cleaned up once the upload completes (or is cancelled by
        // the uploader when it detects the inode is deleted).
        let mut evicted = Vec::new();
        let mut deferred_pinned = Vec::new();
        for &object_id in object_ids {
            if self.block_cache.is_pinned(object_id) {
                // Record for post-upload cleanup instead of silently skipping.
                deferred_pinned.push(object_id);
            } else {
                evicted.push(object_id);
            }
        }
        if !deferred_pinned.is_empty() {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in deferred_pinned.iter() {
                pending_evictions.insert(*object_id);
            }
        }
        if !evicted.is_empty() {
            self.block_cache
                .remove_batch_with_priority(&evicted, L2IoPriority::Background)
                .await;
            if let Err(error) = self.block_store_remove_batch_async(evicted.clone()).await {
                warn!(
                    ino,
                    error = %error,
                    evicted_blocks = evicted.len(),
                        "evict_inode: local backend batch removal failed"
                );
            }
        }
        if !evicted.is_empty() || !deferred_pinned.is_empty() {
            debug!(
                "evict_inode: ino={} evicted {} blocks, deferred pinned={}",
                ino,
                evicted.len(),
                deferred_pinned.len()
            );
        }

        // Invalidate readahead state for this inode.
        self.readahead.evict(ino);
        self.clear_access_tracking_for_inode(ino);
    }

    async fn evict_file_cache_only(&self, ino: Ino) -> VfsResult<()> {
        let object_ids: Vec<u64> = self
            .sync_meta()
            .get_file_blocks(ino)?
            .into_iter()
            .map(|(_, block_ref)| block_ref.object_id)
            .collect();
        let mut evicted = Vec::new();
        let mut deferred_pinned = 0;
        let mut pending_evictions_to_add = Vec::new();
        for object_id in object_ids {
            if self.block_cache.is_pinned(object_id) {
                pending_evictions_to_add.push(object_id);
                deferred_pinned += 1;
                continue;
            }
            evicted.push(object_id);
        }
        if !pending_evictions_to_add.is_empty() {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in pending_evictions_to_add {
                pending_evictions.insert(object_id);
            }
        }
        if !evicted.is_empty() {
            self.block_cache
                .remove_batch_with_priority(&evicted, L2IoPriority::Background)
                .await;
            self.block_store_remove_batch_async(evicted.clone()).await?;
        }
        if !evicted.is_empty() || deferred_pinned > 0 {
            info!(
                "trash-move cache eviction: ino={} evicted {} blocks, deferred pinned={}",
                ino,
                evicted.len(),
                deferred_pinned
            );
        }
        self.readahead.evict(ino);
        self.clear_access_tracking_for_inode(ino);
        Ok(())
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

    fn ignored_directory_has_probable_trash_ancestor(&self, dir_ino: Ino) -> bool {
        let mut current = dir_ino;
        let mut hops = 0usize;
        while current != 0 && hops < 1024 {
            let Some(entry) = self.ignored_entry(current) else {
                break;
            };
            if entry.parent == ROOT_INO && is_probable_trash_component(&entry.name) {
                return true;
            }
            current = entry.parent;
            hops += 1;
        }
        false
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
    // Read Path
    // =========================================================================

    /// Read file data at the given offset.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_ordered(ino, offset, size).await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    async fn read_ordered(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if let Some(attr) = self.ignored_entry_attr(ino) {
            let end = (offset + size as u64).min(attr.size);
            let len = end.saturating_sub(offset) as usize;
            return Ok(Bytes::from(vec![0; len]));
        }
        let attr = self.getattr_async(ino).await?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }
        self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        let end = (offset + size as u64).min(attr.size);
        if offset >= end {
            if size > 0 && offset > 0 {
                debug!(
                    ino,
                    offset,
                    size,
                    attr_size = attr.size,
                    attr_upload_state = ?attr.upload_state,
                    attr_committed_size = attr.committed_size,
                    "read_ordered: early EOF (offset >= attr.size) — possible stale attr"
                );
            }
            return Ok(Bytes::new());
        }
        let effective_size = (end - offset) as usize;
        let mut buf = vec![0u8; effective_size];
        // Reuse low-level read helper directly to avoid nested session-gate
        // acquisition from `read_into()`.
        let written = self.read_into_impl(None, ino, offset, &mut buf).await?;
        buf.truncate(written as usize);
        Ok(Bytes::from(buf))
    }

    /// Read file data directly into a provided buffer (P1.3 — avoid intermediate Bytes allocation).
    ///
    /// Same as `read()` but writes directly to the output buffer, avoiding one memcpy.
    /// Returns the number of bytes written.
    pub async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_into_impl(None, ino, offset, buffer).await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    fn calculate_blocks_needed(
        &self,
        offset: u64,
        buffer_len: usize,
        file_size: u64,
    ) -> Vec<(u32, usize, usize)> {
        let end = (offset + buffer_len as u64).min(file_size);
        if offset >= end {
            return Vec::new();
        }
        let mut blocks_needed: Vec<(u32, usize, usize)> = Vec::new();
        let mut cur = offset;
        while cur < end {
            let idx = block_index_from_offset(cur, self.block_size);
            let off_in = offset_within_block(cur, self.block_size) as usize;
            let space = self.block_size as usize - off_in;
            let to_copy = (end - cur).min(space as u64) as usize;
            blocks_needed.push((idx, off_in, to_copy));
            cur += to_copy as u64;
        }
        blocks_needed
    }

    async fn read_into_impl(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let (attr, resolved_slices) = self
            .resolve_read_metadata_async(ino, offset, buffer.len() as u64)
            .await?;
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
        if self.remote_upload_placeholder_visible(ino, &attr) {
            return Err(VfsError::RemoteContentPending);
        }
        let incarnation_state = self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        let pending_content_epoch = incarnation_state
            .pending_incarnation
            .as_ref()
            .map(|pending| pending.content_epoch);

        let read_end = (offset + buffer.len() as u64).min(attr.size);
        let read_len = read_end.saturating_sub(offset) as usize;
        if read_len == 0 {
            if offset < (offset + buffer.len() as u64) && !buffer.is_empty() {
                // Requested range is beyond attr.size — this is a legitimate
                // EOF response, but warn when the request was not trivially at
                // or past EOF so we can detect attr.size mismatches in logs.
                let req_end = offset.saturating_add(buffer.len() as u64);
                if req_end > 0 && attr.size < req_end {
                    debug!(
                        ino,
                        offset,
                        requested_end = req_end,
                        attr_size = attr.size,
                        attr_upload_state = ?attr.upload_state,
                        attr_committed_size = attr.committed_size,
                        attr_content_epoch = attr.content_epoch,
                        attr_committed_content_epoch = attr.committed_content_epoch,
                        "read_into_impl: returning 0 bytes because attr.size < requested end (possible stale attr.size)"
                    );
                }
            }
            return Ok(0);
        }
        let blocks_needed = self.calculate_blocks_needed(offset, read_len, attr.size);
        let access = self.observe_read_access(fh, ino, offset, buffer.len());
        let mut guard = ForegroundReadScope::new(self, access.policy);
        let mut read_plan = self
            .build_read_plan(
                ino,
                offset,
                read_len as u64,
                pending_content_epoch,
                Some(resolved_slices.as_slice()),
            )
            .await?;
        read_plan.sort_by_key(|span| span.range.start);
        let missing_remote = self
            .prefetch_remote_blocks_for_plan(&read_plan, false)
            .await?;
        let written = self
            .assemble_read_plan(offset, &read_plan, &missing_remote, buffer)
            .await?;
        // Drop the plan as soon as the copy is complete so dirty Bytes do not
        // stay alive across the remaining bookkeeping work.
        drop(read_plan);

        self.enqueue_prefetch_hint(ino, access.stream_id, access.policy, &blocks_needed);
        guard.record_bytes(written as u64);
        Ok(written as u32)
    }

    fn claim_unresolved_ranges(
        unresolved: Vec<ReadRange>,
        candidate: ReadRange,
    ) -> (Vec<ReadRange>, Vec<ReadRange>) {
        let mut next_unresolved = Vec::new();
        let mut claimed = Vec::new();
        for range in unresolved {
            let Some(overlap) = range.intersection(candidate) else {
                next_unresolved.push(range);
                continue;
            };
            if range.start < overlap.start {
                next_unresolved.push(ReadRange {
                    start: range.start,
                    end: overlap.start,
                });
            }
            claimed.push(overlap);
            if overlap.end < range.end {
                next_unresolved.push(ReadRange {
                    start: overlap.end,
                    end: range.end,
                });
            }
        }
        (next_unresolved, claimed)
    }

    async fn build_read_plan(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
        pending_content_epoch: Option<u64>,
        resolved_regular_file_slices: Option<&[studiofs_meta::schema::ResolvedSliceSpan]>,
    ) -> VfsResult<Vec<ReadPlanSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let Some(read_end) = offset.checked_add(length) else {
            return Err(VfsError::Other(format!(
                "read range overflow: ino={ino}, offset={offset}, length={length}"
            )));
        };
        let requested = ReadRange {
            start: offset,
            end: read_end,
        };
        let mut unresolved = vec![requested];
        let mut plan = Vec::new();
        let pending_upload_index = self.snapshot_pending_upload_index();

        let dirty_spans = self.snapshot_pending_dirty_slice_read_spans(ino, offset, length);
        for dirty in dirty_spans {
            if unresolved.is_empty() {
                break;
            }
            let dirty_range = ReadRange {
                start: dirty.start,
                end: dirty.end,
            };
            let (next_unresolved, claimed) = Self::claim_unresolved_ranges(unresolved, dirty_range);
            unresolved = next_unresolved;
            for overlap in claimed {
                let delta = overlap.start.saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                plan.push(ReadPlanSpan {
                    range: overlap,
                    source: ReadPlanSource::Dirty {
                        data: dirty.data.clone(),
                        block_offset,
                    },
                });
            }
        }

        if !unresolved.is_empty() {
            let mut candidate_blocks = BTreeSet::new();
            for range in &unresolved {
                let start_block = block_index_from_offset(range.start, self.block_size);
                let end_block =
                    block_index_from_offset(range.end.saturating_sub(1), self.block_size);
                for block_index in start_block..=end_block {
                    candidate_blocks.insert(block_index);
                }
            }
            for block_index in candidate_blocks {
                if unresolved.is_empty() {
                    break;
                }
                let Some(data) = self
                    .read_pending_overlay_block_async(
                        ino,
                        block_index,
                        pending_content_epoch,
                        Some(&pending_upload_index),
                    )
                    .await?
                else {
                    continue;
                };
                let block_start = (block_index as u64).saturating_mul(self.block_size as u64);
                let block_end = block_start.saturating_add(self.block_size as u64);
                let (next_unresolved, claimed) = Self::claim_unresolved_ranges(
                    unresolved,
                    ReadRange {
                        start: block_start,
                        end: block_end,
                    },
                );
                unresolved = next_unresolved;
                for overlap in claimed {
                    let delta = overlap.start.saturating_sub(block_start);
                    let Some(block_offset) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    plan.push(ReadPlanSpan {
                        range: overlap,
                        source: ReadPlanSource::PendingOverlay {
                            data: data.clone(),
                            block_offset,
                        },
                    });
                }
            }
        }

        if !unresolved.is_empty() {
            if let Some(resolved) = resolved_regular_file_slices {
                unresolved = self.claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |block| ReadPlanSource::Overlay { block },
                );
                unresolved = self.claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Base,
                    |block| ReadPlanSource::Base { block },
                );
            } else {
                let meta = self.meta_async.clone();
                let resolved = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.lookup_regular_file_slices(ino, offset, length)
                })
                .await?;
                unresolved = self.claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |block| ReadPlanSource::Overlay { block },
                );
                unresolved = self.claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Base,
                    |block| ReadPlanSource::Base { block },
                );
            }
        }

        for gap in unresolved {
            plan.push(ReadPlanSpan {
                range: gap,
                source: ReadPlanSource::Zero,
            });
        }
        Ok(plan)
    }

    fn claim_meta_spans_into_plan<'a, I, F>(
        &self,
        resolved: I,
        unresolved: Vec<ReadRange>,
        plan: &mut Vec<ReadPlanSpan>,
        requested: ReadRange,
        source_filter: ResolvedSliceSource,
        source_ctor: F,
    ) -> Vec<ReadRange>
    where
        I: Iterator<Item = &'a studiofs_meta::schema::ResolvedSliceSpan>,
        F: Fn(BlockRef) -> ReadPlanSource + Copy,
    {
        let mut unresolved = unresolved;
        for span in resolved {
            if unresolved.is_empty() {
                break;
            }
            if span.source != source_filter {
                continue;
            }
            let Some(block) = span.block.as_ref() else {
                continue;
            };
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let span_range = ReadRange {
                start: chunk_base.saturating_add(span.range.start as u64),
                end: chunk_base.saturating_add(span.range.end as u64),
            };
            let Some(clipped) = span_range.intersection(requested) else {
                continue;
            };
            let (next_unresolved, claimed) = Self::claim_unresolved_ranges(unresolved, clipped);
            unresolved = next_unresolved;
            for overlap in claimed {
                let delta = overlap.start.saturating_sub(span_range.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = block.offset.checked_add(delta_u32) else {
                    continue;
                };
                let length = overlap.end.saturating_sub(overlap.start);
                let Some(block_length) = u32::try_from(length).ok() else {
                    continue;
                };
                let mut planned_block = block.clone();
                planned_block.offset = block_offset;
                planned_block.length = block_length;
                plan.push(ReadPlanSpan {
                    range: overlap,
                    source: source_ctor(planned_block),
                });
            }
        }
        unresolved
    }

    fn collect_data_block_refs(plan: &[ReadPlanSpan]) -> BTreeMap<u64, BlockRef> {
        let mut block_refs = BTreeMap::new();
        for span in plan {
            let block = match &span.source {
                ReadPlanSource::Overlay { block } | ReadPlanSource::Base { block } => block,
                ReadPlanSource::Dirty { .. }
                | ReadPlanSource::PendingOverlay { .. }
                | ReadPlanSource::Zero => continue,
            };
            block_refs
                .entry(block.object_id)
                .or_insert_with(|| block.clone());
        }
        block_refs
    }

    fn prefetch_read_context_from_attr(&self, ino: Ino, attr: &InodeAttr) -> PrefetchReadContext {
        let incarnation_state = self.refresh_inode_incarnation_state_from_attr(ino, attr);
        PrefetchReadContext {
            size: attr.size,
            pending_content_epoch: incarnation_state
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch),
        }
    }

    fn contiguous_prefetch_block_ranges(block_indices: &mut Vec<u32>) -> Vec<(u32, u32)> {
        block_indices.sort_unstable();
        block_indices.dedup();

        let mut ranges = Vec::new();
        let Some(mut range_start) = block_indices.first().copied() else {
            return ranges;
        };
        let mut range_end = range_start;
        for &block_idx in block_indices.iter().skip(1) {
            if range_end.checked_add(1) == Some(block_idx) {
                range_end = block_idx;
            } else {
                ranges.push((range_start, range_end));
                range_start = block_idx;
                range_end = block_idx;
            }
        }
        ranges.push((range_start, range_end));
        ranges
    }

    async fn fetch_block_for_ref(
        &self,
        block: &BlockRef,
        prefetch: bool,
    ) -> Result<Bytes, crate::fetch::FetchError> {
        let keys = format_object_fragment_keys(&self.data_prefix, block.object_id);
        if prefetch {
            self.fetch_scheduler
                .fetch_block_prefetch(block.object_id, &keys)
                .await
        } else {
            self.fetch_scheduler
                .fetch_block(block.object_id, &keys)
                .await
        }
    }

    async fn prefetch_remote_blocks_for_plan(
        &self,
        plan: &[ReadPlanSpan],
        prefetch: bool,
    ) -> VfsResult<HashSet<u64>> {
        let mut cached = BTreeSet::new();
        let mut missing = BTreeMap::new();
        for (object_id, block) in Self::collect_data_block_refs(plan) {
            if self.local_block_backend.contains(object_id) {
                cached.insert(object_id);
            } else {
                missing.insert(object_id, block);
            }
        }
        for object_id in cached {
            self.local_block_backend.advise_will_need(object_id);
        }
        if missing.is_empty() {
            return Ok(HashSet::new());
        }
        // Prefetch is best-effort warming, so NotFound is recorded for the
        // caller but otherwise ignored here.
        let fetches = missing.into_iter().map(|(object_id, block)| async move {
            match self.fetch_block_for_ref(&block, prefetch).await {
                Ok(_) => Ok::<(u64, bool), VfsError>((object_id, false)),
                Err(crate::fetch::FetchError::NotFound { ref key }) => {
                    warn!(
                        object_id,
                        key,
                        block_offset = block.offset,
                        block_length = block.length,
                        "prefetch_remote_blocks_for_plan: S3 object not found during read prefetch — object missing from S3 (upload not complete or evicted?)"
                    );
                    Ok::<(u64, bool), VfsError>((object_id, true))
                }
                Err(error) => {
                    tracing::debug!(
                        object_id,
                        error = %error,
                        "read planner prefetch failed; falling back to on-demand resolution"
                    );
                    Ok::<(u64, bool), VfsError>((object_id, false))
                }
            }
        });
        let mut not_found = HashSet::new();
        for result in join_all(fetches).await {
            let (object_id, was_not_found) = result?;
            if was_not_found {
                not_found.insert(object_id);
            }
        }
        Ok(not_found)
    }

    fn copy_block_payload_into_range(&self, data: &[u8], block_offset: u32, dst: &mut [u8]) {
        let src_start = block_offset as usize;
        if src_start >= data.len() {
            warn!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                "copy_block_payload_into_range: block_offset >= data.len(), zeroing dst (possible partial-block read bug)"
            );
            dst.fill(0);
            return;
        }
        let copy_len = dst.len().min(data.len().saturating_sub(src_start));
        if copy_len > 0 {
            dst[..copy_len].copy_from_slice(&data[src_start..src_start + copy_len]);
        }
        if copy_len < dst.len() {
            debug!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                copy_len,
                "copy_block_payload_into_range: padding with zeros (partial block or short read)"
            );
            dst[copy_len..].fill(0);
        }
    }

    async fn assemble_read_plan(
        &self,
        offset: u64,
        plan: &[ReadPlanSpan],
        remote_not_found: &HashSet<u64>,
        buffer: &mut [u8],
    ) -> VfsResult<usize> {
        let mut written = 0usize;
        for span in plan {
            let dst_start = span.range.start.saturating_sub(offset) as usize;
            let dst_end = span.range.end.saturating_sub(offset) as usize;
            if dst_start >= buffer.len() || dst_end > buffer.len() || dst_start >= dst_end {
                continue;
            }
            let dst = &mut buffer[dst_start..dst_end];
            match &span.source {
                ReadPlanSource::Dirty { data, block_offset } => {
                    self.copy_block_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadPlanSource::PendingOverlay { data, block_offset } => {
                    self.copy_block_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadPlanSource::Overlay { block } | ReadPlanSource::Base { block } => {
                    if remote_not_found.contains(&block.object_id) {
                        warn!(
                            object_id = block.object_id,
                            file_offset = span.range.start,
                            "assemble_read_plan: object in remote_not_found set, zeroing range (S3 NotFound during prefetch)"
                        );
                        dst.fill(0);
                    } else if let Some(data) = self.block_store_get_async(block.object_id).await? {
                        self.copy_block_payload_into_range(data.as_ref(), block.offset, dst);
                    } else {
                        match self.fetch_block_for_ref(block, false).await {
                            Ok(data) => {
                                self.copy_block_payload_into_range(
                                    data.as_ref(),
                                    block.offset,
                                    dst,
                                );
                            }
                            Err(crate::fetch::FetchError::NotFound { key }) => {
                                warn!(
                                    object_id = block.object_id,
                                    key,
                                    file_offset = span.range.start,
                                    block_offset = block.offset,
                                    block_length = block.length,
                                    "assemble_read_plan: S3 object not found during foreground read, zeroing range (upload incomplete or evicted before upload?)"
                                );
                                dst.fill(0);
                            }
                            Err(error) => {
                                return Err(VfsError::Fetch(error));
                            }
                        }
                    }
                }
                ReadPlanSource::Zero => dst.fill(0),
            }
            written = written.max(dst_end);
        }
        Ok(written)
    }

    /// Enqueue a read hint for background prefetch planning and execution.
    /// Called after successful buffered reads with minimal work on the read path.
    #[inline]
    fn enqueue_prefetch_hint(
        &self,
        ino: Ino,
        stream_id: u64,
        policy: AccessPolicy,
        blocks_needed: &[(u32, usize, usize)],
    ) {
        if self.shutdown_requested() || blocks_needed.is_empty() {
            return;
        }
        if !policy.prefetch_enabled {
            self.state
                .read
                .prefetch_last_hint
                .lock()
                .remove(&(ino, stream_id));
            return;
        }
        let first_block = blocks_needed[0].0;
        let last_block = blocks_needed.last().unwrap().0;
        {
            let mut last_hints = self.state.read.prefetch_last_hint.lock();
            if let Some((prev_first, prev_last)) = last_hints.get(&(ino, stream_id)).copied() {
                // Suppress redundant hints fully covered by the previous range.
                if first_block >= prev_first && last_block <= prev_last {
                    return;
                }
                // Merge overlapping/adjacent ranges to reduce hint churn.
                if first_block <= prev_last.saturating_add(1)
                    && prev_first <= last_block.saturating_add(1)
                {
                    last_hints.insert(
                        (ino, stream_id),
                        (prev_first.min(first_block), prev_last.max(last_block)),
                    );
                } else {
                    last_hints.insert((ino, stream_id), (first_block, last_block));
                }
            } else {
                last_hints.insert((ino, stream_id), (first_block, last_block));
            }
        }
        let _ = self.prefetch_tx.send(PrefetchWorkItem::ReadHint {
            ino,
            stream_id,
            first_block,
            last_block,
            policy,
        });
    }

    /// Compute prefetch blocks from a read hint.
    /// Runs on the background prefetch worker to keep the hot path lean.
    fn plan_prefetch_batches_from_hint(
        &self,
        ino: Ino,
        stream_id: u64,
        first_block: u32,
        last_block: u32,
        policy: AccessPolicy,
    ) -> Vec<PrefetchBatch> {
        if self.shutdown_requested() || !policy.prefetch_enabled || self.readahead.width == 0 {
            return Vec::new();
        }
        // Ignore stale hints for inodes that no longer exist.
        let attr = match self.sync_meta().get_attr(ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => {
                self.readahead.evict(ino);
                self.state
                    .read
                    .prefetch_last_hint
                    .lock()
                    .remove(&(ino, stream_id));
                return Vec::new();
            }
            Err(_) => return Vec::new(),
        };
        let read_context = self.prefetch_read_context_from_attr(ino, &attr);
        let eof_block = if read_context.size == 0 {
            None
        } else {
            Some(((read_context.size.saturating_sub(1)) / self.block_size.max(1) as u64) as u32)
        };
        // Keep ReadAhead state consistent even when prefetch budget is zero.
        let to_prefetch = self.readahead.record_read_for_stream(
            ReadStreamKey { ino, stream_id },
            first_block,
            last_block,
            policy.readahead_policy(),
        );
        // Keep directory-level access state in sync regardless of budget.
        let siblings = self.dir_prefetch.on_child_read(ino);
        // After transient transport failures, pause best-effort prefetch briefly
        // so foreground demand reads can recover the pipeline first.
        if self
            .fetch_scheduler
            .recent_send_error(self.prefetch_pause_on_neterr)
        {
            return Vec::new();
        }
        // Keep headroom for foreground reads: cap best-effort prefetch by
        // current fetch window and in-flight load.
        let window = self.fetch_scheduler.window().floor().max(1.0) as usize;
        let in_flight = self.fetch_scheduler.in_flight();
        let available = window.saturating_sub(in_flight);
        let cycle_cap = self.prefetch_max_per_cycle.max(window / 2).max(1);
        let mut budget = available
            .saturating_sub(self.prefetch_foreground_reserve)
            .min(cycle_cap);
        if budget == 0 {
            return Vec::new();
        }

        let mut primary_blocks = Vec::new();
        for block_idx in to_prefetch {
            if budget == 0 {
                break;
            }
            if let Some(max_block) = eof_block
                && block_idx > max_block
            {
                continue;
            }
            primary_blocks.push(block_idx);
            budget = budget.saturating_sub(1);
        }

        let mut planned = Vec::new();
        if !primary_blocks.is_empty() {
            planned.push(PrefetchBatch::new(ino, primary_blocks, Some(read_context)));
        }

        // Directory-level prefetch: if this child is in a tracked directory
        // and the threshold is met, prefetch block 0 of sibling files.
        for sibling_ino in siblings.into_iter().take(self.dir_prefetch_max_batch) {
            if budget == 0 {
                break;
            }
            planned.push(PrefetchBatch::single(sibling_ino, 0, None));
            budget = budget.saturating_sub(1);
        }
        planned
    }

    async fn get_file_block_async(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> VfsResult<Option<BlockRef>> {
        let meta = self.meta_async.clone();
        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(ino, block_index)
        })
        .await
    }

    /// Prefetch a batch of blocks into cache (called from spawned prefetch tasks).
    ///
    /// Like the foreground read planner, but silently ignores errors and
    /// always marks the readahead inflight state as done when finished.
    async fn read_prefetch_batch(&self, batch: PrefetchBatch) {
        let PrefetchBatch {
            ino,
            mut block_indices,
            read_context,
        } = batch;
        if self.shutdown_requested() {
            self.finish_prefetch_blocks(ino, &block_indices);
            return;
        }

        let result = async {
            block_indices.retain(|block_idx| self.readahead.is_prefetch_relevant(ino, *block_idx));
            if block_indices.is_empty() {
                return Ok(());
            }

            let context = match read_context {
                Some(context) => context,
                None => {
                    let attr = self.getattr_async(ino).await?;
                    if self.remote_upload_placeholder_visible(ino, &attr) {
                        return Ok(());
                    }
                    self.prefetch_read_context_from_attr(ino, &attr)
                }
            };
            if context.size == 0 {
                return Ok(());
            }
            let eof_block =
                ((context.size.saturating_sub(1)) / self.block_size.max(1) as u64) as u32;
            block_indices.retain(|block_idx| *block_idx <= eof_block);
            if block_indices.is_empty() {
                return Ok(());
            }

            for (first_block, last_block) in
                Self::contiguous_prefetch_block_ranges(&mut block_indices)
            {
                let block_start = (first_block as u64).saturating_mul(self.block_size as u64);
                let block_end = ((last_block as u64).saturating_add(1))
                    .saturating_mul(self.block_size as u64)
                    .min(context.size);
                let block_len = block_end.saturating_sub(block_start);
                if block_len == 0 {
                    continue;
                }
                let plan = self
                    .build_read_plan(
                        ino,
                        block_start,
                        block_len,
                        context.pending_content_epoch,
                        None,
                    )
                    .await?;
                // Prefetch is best-effort warming, so we intentionally drop the
                // NotFound set here; foreground reads still resolve the final path.
                let _ = self.prefetch_remote_blocks_for_plan(&plan, true).await?;
            }
            Ok::<(), VfsError>(())
        }
        .await;
        if let Err(error) = result {
            debug!(
                ino,
                block_indices = ?block_indices,
                error = %error,
                "prefetch worker resolver path failed"
            );
        }
        self.finish_prefetch_blocks(ino, &block_indices);
    }

    fn finish_prefetch_blocks(&self, ino: Ino, block_indices: &[u32]) {
        for block_index in block_indices {
            self.readahead.finish_prefetch(ino, *block_index);
        }
    }

    // =========================================================================
    // Write Path
    // =========================================================================

    /// Write data to a file at the given offset.
    ///
    /// Completed full blocks are coalesced into a larger eager-persist batch so
    /// `write()` avoids paying the local durability boundary for every single
    /// block while `flush()` still handles any remaining tail blocks.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Write)
            .await;
        let result = self.write_ordered(ino, offset, data).await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    /// Flush all buffered blocks for a file.
    ///
    /// `write()` may have already coalesced and persisted some full blocks, but
    /// `flush()` is still the definitive "persist everything that is left"
    /// barrier for the inode.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        let session = self
            .enter_file_session_op(ino, FileSessionOperation::Flush)
            .await;
        let result = self
            .flush_ordered(ino, LocalSyncPolicy::Opportunistic)
            .await;
        drop(session);
        self.maybe_gc_file_session(ino);
        result
    }

    // =========================================================================
    // Background Tasks
    // =========================================================================

    /// Background task to process pending uploads.
    async fn background_uploader(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<PendingUpload>,
    ) {
        info!("background uploader started");
        let worker_count = self.upload_worker_count.max(1);

        let dispatch_capacity = worker_count.max(1) * 2;
        let (worker_tx, worker_rx) = tokio::sync::mpsc::channel::<PendingUpload>(dispatch_capacity);
        let worker_rx = std::sync::Arc::new(tokio::sync::Mutex::new(worker_rx));
        let mut workers = JoinSet::new();
        info!(
            upload_worker_count = worker_count,
            dispatch_capacity, "upload worker pool started"
        );

        for _ in 0..worker_count {
            let rx = worker_rx.clone();
            let vfs = self.clone();
            workers.spawn(async move {
                let mut ops_since_yield = 0u32;
                loop {
                    let op = tokio::select! {
                        _ = vfs.state.shutdown.uploader_shutdown_token.cancelled() => None,
                        op = async {
                            let mut rx = rx.lock().await;
                            rx.recv().await
                        } => op,
                    };
                    let Some(op) = op else {
                        break;
                    };
                    if vfs.uploader_shutdown_requested() {
                        break;
                    }

                    match vfs.process_upload(op.clone()).await {
                        Ok(()) => {}
                        Err(VfsError::QosFailure { object_id, .. })
                            if vfs.state.shutdown.uploader_shutdown_token.is_cancelled() =>
                        {
                            debug!(
                                object_id,
                                ino = op.ino,
                                "upload worker qos cancelled during shutdown"
                            );
                            break;
                        }
                        Err(e) => {
                            if vfs.uploader_shutdown_requested() {
                                break;
                            }
                            if Self::is_invalid_regular_file_graph_error(&e) {
                                let attr_size = run_meta(
                                    vfs.meta_async.clone(),
                                    move |engine: Arc<dyn MetaEngine>| engine.get_attr(op.ino),
                                )
                                .await
                                .ok()
                                .map(|attr| attr.size)
                                .unwrap_or(0);
                                warn!(
                                    object_id = op.object_id,
                                    ino = op.ino,
                                    error = %e,
                                    "upload failed due invalid regular-file graph, quarantining inode"
                                );
                                vfs.local_block_backend.mark_upload_failed(op.object_id);
                                vfs.quarantine_invalid_regular_file_graph(
                                    op.ino, attr_size, &e,
                                );
                            } else if vfs.state.upload.deleted_inodes.lock().contains(&op.ino) {
                                // If the inode was deleted, don't retry — cancel the upload.
                                info!(
                                    object_id = op.object_id,
                                    ino = op.ino,
                                    "upload failed for deleted inode, cancelling: {}",
                                    e
                                );
                                if let Err(cancel_error) =
                                    vfs.cancel_upload_for_deleted_inode(&op).await
                                {
                                    warn!(
                                        object_id = op.object_id,
                                        ino = op.ino,
                                        error = %cancel_error,
                                        "upload cancel cleanup failed for deleted inode"
                                    );
                                }
                                vfs.state.upload
                                    .upload_canceled_deleted
                                    .fetch_add(1, Ordering::Relaxed);
                                vfs.maybe_clear_deleted_inode_marker(op.ino);
                            } else {
                                vfs.local_block_backend.mark_upload_failed(op.object_id);
                                vfs.mark_upload_sticky_error(op.ino, &e);
                                let mut retry_op = op;
                                retry_op.retry_count = retry_op.retry_count.saturating_add(1);
                                let delay = vfs.upload_requeue_delay(&retry_op);
                                vfs.state.upload.upload_requeues.fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    object_id = retry_op.object_id,
                                    ino = retry_op.ino,
                                    retry_count = retry_op.retry_count,
                                    delay_ms = delay.as_millis() as u64,
                                    "upload processing error, will requeue: {}",
                                    e
                                );
                                let should_requeue = tokio::select! {
                                    _ = vfs.state.shutdown.uploader_shutdown_token.cancelled() => false,
                                    _ = tokio::time::sleep(delay) => !vfs.uploader_shutdown_requested(),
                                };
                                if should_requeue && !vfs.block_cache.requeue_upload(retry_op) {
                                    error!("failed to requeue upload op after error");
                                }
                            }
                        }
                    }
                    ops_since_yield = ops_since_yield.saturating_add(1);
                    if ops_since_yield >= UPLOAD_WORKER_YIELD_EVERY_OPS {
                        ops_since_yield = 0;
                        tokio::task::yield_now().await;
                    }
                }
            });
        }

        let mut fair: FairInodeScheduler<PendingUpload> = FairInodeScheduler::default();
        let mut dispatched_since_yield = 0u32;
        let mut inbound_closed = false;
        loop {
            if self.uploader_shutdown_requested() || (inbound_closed && fair.is_empty()) {
                break;
            }

            tokio::select! {
                biased;
                _ = self.state.shutdown.uploader_shutdown_token.cancelled() => break,
                permit = worker_tx.reserve(), if !fair.is_empty() => {
                    match permit {
                        Ok(permit) => {
                            if let Some(op) = fair.pop() {
                                debug!(
                                    ino = op.ino,
                                    object_id = op.object_id,
                                    offset = op.offset,
                                    content_epoch = op.content_epoch,
                                    retry_count = op.retry_count,
                                    "uploader dispatch op"
                                );
                                permit.send(op);
                                dispatched_since_yield = dispatched_since_yield.saturating_add(1);
                                if dispatched_since_yield >= UPLOAD_WORKER_YIELD_EVERY_OPS {
                                    dispatched_since_yield = 0;
                                    tokio::task::yield_now().await;
                                }
                            }
                        }
                        Err(_) => break,
                    }
                }
                inbound = rx.recv(), if !inbound_closed => {
                    match inbound {
                        Some(op) => fair.push(op.ino, op),
                        None => inbound_closed = true,
                    }
                }
            }
        }

        drop(worker_tx);
        let join_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if workers.is_empty() {
                break;
            }
            let remaining = join_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                workers.abort_all();
                while workers.join_next().await.is_some() {}
                break;
            }
            match tokio::time::timeout(remaining, workers.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(e))) if e.is_cancelled() => {}
                Ok(Some(Err(e))) => {
                    warn!(error = %e, "uploader worker join failed");
                }
                Ok(None) => break,
                Err(_) => {
                    workers.abort_all();
                    while workers.join_next().await.is_some() {}
                    break;
                }
            }
        }
    }

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
        self.note_file_session_background_transition(op.ino, FileSessionOperation::Upload, "start");
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
            let age_ms = Self::now_unix_millis().saturating_sub(op.created_unix_ms);
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
            self.block_cache.cancel_upload(&op, false);
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
            self.block_cache.cancel_upload(&op, false);
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
            let upload_io_cost = if self.foreground_read_active() {
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
                            self.block_cache.cancel_upload(&op, false);
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
                        self.block_cache.cancel_upload(&op, false);
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
        self.note_file_session_background_transition(
            op.ino,
            FileSessionOperation::Upload,
            "complete",
        );
        Ok(())
    }

    async fn background_upload_stats(self: Arc<Self>) {
        let mut upload_pipeline_summary_was_active = false;
        let mut ticks_since_last_log = 0u64;
        let mut zero_log_counter = 0u8;
        const ZERO_LOG_TARGET: u8 = 3;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.upload_stats_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if let Err(error) = self.block_store_run_pressure_maintenance_async().await {
                warn!(error = %error, "background local backend pressure maintenance failed");
            }
            let pending = self.block_cache.pending_upload_count();
            let pending_upload_bytes = pending as u64 * u64::from(self.block_size);
            let metadata_backlog = self.sync_meta().replication_backlog();
            let pending_metadata_ops = metadata_backlog.total_ops();

            let is_active = Self::upload_pipeline_summary_is_active(pending, pending_metadata_ops);
            let activity_started = is_active && !upload_pipeline_summary_was_active;

            let should_log = if activity_started {
                ticks_since_last_log = 0;
                zero_log_counter = 0;
                true
            } else if is_active {
                ticks_since_last_log += 1;
                ticks_since_last_log >= 10
            } else if upload_pipeline_summary_was_active {
                // Just transitioned from active to inactive
                ticks_since_last_log = 0;
                zero_log_counter = 1;
                true
            } else if zero_log_counter > 0 && zero_log_counter < ZERO_LOG_TARGET {
                // Inactive, but still need to log a few more zero messages
                ticks_since_last_log += 1;
                if ticks_since_last_log >= 10 {
                    zero_log_counter += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            let (p95_ms, max_ms) = self.pending_age_stats_ms();
            let ingress = self.ingress_stats();
            let materialize_batches = self.state.metrics.materialize_batch_stats.snapshot();
            let upload_queue = self.block_cache.cache_pipeline_metrics().upload_queue;
            let foreground_read_protection = self.foreground_read_protection_stats();
            self.adjust_upload_backpressure_from_metrics(
                foreground_read_protection.p90_latency_ns,
                foreground_read_protection.dominant_upload_pressure_boost,
            );
            let dentry_cache = self.dentry_cache_metrics();
            let path_shortcut_cache = self.path_shortcut_cache_metrics();
            let attr_read_cache = self.attr_read_cache_metrics();
            let local_backend_dirty_count = self.local_block_backend.dirty_object_ids().len();
            let transit_memory = self.transit_memory_budget.snapshot();
            let file_sessions = self.file_session_stats();

            if should_log {
                ticks_since_last_log = 0;
                let pending_uploads =
                    Self::format_upload_pipeline_pending_uploads(pending_upload_bytes);
                info!(
                    pending_uploads = %pending_uploads,
                    pending_metadata_ops,
                    "upload pipeline"
                );
            }

            upload_pipeline_summary_was_active = is_active;
            debug!(
                dentry_cache_entries = dentry_cache.occupancy_entries,
                dentry_cache_bytes = dentry_cache.occupancy_bytes,
                path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                attr_read_cache_entries = attr_read_cache.occupancy_entries,
                attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                "upload pipeline caches"
            );
            if !self.upload_pipeline_is_idle(pending, &ingress) {
                debug!(
                    requeues = self.state.upload.upload_requeues.load(Ordering::Relaxed),
                    canceled_deleted = self.state.upload.upload_canceled_deleted.load(Ordering::Relaxed),
                    canceled_stale = self.state.upload.upload_canceled_stale.load(Ordering::Relaxed),
                    foreground_reads_inflight =
                        self.state.read.foreground_reads_inflight.load(Ordering::Relaxed),
                    dirty_mem_blocks = ingress.dirty_mem_blocks,
                    dirty_mem_bytes = ingress.dirty_mem_bytes,
                    spill_records = ingress.spill_records,
                    spill_bytes = ingress.spill_bytes,
                    oldest_spill_age_ms = ingress.oldest_spill_age_ms,
                    forced_spill_blocks = ingress.forced_spill_blocks,
                    drained_mem_blocks = ingress.drained_mem_blocks,
                    drained_mem_bytes = ingress.drained_mem_bytes,
                    drained_spill_blocks = ingress.drained_spill_blocks,
                    drained_spill_bytes = ingress.drained_spill_bytes,
                    checkpoint_persists = ingress.checkpoint_persists,
                    checkpoint_failures = ingress.checkpoint_failures,
                    last_checkpoint_age_ms = ingress.last_checkpoint_age_ms,
                    last_checkpoint_segment_id = ingress.last_checkpoint_segment_id,
                    last_checkpoint_offset = ingress.last_checkpoint_offset,
                    checkpoint_pending_bytes = ingress.checkpoint_pending_bytes,
                    checkpoint_pending_mutations = ingress.checkpoint_pending_mutations,
                    checkpoint_active_segment_lag = ingress.checkpoint_active_segment_lag,
                    checkpoint_live_segments = ingress.checkpoint_live_segments,
                    write_backpressure_wait_ms = ingress.write_backpressure_wait_ms,
                    write_backpressure_events = ingress.write_backpressure_events,
                    fast_backpressure_wait_ms = ingress.fast_backpressure_wait_ms,
                    fast_backpressure_events = ingress.fast_backpressure_events,
                    fast_backpressure_stall_max_ms = ingress.fast_backpressure_stall_max_ms,
                    materialize_batches_total = materialize_batches.batches,
                    materialize_batches_blocks = materialize_batches.blocks,
                    materialize_batches_bytes = materialize_batches.bytes,
                    materialize_batches_inflight = materialize_batches.inflight_batches,
                    materialize_batches_inflight_peak = materialize_batches.inflight_batches_peak,
                    materialize_batch_duration_histogram = ?materialize_batches.duration_histogram.bucket_counts,
                    upload_queue_depth = upload_queue.pending_depth,
                    upload_queue_depth_peak = upload_queue.pending_depth_peak,
                    upload_queue_submitted = upload_queue.submitted,
                    upload_queue_completed = upload_queue.completed,
                    upload_queue_requeued = upload_queue.requeued,
                    upload_queue_canceled = upload_queue.canceled,
                    upload_io_slots_available = self.upload_cache_io_slots.available_permits(),
                    qos_accept_available = self.qos.accept_available(),
                    qos_materialize_available = self.qos.materialize_available(),
                    qos_upload_available = self.qos.upload_available(),
                    qos_commit_available = self.qos.commit_available(),
                    qos_fetch_available = self.qos.fetch_available(),
                    pending_metadata_ops = metadata_backlog.pending_ops,
                    inflight_metadata_ops = metadata_backlog.inflight_ops,
                    dentry_cache_hits = dentry_cache.hits,
                    dentry_cache_hot_hits = dentry_cache.protected_hits,
                    dentry_cache_misses = dentry_cache.misses,
                    dentry_cache_evictions = dentry_cache.evictions,
                    dentry_cache_negative_hits = dentry_cache.negative_hits,
                    dentry_cache_negative_hit_ratio = dentry_cache.negative_hit_ratio(),
                    dentry_cache_hot_hit_ratio = dentry_cache.hot_hit_ratio(),
                    dentry_cache_entries = dentry_cache.occupancy_entries,
                    dentry_cache_bytes = dentry_cache.occupancy_bytes,
                    dentry_cache_hot_entries = dentry_cache.protected_occupancy_entries,
                    dentry_cache_hot_bytes = dentry_cache.protected_occupancy_bytes,
                    dentry_cache_entry_budget = dentry_cache.entry_budget,
                    dentry_cache_byte_budget = dentry_cache.byte_budget,
                    path_shortcut_cache_hits = path_shortcut_cache.hits,
                    path_shortcut_cache_hot_hits = path_shortcut_cache.protected_hits,
                    path_shortcut_cache_misses = path_shortcut_cache.misses,
                    path_shortcut_cache_evictions = path_shortcut_cache.evictions,
                    path_shortcut_cache_negative_hits = path_shortcut_cache.negative_hits,
                    path_shortcut_cache_negative_hit_ratio =
                        path_shortcut_cache.negative_hit_ratio(),
                    path_shortcut_cache_hot_hit_ratio = path_shortcut_cache.hot_hit_ratio(),
                    path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                    path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                    path_shortcut_cache_hot_entries =
                        path_shortcut_cache.protected_occupancy_entries,
                    path_shortcut_cache_hot_bytes = path_shortcut_cache.protected_occupancy_bytes,
                    path_shortcut_cache_entry_budget = path_shortcut_cache.entry_budget,
                    path_shortcut_cache_byte_budget = path_shortcut_cache.byte_budget,
                    attr_read_cache_hits = attr_read_cache.hits,
                    attr_read_cache_hot_hits = attr_read_cache.protected_hits,
                    attr_read_cache_misses = attr_read_cache.misses,
                    attr_read_cache_evictions = attr_read_cache.evictions,
                    attr_read_cache_hot_hit_ratio = attr_read_cache.hot_hit_ratio(),
                    attr_read_cache_entries = attr_read_cache.occupancy_entries,
                    attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                    attr_read_cache_hot_entries = attr_read_cache.protected_occupancy_entries,
                    attr_read_cache_hot_bytes = attr_read_cache.protected_occupancy_bytes,
                    attr_read_cache_entry_budget = attr_read_cache.entry_budget,
                    attr_read_cache_byte_budget = attr_read_cache.byte_budget,
                    "upload pipeline counters"
                );
                debug!(
                    file_session_active = file_sessions.active_sessions,
                    file_session_open_handles = file_sessions.open_handles,
                    file_session_queued_ops = file_sessions.queued_ops,
                    file_session_max_queue_depth = file_sessions.max_queue_depth,
                    file_session_wait_events = file_sessions.wait_events,
                    file_session_wait_ms = file_sessions.wait_ms,
                    file_session_transitions = file_sessions.transitions,
                    "file session counters"
                );
                debug!(
                    file_session_wait_reasons = ?file_sessions.wait_reasons,
                    "file session queue wait reasons"
                );
                debug!(
                    upload_fallback_deferrals =
                        foreground_read_protection.upload_fallback_deferrals,
                    upload_fallback_rereads = foreground_read_protection.upload_fallback_rereads,
                    upload_fallback_reread_bytes =
                        foreground_read_protection.upload_fallback_reread_bytes,
                    upload_fallback_reread_cache_misses =
                        foreground_read_protection.upload_fallback_reread_cache_misses,
                    upload_fallback_wait_ms =
                        foreground_read_protection.upload_fallback_wait_ns / 1_000_000,
                    upload_fallback_wait_max_ms =
                        foreground_read_protection.upload_fallback_wait_max_ns / 1_000_000,
                    upload_fallback_read_ms =
                        foreground_read_protection.upload_fallback_read_ns / 1_000_000,
                    upload_fallback_read_max_ms =
                        foreground_read_protection.upload_fallback_read_max_ns / 1_000_000,
                    "upload pipeline local reread path"
                );
                debug!(
                    transit_memory_budget_bytes = transit_memory.budget_bytes,
                    transit_memory_used_bytes = transit_memory.used_bytes,
                    transit_memory_available_bytes = transit_memory.available_bytes,
                    transit_memory_waiting_foreground = transit_memory.waiting_foreground,
                    transit_memory_waiting_background = transit_memory.waiting_background,
                    "upload pipeline transit memory"
                );
                debug!(
                    local_backend_dirty_count,
                    prefetch_skips = self
                        .fetch_scheduler
                        .metrics
                        .prefetch_skips
                        .load(Ordering::Relaxed),
                    "local backend and fetch counters"
                );
                debug!(
                    upload_io_foreground_cost_base = self.upload_cache_io_foreground_cost_base,
                    upload_io_foreground_cost_step = self.upload_cache_io_foreground_cost_step,
                    upload_io_foreground_cost_dynamic =
                        foreground_read_protection.upload_cache_io_foreground_cost,
                    foreground_read_p90_present =
                        foreground_read_protection.p90_latency_ns.is_some(),
                    foreground_read_p90_ms = foreground_read_protection
                        .p90_latency_ns
                        .map(|ns| ns.saturating_div(1_000_000))
                        .unwrap_or(0),
                    foreground_read_speedometer_samples =
                        foreground_read_protection.speedometer_samples,
                    foreground_read_window_bytes = foreground_read_protection.window_bytes,
                    foreground_read_window_coverage_ms =
                        foreground_read_protection.window_coverage_ms,
                    foreground_read_throughput_bytes_per_sec =
                        foreground_read_protection.throughput_bytes_per_sec,
                    foreground_read_target_ms = self
                        .upload_fg_read_latency_target_ns
                        .saturating_div(1_000_000),
                    upload_scheduler_penalty_ms =
                        foreground_read_protection.upload_scheduler_penalty_ms,
                    "upload pipeline adaptive throttle"
                );
            }
            self.upload_scheduler.log_stats(pending, p95_ms, max_ms);
        }
    }

    /// Background task for garbage collection.
    async fn background_gc(self: Arc<Self>) {
        info!("background GC started");

        let interval = tokio::time::Duration::from_secs(600); // 10 minutes
        let grace_period = 3600i64; // 1 hour grace period

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            let meta = self.meta_async.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "background GC skipped: janitor PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!("background GC skipped: janitor PG lock not acquired");
                continue;
            }

            const CHUNK_COMPACTION_BATCH_LIMIT: usize = 32;
            let meta = self.meta_async.clone();
            match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.compact_fragmented_regular_file_chunks(CHUNK_COMPACTION_BATCH_LIMIT)
            })
            .await
            {
                Ok(compacted_chunks) if compacted_chunks > 0 => {
                    info!(
                        compacted_chunks,
                        "background GC compacted fragmented regular-file chunk rows"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "background GC chunk compaction failed"),
            }

            let meta = self.meta_async.clone();
            let referenced_object_ids = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta_async.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        error = %e,
                        "background GC skipped: metadata reachability query failed"
                    );
                    continue;
                }
            };

            // Get current timestamp
            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);

            // Get pending deletions
            let meta = self.meta_async.clone();
            match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_pending_deletions()
            })
            .await
            {
                Ok(pending) => {
                    let mut deleted_count = 0;
                    for (object_id, timestamp) in pending {
                        if referenced_object_ids.contains(&object_id) {
                            debug!(object_id, "skipping object - still reachable in metadata");
                            continue;
                        }

                        // Check if grace period has passed
                        if now - timestamp < grace_period {
                            debug!("skipping object {} - still in grace period", object_id);
                            continue;
                        }

                        // Delete all cloud fragments for the logical object.
                        let mut delete_failed = false;
                        for key in format_object_fragment_keys(&self.data_prefix, object_id) {
                            if let Err(e) = self.data_write.delete_by_key(&key).await {
                                if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                                {
                                    continue;
                                }
                                delete_failed = true;
                                warn!(
                                    object_id,
                                    key,
                                    error = %e,
                                    "failed to delete object fragment from S3"
                                );
                            }
                        }
                        if !delete_failed {
                            // Remove from pending deletions
                            let meta = self.meta_async.clone();
                            if let Err(e) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                                engine.remove_pending_deletion(object_id)?;
                                Ok(())
                            })
                            .await
                            {
                                warn!("failed to remove pending deletion for {}: {}", object_id, e);
                            }
                            deleted_count += 1;
                            debug!("GC deleted object {}", object_id);
                        }
                    }
                    if deleted_count > 0 {
                        info!("GC deleted {} objects", deleted_count);
                    }
                }
                Err(e) => {
                    let meta = self.meta_async.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    error!("failed to get pending deletions: {}", e);
                }
            }

            let meta = self.meta_async.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(error = %e, "background GC failed to release janitor PG lock");
            }
        }
    }

    /// Background janitor for orphaned objects:
    /// - scans all objects under data_prefix
    /// - compares with currently referenced block object_ids in metadata
    /// - deletes unreferenced objects older than grace window
    async fn background_janitor(self: Arc<Self>) {
        info!(
            interval_secs = self.janitor_interval.as_secs(),
            grace_secs = self.janitor_skip_newer_than.as_secs(),
            max_deletes = self.janitor_max_deletes_per_run,
            "background janitor started"
        );
        let mut cycle: u64 = 0;

        loop {
            cycle = cycle.saturating_add(1);
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.janitor_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            if let Some(lease) = &self.janitor_lease
                && !self.acquire_janitor_lease(lease).await
            {
                debug!(
                    cycle,
                    lock_path = lease.lock_path,
                    "janitor cycle skipped: lease not acquired"
                );
                continue;
            }

            let meta = self.meta_async.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle skipped: PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!(cycle, "janitor cycle skipped: PG lock not acquired");
                continue;
            }

            let meta = self.meta_async.clone();
            let global_snapshot_pins = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta_async.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle skipped: snapshot pin query failed");
                    continue;
                }
            };

            let meta = self.meta_async.clone();
            let referenced = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta_async.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                    continue;
                }
            };
            let meta = self.meta_async.clone();
            let pending_map: HashMap<u64, i64> =
                match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.get_pending_deletions()
                })
                .await
                {
                    Ok(v) => v.into_iter().collect(),
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                        HashMap::new()
                    }
                };

            let meta = self.meta_async.clone();
            let recovered_undo_parents = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.recover_pending_creates()
            })
            .await
            {
                Ok(parents) => parents,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: pending-create replay failed");
                    Vec::new()
                }
            };
            if !recovered_undo_parents.is_empty() {
                for parent in recovered_undo_parents.iter().copied() {
                    self.dentry_cache_invalidate_parent(parent);
                }
                self.bump_namespace_for_parents(recovered_undo_parents.iter().copied());
            }
            let meta = self.meta_async.clone();
            let (undo_total, undo_started, undo_replaying, undo_completed) =
                match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.list_undo_logs()
                })
                .await
                {
                    Ok(records) => {
                        let mut started = 0usize;
                        let mut replaying = 0usize;
                        let mut completed = 0usize;
                        for record in records.iter() {
                            match record.phase {
                                studiofs_meta::engine::UndoLogPhase::Started => {
                                    started = started.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Replaying => {
                                    replaying = replaying.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Completed => {
                                    completed = completed.saturating_add(1)
                                }
                            }
                        }
                        (records.len(), started, replaying, completed)
                    }
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read undo-log metrics");
                        (0usize, 0usize, 0usize, 0usize)
                    }
                };

            let entries = match self
                .data_write
                .operator()
                .list_with(&self.data_prefix)
                .recursive(true)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta_async.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        cycle,
                        prefix = self.data_prefix,
                        error = %e,
                        "janitor cycle failed: list data prefix"
                    );
                    continue;
                }
            };

            let now = SystemTime::now();
            let now_unix = now
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let mut scanned: u64 = 0;
            let mut orphan_candidates: u64 = 0;
            let mut deleted: u64 = 0;
            let mut skipped_young: u64 = 0;
            let mut skipped_unknown_age: u64 = 0;
            let mut skipped_cap: u64 = 0;
            let mut skipped_non_object_key: u64 = 0;
            let mut delete_errors: u64 = 0;
            let mut seen_objects: HashSet<u64> = HashSet::new();

            for entry in entries {
                if entry.metadata().mode() != EntryMode::FILE {
                    continue;
                }
                scanned = scanned.saturating_add(1);
                let path = entry.path();
                let object_id = match parse_object_id_from_key(path) {
                    Ok(id) => id,
                    Err(_) => {
                        skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                        continue;
                    }
                };
                if !seen_objects.insert(object_id) {
                    continue;
                }
                if referenced.contains(&object_id) {
                    continue;
                }
                if global_snapshot_pins.contains(&object_id) {
                    continue;
                }
                orphan_candidates = orphan_candidates.saturating_add(1);
                if deleted >= self.janitor_max_deletes_per_run {
                    skipped_cap = skipped_cap.saturating_add(1);
                    continue;
                }

                let mut is_old_enough = false;
                if let Some(ts) = pending_map.get(&object_id) {
                    let age = now_unix.saturating_sub(*ts) as u64;
                    if age >= self.janitor_skip_newer_than.as_secs() {
                        is_old_enough = true;
                    }
                } else if let Some(last_modified) = entry.metadata().last_modified() {
                    let last_modified_st: SystemTime = last_modified.into();
                    let age = now
                        .duration_since(last_modified_st)
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    if age >= self.janitor_skip_newer_than {
                        is_old_enough = true;
                    }
                } else {
                    skipped_unknown_age = skipped_unknown_age.saturating_add(1);
                }

                if !is_old_enough {
                    skipped_young = skipped_young.saturating_add(1);
                    continue;
                }

                let mut delete_failed = false;
                for key in format_object_fragment_keys(&self.data_prefix, object_id) {
                    if let Err(e) = self.data_write.operator().delete(&key).await {
                        if e.kind() == ErrorKind::NotFound {
                            continue;
                        }
                        delete_failed = true;
                        warn!(cycle, key, error = %e, "janitor: fragment delete failed");
                    }
                }
                if delete_failed {
                    delete_errors = delete_errors.saturating_add(1);
                    continue;
                }
                deleted = deleted.saturating_add(1);
            }

            debug!(
                cycle,
                scanned,
                referenced = referenced.len(),
                snapshot_pins = global_snapshot_pins.len(),
                orphan_candidates,
                recovered_undo_parents = recovered_undo_parents.len(),
                undo_total,
                undo_started,
                undo_replaying,
                undo_completed,
                deleted,
                skipped_young,
                skipped_unknown_age,
                skipped_cap,
                skipped_non_object_key,
                delete_errors,
                "janitor cycle complete"
            );

            let meta = self.meta_async.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(cycle, error = %e, "janitor cycle failed to release PG lock");
            }
        }
    }

    /// Persist the cache checkpoint to disk.
    ///
    /// Called at shutdown so the LRU state survives across remounts.
    pub async fn persist_cache(&self) {
        self.block_cache.shutdown().await;
        if let Err(error) = self.local_block_backend.sync_data() {
            warn!(error = %error, "raw backend sync failed during shutdown");
        } else {
            tracing::info!("raw backend sync persisted");
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
            self.decrement_active_access_class(class);
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
            warn!(
                "VFS shutdown: failed to remove legacy ingress artifacts: {}",
                e
            );
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
    use studiofs_meta::engine::MetaFsOps;
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

    fn make_test_vfs(
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
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
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
    fn open_local_block_backend_detection_errors_map_to_structured_vfs_error() {
        let source = OpenLocalBlockBackendError::DetectCacheBackend {
            cache_root: "ignored".to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated permission error",
            ),
        };
        let mapped = map_open_local_block_backend_error("E:\\cache-root", source);
        match mapped {
            VfsError::CacheBackendDetection { cache_root, source } => {
                assert_eq!(cache_root, "E:\\cache-root");
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn studio_vfs_new_selects_cache_v3_placeholder_backend_for_v3_only_layout() {
        let tmp = TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("cache-v3-bootstrap");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let layout = studiofs_cache::v3::CacheV3Layout::from_cache_root(&cache_dir);
        std::fs::create_dir_all(&layout.v3_root).expect("create v3 root");
        std::fs::write(&layout.layout_marker, b"v1").expect("create v3 marker");

        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");

        assert!(
            vfs.local_block_backend()
                .as_any()
                .is::<studiofs_cache::v3::CacheV3LocalBlockBackend>(),
            "v3-only layout should select the cache-v3 placeholder backend"
        );
    }

    #[test]
    fn studio_vfs_new_with_force_cache3_selects_cache_v3_on_dual_layout() {
        let tmp = TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("cache-v3-force");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let layout = studiofs_cache::v3::CacheV3Layout::from_cache_root(&cache_dir);
        std::fs::create_dir_all(&layout.v3_root).expect("create v3 root");
        std::fs::write(&layout.layout_marker, b"v1").expect("create v3 marker");
        std::fs::write(cache_dir.join("blocks.raw"), b"legacy").expect("create legacy raw");

        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();
        let vfs = StudioVfs::new_with_backend_selection(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
            LocalBackendSelectionMode::ForceCacheV3,
        )
        .expect("create vfs");

        assert!(
            vfs.local_block_backend()
                .as_any()
                .is::<studiofs_cache::v3::CacheV3LocalBlockBackend>(),
            "forced cache-v3 mode should select cache-v3 backend on dual layout"
        );
    }

    #[test]
    fn studio_vfs_new_does_not_spawn_background_workers() {
        let tmp = TempDir::new().expect("tempdir");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info("new-no-start");
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);
        let cache_dir = tmp.path().join("cache");
        let rt = Runtime::new().expect("runtime");
        let _guard = rt.enter();

        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");

        let startup = vfs.startup.lifecycle.lock();
        assert!(
            matches!(startup.status, StartupStatus::Uninitialized),
            "constructor should keep startup lifecycle uninitialized"
        );
        assert!(
            vfs.state.shutdown.background_tasks.lock().is_empty(),
            "constructor should not spawn background tasks"
        );
        assert!(
            vfs.state.shutdown.uploader_task.lock().is_none(),
            "constructor should not spawn uploader task"
        );
        drop(startup);
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
        assert_eq!(derive_upload_cache_io_foreground_cost_base(1), 1);
        assert_eq!(derive_upload_cache_io_foreground_cost_base(8), 2);
        assert_eq!(derive_upload_cache_io_foreground_cost_base(32), 8);
        assert_eq!(derive_upload_cache_io_foreground_cost_step(1), 1);
        assert_eq!(derive_upload_cache_io_foreground_cost_step(8), 1);
        assert_eq!(derive_upload_cache_io_foreground_cost_step(32), 4);
    }

    #[test]
    fn cache_runtime_tuning_scales_with_cache_budget() {
        let small = derive_cache_runtime_tuning(
            64 * 1024 * 1024 * 1024,
            16 * 1024 * 1024,
            512 * 1024 * 1024,
        );
        let large = derive_cache_runtime_tuning(
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
    fn collect_data_block_refs_deduplicates_by_object_id() {
        let first = BlockRef {
            object_id: 42,
            offset: 0,
            length: 16,
            flags: MetaBlockFlags::empty(),
        };
        let second = BlockRef {
            object_id: 42,
            offset: 8,
            length: 8,
            flags: MetaBlockFlags::PARTIAL_BLOCK,
        };
        let plan = vec![
            ReadPlanSpan {
                range: ReadRange { start: 0, end: 16 },
                source: ReadPlanSource::Base {
                    block: first.clone(),
                },
            },
            ReadPlanSpan {
                range: ReadRange { start: 16, end: 24 },
                source: ReadPlanSource::Overlay {
                    block: second.clone(),
                },
            },
            ReadPlanSpan {
                range: ReadRange { start: 24, end: 32 },
                source: ReadPlanSource::Zero,
            },
        ];

        let refs = StudioVfs::collect_data_block_refs(&plan);
        assert_eq!(refs.len(), 1);
        let block = refs.get(&42).expect("collected block ref");
        assert_eq!(block, &first, "first ref should win when object ids match");
    }

    #[test]
    fn contiguous_prefetch_block_ranges_sort_and_split_sparse_batches() {
        let mut blocks = vec![9, 1, 3, 2, 8, 7, 7];

        let ranges = StudioVfs::contiguous_prefetch_block_ranges(&mut blocks);

        assert_eq!(blocks, vec![1, 2, 3, 7, 8, 9]);
        assert_eq!(ranges, vec![(1, 3), (7, 9)]);
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
        StudioVfs::trim_foreground_latency_samples(&mut samples, 400, Duration::from_millis(150));
        let kept: Vec<(u64, u64, u64)> = samples
            .into_iter()
            .map(|sample| (sample.timestamp_ms, sample.latency_ns, sample.bytes_read))
            .collect();
        assert_eq!(kept, vec![(350, 30, 4_096)]);
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
    fn test_file_session_wait_reasons_and_transitions_are_observable() {
        let tmp = tempfile::tempdir().unwrap();
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "file-session-observability");
        let _guard = rt.enter();
        rt.block_on(async {
            let attr = vfs
                .create(1, b"session-observability.txt", 0, 0, 0o644)
                .unwrap();
            let ino = attr.ino;
            let _pin = vfs.file_session_for_inode(ino);

            let session_guard = vfs
                .enter_file_session_op(ino, FileSessionOperation::Write)
                .await;
            let vfs_clone = vfs.clone();
            let waiter = tokio::spawn(async move {
                let _queued = vfs_clone
                    .enter_file_session_op(ino, FileSessionOperation::Read)
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
                .enter_file_session_op(ino, FileSessionOperation::Read)
                .await;
            let vfs_clone = vfs.clone();
            let second_reader = tokio::spawn(async move {
                let _second = vfs_clone
                    .enter_file_session_op(ino, FileSessionOperation::Read)
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

    // =========================================================================
    // HandleTable Unit Tests
    // =========================================================================

    #[test]
    fn test_handle_table_open_close() {
        let ht = HandleTable::new();
        let fh1 = ht.open(10, OpenFlags::RDONLY);
        let fh2 = ht.open(10, OpenFlags::RDWR);
        let fh3 = ht.open(20, OpenFlags::WRONLY);

        assert_ne!(fh1.fh, fh2.fh);
        assert_ne!(fh2.fh, fh3.fh);

        // Open count
        assert_eq!(ht.open_count(10), 2);
        assert_eq!(ht.open_count(20), 1);
        assert_eq!(ht.open_count(99), 0);

        // Get
        let of1 = ht.get(fh1.fh).unwrap();
        assert_eq!(of1.ino, 10);
        assert!(of1.flags.contains(OpenFlags::RDONLY));

        let of3 = ht.get(fh3.fh).unwrap();
        assert_eq!(of3.ino, 20);

        assert!(ht.get(99999).is_none());

        // Close fh1 — ino 10 still has 1 open
        let (closed1, remaining) = ht.close(fh1.fh).unwrap();
        assert_eq!(closed1.ino, 10);
        assert_eq!(remaining, 1);
        assert_eq!(ht.open_count(10), 1);

        // Close fh2 — ino 10 now at 0
        let (closed2, remaining) = ht.close(fh2.fh).unwrap();
        assert_eq!(closed2.ino, 10);
        assert_eq!(remaining, 0);
        assert_eq!(ht.open_count(10), 0);

        // Double-close returns None
        assert!(ht.close(fh1.fh).is_none());
    }

    #[test]
    fn test_handle_table_orphans() {
        let ht = HandleTable::new();
        let _fh = ht.open(42, OpenFlags::RDWR);

        // Not an orphan yet
        assert!(!ht.take_orphan(42));

        // Mark as orphan
        ht.mark_orphan(42);

        // take_orphan returns true and clears it
        assert!(ht.take_orphan(42));
        assert!(!ht.take_orphan(42)); // second call returns false
    }

    #[test]
    fn test_open_flags_bitwise() {
        let f = OpenFlags::RDWR | OpenFlags::APPEND;
        assert!(f.contains(OpenFlags::RDWR));
        assert!(f.contains(OpenFlags::APPEND));
        assert!(!f.contains(OpenFlags::WRONLY));

        let f2 = f.union(OpenFlags::WRONLY);
        assert!(f2.contains(OpenFlags::WRONLY));
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
    fn dentry_cache_is_bounded_and_supports_negative_entries() {
        let single_charge = DentryCache::estimate_entry_charge(b"a");
        let mut cache = DentryCache::new(2, single_charge * 4);

        cache.put(ROOT_INO, b"a", DentryCacheValue::Positive(11), 0, 0);
        cache.put(ROOT_INO, b"b", DentryCacheValue::Negative, 0, 0);
        assert_eq!(cache.entry_len(), 2);
        assert_eq!(
            cache.get(ROOT_INO, b"a", 0, 0),
            Some(DentryCacheValue::Positive(11))
        );
        assert_eq!(
            cache.get(ROOT_INO, b"b", 0, 0),
            Some(DentryCacheValue::Negative)
        );

        cache.put(ROOT_INO, b"c", DentryCacheValue::Positive(33), 0, 0);
        assert_eq!(cache.entry_len(), 2);
        assert_eq!(cache.get(ROOT_INO, b"a", 0, 0), None);
        assert_eq!(
            cache.get(ROOT_INO, b"b", 0, 0),
            Some(DentryCacheValue::Negative)
        );
        assert_eq!(
            cache.get(ROOT_INO, b"c", 0, 0),
            Some(DentryCacheValue::Positive(33))
        );
    }

    #[test]
    fn dentry_cache_enforces_byte_budget_under_scan() {
        let charge = DentryCache::estimate_entry_charge(b"k0000");
        let mut cache = DentryCache::new(1024, charge * 32);

        for i in 0..2000usize {
            let key = format!("k{i:04}");
            cache.put(
                ROOT_INO,
                key.as_bytes(),
                DentryCacheValue::Positive(i as u64),
                0,
                0,
            );
        }

        let metrics = cache.metrics();
        assert!(metrics.occupancy_entries <= metrics.entry_budget);
        assert!(metrics.occupancy_bytes <= metrics.byte_budget);
    }

    #[test]
    fn dentry_cache_tracks_hits_misses_and_negative_ratio() {
        let charge = DentryCache::estimate_entry_charge(b"a");
        let mut cache = DentryCache::new(1, charge * 8);

        assert_eq!(cache.get(ROOT_INO, b"missing", 0, 0), None);
        cache.put(ROOT_INO, b"a", DentryCacheValue::Negative, 0, 0);
        assert_eq!(
            cache.get(ROOT_INO, b"a", 0, 0),
            Some(DentryCacheValue::Negative)
        );
        cache.put(ROOT_INO, b"b", DentryCacheValue::Positive(22), 0, 0);

        let metrics = cache.metrics();
        assert_eq!(metrics.hits, 1);
        assert_eq!(metrics.misses, 1);
        assert_eq!(metrics.negative_hits, 1);
        assert_eq!(metrics.evictions, 1);
        assert!((metrics.negative_hit_ratio() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn dentry_cache_tracks_hotset_occupancy_and_hits() {
        let charge = DentryCache::estimate_entry_charge(b"hot");
        let mut cache = DentryCache::new(8, charge * 64);
        cache.put(ROOT_INO, b"hot", DentryCacheValue::Positive(1), 0, 0);
        let _ = cache.get(ROOT_INO, b"hot", 0, 0);
        let _ = cache.get(ROOT_INO, b"hot", 0, 0);

        let metrics = cache.metrics();
        assert!(metrics.protected_occupancy_entries >= 1);
        assert!(metrics.protected_hits >= 1);
        assert!(metrics.hot_hit_ratio() > 0.0);
    }

    #[test]
    fn dentry_cache_compacts_lazy_orders_without_budget_pressure() {
        let charge = DentryCache::estimate_entry_charge(b"hot");
        let mut cache = DentryCache::new(8, charge * 128);
        cache.put(ROOT_INO, b"hot", DentryCacheValue::Positive(1), 0, 0);

        for _ in 0..5000 {
            assert_eq!(
                cache.get(ROOT_INO, b"hot", 0, 0),
                Some(DentryCacheValue::Positive(1))
            );
        }

        assert!(
            cache.protected_order_len() <= DENTRY_CACHE_ORDER_COMPACT_SLACK + 2,
            "protected lazy queue should stay compact under steady-state hits"
        );
    }

    #[test]
    fn dentry_cache_key_clone_is_refcounted() {
        let key = DentryCacheKey::new(ROOT_INO, b"shared-name");
        let cloned = key.clone();
        assert!(
            Arc::ptr_eq(&key.name, &cloned.name),
            "key clone should not duplicate name bytes"
        );
    }

    #[test]
    fn dentry_cache_rejects_stale_namespace_tokens_lazily() {
        let charge = DentryCache::estimate_entry_charge(b"entry");
        let mut cache = DentryCache::new(8, charge * 32);
        cache.put(ROOT_INO, b"entry", DentryCacheValue::Positive(7), 1, 4);

        assert_eq!(
            cache.get(ROOT_INO, b"entry", 1, 4),
            Some(DentryCacheValue::Positive(7))
        );
        assert_eq!(cache.get(ROOT_INO, b"entry", 2, 4), None);
        assert_eq!(
            cache.get(ROOT_INO, b"entry", 2, 4),
            None,
            "stale entry should be removed on first stale hit"
        );
        assert_eq!(
            cache.metrics().evictions,
            0,
            "stale invalidation should not count as eviction"
        );
    }

    #[test]
    fn path_shortcut_cache_is_bounded_and_supports_negative_entries() {
        let key_a = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("b")],
        )
        .expect("path key a");
        let key_b = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("missing")],
        )
        .expect("path key b");
        let tokens = vec![PathShortcutParentToken {
            parent: ROOT_INO,
            generation: 0,
        }];
        let single_charge = PathShortcutCache::estimate_entry_charge(
            &key_a,
            &PathShortcutCacheValue::Positive(11),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(2, single_charge * 4);

        cache.put(
            key_a.clone(),
            PathShortcutCacheValue::Positive(11),
            0,
            tokens.clone(),
        );
        cache.put(
            key_b.clone(),
            PathShortcutCacheValue::Negative {
                parent: ROOT_INO,
                name: Arc::from(&b"missing"[..]),
            },
            0,
            tokens.clone(),
        );
        assert_eq!(cache.entry_len(), 2);
        assert!(matches!(
            cache.get(&key_a, 0, |_| 0),
            Some(PathShortcutCacheValue::Positive(11))
        ));
        assert!(matches!(
            cache.get(&key_b, 0, |_| 0),
            Some(PathShortcutCacheValue::Negative { .. })
        ));

        let key_c = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("a"), String::from("c")],
        )
        .expect("path key c");
        cache.put(
            key_c.clone(),
            PathShortcutCacheValue::Positive(33),
            0,
            tokens,
        );
        assert_eq!(cache.entry_len(), 2);
        assert!(cache.get(&key_c, 0, |_| 0).is_some());
    }

    #[test]
    fn path_shortcut_cache_rejects_stale_parent_generations() {
        let key = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[
                String::from("a"),
                String::from("b"),
                String::from("file.txt"),
            ],
        )
        .expect("path key");
        let tokens = vec![
            PathShortcutParentToken {
                parent: ROOT_INO,
                generation: 1,
            },
            PathShortcutParentToken {
                parent: 42,
                generation: 7,
            },
        ];
        let charge = PathShortcutCache::estimate_entry_charge(
            &key,
            &PathShortcutCacheValue::Positive(9),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(8, charge * 8);
        cache.put(key.clone(), PathShortcutCacheValue::Positive(9), 3, tokens);

        assert!(
            cache
                .get(&key, 3, |ino| if ino == 42 { 7 } else { 1 })
                .is_some()
        );
        assert!(
            cache
                .get(&key, 3, |ino| if ino == 42 { 8 } else { 1 })
                .is_none(),
            "parent generation drift must invalidate cached path"
        );
        assert_eq!(
            cache.metrics().evictions,
            0,
            "stale invalidation should not count as eviction"
        );
    }

    #[test]
    fn path_shortcut_cache_tracks_hotset_occupancy_and_hits() {
        let key = PathShortcutCacheKey::from_components(
            NamespacePathStyle::Posix,
            &[String::from("hot"), String::from("path")],
        )
        .expect("path key");
        let tokens = vec![PathShortcutParentToken {
            parent: ROOT_INO,
            generation: 0,
        }];
        let charge = PathShortcutCache::estimate_entry_charge(
            &key,
            &PathShortcutCacheValue::Positive(9),
            &tokens,
        );
        let mut cache = PathShortcutCache::new(8, charge * 32);
        cache.put(
            key.clone(),
            PathShortcutCacheValue::Positive(9),
            1,
            tokens.clone(),
        );
        let _ = cache.get(&key, 1, |_| 0);
        let _ = cache.get(&key, 1, |_| 0);

        let metrics = cache.metrics();
        assert!(metrics.protected_occupancy_entries >= 1);
        assert!(metrics.protected_hits >= 1);
        assert!(metrics.hot_hit_ratio() > 0.0);
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
    fn readdir_hydrates_attr_cache_for_followup_getattr() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "readdir-attr-hydration");
        let created = vfs
            .create(ROOT_INO, b"batch-stat.txt", 1000, 1000, 0o644)
            .expect("create file");

        let entries = vfs.readdir(ROOT_INO).expect("readdir root");
        assert!(
            entries.iter().any(|entry| entry.ino == created.ino),
            "test file should be visible in root readdir"
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut cached_after_readdir = false;
        while Instant::now() < deadline {
            if vfs
                .state
                .namespace
                .attr_read_cache
                .lock()
                .contains(created.ino)
            {
                cached_after_readdir = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            cached_after_readdir,
            "readdir should hydrate attr cache for listed children"
        );

        let before = vfs.attr_read_cache_metrics();
        let loaded = vfs.getattr(created.ino).expect("getattr");
        let after = vfs.attr_read_cache_metrics();
        assert_eq!(loaded.ino, created.ino);
        assert!(
            after.hits > before.hits,
            "follow-up getattr should hit attr cache after readdir hydration"
        );
    }

    #[test]
    fn namespace_mutations_update_dentry_cache_entries() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "dentry-cache-hooks");
        let key = DentryCacheKey::new(ROOT_INO, b"cache-test.txt");

        assert!(
            vfs.lookup(ROOT_INO, b"cache-test.txt").is_err(),
            "initial lookup should miss"
        );
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(cache.entry_value(&key), Some(DentryCacheValue::Negative));
        }

        let created = vfs
            .create(ROOT_INO, b"cache-test.txt", 1000, 1000, 0o644)
            .expect("create file");
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(
                cache.entry_value(&key),
                Some(DentryCacheValue::Positive(created.ino))
            );
        }

        vfs.unlink(ROOT_INO, b"cache-test.txt")
            .expect("unlink file");
        {
            let cache = vfs.state.namespace.dentry_cache.lock();
            assert_eq!(cache.entry_value(&key), Some(DentryCacheValue::Negative));
        }
    }

    #[test]
    fn setattr_does_not_bump_namespace_generations() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "setattr-no-namespace-invalidate");
        let created = vfs
            .create(ROOT_INO, b"setattr.txt", 1000, 1000, 0o644)
            .expect("create file");
        let key = DentryCacheKey::new(ROOT_INO, b"setattr.txt");

        let _ = vfs
            .lookup(ROOT_INO, b"setattr.txt")
            .expect("prime dentry cache");
        let before_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let before_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);

        vfs.setattr(
            created.ino,
            SetattrArgs {
                mode: Some(0o600),
                ..SetattrArgs::default()
            },
        )
        .expect("setattr");

        let after_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let after_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);
        assert_eq!(before_epoch, after_epoch);
        assert_eq!(before_root_generation, after_root_generation);

        let cache = vfs.state.namespace.dentry_cache.lock();
        assert_eq!(
            cache.entry_value(&key),
            Some(DentryCacheValue::Positive(created.ino))
        );
    }

    #[test]
    fn write_does_not_bump_namespace_generations() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "write-no-namespace-invalidate");
        let created = vfs
            .create(ROOT_INO, b"write.txt", 1000, 1000, 0o644)
            .expect("create file");
        let key = DentryCacheKey::new(ROOT_INO, b"write.txt");

        let _ = vfs
            .lookup(ROOT_INO, b"write.txt")
            .expect("prime dentry cache");
        let before_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let before_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);

        rt.block_on(async {
            vfs.write(created.ino, 0, b"payload").await.expect("write");
        });

        let after_epoch = vfs.state.namespace.namespace_epoch.load(Ordering::SeqCst);
        let after_root_generation = vfs
            .state
            .namespace
            .directory_generations
            .lock()
            .get(&ROOT_INO)
            .copied()
            .unwrap_or(0);
        assert_eq!(before_epoch, after_epoch);
        assert_eq!(before_root_generation, after_root_generation);

        let cache = vfs.state.namespace.dentry_cache.lock();
        assert_eq!(
            cache.entry_value(&key),
            Some(DentryCacheValue::Positive(created.ino))
        );
    }

    #[test]
    fn namespace_resolver_path_shortcut_hits_and_renames_invalidate() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "path-shortcut-rename-invalidate");

        let a = vfs
            .mkdir(ROOT_INO, b"a", 1000, 1000, 0o755)
            .expect("mkdir a");
        let b = vfs.mkdir(a.ino, b"b", 1000, 1000, 0o755).expect("mkdir b");
        let file = vfs
            .create(b.ino, b"file.txt", 1000, 1000, 0o644)
            .expect("create file");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Posix);
        let first = resolver
            .resolve_path("/a/b/file.txt")
            .expect("resolve first path");
        let second = resolver
            .resolve_path("/a/b/file.txt")
            .expect("resolve warm path");
        assert_eq!(first.ino, file.ino);
        assert_eq!(second.ino, file.ino);
        let metrics = vfs.path_shortcut_cache_metrics();
        assert!(
            metrics.hits >= 1,
            "second full-path resolution should hit path shortcut cache"
        );

        vfs.rename(ROOT_INO, b"a", ROOT_INO, b"x")
            .expect("rename subtree root");
        assert!(
            resolver.resolve_path("/a/b/file.txt").is_err(),
            "old path must stop resolving after rename"
        );
        let moved = resolver
            .resolve_path("/x/b/file.txt")
            .expect("new path resolves after rename");
        assert_eq!(moved.ino, file.ino);
    }

    #[test]
    fn namespace_resolver_path_shortcut_invalidation_on_delete_subtree() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "path-shortcut-delete-invalidate");

        let parent = vfs
            .mkdir(ROOT_INO, b"parent", 1000, 1000, 0o755)
            .expect("mkdir parent");
        let child = vfs
            .mkdir(parent.ino, b"child", 1000, 1000, 0o755)
            .expect("mkdir child");
        vfs.create(child.ino, b"gone.txt", 1000, 1000, 0o644)
            .expect("create file");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Posix);
        resolver
            .resolve_path("/parent/child/gone.txt")
            .expect("warm full path");
        vfs.unlink(child.ino, b"gone.txt")
            .expect("unlink leaf file");
        assert!(
            resolver.resolve_path("/parent/child/gone.txt").is_err(),
            "deleted path must not resolve from stale shortcut"
        );
    }

    #[test]
    fn namespace_resolver_full_path_matches_component_lookup_for_ignored_and_blocked_paths() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "fullpath-component-parity");

        let ignored_dir = vfs
            .mkdir(ROOT_INO, b".Spotlight-V100", 1000, 1000, 0o755)
            .expect("mkdir ignored root metadata dir");
        let ignored_child = vfs
            .mkdir(ignored_dir.ino, b"Store-V2", 1000, 1000, 0o755)
            .expect("mkdir child under ignored dir");

        let resolver = vfs.namespace_resolver(NamespacePathStyle::Windows);
        let full_path_attr = resolver
            .resolve_path("\\.Spotlight-V100\\Store-V2")
            .expect("resolve ignored subtree");
        let component_attr = vfs
            .lookup(
                vfs.lookup(ROOT_INO, b".Spotlight-V100").unwrap().ino,
                b"Store-V2",
            )
            .expect("component lookup ignored subtree");
        assert_eq!(full_path_attr.ino, ignored_child.ino);
        assert_eq!(component_attr.ino, ignored_child.ino);

        meta.create_dir(ROOT_INO, b".Trashes", 1000, 1000, 0o755)
            .expect("seed blocked root dir in metadata");
        let trashes = vfs
            .lookup(ROOT_INO, b".Trashes")
            .expect("component lookup .Trashes");
        assert_eq!(
            resolver
                .resolve_path("\\.Trashes")
                .expect("resolve .Trashes")
                .ino,
            trashes.ino,
            "blocked-root policy should allow .Trashes to be materialized"
        );
    }

    #[test]
    fn namespace_resolver_windows_and_posix_match_on_same_namespace_entry() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "windows-posix-parity");

        let dir = vfs
            .mkdir(ROOT_INO, b"docs", 1000, 1000, 0o755)
            .expect("mkdir docs");
        let file = vfs
            .create(dir.ino, b"readme.txt", 1000, 1000, 0o644)
            .expect("create readme");

        let posix = vfs
            .namespace_resolver(NamespacePathStyle::Posix)
            .resolve_path("/docs/readme.txt")
            .expect("resolve posix path");
        let windows = vfs
            .namespace_resolver(NamespacePathStyle::Windows)
            .resolve_path("\\docs\\readme.txt")
            .expect("resolve windows path");
        assert_eq!(posix.ino, file.ino);
        assert_eq!(windows.ino, file.ino);
    }

    #[test]
    fn namespace_resolver_windows_parent_normalization_matches_winfsp_semantics() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "windows-parent-normalization");

        let dir = vfs
            .mkdir(ROOT_INO, b"folder", 1000, 1000, 0o755)
            .expect("mkdir folder");
        let resolved = vfs
            .namespace_resolver(NamespacePathStyle::Windows)
            .resolve_parent("\\folder\\child.txt.  ")
            .expect("resolve parent with trailing dot/space");

        assert_eq!(resolved.parent, dir.ino);
        assert_eq!(resolved.name, b"child.txt".to_vec());
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
    fn test_vfs_config_defaults_to_fast_cache_safety_mode() {
        assert_eq!(
            VfsConfig::default().cache_safety_mode,
            CacheSafetyMode::Fast
        );
    }

    #[test]
    fn local_sync_policy_strict_always_triggers_sync() {
        let trigger = local_sync_trigger_for_write_path(
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
        let trigger = local_sync_trigger_for_write_path(
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
        let bytes_trigger = local_sync_trigger_for_write_path(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            64 * 1024 * 1024,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(bytes_trigger, Some(LocalSyncTrigger::BytesThreshold));

        let window_trigger = local_sync_trigger_for_write_path(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            1024,
            Duration::from_millis(30),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(window_trigger, Some(LocalSyncTrigger::TimeWindow));

        let deferred = local_sync_trigger_for_write_path(
            CacheSafetyMode::Fast,
            LocalSyncPolicy::Opportunistic,
            1024,
            Duration::from_millis(1),
            64 * 1024 * 1024,
            Duration::from_millis(25),
        );
        assert_eq!(deferred, None);
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
}
