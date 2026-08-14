//! MetaEngine trait — abstract interface for metadata storage.
//!
//! This trait defines all metadata operations. The default implementation is `RedbMetaEngine`,
//! but the design supports swapping to PostgreSQL/Redis for multi-user mode.

use crate::types::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;

/// Aggregate latency for one metadata storage stage.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MetadataWriteStageTiming {
    /// Number of observed operations.
    pub calls: u64,
    /// Total elapsed time in nanoseconds.
    pub total_ns: u64,
    /// Longest observed operation in nanoseconds.
    pub max_ns: u64,
}

/// Shared low-cardinality timing for local metadata write transactions.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MetadataWriteObservability {
    /// Time spent obtaining the local metadata write transaction.
    pub begin_write: MetadataWriteStageTiming,
    /// Time spent holding the transaction before its commit or abort starts.
    pub mutation: MetadataWriteStageTiming,
    /// Time spent committing the local metadata transaction.
    pub commit: MetadataWriteStageTiming,
    /// Aggregate observability for regular-file overlay batches.
    #[serde(default)]
    pub batch: MetadataBatchWriteObservability,
}

/// Counts and latency for regular-file overlay batches.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct MetadataBatchWriteObservability {
    /// Number of submitted batches.
    pub batches: u64,
    /// Number of requested operations.
    pub requested_ops: u64,
    /// Number of operations completed before a failure, or all operations on success.
    pub actual_ops: u64,
    /// Estimated slices submitted across all batches.
    pub estimated_slices: u64,
    /// Estimated payload bytes submitted across all batches.
    pub estimated_payload_bytes: u64,
    /// Number of batches that failed.
    pub failures: u64,
    /// Last failing zero-based operation index, if any.
    pub rollback_index: Option<u64>,
    /// Total batch operation latency in nanoseconds.
    pub total_ns: u64,
}

// =============================================================================
// Errors
// =============================================================================
#[derive(Debug, Clone, thiserror::Error)]
/// Common metadata-layer error surface shared by all engine backends.
pub enum MetaError {
    /// The requested inode does not exist in metadata.
    #[error("inode not found: {0}")]
    InodeNotFound(Ino),

    /// The caller asked for a contiguous single-payload projection of a file range represented by multiple slice spans.
    #[error(
        "non-contiguous regular-file graph cannot be represented as one payload: ino={ino}, block_index={block_index}"
    )]
    NonContiguousRegularFileGraph {
        /// Inode whose graph projection is non-contiguous.
        ino: Ino,
        /// Logical block slot that could not be projected as a single object.
        block_index: u32,
    },

    /// The requested directory entry does not exist under its parent.
    #[error("entry not found: {name:?} in parent {parent}")]
    EntryNotFound {
        /// Parent directory inode.
        parent: Ino,
        /// Entry name that was requested.
        name: String,
    },

    /// A conflicting directory entry already exists under the parent.
    #[error("entry already exists: {name:?} in parent {parent}")]
    EntryExists {
        /// Parent directory inode.
        parent: Ino,
        /// Entry name that already exists.
        name: String,
    },

    /// The target inode is not a directory.
    #[error("not a directory: inode {0}")]
    NotADirectory(Ino),

    /// The target directory still has children and cannot be removed.
    #[error("directory not empty: inode {0}")]
    DirNotEmpty(Ino),

    /// The target inode is a directory where a non-directory was required.
    #[error("is a directory: inode {0}")]
    IsADirectory(Ino),

    /// Backend-specific storage or transport failure.
    #[error("storage error: {0}")]
    Storage(String),

    /// The metadata backend or remote service could not be reached.
    #[error("remote unavailable: {0}")]
    RemoteUnavailable(String),

    /// The metadata store requires repair before it can be used.
    #[error("metadata repair required: {0}")]
    RepairRequired(String),

    /// An operation exceeded its configured deadline.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Serialization or deserialization failure for persisted metadata.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Persisted format metadata is structurally valid but violates the current format contract.
    #[error("invalid format config: {0}")]
    InvalidFormatConfig(#[from] EncryptionContractError),

    /// The metadata store has not been initialized yet.
    #[error("format info not found — run `studiofs init` first")]
    NotFormatted,

    /// The on-disk metadata format version is unsupported by this binary.
    #[error("unsupported format version: found {found}, expected {expected}")]
    UnsupportedFormatVersion {
        /// Version stored in metadata.
        found: u16,
        /// Version expected by the running binary.
        expected: u16,
    },

    /// An xattr write exceeded the backend-supported value size limit.
    #[error("xattr value too large: actual={actual} bytes, limit={limit} bytes")]
    XattrValueTooLarge {
        /// Actual xattr value size in bytes.
        actual: usize,
        /// Maximum allowed xattr value size in bytes.
        limit: usize,
    },

    /// The local metadata store belongs to a different remote volume UUID.
    #[error("disk UUID mismatch: local={local}, remote={remote}")]
    UuidMismatch {
        /// UUID stored in the local metadata database.
        local: String,
        /// UUID observed from the authoritative remote metadata source.
        remote: String,
    },

    /// ACL evaluation rejected the requested access for the principal.
    #[error(
        "access denied: ino={ino}, principal={principal}, required={required:?}, actual={actual:?}"
    )]
    AccessDenied {
        /// Inode that denied access.
        ino: Ino,
        /// Principal being authorized.
        principal: String,
        /// Minimum access required for the attempted operation.
        required: AccessLevel,
        /// Effective access currently granted to the principal.
        actual: AccessLevel,
    },

    /// A namespace mutation failed at a specific position in an ordered batch.
    #[error("namespace batch operation {index} failed: {source}")]
    NamespaceBatchOperation {
        /// Zero-based position of the failed operation.
        index: usize,
        /// Error returned while applying the operation.
        #[source]
        source: Box<MetaError>,
    },
    /// A regular-file overlay mutation failed at a specific position in an ordered batch.
    #[error("regular-file overlay batch operation {index} failed: {source}")]
    RegularFileOverlayBatchOperation {
        /// Zero-based position of the failed operation.
        index: usize,
        /// Error returned while applying the operation.
        #[source]
        source: Box<MetaError>,
    },
}

/// Standard result type returned by metadata APIs.
pub type MetaResult<T> = Result<T, MetaError>;

impl MetaError {
    /// Returns `true` when the error indicates that the redb database file
    /// requires repair (e.g. after an unclean shutdown) and the read-only
    /// probe could not service the open.  Callers can retry with a writable
    /// `Database::create()` which auto-recovers.
    pub fn is_redb_repair_needed(&self) -> bool {
        matches!(self, MetaError::RepairRequired(_))
    }
}

/// `setxattr` flag: create attribute, fail if key already exists.
pub const XATTR_CREATE_FLAG: u32 = 1;
/// `setxattr` flag: replace attribute, fail if key does not exist.
pub const XATTR_REPLACE_FLAG: u32 = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Best-effort counts of local metadata work that still needs remote flush.
pub struct MetadataReplicationBacklog {
    /// Number of queued local operations not yet flushed.
    pub pending_ops: u32,
    /// Number of operations currently being flushed in the background.
    pub inflight_ops: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Best-effort counters for push-plus-poll metadata invalidation.
pub struct MetadataInvalidationDiagnostics {
    /// Number of PostgreSQL metadata NOTIFY messages received by this client.
    pub notifications_received: u64,
    /// Number of metadata listener reconnect attempts after the first listen.
    pub listener_reconnects: u64,
    /// Number of periodic fallback polls that discovered remote metadata.
    pub fallback_poll_discoveries: u64,
    /// Number of metadata sync attempts that returned an error.
    pub sync_errors: u64,
    /// Unix timestamp in milliseconds for the last successful metadata sync.
    pub last_successful_sync_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Outcome of a best-effort local metadata storage compaction attempt.
pub enum LocalStorageCompactionResult {
    /// Compaction reclaimed space or otherwise made progress.
    Compacted,
    /// Compaction was skipped because metadata transactions were active.
    SkippedBusy,
    /// Compaction was not applicable or had no reclaimable work.
    NoWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Whether this engine instance is still allowed to accept new mutations.
pub struct MetadataAuthorityStatus {
    /// `true` when inode id allocation still has local headroom.
    pub inode_id_headroom: bool,
    /// `true` when object ID allocation still has local headroom.
    pub object_id_headroom: bool,
}

impl MetadataAuthorityStatus {
    /// Return a status that permits new mutations.
    pub const fn writable() -> Self {
        Self {
            inode_id_headroom: true,
            object_id_headroom: true,
        }
    }

    /// Return a status that blocks new mutations.
    pub const fn read_only() -> Self {
        Self {
            inode_id_headroom: false,
            object_id_headroom: false,
        }
    }

    /// Return `true` when both inode and block allocators still have headroom.
    pub const fn allows_new_mutations(self) -> bool {
        self.inode_id_headroom && self.object_id_headroom
    }
}

impl Default for MetadataAuthorityStatus {
    fn default() -> Self {
        Self::writable()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Open-unlink orphan that still needs final cleanup after the last handle closes.
pub struct DeferredDeleteRecord {
    /// Orphaned inode awaiting deferred cleanup.
    pub ino: Ino,
    /// Parent directory from which the entry was removed.
    pub parent: Ino,
    /// Original dentry name removed from the parent.
    pub name: Vec<u8>,
    /// Unix timestamp when the record was created.
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Crash-recovery record for a namespace create that may need replay or cleanup.
pub struct PendingCreateRecord {
    /// Newly allocated inode for the pending create.
    pub ino: Ino,
    /// Parent directory that should contain the entry.
    pub parent: Ino,
    /// Entry name that is being created.
    pub name: Vec<u8>,
    /// File type of the pending inode.
    pub kind: FileType,
    /// Unix timestamp when the record was created.
    pub created_at: i64,
}

/// One of the namespace mutations supported by the deferred namespace queue.
#[derive(Debug, Clone)]
pub enum NamespaceMutation {
    /// Create a regular file with a caller-reserved inode.
    CreateFile {
        /// Caller-reserved inode number.
        ino: Ino,
        /// Parent directory inode.
        parent: Ino,
        /// Directory entry name.
        name: Vec<u8>,
        /// Owner user id.
        uid: u32,
        /// Owner group id.
        gid: u32,
        /// Permission and file-type mode bits.
        mode: u16,
    },
    /// Create a directory with a caller-reserved inode.
    CreateDir {
        /// Caller-reserved inode number.
        ino: Ino,
        /// Parent directory inode.
        parent: Ino,
        /// Directory entry name.
        name: Vec<u8>,
        /// Owner user id.
        uid: u32,
        /// Owner group id.
        gid: u32,
        /// Permission and file-type mode bits.
        mode: u16,
    },
    /// Apply an already-resolved attribute-only update.
    SetAttr {
        /// Fallback attribute value when the inode is not committed yet.
        attr: InodeAttr,
        /// Attribute-only delta to apply to the current inode.
        args: SetattrArgs,
    },
}

/// One regular-file overlay mutation within an ordered local batch.
#[derive(Debug, Clone)]
pub struct RegularFileOverlayMutation {
    /// Inode whose local overlay is updated.
    pub ino: Ino,
    /// New slice records grouped by chunk.
    pub chunk_slices: Vec<(ChunkId, Vec<SliceRecord>)>,
    /// Optional inode attributes to persist with the slices.
    pub attr: Option<InodeAttr>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Logical kind of undo-log record persisted for recovery.
pub enum UndoLogKind {
    /// Undo-log entry for a pending create record.
    PendingCreate,
    /// Undo-log entry for a deferred delete record.
    DeferredDelete,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Replay phase of a persisted undo-log record.
pub enum UndoLogPhase {
    #[default]
    /// The operation has been recorded but replay has not started yet.
    Started,
    /// Recovery is actively replaying this record.
    Replaying,
    /// Replay completed and the record can be ignored/cleaned up.
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
/// Generic undo-log entry used to recover namespace mutations after crashes.
pub struct UndoLogRecord {
    /// Monotonic undo-log operation id.
    pub op_id: u64,
    /// Logical kind of record being replayed.
    pub kind: UndoLogKind,
    #[serde(default)]
    /// Replay phase currently persisted for the record.
    pub phase: UndoLogPhase,
    /// Inode associated with the operation.
    pub ino: Ino,
    /// Parent directory involved in the operation.
    pub parent: Ino,
    /// Entry name involved in the operation.
    pub name: Vec<u8>,
    /// Unix timestamp when the record was created.
    pub created_at: i64,
}

impl MetadataReplicationBacklog {
    /// Return the sum of queued and in-flight replication operations.
    pub fn total_ops(self) -> u32 {
        self.pending_ops.saturating_add(self.inflight_ops)
    }
}

/// Strict backend contract.
///
/// This trait is intentionally comprehensive: storage backends implement it once,
/// while user-facing capability traits below expose focused method groups.
///
/// All operations are synchronous (redb is sync). Async callers should use
/// `AsyncMetaEngine` (for example `SyncMetaEngineAdapter`) to establish an
/// explicit async boundary.
pub trait MetaEngineCore: Send + Sync + MetaInodes {
    /// Return shared local metadata write timing when the backend exposes it.
    fn metadata_write_observability(&self) -> MetadataWriteObservability {
        MetadataWriteObservability::default()
    }

    /// Try to acquire the global janitor lease before contacting remote object storage.
    ///
    /// Local-only engines may allow the janitor to proceed immediately. Distributed
    /// engines should return `Ok(false)` when another client already holds the lease.
    fn try_acquire_global_janitor_lock(&self) -> MetaResult<bool>;

    /// Release the global janitor lease, if one was acquired.
    fn release_global_janitor_lock(&self) -> MetaResult<()>;

    /// Return object ids protected by PostgreSQL reachability intervals and
    /// active snapshot lifecycle or mount leases.
    fn get_global_snapshot_pins(&self) -> MetaResult<HashSet<u64>>;

    /// Acquire a PostgreSQL-backed hard-lock lease for this engine's client.
    fn acquire_file_lock_lease(
        &self,
        ino: Ino,
        kind: HardLockKind,
        lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseAcquireOutcome>;

    /// Release this engine's current hard-lock lease for the inode, if present.
    fn release_file_lock_lease(&self, ino: Ino) -> MetaResult<FileLockLeaseReleaseOutcome>;

    /// Renew this engine's current hard-lock lease for the inode, if still owned.
    fn renew_file_lock_lease(
        &self,
        ino: Ino,
        lease_ttl: std::time::Duration,
    ) -> MetaResult<FileLockLeaseRenewOutcome>;

    /// Read the current authoritative hard-lock lease for an inode.
    fn current_file_lock_lease(&self, ino: Ino) -> MetaResult<Option<FileLockLease>>;

    /// Best-effort hook for engines that need background monitoring to keep
    /// `known_file_lock_lease()` fresh for passive peers.
    fn ensure_known_file_lock_monitoring(&self) -> MetaResult<()> {
        Ok(())
    }

    /// Read the last known hard-lock lease cached locally by the engine.
    fn known_file_lock_lease(&self, ino: Ino) -> Option<FileLockLease>;
}

/// Capability trait for reading and writing persisted format metadata.
pub trait MetaFormat: Send + Sync {
    /// Store format info (called during `studiofs init`).
    fn set_format_info(&self, info: &FormatInfo) -> MetaResult<()>;

    /// Load format info.
    fn get_format_info(&self) -> MetaResult<FormatInfo>;
}

/// Capability trait for inode attributes, counters, and lifecycle metadata.
pub trait MetaInodes: Send + Sync {
    /// Get a counter value, returns 0 if not set.
    fn get_counter(&self, name: &str) -> MetaResult<u64>;

    /// Increment a counter atomically, returns the new value.
    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

    /// Decrement a counter atomically (saturating at 0), returns the new value.
    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64>;

    /// Set a counter to a specific value.
    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()>;

    /// Allocate a new inode number.
    fn next_inode(&self) -> MetaResult<Ino>;

    /// Allocate a contiguous batch of inode numbers.
    fn next_inode_ids(&self, count: u32) -> MetaResult<Vec<Ino>> {
        (0..count).map(|_| self.next_inode()).collect()
    }

    /// Get inode attributes.
    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr>;

    /// Get the durable unpublished/published lifecycle state for a regular
    /// file, when present.
    fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>>;

    /// Enumerate active lifecycle records for regular files plus any retained
    /// deleted-before-publish tombstones.
    fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>>;

    /// Batch-fetch inode attributes.
    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            attrs.push(self.get_attr(ino)?);
        }
        Ok(attrs)
    }

    /// Set (create or update) inode attributes.
    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()>;

    /// Remove an inode's attributes.
    fn remove_attr(&self, ino: Ino) -> MetaResult<()>;

    /// Enumerate existing inode attributes.
    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        let next = self.get_counter(counters::NEXT_INODE)?;
        let mut out = Vec::new();
        for ino in ROOT_INO..next {
            match self.get_attr(ino) {
                Ok(attr) => out.push(attr),
                Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(out)
    }
}

/// Capability trait for dentry lookup and directory namespace mutations.
pub trait MetaNamespace: Send + Sync {
    /// Look up a child entry by name in a parent directory.
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;

    /// List all entries in a directory.
    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>>;

    /// Add a directory entry. Returns error if it already exists.
    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()>;

    /// Remove a directory entry by name. Returns the removed entry.
    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue>;
}

/// High-level filesystem operations built on top of namespace and inode primitives.
pub trait MetaFsOps: Send + Sync + MetaInodes {
    /// Apply ordered namespace mutations in one local metadata transaction.
    fn apply_namespace_mutations(
        &self,
        mutations: &[NamespaceMutation],
    ) -> MetaResult<Vec<InodeAttr>>;

    /// Create a new file: allocate inode, set attrs, add dentry.
    fn create_file(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Create a new file using a caller-reserved inode.
    fn create_file_with_ino(
        &self,
        ino: Ino,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Create a new directory: allocate inode, set attrs, add dentry, update parent nlink.
    fn create_dir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Create a new directory using a caller-reserved inode.
    fn create_dir_with_ino(
        &self,
        ino: Ino,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> MetaResult<InodeAttr>;

    /// Unlink a file: remove dentry, decrement nlink, clean up if nlink=0.
    fn unlink(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>;

    /// Open-unlink transaction: remove dentry and decrement nlink, but keep inode alive.
    fn unlink_open_handle(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr>;

    /// Remove an empty directory.
    fn rmdir(&self, parent: Ino, name: &[u8]) -> MetaResult<()>;

    /// Atomic rename: move a directory entry in a single transaction.
    fn rename_atomic(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> MetaResult<RenameAtomicResult>;

    /// Rename a file or directory (delegates to `rename_atomic`).
    fn rename(&self, parent: Ino, name: &[u8], new_parent: Ino, new_name: &[u8]) -> MetaResult<()> {
        self.rename_atomic(parent, name, new_parent, new_name)?;
        Ok(())
    }

    /// Create a hard link in a single atomic transaction.
    fn link_atomic(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr>;

    /// Create a hard link (validates and delegates to `link_atomic`).
    fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> MetaResult<InodeAttr> {
        let attr = self.get_attr(ino)?;
        if attr.kind == FileType::Directory {
            return Err(MetaError::IsADirectory(ino));
        }
        self.link_atomic(ino, new_parent, new_name)
    }

    /// Create a symlink: allocate inode, store target, add dentry.
    fn create_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> MetaResult<InodeAttr>;
}

/// Capability trait for object ids and regular-file overlay/base graphs.
pub trait MetaPayloadObjects: Send + Sync + MetaInodes {
    /// Resolve merged regular-file slice coverage for one chunk-local range.
    ///
    /// Implementations must apply deterministic precedence:
    /// - local overlay over remote-visible base
    /// - later `SliceOrder` wins within a tier
    /// - tombstones mask lower-precedence slices
    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>>;

    /// Resolve merged regular-file slice coverage for a file range.
    ///
    /// `offset` and `length` are byte offsets in file coordinates. The returned
    /// spans are chunk-local and include chunk identity.
    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>>;

    /// Remove the regular-file graph for an inode (used when deleting).
    /// Returns the list of removed object IDs for GC.
    fn remove_regular_file_graph(&self, ino: Ino) -> MetaResult<Vec<u64>>;

    /// Allocate a new sequential object ID.
    fn next_object_id(&self) -> MetaResult<u64>;

    /// Allocate `count` consecutive object IDs in one backend transaction.
    ///
    /// Returns `count` IDs starting from the current counter value.
    fn next_object_ids(&self, count: u32) -> MetaResult<Vec<u64>>;

    /// Flush new local overlay slices for a regular file.
    ///
    /// The incoming slices are appended/merged into the overlay `RegularFileOverlayState`
    /// for the specified chunks.
    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
    ) -> MetaResult<()>;

    /// Return true when a file has persisted local overlay chunk/slice rows.
    ///
    /// This is used by epoch finalization to avoid publishing while local
    /// overlay metadata is still pending promotion to committed/base state.
    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool>;

    /// Collect object ids currently referenced by local regular-file overlay.
    ///
    /// Backends must read the dedicated local overlay graph so shadowed and
    /// unpublished payload references are not lost during upload/GC decisions.
    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>>;

    /// Collect all object ids currently referenced anywhere in the inode's
    /// regular-file graph, together with their deduplicated physical byte
    /// usage.
    ///
    /// The returned byte count is the maximum observed `offset + length` for
    /// each `object_id`, so a staged object referenced by multiple slices is
    /// counted once.
    fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>>;

    /// Return `true` when `object_id` is still referenced by the inode's local
    /// regular-file overlay for `expected_epoch`.
    ///
    /// Slice-native regular-file writes can keep multiple immutable objects for
    /// one logical block. Upload workers use this check to decide whether an
    /// uploaded object still belongs to the current unpublished epoch without
    /// requiring a contiguous per-logical-slot payload-ref projection of the
    /// overlay graph.
    fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool>;

    /// Mark a still-referenced overlay object as remote-visible for the current
    /// unpublished epoch, even when its logical block is represented by
    /// multiple slice records and cannot be synthesized into a single
    /// single payload reference.
    ///
    /// Returns `Ok(true)` when `object_id` is still referenced by the inode's
    /// local regular-file overlay for `expected_epoch`. Returns `Ok(false)`
    /// when the inode/epoch/object no longer matches the caller's view.
    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool>;

    /// Batch form of [`mark_regular_file_overlay_object_uploaded_if_matches`].
    ///
    /// Returns the object ids that still matched the inode/epoch overlay.
    fn mark_regular_file_overlay_objects_uploaded_if_match(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_ids: &[u64],
    ) -> MetaResult<Vec<u64>> {
        let mut marked = Vec::new();
        for &object_id in object_ids {
            if self.mark_regular_file_overlay_object_uploaded_if_matches(
                ino,
                expected_epoch,
                object_id,
            )? {
                marked.push(object_id);
            }
        }
        Ok(marked)
    }

    /// Promote the local regular-file overlay graph to the remote-visible base
    /// graph for a specific inode/content epoch.
    ///
    /// Returns `Ok(true)` when promotion succeeds and `Ok(false)` when
    /// preconditions fail (inode missing, epoch mismatch, or referenced
    /// overlay payloads are not uploaded yet).
    fn publish_regular_file_overlay_to_base_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
        Ok(self
            .publish_regular_file_overlay_to_base_if_matches_batch(&[(ino, expected_epoch)])?
            .into_iter()
            .next()
            .unwrap_or(false))
    }

    /// Promote multiple regular-file overlays in one metadata transaction.
    ///
    /// The result preserves input order and reports which epoch publications
    /// passed their inode/epoch/payload-readiness guards.
    fn publish_regular_file_overlay_to_base_if_matches_batch(
        &self,
        requests: &[(Ino, u64)],
    ) -> MetaResult<Vec<bool>>;

    /// Best-effort background compaction for fragmented regular-file chunk
    /// graphs.
    ///
    /// Implementations may compact fewer than `max_chunks` candidates and are
    /// allowed to skip rows that change while compaction is in flight.
    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        let _ = max_chunks;
        Ok(0)
    }
}

/// Capability trait for garbage-collection bookkeeping and reachability scans.
pub trait MetaGc: Send + Sync {
    /// Best-effort compaction of local metadata storage.
    ///
    /// Returns whether compaction made progress, skipped because the store was
    /// busy, or had no work to do.
    fn compact_local_storage(&self) -> MetaResult<LocalStorageCompactionResult> {
        Ok(LocalStorageCompactionResult::NoWork)
    }

    /// Add a block to pending deletions (mark for GC).
    /// The timestamp is used for grace period.
    fn add_pending_deletion(&self, object_id: u64, timestamp: i64) -> MetaResult<()>;

    /// Remove a block from pending deletions (after GC).
    fn remove_pending_deletion(&self, object_id: u64) -> MetaResult<()>;

    /// Get all pending deletions (object_id, timestamp).
    fn get_pending_deletions(&self) -> MetaResult<Vec<(u64, i64)>>;

    /// Get up to `limit` pending deletions (object_id, timestamp).
    fn get_pending_deletions_limited(&self, limit: usize) -> MetaResult<Vec<(u64, i64)>> {
        Ok(self
            .get_pending_deletions()?
            .into_iter()
            .take(limit)
            .collect())
    }

    /// Get currently referenced object IDs from the full reachability graph.
    ///
    /// Backends should include regular-file graph overlay/base rows and
    /// PostgreSQL-authoritative snapshot reachability so cleanup can reason
    /// about publication races.
    /// and shadow refs directly from metadata reachability.
    ///
    /// Prefer streaming the underlying rows into a shared accumulator instead
    /// of materializing intermediate per-table graphs.
    fn get_referenced_object_ids(&self) -> MetaResult<HashSet<u64>>;
}

/// Capability trait for crash recovery and replay of pending local operations.
pub trait MetaRecovery: Send + Sync {
    /// Replay pending create intents and heal dangling namespace entries.
    /// Returns parent inode ids whose directory namespace was mutated during replay.
    fn recover_pending_creates(&self) -> MetaResult<Vec<Ino>>;

    /// Record a pending create intent for crash recovery.
    fn add_pending_create(&self, record: &PendingCreateRecord) -> MetaResult<()>;

    /// Remove a pending create intent.
    fn remove_pending_create(&self, ino: Ino) -> MetaResult<()>;

    /// List pending create intents.
    fn list_pending_creates(&self) -> MetaResult<Vec<PendingCreateRecord>>;

    /// Append an operation into the generic undo log.
    fn append_undo_log(&self, record: &UndoLogRecord) -> MetaResult<()>;

    /// Remove all undo-log records for the `(kind, ino)` tuple.
    fn remove_undo_logs_for(&self, kind: UndoLogKind, ino: Ino) -> MetaResult<()>;

    /// List undo-log records.
    fn list_undo_logs(&self) -> MetaResult<Vec<UndoLogRecord>>;

    /// Replay generic undo-log records and heal dangling namespace state.
    /// Returns parent inode ids whose directory namespace was mutated during replay.
    fn recover_undo_logs(&self) -> MetaResult<Vec<Ino>>;

    /// Record a deferred delete (open-unlink orphan) for crash-safe cleanup.
    fn add_deferred_delete(&self, record: &DeferredDeleteRecord) -> MetaResult<()>;

    /// Remove a deferred delete record after final cleanup.
    fn remove_deferred_delete(&self, ino: Ino) -> MetaResult<()>;

    /// List deferred delete records pending replay/recovery.
    fn list_deferred_deletes(&self) -> MetaResult<Vec<DeferredDeleteRecord>>;
}

/// Capability trait for symlink target storage.
pub trait MetaSymlink: Send + Sync {
    /// Store a symlink target for the given inode.
    fn set_symlink_target(&self, ino: Ino, target: &[u8]) -> MetaResult<()>;

    /// Get the symlink target for the given inode.
    fn get_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>>;
}

/// Capability trait for extended attribute storage.
pub trait MetaXattr: Send + Sync {
    /// Get an extended attribute value.
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>>;

    /// Get an extended attribute value size without returning the value bytes.
    fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<usize>>;

    /// Set an extended attribute.
    /// Flags:
    /// - `XATTR_CREATE_FLAG`: fail if key exists
    /// - `XATTR_REPLACE_FLAG`: fail if key is missing
    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()>;

    /// Remove an extended attribute.
    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()>;

    /// List all extended attribute names for an inode.
    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>>;
}

/// Capability trait for ACL rule storage and lookup.
pub trait MetaAcl: Send + Sync {
    /// Set (create/update) explicit ACL rule for `(ino, principal)`.
    fn set_acl_rule(&self, ino: Ino, principal: &str, access_level: AccessLevel) -> MetaResult<()>;

    /// Remove explicit ACL rule for `(ino, principal)`.
    fn remove_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<()>;

    /// Get explicit ACL rule for `(ino, principal)`, if present.
    fn get_acl_rule(&self, ino: Ino, principal: &str) -> MetaResult<Option<AccessLevel>>;
}

/// Capability trait for replication, flushing, and remote consistency hooks.
pub trait MetaReplication: Send + Sync {
    /// Verify that the local metadata matches the remote state (e.g. UUID check).
    fn check_consistency(&self) -> MetaResult<()>;

    /// Flush pending metadata mutations to remote store (if any).
    fn flush_metadata(&self) -> MetaResult<()>;

    /// Advance the local metadata durability group-commit policy without waiting
    /// for peer publication.
    fn flush_local_metadata(&self) -> MetaResult<()>;

    /// Force metadata synchronization from remote source (if applicable).
    fn sync_metadata(&self) -> MetaResult<()>;

    /// Execute the strict local metadata durability barrier without waiting for
    /// peer publication.
    fn sync_local_metadata(&self) -> MetaResult<()>;

    /// Record a VFS-local open handle that should keep its inode binding alive
    /// while remote metadata is applied.
    fn note_active_open_inode(&self, ino: Ino);

    /// Release a previously recorded VFS-local open handle.
    fn note_inactive_open_inode(&self, ino: Ino);

    /// Force metadata synchronization from the periodic invalidation fallback poll.
    fn sync_metadata_for_invalidation_poll(&self) -> MetaResult<()>;

    /// Stable identifier used for invalidation self-echo suppression.
    fn invalidation_sender_id(&self) -> Option<String>;

    /// Optional wakeup primitive for push-driven metadata invalidation.
    fn metadata_invalidation_notifier(&self) -> Option<Arc<tokio::sync::Notify>> {
        None
    }

    /// Best-effort diagnostics for push-plus-poll metadata invalidation.
    fn metadata_invalidation_diagnostics(&self) -> MetadataInvalidationDiagnostics {
        MetadataInvalidationDiagnostics::default()
    }

    /// Request a near-immediate flush of pending metadata mutations.
    fn request_eager_flush(&self);

    /// Best-effort metadata version token for change detection.
    fn metadata_version_token(&self) -> Option<String>;

    /// Best-effort namespace-only version token for dentry/path invalidation.
    fn namespace_version_token(&self) -> Option<String>;

    /// Best-effort count of local metadata mutations not yet durably pushed to remote.
    fn replication_backlog(&self) -> MetadataReplicationBacklog;

    /// Best-effort local metadata authority status for deciding whether new
    /// mutations should still be accepted.
    fn metadata_authority_status(&self) -> MetadataAuthorityStatus;

    /// Return the estimated size of remote metadata in bytes, if applicable.
    fn remote_metadata_size(&self) -> MetaResult<Option<u64>>;

    /// Prepare the engine for close: stop background threads and release
    /// resources so that `Database::drop()` can run cleanly before process
    /// exit. Default is a no-op (standalone redb needs no preparation).
    fn prepare_for_close(&self) -> MetaResult<()> {
        Ok(())
    }
}

/// Public aggregate contract for consumers.
pub trait MetaEngine:
    MetaEngineCore
    + MetaFormat
    + MetaInodes
    + MetaNamespace
    + MetaFsOps
    + MetaPayloadObjects
    + MetaGc
    + MetaRecovery
    + MetaSymlink
    + MetaXattr
    + MetaAcl
    + MetaReplication
    + crate::snapshot_ports::UploadedFragmentLedger
{
    /// Returns the PostgreSQL-backed Snapshot v2 job store when this engine
    /// owns one, or `None` for local-only metadata engines.
    fn snapshot_job_store(&self) -> Option<Arc<dyn crate::snapshot_ports::SnapshotJobStore>> {
        None
    }

    /// Returns the PostgreSQL-backed Snapshot v2 source reader when this
    /// engine owns one, or `None` for local-only metadata engines.
    fn snapshot_source_reader(
        &self,
    ) -> Option<Arc<dyn crate::snapshot_ports::SnapshotSourceReader>> {
        None
    }

    /// Returns the PostgreSQL-backed Snapshot v2 mount lease store when this
    /// engine owns one, or `None` for local-only metadata engines.
    fn snapshot_mount_store(&self) -> Option<Arc<dyn crate::snapshot_ports::SnapshotMountStore>> {
        None
    }

    /// Returns the PostgreSQL-backed Snapshot v2 cleanup store when this
    /// engine owns one, or `None` for local-only metadata engines.
    fn snapshot_cleanup_store(
        &self,
    ) -> Option<Arc<dyn crate::snapshot_ports::SnapshotCleanupStore>> {
        None
    }

    /// Returns the PostgreSQL-backed Snapshot v2 daily-policy store when this
    /// engine owns one, or `None` for local-only metadata engines.
    fn snapshot_policy_store(&self) -> Option<Arc<dyn crate::snapshot_ports::SnapshotPolicyStore>> {
        None
    }

    /// Apply ordered regular-file overlay mutations in one local transaction.
    fn apply_regular_file_overlay_batch(
        &self,
        mutations: &[RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>>;

    /// Flush local regular-file overlay slices and optionally inode attributes
    /// in one logical foreground persist step.
    ///
    /// Returns the inode attribute that was **actually committed** to durable
    /// storage, or `None` when no `attr` was provided. Callers should use the
    /// returned value (rather than the input) for any post-commit in-memory
    /// state updates, because backends are permitted to normalise fields such
    /// as `upload_state` inside the commit transaction using information only
    /// they can observe (e.g. whether a committed remote base graph already exists).
    ///
    /// Concrete backends must commit the slice and attribute mutations in one
    /// backend transaction when both are provided.
    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>>;
}
