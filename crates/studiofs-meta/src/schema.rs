//! StudioFS Metadata Types
//!
//! Core types for the virtual filesystem metadata and the chunk/slice object model.
//!
//! Architecture Key Changes:
//! - Regular files are represented as chunk/slice object graphs
//! - Payload objects are immutable - never modified, only created/deleted
//! - Local overlay and remote-visible base views are modeled explicitly
//! - All metadata lives in redb, S3 stores only raw payload objects

use rand::TryRngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::SystemTime;
use zeroize::{Zeroize, ZeroizeOnDrop};

// =============================================================================
// Constants
// =============================================================================

/// Default logical block size: 4 MiB.
pub const DEFAULT_BLOCK_SIZE: u32 = 4 * 1024 * 1024;
/// Default chunk size for the chunk/slice object graph: 64 MiB.
///
/// Follow-up: promote this into persisted `FormatInfo` once chunk-size
/// migration/versioning is introduced.
pub const DEFAULT_CHUNK_SIZE: u32 = 64 * 1024 * 1024;
/// Current on-disk metadata format version.
pub const CURRENT_FORMAT_VERSION: u16 = 4;
/// Supported encryption format version for remote payload envelopes.
pub const CURRENT_ENCRYPTION_FORMAT_VERSION: u16 = 1;
/// Initial active key identifier for formatted encrypted volumes.
pub const CURRENT_ENCRYPTION_KEY_ID: u32 = 1;
/// XChaCha20-Poly1305 key length in bytes.
pub const ENCRYPTION_KEY_LENGTH: usize = 32;

/// Root inode number.
pub const ROOT_INO: Ino = 1;

// =============================================================================
// Inode Types
// =============================================================================

/// Inode number type.
pub type Ino = u64;

/// File type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum FileType {
    /// A regular data file.
    RegularFile = 1,
    /// A directory containing dentries.
    Directory = 2,
    /// A symbolic link.
    Symlink = 3,
}

impl FileType {
    /// Convert from raw u8 value.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::RegularFile),
            2 => Some(Self::Directory),
            3 => Some(Self::Symlink),
            _ => None,
        }
    }
}

/// Coarse remote upload lifecycle exposed by metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
pub enum UploadState {
    /// Payload upload is still in progress.
    Uploading = 1,
    /// Metadata is being updated after upload completion.
    Updating = 2,
    #[default]
    /// The local and committed views are settled.
    Ready = 3,
}

/// Explicit ZeroIO lifecycle for the locally durable view of a regular file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum InodeLifecycleState {
    /// Locally durable and visible on this mount, but not yet uploaded.
    #[default]
    LocalOnly = 1,
    /// Upload is in flight for the current unpublished epoch.
    Uploading = 2,
    /// The current unpublished epoch is fully uploaded and is waiting for
    /// explicit publication.
    PublishReady = 3,
    /// Local and globally published views are aligned.
    Published = 4,
    /// The unpublished epoch was deleted locally and must never publish.
    DeletedBeforePublish = 5,
}

/// Retention class for payload objects inside the single local cache tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum PayloadRetentionClass {
    /// Correctness-critical unpublished payload; must remain pinned locally.
    #[default]
    PinnedUnpublished = 1,
    /// Uploaded unpublished payload retained until explicit publication.
    PublishReadyRetained = 2,
    /// Published payload retained only as a warm local cache copy.
    PublishedWarm = 3,
    /// Payload that belonged to a deleted-before-publish epoch and awaits
    /// deferred cleanup.
    DeletedBeforePublish = 4,
}

/// Coarse ACL access level used for local-first authorization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum AccessLevel {
    /// Hide entry completely (lookup/readdir should behave as not found).
    NoAccess = 0,
    /// Read-only access.
    ReadOnly = 1,
    /// Read-write access.
    #[default]
    ReadWrite = 2,
}

/// Global hard-lock mode coordinated through PostgreSQL leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum HardLockKind {
    /// Multiple readers may hold the lease concurrently.
    Shared = 1,
    /// Exactly one writer holds the lease.
    Exclusive = 2,
}

/// Authoritative or known snapshot of one inode hard-lock lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileLockLease {
    /// Inode protected by this lease.
    pub ino: Ino,
    /// Shared or exclusive lease mode.
    pub kind: HardLockKind,
    /// Monotonic generation assigned by PostgreSQL.
    pub generation: u64,
    /// Unix-millis deadline after which the lease expires.
    pub lease_expires_at_unix_ms: i64,
    #[serde(default)]
    /// Client id holding the exclusive lease, when present.
    pub exclusive_owner: Option<String>,
    #[serde(default)]
    /// Client ids currently holding the shared lease.
    pub shared_owners: BTreeSet<String>,
}

/// Outcome when trying to acquire a hard-lock lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLockLeaseAcquireOutcome {
    /// Lease acquisition succeeded and returned the current authoritative lease.
    Acquired(FileLockLease),
    /// Lease acquisition was blocked by an existing incompatible holder.
    Blocked(FileLockLease),
}

/// Outcome when releasing a hard-lock lease for the current client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLockLeaseReleaseOutcome {
    /// The current client released the lease; remaining authoritative state is returned.
    Released(Option<FileLockLease>),
    /// The current client did not hold the lease; current authoritative state is returned.
    NotHeld(Option<FileLockLease>),
}

/// Outcome when renewing a hard-lock lease for the current client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileLockLeaseRenewOutcome {
    /// Lease renewal succeeded and returned the refreshed lease.
    Renewed(FileLockLease),
    /// Lease ownership was lost; remaining authoritative state is returned.
    Lost(Option<FileLockLease>),
}

impl AccessLevel {
    /// Convert a serialized ACL access level into its enum form.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::NoAccess),
            1 => Some(Self::ReadOnly),
            2 => Some(Self::ReadWrite),
            _ => None,
        }
    }
}

/// ACL rule persisted in metadata and replicated through the event log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclRule {
    /// Inode protected by this ACL rule.
    pub ino: Ino,
    /// Principal that receives the access level.
    pub principal: String,
    /// Effective access granted to the principal.
    pub access_level: AccessLevel,
}

/// Inode attributes - stored in ATTRIBUTES table.
///
/// Key design: timestamps stored as nanoseconds since Unix epoch,
/// simplifying comparisons and range queries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeAttr {
    /// Inode number.
    pub ino: Ino,
    /// File type.
    pub kind: FileType,
    /// Permission mode (Unix-style, e.g. 0o755).
    pub mode: u16,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// File size in bytes.
    pub size: u64,
    /// Size visible to remote clients after committed payload uploads.
    #[serde(default)]
    pub committed_size: u64,
    /// Number of hard links.
    pub nlink: u32,
    /// Modification time (nanoseconds since Unix epoch).
    pub mtime: i64,
    /// Change time (nanoseconds since Unix epoch).
    pub ctime: i64,
    /// Access time (nanoseconds since Unix epoch).
    pub atime: i64,
    /// Parent inode (0 for root).
    pub parent: Ino,
    /// Creation/birth time (nanoseconds since Unix epoch).
    /// Used as Windows CreationTime and macOS crtime.
    /// Falls back to ctime when 0 (backwards compat with older volumes).
    #[serde(default)]
    pub crtime: i64,
    /// Windows file attributes (ARCHIVE, HIDDEN, SYSTEM, READONLY, etc.).
    /// Stored as raw Win32 FILE_ATTRIBUTE_* bitmask. 0 = not set (derive from kind).
    #[serde(default)]
    pub win_attrs: u32,
    /// Monotonic content generation used to discard stale async completions.
    #[serde(default)]
    pub content_epoch: u64,
    /// Last committed content generation visible to remote clients.
    #[serde(default)]
    pub committed_content_epoch: u64,
    /// Coarse remote upload lifecycle for the current inode view.
    #[serde(default)]
    pub upload_state: UploadState,
}

/// Durable lifecycle metadata for a regular file's locally visible epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InodeLifecycleRecord {
    /// Regular-file inode described by this lifecycle row.
    pub ino: Ino,
    /// Current lifecycle state for the locally visible epoch.
    pub state: InodeLifecycleState,
    /// Size visible on the local mount for the current epoch.
    pub local_visible_size: u64,
    /// Size visible to globally published readers.
    pub published_size: u64,
    /// Current locally visible content epoch.
    pub local_content_epoch: u64,
    /// Last globally published content epoch.
    pub published_content_epoch: u64,
    /// Whether the inode still has a published base that local mutations build
    /// on top of.
    #[serde(default)]
    pub has_published_base: bool,
    /// Retention policy for the current local epoch in the shared cache.
    #[serde(default)]
    pub retention: PayloadRetentionClass,
    /// Object ids awaiting deferred cleanup for deleted-before-publish epochs.
    #[serde(default)]
    pub deferred_cleanup_object_ids: Vec<u64>,
}

impl InodeLifecycleRecord {
    /// Return `true` when the local file view has diverged from the published one.
    pub fn local_view_differs_from_published(&self) -> bool {
        self.local_visible_size != self.published_size
            || self.local_content_epoch != self.published_content_epoch
    }
}

impl InodeAttr {
    /// Return `true` when a regular file has locally visible content that is
    /// not yet settled in the committed remote view.
    pub fn is_upload_pending(&self) -> bool {
        self.kind == FileType::RegularFile
            && self.upload_state != UploadState::Ready
            && (self.content_epoch != self.committed_content_epoch
                || self.size != self.committed_size)
    }

    /// Return `true` for a brand-new empty regular file that has not published
    /// its dentry to remote peers yet.
    pub fn is_unpublished_empty_draft(&self) -> bool {
        self.kind == FileType::RegularFile
            && self.upload_state == UploadState::Ready
            && self.size == 0
            && self.committed_size == 0
            && self.content_epoch == self.committed_content_epoch
            && self.committed_content_epoch == 0
    }

    /// Get current time as nanoseconds since Unix epoch.
    pub fn now_nanos() -> i64 {
        let elapsed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let nanos = elapsed
            .as_secs()
            .saturating_mul(1_000_000_000)
            .saturating_add(u64::from(elapsed.subsec_nanos()));
        i64::try_from(nanos).unwrap_or(i64::MAX)
    }

    /// Create attributes for a new directory.
    pub fn new_dir(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = Self::now_nanos();
        Self {
            ino,
            kind: FileType::Directory,
            mode,
            uid,
            gid,
            size: 4096,
            committed_size: 4096,
            nlink: 2, // '.' and '..'
            mtime: now,
            ctime: now,
            atime: now,
            parent,
            crtime: now,
            win_attrs: 0x10, // FILE_ATTRIBUTE_DIRECTORY
            content_epoch: 0,
            committed_content_epoch: 0,
            upload_state: UploadState::Ready,
        }
    }

    /// Create attributes for a new regular file.
    pub fn new_file(ino: Ino, parent: Ino, uid: u32, gid: u32, mode: u16) -> Self {
        let now = Self::now_nanos();
        Self {
            ino,
            kind: FileType::RegularFile,
            mode,
            uid,
            gid,
            size: 0,
            committed_size: 0,
            nlink: 1,
            mtime: now,
            ctime: now,
            atime: now,
            parent,
            crtime: now,
            win_attrs: 0x20, // FILE_ATTRIBUTE_ARCHIVE
            content_epoch: 0,
            committed_content_epoch: 0,
            upload_state: UploadState::Ready,
        }
    }

    /// Update modification and change times to now.
    pub fn touch_mtime(&mut self) {
        let now = Self::now_nanos();
        self.mtime = now;
        self.ctime = now;
    }

    /// Update access time to now.
    pub fn touch_atime(&mut self) {
        self.atime = Self::now_nanos();
    }

    /// Update change time only (metadata change, e.g. chmod/chown).
    pub fn touch_ctime(&mut self) {
        self.ctime = Self::now_nanos();
    }

    /// Create attributes for a new symlink.
    pub fn new_symlink(ino: Ino, parent: Ino, uid: u32, gid: u32, target_len: usize) -> Self {
        let now = Self::now_nanos();
        let target_len = u64::try_from(target_len).unwrap_or(u64::MAX);
        Self {
            ino,
            kind: FileType::Symlink,
            mode: 0o777,
            uid,
            gid,
            size: target_len,
            committed_size: target_len,
            nlink: 1,
            mtime: now,
            ctime: now,
            atime: now,
            parent,
            crtime: now,
            win_attrs: 0x0400, // FILE_ATTRIBUTE_REPARSE_POINT
            content_epoch: 0,
            committed_content_epoch: 0,
            upload_state: UploadState::Ready,
        }
    }

    /// Get the effective creation time (falls back to ctime for old inodes).
    pub fn effective_crtime(&self) -> i64 {
        if self.crtime != 0 {
            self.crtime
        } else {
            self.ctime
        }
    }
}

// =============================================================================
// setattr Arguments
// =============================================================================

/// What to do with a timestamp field in setattr.
#[derive(Debug, Clone, Copy)]
pub enum TimeSpec {
    /// Set to the given value (nanoseconds since Unix epoch).
    Set(i64),
    /// Set to the current system time.
    Now,
    /// Leave unchanged.
    Omit,
}

impl TimeSpec {
    /// Resolve to a concrete nanosecond value (or `None` for `Omit`).
    pub fn resolve(self) -> Option<i64> {
        match self {
            TimeSpec::Set(ns) => Some(ns),
            TimeSpec::Now => Some(InodeAttr::now_nanos()),
            TimeSpec::Omit => None,
        }
    }
}

/// Arguments for `setattr`. All `None`/`Omit` fields are left unchanged.
#[derive(Debug, Clone)]
pub struct SetattrArgs {
    /// New file size, if the caller wants to truncate or extend the inode.
    pub size: Option<u64>,
    /// New permission mode, if any.
    pub mode: Option<u16>,
    /// New owner uid, if any.
    pub uid: Option<u32>,
    /// New owner gid, if any.
    pub gid: Option<u32>,
    /// Requested access-time update behavior.
    pub atime: TimeSpec,
    /// Requested modification-time update behavior.
    pub mtime: TimeSpec,
    /// Creation/birth time.
    pub crtime: TimeSpec,
    /// Windows file attributes (None = don't change).
    pub win_attrs: Option<u32>,
}

impl Default for SetattrArgs {
    fn default() -> Self {
        Self {
            size: None,
            mode: None,
            uid: None,
            gid: None,
            atime: TimeSpec::Omit,
            mtime: TimeSpec::Omit,
            crtime: TimeSpec::Omit,
            win_attrs: None,
        }
    }
}

impl SetattrArgs {
    /// Convenience: create args that only set size.
    pub fn size_only(size: u64) -> Self {
        Self {
            size: Some(size),
            ..Default::default()
        }
    }

    /// Apply the namespace-safe subset of setattr fields.
    pub fn apply_attr_only(&self, attr: &mut InodeAttr) -> bool {
        let mut changed = false;
        if let Some(mode) = self.mode {
            attr.mode = mode & 0o7777;
            changed = true;
        }

        if let Some(uid) = self.uid {
            attr.uid = uid;
            changed = true;
        }

        if let Some(gid) = self.gid {
            attr.gid = gid;
            changed = true;
        }

        if let Some(ns) = self.atime.resolve() {
            attr.atime = ns;
            changed = true;
        }

        if let Some(ns) = self.mtime.resolve() {
            attr.mtime = ns;
            changed = true;
        }

        if let Some(ns) = self.crtime.resolve() {
            attr.crtime = ns;
            changed = true;
        }

        if let Some(wa) = self.win_attrs {
            attr.win_attrs = wa;
            changed = true;
        }

        changed
    }
}

// =============================================================================
// Directory Entry Types
// =============================================================================

/// Directory entry value - stored in DIRECTORY_MAP.
///
/// Key: (parent_ino, name_hash) for O(1) lookup by path component.
/// Name is not stored here - it's part of the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntryValue {
    /// Inode number of the child.
    pub ino: Ino,
    /// File type of the child.
    pub kind: FileType,
}

// =============================================================================
// Payload Object Reference Types
// =============================================================================

bitflags::bitflags! {
    /// Payload object flags retained for compatibility with existing slice records.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BlockFlags: u8 {
        /// Legacy flag from earlier builds that used LZ4 compression.
        const COMPRESSED_LZ4 = 0x01;
        /// Payload object represents less than one logical block's worth of data.
        const PARTIAL_BLOCK = 0x02;
    }
}

impl Default for BlockFlags {
    fn default() -> Self {
        Self::empty()
    }
}

/// Payload object reference embedded in regular-file slice records.
///
/// Points to an immutable S3/local object containing payload bytes for a slice.
///
/// Design decisions:
/// - Sequential object_id (not content hash) - simpler and faster
/// - Immutable objects - never modified, only created/deleted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadRef {
    /// Sequential payload object ID (unique across the volume).
    pub object_id: u64,
    /// Offset within the payload object.
    pub offset: u32,
    /// Length of valid data in this slice view.
    pub length: u32,
    /// Compression and other flags.
    pub flags: BlockFlags,
}

impl PayloadRef {
    /// Create a new payload reference that covers a full logical block.
    pub fn new_full(object_id: u64, length: u32) -> Self {
        Self {
            object_id,
            offset: 0,
            length,
            flags: BlockFlags::empty(),
        }
    }

    /// Create a new payload reference that covers less than one logical block.
    pub fn new_partial(object_id: u64, length: u32) -> Self {
        Self {
            object_id,
            offset: 0,
            length,
            flags: BlockFlags::PARTIAL_BLOCK,
        }
    }

    /// Check if this reference is marked as partial.
    pub fn is_partial(&self) -> bool {
        self.flags.contains(BlockFlags::PARTIAL_BLOCK)
    }

    /// Check if this payload object is marked as compressed.
    pub fn is_compressed(&self) -> bool {
        self.flags.contains(BlockFlags::COMPRESSED_LZ4)
    }
}

// =============================================================================
// Chunk / Slice Graph Types
// =============================================================================

/// Chunk identity within a regular file.
///
/// Chunks are fixed file-range partitions and are stable for a given format
/// version.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct ChunkId(pub u64);

/// Slice identity within a chunk.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SliceId(pub u64);

/// Ordering token used to resolve overlaps within a chunk.
///
/// Later accepted slices win within the same visibility tier.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SliceOrder(pub u64);

/// Chunk-relative byte range for one slice.
///
/// Ranges are half-open: `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRange {
    /// Inclusive chunk-relative start offset.
    pub start: u32,
    /// Exclusive chunk-relative end offset.
    pub end: u32,
}

impl SliceRange {
    /// Create a non-empty half-open slice range.
    pub fn new(start: u32, end: u32) -> Option<Self> {
        if start < end && end <= DEFAULT_CHUNK_SIZE {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Return the covered length in bytes.
    pub fn len(self) -> u32 {
        self.end.saturating_sub(self.start)
    }

    /// Returns `true` when the range does not cover any bytes.
    pub fn is_empty(self) -> bool {
        self.start >= self.end
    }

    /// Returns `true` when `offset` falls inside the range.
    pub fn contains(self, offset: u32) -> bool {
        self.start <= offset && offset < self.end
    }

    /// Returns the intersection between two ranges.
    pub fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        Self::new(start, end)
    }
}

/// Lifecycle state attached to a slice's payload object reference.
///
/// The separate state makes the upload/publication/recovery lifecycle explicit
/// without changing the immutable payload object itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PayloadRefState {
    /// Accepted locally but not yet uploaded.
    #[default]
    LocalOnly,
    /// Upload is in flight.
    Uploading,
    /// Payload has been uploaded and is awaiting publication.
    Uploaded,
    /// Slice is remote-visible.
    Published,
    /// State is being reconstructed after a crash or replay.
    Recovering,
}

impl PayloadRefState {
    /// Return the cache-retention policy implied by this payload lifecycle state.
    pub fn retention_class(self) -> PayloadRetentionClass {
        match self {
            Self::LocalOnly | Self::Uploading | Self::Recovering => {
                PayloadRetentionClass::PinnedUnpublished
            }
            Self::Uploaded => PayloadRetentionClass::PublishReadyRetained,
            Self::Published => PayloadRetentionClass::PublishedWarm,
        }
    }

    /// Return `true` when payload in this state must stay pinned locally.
    pub fn is_pinned_unpublished(self) -> bool {
        matches!(
            self.retention_class(),
            PayloadRetentionClass::PinnedUnpublished
        )
    }
}

/// Per-chunk fragmentation counters and compaction hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChunkFragmentationCounters {
    /// Number of live data-bearing slices in the chunk.
    #[serde(default)]
    pub live_slices: u32,
    /// Number of tombstones stored for the chunk.
    #[serde(default)]
    pub tombstone_slices: u32,
    /// Number of overlapping slices that compaction may want to rewrite.
    #[serde(default)]
    pub overlapping_slices: u32,
    /// Estimated fragmented bytes in the chunk.
    #[serde(default)]
    pub fragmented_bytes: u64,
}

/// Background compaction trigger thresholds for a chunk.
///
/// The defaults intentionally bias toward stable graphs:
/// - at least two live slices,
/// - at least one shadowing overlap or tombstone,
/// - or at least one MiB of shadowed bytes.
pub const DEFAULT_CHUNK_COMPACTION_MIN_LIVE_SLICES: u32 = 2;
/// Minimum overlapping-slice count that triggers chunk compaction.
pub const DEFAULT_CHUNK_COMPACTION_MIN_OVERLAPPING_SLICES: u32 = 1;
/// Minimum tombstone count that triggers chunk compaction.
pub const DEFAULT_CHUNK_COMPACTION_MIN_TOMBSTONE_SLICES: u32 = 1;
/// Minimum fragmented-byte estimate that triggers chunk compaction.
pub const DEFAULT_CHUNK_COMPACTION_MIN_FRAGMENTED_BYTES: u64 = 1024 * 1024;

impl ChunkFragmentationCounters {
    /// Returns `true` when this chunk should be compacted by the background
    /// maintenance pass.
    pub fn should_compact(self) -> bool {
        self.live_slices >= DEFAULT_CHUNK_COMPACTION_MIN_LIVE_SLICES
            && (self.overlapping_slices >= DEFAULT_CHUNK_COMPACTION_MIN_OVERLAPPING_SLICES
                || self.tombstone_slices >= DEFAULT_CHUNK_COMPACTION_MIN_TOMBSTONE_SLICES
                || self.fragmented_bytes >= DEFAULT_CHUNK_COMPACTION_MIN_FRAGMENTED_BYTES)
    }
}

/// One slice entry inside a chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceRecord {
    /// Stable slice identity.
    pub slice_id: SliceId,
    /// Precedence order within the chunk.
    pub order: SliceOrder,
    /// Chunk-relative byte range for the slice.
    pub range: SliceRange,
    /// Payload object reference; `None` represents a tombstone.
    #[serde(default)]
    pub payload: Option<PayloadRef>,
    /// Upload/publication/recovery lifecycle for the referenced payload.
    #[serde(default)]
    pub payload_state: PayloadRefState,
}

impl SliceRecord {
    /// Create a data-bearing slice record.
    pub fn new(
        slice_id: SliceId,
        order: SliceOrder,
        range: SliceRange,
        payload: PayloadRef,
    ) -> Self {
        Self {
            slice_id,
            order,
            range,
            payload: Some(payload),
            payload_state: PayloadRefState::LocalOnly,
        }
    }

    /// Create a tombstone slice record.
    pub fn tombstone(slice_id: SliceId, order: SliceOrder, range: SliceRange) -> Self {
        Self {
            slice_id,
            order,
            range,
            payload: None,
            payload_state: PayloadRefState::LocalOnly,
        }
    }

    /// Returns `true` when this slice removes coverage instead of providing data.
    pub fn is_tombstone(&self) -> bool {
        self.payload.is_none()
    }
}

/// Error returned while constructing chunk-local local slice metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSliceError {
    /// A single slice must cover at least one byte.
    EmptySlice,
    /// A single slice would extend past the end of its chunk.
    CrossesChunkBoundary,
    /// A record for a different inode was pushed into a single-inode batch.
    InodeMismatch {
        /// Inode owned by the batch.
        expected: Ino,
        /// Inode carried by the pushed record.
        actual: Ino,
    },
    /// A file-range or object-range calculation overflowed.
    OffsetOverflow,
}

impl fmt::Display for LocalSliceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySlice => write!(f, "local slice must cover at least one byte"),
            Self::CrossesChunkBoundary => write!(f, "local slice crosses a chunk boundary"),
            Self::InodeMismatch { expected, actual } => write!(
                f,
                "local slice inode mismatch: expected {expected}, got {actual}"
            ),
            Self::OffsetOverflow => write!(f, "local slice offset calculation overflowed"),
        }
    }
}

impl std::error::Error for LocalSliceError {}

/// Chunk-local slice record accepted by the local mounted data plane.
///
/// The record includes inode and chunk identity so writer, reader, flush/fsync,
/// publication, and recovery code can pass slice metadata around without
/// exposing the redb table layout. `payload_object_id == None` represents a
/// tombstone/hole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSliceRecord {
    /// Inode whose regular-file graph receives this slice.
    pub ino: Ino,
    /// Chunk containing this slice.
    pub chunk_id: ChunkId,
    /// Chunk-relative start offset.
    pub chunk_offset: u32,
    /// Slice length in bytes.
    pub length: u32,
    /// Payload object id, or `None` for a tombstone/hole.
    pub payload_object_id: Option<u64>,
    /// Offset inside the payload object, present only for data-bearing slices.
    pub object_offset: Option<u32>,
    /// Caller-side sequence used to preserve accepted order before metadata
    /// assigns durable per-chunk [`SliceOrder`] values.
    pub sequence: u64,
}

impl LocalSliceRecord {
    /// Create a data-bearing chunk-local slice.
    pub fn data(
        ino: Ino,
        chunk_id: ChunkId,
        chunk_offset: u32,
        length: u32,
        payload_object_id: u64,
        object_offset: u32,
    ) -> Result<Self, LocalSliceError> {
        Self::validate_chunk_range(chunk_offset, length)?;
        object_offset
            .checked_add(length)
            .ok_or(LocalSliceError::OffsetOverflow)?;
        Ok(Self {
            ino,
            chunk_id,
            chunk_offset,
            length,
            payload_object_id: Some(payload_object_id),
            object_offset: Some(object_offset),
            sequence: 0,
        })
    }

    /// Create a tombstone/hole chunk-local slice.
    pub fn hole(
        ino: Ino,
        chunk_id: ChunkId,
        chunk_offset: u32,
        length: u32,
    ) -> Result<Self, LocalSliceError> {
        Self::validate_chunk_range(chunk_offset, length)?;
        Ok(Self {
            ino,
            chunk_id,
            chunk_offset,
            length,
            payload_object_id: None,
            object_offset: None,
            sequence: 0,
        })
    }

    fn validate_chunk_range(chunk_offset: u32, length: u32) -> Result<(), LocalSliceError> {
        if length == 0 {
            return Err(LocalSliceError::EmptySlice);
        }
        let end = chunk_offset
            .checked_add(length)
            .ok_or(LocalSliceError::OffsetOverflow)?;
        if end > DEFAULT_CHUNK_SIZE {
            return Err(LocalSliceError::CrossesChunkBoundary);
        }
        Ok(())
    }

    fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = sequence;
        self
    }

    fn to_slice_record(&self) -> SliceRecord {
        let range = SliceRange {
            start: self.chunk_offset,
            end: self.chunk_offset + self.length,
        };
        let payload = self
            .payload_object_id
            .zip(self.object_offset)
            .map(|(object_id, offset)| PayloadRef {
                object_id,
                offset,
                length: self.length,
                flags: BlockFlags::PARTIAL_BLOCK,
            });
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range,
            payload,
            payload_state: PayloadRefState::LocalOnly,
        }
    }
}

/// Ordered local slice batch for one regular file.
///
/// This type is the metadata-facing shape produced by finished slice writers.
/// It can split file-coordinate data and hole ranges into chunk-local records,
/// then convert them into the `(ChunkId, Vec<SliceRecord>)` input expected by
/// atomic local metadata commit APIs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSliceBatch {
    ino: Ino,
    records: Vec<LocalSliceRecord>,
    next_sequence: u64,
}

impl LocalSliceBatch {
    /// Create an empty batch for `ino`.
    pub fn new(ino: Ino) -> Self {
        Self {
            ino,
            records: Vec::new(),
            next_sequence: 0,
        }
    }

    /// Return the inode owned by this batch.
    pub fn ino(&self) -> Ino {
        self.ino
    }

    /// Return the ordered records in this batch.
    pub fn records(&self) -> &[LocalSliceRecord] {
        &self.records
    }

    /// Append one already chunk-local record.
    pub fn push(&mut self, record: LocalSliceRecord) -> Result<(), LocalSliceError> {
        if record.ino != self.ino {
            return Err(LocalSliceError::InodeMismatch {
                expected: self.ino,
                actual: record.ino,
            });
        }
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(LocalSliceError::OffsetOverflow)?;
        self.records.push(record.with_sequence(sequence));
        Ok(())
    }

    /// Append a file-coordinate data range, splitting at chunk boundaries.
    pub fn append_data_range(
        &mut self,
        file_offset: u64,
        length: u32,
        payload_object_id: u64,
        object_offset: u32,
    ) -> Result<(), LocalSliceError> {
        self.append_file_range(
            file_offset,
            length,
            |ino, chunk_id, chunk_offset, segment_len, consumed| {
                let object_offset = (object_offset as u64)
                    .checked_add(consumed)
                    .ok_or(LocalSliceError::OffsetOverflow)?;
                let object_offset =
                    u32::try_from(object_offset).map_err(|_| LocalSliceError::OffsetOverflow)?;
                LocalSliceRecord::data(
                    ino,
                    chunk_id,
                    chunk_offset,
                    segment_len,
                    payload_object_id,
                    object_offset,
                )
            },
        )
    }

    /// Append a file-coordinate tombstone/hole range, splitting at chunk boundaries.
    pub fn append_hole_range(
        &mut self,
        file_offset: u64,
        length: u32,
    ) -> Result<(), LocalSliceError> {
        self.append_file_range(
            file_offset,
            length,
            |ino, chunk_id, chunk_offset, segment_len, _consumed| {
                LocalSliceRecord::hole(ino, chunk_id, chunk_offset, segment_len)
            },
        )
    }

    fn append_file_range<F>(
        &mut self,
        file_offset: u64,
        length: u32,
        mut make_record: F,
    ) -> Result<(), LocalSliceError>
    where
        F: FnMut(Ino, ChunkId, u32, u32, u64) -> Result<LocalSliceRecord, LocalSliceError>,
    {
        if length == 0 {
            return Ok(());
        }
        let end = file_offset
            .checked_add(u64::from(length))
            .ok_or(LocalSliceError::OffsetOverflow)?;
        let mut cursor = file_offset;
        let mut consumed = 0u64;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start
                .checked_add(u64::from(DEFAULT_CHUNK_SIZE))
                .ok_or(LocalSliceError::OffsetOverflow)?;
            let segment_end = end.min(chunk_end);
            let chunk_offset =
                u32::try_from(cursor - chunk_start).map_err(|_| LocalSliceError::OffsetOverflow)?;
            let segment_len =
                u32::try_from(segment_end - cursor).map_err(|_| LocalSliceError::OffsetOverflow)?;
            let record = make_record(self.ino, chunk_id, chunk_offset, segment_len, consumed)?;
            self.push(record)?;
            consumed = consumed
                .checked_add(u64::from(segment_len))
                .ok_or(LocalSliceError::OffsetOverflow)?;
            cursor = segment_end;
        }
        Ok(())
    }

    /// Convert this batch into chunk-grouped metadata records for commit.
    pub fn into_chunk_slices(self) -> Vec<(ChunkId, Vec<SliceRecord>)> {
        let mut chunks: BTreeMap<ChunkId, Vec<SliceRecord>> = BTreeMap::new();
        for record in self.records {
            chunks
                .entry(record.chunk_id)
                .or_default()
                .push(record.to_slice_record());
        }
        chunks.into_iter().collect()
    }
}

/// Chunk-local slice state for the local overlay view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegularFileOverlayState {
    /// Chunk graph keyed by chunk identity.
    #[serde(default)]
    pub chunks: BTreeMap<ChunkId, RegularFileChunkState>,
}

/// Chunk-local slice state for the remote-visible base view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegularFileBaseState {
    /// Chunk graph keyed by chunk identity.
    #[serde(default)]
    pub chunks: BTreeMap<ChunkId, RegularFileChunkState>,
}

/// Per-chunk slice state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegularFileChunkState {
    /// Next slice id to allocate for this chunk.
    pub next_slice_id: SliceId,
    /// Next slice order to allocate for this chunk.
    pub next_slice_order: SliceOrder,
    /// Slice records keyed by slice identity.
    #[serde(default)]
    pub slices: BTreeMap<SliceId, SliceRecord>,
    /// Fragmentation counters and future compaction hooks.
    #[serde(default)]
    pub fragmentation: ChunkFragmentationCounters,
}

impl RegularFileChunkState {
    fn insert_covered_range(covered: &mut Vec<SliceRange>, range: SliceRange) {
        if range.is_empty() {
            return;
        }

        let mut start = range.start;
        let mut end = range.end;
        let mut idx = 0;
        while idx < covered.len() {
            let current = covered[idx];
            if current.end < start {
                idx += 1;
                continue;
            }
            if end < current.start {
                break;
            }
            start = start.min(current.start);
            end = end.max(current.end);
            covered.remove(idx);
        }
        covered.insert(idx, SliceRange { start, end });
    }

    /// Recompute the per-chunk fragmentation counters from the current slice set.
    pub fn fragmentation_metrics(&self) -> ChunkFragmentationCounters {
        let mut slices: Vec<&SliceRecord> = self.slices.values().collect();
        slices.sort_unstable_by(|left, right| {
            right
                .order
                .cmp(&left.order)
                .then_with(|| right.slice_id.cmp(&left.slice_id))
        });

        let mut covered: Vec<SliceRange> = Vec::new();
        let mut counters = ChunkFragmentationCounters::default();

        for slice in slices {
            let shadowed_bytes = covered
                .iter()
                .filter_map(|range| range.intersection(slice.range))
                .map(|range| u64::from(range.len()))
                .sum::<u64>();
            if shadowed_bytes > 0 {
                counters.overlapping_slices = counters.overlapping_slices.saturating_add(1);
            }
            if slice.payload.is_some() {
                counters.live_slices = counters.live_slices.saturating_add(1);
                counters.fragmented_bytes =
                    counters.fragmented_bytes.saturating_add(shadowed_bytes);
            } else {
                counters.tombstone_slices = counters.tombstone_slices.saturating_add(1);
            }
            Self::insert_covered_range(&mut covered, slice.range);
        }

        counters
    }

    /// Refresh the cached fragmentation counters in-place.
    pub fn refresh_fragmentation_metrics(&mut self) {
        self.fragmentation = self.fragmentation_metrics();
    }

    /// Try to allocate the next slice id.
    ///
    /// Returns `None` if the counter would overflow.
    pub fn try_alloc_slice_id(&mut self) -> Option<SliceId> {
        let id = self.next_slice_id;
        self.next_slice_id = SliceId(self.next_slice_id.0.checked_add(1)?);
        Some(id)
    }

    /// Allocate the next slice id.
    ///
    /// Panics if the counter overflows. Prefer [`Self::try_alloc_slice_id`] when
    /// the caller wants to handle exhaustion explicitly.
    pub fn alloc_slice_id(&mut self) -> SliceId {
        self.try_alloc_slice_id().expect("slice id overflow")
    }

    /// Try to allocate the next slice order.
    ///
    /// Returns `None` if the counter would overflow.
    pub fn try_alloc_slice_order(&mut self) -> Option<SliceOrder> {
        let order = self.next_slice_order;
        self.next_slice_order = SliceOrder(self.next_slice_order.0.checked_add(1)?);
        Some(order)
    }

    /// Allocate the next slice order.
    ///
    /// Panics if the counter overflows. Prefer
    /// [`Self::try_alloc_slice_order`] when the caller wants to handle
    /// exhaustion explicitly.
    pub fn alloc_slice_order(&mut self) -> SliceOrder {
        self.try_alloc_slice_order().expect("slice order overflow")
    }

    /// Insert a slice record, keyed by its slice id.
    pub fn insert_slice(&mut self, slice: SliceRecord) -> Option<SliceRecord> {
        self.slices.insert(slice.slice_id, slice)
    }

    /// Returns `true` when the chunk carries no slices.
    pub fn is_empty(&self) -> bool {
        self.slices.is_empty()
    }
}

/// Full regular-file state with explicit overlay and base views.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RegularFileState {
    #[serde(default)]
    /// Locally durable overlay chunks that shadow the published base.
    pub overlay: RegularFileOverlayState,
    #[serde(default)]
    /// Remote-visible published chunk graph.
    pub base: RegularFileBaseState,
}

/// Visibility tier that produced a resolved slice span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolvedSliceSource {
    /// Bytes were provided by the local overlay.
    Overlay,
    /// Bytes were provided by the published base.
    Base,
    /// Coverage was masked by an overlay tombstone.
    OverlayTombstone,
    /// Coverage was masked by a base tombstone.
    BaseTombstone,
    /// No slice covered this range.
    Hole,
}

/// Deterministic resolved span for one chunk-local byte range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSliceSpan {
    /// Chunk containing this span.
    pub chunk_id: ChunkId,
    /// Chunk-relative covered range.
    pub range: SliceRange,
    /// Source tier that won this range.
    pub source: ResolvedSliceSource,
    /// Winning slice id when the span comes from a slice.
    #[serde(default)]
    pub slice_id: Option<SliceId>,
    /// Winning slice order when the span comes from a slice.
    #[serde(default)]
    pub slice_order: Option<SliceOrder>,
    /// Payload ref backing the span, clipped to `range` when data-bearing.
    #[serde(default)]
    pub payload: Option<PayloadRef>,
}

#[derive(Debug, Clone, Copy)]
enum ResolveTier {
    Overlay,
    Base,
}

#[derive(Debug, Clone)]
struct ResolveCandidate {
    tier: ResolveTier,
    slice_id: SliceId,
    slice_order: SliceOrder,
    range: SliceRange,
    payload: Option<PayloadRef>,
}

impl ResolveCandidate {
    fn source(&self) -> ResolvedSliceSource {
        match (self.tier, self.payload.as_ref()) {
            (ResolveTier::Overlay, Some(_)) => ResolvedSliceSource::Overlay,
            (ResolveTier::Base, Some(_)) => ResolvedSliceSource::Base,
            (ResolveTier::Overlay, None) => ResolvedSliceSource::OverlayTombstone,
            (ResolveTier::Base, None) => ResolvedSliceSource::BaseTombstone,
        }
    }

    // Higher tuple wins. `slice_id` is the deterministic tie-breaker when
    // multiple slices share the same tier and order.
    fn precedence_key(&self) -> (u8, u64, u64) {
        let tier = match self.tier {
            ResolveTier::Overlay => 2,
            ResolveTier::Base => 1,
        };
        (tier, self.slice_order.0, self.slice_id.0)
    }
}

fn push_candidate_set(
    out: &mut Vec<ResolveCandidate>,
    chunk: Option<&RegularFileChunkState>,
    tier: ResolveTier,
    query: SliceRange,
) {
    let Some(chunk) = chunk else {
        return;
    };
    for slice in chunk.slices.values() {
        if slice.range.intersection(query).is_some() {
            out.push(ResolveCandidate {
                tier,
                slice_id: slice.slice_id,
                slice_order: slice.order,
                range: slice.range,
                payload: slice.payload,
            });
        }
    }
}

fn subtract_range(base: SliceRange, cut: SliceRange, out: &mut Vec<SliceRange>) {
    if let Some(overlap) = base.intersection(cut) {
        if base.start < overlap.start
            && let Some(left) = SliceRange::new(base.start, overlap.start)
        {
            out.push(left);
        }
        if overlap.end < base.end
            && let Some(right) = SliceRange::new(overlap.end, base.end)
        {
            out.push(right);
        }
    } else {
        out.push(base);
    }
}

/// Merge adjacent resolved spans with identical metadata.
///
/// Spans are sorted by `(chunk_id, start, end)` before merging.
/// This merge is intentionally chunk-local: spans with different `chunk_id`
/// are never coalesced even when their file offsets are contiguous.
pub fn merge_adjacent_spans(mut spans: Vec<ResolvedSliceSpan>) -> Vec<ResolvedSliceSpan> {
    spans.sort_by_key(|span| (span.chunk_id.0, span.range.start, span.range.end));
    let mut merged: Vec<ResolvedSliceSpan> = Vec::with_capacity(spans.len());
    for span in spans {
        if let Some(last) = merged.last_mut()
            && last.chunk_id == span.chunk_id
            && last.range.end == span.range.start
            && last.source == span.source
            && last.slice_id == span.slice_id
            && last.slice_order == span.slice_order
            && last.payload == span.payload
        {
            last.range.end = span.range.end;
            continue;
        }
        merged.push(span);
    }
    merged
}

/// Resolve deterministic chunk-local coverage for a read query.
///
/// Rules:
/// - overlay beats base
/// - within each tier, later slice order wins
/// - tombstones remove coverage before lower-precedence slices
/// - uncovered bytes are returned as explicit hole spans
pub fn resolve_chunk_slice_spans(
    chunk_id: ChunkId,
    query: SliceRange,
    overlay: Option<&RegularFileChunkState>,
    base: Option<&RegularFileChunkState>,
) -> Vec<ResolvedSliceSpan> {
    let mut candidates = Vec::new();
    push_candidate_set(&mut candidates, overlay, ResolveTier::Overlay, query);
    push_candidate_set(&mut candidates, base, ResolveTier::Base, query);
    candidates.sort_by_key(|candidate| Reverse(candidate.precedence_key()));

    let mut unresolved = vec![query];
    let mut resolved = Vec::new();

    for candidate in candidates {
        if unresolved.is_empty() {
            break;
        }
        let mut next_unresolved = Vec::new();
        for pending in unresolved {
            if let Some(overlap) = pending.intersection(candidate.range) {
                subtract_range(pending, overlap, &mut next_unresolved);
                let payload = candidate.payload.as_ref().map(|payload| {
                    let delta = overlap.start.saturating_sub(candidate.range.start);
                    PayloadRef {
                        object_id: payload.object_id,
                        offset: payload.offset.saturating_add(delta),
                        length: overlap.len(),
                        flags: payload.flags,
                    }
                });
                resolved.push(ResolvedSliceSpan {
                    chunk_id,
                    range: overlap,
                    source: candidate.source(),
                    slice_id: Some(candidate.slice_id),
                    slice_order: Some(candidate.slice_order),
                    payload,
                });
            } else {
                next_unresolved.push(pending);
            }
        }
        unresolved = next_unresolved;
    }

    for hole in unresolved {
        resolved.push(ResolvedSliceSpan {
            chunk_id,
            range: hole,
            source: ResolvedSliceSource::Hole,
            slice_id: None,
            slice_order: None,
            payload: None,
        });
    }

    merge_adjacent_spans(resolved)
}

// =============================================================================
// Format Info
// =============================================================================

/// Volume format information - stored in FORMAT_INFO table.
///
/// Created at volume initialization, immutable thereafter.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatInfo {
    /// Schema format version for compatibility checks.
    pub format_version: u16,
    /// Volume name (user-friendly identifier).
    pub name: String,
    /// Unique volume UUID.
    pub uuid: String,
    /// S3 endpoint URL.
    pub endpoint: String,
    /// S3 bucket name.
    pub bucket: String,
    /// S3 access key.
    pub access_key: String,
    /// S3 secret key.
    pub secret_key: String,
    /// S3 region.
    pub region: String,
    /// Optional prefix for data objects in bucket.
    #[serde(default)]
    pub data_prefix: String,
    /// Metadata backend configuration.
    #[serde(default)]
    pub metadata: MetadataConfig,
    /// Transparent remote-object encryption settings.
    pub encryption: EncryptionConfig,
    /// Block size in bytes (configurable at init, default 16MB).
    pub block_size: u32,
}

impl fmt::Debug for FormatInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FormatInfo")
            .field("format_version", &self.format_version)
            .field("name", &self.name)
            .field("uuid", &self.uuid)
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &"***")
            .field("secret_key", &"***")
            .field("region", &self.region)
            .field("data_prefix", &self.data_prefix)
            .field("metadata", &self.metadata)
            .field("encryption", &self.encryption)
            .field("block_size", &self.block_size)
            .finish()
    }
}

/// Supported transparent remote-object encryption algorithm.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EncryptionAlgorithm {
    /// XChaCha20-Poly1305 envelope format v1.
    #[serde(rename = "xchacha20poly1305-v1")]
    XChaCha20Poly1305V1,
}

impl EncryptionAlgorithm {
    /// Stable metadata string stored for persisted key records.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::XChaCha20Poly1305V1 => "xchacha20poly1305-v1",
        }
    }
}

/// Encryption format validation failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EncryptionContractError {
    /// Encryption was disabled in persisted volume metadata.
    #[error("encryption must be enabled")]
    Disabled,
    /// The persisted encryption algorithm is not supported by this binary.
    #[error("unsupported encryption algorithm")]
    UnsupportedAlgorithm,
    /// The persisted active key id is not supported by this binary.
    #[error("unsupported encryption key id: found {found}, expected {expected}")]
    UnsupportedKeyId {
        /// Key id stored in metadata.
        found: u32,
        /// Key id expected by this binary.
        expected: u32,
    },
    /// The persisted encryption format version is not supported by this binary.
    #[error("unsupported encryption format version: found {found}, expected {expected}")]
    UnsupportedFormatVersion {
        /// Encryption format version stored in metadata.
        found: u16,
        /// Encryption format version expected by this binary.
        expected: u16,
    },
    /// Operating-system randomness was unavailable while generating volume key material.
    #[error("operating-system randomness unavailable")]
    OsRandomUnavailable,
}

/// Persisted encryption configuration for a formatted volume.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    /// Remote-object encryption algorithm.
    #[zeroize(skip)]
    pub algorithm: EncryptionAlgorithm,
    /// Active key identifier used by the initial implementation.
    pub active_key_id: u32,
    /// Encryption envelope format version.
    pub format_version: u16,
    /// Automatically generated 256-bit volume key material.
    pub volume_master_key: [u8; ENCRYPTION_KEY_LENGTH],
    /// Whether transparent encryption is enabled for the volume.
    pub enabled: bool,
}

impl fmt::Debug for EncryptionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EncryptionConfig")
            .field("algorithm", &self.algorithm)
            .field("active_key_id", &self.active_key_id)
            .field("format_version", &self.format_version)
            .field("volume_master_key", &"***")
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl EncryptionConfig {
    /// Build the current supported encryption configuration from key material.
    pub fn new(volume_master_key: [u8; ENCRYPTION_KEY_LENGTH]) -> Self {
        Self {
            algorithm: EncryptionAlgorithm::XChaCha20Poly1305V1,
            active_key_id: CURRENT_ENCRYPTION_KEY_ID,
            format_version: CURRENT_ENCRYPTION_FORMAT_VERSION,
            volume_master_key,
            enabled: true,
        }
    }

    /// Generate a current encryption configuration with random volume key material.
    pub fn generate() -> Result<Self, EncryptionContractError> {
        Self::try_generate()
    }

    /// Generate a current encryption configuration with random volume key material.
    pub fn try_generate() -> Result<Self, EncryptionContractError> {
        let mut volume_master_key = [0u8; ENCRYPTION_KEY_LENGTH];
        OsRng
            .try_fill_bytes(&mut volume_master_key)
            .map_err(|_| EncryptionContractError::OsRandomUnavailable)?;
        Ok(Self::new(volume_master_key))
    }

    /// Validate that this config matches the only supported encryption contract.
    pub fn validate(&self) -> Result<(), EncryptionContractError> {
        if !self.enabled {
            return Err(EncryptionContractError::Disabled);
        }
        if self.algorithm != EncryptionAlgorithm::XChaCha20Poly1305V1 {
            return Err(EncryptionContractError::UnsupportedAlgorithm);
        }
        if self.active_key_id != CURRENT_ENCRYPTION_KEY_ID {
            return Err(EncryptionContractError::UnsupportedKeyId {
                found: self.active_key_id,
                expected: CURRENT_ENCRYPTION_KEY_ID,
            });
        }
        if self.format_version != CURRENT_ENCRYPTION_FORMAT_VERSION {
            return Err(EncryptionContractError::UnsupportedFormatVersion {
                found: self.format_version,
                expected: CURRENT_ENCRYPTION_FORMAT_VERSION,
            });
        }
        Ok(())
    }
}

/// S3-style data-store settings used when constructing new [`FormatInfo`] values.
#[derive(Clone, PartialEq, Eq)]
pub struct FormatInfoDataStore {
    /// Object-store endpoint URL.
    pub endpoint: String,
    /// Object-store bucket/container name.
    pub bucket: String,
    /// Access key or equivalent client id.
    pub access_key: String,
    /// Secret key or equivalent client secret.
    pub secret_key: String,
    /// Cloud region used for requests.
    pub region: String,
}

impl std::fmt::Debug for FormatInfoDataStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormatInfoDataStore")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &"***")
            .field("secret_key", &"***")
            .field("region", &self.region)
            .finish()
    }
}

/// Metadata backend selection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetadataBackend {
    #[default]
    /// Use the embedded redb-only metadata backend.
    RedbLocal,
    /// Use PostgreSQL as the authoritative shared metadata backend.
    Postgres,
}

/// Connection details for remote metadata object store.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteStoreConfig {
    /// Metadata object-store endpoint URL.
    pub endpoint: String,
    /// Metadata object-store bucket/container name.
    pub bucket: String,
    /// Access key or equivalent client id.
    pub access_key: String,
    /// Secret key or equivalent client secret.
    pub secret_key: String,
    /// Cloud region used for requests.
    pub region: String,
    #[serde(default = "default_metadata_prefix")]
    /// Prefix under which metadata objects are stored.
    pub prefix: String,
}

impl fmt::Debug for RemoteStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteStoreConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("access_key", &"***")
            .field("secret_key", &"***")
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .finish()
    }
}

/// Metadata runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MetadataConfig {
    #[serde(default)]
    /// Selected metadata backend implementation.
    pub backend: MetadataBackend,
    #[serde(default)]
    /// Optional remote object-store config for shared metadata artifacts.
    pub remote: Option<RemoteStoreConfig>,
    #[serde(default)]
    /// Optional PostgreSQL authority configuration.
    pub postgres: Option<PostgresConfig>,
}

/// Connection details for PostgreSQL metadata backend.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// PostgreSQL server DSN (e.g. postgres://user:pass@host:5432).
    /// StudioFS rewrites it to database `studiofs`.
    pub url: String,
    /// Logical volume id in shared PG schema.
    pub volume_id: i64,
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("url", &"***")
            .field("volume_id", &self.volume_id)
            .finish()
    }
}

fn default_metadata_prefix() -> String {
    "metadata/".to_string()
}

impl FormatInfo {
    /// Create a new format info with the specified parameters.
    pub fn new(
        name: String,
        uuid: String,
        data_store: FormatInfoDataStore,
        block_size_mb: u32,
    ) -> Result<Self, EncryptionContractError> {
        Ok(Self {
            format_version: CURRENT_FORMAT_VERSION,
            name,
            uuid,
            endpoint: data_store.endpoint,
            bucket: data_store.bucket,
            access_key: data_store.access_key,
            secret_key: data_store.secret_key,
            region: data_store.region,
            data_prefix: String::new(),
            metadata: MetadataConfig::default(),
            encryption: EncryptionConfig::generate()?,
            block_size: block_size_mb * 1024 * 1024, // Convert MB to bytes
        })
    }

    /// Validate metadata format contracts enforced at mount/open boundaries.
    pub fn validate_format_contract(&self) -> Result<(), EncryptionContractError> {
        self.encryption.validate()
    }

    /// Get block size in megabytes (for display purposes).
    pub fn block_size_mb(&self) -> u32 {
        self.block_size / (1024 * 1024)
    }
}

// =============================================================================
// Counter Names
// =============================================================================

/// Well-known counter names in the metadata store.
pub mod counters {
    /// Next available inode number.
    pub const NEXT_INODE: &str = "nextInode";
    /// Legacy next object ID counter. New object IDs share the inode allocation stream.
    pub const NEXT_OBJECT_ID: &str = "nextObjectId";
    /// Total used space in bytes.
    pub const USED_SPACE: &str = "usedSpace";
    /// Total number of inodes in use.
    pub const TOTAL_INODES: &str = "totalInodes";
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Calculate the block index for a given file offset.
#[inline]
pub fn block_index_from_offset(offset: u64, block_size: u32) -> u32 {
    (offset / block_size as u64) as u32
}

/// Calculate the offset within a block for a given file offset.
#[inline]
pub fn offset_within_block(offset: u64, block_size: u32) -> u32 {
    (offset % block_size as u64) as u32
}

/// Calculate the chunk id for a given file offset.
#[inline]
pub fn chunk_id_from_offset(offset: u64, chunk_size: u32) -> ChunkId {
    ChunkId(offset / chunk_size as u64)
}

/// Calculate the offset within a chunk for a given file offset.
#[inline]
pub fn offset_within_chunk(offset: u64, chunk_size: u32) -> u32 {
    (offset % chunk_size as u64) as u32
}

/// Calculate the starting file offset for a chunk.
#[inline]
pub fn chunk_start_offset(chunk_id: ChunkId, chunk_size: u32) -> u64 {
    chunk_id.0.saturating_mul(chunk_size as u64)
}

// =============================================================================
// Directory Entries (with Name)
// =============================================================================

/// Directory entry returned by readdir — includes the entry name.
/// Stored in DENTRIES_TABLE as (parent_ino, name_bytes) -> DirEntryValue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    /// Entry name (filename).
    pub name: Vec<u8>,
    /// Inode number of the child.
    pub ino: Ino,
    /// File type of the child.
    pub kind: FileType,
}

/// Result of an atomic rename operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameAtomicResult {
    /// Inode that was moved to the destination path.
    pub moved_ino: Ino,
    /// Inode displaced at the destination path, if any.
    pub displaced_ino: Option<Ino>,
}

/// Replication lifecycle of a filesystem snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotState {
    /// Snapshot is created locally and awaiting remote replication.
    #[default]
    Pending,
    /// Snapshot definition and pinned blocks are remotely visible.
    Synced,
    /// Snapshot replication failed and needs retry.
    Failed,
}

/// Immutable filesystem snapshot descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataSnapshot {
    /// Globally unique snapshot id (UUID string).
    pub id: String,
    /// Inode chosen as snapshot mount root.
    pub root_ino: Ino,
    /// Creation time in Unix-nanos.
    pub created_at: i64,
    /// Replication state.
    pub state: SnapshotState,
    /// Object ids pinned by this snapshot.
    pub pinned_object_ids: Vec<u64>,
    /// Serialized metadata payload for historical mounts.
    #[serde(default)]
    pub snapshot_payload: Vec<u8>,
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_index_calculation() {
        let block_size = 4 * 1024 * 1024; // 4MB

        assert_eq!(block_index_from_offset(0, block_size), 0);
        assert_eq!(block_index_from_offset(1, block_size), 0);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024, block_size), 1);
        assert_eq!(block_index_from_offset(4 * 1024 * 1024 + 1, block_size), 1);
        assert_eq!(block_index_from_offset(16 * 1024 * 1024, block_size), 4);
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
    fn test_payload_ref_flags() {
        let full_payload_ref = PayloadRef::new_full(123, 4 * 1024 * 1024);
        assert!(!full_payload_ref.is_partial());
        assert!(!full_payload_ref.is_compressed());

        let partial_payload_ref = PayloadRef::new_partial(124, 1000);
        assert!(partial_payload_ref.is_partial());
        assert!(!partial_payload_ref.is_compressed());
    }

    #[test]
    fn test_chunk_and_slice_helpers() {
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let chunk = chunk_id_from_offset(chunk_size as u64 + 123, chunk_size);
        assert_eq!(chunk.0, 1);
        assert_eq!(
            offset_within_chunk(chunk_size as u64 + 123, chunk_size),
            123
        );
        assert_eq!(chunk_start_offset(chunk, chunk_size), chunk_size as u64);

        let range = SliceRange::new(10, 42).expect("valid range");
        assert_eq!(range.len(), 32);
        assert!(range.contains(10));
        assert!(range.contains(41));
        assert!(!range.contains(42));
        assert!(SliceRange::new(5, 5).is_none());
        assert!(SliceRange::new(9, 3).is_none());
        assert!(SliceRange::new(0, DEFAULT_CHUNK_SIZE).is_some());
        assert!(SliceRange::new(0, DEFAULT_CHUNK_SIZE + 1).is_none());
        let a = SliceRange::new(100, 300).expect("range a");
        let b = SliceRange::new(250, 450).expect("range b");
        assert_eq!(a.intersection(b), SliceRange::new(250, 300));
    }

    #[test]
    fn test_resolve_chunk_slice_spans_overlay_over_base_with_hole() {
        let query = SliceRange::new(0, 8192).expect("query range");

        let mut base_chunk = RegularFileChunkState::default();
        base_chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 8192).expect("base range"),
            PayloadRef::new_partial(2001, 8192),
        ));

        let mut overlay_chunk = RegularFileChunkState::default();
        overlay_chunk.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(1),
            SliceRange::new(1024, 3072).expect("overlay range"),
            PayloadRef::new_partial(2002, 2048),
        ));
        overlay_chunk.insert_slice(SliceRecord::tombstone(
            SliceId(3),
            SliceOrder(2),
            SliceRange::new(4096, 5120).expect("tombstone range"),
        ));

        let spans =
            resolve_chunk_slice_spans(ChunkId(0), query, Some(&overlay_chunk), Some(&base_chunk));
        assert_eq!(spans.len(), 5);
        assert_eq!(
            spans[0].range,
            SliceRange {
                start: 0,
                end: 1024
            }
        );
        assert_eq!(spans[0].source, ResolvedSliceSource::Base);
        assert_eq!(
            spans[1].range,
            SliceRange {
                start: 1024,
                end: 3072
            }
        );
        assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
        assert_eq!(
            spans[2].range,
            SliceRange {
                start: 3072,
                end: 4096
            }
        );
        assert_eq!(spans[2].source, ResolvedSliceSource::Base);
        assert_eq!(
            spans[3].range,
            SliceRange {
                start: 4096,
                end: 5120
            }
        );
        assert_eq!(spans[3].source, ResolvedSliceSource::OverlayTombstone);
        assert_eq!(
            spans[4].range,
            SliceRange {
                start: 5120,
                end: 8192
            }
        );
        assert_eq!(spans[4].source, ResolvedSliceSource::Base);
    }

    #[test]
    fn test_regular_file_chunk_fragmentation_metrics_count_overlaps_and_tombstones() {
        let mut chunk = RegularFileChunkState::default();
        chunk.insert_slice(SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("first slice"),
            PayloadRef::new_partial(1001, 4096),
        ));
        chunk.insert_slice(SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, 4096).expect("shadowed slice"),
            PayloadRef::new_partial(1002, 4096),
        ));
        chunk.insert_slice(SliceRecord::tombstone(
            SliceId(3),
            SliceOrder(3),
            SliceRange::new(4096, 6144).expect("tombstone range"),
        ));

        let metrics = chunk.fragmentation_metrics();
        assert_eq!(metrics.live_slices, 2);
        assert_eq!(metrics.tombstone_slices, 1);
        assert_eq!(metrics.overlapping_slices, 1);
        assert_eq!(metrics.fragmented_bytes, 4096);
        assert!(metrics.should_compact());

        chunk.refresh_fragmentation_metrics();
        assert_eq!(chunk.fragmentation, metrics);
    }

    #[test]
    fn test_regular_file_chunk_state_allocators_detect_overflow() {
        let mut chunk = RegularFileChunkState {
            next_slice_id: SliceId(u64::MAX),
            next_slice_order: SliceOrder(u64::MAX),
            ..Default::default()
        };

        assert!(chunk.try_alloc_slice_id().is_none());
        assert!(chunk.try_alloc_slice_order().is_none());
    }

    #[test]
    fn test_local_slice_record_rejects_chunk_boundary_crossing() {
        let err = LocalSliceRecord::data(42, ChunkId(7), DEFAULT_CHUNK_SIZE - 16, 32, 9001, 0)
            .expect_err("single slice must not cross a chunk boundary");

        assert_eq!(err, LocalSliceError::CrossesChunkBoundary);
    }

    #[test]
    fn test_local_slice_batch_splits_data_and_holes_by_chunk() {
        let mut batch = LocalSliceBatch::new(42);
        batch
            .append_data_range(DEFAULT_CHUNK_SIZE as u64 - 8, 24, 9001, 4)
            .expect("split data write");
        batch
            .append_hole_range(DEFAULT_CHUNK_SIZE as u64 + 24, 16)
            .expect("split hole");

        let records = batch.records();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].chunk_id, ChunkId(0));
        assert_eq!(records[0].chunk_offset, DEFAULT_CHUNK_SIZE - 8);
        assert_eq!(records[0].length, 8);
        assert_eq!(records[0].object_offset, Some(4));
        assert_eq!(records[1].chunk_id, ChunkId(1));
        assert_eq!(records[1].chunk_offset, 0);
        assert_eq!(records[1].length, 16);
        assert_eq!(records[1].object_offset, Some(12));
        assert_eq!(records[2].chunk_id, ChunkId(1));
        assert_eq!(records[2].chunk_offset, 24);
        assert_eq!(records[2].length, 16);
        assert!(records[2].payload_object_id.is_none());

        let chunk_slices = batch.into_chunk_slices();
        assert_eq!(chunk_slices.len(), 2);
        assert_eq!(chunk_slices[0].0, ChunkId(0));
        assert_eq!(chunk_slices[0].1.len(), 1);
        assert_eq!(
            chunk_slices[0].1[0].range,
            SliceRange::new(DEFAULT_CHUNK_SIZE - 8, DEFAULT_CHUNK_SIZE).unwrap()
        );
        assert_eq!(
            chunk_slices[0].1[0].payload.as_ref().unwrap().object_id,
            9001
        );
        assert_eq!(chunk_slices[0].1[0].payload.as_ref().unwrap().offset, 4);
        assert_eq!(chunk_slices[1].0, ChunkId(1));
        assert_eq!(chunk_slices[1].1.len(), 2);
        assert_eq!(chunk_slices[1].1[0].range, SliceRange::new(0, 16).unwrap());
        assert_eq!(chunk_slices[1].1[0].payload.as_ref().unwrap().offset, 12);
        assert!(chunk_slices[1].1[1].is_tombstone());
        assert_eq!(chunk_slices[1].1[1].range, SliceRange::new(24, 40).unwrap());
    }

    #[test]
    fn test_local_slice_batch_rejects_other_inode_records() {
        let mut batch = LocalSliceBatch::new(42);
        let record = LocalSliceRecord::hole(43, ChunkId(0), 0, 16).expect("valid chunk-local hole");
        let err = batch
            .push(record)
            .expect_err("batch must only contain records for one inode");

        assert_eq!(
            err,
            LocalSliceError::InodeMismatch {
                expected: 42,
                actual: 43
            }
        );
    }

    #[test]
    fn test_regular_file_state_roundtrip() {
        let block = PayloadRef::new_partial(9001, 4096);
        let slice = SliceRecord::new(
            SliceId(7),
            SliceOrder(3),
            SliceRange::new(0, 4096).expect("valid slice"),
            block,
        );

        let mut slices = BTreeMap::new();
        slices.insert(slice.slice_id, slice);
        let chunk = RegularFileChunkState {
            next_slice_id: SliceId(8),
            next_slice_order: SliceOrder(4),
            slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 0,
                overlapping_slices: 0,
                fragmented_bytes: 4096,
            },
        };

        let mut overlay = RegularFileOverlayState::default();
        overlay.chunks.insert(ChunkId(1), chunk.clone());

        let mut base = RegularFileBaseState::default();
        base.chunks.insert(ChunkId(1), chunk);

        let state = RegularFileState { overlay, base };
        let encoded = postcard::to_stdvec(&state).expect("encode regular file state");
        let decoded: RegularFileState =
            postcard::from_bytes(&encoded).expect("decode regular file state");

        assert_eq!(decoded, state);
        assert_eq!(
            decoded
                .overlay
                .chunks
                .get(&ChunkId(1))
                .unwrap()
                .slices
                .len(),
            1
        );
        assert_eq!(
            decoded
                .overlay
                .chunks
                .get(&ChunkId(1))
                .unwrap()
                .slices
                .get(&SliceId(7))
                .unwrap()
                .payload
                .as_ref()
                .unwrap()
                .object_id,
            9001
        );
    }

    #[test]
    fn test_inode_attr_times() {
        let attr = InodeAttr::new_file(2, 1, 1000, 1000, 0o644);
        assert!(attr.mtime > 0);
        assert!(attr.ctime > 0);
        assert!(attr.atime > 0);
    }

    #[test]
    fn test_inode_attr_upload_pending_requires_unpublished_regular_file() {
        let mut attr = InodeAttr::new_file(2, 1, 1000, 1000, 0o644);
        assert!(!attr.is_upload_pending());

        attr.upload_state = UploadState::Uploading;
        attr.size = 1;
        attr.content_epoch = 1;
        assert!(attr.is_upload_pending());

        attr.committed_size = attr.size;
        attr.committed_content_epoch = attr.content_epoch;
        assert!(!attr.is_upload_pending());

        attr.kind = FileType::Directory;
        assert!(!attr.is_upload_pending());
    }

    #[test]
    fn credential_bearing_format_debug_omits_all_secret_values() {
        let access_key = "access-key-sentinel";
        let secret_key = "secret-key-sentinel";
        let dsn = "postgres://user:dsn-password-sentinel@db.example.com/studiofs";
        let mut info = FormatInfo::new(
            "volume".to_string(),
            "volume-uuid".to_string(),
            FormatInfoDataStore {
                endpoint: "https://s3.example.com".to_string(),
                bucket: "bucket".to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                region: "region".to_string(),
            },
            16,
        )
        .expect("build format info");
        info.metadata.remote = Some(RemoteStoreConfig {
            endpoint: "https://metadata.example.com".to_string(),
            bucket: "metadata".to_string(),
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region: "region".to_string(),
            prefix: "metadata/".to_string(),
        });
        info.metadata.postgres = Some(PostgresConfig {
            url: dsn.to_string(),
            volume_id: 7,
        });

        let debug = format!("{info:?}");

        assert!(!debug.contains(access_key));
        assert!(!debug.contains(secret_key));
        assert!(!debug.contains(dsn));
        assert!(debug.contains("***"));
    }
}
