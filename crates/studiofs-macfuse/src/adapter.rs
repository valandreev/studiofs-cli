use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString, OsStr};
use std::fmt;
use std::mem::{self, MaybeUninit};
#[cfg(test)]
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::os::raw::{c_char, c_ulong};
use std::os::unix::ffi::OsStrExt;
use std::sync::{
    Arc, Condvar, Mutex, OnceLock,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::{DashMap, mapref::entry::Entry as DashMapEntry};
use libc::{mode_t, off_t, size_t, stat, statvfs};
#[cfg(test)]
use lru::LruCache;
use parking_lot::Mutex as ParkingMutex;
use studiofs_bridge::fuse_open_reply::{FuseOpenReplyGuard, FuseOpenReplySerializer};
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::engine::XATTR_CREATE_FLAG;
use studiofs_meta::schema::ROOT_INO;
use studiofs_meta::{DirEntry, FileType, InodeAttr, SetattrArgs, TimeSpec, UploadState};
use studiofs_vfs::vfs::{
    OsCacheDisposition, OsCacheOpenDecision, OsCacheParticipation, OsCacheReason,
};
use studiofs_vfs::{
    KernelInvalidator, LocalSyncPolicy, StatFs, StudioVfs, VfsFuseAsyncReadSnapshot,
    VfsMacFuseOpenCacheDecision, VfsResult,
};
use sysinfo::{MemoryRefreshKind, System};
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, SemaphorePermit};
#[cfg(test)]
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::low_level::{DirectoryEntryBuffer, Entry, OpenReply, Reply, Request, raw};

const TTL_SECONDS: f64 = 1.0;
const MACFUSE_ATTR_BLOCK_SIZE: u32 = crate::low_level::MACOS_FUSE_IOSIZE_BYTES;
const MACFUSE_READ_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS: usize = 32;
const MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY: usize = 8 * 1024 * 1024;
// Keep allocator-rounded buffers that grew slightly beyond the reusable read
// cap, while dropping genuinely oversized requests instead of pinning them.
const MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY: usize = MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY;
const MACFUSE_ASYNC_READ_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);
const MACFUSE_AVAILABLE_MEMORY_TTL: Duration = Duration::from_secs(1);
const MACFUSE_READ_INTERRUPTED_ERRNO: i32 = libc::EINTR;
const MACFUSE_SYMLINK_RETRY_ATTEMPTS: u16 = 200;
const MACFUSE_SYMLINK_RETRY_TIMEOUT: Duration = Duration::from_secs(5);
const MACFUSE_SYMLINK_RETRY_DELAY: Duration = Duration::from_millis(25);
const MACOS_PROVENANCE_XATTR: &[u8] = b"com.apple.provenance";
const MACOS_DS_STORE_NAME: &[u8] = b".DS_Store";
const MACOS_UF_HIDDEN: u32 = 0x0000_8000;
#[cfg(test)]
const MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT: usize = 8;

/// macFUSE open-cache flags chosen for one file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MacFuseOpenCachePolicy {
    /// Whether this handle should bypass kernel page cache.
    pub(crate) direct_io: bool,
    /// Whether macFUSE should keep existing cached pages for this handle.
    pub(crate) keep_cache: bool,
    /// Whether this handle can mutate the inode and must participate in writer accounting.
    pub(crate) write_capable: bool,
}

/// Operation adapter shared by macFUSE callbacks and fast tests.
pub(crate) struct StudioVfsAdapter {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    reply_tasks: Arc<ReplyTaskTracker>,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    directory_handles: ParkingMutex<HashMap<u64, Arc<DirectoryHandle>>>,
    next_directory_handle: AtomicU64,
    shared: Arc<AdapterSharedState>,
    open_reply_serializer: FuseOpenReplySerializer,
    file_flags: ParkingMutex<HashMap<u64, u32>>,
    read_queue_byte_limit: u32,
    next_runtime_stats_fh: AtomicU64,
    runtime_stats_handles: Arc<ParkingMutex<HashMap<u64, Vec<u8>>>>,
}

pub(crate) struct DirectoryHandle {
    pub(crate) parent_ino: u64,
    pub(crate) entries: Vec<DirEntry>,
}

#[derive(Debug)]
struct AdapterSharedState {
    // Characterization-only mirror for existing macOS tests. Production cache
    // correctness lives exclusively in StudioVfs's generation-safe seam.
    #[cfg(test)]
    open_cache_snapshots: Arc<ParkingMutex<LruCache<u64, MacFuseOpenCacheSnapshot>>>,
    kernel_inode_liveness: Arc<KernelInodeLiveness>,
    read_admission: Semaphore,
    read_buffers: Arc<ReadBufferPool>,
    read_diagnostics: AsyncReadDiagnostics,
    kernel_invalidations: Arc<KernelInvalidationGate>,
}

#[derive(Debug)]
struct KernelInodeLiveness {
    observed: DashMap<u64, Arc<ParkingMutex<u64>>>,
}

impl Default for KernelInodeLiveness {
    fn default() -> Self {
        let observed = DashMap::new();
        observed.insert(ROOT_INO, Arc::new(ParkingMutex::new(u64::MAX)));
        Self { observed }
    }
}

impl KernelInodeLiveness {
    fn current(&self, ino: u64) -> Option<Arc<ParkingMutex<u64>>> {
        self.observed
            .get(&ino)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn is_current(&self, ino: u64, state: &Arc<ParkingMutex<u64>>) -> bool {
        self.observed
            .get(&ino)
            .is_some_and(|entry| Arc::ptr_eq(entry.value(), state))
    }

    fn current_or_insert(&self, ino: u64) -> Arc<ParkingMutex<u64>> {
        match self.observed.entry(ino) {
            DashMapEntry::Occupied(entry) => Arc::clone(entry.get()),
            DashMapEntry::Vacant(entry) => {
                let state = Arc::new(ParkingMutex::new(0));
                entry.insert(Arc::clone(&state));
                state
            }
        }
    }

    fn observe(&self, ino: u64) {
        loop {
            let state = self.current_or_insert(ino);
            let mut count = state.lock();
            if self.is_current(ino, &state) {
                *count = count.saturating_add(1);
                return;
            }
        }
    }

    fn ensure_observed(&self, ino: u64) {
        loop {
            let state = self.current_or_insert(ino);
            let mut count = state.lock();
            if self.is_current(ino, &state) {
                *count = (*count).max(1);
                return;
            }
        }
    }

    #[cfg(test)]
    fn contains(&self, ino: u64) -> bool {
        self.current(ino).is_some_and(|state| *state.lock() > 0)
    }

    fn close(&self, ino: u64) {
        if ino == ROOT_INO {
            return;
        }
        loop {
            let Some(state) = self.current(ino) else {
                return;
            };
            let mut count = state.lock();
            if !self.is_current(ino, &state) {
                continue;
            }
            *count = 0;
            self.observed
                .remove_if(&ino, |_, current| Arc::ptr_eq(current, &state));
            return;
        }
    }

    fn forget(&self, ino: u64, nlookup: u64) -> bool {
        if ino == ROOT_INO || nlookup == 0 {
            return false;
        }
        loop {
            let Some(state) = self.current(ino) else {
                return false;
            };
            let mut count = state.lock();
            if !self.is_current(ino, &state) {
                continue;
            }
            if *count <= nlookup {
                *count = 0;
                self.observed
                    .remove_if(&ino, |_, current| Arc::ptr_eq(current, &state));
                return true;
            } else {
                *count -= nlookup;
                return false;
            }
        }
    }

    fn notify_if_observed(&self, ino: u64, notify: impl FnOnce()) {
        let Some(state) = self.current(ino) else {
            return;
        };
        let should_notify = {
            let count = state.lock();
            self.is_current(ino, &state) && *count > 0
        };
        if should_notify {
            notify();
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct MacFuseOpenCacheSnapshot {
    content_epoch: u64,
    size: u64,
    kind: MacFuseOpenCacheSnapshotKind,
    graph: Vec<MacFuseOpenCacheGraphSpan>,
}

#[cfg(test)]
impl PartialEq for MacFuseOpenCacheSnapshot {
    fn eq(&self, other: &Self) -> bool {
        // `kind` is intentionally omitted: publication-only transitions must
        // retain the kernel page cache when file contents are unchanged.
        self.content_epoch == other.content_epoch
            && self.size == other.size
            && self.graph == other.graph
    }
}

#[cfg(test)]
impl Eq for MacFuseOpenCacheSnapshot {}

#[cfg(test)]
impl MacFuseOpenCacheSnapshot {
    fn from_attr(attr: &InodeAttr, graph: Vec<MacFuseOpenCacheGraphSpan>) -> Self {
        debug_assert_eq!(attr.kind, FileType::RegularFile);
        Self {
            content_epoch: attr.content_epoch,
            size: attr.size,
            kind: MacFuseOpenCacheSnapshotKind::from_attr(attr),
            graph,
        }
    }
}

/// Publication state of the generation represented by an open-cache snapshot.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacFuseOpenCacheSnapshotKind {
    /// The generation is fully committed and visible through remote metadata.
    RemoteReady,
    /// The generation is locally stable for this mount but not remotely published yet.
    LocalStableUnpublished,
}

#[cfg(test)]
impl MacFuseOpenCacheSnapshotKind {
    fn from_attr(attr: &InodeAttr) -> Self {
        if attr.upload_state == UploadState::Ready
            && attr.content_epoch == attr.committed_content_epoch
            && attr.size == attr.committed_size
        {
            Self::RemoteReady
        } else {
            Self::LocalStableUnpublished
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MacFuseOpenCacheGraphSpan {
    chunk_id: u64,
    range_start: u32,
    range_end: u32,
    source: u8,
    slice_id: Option<u64>,
    slice_order: Option<u64>,
    payload: Option<MacFuseOpenCachePayload>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MacFuseOpenCachePayload {
    object_id: u64,
    offset: u32,
    length: u32,
    flags: u8,
}

#[derive(Debug, Default)]
struct ReadBufferPool {
    buffers: ParkingMutex<Vec<Vec<u8>>>,
}

impl ReadBufferPool {
    #[cfg(test)]
    fn acquire(self: Arc<Self>, size: usize) -> PooledReadBuffer {
        self.acquire_with_capacity(size, size)
    }

    fn acquire_with_capacity(self: Arc<Self>, len: usize, capacity: usize) -> PooledReadBuffer {
        let mut buffer = self.buffers.lock().pop().unwrap_or_default();
        let capacity = capacity.max(len);
        if buffer.capacity() < capacity {
            buffer.reserve_exact(capacity - buffer.len());
        }
        // TODO(SFS-489): avoid this per-read zero-fill once the VFS read path accepts
        // MaybeUninit/ReadBuf-style storage instead of initialized `&mut [u8]`.
        buffer.resize(len, 0);
        PooledReadBuffer { pool: self, buffer }
    }

    fn recycle(&self, buffer: Vec<u8>) {
        // `mem::take` during `Drop` leaves a zero-capacity placeholder; recycling
        // it would only occupy a pool slot without avoiding a future allocation.
        if buffer.capacity() == 0 || buffer.capacity() > MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY
        {
            return;
        }
        let mut buffers = self.buffers.lock();
        if buffers.len() < MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS {
            buffers.push(buffer);
        }
    }

    #[cfg(test)]
    fn available_count(&self) -> usize {
        self.buffers.lock().len()
    }

    #[cfg(test)]
    fn last_available_capacity(&self) -> Option<usize> {
        self.buffers.lock().last().map(Vec::capacity)
    }

    #[cfg(test)]
    fn last_available_len(&self) -> Option<usize> {
        self.buffers.lock().last().map(Vec::len)
    }
}

struct OpenCacheKernelInvalidator {
    #[cfg(test)]
    open_cache_snapshots: Arc<ParkingMutex<LruCache<u64, MacFuseOpenCacheSnapshot>>>,
    kernel_invalidations: Arc<KernelInvalidationGate>,
    inner: Arc<dyn KernelInvalidator>,
}

struct LiveKernelInvalidator {
    kernel_inode_liveness: Arc<KernelInodeLiveness>,
    inner: Arc<dyn KernelInvalidator>,
}

impl KernelInvalidator for LiveKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.kernel_inode_liveness
            .notify_if_observed(ino, || self.inner.inval_inode(ino));
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.kernel_inode_liveness
            .notify_if_observed(ino, || self.inner.inval_inode_attrs(ino));
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) {
        // A cached or negative dentry can outlive our lookup-reference count.
        // macFUSE safely returns ENOENT when the kernel no longer knows it.
        self.inner.inval_entry(parent, name);
    }

    fn reset_diagnostics(&self) {
        self.inner.reset_diagnostics();
    }
}

impl KernelInvalidator for OpenCacheKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        #[cfg(test)]
        self.open_cache_snapshots.lock().pop(&ino);
        if self.kernel_invalidations.defer_data_if_reads_active(ino) {
            self.kernel_invalidations.defer_attrs_if_reads_active(ino);
            debug!(
                ino,
                "macFUSE kernel data invalidation deferred until async reads drain"
            );
            return;
        }
        self.inner.inval_inode(ino);
        self.inner.inval_inode_attrs(ino);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        if self.kernel_invalidations.defer_attrs_if_reads_active(ino) {
            debug!(
                ino,
                "macFUSE kernel attr invalidation deferred until async reads drain"
            );
            return;
        }
        self.inner.inval_inode_attrs(ino);
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) {
        self.inner.inval_entry(parent, name);
    }

    fn reset_diagnostics(&self) {
        self.inner.reset_diagnostics();
    }
}

#[derive(Debug, Default)]
struct KernelInvalidationState {
    active_reads: usize,
    pending_data_inodes: HashSet<u64>,
    pending_attr_inodes: HashSet<u64>,
}

struct KernelInvalidationGate {
    inner: ParkingMutex<Option<Arc<dyn KernelInvalidator>>>,
    state: ParkingMutex<KernelInvalidationState>,
}

impl fmt::Debug for KernelInvalidationGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("KernelInvalidationGate").finish()
    }
}

impl KernelInvalidationGate {
    fn new() -> Self {
        Self {
            inner: ParkingMutex::new(None),
            state: ParkingMutex::new(KernelInvalidationState::default()),
        }
    }

    fn set_inner(&self, inner: Arc<dyn KernelInvalidator>) {
        *self.inner.lock() = Some(inner);
    }

    fn begin_read(&self) {
        self.state.lock().active_reads += 1;
    }

    fn finish_read(&self) -> Option<DeferredKernelInvalidationFlush> {
        let inner = self.inner.lock().clone();
        let mut state = self.state.lock();
        state.active_reads = state.active_reads.saturating_sub(1);
        if state.active_reads > 0 {
            return None;
        }
        let inner = inner?;
        if state.pending_data_inodes.is_empty() && state.pending_attr_inodes.is_empty() {
            return None;
        }
        let data_inodes = state.pending_data_inodes.drain().collect::<Vec<_>>();
        let attr_inodes = state.pending_attr_inodes.drain().collect::<Vec<_>>();
        Some(DeferredKernelInvalidationFlush {
            inner,
            data_inodes,
            attr_inodes,
        })
    }

    fn defer_data_if_reads_active(&self, ino: u64) -> bool {
        let mut state = self.state.lock();
        if state.active_reads == 0 {
            return false;
        }
        state.pending_data_inodes.insert(ino);
        true
    }

    fn defer_attrs_if_reads_active(&self, ino: u64) -> bool {
        let mut state = self.state.lock();
        if state.active_reads == 0 {
            return false;
        }
        state.pending_attr_inodes.insert(ino);
        true
    }
}

impl Default for KernelInvalidationGate {
    fn default() -> Self {
        Self::new()
    }
}

struct DeferredKernelInvalidationFlush {
    inner: Arc<dyn KernelInvalidator>,
    data_inodes: Vec<u64>,
    attr_inodes: Vec<u64>,
}

impl DeferredKernelInvalidationFlush {
    fn flush(self) {
        for ino in self.data_inodes {
            self.inner.inval_inode(ino);
        }
        for ino in self.attr_inodes {
            self.inner.inval_inode_attrs(ino);
        }
    }
}

#[derive(Clone)]
struct ReadExecutionState {
    vfs: Arc<StudioVfs>,
    shared: Arc<AdapterSharedState>,
    runtime_stats_handles: Arc<ParkingMutex<HashMap<u64, Vec<u8>>>>,
}

#[derive(Debug, Clone, Copy)]
struct ReadExecutionRequest {
    ino: u64,
    file_handle: u64,
    offset: u64,
    size: u32,
}

#[derive(Debug)]
struct PooledReadBuffer {
    pool: Arc<ReadBufferPool>,
    buffer: Vec<u8>,
}

impl PooledReadBuffer {
    fn truncate(&mut self, len: usize) {
        self.buffer.truncate(len);
    }
}

impl Deref for PooledReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_slice()
    }
}

impl DerefMut for PooledReadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut_slice()
    }
}

impl AsRef<[u8]> for PooledReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Drop for PooledReadBuffer {
    fn drop(&mut self) {
        self.pool.recycle(mem::take(&mut self.buffer));
    }
}

#[cfg(test)]
pub(crate) fn max_reusable_read_buffer_capacity_for_test() -> usize {
    MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY
}

#[cfg(test)]
pub(crate) fn max_retained_read_buffer_capacity_for_test() -> usize {
    MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY
}

fn clamp_reusable_read_size(size: u32, read_queue_byte_limit: u32) -> u32 {
    size.min(read_queue_byte_limit)
        .min(MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32)
}

fn read_execution_size(size: u32, read_queue_byte_limit: u32) -> u32 {
    clamp_reusable_read_size(
        size,
        read_queue_byte_limit.min(MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32),
    )
}

#[cfg(test)]
fn open_cache_snapshot_capacity() -> NonZeroUsize {
    NonZeroUsize::new(MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT)
        .expect("MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT must be greater than 0")
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueuedReadForTest {
    runtime_handle: Handle,
    handle: JoinHandle<Result<PooledReadBuffer, i32>>,
}

#[derive(Debug, Clone, Default)]
struct ReadCancellationToken {
    token: CancellationToken,
}

impl ReadCancellationToken {
    #[cfg(test)]
    fn cancel(&self) {
        self.token.cancel();
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    fn fuse_token(&self) -> CancellationToken {
        self.token.clone()
    }
}

#[derive(Debug)]
struct AsyncReadDiagnostics {
    started_at: Instant,
    max_pending_bytes: usize,
    pending_reads: CachePaddedAtomicUsize,
    pending_bytes: CachePaddedAtomicUsize,
    peak_pending_reads: CachePaddedAtomicUsize,
    peak_pending_bytes: CachePaddedAtomicUsize,
    completed_reads: CachePaddedAtomicU64,
    failed_reads: CachePaddedAtomicU64,
    canceled_reads: CachePaddedAtomicU64,
    admission_waits: CachePaddedAtomicU64,
    admission_wait_total_ns: CachePaddedAtomicU64,
    last_snapshot_publish_ns: CachePaddedAtomicU64,
}

#[repr(align(128))]
#[derive(Debug, Default)]
struct CachePaddedAtomicUsize(AtomicUsize);

impl CachePaddedAtomicUsize {
    fn new(value: usize) -> Self {
        Self(AtomicUsize::new(value))
    }
}

impl Deref for CachePaddedAtomicUsize {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[repr(align(128))]
#[derive(Debug, Default)]
struct CachePaddedAtomicU64(AtomicU64);

impl CachePaddedAtomicU64 {
    fn new(value: u64) -> Self {
        Self(AtomicU64::new(value))
    }
}

impl Deref for CachePaddedAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsyncReadDiagnostics {
    fn new(max_pending_bytes: usize) -> Self {
        Self {
            started_at: Instant::now(),
            max_pending_bytes,
            pending_reads: CachePaddedAtomicUsize::new(0),
            pending_bytes: CachePaddedAtomicUsize::new(0),
            peak_pending_reads: CachePaddedAtomicUsize::new(0),
            peak_pending_bytes: CachePaddedAtomicUsize::new(0),
            completed_reads: CachePaddedAtomicU64::new(0),
            failed_reads: CachePaddedAtomicU64::new(0),
            canceled_reads: CachePaddedAtomicU64::new(0),
            admission_waits: CachePaddedAtomicU64::new(0),
            admission_wait_total_ns: CachePaddedAtomicU64::new(0),
            last_snapshot_publish_ns: CachePaddedAtomicU64::new(0),
        }
    }

    fn on_queued(&self, bytes: u32) {
        let bytes = bytes as usize;
        let pending_reads = self.pending_reads.fetch_add(1, Ordering::Relaxed) + 1;
        let pending_bytes = self.pending_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak_pending_reads
            .fetch_max(pending_reads, Ordering::Relaxed);
        self.peak_pending_bytes
            .fetch_max(pending_bytes, Ordering::Relaxed);
    }

    fn on_admitted(&self, waited: Duration) {
        if waited.is_zero() {
            return;
        }
        self.admission_waits.fetch_add(1, Ordering::Relaxed);
        self.admission_wait_total_ns.fetch_add(
            waited.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    fn on_finished(&self, bytes: u32, outcome: AsyncReadOutcome) -> usize {
        let pending_reads = self.pending_reads.fetch_sub(1, Ordering::Relaxed) - 1;
        self.pending_bytes
            .fetch_sub(bytes as usize, Ordering::Relaxed);
        match outcome {
            AsyncReadOutcome::Completed => {
                self.completed_reads.fetch_add(1, Ordering::Relaxed);
            }
            AsyncReadOutcome::Failed => {
                self.failed_reads.fetch_add(1, Ordering::Relaxed);
            }
            AsyncReadOutcome::Canceled => {
                self.canceled_reads.fetch_add(1, Ordering::Relaxed);
            }
        }
        pending_reads
    }

    fn snapshot(&self) -> VfsFuseAsyncReadSnapshot {
        VfsFuseAsyncReadSnapshot {
            max_pending_bytes: Some(self.max_pending_bytes),
            pending_reads: self.pending_reads.load(Ordering::Relaxed),
            pending_bytes: self.pending_bytes.load(Ordering::Relaxed),
            peak_pending_reads: self.peak_pending_reads.load(Ordering::Relaxed),
            peak_pending_bytes: self.peak_pending_bytes.load(Ordering::Relaxed),
            completed_reads: self.completed_reads.load(Ordering::Relaxed),
            failed_reads: self.failed_reads.load(Ordering::Relaxed),
            canceled_reads: self.canceled_reads.load(Ordering::Relaxed),
            admission_waits: self.admission_waits.load(Ordering::Relaxed),
            admission_wait_total_ms: self.admission_wait_total_ns.load(Ordering::Relaxed) as f64
                / 1_000_000.0,
        }
    }

    fn publish_snapshot_after_finish(&self, vfs: &StudioVfs, pending_reads: usize) {
        if pending_reads == 0 || self.should_publish_snapshot() {
            vfs.record_fuse_async_read_snapshot(self.snapshot());
        }
    }

    fn should_publish_snapshot(&self) -> bool {
        let now = self.started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let interval = MACFUSE_ASYNC_READ_SNAPSHOT_INTERVAL
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let mut last = self.last_snapshot_publish_ns.load(Ordering::Relaxed);
        loop {
            if now.saturating_sub(last) < interval {
                return false;
            }
            match self.last_snapshot_publish_ns.compare_exchange_weak(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => last = observed,
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsyncReadOutcome {
    Completed,
    Failed,
    Canceled,
}

struct AsyncReadTaskGuard<'a> {
    size: u32,
    shared: &'a AdapterSharedState,
    vfs: &'a StudioVfs,
    outcome: AsyncReadOutcome,
}

impl<'a> AsyncReadTaskGuard<'a> {
    fn new(size: u32, shared: &'a AdapterSharedState, vfs: &'a StudioVfs) -> Self {
        shared.read_diagnostics.on_queued(size);
        shared.kernel_invalidations.begin_read();
        Self {
            size,
            shared,
            vfs,
            outcome: AsyncReadOutcome::Failed,
        }
    }

    fn finish_with(&mut self, outcome: AsyncReadOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for AsyncReadTaskGuard<'_> {
    fn drop(&mut self) {
        let pending_reads = self
            .shared
            .read_diagnostics
            .on_finished(self.size, self.outcome);
        self.shared
            .read_diagnostics
            .publish_snapshot_after_finish(self.vfs, pending_reads);
        if let Some(flush) = self.shared.kernel_invalidations.finish_read() {
            flush.flush();
        }
    }
}

#[cfg(test)]
impl QueuedReadForTest {
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub(crate) fn wait(self) -> Result<Vec<u8>, i32> {
        let buffer = self
            .runtime_handle
            .block_on(self.handle)
            .expect("queued macFUSE read worker should not panic")?;
        let bytes = buffer.to_vec();
        drop(buffer);
        Ok(bytes)
    }

    pub(crate) fn wait_timeout(self, timeout: Duration) -> Option<Result<Vec<u8>, i32>> {
        let joined = self
            .runtime_handle
            .block_on(async move { tokio::time::timeout(timeout, self.handle).await });
        let buffer = match joined {
            Ok(joined) => joined.expect("queued macFUSE read worker should not panic"),
            Err(_) => return None,
        };
        Some(buffer.map(|buffer| {
            let bytes = buffer.to_vec();
            drop(buffer);
            bytes
        }))
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct CancelableQueuedReadForTest {
    queued: QueuedReadForTest,
    cancellation: ReadCancellationToken,
}

#[cfg(test)]
impl CancelableQueuedReadForTest {
    pub(crate) fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub(crate) fn wait_timeout(self, timeout: Duration) -> Option<Result<Vec<u8>, i32>> {
        self.queued.wait_timeout(timeout)
    }
}

fn normalize_close_flush_result(result: Result<(), i32>) -> Result<(), i32> {
    match result {
        // macFUSE invokes `flush` from close(2), after an explicit fsync callback
        // may already have reported a concurrent namespace deletion. Returning
        // ENOENT here prevents the kernel from advancing to `release`, even
        // though the open handle and its locally accepted payload remain valid.
        Err(libc::ENOENT) => Ok(()),
        other => other,
    }
}

impl StudioVfsAdapter {
    /// Create a macFUSE adapter over a StudioVfs instance.
    #[cfg(test)]
    pub(crate) fn new(vfs: Arc<StudioVfs>, runtime_handle: Handle) -> Self {
        Self::new_with_read_only(vfs, runtime_handle, false)
    }

    /// Create a macFUSE adapter over a StudioVfs instance with explicit write policy.
    pub(crate) fn new_with_read_only(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
    ) -> Self {
        Self::new_with_read_queue_byte_limit(
            vfs,
            runtime_handle,
            read_only,
            MACFUSE_READ_MAX_PENDING_BYTES,
        )
    }

    fn new_with_read_queue_byte_limit(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        read_queue_byte_limit: usize,
    ) -> Self {
        let read_queue_byte_limit = read_queue_byte_limit
            .max(1)
            .min(u32::MAX as usize)
            .min(Semaphore::MAX_PERMITS) as u32;
        Self {
            vfs,
            runtime_handle,
            reply_tasks: Arc::new(ReplyTaskTracker::default()),
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            directory_handles: ParkingMutex::new(HashMap::new()),
            next_directory_handle: AtomicU64::new(1),
            shared: Arc::new(AdapterSharedState {
                #[cfg(test)]
                open_cache_snapshots: Arc::new(ParkingMutex::new(LruCache::new(
                    open_cache_snapshot_capacity(),
                ))),
                kernel_inode_liveness: Arc::new(KernelInodeLiveness::default()),
                read_admission: Semaphore::new(read_queue_byte_limit as usize),
                read_buffers: Arc::new(ReadBufferPool::default()),
                read_diagnostics: AsyncReadDiagnostics::new(read_queue_byte_limit as usize),
                kernel_invalidations: Arc::new(KernelInvalidationGate::new()),
            }),
            open_reply_serializer: FuseOpenReplySerializer::new(),
            file_flags: ParkingMutex::new(HashMap::new()),
            read_queue_byte_limit,
            next_runtime_stats_fh: AtomicU64::new(u64::MAX / 2),
            runtime_stats_handles: Arc::new(ParkingMutex::new(HashMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_read_queue_byte_limit_for_test(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_queue_byte_limit: usize,
    ) -> Self {
        Self::new_with_read_queue_byte_limit(vfs, runtime_handle, false, read_queue_byte_limit)
    }

    #[cfg(test)]
    pub(crate) fn read_queue_byte_limit_for_test(&self) -> u32 {
        self.read_queue_byte_limit
    }

    #[cfg(test)]
    pub(crate) fn hot_path_shared_state_arc_count_for_test(&self) -> usize {
        Arc::strong_count(&self.shared)
    }

    #[cfg(test)]
    pub(crate) fn pending_async_read_count_for_test(&self) -> usize {
        self.shared
            .read_diagnostics
            .pending_reads
            .load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn hold_read_capacity_for_test(
        &self,
        bytes: u32,
    ) -> tokio::sync::SemaphorePermit<'_> {
        self.runtime_handle
            .block_on(self.shared.read_admission.acquire_many(bytes))
            .expect("test read capacity should be available")
    }

    #[cfg(test)]
    pub(crate) fn reusable_read_buffer_count_for_test(&self) -> usize {
        self.shared.read_buffers.available_count()
    }

    #[cfg(test)]
    pub(crate) fn reusable_read_buffer_capacity_for_test(&self) -> Option<usize> {
        self.shared.read_buffers.last_available_capacity()
    }

    #[cfg(test)]
    pub(crate) fn kernel_invalidator_for_test(
        &self,
        inner: Arc<dyn KernelInvalidator>,
    ) -> Arc<dyn KernelInvalidator> {
        self.kernel_invalidator(inner)
    }

    #[cfg(test)]
    pub(crate) fn has_open_cache_snapshot_for_test(&self, ino: u64) -> bool {
        self.shared.open_cache_snapshots.lock().contains(&ino)
    }

    #[cfg(test)]
    pub(crate) fn open_cache_snapshot_count_for_test(&self) -> usize {
        self.shared.open_cache_snapshots.lock().len()
    }

    pub(crate) fn kernel_invalidator(
        &self,
        inner: Arc<dyn KernelInvalidator>,
    ) -> Arc<dyn KernelInvalidator> {
        let inner: Arc<dyn KernelInvalidator> = Arc::new(LiveKernelInvalidator {
            kernel_inode_liveness: Arc::clone(&self.shared.kernel_inode_liveness),
            inner,
        });
        self.shared
            .kernel_invalidations
            .set_inner(Arc::clone(&inner));
        Arc::new(OpenCacheKernelInvalidator {
            #[cfg(test)]
            open_cache_snapshots: Arc::clone(&self.shared.open_cache_snapshots),
            kernel_invalidations: Arc::clone(&self.shared.kernel_invalidations),
            inner,
        })
    }

    #[cfg(test)]
    pub(crate) fn open_cache_snapshot_limit_for_test(&self) -> usize {
        MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT
    }

    /// Look up a child inode.
    pub(crate) fn lookup(&self, parent: u64, name: &[u8]) -> Result<InodeAttr, i32> {
        self.vfs
            .lookup(parent, name)
            .inspect(|attr| {
                self.shared.kernel_inode_liveness.observe(attr.ino);
                self.remember_named_file_flags(attr.ino, name);
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    pub(crate) fn forget(&self, ino: u64, nlookup: u64) {
        if self.shared.kernel_inode_liveness.forget(ino, nlookup) {
            self.clear_file_flags(ino);
        }
    }

    /// Get inode attributes.
    pub(crate) fn getattr(&self, ino: u64) -> Result<stat, i32> {
        if self.vfs.is_runtime_stats_file_ino(ino) {
            return Ok(self.attr_to_stat(&self.vfs.runtime_stats_file_attr()));
        }
        self.vfs
            .getattr(ino)
            .map(|attr| self.attr_to_stat(&attr))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    pub(crate) fn access(&self, ino: u64, mask: i32) -> Result<(), i32> {
        let attr = self.getattr(ino)?;
        if mask == libc::F_OK {
            return Ok(());
        }
        if self.read_only && mask & libc::W_OK != 0 {
            return Err(libc::EROFS);
        }
        let mode = attr.st_mode as u32;
        let readable = mode & 0o444 != 0;
        let writable = mode & 0o222 != 0;
        let executable = mode & 0o111 != 0 || mode & libc::S_IFMT as u32 == libc::S_IFDIR as u32;
        if (mask & libc::R_OK == 0 || readable)
            && (mask & libc::W_OK == 0 || writable)
            && (mask & libc::X_OK == 0 || executable)
        {
            Ok(())
        } else {
            Err(libc::EACCES)
        }
    }

    /// Create a directory.
    pub(crate) fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let mode = mode & 0o7777;
        self.vfs
            .mkdir_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
            .inspect(|attr| {
                self.shared.kernel_inode_liveness.observe(attr.ino);
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create a symbolic link.
    pub(crate) fn symlink(
        &self,
        parent: u64,
        name: &[u8],
        target: &[u8],
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let mut retry_attempts = 0;
        let retry_deadline = Instant::now() + MACFUSE_SYMLINK_RETRY_TIMEOUT;
        let attr = loop {
            // Capture this before the metadata lookup so a commit racing the
            // symlink attempt still permits the final retry.
            let deferred_parent_pending = self.vfs.has_deferred_namespace_metadata(parent);
            match self
                .vfs
                .symlink(parent, name, target, self.mount_uid, self.mount_gid)
            {
                Ok(attr) => break Ok(attr),
                Err(error) => {
                    let errno = vfs_err_to_errno(&error);
                    if errno == libc::ENOENT
                        && deferred_parent_pending
                        && retry_attempts < MACFUSE_SYMLINK_RETRY_ATTEMPTS
                        && Instant::now() < retry_deadline
                    {
                        retry_attempts += 1;
                        debug!(
                            parent,
                            name = %String::from_utf8_lossy(name),
                            retry_attempt = retry_attempts,
                            "macFUSE symlink waiting for deferred parent metadata"
                        );
                        std::thread::sleep(MACFUSE_SYMLINK_RETRY_DELAY);
                        continue;
                    }
                    break Err(errno);
                }
            }
        }?;
        self.shared.kernel_inode_liveness.observe(attr.ino);
        Ok(attr)
    }

    /// Create and open a regular file.
    #[cfg(test)]
    pub(crate) fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: i32,
    ) -> Result<(InodeAttr, OpenReply), i32> {
        let attr = self.create_attr(parent, name, mode)?;
        let open = self.open_created_attr(&attr, flags)?;
        Ok((attr, open))
    }

    fn create_for_reply(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: i32,
    ) -> Result<(InodeAttr, OpenReply, FuseOpenReplyGuard), i32> {
        let attr = self.create_attr(parent, name, mode)?;
        let guard = self.open_reply_serializer.lock(attr.ino);
        let open = self.open_created_attr(&attr, flags)?;
        Ok((attr, open, guard))
    }

    #[cfg(studiofs_fskit)]
    pub(crate) fn create_for_fskit(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: i32,
    ) -> Result<(InodeAttr, OpenReply), i32> {
        let attr = self.create_attr(parent, name, mode)?;
        let open = self.open_created_attr(&attr, flags)?;
        Ok((attr, open))
    }

    fn create_attr(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode & 0o7777,
                &self.runtime_handle,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.remember_named_file_flags(attr.ino, name);
        Ok(attr)
    }

    fn open_created_attr(&self, attr: &InodeAttr, flags: i32) -> Result<OpenReply, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        let flag_policy = macfuse_open_cache_policy(flags, trunc);
        let open = self.open_with_result(
            attr.ino,
            flag_policy,
            self.vfs
                .open_created_file_with_known_attr(attr, open_flags, trunc),
        )?;
        self.shared.kernel_inode_liveness.observe(attr.ino);
        Ok(open)
    }

    /// Create a regular file through the macFUSE mknod callback.
    pub(crate) fn mknod(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = mode & 0o7777;
        self.vfs
            .create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
            .inspect(|attr| {
                self.shared.kernel_inode_liveness.observe(attr.ino);
                self.remember_named_file_flags(attr.ino, name);
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Open an existing file.
    pub(crate) fn open(&self, ino: u64, flags: i32) -> Result<OpenReply, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && posix_open_requests_write(flags, trunc) {
            return Err(libc::EROFS);
        }
        if self.vfs.is_runtime_stats_file_ino(ino) {
            if posix_open_requests_write(flags, trunc)
                || open_flags.contains(studiofs_vfs::OpenFlags::WRONLY)
                || open_flags.contains(studiofs_vfs::OpenFlags::RDWR)
            {
                return Err(libc::EROFS);
            }
            let fh = self.next_runtime_stats_fh.fetch_add(1, Ordering::Relaxed);
            let bytes = self.vfs.runtime_stats_file_text().into_bytes();
            self.runtime_stats_handles.lock().insert(fh, bytes);
            return Ok(OpenReply::new(fh).with_direct_io(true));
        }
        let flag_policy = macfuse_open_cache_policy(flags, trunc);
        self.open_with_result(ino, flag_policy, self.vfs.open_file(ino, open_flags, trunc))
            .inspect(|_| self.shared.kernel_inode_liveness.ensure_observed(ino))
    }

    fn is_runtime_stats_file_ino(&self, ino: u64) -> bool {
        self.vfs.is_runtime_stats_file_ino(ino)
    }

    fn lock_open_reply(&self, ino: u64) -> FuseOpenReplyGuard {
        self.open_reply_serializer.lock(ino)
    }

    pub(crate) fn reject_prepared_open(&self, ino: u64, fh: u64) {
        debug!(ino, fh, "macFUSE rolls back rejected open reply");
        self.vfs.abort_os_cache_open(fh);
        #[cfg(test)]
        self.shared.open_cache_snapshots.lock().pop(&ino);
        let _ = self
            .vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
    }

    fn open_with_result(
        &self,
        ino: u64,
        flag_policy: MacFuseOpenCachePolicy,
        open_result: VfsResult<u64>,
    ) -> Result<OpenReply, i32> {
        let fh = open_result.map_err(|error| vfs_err_to_errno(&error))?;
        let policy = if flag_policy.write_capable {
            let decision = self
                .vfs
                .prepare_os_cache_open(fh, OsCacheParticipation::Enabled);
            debug_assert_eq!(decision.reason(), OsCacheReason::WriteCapable);
            self.vfs
                .record_macfuse_open_cache_decision(VfsMacFuseOpenCacheDecision::WriteCapable);
            #[cfg(test)]
            self.shared.open_cache_snapshots.lock().pop(&ino);
            flag_policy
        } else if flag_policy.keep_cache {
            self.read_only_open_cache_policy(ino, fh, available_memory_bytes())
        } else {
            let _ = self
                .vfs
                .prepare_os_cache_open(fh, OsCacheParticipation::Disabled);
            flag_policy
        };
        Ok(OpenReply::new(fh)
            .with_direct_io(policy.direct_io)
            .with_keep_cache(policy.keep_cache))
    }

    fn read_only_open_cache_policy(
        &self,
        ino: u64,
        fh: u64,
        available_memory: Option<u64>,
    ) -> MacFuseOpenCachePolicy {
        let attr = self.vfs.getattr(ino).ok();
        let participation = if attr.is_some() {
            OsCacheParticipation::Enabled
        } else {
            OsCacheParticipation::Disabled
        };
        let decision = if let Some(attr) = attr.as_ref() {
            self.vfs
                .prepare_os_cache_open_with_attr(fh, participation, attr)
        } else {
            self.vfs.prepare_os_cache_open(fh, participation)
        };
        let Some(attr) = attr else {
            self.vfs.record_macfuse_open_cache_decision(
                VfsMacFuseOpenCacheDecision::ReadOnlyUnknownState,
            );
            #[cfg(test)]
            self.shared.open_cache_snapshots.lock().pop(&ino);
            return MacFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: false,
                write_capable: false,
            };
        };
        let keep_cache = decision.disposition() == OsCacheDisposition::Reuse;
        let policy = MacFuseOpenCachePolicy {
            direct_io: available_memory.is_some_and(|available| attr.size > available),
            keep_cache,
            write_capable: false,
        };
        let metric = macfuse_cache_metric(&attr, decision);
        self.vfs.record_macfuse_open_cache_decision(metric);
        #[cfg(test)]
        self.track_open_cache_observation_for_test(ino, &attr, decision);
        debug!(
            ino,
            keep_cache,
            content_epoch = attr.content_epoch,
            size = attr.size,
            reason = ?decision.reason(),
            "macFUSE open cache decision"
        );
        policy
    }

    #[cfg(test)]
    fn track_open_cache_observation_for_test(
        &self,
        ino: u64,
        attr: &InodeAttr,
        decision: OsCacheOpenDecision,
    ) {
        match decision.reason() {
            OsCacheReason::FirstObservation
            | OsCacheReason::GenerationChanged
            | OsCacheReason::StableGeneration => {
                self.shared
                    .open_cache_snapshots
                    .lock()
                    .put(ino, MacFuseOpenCacheSnapshot::from_attr(attr, Vec::new()));
            }
            _ => {
                self.shared.open_cache_snapshots.lock().pop(&ino);
            }
        }
    }

    /// Remove a file entry.
    pub(crate) fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .unlink_with_attr(parent, name)
            .inspect(|unlinked_attr| {
                if let Some(attr) = unlinked_attr
                    && attr.nlink == 0
                {
                    #[cfg(test)]
                    self.shared.open_cache_snapshots.lock().pop(&attr.ino);
                    self.clear_file_flags(attr.ino);
                    self.shared.kernel_inode_liveness.close(attr.ino);
                }
            })
            .map(|_| ())
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Remove an empty directory entry.
    pub(crate) fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let existing = self.vfs.lookup(parent, name).ok();
        // Return ENOTEMPTY promptly when Finder has not issued child deletes
        // yet. Blocking this callback prevents Finder from enumerating and
        // deleting the directory contents. VFS rmdir waits for child deletes
        // that are already in flight before reporting the same error.
        let result = self.vfs.rmdir(parent, name);
        result
            .inspect(|_| {
                if let Some(attr) = &existing {
                    self.clear_file_flags(attr.ino);
                    self.shared.kernel_inode_liveness.close(attr.ino);
                }
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Run the potentially waiting rmdir path away from the single macFUSE
    /// session-loop thread. macFUSE delivers callbacks serially through that
    /// loop; sleeping there would prevent the queued child unlink callbacks
    /// from running and make the directory stay non-empty indefinitely.
    pub(crate) fn queue_rmdir_reply(
        self: &Arc<Self>,
        parent: u64,
        name: Vec<u8>,
        request: Request,
    ) {
        let adapter = Arc::clone(self);
        let task = self.reply_tasks.track();
        self.runtime_handle.spawn_blocking(move || {
            let _task = task;
            let result = adapter.rmdir(parent, &name);
            match result {
                Ok(()) => {
                    let _ = Reply::new(request).error(0);
                }
                Err(errno) => {
                    if callback_error_log_level("rmdir", errno).is_some() {
                        warn!(
                            parent,
                            name = %String::from_utf8_lossy(&name),
                            errno,
                            "macFUSE rmdir callback returned error"
                        );
                    }
                    let _ = Reply::new(request).error(errno);
                }
            }
        });
    }

    /// Run symlink creation away from the single macFUSE session-loop thread.
    /// A parent directory may still be settling in deferred metadata while the
    /// kernel delivers the child symlink callback.
    pub(crate) fn queue_symlink_reply(
        self: &Arc<Self>,
        parent: u64,
        name: Vec<u8>,
        target: Vec<u8>,
        request: Request,
    ) {
        let adapter = Arc::clone(self);
        let task = self.reply_tasks.track();
        self.runtime_handle.spawn_blocking(move || {
            let _task = task;
            match adapter.symlink(parent, &name, &target) {
                Ok(attr) => {
                    debug!(
                        parent,
                        name = %String::from_utf8_lossy(&name),
                        target = %String::from_utf8_lossy(&target),
                        ino = attr.ino,
                        "macFUSE symlink callback created entry"
                    );
                    let stat = adapter.attr_to_stat(&attr);
                    let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                    let _ = Reply::new(request).entry(&entry);
                }
                Err(errno) => {
                    if callback_error_log_level("symlink", errno).is_some() {
                        warn!(
                            parent,
                            name = %String::from_utf8_lossy(&name),
                            target = %String::from_utf8_lossy(&target),
                            errno,
                            "macFUSE symlink callback returned error"
                        );
                    }
                    let _ = Reply::new(request).error(errno);
                }
            }
        });
    }

    /// Rename a namespace entry.
    pub(crate) fn rename(
        &self,
        parent: u64,
        name: &[u8],
        newparent: u64,
        newname: &[u8],
    ) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let overwritten = (parent != newparent || name != newname)
            .then(|| self.vfs.lookup(newparent, newname).ok())
            .flatten();
        self.vfs
            .rename(parent, name, newparent, newname)
            .inspect(|_| {
                if let Some(attr) = &overwritten {
                    self.clear_file_flags(attr.ino);
                    // A regular file with more than one link remains reachable
                    // after the destination dentry is replaced. Directory link
                    // counts include `.`/`..`, and directory hard links are not
                    // allowed, so an overwritten directory is final here too.
                    if attr.kind == FileType::Directory || attr.nlink <= 1 {
                        self.shared.kernel_inode_liveness.close(attr.ino);
                    }
                }
                if let Ok(attr) = self.vfs.lookup(newparent, newname) {
                    self.remember_named_file_flags(attr.ino, newname);
                }
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Read file data through a VFS file handle and record FUSE read diagnostics.
    #[cfg(test)]
    pub(crate) fn read(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size_usize = size as usize;

        if self.vfs.is_runtime_stats_file_ino(ino) {
            let mut out = vec![0u8; size_usize];
            let handles = self.runtime_stats_handles.lock();
            let Some(bytes) = handles.get(&file_handle) else {
                return Err(libc::EBADF);
            };
            let n = studiofs_vfs::read_runtime_stats_buffer(bytes, offset, &mut out) as usize;
            out.truncate(n);
            return Ok(out);
        }

        // Test-only synchronous reads return an owned Vec, so keep them off the
        // queued-read pool; otherwise this helper would consume pooled buffers.
        let mut out = vec![0u8; size_usize];
        let started = Instant::now();
        let result =
            self.runtime_handle
                .block_on(self.vfs.read_into_fh(file_handle, ino, offset, &mut out));
        match result {
            Ok(written) => {
                self.vfs
                    .record_fuse_read_callback(written as usize, started.elapsed(), false);
                out.truncate(written as usize);
                Ok(out)
            }
            Err(error) => {
                self.vfs
                    .record_fuse_read_callback(size_usize, started.elapsed(), true);
                Err(vfs_err_to_errno(&error))
            }
        }
    }

    async fn perform_read(
        state: &ReadExecutionState,
        request: ReadExecutionRequest,
        started: Instant,
        cancellation: ReadCancellationToken,
    ) -> Result<PooledReadBuffer, i32> {
        if cancellation.is_cancelled() {
            return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
        }
        if state.vfs.is_runtime_stats_file_ino(request.ino) {
            let size = request.size as usize;
            let mut out = state
                .shared
                .read_buffers
                .clone()
                .acquire_with_capacity(size, size);
            let handles = state.runtime_stats_handles.lock();
            let Some(bytes) = handles.get(&request.file_handle) else {
                return Err(libc::EBADF);
            };
            let n = studiofs_vfs::read_runtime_stats_buffer(bytes, request.offset, &mut out);
            out.truncate(n as usize);
            return Ok(out);
        }
        let size =
            clamp_reusable_read_size(request.size, MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32);
        let size_usize = size as usize;

        let mut out = state
            .shared
            .read_buffers
            .clone()
            .acquire_with_capacity(size_usize, size_usize);
        if cancellation.is_cancelled() {
            return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
        }
        let result = tokio::select! {
            result = state.vfs.read_into_fh(request.file_handle, request.ino, request.offset, &mut out) => result,
            () = cancellation.cancelled() => return Err(MACFUSE_READ_INTERRUPTED_ERRNO),
        };
        match result {
            Ok(written) => {
                let written = written as usize;
                let reply_len = written.min(size_usize);
                state
                    .vfs
                    .record_fuse_read_callback(reply_len, started.elapsed(), false);
                out.truncate(reply_len);
                out.buffer.shrink_to(MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY);
                Ok(out)
            }
            Err(error) => {
                state
                    .vfs
                    .record_fuse_read_callback(size_usize, started.elapsed(), true);
                Err(vfs_err_to_errno(&error))
            }
        }
    }

    #[cfg(studiofs_fskit)]
    pub(crate) async fn read_for_fskit(
        &self,
        ino: u64,
        file_handle: u64,
        offset: u64,
        size: u32,
        cancellation: CancellationToken,
    ) -> Result<Vec<u8>, i32> {
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            shared: Arc::clone(&self.shared),
            runtime_stats_handles: Arc::clone(&self.runtime_stats_handles),
        };
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        let cancellation = ReadCancellationToken {
            token: cancellation,
        };
        let mut read_guard = AsyncReadTaskGuard::new(size, &state.shared, &state.vfs);
        if cancellation.is_cancelled() {
            read_guard.finish_with(AsyncReadOutcome::Canceled);
            return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
        }
        let _permit = match acquire_read_admission(
            &state.shared.read_admission,
            size,
            &read_guard.shared.read_diagnostics,
            &cancellation,
        )
        .await
        {
            Ok(permit) => permit,
            Err(errno) => {
                let outcome = if errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                    AsyncReadOutcome::Canceled
                } else {
                    AsyncReadOutcome::Failed
                };
                read_guard.finish_with(outcome);
                return Err(errno);
            }
        };
        match Self::perform_read(
            &state,
            ReadExecutionRequest {
                ino,
                file_handle,
                offset,
                size,
            },
            started,
            cancellation,
        )
        .await
        {
            Ok(bytes) => {
                read_guard.finish_with(AsyncReadOutcome::Completed);
                Ok(bytes.to_vec())
            }
            Err(errno) => {
                let outcome = if errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                    AsyncReadOutcome::Canceled
                } else {
                    AsyncReadOutcome::Failed
                };
                read_guard.finish_with(outcome);
                Err(errno)
            }
        }
    }

    pub(crate) fn queue_read_reply(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
        request: Request,
    ) {
        let Ok(offset) = u64::try_from(offset) else {
            let _ = Reply::new(request).error(libc::EINVAL);
            return;
        };
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            shared: Arc::clone(&self.shared),
            runtime_stats_handles: Arc::clone(&self.runtime_stats_handles),
        };
        // CONTEXT: Guard and semaphore permit borrows below depend on this moved
        // state; keep `state` declared before its borrowers so drop order remains valid.
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        let mut request = request;
        request.register_interrupt_token(cancellation.fuse_token());
        let task = self.reply_tasks.track();
        self.runtime_handle.spawn(async move {
            let _task = task;
            let mut read_guard = AsyncReadTaskGuard::new(size, &state.shared, &state.vfs);
            if cancellation.is_cancelled() {
                read_guard.finish_with(AsyncReadOutcome::Canceled);
                log_callback_error("read", MACFUSE_READ_INTERRUPTED_ERRNO);
                let _ = Reply::new(request).error(MACFUSE_READ_INTERRUPTED_ERRNO);
                return;
            }
            let _permit = match acquire_read_admission(
                &state.shared.read_admission,
                size,
                &read_guard.shared.read_diagnostics,
                &cancellation,
            )
            .await
            {
                Ok(permit) => permit,
                Err(errno) => {
                    let outcome = if errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                        AsyncReadOutcome::Canceled
                    } else {
                        AsyncReadOutcome::Failed
                    };
                    read_guard.finish_with(outcome);
                    if callback_error_log_level("read", errno).is_some() {
                        warn!(
                            ino,
                            file_handle,
                            offset,
                            size,
                            errno,
                            "macFUSE read admission returned error"
                        );
                    }
                    let _ = Reply::new(request).error(errno);
                    return;
                }
            };
            // Keep the permit until after the reply consumes `bytes`, so queued read
            // memory remains bounded for the whole kernel-reply handoff.
            match Self::perform_read(
                &state,
                ReadExecutionRequest {
                    ino,
                    file_handle,
                    offset,
                    size,
                },
                started,
                cancellation,
            )
            .await
            {
                Ok(bytes) => {
                    read_guard.finish_with(AsyncReadOutcome::Completed);
                    let _ = Reply::new(request).read_buffer(&bytes);
                }
                Err(errno) => {
                    let outcome = if errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                        AsyncReadOutcome::Canceled
                    } else {
                        AsyncReadOutcome::Failed
                    };
                    read_guard.finish_with(outcome);
                    if callback_error_log_level("read", errno).is_some() {
                        warn!(
                            ino,
                            file_handle,
                            offset,
                            size,
                            errno,
                            "macFUSE read callback returned error"
                        );
                    }
                    let _ = Reply::new(request).error(errno);
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn queue_read_for_test(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<QueuedReadForTest, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            shared: Arc::clone(&self.shared),
            runtime_stats_handles: Arc::clone(&self.runtime_stats_handles),
        };
        // CONTEXT: Guard and semaphore permit borrows below depend on this moved
        // state; keep `state` declared before its borrowers so drop order remains valid.
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        Ok(QueuedReadForTest {
            runtime_handle: self.runtime_handle.clone(),
            handle: self.runtime_handle.spawn(async move {
                let mut read_guard = AsyncReadTaskGuard::new(size, &state.shared, &state.vfs);
                if cancellation.is_cancelled() {
                    read_guard.finish_with(AsyncReadOutcome::Canceled);
                    return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
                }
                let _permit = acquire_read_admission(
                    &state.shared.read_admission,
                    size,
                    &read_guard.shared.read_diagnostics,
                    &cancellation,
                )
                .await
                .inspect_err(|errno| {
                    let outcome = if *errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                        AsyncReadOutcome::Canceled
                    } else {
                        AsyncReadOutcome::Failed
                    };
                    read_guard.finish_with(outcome);
                })?;
                let result = Self::perform_read(
                    &state,
                    ReadExecutionRequest {
                        ino,
                        file_handle,
                        offset,
                        size,
                    },
                    started,
                    cancellation,
                )
                .await;
                match &result {
                    Ok(_) => read_guard.finish_with(AsyncReadOutcome::Completed),
                    Err(errno) if *errno == MACFUSE_READ_INTERRUPTED_ERRNO => {
                        read_guard.finish_with(AsyncReadOutcome::Canceled);
                    }
                    Err(_) => read_guard.finish_with(AsyncReadOutcome::Failed),
                }
                result
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn queue_cancelable_read_for_test(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<CancelableQueuedReadForTest, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            shared: Arc::clone(&self.shared),
            runtime_stats_handles: Arc::clone(&self.runtime_stats_handles),
        };
        // CONTEXT: Guard and semaphore permit borrows below depend on this moved
        // state; keep `state` declared before its borrowers so drop order remains valid.
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        let worker_cancellation = cancellation.clone();
        Ok(CancelableQueuedReadForTest {
            cancellation,
            queued: QueuedReadForTest {
                runtime_handle: self.runtime_handle.clone(),
                handle: self.runtime_handle.spawn(async move {
                    let mut read_guard = AsyncReadTaskGuard::new(size, &state.shared, &state.vfs);
                    if worker_cancellation.is_cancelled() {
                        read_guard.finish_with(AsyncReadOutcome::Canceled);
                        return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
                    }
                    let _permit = acquire_read_admission(
                        &state.shared.read_admission,
                        size,
                        &read_guard.shared.read_diagnostics,
                        &worker_cancellation,
                    )
                    .await
                    .inspect_err(|errno| {
                        let outcome = if *errno == MACFUSE_READ_INTERRUPTED_ERRNO {
                            AsyncReadOutcome::Canceled
                        } else {
                            AsyncReadOutcome::Failed
                        };
                        read_guard.finish_with(outcome);
                    })?;
                    let result = Self::perform_read(
                        &state,
                        ReadExecutionRequest {
                            ino,
                            file_handle,
                            offset,
                            size,
                        },
                        started,
                        worker_cancellation,
                    )
                    .await;
                    match &result {
                        Ok(_) => read_guard.finish_with(AsyncReadOutcome::Completed),
                        Err(errno) if *errno == MACFUSE_READ_INTERRUPTED_ERRNO => {
                            read_guard.finish_with(AsyncReadOutcome::Canceled);
                        }
                        Err(_) => read_guard.finish_with(AsyncReadOutcome::Failed),
                    }
                    result
                }),
            },
        })
    }

    /// Write file data through a VFS file handle.
    pub(crate) fn write(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<u32, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let started = Instant::now();
        let result = self
            .runtime_handle
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                file_handle,
                ino,
                offset,
                data,
                LocalSyncPolicy::Opportunistic,
            ))
            .map_err(|error| vfs_err_to_errno(&error));
        self.vfs
            .record_fuse_write_callback(data.len(), started.elapsed(), result.is_err());
        let written = result?;
        Ok(written)
    }

    /// Flush file data through a VFS file handle.
    pub(crate) fn flush(&self, ino: u64, file_handle: u64) -> Result<(), i32> {
        let result = self
            .runtime_handle
            .block_on(
                self.vfs
                    .flush_volatile_writeback_with_outcome(ino, file_handle),
            )
            .map(|_| ())
            .map_err(|error| vfs_err_to_errno(&error));
        normalize_close_flush_result(result)
    }

    /// Synchronize file data for a handle when available, otherwise for an inode.
    pub(crate) fn fsync(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        datasync: bool,
    ) -> Result<(), i32> {
        let result = if let Some(file_handle) = file_handle.filter(|handle| *handle != 0) {
            // StudioVfs does not currently distinguish fdatasync from fsync on
            // this path; both drain the handle and execute the strict metadata barrier.
            self.runtime_handle.block_on(self.vfs.fsync_fh(file_handle))
        } else {
            self.runtime_handle.block_on(self.vfs.fsync(ino, datasync))
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    /// Change a file length through the VFS setattr path.
    #[cfg(test)]
    pub(crate) fn set_len(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        size: u64,
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .setattr_with_handle(file_handle, ino, SetattrArgs::size_only(size))
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok(attr)
    }

    /// Change file attributes through the VFS setattr path.
    pub(crate) fn setattr(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        args: SetattrArgs,
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .setattr_with_handle(file_handle, ino, args)
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok(attr)
    }

    /// Release a file handle opened through this adapter.
    pub(crate) fn release(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        if self
            .runtime_stats_handles
            .lock()
            .remove(&file_handle)
            .is_some()
        {
            return Ok(());
        }
        let _outcome = self
            .vfs
            .release_fh_with_deferred_last_handle_barrier(file_handle, &self.runtime_handle);
        Ok(())
    }

    fn directory_parent(&self, ino: u64) -> Result<u64, i32> {
        self.vfs
            .getattr(ino)
            .map(|attr| if attr.parent == 0 { ino } else { attr.parent })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    #[cfg(test)]
    pub(crate) fn readdir_for_test(
        &self,
        ino: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let parent_ino = self.directory_parent(ino)?;
        let entries = self
            .vfs
            .readdir(ino)
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.readdir_from_entries(
            ino,
            parent_ino,
            offset,
            size,
            &entries,
            |buffer, name, _attr, next_offset| {
                buffer
                    .push_for_test(0, name, next_offset)
                    .map_err(directory_entry_errno)
            },
        )
    }

    pub(crate) fn opendir(&self, ino: u64) -> Result<u64, i32> {
        let parent_ino = self.directory_parent(ino)?;
        let entries = self
            .vfs
            .readdir(ino)
            .map_err(|error| vfs_err_to_errno(&error))?;
        let file_handle = self.next_directory_handle.fetch_add(1, Ordering::Relaxed);
        self.directory_handles.lock().insert(
            file_handle,
            Arc::new(DirectoryHandle {
                parent_ino,
                entries,
            }),
        );
        Ok(file_handle)
    }

    pub(crate) fn releasedir(&self, file_handle: u64) -> Result<(), i32> {
        self.directory_handles.lock().remove(&file_handle);
        Ok(())
    }

    #[cfg(studiofs_fskit)]
    pub(crate) fn directory_entries_for_fskit(
        &self,
        file_handle: u64,
    ) -> Result<Arc<DirectoryHandle>, i32> {
        self.directory_handles
            .lock()
            .get(&file_handle)
            .cloned()
            .ok_or(libc::EBADF)
    }

    pub(crate) fn readdir_with_handle(
        &self,
        request: &Request,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let entries = self
            .directory_handles
            .lock()
            .get(&file_handle)
            .cloned()
            .ok_or(libc::EBADF)?;
        self.readdir_from_entries(
            ino,
            entries.parent_ino,
            offset,
            size,
            &entries.entries,
            |buffer, name, attr, next_offset| {
                buffer
                    .push(request, name, attr, next_offset)
                    .map_err(directory_entry_errno)
            },
        )
    }

    #[cfg(test)]
    pub(crate) fn readdir_for_test_with_handle(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let entries = self
            .directory_handles
            .lock()
            .get(&file_handle)
            .cloned()
            .ok_or(libc::EBADF)?;
        self.readdir_from_entries(
            ino,
            entries.parent_ino,
            offset,
            size,
            &entries.entries,
            |buffer, name, _attr, next_offset| {
                buffer
                    .push_for_test(0, name, next_offset)
                    .map_err(directory_entry_errno)
            },
        )
    }

    fn readdir_from_entries(
        &self,
        ino: u64,
        parent_ino: u64,
        offset: i64,
        size: usize,
        entries: &[DirEntry],
        mut push: impl FnMut(&mut DirectoryEntryBuffer, &OsStr, &stat, off_t) -> Result<(), i32>,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let mut buffer = DirectoryEntryBuffer::new(size);
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        let parent_attr = minimal_dir_stat(parent_ino);
        if idx == 0 {
            if !push_or_finish(&mut buffer, OsStr::new("."), &dir_attr, 1, &mut push)? {
                return Ok(buffer);
            }
            idx = 1;
        }
        if idx == 1 {
            if !push_or_finish(&mut buffer, OsStr::new(".."), &parent_attr, 2, &mut push)? {
                return Ok(buffer);
            }
            idx = 2;
        }

        for (i, entry) in entries.iter().enumerate().skip(idx.saturating_sub(2)) {
            let entry_idx = i + 2;
            let attr = self.dir_entry_to_stat(entry);
            let next_offset = off_t::try_from(entry_idx + 1).map_err(|_| libc::EOVERFLOW)?;
            if !push_or_finish(
                &mut buffer,
                OsStr::from_bytes(&entry.name),
                &attr,
                next_offset,
                &mut push,
            )? {
                break;
            }
        }
        Ok(buffer)
    }

    /// Read the target for a symlink inode.
    pub(crate) fn readlink(&self, ino: u64) -> Result<Vec<u8>, i32> {
        self.vfs
            .readlink(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get filesystem statistics.
    pub(crate) fn statfs(&self) -> Result<statvfs, i32> {
        self.vfs
            .statfs()
            .map(statfs_to_statvfs)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get a native extended attribute value.
    pub(crate) fn get_xattr(&self, ino: u64, name: &[u8]) -> Result<Option<Vec<u8>>, i32> {
        self.vfs
            .get_xattr(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get a native extended attribute value size without returning the value bytes.
    pub(crate) fn get_xattr_size(&self, ino: u64, name: &[u8]) -> Result<Option<usize>, i32> {
        self.vfs
            .get_xattr_size(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Set a native extended attribute value.
    pub(crate) fn set_xattr(
        &self,
        ino: u64,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let flags = macfuse_set_xattr_flags(name, flags);
        self.vfs
            .set_xattr(ino, name, value, flags)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    /// Remove a native extended attribute value.
    pub(crate) fn remove_xattr(&self, ino: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .remove_xattr(ino, name)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    /// List native extended attribute names.
    pub(crate) fn list_xattrs(&self, ino: u64) -> Result<Vec<Vec<u8>>, i32> {
        self.vfs
            .list_xattrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    pub(crate) fn attr_to_stat(&self, attr: &InodeAttr) -> stat {
        let mut stat = bridge_attr_to_stat(&to_bridge_attr(
            attr,
            self.mount_uid,
            self.mount_gid,
            MACFUSE_ATTR_BLOCK_SIZE,
        ));
        if let Some(flags) = self.file_flags.lock().get(&attr.ino).copied() {
            assign_stat_flags(&mut stat, flags);
            if flags & MACOS_UF_HIDDEN != 0 {
                assign_hidden_file_stat_times(&mut stat);
            }
        }
        stat
    }

    fn remember_callback_file_flags(&self, ino: u64, attr: *const stat, to_set: libc::c_int) {
        if to_set == 0 || setattr_mask_has_flags(to_set) {
            self.file_flags
                .lock()
                .insert(ino, callback_stat_flags(attr));
        }
    }

    fn remember_named_file_flags(&self, ino: u64, name: &[u8]) {
        if name == MACOS_DS_STORE_NAME {
            self.file_flags.lock().insert(ino, MACOS_UF_HIDDEN);
        }
    }

    #[cfg(studiofs_fskit)]
    pub(crate) fn remember_fskit_file_flags(&self, ino: u64, flags: u32) {
        self.file_flags.lock().insert(ino, flags);
    }

    fn clear_file_flags(&self, ino: u64) {
        self.file_flags.lock().remove(&ino);
    }

    #[cfg(test)]
    pub(crate) fn file_flags_contains_for_test(&self, ino: u64) -> bool {
        self.file_flags.lock().contains_key(&ino)
    }

    fn dir_entry_to_stat(&self, entry: &DirEntry) -> stat {
        let mut out = minimal_stat(entry.ino);
        out.st_ino = entry.ino;
        out.st_mode = match entry.kind {
            FileType::RegularFile => libc::S_IFREG,
            FileType::Directory => libc::S_IFDIR,
            FileType::Symlink => libc::S_IFLNK,
        };
        out
    }
}

pub(crate) struct CallbackState {
    adapter: Arc<StudioVfsAdapter>,
}

impl CallbackState {
    pub(crate) fn new(vfs: Arc<StudioVfs>, runtime_handle: Handle, read_only: bool) -> Self {
        Self {
            adapter: Arc::new(StudioVfsAdapter::new_with_read_only(
                vfs,
                runtime_handle,
                read_only,
            )),
        }
    }

    pub(crate) fn kernel_invalidator(
        &self,
        inner: Arc<dyn KernelInvalidator>,
    ) -> Arc<dyn KernelInvalidator> {
        self.adapter.kernel_invalidator(inner)
    }

    pub(crate) fn wait_for_reply_tasks(&self) {
        self.adapter.reply_tasks.wait_for_idle();
    }

    #[cfg(studiofs_fskit)]
    pub(crate) fn adapter(&self) -> Arc<StudioVfsAdapter> {
        Arc::clone(&self.adapter)
    }

    #[cfg(studiofs_fskit)]
    pub(crate) fn track_reply_task(&self) -> ReplyTaskGuard {
        self.adapter.reply_tasks.track()
    }
}

#[derive(Default)]
pub(crate) struct ReplyTaskTracker {
    active: Mutex<usize>,
    idle: Condvar,
}

pub(crate) struct ReplyTaskGuard {
    tracker: Arc<ReplyTaskTracker>,
}

impl ReplyTaskTracker {
    fn track(self: &Arc<Self>) -> ReplyTaskGuard {
        *self
            .active
            .lock()
            .expect("reply task tracker lock should not be poisoned") += 1;
        ReplyTaskGuard {
            tracker: Arc::clone(self),
        }
    }

    fn wait_for_idle(&self) {
        let mut active = self
            .active
            .lock()
            .expect("reply task tracker lock should not be poisoned");
        while *active != 0 {
            active = self
                .idle
                .wait(active)
                .expect("reply task tracker lock should not be poisoned");
        }
    }
}

impl Drop for ReplyTaskGuard {
    fn drop(&mut self) {
        let mut active = self
            .tracker
            .active
            .lock()
            .expect("reply task tracker lock should not be poisoned");
        *active -= 1;
        if *active == 0 {
            self.tracker.idle.notify_all();
        }
    }
}

fn callback_state<'a>(req: *mut raw::FuseReq) -> Option<&'a CallbackState> {
    if req.is_null() {
        return None;
    }
    // SAFETY: `req` is a live macFUSE request supplied by a low-level callback.
    // macFUSE returns the same userdata pointer passed to session creation.
    let userdata = unsafe { raw::fuse_req_userdata(req) }.cast::<CallbackState>();
    if userdata.is_null() {
        None
    } else {
        // SAFETY: `CallbackState` is boxed by `mount_with_request` and kept live
        // until after the session loop exits. Callbacks borrow it immutably only.
        Some(unsafe { &*userdata })
    }
}

fn reply_error(req: *mut raw::FuseReq, errno: i32) {
    if let Some(request) = owned_request(req) {
        let _ = Reply::new(request).error(errno);
    }
}

fn reply_callback_error(req: *mut raw::FuseReq, operation: &'static str, errno: i32) {
    log_callback_error(operation, errno);
    reply_error(req, errno);
}

fn log_callback_error(operation: &'static str, errno: i32) {
    match callback_error_log_level(operation, errno) {
        Some(CallbackErrorLogLevel::Warn) => {
            warn!(operation, errno, "macFUSE callback returned error");
        }
        None => {}
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallbackErrorLogLevel {
    Warn,
}

fn callback_error_log_level(operation: &'static str, errno: i32) -> Option<CallbackErrorLogLevel> {
    if errno == 0 {
        return None;
    }
    if operation == "getxattr" && errno == xattr_not_found_errno() {
        return None;
    }
    if operation == "lookup" && errno == libc::ENOENT {
        return None;
    }
    if operation == "getattr" && errno == libc::ENOENT {
        return None;
    }
    if operation == "removexattr" && (errno == libc::ENOENT || errno == xattr_not_found_errno()) {
        return None;
    }
    if operation == "read" && errno == MACFUSE_READ_INTERRUPTED_ERRNO {
        return None;
    }
    Some(CallbackErrorLogLevel::Warn)
}

fn owned_request(req: *mut raw::FuseReq) -> Option<Request> {
    // SAFETY: Each low-level callback receives a live unreplied request pointer.
    unsafe { Request::from_raw(req) }
}

fn directory_entry_errno(error: crate::low_level::DirectoryEntryError) -> i32 {
    match error {
        crate::low_level::DirectoryEntryError::InsufficientCapacity { .. } => libc::ERANGE,
        crate::low_level::DirectoryEntryError::InteriorNul => libc::EINVAL,
    }
}

fn minimal_dir_stat(ino: u64) -> stat {
    let mut out = minimal_stat(ino);
    out.st_mode = libc::S_IFDIR;
    out
}

fn minimal_stat(ino: u64) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: A zeroed `stat` is a valid C value for integer/time fields.
    // readdir only needs inode and mode bits for directory entry packing.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = ino;
    out
}

fn push_or_finish(
    buffer: &mut DirectoryEntryBuffer,
    name: &OsStr,
    attr: &stat,
    next_offset: off_t,
    push: &mut impl FnMut(&mut DirectoryEntryBuffer, &OsStr, &stat, off_t) -> Result<(), i32>,
) -> Result<bool, i32> {
    match push(buffer, name, attr, next_offset) {
        Ok(()) => Ok(true),
        Err(libc::ERANGE) => Ok(false),
        Err(errno) => Err(errno),
    }
}

fn posix_open_requests_write(flags: i32, trunc: bool) -> bool {
    let access_mode = flags & libc::O_ACCMODE;
    trunc
        || access_mode == libc::O_WRONLY
        || access_mode == libc::O_RDWR
        || flags & libc::O_APPEND != 0
}

/// Translate POSIX/macFUSE open flags into the per-handle cache-retention policy.
pub(crate) fn macfuse_open_cache_policy(flags: i32, trunc: bool) -> MacFuseOpenCachePolicy {
    let write_capable = posix_open_requests_write(flags, trunc);
    MacFuseOpenCachePolicy {
        // Keep write buffers out of macFUSE's corruptible UBC copy path.
        direct_io: write_capable,
        keep_cache: !write_capable,
        write_capable,
    }
}

fn macfuse_cache_metric(
    attr: &InodeAttr,
    decision: OsCacheOpenDecision,
) -> VfsMacFuseOpenCacheDecision {
    let remote_ready = attr.upload_state == UploadState::Ready
        && attr.content_epoch == attr.committed_content_epoch
        && attr.size == attr.committed_size;
    match decision.reason() {
        OsCacheReason::StableGeneration if remote_ready => {
            VfsMacFuseOpenCacheDecision::ReadOnlyKeep
        }
        OsCacheReason::StableGeneration => VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableKeep,
        OsCacheReason::FirstObservation | OsCacheReason::GenerationChanged if remote_ready => {
            VfsMacFuseOpenCacheDecision::ReadOnlyGenerationChanged
        }
        OsCacheReason::FirstObservation | OsCacheReason::GenerationChanged => {
            VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableGenerationChanged
        }
        OsCacheReason::NonRegular => VfsMacFuseOpenCacheDecision::ReadOnlyNotRegularFile,
        OsCacheReason::LocalDirty => VfsMacFuseOpenCacheDecision::ReadOnlyLocalDirtyState,
        OsCacheReason::ActiveWriter => VfsMacFuseOpenCacheDecision::ReadOnlyActiveWriter,
        OsCacheReason::WriteCapable => VfsMacFuseOpenCacheDecision::WriteCapable,
        OsCacheReason::Disabled | OsCacheReason::UnknownState | OsCacheReason::UnknownHandle => {
            VfsMacFuseOpenCacheDecision::ReadOnlyUnknownState
        }
    }
}

struct AvailableMemorySnapshot {
    system: System,
    available: Option<u64>,
    refreshed_at: Instant,
}

impl AvailableMemorySnapshot {
    fn new() -> Self {
        let mut snapshot = Self {
            system: System::new(),
            available: None,
            refreshed_at: Instant::now(),
        };
        snapshot.refresh();
        snapshot
    }

    fn available_memory(&mut self) -> Option<u64> {
        if self.refreshed_at.elapsed() >= MACFUSE_AVAILABLE_MEMORY_TTL {
            self.refresh();
        }
        self.available
    }

    fn refresh(&mut self) {
        self.system
            .refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        let available = self.system.available_memory();
        self.available = (available > 0).then_some(available);
        self.refreshed_at = Instant::now();
    }
}

fn available_memory_bytes() -> Option<u64> {
    static AVAILABLE_MEMORY: OnceLock<ParkingMutex<AvailableMemorySnapshot>> = OnceLock::new();
    AVAILABLE_MEMORY
        .get_or_init(|| ParkingMutex::new(AvailableMemorySnapshot::new()))
        .lock()
        .available_memory()
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` is thread-safe, takes no pointers, and has no Rust-side
    // invariants beyond the platform libc contract.
    unsafe { libc::geteuid() }
}

fn current_gid() -> u32 {
    // SAFETY: `getegid` is thread-safe, takes no pointers, and has no Rust-side
    // invariants beyond the platform libc contract.
    unsafe { libc::getegid() }
}

#[cfg(test)]
mod open_cache_snapshot_tests {
    use super::{
        AvailableMemorySnapshot, MACFUSE_AVAILABLE_MEMORY_TTL, MacFuseOpenCacheGraphSpan,
        MacFuseOpenCachePolicy, MacFuseOpenCacheSnapshot, MacFuseOpenCacheSnapshotKind,
        StudioVfsAdapter,
    };
    use std::time::Instant;
    use studiofs_meta::{InodeAttr, UploadState};
    use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};
    use studiofs_vfs::vfs::OpenFlags;

    #[test]
    fn open_without_cache_must_not_imply_write_capable_handle() {
        let fixture = ContractVfsFixture::new("macfuse-open-cache-independent-policy")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(CONTRACT_ROOT_INO, b"independent-policy.bin", 501, 20, 0o644)
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("open read-only file");
        let policy = MacFuseOpenCachePolicy {
            direct_io: false,
            keep_cache: false,
            write_capable: false,
        };

        let opened = adapter
            .open_with_result(attr.ino, policy, Ok(fh))
            .expect("open with explicit cache policy");

        assert!(!opened.keep_cache());
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .macfuse_open_cache
                .read_only_active_writer,
            0,
            "a cache-disabled read-only handle must not enter writer accounting"
        );
        adapter
            .release(attr.ino, opened.file_handle())
            .expect("release read-only handle");
    }

    #[test]
    fn read_only_open_policy_reuses_generation_attr_for_direct_io() {
        let fixture =
            ContractVfsFixture::new("macfuse-open-cache-direct-io").expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(CONTRACT_ROOT_INO, b"direct-io.bin", 501, 20, 0o644)
            .expect("create file");
        vfs.setattr(attr.ino, studiofs_meta::SetattrArgs::size_only(1025))
            .expect("set file size");
        let adapter = StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let fh = vfs
            .open_file(attr.ino, OpenFlags::RDONLY, false)
            .expect("open read-only file");
        let policy = adapter.read_only_open_cache_policy(attr.ino, fh, Some(1024));

        assert!(policy.direct_io);
        adapter
            .release(attr.ino, fh)
            .expect("release read-only file");
    }

    #[test]
    fn expired_available_memory_snapshot_is_refreshed() {
        let mut snapshot = AvailableMemorySnapshot {
            system: sysinfo::System::new(),
            available: None,
            refreshed_at: Instant::now() - MACFUSE_AVAILABLE_MEMORY_TTL,
        };
        let before_refresh = snapshot.refreshed_at;

        let _ = snapshot.available_memory();

        assert!(snapshot.refreshed_at > before_refresh);
    }

    #[test]
    fn fresh_available_memory_snapshot_is_reused() {
        let mut snapshot = AvailableMemorySnapshot {
            system: sysinfo::System::new(),
            available: None,
            refreshed_at: Instant::now(),
        };

        assert_eq!(snapshot.available_memory(), None);
    }

    #[test]
    fn read_only_open_avoids_a_preliminary_attr_lookup() {
        let fixture =
            ContractVfsFixture::new("macfuse-open-cache-attr-lookups").expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(CONTRACT_ROOT_INO, b"attr-lookups.bin", 501, 20, 0o644)
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let before = vfs.cache_state_snapshot().attr_read_cache;

        let opened = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open read-only file");

        let after = vfs.cache_state_snapshot().attr_read_cache;
        assert_eq!(
            (after.hits + after.misses) - (before.hits + before.misses),
            4,
            "read-only open should resolve attrs only in VFS open and cache-generation checks"
        );
        adapter
            .release(attr.ino, opened.file_handle())
            .expect("release read-only handle");
    }

    #[test]
    fn open_cache_snapshot_identity_ignores_publication_only_transition() {
        let mut attr = InodeAttr::new_file(42, 1, 501, 20, 0o644);
        attr.size = 4096;
        attr.content_epoch = 7;
        attr.committed_size = 0;
        attr.committed_content_epoch = 6;
        attr.upload_state = UploadState::Updating;
        let graph = vec![MacFuseOpenCacheGraphSpan {
            chunk_id: 0,
            range_start: 0,
            range_end: 128,
            source: 1,
            slice_id: Some(10),
            slice_order: Some(0),
            payload: None,
        }];

        let local_stable = MacFuseOpenCacheSnapshot::from_attr(&attr, graph.clone());
        assert_eq!(
            local_stable.kind,
            MacFuseOpenCacheSnapshotKind::LocalStableUnpublished
        );
        attr.committed_size = attr.size;
        attr.committed_content_epoch = attr.content_epoch;
        attr.upload_state = UploadState::Ready;
        let remote_ready = MacFuseOpenCacheSnapshot::from_attr(&attr, graph);
        assert_eq!(remote_ready.kind, MacFuseOpenCacheSnapshotKind::RemoteReady);

        assert_eq!(
            local_stable, remote_ready,
            "publication-only transitions must preserve UBC pages for unchanged content"
        );
    }

    #[test]
    fn write_release_leaves_next_read_open_to_drop_kernel_cache() {
        let fixture = ContractVfsFixture::new("macfuse-open-cache-write-release")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(CONTRACT_ROOT_INO, b"cache-after-write.bin", 501, 20, 0o644)
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        let opened = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open write-capable file");
        let fh = opened.file_handle();
        adapter
            .write(attr.ino, fh, 0, b"fresh sparse payload")
            .expect("write payload");
        adapter.flush(attr.ino, fh).expect("flush payload");
        adapter.release(attr.ino, fh).expect("release writer");

        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "the first read-only open after a write must drop kernel pages before cache can be kept"
        );
    }

    #[test]
    fn rejected_open_reply_aborts_cache_generation_before_retry() {
        let fixture = ContractVfsFixture::new("macfuse-open-cache-rejected-reply")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                CONTRACT_ROOT_INO,
                b"rejected-open-reply.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());

        let rejected = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("prepare first read-only open");
        assert!(!rejected.keep_cache());
        adapter.reject_prepared_open(attr.ino, rejected.file_handle());

        let retry = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("retry read-only open");
        assert!(
            !retry.keep_cache(),
            "a rejected reply must not seed keep_cache for the next open"
        );
        adapter
            .release(attr.ino, retry.file_handle())
            .expect("release retry handle");
    }

    #[test]
    fn rejected_create_reply_aborts_cache_generation_before_retry() {
        let fixture = ContractVfsFixture::new("macfuse-create-cache-rejected-reply")
            .expect("create VFS fixture");
        let adapter = StudioVfsAdapter::new(fixture.vfs(), fixture.runtime().handle().clone());

        let (attr, rejected, reply_guard) = adapter
            .create_for_reply(
                CONTRACT_ROOT_INO,
                b"rejected-create-reply.bin",
                0o644,
                libc::O_RDONLY,
            )
            .expect("prepare create reply");
        assert!(!rejected.keep_cache());
        adapter.reject_prepared_open(attr.ino, rejected.file_handle());
        drop(reply_guard);

        let retry = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("retry read-only open after rejected create");
        assert!(
            !retry.keep_cache(),
            "a rejected create reply must not seed keep_cache for the next open"
        );
        adapter
            .release(attr.ino, retry.file_handle())
            .expect("release retry handle");
    }

    #[test]
    fn runtime_stats_open_stays_direct_and_preserves_macfuse_cache_json() {
        let fixture =
            ContractVfsFixture::new("macfuse-runtime-stats-cache-schema").expect("create fixture");
        let adapter = StudioVfsAdapter::new(fixture.vfs(), fixture.runtime().handle().clone());
        let ino = studiofs_vfs::RUNTIME_STATS_FILE_INO;
        let before = fixture
            .vfs()
            .write_path_observability_snapshot()
            .macfuse_open_cache
            .total;

        let open = adapter
            .open(ino, libc::O_RDONLY)
            .expect("open runtime stats");
        assert!(open.direct_io());
        assert!(!open.keep_cache());
        let after = fixture
            .vfs()
            .write_path_observability_snapshot()
            .macfuse_open_cache
            .total;
        assert_eq!(after, before, "runtime stats must bypass cache preparation");
        let json = serde_json::to_value(fixture.vfs().write_path_observability_snapshot())
            .expect("write-path runtime stats should serialize");
        assert!(
            json.get("macfuse_open_cache").is_some(),
            "the existing macfuse_open_cache JSON section must remain present"
        );
        adapter
            .release(ino, open.file_handle())
            .expect("release runtime stats");
    }

    #[test]
    fn close_flush_ignores_namespace_enoent() {
        assert_eq!(
            super::normalize_close_flush_result(Err(libc::ENOENT)),
            Ok(())
        );
    }

    #[test]
    fn close_flush_preserves_non_namespace_errors() {
        assert_eq!(
            super::normalize_close_flush_result(Err(libc::EIO)),
            Err(libc::EIO)
        );
    }

    #[test]
    fn last_write_release_clears_snapshot_even_when_read_handle_remains_open() {
        let fixture = ContractVfsFixture::new("macfuse-open-cache-write-before-read-release")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                CONTRACT_ROOT_INO,
                b"write-before-read-release.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());
        let seed = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("seed cache observation");
        adapter
            .release(attr.ino, seed.file_handle())
            .expect("release seed");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open retained reader");
        assert!(reader.keep_cache());
        let writer = adapter
            .open(attr.ino, libc::O_WRONLY)
            .expect("open write-capable handle");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer while reader remains open");

        assert!(
            !adapter.has_open_cache_snapshot_for_test(attr.ino),
            "last writer close must invalidate open-cache snapshots even while readers remain open"
        );
        adapter
            .release(attr.ino, reader.file_handle())
            .expect("release retained reader");
    }

    #[test]
    fn read_after_deferred_release_waits_for_last_handle_barrier() {
        let fixture = ContractVfsFixture::new("macfuse-read-after-deferred-release")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(CONTRACT_ROOT_INO, b"read-after-release.bin", 501, 20, 0o644)
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs.clone(), fixture.runtime().handle().clone());
        let payload = b"read after deferred release";

        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open write-capable file");
        adapter
            .write(attr.ino, writer.file_handle(), 0, payload)
            .expect("write payload");
        assert!(
            vfs.inode_has_local_dirty_state(attr.ino),
            "test setup must make the file dirty before fsync"
        );
        fixture
            .runtime()
            .block_on(vfs.fsync(attr.ino, false))
            .expect("fsync payload before deferred release");
        assert!(
            !vfs.inode_has_local_dirty_state(attr.ino),
            "fsync must clean local dirty state so release uses the deferred barrier path"
        );
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer");

        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader after deferred release");
        let read = adapter
            .read(attr.ino, reader.file_handle(), 0, payload.len() as u32)
            .expect("read should wait for deferred release barrier");

        assert_eq!(read, payload);
    }

    #[test]
    fn read_after_volatile_flush_and_release_preserves_local_data() {
        let fixture = ContractVfsFixture::new("macfuse-read-after-volatile-release")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                CONTRACT_ROOT_INO,
                b"read-after-volatile-release.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());
        let payload = vec![0xB7; 256 * 1024];

        let writer = adapter
            .open(attr.ino, libc::O_RDWR)
            .expect("open write-capable file");
        adapter
            .write(attr.ino, writer.file_handle(), 0, &payload)
            .expect("write payload");
        adapter
            .flush(attr.ino, writer.file_handle())
            .expect("volatile flush payload");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer");

        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader immediately after release");
        let read = adapter
            .read(attr.ino, reader.file_handle(), 0, payload.len() as u32)
            .expect("read should preserve locally accepted bytes");

        assert_eq!(read, payload);
        adapter
            .release(attr.ino, reader.file_handle())
            .expect("release reader");
    }

    #[test]
    fn large_write_without_fsync_is_visible_after_writer_release() {
        let fixture = ContractVfsFixture::new("macfuse-large-write-release-read")
            .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                CONTRACT_ROOT_INO,
                b"large-write-release-read.bin",
                501,
                20,
                0o644,
            )
            .expect("create file");
        let adapter = StudioVfsAdapter::new(vfs, fixture.runtime().handle().clone());
        let chunk = vec![0xC3; 8 * 1024 * 1024];

        let writer = adapter.open(attr.ino, libc::O_WRONLY).expect("open writer");
        adapter
            .write(attr.ino, writer.file_handle(), 0, &chunk)
            .expect("write first chunk");
        adapter
            .write(attr.ino, writer.file_handle(), chunk.len() as i64, &chunk)
            .expect("write second chunk");
        let reader = adapter
            .open(attr.ino, libc::O_RDONLY)
            .expect("open reader before writer release");
        adapter
            .release(attr.ino, writer.file_handle())
            .expect("release writer");

        let read = adapter
            .read(attr.ino, reader.file_handle(), 0, (chunk.len() * 2) as u32)
            .expect("read locally accepted bytes");

        assert_eq!(read.len(), chunk.len() * 2);
        assert_eq!(&read[..chunk.len()], chunk.as_slice());
        assert_eq!(&read[chunk.len()..], chunk.as_slice());
        adapter
            .release(attr.ino, reader.file_handle())
            .expect("release reader");
    }
}

fn bridge_attr_to_stat(attr: &BridgeFileAttr) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: A zeroed `stat` is a valid C value for integer/time fields; all
    // fields relevant to macFUSE replies are then initialized below.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = attr.ino;
    out.st_mode = mode_bits(attr);
    out.st_nlink = attr.nlink as _;
    out.st_uid = attr.uid;
    out.st_gid = attr.gid;
    out.st_rdev = attr.rdev as _;
    out.st_size = attr.size as _;
    out.st_blocks = attr.blocks as _;
    out.st_blksize = attr.blksize as _;
    assign_stat_time(&mut out, StatTimeField::Access, attr.atime);
    assign_stat_time(&mut out, StatTimeField::Modified, attr.mtime);
    assign_stat_time(&mut out, StatTimeField::Changed, attr.ctime);
    assign_stat_time(&mut out, StatTimeField::Created, attr.crtime);
    out
}

#[cfg(target_os = "macos")]
fn assign_stat_flags(out: &mut stat, flags: u32) {
    out.st_flags = flags;
}

#[cfg(not(target_os = "macos"))]
fn assign_stat_flags(_out: &mut stat, _flags: u32) {}

#[cfg(target_os = "macos")]
fn assign_hidden_file_stat_times(out: &mut stat) {
    out.st_birthtime = out.st_mtime;
    out.st_birthtime_nsec = out.st_mtime_nsec;
    out.st_ctime = out.st_mtime;
    out.st_ctime_nsec = out.st_mtime_nsec;
}

#[cfg(not(target_os = "macos"))]
fn assign_hidden_file_stat_times(_out: &mut stat) {}

#[cfg(target_os = "macos")]
fn callback_stat_flags(attr: *const stat) -> u32 {
    if attr.is_null() {
        0
    } else {
        // SAFETY: The pointer was checked for null above and macFUSE owns it
        // for the duration of the callback.
        unsafe { (*attr).st_flags }
    }
}

#[cfg(not(target_os = "macos"))]
fn callback_stat_flags(_attr: *const stat) -> u32 {
    0
}

fn mode_bits(attr: &BridgeFileAttr) -> libc::mode_t {
    let kind = match attr.kind {
        BridgeFileType::RegularFile => libc::S_IFREG,
        BridgeFileType::Directory => libc::S_IFDIR,
        BridgeFileType::Symlink => libc::S_IFLNK,
    };
    kind | libc::mode_t::from(attr.perm)
}

enum StatTimeField {
    Access,
    Modified,
    Changed,
    Created,
}

fn assign_stat_time(out: &mut stat, field: StatTimeField, time: std::time::SystemTime) {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs() as _;
    let nanos = duration.subsec_nanos() as _;
    match field {
        StatTimeField::Access => {
            out.st_atime = seconds;
            out.st_atime_nsec = nanos;
        }
        StatTimeField::Modified => {
            out.st_mtime = seconds;
            out.st_mtime_nsec = nanos;
        }
        StatTimeField::Changed => {
            out.st_ctime = seconds;
            out.st_ctime_nsec = nanos;
        }
        StatTimeField::Created => {
            out.st_birthtime = seconds;
            out.st_birthtime_nsec = nanos;
        }
    }
}

fn statfs_to_statvfs(stats: StatFs) -> statvfs {
    let out = MaybeUninit::<statvfs>::zeroed();
    // SAFETY: A zeroed `statvfs` is a valid C value for integer fields; all
    // fields used by macFUSE are then initialized below.
    let mut out = unsafe { out.assume_init() };
    out.f_bsize = stats.bsize as _;
    out.f_frsize = stats.frsize as _;
    out.f_blocks = stats.blocks as _;
    out.f_bfree = stats.bfree as _;
    out.f_bavail = stats.bavail as _;
    out.f_files = stats.files as _;
    out.f_ffree = stats.ffree as _;
    out.f_namemax = stats.namelen as _;
    out
}

/// macFUSE lookup callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_lookup(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for lookup callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.lookup(parent, name) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => {
            if callback_error_log_level("lookup", errno).is_some() {
                warn!(
                    parent,
                    name = %String::from_utf8_lossy(name),
                    errno,
                    "macFUSE lookup callback returned error"
                );
            }
            reply_error(req, errno);
        }
    }
}

/// macFUSE forget callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_forget(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    nlookup: c_ulong,
) {
    let Some(state) = callback_state(req) else {
        return;
    };
    state.adapter.forget(ino, nlookup);
}

/// macFUSE getattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_getattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.getattr(ino) {
        Ok(attr) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&attr, TTL_SECONDS);
            }
        }
        Err(errno) => {
            if callback_error_log_level("getattr", errno).is_some() {
                warn!(ino, errno, "macFUSE getattr callback returned error");
            }
            reply_error(req, errno);
        }
    }
}

/// macFUSE mkdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_mkdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for mkdir callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.mkdir(parent, name, mode) {
        Ok(attr) => {
            debug!(
                parent,
                name = %String::from_utf8_lossy(name),
                ino = attr.ino,
                mode,
                "macFUSE mkdir callback created entry"
            );
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                errno,
                "macFUSE mkdir callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE symlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_symlink(
    req: *mut raw::FuseReq,
    target: *const c_char,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if target.is_null() || name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes NUL-terminated symlink target and entry name strings.
    let target = unsafe { CStr::from_ptr(target) }.to_bytes();
    // SAFETY: macFUSE passes NUL-terminated symlink target and entry name strings.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    let Some(request) = owned_request(req) else {
        return;
    };
    state
        .adapter
        .queue_symlink_reply(parent, name.to_vec(), target.to_vec(), request);
}

/// macFUSE mknod callback for regular-file creation paths that do not use create.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_mknod(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    _rdev: libc::dev_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for mknod callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.mknod(parent, name, mode) {
        Ok(attr) => {
            debug!(
                parent,
                name = %String::from_utf8_lossy(name),
                ino = attr.ino,
                mode,
                "macFUSE mknod callback created entry"
            );
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                mode,
                errno,
                "macFUSE mknod callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE create callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_create(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() || fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for create callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    // SAFETY: macFUSE passes initialized file info for create callbacks.
    let flags = unsafe { (*fi).flags };
    match state.adapter.create_for_reply(parent, name, mode, flags) {
        Ok((attr, open, _reply_guard)) => {
            debug!(
                parent,
                name = %String::from_utf8_lossy(name),
                ino = attr.ino,
                mode,
                flags,
                file_handle = open.file_handle(),
                "macFUSE create callback created entry"
            );
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                if let Err(error) = Reply::new(request).create(&entry, open) {
                    warn!(
                        ino = attr.ino,
                        file_handle = open.file_handle(),
                        ?error,
                        "macFUSE create reply was rejected"
                    );
                    state
                        .adapter
                        .reject_prepared_open(attr.ino, open.file_handle());
                }
            } else {
                state
                    .adapter
                    .reject_prepared_open(attr.ino, open.file_handle());
            }
        }
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                mode,
                flags,
                errno,
                "macFUSE create callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE unlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_unlink(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for unlink callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.unlink(parent, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                errno,
                "macFUSE unlink callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE rmdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_rmdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for rmdir callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    let Some(request) = owned_request(req) else {
        return;
    };
    state
        .adapter
        .queue_rmdir_reply(parent, name.to_vec(), request);
}

/// macFUSE rename callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_rename(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    newparent: raw::FuseIno,
    newname: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() || newname.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes NUL-terminated entry names for rename callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    // SAFETY: macFUSE passes NUL-terminated entry names for rename callbacks.
    let newname = unsafe { CStr::from_ptr(newname) }.to_bytes();
    match state.adapter.rename(parent, name, newparent, newname) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                newparent,
                newname = %String::from_utf8_lossy(newname),
                errno,
                "macFUSE rename callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE open callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_open(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for open.
    let flags = unsafe { (*fi).flags };
    let _reply_guard =
        (!state.adapter.is_runtime_stats_file_ino(ino)).then(|| state.adapter.lock_open_reply(ino));
    match state.adapter.open(ino, flags) {
        Ok(open) => {
            if let Some(request) = owned_request(req) {
                if let Err(error) = Reply::new(request).open(open) {
                    warn!(
                        ino,
                        file_handle = open.file_handle(),
                        ?error,
                        "macFUSE open reply was rejected"
                    );
                    if state.adapter.is_runtime_stats_file_ino(ino) {
                        if let Err(errno) = state.adapter.release(ino, open.file_handle()) {
                            warn!(
                                ino,
                                file_handle = open.file_handle(),
                                errno,
                                "macFUSE failed to close runtime stats after rejected open reply"
                            );
                        }
                    } else {
                        state.adapter.reject_prepared_open(ino, open.file_handle());
                    }
                }
            } else {
                if state.adapter.is_runtime_stats_file_ino(ino) {
                    if let Err(errno) = state.adapter.release(ino, open.file_handle()) {
                        warn!(
                            ino,
                            file_handle = open.file_handle(),
                            errno,
                            "macFUSE failed to close runtime stats without an open request"
                        );
                    }
                } else {
                    state.adapter.reject_prepared_open(ino, open.file_handle());
                }
            }
        }
        Err(errno) => {
            warn!(ino, flags, errno, "macFUSE open callback returned error");
            reply_error(req, errno);
        }
    }
}

/// macFUSE read callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_read(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let size = match u32::try_from(size) {
        Ok(size) => size,
        Err(_) => {
            reply_error(req, libc::EOVERFLOW);
            return;
        }
    };
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for read.
    let file_handle = unsafe { (*fi).fh };
    let Some(request) = owned_request(req) else {
        debug!(
            ino,
            file_handle,
            offset = off,
            size,
            "macFUSE read request ownership unavailable"
        );
        return;
    };
    state
        .adapter
        .queue_read_reply(ino, file_handle, off, size, request);
}

/// macFUSE write callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_write(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    buf: *const c_char,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let data = if size == 0 {
        &[]
    } else {
        if buf.is_null() {
            reply_error(req, libc::EINVAL);
            return;
        }
        // SAFETY: macFUSE passes a valid buffer of `size` bytes for non-empty
        // writes, and the borrowed slice is used only within this callback.
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size) }
    };
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for write.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.write(ino, file_handle, off, data) {
        Ok(count) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).write_count(count as usize);
            }
        }
        Err(errno) => {
            warn!(
                ino,
                file_handle,
                offset = off,
                size,
                errno,
                "macFUSE write callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE flush callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_flush(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for flush.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.flush(ino, file_handle) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                ino,
                file_handle, errno, "macFUSE flush callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE fsync callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_fsync(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    datasync: libc::c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let file_handle = if fi.is_null() {
        None
    } else {
        // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for handle-backed fsync.
        Some(unsafe { (*fi).fh })
    };
    match state.adapter.fsync(file_handle, ino, datasync != 0) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                ino,
                file_handle,
                datasync = datasync != 0,
                errno,
                "macFUSE fsync callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE setattr callback for size-changing truncate/extend operations.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_setattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    attr: *mut stat,
    to_set: libc::c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if attr.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let args = match setattr_args_from_callback(attr, to_set) {
        Ok(args) => args,
        Err(errno) => {
            warn!(
                ino,
                to_set, errno, "macFUSE setattr callback rejected arguments"
            );
            reply_error(req, errno);
            return;
        }
    };
    if setattr_args_are_empty(&args) {
        if setattr_mask_is_accepted_noop(to_set) {
            state
                .adapter
                .remember_callback_file_flags(ino, attr, to_set);
            debug!(
                ino,
                to_set, "macFUSE setattr callback accepted unsupported metadata bits as no-op"
            );
            match state.adapter.getattr(ino) {
                Ok(stat) => {
                    if let Some(request) = owned_request(req) {
                        let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
                    }
                }
                Err(errno) => {
                    warn!(
                        ino,
                        to_set, errno, "macFUSE setattr no-op getattr returned error"
                    );
                    reply_error(req, errno);
                }
            }
            return;
        }
        warn!(
            ino,
            to_set,
            errno = libc::ENOTSUP,
            "macFUSE setattr callback received no supported attributes"
        );
        reply_error(req, libc::ENOTSUP);
        return;
    }
    let file_handle = if fi.is_null() || !setattr_mask_has_fh(to_set) {
        None
    } else {
        // SAFETY: macFUSE guarantees a valid file handle pointer when the
        // callback mask includes the header-defined file-handle bit.
        Some(unsafe { (*fi).fh })
    };
    match state.adapter.setattr(file_handle, ino, args) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
            }
        }
        Err(errno) => {
            warn!(
                ino,
                file_handle, to_set, errno, "macFUSE setattr callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE Darwin setattr extension callback for file flags and extra times.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_setattr_x(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    attr: *mut raw::FuseSetattrX,
    to_set: libc::c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if attr.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let flags = unsafe { (*attr).flags };
    if setattr_mask_has_flags(to_set) {
        state.adapter.file_flags.lock().insert(ino, flags);
    }
    let args = match setattr_x_args_from_callback(attr, to_set) {
        Ok(args) => args,
        Err(errno) => {
            warn!(
                ino,
                to_set, errno, "macFUSE setattr_x callback rejected arguments"
            );
            reply_error(req, errno);
            return;
        }
    };
    debug!(
        ino,
        file_handle = ?setattr_x_file_handle(fi),
        to_set,
        flags,
        "macFUSE setattr_x callback accepted Darwin metadata as overlay"
    );
    if !setattr_args_are_empty(&args) {
        match state.adapter.setattr(setattr_x_file_handle(fi), ino, args) {
            Ok(attr) => {
                let stat = state.adapter.attr_to_stat(&attr);
                if let Some(request) = owned_request(req) {
                    let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
                }
            }
            Err(errno) => {
                warn!(
                    ino,
                    to_set, errno, "macFUSE setattr_x callback returned error"
                );
                reply_error(req, errno);
            }
        }
        return;
    }
    match state.adapter.getattr(ino) {
        Ok(stat) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
            }
        }
        Err(errno) => {
            warn!(
                ino,
                to_set, errno, "macFUSE setattr_x getattr returned error"
            );
            reply_error(req, errno);
        }
    }
}

fn setattr_x_file_handle(fi: *mut raw::FuseFileInfo) -> Option<u64> {
    if fi.is_null() {
        return None;
    }
    // SAFETY: macFUSE owns `fi` for the duration of the callback. Unlike the
    // portable setattr callback, Darwin's setattr_x supplies the open handle
    // without adding FUSE_SET_ATTR_FH to `to_set`.
    Some(unsafe { (*fi).fh })
}

fn setattr_x_args_from_callback(
    attr: *const raw::FuseSetattrX,
    to_set: libc::c_int,
) -> Result<SetattrArgs, i32> {
    // SAFETY: The caller checked `attr` for null and macFUSE passes an
    // initialized Darwin setattr payload for setattr_x callbacks.
    let attr = unsafe { &*attr };
    let mut args = SetattrArgs::default();
    if setattr_mask_has_size(to_set) {
        args.size = Some(u64::try_from(attr.size).map_err(|_| libc::EINVAL)?);
    }
    if setattr_mask_has_mode(to_set) {
        args.mode = Some(attr.mode & 0o7777);
    }
    if setattr_mask_has_uid(to_set) {
        args.uid = Some(attr.uid);
    }
    if setattr_mask_has_gid(to_set) {
        args.gid = Some(attr.gid);
    }
    if setattr_mask_has_atime(to_set) {
        args.atime = TimeSpec::Set(stat_time_to_nanos(
            attr.acctime.tv_sec,
            attr.acctime.tv_nsec,
        )?);
    }
    if setattr_mask_has_mtime(to_set) {
        args.mtime = TimeSpec::Set(stat_time_to_nanos(
            attr.modtime.tv_sec,
            attr.modtime.tv_nsec,
        )?);
    }
    if setattr_mask_has_crtime(to_set) {
        args.crtime = TimeSpec::Set(stat_time_to_nanos(attr.crtime.tv_sec, attr.crtime.tv_nsec)?);
    }
    Ok(args)
}

fn setattr_args_from_callback(attr: *const stat, to_set: libc::c_int) -> Result<SetattrArgs, i32> {
    if setattr_mask_has_unknown(to_set) {
        return Err(libc::ENOTSUP);
    }
    // SAFETY: The caller checked `attr` for null and macFUSE passes an
    // initialized `stat` for setattr callbacks.
    let attr = unsafe { &*attr };
    let mut args = SetattrArgs::default();
    if setattr_mask_has_size(to_set) {
        args.size = Some(u64::try_from(attr.st_size).map_err(|_| libc::EINVAL)?);
    }
    if setattr_mask_has_mode(to_set) {
        args.mode = Some(attr.st_mode & 0o7777);
    }
    if setattr_mask_has_uid(to_set) {
        args.uid = Some(attr.st_uid);
    }
    if setattr_mask_has_gid(to_set) {
        args.gid = Some(attr.st_gid);
    }
    if setattr_mask_has_atime_now(to_set) {
        args.atime = TimeSpec::Now;
    } else if setattr_mask_has_atime(to_set) {
        args.atime = TimeSpec::Set(stat_time_to_nanos(attr.st_atime, attr.st_atime_nsec)?);
    }
    if setattr_mask_has_mtime_now(to_set) {
        args.mtime = TimeSpec::Now;
    } else if setattr_mask_has_mtime(to_set) {
        args.mtime = TimeSpec::Set(stat_time_to_nanos(attr.st_mtime, attr.st_mtime_nsec)?);
    }
    if setattr_mask_has_crtime(to_set) {
        args.crtime = TimeSpec::Set(stat_time_to_nanos(
            attr.st_birthtime,
            attr.st_birthtime_nsec,
        )?);
    }
    Ok(args)
}

fn setattr_args_are_empty(args: &SetattrArgs) -> bool {
    args.size.is_none()
        && args.mode.is_none()
        && args.uid.is_none()
        && args.gid.is_none()
        && args.atime.resolve().is_none()
        && args.mtime.resolve().is_none()
        && args.crtime.resolve().is_none()
        && args.win_attrs.is_none()
}

fn stat_time_to_nanos(seconds: libc::time_t, nanos: libc::c_long) -> Result<i64, i32> {
    if !(0..1_000_000_000).contains(&nanos) {
        return Err(libc::EINVAL);
    }
    let total = i128::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nanos)))
        .ok_or(libc::EOVERFLOW)?;
    i64::try_from(total).map_err(|_| libc::EOVERFLOW)
}

fn setattr_mask_has_size(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined SIZE bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_size_mask() != 0 }
}

fn setattr_mask_has_fh(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim uses the same macFUSE headers and returns either the
    // header-defined FH bit or zero when this FUSE version has no FH bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_fh_mask() != 0 }
}

fn setattr_mask_has_mode(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MODE bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mode_mask() != 0 }
}

fn setattr_mask_has_uid(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined UID bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_uid_mask() != 0 }
}

fn setattr_mask_has_gid(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined GID bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_gid_mask() != 0 }
}

fn setattr_mask_has_atime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined ATIME bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_atime_mask() != 0 }
}

fn setattr_mask_has_mtime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MTIME bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mtime_mask() != 0 }
}

fn setattr_mask_has_atime_now(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined ATIME_NOW bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_atime_now_mask() != 0 }
}

fn setattr_mask_has_mtime_now(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MTIME_NOW bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mtime_now_mask() != 0 }
}

fn setattr_mask_has_crtime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined CRTIME bit, or zero when
    // this FUSE version does not define it.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_crtime_mask() != 0 }
}

fn setattr_mask_has_flags(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined FLAGS bit, or zero when
    // this FUSE version does not define it.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_flags_mask() != 0 }
}

fn setattr_mask_has_unknown(to_set: libc::c_int) -> bool {
    to_set & !setattr_supported_mask() != 0
}

fn setattr_mask_is_accepted_noop(to_set: libc::c_int) -> bool {
    if to_set == 0 {
        return true;
    }
    let ignored = setattr_accepted_noop_mask();
    // SAFETY: The C shim returns the header-defined FH bit, or zero when this
    // FUSE version has no FH bit.
    let fh = unsafe { crate::low_level::studiofs_macfuse_setattr_fh_mask() };
    to_set & ignored != 0 && to_set & !(ignored | fh) == 0
}

fn setattr_accepted_noop_mask() -> libc::c_int {
    // SAFETY: Each C shim returns one header-defined bit for this build, or zero
    // for optional bits not present in the active FUSE headers.
    unsafe {
        crate::low_level::studiofs_macfuse_setattr_chgtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_bkuptime_mask()
            | crate::low_level::studiofs_macfuse_setattr_flags_mask()
    }
}

fn setattr_supported_mask() -> libc::c_int {
    // SAFETY: Each C shim returns one header-defined bit for this build, or zero
    // for optional bits not present in the active FUSE headers.
    unsafe {
        crate::low_level::studiofs_macfuse_setattr_size_mask()
            | crate::low_level::studiofs_macfuse_setattr_fh_mask()
            | crate::low_level::studiofs_macfuse_setattr_mode_mask()
            | crate::low_level::studiofs_macfuse_setattr_uid_mask()
            | crate::low_level::studiofs_macfuse_setattr_gid_mask()
            | crate::low_level::studiofs_macfuse_setattr_atime_mask()
            | crate::low_level::studiofs_macfuse_setattr_mtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_atime_now_mask()
            | crate::low_level::studiofs_macfuse_setattr_mtime_now_mask()
            | crate::low_level::studiofs_macfuse_setattr_crtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_chgtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_bkuptime_mask()
            | crate::low_level::studiofs_macfuse_setattr_flags_mask()
    }
}

/// macFUSE release callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_release(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for release.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.release(ino, file_handle) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                ino,
                file_handle, errno, "macFUSE release callback returned error"
            );
            reply_error(req, errno);
        }
    }
}

/// macFUSE opendir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_opendir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    match state.adapter.opendir(ino) {
        Ok(file_handle) => {
            let Some(request) = owned_request(req) else {
                let _ = state.adapter.releasedir(file_handle);
                return;
            };
            if let Err(error) = Reply::new(request).open(OpenReply::new(file_handle)) {
                warn!(
                    ino,
                    file_handle,
                    ?error,
                    "macFUSE opendir reply was rejected"
                );
                let _ = state.adapter.releasedir(file_handle);
            }
        }
        Err(errno) => reply_callback_error(req, "opendir", errno),
    }
}

/// macFUSE readdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_readdir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for readdir.
    let file_handle = unsafe { (*fi).fh };
    let Some(request) = owned_request(req) else {
        return;
    };
    match state
        .adapter
        .readdir_with_handle(&request, ino, file_handle, off, size)
    {
        Ok(entries) => {
            let _ = Reply::new(request).directory_entries(&entries);
        }
        Err(errno) => {
            log_callback_error("readdir", errno);
            let _ = Reply::new(request).error(errno);
        }
    }
}

/// macFUSE releasedir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_releasedir(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for releasedir.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.releasedir(file_handle) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_callback_error(req, "releasedir", errno),
    }
}

/// macFUSE readlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_readlink(req: *mut raw::FuseReq, ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.readlink(ino) {
        Ok(target) => match CString::new(target) {
            Ok(target) => {
                if let Some(request) = owned_request(req) {
                    let _ = Reply::new(request).readlink(&target);
                }
            }
            Err(_) => reply_error(req, libc::EINVAL),
        },
        Err(errno) => reply_callback_error(req, "readlink", errno),
    }
}

/// macFUSE statfs callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_statfs(req: *mut raw::FuseReq, _ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.statfs() {
        Ok(stats) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).statfs(&stats);
            }
        }
        Err(errno) => reply_callback_error(req, "statfs", errno),
    }
}

/// macFUSE setxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_setxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    value: *const c_char,
    size: size_t,
    flags: libc::c_int,
    position: u32,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(value) = (unsafe { xattr_value_from_callback(req, value, size) }) else {
        return;
    };
    if position != 0 {
        reply_error(req, libc::ENOTSUP);
        return;
    }
    let flags = match u32::try_from(flags) {
        Ok(flags) => flags,
        Err(_) => {
            reply_error(req, libc::EINVAL);
            return;
        }
    };
    match state.adapter.set_xattr(ino, name, value, flags) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_callback_error(req, "setxattr", errno),
    }
}

/// macFUSE getxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_getxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    size: size_t,
    position: u32,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    let position = position as usize;
    if size == 0 {
        match state.adapter.get_xattr_size(ino, name) {
            Ok(Some(value_size)) => {
                reply_xattr_size(req, xattr_size_from_position(value_size, position))
            }
            Ok(None) => reply_callback_error(req, "getxattr", xattr_not_found_errno()),
            Err(errno) => reply_callback_error(req, "getxattr", errno),
        }
        return;
    }
    match state.adapter.get_xattr(ino, name) {
        Ok(Some(value)) => {
            reply_xattr_value(req, size, xattr_value_from_position(&value, position))
        }
        Ok(None) => reply_callback_error(req, "getxattr", xattr_not_found_errno()),
        Err(errno) => reply_callback_error(req, "getxattr", errno),
    }
}

/// macFUSE listxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_listxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.list_xattrs(ino) {
        Ok(names) if size == 0 => reply_xattr_size(req, xattr_name_list_len(&names)),
        Ok(names) => reply_xattr_value(req, size, &format_xattr_name_list(&names)),
        Err(errno) => reply_callback_error(req, "listxattr", errno),
    }
}

/// macFUSE removexattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_removexattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    match state.adapter.remove_xattr(ino, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_callback_error(req, "removexattr", errno),
    }
}

unsafe fn xattr_name_from_callback<'a>(
    req: *mut raw::FuseReq,
    name: *const c_char,
) -> Option<&'a [u8]> {
    // SAFETY CONTRACT: the returned slice is borrowed from callback-owned
    // macFUSE memory and must not escape the current callback invocation.
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return None;
    }
    Some(unsafe { CStr::from_ptr(name) }.to_bytes())
}

unsafe fn xattr_value_from_callback<'a>(
    req: *mut raw::FuseReq,
    value: *const c_char,
    size: size_t,
) -> Option<&'a [u8]> {
    // SAFETY CONTRACT: the returned slice is borrowed from callback-owned
    // macFUSE memory and must not escape the current callback invocation.
    if size == 0 {
        return Some(&[]);
    }
    if value.is_null() {
        reply_error(req, libc::EINVAL);
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(value.cast::<u8>(), size) })
}

fn reply_xattr_size(req: *mut raw::FuseReq, value_len: usize) {
    let Some(request) = owned_request(req) else {
        return;
    };
    let _ = Reply::new(request).xattr_size(value_len);
}

fn xattr_size_from_position(value_len: usize, position: usize) -> usize {
    value_len.saturating_sub(position)
}

fn xattr_value_from_position(value: &[u8], position: usize) -> &[u8] {
    value.get(position..).unwrap_or(&[])
}

fn reply_xattr_value(req: *mut raw::FuseReq, size: size_t, value: &[u8]) {
    let Some(request) = owned_request(req) else {
        return;
    };
    if size < value.len() {
        let _ = Reply::new(request).error(libc::ERANGE);
    } else {
        let _ = Reply::new(request).read_buffer(value);
    }
}

fn format_xattr_name_list(names: &[Vec<u8>]) -> Vec<u8> {
    let total_len = xattr_name_list_len(names);
    let mut out = Vec::with_capacity(total_len);
    for name in names {
        out.extend_from_slice(name);
        out.push(0);
    }
    out
}

fn xattr_name_list_len(names: &[Vec<u8>]) -> usize {
    names.iter().map(|name| name.len() + 1).sum()
}

fn xattr_not_found_errno() -> i32 {
    libc::ENOATTR
}

fn is_missing_xattr_error(err: &studiofs_vfs::VfsError) -> bool {
    matches!(
        err,
        studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    )
}

fn xattr_mutation_err_to_errno(err: &studiofs_vfs::VfsError) -> i32 {
    if is_missing_xattr_error(err) {
        xattr_not_found_errno()
    } else {
        vfs_err_to_errno(err)
    }
}

fn macfuse_set_xattr_flags(name: &[u8], flags: u32) -> u32 {
    if name == MACOS_PROVENANCE_XATTR {
        flags & !XATTR_CREATE_FLAG
    } else {
        flags
    }
}

async fn acquire_read_admission<'a>(
    admission: &'a Semaphore,
    size: u32,
    diagnostics: &AsyncReadDiagnostics,
    cancellation: &ReadCancellationToken,
) -> Result<SemaphorePermit<'a>, i32> {
    match admission.try_acquire_many(size) {
        Ok(permit) => return Ok(permit),
        Err(tokio::sync::TryAcquireError::Closed) => return Err(libc::EIO),
        Err(tokio::sync::TryAcquireError::NoPermits) => {}
    }

    let admission_started = Instant::now();
    tokio::select! {
        permit = admission.acquire_many(size) => {
            let permit = permit.map_err(|_| libc::EIO)?;
            diagnostics.on_admitted(admission_started.elapsed());
            Ok(permit)
        }
        () = cancellation.cancelled() => Err(MACFUSE_READ_INTERRUPTED_ERRNO),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use studiofs_meta::engine::XATTR_REPLACE_FLAG;
    use studiofs_vfs::contract_harness::ContractVfsFixture;

    #[test]
    fn reply_task_tracker_waits_until_async_reply_finishes() {
        let tracker = Arc::new(ReplyTaskTracker::default());
        let task = tracker.track();
        let waiting_tracker = Arc::clone(&tracker);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            waiting_tracker.wait_for_idle();
            done_tx.send(()).expect("signal drained reply task");
        });

        assert!(
            done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "reply task drain must wait while a callback still owns its request"
        );
        drop(task);
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("reply task drain should finish after the callback exits");
        waiter.join().expect("reply drain waiter should exit");
    }

    #[test]
    fn read_buffer_pool_keeps_recycled_initialized_prefix_len() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = Arc::clone(&pool).acquire(16);
        buffer.truncate(5);

        drop(buffer);

        assert_eq!(pool.available_count(), 1);
        assert_eq!(pool.last_available_len(), Some(5));
        assert!(
            pool.last_available_capacity()
                .expect("recycled buffer should retain capacity")
                >= 16
        );
    }

    #[test]
    fn queued_read_execution_size_stays_within_reusable_pool_capacity() {
        let oversized_queue_limit = u32::MAX;
        assert_eq!(
            read_execution_size(u32::MAX, oversized_queue_limit),
            MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32
        );
    }

    #[test]
    fn async_read_diagnostic_counters_are_cache_line_aligned() {
        assert!(std::mem::align_of::<AsyncReadDiagnostics>() >= 128);
        assert_eq!(std::mem::align_of::<CachePaddedAtomicUsize>(), 128);
        assert_eq!(std::mem::size_of::<CachePaddedAtomicUsize>(), 128);
        assert_eq!(std::mem::align_of::<CachePaddedAtomicU64>(), 128);
        assert_eq!(std::mem::size_of::<CachePaddedAtomicU64>(), 128);
    }

    #[test]
    fn adapter_hot_path_state_uses_one_shared_arc() {
        let fixture =
            ContractVfsFixture::new("macfuse-hot-path-shared-state").expect("create VFS fixture");
        let adapter = StudioVfsAdapter::new(fixture.vfs(), fixture.runtime().handle().clone());

        assert_eq!(adapter.hot_path_shared_state_arc_count_for_test(), 1);
    }

    #[test]
    fn adapter_hot_path_state_count_reflects_live_shared_clones() {
        let fixture = ContractVfsFixture::new("macfuse-hot-path-shared-state-clone")
            .expect("create VFS fixture");
        let adapter = StudioVfsAdapter::new(fixture.vfs(), fixture.runtime().handle().clone());
        let _shared = Arc::clone(&adapter.shared);

        assert_eq!(adapter.hot_path_shared_state_arc_count_for_test(), 2);
    }

    #[derive(Debug)]
    struct NoopKernelInvalidator;

    impl KernelInvalidator for NoopKernelInvalidator {
        fn inval_inode(&self, _ino: u64) {}

        fn inval_inode_attrs(&self, _ino: u64) {}
    }

    #[derive(Debug, Default)]
    struct EntryRecordingKernelInvalidator {
        entries: ParkingMutex<Vec<(u64, Vec<u8>)>>,
    }

    impl KernelInvalidator for EntryRecordingKernelInvalidator {
        fn inval_inode(&self, _ino: u64) {}

        fn inval_inode_attrs(&self, _ino: u64) {}

        fn inval_entry(&self, parent: u64, name: &[u8]) {
            self.entries.lock().push((parent, name.to_vec()));
        }
    }

    #[test]
    fn entry_invalidation_reaches_kernel_after_parent_lookup_is_forgotten() {
        let inner = Arc::new(EntryRecordingKernelInvalidator::default());
        let invalidator = LiveKernelInvalidator {
            kernel_inode_liveness: Arc::new(KernelInodeLiveness::default()),
            inner: Arc::clone(&inner) as Arc<dyn KernelInvalidator>,
        };

        invalidator.inval_entry(42, b"recover.bin");

        assert_eq!(*inner.entries.lock(), vec![(42, b"recover.bin".to_vec())]);
    }

    #[test]
    fn kernel_invalidator_does_not_retain_entire_adapter_shared_state() {
        let fixture = ContractVfsFixture::new("macfuse-kernel-invalidator-narrow-state")
            .expect("create VFS fixture");
        let adapter = StudioVfsAdapter::new(fixture.vfs(), fixture.runtime().handle().clone());
        let _invalidator = adapter.kernel_invalidator_for_test(Arc::new(NoopKernelInvalidator));

        assert_eq!(adapter.hot_path_shared_state_arc_count_for_test(), 1);
    }

    #[test]
    fn kernel_inode_liveness_closes_only_after_all_lookup_references_are_forgotten() {
        let liveness = KernelInodeLiveness::default();
        let ino = 42;
        liveness.observe(ino);
        liveness.observe(ino);

        liveness.forget(ino, 1);
        assert!(
            liveness.contains(ino),
            "one remaining lookup reference must keep notification live"
        );

        liveness.forget(ino, 1);
        assert!(
            !liveness.contains(ino),
            "the final forget must close the notification gate"
        );
    }

    #[test]
    fn kernel_inode_liveness_close_does_not_wait_for_external_notification() {
        let liveness = Arc::new(KernelInodeLiveness::default());
        let ino = 42;
        liveness.observe(ino);
        let entered_notification = Arc::new(std::sync::Barrier::new(2));
        let release_notification = Arc::new(std::sync::Barrier::new(2));
        let notifier_liveness = Arc::clone(&liveness);
        let notifier_entered = Arc::clone(&entered_notification);
        let notifier_release = Arc::clone(&release_notification);
        let notifier = std::thread::spawn(move || {
            notifier_liveness.notify_if_observed(ino, || {
                notifier_entered.wait();
                notifier_release.wait();
            });
        });
        entered_notification.wait();

        let closer_liveness = Arc::clone(&liveness);
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let closer = std::thread::spawn(move || {
            closer_liveness.close(ino);
            closed_tx.send(()).expect("signal closed gate");
        });
        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("final unlink must not wait for an external notification callback");

        release_notification.wait();
        notifier.join().expect("notification thread exits");
        closer.join().expect("close thread exits");
        assert!(!liveness.contains(ino));
    }

    #[test]
    fn kernel_inode_liveness_notification_can_reenter_close() {
        let liveness = Arc::new(KernelInodeLiveness::default());
        let ino = 42;
        liveness.observe(ino);
        let callback_liveness = Arc::clone(&liveness);
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let notifier = std::thread::spawn(move || {
            callback_liveness.notify_if_observed(ino, || {
                callback_liveness.close(ino);
                closed_tx.send(()).expect("signal re-entrant close");
            });
        });

        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("notification callback must not deadlock when close re-enters liveness");
        notifier.join().expect("notification thread exits");
        assert!(!liveness.contains(ino));
    }

    #[test]
    fn kernel_inode_liveness_notification_does_not_block_unrelated_inode_updates() {
        let liveness = Arc::new(KernelInodeLiveness::default());
        let notifying_ino = 42;
        liveness.observe(notifying_ino);
        let entered_notification = Arc::new(std::sync::Barrier::new(2));
        let release_notification = Arc::new(std::sync::Barrier::new(2));
        let notifier_liveness = Arc::clone(&liveness);
        let notifier_entered = Arc::clone(&entered_notification);
        let notifier_release = Arc::clone(&release_notification);
        let notifier = std::thread::spawn(move || {
            notifier_liveness.notify_if_observed(notifying_ino, || {
                notifier_entered.wait();
                notifier_release.wait();
            });
        });
        entered_notification.wait();

        let updater_liveness = Arc::clone(&liveness);
        let (updated_tx, updated_rx) = std::sync::mpsc::channel();
        let updater = std::thread::spawn(move || {
            for ino in 1_000..5_096 {
                updater_liveness.ensure_observed(ino);
            }
            updated_tx.send(()).expect("signal unrelated updates");
        });
        let unrelated_updates_completed = updated_rx.recv_timeout(Duration::from_secs(1)).is_ok();

        release_notification.wait();
        notifier.join().expect("notification thread exits");
        updater.join().expect("unrelated inode updater exits");
        assert!(
            unrelated_updates_completed,
            "kernel IPC for one inode must not retain a DashMap shard guard and block unrelated inode updates"
        );
    }

    #[test]
    fn setattr_mask_accepts_bundled_size_changes_and_gates_file_handle() {
        // SAFETY: The C shim returns the platform header value for this build.
        let size_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_size_mask() };
        // SAFETY: The C shim returns the platform header value for this build.
        let fh_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_fh_mask() };
        // SAFETY: The C shim returns the platform header value for this build.
        let mode_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_mode_mask() };

        assert!(setattr_mask_has_size(size_mask));
        assert!(setattr_mask_has_size(size_mask | mode_mask));
        assert!(!setattr_mask_has_size(0));
        assert!(!setattr_mask_has_fh(size_mask));
        assert_eq!(setattr_mask_has_fh(size_mask | fh_mask), fh_mask != 0);
    }

    #[test]
    fn setattr_x_uses_open_handle_for_size_change_without_fh_mask() {
        let mut fi = raw::FuseFileInfo::for_test(libc::O_WRONLY, 67, false, false);

        assert_eq!(setattr_x_file_handle(&mut fi), Some(67));
        assert_eq!(setattr_x_file_handle(std::ptr::null_mut()), None);
    }

    #[test]
    fn setattr_args_preserve_bundled_size_mode_and_mtime() {
        // SAFETY: The C shim returns platform header values for this build.
        let mask = unsafe {
            crate::low_level::studiofs_macfuse_setattr_size_mask()
                | crate::low_level::studiofs_macfuse_setattr_mode_mask()
                | crate::low_level::studiofs_macfuse_setattr_mtime_mask()
        };
        let mut attr = minimal_stat(0);
        attr.st_size = 42;
        attr.st_mode = libc::S_IFREG | 0o640;
        attr.st_mtime = 123;
        attr.st_mtime_nsec = 456;

        let args = setattr_args_from_callback(&attr, mask).expect("build setattr args");

        assert_eq!(args.size, Some(42));
        assert_eq!(args.mode, Some(0o640));
        assert!(matches!(args.mtime, TimeSpec::Set(123_000_000_456)));
    }

    #[test]
    fn setattr_supported_mask_accepts_macos_metadata_bits() {
        // SAFETY: The C shim returns platform header values for this build, or
        // zero for optional bits absent from the active FUSE headers.
        let mask = unsafe {
            crate::low_level::studiofs_macfuse_setattr_size_mask()
                | crate::low_level::studiofs_macfuse_setattr_chgtime_mask()
                | crate::low_level::studiofs_macfuse_setattr_bkuptime_mask()
                | crate::low_level::studiofs_macfuse_setattr_flags_mask()
        };

        assert!(!setattr_mask_has_unknown(mask));
    }

    #[test]
    fn setattr_macos_metadata_bits_are_accepted_noops() {
        // SAFETY: The C shim returns platform header values for this build, or
        // zero for optional bits absent from the active FUSE headers.
        let (flags, chgtime, bkuptime, fh) = unsafe {
            (
                crate::low_level::studiofs_macfuse_setattr_flags_mask(),
                crate::low_level::studiofs_macfuse_setattr_chgtime_mask(),
                crate::low_level::studiofs_macfuse_setattr_bkuptime_mask(),
                crate::low_level::studiofs_macfuse_setattr_fh_mask(),
            )
        };
        let ignored = flags | chgtime | bkuptime;

        assert!(
            setattr_mask_is_accepted_noop(0),
            "macFUSE can report chflags with an empty setattr mask"
        );
        assert_eq!(
            setattr_mask_is_accepted_noop(ignored),
            ignored != 0,
            "at least one optional no-op mask should be accepted when present"
        );
        if flags != 0 {
            assert!(setattr_mask_is_accepted_noop(flags | fh));
        }
        if fh != 0 {
            assert!(!setattr_mask_is_accepted_noop(fh));
        }
    }

    #[test]
    fn callback_error_log_level_ignores_success_and_expected_macos_probes() {
        assert_eq!(callback_error_log_level("write", 0), None);
        assert_eq!(
            callback_error_log_level("getxattr", xattr_not_found_errno()),
            None
        );
        assert_eq!(callback_error_log_level("lookup", libc::ENOENT), None);
        assert_eq!(callback_error_log_level("removexattr", libc::ENOENT), None);
        assert_eq!(
            callback_error_log_level("removexattr", xattr_not_found_errno()),
            None
        );
        assert_eq!(callback_error_log_level("getattr", libc::ENOENT), None);
        assert_eq!(
            callback_error_log_level("read", MACFUSE_READ_INTERRUPTED_ERRNO),
            None
        );
    }

    #[test]
    fn callback_error_log_level_warns_for_write_and_metadata_errors() {
        assert_eq!(
            callback_error_log_level("write", libc::ENOENT),
            Some(CallbackErrorLogLevel::Warn)
        );
        assert_eq!(
            callback_error_log_level("lookup", libc::EIO),
            Some(CallbackErrorLogLevel::Warn)
        );
        assert_eq!(
            callback_error_log_level("getxattr", libc::EIO),
            Some(CallbackErrorLogLevel::Warn)
        );
    }

    #[test]
    fn provenance_xattr_create_flag_is_idempotent_for_macos_copyfile() {
        let flags = XATTR_CREATE_FLAG | XATTR_REPLACE_FLAG;

        assert_eq!(
            macfuse_set_xattr_flags(MACOS_PROVENANCE_XATTR, flags),
            XATTR_REPLACE_FLAG
        );
        assert_eq!(
            macfuse_set_xattr_flags(b"com.apple.FinderInfo", flags),
            flags
        );
    }

    #[test]
    fn missing_xattr_mutation_maps_to_macos_xattr_not_found_errno() {
        let error = studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryNotFound {
            parent: 42,
            name: "com.apple.FinderInfo".into(),
        });

        assert_eq!(xattr_mutation_err_to_errno(&error), xattr_not_found_errno());
    }

    #[test]
    fn non_xattr_not_found_still_maps_to_no_entry() {
        let error = studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::InodeNotFound(42));

        assert_eq!(xattr_mutation_err_to_errno(&error), libc::ENOENT);
    }

    #[test]
    fn xattr_name_list_len_counts_nul_separated_wire_bytes() {
        let names = vec![b"user.alpha".to_vec(), b"com.apple.ResourceFork".to_vec()];

        assert_eq!(
            xattr_name_list_len(&names),
            "user.alpha\0com.apple.ResourceFork\0".len()
        );
        assert_eq!(
            format_xattr_name_list(&names),
            b"user.alpha\0com.apple.ResourceFork\0"
        );
    }

    #[test]
    fn xattr_position_offsets_size_probes_and_value_reads() {
        let value = b"resource-fork-payload";

        assert_eq!(xattr_size_from_position(value.len(), 9), 12);
        assert_eq!(xattr_size_from_position(value.len(), value.len() + 8), 0);
        assert_eq!(xattr_value_from_position(value, 9), b"fork-payload");
        assert_eq!(xattr_value_from_position(value, value.len() + 8), b"");
    }
}
