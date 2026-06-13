use std::collections::{HashMap, HashSet, hash_map::Entry as HashMapEntry};
use std::ffi::{CStr, CString, OsStr};
use std::mem::{self, MaybeUninit};
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use lru::LruCache;
use parking_lot::Mutex as ParkingMutex;
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::engine::XATTR_CREATE_FLAG;
use studiofs_meta::schema::ResolvedSliceSource;
use studiofs_meta::{DirEntry, FileType, InodeAttr, SetattrArgs, TimeSpec, UploadState};
use studiofs_vfs::{
    KernelInvalidator, LocalSyncPolicy, StatFs, StudioVfs, VfsFuseAsyncReadSnapshot,
    VfsMacFuseOpenCacheDecision,
};
use tokio::runtime::Handle;
use tokio::sync::{Semaphore, SemaphorePermit};
#[cfg(test)]
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::low_level::{DirectoryEntryBuffer, Entry, OpenReply, Reply, Request, raw};

const TTL_SECONDS: f64 = 1.0;
const MACFUSE_ATTR_BLOCK_SIZE: u32 = crate::low_level::MACOS_FUSE_IOSIZE_BYTES;
const READDIR_CACHE_TTL: Duration = Duration::from_secs(1);
const READDIR_CACHE_PRUNE_THRESHOLD: usize = 1024;
const READDIR_CACHE_PRUNE_INTERVAL: Duration = Duration::from_secs(1);
const MACFUSE_READ_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS: usize = 32;
const MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY: usize = 8 * 1024 * 1024;
// Keep allocator-rounded buffers that grew slightly beyond the reusable read
// cap, while dropping genuinely oversized requests instead of pinning them.
const MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY: usize = MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY;
const MACFUSE_READ_WINDOW_MAX_BYTES: usize = 8 * 1024 * 1024;
const MACFUSE_READ_WINDOW_MIN_BYTES: usize = 128 * 1024;
const MACFUSE_ASYNC_READ_SNAPSHOT_INTERVAL: Duration = Duration::from_millis(500);
const MACFUSE_READ_INTERRUPTED_ERRNO: i32 = libc::EINTR;
const MACOS_PROVENANCE_XATTR: &[u8] = b"com.apple.provenance";
const MACOS_DS_STORE_NAME: &[u8] = b".DS_Store";
const MACOS_UF_HIDDEN: u32 = 0x0000_8000;
const WRITE_CAPABLE_STATE_SHARDS: usize = 32;
#[cfg(not(test))]
const MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT: usize = 8192;
#[cfg(test)]
const MACFUSE_OPEN_CACHE_SNAPSHOT_LIMIT: usize = 8;

/// macFUSE open-cache flags chosen for one file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MacFuseOpenCachePolicy {
    /// Whether this handle should bypass kernel page cache.
    pub(crate) direct_io: bool,
    /// Whether macFUSE should keep existing cached pages for this handle.
    pub(crate) keep_cache: bool,
}

/// Operation adapter shared by macFUSE callbacks and fast tests.
pub(crate) struct StudioVfsAdapter {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    readdir_cache: Mutex<HashMap<u64, ReaddirCacheEntry>>,
    readdir_cache_last_pruned: Mutex<Option<Instant>>,
    readdir_cache_ready: Condvar,
    shared: Arc<AdapterSharedState>,
    write_capable_state: WriteCapableState,
    file_flags: ParkingMutex<HashMap<u64, u32>>,
    read_queue_byte_limit: u32,
}

#[derive(Debug)]
struct AdapterSharedState {
    open_cache_snapshots: Arc<ParkingMutex<LruCache<u64, MacFuseOpenCacheSnapshot>>>,
    read_admission: Semaphore,
    read_buffers: Arc<ReadBufferPool>,
    read_windows: Arc<ReadWindowStore>,
    read_diagnostics: AsyncReadDiagnostics,
}

#[derive(Debug)]
struct WriteCapableState {
    handles: Box<[ParkingMutex<HashMap<u64, u64>>]>,
    handle_counts: Box<[ParkingMutex<HashMap<u64, usize>>]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReleasedWriteCapableHandle {
    ino: u64,
    last_for_inode: bool,
}

impl Default for WriteCapableState {
    fn default() -> Self {
        Self {
            handles: new_write_capable_shards(),
            handle_counts: new_write_capable_shards(),
        }
    }
}

impl WriteCapableState {
    fn record_open(&self, ino: u64, file_handle: u64) {
        self.increment_count(ino);
        let previous_ino = {
            let mut handles = self.handle_shard(file_handle).lock();
            handles.insert(file_handle, ino)
        };
        if let Some(previous_ino) = previous_ino {
            self.decrement_count(previous_ino);
        }
    }

    fn release_handle(&self, file_handle: u64) -> Option<ReleasedWriteCapableHandle> {
        let write_ino = {
            let mut handles = self.handle_shard(file_handle).lock();
            handles.remove(&file_handle)?
        };
        let last_for_inode = self.decrement_count(write_ino);
        Some(ReleasedWriteCapableHandle {
            ino: write_ino,
            last_for_inode,
        })
    }

    fn active_count(&self, ino: u64) -> usize {
        self.count_shard(ino).lock().get(&ino).copied().unwrap_or(0)
    }

    fn increment_count(&self, ino: u64) {
        *self.count_shard(ino).lock().entry(ino).or_default() += 1;
    }

    fn decrement_count(&self, ino: u64) -> bool {
        match self.count_shard(ino).lock().entry(ino) {
            HashMapEntry::Occupied(mut entry) => {
                let count = entry.get_mut();
                *count = count.saturating_sub(1);
                if *count == 0 {
                    entry.remove();
                    true
                } else {
                    false
                }
            }
            HashMapEntry::Vacant(_) => true,
        }
    }

    fn handle_shard(&self, file_handle: u64) -> &ParkingMutex<HashMap<u64, u64>> {
        &self.handles[write_capable_shard_index(file_handle)]
    }

    fn count_shard(&self, ino: u64) -> &ParkingMutex<HashMap<u64, usize>> {
        &self.handle_counts[write_capable_shard_index(ino)]
    }
}

fn new_write_capable_shards<T>() -> Box<[ParkingMutex<HashMap<u64, T>>]> {
    (0..WRITE_CAPABLE_STATE_SHARDS)
        .map(|_| ParkingMutex::new(HashMap::new()))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

fn write_capable_shard_index(key: u64) -> usize {
    key.wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % WRITE_CAPABLE_STATE_SHARDS
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MacFuseOpenCacheSnapshot {
    content_epoch: u64,
    size: u64,
    kind: MacFuseOpenCacheSnapshotKind,
    graph: Vec<MacFuseOpenCacheGraphSpan>,
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacFuseOpenCacheSnapshotKind {
    /// The generation is fully committed and visible through remote metadata.
    RemoteReady,
    /// The generation is locally stable for this mount but not remotely published yet.
    LocalStableUnpublished,
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MacFuseOpenCachePayload {
    object_id: u64,
    offset: u32,
    length: u32,
    flags: u8,
}

fn open_cache_source_tag(source: ResolvedSliceSource) -> u8 {
    match source {
        ResolvedSliceSource::Overlay => 1,
        ResolvedSliceSource::Base => 2,
        ResolvedSliceSource::OverlayTombstone => 3,
        ResolvedSliceSource::BaseTombstone => 4,
        ResolvedSliceSource::Hole => 5,
    }
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

#[derive(Debug, Default)]
struct ReadWindowStore {
    windows: DashMap<u64, ReadWindow>,
    handles_by_inode: DashMap<u64, HashSet<u64>>,
    // Keep invalidated inode generations for the mount lifetime: dropping them
    // while async reads are still in flight can make an old fill look current.
    invalidation_generations: DashMap<u64, AtomicU64>,
}

impl ReadWindowStore {
    fn try_read(&self, file_handle: u64, ino: u64, offset: u64, out: &mut [u8]) -> Option<usize> {
        if out.is_empty() {
            return None;
        }
        let generation = self.invalidation_generation(ino);
        let window = self.windows.get(&file_handle)?;
        if window.ino != ino || !window.contains(offset, out.len()) {
            return None;
        }
        let copied = copy_window_bytes(window.offset, window.bytes.as_ref(), offset, out);
        if copied == 0 || self.invalidation_generation(ino) != generation {
            return None;
        }
        window
            .next_offset
            .store(offset.saturating_add(copied as u64), Ordering::Release);
        Some(copied)
    }

    fn remember_if_current(
        &self,
        file_handle: u64,
        ino: u64,
        offset: u64,
        next_offset: u64,
        bytes: PooledReadBuffer,
        generation: u64,
    ) -> bool {
        if self.invalidation_generation(ino) != generation {
            return false;
        }
        if bytes.is_empty() {
            return false;
        }
        let mut needs_insert = true;
        if let Some(previous) = self.windows.insert(
            file_handle,
            ReadWindow {
                ino,
                offset,
                next_offset: AtomicU64::new(next_offset),
                bytes,
            },
        ) {
            if previous.ino != ino {
                self.remove_inode_handle(previous.ino, file_handle);
            } else {
                needs_insert = false;
            }
        }
        if self.invalidation_generation(ino) != generation {
            self.invalidate_handle(file_handle);
            return false;
        }
        if needs_insert {
            self.handles_by_inode
                .entry(ino)
                .or_default()
                .insert(file_handle);
        }
        true
    }

    fn invalidate_handle(&self, file_handle: u64) {
        if let Some((_, window)) = self.windows.remove(&file_handle) {
            self.remove_inode_handle(window.ino, file_handle);
        }
    }

    fn invalidate_inode(&self, ino: u64) {
        self.bump_inode_generation(ino);
        let Some((_, handles)) = self.handles_by_inode.remove(&ino) else {
            return;
        };
        for file_handle in handles {
            self.windows
                .remove_if(&file_handle, |_, window| window.ino == ino);
        }
    }

    fn remove_inode_handle(&self, ino: u64, file_handle: u64) {
        let Some(mut handles) = self.handles_by_inode.get_mut(&ino) else {
            return;
        };
        handles.remove(&file_handle);
        if handles.is_empty() {
            drop(handles);
            self.handles_by_inode
                .remove_if(&ino, |_, handles| handles.is_empty());
        }
    }

    fn invalidation_generation(&self, ino: u64) -> u64 {
        self.invalidation_generations
            .get(&ino)
            .map_or(0, |generation| generation.load(Ordering::Acquire))
    }

    fn bump_inode_generation(&self, ino: u64) {
        self.invalidation_generations
            .entry(ino)
            .or_default()
            .fetch_add(1, Ordering::AcqRel);
    }
}

struct ReadWindowKernelInvalidator {
    read_windows: Arc<ReadWindowStore>,
    open_cache_snapshots: Arc<ParkingMutex<LruCache<u64, MacFuseOpenCacheSnapshot>>>,
    inner: Arc<dyn KernelInvalidator>,
}

impl KernelInvalidator for ReadWindowKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.read_windows.invalidate_inode(ino);
        self.open_cache_snapshots.lock().pop(&ino);
        self.inner.inval_inode(ino);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.inner.inval_inode_attrs(ino);
    }
}

#[derive(Debug)]
struct ReadWindow {
    ino: u64,
    offset: u64,
    next_offset: AtomicU64,
    bytes: PooledReadBuffer,
}

impl ReadWindow {
    fn contains(&self, offset: u64, len: usize) -> bool {
        // Read windows are sequential readahead only; random-access reads should
        // fall back to VFS instead of advancing this per-handle window.
        if len == 0 || offset != self.next_offset.load(Ordering::Acquire) {
            return false;
        }
        let Some(relative) = offset.checked_sub(self.offset) else {
            return false;
        };
        relative < self.bytes.len() as u64
    }
}

fn copy_window_bytes(window_offset: u64, bytes: &[u8], offset: u64, out: &mut [u8]) -> usize {
    let relative = (offset - window_offset) as usize;
    let available = bytes.len().saturating_sub(relative);
    let copy_len = available.min(out.len());
    let end = relative + copy_len;
    out[..copy_len].copy_from_slice(&bytes[relative..end]);
    copy_len
}

#[derive(Clone)]
struct ReadExecutionState {
    vfs: Arc<StudioVfs>,
    shared: Arc<AdapterSharedState>,
}

#[derive(Debug, Clone, Copy)]
struct ReadExecutionRequest {
    ino: u64,
    file_handle: u64,
    offset: u64,
    size: u32,
    max_read_len: u32,
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

fn readahead_window_len(size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    size.saturating_mul(8)
        .clamp(MACFUSE_READ_WINDOW_MIN_BYTES, MACFUSE_READ_WINDOW_MAX_BYTES)
}

fn queued_read_admission_bytes(size: u32, read_queue_byte_limit: u32) -> u32 {
    readahead_window_len(size as usize).min(read_queue_byte_limit as usize) as u32
}

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
    pending_reads: AtomicUsize,
    pending_bytes: AtomicUsize,
    peak_pending_reads: AtomicUsize,
    peak_pending_bytes: AtomicUsize,
    completed_reads: AtomicU64,
    failed_reads: AtomicU64,
    canceled_reads: AtomicU64,
    admission_waits: AtomicU64,
    admission_wait_total_ns: AtomicU64,
    last_snapshot_publish_ns: AtomicU64,
}

impl AsyncReadDiagnostics {
    fn new(max_pending_bytes: usize) -> Self {
        Self {
            started_at: Instant::now(),
            max_pending_bytes,
            pending_reads: AtomicUsize::new(0),
            pending_bytes: AtomicUsize::new(0),
            peak_pending_reads: AtomicUsize::new(0),
            peak_pending_bytes: AtomicUsize::new(0),
            completed_reads: AtomicU64::new(0),
            failed_reads: AtomicU64::new(0),
            canceled_reads: AtomicU64::new(0),
            admission_waits: AtomicU64::new(0),
            admission_wait_total_ns: AtomicU64::new(0),
            last_snapshot_publish_ns: AtomicU64::new(0),
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
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            readdir_cache: Mutex::new(HashMap::new()),
            readdir_cache_last_pruned: Mutex::new(None),
            readdir_cache_ready: Condvar::new(),
            shared: Arc::new(AdapterSharedState {
                open_cache_snapshots: Arc::new(ParkingMutex::new(LruCache::new(
                    open_cache_snapshot_capacity(),
                ))),
                read_admission: Semaphore::new(read_queue_byte_limit as usize),
                read_buffers: Arc::new(ReadBufferPool::default()),
                read_windows: Arc::new(ReadWindowStore::default()),
                read_diagnostics: AsyncReadDiagnostics::new(read_queue_byte_limit as usize),
            }),
            write_capable_state: WriteCapableState::default(),
            file_flags: ParkingMutex::new(HashMap::new()),
            read_queue_byte_limit,
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
        Arc::new(ReadWindowKernelInvalidator {
            read_windows: Arc::clone(&self.shared.read_windows),
            open_cache_snapshots: Arc::clone(&self.shared.open_cache_snapshots),
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
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get inode attributes.
    pub(crate) fn getattr(&self, ino: u64) -> Result<stat, i32> {
        self.vfs
            .getattr(ino)
            .map(|attr| self.attr_to_stat(&attr))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create a directory.
    pub(crate) fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .mkdir(parent, name, self.mount_uid, self.mount_gid, mode & 0o7777)
            .inspect(|_| self.invalidate_readdir_cache(parent))
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
        self.vfs
            .symlink(parent, name, target, self.mount_uid, self.mount_gid)
            .inspect(|_| self.invalidate_readdir_cache(parent))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create and open a regular file.
    pub(crate) fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: i32,
    ) -> Result<(InodeAttr, OpenReply), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .create(parent, name, self.mount_uid, self.mount_gid, mode & 0o7777)
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.remember_named_file_flags(attr.ino, name);
        self.invalidate_readdir_cache(parent);
        let open = self.open(attr.ino, flags)?;
        Ok((attr, open))
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
        self.vfs
            .create(parent, name, self.mount_uid, self.mount_gid, mode & 0o7777)
            .inspect(|attr| {
                self.remember_named_file_flags(attr.ino, name);
                self.invalidate_readdir_cache(parent);
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Open an existing file.
    pub(crate) fn open(&self, ino: u64, flags: i32) -> Result<OpenReply, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && posix_open_requests_write(flags, trunc) {
            return Err(libc::EROFS);
        }
        let flag_policy = macfuse_open_cache_policy(flags, trunc);
        self.vfs
            .open_file(ino, open_flags, trunc)
            .map(|fh| {
                let keep_cache = if flag_policy.keep_cache {
                    self.keep_cache_for_current_generation(ino)
                } else {
                    self.record_write_capable_open(ino, fh);
                    self.vfs.record_macfuse_open_cache_decision(
                        VfsMacFuseOpenCacheDecision::WriteCapable,
                    );
                    false
                };
                OpenReply::new(fh)
                    .with_direct_io(flag_policy.direct_io)
                    .with_keep_cache(keep_cache)
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn clear_open_cache_snapshot(&self, ino: u64, reason: &'static str) {
        self.shared.open_cache_snapshots.lock().pop(&ino);
        debug!(ino, reason, "macFUSE open invalidates kernel cache");
    }

    fn record_write_capable_open(&self, ino: u64, file_handle: u64) {
        {
            self.write_capable_state.record_open(ino, file_handle);
            self.shared.open_cache_snapshots.lock().pop(&ino);
        }
        debug!(
            ino,
            reason = "write_capable",
            "macFUSE open invalidates kernel cache"
        );
    }

    fn has_active_write_capable_handles(&self, ino: u64) -> bool {
        self.write_capable_state.active_count(ino) > 0
    }

    fn keep_cache_for_current_generation(&self, ino: u64) -> bool {
        let Ok(attr) = self.vfs.getattr(ino) else {
            self.clear_open_cache_snapshot(ino, "unknown_state");
            self.vfs.record_macfuse_open_cache_decision(
                VfsMacFuseOpenCacheDecision::ReadOnlyUnknownState,
            );
            return false;
        };
        if attr.kind != FileType::RegularFile {
            self.clear_open_cache_snapshot(ino, "not_regular_file");
            self.vfs.record_macfuse_open_cache_decision(
                VfsMacFuseOpenCacheDecision::ReadOnlyNotRegularFile,
            );
            return false;
        };
        if self.vfs.inode_has_local_dirty_state(ino) {
            self.clear_open_cache_snapshot(ino, "local_dirty_state");
            self.vfs.record_macfuse_open_cache_decision(
                VfsMacFuseOpenCacheDecision::ReadOnlyLocalDirtyState,
            );
            return false;
        }
        if self.has_active_write_capable_handles(ino) {
            self.clear_open_cache_snapshot(ino, "active_writer");
            self.vfs.record_macfuse_open_cache_decision(
                VfsMacFuseOpenCacheDecision::ReadOnlyActiveWriter,
            );
            return false;
        }
        let graph = match self.open_cache_graph_signature(ino, attr.size) {
            Ok(graph) => graph,
            Err(error) => {
                debug!(
                    ino,
                    ?error,
                    "macFUSE open cache could not read file graph signature"
                );
                self.clear_open_cache_snapshot(ino, "unknown_state");
                self.vfs.record_macfuse_open_cache_decision(
                    VfsMacFuseOpenCacheDecision::ReadOnlyUnknownState,
                );
                return false;
            }
        };
        let snapshot = MacFuseOpenCacheSnapshot::from_attr(&attr, graph);
        let kind = snapshot.kind;

        let mut snapshots = self.shared.open_cache_snapshots.lock();
        if snapshots
            .get(&ino)
            .is_some_and(|previous| *previous == snapshot)
        {
            debug!(
                ino,
                keep_cache = true,
                content_epoch = snapshot.content_epoch,
                size = snapshot.size,
                reason = "read_only_safe",
                "macFUSE open cache decision"
            );
            let decision = match kind {
                MacFuseOpenCacheSnapshotKind::RemoteReady => {
                    VfsMacFuseOpenCacheDecision::ReadOnlyKeep
                }
                MacFuseOpenCacheSnapshotKind::LocalStableUnpublished => {
                    VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableKeep
                }
            };
            self.vfs.record_macfuse_open_cache_decision(decision);
            return true;
        }

        let content_epoch = snapshot.content_epoch;
        let size = snapshot.size;
        snapshots.put(ino, snapshot);
        debug!(
            ino,
            keep_cache = false,
            content_epoch,
            size,
            reason = "generation_changed",
            "macFUSE open cache decision"
        );
        let decision = match kind {
            MacFuseOpenCacheSnapshotKind::RemoteReady => {
                VfsMacFuseOpenCacheDecision::ReadOnlyGenerationChanged
            }
            MacFuseOpenCacheSnapshotKind::LocalStableUnpublished => {
                VfsMacFuseOpenCacheDecision::ReadOnlyLocalStableGenerationChanged
            }
        };
        self.vfs.record_macfuse_open_cache_decision(decision);
        false
    }

    fn open_cache_graph_signature(
        &self,
        ino: u64,
        size: u64,
    ) -> Result<Vec<MacFuseOpenCacheGraphSpan>, studiofs_meta::MetaError> {
        let spans = self.vfs.meta().lookup_regular_file_slices(ino, 0, size)?;
        Ok(spans
            .into_iter()
            .map(|span| MacFuseOpenCacheGraphSpan {
                chunk_id: span.chunk_id.0,
                range_start: span.range.start,
                range_end: span.range.end,
                source: open_cache_source_tag(span.source),
                slice_id: span.slice_id.map(|slice_id| slice_id.0),
                slice_order: span.slice_order.map(|slice_order| slice_order.0),
                payload: span.payload.map(|payload| MacFuseOpenCachePayload {
                    object_id: payload.object_id,
                    offset: payload.offset,
                    length: payload.length,
                    flags: payload.flags.bits(),
                }),
            })
            .collect())
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
                    self.clear_open_cache_snapshot(attr.ino, "unlink");
                    self.clear_file_flags(attr.ino);
                }
                self.invalidate_readdir_cache(parent);
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
        self.vfs
            .rmdir(parent, name)
            .inspect(|_| {
                if let Some(attr) = &existing {
                    self.clear_file_flags(attr.ino);
                }
                self.invalidate_readdir_cache(parent);
            })
            .map_err(|error| vfs_err_to_errno(&error))
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
        let overwritten = self.vfs.lookup(newparent, newname).ok();
        self.vfs
            .rename(parent, name, newparent, newname)
            .inspect(|_| {
                if let Some(attr) = &overwritten {
                    self.clear_file_flags(attr.ino);
                }
                self.invalidate_readdir_cache(parent);
                if newparent != parent {
                    self.invalidate_readdir_cache(newparent);
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
        let size =
            clamp_reusable_read_size(request.size, MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32);
        let size_usize = (size as usize).min(request.max_read_len as usize);
        let read_len = readahead_window_len(size_usize).min(request.max_read_len as usize);

        let mut out = state
            .shared
            .read_buffers
            .clone()
            .acquire_with_capacity(size_usize, read_len);
        if cancellation.is_cancelled() {
            return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
        }
        if let Some(copied) = state.shared.read_windows.try_read(
            request.file_handle,
            request.ino,
            request.offset,
            &mut out,
        ) {
            out.truncate(copied);
            state
                .vfs
                .record_fuse_read_callback(copied, started.elapsed(), false);
            return Ok(out);
        }

        if read_len > size_usize {
            out.buffer.resize(read_len, 0);
        }
        if cancellation.is_cancelled() {
            return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
        }
        let generation = state
            .shared
            .read_windows
            .invalidation_generation(request.ino);
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
                if written > size_usize {
                    let remainder_len = written - reply_len;
                    let mut remainder = state
                        .shared
                        .read_buffers
                        .clone()
                        .acquire_with_capacity(0, remainder_len);
                    remainder.buffer.extend_from_slice(&out[reply_len..written]);
                    state.shared.read_windows.remember_if_current(
                        request.file_handle,
                        request.ino,
                        request.offset.saturating_add(reply_len as u64),
                        request.offset.saturating_add(reply_len as u64),
                        remainder,
                        generation,
                    );
                    out.truncate(reply_len);
                    return Ok(out);
                } else {
                    state
                        .shared
                        .read_windows
                        .invalidate_handle(request.file_handle);
                }
                out.truncate(reply_len);
                Ok(out)
            }
            Err(error) => {
                state
                    .vfs
                    .record_fuse_read_callback(size_usize, started.elapsed(), true);
                state
                    .shared
                    .read_windows
                    .invalidate_handle(request.file_handle);
                Err(vfs_err_to_errno(&error))
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
        };
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let read_queue_byte_limit = self.read_queue_byte_limit;
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        let mut request = request;
        request.register_interrupt_token(cancellation.fuse_token());
        self.runtime_handle.spawn(async move {
            let admission_bytes = queued_read_admission_bytes(size, read_queue_byte_limit);
            let mut read_guard =
                AsyncReadTaskGuard::new(admission_bytes, &state.shared, &state.vfs);
            if cancellation.is_cancelled() {
                read_guard.finish_with(AsyncReadOutcome::Canceled);
                log_callback_error("read", MACFUSE_READ_INTERRUPTED_ERRNO);
                let _ = Reply::new(request).error(MACFUSE_READ_INTERRUPTED_ERRNO);
                return;
            }
            let _permit = match acquire_read_admission(
                &state.shared.read_admission,
                admission_bytes,
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
                    max_read_len: admission_bytes,
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
        };
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let read_queue_byte_limit = self.read_queue_byte_limit;
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        Ok(QueuedReadForTest {
            runtime_handle: self.runtime_handle.clone(),
            handle: self.runtime_handle.spawn(async move {
                let admission_bytes = queued_read_admission_bytes(size, read_queue_byte_limit);
                let mut read_guard =
                    AsyncReadTaskGuard::new(admission_bytes, &state.shared, &state.vfs);
                if cancellation.is_cancelled() {
                    read_guard.finish_with(AsyncReadOutcome::Canceled);
                    return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
                }
                let _permit = acquire_read_admission(
                    &state.shared.read_admission,
                    admission_bytes,
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
                        max_read_len: admission_bytes,
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
        };
        let size = read_execution_size(size, self.read_queue_byte_limit);
        let read_queue_byte_limit = self.read_queue_byte_limit;
        let started = Instant::now();
        let cancellation = ReadCancellationToken::default();
        let worker_cancellation = cancellation.clone();
        Ok(CancelableQueuedReadForTest {
            cancellation,
            queued: QueuedReadForTest {
                runtime_handle: self.runtime_handle.clone(),
                handle: self.runtime_handle.spawn(async move {
                    let admission_bytes = queued_read_admission_bytes(size, read_queue_byte_limit);
                    let mut read_guard =
                        AsyncReadTaskGuard::new(admission_bytes, &state.shared, &state.vfs);
                    if worker_cancellation.is_cancelled() {
                        read_guard.finish_with(AsyncReadOutcome::Canceled);
                        return Err(MACFUSE_READ_INTERRUPTED_ERRNO);
                    }
                    let _permit = acquire_read_admission(
                        &state.shared.read_admission,
                        admission_bytes,
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
                            max_read_len: admission_bytes,
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
        let written = self
            .runtime_handle
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                file_handle,
                ino,
                offset,
                data,
                LocalSyncPolicy::Opportunistic,
            ))
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.shared.read_windows.invalidate_inode(ino);
        Ok(written)
    }

    /// Flush file data through a VFS file handle.
    pub(crate) fn flush(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.flush_fh(file_handle))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Synchronize file data for a handle when available, otherwise for an inode.
    pub(crate) fn fsync(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        datasync: bool,
    ) -> Result<(), i32> {
        let result = if let Some(file_handle) = file_handle.filter(|handle| *handle != 0) {
            // Handle-backed sync must preserve pending handle-local write state;
            // StudioVfs does not currently distinguish fdatasync from fsync on
            // this path, so both use the handle flush barrier.
            self.runtime_handle.block_on(self.vfs.flush_fh(file_handle))
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
        self.shared.read_windows.invalidate_inode(ino);
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
        self.shared.read_windows.invalidate_inode(ino);
        Ok(attr)
    }

    /// Release a file handle opened through this adapter.
    pub(crate) fn release(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        let released_write_handle = self.write_capable_state.release_handle(file_handle);
        let result = self
            .runtime_handle
            .block_on(self.vfs.release_fh_with_outcome(file_handle))
            .map_err(|error| vfs_err_to_errno(&error));
        self.shared.read_windows.invalidate_handle(file_handle);
        result?;
        if let Some(released) = released_write_handle.filter(|released| released.last_for_inode) {
            self.clear_open_cache_snapshot(released.ino, "clean_write_release");
        }
        Ok(())
    }

    /// Read directory entries from VFS.
    pub(crate) fn readdir(
        &self,
        request: &Request,
        ino: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        self.readdir_with_push(ino, offset, size, |buffer, name, attr, next_offset| {
            buffer
                .push(request, name, attr, next_offset)
                .map_err(directory_entry_errno)
        })
    }

    #[cfg(test)]
    pub(crate) fn readdir_for_test(
        &self,
        ino: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        self.readdir_with_push(ino, offset, size, |buffer, name, _attr, next_offset| {
            buffer
                .push_for_test(0, name, next_offset)
                .map_err(directory_entry_errno)
        })
    }

    fn readdir_with_push(
        &self,
        ino: u64,
        offset: i64,
        size: usize,
        mut push: impl FnMut(&mut DirectoryEntryBuffer, &OsStr, &stat, off_t) -> Result<(), i32>,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let mut buffer = DirectoryEntryBuffer::new(size);
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        if idx == 0 {
            if !push_or_finish(&mut buffer, OsStr::new("."), &dir_attr, 1, &mut push)? {
                return Ok(buffer);
            }
            idx = 1;
        }
        if idx == 1 {
            if !push_or_finish(&mut buffer, OsStr::new(".."), &dir_attr, 2, &mut push)? {
                return Ok(buffer);
            }
            idx = 2;
        }

        let entries = self.cached_readdir_entries(ino)?;
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

    fn attr_to_stat(&self, attr: &InodeAttr) -> stat {
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

    fn cached_readdir_entries(&self, ino: u64) -> Result<Arc<Vec<DirEntry>>, i32> {
        let mut cache = self.readdir_cache.lock().map_err(|_| libc::EIO)?;
        loop {
            let now = Instant::now();
            match cache.get(&ino) {
                Some(ReaddirCacheEntry::Ready { loaded_at, entries })
                    if now.saturating_duration_since(*loaded_at) <= READDIR_CACHE_TTL =>
                {
                    return Ok(Arc::clone(entries));
                }
                Some(ReaddirCacheEntry::Loading) => {
                    cache = self
                        .readdir_cache_ready
                        .wait(cache)
                        .map_err(|_| libc::EIO)?;
                }
                _ => {
                    if cache.len() >= READDIR_CACHE_PRUNE_THRESHOLD
                        && self.should_prune_readdir_cache(now)?
                    {
                        cache.retain(|_, entry| match entry {
                            ReaddirCacheEntry::Ready { loaded_at, .. } => {
                                now.saturating_duration_since(*loaded_at) <= READDIR_CACHE_TTL
                            }
                            ReaddirCacheEntry::Loading => true,
                        });
                    }
                    cache.insert(ino, ReaddirCacheEntry::Loading);
                    break;
                }
            }
        }
        drop(cache);

        let entries_result = self
            .vfs
            .readdir(ino)
            .map(Arc::new)
            .map_err(|error| vfs_err_to_errno(&error));
        let mut cache = self.readdir_cache.lock().map_err(|_| libc::EIO)?;
        match entries_result {
            Ok(entries) => {
                cache.insert(
                    ino,
                    ReaddirCacheEntry::Ready {
                        loaded_at: Instant::now(),
                        entries: Arc::clone(&entries),
                    },
                );
                self.readdir_cache_ready.notify_all();
                Ok(entries)
            }
            Err(errno) => {
                cache.remove(&ino);
                self.readdir_cache_ready.notify_all();
                Err(errno)
            }
        }
    }

    fn invalidate_readdir_cache(&self, ino: u64) {
        if let Ok(mut cache) = self.readdir_cache.lock() {
            cache.remove(&ino);
        }
    }

    fn should_prune_readdir_cache(&self, now: Instant) -> Result<bool, i32> {
        let mut last_pruned = self
            .readdir_cache_last_pruned
            .lock()
            .map_err(|_| libc::EIO)?;
        if last_pruned
            .is_some_and(|last| now.saturating_duration_since(last) < READDIR_CACHE_PRUNE_INTERVAL)
        {
            return Ok(false);
        }
        *last_pruned = Some(now);
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn readdir_cache_entry_count_for_test(&self) -> usize {
        self.readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned")
            .len()
    }

    #[cfg(test)]
    pub(crate) fn fill_expired_readdir_cache_entries_for_test(&self) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let loaded_at = Instant::now() - READDIR_CACHE_TTL - Duration::from_secs(1);
        for ino in 10_000..(10_000 + READDIR_CACHE_PRUNE_THRESHOLD as u64) {
            cache.insert(
                ino,
                ReaddirCacheEntry::Ready {
                    loaded_at,
                    entries: Arc::new(Vec::new()),
                },
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn fill_fresh_readdir_cache_entries_for_test(&self) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let loaded_at = Instant::now();
        for ino in 20_000..(20_000 + READDIR_CACHE_PRUNE_THRESHOLD as u64) {
            cache.insert(
                ino,
                ReaddirCacheEntry::Ready {
                    loaded_at,
                    entries: Arc::new(Vec::new()),
                },
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn set_ready_readdir_cache_loaded_at_for_test(&self, ino: u64, loaded_at: Instant) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let entries = match cache.remove(&ino) {
            Some(ReaddirCacheEntry::Ready { entries, .. }) => entries,
            _ => Arc::new(Vec::new()),
        };
        cache.insert(ino, ReaddirCacheEntry::Ready { loaded_at, entries });
    }

    #[cfg(test)]
    pub(crate) fn readdir_cache_loaded_at_for_test(&self, ino: u64) -> Option<Instant> {
        self.readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned")
            .get(&ino)
            .and_then(|entry| match entry {
                ReaddirCacheEntry::Ready { loaded_at, .. } => Some(*loaded_at),
                ReaddirCacheEntry::Loading => None,
            })
    }
}

enum ReaddirCacheEntry {
    Ready {
        loaded_at: Instant,
        entries: Arc<Vec<DirEntry>>,
    },
    Loading,
}

pub(crate) struct CallbackState {
    adapter: StudioVfsAdapter,
}

impl CallbackState {
    pub(crate) fn new(vfs: Arc<StudioVfs>, runtime_handle: Handle, read_only: bool) -> Self {
        Self {
            adapter: StudioVfsAdapter::new_with_read_only(vfs, runtime_handle, read_only),
        }
    }

    pub(crate) fn kernel_invalidator(
        &self,
        inner: Arc<dyn KernelInvalidator>,
    ) -> Arc<dyn KernelInvalidator> {
        self.adapter.kernel_invalidator(inner)
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
        direct_io: false,
        keep_cache: !write_capable,
    }
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
        MacFuseOpenCacheGraphSpan, MacFuseOpenCacheSnapshot, MacFuseOpenCacheSnapshotKind,
        StudioVfsAdapter,
    };
    use studiofs_meta::{InodeAttr, UploadState};
    use studiofs_vfs::contract_harness::{CONTRACT_ROOT_INO, ContractVfsFixture};

    #[test]
    fn open_cache_snapshot_identity_includes_publication_state() {
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

        assert_ne!(
            local_stable, remote_ready,
            "publication transitions with unchanged graph must still drop kernel pages once"
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
    match state.adapter.symlink(parent, name, target) {
        Ok(attr) => {
            debug!(
                parent,
                name = %String::from_utf8_lossy(name),
                target = %String::from_utf8_lossy(target),
                ino = attr.ino,
                "macFUSE symlink callback created entry"
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
                target = %String::from_utf8_lossy(target),
                errno,
                "macFUSE symlink callback returned error"
            );
            reply_error(req, errno);
        }
    }
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
    match state.adapter.create(parent, name, mode, flags) {
        Ok((attr, open)) => {
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
                let _ = Reply::new(request).create(&entry, open);
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
    match state.adapter.rmdir(parent, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => {
            warn!(
                parent,
                name = %String::from_utf8_lossy(name),
                errno,
                "macFUSE rmdir callback returned error"
            );
            reply_error(req, errno);
        }
    }
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
    match state.adapter.open(ino, flags) {
        Ok(open) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).open(open);
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
    _fi: *mut raw::FuseFileInfo,
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
        to_set, flags, "macFUSE setattr_x callback accepted Darwin metadata as overlay"
    );
    if !setattr_args_are_empty(&args) {
        match state.adapter.setattr(None, ino, args) {
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

/// macFUSE readdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_readdir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Some(request) = owned_request(req) else {
        return;
    };
    match state.adapter.readdir(&request, ino, off, size) {
        Ok(entries) => {
            let _ = Reply::new(request).directory_entries(&entries);
        }
        Err(errno) => {
            log_callback_error("readdir", errno);
            let _ = Reply::new(request).error(errno);
        }
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
    fn readahead_window_len_uses_floor_for_small_reads() {
        assert_eq!(readahead_window_len(0), 0);
        assert_eq!(
            readahead_window_len(4 * 1024),
            MACFUSE_READ_WINDOW_MIN_BYTES
        );
        assert_eq!(
            readahead_window_len(MACFUSE_READ_WINDOW_MAX_BYTES),
            MACFUSE_READ_WINDOW_MAX_BYTES
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

    #[test]
    fn read_window_store_rejects_stale_cache_fill_after_invalidation() {
        let store = ReadWindowStore::default();
        let pool = Arc::new(ReadBufferPool::default());
        let generation = store.invalidation_generation(42);
        let mut stale = Arc::clone(&pool).acquire(5);
        stale.copy_from_slice(b"stale");

        store.invalidate_inode(42);

        assert!(
            !store.remember_if_current(7, 42, 0, 0, stale, generation),
            "window fill must be rejected after an intervening inode invalidation"
        );
        let mut out = [0_u8; 5];
        assert_eq!(store.try_read(7, 42, 0, &mut out), None);
        assert_eq!(pool.available_count(), 1);
        assert!(
            pool.last_available_capacity()
                .is_some_and(|capacity| capacity >= 5),
            "rejected stale cache buffer should be returned to the reusable pool"
        );
    }

    #[test]
    fn write_capable_state_allows_concurrent_per_inode_access_without_external_lock() {
        let state = Arc::new(WriteCapableState::default());

        std::thread::scope(|scope| {
            for shard_index in 0..8 {
                let state = Arc::clone(&state);
                scope.spawn(move || {
                    let ino = 10_000 + shard_index;
                    let file_handle = 20_000 + shard_index;
                    state.record_open(ino, file_handle);
                    assert_eq!(state.active_count(ino), 1);
                    let released = state
                        .release_handle(file_handle)
                        .expect("recorded handle should release");
                    assert_eq!(
                        released,
                        ReleasedWriteCapableHandle {
                            ino,
                            last_for_inode: true,
                        }
                    );
                    assert_eq!(state.active_count(ino), 0);
                });
            }
        });
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
