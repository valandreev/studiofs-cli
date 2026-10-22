//! Sequential RAM read-ahead for bridge-opted-in handles.
//!
//! macFUSE delivers `F_NOCACHE` and `direct_io` reads one callback at a time,
//! so each callback waits for one local payload read. A handle that a bridge
//! opted in loads the next [`RAM_READAHEAD_CHUNK_BYTES`]-aligned chunks of a
//! sequential stream in background tasks; later callbacks copy from memory
//! while several chunk reads keep the disk busy.
//!
//! Coherence: a chunk task reads and installs under a file-session `Read`
//! permit, so local writes (exclusive `Write`) cannot interleave with it. Every
//! content invalidation reaches [`OpenFile::clear_read_window`], which bumps the
//! handle's generation and discards all chunks; a task installs only when the
//! generation it was scheduled with is still current. Callbacks look chunks up
//! and wait for them before entering their own `Read` permit: the session gate
//! is a fair lock, and waiting while holding a permit could deadlock against a
//! queued writer that blocks the chunk task.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tracing::debug;

use super::buffer_pool::BufferPool;
use super::file_session::{FileSessionOperation, OpenFile};
use super::read_context::ForegroundReadScope;
use super::{AccessLevel, Ino, StudioVfs, VfsResult};

/// Size and alignment of one read-ahead chunk; matches the macFUSE `iosize`.
pub(super) const RAM_READAHEAD_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const CHUNK_BYTES: u64 = RAM_READAHEAD_CHUNK_BYTES as u64;
/// Mount-wide memory held by loaded and in-flight chunks.
const RAM_READAHEAD_BUDGET_BYTES: usize = 256 * 1024 * 1024;
/// Sequential reads a handle must serve before it starts reading ahead.
const RAM_READAHEAD_MIN_STREAK: u32 = 2;
/// Smallest useful depth: with one chunk ahead, requests that straddle a
/// chunk boundary miss and the chunk read is wasted.
const RAM_READAHEAD_MIN_DEPTH_CHUNKS: u64 = 2;

/// Default idle per-handle read-ahead depth. macFUSE and WinFSP opt handles in.
///
/// Read-ahead competes with background write-back and upload rereads for the
/// cache disk. On the reference USB disk, 12 MiB raised a 16 GB F_NOCACHE read
/// by about 50% while the upload kept 40-70% of its rate; 32 MiB read faster
/// but stalled the upload until the read ended. Use 32 MiB only while idle
/// and retain the 12 MiB depth while background disk work is pending.
pub(super) const DEFAULT_SEQUENTIAL_RAM_READAHEAD_BYTES: usize =
    if cfg!(any(target_os = "macos", windows)) {
        32 * 1024 * 1024
    } else {
        0
    };

pub(super) const DEFAULT_SEQUENTIAL_RAM_READAHEAD_SHARED_BYTES: usize =
    if cfg!(any(target_os = "macos", windows)) {
        12 * 1024 * 1024
    } else {
        0
    };
const RAM_READAHEAD_SHARED_HOLD: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
enum RamChunkState {
    Pending,
    Ready(Bytes),
    Discarded,
}

#[derive(Debug)]
struct RamChunkSlot {
    state: watch::Sender<RamChunkState>,
    _budget: OwnedSemaphorePermit,
}

impl RamChunkSlot {
    fn new(budget: OwnedSemaphorePermit) -> Self {
        Self {
            state: watch::Sender::new(RamChunkState::Pending),
            _budget: budget,
        }
    }

    fn discard(&self) {
        self.state.send_replace(RamChunkState::Discarded);
    }

    /// Wait until the chunk resolves and return its bytes, or `None` when it
    /// was discarded. Never holds a lock or file-session permit while waiting.
    async fn resolved(&self) -> (Option<Bytes>, bool) {
        let mut rx = self.state.subscribe();
        let mut waited = false;
        loop {
            match &*rx.borrow_and_update() {
                RamChunkState::Ready(data) => return (Some(data.clone()), waited),
                RamChunkState::Discarded => return (None, waited),
                RamChunkState::Pending => {}
            }
            waited = true;
            // The sender lives in this slot, which the caller keeps alive.
            if rx.changed().await.is_err() {
                return (None, waited);
            }
        }
    }
}

/// Resolves a chunk that a task leaves pending, including on cancellation.
struct PendingChunkGuard(Arc<RamChunkSlot>);

impl Drop for PendingChunkGuard {
    fn drop(&mut self) {
        self.0.state.send_if_modified(|state| {
            if matches!(state, RamChunkState::Pending) {
                *state = RamChunkState::Discarded;
                true
            } else {
                false
            }
        });
    }
}

#[derive(Debug)]
struct RamChunk {
    index: u64,
    slot: Arc<RamChunkSlot>,
}

/// Per-handle read-ahead state, owned by [`OpenFile`].
#[derive(Debug, Default)]
pub(super) struct RamReadAheadState {
    /// Configured idle depth at opt-in; zero means the handle did not opt in.
    depth_chunks: u64,
    generation: u64,
    last_read: Option<(u64, u64)>,
    streak: u32,
    /// Last observed file size; chunks are not scheduled at or past it.
    file_size: u64,
    /// Queued chunks in ascending index order.
    chunks: VecDeque<RamChunk>,
}

impl RamReadAheadState {
    pub(super) fn enable(&mut self, depth_chunks: u64, file_size: u64) {
        self.depth_chunks = depth_chunks;
        self.file_size = file_size;
    }

    pub(super) fn disable(&mut self) {
        self.depth_chunks = 0;
        self.discard_all();
    }

    pub(super) fn is_enabled(&self) -> bool {
        self.depth_chunks > 0
    }

    /// Discard every chunk; a pending task observes the new generation and
    /// drops its result.
    pub(super) fn discard_all(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        for chunk in self.chunks.drain(..) {
            chunk.slot.discard();
        }
    }

    fn position(&self, index: u64) -> Result<usize, usize> {
        self.chunks
            .binary_search_by_key(&index, |chunk| chunk.index)
    }

    /// Slots covering `[offset, offset + len)`, or `None` when any is missing.
    fn slots_for(&self, offset: u64, len: usize) -> Option<Vec<Arc<RamChunkSlot>>> {
        let end = offset.checked_add(len as u64)?;
        let first = offset / CHUNK_BYTES;
        let last = end.checked_sub(1)? / CHUNK_BYTES;
        (first..=last)
            .map(|index| {
                let position = self.position(index).ok()?;
                Some(Arc::clone(&self.chunks[position].slot))
            })
            .collect()
    }

    fn install(&self, index: u64, slot: &Arc<RamChunkSlot>, generation: u64, data: Bytes) -> bool {
        let current = self.generation == generation
            && self
                .position(index)
                .is_ok_and(|position| Arc::ptr_eq(&self.chunks[position].slot, slot));
        if current {
            slot.state.send_replace(RamChunkState::Ready(data));
        }
        current
    }
}

#[derive(Debug, Default)]
struct RamReadAheadCounters {
    scheduled_chunks: AtomicU64,
    loaded_bytes: AtomicU64,
    hit_reads: AtomicU64,
    hit_bytes: AtomicU64,
    wait_reads: AtomicU64,
    discarded_chunks: AtomicU64,
    budget_skips: AtomicU64,
}

/// Snapshot of the mount-wide RAM read-ahead counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct RamReadAheadStats {
    /// Chunk loads started.
    pub scheduled_chunks: u64,
    /// Bytes loaded into chunks that were installed.
    pub loaded_bytes: u64,
    /// Read callbacks served entirely from chunks.
    pub hit_reads: u64,
    /// Bytes those callbacks returned.
    pub hit_bytes: u64,
    /// Hits that waited for an in-flight chunk.
    pub wait_reads: u64,
    /// Chunk loads whose result was dropped (invalidated, superseded, or failed).
    pub discarded_chunks: u64,
    /// Scheduling passes stopped by the mount-wide budget (at most one per pass).
    pub budget_skips: u64,
}

/// Mount-wide RAM read-ahead configuration, memory budget, and counters.
#[derive(Debug)]
pub(super) struct RamReadAheadRuntime {
    depth_chunks: u64,
    shared_depth_chunks: u64,
    last_busy: Mutex<Option<Instant>>,
    budget: Arc<Semaphore>,
    buffers: BufferPool,
    counters: RamReadAheadCounters,
}

impl RamReadAheadRuntime {
    pub(super) fn new(depth_bytes: usize, shared_bytes: usize) -> Self {
        let budget_chunks = RAM_READAHEAD_BUDGET_BYTES / RAM_READAHEAD_CHUNK_BYTES;
        let depth_chunks = Self::depth_chunks(depth_bytes);
        Self {
            depth_chunks,
            shared_depth_chunks: Self::depth_chunks(shared_bytes).min(depth_chunks),
            last_busy: Mutex::new(None),
            budget: Arc::new(Semaphore::new(budget_chunks)),
            buffers: BufferPool::new(RAM_READAHEAD_CHUNK_BYTES, budget_chunks),
            counters: RamReadAheadCounters::default(),
        }
    }

    fn effective_depth_chunks(&self, busy: bool, now: Instant) -> u64 {
        let mut last_busy = self.last_busy.lock();
        if busy {
            *last_busy = Some(now);
        }
        if last_busy
            .is_some_and(|last| now.saturating_duration_since(last) < RAM_READAHEAD_SHARED_HOLD)
        {
            self.shared_depth_chunks
        } else {
            self.depth_chunks
        }
    }

    fn depth_chunks(depth_bytes: usize) -> u64 {
        match depth_bytes.div_ceil(RAM_READAHEAD_CHUNK_BYTES) as u64 {
            0 => 0,
            chunks => chunks.max(RAM_READAHEAD_MIN_DEPTH_CHUNKS),
        }
    }

    pub(super) fn stats(&self) -> RamReadAheadStats {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        let counters = &self.counters;
        RamReadAheadStats {
            scheduled_chunks: load(&counters.scheduled_chunks),
            loaded_bytes: load(&counters.loaded_bytes),
            hit_reads: load(&counters.hit_reads),
            hit_bytes: load(&counters.hit_bytes),
            wait_reads: load(&counters.wait_reads),
            discarded_chunks: load(&counters.discarded_chunks),
            budget_skips: load(&counters.budget_skips),
        }
    }
}

impl StudioVfs {
    pub(super) fn ram_readahead_depth_chunks_at(&self, now: Instant) -> u64 {
        let busy = self
            .state
            .upload
            .active_upload_workers
            .load(Ordering::Acquire)
            != 0
            || self.pending_upload_count() != 0
            || self.upload_scheduler.in_flight() != 0
            || self
                .state
                .deferred_materialize_inflight
                .entries
                .try_lock()
                .is_none_or(|entries| !entries.is_empty())
            || self
                .state
                .deferred_materialize_reschedule_needed
                .try_lock()
                .is_none_or(|entries| !entries.is_empty())
            || self
                .state
                .deferred_release_inflight
                .entries
                .try_lock()
                .is_none_or(|entries| !entries.is_empty())
            || self
                .local_payload_backend
                .try_dirty_object_count()
                .is_none_or(|count| count != 0);
        self.ram_readahead.effective_depth_chunks(busy, now)
    }

    /// Opt a handle into sequential RAM read-ahead. Returns `false` when the
    /// mount disables it or the handle is unknown.
    pub fn enable_sequential_ram_readahead(&self, fh: u64) -> bool {
        let depth_chunks = self.ram_readahead.depth_chunks;
        if depth_chunks == 0 {
            return false;
        }
        let Some(open_file) = self.handle_table.get(fh) else {
            return false;
        };
        let file_size = open_file.open_attr().map_or(0, |attr| attr.size);
        open_file
            .ram_readahead()
            .lock()
            .enable(depth_chunks, file_size);
        true
    }

    /// Whether a handle currently reads ahead into RAM.
    pub fn sequential_ram_readahead_enabled(&self, fh: u64) -> bool {
        self.handle_table
            .get(fh)
            .is_some_and(|open_file| open_file.ram_readahead().lock().is_enabled())
    }

    /// Return mount-wide RAM read-ahead counters.
    pub(super) fn ram_readahead_stats(&self) -> RamReadAheadStats {
        self.ram_readahead.stats()
    }

    pub(super) fn ram_readahead_handle(&self, fh: u64, ino: Ino) -> Option<OpenFile> {
        if self.ram_readahead.depth_chunks == 0 {
            return None;
        }
        self.handle_table.get(fh).filter(|open_file| {
            open_file.ino == ino && open_file.ram_readahead().lock().is_enabled()
        })
    }

    pub(super) fn observe_ram_readahead_file_size(&self, open_file: &OpenFile, file_size: u64) {
        if self.ram_readahead.depth_chunks != 0 {
            open_file.ram_readahead().lock().file_size = file_size;
        }
    }

    /// Serve a read from loaded chunks, waiting for in-flight ones. Returns
    /// `None` when any byte of the request is not covered, so the caller falls
    /// back to the normal read path.
    pub(super) async fn read_from_ram_readahead(
        &self,
        open_file: &OpenFile,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<Option<u32>> {
        if buffer.is_empty() {
            return Ok(None);
        }
        let Some(slots) = open_file
            .ram_readahead()
            .lock()
            .slots_for(offset, buffer.len())
        else {
            return Ok(None);
        };
        let ino = open_file.ino;
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
        let mut copied = 0usize;
        let mut waited = false;
        let mut chunk_start = offset - offset % CHUNK_BYTES;
        for slot in slots {
            let (data, slot_waited) = slot.resolved().await;
            waited |= slot_waited;
            let Some(data) = data else {
                return Ok(None);
            };
            let position = offset + copied as u64;
            let rel = (position - chunk_start) as usize;
            if rel >= data.len() {
                break;
            }
            let len = (data.len() - rel).min(buffer.len() - copied);
            buffer[copied..copied + len].copy_from_slice(&data[rel..rel + len]);
            copied += len;
            // Only the chunk that contains EOF is shorter than a full chunk.
            if data.len() < RAM_READAHEAD_CHUNK_BYTES || copied == buffer.len() {
                break;
            }
            chunk_start += CHUNK_BYTES;
        }
        if copied == 0 {
            return Ok(None);
        }
        // Account the hit like a normal read only once it is certain, so a
        // fallback to the read path does not observe the same read twice.
        let access = self.state.read.observe_read_access(
            Some(open_file),
            ino,
            offset,
            buffer.len(),
            self.access_tracker,
        );
        let mut telemetry = ForegroundReadScope::new(
            &self.state.read,
            self.upload_fg_read_latency_window,
            access.policy,
        );
        telemetry.record_bytes(copied as u64);
        drop(telemetry);
        self.note_vfs_read_call(buffer.len());
        open_file.record_read_progress(offset, copied);
        let counters = &self.ram_readahead.counters;
        counters.hit_reads.fetch_add(1, Ordering::Relaxed);
        counters
            .hit_bytes
            .fetch_add(copied as u64, Ordering::Relaxed);
        if waited {
            counters.wait_reads.fetch_add(1, Ordering::Relaxed);
        }
        Ok(Some(copied as u32))
    }

    /// Record a served read and keep the configured depth of chunks queued
    /// ahead of a sequential stream.
    pub(super) fn schedule_ram_readahead(
        self: &Arc<Self>,
        open_file: &OpenFile,
        offset: u64,
        read: usize,
    ) {
        if self.shutdown_requested() {
            return;
        }
        let depth_chunks = self.ram_readahead_depth_chunks_at(Instant::now());
        let mut launches = Vec::new();
        {
            let mut state = open_file.ram_readahead().lock();
            if !state.is_enabled() {
                return;
            }
            let end = offset.saturating_add(read as u64);
            // UBC may round a callback past the application's frame boundary;
            // forward overlapping callbacks still form one continuous stream.
            if state.last_read.is_some_and(|(last_start, last_end)| {
                offset > last_start && offset <= last_end && end > last_end
            }) {
                state.streak = state.streak.saturating_add(1);
            } else {
                if state.last_read.is_some() {
                    state.discard_all();
                }
                state.streak = 1;
            }
            state.last_read = Some((offset, end));
            while state
                .chunks
                .front()
                .is_some_and(|chunk| (chunk.index + 1) * CHUNK_BYTES <= offset)
            {
                state.chunks.pop_front();
            }
            if read == 0 || state.streak < RAM_READAHEAD_MIN_STREAK {
                return;
            }
            let first = end / CHUNK_BYTES;
            // A lead shorter than the next request never covers it: WinFSP
            // passes whole unbuffered frames, which can exceed the depth.
            let depth_chunks = if depth_chunks == 0 {
                0
            } else {
                depth_chunks.max((read as u64).div_ceil(CHUNK_BYTES) + 1)
            };
            let limit = first.saturating_add(depth_chunks);
            while state
                .chunks
                .back()
                .is_some_and(|chunk| chunk.index >= limit)
            {
                if let Some(chunk) = state.chunks.pop_back() {
                    chunk.slot.discard();
                }
            }
            for index in first..limit {
                if index.saturating_mul(CHUNK_BYTES) >= state.file_size {
                    break;
                }
                let Err(position) = state.position(index) else {
                    continue;
                };
                let Ok(budget) = Arc::clone(&self.ram_readahead.budget).try_acquire_owned() else {
                    self.ram_readahead
                        .counters
                        .budget_skips
                        .fetch_add(1, Ordering::Relaxed);
                    break;
                };
                let slot = Arc::new(RamChunkSlot::new(budget));
                state.chunks.insert(
                    position,
                    RamChunk {
                        index,
                        slot: Arc::clone(&slot),
                    },
                );
                launches.push((index, slot, state.generation));
            }
        }
        for (index, slot, generation) in launches {
            self.ram_readahead
                .counters
                .scheduled_chunks
                .fetch_add(1, Ordering::Relaxed);
            let vfs = Arc::clone(self);
            let open_file = open_file.clone();
            // Capture before spawning so cancellation before the first poll resolves the slot.
            let resolve = PendingChunkGuard(Arc::clone(&slot));
            self.spawn_background_task(async move {
                let _resolve = resolve;
                vfs.load_ram_readahead_chunk(open_file, index, slot, generation)
                    .await;
            });
        }
    }

    async fn load_ram_readahead_chunk(
        &self,
        open_file: OpenFile,
        index: u64,
        slot: Arc<RamChunkSlot>,
        generation: u64,
    ) {
        let ino = open_file.ino;
        let offset = index * CHUNK_BYTES;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let installed = match self.read_ram_readahead_chunk(ino, offset).await {
            Ok(Some((data, file_size))) => {
                let len = data.len() as u64;
                let mut state = open_file.ram_readahead().lock();
                let installed = state.install(index, &slot, generation, data);
                if installed {
                    state.file_size = file_size;
                }
                drop(state);
                if installed {
                    self.ram_readahead
                        .counters
                        .loaded_bytes
                        .fetch_add(len, Ordering::Relaxed);
                }
                installed
            }
            Ok(None) => false,
            Err(error) => {
                debug!(ino, offset, error = %error, "RAM read-ahead chunk load failed");
                false
            }
        };
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        if !installed {
            self.ram_readahead
                .counters
                .discarded_chunks
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read one chunk through the normal foreground read path. Returns the
    /// bytes and the file size the read observed, or `None` when the chunk
    /// must not be cached.
    async fn read_ram_readahead_chunk(
        &self,
        ino: Ino,
        offset: u64,
    ) -> VfsResult<Option<(Bytes, u64)>> {
        if self.shutdown_requested() || self.ignored_entry_attr(ino).is_some() {
            return Ok(None);
        }
        let prepared = self
            .prepare_foreground_read(ino, offset, RAM_READAHEAD_CHUNK_BYTES)
            .await?;
        if prepared.context.placeholder_visible(self, ino) {
            return Ok(None);
        }
        let file_size = prepared.context.attr.size;
        if prepared.read_len == 0 {
            return Ok(Some((Bytes::new(), file_size)));
        }
        let mut buffer = self
            .ram_readahead
            .buffers
            .acquire_read_window(prepared.read_len);
        let written = self
            .execute_prepared_foreground_read(None, ino, offset, buffer.as_mut(), &prepared, None)
            .await?;
        Ok(Some((buffer.into_bytes(written), file_size)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pending_chunks_fixture(
        label: &str,
    ) -> (
        crate::contract_harness::ContractVfsFixture,
        OpenFile,
        tokio::runtime::Runtime,
    ) {
        use crate::vfs::{OpenFlags, VfsConfigRaw};
        let fixture = crate::contract_harness::ContractVfsFixture::new_with_config(
            label,
            VfsConfigRaw {
                sequential_ram_readahead_bytes: Some(12 * 1024 * 1024),
                sequential_ram_readahead_shared_bytes: Some(12 * 1024 * 1024),
                ..VfsConfigRaw::default()
            }
            .validate()
            .unwrap(),
        )
        .unwrap();
        let vfs = fixture.vfs();
        let mut attr = vfs.create(1, b"pending.bin", 0, 0, 0o644).unwrap();
        attr.size = 16 * CHUNK_BYTES;
        let size = attr.size;
        let open_file = OpenFile::new(1, attr.ino, OpenFlags::RDONLY, Some(attr), false);
        open_file.ram_readahead().lock().enable(3, size);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        {
            let _entered = runtime.enter();
            vfs.schedule_ram_readahead(&open_file, 0, RAM_READAHEAD_CHUNK_BYTES);
            vfs.schedule_ram_readahead(&open_file, CHUNK_BYTES, RAM_READAHEAD_CHUNK_BYTES);
        }
        (fixture, open_file, runtime)
    }

    #[test]
    fn ram_readahead_cancel_before_first_poll_resolves_pending_slot() {
        let (fixture, open_file, runtime) = pending_chunks_fixture("ram-cancel-unpolled");
        let slot = open_file
            .ram_readahead()
            .lock()
            .slots_for(2 * CHUNK_BYTES, RAM_READAHEAD_CHUNK_BYTES)
            .unwrap()
            .remove(0);
        // The current-thread runtime was never driven, so none of its tasks ran.
        drop(runtime);
        let (data, _) = fixture.runtime().block_on(async {
            tokio::time::timeout(Duration::from_millis(100), slot.resolved())
                .await
                .expect("cancelling an unpolled chunk must unblock its reader")
        });
        assert!(data.is_none());
    }

    #[test]
    fn ram_readahead_seek_discards_existing_loaded_and_pending_slots() {
        let (fixture, open_file, runtime) = pending_chunks_fixture("ram-seek-pending");
        let slots = open_file
            .ram_readahead()
            .lock()
            .slots_for(2 * CHUNK_BYTES, 2 * RAM_READAHEAD_CHUNK_BYTES)
            .unwrap();
        slots[0]
            .state
            .send_replace(RamChunkState::Ready(Bytes::from_static(b"loaded chunk")));
        assert!(matches!(*slots[0].state.borrow(), RamChunkState::Ready(_)));
        assert!(matches!(*slots[1].state.borrow(), RamChunkState::Pending));
        {
            let _entered = runtime.enter();
            fixture
                .vfs()
                .schedule_ram_readahead(&open_file, 0, RAM_READAHEAD_CHUNK_BYTES);
        }
        assert!(matches!(*slots[0].state.borrow(), RamChunkState::Discarded));
        assert!(matches!(*slots[1].state.borrow(), RamChunkState::Discarded));
        assert!(open_file.ram_readahead().lock().chunks.is_empty());
    }

    #[test]
    fn ram_readahead_depth_holds_shared_limit_after_background_work() {
        let mib = 1024 * 1024;
        let runtime = RamReadAheadRuntime::new(32 * mib, 12 * mib);
        let now = Instant::now();
        assert_eq!(runtime.effective_depth_chunks(false, now), 8);
        assert_eq!(runtime.effective_depth_chunks(true, now), 3);
        assert_eq!(
            runtime.effective_depth_chunks(false, now + Duration::from_secs(1)),
            3
        );
        assert_eq!(
            runtime.effective_depth_chunks(false, now + RAM_READAHEAD_SHARED_HOLD),
            8
        );
    }

    #[test]
    fn ram_readahead_shared_depth_never_exceeds_idle_limit() {
        let runtime = RamReadAheadRuntime::new(8 * 1024 * 1024, 32 * 1024 * 1024);
        assert_eq!(runtime.effective_depth_chunks(true, Instant::now()), 2);
        let disabled = RamReadAheadRuntime::new(0, 12 * 1024 * 1024);
        assert_eq!(disabled.effective_depth_chunks(true, Instant::now()), 0);
    }

    #[test]
    fn ram_readahead_depth_rounds_up_to_at_least_two_chunks() {
        let mib = 1024 * 1024;
        assert_eq!(RamReadAheadRuntime::depth_chunks(0), 0);
        assert_eq!(RamReadAheadRuntime::depth_chunks(1), 2);
        assert_eq!(RamReadAheadRuntime::depth_chunks(4 * mib), 2);
        assert_eq!(RamReadAheadRuntime::depth_chunks(9 * mib), 3);
        assert_eq!(RamReadAheadRuntime::depth_chunks(12 * mib), 3);
        assert_eq!(RamReadAheadRuntime::depth_chunks(32 * mib), 8);
    }
}
