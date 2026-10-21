//! Sequential RAM read-ahead for handles that get no kernel read-ahead.
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

use bytes::Bytes;
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

/// Default per-handle read-ahead depth. Only macFUSE opts handles in today.
///
/// Read-ahead competes with background write-back and upload rereads for the
/// cache disk. On the reference USB disk, 12 MiB raised a 16 GB F_NOCACHE read
/// by about 50% while the upload kept 40-70% of its rate; 32 MiB read faster
/// but stalled the upload until the read ended.
pub(super) const DEFAULT_SEQUENTIAL_RAM_READAHEAD_BYTES: usize = if cfg!(target_os = "macos") {
    12 * 1024 * 1024
} else {
    0
};

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
    /// Chunks kept ahead of the stream; zero means the handle did not opt in.
    depth_chunks: u64,
    generation: u64,
    last_end: Option<u64>,
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
    /// Chunks not scheduled because the mount-wide budget was exhausted.
    pub budget_skips: u64,
}

/// Mount-wide RAM read-ahead configuration, memory budget, and counters.
#[derive(Debug)]
pub(super) struct RamReadAheadRuntime {
    depth_chunks: u64,
    budget: Arc<Semaphore>,
    buffers: BufferPool,
    counters: RamReadAheadCounters,
}

impl RamReadAheadRuntime {
    pub(super) fn new(depth_bytes: usize) -> Self {
        let budget_chunks = RAM_READAHEAD_BUDGET_BYTES / RAM_READAHEAD_CHUNK_BYTES;
        Self {
            depth_chunks: Self::depth_chunks(depth_bytes),
            budget: Arc::new(Semaphore::new(budget_chunks)),
            buffers: BufferPool::new(RAM_READAHEAD_CHUNK_BYTES, budget_chunks),
            counters: RamReadAheadCounters::default(),
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
        self.handle_table.get(fh).filter(|open_file| {
            open_file.ino == ino && open_file.ram_readahead().lock().is_enabled()
        })
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
        let mut launches = Vec::new();
        {
            let mut state = open_file.ram_readahead().lock();
            if !state.is_enabled() {
                return;
            }
            let end = offset.saturating_add(read as u64);
            if state.last_end == Some(offset) {
                state.streak = state.streak.saturating_add(1);
            } else {
                if state.last_end.is_some() {
                    state.discard_all();
                }
                state.streak = 1;
            }
            state.last_end = Some(end);
            while state
                .chunks
                .front()
                .is_some_and(|chunk| (chunk.index + 1) * CHUNK_BYTES <= end)
            {
                state.chunks.pop_front();
            }
            if read == 0 || state.streak < RAM_READAHEAD_MIN_STREAK {
                return;
            }
            let first = end / CHUNK_BYTES;
            for index in first..first.saturating_add(state.depth_chunks) {
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
            self.spawn_background_task(async move {
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
        let _resolve = PendingChunkGuard(Arc::clone(&slot));
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
                state.file_size = file_size;
                let installed = state.install(index, &slot, generation, data);
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
