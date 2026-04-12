use super::*;
use futures::stream::{FuturesUnordered, StreamExt};

const ORDERED_PAYLOAD_PUT_PIPELINE_WIDTH: usize = 8;

#[derive(Debug, Clone)]
pub(super) struct PendingBlock {
    pub(super) data: BytesMut,
    pub(super) dirty: bool,
    pub(super) reserved_object_id: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct PendingDirtySlice {
    range: studiofs_meta::schema::SliceRange,
    block_index: u32,
    block_offset: u32,
    seq: u64,
}

#[derive(Debug, Clone)]
pub(super) struct PendingDirtyReadSpan {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) block_offset: u32,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
struct DirtyWriteSnapshot {
    block_index: u32,
    data: Bytes,
}

#[derive(Debug, Clone)]
struct PersistedWriteBlock {
    block_index: u32,
    object_id: u64,
    data: Bytes,
}

#[derive(Debug, Clone, Copy)]
struct PayloadPersistConfig {
    put_concurrency: usize,
    local_sync_policy: LocalSyncPolicy,
}

pub(super) struct LocalSyncGroupState {
    pub(super) pending_bytes: u64,
    pub(super) last_sync_at: Instant,
}

pub(super) struct WriteBufferState {
    pub(super) pending_blocks: Mutex<std::collections::HashMap<(Ino, u32), PendingBlock>>,
    pub(super) pending_block_indexes:
        Mutex<std::collections::HashMap<Ino, std::collections::BTreeSet<u32>>>,
    pending_chunk_slices: Mutex<
        std::collections::HashMap<Ino, std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>>,
    >,
    next_pending_slice_seq: AtomicU64,
    pub(super) pending_bytes: AtomicUsize,
    pub(super) write_backpressure_wait_ms: AtomicU64,
    pub(super) write_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_wait_ms: AtomicU64,
    pub(super) fast_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_stall_max_ms: AtomicU64,
    pub(super) local_sync_group: tokio::sync::Mutex<LocalSyncGroupState>,
    pub(super) local_sync_gate: tokio::sync::Mutex<()>,
}

impl WriteBufferState {
    pub(super) fn new() -> Self {
        Self {
            pending_blocks: Mutex::new(std::collections::HashMap::new()),
            pending_block_indexes: Mutex::new(std::collections::HashMap::new()),
            pending_chunk_slices: Mutex::new(std::collections::HashMap::new()),
            next_pending_slice_seq: AtomicU64::new(1),
            pending_bytes: AtomicUsize::new(0),
            write_backpressure_wait_ms: AtomicU64::new(0),
            write_backpressure_events: AtomicU64::new(0),
            fast_backpressure_wait_ms: AtomicU64::new(0),
            fast_backpressure_events: AtomicU64::new(0),
            fast_backpressure_stall_max_ms: AtomicU64::new(0),
            local_sync_group: tokio::sync::Mutex::new(LocalSyncGroupState {
                pending_bytes: 0,
                last_sync_at: Instant::now(),
            }),
            local_sync_gate: tokio::sync::Mutex::new(()),
        }
    }
}

fn assign_reserved_object_ids(
    pending: &mut std::collections::HashMap<(Ino, u32), PendingBlock>,
    ino: Ino,
    blocks: &mut [PersistedWriteBlock],
    fresh_object_ids: &[u64],
) {
    let mut fresh_ids = fresh_object_ids.iter().copied();
    for block in blocks {
        let key = (ino, block.block_index);
        if let Some(current) = pending.get_mut(&key)
            && current.data.as_ref() == block.data.as_ref()
        {
            if let Some(object_id) = current.reserved_object_id {
                block.object_id = object_id;
                continue;
            }

            let object_id = fresh_ids
                .next()
                .expect("missing fresh object id for write buffer block");
            current.reserved_object_id = Some(object_id);
            block.object_id = object_id;
            continue;
        }

        block.object_id = fresh_ids
            .next()
            .expect("missing fresh object id for write buffer block");
    }
}

fn blocks_can_accept_more(selected_bytes: usize, next_block_bytes: usize, limit: usize) -> bool {
    selected_bytes == 0 || selected_bytes.saturating_add(next_block_bytes) <= limit
}

async fn run_bounded_tasks<I, F, Fut, T, E>(
    items: I,
    concurrency: usize,
    mut make_future: F,
) -> Result<(), E>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut items = items.into_iter();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = concurrency.max(1);
    let mut first_error = None;

    while inflight.len() < max_inflight {
        let Some(item) = items.next() else {
            break;
        };
        inflight.push(make_future(item));
    }

    while let Some(result) = inflight.next().await {
        match result {
            Ok(_) => {
                if first_error.is_none()
                    && let Some(item) = items.next()
                {
                    inflight.push(make_future(item));
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn commit_persisted_block_state(
    pending: &mut std::collections::HashMap<(Ino, u32), PendingBlock>,
    ino: Ino,
    blocks: &[PersistedWriteBlock],
) -> (usize, Vec<u32>) {
    let mut removed_bytes = 0usize;
    let mut removed_block_indexes = Vec::new();
    for block in blocks {
        let key = (ino, block.block_index);
        let mut should_remove = false;
        if let Some(current) = pending.get_mut(&key) {
            if current.reserved_object_id == Some(block.object_id) {
                current.reserved_object_id = None;
            }
            should_remove = current.dirty && current.data.as_ref() == block.data.as_ref();
        }
        if should_remove && let Some(removed) = pending.remove(&key) {
            removed_bytes += removed.data.len();
            removed_block_indexes.push(block.block_index);
        }
    }
    (removed_bytes, removed_block_indexes)
}

impl StudioVfs {
    pub(super) fn total_pending_write_buffer_bytes(&self) -> usize {
        self.state
            .write_buffer
            .pending_bytes
            .load(Ordering::Relaxed)
    }

    fn add_pending_write_buffer_block_indexes<I>(&self, ino: Ino, block_indexes: I)
    where
        I: IntoIterator<Item = u32>,
    {
        let mut indexes = self.state.write_buffer.pending_block_indexes.lock();
        indexes.entry(ino).or_default().extend(block_indexes);
    }

    pub(super) fn has_pending_write_buffer_blocks_for_inode(&self, ino: Ino) -> bool {
        self.state
            .write_buffer
            .pending_block_indexes
            .lock()
            .get(&ino)
            .is_some_and(|indexes| !indexes.is_empty())
    }

    fn clear_matching_dirty_attr_if_committed(&self, ino: Ino, attr: &InodeAttr) {
        if self.has_pending_write_buffer_blocks_for_inode(ino) {
            return;
        }
        let mut dirty_attrs = self.state.namespace.dirty_attrs.lock();
        if let Some(current) = dirty_attrs.get(&ino)
            && current.content_epoch == attr.content_epoch
            && current.size == attr.size
            && current.mtime == attr.mtime
            && current.ctime == attr.ctime
        {
            dirty_attrs.remove(&ino);
        }
    }

    fn add_pending_write_buffer_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.state
            .write_buffer
            .pending_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn subtract_pending_write_buffer_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let previous = self
            .state
            .write_buffer
            .pending_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
        if previous < bytes {
            self.state
                .write_buffer
                .pending_bytes
                .store(0, Ordering::Relaxed);
            debug_assert!(
                previous >= bytes,
                "pending write-buffer bytes underflow: previous={previous}, subtract={bytes}"
            );
        }
    }

    fn replace_pending_write_buffer_block(&self, ino: Ino, block_index: u32, block: PendingBlock) {
        let mut pending = self.state.write_buffer.pending_blocks.lock();
        let mut indexes = self.state.write_buffer.pending_block_indexes.lock();
        let new_len = block.data.len();
        let old_len = pending
            .insert((ino, block_index), block)
            .map(|old| old.data.len())
            .unwrap_or(0);
        indexes.entry(ino).or_default().insert(block_index);
        drop(indexes);
        drop(pending);
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => self.add_pending_write_buffer_bytes(new_len - old_len),
            std::cmp::Ordering::Less => self.subtract_pending_write_buffer_bytes(old_len - new_len),
            std::cmp::Ordering::Equal => {}
        }
    }

    fn next_pending_slice_seq_batch(&self, count: usize) -> VfsResult<std::ops::Range<u64>> {
        let count_u64 = u64::try_from(count).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("pending dirty-slice batch too large: {}", count),
            ))
        })?;
        let next_seq = &self.state.write_buffer.next_pending_slice_seq;
        let mut start = next_seq.load(Ordering::Relaxed);
        loop {
            let end = start.checked_add(count_u64).ok_or_else(|| {
                VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "pending dirty-slice sequence overflow: start={}, count={}",
                        start, count_u64
                    ),
                ))
            })?;
            match next_seq.compare_exchange_weak(start, end, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Ok(start..end),
                Err(observed) => start = observed,
            }
        }
    }

    fn record_pending_dirty_slices_batch(
        &self,
        ino: Ino,
        slices: &[(
            studiofs_meta::schema::ChunkId,
            studiofs_meta::schema::SliceRange,
            u32,
            u32,
        )],
    ) -> VfsResult<()> {
        if slices.is_empty() {
            return Ok(());
        }
        let seqs = self.next_pending_slice_seq_batch(slices.len())?;
        let mut pending_chunk_slices = self.state.write_buffer.pending_chunk_slices.lock();
        let inode_chunks = pending_chunk_slices.entry(ino).or_default();
        for (seq, (chunk_id, range, block_index, block_offset)) in seqs.zip(slices.iter().copied())
        {
            inode_chunks
                .entry(chunk_id.0)
                .or_default()
                .push(PendingDirtySlice {
                    range,
                    block_index,
                    block_offset,
                    seq,
                });
        }
        Ok(())
    }

    fn clear_pending_dirty_slices_for_inode(&self, ino: Ino) {
        self.state
            .write_buffer
            .pending_chunk_slices
            .lock()
            .remove(&ino);
    }

    fn clear_pending_dirty_slices_for_block(&self, ino: Ino, block_index: u32) {
        let mut pending_chunk_slices = self.state.write_buffer.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| slice.block_index != block_index);
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
    }

    fn clear_pending_dirty_slices_for_blocks(&self, ino: Ino, block_indexes: &[u32]) {
        if block_indexes.is_empty() {
            return;
        }
        let block_index_set: std::collections::HashSet<u32> =
            block_indexes.iter().copied().collect();
        let mut pending_chunk_slices = self.state.write_buffer.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| !block_index_set.contains(&slice.block_index));
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
    }

    fn clear_pending_dirty_slices_exceeding_len(&self, ino: Ino, block_index: u32, new_len: usize) {
        let mut pending_chunk_slices = self.state.write_buffer.pending_chunk_slices.lock();
        let Some(chunks) = pending_chunk_slices.get_mut(&ino) else {
            return;
        };

        chunks.retain(|_, slices| {
            slices.retain_mut(|slice| {
                if slice.block_index != block_index {
                    return true;
                }
                let slice_len = slice.range.len() as usize;
                if slice.block_offset as usize >= new_len {
                    return false;
                }
                let allowed_len = new_len.saturating_sub(slice.block_offset as usize);
                if slice_len <= allowed_len {
                    return true;
                }
                let new_end = (slice.range.start as usize).saturating_add(allowed_len);
                if new_end <= slice.range.start as usize
                    || new_end > studiofs_meta::schema::DEFAULT_CHUNK_SIZE as usize
                {
                    return false;
                }
                slice.range.end = new_end as u32;
                true
            });
            !slices.is_empty()
        });

        if chunks.is_empty() {
            pending_chunk_slices.remove(&ino);
        }
    }

    pub(super) fn clear_pending_blocks_for_inode(&self, ino: Ino) {
        let mut removed_bytes = 0usize;
        let mut pending = self.state.write_buffer.pending_blocks.lock();
        let mut indexes = self.state.write_buffer.pending_block_indexes.lock();
        pending.retain(|(candidate, _), block| {
            if *candidate == ino {
                removed_bytes += block.data.len();
                return false;
            }
            true
        });
        indexes.remove(&ino);
        drop(indexes);
        drop(pending);
        self.subtract_pending_write_buffer_bytes(removed_bytes);
        self.clear_pending_dirty_slices_for_inode(ino);
    }

    async fn cleanup_uncommitted_block_store_entries(
        &self,
        ino: Ino,
        blocks: &[PersistedWriteBlock],
        stage: &'static str,
    ) {
        if blocks.is_empty() {
            return;
        }
        let object_ids: Vec<u64> = blocks.iter().map(|block| block.object_id).collect();
        if let Err(error) = self
            .block_store_remove_batch_async(object_ids.clone())
            .await
        {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "write-buffer cleanup of uncommitted block-store entries failed"
            );
        }
    }

    /// Phase 1 of the write-buffer drain: write block payloads to the local block
    /// store and perform grouped local sync.
    ///
    /// Timing is recorded via [`record_payload_persist_observability`] regardless
    /// of success or failure so that error-path latency is also visible in telemetry.
    /// Successful completions additionally contribute to payload burst cadence.
    ///
    /// [`record_payload_persist_observability`]: StudioVfs::record_payload_persist_observability
    async fn persist_payload_phase(
        &self,
        persisted_blocks: &[PersistedWriteBlock],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()> {
        let persisted_bytes: u64 = persisted_blocks.iter().map(|b| b.data.len() as u64).sum();
        let started = Instant::now();
        let result: VfsResult<()> = async {
            run_bounded_tasks(
                persisted_blocks.iter().cloned(),
                persist_config.put_concurrency,
                |block| async move {
                    self.block_store_put_async(block.object_id, block.data, false)
                        .await
                },
            )
            .await?;
            self.maybe_block_store_sync_data_async(
                persisted_bytes,
                persist_config.local_sync_policy,
            )
            .await?;
            Ok(())
        }
        .await;
        let completed_burst = result.is_ok();
        if persisted_bytes > 0 {
            self.record_payload_persist_observability(
                persisted_blocks.len(),
                persisted_bytes,
                started.elapsed(),
                completed_burst,
            );
        }
        result
    }

    /// Pure computation step between Phase 1 and Phase 2: build the overlay
    /// [`SliceRecord`] list from the freshly persisted blocks.
    ///
    /// When dirty-chunk-slice tracking is active this path validates per-write-slice
    /// invariants but still emits **one full-block `SliceRecord` per persisted
    /// block**. See the inline comment for the full rationale.
    ///
    /// [`SliceRecord`]: studiofs_meta::schema::SliceRecord
    fn build_overlay_slice_records(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedWriteBlock],
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) -> VfsResult<
        Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
    > {
        let mut chunks: std::collections::BTreeMap<u64, Vec<studiofs_meta::schema::SliceRecord>> =
            std::collections::BTreeMap::new();

        if !dirty_chunk_slices.is_empty() {
            // Dirty-chunk-slices path: validate per-write-slice invariants but
            // emit ONE full-block SliceRecord per persisted block.
            //
            // Previously this path emitted per-write-slice records with partial
            // BlockRef.offset/length pointing into the new S3 object.  That left
            // stale slices from *earlier* objects (with different object_ids) in
            // the overlay because the "fully_covered" pruning in
            // flush_regular_file_overlay_slices_with_attr_txn only removes a
            // prior slice when the incoming slice completely covers its chunk
            // range.  A partial write slice (e.g. 1MB..1.5MB) never fully covers
            // an old full-block slice (0..1MB), so both survive in the overlay.
            // synthesize_block_ref_from_resolved_spans then sees two spans with
            // different object_ids for block_index=0 and returns
            // FragmentedBlock, quarantining the inode and making all reads
            // return zeros.
            //
            // The fix: always write a single full-block SliceRecord whose
            // BlockRef references the NEW object from offset=0 with the full
            // persisted data length.  This guarantees the incoming slice fully
            // covers any previous slice for the same block and the overlay
            // stays consistent.
            let block_lengths_by_block_index: std::collections::HashMap<u32, usize> =
                persisted_blocks
                    .iter()
                    .map(|persisted| (persisted.block_index, persisted.data.len()))
                    .collect();
            for (_chunk_id, mut slices) in dirty_chunk_slices {
                slices.sort_by_key(|slice| slice.seq);
                for slice in slices {
                    let Some(&block_len) = block_lengths_by_block_index.get(&slice.block_index)
                    else {
                        continue;
                    };
                    let slice_len = slice.range.len();
                    let block_offset = slice.block_offset;
                    let block_end = (block_offset as usize).saturating_add(slice_len as usize);
                    if block_end > block_len {
                        return Err(crate::VfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!(
                                "dirty slice exceeds block length: ino={}, \
                                 block_index={}, block_len={}, block_offset={}, \
                                 slice_len={}",
                                ino, slice.block_index, block_len, block_offset, slice_len
                            ),
                        )));
                    }
                }
            }
        }

        // Write one full-block SliceRecord for every persisted block,
        // whether or not it had dirty-chunk-slice tracking entries.
        // The S3 object always contains the complete block payload from
        // offset 0, so BlockRef.offset=0 and BlockRef.length=data.len()
        // is the canonical (non-fragmented) representation.
        for persisted in persisted_blocks {
            let block_start = (persisted.block_index as u64) * (self.block_size as u64);
            let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64;
            let chunk_id = block_start / chunk_size;
            let chunk_start = chunk_id * chunk_size;
            let slice_start = (block_start - chunk_start) as u32;
            let slice_end = slice_start + (persisted.data.len() as u32);
            let slice_range = studiofs_meta::schema::SliceRange::new(slice_start, slice_end)
                .ok_or_else(|| {
                    crate::VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid slice range {}..{}", slice_start, slice_end),
                    ))
                })?;
            let mut flags = studiofs_meta::schema::BlockFlags::empty();
            if persisted.data.len() < self.block_size as usize {
                flags |= studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK;
            }
            chunks
                .entry(chunk_id)
                .or_default()
                .push(studiofs_meta::schema::SliceRecord {
                    slice_id: studiofs_meta::schema::SliceId::default(),
                    order: studiofs_meta::schema::SliceOrder::default(),
                    range: slice_range,
                    block: Some(studiofs_meta::schema::BlockRef {
                        object_id: persisted.object_id,
                        offset: 0,
                        length: persisted.data.len() as u32,
                        flags,
                    }),
                    block_state: studiofs_meta::schema::BlockRefState::LocalOnly,
                });
        }

        for slices in chunks.values_mut() {
            let mut next_slice_id = 1u64;
            let mut next_slice_order = 1u64;
            for slice in slices.iter_mut() {
                slice.slice_id = studiofs_meta::schema::SliceId(next_slice_id);
                slice.order = studiofs_meta::schema::SliceOrder(next_slice_order);
                next_slice_id = next_slice_id.saturating_add(1);
                next_slice_order = next_slice_order.saturating_add(1);
            }
        }

        Ok(chunks
            .into_iter()
            .map(|(k, v)| (studiofs_meta::schema::ChunkId(k), v))
            .collect())
    }

    /// Phase 2 of the write-buffer drain: commit overlay chunk slice records and
    /// optional inode attribute update to the local metadata engine in a single
    /// transaction.
    ///
    /// Timing is recorded via [`record_metadata_commit_observability`] regardless
    /// of success or failure. Successful commits additionally contribute to
    /// metadata-commit cadence.
    ///
    /// [`record_metadata_commit_observability`]: StudioVfs::record_metadata_commit_observability
    /// Commits overlay slice records and an optional inode attr update in a single
    /// metadata transaction, returning the engine-normalized committed attr (if any).
    ///
    /// The engine (e.g. redb) applies its own `transition_upload_state_for_local_mutation`
    /// with accurate `has_committed_manifest` information, so callers must NOT
    /// pre-transform the attr before passing it here.
    async fn commit_overlay_metadata_phase(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>> {
        let started = Instant::now();
        let result = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.flush_regular_file_overlay_slices_with_attr(ino, &chunk_slices, attr.as_ref())
        })
        .await;
        self.record_metadata_commit_observability(started.elapsed(), result.is_ok());
        result
    }

    async fn persist_write_buffer_blocks(
        &self,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<Vec<PersistedWriteBlock>> {
        if blocks.is_empty() {
            // No payload to persist; commit the attr-only metadata phase so the
            // engine applies its accurate transition (with has_committed_manifest).
            if attr_to_persist.is_some() {
                let committed_attr = self
                    .commit_overlay_metadata_phase(ino, Vec::new(), attr_to_persist)
                    .await?;
                if let Some(attr) = committed_attr {
                    self.state.namespace.attr_read_cache.lock().remove(ino);
                    self.refresh_inode_incarnation_state_from_attr(ino, &attr);
                }
            }
            return Ok(Vec::new());
        }

        let mut persisted_blocks: Vec<PersistedWriteBlock> = blocks
            .into_iter()
            .map(|block| PersistedWriteBlock {
                block_index: block.block_index,
                object_id: 0,
                data: block.data,
            })
            .collect();
        let missing_object_id_count = {
            let pending = self.state.write_buffer.pending_blocks.lock();
            persisted_blocks
                .iter()
                .filter(|block| {
                    pending
                        .get(&(ino, block.block_index))
                        .filter(|current| current.data.as_ref() == block.data.as_ref())
                        .and_then(|current| current.reserved_object_id)
                        .is_none()
                })
                .count() as u32
        };
        let fresh_object_ids = if missing_object_id_count == 0 {
            Vec::new()
        } else {
            run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                engine.next_block_ids(missing_object_id_count)
            })
            .await?
        };
        {
            let mut pending = self.state.write_buffer.pending_blocks.lock();
            assign_reserved_object_ids(&mut pending, ino, &mut persisted_blocks, &fresh_object_ids);
        }

        // Data hits the local backend before metadata. In durable mode this keeps
        // payload barriers on the foreground put path; in fast mode barriers are
        // intentionally grouped behind `sync_data()` cadence. In both modes, the
        // canonical retry source for failure remains the WriteBuffer snapshot
        // guarded by `reserved_object_id`.

        // === Phase 1: Payload Persistence ===
        // Write each block payload to the local block store and perform grouped
        // local sync. On failure, remove the block-store entries before returning.
        if let Err(error) = self
            .persist_payload_phase(&persisted_blocks, persist_config)
            .await
        {
            self.cleanup_uncommitted_block_store_entries(ino, &persisted_blocks, "payload-persist")
                .await;
            return Err(error);
        }

        // === Phase 2: Overlay Metadata Commit ===
        // Build overlay slice records from the persisted blocks, then commit them
        // together with the optional inode attr update in a single metadata
        // transaction. The engine normalises the attr (applying its accurate
        // has_committed_manifest check), so we pass the raw attr without a
        // VFS-level pre-transform. On failure, remove the block-store entries
        // written in Phase 1.
        let chunk_slices =
            match self.build_overlay_slice_records(ino, &persisted_blocks, dirty_chunk_slices) {
                Ok(slices) => slices,
                Err(error) => {
                    self.cleanup_uncommitted_block_store_entries(
                        ino,
                        &persisted_blocks,
                        "metadata-build",
                    )
                    .await;
                    return Err(error);
                }
            };
        let committed_attr = match self
            .commit_overlay_metadata_phase(ino, chunk_slices, attr_to_persist)
            .await
        {
            Ok(attr) => attr,
            Err(error) => {
                self.cleanup_uncommitted_block_store_entries(
                    ino,
                    &persisted_blocks,
                    "metadata-commit",
                )
                .await;
                return Err(error);
            }
        };

        let upload_ops: Vec<(u64, u64, u64, u64)> = persisted_blocks
            .iter()
            .map(|block| {
                (
                    ino,
                    (block.block_index as u64).saturating_mul(self.block_size as u64),
                    block.object_id,
                    content_epoch,
                )
            })
            .collect();

        let mut live_extents = Vec::with_capacity(persisted_blocks.len());
        for block in &persisted_blocks {
            live_extents.push((ino, block.block_index, block.object_id, content_epoch));
        }
        self.record_live_extents(&live_extents);

        if let Some(attr) = committed_attr {
            self.state.namespace.attr_read_cache.lock().remove(ino);
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        self.block_cache.submit_uploads_batch(&upload_ops);
        for (upload_ino, _, _, _) in &upload_ops {
            self.state.file_session.note_background_transition(
                *upload_ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }
        if !upload_ops.is_empty() {
            self.block_cache.flush_upload_journal().await;
        }
        Ok(persisted_blocks)
    }

    async fn apply_write_buffer_backpressure(&self, ino: Ino) -> VfsResult<()> {
        let started = Instant::now();
        let _ = self
            .flush_dirty_full_write_buffer_blocks(
                ino,
                1,
                false,
                None,
                LocalSyncPolicy::Opportunistic,
            )
            .await?;
        if self.total_pending_write_buffer_bytes() > self.max_dirty_bytes.load(Ordering::Acquire) {
            self.flush_ordered(ino, LocalSyncPolicy::Opportunistic)
                .await?;
        }
        let waited_ms = started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        if waited_ms > 0 {
            self.state
                .write_buffer
                .write_backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            self.state
                .write_buffer
                .write_backpressure_wait_ms
                .fetch_add(waited_ms, Ordering::Relaxed);
            self.state
                .write_buffer
                .fast_backpressure_events
                .fetch_add(1, Ordering::Relaxed);
            self.state
                .write_buffer
                .fast_backpressure_wait_ms
                .fetch_add(waited_ms, Ordering::Relaxed);
            self.state
                .write_buffer
                .fast_backpressure_stall_max_ms
                .fetch_max(waited_ms, Ordering::Relaxed);
        }
        Ok(())
    }

    pub(super) fn clear_pending_block(&self, ino: Ino, block_index: u32) {
        let mut pending = self.state.write_buffer.pending_blocks.lock();
        let mut indexes = self.state.write_buffer.pending_block_indexes.lock();
        if let Some(removed) = pending.remove(&(ino, block_index)) {
            if let Some(inode_indexes) = indexes.get_mut(&ino) {
                inode_indexes.remove(&block_index);
                if inode_indexes.is_empty() {
                    indexes.remove(&ino);
                }
            }
            drop(indexes);
            drop(pending);
            self.subtract_pending_write_buffer_bytes(removed.data.len());
            self.clear_pending_dirty_slices_for_block(ino, block_index);
        }
    }

    fn snapshot_dirty_write_buffer_blocks(&self, ino: Ino) -> Vec<DirtyWriteSnapshot> {
        // Point-in-time view of dirty local-visible payload. Writers may race
        // ahead after this snapshot; commit keeps newer bytes dirty so a later
        // flush retries with the fresh view instead of dropping updates.
        let pending = self.state.write_buffer.pending_blocks.lock();
        let indexes = self.state.write_buffer.pending_block_indexes.lock();
        indexes
            .get(&ino)
            .into_iter()
            .flat_map(|inode_indexes| inode_indexes.iter())
            .filter_map(|block_index| {
                pending
                    .get(&(ino, *block_index))
                    .filter(|pending| pending.dirty)
                    .map(|pending| DirtyWriteSnapshot {
                        block_index: *block_index,
                        data: pending.data.clone().freeze(),
                    })
            })
            .collect()
    }

    fn snapshot_dirty_full_write_buffer_blocks(
        &self,
        ino: Ino,
        max_drain_bytes: Option<usize>,
    ) -> (Vec<DirtyWriteSnapshot>, usize, usize) {
        let pending = self.state.write_buffer.pending_blocks.lock();
        let indexes = self.state.write_buffer.pending_block_indexes.lock();
        let mut total_available_bytes = 0usize;
        let mut selected_bytes = 0usize;
        let blocks: Vec<_> = indexes
            .get(&ino)
            .into_iter()
            .flat_map(|inode_indexes| inode_indexes.iter())
            .filter_map(|block_index| {
                let pending = pending.get(&(ino, *block_index))?;
                if !pending.dirty || pending.data.len() < self.block_size as usize {
                    return None;
                }
                total_available_bytes = total_available_bytes.saturating_add(pending.data.len());
                if let Some(limit) = max_drain_bytes
                    && !blocks_can_accept_more(selected_bytes, pending.data.len(), limit)
                {
                    return None;
                }
                let data = pending.data.clone().freeze();
                selected_bytes = selected_bytes.saturating_add(data.len());
                Some(DirtyWriteSnapshot {
                    block_index: *block_index,
                    data,
                })
            })
            .collect();
        (blocks, selected_bytes, total_available_bytes)
    }

    fn snapshot_dirty_write_buffer_chunk_slices(
        &self,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
    ) -> std::collections::BTreeMap<u64, Vec<PendingDirtySlice>> {
        let selected_block_indexes: std::collections::HashSet<u32> =
            blocks.iter().map(|block| block.block_index).collect();
        let block_lengths: std::collections::HashMap<u32, usize> = blocks
            .iter()
            .map(|block| (block.block_index, block.data.len()))
            .collect();
        if selected_block_indexes.is_empty() {
            return std::collections::BTreeMap::new();
        }

        let mut slices_by_chunk = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        if let Some(chunks) = self
            .state
            .write_buffer
            .pending_chunk_slices
            .lock()
            .get(&ino)
        {
            for (chunk_id, slices) in chunks {
                let filtered: Vec<_> = slices
                    .iter()
                    .copied()
                    .filter(|slice| {
                        selected_block_indexes.contains(&slice.block_index)
                            && block_lengths.get(&slice.block_index).is_some_and(|len| {
                                (slice.block_offset as usize)
                                    .saturating_add(slice.range.len() as usize)
                                    <= *len
                            })
                    })
                    .collect();
                debug_assert!(filtered.windows(2).all(|pair| pair[0].seq <= pair[1].seq));
                if !filtered.is_empty() {
                    slices_by_chunk.insert(*chunk_id, filtered);
                }
            }
        }
        slices_by_chunk
    }

    pub(super) fn snapshot_pending_dirty_slice_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<PendingDirtyReadSpan> {
        if length == 0 {
            return Vec::new();
        }
        let Some(end) = offset.checked_add(length) else {
            return Vec::new();
        };
        let pending_blocks = self.state.write_buffer.pending_blocks.lock();
        let pending_chunk_slices = self.state.write_buffer.pending_chunk_slices.lock();
        let Some(chunks) = pending_chunk_slices.get(&ino) else {
            return Vec::new();
        };
        let mut block_data = HashMap::<u32, Bytes>::new();
        let mut spans = Vec::new();
        for (chunk_id, slices) in chunks {
            let chunk_start = studiofs_meta::schema::chunk_start_offset(
                studiofs_meta::schema::ChunkId(*chunk_id),
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            for slice in slices {
                let data = if let Some(data) = block_data.get(&slice.block_index) {
                    data.clone()
                } else {
                    let Some(pending_block) = pending_blocks.get(&(ino, slice.block_index)) else {
                        continue;
                    };
                    let data = pending_block.data.clone().freeze();
                    block_data.insert(slice.block_index, data.clone());
                    data
                };
                let slice_start = chunk_start.saturating_add(slice.range.start as u64);
                let slice_end = chunk_start.saturating_add(slice.range.end as u64);
                let start = slice_start.max(offset);
                let stop = slice_end.min(end);
                if start >= stop {
                    continue;
                }
                let advance = start.saturating_sub(slice_start);
                let Some(advance_u32) = u32::try_from(advance).ok() else {
                    continue;
                };
                let Some(block_offset) = slice.block_offset.checked_add(advance_u32) else {
                    continue;
                };
                let span_len = (stop - start) as usize;
                let Some(block_offset_usize) = usize::try_from(block_offset).ok() else {
                    continue;
                };
                if block_offset_usize.saturating_add(span_len) > data.len() {
                    continue;
                }
                spans.push(PendingDirtyReadSpan {
                    start,
                    end: stop,
                    block_offset,
                    seq: slice.seq,
                    data,
                });
            }
        }
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    fn commit_persisted_write_buffer_blocks(&self, ino: Ino, blocks: &[PersistedWriteBlock]) {
        if blocks.is_empty() {
            return;
        }

        let mut pending = self.state.write_buffer.pending_blocks.lock();
        let mut indexes = self.state.write_buffer.pending_block_indexes.lock();
        let (removed_bytes, removed_block_indexes) =
            commit_persisted_block_state(&mut pending, ino, blocks);
        if let Some(inode_indexes) = indexes.get_mut(&ino) {
            for block_index in &removed_block_indexes {
                inode_indexes.remove(block_index);
            }
            if inode_indexes.is_empty() {
                indexes.remove(&ino);
            }
        }
        drop(indexes);
        drop(pending);
        self.subtract_pending_write_buffer_bytes(removed_bytes);
        self.clear_pending_dirty_slices_for_blocks(ino, &removed_block_indexes);
    }

    async fn flush_dirty_full_write_buffer_blocks(
        &self,
        ino: Ino,
        minimum_batch_bytes: usize,
        track_eager_flush_outcomes: bool,
        fallback_content_epoch: Option<u64>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        let (full_blocks, _full_block_batch_bytes, total_full_block_bytes) = self
            .snapshot_dirty_full_write_buffer_blocks(
                ino,
                Some(self.write_eager_flush_drain_chunk_bytes),
            );
        if full_blocks.is_empty() {
            if track_eager_flush_outcomes {
                self.note_eager_flush_outcome(EagerFlushOutcome::SkippedEmpty);
            }
            return Ok(false);
        }
        if total_full_block_bytes < minimum_batch_bytes {
            if track_eager_flush_outcomes {
                self.note_eager_flush_outcome(EagerFlushOutcome::SkippedBelowThreshold);
            }
            return Ok(false);
        }

        let dirty_attr = { self.state.namespace.dirty_attrs.lock().get(&ino).cloned() };
        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else if let Some(content_epoch) = fallback_content_epoch {
            content_epoch
        } else {
            self.current_content_epoch_async(ino).await?
        };
        let chunk_slices = self.snapshot_dirty_write_buffer_chunk_slices(ino, &full_blocks);
        let attr_to_clear = dirty_attr.clone();
        let persisted_blocks = self
            .persist_write_buffer_blocks(
                ino,
                full_blocks,
                chunk_slices,
                content_epoch,
                dirty_attr,
                PayloadPersistConfig {
                    put_concurrency: self.payload_put_concurrency,
                    local_sync_policy,
                },
            )
            .await?;
        self.commit_persisted_write_buffer_blocks(ino, &persisted_blocks);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.clear_matching_dirty_attr_if_committed(ino, attr);
        }
        if track_eager_flush_outcomes && !persisted_blocks.is_empty() {
            self.note_eager_flush_outcome(EagerFlushOutcome::Triggered);
        }
        Ok(!persisted_blocks.is_empty())
    }

    pub(super) fn trim_local_block_staging(
        &self,
        ino: Ino,
        block_index: u32,
        new_len: usize,
    ) -> VfsResult<bool> {
        let mut changed = false;

        {
            let mut pending = self.state.write_buffer.pending_blocks.lock();
            if let Some(block) = pending.get_mut(&(ino, block_index))
                && block.data.len() > new_len
            {
                let old_len = block.data.len();
                block.data.truncate(new_len);
                block.dirty = true;
                self.subtract_pending_write_buffer_bytes(old_len - block.data.len());
                changed = true;
            }
        }
        if changed {
            self.clear_pending_dirty_slices_exceeding_len(ino, block_index, new_len);
        }

        Ok(changed)
    }

    pub(super) fn load_local_block_bytes_sync(
        &self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) = self.read_pending_overlay_block_sync(
            ino,
            block_index,
            pending_content_epoch,
            pending_upload_index,
        )? {
            return Ok(Some(data));
        }
        Ok(self.local_block_backend.get(object_id))
    }

    pub(super) fn install_truncated_pending_block(
        &self,
        ino: Ino,
        block_index: u32,
        data: Bytes,
        new_len: usize,
    ) {
        let reserved_object_id = self
            .state
            .write_buffer
            .pending_blocks
            .lock()
            .get(&(ino, block_index))
            .and_then(|block| block.reserved_object_id);
        self.replace_pending_write_buffer_block(
            ino,
            block_index,
            PendingBlock {
                data: BytesMut::from(&data[..new_len]),
                dirty: true,
                reserved_object_id,
            },
        );
        self.clear_pending_dirty_slices_exceeding_len(ino, block_index, new_len);
    }

    async fn read_block_if_exists(
        &self,
        ino: Ino,
        block_index: u32,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) = self
            .read_pending_overlay_block_async(
                ino,
                block_index,
                pending_content_epoch,
                pending_upload_index,
            )
            .await?
        {
            return Ok(Some(data));
        }

        let block_ref = match self.get_file_block_async(ino, block_index).await? {
            Some(br) => br,
            None => return Ok(None),
        };

        if let Some(cached) = self.block_store_get_async(block_ref.object_id).await? {
            return Ok(Some(cached));
        }

        let data = self
            .fetch_block_for_ref(&block_ref, false)
            .await
            .map_err(|e| VfsError::Other(e.to_string()))?;

        Ok(Some(data))
    }

    pub(super) async fn write_ordered(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        let _ = self.state.read.observe_write_access(
            None,
            ino,
            offset,
            data.len(),
            self.access_classifier,
        );
        if self.ignored_entry_attr(ino).is_some() {
            let size = offset.saturating_add(data.len() as u64);
            let _ = self.update_ignored_entry(ino, |attr| {
                if size > attr.size {
                    attr.size = size;
                    attr.committed_size = size;
                }
                attr.touch_mtime();
            });
            return Ok(data.len() as u32);
        }

        if data.is_empty() {
            return Ok(0);
        }

        let initial_attr = self.getattr_async(ino).await?;
        let current_size = initial_attr.size;
        self.refresh_inode_incarnation_state_from_attr(ino, &initial_attr);
        // Keep ingress policy consistent with `ingress_pressure_for`: allow
        // the first dirty payload to land in RAM; start backpressure only when
        // dirty state already existed before this write.
        let pending_before_write = self.total_pending_write_buffer_bytes();
        let mut written = 0u64;
        let mut remaining = data.len();
        let mut current_offset = offset;
        let mut data_offset = 0;
        let mut has_full_blocks = false;
        let mut inserted_block_indexes = Vec::new();
        let mut pending_slices_batch: Vec<(
            studiofs_meta::schema::ChunkId,
            studiofs_meta::schema::SliceRange,
            u32,
            u32,
        )> = Vec::new();
        let pending_upload_index = self.snapshot_pending_upload_index();

        while remaining > 0 {
            let block_index = block_index_from_offset(current_offset, self.block_size);
            let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
            let space_in_block = self.block_size as usize - offset_in_block;
            let to_write = remaining.min(space_in_block);
            let write_start = current_offset;
            let write_end = current_offset.saturating_add(to_write as u64);

            let need_load = !self
                .state
                .write_buffer
                .pending_blocks
                .lock()
                .contains_key(&(ino, block_index));
            if need_load {
                let existing_data = match self
                    .read_block_if_exists(ino, block_index, Some(&pending_upload_index))
                    .await
                {
                    Ok(Some(d)) => d,
                    _ => Bytes::new(),
                };
                let logical_len = self.logical_block_len(current_size, block_index);
                let existing_data = if existing_data.len() > logical_len {
                    existing_data.slice(0..logical_len)
                } else {
                    existing_data
                };
                let (inserted_block, inserted_bytes) = {
                    let mut pending = self.state.write_buffer.pending_blocks.lock();
                    match pending.entry((ino, block_index)) {
                        std::collections::hash_map::Entry::Occupied(_) => (false, 0),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(PendingBlock {
                                data: BytesMut::from(existing_data.as_ref()),
                                dirty: false,
                                reserved_object_id: None,
                            });
                            (true, existing_data.len())
                        }
                    }
                };
                if inserted_block {
                    inserted_block_indexes.push(block_index);
                }
                self.add_pending_write_buffer_bytes(inserted_bytes);
            }

            {
                let mut pending = self.state.write_buffer.pending_blocks.lock();
                let pending_block = pending.get_mut(&(ino, block_index)).ok_or_else(|| {
                    VfsError::Io(std::io::Error::other(format!(
                        "pending write-buffer block missing during write: ino={}, block_index={}",
                        ino, block_index
                    )))
                })?;
                self.remove_live_extent(ino, block_index);

                let end_pos = offset_in_block + to_write;
                if pending_block.data.len() < end_pos {
                    let old_len = pending_block.data.len();
                    pending_block.data.resize(end_pos, 0);
                    self.add_pending_write_buffer_bytes(pending_block.data.len() - old_len);
                }

                pending_block.data[offset_in_block..end_pos]
                    .copy_from_slice(&data[data_offset..data_offset + to_write]);
                pending_block.dirty = true;

                if pending_block.data.len() >= self.block_size as usize {
                    has_full_blocks = true;
                }
            }
            let mut slice_cursor = write_start;
            while slice_cursor < write_end {
                let chunk_id = studiofs_meta::schema::chunk_id_from_offset(
                    slice_cursor,
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                );
                let chunk_start = studiofs_meta::schema::chunk_start_offset(
                    chunk_id,
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                );
                let chunk_end = chunk_start + studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64;
                let slice_file_end = write_end.min(chunk_end);
                let slice_start = studiofs_meta::schema::offset_within_chunk(
                    slice_cursor,
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                );
                let mut slice_end = studiofs_meta::schema::offset_within_chunk(
                    slice_file_end,
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                );
                if slice_end == 0 && slice_file_end > slice_cursor {
                    slice_end = studiofs_meta::schema::DEFAULT_CHUNK_SIZE;
                }
                let range =
                    studiofs_meta::schema::SliceRange::new(slice_start, slice_end).ok_or_else(
                        || {
                            VfsError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!(
                                    "invalid dirty slice range during write: ino={}, offset={}..{}, chunk_id={}, range={}..{}",
                                    ino, slice_cursor, slice_file_end, chunk_id.0, slice_start, slice_end
                                ),
                            ))
                        },
                    )?;
                let slice_block_offset = (offset_in_block as u64) + (slice_cursor - write_start);
                let slice_block_offset = u32::try_from(slice_block_offset).map_err(|_| {
                    VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "dirty slice block offset overflow: ino={}, block_index={}, offset={}",
                            ino, block_index, slice_block_offset
                        ),
                    ))
                })?;
                pending_slices_batch.push((chunk_id, range, block_index, slice_block_offset));
                slice_cursor = slice_file_end;
            }

            written += to_write as u64;
            remaining -= to_write;
            current_offset += to_write as u64;
            data_offset += to_write;
        }
        if !inserted_block_indexes.is_empty() {
            self.add_pending_write_buffer_block_indexes(ino, inserted_block_indexes);
        }
        self.record_pending_dirty_slices_batch(ino, &pending_slices_batch)?;

        let mut base_attr = self.getattr_async(ino).await?;
        self.begin_pending_incarnation(ino, &mut base_attr);
        {
            let mut dirty = self.state.namespace.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
            attr.content_epoch = base_attr.content_epoch;
            if current_offset > attr.size {
                attr.size = current_offset;
                attr.touch_mtime();
            }
            self.update_pending_incarnation_size(ino, attr.size);
        }

        if has_full_blocks
            && !self
                .flush_dirty_full_write_buffer_blocks(
                    ino,
                    self.write_eager_flush_batch_bytes,
                    true,
                    Some(base_attr.content_epoch),
                    LocalSyncPolicy::Opportunistic,
                )
                .await?
        {
            let pending_after_write = self.total_pending_write_buffer_bytes();
            if pending_before_write > 0
                && pending_after_write > self.max_dirty_bytes.load(Ordering::Acquire)
            {
                self.apply_write_buffer_backpressure(ino).await?;
            }
            return Ok(written as u32);
        }

        let pending_after_write = self.total_pending_write_buffer_bytes();
        if pending_before_write > 0
            && pending_after_write > self.max_dirty_bytes.load(Ordering::Acquire)
        {
            self.apply_write_buffer_backpressure(ino).await?;
        }

        Ok(written as u32)
    }

    pub(super) async fn flush_ordered(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        self.state
            .metrics
            .flush_ordered_calls
            .fetch_add(1, Ordering::Relaxed);
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if self.ignored_entry_attr(ino).is_some() {
            return Ok(());
        }

        let blocks_to_buffer = self.snapshot_dirty_write_buffer_blocks(ino);
        let dirty_attr = { self.state.namespace.dirty_attrs.lock().get(&ino).cloned() };
        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else {
            self.current_content_epoch_async(ino).await?
        };
        let chunk_slices = self.snapshot_dirty_write_buffer_chunk_slices(ino, &blocks_to_buffer);
        let attr_to_clear = dirty_attr.clone();
        let persisted_blocks = self
            .persist_write_buffer_blocks(
                ino,
                blocks_to_buffer,
                chunk_slices,
                content_epoch,
                dirty_attr,
                PayloadPersistConfig {
                    put_concurrency: ORDERED_PAYLOAD_PUT_PIPELINE_WIDTH,
                    local_sync_policy,
                },
            )
            .await?;
        self.commit_persisted_write_buffer_blocks(ino, &persisted_blocks);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.clear_matching_dirty_attr_if_committed(ino, attr);
        }

        Ok(())
    }

    pub(super) async fn flush_pending_blocks_for_shutdown(&self) {
        let mut inos: BTreeSet<Ino> = {
            let pending = self.state.write_buffer.pending_blocks.lock();
            pending.keys().map(|(ino, _)| *ino).collect()
        };
        {
            let dirty_attrs = self.state.namespace.dirty_attrs.lock();
            inos.extend(dirty_attrs.keys().copied());
        }
        if inos.is_empty() {
            return;
        }

        info!(
            inode_count = inos.len(),
            "VFS shutdown: flushing buffered inodes"
        );
        for ino in inos {
            if let Err(e) = self.flush(ino).await {
                warn!(ino, error = %e, "VFS shutdown: flush inode failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    #[test]
    fn assign_reserved_object_ids_reuses_existing_ids_and_stores_new_ones() {
        let ino = 42;
        let mut pending = std::collections::HashMap::from([
            (
                (ino, 0),
                PendingBlock {
                    data: BytesMut::from(&b"alpha"[..]),
                    dirty: true,
                    reserved_object_id: Some(111),
                },
            ),
            (
                (ino, 1),
                PendingBlock {
                    data: BytesMut::from(&b"beta"[..]),
                    dirty: true,
                    reserved_object_id: None,
                },
            ),
        ]);
        let mut blocks = vec![
            PersistedWriteBlock {
                block_index: 0,
                object_id: 0,
                data: Bytes::from_static(b"alpha"),
            },
            PersistedWriteBlock {
                block_index: 1,
                object_id: 0,
                data: Bytes::from_static(b"beta"),
            },
        ];

        assign_reserved_object_ids(&mut pending, ino, &mut blocks, &[222]);

        assert_eq!(blocks[0].object_id, 111);
        assert_eq!(blocks[1].object_id, 222);
        assert_eq!(
            pending.get(&(ino, 0)).unwrap().reserved_object_id,
            Some(111)
        );
        assert_eq!(
            pending.get(&(ino, 1)).unwrap().reserved_object_id,
            Some(222)
        );
    }

    #[test]
    fn commit_persisted_block_state_clears_reserved_id_without_dropping_newer_bytes() {
        let ino = 7;
        let mut pending = std::collections::HashMap::from([(
            (ino, 3),
            PendingBlock {
                data: BytesMut::from(&b"newer"[..]),
                dirty: true,
                reserved_object_id: Some(333),
            },
        )]);
        let blocks = vec![PersistedWriteBlock {
            block_index: 3,
            object_id: 333,
            data: Bytes::from_static(b"older"),
        }];

        let (removed_bytes, removed_block_indexes) =
            commit_persisted_block_state(&mut pending, ino, &blocks);

        assert_eq!(removed_bytes, 0);
        assert!(removed_block_indexes.is_empty());
        let pending_block = pending.get(&(ino, 3)).unwrap();
        assert_eq!(pending_block.data.as_ref(), b"newer");
        assert_eq!(pending_block.reserved_object_id, None);
    }

    #[test]
    fn run_bounded_tasks_stops_starting_new_work_after_first_error() {
        let third_started = Arc::new(AtomicBool::new(false));
        let third_started_flag = third_started.clone();
        let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(Some(release_rx)));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            let _ = release_tx.send(());
        });

        let result = futures::executor::block_on(run_bounded_tasks([0u8, 1, 2], 2, move |item| {
            let third_started_flag = third_started_flag.clone();
            let release_rx = release_rx.clone();
            async move {
                match item {
                    0 => Err("first failure"),
                    1 => {
                        let rx = release_rx
                            .lock()
                            .expect("release receiver lock")
                            .take()
                            .expect("release receiver only used once");
                        let _ = rx.await;
                        Ok::<(), &'static str>(())
                    }
                    2 => {
                        third_started_flag.store(true, Ordering::Release);
                        Ok::<(), &'static str>(())
                    }
                    _ => Ok::<(), &'static str>(()),
                }
            }
        }));

        assert_eq!(result, Err("first failure"));
        assert!(
            !third_started.load(Ordering::Acquire),
            "the helper should drain already-started work after the first error without launching more tasks"
        );
    }
}
