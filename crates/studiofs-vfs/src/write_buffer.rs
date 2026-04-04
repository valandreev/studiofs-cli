use super::*;

const BLOCK_STORE_PUT_PIPELINE_WIDTH: usize = 8;

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

    async fn persist_write_buffer_blocks(
        &self,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
    ) -> VfsResult<Vec<PersistedWriteBlock>> {
        if blocks.is_empty() {
            if let Some(mut attr) = attr_to_persist {
                Self::transition_upload_state_for_local_mutation(&mut attr);
                let meta = self.meta_async.clone();
                let attr_for_meta = attr.clone();
                run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.set_attr(&attr_for_meta)
                })
                .await?;
                self.state.namespace.attr_read_cache.lock().remove(ino);
                self.refresh_inode_incarnation_state_from_attr(ino, &attr);
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
            run_meta(
                self.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| engine.next_block_ids(missing_object_id_count),
            )
            .await?
        };
        {
            let mut pending = self.state.write_buffer.pending_blocks.lock();
            assign_reserved_object_ids(&mut pending, ino, &mut persisted_blocks, &fresh_object_ids);
        }

        // Data hits BlockStore before metadata so a successful flush/fsync only
        // returns after local bytes are durable. If any pre-metadata step fails,
        // remove the cache entries again: the canonical retry source remains the
        // WriteBuffer snapshot guarded by `reserved_object_id`.
        let mut attr_for_meta = attr_to_persist.clone();
        if let Some(attr) = attr_for_meta.as_mut() {
            Self::transition_upload_state_for_local_mutation(attr);
        }
        let attr_after_commit = attr_for_meta.clone();

        let pre_metadata_result: VfsResult<()> = async {
            for chunk in persisted_blocks.chunks(BLOCK_STORE_PUT_PIPELINE_WIDTH) {
                let put_results = futures::future::join_all(chunk.iter().map(|block| {
                    self.block_store_put_async(block.object_id, block.data.clone(), false)
                }))
                .await;
                for result in put_results {
                    result?;
                }
            }
            self.block_store_sync_data_async().await?;

            let mut chunks: std::collections::BTreeMap<u64, Vec<studiofs_meta::schema::SliceRecord>> =
                std::collections::BTreeMap::new();
            let object_ids_by_block_index: std::collections::HashMap<u32, u64> = persisted_blocks
                .iter()
                .map(|persisted| (persisted.block_index, persisted.object_id))
                .collect();
            let block_lengths_by_block_index: std::collections::HashMap<u32, usize> = persisted_blocks
                .iter()
                .map(|persisted| (persisted.block_index, persisted.data.len()))
                .collect();

            if dirty_chunk_slices.is_empty() {
                for persisted in &persisted_blocks {
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
                    chunks.entry(chunk_id).or_default().push(studiofs_meta::schema::SliceRecord {
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
            } else {
                let mut covered_blocks = std::collections::HashSet::new();
                for (chunk_id, mut slices) in dirty_chunk_slices {
                    slices.sort_by_key(|slice| slice.seq);
                    for slice in slices {
                        let Some(&object_id) = object_ids_by_block_index.get(&slice.block_index) else {
                            continue;
                        };
                        let Some(&block_len) = block_lengths_by_block_index.get(&slice.block_index) else {
                            continue;
                        };
                        let slice_len = slice.range.len();
                        let block_offset = slice.block_offset;
                        let block_end = (block_offset as usize).saturating_add(slice_len as usize);
                        if block_end > block_len {
                            return Err(crate::VfsError::Io(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!(
                                    "dirty slice exceeds block length: ino={}, block_index={}, block_len={}, block_offset={}, slice_len={}",
                                    ino, slice.block_index, block_len, block_offset, slice_len
                                ),
                            )));
                        }
                        covered_blocks.insert(slice.block_index);
                        let mut flags = studiofs_meta::schema::BlockFlags::empty();
                        if block_len < self.block_size as usize {
                            flags |= studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK;
                        }
                        chunks.entry(chunk_id).or_default().push(studiofs_meta::schema::SliceRecord {
                            slice_id: studiofs_meta::schema::SliceId::default(),
                            order: studiofs_meta::schema::SliceOrder::default(),
                            range: slice.range,
                            block: Some(studiofs_meta::schema::BlockRef {
                                object_id,
                                offset: block_offset,
                                length: slice_len,
                                flags,
                            }),
                            block_state: studiofs_meta::schema::BlockRefState::LocalOnly,
                        });
                    }
                }
                for persisted in &persisted_blocks {
                    if covered_blocks.contains(&persisted.block_index) {
                        continue;
                    }
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
                    chunks.entry(chunk_id).or_default().push(studiofs_meta::schema::SliceRecord {
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

            let chunk_slices: Vec<(
                studiofs_meta::schema::ChunkId,
                Vec<studiofs_meta::schema::SliceRecord>,
            )> = chunks
                .into_iter()
                .map(|(k, v)| (studiofs_meta::schema::ChunkId(k), v))
                .collect();

            run_meta(
                self.meta_async.clone(),
                move |engine: Arc<dyn MetaEngine>| {
                    engine.flush_regular_file_overlay_slices_with_attr(
                        ino,
                        &chunk_slices,
                        attr_for_meta.as_ref(),
                    )
                },
            )
            .await?;
            Ok(())
        }
        .await;
        if let Err(error) = pre_metadata_result {
            self.cleanup_uncommitted_block_store_entries(ino, &persisted_blocks, "pre-metadata")
                .await;
            return Err(error);
        }

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

        for block in &persisted_blocks {
            self.block_cache
                .stage_l1(block.object_id, block.data.clone());
            self.record_live_extent(ino, block.block_index, block.object_id, content_epoch);
        }

        if let Some(attr) = attr_after_commit {
            self.state.namespace.attr_read_cache.lock().remove(ino);
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        self.block_cache.submit_uploads_batch(&upload_ops);
        for (upload_ino, _, _, _) in &upload_ops {
            self.note_file_session_background_transition(
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
            .flush_dirty_full_write_buffer_blocks(ino, 1, None)
            .await?;
        if self.total_pending_write_buffer_bytes() > self.max_dirty_bytes.load(Ordering::Acquire) {
            self.flush_ordered(ino).await?;
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
    ) -> (Vec<DirtyWriteSnapshot>, usize) {
        let pending = self.state.write_buffer.pending_blocks.lock();
        let indexes = self.state.write_buffer.pending_block_indexes.lock();
        let mut total_bytes = 0usize;
        let blocks: Vec<_> = indexes
            .get(&ino)
            .into_iter()
            .flat_map(|inode_indexes| inode_indexes.iter())
            .filter_map(|block_index| {
                let pending = pending.get(&(ino, *block_index))?;
                if !pending.dirty || pending.data.len() < self.block_size as usize {
                    return None;
                }
                let data = pending.data.clone().freeze();
                total_bytes += data.len();
                Some(DirtyWriteSnapshot {
                    block_index: *block_index,
                    data,
                })
            })
            .collect();
        (blocks, total_bytes)
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
        fallback_content_epoch: Option<u64>,
    ) -> VfsResult<bool> {
        let (full_blocks, full_block_batch_bytes) =
            self.snapshot_dirty_full_write_buffer_blocks(ino);
        if full_blocks.is_empty() || full_block_batch_bytes < minimum_batch_bytes {
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
            .persist_write_buffer_blocks(ino, full_blocks, chunk_slices, content_epoch, dirty_attr)
            .await?;
        self.commit_persisted_write_buffer_blocks(ino, &persisted_blocks);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.clear_matching_dirty_attr_if_committed(ino, attr);
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
    ) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) =
            self.read_pending_overlay_block_sync(ino, block_index, pending_content_epoch)?
        {
            return Ok(Some(data));
        }
        Ok(self.block_store.get(object_id))
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

    async fn read_block_if_exists(&self, ino: Ino, block_index: u32) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) = self
            .read_pending_overlay_block_async(ino, block_index, pending_content_epoch)
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

        let keys = format_object_fragment_keys(&self.data_prefix, block_ref.object_id);
        let data = self
            .fetch_scheduler
            .fetch_block(block_ref.object_id, &keys)
            .await
            .map_err(|e| VfsError::Other(e.to_string()))?;

        Ok(Some(data))
    }

    pub(super) async fn write_ordered(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        let _ = self.observe_write_access(None, ino, offset, data.len());
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
                let existing_data = match self.read_block_if_exists(ino, block_index).await {
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
                    Some(base_attr.content_epoch),
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

    pub(super) async fn flush_ordered(&self, ino: Ino) -> VfsResult<()> {
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
}
