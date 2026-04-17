use super::*;
use futures::stream::{FuturesUnordered, StreamExt};

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
pub(super) struct DirtyWriteSnapshot {
    pub(super) block_index: u32,
    pub(super) data: Bytes,
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

trait BlockIdAllocatorPort {
    async fn next_block_ids(&self, count: u32) -> VfsResult<Vec<u64>>;
}

trait LocalBlockWriterPort {
    async fn persist_payload(
        &self,
        persisted_blocks: &[PersistedWriteBlock],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()>;

    async fn cleanup_uncommitted(
        &self,
        ino: Ino,
        blocks: &[PersistedWriteBlock],
        stage: &'static str,
    );
}

trait OverlayCommitPort {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>>;
}

trait UploadSubmissionPort {
    // Upload publication is a post-commit step. Implementations must absorb
    // their own durability / retry mechanics so the drain core does not expose
    // a fallible post-commit boundary.
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedWriteBlock],
        content_epoch: u64,
    );
}

trait DrainCommitObserverPort {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>);
}

struct LocalDirtyDrainCore<L, C, B, U, O> {
    local_blocks: L,
    commit_meta: C,
    block_ids: B,
    uploads: U,
    observer: O,
    block_size: u32,
    put_concurrency: usize,
}

type LocalDirtyDrain<'a> = LocalDirtyDrainCore<
    VfsLocalBlockWriter<'a>,
    VfsOverlayCommitter<'a>,
    VfsBlockIdAllocator<'a>,
    VfsUploadSubmitter<'a>,
    VfsDrainCommitObserver<'a>,
>;

struct LocalDirtyFlushSession<'a> {
    ino: Ino,
    vfs: &'a StudioVfs,
    drain: LocalDirtyDrain<'a>,
}

#[derive(Debug)]
struct DrainOutcome {
    persisted_blocks: Vec<PersistedWriteBlock>,
}

#[derive(Clone, Copy)]
struct VfsLocalBlockWriter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsOverlayCommitter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsBlockIdAllocator<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsUploadSubmitter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsDrainCommitObserver<'a> {
    vfs: &'a StudioVfs,
}

pub(super) struct LocalSyncGroupState {
    pub(super) pending_bytes: u64,
    pub(super) last_sync_at: Instant,
}

pub(super) struct LocalDirtyState {
    pub(super) pending_blocks: Mutex<std::collections::HashMap<(Ino, u32), PendingBlock>>,
    pub(super) pending_block_indexes:
        Mutex<std::collections::HashMap<Ino, std::collections::BTreeSet<u32>>>,
    pending_chunk_slices: Mutex<
        std::collections::HashMap<Ino, std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>>,
    >,
    pub(super) pending_bytes: AtomicUsize,
    pub(super) write_backpressure_wait_ms: AtomicU64,
    pub(super) write_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_wait_ms: AtomicU64,
    pub(super) fast_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_stall_max_ms: AtomicU64,
    pub(super) local_sync_group: tokio::sync::Mutex<LocalSyncGroupState>,
    pub(super) local_sync_gate: tokio::sync::Mutex<()>,
}

impl LocalDirtyState {
    pub(super) fn new() -> Self {
        Self {
            pending_blocks: Mutex::new(std::collections::HashMap::new()),
            pending_block_indexes: Mutex::new(std::collections::HashMap::new()),
            pending_chunk_slices: Mutex::new(std::collections::HashMap::new()),
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

#[cfg(test)]
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
                .expect("missing fresh object id for local dirty block");
            current.reserved_object_id = Some(object_id);
            block.object_id = object_id;
            continue;
        }

        block.object_id = fresh_ids
            .next()
            .expect("missing fresh object id for local dirty block");
    }
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

impl<L, C, B, U, O> LocalDirtyDrainCore<L, C, B, U, O>
where
    L: LocalBlockWriterPort,
    C: OverlayCommitPort,
    B: BlockIdAllocatorPort,
    U: UploadSubmissionPort,
    O: DrainCommitObserverPort,
{
    async fn allocate_block_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        if count == 0 {
            Ok(Vec::new())
        } else {
            self.block_ids.next_block_ids(count).await
        }
    }

    fn additional_object_id_count_needed(
        pending: &std::collections::HashMap<(Ino, u32), PendingBlock>,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
    ) -> usize {
        blocks
            .iter()
            .filter(|block| {
                pending
                    .get(&(ino, block.block_index))
                    .filter(|current| current.data.as_ref() == block.data.as_ref())
                    .and_then(|current| current.reserved_object_id)
                    .is_none()
            })
            .count()
    }

    fn try_prepare_persisted_blocks(
        &self,
        pending: &mut std::collections::HashMap<(Ino, u32), PendingBlock>,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
        fresh_object_ids: &mut std::collections::VecDeque<u64>,
    ) -> Result<Vec<PersistedWriteBlock>, usize> {
        let fresh_needed = Self::additional_object_id_count_needed(pending, ino, blocks);
        let additional_needed = fresh_needed.saturating_sub(fresh_object_ids.len());
        if additional_needed > 0 {
            return Err(additional_needed);
        }

        let mut persisted_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            let key = (ino, block.block_index);
            if let Some(current) = pending.get_mut(&key)
                && current.data.as_ref() == block.data.as_ref()
            {
                let object_id = if let Some(object_id) = current.reserved_object_id {
                    object_id
                } else {
                    let object_id = fresh_object_ids
                        .pop_front()
                        .expect("fresh object ids prevalidated");
                    current.reserved_object_id = Some(object_id);
                    object_id
                };
                persisted_blocks.push(PersistedWriteBlock {
                    block_index: block.block_index,
                    object_id,
                    data: block.data.clone(),
                });
                continue;
            }

            let object_id = fresh_object_ids
                .pop_front()
                .expect("fresh object ids prevalidated");
            persisted_blocks.push(PersistedWriteBlock {
                block_index: block.block_index,
                object_id,
                data: block.data.clone(),
            });
        }

        Ok(persisted_blocks)
    }

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
            // Dirty-chunk-slices path: validate per-write-slice invariants, then
            // emit chunk-aware SliceRecords per persisted block.
            //
            // Previously this path emitted per-write-slice records keyed to the
            // dirty-write geometry, which could leave stale slices from older
            // objects in the overlay. Emitting records that cover the committed
            // block geometry fixes that. When a full block straddles a 64MiB
            // chunk boundary, we still need to split it into chunk-local slice
            // records whose BlockRef offsets remain contiguous within the new
            // object, so redb can reconstruct the logical block without raising
            // FragmentedBlock.
            //
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

        for persisted in persisted_blocks {
            self.append_persisted_block_slice_records(&mut chunks, persisted)?;
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

    fn append_persisted_block_slice_records(
        &self,
        chunks: &mut std::collections::BTreeMap<u64, Vec<studiofs_meta::schema::SliceRecord>>,
        persisted: &PersistedWriteBlock,
    ) -> VfsResult<()> {
        let block_start = (persisted.block_index as u64).saturating_mul(self.block_size as u64);
        let block_len = u64::try_from(persisted.data.len()).map_err(|_| {
            crate::VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "persisted block {} length {} exceeds u64::MAX",
                    persisted.block_index,
                    persisted.data.len()
                ),
            ))
        })?;
        let block_end = block_start.checked_add(block_len).ok_or_else(|| {
            crate::VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "persisted block {} end overflows: start={}, len={}",
                    persisted.block_index, block_start, block_len
                ),
            ))
        })?;
        let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64;
        let mut file_pos = block_start;
        let mut flags = studiofs_meta::schema::BlockFlags::empty();
        if persisted.data.len() < self.block_size as usize {
            flags |= studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK;
        }

        while file_pos < block_end {
            let chunk_id = file_pos / chunk_size;
            let chunk_start = chunk_id * chunk_size;
            let chunk_offset = file_pos - chunk_start;
            let remaining_in_block = block_end - file_pos;
            let remaining_in_chunk = chunk_size - chunk_offset;
            let slice_len = remaining_in_block.min(remaining_in_chunk);
            let block_offset = file_pos - block_start;
            let slice_start = u32::try_from(chunk_offset).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "chunk offset {} for block {} exceeds u32::MAX",
                        chunk_offset, persisted.block_index
                    ),
                ))
            })?;
            let slice_len_u32 = u32::try_from(slice_len).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "slice length {} for block {} exceeds u32::MAX",
                        slice_len, persisted.block_index
                    ),
                ))
            })?;
            let slice_end = slice_start.checked_add(slice_len_u32).ok_or_else(|| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "slice end overflow for block {}: start={}, len={}",
                        persisted.block_index, slice_start, slice_len_u32
                    ),
                ))
            })?;
            let slice_range = studiofs_meta::schema::SliceRange::new(slice_start, slice_end)
                .ok_or_else(|| {
                    crate::VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid slice range {}..{}", slice_start, slice_end),
                    ))
                })?;
            let block_offset_u32 = u32::try_from(block_offset).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "block offset {} for block {} exceeds u32::MAX",
                        block_offset, persisted.block_index
                    ),
                ))
            })?;
            chunks
                .entry(chunk_id)
                .or_default()
                .push(studiofs_meta::schema::SliceRecord {
                    slice_id: studiofs_meta::schema::SliceId::default(),
                    order: studiofs_meta::schema::SliceOrder::default(),
                    range: slice_range,
                    block: Some(studiofs_meta::schema::BlockRef {
                        object_id: persisted.object_id,
                        offset: block_offset_u32,
                        length: slice_len_u32,
                        flags,
                    }),
                    block_state: studiofs_meta::schema::BlockRefState::LocalOnly,
                });
            file_pos = file_pos.checked_add(slice_len).ok_or_else(|| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "file position overflow while splitting block {}",
                        persisted.block_index
                    ),
                ))
            })?;
        }

        Ok(())
    }

    async fn execute_drain(
        &self,
        ino: Ino,
        persisted_blocks: Vec<PersistedWriteBlock>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<DrainOutcome> {
        if persisted_blocks.is_empty() {
            if let Some(attr) = attr_to_persist {
                let committed_attr = self
                    .commit_meta
                    .commit_overlay(ino, Vec::new(), Some(attr))
                    .await?;
                self.observer
                    .on_metadata_committed(ino, committed_attr.as_ref());
            }
            return Ok(DrainOutcome {
                persisted_blocks: Vec::new(),
            });
        }

        if let Err(error) = self
            .local_blocks
            .persist_payload(&persisted_blocks, persist_config)
            .await
        {
            self.local_blocks
                .cleanup_uncommitted(ino, &persisted_blocks, "payload-persist")
                .await;
            return Err(error);
        }

        let chunk_slices =
            match self.build_overlay_slice_records(ino, &persisted_blocks, dirty_chunk_slices) {
                Ok(slices) => slices,
                Err(error) => {
                    self.local_blocks
                        .cleanup_uncommitted(ino, &persisted_blocks, "metadata-build")
                        .await;
                    return Err(error);
                }
            };
        let committed_attr = match self
            .commit_meta
            .commit_overlay(ino, chunk_slices, attr_to_persist)
            .await
        {
            Ok(attr) => attr,
            Err(error) => {
                self.local_blocks
                    .cleanup_uncommitted(ino, &persisted_blocks, "metadata-commit")
                    .await;
                return Err(error);
            }
        };

        self.observer
            .on_metadata_committed(ino, committed_attr.as_ref());
        self.uploads
            .submit_uploads(ino, &persisted_blocks, content_epoch)
            .await;

        Ok(DrainOutcome { persisted_blocks })
    }
}

impl<'a> LocalDirtyDrain<'a> {
    fn for_inode(self, vfs: &'a StudioVfs, ino: Ino) -> LocalDirtyFlushSession<'a> {
        LocalDirtyFlushSession {
            ino,
            vfs,
            drain: self,
        }
    }
}

impl BlockIdAllocatorPort for VfsBlockIdAllocator<'_> {
    async fn next_block_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        run_meta(self.vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_block_ids(count)
        })
        .await
    }
}

impl LocalBlockWriterPort for VfsLocalBlockWriter<'_> {
    async fn persist_payload(
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
                    self.vfs
                        .block_store_put_async(block.object_id, block.data, false)
                        .await
                },
            )
            .await?;
            self.vfs
                .maybe_block_store_sync_data_async(
                    persisted_bytes,
                    persist_config.local_sync_policy,
                )
                .await?;
            Ok(())
        }
        .await;
        let completed_burst = result.is_ok();
        if persisted_bytes > 0 {
            self.vfs.record_payload_persist_observability(
                persisted_blocks.len(),
                persisted_bytes,
                started.elapsed(),
                completed_burst,
            );
        }
        result
    }

    async fn cleanup_uncommitted(
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
            .vfs
            .block_store_remove_batch_async(object_ids.clone())
            .await
        {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of uncommitted block-store entries failed"
            );
        }
    }
}

impl OverlayCommitPort for VfsOverlayCommitter<'_> {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>> {
        let started = Instant::now();
        let result = run_meta(self.vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.flush_regular_file_overlay_slices_with_attr(ino, &chunk_slices, attr.as_ref())
        })
        .await;
        self.vfs
            .record_metadata_commit_observability(started.elapsed(), result.is_ok());
        result
    }
}

impl UploadSubmissionPort for VfsUploadSubmitter<'_> {
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedWriteBlock],
        content_epoch: u64,
    ) {
        let upload_ops: Vec<(u64, u64, u64, u64)> = persisted_blocks
            .iter()
            .map(|block| {
                (
                    ino,
                    (block.block_index as u64).saturating_mul(self.vfs.block_size as u64),
                    block.object_id,
                    content_epoch,
                )
            })
            .collect();

        self.vfs.cached_store.submit_uploads_batch(&upload_ops);
        for (upload_ino, _, _, _) in &upload_ops {
            self.vfs.state.file_session.note_background_transition(
                *upload_ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }
        if !upload_ops.is_empty() {
            self.vfs.cached_store.flush_upload_journal().await;
        }

        let mut live_extents = Vec::with_capacity(persisted_blocks.len());
        for block in persisted_blocks {
            live_extents.push((ino, block.block_index, block.object_id, content_epoch));
        }
        self.vfs.record_live_extents(&live_extents);
    }
}

impl DrainCommitObserverPort for VfsDrainCommitObserver<'_> {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>) {
        if let Some(attr) = committed_attr {
            self.vfs.state.namespace.attr_read_cache.lock().remove(ino);
            self.vfs
                .refresh_inode_incarnation_state_from_attr(ino, attr);
        }
    }
}

impl<'a> LocalDirtyFlushSession<'a> {
    async fn drain_snapshot(
        &self,
        blocks: Vec<DirtyWriteSnapshot>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<Vec<PersistedWriteBlock>> {
        let mut fresh_object_ids = std::collections::VecDeque::new();
        let persisted_blocks = loop {
            let prepare_result = {
                let mut pending = self.vfs.state.local_dirty.pending_blocks.lock();
                self.drain.try_prepare_persisted_blocks(
                    &mut pending,
                    self.ino,
                    &blocks,
                    &mut fresh_object_ids,
                )
            };
            match prepare_result {
                Ok(persisted_blocks) => break persisted_blocks,
                Err(additional_needed) => {
                    let additional_needed = u32::try_from(additional_needed).map_err(|_| {
                        VfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!(
                                "requested additional block-id count {} exceeds u32::MAX",
                                additional_needed
                            ),
                        ))
                    })?;
                    let additional_ids = self.drain.allocate_block_ids(additional_needed).await?;
                    fresh_object_ids.extend(additional_ids);
                }
            }
        };
        let attr_to_clear = attr_to_persist.clone();
        let outcome = self
            .drain
            .execute_drain(
                self.ino,
                persisted_blocks,
                dirty_chunk_slices,
                content_epoch,
                attr_to_persist,
                persist_config,
            )
            .await?;
        self.vfs
            .commit_persisted_local_dirty_blocks(self.ino, &outcome.persisted_blocks);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.vfs
                .clear_matching_dirty_attr_if_committed(self.ino, attr);
        }
        Ok(outcome.persisted_blocks)
    }

    async fn flush(&self, local_sync_policy: LocalSyncPolicy) -> VfsResult<()> {
        self.vfs
            .state
            .metrics
            .flush_barrier_calls
            .fetch_add(1, Ordering::Relaxed);
        self.vfs.enforce_access(self.ino, AccessLevel::ReadWrite)?;
        if self.vfs.ignored_entry_attr(self.ino).is_some() {
            return Ok(());
        }

        let blocks_to_buffer = self.vfs.snapshot_dirty_local_dirty_blocks(self.ino);
        let dirty_attr = {
            self.vfs
                .state
                .namespace
                .dirty_attrs
                .lock()
                .get(&self.ino)
                .cloned()
        };
        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else {
            self.vfs.current_content_epoch_async(self.ino).await?
        };
        let chunk_slices = self
            .vfs
            .snapshot_dirty_local_dirty_chunk_slices(self.ino, &blocks_to_buffer);
        self.drain_snapshot(
            blocks_to_buffer,
            chunk_slices,
            content_epoch,
            dirty_attr,
            PayloadPersistConfig {
                put_concurrency: self.drain.put_concurrency,
                local_sync_policy,
            },
        )
        .await?;

        Ok(())
    }
}

impl StudioVfs {
    pub(super) async fn build_dirty_write_snapshots_from_buffered_writes(
        &self,
        ino: Ino,
        writes: &[(u64, Bytes)],
        visible_size: u64,
    ) -> VfsResult<Vec<DirtyWriteSnapshot>> {
        let pending_upload_index = self.snapshot_pending_upload_index();
        let fully_overwritten_blocks =
            self.fully_overwritten_block_indexes_for_buffered_writes(writes, visible_size);
        let mut blocks = std::collections::BTreeMap::<u32, BytesMut>::new();

        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }

            let mut remaining = data.len();
            let mut current_offset = *offset;
            let mut data_offset = 0usize;
            while remaining > 0 {
                let block_index = block_index_from_offset(current_offset, self.block_size);
                let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
                let space_in_block = self.block_size as usize - offset_in_block;
                let to_write = remaining.min(space_in_block);

                if let std::collections::btree_map::Entry::Vacant(entry) = blocks.entry(block_index)
                {
                    let existing_data = if fully_overwritten_blocks.contains(&block_index) {
                        Bytes::new()
                    } else {
                        match self
                            .read_block_if_exists(ino, block_index, Some(&pending_upload_index))
                            .await?
                        {
                            Some(data) => {
                                let logical_len = self.logical_block_len(visible_size, block_index);
                                if data.len() > logical_len {
                                    data.slice(0..logical_len)
                                } else {
                                    data
                                }
                            }
                            None => Bytes::new(),
                        }
                    };
                    entry.insert(BytesMut::from(existing_data.as_ref()));
                }

                let block = blocks
                    .get_mut(&block_index)
                    .expect("block buffer inserted before write");
                let end_pos = offset_in_block + to_write;
                if block.len() < end_pos {
                    block.resize(end_pos, 0);
                }
                block[offset_in_block..end_pos]
                    .copy_from_slice(&data[data_offset..data_offset + to_write]);

                remaining -= to_write;
                current_offset += to_write as u64;
                data_offset += to_write;
            }
        }

        Ok(blocks
            .into_iter()
            .filter_map(|(block_index, mut data)| {
                let logical_len = self.logical_block_len(visible_size, block_index);
                if data.len() > logical_len {
                    data.truncate(logical_len);
                }
                if data.is_empty() {
                    None
                } else {
                    Some(DirtyWriteSnapshot {
                        block_index,
                        data: data.freeze(),
                    })
                }
            })
            .collect())
    }

    fn fully_overwritten_block_indexes_for_buffered_writes(
        &self,
        writes: &[(u64, Bytes)],
        visible_size: u64,
    ) -> std::collections::HashSet<u32> {
        let mut ranges_by_block = std::collections::BTreeMap::<u32, Vec<(usize, usize)>>::new();

        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }

            let mut remaining = data.len();
            let mut current_offset = *offset;
            while remaining > 0 {
                let block_index = block_index_from_offset(current_offset, self.block_size);
                let start = offset_within_block(current_offset, self.block_size) as usize;
                let space_in_block = self.block_size as usize - start;
                let written = remaining.min(space_in_block);
                ranges_by_block
                    .entry(block_index)
                    .or_default()
                    .push((start, start + written));
                remaining -= written;
                current_offset += written as u64;
            }
        }

        ranges_by_block
            .into_iter()
            .filter_map(|(block_index, mut ranges)| {
                let logical_len = self.logical_block_len(visible_size, block_index);
                if logical_len == 0 {
                    return None;
                }
                ranges.sort_unstable_by_key(|(start, _)| *start);
                let mut covered_until = 0usize;
                for (start, end) in ranges {
                    if start > covered_until {
                        return None;
                    }
                    covered_until = covered_until.max(end);
                    if covered_until >= logical_len {
                        return Some(block_index);
                    }
                }
                None
            })
            .collect()
    }

    pub(super) async fn materialize_buffered_writes_direct(
        &self,
        ino: Ino,
        writes: &[(u64, Bytes)],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        let dirty_attr = { self.state.namespace.dirty_attrs.lock().get(&ino).cloned() };
        let visible_size = if let Some(attr) = dirty_attr.as_ref() {
            attr.size
        } else {
            self.getattr_async(ino).await?.size
        };
        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else {
            self.current_content_epoch_async(ino).await?
        };

        let blocks = self
            .build_dirty_write_snapshots_from_buffered_writes(ino, writes, visible_size)
            .await?;
        if blocks.is_empty() {
            return Ok(());
        }

        if matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            let (completed_blocks, buffered_blocks): (Vec<_>, Vec<_>) = blocks
                .into_iter()
                .partition(|block| block.data.len() == self.block_size as usize);

            if !completed_blocks.is_empty() {
                let attr_to_persist = if buffered_blocks.is_empty() {
                    dirty_attr.clone()
                } else {
                    self.derive_opportunistic_prefix_attr(
                        ino,
                        dirty_attr.as_ref(),
                        &completed_blocks,
                    )
                    .await?
                };
                self.persist_dirty_write_snapshots_direct(
                    ino,
                    completed_blocks,
                    content_epoch,
                    attr_to_persist,
                    local_sync_policy,
                )
                .await?;
            }

            if !buffered_blocks.is_empty() {
                self.stage_dirty_write_snapshots(ino, buffered_blocks);
            }

            return Ok(());
        }

        self.persist_dirty_write_snapshots_direct(
            ino,
            blocks,
            content_epoch,
            dirty_attr,
            local_sync_policy,
        )
        .await
    }

    pub(super) async fn derive_opportunistic_prefix_attr(
        &self,
        ino: Ino,
        dirty_attr: Option<&InodeAttr>,
        completed_blocks: &[DirtyWriteSnapshot],
    ) -> VfsResult<Option<InodeAttr>> {
        let Some(max_completed_end) = completed_blocks
            .iter()
            .map(|block| {
                (u64::from(block.block_index) + 1).saturating_mul(u64::from(self.block_size))
            })
            .max()
        else {
            return Ok(None);
        };

        let persisted_attr = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(ino)
        })
        .await?;
        let mut attr = dirty_attr
            .cloned()
            .unwrap_or_else(|| persisted_attr.clone());
        attr.size = persisted_attr.size.max(max_completed_end);
        Ok(Some(attr))
    }

    pub(super) async fn persist_dirty_write_snapshots_direct(
        &self,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let block_count = u32::try_from(blocks.len()).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "requested direct materialization block count {} exceeds u32::MAX",
                    blocks.len()
                ),
            ))
        })?;
        let drain = self.local_dirty_drain();
        let object_ids = drain.allocate_block_ids(block_count).await?;
        let persisted_blocks: Vec<PersistedWriteBlock> = blocks
            .into_iter()
            .zip(object_ids)
            .map(|(block, object_id)| PersistedWriteBlock {
                block_index: block.block_index,
                object_id,
                data: block.data,
            })
            .collect();

        let persisted_block_indexes: Vec<u32> = persisted_blocks
            .iter()
            .map(|block| block.block_index)
            .collect();
        let attr_to_clear = attr_to_persist.clone();
        let _outcome = drain
            .execute_drain(
                ino,
                persisted_blocks,
                std::collections::BTreeMap::new(),
                content_epoch,
                attr_to_persist,
                PayloadPersistConfig {
                    put_concurrency: self.payload_put_concurrency,
                    local_sync_policy,
                },
            )
            .await?;
        for block_index in persisted_block_indexes {
            self.clear_pending_block(ino, block_index);
        }
        if let Some(attr) = attr_to_clear.as_ref() {
            self.clear_matching_dirty_attr_if_committed(ino, attr);
        }

        Ok(())
    }

    fn stage_dirty_write_snapshots(&self, ino: Ino, blocks: Vec<DirtyWriteSnapshot>) {
        for block in blocks {
            let reserved_object_id = self
                .state
                .local_dirty
                .pending_blocks
                .lock()
                .get(&(ino, block.block_index))
                .and_then(|pending| pending.reserved_object_id);
            self.replace_pending_local_dirty_block(
                ino,
                block.block_index,
                PendingBlock {
                    data: BytesMut::from(block.data.as_ref()),
                    dirty: true,
                    reserved_object_id,
                },
            );
            self.clear_pending_dirty_slices_for_block(ino, block.block_index);
        }
    }

    fn local_dirty_drain(&self) -> LocalDirtyDrain<'_> {
        LocalDirtyDrainCore {
            local_blocks: VfsLocalBlockWriter { vfs: self },
            commit_meta: VfsOverlayCommitter { vfs: self },
            block_ids: VfsBlockIdAllocator { vfs: self },
            uploads: VfsUploadSubmitter { vfs: self },
            observer: VfsDrainCommitObserver { vfs: self },
            block_size: self.block_size,
            put_concurrency: self.payload_put_concurrency,
        }
    }

    fn local_dirty_flush_session(&self, ino: Ino) -> LocalDirtyFlushSession<'_> {
        self.local_dirty_drain().for_inode(self, ino)
    }

    pub(super) fn total_pending_local_dirty_bytes(&self) -> usize {
        self.state.local_dirty.pending_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn has_pending_local_dirty_blocks_for_inode(&self, ino: Ino) -> bool {
        self.state
            .local_dirty
            .pending_block_indexes
            .lock()
            .get(&ino)
            .is_some_and(|indexes| !indexes.is_empty())
    }

    fn clear_matching_dirty_attr_if_committed(&self, ino: Ino, attr: &InodeAttr) {
        if self.has_pending_local_dirty_blocks_for_inode(ino) {
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

    fn add_pending_local_dirty_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.state
            .local_dirty
            .pending_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn subtract_pending_local_dirty_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self.state.local_dirty.pending_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |pending| Some(pending.saturating_sub(bytes)),
        );
    }

    fn replace_pending_local_dirty_block(&self, ino: Ino, block_index: u32, block: PendingBlock) {
        let mut pending = self.state.local_dirty.pending_blocks.lock();
        let mut indexes = self.state.local_dirty.pending_block_indexes.lock();
        let new_len = block.data.len();
        let old_len = pending
            .insert((ino, block_index), block)
            .map(|old| old.data.len())
            .unwrap_or(0);
        indexes.entry(ino).or_default().insert(block_index);
        drop(indexes);
        drop(pending);
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => self.add_pending_local_dirty_bytes(new_len - old_len),
            std::cmp::Ordering::Less => self.subtract_pending_local_dirty_bytes(old_len - new_len),
            std::cmp::Ordering::Equal => {}
        }
    }

    fn clear_pending_dirty_slices_for_inode(&self, ino: Ino) {
        self.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .remove(&ino);
    }

    fn clear_pending_dirty_slices_for_block(&self, ino: Ino, block_index: u32) {
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
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
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
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
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
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
        let mut pending = self.state.local_dirty.pending_blocks.lock();
        let mut indexes = self.state.local_dirty.pending_block_indexes.lock();
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
        self.subtract_pending_local_dirty_bytes(removed_bytes);
        self.clear_pending_dirty_slices_for_inode(ino);
    }

    pub(super) fn clear_pending_block(&self, ino: Ino, block_index: u32) {
        let mut pending = self.state.local_dirty.pending_blocks.lock();
        let mut indexes = self.state.local_dirty.pending_block_indexes.lock();
        if let Some(removed) = pending.remove(&(ino, block_index)) {
            if let Some(inode_indexes) = indexes.get_mut(&ino) {
                inode_indexes.remove(&block_index);
                if inode_indexes.is_empty() {
                    indexes.remove(&ino);
                }
            }
            drop(indexes);
            drop(pending);
            self.subtract_pending_local_dirty_bytes(removed.data.len());
            self.clear_pending_dirty_slices_for_block(ino, block_index);
        }
    }

    fn snapshot_dirty_local_dirty_blocks(&self, ino: Ino) -> Vec<DirtyWriteSnapshot> {
        // Point-in-time view of dirty local-visible payload. Writers may race
        // ahead after this snapshot; commit keeps newer bytes dirty so a later
        // flush retries with the fresh view instead of dropping updates.
        let pending = self.state.local_dirty.pending_blocks.lock();
        let indexes = self.state.local_dirty.pending_block_indexes.lock();
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

    fn snapshot_dirty_local_dirty_chunk_slices(
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
        if let Some(chunks) = self.state.local_dirty.pending_chunk_slices.lock().get(&ino) {
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
        let pending_blocks = self.state.local_dirty.pending_blocks.lock();
        let pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
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

    fn commit_persisted_local_dirty_blocks(&self, ino: Ino, blocks: &[PersistedWriteBlock]) {
        if blocks.is_empty() {
            return;
        }

        let mut pending = self.state.local_dirty.pending_blocks.lock();
        let mut indexes = self.state.local_dirty.pending_block_indexes.lock();
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
        self.subtract_pending_local_dirty_bytes(removed_bytes);
        self.clear_pending_dirty_slices_for_blocks(ino, &removed_block_indexes);
    }

    pub(super) fn trim_local_block_staging(
        &self,
        ino: Ino,
        block_index: u32,
        new_len: usize,
    ) -> VfsResult<bool> {
        let mut changed = false;

        {
            let mut pending = self.state.local_dirty.pending_blocks.lock();
            if let Some(block) = pending.get_mut(&(ino, block_index))
                && block.data.len() > new_len
            {
                let old_len = block.data.len();
                block.data.truncate(new_len);
                block.dirty = true;
                self.subtract_pending_local_dirty_bytes(old_len - block.data.len());
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
            .local_dirty
            .pending_blocks
            .lock()
            .get(&(ino, block_index))
            .and_then(|block| block.reserved_object_id);
        self.replace_pending_local_dirty_block(
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

    pub(super) async fn flush_local_dirty_state(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        self.local_dirty_flush_session(ino)
            .flush(local_sync_policy)
            .await
    }

    pub(super) async fn flush_pending_blocks_for_shutdown(&self) {
        let mut inos: BTreeSet<Ino> = {
            let pending = self.state.local_dirty.pending_blocks.lock();
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
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    type UploadRecord = (u64, u64, u64, u64);
    type SharedUploads = Arc<StdMutex<Vec<UploadRecord>>>;
    type SharedInodes = Arc<StdMutex<Vec<Ino>>>;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("local-dirty-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("local-dirty-uuid-{volume_suffix}"),
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

    fn single_block_fixture(
        ino: Ino,
        block_index: u32,
        data: &'static [u8],
        reserved_object_id: Option<u64>,
    ) -> (
        std::collections::HashMap<(Ino, u32), PendingBlock>,
        Vec<DirtyWriteSnapshot>,
    ) {
        (
            std::collections::HashMap::from([(
                (ino, block_index),
                PendingBlock {
                    data: BytesMut::from(data),
                    dirty: true,
                    reserved_object_id,
                },
            )]),
            vec![DirtyWriteSnapshot {
                block_index,
                data: Bytes::from_static(data),
            }],
        )
    }

    #[test]
    fn subtract_pending_local_dirty_bytes_saturates_without_panicking() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-bytes-underflow");
        let _guard = rt.enter();
        vfs.state
            .local_dirty
            .pending_bytes
            .store(1, Ordering::Relaxed);

        vfs.subtract_pending_local_dirty_bytes(2);

        assert_eq!(
            vfs.state.local_dirty.pending_bytes.load(Ordering::Relaxed),
            0
        );
    }

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

    #[derive(Clone, Default)]
    struct EventLog {
        entries: Arc<StdMutex<Vec<String>>>,
    }

    impl EventLog {
        fn push(&self, entry: impl Into<String>) {
            self.entries
                .lock()
                .expect("event log lock")
                .push(entry.into());
        }

        fn snapshot(&self) -> Vec<String> {
            self.entries.lock().expect("event log lock").clone()
        }
    }

    struct PanicBlockIds;

    impl BlockIdAllocatorPort for PanicBlockIds {
        async fn next_block_ids(&self, _count: u32) -> VfsResult<Vec<u64>> {
            panic!("block id allocator should not be used in this test")
        }
    }

    struct TestLocalBlockWriter {
        events: EventLog,
        fail_persist: bool,
    }

    impl LocalBlockWriterPort for TestLocalBlockWriter {
        async fn persist_payload(
            &self,
            _persisted_blocks: &[PersistedWriteBlock],
            _persist_config: PayloadPersistConfig,
        ) -> VfsResult<()> {
            self.events.push("payload");
            if self.fail_persist {
                Err(VfsError::Io(std::io::Error::other("payload failure")))
            } else {
                Ok(())
            }
        }

        async fn cleanup_uncommitted(
            &self,
            _ino: Ino,
            _blocks: &[PersistedWriteBlock],
            stage: &'static str,
        ) {
            self.events.push(format!("cleanup:{stage}"));
        }
    }

    struct TestOverlayCommitter {
        events: EventLog,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
    }

    impl OverlayCommitPort for TestOverlayCommitter {
        async fn commit_overlay(
            &self,
            _ino: Ino,
            _chunk_slices: Vec<(
                studiofs_meta::schema::ChunkId,
                Vec<studiofs_meta::schema::SliceRecord>,
            )>,
            _attr: Option<InodeAttr>,
        ) -> VfsResult<Option<InodeAttr>> {
            self.events.push("metadata");
            if self.fail_commit {
                Err(VfsError::Io(std::io::Error::other("metadata failure")))
            } else {
                Ok(self.committed_attr.clone())
            }
        }
    }

    struct TestUploadSubmitter {
        events: EventLog,
        uploads: SharedUploads,
    }

    impl UploadSubmissionPort for TestUploadSubmitter {
        async fn submit_uploads(
            &self,
            ino: Ino,
            persisted_blocks: &[PersistedWriteBlock],
            content_epoch: u64,
        ) {
            self.events.push("upload");
            let ops: Vec<_> = persisted_blocks
                .iter()
                .map(|block| {
                    (
                        ino,
                        block.block_index as u64,
                        block.object_id,
                        content_epoch,
                    )
                })
                .collect();
            self.uploads.lock().expect("uploads lock").extend(ops);
        }
    }

    struct TestDrainObserver {
        events: EventLog,
        committed_ino: SharedInodes,
    }

    impl DrainCommitObserverPort for TestDrainObserver {
        fn on_metadata_committed(&self, ino: Ino, _committed_attr: Option<&InodeAttr>) {
            self.events.push("observer");
            self.committed_ino.lock().expect("observer lock").push(ino);
        }
    }

    fn test_drain(
        events: EventLog,
        fail_persist: bool,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
        uploads: SharedUploads,
        observed_inodes: SharedInodes,
    ) -> LocalDirtyDrainCore<
        TestLocalBlockWriter,
        TestOverlayCommitter,
        PanicBlockIds,
        TestUploadSubmitter,
        TestDrainObserver,
    > {
        LocalDirtyDrainCore {
            local_blocks: TestLocalBlockWriter {
                events: events.clone(),
                fail_persist,
            },
            commit_meta: TestOverlayCommitter {
                events: events.clone(),
                fail_commit,
                committed_attr,
            },
            block_ids: PanicBlockIds,
            uploads: TestUploadSubmitter {
                events: events.clone(),
                uploads,
            },
            observer: TestDrainObserver {
                events,
                committed_ino: observed_inodes,
            },
            block_size: 4096,
            put_concurrency: 4,
        }
    }

    #[test]
    fn drain_core_orders_payload_then_metadata_then_upload() {
        let ino = 11;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([111]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, &blocks, &mut fresh_object_ids)
            .expect("prepare persisted blocks");

        futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            9,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("drain succeeds");

        assert_eq!(
            pending
                .get(&(ino, 0))
                .and_then(|block| block.reserved_object_id),
            Some(111)
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "observer".to_string(),
                "upload".to_string(),
            ]
        );
        assert_eq!(
            *uploads.lock().expect("uploads lock"),
            vec![(ino, 0, 111, 9)]
        );
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn drain_core_cleans_up_after_metadata_failure() {
        let ino = 12;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            true,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 1, b"beta", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([222]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, &blocks, &mut fresh_object_ids)
            .expect("prepare persisted blocks");

        let error = futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            5,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect_err("drain should fail");

        assert!(
            matches!(error, VfsError::Io(_)),
            "expected metadata failure to bubble up"
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "cleanup:metadata-commit".to_string(),
            ]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert!(observed_inodes.lock().expect("observer lock").is_empty());
    }

    #[test]
    fn prepare_persisted_blocks_requests_more_ids_when_snapshot_goes_stale() {
        let ino = 21;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", Some(700));
        let mut fresh_object_ids = std::collections::VecDeque::new();

        pending.insert(
            (ino, 0),
            PendingBlock {
                data: BytesMut::from(&b"newer"[..]),
                dirty: true,
                reserved_object_id: Some(700),
            },
        );

        assert!(matches!(
            drain.try_prepare_persisted_blocks(&mut pending, ino, &blocks, &mut fresh_object_ids),
            Err(1)
        ));

        fresh_object_ids.push_back(701);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, &blocks, &mut fresh_object_ids)
            .expect("prepare stale snapshot");
        assert_eq!(persisted_blocks[0].object_id, 701);
        let pending_block = pending.get(&(ino, 0)).expect("pending block");
        assert_eq!(pending_block.data.as_ref(), b"newer");
        assert_eq!(pending_block.reserved_object_id, Some(700));
    }

    #[test]
    fn drain_core_commits_attr_only_without_uploading() {
        let ino = 13;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            uploads.clone(),
            observed_inodes.clone(),
        );

        let outcome = futures::executor::block_on(drain.execute_drain(
            ino,
            Vec::new(),
            std::collections::BTreeMap::new(),
            4,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("attr-only drain succeeds");

        assert!(outcome.persisted_blocks.is_empty());
        assert_eq!(
            events.snapshot(),
            vec!["metadata".to_string(), "observer".to_string()]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn build_overlay_slice_records_splits_blocks_across_chunk_boundaries() {
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let mut drain = test_drain(events, false, false, None, uploads, observed_inodes);
        drain.block_size = 5 * 1024 * 1024;

        let chunk_slices = drain
            .build_overlay_slice_records(
                41,
                &[PersistedWriteBlock {
                    block_index: 12,
                    object_id: 9001,
                    data: Bytes::from(vec![0xAB; drain.block_size as usize]),
                }],
                std::collections::BTreeMap::new(),
            )
            .expect("chunk-aware block split");

        assert_eq!(
            chunk_slices.len(),
            2,
            "cross-chunk block should split in two"
        );
        let first = &chunk_slices[0].1;
        let second = &chunk_slices[1].1;
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(chunk_slices[0].0, studiofs_meta::schema::ChunkId(0));
        assert_eq!(chunk_slices[1].0, studiofs_meta::schema::ChunkId(1));
        assert_eq!(first[0].range.start, 60 * 1024 * 1024);
        assert_eq!(
            first[0].range.end,
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE
        );
        assert_eq!(first[0].block.as_ref().expect("block ref").offset, 0);
        assert_eq!(
            first[0].block.as_ref().expect("block ref").length,
            4 * 1024 * 1024
        );
        assert_eq!(second[0].range.start, 0);
        assert_eq!(second[0].range.end, 1024 * 1024);
        assert_eq!(
            second[0].block.as_ref().expect("block ref").offset,
            4 * 1024 * 1024
        );
        assert_eq!(
            second[0].block.as_ref().expect("block ref").length,
            1024 * 1024
        );
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
