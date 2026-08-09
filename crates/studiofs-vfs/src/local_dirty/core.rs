use super::adapters::{
    VfsBlockIdAllocator, VfsDrainCommitObserver, VfsLocalPayloadWriter, VfsOverlayCommitter,
    VfsUploadSubmitter,
};
use super::ports::*;
use super::*;

#[derive(Debug, Clone)]
pub(super) struct PersistedPayloadEntry {
    pub(super) block_index: u32,
    pub(super) object_id: u64,
    data: Option<Bytes>,
    len: usize,
}

impl PersistedPayloadEntry {
    pub(super) fn from_payload(block_index: u32, object_id: u64, data: Bytes) -> Self {
        let len = data.len();
        Self {
            block_index,
            object_id,
            data: Some(data),
            len,
        }
    }

    pub(super) fn from_local_extent(block_index: u32, object_id: u64, len: usize) -> Self {
        Self {
            block_index,
            object_id,
            data: None,
            len,
        }
    }

    pub(super) fn payload_bytes(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn requires_payload_persist(&self) -> bool {
        self.data.is_some()
    }
}

pub(super) struct LocalDirtyDrainCore<L, C, B, U, O> {
    pub(super) local_payloads: L,
    pub(super) commit_meta: C,
    pub(super) object_ids: B,
    pub(super) uploads: U,
    pub(super) observer: O,
    pub(super) block_size: u32,
    pub(super) put_concurrency: usize,
}

pub(super) type LocalDirtyDrain<'a> = LocalDirtyDrainCore<
    VfsLocalPayloadWriter<'a>,
    VfsOverlayCommitter<'a>,
    VfsBlockIdAllocator<'a>,
    VfsUploadSubmitter<'a>,
    VfsDrainCommitObserver<'a>,
>;

pub(super) struct LocalDirtyFlushSession<'a> {
    pub(super) ino: Ino,
    pub(super) vfs: &'a StudioVfs,
    pub(super) drain: LocalDirtyDrain<'a>,
}

#[derive(Debug)]
pub(super) struct DrainOutcome {
    pub(super) persisted_blocks: Vec<PersistedPayloadEntry>,
}

pub(super) fn commit_persisted_payload_state(
    pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
    ino: Ino,
    blocks: &[PersistedPayloadEntry],
) -> (usize, Vec<u32>, Vec<BytesMut>) {
    let mut removed_bytes = 0usize;
    let mut removed_block_indexes = Vec::new();
    let mut removed_buffers = Vec::new();
    for block in blocks {
        let key = (ino, block.block_index);
        let mut should_remove = false;
        if let Some(current) = pending.get_mut(&key) {
            if current.reserved_object_id == Some(block.object_id) {
                if current.is_local_extent() && current.persisted_len == Some(block.len()) {
                    should_remove = true;
                } else {
                    current.reserved_object_id = None;
                    current.persisted_len = None;
                }
            }
            should_remove |= current.dirty
                && block
                    .payload_bytes()
                    .is_some_and(|payload| current.data.as_ref() == payload.as_ref());
        }
        if should_remove && let Some(removed) = pending.remove(&key) {
            removed_bytes += removed.memory_len();
            removed_block_indexes.push(block.block_index);
            if let Some(buffer) = removed.recycle_buffer() {
                removed_buffers.push(buffer);
            }
        }
    }
    (removed_bytes, removed_block_indexes, removed_buffers)
}

impl<L, C, B, U, O> LocalDirtyDrainCore<L, C, B, U, O>
where
    L: LocalPayloadWriterPort,
    C: OverlayCommitPort,
    B: BlockIdAllocatorPort,
    U: UploadSubmissionPort,
    O: DrainCommitObserverPort,
{
    pub(super) async fn allocate_object_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        if count == 0 {
            Ok(Vec::new())
        } else {
            self.object_ids.next_object_ids(count).await
        }
    }

    fn additional_object_id_count_needed(
        pending: &std::collections::HashMap<(Ino, u32), PendingPayload>,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
    ) -> usize {
        blocks
            .iter()
            .filter(|block| {
                pending
                    .get(&(ino, block.block_index))
                    .filter(|current| {
                        (current.is_local_extent() && current.persisted_len == Some(block.len()))
                            || (!current.is_local_extent()
                                && current.data.as_ref() == block.as_ref())
                    })
                    .and_then(|current| current.reserved_object_id)
                    .is_none()
            })
            .count()
    }

    pub(super) fn try_prepare_persisted_blocks(
        &self,
        pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        fresh_object_ids: &mut std::collections::VecDeque<u64>,
    ) -> Result<Vec<PersistedPayloadEntry>, (Vec<DirtyWriteSnapshot>, usize)> {
        let fresh_needed = Self::additional_object_id_count_needed(pending, ino, &blocks);
        let additional_needed = fresh_needed.saturating_sub(fresh_object_ids.len());
        if additional_needed > 0 {
            return Err((blocks, additional_needed));
        }

        let mut persisted_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            let key = (ino, block.block_index);
            if let Some(current) = pending.get_mut(&key) {
                if current.is_local_extent() && current.persisted_len == Some(block.len()) {
                    if let Some(object_id) = current.reserved_object_id {
                        persisted_blocks.push(PersistedPayloadEntry::from_local_extent(
                            block.block_index,
                            object_id,
                            block.len(),
                        ));
                        continue;
                    }
                } else if current.data.as_ref() == block.as_ref() {
                    let object_id = if let Some(object_id) = current.reserved_object_id {
                        object_id
                    } else {
                        let object_id = fresh_object_ids
                            .pop_front()
                            .expect("fresh object ids prevalidated");
                        current.reserved_object_id = Some(object_id);
                        object_id
                    };
                    persisted_blocks.push(PersistedPayloadEntry::from_payload(
                        block.block_index,
                        object_id,
                        block.data.freeze(),
                    ));
                    continue;
                }
            }

            let object_id = fresh_object_ids
                .pop_front()
                .expect("fresh object ids prevalidated");
            persisted_blocks.push(PersistedPayloadEntry::from_payload(
                block.block_index,
                object_id,
                block.data.freeze(),
            ));
        }

        Ok(persisted_blocks)
    }

    pub(super) fn build_overlay_slice_records(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
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
            // Dirty-chunk-slices path: persisted slice payloads can commit
            // directly as overlay SliceRecords, while buffered block snapshots
            // still validate against the final block geometry below.
            let block_lengths_by_block_index: std::collections::HashMap<u32, usize> =
                persisted_blocks
                    .iter()
                    .map(|persisted| (persisted.block_index, persisted.len()))
                    .collect();
            for (chunk_id, mut slices) in dirty_chunk_slices {
                slices.sort_by_key(|slice| slice.order_key(chunk_id));
                for slice in slices {
                    if let Some(object_id) = slice.object_id {
                        let slice_len = slice.range.len();
                        chunks.entry(chunk_id).or_default().push(
                            studiofs_meta::schema::SliceRecord {
                                slice_id: studiofs_meta::schema::SliceId::default(),
                                order: studiofs_meta::schema::SliceOrder::default(),
                                range: slice.range,
                                payload: Some(studiofs_meta::schema::PayloadRef {
                                    object_id,
                                    offset: slice.object_offset,
                                    length: slice_len,
                                    flags: studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK,
                                }),
                                payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
                            },
                        );
                        continue;
                    }
                    let Some(&block_len) = block_lengths_by_block_index.get(&slice.block_index)
                    else {
                        continue;
                    };
                    let slice_len = slice.range.len() as usize;
                    let block_offset = slice.block_offset as usize;
                    let Some(block_end) = block_offset.checked_add(slice_len) else {
                        return Err(crate::VfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!(
                                "dirty slice offset overflow: ino={}, \
                                 block_index={}, block_offset={}, slice_len={}",
                                ino, slice.block_index, block_offset, slice_len
                            ),
                        )));
                    };
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
        persisted: &PersistedPayloadEntry,
    ) -> VfsResult<()> {
        let block_start = (persisted.block_index as u64).saturating_mul(self.block_size as u64);
        let block_len = u64::try_from(persisted.len()).map_err(|_| {
            crate::VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "persisted block {} length {} exceeds u64::MAX",
                    persisted.block_index,
                    persisted.len()
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
        if persisted.len() < self.block_size as usize {
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
                    payload: Some(studiofs_meta::schema::PayloadRef {
                        object_id: persisted.object_id,
                        offset: block_offset_u32,
                        length: slice_len_u32,
                        flags,
                    }),
                    payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
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

    pub(super) async fn execute_drain(
        &self,
        ino: Ino,
        persisted_blocks: Vec<PersistedPayloadEntry>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<DrainOutcome> {
        if persisted_blocks.is_empty() && dirty_chunk_slices.is_empty() {
            if let Some(attr) = attr_to_persist {
                let committed_attr = self
                    .commit_meta
                    .commit_overlay(
                        ino,
                        Vec::new(),
                        Some(attr),
                        persist_config
                            .local_sync_policy
                            .requires_write_path_durability(),
                    )
                    .await?;
                self.observer
                    .on_metadata_committed(ino, committed_attr.as_ref());
            }
            return Ok(DrainOutcome {
                persisted_blocks: Vec::new(),
            });
        }

        if !persisted_blocks.is_empty()
            && let Err(error) = self
                .local_payloads
                .persist_payload(&persisted_blocks, persist_config)
                .await
        {
            self.local_payloads
                .cleanup_uncommitted(ino, &persisted_blocks, "payload-persist")
                .await;
            return Err(error);
        }

        let chunk_slices =
            match self.build_overlay_slice_records(ino, &persisted_blocks, dirty_chunk_slices) {
                Ok(slices) => slices,
                Err(error) => {
                    self.local_payloads
                        .cleanup_uncommitted(ino, &persisted_blocks, "metadata-build")
                        .await;
                    return Err(error);
                }
            };
        let had_attr_to_persist = attr_to_persist.is_some();
        // Upload reconciliation must not run before its overlay exists in redb.
        let committed_attr = match self
            .commit_meta
            .commit_overlay(
                ino,
                chunk_slices,
                attr_to_persist,
                !persisted_blocks.is_empty()
                    || persist_config
                        .local_sync_policy
                        .requires_write_path_durability(),
            )
            .await
        {
            Ok(attr) => attr,
            Err(error) => {
                self.local_payloads
                    .cleanup_uncommitted(ino, &persisted_blocks, "metadata-commit")
                    .await;
                return Err(error);
            }
        };
        if had_attr_to_persist && committed_attr.is_none() {
            // The metadata snapshot can be stale because a remote Ready
            // publication won the race after this payload was materialized.
            // Keep durable payloads in the upload pipeline: the uploader can
            // reattach an uncovered range against the newer authoritative
            // graph and rebase the local epoch. Dropping them here loses a
            // valid write even though payload persistence succeeded.
            if persisted_blocks.is_empty() {
                self.local_payloads
                    .cleanup_uncommitted(ino, &persisted_blocks, "metadata-stale")
                    .await;
                return Ok(DrainOutcome { persisted_blocks });
            }
        }

        self.observer
            .on_metadata_committed(ino, committed_attr.as_ref());
        if !persisted_blocks.is_empty() {
            self.uploads
                .submit_uploads(ino, &persisted_blocks, content_epoch)
                .await;
        }

        Ok(DrainOutcome { persisted_blocks })
    }
}

impl<'a> LocalDirtyDrain<'a> {
    pub(super) fn for_inode(self, vfs: &'a StudioVfs, ino: Ino) -> LocalDirtyFlushSession<'a> {
        LocalDirtyFlushSession {
            ino,
            vfs,
            drain: self,
        }
    }
}
