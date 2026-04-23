use super::*;

fn record_regular_file_object_usage(
    usage: &mut std::collections::BTreeMap<u64, u64>,
    block: &BlockRef,
) {
    let object_bytes = u64::from(block.offset).saturating_add(u64::from(block.length));
    usage
        .entry(block.object_id)
        .and_modify(|existing| *existing = (*existing).max(object_bytes))
        .or_insert(object_bytes);
}

fn regular_file_chunk_states_usage_bytes(
    chunks: &std::collections::BTreeMap<ChunkId, RegularFileChunkState>,
) -> u64 {
    let mut usage = std::collections::BTreeMap::new();
    for chunk_state in chunks.values() {
        for slice in chunk_state.slices.values() {
            if let Some(block) = slice.block.as_ref() {
                record_regular_file_object_usage(&mut usage, block);
            }
        }
    }
    usage.values().copied().sum()
}

impl MetaBlocks for RedbMetaEngine {
    fn get_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        let block_size = self.regular_file_graph_block_size()?;
        let block_start = u64::from(block_index).saturating_mul(u64::from(block_size));
        let spans = self.lookup_regular_file_slices(ino, block_start, u64::from(block_size))?;
        Self::synthesize_block_ref_from_resolved_spans(
            ino,
            spans.as_slice(),
            block_index,
            block_size,
        )
    }

    fn set_file_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let old_block;
        {
            old_block = Self::current_block_in_txn(&txn, ino, block_index, block_size)?;
            let chunk_slices = Self::chunk_slices_for_block(
                block_index,
                block_size,
                Some(block),
                BlockRefState::LocalOnly,
            )?;
            Self::apply_regular_file_chunk_slices_in_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
                chunk_slices.as_slice(),
                true,
            )?;

            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|g| g.value().to_vec());
            if let Some(attr_bytes) = attr_bytes {
                let mut attr: InodeAttr = decode(&attr_bytes)?;
                let has_committed_manifest =
                    Self::regular_file_committed_size_in_txn(&txn, ino)?.unwrap_or(0) > 0;
                Self::transition_upload_state_for_local_mutation(&mut attr, has_committed_manifest);
                drop(inodes);
                Self::set_inode_overlay_in_txn(&txn, &attr)?;
                Self::sync_inode_lifecycle_in_txn(&txn, ino)?;
            }
        }

        Self::append_event_in_txn(
            &txn,
            &crate::events::MetaEvent::SetFileBlock {
                ino,
                block_index,
                block: block.clone(),
            },
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(old_block)
    }

    fn get_file_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_read_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let base_chunks = Self::regular_file_chunk_states_for_inode_in_read_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;
        let block_size = self.regular_file_graph_block_size()?;
        let graph_manifest = Self::manifest_from_chunk_states(
            ino,
            Some(&overlay_chunks),
            Some(&base_chunks),
            block_size,
        )?;
        Ok(graph_manifest)
    }

    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let key = encode_regular_file_chunk_key(ino, chunk_id);
        let overlay: Option<RegularFileChunkState> = overlay_table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode(value.value()))
            .transpose()?;
        let base: Option<RegularFileChunkState> = base_table
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode(value.value()))
            .transpose()?;
        Ok(resolve_chunk_slice_spans(
            chunk_id,
            chunk_range,
            overlay.as_ref(),
            base.as_ref(),
        ))
    }

    fn lookup_regular_file_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| MetaError::Storage("regular-file lookup range overflow".to_string()))?;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let query_end = end.min(chunk_end);
            let start_in_chunk = (cursor - chunk_start) as u32;
            let end_in_chunk = (query_end - chunk_start) as u32;
            let query = SliceRange::new(start_in_chunk, end_in_chunk).ok_or_else(|| {
                MetaError::Storage("invalid chunk-local lookup range".to_string())
            })?;
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let overlay: Option<RegularFileChunkState> = overlay_table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode(value.value()))
                .transpose()?;
            let base: Option<RegularFileChunkState> = base_table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode(value.value()))
                .transpose()?;
            spans.extend(resolve_chunk_slice_spans(
                chunk_id,
                query,
                overlay.as_ref(),
                base.as_ref(),
            ));
            cursor = query_end;
        }
        Ok(merge_adjacent_spans(spans))
    }

    fn remove_file_blocks(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let removed_ids;
        {
            let mut removed_object_ids = std::collections::BTreeSet::new();
            let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )?;
            let base_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                ino,
            )?;
            for chunk_state in overlay_chunks.values().chain(base_chunks.values()) {
                for slice in chunk_state.slices.values() {
                    if let Some(block) = &slice.block {
                        removed_object_ids.insert(block.object_id);
                    }
                }
            }
            removed_ids = removed_object_ids.into_iter().collect();

            Self::clear_regular_file_graph_in_txn(&txn, ino)?;
            Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveFileBlocks { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_ids)
    }

    fn remove_file_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<u64>> {
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let removed_id;
        let mut updated_attr: Option<InodeAttr> = None;
        {
            let current_block = Self::current_block_in_txn(&txn, ino, block_index, block_size)?;
            let base_block = Self::base_block_in_txn(&txn, ino, block_index, block_size)?;
            removed_id = current_block
                .as_ref()
                .map(|block| block.object_id)
                .or(base_block.as_ref().map(|block| block.object_id));

            if current_block.is_some() || base_block.is_some() {
                let overlay_tombstones = Self::chunk_slices_for_block(
                    block_index,
                    block_size,
                    None,
                    BlockRefState::LocalOnly,
                )?;
                Self::apply_regular_file_chunk_slices_in_txn(
                    &txn,
                    REGULAR_FILE_OVERLAY_TABLE,
                    ino,
                    overlay_tombstones.as_slice(),
                    true,
                )?;
            }
            if base_block.is_some() {
                let base_tombstones = Self::chunk_slices_for_block(
                    block_index,
                    block_size,
                    None,
                    BlockRefState::Published,
                )?;
                Self::apply_regular_file_chunk_slices_in_txn(
                    &txn,
                    REGULAR_FILE_BASE_TABLE,
                    ino,
                    base_tombstones.as_slice(),
                    false,
                )?;
            }

            if removed_id.is_some() {
                let new_committed_size =
                    Self::regular_file_committed_size_in_txn(&txn, ino)?.unwrap_or(0);
                let inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let attr_bytes = inodes
                    .get(ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|g| g.value().to_vec());
                if let Some(attr_bytes) = attr_bytes {
                    let mut attr: InodeAttr = decode(&attr_bytes)?;
                    attr.committed_size = new_committed_size;
                    let has_committed_manifest = new_committed_size > 0;
                    Self::transition_upload_state_for_local_mutation(
                        &mut attr,
                        has_committed_manifest,
                    );
                    drop(inodes);
                    Self::set_inode_overlay_in_txn(&txn, &attr)?;
                    Self::sync_inode_lifecycle_in_txn(&txn, ino)?;
                    updated_attr = Some(attr);
                }

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::RemoveFileBlock { ino, block_index },
                )?;
                if let Some(attr) = &updated_attr {
                    Self::append_event_in_txn(
                        &txn,
                        &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                    )?;
                }
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_id)
    }

    fn next_block_id(&self) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let id;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            id = match table
                .get(counters::NEXT_BLOCK_ID)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1, // Start from 1
            };
            table
                .insert(counters::NEXT_BLOCK_ID, id + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(id)
    }

    /// Allocate `count` consecutive block IDs in a single redb transaction.
    fn next_block_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let first_id;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            first_id = match table
                .get(counters::NEXT_BLOCK_ID)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => 1,
            };
            table
                .insert(counters::NEXT_BLOCK_ID, first_id + count as u64)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok((first_id..first_id + count as u64).collect())
    }

    /// Flush a batch of block writes in a **single** redb transaction.
    ///
    /// This replaces N individual transactions (each with its own fsync) with
    /// one transaction - reducing commit overhead by ~100-300x.
    fn flush_blocks_batch(&self, entries: &[BatchFlushEntry]) -> MetaResult<Vec<BatchFlushResult>> {
        self.flush_blocks_batch_with_attr_txn(entries, None)
    }

    fn flush_regular_file_overlay_slices(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
    ) -> MetaResult<()> {
        self.flush_regular_file_overlay_slices_with_attr_txn(ino, chunk_slices, None)
            .map(|_| ())
    }

    fn has_local_regular_file_overlay(&self, ino: Ino) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::has_regular_file_chunk_rows_in_read_txn(&txn, REGULAR_FILE_OVERLAY_TABLE, ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut ids = std::collections::BTreeSet::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let chunk: RegularFileChunkState = decode(value.value())?;
            for slice in chunk.slices.values() {
                if let Some(block) = &slice.block {
                    ids.insert(block.object_id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_read_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let base_chunks = Self::regular_file_chunk_states_for_inode_in_read_txn(
            &txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;
        let mut usage = std::collections::BTreeMap::new();
        for chunk_state in overlay_chunks.values().chain(base_chunks.values()) {
            for slice in chunk_state.slices.values() {
                if let Some(block) = &slice.block {
                    record_regular_file_object_usage(&mut usage, block);
                }
            }
        }
        Ok(usage.into_iter().collect())
    }

    fn has_regular_file_overlay_object_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let attr_bytes = match inodes
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(g) => g.value().to_vec(),
            None => return Ok(false),
        };
        let attr: InodeAttr = decode(&attr_bytes)?;
        if attr.content_epoch != expected_epoch {
            return Ok(false);
        }
        drop(inodes);

        let table = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let chunk: RegularFileChunkState = decode(value.value())?;
            if chunk
                .slices
                .values()
                .filter_map(|slice| slice.block.as_ref())
                .any(|block| block.object_id == object_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let changed_chunk_slices = {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )?;
            let mut changed_chunk_slices = Vec::new();
            let mut matched_object = false;

            for (chunk_id, mut chunk_state) in overlay_chunks {
                let mut changed = false;
                for slice in chunk_state.slices.values_mut() {
                    let Some(block) = slice.block.as_ref() else {
                        continue;
                    };
                    if block.object_id != object_id {
                        continue;
                    }
                    matched_object = true;
                    if slice.block_state != BlockRefState::Uploaded {
                        slice.block_state = BlockRefState::Uploaded;
                        changed = true;
                    }
                }
                if changed {
                    let chunk_slices: Vec<_> = chunk_state.slices.values().cloned().collect();
                    Self::set_regular_file_chunk_state_in_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        true,
                        ino,
                        chunk_id,
                        chunk_slices.as_slice(),
                    )?;
                    changed_chunk_slices.push((chunk_id, chunk_slices));
                }
            }

            if !matched_object {
                return Ok(false);
            }

            if !changed_chunk_slices.is_empty() {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                        ino,
                        chunk_slices: changed_chunk_slices.clone(),
                    },
                )?;
                Self::sync_unpublished_upload_progress_lifecycle_in_txn(&txn, &attr)?;
            }

            changed_chunk_slices
        };

        if changed_chunk_slices.is_empty() {
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(true);
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn commit_uploaded_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            let current = Self::current_block_in_txn(&txn, ino, block_index, block_size)?;
            let Some(current) = current else {
                return Ok(false);
            };
            if current != *block {
                return Ok(false);
            }

            let old_committed = Self::base_block_in_txn(&txn, ino, block_index, block_size)?;
            let committed_slices = Self::chunk_slices_for_block(
                block_index,
                block_size,
                Some(block),
                BlockRefState::Published,
            )?;
            Self::apply_regular_file_chunk_slices_in_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                ino,
                committed_slices.as_slice(),
                false,
            )?;
            let new_committed_size =
                Self::regular_file_committed_size_in_txn(&txn, ino)?.unwrap_or(0);

            let previous_committed_size = attr.committed_size;
            let previous_committed_epoch = attr.committed_content_epoch;
            let previous_upload_state = attr.upload_state;
            let manifest_matches_committed = Self::manifest_matches_committed_in_txn(&txn, ino)?;
            attr.committed_size = new_committed_size;
            if manifest_matches_committed {
                attr.committed_content_epoch = attr.content_epoch;
                attr.upload_state = UploadState::Ready;
            } else {
                Self::transition_upload_state_for_local_mutation(&mut attr, new_committed_size > 0);
            }
            let committed_changed = previous_committed_size != attr.committed_size
                || previous_committed_epoch != attr.committed_content_epoch
                || previous_upload_state != attr.upload_state;
            Self::set_inode_overlay_in_txn(&txn, &attr)?;
            Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::CommitFileBlock {
                    ino,
                    block_index,
                    block: block.clone(),
                },
            )?;
            if committed_changed {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
            }

            if let Some(old) = old_committed
                && old.object_id != block.object_id
            {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let mut pending_del_table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending_del_table
                    .insert(old.object_id, timestamp)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_del_table);
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddPendingDeletion {
                        object_id: old.object_id,
                        timestamp,
                    },
                )?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn mark_file_block_uploaded_if_matches(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            let current = Self::current_block_in_txn(&txn, ino, block_index, block_size)?;
            let Some(current) = current else {
                return Ok(false);
            };
            if current != *block {
                return Ok(false);
            }

            let uploaded_slices = Self::chunk_slices_for_block(
                block_index,
                block_size,
                Some(block),
                BlockRefState::Uploaded,
            )?;
            Self::apply_regular_file_chunk_slices_in_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
                uploaded_slices.as_slice(),
                true,
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                    ino,
                    chunk_slices: uploaded_slices.clone(),
                },
            )?;
            Self::sync_unpublished_upload_progress_lifecycle_in_txn(&txn, &attr)?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn publish_file_manifest_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        expected_manifest: &[(u32, BlockRef)],
    ) -> MetaResult<bool> {
        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let published_blocks;
        {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }
            drop(inodes);

            published_blocks = Self::current_manifest_in_txn(&txn, ino)?;
            if published_blocks.as_slice() != expected_manifest {
                return Ok(false);
            }

            let publish_regular_graph = Self::has_regular_file_chunk_rows_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )?;
            let (regular_graph_committed_size, removed_object_ids, publish_event) =
                if publish_regular_graph {
                    let (committed_size, removed_object_ids) =
                        Self::publish_regular_file_overlay_to_base_in_txn(&txn, ino)?;
                    let published_base_chunks =
                        Self::regular_file_chunk_rows_for_inode_in_write_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            ino,
                        )?;
                    (
                        Some(committed_size),
                        removed_object_ids,
                        crate::events::MetaEvent::PublishRegularFileBaseSlices {
                            ino,
                            chunk_slices: published_base_chunks,
                        },
                    )
                } else {
                    let previous_base_manifest =
                        Self::base_manifest_from_graph_rows_in_txn(&txn, ino)?;
                    Self::replace_regular_file_chunk_rows_from_blocks_in_txn(
                        &txn,
                        REGULAR_FILE_BASE_TABLE,
                        ino,
                        published_blocks.as_slice(),
                        block_size,
                        true,
                        false,
                    )?;
                    let previous_ids: std::collections::HashSet<_> = previous_base_manifest
                        .iter()
                        .map(|(_, block)| block.object_id)
                        .collect();
                    let next_ids: std::collections::HashSet<_> = published_blocks
                        .iter()
                        .map(|(_, block)| block.object_id)
                        .collect();
                    let published_base_chunks =
                        Self::regular_file_chunk_rows_for_inode_in_write_txn(
                            &txn,
                            REGULAR_FILE_BASE_TABLE,
                            ino,
                        )?;
                    (
                        Some(Self::legacy_committed_size_from_blocks(
                            published_blocks.as_slice(),
                        )),
                        previous_ids.difference(&next_ids).copied().collect(),
                        crate::events::MetaEvent::PublishRegularFileBaseSlices {
                            ino,
                            chunk_slices: published_base_chunks,
                        },
                    )
                };

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            for object_id in removed_object_ids {
                let mut pending_del_table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending_del_table
                    .insert(object_id, timestamp)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                drop(pending_del_table);
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::AddPendingDeletion {
                        object_id,
                        timestamp,
                    },
                )?;
            }

            attr.committed_size = regular_graph_committed_size
                .map(|size| size.min(attr.size))
                .unwrap_or(attr.size);
            attr.committed_content_epoch = attr.content_epoch;
            attr.upload_state = UploadState::Ready;
            Self::set_inode_overlay_in_txn(&txn, &attr)?;
            Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &publish_event)?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn publish_regular_file_overlay_to_base_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(false),
            };
            let mut attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(false);
            }

            let (committed_size, removed_object_ids) =
                Self::publish_regular_file_overlay_to_base_in_txn(&txn, ino)?;
            let published_base_chunks = Self::regular_file_chunk_rows_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                ino,
            )?;
            if !removed_object_ids.is_empty() {
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let mut pending_del_table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for object_id in removed_object_ids {
                    pending_del_table
                        .insert(object_id, timestamp)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    Self::append_event_in_txn(
                        &txn,
                        &crate::events::MetaEvent::AddPendingDeletion {
                            object_id,
                            timestamp,
                        },
                    )?;
                }
            }
            attr.committed_size = committed_size.min(attr.size);
            attr.committed_content_epoch = attr.content_epoch;
            attr.upload_state = UploadState::Ready;
            drop(inodes);
            Self::set_inode_overlay_in_txn(&txn, &attr)?;
            Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::PublishRegularFileBaseSlices {
                    ino,
                    chunk_slices: published_base_chunks,
                },
            )?;
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(true)
    }

    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        if max_chunks == 0 {
            return Ok(0);
        }
        const COMPACTION_TXN_BATCH_SIZE: usize = 64;

        let mut compacted = 0usize;
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let pending_table = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut candidates: Vec<(Ino, ChunkId)> = Vec::new();

        for row in pending_table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, chunk_id) = decode_regular_file_chunk_key(key.value())?;
            candidates.push((ino, chunk_id));
            if candidates.len() >= max_chunks {
                break;
            }
        }
        drop(pending_table);
        drop(txn);

        for batch in candidates.chunks(COMPACTION_TXN_BATCH_SIZE) {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut pending_table = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for &(ino, chunk_id) in batch {
                if compacted >= max_chunks {
                    break;
                }

                let key = encode_regular_file_chunk_key(ino, chunk_id);
                let current = overlay_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|value| decode::<RegularFileChunkState>(value.value()))
                    .transpose()?;

                let Some(current) = current else {
                    let _ = pending_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    continue;
                };
                if !current.fragmentation_metrics().should_compact() {
                    let _ = pending_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    continue;
                }

                let Some(compacted_chunk) =
                    Self::compact_regular_file_chunk_state(chunk_id, &current)?
                else {
                    let _ = overlay_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = pending_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    Self::append_event_in_txn(
                        &txn,
                        &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                            ino,
                            chunk_slices: vec![(chunk_id, Vec::new())],
                        },
                    )?;
                    compacted = compacted.saturating_add(1);
                    continue;
                };

                if compacted_chunk == current {
                    let _ = pending_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    continue;
                }

                let encoded = encode(&compacted_chunk)?;
                overlay_table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;

                let compacted_slices: Vec<crate::schema::SliceRecord> =
                    compacted_chunk.slices.values().cloned().collect();
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                        ino,
                        chunk_slices: vec![(chunk_id, compacted_slices)],
                    },
                )?;

                if compacted_chunk.fragmentation.should_compact() {
                    let _ = pending_table
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending_table
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
                compacted = compacted.saturating_add(1);
            }

            drop(pending_table);
            drop(overlay_table);
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if compacted >= max_chunks {
                break;
            }
        }

        Ok(compacted)
    }
}

impl RedbMetaEngine {
    fn flush_regular_file_overlay_slices_with_attr_txn(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let normalized_attr = if let Some(attr) = attr {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_manifest =
                    !Self::base_manifest_from_graph_rows_in_txn(&txn, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Some(normalized)
        } else {
            None
        };
        let mut final_chunk_slices: Vec<(ChunkId, Vec<crate::schema::SliceRecord>)> =
            Vec::with_capacity(chunk_slices.len());

        {
            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut counters = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut pending_compaction = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            let mut space_delta: i64 = 0;

            for (chunk_id, new_slices) in chunk_slices {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let mut state = if let Some(entry) = overlay_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    decode::<crate::schema::RegularFileChunkState>(entry.value())?
                } else {
                    crate::schema::RegularFileChunkState::default()
                };

                // Pre-calculate which existing slices are fully covered by the incoming ones.
                // This bounds metadata growth and performs O(N*M) where N is incoming slices and M is existing in one chunk.
                let mut fully_covered = Vec::new();
                for (id, existing) in &state.slices {
                    for new_slice in new_slices.iter() {
                        if new_slice.range.start <= existing.range.start
                            && new_slice.range.end >= existing.range.end
                        {
                            fully_covered.push(*id);
                            break;
                        }
                    }
                }

                for id in fully_covered {
                    if let Some(removed) = state.slices.remove(&id) {
                        if let Some(block) = &removed.block {
                            space_delta -= block.length as i64;
                        }
                    }
                }

                for mut new_slice in new_slices.iter().cloned() {
                    let slice_id = state
                        .try_alloc_slice_id()
                        .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
                    let slice_order = state
                        .try_alloc_slice_order()
                        .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
                    new_slice.slice_id = slice_id;
                    new_slice.order = slice_order;

                    if let Some(block) = &new_slice.block {
                        space_delta += block.length as i64;
                    }

                    state.slices.insert(slice_id, new_slice);
                }

                state.refresh_fragmentation_metrics();

                if state.fragmentation.should_compact() {
                    pending_compaction
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending_compaction
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }

                let data = encode(&state)?;
                overlay_table
                    .insert(key.as_slice(), data.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                final_chunk_slices.push((*chunk_id, state.slices.values().cloned().collect()));
            }

            if space_delta != 0 {
                let current_used = match counters
                    .get(counters::USED_SPACE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    Some(v) => v.value(),
                    None => 0,
                };
                counters
                    .insert(
                        counters::USED_SPACE,
                        current_used.saturating_add_signed(space_delta),
                    )
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::set_inode_overlay_in_txn(&txn, attr)?;
            }
        }
        Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

        Self::append_event_in_txn(
            &txn,
            &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                ino,
                chunk_slices: final_chunk_slices,
            },
        )?;
        if let Some(attr) = normalized_attr.as_ref() {
            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(normalized_attr)
    }

    fn flush_blocks_batch_with_attr_txn(
        &self,
        entries: &[BatchFlushEntry],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Vec<BatchFlushResult>> {
        if entries.is_empty() && attr.is_none() {
            return Ok(Vec::new());
        }

        let block_size = self.regular_file_graph_block_size()?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let mut results = Vec::with_capacity(entries.len());
        let mut used_space_delta: i64 = 0;

        let normalized_attr = if let Some(attr) = attr {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_manifest =
                    !Self::base_manifest_from_graph_rows_in_txn(&txn, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Some(normalized)
        } else {
            None
        };

        {
            let mut touched_inodes = std::collections::BTreeSet::new();
            let mut overlay_usage_before = std::collections::BTreeMap::<Ino, u64>::new();
            if !entries.is_empty() {
                for entry in entries {
                    touched_inodes.insert(entry.ino);
                }
                for &ino in &touched_inodes {
                    let chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        ino,
                    )?;
                    overlay_usage_before
                        .insert(ino, regular_file_chunk_states_usage_bytes(&chunks));
                }

                for e in entries {
                    let block_ref = BlockRef {
                        object_id: e.object_id,
                        offset: 0,
                        length: e.length,
                        flags: e.flags,
                    };
                    let old_block =
                        match Self::current_block_in_txn(&txn, e.ino, e.block_index, block_size) {
                            Ok(old_block) => old_block,
                            Err(MetaError::FragmentedBlock { .. }) => None,
                            Err(err) => return Err(err),
                        };
                    let chunk_slices = Self::chunk_slices_for_block(
                        e.block_index,
                        block_size,
                        Some(&block_ref),
                        BlockRefState::LocalOnly,
                    )?;
                    Self::apply_regular_file_chunk_slices_in_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        e.ino,
                        chunk_slices.as_slice(),
                        true,
                    )?;
                    results.push(BatchFlushResult { old_block });
                }

                for &ino in &touched_inodes {
                    let chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                        &txn,
                        REGULAR_FILE_OVERLAY_TABLE,
                        ino,
                    )?;
                    let before = overlay_usage_before.get(&ino).copied().unwrap_or(0);
                    let after = regular_file_chunk_states_usage_bytes(&chunks);
                    used_space_delta += after as i64 - before as i64;
                }
            }

            if !touched_inodes.is_empty() {
                let inodes = txn
                    .open_table(INODES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let mut current_attrs = Vec::new();
                for &ino in &touched_inodes {
                    if normalized_attr.as_ref().is_some_and(|attr| attr.ino == ino) {
                        continue;
                    }
                    let attr_bytes = inodes
                        .get(ino)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                        .map(|g| g.value().to_vec());
                    if let Some(attr_bytes) = attr_bytes {
                        let mut current_attr: InodeAttr = decode(&attr_bytes)?;
                        let has_committed_manifest =
                            !Self::base_manifest_from_graph_rows_in_txn(&txn, ino)?.is_empty();
                        Self::transition_upload_state_for_local_mutation(
                            &mut current_attr,
                            has_committed_manifest,
                        );
                        current_attrs.push(current_attr);
                    }
                }
                drop(inodes);
                for current_attr in current_attrs {
                    Self::set_inode_overlay_in_txn(&txn, &current_attr)?;
                }
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::set_inode_overlay_in_txn(&txn, attr)?;
            }
            if let Some(attr) = normalized_attr.as_ref() {
                touched_inodes.insert(attr.ino);
            }
            for &ino in &touched_inodes {
                Self::sync_inode_lifecycle_in_txn(&txn, ino)?;
            }

            if !entries.is_empty() {
                {
                    let mut counters_table = txn
                        .open_table(COUNTERS_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    let current_used = match counters_table
                        .get(counters::USED_SPACE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?
                    {
                        Some(v) => v.value(),
                        None => 0,
                    };
                    let new_used = if used_space_delta >= 0 {
                        current_used.saturating_add(used_space_delta as u64)
                    } else {
                        current_used.saturating_sub((-used_space_delta) as u64)
                    };
                    counters_table
                        .insert(counters::USED_SPACE, new_used)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }

                let batch_blocks: Vec<(Ino, u32, BlockRef)> = entries
                    .iter()
                    .map(|e| {
                        (
                            e.ino,
                            e.block_index,
                            BlockRef {
                                object_id: e.object_id,
                                offset: 0,
                                length: e.length,
                                flags: e.flags,
                            },
                        )
                    })
                    .collect();

                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetFileBlocksBatch {
                        blocks: batch_blocks,
                    },
                )?;
            }

            if let Some(attr) = normalized_attr.as_ref() {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
                )?;
            }
        }

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(results)
    }
}

impl crate::engine::MetaEngine for RedbMetaEngine {
    fn flush_blocks_batch_with_attr(
        &self,
        entries: &[BatchFlushEntry],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Vec<BatchFlushResult>> {
        self.flush_blocks_batch_with_attr_txn(entries, attr)
    }

    fn flush_regular_file_overlay_slices_with_attr(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        self.flush_regular_file_overlay_slices_with_attr_txn(ino, chunk_slices, attr)
    }
}
