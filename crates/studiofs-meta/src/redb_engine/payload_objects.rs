use super::*;

fn record_regular_file_object_usage(
    usage: &mut std::collections::BTreeMap<u64, u64>,
    payload: &PayloadRef,
) {
    let object_bytes = u64::from(payload.offset).saturating_add(u64::from(payload.length));
    usage
        .entry(payload.object_id)
        .and_modify(|existing| *existing = (*existing).max(object_bytes))
        .or_insert(object_bytes);
}

/// Upload-completed slices of one chunk, indexed by their immutable payload
/// reference so reconciling a whole incoming chunk stays linear in its size.
pub(super) struct CompletedOverlayPayloads<'a> {
    by_payload: std::collections::HashMap<(u64, u32, u32, u8), Vec<&'a crate::schema::SliceRecord>>,
}

impl<'a> CompletedOverlayPayloads<'a> {
    pub(super) fn new(state: &'a RegularFileChunkState) -> Self {
        let mut by_payload: std::collections::HashMap<_, Vec<_>> = std::collections::HashMap::new();
        // Candidates keep `state.slices` order, so the first match is unchanged.
        for existing in state.slices.values() {
            if let (Some(payload), true) = (
                existing.payload,
                matches!(
                    existing.payload_state,
                    crate::schema::PayloadRefState::Uploaded
                        | crate::schema::PayloadRefState::Published
                ),
            ) {
                by_payload
                    .entry(payload_key(payload))
                    .or_default()
                    .push(existing);
            }
        }
        Self { by_payload }
    }

    /// Payload objects are immutable. A late flush of the same object may
    /// replace its slice record, but it must not erase upload completion.
    pub(super) fn preserve(&self, new_slice: &mut crate::schema::SliceRecord) {
        let Some(payload) = new_slice.payload else {
            return;
        };
        if !matches!(
            new_slice.payload_state,
            crate::schema::PayloadRefState::LocalOnly
                | crate::schema::PayloadRefState::Uploading
                | crate::schema::PayloadRefState::Recovering
        ) {
            return;
        }
        let Some(candidates) = self.by_payload.get(&payload_key(payload)) else {
            return;
        };
        if let Some(existing) = candidates.iter().find(|existing| {
            new_slice.range.start <= existing.range.start
                && new_slice.range.end >= existing.range.end
        }) {
            new_slice.payload_state = existing.payload_state;
        }
    }
}

fn payload_key(payload: PayloadRef) -> (u64, u32, u32, u8) {
    (
        payload.object_id,
        payload.offset,
        payload.length,
        payload.flags.bits(),
    )
}

/// Answers whether any single slice of a batch contains a range, in
/// O(log n) per query instead of scanning the batch.
struct SingleSliceCover {
    /// Slice starts in ascending order.
    starts: Vec<u32>,
    /// Largest end among slices whose start is at or before `starts[i]`.
    max_end_through: Vec<u32>,
}

impl SingleSliceCover {
    fn new(slices: &[crate::schema::SliceRecord]) -> Self {
        let mut ranges: Vec<_> = slices
            .iter()
            .map(|slice| (slice.range.start, slice.range.end))
            .collect();
        ranges.sort_unstable();
        let mut max_end = 0;
        let (starts, max_end_through) = ranges
            .into_iter()
            .map(|(start, end)| {
                max_end = max_end.max(end);
                (start, max_end)
            })
            .unzip();
        Self {
            starts,
            max_end_through,
        }
    }

    fn covers(&self, range: crate::schema::SliceRange) -> bool {
        // Slices starting after `range.start` cannot contain it.
        let candidates = self.starts.partition_point(|start| *start <= range.start);
        candidates > 0 && self.max_end_through[candidates - 1] >= range.end
    }
}

impl RedbMetaEngine {
    pub(crate) fn has_local_regular_file_overlay_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        Self::has_regular_file_chunk_rows_in_write_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)
    }
}

impl MetaPayloadObjects for RedbMetaEngine {
    fn lookup_regular_file_chunk_slices(
        &self,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_range: SliceRange,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        let txn = self.begin_read_txn()?;
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
        let txn = self.begin_read_txn()?;
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

    fn remove_regular_file_graph(
        &self,
        ino: Ino,
        deletion_timestamp: Option<i64>,
    ) -> MetaResult<Vec<u64>> {
        let txn = self.begin_write_txn()?;
        let removed_ids: Vec<u64>;
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
                    if let Some(block) = &slice.payload {
                        removed_object_ids.insert(block.object_id);
                    }
                }
            }
            removed_ids = removed_object_ids.into_iter().collect();

            Self::clear_regular_file_graph_in_txn(&txn, ino)?;
            Self::remove_pending_regular_file_recovery_in_txn(&txn, ino)?;
            Self::sync_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveRegularFileGraph { ino },
            )?;
        }
        if let Some(timestamp) = deletion_timestamp {
            fs_ops::NamespaceDeleteTxn::new(&txn)
                .queue_pending_deletions_in_txn(&removed_ids, timestamp)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(removed_ids)
    }

    fn next_object_id(&self) -> MetaResult<u64> {
        self.next_object_ids(1)?
            .into_iter()
            .next()
            .ok_or_else(|| MetaError::Storage("missing allocated object ID".to_string()))
    }

    /// Allocate `count` consecutive object IDs from the shared local inode/object stream.
    fn next_object_ids(&self, count: u32) -> MetaResult<Vec<u64>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let txn = self.begin_write_txn()?;
        let first_id;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            first_id = match table
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => ROOT_INO + 1,
            };
            let next_id = first_id
                .checked_add(count as u64)
                .ok_or_else(|| MetaError::Storage("object ID allocation overflow".to_string()))?;
            table
                .insert(counters::NEXT_INODE, next_id)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok((first_id..first_id + count as u64).collect())
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
        let txn = self.begin_read_txn()?;
        Self::has_regular_file_chunk_rows_in_read_txn(&txn, REGULAR_FILE_OVERLAY_TABLE, ino)
    }

    fn get_regular_file_overlay_object_ids(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        let txn = self.begin_read_txn()?;
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
                if let Some(block) = &slice.payload {
                    ids.insert(block.object_id);
                }
            }
        }
        Ok(ids.into_iter().collect())
    }

    fn get_regular_file_graph_object_usage(&self, ino: Ino) -> MetaResult<Vec<(u64, u64)>> {
        let txn = self.begin_read_txn()?;
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
                if let Some(block) = &slice.payload {
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
        Ok(!self
            .regular_file_overlay_objects_if_match(ino, expected_epoch, &[object_id])?
            .is_empty())
    }

    fn regular_file_overlay_objects_if_match(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_ids: &[u64],
    ) -> MetaResult<Vec<u64>> {
        if object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let attr_bytes = match inodes
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(g) => g.value().to_vec(),
            None => return Ok(Vec::new()),
        };
        let attr: InodeAttr = decode(&attr_bytes)?;
        if attr.content_epoch != expected_epoch {
            return Ok(Vec::new());
        }
        drop(inodes);

        // One overlay scan answers the whole batch; it stops once every
        // requested object has been seen.
        let mut unseen: HashSet<u64> = object_ids.iter().copied().collect();
        let mut referenced = HashSet::new();
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
            for block in chunk.slices.values().filter_map(|slice| slice.payload) {
                if unseen.remove(&block.object_id) {
                    referenced.insert(block.object_id);
                }
            }
            if unseen.is_empty() {
                break;
            }
        }
        Ok(object_ids
            .iter()
            .copied()
            .filter(|object_id| referenced.contains(object_id))
            .collect())
    }

    fn mark_regular_file_overlay_object_uploaded_if_matches(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_id: u64,
    ) -> MetaResult<bool> {
        Ok(self
            .mark_regular_file_overlay_objects_uploaded_if_match(ino, expected_epoch, &[object_id])?
            .contains(&object_id))
    }

    fn mark_regular_file_overlay_objects_uploaded_if_match(
        &self,
        ino: Ino,
        expected_epoch: u64,
        object_ids: &[u64],
    ) -> MetaResult<Vec<u64>> {
        if object_ids.is_empty() {
            return Ok(Vec::new());
        }
        let object_ids_set: HashSet<u64> = object_ids.iter().copied().collect();
        let txn = self.begin_write_txn()?;
        let matched_ids = {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_bytes = match inodes
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(g) => g.value().to_vec(),
                None => return Ok(Vec::new()),
            };
            let attr: InodeAttr = decode(&attr_bytes)?;
            if attr.content_epoch != expected_epoch {
                return Ok(Vec::new());
            }
            drop(inodes);

            let mut overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )?;
            let mut changed_chunk_slices = Vec::new();
            let mut matched_ids = HashSet::new();

            for (&chunk_id, chunk_state) in &mut overlay_chunks {
                let mut changed = false;
                for slice in chunk_state.slices.values_mut() {
                    let Some(block) = slice.payload.as_ref() else {
                        continue;
                    };
                    if !object_ids_set.contains(&block.object_id) {
                        continue;
                    }
                    matched_ids.insert(block.object_id);
                    if slice.payload_state != PayloadRefState::Uploaded {
                        slice.payload_state = PayloadRefState::Uploaded;
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

            if matched_ids.is_empty() {
                return Ok(Vec::new());
            }

            if !changed_chunk_slices.is_empty() {
                Self::append_event_in_txn(
                    &txn,
                    &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                        ino,
                        chunk_slices: changed_chunk_slices.clone(),
                    },
                )?;
            }
            // The updated chunks are the overlay this transaction commits, so
            // lifecycle and recovery reuse them instead of decoding it again.
            Self::sync_unpublished_upload_progress_lifecycle_with_chunks_in_txn(
                &txn,
                &attr,
                &overlay_chunks,
            )?;

            matched_ids
        };
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(object_ids
            .iter()
            .copied()
            .filter(|object_id| matched_ids.contains(object_id))
            .collect())
    }

    fn publish_regular_file_overlay_to_base_if_matches_batch(
        &self,
        requests: &[(Ino, u64)],
    ) -> MetaResult<Vec<bool>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.begin_write_txn()?;
        let mut published = Vec::with_capacity(requests.len());
        for &(ino, expected_epoch) in requests {
            published.push(
                Self::publish_regular_file_overlay_to_base_if_matches_in_txn(
                    &txn,
                    ino,
                    expected_epoch,
                )?,
            );
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(published)
    }

    fn compact_fragmented_regular_file_chunks(&self, max_chunks: usize) -> MetaResult<usize> {
        if max_chunks == 0 {
            return Ok(0);
        }
        const COMPACTION_TXN_BATCH_SIZE: usize = 64;

        let mut compacted = 0usize;
        let txn = self.begin_read_txn()?;
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
            let txn = self.begin_write_txn()?;
            let mut recovery_inodes = HashSet::new();
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
                    recovery_inodes.insert(ino);
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
                recovery_inodes.insert(ino);

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
            for ino in recovery_inodes {
                Self::sync_pending_regular_file_recovery_for_inode_in_txn(&txn, ino)?;
            }
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
    pub(crate) fn list_published_object_references(&self) -> MetaResult<Vec<(Ino, u64)>> {
        let txn = self.begin_read_txn()?;
        let chunks = Self::regular_file_chunk_states_from_txn(&txn, REGULAR_FILE_BASE_TABLE)?;
        let mut references = std::collections::BTreeSet::new();
        for (ino, chunks) in chunks {
            for chunk in chunks.values() {
                for slice in chunk.slices.values() {
                    if slice.payload_state == PayloadRefState::Published
                        && let Some(payload) = &slice.payload
                    {
                        references.insert((ino, payload.object_id));
                    }
                }
            }
        }
        Ok(references.into_iter().collect())
    }

    fn publish_regular_file_overlay_to_base_if_matches_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        expected_epoch: u64,
    ) -> MetaResult<bool> {
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
        let base_regular_file = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let was_remote_visible = Self::regular_file_remote_visible_without_published_base(&attr)
            || Self::regular_file_has_published_base_from_opened_table(&base_regular_file, ino)?;
        drop(base_regular_file);
        let is_unpublished_empty = !was_remote_visible
            && attr.size == 0
            && attr.committed_size == 0
            && attr.content_epoch == 0
            && attr.committed_content_epoch == 0;

        let (payload_covered_size, removed_object_ids) =
            match Self::publish_regular_file_overlay_to_base_in_txn(txn, ino)? {
                Some(published) => published,
                None if is_unpublished_empty => (0, Vec::new()),
                None => return Ok(false),
            };
        let published_base_chunks = Self::regular_file_chunk_rows_for_inode_in_write_txn(
            txn,
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
                    txn,
                    &crate::events::MetaEvent::AddPendingDeletion {
                        object_id,
                        timestamp,
                    },
                )?;
            }
        }
        if is_unpublished_empty {
            attr.content_epoch = attr.content_epoch.saturating_add(1);
        }
        attr.size = attr.size.max(payload_covered_size);
        attr.committed_size = attr.size;
        attr.committed_content_epoch = attr.content_epoch;
        attr.upload_state = UploadState::Ready;
        drop(inodes);
        Self::set_inode_in_txn(txn, &attr)?;
        Self::sync_inode_lifecycle_in_txn(txn, ino)?;

        let dentry_keys =
            Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?;
        if !was_remote_visible {
            for key in dentry_keys {
                let (parent, name) = decode_dentry_key(&key)?;
                Self::append_event_in_txn(
                    txn,
                    &crate::events::MetaEvent::AddEntry {
                        parent,
                        name: name.to_vec(),
                        entry: DirEntryValue {
                            ino,
                            kind: FileType::RegularFile,
                        },
                    },
                )?;
            }
        }
        Self::append_event_in_txn(
            txn,
            &crate::events::MetaEvent::PublishRegularFileBaseSlices {
                ino,
                chunk_slices: published_base_chunks,
            },
        )?;
        Self::append_event_in_txn(txn, &crate::events::MetaEvent::SetAttr { attr })?;

        Ok(true)
    }

    fn apply_regular_file_overlay_mutation_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<(Option<InodeAttr>, bool)> {
        if let Some(attr) = attr {
            match Self::get_attr_merged_in_write_txn(txn, ino) {
                Ok(current) if current.content_epoch > attr.content_epoch => {
                    return Ok((None, false));
                }
                Ok(current)
                    if Self::is_fully_published_regular_file(&current)
                        && current.content_epoch == attr.content_epoch
                        && !Self::is_fully_published_regular_file(attr) =>
                {
                    // A flush can carry an attr snapshot that was taken before the
                    // background publisher committed this epoch. Do not let that
                    // snapshot roll a Ready inode back to Uploading after the
                    // publication ticket has already been consumed.
                    return Ok((None, false));
                }
                Ok(_) | Err(MetaError::InodeNotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        let normalized_attr = if let Some(attr) = attr {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_graph = Self::has_regular_file_chunk_rows_in_write_txn(
                    txn,
                    REGULAR_FILE_BASE_TABLE,
                    normalized.ino,
                )?;
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_graph,
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

            for (chunk_id, incoming_slices) in chunk_slices {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let mut state = if let Some(entry) = overlay_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    decode::<crate::schema::RegularFileChunkState>(entry.value())?
                } else {
                    crate::schema::RegularFileChunkState::default()
                };
                let mut new_slices = incoming_slices.to_vec();
                let completed = CompletedOverlayPayloads::new(&state);
                for new_slice in &mut new_slices {
                    completed.preserve(new_slice);
                }

                // Existing slices fully covered by a single incoming slice are
                // dropped, which bounds metadata growth.
                let incoming_cover = SingleSliceCover::new(&new_slices);
                let fully_covered: Vec<_> = state
                    .slices
                    .iter()
                    .filter(|(_, existing)| incoming_cover.covers(existing.range))
                    .map(|(id, _)| *id)
                    .collect();

                for id in fully_covered {
                    if let Some(removed) = state.slices.remove(&id)
                        && let Some(block) = &removed.payload
                    {
                        space_delta -= block.length as i64;
                    }
                }

                for mut new_slice in new_slices {
                    let slice_id = state
                        .try_alloc_slice_id()
                        .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
                    let slice_order = state
                        .try_alloc_slice_order()
                        .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
                    new_slice.slice_id = slice_id;
                    new_slice.order = slice_order;

                    if let Some(block) = &new_slice.payload {
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
                Self::set_inode_in_txn(txn, attr)?;
            }
        }

        Self::sync_inode_lifecycle_in_txn(txn, ino)?;
        Self::sync_pending_regular_file_recovery_for_inode_in_txn(txn, ino)?;

        Self::append_event_in_txn(
            txn,
            &crate::events::MetaEvent::SetRegularFileOverlaySlices {
                ino,
                chunk_slices: final_chunk_slices,
            },
        )?;
        if let Some(attr) = normalized_attr.as_ref() {
            Self::append_event_in_txn(
                txn,
                &crate::events::MetaEvent::SetAttr { attr: attr.clone() },
            )?;
        }

        Ok((normalized_attr, true))
    }

    fn is_fully_published_regular_file(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state == UploadState::Ready
            && attr.content_epoch == attr.committed_content_epoch
            && attr.size == attr.committed_size
    }

    fn flush_regular_file_overlay_slices_with_attr_txn(
        &self,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<crate::schema::SliceRecord>)],
        attr: Option<&InodeAttr>,
    ) -> MetaResult<Option<InodeAttr>> {
        let txn = self.begin_write_txn()?;
        let (result, applied) =
            self.apply_regular_file_overlay_mutation_in_txn(&txn, ino, chunk_slices, attr)?;
        if !applied {
            return Ok(result);
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(result)
    }

    fn apply_regular_file_overlay_batch(
        &self,
        mutations: &[crate::engine::RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }

        let started = std::time::Instant::now();
        let estimated = mutations.iter().fold(
            super::EstimatedBatchMetrics {
                slices: 0,
                payload_bytes: 0,
            },
            |mut estimated, mutation| {
                for (_, slices) in &mutation.chunk_slices {
                    estimated.slices += slices.len() as u64;
                    estimated.payload_bytes += slices
                        .iter()
                        .filter_map(|slice| slice.payload.as_ref())
                        .map(|payload| u64::from(payload.length))
                        .sum::<u64>();
                }
                estimated
            },
        );
        let txn = match self.begin_write_txn() {
            Ok(txn) => txn,
            Err(error) => {
                self.write_metrics.observe_batch(
                    mutations.len() as u64,
                    0,
                    estimated,
                    None,
                    true,
                    started.elapsed(),
                );
                return Err(error);
            }
        };
        let mut results = Vec::with_capacity(mutations.len());
        for (index, mutation) in mutations.iter().enumerate() {
            let result = self
                .apply_regular_file_overlay_mutation_in_txn(
                    &txn,
                    mutation.ino,
                    &mutation.chunk_slices,
                    mutation.attr.as_ref(),
                )
                .map_err(|source| MetaError::RegularFileOverlayBatchOperation {
                    index,
                    source: Box::new(source),
                });
            let (result, _applied) = match result {
                Ok(result) => result,
                Err(error) => {
                    self.write_metrics.observe_batch(
                        mutations.len() as u64,
                        results.len() as u64,
                        estimated,
                        Some(index),
                        true,
                        started.elapsed(),
                    );
                    return Err(error);
                }
            };
            results.push(result);
        }
        if let Err(error) = txn.commit() {
            self.write_metrics.observe_batch(
                mutations.len() as u64,
                results.len() as u64,
                estimated,
                None,
                true,
                started.elapsed(),
            );
            return Err(MetaError::Storage(error.to_string()));
        }
        self.write_metrics.observe_batch(
            mutations.len() as u64,
            results.len() as u64,
            estimated,
            None,
            false,
            started.elapsed(),
        );
        Ok(results)
    }
}

impl crate::engine::MetaEngine for RedbMetaEngine {
    fn apply_regular_file_overlay_batch(
        &self,
        mutations: &[crate::engine::RegularFileOverlayMutation],
    ) -> MetaResult<Vec<Option<InodeAttr>>> {
        self.apply_regular_file_overlay_batch(mutations)
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

#[cfg(test)]
mod overlay_index_tests {
    use super::*;
    use crate::schema::{
        BlockFlags, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
    };

    fn slice(start: u32, end: u32, object_id: u64, state: PayloadRefState) -> SliceRecord {
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(start, end).expect("slice range"),
            payload: Some(PayloadRef {
                object_id,
                offset: 0,
                length: end - start,
                flags: BlockFlags::PARTIAL_BLOCK,
            }),
            payload_state: state,
        }
    }

    #[test]
    fn single_slice_cover_matches_pairwise_scan() {
        let mut seed = 0x2545_f491_u32;
        let mut next = move |bound: u32| {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            seed % bound
        };
        for _ in 0..200 {
            let batch: Vec<_> = (0..1 + next(12))
                .map(|_| {
                    let start = next(64);
                    slice(start, start + 1 + next(24), 1, PayloadRefState::LocalOnly)
                })
                .collect();
            let cover = SingleSliceCover::new(&batch);
            for _ in 0..32 {
                let start = next(80);
                let range = SliceRange::new(start, start + 1 + next(16)).expect("query range");
                let expected = batch.iter().any(|candidate| {
                    candidate.range.start <= range.start && candidate.range.end >= range.end
                });
                assert_eq!(
                    cover.covers(range),
                    expected,
                    "range {range:?} in {batch:?}"
                );
            }
        }
        assert!(!SingleSliceCover::new(&[]).covers(SliceRange::new(0, 1).expect("range")));
    }

    #[test]
    fn completed_overlay_payloads_keep_upload_completion_for_the_same_object_span() {
        let mut state = RegularFileChunkState::default();
        state
            .slices
            .insert(SliceId(1), slice(0, 4096, 7, PayloadRefState::Uploaded));
        state.slices.insert(
            SliceId(2),
            slice(8192, 12288, 8, PayloadRefState::LocalOnly),
        );
        let completed = CompletedOverlayPayloads::new(&state);

        let mut replayed = slice(0, 4096, 7, PayloadRefState::LocalOnly);
        completed.preserve(&mut replayed);
        assert_eq!(replayed.payload_state, PayloadRefState::Uploaded);

        // A different object, an unfinished upload, or a narrower range keeps its state.
        let mut other_object = slice(0, 4096, 9, PayloadRefState::LocalOnly);
        let mut unfinished = slice(8192, 12288, 8, PayloadRefState::LocalOnly);
        let mut narrower = slice(0, 2048, 7, PayloadRefState::LocalOnly);
        narrower.payload = state.slices[&SliceId(1)].payload;
        for candidate in [&mut other_object, &mut unfinished, &mut narrower] {
            completed.preserve(candidate);
            assert_eq!(candidate.payload_state, PayloadRefState::LocalOnly);
        }
    }
}
