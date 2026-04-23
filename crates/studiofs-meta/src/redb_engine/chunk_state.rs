use super::*;

impl RedbMetaEngine {
    pub(super) fn regular_file_chunk_states_from_txn(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
    ) -> MetaResult<BTreeMap<Ino, BTreeMap<ChunkId, RegularFileChunkState>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut states: BTreeMap<Ino, BTreeMap<ChunkId, RegularFileChunkState>> = BTreeMap::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (ino, chunk_id) = decode_regular_file_chunk_key(key.value())?;
            let chunk_state: RegularFileChunkState = decode(value.value())?;
            states.entry(ino).or_default().insert(chunk_id, chunk_state);
        }
        Ok(states)
    }

    pub(super) fn regular_file_chunk_states_for_inode_in_write_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<BTreeMap<ChunkId, RegularFileChunkState>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut chunks = BTreeMap::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let (decoded_ino, chunk_id) = decode_regular_file_chunk_key(key.value())?;
            debug_assert_eq!(decoded_ino, ino);
            let chunk_state: RegularFileChunkState = decode(value.value())?;
            chunks.insert(chunk_id, chunk_state);
        }
        Ok(chunks)
    }

    pub(super) fn regular_file_chunk_rows_for_inode_in_write_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
        Ok(
            Self::regular_file_chunk_states_for_inode_in_write_txn(txn, table_def, ino)?
                .into_iter()
                .map(|(chunk_id, chunk_state)| {
                    (
                        chunk_id,
                        chunk_state.slices.values().cloned().collect::<Vec<_>>(),
                    )
                })
                .collect(),
        )
    }

    pub(super) fn regular_file_chunk_states_for_inode_in_read_txn(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<BTreeMap<ChunkId, RegularFileChunkState>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut chunks = BTreeMap::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let (_decoded_ino, chunk_id) = decode_regular_file_chunk_key(key.value())?;
            chunks.insert(chunk_id, decode(value.value())?);
        }
        Ok(chunks)
    }

    pub(super) fn has_regular_file_chunk_rows_in_write_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut iter = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match iter.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    pub(super) fn has_regular_file_chunk_rows_in_read_txn(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut iter = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match iter.next() {
            Some(row) => {
                let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                Ok(key.value().starts_with(prefix.as_ref()))
            }
            None => Ok(false),
        }
    }

    /// Build a data-only signature for a regular-file chunk graph.
    ///
    /// Internal counters and slice lifecycle fields are intentionally ignored so
    /// callers can detect real overlay data divergence without flagging pure
    /// bookkeeping changes.
    pub(super) fn regular_file_graph_signature(
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
    ) -> RegularFileGraphSignature {
        let mut signature = BTreeMap::new();
        for (chunk_id, chunk) in chunks {
            if chunk.slices.is_empty() {
                continue;
            }
            let mut chunk_signature = Vec::with_capacity(chunk.slices.len());
            for slice in chunk.slices.values() {
                chunk_signature.push((slice.range, slice.block.clone()));
            }
            signature.insert(*chunk_id, chunk_signature);
        }
        signature
    }

    pub(super) fn regular_file_chunk_state_from_slices(
        chunk_slices: &[crate::schema::SliceRecord],
    ) -> MetaResult<RegularFileChunkState> {
        let mut state = RegularFileChunkState::default();
        let mut max_slice_id: Option<SliceId> = None;
        let mut max_slice_order: Option<SliceOrder> = None;

        for slice in chunk_slices.iter().cloned() {
            max_slice_id = Some(match max_slice_id {
                Some(current) => current.max(slice.slice_id),
                None => slice.slice_id,
            });
            max_slice_order = Some(match max_slice_order {
                Some(current) => current.max(slice.order),
                None => slice.order,
            });
            state.slices.insert(slice.slice_id, slice);
        }

        state.next_slice_id = match max_slice_id {
            Some(slice_id) => SliceId(
                slice_id
                    .0
                    .checked_add(1)
                    .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?,
            ),
            None => SliceId(0),
        };
        state.next_slice_order = match max_slice_order {
            Some(slice_order) => SliceOrder(
                slice_order
                    .0
                    .checked_add(1)
                    .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?,
            ),
            None => SliceOrder(0),
        };
        state.refresh_fragmentation_metrics();
        Ok(state)
    }

    pub(super) fn set_regular_file_chunk_state_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        track_pending_compaction: bool,
        ino: Ino,
        chunk_id: ChunkId,
        chunk_slices: &[crate::schema::SliceRecord],
    ) -> MetaResult<Option<RegularFileChunkState>> {
        let key = encode_regular_file_chunk_key(ino, chunk_id);
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if chunk_slices.is_empty() {
            let _ = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if track_pending_compaction {
                let mut pending = txn
                    .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                let _ = pending
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            return Ok(None);
        }

        let state = Self::regular_file_chunk_state_from_slices(chunk_slices)?;
        let encoded = encode(&state)?;
        table
            .insert(key.as_slice(), encoded.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        if track_pending_compaction {
            let mut pending = txn
                .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if state.fragmentation.should_compact() {
                pending
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            } else {
                let _ = pending
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        Ok(Some(state))
    }

    /// Collect object IDs reachable from a regular-file chunk table.
    ///
    /// This streams the table row-by-row so reachability checks never need to
    /// materialize the whole chunk graph in memory.
    pub(super) fn regular_file_graph_object_ids_from_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        refs: &mut HashSet<u64>,
    ) -> MetaResult<()> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let chunk: RegularFileChunkState = decode(value.value())?;
            for slice in chunk.slices.values() {
                if let Some(block) = &slice.block {
                    refs.insert(block.object_id);
                }
            }
        }
        Ok(())
    }

    pub(super) fn clear_table_in_txn<K, V>(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<K, V>,
    ) -> MetaResult<()>
    where
        K: redb::Key + 'static,
        V: redb::Value + 'static,
    {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .retain(|_, _| false)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn regular_file_snapshot_rows_from_txn<TState, F>(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        wrap_state: F,
    ) -> MetaResult<Vec<(u64, Vec<u8>)>>
    where
        TState: serde::Serialize,
        F: Fn(BTreeMap<ChunkId, RegularFileChunkState>) -> TState,
    {
        let states = Self::regular_file_chunk_states_from_txn(txn, table_def)?;
        let mut snapshot_rows = Vec::with_capacity(states.len());
        for (ino, chunks) in states {
            let state = wrap_state(chunks);
            snapshot_rows.push((ino, encode(&state)?));
        }
        Ok(snapshot_rows)
    }

    pub(super) fn clear_regular_file_chunk_rows_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut keys_to_remove = Vec::new();
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            keys_to_remove.push(key.value().to_vec());
        }
        for key in keys_to_remove {
            let _ = table
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn clear_regular_file_pending_compaction_rows_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = regular_file_chunk_prefix(ino);
        let mut keys_to_remove = Vec::new();
        for row in pending
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            keys_to_remove.push(key.value().to_vec());
        }
        for key in keys_to_remove {
            let _ = pending
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn clear_regular_file_graph_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_BASE_TABLE, ino)?;
        Ok(())
    }

    pub(super) fn replace_regular_file_chunk_rows_from_blocks_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        blocks: &[(u32, BlockRef)],
        block_size: u32,
        published: bool,
        track_pending_compaction: bool,
    ) -> MetaResult<()> {
        if track_pending_compaction {
            Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        }
        Self::clear_regular_file_chunk_rows_in_txn(txn, table_def, ino)?;
        if blocks.is_empty() {
            return Ok(());
        }

        let graph = Self::convert_blocks_to_chunk_state(blocks, block_size)?;
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for (chunk_id, chunk_state) in graph.chunks {
            let chunk_state = if published {
                Self::chunk_for_published_base(&chunk_state)
            } else {
                chunk_state
            };
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let encoded = encode(&chunk_state)?;
            table
                .insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if track_pending_compaction && chunk_state.fragmentation.should_compact() {
                let mut pending = txn
                    .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                pending
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Ok(())
    }

    pub(super) fn replace_regular_file_chunk_rows_from_chunk_slices_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        track_pending_compaction: bool,
    ) -> MetaResult<()> {
        if track_pending_compaction {
            Self::clear_regular_file_pending_compaction_rows_in_txn(txn, ino)?;
        }
        Self::clear_regular_file_chunk_rows_in_txn(txn, table_def, ino)?;
        for (chunk_id, slices) in chunk_slices {
            Self::set_regular_file_chunk_state_in_txn(
                txn,
                table_def,
                track_pending_compaction,
                ino,
                *chunk_id,
                slices,
            )?;
        }
        Ok(())
    }

    pub(super) fn apply_regular_file_chunk_slices_in_txn(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        chunk_slices: &[(ChunkId, Vec<SliceRecord>)],
        track_pending_compaction: bool,
    ) -> MetaResult<()> {
        fn visible_data_ranges(
            chunk_id: ChunkId,
            current: &RegularFileChunkState,
            lower_precedence: Option<&RegularFileChunkState>,
            queries: &[SliceRange],
        ) -> Vec<SliceRange> {
            if queries.is_empty() {
                return Vec::new();
            }
            let mut visible = Vec::new();
            for query in queries {
                visible.extend(
                    resolve_chunk_slice_spans(chunk_id, *query, Some(current), lower_precedence)
                        .into_iter()
                        .filter_map(|span| span.block.map(|_| span.range)),
                );
            }
            if visible.len() <= 1 {
                visible
            } else {
                merge_covered_ranges(visible)
            }
        }

        fn intersect_ranges(query: SliceRange, visible: &[SliceRange]) -> Vec<SliceRange> {
            visible
                .iter()
                .filter_map(|range| query.intersection(*range))
                .collect()
        }

        fn merge_covered_ranges(mut merged: Vec<SliceRange>) -> Vec<SliceRange> {
            merged.sort_unstable_by_key(|range| (range.start, range.end));

            let mut out: Vec<SliceRange> = Vec::new();
            for range in merged {
                if let Some(last) = out.last_mut()
                    && range.start <= last.end
                {
                    last.end = last.end.max(range.end);
                    continue;
                }
                out.push(range);
            }
            out
        }

        if chunk_slices.is_empty() {
            return Ok(());
        }

        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut pending = if track_pending_compaction {
            Some(
                txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?,
            )
        } else {
            None
        };
        let use_overlay_base_precedence = redb::TableHandle::name(&table_def)
            == redb::TableHandle::name(&REGULAR_FILE_OVERLAY_TABLE);
        let base_table = if use_overlay_base_precedence {
            Some(
                txn.open_table(REGULAR_FILE_BASE_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?,
            )
        } else {
            None
        };

        for (chunk_id, new_slices) in chunk_slices {
            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            let mut state = if let Some(entry) = table
                .get(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                decode::<RegularFileChunkState>(entry.value())?
            } else {
                RegularFileChunkState::default()
            };
            let tombstone_queries = merge_covered_ranges(
                new_slices
                    .iter()
                    .filter(|slice| slice.block.is_none())
                    .map(|slice| slice.range)
                    .collect::<Vec<_>>(),
            );
            let lower_precedence = if tombstone_queries.is_empty() {
                None
            } else if let Some(base_table) = base_table.as_ref() {
                base_table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|value| decode::<RegularFileChunkState>(value.value()))
                    .transpose()?
            } else {
                None
            };
            let visible_ranges = visible_data_ranges(
                *chunk_id,
                &state,
                lower_precedence.as_ref(),
                tombstone_queries.as_slice(),
            );
            let mut effective_new_slices = Vec::new();
            for new_slice in new_slices.iter().cloned() {
                if new_slice.block.is_some() {
                    effective_new_slices.push(new_slice);
                    continue;
                }
                for intersection in intersect_ranges(new_slice.range, &visible_ranges) {
                    let mut pruned = new_slice.clone();
                    pruned.range = intersection;
                    effective_new_slices.push(pruned);
                }
            }

            let covered_ranges = merge_covered_ranges(
                effective_new_slices
                    .iter()
                    .map(|slice| slice.range)
                    .collect::<Vec<_>>(),
            );
            let mut fully_covered = Vec::new();
            for (id, existing) in &state.slices {
                if covered_ranges.iter().any(|range| {
                    range.start <= existing.range.start && range.end >= existing.range.end
                }) {
                    fully_covered.push(*id);
                }
            }

            for id in fully_covered {
                state.slices.remove(&id);
            }

            for mut new_slice in effective_new_slices {
                let slice_id = state
                    .try_alloc_slice_id()
                    .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
                let slice_order = state
                    .try_alloc_slice_order()
                    .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
                new_slice.slice_id = slice_id;
                new_slice.order = slice_order;
                state.slices.insert(slice_id, new_slice);
            }

            state.refresh_fragmentation_metrics();

            if state.is_empty() {
                let _ = table
                    .remove(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            } else {
                let encoded = encode(&state)?;
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            if let Some(pending) = pending.as_mut() {
                if state.fragmentation.should_compact() {
                    pending
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }

        Ok(())
    }

    /// Convert flat block list to chunk/slice state using the provided block size.
    pub(super) fn convert_blocks_to_chunk_state(
        blocks: &[(u32, BlockRef)],
        block_size: u32,
    ) -> MetaResult<RegularFileOverlayState> {
        let mut state = RegularFileOverlayState::default();

        for (block_index, block_ref) in blocks {
            let block_start_offset = (*block_index as u64) * (block_size as u64);
            let chunk_id = ChunkId(block_start_offset / (DEFAULT_CHUNK_SIZE as u64));
            let chunk_start_offset = chunk_id.0 * (DEFAULT_CHUNK_SIZE as u64);
            let slice_start = (block_start_offset - chunk_start_offset) as u32;
            let slice_end = slice_start.checked_add(block_ref.length).ok_or_else(|| {
                MetaError::Storage(format!(
                    "slice range overflow for block index {}: start={}, len={}",
                    block_index, slice_start, block_ref.length
                ))
            })?;

            let slice_range = SliceRange::new(slice_start, slice_end).ok_or_else(|| {
                MetaError::Storage(format!(
                    "invalid slice range for block index {}: start={}, end={}",
                    block_index, slice_start, slice_end
                ))
            })?;

            let chunk_state = state.chunks.entry(chunk_id).or_default();
            let slice_id = chunk_state.alloc_slice_id();
            let slice_order = chunk_state.alloc_slice_order();
            let slice_record =
                SliceRecord::new(slice_id, slice_order, slice_range, block_ref.clone());
            chunk_state.insert_slice(slice_record);
        }

        for chunk_state in state.chunks.values_mut() {
            chunk_state.refresh_fragmentation_metrics();
        }

        Ok(state)
    }

    pub(super) fn replace_regular_file_chunk_rows_from_snapshot_rows<F>(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        snapshot_rows: &[(u64, Vec<u8>)],
        parse_chunks: F,
        track_pending_compaction: bool,
    ) -> MetaResult<()>
    where
        F: Fn(&[u8]) -> MetaResult<BTreeMap<ChunkId, RegularFileChunkState>>,
    {
        let mut table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .retain(|_, _| false)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for (ino, bytes) in snapshot_rows {
            let chunks = parse_chunks(bytes)?;
            for (chunk_id, chunk_state) in chunks {
                let key = encode_regular_file_chunk_key(*ino, chunk_id);
                let encoded = encode(&chunk_state)?;
                table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if track_pending_compaction && chunk_state.fragmentation.should_compact() {
                    let mut pending = txn
                        .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                    pending
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn regular_file_committed_size_from_chunks(
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
    ) -> u64 {
        let mut size = 0u64;
        for (chunk_id, chunk_state) in chunks {
            let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
            for slice in chunk_state.slices.values() {
                if slice.block.is_some() {
                    let absolute_end = chunk_start.saturating_add(slice.range.end as u64);
                    size = size.max(absolute_end);
                }
            }
        }
        size
    }

    pub(super) fn manifest_from_chunk_states(
        ino: Ino,
        overlay_chunks: Option<&BTreeMap<ChunkId, RegularFileChunkState>>,
        base_chunks: Option<&BTreeMap<ChunkId, RegularFileChunkState>>,
        block_size: u32,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let mut chunk_ids = BTreeSet::new();
        if let Some(overlay_chunks) = overlay_chunks {
            chunk_ids.extend(overlay_chunks.keys().copied());
        }
        if let Some(base_chunks) = base_chunks {
            chunk_ids.extend(base_chunks.keys().copied());
        }
        if chunk_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut size = 0u64;
        for chunk_id in &chunk_ids {
            let query = SliceRange::new(0, DEFAULT_CHUNK_SIZE)
                .ok_or_else(|| MetaError::Storage("invalid full-chunk range".to_string()))?;
            for span in resolve_chunk_slice_spans(
                *chunk_id,
                query,
                overlay_chunks.and_then(|chunks| chunks.get(chunk_id)),
                base_chunks.and_then(|chunks| chunks.get(chunk_id)),
            ) {
                if span.block.is_some() {
                    let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
                    size = size.max(chunk_start.saturating_add(span.range.end as u64));
                }
            }
        }
        if size == 0 {
            return Ok(Vec::new());
        }

        let mut spans = Vec::new();
        let mut cursor = 0u64;
        while cursor < size {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let query_end = size.min(chunk_end);
            let query = SliceRange::new(
                (cursor - chunk_start) as u32,
                (query_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("invalid chunk-local manifest range".to_string()))?;
            spans.extend(resolve_chunk_slice_spans(
                chunk_id,
                query,
                overlay_chunks.and_then(|chunks| chunks.get(&chunk_id)),
                base_chunks.and_then(|chunks| chunks.get(&chunk_id)),
            ));
            cursor = query_end;
        }
        let spans = merge_adjacent_spans(spans);
        if block_size == 0 {
            return Err(MetaError::Storage(
                "invalid zero block size in manifest synthesis".to_string(),
            ));
        }
        let block_count = ((size.saturating_sub(1)) / block_size as u64).saturating_add(1);
        let mut synthesized = Vec::new();
        for idx in 0..block_count {
            let block_index = idx as u32;
            if let Some(block) = Self::synthesize_block_ref_from_resolved_spans(
                ino,
                spans.as_slice(),
                block_index,
                block_size,
            )? {
                synthesized.push((block_index, block));
            }
        }
        Ok(synthesized)
    }

    pub(super) fn legacy_committed_size_from_blocks(blocks: &[(u32, BlockRef)]) -> u64 {
        let mut size = 0u64;
        let mut expected = 0u32;
        for (block_index, block) in blocks {
            if *block_index != expected {
                break;
            }
            size = size.saturating_add(block.length as u64);
            expected = expected.saturating_add(1);
            if block.is_partial() {
                break;
            }
        }
        size
    }

    pub(super) fn chunk_for_published_base(chunk: &RegularFileChunkState) -> RegularFileChunkState {
        let mut published = chunk.clone();
        for slice in published.slices.values_mut() {
            slice.block_state = BlockRefState::Published;
        }
        published
    }

    pub(super) fn compact_regular_file_chunk_state(
        chunk_id: ChunkId,
        chunk: &RegularFileChunkState,
    ) -> MetaResult<Option<RegularFileChunkState>> {
        let full_range = SliceRange::new(0, DEFAULT_CHUNK_SIZE)
            .expect("full chunk range should always be valid");
        let resolved = resolve_chunk_slice_spans(chunk_id, full_range, Some(chunk), None);
        let mut compacted = RegularFileChunkState {
            next_slice_id: chunk.next_slice_id,
            next_slice_order: chunk.next_slice_order,
            slices: BTreeMap::new(),
            fragmentation: ChunkFragmentationCounters::default(),
        };
        let mut merged: Vec<(SliceRange, Option<BlockRef>, BlockRefState)> = Vec::new();

        for span in resolved {
            let Some(slice_id) = span.slice_id else {
                continue;
            };
            let source_state = chunk
                .slices
                .get(&slice_id)
                .map(|slice| slice.block_state)
                .ok_or_else(|| {
                    MetaError::Storage("compaction source slice is missing".to_string())
                })?;
            let block = span.block.clone();

            if let Some((last_range, last_block, last_state)) = merged.last_mut() {
                match (last_block.as_mut(), block.as_ref()) {
                    (Some(last_block), Some(block))
                        if *last_state == source_state
                            && last_block.object_id == block.object_id
                            && last_block.offset.saturating_add(last_block.length)
                                == block.offset
                            && last_block.flags == block.flags
                            && last_range.end == span.range.start =>
                    {
                        last_range.end = span.range.end;
                        last_block.length = last_block.length.saturating_add(block.length);
                        continue;
                    }
                    (None, None)
                        if *last_state == source_state && last_range.end == span.range.start =>
                    {
                        last_range.end = span.range.end;
                        continue;
                    }
                    _ => {}
                }
            }

            merged.push((span.range, block, source_state));
        }

        if merged.is_empty() {
            return Ok(None);
        }

        for (range, block, block_state) in merged {
            let slice_id = compacted
                .try_alloc_slice_id()
                .ok_or_else(|| MetaError::Storage("slice id overflow".to_string()))?;
            let slice_order = compacted
                .try_alloc_slice_order()
                .ok_or_else(|| MetaError::Storage("slice order overflow".to_string()))?;
            compacted.slices.insert(
                slice_id,
                SliceRecord {
                    slice_id,
                    order: slice_order,
                    range,
                    block,
                    block_state,
                },
            );
        }

        compacted.refresh_fragmentation_metrics();
        Ok(Some(compacted))
    }

    pub(super) fn publish_regular_file_overlay_to_base_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<(u64, Vec<u64>)> {
        // Publication semantics are "overlay becomes the new remote-visible
        // base" for the inode, so base rows are reconciled to exactly match the
        // normalized overlay graph.
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let existing_base = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;

        let mut desired_base = BTreeMap::new();
        for (chunk_id, chunk_state) in &overlay_chunks {
            desired_base.insert(*chunk_id, Self::chunk_for_published_base(chunk_state));
        }

        let mut existing_object_ids = BTreeSet::new();
        for chunk_state in existing_base.values() {
            for slice in chunk_state.slices.values() {
                if let Some(block) = slice.block.as_ref() {
                    existing_object_ids.insert(block.object_id);
                }
            }
        }

        let mut desired_object_ids = BTreeSet::new();
        for chunk_state in desired_base.values() {
            for slice in chunk_state.slices.values() {
                if let Some(block) = slice.block.as_ref() {
                    desired_object_ids.insert(block.object_id);
                }
            }
        }

        let mut stale_chunk_ids: BTreeSet<ChunkId> = existing_base.keys().copied().collect();
        let mut base = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for (chunk_id, desired_chunk) in &desired_base {
            stale_chunk_ids.remove(chunk_id);
            let unchanged = existing_base
                .get(chunk_id)
                .is_some_and(|existing_chunk| existing_chunk == desired_chunk);
            if unchanged {
                continue;
            }

            let key = encode_regular_file_chunk_key(ino, *chunk_id);
            let encoded = encode(desired_chunk)?;
            base.insert(key.as_slice(), encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        for chunk_id in stale_chunk_ids {
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let _ = base
                .remove(key.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        // Overlay rows are local/staging state. Once promoted to committed base,
        // clear them to avoid unbounded growth and stale finalize gates.
        Self::clear_regular_file_chunk_rows_in_txn(txn, REGULAR_FILE_OVERLAY_TABLE, ino)?;
        Ok((
            Self::regular_file_committed_size_from_chunks(&overlay_chunks),
            existing_object_ids
                .difference(&desired_object_ids)
                .copied()
                .collect(),
        ))
    }

    pub(super) fn regular_file_graph_block_size(&self) -> MetaResult<u32> {
        match self.get_format_info() {
            Ok(info) => Ok(info.block_size),
            Err(MetaError::NotFormatted) => Ok(DEFAULT_BLOCK_SIZE),
            Err(err) => Err(err),
        }
    }

    pub(super) fn synthesize_block_ref_from_resolved_spans(
        ino: Ino,
        spans: &[ResolvedSliceSpan],
        block_index: u32,
        block_size: u32,
    ) -> MetaResult<Option<BlockRef>> {
        #[derive(Clone, Copy)]
        struct Range {
            start: u64,
            end: u64,
        }

        fn claim_ranges(unresolved: Vec<Range>, candidate: Range) -> (Vec<Range>, Vec<Range>) {
            let mut next_unresolved = Vec::new();
            let mut claimed = Vec::new();
            for range in unresolved {
                let overlap_start = range.start.max(candidate.start);
                let overlap_end = range.end.min(candidate.end);
                if overlap_start >= overlap_end {
                    next_unresolved.push(range);
                    continue;
                }
                if range.start < overlap_start {
                    next_unresolved.push(Range {
                        start: range.start,
                        end: overlap_start,
                    });
                }
                claimed.push(Range {
                    start: overlap_start,
                    end: overlap_end,
                });
                if overlap_end < range.end {
                    next_unresolved.push(Range {
                        start: overlap_end,
                        end: range.end,
                    });
                }
            }
            (next_unresolved, claimed)
        }

        let block_start = (block_index as u64).saturating_mul(block_size as u64);
        let block_end = block_start.saturating_add(block_size as u64);
        let mut unresolved = vec![Range {
            start: block_start,
            end: block_end,
        }];
        let mut claimed_refs: Vec<(u64, BlockRef)> = Vec::new();

        for source in [ResolvedSliceSource::Overlay, ResolvedSliceSource::Base] {
            for span in spans.iter().filter(|span| span.source == source) {
                if unresolved.is_empty() {
                    break;
                }
                let Some(block) = span.block.as_ref() else {
                    continue;
                };
                let chunk_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
                let span_range = Range {
                    start: chunk_start.saturating_add(span.range.start as u64),
                    end: chunk_start.saturating_add(span.range.end as u64),
                };
                let (next_unresolved, claimed) = claim_ranges(unresolved, span_range);
                unresolved = next_unresolved;
                for overlap in claimed {
                    let delta = overlap.start.saturating_sub(span_range.start);
                    let Some(delta_u32) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    let Some(block_offset) = block.offset.checked_add(delta_u32) else {
                        continue;
                    };
                    let take_len = overlap.end.saturating_sub(overlap.start);
                    let Some(take_len_u32) = u32::try_from(take_len).ok() else {
                        continue;
                    };
                    claimed_refs.push((
                        overlap.start,
                        BlockRef {
                            object_id: block.object_id,
                            offset: block_offset,
                            length: take_len_u32,
                            flags: block.flags,
                        },
                    ));
                }
            }
        }

        if claimed_refs.is_empty() {
            return Ok(None);
        }
        claimed_refs.sort_by_key(|(start, _)| *start);

        let mut chosen: Option<BlockRef> = None;
        for (_, part) in claimed_refs {
            if let Some(existing) = chosen.as_mut() {
                if existing.object_id != part.object_id
                    || existing.offset.saturating_add(existing.length) != part.offset
                {
                    return Err(MetaError::FragmentedBlock { ino, block_index });
                }
                existing.length = existing.length.saturating_add(part.length);
                existing.flags |= part.flags;
            } else {
                chosen = Some(part);
            }
        }

        Ok(chosen.filter(|block| block.length > 0))
    }

    fn regular_file_committed_size_from_base_table(
        table: &redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Option<u64>> {
        let prefix = regular_file_chunk_prefix(ino);
        let mut size = 0u64;
        let mut saw_row = false;
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            let (decoded_ino, chunk_id) = decode_regular_file_chunk_key(key.value())?;
            debug_assert_eq!(decoded_ino, ino);
            saw_row = true;
            let chunk: RegularFileChunkState = decode(value.value())?;
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            for slice in chunk.slices.values() {
                if slice.block.is_some() {
                    let slice_end = chunk_start.saturating_add(slice.range.end as u64);
                    size = size.max(slice_end);
                }
            }
        }
        Ok(saw_row.then_some(size))
    }

    #[cfg(test)]
    pub(super) fn compute_committed_size_in_txn(
        txn: &redb::WriteTransaction,
        committed: &redb::Table<'_, &[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<u64> {
        let _ = txn;
        Ok(Self::regular_file_committed_size_from_base_table(committed, ino)?.unwrap_or(0))
    }

    pub(super) fn regular_file_committed_size_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<u64>> {
        let table = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::regular_file_committed_size_from_base_table(&table, ino)
    }

    pub(super) fn current_manifest_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let attr = Self::get_attr_from_write_table(txn, INODES_TABLE, ino)?
            .or_else(|| {
                Self::get_attr_from_write_table(txn, BASE_INODES_TABLE, ino)
                    .ok()
                    .flatten()
            })
            .ok_or(MetaError::InodeNotFound(ino))?;
        if attr.size == 0 {
            return Ok(Vec::new());
        }
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let base_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;
        let block_size = Self::regular_file_graph_block_size_from_write_txn(txn)?;
        Self::manifest_from_chunk_states(ino, Some(&overlay_chunks), Some(&base_chunks), block_size)
    }

    pub(super) fn lookup_regular_file_slices_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        offset: u64,
        length: u64,
        include_overlay: bool,
        include_base: bool,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| MetaError::Storage("regular-file lookup range overflow".to_string()))?;
        let overlay_table = if include_overlay {
            Some(
                txn.open_table(REGULAR_FILE_OVERLAY_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?,
            )
        } else {
            None
        };
        let base_table = if include_base {
            Some(
                txn.open_table(REGULAR_FILE_BASE_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?,
            )
        } else {
            None
        };
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let query_end = end.min(chunk_end);
            let query = SliceRange::new(
                (cursor - chunk_start) as u32,
                (query_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("invalid chunk-local lookup range".to_string()))?;
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let overlay: Option<RegularFileChunkState> = if let Some(table) = overlay_table.as_ref()
            {
                table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|value| decode(value.value()))
                    .transpose()?
            } else {
                None
            };
            let base: Option<RegularFileChunkState> = if let Some(table) = base_table.as_ref() {
                table
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                    .map(|value| decode(value.value()))
                    .transpose()?
            } else {
                None
            };
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

    pub(super) fn current_block_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        block_index: u32,
        block_size: u32,
    ) -> MetaResult<Option<BlockRef>> {
        let block_start = u64::from(block_index).saturating_mul(u64::from(block_size));
        let spans = Self::lookup_regular_file_slices_in_write_txn(
            txn,
            ino,
            block_start,
            u64::from(block_size),
            true,
            true,
        )?;
        Self::synthesize_block_ref_from_resolved_spans(
            ino,
            spans.as_slice(),
            block_index,
            block_size,
        )
    }

    pub(super) fn chunk_slices_for_block(
        block_index: u32,
        block_size: u32,
        block: Option<&BlockRef>,
        block_state: BlockRefState,
    ) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
        if block_size == 0 {
            return Err(MetaError::Storage(
                "invalid zero block size for block mutation".to_string(),
            ));
        }
        if let Some(block) = block
            && block.length > block_size
        {
            return Err(MetaError::Storage(format!(
                "block length {} exceeds block size {}",
                block.length, block_size
            )));
        }

        let block_start = u64::from(block_index)
            .checked_mul(u64::from(block_size))
            .ok_or_else(|| MetaError::Storage("block offset overflow".to_string()))?;
        let data_end = block_start
            .checked_add(u64::from(block.map_or(0, |entry| entry.length)))
            .ok_or_else(|| MetaError::Storage("block data range overflow".to_string()))?;
        let block_end = block_start
            .checked_add(u64::from(block_size))
            .ok_or_else(|| MetaError::Storage("block slot range overflow".to_string()))?;

        let mut chunks = BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
        let mut push_segment = |segment_start: u64,
                                segment_end: u64,
                                data_block: Option<&BlockRef>|
         -> MetaResult<()> {
            if segment_start >= segment_end {
                return Ok(());
            }
            let mut cursor = segment_start;
            while cursor < segment_end {
                let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
                let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
                let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
                let piece_end = segment_end.min(chunk_end);
                let range = SliceRange::new(
                    (cursor - chunk_start) as u32,
                    (piece_end - chunk_start) as u32,
                )
                .ok_or_else(|| MetaError::Storage("invalid block mutation range".to_string()))?;
                let slice = if let Some(block) = data_block {
                    let delta = cursor.saturating_sub(block_start);
                    let delta = u32::try_from(delta).map_err(|_| {
                        MetaError::Storage("block offset delta overflow".to_string())
                    })?;
                    let length = u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                        MetaError::Storage("block slice length overflow".to_string())
                    })?;
                    let offset = block.offset.checked_add(delta).ok_or_else(|| {
                        MetaError::Storage("block slice offset overflow".to_string())
                    })?;
                    let mut slice = SliceRecord::new(
                        SliceId(0),
                        SliceOrder(0),
                        range,
                        BlockRef {
                            object_id: block.object_id,
                            offset,
                            length,
                            flags: block.flags,
                        },
                    );
                    slice.block_state = block_state;
                    slice
                } else {
                    let mut slice = SliceRecord::tombstone(SliceId(0), SliceOrder(0), range);
                    slice.block_state = block_state;
                    slice
                };
                chunks.entry(chunk_id).or_default().push(slice);
                cursor = piece_end;
            }
            Ok(())
        };

        push_segment(block_start, data_end, block)?;
        if data_end < block_end {
            push_segment(data_end, block_end, None)?;
        }
        Ok(chunks.into_iter().collect())
    }

    pub(super) fn base_manifest_from_graph_rows_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Vec<(u32, BlockRef)>> {
        let base_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            ino,
        )?;
        let block_size = Self::regular_file_graph_block_size_from_write_txn(txn)?;
        Self::manifest_from_chunk_states(ino, None, Some(&base_chunks), block_size)
    }

    pub(super) fn regular_file_graph_block_size_from_write_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<u32> {
        match Self::format_info_from_write_txn(txn) {
            Ok(info) => Ok(info.block_size),
            Err(MetaError::NotFormatted) => Ok(DEFAULT_BLOCK_SIZE),
            Err(err) => Err(err),
        }
    }

    pub(super) fn transition_upload_state_for_local_mutation(
        attr: &mut InodeAttr,
        has_committed_manifest: bool,
    ) {
        if attr.kind != FileType::RegularFile {
            attr.upload_state = UploadState::Ready;
            return;
        }
        if attr.content_epoch == attr.committed_content_epoch && attr.size == attr.committed_size {
            attr.upload_state = UploadState::Ready;
        } else if has_committed_manifest
            || attr.committed_size > 0
            || attr.committed_content_epoch > 0
        {
            attr.upload_state = UploadState::Updating;
        } else {
            attr.upload_state = UploadState::Uploading;
        }
    }

    pub(super) fn manifest_matches_committed_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        let current = Self::current_manifest_in_txn(txn, ino)?;
        Ok(current == Self::base_manifest_from_graph_rows_in_txn(txn, ino)?)
    }
}
