impl RedbMetaEngine {
    /// Migrate regular files from the old FILE_BLOCKS flat model to the new chunk/slice/block model.
    ///
    /// This migration is performed when upgrading from format version 2 to 3.
    /// It reads all FILE_BLOCKS entries, converts them to chunk/slice format,
    /// and stores them in the new tables while keeping the legacy flat tables
    /// populated for compatibility during the transition.
    ///
    /// Key design decisions (addressing review findings):
    /// - FILE_BLOCKS (uncommitted/local) → LocalOnly in OVERLAY table
    /// - COMMITTED_FILE_BLOCKS → Published in BASE table
    /// - Batch processing (100 inodes per transaction) for efficiency
    /// - Streaming approach to minimize memory usage
    ///
    /// Rollback is not supported in-place; interrupted upgrades are detected by
    /// startup validation and can be resumed by rerunning the migration.
    fn migrate_regular_files_to_chunk_slice_model(&self) -> MetaResult<()> {
        const BATCH_SIZE: usize = 100;

        {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let migration_status = txn
                .open_table(MIGRATION_STATUS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            if let Some(status) = migration_status
                .get(REGULAR_FILE_MIGRATION_NAME)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|v| v.value())
            {
                if status == MIGRATION_STATUS_COMPLETED {
                    debug!(
                        "migration {} already completed",
                        REGULAR_FILE_MIGRATION_NAME
                    );
                    return Ok(());
                }
            }
        }

        let format_info = self.get_format_info()?;
        let block_size = format_info.block_size;

        info!(
            "starting migration: regular files from v2 to v3 chunk/slice model (block_size={})",
            block_size
        );

        {
            let txn = self
                .db
                .begin_write()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            {
                let mut migration_status = txn
                    .open_table(MIGRATION_STATUS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                migration_status
                    .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            txn.commit()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        let mut pending_inodes: Vec<Ino> = Vec::with_capacity(BATCH_SIZE);
        let mut pending_set: HashSet<Ino> = HashSet::with_capacity(BATCH_SIZE);
        let mut last_processed_key: Vec<u8> = Vec::new();

        for table_name in &[FILE_BLOCKS_TABLE, COMMITTED_FILE_BLOCKS_TABLE] {
            last_processed_key.clear();

            loop {
                pending_inodes.clear();
                pending_set.clear();

                {
                    let txn = self
                        .db
                        .begin_read()
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    let table = txn
                        .open_table(*table_name)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;

                    let iter = if last_processed_key.is_empty() {
                        table
                            .iter()
                            .map_err(|e| MetaError::Storage(e.to_string()))?
                    } else {
                        table
                            .range(last_processed_key.as_slice()..)
                            .map_err(|e| MetaError::Storage(e.to_string()))?
                    };

                    for row in iter {
                        let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                        let key_bytes = key.value();

                        let (ino, _) = decode_file_block_key(key_bytes);

                        if pending_set.insert(ino) {
                            pending_inodes.push(ino);
                        }

                        if pending_inodes.len() >= BATCH_SIZE {
                            break;
                        }
                    }
                }

                if pending_inodes.is_empty() {
                    break;
                }

                self.migrate_inode_batch(pending_inodes.as_slice(), block_size)?;

                last_processed_key =
                    encode_file_block_key(*pending_inodes.last().unwrap(), u32::MAX).to_vec();
            }
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut migration_status = txn
                .open_table(MIGRATION_STATUS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            migration_status
                .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_COMPLETED)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        info!("migration completed: regular files v2 to v3 chunk/slice model");
        Ok(())
    }

    /// Migrate a batch of inodes in a single transaction.
    fn migrate_inode_batch(&self, inodes: &[Ino], block_size: u32) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut pending_compaction = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for &ino in inodes {
            if Self::has_regular_file_chunk_rows_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                ino,
            )? {
                continue;
            }

            let local_blocks =
                Self::get_file_blocks_from_txn_internal(&txn, FILE_BLOCKS_TABLE, ino)?;

            let committed_blocks =
                Self::get_file_blocks_from_txn_internal(&txn, COMMITTED_FILE_BLOCKS_TABLE, ino)?;

            if local_blocks.is_empty() && committed_blocks.is_empty() {
                continue;
            }

            let overlay_state = Self::convert_blocks_to_chunk_state(&local_blocks, block_size)?;
            let mut base_state =
                Self::convert_blocks_to_chunk_state(&committed_blocks, block_size)?;

            for chunk in base_state.chunks.values_mut() {
                for slice in chunk.slices.values_mut() {
                    slice.block_state = BlockRefState::Published;
                }
            }

            let mut overlay_table = txn
                .open_table(REGULAR_FILE_OVERLAY_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (chunk_id, chunk_state) in &overlay_state.chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let encoded = encode(chunk_state)?;
                overlay_table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                if chunk_state.fragmentation.should_compact() {
                    pending_compaction
                        .insert(key.as_slice(), 1)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                } else {
                    let _ = pending_compaction
                        .remove(key.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }

            let mut base_table = txn
                .open_table(REGULAR_FILE_BASE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (chunk_id, chunk_state) in &base_state.chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let encoded = encode(chunk_state)?;
                base_table
                    .insert(key.as_slice(), encoded.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }

            // Keep the legacy block tables populated for now.
            //
            // Existing MetaBlocks methods (`get_file_block`, `get_file_blocks`,
            // `set_file_block`, `remove_file_block`) and VFS startup flows still
            // read from FILE_BLOCKS_TABLE / COMMITTED_FILE_BLOCKS_TABLE. Deleting
            // those rows here would make migrated volumes appear to have empty
            // manifests until all consumers are switched to the chunk/slice APIs.
            //
            // The migration therefore remains additive: it backfills the regular
            // file chunk tables while preserving the legacy source of truth for
            // compatibility.
        }

        drop(pending_compaction);
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Ok(())
    }

    /// Convert flat block list to chunk/slice state using the provided block size.
    fn convert_blocks_to_chunk_state(
        blocks: &[(u32, BlockRef)],
        block_size: u32,
    ) -> MetaResult<RegularFileOverlayState> {
        use crate::schema::DEFAULT_CHUNK_SIZE;

        let mut state = RegularFileOverlayState::default();

        for (block_index, block_ref) in blocks {
            let block_start_offset = (*block_index as u64) * (block_size as u64);
            let chunk_id = ChunkId(block_start_offset / (DEFAULT_CHUNK_SIZE as u64));
            let chunk_start_offset = chunk_id.0 * (DEFAULT_CHUNK_SIZE as u64);
            let slice_start = (block_start_offset - chunk_start_offset) as u32;
            let slice_end = slice_start + block_ref.length;

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

    /// Validate that migration from v2 to v3 completed successfully.
    /// Useful for manual recovery scenarios and debugging.
    #[allow(dead_code)]
    fn validate_migration_completed(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let migration_status = txn
            .open_table(MIGRATION_STATUS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        debug!(
            "checking migration status for {}",
            REGULAR_FILE_MIGRATION_NAME
        );

        let migration_entry = migration_status
            .get(REGULAR_FILE_MIGRATION_NAME)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let migration_completed = migration_entry
            .as_ref()
            .map(|v| {
                let status = v.value();
                debug!(
                    "migration status value: {} (expected: {})",
                    status, MIGRATION_STATUS_COMPLETED
                );
                status == MIGRATION_STATUS_COMPLETED
            })
            .unwrap_or(false);

        debug!("migration_completed: {}", migration_completed);

        if !migration_completed {
            let reason = migration_entry
                .as_ref()
                .map(|v| format!("status={}", v.value()))
                .unwrap_or_else(|| "not found".to_string());
            return Err(MetaError::Storage(format!(
                "migration from format v2 to v3 did not complete successfully ({})",
                reason
            )));
        }

        debug!("migration validation passed: completion marker is set");
        Ok(())
    }
}
