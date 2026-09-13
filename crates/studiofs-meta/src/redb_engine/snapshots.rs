use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotImportMode {
    Local,
    RemoteReplace,
    RemoteReplacePreservingEventLog,
    RemotePreserveLocalState,
}

impl RedbMetaEngine {
    /// Visit regular-file base chunks without retaining the whole graph in memory.
    pub fn visit_regular_file_base_chunks<F>(&self, visit: F) -> MetaResult<()>
    where
        F: FnMut(u64, RegularFileChunkState),
    {
        let txn = self.begin_read_txn()?;
        Self::visit_regular_file_chunk_states_from_txn(&txn, REGULAR_FILE_BASE_TABLE, visit)
    }

    /// Export the current merged redb state into a serializable snapshot.
    pub fn export_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let txn = self.begin_read_txn()?;
        let mut snapshot = RedbSnapshot::default();

        {
            let table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .settings
                    .push((k.value().to_string(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.counters.push((k.value().to_string(), v.value()));
            }
        }
        {
            let table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.inodes.push((k.value(), v.value().to_vec()));
            }
        }
        {
            let table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .dentries
                    .push((k.value().to_vec(), v.value().to_vec()));
            }
        }
        {
            snapshot.regular_file_overlay = Self::regular_file_snapshot_rows_from_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                |chunks| RegularFileOverlayState { chunks },
            )?;
        }
        {
            snapshot.regular_file_base = Self::regular_file_snapshot_rows_from_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                |chunks| RegularFileBaseState { chunks },
            )?;
        }
        {
            let table = txn
                .open_table(PENDING_DELETIONS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.pending_deletions.push((k.value(), v.value()));
            }
        }
        {
            let base = txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(SYMLINK_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<u64, Vec<u8>>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value(), v.value().to_vec());
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(&k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value(), v.value().to_vec());
            }
            snapshot.symlinks.extend(merged);
        }
        {
            let base = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key = k.value().to_vec();
                let decoded = decode_xattr_value_from_storage(xattr_name_from_key(&key), v.value());
                merged.insert(key, decoded);
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key = k.value().to_vec();
                let decoded = decode_xattr_value_from_storage(xattr_name_from_key(&key), v.value());
                merged.insert(key, decoded);
            }
            snapshot.xattrs.extend(merged);
        }
        {
            let base = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut merged = std::collections::BTreeMap::<Vec<u8>, u8>::new();
            for row in base.iter().map_err(|e| MetaError::Storage(e.to_string()))? {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value().to_vec(), v.value());
            }
            for row in tombstones
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, _v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.remove(k.value());
            }
            for row in overlay
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                merged.insert(k.value().to_vec(), v.value());
            }
            snapshot.acl_rules.extend(merged);
        }
        {
            let table = txn
                .open_table(EVENT_LOG_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot.event_log.push((k.value(), v.value().to_vec()));
            }
        }
        Ok(snapshot)
    }

    /// Replace local redb state with a snapshot that should remain fully visible locally.
    pub fn import_snapshot(&self, snapshot: &RedbSnapshot) -> MetaResult<()> {
        self.import_snapshot_with_mode(snapshot, SnapshotImportMode::Local)
    }

    /// Replace local redb state with a snapshot imported from the remote authority.
    pub fn import_remote_snapshot(&self, snapshot: &RedbSnapshot) -> MetaResult<()> {
        self.import_snapshot_with_mode(snapshot, SnapshotImportMode::RemoteReplace)
    }

    /// Import a remote snapshot while retaining the local event journal needed
    /// to reconstruct a redb-repaired pending regular-file graph.
    pub fn import_remote_snapshot_preserving_local_event_log(
        &self,
        snapshot: &RedbSnapshot,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(
            snapshot,
            SnapshotImportMode::RemoteReplacePreservingEventLog,
        )
    }

    /// Import a remote snapshot while preserving this client's local overlay and replay log.
    pub fn import_remote_snapshot_preserving_local_state(
        &self,
        snapshot: &RedbSnapshot,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(snapshot, SnapshotImportMode::RemotePreserveLocalState)
    }

    /// Replace the serving view from the narrow checkpoint used by PG
    /// replication. Local counters, journals, and cleanup state are retained
    /// from the destination database rather than being copied into the source
    /// bytes.
    pub(crate) fn import_serving_checkpoint(
        &self,
        checkpoint: &crate::snapshot::ServingCheckpoint,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(
            &self.redb_snapshot_from_serving_checkpoint(checkpoint)?,
            SnapshotImportMode::RemoteReplace,
        )
    }

    /// Import the postcard-encoded serving checkpoint used by PostgreSQL.
    ///
    /// This keeps the internal checkpoint representation private to the
    /// metadata crate while allowing bootstrap callers such as `discover` to
    /// create a local redb view from the thin remote checkpoint.
    pub fn import_serving_checkpoint_bytes(&self, bytes: &[u8]) -> MetaResult<()> {
        let checkpoint: crate::snapshot::ServingCheckpoint =
            postcard::from_bytes(bytes).map_err(|error| {
                MetaError::Serialization(format!("serving checkpoint decode failed: {error}"))
            })?;
        self.import_serving_checkpoint(&checkpoint)
    }

    /// Initialize a local database from a validated PostgreSQL serving checkpoint.
    ///
    /// The caller remains responsible for installing the completed database at
    /// its active path. This method records the replay positions and fresh
    /// client gate together with the imported serving view, then forces the
    /// local durability barrier before returning.
    pub fn initialize_discovered_postgres_checkpoint(
        &self,
        format_info: &FormatInfo,
        checkpoint_bytes: &[u8],
        checkpoint_version: i64,
    ) -> MetaResult<()> {
        if checkpoint_version < 0 {
            return Err(MetaError::InvalidCheckpointDescriptor {
                reason: crate::CheckpointDescriptorFailure::Version,
            });
        }
        if format_info.metadata.backend != MetadataBackend::Postgres {
            return Err(MetaError::InvalidCheckpointDescriptor {
                reason: crate::CheckpointDescriptorFailure::FormatInfo,
            });
        }
        let Some(postgres) = format_info.metadata.postgres.as_ref() else {
            return Err(MetaError::InvalidCheckpointDescriptor {
                reason: crate::CheckpointDescriptorFailure::FormatInfo,
            });
        };
        if postgres.url.trim().is_empty() {
            return Err(MetaError::InvalidCheckpointDescriptor {
                reason: crate::CheckpointDescriptorFailure::FormatInfo,
            });
        }
        let checkpoint_version = u64::try_from(checkpoint_version).map_err(|_| {
            MetaError::InvalidCheckpointDescriptor {
                reason: crate::CheckpointDescriptorFailure::Version,
            }
        })?;
        self.import_serving_checkpoint_bytes(checkpoint_bytes)?;
        self.set_format_info(format_info)?;
        let root_attr = self.get_attr(ROOT_INO)?;
        if root_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(ROOT_INO));
        }

        let txn = self.begin_write_txn()?;
        {
            let mut counters = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (name, value) in [
                (
                    crate::pg_engine::LOCAL_PG_VERSION_COUNTER,
                    checkpoint_version,
                ),
                (
                    crate::pg_engine::LOCAL_PG_NAMESPACE_VERSION_COUNTER,
                    checkpoint_version,
                ),
                (crate::pg_engine::LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER, 1),
            ] {
                counters
                    .insert(name, value)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.sync_local_metadata()
    }

    /// Replace the serving view while retaining the local event journal used
    /// by the repair/replay path.
    pub(crate) fn import_serving_checkpoint_preserving_local_event_log(
        &self,
        checkpoint: &crate::snapshot::ServingCheckpoint,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(
            &self.redb_snapshot_from_serving_checkpoint(checkpoint)?,
            SnapshotImportMode::RemoteReplacePreservingEventLog,
        )
    }

    /// Replace the serving view while preserving local overlay state and its
    /// replay journal.
    pub(crate) fn import_serving_checkpoint_preserving_local_state(
        &self,
        checkpoint: &crate::snapshot::ServingCheckpoint,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(
            &self.redb_snapshot_from_serving_checkpoint(checkpoint)?,
            SnapshotImportMode::RemotePreserveLocalState,
        )
    }

    pub(crate) fn redb_snapshot_from_serving_checkpoint(
        &self,
        checkpoint: &crate::snapshot::ServingCheckpoint,
    ) -> MetaResult<RedbSnapshot> {
        let mut snapshot = self.export_snapshot()?;
        let next_inode = checkpoint
            .inodes
            .iter()
            .map(|(ino, _)| *ino)
            .max()
            .unwrap_or(ROOT_INO)
            .checked_add(1)
            .ok_or_else(|| MetaError::Storage("imported inode counter overflow".to_string()))?;
        let total_inodes = u64::try_from(checkpoint.inodes.len())
            .map_err(|_| MetaError::Storage("imported inode count overflow".to_string()))?;
        let mut referenced_objects = BTreeMap::new();
        for (_, bytes) in &checkpoint.regular_file_base {
            let regular: RegularFileBaseState = decode(bytes)?;
            for chunk in regular.chunks.values() {
                for slice in chunk.slices.values() {
                    if let Some(block) = slice.payload.as_ref() {
                        referenced_objects
                            .entry(block.object_id)
                            .or_insert(block.length as u64);
                    }
                }
            }
        }
        let used_space = referenced_objects
            .values()
            .fold(0_u64, |acc, len| acc.saturating_add(*len));

        snapshot.inodes = checkpoint.inodes.clone();
        snapshot.dentries = checkpoint.dentries.clone();
        snapshot.regular_file_overlay = checkpoint.regular_file_base.clone();
        snapshot.regular_file_base = checkpoint.regular_file_base.clone();
        snapshot.counters = vec![
            (counters::NEXT_INODE.to_string(), next_inode),
            (counters::TOTAL_INODES.to_string(), total_inodes),
            (counters::USED_SPACE.to_string(), used_space),
        ];
        snapshot.pending_deletions.clear();
        snapshot.symlinks = checkpoint.symlinks.clone();
        snapshot.xattrs = checkpoint.xattrs.clone();
        snapshot.acl_rules = checkpoint.acl_rules.clone();
        snapshot.event_log.clear();
        Ok(snapshot)
    }

    fn import_snapshot_with_mode(
        &self,
        snapshot: &RedbSnapshot,
        mode: SnapshotImportMode,
    ) -> MetaResult<()> {
        let enable_remote_namespace_filter = mode != SnapshotImportMode::Local;
        let preserve_local_state = mode == SnapshotImportMode::RemotePreserveLocalState;
        let preserve_event_log =
            preserve_local_state || mode == SnapshotImportMode::RemoteReplacePreservingEventLog;
        let (preserved_inodes, preserved_dentries) = if preserve_local_state {
            let txn = self.begin_read_txn()?;
            let mut overlay_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
            overlay_inodes.extend(Self::local_overlay_attr_inodes_from_txn(&txn)?);

            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut preserved_inodes = Vec::new();
            for ino in overlay_inodes {
                if let Some(value) = inodes
                    .get(ino)
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    preserved_inodes.push((ino, value.value().to_vec()));
                }
            }
            drop(inodes);

            let current = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let base = txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let tombstones = txn
                .open_table(DENTRY_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let overlay_dentries =
                Self::collect_overlay_dentries_from_tables(&current, &base, &tombstones)?;
            let mut preserved_dentries = Vec::new();
            for key in overlay_dentries {
                if let Some(value) = current
                    .get(key.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    preserved_dentries.push((key, value.value().to_vec()));
                }
            }
            drop(tombstones);
            drop(base);
            drop(current);

            (preserved_inodes, preserved_dentries)
        } else {
            (Vec::new(), Vec::new())
        };
        let txn = self.begin_write_txn()?;
        txn.invalidate(InvalidationIntent::Reset);

        if !preserve_local_state {
            Self::clear_table_in_txn(&txn, PENDING_REGULAR_FILE_COMPACTION_TABLE)?;
        }
        Self::clear_table_in_txn(&txn, INODE_LIFECYCLE_TABLE)?;

        {
            let mut table = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.settings {
                table
                    .insert(k.as_str(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Self::set_remote_namespace_visibility_filter_in_txn(&txn, enable_remote_namespace_filter)?;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|name, _| {
                    preserve_local_state
                        || (preserve_event_log
                            && (name == "nextEventSeq"
                                || crate::pg_engine::is_local_pg_snapshot_state_counter(name)))
                        || (enable_remote_namespace_filter
                            && name == crate::pg_engine::LOCAL_PG_FRESH_CLIENT_PENDING_COUNTER)
                })
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.counters {
                if enable_remote_namespace_filter
                    && (k.as_str() == "nextEventSeq"
                        || preserve_local_state && k.as_str() == counters::NEXT_INODE
                        || crate::pg_engine::is_local_pg_snapshot_state_counter(k))
                {
                    continue;
                }
                table
                    .insert(k.as_str(), *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            let mut table = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            for (k, v) in &preserved_inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            if preserve_local_state {
                let tombstones = txn
                    .open_table(INODE_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for row in tombstones
                    .iter()
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    let (k, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table
                        .remove(k.value())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        {
            let mut table = txn
                .open_table(BASE_INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.inodes {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, INODE_TOMBSTONES_TABLE)?;
            }
        }
        {
            let mut table = txn
                .open_table(DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            for (k, v) in &preserved_dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
            if preserve_local_state {
                let tombstones = txn
                    .open_table(DENTRY_TOMBSTONES_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for row in tombstones
                    .iter()
                    .map_err(|e| MetaError::Storage(e.to_string()))?
                {
                    let (k, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                    let _ = table
                        .remove(k.value())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        Self::rebuild_dentry_index_in_txn(&txn, DENTRIES_TABLE, DENTRIES_BY_INODE_TABLE)?;
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, DENTRY_TOMBSTONES_TABLE)?;
            }
        }
        {
            let mut table = txn
                .open_table(BASE_DENTRIES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.dentries {
                table
                    .insert(k.as_slice(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Self::rebuild_dentry_index_in_txn(&txn, BASE_DENTRIES_TABLE, BASE_DENTRIES_BY_INODE_TABLE)?;
        {
            let mut settings = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            settings
                .insert(
                    DENTRY_BY_INODE_INDEX_VERSION_SETTING,
                    CURRENT_DENTRY_BY_INODE_INDEX_VERSION,
                )
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        {
            if !preserve_local_state {
                Self::replace_regular_file_chunk_rows_from_snapshot_rows(
                    &txn,
                    REGULAR_FILE_OVERLAY_TABLE,
                    snapshot.regular_file_overlay.as_slice(),
                    |bytes| {
                        let state: RegularFileOverlayState = decode(bytes)?;
                        Ok(state.chunks)
                    },
                    true,
                )?;
            }
        }
        {
            Self::replace_regular_file_chunk_rows_from_snapshot_rows(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                snapshot.regular_file_base.as_slice(),
                |bytes| {
                    let state: RegularFileBaseState = decode(bytes)?;
                    Ok(state.chunks)
                },
                false,
            )?;
        }
        {
            if !preserve_local_state {
                let mut table = txn
                    .open_table(PENDING_DELETIONS_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                table
                    .retain(|_, _| false)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for (k, v) in &snapshot.pending_deletions {
                    table
                        .insert(*k, *v)
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, SYMLINKS_TABLE)?;
            }
        }
        {
            let mut table = txn
                .open_table(BASE_SYMLINKS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.symlinks {
                table
                    .insert(*k, v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, SYMLINK_TOMBSTONES_TABLE)?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, XATTRS_TABLE)?;
            }
        }
        {
            let mut table = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.xattrs {
                let name = xattr_name_from_key(k.as_slice());
                let stored = encode_xattr_value_for_storage(name, v.as_slice());
                table
                    .insert(k.as_slice(), stored.as_ref())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, XATTR_TOMBSTONES_TABLE)?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, ACL_RULES_TABLE)?;
            }
        }
        {
            let mut table = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.acl_rules {
                table
                    .insert(k.as_slice(), *v)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        {
            if !preserve_local_state {
                Self::clear_table_in_txn(&txn, ACL_RULE_TOMBSTONES_TABLE)?;
            }
        }
        {
            if !preserve_event_log {
                let mut table = txn
                    .open_table(EVENT_LOG_TABLE)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                table
                    .retain(|_, _| false)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
                for (k, v) in &snapshot.event_log {
                    table
                        .insert(*k, v.as_slice())
                        .map_err(|e| MetaError::Storage(e.to_string()))?;
                }
            }
        }
        Self::rebuild_inode_lifecycle_table_in_txn(&txn)?;

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }
}
