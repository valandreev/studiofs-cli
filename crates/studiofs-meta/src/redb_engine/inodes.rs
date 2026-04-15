use super::*;

const INODE_LIFECYCLE_SCHEMA_VERSION_KEY: &str = "inode_lifecycle_schema_version";
const CURRENT_INODE_LIFECYCLE_SCHEMA_VERSION: u64 = 1;

impl RedbMetaEngine {
    pub fn ensure_inode_lifecycle_table(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let settings = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current_version = settings
            .get(INODE_LIFECYCLE_SCHEMA_VERSION_KEY)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| {
                let raw = value.value();
                if raw.len() != 8 {
                    return Err(MetaError::Storage(
                        "invalid inode lifecycle schema version payload".to_string(),
                    ));
                }
                let bytes: [u8; 8] = raw.try_into().map_err(|_| {
                    MetaError::Storage("invalid inode lifecycle schema version bytes".to_string())
                })?;
                Ok(u64::from_le_bytes(bytes))
            })
            .transpose()?;
        drop(settings);
        if current_version == Some(CURRENT_INODE_LIFECYCLE_SCHEMA_VERSION)
            && Self::inode_lifecycle_table_is_current_in_txn(&txn)?
        {
            txn.abort().map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(());
        }
        Self::rebuild_inode_lifecycle_table_in_txn(&txn)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode(value.value()))
            .transpose()
    }

    pub fn rebuild_inode_lifecycle_table(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::rebuild_inode_lifecycle_table_in_txn(&txn)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn rebuild_inode_lifecycle_table_in_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<()> {
        let preserved_deleted = {
            let table = txn
                .open_table(INODE_LIFECYCLE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut rows = Vec::new();
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let record: InodeLifecycleRecord = decode(value.value())?;
                if record.state == InodeLifecycleState::DeletedBeforePublish {
                    rows.push(record);
                }
            }
            rows
        };
        let mut lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        lifecycle
            .retain(|_, _| false)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for record in preserved_deleted {
            let bytes = encode(&record)?;
            lifecycle
                .insert(record.ino, bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        let regular_attrs = {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut attrs = Vec::new();
            for row in inodes
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let attr: InodeAttr = decode(value.value())?;
                if attr.kind == FileType::RegularFile {
                    attrs.push(attr);
                }
            }
            attrs
        };
        for attr in regular_attrs {
            let record = Self::compute_inode_lifecycle_record_in_txn(txn, &attr)?;
            let bytes = encode(&record)?;
            lifecycle
                .insert(record.ino, bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        drop(lifecycle);
        Self::set_inode_lifecycle_schema_version_in_txn(txn)?;
        Ok(())
    }

    fn inode_lifecycle_table_is_current_in_txn(txn: &redb::WriteTransaction) -> MetaResult<bool> {
        let lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut lifecycle_rows = lifecycle
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if let Some(row) = lifecycle_rows.next() {
            let _ = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(true);
        }
        drop(lifecycle_rows);
        drop(lifecycle);

        Ok(!Self::has_regular_file_inodes_in_txn(txn)?)
    }

    fn has_regular_file_inodes_in_txn(txn: &redb::WriteTransaction) -> MetaResult<bool> {
        let inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in inodes
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr: InodeAttr = decode(value.value())?;
            if attr.kind == FileType::RegularFile {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn set_inode_lifecycle_schema_version_in_txn(txn: &redb::WriteTransaction) -> MetaResult<()> {
        let mut settings = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let bytes = CURRENT_INODE_LIFECYCLE_SCHEMA_VERSION.to_le_bytes();
        settings
            .insert(INODE_LIFECYCLE_SCHEMA_VERSION_KEY, bytes.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn put_inode_lifecycle_in_txn(
        txn: &redb::WriteTransaction,
        record: &InodeLifecycleRecord,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let bytes = encode(record)?;
        table
            .insert(record.ino, bytes.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_inode_lifecycle_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = table
            .remove(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn record_deleted_before_publish_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
        deferred_cleanup_object_ids: Vec<u64>,
    ) -> MetaResult<InodeLifecycleRecord> {
        let has_published_base = attr.committed_size > 0 || attr.committed_content_epoch > 0;
        let record = InodeLifecycleRecord {
            ino: attr.ino,
            state: InodeLifecycleState::DeletedBeforePublish,
            local_visible_size: attr.size,
            published_size: attr.committed_size,
            local_content_epoch: attr.content_epoch,
            published_content_epoch: attr.committed_content_epoch,
            has_published_base,
            retention: PayloadRetentionClass::DeletedBeforePublish,
            deferred_cleanup_object_ids,
        };
        Self::put_inode_lifecycle_in_txn(txn, &record)?;
        Ok(record)
    }

    pub(super) fn sync_inode_lifecycle_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<InodeLifecycleRecord>> {
        let attr = match Self::get_attr_from_write_table(txn, INODES_TABLE, ino)? {
            Some(attr) => attr,
            None => {
                Self::remove_inode_lifecycle_in_txn(txn, ino)?;
                return Ok(None);
            }
        };
        if attr.kind != FileType::RegularFile {
            Self::remove_inode_lifecycle_in_txn(txn, ino)?;
            return Ok(None);
        }
        let record = Self::compute_inode_lifecycle_record_in_txn(txn, &attr)?;
        Self::put_inode_lifecycle_in_txn(txn, &record)?;
        Ok(Some(record))
    }

    pub(super) fn compute_inode_lifecycle_record_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<InodeLifecycleRecord> {
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            attr.ino,
        )?;
        let has_published_base = !Self::base_manifest_from_graph_rows_in_txn(txn, attr.ino)?
            .is_empty()
            || attr.committed_size > 0
            || attr.committed_content_epoch > 0;
        let local_matches_published = attr.size == attr.committed_size
            && attr.content_epoch == attr.committed_content_epoch
            && Self::manifest_matches_committed_in_txn(txn, attr.ino)?;

        let overlay_state = Self::overlay_lifecycle_state(overlay_chunks.values());
        let state = if local_matches_published {
            if has_published_base {
                InodeLifecycleState::Published
            } else {
                InodeLifecycleState::LocalOnly
            }
        } else {
            match overlay_state {
                Some(InodeLifecycleState::Uploading) => InodeLifecycleState::Uploading,
                Some(InodeLifecycleState::PublishReady) => InodeLifecycleState::PublishReady,
                Some(InodeLifecycleState::LocalOnly) | None => InodeLifecycleState::LocalOnly,
                Some(InodeLifecycleState::Published)
                | Some(InodeLifecycleState::DeletedBeforePublish) => InodeLifecycleState::LocalOnly,
            }
        };
        let has_unpublished_payload = matches!(
            overlay_state,
            Some(InodeLifecycleState::LocalOnly)
                | Some(InodeLifecycleState::Uploading)
                | Some(InodeLifecycleState::PublishReady)
        );
        let retention = match state {
            InodeLifecycleState::LocalOnly | InodeLifecycleState::Uploading
                if !has_unpublished_payload && has_published_base =>
            {
                PayloadRetentionClass::PublishedWarm
            }
            InodeLifecycleState::LocalOnly | InodeLifecycleState::Uploading => {
                PayloadRetentionClass::PinnedUnpublished
            }
            InodeLifecycleState::PublishReady => PayloadRetentionClass::PublishReadyRetained,
            InodeLifecycleState::Published => PayloadRetentionClass::PublishedWarm,
            InodeLifecycleState::DeletedBeforePublish => {
                PayloadRetentionClass::DeletedBeforePublish
            }
        };

        Ok(InodeLifecycleRecord {
            ino: attr.ino,
            state,
            local_visible_size: attr.size,
            published_size: attr.committed_size,
            local_content_epoch: attr.content_epoch,
            published_content_epoch: attr.committed_content_epoch,
            has_published_base,
            retention,
            deferred_cleanup_object_ids: Vec::new(),
        })
    }

    fn overlay_lifecycle_state<'a>(
        chunk_states: impl Iterator<Item = &'a RegularFileChunkState>,
    ) -> Option<InodeLifecycleState> {
        let mut saw_local_only = false;
        let mut saw_uploaded = false;
        for chunk_state in chunk_states {
            for slice in chunk_state.slices.values() {
                match slice.block_state {
                    BlockRefState::Uploading => return Some(InodeLifecycleState::Uploading),
                    BlockRefState::LocalOnly | BlockRefState::Recovering => {
                        saw_local_only = true;
                    }
                    BlockRefState::Uploaded => {
                        saw_uploaded = true;
                    }
                    BlockRefState::Published => {}
                }
            }
        }
        if saw_local_only {
            Some(InodeLifecycleState::LocalOnly)
        } else if saw_uploaded {
            Some(InodeLifecycleState::PublishReady)
        } else {
            None
        }
    }

    pub(super) fn local_overlay_inodes_from_txn(
        txn: &redb::ReadTransaction,
    ) -> MetaResult<HashSet<Ino>> {
        let mut overlay = HashSet::new();

        let regular_overlay =
            Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_OVERLAY_TABLE)?;
        let regular_base = Self::regular_file_chunk_states_from_txn(txn, REGULAR_FILE_BASE_TABLE)?;
        for (ino, overlay_chunks) in regular_overlay {
            let overlay_signature = Self::regular_file_graph_signature(&overlay_chunks);
            if overlay_signature.is_empty() {
                continue;
            }
            let base_signature = regular_base
                .get(&ino)
                .map(Self::regular_file_graph_signature)
                .unwrap_or_default();
            if overlay_signature != base_signature {
                overlay.insert(ino);
            }
        }

        Ok(overlay)
    }

    pub(super) fn local_overlay_attr_inodes_from_txn(
        txn: &redb::ReadTransaction,
    ) -> MetaResult<HashSet<Ino>> {
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Self::collect_overlay_inode_attrs_from_tables(&current, &base, &tombstones)
    }

    pub(super) fn resolve_attr_in_read_snapshot(
        txn: &redb::ReadTransaction,
        ino: Ino,
        overlay_block_inodes: &HashSet<Ino>,
        overlay_attr_inodes: &HashSet<Ino>,
    ) -> MetaResult<InodeAttr> {
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let prefer_current =
            overlay_attr_inodes.contains(&ino) || overlay_block_inodes.contains(&ino);

        Self::resolve_attr_from_opened_tables(&base, &current, &tombstones, ino, prefer_current)
    }

    pub fn local_overlay_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_inodes_from_txn(&txn)
    }

    /// Returns inode attrs that diverged from the last imported/applied base view.
    pub fn local_overlay_attr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::local_overlay_attr_inodes_from_txn(&txn)
    }
}

impl MetaInodes for RedbMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        match table
            .get(name)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => Ok(v.value()),
            None => Ok(0),
        }
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val = Self::update_counter_in_txn(&txn, name, |current| current + delta)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let new_val =
            Self::update_counter_in_txn(&txn, name, |current| current.saturating_sub(delta))?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(name, value)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn next_inode(&self) -> MetaResult<Ino> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let ino;
        {
            let mut table = txn
                .open_table(COUNTERS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            ino = match table
                .get(counters::NEXT_INODE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(v) => v.value(),
                None => ROOT_INO + 1,
            };
            table
                .insert(counters::NEXT_INODE, ino + 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(ino)
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        Self::resolve_attr_in_read_snapshot(&txn, ino, &overlay_block_inodes, &overlay_attr_inodes)
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            let mut normalized = attr.clone();
            if normalized.kind == FileType::RegularFile {
                let has_committed_manifest =
                    !Self::base_manifest_from_graph_rows_in_txn(&txn, normalized.ino)?.is_empty();
                Self::transition_upload_state_for_local_mutation(
                    &mut normalized,
                    has_committed_manifest,
                );
            }
            Self::set_inode_overlay_in_txn(&txn, &normalized)?;
            Self::sync_inode_lifecycle_in_txn(&txn, normalized.ino)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetAttr {
                    attr: normalized.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_inode_overlay_in_txn(&txn, ino)?;

            Self::clear_inode_overlay_aux_in_txn(&txn, ino)?;
            Self::remove_inode_lifecycle_in_txn(&txn, ino)?;

            Self::append_event_in_txn(&txn, &crate::events::MetaEvent::RemoveAttr { ino })?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_inode_attrs(&self) -> MetaResult<Vec<InodeAttr>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_block_inodes = Self::local_overlay_inodes_from_txn(&txn)?;
        let overlay_attr_inodes = Self::local_overlay_attr_inodes_from_txn(&txn)?;

        let mut inos = std::collections::BTreeSet::new();
        for table_def in [BASE_INODES_TABLE, INODES_TABLE] {
            let table = txn
                .open_table(table_def)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (ino, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                inos.insert(ino.value());
            }
        }

        let mut attrs = Vec::with_capacity(inos.len());
        for ino in inos {
            match Self::resolve_attr_in_read_snapshot(
                &txn,
                ino,
                &overlay_block_inodes,
                &overlay_attr_inodes,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_format_info(name: &str) -> FormatInfo {
        FormatInfo::new(
            name.to_string(),
            format!("{name}-uuid"),
            FormatInfoDataStore {
                endpoint: "https://storage.example".to_string(),
                bucket: "bucket-demo".to_string(),
                access_key: "access-key".to_string(),
                secret_key: "secret-key".to_string(),
                region: String::new(),
            },
            u32::from(CURRENT_FORMAT_VERSION),
        )
    }

    #[test]
    fn ensure_inode_lifecycle_table_recovers_empty_table_with_current_version() {
        let temp = tempdir().unwrap();
        let db_path = temp.path().join("inode-lifecycle-empty.redb");

        let file_ino = {
            let engine = RedbMetaEngine::open(&db_path).unwrap();
            engine.init_root(1000, 1000).unwrap();
            engine
                .set_format_info(&test_format_info("inode-lifecycle-empty"))
                .unwrap();
            let file = engine
                .create_file(ROOT_INO, b"rebuild-me.bin", 1000, 1000, 0o644)
                .unwrap();
            let mut attr = engine.get_attr(file.ino).unwrap();
            attr.size = 4096;
            attr.content_epoch = 1;
            engine.set_attr(&attr).unwrap();
            engine
                .set_file_block(file.ino, 0, &BlockRef::new_partial(777, 4096))
                .unwrap();
            file.ino
        };

        {
            let db = Database::create(&db_path).unwrap();
            let txn = db.begin_write().unwrap();
            let mut lifecycle = txn.open_table(INODE_LIFECYCLE_TABLE).unwrap();
            lifecycle.retain(|_, _| false).unwrap();
            drop(lifecycle);
            let mut settings = txn.open_table(SETTINGS_TABLE).unwrap();
            settings
                .insert(
                    INODE_LIFECYCLE_SCHEMA_VERSION_KEY,
                    CURRENT_INODE_LIFECYCLE_SCHEMA_VERSION
                        .to_le_bytes()
                        .as_slice(),
                )
                .unwrap();
            drop(settings);
            txn.commit().unwrap();
        }

        let reopened = RedbMetaEngine::open(&db_path).unwrap();
        let lifecycle = reopened.get_inode_lifecycle(file_ino).unwrap().unwrap();
        assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
        assert_eq!(lifecycle.local_visible_size, 4096);
        assert_eq!(lifecycle.local_content_epoch, 1);
    }
}
