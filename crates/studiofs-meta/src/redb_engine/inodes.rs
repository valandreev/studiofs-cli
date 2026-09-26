use super::*;

const INODE_LIFECYCLE_SCHEMA_VERSION_KEY: &str = "inode_lifecycle_schema_version";
const CURRENT_INODE_LIFECYCLE_SCHEMA_VERSION: u64 = 1;
const PENDING_REGULAR_FILE_RECOVERY_WIRE_MAGIC: &[u8; 8] = b"SFS-PFR\0";
const PENDING_REGULAR_FILE_RECOVERY_WIRE_VERSION: u8 = 1;
pub(crate) type PendingRecoveryDentry = (Ino, Vec<u8>);

#[derive(serde::Deserialize)]
struct LegacyPendingRegularFileRecoveryRecord {
    attr: InodeAttr,
    overlay: crate::schema::RegularFileOverlayState,
}

/// Local-only recovery copy of uploaded slices for one unpublished content epoch.
/// Snapshot replacement excludes this versioned row; proven publication,
/// superseding epochs, or inode deletion retire it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct PendingRegularFileRecoveryRecord {
    pub(crate) attr: InodeAttr,
    pub(crate) overlay: crate::schema::RegularFileOverlayState,
    #[serde(default)]
    pub(crate) dentries: Option<Vec<PendingRecoveryDentry>>,
    #[serde(default)]
    pub(crate) had_published_inode: Option<bool>,
}

fn encode_pending_regular_file_recovery(
    record: &PendingRegularFileRecoveryRecord,
) -> MetaResult<Vec<u8>> {
    let payload = encode(record)?;
    let mut bytes =
        Vec::with_capacity(PENDING_REGULAR_FILE_RECOVERY_WIRE_MAGIC.len() + 1 + payload.len());
    bytes.extend_from_slice(PENDING_REGULAR_FILE_RECOVERY_WIRE_MAGIC);
    bytes.push(PENDING_REGULAR_FILE_RECOVERY_WIRE_VERSION);
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_pending_regular_file_recovery(
    bytes: &[u8],
) -> MetaResult<PendingRegularFileRecoveryRecord> {
    if let Some(versioned) = bytes.strip_prefix(PENDING_REGULAR_FILE_RECOVERY_WIRE_MAGIC) {
        let Some((&version, payload)) = versioned.split_first() else {
            return Err(MetaError::Storage(
                "pending regular-file recovery row has a truncated version".to_string(),
            ));
        };
        if version != PENDING_REGULAR_FILE_RECOVERY_WIRE_VERSION {
            return Err(MetaError::Storage(format!(
                "unsupported pending regular-file recovery row version: {version}"
            )));
        }
        return decode(payload);
    }

    let legacy: LegacyPendingRegularFileRecoveryRecord = decode(bytes)?;
    Ok(PendingRegularFileRecoveryRecord {
        attr: legacy.attr,
        overlay: legacy.overlay,
        dentries: None,
        had_published_inode: None,
    })
}

fn chunks_have_uploaded_payload(chunks: &BTreeMap<ChunkId, RegularFileChunkState>) -> bool {
    chunks
        .values()
        .flat_map(|chunk| chunk.slices.values())
        .any(|slice| {
            slice.payload.is_some()
                && slice.payload_state == crate::schema::PayloadRefState::Uploaded
        })
}

impl RedbMetaEngine {
    pub(crate) fn set_attr_in_txn(
        txn: &WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<InodeAttr> {
        let mut normalized = attr.clone();
        if normalized.kind == FileType::RegularFile {
            let has_committed_graph = Self::has_regular_file_chunk_rows_in_write_txn(
                txn,
                REGULAR_FILE_BASE_TABLE,
                normalized.ino,
            )?;
            Self::transition_upload_state_for_local_mutation(&mut normalized, has_committed_graph);
        }
        Self::set_inode_overlay_in_txn(txn, &normalized)?;
        Self::sync_inode_lifecycle_in_txn(txn, normalized.ino)?;
        Self::append_event_in_txn(
            txn,
            &crate::events::MetaEvent::SetAttr {
                attr: normalized.clone(),
            },
        )?;
        Ok(normalized)
    }

    pub(crate) fn get_base_attr(&self, ino: Ino) -> MetaResult<Option<InodeAttr>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode::<InodeAttr>(value.value()))
            .transpose()
    }

    pub(crate) fn get_base_attr_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<InodeAttr>> {
        let table = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode::<InodeAttr>(value.value()))
            .transpose()
    }

    /// Ensure the inode lifecycle table exists and matches the current schema version.
    pub fn ensure_inode_lifecycle_table(&self) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
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

    pub(super) fn read_counter_from_read_txn(txn: &ReadTransaction, name: &str) -> MetaResult<u64> {
        let table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(table
            .get(name)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map_or(0, |value| value.value()))
    }

    /// Return the current lifecycle record for one inode, if it is a regular file.
    ///
    /// The persisted lifecycle row is a recovery aid, not the source of truth for
    /// the current overlay state. Upload completion can update overlay slices in
    /// separate transactions, leaving that row briefly stale; derive the state
    /// from the same read snapshot used by the foreground metadata.
    pub fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        let txn = self.begin_read_txn()?;
        Self::get_inode_lifecycle_from_read_txn(&txn, ino)
    }

    pub(super) fn get_inode_lifecycle_from_read_txn(
        txn: &ReadTransaction,
        ino: Ino,
    ) -> MetaResult<Option<InodeLifecycleRecord>> {
        let stored_lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode::<InodeLifecycleRecord>(value.value()))
            .transpose()?;
        let Some(stored_lifecycle) = stored_lifecycle else {
            return Ok(None);
        };
        if stored_lifecycle.state == InodeLifecycleState::DeletedBeforePublish {
            return Ok(Some(stored_lifecycle));
        }

        let prefer_current = Self::prefer_current_attr_for_inode_from_txn(txn, ino)?;
        let attr =
            match Self::resolve_attr_with_preference_in_read_snapshot(txn, ino, prefer_current) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => return Ok(Some(stored_lifecycle)),
                Err(error) => return Err(error),
            };
        if attr.kind != FileType::RegularFile {
            return Ok(Some(stored_lifecycle));
        }

        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_read_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        if overlay_chunks.is_empty() {
            return Ok(Some(stored_lifecycle));
        }
        let has_published_base =
            Self::has_regular_file_chunk_rows_in_read_txn(txn, REGULAR_FILE_BASE_TABLE, ino)?
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0;

        Ok(Some(Self::compute_inode_lifecycle_record(
            &attr,
            overlay_chunks.values(),
            has_published_base,
            stored_lifecycle.deferred_cleanup_object_ids,
        )))
    }

    pub(crate) fn get_inode_lifecycle_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<InodeLifecycleRecord>> {
        let stored_lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|value| decode::<InodeLifecycleRecord>(value.value()))
            .transpose()?;
        let Some(stored_lifecycle) = stored_lifecycle else {
            return Ok(None);
        };
        if stored_lifecycle.state == InodeLifecycleState::DeletedBeforePublish {
            return Ok(Some(stored_lifecycle));
        }
        let attr = match Self::get_attr_merged_in_write_txn(txn, ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(Some(stored_lifecycle)),
            Err(error) => return Err(error),
        };
        if attr.kind != FileType::RegularFile {
            return Ok(Some(stored_lifecycle));
        }
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        if overlay_chunks.is_empty() {
            return Ok(Some(stored_lifecycle));
        }
        let has_published_base =
            Self::has_regular_file_chunk_rows_in_write_txn(txn, REGULAR_FILE_BASE_TABLE, ino)?
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0;
        Ok(Some(Self::compute_inode_lifecycle_record(
            &attr,
            overlay_chunks.values(),
            has_published_base,
            stored_lifecycle.deferred_cleanup_object_ids,
        )))
    }

    /// Return all persisted inode lifecycle rows.
    pub fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut records = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            records.push(decode(value.value())?);
        }
        Ok(records)
    }

    /// Rebuild inode lifecycle rows from the current foreground metadata state.
    pub fn rebuild_inode_lifecycle_table(&self) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
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
        let regular_inos = {
            let inodes = txn
                .open_table(INODES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut inos = Vec::new();
            for row in inodes
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (ino, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                let attr: InodeAttr = decode(value.value())?;
                if attr.kind == FileType::RegularFile {
                    inos.push(ino.value());
                }
            }
            inos
        };
        for ino in regular_inos {
            let Some(attr) = Self::get_attr_from_write_table(txn, INODES_TABLE, ino)? else {
                continue;
            };
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
        Self::remove_pending_regular_file_recovery_in_txn(txn, ino)?;
        let mut table = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = table
            .remove(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(crate) fn pending_uploaded_regular_file_recoveries(
        &self,
    ) -> MetaResult<Vec<PendingRegularFileRecoveryRecord>> {
        let txn = self.begin_read_txn()?;
        let table = txn
            .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        table
            .iter()
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|row| {
                let (_, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                decode_pending_regular_file_recovery(value.value())
            })
            .collect()
    }

    pub(crate) fn pending_recovery_payloads_are_published(
        &self,
        recovery: &PendingRegularFileRecoveryRecord,
    ) -> MetaResult<bool> {
        let txn = self.begin_read_txn()?;
        Self::pending_recovery_payloads_are_published_in_read_txn(&txn, recovery)
    }

    pub(crate) fn reconcile_pending_regular_file_recoveries(&self) -> MetaResult<()> {
        if self.db.is_none() {
            return Ok(());
        }

        let txn = self.begin_immediate_write_txn()?;
        let records = {
            let table = txn
                .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            table
                .iter()
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .map(|row| {
                    let (_, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                    decode_pending_regular_file_recovery(value.value())
                })
                .collect::<MetaResult<Vec<_>>>()?
        };

        for record in records {
            if Self::pending_recovery_is_obsolete_in_write_txn(&txn, &record)? {
                Self::remove_pending_regular_file_recovery_in_txn(&txn, record.attr.ino)?;
            }
        }
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    fn pending_recovery_payloads_are_published_in_read_txn(
        txn: &redb::ReadTransaction,
        recovery: &PendingRegularFileRecoveryRecord,
    ) -> MetaResult<bool> {
        let recovery_ids: std::collections::HashSet<_> = recovery
            .overlay
            .chunks
            .values()
            .flat_map(|chunk| chunk.slices.values())
            .filter_map(|slice| slice.payload.as_ref().map(|payload| payload.object_id))
            .collect();
        if recovery_ids.is_empty() {
            return Ok(false);
        }
        let base = Self::regular_file_chunk_states_for_inode_in_read_txn(
            txn,
            REGULAR_FILE_BASE_TABLE,
            recovery.attr.ino,
        )?;
        let base_ids: std::collections::HashSet<_> = base
            .values()
            .flat_map(|chunk| chunk.slices.values())
            .filter_map(|slice| slice.payload.as_ref().map(|payload| payload.object_id))
            .collect();
        Ok(recovery_ids.is_subset(&base_ids))
    }

    fn pending_recovery_is_obsolete_in_write_txn(
        txn: &redb::WriteTransaction,
        recovery: &PendingRegularFileRecoveryRecord,
    ) -> MetaResult<bool> {
        let Some(current) = Self::get_attr_from_write_table(txn, INODES_TABLE, recovery.attr.ino)?
        else {
            return Ok(recovery.had_published_inode == Some(true));
        };
        if current.kind != FileType::RegularFile || current.crtime != recovery.attr.crtime {
            return Ok(true);
        }
        if current.content_epoch > recovery.attr.content_epoch {
            return Ok(true);
        }
        if !current.is_upload_pending() {
            let base = Self::regular_file_chunk_states_for_inode_in_write_txn(
                txn,
                REGULAR_FILE_BASE_TABLE,
                recovery.attr.ino,
            )?;
            let recovery_ids: std::collections::HashSet<_> = recovery
                .overlay
                .chunks
                .values()
                .flat_map(|chunk| chunk.slices.values())
                .filter_map(|slice| slice.payload.as_ref().map(|payload| payload.object_id))
                .collect();
            let base_ids: std::collections::HashSet<_> = base
                .values()
                .flat_map(|chunk| chunk.slices.values())
                .filter_map(|slice| slice.payload.as_ref().map(|payload| payload.object_id))
                .collect();
            return Ok(!recovery_ids.is_empty() && recovery_ids.is_subset(&base_ids));
        }
        Ok(false)
    }

    pub(super) fn pending_recovery_object_ids_from_read_txn(
        txn: &ReadTransaction,
        refs: &mut std::collections::HashSet<u64>,
    ) -> MetaResult<()> {
        let recoveries = txn
            .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        for row in recoveries
            .iter()
            .map_err(|error| MetaError::Storage(error.to_string()))?
        {
            let (_, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
            let record = decode_pending_regular_file_recovery(value.value())?;
            for slice in record
                .overlay
                .chunks
                .values()
                .flat_map(|chunk| chunk.slices.values())
            {
                if let Some(payload) = &slice.payload {
                    refs.insert(payload.object_id);
                }
            }
        }
        Ok(())
    }

    /// Capture legacy pending overlays from the recovered live view before a
    /// remote snapshot replaces that view during redb repair.
    pub(crate) fn backfill_missing_uploaded_regular_file_recoveries(&self) -> MetaResult<()> {
        let txn = self.begin_immediate_write_txn()?;
        let attrs = {
            let table = txn
                .open_table(INODES_TABLE)
                .map_err(|error| MetaError::Storage(error.to_string()))?;
            let mut attrs = Vec::new();
            for row in table
                .iter()
                .map_err(|error| MetaError::Storage(error.to_string()))?
            {
                let (_, value) = row.map_err(|error| MetaError::Storage(error.to_string()))?;
                let attr: InodeAttr = decode(value.value())?;
                if attr.kind == FileType::RegularFile && attr.is_upload_pending() {
                    attrs.push(attr);
                }
            }
            attrs
        };

        let mut recoveries = txn
            .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        for attr in attrs {
            let previous = recoveries
                .get(attr.ino)
                .map_err(|error| MetaError::Storage(error.to_string()))?
                .map(|value| decode_pending_regular_file_recovery(value.value()))
                .transpose()?;
            let chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
                &txn,
                REGULAR_FILE_OVERLAY_TABLE,
                attr.ino,
            )?;
            let ino = attr.ino;
            let (dentries, had_published_inode) =
                Self::pending_recovery_namespace_in_write_txn(&txn, ino)?;
            let record = match previous {
                Some(mut previous)
                    if previous.attr.crtime == attr.crtime
                        && previous.attr.content_epoch > attr.content_epoch =>
                {
                    previous.dentries.get_or_insert(dentries);
                    previous.had_published_inode =
                        Some(previous.had_published_inode.unwrap_or(false) || had_published_inode);
                    previous
                }
                Some(previous)
                    if previous.attr.crtime == attr.crtime
                        && previous.attr.content_epoch == attr.content_epoch =>
                {
                    let overlay = Self::uploaded_regular_file_recovery_overlay(
                        &chunks,
                        Some(&previous.overlay),
                    );
                    if overlay.chunks.is_empty() {
                        let mut previous = previous;
                        previous.attr = attr.clone();
                        previous.dentries = Some(dentries);
                        previous.had_published_inode = Some(
                            previous.had_published_inode.unwrap_or(false) || had_published_inode,
                        );
                        previous
                    } else {
                        PendingRegularFileRecoveryRecord {
                            attr: attr.clone(),
                            overlay,
                            dentries: Some(dentries),
                            had_published_inode: Some(had_published_inode),
                        }
                    }
                }
                _ => {
                    let overlay = Self::uploaded_regular_file_recovery_overlay(&chunks, None);
                    if overlay.chunks.is_empty() {
                        let _ = recoveries
                            .remove(ino)
                            .map_err(|error| MetaError::Storage(error.to_string()))?;
                        continue;
                    }
                    PendingRegularFileRecoveryRecord {
                        attr: attr.clone(),
                        overlay,
                        dentries: Some(dentries),
                        had_published_inode: Some(had_published_inode),
                    }
                }
            };
            let bytes = encode_pending_regular_file_recovery(&record)?;
            recoveries
                .insert(ino, bytes.as_slice())
                .map_err(|error| MetaError::Storage(error.to_string()))?;
        }

        drop(recoveries);
        txn.commit()
            .map_err(|error| MetaError::Storage(error.to_string()))
    }

    fn pending_recovery_namespace_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<(Vec<PendingRecoveryDentry>, bool)> {
        let dentries =
            Self::dentry_keys_for_inode_in_write_index(txn, DENTRIES_BY_INODE_TABLE, ino)?
                .into_iter()
                .map(|key| {
                    let (parent, name) = decode_dentry_key(&key)?;
                    Ok((parent, name.to_vec()))
                })
                .collect::<MetaResult<Vec<_>>>()?;
        let had_published_inode = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .get(ino)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .is_some();
        Ok((dentries, had_published_inode))
    }

    fn uploaded_regular_file_recovery_overlay(
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
        previous: Option<&crate::schema::RegularFileOverlayState>,
    ) -> crate::schema::RegularFileOverlayState {
        let chunks = chunks
            .iter()
            .filter_map(|(chunk_id, chunk)| {
                let previous_chunk = previous.and_then(|overlay| overlay.chunks.get(chunk_id));
                let slices = chunk
                    .slices
                    .iter()
                    .filter_map(|(slice_id, slice)| {
                        let mut slice = slice.clone();
                        if let Some(previous_chunk) = previous_chunk {
                            Self::preserve_completed_overlay_payload_state(
                                previous_chunk,
                                &mut slice,
                            );
                        }
                        (slice.payload.is_some()
                            && slice.payload_state == crate::schema::PayloadRefState::Uploaded)
                            .then_some((*slice_id, slice))
                    })
                    .collect::<BTreeMap<_, _>>();
                if slices.is_empty() {
                    None
                } else {
                    Some((
                        *chunk_id,
                        RegularFileChunkState {
                            next_slice_id: chunk.next_slice_id,
                            next_slice_order: chunk.next_slice_order,
                            slices,
                            fragmentation: chunk.fragmentation,
                        },
                    ))
                }
            })
            .collect();
        crate::schema::RegularFileOverlayState { chunks }
    }

    pub(super) fn remove_pending_regular_file_recovery_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let _ = table
            .remove(ino)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(())
    }

    pub(super) fn sync_pending_regular_file_recovery_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
        chunks: &BTreeMap<ChunkId, RegularFileChunkState>,
    ) -> MetaResult<()> {
        if attr.kind != FileType::RegularFile || !attr.is_upload_pending() {
            return Self::remove_pending_regular_file_recovery_in_txn(txn, attr.ino);
        }

        let mut recoveries = txn
            .open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        let previous = recoveries
            .get(attr.ino)
            .map_err(|error| MetaError::Storage(error.to_string()))?
            .map(|value| decode_pending_regular_file_recovery(value.value()))
            .transpose()?;

        if let Some(previous) = previous.as_ref()
            && previous.attr.crtime == attr.crtime
            && previous.attr.is_upload_pending()
            && previous.attr.content_epoch > attr.content_epoch
        {
            tracing::warn!(
                ino = attr.ino,
                recovery_epoch = previous.attr.content_epoch,
                current_epoch = attr.content_epoch,
                "kept a newer pending regular-file recovery row"
            );
            return Ok(());
        }

        let previous_for_epoch = previous.as_ref().filter(|previous| {
            previous.attr.crtime == attr.crtime
                && previous.attr.is_upload_pending()
                && previous.attr.content_epoch == attr.content_epoch
        });
        if !chunks_have_uploaded_payload(chunks) {
            if previous_for_epoch.is_none() {
                let _ = recoveries
                    .remove(attr.ino)
                    .map_err(|error| MetaError::Storage(error.to_string()))?;
            }
            return Ok(());
        }

        let overlay = Self::uploaded_regular_file_recovery_overlay(
            chunks,
            previous_for_epoch.map(|previous| &previous.overlay),
        );
        if previous_for_epoch.is_some_and(|previous| {
            previous.attr.crtime == attr.crtime
                && previous.attr.content_epoch == attr.content_epoch
                && previous.overlay.chunks == overlay.chunks
                && previous.dentries.is_some()
                && previous.had_published_inode.is_some()
        }) {
            return Ok(());
        }

        if overlay.chunks.is_empty() {
            return Ok(());
        }

        let (dentries, had_published_inode) =
            Self::pending_recovery_namespace_in_write_txn(txn, attr.ino)?;
        let bytes = encode_pending_regular_file_recovery(&PendingRegularFileRecoveryRecord {
            attr: attr.clone(),
            overlay,
            dentries: Some(dentries),
            had_published_inode: Some(had_published_inode),
        })?;
        recoveries
            .insert(attr.ino, bytes.as_slice())
            .map_err(|error| MetaError::Storage(error.to_string()))?;
        Ok(())
    }

    pub(super) fn sync_pending_regular_file_recovery_for_inode_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let Some(attr) = Self::get_attr_from_write_table(txn, INODES_TABLE, ino)? else {
            return Self::remove_pending_regular_file_recovery_in_txn(txn, ino);
        };
        if attr.kind != FileType::RegularFile {
            return Self::remove_pending_regular_file_recovery_in_txn(txn, ino);
        }
        let chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        Self::sync_pending_regular_file_recovery_in_txn(txn, &attr, &chunks)
    }

    pub(super) fn record_deleted_before_publish_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
        deferred_cleanup_object_ids: Vec<u64>,
    ) -> MetaResult<InodeLifecycleRecord> {
        Self::remove_pending_regular_file_recovery_in_txn(txn, attr.ino)?;
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
            Self::remove_pending_regular_file_recovery_in_txn(txn, ino)?;
            Self::remove_inode_lifecycle_in_txn(txn, ino)?;
            return Ok(None);
        }
        if !attr.is_upload_pending() {
            Self::remove_pending_regular_file_recovery_in_txn(txn, ino)?;
        }
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino,
        )?;
        let has_published_base =
            Self::has_regular_file_chunk_rows_in_write_txn(txn, REGULAR_FILE_BASE_TABLE, ino)?
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0;
        let record = Self::compute_inode_lifecycle_record(
            &attr,
            overlay_chunks.values(),
            has_published_base,
            Vec::new(),
        );
        Self::put_inode_lifecycle_in_txn(txn, &record)?;
        Ok(Some(record))
    }

    pub(super) fn sync_unpublished_upload_progress_lifecycle_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<InodeLifecycleRecord> {
        let overlay_chunks = Self::regular_file_chunk_states_for_inode_in_write_txn(
            txn,
            REGULAR_FILE_OVERLAY_TABLE,
            attr.ino,
        )?;
        Self::sync_pending_regular_file_recovery_in_txn(txn, attr, &overlay_chunks)?;
        let overlay_state = Self::overlay_lifecycle_state(overlay_chunks.values());
        let has_published_base =
            Self::has_regular_file_chunk_rows_in_write_txn(txn, REGULAR_FILE_BASE_TABLE, attr.ino)?
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0;
        let (local_matches_published, blockless_graph_ready) = if overlay_state.is_some() {
            (false, false)
        } else {
            (
                attr.size == attr.committed_size
                    && attr.content_epoch == attr.committed_content_epoch,
                attr.size != attr.committed_size
                    || attr.content_epoch != attr.committed_content_epoch,
            )
        };
        let (state, retention) = Self::derive_inode_lifecycle_state_and_retention(
            has_published_base,
            local_matches_published,
            overlay_state,
            blockless_graph_ready,
        );
        let record = InodeLifecycleRecord {
            ino: attr.ino,
            state,
            local_visible_size: attr.size,
            published_size: attr.committed_size,
            local_content_epoch: attr.content_epoch,
            published_content_epoch: attr.committed_content_epoch,
            has_published_base,
            retention,
            deferred_cleanup_object_ids: txn
                .open_table(INODE_LIFECYCLE_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .get(attr.ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| decode::<InodeLifecycleRecord>(value.value()))
                .transpose()?
                .map(|record| record.deferred_cleanup_object_ids)
                .unwrap_or_default(),
        };
        Self::put_inode_lifecycle_in_txn(txn, &record)?;
        Ok(record)
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
        let has_published_base =
            Self::has_regular_file_chunk_rows_in_write_txn(txn, REGULAR_FILE_BASE_TABLE, attr.ino)?
                || attr.committed_size > 0
                || attr.committed_content_epoch > 0;
        Ok(Self::compute_inode_lifecycle_record(
            attr,
            overlay_chunks.values(),
            has_published_base,
            Vec::new(),
        ))
    }

    fn compute_inode_lifecycle_record<'a>(
        attr: &InodeAttr,
        overlay_chunks: impl Iterator<Item = &'a RegularFileChunkState>,
        has_published_base: bool,
        deferred_cleanup_object_ids: Vec<u64>,
    ) -> InodeLifecycleRecord {
        let overlay_state = Self::overlay_lifecycle_state(overlay_chunks);
        let (local_matches_published, blockless_graph_ready) = if overlay_state.is_some() {
            (false, false)
        } else {
            (
                attr.size == attr.committed_size
                    && attr.content_epoch == attr.committed_content_epoch,
                attr.size != attr.committed_size
                    || attr.content_epoch != attr.committed_content_epoch,
            )
        };
        let (state, retention) = Self::derive_inode_lifecycle_state_and_retention(
            has_published_base,
            local_matches_published,
            overlay_state,
            blockless_graph_ready,
        );

        InodeLifecycleRecord {
            ino: attr.ino,
            state,
            local_visible_size: attr.size,
            published_size: attr.committed_size,
            local_content_epoch: attr.content_epoch,
            published_content_epoch: attr.committed_content_epoch,
            has_published_base,
            retention,
            deferred_cleanup_object_ids,
        }
    }

    fn derive_inode_lifecycle_state_and_retention(
        has_published_base: bool,
        local_matches_published: bool,
        overlay_state: Option<InodeLifecycleState>,
        blockless_graph_ready: bool,
    ) -> (InodeLifecycleState, PayloadRetentionClass) {
        let state = if local_matches_published {
            if has_published_base {
                InodeLifecycleState::Published
            } else {
                InodeLifecycleState::LocalOnly
            }
        } else if blockless_graph_ready {
            InodeLifecycleState::PublishReady
        } else {
            match overlay_state {
                Some(InodeLifecycleState::Uploading) => InodeLifecycleState::Uploading,
                Some(InodeLifecycleState::PublishReady) => InodeLifecycleState::PublishReady,
                _ => InodeLifecycleState::LocalOnly,
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
        (state, retention)
    }

    fn overlay_lifecycle_state<'a>(
        chunk_states: impl Iterator<Item = &'a RegularFileChunkState>,
    ) -> Option<InodeLifecycleState> {
        let mut saw_local_only = false;
        let mut saw_uploaded = false;
        for chunk_state in chunk_states {
            for slice in chunk_state.slices.values() {
                if slice.payload.is_none() {
                    saw_uploaded = true;
                    continue;
                }
                match slice.payload_state {
                    PayloadRefState::Uploading => return Some(InodeLifecycleState::Uploading),
                    PayloadRefState::LocalOnly | PayloadRefState::Recovering => {
                        saw_local_only = true;
                    }
                    PayloadRefState::Uploaded => {
                        saw_uploaded = true;
                    }
                    PayloadRefState::Published => {}
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

    fn local_overlay_attr_inode_from_txn(
        txn: &redb::ReadTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Self::local_overlay_attr_inode_from_opened_tables(&current, &base, &tombstones, ino)
    }

    fn local_overlay_attr_inode_from_opened_tables<TCurrent, TBase, TTombstones>(
        current: &TCurrent,
        base: &TBase,
        tombstones: &TTombstones,
        ino: Ino,
    ) -> MetaResult<bool>
    where
        TCurrent: ReadableTable<u64, &'static [u8]>,
        TBase: ReadableTable<u64, &'static [u8]>,
        TTombstones: ReadableTable<u64, u8>,
    {
        if tombstones
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some()
        {
            return Ok(true);
        }

        let current = current
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = base
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(Self::attr_differs_from_base(
            current.as_ref().map(|value| value.value()),
            base.as_ref().map(|value| value.value()),
        ))
    }

    fn attr_differs_from_base(current: Option<&[u8]>, base: Option<&[u8]>) -> bool {
        match (current, base) {
            (Some(current), Some(base)) => current != base,
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    fn prefer_current_attr_for_inode_from_txn(
        txn: &redb::ReadTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        Ok(Self::local_overlay_attr_inode_from_txn(txn, ino)?
            || Self::regular_file_overlay_differs_from_base_in_read_txn(txn, ino)?)
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
        let prefer_current =
            overlay_attr_inodes.contains(&ino) || overlay_block_inodes.contains(&ino);

        Self::resolve_attr_with_preference_in_read_snapshot(txn, ino, prefer_current)
    }

    fn resolve_attr_with_preference_in_read_snapshot(
        txn: &redb::ReadTransaction,
        ino: Ino,
        prefer_current: bool,
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

        Self::resolve_attr_from_opened_tables(&base, &current, &tombstones, ino, prefer_current)
    }

    /// Returns inode ids whose local state differs from the last imported base view.
    pub fn local_overlay_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self.begin_read_txn()?;
        Self::local_overlay_inodes_from_txn(&txn)
    }

    /// Returns inode attrs that diverged from the last imported/applied base view.
    pub fn local_overlay_attr_inodes(&self) -> MetaResult<HashSet<Ino>> {
        let txn = self.begin_read_txn()?;
        Self::local_overlay_attr_inodes_from_txn(&txn)
    }
}

impl MetaInodes for RedbMetaEngine {
    fn get_counter(&self, name: &str) -> MetaResult<u64> {
        let txn = self.begin_read_txn()?;
        Self::read_counter_from_read_txn(&txn, name)
    }

    fn incr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self.begin_write_txn()?;
        let new_val = Self::update_counter_in_txn(&txn, name, |current| current + delta)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn decr_counter(&self, name: &str, delta: u64) -> MetaResult<u64> {
        let txn = self.begin_write_txn()?;
        let new_val =
            Self::update_counter_in_txn(&txn, name, |current| current.saturating_sub(delta))?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    fn set_counter(&self, name: &str, value: u64) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
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
        let txn = self.begin_write_txn()?;
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

    fn next_inode_ids(&self, count: u32) -> MetaResult<Vec<Ino>> {
        self.next_object_ids(count)
    }

    fn get_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        let txn = self.begin_read_txn()?;
        let prefer_current = Self::prefer_current_attr_for_inode_from_txn(&txn, ino)?;
        Self::resolve_attr_with_preference_in_read_snapshot(&txn, ino, prefer_current)
    }

    fn get_inode_lifecycle(&self, ino: Ino) -> MetaResult<Option<InodeLifecycleRecord>> {
        RedbMetaEngine::get_inode_lifecycle(self, ino)
    }

    fn list_inode_lifecycles(&self) -> MetaResult<Vec<InodeLifecycleRecord>> {
        RedbMetaEngine::list_inode_lifecycles(self)
    }

    fn get_attrs_batch(&self, inos: &[Ino]) -> MetaResult<Vec<InodeAttr>> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let overlay_chunks = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_chunks = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut attrs = Vec::with_capacity(inos.len());
        for &ino in inos {
            if tombstones
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                continue;
            }
            let current = current
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let base = base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let attr_overlay = Self::attr_differs_from_base(
                current.as_ref().map(|value| value.value()),
                base.as_ref().map(|value| value.value()),
            );
            let prefer_current = if attr_overlay {
                true
            } else {
                Self::regular_file_overlay_differs_from_base_in_opened_tables(
                    &overlay_chunks,
                    &base_chunks,
                    ino,
                )?
            };
            match Self::resolve_attr_from_opened_values(
                base.as_ref().map(|value| value.value()),
                current.as_ref().map(|value| value.value()),
                ino,
                prefer_current,
            ) {
                Ok(attr) => attrs.push(attr),
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }
        Ok(attrs)
    }

    fn set_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        Self::set_attr_in_txn(&txn, attr)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_attr(&self, ino: Ino) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
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
        let txn = self.begin_read_txn()?;
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
        .expect("generate format info")
    }

    #[test]
    fn ensure_inode_lifecycle_table_recovers_empty_table_with_current_version() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
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
                .flush_regular_file_overlay_slices(
                    file.ino,
                    &[(ChunkId(0), {
                        let mut slice = SliceRecord::new(
                            SliceId(0),
                            SliceOrder(0),
                            SliceRange::new(0, 4096).unwrap(),
                            PayloadRef::new_partial(777, 4096),
                        );
                        slice.payload_state = PayloadRefState::LocalOnly;
                        vec![slice]
                    })],
                )
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

    #[test]
    fn backfill_captures_uploaded_pending_overlay_when_recovery_row_is_missing() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = temp.path().join("pending-file-recovery-backfill.redb");
        let engine = RedbMetaEngine::open(&db_path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        engine
            .set_format_info(&test_format_info("pending-file-recovery-backfill"))
            .unwrap();
        let file = engine
            .create_file(ROOT_INO, b"recover-after-repair.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();
        engine
            .flush_regular_file_overlay_slices(
                file.ino,
                &[(ChunkId(0), {
                    let slice = SliceRecord::new(
                        SliceId(0),
                        SliceOrder(0),
                        SliceRange::new(0, 4096).unwrap(),
                        PayloadRef::new_partial(777_101, 4096),
                    );
                    vec![slice]
                })],
            )
            .unwrap();
        assert!(
            engine
                .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 777_101)
                .unwrap()
        );

        let txn = engine.begin_write_txn().unwrap();
        let mut recovery = txn.open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE).unwrap();
        recovery.remove(file.ino).unwrap();
        drop(recovery);
        txn.commit().unwrap();

        engine
            .backfill_missing_uploaded_regular_file_recoveries()
            .unwrap();
        let recoveries = engine.pending_uploaded_regular_file_recoveries().unwrap();
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].attr.ino, file.ino);
        assert_eq!(
            recoveries[0].dentries,
            Some(vec![(ROOT_INO, b"recover-after-repair.bin".to_vec())])
        );
        assert_eq!(recoveries[0].had_published_inode, Some(false));
        assert!(
            recoveries[0]
                .overlay
                .chunks
                .values()
                .flat_map(|chunk| chunk.slices.values())
                .any(|slice| slice.payload_state == PayloadRefState::Uploaded)
        );
    }

    #[test]
    fn recovery_record_decodes_rows_without_namespace_fields() {
        #[derive(serde::Serialize)]
        struct LegacyRecoveryRecord {
            attr: InodeAttr,
            overlay: crate::schema::RegularFileOverlayState,
        }

        let legacy = LegacyRecoveryRecord {
            attr: InodeAttr::new_file(42, ROOT_INO, 0, 0, 0o644),
            overlay: crate::schema::RegularFileOverlayState {
                chunks: BTreeMap::new(),
            },
        };
        let bytes = postcard::to_stdvec(&legacy).expect("encode legacy row");
        let decoded = decode_pending_regular_file_recovery(&bytes).expect("decode old row");

        assert_eq!(decoded.dentries, None);
        assert_eq!(decoded.had_published_inode, None);

        let current = PendingRegularFileRecoveryRecord {
            attr: legacy.attr,
            overlay: legacy.overlay,
            dentries: Some(vec![(ROOT_INO, b"recovered.bin".to_vec())]),
            had_published_inode: Some(false),
        };
        let bytes = encode_pending_regular_file_recovery(&current).expect("encode versioned row");
        assert!(bytes.starts_with(PENDING_REGULAR_FILE_RECOVERY_WIRE_MAGIC));
        let decoded = decode_pending_regular_file_recovery(&bytes).expect("decode versioned row");
        assert_eq!(decoded.attr, current.attr);
        assert_eq!(decoded.overlay.chunks, current.overlay.chunks);
        assert_eq!(decoded.dentries, current.dentries);
        assert_eq!(decoded.had_published_inode, current.had_published_inode);
    }

    #[test]
    fn recovery_overlay_keeps_only_uploaded_payload_slices() {
        let mut uploaded = SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            SliceRange::new(0, 4096).unwrap(),
            PayloadRef::new_partial(777_201, 4096),
        );
        uploaded.payload_state = PayloadRefState::Uploaded;
        let local_only = SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(4096, 8192).unwrap(),
            PayloadRef::new_partial(777_202, 4096),
        );
        let mut chunk = RegularFileChunkState::default();
        chunk.insert_slice(uploaded);
        chunk.insert_slice(local_only);

        let recovery = RedbMetaEngine::uploaded_regular_file_recovery_overlay(
            &BTreeMap::from([(ChunkId(0), chunk)]),
            None,
        );
        let payload_ids: Vec<_> = recovery
            .chunks
            .values()
            .flat_map(|chunk| chunk.slices.values())
            .filter_map(|slice| slice.payload.map(|payload| payload.object_id))
            .collect();

        assert_eq!(payload_ids, vec![777_201]);
    }

    #[test]
    fn sync_recovery_replaces_a_previous_inode_identity() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = temp
            .path()
            .join("pending-file-recovery-inode-identity.redb");
        let engine = RedbMetaEngine::open(&db_path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        engine
            .set_format_info(&test_format_info("pending-file-recovery-inode-identity"))
            .unwrap();
        let file = engine
            .create_file(ROOT_INO, b"reused-inode.bin", 1000, 1000, 0o644)
            .unwrap();

        let mut old_attr = engine.get_attr(file.ino).unwrap();
        old_attr.size = 4096;
        old_attr.content_epoch = 2;
        engine.set_attr(&old_attr).unwrap();
        engine
            .flush_regular_file_overlay_slices(
                file.ino,
                &[(ChunkId(0), {
                    vec![SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(0, 4096).unwrap(),
                        PayloadRef::new_partial(777_201, 4096),
                    )]
                })],
            )
            .unwrap();
        assert!(
            engine
                .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 2, 777_201)
                .unwrap()
        );

        let mut reused_attr = engine.get_attr(file.ino).unwrap();
        reused_attr.crtime += 1;
        reused_attr.content_epoch = 1;
        reused_attr.upload_state = UploadState::Uploading;
        let mut new_slice = SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, 4096).unwrap(),
            PayloadRef::new_partial(777_202, 4096),
        );
        new_slice.payload_state = PayloadRefState::Uploaded;
        let mut new_chunk = RegularFileChunkState::default();
        new_chunk.insert_slice(new_slice);
        let new_chunks = std::collections::BTreeMap::from([(ChunkId(0), new_chunk)]);

        let txn = engine.begin_immediate_write_txn().unwrap();
        RedbMetaEngine::set_inode_overlay_in_txn(&txn, &reused_attr).unwrap();
        let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let key = encode_regular_file_chunk_key(file.ino, ChunkId(0));
        overlay.remove(key.as_slice()).unwrap();
        drop(overlay);
        RedbMetaEngine::sync_pending_regular_file_recovery_in_txn(&txn, &reused_attr, &new_chunks)
            .unwrap();
        txn.commit().unwrap();

        let recoveries = engine.pending_uploaded_regular_file_recoveries().unwrap();
        assert_eq!(recoveries.len(), 1);
        assert_eq!(recoveries[0].attr.crtime, reused_attr.crtime);
        assert_eq!(recoveries[0].attr.content_epoch, reused_attr.content_epoch);
        let payload_ids: Vec<_> = recoveries[0]
            .overlay
            .chunks
            .values()
            .flat_map(|chunk| chunk.slices.values())
            .filter_map(|slice| slice.payload.map(|payload| payload.object_id))
            .collect();
        assert_eq!(payload_ids, vec![777_202]);

        assert_eq!(
            engine.get_referenced_object_ids().unwrap(),
            std::collections::HashSet::from([777_202])
        );
    }
}
