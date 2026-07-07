use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotImportMode {
    Local,
    RemoteReplace,
    RemotePreserveLocalState,
}

impl RedbMetaEngine {
    /// Encode a [`RedbSnapshot`] into the canonical persisted snapshot payload format.
    pub fn encode_snapshot_payload(snapshot: &RedbSnapshot) -> MetaResult<Vec<u8>> {
        encode(snapshot)
    }

    /// Decode a persisted snapshot payload, including legacy JSON-encoded snapshots.
    pub fn decode_snapshot_payload(payload: &[u8]) -> MetaResult<RedbSnapshot> {
        match decode(payload) {
            Ok(snapshot) => Ok(snapshot),
            Err(MetaError::Serialization(_)) => serde_json::from_slice::<RedbSnapshot>(payload)
                .map_err(|e| MetaError::Serialization(e.to_string())),
            Err(err) => Err(err),
        }
    }

    /// Export the current merged redb state into a serializable snapshot.
    pub fn export_snapshot(&self) -> MetaResult<RedbSnapshot> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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
        {
            let table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for row in table
                .iter()
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
                snapshot
                    .snapshots
                    .push((k.value().to_string(), v.value().to_vec()));
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

    /// Import a remote snapshot while preserving this client's local overlay and replay log.
    pub fn import_remote_snapshot_preserving_local_state(
        &self,
        snapshot: &RedbSnapshot,
    ) -> MetaResult<()> {
        self.import_snapshot_with_mode(snapshot, SnapshotImportMode::RemotePreserveLocalState)
    }

    fn import_snapshot_with_mode(
        &self,
        snapshot: &RedbSnapshot,
        mode: SnapshotImportMode,
    ) -> MetaResult<()> {
        let enable_remote_namespace_filter = mode != SnapshotImportMode::Local;
        let preserve_local_state = mode == SnapshotImportMode::RemotePreserveLocalState;
        let (preserved_inodes, preserved_dentries) = if preserve_local_state {
            let txn = self
                .db
                .begin_read()
                .map_err(|e| MetaError::Storage(e.to_string()))?;
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
                .retain(|_, _| preserve_local_state)
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
            if !preserve_local_state {
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
        {
            let mut table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for (k, v) in &snapshot.snapshots {
                table
                    .insert(k.as_str(), v.as_slice())
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }
        Self::rebuild_inode_lifecycle_table_in_txn(&txn)?;

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn collect_snapshot_pinned_object_ids(&self, root_ino: Ino) -> MetaResult<Vec<u64>> {
        let mut pinned = HashSet::new();
        let mut stack = vec![root_ino];
        let mut visited = HashSet::new();

        while let Some(ino) = stack.pop() {
            if !visited.insert(ino) {
                continue;
            }
            let attr = self.get_attr(ino)?;
            match attr.kind {
                FileType::RegularFile => {
                    // Snapshots pin the visible file contents, so resolve the
                    // overlay/base graph instead of synthesizing single-payload rows.
                    for span in self.lookup_regular_file_slices(ino, 0, attr.size)? {
                        if let Some(block) = span.payload {
                            pinned.insert(block.object_id);
                        }
                    }
                }
                FileType::Directory => {
                    for entry in self.readdir(ino)? {
                        stack.push(entry.ino);
                    }
                }
                FileType::Symlink => {}
            }
        }

        let mut out: Vec<u64> = pinned.into_iter().collect();
        out.sort_unstable();
        Ok(out)
    }
}

impl MetaSnapshots for RedbMetaEngine {
    fn create_snapshot(&self, root_ino: Ino) -> MetaResult<MetadataSnapshot> {
        let root_attr = self.get_attr(root_ino)?;
        if root_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(root_ino));
        }

        let pinned_object_ids = self.collect_snapshot_pinned_object_ids(root_ino)?;
        let snapshot_payload = Self::encode_snapshot_payload(&self.export_snapshot()?)?;

        let snapshot = MetadataSnapshot {
            id: uuid::Uuid::new_v4().to_string(),
            root_ino,
            created_at: InodeAttr::now_nanos(),
            state: SnapshotState::Pending,
            pinned_object_ids,
            snapshot_payload,
        };

        let bytes = encode(&snapshot)?;
        let txn = self.begin_write_txn()?;
        {
            let mut table = txn
                .open_table(SNAPSHOTS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            table
                .insert(snapshot.id.as_str(), bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::CreateSnapshot {
                    snapshot: snapshot.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(snapshot)
    }

    fn list_snapshots(&self) -> MetaResult<Vec<MetadataSnapshot>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut snapshots = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (_id, raw) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let snapshot: MetadataSnapshot = decode(raw.value())?;
            snapshots.push(snapshot);
        }
        snapshots.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(snapshots)
    }

    fn get_snapshot(&self, snapshot_id: &str) -> MetaResult<Option<MetadataSnapshot>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let table = txn
            .open_table(SNAPSHOTS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let Some(raw) = table
            .get(snapshot_id)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        else {
            return Ok(None);
        };
        let snapshot: MetadataSnapshot = decode(raw.value())?;
        Ok(Some(snapshot))
    }
}
