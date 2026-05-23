use super::*;

impl RedbMetaEngine {
    /// Enable or disable filtering that hides remote-only namespace rows locally.
    pub fn set_remote_namespace_visibility_filter(&self, enabled: bool) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::set_remote_namespace_visibility_filter_in_txn(&txn, enabled)?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remote_namespace_visibility_filter_enabled_in_read_txn(
        txn: &redb::ReadTransaction,
    ) -> MetaResult<bool> {
        let settings = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(settings
            .get(REMOTE_NAMESPACE_VISIBILITY_FILTER_SETTING)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    pub(super) fn remote_namespace_visibility_filter_enabled_in_write_txn(
        txn: &redb::WriteTransaction,
    ) -> MetaResult<bool> {
        let settings = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(settings
            .get(REMOTE_NAMESPACE_VISIBILITY_FILTER_SETTING)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    pub(super) fn set_remote_namespace_visibility_filter_in_txn(
        txn: &redb::WriteTransaction,
        enabled: bool,
    ) -> MetaResult<()> {
        let mut settings = txn
            .open_table(SETTINGS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if enabled {
            let enabled_value = [1_u8];
            settings
                .insert(
                    REMOTE_NAMESPACE_VISIBILITY_FILTER_SETTING,
                    enabled_value.as_slice(),
                )
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = settings.remove(REMOTE_NAMESPACE_VISIBILITY_FILTER_SETTING);
        }
        Ok(())
    }

    pub(super) fn regular_file_remote_visible_without_published_base(attr: &InodeAttr) -> bool {
        attr.size == 0 && attr.upload_state == UploadState::Updating
            || attr.committed_size > 0
            || attr.committed_content_epoch > 0
    }

    pub(super) fn ensure_dentry_by_inode_indexes(&self) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let current_version = {
            let settings = txn
                .open_table(SETTINGS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            settings
                .get(DENTRY_BY_INODE_INDEX_VERSION_SETTING)
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .map(|value| value.value().to_vec())
        };

        if current_version.as_deref() != Some(CURRENT_DENTRY_BY_INODE_INDEX_VERSION) {
            Self::rebuild_dentry_index_in_txn(&txn, DENTRIES_TABLE, DENTRIES_BY_INODE_TABLE)?;
            Self::rebuild_dentry_index_in_txn(
                &txn,
                BASE_DENTRIES_TABLE,
                BASE_DENTRIES_BY_INODE_TABLE,
            )?;
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

        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn inode_has_local_lifecycle_from_opened_table<T>(table: &T, ino: Ino) -> MetaResult<bool>
    where
        T: ReadableTable<u64, &'static [u8]>,
    {
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))
            .map(|value| value.is_some())
    }

    fn names_equal_case_insensitive(left: &[u8], right: &[u8], right_lower: &str) -> bool {
        if left.eq_ignore_ascii_case(right) {
            return true;
        }

        String::from_utf8_lossy(left).to_lowercase() == right_lower
    }

    pub(super) fn lookup_in_opened_dentry_table<T>(
        table: &T,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>>
    where
        T: ReadableTable<&'static [u8], &'static [u8]>,
    {
        let key = encode_dentry_key(parent, name);

        if let Ok(Some(entry)) = table.get(key.as_slice()) {
            return decode(entry.value()).map(Some);
        }

        let name_lower = String::from_utf8_lossy(name).to_lowercase();
        let prefix = dentry_prefix(parent);
        let range_end = prefix_successor(&prefix);
        let iter = match range_end.as_ref() {
            Some(end) => table.range(prefix.as_slice()..end.as_ref()),
            None => table.range(prefix.as_slice()..),
        }
        .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (k, v) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            let (decoded_parent, stored_name) = decode_dentry_key(key_bytes)?;
            if decoded_parent != parent {
                break;
            }
            if Self::names_equal_case_insensitive(stored_name, name, &name_lower) {
                return decode(v.value()).map(Some);
            }
        }

        Ok(None)
    }

    pub(super) fn readdir_from_opened_dentry_table<T>(
        table: &T,
        parent: Ino,
    ) -> MetaResult<Vec<DirEntry>>
    where
        T: ReadableTable<&'static [u8], &'static [u8]>,
    {
        let prefix = dentry_prefix(parent);
        let mut entries = Vec::new();
        let range_end = prefix_successor(&prefix);
        let iter = match range_end.as_ref() {
            Some(end) => table.range(prefix.as_slice()..end.as_ref()),
            None => table.range(prefix.as_slice()..),
        }
        .map_err(|e| MetaError::Storage(e.to_string()))?;

        for result in iter {
            let (key, value) = result.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value();
            let (decoded_parent, name) = decode_dentry_key(key_bytes)?;
            if decoded_parent != parent {
                break;
            }
            let entry_val: DirEntryValue = decode(value.value())?;
            entries.push(DirEntry {
                name: name.to_vec(),
                ino: entry_val.ino,
                kind: entry_val.kind,
            });
        }

        Ok(entries)
    }

    pub(super) fn collect_overlay_dentries_from_tables<TCurrent, TBase, TTombstones>(
        current: &TCurrent,
        base: &TBase,
        tombstones: &TTombstones,
    ) -> MetaResult<HashSet<Vec<u8>>>
    where
        TCurrent: ReadableTable<&'static [u8], &'static [u8]>,
        TBase: ReadableTable<&'static [u8], &'static [u8]>,
        TTombstones: ReadableTable<&'static [u8], u8>,
    {
        let mut overlay = HashSet::new();

        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = key.value().to_vec();
            match base
                .get(key_bytes.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_value) if base_value.value() == value.value() => {}
                _ => {
                    overlay.insert(key_bytes);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(key.value().to_vec());
        }

        Ok(overlay)
    }

    pub(super) fn lookup_merged_from_opened_tables<TBase, TCurrent, TTombstones>(
        base: &TBase,
        current: &TCurrent,
        tombstones: &TTombstones,
        parent: Ino,
        name: &[u8],
        overlay_dentries: &HashSet<Vec<u8>>,
    ) -> MetaResult<Option<DirEntryValue>>
    where
        TBase: ReadableTable<&'static [u8], &'static [u8]>,
        TCurrent: ReadableTable<&'static [u8], &'static [u8]>,
        TTombstones: ReadableTable<&'static [u8], u8>,
    {
        let key = encode_dentry_key(parent, name);
        if tombstones
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some()
        {
            return Ok(None);
        }

        if overlay_dentries.contains(&key) {
            return Self::lookup_in_opened_dentry_table(current, parent, name);
        }

        if let Some(entry) = Self::lookup_in_opened_dentry_table(base, parent, name)? {
            return Ok(Some(entry));
        }

        Self::lookup_in_opened_dentry_table(current, parent, name)
    }

    pub(super) fn regular_file_has_published_base_from_opened_table<T>(
        table: &T,
        ino: Ino,
    ) -> MetaResult<bool>
    where
        T: ReadableTable<&'static [u8], &'static [u8]>,
    {
        let prefix = regular_file_chunk_prefix(ino);
        for row in table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }

            let chunk_state: RegularFileChunkState = decode(value.value())?;
            if chunk_state.slices.values().any(|slice| {
                slice.payload_state == PayloadRefState::Published && slice.payload.is_some()
            }) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) fn remote_namespace_entry_visible_from_opened_tables<
        TBaseInodes,
        TCurrentInodes,
        TTombstones,
        TLifecycle,
        TBaseChunks,
    >(
        base_inodes: &TBaseInodes,
        current_inodes: &TCurrentInodes,
        inode_tombstones: &TTombstones,
        inode_lifecycle: &TLifecycle,
        base_regular_file: &TBaseChunks,
        entry: &DirEntryValue,
    ) -> MetaResult<bool>
    where
        TBaseInodes: ReadableTable<u64, &'static [u8]>,
        TCurrentInodes: ReadableTable<u64, &'static [u8]>,
        TTombstones: ReadableTable<u64, u8>,
        TLifecycle: ReadableTable<u64, &'static [u8]>,
        TBaseChunks: ReadableTable<&'static [u8], &'static [u8]>,
    {
        if entry.kind != FileType::RegularFile {
            return Ok(true);
        }

        let attr = match Self::resolve_attr_from_opened_tables(
            base_inodes,
            current_inodes,
            inode_tombstones,
            entry.ino,
            true,
        ) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Ok(false),
            Err(err) => return Err(err),
        };

        if Self::inode_has_local_lifecycle_from_opened_table(inode_lifecycle, entry.ino)? {
            return Ok(true);
        }

        let visible_in_remote_namespace =
            Self::regular_file_remote_visible_without_published_base(&attr)
                || Self::regular_file_has_published_base_from_opened_table(
                    base_regular_file,
                    entry.ino,
                )?;
        Ok(visible_in_remote_namespace)
    }

    pub(super) fn readdir_merged_from_opened_tables<TBase, TCurrent, TTombstones>(
        base: &TBase,
        current: &TCurrent,
        tombstones: &TTombstones,
        parent: Ino,
        overlay_dentries: &HashSet<Vec<u8>>,
    ) -> MetaResult<Vec<DirEntry>>
    where
        TBase: ReadableTable<&'static [u8], &'static [u8]>,
        TCurrent: ReadableTable<&'static [u8], &'static [u8]>,
        TTombstones: ReadableTable<&'static [u8], u8>,
    {
        let mut merged = std::collections::BTreeMap::<Vec<u8>, DirEntry>::new();
        for entry in Self::readdir_from_opened_dentry_table(base, parent)? {
            merged.insert(entry.name.clone(), entry);
        }

        let prefix = dentry_prefix(parent);
        let range_end = prefix_successor(&prefix);
        let rows = match range_end.as_ref() {
            Some(end) => tombstones.range(prefix.as_slice()..end.as_ref()),
            None => tombstones.range(prefix.as_slice()..),
        }
        .map_err(|e| MetaError::Storage(e.to_string()))?;
        for row in rows {
            let (key, _value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (decoded_parent, name) = decode_dentry_key(key.value())?;
            if decoded_parent != parent {
                break;
            }
            merged.remove(name);
        }

        for entry in Self::readdir_from_opened_dentry_table(current, parent)? {
            let key = encode_dentry_key(parent, &entry.name);
            if overlay_dentries.contains(&key) {
                merged.insert(entry.name.clone(), entry);
            }
        }

        let base_names: std::collections::HashSet<Vec<u8>> =
            Self::readdir_from_opened_dentry_table(base, parent)?
                .into_iter()
                .map(|entry| entry.name)
                .collect();
        for key in overlay_dentries {
            let (overlay_parent, overlay_name) = decode_dentry_key(key)?;
            if overlay_parent != parent {
                continue;
            }
            if !base_names.contains(overlay_name)
                && let Some(entry) =
                    Self::lookup_in_opened_dentry_table(current, parent, overlay_name)?
            {
                merged.insert(
                    overlay_name.to_vec(),
                    DirEntry {
                        name: overlay_name.to_vec(),
                        ino: entry.ino,
                        kind: entry.kind,
                    },
                );
            }
            if base_names.contains(overlay_name)
                && Self::lookup_in_opened_dentry_table(current, parent, overlay_name)?.is_none()
            {
                merged.remove(overlay_name);
            }
        }

        Ok(merged.into_values().collect())
    }

    pub(super) fn get_attr_from_opened_table<T>(
        table: &T,
        ino: Ino,
    ) -> MetaResult<Option<InodeAttr>>
    where
        T: ReadableTable<u64, &'static [u8]>,
    {
        table
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| decode(entry.value()))
            .transpose()
    }

    pub(super) fn collect_overlay_inode_attrs_from_tables<TCurrent, TBase, TTombstones>(
        current: &TCurrent,
        base: &TBase,
        tombstones: &TTombstones,
    ) -> MetaResult<HashSet<Ino>>
    where
        TCurrent: ReadableTable<u64, &'static [u8]>,
        TBase: ReadableTable<u64, &'static [u8]>,
        TTombstones: ReadableTable<u64, u8>,
    {
        let mut overlay = HashSet::new();

        for row in current
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, current_bytes) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let ino = ino.value();
            match base
                .get(ino)
                .map_err(|e| MetaError::Storage(e.to_string()))?
            {
                Some(base_bytes) if base_bytes.value() == current_bytes.value() => {}
                _ => {
                    overlay.insert(ino);
                }
            }
        }

        for row in tombstones
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (ino, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            overlay.insert(ino.value());
        }

        Ok(overlay)
    }

    pub(super) fn resolve_attr_from_opened_tables<TBase, TCurrent, TTombstones>(
        base: &TBase,
        current: &TCurrent,
        tombstones: &TTombstones,
        ino: Ino,
        prefer_current: bool,
    ) -> MetaResult<InodeAttr>
    where
        TBase: ReadableTable<u64, &'static [u8]>,
        TCurrent: ReadableTable<u64, &'static [u8]>,
        TTombstones: ReadableTable<u64, u8>,
    {
        if tombstones
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some()
        {
            return Err(MetaError::InodeNotFound(ino));
        }

        let load_current = || Self::get_attr_from_opened_table(current, ino);
        let load_base = || Self::get_attr_from_opened_table(base, ino);

        if prefer_current {
            if let Some(attr) = load_current()? {
                return Ok(attr);
            }
            if let Some(attr) = load_base()? {
                return Ok(attr);
            }
        } else {
            if let Some(attr) = load_base()? {
                return Ok(attr);
            }
            if let Some(attr) = load_current()? {
                return Ok(attr);
            }
        }

        Err(MetaError::InodeNotFound(ino))
    }

    pub(super) fn lookup_in_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::lookup_in_opened_dentry_table(&table, parent, name)
    }

    pub(super) fn get_attr_from_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<u64, &[u8]>,
        ino: Ino,
    ) -> MetaResult<Option<InodeAttr>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::get_attr_from_opened_table(&table, ino)
    }

    pub(super) fn append_event_in_txn(
        txn: &redb::WriteTransaction,
        event: &crate::events::MetaEvent,
    ) -> MetaResult<()> {
        let mut counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Get the next sequence ID for the event log
        let seq_id = match counters
            .get("nextEventSeq")
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 1,
        };
        counters
            .insert("nextEventSeq", seq_id + 1)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        // Encode and insert the event
        let mut events = txn
            .open_table(EVENT_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let data = encode(event)?;
        events
            .insert(seq_id, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        Ok(())
    }

    pub(super) fn lookup_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
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
        let key = encode_dentry_key(parent, name);
        let entry = Self::lookup_merged_from_opened_tables(
            &base,
            &current,
            &tombstones,
            parent,
            name,
            &overlay_dentries,
        )?;
        if !Self::remote_namespace_visibility_filter_enabled_in_write_txn(txn)? {
            return Ok(entry);
        }
        let current_inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_inodes = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let inode_tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let inode_lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_regular_file = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(match entry {
            Some(entry)
                if overlay_dentries.contains(&key)
                    || Self::remote_namespace_entry_visible_from_opened_tables(
                        &base_inodes,
                        &current_inodes,
                        &inode_tombstones,
                        &inode_lifecycle,
                        &base_regular_file,
                        &entry,
                    )? =>
            {
                Some(entry)
            }
            _ => None,
        })
    }

    pub(super) fn get_attr_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
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

        Self::resolve_attr_from_opened_tables(&base, &current, &tombstones, ino, true)
    }

    pub(super) fn readdir_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
    ) -> MetaResult<Vec<DirEntry>> {
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
        let merged = Self::readdir_merged_from_opened_tables(
            &base,
            &current,
            &tombstones,
            parent,
            &overlay_dentries,
        )?;
        if !Self::remote_namespace_visibility_filter_enabled_in_write_txn(txn)? {
            return Ok(merged);
        }
        let current_inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_inodes = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let inode_tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let inode_lifecycle = txn
            .open_table(INODE_LIFECYCLE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base_regular_file = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut visible = Vec::with_capacity(merged.len());
        for entry in merged {
            let key = encode_dentry_key(parent, &entry.name);
            if overlay_dentries.contains(&key)
                || Self::remote_namespace_entry_visible_from_opened_tables(
                    &base_inodes,
                    &current_inodes,
                    &inode_tombstones,
                    &inode_lifecycle,
                    &base_regular_file,
                    &DirEntryValue {
                        ino: entry.ino,
                        kind: entry.kind,
                    },
                )?
            {
                visible.push(entry);
            }
        }
        Ok(visible)
    }

    pub(super) fn has_entries_merged_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
    ) -> MetaResult<bool> {
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let prefix = dentry_prefix(parent);

        for row in current
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            if tombstones
                .get(key.value())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_none()
            {
                return Ok(true);
            }
        }

        for row in base
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, _) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            if tombstones
                .get(key.value())
                .map_err(|e| MetaError::Storage(e.to_string()))?
                .is_some()
            {
                continue;
            }
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) fn get_bytes_from_table_u64(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<u64, &[u8]>,
        key: u64,
    ) -> MetaResult<Option<Vec<u8>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| entry.value().to_vec()))
    }

    pub(super) fn get_bytes_from_table_slice(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        key: &[u8],
    ) -> MetaResult<Option<Vec<u8>>> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .map(|entry| entry.value().to_vec()))
    }

    pub(super) fn has_u64_marker(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<u64, u8>,
        key: u64,
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    pub(super) fn has_slice_marker(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], u8>,
        key: &[u8],
    ) -> MetaResult<bool> {
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(table
            .get(key)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some())
    }

    pub(super) fn keys_for_inode_in_table(
        txn: &redb::ReadTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = xattr_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut keys = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            keys.push(key_bytes.to_vec());
        }
        Ok(keys)
    }

    pub(super) fn keys_for_inode_in_write_table(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = xattr_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut keys = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            keys.push(key_bytes.to_vec());
        }
        Ok(keys)
    }

    pub(super) fn dentry_keys_for_inode_in_write_index(
        txn: &redb::WriteTransaction,
        table_def: TableDefinition<&[u8], u8>,
        ino: Ino,
    ) -> MetaResult<Vec<Vec<u8>>> {
        let prefix = dentry_by_inode_prefix(ino);
        let table = txn
            .open_table(table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let range = table
            .range(prefix.as_ref()..)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut keys = Vec::new();
        for item in range {
            let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
            let key_bytes = k.value();
            if !key_bytes.starts_with(&prefix) {
                break;
            }
            let (_ino, parent, name) = decode_dentry_by_inode_key(key_bytes)?;
            keys.push(encode_dentry_key(parent, name));
        }
        Ok(keys)
    }

    pub(super) fn merged_symlink_target(
        &self,
        txn: &redb::ReadTransaction,
        ino: Ino,
    ) -> MetaResult<Option<Vec<u8>>> {
        if Self::has_u64_marker(txn, SYMLINK_TOMBSTONES_TABLE, ino)? {
            return Ok(None);
        }
        if let Some(target) = Self::get_bytes_from_table_u64(txn, SYMLINKS_TABLE, ino)? {
            return Ok(Some(target));
        }
        Self::get_bytes_from_table_u64(txn, BASE_SYMLINKS_TABLE, ino)
    }

    pub(super) fn merged_xattr_value(
        &self,
        txn: &redb::ReadTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<Option<Vec<u8>>> {
        let key = encode_xattr_key(ino, name);
        if Self::has_slice_marker(txn, XATTR_TOMBSTONES_TABLE, key.as_slice())? {
            return Ok(None);
        }
        if let Some(value) = Self::get_bytes_from_table_slice(txn, XATTRS_TABLE, key.as_slice())? {
            return Ok(Some(decode_xattr_value_from_storage_owned(name, value)));
        }
        Ok(
            Self::get_bytes_from_table_slice(txn, BASE_XATTRS_TABLE, key.as_slice())?
                .map(|value| decode_xattr_value_from_storage_owned(name, value)),
        )
    }

    pub(super) fn set_xattr_value_in_table_in_txn(
        txn: &redb::WriteTransaction,
        table: TableDefinition<&[u8], &[u8]>,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let stored = encode_xattr_value_for_storage(name, value);
        let mut xattrs = txn
            .open_table(table)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        xattrs
            .insert(key.as_slice(), stored.as_ref())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn set_inode_overlay_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(attr.ino);
        drop(tombstones);

        let data = encode(attr)?;
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        inodes
            .insert(attr.ino, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_inode_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = inodes.remove(ino);
        drop(inodes);

        let has_base = txn
            .open_table(BASE_INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(ino, 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(ino);
        }
        Ok(())
    }

    pub(super) fn set_dentry_overlay_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            DENTRIES_TABLE,
            DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;
        Self::insert_dentry_index_in_txn(txn, DENTRIES_BY_INODE_TABLE, parent, name, entry)?;

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        let data = encode(entry)?;
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        dentries
            .insert(key.as_slice(), data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_dentry_overlay_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            DENTRIES_TABLE,
            DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;

        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentries.remove(key.as_slice());
        drop(dentries);

        let has_base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(key.as_slice());
        }
        Ok(())
    }

    pub(super) fn set_inode_mirror_in_txn(
        txn: &redb::WriteTransaction,
        attr: &InodeAttr,
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(attr.ino);
        drop(tombstones);

        let data = encode(attr)?;
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        inodes
            .insert(attr.ino, data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_inode_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut inodes = txn
            .open_table(INODES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = inodes.remove(ino);
        drop(inodes);

        let mut tombstones = txn
            .open_table(INODE_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        Ok(())
    }

    pub(super) fn set_dentry_mirror_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            DENTRIES_TABLE,
            DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;
        Self::insert_dentry_index_in_txn(txn, DENTRIES_BY_INODE_TABLE, parent, name, entry)?;

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        let data = encode(entry)?;
        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        dentries
            .insert(key.as_slice(), data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_dentry_mirror_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            DENTRIES_TABLE,
            DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;

        let mut dentries = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentries.remove(key.as_slice());
        drop(dentries);

        let mut tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        Ok(())
    }

    pub(super) fn set_base_dentry_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            BASE_DENTRIES_TABLE,
            BASE_DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;
        Self::insert_dentry_index_in_txn(txn, BASE_DENTRIES_BY_INODE_TABLE, parent, name, entry)?;

        let data = encode(entry)?;
        let mut dentries = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        dentries
            .insert(key.as_slice(), data.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_base_dentry_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        Self::remove_existing_dentry_index_in_txn(
            txn,
            BASE_DENTRIES_TABLE,
            BASE_DENTRIES_BY_INODE_TABLE,
            parent,
            name,
        )?;
        let mut dentries = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = dentries.remove(key.as_slice());
        Ok(())
    }

    pub(super) fn rebuild_dentry_index_in_txn(
        txn: &redb::WriteTransaction,
        dentry_table_def: TableDefinition<&[u8], &[u8]>,
        index_table_def: TableDefinition<&[u8], u8>,
    ) -> MetaResult<()> {
        Self::clear_table_in_txn(txn, index_table_def)?;
        let dentries = txn
            .open_table(dentry_table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut index = txn
            .open_table(index_table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        for row in dentries
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (key, value) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let (parent, name) = decode_dentry_key(key.value())?;
            let entry: DirEntryValue = decode(value.value())?;
            let index_key = encode_dentry_by_inode_key(entry.ino, parent, name);
            index
                .insert(index_key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn insert_dentry_index_in_txn(
        txn: &redb::WriteTransaction,
        index_table_def: TableDefinition<&[u8], u8>,
        parent: Ino,
        name: &[u8],
        entry: &DirEntryValue,
    ) -> MetaResult<()> {
        let index_key = encode_dentry_by_inode_key(entry.ino, parent, name);
        let mut index = txn
            .open_table(index_table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        index
            .insert(index_key.as_slice(), 1)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_existing_dentry_index_in_txn(
        txn: &redb::WriteTransaction,
        dentry_table_def: TableDefinition<&[u8], &[u8]>,
        index_table_def: TableDefinition<&[u8], u8>,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let Some(entry) = Self::lookup_in_write_table(txn, dentry_table_def, parent, name)? else {
            return Ok(());
        };
        let index_key = encode_dentry_by_inode_key(entry.ino, parent, name);
        let mut index = txn
            .open_table(index_table_def)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = index.remove(index_key.as_slice());
        Ok(())
    }

    pub(super) fn set_symlink_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        target: &[u8],
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        drop(tombstones);

        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        symlinks
            .insert(ino, target)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_symlink_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = symlinks.remove(ino);
        drop(symlinks);

        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        Ok(())
    }

    pub(super) fn set_xattr_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        Self::set_xattr_value_in_table_in_txn(txn, XATTRS_TABLE, ino, name, value)?;
        Ok(())
    }

    pub(super) fn remove_xattr_mirror_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = xattrs.remove(key.as_slice());
        drop(xattrs);

        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        Ok(())
    }

    pub(super) fn set_symlink_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        target: &[u8],
    ) -> MetaResult<()> {
        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(ino);
        drop(tombstones);

        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        symlinks
            .insert(ino, target)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_symlink_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        let mut symlinks = txn
            .open_table(SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = symlinks.remove(ino);
        drop(symlinks);

        let has_base = txn
            .open_table(BASE_SYMLINKS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(ino)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(SYMLINK_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(ino, 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(ino);
        }
        Ok(())
    }

    pub(super) fn set_xattr_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = tombstones.remove(key.as_slice());
        drop(tombstones);

        Self::set_xattr_value_in_table_in_txn(txn, XATTRS_TABLE, ino, name, value)?;
        Ok(())
    }

    pub(super) fn remove_xattr_overlay_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
        name: &[u8],
    ) -> MetaResult<()> {
        let key = encode_xattr_key(ino, name);
        let mut xattrs = txn
            .open_table(XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let _ = xattrs.remove(key.as_slice());
        drop(xattrs);

        let has_base = txn
            .open_table(BASE_XATTRS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .get(key.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?
            .is_some();

        let mut tombstones = txn
            .open_table(XATTR_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        if has_base {
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        } else {
            let _ = tombstones.remove(key.as_slice());
        }
        Ok(())
    }

    pub(super) fn next_undo_op_id_in_txn(txn: &redb::WriteTransaction) -> MetaResult<u64> {
        let mut counters = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = match counters
            .get(NEXT_UNDO_OP_ID_COUNTER)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 1,
        };
        let next = current.saturating_add(1);
        counters
            .insert(NEXT_UNDO_OP_ID_COUNTER, next)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(current)
    }

    pub(super) fn append_undo_log_in_txn(
        txn: &redb::WriteTransaction,
        record: &UndoLogRecord,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let encoded = encode(record)?;
        table
            .insert(record.op_id, encoded.as_slice())
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn remove_undo_logs_for_in_txn(
        txn: &redb::WriteTransaction,
        kind: UndoLogKind,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::set_undo_log_phase_for_in_txn(txn, kind, ino, UndoLogPhase::Completed)?;
        Ok(())
    }

    pub(super) fn set_undo_log_phase_for(
        &self,
        kind: UndoLogKind,
        ino: Ino,
        phase: UndoLogPhase,
    ) -> MetaResult<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::set_undo_log_phase_for_in_txn(&txn, kind, ino, phase)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    pub(super) fn set_undo_log_phase_for_in_txn(
        txn: &redb::WriteTransaction,
        kind: UndoLogKind,
        ino: Ino,
        phase: UndoLogPhase,
    ) -> MetaResult<()> {
        let mut table = txn
            .open_table(UNDO_LOG_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut updates: Vec<(u64, UndoLogRecord)> = Vec::new();
        for row in table
            .iter()
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            let (k, v) = row.map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut record: UndoLogRecord = decode(v.value())?;
            if record.kind == kind && record.ino == ino {
                record.phase = phase;
                updates.push((k.value(), record));
            }
        }
        for (key, record) in updates {
            let encoded = encode(&record)?;
            table
                .insert(key, encoded.as_slice())
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn update_counter_in_txn<F>(
        txn: &redb::WriteTransaction,
        name: &str,
        update: F,
    ) -> MetaResult<u64>
    where
        F: FnOnce(u64) -> u64,
    {
        let mut table = txn
            .open_table(COUNTERS_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let current = match table
            .get(name)
            .map_err(|e| MetaError::Storage(e.to_string()))?
        {
            Some(v) => v.value(),
            None => 0,
        };
        let new_val = update(current);
        table
            .insert(name, new_val)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(new_val)
    }

    pub(super) fn clear_inode_overlay_aux_in_txn_with_options(
        txn: &redb::WriteTransaction,
        ino: Ino,
        clear_regular_file_graph: bool,
    ) -> MetaResult<()> {
        Self::remove_symlink_overlay_in_txn(txn, ino)?;

        let current_keys = {
            let txn_read = txn
                .open_table(XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = xattr_prefix(ino);
            let range = txn_read
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        for key in current_keys {
            let name = xattr_name_from_key(&key).to_vec();
            Self::remove_xattr_overlay_in_txn(txn, ino, &name)?;
        }

        if clear_regular_file_graph {
            Self::clear_regular_file_graph_in_txn(txn, ino)?;
        }

        let base_keys = {
            let txn_read = txn
                .open_table(BASE_XATTRS_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = xattr_prefix(ino);
            let range = txn_read
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        for key in base_keys {
            let mut tombstones = txn
                .open_table(XATTR_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            tombstones
                .insert(key.as_slice(), 1)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
        }

        let acl_keys = {
            let table = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = acl_rule_prefix(ino);
            let range = table
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        if !acl_keys.is_empty() {
            let mut table = txn
                .open_table(ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for key in &acl_keys {
                let _ = table.remove(key.as_slice());
            }
            drop(table);
        }

        let base_acl_keys = {
            let table = txn
                .open_table(BASE_ACL_RULES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let prefix = acl_rule_prefix(ino);
            let range = table
                .range(prefix.as_ref()..)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let mut keys = Vec::new();
            for item in range {
                let (k, _v) = item.map_err(|e| MetaError::Storage(e.to_string()))?;
                let key_bytes = k.value();
                if !key_bytes.starts_with(&prefix) {
                    break;
                }
                keys.push(key_bytes.to_vec());
            }
            keys
        };
        if !base_acl_keys.is_empty() {
            let mut tombstones = txn
                .open_table(ACL_RULE_TOMBSTONES_TABLE)
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            for key in base_acl_keys {
                tombstones
                    .insert(key.as_slice(), 1)
                    .map_err(|e| MetaError::Storage(e.to_string()))?;
            }
        }

        Ok(())
    }

    pub(super) fn clear_inode_overlay_aux_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<()> {
        Self::clear_inode_overlay_aux_in_txn_with_options(txn, ino, true)
    }
}
