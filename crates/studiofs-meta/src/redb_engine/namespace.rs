use super::*;

impl RedbMetaEngine {
    fn remove_namespace_entry(
        &self,
        parent: Ino,
        name: &[u8],
        expected_ino: Option<Ino>,
    ) -> MetaResult<Option<DirEntryValue>> {
        let txn = self.begin_write_txn()?;
        let Some(entry) = Self::lookup_merged_in_write_txn(&txn, parent, name)? else {
            txn.abort().map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(None);
        };
        if expected_ino.is_some_and(|ino| ino != entry.ino) {
            txn.abort().map_err(|e| MetaError::Storage(e.to_string()))?;
            return Ok(None);
        }
        Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;
        Self::append_event_in_txn(
            &txn,
            &crate::events::MetaEvent::RemoveEntry {
                parent,
                name: name.to_vec(),
            },
        )?;
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(Some(entry))
    }

    /// Returns dentry keys whose local state differs from the last imported base view.
    pub fn local_overlay_dentries(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;

        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Self::collect_overlay_dentries_from_tables(&current, &base, &tombstones)
    }
}

impl MetaNamespace for RedbMetaEngine {
    fn lookup(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let (entry, is_overlay) =
            Self::lookup_merged_from_opened_tables(&base, &current, &tombstones, parent, name)?;
        let visible = match entry {
            Some(entry)
                if is_overlay
                    || entry.kind != FileType::RegularFile
                    || !Self::remote_namespace_visibility_filter_enabled_in_read_txn(&txn)? =>
            {
                Some(entry)
            }
            Some(entry) => {
                #[cfg(test)]
                super::tests::VISIBILITY_TABLE_OPENS.with(|count| count.set(count.get() + 5));
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
                Self::remote_namespace_entry_visible_from_opened_tables(
                    &base_inodes,
                    &current_inodes,
                    &inode_tombstones,
                    &inode_lifecycle,
                    &base_regular_file,
                    &entry,
                )?
                .then_some(entry)
            }
            None => None,
        };

        visible.ok_or_else(|| MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        let txn = self.begin_read_txn()?;
        let current = txn
            .open_table(DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let base = txn
            .open_table(BASE_DENTRIES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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
        let remote_filter_enabled =
            Self::remote_namespace_visibility_filter_enabled_in_read_txn(&txn)?;
        let overlay_dentries =
            Self::collect_overlay_dentries_from_tables(&current, &base, &tombstones)?;

        let merged = Self::readdir_merged_from_opened_tables(
            &base,
            &current,
            &tombstones,
            parent,
            &overlay_dentries,
        )?;
        let mut visible = Vec::with_capacity(merged.len());
        for entry in merged {
            let key = encode_dentry_key(parent, &entry.name);
            if !remote_filter_enabled
                || overlay_dentries.contains(&key)
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

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        let txn = self.begin_write_txn()?;
        if Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some() {
            txn.abort().map_err(|e| MetaError::Storage(e.to_string()))?;
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).into_owned(),
            });
        }

        {
            Self::set_dentry_overlay_in_txn(&txn, parent, name, entry)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::AddEntry {
                    parent,
                    name: name.to_vec(),
                    entry: entry.clone(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_entry(&self, parent: Ino, name: &[u8]) -> MetaResult<DirEntryValue> {
        self.remove_namespace_entry(parent, name, None)?
            .ok_or_else(|| MetaError::EntryNotFound {
                parent,
                name: String::from_utf8_lossy(name).into_owned(),
            })
    }

    fn remove_entry_if_matches(
        &self,
        parent: Ino,
        name: &[u8],
        expected_ino: Ino,
    ) -> MetaResult<Option<DirEntryValue>> {
        self.remove_namespace_entry(parent, name, Some(expected_ino))
    }
}
