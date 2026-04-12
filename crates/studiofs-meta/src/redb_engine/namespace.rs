use super::*;

impl RedbMetaEngine {
    pub fn local_overlay_dentries(&self) -> MetaResult<HashSet<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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

        Self::lookup_merged_from_opened_tables(
            &base,
            &current,
            &tombstones,
            parent,
            name,
            &overlay_dentries,
        )?
        .ok_or(MetaError::EntryNotFound {
            parent,
            name: String::from_utf8_lossy(name).to_string(),
        })
    }

    fn readdir(&self, parent: Ino) -> MetaResult<Vec<DirEntry>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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

        Self::readdir_merged_from_opened_tables(
            &base,
            &current,
            &tombstones,
            parent,
            &overlay_dentries,
        )
    }

    fn add_entry(&self, parent: Ino, name: &[u8], entry: &DirEntryValue) -> MetaResult<()> {
        if self.lookup(parent, name).is_ok() {
            return Err(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }

        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
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
        let entry_val = self.lookup(parent, name)?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        {
            Self::remove_dentry_overlay_in_txn(&txn, parent, name)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveEntry {
                    parent,
                    name: name.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(entry_val)
    }
}
