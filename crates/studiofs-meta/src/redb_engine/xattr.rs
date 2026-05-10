use super::*;

impl MetaXattr for RedbMetaEngine {
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        self.merged_xattr_value(&txn, ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        let current = self.get_xattr(ino, name)?;
        let txn = self.begin_crash_safe_write()?;
        {
            if flags & XATTR_CREATE_FLAG != 0 && current.is_some() {
                return Err(MetaError::EntryExists {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }
            if flags & XATTR_REPLACE_FLAG != 0 && current.is_none() {
                return Err(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).to_string(),
                });
            }

            Self::set_xattr_overlay_in_txn(&txn, ino, name, value)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetXattr {
                    ino,
                    name: name.to_vec(),
                    value: value.to_vec(),
                    flags,
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn remove_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        if self.get_xattr(ino, name)?.is_none() {
            return Err(MetaError::EntryNotFound {
                parent: ino,
                name: String::from_utf8_lossy(name).to_string(),
            });
        }
        let txn = self.begin_crash_safe_write()?;
        {
            Self::remove_xattr_overlay_in_txn(&txn, ino, name)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveXattr {
                    ino,
                    name: name.to_vec(),
                },
            )?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let mut names = std::collections::BTreeSet::<Vec<u8>>::new();

        for key in Self::keys_for_inode_in_table(&txn, BASE_XATTRS_TABLE, ino)? {
            if !Self::has_slice_marker(&txn, XATTR_TOMBSTONES_TABLE, key.as_slice())? {
                names.insert(xattr_name_from_key(&key).to_vec());
            }
        }
        for key in Self::keys_for_inode_in_table(&txn, XATTRS_TABLE, ino)? {
            names.insert(xattr_name_from_key(&key).to_vec());
        }

        Ok(names.into_iter().collect())
    }
}
