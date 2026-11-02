use super::*;

impl RedbMetaEngine {
    /// POSIX advances ctime on every xattr mutation; journal it like other attr-only ctime touches.
    fn touch_xattr_ctime_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
        let mut attr = Self::get_attr_merged_in_write_txn(txn, ino)?;
        attr.touch_ctime();
        Self::set_inode_in_txn(txn, &attr)?;
        Self::append_event_in_txn(txn, &crate::events::MetaEvent::SetAttr { attr })
    }
}

impl MetaXattr for RedbMetaEngine {
    fn get_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
        self.merged_xattr_value(&txn, ino, name)
    }

    fn get_xattr_size(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<usize>> {
        let txn = self.begin_read_txn()?;
        self.merged_xattr_value_size(&txn, ino, name)
    }

    fn set_xattr(&self, ino: Ino, name: &[u8], value: &[u8], flags: u32) -> MetaResult<()> {
        let current = self.get_xattr(ino, name)?;
        let txn = self.begin_write_txn()?;
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

            Self::set_xattr_in_txn(&txn, ino, name, value)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::SetXattr {
                    ino,
                    name: name.to_vec(),
                    value: value.to_vec(),
                    flags,
                },
            )?;
            Self::touch_xattr_ctime_in_txn(&txn, ino)?;
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
        let txn = self.begin_write_txn()?;
        {
            Self::remove_xattr_overlay_in_txn(&txn, ino, name)?;

            Self::append_event_in_txn(
                &txn,
                &crate::events::MetaEvent::RemoveXattr {
                    ino,
                    name: name.to_vec(),
                },
            )?;
            Self::touch_xattr_ctime_in_txn(&txn, ino)?;
        }
        txn.commit()
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        Ok(())
    }

    fn list_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
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
