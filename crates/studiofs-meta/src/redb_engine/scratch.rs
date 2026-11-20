use super::*;

pub(super) fn storage_error(error: impl std::fmt::Display) -> MetaError {
    MetaError::Storage(error.to_string())
}

impl RedbMetaEngine {
    pub(super) fn insert_scratch_entry(
        &self,
        parent: Ino,
        name: &[u8],
        attr: &InodeAttr,
    ) -> MetaResult<()> {
        let key = encode_dentry_key(parent, name);
        let bytes = postcard::to_allocvec(attr).map_err(storage_error)?;
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            if dentries.get(key.as_slice())?.is_some() {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            }
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            if inodes.get(attr.ino)?.is_some() {
                return Err(MetaError::TargetChanged);
            }
            inodes.insert(attr.ino, bytes.as_slice())?;
            dentries.insert(key.as_slice(), attr.ino)?;
        }
        txn.commit()
    }

    pub(super) fn read_scratch_dentry(&self, parent: Ino, name: &[u8]) -> MetaResult<Option<Ino>> {
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        Ok(table
            .get(encode_dentry_key(parent, name).as_slice())?
            .map(|row| row.value()))
    }

    pub(super) fn read_scratch_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let row = table.get(ino)?.ok_or(MetaError::InodeNotFound(ino))?;
        postcard::from_bytes(row.value()).map_err(storage_error)
    }

    pub(super) fn replace_scratch_block(
        &self,
        ino: Ino,
        block: u32,
        object_id: u64,
        end: u64,
    ) -> MetaResult<Option<u64>> {
        let txn = self.begin_immediate_write_txn()?;
        let previous;
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr: InodeAttr = {
                let row = inodes.get(ino)?.ok_or(MetaError::InodeNotFound(ino))?;
                postcard::from_bytes(row.value()).map_err(storage_error)?
            };
            if attr.kind != crate::schema::FileType::RegularFile {
                return Err(MetaError::TargetChanged);
            }
            attr.size = attr.size.max(end);
            attr.touch_mtime();
            inodes.insert(
                ino,
                postcard::to_allocvec(&attr)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
            let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
            previous = overlay
                .insert((ino, block), object_id)?
                .map(|row| row.value());
        }
        txn.commit()?;
        Ok(previous)
    }

    pub(super) fn read_scratch_block(&self, ino: Ino, block: u32) -> MetaResult<Option<u64>> {
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        Ok(table.get((ino, block))?.map(|row| row.value()))
    }

    pub(super) fn write_scratch_xattr(
        &self,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        let txn = self.begin_immediate_write_txn()?;
        {
            if txn
                .open_table(SCRATCH_INODES)
                .map_err(storage_error)?
                .get(ino)?
                .is_none()
            {
                return Err(MetaError::InodeNotFound(ino));
            }
            txn.open_table(SCRATCH_XATTRS)
                .map_err(storage_error)?
                .insert(encode_xattr_key(ino, name).as_slice(), value)?;
        }
        txn.commit()
    }

    pub(super) fn read_scratch_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
        Ok(table
            .get(encode_xattr_key(ino, name).as_slice())?
            .map(|row| row.value().to_vec()))
    }
}
