use super::*;
use crate::schema::{SliceRecord, chunk_start_offset};

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

    pub(super) fn replace_scratch_overlay(
        &self,
        ino: Ino,
        chunks: &[(ChunkId, Vec<SliceRecord>)],
    ) -> MetaResult<Vec<u64>> {
        let txn = self.begin_immediate_write_txn()?;
        let mut displaced = HashSet::new();
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr: InodeAttr = {
                let row = inodes.get(ino)?.ok_or(MetaError::InodeNotFound(ino))?;
                postcard::from_bytes(row.value()).map_err(storage_error)?
            };
            if attr.kind != crate::schema::FileType::RegularFile {
                return Err(MetaError::TargetChanged);
            }
            let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
            for (chunk_id, incoming) in chunks {
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                let mut state: RegularFileChunkState = overlay
                    .get(key.as_slice())?
                    .map(|row| decode(row.value()))
                    .transpose()?
                    .unwrap_or_default();
                let mut previous_objects: HashSet<_> = state
                    .slices
                    .values()
                    .filter_map(|slice| slice.payload.map(|payload| payload.object_id))
                    .collect();
                for slice in incoming {
                    let mut slice = slice.clone();
                    slice.slice_id = state
                        .try_alloc_slice_id()
                        .ok_or_else(|| storage_error("scratch slice ID overflow"))?;
                    slice.order = state
                        .try_alloc_slice_order()
                        .ok_or_else(|| storage_error("scratch slice order overflow"))?;
                    let end = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE)
                        .checked_add(u64::from(slice.range.end))
                        .ok_or_else(|| storage_error("scratch slice exceeds file offset range"))?;
                    attr.size = attr.size.max(end);
                    previous_objects.extend(slice.payload.map(|payload| payload.object_id));
                    state.insert_slice(slice);
                }
                let compacted = Self::compact_regular_file_chunk_state(*chunk_id, &state)?;
                let state = compacted.as_ref().unwrap_or(&state);
                for payload in state.slices.values().filter_map(|slice| slice.payload) {
                    previous_objects.remove(&payload.object_id);
                }
                displaced.extend(previous_objects);
                overlay.insert(key.as_slice(), encode(state)?.as_slice())?;
            }
            // A packed object can remain live in a different chunk.
            if !displaced.is_empty() {
                for state in
                    Self::regular_file_chunk_states_for_inode_in_opened_table(&overlay, ino)?
                        .values()
                {
                    for payload in state.slices.values().filter_map(|slice| slice.payload) {
                        displaced.remove(&payload.object_id);
                    }
                }
            }
            attr.touch_mtime();
            attr.touch_ctime();
            inodes.insert(
                ino,
                postcard::to_allocvec(&attr)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
        }
        txn.commit()?;
        Ok(displaced.into_iter().collect())
    }

    pub(super) fn read_scratch_overlay(
        &self,
        ino: Ino,
        chunk: ChunkId,
    ) -> MetaResult<Option<RegularFileChunkState>> {
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        table
            .get(encode_regular_file_chunk_key(ino, chunk).as_slice())?
            .map(|row| decode(row.value()))
            .transpose()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{PayloadRef, SliceId, SliceOrder, SliceRange};

    fn slice(object_id: u64, start: u32, end: u32) -> SliceRecord {
        SliceRecord::new(
            SliceId(0),
            SliceOrder(0),
            SliceRange::new(start, end).unwrap(),
            PayloadRef::new_partial(object_id, end - start),
        )
    }

    #[test]
    fn scratch_overlay_roots_are_cache_live_and_displaced_on_overwrite() {
        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();
        let attr = InodeAttr::new_file(42, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"scene.tmp", &attr)
            .unwrap();

        let displaced = meta
            .commit_scratch_overlay(42, &[(ChunkId(0), vec![slice(91, 0, 8)])])
            .unwrap();
        assert!(displaced.is_empty());
        assert_eq!(meta.get_scratch_attr(42).unwrap().size, 8);
        assert!(
            meta.get_local_cache_referenced_object_ids()
                .unwrap()
                .contains(&91)
        );
        assert!(!meta.get_referenced_object_ids().unwrap().contains(&91));
        assert!(
            !meta
                .get_local_referenced_object_ids()
                .unwrap()
                .contains(&91)
        );

        let displaced = meta
            .commit_scratch_overlay(42, &[(ChunkId(0), vec![slice(92, 0, 8)])])
            .unwrap();
        assert_eq!(displaced, vec![91]);
        let cache_roots = meta.get_local_cache_referenced_object_ids().unwrap();
        assert!(!cache_roots.contains(&91) && cache_roots.contains(&92));
    }
}
