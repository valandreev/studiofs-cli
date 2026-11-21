use super::fs_ops::{NamespaceDeleteTxn, current_unix_seconds};
use super::*;
use crate::schema::{
    SliceId, SliceOrder, SliceRange, SliceRecord, chunk_id_from_offset, chunk_start_offset,
    merge_adjacent_spans, resolve_chunk_slice_spans,
};

pub(super) fn storage_error(error: impl std::fmt::Display) -> MetaError {
    MetaError::Storage(error.to_string())
}

/// In-memory index of scratch inodes and per-parent scratch child counts.
///
/// Keeps tracked foreground lookups, readdir, getattr, and rmdir checks at O(1)
/// with zero redb reads when no scratch entries are present in the target scope.
#[derive(Debug, Default)]
pub(super) struct ScratchIndex {
    inodes: HashSet<Ino>,
    parent_child_counts: HashMap<Ino, usize>,
}

impl ScratchIndex {
    fn contains_inode(&self, ino: Ino) -> bool {
        self.inodes.contains(&ino)
    }

    fn parent_has_children(&self, parent: Ino) -> bool {
        self.parent_child_counts.get(&parent).copied().unwrap_or(0) > 0
    }

    fn note_insert(&mut self, parent: Ino, ino: Ino) {
        self.inodes.insert(ino);
        *self.parent_child_counts.entry(parent).or_insert(0) += 1;
    }

    fn note_increment_parent(&mut self, parent: Ino) {
        *self.parent_child_counts.entry(parent).or_insert(0) += 1;
    }

    fn note_remove_dentry(&mut self, parent: Ino) {
        if let Some(count) = self.parent_child_counts.get_mut(&parent) {
            if *count <= 1 {
                self.parent_child_counts.remove(&parent);
            } else {
                *count -= 1;
            }
        }
    }

    fn note_remove_inode(&mut self, ino: Ino) {
        self.inodes.remove(&ino);
    }

    fn load_from_read_txn(txn: &redb::ReadTransaction) -> MetaResult<Self> {
        let mut index = Self::default();
        match txn.open_table(SCRATCH_INODES) {
            Ok(inodes) => {
                for row in inodes.iter().map_err(storage_error)? {
                    let (ino, _) = row.map_err(storage_error)?;
                    index.inodes.insert(ino.value());
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(storage_error(error)),
        }
        match txn.open_table(SCRATCH_DENTRIES) {
            Ok(dentries) => {
                for row in dentries.iter().map_err(storage_error)? {
                    let (key, _) = row.map_err(storage_error)?;
                    let (parent, _) = decode_dentry_key(key.value())?;
                    *index.parent_child_counts.entry(parent).or_insert(0) += 1;
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(error) => return Err(storage_error(error)),
        }
        Ok(index)
    }
}

fn find_scratch_dentry_in_table<T>(
    table: &T,
    parent: Ino,
    name: &[u8],
) -> MetaResult<Option<(Vec<u8>, Ino)>>
where
    T: ReadableTable<&'static [u8], u64>,
{
    let exact_key = encode_dentry_key(parent, name);
    if let Some(row) = table.get(exact_key.as_slice()).map_err(storage_error)? {
        return Ok(Some((name.to_vec(), row.value())));
    }

    let prefix = dentry_prefix(parent);
    let range_end = prefix_successor(&prefix);
    let iter = match range_end.as_ref() {
        Some(end) => table.range(prefix.as_slice()..end.as_ref()),
        None => table.range(prefix.as_slice()..),
    }
    .map_err(storage_error)?;

    let mut target_lower = None;
    for row in iter {
        let (key, value) = row.map_err(storage_error)?;
        let (decoded_parent, stored_name) = decode_dentry_key(key.value())?;
        if decoded_parent != parent {
            break;
        }
        if RedbMetaEngine::names_equal_case_insensitive(stored_name, name, &mut target_lower) {
            return Ok(Some((stored_name.to_vec(), value.value())));
        }
    }
    Ok(None)
}

fn read_scratch_attr_from_table<T>(table: &T, ino: Ino) -> MetaResult<Option<InodeAttr>>
where
    T: ReadableTable<u64, &'static [u8]>,
{
    table
        .get(ino)
        .map_err(storage_error)?
        .map(|row| postcard::from_bytes(row.value()).map_err(storage_error))
        .transpose()
}

fn remove_scratch_conflict_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<()> {
    let mut conflicts = txn.open_table(SCRATCH_CONFLICTS).map_err(storage_error)?;
    let _ = conflicts.remove(ino).map_err(storage_error)?;
    Ok(())
}

fn remove_scratch_inode_aux_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<Vec<u64>> {
    remove_scratch_conflict_in_txn(txn, ino)?;

    // Symlink target
    {
        let mut symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
        let _ = symlinks.remove(ino).map_err(storage_error)?;
    }

    // Extended attributes
    {
        let mut xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
        let prefix = xattr_prefix(ino);
        let range_end = prefix_successor(&prefix);
        let keys: Vec<Vec<u8>> = {
            let iter = match range_end.as_ref() {
                Some(end) => xattrs.range(prefix.as_slice()..end.as_ref()),
                None => xattrs.range(prefix.as_slice()..),
            }
            .map_err(storage_error)?;
            let mut collected = Vec::new();
            for row in iter {
                let (key, _) = row.map_err(storage_error)?;
                if !key.value().starts_with(prefix.as_ref()) {
                    break;
                }
                collected.push(key.value().to_vec());
            }
            collected
        };
        for key in keys {
            let _ = xattrs.remove(key.as_slice()).map_err(storage_error)?;
        }
    }

    // Overlay chunks and payload object IDs
    let mut freed_objects = HashSet::new();
    {
        let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        let prefix = regular_file_chunk_prefix(ino);
        let range_end = prefix_successor(&prefix);
        let keys: Vec<Vec<u8>> = {
            let iter = match range_end.as_ref() {
                Some(end) => overlay.range(prefix.as_slice()..end.as_ref()),
                None => overlay.range(prefix.as_slice()..),
            }
            .map_err(storage_error)?;
            let mut collected = Vec::new();
            for row in iter {
                let (key, value) = row.map_err(storage_error)?;
                if !key.value().starts_with(prefix.as_ref()) {
                    break;
                }
                let state: RegularFileChunkState = decode(value.value())?;
                for payload in state.slices.values().filter_map(|slice| slice.payload) {
                    freed_objects.insert(payload.object_id);
                }
                collected.push(key.value().to_vec());
            }
            collected
        };
        for key in keys {
            let _ = overlay.remove(key.as_slice()).map_err(storage_error)?;
        }
    }

    Ok(freed_objects.into_iter().collect())
}

impl RedbMetaEngine {
    pub(super) fn refresh_scratch_index(&self) -> MetaResult<()> {
        let txn = self.begin_read_txn()?;
        let loaded = ScratchIndex::load_from_read_txn(&txn)?;
        drop(txn);
        *self.scratch_index_write() = loaded;
        Ok(())
    }

    /// The index holds only counters that every mutation keeps consistent
    /// before it can panic, so a poisoned lock still guards valid state.
    fn scratch_index_write(&self) -> std::sync::RwLockWriteGuard<'_, ScratchIndex> {
        self.scratch_index
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn scratch_index_contains_inode(&self, ino: Ino) -> MetaResult<bool> {
        let guard = self
            .scratch_index
            .read()
            .map_err(|_| storage_error("scratch index lock poisoned"))?;
        Ok(guard.contains_inode(ino))
    }

    fn scratch_index_parent_has_children(&self, parent: Ino) -> MetaResult<bool> {
        let guard = self
            .scratch_index
            .read()
            .map_err(|_| storage_error("scratch index lock poisoned"))?;
        Ok(guard.parent_has_children(parent))
    }

    pub(super) fn has_scratch_children_in_write_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
    ) -> MetaResult<bool> {
        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let prefix = dentry_prefix(parent);
        let range_end = prefix_successor(&prefix);
        let mut iter = match range_end.as_ref() {
            Some(end) => dentries.range(prefix.as_slice()..end.as_ref()),
            None => dentries.range(prefix.as_slice()..),
        }
        .map_err(storage_error)?;
        if let Some(row) = iter.next() {
            let (key, _) = row.map_err(storage_error)?;
            let (decoded_parent, _) = decode_dentry_key(key.value())?;
            return Ok(decoded_parent == parent);
        }
        Ok(false)
    }

    fn insert_scratch_entry_inner(
        &self,
        parent: Ino,
        name: &[u8],
        attr: &InodeAttr,
        symlink_target: Option<&[u8]>,
    ) -> MetaResult<()> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let key = encode_dentry_key(parent, name);
        let bytes = postcard::to_allocvec(attr).map_err(storage_error)?;
        let txn = self.begin_immediate_write_txn()?;
        {
            // Verify parent exists (either scratch or tracked) and is a directory.
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let scratch_parent_attr = read_scratch_attr_from_table(&inodes, parent)?;
            let parent_kind = if let Some(parent_attr) = &scratch_parent_attr {
                parent_attr.kind
            } else {
                Self::get_attr_merged_in_write_txn(&txn, parent)?.kind
            };
            if parent_kind != FileType::Directory {
                return Err(MetaError::NotADirectory(parent));
            }

            let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            if find_scratch_dentry_in_table(&dentries, parent, name)?.is_some()
                || Self::lookup_merged_in_write_txn(&txn, parent, name)?.is_some()
            {
                return Err(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            }
            if inodes.get(attr.ino)?.is_some() {
                return Err(MetaError::TargetChanged);
            }

            inodes.insert(attr.ino, bytes.as_slice())?;
            dentries.insert(key.as_slice(), attr.ino)?;
            if let Some(target) = symlink_target {
                let mut symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
                symlinks.insert(attr.ino, target)?;
            }
            if let Some(mut parent_attr) = scratch_parent_attr {
                if attr.kind == FileType::Directory {
                    parent_attr.nlink = parent_attr.nlink.saturating_add(1);
                }
                parent_attr.touch_mtime();
                parent_attr.touch_ctime();
                let parent_bytes = postcard::to_allocvec(&parent_attr).map_err(storage_error)?;
                inodes.insert(parent, parent_bytes.as_slice())?;
            }
        }
        // Record in index before commit so concurrent readers never miss a committed row.
        self.scratch_index_write().note_insert(parent, attr.ino);
        if let Err(error) = txn.commit() {
            let mut index = self.scratch_index_write();
            index.note_remove_dentry(parent);
            index.note_remove_inode(attr.ino);
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn insert_scratch_entry(
        &self,
        parent: Ino,
        name: &[u8],
        attr: &InodeAttr,
    ) -> MetaResult<()> {
        self.insert_scratch_entry_inner(parent, name, attr, None)
    }

    pub(super) fn insert_scratch_symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        attr: &InodeAttr,
    ) -> MetaResult<()> {
        self.insert_scratch_entry_inner(parent, name, attr, Some(target))
    }

    pub(super) fn read_scratch_dentry(&self, parent: Ino, name: &[u8]) -> MetaResult<Option<Ino>> {
        if !self.scratch_index_parent_has_children(parent)? {
            return Ok(None);
        }
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        Ok(find_scratch_dentry_in_table(&table, parent, name)?.map(|(_, ino)| ino))
    }

    pub(super) fn scratch_parent_has_children(&self, parent: Ino) -> MetaResult<bool> {
        self.scratch_index_parent_has_children(parent)
    }

    pub(super) fn list_scratch_children(
        &self,
        parent: Ino,
    ) -> MetaResult<Vec<(Vec<u8>, InodeAttr)>> {
        if !self.scratch_index_parent_has_children(parent)? {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let prefix = dentry_prefix(parent);
        let range_end = prefix_successor(&prefix);
        let iter = match range_end.as_ref() {
            Some(end) => dentries.range(prefix.as_slice()..end.as_ref()),
            None => dentries.range(prefix.as_slice()..),
        }
        .map_err(storage_error)?;

        let mut entries = Vec::new();
        for row in iter {
            let (key, value) = row.map_err(storage_error)?;
            let (decoded_parent, name) = decode_dentry_key(key.value())?;
            if decoded_parent != parent {
                break;
            }
            if let Some(attr) = read_scratch_attr_from_table(&inodes, value.value())? {
                entries.push((name.to_vec(), attr));
            }
        }
        Ok(entries)
    }

    pub(super) fn read_scratch_attr(&self, ino: Ino) -> MetaResult<InodeAttr> {
        if !self.scratch_index_contains_inode(ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        read_scratch_attr_from_table(&table, ino)?.ok_or(MetaError::InodeNotFound(ino))
    }

    pub(super) fn update_scratch_attr(&self, attr: &InodeAttr) -> MetaResult<()> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let bytes = postcard::to_allocvec(attr).map_err(storage_error)?;
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            if inodes.get(attr.ino)?.is_none() {
                return Err(MetaError::InodeNotFound(attr.ino));
            }
            inodes.insert(attr.ino, bytes.as_slice())?;
        }
        txn.commit()
    }

    pub(super) fn truncate_scratch_file(
        &self,
        ino: Ino,
        new_size: u64,
        attr: &InodeAttr,
    ) -> MetaResult<Vec<u64>> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let mut displaced = HashSet::new();
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let current =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
            if current.kind == FileType::Directory {
                return Err(MetaError::IsADirectory(ino));
            }
            if current.kind != FileType::RegularFile {
                return Err(MetaError::TargetChanged);
            }

            let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
            let chunks = Self::regular_file_chunk_states_for_inode_in_opened_table(&overlay, ino)?;
            for state in chunks.values() {
                for payload in state.slices.values().filter_map(|slice| slice.payload) {
                    displaced.insert(payload.object_id);
                }
            }

            for (chunk_id, state) in &chunks {
                let chunk_start = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
                let chunk_end = chunk_start.saturating_add(u64::from(DEFAULT_CHUNK_SIZE));
                let key = encode_regular_file_chunk_key(ino, *chunk_id);
                if chunk_start >= new_size {
                    let _ = overlay.remove(key.as_slice()).map_err(storage_error)?;
                } else if new_size < chunk_end {
                    let keep_end = u32::try_from(new_size - chunk_start)
                        .map_err(|_| storage_error("scratch truncate chunk offset overflow"))?;
                    if let Some(keep_range) = SliceRange::new(0, keep_end) {
                        let surviving_slices: Vec<SliceRecord> =
                            resolve_chunk_slice_spans(*chunk_id, keep_range, Some(state), None)
                                .into_iter()
                                .filter_map(|span| {
                                    span.payload.map(|payload| {
                                        SliceRecord::new(
                                            SliceId(0),
                                            SliceOrder(0),
                                            span.range,
                                            payload,
                                        )
                                    })
                                })
                                .collect();
                        if surviving_slices.is_empty() {
                            let _ = overlay.remove(key.as_slice()).map_err(storage_error)?;
                        } else {
                            let next_state =
                                Self::regular_file_chunk_state_from_slices(&surviving_slices)?;
                            overlay.insert(key.as_slice(), encode(&next_state)?.as_slice())?;
                        }
                    } else {
                        let _ = overlay.remove(key.as_slice()).map_err(storage_error)?;
                    }
                }
            }

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

            let mut updated = attr.clone();
            updated.size = new_size;
            inodes.insert(
                ino,
                postcard::to_allocvec(&updated)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
        }
        txn.commit()?;
        Ok(displaced.into_iter().collect())
    }

    pub(super) fn replace_scratch_overlay(
        &self,
        ino: Ino,
        chunks: &[(ChunkId, Vec<SliceRecord>)],
    ) -> MetaResult<Vec<u64>> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
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
        if !self.scratch_index_contains_inode(ino)? {
            return Ok(None);
        }
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        table
            .get(encode_regular_file_chunk_key(ino, chunk).as_slice())?
            .map(|row| decode(row.value()))
            .transpose()
    }

    pub(super) fn read_scratch_slices(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> MetaResult<Vec<ResolvedSliceSpan>> {
        if !self.scratch_index_contains_inode(ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }
        if length == 0 {
            return Ok(Vec::new());
        }
        let end = offset
            .checked_add(length)
            .ok_or_else(|| storage_error("scratch lookup range overflow"))?;
        let txn = self.begin_read_txn()?;
        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        if inodes.get(ino)?.is_none() {
            return Err(MetaError::InodeNotFound(ino));
        }
        let overlay_table = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        let mut spans = Vec::new();
        let mut cursor = offset;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(u64::from(DEFAULT_CHUNK_SIZE));
            let query_end = end.min(chunk_end);
            let query = SliceRange::new(
                (cursor - chunk_start) as u32,
                (query_end - chunk_start) as u32,
            )
            .ok_or_else(|| storage_error("invalid scratch chunk-local lookup range"))?;
            let key = encode_regular_file_chunk_key(ino, chunk_id);
            let chunk: Option<RegularFileChunkState> = overlay_table
                .get(key.as_slice())?
                .map(|row| decode(row.value()))
                .transpose()?;
            spans.extend(resolve_chunk_slice_spans(
                chunk_id,
                query,
                chunk.as_ref(),
                None,
            ));
            cursor = query_end;
        }
        Ok(merge_adjacent_spans(spans))
    }

    pub(super) fn read_scratch_symlink_target(&self, ino: Ino) -> MetaResult<Vec<u8>> {
        if !self.scratch_index_contains_inode(ino)? {
            return Err(MetaError::InodeNotFound(ino));
        }
        let txn = self.begin_read_txn()?;
        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let attr =
            read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
        if attr.kind != FileType::Symlink {
            return Err(storage_error(format!(
                "scratch inode {ino} is not a symlink"
            )));
        }
        let symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
        let target = symlinks
            .get(ino)
            .map_err(storage_error)?
            .ok_or(MetaError::InodeNotFound(ino))?;
        Ok(target.value().to_vec())
    }

    pub(super) fn remove_scratch_entry(
        &self,
        parent: Ino,
        name: &[u8],
        keep_open_inode: bool,
    ) -> MetaResult<(InodeAttr, Vec<u64>)> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let (attr, removed_inode, freed_objects) = {
            let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            let Some((stored_name, ino)) = find_scratch_dentry_in_table(&dentries, parent, name)?
            else {
                return Err(MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            };
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
            if attr.kind == FileType::Directory {
                return Err(MetaError::IsADirectory(ino));
            }
            let _ = dentries
                .remove(encode_dentry_key(parent, &stored_name).as_slice())
                .map_err(storage_error)?;
            if let Some(mut parent_attr) = read_scratch_attr_from_table(&inodes, parent)? {
                parent_attr.touch_mtime();
                parent_attr.touch_ctime();
                inodes.insert(
                    parent,
                    postcard::to_allocvec(&parent_attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
            }

            attr.nlink = attr.nlink.saturating_sub(1);
            attr.touch_ctime();
            if keep_open_inode || attr.nlink > 0 {
                inodes.insert(
                    ino,
                    postcard::to_allocvec(&attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
                drop(inodes);
                drop(dentries);
                remove_scratch_conflict_in_txn(&txn, ino)?;
                (attr, None, Vec::new())
            } else {
                let _ = inodes.remove(ino).map_err(storage_error)?;
                drop(inodes);
                drop(dentries);
                let freed = remove_scratch_inode_aux_in_txn(&txn, ino)?;
                (attr, Some(ino), freed)
            }
        };
        txn.commit()?;
        let mut index = self.scratch_index_write();
        index.note_remove_dentry(parent);
        if let Some(ino) = removed_inode {
            index.note_remove_inode(ino);
        }
        Ok((attr, freed_objects))
    }

    pub(super) fn cleanup_orphan_scratch_inode(&self, ino: Ino) -> MetaResult<Vec<u64>> {
        if !self.scratch_index_contains_inode(ino)? {
            return Ok(Vec::new());
        }
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let (removed, freed_objects) = {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let Some(attr) = read_scratch_attr_from_table(&inodes, ino)? else {
                return Ok(Vec::new());
            };
            if attr.nlink > 0 {
                return Ok(Vec::new());
            }
            let _ = inodes.remove(ino).map_err(storage_error)?;
            drop(inodes);
            let freed = remove_scratch_inode_aux_in_txn(&txn, ino)?;
            (true, freed)
        };
        txn.commit()?;
        if removed {
            self.scratch_index_write().note_remove_inode(ino);
        }
        Ok(freed_objects)
    }

    pub(super) fn remove_scratch_dir(&self, parent: Ino, name: &[u8]) -> MetaResult<InodeAttr> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let attr = {
            let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            let Some((stored_name, ino)) = find_scratch_dentry_in_table(&dentries, parent, name)?
            else {
                return Err(MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            };
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
            if attr.kind != FileType::Directory {
                return Err(MetaError::NotADirectory(ino));
            }
            drop(dentries);
            if Self::has_scratch_children_in_write_txn(&txn, ino)? {
                return Err(MetaError::DirNotEmpty(ino));
            }
            let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            let _ = dentries
                .remove(encode_dentry_key(parent, &stored_name).as_slice())
                .map_err(storage_error)?;
            let _ = inodes.remove(ino).map_err(storage_error)?;
            if let Some(mut parent_attr) = read_scratch_attr_from_table(&inodes, parent)? {
                parent_attr.nlink = parent_attr.nlink.saturating_sub(1);
                parent_attr.touch_mtime();
                parent_attr.touch_ctime();
                inodes.insert(
                    parent,
                    postcard::to_allocvec(&parent_attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
            }
            drop(inodes);
            drop(dentries);
            let _ = remove_scratch_inode_aux_in_txn(&txn, ino)?;
            attr.nlink = 0;
            attr.touch_ctime();
            attr
        };
        txn.commit()?;
        let mut index = self.scratch_index_write();
        index.note_remove_dentry(parent);
        index.note_remove_inode(attr.ino);
        Ok(attr)
    }

    pub(super) fn move_scratch_entry(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        replace_if_exists: bool,
        keep_displaced_open: bool,
    ) -> MetaResult<ScratchRenameResult> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let (result, moved_across_parents, displaced_removed_ino) = {
            let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            let Some((src_stored_name, src_ino)) =
                find_scratch_dentry_in_table(&dentries, parent, name)?
            else {
                return Err(MetaError::EntryNotFound {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            };
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut src_attr = read_scratch_attr_from_table(&inodes, src_ino)?
                .ok_or(MetaError::InodeNotFound(src_ino))?;

            // Validate destination parent exists (scratch or tracked) and is a directory.
            let new_parent_scratch_attr = read_scratch_attr_from_table(&inodes, new_parent)?;
            let new_parent_kind = if let Some(attr) = &new_parent_scratch_attr {
                attr.kind
            } else {
                Self::get_attr_merged_in_write_txn(&txn, new_parent)?.kind
            };
            if new_parent_kind != FileType::Directory {
                return Err(MetaError::NotADirectory(new_parent));
            }

            let dst_opt = find_scratch_dentry_in_table(&dentries, new_parent, new_name)?;
            if let Some((_, dst_ino)) = &dst_opt
                && *dst_ino == src_ino
            {
                if parent == new_parent && src_stored_name.as_slice() == new_name {
                    return Ok(ScratchRenameResult {
                        moved_ino: src_ino,
                        namespace_changed: false,
                        displaced: None,
                        removed_object_ids: Vec::new(),
                    });
                }
                // Case-only rename on the same scratch inode.
                let _ = dentries
                    .remove(encode_dentry_key(parent, &src_stored_name).as_slice())
                    .map_err(storage_error)?;
                dentries.insert(encode_dentry_key(new_parent, new_name).as_slice(), src_ino)?;
                src_attr.touch_ctime();
                inodes.insert(
                    src_ino,
                    postcard::to_allocvec(&src_attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
                if let Some(mut parent_attr) = new_parent_scratch_attr {
                    parent_attr.touch_mtime();
                    parent_attr.touch_ctime();
                    inodes.insert(
                        new_parent,
                        postcard::to_allocvec(&parent_attr)
                            .map_err(storage_error)?
                            .as_slice(),
                    )?;
                }
                drop(inodes);
                drop(dentries);
                remove_scratch_conflict_in_txn(&txn, src_ino)?;
                txn.commit()?;
                return Ok(ScratchRenameResult {
                    moved_ino: src_ino,
                    namespace_changed: true,
                    displaced: None,
                    removed_object_ids: Vec::new(),
                });
            }

            // Never silently shadow an existing tracked entry at the destination.
            if Self::lookup_merged_in_write_txn(&txn, new_parent, new_name)?.is_some() {
                return Err(MetaError::EntryExists {
                    parent: new_parent,
                    name: String::from_utf8_lossy(new_name).into_owned(),
                });
            }

            if dst_opt.is_some() && !replace_if_exists {
                return Err(MetaError::EntryExists {
                    parent: new_parent,
                    name: String::from_utf8_lossy(new_name).into_owned(),
                });
            }

            let mut displaced_attr = None;
            let mut displaced_removed_ino = None;
            let mut displaced_is_dir = false;
            if let Some((_, dst_ino)) = &dst_opt {
                let dst_attr = read_scratch_attr_from_table(&inodes, *dst_ino)?
                    .ok_or(MetaError::InodeNotFound(*dst_ino))?;
                if src_attr.kind == FileType::Directory && dst_attr.kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(*dst_ino));
                }
                if src_attr.kind != FileType::Directory && dst_attr.kind == FileType::Directory {
                    return Err(MetaError::IsADirectory(*dst_ino));
                }
                if dst_attr.kind == FileType::Directory {
                    displaced_is_dir = true;
                    drop(dentries);
                    if Self::has_scratch_children_in_write_txn(&txn, *dst_ino)? {
                        return Err(MetaError::DirNotEmpty(*dst_ino));
                    }
                    dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                }
                displaced_attr = Some(dst_attr);
            }

            // Remove source dentry.
            let _ = dentries
                .remove(encode_dentry_key(parent, &src_stored_name).as_slice())
                .map_err(storage_error)?;

            // Remove displaced scratch dentry & inode (or mark orphan if open).
            let mut removed_object_ids = Vec::new();
            let mut final_displaced = None;
            if let (Some((dst_stored_name, dst_ino)), Some(mut dst_attr)) =
                (dst_opt, displaced_attr)
            {
                let _ = dentries
                    .remove(encode_dentry_key(new_parent, &dst_stored_name).as_slice())
                    .map_err(storage_error)?;
                dst_attr.nlink = if dst_attr.kind == FileType::Directory {
                    0
                } else {
                    dst_attr.nlink.saturating_sub(1)
                };
                dst_attr.touch_ctime();
                if dst_attr.kind != FileType::Directory
                    && (keep_displaced_open || dst_attr.nlink > 0)
                {
                    inodes.insert(
                        dst_ino,
                        postcard::to_allocvec(&dst_attr)
                            .map_err(storage_error)?
                            .as_slice(),
                    )?;
                } else {
                    let _ = inodes.remove(dst_ino).map_err(storage_error)?;
                    displaced_removed_ino = Some(dst_ino);
                }
                final_displaced = Some(dst_attr);
            }

            // Insert destination dentry & update source inode + parent attrs.
            dentries.insert(encode_dentry_key(new_parent, new_name).as_slice(), src_ino)?;
            src_attr.parent = new_parent;
            src_attr.touch_ctime();
            inodes.insert(
                src_ino,
                postcard::to_allocvec(&src_attr)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;

            if parent != new_parent
                && let Some(mut old_parent_attr) = read_scratch_attr_from_table(&inodes, parent)?
            {
                if src_attr.kind == FileType::Directory {
                    old_parent_attr.nlink = old_parent_attr.nlink.saturating_sub(1);
                }
                old_parent_attr.touch_mtime();
                old_parent_attr.touch_ctime();
                inodes.insert(
                    parent,
                    postcard::to_allocvec(&old_parent_attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
            }
            if let Some(mut dst_parent_attr) = new_parent_scratch_attr {
                if src_attr.kind == FileType::Directory && parent != new_parent {
                    dst_parent_attr.nlink = dst_parent_attr.nlink.saturating_add(1);
                }
                if displaced_is_dir {
                    dst_parent_attr.nlink = dst_parent_attr.nlink.saturating_sub(1);
                }
                dst_parent_attr.touch_mtime();
                dst_parent_attr.touch_ctime();
                inodes.insert(
                    new_parent,
                    postcard::to_allocvec(&dst_parent_attr)
                        .map_err(storage_error)?
                        .as_slice(),
                )?;
            }

            drop(inodes);
            drop(dentries);
            remove_scratch_conflict_in_txn(&txn, src_ino)?;
            if let Some(dst_ino) = displaced_removed_ino {
                removed_object_ids = remove_scratch_inode_aux_in_txn(&txn, dst_ino)?;
            }

            (
                ScratchRenameResult {
                    moved_ino: src_ino,
                    namespace_changed: true,
                    displaced: final_displaced,
                    removed_object_ids,
                },
                parent != new_parent,
                displaced_removed_ino,
            )
        };

        if moved_across_parents {
            self.scratch_index_write().note_increment_parent(new_parent);
        }
        if let Err(error) = txn.commit() {
            if moved_across_parents {
                self.scratch_index_write().note_remove_dentry(new_parent);
            }
            return Err(error);
        }
        let mut index = self.scratch_index_write();
        if moved_across_parents {
            index.note_remove_dentry(parent);
        }
        if result.displaced.is_some() {
            index.note_remove_dentry(new_parent);
        }
        if let Some(dst_ino) = displaced_removed_ino {
            index.note_remove_inode(dst_ino);
        }
        Ok(result)
    }

    pub(super) fn demote_scratch(
        &self,
        (parent, name): (Ino, &[u8]),
        (new_parent, new_name): (Ino, &[u8]),
        plan: &crate::engine::ScratchDemotion,
    ) -> MetaResult<ScratchRenameResult> {
        use crate::events::MetaEvent;

        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let snapshot = self.begin_read_txn()?;
        let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let mut target_parent =
            read_scratch_attr_from_table(&inodes, new_parent)?.ok_or(MetaError::TargetChanged)?;
        if target_parent.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(new_parent));
        }
        let existing = find_scratch_dentry_in_table(&dentries, new_parent, new_name)?;
        if Self::lookup_merged_in_write_txn(&txn, new_parent, new_name)?.is_some() {
            return Err(MetaError::EntryExists {
                parent: new_parent,
                name: String::from_utf8_lossy(new_name).into_owned(),
            });
        }

        let mut ancestor = new_parent;
        let mut ancestors = HashSet::new();
        while ancestor != ROOT_INO {
            if ancestor == plan.source || !ancestors.insert(ancestor) {
                return Err(MetaError::TargetChanged);
            }
            ancestor = match read_scratch_attr_from_table(&inodes, ancestor)? {
                Some(attr) => attr.parent,
                None => Self::get_attr_merged_in_write_txn(&txn, ancestor)?.parent,
            };
        }

        let source = Self::lookup_merged_in_write_txn(&txn, parent, name)?
            .filter(|entry| entry.ino == plan.source)
            .ok_or(MetaError::TargetChanged)?;
        let mut removed_object_ids = Vec::new();
        let mut displaced = None;
        let mut removed_inode = None;
        if existing.as_ref().map(|(_, ino)| *ino) != plan.displaced.map(|(ino, _)| ino) {
            return Err(MetaError::TargetChanged);
        }
        if let (Some((name, ino)), Some((_, keep_open))) = (existing, plan.displaced) {
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::TargetChanged)?;
            if attr.kind == FileType::Directory {
                if source.kind != FileType::Directory {
                    return Err(MetaError::IsADirectory(ino));
                }
                drop(dentries);
                if Self::has_scratch_children_in_write_txn(&txn, ino)? {
                    return Err(MetaError::DirNotEmpty(ino));
                }
                dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                target_parent.nlink = target_parent.nlink.saturating_sub(1);
                attr.nlink = 0;
            } else {
                if source.kind == FileType::Directory {
                    return Err(MetaError::NotADirectory(ino));
                }
                attr.nlink = attr.nlink.saturating_sub(1);
            }
            dentries.remove(encode_dentry_key(new_parent, &name).as_slice())?;
            attr.touch_ctime();
            if attr.nlink > 0 || (keep_open && attr.kind != FileType::Directory) {
                inodes.insert(ino, encode(&attr)?.as_slice())?;
            } else {
                inodes.remove(ino)?;
                removed_object_ids = remove_scratch_inode_aux_in_txn(&txn, ino)?;
                removed_inode = Some(ino);
            }
            displaced = Some(attr);
        }
        let stored_name = if Self::lookup_merged_exact_in_write_txn(&txn, parent, name)?
            .is_some_and(|entry| entry.ino == source.ino)
        {
            name.to_vec()
        } else {
            Self::merged_children_in_write_txn(&txn, parent)?
                .0
                .into_iter()
                .find(|entry| {
                    entry.ino == source.ino
                        && fold_dentry_name(&entry.name) == fold_dentry_name(name)
                })
                .ok_or(MetaError::TargetChanged)?
                .name
        };
        let mut pending = vec![(parent, stored_name, source.ino)];
        let mut moved = Vec::with_capacity(plan.frozen.len());
        let mut visited = HashSet::with_capacity(plan.frozen.len());
        while let Some((old_parent, old_name, ino)) = pending.pop() {
            if !plan.frozen.contains(&ino) || !visited.insert(ino) {
                return Err(MetaError::TargetChanged);
            }
            Self::reject_receipt_removal_in_txn(&txn, ino)?;
            let mut attr = Self::get_attr_merged_in_write_txn(&txn, ino)?;
            if attr.kind == FileType::Directory {
                let (children, _) = Self::merged_children_in_write_txn(&txn, ino)?;
                pending.extend(
                    children
                        .into_iter()
                        .map(|child| (ino, child.name, child.ino)),
                );
                if pending.len() + visited.len() > plan.frozen.len() {
                    return Err(MetaError::TargetChanged);
                }
                let prefix = dentry_prefix(ino);
                for row in dentries.range(prefix.as_slice()..)? {
                    let (key, child) = row?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    if read_scratch_attr_from_table(&inodes, child.value())?
                        .is_some_and(|child| child.kind == FileType::Directory)
                    {
                        attr.nlink = attr.nlink.saturating_add(1);
                    }
                }
            } else if attr.nlink != 1 {
                return Err(MetaError::TargetChanged);
            }
            let local_only = attr.kind == FileType::RegularFile
                && attr.committed_content_epoch == 0
                && Self::lookup_exact_in_write_table(
                    &txn,
                    BASE_DENTRIES_TABLE,
                    old_parent,
                    &old_name,
                )?
                .is_none_or(|entry| entry.ino != ino);
            if attr.kind == FileType::RegularFile {
                Self::demote_file_in_txn(&txn, &mut attr, plan)?;
            }
            if attr.kind == FileType::Symlink {
                let target = self
                    .merged_symlink_target(&snapshot, ino)?
                    .ok_or(MetaError::TargetChanged)?;
                txn.open_table(SCRATCH_SYMLINKS)
                    .map_err(storage_error)?
                    .insert(ino, target.as_slice())?;
            }
            let mut xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
            for table in [BASE_XATTRS_TABLE, XATTRS_TABLE] {
                for key in Self::keys_for_inode_in_table(&snapshot, table, ino)? {
                    if let Some(value) =
                        self.merged_xattr_value(&snapshot, ino, xattr_name_from_key(&key))?
                    {
                        xattrs.insert(key.as_slice(), value.as_slice())?;
                    }
                }
            }

            let (scratch_parent, scratch_name) = if ino == plan.source {
                (new_parent, new_name)
            } else {
                (old_parent, old_name.as_slice())
            };
            attr.parent = scratch_parent;
            attr.touch_ctime();
            dentries.insert(
                encode_dentry_key(scratch_parent, scratch_name).as_slice(),
                ino,
            )?;
            inodes.insert(ino, encode(&attr)?.as_slice())?;
            moved.push((old_parent, old_name, scratch_parent, attr, local_only));
        }
        if moved.len() != plan.frozen.len() {
            return Err(MetaError::TargetChanged);
        }

        let delete = NamespaceDeleteTxn::new(&txn);
        // Remove descendants while each old dentry still identifies its inode.
        for (old_parent, old_name, _, attr, local_only) in moved.iter().rev() {
            let ino = attr.ino;
            let removal = if *local_only {
                MetaEvent::RemoveLocalEntry {
                    parent: *old_parent,
                    name: old_name.clone(),
                }
            } else {
                MetaEvent::RemoveEntry {
                    parent: *old_parent,
                    name: old_name.clone(),
                }
            };
            Self::append_event_in_txn(&txn, &removal)?;
            Self::remove_dentry_overlay_in_txn(&txn, *old_parent, old_name)?;
            Self::remove_inode_overlay_in_txn(&txn, ino)?;
            if attr.kind == FileType::RegularFile {
                let uploaded = delete.uploaded_overlay_object_ids_in_write_txn(ino)?;
                delete.queue_pending_deletions_in_txn(&uploaded, current_unix_seconds())?;
            }
            Self::clear_inode_overlay_aux_in_txn(&txn, ino)?;
            if attr.kind == FileType::RegularFile {
                Self::refresh_pending_regular_file_recovery_metadata_in_txn(&txn, ino)?;
                Self::append_event_in_txn(&txn, &MetaEvent::RemoveRegularFileGraph { ino })?;
            }
            Self::remove_inode_lifecycle_in_txn(&txn, ino)?;
            Self::append_event_in_txn(&txn, &MetaEvent::RemoveAttr { ino })?;
        }
        let mut source_parent = Self::get_attr_merged_in_write_txn(&txn, parent)?;
        if source.kind == FileType::Directory {
            source_parent.nlink = source_parent.nlink.saturating_sub(1);
            target_parent.nlink = target_parent.nlink.saturating_add(1);
        }
        source_parent.touch_mtime();
        target_parent.touch_mtime();
        target_parent.touch_ctime();
        Self::set_inode_in_txn(&txn, &source_parent)?;
        Self::append_event_in_txn(
            &txn,
            &MetaEvent::SetAttr {
                attr: source_parent,
            },
        )?;
        inodes.insert(new_parent, encode(&target_parent)?.as_slice())?;
        drop(inodes);
        drop(dentries);
        let mut index = self.scratch_index_write();
        txn.commit()?;
        for (_, _, scratch_parent, attr, _) in moved {
            index.note_insert(scratch_parent, attr.ino);
        }
        if displaced.is_some() {
            index.note_remove_dentry(new_parent);
        }
        if let Some(ino) = removed_inode {
            index.note_remove_inode(ino);
        }
        Ok(ScratchRenameResult {
            moved_ino: plan.source,
            namespace_changed: true,
            displaced,
            removed_object_ids,
        })
    }

    fn demote_file_in_txn(
        txn: &redb::WriteTransaction,
        attr: &mut InodeAttr,
        plan: &crate::engine::ScratchDemotion,
    ) -> MetaResult<()> {
        let (size, expected) = plan.files.get(&attr.ino).ok_or(MetaError::TargetChanged)?;
        let spans =
            Self::lookup_regular_file_stored_slices_in_write_txn(txn, attr.ino, 0, attr.size)?;
        if attr.size != *size || spans != *expected {
            return Err(MetaError::TargetChanged);
        }
        let mut chunks = std::collections::BTreeMap::<ChunkId, RegularFileChunkState>::new();
        for span in spans {
            let Some(mut payload) = span.payload else {
                continue;
            };
            payload.object_id = *plan
                .objects
                .get(&payload.object_id)
                .ok_or(MetaError::TargetChanged)?;
            let chunk = chunks.entry(span.chunk_id).or_default();
            let id = chunk
                .try_alloc_slice_id()
                .ok_or_else(|| storage_error("scratch slice ID overflow"))?;
            let order = chunk
                .try_alloc_slice_order()
                .ok_or_else(|| storage_error("scratch slice order overflow"))?;
            chunk.insert_slice(SliceRecord::new(id, order, span.range, payload));
        }
        let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        for (chunk, state) in chunks {
            overlay.insert(
                encode_regular_file_chunk_key(attr.ino, chunk).as_slice(),
                encode(&state)?.as_slice(),
            )?;
        }
        attr.upload_state = UploadState::Ready;
        attr.committed_size = 0;
        attr.committed_content_epoch = 0;
        Ok(())
    }

    pub(super) fn promote_scratch(
        &self,
        (parent, name): (Ino, &[u8]),
        (new_parent, new_name): (Ino, &[u8]),
        source: Ino,
        frozen: &HashSet<Ino>,
        replace: bool,
    ) -> MetaResult<(RenameAtomicResult, Vec<InodeAttr>)> {
        use crate::events::MetaEvent;

        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        if Self::get_attr_merged_in_write_txn(&txn, new_parent)?.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(new_parent));
        }
        let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let (stored_name, ino) = find_scratch_dentry_in_table(&dentries, parent, name)?
            .filter(|(_, ino)| *ino == source)
            .ok_or(MetaError::TargetChanged)?;
        if find_scratch_dentry_in_table(&dentries, new_parent, new_name)?
            .is_some_and(|(_, target)| target != source)
        {
            return Err(MetaError::EntryExists {
                parent: new_parent,
                name: String::from_utf8_lossy(new_name).into_owned(),
            });
        }
        let mut pending = vec![(parent, stored_name.clone(), ino)];
        let mut moved = Vec::new();
        while let Some((entry_parent, entry_name, ino)) = pending.pop() {
            if !frozen.contains(&ino) || moved.len() == frozen.len() {
                return Err(MetaError::TargetChanged);
            }
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::TargetChanged)?;
            if attr.kind == FileType::Directory {
                let start = dentry_prefix(ino);
                for row in dentries.range(start.as_slice()..)? {
                    let (key, child) = row?;
                    if !key.value().starts_with(&start) {
                        break;
                    }
                    if pending.len() + moved.len() + 1 >= frozen.len() {
                        return Err(MetaError::TargetChanged);
                    }
                    let (_, child_name) = decode_dentry_key(key.value())?;
                    pending.push((ino, child_name.to_vec(), child.value()));
                }
            }
            if ino == source {
                attr.parent = new_parent;
            }
            Self::set_inode_in_txn(&txn, &attr)?;
            if attr.kind == FileType::RegularFile && attr.size != 0 {
                attr.content_epoch = attr.content_epoch.max(1);
                let overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
                let chunks: Vec<_> =
                    Self::regular_file_chunk_states_for_inode_in_opened_table(&overlay, ino)?
                        .into_iter()
                        .map(|(chunk, state)| (chunk, state.slices.into_values().collect()))
                        .collect();
                drop(overlay);
                if let (Some(normalized), _) = self.apply_regular_file_overlay_mutation_in_txn(
                    &txn,
                    ino,
                    &chunks,
                    Some(&attr),
                )? {
                    attr = normalized;
                }
            }
            Self::append_event_in_txn(&txn, &MetaEvent::SetAttr { attr: attr.clone() })?;
            if ino != source {
                let entry = DirEntryValue {
                    ino,
                    kind: attr.kind,
                };
                Self::set_dentry_in_txn(&txn, entry_parent, &entry_name, &entry)?;
                Self::append_event_in_txn(
                    &txn,
                    &MetaEvent::AddEntry {
                        parent: entry_parent,
                        name: entry_name.clone(),
                        entry,
                    },
                )?;
                Self::refresh_pending_regular_file_recovery_metadata_in_txn(&txn, ino)?;
            }
            {
                let symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
                if let Some(row) = symlinks.get(ino)? {
                    let target = row.value().to_vec();
                    Self::set_symlink_in_txn(&txn, ino, &target)?;
                    Self::append_event_in_txn(&txn, &MetaEvent::SetSymlinkTarget { ino, target })?;
                }
            }
            {
                let xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
                let start = xattr_prefix(ino);
                for row in xattrs.range(start.as_slice()..)? {
                    let (key, value) = row?;
                    if !key.value().starts_with(&start) {
                        break;
                    }
                    let name = xattr_name_from_key(key.value()).to_vec();
                    let value = value.value().to_vec();
                    Self::set_xattr_in_txn(&txn, ino, &name, &value)?;
                    Self::append_event_in_txn(
                        &txn,
                        &MetaEvent::SetXattr {
                            ino,
                            name,
                            value,
                            flags: 0,
                        },
                    )?;
                }
            }
            remove_scratch_inode_aux_in_txn(&txn, ino)?;
            dentries.remove(encode_dentry_key(entry_parent, &entry_name).as_slice())?;
            inodes.remove(ino)?;
            moved.push((entry_parent, attr));
        }
        if let Some(mut old_parent) = read_scratch_attr_from_table(&inodes, parent)? {
            if moved[0].1.kind == FileType::Directory {
                old_parent.nlink = old_parent.nlink.saturating_sub(1);
            }
            old_parent.touch_mtime();
            old_parent.touch_ctime();
            inodes.insert(
                parent,
                postcard::to_allocvec(&old_parent)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
        }
        drop(dentries);
        drop(inodes);
        let result = Self::rename_or_promote_in_txn(
            &txn,
            (parent, &stored_name),
            (new_parent, new_name),
            replace,
            true,
            Some(DirEntryValue {
                ino: source,
                kind: moved[0].1.kind,
            }),
        )?;
        txn.commit()?;
        let mut index = self.scratch_index_write();
        let attrs = moved
            .into_iter()
            .map(|(parent, attr)| {
                index.note_remove_dentry(parent);
                index.note_remove_inode(attr.ino);
                attr
            })
            .collect();
        Ok((result, attrs))
    }

    pub(super) fn list_tracked_parent_scratch_entries_inner(
        &self,
    ) -> MetaResult<Vec<(Ino, Vec<u8>, InodeAttr)>> {
        {
            let guard = self
                .scratch_index
                .read()
                .map_err(|_| storage_error("scratch index lock poisoned"))?;
            if guard.parent_child_counts.is_empty() {
                return Ok(Vec::new());
            }
        }
        let txn = self.begin_read_txn()?;
        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let mut out = Vec::new();
        for row in dentries.iter().map_err(storage_error)? {
            let (key, value) = row.map_err(storage_error)?;
            let (parent, name) = decode_dentry_key(key.value())?;
            if inodes.get(parent).map_err(storage_error)?.is_some() {
                continue;
            }
            if let Some(attr) = read_scratch_attr_from_table(&inodes, value.value())? {
                out.push((parent, name.to_vec(), attr));
            }
        }
        Ok(out)
    }

    pub(super) fn promote_scratch_auto_inner(
        &self,
        parent: Ino,
        name: &[u8],
        source: Ino,
        frozen: &HashSet<Ino>,
        conflict_client: &str,
    ) -> MetaResult<ScratchAutoPromoteResult> {
        use crate::events::MetaEvent;

        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let mut parent_attr = match Self::get_attr_merged_in_write_txn(&txn, parent) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Err(MetaError::TargetChanged),
            Err(error) => return Err(error),
        };
        if parent_attr.kind != FileType::Directory {
            return Err(MetaError::NotADirectory(parent));
        }

        let stored_name = {
            let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            find_scratch_dentry_in_table(&dentries, parent, name)?
                .filter(|(_, ino)| *ino == source)
                .map(|(stored, _)| stored)
                .ok_or(MetaError::TargetChanged)?
        };
        {
            let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            if read_scratch_attr_from_table(&inodes, source)?.is_none() {
                return Err(MetaError::TargetChanged);
            }
        }

        let existing_tracked =
            Self::find_colliding_live_tracked_entry_in_txn(&txn, parent, &stored_name)?;
        let (target_name, conflict_renamed) = match existing_tracked {
            Some(existing) if existing.ino != source => {
                let conflict = allocate_auto_promote_conflict_name_in_txn(
                    &txn,
                    parent,
                    &stored_name,
                    conflict_client,
                )?;
                tracing::warn!(
                    parent,
                    scratch_ino = source,
                    existing_ino = existing.ino,
                    original_name = %String::from_utf8_lossy(&stored_name),
                    conflict_name = %String::from_utf8_lossy(&conflict),
                    "auto-promote target name taken in local base; renaming scratch entry before promotion"
                );
                (conflict, true)
            }
            _ => (stored_name.clone(), false),
        };

        let mut pending = vec![(parent, stored_name.clone(), target_name.clone(), source)];
        let mut moved = Vec::new();
        while let Some((entry_parent, old_scratch_name, tracked_name, ino)) = pending.pop() {
            if !frozen.contains(&ino) || moved.len() == frozen.len() {
                return Err(MetaError::TargetChanged);
            }
            let mut attr = {
                let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::TargetChanged)?
            };
            if attr.kind == FileType::Directory {
                let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                let start = dentry_prefix(ino);
                let range_end = prefix_successor(&start);
                let iter = match range_end.as_ref() {
                    Some(end) => dentries.range(start.as_slice()..end.as_ref()),
                    None => dentries.range(start.as_slice()..),
                }
                .map_err(storage_error)?;
                let mut children = Vec::new();
                for row in iter {
                    let (key, child) = row.map_err(storage_error)?;
                    let (decoded_parent, child_name) = decode_dentry_key(key.value())?;
                    if decoded_parent != ino {
                        break;
                    }
                    if pending.len() + moved.len() + children.len() + 1 >= frozen.len() {
                        return Err(MetaError::TargetChanged);
                    }
                    let child_name = child_name.to_vec();
                    children.push((ino, child_name.clone(), child_name, child.value()));
                }
                for child in children.into_iter().rev() {
                    pending.push(child);
                }
            }

            attr.parent = entry_parent;
            if attr.kind == FileType::RegularFile && attr.size != 0 {
                attr.content_epoch = attr.content_epoch.max(1);
            }
            attr.touch_ctime();
            Self::set_inode_in_txn(&txn, &attr)?;
            if attr.kind == FileType::RegularFile && attr.size != 0 {
                let chunks: Vec<_> = {
                    let overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
                    Self::regular_file_chunk_states_for_inode_in_opened_table(&overlay, ino)?
                        .into_iter()
                        .map(|(chunk, state)| (chunk, state.slices.into_values().collect()))
                        .collect()
                };
                let (normalized, applied) = self.apply_regular_file_overlay_mutation_in_txn(
                    &txn,
                    ino,
                    &chunks,
                    Some(&attr),
                )?;
                if let Some(normalized) = normalized {
                    attr = normalized;
                }
                if !applied {
                    Self::append_event_in_txn(&txn, &MetaEvent::SetAttr { attr: attr.clone() })?;
                }
            } else {
                if attr.kind == FileType::RegularFile {
                    Self::sync_inode_lifecycle_in_txn(&txn, ino)?;
                }
                Self::append_event_in_txn(&txn, &MetaEvent::SetAttr { attr: attr.clone() })?;
            }

            if attr.kind == FileType::Symlink {
                let target = {
                    let symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
                    symlinks
                        .get(ino)
                        .map_err(storage_error)?
                        .map(|row| row.value().to_vec())
                };
                if let Some(target) = target {
                    Self::set_symlink_in_txn(&txn, ino, &target)?;
                    Self::append_event_in_txn(&txn, &MetaEvent::SetSymlinkTarget { ino, target })?;
                }
            }

            // Emit parent SetAttr immediately before the root AddEntry so
            // `RemoteApplyCoordinator::local_event_is_contextually_visible`
            // hides parent mtime updates along with hidden zero-size drafts.
            if ino == source {
                if attr.kind == FileType::Directory {
                    parent_attr.nlink = parent_attr.nlink.saturating_add(1);
                }
                parent_attr.touch_mtime();
                Self::set_inode_in_txn(&txn, &parent_attr)?;
                Self::append_event_in_txn(
                    &txn,
                    &MetaEvent::SetAttr {
                        attr: parent_attr.clone(),
                    },
                )?;
            }

            // Emit AddEntry before child entries and before SetXattr so ADR 0012
            // directory merges record the alias before rewriting children/xattrs.
            let entry = DirEntryValue {
                ino,
                kind: attr.kind,
            };
            Self::set_dentry_in_txn(&txn, entry_parent, &tracked_name, &entry)?;
            Self::append_event_in_txn(
                &txn,
                &MetaEvent::AddEntry {
                    parent: entry_parent,
                    name: tracked_name,
                    entry,
                },
            )?;

            {
                let xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
                let start = xattr_prefix(ino);
                let range_end = prefix_successor(&start);
                let iter = match range_end.as_ref() {
                    Some(end) => xattrs.range(start.as_slice()..end.as_ref()),
                    None => xattrs.range(start.as_slice()..),
                }
                .map_err(storage_error)?;
                let mut collected = Vec::new();
                for row in iter {
                    let (key, value) = row.map_err(storage_error)?;
                    if !key.value().starts_with(&start) {
                        break;
                    }
                    collected.push((
                        xattr_name_from_key(key.value()).to_vec(),
                        value.value().to_vec(),
                    ));
                }
                drop(xattrs);
                for (xname, xval) in collected {
                    Self::set_xattr_in_txn(&txn, ino, &xname, &xval)?;
                    Self::append_event_in_txn(
                        &txn,
                        &MetaEvent::SetXattr {
                            ino,
                            name: xname,
                            value: xval,
                            flags: 0,
                        },
                    )?;
                }
            }

            if attr.kind == FileType::RegularFile {
                Self::refresh_pending_regular_file_recovery_metadata_in_txn(&txn, ino)?;
            }

            remove_scratch_inode_aux_in_txn(&txn, ino)?;
            {
                let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                let _ = dentries
                    .remove(encode_dentry_key(entry_parent, &old_scratch_name).as_slice())
                    .map_err(storage_error)?;
            }
            {
                let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                let _ = inodes.remove(ino).map_err(storage_error)?;
            }
            moved.push((entry_parent, attr));
        }

        let added_inodes = u64::try_from(moved.len()).unwrap_or(u64::MAX);
        Self::update_counter_in_txn(&txn, counters::TOTAL_INODES, |total| {
            total
                .checked_add(added_inodes)
                .ok_or_else(|| MetaError::Storage("total inodes counter overflow".to_string()))
        })?;
        txn.commit()?;

        let mut index = self.scratch_index_write();
        let mut attrs = Vec::with_capacity(moved.len());
        for (entry_parent, attr) in moved {
            index.note_remove_dentry(entry_parent);
            index.note_remove_inode(attr.ino);
            attrs.push(attr);
        }
        Ok(ScratchAutoPromoteResult {
            promoted_ino: source,
            parent,
            original_name: stored_name,
            final_name: target_name,
            conflict_renamed,
            attrs,
        })
    }

    pub(super) fn write_scratch_xattr(
        &self,
        ino: Ino,
        name: &[u8],
        value: &[u8],
    ) -> MetaResult<()> {
        self.write_scratch_xattr_with_flags(ino, name, value, 0)
    }

    pub(super) fn write_scratch_xattr_with_flags(
        &self,
        ino: Ino,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> MetaResult<()> {
        Self::reject_reserved_xattr(ino, name)?;
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
            let mut xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
            let key = encode_xattr_key(ino, name);
            let exists = xattrs.get(key.as_slice())?.is_some();
            if flags & XATTR_CREATE_FLAG != 0 && exists {
                return Err(MetaError::EntryExists {
                    parent: ino,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            }
            if flags & XATTR_REPLACE_FLAG != 0 && !exists {
                return Err(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            }
            xattrs.insert(key.as_slice(), value)?;
            attr.touch_ctime();
            inodes.insert(
                ino,
                postcard::to_allocvec(&attr)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
        }
        txn.commit()
    }

    pub(super) fn read_scratch_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<Option<Vec<u8>>> {
        if !self.scratch_index_contains_inode(ino)? {
            return Ok(None);
        }
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
        Ok(table
            .get(encode_xattr_key(ino, name).as_slice())?
            .map(|row| row.value().to_vec()))
    }

    pub(super) fn delete_scratch_xattr(&self, ino: Ino, name: &[u8]) -> MetaResult<()> {
        Self::reject_reserved_xattr(ino, name)?;
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        {
            let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
            let mut attr =
                read_scratch_attr_from_table(&inodes, ino)?.ok_or(MetaError::InodeNotFound(ino))?;
            let mut xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
            let key = encode_xattr_key(ino, name);
            if xattrs.remove(key.as_slice())?.is_none() {
                return Err(MetaError::EntryNotFound {
                    parent: ino,
                    name: String::from_utf8_lossy(name).into_owned(),
                });
            }
            attr.touch_ctime();
            inodes.insert(
                ino,
                postcard::to_allocvec(&attr)
                    .map_err(storage_error)?
                    .as_slice(),
            )?;
        }
        txn.commit()
    }

    pub(super) fn enumerate_scratch_xattrs(&self, ino: Ino) -> MetaResult<Vec<Vec<u8>>> {
        if !self.scratch_index_contains_inode(ino)? {
            return Ok(Vec::new());
        }
        let txn = self.begin_read_txn()?;
        let table = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
        let prefix = xattr_prefix(ino);
        let range_end = prefix_successor(&prefix);
        let iter = match range_end.as_ref() {
            Some(end) => table.range(prefix.as_slice()..end.as_ref()),
            None => table.range(prefix.as_slice()..),
        }
        .map_err(storage_error)?;
        let mut names = BTreeSet::new();
        for row in iter {
            let (key, _) = row.map_err(storage_error)?;
            if !key.value().starts_with(prefix.as_ref()) {
                break;
            }
            names.insert(xattr_name_from_key(key.value()).to_vec());
        }
        Ok(names.into_iter().collect())
    }

    /// Scans `SCRATCH_ORPHANS` so local cache GC treats orphaned scratch file
    /// payloads as live until `scratch restore` or `scratch clean` removes them.
    pub(super) fn scratch_orphan_object_ids_from_read_txn(
        txn: &redb::ReadTransaction,
        refs: &mut HashSet<u64>,
    ) -> MetaResult<()> {
        let orphans = match txn.open_table(SCRATCH_ORPHANS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
            Err(error) => return Err(storage_error(error)),
        };
        for row in orphans.iter().map_err(storage_error)? {
            let (_, value) = row.map_err(storage_error)?;
            let record: ScratchOrphanRecord = decode(value.value())?;
            for entry in &record.entries {
                for (_, chunk_state) in &entry.chunks {
                    for slice in chunk_state.slices.values() {
                        if let Some(payload) = slice.payload {
                            refs.insert(payload.object_id);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn resolved_scratch_cache_dir(&self) -> PathBuf {
        if let Ok(guard) = self.scratch_cache_dir.read()
            && let Some(dir) = guard.as_ref()
        {
            return dir.clone();
        }
        self.metadata_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("cache")
    }

    pub(super) fn resolved_scratch_orphans_dir(&self) -> PathBuf {
        self.resolved_scratch_cache_dir().join("scratch-orphans")
    }

    pub(super) fn configure_scratch_cache_dir(&self, cache_dir: PathBuf) -> MetaResult<()> {
        {
            let mut guard = self
                .scratch_cache_dir
                .write()
                .map_err(|_| storage_error("scratch cache dir lock poisoned"))?;
            *guard = Some(cache_dir);
        }
        if self.db.is_some() {
            self.materialize_pending_scratch_orphans()?;
        }
        Ok(())
    }

    pub(super) fn read_scratch_conflicts(&self) -> MetaResult<Vec<ScratchConflictRecord>> {
        let txn = self.begin_read_txn()?;
        let table = match txn.open_table(SCRATCH_CONFLICTS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(storage_error(error)),
        };
        let mut records = Vec::new();
        for row in table.iter().map_err(storage_error)? {
            let (_, value) = row.map_err(storage_error)?;
            records.push(decode(value.value())?);
        }
        Ok(records)
    }

    pub(super) fn delete_scratch_conflict(&self, ino: Ino) -> MetaResult<bool> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let txn = self.begin_immediate_write_txn()?;
        let removed = {
            let mut table = txn.open_table(SCRATCH_CONFLICTS).map_err(storage_error)?;
            table.remove(ino).map_err(storage_error)?.is_some()
        };
        if removed {
            txn.commit()?;
        } else {
            let _ = txn.abort();
        }
        Ok(removed)
    }

    pub(super) fn read_scratch_orphans(&self) -> MetaResult<Vec<ScratchOrphanRecord>> {
        let txn = self.begin_read_txn()?;
        let table = match txn.open_table(SCRATCH_ORPHANS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(storage_error(error)),
        };
        let mut records = Vec::new();
        for row in table.iter().map_err(storage_error)? {
            let (_, value) = row.map_err(storage_error)?;
            records.push(decode(value.value())?);
        }
        Ok(records)
    }

    /// Captures the pre-mutation volume-relative path of every tracked parent
    /// directory that currently holds scratch children, before a replay or
    /// snapshot import transaction mutates tracked tables.
    pub(crate) fn capture_pre_mutation_scratch_context_in_txn(
        &self,
        txn: &redb::WriteTransaction,
    ) -> MetaResult<PreMutationScratchContext> {
        use redb::ReadableTableMetadata;

        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        if dentries.is_empty().map_err(storage_error)? {
            return Ok(PreMutationScratchContext::default());
        }
        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let mut tracked_parents = BTreeSet::new();
        for row in dentries.iter().map_err(storage_error)? {
            let (key, _) = row.map_err(storage_error)?;
            let (parent, _) = decode_dentry_key(key.value())?;
            if inodes.get(parent).map_err(storage_error)?.is_none() {
                tracked_parents.insert(parent);
            }
        }
        drop(inodes);
        drop(dentries);

        let mut tracked_parent_paths = HashMap::new();
        for parent in tracked_parents {
            if let Some(path) = Self::resolve_tracked_dir_path_in_write_txn(txn, parent)? {
                tracked_parent_paths.insert(parent, path);
            }
        }
        Ok(PreMutationScratchContext {
            tracked_parent_paths,
        })
    }

    /// Reconciles local scratch state inside the same write transaction after
    /// foreign replay or snapshot import mutates the tracked namespace:
    /// 1. Re-parents scratch children off ADR 0012 merged directory aliases onto
    ///    canonical directory inodes.
    /// 2. Relocates scratch entries/subtrees whose tracked parent is no longer
    ///    live and reachable from `ROOT_INO` into `SCRATCH_ORPHANS`.
    /// 3. Resolves tracked-vs-scratch name collisions in favor of tracked
    ///    entries, renaming local scratch entries to `<name>.scratch-conflict-<n>`.
    pub(crate) fn reconcile_scratch_after_tracked_mutations_in_txn(
        &self,
        txn: &RedbWriteTransaction<'_>,
        pre: &PreMutationScratchContext,
    ) -> MetaResult<ScratchReconciliationOutcome> {
        use redb::ReadableTableMetadata;

        let mut outcome = ScratchReconciliationOutcome::default();
        let has_scratch_dentries = {
            let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            !dentries.is_empty().map_err(storage_error)?
        };
        if !has_scratch_dentries {
            return Ok(outcome);
        }

        let mut parent_paths = pre.tracked_parent_paths.clone();

        // Phase A: Re-parent scratch entries under ADR 0012 merged directory aliases.
        self.reparent_scratch_alias_children_in_txn(txn, &mut parent_paths, &mut outcome)?;

        // Phase B: Relocate orphaned scratch entries/subtrees whose tracked parent was removed.
        self.relocate_orphaned_scratch_entries_in_txn(txn, &parent_paths, &mut outcome)?;

        // Phase C: Resolve tracked-vs-scratch name collisions (`<name>.scratch-conflict-<n>`).
        self.resolve_scratch_name_conflicts_in_txn(txn, &parent_paths, &mut outcome)?;

        Ok(outcome)
    }

    /// Refreshes the in-memory `ScratchIndex` after a reconciled transaction
    /// commits and materializes any pending orphan batches onto disk.
    pub(crate) fn finish_scratch_reconciliation(
        &self,
        outcome: ScratchReconciliationOutcome,
    ) -> MetaResult<()> {
        if !outcome.changed {
            return Ok(());
        }
        {
            let _mutation_guard = self
                .scratch_mutation_lock
                .lock()
                .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
            self.refresh_scratch_index()?;
        }
        // The orphan records are already committed; a disk failure here only
        // defers materialization to the next `set_scratch_cache_dir`.
        if outcome.orphans_created > 0
            && let Err(error) = self.materialize_pending_scratch_orphans()
        {
            tracing::warn!(%error, "deferring scratch orphan materialization");
        }
        Ok(())
    }

    fn reparent_scratch_alias_children_in_txn(
        &self,
        txn: &RedbWriteTransaction<'_>,
        parent_paths: &mut HashMap<Ino, Vec<u8>>,
        outcome: &mut ScratchReconciliationOutcome,
    ) -> MetaResult<()> {
        const MAX_ALIAS_HOPS: usize = 8;
        let tracked_parents = collect_tracked_scratch_parents_in_txn(txn)?;
        if tracked_parents.is_empty() {
            return Ok(());
        }

        let mut remaps = Vec::new();
        {
            let aliases = txn
                .open_table(DIRECTORY_ALIASES_TABLE)
                .map_err(storage_error)?;
            for parent in tracked_parents {
                let mut current = parent;
                let mut steps = 0usize;
                while let Some(target) = aliases.get(current).map_err(storage_error)? {
                    current = target.value();
                    steps += 1;
                    if steps >= MAX_ALIAS_HOPS {
                        break;
                    }
                }
                if current != parent {
                    remaps.push((parent, current));
                }
            }
        }

        for (alias_parent, canonical_parent) in remaps {
            // Re-parent only once the canonical directory is live and the alias is no longer
            // bound at its path in the merged namespace.
            if Self::tracked_directory_is_live_and_reachable_in_txn(txn, alias_parent)? {
                continue;
            }
            if !Self::tracked_directory_is_live_and_reachable_in_txn(txn, canonical_parent)? {
                continue;
            }
            if let Some(old_path) = parent_paths.get(&alias_parent).cloned() {
                parent_paths.entry(canonical_parent).or_insert(old_path);
            }

            let children = collect_scratch_dentries_for_parent_in_txn(txn, alias_parent)?;
            if children.is_empty() {
                continue;
            }
            let alias_ancestors = Self::invalidation_ancestors(txn, alias_parent)?;
            let canonical_ancestors = Self::invalidation_ancestors(txn, canonical_parent)?;

            for (child_name, child_ino) in children {
                let existing_winner = {
                    let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                    find_scratch_dentry_in_table(&dentries, canonical_parent, &child_name)?
                        .map(|(_, ino)| ino)
                };
                let target_name = if existing_winner.is_none() {
                    child_name.clone()
                } else {
                    allocate_conflict_scratch_name_in_txn(txn, canonical_parent, &child_name)?
                };
                {
                    let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                    let _ = dentries
                        .remove(encode_dentry_key(alias_parent, &child_name).as_slice())
                        .map_err(storage_error)?;
                    dentries
                        .insert(
                            encode_dentry_key(canonical_parent, &target_name).as_slice(),
                            child_ino,
                        )
                        .map_err(storage_error)?;
                }
                {
                    let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                    if let Some(mut attr) = read_scratch_attr_from_table(&inodes, child_ino)? {
                        attr.parent = canonical_parent;
                        attr.touch_ctime();
                        inodes
                            .insert(child_ino, encode(&attr)?.as_slice())
                            .map_err(storage_error)?;
                    }
                }
                if let Some(winning_ino) = existing_winner {
                    let canonical_path = parent_paths
                        .get(&canonical_parent)
                        .cloned()
                        .or(Self::resolve_tracked_dir_path_in_write_txn(
                            txn,
                            canonical_parent,
                        )?)
                        .unwrap_or_default();
                    let record = ScratchConflictRecord {
                        ino: child_ino,
                        parent: canonical_parent,
                        parent_path: canonical_path,
                        original_name: child_name.clone(),
                        conflict_name: target_name.clone(),
                        winning_ino,
                        recorded_at_unix_ms: current_unix_millis(),
                    };
                    let mut conflicts = txn.open_table(SCRATCH_CONFLICTS).map_err(storage_error)?;
                    conflicts
                        .insert(child_ino, encode(&record)?.as_slice())
                        .map_err(storage_error)?;
                    tracing::warn!(
                        parent = canonical_parent,
                        scratch_ino = child_ino,
                        original_name = %String::from_utf8_lossy(&child_name),
                        conflict_name = %String::from_utf8_lossy(&target_name),
                        "merged directory alias collided with existing scratch entry; renamed scratch entry"
                    );
                }

                txn.invalidate(InvalidationIntent::Entry {
                    parent: alias_parent,
                    name: child_name,
                    ancestors: alias_ancestors.clone(),
                    ino: Some(child_ino),
                });
                txn.invalidate(InvalidationIntent::Entry {
                    parent: canonical_parent,
                    name: target_name,
                    ancestors: canonical_ancestors.clone(),
                    ino: Some(child_ino),
                });
                txn.invalidate(InvalidationIntent::Attributes { ino: child_ino });
                outcome.changed = true;
            }
            txn.invalidate(InvalidationIntent::Directory { ino: alias_parent });
            txn.invalidate(InvalidationIntent::Directory {
                ino: canonical_parent,
            });
        }
        Ok(())
    }

    fn relocate_orphaned_scratch_entries_in_txn(
        &self,
        txn: &RedbWriteTransaction<'_>,
        parent_paths: &HashMap<Ino, Vec<u8>>,
        outcome: &mut ScratchReconciliationOutcome,
    ) -> MetaResult<()> {
        let tracked_parents = collect_tracked_scratch_parents_in_txn(txn)?;
        let mut orphaned_roots = Vec::new();
        for parent_ino in tracked_parents {
            if Self::tracked_directory_is_live_and_reachable_in_txn(txn, parent_ino)? {
                continue;
            }
            let parent_old_path = parent_paths
                .get(&parent_ino)
                .cloned()
                .unwrap_or_else(|| format!("inode-{parent_ino}").into_bytes());
            let direct_children = collect_scratch_dentries_for_parent_in_txn(txn, parent_ino)?;
            for (child_name, child_ino) in direct_children {
                orphaned_roots.push((
                    parent_ino,
                    join_volume_rel_path(&parent_old_path, &child_name),
                    child_name,
                    child_ino,
                ));
            }
        }
        if orphaned_roots.is_empty() {
            return Ok(());
        }

        let timestamp_unix_ms = {
            let orphans = txn.open_table(SCRATCH_ORPHANS).map_err(storage_error)?;
            let mut ts = current_unix_millis();
            'probe: loop {
                for (_, root_old_path, _, _) in &orphaned_roots {
                    if orphans
                        .get(encode_scratch_orphan_key(ts, root_old_path).as_slice())
                        .map_err(storage_error)?
                        .is_some()
                    {
                        ts = ts.saturating_add(1);
                        continue 'probe;
                    }
                }
                break ts;
            }
        };

        for (tracked_parent, root_old_path, child_name, root_ino) in orphaned_roots {
            let mut entries = Vec::new();
            collect_and_remove_scratch_subtree_in_txn(
                txn,
                tracked_parent,
                &child_name,
                root_ino,
                &root_old_path,
                &mut entries,
            )?;
            if entries.is_empty() {
                continue;
            }
            let record = ScratchOrphanRecord {
                timestamp_unix_ms,
                root_old_path: root_old_path.clone(),
                entries,
                materialized: false,
            };
            {
                let mut orphans = txn.open_table(SCRATCH_ORPHANS).map_err(storage_error)?;
                let key = encode_scratch_orphan_key(timestamp_unix_ms, &root_old_path);
                orphans
                    .insert(key.as_slice(), encode(&record)?.as_slice())
                    .map_err(storage_error)?;
            }
            tracing::warn!(
                parent = tracked_parent,
                scratch_ino = root_ino,
                old_path = %String::from_utf8_lossy(&root_old_path),
                orphan_timestamp = timestamp_unix_ms,
                "tracked parent removed; relocated scratch subtree to orphan storage"
            );
            txn.invalidate(InvalidationIntent::Entry {
                parent: tracked_parent,
                name: child_name,
                ancestors: None,
                ino: Some(root_ino),
            });
            txn.invalidate(InvalidationIntent::Directory {
                ino: tracked_parent,
            });
            for orphaned in &record.entries {
                txn.invalidate(InvalidationIntent::Attributes { ino: orphaned.ino });
            }
            outcome.changed = true;
            outcome.orphans_created += 1;
        }
        Ok(())
    }

    fn resolve_scratch_name_conflicts_in_txn(
        &self,
        txn: &RedbWriteTransaction<'_>,
        parent_paths: &HashMap<Ino, Vec<u8>>,
        outcome: &mut ScratchReconciliationOutcome,
    ) -> MetaResult<()> {
        let tracked_parents = collect_tracked_scratch_parents_in_txn(txn)?;
        for parent_ino in tracked_parents {
            let scratch_children = collect_scratch_dentries_for_parent_in_txn(txn, parent_ino)?;
            if scratch_children.is_empty() {
                continue;
            }
            let mut cached_parent_path: Option<Vec<u8>> = None;
            let mut cached_ancestors: Option<Option<Vec<Vec<u8>>>> = None;
            for (stored_name, scratch_ino) in scratch_children {
                let Some(tracked_winner) =
                    Self::find_colliding_live_tracked_entry_in_txn(txn, parent_ino, &stored_name)?
                else {
                    continue;
                };
                if tracked_winner.ino == scratch_ino {
                    continue;
                }

                let conflict_name =
                    allocate_conflict_scratch_name_in_txn(txn, parent_ino, &stored_name)?;
                {
                    let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                    let _ = dentries
                        .remove(encode_dentry_key(parent_ino, &stored_name).as_slice())
                        .map_err(storage_error)?;
                    dentries
                        .insert(
                            encode_dentry_key(parent_ino, &conflict_name).as_slice(),
                            scratch_ino,
                        )
                        .map_err(storage_error)?;
                }
                {
                    let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                    if let Some(mut attr) = read_scratch_attr_from_table(&inodes, scratch_ino)? {
                        attr.touch_ctime();
                        inodes
                            .insert(scratch_ino, encode(&attr)?.as_slice())
                            .map_err(storage_error)?;
                    }
                }
                let parent_path = if let Some(path) = &cached_parent_path {
                    path.clone()
                } else {
                    let resolved = Self::resolve_tracked_dir_path_in_write_txn(txn, parent_ino)?
                        .or_else(|| parent_paths.get(&parent_ino).cloned())
                        .unwrap_or_default();
                    cached_parent_path = Some(resolved.clone());
                    resolved
                };
                let conflict_record = ScratchConflictRecord {
                    ino: scratch_ino,
                    parent: parent_ino,
                    parent_path,
                    original_name: stored_name.clone(),
                    conflict_name: conflict_name.clone(),
                    winning_ino: tracked_winner.ino,
                    recorded_at_unix_ms: current_unix_millis(),
                };
                {
                    let mut conflicts = txn.open_table(SCRATCH_CONFLICTS).map_err(storage_error)?;
                    conflicts
                        .insert(scratch_ino, encode(&conflict_record)?.as_slice())
                        .map_err(storage_error)?;
                }
                tracing::warn!(
                    parent = parent_ino,
                    scratch_ino,
                    winning_ino = tracked_winner.ino,
                    original_name = %String::from_utf8_lossy(&stored_name),
                    conflict_name = %String::from_utf8_lossy(&conflict_name),
                    "replicated tracked entry conflicted with local scratch entry; renamed scratch entry"
                );
                let ancestors = if let Some(ancestors) = &cached_ancestors {
                    ancestors.clone()
                } else {
                    let computed = Self::invalidation_ancestors(txn, parent_ino)?;
                    cached_ancestors = Some(computed.clone());
                    computed
                };
                txn.invalidate(InvalidationIntent::Entry {
                    parent: parent_ino,
                    name: stored_name,
                    ancestors: ancestors.clone(),
                    ino: Some(scratch_ino),
                });
                txn.invalidate(InvalidationIntent::Entry {
                    parent: parent_ino,
                    name: conflict_name,
                    ancestors,
                    ino: Some(scratch_ino),
                });
                txn.invalidate(InvalidationIntent::Directory { ino: parent_ino });
                txn.invalidate(InvalidationIntent::Attributes { ino: scratch_ino });
                outcome.changed = true;
                outcome.conflicts_resolved += 1;
            }
        }
        Ok(())
    }

    fn find_colliding_live_tracked_entry_in_txn(
        txn: &redb::WriteTransaction,
        parent: Ino,
        name: &[u8],
    ) -> MetaResult<Option<DirEntryValue>> {
        if let Some(entry) = Self::lookup_merged_in_write_txn(txn, parent, name)? {
            return Ok(Some(entry));
        }
        // If `name` itself has an exact tombstone matching an unlinked prior tracked entry,
        // `lookup_merged_in_write_txn` returns None before checking case variants. Check whether a
        // live case-equivalent tracked dentry exists under `parent`.
        let key = encode_dentry_key(parent, name);
        let has_tombstone = {
            let tombstones = txn
                .open_table(DENTRY_TOMBSTONES_TABLE)
                .map_err(storage_error)?;
            tombstones.get(key.as_slice())?.is_some()
        };
        if !has_tombstone {
            return Ok(None);
        }
        let current = txn.open_table(DENTRIES_TABLE).map_err(storage_error)?;
        let base = txn.open_table(BASE_DENTRIES_TABLE).map_err(storage_error)?;
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(storage_error)?;
        let current_folded = txn
            .open_table(DENTRIES_BY_FOLDED_NAME_TABLE)
            .map_err(storage_error)?;
        let base_folded = txn
            .open_table(BASE_DENTRIES_BY_FOLDED_NAME_TABLE)
            .map_err(storage_error)?;
        let prefix = folded_dentry_prefix(parent, name)?;
        for rows in [
            current_folded.range(prefix.as_slice()..)?,
            base_folded.range(prefix.as_slice()..)?,
        ] {
            for row in rows {
                let (folded_key, _) = row.map_err(storage_error)?;
                let Some(stored_name) = folded_key.value().strip_prefix(prefix.as_slice()) else {
                    break;
                };
                if stored_name == name {
                    continue;
                }
                let stored_key = encode_dentry_key(parent, stored_name);
                let (entry, _) = Self::lookup_merged_exact_key_from_opened_tables(
                    &base,
                    &current,
                    &tombstones,
                    &stored_key,
                )?;
                if let Some(entry) = entry {
                    return Ok(Some(entry));
                }
            }
        }
        Ok(None)
    }

    fn tracked_directory_is_live_and_reachable_in_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<bool> {
        Ok(Self::resolve_tracked_dir_path_in_write_txn(txn, ino)?.is_some())
    }

    fn resolve_tracked_dir_path_in_write_txn(
        txn: &redb::WriteTransaction,
        ino: Ino,
    ) -> MetaResult<Option<Vec<u8>>> {
        if ino == ROOT_INO {
            return match Self::get_attr_merged_in_write_txn(txn, ROOT_INO) {
                Ok(attr) if attr.kind == FileType::Directory => Ok(Some(Vec::new())),
                Ok(_) | Err(MetaError::InodeNotFound(_)) => Ok(None),
                Err(error) => Err(error),
            };
        }

        let mut cur = ino;
        let mut segments = Vec::new();
        let mut visited = HashSet::new();
        while cur != ROOT_INO {
            if segments.len() >= 256 || !visited.insert(cur) {
                return Ok(None);
            }
            match Self::get_attr_merged_in_write_txn(txn, cur) {
                Ok(attr) if attr.kind == FileType::Directory => {}
                Ok(_) | Err(MetaError::InodeNotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            let Some((parent, name)) = find_live_tracked_dir_parent_in_write_txn(txn, cur)? else {
                return Ok(None);
            };
            segments.push(name);
            cur = parent;
        }
        match Self::get_attr_merged_in_write_txn(txn, ROOT_INO) {
            Ok(attr) if attr.kind == FileType::Directory => {}
            Ok(_) | Err(MetaError::InodeNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        }

        segments.reverse();
        let total_len: usize = segments
            .iter()
            .map(Vec::len)
            .sum::<usize>()
            .saturating_add(segments.len().saturating_sub(1));
        let mut path = Vec::with_capacity(total_len);
        for (idx, seg) in segments.iter().enumerate() {
            if idx > 0 {
                path.push(b'/');
            }
            path.extend_from_slice(seg);
        }
        Ok(Some(path))
    }

    /// Materializes all pending `SCRATCH_ORPHANS` entries onto the local
    /// filesystem under `<cache>/scratch-orphans/<timestamp>/<old path>` using a
    /// staging directory and atomic rename so crashes leave either no partial
    /// target or an idempotently replaceable staging dir.
    pub(super) fn materialize_pending_scratch_orphans(&self) -> MetaResult<()> {
        if self.db.is_none() {
            return Ok(());
        }
        // Payload objects live under the mount's cache dir; materializing
        // against a guessed fallback would write empty files and mark them done.
        let Some(cache_root) = self
            .scratch_cache_dir
            .read()
            .map_err(|_| storage_error("scratch cache dir lock poisoned"))?
            .clone()
        else {
            return Ok(());
        };
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let pending_keys: Vec<(Vec<u8>, ScratchOrphanRecord)> = {
            let txn = self.begin_read_txn()?;
            let table = match txn.open_table(SCRATCH_ORPHANS) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(()),
                Err(error) => return Err(storage_error(error)),
            };
            let mut out = Vec::new();
            for row in table.iter().map_err(storage_error)? {
                let (key, value) = row.map_err(storage_error)?;
                let record: ScratchOrphanRecord = decode(value.value())?;
                if !record.materialized {
                    out.push((key.value().to_vec(), record));
                }
            }
            out
        };
        if pending_keys.is_empty() {
            return Ok(());
        }

        let orphans_root = cache_root.join("scratch-orphans");
        std::fs::create_dir_all(&orphans_root)
            .map_err(|e| storage_error(format!("create scratch-orphans root failed: {e}")))?;

        let mut completed_keys = Vec::with_capacity(pending_keys.len());
        for (key, mut record) in pending_keys {
            if let Err(error) =
                materialize_single_orphan_record(&cache_root, &orphans_root, &record)
            {
                tracing::warn!(
                    %error,
                    timestamp_unix_ms = record.timestamp_unix_ms,
                    "scratch orphan stays pending; redb record keeps its payload"
                );
                continue;
            }
            record.materialized = true;
            completed_keys.push((key, record));
        }
        if completed_keys.is_empty() {
            return Ok(());
        }

        let txn = self.begin_immediate_write_txn()?;
        {
            let mut table = txn.open_table(SCRATCH_ORPHANS).map_err(storage_error)?;
            for (key, record) in completed_keys {
                if table.get(key.as_slice()).map_err(storage_error)?.is_some() {
                    table
                        .insert(key.as_slice(), encode(&record)?.as_slice())
                        .map_err(storage_error)?;
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    pub(super) fn restore_scratch_orphan_entry(
        &self,
        timestamp_unix_ms: u64,
        root_old_path: &[u8],
        target_parent: Option<Ino>,
        target_name: Option<&[u8]>,
    ) -> MetaResult<InodeAttr> {
        let _mutation_guard = self
            .scratch_mutation_lock
            .lock()
            .map_err(|_| storage_error("scratch mutation lock poisoned"))?;
        let orphan_key = encode_scratch_orphan_key(timestamp_unix_ms, root_old_path);
        let txn = self.begin_immediate_write_txn()?;

        let record: ScratchOrphanRecord = {
            let orphans = txn.open_table(SCRATCH_ORPHANS).map_err(storage_error)?;
            let Some(row) = orphans.get(orphan_key.as_slice()).map_err(storage_error)? else {
                return Err(MetaError::EntryNotFound {
                    parent: ROOT_INO,
                    name: String::from_utf8_lossy(root_old_path).into_owned(),
                });
            };
            decode(row.value())?
        };
        if record.entries.is_empty() {
            return Err(storage_error("scratch orphan record has no entries"));
        }

        let mut created_index_entries: Vec<(Ino, Ino)> = Vec::new();
        let (dest_parent, dest_name) = match (target_parent, target_name) {
            (Some(parent), Some(name)) => {
                if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
                    return Err(storage_error("invalid restore target name"));
                }
                let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                let parent_kind = if let Some(attr) = read_scratch_attr_from_table(&inodes, parent)?
                {
                    attr.kind
                } else {
                    Self::get_attr_merged_in_write_txn(&txn, parent)?.kind
                };
                if parent_kind != FileType::Directory {
                    return Err(MetaError::NotADirectory(parent));
                }
                let _ = (dentries, inodes);
                (parent, name.to_vec())
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(storage_error(
                    "target_parent and target_name must either both be provided or both be omitted",
                ));
            }
            (None, None) => {
                let components: Vec<&[u8]> = record
                    .root_old_path
                    .split(|b| *b == b'/')
                    .filter(|part| !part.is_empty())
                    .collect();
                let Some((&final_name, dir_components)) = components.split_last() else {
                    return Err(storage_error("scratch orphan old_path has no filename"));
                };
                let root_attr = Self::get_attr_merged_in_write_txn(&txn, ROOT_INO)?;
                let mut cur_parent = ROOT_INO;
                let mut parent_is_scratch = false;
                for &comp in dir_components {
                    if comp == b"." || comp == b".." {
                        return Err(storage_error("invalid path component in scratch orphan"));
                    }
                    if !parent_is_scratch
                        && let Some(tracked) =
                            Self::lookup_merged_in_write_txn(&txn, cur_parent, comp)?
                    {
                        if tracked.kind != FileType::Directory {
                            return Err(MetaError::NotADirectory(tracked.ino));
                        }
                        cur_parent = tracked.ino;
                        continue;
                    }
                    let existing_scratch = {
                        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                        find_scratch_dentry_in_table(&dentries, cur_parent, comp)?
                    };
                    if let Some((_, scratch_dir_ino)) = existing_scratch {
                        let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                        let attr = read_scratch_attr_from_table(&inodes, scratch_dir_ino)?
                            .ok_or(MetaError::InodeNotFound(scratch_dir_ino))?;
                        if attr.kind != FileType::Directory {
                            return Err(MetaError::NotADirectory(scratch_dir_ino));
                        }
                        cur_parent = scratch_dir_ino;
                        parent_is_scratch = true;
                        continue;
                    }
                    let new_dir_ino = allocate_next_inode_in_txn(&txn)?;
                    let dir_attr = InodeAttr::new_dir(
                        new_dir_ino,
                        cur_parent,
                        root_attr.uid,
                        root_attr.gid,
                        0o755,
                    );
                    {
                        let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                        let mut dentries =
                            txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                        if let Some(mut p_attr) = read_scratch_attr_from_table(&inodes, cur_parent)?
                        {
                            p_attr.nlink = p_attr.nlink.saturating_add(1);
                            p_attr.touch_mtime();
                            p_attr.touch_ctime();
                            inodes
                                .insert(cur_parent, encode(&p_attr)?.as_slice())
                                .map_err(storage_error)?;
                        }
                        inodes
                            .insert(new_dir_ino, encode(&dir_attr)?.as_slice())
                            .map_err(storage_error)?;
                        dentries
                            .insert(encode_dentry_key(cur_parent, comp).as_slice(), new_dir_ino)
                            .map_err(storage_error)?;
                    }
                    created_index_entries.push((cur_parent, new_dir_ino));
                    cur_parent = new_dir_ino;
                    parent_is_scratch = true;
                }
                (cur_parent, final_name.to_vec())
            }
        };

        {
            let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
            if find_scratch_dentry_in_table(&dentries, dest_parent, &dest_name)?.is_some()
                || Self::lookup_merged_in_write_txn(&txn, dest_parent, &dest_name)?.is_some()
            {
                return Err(MetaError::EntryExists {
                    parent: dest_parent,
                    name: String::from_utf8_lossy(&dest_name).into_owned(),
                });
            }
        }

        // Map original orphan inodes to themselves unless already occupied in the namespace.
        let mut ino_remap = HashMap::new();
        for entry in &record.entries {
            let occupied = {
                let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                inodes.get(entry.ino).map_err(storage_error)?.is_some()
            } || !matches!(
                Self::get_attr_merged_in_write_txn(&txn, entry.ino),
                Err(MetaError::InodeNotFound(_))
            );
            let final_ino = if occupied {
                allocate_next_inode_in_txn(&txn)?
            } else {
                entry.ino
            };
            ino_remap.insert(entry.ino, final_ino);
        }

        let mut restored_root_attr = None;
        for (idx, entry) in record.entries.iter().enumerate() {
            let new_ino = *ino_remap
                .get(&entry.ino)
                .ok_or_else(|| storage_error("missing remapped orphan inode"))?;
            let (entry_parent, entry_name) = if idx == 0 {
                (dest_parent, dest_name.clone())
            } else {
                let mapped_parent = ino_remap
                    .get(&entry.parent_ino)
                    .copied()
                    .unwrap_or(entry.parent_ino);
                let name = entry
                    .old_path
                    .rsplit(|b| *b == b'/')
                    .next()
                    .unwrap_or(entry.old_path.as_slice())
                    .to_vec();
                (mapped_parent, name)
            };

            let mut attr = entry.attr.clone();
            attr.ino = new_ino;
            attr.parent = entry_parent;
            attr.touch_ctime();
            if idx == 0 {
                restored_root_attr = Some(attr.clone());
            }

            {
                let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
                let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
                if idx == 0
                    && attr.kind == FileType::Directory
                    && let Some(mut parent_attr) =
                        read_scratch_attr_from_table(&inodes, dest_parent)?
                {
                    parent_attr.nlink = parent_attr.nlink.saturating_add(1);
                    parent_attr.touch_mtime();
                    parent_attr.touch_ctime();
                    inodes
                        .insert(dest_parent, encode(&parent_attr)?.as_slice())
                        .map_err(storage_error)?;
                }
                inodes
                    .insert(new_ino, encode(&attr)?.as_slice())
                    .map_err(storage_error)?;
                dentries
                    .insert(
                        encode_dentry_key(entry_parent, &entry_name).as_slice(),
                        new_ino,
                    )
                    .map_err(storage_error)?;
            }
            if let Some(target) = &entry.symlink_target {
                let mut symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
                symlinks
                    .insert(new_ino, target.as_slice())
                    .map_err(storage_error)?;
            }
            if !entry.xattrs.is_empty() {
                let mut xattrs = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
                for (name, value) in &entry.xattrs {
                    xattrs
                        .insert(encode_xattr_key(new_ino, name).as_slice(), value.as_slice())
                        .map_err(storage_error)?;
                }
            }
            if !entry.chunks.is_empty() {
                let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
                for (chunk_id, state) in &entry.chunks {
                    overlay
                        .insert(
                            encode_regular_file_chunk_key(new_ino, *chunk_id).as_slice(),
                            encode(state)?.as_slice(),
                        )
                        .map_err(storage_error)?;
                }
            }
            created_index_entries.push((entry_parent, new_ino));
        }

        {
            let mut orphans = txn.open_table(SCRATCH_ORPHANS).map_err(storage_error)?;
            let _ = orphans
                .remove(orphan_key.as_slice())
                .map_err(storage_error)?;
        }

        txn.invalidate(InvalidationIntent::Entry {
            parent: dest_parent,
            name: dest_name,
            ancestors: Self::invalidation_ancestors(&txn, dest_parent)?,
            ino: restored_root_attr.as_ref().map(|attr| attr.ino),
        });
        txn.invalidate(InvalidationIntent::Directory { ino: dest_parent });
        for &(_, ino) in &created_index_entries {
            txn.invalidate(InvalidationIntent::Attributes { ino });
        }

        txn.commit()?;

        {
            let mut index = self.scratch_index_write();
            for (parent, ino) in created_index_entries {
                index.note_insert(parent, ino);
            }
        }

        let orphans_root = self.resolved_scratch_orphans_dir();
        let disk_target = orphan_disk_target_path(&orphans_root, timestamp_unix_ms, root_old_path);
        remove_path_best_effort(&disk_target);
        cleanup_empty_orphan_ancestor_dirs(
            disk_target.parent(),
            &orphans_root.join(timestamp_unix_ms.to_string()),
            &orphans_root,
        );

        restored_root_attr.ok_or_else(|| storage_error("restored scratch orphan root missing"))
    }
}

/// Pre-mutation snapshot of volume-relative paths for tracked parents holding
/// scratch children before a replay or snapshot import transaction runs.
#[derive(Debug, Default)]
pub(crate) struct PreMutationScratchContext {
    tracked_parent_paths: HashMap<Ino, Vec<u8>>,
}

/// Summary of scratch reconciliation mutations performed inside a replay or
/// snapshot import transaction.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
pub(crate) struct ScratchReconciliationOutcome {
    pub(crate) changed: bool,
    pub(crate) conflicts_resolved: usize,
    pub(crate) orphans_created: usize,
}

fn current_unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn join_volume_rel_path(parent: &[u8], name: &[u8]) -> Vec<u8> {
    if parent.is_empty() {
        return name.to_vec();
    }
    let mut joined = Vec::with_capacity(parent.len() + 1 + name.len());
    joined.extend_from_slice(parent);
    joined.push(b'/');
    joined.extend_from_slice(name);
    joined
}

fn collect_tracked_scratch_parents_in_txn(txn: &redb::WriteTransaction) -> MetaResult<Vec<Ino>> {
    let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
    let inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
    let mut parents = BTreeSet::new();
    for row in dentries.iter().map_err(storage_error)? {
        let (key, _) = row.map_err(storage_error)?;
        let (parent, _) = decode_dentry_key(key.value())?;
        if inodes.get(parent).map_err(storage_error)?.is_none() {
            parents.insert(parent);
        }
    }
    Ok(parents.into_iter().collect())
}

fn collect_scratch_dentries_for_parent_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
) -> MetaResult<Vec<(Vec<u8>, Ino)>> {
    let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
    let prefix = dentry_prefix(parent);
    let range_end = prefix_successor(&prefix);
    let iter = match range_end.as_ref() {
        Some(end) => dentries.range(prefix.as_slice()..end.as_ref()),
        None => dentries.range(prefix.as_slice()..),
    }
    .map_err(storage_error)?;
    let mut out = Vec::new();
    for row in iter {
        let (key, value) = row.map_err(storage_error)?;
        let (decoded_parent, name) = decode_dentry_key(key.value())?;
        if decoded_parent != parent {
            break;
        }
        out.push((name.to_vec(), value.value()));
    }
    Ok(out)
}

fn sanitize_conflict_client(client: &str) -> String {
    let sanitized: String = client
        .trim()
        .chars()
        .map(|ch| {
            if ch == '\0'
                || ch == '/'
                || ch == '\\'
                || ch.is_whitespace()
                || matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*')
            {
                '-'
            } else {
                ch
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches('-');
    if trimmed.is_empty() {
        "local".to_string()
    } else {
        trimmed.to_string()
    }
}

fn scratch_or_tracked_candidate_taken_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
    candidate: &[u8],
) -> MetaResult<bool> {
    let scratch_taken = {
        let dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        find_scratch_dentry_in_table(&dentries, parent, candidate)?.is_some()
    };
    if scratch_taken {
        return Ok(true);
    }
    if RedbMetaEngine::find_colliding_live_tracked_entry_in_txn(txn, parent, candidate)?.is_some() {
        return Ok(true);
    }
    // Also avoid un-tombstoned raw tracked entries (e.g., hidden draft regular files).
    let candidate_key = encode_dentry_key(parent, candidate);
    let tombstoned = {
        let tombstones = txn
            .open_table(DENTRY_TOMBSTONES_TABLE)
            .map_err(storage_error)?;
        tombstones
            .get(candidate_key.as_slice())
            .map_err(storage_error)?
            .is_some()
    };
    if !tombstoned {
        let raw_tracked =
            RedbMetaEngine::lookup_exact_in_write_table(txn, DENTRIES_TABLE, parent, candidate)?
                .is_some()
                || RedbMetaEngine::lookup_exact_in_write_table(
                    txn,
                    BASE_DENTRIES_TABLE,
                    parent,
                    candidate,
                )?
                .is_some();
        if raw_tracked {
            return Ok(true);
        }
    }
    Ok(false)
}

fn allocate_conflict_scratch_name_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
    original_name: &[u8],
) -> MetaResult<Vec<u8>> {
    for n in 1_u64.. {
        let suffix = format!(".scratch-conflict-{n}");
        let mut candidate = Vec::with_capacity(original_name.len() + suffix.len());
        candidate.extend_from_slice(original_name);
        candidate.extend_from_slice(suffix.as_bytes());
        if !scratch_or_tracked_candidate_taken_in_txn(txn, parent, &candidate)? {
            return Ok(candidate);
        }
    }
    Err(storage_error("exhausted scratch conflict rename suffixes"))
}

fn allocate_auto_promote_conflict_name_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
    original_name: &[u8],
    conflict_client: &str,
) -> MetaResult<Vec<u8>> {
    let client = sanitize_conflict_client(conflict_client);
    let first_suffix = format!(".scratch-conflict-{client}");
    let mut first = Vec::with_capacity(original_name.len() + first_suffix.len());
    first.extend_from_slice(original_name);
    first.extend_from_slice(first_suffix.as_bytes());
    if !scratch_or_tracked_candidate_taken_in_txn(txn, parent, &first)? {
        return Ok(first);
    }
    for n in 2_u64.. {
        let suffix = format!(".scratch-conflict-{client}-{n}");
        let mut candidate = Vec::with_capacity(original_name.len() + suffix.len());
        candidate.extend_from_slice(original_name);
        candidate.extend_from_slice(suffix.as_bytes());
        if !scratch_or_tracked_candidate_taken_in_txn(txn, parent, &candidate)? {
            return Ok(candidate);
        }
    }
    Err(storage_error(
        "exhausted auto-promote conflict rename suffixes",
    ))
}

fn collect_and_remove_scratch_subtree_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
    name: &[u8],
    ino: Ino,
    old_path: &[u8],
    out: &mut Vec<ScratchOrphanEntry>,
) -> MetaResult<()> {
    {
        let mut dentries = txn.open_table(SCRATCH_DENTRIES).map_err(storage_error)?;
        let _ = dentries
            .remove(encode_dentry_key(parent, name).as_slice())
            .map_err(storage_error)?;
    }
    let Some(attr) = ({
        let mut inodes = txn.open_table(SCRATCH_INODES).map_err(storage_error)?;
        let attr = read_scratch_attr_from_table(&inodes, ino)?;
        let _ = inodes.remove(ino).map_err(storage_error)?;
        attr
    }) else {
        return Ok(());
    };

    remove_scratch_conflict_in_txn(txn, ino)?;

    let symlink_target = if attr.kind == FileType::Symlink {
        let mut symlinks = txn.open_table(SCRATCH_SYMLINKS).map_err(storage_error)?;
        symlinks
            .remove(ino)
            .map_err(storage_error)?
            .map(|row| row.value().to_vec())
    } else {
        None
    };

    let xattrs = {
        let mut xattrs_table = txn.open_table(SCRATCH_XATTRS).map_err(storage_error)?;
        let prefix = xattr_prefix(ino);
        let range_end = prefix_successor(&prefix);
        let rows: Vec<(Vec<u8>, Vec<u8>)> = {
            let iter = match range_end.as_ref() {
                Some(end) => xattrs_table.range(prefix.as_slice()..end.as_ref()),
                None => xattrs_table.range(prefix.as_slice()..),
            }
            .map_err(storage_error)?;
            let mut collected = Vec::new();
            for row in iter {
                let (key, val) = row.map_err(storage_error)?;
                if !key.value().starts_with(prefix.as_ref()) {
                    break;
                }
                collected.push((key.value().to_vec(), val.value().to_vec()));
            }
            collected
        };
        let mut out_xattrs = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            let _ = xattrs_table.remove(key.as_slice()).map_err(storage_error)?;
            out_xattrs.push((xattr_name_from_key(&key).to_vec(), value));
        }
        out_xattrs
    };

    let chunks = {
        let mut overlay = txn.open_table(SCRATCH_OVERLAY).map_err(storage_error)?;
        let prefix = regular_file_chunk_prefix(ino);
        let range_end = prefix_successor(&prefix);
        let rows: Vec<(Vec<u8>, RegularFileChunkState)> = {
            let iter = match range_end.as_ref() {
                Some(end) => overlay.range(prefix.as_slice()..end.as_ref()),
                None => overlay.range(prefix.as_slice()..),
            }
            .map_err(storage_error)?;
            let mut collected = Vec::new();
            for row in iter {
                let (key, val) = row.map_err(storage_error)?;
                if !key.value().starts_with(prefix.as_ref()) {
                    break;
                }
                let state: RegularFileChunkState = decode(val.value())?;
                collected.push((key.value().to_vec(), state));
            }
            collected
        };
        let mut out_chunks = Vec::with_capacity(rows.len());
        for (key, state) in rows {
            let (_, chunk_id) = decode_regular_file_chunk_key(&key)?;
            let _ = overlay.remove(key.as_slice()).map_err(storage_error)?;
            out_chunks.push((chunk_id, state));
        }
        out_chunks
    };

    out.push(ScratchOrphanEntry {
        old_path: old_path.to_vec(),
        ino,
        parent_ino: parent,
        attr: attr.clone(),
        symlink_target,
        xattrs,
        chunks,
    });

    if attr.kind == FileType::Directory {
        let children = collect_scratch_dentries_for_parent_in_txn(txn, ino)?;
        for (child_name, child_ino) in children {
            let child_old_path = join_volume_rel_path(old_path, &child_name);
            collect_and_remove_scratch_subtree_in_txn(
                txn,
                ino,
                &child_name,
                child_ino,
                &child_old_path,
                out,
            )?;
        }
    }
    Ok(())
}

fn find_live_tracked_dir_parent_in_write_txn(
    txn: &redb::WriteTransaction,
    ino: Ino,
) -> MetaResult<Option<(Ino, Vec<u8>)>> {
    let prefix = dentry_by_inode_prefix(ino);
    let range_end = prefix_successor(&prefix);
    for index_def in [DENTRIES_BY_INODE_TABLE, BASE_DENTRIES_BY_INODE_TABLE] {
        let candidates: Vec<(Ino, Vec<u8>)> = {
            let index = txn.open_table(index_def).map_err(storage_error)?;
            let iter = match range_end.as_ref() {
                Some(end) => index.range(prefix.as_slice()..end.as_ref()),
                None => index.range(prefix.as_slice()..),
            }
            .map_err(storage_error)?;
            let mut found = Vec::new();
            for row in iter {
                let (key, _) = row.map_err(storage_error)?;
                if !key.value().starts_with(&prefix) {
                    break;
                }
                let (_, parent, name) = decode_dentry_by_inode_key(key.value())?;
                found.push((parent, name.to_vec()));
            }
            found
        };
        for (parent, name) in candidates {
            if lookup_merged_exact_without_remote_filter_in_txn(txn, parent, &name)?
                .is_some_and(|entry| entry.ino == ino && entry.kind == FileType::Directory)
            {
                return Ok(Some((parent, name)));
            }
        }
    }

    // Fallback when a synthetic fixture inserted dentries without populating reverse indexes.
    for table_def in [DENTRIES_TABLE, BASE_DENTRIES_TABLE] {
        let candidates: Vec<(Ino, Vec<u8>)> = {
            let table = txn.open_table(table_def).map_err(storage_error)?;
            let mut found = Vec::new();
            for row in table.iter().map_err(storage_error)? {
                let (key, val) = row.map_err(storage_error)?;
                let entry: DirEntryValue = decode(val.value())?;
                if entry.ino == ino && entry.kind == FileType::Directory {
                    let (parent, name) = decode_dentry_key(key.value())?;
                    found.push((parent, name.to_vec()));
                }
            }
            found
        };
        for (parent, name) in candidates {
            if lookup_merged_exact_without_remote_filter_in_txn(txn, parent, &name)?
                .is_some_and(|entry| entry.ino == ino && entry.kind == FileType::Directory)
            {
                return Ok(Some((parent, name)));
            }
        }
    }
    Ok(None)
}

fn lookup_merged_exact_without_remote_filter_in_txn(
    txn: &redb::WriteTransaction,
    parent: Ino,
    name: &[u8],
) -> MetaResult<Option<DirEntryValue>> {
    let key = encode_dentry_key(parent, name);
    let current = txn.open_table(DENTRIES_TABLE).map_err(storage_error)?;
    let base = txn.open_table(BASE_DENTRIES_TABLE).map_err(storage_error)?;
    let tombstones = txn
        .open_table(DENTRY_TOMBSTONES_TABLE)
        .map_err(storage_error)?;
    let (entry, _) = RedbMetaEngine::lookup_merged_exact_key_from_opened_tables(
        &base,
        &current,
        &tombstones,
        &key,
    )?;
    Ok(entry)
}

fn allocate_next_inode_in_txn(txn: &redb::WriteTransaction) -> MetaResult<Ino> {
    let mut table = txn.open_table(COUNTERS_TABLE).map_err(storage_error)?;
    let ino = match table.get(counters::NEXT_INODE).map_err(storage_error)? {
        Some(v) => v.value(),
        None => ROOT_INO + 1,
    };
    let next_ino = ino
        .checked_add(1)
        .ok_or_else(|| MetaError::Storage("inode allocation overflow".to_string()))?;
    table
        .insert(counters::NEXT_INODE, next_ino)
        .map_err(storage_error)?;
    Ok(ino)
}

fn local_payload_object_path(cache_root: &Path, object_id: u64) -> PathBuf {
    let mut z = object_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    let hash = z ^ (z >> 31);
    let [shard_0, shard_1, ..] = hash.to_be_bytes();
    cache_root
        .join("payload")
        .join("objects")
        .join(format!("{shard_0:02x}"))
        .join(format!("{shard_1:02x}"))
        .join(format!("{object_id:016x}.object"))
}

fn sanitize_host_component(component: &[u8]) -> String {
    let text = String::from_utf8_lossy(component);
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        let invalid = ch == '\0'
            || ch == '\\'
            || ch == '/'
            || (cfg!(windows) && matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*'));
        if invalid {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() || out == "." || out == ".." {
        format!("entry_{:02x}", component.first().copied().unwrap_or(0))
    } else {
        out
    }
}

fn rel_bytes_to_path(rel_bytes: &[u8]) -> PathBuf {
    let mut path = PathBuf::new();
    for segment in rel_bytes.split(|b| *b == b'/') {
        if segment.is_empty() {
            continue;
        }
        path.push(sanitize_host_component(segment));
    }
    if path.as_os_str().is_empty() {
        path.push("orphan");
    }
    path
}

fn orphan_disk_target_path(
    orphans_root: &Path,
    timestamp_unix_ms: u64,
    old_path: &[u8],
) -> PathBuf {
    orphans_root
        .join(timestamp_unix_ms.to_string())
        .join(rel_bytes_to_path(old_path))
}

fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3_u64);
    }
    format!("{hash:016x}")
}

fn materialize_single_orphan_record(
    cache_root: &Path,
    orphans_root: &Path,
    record: &ScratchOrphanRecord,
) -> MetaResult<()> {
    let Some(root_entry) = record.entries.first() else {
        return Ok(());
    };
    let final_path = orphan_disk_target_path(
        orphans_root,
        record.timestamp_unix_ms,
        &record.root_old_path,
    );
    let staging_dir = orphans_root.join(format!(
        ".staging-{}-{}",
        record.timestamp_unix_ms,
        fnv1a64_hex(&record.root_old_path)
    ));
    remove_path_best_effort(&staging_dir);
    std::fs::create_dir_all(&staging_dir)
        .map_err(|e| storage_error(format!("create scratch orphan staging dir failed: {e}")))?;

    let staged_root = staging_dir.join("target");
    if root_entry.attr.kind == FileType::Directory {
        std::fs::create_dir_all(&staged_root)
            .map_err(|e| storage_error(format!("create staged orphan root dir failed: {e}")))?;
        for entry in record.entries.iter().skip(1) {
            let rel_slice = entry
                .old_path
                .strip_prefix(record.root_old_path.as_slice())
                .map(|rest| rest.strip_prefix(b"/").unwrap_or(rest))
                .unwrap_or(entry.old_path.as_slice());
            let entry_path = staged_root.join(rel_bytes_to_path(rel_slice));
            materialize_orphan_entry_node(cache_root, &entry_path, entry)?;
        }
    } else {
        materialize_orphan_entry_node(cache_root, &staged_root, root_entry)?;
    }

    if let Some(parent) = final_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| storage_error(format!("create scratch orphan parent dir failed: {e}")))?;
    }
    remove_path_best_effort(&final_path);
    std::fs::rename(&staged_root, &final_path)
        .map_err(|e| storage_error(format!("install materialized scratch orphan failed: {e}")))?;
    remove_path_best_effort(&staging_dir);
    Ok(())
}

fn materialize_orphan_entry_node(
    cache_root: &Path,
    dest_path: &Path,
    entry: &ScratchOrphanEntry,
) -> MetaResult<()> {
    match entry.attr.kind {
        FileType::Directory => {
            std::fs::create_dir_all(dest_path)
                .map_err(|e| storage_error(format!("create orphan dir failed: {e}")))?;
        }
        FileType::RegularFile => {
            write_orphan_regular_file(cache_root, dest_path, entry)?;
        }
        FileType::Symlink => {
            if let Some(parent) = dest_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    storage_error(format!("create orphan symlink parent failed: {e}"))
                })?;
            }
            let target = entry.symlink_target.as_deref().unwrap_or(b"");
            write_orphan_symlink(dest_path, target)?;
        }
    }
    Ok(())
}

fn write_orphan_regular_file(
    cache_root: &Path,
    dest_path: &Path,
    entry: &ScratchOrphanEntry,
) -> MetaResult<()> {
    use std::io::{Read, Seek, SeekFrom, Write};

    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| storage_error(format!("create orphan file parent failed: {e}")))?;
    }
    let mut out = std::fs::File::create(dest_path)
        .map_err(|e| storage_error(format!("create orphan file failed: {e}")))?;
    if entry.attr.size > 0 {
        out.set_len(entry.attr.size)
            .map_err(|e| storage_error(format!("set orphan file length failed: {e}")))?;
    }
    let Some(full_chunk_range) = SliceRange::new(0, DEFAULT_CHUNK_SIZE) else {
        return Err(storage_error("invalid default chunk range"));
    };
    for (chunk_id, chunk_state) in &entry.chunks {
        let chunk_base = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
        for span in resolve_chunk_slice_spans(*chunk_id, full_chunk_range, Some(chunk_state), None)
        {
            let Some(payload) = span.payload else {
                continue;
            };
            let file_offset = chunk_base.saturating_add(u64::from(span.range.start));
            if file_offset >= entry.attr.size {
                continue;
            }
            let remaining = entry.attr.size - file_offset;
            let copy_len_u64 = remaining.min(u64::from(payload.length));
            let Ok(copy_len) = usize::try_from(copy_len_u64) else {
                continue;
            };
            if copy_len == 0 {
                continue;
            }
            let object_path = local_payload_object_path(cache_root, payload.object_id);
            let mut obj_file = std::fs::File::open(&object_path).map_err(|e| {
                storage_error(format!(
                    "open orphan payload object {} failed: {e}",
                    payload.object_id
                ))
            })?;
            obj_file
                .seek(SeekFrom::Start(u64::from(payload.offset)))
                .map_err(|e| storage_error(format!("seek orphan payload object failed: {e}")))?;
            let mut buf = vec![0_u8; copy_len];
            obj_file.read_exact(&mut buf).map_err(|e| {
                storage_error(format!(
                    "read orphan payload object {} failed: {e}",
                    payload.object_id
                ))
            })?;
            out.seek(SeekFrom::Start(file_offset))
                .map_err(|e| storage_error(format!("seek orphan output file failed: {e}")))?;
            out.write_all(&buf)
                .map_err(|e| storage_error(format!("write orphan output file failed: {e}")))?;
        }
    }
    out.sync_data()
        .map_err(|e| storage_error(format!("sync orphan output file failed: {e}")))?;
    Ok(())
}

fn write_orphan_symlink(dest_path: &Path, target: &[u8]) -> MetaResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        std::os::unix::fs::symlink(std::ffi::OsStr::from_bytes(target), dest_path)
            .map_err(|e| storage_error(format!("create orphan symlink failed: {e}")))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        let target_str = String::from_utf8_lossy(target);
        match std::os::windows::fs::symlink_file(target_str.as_ref(), dest_path) {
            Ok(()) => Ok(()),
            Err(_) => {
                // Unprivileged Windows processes cannot create symlinks; persist the target text
                // so the orphan area remains intact while SCRATCH_ORPHANS holds the symlink kind.
                std::fs::write(dest_path, target).map_err(|e| {
                    storage_error(format!("write orphan symlink fallback failed: {e}"))
                })
            }
        }
    }
}

fn remove_path_best_effort(path: &Path) {
    let Ok(metadata) = std::fs::symlink_metadata(path) else {
        return;
    };
    if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
        let _ = std::fs::remove_dir_all(path);
    } else {
        let _ = std::fs::remove_file(path);
    }
}

fn cleanup_empty_orphan_ancestor_dirs(
    mut dir: Option<&Path>,
    stop_inclusive: &Path,
    orphans_root: &Path,
) {
    while let Some(current) = dir {
        if current == orphans_root {
            break;
        }
        if std::fs::remove_dir(current).is_err() {
            break;
        }
        if current == stop_inclusive {
            break;
        }
        dir = current.parent();
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

    #[test]
    fn scratch_namespace_ops_and_tracked_rmdir_protection_emit_no_events() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("meta.redb");
        let meta = RedbMetaEngine::open(&db_path).unwrap();
        meta.init_root(0, 0).unwrap();
        let tracked_dir = meta.create_dir(ROOT_INO, b"tracked", 0, 0, 0o755).unwrap();
        let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();

        // Create scratch file, dir, and symlink under tracked_dir.
        let scratch_file = InodeAttr::new_file(101, tracked_dir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(tracked_dir.ino, b"Temp.tmp", &scratch_file)
            .unwrap();
        let scratch_dir = InodeAttr::new_dir(102, tracked_dir.ino, 0, 0, 0o755);
        meta.create_scratch_entry(tracked_dir.ino, b"sub", &scratch_dir)
            .unwrap();
        let scratch_link = InodeAttr::new_symlink(103, scratch_dir.ino, 0, 0, 6);
        meta.create_scratch_symlink(scratch_dir.ino, b"lnk", b"target", &scratch_link)
            .unwrap();

        // Case-insensitive lookup and symlink target read.
        assert_eq!(
            meta.lookup_scratch_entry(tracked_dir.ino, b"temp.tmp")
                .unwrap(),
            Some(101)
        );
        assert_eq!(meta.get_scratch_symlink_target(103).unwrap(), b"target");
        assert!(meta.has_scratch_children(tracked_dir.ino).unwrap());

        // Tracked rmdir fails with DirNotEmpty when scratch children exist.
        assert!(matches!(
            meta.rmdir(ROOT_INO, b"tracked"),
            Err(MetaError::DirNotEmpty(ino)) if ino == tracked_dir.ino
        ));

        // Commit overlay, lookup slices, truncate partially, and check displaced objects.
        meta.commit_scratch_overlay(
            101,
            &[
                (ChunkId(0), vec![slice(501, 0, 16)]),
                (ChunkId(1), vec![slice(502, 0, 16)]),
            ],
        )
        .unwrap();
        let spans = meta.lookup_scratch_slices(101, 0, 16).unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].payload.unwrap().object_id, 501);
        let mut trunc_attr = meta.get_scratch_attr(101).unwrap();
        trunc_attr.size = 8;
        let displaced = meta.truncate_scratch(101, 8, &trunc_attr).unwrap();
        assert_eq!(displaced, vec![502]);
        assert_eq!(meta.get_scratch_attr(101).unwrap().size, 8);

        // Xattrs with flags and list/remove.
        meta.set_scratch_xattr_with_flags(101, b"user.a", b"1", XATTR_CREATE_FLAG)
            .unwrap();
        assert!(matches!(
            meta.set_scratch_xattr_with_flags(101, b"user.a", b"2", XATTR_CREATE_FLAG),
            Err(MetaError::EntryExists { .. })
        ));
        meta.set_scratch_xattr_with_flags(101, b"user.a", b"2", XATTR_REPLACE_FLAG)
            .unwrap();
        assert_eq!(
            meta.list_scratch_xattrs(101).unwrap(),
            vec![b"user.a".to_vec()]
        );
        meta.remove_scratch_xattr(101, b"user.a").unwrap();
        assert!(meta.list_scratch_xattrs(101).unwrap().is_empty());

        // Open-unlink keeps inode and overlay until finalize_orphan_scratch_inode.
        let (unlinked, freed_now) = meta
            .unlink_scratch_entry(tracked_dir.ino, b"Temp.tmp", true)
            .unwrap();
        assert_eq!(unlinked.nlink, 0);
        assert!(freed_now.is_empty());
        assert_eq!(meta.get_scratch_attr(101).unwrap().nlink, 0);
        let freed_late = meta.finalize_orphan_scratch_inode(101).unwrap();
        assert_eq!(freed_late, vec![501]);
        assert!(matches!(
            meta.get_scratch_attr(101),
            Err(MetaError::InodeNotFound(101))
        ));

        // Non-empty scratch rmdir fails; after unlinking child it succeeds.
        assert!(matches!(
            meta.rmdir_scratch_entry(tracked_dir.ino, b"sub"),
            Err(MetaError::DirNotEmpty(102))
        ));
        let (_, _) = meta
            .unlink_scratch_entry(scratch_dir.ino, b"lnk", false)
            .unwrap();
        meta.rmdir_scratch_entry(tracked_dir.ino, b"sub").unwrap();
        assert!(!meta.has_scratch_children(tracked_dir.ino).unwrap());

        // Zero events emitted by all scratch operations.
        assert_eq!(
            meta.read_events_range(0, u64::MAX).unwrap().len(),
            events_before
        );

        // Index reloads cleanly across reopen.
        let persisted = InodeAttr::new_file(104, tracked_dir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(tracked_dir.ino, b"keep.tmp", &persisted)
            .unwrap();
        drop(meta);
        let reopened = RedbMetaEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened
                .lookup_scratch_entry(tracked_dir.ino, b"KEEP.TMP")
                .unwrap(),
            Some(104)
        );
        assert_eq!(reopened.readdir_scratch(tracked_dir.ino).unwrap().len(), 1);
    }

    #[test]
    fn replay_tracked_add_and_rename_conflict_renames_scratch_entry_and_records_status() {
        use crate::events::MetaEvent;

        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();

        let scratch_temp = InodeAttr::new_file(101, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"Temp.tmp", &scratch_temp)
            .unwrap();

        let scratch_ren = InodeAttr::new_file(102, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"ren.tmp", &scratch_ren)
            .unwrap();
        // Pre-populate .scratch-conflict-1 so the second conflict advances to -2.
        let taken_conflict = InodeAttr::new_file(103, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"ren.tmp.scratch-conflict-1", &taken_conflict)
            .unwrap();

        let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();

        // Peer inserts tracked `temp.tmp` (case-insensitive collision with `Temp.tmp`).
        let mut peer_temp = InodeAttr::new_file(201, ROOT_INO, 0, 0, 0o644);
        peer_temp.upload_state = UploadState::Updating;
        let peer_dir_src = InodeAttr::new_file(202, ROOT_INO, 0, 0, 0o644);
        meta.apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: peer_temp.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"temp.tmp".to_vec(),
                entry: DirEntryValue {
                    ino: peer_temp.ino,
                    kind: FileType::RegularFile,
                },
            },
            MetaEvent::SetAttr {
                attr: peer_dir_src.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"staging.dat".to_vec(),
                entry: DirEntryValue {
                    ino: peer_dir_src.ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();

        // Tracked `temp.tmp` wins original name; scratch `Temp.tmp` moved to `Temp.tmp.scratch-conflict-1`.
        assert_eq!(meta.lookup(ROOT_INO, b"temp.tmp").unwrap().ino, 201);
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"Temp.tmp").unwrap(),
            None
        );
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"Temp.tmp.scratch-conflict-1")
                .unwrap(),
            Some(101)
        );

        // Peer renames `staging.dat` -> `ren.tmp`, colliding with scratch `ren.tmp`.
        meta.apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: peer_dir_src.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"ren.tmp".to_vec(),
                entry: DirEntryValue {
                    ino: peer_dir_src.ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();
        meta.apply_replica_serving_state(&[MetaEvent::RenameEntry {
            parent: ROOT_INO,
            name: b"staging.dat".to_vec(),
            new_parent: ROOT_INO,
            new_name: b"ren.tmp".to_vec(),
            entry: DirEntryValue {
                ino: peer_dir_src.ino,
                kind: FileType::RegularFile,
            },
            displaced_set_attr: Box::new(None),
            displaced_remove_attr_ino: None,
            moved_dir_attr: Box::new(None),
            old_parent_attr: Box::new(None),
            new_parent_attr: Box::new(None),
        }])
        .unwrap();

        assert_eq!(meta.lookup(ROOT_INO, b"ren.tmp").unwrap().ino, 202);
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"ren.tmp.scratch-conflict-2")
                .unwrap(),
            Some(102)
        );

        let conflicts = meta.list_scratch_conflicts().unwrap();
        assert_eq!(conflicts.len(), 2);
        let c1 = conflicts.iter().find(|c| c.ino == 101).unwrap();
        assert_eq!(c1.original_name, b"Temp.tmp");
        assert_eq!(c1.conflict_name, b"Temp.tmp.scratch-conflict-1");
        assert_eq!(c1.winning_ino, 201);
        let c2 = conflicts.iter().find(|c| c.ino == 102).unwrap();
        assert_eq!(c2.original_name, b"ren.tmp");
        assert_eq!(c2.conflict_name, b"ren.tmp.scratch-conflict-2");
        assert_eq!(c2.winning_ino, 202);

        // Zero event_log rows published by conflict resolution.
        assert_eq!(
            meta.read_events_range(0, u64::MAX).unwrap().len(),
            events_before
        );

        // Clearing / unlinking removes conflict status records.
        assert!(meta.clear_scratch_conflict(101).unwrap());
        meta.unlink_scratch_entry(ROOT_INO, b"ren.tmp.scratch-conflict-2", false)
            .unwrap();
        assert!(meta.list_scratch_conflicts().unwrap().is_empty());
    }

    #[test]
    fn foreign_apply_preserves_local_authority_when_tracked_entry_was_locally_unlinked() {
        use crate::events::MetaEvent;
        use crate::pg_engine::{ScratchTestReplayInput, apply_decoded_batches_for_scratch_test};

        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();

        // Install replicated tracked file `local-del.tmp` (`ino = 500`) in base and serving.
        let mut peer_attr = InodeAttr::new_file(500, ROOT_INO, 0, 0, 0o644);
        peer_attr.upload_state = UploadState::Updating;
        meta.apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: peer_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"local-del.tmp".to_vec(),
                entry: DirEntryValue {
                    ino: 500,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();
        meta.apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: peer_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"local-del.tmp".to_vec(),
                entry: DirEntryValue {
                    ino: 500,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();

        // Local client unlinks the tracked file (creating local dentry tombstone + overlay) and
        // creates a new local scratch file with the same name (`ino = 600`).
        meta.unlink(ROOT_INO, b"local-del.tmp").unwrap();
        let scratch_attr = InodeAttr::new_file(600, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"local-del.tmp", &scratch_attr)
            .unwrap();

        // Stale foreign batch touches `(ROOT_INO, b"local-del.tmp")` while local overlay protects it.
        let next_seq = meta
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        apply_decoded_batches_for_scratch_test(
            &meta,
            ScratchTestReplayInput {
                batches: vec![(
                    2,
                    vec![
                        MetaEvent::SetAttr {
                            attr: peer_attr.clone(),
                        },
                        MetaEvent::AddEntry {
                            parent: ROOT_INO,
                            name: b"local-del.tmp".to_vec(),
                            entry: DirEntryValue {
                                ino: 500,
                                kind: FileType::RegularFile,
                            },
                        },
                    ],
                    false,
                )],
                last_seq: 1,
                current_namespace_version: 1,
                flushed_event_seq: 1,
                next_event_seq: next_seq,
                protected_dentry_keys: vec![encode_dentry_key(ROOT_INO, b"local-del.tmp")],
                protected_attr_inodes: vec![500],
            },
        )
        .unwrap();

        // Local tombstone protects local authority; scratch entry must NOT be conflict-renamed.
        assert!(matches!(
            meta.lookup(ROOT_INO, b"local-del.tmp"),
            Err(MetaError::EntryNotFound { .. })
        ));
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"local-del.tmp")
                .unwrap(),
            Some(600)
        );
        assert!(meta.list_scratch_conflicts().unwrap().is_empty());
    }

    #[test]
    fn replay_parent_removal_relocates_scratch_subtree_to_orphans_and_preserves_payload_and_metadata()
     {
        use crate::events::MetaEvent;

        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let cache_dir = temp.path().join("custom-cache");
        meta.set_scratch_cache_dir(cache_dir.clone()).unwrap();
        meta.init_root(0, 0).unwrap();

        let work = InodeAttr::new_dir(10, ROOT_INO, 0, 0, 0o755);
        let build = InodeAttr::new_dir(11, work.ino, 0, 0, 0o755);
        meta.apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr: work.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"work".to_vec(),
                entry: DirEntryValue {
                    ino: work.ino,
                    kind: FileType::Directory,
                },
            },
            MetaEvent::SetAttr {
                attr: build.clone(),
            },
            MetaEvent::AddEntry {
                parent: work.ino,
                name: b"build".to_vec(),
                entry: DirEntryValue {
                    ino: build.ino,
                    kind: FileType::Directory,
                },
            },
        ])
        .unwrap();

        // Write actual payload object into cache layout so disk materialization restores bytes too.
        let payload_path = local_payload_object_path(&cache_dir, 701);
        std::fs::create_dir_all(payload_path.parent().unwrap()).unwrap();
        std::fs::write(&payload_path, b"object-701-bytes").unwrap();
        let dep_payload_path = local_payload_object_path(&cache_dir, 702);
        std::fs::create_dir_all(dep_payload_path.parent().unwrap()).unwrap();
        std::fs::write(&dep_payload_path, b"dep-702").unwrap();

        let app_attr = InodeAttr::new_file(101, build.ino, 1000, 1000, 0o640);
        meta.create_scratch_entry(build.ino, b"app.o", &app_attr)
            .unwrap();
        meta.commit_scratch_overlay(101, &[(ChunkId(0), vec![slice(701, 0, 16)])])
            .unwrap();
        meta.set_scratch_xattr(101, b"user.scratch", b"keep-me")
            .unwrap();

        let deps_attr = InodeAttr::new_dir(102, build.ino, 0, 0, 0o755);
        meta.create_scratch_entry(build.ino, b"deps", &deps_attr)
            .unwrap();
        let lib_attr = InodeAttr::new_file(103, deps_attr.ino, 0, 0, 0o644);
        meta.create_scratch_entry(deps_attr.ino, b"lib.a", &lib_attr)
            .unwrap();
        meta.commit_scratch_overlay(103, &[(ChunkId(0), vec![slice(702, 0, 7)])])
            .unwrap();
        let symlink_attr = InodeAttr::new_symlink(104, deps_attr.ino, 0, 0, 5);
        meta.create_scratch_symlink(deps_attr.ino, b"latest", b"lib.a", &symlink_attr)
            .unwrap();

        let events_before = meta.read_events_range(0, u64::MAX).unwrap().len();

        // Peer removes `/work/build`.
        meta.apply_replica_serving_state(&[
            MetaEvent::RemoveAttr { ino: build.ino },
            MetaEvent::RemoveEntry {
                parent: work.ino,
                name: b"build".to_vec(),
            },
        ])
        .unwrap();

        // Relocated outside the active scratch namespace with zero event_log emissions.
        assert_eq!(
            meta.read_events_range(0, u64::MAX).unwrap().len(),
            events_before
        );
        assert!(!meta.has_scratch_children(build.ino).unwrap());
        assert!(matches!(
            meta.get_scratch_attr(101),
            Err(MetaError::InodeNotFound(101))
        ));
        assert!(matches!(
            meta.get_scratch_attr(102),
            Err(MetaError::InodeNotFound(102))
        ));

        // Local GC cache roots still protect payload object IDs 701 and 702.
        let cache_roots = meta.get_local_cache_referenced_object_ids().unwrap();
        assert!(cache_roots.contains(&701));
        assert!(cache_roots.contains(&702));

        let orphans = meta.list_scratch_orphans().unwrap();
        assert_eq!(orphans.len(), 2);
        let app_orphan = orphans
            .iter()
            .find(|r| r.root_old_path == b"work/build/app.o")
            .unwrap();
        assert!(app_orphan.materialized);
        assert_eq!(app_orphan.entries.len(), 1);
        assert_eq!(
            app_orphan.entries[0].xattrs,
            vec![(b"user.scratch".to_vec(), b"keep-me".to_vec())]
        );

        let deps_orphan = orphans
            .iter()
            .find(|r| r.root_old_path == b"work/build/deps")
            .unwrap();
        assert!(deps_orphan.materialized);
        assert_eq!(deps_orphan.entries.len(), 3);

        // Verify on-disk orphan layout at `<cache>/scratch-orphans/<timestamp>/<old path>`.
        let orphans_dir = meta.scratch_orphans_dir().unwrap();
        let disk_app = orphans_dir
            .join(app_orphan.timestamp_unix_ms.to_string())
            .join("work/build/app.o");
        assert_eq!(std::fs::read(&disk_app).unwrap(), b"object-701-bytes");
        let disk_lib = orphans_dir
            .join(deps_orphan.timestamp_unix_ms.to_string())
            .join("work/build/deps/lib.a");
        assert_eq!(std::fs::read(&disk_lib).unwrap(), b"dep-702");
        let disk_link = orphans_dir
            .join(deps_orphan.timestamp_unix_ms.to_string())
            .join("work/build/deps/latest");
        assert!(disk_link.exists());

        // Restore subtree back into the scratch layer (auto-recreating `build` as scratch dir).
        let restored_deps = meta
            .restore_scratch_orphan(
                deps_orphan.timestamp_unix_ms,
                b"work/build/deps",
                None,
                None,
            )
            .unwrap();
        assert_eq!(restored_deps.ino, 102);
        let restored_build_ino = meta
            .lookup_scratch_entry(work.ino, b"build")
            .unwrap()
            .unwrap();
        assert_eq!(
            meta.lookup_scratch_entry(restored_build_ino, b"deps")
                .unwrap(),
            Some(102)
        );
        assert_eq!(meta.lookup_scratch_entry(102, b"lib.a").unwrap(), Some(103));
        assert_eq!(meta.get_scratch_symlink_target(104).unwrap(), b"lib.a");
        assert!(!disk_lib.exists());

        let restored_app = meta
            .restore_scratch_orphan(
                app_orphan.timestamp_unix_ms,
                b"work/build/app.o",
                None,
                None,
            )
            .unwrap();
        assert_eq!(restored_app.ino, 101);
        assert_eq!(
            meta.get_scratch_xattr(101, b"user.scratch").unwrap(),
            Some(b"keep-me".to_vec())
        );
        assert!(meta.list_scratch_orphans().unwrap().is_empty());
        assert!(!disk_app.exists());
    }

    #[test]
    fn snapshot_restore_relocates_orphaned_scratch_entries_and_resolves_conflicts() {
        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        let cache_dir = temp.path().join("cache");
        meta.set_scratch_cache_dir(cache_dir.clone()).unwrap();
        let payload_path = local_payload_object_path(&cache_dir, 801);
        std::fs::create_dir_all(payload_path.parent().unwrap()).unwrap();
        std::fs::write(&payload_path, b"object-801!!").unwrap();
        meta.init_root(0, 0).unwrap();

        let gone = meta.create_dir(ROOT_INO, b"gone", 0, 0, 0o755).unwrap();
        let gone_sub = meta.create_dir(gone.ino, b"sub", 0, 0, 0o755).unwrap();

        let cache_file = InodeAttr::new_file(301, gone_sub.ino, 0, 0, 0o644);
        meta.create_scratch_entry(gone_sub.ino, b"cache.tmp", &cache_file)
            .unwrap();
        meta.commit_scratch_overlay(301, &[(ChunkId(0), vec![slice(801, 0, 12)])])
            .unwrap();

        let root_scratch = InodeAttr::new_file(302, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"readme.tmp", &root_scratch)
            .unwrap();

        // Build a remote snapshot that omits `gone` and adds a tracked `readme.tmp`.
        let remote_dir = tempfile::tempdir().unwrap();
        let remote = RedbMetaEngine::open(remote_dir.path().join("remote.redb")).unwrap();
        remote.init_root(0, 0).unwrap();
        let tracked_readme = remote
            .create_file(ROOT_INO, b"readme.tmp", 0, 0, 0o644)
            .unwrap();
        let snapshot = remote.export_snapshot().unwrap();

        meta.import_remote_snapshot(&snapshot).unwrap();

        // `readme.tmp` tracked entry wins; scratch entry renamed to `readme.tmp.scratch-conflict-1`.
        assert_eq!(
            meta.lookup(ROOT_INO, b"readme.tmp").unwrap().ino,
            tracked_readme.ino
        );
        assert_eq!(
            meta.lookup_scratch_entry(ROOT_INO, b"readme.tmp.scratch-conflict-1")
                .unwrap(),
            Some(302)
        );
        assert_eq!(meta.list_scratch_conflicts().unwrap().len(), 1);

        // `gone/sub/cache.tmp` was orphaned and kept object 801 live in cache GC roots.
        let orphans = meta.list_scratch_orphans().unwrap();
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].root_old_path, b"gone/sub/cache.tmp");
        assert!(orphans[0].materialized);
        assert!(
            meta.get_local_cache_referenced_object_ids()
                .unwrap()
                .contains(&801)
        );
    }

    #[test]
    fn replay_parent_rename_and_adr_0012_directory_merge_carry_and_reparent_scratch_children() {
        use crate::events::MetaEvent;
        use crate::pg_engine::{ScratchTestReplayInput, apply_decoded_batches_for_scratch_test};

        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();

        // Part 1: Peer renames tracked parent `/src` -> `/source`; scratch child stays attached.
        let src_dir = InodeAttr::new_dir(40, ROOT_INO, 0, 0, 0o755);
        meta.apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: src_dir.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"src".to_vec(),
                entry: DirEntryValue {
                    ino: src_dir.ino,
                    kind: FileType::Directory,
                },
            },
        ])
        .unwrap();
        meta.apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: src_dir.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"src".to_vec(),
                entry: DirEntryValue {
                    ino: src_dir.ino,
                    kind: FileType::Directory,
                },
            },
        ])
        .unwrap();
        let scratch_child = InodeAttr::new_file(401, src_dir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(src_dir.ino, b"build.tmp", &scratch_child)
            .unwrap();

        let rename_event = MetaEvent::RenameEntry {
            parent: ROOT_INO,
            name: b"src".to_vec(),
            new_parent: ROOT_INO,
            new_name: b"source".to_vec(),
            entry: DirEntryValue {
                ino: src_dir.ino,
                kind: FileType::Directory,
            },
            displaced_set_attr: Box::new(None),
            displaced_remove_attr_ino: None,
            moved_dir_attr: Box::new(None),
            old_parent_attr: Box::new(None),
            new_parent_attr: Box::new(None),
        };
        meta.apply_replica_events_to_base(std::slice::from_ref(&rename_event))
            .unwrap();
        meta.apply_replica_serving_state(&[rename_event]).unwrap();

        assert_eq!(meta.lookup(ROOT_INO, b"source").unwrap().ino, src_dir.ino);
        assert_eq!(
            meta.lookup_scratch_entry(src_dir.ino, b"build.tmp")
                .unwrap(),
            Some(401)
        );
        assert!(meta.list_scratch_orphans().unwrap().is_empty());

        // Part 2: ADR 0012 concurrent same-name directory merge re-parents scratch children.
        let flushed_before = meta
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        let local_merged = meta.create_dir(ROOT_INO, b"merged", 0, 0, 0o755).unwrap();
        let end_local = meta
            .get_counter(crate::schema::counters::NEXT_EVENT_SEQ)
            .unwrap();
        let self_events = meta.read_events_range(flushed_before, end_local).unwrap();

        let scratch_in_merged = InodeAttr::new_file(402, local_merged.ino, 0, 0, 0o644);
        meta.create_scratch_entry(local_merged.ino, b"note.tmp", &scratch_in_merged)
            .unwrap();

        let peer_canonical = InodeAttr::new_dir(250, ROOT_INO, 0, 0, 0o755);
        let mut peer_tracked_colliding = InodeAttr::new_file(251, peer_canonical.ino, 0, 0, 0o644);
        peer_tracked_colliding.upload_state = UploadState::Updating;

        apply_decoded_batches_for_scratch_test(
            &meta,
            ScratchTestReplayInput {
                batches: vec![
                    (
                        1,
                        vec![
                            MetaEvent::SetAttr {
                                attr: peer_canonical.clone(),
                            },
                            MetaEvent::AddEntry {
                                parent: ROOT_INO,
                                name: b"merged".to_vec(),
                                entry: DirEntryValue {
                                    ino: peer_canonical.ino,
                                    kind: FileType::Directory,
                                },
                            },
                            MetaEvent::SetAttr {
                                attr: peer_tracked_colliding.clone(),
                            },
                            MetaEvent::AddEntry {
                                parent: peer_canonical.ino,
                                name: b"note.tmp".to_vec(),
                                entry: DirEntryValue {
                                    ino: peer_tracked_colliding.ino,
                                    kind: FileType::RegularFile,
                                },
                            },
                        ],
                        false,
                    ),
                    (2, self_events, true),
                ],
                last_seq: 0,
                current_namespace_version: 0,
                flushed_event_seq: flushed_before,
                next_event_seq: end_local,
                protected_dentry_keys: vec![encode_dentry_key(ROOT_INO, b"merged")],
                protected_attr_inodes: vec![local_merged.ino],
            },
        )
        .unwrap();

        // Local directory merged into canonical 250; scratch child re-parented to 250 and
        // conflict-renamed against peer's tracked `note.tmp` (251).
        assert_eq!(meta.lookup(ROOT_INO, b"merged").unwrap().ino, 250);
        assert_eq!(meta.lookup(250, b"note.tmp").unwrap().ino, 251);
        assert_eq!(
            meta.lookup_scratch_entry(local_merged.ino, b"note.tmp")
                .unwrap(),
            None
        );
        assert_eq!(
            meta.lookup_scratch_entry(250, b"note.tmp.scratch-conflict-1")
                .unwrap(),
            Some(402)
        );
        assert_eq!(meta.get_scratch_attr(402).unwrap().parent, 250);
        assert!(meta.list_scratch_orphans().unwrap().is_empty());
    }

    #[test]
    fn adr_0012_alias_merge_scratch_collision_records_existing_scratch_winner() {
        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();

        let canonical = meta
            .create_dir(ROOT_INO, b"canonical", 0, 0, 0o755)
            .unwrap();
        let alias = meta.create_dir(ROOT_INO, b"alias", 0, 0, 0o755).unwrap();
        let existing = InodeAttr::new_file(601, canonical.ino, 0, 0, 0o644);
        meta.create_scratch_entry(canonical.ino, b"a.tmp", &existing)
            .unwrap();
        let moved = InodeAttr::new_file(602, alias.ino, 0, 0, 0o644);
        meta.create_scratch_entry(alias.ino, b"a.tmp", &moved)
            .unwrap();

        let txn = meta.begin_write_txn().unwrap();
        let pre = meta
            .capture_pre_mutation_scratch_context_in_txn(&txn)
            .unwrap();
        // Merge: the alias directory loses its path and resolves to the canonical one.
        RedbMetaEngine::apply_replica_serving_state_in_txn(
            &txn,
            &[crate::events::MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"alias".to_vec(),
            }],
            false,
        )
        .unwrap();
        txn.open_table(DIRECTORY_ALIASES_TABLE)
            .unwrap()
            .insert(alias.ino, canonical.ino)
            .unwrap();
        let outcome = meta
            .reconcile_scratch_after_tracked_mutations_in_txn(&txn, &pre)
            .unwrap();
        txn.commit().unwrap();
        meta.finish_scratch_reconciliation(outcome).unwrap();

        assert_eq!(
            meta.lookup_scratch_entry(canonical.ino, b"a.tmp.scratch-conflict-1")
                .unwrap(),
            Some(602)
        );
        let conflicts = meta.list_scratch_conflicts().unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].ino, 602);
        // The existing scratch entry kept the name, not the directory inode.
        assert_eq!(conflicts[0].winning_ino, 601);
    }

    #[test]
    fn orphan_relocation_is_crash_safe_across_uncommitted_abort_and_post_commit_recovery() {
        use crate::events::MetaEvent;

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("meta.redb");
        let meta = RedbMetaEngine::open(&db_path).unwrap();
        meta.init_root(0, 0).unwrap();

        let parent_dir = InodeAttr::new_dir(50, ROOT_INO, 0, 0, 0o755);
        meta.apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: parent_dir.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"dir".to_vec(),
                entry: DirEntryValue {
                    ino: parent_dir.ino,
                    kind: FileType::Directory,
                },
            },
        ])
        .unwrap();

        let scratch_file = InodeAttr::new_file(501, parent_dir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(parent_dir.ino, b"work.tmp", &scratch_file)
            .unwrap();
        meta.commit_scratch_overlay(501, &[(ChunkId(0), vec![slice(901, 0, 10)])])
            .unwrap();

        // Case 1: Simulated crash before redb commit leaves the entry in the namespace.
        {
            let txn = meta.begin_write_txn().unwrap();
            let pre = meta
                .capture_pre_mutation_scratch_context_in_txn(&txn)
                .unwrap();
            RedbMetaEngine::apply_replica_serving_state_in_txn(
                &txn,
                &[
                    MetaEvent::RemoveAttr {
                        ino: parent_dir.ino,
                    },
                    MetaEvent::RemoveEntry {
                        parent: ROOT_INO,
                        name: b"dir".to_vec(),
                    },
                ],
                false,
            )
            .unwrap();
            let outcome = meta
                .reconcile_scratch_after_tracked_mutations_in_txn(&txn, &pre)
                .unwrap();
            assert_eq!(outcome.orphans_created, 1);
            txn.abort().unwrap();
        }
        assert_eq!(
            meta.lookup_scratch_entry(parent_dir.ino, b"work.tmp")
                .unwrap(),
            Some(501)
        );
        assert!(meta.list_scratch_orphans().unwrap().is_empty());

        // Case 2: Simulated crash immediately after redb commit (materialized == false + stale
        // staging dir left behind) recovers cleanly into the orphan area upon reopen.
        let orphan_ts;
        {
            let txn = meta.begin_write_txn().unwrap();
            let pre = meta
                .capture_pre_mutation_scratch_context_in_txn(&txn)
                .unwrap();
            RedbMetaEngine::apply_replica_serving_state_in_txn(
                &txn,
                &[
                    MetaEvent::RemoveAttr {
                        ino: parent_dir.ino,
                    },
                    MetaEvent::RemoveEntry {
                        parent: ROOT_INO,
                        name: b"dir".to_vec(),
                    },
                ],
                false,
            )
            .unwrap();
            let outcome = meta
                .reconcile_scratch_after_tracked_mutations_in_txn(&txn, &pre)
                .unwrap();
            assert_eq!(outcome.orphans_created, 1);
            txn.commit().unwrap();

            let orphans = meta.list_scratch_orphans().unwrap();
            assert_eq!(orphans.len(), 1);
            assert!(!orphans[0].materialized);
            orphan_ts = orphans[0].timestamp_unix_ms;
            let partial_staging = meta.scratch_orphans_dir().unwrap().join(format!(
                ".staging-{orphan_ts}-{}",
                fnv1a64_hex(b"dir/work.tmp")
            ));
            std::fs::create_dir_all(&partial_staging).unwrap();
            std::fs::write(partial_staging.join("partial"), b"stale").unwrap();
        }
        drop(meta);

        let reopened = RedbMetaEngine::open(&db_path).unwrap();
        assert_eq!(
            reopened
                .lookup_scratch_entry(parent_dir.ino, b"work.tmp")
                .unwrap(),
            None
        );
        // Opening alone must not materialize against a guessed cache root.
        let orphans = reopened.list_scratch_orphans().unwrap();
        assert_eq!(orphans.len(), 1);
        assert!(!orphans[0].materialized);
        // A missing payload object leaves the orphan pending instead of zero-filling it.
        let cache_dir = temp.path().join("cache");
        reopened.set_scratch_cache_dir(cache_dir.clone()).unwrap();
        assert!(!reopened.list_scratch_orphans().unwrap()[0].materialized);
        let payload_path = local_payload_object_path(&cache_dir, 901);
        std::fs::create_dir_all(payload_path.parent().unwrap()).unwrap();
        std::fs::write(&payload_path, b"object-901").unwrap();
        reopened.set_scratch_cache_dir(cache_dir).unwrap();
        let orphans = reopened.list_scratch_orphans().unwrap();
        assert!(orphans[0].materialized);
        assert_eq!(orphans[0].root_old_path, b"dir/work.tmp");
        let disk_path = reopened
            .scratch_orphans_dir()
            .unwrap()
            .join(orphan_ts.to_string())
            .join("dir/work.tmp");
        assert_eq!(std::fs::read(&disk_path).unwrap(), b"object-901");
    }

    #[test]
    fn scratch_auto_promote_emits_add_entry_preserves_inodes_and_conflict_renames() {
        use crate::contract_harness::{
            project_local_events_for_replay, replay_ordered_batches_with_adr_0012,
        };
        use crate::events::MetaEvent;
        use crate::schema::counters::NEXT_EVENT_SEQ;

        let temp = tempfile::tempdir().unwrap();
        let meta = RedbMetaEngine::open(temp.path().join("meta.redb")).unwrap();
        meta.init_root(0, 0).unwrap();

        // Scratch dir with child file + symlink; plus a tracked sibling and nested scratch child.
        let sdir = InodeAttr::new_dir(200, ROOT_INO, 0, 0, 0o755);
        meta.create_scratch_entry(ROOT_INO, b"shared", &sdir)
            .unwrap();
        meta.set_scratch_xattr(sdir.ino, b"user.dir", b"dir-xattr")
            .unwrap();
        let nested_file = InodeAttr::new_file(201, sdir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(sdir.ino, b"child.bin", &nested_file)
            .unwrap();
        meta.commit_scratch_overlay(nested_file.ino, &[(ChunkId(0), vec![slice(801, 0, 12)])])
            .unwrap();
        let mut sized_child = nested_file.clone();
        sized_child.size = 12;
        meta.set_scratch_attr(&sized_child).unwrap();
        let nested_link = InodeAttr::new_symlink(202, sdir.ino, 0, 0, 9);
        meta.create_scratch_symlink(sdir.ino, b"link", b"child.bin", &nested_link)
            .unwrap();

        let tracked_entries = meta.list_tracked_parent_scratch_entries().unwrap();
        assert_eq!(tracked_entries.len(), 1);
        assert_eq!(tracked_entries[0].0, ROOT_INO);
        assert_eq!(tracked_entries[0].1, b"shared");
        assert_eq!(tracked_entries[0].2.ino, sdir.ino);

        let before_seq = meta.get_counter(NEXT_EVENT_SEQ).unwrap();
        let frozen = HashSet::from([sdir.ino, nested_file.ino, nested_link.ino]);
        let promoted = meta
            .promote_scratch_auto(ROOT_INO, b"shared", sdir.ino, &frozen, "client-a")
            .unwrap();
        assert_eq!(promoted.promoted_ino, sdir.ino);
        assert_eq!(promoted.final_name, b"shared");
        assert!(!promoted.conflict_renamed);
        assert_eq!(promoted.attrs.len(), 3);
        assert!(
            meta.list_tracked_parent_scratch_entries()
                .unwrap()
                .is_empty()
        );

        assert_eq!(meta.lookup(ROOT_INO, b"shared").unwrap().ino, sdir.ino);
        assert_eq!(
            meta.lookup(sdir.ino, b"child.bin").unwrap().ino,
            nested_file.ino
        );
        assert_eq!(
            meta.get_symlink_target(nested_link.ino).unwrap(),
            b"child.bin"
        );
        assert_eq!(
            meta.get_xattr(sdir.ino, b"user.dir").unwrap().as_deref(),
            Some(b"dir-xattr".as_slice())
        );

        let after_seq = meta.get_counter(NEXT_EVENT_SEQ).unwrap();
        let events = meta.read_events_range(before_seq, after_seq).unwrap();
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, MetaEvent::RenameEntry { .. })),
            "auto-promote must emit AddEntry rather than RenameEntry"
        );
        let root_add_idx = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    MetaEvent::AddEntry { parent, name, entry }
                        if *parent == ROOT_INO && name == b"shared" && entry.ino == sdir.ino
                )
            })
            .expect("root AddEntry");
        let child_add_idx = events
            .iter()
            .position(|event| {
                matches!(
                    event,
                    MetaEvent::AddEntry { parent, name, .. }
                        if *parent == sdir.ino && name == b"child.bin"
                )
            })
            .expect("child AddEntry");
        let dir_xattr_idx = events
            .iter()
            .position(|event| matches!(event, MetaEvent::SetXattr { ino, .. } if *ino == sdir.ino))
            .expect("dir SetXattr");
        assert!(root_add_idx < child_add_idx);
        assert!(root_add_idx < dir_xattr_idx);

        // Taken name in tracked base renames to `<name>.scratch-conflict-<client>` and then `-2`.
        let sfile1 = InodeAttr::new_file(203, ROOT_INO, 0, 0, 0o644);
        meta.create_scratch_entry(ROOT_INO, b"taken.tmp", &sfile1)
            .unwrap();
        let tracked_subdir = meta
            .create_dir(ROOT_INO, b"taken_sub", 0, 0, 0o755)
            .unwrap();
        meta.create_file(
            tracked_subdir.ino,
            b"taken.tmp.scratch-conflict-client-1",
            0,
            0,
            0o644,
        )
        .unwrap();
        let sfile2 = InodeAttr::new_file(204, tracked_subdir.ino, 0, 0, 0o644);
        meta.create_scratch_entry(tracked_subdir.ino, b"taken.tmp", &sfile2)
            .unwrap();

        // Populate tracked `taken.tmp` directly in the local tracked base (without running foreign-apply
        // scratch reconciliation) so `promote_scratch_auto` performs the taken-name conflict rename itself.
        let tracked_root_taken = meta
            .create_file(ROOT_INO, b"taken.tmp", 0, 0, 0o644)
            .unwrap();
        let tracked_sub_taken = meta
            .create_file(tracked_subdir.ino, b"taken.tmp", 0, 0, 0o644)
            .unwrap();

        let res1 = meta
            .promote_scratch_auto(
                ROOT_INO,
                b"taken.tmp",
                sfile1.ino,
                &HashSet::from([sfile1.ino]),
                "client/1",
            )
            .unwrap();
        assert!(res1.conflict_renamed);
        assert_eq!(res1.final_name, b"taken.tmp.scratch-conflict-client-1");
        assert_eq!(
            meta.lookup(ROOT_INO, b"taken.tmp.scratch-conflict-client-1")
                .unwrap()
                .ino,
            sfile1.ino
        );
        assert_eq!(
            meta.lookup(ROOT_INO, b"taken.tmp").unwrap().ino,
            tracked_root_taken.ino
        );

        let res2 = meta
            .promote_scratch_auto(
                tracked_subdir.ino,
                b"taken.tmp",
                sfile2.ino,
                &HashSet::from([sfile2.ino]),
                "client/1",
            )
            .unwrap();
        assert!(res2.conflict_renamed);
        assert_eq!(res2.final_name, b"taken.tmp.scratch-conflict-client-1-2");
        assert_eq!(
            meta.lookup(tracked_subdir.ino, b"taken.tmp.scratch-conflict-client-1-2")
                .unwrap()
                .ino,
            sfile2.ino
        );
        assert_eq!(
            meta.lookup(tracked_subdir.ino, b"taken.tmp").unwrap().ino,
            tracked_sub_taken.ino
        );

        // Concurrent directory auto-promote from two clients merges under ADR 0012.
        let temp_a = tempfile::tempdir().unwrap();
        let client_a = RedbMetaEngine::open(temp_a.path().join("a.redb")).unwrap();
        client_a.init_root(0, 0).unwrap();
        let temp_b = tempfile::tempdir().unwrap();
        let client_b = RedbMetaEngine::open(temp_b.path().join("b.redb")).unwrap();
        client_b.init_root(0, 0).unwrap();

        let dir_a = InodeAttr::new_dir(1000, ROOT_INO, 0, 0, 0o755);
        client_a
            .create_scratch_entry(ROOT_INO, b"merge", &dir_a)
            .unwrap();
        let file_a = InodeAttr::new_file(1001, dir_a.ino, 0, 0, 0o644);
        client_a
            .create_scratch_entry(dir_a.ino, b"a.txt", &file_a)
            .unwrap();
        client_a
            .commit_scratch_overlay(file_a.ino, &[(ChunkId(0), vec![slice(1001, 0, 4)])])
            .unwrap();
        let mut a_ready = file_a.clone();
        a_ready.size = 4;
        a_ready.content_epoch = 1;
        client_a.set_scratch_attr(&a_ready).unwrap();

        let dir_b = InodeAttr::new_dir(2000, ROOT_INO, 0, 0, 0o755);
        client_b
            .create_scratch_entry(ROOT_INO, b"merge", &dir_b)
            .unwrap();
        let file_b = InodeAttr::new_file(2001, dir_b.ino, 0, 0, 0o644);
        client_b
            .create_scratch_entry(dir_b.ino, b"b.txt", &file_b)
            .unwrap();
        client_b
            .commit_scratch_overlay(file_b.ino, &[(ChunkId(0), vec![slice(2001, 0, 4)])])
            .unwrap();
        let mut b_ready = file_b.clone();
        b_ready.size = 4;
        b_ready.content_epoch = 1;
        client_b.set_scratch_attr(&b_ready).unwrap();

        let a_start = client_a.get_counter(NEXT_EVENT_SEQ).unwrap();
        client_a
            .promote_scratch_auto(
                ROOT_INO,
                b"merge",
                dir_a.ino,
                &HashSet::from([dir_a.ino, file_a.ino]),
                "client-a",
            )
            .unwrap();
        assert!(
            client_a
                .mark_regular_file_overlay_object_uploaded_if_matches(file_a.ino, 1, 1001)
                .unwrap()
        );
        assert!(
            client_a
                .publish_regular_file_overlay_to_base_if_matches(file_a.ino, 1)
                .unwrap()
        );
        let a_end = client_a.get_counter(NEXT_EVENT_SEQ).unwrap();
        let batch_a =
            project_local_events_for_replay(&client_a.read_events_range(a_start, a_end).unwrap());

        let b_start = client_b.get_counter(NEXT_EVENT_SEQ).unwrap();
        client_b
            .promote_scratch_auto(
                ROOT_INO,
                b"merge",
                dir_b.ino,
                &HashSet::from([dir_b.ino, file_b.ino]),
                "client-b",
            )
            .unwrap();
        assert!(
            client_b
                .mark_regular_file_overlay_object_uploaded_if_matches(file_b.ino, 1, 2001)
                .unwrap()
        );
        assert!(
            client_b
                .publish_regular_file_overlay_to_base_if_matches(file_b.ino, 1)
                .unwrap()
        );
        let b_end = client_b.get_counter(NEXT_EVENT_SEQ).unwrap();
        let batch_b =
            project_local_events_for_replay(&client_b.read_events_range(b_start, b_end).unwrap());

        replay_ordered_batches_with_adr_0012(
            &client_a,
            vec![(1, batch_a.clone(), true), (2, batch_b.clone(), false)],
            0,
            a_start,
            a_end,
        )
        .unwrap();
        replay_ordered_batches_with_adr_0012(
            &client_b,
            vec![(1, batch_a, false), (2, batch_b, true)],
            0,
            b_start,
            b_end,
        )
        .unwrap();

        let merged_a = client_a.lookup(ROOT_INO, b"merge").unwrap();
        let merged_b = client_b.lookup(ROOT_INO, b"merge").unwrap();
        assert_eq!(merged_a.ino, dir_a.ino);
        assert_eq!(merged_b.ino, dir_a.ino);
        assert!(client_a.lookup(merged_a.ino, b"a.txt").is_ok());
        assert!(client_a.lookup(merged_a.ino, b"b.txt").is_ok());
        assert!(client_b.lookup(merged_b.ino, b"a.txt").is_ok());
        assert!(client_b.lookup(merged_b.ino, b"b.txt").is_ok());
    }
}
