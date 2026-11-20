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

fn remove_scratch_inode_aux_in_txn(txn: &redb::WriteTransaction, ino: Ino) -> MetaResult<Vec<u64>> {
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
        *self
            .scratch_index
            .write()
            .map_err(|_| storage_error("scratch index lock poisoned"))? = loaded;
        Ok(())
    }

    fn rebuild_scratch_index_under_mutation_lock(&self) {
        if let Ok(txn) = self.begin_read_txn()
            && let Ok(loaded) = ScratchIndex::load_from_read_txn(&txn)
            && let Ok(mut guard) = self.scratch_index.write()
        {
            *guard = loaded;
        }
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
        self.scratch_index
            .write()
            .map_err(|_| storage_error("scratch index lock poisoned"))?
            .note_insert(parent, attr.ino);
        if let Err(error) = txn.commit() {
            self.rebuild_scratch_index_under_mutation_lock();
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
        let mut index = self
            .scratch_index
            .write()
            .map_err(|_| storage_error("scratch index lock poisoned"))?;
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
            self.scratch_index
                .write()
                .map_err(|_| storage_error("scratch index lock poisoned"))?
                .note_remove_inode(ino);
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
        let mut index = self
            .scratch_index
            .write()
            .map_err(|_| storage_error("scratch index lock poisoned"))?;
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
            if let Some((dst_stored_name, dst_ino)) = dst_opt {
                let _ = dentries
                    .remove(encode_dentry_key(new_parent, &dst_stored_name).as_slice())
                    .map_err(storage_error)?;
                let mut dst_attr = displaced_attr.expect("displaced attr loaded");
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
            self.scratch_index
                .write()
                .map_err(|_| storage_error("scratch index lock poisoned"))?
                .note_increment_parent(new_parent);
        }
        if let Err(error) = txn.commit() {
            self.rebuild_scratch_index_under_mutation_lock();
            return Err(error);
        }
        let mut index = self
            .scratch_index
            .write()
            .map_err(|_| storage_error("scratch index lock poisoned"))?;
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
}
