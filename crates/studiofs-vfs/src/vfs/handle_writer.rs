use super::local_dirty::{DirtyWriteSnapshot, PersistedPendingLocalDirtySliceSpec};
use super::{Bytes, FileType, Ino, LocalSyncPolicy, StudioVfs, VfsError, VfsResult};
use crate::VfsMeta;
use crate::runtime::run_meta;
use bytes::BytesMut;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use studiofs_meta::schema::offset_within_block;

const JUICEFS_LIKE_SLICE_FLUSH_BYTES: usize = 4 * 1024 * 1024;
const HANDLE_WRITER_SHARD_COUNT: usize = 64;

#[derive(Debug, Clone)]
pub(super) struct HandleWriterReadSpan {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) block_offset: u64,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
pub(super) struct BufferedHandleWrite {
    pub(super) block_index: u32,
    pub(super) offset: u64,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
struct SpillBatchSegment {
    write: BufferedHandleWrite,
    object_offset: u32,
}

#[derive(Debug, Clone)]
struct HandleWriterSpillBatch {
    payload: Bytes,
    segments: Vec<SpillBatchSegment>,
}

#[derive(Debug, Clone)]
struct HandleSliceWriter {
    ino: Ino,
    block_size: u32,
    logical_size: u64,
    open: bool,
    dirty_bytes: usize,
    writes: BTreeMap<u64, BufferedHandleWrite>,
    ready_full_block_indexes: HashSet<u32>,
}

#[derive(Debug, Default)]
struct HandleWriterRegistryState {
    writers: HashMap<u64, HandleSliceWriter>,
    writers_by_ino: HashMap<Ino, HashSet<u64>>,
}

#[derive(Debug)]
pub(super) struct HandleWriterRegistry {
    shards: Box<[Mutex<HandleWriterRegistryState>]>,
    handle_shards: Mutex<HashMap<u64, usize>>,
    total_dirty_bytes: AtomicUsize,
    next_seq: AtomicU64,
}

impl HandleSliceWriter {
    fn new(ino: Ino, current_size: u64, block_size: u32) -> Self {
        Self {
            ino,
            block_size,
            logical_size: current_size,
            open: true,
            dirty_bytes: 0,
            writes: BTreeMap::new(),
            ready_full_block_indexes: HashSet::new(),
        }
    }

    fn upsert_write(&mut self, block_index: u32, seq: u64, offset: u64, payload: Bytes) {
        if payload.is_empty() {
            return;
        }

        let end = offset.saturating_add(payload.len() as u64);
        let mut overlap_starts: Vec<u64> = self
            .writes
            .range(offset..end)
            .map(|(&start, _)| start)
            .collect();
        if let Some((&start, existing)) = self.writes.range(..=offset).next_back()
            && start.saturating_add(existing.data.len() as u64) > offset
        {
            overlap_starts.push(start);
        }
        overlap_starts.sort_unstable();
        overlap_starts.dedup();

        for start in overlap_starts {
            let Some(existing) = self.writes.remove(&start) else {
                continue;
            };
            self.dirty_bytes = self.dirty_bytes.saturating_sub(existing.data.len());

            let existing_end = start.saturating_add(existing.data.len() as u64);
            if start < offset {
                let keep_len = (offset - start) as usize;
                self.insert_slice(
                    existing.block_index,
                    start,
                    existing.seq,
                    existing.data.slice(0..keep_len),
                );
            }
            if existing_end > end {
                let keep_from = (end - start) as usize;
                self.insert_slice(
                    existing.block_index,
                    end,
                    existing.seq,
                    existing.data.slice(keep_from..),
                );
            }
        }

        self.insert_slice(block_index, offset, seq, payload);
        self.logical_size = self.logical_size.max(end);
        self.recompute_ready_full_block(block_index);
    }

    fn insert_slice(&mut self, block_index: u32, offset: u64, seq: u64, data: Bytes) {
        if data.is_empty() {
            return;
        }
        self.dirty_bytes = self.dirty_bytes.saturating_add(data.len());
        self.writes.insert(
            offset,
            BufferedHandleWrite {
                block_index,
                offset,
                seq,
                data,
            },
        );
    }

    fn snapshot_read_spans(&self, offset: u64, end: u64, spans: &mut Vec<HandleWriterReadSpan>) {
        for write in self.writes.values() {
            let write_end = write.offset.saturating_add(write.data.len() as u64);
            let start = write.offset.max(offset);
            let stop = write_end.min(end);
            if start >= stop {
                continue;
            }
            spans.push(HandleWriterReadSpan {
                start,
                end: stop,
                block_offset: start.saturating_sub(write.offset),
                seq: write.seq,
                data: write.data.clone(),
            });
        }
    }

    fn snapshot_materialization_writes(&self, out: &mut Vec<BufferedHandleWrite>) {
        out.extend(self.writes.values().cloned());
    }

    fn snapshot_ready_full_block_indexes(&self, out: &mut HashSet<u32>) {
        out.extend(self.ready_full_block_indexes.iter().copied());
    }

    #[cfg(test)]
    fn mark_materialized(&mut self, seq: u64) {
        let mut removed_bytes = 0usize;
        let mut affected_blocks = HashSet::new();
        self.writes.retain(|_, write| {
            if write.seq <= seq {
                removed_bytes = removed_bytes.saturating_add(write.data.len());
                affected_blocks.insert(write.block_index);
                false
            } else {
                true
            }
        });
        self.dirty_bytes = self.dirty_bytes.saturating_sub(removed_bytes);
        for block_index in affected_blocks {
            self.recompute_ready_full_block(block_index);
        }
    }

    fn mark_blocks_materialized(&mut self, seqs_by_block: &HashMap<u32, u64>) {
        let mut removed_bytes = 0usize;
        let mut affected_blocks = HashSet::new();
        self.writes.retain(|_, write| {
            let should_remove = seqs_by_block
                .get(&write.block_index)
                .is_some_and(|cutoff| write.seq <= *cutoff);
            if should_remove {
                removed_bytes = removed_bytes.saturating_add(write.data.len());
                affected_blocks.insert(write.block_index);
                false
            } else {
                true
            }
        });
        self.dirty_bytes = self.dirty_bytes.saturating_sub(removed_bytes);
        for block_index in affected_blocks {
            self.recompute_ready_full_block(block_index);
        }
    }

    fn recompute_ready_full_block(&mut self, block_index: u32) {
        self.ready_full_block_indexes.remove(&block_index);
        let logical_len = self.logical_block_len(block_index);
        if logical_len != self.block_size as usize {
            return;
        }

        let block_start = u64::from(block_index).saturating_mul(u64::from(self.block_size));
        let block_end = block_start.saturating_add(u64::from(self.block_size));
        let mut ranges = Vec::new();
        for write in self
            .writes
            .range(block_start..block_end)
            .map(|(_, write)| write)
        {
            if write.data.is_empty() {
                continue;
            }
            let start = offset_within_block(write.offset, self.block_size) as usize;
            let end = start.saturating_add(write.data.len());
            ranges.push((start, end));
        }
        if ranges.is_empty() {
            return;
        }
        ranges.sort_unstable_by_key(|(start, _)| *start);
        let mut covered_until = 0usize;
        for (start, end) in ranges {
            if start > covered_until {
                return;
            }
            covered_until = covered_until.max(end);
            if covered_until >= logical_len {
                self.ready_full_block_indexes.insert(block_index);
                return;
            }
        }
    }

    fn logical_block_len(&self, block_index: u32) -> usize {
        let block_start = block_index as u64 * self.block_size as u64;
        if block_start >= self.logical_size {
            return 0;
        }
        self.logical_size
            .saturating_sub(block_start)
            .min(self.block_size as u64) as usize
    }
}

impl HandleWriterRegistry {
    pub(super) fn new() -> Self {
        Self {
            shards: (0..HANDLE_WRITER_SHARD_COUNT)
                .map(|_| Mutex::new(HandleWriterRegistryState::default()))
                .collect(),
            handle_shards: Mutex::new(HashMap::new()),
            total_dirty_bytes: AtomicUsize::new(0),
            next_seq: AtomicU64::new(1),
        }
    }

    fn shard_index_for_inode(ino: Ino) -> usize {
        (ino % HANDLE_WRITER_SHARD_COUNT as u64) as usize
    }

    #[cfg(test)]
    fn shard_index_for_inode_for_test(&self, ino: Ino) -> usize {
        Self::shard_index_for_inode(ino)
    }

    #[cfg(test)]
    pub(super) fn write(
        &self,
        fh: u64,
        ino: Ino,
        current_size: u64,
        block_size: u32,
        offset: u64,
        data: &[u8],
    ) -> u32 {
        self.write_bytes(
            fh,
            ino,
            current_size,
            block_size,
            offset,
            Bytes::copy_from_slice(data),
        )
    }

    pub(super) fn write_bytes(
        &self,
        fh: u64,
        ino: Ino,
        current_size: u64,
        block_size: u32,
        offset: u64,
        data: Bytes,
    ) -> u32 {
        let data_len = data.len();
        if data_len == 0 {
            return 0;
        }

        let shard_index = Self::shard_index_for_inode(ino);
        let previous_shard_index = { self.handle_shards.lock().get(&fh).copied() };
        let moved_writer = previous_shard_index
            .filter(|previous| *previous != shard_index)
            .and_then(|previous| {
                let mut previous_shard = self.shards[previous].lock();
                Self::remove_writer_without_dirty_adjustment(&mut previous_shard, fh)
            });
        let mut writers = self.shards[shard_index].lock();
        let previous_ino = writers
            .writers
            .get(&fh)
            .or(moved_writer.as_ref())
            .map(|writer| writer.ino);
        if let Some(writer) = moved_writer {
            writers.writers.insert(fh, writer);
        }
        let (old_dirty_bytes, new_dirty_bytes) = {
            let writer = writers
                .writers
                .entry(fh)
                .or_insert_with(|| HandleSliceWriter::new(ino, current_size, block_size));
            let old_dirty_bytes = writer.dirty_bytes;
            writer.ino = ino;
            writer.block_size = block_size;
            writer.open = true;
            writer.logical_size = writer.logical_size.max(current_size);
            assert!(block_size > 0, "block_size must be non-zero");
            let mut current_offset = offset;
            let mut data_offset = 0usize;
            let mut remaining = data_len;
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            while remaining > 0 {
                let block_index = (current_offset / u64::from(block_size)) as u32;
                let offset_in_block = (current_offset % u64::from(block_size)) as usize;
                let space_in_block = block_size as usize - offset_in_block;
                let chunk_len = remaining.min(space_in_block);
                writer.upsert_write(
                    block_index,
                    seq,
                    current_offset,
                    data.slice(data_offset..data_offset + chunk_len),
                );
                current_offset = current_offset.saturating_add(chunk_len as u64);
                data_offset += chunk_len;
                remaining -= chunk_len;
            }
            (old_dirty_bytes, writer.dirty_bytes)
        };
        self.adjust_total_dirty_bytes(old_dirty_bytes, new_dirty_bytes);
        Self::reindex_writer(&mut writers, fh, previous_ino, ino);
        drop(writers);
        self.handle_shards.lock().insert(fh, shard_index);
        data_len as u32
    }

    pub(super) fn snapshot_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<HandleWriterReadSpan> {
        if length == 0 {
            return Vec::new();
        }
        let Some(end) = offset.checked_add(length) else {
            return Vec::new();
        };
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let mut spans = Vec::new();
        if let Some(handles) = writers.writers_by_ino.get(&ino) {
            for fh in handles {
                if let Some(writer) = writers.writers.get(fh) {
                    writer.snapshot_read_spans(offset, end, &mut spans);
                }
            }
        }
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    pub(super) fn max_visible_size(&self, ino: Ino) -> Option<u64> {
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        writers.writers_by_ino.get(&ino).and_then(|handles| {
            handles
                .iter()
                .filter_map(|fh| writers.writers.get(fh))
                .filter(|writer| !writer.writes.is_empty())
                .map(|writer| writer.logical_size)
                .max()
        })
    }

    pub(super) fn has_dirty_writers(&self, ino: Ino) -> bool {
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        writers.writers_by_ino.get(&ino).is_some_and(|handles| {
            handles
                .iter()
                .filter_map(|fh| writers.writers.get(fh))
                .any(|writer| !writer.writes.is_empty())
        })
    }

    pub(super) fn dirty_bytes_for_inode(&self, ino: Ino) -> usize {
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        writers
            .writers_by_ino
            .get(&ino)
            .into_iter()
            .flatten()
            .filter_map(|fh| writers.writers.get(fh))
            .map(|writer| writer.dirty_bytes)
            .sum()
    }

    pub(super) fn total_dirty_bytes(&self) -> usize {
        self.total_dirty_bytes.load(Ordering::Acquire)
    }

    pub(super) fn total_buffered_write_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                let writers = shard.lock();
                writers
                    .writers
                    .values()
                    .map(|writer| writer.writes.len())
                    .sum::<usize>()
            })
            .sum()
    }

    pub(super) fn snapshot_dirty_inodes_by_bytes(&self) -> Vec<(Ino, usize)> {
        self.shards
            .iter()
            .flat_map(|shard| {
                let writers = shard.lock();
                writers
                    .writers_by_ino
                    .iter()
                    .filter_map(|(&ino, handles)| {
                        let dirty_bytes: usize = handles
                            .iter()
                            .filter_map(|fh| writers.writers.get(fh))
                            .map(|writer| writer.dirty_bytes)
                            .sum();
                        (dirty_bytes > 0).then_some((ino, dirty_bytes))
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    pub(super) fn mark_closed(&self, fh: u64) {
        let Some(shard_index) = self.handle_shards.lock().get(&fh).copied() else {
            return;
        };
        let remove = {
            let mut writers = self.shards[shard_index].lock();
            let remove = if let Some(writer) = writers.writers.get_mut(&fh) {
                writer.open = false;
                writer.writes.is_empty()
            } else {
                false
            };
            if remove {
                self.remove_writer(&mut writers, fh);
            }
            remove
        };
        if remove {
            self.handle_shards.lock().remove(&fh);
        }
    }

    pub(super) fn clear_inode(&self, ino: Ino) {
        let mut writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let inode_handles: Vec<u64> = writers
            .writers_by_ino
            .get(&ino)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        for fh in &inode_handles {
            self.remove_writer(&mut writers, *fh);
        }
        drop(writers);
        self.remove_handle_shard_entries(&inode_handles);
    }

    pub(super) fn snapshot_materialization_writes(&self, ino: Ino) -> Vec<BufferedHandleWrite> {
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let mut writes = Vec::new();
        if let Some(handles) = writers.writers_by_ino.get(&ino) {
            for fh in handles {
                if let Some(writer) = writers.writers.get(fh) {
                    writer.snapshot_materialization_writes(&mut writes);
                }
            }
        }
        writes.sort_by_key(|write| write.seq);
        writes
    }

    pub(super) fn snapshot_ready_full_block_indexes(&self, ino: Ino) -> HashSet<u32> {
        let writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let mut ready = HashSet::new();
        if let Some(handles) = writers.writers_by_ino.get(&ino) {
            for fh in handles {
                if let Some(writer) = writers.writers.get(fh) {
                    writer.snapshot_ready_full_block_indexes(&mut ready);
                }
            }
        }
        ready
    }

    #[cfg(test)]
    pub(super) fn mark_materialized(&self, ino: Ino, seq: u64) {
        let mut writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let mut empty_closed = Vec::new();
        let inode_handles: Vec<u64> = writers
            .writers_by_ino
            .get(&ino)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        for fh in inode_handles {
            let Some((old_dirty_bytes, new_dirty_bytes, remove_closed)) =
                (match writers.writers.get_mut(&fh) {
                    Some(writer) => {
                        let old_dirty_bytes = writer.dirty_bytes;
                        writer.mark_materialized(seq);
                        Some((
                            old_dirty_bytes,
                            writer.dirty_bytes,
                            writer.writes.is_empty() && !writer.open,
                        ))
                    }
                    None => None,
                })
            else {
                continue;
            };
            self.adjust_total_dirty_bytes(old_dirty_bytes, new_dirty_bytes);
            if remove_closed {
                empty_closed.push(fh);
            }
        }
        for fh in &empty_closed {
            self.remove_writer(&mut writers, *fh);
        }
        drop(writers);
        self.remove_handle_shard_entries(&empty_closed);
    }

    pub(super) fn mark_blocks_materialized(&self, ino: Ino, seqs_by_block: &HashMap<u32, u64>) {
        let mut writers = self.shards[Self::shard_index_for_inode(ino)].lock();
        let mut empty_closed = Vec::new();
        let inode_handles: Vec<u64> = writers
            .writers_by_ino
            .get(&ino)
            .into_iter()
            .flatten()
            .copied()
            .collect();
        for fh in inode_handles {
            let Some((old_dirty_bytes, new_dirty_bytes, remove_closed)) =
                (match writers.writers.get_mut(&fh) {
                    Some(writer) => {
                        let old_dirty_bytes = writer.dirty_bytes;
                        writer.mark_blocks_materialized(seqs_by_block);
                        Some((
                            old_dirty_bytes,
                            writer.dirty_bytes,
                            writer.writes.is_empty() && !writer.open,
                        ))
                    }
                    None => None,
                })
            else {
                continue;
            };
            self.adjust_total_dirty_bytes(old_dirty_bytes, new_dirty_bytes);
            if remove_closed {
                empty_closed.push(fh);
            }
        }
        for fh in &empty_closed {
            self.remove_writer(&mut writers, *fh);
        }
        drop(writers);
        self.remove_handle_shard_entries(&empty_closed);
    }

    #[cfg(test)]
    pub(super) fn open_writer_for_test(&self, fh: u64, ino: Ino, current_size: u64) {
        let shard_index = Self::shard_index_for_inode(ino);
        let mut writers = self.shards[shard_index].lock();
        if let std::collections::hash_map::Entry::Vacant(entry) = writers.writers.entry(fh) {
            entry.insert(HandleSliceWriter::new(ino, current_size, 4096));
            writers.writers_by_ino.entry(ino).or_default().insert(fh);
            drop(writers);
            self.handle_shards.lock().insert(fh, shard_index);
        }
    }

    fn adjust_total_dirty_bytes(&self, old_bytes: usize, new_bytes: usize) {
        match new_bytes.cmp(&old_bytes) {
            std::cmp::Ordering::Greater => {
                self.total_dirty_bytes
                    .fetch_add(new_bytes - old_bytes, Ordering::AcqRel);
            }
            std::cmp::Ordering::Less => {
                let delta = old_bytes - new_bytes;
                let _ = self.total_dirty_bytes.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |current| {
                        debug_assert!(
                            current >= delta,
                            "total_dirty_bytes underflow: current={}, delta={}",
                            current,
                            delta
                        );
                        Some(current.saturating_sub(delta))
                    },
                );
            }
            std::cmp::Ordering::Equal => {}
        }
    }

    fn remove_handle_shard_entries(&self, handles: &[u64]) {
        if handles.is_empty() {
            return;
        }
        let mut handle_shards = self.handle_shards.lock();
        for fh in handles {
            handle_shards.remove(fh);
        }
    }

    fn reindex_writer(
        state: &mut HandleWriterRegistryState,
        fh: u64,
        previous_ino: Option<Ino>,
        next_ino: Ino,
    ) {
        if let Some(previous_ino) = previous_ino
            && previous_ino != next_ino
        {
            Self::remove_writer_from_inode_index(state, previous_ino, fh);
        }
        state.writers_by_ino.entry(next_ino).or_default().insert(fh);
    }

    fn remove_writer_from_inode_index(state: &mut HandleWriterRegistryState, ino: Ino, fh: u64) {
        let mut remove_inode = false;
        if let Some(handles) = state.writers_by_ino.get_mut(&ino) {
            handles.remove(&fh);
            remove_inode = handles.is_empty();
        }
        if remove_inode {
            state.writers_by_ino.remove(&ino);
        }
    }

    fn remove_writer_without_dirty_adjustment(
        state: &mut HandleWriterRegistryState,
        fh: u64,
    ) -> Option<HandleSliceWriter> {
        let writer = state.writers.remove(&fh)?;
        Self::remove_writer_from_inode_index(state, writer.ino, fh);
        Some(writer)
    }

    fn remove_writer(&self, state: &mut HandleWriterRegistryState, fh: u64) {
        let Some(writer) = Self::remove_writer_without_dirty_adjustment(state, fh) else {
            return;
        };
        self.adjust_total_dirty_bytes(writer.dirty_bytes, 0);
    }
}

impl StudioVfs {
    fn fully_overwritten_full_block_indexes_for_handle_writes(
        &self,
        writes: &[BufferedHandleWrite],
        visible_size: u64,
    ) -> std::collections::HashSet<u32> {
        let mut ranges_by_block = std::collections::BTreeMap::<u32, Vec<(usize, usize)>>::new();

        for write in writes {
            if write.data.is_empty() {
                continue;
            }
            let start = offset_within_block(write.offset, self.block_size) as usize;
            let end = start.saturating_add(write.data.len());
            ranges_by_block
                .entry(write.block_index)
                .or_default()
                .push((start, end));
        }

        ranges_by_block
            .into_iter()
            .filter_map(|(block_index, mut ranges)| {
                let logical_len = self.logical_block_len(visible_size, block_index);
                if logical_len != self.block_size as usize {
                    return None;
                }
                ranges.sort_unstable_by_key(|(start, _)| *start);
                let mut covered_until = 0usize;
                for (start, end) in ranges {
                    if start > covered_until {
                        return None;
                    }
                    covered_until = covered_until.max(end);
                    if covered_until >= logical_len {
                        return Some(block_index);
                    }
                }
                None
            })
            .collect()
    }

    fn direct_full_block_snapshots_for_handle_writes(
        &self,
        writes: &[BufferedHandleWrite],
        visible_size: u64,
        selected_block_indexes: &HashSet<u32>,
        fully_overwritten_blocks: &HashSet<u32>,
    ) -> std::collections::BTreeMap<u32, DirtyWriteSnapshot> {
        let mut counts = HashMap::<u32, usize>::new();
        let mut candidates = std::collections::BTreeMap::<u32, DirtyWriteSnapshot>::new();

        for write in writes {
            if write.data.is_empty() || !selected_block_indexes.contains(&write.block_index) {
                continue;
            }
            *counts.entry(write.block_index).or_default() += 1;
            if !fully_overwritten_blocks.contains(&write.block_index) {
                candidates.remove(&write.block_index);
                continue;
            }
            let logical_len = self.logical_block_len(visible_size, write.block_index);
            let offset_in_block = offset_within_block(write.offset, self.block_size) as usize;
            if offset_in_block == 0 && write.data.len() == logical_len {
                candidates.insert(
                    write.block_index,
                    DirtyWriteSnapshot::from_shared(write.block_index, write.data.clone()),
                );
            } else {
                candidates.remove(&write.block_index);
            }
        }

        candidates.retain(|block_index, _| counts.get(block_index) == Some(&1));
        candidates
    }

    fn fully_overwritten_full_block_indexes_for_current_handle_write(
        &self,
        writes: &[BufferedHandleWrite],
        visible_size: u64,
    ) -> HashSet<u32> {
        let Some(current_seq) = writes.last().map(|write| write.seq) else {
            return HashSet::new();
        };

        writes
            .iter()
            .filter(|write| write.seq == current_seq)
            .filter_map(|write| {
                let logical_len = self.logical_block_len(visible_size, write.block_index);
                let offset_in_block = offset_within_block(write.offset, self.block_size) as usize;
                (logical_len == self.block_size as usize
                    && offset_in_block == 0
                    && write.data.len() == logical_len)
                    .then_some(write.block_index)
            })
            .collect()
    }

    fn eager_materialization_block_cap(&self) -> usize {
        self.write_eager_flush_drain_chunk_bytes
            .div_ceil(self.block_size as usize)
            .max(1)
    }

    pub(super) fn handle_writer_slice_flush_bytes(&self) -> usize {
        JUICEFS_LIKE_SLICE_FLUSH_BYTES
            .min(self.block_size as usize)
            .max(1)
    }

    pub(super) fn handle_writer_active_tail_bytes(&self) -> usize {
        (self.handle_writer_slice_flush_bytes() / 2).max(1)
    }

    fn handle_writer_slice_spill_budget_bytes(&self) -> usize {
        self.write_eager_flush_drain_chunk_bytes
            .max(self.handle_writer_slice_flush_bytes())
    }

    fn select_handle_writer_slices_for_spill(
        &self,
        writes: &[BufferedHandleWrite],
    ) -> Vec<BufferedHandleWrite> {
        let mut ordered = writes.to_vec();
        ordered.sort_by_key(|write| (write.seq, write.offset));
        let mut retain_tail_bytes = self.handle_writer_active_tail_bytes();
        let mut spillable = Vec::new();
        for write in ordered.into_iter().rev() {
            let write_len = write.data.len();
            if retain_tail_bytes == 0 {
                spillable.push(write);
                continue;
            }
            if write_len <= retain_tail_bytes {
                retain_tail_bytes = retain_tail_bytes.saturating_sub(write_len);
                continue;
            }

            retain_tail_bytes = 0;
        }
        spillable.reverse();

        let spill_bytes_cap = self.handle_writer_slice_spill_budget_bytes();
        let mut selected = Vec::new();
        let mut selected_bytes = 0usize;
        for write in spillable {
            let remaining = spill_bytes_cap.saturating_sub(selected_bytes);
            if remaining == 0 || write.data.len() > remaining {
                break;
            }
            if write.data.is_empty() {
                break;
            }
            selected_bytes = selected_bytes.saturating_add(write.data.len());
            selected.push(write);
            if selected_bytes >= spill_bytes_cap {
                break;
            }
        }
        selected
    }

    fn should_spill_selected_handle_writer_slices(
        &self,
        selected: &[BufferedHandleWrite],
        budget_exceeded: bool,
    ) -> bool {
        if selected.is_empty() {
            return false;
        }
        if budget_exceeded {
            return true;
        }
        let selected_bytes: usize = selected.iter().map(|write| write.data.len()).sum();
        selected_bytes >= self.handle_writer_slice_flush_bytes()
    }

    fn build_handle_writer_slice_spill_batches(
        &self,
        selected: Vec<BufferedHandleWrite>,
    ) -> Vec<HandleWriterSpillBatch> {
        if selected.is_empty() {
            return Vec::new();
        }

        let spill_bytes_cap = self.handle_writer_slice_flush_bytes();
        let mut batches = Vec::new();
        let mut current_payload = BytesMut::new();
        let mut current_segments = Vec::new();
        let mut current_end: Option<u64> = None;

        let flush_current_batch =
            |payload: &mut BytesMut,
             segments: &mut Vec<SpillBatchSegment>,
             out: &mut Vec<HandleWriterSpillBatch>| {
                if segments.is_empty() {
                    payload.clear();
                    return;
                }
                out.push(HandleWriterSpillBatch {
                    payload: payload.split().freeze(),
                    segments: std::mem::take(segments),
                });
            };

        for write in selected {
            let contiguous = current_end.is_none_or(|end| end == write.offset);
            let fits_current_batch = current_payload.is_empty()
                || current_payload.len().saturating_add(write.data.len()) <= spill_bytes_cap;
            if !current_segments.is_empty() && (!contiguous || !fits_current_batch) {
                flush_current_batch(&mut current_payload, &mut current_segments, &mut batches);
            }

            let object_offset = u32::try_from(current_payload.len())
                .expect("spill batch payload offset must fit in u32");
            current_payload.extend_from_slice(write.data.as_ref());
            current_end = Some(write.offset.saturating_add(write.data.len() as u64));
            current_segments.push(SpillBatchSegment {
                write,
                object_offset,
            });
        }

        flush_current_batch(&mut current_payload, &mut current_segments, &mut batches);
        batches
    }

    async fn persist_handle_writer_spill_batches(
        &self,
        ino: Ino,
        spill_batches: Vec<HandleWriterSpillBatch>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        if spill_batches.is_empty() {
            return Ok(false);
        }

        let object_count = u32::try_from(spill_batches.len()).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "requested slice spill batch count {} exceeds u32::MAX",
                    spill_batches.len()
                ),
            ))
        })?;
        let object_ids = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_object_ids(object_count)
        })
        .await?;
        let payloads: Vec<(u64, Bytes)> = spill_batches
            .iter()
            .zip(object_ids.iter().copied())
            .map(|(batch, object_id)| (object_id, batch.payload.clone()))
            .collect();
        let logical_block_count = spill_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .segments
                    .iter()
                    .map(|segment| segment.write.block_index)
            })
            .collect::<HashSet<_>>()
            .len();
        self.persist_local_object_payloads(payloads, logical_block_count, local_sync_policy)
            .await?;
        for (batch, object_id) in spill_batches.iter().zip(object_ids.iter().copied()) {
            for segment in &batch.segments {
                self.install_persisted_pending_local_dirty_slice(
                    ino,
                    PersistedPendingLocalDirtySliceSpec {
                        block_index: segment.write.block_index,
                        file_offset: segment.write.offset,
                        len: segment.write.data.len(),
                        seq: segment.write.seq,
                        object_id,
                        initial_object_offset: segment.object_offset,
                    },
                );
            }
        }
        let mut seqs_by_block = HashMap::<u32, u64>::new();
        for batch in &spill_batches {
            for segment in &batch.segments {
                seqs_by_block
                    .entry(segment.write.block_index)
                    .and_modify(|seq| *seq = (*seq).max(segment.write.seq))
                    .or_insert(segment.write.seq);
            }
        }
        self.state
            .handle_writers
            .mark_blocks_materialized(ino, &seqs_by_block);
        Ok(true)
    }

    pub(super) async fn spill_handle_writer_slices_for_inode(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        if !matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            return Ok(false);
        }
        let writes = self
            .state
            .handle_writers
            .snapshot_materialization_writes(ino);
        let selected = self.select_handle_writer_slices_for_spill(&writes);
        let budget_exceeded = self.total_pending_write_buffer_bytes() > self.max_dirty_bytes();
        if !self.should_spill_selected_handle_writer_slices(&selected, budget_exceeded) {
            return Ok(false);
        }
        let spill_batches = self.build_handle_writer_slice_spill_batches(selected);
        self.persist_handle_writer_spill_batches(ino, spill_batches, local_sync_policy)
            .await
    }

    fn select_completed_blocks_for_eager_materialization(
        &self,
        completed_block_indexes: &HashSet<u32>,
        current_write_full_block_indexes: &HashSet<u32>,
        local_sync_policy: LocalSyncPolicy,
    ) -> HashSet<u32> {
        if matches!(local_sync_policy, LocalSyncPolicy::Strict) {
            return completed_block_indexes.clone();
        }

        let block_cap = self.eager_materialization_block_cap();
        if completed_block_indexes.len() <= block_cap {
            return completed_block_indexes.clone();
        }

        let mut selected = BTreeSet::new();
        for block_index in current_write_full_block_indexes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
        {
            if completed_block_indexes.contains(&block_index) {
                selected.insert(block_index);
                if selected.len() >= block_cap {
                    return selected.into_iter().collect();
                }
            }
        }

        for block_index in completed_block_indexes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
        {
            selected.insert(block_index);
            if selected.len() >= block_cap {
                break;
            }
        }

        selected.into_iter().collect()
    }

    async fn build_dirty_write_snapshots_for_handle_writer_blocks(
        &self,
        ino: Ino,
        writes: &[BufferedHandleWrite],
        visible_size: u64,
        selected_block_indexes: &HashSet<u32>,
        fully_overwritten_blocks: &HashSet<u32>,
        include_partial_blocks: bool,
    ) -> VfsResult<Vec<DirtyWriteSnapshot>> {
        if selected_block_indexes.is_empty() {
            return Ok(Vec::new());
        }

        let pending_upload_index = self.snapshot_pending_upload_index();
        let direct_snapshots = self.direct_full_block_snapshots_for_handle_writes(
            writes,
            visible_size,
            selected_block_indexes,
            fully_overwritten_blocks,
        );
        let mut blocks = std::collections::BTreeMap::<u32, BytesMut>::new();

        for write in writes {
            if write.data.is_empty() || !selected_block_indexes.contains(&write.block_index) {
                continue;
            }
            if direct_snapshots.contains_key(&write.block_index) {
                continue;
            }

            if let std::collections::btree_map::Entry::Vacant(entry) =
                blocks.entry(write.block_index)
            {
                let existing_data = if fully_overwritten_blocks.contains(&write.block_index) {
                    Bytes::new()
                } else {
                    let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
                    match self
                        .read_pending_overlay_block_async(
                            ino,
                            write.block_index,
                            pending_content_epoch,
                            Some(&pending_upload_index),
                        )
                        .await?
                    {
                        Some(data) => {
                            let logical_len =
                                self.logical_block_len(visible_size, write.block_index);
                            if data.len() > logical_len {
                                data.slice(0..logical_len)
                            } else {
                                data
                            }
                        }
                        None => self
                            .read_block_if_exists(
                                ino,
                                write.block_index,
                                Some(&pending_upload_index),
                            )
                            .await?
                            .unwrap_or_default(),
                    }
                };
                entry.insert(self.clone_into_write_buffer(existing_data.as_ref()));
            }

            let offset_in_block = offset_within_block(write.offset, self.block_size) as usize;
            let block = blocks
                .get_mut(&write.block_index)
                .expect("block buffer inserted before write");
            let end_pos = offset_in_block + write.data.len();
            if block.len() < end_pos {
                block.resize(end_pos, 0);
            }
            block[offset_in_block..end_pos].copy_from_slice(write.data.as_ref());
        }

        let mut snapshots: Vec<DirtyWriteSnapshot> = direct_snapshots.into_values().collect();
        snapshots.extend(blocks.into_iter().filter_map(|(block_index, mut data)| {
            let logical_len = self.logical_block_len(visible_size, block_index);
            if data.len() > logical_len {
                data.truncate(logical_len);
            }
            if include_partial_blocks || data.len() == self.block_size as usize {
                Some(DirtyWriteSnapshot::from_mutable(block_index, data))
            } else {
                None
            }
        }));
        Ok(snapshots)
    }

    pub(super) fn snapshot_handle_writer_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<HandleWriterReadSpan> {
        self.state
            .handle_writers
            .snapshot_read_spans(ino, offset, length)
    }

    pub(super) fn inode_has_dirty_handle_writers(&self, ino: Ino) -> bool {
        self.state.handle_writers.has_dirty_writers(ino)
    }

    #[cfg(test)]
    pub(super) async fn record_handle_writer_state(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
    ) -> VfsResult<u32> {
        self.record_handle_writer_state_bytes(fh, ino, offset, Bytes::copy_from_slice(data))
            .await
    }

    #[allow(dead_code)]
    pub(super) async fn record_handle_writer_state_bytes(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: Bytes,
    ) -> VfsResult<u32> {
        let mut base_attr =
            if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino).cloned() {
                attr
            } else {
                self.getattr_async(ino).await?
            };
        self.refresh_inode_incarnation_state_from_attr(ino, &base_attr);
        self.begin_pending_incarnation(ino, &mut base_attr);
        let written = self.state.handle_writers.write_bytes(
            fh,
            ino,
            base_attr.size,
            self.block_size,
            offset,
            data,
        );
        {
            let mut dirty = self.state.namespace.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
            attr.content_epoch = base_attr.content_epoch;
            attr.kind = FileType::RegularFile;
            let new_size = self
                .state
                .handle_writers
                .max_visible_size(ino)
                .unwrap_or(attr.size);
            if new_size > attr.size {
                attr.size = new_size;
            }
            attr.touch_mtime();
            self.update_pending_incarnation_size(ino, attr.size);
        }
        Ok(written)
    }

    pub(super) async fn materialize_handle_writers_for_limits(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        let mut per_inode_dirty = self.state.handle_writers.dirty_bytes_for_inode(ino);
        let mut total_dirty = self.total_pending_write_buffer_bytes();
        let max_dirty = self.max_dirty_bytes();
        let mut budget_exceeded = total_dirty > max_dirty;
        if per_inode_dirty == 0 && !budget_exceeded {
            return Ok(false);
        }

        let mut made_progress = false;
        if per_inode_dirty > 0 && matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            made_progress |= self
                .spill_handle_writer_slices_for_inode(ino, local_sync_policy)
                .await?;
            per_inode_dirty = self.state.handle_writers.dirty_bytes_for_inode(ino);
            total_dirty = self.total_pending_write_buffer_bytes();
            budget_exceeded = total_dirty > max_dirty;
            if per_inode_dirty == 0 && !budget_exceeded {
                return Ok(made_progress);
            }
        }
        if per_inode_dirty > 0
            && per_inode_dirty < self.write_eager_flush_batch_bytes
            && !budget_exceeded
        {
            return Ok(made_progress);
        }
        if per_inode_dirty > 0 && !budget_exceeded {
            made_progress |= self
                .materialize_completed_handle_writer_blocks_for_inode(ino, local_sync_policy)
                .await?;
            let per_inode_dirty_after = self.state.handle_writers.dirty_bytes_for_inode(ino);
            if per_inode_dirty_after == 0
                || (per_inode_dirty_after < self.write_eager_flush_batch_bytes && !budget_exceeded)
            {
                return Ok(made_progress);
            }
        }

        if self.state.handle_writers.dirty_bytes_for_inode(ino) > 0 {
            made_progress |= self
                .materialize_handle_writers_for_inode(ino, local_sync_policy)
                .await?;
        }

        // #region debug log
        if budget_exceeded {
            self.debug_write_probe(
                "H1",
                "crates/studiofs-vfs/src/vfs/handle_writer.rs:738",
                "limit-drain pass started",
                serde_json::json!({
                    "ino": ino,
                    "per_inode_dirty_bytes": per_inode_dirty,
                    "total_dirty_bytes": total_dirty,
                    "max_dirty_bytes": max_dirty,
                    "dirty_inode_count": self.state.handle_writers.snapshot_dirty_inodes_by_bytes().len(),
                    "buffered_write_count": self.state.handle_writers.total_buffered_write_count(),
                }),
            );
        }
        // #endregion debug log

        // Graduated backpressure: drain the dirtiest inodes until we are below the budget.
        // We yield between iterations so that other write tasks can make forward progress
        // (buffering new data) instead of all blocking behind a single sequential drain.
        // This mirrors the soft-backpressure approach used by JuiceFS and avoids the
        // burst-then-stall pattern where a single write blocks on materializing every other
        // inode sequentially.
        let mut stall_iterations: u32 = 0;
        while self.total_pending_write_buffer_bytes() > max_dirty {
            let mut dirty_inodes = self.state.handle_writers.snapshot_dirty_inodes_by_bytes();
            dirty_inodes.sort_by_key(|(_, dirty_bytes)| std::cmp::Reverse(*dirty_bytes));
            if dirty_inodes.is_empty() {
                break;
            }
            let mut pass_made_progress = false;
            for (next_ino, _) in dirty_inodes {
                if self.total_pending_write_buffer_bytes() <= max_dirty {
                    break;
                }
                let materialized = self
                    .materialize_handle_writers_for_inode(next_ino, local_sync_policy)
                    .await?;
                if materialized {
                    pass_made_progress = true;
                }
            }
            made_progress |= pass_made_progress;
            if !pass_made_progress {
                break;
            }
            stall_iterations = stall_iterations.saturating_add(1);
            // Yield after each drain pass to let concurrent writers buffer new data.
            // Back off progressively when we are still over budget after multiple passes.
            if stall_iterations <= 2 {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(std::time::Duration::from_millis(
                    (stall_iterations.min(10) as u64).saturating_sub(2),
                ))
                .await;
            }
        }

        // #region debug log
        if budget_exceeded || stall_iterations > 0 {
            self.debug_write_probe(
                "H1",
                "crates/studiofs-vfs/src/vfs/handle_writer.rs:815",
                "limit-drain pass completed",
                serde_json::json!({
                    "ino": ino,
                    "made_progress": made_progress,
                    "stall_iterations": stall_iterations,
                    "per_inode_dirty_bytes_after": self.state.handle_writers.dirty_bytes_for_inode(ino),
                    "total_dirty_bytes_after": self.total_pending_write_buffer_bytes(),
                    "max_dirty_bytes": max_dirty,
                    "dirty_inode_count_after": self.state.handle_writers.snapshot_dirty_inodes_by_bytes().len(),
                    "buffered_write_count_after": self.state.handle_writers.total_buffered_write_count(),
                }),
            );
        }
        // #endregion debug log

        Ok(made_progress)
    }

    pub(super) async fn materialize_handle_writers_for_inode(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        let writes = self
            .state
            .handle_writers
            .snapshot_materialization_writes(ino);
        if writes.is_empty() {
            return Ok(false);
        }
        let spill_batches = self.build_handle_writer_slice_spill_batches(writes);
        self.persist_handle_writer_spill_batches(ino, spill_batches, local_sync_policy)
            .await
    }

    pub(super) async fn materialize_completed_handle_writer_blocks_for_inode(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        let writes = self
            .state
            .handle_writers
            .snapshot_materialization_writes(ino);
        if writes.is_empty() {
            return Ok(false);
        }

        let dirty_attr = { self.state.namespace.dirty_attrs.lock().get(&ino).cloned() };
        let visible_size = if let Some(attr) = dirty_attr.as_ref() {
            attr.size
        } else {
            self.getattr_async(ino).await?.size
        };
        let mut completed_block_indexes = self
            .state
            .handle_writers
            .snapshot_ready_full_block_indexes(ino);
        if completed_block_indexes.is_empty() {
            completed_block_indexes =
                self.fully_overwritten_full_block_indexes_for_handle_writes(&writes, visible_size);
        }
        if completed_block_indexes.is_empty() {
            return Ok(false);
        }
        let current_write_full_block_indexes = self
            .fully_overwritten_full_block_indexes_for_current_handle_write(&writes, visible_size);
        let selected_block_indexes = self.select_completed_blocks_for_eager_materialization(
            &completed_block_indexes,
            &current_write_full_block_indexes,
            local_sync_policy,
        );

        let mut seqs_by_block = HashMap::<u32, u64>::new();
        for write in &writes {
            if selected_block_indexes.contains(&write.block_index) {
                seqs_by_block
                    .entry(write.block_index)
                    .and_modify(|seq| *seq = (*seq).max(write.seq))
                    .or_insert(write.seq);
            }
        }
        let current_write_crosses_boundary_with_partial_tail = writes
            .last()
            .map(|latest| latest.seq)
            .map(|current_seq| {
                let touched_blocks: BTreeSet<u32> = writes
                    .iter()
                    .filter(|write| write.seq == current_seq)
                    .map(|write| write.block_index)
                    .collect();
                let Some(&tail_block_index) = touched_blocks.iter().next_back() else {
                    return false;
                };
                if touched_blocks.len() < 2 {
                    return false;
                }
                let tail_is_partial = !completed_block_indexes.contains(&tail_block_index);
                let completed_prefix_in_current_callback = touched_blocks
                    .iter()
                    .copied()
                    .filter(|block_index| *block_index != tail_block_index)
                    .any(|block_index| completed_block_indexes.contains(&block_index));
                completed_prefix_in_current_callback && tail_is_partial
            })
            .unwrap_or(false);
        let enough_completed_blocks =
            completed_block_indexes.len() >= self.payload_put_concurrency.max(2);
        let budget_exceeded = self.total_pending_write_buffer_bytes() > self.max_dirty_bytes();
        let eager_single_full_block_accept = !current_write_full_block_indexes.is_empty()
            && visible_size <= u64::from(self.block_size);
        let current_write_completed_full_block = !current_write_full_block_indexes.is_empty();
        let should_materialize_now = matches!(local_sync_policy, LocalSyncPolicy::Strict)
            || eager_single_full_block_accept
            || current_write_completed_full_block
            || current_write_crosses_boundary_with_partial_tail
            || enough_completed_blocks
            || budget_exceeded
            || self.state.handle_writers.dirty_bytes_for_inode(ino)
                >= self.write_eager_flush_batch_bytes;
        if !should_materialize_now {
            // #region debug log
            if self.state.handle_writers.dirty_bytes_for_inode(ino)
                >= super::aggressive_materialization_threshold_bytes(self.max_dirty_bytes())
            {
                self.debug_write_probe(
                    "H3",
                    "crates/studiofs-vfs/src/vfs/handle_writer.rs:923",
                    "completed-block materialization skipped",
                    serde_json::json!({
                        "ino": ino,
                        "visible_size": visible_size,
                        "completed_payload_count": completed_block_indexes.len(),
                        "current_write_full_block_count": current_write_full_block_indexes.len(),
                        "current_write_crosses_boundary_with_partial_tail": current_write_crosses_boundary_with_partial_tail,
                        "enough_completed_blocks": enough_completed_blocks,
                        "budget_exceeded": budget_exceeded,
                        "per_inode_dirty_bytes": self.state.handle_writers.dirty_bytes_for_inode(ino),
                        "batch_threshold_bytes": self.write_eager_flush_batch_bytes,
                    }),
                );
            }
            // #endregion debug log
            return Ok(false);
        }
        let completed_blocks = self
            .build_dirty_write_snapshots_for_handle_writer_blocks(
                ino,
                &writes,
                visible_size,
                &selected_block_indexes,
                &selected_block_indexes,
                false,
            )
            .await?;
        if completed_blocks.is_empty() {
            return Ok(false);
        }
        if matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            let snapshot_writes: Vec<_> = completed_blocks
                .iter()
                .map(|block| BufferedHandleWrite {
                    block_index: block.block_index,
                    offset: u64::from(block.block_index).saturating_mul(u64::from(self.block_size)),
                    seq: seqs_by_block
                        .get(&block.block_index)
                        .copied()
                        .unwrap_or_default(),
                    data: Bytes::copy_from_slice(block.as_ref()),
                })
                .collect();
            let spill_batches = self.build_handle_writer_slice_spill_batches(snapshot_writes);
            return self
                .persist_handle_writer_spill_batches(ino, spill_batches, local_sync_policy)
                .await;
        }
        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else {
            self.current_content_epoch_async(ino).await?
        };
        let attr_to_persist = self
            .derive_opportunistic_prefix_attr(ino, dirty_attr.as_ref(), &completed_blocks)
            .await?;

        self.persist_dirty_write_snapshots_direct(
            ino,
            completed_blocks,
            content_epoch,
            attr_to_persist,
            local_sync_policy,
        )
        .await?;
        self.state
            .handle_writers
            .mark_blocks_materialized(ino, &seqs_by_block);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handle_writer_registry_tracks_dirty_and_clean_handle_state() {
        let registry = HandleWriterRegistry::new();
        let fh = 42;
        let ino = 7;

        registry.open_writer_for_test(fh, ino, 0);
        registry.write(fh, ino, 0, 4096, 0, b"abc");
        assert!(registry.has_dirty_writers(ino));

        let materialized = registry.snapshot_materialization_writes(ino);
        assert_eq!(materialized.len(), 1);
        assert_eq!(materialized[0].offset, 0);
        assert_eq!(materialized[0].data.as_ref(), b"abc");

        registry.mark_closed(fh);
        registry.mark_materialized(ino, materialized[0].seq);
        assert!(!registry.has_dirty_writers(ino));
    }

    #[test]
    fn handle_writer_registry_coalesces_overwritten_ranges() {
        let registry = HandleWriterRegistry::new();
        let fh = 7;
        let ino = 9;

        registry.open_writer_for_test(fh, ino, 0);
        registry.write(fh, ino, 0, 4096, 0, b"abcdef");
        registry.write(fh, ino, 6, 4096, 2, b"XY");

        let mut materialized = registry.snapshot_materialization_writes(ino);
        materialized.sort_by_key(|write| write.offset);
        assert_eq!(materialized.len(), 3);
        assert_eq!(materialized[0].offset, 0);
        assert_eq!(materialized[0].data.as_ref(), b"ab");
        assert_eq!(materialized[1].offset, 2);
        assert_eq!(materialized[1].data.as_ref(), b"XY");
        assert_eq!(materialized[2].offset, 4);
        assert_eq!(materialized[2].data.as_ref(), b"ef");

        let spans = registry.snapshot_read_spans(ino, 0, 6);
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].start, 2);
        assert_eq!(spans[0].data.as_ref(), b"XY");
    }

    #[test]
    fn handle_writer_registry_tracks_ready_full_blocks_incrementally() {
        let registry = HandleWriterRegistry::new();
        let fh = 11;
        let ino = 13;
        let first_half = vec![0x11_u8; 2048];
        let second_half = vec![0x22_u8; 2048];

        registry.open_writer_for_test(fh, ino, 4096);
        registry.write(fh, ino, 0, 4096, 0, &first_half);
        assert!(
            registry.snapshot_ready_full_block_indexes(ino).is_empty(),
            "partial coverage should not advertise a ready full block"
        );

        registry.write(fh, ino, 2048, 4096, 2048, &second_half);
        assert_eq!(
            registry.snapshot_ready_full_block_indexes(ino),
            HashSet::from([0]),
            "once the logical block is fully covered it should be marked ready"
        );

        let cutoff = registry
            .snapshot_materialization_writes(ino)
            .into_iter()
            .map(|write| write.seq)
            .max()
            .expect("materialized writes");
        registry.mark_blocks_materialized(ino, &HashMap::from([(0, cutoff)]));
        assert!(
            registry.snapshot_ready_full_block_indexes(ino).is_empty(),
            "marking the block materialized should clear the ready-full-block index"
        );
    }

    #[test]
    fn handle_writer_registry_clear_inode_drops_all_dirty_state() {
        let registry = HandleWriterRegistry::new();
        let ino = 11;

        registry.open_writer_for_test(1, ino, 0);
        registry.open_writer_for_test(2, ino, 0);
        registry.write(1, ino, 0, 4096, 0, b"hello");
        registry.write(2, ino, 5, 4096, 5, b"world");

        assert!(registry.has_dirty_writers(ino));
        assert_eq!(registry.dirty_bytes_for_inode(ino), 10);

        registry.clear_inode(ino);

        assert!(!registry.has_dirty_writers(ino));
        assert_eq!(registry.dirty_bytes_for_inode(ino), 0);
        assert_eq!(registry.total_dirty_bytes(), 0);
    }

    #[test]
    fn handle_writer_registry_splits_write_into_block_slices_without_extra_copying() {
        let registry = HandleWriterRegistry::new();
        let fh = 99;
        let ino = 17;
        let payload = Bytes::from_static(b"abcdefgh");

        registry.write_bytes(fh, ino, 0, 4, 0, payload.clone());

        let mut materialized = registry.snapshot_materialization_writes(ino);
        materialized.sort_by_key(|write| write.offset);
        assert_eq!(materialized.len(), 2);
        assert_eq!(materialized[0].data.as_ref(), b"abcd");
        assert_eq!(materialized[1].data.as_ref(), b"efgh");
        assert_eq!(materialized[0].data.as_ptr(), payload.as_ptr());
        assert_eq!(
            materialized[1].data.as_ptr(),
            payload.as_ptr().wrapping_add(4)
        );
    }

    #[test]
    fn handle_writer_registry_shards_independent_inode_writes_without_lost_dirty_state() {
        let registry = Arc::new(HandleWriterRegistry::new());
        let first_ino = 1;
        let second_ino = 2;

        assert_ne!(
            registry.shard_index_for_inode_for_test(first_ino),
            registry.shard_index_for_inode_for_test(second_ino),
            "test inodes should exercise distinct writer-state shards"
        );

        let first_registry = Arc::clone(&registry);
        let first = std::thread::spawn(move || {
            first_registry.write(101, first_ino, 0, 4096, 0, b"alpha");
        });
        let second_registry = Arc::clone(&registry);
        let second = std::thread::spawn(move || {
            second_registry.write(202, second_ino, 0, 4096, 0, b"bravo!");
        });

        first.join().expect("first writer thread should not panic");
        second
            .join()
            .expect("second writer thread should not panic");

        assert_eq!(registry.dirty_bytes_for_inode(first_ino), 5);
        assert_eq!(registry.dirty_bytes_for_inode(second_ino), 6);
        assert_eq!(registry.total_dirty_bytes(), 11);

        let first_writes = registry.snapshot_materialization_writes(first_ino);
        let second_writes = registry.snapshot_materialization_writes(second_ino);
        assert_eq!(first_writes.len(), 1);
        assert_eq!(first_writes[0].data.as_ref(), b"alpha");
        assert_eq!(second_writes.len(), 1);
        assert_eq!(second_writes[0].data.as_ref(), b"bravo!");
    }
}
