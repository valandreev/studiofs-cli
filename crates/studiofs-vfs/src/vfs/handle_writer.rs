use super::{Bytes, FileType, Ino, LocalSyncPolicy, StudioVfs, VfsResult, now_unix_secs};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub(super) struct HandleWriterReadSpan {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) block_offset: u32,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
pub(super) struct BufferedHandleWrite {
    pub(super) offset: u64,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
struct HandleSliceWriter {
    ino: Ino,
    logical_size: u64,
    open: bool,
    dirty_bytes: usize,
    writes: BTreeMap<u64, BufferedHandleWrite>,
}

#[derive(Debug, Default)]
struct HandleWriterRegistryState {
    writers: HashMap<u64, HandleSliceWriter>,
    writers_by_ino: HashMap<Ino, HashSet<u64>>,
    total_dirty_bytes: usize,
}

#[derive(Debug)]
pub(super) struct HandleWriterRegistry {
    writers: Mutex<HandleWriterRegistryState>,
    next_seq: AtomicU64,
}

impl HandleSliceWriter {
    fn new(ino: Ino, current_size: u64) -> Self {
        Self {
            ino,
            logical_size: current_size,
            open: true,
            dirty_bytes: 0,
            writes: BTreeMap::new(),
        }
    }

    fn upsert_write(&mut self, seq: u64, offset: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        let payload = Bytes::copy_from_slice(data);
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
                self.insert_slice(start, existing.seq, existing.data.slice(0..keep_len));
            }
            if existing_end > end {
                let keep_from = (end - start) as usize;
                self.insert_slice(end, existing.seq, existing.data.slice(keep_from..));
            }
        }

        self.insert_slice(offset, seq, payload);
        self.logical_size = self.logical_size.max(end);
    }

    fn insert_slice(&mut self, offset: u64, seq: u64, data: Bytes) {
        if data.is_empty() {
            return;
        }
        self.dirty_bytes = self.dirty_bytes.saturating_add(data.len());
        self.writes
            .insert(offset, BufferedHandleWrite { offset, seq, data });
    }

    fn snapshot_read_spans(&self, offset: u64, end: u64, spans: &mut Vec<HandleWriterReadSpan>) {
        for write in self.writes.values() {
            let write_end = write.offset.saturating_add(write.data.len() as u64);
            let start = write.offset.max(offset);
            let stop = write_end.min(end);
            if start >= stop {
                continue;
            }
            let delta = start.saturating_sub(write.offset);
            let Some(block_offset) = u32::try_from(delta).ok() else {
                continue;
            };
            spans.push(HandleWriterReadSpan {
                start,
                end: stop,
                block_offset,
                seq: write.seq,
                data: write.data.clone(),
            });
        }
    }

    fn snapshot_materialization_writes(&self, out: &mut Vec<BufferedHandleWrite>) {
        out.extend(self.writes.values().cloned());
    }

    fn mark_materialized(&mut self, seq: u64) {
        self.writes.retain(|_, write| write.seq > seq);
        self.dirty_bytes = self.writes.values().map(|write| write.data.len()).sum();
    }
}

impl HandleWriterRegistry {
    pub(super) fn new() -> Self {
        Self {
            writers: Mutex::new(HandleWriterRegistryState::default()),
            next_seq: AtomicU64::new(1),
        }
    }

    pub(super) fn write(
        &self,
        fh: u64,
        ino: Ino,
        current_size: u64,
        offset: u64,
        data: &[u8],
    ) -> u32 {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let mut writers = self.writers.lock();
        let previous_ino = writers.writers.get(&fh).map(|writer| writer.ino);
        let (old_dirty_bytes, new_dirty_bytes) = {
            let writer = writers
                .writers
                .entry(fh)
                .or_insert_with(|| HandleSliceWriter::new(ino, current_size));
            let old_dirty_bytes = writer.dirty_bytes;
            writer.ino = ino;
            writer.open = true;
            writer.logical_size = writer.logical_size.max(current_size);
            writer.upsert_write(seq, offset, data);
            (old_dirty_bytes, writer.dirty_bytes)
        };
        Self::adjust_total_dirty_bytes(
            &mut writers.total_dirty_bytes,
            old_dirty_bytes,
            new_dirty_bytes,
        );
        Self::reindex_writer(&mut writers, fh, previous_ino, ino);
        data.len() as u32
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
        let writers = self.writers.lock();
        let mut spans = Vec::new();
        for fh in writers.writers_by_ino.get(&ino).into_iter().flatten() {
            if let Some(writer) = writers.writers.get(fh) {
                writer.snapshot_read_spans(offset, end, &mut spans);
            }
        }
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    pub(super) fn max_visible_size(&self, ino: Ino) -> Option<u64> {
        let writers = self.writers.lock();
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
        let writers = self.writers.lock();
        writers.writers_by_ino.get(&ino).is_some_and(|handles| {
            handles
                .iter()
                .filter_map(|fh| writers.writers.get(fh))
                .any(|writer| !writer.writes.is_empty())
        })
    }

    pub(super) fn dirty_bytes_for_inode(&self, ino: Ino) -> usize {
        let writers = self.writers.lock();
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
        self.writers.lock().total_dirty_bytes
    }

    pub(super) fn mark_closed(&self, fh: u64) {
        let mut writers = self.writers.lock();
        let remove = if let Some(writer) = writers.writers.get_mut(&fh) {
            writer.open = false;
            writer.writes.is_empty()
        } else {
            false
        };
        if remove {
            Self::remove_writer(&mut writers, fh);
        }
    }

    pub(super) fn snapshot_materialization_writes(&self, ino: Ino) -> Vec<BufferedHandleWrite> {
        let writers = self.writers.lock();
        let mut writes = Vec::new();
        for fh in writers.writers_by_ino.get(&ino).into_iter().flatten() {
            if let Some(writer) = writers.writers.get(fh) {
                writer.snapshot_materialization_writes(&mut writes);
            }
        }
        writes.sort_by_key(|write| write.seq);
        writes
    }

    pub(super) fn mark_materialized(&self, ino: Ino, seq: u64) {
        let mut writers = self.writers.lock();
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
            Self::adjust_total_dirty_bytes(
                &mut writers.total_dirty_bytes,
                old_dirty_bytes,
                new_dirty_bytes,
            );
            if remove_closed {
                empty_closed.push(fh);
            }
        }
        for fh in empty_closed {
            Self::remove_writer(&mut writers, fh);
        }
    }

    #[cfg(test)]
    pub(super) fn open_writer_for_test(&self, fh: u64, ino: Ino, current_size: u64) {
        let mut writers = self.writers.lock();
        if let std::collections::hash_map::Entry::Vacant(entry) = writers.writers.entry(fh) {
            entry.insert(HandleSliceWriter::new(ino, current_size));
            writers.writers_by_ino.entry(ino).or_default().insert(fh);
        }
    }

    fn adjust_total_dirty_bytes(total: &mut usize, old_bytes: usize, new_bytes: usize) {
        match new_bytes.cmp(&old_bytes) {
            std::cmp::Ordering::Greater => {
                *total = total.saturating_add(new_bytes - old_bytes);
            }
            std::cmp::Ordering::Less => {
                *total = total.saturating_sub(old_bytes - new_bytes);
            }
            std::cmp::Ordering::Equal => {}
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

    fn remove_writer(state: &mut HandleWriterRegistryState, fh: u64) {
        let Some(writer) = state.writers.remove(&fh) else {
            return;
        };
        state.total_dirty_bytes = state.total_dirty_bytes.saturating_sub(writer.dirty_bytes);
        Self::remove_writer_from_inode_index(state, writer.ino, fh);
    }
}

impl StudioVfs {
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

    pub(super) async fn record_handle_writer_state(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
    ) -> VfsResult<u32> {
        let mut base_attr =
            if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino).cloned() {
                attr
            } else {
                self.getattr_async(ino).await?
            };
        self.refresh_inode_incarnation_state_from_attr(ino, &base_attr);
        self.begin_pending_incarnation(ino, &mut base_attr);
        let written = self
            .state
            .handle_writers
            .write(fh, ino, base_attr.size, offset, data);
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
            attr.mtime = now_unix_secs();
            attr.ctime = attr.mtime;
            self.update_pending_incarnation_size(ino, attr.size);
        }
        Ok(written)
    }

    pub(super) fn should_auto_materialize_handle_writers(&self, ino: Ino) -> bool {
        let per_inode_dirty = self.state.handle_writers.dirty_bytes_for_inode(ino);
        if per_inode_dirty == 0 {
            return false;
        }
        per_inode_dirty >= self.write_eager_flush_batch_bytes
            || self.state.handle_writers.total_dirty_bytes() >= self.max_dirty_bytes()
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
        let max_seq = writes
            .last()
            .map(|write| write.seq)
            .expect("non-empty writes have a max seq");
        let buffered_writes: Vec<(u64, Bytes)> = writes
            .into_iter()
            .map(|write| (write.offset, write.data))
            .collect();
        self.materialize_buffered_writes_direct(ino, &buffered_writes, local_sync_policy)
            .await?;
        self.state.handle_writers.mark_materialized(ino, max_seq);
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
        registry.write(fh, ino, 0, 0, b"abc");
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
        registry.write(fh, ino, 0, 0, b"abcdef");
        registry.write(fh, ino, 6, 2, b"XY");

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
}
