use super::{Bytes, FileType, Ino, LocalSyncPolicy, StudioVfs, VfsResult, now_unix_secs};
use parking_lot::Mutex;
use std::collections::HashMap;
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
    writes: Vec<BufferedHandleWrite>,
}

#[derive(Debug)]
pub(super) struct HandleWriterRegistry {
    writers: Mutex<HashMap<u64, HandleSliceWriter>>,
    next_seq: AtomicU64,
}

impl HandleWriterRegistry {
    pub(super) fn new() -> Self {
        Self {
            writers: Mutex::new(HashMap::new()),
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
        let writer = writers.entry(fh).or_insert_with(|| HandleSliceWriter {
            ino,
            logical_size: current_size,
            open: true,
            writes: Vec::new(),
        });
        writer.ino = ino;
        writer.open = true;
        writer.logical_size = writer
            .logical_size
            .max(current_size)
            .max(offset.saturating_add(data.len() as u64));
        writer.writes.push(BufferedHandleWrite {
            offset,
            seq,
            data: Bytes::copy_from_slice(data),
        });
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
        for writer in writers.values() {
            if writer.ino != ino {
                continue;
            }
            for write in &writer.writes {
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
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    pub(super) fn max_visible_size(&self, ino: Ino) -> Option<u64> {
        let writers = self.writers.lock();
        writers
            .values()
            .filter(|writer| writer.ino == ino && !writer.writes.is_empty())
            .map(|writer| writer.logical_size)
            .max()
    }

    pub(super) fn has_dirty_writers(&self, ino: Ino) -> bool {
        let writers = self.writers.lock();
        writers
            .values()
            .any(|writer| writer.ino == ino && !writer.writes.is_empty())
    }

    pub(super) fn mark_closed(&self, fh: u64) {
        let mut writers = self.writers.lock();
        let remove = if let Some(writer) = writers.get_mut(&fh) {
            writer.open = false;
            writer.writes.is_empty()
        } else {
            false
        };
        if remove {
            writers.remove(&fh);
        }
    }

    pub(super) fn snapshot_materialization_writes(&self, ino: Ino) -> Vec<BufferedHandleWrite> {
        let writers = self.writers.lock();
        let mut writes: Vec<_> = writers
            .values()
            .filter(|writer| writer.ino == ino)
            .flat_map(|writer| writer.writes.iter().cloned())
            .collect();
        writes.sort_by_key(|write| write.seq);
        writes
    }

    pub(super) fn mark_materialized(&self, ino: Ino, seq: u64) {
        let mut writers = self.writers.lock();
        let mut empty_closed = Vec::new();
        for (fh, writer) in writers.iter_mut() {
            if writer.ino != ino {
                continue;
            }
            writer.writes.retain(|write| write.seq > seq);
            if writer.writes.is_empty() && !writer.open {
                empty_closed.push(*fh);
            }
        }
        for fh in empty_closed {
            writers.remove(&fh);
        }
    }

    #[cfg(test)]
    pub(super) fn open_writer_for_test(&self, fh: u64, ino: Ino, current_size: u64) {
        let mut writers = self.writers.lock();
        writers.entry(fh).or_insert_with(|| HandleSliceWriter {
            ino,
            logical_size: current_size,
            open: true,
            writes: Vec::new(),
        });
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
        let mut base_attr = self.getattr_async(ino).await?;
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
}
