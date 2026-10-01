use bytes::Bytes;
use parking_lot::Mutex;
use smallvec::{SmallVec, smallvec};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use studiofs_cache::{
    CachedStore, CachedStoreWriterConfig, CachedStoreWriterError, CachedStoreWriterSlice,
    DetachedStagedSlice, StagedSlice,
};
use studiofs_meta::schema::{ChunkId, chunk_id_from_offset, chunk_start_offset};
use studiofs_meta::types::Ino;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct WriterStackReadSpan {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) block_offset: u64,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WriterStackReadClaim {
    pub(super) start: u64,
    pub(super) end: u64,
}

#[derive(Debug, Clone)]
enum WriterStackReadSource {
    Materializing {
        start: u64,
        end: u64,
        source_start: u64,
        seq: u64,
        data: Bytes,
    },
    Active {
        start: u64,
        end: u64,
        seq: u64,
        chunk_id: ChunkId,
        slice_index: usize,
    },
}

impl WriterStackReadSource {
    fn start(&self) -> u64 {
        match self {
            Self::Materializing { start, .. } | Self::Active { start, .. } => *start,
        }
    }

    fn end(&self) -> u64 {
        match self {
            Self::Materializing { end, .. } | Self::Active { end, .. } => *end,
        }
    }

    fn seq(&self) -> u64 {
        match self {
            Self::Materializing { seq, .. } | Self::Active { seq, .. } => *seq,
        }
    }
}

type WriterStackReadClaimList = SmallVec<[WriterStackReadClaim; 8]>;
type WriterStackReadSourceList = SmallVec<[WriterStackReadSource; 8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FinishedWriterSlice {
    pub(super) ino: Ino,
    pub(super) chunk_id: ChunkId,
    pub(super) chunk_offset: u32,
    pub(super) len: u32,
    pub(super) object_id: u64,
    pub(super) object_offset: u32,
    pub(super) seq: u64,
    pub(super) content_epoch: u64,
}

#[derive(Debug, Clone)]
struct MaterializingReadSpan {
    start: u64,
    end: u64,
    object_id: u64,
    seq: u64,
    data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(super) struct WriterAccept {
    pub(super) accepted_bytes: u32,
    pub(super) dirty_bytes: usize,
    pub(super) visible_len: u64,
    pub(super) throttled: bool,
    pub(super) next_object_id: u64,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub(super) struct WriterStackConfig {
    pub(super) block_size: u32,
    pub(super) chunk_size: u32,
    pub(super) page_size: usize,
    pub(super) dirty_byte_limit: Option<usize>,
    pub(super) max_buffer_bytes: Option<usize>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(super) struct WriterWriteSpec {
    pub(super) fh: u64,
    pub(super) ino: Ino,
    pub(super) current_size: u64,
    pub(super) content_epoch: u64,
    pub(super) first_object_id: u64,
    pub(super) offset: u64,
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub(super) enum WriterStackError {
    #[error("writer geometry must use non-zero block, chunk, and page sizes")]
    InvalidGeometry,
    #[error("writer offset range overflow")]
    OffsetOverflow,
    #[error("write length exceeds u32::MAX")]
    WriteTooLarge,
    #[error("writer stack object reservation exceeds u32::MAX")]
    ObjectReservationTooLarge,
    #[error("cached-store writer failed: {source}")]
    CachedStore {
        #[source]
        source: Arc<CachedStoreWriterError>,
    },
    #[error("sticky writer error for inode {ino}: {message}")]
    Sticky {
        ino: Ino,
        message: String,
        #[source]
        source: Option<Arc<CachedStoreWriterError>>,
    },
}

impl From<CachedStoreWriterError> for WriterStackError {
    fn from(error: CachedStoreWriterError) -> Self {
        Self::CachedStore {
            source: Arc::new(error),
        }
    }
}

#[allow(dead_code)]
struct SliceWriter {
    seq: u64,
    start: u64,
    end: u64,
    chunk_offset: u32,
    content_epoch: u64,
    writer: CachedStoreWriterSlice,
    frozen: bool,
    finished: Vec<FinishedWriterSlice>,
    materializing: Vec<MaterializingReadSpan>,
    pending_materialization: Vec<DetachedFinishedWriterSlice>,
}

#[derive(Clone)]
struct DetachedFinishedWriterSlice {
    finished: FinishedWriterSlice,
    detached: DetachedStagedSlice,
}

#[allow(dead_code)]
struct ChunkWriter {
    chunk_id: ChunkId,
    slices: Vec<SliceWriter>,
}

#[allow(dead_code)]
struct FileWriter {
    ino: Ino,
    visible_len: u64,
    next_object_id: u64,
    chunks: BTreeMap<ChunkId, ChunkWriter>,
    dirty_bytes: usize,
    last_write: Instant,
    sticky_error: Option<String>,
    sticky_error_source: Option<Arc<CachedStoreWriterError>>,
}

#[derive(Default)]
#[allow(dead_code)]
struct WriterShard {
    files: HashMap<Ino, Arc<Mutex<FileWriter>>>,
    handle_counts_by_ino: HashMap<Ino, usize>,
    dirty_inodes: HashSet<Ino>,
}

#[derive(Default)]
#[allow(dead_code)]
struct HandleShard {
    ino_by_handle: HashMap<u64, Ino>,
}

enum WriterPayload<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl WriterPayload<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Borrowed(data) => data.len(),
            Self::Owned(data) => data.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn borrowed_slice(&self, range: std::ops::Range<usize>) -> &[u8] {
        match self {
            Self::Borrowed(data) => &data[range],
            Self::Owned(data) => &data[range],
        }
    }

    fn owned_slice(&self, range: std::ops::Range<usize>) -> Bytes {
        match self {
            Self::Borrowed(data) => Bytes::copy_from_slice(&data[range]),
            Self::Owned(data) => data.slice(range),
        }
    }
}

#[allow(dead_code)]
pub(super) struct MountedWriterStack {
    cached_store: Arc<CachedStore>,
    config: WriterStackConfig,
    shards: Box<[Mutex<WriterShard>]>,
    handle_shards: Box<[Mutex<HandleShard>]>,
    next_seq: AtomicU64,
    dirty_bytes: AtomicUsize,
}

#[allow(dead_code)]
impl MountedWriterStack {
    const SHARDS: usize = 64;

    pub(super) fn new(cached_store: Arc<CachedStore>, config: WriterStackConfig) -> Self {
        Self {
            cached_store,
            config,
            shards: (0..Self::SHARDS)
                .map(|_| Mutex::new(WriterShard::default()))
                .collect(),
            handle_shards: (0..Self::SHARDS)
                .map(|_| Mutex::new(HandleShard::default()))
                .collect(),
            next_seq: AtomicU64::new(1),
            dirty_bytes: AtomicUsize::new(0),
        }
    }

    pub(super) fn page_size(&self) -> usize {
        self.config.page_size
    }

    pub(super) fn write_bytes<D>(
        &self,
        spec: WriterWriteSpec,
        data: D,
    ) -> Result<WriterAccept, WriterStackError>
    where
        D: AsRef<[u8]>,
    {
        self.write_payload(spec, WriterPayload::Borrowed(data.as_ref()))
    }

    pub(super) fn write_owned_bytes(
        &self,
        spec: WriterWriteSpec,
        data: Bytes,
    ) -> Result<WriterAccept, WriterStackError> {
        self.write_payload(spec, WriterPayload::Owned(data))
    }

    fn write_payload(
        &self,
        spec: WriterWriteSpec,
        data: WriterPayload<'_>,
    ) -> Result<WriterAccept, WriterStackError> {
        self.validate_geometry()?;
        if data.is_empty() {
            let next_object_id =
                self.next_object_id_for_empty_write(spec.ino, spec.first_object_id);
            return Ok(WriterAccept {
                accepted_bytes: 0,
                dirty_bytes: self.total_dirty_bytes(),
                visible_len: spec.current_size,
                throttled: false,
                next_object_id,
            });
        }
        let accepted_bytes =
            u32::try_from(data.len()).map_err(|_| WriterStackError::WriteTooLarge)?;
        spec.offset
            .checked_add(data.len() as u64)
            .ok_or(WriterStackError::OffsetOverflow)?;

        self.associate_handle(spec.fh, spec.ino);
        let shard_index = Self::inode_shard_index(spec.ino);
        let file = {
            let mut shard = self.shards[shard_index].lock();
            Arc::clone(shard.files.entry(spec.ino).or_insert_with(|| {
                Arc::new(Mutex::new(FileWriter {
                    ino: spec.ino,
                    visible_len: spec.current_size,
                    next_object_id: spec.first_object_id,
                    chunks: BTreeMap::new(),
                    dirty_bytes: 0,
                    last_write: Instant::now(),
                    sticky_error: None,
                    sticky_error_source: None,
                }))
            }))
        };
        let mut file = file.lock();
        if let Some(message) = file.sticky_error.clone() {
            return Err(WriterStackError::Sticky {
                ino: spec.ino,
                message,
                source: file.sticky_error_source.clone(),
            });
        }
        if file.next_object_id < spec.first_object_id {
            file.next_object_id = spec.first_object_id;
        }
        file.visible_len = file.visible_len.max(spec.current_size);

        let mut cursor = spec.offset;
        let mut data_offset = 0usize;
        let mut next_object_id = file.next_object_id;
        let mut throttled = false;
        let mut accepted_dirty_bytes = 0usize;
        while data_offset < data.len() {
            let chunk_id = chunk_id_from_offset(cursor, self.config.chunk_size);
            let chunk_start = chunk_start_offset(chunk_id, self.config.chunk_size);
            let chunk_end = chunk_start.saturating_add(u64::from(self.config.chunk_size));
            let take = (chunk_end - cursor).min((data.len() - data_offset) as u64) as usize;
            let segment_range = data_offset..data_offset + take;
            let end = cursor
                .checked_add(take as u64)
                .ok_or(WriterStackError::OffsetOverflow)?;
            let (accepted_delta, object_ids_used, segment_throttled) = {
                let chunk = file.chunks.entry(chunk_id).or_insert_with(|| ChunkWriter {
                    chunk_id,
                    slices: Vec::new(),
                });
                if let Some(slice) = chunk.slices.last_mut().filter(|slice| {
                    !slice.frozen
                        && slice.end == cursor
                        && slice.content_epoch == spec.content_epoch
                }) {
                    let before_objects =
                        staged_object_count(slice.writer.dirty_bytes(), self.config.block_size);
                    let result = match &data {
                        WriterPayload::Borrowed(_) => slice
                            .writer
                            .write_at(cursor, data.borrowed_slice(segment_range))?,
                        WriterPayload::Owned(_) => slice
                            .writer
                            .write_owned_at(cursor, data.owned_slice(segment_range))?,
                    };
                    let after_objects =
                        staged_object_count(result.dirty_bytes, self.config.block_size);
                    slice.end = end;
                    (
                        result.accepted_bytes,
                        after_objects.saturating_sub(before_objects),
                        result.throttled,
                    )
                } else {
                    let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
                    let mut writer = self
                        .cached_store
                        .open_writer_slice(CachedStoreWriterConfig {
                            ino: spec.ino,
                            base_offset: cursor,
                            first_object_id: next_object_id,
                            content_epoch: spec.content_epoch,
                            block_size: self.config.block_size as usize,
                            page_size: self.config.page_size,
                            dirty_byte_limit: self.config.dirty_byte_limit,
                            max_buffer_bytes: self.config.max_buffer_bytes,
                            submit_upload: false,
                        });
                    let result = match &data {
                        WriterPayload::Borrowed(_) => {
                            writer.write_at(cursor, data.borrowed_slice(segment_range))?
                        }
                        WriterPayload::Owned(_) => {
                            writer.write_owned_at(cursor, data.owned_slice(segment_range))?
                        }
                    };
                    let object_ids_used =
                        staged_object_count(result.dirty_bytes, self.config.block_size);
                    chunk.slices.push(SliceWriter {
                        seq,
                        start: cursor,
                        end,
                        chunk_offset: cursor.saturating_sub(chunk_start) as u32,
                        content_epoch: spec.content_epoch,
                        writer,
                        frozen: false,
                        finished: Vec::new(),
                        materializing: Vec::new(),
                        pending_materialization: Vec::new(),
                    });
                    (result.accepted_bytes, object_ids_used, result.throttled)
                }
            };
            throttled |= segment_throttled;
            file.visible_len = file.visible_len.max(end);
            file.dirty_bytes = file.dirty_bytes.saturating_add(accepted_delta);
            accepted_dirty_bytes = accepted_dirty_bytes.saturating_add(accepted_delta);
            self.dirty_bytes.fetch_add(accepted_delta, Ordering::AcqRel);
            cursor = end;
            data_offset += take;
            next_object_id = next_object_id.saturating_add(object_ids_used);
            file.next_object_id = next_object_id;
        }
        if accepted_dirty_bytes > 0 {
            file.last_write = Instant::now();
        }
        let visible_len = file.visible_len;
        drop(file);
        if accepted_dirty_bytes > 0 {
            self.shards[shard_index]
                .lock()
                .dirty_inodes
                .insert(spec.ino);
        }

        Ok(WriterAccept {
            accepted_bytes,
            dirty_bytes: self.total_dirty_bytes(),
            visible_len,
            throttled,
            next_object_id,
        })
    }

    pub(super) fn snapshot_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<WriterStackReadSpan> {
        let Some(end) = offset.checked_add(length) else {
            return Vec::new();
        };
        if length == 0 {
            return Vec::new();
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return Vec::new();
            };
            Arc::clone(file)
        };
        let file = file.lock();
        if file.ino != ino {
            return Vec::new();
        }
        let mut spans = Vec::new();
        let start_chunk = chunk_id_from_offset(offset, self.config.chunk_size);
        let end_chunk = chunk_id_from_offset(end.saturating_sub(1), self.config.chunk_size);
        for chunk in file
            .chunks
            .range(start_chunk..=end_chunk)
            .map(|(_, chunk)| chunk)
        {
            for slice in &chunk.slices {
                for materializing in &slice.materializing {
                    let start = materializing.start.max(offset);
                    let stop = materializing.end.min(end);
                    if start >= stop {
                        continue;
                    }
                    spans.push(WriterStackReadSpan {
                        start,
                        end: stop,
                        block_offset: start.saturating_sub(materializing.start),
                        seq: materializing.seq,
                        data: materializing.data.clone(),
                    });
                }
                let start = slice.start.max(offset);
                let stop = slice.end.min(end);
                if start >= stop {
                    continue;
                }
                let len = usize::try_from(stop.saturating_sub(start)).unwrap_or(usize::MAX);
                let mut data = vec![0; len];
                let read = slice.writer.read_accepted(start, &mut data);
                if read == 0 {
                    continue;
                }
                data.truncate(read);
                spans.push(WriterStackReadSpan {
                    start,
                    end: start.saturating_add(read as u64),
                    block_offset: 0,
                    seq: slice.seq,
                    data: Bytes::from(data),
                });
            }
        }
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    pub(super) fn snapshot_read_claims(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> WriterStackReadClaimList {
        let Some(end) = offset.checked_add(length) else {
            return WriterStackReadClaimList::new();
        };
        if length == 0 {
            return WriterStackReadClaimList::new();
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return WriterStackReadClaimList::new();
            };
            Arc::clone(file)
        };
        let file = file.lock();
        if file.ino != ino {
            return WriterStackReadClaimList::new();
        }
        let mut sources = Self::collect_read_sources(&file, offset, end, self.config.chunk_size);
        sources.sort_by_key(|source| std::cmp::Reverse(source.seq()));
        Self::claim_read_sources(&sources, offset, end, |_source, claim| Some(claim))
    }

    pub(super) fn copy_read_spans_into(
        &self,
        ino: Ino,
        offset: u64,
        out: &mut [u8],
    ) -> WriterStackReadClaimList {
        let Ok(length) = u64::try_from(out.len()) else {
            return WriterStackReadClaimList::new();
        };
        let Some(end) = offset.checked_add(length) else {
            return WriterStackReadClaimList::new();
        };
        if out.is_empty() {
            return WriterStackReadClaimList::new();
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return WriterStackReadClaimList::new();
            };
            Arc::clone(file)
        };
        let file = file.lock();
        if file.ino != ino {
            return WriterStackReadClaimList::new();
        }
        let mut sources = Self::collect_read_sources(&file, offset, end, self.config.chunk_size);
        sources.sort_by_key(|source| std::cmp::Reverse(source.seq()));
        Self::claim_read_sources(&sources, offset, end, |source, claim| {
            let dst_start = usize::try_from(claim.start.saturating_sub(offset)).ok()?;
            let dst_end = usize::try_from(claim.end.saturating_sub(offset)).ok()?;
            if dst_start >= out.len() || dst_end > out.len() || dst_start >= dst_end {
                return None;
            }
            let dst = &mut out[dst_start..dst_end];
            match source {
                WriterStackReadSource::Materializing {
                    source_start, data, ..
                } => {
                    let src_start =
                        usize::try_from(claim.start.saturating_sub(*source_start)).ok()?;
                    let src_end = src_start.checked_add(dst.len())?;
                    if src_end > data.len() {
                        return None;
                    }
                    dst.copy_from_slice(&data.as_ref()[src_start..src_end]);
                    Some(claim)
                }
                WriterStackReadSource::Active {
                    seq,
                    chunk_id,
                    slice_index,
                    ..
                } => {
                    let slice = file
                        .chunks
                        .get(chunk_id)
                        .and_then(|chunk| chunk.slices.get(*slice_index))
                        .filter(|slice| slice.seq == *seq)?;
                    let read = slice.writer.read_accepted(claim.start, dst);
                    assert_eq!(
                        read,
                        dst.len(),
                        "writer-stack active source claim must be fully readable"
                    );
                    Some(claim)
                }
            }
        })
    }

    fn collect_read_sources(
        file: &FileWriter,
        offset: u64,
        end: u64,
        chunk_size: u32,
    ) -> WriterStackReadSourceList {
        let mut sources = WriterStackReadSourceList::new();
        let start_chunk = chunk_id_from_offset(offset, chunk_size);
        let end_chunk = chunk_id_from_offset(end.saturating_sub(1), chunk_size);
        for chunk in file
            .chunks
            .range(start_chunk..=end_chunk)
            .map(|(_, chunk)| chunk)
        {
            for (slice_index, slice) in chunk.slices.iter().enumerate() {
                for materializing in &slice.materializing {
                    let start = materializing.start.max(offset);
                    let stop = materializing.end.min(end);
                    if start >= stop {
                        continue;
                    }
                    sources.push(WriterStackReadSource::Materializing {
                        start,
                        end: stop,
                        source_start: materializing.start,
                        seq: materializing.seq,
                        data: materializing.data.clone(),
                    });
                }
                let Some((accepted_start, accepted_end)) = slice.writer.accepted_range() else {
                    continue;
                };
                let start = accepted_start.max(offset);
                let stop = accepted_end.min(end);
                if start >= stop {
                    continue;
                }
                sources.push(WriterStackReadSource::Active {
                    start,
                    end: stop,
                    seq: slice.seq,
                    chunk_id: chunk.chunk_id,
                    slice_index,
                });
            }
        }
        sources
    }

    fn claim_read_sources<F>(
        sources: &[WriterStackReadSource],
        offset: u64,
        end: u64,
        mut on_claim: F,
    ) -> WriterStackReadClaimList
    where
        F: FnMut(&WriterStackReadSource, WriterStackReadClaim) -> Option<WriterStackReadClaim>,
    {
        let mut unresolved = smallvec![WriterStackReadClaim { start: offset, end }];
        let mut scratch = WriterStackReadClaimList::new();
        let mut claimed = WriterStackReadClaimList::new();
        let mut copied = WriterStackReadClaimList::new();
        for source in sources {
            if unresolved.is_empty() {
                break;
            }
            let candidate = WriterStackReadClaim {
                start: source.start(),
                end: source.end(),
            };
            Self::claim_unresolved_read_claims(
                &mut unresolved,
                candidate,
                &mut scratch,
                &mut claimed,
            );
            for claim in claimed.iter().copied() {
                let Some(copied_claim) = on_claim(source, claim) else {
                    continue;
                };
                if copied_claim.start < copied_claim.end {
                    copied.push(copied_claim);
                }
            }
        }
        copied.sort_by_key(|claim| claim.start);
        copied
    }

    fn claim_unresolved_read_claims(
        unresolved: &mut WriterStackReadClaimList,
        candidate: WriterStackReadClaim,
        scratch: &mut WriterStackReadClaimList,
        claimed: &mut WriterStackReadClaimList,
    ) {
        scratch.clear();
        claimed.clear();
        for range in unresolved.drain(..) {
            if range.end <= candidate.start || range.start >= candidate.end {
                scratch.push(range);
                continue;
            }
            if range.start < candidate.start {
                scratch.push(WriterStackReadClaim {
                    start: range.start,
                    end: candidate.start,
                });
            }
            let claim_start = range.start.max(candidate.start);
            let claim_end = range.end.min(candidate.end);
            if claim_start < claim_end {
                claimed.push(WriterStackReadClaim {
                    start: claim_start,
                    end: claim_end,
                });
            }
            if candidate.end < range.end {
                scratch.push(WriterStackReadClaim {
                    start: candidate.end,
                    end: range.end,
                });
            }
        }
        std::mem::swap(unresolved, scratch);
    }

    pub(super) fn finish_inode(
        &self,
        ino: Ino,
    ) -> Result<Vec<FinishedWriterSlice>, WriterStackError> {
        self.prepare_finish_inode(ino)?;
        let finished = self.finish_prepared_inode_with_object_ids(ino, None)?;
        let object_ids = finished
            .iter()
            .map(|slice| slice.object_id)
            .collect::<Vec<_>>();
        self.clear_materializing_object_ids(ino, &object_ids);
        Ok(finished)
    }

    pub(super) fn prepare_finish_inode(&self, ino: Ino) -> Result<u32, WriterStackError> {
        self.prepare_finish_inode_with_idle_guard(ino, None)
    }

    pub(super) fn prepare_finish_inode_if_idle(
        &self,
        ino: Ino,
        min_idle: Duration,
    ) -> Result<u32, WriterStackError> {
        self.prepare_finish_inode_with_idle_guard(ino, Some(min_idle))
    }

    fn prepare_finish_inode_with_idle_guard(
        &self,
        ino: Ino,
        min_idle: Option<Duration>,
    ) -> Result<u32, WriterStackError> {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return Ok(0);
            };
            Arc::clone(file)
        };
        let mut file = file.lock();
        if file.ino != ino {
            return Ok(0);
        };
        if min_idle.is_some_and(|idle| file.last_write.elapsed() < idle) {
            return Ok(0);
        }
        let mut object_count = 0u64;
        for chunk in file.chunks.values_mut() {
            for slice in &mut chunk.slices {
                if !slice.frozen && slice.writer.dirty_bytes() > 0 {
                    slice.frozen = true;
                    object_count = object_count.saturating_add(staged_object_count(
                        slice.writer.dirty_bytes(),
                        self.config.block_size,
                    ));
                }
            }
        }
        u32::try_from(object_count).map_err(|_| WriterStackError::ObjectReservationTooLarge)
    }

    pub(super) fn finish_prepared_inode(
        &self,
        ino: Ino,
        first_object_id: u64,
    ) -> Result<Vec<FinishedWriterSlice>, WriterStackError> {
        self.finish_prepared_inode_with_object_ids(ino, Some(first_object_id))
    }

    fn finish_prepared_inode_with_object_ids(
        &self,
        ino: Ino,
        first_object_id: Option<u64>,
    ) -> Result<Vec<FinishedWriterSlice>, WriterStackError> {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return Ok(Vec::new());
            };
            Arc::clone(file)
        };
        let mut file = file.lock();
        if file.ino != ino {
            return Ok(Vec::new());
        };
        let mut finished = Vec::new();
        let mut detached_finished = Vec::new();
        let mut staged_dirty_bytes = 0usize;
        let chunk_size = self.config.chunk_size;
        let mut next_object_id = first_object_id;
        for chunk in file.chunks.values_mut() {
            chunk.slices.sort_by_key(|slice| slice.seq);
            for slice in &mut chunk.slices {
                if !slice.pending_materialization.is_empty() {
                    let pending = slice.pending_materialization.clone();
                    finished.extend(pending.iter().map(|entry| entry.finished.clone()));
                    detached_finished.extend(pending);
                }
                if !slice.frozen {
                    continue;
                }
                if slice.writer.dirty_bytes() == 0 {
                    continue;
                }
                if let Some(object_id) = next_object_id {
                    let object_count =
                        staged_object_count(slice.writer.dirty_bytes(), self.config.block_size);
                    slice.writer.set_next_object_id(object_id);
                    next_object_id = Some(object_id.saturating_add(object_count));
                }
                slice.frozen = true;
                let detached = slice.writer.detach_finish()?;
                let converted = detached
                    .into_iter()
                    .map(|detached| DetachedFinishedWriterSlice {
                        finished: finished_slice_from_staged(
                            chunk.chunk_id,
                            chunk_size,
                            slice,
                            &detached.staged,
                        ),
                        detached,
                    })
                    .collect::<Vec<_>>();
                slice.materializing.extend(converted.iter().map(|entry| {
                    materializing_read_span_from_detached(slice.seq, &entry.detached)
                }));
                let staged_len: usize = converted
                    .iter()
                    .map(|entry| entry.finished.len as usize)
                    .sum();
                staged_dirty_bytes = staged_dirty_bytes.saturating_add(staged_len);
                let converted_finished = converted
                    .iter()
                    .map(|entry| entry.finished.clone())
                    .collect::<Vec<_>>();
                slice.finished = converted_finished.clone();
                slice.pending_materialization.extend(converted.clone());
                finished.extend(converted_finished);
                detached_finished.extend(converted);
            }
        }
        file.dirty_bytes = file.dirty_bytes.saturating_sub(staged_dirty_bytes);
        let clean = file.dirty_bytes == 0;
        drop(file);
        self.subtract_dirty(staged_dirty_bytes);
        if clean {
            self.shards[Self::inode_shard_index(ino)]
                .lock()
                .dirty_inodes
                .remove(&ino);
        }
        if clean {
            self.remove_file_if_clean_and_closed(ino);
        }
        self.persist_detached_finished_slices(ino, detached_finished)?;
        Ok(finished)
    }

    pub(super) fn completed_block_count_for_inode(
        &self,
        ino: Ino,
    ) -> Result<u32, WriterStackError> {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return Ok(0);
            };
            Arc::clone(file)
        };
        let file = file.lock();
        if file.ino != ino {
            return Ok(0);
        };
        let mut object_count = 0usize;
        for chunk in file.chunks.values() {
            for slice in &chunk.slices {
                if slice.frozen {
                    continue;
                }
                object_count = object_count
                    .checked_add(slice.writer.completed_block_count())
                    .ok_or(WriterStackError::ObjectReservationTooLarge)?;
            }
        }
        u32::try_from(object_count).map_err(|_| WriterStackError::ObjectReservationTooLarge)
    }

    pub(super) fn flush_completed_blocks_for_inode(
        &self,
        ino: Ino,
        first_object_id: u64,
        max_blocks: usize,
    ) -> Result<Vec<FinishedWriterSlice>, WriterStackError> {
        if max_blocks == 0 {
            return Ok(Vec::new());
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            let Some(file) = shard.files.get(&ino) else {
                return Ok(Vec::new());
            };
            Arc::clone(file)
        };
        let mut file = file.lock();
        if file.ino != ino {
            return Ok(Vec::new());
        };

        let mut finished = Vec::new();
        let mut detached_finished = Vec::new();
        let mut staged_dirty_bytes = 0usize;
        let chunk_size = self.config.chunk_size;
        let mut next_object_id = first_object_id;
        let mut remaining_blocks = max_blocks;
        for chunk in file.chunks.values_mut() {
            chunk.slices.sort_by_key(|slice| slice.seq);
            for slice in &mut chunk.slices {
                if !slice.pending_materialization.is_empty() {
                    let pending = slice.pending_materialization.clone();
                    finished.extend(pending.iter().map(|entry| entry.finished.clone()));
                    detached_finished.extend(pending);
                }
                if remaining_blocks == 0 {
                    break;
                }
                if slice.frozen || slice.writer.completed_block_count() == 0 {
                    continue;
                }
                slice.writer.set_next_object_id(next_object_id);
                let detached = slice.writer.detach_completed_blocks(remaining_blocks)?;
                if detached.is_empty() {
                    continue;
                }
                remaining_blocks = remaining_blocks.saturating_sub(detached.len());
                next_object_id = next_object_id.saturating_add(detached.len() as u64);
                let converted = detached
                    .into_iter()
                    .map(|detached| DetachedFinishedWriterSlice {
                        finished: finished_slice_from_staged(
                            chunk.chunk_id,
                            chunk_size,
                            slice,
                            &detached.staged,
                        ),
                        detached,
                    })
                    .collect::<Vec<_>>();
                slice.materializing.extend(converted.iter().map(|entry| {
                    materializing_read_span_from_detached(slice.seq, &entry.detached)
                }));
                let staged_len: usize = converted
                    .iter()
                    .map(|entry| entry.finished.len as usize)
                    .sum();
                staged_dirty_bytes = staged_dirty_bytes.saturating_add(staged_len);
                let converted_finished = converted
                    .iter()
                    .map(|entry| entry.finished.clone())
                    .collect::<Vec<_>>();
                slice.finished.extend(converted_finished.clone());
                slice.pending_materialization.extend(converted.clone());
                finished.extend(converted_finished);
                detached_finished.extend(converted);
            }
        }
        file.dirty_bytes = file.dirty_bytes.saturating_sub(staged_dirty_bytes);
        let clean = file.dirty_bytes == 0;
        drop(file);
        self.subtract_dirty(staged_dirty_bytes);
        if clean {
            self.shards[Self::inode_shard_index(ino)]
                .lock()
                .dirty_inodes
                .remove(&ino);
            self.remove_file_if_clean_and_closed(ino);
        }
        self.persist_detached_finished_slices(ino, detached_finished)?;
        Ok(finished)
    }

    pub(super) fn mark_closed(&self, fh: u64) {
        let handle_shard_index = Self::handle_shard_index(fh);
        let ino = self.handle_shards[handle_shard_index]
            .lock()
            .ino_by_handle
            .remove(&fh);
        if let Some(ino) = ino {
            self.release_handle_for_inode(ino);
        }
    }

    pub(super) fn clear_inode(&self, ino: Ino) {
        let file = {
            let mut shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.handle_counts_by_ino.remove(&ino);
            shard.dirty_inodes.remove(&ino);
            shard.files.remove(&ino)
        };
        if let Some(file) = file {
            self.subtract_dirty(file.lock().dirty_bytes);
        }
    }

    pub(super) fn has_dirty_writers(&self, ino: Ino) -> bool {
        self.dirty_bytes_for_inode(ino) > 0
    }

    pub(super) fn dirty_bytes_for_inode(&self, ino: Ino) -> usize {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.files.get(&ino).cloned()
        };
        file.map(|file| file.lock().dirty_bytes).unwrap_or(0)
    }

    pub(super) fn clear_materializing_object_ids(&self, ino: Ino, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.files.get(&ino).cloned()
        };
        let Some(file) = file else {
            return;
        };
        {
            let mut file = file.lock();
            for chunk in file.chunks.values_mut() {
                for slice in &mut chunk.slices {
                    slice
                        .materializing
                        .retain(|span| !object_ids.contains(&span.object_id));
                }
            }
        }
        self.remove_file_if_clean_and_closed(ino);
    }

    pub(super) fn snapshot_dirty_inodes_by_bytes(&self) -> Vec<Ino> {
        let mut dirty = Vec::new();
        for shard in self.shards.iter() {
            let shard = shard.lock();
            for ino in &shard.dirty_inodes {
                if let Some(file) = shard.files.get(ino) {
                    let bytes = file.lock().dirty_bytes;
                    if bytes > 0 {
                        dirty.push((*ino, bytes));
                    }
                }
            }
        }
        dirty.sort_unstable_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
        dirty.into_iter().map(|(ino, _)| ino).collect()
    }

    pub(super) fn visible_len(&self, ino: Ino) -> Option<u64> {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.files.get(&ino).cloned()
        };
        file.map(|file| file.lock().visible_len)
    }

    #[cfg(test)]
    pub(super) fn inject_sticky_error_for_test(&self, ino: Ino, message: impl Into<String>) {
        let file = {
            let mut shard = self.shards[Self::inode_shard_index(ino)].lock();
            Arc::clone(shard.files.entry(ino).or_insert_with(|| {
                Arc::new(Mutex::new(FileWriter {
                    ino,
                    visible_len: 0,
                    next_object_id: 0,
                    chunks: BTreeMap::new(),
                    dirty_bytes: 0,
                    last_write: Instant::now(),
                    sticky_error: None,
                    sticky_error_source: None,
                }))
            }))
        };
        let mut file = file.lock();
        file.sticky_error = Some(message.into());
        file.sticky_error_source = None;
    }

    pub(super) fn total_dirty_bytes(&self) -> usize {
        self.dirty_bytes.load(Ordering::Acquire)
    }

    fn subtract_dirty(&self, amount: usize) {
        let _ = self
            .dirty_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_sub(amount))
            });
    }

    fn persist_detached_finished_slices(
        &self,
        ino: Ino,
        detached_finished: Vec<DetachedFinishedWriterSlice>,
    ) -> Result<(), WriterStackError> {
        let object_ids = detached_finished
            .iter()
            .map(|entry| entry.finished.object_id)
            .collect::<Vec<_>>();
        let detached = detached_finished
            .into_iter()
            .map(|entry| entry.detached)
            .collect::<Vec<_>>();
        if detached.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.cached_store.persist_detached_staged_slices(detached) {
            let source = Arc::new(error);
            self.set_sticky_error(
                ino,
                format!("writer stack materialization failed: {source}"),
                Some(Arc::clone(&source)),
            );
            return Err(WriterStackError::CachedStore { source });
        }
        self.clear_pending_materialization_object_ids(ino, &object_ids);
        Ok(())
    }

    fn clear_pending_materialization_object_ids(&self, ino: Ino, object_ids: &[u64]) {
        if object_ids.is_empty() {
            return;
        }
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.files.get(&ino).cloned()
        };
        let Some(file) = file else {
            return;
        };
        {
            let mut file = file.lock();
            for chunk in file.chunks.values_mut() {
                for slice in &mut chunk.slices {
                    slice
                        .pending_materialization
                        .retain(|entry| !object_ids.contains(&entry.finished.object_id));
                }
            }
            let has_pending_materialization = file.chunks.values().any(|chunk| {
                chunk
                    .slices
                    .iter()
                    .any(|slice| !slice.pending_materialization.is_empty())
            });
            if !has_pending_materialization {
                file.sticky_error = None;
                file.sticky_error_source = None;
            }
        }
        self.remove_file_if_clean_and_closed(ino);
    }

    fn set_sticky_error(
        &self,
        ino: Ino,
        message: impl Into<String>,
        source: Option<Arc<CachedStoreWriterError>>,
    ) {
        let file = {
            let shard = self.shards[Self::inode_shard_index(ino)].lock();
            shard.files.get(&ino).cloned()
        };
        if let Some(file) = file {
            let mut file = file.lock();
            file.sticky_error = Some(message.into());
            file.sticky_error_source = source;
        }
    }

    fn validate_geometry(&self) -> Result<(), WriterStackError> {
        if self.config.block_size == 0 || self.config.chunk_size == 0 || self.config.page_size == 0
        {
            Err(WriterStackError::InvalidGeometry)
        } else {
            Ok(())
        }
    }

    fn inode_shard_index(ino: Ino) -> usize {
        (ino % Self::SHARDS as u64) as usize
    }

    fn handle_shard_index(fh: u64) -> usize {
        (fh % Self::SHARDS as u64) as usize
    }

    fn associate_handle(&self, fh: u64, ino: Ino) {
        let handle_shard_index = Self::handle_shard_index(fh);
        let previous_ino = self.handle_shards[handle_shard_index]
            .lock()
            .ino_by_handle
            .insert(fh, ino);
        if previous_ino == Some(ino) {
            return;
        }
        if let Some(previous_ino) = previous_ino {
            self.release_handle_for_inode(previous_ino);
        }
        let mut shard = self.shards[Self::inode_shard_index(ino)].lock();
        *shard.handle_counts_by_ino.entry(ino).or_default() += 1;
    }

    fn release_handle_for_inode(&self, ino: Ino) {
        let mut shard = self.shards[Self::inode_shard_index(ino)].lock();
        if let Some(count) = shard.handle_counts_by_ino.get_mut(&ino) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                shard.handle_counts_by_ino.remove(&ino);
                if shard
                    .files
                    .get(&ino)
                    .is_some_and(|file| file_is_clean_and_materialized(&file.lock()))
                {
                    shard.files.remove(&ino);
                }
            }
        }
    }

    fn next_object_id_for_empty_write(&self, ino: Ino, first_object_id: u64) -> u64 {
        let shard = self.shards[Self::inode_shard_index(ino)].lock();
        shard
            .files
            .get(&ino)
            .map(|file| first_object_id.max(file.lock().next_object_id))
            .unwrap_or(first_object_id)
    }

    fn remove_file_if_clean_and_closed(&self, ino: Ino) {
        let mut shard = self.shards[Self::inode_shard_index(ino)].lock();
        if shard.handle_counts_by_ino.contains_key(&ino) {
            return;
        }
        if shard
            .files
            .get(&ino)
            .is_some_and(|file| file_is_clean_and_materialized(&file.lock()))
        {
            shard.dirty_inodes.remove(&ino);
            shard.files.remove(&ino);
        }
    }
}

impl super::StudioVfs {
    pub(super) fn inode_has_dirty_writer_stack(&self, ino: Ino) -> bool {
        self.state.writer_stack.has_dirty_writers(ino)
    }
}

#[allow(dead_code)]
fn staged_object_count(len: usize, block_size: u32) -> u64 {
    let block_size = block_size as usize;
    if len == 0 || block_size == 0 {
        return 0;
    }
    len.div_ceil(block_size) as u64
}

fn saturating_u64_from_usize(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn file_is_clean_and_materialized(file: &FileWriter) -> bool {
    file.dirty_bytes == 0
        && file.chunks.values().all(|chunk| {
            chunk
                .slices
                .iter()
                .all(|slice| slice.materializing.is_empty())
        })
}

fn materializing_read_span_from_detached(
    seq: u64,
    detached: &DetachedStagedSlice,
) -> MaterializingReadSpan {
    MaterializingReadSpan {
        start: detached.staged.offset,
        end: detached
            .staged
            .offset
            .saturating_add(saturating_u64_from_usize(detached.staged.len)),
        object_id: detached.staged.object_id,
        seq,
        data: detached.payload.clone(),
    }
}

#[allow(dead_code)]
fn finished_slice_from_staged(
    chunk_id: ChunkId,
    chunk_size: u32,
    slice: &SliceWriter,
    staged: &StagedSlice,
) -> FinishedWriterSlice {
    let chunk_start = chunk_start_offset(chunk_id, chunk_size);
    let staged_delta = staged.offset.saturating_sub(chunk_start);
    FinishedWriterSlice {
        ino: staged.ino,
        chunk_id,
        chunk_offset: staged_delta as u32,
        len: staged.len as u32,
        object_id: staged.object_id,
        object_offset: 0,
        seq: slice.seq,
        content_epoch: slice.content_epoch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::sync::mpsc as std_mpsc;
    use std::time::Duration;
    use studiofs_cache::local::{
        LocalPayloadBackend, LocalPayloadBackendError, LocalPayloadLifecycleState, LocalPayloadPut,
        LocalPayloadStorageStats,
    };
    use studiofs_cache::{CachedStoreConfig, open_local_payload_backend};
    use tokio::sync::mpsc;

    struct SlowPutBackend {
        inner: Arc<dyn LocalPayloadBackend>,
        entered_tx: std_mpsc::Sender<()>,
        release_rx: Mutex<std_mpsc::Receiver<()>>,
    }

    struct TransientFailingPutBackend {
        inner: Arc<dyn LocalPayloadBackend>,
        failures_remaining: AtomicUsize,
    }

    impl TransientFailingPutBackend {
        fn new(inner: Arc<dyn LocalPayloadBackend>, failures: usize) -> Self {
            Self {
                inner,
                failures_remaining: AtomicUsize::new(failures),
            }
        }

        fn maybe_fail(&self) -> Result<(), LocalPayloadBackendError> {
            let failed = self
                .failures_remaining
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok();
            if failed {
                return Err(LocalPayloadBackendError::CapacityExhausted { needed: 4, free: 0 });
            }
            Ok(())
        }
    }

    impl SlowPutBackend {
        fn new(
            inner: Arc<dyn LocalPayloadBackend>,
            entered_tx: std_mpsc::Sender<()>,
            release_rx: std_mpsc::Receiver<()>,
        ) -> Self {
            Self {
                inner,
                entered_tx,
                release_rx: Mutex::new(release_rx),
            }
        }
    }

    impl LocalPayloadBackend for SlowPutBackend {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get(object_id)
        }

        fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
            self.inner.read_into(object_id, data_offset, out)
        }

        fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get_for_upload(object_id)
        }

        fn contains(&self, object_id: u64) -> bool {
            self.inner.contains(object_id)
        }

        fn advise_will_need(&self, object_id: u64) {
            self.inner.advise_will_need(object_id);
        }

        fn put(
            &self,
            object_id: u64,
            data: &[u8],
            evictable: bool,
        ) -> Result<(), LocalPayloadBackendError> {
            let _ = self.entered_tx.send(());
            let _ = self.release_rx.lock().recv();
            self.inner.put(object_id, data, evictable)
        }

        fn sync_data(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.sync_data()
        }

        fn run_pressure_maintenance(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.run_pressure_maintenance()
        }

        fn mark_uploading(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_uploading(object_id)
        }

        fn mark_upload_failed(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_upload_failed(object_id)
        }

        fn mark_uploaded(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_uploaded(object_id)
        }

        fn pin(&self, object_id: u64) {
            self.inner.pin(object_id);
        }

        fn unpin(&self, object_id: u64) {
            self.inner.unpin(object_id);
        }

        fn remove(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.remove(object_id)
        }

        fn remove_batch(&self, object_ids: &[u64]) -> Result<(), LocalPayloadBackendError> {
            self.inner.remove_batch(object_ids)
        }

        fn dirty_object_ids(&self) -> Vec<u64> {
            self.inner.dirty_object_ids()
        }

        fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState {
            self.inner.lifecycle_state(object_id)
        }

        fn is_evictable(&self, object_id: u64) -> bool {
            self.inner.is_evictable(object_id)
        }

        fn storage_stats(&self) -> LocalPayloadStorageStats {
            self.inner.storage_stats()
        }

        fn object_stats(
            &self,
            object_id: u64,
        ) -> Option<studiofs_cache::local::LocalPayloadObjectStats> {
            self.inner.object_stats(object_id)
        }

        fn object_ids(&self) -> Vec<u64> {
            self.inner.object_ids()
        }
    }

    impl LocalPayloadBackend for TransientFailingPutBackend {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get(object_id)
        }

        fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
            self.inner.read_into(object_id, data_offset, out)
        }

        fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get_for_upload(object_id)
        }

        fn contains(&self, object_id: u64) -> bool {
            self.inner.contains(object_id)
        }

        fn advise_will_need(&self, object_id: u64) {
            self.inner.advise_will_need(object_id);
        }

        fn put(
            &self,
            object_id: u64,
            data: &[u8],
            evictable: bool,
        ) -> Result<(), LocalPayloadBackendError> {
            self.maybe_fail()?;
            self.inner.put(object_id, data, evictable)
        }

        fn put_batch(&self, entries: &[LocalPayloadPut]) -> Result<(), LocalPayloadBackendError> {
            self.maybe_fail()?;
            self.inner.put_batch(entries)
        }

        fn sync_data(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.sync_data()
        }

        fn run_pressure_maintenance(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.run_pressure_maintenance()
        }

        fn mark_uploading(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_uploading(object_id)
        }

        fn mark_upload_failed(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_upload_failed(object_id)
        }

        fn mark_uploaded(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.mark_uploaded(object_id)
        }

        fn pin(&self, object_id: u64) {
            self.inner.pin(object_id);
        }

        fn unpin(&self, object_id: u64) {
            self.inner.unpin(object_id);
        }

        fn remove(&self, object_id: u64) -> Result<(), LocalPayloadBackendError> {
            self.inner.remove(object_id)
        }

        fn remove_batch(&self, object_ids: &[u64]) -> Result<(), LocalPayloadBackendError> {
            self.inner.remove_batch(object_ids)
        }

        fn dirty_object_ids(&self) -> Vec<u64> {
            self.inner.dirty_object_ids()
        }

        fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState {
            self.inner.lifecycle_state(object_id)
        }

        fn is_evictable(&self, object_id: u64) -> bool {
            self.inner.is_evictable(object_id)
        }

        fn storage_stats(&self) -> LocalPayloadStorageStats {
            self.inner.storage_stats()
        }

        fn object_stats(
            &self,
            object_id: u64,
        ) -> Option<studiofs_cache::local::LocalPayloadObjectStats> {
            self.inner.object_stats(object_id)
        }

        fn object_ids(&self) -> Vec<u64> {
            self.inner.object_ids()
        }
    }

    fn temp_stack(
        chunk_size: u32,
        block_size: u32,
    ) -> (tempfile::TempDir, MountedWriterStack, mpsc::Receiver<()>) {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000291")
                .expect("open backend")
                .backend;
        let (store, rx) =
            CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open store");
        let stack = MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size,
                chunk_size,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        );
        (dir, stack, rx)
    }

    fn temp_stack_with_config(
        config: WriterStackConfig,
    ) -> (tempfile::TempDir, MountedWriterStack, mpsc::Receiver<()>) {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000291")
                .expect("open backend")
                .backend;
        let (store, rx) =
            CachedStore::open(CachedStoreConfig::new(dir.path()), backend).expect("open store");
        let stack = MountedWriterStack::new(store, config);
        (dir, stack, rx)
    }

    fn write_spec(fh: u64, ino: Ino, offset: u64, first_object_id: u64) -> WriterWriteSpec {
        WriterWriteSpec {
            fh,
            ino,
            current_size: 0,
            content_epoch: 9,
            first_object_id,
            offset,
        }
    }

    #[test]
    fn snapshot_dirty_inodes_by_bytes_orders_largest_first() {
        let (_dir, stack, _rx) = temp_stack(64, 16);

        stack
            .write_bytes(write_spec(1, 11, 0, 100), Bytes::from_static(b"small"))
            .expect("small write");
        stack
            .write_bytes(
                write_spec(2, 22, 0, 200),
                Bytes::from_static(b"larger dirty inode"),
            )
            .expect("large write");
        stack
            .write_bytes(write_spec(3, 33, 0, 300), Bytes::from_static(b"medium"))
            .expect("medium write");

        let dirty = stack.snapshot_dirty_inodes_by_bytes();

        assert_eq!(dirty, vec![22, 33, 11]);
    }

    #[test]
    fn writer_stack_splits_writes_at_chunk_boundaries() {
        let (_dir, stack, _rx) = temp_stack(8, 4);

        let accepted = stack
            .write_bytes(
                WriterWriteSpec {
                    current_size: 0,
                    ..write_spec(1, 7, 6, 100)
                },
                Bytes::from_static(b"abcdef"),
            )
            .expect("accept write");

        assert_eq!(accepted.accepted_bytes, 6);
        let finished = stack.finish_inode(7).expect("finish inode");
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[0].chunk_id, ChunkId(0));
        assert_eq!(finished[0].chunk_offset, 6);
        assert_eq!(finished[0].len, 2);
        assert_eq!(finished[1].chunk_id, ChunkId(1));
        assert_eq!(finished[1].chunk_offset, 0);
        assert_eq!(finished[1].len, 4);
    }

    #[test]
    fn writer_stack_read_spans_return_latest_overwrite_first() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(1, 7, 0, 100), Bytes::from_static(b"abcdef"))
            .expect("base write");
        stack
            .write_bytes(write_spec(1, 7, 2, 101), Bytes::from_static(b"XY"))
            .expect("overwrite");

        let spans = stack.snapshot_read_spans(7, 0, 6);
        assert_eq!(spans[0].start, 2);
        assert_eq!(spans[0].end, 4);
        assert_eq!(spans[0].data.as_ref(), b"XY");
        assert!(spans[0].seq > spans[1].seq);
    }

    #[test]
    fn writer_stack_copies_from_borrowed_input_without_retaining_source() {
        let (_dir, stack, _rx) = temp_stack(64, 8);
        let mut payload = b"borrowed-input".to_vec();

        stack
            .write_bytes(write_spec(1, 7, 0, 100), payload.as_slice())
            .expect("borrowed write");
        payload.fill(b'X');

        let spans = stack.snapshot_read_spans(7, 0, 14);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].data.as_ref(), b"borrowed-input");
    }

    #[test]
    fn writer_stack_accepts_owned_bytes_without_borrowed_source() {
        let (_dir, stack, _rx) = temp_stack(64, 8);
        let payload = Bytes::from(Vec::from(&b"owned-input"[..]));

        stack
            .write_owned_bytes(write_spec(1, 7, 0, 100), payload)
            .expect("owned write");

        let spans = stack.snapshot_read_spans(7, 0, 11);
        assert_eq!(spans[0].data.as_ref(), b"owned-input");
    }

    #[test]
    fn writer_stack_read_span_offsets_are_relative_to_returned_data() {
        let (_dir, stack, _rx) = temp_stack(64, 8);
        let payload = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz0123456789");

        stack
            .write_bytes(write_spec(1, 7, 0, 100), payload.clone())
            .expect("dirty write");

        let spans = stack.snapshot_read_spans(7, 30, 6);

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 30);
        assert_eq!(spans[0].end, 36);
        assert_eq!(spans[0].data.as_ref(), b"456789");
        assert_eq!(
            spans[0].block_offset, 0,
            "returned dirty data is already clipped to the span, so assembler offset must be data-relative"
        );
    }

    #[test]
    fn writer_stack_copies_fragmented_read_spans_into_existing_buffer() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(1, 7, 0, 100), Bytes::from_static(b"hello"))
            .expect("base write");
        stack
            .write_bytes(write_spec(2, 7, 1, 101), Bytes::from_static(b"XYZ"))
            .expect("overlap write");
        stack
            .write_bytes(write_spec(3, 7, 7, 102), Bytes::from_static(b"!"))
            .expect("sparse tail write");

        let mut out = [0_u8; 8];
        let claims = stack.copy_read_spans_into(7, 0, &mut out);

        assert_eq!(&out, b"hXYZo\0\0!");
        assert_eq!(
            claims
                .iter()
                .map(|claim| (claim.start, claim.end))
                .collect::<Vec<_>>(),
            vec![(0, 1), (1, 4), (4, 5), (7, 8)]
        );
    }

    #[test]
    fn writer_stack_finish_preserves_creation_order_within_chunk() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(1, 7, 8, 200), Bytes::from_static(b"tail"))
            .expect("tail write");
        stack
            .write_bytes(write_spec(1, 7, 0, 201), Bytes::from_static(b"head"))
            .expect("head write");

        let finished = stack.finish_inode(7).expect("finish inode");
        assert_eq!(finished.len(), 2);
        assert_eq!(finished[0].seq + 1, finished[1].seq);
        assert_eq!(finished[0].chunk_offset, 8);
        assert_eq!(finished[1].chunk_offset, 0);
    }

    #[test]
    fn writer_stack_tracks_dirty_bytes_and_cleans_up_closed_finished_handles() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(99, 7, 0, 300), Bytes::from_static(b"dirty"))
            .expect("write");

        assert!(stack.has_dirty_writers(7));
        assert_eq!(stack.total_dirty_bytes(), 5);
        stack.finish_inode(7).expect("finish");
        assert!(!stack.has_dirty_writers(7));
        assert_eq!(stack.total_dirty_bytes(), 0);
        stack.mark_closed(99);
        assert!(!stack.has_dirty_writers(7));
    }

    #[test]
    fn writer_stack_append_updates_visible_length_without_finishing() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        let accepted = stack
            .write_bytes(
                WriterWriteSpec {
                    current_size: 4,
                    ..write_spec(1, 7, 4, 400)
                },
                Bytes::from_static(b"tail"),
            )
            .expect("append");

        assert_eq!(accepted.visible_len, 8);
        assert_eq!(stack.visible_len(7), Some(8));
        assert!(stack.has_dirty_writers(7));
    }

    #[test]
    fn writer_stack_sparse_write_keeps_only_accepted_range_dirty() {
        let (_dir, stack, _rx) = temp_stack(32, 8);

        let accepted = stack
            .write_bytes(write_spec(1, 7, 24, 500), Bytes::from_static(b"end"))
            .expect("sparse write");

        assert_eq!(accepted.visible_len, 27);
        let spans = stack.snapshot_read_spans(7, 0, 32);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 24);
        assert_eq!(spans[0].end, 27);
        assert_eq!(spans[0].data.as_ref(), b"end");
    }

    #[test]
    fn writer_stack_finishes_full_chunk_and_partial_tail_separately() {
        let (_dir, stack, _rx) = temp_stack(8, 4);

        stack
            .write_bytes(write_spec(1, 7, 0, 600), Bytes::from_static(b"abcdefghijk"))
            .expect("write full chunk plus tail");

        let finished = stack.finish_inode(7).expect("finish");
        assert_eq!(finished.len(), 3);
        assert_eq!(finished[0].chunk_id, ChunkId(0));
        assert_eq!(finished[0].len, 4);
        assert_eq!(finished[1].chunk_id, ChunkId(0));
        assert_eq!(finished[1].chunk_offset, 4);
        assert_eq!(finished[1].len, 4);
        assert_eq!(finished[2].chunk_id, ChunkId(1));
        assert_eq!(finished[2].len, 3);
        assert_eq!(
            finished
                .iter()
                .map(|slice| slice.object_id)
                .collect::<Vec<_>>(),
            vec![600, 601, 602]
        );
    }

    #[test]
    fn writer_stack_flush_completed_blocks_leaves_partial_tail_buffered() {
        let (_dir, stack, _rx) = temp_stack(64, 8);

        stack
            .write_bytes(
                write_spec(1, 7, 0, 500),
                Bytes::from_static(b"abcdefghijkl"),
            )
            .expect("write full block and tail");

        let finished = stack
            .flush_completed_blocks_for_inode(7, 900, usize::MAX)
            .expect("flush completed blocks");

        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].object_id, 900);
        assert_eq!(finished[0].chunk_offset, 0);
        assert_eq!(finished[0].len, 8);
        assert_eq!(stack.dirty_bytes_for_inode(7), 4);
        let spans = stack.snapshot_read_spans(7, 8, 4);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].data.as_ref(), b"ijkl");
    }

    #[test]
    fn writer_stack_returns_next_object_id_after_acceptance() {
        let (_dir, stack, _rx) = temp_stack(8, 4);

        let accepted = stack
            .write_bytes(write_spec(1, 7, 0, 600), Bytes::from_static(b"abcdefghijk"))
            .expect("write full chunk plus tail");

        assert_eq!(accepted.next_object_id, 603);
    }

    #[test]
    fn writer_stack_empty_write_preserves_inode_next_object_id() {
        let (_dir, stack, _rx) = temp_stack(8, 4);

        let accepted = stack
            .write_bytes(write_spec(1, 7, 0, 600), Bytes::from_static(b"abcdefghijk"))
            .expect("write full chunk plus tail");
        assert_eq!(accepted.next_object_id, 603);

        let empty = stack
            .write_bytes(write_spec(1, 7, 11, 100), Bytes::new())
            .expect("empty write");

        assert_eq!(empty.next_object_id, 603);
    }

    #[test]
    fn writer_stack_reassigns_handle_across_inode_shards() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(99, 7, 0, 700), Bytes::from_static(b"old"))
            .expect("write old inode");
        stack
            .write_bytes(write_spec(99, 8, 0, 800), Bytes::from_static(b"new"))
            .expect("write new inode with same handle");

        stack.mark_closed(99);
        stack.finish_inode(7).expect("finish old inode");
        stack.finish_inode(8).expect("finish new inode");

        assert_eq!(stack.visible_len(7), None);
        assert_eq!(stack.visible_len(8), None);
    }

    #[test]
    fn writer_stack_reuses_unfrozen_slice_for_contiguous_writes() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        let first = stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"ab"))
            .expect("first write");
        let second = stack
            .write_bytes(
                WriterWriteSpec {
                    first_object_id: first.next_object_id,
                    ..write_spec(1, 7, 2, 999)
                },
                Bytes::from_static(b"cd"),
            )
            .expect("contiguous write");

        assert_eq!(second.next_object_id, 701);
        let finished = stack.finish_inode(7).expect("finish inode");
        assert_eq!(finished.len(), 1);
        assert_eq!(finished[0].object_id, 700);
        assert_eq!(finished[0].len, 4);
    }

    #[test]
    fn writer_stack_finish_cleans_closed_clean_file() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(99, 7, 0, 800), Bytes::from_static(b"dirty"))
            .expect("write");
        stack.mark_closed(99);
        assert_eq!(stack.visible_len(7), Some(5));

        stack.finish_inode(7).expect("finish after close");

        assert_eq!(stack.visible_len(7), None);
        assert!(!stack.has_dirty_writers(7));
    }

    #[test]
    fn writer_stack_failed_write_does_not_extend_visible_len() {
        let (_dir, stack, _rx) = temp_stack_with_config(WriterStackConfig {
            block_size: 4,
            chunk_size: 16,
            page_size: 4,
            dirty_byte_limit: None,
            max_buffer_bytes: Some(2),
        });

        let err = stack
            .write_bytes(write_spec(1, 7, 0, 900), Bytes::from_static(b"toolong"))
            .expect_err("buffer limit should reject write");

        assert!(matches!(
            err,
            WriterStackError::CachedStore {
                source: error,
            } if matches!(error.as_ref(), CachedStoreWriterError::BufferLimitExceeded { .. })
        ));
        assert_eq!(stack.visible_len(7), Some(0));
        assert!(!stack.has_dirty_writers(7));
    }

    #[test]
    fn writer_stack_finish_does_not_hold_shard_lock_during_staging() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000291")
                .expect("open backend")
                .backend;
        let (entered_tx, entered_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let slow_backend = Arc::new(SlowPutBackend::new(backend, entered_tx, release_rx));
        let (store, _rx) = CachedStore::open(CachedStoreConfig::new(dir.path()), slow_backend)
            .expect("open store");
        let stack = Arc::new(MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size: 4,
                chunk_size: 8,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        ));

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"slow"))
            .expect("write slow file");
        let finishing = Arc::clone(&stack);
        let finish_thread = std::thread::spawn(move || finishing.finish_inode(7));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("finish should enter backend put");

        let writing = Arc::clone(&stack);
        let (done_tx, done_rx) = std_mpsc::channel();
        let write_thread = std::thread::spawn(move || {
            let result =
                writing.write_bytes(write_spec(2, 71, 0, 800), Bytes::from_static(b"fast"));
            done_tx.send(result).expect("send write result");
        });

        let unrelated_write = done_rx.recv_timeout(Duration::from_millis(100));
        release_tx.send(()).expect("release slow put");
        finish_thread
            .join()
            .expect("finish thread")
            .expect("finish inode");
        write_thread.join().expect("write thread");
        unrelated_write
            .expect("unrelated inode in the same shard should not wait for staging")
            .expect("write unrelated inode");
    }

    #[test]
    fn writer_stack_finish_does_not_hold_file_lock_during_staging() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000291")
                .expect("open backend")
                .backend;
        let (entered_tx, entered_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let slow_backend = Arc::new(SlowPutBackend::new(backend, entered_tx, release_rx));
        let (store, _rx) = CachedStore::open(CachedStoreConfig::new(dir.path()), slow_backend)
            .expect("open store");
        let stack = Arc::new(MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size: 4,
                chunk_size: 16,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        ));

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"slow"))
            .expect("write slow file");
        let finishing = Arc::clone(&stack);
        let finish_thread = std::thread::spawn(move || finishing.finish_inode(7));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("finish should enter backend put");

        let writing = Arc::clone(&stack);
        let (done_tx, done_rx) = std_mpsc::channel();
        let write_thread = std::thread::spawn(move || {
            let result = writing.write_bytes(write_spec(2, 7, 4, 800), Bytes::from_static(b"fast"));
            done_tx.send(result).expect("send write result");
        });

        let same_inode_write = done_rx.recv_timeout(Duration::from_millis(100));
        release_tx.send(()).expect("release slow put");
        finish_thread
            .join()
            .expect("finish thread")
            .expect("finish inode");
        write_thread.join().expect("write thread");
        same_inode_write
            .expect("same inode write should not wait for staging")
            .expect("write same inode");
    }

    #[test]
    fn writer_stack_detached_completed_block_remains_readable_while_staging() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000292")
                .expect("open backend")
                .backend;
        let (entered_tx, entered_rx) = std_mpsc::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let slow_backend = Arc::new(SlowPutBackend::new(backend, entered_tx, release_rx));
        let (store, _rx) = CachedStore::open(CachedStoreConfig::new(dir.path()), slow_backend)
            .expect("open store");
        let stack = Arc::new(MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size: 4,
                chunk_size: 16,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        ));

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"slow"))
            .expect("write slow file");
        let staging = Arc::clone(&stack);
        let staging_thread =
            std::thread::spawn(move || staging.flush_completed_blocks_for_inode(7, 900, 1));
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("flush should enter backend put");

        let spans = stack.snapshot_read_spans(7, 0, 4);

        release_tx.send(()).expect("release slow put");
        staging_thread
            .join()
            .expect("staging thread")
            .expect("flush completed block");
        assert_eq!(spans.len(), 1);
        assert_eq!(
            spans[0].data.as_ref(),
            b"slow",
            "detached-but-not-yet-installed bytes must remain visible to same-mount reads"
        );
    }

    #[test]
    fn writer_stack_concurrent_files_do_not_share_dirty_locks_or_cleanup() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"one"))
            .expect("write first file");
        stack
            .write_bytes(write_spec(2, 70, 0, 800), Bytes::from_static(b"two"))
            .expect("write second file");

        assert!(stack.has_dirty_writers(7));
        assert!(stack.has_dirty_writers(70));
        stack.finish_inode(7).expect("finish first");
        assert!(!stack.has_dirty_writers(7));
        assert!(stack.has_dirty_writers(70));
    }

    #[test]
    fn writer_stack_sticky_errors_block_followup_writes() {
        let (_dir, stack, _rx) = temp_stack(16, 8);

        stack.inject_sticky_error_for_test(7, "staging failed");

        let err = stack
            .write_bytes(write_spec(1, 7, 0, 900), Bytes::from_static(b"data"))
            .expect_err("sticky error");
        assert!(matches!(err, WriterStackError::Sticky { ino: 7, .. }));
    }

    #[test]
    fn writer_stack_retries_transient_materialization_failure() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000293")
                .expect("open backend")
                .backend;
        let failing_backend = Arc::new(TransientFailingPutBackend::new(backend, 1));
        let (store, _rx) = CachedStore::open(CachedStoreConfig::new(dir.path()), failing_backend)
            .expect("open store");
        let stack = MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size: 4,
                chunk_size: 16,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        );

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"data"))
            .expect("write initial block");
        stack
            .finish_inode(7)
            .expect_err("first materialization fails");

        let retried = stack
            .finish_inode(7)
            .expect("second materialization retries detached payload");
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].object_id, 700);

        stack
            .write_bytes(write_spec(1, 7, 4, 701), Bytes::from_static(b"more"))
            .expect("successful retry clears sticky writer error");
    }

    #[test]
    fn storage_full_remains_on_writer_stack_sticky_write() {
        let dir = studiofs_test_support::workspace_target_tempdir("test-");
        let backend =
            open_local_payload_backend(dir.path(), "00000000-0000-0000-0000-000000000294")
                .expect("open backend")
                .backend;
        let failing_backend = Arc::new(TransientFailingPutBackend::new(backend, 1));
        let (store, _rx) = CachedStore::open(CachedStoreConfig::new(dir.path()), failing_backend)
            .expect("open store");
        let stack = MountedWriterStack::new(
            store,
            WriterStackConfig {
                block_size: 4,
                chunk_size: 16,
                page_size: 4,
                dirty_byte_limit: None,
                max_buffer_bytes: None,
            },
        );

        stack
            .write_bytes(write_spec(1, 7, 0, 700), Bytes::from_static(b"data"))
            .expect("write initial block");
        let materialization_error = stack
            .finish_inode(7)
            .expect_err("capacity failure during materialization");
        assert!(error_chain_has_capacity_exhausted(&materialization_error));

        let sticky_error = stack
            .write_bytes(write_spec(1, 7, 4, 701), Bytes::from_static(b"more"))
            .expect_err("failed materialization blocks follow-up writes");
        assert!(matches!(
            sticky_error,
            WriterStackError::Sticky { ino: 7, .. }
        ));
        assert!(error_chain_has_capacity_exhausted(&sticky_error));
    }

    fn error_chain_has_capacity_exhausted(error: &(dyn std::error::Error + 'static)) -> bool {
        let mut current = Some(error);
        while let Some(error) = current {
            if matches!(
                error.downcast_ref::<LocalPayloadBackendError>(),
                Some(LocalPayloadBackendError::CapacityExhausted { .. })
            ) {
                return true;
            }
            current = error.source();
        }
        false
    }
}
