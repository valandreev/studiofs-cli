use super::*;
use crate::utils::{IntervalRange, trim_range_to_available_len};
use futures::stream::{FuturesUnordered, StreamExt};
use std::time::{Duration, Instant};

const PENDING_SLICE_COMMIT_MAX_AGE: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub(super) struct PendingPayload {
    pub(super) data: BytesMut,
    pub(super) dirty: bool,
    pub(super) reserved_object_id: Option<u64>,
    pub(super) persisted_len: Option<usize>,
}

impl PendingPayload {
    fn buffered(data: BytesMut, reserved_object_id: Option<u64>) -> Self {
        Self {
            data,
            dirty: true,
            reserved_object_id,
            persisted_len: None,
        }
    }

    #[cfg(test)]
    fn local_extent(object_id: u64, len: usize) -> Self {
        Self {
            data: BytesMut::new(),
            dirty: false,
            reserved_object_id: Some(object_id),
            persisted_len: Some(len),
        }
    }

    fn is_local_extent(&self) -> bool {
        !self.dirty && self.persisted_len.is_some() && self.reserved_object_id.is_some()
    }

    fn memory_len(&self) -> usize {
        if self.is_local_extent() {
            0
        } else {
            self.data.len()
        }
    }

    fn logical_len(&self) -> usize {
        self.persisted_len.unwrap_or(self.data.len())
    }

    fn reusable_reserved_object_id(&self) -> Option<u64> {
        (!self.is_local_extent())
            .then_some(self.reserved_object_id)
            .flatten()
    }

    pub(super) fn local_extent_id(&self) -> Option<u64> {
        self.is_local_extent()
            .then_some(self.reserved_object_id)
            .flatten()
    }

    fn recycle_buffer(self) -> Option<BytesMut> {
        (self.memory_len() > 0).then_some(self.data)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingDirtySlice {
    range: studiofs_meta::schema::SliceRange,
    block_index: u32,
    block_offset: u32,
    seq: u64,
    object_id: Option<u64>,
    object_offset: u32,
    created_at: Instant,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PersistedPendingLocalDirtySliceSpec {
    pub(super) block_index: u32,
    pub(super) file_offset: u64,
    pub(super) len: usize,
    pub(super) seq: u64,
    pub(super) object_id: u64,
    pub(super) initial_object_offset: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingDirtySliceCommitKey {
    chunk_id: u64,
    block_index: u32,
    block_offset: u32,
    seq: u64,
    object_id: Option<u64>,
    object_offset: u32,
    range_start: u32,
    range_end: u32,
}

impl PendingDirtySlice {
    fn persisted(
        range: studiofs_meta::schema::SliceRange,
        block_index: u32,
        block_offset: u32,
        seq: u64,
        object_id: u64,
        object_offset: u32,
    ) -> Self {
        Self {
            range,
            block_index,
            block_offset,
            seq,
            object_id: Some(object_id),
            object_offset,
            created_at: Instant::now(),
        }
    }

    fn is_persisted(self) -> bool {
        self.object_id.is_some()
    }

    fn len(self) -> usize {
        self.range.len() as usize
    }

    fn age(self, now: Instant) -> Duration {
        now.saturating_duration_since(self.created_at)
    }

    fn prefix(self, len: usize) -> Option<Self> {
        let len = len.min(self.len());
        let range_end = self.range.start.checked_add(u32::try_from(len).ok()?)?;
        let range = studiofs_meta::schema::SliceRange::new(self.range.start, range_end)?;
        Some(Self { range, ..self })
    }

    fn committed_prefix_len(self, committed: Self) -> Option<u32> {
        if self.block_index != committed.block_index
            || self.seq != committed.seq
            || self.object_id != committed.object_id
            || self.range.start != committed.range.start
            || self.block_offset != committed.block_offset
            || self.object_offset != committed.object_offset
            || committed.range.end > self.range.end
        {
            return None;
        }
        Some(committed.range.len())
    }

    fn trim_prefix(self, len: u32) -> Option<Self> {
        if len >= self.range.len() {
            return None;
        }
        let range_start = self.range.start.checked_add(len)?;
        let range = studiofs_meta::schema::SliceRange::new(range_start, self.range.end)?;
        let block_offset = self.block_offset.checked_add(len)?;
        let object_offset = self.object_offset.checked_add(len)?;
        Some(Self {
            range,
            block_offset,
            object_offset,
            ..self
        })
    }

    fn commit_key(self, chunk_id: u64) -> PendingDirtySliceCommitKey {
        PendingDirtySliceCommitKey {
            chunk_id,
            block_index: self.block_index,
            block_offset: self.block_offset,
            seq: self.seq,
            object_id: self.object_id,
            object_offset: self.object_offset,
            range_start: self.range.start,
            range_end: self.range.end,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PendingDirtyReadSpan {
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) block_offset: u32,
    pub(super) seq: u64,
    pub(super) data: Bytes,
}

#[derive(Debug, Clone)]
pub(super) enum DirtyWriteData {
    Mutable(BytesMut),
    Shared(Bytes),
}

#[derive(Debug, Clone)]
struct BufferedWriteSpillSegment {
    block_index: u32,
    file_offset: u64,
    len: usize,
    object_offset: u32,
}

#[derive(Debug, Clone)]
struct BufferedWriteSpillBatch {
    payload: Bytes,
    segments: Vec<BufferedWriteSpillSegment>,
}

impl DirtyWriteData {
    fn len(&self) -> usize {
        match self {
            Self::Mutable(data) => data.len(),
            Self::Shared(data) => data.len(),
        }
    }

    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Mutable(data) => data.as_ref(),
            Self::Shared(data) => data.as_ref(),
        }
    }

    fn freeze(self) -> Bytes {
        match self {
            Self::Mutable(data) => data.freeze(),
            Self::Shared(data) => data,
        }
    }

    fn into_pending_payload_data(self, vfs: &StudioVfs) -> BytesMut {
        match self {
            Self::Mutable(data) => data,
            Self::Shared(data) => vfs.clone_into_write_buffer(data.as_ref()),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct DirtyWriteSnapshot {
    pub(super) block_index: u32,
    pub(super) data: DirtyWriteData,
}

impl DirtyWriteSnapshot {
    pub(super) fn from_mutable(block_index: u32, data: BytesMut) -> Self {
        Self {
            block_index,
            data: DirtyWriteData::Mutable(data),
        }
    }

    pub(super) fn from_shared(block_index: u32, data: Bytes) -> Self {
        Self {
            block_index,
            data: DirtyWriteData::Shared(data),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.data.len()
    }

    pub(super) fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[derive(Debug, Clone)]
struct PersistedPayloadEntry {
    block_index: u32,
    object_id: u64,
    data: Option<Bytes>,
    len: usize,
}

impl PersistedPayloadEntry {
    fn from_payload(block_index: u32, object_id: u64, data: Bytes) -> Self {
        let len = data.len();
        Self {
            block_index,
            object_id,
            data: Some(data),
            len,
        }
    }

    #[cfg(test)]
    fn from_local_extent(block_index: u32, object_id: u64, len: usize) -> Self {
        Self {
            block_index,
            object_id,
            data: None,
            len,
        }
    }

    fn payload_bytes(&self) -> Option<&Bytes> {
        self.data.as_ref()
    }

    fn len(&self) -> usize {
        self.len
    }

    fn requires_payload_persist(&self) -> bool {
        self.data.is_some()
    }
}

#[derive(Debug, Clone, Copy)]
struct PayloadPersistConfig {
    put_concurrency: usize,
    local_sync_policy: LocalSyncPolicy,
}

trait BlockIdAllocatorPort {
    async fn next_object_ids(&self, count: u32) -> VfsResult<Vec<u64>>;
}

trait LocalPayloadWriterPort {
    async fn persist_payload(
        &self,
        persisted_blocks: &[PersistedPayloadEntry],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()>;

    async fn cleanup_uncommitted(
        &self,
        ino: Ino,
        blocks: &[PersistedPayloadEntry],
        stage: &'static str,
    );
}

trait OverlayCommitPort {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>>;
}

trait UploadSubmissionPort {
    // Upload publication is a post-commit step. Implementations must absorb
    // their own durability / retry mechanics so the drain core does not expose
    // a fallible post-commit boundary.
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
        content_epoch: u64,
    );
}

trait DrainCommitObserverPort {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>);
}

struct LocalDirtyDrainCore<L, C, B, U, O> {
    local_payloads: L,
    commit_meta: C,
    object_ids: B,
    uploads: U,
    observer: O,
    block_size: u32,
    put_concurrency: usize,
}

type LocalDirtyDrain<'a> = LocalDirtyDrainCore<
    VfsLocalPayloadWriter<'a>,
    VfsOverlayCommitter<'a>,
    VfsBlockIdAllocator<'a>,
    VfsUploadSubmitter<'a>,
    VfsDrainCommitObserver<'a>,
>;

struct LocalDirtyFlushSession<'a> {
    ino: Ino,
    vfs: &'a StudioVfs,
    drain: LocalDirtyDrain<'a>,
}

#[derive(Debug)]
struct DrainOutcome {
    persisted_blocks: Vec<PersistedPayloadEntry>,
}

#[derive(Clone, Copy)]
struct VfsLocalPayloadWriter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsOverlayCommitter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsBlockIdAllocator<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsUploadSubmitter<'a> {
    vfs: &'a StudioVfs,
}

#[derive(Clone, Copy)]
struct VfsDrainCommitObserver<'a> {
    vfs: &'a StudioVfs,
}

pub(super) struct LocalSyncGroupState {
    pub(super) pending_bytes: u64,
    pub(super) last_sync_at: Instant,
}

pub(super) struct LocalDirtyState {
    pub(super) pending_payloads: Mutex<std::collections::HashMap<(Ino, u32), PendingPayload>>,
    pub(super) pending_payload_indexes:
        Mutex<std::collections::HashMap<Ino, std::collections::BTreeSet<u32>>>,
    pending_chunk_slices: Mutex<
        std::collections::HashMap<Ino, std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>>,
    >,
    pub(super) pending_bytes: AtomicUsize,
    pub(super) write_backpressure_wait_ms: AtomicU64,
    pub(super) write_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_wait_ms: AtomicU64,
    pub(super) fast_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_stall_max_ms: AtomicU64,
    pub(super) local_sync_group: tokio::sync::Mutex<LocalSyncGroupState>,
    pub(super) local_sync_gate: tokio::sync::Mutex<()>,
}

impl LocalDirtyState {
    pub(super) fn new() -> Self {
        Self {
            pending_payloads: Mutex::new(std::collections::HashMap::new()),
            pending_payload_indexes: Mutex::new(std::collections::HashMap::new()),
            pending_chunk_slices: Mutex::new(std::collections::HashMap::new()),
            pending_bytes: AtomicUsize::new(0),
            write_backpressure_wait_ms: AtomicU64::new(0),
            write_backpressure_events: AtomicU64::new(0),
            fast_backpressure_wait_ms: AtomicU64::new(0),
            fast_backpressure_events: AtomicU64::new(0),
            fast_backpressure_stall_max_ms: AtomicU64::new(0),
            local_sync_group: tokio::sync::Mutex::new(LocalSyncGroupState {
                pending_bytes: 0,
                last_sync_at: Instant::now(),
            }),
            local_sync_gate: tokio::sync::Mutex::new(()),
        }
    }
}

#[cfg(test)]
fn assign_reserved_object_ids(
    pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
    ino: Ino,
    blocks: &mut [PersistedPayloadEntry],
    fresh_object_ids: &[u64],
) {
    let mut fresh_ids = fresh_object_ids.iter().copied();
    for block in blocks {
        let key = (ino, block.block_index);
        if let Some(current) = pending.get_mut(&key)
            && !current.is_local_extent()
            && block
                .payload_bytes()
                .is_some_and(|payload| current.data.as_ref() == payload.as_ref())
        {
            if let Some(object_id) = current.reserved_object_id {
                block.object_id = object_id;
                continue;
            }

            let object_id = fresh_ids
                .next()
                .expect("missing fresh object id for local dirty payload entry");
            current.reserved_object_id = Some(object_id);
            block.object_id = object_id;
            continue;
        }

        block.object_id = fresh_ids
            .next()
            .expect("missing fresh object id for local dirty payload entry");
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn run_bounded_tasks<I, F, Fut, T, E>(
    items: I,
    concurrency: usize,
    mut make_future: F,
) -> Result<(), E>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut items = items.into_iter();
    let mut inflight = FuturesUnordered::new();
    let max_inflight = concurrency.max(1);
    let mut first_error = None;

    while inflight.len() < max_inflight {
        let Some(item) = items.next() else {
            break;
        };
        inflight.push(make_future(item));
    }

    while let Some(result) = inflight.next().await {
        match result {
            Ok(_) => {
                if first_error.is_none()
                    && let Some(item) = items.next()
                {
                    inflight.push(make_future(item));
                }
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn commit_persisted_payload_state(
    pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
    ino: Ino,
    blocks: &[PersistedPayloadEntry],
) -> (usize, Vec<u32>, Vec<BytesMut>) {
    let mut removed_bytes = 0usize;
    let mut removed_block_indexes = Vec::new();
    let mut removed_buffers = Vec::new();
    for block in blocks {
        let key = (ino, block.block_index);
        let mut should_remove = false;
        if let Some(current) = pending.get_mut(&key) {
            if current.reserved_object_id == Some(block.object_id) {
                if current.is_local_extent() && current.persisted_len == Some(block.len()) {
                    should_remove = true;
                } else {
                    current.reserved_object_id = None;
                    current.persisted_len = None;
                }
            }
            should_remove |= current.dirty
                && block
                    .payload_bytes()
                    .is_some_and(|payload| current.data.as_ref() == payload.as_ref());
        }
        if should_remove && let Some(removed) = pending.remove(&key) {
            removed_bytes += removed.memory_len();
            removed_block_indexes.push(block.block_index);
            if let Some(buffer) = removed.recycle_buffer() {
                removed_buffers.push(buffer);
            }
        }
    }
    (removed_bytes, removed_block_indexes, removed_buffers)
}

impl<L, C, B, U, O> LocalDirtyDrainCore<L, C, B, U, O>
where
    L: LocalPayloadWriterPort,
    C: OverlayCommitPort,
    B: BlockIdAllocatorPort,
    U: UploadSubmissionPort,
    O: DrainCommitObserverPort,
{
    async fn allocate_object_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        if count == 0 {
            Ok(Vec::new())
        } else {
            self.object_ids.next_object_ids(count).await
        }
    }

    fn additional_object_id_count_needed(
        pending: &std::collections::HashMap<(Ino, u32), PendingPayload>,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
    ) -> usize {
        blocks
            .iter()
            .filter(|block| {
                pending
                    .get(&(ino, block.block_index))
                    .filter(|current| {
                        !current.is_local_extent() && current.data.as_ref() == block.as_ref()
                    })
                    .and_then(|current| current.reserved_object_id)
                    .is_none()
            })
            .count()
    }

    fn try_prepare_persisted_blocks(
        &self,
        pending: &mut std::collections::HashMap<(Ino, u32), PendingPayload>,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        fresh_object_ids: &mut std::collections::VecDeque<u64>,
    ) -> Result<Vec<PersistedPayloadEntry>, (Vec<DirtyWriteSnapshot>, usize)> {
        let fresh_needed = Self::additional_object_id_count_needed(pending, ino, &blocks);
        let additional_needed = fresh_needed.saturating_sub(fresh_object_ids.len());
        if additional_needed > 0 {
            return Err((blocks, additional_needed));
        }

        let mut persisted_blocks = Vec::with_capacity(blocks.len());
        for block in blocks {
            let key = (ino, block.block_index);
            if let Some(current) = pending.get_mut(&key)
                && !current.is_local_extent()
                && current.data.as_ref() == block.as_ref()
            {
                let object_id = if let Some(object_id) = current.reserved_object_id {
                    object_id
                } else {
                    let object_id = fresh_object_ids
                        .pop_front()
                        .expect("fresh object ids prevalidated");
                    current.reserved_object_id = Some(object_id);
                    object_id
                };
                persisted_blocks.push(PersistedPayloadEntry::from_payload(
                    block.block_index,
                    object_id,
                    block.data.freeze(),
                ));
                continue;
            }

            let object_id = fresh_object_ids
                .pop_front()
                .expect("fresh object ids prevalidated");
            persisted_blocks.push(PersistedPayloadEntry::from_payload(
                block.block_index,
                object_id,
                block.data.freeze(),
            ));
        }

        Ok(persisted_blocks)
    }

    fn build_overlay_slice_records(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) -> VfsResult<
        Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
    > {
        let mut chunks: std::collections::BTreeMap<u64, Vec<studiofs_meta::schema::SliceRecord>> =
            std::collections::BTreeMap::new();

        if !dirty_chunk_slices.is_empty() {
            // Dirty-chunk-slices path: persisted slice payloads can commit
            // directly as overlay SliceRecords, while buffered block snapshots
            // still validate against the final block geometry below.
            let block_lengths_by_block_index: std::collections::HashMap<u32, usize> =
                persisted_blocks
                    .iter()
                    .map(|persisted| (persisted.block_index, persisted.len()))
                    .collect();
            for (chunk_id, mut slices) in dirty_chunk_slices {
                slices.sort_by_key(|slice| slice.seq);
                for slice in slices {
                    if let Some(object_id) = slice.object_id {
                        let slice_len = slice.range.len();
                        chunks.entry(chunk_id).or_default().push(
                            studiofs_meta::schema::SliceRecord {
                                slice_id: studiofs_meta::schema::SliceId::default(),
                                order: studiofs_meta::schema::SliceOrder::default(),
                                range: slice.range,
                                payload: Some(studiofs_meta::schema::PayloadRef {
                                    object_id,
                                    offset: slice.object_offset,
                                    length: slice_len,
                                    flags: studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK,
                                }),
                                payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
                            },
                        );
                        continue;
                    }
                    let Some(&block_len) = block_lengths_by_block_index.get(&slice.block_index)
                    else {
                        continue;
                    };
                    let slice_len = slice.range.len();
                    let block_offset = slice.block_offset;
                    let block_end = (block_offset as usize).saturating_add(slice_len as usize);
                    if block_end > block_len {
                        return Err(crate::VfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!(
                                "dirty slice exceeds block length: ino={}, \
                                 block_index={}, block_len={}, block_offset={}, \
                                 slice_len={}",
                                ino, slice.block_index, block_len, block_offset, slice_len
                            ),
                        )));
                    }
                }
            }
        }

        for persisted in persisted_blocks {
            self.append_persisted_block_slice_records(&mut chunks, persisted)?;
        }

        for slices in chunks.values_mut() {
            let mut next_slice_id = 1u64;
            let mut next_slice_order = 1u64;
            for slice in slices.iter_mut() {
                slice.slice_id = studiofs_meta::schema::SliceId(next_slice_id);
                slice.order = studiofs_meta::schema::SliceOrder(next_slice_order);
                next_slice_id = next_slice_id.saturating_add(1);
                next_slice_order = next_slice_order.saturating_add(1);
            }
        }

        Ok(chunks
            .into_iter()
            .map(|(k, v)| (studiofs_meta::schema::ChunkId(k), v))
            .collect())
    }

    fn append_persisted_block_slice_records(
        &self,
        chunks: &mut std::collections::BTreeMap<u64, Vec<studiofs_meta::schema::SliceRecord>>,
        persisted: &PersistedPayloadEntry,
    ) -> VfsResult<()> {
        let block_start = (persisted.block_index as u64).saturating_mul(self.block_size as u64);
        let block_len = u64::try_from(persisted.len()).map_err(|_| {
            crate::VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "persisted block {} length {} exceeds u64::MAX",
                    persisted.block_index,
                    persisted.len()
                ),
            ))
        })?;
        let block_end = block_start.checked_add(block_len).ok_or_else(|| {
            crate::VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "persisted block {} end overflows: start={}, len={}",
                    persisted.block_index, block_start, block_len
                ),
            ))
        })?;
        let chunk_size = studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64;
        let mut file_pos = block_start;
        let mut flags = studiofs_meta::schema::BlockFlags::empty();
        if persisted.len() < self.block_size as usize {
            flags |= studiofs_meta::schema::BlockFlags::PARTIAL_BLOCK;
        }

        while file_pos < block_end {
            let chunk_id = file_pos / chunk_size;
            let chunk_start = chunk_id * chunk_size;
            let chunk_offset = file_pos - chunk_start;
            let remaining_in_block = block_end - file_pos;
            let remaining_in_chunk = chunk_size - chunk_offset;
            let slice_len = remaining_in_block.min(remaining_in_chunk);
            let block_offset = file_pos - block_start;
            let slice_start = u32::try_from(chunk_offset).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "chunk offset {} for block {} exceeds u32::MAX",
                        chunk_offset, persisted.block_index
                    ),
                ))
            })?;
            let slice_len_u32 = u32::try_from(slice_len).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "slice length {} for block {} exceeds u32::MAX",
                        slice_len, persisted.block_index
                    ),
                ))
            })?;
            let slice_end = slice_start.checked_add(slice_len_u32).ok_or_else(|| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "slice end overflow for block {}: start={}, len={}",
                        persisted.block_index, slice_start, slice_len_u32
                    ),
                ))
            })?;
            let slice_range = studiofs_meta::schema::SliceRange::new(slice_start, slice_end)
                .ok_or_else(|| {
                    crate::VfsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("invalid slice range {}..{}", slice_start, slice_end),
                    ))
                })?;
            let block_offset_u32 = u32::try_from(block_offset).map_err(|_| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "block offset {} for block {} exceeds u32::MAX",
                        block_offset, persisted.block_index
                    ),
                ))
            })?;
            chunks
                .entry(chunk_id)
                .or_default()
                .push(studiofs_meta::schema::SliceRecord {
                    slice_id: studiofs_meta::schema::SliceId::default(),
                    order: studiofs_meta::schema::SliceOrder::default(),
                    range: slice_range,
                    payload: Some(studiofs_meta::schema::PayloadRef {
                        object_id: persisted.object_id,
                        offset: block_offset_u32,
                        length: slice_len_u32,
                        flags,
                    }),
                    payload_state: studiofs_meta::schema::PayloadRefState::LocalOnly,
                });
            file_pos = file_pos.checked_add(slice_len).ok_or_else(|| {
                crate::VfsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "file position overflow while splitting block {}",
                        persisted.block_index
                    ),
                ))
            })?;
        }

        Ok(())
    }

    async fn execute_drain(
        &self,
        ino: Ino,
        persisted_blocks: Vec<PersistedPayloadEntry>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<DrainOutcome> {
        if persisted_blocks.is_empty() {
            if let Some(attr) = attr_to_persist {
                let committed_attr = self
                    .commit_meta
                    .commit_overlay(ino, Vec::new(), Some(attr))
                    .await?;
                self.observer
                    .on_metadata_committed(ino, committed_attr.as_ref());
            }
            return Ok(DrainOutcome {
                persisted_blocks: Vec::new(),
            });
        }

        if let Err(error) = self
            .local_payloads
            .persist_payload(&persisted_blocks, persist_config)
            .await
        {
            self.local_payloads
                .cleanup_uncommitted(ino, &persisted_blocks, "payload-persist")
                .await;
            return Err(error);
        }

        let chunk_slices =
            match self.build_overlay_slice_records(ino, &persisted_blocks, dirty_chunk_slices) {
                Ok(slices) => slices,
                Err(error) => {
                    self.local_payloads
                        .cleanup_uncommitted(ino, &persisted_blocks, "metadata-build")
                        .await;
                    return Err(error);
                }
            };
        let committed_attr = match self
            .commit_meta
            .commit_overlay(ino, chunk_slices, attr_to_persist)
            .await
        {
            Ok(attr) => attr,
            Err(error) => {
                self.local_payloads
                    .cleanup_uncommitted(ino, &persisted_blocks, "metadata-commit")
                    .await;
                return Err(error);
            }
        };

        self.observer
            .on_metadata_committed(ino, committed_attr.as_ref());
        self.uploads
            .submit_uploads(ino, &persisted_blocks, content_epoch)
            .await;

        Ok(DrainOutcome { persisted_blocks })
    }
}

impl<'a> LocalDirtyDrain<'a> {
    fn for_inode(self, vfs: &'a StudioVfs, ino: Ino) -> LocalDirtyFlushSession<'a> {
        LocalDirtyFlushSession {
            ino,
            vfs,
            drain: self,
        }
    }
}

impl BlockIdAllocatorPort for VfsBlockIdAllocator<'_> {
    async fn next_object_ids(&self, count: u32) -> VfsResult<Vec<u64>> {
        run_meta(self.vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_object_ids(count)
        })
        .await
    }
}

impl LocalPayloadWriterPort for VfsLocalPayloadWriter<'_> {
    async fn persist_payload(
        &self,
        persisted_blocks: &[PersistedPayloadEntry],
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<()> {
        let payload_entries: Vec<&PersistedPayloadEntry> = persisted_blocks
            .iter()
            .filter(|block| block.requires_payload_persist())
            .collect();
        if payload_entries.is_empty() {
            return Ok(());
        }
        let persisted_bytes: u64 = payload_entries.iter().map(|b| b.len() as u64).sum();
        let started = Instant::now();
        let result: VfsResult<()> = async {
            let persisted_object_ids: Vec<u64> = payload_entries
                .iter()
                .map(|block| block.object_id)
                .collect();
            for chunk in payload_entries.chunks(persist_config.put_concurrency.max(1)) {
                let batch = chunk
                    .iter()
                    .map(|block| studiofs_cache::local::LocalPayloadPut {
                        object_id: block.object_id,
                        data: block
                            .payload_bytes()
                            .expect("payload entries keep inline bytes")
                            .clone(),
                        evictable: false,
                    })
                    .collect();
                self.vfs.payload_store_put_batch_async(batch).await?;
            }
            for chunk in payload_entries.chunks(persist_config.put_concurrency.max(1)) {
                let batch = chunk
                    .iter()
                    .map(|block| studiofs_cache::local::LocalPayloadPut {
                        object_id: block.object_id,
                        data: block
                            .payload_bytes()
                            .expect("payload entries keep inline bytes")
                            .clone(),
                        evictable: false,
                    })
                    .collect();
                self.vfs.cache_upload_hot_batch(batch);
            }
            self.vfs
                .maybe_payload_store_sync_data_async(
                    persisted_bytes,
                    persist_config.local_sync_policy,
                )
                .await?;
            if !persisted_object_ids.is_empty()
                && let Err(error) = self
                    .vfs
                    .upload_stage_link_batch_async(persisted_object_ids)
                    .await
            {
                warn!(
                    persisted_payload_entry_count = payload_entries.len(),
                    error = %error,
                    "failed to create zero-copy upload links for durable payload objects"
                );
            }
            Ok(())
        }
        .await;
        let completed_burst = result.is_ok();
        if persisted_bytes > 0 {
            self.vfs.record_payload_persist_observability(
                payload_entries.len(),
                persisted_bytes,
                started.elapsed(),
                completed_burst,
            );
        }
        result
    }

    async fn cleanup_uncommitted(
        &self,
        ino: Ino,
        blocks: &[PersistedPayloadEntry],
        stage: &'static str,
    ) {
        let object_ids: Vec<u64> = blocks
            .iter()
            .filter(|block| block.requires_payload_persist())
            .map(|block| block.object_id)
            .collect();
        if object_ids.is_empty() {
            return;
        }
        if let Err(error) = self.vfs.payload_store_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of uncommitted payload-store entries failed"
            );
        }
        if let Err(error) = self.vfs.upload_stage_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of uncommitted upload entries failed"
            );
        }
        self.vfs.upload_hot_remove_batch(&object_ids);
    }
}

impl OverlayCommitPort for VfsOverlayCommitter<'_> {
    async fn commit_overlay(
        &self,
        ino: Ino,
        chunk_slices: Vec<(
            studiofs_meta::schema::ChunkId,
            Vec<studiofs_meta::schema::SliceRecord>,
        )>,
        attr: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>> {
        let started = Instant::now();
        let result = run_meta(self.vfs.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.flush_regular_file_overlay_slices_with_attr(ino, &chunk_slices, attr.as_ref())
        })
        .await;
        self.vfs
            .record_metadata_commit_observability(started.elapsed(), result.is_ok());
        result
    }
}

impl UploadSubmissionPort for VfsUploadSubmitter<'_> {
    async fn submit_uploads(
        &self,
        ino: Ino,
        persisted_blocks: &[PersistedPayloadEntry],
        content_epoch: u64,
    ) {
        let mut submit_ops = Vec::with_capacity(persisted_blocks.len());
        let mut live_extents = Vec::with_capacity(persisted_blocks.len());
        for block in persisted_blocks {
            let offset = u64::from(block.block_index).saturating_mul(self.vfs.block_size as u64);
            submit_ops.push((ino, offset, block.object_id, content_epoch));
            live_extents.push((ino, block.block_index, block.object_id, content_epoch));
        }
        self.vfs.record_live_extents(&live_extents);
        if !submit_ops.is_empty() {
            self.vfs.cached_store.submit_uploads_batch(&submit_ops);
            self.vfs.state.file_session.note_background_transition(
                ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }
    }
}

impl DrainCommitObserverPort for VfsDrainCommitObserver<'_> {
    fn on_metadata_committed(&self, ino: Ino, committed_attr: Option<&InodeAttr>) {
        if let Some(attr) = committed_attr {
            self.vfs.state.namespace.attr_read_cache.lock().remove(ino);
            self.vfs
                .refresh_inode_incarnation_state_from_attr(ino, attr);
        }
    }
}

impl<'a> LocalDirtyFlushSession<'a> {
    async fn commit_pending_slice_snapshot(
        &self,
        pending_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
    ) -> VfsResult<()> {
        if pending_slices.is_empty() {
            return Ok(());
        }

        let chunk_slices = self.drain.build_overlay_slice_records(
            self.ino,
            &Vec::new(),
            pending_slices.clone(),
        )?;
        let attr_to_clear = attr_to_persist.clone();
        let committed_attr = self
            .drain
            .commit_meta
            .commit_overlay(self.ino, chunk_slices, attr_to_persist)
            .await?;
        self.drain
            .observer
            .on_metadata_committed(self.ino, committed_attr.as_ref());

        let upload_ops: Vec<_> = pending_slices
            .iter()
            .flat_map(|(chunk_id, slices)| {
                slices.iter().filter_map(|slice| {
                    slice.object_id.map(|object_id| {
                        let file_offset = studiofs_meta::schema::chunk_start_offset(
                            studiofs_meta::schema::ChunkId(*chunk_id),
                            studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                        )
                        .saturating_add(slice.range.start as u64);
                        (self.ino, file_offset, object_id, content_epoch)
                    })
                })
            })
            .collect();
        if !upload_ops.is_empty() {
            let mut latest_live_extents = std::collections::BTreeMap::<u32, (u64, u64)>::new();
            for slices in pending_slices.values() {
                for slice in slices {
                    let Some(object_id) = slice.object_id else {
                        continue;
                    };
                    if slice.block_offset != 0
                        || slice.object_offset != 0
                        || slice.len() != self.vfs.block_size as usize
                    {
                        continue;
                    }
                    latest_live_extents
                        .entry(slice.block_index)
                        .and_modify(|(current_object_id, current_seq)| {
                            if slice.seq > *current_seq
                                || (slice.seq == *current_seq && object_id > *current_object_id)
                            {
                                *current_object_id = object_id;
                                *current_seq = slice.seq;
                            }
                        })
                        .or_insert((object_id, slice.seq));
                }
            }
            let live_extents: Vec<_> = latest_live_extents
                .into_iter()
                .map(|(block_index, (object_id, _))| {
                    (self.ino, block_index, object_id, content_epoch)
                })
                .collect();
            self.vfs.record_live_extents(&live_extents);
            self.vfs.cached_store.submit_uploads_batch(&upload_ops);
            self.vfs.state.file_session.note_background_transition(
                self.ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }

        self.vfs
            .commit_pending_local_dirty_slices(self.ino, &pending_slices);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.vfs
                .clear_matching_dirty_attr_if_committed(self.ino, attr);
        }
        Ok(())
    }

    async fn drain_snapshot(
        &self,
        mut blocks: Vec<DirtyWriteSnapshot>,
        dirty_chunk_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        persist_config: PayloadPersistConfig,
    ) -> VfsResult<Vec<PersistedPayloadEntry>> {
        let mut fresh_object_ids = std::collections::VecDeque::new();
        let persisted_blocks = loop {
            let prepare_result = {
                let mut pending = self.vfs.state.local_dirty.pending_payloads.lock();
                self.drain.try_prepare_persisted_blocks(
                    &mut pending,
                    self.ino,
                    blocks,
                    &mut fresh_object_ids,
                )
            };
            match prepare_result {
                Ok(persisted_blocks) => break persisted_blocks,
                Err((returned_blocks, additional_needed)) => {
                    blocks = returned_blocks;
                    let additional_needed = u32::try_from(additional_needed).map_err(|_| {
                        VfsError::Io(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!(
                                "requested additional object-id count {} exceeds u32::MAX",
                                additional_needed
                            ),
                        ))
                    })?;
                    let additional_ids = self.drain.allocate_object_ids(additional_needed).await?;
                    fresh_object_ids.extend(additional_ids);
                }
            }
        };
        let attr_to_clear = attr_to_persist.clone();
        let outcome = self
            .drain
            .execute_drain(
                self.ino,
                persisted_blocks,
                dirty_chunk_slices,
                content_epoch,
                attr_to_persist,
                persist_config,
            )
            .await?;
        self.vfs
            .commit_persisted_local_dirty_payloads(self.ino, &outcome.persisted_blocks);
        if let Some(attr) = attr_to_clear.as_ref() {
            self.vfs
                .clear_matching_dirty_attr_if_committed(self.ino, attr);
        }
        Ok(outcome.persisted_blocks)
    }

    async fn flush(&self, local_sync_policy: LocalSyncPolicy) -> VfsResult<()> {
        self.vfs
            .state
            .metrics
            .flush_barrier_calls
            .fetch_add(1, Ordering::Relaxed);
        self.vfs.enforce_access(self.ino, AccessLevel::ReadWrite)?;
        if self.vfs.ignored_entry_attr(self.ino).is_some() {
            return Ok(());
        }

        let blocks_to_buffer = self.vfs.snapshot_dirty_local_dirty_payloads(self.ino);
        let mut dirty_attr = {
            self.vfs
                .state
                .namespace
                .dirty_attrs
                .lock()
                .get(&self.ino)
                .cloned()
        };
        if !blocks_to_buffer.is_empty() {
            let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
                attr.content_epoch
            } else {
                self.vfs.current_content_epoch_async(self.ino).await?
            };
            let chunk_slices = self
                .vfs
                .snapshot_dirty_local_dirty_chunk_slices(self.ino, &blocks_to_buffer);
            self.drain_snapshot(
                blocks_to_buffer,
                chunk_slices,
                content_epoch,
                dirty_attr,
                PayloadPersistConfig {
                    put_concurrency: self.drain.put_concurrency,
                    local_sync_policy,
                },
            )
            .await?;
            dirty_attr = self
                .vfs
                .state
                .namespace
                .dirty_attrs
                .lock()
                .get(&self.ino)
                .cloned();
        }

        let pending_slices = self
            .vfs
            .snapshot_pending_persisted_local_dirty_slices(self.ino);
        let pending_slices = self
            .vfs
            .select_pending_persisted_local_dirty_slice_commit_wave(
                self.ino,
                pending_slices,
                local_sync_policy,
            );
        if !pending_slices.is_empty() {
            let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
                attr.content_epoch
            } else {
                self.vfs.current_content_epoch_async(self.ino).await?
            };
            self.commit_pending_slice_snapshot(pending_slices, content_epoch, dirty_attr)
                .await?;
            dirty_attr = self
                .vfs
                .state
                .namespace
                .dirty_attrs
                .lock()
                .get(&self.ino)
                .cloned();
        }

        if let Some(attr) = dirty_attr
            && (!matches!(local_sync_policy, LocalSyncPolicy::Opportunistic)
                || !self
                    .vfs
                    .has_pending_local_dirty_payloads_for_inode(self.ino))
        {
            let _ = self
                .drain
                .execute_drain(
                    self.ino,
                    Vec::new(),
                    Default::default(),
                    attr.content_epoch,
                    Some(attr),
                    PayloadPersistConfig {
                        put_concurrency: self.drain.put_concurrency,
                        local_sync_policy,
                    },
                )
                .await?;
        }

        Ok(())
    }
}

impl StudioVfs {
    fn direct_full_block_snapshots_for_buffered_writes(
        &self,
        writes: &[(u64, Bytes)],
        visible_size: u64,
        fully_overwritten_blocks: &std::collections::HashSet<u32>,
    ) -> std::collections::BTreeMap<u32, DirtyWriteSnapshot> {
        let mut counts = std::collections::HashMap::<u32, usize>::new();
        let mut candidates = std::collections::BTreeMap::<u32, DirtyWriteSnapshot>::new();

        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }

            let mut remaining = data.len();
            let mut current_offset = *offset;
            let mut data_offset = 0usize;
            while remaining > 0 {
                let block_index = block_index_from_offset(current_offset, self.block_size);
                let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
                let space_in_block = self.block_size as usize - offset_in_block;
                let to_write = remaining.min(space_in_block);
                *counts.entry(block_index).or_default() += 1;
                if fully_overwritten_blocks.contains(&block_index) {
                    let logical_len = self.logical_block_len(visible_size, block_index);
                    if offset_in_block == 0 && to_write == logical_len {
                        candidates.insert(
                            block_index,
                            DirtyWriteSnapshot::from_shared(
                                block_index,
                                data.slice(data_offset..data_offset + to_write),
                            ),
                        );
                    } else {
                        candidates.remove(&block_index);
                    }
                }
                remaining -= to_write;
                current_offset += to_write as u64;
                data_offset += to_write;
            }
        }

        candidates.retain(|block_index, _| counts.get(block_index) == Some(&1));
        candidates
    }

    pub(super) async fn build_dirty_write_snapshots_from_buffered_writes(
        &self,
        ino: Ino,
        writes: &[(u64, Bytes)],
        visible_size: u64,
    ) -> VfsResult<Vec<DirtyWriteSnapshot>> {
        let pending_upload_index = self.snapshot_pending_upload_index();
        let fully_overwritten_blocks =
            self.fully_overwritten_block_indexes_for_buffered_writes(writes, visible_size);
        let direct_snapshots = self.direct_full_block_snapshots_for_buffered_writes(
            writes,
            visible_size,
            &fully_overwritten_blocks,
        );
        let mut blocks = std::collections::BTreeMap::<u32, BytesMut>::new();

        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }

            let mut remaining = data.len();
            let mut current_offset = *offset;
            let mut data_offset = 0usize;
            while remaining > 0 {
                let block_index = block_index_from_offset(current_offset, self.block_size);
                let offset_in_block = offset_within_block(current_offset, self.block_size) as usize;
                let space_in_block = self.block_size as usize - offset_in_block;
                let to_write = remaining.min(space_in_block);
                if direct_snapshots.contains_key(&block_index) {
                    remaining -= to_write;
                    current_offset += to_write as u64;
                    data_offset += to_write;
                    continue;
                }

                if let std::collections::btree_map::Entry::Vacant(entry) = blocks.entry(block_index)
                {
                    let existing_data = if fully_overwritten_blocks.contains(&block_index) {
                        Bytes::new()
                    } else {
                        match self
                            .read_block_if_exists(ino, block_index, Some(&pending_upload_index))
                            .await?
                        {
                            Some(data) => {
                                let logical_len = self.logical_block_len(visible_size, block_index);
                                if data.len() > logical_len {
                                    data.slice(0..logical_len)
                                } else {
                                    data
                                }
                            }
                            None => Bytes::new(),
                        }
                    };
                    entry.insert(self.clone_into_write_buffer(existing_data.as_ref()));
                }

                let block = blocks
                    .get_mut(&block_index)
                    .expect("block buffer inserted before write");
                let end_pos = offset_in_block + to_write;
                if block.len() < end_pos {
                    block.resize(end_pos, 0);
                }
                block[offset_in_block..end_pos]
                    .copy_from_slice(&data[data_offset..data_offset + to_write]);

                remaining -= to_write;
                current_offset += to_write as u64;
                data_offset += to_write;
            }
        }

        let mut snapshots: Vec<DirtyWriteSnapshot> = direct_snapshots.into_values().collect();
        snapshots.extend(blocks.into_iter().filter_map(|(block_index, mut data)| {
            let logical_len = self.logical_block_len(visible_size, block_index);
            if data.len() > logical_len {
                data.truncate(logical_len);
            }
            if data.is_empty() {
                None
            } else {
                Some(DirtyWriteSnapshot::from_mutable(block_index, data))
            }
        }));
        Ok(snapshots)
    }

    fn build_dirty_write_snapshot_slice_spill_batches(
        &self,
        blocks: &[DirtyWriteSnapshot],
    ) -> Vec<BufferedWriteSpillBatch> {
        if blocks.is_empty() {
            return Vec::new();
        }

        let spill_bytes_cap = self.handle_writer_slice_flush_bytes();
        let mut ordered_blocks: Vec<&DirtyWriteSnapshot> = blocks.iter().collect();
        ordered_blocks.sort_unstable_by_key(|block| block.block_index);

        let mut batches = Vec::new();
        let mut current_payload = BytesMut::new();
        let mut current_segments = Vec::new();

        let flush_current_batch =
            |payload: &mut BytesMut,
             segments: &mut Vec<BufferedWriteSpillSegment>,
             out: &mut Vec<BufferedWriteSpillBatch>| {
                if segments.is_empty() {
                    payload.clear();
                    return;
                }
                out.push(BufferedWriteSpillBatch {
                    payload: payload.split().freeze(),
                    segments: std::mem::take(segments),
                });
            };

        for block in ordered_blocks {
            if block.len() == 0 {
                continue;
            }

            let fits_current_batch = current_payload.is_empty()
                || current_payload.len().saturating_add(block.len()) <= spill_bytes_cap;
            if !current_segments.is_empty() && !fits_current_batch {
                flush_current_batch(&mut current_payload, &mut current_segments, &mut batches);
            }

            let object_offset = u32::try_from(current_payload.len())
                .expect("snapshot spill payload offset must fit in u32");
            let file_offset =
                u64::from(block.block_index).saturating_mul(u64::from(self.block_size));
            current_payload.extend_from_slice(block.as_ref());
            current_segments.push(BufferedWriteSpillSegment {
                block_index: block.block_index,
                file_offset,
                len: block.len(),
                object_offset,
            });
        }

        flush_current_batch(&mut current_payload, &mut current_segments, &mut batches);
        batches
    }

    async fn persist_buffered_write_slice_spill_batches(
        &self,
        ino: Ino,
        spill_batches: Vec<BufferedWriteSpillBatch>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<bool> {
        if spill_batches.is_empty() {
            return Ok(false);
        }

        let object_count = u32::try_from(spill_batches.len()).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "requested buffered-write spill batch count {} exceeds u32::MAX",
                    spill_batches.len()
                ),
            ))
        })?;
        let object_ids = self
            .local_dirty_drain()
            .allocate_object_ids(object_count)
            .await?;
        let payloads: Vec<(u64, Bytes)> = spill_batches
            .iter()
            .zip(object_ids.iter().copied())
            .map(|(batch, object_id)| (object_id, batch.payload.clone()))
            .collect();
        let logical_block_count = spill_batches
            .iter()
            .flat_map(|batch| batch.segments.iter().map(|segment| segment.block_index))
            .collect::<std::collections::HashSet<_>>()
            .len();
        let touched_blocks: std::collections::HashSet<u32> = spill_batches
            .iter()
            .flat_map(|batch| batch.segments.iter().map(|segment| segment.block_index))
            .collect();
        self.persist_local_object_payloads(payloads, logical_block_count, local_sync_policy)
            .await?;
        for block_index in &touched_blocks {
            self.clear_pending_payload(ino, *block_index);
        }
        for (batch, object_id) in spill_batches.iter().zip(object_ids.iter().copied()) {
            for segment in &batch.segments {
                self.install_persisted_pending_local_dirty_slice(
                    ino,
                    PersistedPendingLocalDirtySliceSpec {
                        block_index: segment.block_index,
                        file_offset: segment.file_offset,
                        len: segment.len,
                        seq: object_id,
                        object_id,
                        initial_object_offset: segment.object_offset,
                    },
                );
            }
        }
        Ok(true)
    }

    fn fully_overwritten_block_indexes_for_buffered_writes(
        &self,
        writes: &[(u64, Bytes)],
        visible_size: u64,
    ) -> std::collections::HashSet<u32> {
        let mut ranges_by_block = std::collections::BTreeMap::<u32, Vec<(usize, usize)>>::new();

        for (offset, data) in writes {
            if data.is_empty() {
                continue;
            }

            let mut remaining = data.len();
            let mut current_offset = *offset;
            while remaining > 0 {
                let block_index = block_index_from_offset(current_offset, self.block_size);
                let start = offset_within_block(current_offset, self.block_size) as usize;
                let space_in_block = self.block_size as usize - start;
                let written = remaining.min(space_in_block);
                ranges_by_block
                    .entry(block_index)
                    .or_default()
                    .push((start, start + written));
                remaining -= written;
                current_offset += written as u64;
            }
        }

        ranges_by_block
            .into_iter()
            .filter_map(|(block_index, mut ranges)| {
                let logical_len = self.logical_block_len(visible_size, block_index);
                if logical_len == 0 {
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

    pub(super) async fn materialize_buffered_writes_direct(
        &self,
        ino: Ino,
        writes: &[(u64, Bytes)],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        let dirty_attr = { self.state.namespace.dirty_attrs.lock().get(&ino).cloned() };
        let visible_size = if let Some(attr) = dirty_attr.as_ref() {
            attr.size
        } else {
            self.getattr_async(ino).await?.size
        };
        let blocks = self
            .build_dirty_write_snapshots_from_buffered_writes(ino, writes, visible_size)
            .await?;
        if blocks.is_empty() {
            return Ok(());
        }

        if matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            let (completed_blocks, buffered_blocks): (Vec<_>, Vec<_>) = blocks
                .into_iter()
                .partition(|block| block.len() == self.block_size as usize);
            if !completed_blocks.is_empty() {
                let spill_batches =
                    self.build_dirty_write_snapshot_slice_spill_batches(&completed_blocks);
                self.persist_buffered_write_slice_spill_batches(
                    ino,
                    spill_batches,
                    local_sync_policy,
                )
                .await?;
            }
            if !buffered_blocks.is_empty() {
                self.stage_dirty_write_snapshots(ino, buffered_blocks);
            }
            return Ok(());
        }

        let content_epoch = if let Some(attr) = dirty_attr.as_ref() {
            attr.content_epoch
        } else {
            self.current_content_epoch_async(ino).await?
        };

        self.persist_dirty_write_snapshots_direct(
            ino,
            blocks,
            content_epoch,
            dirty_attr,
            local_sync_policy,
        )
        .await
    }

    pub(super) async fn derive_opportunistic_prefix_attr(
        &self,
        ino: Ino,
        dirty_attr: Option<&InodeAttr>,
        completed_blocks: &[DirtyWriteSnapshot],
    ) -> VfsResult<Option<InodeAttr>> {
        let Some(max_completed_end) = completed_blocks
            .iter()
            .map(|block| {
                (u64::from(block.block_index) + 1).saturating_mul(u64::from(self.block_size))
            })
            .max()
        else {
            return Ok(None);
        };

        let persisted_attr = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.get_attr(ino)
        })
        .await?;
        let mut attr = dirty_attr
            .cloned()
            .unwrap_or_else(|| persisted_attr.clone());
        attr.size = persisted_attr.size.max(max_completed_end);
        Ok(Some(attr))
    }

    pub(super) async fn persist_dirty_write_snapshots_direct(
        &self,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let spill_batches = self.build_dirty_write_snapshot_slice_spill_batches(&blocks);
        if spill_batches.is_empty() {
            return Ok(());
        }

        let object_count = u32::try_from(spill_batches.len()).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "requested direct snapshot spill batch count {} exceeds u32::MAX",
                    spill_batches.len()
                ),
            ))
        })?;
        let object_ids = self
            .local_dirty_drain()
            .allocate_object_ids(object_count)
            .await?;
        let payloads: Vec<(u64, Bytes)> = spill_batches
            .iter()
            .zip(object_ids.iter().copied())
            .map(|(batch, object_id)| (object_id, batch.payload.clone()))
            .collect();
        let logical_block_count = spill_batches
            .iter()
            .flat_map(|batch| batch.segments.iter().map(|segment| segment.block_index))
            .collect::<std::collections::HashSet<_>>()
            .len();
        let touched_block_indexes: std::collections::HashSet<u32> =
            blocks.iter().map(|block| block.block_index).collect();
        self.persist_local_object_payloads(payloads, logical_block_count, local_sync_policy)
            .await?;
        for block_index in &touched_block_indexes {
            self.clear_pending_payload(ino, *block_index);
        }
        let mut installed_slices = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        for (batch, object_id) in spill_batches.iter().zip(object_ids.iter().copied()) {
            for segment in &batch.segments {
                let entries = self.build_persisted_pending_local_dirty_slice_entries(
                    PersistedPendingLocalDirtySliceSpec {
                        block_index: segment.block_index,
                        file_offset: segment.file_offset,
                        len: segment.len,
                        seq: object_id,
                        object_id,
                        initial_object_offset: segment.object_offset,
                    },
                );
                for (chunk_id, slice) in &entries {
                    installed_slices.entry(*chunk_id).or_default().push(*slice);
                }
                self.insert_pending_local_dirty_slice_entries(ino, entries);
            }
        }
        self.local_dirty_flush_session(ino)
            .commit_pending_slice_snapshot(installed_slices, content_epoch, attr_to_persist)
            .await?;

        Ok(())
    }

    pub(super) fn stage_dirty_write_snapshots(&self, ino: Ino, blocks: Vec<DirtyWriteSnapshot>) {
        for block in blocks {
            let reserved_object_id = self
                .state
                .local_dirty
                .pending_payloads
                .lock()
                .get(&(ino, block.block_index))
                .and_then(|pending| pending.reusable_reserved_object_id());
            self.replace_pending_local_dirty_payload(
                ino,
                block.block_index,
                PendingPayload::buffered(
                    block.data.into_pending_payload_data(self),
                    reserved_object_id,
                ),
            );
            self.clear_pending_dirty_slices_for_block(ino, block.block_index);
        }
    }

    fn local_dirty_drain(&self) -> LocalDirtyDrain<'_> {
        LocalDirtyDrainCore {
            local_payloads: VfsLocalPayloadWriter { vfs: self },
            commit_meta: VfsOverlayCommitter { vfs: self },
            object_ids: VfsBlockIdAllocator { vfs: self },
            uploads: VfsUploadSubmitter { vfs: self },
            observer: VfsDrainCommitObserver { vfs: self },
            block_size: self.block_size,
            put_concurrency: self.payload_put_concurrency,
        }
    }

    fn local_dirty_flush_session(&self, ino: Ino) -> LocalDirtyFlushSession<'_> {
        self.local_dirty_drain().for_inode(self, ino)
    }

    pub(super) fn total_pending_local_dirty_bytes(&self) -> usize {
        self.state.local_dirty.pending_bytes.load(Ordering::Relaxed)
    }

    pub(super) fn has_pending_local_dirty_payloads_for_inode(&self, ino: Ino) -> bool {
        if self
            .state
            .local_dirty
            .pending_payload_indexes
            .lock()
            .get(&ino)
            .is_some_and(|indexes| !indexes.is_empty())
        {
            return true;
        }
        self.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .get(&ino)
            .is_some_and(|chunks| chunks.values().any(|slices| !slices.is_empty()))
    }

    pub(super) fn snapshot_pending_local_dirty_inodes(&self) -> Vec<Ino> {
        let pending = self.state.local_dirty.pending_payloads.lock();
        let indexes = self.state.local_dirty.pending_payload_indexes.lock();
        let pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let mut inode_bytes = std::collections::BTreeMap::<Ino, usize>::new();
        for (ino, block_indexes) in indexes.iter() {
            if block_indexes.is_empty() {
                continue;
            }
            let bytes = block_indexes
                .iter()
                .filter_map(|block_index| pending.get(&(*ino, *block_index)))
                .map(|pending| pending.logical_len())
                .sum::<usize>();
            if bytes > 0 {
                *inode_bytes.entry(*ino).or_default() += bytes;
            }
        }
        for (ino, chunks) in pending_chunk_slices.iter() {
            let bytes = chunks
                .values()
                .flat_map(|slices| slices.iter())
                .map(|slice| slice.range.len() as usize)
                .sum::<usize>();
            if bytes > 0 {
                *inode_bytes.entry(*ino).or_default() += bytes;
            }
        }
        let mut inos_with_bytes: Vec<(Ino, usize)> = inode_bytes.into_iter().collect();
        inos_with_bytes.sort_unstable_by_key(|(_, bytes)| std::cmp::Reverse(*bytes));
        inos_with_bytes.into_iter().map(|(ino, _)| ino).collect()
    }

    #[cfg(test)]
    pub(super) fn pending_persisted_local_dirty_slice_bytes_for_inode(&self, ino: Ino) -> usize {
        self.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .get(&ino)
            .map(|chunks| {
                chunks
                    .values()
                    .flat_map(|slices| slices.iter())
                    .filter(|slice| slice.is_persisted())
                    .map(|slice| slice.range.len() as usize)
                    .sum()
            })
            .unwrap_or(0)
    }

    fn pending_slice_commit_wave_bytes(&self) -> usize {
        self.handle_writer_slice_flush_bytes()
            .saturating_mul(2)
            .min(self.write_eager_flush_drain_chunk_bytes)
            .max(self.handle_writer_slice_flush_bytes())
    }

    fn pending_slice_commit_hot_tail_bytes(&self) -> usize {
        self.handle_writer_slice_flush_bytes()
    }

    fn oldest_pending_persisted_local_dirty_slice_age(
        &self,
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) -> Option<Duration> {
        let now = Instant::now();
        pending_slices
            .values()
            .flat_map(|slices| slices.iter().copied())
            .map(|slice| slice.age(now))
            .max()
    }

    fn select_pending_persisted_local_dirty_slice_commit_wave(
        &self,
        ino: Ino,
        pending_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        local_sync_policy: LocalSyncPolicy,
    ) -> std::collections::BTreeMap<u64, Vec<PendingDirtySlice>> {
        if pending_slices.is_empty() {
            return pending_slices;
        }
        if !matches!(local_sync_policy, LocalSyncPolicy::Opportunistic) {
            return pending_slices;
        }
        if self.handle_table.open_count(ino) == 0 {
            return pending_slices;
        }

        let has_dirty_tail = self.state.handle_writers.dirty_bytes_for_inode(ino) > 0;
        if !has_dirty_tail {
            return pending_slices;
        }

        let total_bytes: usize = pending_slices
            .values()
            .flat_map(|slices| slices.iter().copied())
            .map(PendingDirtySlice::len)
            .sum();
        let oldest_age = self.oldest_pending_persisted_local_dirty_slice_age(&pending_slices);
        let commit_wave_bytes = self.pending_slice_commit_wave_bytes();
        if total_bytes < commit_wave_bytes
            && oldest_age.is_none_or(|age| age < PENDING_SLICE_COMMIT_MAX_AGE)
        {
            return std::collections::BTreeMap::new();
        }

        let mut ordered_slices = Vec::<(u64, PendingDirtySlice)>::new();
        for (chunk_id, slices) in &pending_slices {
            for slice in slices {
                ordered_slices.push((*chunk_id, *slice));
            }
        }
        ordered_slices.sort_by_key(|(chunk_id, slice)| {
            (
                slice.seq,
                studiofs_meta::schema::chunk_start_offset(
                    studiofs_meta::schema::ChunkId(*chunk_id),
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                )
                .saturating_add(slice.range.start as u64),
                slice.block_offset,
                slice.object_offset,
            )
        });

        let mut retain_tail_bytes = self.pending_slice_commit_hot_tail_bytes();
        let mut commit_candidates = Vec::<(u64, PendingDirtySlice)>::new();
        for (chunk_id, slice) in ordered_slices.iter().rev().copied() {
            let slice_len = slice.len();
            if retain_tail_bytes == 0 {
                commit_candidates.push((chunk_id, slice));
                continue;
            }
            if slice_len <= retain_tail_bytes {
                retain_tail_bytes = retain_tail_bytes.saturating_sub(slice_len);
                continue;
            }

            let commit_len = slice_len.saturating_sub(retain_tail_bytes);
            retain_tail_bytes = 0;
            if let Some(prefix) = slice.prefix(commit_len) {
                commit_candidates.push((chunk_id, prefix));
            }
        }
        commit_candidates.reverse();

        let mut selected = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        let mut selected_bytes = 0usize;
        for (chunk_id, slice) in commit_candidates.iter().copied() {
            let remaining = commit_wave_bytes.saturating_sub(selected_bytes);
            if remaining == 0 {
                break;
            }
            let commit_len = remaining.min(slice.len());
            let Some(prefix) = slice.prefix(commit_len) else {
                continue;
            };
            selected.entry(chunk_id).or_default().push(prefix);
            selected_bytes = selected_bytes.saturating_add(prefix.len());
            if commit_len < slice.len() {
                break;
            }
        }
        if selected.is_empty()
            && oldest_age.is_some_and(|age| age >= PENDING_SLICE_COMMIT_MAX_AGE)
            && let Some((chunk_id, slice)) = ordered_slices.first().copied()
        {
            let commit_len = commit_wave_bytes.min(slice.len());
            if let Some(prefix) = slice.prefix(commit_len) {
                selected.entry(chunk_id).or_default().push(prefix);
            }
        }

        for slices in selected.values_mut() {
            slices.sort_by_key(|slice| (slice.seq, slice.range.start, slice.block_offset));
        }
        selected
    }

    fn pending_buffered_local_dirty_bytes_for_inode(&self, ino: Ino) -> usize {
        self.state
            .local_dirty
            .pending_payloads
            .lock()
            .iter()
            .filter(|((candidate_ino, _), _)| *candidate_ino == ino)
            .map(|(_, block)| block.memory_len())
            .sum()
    }

    pub(super) fn should_background_flush_pending_local_dirty_inode(&self, ino: Ino) -> bool {
        if !self.has_pending_local_dirty_payloads_for_inode(ino) {
            return false;
        }

        let pending_slices = self.snapshot_pending_persisted_local_dirty_slices(ino);
        let pending_slice_bytes: usize = pending_slices
            .values()
            .flat_map(|slices| slices.iter().copied())
            .map(PendingDirtySlice::len)
            .sum();
        if pending_slice_bytes == 0 {
            if self.handle_table.open_count(ino) == 0 {
                return true;
            }

            let buffered_pending_bytes = self.pending_buffered_local_dirty_bytes_for_inode(ino);
            return buffered_pending_bytes >= self.local_sync_group_min_bytes as usize
                || self.total_pending_local_dirty_bytes() > self.max_dirty_bytes();
        }

        if self.handle_table.open_count(ino) == 0 {
            return true;
        }

        if self.state.handle_writers.dirty_bytes_for_inode(ino) == 0 {
            return true;
        }

        pending_slice_bytes >= self.pending_slice_commit_wave_bytes()
            || self
                .oldest_pending_persisted_local_dirty_slice_age(&pending_slices)
                .is_some_and(|age| age >= PENDING_SLICE_COMMIT_MAX_AGE)
    }

    pub(super) async fn persist_local_object_payloads(
        &self,
        entries: Vec<(u64, Bytes)>,
        logical_block_count: usize,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let persisted_bytes: u64 = entries.iter().map(|(_, data)| data.len() as u64).sum();
        let persisted_object_ids: Vec<u64> =
            entries.iter().map(|(object_id, _)| *object_id).collect();
        let started = Instant::now();
        let result: VfsResult<()> = async {
            for chunk in entries.chunks(self.payload_put_concurrency.max(1)) {
                let batch = chunk
                    .iter()
                    .map(|(object_id, data)| studiofs_cache::local::LocalPayloadPut {
                        object_id: *object_id,
                        data: data.clone(),
                        evictable: false,
                    })
                    .collect();
                self.payload_store_put_batch_async(batch).await?;
            }
            for chunk in entries.chunks(self.payload_put_concurrency.max(1)) {
                let batch = chunk
                    .iter()
                    .map(|(object_id, data)| studiofs_cache::local::LocalPayloadPut {
                        object_id: *object_id,
                        data: data.clone(),
                        evictable: false,
                    })
                    .collect();
                self.cache_upload_hot_batch(batch);
            }
            self.maybe_payload_store_sync_data_async(persisted_bytes, local_sync_policy)
                .await?;
            if !persisted_object_ids.is_empty()
                && let Err(error) = self
                    .upload_stage_link_batch_async(persisted_object_ids.clone())
                    .await
            {
                warn!(
                    persisted_object_count = persisted_object_ids.len(),
                    error = %error,
                    "failed to create zero-copy upload links for durable slice payloads"
                );
            }
            Ok(())
        }
        .await;
        self.record_payload_persist_observability(
            logical_block_count,
            persisted_bytes,
            started.elapsed(),
            result.is_ok(),
        );
        if result.is_err() {
            let _ = self
                .payload_store_remove_batch_async(&persisted_object_ids)
                .await;
            let _ = self
                .upload_stage_remove_batch_async(&persisted_object_ids)
                .await;
            self.upload_hot_remove_batch(&persisted_object_ids);
        }
        result
    }

    fn build_persisted_pending_local_dirty_slice_entries(
        &self,
        spec: PersistedPendingLocalDirtySliceSpec,
    ) -> Vec<(u64, PendingDirtySlice)> {
        if spec.len == 0 {
            return Vec::new();
        }
        let mut remaining = spec.len;
        let mut current_offset = spec.file_offset;
        let mut object_offset = spec.initial_object_offset as usize;
        let mut entries = Vec::new();
        while remaining > 0 {
            let chunk_id = studiofs_meta::schema::chunk_id_from_offset(
                current_offset,
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            let chunk_offset = studiofs_meta::schema::offset_within_chunk(
                current_offset,
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            let remaining_in_chunk =
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE as usize - chunk_offset as usize;
            let slice_len = remaining.min(remaining_in_chunk);
            let slice_end = chunk_offset.saturating_add(slice_len as u32);
            let Some(range) = studiofs_meta::schema::SliceRange::new(chunk_offset, slice_end)
            else {
                break;
            };
            let block_offset = offset_within_block(current_offset, self.block_size);
            entries.push((
                chunk_id.0,
                PendingDirtySlice::persisted(
                    range,
                    spec.block_index,
                    block_offset,
                    spec.seq,
                    spec.object_id,
                    object_offset as u32,
                ),
            ));
            current_offset = current_offset.saturating_add(slice_len as u64);
            object_offset = object_offset.saturating_add(slice_len);
            remaining -= slice_len;
        }
        entries
    }

    fn insert_pending_local_dirty_slice_entries(
        &self,
        ino: Ino,
        entries: Vec<(u64, PendingDirtySlice)>,
    ) {
        if entries.is_empty() {
            return;
        }
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let inode_chunks = pending_chunk_slices.entry(ino).or_default();
        for (chunk_id, slice) in entries {
            inode_chunks.entry(chunk_id).or_default().push(slice);
        }
    }

    pub(super) fn install_persisted_pending_local_dirty_slice(
        &self,
        ino: Ino,
        spec: PersistedPendingLocalDirtySliceSpec,
    ) {
        let entries = self.build_persisted_pending_local_dirty_slice_entries(spec);
        self.insert_pending_local_dirty_slice_entries(ino, entries);
    }

    fn snapshot_pending_persisted_local_dirty_slices(
        &self,
        ino: Ino,
    ) -> std::collections::BTreeMap<u64, Vec<PendingDirtySlice>> {
        let pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        pending_chunk_slices
            .get(&ino)
            .map(|chunks| {
                chunks
                    .iter()
                    .filter_map(|(chunk_id, slices)| {
                        let persisted: Vec<_> = slices
                            .iter()
                            .copied()
                            .filter(|slice| slice.is_persisted())
                            .collect();
                        (!persisted.is_empty()).then_some((*chunk_id, persisted))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn commit_pending_local_dirty_slices(
        &self,
        ino: Ino,
        committed: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) {
        if committed.is_empty() {
            return;
        }
        let committed_entries: std::collections::HashSet<PendingDirtySliceCommitKey> = committed
            .iter()
            .flat_map(|(chunk_id, slices)| slices.iter().map(|slice| slice.commit_key(*chunk_id)))
            .collect();
        let mut committed_by_chunk = committed.clone();
        for slices in committed_by_chunk.values_mut() {
            slices.sort_by_key(|slice| (slice.seq, slice.range.start, slice.block_offset));
        }
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let Some(chunks) = pending_chunk_slices.get_mut(&ino) else {
            return;
        };
        chunks.retain(|chunk_id, slices| {
            slices.retain_mut(|slice| {
                if committed_entries.contains(&slice.commit_key(*chunk_id)) {
                    return false;
                }
                let Some(committed_slices) = committed_by_chunk.get(chunk_id) else {
                    return true;
                };
                let mut current = *slice;
                for committed_slice in committed_slices {
                    let Some(committed_len) = current.committed_prefix_len(*committed_slice) else {
                        continue;
                    };
                    let Some(trimmed) = current.trim_prefix(committed_len) else {
                        return false;
                    };
                    current = trimmed;
                }
                *slice = current;
                true
            });
            !slices.is_empty()
        });
        if chunks.is_empty() {
            pending_chunk_slices.remove(&ino);
        }
    }

    pub(super) fn clear_matching_dirty_attr_if_committed(&self, ino: Ino, attr: &InodeAttr) {
        if self.has_pending_local_dirty_payloads_for_inode(ino) {
            return;
        }
        let mut dirty_attrs = self.state.namespace.dirty_attrs.lock();
        if let Some(current) = dirty_attrs.get(&ino)
            && current.content_epoch == attr.content_epoch
            && current.size == attr.size
            && current.mtime == attr.mtime
            && current.ctime == attr.ctime
        {
            dirty_attrs.remove(&ino);
        }
    }

    fn add_pending_local_dirty_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.state
            .local_dirty
            .pending_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn subtract_pending_local_dirty_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self.state.local_dirty.pending_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |pending| Some(pending.saturating_sub(bytes)),
        );
    }

    fn replace_pending_local_dirty_payload(
        &self,
        ino: Ino,
        block_index: u32,
        payload: PendingPayload,
    ) {
        let mut pending = self.state.local_dirty.pending_payloads.lock();
        let mut indexes = self.state.local_dirty.pending_payload_indexes.lock();
        let new_len = payload.memory_len();
        let replaced = pending.insert((ino, block_index), payload);
        let old_len = replaced.as_ref().map(|old| old.memory_len()).unwrap_or(0);
        indexes.entry(ino).or_default().insert(block_index);
        drop(indexes);
        drop(pending);
        match new_len.cmp(&old_len) {
            std::cmp::Ordering::Greater => self.add_pending_local_dirty_bytes(new_len - old_len),
            std::cmp::Ordering::Less => self.subtract_pending_local_dirty_bytes(old_len - new_len),
            std::cmp::Ordering::Equal => {}
        }
        if let Some(old) = replaced {
            if let Some(buffer) = old.recycle_buffer() {
                self.recycle_write_buffer(buffer);
            }
        }
    }

    fn clear_pending_dirty_slices_for_inode(&self, ino: Ino) {
        self.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .remove(&ino);
    }

    fn clear_pending_dirty_slices_for_block(&self, ino: Ino, block_index: u32) {
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| slice.block_index != block_index);
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
    }

    fn clear_pending_dirty_slices_for_blocks(&self, ino: Ino, block_indexes: &[u32]) {
        if block_indexes.is_empty() {
            return;
        }
        let block_index_set: std::collections::HashSet<u32> =
            block_indexes.iter().copied().collect();
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| !block_index_set.contains(&slice.block_index));
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
    }

    fn clear_pending_dirty_slices_exceeding_len(&self, ino: Ino, block_index: u32, new_len: usize) {
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let Some(chunks) = pending_chunk_slices.get_mut(&ino) else {
            return;
        };

        chunks.retain(|_, slices| {
            slices.retain_mut(|slice| {
                if slice.block_index != block_index {
                    return true;
                }
                let slice_len = slice.range.len() as usize;
                if slice.block_offset as usize >= new_len {
                    return false;
                }
                if slice_len <= new_len.saturating_sub(slice.block_offset as usize) {
                    return true;
                }
                let Some(slice_range) =
                    IntervalRange::new(slice.range.start as u64, slice.range.end as u64)
                else {
                    return false;
                };
                let Some(trimmed) = trim_range_to_available_len(
                    slice_range,
                    slice.block_offset as u64,
                    new_len as u64,
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE as u64,
                ) else {
                    return false;
                };
                let Some(new_end) = u32::try_from(trimmed.end()).ok() else {
                    return false;
                };
                slice.range.end = new_end;
                true
            });
            !slices.is_empty()
        });

        if chunks.is_empty() {
            pending_chunk_slices.remove(&ino);
        }
    }

    pub(super) fn clear_pending_payloads_for_inode(&self, ino: Ino) {
        let mut removed_bytes = 0usize;
        let mut removed_buffers = Vec::new();
        let mut pending = self.state.local_dirty.pending_payloads.lock();
        let mut indexes = self.state.local_dirty.pending_payload_indexes.lock();
        let block_indexes: Vec<u32> = indexes
            .get(&ino)
            .into_iter()
            .flat_map(|inode_indexes| inode_indexes.iter().copied())
            .collect();
        for block_index in block_indexes {
            if let Some(removed) = pending.remove(&(ino, block_index)) {
                removed_bytes += removed.memory_len();
                if let Some(buffer) = removed.recycle_buffer() {
                    removed_buffers.push(buffer);
                }
            }
        }
        indexes.remove(&ino);
        drop(indexes);
        drop(pending);
        self.subtract_pending_local_dirty_bytes(removed_bytes);
        for buffer in removed_buffers {
            self.recycle_write_buffer(buffer);
        }
        self.clear_pending_dirty_slices_for_inode(ino);
    }

    pub(super) fn clear_pending_payload(&self, ino: Ino, block_index: u32) {
        let mut pending = self.state.local_dirty.pending_payloads.lock();
        let mut indexes = self.state.local_dirty.pending_payload_indexes.lock();
        if let Some(removed) = pending.remove(&(ino, block_index)) {
            if let Some(inode_indexes) = indexes.get_mut(&ino) {
                inode_indexes.remove(&block_index);
                if inode_indexes.is_empty() {
                    indexes.remove(&ino);
                }
            }
            drop(indexes);
            drop(pending);
            self.subtract_pending_local_dirty_bytes(removed.memory_len());
            if let Some(buffer) = removed.recycle_buffer() {
                self.recycle_write_buffer(buffer);
            }
            self.clear_pending_dirty_slices_for_block(ino, block_index);
        }
    }

    fn snapshot_dirty_local_dirty_payloads(&self, ino: Ino) -> Vec<DirtyWriteSnapshot> {
        // Point-in-time view of dirty local-visible payload. Writers may race
        // ahead after this snapshot; commit keeps newer bytes dirty so a later
        // flush retries with the fresh view instead of dropping updates.
        let pending = self.state.local_dirty.pending_payloads.lock();
        let indexes = self.state.local_dirty.pending_payload_indexes.lock();
        indexes
            .get(&ino)
            .into_iter()
            .flat_map(|inode_indexes| inode_indexes.iter())
            .filter_map(|block_index| {
                pending
                    .get(&(ino, *block_index))
                    .filter(|pending| pending.dirty)
                    .map(|pending| {
                        DirtyWriteSnapshot::from_mutable(
                            *block_index,
                            self.clone_into_write_buffer(pending.data.as_ref()),
                        )
                    })
            })
            .collect()
    }

    fn snapshot_dirty_local_dirty_chunk_slices(
        &self,
        ino: Ino,
        blocks: &[DirtyWriteSnapshot],
    ) -> std::collections::BTreeMap<u64, Vec<PendingDirtySlice>> {
        let selected_block_indexes: std::collections::HashSet<u32> =
            blocks.iter().map(|block| block.block_index).collect();
        let block_lengths: std::collections::HashMap<u32, usize> = blocks
            .iter()
            .map(|block| (block.block_index, block.len()))
            .collect();
        if selected_block_indexes.is_empty() {
            return std::collections::BTreeMap::new();
        }

        let mut slices_by_chunk = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        if let Some(chunks) = self.state.local_dirty.pending_chunk_slices.lock().get(&ino) {
            for (chunk_id, slices) in chunks {
                let filtered: Vec<_> = slices
                    .iter()
                    .copied()
                    .filter(|slice| {
                        selected_block_indexes.contains(&slice.block_index)
                            && block_lengths.get(&slice.block_index).is_some_and(|len| {
                                (slice.block_offset as usize)
                                    .saturating_add(slice.range.len() as usize)
                                    <= *len
                            })
                    })
                    .collect();
                debug_assert!(filtered.windows(2).all(|pair| pair[0].seq <= pair[1].seq));
                if !filtered.is_empty() {
                    slices_by_chunk.insert(*chunk_id, filtered);
                }
            }
        }
        slices_by_chunk
    }

    pub(super) fn snapshot_pending_dirty_slice_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<PendingDirtyReadSpan> {
        if length == 0 {
            return Vec::new();
        }
        let Some(end) = offset.checked_add(length) else {
            return Vec::new();
        };
        let slice_entries: Vec<(u64, PendingDirtySlice)> = {
            let pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
            let Some(chunks) = pending_chunk_slices.get(&ino) else {
                return Vec::new();
            };
            chunks
                .iter()
                .flat_map(|(chunk_id, slices)| {
                    slices.iter().copied().map(|slice| (*chunk_id, slice))
                })
                .collect()
        };
        if slice_entries.is_empty() {
            return Vec::new();
        }

        let pending_payloads = self.state.local_dirty.pending_payloads.lock();
        let mut buffered_block_data = HashMap::<u32, Bytes>::new();
        let mut persisted_slice_data = HashMap::<u64, Bytes>::new();
        let mut spans = Vec::new();
        let Some(requested_range) = IntervalRange::new(offset, end) else {
            return Vec::new();
        };
        for (chunk_id, slice) in slice_entries {
            let chunk_start = studiofs_meta::schema::chunk_start_offset(
                studiofs_meta::schema::ChunkId(chunk_id),
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            let data = if let Some(object_id) = slice.object_id {
                if let Some(data) = persisted_slice_data.get(&object_id) {
                    data.clone()
                } else {
                    let Some(data) = self.local_payload_backend.get(object_id) else {
                        continue;
                    };
                    persisted_slice_data.insert(object_id, data.clone());
                    data
                }
            } else if let Some(data) = buffered_block_data.get(&slice.block_index) {
                data.clone()
            } else {
                let Some(pending_payload) = pending_payloads.get(&(ino, slice.block_index)) else {
                    continue;
                };
                if pending_payload.is_local_extent() {
                    continue;
                }
                let data = pending_payload.data.clone().freeze();
                buffered_block_data.insert(slice.block_index, data.clone());
                data
            };

            let Some(slice_range) = IntervalRange::new(
                chunk_start.saturating_add(slice.range.start as u64),
                chunk_start.saturating_add(slice.range.end as u64),
            ) else {
                continue;
            };
            let Some(overlap) = slice_range.intersection(requested_range) else {
                continue;
            };
            let start = overlap.start();
            let stop = overlap.end();
            let slice_start = slice_range.start();
            let advance = start.saturating_sub(slice_start);
            let Some(advance_u32) = u32::try_from(advance).ok() else {
                continue;
            };
            let block_offset = if slice.object_id.is_some() {
                slice.object_offset.checked_add(advance_u32)
            } else {
                slice.block_offset.checked_add(advance_u32)
            };
            let Some(block_offset) = block_offset else {
                continue;
            };
            let span_len = (stop - start) as usize;
            let Some(block_offset_usize) = usize::try_from(block_offset).ok() else {
                continue;
            };
            if block_offset_usize.saturating_add(span_len) > data.len() {
                continue;
            }
            spans.push(PendingDirtyReadSpan {
                start,
                end: stop,
                block_offset,
                seq: slice.seq,
                data,
            });
        }
        spans.sort_by_key(|span| std::cmp::Reverse(span.seq));
        spans
    }

    pub(super) fn snapshot_pending_local_dirty_block_read_spans(
        &self,
        ino: Ino,
        offset: u64,
        length: u64,
    ) -> Vec<PendingDirtyReadSpan> {
        if length == 0 {
            return Vec::new();
        }
        let Some(end) = offset.checked_add(length) else {
            return Vec::new();
        };

        let pending = self.state.local_dirty.pending_payloads.lock();
        let indexes = self.state.local_dirty.pending_payload_indexes.lock();
        let Some(inode_indexes) = indexes.get(&ino) else {
            return Vec::new();
        };
        if inode_indexes.is_empty() {
            return Vec::new();
        }

        let start_block = block_index_from_offset(offset, self.block_size);
        let end_block = block_index_from_offset(end.saturating_sub(1), self.block_size);
        let mut spans = Vec::new();
        for block_index in inode_indexes.range(start_block..=end_block) {
            let Some(block) = pending.get(&(ino, *block_index)) else {
                continue;
            };
            if block.is_local_extent() || !block.dirty || block.data.is_empty() {
                continue;
            }

            let block_start = u64::from(*block_index).saturating_mul(u64::from(self.block_size));
            let block_end = block_start.saturating_add(block.data.len() as u64);
            let start = block_start.max(offset);
            let stop = block_end.min(end);
            if start >= stop {
                continue;
            }

            let delta = start.saturating_sub(block_start);
            let Some(block_offset) = u32::try_from(delta).ok() else {
                continue;
            };
            spans.push(PendingDirtyReadSpan {
                start,
                end: stop,
                block_offset,
                seq: 0,
                data: block.data.clone().freeze(),
            });
        }
        spans.sort_by_key(|span| span.start);
        spans
    }

    pub(super) fn has_pending_dirty_slices_for_block(&self, ino: Ino, block_index: u32) -> bool {
        self.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .get(&ino)
            .is_some_and(|chunks| {
                chunks
                    .values()
                    .any(|slices| slices.iter().any(|slice| slice.block_index == block_index))
            })
    }

    fn commit_persisted_local_dirty_payloads(&self, ino: Ino, blocks: &[PersistedPayloadEntry]) {
        if blocks.is_empty() {
            return;
        }

        let mut pending = self.state.local_dirty.pending_payloads.lock();
        let mut indexes = self.state.local_dirty.pending_payload_indexes.lock();
        let (removed_bytes, removed_block_indexes, removed_buffers) =
            commit_persisted_payload_state(&mut pending, ino, blocks);
        if let Some(inode_indexes) = indexes.get_mut(&ino) {
            for block_index in &removed_block_indexes {
                inode_indexes.remove(block_index);
            }
            if inode_indexes.is_empty() {
                indexes.remove(&ino);
            }
        }
        drop(indexes);
        drop(pending);
        self.subtract_pending_local_dirty_bytes(removed_bytes);
        for buffer in removed_buffers {
            self.recycle_write_buffer(buffer);
        }
        self.clear_pending_dirty_slices_for_blocks(ino, &removed_block_indexes);
    }

    pub(super) fn trim_local_payload_staging(
        &self,
        ino: Ino,
        block_index: u32,
        new_len: usize,
    ) -> VfsResult<bool> {
        let mut changed = false;

        {
            let mut pending = self.state.local_dirty.pending_payloads.lock();
            if let Some(block) = pending.get_mut(&(ino, block_index))
                && !block.is_local_extent()
                && block.data.len() > new_len
            {
                let old_len = block.data.len();
                block.data.truncate(new_len);
                block.dirty = true;
                block.persisted_len = None;
                self.subtract_pending_local_dirty_bytes(old_len - block.data.len());
                changed = true;
            }
        }
        if changed {
            self.clear_pending_dirty_slices_exceeding_len(ino, block_index, new_len);
        }

        Ok(changed)
    }

    pub(super) async fn read_block_if_exists(
        &self,
        ino: Ino,
        block_index: u32,
        pending_upload_index: Option<&PendingUploadCatalog>,
    ) -> VfsResult<Option<Bytes>> {
        let pending_content_epoch = self.pending_content_epoch_for_inode(ino);
        if let Some(data) = self
            .read_pending_overlay_block_async(
                ino,
                block_index,
                pending_content_epoch,
                pending_upload_index,
            )
            .await?
        {
            return Ok(Some(data));
        }

        let visible_size = if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino) {
            attr.size
        } else {
            self.getattr_async(ino).await?.size
        };
        self.read_metadata_block_from_slice_graph_async(ino, block_index, visible_size)
            .await
    }

    pub(super) async fn flush_local_dirty_state(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        self.local_dirty_flush_session(ino)
            .flush(local_sync_policy)
            .await
    }

    pub(super) async fn flush_pending_payloads_for_shutdown(&self) {
        let mut inos: BTreeSet<Ino> = self
            .snapshot_pending_local_dirty_inodes()
            .into_iter()
            .collect();
        {
            let dirty_attrs = self.state.namespace.dirty_attrs.lock();
            inos.extend(dirty_attrs.keys().copied());
        }
        if inos.is_empty() {
            return;
        }

        info!(
            inode_count = inos.len(),
            "VFS shutdown: flushing dirty inodes"
        );
        for ino in inos {
            if let Err(e) = self.flush(ino).await {
                warn!(ino, error = %e, "VFS shutdown: flush inode failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    type UploadRecord = (u64, u64, u64, u64);
    type SharedUploads = Arc<StdMutex<Vec<UploadRecord>>>;
    type SharedInodes = Arc<StdMutex<Vec<Ino>>>;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("local-dirty-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("local-dirty-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    fn single_block_fixture(
        ino: Ino,
        block_index: u32,
        data: &'static [u8],
        reserved_object_id: Option<u64>,
    ) -> (
        std::collections::HashMap<(Ino, u32), PendingPayload>,
        Vec<DirtyWriteSnapshot>,
    ) {
        (
            std::collections::HashMap::from([(
                (ino, block_index),
                PendingPayload::buffered(BytesMut::from(data), reserved_object_id),
            )]),
            vec![DirtyWriteSnapshot::from_mutable(
                block_index,
                BytesMut::from(data),
            )],
        )
    }

    #[test]
    fn subtract_pending_local_dirty_bytes_saturates_without_panicking() {
        let tmp = TempDir::new().expect("tempdir");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-bytes-underflow");
        let _guard = rt.enter();
        vfs.state
            .local_dirty
            .pending_bytes
            .store(1, Ordering::Relaxed);

        vfs.subtract_pending_local_dirty_bytes(2);

        assert_eq!(
            vfs.state.local_dirty.pending_bytes.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn commit_pending_local_dirty_slices_trims_committed_prefix() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-slice-prefix");
        let ino = 77;
        let chunk_id = 0;
        let original = PendingDirtySlice::persisted(
            studiofs_meta::schema::SliceRange::new(0, 4096).expect("slice range"),
            3,
            0,
            11,
            7001,
            0,
        );
        vfs.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .entry(ino)
            .or_default()
            .insert(chunk_id, vec![original]);

        let committed_prefix = original.prefix(1024).expect("prefix slice");
        let committed = std::collections::BTreeMap::from([(chunk_id, vec![committed_prefix])]);

        vfs.commit_pending_local_dirty_slices(ino, &committed);

        {
            let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
            let remaining = pending
                .get(&ino)
                .and_then(|chunks| chunks.get(&chunk_id))
                .expect("remaining suffix");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].range.start, 1024);
            assert_eq!(remaining[0].range.end, 4096);
            assert_eq!(remaining[0].block_offset, 1024);
            assert_eq!(remaining[0].object_offset, 1024);
            assert_eq!(remaining[0].seq, original.seq);
            assert_eq!(remaining[0].object_id, original.object_id);
        }

        vfs.commit_pending_local_dirty_slices(ino, &committed);

        {
            let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
            let remaining = pending
                .get(&ino)
                .and_then(|chunks| chunks.get(&chunk_id))
                .expect("prefix should not match suffix again");
            assert_eq!(remaining.len(), 1);
            assert_eq!(remaining[0].range.start, 1024);
            assert_eq!(remaining[0].range.end, 4096);
        }
    }

    #[test]
    fn trim_local_payload_staging_trims_and_drops_pending_slices() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "trim-pending-slices");
        let ino = 78;
        let block_index = 3;
        let chunk_id = 0;

        vfs.state.local_dirty.pending_payloads.lock().insert(
            (ino, block_index),
            PendingPayload::buffered(BytesMut::from(&b"abcdefgh"[..]), None),
        );
        vfs.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .entry(ino)
            .or_default()
            .insert(
                chunk_id,
                vec![
                    PendingDirtySlice {
                        range: studiofs_meta::schema::SliceRange::new(100, 106)
                            .expect("slice range"),
                        block_index,
                        block_offset: 2,
                        seq: 10,
                        object_id: None,
                        object_offset: 0,
                        created_at: Instant::now(),
                    },
                    PendingDirtySlice {
                        range: studiofs_meta::schema::SliceRange::new(200, 202)
                            .expect("slice range"),
                        block_index,
                        block_offset: 5,
                        seq: 11,
                        object_id: None,
                        object_offset: 0,
                        created_at: Instant::now(),
                    },
                ],
            );

        let changed = vfs
            .trim_local_payload_staging(ino, block_index, 5)
            .expect("trim payload staging");
        assert!(changed, "payload bytes should be truncated");
        assert_eq!(
            vfs.state
                .local_dirty
                .pending_payloads
                .lock()
                .get(&(ino, block_index))
                .expect("pending payload")
                .data
                .len(),
            5
        );

        let pending = vfs.state.local_dirty.pending_chunk_slices.lock();
        let remaining = pending
            .get(&ino)
            .and_then(|chunks| chunks.get(&chunk_id))
            .expect("trimmed slice should remain");
        assert_eq!(remaining.len(), 1, "slice beyond new len should be dropped");
        assert_eq!(remaining[0].range.start, 100);
        assert_eq!(remaining[0].range.end, 103);
        assert_eq!(remaining[0].block_offset, 2);
    }

    #[test]
    fn snapshot_pending_dirty_slice_read_spans_handles_boundaries_and_offsets() {
        let tmp = TempDir::new().expect("tempdir");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "snapshot-pending-slices");
        let ino = 79;
        let block_index = 4;
        let chunk_id = 0;

        vfs.state.local_dirty.pending_payloads.lock().insert(
            (ino, block_index),
            PendingPayload::buffered(BytesMut::from(&b"0123456789"[..]), None),
        );
        vfs.state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .entry(ino)
            .or_default()
            .insert(
                chunk_id,
                vec![PendingDirtySlice {
                    range: studiofs_meta::schema::SliceRange::new(100, 106).expect("slice range"),
                    block_index,
                    block_offset: 2,
                    seq: 7,
                    object_id: None,
                    object_offset: 0,
                    created_at: Instant::now(),
                }],
            );

        assert!(
            vfs.snapshot_pending_dirty_slice_read_spans(ino, 94, 6)
                .is_empty(),
            "request ending at slice start should not overlap"
        );
        assert!(
            vfs.snapshot_pending_dirty_slice_read_spans(ino, 106, 4)
                .is_empty(),
            "request starting at slice end should not overlap"
        );

        let spans = vfs.snapshot_pending_dirty_slice_read_spans(ino, 102, 10);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].start, 102);
        assert_eq!(spans[0].end, 106);
        assert_eq!(spans[0].block_offset, 4);
        assert_eq!(spans[0].seq, 7);
        assert_eq!(spans[0].data.as_ref(), b"0123456789");
    }

    #[test]
    fn assign_reserved_object_ids_reuses_existing_ids_and_stores_new_ones() {
        let ino = 42;
        let mut pending = std::collections::HashMap::from([
            (
                (ino, 0),
                PendingPayload::buffered(BytesMut::from(&b"alpha"[..]), Some(111)),
            ),
            (
                (ino, 1),
                PendingPayload::buffered(BytesMut::from(&b"beta"[..]), None),
            ),
        ]);
        let mut blocks = vec![
            PersistedPayloadEntry::from_payload(0, 0, Bytes::from_static(b"alpha")),
            PersistedPayloadEntry::from_payload(1, 0, Bytes::from_static(b"beta")),
        ];

        assign_reserved_object_ids(&mut pending, ino, &mut blocks, &[222]);

        assert_eq!(blocks[0].object_id, 111);
        assert_eq!(blocks[1].object_id, 222);
        assert_eq!(
            pending.get(&(ino, 0)).unwrap().reserved_object_id,
            Some(111)
        );
        assert_eq!(
            pending.get(&(ino, 1)).unwrap().reserved_object_id,
            Some(222)
        );
    }

    #[test]
    fn commit_persisted_payload_state_clears_reserved_id_without_dropping_newer_bytes() {
        let ino = 7;
        let mut pending = std::collections::HashMap::from([(
            (ino, 3),
            PendingPayload::buffered(BytesMut::from(&b"newer"[..]), Some(333)),
        )]);
        let blocks = vec![PersistedPayloadEntry::from_payload(
            3,
            333,
            Bytes::from_static(b"older"),
        )];

        let (removed_bytes, removed_block_indexes, removed_buffers) =
            commit_persisted_payload_state(&mut pending, ino, &blocks);

        assert_eq!(removed_bytes, 0);
        assert!(removed_block_indexes.is_empty());
        assert!(removed_buffers.is_empty());
        let pending_payload = pending.get(&(ino, 3)).unwrap();
        assert_eq!(pending_payload.data.as_ref(), b"newer");
        assert_eq!(pending_payload.reserved_object_id, None);
    }

    #[test]
    fn commit_persisted_payload_state_removes_matching_local_extent_after_metadata_commit() {
        let ino = 8;
        let mut pending =
            std::collections::HashMap::from([((ino, 4), PendingPayload::local_extent(444, 4096))]);
        let blocks = vec![PersistedPayloadEntry::from_local_extent(4, 444, 4096)];

        let (removed_bytes, removed_block_indexes, removed_buffers) =
            commit_persisted_payload_state(&mut pending, ino, &blocks);

        assert_eq!(removed_bytes, 0);
        assert_eq!(removed_block_indexes, vec![4]);
        assert!(removed_buffers.is_empty());
        assert!(
            !pending.contains_key(&(ino, 4)),
            "matching local extents should disappear once metadata commit catches up"
        );
    }

    #[derive(Clone, Default)]
    struct EventLog {
        entries: Arc<StdMutex<Vec<String>>>,
    }

    impl EventLog {
        fn push(&self, entry: impl Into<String>) {
            self.entries
                .lock()
                .expect("event log lock")
                .push(entry.into());
        }

        fn snapshot(&self) -> Vec<String> {
            self.entries.lock().expect("event log lock").clone()
        }
    }

    struct PanicBlockIds;

    impl BlockIdAllocatorPort for PanicBlockIds {
        async fn next_object_ids(&self, _count: u32) -> VfsResult<Vec<u64>> {
            panic!("object ID allocator should not be used in this test")
        }
    }

    struct TestLocalPayloadWriter {
        events: EventLog,
        fail_persist: bool,
    }

    impl LocalPayloadWriterPort for TestLocalPayloadWriter {
        async fn persist_payload(
            &self,
            _persisted_blocks: &[PersistedPayloadEntry],
            _persist_config: PayloadPersistConfig,
        ) -> VfsResult<()> {
            self.events.push("payload");
            if self.fail_persist {
                Err(VfsError::Io(std::io::Error::other("payload failure")))
            } else {
                Ok(())
            }
        }

        async fn cleanup_uncommitted(
            &self,
            _ino: Ino,
            _blocks: &[PersistedPayloadEntry],
            stage: &'static str,
        ) {
            self.events.push(format!("cleanup:{stage}"));
        }
    }

    struct TestOverlayCommitter {
        events: EventLog,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
    }

    impl OverlayCommitPort for TestOverlayCommitter {
        async fn commit_overlay(
            &self,
            _ino: Ino,
            _chunk_slices: Vec<(
                studiofs_meta::schema::ChunkId,
                Vec<studiofs_meta::schema::SliceRecord>,
            )>,
            _attr: Option<InodeAttr>,
        ) -> VfsResult<Option<InodeAttr>> {
            self.events.push("metadata");
            if self.fail_commit {
                Err(VfsError::Io(std::io::Error::other("metadata failure")))
            } else {
                Ok(self.committed_attr.clone())
            }
        }
    }

    struct TestUploadSubmitter {
        events: EventLog,
        uploads: SharedUploads,
    }

    impl UploadSubmissionPort for TestUploadSubmitter {
        async fn submit_uploads(
            &self,
            ino: Ino,
            persisted_blocks: &[PersistedPayloadEntry],
            content_epoch: u64,
        ) {
            self.events.push("upload");
            let ops: Vec<_> = persisted_blocks
                .iter()
                .map(|block| {
                    (
                        ino,
                        block.block_index as u64,
                        block.object_id,
                        content_epoch,
                    )
                })
                .collect();
            self.uploads.lock().expect("uploads lock").extend(ops);
        }
    }

    struct TestDrainObserver {
        events: EventLog,
        committed_ino: SharedInodes,
    }

    impl DrainCommitObserverPort for TestDrainObserver {
        fn on_metadata_committed(&self, ino: Ino, _committed_attr: Option<&InodeAttr>) {
            self.events.push("observer");
            self.committed_ino.lock().expect("observer lock").push(ino);
        }
    }

    fn test_drain(
        events: EventLog,
        fail_persist: bool,
        fail_commit: bool,
        committed_attr: Option<InodeAttr>,
        uploads: SharedUploads,
        observed_inodes: SharedInodes,
    ) -> LocalDirtyDrainCore<
        TestLocalPayloadWriter,
        TestOverlayCommitter,
        PanicBlockIds,
        TestUploadSubmitter,
        TestDrainObserver,
    > {
        LocalDirtyDrainCore {
            local_payloads: TestLocalPayloadWriter {
                events: events.clone(),
                fail_persist,
            },
            commit_meta: TestOverlayCommitter {
                events: events.clone(),
                fail_commit,
                committed_attr,
            },
            object_ids: PanicBlockIds,
            uploads: TestUploadSubmitter {
                events: events.clone(),
                uploads,
            },
            observer: TestDrainObserver {
                events,
                committed_ino: observed_inodes,
            },
            block_size: 4096,
            put_concurrency: 4,
        }
    }

    #[test]
    fn drain_core_orders_payload_then_metadata_then_upload() {
        let ino = 11;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([111]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            9,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("drain succeeds");

        assert_eq!(
            pending
                .get(&(ino, 0))
                .and_then(|block| block.reserved_object_id),
            Some(111)
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "observer".to_string(),
                "upload".to_string(),
            ]
        );
        assert_eq!(
            *uploads.lock().expect("uploads lock"),
            vec![(ino, 0, 111, 9)]
        );
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn drain_core_cleans_up_after_metadata_failure() {
        let ino = 12;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            true,
            None,
            uploads.clone(),
            observed_inodes.clone(),
        );
        let (mut pending, blocks) = single_block_fixture(ino, 1, b"beta", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([222]);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        let error = futures::executor::block_on(drain.execute_drain(
            ino,
            persisted_blocks,
            std::collections::BTreeMap::new(),
            5,
            None,
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect_err("drain should fail");

        assert!(
            matches!(error, VfsError::Io(_)),
            "expected metadata failure to bubble up"
        );
        assert_eq!(
            events.snapshot(),
            vec![
                "payload".to_string(),
                "metadata".to_string(),
                "cleanup:metadata-commit".to_string(),
            ]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert!(observed_inodes.lock().expect("observer lock").is_empty());
    }

    #[test]
    fn prepare_persisted_blocks_requests_more_ids_when_snapshot_goes_stale() {
        let ino = 21;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", Some(700));
        let mut fresh_object_ids = std::collections::VecDeque::new();

        pending.insert(
            (ino, 0),
            PendingPayload::buffered(BytesMut::from(&b"newer"[..]), Some(700)),
        );

        let (blocks, additional_needed) = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect_err("stale snapshot should request another object id");
        assert_eq!(additional_needed, 1);

        fresh_object_ids.push_back(701);
        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare stale snapshot");
        assert_eq!(persisted_blocks[0].object_id, 701);
        let pending_payload = pending.get(&(ino, 0)).expect("pending payload");
        assert_eq!(pending_payload.data.as_ref(), b"newer");
        assert_eq!(pending_payload.reserved_object_id, Some(700));
    }

    #[test]
    fn prepare_persisted_blocks_freezes_snapshot_without_copying_payload() {
        let ino = 22;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(events, false, false, None, uploads, observed_inodes);
        let (mut pending, blocks) = single_block_fixture(ino, 0, b"alpha", None);
        let mut fresh_object_ids = std::collections::VecDeque::from([702]);
        let snapshot_ptr = blocks[0].as_ref().as_ptr();

        let persisted_blocks = drain
            .try_prepare_persisted_blocks(&mut pending, ino, blocks, &mut fresh_object_ids)
            .expect("prepare persisted payloads");

        assert_eq!(
            persisted_blocks[0]
                .payload_bytes()
                .expect("prepared block keeps inline payload")
                .as_ptr(),
            snapshot_ptr
        );
    }

    #[test]
    fn drain_core_commits_attr_only_without_uploading() {
        let ino = 13;
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let drain = test_drain(
            events.clone(),
            false,
            false,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            uploads.clone(),
            observed_inodes.clone(),
        );

        let outcome = futures::executor::block_on(drain.execute_drain(
            ino,
            Vec::new(),
            std::collections::BTreeMap::new(),
            4,
            Some(InodeAttr::new_file(ino, 1, 1000, 1000, 0o644)),
            PayloadPersistConfig {
                put_concurrency: 4,
                local_sync_policy: LocalSyncPolicy::Opportunistic,
            },
        ))
        .expect("attr-only drain succeeds");

        assert!(outcome.persisted_blocks.is_empty());
        assert_eq!(
            events.snapshot(),
            vec!["metadata".to_string(), "observer".to_string()]
        );
        assert!(uploads.lock().expect("uploads lock").is_empty());
        assert_eq!(*observed_inodes.lock().expect("observer lock"), vec![ino]);
    }

    #[test]
    fn build_overlay_slice_records_splits_blocks_across_chunk_boundaries() {
        let events = EventLog::default();
        let uploads = Arc::new(StdMutex::new(Vec::new()));
        let observed_inodes = Arc::new(StdMutex::new(Vec::new()));
        let mut drain = test_drain(events, false, false, None, uploads, observed_inodes);
        drain.block_size = 5 * 1024 * 1024;

        let chunk_slices = drain
            .build_overlay_slice_records(
                41,
                &[PersistedPayloadEntry::from_payload(
                    12,
                    9001,
                    Bytes::from(vec![0xAB; drain.block_size as usize]),
                )],
                std::collections::BTreeMap::new(),
            )
            .expect("chunk-aware block split");

        assert_eq!(
            chunk_slices.len(),
            2,
            "cross-chunk payload should split in two"
        );
        let first = &chunk_slices[0].1;
        let second = &chunk_slices[1].1;
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(chunk_slices[0].0, studiofs_meta::schema::ChunkId(0));
        assert_eq!(chunk_slices[1].0, studiofs_meta::schema::ChunkId(1));
        assert_eq!(first[0].range.start, 60 * 1024 * 1024);
        assert_eq!(
            first[0].range.end,
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE
        );
        assert_eq!(first[0].payload.as_ref().expect("payload ref").offset, 0);
        assert_eq!(
            first[0].payload.as_ref().expect("payload ref").length,
            4 * 1024 * 1024
        );
        assert_eq!(second[0].range.start, 0);
        assert_eq!(second[0].range.end, 1024 * 1024);
        assert_eq!(
            second[0].payload.as_ref().expect("payload ref").offset,
            4 * 1024 * 1024
        );
        assert_eq!(
            second[0].payload.as_ref().expect("payload ref").length,
            1024 * 1024
        );
    }

    #[test]
    fn run_bounded_tasks_stops_starting_new_work_after_first_error() {
        let third_started = Arc::new(AtomicBool::new(false));
        let third_started_flag = third_started.clone();
        let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
        let release_rx = Arc::new(StdMutex::new(Some(release_rx)));
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            let _ = release_tx.send(());
        });

        let result = futures::executor::block_on(run_bounded_tasks([0u8, 1, 2], 2, move |item| {
            let third_started_flag = third_started_flag.clone();
            let release_rx = release_rx.clone();
            async move {
                match item {
                    0 => Err("first failure"),
                    1 => {
                        let rx = release_rx
                            .lock()
                            .expect("release receiver lock")
                            .take()
                            .expect("release receiver only used once");
                        let _ = rx.await;
                        Ok::<(), &'static str>(())
                    }
                    2 => {
                        third_started_flag.store(true, Ordering::Release);
                        Ok::<(), &'static str>(())
                    }
                    _ => Ok::<(), &'static str>(()),
                }
            }
        }));

        assert_eq!(result, Err("first failure"));
        assert!(
            !third_started.load(Ordering::Acquire),
            "the helper should drain already-started work after the first error without launching more tasks"
        );
    }
}
