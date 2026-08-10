use super::overlay::PendingUploadCatalog;
use super::*;
use crate::utils::{IntervalRange, trim_range_to_available_len};
use crate::{LockTier, OrderedMutex};
use std::time::{Duration, Instant};

const PENDING_SLICE_COMMIT_MAX_AGE: Duration = Duration::from_secs(1);

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

    #[cfg(test)]
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

    fn order_key(self, chunk_id: u64) -> (u64, u64, u64, Option<u64>, u32) {
        let chunk_start = studiofs_meta::schema::chunk_start_offset(
            studiofs_meta::schema::ChunkId(chunk_id),
            studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
        );
        (
            self.seq,
            chunk_start.saturating_add(self.range.start as u64),
            chunk_start.saturating_add(self.range.end as u64),
            self.object_id,
            self.object_offset,
        )
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

#[path = "local_dirty/adapters.rs"]
mod adapters;
#[path = "local_dirty/core.rs"]
mod core;
#[path = "local_dirty/ports.rs"]
mod ports;
#[cfg(test)]
#[path = "local_dirty/test_support.rs"]
mod test_support;

use adapters::*;
use core::*;
use ports::*;

pub(super) struct LocalSyncGroupState {
    pub(super) pending_bytes: u64,
    pub(super) last_sync_at: Instant,
}

const OVERLAY_BATCH_MAX_OPERATIONS: usize = 256;
const OVERLAY_BATCH_MAX_QUEUED_OPERATIONS: usize = 256;
const OVERLAY_BATCH_MAX_QUEUED_DESCRIPTOR_BYTES: usize = 4 * 1024 * 1024;
const OVERLAY_BATCH_COALESCE_DELAY: Duration = Duration::from_millis(1);

type OverlayCompletionSender =
    tokio::sync::oneshot::Sender<Result<Option<InodeAttr>, Arc<VfsError>>>;
type OverlayCompletionList = smallvec::SmallVec<[OverlayCompletionSender; 2]>;

struct QueuedOverlayMutation {
    sequence: u128,
    mutation: studiofs_meta::RegularFileOverlayMutation,
    completion: OverlayCompletionSender,
    write_path_started_at: Instant,
    descriptor_bytes: usize,
    admission: tokio::sync::OwnedSemaphorePermit,
}

#[derive(Default)]
struct OverlayBatchInner {
    queue: std::collections::VecDeque<QueuedOverlayMutation>,
    queued_descriptor_bytes: usize,
    draining: bool,
    next_sequence: u128,
    completed_sequence: u128,
    last_admitted_sequence: Option<u128>,
    enqueued: u64,
    coalesced: u64,
    batches: u64,
    requested_operations: u64,
    actual_operations: u64,
    forced_barriers: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OverlayBatchObservability {
    /// Number of mutations admitted to the shared volatile queue.
    pub enqueued: u64,
    /// Number of mutations merged into an earlier mutation for the same inode.
    pub coalesced: u64,
    /// Current number of queued mutations, excluding the in-flight batch.
    pub queue_depth: u64,
    /// Current descriptor memory reserved by queued mutations, in bytes.
    pub queue_bytes: u64,
    /// Age of the oldest queued mutation, in milliseconds.
    pub dirty_age_ms: u64,
    /// Number of redb batches completed by the worker.
    pub batches: u64,
    /// Number of mutations requested by callers.
    pub requested_operations: u64,
    /// Number of merged mutations executed by redb.
    pub actual_operations: u64,
    /// Number of strict barriers that waited for the volatile queue.
    pub forced_barriers: u64,
}

pub(super) struct OverlayBatchState {
    inner: Mutex<OverlayBatchInner>,
    admission: Arc<tokio::sync::Semaphore>,
    space_available: tokio::sync::Notify,
    runtime: std::sync::OnceLock<tokio::runtime::Handle>,
}

pub(super) struct LocalDirtyState {
    pub(super) pending_payloads:
        OrderedMutex<std::collections::HashMap<(Ino, u32), PendingPayload>>,
    pub(super) pending_payload_indexes:
        OrderedMutex<std::collections::HashMap<Ino, std::collections::BTreeSet<u32>>>,
    pending_chunk_slices: OrderedMutex<
        std::collections::HashMap<Ino, std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>>,
    >,
    pub(super) pending_bytes: AtomicUsize,
    pending_persisted_slice_bytes: AtomicUsize,
    pending_slice_count: AtomicUsize,
    pending_persisted_slice_count: AtomicUsize,
    pub(super) write_backpressure_wait_ms: AtomicU64,
    pub(super) write_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_wait_ms: AtomicU64,
    pub(super) fast_backpressure_events: AtomicU64,
    pub(super) fast_backpressure_stall_max_ms: AtomicU64,
    pub(super) hard_backpressure_events: AtomicU64,
    pub(super) local_sync_unsynced_bytes: AtomicU64,
    pub(super) local_sync_group: tokio::sync::Mutex<LocalSyncGroupState>,
    pub(super) local_sync_gate: tokio::sync::Mutex<()>,
    pub(super) overlay_batch: OverlayBatchState,
    pub(super) overlay_metadata_error: Mutex<Option<Arc<str>>>,
    pub(super) overlay_metadata_degraded: AtomicBool,
}

impl LocalDirtyState {
    pub(super) fn new() -> Self {
        Self {
            pending_payloads: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::LocalDirty,
            ),
            pending_payload_indexes: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::LocalDirty,
            ),
            pending_chunk_slices: OrderedMutex::new(
                std::collections::HashMap::new(),
                LockTier::LocalDirty,
            ),
            pending_bytes: AtomicUsize::new(0),
            pending_persisted_slice_bytes: AtomicUsize::new(0),
            pending_slice_count: AtomicUsize::new(0),
            pending_persisted_slice_count: AtomicUsize::new(0),
            write_backpressure_wait_ms: AtomicU64::new(0),
            write_backpressure_events: AtomicU64::new(0),
            fast_backpressure_wait_ms: AtomicU64::new(0),
            fast_backpressure_events: AtomicU64::new(0),
            fast_backpressure_stall_max_ms: AtomicU64::new(0),
            hard_backpressure_events: AtomicU64::new(0),
            local_sync_unsynced_bytes: AtomicU64::new(0),
            local_sync_group: tokio::sync::Mutex::new(LocalSyncGroupState {
                pending_bytes: 0,
                last_sync_at: Instant::now(),
            }),
            local_sync_gate: tokio::sync::Mutex::new(()),
            overlay_batch: OverlayBatchState {
                inner: Mutex::new(OverlayBatchInner {
                    next_sequence: 1,
                    ..OverlayBatchInner::default()
                }),
                admission: Arc::new(tokio::sync::Semaphore::new(
                    OVERLAY_BATCH_MAX_QUEUED_OPERATIONS,
                )),
                space_available: tokio::sync::Notify::new(),
                runtime: std::sync::OnceLock::new(),
            },
            overlay_metadata_error: Mutex::new(None),
            overlay_metadata_degraded: AtomicBool::new(false),
        }
    }

    /// Retain the first overlay commit failure and reject later volatile admissions.
    pub(super) fn mark_overlay_metadata_error(&self, error: String) {
        let mut retained = self.overlay_metadata_error.lock();
        if retained.is_none() {
            *retained = Some(error.into());
        }
        self.overlay_metadata_degraded
            .store(true, Ordering::Release);
    }
}

impl StudioVfs {
    pub(super) fn register_overlay_batch_runtime(&self, runtime: tokio::runtime::Handle) {
        let _ = self.state.local_dirty.overlay_batch.runtime.set(runtime);
    }

    /// Return the sticky background overlay failure, if the worker is degraded.
    pub(super) fn ensure_volatile_overlay_metadata_healthy(&self) -> VfsResult<()> {
        if !self
            .state
            .local_dirty
            .overlay_metadata_degraded
            .load(Ordering::Acquire)
        {
            return Ok(());
        }
        let error = self.state.local_dirty.overlay_metadata_error.lock().clone();
        if let Some(error) = error {
            return Err(VfsError::Other(format!(
                "local metadata commit is degraded: {error}"
            )));
        }
        Ok(())
    }
}

impl StudioVfs {
    /// Wait until every overlay mutation admitted before this call has completed successfully.
    pub(super) async fn wait_for_volatile_overlay_batches(&self) -> VfsResult<()> {
        let cutoff = {
            let mut inner = self.state.local_dirty.overlay_batch.inner.lock();
            inner.forced_barriers = inner.forced_barriers.saturating_add(1);
            inner
                .last_admitted_sequence
                .unwrap_or(inner.completed_sequence)
        };
        loop {
            let notified = self
                .state
                .local_dirty
                .overlay_batch
                .space_available
                .notified();
            let complete = {
                let inner = self.state.local_dirty.overlay_batch.inner.lock();
                inner.completed_sequence >= cutoff
            };
            if complete {
                return self.ensure_volatile_overlay_metadata_healthy();
            }
            self.ensure_volatile_overlay_metadata_healthy()?;
            notified.await;
        }
    }

    /// Snapshot shared overlay batching counters and queue gauges.
    pub(crate) fn overlay_batch_observability(&self) -> OverlayBatchObservability {
        let inner = self.state.local_dirty.overlay_batch.inner.lock();
        let dirty_age_ms = inner
            .queue
            .front()
            .map(|queued| queued.write_path_started_at.elapsed().as_millis() as u64)
            .unwrap_or(0);
        OverlayBatchObservability {
            enqueued: inner.enqueued,
            coalesced: inner.coalesced,
            queue_depth: inner.queue.len() as u64,
            queue_bytes: inner.queued_descriptor_bytes as u64,
            dirty_age_ms,
            batches: inner.batches,
            requested_operations: inner.requested_operations,
            actual_operations: inner.actual_operations,
            forced_barriers: inner.forced_barriers,
        }
    }

    /// Admit one regular-file overlay mutation to the bounded shared volatile worker.
    pub(super) async fn commit_regular_file_overlay_batch(
        &self,
        mutation: studiofs_meta::RegularFileOverlayMutation,
        wait_for_completion: bool,
    ) -> VfsResult<Option<InodeAttr>> {
        let admitted_attr = mutation.attr.clone();
        if self.shutdown_requested()
            && !self
                .state
                .shutdown
                .shutdown_final_drain_active
                .load(Ordering::Acquire)
        {
            return Err(VfsError::Other(
                "overlay metadata queue is stopping for shutdown".to_string(),
            ));
        }
        self.ensure_volatile_overlay_metadata_healthy()?;
        let descriptor_bytes = overlay_mutation_descriptor_bytes(&mutation);
        if descriptor_bytes > OVERLAY_BATCH_MAX_QUEUED_DESCRIPTOR_BYTES {
            return Err(VfsError::Other(format!(
                "overlay metadata mutation ({descriptor_bytes} bytes) exceeds the {} byte queue limit",
                OVERLAY_BATCH_MAX_QUEUED_DESCRIPTOR_BYTES
            )));
        }
        let admission_semaphore = self.state.local_dirty.overlay_batch.admission.clone();
        let mut admission = None;
        let (completion, receiver) = tokio::sync::oneshot::channel();
        let mut mutation = Some(mutation);
        let mut completion = Some(completion);
        let is_drainer = loop {
            if admission.is_none() {
                admission = Some(admission_semaphore.clone().acquire_owned().await.map_err(
                    |_| VfsError::Other("overlay metadata queue is closed".to_string()),
                )?);
            }
            let notified = self
                .state
                .local_dirty
                .overlay_batch
                .space_available
                .notified();
            let admitted = {
                let mut inner = self.state.local_dirty.overlay_batch.inner.lock();
                if self.shutdown_requested()
                    && !self
                        .state
                        .shutdown
                        .shutdown_final_drain_active
                        .load(Ordering::Acquire)
                {
                    return Err(VfsError::Other(
                        "overlay metadata queue is stopping for shutdown".to_string(),
                    ));
                }
                self.ensure_volatile_overlay_metadata_healthy()?;
                if inner
                    .queued_descriptor_bytes
                    .saturating_add(descriptor_bytes)
                    > OVERLAY_BATCH_MAX_QUEUED_DESCRIPTOR_BYTES
                {
                    None
                } else {
                    let sequence = inner.next_sequence;
                    let next_sequence = inner.next_sequence.checked_add(1).ok_or_else(|| {
                        VfsError::Other("overlay batch sequence exhausted".to_string())
                    })?;
                    let queued_mutation = mutation.take().ok_or_else(|| {
                        VfsError::Other("overlay mutation was already admitted".to_string())
                    })?;
                    let queued_completion = completion.take().ok_or_else(|| {
                        VfsError::Other("overlay completion was already queued".to_string())
                    })?;
                    let queued_admission = admission.take().ok_or_else(|| {
                        VfsError::Other("overlay queue admission was already consumed".to_string())
                    })?;
                    inner.next_sequence = next_sequence;
                    inner.last_admitted_sequence = Some(sequence);
                    inner.enqueued = inner.enqueued.saturating_add(1);
                    inner.queued_descriptor_bytes = inner
                        .queued_descriptor_bytes
                        .saturating_add(descriptor_bytes);
                    inner.queue.push_back(QueuedOverlayMutation {
                        sequence,
                        mutation: queued_mutation,
                        completion: queued_completion,
                        write_path_started_at: self.write_path_started_at,
                        descriptor_bytes,
                        admission: queued_admission,
                    });
                    Some(if inner.draining {
                        false
                    } else {
                        inner.draining = true;
                        true
                    })
                }
            };
            if let Some(is_drainer) = admitted {
                break is_drainer;
            }
            drop(admission.take());
            notified.await;
        };

        if is_drainer {
            let state = Arc::clone(&self.state);
            let meta = self.meta.clone();
            let runtime = state.local_dirty.overlay_batch.runtime.get().cloned();
            let drainer = async move {
                drain_regular_file_overlay_batches(state, meta).await;
            };
            if let Some(runtime) = runtime {
                runtime.spawn(drainer);
            } else {
                tokio::spawn(drainer);
            }
        }

        if !wait_for_completion {
            return Ok(admitted_attr);
        }
        receiver
            .await
            .map_err(|_| VfsError::Other("overlay batch drainer stopped".to_string()))?
            .map_err(|error| clone_overlay_batch_error(&error))
    }
}

fn clone_overlay_batch_error(error: &VfsError) -> VfsError {
    match error {
        VfsError::Meta(error) => VfsError::Meta((*error).clone()),
        VfsError::Io(error) => VfsError::Io(clone_io_error(error)),
        VfsError::Data(error) => {
            VfsError::Data(studiofs_data::store::DataError::Storage(error.to_string()))
        }
        VfsError::Object(error) => VfsError::Object(clone_object_error(error)),
        VfsError::BlockNotFound(ino, block) => VfsError::BlockNotFound(*ino, *block),
        VfsError::WouldBlock => VfsError::WouldBlock,
        VfsError::RemoteContentPending => VfsError::RemoteContentPending,
        VfsError::InvalidName(name) => VfsError::InvalidName(name.clone()),
        VfsError::Other(message) => VfsError::Other(message.clone()),
        _ => VfsError::Other(error.to_string()),
    }
}

fn clone_object_error(
    error: &studiofs_data::object::ObjectError,
) -> studiofs_data::object::ObjectError {
    use studiofs_data::object::ObjectError;
    match error {
        ObjectError::InvalidMagic => ObjectError::InvalidMagic,
        ObjectError::UnsupportedVersion(version) => ObjectError::UnsupportedVersion(*version),
        ObjectError::Io(error) => ObjectError::Io(clone_io_error(error)),
        ObjectError::LegacyCompressionUnsupported => ObjectError::LegacyCompressionUnsupported,
        ObjectError::ChecksumMismatch { expected, computed } => ObjectError::ChecksumMismatch {
            expected: *expected,
            computed: *computed,
        },
        ObjectError::ObjectTooLarge(size) => ObjectError::ObjectTooLarge(*size),
        ObjectError::InvalidObjectId(value) => ObjectError::InvalidObjectId(value.clone()),
        ObjectError::InvalidFragmentLayout(value) => {
            ObjectError::InvalidFragmentLayout(value.clone())
        }
    }
}

fn clone_io_error(error: &std::io::Error) -> std::io::Error {
    error.raw_os_error().map_or_else(
        || std::io::Error::new(error.kind(), error.to_string()),
        std::io::Error::from_raw_os_error,
    )
}

fn overlay_mutation_descriptor_bytes(
    mutation: &studiofs_meta::RegularFileOverlayMutation,
) -> usize {
    std::mem::size_of_val(mutation)
        .saturating_add(
            mutation
                .chunk_slices
                .capacity()
                .saturating_mul(std::mem::size_of::<(
                    studiofs_meta::schema::ChunkId,
                    Vec<studiofs_meta::schema::SliceRecord>,
                )>()),
        )
        .saturating_add(
            mutation
                .chunk_slices
                .iter()
                .map(|(_, slices)| {
                    slices
                        .capacity()
                        .saturating_mul(std::mem::size_of::<studiofs_meta::schema::SliceRecord>())
                })
                .sum::<usize>(),
        )
}

struct OverlayBatchDrainingGuard {
    state: Arc<VfsState>,
    armed: bool,
}

impl OverlayBatchDrainingGuard {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OverlayBatchDrainingGuard {
    fn drop(&mut self) {
        if self.armed {
            let error = Arc::new(VfsError::Other("overlay batch drainer stopped".to_string()));
            self.state
                .local_dirty
                .mark_overlay_metadata_error(error.to_string());
            fail_queued_overlay_mutations(&self.state, &error);
        }
    }
}

async fn drain_regular_file_overlay_batches(state: Arc<VfsState>, meta: MetaExecutor<dyn VfsMeta>) {
    let mut draining_guard = OverlayBatchDrainingGuard {
        state: Arc::clone(&state),
        armed: true,
    };
    tokio::time::sleep(OVERLAY_BATCH_COALESCE_DELAY).await;

    loop {
        let mut mutations: Vec<studiofs_meta::RegularFileOverlayMutation> =
            Vec::with_capacity(OVERLAY_BATCH_MAX_OPERATIONS);
        let mut completions: Vec<OverlayCompletionList> =
            Vec::with_capacity(OVERLAY_BATCH_MAX_OPERATIONS);
        let mut admissions = Vec::with_capacity(OVERLAY_BATCH_MAX_OPERATIONS);
        let mut last_sequence = None;
        let mut write_path_started_at = None;
        let mut admitted_operations = 0;
        let mut coalesced_operations = 0;
        {
            let mut inner = state.local_dirty.overlay_batch.inner.lock();
            while let Some(queued) = inner.queue.pop_front() {
                inner.queued_descriptor_bytes = inner
                    .queued_descriptor_bytes
                    .saturating_sub(queued.descriptor_bytes);
                let can_merge = mutations.last().is_some_and(|mutation| {
                    mutation.ino == queued.mutation.ino
                        && compatible_overlay_epochs(mutation, &queued.mutation)
                });
                if can_merge {
                    let index = mutations.len() - 1;
                    merge_overlay_mutations(&mut mutations[index], queued.mutation);
                    completions[index].push(queued.completion);
                    coalesced_operations += 1;
                } else {
                    mutations.push(queued.mutation);
                    let mut mutation_completions = OverlayCompletionList::new();
                    mutation_completions.push(queued.completion);
                    completions.push(mutation_completions);
                }
                last_sequence = Some(queued.sequence);
                admitted_operations += 1;
                write_path_started_at.get_or_insert(queued.write_path_started_at);
                admissions.push(queued.admission);
                if admitted_operations == OVERLAY_BATCH_MAX_OPERATIONS {
                    break;
                }
            }
            if mutations.is_empty() {
                inner.draining = false;
                draining_guard.disarm();
                state
                    .local_dirty
                    .overlay_batch
                    .space_available
                    .notify_waiters();
                return;
            }
        }
        for mutation in &mut mutations {
            mutation
                .chunk_slices
                .sort_unstable_by_key(|(chunk_id, _)| *chunk_id);
        }
        if coalesced_operations > 0 {
            let mut inner = state.local_dirty.overlay_batch.inner.lock();
            inner.coalesced = inner.coalesced.saturating_add(coalesced_operations as u64);
        }
        state
            .local_dirty
            .overlay_batch
            .space_available
            .notify_waiters();

        let started = Instant::now();
        let actual_operations = mutations.len();
        let result = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.apply_regular_file_overlay_batch(&mutations)
        })
        .await;
        {
            let mut inner = state.local_dirty.overlay_batch.inner.lock();
            inner.batches = inner.batches.saturating_add(1);
            inner.requested_operations = inner
                .requested_operations
                .saturating_add(admitted_operations as u64);
            inner.actual_operations = inner
                .actual_operations
                .saturating_add(actual_operations as u64);
        }
        if let Some(write_path_started_at) = write_path_started_at {
            record_overlay_batch_observability(
                &state,
                write_path_started_at,
                started.elapsed(),
                result.is_ok(),
            );
        }

        match result {
            Ok(attrs) if attrs.len() == completions.len() => {
                for (waiters, attr) in completions.into_iter().zip(attrs) {
                    for completion in waiters {
                        let _ = completion.send(Ok(attr.clone()));
                    }
                }
            }
            Ok(_) => {
                let error = VfsError::Other(
                    "overlay batch returned an unexpected result count".to_string(),
                );
                state
                    .local_dirty
                    .mark_overlay_metadata_error(error.to_string());
                let error = Arc::new(error);
                for waiters in completions {
                    for completion in waiters {
                        let _ = completion.send(Err(Arc::clone(&error)));
                    }
                }
                if let Some(sequence) = last_sequence {
                    mark_overlay_sequence_complete(&state, sequence);
                }
                fail_queued_overlay_mutations(&state, &error);
                draining_guard.disarm();
                return;
            }
            Err(error) => {
                state
                    .local_dirty
                    .mark_overlay_metadata_error(error.to_string());
                let error = Arc::new(error);
                for waiters in completions {
                    for completion in waiters {
                        let _ = completion.send(Err(Arc::clone(&error)));
                    }
                }
                if let Some(sequence) = last_sequence {
                    mark_overlay_sequence_complete(&state, sequence);
                }
                fail_queued_overlay_mutations(&state, &error);
                draining_guard.disarm();
                return;
            }
        }
        if let Some(sequence) = last_sequence {
            mark_overlay_sequence_complete(&state, sequence);
        }
    }
}

fn mark_overlay_sequence_complete(state: &Arc<VfsState>, sequence: u128) {
    let mut inner = state.local_dirty.overlay_batch.inner.lock();
    inner.completed_sequence = inner.completed_sequence.max(sequence);
    state
        .local_dirty
        .overlay_batch
        .space_available
        .notify_waiters();
}

fn fail_queued_overlay_mutations(state: &Arc<VfsState>, error: &Arc<VfsError>) {
    let mut inner = state.local_dirty.overlay_batch.inner.lock();
    inner.draining = false;
    while let Some(queued) = inner.queue.pop_front() {
        inner.queued_descriptor_bytes = inner
            .queued_descriptor_bytes
            .saturating_sub(queued.descriptor_bytes);
        inner.completed_sequence = inner.completed_sequence.max(queued.sequence);
        let _ = queued.completion.send(Err(Arc::clone(error)));
    }
    state
        .local_dirty
        .overlay_batch
        .space_available
        .notify_waiters();
}

fn merge_overlay_mutations(
    target: &mut studiofs_meta::RegularFileOverlayMutation,
    incoming: studiofs_meta::RegularFileOverlayMutation,
) {
    for (chunk_id, slices) in incoming.chunk_slices {
        if let Some((_, target_slices)) = target
            .chunk_slices
            .iter_mut()
            .find(|(target_chunk_id, _)| *target_chunk_id == chunk_id)
        {
            target_slices.extend(slices);
        } else {
            target.chunk_slices.push((chunk_id, slices));
        }
    }
    if incoming.attr.is_some() {
        target.attr = incoming.attr;
    }
}

fn compatible_overlay_epochs(
    left: &studiofs_meta::RegularFileOverlayMutation,
    right: &studiofs_meta::RegularFileOverlayMutation,
) -> bool {
    match (left.attr.as_ref(), right.attr.as_ref()) {
        (Some(left), Some(right)) => left.content_epoch == right.content_epoch,
        _ => true,
    }
}

fn record_overlay_batch_observability(
    state: &VfsState,
    write_path_started_at: Instant,
    elapsed: Duration,
    completed_commit: bool,
) {
    let elapsed_ns = StudioVfs::duration_ns_u64(elapsed);
    let metrics = &state.metrics;
    StudioVfs::observe_latency_counter(
        &metrics.metadata_commit_calls,
        &metrics.metadata_commit_total_ns,
        &metrics.metadata_commit_max_ns,
        &metrics.metadata_commit_histogram,
        elapsed_ns,
    );
    if !completed_commit {
        return;
    }
    metrics
        .metadata_commit_completed_calls
        .fetch_add(1, Ordering::Relaxed);
    let completed_ns = StudioVfs::duration_ns_u64(write_path_started_at.elapsed());
    let previous_completed_ns = metrics
        .metadata_commit_last_completed_ns
        .swap(completed_ns, Ordering::Relaxed);
    if previous_completed_ns == 0 || completed_ns <= previous_completed_ns {
        return;
    }
    let interval_ns = completed_ns.saturating_sub(previous_completed_ns);
    metrics
        .metadata_commit_interval_total_ns
        .fetch_add(interval_ns, Ordering::Relaxed);
    metrics
        .metadata_commit_interval_max_ns
        .fetch_max(interval_ns, Ordering::Relaxed);
    metrics
        .metadata_commit_interval_histogram
        .observe_ns(interval_ns);
}

impl<'a> LocalDirtyFlushSession<'a> {
    fn pending_slices_visible_end(
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) -> u64 {
        pending_slices
            .iter()
            .flat_map(|(chunk_id, slices)| {
                let chunk_start = studiofs_meta::schema::chunk_start_offset(
                    studiofs_meta::schema::ChunkId(*chunk_id),
                    studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
                );
                slices
                    .iter()
                    .map(move |slice| chunk_start.saturating_add(u64::from(slice.range.end)))
            })
            .max()
            .unwrap_or(0)
    }

    fn pending_slice_object_ids(
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
    ) -> Vec<u64> {
        pending_slices
            .values()
            .flat_map(|slices| slices.iter().filter_map(|slice| slice.object_id))
            .collect()
    }

    async fn cleanup_stale_pending_slice_objects(
        &self,
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        stage: &'static str,
    ) {
        let object_ids = Self::pending_slice_object_ids(pending_slices);
        if object_ids.is_empty() {
            return;
        }
        if let Err(error) = self.vfs.payload_store_remove_batch_async(&object_ids).await {
            warn!(
                ino = self.ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of stale pending-slice payload-store entries failed"
            );
        }
        if let Err(error) = self.vfs.upload_stage_remove_batch_async(&object_ids).await {
            warn!(
                ino = self.ino,
                stage,
                object_count = object_ids.len(),
                error = %error,
                "local-dirty cleanup of stale pending-slice upload entries failed"
            );
        }
        self.vfs.upload_hot_remove_batch(&object_ids);
    }

    fn attr_for_pending_slice_commit(
        &self,
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
    ) -> VfsResult<Option<InodeAttr>> {
        // A background slice commit can outlive the dirty-attr snapshot that
        // advanced EOF; keep metadata size covering any slices made visible.
        let visible_end = Self::pending_slices_visible_end(pending_slices);
        if visible_end == 0 {
            return Ok(attr_to_persist);
        }

        let had_attr = attr_to_persist.is_some();
        let mut attr = if let Some(attr) = attr_to_persist {
            attr
        } else {
            self.vfs
                .sync_meta()
                .get_attr(self.ino)
                .map_err(VfsError::Meta)?
        };
        if !had_attr && attr.content_epoch > content_epoch {
            return Ok(None);
        }

        let mut changed = false;
        if attr.size < visible_end {
            attr.size = visible_end;
            changed = true;
        }
        if attr.content_epoch < content_epoch {
            attr.content_epoch = content_epoch;
            changed = true;
        }

        if had_attr || changed {
            Ok(Some(attr))
        } else {
            Ok(None)
        }
    }

    fn rebase_pending_slice_commit_attr(
        &self,
        pending_slices: &std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        current_attr: InodeAttr,
    ) -> VfsResult<Option<InodeAttr>> {
        let rebased_epoch = current_attr
            .content_epoch
            .saturating_add(1)
            .max(content_epoch.saturating_add(1));
        let mut attr =
            self.attr_for_pending_slice_commit(pending_slices, rebased_epoch, Some(current_attr))?;
        if let Some(attr) = attr.as_mut() {
            attr.upload_state = if attr.committed_size > 0 || attr.committed_content_epoch > 0 {
                MetaUploadState::Updating
            } else {
                MetaUploadState::Uploading
            };
        }
        Ok(attr)
    }

    fn replace_dirty_attr_if_matches(&self, expected: &InodeAttr, replacement: &InodeAttr) {
        let mut dirty_attrs = self.vfs.state.namespace.dirty_attrs.lock();
        if dirty_attrs
            .get(&self.ino)
            .is_some_and(|current| current == expected)
        {
            dirty_attrs.insert(self.ino, replacement.clone());
        }
    }

    async fn commit_pending_slice_snapshot(
        &self,
        pending_slices: std::collections::BTreeMap<u64, Vec<PendingDirtySlice>>,
        content_epoch: u64,
        attr_to_persist: Option<InodeAttr>,
    ) -> VfsResult<()> {
        if pending_slices.is_empty() {
            return Ok(());
        }

        let mut content_epoch = content_epoch;
        let mut attr_to_persist = attr_to_persist;
        let mut rebased_dirty_attr = None;
        match self.vfs.sync_meta().get_attr(self.ino) {
            Ok(current_attr)
                if current_attr.content_epoch == content_epoch
                    && current_attr.upload_state == MetaUploadState::Ready
                    && current_attr.committed_content_epoch == current_attr.content_epoch
                    && current_attr.committed_size == current_attr.size
                    && attr_to_persist
                        .as_ref()
                        .is_some_and(|attr| attr.upload_state != MetaUploadState::Ready) =>
            {
                // Content epochs are allocated independently by each client.
                // A peer can therefore publish the same numeric epoch while
                // this client still owns an accepted payload for that epoch.
                // Rebase the local overlay so the Ready peer snapshot cannot
                // make the pending payload look stale and discard it.
                let rebased_epoch = current_attr.content_epoch.saturating_add(1);
                content_epoch = rebased_epoch;
                if let Some(attr) = attr_to_persist.as_mut() {
                    rebased_dirty_attr = Some(attr.clone());
                    attr.size = attr.size.max(current_attr.size);
                    attr.content_epoch = rebased_epoch;
                    attr.committed_size = current_attr.committed_size;
                    attr.committed_content_epoch = current_attr.committed_content_epoch;
                    attr.upload_state = if current_attr.committed_size > 0
                        || current_attr.committed_content_epoch > 0
                    {
                        MetaUploadState::Updating
                    } else {
                        MetaUploadState::Uploading
                    };
                }
            }
            Ok(current_attr) if current_attr.content_epoch > content_epoch => {
                let visible_end = Self::pending_slices_visible_end(&pending_slices);
                if visible_end > 0
                    && (current_attr.size >= visible_end
                        || current_attr.upload_state == MetaUploadState::Updating)
                {
                    // An in-flight update of an already-published file may have
                    // temporarily reduced the visible EOF. A locally accepted
                    // sparse write beyond that EOF still belongs to the newer
                    // local view and must be rebased instead of discarded.
                    content_epoch = current_attr.content_epoch;
                    attr_to_persist = Some(current_attr);
                } else {
                    self.cleanup_stale_pending_slice_objects(&pending_slices, "metadata-precheck")
                        .await;
                    self.vfs
                        .commit_pending_local_dirty_slices(self.ino, &pending_slices);
                    return Ok(());
                }
            }
            Ok(_) => {}
            Err(studiofs_meta::MetaError::InodeNotFound(_)) => {
                self.cleanup_stale_pending_slice_objects(&pending_slices, "metadata-precheck")
                    .await;
                self.vfs
                    .commit_pending_local_dirty_slices(self.ino, &pending_slices);
                return Ok(());
            }
            Err(error) => return Err(VfsError::Meta(error)),
        }

        let chunk_slices =
            self.drain
                .build_overlay_slice_records(self.ino, &[], pending_slices.clone())?;
        let mut attr_to_clear =
            self.attr_for_pending_slice_commit(&pending_slices, content_epoch, attr_to_persist)?;
        // These slices are submitted for upload below, so redb must commit first.
        let mut committed_attr = self
            .drain
            .commit_meta
            .commit_overlay(self.ino, chunk_slices.clone(), attr_to_clear.clone(), true)
            .await?;

        if attr_to_clear.is_some() && committed_attr.is_none() {
            // The metadata snapshot can become stale after the precheck but
            // before the overlay transaction starts. Rebase the same local
            // payloads onto the current inode view and retry so a concurrent
            // publication cannot make an accepted sparse write disappear.
            for _ in 0..3 {
                let current_attr = match self.vfs.sync_meta().get_attr(self.ino) {
                    Ok(attr) => attr,
                    Err(studiofs_meta::MetaError::InodeNotFound(_)) => {
                        self.cleanup_stale_pending_slice_objects(
                            &pending_slices,
                            "metadata-retry-inode-missing",
                        )
                        .await;
                        self.vfs
                            .commit_pending_local_dirty_slices(self.ino, &pending_slices);
                        return Ok(());
                    }
                    Err(error) => return Err(VfsError::Meta(error)),
                };
                attr_to_clear = self.rebase_pending_slice_commit_attr(
                    &pending_slices,
                    content_epoch,
                    current_attr,
                )?;
                content_epoch = attr_to_clear
                    .as_ref()
                    .map(|attr| attr.content_epoch)
                    .unwrap_or(content_epoch);
                committed_attr = self
                    .drain
                    .commit_meta
                    .commit_overlay(self.ino, chunk_slices.clone(), attr_to_clear.clone(), true)
                    .await?;
                if committed_attr.is_some() {
                    break;
                }
            }
        }
        let skipped_stale_commit = attr_to_clear.is_some() && committed_attr.is_none();
        self.drain
            .observer
            .on_metadata_committed(self.ino, committed_attr.as_ref());
        if skipped_stale_commit {
            // Keep the payload and pending-slice entry for a later flush if a
            // concurrent metadata writer wins all bounded retry attempts.
            warn!(
                ino = self.ino,
                "local-dirty pending-slice metadata remained stale after retries; retaining payload"
            );
            return Err(VfsError::WouldBlock);
        }

        let mut upload_ops_by_object = std::collections::BTreeMap::<u64, (u64, u64, u64)>::new();
        for (chunk_id, slices) in &pending_slices {
            let chunk_start = studiofs_meta::schema::chunk_start_offset(
                studiofs_meta::schema::ChunkId(*chunk_id),
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            for slice in slices {
                let Some(object_id) = slice.object_id else {
                    continue;
                };
                let file_offset = chunk_start.saturating_add(slice.range.start as u64);
                let object_file_offset = file_offset.saturating_sub(u64::from(slice.object_offset));
                let payload_end = file_offset.saturating_add(slice.len() as u64);
                upload_ops_by_object
                    .entry(object_id)
                    .and_modify(|(offset, start, end)| {
                        *offset = (*offset).min(file_offset);
                        *start = (*start).min(object_file_offset);
                        *end = (*end).max(payload_end);
                    })
                    .or_insert((file_offset, object_file_offset, payload_end));
            }
        }
        let upload_ops: Vec<_> = upload_ops_by_object
            .iter()
            .map(|(object_id, (offset, object_file_offset, payload_end))| {
                (
                    self.ino,
                    *offset,
                    *object_file_offset,
                    *object_id,
                    content_epoch,
                    payload_end.saturating_sub(*object_file_offset),
                )
            })
            .collect();
        if !upload_ops.is_empty() {
            let mut latest_live_extents = std::collections::BTreeMap::<u32, (u64, u64)>::new();
            for slices in pending_slices.values() {
                for slice in slices {
                    let Some(object_id) = slice.object_id else {
                        continue;
                    };
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
            // submit_uploads_batch pins payloads before the uploader creates
            // stage hardlinks, so cache eviction cannot remove them in between.
            self.vfs.cached_store.submit_uploads_batch(&upload_ops);
            self.vfs.state.file_session.note_background_transition(
                self.ino,
                FileSessionOperation::Upload,
                "queued",
            );
        }

        self.vfs
            .commit_pending_local_dirty_slices(self.ino, &pending_slices);
        if let (Some(expected), Some(replacement)) =
            (rebased_dirty_attr.as_ref(), attr_to_clear.as_ref())
        {
            self.replace_dirty_attr_if_matches(expected, replacement);
        }
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

    async fn flush(
        &self,
        local_sync_policy: LocalSyncPolicy,
        count_foreground_barrier: bool,
    ) -> VfsResult<()> {
        if count_foreground_barrier {
            self.vfs
                .state
                .metrics
                .flush_barrier_calls
                .fetch_add(1, Ordering::Relaxed);
        }
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
            if self
                .vfs
                .discard_stale_zero_size_dirty_attr_after_current_publish(self.ino, &attr)
                .await?
            {
                return Ok(());
            }
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

        let spill_bytes_cap = self.writer_stack_steady_drain_quota_bytes().min(
            studiofs_data::object::CLOUD_FRAGMENT_SIZE
                * studiofs_data::object::CLOUD_FRAGMENT_COUNT,
        );
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

    async fn persist_buffered_write_blocks_as_local_extents(
        &self,
        ino: Ino,
        blocks: Vec<DirtyWriteSnapshot>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let object_count = u32::try_from(blocks.len()).map_err(|_| {
            VfsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "requested buffered-write local extent count {} exceeds u32::MAX",
                    blocks.len()
                ),
            ))
        })?;
        let object_ids = self
            .local_dirty_drain()
            .allocate_object_ids(object_count)
            .await?;
        let mut local_extents = Vec::with_capacity(blocks.len());
        let payloads = blocks
            .into_iter()
            .zip(object_ids.iter().copied())
            .map(|(block, object_id)| {
                let block_index = block.block_index;
                let len = block.len();
                local_extents.push((block_index, object_id, len));
                (object_id, block.data.freeze())
            })
            .collect::<Vec<_>>();
        self.persist_local_object_payloads(payloads, local_extents.len(), local_sync_policy)
            .await?;
        for (block_index, object_id, len) in local_extents {
            self.replace_pending_local_dirty_payload(
                ino,
                block_index,
                PendingPayload::local_extent(object_id, len),
            );
            self.clear_pending_dirty_slices_for_block(ino, block_index);
        }
        Ok(())
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
            if self.disable_background_slab_flush {
                let (completed_blocks, buffered_blocks): (Vec<_>, Vec<_>) = blocks
                    .into_iter()
                    .partition(|block| block.len() == self.block_size as usize);
                self.persist_buffered_write_blocks_as_local_extents(
                    ino,
                    completed_blocks,
                    local_sync_policy,
                )
                .await?;
                if !buffered_blocks.is_empty() {
                    self.stage_dirty_write_snapshots(ino, buffered_blocks);
                }
                return Ok(());
            }
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

    fn append_unique_dirty_inodes(
        ordered: &mut Vec<Ino>,
        seen: &mut BTreeSet<Ino>,
        inodes: impl IntoIterator<Item = Ino>,
    ) {
        for ino in inodes {
            if seen.insert(ino) {
                ordered.push(ino);
            }
        }
    }

    fn shutdown_dirty_inodes_by_priority(&self) -> Vec<Ino> {
        let mut ordered = Vec::new();
        let mut seen = BTreeSet::new();
        Self::append_unique_dirty_inodes(
            &mut ordered,
            &mut seen,
            self.snapshot_pending_local_dirty_inodes(),
        );
        Self::append_unique_dirty_inodes(
            &mut ordered,
            &mut seen,
            self.state.writer_stack.snapshot_dirty_inodes_by_bytes(),
        );
        {
            let dirty_attrs = self.state.namespace.dirty_attrs.lock();
            Self::append_unique_dirty_inodes(&mut ordered, &mut seen, dirty_attrs.keys().copied());
        }
        ordered
    }

    pub(super) fn total_pending_persisted_local_dirty_slice_bytes(&self) -> usize {
        self.state
            .local_dirty
            .pending_persisted_slice_bytes
            .load(Ordering::Relaxed)
    }

    pub(super) fn pending_local_dirty_slice_counts(&self) -> (usize, usize) {
        (
            self.state
                .local_dirty
                .pending_slice_count
                .load(Ordering::Relaxed),
            self.state
                .local_dirty
                .pending_persisted_slice_count
                .load(Ordering::Relaxed),
        )
    }

    pub(super) fn total_landed_write_drain_lag_bytes(&self) -> usize {
        self.state
            .writer_stack
            .total_dirty_bytes()
            .saturating_add(self.total_pending_local_dirty_bytes())
            .saturating_add(self.total_pending_persisted_local_dirty_slice_bytes())
    }

    fn pending_slice_commit_wave_bytes(&self) -> usize {
        (studiofs_meta::schema::DEFAULT_CHUNK_SIZE as usize)
            .max(self.block_size as usize)
            .max(1)
    }

    fn pending_slice_commit_hot_tail_bytes(&self) -> usize {
        self.writer_stack_steady_drain_quota_bytes()
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

        let has_dirty_tail = self.state.writer_stack.dirty_bytes_for_inode(ino) > 0;
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
        let upload_pipeline_idle =
            self.cached_store.pending_upload_count() == 0 && self.upload_scheduler.in_flight() == 0;
        if upload_pipeline_idle && oldest_age.is_some_and(|age| age >= PENDING_SLICE_COMMIT_MAX_AGE)
        {
            return pending_slices;
        }

        let mut ordered_slices = Vec::<(u64, PendingDirtySlice)>::new();
        for (chunk_id, slices) in &pending_slices {
            for slice in slices {
                ordered_slices.push((*chunk_id, *slice));
            }
        }
        ordered_slices.sort_by_key(|(chunk_id, slice)| slice.order_key(*chunk_id));

        let mut object_group_indices = std::collections::HashMap::<u64, usize>::new();
        let mut ordered_object_groups = Vec::<Vec<(u64, PendingDirtySlice)>>::new();
        for (chunk_id, slice) in ordered_slices.iter().copied() {
            let object_id = slice
                .object_id
                .expect("persisted pending slices must retain their object id");
            let group_index = *object_group_indices.entry(object_id).or_insert_with(|| {
                ordered_object_groups.push(Vec::new());
                ordered_object_groups.len() - 1
            });
            ordered_object_groups[group_index].push((chunk_id, slice));
        }

        let now = Instant::now();
        let mut retain_tail_bytes = self
            .pending_slice_commit_hot_tail_bytes()
            .min(total_bytes.saturating_sub(commit_wave_bytes));
        let mut commit_candidate_groups = Vec::<&Vec<(u64, PendingDirtySlice)>>::new();
        for group in ordered_object_groups.iter().rev() {
            let group_len = group.iter().map(|(_, slice)| slice.len()).sum::<usize>();
            let protect_fresh_tail = retain_tail_bytes > 0
                && group
                    .iter()
                    .any(|(_, slice)| slice.age(now) < PENDING_SLICE_COMMIT_MAX_AGE);
            if !protect_fresh_tail {
                commit_candidate_groups.push(group);
                continue;
            }
            retain_tail_bytes = retain_tail_bytes.saturating_sub(group_len);
        }
        commit_candidate_groups.reverse();

        let mut selected = std::collections::BTreeMap::<u64, Vec<PendingDirtySlice>>::new();
        let mut selected_bytes = 0usize;
        for group in commit_candidate_groups {
            if selected_bytes >= commit_wave_bytes {
                break;
            }
            for (chunk_id, slice) in group.iter().copied() {
                selected.entry(chunk_id).or_default().push(slice);
                selected_bytes = selected_bytes.saturating_add(slice.len());
            }
        }
        if selected.is_empty()
            && oldest_age.is_some_and(|age| age >= PENDING_SLICE_COMMIT_MAX_AGE)
            && let Some(group) = ordered_object_groups.first()
        {
            for (chunk_id, slice) in group.iter().copied() {
                selected.entry(chunk_id).or_default().push(slice);
            }
        }

        for (chunk_id, slices) in &mut selected {
            slices.sort_by_key(|slice| slice.order_key(*chunk_id));
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

        if self.state.writer_stack.dirty_bytes_for_inode(ino) == 0 {
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
        let mut persisted_object_ids: Vec<u64> =
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
            Ok(())
        }
        .await;
        if result.is_ok() {
            let persisted_payload_count = persisted_object_ids.len();
            let object_ids = std::mem::take(&mut persisted_object_ids);
            if let Err(error) = self.upload_stage_link_batch_async(object_ids).await {
                warn!(
                    persisted_payload_count,
                    error = %error,
                    "failed to create zero-copy upload links for persisted local dirty payloads"
                );
            }
        }
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
        let added_bytes = entries
            .iter()
            .filter(|(_, slice)| slice.is_persisted())
            .map(|(_, slice)| slice.len())
            .sum();
        let added_slices = entries.len();
        let added_persisted_slices = entries
            .iter()
            .filter(|(_, slice)| slice.is_persisted())
            .count();
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let inode_chunks = pending_chunk_slices.entry(ino).or_default();
        for (chunk_id, slice) in entries {
            inode_chunks.entry(chunk_id).or_default().push(slice);
        }
        drop(pending_chunk_slices);
        self.add_pending_persisted_slice_bytes(added_bytes);
        self.add_pending_slice_counts(added_slices, added_persisted_slices);
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
        for (chunk_id, slices) in &mut committed_by_chunk {
            slices.sort_by_key(|slice| slice.order_key(*chunk_id));
        }
        let mut removed_bytes = 0usize;
        let mut removed_slices = 0usize;
        let mut removed_persisted_slices = 0usize;
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        let Some(chunks) = pending_chunk_slices.get_mut(&ino) else {
            return;
        };
        chunks.retain(|chunk_id, slices| {
            slices.retain_mut(|slice| {
                if committed_entries.contains(&slice.commit_key(*chunk_id)) {
                    removed_bytes = removed_bytes.saturating_add(slice.len());
                    removed_slices = removed_slices.saturating_add(1);
                    removed_persisted_slices =
                        removed_persisted_slices.saturating_add(usize::from(slice.is_persisted()));
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
                        removed_bytes = removed_bytes.saturating_add(current.len());
                        removed_slices = removed_slices.saturating_add(1);
                        removed_persisted_slices = removed_persisted_slices
                            .saturating_add(usize::from(current.is_persisted()));
                        return false;
                    };
                    removed_bytes = removed_bytes.saturating_add(committed_len as usize);
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
        drop(pending_chunk_slices);
        self.subtract_pending_persisted_slice_bytes(removed_bytes);
        self.subtract_pending_slice_counts(removed_slices, removed_persisted_slices);
    }

    pub(super) fn clear_matching_dirty_attr_if_committed(&self, ino: Ino, attr: &InodeAttr) {
        if self.has_pending_local_dirty_payloads_for_inode(ino) {
            return;
        }
        if self.has_deferred_namespace_metadata(ino) {
            return;
        }
        let mut dirty_attrs = self.state.namespace.dirty_attrs.lock();
        let should_clear = dirty_attrs.get(&ino).is_some_and(|current| {
            let exact_match = current.content_epoch == attr.content_epoch
                && current.size == attr.size
                && current.mtime == attr.mtime
                && current.ctime == attr.ctime;
            let committed_directory_attr_covers_current = current.kind != FileType::RegularFile
                && current.kind == attr.kind
                && current.content_epoch == attr.content_epoch;
            let published_attr_covers_current = current.content_epoch == attr.content_epoch
                && Self::is_fully_published_regular_file(attr)
                && current.size <= attr.size
                && current.mtime <= attr.mtime
                && current.ctime <= attr.ctime;
            exact_match
                || committed_directory_attr_covers_current
                || published_attr_covers_current
                || Self::is_stale_zero_size_dirty_attr_after_publish(current, attr)
        });
        if should_clear {
            dirty_attrs.remove(&ino);
        }
    }

    pub(super) fn is_stale_zero_size_dirty_attr_after_publish(
        current: &InodeAttr,
        published: &InodeAttr,
    ) -> bool {
        current.kind == FileType::RegularFile
            && published.kind == FileType::RegularFile
            && Self::is_fully_published_regular_file(published)
            && current.upload_state != MetaUploadState::Ready
            && current.size == 0
            && current.committed_size <= published.committed_size
            && (current.committed_content_epoch < published.committed_content_epoch
                || (current.committed_content_epoch == published.committed_content_epoch
                    && current.mtime <= published.mtime
                    && current.ctime <= published.ctime))
    }

    pub(super) async fn discard_stale_zero_size_dirty_attr_after_current_publish(
        &self,
        ino: Ino,
        attr: &InodeAttr,
    ) -> VfsResult<bool> {
        let current_attr = {
            let meta = self.meta.clone();
            run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino)).await?
        };
        if !Self::is_stale_zero_size_dirty_attr_after_publish(attr, &current_attr) {
            return Ok(false);
        }
        self.clear_matching_dirty_attr_if_committed(ino, &current_attr);
        self.invalidate_attr_read_cache([ino]);
        self.refresh_inode_incarnation_state_from_attr(ino, &current_attr);
        Ok(true)
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

    fn add_pending_persisted_slice_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.state
            .local_dirty
            .pending_persisted_slice_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn subtract_pending_persisted_slice_bytes(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let _ = self
            .state
            .local_dirty
            .pending_persisted_slice_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                Some(pending.saturating_sub(bytes))
            });
    }

    fn add_pending_slice_counts(&self, total: usize, persisted: usize) {
        if total > 0 {
            self.state
                .local_dirty
                .pending_slice_count
                .fetch_add(total, Ordering::Relaxed);
        }
        if persisted > 0 {
            self.state
                .local_dirty
                .pending_persisted_slice_count
                .fetch_add(persisted, Ordering::Relaxed);
        }
    }

    fn subtract_pending_slice_counts(&self, total: usize, persisted: usize) {
        if total > 0 {
            let _ = self.state.local_dirty.pending_slice_count.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |pending| {
                    debug_assert!(
                        pending >= total,
                        "pending_slice_count underflow: pending={pending}, subtract={total}"
                    );
                    Some(pending.saturating_sub(total))
                },
            );
        }
        if persisted > 0 {
            let _ = self
                .state
                .local_dirty
                .pending_persisted_slice_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |pending| {
                    debug_assert!(
                        pending >= persisted,
                        "pending_persisted_slice_count underflow: pending={pending}, subtract={persisted}"
                    );
                    Some(pending.saturating_sub(persisted))
                });
        }
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
        if let Some(old) = replaced
            && let Some(buffer) = old.recycle_buffer()
        {
            self.recycle_write_buffer(buffer);
        }
    }

    fn clear_pending_dirty_slices_for_inode(&self, ino: Ino) {
        let (removed_bytes, removed_slices, removed_persisted_slices) = self
            .state
            .local_dirty
            .pending_chunk_slices
            .lock()
            .remove(&ino)
            .map(|chunks| {
                chunks.values().flat_map(|slices| slices.iter()).fold(
                    (0usize, 0usize, 0usize),
                    |(bytes, total, persisted), slice| {
                        (
                            bytes.saturating_add(if slice.is_persisted() {
                                slice.len()
                            } else {
                                0
                            }),
                            total.saturating_add(1),
                            persisted.saturating_add(usize::from(slice.is_persisted())),
                        )
                    },
                )
            })
            .unwrap_or((0, 0, 0));
        self.subtract_pending_persisted_slice_bytes(removed_bytes);
        self.subtract_pending_slice_counts(removed_slices, removed_persisted_slices);
    }

    fn clear_pending_dirty_slices_for_block(&self, ino: Ino, block_index: u32) {
        let mut removed_bytes = 0usize;
        let mut removed_slices = 0usize;
        let mut removed_persisted_slices = 0usize;
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| {
                    let remove = slice.block_index == block_index;
                    if remove {
                        removed_slices = removed_slices.saturating_add(1);
                        if slice.is_persisted() {
                            removed_bytes = removed_bytes.saturating_add(slice.len());
                            removed_persisted_slices = removed_persisted_slices.saturating_add(1);
                        }
                    }
                    !remove
                });
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
        drop(pending_chunk_slices);
        self.subtract_pending_persisted_slice_bytes(removed_bytes);
        self.subtract_pending_slice_counts(removed_slices, removed_persisted_slices);
    }

    fn clear_pending_dirty_slices_for_blocks(&self, ino: Ino, block_indexes: &[u32]) {
        if block_indexes.is_empty() {
            return;
        }
        let block_index_set: std::collections::HashSet<u32> =
            block_indexes.iter().copied().collect();
        let mut removed_bytes = 0usize;
        let mut removed_slices = 0usize;
        let mut removed_persisted_slices = 0usize;
        let mut pending_chunk_slices = self.state.local_dirty.pending_chunk_slices.lock();
        if let Some(chunks) = pending_chunk_slices.get_mut(&ino) {
            chunks.retain(|_, slices| {
                slices.retain(|slice| {
                    let remove = block_index_set.contains(&slice.block_index);
                    if remove {
                        removed_slices = removed_slices.saturating_add(1);
                        if slice.is_persisted() {
                            removed_bytes = removed_bytes.saturating_add(slice.len());
                            removed_persisted_slices = removed_persisted_slices.saturating_add(1);
                        }
                    }
                    !remove
                });
                !slices.is_empty()
            });
            if chunks.is_empty() {
                pending_chunk_slices.remove(&ino);
            }
        }
        drop(pending_chunk_slices);
        self.subtract_pending_persisted_slice_bytes(removed_bytes);
        self.subtract_pending_slice_counts(removed_slices, removed_persisted_slices);
    }

    fn clear_pending_dirty_slices_exceeding_len(&self, ino: Ino, block_index: u32, new_len: usize) {
        let mut removed_bytes = 0usize;
        let mut removed_slices = 0usize;
        let mut removed_persisted_slices = 0usize;
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
                    removed_slices = removed_slices.saturating_add(1);
                    if slice.is_persisted() {
                        removed_bytes = removed_bytes.saturating_add(slice_len);
                        removed_persisted_slices = removed_persisted_slices.saturating_add(1);
                    }
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
                    removed_slices = removed_slices.saturating_add(1);
                    if slice.is_persisted() {
                        removed_persisted_slices = removed_persisted_slices.saturating_add(1);
                    }
                    return false;
                };
                let Some(new_end) = u32::try_from(trimmed.end()).ok() else {
                    removed_slices = removed_slices.saturating_add(1);
                    if slice.is_persisted() {
                        removed_bytes = removed_bytes.saturating_add(slice_len);
                        removed_persisted_slices = removed_persisted_slices.saturating_add(1);
                    }
                    return false;
                };
                if slice.is_persisted() {
                    let new_len = new_end.saturating_sub(slice.range.start) as usize;
                    removed_bytes = removed_bytes.saturating_add(slice_len.saturating_sub(new_len));
                }
                slice.range.end = new_end;
                true
            });
            !slices.is_empty()
        });

        if chunks.is_empty() {
            pending_chunk_slices.remove(&ino);
        }
        drop(pending_chunk_slices);
        self.subtract_pending_persisted_slice_bytes(removed_bytes);
        self.subtract_pending_slice_counts(removed_slices, removed_persisted_slices);
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
        let removed = pending.remove(&(ino, block_index));
        if removed.is_some()
            && let Some(inode_indexes) = indexes.get_mut(&ino)
        {
            inode_indexes.remove(&block_index);
            if inode_indexes.is_empty() {
                indexes.remove(&ino);
            }
        }
        drop(indexes);
        drop(pending);

        if let Some(removed) = removed {
            self.subtract_pending_local_dirty_bytes(removed.memory_len());
            if let Some(buffer) = removed.recycle_buffer() {
                self.recycle_write_buffer(buffer);
            }
        }
        self.clear_pending_dirty_slices_for_block(ino, block_index);
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
                let pending = pending.get(&(ino, *block_index))?;
                if pending.is_local_extent() {
                    let object_id = pending
                        .local_extent_id()
                        .expect("local extent pending payload must have an object id");
                    let data = self
                        .local_payload_backend
                        .get(object_id)
                        .expect("local payload missing for dirty local extent snapshot");
                    Some(DirtyWriteSnapshot::from_shared(*block_index, data))
                } else if pending.dirty {
                    Some(DirtyWriteSnapshot::from_mutable(
                        *block_index,
                        self.clone_into_write_buffer(pending.data.as_ref()),
                    ))
                } else {
                    None
                }
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
            let Some(slice_range) = IntervalRange::new(
                chunk_start.saturating_add(slice.range.start as u64),
                chunk_start.saturating_add(slice.range.end as u64),
            ) else {
                continue;
            };
            let Some(overlap) = slice_range.intersection(requested_range) else {
                continue;
            };

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
            if block.is_local_extent() {
                let object_id = block
                    .local_extent_id()
                    .expect("local extent pending payload must have an object id");
                let data = self
                    .local_payload_backend
                    .get(object_id)
                    .expect("local payload missing during dirty read span resolution");
                let block_start =
                    u64::from(*block_index).saturating_mul(u64::from(self.block_size));
                let block_end = block_start.saturating_add(block.logical_len() as u64);
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
                    data,
                });
                continue;
            }
            if !block.dirty || block.data.is_empty() {
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
        self.wait_for_deferred_namespace_metadata(ino).await?;
        self.local_dirty_flush_session(ino)
            .flush(local_sync_policy, true)
            .await?;
        self.landing_zone.clear_inode(ino).map_err(VfsError::Io)?;
        Ok(())
    }

    pub(super) async fn flush_local_dirty_state_background(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<()> {
        self.wait_for_deferred_namespace_metadata(ino).await?;
        self.local_dirty_flush_session(ino)
            .flush(local_sync_policy, false)
            .await?;
        self.landing_zone.clear_inode(ino).map_err(VfsError::Io)?;
        Ok(())
    }

    pub(super) async fn flush_pending_payloads_for_shutdown(&self) -> VfsResult<()> {
        let inos = self.shutdown_dirty_inodes_by_priority();
        if inos.is_empty() {
            return Ok(());
        }

        info!(
            inode_count = inos.len(),
            "VFS shutdown: flushing dirty inodes"
        );
        let mut first_error = None;
        for ino in inos {
            if let Err(e) = self.flush(ino).await {
                warn!(ino, error = %e, "VFS shutdown: flush inode failed");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
