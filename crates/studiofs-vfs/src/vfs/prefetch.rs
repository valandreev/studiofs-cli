use super::{
    AccessPolicy, Ino, JoinSet, MetaError, Ordering, ReadRequestContext, ReadStreamKey, Semaphore,
    StudioVfs, VecDeque,
};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tracing::info;

#[derive(Debug, Clone)]
pub(super) enum PrefetchWorkItem {
    ReadHint {
        ino: Ino,
        stream_id: u64,
        first_block: u32,
        last_block: u32,
        policy: AccessPolicy,
    },
    Batch(PrefetchBatch),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PrefetchHintRange {
    first_block: u32,
    last_block: u32,
}

pub(super) enum PrefetchHintUpdate {
    Suppress,
    Store(PrefetchHintRange),
}

impl PrefetchHintRange {
    pub(super) fn from_blocks_needed(blocks_needed: &[(u32, usize, usize)]) -> Option<Self> {
        let first_block = blocks_needed.first().map(|entry| entry.0)?;
        let last_block = blocks_needed.last().map(|entry| entry.0)?;
        Some(Self {
            first_block,
            last_block,
        })
    }

    pub(super) fn as_stored(self) -> (u32, u32) {
        (self.first_block, self.last_block)
    }

    pub(super) fn update_against(self, previous: Option<(u32, u32)>) -> PrefetchHintUpdate {
        let Some((prev_first, prev_last)) = previous else {
            return PrefetchHintUpdate::Store(self);
        };
        if self.first_block >= prev_first && self.last_block <= prev_last {
            return PrefetchHintUpdate::Suppress;
        }
        if self.first_block <= prev_last.saturating_add(1)
            && prev_first <= self.last_block.saturating_add(1)
        {
            return PrefetchHintUpdate::Store(Self {
                first_block: prev_first.min(self.first_block),
                last_block: prev_last.max(self.last_block),
            });
        }
        PrefetchHintUpdate::Store(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) struct PrefetchReadContext {
    pub(super) size: u64,
    pub(super) pending_content_epoch: Option<u64>,
}

impl PrefetchReadContext {
    pub(super) fn eof_block(&self, block_size: u32) -> Option<u32> {
        if self.size == 0 {
            None
        } else {
            Some(((self.size.saturating_sub(1)) / block_size.max(1) as u64) as u32)
        }
    }
}

struct PrefetchBatchBuilder<'a> {
    vfs: &'a StudioVfs,
    ino: Ino,
    stream_id: u64,
    first_block: u32,
    last_block: u32,
    policy: AccessPolicy,
}

impl<'a> PrefetchBatchBuilder<'a> {
    fn new(
        vfs: &'a StudioVfs,
        ino: Ino,
        stream_id: u64,
        first_block: u32,
        last_block: u32,
        policy: AccessPolicy,
    ) -> Self {
        Self {
            vfs,
            ino,
            stream_id,
            first_block,
            last_block,
            policy,
        }
    }

    fn plan_batches(&self) -> Vec<PrefetchBatch> {
        if self.vfs.shutdown_requested()
            || !self.policy.prefetch_enabled
            || self.vfs.readahead.width == 0
        {
            return Vec::new();
        }
        let Some(read_context) = self.resolve_read_context() else {
            return Vec::new();
        };
        let (to_prefetch, siblings) = self.record_prefetch_sources();
        if self
            .vfs
            .fetch_scheduler
            .recent_send_error(self.vfs.prefetch_pause_on_neterr)
        {
            return Vec::new();
        }
        let primary_blocks = Self::primary_blocks_within_eof(
            to_prefetch,
            read_context.eof_block(self.vfs.block_size),
        );
        let mut planned = Vec::new();
        if !primary_blocks.is_empty() {
            planned.push(PrefetchBatch::new(
                self.ino,
                primary_blocks,
                Some(read_context),
            ));
        }
        self.append_sibling_batches(&mut planned, siblings);
        planned
    }

    fn resolve_read_context(&self) -> Option<PrefetchReadContext> {
        match self.vfs.sync_meta().get_attr(self.ino) {
            Ok(attr) => Some(self.vfs.prefetch_read_context_from_attr(self.ino, &attr)),
            Err(MetaError::InodeNotFound(_)) => {
                self.clear_stale_stream_state();
                None
            }
            Err(_) => None,
        }
    }

    fn clear_stale_stream_state(&self) {
        self.vfs.readahead.evict(self.ino);
        self.vfs
            .state
            .read
            .clear_prefetch_hint(self.ino, self.stream_id);
    }

    fn record_prefetch_sources(&self) -> (Vec<u32>, Vec<Ino>) {
        let to_prefetch = self.vfs.readahead.record_read_for_stream(
            ReadStreamKey {
                ino: self.ino,
                stream_id: self.stream_id,
            },
            self.first_block,
            self.last_block,
            self.policy.readahead_policy(),
        );
        if let Some(path) = self.vfs.debug_pending_target_path(self.ino) {
            info!(
                ino = self.ino,
                path = %path,
                stream_id = self.stream_id,
                access_class = self.policy.class.as_str(),
                hint_first_block = self.first_block,
                hint_last_block = self.last_block,
                planned_prefetch_blocks = to_prefetch.len(),
                first_prefetch_block = to_prefetch.first().copied().unwrap_or(u32::MAX),
                last_prefetch_block = to_prefetch.last().copied().unwrap_or(u32::MAX),
                "debug target prefetch plan"
            );
        }
        let siblings = self
            .vfs
            .dir_prefetch
            .on_child_read_limited(self.ino, self.vfs.dir_prefetch_max_batch);
        (to_prefetch, siblings)
    }

    fn primary_blocks_within_eof(to_prefetch: Vec<u32>, eof_block: Option<u32>) -> Vec<u32> {
        let mut primary_blocks = Vec::new();
        for object_idx in to_prefetch {
            if let Some(max_block) = eof_block
                && object_idx > max_block
            {
                continue;
            }
            primary_blocks.push(object_idx);
        }
        primary_blocks
    }

    fn append_sibling_batches(&self, planned: &mut Vec<PrefetchBatch>, siblings: Vec<Ino>) {
        for sibling_ino in siblings.into_iter().take(self.vfs.dir_prefetch_max_batch) {
            planned.push(PrefetchBatch::single(sibling_ino, 0, None));
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PrefetchBatch {
    pub(super) ino: Ino,
    pub(super) block_indices: Vec<u32>,
    pub(super) read_context: Option<PrefetchReadContext>,
}

impl PrefetchBatch {
    pub(super) fn single(
        ino: Ino,
        object_idx: u32,
        read_context: Option<PrefetchReadContext>,
    ) -> Self {
        Self {
            ino,
            block_indices: vec![object_idx],
            read_context,
        }
    }

    fn block_count(&self) -> usize {
        self.block_indices.len()
    }

    fn is_empty(&self) -> bool {
        self.block_indices.is_empty()
    }

    pub(super) fn new(
        ino: Ino,
        block_indices: Vec<u32>,
        read_context: Option<PrefetchReadContext>,
    ) -> Self {
        Self {
            ino,
            block_indices,
            read_context,
        }
    }
}

pub(super) struct PrefetchWorker {
    vfs: Arc<StudioVfs>,
    prefetch_rx: UnboundedReceiver<PrefetchWorkItem>,
    prefetch_sem: Arc<Semaphore>,
    pending_queue: VecDeque<PrefetchBatch>,
    pending_set: HashSet<(Ino, u32)>,
    inflight: JoinSet<()>,
}

impl PrefetchWorker {
    pub(super) fn new(
        vfs: Arc<StudioVfs>,
        prefetch_rx: UnboundedReceiver<PrefetchWorkItem>,
    ) -> Self {
        let prefetch_limit = vfs.prefetch_concurrency.max(1);
        Self {
            vfs,
            prefetch_rx,
            prefetch_sem: Arc::new(Semaphore::new(prefetch_limit)),
            pending_queue: VecDeque::new(),
            pending_set: HashSet::new(),
            inflight: JoinSet::new(),
        }
    }

    pub(super) async fn run(mut self) {
        loop {
            self.launch_queued_batches();

            tokio::select! {
                _ = self.vfs.state.shutdown.shutdown_notify.notified() => {
                    self.shutdown().await;
                    break;
                }
                Some(work_item) = self.prefetch_rx.recv() => {
                    self.enqueue_work_item(work_item);
                }
                joined = self.inflight.join_next(), if !self.inflight.is_empty() => {
                    if joined.is_none() {
                        break;
                    }
                }
            }
        }
    }

    fn launch_queued_batches(&mut self) {
        while !self.pending_queue.is_empty() {
            if self.vfs.shutdown_requested() {
                self.clear_pending_queue();
                break;
            }
            let permit = match self.prefetch_sem.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => break,
            };
            let batch = self.pending_queue.pop_front().expect("batch exists");
            let PrefetchBatch {
                ino,
                block_indices,
                read_context,
            } = batch;
            for object_idx in &block_indices {
                self.pending_set.remove(&(ino, *object_idx));
            }
            self.update_queue_depth_metric();

            let mut started_blocks = Vec::with_capacity(block_indices.len());
            for object_idx in block_indices {
                if self.vfs.readahead.start_prefetch(ino, object_idx) {
                    started_blocks.push(object_idx);
                }
            }
            if started_blocks.is_empty() {
                continue;
            }

            let vfs = self.vfs.clone();
            self.inflight.spawn(async move {
                let _permit = permit;
                let _ = vfs
                    .read_prefetch_batch(PrefetchBatch::new(ino, started_blocks, read_context))
                    .await;
            });
        }
    }

    fn enqueue_work_item(&mut self, work_item: PrefetchWorkItem) {
        if self.vfs.shutdown_requested() {
            return;
        }

        let planned = match work_item {
            PrefetchWorkItem::ReadHint {
                ino,
                stream_id,
                first_block,
                last_block,
                policy,
            } => self.vfs.plan_prefetch_batches_from_hint(
                ino,
                stream_id,
                first_block,
                last_block,
                policy,
            ),
            PrefetchWorkItem::Batch(batch) => vec![batch],
        };

        for mut batch in planned {
            let ino = batch.ino;
            batch
                .block_indices
                .retain(|object_idx| self.pending_set.insert((ino, *object_idx)));
            if !batch.is_empty() {
                self.pending_queue.push_back(batch);
            }
        }
        self.update_queue_depth_metric();
    }

    fn clear_pending_queue(&mut self) {
        self.pending_queue.clear();
        self.pending_set.clear();
        self.update_queue_depth_metric();
    }

    fn update_queue_depth_metric(&self) {
        self.vfs.state.metrics.prefetch_queue_depth.store(
            self.pending_queue
                .iter()
                .map(|queued| queued.block_count() as u64)
                .sum(),
            Ordering::Relaxed,
        );
    }

    async fn shutdown(&mut self) {
        self.clear_pending_queue();
        self.inflight.abort_all();
        while self.inflight.join_next().await.is_some() {}
    }
}

impl StudioVfs {
    pub(super) fn prefetch_read_context_from_attr(
        &self,
        ino: Ino,
        attr: &super::InodeAttr,
    ) -> PrefetchReadContext {
        ReadRequestContext::from_attr(self, ino, attr.clone()).prefetch_context()
    }

    pub(super) fn contiguous_prefetch_block_ranges(
        block_indices: &mut Vec<u32>,
    ) -> Vec<(u32, u32)> {
        block_indices.sort_unstable();
        block_indices.dedup();

        let mut ranges = Vec::new();
        let Some(mut range_start) = block_indices.first().copied() else {
            return ranges;
        };
        let mut range_end = range_start;
        for &object_idx in block_indices.iter().skip(1) {
            if range_end.checked_add(1) == Some(object_idx) {
                range_end = object_idx;
            } else {
                ranges.push((range_start, range_end));
                range_start = object_idx;
                range_end = object_idx;
            }
        }
        ranges.push((range_start, range_end));
        ranges
    }

    pub(super) fn enqueue_prefetch_hint(
        &self,
        ino: Ino,
        stream_id: u64,
        policy: AccessPolicy,
        blocks_needed: &[(u32, usize, usize)],
    ) {
        if self.shutdown_requested() || blocks_needed.is_empty() {
            return;
        }
        if !policy.prefetch_enabled {
            self.state.read.clear_prefetch_hint(ino, stream_id);
            return;
        }
        let Some(range) = PrefetchHintRange::from_blocks_needed(blocks_needed) else {
            return;
        };
        let update = self
            .state
            .read
            .record_prefetch_hint_range(ino, stream_id, range);
        if let Some(path) = self.debug_pending_target_path(ino) {
            let (effective_first_block, effective_last_block, suppressed) = match update {
                PrefetchHintUpdate::Suppress => (range.first_block, range.last_block, true),
                PrefetchHintUpdate::Store(stored) => (stored.first_block, stored.last_block, false),
            };
            info!(
                ino,
                path = %path,
                stream_id,
                access_class = policy.class.as_str(),
                prefetch_enabled = policy.prefetch_enabled,
                requested_first_block = range.first_block,
                requested_last_block = range.last_block,
                effective_first_block,
                effective_last_block,
                suppressed,
                "debug target prefetch hint"
            );
        }
        let PrefetchHintUpdate::Store(_) = update else {
            return;
        };
        // The stored range is only for duplicate suppression. Feed the actual
        // current read range to readahead so the sequential detector sees
        // forward progress instead of a repeatedly merged historical span.
        let _ = self.prefetch_tx.send(PrefetchWorkItem::ReadHint {
            ino,
            stream_id,
            first_block: range.first_block,
            last_block: range.last_block,
            policy,
        });
    }

    pub(super) fn plan_prefetch_batches_from_hint(
        &self,
        ino: Ino,
        stream_id: u64,
        first_block: u32,
        last_block: u32,
        policy: AccessPolicy,
    ) -> Vec<PrefetchBatch> {
        PrefetchBatchBuilder::new(self, ino, stream_id, first_block, last_block, policy)
            .plan_batches()
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;
    use std::sync::Arc;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("prefetch-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("prefetch-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
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

    #[test]
    fn contiguous_prefetch_block_ranges_sort_and_split_sparse_batches() {
        let mut blocks = vec![9, 1, 3, 2, 8, 7, 7];

        let ranges = StudioVfs::contiguous_prefetch_block_ranges(&mut blocks);

        assert_eq!(blocks, vec![1, 2, 3, 7, 8, 9]);
        assert_eq!(ranges, vec![(1, 3), (7, 9)]);
    }

    #[test]
    fn enqueue_prefetch_hint_merges_adjacent_ranges_and_suppresses_covered_ranges() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "prefetch-hint-range-merge");
        let ino = 9_301;
        let stream_id = 77;
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker);

        vfs.enqueue_prefetch_hint(ino, stream_id, policy, &[(2, 0, 1), (3, 0, 1)]);
        assert_eq!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .get(&(ino, stream_id)),
            Some(&(2, 3))
        );

        vfs.enqueue_prefetch_hint(ino, stream_id, policy, &[(4, 0, 1)]);
        assert_eq!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .get(&(ino, stream_id)),
            Some(&(2, 4))
        );

        vfs.enqueue_prefetch_hint(ino, stream_id, policy, &[(3, 0, 1)]);
        assert_eq!(
            vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .get(&(ino, stream_id)),
            Some(&(2, 4))
        );
    }

    #[test]
    fn plan_prefetch_batches_from_hint_clears_stale_hint_for_missing_inode() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "prefetch-reader-missing-inode");
        let ino = 9_302;
        let stream_id = 88;
        let key = ReadStreamKey { ino, stream_id };
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker);

        let _ = vfs
            .readahead
            .record_read_for_stream(key, 0, 0, policy.readahead_policy());
        vfs.state
            .read
            .prefetch_last_hint
            .lock()
            .insert((ino, stream_id), (0, 2));
        assert!(vfs.readahead.debug_has_stream(key));

        let planned = vfs.plan_prefetch_batches_from_hint(ino, stream_id, 0, 2, policy);

        assert!(planned.is_empty());
        assert!(
            !vfs.state
                .read
                .prefetch_last_hint
                .lock()
                .contains_key(&(ino, stream_id))
        );
        assert!(!vfs.readahead.debug_has_stream(key));
    }

    #[test]
    fn plan_prefetch_batches_from_hint_yields_primary_batch_for_sequential_read() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "prefetch-reader-primary-batch");

        let created = vfs
            .create(ROOT_INO, b"prefetch-budget-caps.bin", 1000, 1000, 0o644)
            .expect("create file");
        let stream_id = 123;
        let key = ReadStreamKey {
            ino: created.ino,
            stream_id,
        };
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker);

        let _ = vfs
            .readahead
            .record_read_for_stream(key, 0, 0, policy.readahead_policy());
        let planned = vfs.plan_prefetch_batches_from_hint(created.ino, stream_id, 1, 1, policy);

        assert_eq!(
            planned.len(),
            1,
            "sequential reads should still yield one primary batch"
        );
        assert!(
            planned[0].block_indices.len() >= policy.readahead_min_prefetch as usize,
            "legacy reserve/cycle caps should not suppress sequential prefetch"
        );
    }

    #[test]
    fn plan_prefetch_batches_from_hint_yields_frame_sequence_batch_after_readdir() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "prefetch-frame-sequence-batch");

        let first = vfs
            .create(ROOT_INO, b"plate_0100.exr", 1000, 1000, 0o644)
            .expect("create first frame");
        let second = vfs
            .create(ROOT_INO, b"plate_0101.exr", 1000, 1000, 0o644)
            .expect("create second frame");
        let _notes = vfs
            .create(ROOT_INO, b"notes.txt", 1000, 1000, 0o644)
            .expect("create unrelated file");
        let _ = vfs.readdir(ROOT_INO).expect("readdir root");

        let stream_id = 456;
        let policy = AccessPolicy::from_class(AccessClass::SequentialMediaRead, vfs.access_tracker);
        let planned = vfs.plan_prefetch_batches_from_hint(first.ino, stream_id, 0, 0, policy);

        assert_eq!(planned.len(), 1);
        assert_eq!(
            planned[0].ino, second.ino,
            "frame sequence reads should schedule the next frame through the existing batch path"
        );
        assert_eq!(planned[0].block_indices, vec![0]);
    }
}
