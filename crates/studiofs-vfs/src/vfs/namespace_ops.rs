use super::namespace::{DeferredNamespaceInner, DeferredNamespaceKey, DeferredNamespaceOp};
use super::{
    ATTR_CACHE_TTL, AccessLevel, Arc, ChunkId, DEFAULT_CHUNK_SIZE, DentryCacheValue,
    DirAccessEntry, DirEntry, FileSessionOperation, FileType, Ino, InodeAttr,
    InodeIncarnationState, Instant, LocalSyncPolicy, MetaError, NamespacePathStyle,
    PendingIncarnation, ResolvedSliceSource, SetattrArgs, SliceId, SliceOrder, SliceRange,
    SliceRecord, StudioVfs, UnlinkTarget, VfsError, VfsForegroundWriteOp, VfsForegroundWriteTimer,
    VfsMeta, VfsResult, VfsUploadState, block_index_from_offset, chunk_id_from_offset,
    chunk_start_offset, debug, error, info, offset_within_block, validate_name, warn,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::Duration;
use studiofs_meta::engine::NamespaceMutation;
use studiofs_meta::schema::PayloadRefState;
use tracing::trace;

struct TruncateMutation {
    removed_objects: Vec<(u32, u64)>,
    restore_slices: ChunkSliceBatch,
    installed_pending_tail_block: Option<u32>,
}

type ChunkSliceBatch = Vec<(ChunkId, Vec<SliceRecord>)>;
type RemovedObjectList = Vec<(u32, u64)>;
pub(super) const NAMESPACE_INODE_BATCH: u32 = 4096;
const DEFERRED_NAMESPACE_MAX_RETRIES: u8 = 3;
const DEFERRED_NAMESPACE_BATCH_MAX_OPERATIONS: usize = 256;
const DEFERRED_NAMESPACE_BATCH_LINGER_DELAY: Duration = Duration::from_millis(10);
pub(super) const DEFERRED_NAMESPACE_BACKLOG_MAX_OPERATIONS: usize = 4096;
const DEFERRED_NAMESPACE_BACKLOG_MAX_BYTES: usize = 1024 * 1024;
const RMDIR_SETTLE_RETRY_ATTEMPTS: u16 = 80;
const RMDIR_INFLIGHT_RETRY_DELAY: Duration = Duration::from_millis(25);
const RMDIR_CLEANUP_RETRY_ATTEMPTS: u16 = 200;

#[cfg(not(target_os = "macos"))]
fn setattr_args_are_size_only(args: &SetattrArgs) -> bool {
    args.size.is_some()
        && args.mode.is_none()
        && args.uid.is_none()
        && args.gid.is_none()
        && args.atime.resolve().is_none()
        && args.mtime.resolve().is_none()
        && args.crtime.resolve().is_none()
        && args.win_attrs.is_none()
}

fn handle_backed_growth_may_defer_strict_flush(
    fh: Option<u64>,
    new_size: u64,
    old_size: u64,
    args: &SetattrArgs,
) -> bool {
    #[cfg(target_os = "macos")]
    {
        let _ = (fh, new_size, old_size, args);
        // WinFSP returns fresh FileInfo from the size callback and bounds its
        // file-info cache. macFUSE can keep a stale vnode EOF after sparse
        // growth, so publish metadata-only size changes before replying.
        false
    }
    #[cfg(not(target_os = "macos"))]
    {
        fh.is_some() && new_size > old_size && setattr_args_are_size_only(args)
    }
}

impl StudioVfs {
    fn observe_metadata_commit<T>(&self, operation: impl FnOnce() -> VfsResult<T>) -> VfsResult<T> {
        let started = Instant::now();
        let result = operation();
        self.record_metadata_commit_observability(started.elapsed(), result.is_ok());
        result
    }

    fn observe_namespace_inode_allocation<T>(
        &self,
        operation: impl FnOnce() -> studiofs_meta::MetaResult<T>,
    ) -> VfsResult<T> {
        let started = Instant::now();
        let result = operation();
        self.record_namespace_inode_allocation_observability(started.elapsed());
        Ok(result?)
    }

    fn observe_namespace_mutation<T>(
        &self,
        operation: impl FnOnce() -> studiofs_meta::MetaResult<T>,
    ) -> VfsResult<T> {
        let started = Instant::now();
        let result = operation();
        self.record_namespace_mutation_observability(started.elapsed());
        Ok(result?)
    }

    fn runtime_stats_entry_exists_error(parent: Ino, name: &[u8]) -> VfsError {
        VfsError::Meta(MetaError::EntryExists {
            parent,
            name: String::from_utf8_lossy(name).into_owned(),
        })
    }

    fn runtime_stats_access_denied_error() -> VfsError {
        VfsError::Meta(MetaError::AccessDenied {
            ino: super::RUNTIME_STATS_FILE_INO,
            principal: "runtime-stats-surface".to_string(),
            required: AccessLevel::ReadWrite,
            actual: AccessLevel::ReadOnly,
        })
    }

    fn reject_runtime_stats_entry_creation(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        if self.is_runtime_stats_file_name(parent, name) {
            return Err(Self::runtime_stats_entry_exists_error(parent, name));
        }
        Ok(())
    }

    fn reject_runtime_stats_entry_mutation(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        if self.is_runtime_stats_file_name(parent, name) {
            return Err(Self::runtime_stats_access_denied_error());
        }
        Ok(())
    }

    /// Update inode metadata fields without an open-file handle context.
    ///
    /// This is the handle-free entry point for setattr operations and delegates to
    /// `setattr_with_handle` with no handle.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        self.setattr_with_handle(None, ino, args)
    }

    /// Reserve the next inode from a shared batch, refilling it with one metadata allocation
    /// when exhausted. The reservation lock serializes refill so concurrent callers cannot
    /// overlap or discard durable inode ranges.
    pub(super) fn reserve_namespace_ino(&self) -> VfsResult<Ino> {
        let mut reserved_inos = self.state.namespace.deferred.reserved_inos.lock();
        loop {
            if let Some(ino) = reserved_inos.pop_front() {
                return Ok(ino);
            }

            let inos = self.observe_namespace_inode_allocation(|| {
                self.sync_meta().next_inode_ids(NAMESPACE_INODE_BATCH)
            })?;
            if inos.is_empty() {
                return Err(VfsError::Other("allocated inode batch is empty".into()));
            }
            reserved_inos.extend(inos);
        }
    }

    #[cfg(target_os = "linux")]
    pub(super) fn prewarm_namespace_inode_reservation(&self) -> VfsResult<()> {
        let ino = self.reserve_namespace_ino()?;
        self.return_reserved_namespace_ino(ino);
        #[cfg(test)]
        self.state
            .metrics
            .metadata_sync_barrier_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.sync_meta()
            .sync_local_metadata()
            .map_err(VfsError::Meta)
    }

    fn return_reserved_namespace_ino(&self, ino: Ino) {
        self.state
            .namespace
            .deferred
            .reserved_inos
            .lock()
            .push_front(ino);
    }

    fn deferred_namespace_error(&self, ino: Ino) -> Option<Arc<str>> {
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .errors
            .get(&ino)
            .cloned()
    }

    fn ensure_no_deferred_namespace_error(&self, ino: Ino) -> VfsResult<()> {
        if let Some(error) = self.deferred_namespace_error(ino) {
            return Err(VfsError::Other(format!(
                "deferred namespace metadata commit failed for inode {ino}: {error}"
            )));
        }
        Ok(())
    }

    fn deferred_namespace_lookup(&self, parent: Ino, name: &[u8]) -> Option<InodeAttr> {
        let key = DeferredNamespaceKey::new(parent, name);
        let inner = self.state.namespace.deferred.inner.lock();
        let ino = inner.entries.get(&key).copied()?;
        inner.attrs.get(&ino).cloned()
    }

    pub(super) fn deferred_namespace_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .attrs
            .get(&ino)
            .cloned()
    }

    fn deferred_namespace_op_estimated_bytes(op: &DeferredNamespaceOp) -> usize {
        std::mem::size_of::<DeferredNamespaceOp>()
            + match op {
                DeferredNamespaceOp::CreateFile { name, .. }
                | DeferredNamespaceOp::CreateDir { name, .. } => name.len(),
                DeferredNamespaceOp::SetAttr { .. } => 0,
            }
    }

    fn deferred_namespace_admission_error(
        &self,
        inner: &DeferredNamespaceInner,
        op_bytes: usize,
    ) -> Option<VfsError> {
        if self.shutdown_requested() {
            return Some(VfsError::Other(
                "VFS shutdown has stopped namespace admission".to_string(),
            ));
        }
        if self
            .state
            .namespace
            .deferred
            .degraded
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Some(VfsError::MetadataPersistenceDegraded);
        }
        if inner.backlog_operations >= DEFERRED_NAMESPACE_BACKLOG_MAX_OPERATIONS
            || op_bytes > DEFERRED_NAMESPACE_BACKLOG_MAX_BYTES
            || inner.backlog_bytes.saturating_add(op_bytes) > DEFERRED_NAMESPACE_BACKLOG_MAX_BYTES
        {
            return Some(VfsError::WouldBlock);
        }
        None
    }

    fn ensure_deferred_namespace_admission(&self, op_bytes: usize) -> VfsResult<()> {
        let inner = self.state.namespace.deferred.inner.lock();
        if let Some(error) = self.deferred_namespace_admission_error(&inner, op_bytes) {
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn enqueue_deferred_namespace_op(
        &self,
        key: Option<DeferredNamespaceKey>,
        attr: InodeAttr,
        op: DeferredNamespaceOp,
    ) -> VfsResult<()> {
        let op_bytes = Self::deferred_namespace_op_estimated_bytes(&op);
        {
            let mut inner = self.state.namespace.deferred.inner.lock();
            if let Some(key) = key.as_ref()
                && inner.entries.contains_key(key)
            {
                return Err(VfsError::Meta(MetaError::EntryExists {
                    parent: key.parent,
                    name: String::from_utf8_lossy(&key.name).into_owned(),
                }));
            }
            if let Some(error) = self.deferred_namespace_admission_error(&inner, op_bytes) {
                return Err(error);
            }
            if let Some(key) = key {
                inner.entries.insert(key, attr.ino);
            }
            inner.attrs.insert(attr.ino, attr.clone());
            inner.errors.remove(&attr.ino);
            *inner.pending_inodes.entry(attr.ino).or_default() += 1;
            inner.queue.push_back(op);
            inner.admitted_operations += 1;
            inner.backlog_operations += 1;
            inner.backlog_bytes += op_bytes;
        }
        self.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(attr.ino, attr);
        self.state.namespace.deferred.notify.notify_waiters();
        Ok(())
    }

    fn pop_deferred_namespace_batch(
        &self,
        max_operations: usize,
        cutoff: Option<u64>,
        batch: &mut Vec<DeferredNamespaceOp>,
    ) {
        let mut inner = self.state.namespace.deferred.inner.lock();
        let remaining_before_cutoff = cutoff.map(|cutoff| {
            cutoff
                .saturating_sub(inner.settled_operations)
                .saturating_sub(batch.len() as u64)
                .try_into()
                .unwrap_or(usize::MAX)
        });
        let count = inner
            .queue
            .len()
            .min(max_operations)
            .min(remaining_before_cutoff.unwrap_or(usize::MAX));
        batch.extend(inner.queue.drain(..count));
    }

    fn complete_deferred_namespace_op(&self, op: &DeferredNamespaceOp, committed_attr: &InodeAttr) {
        let ino = op.ino();
        let op_bytes = Self::deferred_namespace_op_estimated_bytes(op);
        {
            let mut inner = self.state.namespace.deferred.inner.lock();
            match op {
                DeferredNamespaceOp::CreateFile { parent, name, .. }
                | DeferredNamespaceOp::CreateDir { parent, name, .. } => {
                    inner
                        .entries
                        .remove(&DeferredNamespaceKey::new(*parent, name));
                }
                DeferredNamespaceOp::SetAttr { .. } => {}
            }
            let pending_count = inner.pending_inodes.get(&ino).copied().unwrap_or_default();
            if pending_count <= 1 {
                inner.pending_inodes.remove(&ino);
                inner.attrs.remove(&ino);
            } else {
                inner.pending_inodes.insert(ino, pending_count - 1);
            }
            inner.settled_operations += 1;
            inner.backlog_operations = inner.backlog_operations.saturating_sub(1);
            inner.backlog_bytes = inner.backlog_bytes.saturating_sub(op_bytes);
        }
        self.clear_matching_dirty_attr_if_committed(ino, committed_attr);
        self.invalidate_attr_read_cache([ino]);
        self.refresh_inode_incarnation_state_from_attr(ino, committed_attr);
        self.state.namespace.deferred.notify.notify_waiters();
    }

    fn abandon_failed_deferred_namespace_batch(
        &self,
        ops: &[DeferredNamespaceOp],
        error: &VfsError,
    ) {
        let error: Arc<str> = Arc::from(error.to_string());
        let mut affected_inodes = HashSet::new();
        {
            let mut inner = self.state.namespace.deferred.inner.lock();
            for op in ops {
                let ino = op.ino();
                let op_bytes = Self::deferred_namespace_op_estimated_bytes(op);
                affected_inodes.insert(ino);
                inner.errors.insert(ino, Arc::clone(&error));
                match op {
                    DeferredNamespaceOp::CreateFile { parent, name, .. }
                    | DeferredNamespaceOp::CreateDir { parent, name, .. } => {
                        inner
                            .entries
                            .remove(&DeferredNamespaceKey::new(*parent, name));
                    }
                    DeferredNamespaceOp::SetAttr { .. } => {}
                }
                let pending_count = inner.pending_inodes.get(&ino).copied().unwrap_or_default();
                if pending_count <= 1 {
                    inner.pending_inodes.remove(&ino);
                    inner.attrs.remove(&ino);
                } else {
                    inner.pending_inodes.insert(ino, pending_count - 1);
                }
                inner.backlog_operations = inner.backlog_operations.saturating_sub(1);
                inner.backlog_bytes = inner.backlog_bytes.saturating_sub(op_bytes);
            }
            inner.settled_operations += ops.len() as u64;
        }
        {
            let mut dirty_attrs = self.state.namespace.dirty_attrs.lock();
            for ino in &affected_inodes {
                dirty_attrs.remove(ino);
            }
        }
        self.invalidate_attr_read_cache(affected_inodes);
        self.state.namespace.deferred.notify.notify_waiters();
    }

    pub(super) async fn wait_for_deferred_namespace_metadata(&self, ino: Ino) -> VfsResult<()> {
        loop {
            let notified = self.state.namespace.deferred.notify.notified();
            {
                let inner = self.state.namespace.deferred.inner.lock();
                if let Some(error) = inner.errors.get(&ino) {
                    return Err(VfsError::Other(format!(
                        "deferred namespace metadata commit failed for inode {ino}: {error}"
                    )));
                }
                if !inner.pending_inodes.contains_key(&ino) {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    async fn wait_for_deferred_namespace_entry(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        if let Some(attr) = self.deferred_namespace_lookup(parent, name) {
            self.wait_for_deferred_namespace_metadata(attr.ino).await?;
        }
        Ok(())
    }

    /// Return whether deferred namespace metadata is currently pending for `ino`.
    ///
    /// This is a non-blocking snapshot and may change immediately after return.
    pub fn has_deferred_namespace_metadata(&self, ino: Ino) -> bool {
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .pending_inodes
            .contains_key(&ino)
    }

    /// Return an error when deferred namespace metadata persistence is degraded.
    pub fn ensure_deferred_namespace_metadata_healthy(&self) -> VfsResult<()> {
        if !self.read_only_mount
            && self
                .state
                .namespace
                .deferred
                .degraded
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(VfsError::MetadataPersistenceDegraded);
        }
        Ok(())
    }

    pub(super) fn deferred_namespace_shutdown_error(&self) -> Option<VfsError> {
        if self
            .state
            .namespace
            .deferred
            .degraded
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Some(VfsError::MetadataPersistenceDegraded);
        }
        let inner = self.state.namespace.deferred.inner.lock();
        let draining = self
            .state
            .namespace
            .deferred
            .draining
            .load(std::sync::atomic::Ordering::Acquire);
        if self.shutdown_requested()
            && (draining || !inner.queue.is_empty() || inner.backlog_operations != 0)
        {
            return Some(VfsError::Other(
                "deferred namespace metadata drain did not complete".to_string(),
            ));
        }
        None
    }

    /// Schedules the background drain for namespace metadata admitted in memory.
    pub fn schedule_deferred_namespace_metadata_drain(
        self: &Arc<Self>,
        runtime: &tokio::runtime::Handle,
    ) {
        if self
            .state
            .namespace
            .deferred
            .draining
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let vfs = Arc::clone(self);
        self.track_background_task(runtime.spawn(async move {
            vfs.drain_deferred_namespace_metadata(None).await;
        }));
    }

    pub(super) async fn drain_deferred_namespace_metadata_for_barrier(&self) -> VfsResult<()> {
        let cutoff = self
            .state
            .namespace
            .deferred
            .inner
            .lock()
            .admitted_operations;
        #[cfg(test)]
        self.state
            .namespace
            .deferred
            .barrier_cutoff
            .store(cutoff, std::sync::atomic::Ordering::Release);
        if !self
            .state
            .namespace
            .deferred
            .draining
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            self.drain_deferred_namespace_metadata(Some(cutoff)).await;
        } else {
            loop {
                let notified = self.state.namespace.deferred.notify.notified();
                let (settled, take_over) = {
                    let inner = self.state.namespace.deferred.inner.lock();
                    if inner.settled_operations >= cutoff {
                        (true, false)
                    } else {
                        let draining = self
                            .state
                            .namespace
                            .deferred
                            .draining
                            .load(std::sync::atomic::Ordering::Acquire);
                        if !draining && !inner.queue.is_empty() {
                            (
                                false,
                                self.state
                                    .namespace
                                    .deferred
                                    .draining
                                    .compare_exchange(
                                        false,
                                        true,
                                        std::sync::atomic::Ordering::AcqRel,
                                        std::sync::atomic::Ordering::Acquire,
                                    )
                                    .is_ok(),
                            )
                        } else {
                            (false, false)
                        }
                    }
                };
                if settled {
                    break;
                }
                if take_over {
                    self.drain_deferred_namespace_metadata(Some(cutoff)).await;
                    break;
                }
                notified.await;
            }
        }
        if let Some(error) = self.deferred_namespace_shutdown_error() {
            return Err(error);
        }
        Ok(())
    }

    pub(super) async fn wait_for_deferred_namespace_metadata_during_shutdown(
        &self,
        timeout: Duration,
    ) -> VfsResult<()> {
        let cutoff = self
            .state
            .namespace
            .deferred
            .inner
            .lock()
            .admitted_operations;
        self.resume_deferred_namespace_metadata_drain();

        let sleep = tokio::time::sleep(timeout);
        tokio::pin!(sleep);
        loop {
            let notified = self.state.namespace.deferred.notify.notified();
            let drained = {
                let inner = self.state.namespace.deferred.inner.lock();
                inner.settled_operations >= cutoff
                    && !self
                        .state
                        .namespace
                        .deferred
                        .draining
                        .load(std::sync::atomic::Ordering::Acquire)
                    && inner.queue.is_empty()
                    && inner.backlog_operations == 0
            };
            if drained {
                return self.deferred_namespace_shutdown_error().map_or(Ok(()), Err);
            }
            tokio::select! {
                _ = &mut sleep => {
                    return Err(VfsError::Other(
                        "deferred namespace metadata drain timed out during shutdown".to_string(),
                    ));
                }
                _ = notified => {}
            }
        }
    }

    fn resume_deferred_namespace_metadata_drain(&self) {
        let Some(vfs) = self.state.namespace.deferred.upgrade_owner() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        vfs.schedule_deferred_namespace_metadata_drain(&runtime);
    }

    fn finish_deferred_namespace_drain(&self, resume_background_drain: bool) {
        self.state
            .namespace
            .deferred
            .draining
            .store(false, std::sync::atomic::Ordering::Release);
        if resume_background_drain && !self.state.namespace.deferred.inner.lock().queue.is_empty() {
            self.resume_deferred_namespace_metadata_drain();
        }
        self.state.namespace.deferred.notify.notify_waiters();
    }

    async fn drain_deferred_namespace_metadata(&self, cutoff: Option<u64>) {
        let mut batch = Vec::with_capacity(DEFERRED_NAMESPACE_BATCH_MAX_OPERATIONS);
        loop {
            if cutoff.is_some_and(|cutoff| {
                self.state
                    .namespace
                    .deferred
                    .inner
                    .lock()
                    .settled_operations
                    >= cutoff
            }) {
                self.finish_deferred_namespace_drain(true);
                break;
            }
            batch.clear();
            self.pop_deferred_namespace_batch(
                DEFERRED_NAMESPACE_BATCH_MAX_OPERATIONS,
                cutoff,
                &mut batch,
            );
            if batch.is_empty() {
                self.state
                    .namespace
                    .deferred
                    .draining
                    .store(false, std::sync::atomic::Ordering::Release);
                if self.state.namespace.deferred.inner.lock().queue.is_empty()
                    || self
                        .state
                        .namespace
                        .deferred
                        .draining
                        .swap(true, std::sync::atomic::Ordering::AcqRel)
                {
                    self.state.namespace.deferred.notify.notify_waiters();
                    break;
                }
                continue;
            }

            let sleep = tokio::time::sleep(DEFERRED_NAMESPACE_BATCH_LINGER_DELAY);
            tokio::pin!(sleep);
            while batch.len() < DEFERRED_NAMESPACE_BATCH_MAX_OPERATIONS {
                let notified = self.state.namespace.deferred.notify.notified();
                tokio::select! {
                    _ = notified => {
                        let room = DEFERRED_NAMESPACE_BATCH_MAX_OPERATIONS - batch.len();
                        self.pop_deferred_namespace_batch(room, cutoff, &mut batch);
                    }
                    _ = &mut sleep => break,
                }
            }

            let mut attempts = 0;
            loop {
                let result = self.commit_deferred_namespace_batch(&batch).await;
                match result {
                    Ok(committed_attrs) => {
                        for (op, committed_attr) in batch.iter().zip(committed_attrs) {
                            self.complete_deferred_namespace_op(op, &committed_attr);
                        }
                        break;
                    }
                    Err(error) => {
                        if let Some(conflict_index) = Self::namespace_entry_conflict_index(&error)
                            .filter(|index| *index < batch.len())
                        {
                            let conflicting_operation = batch.remove(conflict_index);
                            let mut abandoned_inodes = HashSet::from([conflicting_operation.ino()]);
                            let mut abandoned_operations = vec![conflicting_operation];
                            for op in batch.split_off(conflict_index) {
                                let depends_on_abandoned_inode = match &op {
                                    DeferredNamespaceOp::CreateFile { parent, .. }
                                    | DeferredNamespaceOp::CreateDir { parent, .. } => {
                                        abandoned_inodes.contains(&op.ino())
                                            || abandoned_inodes.contains(parent)
                                    }
                                    DeferredNamespaceOp::SetAttr { .. } => {
                                        abandoned_inodes.contains(&op.ino())
                                    }
                                };
                                if depends_on_abandoned_inode {
                                    abandoned_inodes.insert(op.ino());
                                    abandoned_operations.push(op);
                                } else {
                                    batch.push(op);
                                }
                            }
                            self.abandon_failed_deferred_namespace_batch(
                                &abandoned_operations,
                                &error,
                            );
                            warn!(
                                error = %error,
                                conflict_index,
                                abandoned_operations = abandoned_operations.len(),
                                remaining_operations = batch.len(),
                                "deferred namespace metadata operations lost a concurrent namespace conflict; retrying independent operations without degrading mount"
                            );
                            if batch.is_empty() {
                                break;
                            }
                            continue;
                        }
                        attempts += 1;
                        if attempts >= DEFERRED_NAMESPACE_MAX_RETRIES {
                            self.state
                                .namespace
                                .deferred
                                .degraded
                                .store(true, std::sync::atomic::Ordering::Release);
                            let queued = std::mem::take(
                                &mut self.state.namespace.deferred.inner.lock().queue,
                            );
                            let failed_operations = batch.len() + queued.len();
                            batch.extend(queued);
                            self.abandon_failed_deferred_namespace_batch(&batch, &error);
                            error!(
                                error = %error,
                                operations = failed_operations,
                                "deferred namespace metadata batch commit failed permanently; abandoning batch"
                            );
                            break;
                        }
                        warn!(
                            attempt = attempts,
                            max_attempts = DEFERRED_NAMESPACE_MAX_RETRIES,
                            error = %error,
                            operations = batch.len(),
                            "deferred namespace metadata batch commit failed; retrying"
                        );
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        }
    }

    fn namespace_entry_conflict_index(error: &VfsError) -> Option<usize> {
        match error {
            VfsError::Meta(MetaError::NamespaceBatchOperation { index, source })
                if matches!(source.as_ref(), MetaError::EntryExists { .. }) =>
            {
                Some(*index)
            }
            _ => None,
        }
    }

    fn deferred_namespace_mutation(op: &DeferredNamespaceOp) -> NamespaceMutation {
        match op {
            DeferredNamespaceOp::CreateFile {
                attr,
                parent,
                name,
                uid,
                gid,
                mode,
            } => NamespaceMutation::CreateFile {
                ino: attr.ino,
                parent: *parent,
                name: name.clone(),
                uid: *uid,
                gid: *gid,
                mode: *mode,
            },
            DeferredNamespaceOp::CreateDir {
                attr,
                parent,
                name,
                uid,
                gid,
                mode,
            } => NamespaceMutation::CreateDir {
                ino: attr.ino,
                parent: *parent,
                name: name.clone(),
                uid: *uid,
                gid: *gid,
                mode: *mode,
            },
            DeferredNamespaceOp::SetAttr { attr, args } => NamespaceMutation::SetAttr {
                attr: attr.clone(),
                args: args.clone(),
            },
        }
    }

    async fn commit_deferred_namespace_batch(
        &self,
        ops: &[DeferredNamespaceOp],
    ) -> VfsResult<Vec<InodeAttr>> {
        let meta = self.meta.clone();
        let started = Instant::now();
        let mutations = ops
            .iter()
            .map(Self::deferred_namespace_mutation)
            .collect::<Vec<_>>();
        let stage_started = Instant::now();
        let result = crate::runtime::run_meta(meta, move |engine| {
            engine.apply_namespace_mutations(&mutations)
        })
        .await;
        let result = match result {
            Ok(committed_attrs) if committed_attrs.len() != ops.len() => {
                Err(VfsError::Other(format!(
                    "deferred namespace batch returned {} results for {} operations",
                    committed_attrs.len(),
                    ops.len()
                )))
            }
            result => result,
        };
        self.record_namespace_mutation_observability(stage_started.elapsed());
        if result.is_ok() {
            for op in ops {
                if matches!(
                    op,
                    DeferredNamespaceOp::CreateFile { .. } | DeferredNamespaceOp::CreateDir { .. }
                ) {
                    self.record_namespace_create_completed();
                }
            }
        }
        self.record_metadata_commit_observability(started.elapsed(), result.is_ok());
        result
    }

    fn setattr_attr_only_deferred_local(
        &self,
        ino: Ino,
        args: SetattrArgs,
    ) -> VfsResult<InodeAttr> {
        if args.size.is_some() {
            return self.setattr_with_handle(None, ino, args);
        }
        if self.is_runtime_stats_file_ino(ino) {
            return Err(Self::runtime_stats_access_denied_error());
        }
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if let Some(attr) = self.update_ignored_entry(ino, |attr| {
            if args.apply_attr_only(attr) {
                attr.touch_ctime();
            }
        }) {
            return Ok(attr);
        }

        let mut attr = self.attr_only_setattr_base_attr(ino)?;
        if args.apply_attr_only(&mut attr) {
            attr.touch_ctime();
        }

        Ok(attr)
    }

    fn attr_only_setattr_base_attr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.state.namespace.dirty_attrs.lock().get(&ino).cloned() {
            return Ok(attr);
        }
        self.getattr(ino)
    }

    /// Apply attr-only metadata updates locally and commit them from the deferred metadata worker.
    pub fn setattr_attr_only_deferred_metadata(
        self: &Arc<Self>,
        ino: Ino,
        args: SetattrArgs,
        runtime: &tokio::runtime::Handle,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Setattr);
        self.ensure_deferred_namespace_admission(std::mem::size_of::<DeferredNamespaceOp>())?;
        let attr = self.setattr_attr_only_deferred_local(ino, args.clone())?;
        self.enqueue_deferred_namespace_op(
            None,
            attr.clone(),
            DeferredNamespaceOp::SetAttr {
                attr: attr.clone(),
                args,
            },
        )?;
        self.invalidate_attr_read_cache([ino]);
        self.schedule_deferred_namespace_metadata_drain(runtime);
        Ok(attr)
    }

    /// Update inode attributes, optionally while holding an open handle.
    ///
    /// `fh` is used to enforce mutating handle access for size-changing updates.
    /// Other metadata updates mutate VFS and metadata-layer state in place while
    /// updating ctime and dirty attribute caches.
    pub fn setattr_with_handle(
        &self,
        fh: Option<u64>,
        ino: Ino,
        args: SetattrArgs,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Setattr);
        if self.is_runtime_stats_file_ino(ino) {
            return Err(Self::runtime_stats_access_denied_error());
        }
        self.enforce_access(ino, AccessLevel::ReadWrite)?;
        if let Some(attr) = self.update_ignored_entry(ino, |attr| {
            if let Some(new_size) = args.size {
                attr.size = new_size;
                attr.committed_size = new_size;
            }
            if let Some(mode) = args.mode {
                attr.mode = mode & 0o7777;
            }
            if let Some(uid) = args.uid {
                attr.uid = uid;
            }
            if let Some(gid) = args.gid {
                attr.gid = gid;
            }
            if let Some(ns) = args.atime.resolve() {
                attr.atime = ns;
            }
            if let Some(ns) = args.mtime.resolve() {
                attr.mtime = ns;
            }
            if let Some(ns) = args.crtime.resolve() {
                attr.crtime = ns;
            }
            if let Some(wa) = args.win_attrs {
                attr.win_attrs = wa;
            }
            attr.touch_ctime();
        }) {
            return Ok(attr);
        }

        let mut attr = self.attr_only_setattr_base_attr(ino)?;
        if let Some(new_size) = args.size
            && new_size != attr.size
        {
            if let Some(handle) = fh {
                self.enforce_mutating_handle_access(handle, ino)?;
            } else {
                self.enforce_no_known_foreign_hard_lock(ino)?;
            }
            let defer_handle_growth_flush =
                handle_backed_growth_may_defer_strict_flush(fh, new_size, attr.size, &args);
            let updated = Self::run_sync_future(self.setattr_with_size_change(
                ino,
                args,
                defer_handle_growth_flush,
            ))?;
            if let Some(handle) = fh {
                self.handle_table.update_open_attr(handle, updated.clone());
            }
            return Ok(updated);
        }

        if args.apply_attr_only(&mut attr) {
            attr.touch_ctime();
            self.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, attr.clone());
            self.sync_meta().set_attr(&attr)?;
            self.invalidate_attr_read_cache([ino]);
            self.clear_matching_dirty_attr_if_committed(ino, &attr);
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
        }

        Ok(attr)
    }

    async fn setattr_with_size_change(
        &self,
        ino: Ino,
        args: SetattrArgs,
        defer_handle_growth_flush: bool,
    ) -> VfsResult<InodeAttr> {
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Flush)
            .await;
        let result: VfsResult<InodeAttr> = async {
            let candidate_old_attr = if defer_handle_growth_flush {
                Some(self.getattr_async(ino).await?)
            } else {
                None
            };
            let has_active_dirty_writer =
                candidate_old_attr.is_some() && self.state.writer_stack.has_dirty_writers(ino);
            let defer_strict_flush_for_growth =
                candidate_old_attr.as_ref().is_some_and(|old_attr| {
                    args.size.is_some_and(|new_size| new_size > old_attr.size)
                        && (has_active_dirty_writer
                            || (!Self::needs_epoch_finalization(old_attr)
                                && !self.inode_has_materialized_publish_work(
                                    ino,
                                    old_attr.content_epoch,
                                )
                                && !self.inode_has_unmaterialized_overlay_for_finalize(ino)))
                });
            let old_attr = if defer_strict_flush_for_growth {
                candidate_old_attr.expect("candidate attr present for deferred growth")
            } else {
                let persisted_slice_bytes_before_flush =
                    self.total_pending_persisted_local_dirty_slice_bytes();
                let materialized_writer_stack_bytes =
                    self.materialize_writer_stack_for_inode(ino).await?;
                self.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
                    .await?;
                self.maybe_payload_store_sync_data_async(
                    materialized_writer_stack_bytes
                        .saturating_add(persisted_slice_bytes_before_flush)
                        as u64,
                    LocalSyncPolicy::Strict,
                )
                .await?;

                self.getattr_async(ino).await?
            };
            let defer_growth_for_active_dirty_writer =
                defer_strict_flush_for_growth && has_active_dirty_writer;
            let mut attr = old_attr.clone();
            let mut truncate_mutation = None;
            let mut requeue_retained_uploads = false;

            if let Some(new_size) = args.size
                && new_size != attr.size
            {
                if new_size < attr.size {
                    self.state.writer_stack.clear_inode(ino);
                    self.begin_size_change_incarnation(ino, &mut attr);
                    truncate_mutation = Some(self.truncate_blocks(
                        ino,
                        new_size,
                        attr.size,
                        old_attr.content_epoch,
                    )?);
                    requeue_retained_uploads = true;
                } else {
                    let has_publish_work_for_current_epoch = Self::needs_epoch_finalization(&attr)
                        || self.inode_has_materialized_publish_work(ino, attr.content_epoch);
                    // Active dirty writers keep the current epoch open, so size-only
                    // handle-backed growth can merge into that pending incarnation
                    // instead of forcing strict syncs and upload requeues.
                    if has_publish_work_for_current_epoch && !defer_growth_for_active_dirty_writer {
                        self.begin_size_change_incarnation(ino, &mut attr);
                        requeue_retained_uploads = true;
                    } else {
                        self.begin_pending_incarnation(ino, &mut attr);
                    }
                }
                attr.size = new_size;
                self.update_pending_incarnation_size(ino, attr.size);
            }

            if let Some(mode) = args.mode {
                attr.mode = mode & 0o7777;
            }

            if let Some(uid) = args.uid {
                attr.uid = uid;
            }

            if let Some(gid) = args.gid {
                attr.gid = gid;
            }

            if let Some(ns) = args.atime.resolve() {
                attr.atime = ns;
            }

            if let Some(ns) = args.mtime.resolve() {
                attr.mtime = ns;
            }

            if let Some(ns) = args.crtime.resolve() {
                attr.crtime = ns;
            }

            if let Some(wa) = args.win_attrs {
                attr.win_attrs = wa;
            }

            attr.touch_ctime();
            self.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, attr.clone());
            self.invalidate_attr_read_cache([ino]);

            if !defer_strict_flush_for_growth {
                let flush_result = match self.fail_size_change_setattr_before_flush_for_test() {
                    Ok(()) => {
                        self.flush_local_dirty_state(ino, LocalSyncPolicy::Strict)
                            .await
                    }
                    Err(error) => Err(error),
                };
                if let Err(error) = flush_result {
                    self.rollback_failed_size_change_setattr(
                        ino,
                        &old_attr,
                        truncate_mutation.as_ref(),
                    )?;
                    return Err(error);
                }
                if truncate_mutation.is_some() {
                    self.clear_matching_dirty_attr_if_committed(ino, &attr);
                }
            }

            if requeue_retained_uploads {
                self.requeue_retained_overlay_uploads_for_size_change(
                    ino,
                    attr.size,
                    attr.content_epoch,
                );
                if attr.size > old_attr.size {
                    self.preserve_resolved_regular_file_graph_for_size_change(ino, attr.size)?;
                }
            }
            self.finalize_truncate_cleanup(ino, truncate_mutation.as_ref());
            self.refresh_inode_incarnation_state_from_attr(ino, &attr);
            Ok(attr)
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    fn inode_has_materialized_publish_work(&self, ino: Ino, content_epoch: u64) -> bool {
        self.pending_uploads_for_inode(ino)
            .iter()
            .any(|(_, _, pending_epoch)| *pending_epoch == content_epoch)
            || self
                .sync_meta()
                .has_local_regular_file_overlay(ino)
                .unwrap_or_else(|error| {
                    warn!(
                        ino,
                        error = %error,
                        "failed to inspect local regular-file overlay before size growth"
                    );
                    true
                })
            || self
                .state
                .upload
                .pending_epoch_finalizations
                .lock()
                .iter()
                .any(|pending| pending.0 == ino && pending.1 == content_epoch)
    }

    fn rollback_failed_size_change_setattr(
        &self,
        ino: Ino,
        old_attr: &InodeAttr,
        truncate_mutation: Option<&TruncateMutation>,
    ) -> VfsResult<()> {
        self.clear_pending_payloads_for_inode(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.invalidate_attr_read_cache([ino]);

        self.rollback_truncate_mutation(ino, old_attr.content_epoch, truncate_mutation)?;

        self.sync_meta().set_attr(old_attr)?;
        self.refresh_inode_incarnation_state_from_attr(ino, old_attr);
        Ok(())
    }

    fn rollback_truncate_mutation(
        &self,
        ino: Ino,
        restore_content_epoch: u64,
        truncate_mutation: Option<&TruncateMutation>,
    ) -> VfsResult<()> {
        let Some(mutation) = truncate_mutation else {
            return Ok(());
        };

        if let Some(block_index) = mutation.installed_pending_tail_block {
            self.clear_pending_payload(ino, block_index);
        }

        if !mutation.restore_slices.is_empty() {
            self.sync_meta()
                .flush_regular_file_overlay_slices_with_attr(ino, &mutation.restore_slices, None)?;
        }

        let mut live_extents = Vec::new();
        for (chunk_id, slices) in &mutation.restore_slices {
            let chunk_base = chunk_start_offset(*chunk_id, DEFAULT_CHUNK_SIZE);
            for slice in slices {
                let Some(block) = slice.payload.as_ref() else {
                    continue;
                };
                let absolute_offset = chunk_base.saturating_add(slice.range.start as u64);
                let block_index = block_index_from_offset(absolute_offset, self.block_size);
                live_extents.push((ino, block_index, block.object_id, restore_content_epoch));
            }
        }
        if !live_extents.is_empty() {
            self.record_live_extents(&live_extents);
        }

        Ok(())
    }

    fn finalize_truncate_cleanup(&self, ino: Ino, truncate_mutation: Option<&TruncateMutation>) {
        let Some(mutation) = truncate_mutation else {
            return;
        };

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut scheduled_for_gc = std::collections::HashSet::new();
        for (block_index, object_id) in &mutation.removed_objects {
            if scheduled_for_gc.insert(*object_id)
                && let Err(error) = self.sync_meta().add_pending_deletion(*object_id, timestamp)
            {
                warn!(
                    ino,
                    object_id = *object_id,
                    error = %error,
                    "failed to schedule truncated object for deferred deletion"
                );
            }
            self.evict_block_from_caches(ino, *block_index, *object_id);
        }
    }

    fn requeue_retained_overlay_uploads_for_size_change(
        &self,
        ino: Ino,
        size: u64,
        content_epoch: u64,
    ) {
        let mut retained = std::collections::BTreeMap::<u64, (u64, u64, u64, u32)>::new();
        if size > 0 {
            match self.sync_meta().lookup_regular_file_slices(ino, 0, size) {
                Ok(spans) => {
                    for span in spans {
                        if span.source != ResolvedSliceSource::Overlay {
                            continue;
                        }
                        let Some(payload) = span.payload else {
                            continue;
                        };
                        let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
                        let absolute_offset =
                            chunk_base.saturating_add(u64::from(span.range.start));
                        let object_file_offset =
                            absolute_offset.saturating_sub(u64::from(payload.offset));
                        let payload_end = absolute_offset.saturating_add(u64::from(payload.length));
                        retained
                            .entry(payload.object_id)
                            .and_modify(|(offset, object_start, end, block_index)| {
                                if absolute_offset < *offset {
                                    *offset = absolute_offset;
                                    *block_index =
                                        block_index_from_offset(absolute_offset, self.block_size);
                                }
                                *object_start = (*object_start).min(object_file_offset);
                                *end = (*end).max(payload_end);
                            })
                            .or_insert_with(|| {
                                (
                                    absolute_offset,
                                    object_file_offset,
                                    payload_end,
                                    block_index_from_offset(absolute_offset, self.block_size),
                                )
                            });
                    }
                }
                Err(error) => {
                    warn!(
                        ino,
                        error = %error,
                        "failed to inspect retained overlay payloads after size change"
                    );
                    return;
                }
            }
        }

        if retained.is_empty() {
            self.cancel_pending_uploads_for_inode(ino);
            return;
        }

        let object_ids: Vec<_> = retained.keys().copied().collect();
        if let Err(error) = self.cached_store.link_upload_stage_batch(&object_ids) {
            warn!(
                ino,
                object_count = object_ids.len(),
                error = %error,
                "failed to relink retained overlay payloads after size change"
            );
        }

        let upload_ops: Vec<_> = retained
            .iter()
            .map(|(object_id, (_, object_file_offset, payload_end, _))| {
                (
                    ino,
                    *object_file_offset,
                    *object_file_offset,
                    *object_id,
                    content_epoch,
                    payload_end.saturating_sub(*object_file_offset),
                )
            })
            .collect();
        let mut retained_pending = std::collections::HashMap::new();
        let mut cancel_pending = Vec::new();
        for op in self.pending_uploads_snapshot_for_inode(ino) {
            if retained.contains_key(&op.object_id) {
                retained_pending.insert(op.object_id, op);
            } else {
                cancel_pending.push(op);
            }
        }
        if !retained_pending.is_empty() {
            self.cancel_upload_token_for_inode(ino);
        }
        let mut submit_upload_ops = Vec::new();
        for op in upload_ops {
            let (op_ino, offset, object_file_offset, object_id, op_epoch, payload_len) = op;
            if let Some(current) = retained_pending.remove(&object_id) {
                let mut rebased = current.clone();
                rebased.offset = offset;
                rebased.object_file_offset = object_file_offset;
                rebased.content_epoch = op_epoch;
                rebased.payload_len = payload_len;
                if self.cached_store.requeue_rebased_upload(&current, rebased) {
                    continue;
                }
            }
            submit_upload_ops.push((
                op_ino,
                offset,
                object_file_offset,
                object_id,
                op_epoch,
                payload_len,
            ));
        }
        for op in cancel_pending {
            self.cached_store.cancel_upload(&op);
        }
        let live_extents: Vec<_> = retained
            .into_iter()
            .map(|(object_id, (_, _, _, block_index))| (ino, block_index, object_id, content_epoch))
            .collect();
        self.record_live_extents(&live_extents);
        self.cached_store.submit_uploads_batch(&submit_upload_ops);
        self.state.file_session.note_background_transition(
            ino,
            FileSessionOperation::Upload,
            "queued",
        );
    }

    fn preserve_resolved_regular_file_graph_for_size_change(
        &self,
        ino: Ino,
        size: u64,
    ) -> VfsResult<()> {
        if size == 0 {
            return Ok(());
        }

        let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
        for span in self.sync_meta().lookup_regular_file_slices(ino, 0, size)? {
            match span.source {
                ResolvedSliceSource::Base | ResolvedSliceSource::Hole => {}
                ResolvedSliceSource::Overlay => {
                    let Some(payload) = span.payload else {
                        continue;
                    };
                    let mut slice =
                        SliceRecord::new(SliceId(0), SliceOrder(0), span.range, payload);
                    slice.payload_state = PayloadRefState::Uploaded;
                    chunks.entry(span.chunk_id).or_default().push(slice);
                }
                ResolvedSliceSource::OverlayTombstone | ResolvedSliceSource::BaseTombstone => {
                    chunks
                        .entry(span.chunk_id)
                        .or_default()
                        .push(SliceRecord::tombstone(
                            SliceId(0),
                            SliceOrder(0),
                            span.range,
                        ));
                }
            }
        }

        if chunks.is_empty() {
            return Ok(());
        }

        self.sync_meta()
            .flush_regular_file_overlay_slices_with_attr(
                ino,
                &chunks.into_iter().collect::<Vec<_>>(),
                None,
            )?;
        Ok(())
    }

    fn truncate_tombstone_slices(
        &self,
        start_offset: u64,
        end_offset: u64,
    ) -> VfsResult<ChunkSliceBatch> {
        let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
        let mut cursor = start_offset;
        while cursor < end_offset {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let chunk_end = chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64);
            let piece_end = end_offset.min(chunk_end);
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| VfsError::Other("invalid truncate tombstone range".to_string()))?;
            chunks
                .entry(chunk_id)
                .or_default()
                .push(SliceRecord::tombstone(SliceId(0), SliceOrder(0), range));
            cursor = piece_end;
        }
        Ok(chunks.into_iter().collect())
    }

    fn truncate_restore_slices_and_removed_objects(
        &self,
        ino: Ino,
        new_size: u64,
        old_size: u64,
    ) -> VfsResult<(ChunkSliceBatch, RemovedObjectList)> {
        let mut kept_object_ids = std::collections::HashSet::new();
        if new_size > 0 {
            for span in self
                .sync_meta()
                .lookup_regular_file_slices(ino, 0, new_size)?
            {
                if let Some(block) = span.payload {
                    kept_object_ids.insert(block.object_id);
                }
            }
        }

        let mut restore_chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
        let mut removed_objects = Vec::new();
        let mut removed_seen = std::collections::HashSet::new();
        for span in self.sync_meta().lookup_regular_file_slices(
            ino,
            new_size,
            old_size.saturating_sub(new_size),
        )? {
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let absolute_offset = chunk_base.saturating_add(span.range.start as u64);
            let block_index = block_index_from_offset(absolute_offset, self.block_size);
            match span.payload {
                Some(block) => {
                    if matches!(span.source, ResolvedSliceSource::Overlay)
                        && !kept_object_ids.contains(&block.object_id)
                        && removed_seen.insert(block.object_id)
                    {
                        removed_objects.push((block_index, block.object_id));
                    }
                    let mut slice = SliceRecord::new(SliceId(0), SliceOrder(0), span.range, block);
                    slice.payload_state = match span.source {
                        ResolvedSliceSource::Base => PayloadRefState::Published,
                        _ => PayloadRefState::LocalOnly,
                    };
                    restore_chunks.entry(span.chunk_id).or_default().push(slice);
                }
                None if matches!(
                    span.source,
                    ResolvedSliceSource::OverlayTombstone | ResolvedSliceSource::BaseTombstone
                ) =>
                {
                    restore_chunks
                        .entry(span.chunk_id)
                        .or_default()
                        .push(SliceRecord::tombstone(
                            SliceId(0),
                            SliceOrder(0),
                            span.range,
                        ));
                }
                None => {}
            }
        }

        Ok((restore_chunks.into_iter().collect(), removed_objects))
    }

    fn truncate_blocks(
        &self,
        ino: Ino,
        new_size: u64,
        old_size: u64,
        restore_content_epoch: u64,
    ) -> VfsResult<TruncateMutation> {
        let first_block_to_remove = if new_size == 0 {
            0
        } else {
            block_index_from_offset(new_size - 1, self.block_size) + 1
        };
        let (restore_slices, removed_objects) =
            self.truncate_restore_slices_and_removed_objects(ino, new_size, old_size)?;
        let mutation = TruncateMutation {
            removed_objects,
            restore_slices,
            installed_pending_tail_block: None,
        };

        let tombstones = self.truncate_tombstone_slices(new_size, old_size)?;
        if let Err(error) = self
            .sync_meta()
            .flush_regular_file_overlay_slices_with_attr(ino, &tombstones, None)
        {
            return Err(error.into());
        }

        let mut removed_block_count = 0usize;
        for object_idx in
            first_block_to_remove..=block_index_from_offset(old_size - 1, self.block_size)
        {
            self.clear_pending_payload(ino, object_idx);
            self.remove_live_extent(ino, object_idx);
            removed_block_count = removed_block_count.saturating_add(1);
            if let Err(error) = self
                .fail_size_change_truncate_after_first_removed_block_for_test(removed_block_count)
            {
                self.rollback_truncate_mutation(ino, restore_content_epoch, Some(&mutation))?;
                return Err(error);
            }
        }

        if new_size > 0 {
            let new_last_block = block_index_from_offset(new_size - 1, self.block_size);
            let last_block_offset = offset_within_block(new_size - 1, self.block_size);
            let new_last_len = last_block_offset as usize + 1;
            self.remove_live_extent(ino, new_last_block);
            let trimmed_local =
                match self.trim_local_payload_staging(ino, new_last_block, new_last_len) {
                    Ok(trimmed_local) => trimmed_local,
                    Err(error) => {
                        self.rollback_truncate_mutation(
                            ino,
                            restore_content_epoch,
                            Some(&mutation),
                        )?;
                        return Err(error);
                    }
                };
            if !trimmed_local {
                let _ = new_last_block;
                let _ = new_last_len;
            }
        }

        Ok(mutation)
    }

    #[cfg(test)]
    pub(super) fn arm_fail_next_size_change_setattr_before_flush_for_test(&self) {
        self.fail_next_size_change_setattr_before_flush
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn fail_size_change_setattr_before_flush_for_test(&self) -> VfsResult<()> {
        if self
            .fail_next_size_change_setattr_before_flush
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            Err(VfsError::Other(
                "simulated size-changing setattr local persist failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(not(test))]
    fn fail_size_change_setattr_before_flush_for_test(&self) -> VfsResult<()> {
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn arm_fail_next_size_change_truncate_after_first_removed_block_for_test(&self) {
        self.fail_next_size_change_truncate_after_first_removed_block
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn fail_size_change_truncate_after_first_removed_block_for_test(
        &self,
        removed_block_count: usize,
    ) -> VfsResult<()> {
        if removed_block_count == 1
            && self
                .fail_next_size_change_truncate_after_first_removed_block
                .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            Err(VfsError::Other(
                "simulated truncate mutation failure after first removed block".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(not(test))]
    fn fail_size_change_truncate_after_first_removed_block_for_test(
        &self,
        _removed_block_count: usize,
    ) -> VfsResult<()> {
        Ok(())
    }

    fn heal_orphaned_dentry(&self, parent: Ino, name: &[u8], ino_hint: Option<Ino>) {
        match self.sync_meta().remove_entry(parent, name) {
            Ok(removed) => {
                debug!(
                    parent,
                    removed_ino = removed.ino,
                    "healed orphaned dentry after inode-not-found"
                );
            }
            Err(MetaError::EntryNotFound { .. }) => {
                debug!(parent, "orphaned dentry already absent during heal");
            }
            Err(err) => {
                warn!(
                    parent,
                    name = %String::from_utf8_lossy(name),
                    error = %err,
                    "failed to heal orphaned dentry after inode-not-found"
                );
            }
        }

        if let Some(ino) = ino_hint {
            self.clear_pending_payloads_for_inode(ino);
            self.clear_inode_incarnation(ino);
            self.state.namespace.dirty_attrs.lock().remove(&ino);
            self.invalidate_attr_read_cache([ino]);
        }

        self.bump_namespace_for_parents([parent]);
        self.dentry_cache_insert_negative(parent, name);
    }

    /// Heal a directory entry whose name still exists in metadata but whose inode vanished.
    ///
    /// Returns `true` when the dangling dentry was detected and repaired, and
    /// `false` when there was no repairable dangling reference.
    pub fn heal_dangling_entry_if_inode_missing(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<bool> {
        let entry = match self.sync_meta().lookup(parent, name) {
            Ok(entry) => entry,
            Err(MetaError::EntryNotFound { .. }) => return Ok(false),
            Err(err) => return Err(VfsError::Meta(err)),
        };

        match self.sync_meta().get_attr(entry.ino) {
            Ok(_) => Ok(false),
            Err(MetaError::InodeNotFound(_)) => {
                self.heal_orphaned_dentry(parent, name, Some(entry.ino));
                Ok(true)
            }
            Err(err) => Err(VfsError::Meta(err)),
        }
    }

    pub(super) fn lookup_component_impl(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        self.enforce_access(parent, AccessLevel::ReadOnly)?;
        if parent == super::ROOT_INO && super::is_probable_trash_component(name) {
            self.heal_dangling_entry_if_inode_missing(parent, name)?;
            self.dentry_cache_insert_negative(parent, name);
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        if let Some(ignored) = self.ignored_entry_lookup(parent, name) {
            return Ok(ignored.attr);
        }
        if let Some(attr) = self.deferred_namespace_lookup(parent, name) {
            self.enforce_access(attr.ino, AccessLevel::ReadOnly)?;
            return Ok(attr);
        }
        if self.is_blocked_root_metadata_path(parent, name) {
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        if self.ignored_dir_attr(parent).is_some() {
            return Err(Self::ignored_entry_not_found(parent, name));
        }
        if let Some(cached) = self.dentry_cache_lookup(parent, name) {
            match cached {
                super::DentryCacheValue::Positive(child_ino) => {
                    match self.getattr_unchecked(child_ino) {
                        Ok(attr) => {
                            self.enforce_access(child_ino, AccessLevel::ReadOnly)?;
                            return Ok(attr);
                        }
                        Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                            self.dentry_cache_invalidate(parent, name);
                            self.heal_orphaned_dentry(parent, name, Some(child_ino));
                            return Err(Self::ignored_entry_not_found(parent, name));
                        }
                        Err(err) => return Err(err),
                    }
                }
                super::DentryCacheValue::Negative => {
                    return Err(Self::ignored_entry_not_found(parent, name));
                }
            }
        }
        let parent_is_uncommitted_local_dir = {
            let inner = self.state.namespace.deferred.inner.lock();
            inner.locally_created_dirs.contains(&parent) && inner.attrs.contains_key(&parent)
        };
        if parent_is_uncommitted_local_dir {
            self.dentry_cache_insert_negative(parent, name);
            return Err(Self::ignored_entry_not_found(parent, name));
        }

        let observed = self.namespace_cache_token_for_parent(parent);
        trace!(
            target: "studiofs_vfs::metadata_baseline::metadata_lookup",
            "VFS namespace metadata lookup"
        );
        let entry = match self.sync_meta().lookup(parent, name) {
            Ok(entry) => entry,
            Err(MetaError::EntryNotFound { .. }) => {
                self.dentry_cache_insert_observed(
                    parent,
                    name,
                    DentryCacheValue::Negative,
                    observed,
                );
                return Err(Self::ignored_entry_not_found(parent, name));
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };
        self.dentry_cache_insert_observed(
            parent,
            name,
            DentryCacheValue::Positive(entry.ino),
            observed,
        );
        if let Err(err) = self.enforce_access(entry.ino, AccessLevel::ReadOnly) {
            if matches!(err, VfsError::Meta(MetaError::InodeNotFound(_))) {
                self.heal_orphaned_dentry(parent, name, Some(entry.ino));
                return Err(Self::ignored_entry_not_found(parent, name));
            }
            return Err(err);
        }
        match self.getattr_unchecked(entry.ino) {
            Ok(attr) => Ok(attr),
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                self.heal_orphaned_dentry(parent, name, Some(entry.ino));
                Err(Self::ignored_entry_not_found(parent, name))
            }
            Err(err) => Err(err),
        }
    }

    /// Resolve `name` in `parent` using POSIX namespace semantics.
    ///
    /// Returns the resolved `InodeAttr` or a metadata error when the entry is
    /// absent or blocked.
    pub fn lookup(&self, parent: Ino, name: &[u8]) -> VfsResult<InodeAttr> {
        if self.is_runtime_stats_file_name(parent, name) {
            return Ok(self.runtime_stats_file_attr());
        }
        self.namespace_resolver(NamespacePathStyle::Posix)
            .lookup_component(parent, name)
    }

    fn readdir_entries_and_attrs(
        &self,
        ino: Ino,
    ) -> VfsResult<(Vec<DirEntry>, HashMap<Ino, InodeAttr>)> {
        self.enforce_metadata_serving_lifecycle()?;
        let parent_effective = self.effective_access_level(ino)?;
        if parent_effective == AccessLevel::NoAccess {
            return Ok((Vec::new(), HashMap::new()));
        }
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if self.ignored_dir_attr(ino).is_some() {
            return Ok((Vec::new(), HashMap::new()));
        }

        let meta = self.sync_meta();
        let observed = self.namespace_cache_token_for_parent(ino);
        trace!(
            target: "studiofs_vfs::metadata_baseline::readdir",
            "VFS readdir"
        );
        let mut entries = meta.readdir(ino)?;
        if ino == super::ROOT_INO {
            entries.retain(|entry| {
                !super::is_blocked_root_macos_metadata(&entry.name)
                    && !super::is_probable_trash_component(&entry.name)
            });
        }

        let existing_names = entries
            .iter()
            .map(|entry| entry.name.clone())
            .collect::<HashSet<_>>();
        let mut confirmed_entry_inos = HashSet::new();
        let mut attrs_by_ino = HashMap::new();
        {
            let inner = self.state.namespace.deferred.inner.lock();
            for (key, child_ino) in &inner.entries {
                if key.parent != ino || existing_names.contains(key.name.as_ref()) {
                    continue;
                }
                if let Some(attr) = inner.attrs.get(child_ino) {
                    entries.push(DirEntry {
                        name: key.name.as_ref().to_vec(),
                        ino: *child_ino,
                        kind: attr.kind,
                    });
                    attrs_by_ino.insert(*child_ino, attr.clone());
                    confirmed_entry_inos.insert(*child_ino);
                }
            }
        }
        if !entries.is_empty() {
            let child_inos = entries
                .iter()
                .map(|entry| entry.ino)
                .take(self.readdir_attr_batch_max_inodes)
                .collect::<Vec<_>>();
            let attr_observations = {
                let attr_cache = self.state.namespace.attr_read_cache.lock();
                entries
                    .iter()
                    .map(|entry| (entry.ino, attr_cache.observation(entry.ino)))
                    .collect::<HashMap<_, _>>()
            };
            let mut attrs = Vec::new();
            let attr_batch_len = self
                .readdir_attr_batch_chunk
                .min(self.readdir_attr_batch_max_inodes)
                .max(1);
            for chunk in child_inos.chunks(attr_batch_len) {
                trace!(
                    target: "studiofs_vfs::metadata_baseline::get_attrs_batch",
                    "VFS batched attribute lookup"
                );
                attrs.extend(meta.get_attrs_batch(chunk)?);
            }
            attrs_by_ino = attrs
                .into_iter()
                .map(|attr| (attr.ino, attr))
                .collect::<HashMap<_, _>>();
            confirmed_entry_inos.extend(attrs_by_ino.keys().copied());
            let now = Instant::now();
            let mut attr_cache = self.state.namespace.attr_read_cache.lock();
            for entry in &entries {
                if let Some(attr) = attrs_by_ino.get(&entry.ino) {
                    if let Some(observation) = attr_observations.get(&entry.ino).copied() {
                        attr_cache.put_observed(
                            entry.ino,
                            attr.clone(),
                            ATTR_CACHE_TTL,
                            now,
                            observation,
                        );
                    }
                } else {
                    attr_cache.remove(entry.ino);
                }
            }
            drop(attr_cache);
        }

        let mut cache_updates: Vec<(Ino, AccessLevel)> = Vec::with_capacity(entries.len());
        entries.retain(|entry| match self.acl_rule_for_inode(entry.ino) {
            Ok(Some(level)) => {
                cache_updates.push((entry.ino, level));
                level != AccessLevel::NoAccess
            }
            Ok(None) => {
                cache_updates.push((entry.ino, parent_effective));
                parent_effective != AccessLevel::NoAccess
            }
            Err(_) => false,
        });
        for (entry_ino, level) in cache_updates {
            self.effective_access_level_cache_insert(entry_ino, level);
        }

        let mut needs_generation_bump = false;
        for entry in &entries {
            if !confirmed_entry_inos.contains(&entry.ino) {
                continue;
            }
            match self.dentry_cache_lookup(ino, &entry.name) {
                Some(DentryCacheValue::Positive(cached_ino)) if cached_ino == entry.ino => {}
                Some(_) => {
                    needs_generation_bump = true;
                    break;
                }
                None => {}
            }
        }
        if needs_generation_bump {
            self.bump_directory_generation(ino);
        }
        for entry in &entries {
            if !confirmed_entry_inos.contains(&entry.ino) {
                continue;
            }
            self.dentry_cache_insert_observed(
                ino,
                &entry.name,
                DentryCacheValue::Positive(entry.ino),
                observed,
            );
        }

        let file_children: Vec<DirAccessEntry<'_>> = entries
            .iter()
            .filter(|e| e.kind == FileType::RegularFile)
            .map(|e| DirAccessEntry::new(e.ino, &e.name))
            .collect();
        if !file_children.is_empty() {
            self.dir_prefetch.on_readdir_entries(ino, file_children);
        }

        Ok((entries, attrs_by_ino))
    }

    /// Return the coherence observation for a bridge-side directory snapshot.
    pub fn directory_enumeration_observation(
        &self,
        ino: Ino,
    ) -> VfsResult<super::DirectoryEnumerationObservation> {
        let access_level = self.effective_access_level(ino)?;
        let namespace = self.namespace_cache_token_for_parent(ino);
        Ok(super::DirectoryEnumerationObservation {
            namespace_epoch: namespace.namespace_epoch,
            directory_generation: namespace.parent_generation,
            attribute_generation: self
                .state
                .namespace
                .attribute_generation
                .load(Ordering::Acquire),
            acl_epoch: self.state.acl.acl_epoch.load(Ordering::Acquire),
            acl_namespace_epoch: self.state.acl.acl_namespace_epoch.load(Ordering::Acquire),
            access_level: access_level as u8,
        })
    }

    /// Read directory entries from `ino` with namespace filtering and access checks.
    ///
    /// The method applies ACL filtering, skips inaccessible entries, drops platform/root
    /// blocked entries when necessary, and hydrates the attribute cache.
    pub fn readdir(&self, ino: Ino) -> VfsResult<Vec<DirEntry>> {
        let (entries, _) = self.readdir_entries_and_attrs(ino)?;
        Ok(entries)
    }

    /// Read directory entries and child attributes from the same metadata batch.
    pub fn readdir_with_attrs(&self, ino: Ino) -> VfsResult<Vec<super::DirEntryWithAttr>> {
        let (entries, mut attrs_by_ino) = self.readdir_entries_and_attrs(ino)?;
        Ok(entries
            .into_iter()
            .map(|entry| {
                let attr = attrs_by_ino.remove(&entry.ino);
                super::DirEntryWithAttr { entry, attr }
            })
            .collect())
    }

    fn heal_dangling_directory_children(&self, dir_ino: Ino) -> VfsResult<(usize, Vec<Ino>)> {
        let meta = self.sync_meta();
        let entries = meta.readdir(dir_ino)?;
        if entries.is_empty() {
            return Ok((0, Vec::new()));
        }

        let inflight_delete_inos = entries
            .iter()
            .filter(|entry| self.state.file_session.has_delete_inflight(entry.ino))
            .map(|entry| entry.ino)
            .collect::<Vec<_>>();
        let child_inos = entries.iter().map(|entry| entry.ino).collect::<Vec<_>>();
        let mut live_inos = HashSet::new();
        let attr_batch_len = self
            .readdir_attr_batch_chunk
            .min(self.readdir_attr_batch_max_inodes)
            .max(1);
        for chunk in child_inos.chunks(attr_batch_len) {
            live_inos.extend(
                meta.get_attrs_batch(chunk)?
                    .into_iter()
                    .map(|attr| attr.ino),
            );
        }

        let mut healed = 0;
        for entry in entries {
            if live_inos.contains(&entry.ino) {
                continue;
            }
            self.heal_orphaned_dentry(dir_ino, &entry.name, Some(entry.ino));
            healed += 1;
        }
        Ok((healed, inflight_delete_inos))
    }

    fn resolve_unlink_target(
        &self,
        meta: &Arc<dyn VfsMeta>,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<UnlinkTarget> {
        let entry = meta.lookup(parent, name)?;
        Ok(UnlinkTarget {
            ino: entry.ino,
            open_handles: self.handle_table.open_count(entry.ino),
        })
    }

    fn unlink_with_open_handles(
        &self,
        meta: &Arc<dyn VfsMeta>,
        parent: Ino,
        name: &[u8],
        target: UnlinkTarget,
    ) -> VfsResult<Option<InodeAttr>> {
        let attr = match meta.unlink_open_handle(parent, name) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => {
                self.heal_orphaned_dentry(parent, name, Some(target.ino));
                return Ok(None);
            }
            Err(MetaError::IsADirectory(_)) => {
                return Err(VfsError::Meta(MetaError::IsADirectory(target.ino)));
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };
        if attr.nlink == 0 {
            self.retire_os_cache_inode(attr.ino);
            self.mark_inode_deleted_for_uploads(attr.ino);
        }

        self.handle_table.mark_orphan(target.ino);
        debug!(
            "unlink ino={}: {} open handles, deferred cleanup (orphan)",
            target.ino, target.open_handles
        );
        Ok(Some(attr))
    }

    async fn unlink_without_open_handles(
        &self,
        meta: &Arc<dyn VfsMeta>,
        parent: Ino,
        name: &[u8],
        target: UnlinkTarget,
    ) -> VfsResult<Option<InodeAttr>> {
        let pre_unlink_object_ids = self
            .regular_file_graph_usage(meta, target.ino)
            .map(|usage| usage.object_ids)
            .unwrap_or_default();
        let attr = match meta.unlink(parent, name) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => {
                self.heal_orphaned_dentry(parent, name, Some(target.ino));
                return Ok(None);
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };

        if attr.nlink == 0 {
            self.retire_os_cache_inode(attr.ino);
            self.cleanup_deleted_inode_storage(
                meta,
                attr.ino,
                attr.size,
                "unlink",
                pre_unlink_object_ids,
            )
            .await?;
        } else {
            info!(
                "unlink ino={}: nlink={}, keeping inode alive (hardlink)",
                attr.ino, attr.nlink
            );
        }

        Ok(Some(attr))
    }

    /// Create a new hard link named `new_name` under `new_parent` for `ino`.
    ///
    /// In ignored-path mode, this records a synthetic ignored entry so namespace
    /// behavior remains consistent while avoiding metadata mutations.
    pub fn link(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr> {
        Self::run_sync_future(self.link_inner(ino, new_parent, new_name))
    }

    async fn link_inner(&self, ino: Ino, new_parent: Ino, new_name: &[u8]) -> VfsResult<InodeAttr> {
        let namespace_mutations = self.namespace_mutations();
        if self.is_runtime_stats_file_ino(ino) {
            return Err(Self::runtime_stats_access_denied_error());
        }
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        self.enforce_access(new_parent, AccessLevel::ReadWrite)?;
        validate_name(new_name)?;
        self.reject_runtime_stats_entry_creation(new_parent, new_name)?;
        if self.should_ignore_regular_file_path(new_parent, new_name) {
            let source_attr = self.getattr(ino)?;
            let ignored = self.create_ignored_entry(
                new_parent,
                new_name,
                source_attr.uid,
                source_attr.gid,
                source_attr.mode,
                FileType::RegularFile,
            );
            let ignored = self
                .update_ignored_entry(ignored.ino, |attr| {
                    attr.size = source_attr.size;
                    attr.committed_size = source_attr.committed_size;
                    attr.atime = source_attr.atime;
                    attr.mtime = source_attr.mtime;
                    attr.ctime = source_attr.ctime;
                    attr.crtime = source_attr.crtime;
                })
                .unwrap_or(ignored);
            namespace_mutations.record_ignored_insertion(new_parent, std::iter::empty());
            return Ok(ignored);
        }
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let result = (|| {
            // `sync_meta().link` only knows the committed metadata snapshot.
            // Serialize the merge with writes, release, and other links, then
            // preserve the effective local-first size/epoch fields.
            let mut visible_attr = self.getattr(ino)?;
            let attr = self.sync_meta().link(ino, new_parent, new_name)?;
            namespace_mutations.record_insertion(new_parent, new_name, ino, [ino]);
            self.bump_acl_namespace_epoch();
            visible_attr.nlink = attr.nlink;
            visible_attr.ctime = attr.ctime;
            if let Some(dirty_attr) = self.state.namespace.dirty_attrs.lock().get_mut(&ino) {
                dirty_attr.nlink = attr.nlink;
                dirty_attr.ctime = attr.ctime;
            }
            self.invalidate_attr_read_cache([ino]);
            self.state.namespace.attr_read_cache.lock().put(
                ino,
                visible_attr.clone(),
                ATTR_CACHE_TTL,
                Instant::now(),
            );
            self.sync_meta().request_eager_flush();
            Ok(visible_attr)
        })();
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Create a regular file under `parent`.
    ///
    /// Applies name validation, handles ignored-path routing, sets up the initial
    /// incarnation state for upload, and returns the created inode attributes.
    pub fn create(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Create);
        self.create_with_metadata_flush(parent, name, uid, gid, mode, true)
    }

    /// Create a regular file without forcing a foreground metadata flush.
    pub fn create_deferred_metadata_flush(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Create);
        self.create_with_metadata_flush(parent, name, uid, gid, mode, false)
    }

    fn create_with_metadata_flush(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        request_eager_flush: bool,
    ) -> VfsResult<InodeAttr> {
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        self.reject_runtime_stats_entry_creation(parent, name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr =
                self.create_ignored_entry(parent, name, uid, gid, mode, FileType::RegularFile);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        let attr = self.observe_metadata_commit(|| {
            let ino = self.reserve_namespace_ino()?;
            self.observe_namespace_mutation(|| {
                self.sync_meta()
                    .create_file_with_ino(ino, parent, name, uid, gid, mode & 0o7777)
            })
        })?;
        self.record_namespace_create_completed();
        self.state.incarnation.inode_incarnations.lock().insert(
            attr.ino,
            InodeIncarnationState {
                committed_incarnation: None,
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: attr.content_epoch,
                    size: attr.size,
                    base_size: 0,
                }),
                upload_state: VfsUploadState::Uploading,
            },
        );
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        if request_eager_flush {
            self.sync_meta().request_eager_flush();
        }
        Ok(attr)
    }

    fn reject_existing_deferred_or_persisted_entry(
        &self,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<()> {
        if self.deferred_namespace_lookup(parent, name).is_some() {
            return Err(VfsError::Meta(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).into_owned(),
            }));
        }
        if let Some(cached) = self.dentry_cache_lookup(parent, name) {
            return match cached {
                DentryCacheValue::Negative => Ok(()),
                DentryCacheValue::Positive(_) => Err(VfsError::Meta(MetaError::EntryExists {
                    parent,
                    name: String::from_utf8_lossy(name).into_owned(),
                })),
            };
        }
        if self
            .state
            .namespace
            .deferred
            .inner
            .lock()
            .locally_created_dirs
            .contains(&parent)
        {
            return Ok(());
        }
        match self.sync_meta().lookup(parent, name) {
            Ok(_) => Err(VfsError::Meta(MetaError::EntryExists {
                parent,
                name: String::from_utf8_lossy(name).into_owned(),
            })),
            Err(MetaError::EntryNotFound { .. }) => Ok(()),
            Err(error) => Err(VfsError::Meta(error)),
        }
    }

    /// Creates a file in the in-memory namespace and defers the metadata commit.
    pub fn create_with_deferred_metadata_admission(
        self: &Arc<Self>,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        runtime: &tokio::runtime::Handle,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Create);
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.ensure_no_deferred_namespace_error(parent)?;
        validate_name(name)?;
        self.reject_runtime_stats_entry_creation(parent, name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr =
                self.create_ignored_entry(parent, name, uid, gid, mode, FileType::RegularFile);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        self.reject_existing_deferred_or_persisted_entry(parent, name)?;
        self.ensure_deferred_namespace_admission(
            std::mem::size_of::<DeferredNamespaceOp>() + name.len(),
        )?;

        let ino = self.reserve_namespace_ino()?;
        let attr = InodeAttr::new_file(ino, parent, uid, gid, mode & 0o7777);
        let enqueue_result = self.enqueue_deferred_namespace_op(
            Some(DeferredNamespaceKey::new(parent, name)),
            attr.clone(),
            DeferredNamespaceOp::CreateFile {
                attr: attr.clone(),
                parent,
                name: name.to_vec(),
                uid,
                gid,
                mode: mode & 0o7777,
            },
        );
        if let Err(error) = enqueue_result {
            self.return_reserved_namespace_ino(attr.ino);
            return Err(error);
        }
        self.state.incarnation.inode_incarnations.lock().insert(
            attr.ino,
            InodeIncarnationState {
                committed_incarnation: None,
                pending_incarnation: Some(PendingIncarnation {
                    content_epoch: attr.content_epoch,
                    size: attr.size,
                    base_size: 0,
                }),
                upload_state: VfsUploadState::Uploading,
            },
        );
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        self.schedule_deferred_namespace_metadata_drain(runtime);
        Ok(attr)
    }

    /// Create a directory under `parent`.
    ///
    /// Applies name validation, handles ignored-path routing, records namespace
    /// mutations, and returns the created directory attributes.
    pub fn mkdir(
        &self,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Mkdir);
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        self.reject_runtime_stats_entry_creation(parent, name)?;
        if self.should_ignore_directory_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, mode, FileType::Directory);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        let attr = self.observe_metadata_commit(|| {
            let ino = self.reserve_namespace_ino()?;
            self.observe_namespace_mutation(|| {
                self.sync_meta()
                    .create_dir_with_ino(ino, parent, name, uid, gid, mode & 0o7777)
            })
        })?;
        self.record_namespace_create_completed();
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .locally_created_dirs
            .insert(attr.ino);
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Creates a directory in the in-memory namespace and defers the metadata commit.
    pub fn mkdir_with_deferred_metadata_admission(
        self: &Arc<Self>,
        parent: Ino,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        runtime: &tokio::runtime::Handle,
    ) -> VfsResult<InodeAttr> {
        let _timer = VfsForegroundWriteTimer::new(self, VfsForegroundWriteOp::Mkdir);
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.ensure_no_deferred_namespace_error(parent)?;
        validate_name(name)?;
        self.reject_runtime_stats_entry_creation(parent, name)?;
        if self.should_ignore_directory_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, mode, FileType::Directory);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        self.reject_existing_deferred_or_persisted_entry(parent, name)?;
        self.ensure_deferred_namespace_admission(
            std::mem::size_of::<DeferredNamespaceOp>() + name.len(),
        )?;

        let ino = self.reserve_namespace_ino()?;
        let attr = InodeAttr::new_dir(ino, parent, uid, gid, mode & 0o7777);
        let enqueue_result = self.enqueue_deferred_namespace_op(
            Some(DeferredNamespaceKey::new(parent, name)),
            attr.clone(),
            DeferredNamespaceOp::CreateDir {
                attr: attr.clone(),
                parent,
                name: name.to_vec(),
                uid,
                gid,
                mode: mode & 0o7777,
            },
        );
        if let Err(error) = enqueue_result {
            self.return_reserved_namespace_ino(attr.ino);
            return Err(error);
        }
        self.state
            .namespace
            .deferred
            .inner
            .lock()
            .locally_created_dirs
            .insert(attr.ino);
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        self.schedule_deferred_namespace_metadata_drain(runtime);
        Ok(attr)
    }

    /// Create a symlink entry under `parent` named `name` pointing to `target`.
    pub fn symlink(
        &self,
        parent: Ino,
        name: &[u8],
        target: &[u8],
        uid: u32,
        gid: u32,
    ) -> VfsResult<InodeAttr> {
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        self.reject_runtime_stats_entry_creation(parent, name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, 0o777, FileType::Symlink);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_symlink(parent, name, target, uid, gid)?;
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        self.sync_meta().request_eager_flush();
        Ok(attr)
    }

    /// Read the symlink target bytes for `ino`.
    pub fn readlink(&self, ino: Ino) -> VfsResult<Vec<u8>> {
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if matches!(self.ignored_entry_attr(ino), Some(attr) if attr.kind == FileType::Symlink) {
            return Ok(Vec::new());
        }
        Ok(self.sync_meta().get_symlink_target(ino)?)
    }

    /// Unlink the entry `name` from directory `parent`.
    pub fn unlink(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        Self::run_sync_future(self.unlink_inner(parent, name)).map(|_| ())
    }

    /// Unlink the entry `name` and return the removed inode attributes when
    /// the entry came from durable metadata.
    pub fn unlink_with_attr(&self, parent: Ino, name: &[u8]) -> VfsResult<Option<InodeAttr>> {
        Self::run_sync_future(self.unlink_inner(parent, name))
    }

    async fn unlink_inner(&self, parent: Ino, name: &[u8]) -> VfsResult<Option<InodeAttr>> {
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.reject_runtime_stats_entry_mutation(parent, name)?;
        if self.unlink_ignored_entry(parent, name) || self.ignored_dir_attr(parent).is_some() {
            namespace_mutations.record_removal(parent, name, std::iter::empty());
            return Ok(None);
        }

        let meta = self.sync_meta();
        // Serialize namespace mutations targeting one directory. macFUSE can
        // dispatch the child unlink callbacks concurrently with rmdir; keeping
        // the parent session visible lets rmdir wait for callbacks already
        // queued behind the metadata transaction.
        let parent_delete_session = self
            .state
            .file_session
            .enter_op(parent, FileSessionOperation::Delete)
            .await;
        self.wait_for_deferred_namespace_entry(parent, name).await?;
        let initial_target = self.resolve_unlink_target(&meta, parent, name)?;
        let delete_session = self
            .state
            .file_session
            .enter_op(initial_target.ino, FileSessionOperation::Delete)
            .await;
        let result = async {
            let mut unlink_target = self.resolve_unlink_target(&meta, parent, name)?;
            self.enforce_no_known_foreign_hard_lock(unlink_target.ino)?;
            unlink_target.open_handles = self.handle_table.open_count(unlink_target.ino);
            let Some(unlinked_attr) = (if unlink_target.open_handles > 0 {
                self.unlink_with_open_handles(&meta, parent, name, unlink_target)?
            } else {
                self.unlink_without_open_handles(&meta, parent, name, unlink_target)
                    .await?
            }) else {
                return Ok(None);
            };

            namespace_mutations.record_removal(parent, name, [unlinked_attr.ino]);
            self.invalidate_attr_read_cache([unlinked_attr.ino]);
            if unlinked_attr.nlink > 0 {
                self.state.namespace.attr_read_cache.lock().put(
                    unlinked_attr.ino,
                    unlinked_attr.clone(),
                    ATTR_CACHE_TTL,
                    Instant::now(),
                );
            }
            // Keep ordinary namespace deletes local-first. The metadata
            // engine batches this remote-visible event through its normal
            // group-commit window; explicit barriers remain responsible for
            // callers that require immediate publication.
            Ok(Some(unlinked_attr))
        }
        .await;
        drop(delete_session);
        self.state.file_session.gc_if_idle(initial_target.ino);
        drop(parent_delete_session);
        self.state.file_session.gc_if_idle(parent);
        result
    }

    /// Remove an empty directory `name` from directory `parent`.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        Self::run_sync_future(self.rmdir_inner(parent, name))
    }

    /// Retry a cleanup-triggered directory removal while child callbacks catch up.
    pub fn rmdir_for_cleanup(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        Self::run_sync_future(self.rmdir_for_cleanup_inner(parent, name))
    }

    async fn rmdir_for_cleanup_inner(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        for attempt in 0..RMDIR_CLEANUP_RETRY_ATTEMPTS {
            match self.rmdir_inner(parent, name).await {
                Err(VfsError::Meta(MetaError::DirNotEmpty(_)))
                    if attempt + 1 < RMDIR_CLEANUP_RETRY_ATTEMPTS =>
                {
                    tokio::time::sleep(RMDIR_INFLIGHT_RETRY_DELAY).await;
                }
                result => return result,
            }
        }
        Err(VfsError::Other(
            "directory cleanup retry loop exhausted without a result".to_string(),
        ))
    }

    async fn rmdir_inner(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.reject_runtime_stats_entry_mutation(parent, name)?;
        if self.unlink_ignored_subtree(parent, name) || self.ignored_dir_attr(parent).is_some() {
            namespace_mutations.record_removal(parent, name, std::iter::empty());
            return Ok(());
        }
        // Serialize directory removal with child namespace mutations. A
        // nested rmdir holds this parent gate while it waits for its own
        // children, so an outer rmdir can observe and wait for that work.
        let _parent_delete_session = self
            .state
            .file_session
            .enter_op(parent, FileSessionOperation::Delete)
            .await;
        self.wait_for_deferred_namespace_entry(parent, name).await?;
        let meta = self.sync_meta();
        let removed = meta.lookup(parent, name).ok().map(|entry| entry.ino);
        let mut retry_attempts: u16 = 0;
        let mut settle_attempts: u16 = 0;
        loop {
            let rmdir_result = if let Some(removed_ino) = removed {
                // A child unlink/rmdir takes the target directory's Delete
                // gate. Hold its fair Read barrier across the emptiness
                // transaction so an already queued child mutation cannot
                // slip between the in-flight check and meta.rmdir.
                let _target_namespace_read = self
                    .state
                    .file_session
                    .enter_op(removed_ino, FileSessionOperation::Read)
                    .await;
                meta.rmdir(parent, name)
            } else {
                meta.rmdir(parent, name)
            };
            match rmdir_result {
                Ok(()) => break,
                Err(err @ MetaError::DirNotEmpty(_)) => {
                    let Some(removed_ino) = removed else {
                        return Err(VfsError::Meta(err));
                    };
                    let (healed, child_delete_inos) =
                        self.heal_dangling_directory_children(removed_ino)?;
                    if healed > 0 {
                        debug!(ino = removed_ino, healed, "rmdir healed dangling children");
                        continue;
                    }
                    if let Some(delete_ino) = child_delete_inos.into_iter().next().or_else(|| {
                        self.state
                            .file_session
                            .has_delete_inflight(removed_ino)
                            .then_some(removed_ino)
                    }) {
                        retry_attempts = retry_attempts.saturating_add(1);
                        self.state
                            .file_session
                            .wait_for_delete_inflight(delete_ino)
                            .await;
                        continue;
                    } else {
                        // A child delete can finish between the failed
                        // rmdir transaction and this check. Give the local
                        // metadata view a short settling window before
                        // returning ENOTEMPTY; a first, ordinary rmdir with
                        // no observed child mutation remains immediate.
                        if retry_attempts == 0 || settle_attempts >= RMDIR_SETTLE_RETRY_ATTEMPTS {
                            return Err(VfsError::Meta(err));
                        }
                        settle_attempts += 1;
                    }
                    debug!(
                        ino = removed_ino,
                        retry_attempt = retry_attempts,
                        settle_attempt = settle_attempts,
                        "rmdir waiting for in-flight child delete"
                    );
                    tokio::time::sleep(RMDIR_INFLIGHT_RETRY_DELAY).await;
                }
                Err(err) => return Err(VfsError::Meta(err)),
            }
        }
        namespace_mutations.record_removal(parent, name, std::iter::empty());
        if let Some(removed_ino) = removed {
            self.bump_directory_generation(removed_ino);
        }
        // Directory removals follow the same local-first group-commit path as
        // child unlinks instead of waking PostgreSQL once per entry.
        Ok(())
    }

    /// Atomically rename `name` in `parent` to `new_name` in `new_parent`.
    pub fn rename(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        Self::run_sync_future(self.rename_inner(parent, name, new_parent, new_name))
    }

    async fn delete_source_tree_and_invalidate_trash_target(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
        attr: InodeAttr,
    ) -> VfsResult<()> {
        info!(
            parent,
            name = %String::from_utf8_lossy(name),
            source_ino = attr.ino,
            new_parent,
            new_name = %String::from_utf8_lossy(new_name),
            source_kind = ?attr.kind,
            "VFS trash rename is destructive and non-atomic; deleting source tree instead of preserving a trash entry"
        );
        let mut stack = vec![(parent, name.to_vec(), attr.ino, attr.kind, false)];
        while let Some((current_parent, current_name, current_ino, current_kind, visited)) =
            stack.pop()
        {
            if current_kind != FileType::Directory {
                self.unlink_inner(current_parent, &current_name).await?;
                continue;
            }

            if visited {
                self.rmdir_inner(current_parent, &current_name).await?;
                continue;
            }

            stack.push((
                current_parent,
                current_name,
                current_ino,
                current_kind,
                true,
            ));
            for entry in self.sync_meta().readdir(current_ino)? {
                stack.push((current_ino, entry.name, entry.ino, entry.kind, false));
            }
        }
        self.invalidate_attr_read_cache([new_parent]);
        self.bump_namespace_for_parents([new_parent]);
        self.dentry_cache_insert_negative(new_parent, new_name);
        info!(
            parent,
            name = %String::from_utf8_lossy(name),
            source_ino = attr.ino,
            new_parent,
            new_name = %String::from_utf8_lossy(new_name),
            "VFS trash rename destructive delete completed"
        );
        Ok(())
    }

    async fn rename_inner(
        &self,
        parent: Ino,
        name: &[u8],
        new_parent: Ino,
        new_name: &[u8],
    ) -> VfsResult<()> {
        let rename_started = Instant::now();
        let name_display = String::from_utf8_lossy(name);
        let new_name_display = String::from_utf8_lossy(new_name);
        debug!(
            parent,
            name = %name_display,
            new_parent,
            new_name = %new_name_display,
            "VFS rename started"
        );
        let namespace_mutations = self.namespace_mutations();
        let meta = self.sync_meta();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        self.enforce_access(new_parent, AccessLevel::ReadWrite)?;
        if parent == new_parent && name == new_name {
            debug!(
                parent,
                name = %name_display,
                new_parent,
                new_name = %new_name_display,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename skipped identical source and target"
            );
            return Ok(());
        }
        validate_name(new_name)?;
        self.reject_runtime_stats_entry_mutation(parent, name)?;
        self.reject_runtime_stats_entry_mutation(new_parent, new_name)?;
        let source_ignored = self.ignored_entry_lookup(parent, name);
        let target_is_ignored = self.should_ignore_regular_file_path(new_parent, new_name)
            || self.should_ignore_directory_path(new_parent, new_name);
        let target_is_ignored_trash = self.ignored_dir_attr(new_parent).is_some()
            && self.ignored_directory_has_probable_trash_ancestor(new_parent);
        if let Some(source) = source_ignored {
            if target_is_ignored && !target_is_ignored_trash {
                self.create_ignored_entry(
                    new_parent,
                    new_name,
                    source.attr.uid,
                    source.attr.gid,
                    source.attr.mode,
                    source.attr.kind,
                );
            }
            self.unlink_ignored_subtree(parent, name);
            self.bump_acl_namespace_epoch();
            namespace_mutations.record_ignored_rename(parent, name, new_parent, std::iter::empty());
            if target_is_ignored_trash {
                self.dentry_cache_insert_negative(new_parent, new_name);
            }
            debug!(
                parent,
                name = %name_display,
                new_parent,
                new_name = %new_name_display,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename completed for ignored source"
            );
            return Ok(());
        }
        if target_is_ignored {
            let source_entry = meta.lookup(parent, name)?;
            debug!(
                parent,
                name = %name_display,
                source_ino = source_entry.ino,
                new_parent,
                new_name = %new_name_display,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename target is ignored; unlinking source"
            );
            let source_attr = meta.get_attr(source_entry.ino)?;
            self.enforce_no_known_foreign_hard_lock(source_entry.ino)?;
            if target_is_ignored_trash {
                return self
                    .delete_source_tree_and_invalidate_trash_target(
                        parent,
                        name,
                        new_parent,
                        new_name,
                        source_attr,
                    )
                    .await;
            }
            self.bump_namespace_for_parents([new_parent]);
            let result = self.unlink_inner(parent, name).await;
            match &result {
                Ok(_) => debug!(
                    parent,
                    name = %name_display,
                    source_ino = source_entry.ino,
                    new_parent,
                    new_name = %new_name_display,
                    elapsed_ms = rename_started.elapsed().as_millis(),
                    "VFS rename completed via ignored target unlink"
                ),
                Err(err) => warn!(
                    parent,
                    name = %name_display,
                    source_ino = source_entry.ino,
                    new_parent,
                    new_name = %new_name_display,
                    elapsed_ms = rename_started.elapsed().as_millis(),
                    error = %err,
                    "VFS rename failed during ignored target unlink"
                ),
            }
            return result.map(|_| ());
        }
        debug!(
            parent,
            name = %name_display,
            new_parent,
            new_name = %new_name_display,
            elapsed_ms = rename_started.elapsed().as_millis(),
            "VFS rename looking up source"
        );
        self.wait_for_deferred_namespace_entry(parent, name).await?;
        self.wait_for_deferred_namespace_entry(new_parent, new_name)
            .await?;
        let source_entry = meta.lookup(parent, name)?;
        debug!(
            parent,
            name = %name_display,
            source_ino = source_entry.ino,
            new_parent,
            new_name = %new_name_display,
            elapsed_ms = rename_started.elapsed().as_millis(),
            "VFS rename checking source lock"
        );
        self.enforce_no_known_foreign_hard_lock(source_entry.ino)?;
        let source_attr = meta.get_attr(source_entry.ino)?;
        let target_is_trash = self.directory_has_probable_trash_ancestor(new_parent)?;
        if target_is_trash {
            debug!(
                parent,
                name = %name_display,
                source_ino = source_entry.ino,
                new_parent,
                new_name = %new_name_display,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename into trash-like directory completed via delete"
            );
            return self
                .delete_source_tree_and_invalidate_trash_target(
                    parent,
                    name,
                    new_parent,
                    new_name,
                    source_attr,
                )
                .await;
        }
        let displaced_target_will_retire = if let Ok(target_entry) =
            meta.lookup(new_parent, new_name)
            && target_entry.ino != source_entry.ino
        {
            debug!(
                parent,
                name = %name_display,
                source_ino = source_entry.ino,
                new_parent,
                new_name = %new_name_display,
                target_ino = target_entry.ino,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename checking displaced target lock"
            );
            self.enforce_no_known_foreign_hard_lock(target_entry.ino)?;
            let target_attr = meta.get_attr(target_entry.ino)?;
            target_attr.kind == FileType::Directory || target_attr.nlink <= 1
        } else {
            false
        };
        debug!(
            parent,
            name = %name_display,
            source_ino = source_entry.ino,
            new_parent,
            new_name = %new_name_display,
            elapsed_ms = rename_started.elapsed().as_millis(),
            "VFS rename calling metadata rename_atomic"
        );
        let rename_result = meta.rename_atomic(parent, name, new_parent, new_name)?;
        debug!(
            parent,
            name = %name_display,
            source_ino = source_entry.ino,
            new_parent,
            new_name = %new_name_display,
            moved_ino = rename_result.moved_ino,
            displaced_ino = ?rename_result.displaced_ino,
            elapsed_ms = rename_started.elapsed().as_millis(),
            "VFS rename metadata rename_atomic returned"
        );
        namespace_mutations.record_rename(
            parent,
            name,
            new_parent,
            new_name,
            rename_result.moved_ino,
        );
        self.bump_acl_namespace_epoch();

        if let Some(ino) = rename_result.displaced_ino {
            if displaced_target_will_retire {
                self.retire_os_cache_inode(ino);
            }
            debug!(
                parent,
                name = %name_display,
                new_parent,
                new_name = %new_name_display,
                displaced_ino = ino,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename collecting displaced target storage usage"
            );
            let displaced_usage = meta.get_regular_file_graph_object_usage(ino)?;
            let total_bytes = displaced_usage
                .iter()
                .fold(0_u64, |acc, (_, bytes)| acc.saturating_add(*bytes));
            let displaced_object_ids = displaced_usage
                .into_iter()
                .map(|(object_id, _)| object_id)
                .collect();

            debug!(
                parent,
                name = %name_display,
                new_parent,
                new_name = %new_name_display,
                displaced_ino = ino,
                total_bytes,
                elapsed_ms = rename_started.elapsed().as_millis(),
                "VFS rename cleaning displaced target storage"
            );
            self.cleanup_deleted_inode_storage_deferred_cache_eviction(
                &meta,
                ino,
                total_bytes,
                "rename overwrite",
                displaced_object_ids,
            )?;
            self.bump_directory_generation(ino);
            self.dentry_cache_invalidate_parent(ino);
        }

        meta.request_eager_flush();
        debug!(
            parent,
            name = %name_display,
            new_parent,
            new_name = %new_name_display,
            moved_ino = rename_result.moved_ino,
            displaced_ino = ?rename_result.displaced_ino,
            elapsed_ms = rename_started.elapsed().as_millis(),
            "VFS rename completed"
        );
        Ok(())
    }
}
