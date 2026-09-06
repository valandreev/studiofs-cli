use futures::StreamExt;
use opendal::ErrorKind;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use studiofs_meta::engine::MetadataCoherenceProgress;
use tokio::time::MissedTickBehavior;

use super::*;
use crate::coherence::InvalidationAcceptance;
use crate::vfs::upload_lifecycle::UploadLifecycleWorker;
use crate::{JanitorMeta, VfsMeta};

const JANITOR_RECONCILIATION_CONCURRENCY: usize = 32;

struct MetadataTraversalSnapshot {
    inodes: HashSet<Ino>,
    payloads: HashMap<Ino, Vec<u64>>,
    entries: HashSet<(Ino, Vec<u8>, Ino)>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SemanticInvalidationDrain {
    pub(super) had_batches: bool,
    pub(super) all_accepted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetadataInvalidationWakeReason {
    BackendNotify,
    FallbackPoll,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct UploadTroubleCounters {
    pub(super) uploads_ok: u64,
    pub(super) uploads_err: u64,
    pub(super) retries: u64,
    pub(super) exhausted: u64,
    pub(super) timeouts: u64,
    pub(super) temporary_errors: u64,
    pub(super) status_5xx_errors: u64,
    pub(super) network_errors: u64,
    pub(super) bytes_uploaded: u64,
}

impl UploadTroubleCounters {
    fn from_metrics(metrics: &crate::upload::UploadMetrics) -> Self {
        Self {
            uploads_ok: metrics.uploads_ok.load(Ordering::Relaxed),
            uploads_err: metrics.uploads_err.load(Ordering::Relaxed),
            retries: metrics.retries.load(Ordering::Relaxed),
            exhausted: metrics.exhausted.load(Ordering::Relaxed),
            timeouts: metrics.timeout_err.load(Ordering::Relaxed),
            temporary_errors: metrics.temporary_err.load(Ordering::Relaxed),
            status_5xx_errors: metrics.status_5xx_err.load(Ordering::Relaxed),
            network_errors: metrics.network_err.load(Ordering::Relaxed),
            bytes_uploaded: metrics.bytes_uploaded.load(Ordering::Relaxed),
        }
    }

    fn delta_since(self, previous: Self) -> Self {
        Self {
            uploads_ok: self.uploads_ok.saturating_sub(previous.uploads_ok),
            uploads_err: self.uploads_err.saturating_sub(previous.uploads_err),
            retries: self.retries.saturating_sub(previous.retries),
            exhausted: self.exhausted.saturating_sub(previous.exhausted),
            timeouts: self.timeouts.saturating_sub(previous.timeouts),
            temporary_errors: self
                .temporary_errors
                .saturating_sub(previous.temporary_errors),
            status_5xx_errors: self
                .status_5xx_errors
                .saturating_sub(previous.status_5xx_errors),
            network_errors: self.network_errors.saturating_sub(previous.network_errors),
            bytes_uploaded: self.bytes_uploaded.saturating_sub(previous.bytes_uploaded),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UploadTroubleSignal {
    None,
    Warn,
    Recovered,
}

#[derive(Debug, Default)]
pub(super) struct UploadTroubleState {
    previous: UploadTroubleCounters,
    active: bool,
}

impl UploadTroubleState {
    pub(super) fn observe(
        &mut self,
        current: UploadTroubleCounters,
        pending_uploads: usize,
        active_uploads: usize,
        pending_retry_count: u64,
    ) -> UploadTroubleSignal {
        let delta = current.delta_since(self.previous);
        self.previous = current;

        if upload_trouble_delta_indicates_remote_problem(delta) {
            self.active = true;
        }

        let has_backlog = pending_uploads != 0 || active_uploads != 0 || pending_retry_count != 0;
        if self.active && has_backlog {
            UploadTroubleSignal::Warn
        } else if self.active {
            self.active = false;
            UploadTroubleSignal::Recovered
        } else {
            UploadTroubleSignal::None
        }
    }

    #[cfg(test)]
    pub(super) fn is_active(&self) -> bool {
        self.active
    }
}

fn upload_trouble_delta_indicates_remote_problem(delta: UploadTroubleCounters) -> bool {
    delta.timeouts != 0
        || delta.network_errors != 0
        || delta.status_5xx_errors != 0
        || delta.exhausted != 0
        || (delta.uploads_err != 0 && delta.uploads_ok == 0)
}

fn epoch_finalization_retry_delay(
    status: &commit::EpochFinalizeStatus,
    upload_requeue_base: Duration,
) -> Option<Duration> {
    match status {
        commit::EpochFinalizeStatus::WaitingForUploads | commit::EpochFinalizeStatus::Deferred => {
            Some(upload_requeue_base.max(Duration::from_millis(25)))
        }
        commit::EpochFinalizeStatus::Published | commit::EpochFinalizeStatus::Skipped => None,
    }
}

fn should_run_invalidation_poll_sync(
    wake_reason: MetadataInvalidationWakeReason,
    last_namespace_token: &Option<String>,
    last_metadata_token: &Option<String>,
    current_namespace_token: &Option<String>,
    current_metadata_token: &Option<String>,
) -> bool {
    match wake_reason {
        MetadataInvalidationWakeReason::BackendNotify => {
            last_namespace_token == current_namespace_token
                && last_metadata_token == current_metadata_token
        }
        MetadataInvalidationWakeReason::FallbackPoll => true,
    }
}

fn metadata_attr_cache_invalidation_set(
    previous_snapshot: &HashSet<Ino>,
    current_snapshot: &HashSet<Ino>,
    namespace_changed: bool,
    runtime_inodes: HashSet<Ino>,
) -> HashSet<Ino> {
    let mut attr_cache_inodes = previous_snapshot.clone();
    attr_cache_inodes.extend(current_snapshot.iter().copied());
    if namespace_changed {
        attr_cache_inodes.extend(runtime_inodes);
    }
    attr_cache_inodes
}

fn metadata_token_changes(
    last_namespace_token: &Option<String>,
    last_metadata_token: &Option<String>,
    after_namespace_token: &Option<String>,
    after_metadata_token: &Option<String>,
) -> (bool, bool) {
    (
        last_namespace_token != after_namespace_token,
        last_metadata_token != after_metadata_token,
    )
}

fn metadata_namespace_changed(
    token_namespace_changed: bool,
    previous_snapshot: Option<&HashSet<Ino>>,
    current_snapshot: &HashSet<Ino>,
) -> bool {
    token_namespace_changed || previous_snapshot.is_none_or(|previous| previous != current_snapshot)
}

fn metadata_namespace_entry_changes(
    previous: Option<&HashSet<(Ino, Vec<u8>, Ino)>>,
    current: &HashSet<(Ino, Vec<u8>, Ino)>,
) -> HashSet<(Ino, Vec<u8>)> {
    let Some(previous) = previous else {
        return HashSet::new();
    };
    previous
        .symmetric_difference(current)
        .map(|(parent, name, _ino)| (*parent, name.clone()))
        .collect()
}

fn metadata_invalidation_fallback_timer(interval: Duration) -> tokio::time::Interval {
    let mut timer = tokio::time::interval(interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    timer
}

impl StudioVfs {
    pub(super) async fn background_aggressive_materialization(self: Arc<Self>) {
        let mut interval = tokio::time::interval(AGGRESSIVE_MATERIALIZATION_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if !self.aggressive_materialization_should_run() {
                continue;
            }

            let dirty_inodes = self.state.writer_stack.snapshot_dirty_inodes_by_bytes();
            for ino in dirty_inodes {
                if self.shutdown_requested() {
                    break;
                }
                self.spawn_deferred_materialization_if_needed(ino);
            }
        }
    }

    pub(super) async fn background_local_dirty_flush(self: Arc<Self>) {
        // ponytail: keep flush concurrency bounded at 32; raise only after measuring redb and payload I/O pressure.
        const MAX_LOCAL_DIRTY_FLUSHES_IN_FLIGHT: usize = 32;
        let mut interval = tokio::time::interval(LOCAL_DIRTY_FLUSH_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = interval.tick() => {}
            }
            if self.shutdown_requested() {
                break;
            }
            let dirty_inodes = self.snapshot_pending_local_dirty_inodes();
            if dirty_inodes.is_empty() {
                continue;
            }
            for inode_batch in dirty_inodes.chunks(MAX_LOCAL_DIRTY_FLUSHES_IN_FLIGHT) {
                let results =
                    futures::future::join_all(inode_batch.iter().copied().filter_map(|ino| {
                        if self.shutdown_requested()
                            || !self.should_background_flush_pending_local_dirty_inode(ino)
                        {
                            return None;
                        }
                        let vfs = Arc::clone(&self);
                        Some(async move {
                            let session = vfs
                                .state
                                .file_session
                                .enter_op(ino, FileSessionOperation::Materialize)
                                .await;
                            let flush_result = async {
                                if vfs.shutdown_requested()
                                    || !vfs.should_background_flush_pending_local_dirty_inode(ino)
                                {
                                    return Ok(());
                                }
                                vfs.flush_local_dirty_state_background(
                                    ino,
                                    LocalSyncPolicy::Opportunistic,
                                )
                                .await
                            }
                            .await;
                            drop(session);
                            vfs.state.file_session.gc_if_idle(ino);
                            (ino, flush_result)
                        })
                    }))
                    .await;
                for (ino, flush_result) in results {
                    if let Err(error) = flush_result {
                        warn!(
                            ino,
                            error = %error,
                            "background local-dirty flush failed; will retry"
                        );
                    }
                }
                if self.shutdown_requested() {
                    break;
                }
            }
        }
    }

    pub(super) async fn background_epoch_finalizer(self: Arc<Self>) {
        const MAX_FINALIZATIONS_IN_FLIGHT: usize = 32;
        let publisher = Arc::new(commit::EpochPublicationBatcher::default());

        'finalizer: loop {
            let Some((ino, content_epoch)) = self.pop_epoch_finalization() else {
                tokio::select! {
                    _ = self.state.shutdown.shutdown_notify.notified() => break,
                    _ = self.state.upload.pending_epoch_finalizations_notify.notified() => {}
                }
                continue;
            };

            let mut work = vec![(ino, content_epoch)];
            while work.len() < MAX_FINALIZATIONS_IN_FLIGHT {
                if self.shutdown_requested() {
                    break;
                }
                let Some(next) = self.pop_epoch_finalization() else {
                    break;
                };
                work.push(next);
            }
            let shutdown_token = self.state.shutdown.background_shutdown_token.clone();
            let results =
                futures::future::join_all(work.into_iter().map(|(ino, content_epoch)| {
                    let vfs = Arc::clone(&self);
                    let publisher = Arc::clone(&publisher);
                    let shutdown_token = shutdown_token.clone();
                    async move {
                        let result = run_epoch_finalization_until_shutdown(
                            shutdown_token,
                            commit::CommitCoordinator::finalize_inode_epoch_with_publisher(
                                &vfs,
                                ino,
                                content_epoch,
                                publisher,
                            ),
                        )
                        .await;
                        (ino, content_epoch, result)
                    }
                }))
                .await;

            for (ino, content_epoch, finalize_result) in results {
                let Some(finalize_result) = finalize_result else {
                    break 'finalizer;
                };
                match finalize_result {
                    Ok(commit::EpochFinalizeStatus::Published) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "published",
                        );
                    }
                    Ok(status @ commit::EpochFinalizeStatus::WaitingForUploads) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "waiting_uploads",
                        );
                        // Upload completion normally requeues the epoch, but a concurrent
                        // finalizer/upload transition can otherwise lose that notification.
                        if let Some(retry_delay) =
                            epoch_finalization_retry_delay(&status, self.upload_requeue_base)
                        {
                            self.schedule_epoch_finalization_retry(ino, content_epoch, retry_delay);
                        }
                    }
                    Ok(commit::EpochFinalizeStatus::Skipped) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "skipped",
                        );
                    }
                    Ok(status @ commit::EpochFinalizeStatus::Deferred) => {
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "deferred",
                        );
                        if let Some(retry_delay) =
                            epoch_finalization_retry_delay(&status, self.upload_requeue_base)
                        {
                            self.schedule_epoch_finalization_retry(ino, content_epoch, retry_delay);
                        }
                    }
                    Err(err) => {
                        self.state
                            .metrics
                            .zeroio_publish_failures
                            .fetch_add(1, Ordering::Relaxed);
                        self.record_runtime_event(
                            "zeroio",
                            "publish_failed",
                            "zeroio publish failed",
                            serde_json::json!({
                                "ino": ino,
                                "content_epoch": content_epoch,
                                "path": self.best_effort_path_for_inode(ino),
                                "error": err.to_string(),
                            }),
                        );
                        if Self::is_invalid_regular_file_graph_error(&err) {
                            let attr_size =
                                run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                                    engine.get_attr(ino)
                                })
                                .await
                                .ok()
                                .map(|attr| attr.size)
                                .unwrap_or(0);
                            warn!(
                                ino,
                                content_epoch,
                                error = %err,
                                "epoch finalization failed due to invalid regular-file graph, quarantining inode"
                            );
                            self.quarantine_invalid_regular_file_graph(ino, attr_size, &err);
                            self.state.file_session.note_background_transition(
                                ino,
                                FileSessionOperation::Finalize,
                                "failed",
                            );
                            continue;
                        }
                        if self.shutdown_requested() {
                            break 'finalizer;
                        }
                        self.state.file_session.note_background_transition(
                            ino,
                            FileSessionOperation::Finalize,
                            "failed",
                        );
                        warn!(
                            ino,
                            content_epoch,
                            error = %err,
                            "epoch finalization failed, will retry"
                        );
                        self.schedule_epoch_finalization_retry(
                            ino,
                            content_epoch,
                            self.upload_requeue_base,
                        );
                    }
                }
            }
        }
    }

    fn schedule_epoch_finalization_retry(
        self: &Arc<Self>,
        ino: Ino,
        content_epoch: u64,
        delay: Duration,
    ) {
        if self.shutdown_requested() {
            return;
        }
        let vfs = self.clone();
        tokio::spawn(async move {
            if vfs.shutdown_requested() {
                return;
            }
            tokio::select! {
                _ = vfs.state.shutdown.shutdown_notify.notified() => {}
                _ = tokio::time::sleep(delay) => {
                    if !vfs.shutdown_requested() {
                        vfs.queue_epoch_finalization(ino, content_epoch);
                    }
                }
            }
        });
    }

    fn kernel_invalidator(&self) -> Option<Arc<dyn KernelInvalidator>> {
        self.kernel_invalidator.read().clone()
    }

    /// Retire the local cached view before handing committed scopes to the bridge.
    /// Content remains subject to the same active-writer protection as legacy
    /// invalidation; the commit generation is not a new open-handle version.
    pub(super) fn apply_semantic_invalidation_batch(
        &self,
        mut batch: studiofs_meta::coherence::InvalidationBatch,
    ) -> (InvalidationAcceptance, bool) {
        use studiofs_meta::coherence::InvalidationIntent;

        let mut deferred = false;
        let mut attr_inodes = HashSet::new();
        let mut full_content_inodes = HashSet::new();
        let mut namespace_parents = HashSet::new();
        let mut reset = false;
        batch.intents.retain(|intent| {
            match intent {
                InvalidationIntent::Attributes { ino } | InvalidationIntent::Content { ino } => {
                    attr_inodes.insert(*ino);
                    let full = matches!(intent, InvalidationIntent::Content { .. });
                    if full {
                        full_content_inodes.insert(*ino);
                    }
                    if self.should_skip_kernel_invalidation_for_active_open(*ino, full) {
                        deferred = true;
                        return false;
                    }
                }
                InvalidationIntent::Entry { parent, name, .. } => {
                    self.dentry_cache_invalidate(*parent, name);
                    namespace_parents.insert(*parent);
                }
                InvalidationIntent::Directory { ino } => {
                    self.dentry_cache_invalidate_parent(*ino);
                    attr_inodes.insert(*ino);
                    namespace_parents.insert(*ino);
                }
                InvalidationIntent::Reset => {
                    reset = true;
                }
            }
            true
        });
        if reset {
            self.bump_namespace_epoch();
            self.retire_all_attr_read_observations();
            attr_inodes.extend(self.runtime_namespace_invalidation_inodes());
        }
        if !attr_inodes.is_empty() {
            self.invalidate_attr_read_cache(attr_inodes);
        }
        for ino in full_content_inodes {
            self.retire_os_cache_inode(ino);
        }
        if !namespace_parents.is_empty() {
            self.bump_namespace_for_parents(namespace_parents);
        }
        self.bump_acl_namespace_epoch();
        if deferred {
            debug!(
                generation = batch.generation,
                "semantic invalidation deferred for active writer; generation coverage incomplete"
            );
        }
        let generation_complete = !deferred;
        let invalidator = self.kernel_invalidator();
        let has_invalidator = invalidator.is_some();
        let acceptance = match invalidator {
            None => InvalidationAcceptance::Unsupported,
            Some(_invalidator) if batch.intents.is_empty() => InvalidationAcceptance::Failed,
            Some(invalidator) => invalidator.invalidate_batch(&batch, generation_complete),
        };
        if acceptance != InvalidationAcceptance::Queued || !generation_complete {
            debug!(
                generation = batch.generation,
                ?acceptance,
                generation_complete,
                "semantic invalidation not fully admitted"
            );
        }
        if has_invalidator {
            self.note_coherence_invalidation(
                batch.generation,
                acceptance,
                generation_complete,
                now_unix_millis(),
            );
        }
        (acceptance, generation_complete)
    }

    pub(super) fn drain_semantic_invalidations(&self) -> SemanticInvalidationDrain {
        let mut outcome = SemanticInvalidationDrain {
            all_accepted: true,
            ..SemanticInvalidationDrain::default()
        };
        for batch in self.sync_meta().take_invalidation_batches() {
            outcome.had_batches = true;
            let (acceptance, generation_complete) = self.apply_semantic_invalidation_batch(batch);
            outcome.all_accepted &=
                acceptance == InvalidationAcceptance::Queued && generation_complete;
        }
        outcome
    }

    fn retire_untrusted_coherence_cache(&self) {
        let mut known_inodes = self.runtime_namespace_invalidation_inodes();
        // The root directory is the conservative anchor for OS directory/name
        // cache state, which is not represented in `OsCacheState` generations.
        known_inodes.insert(ROOT_INO);
        self.bump_namespace_epoch();
        self.bump_acl_namespace_epoch();
        self.retire_all_attr_read_observations();
        self.invalidate_attr_read_cache(known_inodes.clone());
        self.invalidate_kernel_inodes(known_inodes);
    }

    fn note_coherence_invalidation(
        &self,
        generation: u64,
        acceptance: InvalidationAcceptance,
        generation_complete: bool,
        now_unix_ms: u64,
    ) {
        self.apply_coherence_transition(|health| {
            health.note_invalidation(generation, acceptance, generation_complete, now_unix_ms);
        });
    }

    fn apply_coherence_transition(&self, transition: impl FnOnce(&mut CoherenceHealthTracker)) {
        let became_unsafe = {
            let mut health = self.coherence_health.lock();
            let was_safe = health.allows_cache();
            transition(&mut health);
            was_safe && !health.allows_cache()
        };
        if became_unsafe {
            self.retire_untrusted_coherence_cache();
        }
    }

    pub(super) fn note_coherence_sync_started(&self, now_unix_ms: u64) {
        self.apply_coherence_transition(|health| health.note_sync_started(now_unix_ms));
    }

    pub(super) fn note_coherence_sync_success(
        &self,
        progress: MetadataCoherenceProgress,
        coverage_complete: bool,
        now_unix_ms: u64,
    ) {
        self.apply_coherence_transition(|health| {
            if !progress.source_connected {
                health.note_sync_failure(
                    crate::coherence::CoherenceHealthReason::Disconnected,
                    now_unix_ms,
                );
            } else if progress.remote_authoritative && progress.remote_head_version.is_none() {
                health.note_sync_failure(
                    crate::coherence::CoherenceHealthReason::MissingProgress,
                    now_unix_ms,
                );
            } else {
                health.note_sync_success(
                    progress.applied_version,
                    progress.remote_head_version,
                    coverage_complete,
                    now_unix_ms,
                );
            }
        });
        if coverage_complete {
            self.apply_coherence_transition(|health| {
                health.note_coverage_confirmed(progress.applied_version, now_unix_ms);
            });
        }
    }

    pub(super) fn note_coherence_sync_failure(&self, now_unix_ms: u64) {
        self.apply_coherence_transition(|health| {
            health.note_sync_failure(
                crate::coherence::CoherenceHealthReason::Disconnected,
                now_unix_ms,
            );
        });
    }

    pub(super) fn note_coherence_apply_failure(&self, now_unix_ms: u64) {
        self.apply_coherence_transition(|health| {
            health.note_sync_failure(
                crate::coherence::CoherenceHealthReason::ApplyFailed,
                now_unix_ms,
            );
        });
    }

    pub(super) fn note_coherence_delivery_age(&self, now_unix_ms: u64) {
        let age = self
            .kernel_invalidator()
            .and_then(|invalidator| invalidator.invalidation_delivery_age_ms());
        self.apply_coherence_transition(|health| health.note_delivery_age(age, now_unix_ms));
    }

    pub(super) fn stop_coherence(&self, now_unix_ms: u64) {
        self.apply_coherence_transition(|health| health.stop(now_unix_ms));
    }

    fn collect_metadata_snapshot<M: ?Sized + JanitorMeta>(
        engine: &M,
    ) -> MetaResult<MetadataTraversalSnapshot> {
        let mut seen = HashSet::from([ROOT_INO]);
        let mut stack = vec![ROOT_INO];
        let mut payloads = HashMap::new();
        let mut namespace_entries = HashSet::new();

        while let Some(ino) = stack.pop() {
            let attr = match engine.get_attr(ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(err) => return Err(err),
            };
            match attr.kind {
                FileType::Directory => {
                    let entries = match engine.readdir(ino) {
                        Ok(entries) => entries,
                        Err(MetaError::InodeNotFound(_)) => continue,
                        Err(err) => return Err(err),
                    };
                    for entry in entries {
                        namespace_entries.insert((ino, entry.name.clone(), entry.ino));
                        if seen.insert(entry.ino) {
                            stack.push(entry.ino);
                        }
                    }
                }
                FileType::RegularFile => {
                    let mut object_ids = engine
                        .get_regular_file_graph_object_usage(ino)?
                        .into_iter()
                        .map(|(object_id, _)| object_id)
                        .collect::<Vec<_>>();
                    object_ids.sort_unstable();
                    object_ids.dedup();
                    if !object_ids.is_empty() {
                        payloads.insert(ino, object_ids);
                    }
                }
                FileType::Symlink => {}
            }
        }

        Ok(MetadataTraversalSnapshot {
            inodes: seen,
            payloads,
            entries: namespace_entries,
        })
    }

    pub(super) async fn lazy_evict_removed_metadata_payloads(
        &self,
        removed_payloads: HashMap<Ino, Vec<u64>>,
        current_inodes: &HashSet<Ino>,
        retained_inodes: &HashSet<Ino>,
    ) {
        for (ino, object_ids) in removed_payloads {
            if object_ids.is_empty() {
                continue;
            }
            let inode_deleted = !current_inodes.contains(&ino) && !retained_inodes.contains(&ino);
            if !inode_deleted {
                self.evict_payload_objects_from_caches(ino, &object_ids)
                    .await;
                continue;
            }
            self.mark_inode_deleted_for_uploads(ino);
            if self.handle_table.open_count(ino) > 0 {
                self.handle_table.mark_orphan(ino);
                for object_id in object_ids {
                    self.record_deferred_deleted_local_payload(ino, object_id);
                }
                continue;
            }
            self.evict_inode_from_caches(ino, &object_ids).await;
            self.maybe_clear_deleted_inode_marker(ino);
        }
    }

    pub(super) fn removed_metadata_payloads(
        previous: &HashMap<Ino, Vec<u64>>,
        current: &HashMap<Ino, Vec<u64>>,
    ) -> HashMap<Ino, Vec<u64>> {
        debug_assert!(
            previous
                .values()
                .all(|ids| ids.windows(2).all(|pair| pair[0] < pair[1])),
            "previous metadata payload snapshot must be sorted and deduplicated"
        );
        debug_assert!(
            current
                .values()
                .all(|ids| ids.windows(2).all(|pair| pair[0] < pair[1])),
            "current metadata payload snapshot must be sorted and deduplicated"
        );

        previous
            .iter()
            .filter_map(|(ino, previous_object_ids)| {
                let current_object_ids = current.get(ino).map(Vec::as_slice).unwrap_or(&[]);
                let mut removed = Vec::new();
                let mut previous_index = 0;
                let mut current_index = 0;
                while previous_index < previous_object_ids.len() {
                    if current_index >= current_object_ids.len() {
                        removed.extend_from_slice(&previous_object_ids[previous_index..]);
                        break;
                    }
                    let previous_object_id = previous_object_ids[previous_index];
                    let current_object_id = current_object_ids[current_index];
                    if previous_object_id == current_object_id {
                        previous_index += 1;
                        current_index += 1;
                    } else if previous_object_id < current_object_id {
                        removed.push(previous_object_id);
                        previous_index += 1;
                    } else {
                        current_index += 1;
                    }
                }
                if removed.is_empty() {
                    None
                } else {
                    Some((*ino, removed))
                }
            })
            .collect()
    }

    pub(super) fn changed_metadata_payload_inodes(
        previous: &HashMap<Ino, Vec<u64>>,
        current: &HashMap<Ino, Vec<u64>>,
    ) -> HashSet<Ino> {
        let mut changed = HashSet::new();
        for (ino, previous_object_ids) in previous {
            if current.get(ino) != Some(previous_object_ids) {
                changed.insert(*ino);
            }
        }
        for ino in current.keys() {
            if !previous.contains_key(ino) {
                changed.insert(*ino);
            }
        }
        changed
    }

    pub(super) fn invalidate_kernel_inodes(&self, inodes: impl IntoIterator<Item = Ino>) {
        let mut unique = HashSet::new();
        for ino in inodes {
            if !unique.insert(ino) {
                continue;
            }
            // Generation retirement is a VFS correctness boundary, not a
            // best-effort side effect of having a mounted platform notifier.
            self.retire_os_cache_inode(ino);
            if let Some(invalidator) = self.kernel_invalidator()
                && !self.should_skip_kernel_invalidation_for_active_open(ino, true)
            {
                invalidator.inval_inode(ino);
            }
        }
    }

    pub(super) fn invalidate_kernel_entries(
        &self,
        entries: impl IntoIterator<Item = (Ino, Vec<u8>)>,
    ) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        for (parent, name) in entries {
            invalidator.inval_entry(parent, &name);
        }
    }

    pub(super) fn invalidate_kernel_inode_attrs(&self, inodes: impl IntoIterator<Item = Ino>) {
        let Some(invalidator) = self.kernel_invalidator() else {
            return;
        };
        let mut unique = HashSet::new();
        for ino in inodes {
            if unique.insert(ino)
                && !self.should_skip_kernel_invalidation_for_active_open(ino, false)
            {
                invalidator.inval_inode_attrs(ino);
            }
        }
    }

    fn should_skip_kernel_invalidation_for_active_open(&self, ino: Ino, full: bool) -> bool {
        if !self.handle_table.has_active_open_overlay(ino) {
            return false;
        }
        let pending = if full {
            &self.deferred_kernel_invalidations
        } else {
            &self.deferred_kernel_inode_attrs
        };
        pending.insert(ino);
        if self.handle_table.has_active_open_overlay(ino) {
            true
        } else {
            pending.remove(&ino);
            false
        }
    }

    /// Protect accepted local state when a native bridge can only invalidate
    /// attributes together with content. A true result requires the bridge to
    /// defer delivery and leaves the ordinary handle-release retry armed.
    pub fn defer_kernel_invalidation_for_active_writer(&self, ino: Ino) -> bool {
        if self.should_skip_kernel_invalidation_for_active_open(ino, true) {
            return true;
        }
        if self.inode_has_local_dirty_state(ino) {
            self.deferred_kernel_invalidations.insert(ino);
            return true;
        }
        false
    }

    pub(super) fn flush_deferred_kernel_invalidations(&self, ino: Ino) {
        if self.kernel_invalidator().is_none() {
            return;
        }
        let full = self.deferred_kernel_invalidations.remove(&ino).is_some();
        let attrs = self.deferred_kernel_inode_attrs.remove(&ino).is_some();
        if full {
            self.invalidate_kernel_inodes([ino]);
        } else if attrs {
            self.invalidate_kernel_inode_attrs([ino]);
        }
    }

    pub(super) fn invalidate_attr_read_cache(
        &self,
        inodes: impl IntoIterator<Item = Ino>,
    ) -> Vec<Ino> {
        let mut cache = self.state.namespace.attr_read_cache.lock();
        let mut read_plan_cache = self.state.read.published_read_plan_cache.lock();
        let mut removed = Vec::new();
        for ino in inodes {
            self.handle_table.clear_read_windows_for_inode(ino);
            read_plan_cache.pop(&ino);
            if cache.remove(ino) {
                removed.push(ino);
            }
        }
        removed
    }

    pub(super) fn invalidate_attr_read_cache_if_observed(
        &self,
        ino: Ino,
        expected: super::AttrReadObservation,
    ) -> Option<super::AttrReadObservation> {
        let mut cache = self.state.namespace.attr_read_cache.lock();
        if cache.observation(ino) != expected {
            return None;
        }
        self.handle_table.clear_read_windows_for_inode(ino);
        self.state.read.published_read_plan_cache.lock().pop(&ino);
        cache.remove(ino);
        Some(cache.observation(ino))
    }

    pub(super) fn retire_all_attr_read_observations(&self) {
        self.state
            .namespace
            .attr_read_cache
            .lock()
            .retire_observations();
    }

    pub(super) fn read_cache_invalidation_set(
        attr_cache_inodes: &HashSet<Ino>,
        changed_payload_inodes: &HashSet<Ino>,
    ) -> HashSet<Ino> {
        attr_cache_inodes
            .iter()
            .copied()
            .chain(changed_payload_inodes.iter().copied())
            .collect()
    }

    pub(super) fn runtime_namespace_invalidation_inodes(&self) -> HashSet<Ino> {
        let mut inodes = self
            .state
            .namespace
            .dirty_attrs
            .lock()
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        inodes.extend(self.state.namespace.attr_read_cache.lock().keys());
        inodes.extend(
            self.pending_uploads_snapshot()
                .into_iter()
                .map(|upload| upload.ino),
        );
        inodes.extend(self.os_cache.lock().generation_inodes());
        inodes
    }

    async fn wait_for_metadata_invalidation_notify(notifier: Option<&Arc<tokio::sync::Notify>>) {
        if let Some(notifier) = notifier {
            notifier.notified().await;
        } else {
            std::future::pending::<()>().await;
        }
    }

    pub(super) async fn background_metadata_invalidation(self: Arc<Self>, interval: Duration) {
        let meta = self.sync_meta();
        let metadata_notifier = meta.metadata_invalidation_notifier();
        // Do not mark startup metadata as already invalidated. The PG listener
        // can advance local metadata before this task reaches its first wait;
        // the first wake must still clear VFS and kernel caches for that state.
        let mut last_namespace_token = None;
        let mut last_metadata_token = None;
        let mut last_snapshot: Option<HashSet<Ino>> = None;
        let mut last_namespace_entries: Option<HashSet<(Ino, Vec<u8>, Ino)>> = None;
        let mut last_payload_snapshot: Option<HashMap<Ino, Vec<u64>>> = None;
        let mut fallback_timer = metadata_invalidation_fallback_timer(interval);

        loop {
            let wake_reason = tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = Self::wait_for_metadata_invalidation_notify(metadata_notifier.as_ref()) => {
                    MetadataInvalidationWakeReason::BackendNotify
                },
                _ = fallback_timer.tick() => MetadataInvalidationWakeReason::FallbackPoll,
            };
            if self.shutdown_requested() {
                break;
            }
            let now_unix_ms = now_unix_millis();
            self.note_coherence_sync_started(now_unix_ms);
            // Local commits must not wait for a remote poll to finish before
            // their already-visible state can invalidate the mounted cache.
            let before_sync_coverage = self.drain_semantic_invalidations();
            let meta = self.meta.clone();
            let previous_namespace_token = last_namespace_token.clone();
            let previous_metadata_token = last_metadata_token.clone();
            let sync = run_meta(meta.clone(), move |engine: Arc<dyn VfsMeta>| {
                let before_namespace = engine.namespace_version_token();
                let before_metadata = engine.metadata_version_token();
                if should_run_invalidation_poll_sync(
                    wake_reason,
                    &previous_namespace_token,
                    &previous_metadata_token,
                    &before_namespace,
                    &before_metadata,
                ) {
                    engine.sync_metadata_for_invalidation_poll()?;
                }
                let after_namespace = engine.namespace_version_token();
                let after_metadata = engine.metadata_version_token();
                Ok((
                    before_namespace,
                    after_namespace,
                    before_metadata,
                    after_metadata,
                    engine.metadata_coherence_progress(),
                ))
            })
            .await;
            // Earlier transactions may have committed even if the remaining
            // remote sync failed or did not advance its public version token.
            let after_sync_coverage = self.drain_semantic_invalidations();
            let Ok((
                _before_namespace,
                after_namespace_token,
                _before_metadata,
                after_metadata_token,
                progress,
            )) = sync
            else {
                self.note_coherence_sync_failure(now_unix_millis());
                continue;
            };

            self.note_coherence_sync_success(
                progress,
                (!before_sync_coverage.had_batches && !after_sync_coverage.had_batches)
                    || (before_sync_coverage.all_accepted && after_sync_coverage.all_accepted),
                now_unix_millis(),
            );
            self.note_coherence_delivery_age(now_unix_millis());

            let (token_namespace_changed, metadata_changed) = metadata_token_changes(
                &last_namespace_token,
                &last_metadata_token,
                &after_namespace_token,
                &after_metadata_token,
            );

            if !token_namespace_changed && !metadata_changed {
                continue;
            }

            let meta = self.meta.clone();
            let current_snapshot = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                Self::collect_metadata_snapshot(engine.as_ref())
            })
            .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    warn!(
                        ?wake_reason,
                        error = %error,
                        "metadata invalidation snapshot failed; retrying on the next wake"
                    );
                    self.note_coherence_apply_failure(now_unix_millis());
                    continue;
                }
            };
            let namespace_changed = metadata_namespace_changed(
                token_namespace_changed,
                last_snapshot.as_ref(),
                &current_snapshot.inodes,
            ) || last_namespace_entries
                .as_ref()
                .is_some_and(|previous| previous != &current_snapshot.entries);

            let changed_namespace_entries = if namespace_changed {
                metadata_namespace_entry_changes(
                    last_namespace_entries.as_ref(),
                    &current_snapshot.entries,
                )
            } else {
                HashSet::new()
            };

            let changed_payload_inodes = match &last_payload_snapshot {
                Some(last_payload_snapshot) => Self::changed_metadata_payload_inodes(
                    last_payload_snapshot,
                    &current_snapshot.payloads,
                ),
                None => current_snapshot.payloads.keys().copied().collect(),
            };
            let removed_payloads = match &last_payload_snapshot {
                Some(last_payload_snapshot) => Self::removed_metadata_payloads(
                    last_payload_snapshot,
                    &current_snapshot.payloads,
                ),
                None => HashMap::new(),
            };
            let runtime_inodes = if namespace_changed {
                self.runtime_namespace_invalidation_inodes()
            } else {
                HashSet::new()
            };
            self.lazy_evict_removed_metadata_payloads(
                removed_payloads,
                &current_snapshot.inodes,
                &runtime_inodes,
            )
            .await;
            let attr_cache_inodes = match &last_snapshot {
                Some(last_snapshot) => metadata_attr_cache_invalidation_set(
                    last_snapshot,
                    &current_snapshot.inodes,
                    namespace_changed,
                    runtime_inodes,
                ),
                None => {
                    let mut inodes = current_snapshot.inodes.clone();
                    if namespace_changed {
                        inodes.extend(runtime_inodes);
                    }
                    inodes
                }
            };
            let read_cache_inodes =
                Self::read_cache_invalidation_set(&attr_cache_inodes, &changed_payload_inodes);
            self.invalidate_attr_read_cache(read_cache_inodes);
            self.invalidate_kernel_inodes(
                attr_cache_inodes
                    .iter()
                    .copied()
                    .chain(changed_payload_inodes.iter().copied()),
            );
            self.invalidate_kernel_entries(changed_namespace_entries);
            if namespace_changed {
                self.bump_namespace_epoch();
            }
            if namespace_changed || metadata_changed {
                self.bump_acl_namespace_epoch();
            }
            last_snapshot = Some(current_snapshot.inodes);
            last_namespace_entries = Some(current_snapshot.entries);
            last_payload_snapshot = Some(current_snapshot.payloads);
            last_namespace_token = after_namespace_token;
            last_metadata_token = after_metadata_token;
        }
    }

    pub(super) async fn background_uploader(self: Arc<Self>, rx: tokio::sync::mpsc::Receiver<()>) {
        UploadLifecycleWorker::new(self).run(rx).await;
    }

    pub(super) async fn background_upload_stats(self: Arc<Self>) {
        let mut upload_pipeline_summary_was_active = false;
        let mut ticks_since_last_log = 0u64;
        let mut zero_log_counter = 0u8;
        let mut s3_upload_problem_active = UploadTroubleState::default();
        const ZERO_LOG_TARGET: u8 = 3;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.upload_stats_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            if let Err(error) = self.payload_store_run_pressure_maintenance_async().await {
                warn!(error = %error, "background local backend pressure maintenance failed");
            }
            let pending_uploads = self.pending_uploads_snapshot();
            let pending = pending_uploads.len();
            let pending_upload_bytes = pending_uploads
                .iter()
                .map(|op| {
                    self.local_payload_backend
                        .object_stats(op.object_id)
                        .map(|stats| stats.payload_len)
                        .unwrap_or(u64::from(self.block_size))
                })
                .sum::<u64>();
            let metadata_backlog = self.sync_meta().replication_backlog();
            let pending_metadata_ops = metadata_backlog.total_ops();
            let active_uploads = self.upload_scheduler.in_flight();
            let active_upload_workers = self
                .state
                .upload
                .active_upload_workers
                .load(Ordering::Relaxed);
            let upload_queue = self.cached_store.cache_pipeline_metrics().upload_queue;
            let is_active = Self::upload_pipeline_summary_is_active(
                pending,
                active_uploads,
                active_upload_workers,
                pending_metadata_ops,
            );
            let activity_started = is_active && !upload_pipeline_summary_was_active;

            let should_log = if activity_started {
                ticks_since_last_log = 0;
                zero_log_counter = 0;
                true
            } else if is_active {
                ticks_since_last_log += 1;
                ticks_since_last_log >= 10
            } else if upload_pipeline_summary_was_active {
                ticks_since_last_log = 0;
                zero_log_counter = 1;
                true
            } else if zero_log_counter > 0 && zero_log_counter < ZERO_LOG_TARGET {
                ticks_since_last_log += 1;
                if ticks_since_last_log >= 10 {
                    zero_log_counter += 1;
                    true
                } else {
                    false
                }
            } else {
                false
            };

            let (p95_ms, max_ms) = self.pending_age_stats_ms(&pending_uploads);
            let ingress = self.ingress_stats();
            let materialize_batches = self.state.metrics.materialize_batch_stats.snapshot();
            let foreground_read_protection = self.foreground_read_protection_stats();
            self.adjust_upload_backpressure_from_metrics(
                foreground_read_protection.p90_latency_ns,
                foreground_read_protection.dominant_upload_pressure_boost,
            );
            let dentry_cache = self.dentry_cache_metrics();
            let path_shortcut_cache = self.path_shortcut_cache_metrics();
            let attr_read_cache = self.attr_read_cache_metrics();
            let local_backend_dirty_count = self.local_payload_backend.dirty_object_count();
            let transit_memory = self.transit_memory_budget.snapshot();
            let file_sessions = self.file_session_stats();

            if should_log {
                ticks_since_last_log = 0;
                let pending_retry_count = pending_uploads
                    .iter()
                    .filter(|op| op.retry_count != 0)
                    .count() as u64;
                let pending_uploads_human =
                    Self::format_upload_pipeline_pending_uploads(pending_upload_bytes);
                let upload_trouble_counters =
                    UploadTroubleCounters::from_metrics(&self.upload_scheduler.metrics);
                let upload_trouble_signal = s3_upload_problem_active.observe(
                    upload_trouble_counters,
                    pending,
                    active_uploads,
                    pending_retry_count,
                );
                let became_idle = !is_active && upload_pipeline_summary_was_active;
                if let Some((event_kind, event_title)) =
                    Self::upload_pipeline_runtime_event(activity_started, became_idle, is_active)
                {
                    self.record_runtime_event(
                        "upload_pipeline",
                        event_kind,
                        event_title,
                        serde_json::json!({
                            "pending_uploads": pending,
                            "pending_upload_bytes": pending_upload_bytes,
                            "pending_uploads_human": pending_uploads_human,
                            "active_uploads": active_uploads,
                            "active_upload_workers": active_upload_workers,
                            "pending_metadata_ops": pending_metadata_ops,
                            "inflight_metadata_ops": metadata_backlog.inflight_ops,
                            "dirty_mem_blocks": ingress.dirty_mem_blocks,
                            "dirty_mem_bytes": ingress.dirty_mem_bytes,
                            "write_backpressure_events": ingress.write_backpressure_events,
                            "write_backpressure_wait_ms": ingress.write_backpressure_wait_ms,
                            "foreground_read_p90_latency_ns": foreground_read_protection.p90_latency_ns,
                        }),
                    );
                }
                info!(
                    pending_uploads = %pending_uploads_human,
                    active_uploads,
                    active_upload_workers,
                    pending_metadata_ops,
                    "upload pipeline"
                );
                match upload_trouble_signal {
                    UploadTroubleSignal::Warn => {
                        warn!(
                            pending_uploads = pending,
                            pending_uploads_human = %pending_uploads_human,
                            active_uploads,
                            timeouts = upload_trouble_counters.timeouts,
                            network_errors = upload_trouble_counters.network_errors,
                            retries = upload_trouble_counters.retries,
                            exhausted = upload_trouble_counters.exhausted,
                            oldest_pending_upload_age_ms = max_ms,
                            last_put_bytes_uploaded = upload_trouble_counters.bytes_uploaded,
                            "S3 upload pipeline appears stuck; uploads are retrying"
                        );
                    }
                    UploadTroubleSignal::Recovered => {
                        info!("S3 upload pipeline recovered");
                    }
                    UploadTroubleSignal::None => {}
                }
            }

            upload_pipeline_summary_was_active = is_active;
            debug!(
                dentry_cache_entries = dentry_cache.occupancy_entries,
                dentry_cache_bytes = dentry_cache.occupancy_bytes,
                path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                attr_read_cache_entries = attr_read_cache.occupancy_entries,
                attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                "upload pipeline caches"
            );
            if !self.upload_pipeline_is_idle(pending, &ingress) {
                debug!(
                    requeues = self.state.upload.upload_requeues.load(Ordering::Relaxed),
                    canceled_deleted = self.state.upload.upload_canceled_deleted.load(Ordering::Relaxed),
                    canceled_stale = self.state.upload.upload_canceled_stale.load(Ordering::Relaxed),
                    foreground_reads_inflight =
                        self.state.read.foreground_reads_inflight.load(Ordering::Relaxed),
                    dirty_mem_blocks = ingress.dirty_mem_blocks,
                    dirty_mem_bytes = ingress.dirty_mem_bytes,
                    write_backpressure_wait_ms = ingress.write_backpressure_wait_ms,
                    write_backpressure_events = ingress.write_backpressure_events,
                    fast_backpressure_wait_ms = ingress.fast_backpressure_wait_ms,
                    fast_backpressure_events = ingress.fast_backpressure_events,
                    fast_backpressure_stall_max_ms = ingress.fast_backpressure_stall_max_ms,
                    materialize_batches_total = materialize_batches.batches,
                    materialize_batches_payloads = materialize_batches.payloads,
                    materialize_batches_bytes = materialize_batches.bytes,
                    materialize_batches_inflight = materialize_batches.inflight_batches,
                    materialize_batches_inflight_peak = materialize_batches.inflight_batches_peak,
                    materialize_batch_duration_histogram = ?materialize_batches.duration_histogram.bucket_counts,
                    upload_queue_depth = upload_queue.pending_depth,
                    upload_queue_depth_peak = upload_queue.pending_depth_peak,
                    upload_queue_submitted = upload_queue.submitted,
                    upload_queue_completed = upload_queue.completed,
                    upload_queue_requeued = upload_queue.requeued,
                    upload_queue_canceled = upload_queue.canceled,
                    upload_io_slots_available = self.upload_cache_io_slots.available_permits(),
                    qos_accept_available = self.qos.accept_available(),
                    qos_materialize_available = self.qos.materialize_available(),
                    qos_commit_available = self.qos.commit_available(),
                    qos_fetch_available = self.qos.fetch_available(),
                    pending_metadata_ops = metadata_backlog.pending_ops,
                    inflight_metadata_ops = metadata_backlog.inflight_ops,
                    dentry_cache_hits = dentry_cache.hits,
                    dentry_cache_hot_hits = dentry_cache.protected_hits,
                    dentry_cache_misses = dentry_cache.misses,
                    dentry_cache_evictions = dentry_cache.evictions,
                    dentry_cache_negative_hits = dentry_cache.negative_hits,
                    dentry_cache_negative_hit_ratio = dentry_cache.negative_hit_ratio(),
                    dentry_cache_hot_hit_ratio = dentry_cache.hot_hit_ratio(),
                    dentry_cache_entries = dentry_cache.occupancy_entries,
                    dentry_cache_bytes = dentry_cache.occupancy_bytes,
                    dentry_cache_hot_entries = dentry_cache.protected_occupancy_entries,
                    dentry_cache_hot_bytes = dentry_cache.protected_occupancy_bytes,
                    dentry_cache_entry_budget = dentry_cache.entry_budget,
                    dentry_cache_byte_budget = dentry_cache.byte_budget,
                    path_shortcut_cache_hits = path_shortcut_cache.hits,
                    path_shortcut_cache_hot_hits = path_shortcut_cache.protected_hits,
                    path_shortcut_cache_misses = path_shortcut_cache.misses,
                    path_shortcut_cache_evictions = path_shortcut_cache.evictions,
                    path_shortcut_cache_negative_hits = path_shortcut_cache.negative_hits,
                    path_shortcut_cache_negative_hit_ratio =
                        path_shortcut_cache.negative_hit_ratio(),
                    path_shortcut_cache_hot_hit_ratio = path_shortcut_cache.hot_hit_ratio(),
                    path_shortcut_cache_entries = path_shortcut_cache.occupancy_entries,
                    path_shortcut_cache_bytes = path_shortcut_cache.occupancy_bytes,
                    path_shortcut_cache_hot_entries =
                        path_shortcut_cache.protected_occupancy_entries,
                    path_shortcut_cache_hot_bytes = path_shortcut_cache.protected_occupancy_bytes,
                    path_shortcut_cache_entry_budget = path_shortcut_cache.entry_budget,
                    path_shortcut_cache_byte_budget = path_shortcut_cache.byte_budget,
                    attr_read_cache_hits = attr_read_cache.hits,
                    attr_read_cache_hot_hits = attr_read_cache.protected_hits,
                    attr_read_cache_misses = attr_read_cache.misses,
                    attr_read_cache_evictions = attr_read_cache.evictions,
                    attr_read_cache_hot_hit_ratio = attr_read_cache.hot_hit_ratio(),
                    attr_read_cache_entries = attr_read_cache.occupancy_entries,
                    attr_read_cache_bytes = attr_read_cache.occupancy_bytes,
                    attr_read_cache_hot_entries = attr_read_cache.protected_occupancy_entries,
                    attr_read_cache_hot_bytes = attr_read_cache.protected_occupancy_bytes,
                    attr_read_cache_entry_budget = attr_read_cache.entry_budget,
                    attr_read_cache_byte_budget = attr_read_cache.byte_budget,
                    "upload pipeline counters"
                );
                debug!(
                    file_session_active = file_sessions.active_sessions,
                    file_session_open_handles = file_sessions.open_handles,
                    file_session_queued_ops = file_sessions.queued_ops,
                    file_session_max_queue_depth = file_sessions.max_queue_depth,
                    file_session_wait_events = file_sessions.wait_events,
                    file_session_wait_ms = file_sessions.wait_ms,
                    file_session_transitions = file_sessions.transitions,
                    "file session counters"
                );
                debug!(
                    file_session_wait_reasons = ?file_sessions.wait_reasons,
                    "file session queue wait reasons"
                );
                debug!(
                    upload_fallback_deferrals =
                        foreground_read_protection.upload_fallback_deferrals,
                    upload_fallback_rereads = foreground_read_protection.upload_fallback_rereads,
                    upload_fallback_reread_bytes =
                        foreground_read_protection.upload_fallback_reread_bytes,
                    upload_fallback_reread_cache_misses =
                        foreground_read_protection.upload_fallback_reread_cache_misses,
                    upload_fallback_wait_ms =
                        foreground_read_protection.upload_fallback_wait_ns / 1_000_000,
                    upload_fallback_wait_max_ms =
                        foreground_read_protection.upload_fallback_wait_max_ns / 1_000_000,
                    upload_fallback_read_ms =
                        foreground_read_protection.upload_fallback_read_ns / 1_000_000,
                    upload_fallback_read_max_ms =
                        foreground_read_protection.upload_fallback_read_max_ns / 1_000_000,
                    foreground_local_payload_reads =
                        foreground_read_protection.foreground_local_payload_reads,
                    foreground_local_payload_read_bytes =
                        foreground_read_protection.foreground_local_payload_read_bytes,
                    foreground_local_payload_read_io_ms =
                        foreground_read_protection.foreground_local_payload_read_io_ns / 1_000_000,
                    foreground_local_payload_read_io_max_ms = foreground_read_protection
                        .foreground_local_payload_read_io_max_ns
                        / 1_000_000,
                    upload_hot_payload_reads = foreground_read_protection.upload_hot_payload_reads,
                    upload_hot_payload_read_bytes =
                        foreground_read_protection.upload_hot_payload_read_bytes,
                    upload_staged_payload_reads =
                        foreground_read_protection.upload_staged_payload_reads,
                    upload_staged_payload_read_bytes =
                        foreground_read_protection.upload_staged_payload_read_bytes,
                    "upload pipeline local reread path"
                );
                debug!(
                    transit_memory_budget_bytes = transit_memory.budget_bytes,
                    transit_memory_used_bytes = transit_memory.used_bytes,
                    transit_memory_available_bytes = transit_memory.available_bytes,
                    transit_memory_waiting_foreground = transit_memory.waiting_foreground,
                    transit_memory_waiting_background = transit_memory.waiting_background,
                    "upload pipeline transit memory"
                );
                debug!(
                    local_backend_dirty_count,
                    prefetch_skips = self
                        .fetch_scheduler
                        .metrics
                        .prefetch_skips
                        .load(Ordering::Relaxed),
                    "local backend and fetch counters"
                );
                debug!(
                    upload_io_foreground_cost_base = self.upload_cache_io_foreground_cost_base,
                    upload_io_foreground_cost_step = self.upload_cache_io_foreground_cost_step,
                    upload_io_foreground_cost_dynamic =
                        foreground_read_protection.upload_cache_io_foreground_cost,
                    foreground_read_p90_present =
                        foreground_read_protection.p90_latency_ns.is_some(),
                    foreground_read_p90_ms = foreground_read_protection
                        .p90_latency_ns
                        .map(|ns| ns.saturating_div(1_000_000))
                        .unwrap_or(0),
                    foreground_read_speedometer_samples =
                        foreground_read_protection.speedometer_samples,
                    foreground_read_window_bytes = foreground_read_protection.window_bytes,
                    foreground_read_window_coverage_ms =
                        foreground_read_protection.window_coverage_ms,
                    foreground_read_throughput_bytes_per_sec =
                        foreground_read_protection.throughput_bytes_per_sec,
                    foreground_read_target_ms = self
                        .upload_fg_read_latency_target_ns
                        .saturating_div(1_000_000),
                    upload_scheduler_penalty_ms =
                        foreground_read_protection.upload_scheduler_penalty_ms,
                    "upload pipeline adaptive throttle"
                );
            }
            self.upload_scheduler.log_stats(pending, p95_ms, max_ms);
        }
    }

    pub(super) async fn background_gc(self: Arc<Self>) {
        debug!("background GC started");

        let interval = tokio::time::Duration::from_secs(600);
        let grace_period = 3600i64;
        let mut gc_cycle = 0_u64;

        loop {
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }
            gc_cycle = gc_cycle.saturating_add(1);

            let meta = self.meta.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "background GC skipped: janitor PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!("background GC skipped: janitor PG lock not acquired");
                continue;
            }

            const CHUNK_COMPACTION_BATCH_LIMIT: usize = 32;
            let meta = self.meta.clone();
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.compact_fragmented_regular_file_chunks(CHUNK_COMPACTION_BATCH_LIMIT)
            })
            .await
            {
                Ok(compacted_chunks) if compacted_chunks > 0 => {
                    info!(
                        compacted_chunks,
                        "background GC compacted fragmented regular-file chunk rows"
                    );
                }
                Ok(_) => {}
                Err(e) => warn!(error = %e, "background GC chunk compaction failed"),
            }

            let meta = self.meta.clone();
            let referenced_object_ids = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        error = %e,
                        "background GC skipped: metadata reachability query failed"
                    );
                    continue;
                }
            };

            let meta = self.meta.clone();
            let snapshot_pins = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        error = %e,
                        "background GC skipped: snapshot protection query failed"
                    );
                    continue;
                }
            };

            let now = std::time::SystemTime::now()
                .duration_since(std::time::SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let deletion_owner = format!("background-gc-{gc_cycle}");

            const PENDING_DELETION_GC_BATCH_LIMIT: usize = 1_000;
            let meta = self.meta.clone();
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_pending_deletions_limited(PENDING_DELETION_GC_BATCH_LIMIT + 1)
            })
            .await
            {
                Ok(pending) => {
                    let mut deleted_count = 0;
                    let pending_total = pending.len();
                    for (object_id, timestamp) in
                        pending.into_iter().take(PENDING_DELETION_GC_BATCH_LIMIT)
                    {
                        if referenced_object_ids.contains(&object_id) {
                            debug!(object_id, "skipping object - still reachable in metadata");
                            continue;
                        }

                        if snapshot_pins.contains(&object_id) {
                            debug!(object_id, "skipping object - protected by a snapshot cut");
                            continue;
                        }

                        if now - timestamp < grace_period {
                            debug!("skipping object {} - still in grace period", object_id);
                            continue;
                        }

                        let meta = self.meta.clone();
                        let claim_owner = deletion_owner.clone();
                        let deletion_claim = match run_meta(
                            meta,
                            move |engine: Arc<dyn VfsMeta>| {
                                engine.claim_uploaded_object_deletion(object_id, &claim_owner)
                            },
                        )
                        .await
                        {
                            Ok(claim) => claim,
                            Err(error) => {
                                warn!(object_id, error = %error, "GC: failed to claim object deletion");
                                continue;
                            }
                        };
                        let fencing_token = match deletion_claim {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Untracked => {
                                None
                            }
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Claimed {
                                fencing_token,
                            } => Some(fencing_token),
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Protected => {
                                debug!(object_id, "skipping object - protected by upload ledger");
                                continue;
                            }
                        };

                        let delete_results = futures::future::join_all(
                            format_object_fragment_keys(&self.data_prefix, object_id)
                                .into_iter()
                                .map(|key| async {
                                    let result = self.data_write.delete_by_key(&key).await;
                                    (key, result)
                                }),
                        )
                        .await;
                        let mut delete_failed = false;
                        for (key, result) in delete_results {
                            if let Err(e) = result {
                                if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                                {
                                    continue;
                                }
                                delete_failed = true;
                                warn!(
                                    object_id,
                                    key,
                                    error = %e,
                                    "failed to delete object fragment from S3"
                                );
                            }
                        }
                        let deletion_outcome = if delete_failed {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Retryable
                        } else {
                            studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Deleted
                        };
                        if let Some(fencing_token) = fencing_token {
                            let meta = self.meta.clone();
                            let finish_owner = deletion_owner.clone();
                            if let Err(error) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                                engine.finish_uploaded_object_deletion(
                                    object_id,
                                    &finish_owner,
                                    fencing_token,
                                    deletion_outcome,
                                )
                            })
                            .await
                            {
                                warn!(object_id, error = %error, "GC: failed to finish object deletion claim");
                                continue;
                            }
                        }
                        if !delete_failed {
                            let meta = self.meta.clone();
                            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                                engine.remove_pending_deletion(object_id)
                            })
                            .await
                            {
                                warn!("failed to remove pending deletion for {}: {}", object_id, e);
                            }
                            deleted_count += 1;
                            debug!("GC deleted object {}", object_id);
                        }
                    }
                    if deleted_count > 0 {
                        info!("GC deleted {} objects", deleted_count);
                    }
                    if pending_total > PENDING_DELETION_GC_BATCH_LIMIT {
                        debug!(
                            pending_total,
                            batch_limit = PENDING_DELETION_GC_BATCH_LIMIT,
                            "GC limited pending deletion cleanup batch"
                        );
                    }
                }
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    error!("failed to get pending deletions: {}", e);
                }
            }

            let meta = self.meta.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(error = %e, "background GC failed to release janitor PG lock");
            }
        }
    }

    pub(super) async fn background_janitor(self: Arc<Self>) {
        debug!(
            interval_secs = self.janitor_interval.as_secs(),
            grace_secs = self.janitor_skip_newer_than.as_secs(),
            max_deletes = self.janitor_max_deletes_per_run,
            "background janitor started"
        );
        let mut cycle: u64 = 0;

        loop {
            cycle = cycle.saturating_add(1);
            tokio::select! {
                _ = self.state.shutdown.shutdown_notify.notified() => break,
                _ = tokio::time::sleep(self.janitor_interval) => {}
            }
            if self.shutdown_requested() {
                break;
            }

            let meta = self.meta.clone();
            let lock_acquired = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.try_acquire_global_janitor_lock()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor cycle skipped: PG lock check failed");
                    continue;
                }
            };
            if !lock_acquired {
                debug!(cycle, "janitor cycle skipped: PG lock not acquired");
                continue;
            }

            let meta = self.meta.clone();
            let mut global_snapshot_pins = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_global_snapshot_pins()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle skipped: snapshot pin query failed");
                    continue;
                }
            };

            let meta = self.meta.clone();
            let reconciliation_owner = format!("janitor-{cycle}");
            let claim_owner = reconciliation_owner.clone();
            let reconciliation_claims = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.claim_uploaded_fragment_reconciliation(&claim_owner)
            })
            .await
            {
                Ok(claims) => claims,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle skipped: upload reconciliation claim failed");
                    continue;
                }
            };
            let reconciliation_claim_count = reconciliation_claims.len();
            for claim in &reconciliation_claims {
                global_snapshot_pins.insert(claim.entry.object_id);
            }
            let reconciliation_shutdown = self.state.shutdown.background_shutdown_token.clone();
            let reconciliation_data_store = self.fetch_scheduler.data_store().clone();
            let reconciliation_outcomes = futures::stream::iter(
                reconciliation_claims.into_iter().map(|claim| {
                    let shutdown = reconciliation_shutdown.clone();
                    let data_store = reconciliation_data_store.clone();
                    async move {
                        let proof = run_with_timeout_or_cancel(
                            &shutdown,
                            REMOTE_OBJECT_IO_TIMEOUT,
                            data_store.get_by_key(&claim.entry.object_key),
                        )
                        .await;
                        let outcome = match proof {
                            TimeoutOrCancel::Completed(Ok(remote_payload)) => {
                                let remote_bytes = remote_payload.to_bytes();
                                let remote_sha256: [u8; 32] = Sha256::digest(&remote_bytes).into();
                                if remote_bytes.len() as u64 == claim.entry.expected_byte_length
                                    && remote_sha256 == claim.entry.expected_sha256
                                {
                                    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::RemoteConfirmed
                                } else {
                                    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable
                                }
                            }
                            TimeoutOrCancel::Completed(Err(_))
                            | TimeoutOrCancel::TimedOut
                            | TimeoutOrCancel::Cancelled => {
                                studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable
                            }
                        };
                        (claim, outcome)
                    }
                }),
            )
            .buffer_unordered(JANITOR_RECONCILIATION_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
            let mut upload_reconciliation_confirmed: u64 = 0;
            let mut upload_reconciliation_retryable: u64 = 0;
            let mut upload_reconciliation_completion_errors: u64 = 0;
            for (claim, outcome) in reconciliation_outcomes {
                match outcome {
                    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::RemoteConfirmed => {
                        upload_reconciliation_confirmed =
                            upload_reconciliation_confirmed.saturating_add(1);
                    }
                    studiofs_meta::snapshot_ports::UploadedFragmentReconciliationOutcome::Retryable => {
                        upload_reconciliation_retryable =
                            upload_reconciliation_retryable.saturating_add(1);
                    }
                }
                let owner = reconciliation_owner.clone();
                let object_id = claim.entry.object_id;
                let meta = self.meta.clone();
                if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.finish_uploaded_fragment_reconciliation(&owner, &claim, outcome)
                })
                .await
                {
                    upload_reconciliation_completion_errors =
                        upload_reconciliation_completion_errors.saturating_add(1);
                    warn!(
                        cycle,
                        object_id,
                        error = %e,
                        "janitor: upload reconciliation completion failed"
                    );
                }
            }

            let meta = self.meta.clone();
            let referenced = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.get_referenced_object_ids()
            })
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(cycle, error = %e, "janitor cycle failed: get_referenced_object_ids");
                    continue;
                }
            };
            let meta = self.meta.clone();
            let pending_map: HashMap<u64, i64> =
                match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.get_pending_deletions()
                })
                .await
                {
                    Ok(v) => v.into_iter().collect(),
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read pending deletions");
                        HashMap::new()
                    }
                };

            let meta = self.meta.clone();
            let recovered_undo_parents = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.recover_pending_creates()
            })
            .await
            {
                Ok(parents) => parents,
                Err(e) => {
                    warn!(cycle, error = %e, "janitor: pending-create replay failed");
                    Vec::new()
                }
            };
            if !recovered_undo_parents.is_empty() {
                for parent in recovered_undo_parents.iter().copied() {
                    self.dentry_cache_invalidate_parent(parent);
                }
                self.bump_namespace_for_parents(recovered_undo_parents.iter().copied());
            }
            let meta = self.meta.clone();
            let (undo_total, undo_started, undo_replaying, undo_completed) =
                match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.list_undo_logs()
                })
                .await
                {
                    Ok(records) => {
                        let mut started = 0usize;
                        let mut replaying = 0usize;
                        let mut completed = 0usize;
                        for record in records.iter() {
                            match record.phase {
                                studiofs_meta::engine::UndoLogPhase::Started => {
                                    started = started.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Replaying => {
                                    replaying = replaying.saturating_add(1)
                                }
                                studiofs_meta::engine::UndoLogPhase::Completed => {
                                    completed = completed.saturating_add(1)
                                }
                            }
                        }
                        (records.len(), started, replaying, completed)
                    }
                    Err(e) => {
                        warn!(cycle, error = %e, "janitor: failed to read undo-log metrics");
                        (0usize, 0usize, 0usize, 0usize)
                    }
                };

            let lister = match self.data_write.list_object_entries(&self.data_prefix).await {
                Ok(v) => v,
                Err(e) => {
                    let meta = self.meta.clone();
                    let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.release_global_janitor_lock()
                    })
                    .await;
                    warn!(
                        cycle,
                        prefix = self.data_prefix,
                        error = %e,
                        "janitor cycle failed: list data prefix"
                    );
                    continue;
                }
            };
            let mut lister = std::pin::pin!(lister);

            let now = SystemTime::now();
            let now_unix = now
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let mut scanned: u64 = 0;
            let mut orphan_candidates: u64 = 0;
            let mut deleted: u64 = 0;
            let mut skipped_young: u64 = 0;
            let mut skipped_unknown_age: u64 = 0;
            let mut skipped_cap: u64 = 0;
            let mut skipped_non_object_key: u64 = 0;
            let mut delete_errors: u64 = 0;
            let mut seen_objects: HashSet<u64> = HashSet::new();
            let deletion_owner = format!("janitor-delete-{cycle}");

            let mut list_error = None;
            while let Some(entry_result) = lister.next().await {
                let entry = match entry_result {
                    Ok(entry) => entry,
                    Err(err) => {
                        list_error = Some(err);
                        break;
                    }
                };
                scanned = scanned.saturating_add(1);
                let path = entry.key.as_str();
                let object_id = match parse_object_id_from_key(path) {
                    Ok(id) => id,
                    Err(_) => {
                        skipped_non_object_key = skipped_non_object_key.saturating_add(1);
                        continue;
                    }
                };
                if !seen_objects.insert(object_id) {
                    continue;
                }
                if referenced.contains(&object_id) {
                    continue;
                }
                if global_snapshot_pins.contains(&object_id) {
                    continue;
                }
                orphan_candidates = orphan_candidates.saturating_add(1);
                if deleted >= self.janitor_max_deletes_per_run {
                    skipped_cap = skipped_cap.saturating_add(1);
                    continue;
                }

                let mut is_old_enough = false;
                if let Some(ts) = pending_map.get(&object_id) {
                    let age = now_unix.saturating_sub(*ts) as u64;
                    if age >= self.janitor_skip_newer_than.as_secs() {
                        is_old_enough = true;
                    }
                } else if let Some(last_modified_st) = entry.last_modified {
                    let age = now
                        .duration_since(last_modified_st)
                        .unwrap_or_else(|_| Duration::from_secs(0));
                    if age >= self.janitor_skip_newer_than {
                        is_old_enough = true;
                    }
                } else {
                    skipped_unknown_age = skipped_unknown_age.saturating_add(1);
                }

                if !is_old_enough {
                    skipped_young = skipped_young.saturating_add(1);
                    continue;
                }

                let meta = self.meta.clone();
                let object_deletion_owner = deletion_owner.clone();
                let claim_owner = object_deletion_owner.clone();
                let deletion_claim = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.claim_uploaded_object_deletion(object_id, &claim_owner)
                })
                .await
                {
                    Ok(claim) => claim,
                    Err(error) => {
                        delete_errors = delete_errors.saturating_add(1);
                        warn!(cycle, object_id, error = %error, "janitor: failed to claim object deletion");
                        continue;
                    }
                };
                let fencing_token = match deletion_claim {
                    studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Untracked => None,
                    studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Claimed {
                        fencing_token,
                    } => Some(fencing_token),
                    studiofs_meta::snapshot_ports::UploadedObjectDeletionClaim::Protected => {
                        continue;
                    }
                };

                let delete_results = futures::future::join_all(
                    format_object_fragment_keys(&self.data_prefix, object_id)
                        .into_iter()
                        .map(|key| async {
                            let result = self.data_write.delete_by_key(&key).await;
                            (key, result)
                        }),
                )
                .await;
                let mut delete_failed = false;
                for (key, result) in delete_results {
                    if let Err(e) = result {
                        if matches!(&e, DataError::OpenDal(oe) if oe.kind() == ErrorKind::NotFound)
                        {
                            continue;
                        }
                        delete_failed = true;
                        warn!(cycle, key, error = %e, "janitor: fragment delete failed");
                    }
                }
                let deletion_outcome = if delete_failed {
                    studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Retryable
                } else {
                    studiofs_meta::snapshot_ports::UploadedObjectDeletionOutcome::Deleted
                };
                if let Some(fencing_token) = fencing_token {
                    let meta = self.meta.clone();
                    if let Err(error) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                        engine.finish_uploaded_object_deletion(
                            object_id,
                            &object_deletion_owner,
                            fencing_token,
                            deletion_outcome,
                        )
                    })
                    .await
                    {
                        delete_errors = delete_errors.saturating_add(1);
                        warn!(cycle, object_id, error = %error, "janitor: failed to finish object deletion claim");
                        continue;
                    }
                }
                if delete_failed {
                    delete_errors = delete_errors.saturating_add(1);
                    continue;
                }
                deleted = deleted.saturating_add(1);
            }

            if let Some(err) = list_error {
                let meta = self.meta.clone();
                let _ = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.release_global_janitor_lock()
                })
                .await;
                warn!(
                    cycle,
                    prefix = self.data_prefix,
                    error = %err,
                    "janitor cycle failed while streaming data prefix"
                );
                continue;
            }

            debug!(
                cycle,
                scanned,
                referenced = referenced.len(),
                snapshot_pins = global_snapshot_pins.len(),
                upload_reconciliation_claims = reconciliation_claim_count,
                upload_reconciliation_confirmed,
                upload_reconciliation_retryable,
                upload_reconciliation_completion_errors,
                orphan_candidates,
                recovered_undo_parents = recovered_undo_parents.len(),
                undo_total,
                undo_started,
                undo_replaying,
                undo_completed,
                deleted,
                skipped_young,
                skipped_unknown_age,
                skipped_cap,
                skipped_non_object_key,
                delete_errors,
                "janitor cycle complete"
            );

            let meta = self.meta.clone();
            if let Err(e) = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.release_global_janitor_lock()
            })
            .await
            {
                warn!(cycle, error = %e, "janitor cycle failed to release PG lock");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(value: &str) -> Option<String> {
        Some(value.to_string())
    }

    #[test]
    fn notify_wake_skips_poll_sync_when_backend_already_advanced_tokens() {
        assert!(!should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::BackendNotify,
            &token("1"),
            &token("1"),
            &token("2"),
            &token("1"),
        ));
    }

    #[test]
    fn notify_wake_still_syncs_when_tokens_did_not_advance() {
        assert!(should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::BackendNotify,
            &token("1"),
            &token("1"),
            &token("1"),
            &token("1"),
        ));
    }

    #[test]
    fn fallback_poll_syncs_even_when_local_tokens_changed() {
        assert!(should_run_invalidation_poll_sync(
            MetadataInvalidationWakeReason::FallbackPoll,
            &token("1"),
            &token("1"),
            &token("2"),
            &token("1"),
        ));
    }

    #[test]
    fn unprimed_metadata_tokens_force_first_invalidation() {
        let (namespace_changed, metadata_changed) =
            metadata_token_changes(&None, &None, &token("7"), &token("9"));

        assert!(
            namespace_changed,
            "startup namespace token must be treated as pending until the first worker wake"
        );
        assert!(
            metadata_changed,
            "startup metadata token must be treated as pending until the first worker wake"
        );
    }

    #[test]
    fn matching_metadata_tokens_do_not_invalidate_after_first_wake() {
        let (namespace_changed, metadata_changed) =
            metadata_token_changes(&token("7"), &token("9"), &token("7"), &token("9"));

        assert!(!namespace_changed);
        assert!(!metadata_changed);
    }

    #[test]
    fn metadata_snapshot_change_forces_namespace_invalidation_when_token_is_stale() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 10, 20]);

        assert!(metadata_namespace_changed(false, Some(&previous), &current));
    }

    #[test]
    fn metadata_namespace_entry_replacement_invalidates_the_same_name() {
        let previous = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 10)]);
        let current = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 20)]);

        assert_eq!(
            metadata_namespace_entry_changes(Some(&previous), &current),
            HashSet::from([(ROOT_INO, b"recovery.bin".to_vec())])
        );
    }

    #[test]
    fn initial_metadata_namespace_snapshot_only_establishes_entry_baseline() {
        let current = HashSet::from([(ROOT_INO, b"recovery.bin".to_vec(), 10)]);

        assert!(metadata_namespace_entry_changes(None, &current).is_empty());
    }

    #[test]
    fn unchanged_metadata_snapshot_does_not_force_namespace_invalidation() {
        let snapshot = HashSet::from([ROOT_INO, 10]);

        assert!(!metadata_namespace_changed(
            false,
            Some(&snapshot),
            &snapshot
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn metadata_invalidation_fallback_timer_ticks_immediately() {
        let mut timer = metadata_invalidation_fallback_timer(Duration::from_secs(60));

        assert!(
            tokio::time::timeout(Duration::ZERO, timer.tick())
                .await
                .is_ok(),
            "the first fallback poll must establish the startup invalidation baseline"
        );
    }

    #[test]
    fn metadata_background_invalidation_keeps_runtime_only_inodes_in_attr_cache_set() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 20]);
        let runtime_only = HashSet::from([30]);

        let attr_cache_inodes =
            metadata_attr_cache_invalidation_set(&previous, &current, true, runtime_only);

        assert!(
            attr_cache_inodes.contains(&30),
            "runtime-only inodes should still clear VFS attr/read caches after namespace changes"
        );
    }

    #[test]
    fn metadata_background_invalidation_includes_metadata_only_published_inodes() {
        let previous = HashSet::from([ROOT_INO, 10]);
        let current = HashSet::from([ROOT_INO, 10, 20]);

        let attr_cache_inodes =
            metadata_attr_cache_invalidation_set(&previous, &current, false, HashSet::new());

        assert!(
            attr_cache_inodes.contains(&20),
            "metadata-only publishes must invalidate newly reachable inode attrs"
        );
    }

    #[test]
    fn metadata_background_invalidation_clears_read_cache_for_payload_only_changes() {
        let attr_cache_inodes = HashSet::from([10]);
        let changed_payload_inodes = HashSet::from([20]);

        let read_cache_inodes =
            StudioVfs::read_cache_invalidation_set(&attr_cache_inodes, &changed_payload_inodes);

        assert!(
            read_cache_inodes.contains(&20),
            "payload-only graph updates must drop stale published read plans"
        );
        assert!(
            read_cache_inodes.contains(&10),
            "attr invalidations must keep clearing read-plan cache entries"
        );
    }

    #[test]
    fn epoch_finalizer_retries_after_waiting_for_uploads() {
        let retry_base = Duration::from_millis(100);

        assert_eq!(
            epoch_finalization_retry_delay(
                &commit::EpochFinalizeStatus::WaitingForUploads,
                retry_base,
            ),
            Some(retry_base)
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Deferred, retry_base),
            Some(retry_base)
        );
        assert_eq!(
            epoch_finalization_retry_delay(
                &commit::EpochFinalizeStatus::WaitingForUploads,
                Duration::ZERO,
            ),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Published, retry_base),
            None
        );
        assert_eq!(
            epoch_finalization_retry_delay(&commit::EpochFinalizeStatus::Skipped, retry_base),
            None
        );
    }
}
