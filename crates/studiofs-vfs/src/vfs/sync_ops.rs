use super::{
    AccessLevel, DeferredInodeGateEntry, DirtyAttrPersistPolicy, ExplicitFlushOutcome,
    FileSessionOperation, FileType, FsyncOutcome, HandleReleaseContext, Ino, InodeAttr,
    LastHandleReleaseAction, LocalSyncBarrierOutcome, LocalSyncPolicy, MetaError, MetaUploadState,
    OpenFile, OpenFlags, PrefetchedWriterStackObjectIds, ReleaseFhOutcome, StickyErrorPolicy,
    StudioVfs, VfsResult,
};
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::runtime::run_meta;
use crate::utils::now_unix_millis;
use crate::{VfsError, VfsMeta};

use std::time::{Duration, Instant};
use studiofs_meta::types::block_index_from_offset;

const MAX_FLUSH_QUIESCE_PASSES: usize = 8;

enum VfsWritePayload<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl VfsWritePayload<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Borrowed(data) => data.len(),
            Self::Owned(data) => data.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(data) => data,
            Self::Owned(data) => data,
        }
    }
}

pub(super) fn merge_write_dirty_attr(attr: &mut InodeAttr, base_attr: &InodeAttr) {
    if attr.content_epoch < base_attr.content_epoch {
        *attr = base_attr.clone();
    } else if attr.content_epoch == base_attr.content_epoch
        && base_attr.upload_state != MetaUploadState::Ready
    {
        attr.upload_state = base_attr.upload_state;
    }
}

impl StudioVfs {
    async fn flush_metadata_barrier(&self) -> VfsResult<()> {
        self.drain_deferred_namespace_metadata_for_barrier().await?;
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.flush_local_metadata()
        })
        .await
    }

    async fn sync_metadata_barrier(&self) -> VfsResult<()> {
        #[cfg(test)]
        self.state
            .metrics
            .metadata_sync_barrier_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.drain_deferred_namespace_metadata_for_barrier().await?;
        self.wait_for_volatile_overlay_batches().await?;
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.sync_local_metadata()
        })
        .await?;
        self.ensure_volatile_overlay_metadata_healthy()
    }

    /// Spawn a background task that replays landing-zone segments left from a previous run.
    ///
    /// This is used during startup to recover data that was durably written locally but not
    /// yet integrated into the normal dirty/memory materialization pipeline.
    pub(super) fn spawn_landing_zone_recovery_worker(self: &Arc<Self>) {
        let vfs = self.clone();
        self.track_background_task(tokio::spawn(async move {
            if let Err(error) = vfs.recover_landing_zone_for_startup().await {
                warn!(error = %error, "landing-zone startup recovery failed");
            }
        }));
    }

    async fn recover_landing_zone_for_startup(&self) -> VfsResult<()> {
        let recovered = self
            .landing_zone
            .recover_with_data()
            .map_err(VfsError::Io)?;
        let mut replayed_inodes = BTreeSet::new();
        for item in recovered {
            self.write_with_local_sync_policy(
                item.segment.ino,
                item.segment.offset,
                &item.data,
                LocalSyncPolicy::Strict,
            )
            .await?;
            replayed_inodes.insert(item.segment.ino);
        }
        self.landing_zone
            .clear_inodes(&replayed_inodes)
            .map_err(VfsError::Io)?;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn arm_fail_next_inline_dirty_attr_persist_for_test(&self) {
        self.fail_next_inline_dirty_attr_persist
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn fail_inline_dirty_attr_persist_for_test(&self) -> VfsResult<()> {
        if self
            .fail_next_inline_dirty_attr_persist
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            Err(super::VfsError::Other(
                "simulated inline dirty attr persist failure".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    #[cfg(not(test))]
    fn fail_inline_dirty_attr_persist_for_test(&self) -> VfsResult<()> {
        Ok(())
    }

    fn suppress_deleted_inode_materialization_error(
        &self,
        ino: Ino,
        error: &super::VfsError,
    ) -> bool {
        if !matches!(
            error,
            super::VfsError::Meta(studiofs_meta::MetaError::InodeNotFound(_))
        ) {
            return false;
        }
        if self.handle_table.open_count(ino) != 0 {
            return false;
        }
        self.state.writer_stack.clear_inode(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        true
    }

    fn should_continue_deferred_materialization(&self, ino: Ino) -> bool {
        self.state
            .writer_stack
            .completed_block_count_for_inode(ino)
            .is_ok_and(|count| count > 0)
    }

    fn should_linger_deferred_materialization(&self, ino: Ino) -> bool {
        if self.shutdown_requested() {
            return false;
        }
        if self.handle_table.open_count(ino) == 0 {
            return false;
        }
        self.should_continue_deferred_materialization(ino)
    }

    fn should_linger_deferred_tail_materialization(&self, ino: Ino) -> bool {
        !self.shutdown_requested()
            && self.handle_table.write_open_count(ino) > 0
            && self.state.writer_stack.dirty_bytes_for_inode(ino) > 0
    }

    fn should_linger_clean_deferred_materialization(&self, ino: Ino) -> bool {
        !self.shutdown_requested() && self.handle_table.write_open_count(ino) > 0
    }

    fn deferred_materialization_linger_duration() -> Duration {
        Duration::from_millis(
            super::DEFERRED_MATERIALIZATION_LINGER_WAIT_MS
                * super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES as u64,
        )
    }

    pub(super) async fn wait_for_deferred_materialization_inflight(&self, ino: Ino) {
        self.state
            .deferred_materialize_inflight
            .wait_interrupting(ino)
            .await;
        self.state
            .deferred_materialize_reschedule_needed
            .lock()
            .remove(&ino);
    }

    pub(super) async fn wait_for_deferred_release_inflight(&self, ino: Ino) {
        self.state.deferred_release_inflight.wait(ino).await;
    }

    pub(super) fn register_deferred_release_barrier(
        &self,
        ino: Ino,
    ) -> (DeferredInodeGateEntry, tokio::sync::OwnedSemaphorePermit) {
        let replaced = self.state.deferred_release_inflight.contains(ino);
        let (entry, guard) = self.state.deferred_release_inflight.replace(ino);
        if replaced {
            debug!(
                ino,
                "replaced in-flight deferred last-handle release barrier"
            );
        }
        (entry, guard)
    }

    fn observe_sustained_write_admission(
        &self,
        ino: Ino,
        offset: u64,
        len: usize,
    ) -> super::SustainedWriteAdmission {
        if len == 0 {
            return super::SustainedWriteAdmission::default();
        }
        let dirty_bytes_after_write = self
            .state
            .writer_stack
            .dirty_bytes_for_inode(ino)
            .saturating_add(len);
        let mut streams = self.state.sustained_write_streams.lock();
        streams.entry(ino).or_default().observe(
            now_unix_millis(),
            offset,
            len,
            self.write_eager_flush_drain_chunk_bytes,
            dirty_bytes_after_write,
        )
    }

    pub(super) async fn maybe_pre_drain_sustained_write_buffer(
        self: &Arc<Self>,
        ino: Ino,
        offset: u64,
        len: usize,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u64> {
        if !local_sync_policy.is_opportunistic() || len == 0 {
            return Ok(0);
        }
        let admission = self.observe_sustained_write_admission(ino, offset, len);
        if !admission.pre_drain {
            return Ok(0);
        }
        let pressure_after_write = self.total_pending_write_buffer_bytes().saturating_add(len);
        let pacing_threshold = self
            .max_dirty_bytes()
            .saturating_div(2)
            .max(self.write_eager_flush_drain_chunk_bytes);
        // Sustained-stream detection is a pre-drain signal only. Adaptive
        // admission already owns pacing from measured pressure and drain rate;
        // adding a second time-based sleep here produces periodic callback stalls.
        let delay_ms = 0;

        // #region debug log
        self.debug_write_probe(
            "H2",
            concat!(file!(), ":", line!()),
            "sustained write pre-drain",
            serde_json::json!({
                "ino": ino,
                "offset": offset,
                "len": len,
                "duration_ms": admission.duration_ms,
                "delay_ms": delay_ms,
                "raw_delay_ms": admission.delay_ms,
                "pacing_threshold_bytes": pacing_threshold,
                "pressure_after_write": pressure_after_write,
                "per_inode_dirty_bytes": self.state.writer_stack.dirty_bytes_for_inode(ino),
                "total_dirty_bytes": self.state.writer_stack.total_dirty_bytes(),
            }),
        );
        // #endregion debug log

        self.spawn_deferred_materialization_if_needed(ino).await;
        Ok(delay_ms)
    }

    fn record_deferred_write_hard_backpressure(&self, ino: Ino, hard_threshold_bytes: usize) {
        self.state
            .local_dirty
            .hard_backpressure_events
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // #region debug log
        self.debug_write_probe(
            "H2",
            concat!(file!(), ":", line!()),
            "hard backpressure observed on deferred write",
            serde_json::json!({
                "ino": ino,
                "per_inode_dirty_bytes": self.state.writer_stack.dirty_bytes_for_inode(ino),
                "total_dirty_bytes": self.total_pending_write_buffer_bytes(),
                "hard_threshold_bytes": hard_threshold_bytes,
                "max_dirty_bytes": self.max_dirty_bytes(),
            }),
        );
        // #endregion debug log
    }

    /// Spawn a background deferred-materialization task for `ino` when needed.
    ///
    /// Deferred materialization drains buffered writer-stack data into the local dirty-state
    /// persistence path in the background, while the inode still has open handles.
    pub(super) async fn spawn_deferred_materialization_if_needed(self: &Arc<Self>, ino: Ino) {
        if self.disable_background_slab_flush {
            return;
        }
        if self.shutdown_requested() {
            return;
        }

        let Some((entry, guard)) = self
            .state
            .deferred_materialize_inflight
            .try_register_or_update_occupied(ino, || {
                if self.state.writer_stack.dirty_bytes_for_inode(ino) > 0 {
                    self.state
                        .deferred_materialize_reschedule_needed
                        .lock()
                        .insert(ino);
                }
            })
        else {
            // #region debug log
            self.debug_write_probe(
                "H2",
                concat!(file!(), ":", line!()),
                "deferred materialization already inflight",
                serde_json::json!({
                    "ino": ino,
                    "per_inode_dirty_bytes": self.state.writer_stack.dirty_bytes_for_inode(ino),
                    "total_dirty_bytes": self.state.writer_stack.total_dirty_bytes(),
                    "open_count": self.handle_table.open_count(ino),
                }),
            );
            // #endregion debug log
            return;
        };
        // #region debug log
        self.debug_write_probe(
            "H2",
            concat!(file!(), ":", line!()),
            "deferred materialization scheduled",
            serde_json::json!({
                "ino": ino,
                "per_inode_dirty_bytes": self.state.writer_stack.dirty_bytes_for_inode(ino),
                "total_dirty_bytes": self.state.writer_stack.total_dirty_bytes(),
                "open_count": self.handle_table.open_count(ino),
            }),
        );
        // #endregion debug log
        self.start_deferred_materialization_task(entry, guard);
    }

    fn start_deferred_materialization_task(
        self: &Arc<Self>,
        entry: DeferredInodeGateEntry,
        guard: tokio::sync::OwnedSemaphorePermit,
    ) {
        let vfs = Arc::clone(self);
        self.track_background_task(tokio::spawn(async move {
            let _guard = guard;
            let ino = entry.ino;
            let mut allow_reschedule_on_exit = true;
            let mut linger_interrupted = false;
            let mut no_progress_retries = 0usize;
            loop {
                let writer_stack_dirty_before = vfs.state.writer_stack.dirty_bytes_for_inode(ino);
                let dirty_before = writer_stack_dirty_before;
                let total_before = vfs.total_pending_write_buffer_bytes();
                if dirty_before == 0 {
                    if no_progress_retries < super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES
                        && vfs.should_linger_clean_deferred_materialization(ino)
                    {
                        no_progress_retries += 1;
                        if entry
                            .wait_linger_or_interrupt(Duration::from_millis(
                                super::DEFERRED_MATERIALIZATION_LINGER_WAIT_MS,
                            ))
                            .await
                        {
                            linger_interrupted = true;
                            break;
                        }
                        continue;
                    }
                    break;
                }

                if let Err(error) = vfs
                    .prefetch_writer_stack_object_ids_for_completed_blocks(ino)
                    .await
                {
                    allow_reschedule_on_exit = false;
                    if !vfs.suppress_deleted_inode_materialization_error(ino, &error) {
                        warn!(
                            ino,
                            error = %error,
                            "deferred writer-stack object id prefetch failed; will retry at next strict barrier"
                        );
                    }
                    break;
                }

                let materialize_writer_stack_started = Instant::now();
                let writer_stack_materialized_bytes =
                    match vfs
                        .materialize_writer_stack_completed_blocks_for_inode(ino)
                        .await
                    {
                    Ok(materialized_bytes) => materialized_bytes,
                    Err(e) => {
                        allow_reschedule_on_exit = false;
                        if !vfs.suppress_deleted_inode_materialization_error(ino, &e) {
                            warn!(
                                ino,
                                error = %e,
                                "deferred writer-stack materialization failed; will retry at next strict barrier"
                            );
                        }
                        break;
                    }
                };
                vfs.record_vfs_write_materialize_completed(
                    materialize_writer_stack_started.elapsed(),
                );

                let materialize_for_limits_started = Instant::now();
                let limits_materialized = match vfs
                    .materialize_writer_stack_completed_blocks_for_limits(ino)
                    .await
                {
                    Ok(materialized) => materialized > 0,
                    Err(e) => {
                        allow_reschedule_on_exit = false;
                        if !vfs.suppress_deleted_inode_materialization_error(ino, &e) {
                            warn!(
                                ino,
                                error = %e,
                                "deferred writer-stack budget materialization failed; will retry at next strict barrier"
                            );
                            // #region debug log
                            vfs.debug_write_probe(
                                "H1",
                                concat!(file!(), ":", line!()),
                                "deferred writer-stack limit-pass failure",
                                serde_json::json!({
                                    "ino": ino,
                                    "error": e.to_string(),
                                    "meta_exists": vfs.sync_meta().get_attr(ino).is_ok(),
                                    "deleted_marker": vfs.state.upload.deleted_inodes.lock().contains(&ino),
                                    "open_count": vfs.handle_table.open_count(ino),
                                    "per_inode_dirty_bytes": vfs.state.writer_stack.dirty_bytes_for_inode(ino),
                                    "total_dirty_bytes": vfs.state.writer_stack.total_dirty_bytes(),
                                }),
                            );
                            // #endregion debug log
                        }
                        break;
                    }
                };
                vfs.record_vfs_write_materialize_for_limits(
                    materialize_for_limits_started.elapsed(),
                );

                let dirty_after = vfs.state.writer_stack.dirty_bytes_for_inode(ino);
                let total_after = vfs.total_pending_write_buffer_bytes();
                let completed_blocks_remain = vfs.should_continue_deferred_materialization(ino);
                let made_progress = writer_stack_materialized_bytes > 0
                    || limits_materialized
                    || dirty_after < dirty_before
                    || total_after < total_before;

                if dirty_after > 0
                    && !completed_blocks_remain
                    && !vfs.should_linger_deferred_tail_materialization(ino)
                {
                    let closed_writer_materialized = match vfs
                        .materialize_writer_stack_for_inode(ino)
                        .await
                    {
                        Ok(materialized_bytes) => materialized_bytes,
                        Err(e) => {
                            allow_reschedule_on_exit = false;
                            if !vfs.suppress_deleted_inode_materialization_error(ino, &e) {
                                warn!(
                                    ino,
                                    error = %e,
                                    "deferred writer-stack materialization after writer close failed; strict barrier will retry"
                                );
                            }
                            break;
                        }
                    };
                    if closed_writer_materialized > 0 {
                        tokio::task::yield_now().await;
                        continue;
                    }
                }

                if dirty_after > 0
                    && !completed_blocks_remain
                    && vfs.should_linger_deferred_tail_materialization(ino)
                {
                    if no_progress_retries >= super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES {
                        let idle_tail_started = Instant::now();
                        let idle_tail_materialized = match vfs
                            .materialize_writer_stack_idle_tail_for_inode(
                                ino,
                                Self::deferred_materialization_linger_duration(),
                            )
                            .await
                        {
                            Ok(materialized_bytes) => materialized_bytes,
                            Err(e) => {
                                allow_reschedule_on_exit = false;
                                if !vfs.suppress_deleted_inode_materialization_error(ino, &e) {
                                    warn!(
                                        ino,
                                        error = %e,
                                        "deferred writer-stack idle-tail materialization failed; will retry at next strict barrier"
                                    );
                                }
                                break;
                            }
                        };
                        vfs.record_vfs_write_materialize_completed(idle_tail_started.elapsed());
                        if idle_tail_materialized > 0 {
                            no_progress_retries = 0;
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }

                    no_progress_retries = no_progress_retries
                        .saturating_add(1)
                        .min(super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES);
                    if entry
                        .wait_linger_or_interrupt(Duration::from_millis(
                            super::DEFERRED_MATERIALIZATION_LINGER_WAIT_MS,
                        ))
                        .await
                    {
                        linger_interrupted = true;
                        break;
                    }
                    continue;
                }

                if made_progress {
                    no_progress_retries = 0;
                } else if no_progress_retries < super::DEFERRED_MATERIALIZATION_LINGER_MAX_RETRIES
                    && vfs.should_linger_deferred_materialization(ino)
                {
                    no_progress_retries += 1;
                    if entry
                        .wait_linger_or_interrupt(Duration::from_millis(
                            super::DEFERRED_MATERIALIZATION_LINGER_WAIT_MS,
                        ))
                        .await
                    {
                        linger_interrupted = true;
                        break;
                    }
                    continue;
                }
                if !made_progress || !completed_blocks_remain {
                    break;
                }
                tokio::task::yield_now().await;
            }

            if linger_interrupted && vfs.state.writer_stack.dirty_bytes_for_inode(ino) > 0 {
                let interrupted_materialize_started = Instant::now();
                match vfs.materialize_writer_stack_for_inode(ino).await {
                    Ok(_) => vfs.record_vfs_write_materialize_completed(
                        interrupted_materialize_started.elapsed(),
                    ),
                    Err(error) => {
                        allow_reschedule_on_exit = false;
                        if !vfs.suppress_deleted_inode_materialization_error(ino, &error) {
                            warn!(
                                ino,
                                error = %error,
                                "interrupted deferred writer-stack materialization failed; strict barrier will retry"
                            );
                        }
                    }
                }
            }

            if vfs.inode_has_local_dirty_state(ino)
                && let Err(error) = vfs
                    .flush_local_dirty_state_background(ino, LocalSyncPolicy::Opportunistic)
                    .await
            {
                allow_reschedule_on_exit = false;
                if !vfs.suppress_deleted_inode_materialization_error(ino, &error) {
                    warn!(
                        ino,
                        error = %error,
                        "deferred local metadata drain failed; will retry at next strict barrier"
                    );
                }
            }

            vfs.finish_deferred_materialization_task(entry, allow_reschedule_on_exit);
        }));
    }

    /// Finish bookkeeping for a deferred-materialization task for `ino`.
    ///
    /// Clears the in-flight marker and optionally re-schedules work if additional dirty bytes
    /// were observed while the task was running.
    pub(super) fn finish_deferred_materialization_task(
        self: &Arc<Self>,
        entry: DeferredInodeGateEntry,
        allow_reschedule: bool,
    ) {
        let ino = entry.ino;
        let next_task = self
            .state
            .deferred_materialize_inflight
            .replace_if_current_or_remove(&entry, || {
                let reschedule_needed = self
                    .state
                    .deferred_materialize_reschedule_needed
                    .lock()
                    .remove(&ino);
                allow_reschedule
                    && reschedule_needed
                    && !self.shutdown_requested()
                    && self.state.writer_stack.dirty_bytes_for_inode(ino) > 0
            });
        let Some((next_entry, next_guard)) = next_task else {
            return;
        };
        self.start_deferred_materialization_task(next_entry, next_guard);
    }

    /// Release a file handle.
    ///
    /// If the inode was unlinked while this handle was open (orphan) and this
    /// was the last open handle, perform the deferred deletion now.
    ///
    /// This is also the last-handle local-state drain point. If a writable
    /// handle reaches release with dirty payloads or attrs still pending,
    /// `release_fh` must push that state through the current local
    /// payload/metadata pipeline before the handle disappears so later opens,
    /// open-unlink cleanup, and sticky-error reporting all see a coherent
    /// inode. In durable mode this remains a strict local-durability barrier;
    /// in fast mode it only guarantees that local unpublished state is
    /// materialized, not that payload bytes have crossed a crash-safe sync
    /// boundary. Clean last-handle closes still enter the file-session gate so
    /// release cannot report close completion while another operation is still
    /// active for the inode.
    pub async fn release_fh(&self, fh: u64) -> VfsResult<()> {
        self.release_fh_with_outcome(fh).await.map(|_| ())
    }

    fn close_handle_for_release(&self, fh: u64) -> Option<HandleReleaseContext> {
        self.state.writer_stack.mark_closed(fh);
        let Some((open_file, remaining)) = self.handle_table.close(fh) else {
            debug!("release_fh: unknown fh={}", fh);
            return None;
        };
        let (os_cache_writer_lease, os_cache_foreground_read_lease) =
            open_file.take_os_cache_release_lifetimes();
        let ino = open_file.ino;
        let stream_id = open_file.stream_id();
        if open_file.active_open_overlay {
            self.sync_meta().note_inactive_open_inode(ino);
        }
        self.state
            .read
            .decrement_active_access_class(open_file.current_access_class());
        debug!("release_fh fh={} ino={} remaining={}", fh, ino, remaining);
        self.state.file_session.note_close_handle(ino, remaining);
        self.clear_access_tracking_for_stream(ino, stream_id);
        self.debug_pending_inode_state(ino, "release_handle_closed");
        if remaining > 0 {
            drop(os_cache_writer_lease);
            drop(os_cache_foreground_read_lease);
            return None;
        }
        if self.inode_has_local_dirty_state(ino) {
            self.invalidate_kernel_inode_attrs([ino]);
        }
        self.flush_deferred_kernel_invalidations(ino);
        self.debug_pending_inode_state(ino, "release_last_handle");
        Some(HandleReleaseContext {
            open_file,
            ino,
            os_cache_writer_lease,
            os_cache_foreground_read_lease,
        })
    }

    async fn cleanup_orphan_on_last_handle_close(&self, ino: Ino) -> VfsResult<ReleaseFhOutcome> {
        if self.release_ignored_entry(ino) {
            return Ok(ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: false,
            });
        }
        // Last handle closed on an unlinked file — final cleanup
        debug!("orphan cleanup: ino={} (last fd closed)", ino);

        // A deferred materialization task may have snapped dirty state before
        // unlink marked this inode deleted. Let it finish first so final orphan
        // cleanup removes the complete graph and no late materialization can
        // recreate payload refs after the last handle closes.
        self.wait_for_deferred_materialization_inflight(ino).await;

        let cleanup = self.collect_orphan_deleted_inode_cleanup(ino).await?;
        let removed_blocks = cleanup.removed_ids.len();
        let freed_bytes = cleanup.freed_bytes;
        let deferred_local_payloads = self
            .finalize_deleted_inode_runtime_cleanup(ino, cleanup.removed_ids)
            .await;
        debug!(
            "orphan cleanup complete: ino={} removed_blocks={} deferred_local_payloads={} freed_bytes={}",
            ino, removed_blocks, deferred_local_payloads, freed_bytes
        );
        Ok(ReleaseFhOutcome {
            closed_last_handle: true,
            flushed_local_dirty_state: false,
        })
    }

    fn take_last_handle_release_action(
        &self,
        context: &HandleReleaseContext,
    ) -> LastHandleReleaseAction {
        let ino = context.ino;
        if let Some(action) = self.take_ignored_entry_release_action(ino) {
            return action;
        }

        if self.handle_table.take_orphan(ino) {
            return LastHandleReleaseAction::OrphanCleanup;
        }

        if !self.inode_has_local_dirty_state(ino) {
            return LastHandleReleaseAction::Clean;
        }

        let sticky_error_policy = if context.open_file.flags.contains(OpenFlags::WRONLY)
            || context.open_file.flags.contains(OpenFlags::RDWR)
        {
            StickyErrorPolicy::Report
        } else {
            StickyErrorPolicy::Ignore
        };
        LastHandleReleaseAction::LocalSync {
            sticky_error_policy,
        }
    }

    fn take_ignored_entry_release_action(&self, ino: Ino) -> Option<LastHandleReleaseAction> {
        let attr = self.ignored_entry_attr(ino)?;
        let is_orphan = self.handle_table.take_orphan(ino);
        if is_orphan {
            self.release_ignored_entry(ino);
        } else if attr.kind == FileType::RegularFile {
            self.detach_ignored_entry_name(ino);
        }
        Some(LastHandleReleaseAction::IgnoredEntry)
    }

    async fn execute_last_handle_release_action(
        &self,
        ino: Ino,
        action: LastHandleReleaseAction,
    ) -> VfsResult<ReleaseFhOutcome> {
        match action {
            LastHandleReleaseAction::IgnoredEntry => Ok(ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: false,
            }),
            LastHandleReleaseAction::Clean => {
                self.flush_metadata_barrier().await?;
                Ok(ReleaseFhOutcome {
                    closed_last_handle: true,
                    flushed_local_dirty_state: false,
                })
            }
            LastHandleReleaseAction::OrphanCleanup => {
                let outcome = self.cleanup_orphan_on_last_handle_close(ino).await?;
                self.flush_metadata_barrier().await?;
                Ok(outcome)
            }
            LastHandleReleaseAction::LocalSync {
                sticky_error_policy,
            } => {
                let outcome = match self
                    .local_sync_barrier_with_outcome(
                        ino,
                        self.local_sync_policy_for_barrier(ino),
                        sticky_error_policy,
                        DirtyAttrPersistPolicy::PersistIfPresent,
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error) if Self::is_inode_not_found_error(&error, ino) => {
                        // A metadata miss alone is not a deletion tombstone: cleanup
                        // would evict locally durable payloads before their upload.
                        // Only an explicit open-unlink marker permits the destructive
                        // last-handle orphan path.
                        if self.handle_table.take_orphan(ino) {
                            return self.cleanup_orphan_on_last_handle_close(ino).await;
                        }
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                Ok(ReleaseFhOutcome {
                    closed_last_handle: true,
                    flushed_local_dirty_state: outcome.had_local_dirty_state,
                })
            }
        }
    }

    async fn execute_deferred_last_handle_release_action(
        &self,
        ino: Ino,
        action: LastHandleReleaseAction,
    ) -> VfsResult<ReleaseFhOutcome> {
        match action {
            LastHandleReleaseAction::LocalSync {
                sticky_error_policy,
            } => {
                let outcome = match self
                    .local_sync_barrier_with_outcome(
                        ino,
                        LocalSyncPolicy::Opportunistic,
                        sticky_error_policy,
                        DirtyAttrPersistPolicy::PersistIfPresent,
                    )
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(error) if Self::is_inode_not_found_error(&error, ino) => {
                        // The action was selected before a concurrent unlink marked
                        // the open inode orphaned. Match the synchronous last-handle
                        // path: only that explicit marker permits destructive cleanup.
                        if self.handle_table.take_orphan(ino) {
                            return self.cleanup_orphan_on_last_handle_close(ino).await;
                        }
                        return Err(error);
                    }
                    Err(error) => return Err(error),
                };
                Ok(ReleaseFhOutcome {
                    closed_last_handle: true,
                    flushed_local_dirty_state: outcome.had_local_dirty_state,
                })
            }
            action => self.execute_last_handle_release_action(ino, action).await,
        }
    }

    async fn release_last_handle_with_outcome(
        &self,
        context: HandleReleaseContext,
    ) -> VfsResult<ReleaseFhOutcome> {
        let ino = context.ino;
        let action = self.take_last_handle_release_action(&context);
        let result = self.execute_last_handle_release_action(ino, action).await;
        drop(context);
        result
    }

    /// Release a handle and report whether this close owned the actual
    /// last-handle path, plus whether it had to drain local dirty state there.
    pub async fn release_fh_with_outcome(&self, fh: u64) -> VfsResult<ReleaseFhOutcome> {
        let Some(context) = self.close_handle_for_release(fh) else {
            // Non-final handle closes stop here and never enter the
            // last-handle drain path, so any error returned by this method
            // necessarily comes from the final release flow below.
            return Ok(ReleaseFhOutcome::default());
        };
        let ino = context.ino;
        if let Some(action) = self.take_ignored_entry_release_action(ino) {
            let result = self.execute_last_handle_release_action(ino, action).await;
            self.state.file_session.gc_if_idle(ino);
            return result;
        }

        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Release)
            .await;
        let result = self.release_last_handle_with_outcome(context).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Release a handle and defer the last-handle durability barrier to a background task.
    ///
    /// This returns synchronously with an outcome describing whether this close observed the
    /// last-handle transition, while the actual last-handle drain work is executed later on the
    /// provided runtime (and any error is logged).
    pub fn release_fh_with_deferred_last_handle_barrier(
        self: &Arc<Self>,
        fh: u64,
        runtime: &tokio::runtime::Handle,
    ) -> ReleaseFhOutcome {
        let Some(context) = self.close_handle_for_release(fh) else {
            return ReleaseFhOutcome::default();
        };
        let ino = context.ino;
        let action = self.take_last_handle_release_action(&context);
        let flushed_local_dirty_state = matches!(action, LastHandleReleaseAction::LocalSync { .. });
        if matches!(
            action,
            LastHandleReleaseAction::Clean | LastHandleReleaseAction::IgnoredEntry
        ) {
            self.state.file_session.gc_if_idle(ino);
            return ReleaseFhOutcome {
                closed_last_handle: true,
                flushed_local_dirty_state: false,
            };
        }
        let HandleReleaseContext {
            os_cache_writer_lease,
            os_cache_foreground_read_lease,
            ..
        } = context;
        let vfs = Arc::clone(self);
        let (barrier, guard) = self.register_deferred_release_barrier(ino);
        self.track_background_task(runtime.spawn(async move {
            let _os_cache_writer_lease = os_cache_writer_lease;
            let _os_cache_foreground_read_lease = os_cache_foreground_read_lease;
            tokio::task::yield_now().await;
            let session = vfs
                .state
                .file_session
                .enter_op(ino, FileSessionOperation::Release)
                .await;
            let result = vfs
                .execute_deferred_last_handle_release_action(ino, action)
                .await;
            drop(session);
            vfs.state.file_session.gc_if_idle(ino);
            if let Err(error) = result {
                warn!(
                    ino,
                    error = %error,
                    "deferred last-handle release barrier failed"
                );
            }
            // Drop the registry entry before releasing the async guard so waiters cannot
            // wake up and observe an unlocked but still-registered barrier.
            drop(barrier);
            drop(guard);
        }));
        ReleaseFhOutcome {
            closed_last_handle: true,
            flushed_local_dirty_state,
        }
    }

    /// Admit a platform writeback flush to the shared volatile local-dirty worker.
    ///
    /// Ordinary flush returns after the VFS has accepted the latest local state;
    /// the background worker commits metadata within its bounded age window.
    /// `fsync` and shutdown use the strict local-redb barrier instead.
    pub async fn flush_volatile_writeback_with_outcome(
        self: &Arc<Self>,
        ino: Ino,
        fh: u64,
    ) -> VfsResult<ExplicitFlushOutcome> {
        let target_ino = if fh != 0 {
            self.handle_table.get(fh).map(|open_file| open_file.ino)
        } else {
            None
        }
        .unwrap_or(ino);
        self.wait_for_deferred_release_inflight(target_ino).await;
        self.ensure_volatile_overlay_metadata_healthy()?;
        let had_local_dirty_state = self.inode_has_local_dirty_state(target_ino);
        if self.state.writer_stack.has_dirty_writers(target_ino) {
            self.spawn_deferred_materialization_if_needed(target_ino)
                .await;
        }
        Ok(ExplicitFlushOutcome {
            flushed_local_dirty_state: had_local_dirty_state,
        })
    }

    async fn local_sync_barrier_with_outcome(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
        sticky_error_policy: StickyErrorPolicy,
        dirty_attr_persist_policy: DirtyAttrPersistPolicy,
    ) -> VfsResult<LocalSyncBarrierOutcome> {
        // Wait for any in-flight deferred materialization task to finish
        // before we run our own strict materialization pass. A strict waiter
        // interrupts only the task's coalescing sleep; the task still drains
        // its accepted tail and normal local-dirty epilogue under the existing
        // gate. Without this ordering the flush/close path and the background
        // task would race on writer-stack snapshots, producing fragmented
        // blocks.
        //
        // The parking_lot lock is scoped so the MutexGuard (which is !Send)
        // is dropped before the .await point.  We then acquire the
        // tokio::sync::Mutex which blocks until the background task releases it.
        self.wait_for_deferred_materialization_inflight(ino).await;

        let mut materialized_writer_stack_bytes =
            self.materialize_writer_stack_for_inode(ino).await?;
        let flushed_file_data = materialized_writer_stack_bytes > 0
            || self.has_pending_local_dirty_payloads_for_inode(ino);
        let residual_pending_local_payloads = self.has_pending_local_dirty_payloads_for_inode(ino);
        let dirty_attr_present = self.state.namespace.dirty_attrs.lock().contains_key(&ino);
        let persist_dirty_attr_inline = !residual_pending_local_payloads
            && dirty_attr_present
            && matches!(
                dirty_attr_persist_policy,
                DirtyAttrPersistPolicy::PersistIfPresent
            );
        let residual_local_dirty_state =
            residual_pending_local_payloads || (dirty_attr_present && !persist_dirty_attr_inline);
        let had_local_dirty_state = materialized_writer_stack_bytes > 0
            || residual_local_dirty_state
            || persist_dirty_attr_inline;
        let persisted_slice_bytes_before_flush =
            self.total_pending_persisted_local_dirty_slice_bytes();
        if residual_local_dirty_state {
            if local_sync_policy.requires_write_path_durability() {
                materialized_writer_stack_bytes = materialized_writer_stack_bytes.saturating_add(
                    self.flush_local_dirty_state_until_quiescent(ino, local_sync_policy)
                        .await?,
                );
            } else {
                self.flush_local_dirty_state(ino, local_sync_policy).await?;
            }
        }
        let mut payload_store_sync_performed = false;
        if local_sync_policy.requires_write_path_durability() {
            payload_store_sync_performed |= self
                .maybe_payload_store_sync_data_async(
                    materialized_writer_stack_bytes
                        .saturating_add(persisted_slice_bytes_before_flush)
                        as u64,
                    local_sync_policy,
                )
                .await?;
        }
        if local_sync_policy.requires_write_path_durability()
            && (self.state.writer_stack.has_dirty_writers(ino)
                || self.has_pending_local_dirty_payloads_for_inode(ino))
        {
            materialized_writer_stack_bytes = materialized_writer_stack_bytes.saturating_add(
                self.flush_local_dirty_state_until_quiescent(ino, local_sync_policy)
                    .await?,
            );
            payload_store_sync_performed |= self
                .maybe_payload_store_sync_data_async(
                    materialized_writer_stack_bytes as u64,
                    local_sync_policy,
                )
                .await?;
        }
        if local_sync_policy.requires_write_path_durability()
            && !payload_store_sync_performed
            && self.inode_has_pending_uploads(ino)
        {
            // A background drain may have created the upload record after the
            // writer-stack snapshot, without contributing to the sync group.
            self.payload_store_sync_data_async().await?;
        }

        let dirty_attr = match dirty_attr_persist_policy {
            DirtyAttrPersistPolicy::Ignore => None,
            DirtyAttrPersistPolicy::PersistIfPresent if !persist_dirty_attr_inline => None,
            DirtyAttrPersistPolicy::PersistIfPresent => {
                self.state.namespace.dirty_attrs.lock().get(&ino).cloned()
            }
        };
        let persisted_dirty_attrs = if let Some(attr) = dirty_attr {
            if self
                .discard_stale_zero_size_dirty_attr_after_current_publish(ino, &attr)
                .await?
            {
                false
            } else {
                self.fail_inline_dirty_attr_persist_for_test()?;
                let meta = self.meta.clone();
                let attr_to_persist = attr.clone();
                let committed_attr = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.flush_regular_file_overlay_slices_with_attr(
                        ino,
                        &[],
                        Some(&attr_to_persist),
                    )
                })
                .await
                {
                    Ok(committed_attr) => committed_attr,
                    Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                        warn!(
                            ino,
                            "dirty attr persist skipped because authoritative inode was removed"
                        );
                        return Ok(LocalSyncBarrierOutcome {
                            had_local_dirty_state,
                            flushed_file_data,
                            persisted_dirty_attrs: false,
                        });
                    }
                    Err(error) => return Err(error),
                };
                let committed_attr = committed_attr.as_ref().unwrap_or(&attr);
                self.clear_matching_dirty_attr_if_committed(ino, committed_attr);
                self.invalidate_attr_read_cache([ino]);
                self.refresh_inode_incarnation_state_from_attr(ino, committed_attr);
                true
            }
        } else {
            false
        };

        if matches!(sticky_error_policy, StickyErrorPolicy::Report) {
            self.ensure_no_upload_sticky_error(ino)?;
        } else if let Some(msg) = self
            .state
            .upload
            .upload_sticky_errors
            .lock()
            .get(&ino)
            .cloned()
        {
            warn!(
                ino,
                error = %msg,
                "sticky upload error present at flush barrier (policy=Ignore, local data safe)"
            );
        }

        if local_sync_policy.requires_write_path_durability() {
            self.sync_metadata_barrier().await?;
        } else {
            self.flush_metadata_barrier().await?;
        }

        Ok(LocalSyncBarrierOutcome {
            had_local_dirty_state,
            flushed_file_data,
            persisted_dirty_attrs,
        })
    }

    async fn flush_local_dirty_state_until_quiescent(
        &self,
        ino: Ino,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<usize> {
        let mut materialized_writer_stack_bytes = 0usize;
        // A strict flush is a finite barrier: each pass drains dirty state
        // that may have appeared while the previous pass materialized slices.
        for _ in 0..MAX_FLUSH_QUIESCE_PASSES {
            materialized_writer_stack_bytes = materialized_writer_stack_bytes
                .saturating_add(self.materialize_writer_stack_for_inode(ino).await?);
            if !self.state.writer_stack.has_dirty_writers(ino)
                && !self.has_pending_local_dirty_payloads_for_inode(ino)
            {
                return Ok(materialized_writer_stack_bytes);
            }
            self.flush_local_dirty_state(ino, local_sync_policy).await?;
            if !self.state.writer_stack.has_dirty_writers(ino)
                && !self.has_pending_local_dirty_payloads_for_inode(ino)
            {
                return Ok(materialized_writer_stack_bytes);
            }
        }
        Err(VfsError::Other(format!(
            "strict flush barrier failed to quiesce local dirty state for inode {ino} after {MAX_FLUSH_QUIESCE_PASSES} passes"
        )))
    }

    fn local_sync_policy_for_barrier(&self, ino: Ino) -> LocalSyncPolicy {
        let has_pending_local_state =
            self.inode_has_local_dirty_state(ino) || self.state.writer_stack.has_dirty_writers(ino);
        if has_pending_local_state {
            LocalSyncPolicy::Strict
        } else {
            LocalSyncPolicy::Opportunistic
        }
    }

    /// Run an ino-scoped local sync barrier and return whether it flushed local dirty state.
    ///
    /// This is used for callback paths that need an explicit per-inode durability barrier.
    pub(super) async fn flush_inode_barrier_with_outcome(
        &self,
        ino: Ino,
        sticky_error_policy: StickyErrorPolicy,
    ) -> VfsResult<ExplicitFlushOutcome> {
        self.wait_for_deferred_release_inflight(ino).await;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Flush)
            .await;
        let result: VfsResult<ExplicitFlushOutcome> = async {
            let outcome = self
                .local_sync_barrier_with_outcome(
                    ino,
                    self.local_sync_policy_for_barrier(ino),
                    sticky_error_policy,
                    DirtyAttrPersistPolicy::Ignore,
                )
                .await?;
            Ok(ExplicitFlushOutcome {
                flushed_local_dirty_state: outcome.had_local_dirty_state,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Flush data for a file handle (per-fd close in POSIX).
    ///
    /// Called by FUSE `flush()`. Flushes pending writes for the inode.
    ///
    /// This is the explicit per-handle barrier used by FUSE and by WinFSP
    /// callback paths that decide to make dirty state explicit while the handle
    /// is still valid. Clean handles should return quickly; dirty handles must
    /// persist current local state before success or error is reported. Under
    /// the single-mode Fast Local Authority contract this is a strict local-
    /// durability barrier whenever dirty unpublished state remains.
    pub async fn flush_fh_with_outcome(&self, fh: u64) -> VfsResult<ExplicitFlushOutcome> {
        self.flush_fh_with_outcome_inner(fh, false).await
    }

    async fn flush_fh_with_outcome_inner(
        &self,
        fh: u64,
        force_strict_metadata_sync: bool,
    ) -> VfsResult<ExplicitFlushOutcome> {
        let Some(open_file) = self.handle_table.get(fh) else {
            return Ok(ExplicitFlushOutcome::default());
        };
        let ((), metadata_result) = tokio::join!(
            self.wait_for_deferred_release_inflight(open_file.ino),
            self.wait_for_deferred_namespace_metadata(open_file.ino)
        );
        metadata_result?;
        if !self.inode_has_local_dirty_state(open_file.ino)
            && !self.state.writer_stack.has_dirty_writers(open_file.ino)
        {
            if force_strict_metadata_sync {
                let outcome = self
                    .local_sync_barrier_with_outcome(
                        open_file.ino,
                        LocalSyncPolicy::Strict,
                        StickyErrorPolicy::Ignore,
                        DirtyAttrPersistPolicy::Ignore,
                    )
                    .await?;
                self.state.file_session.gc_if_idle(open_file.ino);
                return Ok(ExplicitFlushOutcome {
                    flushed_local_dirty_state: outcome.had_local_dirty_state,
                });
            } else {
                self.flush_metadata_barrier().await?;
            }
            self.state.file_session.gc_if_idle(open_file.ino);
            return Ok(ExplicitFlushOutcome::default());
        }
        let session = self
            .state
            .file_session
            .enter_op(open_file.ino, FileSessionOperation::Flush)
            .await;
        // Flush guarantees LOCAL durability (NVMe) — not S3 upload completion.
        // With deferred materialization the upload pipeline may still be
        // processing blocks when flush runs.  A transient upload failure would
        // set a sticky error that used to fail the flush, even though data was
        // safely persisted locally.  Use Ignore here (consistent with the
        // ino-level flush_with_outcome) so that a background upload hiccup
        // does not surface as a flush error to the application.
        let result: VfsResult<ExplicitFlushOutcome> = async {
            let outcome = match self
                .local_sync_barrier_with_outcome(
                    open_file.ino,
                    if force_strict_metadata_sync {
                        LocalSyncPolicy::Strict
                    } else {
                        self.local_sync_policy_for_barrier(open_file.ino)
                    },
                    StickyErrorPolicy::Ignore,
                    DirtyAttrPersistPolicy::PersistIfPresent,
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(error) if Self::is_inode_not_found_error(&error, open_file.ino) => {
                    self.handle_table.mark_orphan(open_file.ino);
                    return Ok(ExplicitFlushOutcome::default());
                }
                Err(error) => return Err(error),
            };
            Ok(ExplicitFlushOutcome {
                flushed_local_dirty_state: outcome.had_local_dirty_state,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(open_file.ino);
        result
    }

    /// Flush data for a file handle (per-fd close in POSIX).
    pub async fn flush_fh(&self, fh: u64) -> VfsResult<()> {
        self.flush_fh_with_outcome(fh).await.map(|_| ())
    }

    /// Drain handle-local state, then execute the shared strict metadata barrier.
    pub async fn fsync_fh_with_outcome(&self, fh: u64) -> VfsResult<ExplicitFlushOutcome> {
        let had_local_dirty_state = self.get_open_file(fh).is_some_and(|open_file| {
            self.inode_has_local_dirty_state(open_file.ino)
                || self.state.writer_stack.has_dirty_writers(open_file.ino)
                || self.inode_has_pending_uploads(open_file.ino)
        });
        let outcome = self.flush_fh_with_outcome_inner(fh, true).await?;
        Ok(ExplicitFlushOutcome {
            flushed_local_dirty_state: had_local_dirty_state || outcome.flushed_local_dirty_state,
        })
    }

    /// Synchronize buffered file data and metadata for a file handle.
    pub async fn fsync_fh(&self, fh: u64) -> VfsResult<()> {
        self.fsync_fh_with_outcome(fh).await.map(|_| ())
    }

    /// fsync implementation that reports whether file data and/or dirty attrs were persisted.
    pub(super) async fn fsync_with_outcome(
        &self,
        ino: Ino,
        datasync: bool,
    ) -> VfsResult<FsyncOutcome> {
        let ((), metadata_result) = tokio::join!(
            self.wait_for_deferred_release_inflight(ino),
            self.wait_for_deferred_namespace_metadata(ino)
        );
        metadata_result?;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Fsync)
            .await;
        let result: VfsResult<FsyncOutcome> = async {
            self.enforce_access(ino, AccessLevel::ReadOnly)?;
            if self.ignored_entry_attr(ino).is_some() {
                return Ok(FsyncOutcome::default());
            }

            let outcome = self
                .local_sync_barrier_with_outcome(
                    ino,
                    LocalSyncPolicy::Strict,
                    StickyErrorPolicy::Report,
                    if datasync {
                        DirtyAttrPersistPolicy::Ignore
                    } else {
                        DirtyAttrPersistPolicy::PersistIfPresent
                    },
                )
                .await?;
            Ok(FsyncOutcome {
                flushed_file_data: outcome.flushed_file_data,
                persisted_dirty_attrs: outcome.persisted_dirty_attrs,
            })
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// fsync — flush data (and optionally metadata) to durable storage.
    ///
    /// 1. Flushes pending local dirty state to the dirty-mem / slab pipeline.
    /// 2. If `!datasync`, also commits dirty attrs to redb.
    ///
    /// S3 upload remains async — same as LucidLink behaviour.
    pub async fn fsync(&self, ino: Ino, datasync: bool) -> VfsResult<()> {
        self.fsync_with_outcome(ino, datasync).await.map(|_| ())
    }

    /// Get information about an open file handle.
    pub fn get_open_file(&self, fh: u64) -> Option<OpenFile> {
        self.handle_table.get(fh)
    }

    /// Get the last attributes retained by an open file handle.
    pub fn get_open_file_attr(&self, fh: u64) -> Option<InodeAttr> {
        self.handle_table
            .get(fh)
            .and_then(|open_file| open_file.open_attr())
    }

    /// Get the number of open handles for an inode.
    pub fn open_count(&self, ino: Ino) -> u32 {
        self.handle_table.open_count(ino)
    }

    /// Read file data into `buffer` using the buffered read path.
    pub async fn read_into_fh(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let debug_path = self.debug_pending_target_path(ino);
        if let Some(path) = &debug_path {
            let session = self.state.file_session.debug_snapshot(ino);
            info!(
                ino,
                fh,
                path = %path,
                offset,
                requested_len = buffer.len(),
                session_open_handles = session.as_ref().map(|snapshot| snapshot.open_handles),
                session_queued_ops = session.as_ref().map(|snapshot| snapshot.queued_ops),
                session_active_shared_locks =
                    session.as_ref().map(|snapshot| snapshot.active_shared_locks),
                session_active_writer = session.as_ref().and_then(|snapshot| snapshot.active_writer),
                session_last_transition_op = session
                    .as_ref()
                    .map(|snapshot| snapshot.last_transition_label.0),
                session_last_transition_phase = session
                    .as_ref()
                    .map(|snapshot| snapshot.last_transition_label.1),
                session_last_transition_unix_ms = session
                    .as_ref()
                    .map(|snapshot| snapshot.last_transition_unix_ms),
                "debug target read_into_fh before file-session gate"
            );
        }
        self.wait_for_deferred_release_inflight(ino).await;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        if let Some(path) = &debug_path {
            info!(
                ino,
                fh,
                path = %path,
                offset,
                requested_len = buffer.len(),
                "debug target read_into_fh entered file-session gate"
            );
        }
        let result = self.read_into_impl(Some(fh), ino, offset, buffer).await;
        if let Some(path) = &debug_path {
            info!(
                ino,
                fh,
                path = %path,
                offset,
                result = ?result.as_ref().map(|n| *n),
                "debug target read_into_fh completed read_into_impl"
            );
        }
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Write file data using the buffered write path.
    pub async fn write_fh(&self, fh: u64, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        self.write_fh_with_local_sync_policy(fh, ino, offset, data, LocalSyncPolicy::Opportunistic)
            .await
    }

    /// Write file data using the buffered write path with an explicit local sync policy.
    ///
    /// The `local_sync_policy` controls whether this write must establish strict local
    /// durability ordering with concurrent flush/fsync operations on the same inode.
    pub async fn write_fh_with_local_sync_policy(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        // `session` is wrapped in Option so we can release it early for opportunistic writes
        // before local-disk materialization. Durability-required writes hold the gate through
        // all I/O to maintain ordering guarantees with concurrent flushes on the same inode.
        let mut session = Some(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await,
        );
        let open_file = self.enforce_mutating_handle_access(fh, ino).ok();
        let _ = self.state.read.observe_write_access(
            open_file.as_ref(),
            ino,
            offset,
            data.len(),
            self.access_tracker,
        );
        let result = async {
            self.enforce_write_access_for_handle(fh, ino)?;
            if data.is_empty() {
                return Ok(0);
            }
            if self.ignored_entry_attr(ino).is_some() {
                let size = offset.saturating_add(data.len() as u64);
                let _ = self.update_ignored_entry(ino, |attr| {
                    if size > attr.size {
                        attr.size = size;
                        attr.committed_size = size;
                    }
                    attr.touch_mtime();
                });
                return Ok(data.len() as u32);
            }
            self.note_vfs_write_call(data.len());

            let record_writer_stack_started = Instant::now();
            let written = self
                .record_writer_stack_state_bytes(fh, ino, offset, data)
                .await?;
            self.record_vfs_write_record_writer_stack_state(record_writer_stack_started.elapsed());

            // Release the per-inode write gate before local-disk materialization for
            // opportunistic writes. This lets concurrent writes to the same inode proceed
            // through buffering while this write's materialization is in-flight.
            //
            // Duplicate materialization is safe: the local payload store uses last-writer-wins
            // for overlapping block ranges, and orphaned objects are reclaimed by the
            // background janitor. Durability-required writes keep the gate to serialize
            // flush/fsync callers with any concurrent materialization on the same inode.
            if local_sync_policy.is_opportunistic() {
                session = None;
            }

            if local_sync_policy.requires_write_path_durability() {
                self.wait_for_deferred_materialization_inflight(ino).await;
                let materialize_strict_started = Instant::now();
                // This starts with the target inode, then only drains other dirty
                // inodes when the global dirty budget still requires it. Using it
                // as the strict write-through entry point avoids probing the same
                // inode again in a follow-up limits pass.
                let strict_result = self.materialize_writer_stack_for_limits(ino).await;
                self.record_vfs_write_materialize_strict(materialize_strict_started.elapsed());
                let materialized_bytes = strict_result?;
                if materialized_bytes > 0 {
                    self.maybe_payload_store_sync_data_async(
                        materialized_bytes as u64,
                        local_sync_policy,
                    )
                    .await?;
                }
            }
            let dirty_pressure_requires_materialization = self.total_pending_write_buffer_bytes()
                > self.max_dirty_bytes()
                || self.state.writer_stack.total_dirty_bytes() > self.write_eager_flush_batch_bytes;
            let should_materialize_for_limits =
                local_sync_policy.is_opportunistic() && dirty_pressure_requires_materialization;
            if should_materialize_for_limits {
                let materialize_for_limits_started = Instant::now();
                let limits_result = self.materialize_writer_stack_for_limits(ino).await;
                self.record_vfs_write_materialize_for_limits(
                    materialize_for_limits_started.elapsed(),
                );
                limits_result?;
            }
            if local_sync_policy.requires_write_path_durability() {
                self.flush_local_dirty_state(ino, local_sync_policy).await?;
                self.sync_metadata_barrier().await?;
            }
            Ok(written)
        }
        .await;
        // For opportunistic writes `session` is already None (gate released above).
        // For durability-required writes `session` holds the gate until here.
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Write with deferred materialization for high-throughput streaming writes.
    ///
    /// For `Opportunistic` writes the callback returns as soon as data is buffered
    /// in the `MountedWriterStack`. NVMe materialization and budget enforcement
    /// are spawned as a fire-and-forget background task so the calling thread
    /// (e.g. WinFSP dispatcher) is never blocked on local I/O.
    ///
    /// For `Strict` writes, behaviour is identical to
    /// `write_fh_with_local_sync_policy` — materialization is awaited inline.
    ///
    /// # Safety of fire-and-forget materialization
    ///
    /// * Data is safely held in `MountedWriterStack` memory buffers.
    /// * Any materialization error will surface at the next `Strict` barrier
    ///   (flush / fsync / close) because those paths call
    ///   `materialize_writer_stack_for_inode` synchronously.
    /// * Duplicate materialization is safe: the payload store uses last-writer-wins
    ///   for overlapping ranges, and orphaned objects are reclaimed by the janitor.
    /// * Soft pacing is deliberately kept bounded on this foreground writeback
    ///   path. Opportunistic writes never materialize inline under dirty-budget
    ///   pressure; background materialization catches up while the callback
    ///   returns after copying data into the writer stack.
    pub async fn write_fh_with_deferred_materialize(
        self: &Arc<Self>,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        self.write_fh_payload_with_deferred_materialize(
            fh,
            ino,
            offset,
            VfsWritePayload::Borrowed(data),
            local_sync_policy,
        )
        .await
    }

    /// Write owned file data using the deferred-materialization path.
    ///
    /// This is the owned-buffer variant of [`Self::write_fh_with_deferred_materialize`].
    pub async fn write_fh_owned_with_deferred_materialize(
        self: &Arc<Self>,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: Bytes,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        self.write_fh_payload_with_deferred_materialize(
            fh,
            ino,
            offset,
            VfsWritePayload::Owned(data),
            local_sync_policy,
        )
        .await
    }

    async fn write_fh_payload_with_deferred_materialize(
        self: &Arc<Self>,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: VfsWritePayload<'_>,
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        // ── Hard backpressure safety valve ───────────────────────────────
        // If background materialization falls behind and dirty memory is
        // dangerously high, fall back to the fully-synchronous path so we
        // exert real TCP-style backpressure on the caller and avoid OOM.
        let hard_backpressure_threshold =
            super::hard_backpressure_threshold_bytes(self.max_dirty_bytes());
        if self.total_pending_write_buffer_bytes() > hard_backpressure_threshold {
            self.record_deferred_write_hard_backpressure(ino, hard_backpressure_threshold);
            if local_sync_policy.requires_write_path_durability() {
                // Slow-path durability backpressure may copy owned payloads;
                // keeping memory bounded wins over preserving zero-copy here.
                return self
                    .write_fh_with_local_sync_policy(
                        fh,
                        ino,
                        offset,
                        data.as_slice(),
                        local_sync_policy,
                    )
                    .await;
            }
        }
        if local_sync_policy.is_opportunistic() {
            let pre_drain_started = Instant::now();
            let sustained_delay_ms = self
                .maybe_pre_drain_sustained_write_buffer(ino, offset, data.len(), local_sync_policy)
                .await?;
            self.record_vfs_write_pre_drain(pre_drain_started.elapsed());
            let admission_throttle_started = Instant::now();
            self.maybe_throttle_write_admission(data.len(), sustained_delay_ms)
                .await;
            self.record_vfs_write_admission_throttle(admission_throttle_started.elapsed());
        }
        // ── Acquire per-inode write gate ─────────────────────────────────
        let file_session_wait_started = Instant::now();
        let mut session = Some(
            self.state
                .file_session
                .enter_op(ino, FileSessionOperation::Write)
                .await,
        );
        self.record_vfs_write_file_session_wait(file_session_wait_started.elapsed());
        let open_file = self.enforce_mutating_handle_access(fh, ino).ok();
        let _ = self.state.read.observe_write_access(
            open_file.as_ref(),
            ino,
            offset,
            data.len(),
            self.access_tracker,
        );
        if self.total_pending_write_buffer_bytes() > hard_backpressure_threshold {
            self.record_deferred_write_hard_backpressure(ino, hard_backpressure_threshold);
            if local_sync_policy.requires_write_path_durability() {
                drop(session);
                self.state.file_session.gc_if_idle(ino);
                // Slow-path durability backpressure may copy owned payloads;
                // keeping memory bounded wins over preserving zero-copy here.
                return self
                    .write_fh_with_local_sync_policy(
                        fh,
                        ino,
                        offset,
                        data.as_slice(),
                        local_sync_policy,
                    )
                    .await;
            }
        }
        // ── Buffer phase (fast: ~0.1 ms) ────────────────────────────────
        let result = async {
            self.enforce_write_access_for_handle(fh, ino)?;
            if data.is_empty() {
                return Ok(0);
            }
            if self.ignored_entry_attr(ino).is_some() {
                let size = offset.saturating_add(data.len() as u64);
                let _ = self.update_ignored_entry(ino, |attr| {
                    if size > attr.size {
                        attr.size = size;
                        attr.committed_size = size;
                    }
                    attr.touch_mtime();
                });
                return Ok(data.len() as u32);
            }
            self.note_vfs_write_call(data.len());

            let record_writer_stack_started = Instant::now();
            let written = match data {
                VfsWritePayload::Borrowed(data) => {
                    self.record_writer_stack_state_bytes(fh, ino, offset, data)
                        .await
                }
                VfsWritePayload::Owned(data) => {
                    self.record_writer_stack_state_owned_bytes(fh, ino, offset, data)
                        .await
                }
            };
            self.record_vfs_write_record_writer_stack_state(record_writer_stack_started.elapsed());
            written
        }
        .await;

        // Release the gate immediately: data is safe in MountedWriterStack.
        if local_sync_policy.is_opportunistic() {
            session = None;
        }
        if let Ok(written) = &result {
            self.record_write_admission_accepted_bytes(*written as usize);
        }

        // ── Materialization phase ────────────────────────────────────────
        match (&result, local_sync_policy) {
            (Ok(written), LocalSyncPolicy::Opportunistic) if *written > 0 => {
                let spawn_started = Instant::now();
                self.spawn_deferred_materialization_if_needed(ino).await;
                self.record_vfs_write_spawn_deferred_materialization(spawn_started.elapsed());
            }
            (Ok(_), policy) if policy.requires_write_path_durability() => {
                // Durability-required: await writer-stack materialization inline.
                self.wait_for_deferred_materialization_inflight(ino).await;
                let materialize_strict_started = Instant::now();
                let materialized_bytes = match self.materialize_writer_stack_for_limits(ino).await {
                    Ok(materialized_bytes) => materialized_bytes,
                    Err(e) => {
                        drop(session);
                        self.state.file_session.gc_if_idle(ino);
                        return Err(e);
                    }
                };
                self.record_vfs_write_materialize_strict(materialize_strict_started.elapsed());

                if let Err(e) = self.flush_local_dirty_state(ino, local_sync_policy).await {
                    drop(session);
                    self.state.file_session.gc_if_idle(ino);
                    return Err(e);
                }
                if let Err(e) = self
                    .maybe_payload_store_sync_data_async(
                        materialized_bytes as u64,
                        local_sync_policy,
                    )
                    .await
                {
                    drop(session);
                    self.state.file_session.gc_if_idle(ino);
                    return Err(e);
                }
                if let Err(e) = self.sync_metadata_barrier().await {
                    drop(session);
                    self.state.file_session.gc_if_idle(ino);
                    return Err(e);
                }
            }
            _ => {
                // Write failed or unknown policy — nothing to materialize.
            }
        }

        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Record a write into the per-inode writer stack and update relevant local metadata.
    ///
    /// This is the shared entry point used by write paths to stage data for eventual
    /// materialization into local dirty state and upload.
    pub(super) async fn record_writer_stack_state_bytes(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: &[u8],
    ) -> VfsResult<u32> {
        self.record_writer_stack_state_payload(fh, ino, offset, VfsWritePayload::Borrowed(data))
            .await
    }

    pub(super) async fn record_writer_stack_state_owned_bytes(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: Bytes,
    ) -> VfsResult<u32> {
        self.record_writer_stack_state_payload(fh, ino, offset, VfsWritePayload::Owned(data))
            .await
    }

    async fn record_writer_stack_state_payload(
        &self,
        fh: u64,
        ino: Ino,
        offset: u64,
        data: VfsWritePayload<'_>,
    ) -> VfsResult<u32> {
        let dirty_attr = self.state.namespace.dirty_attrs.lock().get(&ino).cloned();
        let open_attr = self
            .handle_table
            .get(fh)
            .filter(|open_file| open_file.ino == ino)
            .and_then(|open_file| open_file.open_attr());
        let mut base_attr = match (dirty_attr, open_attr) {
            (Some(dirty), Some(open)) if open.content_epoch > dirty.content_epoch => {
                self.note_vfs_write_open_attr_hit();
                open
            }
            (Some(dirty), _) => {
                self.note_vfs_write_dirty_attr_hit();
                dirty
            }
            (None, Some(open)) => {
                self.note_vfs_write_open_attr_hit();
                open
            }
            (None, None) => {
                let getattr_started = Instant::now();
                let getattr_result = self.getattr_async(ino).await;
                self.record_vfs_write_getattr_async(getattr_started.elapsed());
                let mut attr = match getattr_result {
                    Ok(attr) => attr,
                    Err(error)
                        if Self::is_inode_not_found_error(&error, ino)
                            && self
                                .handle_table
                                .get(fh)
                                .is_some_and(|open_file| open_file.active_open_overlay) =>
                    {
                        self.note_vfs_write_active_open_overlay_fallback();
                        let open_file = self
                            .handle_table
                            .get(fh)
                            .ok_or_else(|| VfsError::Meta(MetaError::InodeNotFound(ino)))?;
                        self.handle_table.mark_orphan(ino);
                        open_file
                            .open_attr()
                            .ok_or_else(|| VfsError::Meta(MetaError::InodeNotFound(ino)))?
                    }
                    Err(error) => return Err(error),
                };
                self.refresh_inode_incarnation_state_from_attr(ino, &attr);
                self.begin_pending_incarnation(ino, &mut attr);
                attr.upload_state = if attr.committed_size > 0 || attr.committed_content_epoch > 0 {
                    MetaUploadState::Updating
                } else {
                    MetaUploadState::Uploading
                };
                attr
            }
        };
        let starts_writer_only_empty_draft =
            base_attr.is_unpublished_empty_draft() && !data.is_empty();
        let writer_only_restore_attr = starts_writer_only_empty_draft.then(|| base_attr.clone());
        let starts_payload_after_empty_local_incarnation = base_attr.is_unpublished_empty_draft()
            || (base_attr.size == 0
                && base_attr.committed_size == 0
                && base_attr.content_epoch > base_attr.committed_content_epoch
                && base_attr.upload_state != MetaUploadState::Ready);
        if starts_payload_after_empty_local_incarnation && !data.is_empty() {
            self.begin_size_change_incarnation(ino, &mut base_attr);
            base_attr.upload_state = MetaUploadState::Uploading;
        } else if base_attr.upload_state == MetaUploadState::Ready && !data.is_empty() {
            self.begin_pending_incarnation(ino, &mut base_attr);
            base_attr.upload_state =
                if base_attr.committed_size > 0 || base_attr.committed_content_epoch > 0 {
                    MetaUploadState::Updating
                } else {
                    MetaUploadState::Uploading
                };
        }

        if starts_writer_only_empty_draft {
            self.sync_meta().set_attr(&base_attr)?;
            let meta = self.meta.clone();
            run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.sync_local_metadata()
            })
            .await?;
        }

        let spec = super::writer_stack::WriterWriteSpec {
            fh,
            ino,
            current_size: base_attr.size,
            content_epoch: base_attr.content_epoch,
            first_object_id: 0,
            offset,
        };
        let accepted = match data {
            VfsWritePayload::Borrowed(data) => self.state.writer_stack.write_bytes(spec, data),
            VfsWritePayload::Owned(data) => self.state.writer_stack.write_owned_bytes(spec, data),
        };
        let accepted = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                if let Some(restore_attr) = writer_only_restore_attr {
                    match self.sync_meta().set_attr(&restore_attr) {
                        Err(restore_error) => warn!(
                            ino,
                            error = %restore_error,
                            "failed to restore empty inode after writer stack rejection"
                        ),
                        Ok(()) => {
                            let meta = self.meta.clone();
                            if let Err(restore_error) =
                                run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                                    engine.sync_local_metadata()
                                })
                                .await
                            {
                                warn!(
                                    ino,
                                    error = %restore_error,
                                    "failed to persist restored empty inode after writer stack rejection"
                                );
                            }
                        }
                    }
                }
                return Err(VfsError::Other(format!(
                    "writer stack write failed: {error}"
                )));
            }
        };
        {
            let mut dirty = self.state.namespace.dirty_attrs.lock();
            let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
            merge_write_dirty_attr(attr, &base_attr);
            attr.kind = FileType::RegularFile;
            if accepted.visible_len > attr.size {
                if attr.upload_state == MetaUploadState::Ready || attr.size < attr.committed_size {
                    self.begin_size_change_incarnation(ino, attr);
                    attr.upload_state =
                        if attr.committed_size > 0 || attr.committed_content_epoch > 0 {
                            MetaUploadState::Updating
                        } else {
                            MetaUploadState::Uploading
                        };
                }
                attr.size = accepted.visible_len;
            }
            attr.touch_mtime();
            self.update_pending_incarnation_size(ino, attr.size);
        }
        self.invalidate_attr_read_cache([ino]);
        Ok(accepted.accepted_bytes)
    }

    async fn reserve_writer_stack_object_ids_by_count(
        &self,
        ino: Ino,
        object_count: u32,
    ) -> VfsResult<u64> {
        if object_count == 0 {
            return Ok(0);
        }
        if let Some(first_object_id) =
            self.take_prefetched_writer_stack_object_ids(ino, object_count)
        {
            return Ok(first_object_id);
        }
        let ids = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_object_ids(object_count)
        })
        .await?;
        ids.first()
            .copied()
            .ok_or_else(|| VfsError::Other("writer stack object id reservation was empty".into()))
    }

    fn take_prefetched_writer_stack_object_ids(&self, ino: Ino, object_count: u32) -> Option<u64> {
        let object_count = u64::from(object_count);
        let mut ranges = self.state.writer_stack_prefetched_object_ids.lock();
        let range_index = ranges
            .iter()
            .position(|range| range.ino == ino && range.len() >= object_count)?;
        let range = ranges.get_mut(range_index)?;
        let first_object_id = range.start;
        range.start = range.start.saturating_add(object_count);
        if range.is_empty() {
            ranges.remove(range_index);
        }
        Some(first_object_id)
    }

    pub(super) async fn prefetch_writer_stack_object_ids_for_completed_blocks(
        &self,
        ino: Ino,
    ) -> VfsResult<()> {
        let object_count = self
            .state
            .writer_stack
            .completed_block_count_for_inode(ino)
            .map_err(|err| {
                VfsError::Other(format!("writer stack completed block count failed: {err}"))
            })?;
        let object_count = self.completed_block_materialization_batch_blocks(object_count);
        if object_count == 0 {
            return Ok(());
        }
        if self
            .state
            .writer_stack_prefetched_object_ids
            .lock()
            .iter()
            .any(|range| range.ino == ino && range.len() >= u64::from(object_count))
        {
            return Ok(());
        }
        let ids = run_meta(self.meta.clone(), move |engine: Arc<dyn VfsMeta>| {
            engine.next_object_ids(object_count)
        })
        .await?;
        let len = u64::try_from(ids.len())
            .map_err(|_| VfsError::Other("writer stack object id prefetch overflow".into()))?;
        let start = ids
            .first()
            .copied()
            .ok_or_else(|| VfsError::Other("writer stack object id prefetch was empty".into()))?;
        let range = PrefetchedWriterStackObjectIds::new(ino, start, len)
            .ok_or_else(|| VfsError::Other("writer stack object id prefetch overflow".into()))?;
        let mut ranges = self.state.writer_stack_prefetched_object_ids.lock();
        if ranges.len() >= super::WRITER_STACK_PREFETCHED_OBJECT_ID_RANGE_LIMIT {
            ranges.pop_front();
        }
        ranges.push_back(range);
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn writer_stack_object_upper_bound(
        &self,
        offset: u64,
        len: usize,
    ) -> VfsResult<u32> {
        let mut remaining = len;
        let mut cursor = offset;
        let mut object_count = 0usize;
        while remaining > 0 {
            let chunk_id = studiofs_meta::schema::chunk_id_from_offset(
                cursor,
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            let chunk_start = studiofs_meta::schema::chunk_start_offset(
                chunk_id,
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            );
            let chunk_end =
                chunk_start.saturating_add(u64::from(studiofs_meta::schema::DEFAULT_CHUNK_SIZE));
            let take = (chunk_end - cursor).min(remaining as u64) as usize;
            object_count = object_count
                .checked_add(take.div_ceil(self.block_size as usize))
                .ok_or_else(|| {
                    VfsError::Other("writer stack object reservation overflow".into())
                })?;
            cursor = cursor.saturating_add(take as u64);
            remaining -= take;
        }
        u32::try_from(object_count)
            .map_err(|_| VfsError::Other("writer stack object reservation exceeds u32::MAX".into()))
    }

    /// Materialize pending writer-stack data for `ino` into persisted local dirty state.
    ///
    /// Returns the number of bytes materialized in this pass.
    pub(super) async fn materialize_writer_stack_for_inode(&self, ino: Ino) -> VfsResult<usize> {
        let object_count = self
            .state
            .writer_stack
            .prepare_finish_inode(ino)
            .map_err(|err| VfsError::Other(format!("writer stack finish failed: {err}")))?;
        if object_count == 0 {
            return Ok(0);
        }
        let first_object_id = self
            .reserve_writer_stack_object_ids_by_count(ino, object_count)
            .await?;
        let finished = self
            .state
            .writer_stack
            .finish_prepared_inode(ino, first_object_id)
            .map_err(|err| VfsError::Other(format!("writer stack finish failed: {err}")))?;
        if finished.is_empty() {
            return Ok(0);
        }
        let installed_object_ids = finished
            .iter()
            .map(|slice| slice.object_id)
            .collect::<Vec<_>>();
        let materialized_bytes = self.install_finished_writer_stack_slices(ino, finished);
        self.state
            .writer_stack
            .clear_materializing_object_ids(ino, &installed_object_ids);
        Ok(materialized_bytes)
    }

    /// Materialize writer-stack data for `ino` when it has been idle for at least `min_idle`.
    ///
    /// Returns the number of bytes materialized in this pass.
    pub(super) async fn materialize_writer_stack_idle_tail_for_inode(
        &self,
        ino: Ino,
        min_idle: Duration,
    ) -> VfsResult<usize> {
        let object_count = self
            .state
            .writer_stack
            .prepare_finish_inode_if_idle(ino, min_idle)
            .map_err(|err| VfsError::Other(format!("writer stack idle finish failed: {err}")))?;
        if object_count == 0 {
            return Ok(0);
        }
        let first_object_id = self
            .reserve_writer_stack_object_ids_by_count(ino, object_count)
            .await?;
        let finished = self
            .state
            .writer_stack
            .finish_prepared_inode(ino, first_object_id)
            .map_err(|err| VfsError::Other(format!("writer stack idle finish failed: {err}")))?;
        if finished.is_empty() {
            return Ok(0);
        }
        let installed_object_ids = finished
            .iter()
            .map(|slice| slice.object_id)
            .collect::<Vec<_>>();
        let materialized_bytes = self.install_finished_writer_stack_slices(ino, finished);
        self.state
            .writer_stack
            .clear_materializing_object_ids(ino, &installed_object_ids);
        Ok(materialized_bytes)
    }

    /// Materialize writer-stack completed blocks for `ino` into persisted local dirty state.
    ///
    /// Returns the number of bytes materialized in this pass.
    pub(super) async fn materialize_writer_stack_completed_blocks_for_inode(
        &self,
        ino: Ino,
    ) -> VfsResult<usize> {
        let object_count = self
            .state
            .writer_stack
            .completed_block_count_for_inode(ino)
            .map_err(|err| {
                VfsError::Other(format!("writer stack completed block count failed: {err}"))
            })?;
        if object_count == 0 {
            return Ok(0);
        }
        let object_count = self.completed_block_materialization_batch_blocks(object_count);
        let first_object_id = self
            .reserve_writer_stack_object_ids_by_count(ino, object_count)
            .await?;
        let finished = self
            .state
            .writer_stack
            .flush_completed_blocks_for_inode(ino, first_object_id, object_count as usize)
            .map_err(|err| {
                VfsError::Other(format!("writer stack completed block flush failed: {err}"))
            })?;
        if finished.is_empty() {
            return Ok(0);
        }
        let installed_object_ids = finished
            .iter()
            .map(|slice| slice.object_id)
            .collect::<Vec<_>>();
        let materialized_bytes = self.install_finished_writer_stack_slices(ino, finished);
        self.state
            .writer_stack
            .clear_materializing_object_ids(ino, &installed_object_ids);
        Ok(materialized_bytes)
    }

    fn completed_block_materialization_batch_blocks(&self, available_blocks: u32) -> u32 {
        if available_blocks == 0 {
            return 0;
        }
        let block_size = self.block_size.max(1) as usize;
        let target_bytes = self.write_eager_flush_batch_bytes.max(block_size);
        let max_blocks = target_bytes.div_ceil(block_size).max(1);
        let max_blocks = u32::try_from(max_blocks).unwrap_or(u32::MAX);
        available_blocks.min(max_blocks)
    }

    fn install_finished_writer_stack_slices(
        &self,
        ino: Ino,
        finished: Vec<super::writer_stack::FinishedWriterSlice>,
    ) -> usize {
        let materialized_bytes = finished.iter().map(|slice| slice.len as usize).sum();
        for slice in finished {
            let file_offset = studiofs_meta::schema::chunk_start_offset(
                slice.chunk_id,
                studiofs_meta::schema::DEFAULT_CHUNK_SIZE,
            )
            .saturating_add(u64::from(slice.chunk_offset));
            self.install_persisted_pending_local_dirty_slice(
                ino,
                super::local_dirty::PersistedPendingLocalDirtySliceSpec {
                    block_index: block_index_from_offset(file_offset, self.block_size),
                    file_offset,
                    len: slice.len as usize,
                    seq: slice.seq,
                    object_id: slice.object_id,
                    initial_object_offset: slice.object_offset,
                },
            );
        }
        materialized_bytes
    }

    async fn materialize_writer_stack_for_limits(&self, preferred_ino: Ino) -> VfsResult<usize> {
        let mut materialized_bytes = self
            .materialize_writer_stack_for_inode(preferred_ino)
            .await?;
        let dirty_inodes = self.state.writer_stack.snapshot_dirty_inodes_by_bytes();
        for ino in dirty_inodes {
            if self.total_pending_write_buffer_bytes() <= self.max_dirty_bytes() {
                break;
            }
            if ino == preferred_ino {
                continue;
            }
            let materialized = self.materialize_writer_stack_for_inode(ino).await?;
            if materialized == 0 {
                continue;
            }
            materialized_bytes = materialized_bytes.saturating_add(materialized);
        }
        Ok(materialized_bytes)
    }

    async fn materialize_writer_stack_completed_blocks_for_limits(
        &self,
        preferred_ino: Ino,
    ) -> VfsResult<usize> {
        let mut materialized_bytes = self
            .materialize_writer_stack_completed_blocks_for_inode(preferred_ino)
            .await?;
        let dirty_inodes = self.state.writer_stack.snapshot_dirty_inodes_by_bytes();
        for ino in dirty_inodes {
            if self.state.writer_stack.total_dirty_bytes() <= self.max_dirty_bytes() {
                break;
            }
            if ino == preferred_ino {
                continue;
            }
            let materialized = self
                .materialize_writer_stack_completed_blocks_for_inode(ino)
                .await?;
            if materialized == 0 {
                continue;
            }
            materialized_bytes = materialized_bytes.saturating_add(materialized);
        }
        Ok(materialized_bytes)
    }
}
