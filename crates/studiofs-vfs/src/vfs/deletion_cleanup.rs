use super::{DeletedInodeCleanup, Ino, MetaError, StudioVfs, VfsMeta, VfsResult};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::runtime::{run_local_cache_io_blocking, run_meta};

/// Inodes whose session gates one reachability recheck and metadata sync may cover.
const RETIRED_PAYLOAD_INODES_PER_PASS: usize = 64;

pub(super) struct RegularFileGraphUsage {
    pub(super) object_ids: Vec<u64>,
    pub(super) total_bytes: u64,
}

impl StudioVfs {
    pub(super) async fn reclaim_retired_local_payloads(&self) -> VfsResult<()> {
        let cancel = &self.state.shutdown.background_shutdown_token;
        self.reclaim_retired_payload_sweep(self.cached_store.take_retired_upload_sweep(), &|| {
            cancel.is_cancelled()
        })
        .await
    }

    /// Final sweep over every candidate; what the budget cannot cover falls to startup cleanup.
    pub(super) async fn reclaim_all_retired_local_payloads(
        &self,
        budget: std::time::Duration,
    ) -> VfsResult<()> {
        let deadline = std::time::Instant::now() + budget;
        self.reclaim_retired_payload_sweep(self.cached_store.retired_uploads(), &|| {
            std::time::Instant::now() >= deadline
        })
        .await
    }

    async fn reclaim_retired_payload_sweep(
        &self,
        sweep: HashMap<u64, Vec<u64>>,
        should_stop: &(impl Fn() -> bool + Sync),
    ) -> VfsResult<()> {
        let mut unfinished: HashSet<u64> = sweep.keys().copied().collect();
        let result = self
            .reclaim_retired_payload_candidates(sweep, should_stop, &mut unfinished)
            .await;
        self.cached_store.requeue_retired_upload_sweep(unfinished);
        result
    }

    async fn reclaim_retired_payload_candidates(
        &self,
        sweep: HashMap<u64, Vec<u64>>,
        should_stop: &(impl Fn() -> bool + Sync),
        unfinished: &mut HashSet<u64>,
    ) -> VfsResult<()> {
        // shortcut: a graph change without a later terminal upload (a foreign apply, a sweep
        // racing a volatile overlay batch) waits for the final sweep; add a graph generation
        // if such uploaded objects matter before LRU eviction or unmount.
        let sweep: Vec<_> = sweep
            .into_iter()
            .filter(|(ino, _)| self.handle_table.open_count(*ino) == 0)
            .collect();
        let cached_store = self.cached_store.clone();
        let backend = self.local_payload_backend.clone();
        let (present, _) = run_local_cache_io_blocking(move || {
            Ok(sweep
                .into_iter()
                .map(|(ino, candidates)| {
                    let (absent, present): (Vec<_>, Vec<_>) =
                        candidates.into_iter().partition(|id| {
                            !backend.contains(*id) && !cached_store.has_staged_upload(*id)
                        });
                    cached_store.forget_retired_uploads(ino, &absent);
                    (ino, present)
                })
                .collect::<Vec<_>>())
        })
        .await?;
        let (empty, present): (Vec<_>, Vec<_>) = present
            .into_iter()
            .partition(|(_, candidates)| candidates.is_empty());
        for (ino, _) in empty {
            unfinished.remove(&ino);
        }
        if present.is_empty() {
            return Ok(());
        }

        // One shared scan keeps inodes whose candidates are all current away from their gates.
        let observed = self
            .meta
            .run_maintenance(|meta| meta.get_local_referenced_object_ids())
            .await?;
        let mut displaced = Vec::new();
        for (ino, candidates) in present {
            let candidates: Vec<_> = candidates
                .into_iter()
                .filter(|id| !observed.contains(id))
                .collect();
            if candidates.is_empty() {
                unfinished.remove(&ino);
            } else {
                displaced.push((ino, candidates));
            }
        }
        drop(observed);

        let mut first_error = None;
        for batch in displaced.chunks(RETIRED_PAYLOAD_INODES_PER_PASS) {
            if should_stop() {
                break;
            }
            if let Err(error) = self.reclaim_retired_payload_batch(batch, unfinished).await {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Runs one reachability recheck and metadata sync for a bounded set of inodes.
    async fn reclaim_retired_payload_batch(
        &self,
        batch: &[(u64, Vec<u64>)],
        unfinished: &mut HashSet<u64>,
    ) -> VfsResult<()> {
        // Skip gate and scan work while read-ahead is busy; the barrier below is the safety check.
        if self.state.read.prefetch_payload_gate.try_write().is_err() {
            return Ok(());
        }
        let mut admitted = Vec::with_capacity(batch.len());
        for (ino, candidates) in batch {
            // Try-acquire: a queued writer would make later reads wait behind a slow reader.
            let cleanup = self.file_session_cleanup(*ino);
            let Some(permit) = self.state.file_session.try_enter_upload(*ino) else {
                continue;
            };
            let session = cleanup.with_permit(permit);
            if self.handle_table.open_count(*ino) == 0 {
                admitted.push((*ino, candidates, session));
            }
        }
        if admitted.is_empty() {
            return Ok(());
        }

        // Recheck under the inode gates: rollback may have restored a candidate since the scan.
        self.wait_for_volatile_overlay_batches().await?;
        #[cfg(test)]
        self.state
            .read
            .run_before_retired_payload_recheck_for_test();
        let referenced = self
            .meta
            .run_maintenance(|meta| meta.get_local_referenced_object_ids())
            .await?;
        // Barrier: a prefetch that resolved before the recheck may still read a displaced object.
        // Later ones resolve the rechecked graph, which the held inode gates keep unchanged.
        if self.state.read.prefetch_payload_gate.try_write().is_err() {
            return Ok(());
        }
        let mut removals = Vec::new();
        for (ino, candidates, session) in admitted {
            let owned: HashSet<_> = self
                .pending_uploads_snapshot_for_inode(ino)
                .into_iter()
                .map(|op| op.object_id)
                .chain(
                    self.live_extent_records_for_inode(ino)
                        .into_iter()
                        .map(|(_, id, _)| id),
                )
                .collect();
            let (busy, objects): (Vec<_>, Vec<_>) = candidates
                .iter()
                .copied()
                .filter(|id| !referenced.contains(id))
                .partition(|id| owned.contains(id) || self.cached_store.is_pinned(*id));
            if !objects.is_empty() {
                removals.push((ino, objects, busy.is_empty(), session));
            } else if busy.is_empty() {
                unfinished.remove(&ino);
            }
        }
        if removals.is_empty() {
            return Ok(());
        }

        // A crash must not restore an older redb graph after its dirty bytes are gone.
        self.meta
            .run_maintenance(|meta| meta.sync_local_metadata())
            .await?;
        let cached_store = self.cached_store.clone();
        let backend = self.local_payload_backend.clone();
        let state = self.state.clone();
        let ((finished, first_error), _) = run_local_cache_io_blocking(move || {
            #[cfg(test)]
            state.read.run_before_retired_payload_removal_for_test();
            let mut finished = Vec::new();
            let mut first_error = None;
            for (ino, objects, finishes_inode, _session) in removals {
                let removed = objects
                    .chunks(studiofs_cache::PAYLOAD_REMOVAL_CHUNK_OBJECTS)
                    .try_for_each(|batch| -> VfsResult<()> {
                        cached_store.remove_staged_upload_batch(batch)?;
                        backend.remove_batch(batch)?;
                        cached_store.remove_hot_upload_batch(batch);
                        {
                            let mut evictions = state.upload.pending_evictions.lock();
                            for id in batch {
                                evictions.remove(id);
                            }
                        }
                        cached_store.forget_retired_uploads(ino, batch);
                        Ok(())
                    });
                match removed {
                    Ok(()) => {
                        tracing::info!(
                            ino,
                            removed_objects = objects.len(),
                            "reclaimed retired local payloads"
                        );
                        if finishes_inode {
                            finished.push(ino);
                        }
                    }
                    // One unremovable object must not block the other inodes.
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            Ok((finished, first_error))
        })
        .await?;
        for ino in finished {
            unfinished.remove(&ino);
        }
        first_error.map_or(Ok(()), Err)
    }

    pub(super) async fn background_local_payload_reclamation(self: Arc<Self>) {
        let cancel = self.state.shutdown.background_shutdown_token.clone();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = interval.tick() => {}
            }
            if let Err(error) = self.reclaim_retired_local_payloads().await {
                warn!(%error, "local payload reclamation deferred; retaining candidates for retry");
            }
        }
    }

    pub(super) fn regular_file_graph_usage(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
    ) -> VfsResult<RegularFileGraphUsage> {
        let usage = match meta.get_regular_file_graph_object_usage(ino) {
            Ok(usage) => usage,
            Err(MetaError::InodeNotFound(_)) => Vec::new(),
            Err(err) => return Err(err.into()),
        };
        Ok(RegularFileGraphUsage {
            total_bytes: usage
                .iter()
                .fold(0_u64, |acc, (_, bytes)| acc.saturating_add(*bytes)),
            object_ids: usage.into_iter().map(|(object_id, _)| object_id).collect(),
        })
    }

    pub(super) fn queue_pending_deletions_for_object_ids(
        &self,
        meta: &Arc<dyn VfsMeta>,
        removed_ids: &[u64],
        reason: &str,
    ) -> VfsResult<()> {
        if removed_ids.is_empty() {
            return Ok(());
        }
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let entries: Vec<_> = removed_ids
            .iter()
            .map(|&object_id| (object_id, timestamp))
            .collect();
        meta.add_pending_deletions(&entries)?;
        for &object_id in removed_ids {
            debug!("marking object {} for deletion ({})", object_id, reason);
        }
        Ok(())
    }

    pub(super) fn collect_deleted_inode_cleanup(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
        used_space_bytes: u64,
        gc_reason: &str,
        fallback_object_ids: Vec<u64>,
    ) -> VfsResult<DeletedInodeCleanup> {
        self.collect_deleted_inode_cleanup_impl(
            meta,
            ino,
            used_space_bytes,
            gc_reason,
            fallback_object_ids,
            false,
        )
    }

    pub(super) fn collect_deleted_inode_cleanup_after_unlink(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
        used_space_bytes: u64,
        gc_reason: &str,
        fallback_object_ids: Vec<u64>,
    ) -> VfsResult<DeletedInodeCleanup> {
        self.collect_deleted_inode_cleanup_impl(
            meta,
            ino,
            used_space_bytes,
            gc_reason,
            fallback_object_ids,
            true,
        )
    }

    fn collect_deleted_inode_cleanup_impl(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
        used_space_bytes: u64,
        gc_reason: &str,
        fallback_object_ids: Vec<u64>,
        metadata_already_removed: bool,
    ) -> VfsResult<DeletedInodeCleanup> {
        self.mark_inode_deleted_for_uploads(ino);

        let graph_usage = self.regular_file_graph_usage(meta, ino)?;
        let removed_ids = if metadata_already_removed {
            Vec::new()
        } else {
            meta.remove_regular_file_graph(ino, None)?
        };
        let cleanup_ids = if !removed_ids.is_empty() {
            removed_ids
        } else if !graph_usage.object_ids.is_empty() {
            graph_usage.object_ids
        } else {
            fallback_object_ids
        };
        if !cleanup_ids.is_empty() {
            self.queue_pending_deletions_for_object_ids(meta, &cleanup_ids, gc_reason)?;
        }

        let freed_bytes = if graph_usage.total_bytes > 0 {
            graph_usage.total_bytes
        } else {
            used_space_bytes
        };

        if freed_bytes > 0 {
            meta.decr_counter(studiofs_meta::types::counters::USED_SPACE, freed_bytes)?;
        }

        Ok(DeletedInodeCleanup {
            removed_ids: cleanup_ids,
            freed_bytes,
        })
    }

    pub(super) async fn collect_orphan_deleted_inode_cleanup(
        &self,
        ino: Ino,
    ) -> VfsResult<DeletedInodeCleanup> {
        self.mark_inode_deleted_for_uploads(ino);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = self.meta.clone();
        run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            let graph_usage = match engine.get_regular_file_graph_object_usage(ino) {
                Ok(usage) => usage,
                Err(MetaError::InodeNotFound(_)) => Vec::new(),
                Err(err) => return Err(err),
            };
            let freed_bytes = graph_usage
                .iter()
                .fold(0_u64, |acc, (_, bytes)| acc.saturating_add(*bytes));

            let removed_ids = match engine.remove_regular_file_graph(ino, None) {
                Ok(ids) => ids,
                Err(MetaError::InodeNotFound(_)) => Vec::new(),
                Err(err) => return Err(err),
            };
            let entries: Vec<_> = removed_ids
                .iter()
                .map(|&object_id| (object_id, timestamp))
                .collect();
            engine.add_pending_deletions(&entries)?;

            if freed_bytes > 0 {
                engine.decr_counter(studiofs_meta::types::counters::USED_SPACE, freed_bytes)?;
            }

            match engine.remove_attr(ino) {
                Ok(()) | Err(MetaError::InodeNotFound(_)) => {}
                Err(err) => return Err(err),
            }
            let _ = engine.remove_deferred_delete(ino);

            Ok(DeletedInodeCleanup {
                removed_ids,
                freed_bytes,
            })
        })
        .await
    }

    pub(super) async fn finalize_deleted_inode_runtime_cleanup(
        &self,
        ino: Ino,
        removed_ids: Vec<u64>,
    ) -> usize {
        let removed_ids_len = removed_ids.len();

        // Deleted inodes should never retain staged writes, even if no committed
        // payload slice refs existed yet. Preserve the old immediate-unlink contract by
        // also clearing live extent identities and read-side runtime state even
        // when no object IDs remain to evict.
        self.cancel_pending_uploads_for_inode(ino);
        self.state.writer_stack.clear_inode(ino);
        self.clear_pending_payloads_for_inode(ino);
        self.remove_live_extents_for_inode(ino);
        self.readahead.evict(ino);
        self.clear_access_tracking(ino);

        if removed_ids_len > 0 {
            self.evict_inode_from_caches(ino, &removed_ids).await;
        }

        let deferred_local_payloads = self.take_deferred_deleted_local_payloads(ino);
        let deferred_local_payloads_len = deferred_local_payloads.len();
        if deferred_local_payloads_len > 0 {
            self.evict_inode_from_caches(ino, &deferred_local_payloads)
                .await;
        }

        self.clear_inode_incarnation(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.invalidate_attr_read_cache([ino]);
        self.maybe_clear_deleted_inode_marker(ino);
        deferred_local_payloads_len
    }

    pub(super) fn finalize_deleted_inode_runtime_cleanup_deferred_cache_eviction(
        &self,
        ino: Ino,
        removed_ids: Vec<u64>,
    ) -> usize {
        let removed_ids_len = removed_ids.len();
        self.cancel_pending_uploads_for_inode(ino);
        self.state.writer_stack.clear_inode(ino);
        self.clear_pending_payloads_for_inode(ino);
        self.remove_live_extents_for_inode(ino);
        self.readahead.evict(ino);
        self.clear_access_tracking(ino);

        if removed_ids_len > 0 {
            self.schedule_deleted_inode_cache_eviction(ino, removed_ids);
        }

        let deferred_local_payloads = self.take_deferred_deleted_local_payloads(ino);
        let deferred_local_payloads_len = deferred_local_payloads.len();
        if deferred_local_payloads_len > 0 {
            self.schedule_deleted_inode_cache_eviction(ino, deferred_local_payloads);
        }

        self.clear_inode_incarnation(ino);
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.invalidate_attr_read_cache([ino]);
        self.maybe_clear_deleted_inode_marker(ino);
        deferred_local_payloads_len
    }

    fn schedule_deleted_inode_cache_eviction(&self, ino: Ino, object_ids: Vec<u64>) {
        if object_ids.is_empty() {
            return;
        }
        let cached_store = self.cached_store.clone();
        let local_payload_backend = self.local_payload_backend.clone();
        let state = self.state.clone();
        let cleanup = move || {
            #[cfg(test)]
            state.upload.pause_deleted_inode_eviction_for_test();
            let mut forced_pinned = 0;
            for &object_id in &object_ids {
                if cached_store.is_pinned(object_id) {
                    forced_pinned += 1;
                    cached_store.unpin(object_id);
                }
            }
            {
                let mut pending_evictions = state.upload.pending_evictions.lock();
                for object_id in &object_ids {
                    pending_evictions.remove(object_id);
                }
            }
            cached_store.remove_hot_upload_batch(&object_ids);
            if let Err(error) = cached_store.remove_staged_upload_batch(&object_ids) {
                warn!(
                    ino,
                    object_count = object_ids.len(),
                    error = %error,
                    "deferred deleted inode upload-stage cleanup failed"
                );
            }
            if let Err(error) = local_payload_backend.remove_batch(&object_ids) {
                warn!(
                    ino,
                    object_count = object_ids.len(),
                    error = %error,
                    "deferred deleted inode local payload cleanup failed"
                );
            }
            debug!(
                "deferred deleted inode cache eviction: ino={} evicted {} blocks, forced_pinned={}",
                ino,
                object_ids.len(),
                forced_pinned
            );
        };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            self.spawn_background_cleanup_task_on(&handle, async move {
                if let Err(error) = run_local_cache_io_blocking(move || {
                    cleanup();
                    Ok(())
                })
                .await
                {
                    warn!(
                        ino,
                        error = %error,
                        "deferred deleted inode cache eviction failed"
                    );
                }
            });
        } else {
            if let Err(error) = self.spawn_background_cleanup_thread(
                "studiofs-deleted-inode-cache-evict".to_string(),
                cleanup,
            ) {
                warn!(ino, error = %error, "failed to start deleted inode cache cleanup thread");
            }
        }
    }

    pub(super) async fn cleanup_deleted_inode_storage(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
        used_space_bytes: u64,
        gc_reason: &str,
        fallback_object_ids: Vec<u64>,
    ) -> VfsResult<()> {
        let cleanup = self.collect_deleted_inode_cleanup_after_unlink(
            meta,
            ino,
            used_space_bytes,
            gc_reason,
            fallback_object_ids,
        )?;
        self.finalize_deleted_inode_runtime_cleanup(ino, cleanup.removed_ids)
            .await;
        Ok(())
    }

    pub(super) fn cleanup_deleted_inode_storage_deferred_cache_eviction(
        &self,
        meta: &Arc<dyn VfsMeta>,
        ino: Ino,
        used_space_bytes: u64,
        gc_reason: &str,
        fallback_object_ids: Vec<u64>,
    ) -> VfsResult<()> {
        let cleanup = self.collect_deleted_inode_cleanup(
            meta,
            ino,
            used_space_bytes,
            gc_reason,
            fallback_object_ids,
        )?;
        self.finalize_deleted_inode_runtime_cleanup_deferred_cache_eviction(
            ino,
            cleanup.removed_ids,
        );
        Ok(())
    }

    /// Immediately evict **all** blocks for `ino` from every cache tier.
    ///
    /// Called by `unlink()` so that deleted file data is no longer occupying
    /// disk / RAM space without waiting for the next background GC run.
    pub(super) async fn evict_inode_from_caches(&self, ino: Ino, object_ids: &[u64]) {
        if object_ids.is_empty() {
            self.readahead.evict(ino);
            self.clear_access_tracking(ino);
            return;
        }
        #[cfg(test)]
        self.state.upload.pause_deleted_inode_eviction_for_test();

        // L1.5 — dirty memory buffer: drain blocks buffered post-flush.
        self.clear_pending_payloads_for_inode(ino);

        self.remove_live_extents_for_inode(ino);

        // User-visible delete cleanup is allowed to spend foreground time
        // reclaiming local space. Cancel pending uploads first, then remove
        // every local artifact owned by the deleted inode instead of waiting
        // for ordinary cache pressure or upload completion.
        self.cancel_pending_uploads_for_inode(ino);

        let mut forced_pinned = 0;
        for &object_id in object_ids {
            if self.cached_store.is_pinned(object_id) {
                forced_pinned += 1;
                self.cached_store.unpin(object_id);
            }
        }

        {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in object_ids {
                pending_evictions.remove(object_id);
            }
        }

        self.upload_hot_remove_batch(object_ids);
        if let Err(error) = self.upload_stage_remove_batch_async(object_ids).await {
            warn!(
                ino,
                error = %error,
                evicted_blocks = object_ids.len(),
                "evict_inode: upload stage batch removal failed"
            );
        }
        if !object_ids.is_empty()
            && let Err(error) = self.payload_store_remove_batch_async(object_ids).await
        {
            warn!(
                ino,
                error = %error,
                evicted_blocks = object_ids.len(),
                "evict_inode: local backend batch removal failed"
            );
        }
        debug!(
            "evict_inode: ino={} evicted {} blocks, forced_pinned={}",
            ino,
            object_ids.len(),
            forced_pinned
        );

        // Invalidate readahead state for this inode.
        self.readahead.evict(ino);
        self.clear_access_tracking(ino);
    }

    pub(super) async fn evict_payload_objects_from_caches(&self, ino: Ino, object_ids: &[u64]) {
        let pending_upload_object_ids = self
            .pending_uploads_snapshot_for_inode(ino)
            .into_iter()
            .map(|op| op.object_id)
            .collect::<std::collections::HashSet<_>>();
        // A completed upload remains read-owned by its live extent until publication;
        // the finalizer may already be running and no longer be in the pending queue.
        let live_extent_object_ids = self
            .live_extent_records_for_inode(ino)
            .into_iter()
            .map(|(_, object_id, _)| object_id)
            .collect::<std::collections::HashSet<_>>();
        let object_ids = object_ids
            .iter()
            .copied()
            .filter(|object_id| {
                !pending_upload_object_ids.contains(object_id)
                    && !live_extent_object_ids.contains(object_id)
            })
            .collect::<Vec<_>>();
        if object_ids.is_empty() {
            return;
        }

        let mut forced_pinned = 0;
        for &object_id in &object_ids {
            if self.cached_store.is_pinned(object_id) {
                forced_pinned += 1;
                self.cached_store.unpin(object_id);
            }
        }

        {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in &object_ids {
                pending_evictions.remove(object_id);
            }
        }

        self.upload_hot_remove_batch(&object_ids);
        if let Err(error) = self.upload_stage_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                error = %error,
                evicted_blocks = object_ids.len(),
                "evict_payload_objects: upload stage batch removal failed"
            );
        }
        if let Err(error) = self.payload_store_remove_batch_async(&object_ids).await {
            warn!(
                ino,
                error = %error,
                evicted_blocks = object_ids.len(),
                "evict_payload_objects: local backend batch removal failed"
            );
        }
        debug!(
            "evict_payload_objects: ino={} evicted {} blocks, forced_pinned={}",
            ino,
            object_ids.len(),
            forced_pinned
        );

        self.readahead.evict(ino);
        self.clear_access_tracking(ino);
    }
}
