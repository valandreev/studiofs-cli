use super::{DeletedInodeCleanup, Ino, MetaError, StudioVfs, VfsMeta, VfsResult};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::runtime::run_meta;

pub(super) struct RegularFileGraphUsage {
    pub(super) object_ids: Vec<u64>,
    pub(super) total_bytes: u64,
}

impl StudioVfs {
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
        for &object_id in removed_ids {
            meta.add_pending_deletion(object_id, timestamp)?;
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
        self.mark_inode_deleted_for_uploads(ino);

        let graph_usage = self.regular_file_graph_usage(meta, ino)?;
        let removed_ids = meta.remove_regular_file_graph(ino)?;
        self.queue_pending_deletions_for_object_ids(meta, &removed_ids, gc_reason)?;
        let cleanup_ids = if !removed_ids.is_empty() {
            removed_ids
        } else if !graph_usage.object_ids.is_empty() {
            graph_usage.object_ids
        } else {
            fallback_object_ids
        };

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

            let removed_ids = match engine.remove_regular_file_graph(ino) {
                Ok(ids) => ids,
                Err(MetaError::InodeNotFound(_)) => Vec::new(),
                Err(err) => return Err(err),
            };
            for &object_id in &removed_ids {
                engine.add_pending_deletion(object_id, timestamp)?;
            }

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
        // #region debug log
        self.debug_write_probe(
            "H1",
            "crates/studiofs-vfs/src/vfs/deletion_cleanup.rs:101",
            "finalizing deleted inode runtime cleanup",
            serde_json::json!({
                "ino": ino,
                "removed_ids_len": removed_ids_len,
                "open_count": self.handle_table.open_count(ino),
                "deleted_marker": self.state.upload.deleted_inodes.lock().contains(&ino),
                "dirty_handle_bytes": self.state.handle_writers.dirty_bytes_for_inode(ino),
                "has_pending_local_dirty_payloads": self.has_pending_local_dirty_payloads_for_inode(ino),
                "has_inflight_materialization": self.state.deferred_materialize_inflight.lock().contains_key(&ino),
            }),
        );
        // #endregion debug log

        // Deleted inodes should never retain staged writes, even if no committed
        // payload slice refs existed yet. Preserve the old immediate-unlink contract by
        // also clearing live extent identities and read-side runtime state even
        // when no object IDs remain to evict.
        self.cancel_pending_uploads_for_inode(ino);
        self.state.handle_writers.clear_inode(ino);
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
        self.state.namespace.attr_read_cache.lock().remove(ino);
        self.maybe_clear_deleted_inode_marker(ino);
        deferred_local_payloads_len
    }

    pub(super) async fn cleanup_deleted_inode_storage(
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
        self.finalize_deleted_inode_runtime_cleanup(ino, cleanup.removed_ids)
            .await;
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

        // L1.5 — dirty memory buffer: drain blocks buffered post-flush.
        self.clear_pending_payloads_for_inode(ino);

        self.remove_live_extents_for_inode(ino);

        // User-visible delete cleanup is allowed to spend foreground time
        // reclaiming local space. Cancel pending uploads first, then remove
        // every local artifact owned by the deleted inode instead of waiting
        // for ordinary cache pressure or upload completion.
        self.cancel_pending_uploads_for_inode(ino);

        let mut evicted = Vec::with_capacity(object_ids.len());
        let mut forced_pinned = 0;
        for &object_id in object_ids {
            if self.cached_store.is_pinned(object_id) {
                forced_pinned += 1;
                self.cached_store.unpin(object_id);
            }
            evicted.push(object_id);
        }

        {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in &evicted {
                pending_evictions.remove(object_id);
            }
        }

        self.upload_hot_remove_batch(&evicted);
        if let Err(error) = self.upload_stage_remove_batch_async(&evicted).await {
            warn!(
                ino,
                error = %error,
                evicted_blocks = evicted.len(),
                "evict_inode: upload stage batch removal failed"
            );
        }
        if !evicted.is_empty() {
            if let Err(error) = self.payload_store_remove_batch_async(&evicted).await {
                warn!(
                    ino,
                    error = %error,
                    evicted_blocks = evicted.len(),
                    "evict_inode: local backend batch removal failed"
                );
            }
        }
        debug!(
            "evict_inode: ino={} evicted {} blocks, forced_pinned={}",
            ino,
            evicted.len(),
            forced_pinned
        );

        // Invalidate readahead state for this inode.
        self.readahead.evict(ino);
        self.clear_access_tracking(ino);
    }

    pub(super) async fn evict_file_cache_only(&self, ino: Ino) -> VfsResult<()> {
        let object_ids = self
            .regular_file_graph_usage(&self.sync_meta(), ino)?
            .object_ids;
        let mut evicted = Vec::new();
        let mut deferred_pinned = 0;
        let mut pending_evictions_to_add = Vec::new();
        for object_id in object_ids {
            if self.cached_store.is_pinned(object_id) {
                pending_evictions_to_add.push(object_id);
                deferred_pinned += 1;
                continue;
            }
            evicted.push(object_id);
        }
        if !pending_evictions_to_add.is_empty() {
            let mut pending_evictions = self.state.upload.pending_evictions.lock();
            for object_id in pending_evictions_to_add {
                pending_evictions.insert(object_id);
            }
        }
        if !evicted.is_empty() {
            self.payload_store_remove_batch_async(&evicted).await?;
        }
        if !evicted.is_empty() || deferred_pinned > 0 {
            info!(
                "trash-move cache eviction: ino={} evicted {} blocks, deferred pinned={}",
                ino,
                evicted.len(),
                deferred_pinned
            );
        }
        self.readahead.evict(ino);
        self.clear_access_tracking(ino);
        Ok(())
    }
}
