use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;

use super::file_session::FileSessionOperation;
use super::overlay::{ExtentRecordId, PendingUploadCatalog, PendingUploadKey, pending_upload_key};
use super::{PendingUpload, StudioVfs, VfsResult, ZEROIO_REPLAY_EVENT_SAMPLE_LIMIT};
use studiofs_meta::MetaError;
use studiofs_meta::schema::{
    DEFAULT_CHUNK_SIZE, UploadState as MetaUploadState, chunk_start_offset,
};
use studiofs_meta::types::{FileType, Ino, InodeAttr, block_index_from_offset};
use tracing::{debug, info, warn};

pub(super) struct StartupReplayCoordinator<'a> {
    vfs: &'a StudioVfs,
    pending_catalog: PendingUploadCatalog,
    dirty_set: HashSet<u64>,
    recovered_dirty_object_ids: HashSet<u64>,
    recovered_dirty_object_bytes: HashMap<u64, u64>,
    quarantined_inodes: HashSet<Ino>,
    quarantined_object_ids: HashSet<u64>,
    removed_orphan_dirty_object_ids: HashSet<u64>,
    replay_ops: Vec<(u64, u64, u64, u64)>,
    live_extents: HashMap<(Ino, u32), ExtentRecordId>,
    pending_epoch_finalizations: Vec<(Ino, u64)>,
    seen_replay_inodes: HashSet<Ino>,
    valid_pending_uploads: HashSet<PendingUploadKey>,
    stale_pending_uploads: HashMap<PendingUploadKey, PendingUpload>,
    lost_unpublished_upload_inodes: HashSet<Ino>,
    restored_live_extents: usize,
    replay_failures: u64,
    orphan_dirty_payloads_removed: u64,
    unreferenced_local_payloads_removed: u64,
}

#[derive(Debug, Clone, Copy)]
struct StartupGraphObject {
    block_index: u32,
    object_id: u64,
    payload_len: u64,
    absolute_offset: u64,
}

impl<'a> StartupReplayCoordinator<'a> {
    pub(super) fn new(vfs: &'a StudioVfs, pending_uploads: &[PendingUpload]) -> Self {
        Self {
            vfs,
            pending_catalog: PendingUploadCatalog::from_ops(pending_uploads.iter().cloned()),
            dirty_set: vfs
                .local_payload_backend
                .dirty_object_ids()
                .into_iter()
                .collect(),
            recovered_dirty_object_ids: HashSet::new(),
            recovered_dirty_object_bytes: HashMap::new(),
            quarantined_inodes: HashSet::new(),
            quarantined_object_ids: HashSet::new(),
            removed_orphan_dirty_object_ids: HashSet::new(),
            replay_ops: Vec::new(),
            live_extents: HashMap::new(),
            pending_epoch_finalizations: Vec::new(),
            seen_replay_inodes: HashSet::new(),
            valid_pending_uploads: HashSet::new(),
            stale_pending_uploads: HashMap::new(),
            lost_unpublished_upload_inodes: HashSet::new(),
            restored_live_extents: 0,
            replay_failures: 0,
            orphan_dirty_payloads_removed: 0,
            unreferenced_local_payloads_removed: 0,
        }
    }

    pub(super) fn run(mut self, attrs: &[InodeAttr]) {
        for attr in attrs {
            self.process_attr(attr);
        }
        self.finish();
    }

    fn process_attr(&mut self, attr: &InodeAttr) {
        let ino = attr.ino;
        let needs_finalization = StudioVfs::needs_epoch_finalization(attr);
        let has_pending_uploads = self.pending_catalog.has_inode(ino);
        if has_pending_uploads {
            self.seen_replay_inodes.insert(ino);
        }
        if !self.needs_scan(attr) && !has_pending_uploads {
            return;
        }

        let graph_objects = match self.regular_file_graph_objects(attr) {
            Ok(graph_objects) => graph_objects,
            Err(error) => {
                self.replay_failures = self.replay_failures.saturating_add(1);
                warn!(
                    ino,
                    error = %error,
                    "failed to enumerate regular-file graph during startup recovery"
                );
                return;
            }
        };

        if has_pending_uploads {
            self.classify_pending_uploads_for_attr(attr, graph_objects.as_slice());
        }

        if graph_objects.is_empty() && Self::is_lost_unpublished_regular_upload(attr) {
            self.lost_unpublished_upload_inodes.insert(ino);
            return;
        }

        if self.has_dirty_or_recovered_objects() {
            for graph_object in &graph_objects {
                self.record_recovered_graph_object_if_local(ino, graph_object, attr.content_epoch);
            }
        }

        if needs_finalization {
            self.record_epoch_finalization(ino, attr.content_epoch, graph_objects.as_slice());
        }
    }

    fn is_lost_unpublished_regular_upload(attr: &InodeAttr) -> bool {
        attr.kind == FileType::RegularFile
            && attr.upload_state != MetaUploadState::Ready
            && attr.committed_size == 0
            && attr.committed_content_epoch == 0
    }

    fn classify_pending_uploads_for_attr(
        &mut self,
        attr: &InodeAttr,
        graph_objects: &[StartupGraphObject],
    ) {
        let current_graph_objects: HashSet<_> = graph_objects
            .iter()
            .map(|graph_object| (graph_object.absolute_offset, graph_object.object_id))
            .collect();

        for op in self.pending_catalog.ops_for_inode(attr.ino) {
            let key = pending_upload_key(op);
            let is_live = attr.upload_state != MetaUploadState::Ready
                && op.content_epoch == attr.content_epoch
                && current_graph_objects.contains(&(op.offset, op.object_id));
            if is_live {
                self.valid_pending_uploads.insert(key);
                self.stale_pending_uploads.remove(&key);
            } else if !self.valid_pending_uploads.contains(&key) {
                self.stale_pending_uploads.insert(key, op.clone());
            }
        }
    }

    fn regular_file_graph_objects(&self, attr: &InodeAttr) -> VfsResult<Vec<StartupGraphObject>> {
        if attr.size == 0 {
            return Ok(Vec::new());
        }
        let spans = self
            .vfs
            .sync_meta()
            .lookup_regular_file_slices(attr.ino, 0, attr.size)?;
        let mut objects = Vec::new();
        for span in spans {
            let Some(block) = span.payload else {
                continue;
            };
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let absolute_offset = chunk_base.saturating_add(span.range.start as u64);
            objects.push(StartupGraphObject {
                block_index: block_index_from_offset(absolute_offset, self.vfs.block_size),
                object_id: block.object_id,
                payload_len: u64::from(block.offset).saturating_add(u64::from(block.length)),
                absolute_offset,
            });
        }
        Ok(objects)
    }

    fn needs_scan(&self, attr: &InodeAttr) -> bool {
        !self.dirty_set.is_empty()
            || !self.recovered_dirty_object_ids.is_empty()
            || StudioVfs::needs_epoch_finalization(attr)
    }

    fn has_dirty_or_recovered_objects(&self) -> bool {
        !self.dirty_set.is_empty() || !self.recovered_dirty_object_ids.is_empty()
    }

    fn take_dirty_object(&mut self, object_id: u64) -> bool {
        self.dirty_set.remove(&object_id)
    }

    fn discard_quarantined_dirty_objects(&mut self) {
        if self.quarantined_object_ids.is_empty() {
            return;
        }
        self.dirty_set
            .retain(|object_id| !self.quarantined_object_ids.contains(object_id));
    }

    fn record_replayed_graph_object(
        &mut self,
        ino: Ino,
        graph_object: &StartupGraphObject,
        content_epoch: u64,
    ) {
        self.replay_ops.push((
            ino,
            graph_object.absolute_offset,
            graph_object.object_id,
            content_epoch,
        ));
        self.record_live_extent(
            ino,
            graph_object.block_index,
            graph_object.object_id,
            content_epoch,
        );
        self.restored_live_extents = self.restored_live_extents.saturating_add(1);
    }

    fn record_recovered_graph_object_if_local(
        &mut self,
        ino: Ino,
        graph_object: &StartupGraphObject,
        content_epoch: u64,
    ) {
        if self.take_dirty_object(graph_object.object_id) {
            self.recovered_dirty_object_ids
                .insert(graph_object.object_id);
            self.record_recovered_object_bytes(graph_object);
            self.record_replayed_graph_object(ino, graph_object, content_epoch);
        } else if self
            .recovered_dirty_object_ids
            .contains(&graph_object.object_id)
        {
            self.record_recovered_object_bytes(graph_object);
            self.record_recovered_live_extent(ino, graph_object, content_epoch);
        }
    }

    fn record_recovered_object_bytes(&mut self, graph_object: &StartupGraphObject) {
        self.recovered_dirty_object_bytes
            .entry(graph_object.object_id)
            .and_modify(|current| *current = (*current).max(graph_object.payload_len))
            .or_insert(graph_object.payload_len);
    }

    fn record_recovered_live_extent(
        &mut self,
        ino: Ino,
        graph_object: &StartupGraphObject,
        content_epoch: u64,
    ) {
        self.record_live_extent(
            ino,
            graph_object.block_index,
            graph_object.object_id,
            content_epoch,
        );
        self.restored_live_extents = self.restored_live_extents.saturating_add(1);
    }

    fn record_epoch_finalization(
        &mut self,
        ino: Ino,
        content_epoch: u64,
        graph_objects: &[StartupGraphObject],
    ) {
        for graph_object in graph_objects {
            self.record_live_extent(
                ino,
                graph_object.block_index,
                graph_object.object_id,
                content_epoch,
            );
        }
        self.pending_epoch_finalizations.push((ino, content_epoch));
    }

    fn merge_pending_live_extents(&mut self) {
        let live_extents = self.pending_catalog.live_extents(
            self.vfs.block_size,
            &self.quarantined_inodes,
            &self.valid_pending_uploads,
        );
        for (ino, block_index, object_id, content_epoch) in live_extents {
            self.record_pending_live_extent(ino, block_index, object_id, content_epoch);
        }
    }

    fn mark_pending_uploads_without_attrs_stale(&mut self) {
        for op in self.pending_catalog.ops() {
            if self.seen_replay_inodes.contains(&op.ino) {
                continue;
            }
            let key = pending_upload_key(op);
            if !self.valid_pending_uploads.contains(&key) {
                self.stale_pending_uploads.insert(key, op.clone());
            }
        }
    }

    fn cancel_stale_pending_uploads(&self) {
        if self.stale_pending_uploads.is_empty() {
            return;
        }

        for op in self.stale_pending_uploads.values() {
            self.vfs.cached_store.cancel_upload(op);
            self.vfs
                .state
                .upload
                .pending_evictions
                .lock()
                .remove(&op.object_id);
            self.vfs
                .debug_pending_inode_state(op.ino, "startup_stale_pending_upload_canceled");
        }
    }

    fn remove_lost_unpublished_upload_inodes(&self) {
        if self.lost_unpublished_upload_inodes.is_empty() {
            return;
        }

        for ino in &self.lost_unpublished_upload_inodes {
            let meta = self.vfs.sync_meta();
            let attr = match meta.get_attr(*ino) {
                Ok(attr) => attr,
                Err(MetaError::InodeNotFound(_)) => continue,
                Err(error) => {
                    warn!(
                        ino,
                        error = %error,
                        "startup recovery failed to load lost unpublished upload inode"
                    );
                    continue;
                }
            };
            if !Self::is_lost_unpublished_regular_upload(&attr) {
                continue;
            }
            let entries = match meta.readdir(attr.parent) {
                Ok(entries) => entries,
                Err(error) => {
                    warn!(
                        ino,
                        parent = attr.parent,
                        error = %error,
                        "startup recovery failed to enumerate parent for lost unpublished upload inode"
                    );
                    continue;
                }
            };
            for entry in entries.into_iter().filter(|entry| entry.ino == *ino) {
                if let Err(error) = meta.remove_entry(attr.parent, &entry.name) {
                    warn!(
                        ino,
                        parent = attr.parent,
                        name = %String::from_utf8_lossy(&entry.name),
                        error = %error,
                        "startup recovery failed to remove lost unpublished upload dentry"
                    );
                }
            }
            if let Err(error) = meta.remove_attr(*ino) {
                warn!(
                    ino,
                    error = %error,
                    "startup recovery failed to remove lost unpublished upload inode"
                );
                continue;
            }
            info!(
                ino,
                "startup recovery removed lost unpublished upload inode without durable payload graph"
            );
        }
    }

    fn live_extents_batch(&self) -> Vec<(Ino, u32, u64, u64)> {
        let mut extents: Vec<_> = self
            .live_extents
            .iter()
            .map(|(&(ino, block_index), extent)| {
                (ino, block_index, extent.object_id, extent.content_epoch)
            })
            .collect();
        extents.sort_by_key(|(ino, block_index, _, _)| (*ino, *block_index));
        extents
    }

    fn queued_epoch_count(&self) -> usize {
        self.pending_epoch_finalizations.len()
    }

    fn record_live_extent_entry(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
        allow_equal_epoch: bool,
    ) {
        let next = ExtentRecordId {
            object_id,
            content_epoch,
        };
        match self.live_extents.entry((ino, block_index)) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(next);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let current_epoch = entry.get().content_epoch;
                if next.content_epoch > current_epoch
                    || (allow_equal_epoch && next.content_epoch == current_epoch)
                {
                    entry.insert(next);
                }
            }
        }
    }

    fn record_live_extent(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
    ) {
        self.record_live_extent_entry(ino, block_index, object_id, content_epoch, true);
    }

    fn record_pending_live_extent(
        &mut self,
        ino: Ino,
        block_index: u32,
        object_id: u64,
        content_epoch: u64,
    ) {
        self.record_live_extent_entry(ino, block_index, object_id, content_epoch, false);
    }

    fn cleanup_orphan_dirty_payloads(&mut self, reachable_object_ids: Option<&HashSet<u64>>) {
        if self.dirty_set.is_empty() {
            return;
        }
        if !self.quarantined_inodes.is_empty() {
            self.warn_orphan_dirty_payloads();
            return;
        }
        if !self.retain_non_metadata_reachable_dirty_payloads(reachable_object_ids) {
            return;
        }
        self.remove_orphan_dirty_payloads();
    }

    fn retain_non_metadata_reachable_dirty_payloads(
        &mut self,
        reachable_object_ids: Option<&HashSet<u64>>,
    ) -> bool {
        if let Some(reachable_object_ids) = reachable_object_ids {
            self.dirty_set
                .retain(|object_id| !reachable_object_ids.contains(object_id));
            true
        } else {
            self.dirty_set.clear();
            false
        }
    }

    fn remove_orphan_dirty_payloads(&mut self) {
        if self.dirty_set.is_empty() {
            return;
        }
        let orphan_count = self.dirty_set.len() as u64;
        self.orphan_dirty_payloads_removed = self
            .orphan_dirty_payloads_removed
            .saturating_add(orphan_count);
        let orphan_object_ids: Vec<_> = self.dirty_set.iter().copied().collect();
        self.removed_orphan_dirty_object_ids
            .extend(orphan_object_ids.iter().copied());
        for object_id in &orphan_object_ids {
            warn!(
                object_id,
                "removing orphan dirty payload object without metadata reference"
            );
        }
        self.vfs
            .cached_store
            .remove_hot_upload_batch(&orphan_object_ids);
        self.vfs
            .cached_store
            .remove_staged_upload_batch(&orphan_object_ids);
        for object_id in &orphan_object_ids {
            self.vfs.local_payload_backend.remove(*object_id);
        }
        {
            let mut pending_evictions = self.vfs.state.upload.pending_evictions.lock();
            for object_id in &orphan_object_ids {
                pending_evictions.remove(object_id);
            }
        }
        self.vfs
            .state
            .upload
            .upload_canceled_stale
            .fetch_add(orphan_count, Ordering::Relaxed);
        info!(
            orphan_dirty_payloads = orphan_object_ids.len(),
            "startup cleanup removed orphan dirty local backend entries"
        );
        self.dirty_set.clear();
    }

    fn cleanup_unreferenced_local_payloads(
        &mut self,
        local_object_ids: Vec<u64>,
        protected_object_ids: &HashSet<u64>,
        reachable_object_ids: Option<&HashSet<u64>>,
    ) {
        if local_object_ids.is_empty() {
            return;
        }

        let Some(reachable_object_ids) = reachable_object_ids else {
            return;
        };

        let stale_object_ids: Vec<_> = local_object_ids
            .into_iter()
            .filter(|object_id| {
                !reachable_object_ids.contains(object_id)
                    && !protected_object_ids.contains(object_id)
                    && !self.quarantined_object_ids.contains(object_id)
                    && !self.removed_orphan_dirty_object_ids.contains(object_id)
            })
            .collect();
        if stale_object_ids.is_empty() {
            return;
        }

        self.vfs
            .cached_store
            .remove_hot_upload_batch(&stale_object_ids);
        self.vfs
            .cached_store
            .remove_staged_upload_batch(&stale_object_ids);
        self.vfs.cached_store.remove_batch(&stale_object_ids);
        {
            let mut pending_evictions = self.vfs.state.upload.pending_evictions.lock();
            for object_id in &stale_object_ids {
                pending_evictions.remove(object_id);
            }
        }

        let stale_count = stale_object_ids.len() as u64;
        self.unreferenced_local_payloads_removed = self
            .unreferenced_local_payloads_removed
            .saturating_add(stale_count);
        self.vfs
            .state
            .upload
            .upload_canceled_stale
            .fetch_add(stale_count, Ordering::Relaxed);
        info!(
            unreferenced_payloads = stale_object_ids.len(),
            "startup cleanup removed local payload objects without metadata references"
        );
    }

    fn metadata_reachable_object_ids(&self) -> Option<HashSet<u64>> {
        match self.vfs.sync_meta().get_referenced_object_ids() {
            Ok(ids) => Some(ids),
            Err(error) => {
                warn!(
                    error = %error,
                    "failed to enumerate metadata reachability during startup recovery; preserving local payload objects"
                );
                None
            }
        }
    }

    fn warn_orphan_dirty_payloads(&self) {
        warn!(
            quarantined_inodes = self.quarantined_inodes.len(),
            orphan_dirty_payloads = self.dirty_set.len(),
            "startup recovery preserved dirty local backend entries because invalid graphs were quarantined"
        );
    }

    fn finish(mut self) {
        self.mark_pending_uploads_without_attrs_stale();
        self.discard_quarantined_dirty_objects();
        self.merge_pending_live_extents();
        let replayed_blocks = self.restored_live_extents as u64;
        let mut replayed_object_ids: Vec<_> = self
            .replay_ops
            .iter()
            .map(|(_, _, object_id, _)| *object_id)
            .collect();
        replayed_object_ids.sort_unstable();
        replayed_object_ids.dedup();
        let replayed_bytes = replayed_object_ids.iter().fold(0_u64, |total, object_id| {
            total.saturating_add(
                self.recovered_dirty_object_bytes
                    .get(object_id)
                    .copied()
                    .unwrap_or(0),
            )
        });
        let replayed_epochs: BTreeSet<_> = self
            .replay_ops
            .iter()
            .map(|(ino, _, _, content_epoch)| (*ino, *content_epoch))
            .collect();
        self.vfs
            .state
            .metrics
            .zeroio_replay_failures
            .fetch_add(self.replay_failures, Ordering::Relaxed);

        if !self.replay_ops.is_empty() {
            info!(
                replayed_dirty_payloads = self.replay_ops.len(),
                restored_live_extents = self.restored_live_extents,
                "startup replay restored runtime pending uploads from local backend"
            );
        }

        let live_extents = self.live_extents_batch();
        if !live_extents.is_empty() {
            self.vfs.record_live_extents(&live_extents);
        }
        if !self.replay_ops.is_empty() {
            if let Err(error) = self
                .vfs
                .cached_store
                .link_upload_stage_batch(&replayed_object_ids)
            {
                warn!(
                    replayed_dirty_payloads = self.replay_ops.len(),
                    replayed_object_count = replayed_object_ids.len(),
                    error = %error,
                    "startup replay failed to restore upload links for runtime pending uploads"
                );
            }
            self.vfs.cached_store.submit_uploads_batch(&self.replay_ops);
            for (ino, _) in replayed_epochs.iter().copied() {
                self.vfs.state.file_session.note_background_transition(
                    ino,
                    FileSessionOperation::Upload,
                    "queued",
                );
            }
        }

        self.remove_lost_unpublished_upload_inodes();
        for (ino, content_epoch) in &self.pending_epoch_finalizations {
            self.vfs.queue_epoch_finalization(*ino, *content_epoch);
        }
        if self.queued_epoch_count() > 0 {
            debug!(
                queued_epochs = self.queued_epoch_count(),
                "seeded epoch finalizer queue from metadata"
            );
        }
        self.cancel_stale_pending_uploads();
        {
            let mut replay_counts = self.vfs.state.upload.startup_replay_counts.lock();
            for (ino, content_epoch) in replayed_epochs.iter().copied() {
                *replay_counts.entry((ino, content_epoch)).or_insert(0) += 1;
            }
        }

        for (ino, content_epoch) in replayed_epochs
            .iter()
            .copied()
            .take(ZEROIO_REPLAY_EVENT_SAMPLE_LIMIT)
        {
            self.vfs.record_runtime_event(
                "zeroio",
                "epoch_replayed_after_restart",
                "zeroio epoch replayed after restart",
                serde_json::json!({
                    "ino": ino,
                    "content_epoch": content_epoch,
                    "path": self.vfs.best_effort_path_for_inode(ino),
                }),
            );
        }
        let local_object_ids = self.vfs.local_payload_backend.object_ids();
        let reachable_object_ids = if self.dirty_set.is_empty() && local_object_ids.is_empty() {
            None
        } else {
            self.metadata_reachable_object_ids()
        };
        self.cleanup_orphan_dirty_payloads(reachable_object_ids.as_ref());
        let protected_object_ids: HashSet<_> = replayed_object_ids
            .iter()
            .copied()
            .chain(self.dirty_set.iter().copied())
            .collect();
        self.cleanup_unreferenced_local_payloads(
            local_object_ids,
            &protected_object_ids,
            reachable_object_ids.as_ref(),
        );

        self.vfs.record_runtime_event(
            "zeroio",
            "startup_replay_summary",
            "zeroio startup replay summary",
            serde_json::json!({
                "replayed_epochs": replayed_epochs.len() as u64,
                "replayed_blocks": replayed_blocks,
                "replayed_bytes": replayed_bytes,
                "queued_uploads": self.replay_ops.len() as u64,
                "queued_publish_ready_epochs": self.queued_epoch_count() as u64,
                "quarantined_inodes": self.quarantined_inodes.len() as u64,
                "orphan_dirty_payloads_removed": self.orphan_dirty_payloads_removed,
                "unreferenced_local_payloads_removed": self.unreferenced_local_payloads_removed,
                "discarded_memory_only_bytes": 0_u64,
                "replay_failures": self.replay_failures,
            }),
        );

        if !self.quarantined_inodes.is_empty() {
            self.vfs
                .cancel_pending_uploads_for_inodes(&self.quarantined_inodes);
        }
    }
}
