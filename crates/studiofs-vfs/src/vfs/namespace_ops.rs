use super::{
    ATTR_CACHE_TTL, AccessLevel, Arc, ChunkId, DEFAULT_CHUNK_SIZE, DentryCacheValue,
    DirAccessEntry, DirEntry, FileSessionOperation, FileType, Ino, InodeAttr,
    InodeIncarnationState, Instant, LocalSyncPolicy, MetaError, NamespacePathStyle,
    PendingIncarnation, ResolvedSliceSource, SetattrArgs, SliceId, SliceOrder, SliceRange,
    SliceRecord, StudioVfs, UnlinkTarget, VfsError, VfsMeta, VfsResult, VfsState, VfsUploadState,
    block_index_from_offset, chunk_id_from_offset, chunk_start_offset, debug, info,
    offset_within_block, validate_name, warn,
};
use std::collections::{HashMap, HashSet};
use studiofs_meta::schema::PayloadRefState;

struct TruncateMutation {
    removed_objects: Vec<(u32, u64)>,
    restore_slices: ChunkSliceBatch,
    installed_pending_tail_block: Option<u32>,
}

type ChunkSliceBatch = Vec<(ChunkId, Vec<SliceRecord>)>;
type RemovedObjectList = Vec<(u32, u64)>;

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
    /// Update inode metadata fields without an open-file handle context.
    ///
    /// This is the handle-free entry point for setattr operations and delegates to
    /// `setattr_with_handle` with no handle.
    pub fn setattr(&self, ino: Ino, args: SetattrArgs) -> VfsResult<InodeAttr> {
        self.setattr_with_handle(None, ino, args)
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

        let mut attr = self.getattr(ino)?;
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
            return Self::run_sync_future(self.setattr_with_size_change(
                ino,
                args.clone(),
                defer_handle_growth_flush,
            ));
        }

        let mut changed = false;
        if let Some(mode) = args.mode {
            attr.mode = mode & 0o7777;
            changed = true;
        }

        if let Some(uid) = args.uid {
            attr.uid = uid;
            changed = true;
        }

        if let Some(gid) = args.gid {
            attr.gid = gid;
            changed = true;
        }

        if let Some(ns) = args.atime.resolve() {
            attr.atime = ns;
            changed = true;
        }

        if let Some(ns) = args.mtime.resolve() {
            attr.mtime = ns;
            changed = true;
        }

        if let Some(ns) = args.crtime.resolve() {
            attr.crtime = ns;
            changed = true;
        }

        if let Some(wa) = args.win_attrs {
            attr.win_attrs = wa;
            changed = true;
        }

        if changed {
            attr.touch_ctime();
            self.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, attr.clone());
            self.state.namespace.attr_read_cache.lock().remove(ino);
            self.sync_meta().set_attr(&attr)?;
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
            self.state.namespace.attr_read_cache.lock().remove(ino);

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
        self.state.namespace.attr_read_cache.lock().remove(ino);

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

        self.cancel_pending_uploads_for_inode(ino);
        if retained.is_empty() {
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
        let live_extents: Vec<_> = retained
            .into_iter()
            .map(|(object_id, (_, _, _, block_index))| (ino, block_index, object_id, content_epoch))
            .collect();
        self.record_live_extents(&live_extents);
        self.cached_store.submit_uploads_batch(&upload_ops);
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
            self.state.namespace.attr_read_cache.lock().remove(ino);
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

        let entry = match self.sync_meta().lookup(parent, name) {
            Ok(entry) => entry,
            Err(MetaError::EntryNotFound { .. }) => {
                self.dentry_cache_insert_negative(parent, name);
                return Err(Self::ignored_entry_not_found(parent, name));
            }
            Err(err) => return Err(VfsError::Meta(err)),
        };
        self.dentry_cache_insert_positive(parent, name, entry.ino);
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

    /// Read directory entries from `ino` with namespace filtering and access checks.
    ///
    /// The method applies ACL filtering, skips inaccessible entries, drops platform/root
    /// blocked entries when necessary, and schedules batched attribute hydration for the
    /// returned entries.
    pub fn readdir(&self, ino: Ino) -> VfsResult<Vec<DirEntry>> {
        let parent_effective = self.effective_access_level(ino)?;
        if parent_effective == AccessLevel::NoAccess {
            return Ok(Vec::new());
        }
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if self.ignored_dir_attr(ino).is_some() {
            return Ok(Vec::new());
        }

        let meta = self.sync_meta();
        let mut entries = meta.readdir(ino)?;
        if ino == super::ROOT_INO {
            entries.retain(|entry| {
                !super::is_blocked_root_macos_metadata(&entry.name)
                    && !super::is_probable_trash_component(&entry.name)
            });
        }

        let mut confirmed_entry_inos = HashSet::new();
        if !entries.is_empty() {
            let child_inos = entries.iter().map(|entry| entry.ino).collect::<Vec<_>>();
            let attrs = meta.get_attrs_batch(&child_inos)?;
            let attrs_by_ino = attrs
                .into_iter()
                .map(|attr| (attr.ino, attr))
                .collect::<HashMap<_, _>>();
            confirmed_entry_inos.extend(attrs_by_ino.keys().copied());
            let now = Instant::now();
            let mut attr_cache = self.state.namespace.attr_read_cache.lock();
            for entry in &entries {
                if let Some(attr) = attrs_by_ino.get(&entry.ino) {
                    attr_cache.put(entry.ino, attr.clone(), ATTR_CACHE_TTL, now);
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
            self.dentry_cache_insert_positive(ino, &entry.name, entry.ino);
        }

        let file_children: Vec<DirAccessEntry<'_>> = entries
            .iter()
            .filter(|e| e.kind == FileType::RegularFile)
            .map(|e| DirAccessEntry::new(e.ino, &e.name))
            .collect();
        if !file_children.is_empty() {
            self.dir_prefetch.on_readdir_entries(ino, file_children);
        }
        self.schedule_readdir_attr_hydration(&entries);

        Ok(entries)
    }

    fn schedule_readdir_attr_hydration(&self, entries: &[DirEntry]) {
        if entries.is_empty()
            || self.readdir_attr_batch_chunk == 0
            || self.readdir_attr_batch_max_inodes == 0
        {
            return;
        }

        let mut inodes = Vec::with_capacity(entries.len().min(self.readdir_attr_batch_max_inodes));
        for entry in entries.iter().take(self.readdir_attr_batch_max_inodes) {
            if self.ignored_entry_attr(entry.ino).is_some() {
                continue;
            }
            inodes.push(entry.ino);
        }
        if inodes.is_empty() {
            return;
        }

        let meta = self.sync_meta();
        let state = self.state.clone();
        let chunk_size = self.readdir_attr_batch_chunk;
        let hydrate = move || {
            Self::hydrate_readdir_attr_cache_batches(meta, state, inodes, chunk_size);
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn_blocking(hydrate));
        } else {
            drop(std::thread::spawn(hydrate));
        }
    }

    fn hydrate_readdir_attr_cache_batches(
        meta: Arc<dyn VfsMeta>,
        state: Arc<VfsState>,
        inodes: Vec<Ino>,
        chunk_size: usize,
    ) {
        for chunk in inodes.chunks(chunk_size.max(1)) {
            let attrs = match meta.get_attrs_batch(chunk) {
                Ok(attrs) => attrs,
                Err(err) => {
                    debug!(
                        chunk_len = chunk.len(),
                        error = %err,
                        "readdir attr hydration batch failed"
                    );
                    continue;
                }
            };

            let now = Instant::now();
            let dirty_attrs = state.namespace.dirty_attrs.lock();
            let mut attr_cache = state.namespace.attr_read_cache.lock();
            for attr in attrs {
                if dirty_attrs.contains_key(&attr.ino) {
                    continue;
                }
                attr_cache.put(attr.ino, attr, ATTR_CACHE_TTL, now);
            }
        }
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
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        self.enforce_access(new_parent, AccessLevel::ReadWrite)?;
        validate_name(new_name)?;
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
        let attr = self.sync_meta().link(ino, new_parent, new_name)?;
        namespace_mutations.record_insertion(new_parent, new_name, ino, [ino]);
        self.bump_acl_namespace_epoch();
        self.state.namespace.dirty_attrs.lock().remove(&ino);
        self.sync_meta().request_eager_flush();
        Ok(attr)
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
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        if self.should_ignore_regular_file_path(parent, name) {
            let attr =
                self.create_ignored_entry(parent, name, uid, gid, mode, FileType::RegularFile);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_file(parent, name, uid, gid, mode & 0o7777)?;
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
        self.sync_meta().request_eager_flush();
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
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        validate_name(name)?;
        if self.should_ignore_directory_path(parent, name) {
            let attr = self.create_ignored_entry(parent, name, uid, gid, mode, FileType::Directory);
            namespace_mutations.record_ignored_insertion(parent, std::iter::empty());
            return Ok(attr);
        }
        let attr = self
            .sync_meta()
            .create_dir(parent, name, uid, gid, mode & 0o7777)?;
        namespace_mutations.record_insertion(parent, name, attr.ino, std::iter::empty());
        self.sync_meta().request_eager_flush();
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
        if self.unlink_ignored_entry(parent, name) || self.ignored_dir_attr(parent).is_some() {
            namespace_mutations.record_removal(parent, name, std::iter::empty());
            return Ok(None);
        }

        let meta = self.sync_meta();
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
            if unlinked_attr.nlink > 0 {
                self.state.namespace.attr_read_cache.lock().put(
                    unlinked_attr.ino,
                    unlinked_attr.clone(),
                    ATTR_CACHE_TTL,
                    Instant::now(),
                );
            }
            meta.request_eager_flush();
            Ok(Some(unlinked_attr))
        }
        .await;
        drop(delete_session);
        self.state.file_session.gc_if_idle(initial_target.ino);
        result
    }

    /// Remove an empty directory `name` from directory `parent`.
    pub fn rmdir(&self, parent: Ino, name: &[u8]) -> VfsResult<()> {
        let namespace_mutations = self.namespace_mutations();
        self.enforce_access(parent, AccessLevel::ReadWrite)?;
        if self.unlink_ignored_subtree(parent, name) || self.ignored_dir_attr(parent).is_some() {
            namespace_mutations.record_removal(parent, name, std::iter::empty());
            return Ok(());
        }
        let removed = self
            .sync_meta()
            .lookup(parent, name)
            .ok()
            .map(|entry| entry.ino);
        self.sync_meta().rmdir(parent, name)?;
        namespace_mutations.record_removal(parent, name, std::iter::empty());
        if let Some(removed_ino) = removed {
            self.bump_directory_generation(removed_ino);
        }
        self.sync_meta().request_eager_flush();
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
                self.rmdir(current_parent, &current_name)?;
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
        if let Ok(target_entry) = meta.lookup(new_parent, new_name)
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
        }
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
