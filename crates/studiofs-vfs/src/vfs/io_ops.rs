use studiofs_meta::schema::TimeSpec;

use super::{
    AccessLevel, Bytes, ExplicitFlushOutcome, FOREGROUND_READ_WINDOW_BYTES, FileSessionOperation,
    FileType, HashMap, HashSet, Ino, InodeAttr, LocalSyncPolicy, PayloadRef, SetattrArgs,
    StickyErrorPolicy, StudioVfs, VfsError, VfsResult, block_index_from_offset, debug, error,
    format_object_fragment_keys, offset_within_block, warn,
};

pub(super) struct PayloadPins<'a> {
    backend: &'a dyn studiofs_cache::local::LocalPayloadBackend,
    ids: HashSet<u64>,
}

impl<'a> PayloadPins<'a> {
    pub(super) fn new(
        backend: &'a dyn studiofs_cache::local::LocalPayloadBackend,
        ids: HashSet<u64>,
    ) -> Self {
        for &id in &ids {
            backend.pin(id);
        }
        Self { backend, ids }
    }
}

impl Drop for PayloadPins<'_> {
    fn drop(&mut self) {
        for &id in &self.ids {
            self.backend.unpin(id);
        }
    }
}

fn direct_foreground_read_len(prepared_read_len: usize, caller_buffer_len: usize) -> usize {
    prepared_read_len.min(caller_buffer_len)
}

impl StudioVfs {
    /// Read file data at the given offset.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        self.reject_private_store_ino(ino)?;
        self.wait_for_deferred_release_inflight(ino).await;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_ordered(ino, offset, size).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    async fn read_ordered(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        if self.is_synthesized_ignored_ino(ino) {
            let ignored = self.visible_ignored_entry_snapshot(ino)?;
            let end = (offset + size as u64).min(ignored.attr.size);
            let len = end.saturating_sub(offset) as usize;
            let mut buf = vec![0u8; len];
            let read = self
                .read_ignored_entry_snapshot_into(&ignored, offset, &mut buf)
                .await?;
            buf.truncate(read as usize);
            return Ok(Bytes::from(buf));
        }
        let prepared = self
            .prepare_foreground_read(ino, offset, size as usize)
            .await?;
        if prepared.context.placeholder_visible(self, ino) {
            return Err(VfsError::RemoteContentPending);
        }
        if prepared.read_len == 0 {
            if size > 0 && offset > 0 {
                debug!(
                    ino,
                    offset,
                    size,
                    attr_size = prepared.context.attr.size,
                    attr_upload_state = ?prepared.context.attr.upload_state,
                    attr_committed_size = prepared.context.attr.committed_size,
                    "read_ordered: early EOF (offset >= attr.size) — possible stale attr"
                );
            }
            return Ok(Bytes::new());
        }
        let mut buf = vec![0u8; prepared.read_len];
        let written = self
            .execute_prepared_foreground_read(None, ino, offset, &mut buf, &prepared, None)
            .await?;
        buf.truncate(written as usize);
        Ok(Bytes::from(buf))
    }

    /// Read file data directly into a provided buffer (P1.3 — avoid intermediate Bytes allocation).
    ///
    /// Same as `read()` but writes directly to the output buffer, avoiding one memcpy.
    /// Returns the number of bytes written.
    /// On success, every byte up to the returned length is overwritten, including zero-filled holes.
    /// On error, the buffer may be partially modified; its contents must not be used as a read result.
    pub async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
        self.reject_private_store_ino(ino)?;
        self.wait_for_deferred_release_inflight(ino).await;
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Read)
            .await;
        let result = self.read_into_impl(None, ino, offset, buffer).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Calculate block offsets and spans needed to read `buffer_len` bytes from `offset`.
    ///
    /// The returned tuples are `(block_index, offset_within_block, bytes_to_copy)` and
    /// contain only ranges that intersect the given `file_size`.
    pub(super) fn calculate_blocks_needed(
        &self,
        offset: u64,
        buffer_len: usize,
        file_size: u64,
    ) -> Vec<(u32, usize, usize)> {
        let end = (offset + buffer_len as u64).min(file_size);
        if offset >= end {
            return Vec::new();
        }
        let mut blocks_needed: Vec<(u32, usize, usize)> = Vec::new();
        let mut cur = offset;
        while cur < end {
            let idx = block_index_from_offset(cur, self.block_size);
            let off_in = offset_within_block(cur, self.block_size) as usize;
            let space = self.block_size as usize - off_in;
            let to_copy = (end - cur).min(space as u64) as usize;
            blocks_needed.push((idx, off_in, to_copy));
            cur += to_copy as u64;
        }
        blocks_needed
    }

    /// Read file data into `buffer` (optionally pinned to a specific handle).
    ///
    /// Returns the number of bytes written, honoring current file size, local cache state,
    /// and foreground read planning.
    pub(super) async fn read_into_impl(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        let debug_path = self.debug_pending_target_path(ino);
        if let Some(path) = &debug_path {
            tracing::info!(
                ino,
                fh = fh.unwrap_or_default(),
                path = %path,
                offset,
                requested_len = buffer.len(),
                "debug target read_into_impl started"
            );
        }
        self.note_vfs_read_call(buffer.len());
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
        if let Some(path) = &debug_path {
            tracing::info!(
                ino,
                fh = fh.unwrap_or_default(),
                path = %path,
                offset,
                requested_len = buffer.len(),
                "debug target read_into_impl access granted"
            );
        }
        if self.is_synthesized_ignored_ino(ino) {
            let ignored = self.visible_ignored_entry_snapshot(ino)?;
            let end = offset
                .saturating_add(buffer.len() as u64)
                .min(ignored.attr.size);
            let len = end.saturating_sub(offset) as usize;
            return self
                .read_ignored_entry_snapshot_into(&ignored, offset, &mut buffer[..len])
                .await;
        }
        let open_file = fh.and_then(|handle| self.handle_table.get(handle));
        if let Some(open_file) = &open_file
            && let Some(read) = open_file.read_window_hit(ino, offset, buffer)
        {
            return Ok(read);
        }
        let read_window_token = open_file.as_ref().and_then(|open_file| {
            open_file.arm_read_window_prefetch(offset, buffer.len(), FOREGROUND_READ_WINDOW_BYTES)
        });
        let requested_len = if read_window_token.is_some() {
            FOREGROUND_READ_WINDOW_BYTES
        } else {
            buffer.len()
        };
        let prepared = self
            .prepare_foreground_read(ino, offset, requested_len)
            .await?;
        if let Some(open_file) = &open_file {
            self.observe_ram_readahead_file_size(open_file, prepared.context.attr.size);
        }
        if let Some(path) = &debug_path {
            tracing::info!(
                ino,
                fh = fh.unwrap_or_default(),
                path = %path,
                offset,
                requested_len,
                read_len = prepared.read_len,
                "debug target read_into_impl prepared foreground read"
            );
        }
        if prepared.context.placeholder_visible(self, ino) {
            return Err(VfsError::RemoteContentPending);
        }
        if prepared.read_len == 0 {
            if offset < (offset + buffer.len() as u64) && !buffer.is_empty() {
                // Requested range is beyond attr.size — this is a legitimate
                // EOF response, but warn when the request was not trivially at
                // or past EOF so we can detect attr.size mismatches in logs.
                let req_end = offset.saturating_add(buffer.len() as u64);
                if req_end > 0 && prepared.context.attr.size < req_end {
                    debug!(
                        ino,
                        offset,
                        requested_end = req_end,
                        attr_size = prepared.context.attr.size,
                        attr_upload_state = ?prepared.context.attr.upload_state,
                        attr_committed_size = prepared.context.attr.committed_size,
                        attr_content_epoch = prepared.context.attr.content_epoch,
                        attr_committed_content_epoch = prepared.context.attr.committed_content_epoch,
                        "read_into_impl: returning 0 bytes because attr.size < requested end (possible stale attr.size)"
                    );
                }
            }
            return Ok(0);
        }
        let read_window_is_current = read_window_token.is_some_and(|token| {
            open_file
                .as_ref()
                .is_some_and(|open_file| open_file.is_read_ahead_token_current(token))
        });
        let written = if prepared.read_len <= buffer.len() || !read_window_is_current {
            let direct_len = direct_foreground_read_len(prepared.read_len, buffer.len());
            self.execute_prepared_foreground_read(
                fh,
                ino,
                offset,
                &mut buffer[..direct_len],
                &prepared,
                None,
            )
            .await?
        } else {
            let mut window = self
                .read_window_buffer_pool
                .acquire_read_window(prepared.read_len);
            let token = read_window_token.expect("current speculative read has an owner token");
            let written = self
                .execute_prepared_foreground_read(
                    fh,
                    ino,
                    offset,
                    window.as_mut(),
                    &prepared,
                    Some((token, buffer.len())),
                )
                .await?;
            let read = buffer.len().min(written);
            buffer[..read].copy_from_slice(&window.as_ref()[..read]);
            if let Some(open_file) = &open_file {
                open_file.replace_read_window(ino, offset, window.into_bytes(written));
            }
            read
        };
        if let Some(open_file) = &open_file {
            open_file.record_read_progress(offset, written);
        }
        if let Some(path) = &debug_path {
            tracing::info!(
                ino,
                fh = fh.unwrap_or_default(),
                path = %path,
                offset,
                requested_len = buffer.len(),
                written,
                "debug target read_into_impl executed foreground read"
            );
        }
        Ok(written as u32)
    }

    /// Fetch a payload segment from remote object storage.
    ///
    /// When `prefetch` is true, the block is fetched through the prefetch scheduler
    /// to prioritize non-critical background retrieval.
    pub(super) async fn fetch_block_for_ref(
        &self,
        block: &PayloadRef,
        prefetch: bool,
    ) -> Result<Bytes, crate::fetch::FetchError> {
        let keys = format_object_fragment_keys(&self.data_prefix, block.object_id);
        if prefetch {
            self.fetch_scheduler
                .fetch_block_prefetch(block.object_id, &keys)
                .await
        } else {
            self.fetch_scheduler
                .fetch_block(block.object_id, &keys)
                .await
        }
    }

    /// Fetch a payload segment after a local cache miss.
    ///
    /// This path is used when a block lookup misses local cache and should retry fetching
    /// from the remote source immediately.
    pub(super) async fn fetch_block_for_ref_after_local_miss(
        &self,
        block: &PayloadRef,
    ) -> Result<Bytes, crate::fetch::FetchError> {
        let keys = format_object_fragment_keys(&self.data_prefix, block.object_id);
        self.fetch_scheduler
            .fetch_block_after_local_miss(block.object_id, &keys)
            .await
    }

    pub(super) async fn fetch_payload_range_for_ref_after_local_miss(
        &self,
        block: &PayloadRef,
        length: usize,
    ) -> Result<Option<Bytes>, crate::fetch::FetchError> {
        if length == 0 || length > crate::fetch::MAX_PARTIAL_FETCH_SIZE {
            return Ok(None);
        }
        let start = u64::from(block.offset);
        let Some(end) = start.checked_add(length as u64) else {
            return Ok(None);
        };
        let fragment_size = studiofs_data::object::CLOUD_FRAGMENT_SIZE as u64;
        if start / fragment_size != (end - 1) / fragment_size {
            return Ok(None);
        }

        let keys = format_object_fragment_keys(&self.data_prefix, block.object_id);
        self.fetch_scheduler
            .fetch_block_range_after_local_miss(block.object_id, &keys, start..end)
            .await
            .map(Some)
    }

    /// Copy raw payload bytes into `dst`, zero-filling areas beyond available source data.
    ///
    /// This helper converts block-relative offsets into destination spans while preserving
    /// partial-read behavior with zero padding.
    pub(super) fn copy_payload_into_range(&self, data: &[u8], block_offset: u64, dst: &mut [u8]) {
        let Ok(src_start) = usize::try_from(block_offset) else {
            warn!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                "copy_payload_into_range: block_offset does not fit usize, zeroing dst"
            );
            dst.fill(0);
            return;
        };
        if src_start >= data.len() {
            warn!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                "copy_payload_into_range: block_offset >= data.len(), zeroing dst (possible partial-payload read bug)"
            );
            dst.fill(0);
            return;
        }
        let copy_len = dst.len().min(data.len().saturating_sub(src_start));
        if copy_len > 0 {
            dst[..copy_len].copy_from_slice(&data[src_start..src_start + copy_len]);
        }
        if copy_len < dst.len() {
            debug!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                copy_len,
                "copy_payload_into_range: padding with zeros (partial payload or short read)"
            );
            dst[copy_len..].fill(0);
        }
    }

    /// Copy a committed payload span into `dst` by exact byte range.
    ///
    /// Returns `VfsError::Other` on invalid ranges or when the source payload is shorter
    /// than the requested destination span.
    pub(super) fn copy_committed_payload_into_range(
        &self,
        data: &[u8],
        block: &PayloadRef,
        file_offset: u64,
        dst: &mut [u8],
    ) -> VfsResult<()> {
        let src_start = usize::try_from(block.offset).map_err(|_| {
            VfsError::Other(format!(
                "committed payload object offset does not fit usize: object_id={}, offset={}",
                block.object_id, block.offset
            ))
        })?;
        let Some(src_end) = src_start.checked_add(dst.len()) else {
            return Err(VfsError::Other(format!(
                "committed payload object copy range overflow: object_id={}, offset={}, dst_len={}",
                block.object_id,
                block.offset,
                dst.len()
            )));
        };
        if src_end > data.len() {
            error!(
                object_id = block.object_id,
                file_offset,
                block_offset = block.offset,
                block_length = block.length,
                payload_len = data.len(),
                requested = dst.len(),
                "committed payload object shorter than requested span"
            );
            return Err(VfsError::Other(format!(
                "committed payload object too short: object_id={}, payload_len={}, required_end={}",
                block.object_id,
                data.len(),
                src_end
            )));
        }
        dst.copy_from_slice(&data[src_start..src_end]);
        Ok(())
    }

    /// Write data to a file at the given offset.
    ///
    /// Ordinary foreground writes now keep Local-First visibility immediately,
    /// eagerly accepting completed full blocks while allowing the current
    /// partial tail block to remain buffered until an explicit durability
    /// barrier (`flush`, `fsync`, size-changing ops, last-handle release).
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        self.write_with_local_sync_policy(ino, offset, data, LocalSyncPolicy::Opportunistic)
            .await
    }

    /// Write data with an explicit local synchronization policy.
    ///
    /// Allows callers to choose how strongly the caller wants the write materialized
    /// before completion while sharing the same write path as `write()`.
    pub async fn write_with_local_sync_policy(
        &self,
        ino: Ino,
        offset: u64,
        data: &[u8],
        local_sync_policy: LocalSyncPolicy,
    ) -> VfsResult<u32> {
        self.reject_private_store_ino(ino)?;
        let _sync_safety_activity = self.state.sync_safety_activity.begin();
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let result = async {
            self.enforce_access(ino, AccessLevel::ReadWrite)?;
            self.enforce_no_known_foreign_hard_lock(ino)?;
            let _ = self.state.read.observe_write_access(
                None,
                ino,
                offset,
                data.len(),
                self.access_tracker,
            );
            if self.is_synthesized_ignored_ino(ino) {
                return self.write_ignored_entry(ino, offset, data.len());
            }
            if data.is_empty() {
                return Ok(0);
            }
            if self.is_scratch_inode(ino)? {
                let accepted = self.record_scratch_writer_stack_payload(
                    0,
                    ino,
                    offset,
                    super::sync_ops::VfsWritePayload::Borrowed(data),
                )?;
                let materialized_bytes = self.materialize_writer_stack_for_inode(ino).await?;
                self.flush_local_dirty_state(ino, local_sync_policy).await?;
                if local_sync_policy.requires_write_path_durability() {
                    self.maybe_payload_store_sync_data_async(
                        materialized_bytes as u64,
                        local_sync_policy,
                    )
                    .await?;
                    self.sync_metadata_barrier().await?;
                }
                return Ok(accepted);
            }

            let mut base_attr = self.getattr_async(ino).await?;
            let end = offset
                .checked_add(data.len() as u64)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            let _quota = self.reserve_logical_growth(ino, base_attr.size, end)?;
            self.refresh_inode_incarnation_state_from_attr(ino, &base_attr);
            self.begin_pending_incarnation(ino, &mut base_attr);
            let (previous_dirty, submitted) = {
                // The newer-epoch dirty attr is what keeps a later Ready attr
                // from superseding these bytes before they are materialized.
                let _supersede_admission = self.state.writer_stack.supersede_admission(ino);
                let mut dirty = self.state.namespace.dirty_attrs.lock();
                let previous = dirty.get(&ino).cloned();
                let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
                attr.content_epoch = base_attr.content_epoch;
                attr.size = attr.size.max(end);
                attr.touch_mtime();
                attr.touch_ctime();
                self.update_pending_incarnation_size(ino, attr.size);
                (previous, attr.clone())
            };
            self.invalidate_attr_read_cache([ino]);

            let writes = [(offset, Bytes::copy_from_slice(data))];
            if let Err(error) = self
                .materialize_buffered_writes_direct(ino, &writes, local_sync_policy)
                .await
            {
                let _admission = self.state.writer_stack.supersede_admission(ino);
                let mut dirty = self.state.namespace.dirty_attrs.lock();
                if dirty.get(&ino) == Some(&submitted) {
                    let restored_size = previous_dirty
                        .as_ref()
                        .map_or(base_attr.size, |previous| previous.size);
                    match previous_dirty {
                        Some(previous) => {
                            dirty.insert(ino, previous);
                        }
                        None => {
                            dirty.remove(&ino);
                        }
                    }
                    self.update_pending_incarnation_size(ino, restored_size);
                }
                drop(dirty);
                drop(_admission);
                self.invalidate_attr_read_cache([ino]);
                return Err(error);
            }
            Ok(data.len() as u32)
        }
        .await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Flush all buffered blocks for a file.
    ///
    /// `write()` may have already coalesced and persisted some full blocks, but
    /// `flush()` is still the definitive "materialize everything that is left"
    /// barrier for the inode. In durable mode success means the inode crossed
    /// the strict local durable boundary; in fast mode success only means the
    /// inode reached the latest local unpublished state and upload/publish
    /// remain deferred background work. Platform bridges may call this directly
    /// or schedule it from their own background drain path.
    pub async fn flush_with_outcome(&self, ino: Ino) -> VfsResult<ExplicitFlushOutcome> {
        self.reject_private_store_ino(ino)?;
        self.flush_inode_barrier_with_outcome(ino, StickyErrorPolicy::Ignore)
            .await
    }

    /// Flush buffered data and wait for `flush_with_outcome` completion.
    ///
    /// Drops all detail and only reports success/failure of the flush attempt.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        self.flush_with_outcome(ino).await.map(|_| ())
    }

    /// Restore the retained inode and hardlinks using fresh durable local bodies.
    /// Source session gates order live I/O against the atomic payload/namespace
    /// handoff. Remote-only content must be materialized before this operation.
    /// `store` must belong to this VFS's own engine.
    pub async fn restore_retained_entry(
        &self,
        store: &dyn studiofs_meta::trash::MetaRetainedStore,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<studiofs_meta::trash::RetainedEntry> {
        // The copy and its cleanup reach the private receipt subtree through the
        // ordinary entrypoints; only this seam may do so.
        super::retained_views::trusted_retained_scope(
            self.restore_retained_entry_trusted(store, receipt, parent, name),
        )
        .await
    }

    /// Exclusive session gates on each source inode, held across the restore
    /// commit. Gates are taken in fence order; restore is the only multi-gate holder.
    async fn enter_restore_source_gates(
        &self,
        fences: &[studiofs_meta::trash::RetainedSourceFence],
    ) -> Vec<(studiofs_meta::Ino, super::file_session::FileSessionPermit)> {
        let mut gates = Vec::with_capacity(fences.len());
        for fence in fences {
            let permit = self
                .state
                .file_session
                .enter_op(fence.ino, FileSessionOperation::Restore)
                .await;
            gates.push((fence.ino, permit));
        }
        gates
    }

    fn release_restore_source_gates(
        &self,
        gates: Vec<(studiofs_meta::Ino, super::file_session::FileSessionPermit)>,
    ) {
        for (ino, permit) in gates {
            drop(permit);
            self.state.file_session.gc_if_idle(ino);
        }
    }

    async fn restore_retained_entry_trusted(
        &self,
        store: &dyn studiofs_meta::trash::MetaRetainedStore,
        receipt: Ino,
        parent: Ino,
        name: &[u8],
    ) -> VfsResult<studiofs_meta::trash::RetainedEntry> {
        // Serialize restore admissions; source permits below order all live I/O
        // against payload adoption and the namespace commit.
        // ponytail: one gate for all receipts; key it by receipt if concurrent
        // restores of distinct large files become a real workload.
        #[cfg(test)]
        self.run_retained_restore_hook_for_test(super::RetainedRestoreStage::Entered)
            .await;
        let _restore_gate = self.retained_restore_gate.lock().await;
        if self.inode_is_in_private_store(parent)? {
            return Err(VfsError::Meta(studiofs_meta::MetaError::InodeNotFound(
                parent,
            )));
        }
        self.enforce_public_access(parent, AccessLevel::ReadWrite)?;
        // The receipt is the stage parent; the current original-parent ACL
        // governs restore, so a missing or denied one keeps the entry retained.
        self.enforce_access(receipt, AccessLevel::ReadWrite)?;
        let entry = store.retained_entry(receipt)?;
        let retained = entry
            .as_ref()
            .is_some_and(|entry| entry.state == studiofs_meta::trash::RetainedEntryState::Retained);
        if let Some(entry) = entry.as_ref().filter(|_| retained) {
            self.enforce_public_access(entry.original_parent, AccessLevel::ReadWrite)?;
        }
        super::validate_name(name)?;
        self.reject_runtime_stats_entry_mutation(parent, name)?;
        if self.should_ignore_regular_file_path(parent, name)
            || self.should_ignore_directory_path(parent, name)
            || self.directory_has_probable_trash_ancestor(parent)?
        {
            self.reject_nontransactional_no_replace(parent, name)?;
            return Err(VfsError::OperationNotSupported);
        }
        if entry.is_some() && !retained {
            // A terminal receipt never changes the namespace: the store answers
            // an identical restore retry idempotently and refuses the rest. Its
            // payload has already left `receipt/data`.
            return Ok(store
                .restore_retained_entry(receipt, parent, name, b"stage", &[])?
                .entry);
        }
        self.wait_for_deferred_namespace_entry(receipt, b"data")
            .await?;
        self.wait_for_deferred_namespace_entry(parent, name).await?;
        let destination_free = match self.lookup(parent, name) {
            Ok(_) => false,
            Err(VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })) => true,
            Err(error) => return Err(error),
        };
        if !destination_free || !retained {
            return Ok(store
                .restore_retained_entry(receipt, parent, name, b"stage", &[])?
                .entry);
        }
        let owner = self.retained_restore_owner()?;
        let own_prefix = format!("restore-{owner:016x}-");
        for abandoned in self.sync_meta().readdir(receipt)? {
            if abandoned.name.starts_with(own_prefix.as_bytes()) {
                self.cleanup_retained_restore_stage(receipt, &abandoned.name)
                    .await;
            }
        }
        let root = self.sync_meta().lookup(receipt, b"data")?;
        let mut inodes = HashSet::new();
        let mut pending = vec![root.ino];
        while let Some(ino) = pending.pop() {
            if !inodes.insert(ino) {
                continue;
            }
            let attr = self.sync_meta().get_attr(ino)?;
            self.enforce_no_known_foreign_hard_lock(ino)?;
            if attr.kind == FileType::Directory {
                pending.extend(
                    self.sync_meta()
                        .readdir(ino)?
                        .into_iter()
                        .filter(|entry| entry.name != b"." && entry.name != b"..")
                        .map(|entry| entry.ino),
                );
            }
            self.wait_for_deferred_materialization_inflight(ino).await;
        }
        let mut fences: Vec<_> = inodes
            .iter()
            .map(|&ino| studiofs_meta::trash::RetainedSourceFence {
                ino,
                ctime: 0,
                size: 0,
                content_epoch: 0,
            })
            .collect();
        fences.sort_by_key(|fence| fence.ino);
        #[cfg(test)]
        self.run_retained_restore_hook_for_test(super::RetainedRestoreStage::BeforeSourceGates)
            .await;
        let gates = self.enter_restore_source_gates(&fences).await;
        let stage_name = format!("restore-{owner:016x}-{}", self.sync_meta().next_inode()?);
        let result = async {
            for fence in &fences {
                self.flush_retained_source_ordered(fence.ino).await?;
            }
            let root = self.sync_meta().get_attr(root.ino)?;
            let staged = self
                .copy_retained_entry_to_stage(receipt, stage_name.as_bytes(), &root)
                .await?;
            if staged.visited_inos.iter().copied().collect::<HashSet<_>>() != inodes {
                return Err(VfsError::Meta(studiofs_meta::MetaError::TargetChanged));
            }
            // Stage bodies and DIRTY upload intents precede the atomic metadata handoff.
            self.cached_store()
                .sync_data()
                .map_err(std::io::Error::other)?;
            let mutations = self.namespace_mutations();
            let restored = store.restore_retained_entry(
                receipt,
                parent,
                name,
                stage_name.as_bytes(),
                &staged.source_fences,
            )?;
            // The commit removed stage graphs; restart now maps DIRTY objects only
            // to the original inode/new epoch. No stage upload was ever submitted.
            for &ino in restored.upload_epochs.keys() {
                // Pre-restore uploads targeted superseded objects and earlier epochs.
                self.cancel_pending_uploads_for_inode(ino);
                self.remove_live_extents_for_inode(ino);
                self.remove_pending_epoch_finalizations_for_inode(ino);
                self.clear_upload_sticky_error(ino);
            }
            let mut live_extents = Vec::with_capacity(staged.uploads.len());
            let mut uploaded_inos = HashSet::new();
            for (ino, payload) in &staged.uploads {
                let epoch = restored.upload_epochs[ino];
                live_extents.push((
                    *ino,
                    block_index_from_offset(payload.offset, self.block_size),
                    payload.object_id,
                    epoch,
                ));
                uploaded_inos.insert(*ino);
                self.cached_store().submit_upload_with_payload(
                    *ino,
                    payload.offset,
                    Some(studiofs_cache::ObjectFileMapping::whole(
                        payload.offset,
                        payload.len as u64,
                    )),
                    payload.object_id,
                    epoch,
                    payload.len as u64,
                );
            }
            self.record_live_extents(&live_extents);
            for ino in uploaded_inos {
                self.state.file_session.note_background_transition(
                    ino,
                    FileSessionOperation::Upload,
                    "queued",
                );
            }
            mutations.record_rename(receipt, b"data", parent, name, root.ino);
            for &ino in &staged.visited_inos {
                if let Ok(attr) = self.sync_meta().get_attr(ino) {
                    self.refresh_inode_incarnation_state_from_attr(ino, &attr);
                }
                self.invalidate_attr_read_cache([ino]);
                self.handle_table.clear_read_windows_for_inode(ino);
            }
            self.bump_acl_namespace_epoch();
            self.invalidate_kernel_entries([
                (receipt, b"data".to_vec()),
                (receipt, stage_name.as_bytes().to_vec()),
                (parent, name.to_vec()),
            ]);
            self.sync_meta().request_eager_flush();
            Ok(restored.entry)
        }
        .await;
        self.release_restore_source_gates(gates);
        if result.is_err() {
            self.cleanup_retained_restore_stage(receipt, stage_name.as_bytes())
                .await;
        }
        if matches!(result, Err(VfsError::RemoteContentPending)) {
            // No source permit or metadata transaction may outlive this enqueue.
            let mut missing = HashSet::new();
            'enqueue: for ino in inodes {
                let Ok(attr) = self.sync_meta().get_attr(ino) else {
                    continue;
                };
                if attr.kind != FileType::RegularFile {
                    continue;
                }
                let Ok(spans) = self
                    .sync_meta()
                    .lookup_regular_file_slices(ino, 0, attr.size)
                else {
                    continue;
                };
                for span in spans {
                    if let Some(payload) = span.payload
                        && missing.insert(payload.object_id)
                        && self.cached_store().get(payload.object_id).is_none()
                        && !self.try_enqueue_prefetch_work_item(
                            super::prefetch::PrefetchWorkItem::RestorePayload(payload.object_id),
                        )
                    {
                        break 'enqueue;
                    }
                }
            }
        }
        result
    }

    async fn copy_retained_entry_to_stage(
        &self,
        receipt: Ino,
        stage_name: &[u8],
        root_src: &InodeAttr,
    ) -> VfsResult<StagedRetainedCopy> {
        let mut seen = HashSet::from([root_src.ino]);
        let mut visited_inos = vec![root_src.ino];
        let mut source_fences = Vec::new();
        let mut uploads = Vec::new();
        source_fences.push(studiofs_meta::trash::RetainedSourceFence {
            ino: root_src.ino,
            ctime: root_src.ctime,
            size: root_src.size,
            content_epoch: root_src.content_epoch,
        });
        let stage_root = match root_src.kind {
            FileType::RegularFile => {
                let dst = self.create(
                    receipt,
                    stage_name,
                    root_src.uid,
                    root_src.gid,
                    root_src.mode,
                )?;
                uploads.extend(
                    self.copy_retained_file_bytes(root_src.ino, dst.ino, root_src.size)
                        .await?
                        .into_iter()
                        .map(|payload| (root_src.ino, payload)),
                );
                self.restore_copied_times(dst.ino, root_src.atime, root_src.mtime)?;
                return Ok(StagedRetainedCopy {
                    visited_inos,

                    source_fences,
                    uploads,
                });
            }
            FileType::Symlink => {
                let target = self.readlink(root_src.ino)?;
                let dst = self.symlink(receipt, stage_name, &target, root_src.uid, root_src.gid)?;
                self.restore_copied_times(dst.ino, root_src.atime, root_src.mtime)?;
                return Ok(StagedRetainedCopy {
                    visited_inos,

                    source_fences,
                    uploads,
                });
            }
            FileType::Directory => self.mkdir(
                receipt,
                stage_name,
                root_src.uid,
                root_src.gid,
                root_src.mode,
            )?,
        };

        let mut dir_stack = vec![(root_src.ino, stage_root.ino)];
        // Directory times wait until the walk ends: each child creation moves its parent's mtime.
        let mut dir_times = vec![(stage_root.ino, root_src.atime, root_src.mtime)];
        let mut hardlinks: HashMap<Ino, Ino> = HashMap::new();
        while let Some((src_dir, dst_dir)) = dir_stack.pop() {
            let children = self.readdir_with_attrs_unguarded(src_dir)?;
            for child in children {
                if child.entry.name == b"." || child.entry.name == b".." {
                    continue;
                }
                let child_attr = match child.attr {
                    Some(attr) => attr,
                    None => self.getattr(child.entry.ino)?,
                };
                let child_attr = self.sync_meta().get_attr(child_attr.ino)?;
                let first_visit = seen.insert(child_attr.ino);
                if first_visit {
                    visited_inos.push(child_attr.ino);
                    source_fences.push(studiofs_meta::trash::RetainedSourceFence {
                        ino: child_attr.ino,
                        ctime: child_attr.ctime,
                        size: child_attr.size,
                        content_epoch: child_attr.content_epoch,
                    });
                }
                if child_attr.kind != FileType::Directory
                    && child_attr.nlink > 1
                    && let Some(&existing_dst_ino) = hardlinks.get(&child_attr.ino)
                {
                    self.link_inner(existing_dst_ino, dst_dir, &child.entry.name)
                        .await?;
                    continue;
                }
                match child_attr.kind {
                    FileType::Directory => {
                        let dst_sub = self.mkdir(
                            dst_dir,
                            &child.entry.name,
                            child_attr.uid,
                            child_attr.gid,
                            child_attr.mode,
                        )?;
                        dir_times.push((dst_sub.ino, child_attr.atime, child_attr.mtime));
                        dir_stack.push((child_attr.ino, dst_sub.ino));
                    }
                    FileType::RegularFile => {
                        let dst_file = self.create(
                            dst_dir,
                            &child.entry.name,
                            child_attr.uid,
                            child_attr.gid,
                            child_attr.mode,
                        )?;
                        if child_attr.nlink > 1 {
                            hardlinks.insert(child_attr.ino, dst_file.ino);
                        }
                        uploads.extend(
                            self.copy_retained_file_bytes(
                                child_attr.ino,
                                dst_file.ino,
                                child_attr.size,
                            )
                            .await?
                            .into_iter()
                            .map(|payload| (child_attr.ino, payload)),
                        );
                        self.restore_copied_times(
                            dst_file.ino,
                            child_attr.atime,
                            child_attr.mtime,
                        )?;
                    }
                    FileType::Symlink => {
                        let target = self.readlink(child_attr.ino)?;
                        let dst_link = self.symlink(
                            dst_dir,
                            &child.entry.name,
                            &target,
                            child_attr.uid,
                            child_attr.gid,
                        )?;
                        self.restore_copied_times(
                            dst_link.ino,
                            child_attr.atime,
                            child_attr.mtime,
                        )?;
                        if child_attr.nlink > 1 {
                            hardlinks.insert(child_attr.ino, dst_link.ino);
                        }
                    }
                }
            }
        }
        // Deepest first; parents are pushed before children, so reversing applies children first.
        for (ino, atime, mtime) in dir_times.into_iter().rev() {
            self.restore_copied_times(ino, atime, mtime)?;
        }
        Ok(StagedRetainedCopy {
            visited_inos,

            source_fences,
            uploads,
        })
    }

    /// Applies the source's atime and mtime to a freshly created stage node.
    fn restore_copied_times(&self, dst: Ino, atime: i64, mtime: i64) -> VfsResult<()> {
        self.setattr(
            dst,
            SetattrArgs {
                atime: TimeSpec::Set(atime),
                mtime: TimeSpec::Set(mtime),
                ..Default::default()
            },
        )?;
        Ok(())
    }

    async fn copy_retained_file_bytes(
        &self,
        src_ino: Ino,
        dst_ino: Ino,
        size: u64,
    ) -> VfsResult<Vec<studiofs_cache::StagedSlice>> {
        let mut offset = 0;
        let mut payloads = Vec::new();
        while offset < size {
            let len = (size - offset)
                .min(FOREGROUND_READ_WINDOW_BYTES as u64)
                .min(self.block_size() as u64) as u32;
            let ids: HashSet<_> = self
                .sync_meta()
                .lookup_regular_file_slices(src_ino, offset, u64::from(len))?
                .into_iter()
                .filter_map(|span| span.payload.map(|payload| payload.object_id))
                .collect();
            let backend = self.cached_store().local_payload_backend().as_ref();
            let pins = PayloadPins::new(backend, ids);
            let local = pins
                .ids
                .iter()
                .all(|&id| self.cached_store().get(id).is_some());
            let read = if local {
                self.read_ordered(src_ino, offset, len).await
            } else {
                Err(VfsError::RemoteContentPending)
            };
            drop(pins);
            let bytes = read?;
            if bytes.len() != len as usize {
                return Err(VfsError::Io(std::io::Error::other(
                    "incomplete retained payload",
                )));
            }
            #[cfg(test)]
            self.run_retained_restore_hook_for_test(super::RetainedRestoreStage::CopyRead)
                .await;
            let object_id = self.sync_meta().next_object_id()?;
            let mut writer =
                self.cached_store()
                    .open_writer_slice(studiofs_cache::CachedStoreWriterConfig {
                        ino: dst_ino,
                        base_offset: offset,
                        first_object_id: object_id,
                        content_epoch: 1,
                        block_size: self.block_size() as usize,
                        page_size: 4096,
                        dirty_byte_limit: None,
                        max_buffer_bytes: None,
                        submit_upload: false,
                    });
            writer
                .write_at(offset, &bytes)
                .map_err(std::io::Error::other)?;
            let staged = writer.finish().map_err(std::io::Error::other)?;
            let mut batch = studiofs_meta::schema::LocalSliceBatch::new(dst_ino);
            for payload in &staged {
                batch
                    .append_data_range(payload.offset, payload.len as u32, payload.object_id, 0)
                    .map_err(std::io::Error::other)?;
            }
            let mut attr = self.sync_meta().get_attr(dst_ino)?;
            // The source owns the logical EOF. Private stage slices carry only payload.
            attr.content_epoch = 1;
            self.sync_meta()
                .flush_regular_file_overlay_slices_with_attr(
                    dst_ino,
                    &batch.into_chunk_slices(),
                    Some(&attr),
                )?;
            payloads.extend(staged);
            offset += u64::from(len);
        }
        Ok(payloads)
    }

    async fn cleanup_retained_restore_stage(&self, receipt: Ino, stage_name: &[u8]) {
        let Ok(root) = self.sync_meta().lookup(receipt, stage_name) else {
            return;
        };
        let mut stack = vec![(receipt, stage_name.to_vec(), root.ino, root.kind, false)];
        while let Some((parent, name, ino, kind, visited)) = stack.pop() {
            if kind != FileType::Directory {
                let _ = self.unlink_inner(parent, &name).await;
                continue;
            }
            if visited {
                let _ = self.rmdir_inner(parent, &name).await;
                continue;
            }
            stack.push((parent, name, ino, kind, true));
            if let Ok(children) = self.sync_meta().readdir(ino) {
                for child in children {
                    if child.name == b"." || child.name == b".." {
                        continue;
                    }
                    stack.push((ino, child.name, child.ino, child.kind, false));
                }
            }
        }
    }

    /// This volume's restore-stage owner tag, persisted in a local-only counter.
    /// Peers draw their own random tags, so a stage carrying another tag is never ours.
    pub(super) fn retained_restore_owner(&self) -> VfsResult<u64> {
        let name = studiofs_meta::LOCAL_PG_RETAINED_RESTORE_OWNER_COUNTER;
        let meta = self.sync_meta();
        let owner = meta.get_counter(name)?;
        if owner != 0 {
            return Ok(owner);
        }
        let fresh = uuid::Uuid::new_v4().as_u64_pair().0.max(1);
        meta.set_counter(name, fresh)?;
        Ok(fresh)
    }

    #[cfg(test)]
    async fn run_retained_restore_hook_for_test(&self, stage: super::RetainedRestoreStage) {
        let hook = self.retained_restore_hook.lock().clone();
        if let Some(hook) = hook {
            hook(self, stage).await;
        }
    }
}

struct StagedRetainedCopy {
    visited_inos: Vec<Ino>,
    source_fences: Vec<studiofs_meta::trash::RetainedSourceFence>,
    uploads: Vec<(Ino, studiofs_cache::StagedSlice)>,
}

#[cfg(test)]
mod tests {
    use super::super::{OpenFile, OpenFlags, ReadAheadOwner};
    use super::direct_foreground_read_len;

    #[test]
    fn kernel_switch_after_oversized_window_prepare_uses_caller_buffer() {
        let open_file = OpenFile::new(7, 42, OpenFlags::RDONLY, None, false);
        open_file.record_read_progress(0, 128);
        let armed = open_file
            .arm_read_window_prefetch(128, 128, 4096)
            .expect("sequential VFS-owned read arms the speculative window");
        let prepared_read_len = 4096;
        let mut caller_buffer = [0_u8; 128];

        open_file.set_read_ahead_owner(ReadAheadOwner::Kernel);
        assert!(!open_file.is_read_ahead_token_current(armed));

        let direct_len = direct_foreground_read_len(prepared_read_len, caller_buffer.len());
        caller_buffer[..direct_len].fill(1);

        assert_eq!(direct_len, caller_buffer.len());
        assert!(caller_buffer.iter().all(|byte| *byte == 1));
    }
}
