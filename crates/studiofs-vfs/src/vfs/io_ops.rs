use super::{
    AccessLevel, Bytes, ExplicitFlushOutcome, FileSessionOperation, Ino, LocalSyncPolicy,
    PayloadRef, StickyErrorPolicy, StudioVfs, VfsError, VfsResult, block_index_from_offset, debug,
    error, format_object_fragment_keys, offset_within_block, warn,
};

impl StudioVfs {
    /// Read file data at the given offset.
    pub async fn read(&self, ino: Ino, offset: u64, size: u32) -> VfsResult<Bytes> {
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
        if let Some(attr) = self.ignored_entry_attr(ino) {
            let end = (offset + size as u64).min(attr.size);
            let len = end.saturating_sub(offset) as usize;
            return Ok(Bytes::from(vec![0; len]));
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
            .execute_prepared_foreground_read(None, ino, offset, &mut buf, &prepared)
            .await?;
        buf.truncate(written as usize);
        Ok(Bytes::from(buf))
    }

    /// Read file data directly into a provided buffer (P1.3 — avoid intermediate Bytes allocation).
    ///
    /// Same as `read()` but writes directly to the output buffer, avoiding one memcpy.
    /// Returns the number of bytes written.
    pub async fn read_into(&self, ino: Ino, offset: u64, buffer: &mut [u8]) -> VfsResult<u32> {
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
        if let Some(attr) = self.ignored_entry_attr(ino) {
            let end = offset.saturating_add(buffer.len() as u64).min(attr.size);
            let len = end.saturating_sub(offset) as usize;
            if len > 0 {
                buffer[..len].fill(0);
            }
            return Ok(len as u32);
        }
        let prepared = self
            .prepare_foreground_read(ino, offset, buffer.len())
            .await?;
        if let Some(path) = &debug_path {
            tracing::info!(
                ino,
                fh = fh.unwrap_or_default(),
                path = %path,
                offset,
                requested_len = buffer.len(),
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
        let written = self
            .execute_prepared_foreground_read(fh, ino, offset, buffer, &prepared)
            .await?;
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
            if data.is_empty() {
                return Ok(0);
            }

            let mut base_attr = self.getattr_async(ino).await?;
            self.refresh_inode_incarnation_state_from_attr(ino, &base_attr);
            self.begin_pending_incarnation(ino, &mut base_attr);
            {
                let mut dirty = self.state.namespace.dirty_attrs.lock();
                let attr = dirty.entry(ino).or_insert_with(|| base_attr.clone());
                attr.content_epoch = base_attr.content_epoch;
                let new_size = offset.saturating_add(data.len() as u64);
                if new_size > attr.size {
                    attr.size = new_size;
                }
                attr.touch_mtime();
                attr.touch_ctime();
                self.update_pending_incarnation_size(ino, attr.size);
            }
            self.invalidate_attr_read_cache([ino]);

            let writes = [(offset, Bytes::copy_from_slice(data))];
            self.materialize_buffered_writes_direct(ino, &writes, local_sync_policy)
                .await?;
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
        self.flush_inode_barrier_with_outcome(ino, StickyErrorPolicy::Ignore)
            .await
    }

    /// Flush buffered data and wait for `flush_with_outcome` completion.
    ///
    /// Drops all detail and only reports success/failure of the flush attempt.
    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        self.flush_with_outcome(ino).await.map(|_| ())
    }
}
