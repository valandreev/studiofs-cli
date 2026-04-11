use super::{
    AccessLevel, BlockRef, Bytes, ExplicitFlushOutcome, FileSessionOperation, Ino, MetaEngine,
    StickyErrorPolicy, StudioVfs, VfsError, VfsResult, block_index_from_offset, debug, error,
    format_object_fragment_keys, offset_within_block, run_meta, warn,
};
use std::sync::Arc;

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

    pub(super) async fn read_into_impl(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
    ) -> VfsResult<u32> {
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
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
        Ok(written as u32)
    }

    pub(super) async fn fetch_block_for_ref(
        &self,
        block: &BlockRef,
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

    pub(super) fn copy_block_payload_into_range(
        &self,
        data: &[u8],
        block_offset: u32,
        dst: &mut [u8],
    ) {
        let src_start = block_offset as usize;
        if src_start >= data.len() {
            warn!(
                block_offset,
                data_len = data.len(),
                dst_len = dst.len(),
                "copy_block_payload_into_range: block_offset >= data.len(), zeroing dst (possible partial-block read bug)"
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
                "copy_block_payload_into_range: padding with zeros (partial block or short read)"
            );
            dst[copy_len..].fill(0);
        }
    }

    pub(super) fn copy_committed_block_payload_into_range(
        &self,
        data: &[u8],
        block: &BlockRef,
        file_offset: u64,
        dst: &mut [u8],
    ) -> VfsResult<()> {
        let src_start = usize::try_from(block.offset).map_err(|_| {
            VfsError::Other(format!(
                "committed block offset does not fit usize: object_id={}, offset={}",
                block.object_id, block.offset
            ))
        })?;
        let Some(src_end) = src_start.checked_add(dst.len()) else {
            return Err(VfsError::Other(format!(
                "committed block copy range overflow: object_id={}, offset={}, dst_len={}",
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
                "committed block payload shorter than requested span"
            );
            return Err(VfsError::Other(format!(
                "committed block payload too short: object_id={}, payload_len={}, required_end={}",
                block.object_id,
                data.len(),
                src_end
            )));
        }
        dst.copy_from_slice(&data[src_start..src_end]);
        Ok(())
    }

    pub(super) async fn get_file_block_async(
        &self,
        ino: Ino,
        block_index: u32,
    ) -> VfsResult<Option<BlockRef>> {
        let meta = self.meta_async.clone();
        run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_file_block(ino, block_index)
        })
        .await
    }

    /// Write data to a file at the given offset.
    ///
    /// Completed full blocks are coalesced into a larger eager-persist batch so
    /// `write()` avoids paying the local durability boundary for every single
    /// block while `flush()` still handles any remaining tail blocks.
    pub async fn write(&self, ino: Ino, offset: u64, data: &[u8]) -> VfsResult<u32> {
        let session = self
            .state
            .file_session
            .enter_op(ino, FileSessionOperation::Write)
            .await;
        let result = self.write_ordered(ino, offset, data).await;
        drop(session);
        self.state.file_session.gc_if_idle(ino);
        result
    }

    /// Flush all buffered blocks for a file.
    ///
    /// `write()` may have already coalesced and persisted some full blocks, but
    /// `flush()` is still the definitive "persist everything that is left"
    /// barrier for the inode. WinFSP file-scoped `Flush` relies on this
    /// remaining a mandatory dirty-inode barrier.
    pub async fn flush_with_outcome(&self, ino: Ino) -> VfsResult<ExplicitFlushOutcome> {
        self.flush_inode_barrier_with_outcome(ino, StickyErrorPolicy::Ignore)
            .await
    }

    pub async fn flush(&self, ino: Ino) -> VfsResult<()> {
        self.flush_with_outcome(ino).await.map(|_| ())
    }
}
