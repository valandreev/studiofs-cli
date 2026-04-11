use super::{
    ATTR_CACHE_TTL, AccessLevel, ForegroundReadScope, PrefetchBatch, PrefetchReadContext,
    ReadRequestContext, StudioVfs, VfsError, VfsResult,
};
use crate::runtime::run_meta;
use bytes::Bytes;
use futures::future::join_all;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Instant;
use studiofs_meta::schema::{
    DEFAULT_CHUNK_SIZE, ResolvedSliceSource, ResolvedSliceSpan, chunk_start_offset,
};
use studiofs_meta::types::{BlockRef, Ino, InodeAttr, block_index_from_offset};
use studiofs_meta::{MetaEngine, MetaError};
use tracing::{debug, error, warn};

#[derive(Debug, Clone, Copy)]
struct ReadRange {
    start: u64,
    end: u64,
}

impl ReadRange {
    fn intersection(self, other: Self) -> Option<Self> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then_some(Self { start, end })
    }
}

#[derive(Debug, Clone)]
enum ReadPlanSource {
    Dirty { data: Bytes, block_offset: u32 },
    PendingOverlay { data: Bytes, block_offset: u32 },
    Overlay { block: BlockRef },
    Base { block: BlockRef },
    Zero,
}

#[derive(Debug, Clone)]
struct ReadPlanSpan {
    range: ReadRange,
    source: ReadPlanSource,
}

struct ReadPipeline<'a> {
    vfs: &'a StudioVfs,
    ino: Ino,
    offset: u64,
    length: u64,
    pending_content_epoch: Option<u64>,
    resolved_regular_file_slices: Option<&'a [ResolvedSliceSpan]>,
}

impl<'a> ReadPipeline<'a> {
    fn new(
        vfs: &'a StudioVfs,
        ino: Ino,
        offset: u64,
        length: u64,
        pending_content_epoch: Option<u64>,
        resolved_regular_file_slices: Option<&'a [ResolvedSliceSpan]>,
    ) -> Self {
        Self {
            vfs,
            ino,
            offset,
            length,
            pending_content_epoch,
            resolved_regular_file_slices,
        }
    }

    async fn execute_into(&self, buffer: &mut [u8]) -> VfsResult<usize> {
        let mut plan = self.build_plan().await?;
        plan.sort_by_key(|span| span.range.start);
        let missing_remote = Self::prefetch_remote_blocks(self.vfs, &plan, false).await?;
        let written =
            Self::assemble_into(self.vfs, self.offset, &plan, &missing_remote, buffer).await?;
        drop(plan);
        Ok(written)
    }

    async fn prefetch_only(&self, prefetch: bool) -> VfsResult<()> {
        let mut plan = self.build_plan().await?;
        plan.sort_by_key(|span| span.range.start);
        let _ = Self::prefetch_remote_blocks(self.vfs, &plan, prefetch).await?;
        Ok(())
    }

    async fn build_plan(&self) -> VfsResult<Vec<ReadPlanSpan>> {
        Self::build_read_plan_with(
            self.vfs,
            self.ino,
            self.offset,
            self.length,
            self.pending_content_epoch,
            self.resolved_regular_file_slices,
        )
        .await
    }

    async fn build_read_plan_with(
        vfs: &StudioVfs,
        ino: Ino,
        offset: u64,
        length: u64,
        pending_content_epoch: Option<u64>,
        resolved_regular_file_slices: Option<&[ResolvedSliceSpan]>,
    ) -> VfsResult<Vec<ReadPlanSpan>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let Some(read_end) = offset.checked_add(length) else {
            return Err(VfsError::Other(format!(
                "read range overflow: ino={ino}, offset={offset}, length={length}"
            )));
        };
        let requested = ReadRange {
            start: offset,
            end: read_end,
        };
        let mut unresolved = vec![requested];
        let mut plan = Vec::new();
        let pending_upload_index = vfs.snapshot_pending_upload_index();

        let dirty_spans = vfs.snapshot_pending_dirty_slice_read_spans(ino, offset, length);
        for dirty in dirty_spans {
            if unresolved.is_empty() {
                break;
            }
            let dirty_range = ReadRange {
                start: dirty.start,
                end: dirty.end,
            };
            let (next_unresolved, claimed) = Self::claim_unresolved_ranges(unresolved, dirty_range);
            unresolved = next_unresolved;
            for overlap in claimed {
                let delta = overlap.start.saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                plan.push(ReadPlanSpan {
                    range: overlap,
                    source: ReadPlanSource::Dirty {
                        data: dirty.data.clone(),
                        block_offset,
                    },
                });
            }
        }

        if !unresolved.is_empty() {
            let mut candidate_blocks = BTreeSet::new();
            for range in &unresolved {
                let start_block = block_index_from_offset(range.start, vfs.block_size);
                let end_block =
                    block_index_from_offset(range.end.saturating_sub(1), vfs.block_size);
                for block_index in start_block..=end_block {
                    candidate_blocks.insert(block_index);
                }
            }
            for block_index in candidate_blocks {
                if unresolved.is_empty() {
                    break;
                }
                let Some(data) = vfs
                    .read_pending_overlay_block_async(
                        ino,
                        block_index,
                        pending_content_epoch,
                        Some(&pending_upload_index),
                    )
                    .await?
                else {
                    continue;
                };
                let block_start = (block_index as u64).saturating_mul(vfs.block_size as u64);
                let block_end = block_start.saturating_add(vfs.block_size as u64);
                let (next_unresolved, claimed) = Self::claim_unresolved_ranges(
                    unresolved,
                    ReadRange {
                        start: block_start,
                        end: block_end,
                    },
                );
                unresolved = next_unresolved;
                for overlap in claimed {
                    let delta = overlap.start.saturating_sub(block_start);
                    let Some(block_offset) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    plan.push(ReadPlanSpan {
                        range: overlap,
                        source: ReadPlanSource::PendingOverlay {
                            data: data.clone(),
                            block_offset,
                        },
                    });
                }
            }
        }

        if !unresolved.is_empty() {
            if let Some(resolved) = resolved_regular_file_slices {
                unresolved = Self::claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |block| ReadPlanSource::Overlay { block },
                );
                unresolved = Self::claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Base,
                    |block| ReadPlanSource::Base { block },
                );
            } else {
                let meta = vfs.meta_async.clone();
                let resolved = run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
                    engine.lookup_regular_file_slices(ino, offset, length)
                })
                .await?;
                unresolved = Self::claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |block| ReadPlanSource::Overlay { block },
                );
                unresolved = Self::claim_meta_spans_into_plan(
                    resolved.iter(),
                    unresolved,
                    &mut plan,
                    requested,
                    ResolvedSliceSource::Base,
                    |block| ReadPlanSource::Base { block },
                );
            }
        }

        for gap in unresolved {
            plan.push(ReadPlanSpan {
                range: gap,
                source: ReadPlanSource::Zero,
            });
        }
        Ok(plan)
    }

    fn claim_unresolved_ranges(
        unresolved: Vec<ReadRange>,
        candidate: ReadRange,
    ) -> (Vec<ReadRange>, Vec<ReadRange>) {
        let mut next_unresolved = Vec::new();
        let mut claimed = Vec::new();
        for range in unresolved {
            let Some(overlap) = range.intersection(candidate) else {
                next_unresolved.push(range);
                continue;
            };
            if range.start < overlap.start {
                next_unresolved.push(ReadRange {
                    start: range.start,
                    end: overlap.start,
                });
            }
            claimed.push(overlap);
            if overlap.end < range.end {
                next_unresolved.push(ReadRange {
                    start: overlap.end,
                    end: range.end,
                });
            }
        }
        (next_unresolved, claimed)
    }

    fn claim_meta_spans_into_plan<'b, I, F>(
        resolved: I,
        unresolved: Vec<ReadRange>,
        plan: &mut Vec<ReadPlanSpan>,
        requested: ReadRange,
        source_filter: ResolvedSliceSource,
        source_ctor: F,
    ) -> Vec<ReadRange>
    where
        I: Iterator<Item = &'b ResolvedSliceSpan>,
        F: Fn(BlockRef) -> ReadPlanSource + Copy,
    {
        let mut unresolved = unresolved;
        for span in resolved {
            if unresolved.is_empty() {
                break;
            }
            if span.source != source_filter {
                continue;
            }
            let Some(block) = span.block.as_ref() else {
                continue;
            };
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let span_range = ReadRange {
                start: chunk_base.saturating_add(span.range.start as u64),
                end: chunk_base.saturating_add(span.range.end as u64),
            };
            let Some(clipped) = span_range.intersection(requested) else {
                continue;
            };
            let (next_unresolved, claimed) = Self::claim_unresolved_ranges(unresolved, clipped);
            unresolved = next_unresolved;
            for overlap in claimed {
                let delta = overlap.start.saturating_sub(span_range.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = block.offset.checked_add(delta_u32) else {
                    continue;
                };
                let length = overlap.end.saturating_sub(overlap.start);
                let Some(block_length) = u32::try_from(length).ok() else {
                    continue;
                };
                let mut planned_block = block.clone();
                planned_block.offset = block_offset;
                planned_block.length = block_length;
                plan.push(ReadPlanSpan {
                    range: overlap,
                    source: source_ctor(planned_block),
                });
            }
        }
        unresolved
    }

    fn collect_data_block_refs(plan: &[ReadPlanSpan]) -> BTreeMap<u64, BlockRef> {
        let mut block_refs = BTreeMap::new();
        for span in plan {
            let block = match &span.source {
                ReadPlanSource::Overlay { block } | ReadPlanSource::Base { block } => block,
                ReadPlanSource::Dirty { .. }
                | ReadPlanSource::PendingOverlay { .. }
                | ReadPlanSource::Zero => continue,
            };
            block_refs
                .entry(block.object_id)
                .or_insert_with(|| block.clone());
        }
        block_refs
    }

    async fn prefetch_remote_blocks(
        vfs: &StudioVfs,
        plan: &[ReadPlanSpan],
        prefetch: bool,
    ) -> VfsResult<HashSet<u64>> {
        let mut cached = BTreeSet::new();
        let mut missing = BTreeMap::new();
        for (object_id, block) in Self::collect_data_block_refs(plan) {
            if vfs.local_block_backend.contains(object_id) {
                cached.insert(object_id);
            } else {
                missing.insert(object_id, block);
            }
        }
        for object_id in cached {
            vfs.local_block_backend.advise_will_need(object_id);
        }
        if missing.is_empty() {
            return Ok(HashSet::new());
        }
        let fetches = missing.into_iter().map(|(object_id, block)| async move {
            match vfs.fetch_block_for_ref(&block, prefetch).await {
                Ok(_) => Ok::<(u64, bool), VfsError>((object_id, false)),
                Err(crate::fetch::FetchError::NotFound { ref key }) => {
                    warn!(
                        object_id,
                        key,
                        block_offset = block.offset,
                        block_length = block.length,
                        "prefetch_remote_blocks_for_plan: S3 object not found during read prefetch — object missing from S3 (upload not complete or evicted?)"
                    );
                    Ok::<(u64, bool), VfsError>((object_id, true))
                }
                Err(error) => {
                    debug!(
                        object_id,
                        error = %error,
                        "read planner prefetch failed; falling back to on-demand resolution"
                    );
                    Ok::<(u64, bool), VfsError>((object_id, false))
                }
            }
        });
        let mut not_found = HashSet::new();
        for result in join_all(fetches).await {
            let (object_id, was_not_found) = result?;
            if was_not_found {
                not_found.insert(object_id);
            }
        }
        Ok(not_found)
    }

    async fn assemble_into(
        vfs: &StudioVfs,
        offset: u64,
        plan: &[ReadPlanSpan],
        remote_not_found: &HashSet<u64>,
        buffer: &mut [u8],
    ) -> VfsResult<usize> {
        let mut written = 0usize;
        for span in plan {
            let dst_start = span.range.start.saturating_sub(offset) as usize;
            let dst_end = span.range.end.saturating_sub(offset) as usize;
            if dst_start >= buffer.len() || dst_end > buffer.len() || dst_start >= dst_end {
                continue;
            }
            let dst = &mut buffer[dst_start..dst_end];
            match &span.source {
                ReadPlanSource::Dirty { data, block_offset } => {
                    vfs.copy_block_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadPlanSource::PendingOverlay { data, block_offset } => {
                    vfs.copy_block_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadPlanSource::Overlay { block } | ReadPlanSource::Base { block } => {
                    if remote_not_found.contains(&block.object_id) {
                        warn!(
                            object_id = block.object_id,
                            file_offset = span.range.start,
                            "assemble_read_plan: object in remote_not_found set, zeroing range (S3 NotFound during prefetch)"
                        );
                        dst.fill(0);
                    } else {
                        let block_offset = usize::try_from(block.offset).map_err(|_| {
                            VfsError::Other(format!(
                                "block offset does not fit usize: object_id={}, offset={}",
                                block.object_id, block.offset
                            ))
                        })?;
                        let mut span_resolved = false;
                        if let Some(read) = vfs
                            .block_store_read_into_async(block.object_id, block_offset, dst)
                            .await?
                        {
                            if read == dst.len() {
                                span_resolved = true;
                            }
                            if !span_resolved {
                                error!(
                                    object_id = block.object_id,
                                    file_offset = span.range.start,
                                    block_offset = block.offset,
                                    requested = dst.len(),
                                    read,
                                    "assemble_read_plan: short local backend read for committed block; evicting and refetching"
                                );
                                dst.fill(0);
                                if let Err(evict_error) =
                                    vfs.block_store_remove_async(block.object_id).await
                                {
                                    warn!(
                                        object_id = block.object_id,
                                        error = %evict_error,
                                        "assemble_read_plan: failed to evict short-read local block before refetch"
                                    );
                                }
                            }
                        }

                        if !span_resolved {
                            match vfs.fetch_block_for_ref(block, false).await {
                                Ok(data) => {
                                    vfs.copy_committed_block_payload_into_range(
                                        data.as_ref(),
                                        block,
                                        span.range.start,
                                        dst,
                                    )?;
                                }
                                Err(crate::fetch::FetchError::NotFound { key }) => {
                                    warn!(
                                        object_id = block.object_id,
                                        key,
                                        file_offset = span.range.start,
                                        block_offset = block.offset,
                                        block_length = block.length,
                                        "assemble_read_plan: S3 object not found during foreground read, zeroing range (upload incomplete or evicted before upload?)"
                                    );
                                    dst.fill(0);
                                }
                                Err(error) => {
                                    return Err(VfsError::Fetch(error));
                                }
                            }
                        }
                    }
                }
                ReadPlanSource::Zero => dst.fill(0),
            }
            written = written.max(dst_end);
        }
        Ok(written)
    }
}

struct ReadMetadata {
    attr: InodeAttr,
    resolved_slices: Vec<ResolvedSliceSpan>,
}

pub(super) struct PreparedForegroundRead {
    pub(super) context: ReadRequestContext,
    pub(super) resolved_slices: Vec<ResolvedSliceSpan>,
    pub(super) read_len: usize,
}

struct ReadMetadataResolver<'a> {
    vfs: &'a StudioVfs,
    ino: Ino,
}

impl<'a> ReadMetadataResolver<'a> {
    fn new(vfs: &'a StudioVfs, ino: Ino) -> Self {
        Self { vfs, ino }
    }

    fn resolve_attr_sync(&self) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.ignored_attr() {
            return Ok(attr);
        }
        if let Some(attr) = self.dirty_attr() {
            return Ok(attr);
        }
        if let Some(attr) = self.cached_attr() {
            return Ok(attr);
        }
        self.load_attr_from_meta_sync()
    }

    async fn resolve_attr(&self) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.ignored_attr() {
            return Ok(attr);
        }
        if let Some(attr) = self.dirty_attr() {
            return Ok(attr);
        }
        if let Some(attr) = self.cached_attr() {
            return Ok(attr);
        }
        self.load_attr_from_meta().await
    }

    async fn resolve_for_read(&self, offset: u64, buffer_len: u64) -> VfsResult<ReadMetadata> {
        if let Some(attr) = self.ignored_attr() {
            return Ok(ReadMetadata {
                attr,
                resolved_slices: Vec::new(),
            });
        }
        if let Some(attr) = self.dirty_attr() {
            let resolved_slices = self
                .resolve_slices_for_attr(&attr, offset, buffer_len)
                .await?;
            return Ok(ReadMetadata {
                attr,
                resolved_slices,
            });
        }
        if let Some(attr) = self.cached_attr() {
            let resolved_slices = self
                .resolve_slices_for_attr(&attr, offset, buffer_len)
                .await?;
            return Ok(ReadMetadata {
                attr,
                resolved_slices,
            });
        }
        self.load_attr_and_slices_from_meta(offset, buffer_len)
            .await
    }

    fn ignored_attr(&self) -> Option<InodeAttr> {
        self.vfs.ignored_entry_attr(self.ino)
    }

    fn dirty_attr(&self) -> Option<InodeAttr> {
        self.vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&self.ino)
            .cloned()
    }

    fn cached_attr(&self) -> Option<InodeAttr> {
        self.vfs
            .state
            .namespace
            .attr_read_cache
            .lock()
            .get(self.ino, Instant::now())
    }

    fn cache_attr(&self, attr: &InodeAttr) {
        self.vfs.state.namespace.attr_read_cache.lock().put(
            self.ino,
            attr.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );
    }

    fn clear_stale_state(&self) {
        self.vfs.clear_pending_blocks_for_inode(self.ino);
        self.vfs.clear_inode_incarnation(self.ino);
        self.vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .remove(&self.ino);
        self.vfs
            .state
            .namespace
            .attr_read_cache
            .lock()
            .remove(self.ino);
        let _ = self.vfs.handle_table.take_orphan(self.ino);
    }

    fn stale_inode_not_found(&self) -> VfsError {
        self.clear_stale_state();
        VfsError::Meta(MetaError::InodeNotFound(self.ino))
    }

    fn read_len_for_attr(&self, attr: &InodeAttr, offset: u64, buffer_len: u64) -> u64 {
        attr.size.saturating_sub(offset).min(buffer_len)
    }

    async fn resolve_slices_for_attr(
        &self,
        attr: &InodeAttr,
        offset: u64,
        buffer_len: u64,
    ) -> VfsResult<Vec<ResolvedSliceSpan>> {
        let read_len = self.read_len_for_attr(attr, offset, buffer_len);
        if read_len == 0 {
            return Ok(Vec::new());
        }
        let ino = self.ino;
        let meta = self.vfs.meta_async.clone();
        match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.lookup_regular_file_slices(ino, offset, read_len)
        })
        .await
        {
            Ok(resolved) => Ok(resolved),
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => Err(self.stale_inode_not_found()),
            Err(err) => Err(err),
        }
    }

    async fn load_attr_from_meta(&self) -> VfsResult<InodeAttr> {
        let ino = self.ino;
        let meta = self.vfs.meta_async.clone();
        let attr = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            engine.get_attr(ino)
        })
        .await
        {
            Ok(attr) => attr,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                return Err(self.stale_inode_not_found());
            }
            Err(err) => return Err(err),
        };
        self.cache_attr(&attr);
        Ok(attr)
    }

    fn load_attr_from_meta_sync(&self) -> VfsResult<InodeAttr> {
        let attr = match self.vfs.sync_meta().get_attr(self.ino) {
            Ok(attr) => attr,
            Err(MetaError::InodeNotFound(_)) => return Err(self.stale_inode_not_found()),
            Err(err) => return Err(VfsError::Meta(err)),
        };
        self.cache_attr(&attr);
        Ok(attr)
    }

    async fn load_attr_and_slices_from_meta(
        &self,
        offset: u64,
        buffer_len: u64,
    ) -> VfsResult<ReadMetadata> {
        let ino = self.ino;
        let meta = self.vfs.meta_async.clone();
        let (attr, resolved_slices) = match run_meta(meta, move |engine: Arc<dyn MetaEngine>| {
            let attr = engine.get_attr(ino)?;
            let read_len = attr.size.saturating_sub(offset).min(buffer_len);
            let resolved_slices = if read_len == 0 {
                Vec::new()
            } else {
                engine.lookup_regular_file_slices(ino, offset, read_len)?
            };
            Ok((attr, resolved_slices))
        })
        .await
        {
            Ok(result) => result,
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                return Err(self.stale_inode_not_found());
            }
            Err(err) => return Err(err),
        };
        self.cache_attr(&attr);
        Ok(ReadMetadata {
            attr,
            resolved_slices,
        })
    }
}

impl StudioVfs {
    pub(super) fn getattr_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        ReadMetadataResolver::new(self, ino).resolve_attr_sync()
    }

    pub(super) async fn getattr_async_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        ReadMetadataResolver::new(self, ino).resolve_attr().await
    }

    pub(super) async fn resolve_read_metadata_async(
        &self,
        ino: Ino,
        offset: u64,
        buffer_len: u64,
    ) -> VfsResult<(InodeAttr, Vec<ResolvedSliceSpan>)> {
        let resolved = ReadMetadataResolver::new(self, ino)
            .resolve_for_read(offset, buffer_len)
            .await?;
        Ok((resolved.attr, resolved.resolved_slices))
    }

    pub(super) async fn prepare_foreground_read(
        &self,
        ino: Ino,
        offset: u64,
        requested_len: usize,
    ) -> VfsResult<PreparedForegroundRead> {
        let (attr, resolved_slices) = self
            .resolve_read_metadata_async(ino, offset, requested_len as u64)
            .await?;
        let context = ReadRequestContext::from_attr(self, ino, attr);
        let read_len = context.read_len(offset, requested_len);
        Ok(PreparedForegroundRead {
            context,
            resolved_slices,
            read_len,
        })
    }

    pub(super) async fn execute_prepared_foreground_read(
        &self,
        fh: Option<u64>,
        ino: Ino,
        offset: u64,
        buffer: &mut [u8],
        prepared: &PreparedForegroundRead,
    ) -> VfsResult<usize> {
        debug_assert!(prepared.read_len > 0);
        let blocks_needed =
            self.calculate_blocks_needed(offset, prepared.read_len, prepared.context.attr.size);
        let open_file = fh.and_then(|handle| self.handle_table.get(handle));
        let access = self.state.read.observe_read_access(
            open_file.as_ref(),
            ino,
            offset,
            buffer.len(),
            self.access_classifier,
        );
        let mut guard = ForegroundReadScope::new(
            &self.state.read,
            self.upload_fg_read_latency_window,
            access.policy,
        );
        let read_pipeline = ReadPipeline::new(
            self,
            ino,
            offset,
            prepared.read_len as u64,
            prepared.context.pending_content_epoch,
            Some(prepared.resolved_slices.as_slice()),
        );
        let written = read_pipeline.execute_into(buffer).await?;

        self.enqueue_prefetch_hint(ino, access.stream_id, access.policy, &blocks_needed);
        guard.record_bytes(written as u64);
        Ok(written)
    }

    pub(super) async fn resolve_prefetch_batch_context(
        &self,
        ino: Ino,
        read_context: Option<PrefetchReadContext>,
    ) -> VfsResult<Option<PrefetchReadContext>> {
        if let Some(read_context) = read_context {
            return Ok(Some(read_context));
        }
        let attr = self.getattr_async(ino).await?;
        let read_context = ReadRequestContext::from_attr(self, ino, attr);
        if read_context.placeholder_visible(self, ino) {
            return Ok(None);
        }
        Ok(Some(read_context.prefetch_context()))
    }

    pub(super) async fn read_prefetch_batch(&self, batch: PrefetchBatch) {
        let PrefetchBatch {
            ino,
            mut block_indices,
            read_context,
        } = batch;
        if self.shutdown_requested() {
            self.finish_prefetch_blocks(ino, &block_indices);
            return;
        }

        let result = async {
            block_indices.retain(|block_idx| self.readahead.is_prefetch_relevant(ino, *block_idx));
            if block_indices.is_empty() {
                return Ok(());
            }

            let Some(context) = self
                .resolve_prefetch_batch_context(ino, read_context)
                .await?
            else {
                return Ok(());
            };
            if context.size == 0 {
                return Ok(());
            }
            let Some(eof_block) = context.eof_block(self.block_size) else {
                return Ok(());
            };
            block_indices.retain(|block_idx| *block_idx <= eof_block);
            if block_indices.is_empty() {
                return Ok(());
            }

            for (first_block, last_block) in
                Self::contiguous_prefetch_block_ranges(&mut block_indices)
            {
                let block_start = (first_block as u64).saturating_mul(self.block_size as u64);
                let block_end = ((last_block as u64).saturating_add(1))
                    .saturating_mul(self.block_size as u64)
                    .min(context.size);
                let block_len = block_end.saturating_sub(block_start);
                if block_len == 0 {
                    continue;
                }
                let read_pipeline = ReadPipeline::new(
                    self,
                    ino,
                    block_start,
                    block_len,
                    context.pending_content_epoch,
                    None,
                );
                read_pipeline.prefetch_only(true).await?;
            }
            Ok::<(), VfsError>(())
        }
        .await;
        if let Err(error) = result {
            debug!(
                ino,
                block_indices = ?block_indices,
                error = %error,
                "prefetch worker resolver path failed"
            );
        }
        self.finish_prefetch_blocks(ino, &block_indices);
    }

    fn finish_prefetch_blocks(&self, ino: Ino, block_indices: &[u32]) {
        for block_index in block_indices {
            self.readahead.finish_prefetch(ino, *block_index);
        }
    }

    pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        let attr = self.getattr_unchecked(ino)?;
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        Ok(attr)
    }

    pub(super) async fn getattr_async(&self, ino: Ino) -> VfsResult<InodeAttr> {
        let attr = self.getattr_async_unchecked(ino).await?;
        self.enforce_access_async(ino, AccessLevel::ReadOnly)
            .await?;
        Ok(attr)
    }
}

#[cfg(test)]
mod tests {
    use super::super::write_buffer::PendingBlock;
    use super::super::*;
    use super::{ReadPipeline, ReadPlanSource, ReadPlanSpan, ReadRange};
    use bytes::{Bytes, BytesMut};
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::Ordering as AtomicOrdering;
    use std::time::Instant;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig, ROOT_INO};
    use studiofs_meta::{MetaBlocks, MetaFormat, MetaInodes, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("read-path-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("read-path-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            block_size: 4 * 1024 * 1024,
        }
    }

    fn make_unstarted_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta.clone(),
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        (rt, vfs, meta)
    }

    fn make_test_vfs(
        tmp: &TempDir,
        volume_suffix: &str,
    ) -> (Arc<Runtime>, Arc<StudioVfs>, Arc<RedbMetaEngine>) {
        let (rt, vfs, meta) = make_unstarted_test_vfs(tmp, volume_suffix);
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
        (rt, vfs, meta)
    }

    #[test]
    fn collect_data_block_refs_deduplicates_by_object_id() {
        let first = BlockRef {
            object_id: 42,
            offset: 0,
            length: 16,
            flags: MetaBlockFlags::empty(),
        };
        let second = BlockRef {
            object_id: 42,
            offset: 8,
            length: 8,
            flags: MetaBlockFlags::PARTIAL_BLOCK,
        };
        let plan = vec![
            ReadPlanSpan {
                range: ReadRange { start: 0, end: 16 },
                source: ReadPlanSource::Base {
                    block: first.clone(),
                },
            },
            ReadPlanSpan {
                range: ReadRange { start: 16, end: 24 },
                source: ReadPlanSource::Overlay {
                    block: second.clone(),
                },
            },
            ReadPlanSpan {
                range: ReadRange { start: 24, end: 32 },
                source: ReadPlanSource::Zero,
            },
        ];

        let refs = ReadPipeline::collect_data_block_refs(&plan);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs.get(&42), Some(&first));
    }

    #[test]
    fn resolve_read_metadata_async_prefers_dirty_attr_over_cached_attr() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "read-metadata-dirty-over-cache");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"dirty-over-cache.txt", 1000, 1000, 0o644)
                .expect("create file");
            let ino = created.ino;

            let mut cached_attr = meta.get_attr(ino).expect("load cached attr baseline");
            cached_attr.size = 1;
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                cached_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );

            let mut dirty_attr = meta.get_attr(ino).expect("load dirty attr baseline");
            dirty_attr.size = 5;
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, dirty_attr);

            let (resolved_attr, resolved_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 0)
                .await
                .expect("resolve read metadata");

            assert_eq!(resolved_attr.size, 5);
            assert!(resolved_slices.is_empty());
        });
    }

    #[test]
    fn getattr_unchecked_prefers_dirty_attr_over_cached_attr() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "getattr-dirty-over-cache");
        let created = vfs
            .create(ROOT_INO, b"getattr-dirty-over-cache.txt", 1000, 1000, 0o644)
            .expect("create file");
        let ino = created.ino;

        let mut cached_attr = meta.get_attr(ino).expect("load cached attr baseline");
        cached_attr.size = 1;
        vfs.state.namespace.attr_read_cache.lock().put(
            ino,
            cached_attr,
            ATTR_CACHE_TTL,
            Instant::now(),
        );

        let mut dirty_attr = meta.get_attr(ino).expect("load dirty attr baseline");
        dirty_attr.size = 7;
        vfs.state
            .namespace
            .dirty_attrs
            .lock()
            .insert(ino, dirty_attr);

        let resolved_attr = vfs.getattr_unchecked(ino).expect("getattr_unchecked");
        assert_eq!(resolved_attr.size, 7);
    }

    #[test]
    fn resolve_read_metadata_async_clears_stale_runtime_state_when_inode_is_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "read-metadata-stale-cache-missing-ino");
        let _guard = rt.enter();
        rt.block_on(async {
            let ino = 9_001;
            let mut cached_attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
            cached_attr.size = 5;
            vfs.state.write_buffer.pending_blocks.lock().insert(
                (ino, 0),
                PendingBlock {
                    data: BytesMut::from(&b"stale"[..]),
                    dirty: true,
                    reserved_object_id: None,
                },
            );
            vfs.state
                .write_buffer
                .pending_block_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(0);
            vfs.state
                .write_buffer
                .pending_bytes
                .fetch_add(5, AtomicOrdering::Relaxed);
            vfs.refresh_inode_incarnation_state_from_attr(ino, &cached_attr);
            vfs.handle_table.mark_orphan(ino);

            let err = vfs
                .resolve_read_metadata_async(ino, 0, 16)
                .await
                .expect_err("missing inode should fail");

            assert!(matches!(
                err,
                VfsError::Meta(MetaError::InodeNotFound(found)) if found == ino
            ));
            assert!(!vfs.state.namespace.attr_read_cache.lock().contains(ino));
            assert!(!vfs.state.namespace.dirty_attrs.lock().contains_key(&ino));
            assert!(
                !vfs.state
                    .write_buffer
                    .pending_blocks
                    .lock()
                    .contains_key(&(ino, 0))
            );
            assert!(
                !vfs.state
                    .incarnation
                    .inode_incarnations
                    .lock()
                    .contains_key(&ino)
            );
            assert!(!vfs.handle_table.take_orphan(ino));
        });
    }

    #[test]
    fn getattr_unchecked_clears_stale_runtime_state_when_inode_is_missing() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "getattr-stale-runtime-missing-ino");
        let ino = 9_101;
        let mut cached_attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
        cached_attr.size = 5;
        vfs.state.write_buffer.pending_blocks.lock().insert(
            (ino, 0),
            PendingBlock {
                data: BytesMut::from(&b"stale"[..]),
                dirty: true,
                reserved_object_id: None,
            },
        );
        vfs.state
            .write_buffer
            .pending_block_indexes
            .lock()
            .entry(ino)
            .or_default()
            .insert(0);
        vfs.state
            .write_buffer
            .pending_bytes
            .fetch_add(5, AtomicOrdering::Relaxed);
        vfs.refresh_inode_incarnation_state_from_attr(ino, &cached_attr);
        vfs.handle_table.mark_orphan(ino);

        let err = vfs
            .getattr_unchecked(ino)
            .expect_err("missing inode should fail");

        assert!(matches!(
            err,
            VfsError::Meta(MetaError::InodeNotFound(found)) if found == ino
        ));
        assert!(!vfs.state.namespace.attr_read_cache.lock().contains(ino));
        assert!(!vfs.state.namespace.dirty_attrs.lock().contains_key(&ino));
        assert!(
            !vfs.state
                .write_buffer
                .pending_blocks
                .lock()
                .contains_key(&(ino, 0))
        );
        assert!(
            !vfs.state
                .incarnation
                .inode_incarnations
                .lock()
                .contains_key(&ino)
        );
        assert!(!vfs.handle_table.take_orphan(ino));
    }

    #[test]
    fn prefetch_read_context_from_attr_uses_pending_content_epoch() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "prefetch-read-context-pending-epoch");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"prefetch-pending-epoch.bin", 1000, 1000, 0o644)
                .expect("create file");
            vfs.write(created.ino, 0, b"pending epoch")
                .await
                .expect("stage dirty write");

            let pending_attr = vfs.getattr(created.ino).expect("load pending attr");
            let expected_pending_epoch = vfs
                .refresh_inode_incarnation_state_from_attr(created.ino, &pending_attr)
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch);
            let read_context = vfs.prefetch_read_context_from_attr(created.ino, &pending_attr);

            assert!(expected_pending_epoch.is_some());
            assert_eq!(read_context.pending_content_epoch, expected_pending_epoch);
            assert_eq!(read_context.size, pending_attr.size);
        });
    }

    #[test]
    fn assemble_read_plan_reports_written_for_committed_cache_hit() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "assemble-read-plan-written");
        rt.block_on(async {
            let object_id = 0xCC77;
            vfs.block_store_put_async(object_id, Bytes::from_static(b"0123456789"), false)
                .await
                .expect("block store put");

            let mut block = BlockRef::new_full(object_id, 10);
            block.offset = 2;
            block.length = 4;
            let plan = vec![ReadPlanSpan {
                range: ReadRange { start: 0, end: 4 },
                source: ReadPlanSource::Base { block },
            }];
            let mut buffer = [0_u8; 4];
            let written = ReadPipeline::assemble_into(
                &vfs,
                0,
                &plan,
                &std::collections::HashSet::new(),
                &mut buffer,
            )
            .await
            .expect("assemble read plan");
            assert_eq!(written, 4);
            assert_eq!(&buffer, b"2345");
        });
    }

    #[test]
    fn build_read_plan_prefers_dirty_span_over_committed_base_coverage() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "build-read-plan-dirty-over-base");
        rt.block_on(async {
            let attr = vfs
                .create(1, b"dirty-over-base.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let object_id = 0xA11CE;
            vfs.block_store_put_async(object_id, Bytes::from_static(b"abcdefgh"), false)
                .await
                .expect("seed committed block");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.set_file_block(ino, 0, &BlockRef::new_partial(object_id, 8))
                .expect("persist committed block");

            vfs.write(ino, 2, b"XY").await.expect("stage dirty patch");

            let pending_attr = vfs.getattr(ino).expect("load pending attr");
            let pending_content_epoch = vfs
                .refresh_inode_incarnation_state_from_attr(ino, &pending_attr)
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch);
            let plan = ReadPipeline::new(&vfs, ino, 0, 8, pending_content_epoch, None)
                .build_plan()
                .await
                .expect("build read plan");

            assert!(plan.iter().any(|span| matches!(
                &span.source,
                ReadPlanSource::Dirty { .. } if span.range.start == 2 && span.range.end == 4
            )));
            assert!(!plan.iter().any(|span| {
                matches!(
                    &span.source,
                    ReadPlanSource::Overlay { .. } | ReadPlanSource::Base { .. }
                ) && span.range.start < 4
                    && span.range.end > 2
            }));
        });
    }

    #[test]
    fn assemble_read_plan_zero_fills_remote_not_found_committed_span() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "assemble-read-plan-not-found");
        rt.block_on(async {
            let object_id = 0xF404;
            let plan = vec![ReadPlanSpan {
                range: ReadRange { start: 0, end: 4 },
                source: ReadPlanSource::Base {
                    block: BlockRef::new_partial(object_id, 4),
                },
            }];
            let mut remote_not_found = HashSet::new();
            remote_not_found.insert(object_id);
            let mut buffer = [0xAB_u8; 4];

            let written =
                ReadPipeline::assemble_into(&vfs, 0, &plan, &remote_not_found, &mut buffer)
                    .await
                    .expect("assemble read plan");

            assert_eq!(written, 4);
            assert_eq!(buffer, [0_u8; 4]);
        });
    }
}
