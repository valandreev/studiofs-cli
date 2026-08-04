use super::file_session::ReadAheadToken;
use super::overlay::PendingOverlayBlockSource;
use super::writer_stack::WriterStackReadClaim;
use super::{
    ATTR_CACHE_TTL, AccessLevel, ForegroundReadScope, PUBLISHED_READ_PLAN_CACHE_WINDOW_BLOCKS,
    PrefetchBatch, PrefetchReadContext, PublishedReadPlanCacheKey, ReadRequestContext, StudioVfs,
    VfsError, VfsResult,
};
use crate::VfsMeta;
use crate::runtime::run_meta;
use crate::utils::{IntervalRange, subtract_claimed_range};
use bytes::Bytes;
use futures::{StreamExt, stream};
use smallvec::{SmallVec, smallvec};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use studiofs_meta::MetaError;
use studiofs_meta::schema::{
    DEFAULT_CHUNK_SIZE, FileType, ResolvedSliceSource, ResolvedSliceSpan,
    UploadState as MetaUploadState, chunk_start_offset,
};
use studiofs_meta::types::{Ino, InodeAttr, PayloadRef, block_index_from_offset};
use tracing::{debug, error, info, warn};

type ReadRange = IntervalRange;

#[derive(Debug, Clone)]
enum ReadSegmentSource {
    Dirty { data: Bytes, block_offset: u64 },
    PendingOverlay { data: Bytes, block_offset: u32 },
    PendingExtent { object_id: u64, block_offset: u32 },
    Overlay { payload: PayloadRef },
    Base { payload: PayloadRef },
    Zero,
}

#[derive(Debug, Clone)]
struct ReadSegment {
    range: ReadRange,
    source: ReadSegmentSource,
}

type ReadRangeList = SmallVec<[ReadRange; 8]>;
type ReadBlockList = SmallVec<[u32; 16]>;
const MAX_FOREGROUND_COLD_READ_FETCH_IN_FLIGHT: usize = 64;

struct ReadClaimBuffers {
    claimed: ReadRangeList,
    scratch: ReadRangeList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteFetchMode {
    ForegroundColdRead,
    BestEffortPrefetch,
}

impl ReadClaimBuffers {
    fn new() -> Self {
        Self {
            claimed: ReadRangeList::new(),
            scratch: ReadRangeList::new(),
        }
    }
}

struct ReadDataPlanner {
    requested: ReadRange,
    unresolved: ReadRangeList,
    segments: Vec<ReadSegment>,
    claim_buffers: ReadClaimBuffers,
}

impl ReadDataPlanner {
    fn new(offset: u64, length: u64) -> Option<Self> {
        let read_end = offset.checked_add(length)?;
        let requested = ReadRange::new(offset, read_end)?;
        Some(Self {
            requested,
            unresolved: smallvec![requested],
            segments: Vec::new(),
            claim_buffers: ReadClaimBuffers::new(),
        })
    }

    fn is_complete(&self) -> bool {
        self.unresolved.is_empty()
    }

    fn claim_range(&mut self, candidate: ReadRange) {
        Self::claim_unresolved_ranges(&mut self.unresolved, candidate, &mut self.claim_buffers);
    }

    #[cfg(test)]
    fn claim_resolved_metadata(mut self, resolved: &[ResolvedSliceSpan]) -> Self {
        self.claim_resolved_metadata_in_place(resolved);
        self
    }

    fn claim_resolved_metadata_in_place(&mut self, resolved: &[ResolvedSliceSpan]) {
        self.claim_meta_spans(resolved.iter(), ResolvedSliceSource::Overlay, |payload| {
            ReadSegmentSource::Overlay { payload }
        });
        self.claim_meta_spans(resolved.iter(), ResolvedSliceSource::Base, |payload| {
            ReadSegmentSource::Base { payload }
        });
    }

    fn claim_meta_spans<'b, I, F>(
        &mut self,
        resolved: I,
        source_filter: ResolvedSliceSource,
        source_ctor: F,
    ) where
        I: Iterator<Item = &'b ResolvedSliceSpan>,
        F: Fn(PayloadRef) -> ReadSegmentSource + Copy,
    {
        for span in resolved {
            if self.is_complete() {
                break;
            }
            if span.source != source_filter {
                continue;
            }
            let Some(block) = span.payload.as_ref() else {
                continue;
            };
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let Some(span_range) = ReadRange::new(
                chunk_base.saturating_add(span.range.start as u64),
                chunk_base.saturating_add(span.range.end as u64),
            ) else {
                continue;
            };
            let Some(clipped) = span_range.intersection(self.requested) else {
                continue;
            };
            self.claim_range(clipped);
            let claim_buffers = &self.claim_buffers;
            let segments = &mut self.segments;
            for overlap in &claim_buffers.claimed {
                let delta = overlap.start().saturating_sub(span_range.start());
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = block.offset.checked_add(delta_u32) else {
                    continue;
                };
                let length = overlap.end().saturating_sub(overlap.start());
                let Some(block_length) = u32::try_from(length).ok() else {
                    continue;
                };
                let mut planned_block = *block;
                planned_block.offset = block_offset;
                planned_block.length = block_length;
                segments.push(ReadSegment {
                    range: *overlap,
                    source: source_ctor(planned_block),
                });
            }
        }
    }

    fn finish(mut self) -> Vec<ReadSegment> {
        for gap in self.unresolved {
            self.segments.push(ReadSegment {
                range: gap,
                source: ReadSegmentSource::Zero,
            });
        }
        self.segments
    }

    fn claim_unresolved_ranges(
        unresolved: &mut ReadRangeList,
        candidate: ReadRange,
        buffers: &mut ReadClaimBuffers,
    ) {
        buffers.claimed.clear();
        buffers.scratch.clear();
        subtract_claimed_range(
            unresolved.drain(..),
            candidate,
            &mut buffers.scratch,
            &mut buffers.claimed,
        );
        std::mem::swap(unresolved, &mut buffers.scratch);
    }
}

struct DataReader<'a> {
    vfs: &'a StudioVfs,
    ino: Ino,
    offset: u64,
    length: u64,
    pending_content_epoch: Option<u64>,
    resolved_regular_file_slices: Option<&'a [ResolvedSliceSpan]>,
}

impl<'a> DataReader<'a> {
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

    async fn read_into(&self, buffer: &mut [u8]) -> VfsResult<usize> {
        let writer_stack_claims =
            self.vfs
                .state
                .writer_stack
                .copy_read_spans_into(self.ino, self.offset, buffer);
        let mut segments = self.build_segments_excluding(&writer_stack_claims).await?;
        segments.sort_by_key(|segment| segment.range.start());
        Self::log_debug_target_segments(self.vfs, self.ino, self.offset, self.length, &segments);
        // Claims are generated inside this read range; track the furthest byte
        // already satisfied by the direct writer-stack copy.
        let writer_stack_written = writer_stack_claims
            .iter()
            .filter_map(|claim| usize::try_from(claim.end.saturating_sub(self.offset)).ok())
            .max()
            .unwrap_or(0);
        Self::fetch_missing_remote_blocks(self.vfs, &segments).await?;
        let written = Self::assemble_into(self.vfs, self.offset, &segments, buffer)
            .await?
            .max(writer_stack_written);
        Ok(written)
    }

    async fn prefetch_only(&self) -> VfsResult<()> {
        let writer_stack_claims =
            self.vfs
                .state
                .writer_stack
                .snapshot_read_claims(self.ino, self.offset, self.length);
        let mut segments = self.build_segments_excluding(&writer_stack_claims).await?;
        segments.sort_by_key(|segment| segment.range.start());
        Self::populate_missing_remote_blocks(self.vfs, &segments, true).await?;
        Ok(())
    }

    fn log_debug_target_segments(
        vfs: &StudioVfs,
        ino: Ino,
        offset: u64,
        length: u64,
        segments: &[ReadSegment],
    ) {
        let Some(path) = vfs.debug_pending_target_path(ino) else {
            return;
        };
        let mut dirty_spans = 0usize;
        let mut pending_overlay_spans = 0usize;
        let mut pending_extent_spans = 0usize;
        let mut overlay_spans = 0usize;
        let mut base_spans = 0usize;
        let mut zero_spans = 0usize;
        for segment in segments {
            match &segment.source {
                ReadSegmentSource::Dirty { .. } => dirty_spans += 1,
                ReadSegmentSource::PendingOverlay { .. } => pending_overlay_spans += 1,
                ReadSegmentSource::PendingExtent { .. } => pending_extent_spans += 1,
                ReadSegmentSource::Overlay { .. } => overlay_spans += 1,
                ReadSegmentSource::Base { .. } => base_spans += 1,
                ReadSegmentSource::Zero => zero_spans += 1,
            }
        }
        info!(
            ino,
            path = %path,
            offset,
            length,
            spans = segments.len(),
            dirty_spans,
            pending_overlay_spans,
            pending_extent_spans,
            overlay_spans,
            base_spans,
            zero_spans,
            "debug target data reader resolution"
        );
    }

    #[cfg(test)]
    fn collect_data_payload_refs(segments: &[ReadSegment]) -> BTreeMap<u64, PayloadRef> {
        let mut payload_refs = BTreeMap::new();
        for segment in segments {
            let payload = match &segment.source {
                ReadSegmentSource::Overlay { payload } | ReadSegmentSource::Base { payload } => {
                    payload
                }
                ReadSegmentSource::Dirty { .. }
                | ReadSegmentSource::PendingOverlay { .. }
                | ReadSegmentSource::PendingExtent { .. }
                | ReadSegmentSource::Zero => continue,
            };
            payload_refs
                .entry(payload.object_id)
                .or_insert_with(|| *payload);
        }
        payload_refs
    }

    #[cfg(test)]
    async fn build_segments(&self) -> VfsResult<Vec<ReadSegment>> {
        let writer_stack_claims =
            self.vfs
                .state
                .writer_stack
                .snapshot_read_claims(self.ino, self.offset, self.length);
        self.build_segments_excluding(&writer_stack_claims).await
    }

    async fn build_segments_excluding(
        &self,
        writer_stack_claims: &[WriterStackReadClaim],
    ) -> VfsResult<Vec<ReadSegment>> {
        Self::build_segments_with(
            self.vfs,
            self.ino,
            self.offset,
            self.length,
            self.pending_content_epoch,
            self.resolved_regular_file_slices,
            writer_stack_claims,
        )
        .await
    }

    async fn build_segments_with(
        vfs: &StudioVfs,
        ino: Ino,
        offset: u64,
        length: u64,
        pending_content_epoch: Option<u64>,
        resolved_regular_file_slices: Option<&[ResolvedSliceSpan]>,
        writer_stack_claims: &[WriterStackReadClaim],
    ) -> VfsResult<Vec<ReadSegment>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let mut planner = ReadDataPlanner::new(offset, length).ok_or_else(|| {
            VfsError::Other(format!(
                "read range overflow: ino={ino}, offset={offset}, length={length}"
            ))
        })?;
        let mut pending_upload_index = None;

        for claim in writer_stack_claims {
            if planner.is_complete() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(claim.start, claim.end) else {
                continue;
            };
            planner.claim_range(dirty_range);
        }

        let dirty_spans = vfs.snapshot_pending_dirty_slice_read_spans(ino, offset, length);
        for dirty in dirty_spans {
            if planner.is_complete() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(dirty.start, dirty.end) else {
                continue;
            };
            planner.claim_range(dirty_range);
            let claim_buffers = &planner.claim_buffers;
            let segments = &mut planner.segments;
            for overlap in &claim_buffers.claimed {
                let delta = overlap.start().saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                segments.push(ReadSegment {
                    range: *overlap,
                    source: ReadSegmentSource::Dirty {
                        data: dirty.data.clone(),
                        block_offset: u64::from(block_offset),
                    },
                });
            }
        }

        let pending_payload_spans =
            vfs.snapshot_pending_local_dirty_block_read_spans(ino, offset, length);
        for dirty in pending_payload_spans {
            if planner.is_complete() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(dirty.start, dirty.end) else {
                continue;
            };
            planner.claim_range(dirty_range);
            let claim_buffers = &planner.claim_buffers;
            let segments = &mut planner.segments;
            for overlap in &claim_buffers.claimed {
                let delta = overlap.start().saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                segments.push(ReadSegment {
                    range: *overlap,
                    source: ReadSegmentSource::Dirty {
                        data: dirty.data.clone(),
                        block_offset: u64::from(block_offset),
                    },
                });
            }
        }

        if !planner.is_complete() {
            let mut candidate_blocks = ReadBlockList::new();
            for range in &planner.unresolved {
                let start_block = block_index_from_offset(range.start(), vfs.block_size);
                let end_block =
                    block_index_from_offset(range.end().saturating_sub(1), vfs.block_size);
                for block_index in start_block..=end_block {
                    candidate_blocks.push(block_index);
                }
            }
            candidate_blocks.sort_unstable();
            candidate_blocks.dedup();
            for block_index in candidate_blocks {
                if planner.is_complete() {
                    break;
                }
                let source = Self::resolve_pending_overlay_block_source(
                    vfs,
                    ino,
                    block_index,
                    pending_content_epoch,
                    resolved_regular_file_slices,
                    &mut pending_upload_index,
                    planner.requested,
                );
                let Some(source) = source else { continue };
                if matches!(
                    &source,
                    PendingOverlayBlockSource::Extent(object_id)
                        if !vfs.local_payload_backend.contains(*object_id)
                ) {
                    continue;
                }
                let block_start = (block_index as u64).saturating_mul(vfs.block_size as u64);
                let block_end = block_start.saturating_add(vfs.block_size as u64);
                planner.claim_range(
                    ReadRange::new(block_start, block_end).expect("block range is non-empty"),
                );
                let claim_buffers = &planner.claim_buffers;
                let segments = &mut planner.segments;
                for overlap in &claim_buffers.claimed {
                    let delta = overlap.start().saturating_sub(block_start);
                    let Some(block_offset) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    let source = match &source {
                        PendingOverlayBlockSource::Pending(data) => {
                            ReadSegmentSource::PendingOverlay {
                                data: data.clone(),
                                block_offset,
                            }
                        }
                        PendingOverlayBlockSource::Extent(object_id) => {
                            ReadSegmentSource::PendingExtent {
                                object_id: *object_id,
                                block_offset,
                            }
                        }
                    };
                    segments.push(ReadSegment {
                        range: *overlap,
                        source,
                    });
                }
            }
        }

        if !planner.is_complete() {
            if let Some(resolved) = resolved_regular_file_slices {
                planner.claim_resolved_metadata_in_place(resolved);
            } else {
                let meta = vfs.meta.clone();
                let resolved = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.lookup_regular_file_slices(ino, offset, length)
                })
                .await?;
                planner.claim_resolved_metadata_in_place(&resolved);
            }
        }

        Ok(planner.finish())
    }

    fn resolve_pending_overlay_block_source(
        vfs: &StudioVfs,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        resolved: Option<&[ResolvedSliceSpan]>,
        pending_upload_index: &mut Option<crate::vfs::overlay::PendingUploadCatalog>,
        requested: ReadRange,
    ) -> Option<PendingOverlayBlockSource> {
        let mut source = Self::pending_overlay_block_source_from_index(
            vfs,
            ino,
            block_index,
            pending_content_epoch,
            resolved,
            pending_upload_index.as_ref(),
            requested,
        );
        if source.is_none()
            && pending_upload_index.is_none()
            && vfs.cached_store.has_pending_uploads_for_ino(ino)
        {
            *pending_upload_index = Some(vfs.snapshot_pending_upload_index_for_inode(ino));
            source = Self::pending_overlay_block_source_from_index(
                vfs,
                ino,
                block_index,
                pending_content_epoch,
                resolved,
                pending_upload_index.as_ref(),
                requested,
            );
        }
        source
    }

    fn pending_overlay_block_source_from_index(
        vfs: &StudioVfs,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: Option<u64>,
        resolved: Option<&[ResolvedSliceSpan]>,
        pending_upload_index: Option<&crate::vfs::overlay::PendingUploadCatalog>,
        requested: ReadRange,
    ) -> Option<PendingOverlayBlockSource> {
        if let (Some(epoch), Some(resolved)) = (pending_content_epoch, resolved) {
            Self::pending_overlay_block_source_from_resolved(
                vfs,
                ino,
                block_index,
                epoch,
                pending_upload_index,
                resolved,
                requested,
            )
        } else {
            vfs.pending_overlay_block_source(
                ino,
                block_index,
                pending_content_epoch,
                pending_upload_index,
            )
        }
    }

    fn pending_overlay_block_source_from_resolved(
        vfs: &StudioVfs,
        ino: Ino,
        block_index: u32,
        pending_content_epoch: u64,
        pending_upload_index: Option<&crate::vfs::overlay::PendingUploadCatalog>,
        resolved: &[ResolvedSliceSpan],
        requested: ReadRange,
    ) -> Option<PendingOverlayBlockSource> {
        let extent = vfs.pending_live_extent_record(
            ino,
            block_index,
            Some(pending_content_epoch),
            pending_upload_index,
        )?;
        if Self::resolved_slices_match_pending_extent(
            vfs.block_size,
            block_index,
            extent.object_id,
            resolved,
            requested,
        ) {
            Some(PendingOverlayBlockSource::Extent(extent.object_id))
        } else {
            None
        }
    }

    fn resolved_slices_match_pending_extent(
        block_size: u32,
        block_index: u32,
        object_id: u64,
        resolved: &[ResolvedSliceSpan],
        requested: ReadRange,
    ) -> bool {
        let block_start = u64::from(block_index).saturating_mul(u64::from(block_size));
        let block_end = block_start.saturating_add(u64::from(block_size));
        let Some(block_range) = ReadRange::new(block_start, block_end) else {
            return false;
        };
        let Some(target) = block_range.intersection(requested) else {
            return false;
        };
        let mut cursor = target.start();
        let start_index = resolved.partition_point(|span| {
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            chunk_base.saturating_add(u64::from(span.range.end)) <= target.start()
        });
        for span in &resolved[start_index..] {
            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let span_start = chunk_base.saturating_add(u64::from(span.range.start));
            if span_start >= target.end() {
                break;
            }
            let span_end = chunk_base.saturating_add(u64::from(span.range.end));
            let Some(span_range) = ReadRange::new(span_start, span_end) else {
                continue;
            };
            let Some(overlap) = span_range.intersection(target) else {
                continue;
            };
            if overlap.start() != cursor {
                return false;
            }
            if !matches!(
                span.source,
                ResolvedSliceSource::Overlay | ResolvedSliceSource::Base
            ) {
                return false;
            }
            let Some(payload) = span.payload.as_ref() else {
                return false;
            };
            let span_delta = overlap.start().saturating_sub(span_range.start());
            let Some(payload_offset) = u64::from(payload.offset).checked_add(span_delta) else {
                return false;
            };
            let expected_offset = overlap.start().saturating_sub(block_start);
            if payload.object_id != object_id || payload_offset != expected_offset {
                return false;
            }
            cursor = overlap.end();
        }
        cursor == target.end()
    }

    #[cfg(test)]
    fn claim_unresolved_ranges(
        unresolved: &mut ReadRangeList,
        candidate: ReadRange,
        buffers: &mut ReadClaimBuffers,
    ) {
        ReadDataPlanner::claim_unresolved_ranges(unresolved, candidate, buffers);
    }

    async fn populate_missing_remote_blocks(
        vfs: &StudioVfs,
        segments: &[ReadSegment],
        prefetch: bool,
    ) -> VfsResult<()> {
        let mode = if prefetch {
            RemoteFetchMode::BestEffortPrefetch
        } else {
            RemoteFetchMode::ForegroundColdRead
        };
        Self::fetch_remote_blocks(vfs, segments, mode).await
    }

    async fn fetch_missing_remote_blocks(
        vfs: &StudioVfs,
        segments: &[ReadSegment],
    ) -> VfsResult<()> {
        Self::fetch_remote_blocks(vfs, segments, RemoteFetchMode::ForegroundColdRead).await
    }

    fn has_local_payload_for_read(vfs: &StudioVfs, object_id: u64) -> bool {
        vfs.local_payload_backend.contains(object_id)
            || vfs.cached_store.has_staged_upload(object_id)
            || vfs.cached_store.get_hot_upload(object_id).is_some()
    }

    async fn fetch_remote_blocks(
        vfs: &StudioVfs,
        segments: &[ReadSegment],
        mode: RemoteFetchMode,
    ) -> VfsResult<()> {
        let mut remote_blocks = BTreeMap::new();
        let mut local_pending = BTreeSet::new();
        for segment in segments {
            match &segment.source {
                ReadSegmentSource::Overlay { payload } | ReadSegmentSource::Base { payload } => {
                    remote_blocks
                        .entry(payload.object_id)
                        .or_insert_with(|| *payload);
                }
                ReadSegmentSource::PendingExtent { object_id, .. } => {
                    local_pending.insert(*object_id);
                }
                ReadSegmentSource::Dirty { .. }
                | ReadSegmentSource::PendingOverlay { .. }
                | ReadSegmentSource::Zero => {}
            }
        }
        for object_id in local_pending {
            vfs.local_payload_backend.advise_will_need(object_id);
        }
        if remote_blocks.is_empty() {
            return Ok(());
        }
        if matches!(mode, RemoteFetchMode::BestEffortPrefetch) {
            for object_id in remote_blocks.keys() {
                if vfs.local_payload_backend.contains(*object_id) {
                    vfs.local_payload_backend.advise_will_need(*object_id);
                }
            }
        }
        remote_blocks.retain(|object_id, _| !Self::has_local_payload_for_read(vfs, *object_id));
        if remote_blocks.is_empty() {
            return Ok(());
        }
        if matches!(mode, RemoteFetchMode::ForegroundColdRead) && remote_blocks.len() <= 1 {
            // Keep single-block foreground cold reads on the legacy assembly
            // path. The eager warmup path is only for multi-block reads where
            // parallelism can hide S3 latency without adding an extra fetch
            // stage to the common 4 MiB callback.
            return Ok(());
        }

        let concurrency = match mode {
            RemoteFetchMode::ForegroundColdRead => Self::foreground_cold_read_concurrency(
                vfs.fetch_scheduler().effective_limit(),
                remote_blocks.len(),
            ),
            RemoteFetchMode::BestEffortPrefetch => remote_blocks.len().max(1),
        };

        let mut fetches = stream::iter(remote_blocks.into_values())
            .map(|block| async move { Self::fetch_remote_block(vfs, block, mode).await })
            .buffer_unordered(concurrency);
        while let Some(result) = fetches.next().await {
            result?;
        }
        Ok(())
    }

    fn foreground_cold_read_concurrency(fetch_limit: usize, block_count: usize) -> usize {
        if block_count == 0 {
            return 0;
        }
        fetch_limit
            .max(1)
            .min(block_count)
            .min(MAX_FOREGROUND_COLD_READ_FETCH_IN_FLIGHT)
    }

    async fn fetch_remote_block(
        vfs: &StudioVfs,
        block: PayloadRef,
        mode: RemoteFetchMode,
    ) -> VfsResult<()> {
        let prefetch = matches!(mode, RemoteFetchMode::BestEffortPrefetch);
        let result = if prefetch {
            vfs.fetch_block_for_ref(&block, true).await
        } else {
            vfs.fetch_block_for_ref_after_local_miss(&block).await
        };
        match result {
            Ok(_) => {
                if prefetch {
                    let removed = vfs
                        .state
                        .metrics
                        .prefetched_object_remaining_bytes
                        .lock()
                        .push(block.object_id, block.length)
                        .map(|(object_id, _)| object_id);
                    if let Some(object_id) = removed
                        && object_id != block.object_id
                    {
                        vfs.state.metrics.prefetched_object_ids.remove(&object_id);
                    }
                    vfs.state
                        .metrics
                        .prefetched_object_ids
                        .insert(block.object_id);
                }
                Ok::<(), VfsError>(())
            }
            Err(crate::fetch::FetchError::NotFound { ref key }) => {
                warn!(
                    object_id = block.object_id,
                    key,
                    block_offset = block.offset,
                    block_length = block.length,
                    prefetch,
                    "populate_missing_remote_blocks: S3 object not found during remote read warmup"
                );
                Ok::<(), VfsError>(())
            }
            Err(error) => {
                if prefetch {
                    debug!(
                        object_id = block.object_id,
                        error = %error,
                        "data reader prefetch failed; falling back to on-demand resolution"
                    );
                    Ok::<(), VfsError>(())
                } else {
                    Err(VfsError::Fetch(error))
                }
            }
        }
    }

    async fn assemble_into(
        vfs: &StudioVfs,
        offset: u64,
        segments: &[ReadSegment],
        buffer: &mut [u8],
    ) -> VfsResult<usize> {
        let mut written = 0usize;
        let mut index = 0usize;
        while index < segments.len() {
            let segment = &segments[index];
            let dst_start = segment.range.start().saturating_sub(offset) as usize;
            let dst_end = segment.range.end().saturating_sub(offset) as usize;
            if dst_start >= buffer.len() || dst_end > buffer.len() || dst_start >= dst_end {
                index += 1;
                continue;
            }
            match &segment.source {
                ReadSegmentSource::Dirty { data, block_offset } => {
                    let dst = &mut buffer[dst_start..dst_end];
                    vfs.copy_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadSegmentSource::PendingOverlay { data, block_offset } => {
                    let dst = &mut buffer[dst_start..dst_end];
                    vfs.copy_payload_into_range(data.as_ref(), u64::from(*block_offset), dst);
                }
                ReadSegmentSource::PendingExtent {
                    object_id,
                    block_offset,
                } => {
                    let dst = &mut buffer[dst_start..dst_end];
                    let block_offset = usize::try_from(*block_offset).map_err(|_| {
                        VfsError::Other(format!(
                            "pending extent offset does not fit usize: object_id={}, offset={}",
                            object_id, block_offset
                        ))
                    })?;
                    let read = vfs
                        .payload_store_read_into_async(*object_id, block_offset, dst)
                        .await?;
                    match read {
                        Some(read) if read == dst.len() => {}
                        Some(read) => {
                            debug!(
                                object_id,
                                file_offset = segment.range.start(),
                                block_offset,
                                requested = dst.len(),
                                read,
                                "data_reader: short local read for pending extent; padding tail with zeros"
                            );
                            dst[read..].fill(0);
                        }
                        None => {
                            warn!(
                                object_id,
                                file_offset = segment.range.start(),
                                block_offset,
                                requested = dst.len(),
                                "data_reader: pending extent missing from local backend, zeroing range"
                            );
                            dst.fill(0);
                        }
                    }
                }
                ReadSegmentSource::Overlay { payload } | ReadSegmentSource::Base { payload } => {
                    let (run_end, run_dst_end) =
                        Self::committed_local_read_run(segments, index, offset, buffer.len());
                    let mut run_payload = *payload;
                    run_payload.length =
                        u32::try_from(run_dst_end.saturating_sub(dst_start)).map_err(|_| {
                            VfsError::Other(format!(
                                "committed payload read run length does not fit u32: object_id={}, len={}",
                                payload.object_id,
                                run_dst_end.saturating_sub(dst_start)
                            ))
                        })?;
                    let dst = &mut buffer[dst_start..run_dst_end];
                    Self::assemble_committed_payload_into(
                        vfs,
                        &run_payload,
                        segment.range.start(),
                        dst,
                    )
                    .await?;
                    written = written.max(run_dst_end);
                    index = run_end;
                    continue;
                }
                ReadSegmentSource::Zero => {
                    let dst = &mut buffer[dst_start..dst_end];
                    dst.fill(0);
                }
            }
            written = written.max(dst_end);
            index += 1;
        }
        Ok(written)
    }

    fn committed_segment_payload(segment: &ReadSegment) -> Option<&PayloadRef> {
        match &segment.source {
            ReadSegmentSource::Overlay { payload } | ReadSegmentSource::Base { payload } => {
                Some(payload)
            }
            ReadSegmentSource::Dirty { .. }
            | ReadSegmentSource::PendingOverlay { .. }
            | ReadSegmentSource::PendingExtent { .. }
            | ReadSegmentSource::Zero => None,
        }
    }

    fn committed_local_read_run(
        segments: &[ReadSegment],
        start_index: usize,
        read_offset: u64,
        buffer_len: usize,
    ) -> (usize, usize) {
        let Some(first) = segments.get(start_index) else {
            return (start_index, 0);
        };
        let Some(first_payload) = Self::committed_segment_payload(first) else {
            let dst_end = first.range.end().saturating_sub(read_offset) as usize;
            return (start_index + 1, dst_end.min(buffer_len));
        };
        let mut run_end = start_index + 1;
        let mut file_end = first.range.end();
        let mut dst_end = file_end.saturating_sub(read_offset) as usize;
        let mut next_payload_offset = u64::from(first_payload.offset)
            .saturating_add(first.range.end().saturating_sub(first.range.start()));

        while let Some(next) = segments.get(run_end) {
            let Some(next_payload) = Self::committed_segment_payload(next) else {
                break;
            };
            let next_dst_start = next.range.start().saturating_sub(read_offset) as usize;
            let next_dst_end = next.range.end().saturating_sub(read_offset) as usize;
            if next_payload.object_id != first_payload.object_id
                || next.range.start() != file_end
                || u64::from(next_payload.offset) != next_payload_offset
                || next_dst_start != dst_end
                || next_dst_end > buffer_len
                || next_dst_start >= next_dst_end
            {
                break;
            }
            run_end += 1;
            file_end = next.range.end();
            dst_end = next_dst_end;
            next_payload_offset = next_payload_offset
                .saturating_add(next.range.end().saturating_sub(next.range.start()));
        }

        (run_end, dst_end)
    }

    async fn assemble_committed_payload_into(
        vfs: &StudioVfs,
        payload: &PayloadRef,
        file_offset: u64,
        dst: &mut [u8],
    ) -> VfsResult<()> {
        let block_offset = usize::try_from(payload.offset).map_err(|_| {
            VfsError::Other(format!(
                "block offset does not fit usize: object_id={}, offset={}",
                payload.object_id, payload.offset
            ))
        })?;
        let mut span_resolved = false;
        if let Some(read) = vfs
            .payload_store_read_into_async(payload.object_id, block_offset, dst)
            .await?
        {
            if read == dst.len() {
                span_resolved = true;
                let prefetched = &vfs.state.metrics.prefetched_object_ids;
                if prefetched.contains(&payload.object_id) {
                    let mut remaining_bytes =
                        vfs.state.metrics.prefetched_object_remaining_bytes.lock();
                    let remove =
                        if let Some(remaining) = remaining_bytes.get_mut(&payload.object_id) {
                            let read_bytes = u32::try_from(read).unwrap_or(u32::MAX);
                            let useful = (*remaining).min(read_bytes);
                            *remaining = (*remaining).saturating_sub(useful);
                            vfs.state
                                .metrics
                                .readahead_used_bytes
                                .fetch_add(u64::from(useful), Ordering::Relaxed);
                            *remaining == 0
                        } else {
                            true
                        };
                    if remove {
                        remaining_bytes.pop(&payload.object_id);
                        drop(remaining_bytes);
                        prefetched.remove(&payload.object_id);
                    }
                }
            }
            if !span_resolved {
                error!(
                    object_id = payload.object_id,
                    file_offset,
                    block_offset = payload.offset,
                    requested = dst.len(),
                    read,
                    "data_reader: short local backend read for committed payload object; evicting and refetching"
                );
                if let Err(evict_error) = vfs.payload_store_remove_async(payload.object_id).await {
                    warn!(
                        object_id = payload.object_id,
                        error = %evict_error,
                        "data_reader: failed to evict short-read local payload before refetch"
                    );
                }
            }
        }

        if !span_resolved && let Some(data) = vfs.upload_stage_get_async(payload.object_id).await? {
            vfs.copy_committed_payload_into_range(data.as_ref(), payload, file_offset, dst)?;
            span_resolved = true;
        }

        if !span_resolved {
            match vfs.fetch_block_for_ref_after_local_miss(payload).await {
                Ok(data) => {
                    vfs.copy_committed_payload_into_range(
                        data.as_ref(),
                        payload,
                        file_offset,
                        dst,
                    )?;
                }
                Err(crate::fetch::FetchError::NotFound { key }) => {
                    warn!(
                        object_id = payload.object_id,
                        key,
                        file_offset,
                        block_offset = payload.offset,
                        block_length = payload.length,
                        "data_reader: S3 object not found during foreground read, zeroing range (upload incomplete or evicted before upload?)"
                    );
                    dst.fill(0);
                }
                Err(error) => {
                    return Err(VfsError::Fetch(error));
                }
            }
        }
        Ok(())
    }
}

struct ReadMetadata {
    attr: InodeAttr,
    resolved_slices: Arc<[ResolvedSliceSpan]>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataRefreshDecision {
    UseLocal,
    RefreshAuthoritative,
}

impl MetadataRefreshDecision {
    fn for_local_attr(
        attr: &InodeAttr,
        has_pending_local_payloads: bool,
        has_dirty_writer_stack: bool,
        has_pending_uploads: bool,
        has_pending_incarnation: bool,
        has_pending_namespace_metadata: bool,
    ) -> Self {
        let tracks_pending_regular_content = attr.kind == FileType::RegularFile
            && (attr.upload_state != MetaUploadState::Ready
                || attr.content_epoch != attr.committed_content_epoch);
        if !tracks_pending_regular_content
            || has_pending_local_payloads
            || has_dirty_writer_stack
            || has_pending_uploads
            || has_pending_incarnation
            || has_pending_namespace_metadata
        {
            return Self::UseLocal;
        }
        Self::RefreshAuthoritative
    }
}

pub(super) struct PreparedForegroundRead {
    pub(super) context: ReadRequestContext,
    pub(super) resolved_slices: Arc<[ResolvedSliceSpan]>,
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
        if let Some(attr) = self.cached_attr()? {
            return Ok(attr);
        }
        self.load_attr_from_meta_sync()
    }

    async fn resolve_attr(&self) -> VfsResult<InodeAttr> {
        if let Some(attr) = self.ignored_attr() {
            return Ok(attr);
        }
        if let Some(attr) = self.dirty_attr_async().await? {
            return Ok(attr);
        }
        if let Some(attr) = self.cached_attr_async().await? {
            return Ok(attr);
        }
        self.load_attr_from_meta().await
    }

    async fn resolve_for_read(&self, offset: u64, buffer_len: u64) -> VfsResult<ReadMetadata> {
        if let Some(attr) = self.ignored_attr() {
            return Ok(ReadMetadata {
                attr,
                resolved_slices: Arc::from(Vec::<ResolvedSliceSpan>::new()),
            });
        }
        if let Some(attr) = self.dirty_attr_async().await? {
            let resolved_slices = self
                .resolve_slices_for_attr(&attr, offset, buffer_len, true)
                .await?;
            return Ok(ReadMetadata {
                attr,
                resolved_slices,
            });
        }
        if let Some(attr) = self.cached_attr_async().await? {
            let resolved_slices = self
                .resolve_slices_for_attr(&attr, offset, buffer_len, false)
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
        let dirty_attr = self
            .vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&self.ino)
            .cloned()?;

        if MetadataRefreshDecision::for_local_attr(
            &dirty_attr,
            self.vfs
                .has_pending_local_dirty_payloads_for_inode(self.ino),
            self.vfs.inode_has_dirty_writer_stack(self.ino),
            self.vfs.inode_has_pending_uploads(self.ino),
            self.vfs
                .has_pending_incarnation_newer_than(self.ino, dirty_attr.committed_content_epoch),
            self.vfs.has_deferred_namespace_metadata(self.ino),
        ) == MetadataRefreshDecision::UseLocal
        {
            return Some(dirty_attr);
        }

        let remote_attr = match self.vfs.sync_meta().get_attr(self.ino) {
            Ok(remote_attr) => remote_attr,
            Err(error) => {
                warn!(
                    ino = self.ino,
                    error = %error,
                    "failed to refresh drained dirty attr from authoritative metadata"
                );
                return Some(dirty_attr);
            }
        };
        if self
            .vfs
            .ready_attr_supersedes_local_regular_file_state(self.ino, &remote_attr)
        {
            self.vfs.clear_superseded_local_regular_file_state(
                self.ino,
                remote_attr.committed_content_epoch,
                false,
            );
            return Some(remote_attr);
        }

        Some(dirty_attr)
    }

    async fn dirty_attr_async(&self) -> VfsResult<Option<InodeAttr>> {
        let Some(dirty_attr) = self
            .vfs
            .state
            .namespace
            .dirty_attrs
            .lock()
            .get(&self.ino)
            .cloned()
        else {
            return Ok(None);
        };

        if MetadataRefreshDecision::for_local_attr(
            &dirty_attr,
            self.vfs
                .has_pending_local_dirty_payloads_for_inode(self.ino),
            self.vfs.inode_has_dirty_writer_stack(self.ino),
            self.vfs.inode_has_pending_uploads(self.ino),
            self.vfs
                .has_pending_incarnation_newer_than(self.ino, dirty_attr.committed_content_epoch),
            self.vfs.has_deferred_namespace_metadata(self.ino),
        ) == MetadataRefreshDecision::UseLocal
        {
            return Ok(Some(dirty_attr));
        }

        let ino = self.ino;
        let meta = self.vfs.meta.clone();
        let remote_attr =
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino)).await {
                Ok(remote_attr) => remote_attr,
                Err(error) => {
                    warn!(
                        ino = self.ino,
                        error = %error,
                        "failed to refresh drained dirty attr from authoritative metadata"
                    );
                    return Ok(Some(dirty_attr));
                }
            };
        if self
            .vfs
            .ready_attr_supersedes_local_regular_file_state(self.ino, &remote_attr)
        {
            self.vfs.clear_superseded_local_regular_file_state(
                self.ino,
                remote_attr.committed_content_epoch,
                false,
            );
            return Ok(Some(remote_attr));
        }

        Ok(Some(dirty_attr))
    }

    fn cached_attr(&self) -> VfsResult<Option<InodeAttr>> {
        let cached_attr = self
            .vfs
            .state
            .namespace
            .attr_read_cache
            .lock()
            .get(self.ino, Instant::now());
        let Some(cached_attr) = cached_attr else {
            return Ok(None);
        };

        if MetadataRefreshDecision::for_local_attr(
            &cached_attr,
            self.vfs
                .has_pending_local_dirty_payloads_for_inode(self.ino),
            self.vfs.inode_has_dirty_writer_stack(self.ino),
            self.vfs.inode_has_pending_uploads(self.ino),
            self.vfs
                .has_pending_incarnation_newer_than(self.ino, cached_attr.committed_content_epoch),
            self.vfs.has_deferred_namespace_metadata(self.ino),
        ) == MetadataRefreshDecision::UseLocal
        {
            return Ok(Some(cached_attr));
        }

        let remote_attr = match self.vfs.sync_meta().get_attr(self.ino) {
            Ok(remote_attr) => remote_attr,
            Err(MetaError::InodeNotFound(_)) => return Err(self.stale_inode_not_found()),
            Err(error) => {
                warn!(
                    ino = self.ino,
                    error = %error,
                    "failed to refresh cached pending attr from authoritative metadata"
                );
                return Ok(Some(cached_attr));
            }
        };
        if StudioVfs::is_fully_published_regular_file(&remote_attr) {
            self.vfs.clear_superseded_local_regular_file_state(
                self.ino,
                remote_attr.committed_content_epoch,
                false,
            );
        }
        self.cache_attr(&remote_attr);
        Ok(Some(remote_attr))
    }

    async fn cached_attr_async(&self) -> VfsResult<Option<InodeAttr>> {
        let Some(cached_attr) = self
            .vfs
            .state
            .namespace
            .attr_read_cache
            .lock()
            .get(self.ino, Instant::now())
        else {
            return Ok(None);
        };

        if MetadataRefreshDecision::for_local_attr(
            &cached_attr,
            self.vfs
                .has_pending_local_dirty_payloads_for_inode(self.ino),
            self.vfs.inode_has_dirty_writer_stack(self.ino),
            self.vfs.inode_has_pending_uploads(self.ino),
            self.vfs
                .has_pending_incarnation_newer_than(self.ino, cached_attr.committed_content_epoch),
            self.vfs.has_deferred_namespace_metadata(self.ino),
        ) == MetadataRefreshDecision::UseLocal
        {
            return Ok(Some(cached_attr));
        }

        let ino = self.ino;
        let meta = self.vfs.meta.clone();
        let remote_attr =
            match run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino)).await {
                Ok(remote_attr) => remote_attr,
                Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                    return Err(self.stale_inode_not_found());
                }
                Err(error) => {
                    warn!(
                        ino = self.ino,
                        error = %error,
                        "failed to refresh cached pending attr from authoritative metadata"
                    );
                    return Ok(Some(cached_attr));
                }
            };
        if StudioVfs::is_fully_published_regular_file(&remote_attr) {
            self.vfs.clear_superseded_local_regular_file_state(
                self.ino,
                remote_attr.committed_content_epoch,
                false,
            );
        }
        self.cache_attr(&remote_attr);
        Ok(Some(remote_attr))
    }

    fn cache_attr(&self, attr: &InodeAttr) {
        if self
            .vfs
            .state
            .upload
            .deleted_inodes
            .lock()
            .contains(&self.ino)
        {
            self.vfs
                .state
                .namespace
                .attr_read_cache
                .lock()
                .remove(self.ino);
            return;
        }
        self.vfs.state.namespace.attr_read_cache.lock().put(
            self.ino,
            attr.clone(),
            ATTR_CACHE_TTL,
            Instant::now(),
        );
    }

    fn clear_stale_state(&self) {
        self.vfs.clear_pending_payloads_for_inode(self.ino);
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
        dirty_attr_present: bool,
    ) -> VfsResult<Arc<[ResolvedSliceSpan]>> {
        let read_len = self.read_len_for_attr(attr, offset, buffer_len);
        if read_len == 0 {
            return Ok(Arc::from(Vec::<ResolvedSliceSpan>::new()));
        }
        let dirty_attr_cacheable =
            dirty_attr_present && !self.vfs.inode_has_dirty_writer_stack(self.ino);
        if dirty_attr_present && !dirty_attr_cacheable {
            let ino = self.ino;
            let meta = self.vfs.meta.clone();
            let resolved = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.lookup_regular_file_slices(ino, offset, read_len)
            })
            .await
            {
                Ok(resolved) => resolved,
                Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                    return Err(self.stale_inode_not_found());
                }
                Err(err) => return Err(err),
            };
            return Ok(Arc::from(resolved));
        }

        let cache_key = if dirty_attr_cacheable {
            PublishedReadPlanCacheKey::from_locally_stable_attr(attr)
        } else {
            PublishedReadPlanCacheKey::from_attr(attr)
        };
        if let Some(key) = cache_key {
            let requested_end = offset.saturating_add(read_len);
            if let Some(cached) = self.vfs.state.read.published_read_plan_cache_get(
                self.ino,
                key,
                offset,
                requested_end,
            ) {
                return Ok(cached);
            }

            let cache_window_len = read_len.max(
                (self.vfs.block_size as u64)
                    .saturating_mul(PUBLISHED_READ_PLAN_CACHE_WINDOW_BLOCKS),
            );
            let cache_end =
                offset.saturating_add(attr.size.saturating_sub(offset).min(cache_window_len));
            let cache_read_len = cache_end.saturating_sub(offset);
            let ino = self.ino;
            let meta = self.vfs.meta.clone();
            let resolved = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                engine.lookup_regular_file_slices(ino, offset, cache_read_len)
            })
            .await
            {
                Ok(resolved) => resolved,
                Err(VfsError::Meta(MetaError::InodeNotFound(_))) => {
                    return Err(self.stale_inode_not_found());
                }
                Err(err) => return Err(err),
            };
            let resolved = Arc::<[ResolvedSliceSpan]>::from(resolved);
            self.vfs.state.read.published_read_plan_cache_put(
                self.ino,
                key,
                offset,
                cache_end,
                resolved.clone(),
            );
            return Ok(resolved);
        }
        let ino = self.ino;
        let meta = self.vfs.meta.clone();
        match run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.lookup_regular_file_slices(ino, offset, read_len)
        })
        .await
        {
            Ok(resolved) => Ok(Arc::from(resolved)),
            Err(VfsError::Meta(MetaError::InodeNotFound(_))) => Err(self.stale_inode_not_found()),
            Err(err) => Err(err),
        }
    }

    async fn load_attr_from_meta(&self) -> VfsResult<InodeAttr> {
        let ino = self.ino;
        let meta = self.vfs.meta.clone();
        let attr = match run_meta(meta, move |engine: Arc<dyn VfsMeta>| engine.get_attr(ino)).await
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
        let attr = self.load_attr_from_meta().await?;
        let resolved_slices = self
            .resolve_slices_for_attr(&attr, offset, buffer_len, false)
            .await?;
        self.cache_attr(&attr);
        Ok(ReadMetadata {
            attr,
            resolved_slices,
        })
    }
}

impl StudioVfs {
    pub(super) async fn read_metadata_block_from_slice_graph_async(
        &self,
        ino: Ino,
        block_index: u32,
        visible_size: u64,
    ) -> VfsResult<Option<Bytes>> {
        let logical_len = self.logical_block_len(visible_size, block_index);
        if logical_len == 0 {
            return Ok(None);
        }

        let block_start = (block_index as u64).saturating_mul(self.block_size as u64);
        let block_end = block_start.saturating_add(logical_len as u64);
        let meta = self.meta.clone();
        let resolved = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
            engine.lookup_regular_file_slices(ino, block_start, logical_len as u64)
        })
        .await?;
        if resolved.is_empty() {
            return Ok(None);
        }

        let requested =
            ReadRange::new(block_start, block_end).expect("metadata block range is non-empty");
        let mut buffer = vec![0u8; logical_len];
        for span in &resolved {
            let Some(block) = span.payload.as_ref() else {
                continue;
            };

            let chunk_base = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
            let Some(span_range) = ReadRange::new(
                chunk_base.saturating_add(span.range.start as u64),
                chunk_base.saturating_add(span.range.end as u64),
            ) else {
                continue;
            };
            let Some(overlap) = span_range.intersection(requested) else {
                continue;
            };

            let dst_start = usize::try_from(overlap.start().saturating_sub(block_start)).map_err(
                |_| {
                    VfsError::Other(format!(
                        "metadata payload read start does not fit usize: ino={}, block_index={}, start={}",
                        ino, block_index, overlap.start()
                    ))
                },
            )?;
            let dst_end =
                usize::try_from(overlap.end().saturating_sub(block_start)).map_err(|_| {
                    VfsError::Other(format!(
                        "metadata payload read end does not fit usize: ino={}, block_index={}, end={}",
                        ino, block_index, overlap.end()
                    ))
                })?;
            let dst = &mut buffer[dst_start..dst_end];

            let delta = overlap.start().saturating_sub(span_range.start());
            let delta_u32 = u32::try_from(delta).map_err(|_| {
                VfsError::Other(format!(
                    "metadata payload read delta does not fit u32: ino={}, block_index={}, delta={}",
                    ino, block_index, delta
                ))
            })?;
            let mut planned_block = *block;
            planned_block.offset =
                planned_block.offset.checked_add(delta_u32).ok_or_else(|| {
                    VfsError::Other(format!(
                        "metadata payload read offset overflow: ino={}, block_index={}, object_id={}",
                        ino, block_index, planned_block.object_id
                    ))
                })?;
            planned_block.length =
                u32::try_from(overlap.end().saturating_sub(overlap.start())).map_err(|_| {
                    VfsError::Other(format!(
                        "metadata payload read span too large: ino={}, block_index={}, start={}, end={}",
                        ino, block_index, overlap.start(), overlap.end()
                    ))
                })?;

            let mut span_resolved = false;
            let block_offset = usize::try_from(planned_block.offset).map_err(|_| {
                VfsError::Other(format!(
                    "metadata payload offset does not fit usize: object_id={}, offset={}",
                    planned_block.object_id, planned_block.offset
                ))
            })?;
            if let Some(read) = self
                .payload_store_read_into_async(planned_block.object_id, block_offset, dst)
                .await?
            {
                if read == dst.len() {
                    span_resolved = true;
                } else {
                    error!(
                        object_id = planned_block.object_id,
                        ino,
                        block_index,
                        requested = dst.len(),
                        read,
                        "metadata slice-graph base read saw short local payload read; evicting before refetch"
                    );
                    if let Err(evict_error) = self
                        .payload_store_remove_async(planned_block.object_id)
                        .await
                    {
                        warn!(
                            object_id = planned_block.object_id,
                            error = %evict_error,
                            "metadata slice-graph base read failed to evict short-read local payload"
                        );
                    }
                }
            }

            if !span_resolved {
                match self
                    .fetch_block_for_ref_after_local_miss(&planned_block)
                    .await
                {
                    Ok(data) => {
                        self.copy_committed_payload_into_range(
                            data.as_ref(),
                            &planned_block,
                            overlap.start(),
                            dst,
                        )?;
                    }
                    Err(crate::fetch::FetchError::NotFound { key }) => {
                        warn!(
                            object_id = planned_block.object_id,
                            ino,
                            block_index,
                            key = %key,
                            "metadata slice-graph base read missing remote fragment"
                        );
                        return Err(VfsError::Fetch(crate::fetch::FetchError::NotFound { key }));
                    }
                    Err(error) => return Err(VfsError::Fetch(error)),
                }
            }
        }

        Ok(Some(Bytes::from(buffer)))
    }

    pub(super) fn getattr_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if self.is_runtime_stats_file_ino(ino) {
            return Ok(self.runtime_stats_file_attr());
        }
        if let Some(attr) = self.deferred_namespace_attr(ino) {
            return Ok(attr);
        }
        ReadMetadataResolver::new(self, ino).resolve_attr_sync()
    }

    pub(super) async fn getattr_async_unchecked(&self, ino: Ino) -> VfsResult<InodeAttr> {
        if self.is_runtime_stats_file_ino(ino) {
            return Ok(self.runtime_stats_file_attr());
        }
        if let Some(attr) = self.deferred_namespace_attr(ino) {
            return Ok(attr);
        }
        ReadMetadataResolver::new(self, ino).resolve_attr().await
    }

    pub(super) async fn resolve_read_metadata_async(
        &self,
        ino: Ino,
        offset: u64,
        buffer_len: u64,
    ) -> VfsResult<(InodeAttr, Arc<[ResolvedSliceSpan]>)> {
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
        let debug_path = self.debug_pending_target_path(ino);
        if let Some(path) = &debug_path {
            info!(
                ino,
                path = %path,
                offset,
                requested_len,
                "debug target prepare_foreground_read resolving metadata"
            );
        }
        let (attr, resolved_slices) = self
            .resolve_read_metadata_async(ino, offset, requested_len as u64)
            .await?;
        let context = ReadRequestContext::from_attr(self, ino, attr);
        let read_len = context.read_len(offset, requested_len);
        if let Some(path) = &debug_path {
            info!(
                ino,
                path = %path,
                offset,
                requested_len,
                read_len,
                attr_size = context.attr.size,
                attr_upload_state = ?context.attr.upload_state,
                attr_content_epoch = context.attr.content_epoch,
                attr_committed_content_epoch = context.attr.committed_content_epoch,
                resolved_slice_count = resolved_slices.len(),
                "debug target prepare_foreground_read resolved metadata"
            );
        }
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
        speculative_read: Option<(ReadAheadToken, usize)>,
    ) -> VfsResult<usize> {
        debug_assert!(prepared.read_len > 0);
        let planned_read_len = prepared.read_len.min(buffer.len());
        let blocks_needed =
            self.calculate_blocks_needed(offset, planned_read_len, prepared.context.attr.size);
        let open_file = fh.and_then(|handle| self.handle_table.get(handle));
        let owner_token = open_file
            .as_ref()
            .and_then(|open_file| open_file.vfs_read_ahead_token());
        let access = self.state.read.observe_read_access(
            open_file.as_ref(),
            ino,
            offset,
            buffer.len(),
            self.access_tracker,
        );
        if let Some(path) = self.debug_pending_target_path(ino) {
            info!(
                ino,
                path = %path,
                fh = fh.unwrap_or_default(),
                offset,
                requested_len = buffer.len(),
                read_len = prepared.read_len,
                stream_id = access.stream_id,
                previous_access_class = access.previous_class.as_str(),
                access_class = access.class.as_str(),
                prefetch_enabled = access.policy.prefetch_enabled,
                readahead_base_width = access.policy.readahead_base_width,
                readahead_max_width = access.policy.readahead_max_width,
                readahead_min_prefetch = access.policy.readahead_min_prefetch,
                first_block = blocks_needed.first().map(|entry| entry.0).unwrap_or(u32::MAX),
                last_block = blocks_needed.last().map(|entry| entry.0).unwrap_or(u32::MAX),
                block_count = blocks_needed.len(),
                "debug target foreground read"
            );
        }
        let mut guard = ForegroundReadScope::new(
            &self.state.read,
            self.upload_fg_read_latency_window,
            access.policy,
        );
        self.enqueue_prefetch_hint(
            ino,
            access.stream_id,
            access.policy,
            &blocks_needed,
            owner_token,
        );
        // This is the launch linearization point for speculative read-window
        // I/O. A Kernel switch before this check shrinks the DataReader to the
        // original caller request; I/O already launched after a successful
        // check is allowed to finish.
        let read_len = match speculative_read {
            Some((token, fallback_len)) if !self.read_ahead_token_is_current(token) => {
                planned_read_len.min(fallback_len)
            }
            _ => planned_read_len,
        };
        let data_reader = DataReader::new(
            self,
            ino,
            offset,
            read_len as u64,
            prepared.context.pending_content_epoch,
            Some(prepared.resolved_slices.as_ref()),
        );
        let written = data_reader.read_into(buffer).await?;

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
            owner_token,
        } = batch;
        if self.shutdown_requested()
            || owner_token.is_some_and(|token| !self.read_ahead_token_is_current(token))
        {
            self.finish_prefetch_blocks(ino, &block_indices);
            return;
        }

        let result = async {
            let owner_is_current =
                || owner_token.is_none_or(|token| self.read_ahead_token_is_current(token));
            block_indices
                .retain(|object_idx| self.readahead.is_prefetch_relevant(ino, *object_idx));
            if block_indices.is_empty() {
                return Ok(());
            }

            let Some(queued_context) = self
                .resolve_prefetch_batch_context(ino, read_context)
                .await?
            else {
                return Ok(());
            };
            if !owner_is_current() {
                return Ok(());
            }
            if queued_context.size == 0 {
                return Ok(());
            }
            let Some(eof_block) = queued_context.eof_block(self.block_size) else {
                return Ok(());
            };
            block_indices.retain(|object_idx| *object_idx <= eof_block);
            if block_indices.is_empty() {
                return Ok(());
            }

            for (first_block, last_block) in
                Self::contiguous_prefetch_block_ranges(&mut block_indices)
            {
                let block_start = (first_block as u64).saturating_mul(self.block_size as u64);
                let queued_block_end = ((last_block as u64).saturating_add(1))
                    .saturating_mul(self.block_size as u64)
                    .min(queued_context.size);
                let queued_block_len = queued_block_end.saturating_sub(block_start);
                if queued_block_len == 0 {
                    continue;
                }
                let (attr, resolved_slices) = self
                    .resolve_read_metadata_async(ino, block_start, queued_block_len)
                    .await?;
                if !owner_is_current() {
                    return Ok(());
                }
                let context = ReadRequestContext::from_attr(self, ino, attr);
                if context.placeholder_visible(self, ino) {
                    return Ok(());
                }
                if context.pending_content_epoch != queued_context.pending_content_epoch {
                    return Ok(());
                }
                let block_end = queued_block_end.min(context.attr.size);
                let block_len = block_end.saturating_sub(block_start);
                if block_len == 0 {
                    continue;
                }
                let data_reader = DataReader::new(
                    self,
                    ino,
                    block_start,
                    block_len,
                    context.pending_content_epoch,
                    Some(resolved_slices.as_ref()),
                );
                if !owner_is_current() {
                    return Ok(());
                }
                data_reader.prefetch_only().await?;
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

    /// Returns the persisted inode attributes for `ino`, enforcing `ReadOnly` access.
    ///
    /// This is the read-path entrypoint used by metadata callbacks that need a consistent view of
    /// file type, size, timestamps, and related attributes. Callers that only need the current
    /// in-memory attribute immediately after a successful write should prefer
    /// [`peek_dirty_attr`](Self::peek_dirty_attr) to avoid a metadata round-trip.
    pub fn getattr(&self, ino: Ino) -> VfsResult<InodeAttr> {
        let attr = self.getattr_unchecked(ino)?;
        self.enforce_access(ino, AccessLevel::ReadOnly)?;
        Ok(attr)
    }

    /// Returns the in-memory dirty attribute for `ino` without a metadata round-trip.
    ///
    /// After any successful write the dirty-attr map is always populated with an up-to-date
    /// size and mtime. Callers that only need file info immediately after a write should prefer
    /// this over `getattr` to avoid re-running the full `ReadMetadataResolver` on every write
    /// callback. Returns `None` when there is no in-memory dirty state for the inode.
    pub fn peek_dirty_attr(&self, ino: Ino) -> Option<InodeAttr> {
        self.state.namespace.dirty_attrs.lock().get(&ino).cloned()
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
    use super::super::local_dirty::PendingPayload;
    use super::super::*;
    use super::{
        DataReader, MetadataRefreshDecision, ReadClaimBuffers, ReadDataPlanner,
        ReadMetadataResolver, ReadRange, ReadRangeList, ReadSegment, ReadSegmentSource,
        RemoteFetchMode,
    };
    use bytes::{Bytes, BytesMut};
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::Instant;
    use studiofs_cache::local::{
        LocalPayloadBackend, LocalPayloadBackendError, LocalPayloadLifecycleState,
        LocalPayloadObjectStats, LocalPayloadStorageStats,
    };
    use studiofs_meta::schema::{
        CURRENT_FORMAT_VERSION, ChunkId, DEFAULT_BLOCK_SIZE, FormatInfo, MetadataConfig,
        PayloadRefState, ROOT_INO, ResolvedSliceSource, ResolvedSliceSpan, SliceId, SliceOrder,
        SliceRange, SliceRecord,
    };
    use studiofs_meta::types::BlockFlags as MetaBlockFlags;
    use studiofs_meta::{MetaFormat, MetaInodes, MetaPayloadObjects, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn read_range(start: u64, end: u64) -> ReadRange {
        ReadRange::new(start, end).expect("test range is non-empty")
    }

    #[derive(Default)]
    struct RecordingReadPathKernelInvalidator {
        inodes: Mutex<Vec<Ino>>,
    }

    impl RecordingReadPathKernelInvalidator {
        fn snapshot(&self) -> Vec<Ino> {
            self.inodes.lock().expect("invalidator lock").clone()
        }
    }

    impl KernelInvalidator for RecordingReadPathKernelInvalidator {
        fn inval_inode(&self, ino: Ino) {
            self.inodes.lock().expect("invalidator lock").push(ino);
        }
    }

    trait FileGraphMetaFixture {
        fn stage_payload_slice(
            &self,
            ino: Ino,
            block_index: u32,
            payload: &PayloadRef,
        ) -> MetaResult<Option<PayloadRef>>;
    }

    fn fixture_payload_slices(
        block_index: u32,
        payload: &PayloadRef,
    ) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
        let block_start = u64::from(block_index)
            .checked_mul(u64::from(DEFAULT_BLOCK_SIZE))
            .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
        let data_end = block_start
            .checked_add(u64::from(payload.length))
            .ok_or_else(|| MetaError::Storage("fixture data range overflow".to_string()))?;
        let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
        let mut cursor = block_start;
        while cursor < data_end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let piece_end = data_end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let delta = u32::try_from(cursor.saturating_sub(block_start))
                .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
            let mut slice = SliceRecord::new(
                SliceId(0),
                SliceOrder(0),
                range,
                PayloadRef {
                    object_id: payload.object_id,
                    offset: payload.offset.saturating_add(delta),
                    length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                        MetaError::Storage("fixture slice length overflow".to_string())
                    })?,
                    flags: payload.flags,
                },
            );
            slice.payload_state = PayloadRefState::Published;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(chunks.into_iter().collect())
    }

    impl FileGraphMetaFixture for Arc<RedbMetaEngine> {
        fn stage_payload_slice(
            &self,
            ino: Ino,
            block_index: u32,
            payload: &PayloadRef,
        ) -> MetaResult<Option<PayloadRef>> {
            self.flush_regular_file_overlay_slices(
                ino,
                fixture_payload_slices(block_index, payload)?.as_slice(),
            )?;
            self.publish_regular_file_overlay_to_base_if_matches(
                ino,
                self.get_attr(ino)?.content_epoch,
            )?;
            Ok(None)
        }
    }

    struct CountingLocalPayloadBackend {
        inner: Arc<dyn LocalPayloadBackend>,
        get_calls: AtomicU64,
        read_into_calls: AtomicU64,
        advise_will_need_calls: AtomicU64,
    }

    impl CountingLocalPayloadBackend {
        fn new(inner: Arc<dyn LocalPayloadBackend>) -> Self {
            Self {
                inner,
                get_calls: AtomicU64::new(0),
                read_into_calls: AtomicU64::new(0),
                advise_will_need_calls: AtomicU64::new(0),
            }
        }

        fn reset_counters(&self) {
            self.get_calls.store(0, AtomicOrdering::Relaxed);
            self.read_into_calls.store(0, AtomicOrdering::Relaxed);
            self.advise_will_need_calls
                .store(0, AtomicOrdering::Relaxed);
        }

        fn get_calls(&self) -> u64 {
            self.get_calls.load(AtomicOrdering::Relaxed)
        }

        fn read_into_calls(&self) -> u64 {
            self.read_into_calls.load(AtomicOrdering::Relaxed)
        }

        fn advise_will_need_calls(&self) -> u64 {
            self.advise_will_need_calls.load(AtomicOrdering::Relaxed)
        }
    }

    impl LocalPayloadBackend for CountingLocalPayloadBackend {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn get(&self, object_id: u64) -> Option<Bytes> {
            self.get_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get(object_id)
        }

        fn read_into(&self, object_id: u64, data_offset: usize, out: &mut [u8]) -> Option<usize> {
            self.read_into_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.read_into(object_id, data_offset, out)
        }

        fn get_for_upload(&self, object_id: u64) -> Option<Bytes> {
            self.inner.get_for_upload(object_id)
        }

        fn contains(&self, object_id: u64) -> bool {
            self.inner.contains(object_id)
        }

        fn advise_will_need(&self, object_id: u64) {
            self.advise_will_need_calls
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.advise_will_need(object_id);
        }

        fn put(
            &self,
            object_id: u64,
            data: &[u8],
            evictable: bool,
        ) -> Result<(), LocalPayloadBackendError> {
            self.inner.put(object_id, data, evictable)
        }

        fn sync_data(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.sync_data()
        }

        fn run_pressure_maintenance(&self) -> Result<(), LocalPayloadBackendError> {
            self.inner.run_pressure_maintenance()
        }

        fn mark_uploading(
            &self,
            object_id: u64,
        ) -> Result<(), studiofs_cache::local::LocalPayloadBackendError> {
            self.inner.mark_uploading(object_id)
        }

        fn mark_upload_failed(
            &self,
            object_id: u64,
        ) -> Result<(), studiofs_cache::local::LocalPayloadBackendError> {
            self.inner.mark_upload_failed(object_id)
        }

        fn mark_uploaded(
            &self,
            object_id: u64,
        ) -> Result<(), studiofs_cache::local::LocalPayloadBackendError> {
            self.inner.mark_uploaded(object_id)
        }

        fn pin(&self, object_id: u64) {
            self.inner.pin(object_id);
        }

        fn unpin(&self, object_id: u64) {
            self.inner.unpin(object_id);
        }

        fn pin_batch(&self, object_ids: &[u64]) {
            self.inner.pin_batch(object_ids);
        }

        fn unpin_batch(&self, object_ids: &[u64]) {
            self.inner.unpin_batch(object_ids);
        }

        fn remove(
            &self,
            object_id: u64,
        ) -> Result<(), studiofs_cache::local::LocalPayloadBackendError> {
            self.inner.remove(object_id)
        }

        fn remove_batch(
            &self,
            object_ids: &[u64],
        ) -> Result<(), studiofs_cache::local::LocalPayloadBackendError> {
            self.inner.remove_batch(object_ids)
        }

        fn dirty_object_ids(&self) -> Vec<u64> {
            self.inner.dirty_object_ids()
        }

        fn lifecycle_state(&self, object_id: u64) -> LocalPayloadLifecycleState {
            self.inner.lifecycle_state(object_id)
        }

        fn is_evictable(&self, object_id: u64) -> bool {
            self.inner.is_evictable(object_id)
        }

        fn storage_stats(&self) -> LocalPayloadStorageStats {
            self.inner.storage_stats()
        }

        fn object_stats(&self, object_id: u64) -> Option<LocalPayloadObjectStats> {
            self.inner.object_stats(object_id)
        }

        fn object_ids(&self) -> Vec<u64> {
            self.inner.object_ids()
        }
    }

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
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
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
    fn committed_read_uses_upload_stage_when_local_payload_is_unavailable() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "read-upload-stage-fallback");
        let _guard = rt.enter();
        rt.block_on(async {
            let object_id = 8_001;
            let data = Bytes::from_static(b"read from upload stage");
            vfs.local_payload_backend()
                .put(object_id, data.as_ref(), false)
                .expect("persist local payload");
            vfs.cached_store()
                .link_upload_stage_batch(&[object_id])
                .expect("link payload into upload stage");
            vfs.payload_store_remove_async(object_id)
                .await
                .expect("evict primary local payload");

            assert!(
                !vfs.local_payload_backend().contains(object_id),
                "setup must force the committed-read local miss"
            );
            assert!(
                vfs.cached_store().has_staged_upload(object_id),
                "setup must retain the upload-stage copy"
            );

            let payload = PayloadRef {
                object_id,
                offset: 0,
                length: u32::try_from(data.len()).expect("payload length fits u32"),
                flags: MetaBlockFlags::empty(),
            };
            let mut out = vec![0; data.len()];

            DataReader::assemble_committed_payload_into(&vfs, &payload, 0, &mut out)
                .await
                .expect("committed read should resolve the staged payload");

            assert_eq!(out, data);
        });
    }

    #[test]
    fn prefetch_treats_staged_payload_as_local_after_primary_eviction() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "prefetch-upload-stage-local");
        let _guard = rt.enter();
        rt.block_on(async {
            let object_id = 8_002;
            let data = Bytes::from_static(b"prefetch should stay local");
            vfs.local_payload_backend()
                .put(object_id, data.as_ref(), false)
                .expect("persist local payload");
            vfs.cached_store()
                .link_upload_stage_batch(&[object_id])
                .expect("link payload into upload stage");
            vfs.payload_store_remove_async(object_id)
                .await
                .expect("evict primary local payload");

            assert!(
                DataReader::has_local_payload_for_read(&vfs, object_id),
                "prefetch must recognize the durable upload-stage copy after cache eviction"
            );

            let payload = PayloadRef {
                object_id,
                offset: 0,
                length: u32::try_from(data.len()).expect("payload length fits u32"),
                flags: MetaBlockFlags::empty(),
            };
            let segment = ReadSegment {
                range: read_range(0, data.len() as u64),
                source: ReadSegmentSource::Base { payload },
            };
            let s3_gets_before = vfs
                .fetch_scheduler
                .metrics
                .s3_get_calls
                .load(AtomicOrdering::Relaxed);
            DataReader::fetch_remote_blocks(&vfs, &[segment], RemoteFetchMode::BestEffortPrefetch)
                .await
                .expect("staged prefetch should not fail");
            assert_eq!(
                vfs.fetch_scheduler
                    .metrics
                    .s3_get_calls
                    .load(AtomicOrdering::Relaxed),
                s3_gets_before,
                "prefetch must not query S3 for a payload that remains in the upload stage"
            );
        });
    }

    fn mark_all_overlay_objects_uploaded(
        vfs: &Arc<StudioVfs>,
        meta: &Arc<RedbMetaEngine>,
        ino: Ino,
    ) -> (u64, Vec<u64>) {
        let attr = meta.get_attr(ino).expect("load inode attrs");
        let object_ids = meta
            .get_regular_file_overlay_object_ids(ino)
            .expect("load overlay object ids");
        for object_id in &object_ids {
            vfs.local_payload_backend()
                .mark_uploaded(*object_id)
                .expect("mark local payload uploaded");
            assert!(
                meta.mark_regular_file_overlay_object_uploaded_if_matches(
                    ino,
                    attr.content_epoch,
                    *object_id
                )
                .expect("mark overlay object uploaded"),
                "expected metadata upload mark to apply for every overlay object"
            );
        }
        (attr.content_epoch, object_ids)
    }

    fn publish_current_file_graph(
        meta: &Arc<RedbMetaEngine>,
        ino: Ino,
        content_epoch: u64,
        _object_ids: &[u64],
    ) {
        assert!(
            meta.publish_regular_file_overlay_to_base_if_matches(ino, content_epoch)
                .expect("publish current file graph"),
            "expected file graph publish to apply"
        );
    }

    #[test]
    fn collect_data_payload_refs_deduplicates_by_object_id() {
        let first = PayloadRef {
            object_id: 42,
            offset: 0,
            length: 16,
            flags: MetaBlockFlags::empty(),
        };
        let second = PayloadRef {
            object_id: 42,
            offset: 8,
            length: 8,
            flags: MetaBlockFlags::PARTIAL_BLOCK,
        };
        let segments = vec![
            ReadSegment {
                range: read_range(0, 16),
                source: ReadSegmentSource::Base { payload: first },
            },
            ReadSegment {
                range: read_range(16, 24),
                source: ReadSegmentSource::Overlay { payload: second },
            },
            ReadSegment {
                range: read_range(24, 32),
                source: ReadSegmentSource::Zero,
            },
        ];

        let refs = DataReader::collect_data_payload_refs(&segments);
        assert_eq!(refs.len(), 1);
        assert_eq!(refs.get(&42), Some(&first));
    }

    #[test]
    fn claim_unresolved_ranges_keeps_common_fragmented_case_inline() {
        let mut unresolved = smallvec::smallvec![read_range(0, 8), read_range(16, 24),];
        let mut buffers = ReadClaimBuffers::new();

        DataReader::claim_unresolved_ranges(&mut unresolved, read_range(4, 20), &mut buffers);

        assert_eq!(buffers.claimed.len(), 2);
        assert_eq!(buffers.claimed[0].start(), 4);
        assert_eq!(buffers.claimed[0].end(), 8);
        assert_eq!(buffers.claimed[1].start(), 16);
        assert_eq!(buffers.claimed[1].end(), 20);
        assert_eq!(unresolved.len(), 2);
        assert_eq!(unresolved[0].start(), 0);
        assert_eq!(unresolved[0].end(), 4);
        assert_eq!(unresolved[1].start(), 20);
        assert_eq!(unresolved[1].end(), 24);
        assert!(
            !buffers.claimed.spilled(),
            "short claimed read ranges should avoid heap allocation"
        );
        assert!(
            !unresolved.spilled(),
            "short unresolved read ranges should avoid heap allocation"
        );
    }

    #[test]
    fn claim_unresolved_ranges_reuses_spilled_buffers_for_fragmented_reads() {
        let mut unresolved = ReadRangeList::new();
        for i in 0..12 {
            unresolved.push(read_range(i * 10, i * 10 + 5));
        }
        let mut buffers = ReadClaimBuffers::new();

        DataReader::claim_unresolved_ranges(&mut unresolved, read_range(12, 97), &mut buffers);

        assert!(buffers.claimed.spilled());
        assert!(buffers.scratch.spilled());
        let claimed_capacity = buffers.claimed.capacity();
        let scratch_capacity = buffers.scratch.capacity();
        assert_eq!(buffers.claimed.len(), 9);
        assert_eq!(unresolved.len(), 4);

        DataReader::claim_unresolved_ranges(&mut unresolved, read_range(0, 200), &mut buffers);

        assert!(buffers.claimed.capacity() >= claimed_capacity);
        assert!(unresolved.capacity() >= scratch_capacity);
        assert_eq!(buffers.claimed.len(), 4);
        assert!(unresolved.is_empty());
    }

    #[test]
    fn data_planner_builds_metadata_segments_without_vfs_reads() {
        let overlay_payload = PayloadRef {
            object_id: 7,
            offset: 4,
            length: 4,
            flags: MetaBlockFlags::PARTIAL_BLOCK,
        };
        let base_payload = PayloadRef {
            object_id: 8,
            offset: 10,
            length: 4,
            flags: MetaBlockFlags::PARTIAL_BLOCK,
        };
        let resolved = vec![
            ResolvedSliceSpan {
                chunk_id: ChunkId(0),
                range: SliceRange::new(0, 4).expect("overlay range"),
                source: ResolvedSliceSource::Overlay,
                slice_id: Some(SliceId(1)),
                slice_order: Some(SliceOrder(1)),
                payload: Some(overlay_payload),
            },
            ResolvedSliceSpan {
                chunk_id: ChunkId(0),
                range: SliceRange::new(8, 12).expect("base range"),
                source: ResolvedSliceSource::Base,
                slice_id: Some(SliceId(2)),
                slice_order: Some(SliceOrder(2)),
                payload: Some(base_payload),
            },
        ];

        let segments = ReadDataPlanner::new(0, 12)
            .expect("valid planner")
            .claim_resolved_metadata(&resolved)
            .finish();

        assert_eq!(segments.len(), 3);
        assert!(matches!(
            &segments[0].source,
            ReadSegmentSource::Overlay { payload }
                if segments[0].range == read_range(0, 4)
                    && payload.object_id == overlay_payload.object_id
                    && payload.offset == overlay_payload.offset
                    && payload.length == overlay_payload.length
        ));
        assert!(matches!(
            &segments[1].source,
            ReadSegmentSource::Base { payload }
                if segments[1].range == read_range(8, 12)
                    && payload.object_id == base_payload.object_id
                    && payload.offset == base_payload.offset
                    && payload.length == base_payload.length
        ));
        assert!(matches!(&segments[2].source, ReadSegmentSource::Zero));
        assert_eq!(segments[2].range, read_range(4, 8));
    }

    #[test]
    fn metadata_refresh_decision_separates_drained_attrs_from_active_local_state() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "metadata-refresh-decision");
        let _guard = rt.enter();
        let created = vfs
            .create(ROOT_INO, b"refresh-decision.txt", 1000, 1000, 0o644)
            .expect("create file");
        let mut pending_attr = meta.get_attr(created.ino).expect("load attr");
        pending_attr.upload_state = MetaUploadState::Updating;
        pending_attr.content_epoch = pending_attr.committed_content_epoch + 1;

        assert_eq!(
            MetadataRefreshDecision::for_local_attr(
                &pending_attr,
                true,
                false,
                false,
                false,
                false
            ),
            MetadataRefreshDecision::UseLocal
        );
        assert_eq!(
            MetadataRefreshDecision::for_local_attr(
                &pending_attr,
                false,
                false,
                false,
                false,
                false
            ),
            MetadataRefreshDecision::RefreshAuthoritative
        );
        assert_eq!(
            MetadataRefreshDecision::for_local_attr(
                &pending_attr,
                false,
                false,
                false,
                false,
                true
            ),
            MetadataRefreshDecision::UseLocal
        );
    }

    #[test]
    fn resolve_read_metadata_async_prefers_dirty_attr_over_cached_attr() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn resolve_read_metadata_async_reuses_published_read_plan_cache_for_hot_window() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "read-metadata-plan-cache");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"published-read-plan-cache.txt", 1000, 1000, 0o644)
                .expect("create file");
            let ino = created.ino;
            let payload = vec![0x4A_u8; 512 * 1024];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);
            vfs.state.namespace.dirty_attrs.lock().remove(&ino);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let (_, first_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 64 * 1024)
                .await
                .expect("resolve first read");
            let (_, second_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve second read");

            assert!(
                Arc::ptr_eq(&first_slices, &second_slices),
                "sequential reads inside the widened published window should reuse the cached slice plan"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_keeps_multiple_published_read_plan_windows_per_inode() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "read-metadata-plan-cache-windows");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"published-read-plan-cache-windows.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let block_size = vfs.block_size as u64;
            let read_len = block_size.min(128 * 1024);
            let second_window_start =
                block_size.saturating_mul(PUBLISHED_READ_PLAN_CACHE_WINDOW_BLOCKS + 1);
            let payload = vec![0x4B_u8; (second_window_start + read_len) as usize];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);
            vfs.state.namespace.dirty_attrs.lock().remove(&ino);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let (_, first_window_slices) = vfs
                .resolve_read_metadata_async(ino, 0, read_len)
                .await
                .expect("resolve first window");
            let (_, second_window_slices) = vfs
                .resolve_read_metadata_async(ino, second_window_start, read_len)
                .await
                .expect("resolve second window");
            let (_, first_window_again_slices) = vfs
                .resolve_read_metadata_async(ino, read_len, read_len)
                .await
                .expect("resolve first window again");

            assert!(
                !Arc::ptr_eq(&first_window_slices, &second_window_slices),
                "disjoint hot windows should have separate resolved slice plans"
            );
            assert!(
                Arc::ptr_eq(&first_window_slices, &first_window_again_slices),
                "adding another hot window for the same inode must not evict the first window"
            );
        });
    }

    #[test]
    fn metadata_invalidation_clears_published_read_plan_cache_without_attr_change() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "read-metadata-plan-cache-invalidate");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"published-read-plan-cache-invalidate.txt", 1000, 1000, 0o644)
                .expect("create file");
            let ino = created.ino;
            let payload = b"prefix payload";

            vfs.write(ino, 0, payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);

            let tail_start = 3_145_900_u64;
            let tail_len = 26_u32;
            let mut attr = meta.get_attr(ino).expect("load published attr");
            attr.size = tail_start + u64::from(tail_len);
            attr.committed_size = attr.size;
            attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&attr).expect("extend logical EOF");
            vfs.state.namespace.dirty_attrs.lock().remove(&ino);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let (_, stale_slices) = vfs
                .resolve_read_metadata_async(ino, tail_start, u64::from(tail_len))
                .await
                .expect("resolve stale tail hole");
            assert!(
                stale_slices.iter().all(|span| span.payload.is_none()),
                "setup should cache the old hole-only tail plan: {stale_slices:?}"
            );

            let tail_payload = PayloadRef::new_partial(93_301, tail_len);
            meta.apply_replica_aux_state(&[studiofs_meta::events::MetaEvent::PublishRegularFileBaseSlices {
                ino,
                chunk_slices: vec![(
                    ChunkId(0),
                    vec![SliceRecord {
                        slice_id: SliceId(0),
                        order: SliceOrder(0),
                        range: SliceRange::new(tail_start as u32, tail_start as u32 + tail_len)
                            .expect("tail range fits in chunk"),
                        payload: Some(tail_payload),
                        payload_state: PayloadRefState::Published,
                    }],
                )],
            }])
            .expect("apply foreign base graph update");

            vfs.invalidate_attr_read_cache([ino]);
            let (_, fresh_slices) = vfs
                .resolve_read_metadata_async(ino, tail_start, u64::from(tail_len))
                .await
                .expect("resolve fresh tail payload");
            assert!(
                fresh_slices
                    .iter()
                    .any(|span| span.payload.as_ref().map(|payload| payload.object_id)
                        == Some(tail_payload.object_id)),
                "metadata invalidation should drop the stale published read-plan cache: {fresh_slices:?}"
            );
        });
    }

    #[test]
    fn local_write_clears_stale_published_read_plan_cache() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "read-metadata-plan-cache-write");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"published-read-plan-cache-write.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let tail_start = 64 * 1024_u64;
            let tail_payload = b"tail payload after cached hole";

            let mut attr = meta.get_attr(ino).expect("load attr");
            attr.size = tail_start + tail_payload.len() as u64;
            attr.committed_size = attr.size;
            attr.upload_state = MetaUploadState::Ready;
            attr.content_epoch = 7;
            attr.committed_content_epoch = 7;
            meta.set_attr(&attr).expect("publish sparse hole attr");
            vfs.state.namespace.dirty_attrs.lock().remove(&ino);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let (_, stale_slices) = vfs
                .resolve_read_metadata_async(ino, tail_start, tail_payload.len() as u64)
                .await
                .expect("resolve stale hole");
            assert!(
                stale_slices.iter().all(|span| span.payload.is_none()),
                "setup should cache the hole-only read plan: {stale_slices:?}"
            );
            assert!(
                vfs.state
                    .read
                    .published_read_plan_cache
                    .lock()
                    .contains(&ino),
                "setup should leave a published read-plan cache entry"
            );

            vfs.write_with_local_sync_policy(
                ino,
                tail_start,
                tail_payload,
                LocalSyncPolicy::Strict,
            )
            .await
            .expect("write tail payload");

            assert!(
                !vfs.state
                    .read
                    .published_read_plan_cache
                    .lock()
                    .contains(&ino),
                "local writes must evict cached hole read plans for the same inode"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_reuses_dirty_attr_read_plan_when_local_dirty_is_drained() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "read-metadata-dirty-plan-cache");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"dirty-read-plan-cache.txt", 1000, 1000, 0o644)
                .expect("create file");
            let ino = created.ino;
            let payload = vec![0x51_u8; 512 * 1024];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let mut dirty_attr = meta.get_attr(ino).expect("load published attr");
            dirty_attr.upload_state = MetaUploadState::Updating;
            dirty_attr.content_epoch = dirty_attr.content_epoch.saturating_add(1);
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, dirty_attr);

            assert!(
                !vfs.has_pending_local_dirty_payloads_for_inode(ino),
                "setup should leave no pending local payloads after flush"
            );
            let (_, first_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 64 * 1024)
                .await
                .expect("resolve first read");
            let (_, second_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve second read");

            assert!(
                Arc::ptr_eq(&first_slices, &second_slices),
                "dirty attrs with drained local payload state should reuse the widened slice plan"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_reuses_dirty_attr_read_plan_with_pending_local_payloads() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) =
            make_unstarted_test_vfs(&tmp, "read-metadata-pending-local-plan-cache");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"pending-local-read-plan-cache.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let payload = vec![0x52_u8; 512 * 1024];

            vfs.write_with_local_sync_policy(ino, 0, &payload, LocalSyncPolicy::Opportunistic)
                .await
                .expect("write payload");

            assert!(
                vfs.has_pending_local_dirty_payloads_for_inode(ino),
                "setup should keep local payloads pending materialization"
            );
            assert!(
                !vfs.inode_has_dirty_writer_stack(ino),
                "flushed payloads should not leave active writer-stack state"
            );

            let (_, first_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 64 * 1024)
                .await
                .expect("resolve first read");
            let (_, second_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve second read");

            assert!(
                Arc::ptr_eq(&first_slices, &second_slices),
                "dirty attrs with pending local payloads should reuse the widened slice plan"
            );
        });
    }

    #[test]
    fn ready_remote_attr_supersedes_matching_local_dirty_read_state() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "ready-remote-supersedes-local-read-state");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"ready-remote-supersedes-local-read-state.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let published_payload = b"published winner";

            vfs.write(ino, 0, published_payload)
                .await
                .expect("write published payload");
            vfs.flush(ino).await.expect("flush published payload");
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
            vfs.cancel_pending_uploads_for_inode(ino);

            let published_attr = meta.get_attr(ino).expect("load published attr");
            assert_eq!(published_attr.upload_state, MetaUploadState::Ready);
            assert_eq!(
                published_attr.content_epoch,
                published_attr.committed_content_epoch
            );

            let mut stale_dirty_attr = published_attr.clone();
            stale_dirty_attr.size = 5;
            stale_dirty_attr.upload_state = MetaUploadState::Updating;
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, stale_dirty_attr.clone());
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                stale_dirty_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );
            vfs.state.local_dirty.pending_payloads.lock().insert(
                (ino, 0),
                PendingPayload {
                    data: BytesMut::from(&b"stale"[..]),
                    dirty: true,
                    reserved_object_id: None,
                    persisted_len: None,
                },
            );
            vfs.state
                .local_dirty
                .pending_payload_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(0);
            vfs.state
                .local_dirty
                .pending_bytes
                .fetch_add(5, AtomicOrdering::Relaxed);

            vfs.refresh_inode_incarnation_state_from_attr(ino, &published_attr);

            assert!(
                !vfs.has_pending_local_dirty_payloads_for_inode(ino),
                "authoritative ready attr should clear same-epoch stale local payloads"
            );
            assert!(
                !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
                "authoritative ready attr should clear same-epoch dirty attrs"
            );
            assert!(
                !vfs.state.namespace.attr_read_cache.lock().contains(ino),
                "authoritative ready attr should drop cached stale attrs"
            );

            let read = vfs
                .read(ino, 0, published_payload.len() as u32)
                .await
                .expect("read published winner");
            assert_eq!(read.as_ref(), published_payload);
        });
    }

    #[test]
    fn ready_remote_attr_preserves_newer_local_dirty_read_state() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "ready-remote-preserves-newer-local-read-state");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"ready-remote-preserves-newer-local-read-state.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let published_payload = b"published winner";

            vfs.write(ino, 0, published_payload)
                .await
                .expect("write published payload");
            vfs.flush(ino).await.expect("flush published payload");
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
            vfs.cancel_pending_uploads_for_inode(ino);

            let published_attr = meta.get_attr(ino).expect("load published attr");
            let mut newer_dirty_attr = published_attr.clone();
            newer_dirty_attr.size = 5;
            newer_dirty_attr.upload_state = MetaUploadState::Updating;
            newer_dirty_attr.content_epoch = published_attr.committed_content_epoch + 1;
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, newer_dirty_attr.clone());
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                newer_dirty_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );
            vfs.state.local_dirty.pending_payloads.lock().insert(
                (ino, 0),
                PendingPayload {
                    data: BytesMut::from(&b"local"[..]),
                    dirty: true,
                    reserved_object_id: None,
                    persisted_len: None,
                },
            );
            vfs.state
                .local_dirty
                .pending_payload_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(0);
            vfs.state
                .local_dirty
                .pending_bytes
                .fetch_add(5, AtomicOrdering::Relaxed);

            vfs.refresh_inode_incarnation_state_from_attr(ino, &published_attr);

            assert!(
                vfs.has_pending_local_dirty_payloads_for_inode(ino),
                "ready metadata must not clear newer local payloads"
            );
            assert!(
                vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
                "ready metadata must not clear newer dirty attrs"
            );
            assert!(
                vfs.state.namespace.attr_read_cache.lock().contains(ino),
                "ready metadata must not drop newer cached attrs"
            );
        });
    }

    #[test]
    fn ready_remote_attr_supersedes_newer_dirty_attr_after_local_payloads_drain() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "ready-remote-supersedes-drained-dirty-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"ready-remote-supersedes-drained-dirty-attr.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let published_payload = b"published winner";

            vfs.write(ino, 0, published_payload)
                .await
                .expect("write published payload");
            vfs.flush(ino).await.expect("flush published payload");
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
            vfs.cancel_pending_uploads_for_inode(ino);

            let published_attr = meta.get_attr(ino).expect("load published attr");
            let mut drained_dirty_attr = published_attr.clone();
            drained_dirty_attr.size = 5;
            drained_dirty_attr.upload_state = MetaUploadState::Updating;
            drained_dirty_attr.content_epoch = published_attr.committed_content_epoch + 1;
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, drained_dirty_attr.clone());
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                drained_dirty_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );

            assert!(
                !vfs.has_pending_local_dirty_payloads_for_inode(ino),
                "setup should model a dirty attr whose local payloads already drained"
            );
            assert!(
                vfs.pending_uploads_for_inode(ino).is_empty(),
                "setup should have no pending uploads"
            );
            assert!(
                !vfs.live_extent_records_for_inode(ino).is_empty(),
                "setup should keep stale live extents before authoritative ready refresh"
            );
            assert!(
                !vfs.state.writer_stack.has_dirty_writers(ino),
                "setup should have no dirty writer-stack state"
            );
            assert!(
                vfs.state
                    .upload
                    .pending_epoch_finalizations
                    .lock()
                    .iter()
                    .any(|pending| pending.0 == ino),
                "setup should keep stale pending finalization before authoritative ready refresh"
            );

            vfs.refresh_inode_incarnation_state_from_attr(ino, &published_attr);

            assert!(
                !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
                "ready metadata should clear newer dirty attrs once local payloads are drained"
            );
            assert!(
                !vfs.state.namespace.attr_read_cache.lock().contains(ino),
                "ready metadata should drop cached drained dirty attrs"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_refreshes_drained_dirty_attr_from_ready_meta() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "resolve-refreshes-drained-dirty-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"resolve-refreshes-drained-dirty-attr.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let published_payload = b"published winner";

            vfs.write(ino, 0, published_payload)
                .await
                .expect("write published payload");
            vfs.flush(ino).await.expect("flush published payload");
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
            vfs.cancel_pending_uploads_for_inode(ino);

            let published_attr = meta.get_attr(ino).expect("load published attr");
            let mut drained_dirty_attr = published_attr.clone();
            drained_dirty_attr.size = 5;
            drained_dirty_attr.upload_state = MetaUploadState::Updating;
            drained_dirty_attr.content_epoch = published_attr.committed_content_epoch + 1;
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, drained_dirty_attr.clone());
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                drained_dirty_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );
            let invalidator = Arc::new(RecordingReadPathKernelInvalidator::default());
            vfs.set_kernel_invalidator(invalidator.clone());

            let (resolved_attr, _) = vfs
                .resolve_read_metadata_async(ino, 0, published_payload.len() as u64)
                .await
                .expect("resolve published metadata");

            assert_eq!(
                resolved_attr.size, published_attr.size,
                "read resolution must refresh drained dirty attrs from authoritative ready metadata"
            );
            assert!(
                !vfs.state.namespace.dirty_attrs.lock().contains_key(&ino),
                "refresh should clear stale dirty attrs before serving the read"
            );
            assert_eq!(
                invalidator.snapshot(),
                Vec::<Ino>::new(),
                "read-path refresh must not call WinFSP kernel invalidation while a mounted syscall is in flight"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_refreshes_drained_cached_pending_attr_from_ready_meta() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "resolve-refreshes-drained-cached-pending-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"resolve-refreshes-drained-cached-pending-attr.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let published_payload = b"published winner";

            vfs.write(ino, 0, published_payload)
                .await
                .expect("write published payload");
            vfs.flush(ino).await.expect("flush published payload");
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, published_epoch, &published_blocks);
            vfs.cancel_pending_uploads_for_inode(ino);

            let published_attr = meta.get_attr(ino).expect("load published attr");
            let mut stale_cached_attr = published_attr.clone();
            stale_cached_attr.size = 5;
            stale_cached_attr.upload_state = MetaUploadState::Updating;
            stale_cached_attr.content_epoch = published_attr.committed_content_epoch + 1;
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                stale_cached_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );
            let invalidator = Arc::new(RecordingReadPathKernelInvalidator::default());
            vfs.set_kernel_invalidator(invalidator.clone());

            let (resolved_attr, _) = vfs
                .resolve_read_metadata_async(ino, 0, published_payload.len() as u64)
                .await
                .expect("resolve published metadata");

            assert_eq!(
                resolved_attr.size, published_attr.size,
                "read resolution must refresh drained cached pending attrs from authoritative ready metadata"
            );
            assert_eq!(
                vfs.state
                    .namespace
                    .attr_read_cache
                    .lock()
                    .get(ino, Instant::now())
                    .expect("cached attr should be repopulated from ready metadata")
                    .size,
                published_attr.size,
                "stale pending attr cache should be replaced with authoritative ready metadata"
            );
            assert_eq!(
                invalidator.snapshot(),
                Vec::<Ino>::new(),
                "read-path cache refresh must not call WinFSP kernel invalidation while a mounted syscall is in flight"
            );
        });
    }

    #[test]
    fn cached_attr_async_refreshes_drained_cached_attr_from_unpublished_meta() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "resolve-refreshes-drained-unpublished-cached-attr");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"resolve-refreshes-drained-unpublished-cached-attr.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let mut remote_attr = meta.get_attr(ino).expect("load remote attr");
            remote_attr.size = 0;
            remote_attr.upload_state = MetaUploadState::Updating;
            remote_attr.content_epoch = remote_attr.committed_content_epoch + 1;
            meta.set_attr(&remote_attr)
                .expect("persist unpublished remote attr");

            let mut stale_cached_attr = remote_attr.clone();
            stale_cached_attr.size = 5;
            vfs.state.namespace.attr_read_cache.lock().put(
                ino,
                stale_cached_attr,
                ATTR_CACHE_TTL,
                Instant::now(),
            );

            let resolved_attr = ReadMetadataResolver::new(&vfs, ino)
                .cached_attr_async()
                .await
                .expect("resolve cached attr")
                .expect("cached attr");

            assert_eq!(
                resolved_attr.size, remote_attr.size,
                "a drained local cache must not keep a private unpublished payload after restart"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_reuses_dirty_attr_read_plan_when_local_dirty_exists() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) =
            make_unstarted_test_vfs(&tmp, "read-metadata-dirty-plan-cache-bypass");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"dirty-read-plan-cache-bypass.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let payload = vec![0x52_u8; 512 * 1024];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let mut dirty_attr = meta.get_attr(ino).expect("load published attr");
            dirty_attr.upload_state = MetaUploadState::Updating;
            dirty_attr.content_epoch = dirty_attr.content_epoch.saturating_add(1);
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, dirty_attr);

            let (_, cached_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 64 * 1024)
                .await
                .expect("resolve cached read plan");

            vfs.state.local_dirty.pending_payloads.lock().insert(
                (ino, 1),
                PendingPayload {
                    data: BytesMut::from(&payload[64 * 1024..128 * 1024]),
                    dirty: true,
                    reserved_object_id: None,
                    persisted_len: None,
                },
            );
            vfs.state
                .local_dirty
                .pending_payload_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(1);
            vfs.state
                .local_dirty
                .pending_bytes
                .fetch_add(64 * 1024, AtomicOrdering::Relaxed);

            let (_, local_dirty_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve while local dirty state is active");

            assert!(
                Arc::ptr_eq(&cached_slices, &local_dirty_slices),
                "active local dirty payloads should reuse the dirty-attr read-plan cache"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_bypasses_dirty_attr_read_plan_when_writer_stack_exists() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "read-metadata-writer-stack-plan-cache-bypass");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"writer-stack-read-plan-cache-bypass.txt",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let ino = created.ino;
            let payload = vec![0x53_u8; 512 * 1024];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino).await.expect("flush durable local payload");

            let (content_epoch, blocks) = mark_all_overlay_objects_uploaded(&vfs, &meta, ino);
            publish_current_file_graph(&meta, ino, content_epoch, &blocks);
            vfs.state.namespace.attr_read_cache.lock().remove(ino);

            let mut dirty_attr = meta.get_attr(ino).expect("load published attr");
            dirty_attr.upload_state = MetaUploadState::Updating;
            dirty_attr.content_epoch = dirty_attr.content_epoch.saturating_add(1);
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, dirty_attr.clone());

            let (_, cached_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 64 * 1024)
                .await
                .expect("resolve cached read plan");

            let fh = vfs
                .open_file(ino, OpenFlags::RDWR, false)
                .expect("open file");
            vfs.write_fh(fh, ino, 64 * 1024, b"writer-stack-dirty")
                .await
                .expect("stage writer-stack dirty bytes");

            dirty_attr.size = dirty_attr.size.max(64 * 1024 + 18);
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, dirty_attr);

            assert!(
                vfs.state.writer_stack.has_dirty_writers(ino),
                "setup should leave active writer-stack state"
            );

            let (_, bypassed_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve while writer-stack state is active");

            assert!(
                !Arc::ptr_eq(&cached_slices, &bypassed_slices),
                "active writer-stack state must bypass the dirty-attr read-plan cache"
            );

            vfs.release_fh(fh).await.expect("release file handle");
        });
    }

    #[test]
    fn getattr_unchecked_prefers_dirty_attr_over_cached_attr() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn cache_attr_skips_deleted_inode_tombstones() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "cache-attr-deleted");
        let ino = 9_201;
        let mut attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
        attr.upload_state = MetaUploadState::Updating;
        attr.content_epoch = attr.committed_content_epoch + 1;

        vfs.mark_inode_deleted_for_uploads(ino);
        ReadMetadataResolver::new(&vfs, ino).cache_attr(&attr);

        assert!(
            !vfs.state.namespace.attr_read_cache.lock().contains(ino),
            "deleted inode tombstones must reject late attr-cache fills"
        );
    }

    #[test]
    fn getattr_unchecked_rejects_cached_pending_attr_after_inode_delete() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, meta) = make_test_vfs(&tmp, "getattr-stale-cache-delete");
        let created = vfs
            .create(
                ROOT_INO,
                b"getattr-stale-cache-delete.txt",
                1000,
                1000,
                0o644,
            )
            .expect("create file");
        let ino = created.ino;
        let mut cached_attr = meta.get_attr(ino).expect("load attr");
        cached_attr.upload_state = MetaUploadState::Updating;
        cached_attr.content_epoch = cached_attr.committed_content_epoch + 1;
        vfs.state.namespace.attr_read_cache.lock().put(
            ino,
            cached_attr,
            ATTR_CACHE_TTL,
            Instant::now(),
        );
        meta.remove_attr(ino).expect("remove inode attr");

        let err = vfs
            .getattr_unchecked(ino)
            .expect_err("stale pending cache should not hide deleted metadata");

        assert!(matches!(
            err,
            VfsError::Meta(MetaError::InodeNotFound(found)) if found == ino
        ));
        assert!(!vfs.state.namespace.attr_read_cache.lock().contains(ino));
    }

    #[test]
    fn resolve_read_metadata_async_clears_stale_runtime_state_when_inode_is_missing() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "read-metadata-stale-cache-missing-ino");
        let _guard = rt.enter();
        rt.block_on(async {
            let ino = 9_001;
            let mut cached_attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
            cached_attr.size = 5;
            vfs.state.local_dirty.pending_payloads.lock().insert(
                (ino, 0),
                PendingPayload {
                    data: BytesMut::from(&b"stale"[..]),
                    dirty: true,
                    reserved_object_id: None,
                    persisted_len: None,
                },
            );
            vfs.state
                .local_dirty
                .pending_payload_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(0);
            vfs.state
                .local_dirty
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
                    .local_dirty
                    .pending_payloads
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
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (_rt, vfs, _meta) = make_test_vfs(&tmp, "getattr-stale-runtime-missing-ino");
        let ino = 9_101;
        let mut cached_attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
        cached_attr.size = 5;
        vfs.state.local_dirty.pending_payloads.lock().insert(
            (ino, 0),
            PendingPayload {
                data: BytesMut::from(&b"stale"[..]),
                dirty: true,
                reserved_object_id: None,
                persisted_len: None,
            },
        );
        vfs.state
            .local_dirty
            .pending_payload_indexes
            .lock()
            .entry(ino)
            .or_default()
            .insert(0);
        vfs.state
            .local_dirty
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
                .local_dirty
                .pending_payloads
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
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
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
    fn committed_local_cache_hit_uses_direct_backend_read_path() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, mut vfs, _meta) = make_unstarted_test_vfs(&tmp, "assemble-read-plan-written");
        let inner = vfs.local_payload_backend.clone();
        let counting_backend = Arc::new(CountingLocalPayloadBackend::new(inner));
        Arc::get_mut(&mut vfs)
            .expect("unstarted test VFS should be uniquely owned")
            .local_payload_backend = counting_backend.clone();
        rt.block_on(async {
            let object_id = 0xCC77;
            vfs.payload_store_put_async(object_id, Bytes::from_static(b"0123456789"), false)
                .await
                .expect("payload store put");

            let mut payload = PayloadRef::new_full(object_id, 10);
            payload.offset = 2;
            payload.length = 4;
            let segments = vec![ReadSegment {
                range: read_range(0, 4),
                source: ReadSegmentSource::Base { payload },
            }];
            let mut buffer = [0_u8; 4];
            counting_backend.reset_counters();
            let written = DataReader::assemble_into(&vfs, 0, &segments, &mut buffer)
                .await
                .expect("assemble data reader output");
            assert_eq!(written, 4);
            assert_eq!(&buffer, b"2345");
            assert_eq!(
                counting_backend.get_calls(),
                0,
                "committed local hits should stay on direct partial read_into()"
            );
            assert!(
                counting_backend.read_into_calls() > 0,
                "committed local hits should use direct partial read_into()"
            );
        });
    }

    #[test]
    fn committed_adjacent_local_segments_use_one_backend_read() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, mut vfs, _meta) = make_unstarted_test_vfs(&tmp, "assemble-adjacent-read-plan");
        let inner = vfs.local_payload_backend.clone();
        let counting_backend = Arc::new(CountingLocalPayloadBackend::new(inner));
        Arc::get_mut(&mut vfs)
            .expect("unstarted test VFS should be uniquely owned")
            .local_payload_backend = counting_backend.clone();
        rt.block_on(async {
            let object_id = 0xACED;
            vfs.payload_store_put_async(object_id, Bytes::from_static(b"0123456789"), false)
                .await
                .expect("payload store put");

            let mut first = PayloadRef::new_full(object_id, 10);
            first.offset = 2;
            first.length = 2;
            let mut second = PayloadRef::new_full(object_id, 10);
            second.offset = 4;
            second.length = 3;
            let segments = vec![
                ReadSegment {
                    range: read_range(0, 2),
                    source: ReadSegmentSource::Base { payload: first },
                },
                ReadSegment {
                    range: read_range(2, 5),
                    source: ReadSegmentSource::Base { payload: second },
                },
            ];
            let mut buffer = [0_u8; 5];
            counting_backend.reset_counters();
            let written = DataReader::assemble_into(&vfs, 0, &segments, &mut buffer)
                .await
                .expect("assemble data reader output");

            assert_eq!(written, 5);
            assert_eq!(&buffer, b"23456");
            assert_eq!(
                counting_backend.get_calls(),
                0,
                "adjacent committed local hits should stay on direct partial read_into()"
            );
            assert_eq!(
                counting_backend.read_into_calls(),
                1,
                "adjacent committed local hits for the same object should coalesce into one backend read"
            );
        });
    }

    #[test]
    fn data_reader_prefers_local_unpublished_bytes_over_published_base() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "data-reader-dirty-over-base");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"data-reader-dirty-over-base.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let object_id = 0xDADA;
            vfs.payload_store_put_async(object_id, Bytes::from_static(b"abcdefgh"), false)
                .await
                .expect("seed committed payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_id, 8))
                .expect("persist committed payload slice");

            vfs.write(ino, 2, b"XY").await.expect("stage dirty patch");

            let (resolved_attr, resolved_slices) = vfs
                .resolve_read_metadata_async(ino, 0, 8)
                .await
                .expect("resolve read metadata");
            let read_context = ReadRequestContext::from_attr(&vfs, ino, resolved_attr);
            let mut buffer = [0_u8; 8];

            let written = DataReader::new(
                &vfs,
                ino,
                0,
                8,
                read_context.pending_content_epoch,
                Some(resolved_slices.as_ref()),
            )
            .read_into(&mut buffer)
            .await
            .expect("read via data reader");

            assert_eq!(written, 8);
            assert_eq!(&buffer, b"abXYefgh");
        });
    }

    #[test]
    fn prepared_pending_extent_read_uses_resolved_slice_snapshot() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "pending-extent-prepared-snapshot");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"pending-extent-prepared-snapshot.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let pending_object_id = 0xAA11;
            let newer_object_id = 0xBB22;

            vfs.payload_store_put_async(pending_object_id, Bytes::from_static(b"pending"), false)
                .await
                .expect("seed pending payload object");
            vfs.payload_store_put_async(newer_object_id, Bytes::from_static(b"newer!!"), false)
                .await
                .expect("seed newer payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 7;
            staged_attr.committed_size = 7;
            staged_attr.upload_state = MetaUploadState::Updating;
            staged_attr.committed_content_epoch = 1;
            staged_attr.content_epoch = 2;
            meta.set_attr(&staged_attr).expect("persist staged attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(pending_object_id, 7))
                .expect("persist prepared pending payload slice");

            let resolved_slices = meta
                .lookup_regular_file_slices(ino, 0, 7)
                .expect("resolve prepared slice snapshot");
            vfs.state.incarnation.live_extent_records.lock().insert(
                (ino, 0),
                crate::vfs::overlay::ExtentRecordId {
                    object_id: pending_object_id,
                    content_epoch: staged_attr.content_epoch,
                },
            );

            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(newer_object_id, 7))
                .expect("mutate live slice graph after preparing read");

            let segments = DataReader::new(
                &vfs,
                ino,
                0,
                7,
                Some(staged_attr.content_epoch),
                Some(resolved_slices.as_slice()),
            )
            .build_segments()
            .await
            .expect("resolve data reader segments");

            assert!(
                segments.iter().any(|segment| matches!(
                    segment.source,
                    ReadSegmentSource::PendingExtent { object_id, .. }
                        if object_id == pending_object_id
                )),
                "prepared pending reads should keep the pending extent classification from their resolved slice snapshot: {segments:?}"
            );
        });
    }

    #[test]
    fn prepared_pending_extent_read_prefers_newer_dirty_payload() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "pending-extent-dirty-over-snapshot");
        rt.block_on(async {
            let attr = vfs
                .create(
                    ROOT_INO,
                    b"pending-extent-dirty-over-snapshot.bin",
                    0,
                    0,
                    0o644,
                )
                .expect("create test file");
            let ino = attr.ino;
            let object_id = 0xDD44;

            vfs.payload_store_put_async(object_id, Bytes::from_static(b"snapshot"), false)
                .await
                .expect("seed pending payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Updating;
            staged_attr.committed_content_epoch = 1;
            staged_attr.content_epoch = 2;
            meta.set_attr(&staged_attr).expect("persist staged attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_id, 8))
                .expect("persist prepared pending payload slice");

            let resolved_slices = meta
                .lookup_regular_file_slices(ino, 0, 8)
                .expect("resolve prepared slice snapshot");
            vfs.state.incarnation.live_extent_records.lock().insert(
                (ino, 0),
                crate::vfs::overlay::ExtentRecordId {
                    object_id,
                    content_epoch: staged_attr.content_epoch,
                },
            );
            vfs.state.local_dirty.pending_payloads.lock().insert(
                (ino, 0),
                PendingPayload {
                    data: BytesMut::from(&b"dirty!!!"[..]),
                    dirty: true,
                    reserved_object_id: None,
                    persisted_len: None,
                },
            );
            vfs.state
                .local_dirty
                .pending_payload_indexes
                .lock()
                .entry(ino)
                .or_default()
                .insert(0);

            let segments = DataReader::new(
                &vfs,
                ino,
                0,
                8,
                Some(staged_attr.content_epoch),
                Some(resolved_slices.as_slice()),
            )
            .build_segments()
            .await
            .expect("resolve data reader segments");

            assert!(
                segments
                    .iter()
                    .any(|segment| matches!(segment.source, ReadSegmentSource::Dirty { .. })),
                "newer dirty payload must stay ahead of prepared pending extents: {segments:?}"
            );
        });
    }

    #[test]
    fn prepared_pending_extent_read_requires_full_requested_coverage() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "pending-extent-prepared-partial");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"pending-extent-prepared-partial.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let object_id = 0xCC33;

            vfs.payload_store_put_async(object_id, Bytes::from_static(b"abcdefgh"), false)
                .await
                .expect("seed pending payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Updating;
            staged_attr.committed_content_epoch = 1;
            staged_attr.content_epoch = 2;
            meta.set_attr(&staged_attr).expect("persist staged attr");
            vfs.state.incarnation.live_extent_records.lock().insert(
                (ino, 0),
                crate::vfs::overlay::ExtentRecordId {
                    object_id,
                    content_epoch: staged_attr.content_epoch,
                },
            );

            let resolved_slices = vec![ResolvedSliceSpan {
                chunk_id: ChunkId(0),
                range: SliceRange::new(0, 4).expect("slice range"),
                source: ResolvedSliceSource::Overlay,
                slice_id: None,
                slice_order: None,
                payload: Some(PayloadRef::new_partial(object_id, 4)),
            }];

            let segments = DataReader::new(
                &vfs,
                ino,
                0,
                8,
                Some(staged_attr.content_epoch),
                Some(resolved_slices.as_slice()),
            )
            .build_segments()
            .await
            .expect("resolve data reader segments");

            assert!(
                !segments.iter().any(|segment| matches!(
                    segment.source,
                    ReadSegmentSource::PendingExtent { object_id: pending, .. }
                        if pending == object_id
                )),
                "partial prepared coverage must not promote the whole block to a pending extent: {segments:?}"
            );
        });
    }

    #[test]
    fn data_reader_zero_fills_sparse_gaps() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "data-reader-zero-fill");
        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"data-reader-zero-fill.bin", 1000, 1000, 0o644)
                .expect("create file");

            vfs.write(created.ino, 4, b"XY")
                .await
                .expect("write sparse payload");

            let (resolved_attr, resolved_slices) = vfs
                .resolve_read_metadata_async(created.ino, 0, 6)
                .await
                .expect("resolve read metadata");
            let read_context = ReadRequestContext::from_attr(&vfs, created.ino, resolved_attr);
            let mut buffer = [0xAB_u8; 6];

            let written = DataReader::new(
                &vfs,
                created.ino,
                0,
                6,
                read_context.pending_content_epoch,
                Some(resolved_slices.as_ref()),
            )
            .read_into(&mut buffer)
            .await
            .expect("read sparse range");

            assert_eq!(written, 6);
            assert_eq!(buffer, [0, 0, 0, 0, b'X', b'Y']);
        });
    }

    #[test]
    fn data_reader_resolution_prefers_local_unpublished_span_over_committed_base_coverage() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "build-read-plan-dirty-over-base");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"dirty-over-base.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let object_id = 0xA11CE;
            vfs.payload_store_put_async(object_id, Bytes::from_static(b"abcdefgh"), false)
                .await
                .expect("seed committed payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_id, 8))
                .expect("persist committed payload slice");

            vfs.write(ino, 2, b"XY").await.expect("stage dirty patch");

            let pending_attr = vfs.getattr(ino).expect("load pending attr");
            let pending_content_epoch = vfs
                .refresh_inode_incarnation_state_from_attr(ino, &pending_attr)
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch);
            let segments = DataReader::new(&vfs, ino, 0, 8, pending_content_epoch, None)
                .build_segments()
                .await
                .expect("resolve data reader segments");

            assert!(segments.iter().any(|segment| {
                matches!(
                    &segment.source,
                    ReadSegmentSource::Dirty { .. }
                        | ReadSegmentSource::PendingOverlay { .. }
                        | ReadSegmentSource::PendingExtent { .. }
                        | ReadSegmentSource::Overlay { .. }
                        if segment.range.start() < 4 && segment.range.end() > 2
                )
            }));
            assert!(!segments.iter().any(|segment| {
                matches!(&segment.source, ReadSegmentSource::Base { .. })
                    && segment.range.start() < 4
                    && segment.range.end() > 2
            }));
        });
    }

    #[test]
    fn data_reader_excludes_writer_stack_claims_from_fallback_segments() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_test_vfs(&tmp, "writer-stack-direct-read-plan");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"writer-stack-direct-read-plan.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let fh = vfs.open_file(ino, OpenFlags::RDWR, false).expect("open file");

            vfs.write_fh_with_local_sync_policy(
                fh,
                ino,
                0,
                b"hello",
                LocalSyncPolicy::Opportunistic,
            )
            .await
            .expect("write through writer stack");
            vfs.clear_pending_payloads_for_inode(ino);

            let pending_attr = vfs.getattr(ino).expect("load pending attr");
            let pending_content_epoch = vfs
                .refresh_inode_incarnation_state_from_attr(ino, &pending_attr)
                .pending_incarnation
                .as_ref()
                .map(|pending| pending.content_epoch);
            let segments = DataReader::new(&vfs, ino, 0, 5, pending_content_epoch, None)
                .build_segments()
                .await
                .expect("resolve data reader segments");

            assert!(
                !segments.iter().any(|segment| {
                    matches!(&segment.source, ReadSegmentSource::Dirty { .. })
                        && segment.range.start() < 5
                        && segment.range.end() > 0
                }),
                "writer-stack bytes should be copied into the caller read buffer before fallback segments are assembled: {segments:?}"
            );

            vfs.release_fh(fh).await.expect("release file");
        });
    }

    #[test]
    fn data_reader_missing_pending_extent_falls_back_to_committed_base() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "missing-pending-extent-fallback");
        rt.block_on(async {
            let attr = vfs
                .create(ROOT_INO, b"pending-extent-fallback.bin", 0, 0, 0o644)
                .expect("create test file");
            let ino = attr.ino;
            let committed_object_id = 0xC0FFEE;
            let missing_pending_object_id = 0xDEADBEEF;

            vfs.payload_store_put_async(
                committed_object_id,
                Bytes::from_static(b"abcdefgh"),
                false,
            )
            .await
            .expect("seed committed payload object");

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = 8;
            staged_attr.committed_size = 8;
            staged_attr.upload_state = MetaUploadState::Updating;
            staged_attr.committed_content_epoch = 1;
            staged_attr.content_epoch = 2;
            meta.set_attr(&staged_attr).expect("persist staged attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(committed_object_id, 8))
                .expect("persist committed payload slice");

            vfs.state.incarnation.live_extent_records.lock().insert(
                (ino, 0),
                crate::vfs::overlay::ExtentRecordId {
                    object_id: missing_pending_object_id,
                    content_epoch: staged_attr.content_epoch,
                },
            );

            let read = vfs.read(ino, 0, 8).await.expect("read committed fallback");
            assert_eq!(
                read.as_ref(),
                b"abcdefgh",
                "a missing local pending extent on another mount must not shadow the committed base view"
            );
        });
    }

    #[test]
    fn materialized_pending_local_read_uses_direct_backend_read_path() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, mut vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-local-read-fast-path");
        let inner = vfs.local_payload_backend.clone();
        let counting_backend = Arc::new(CountingLocalPayloadBackend::new(inner));
        Arc::get_mut(&mut vfs)
            .expect("unstarted test VFS should be uniquely owned")
            .local_payload_backend = counting_backend.clone();

        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"pending-local-read.bin", 1000, 1000, 0o644)
                .expect("create file");
            let payload = vec![0x5Au8; 512 * 1024];

            vfs.write(created.ino, 0, &payload)
                .await
                .expect("write payload");
            vfs.flush(created.ino)
                .await
                .expect("materialize pending local state");

            assert!(
                !vfs.has_pending_local_dirty_payloads_for_inode(created.ino),
                "explicit flush should materialize local pending extents before read"
            );
            assert!(
                !vfs.pending_uploads_for_inode(created.ino).is_empty(),
                "file should remain unpublished so the read stays on the pending incarnation path"
            );
            assert!(
                vfs.inode_incarnation_state(created.ino)
                    .await
                    .expect("load incarnation state")
                    .pending_incarnation
                    .is_some(),
                "newly written file should still expose a pending incarnation locally"
            );

            let pending_object_ids: Vec<_> = vfs
                .pending_uploads_for_inode(created.ino)
                .into_iter()
                .map(|(_, object_id, _)| object_id)
                .collect();
            vfs.upload_hot_remove_batch(&pending_object_ids);

            counting_backend.reset_counters();

            let read = vfs
                .read(created.ino, 0, payload.len() as u32)
                .await
                .expect("read payload");
            assert_eq!(read.as_ref(), payload.as_slice());
        });

        assert_eq!(
            counting_backend.get_calls(),
            0,
            "materialized pending local reads should not fall back to whole-block get()"
        );
        assert!(
            counting_backend.read_into_calls() > 0,
            "materialized pending local reads should use direct partial read_into()"
        );
    }

    #[test]
    fn materialized_pending_local_read_uses_live_extent_records_without_queue_snapshot() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) = make_unstarted_test_vfs(&tmp, "pending-local-read-no-snapshot");

        rt.block_on(async {
            let created = vfs
                .create(ROOT_INO, b"pending-local-read-no-snapshot.bin", 1000, 1000, 0o644)
                .expect("create file");
            let payload = vec![0x6Bu8; 512 * 1024];

            vfs.write(created.ino, 0, &payload)
                .await
                .expect("write payload");
            vfs.flush(created.ino)
                .await
                .expect("materialize pending local state");

            assert!(
                vfs.cached_store().pending_upload_count() > 0,
                "unstarted VFS should retain uploads in the runtime queue"
            );
            assert!(
                !vfs.live_extent_records_for_inode(created.ino).is_empty(),
                "flush should record live extents for the pending incarnation"
            );

            let snapshot_calls_before = vfs
                .cached_store()
                .cache_pipeline_metrics()
                .upload_queue
                .snapshot_pending_calls;

            let read = vfs
                .read(created.ino, 0, payload.len() as u32)
                .await
                .expect("read payload");
            assert_eq!(read.as_ref(), payload.as_slice());

            let snapshot_calls_after = vfs
                .cached_store()
                .cache_pipeline_metrics()
                .upload_queue
                .snapshot_pending_calls;
            assert_eq!(
                snapshot_calls_after, snapshot_calls_before,
                "hot foreground reads backed by live extent records must not clone the pending upload queue"
            );
        });
    }

    #[test]
    fn published_read_clears_stale_live_extents_without_queue_snapshot() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_unstarted_test_vfs(&tmp, "published-read-no-snapshot");

        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"published-read-no-snapshot.bin",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create file");
            let payload = vec![0xC3u8; 256 * 1024];

            vfs.write(created.ino, 0, &payload)
                .await
                .expect("write payload");
            vfs.flush(created.ino)
                .await
                .expect("materialize pending local state");

            for op in vfs.pending_uploads_snapshot() {
                vfs.cached_store().complete_upload(&op);
            }
            let (published_epoch, published_blocks) =
                mark_all_overlay_objects_uploaded(&vfs, &meta, created.ino);
            publish_current_file_graph(&meta, created.ino, published_epoch, &published_blocks);

            assert_eq!(vfs.cached_store().pending_upload_count(), 0);
            assert!(
                !vfs.live_extent_records_for_inode(created.ino).is_empty(),
                "test needs stale live extents to exercise ready-attr cleanup"
            );

            let snapshot_calls_before = vfs
                .cached_store()
                .cache_pipeline_metrics()
                .upload_queue
                .snapshot_pending_calls;

            let read = vfs
                .read(created.ino, 0, payload.len() as u32)
                .await
                .expect("read published payload");
            assert_eq!(read.as_ref(), payload.as_slice());

            let snapshot_calls_after = vfs
                .cached_store()
                .cache_pipeline_metrics()
                .upload_queue
                .snapshot_pending_calls;
            assert_eq!(
                snapshot_calls_after, snapshot_calls_before,
                "published foreground reads must not clone an empty pending upload queue"
            );
            assert!(
                vfs.live_extent_records_for_inode(created.ino).is_empty(),
                "ready metadata should clear stale live extents after upload completion"
            );
        });
    }

    #[test]
    fn foreground_cold_read_starts_multiple_missing_remote_fetches_before_first_completes() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "foreground-cold-read-parallel");
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"foreground-cold-read-parallel.bin",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create test file");
            let ino = created.ino;
            let block_size = u64::from(DEFAULT_BLOCK_SIZE);
            let object_a = 0x000C_01DA;
            let object_b = 0x000C_01DB;

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = block_size + 4;
            staged_attr.committed_size = staged_attr.size;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_a, 4))
                .expect("stage first published payload");
            meta.stage_payload_slice(ino, 1, &PayloadRef::new_partial(object_b, 4))
                .expect("stage second published payload");

            let scheduler = vfs.fetch_scheduler().clone();
            let inflight_a = scheduler.test_insert_inflight_fetch(object_a);
            let inflight_b = scheduler.test_insert_inflight_fetch(object_b);
            let joins_before = scheduler
                .metrics
                .singleflight_joins
                .load(AtomicOrdering::Relaxed);

            let read_task = tokio::spawn({
                let vfs = vfs.clone();
                async move { vfs.read(ino, 0, (block_size + 4) as u32).await }
            });

            tokio::time::timeout(std::time::Duration::from_millis(250), async {
                loop {
                    let joins = scheduler
                        .metrics
                        .singleflight_joins
                        .load(AtomicOrdering::Relaxed);
                    if joins >= joins_before + 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("foreground cold read should start both missing block fetches concurrently");

            vfs.local_payload_backend()
                .put(object_a, b"ABCD", false)
                .expect("seed first fetched payload");
            vfs.local_payload_backend()
                .put(object_b, b"WXYZ", false)
                .expect("seed second fetched payload");
            let _ = inflight_a.send(crate::fetch::BroadcastResult::Success(Bytes::from_static(
                b"ABCD",
            )));
            let _ = inflight_b.send(crate::fetch::BroadcastResult::Success(Bytes::from_static(
                b"WXYZ",
            )));

            let read = read_task
                .await
                .expect("read task should join cleanly")
                .expect("foreground read should complete");
            assert_eq!(&read[..4], b"ABCD");
            assert_eq!(&read[read.len() - 4..], b"WXYZ");
        });
    }

    #[test]
    fn sequential_single_block_read_starts_next_prefetch_before_current_fetch_completes() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, meta) = make_test_vfs(&tmp, "sequential-single-block-prefetch-ahead");
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"sequential-single-block-prefetch-ahead.bin",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create test file");
            let ino = created.ino;
            let block_size = u64::from(DEFAULT_BLOCK_SIZE);
            let object_0 = 0x5001;
            let object_1 = 0x5002;
            let object_2 = 0x5003;

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = block_size.saturating_mul(2).saturating_add(4);
            staged_attr.committed_size = staged_attr.size;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_0, 4))
                .expect("stage first published payload");
            meta.stage_payload_slice(ino, 1, &PayloadRef::new_partial(object_1, 4))
                .expect("stage second published payload");
            meta.stage_payload_slice(ino, 2, &PayloadRef::new_partial(object_2, 4))
                .expect("stage third published payload");

            vfs.payload_store_put_async(object_0, Bytes::from_static(b"AAAA"), false)
                .await
                .expect("seed first local payload");
            let first_read = vfs.read(ino, 0, 4).await.expect("read first block");
            assert_eq!(first_read.as_ref(), b"AAAA");

            let scheduler = vfs.fetch_scheduler().clone();
            let current_fetch = scheduler.test_insert_inflight_fetch(object_1);
            let next_prefetch = scheduler.test_insert_inflight_fetch(object_2);
            let joins_before = scheduler
                .metrics
                .singleflight_joins
                .load(AtomicOrdering::Relaxed);

            let read_task = tokio::spawn({
                let vfs = vfs.clone();
                async move { vfs.read(ino, block_size, 4).await }
            });

            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    let joins = scheduler
                        .metrics
                        .singleflight_joins
                        .load(AtomicOrdering::Relaxed);
                    if joins >= joins_before + 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("sequential read should start next-block prefetch while the current cold fetch is still pending");

            vfs.local_payload_backend()
                .put(object_1, b"BBBB", false)
                .expect("seed current fetched payload");
            vfs.local_payload_backend()
                .put(object_2, b"CCCC", false)
                .expect("seed next prefetched payload");
            let _ = current_fetch.send(crate::fetch::BroadcastResult::Success(
                Bytes::from_static(b"BBBB"),
            ));
            let _ = next_prefetch.send(crate::fetch::BroadcastResult::Success(
                Bytes::from_static(b"CCCC"),
            ));

            let read = read_task
                .await
                .expect("read task should join cleanly")
                .expect("foreground read should complete");
            assert_eq!(read.as_ref(), b"BBBB");
        });
    }

    #[test]
    fn prefetch_batch_resolves_one_snapshot_for_contiguous_pending_extents() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, vfs, _meta) =
            make_unstarted_test_vfs(&tmp, "prefetch-contiguous-pending-snapshot");
        let _guard = rt.enter();
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"prefetch-contiguous-pending-snapshot.bin",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create test file");
            let ino = created.ino;
            let block_size = vfs.block_size as usize;
            let payload = vec![0x6B; block_size.saturating_mul(2).saturating_add(4)];

            vfs.write(ino, 0, &payload).await.expect("write payload");
            vfs.flush(ino)
                .await
                .expect("materialize pending local extents");

            assert_eq!(
                vfs.live_extent_records_for_inode(ino).len(),
                3,
                "setup should materialize three pending extents"
            );
            let attr = vfs.getattr(ino).expect("load pending attr");
            let mut read_context = vfs.prefetch_read_context_from_attr(ino, &attr);
            read_context.size = read_context.size.saturating_add(vfs.block_size as u64);
            vfs.state
                .namespace
                .dirty_attrs
                .lock()
                .insert(ino, attr.clone());
            vfs.state
                .read
                .published_read_plan_cache
                .lock()
                .pop(&ino);

            vfs.read_prefetch_batch(PrefetchBatch::new(
                ino,
                vec![0, 1, 2, 3],
                Some(read_context),
            ))
            .await;

            let key = PublishedReadPlanCacheKey::from_locally_stable_attr(&attr)
                .expect("regular pending attr should be cacheable");
            assert!(
                vfs.state
                    .read
                    .published_read_plan_cache_get(ino, key, 0, attr.size)
                    .is_some(),
                "one contiguous prefetch range should resolve and reuse one canonical slice snapshot"
            );
        });
    }

    #[test]
    fn sequential_prefetch_warms_existing_local_payloads() {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let (rt, mut vfs, meta) =
            make_unstarted_test_vfs(&tmp, "sequential-prefetch-warms-local-payloads");
        let inner = vfs.local_payload_backend.clone();
        let counting_backend = Arc::new(CountingLocalPayloadBackend::new(inner));
        Arc::get_mut(&mut vfs)
            .expect("unstarted test VFS should be uniquely owned")
            .local_payload_backend = counting_backend.clone();
        let _guard = rt.enter();
        vfs.start().expect("start vfs background workers");
        rt.block_on(async {
            let created = vfs
                .create(
                    ROOT_INO,
                    b"sequential-prefetch-warms-local-payloads.bin",
                    1000,
                    1000,
                    0o644,
                )
                .expect("create test file");
            let ino = created.ino;
            let block_size = u64::from(DEFAULT_BLOCK_SIZE);
            let object_0 = 0x5101;
            let object_1 = 0x5102;
            let object_2 = 0x5103;

            let mut staged_attr = meta.get_attr(ino).expect("load staged attr");
            staged_attr.size = block_size.saturating_mul(2).saturating_add(4);
            staged_attr.committed_size = staged_attr.size;
            staged_attr.upload_state = MetaUploadState::Ready;
            meta.set_attr(&staged_attr).expect("persist committed attr");
            meta.stage_payload_slice(ino, 0, &PayloadRef::new_partial(object_0, 4))
                .expect("stage first published payload");
            meta.stage_payload_slice(ino, 1, &PayloadRef::new_partial(object_1, 4))
                .expect("stage second published payload");
            meta.stage_payload_slice(ino, 2, &PayloadRef::new_partial(object_2, 4))
                .expect("stage third published payload");

            vfs.payload_store_put_async(object_0, Bytes::from_static(b"AAAA"), false)
                .await
                .expect("seed first local payload");
            vfs.payload_store_put_async(object_1, Bytes::from_static(b"BBBB"), false)
                .await
                .expect("seed second local payload");
            vfs.payload_store_put_async(object_2, Bytes::from_static(b"CCCC"), false)
                .await
                .expect("seed third local payload");

            let first_read = vfs.read(ino, 0, 4).await.expect("read first block");
            assert_eq!(first_read.as_ref(), b"AAAA");
            let second_read = vfs
                .read(ino, block_size, 4)
                .await
                .expect("read second block");
            assert_eq!(second_read.as_ref(), b"BBBB");

            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                loop {
                    if counting_backend.advise_will_need_calls() > 0 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect(
                "prefetch should warm already-local payloads instead of treating them as a no-op",
            );
        });
    }

    #[test]
    fn foreground_cold_read_concurrency_caps_extreme_aimd_windows() {
        assert_eq!(
            DataReader::foreground_cold_read_concurrency(1024, 512),
            64,
            "foreground cold-prime should not create hundreds of active fetch futures for one oversized read"
        );
    }
}
