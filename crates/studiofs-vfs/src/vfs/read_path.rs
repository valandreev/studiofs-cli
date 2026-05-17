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
                .or_insert_with(|| payload.clone());
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
        let Some(read_end) = offset.checked_add(length) else {
            return Err(VfsError::Other(format!(
                "read range overflow: ino={ino}, offset={offset}, length={length}"
            )));
        };
        let requested = ReadRange::new(offset, read_end).expect("non-empty checked read range");
        let mut unresolved = smallvec![requested];
        let mut claim_buffers = ReadClaimBuffers::new();
        let mut segments = Vec::new();
        let pending_upload_index = vfs.snapshot_pending_upload_index();

        for claim in writer_stack_claims {
            if unresolved.is_empty() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(claim.start, claim.end) else {
                continue;
            };
            Self::claim_unresolved_ranges(&mut unresolved, dirty_range, &mut claim_buffers);
        }

        let dirty_spans = vfs.snapshot_pending_dirty_slice_read_spans(ino, offset, length);
        for dirty in dirty_spans {
            if unresolved.is_empty() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(dirty.start, dirty.end) else {
                continue;
            };
            Self::claim_unresolved_ranges(&mut unresolved, dirty_range, &mut claim_buffers);
            for overlap in claim_buffers.claimed.iter().copied() {
                let delta = overlap.start().saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                segments.push(ReadSegment {
                    range: overlap,
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
            if unresolved.is_empty() {
                break;
            }
            let Some(dirty_range) = ReadRange::new(dirty.start, dirty.end) else {
                continue;
            };
            Self::claim_unresolved_ranges(&mut unresolved, dirty_range, &mut claim_buffers);
            for overlap in claim_buffers.claimed.iter().copied() {
                let delta = overlap.start().saturating_sub(dirty.start);
                let Some(delta_u32) = u32::try_from(delta).ok() else {
                    continue;
                };
                let Some(block_offset) = dirty.block_offset.checked_add(delta_u32) else {
                    continue;
                };
                segments.push(ReadSegment {
                    range: overlap,
                    source: ReadSegmentSource::Dirty {
                        data: dirty.data.clone(),
                        block_offset: u64::from(block_offset),
                    },
                });
            }
        }

        if !unresolved.is_empty() {
            let mut candidate_blocks = ReadBlockList::new();
            for range in &unresolved {
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
                if unresolved.is_empty() {
                    break;
                }
                let Some(source) = vfs.pending_overlay_block_source(
                    ino,
                    block_index,
                    pending_content_epoch,
                    Some(&pending_upload_index),
                ) else {
                    continue;
                };
                if matches!(
                    &source,
                    PendingOverlayBlockSource::Extent(object_id)
                        if !vfs.local_payload_backend.contains(*object_id)
                ) {
                    continue;
                }
                let block_start = (block_index as u64).saturating_mul(vfs.block_size as u64);
                let block_end = block_start.saturating_add(vfs.block_size as u64);
                Self::claim_unresolved_ranges(
                    &mut unresolved,
                    ReadRange::new(block_start, block_end).expect("block range is non-empty"),
                    &mut claim_buffers,
                );
                for overlap in claim_buffers.claimed.iter().copied() {
                    let delta = overlap.start().saturating_sub(block_start);
                    let Some(block_offset) = u32::try_from(delta).ok() else {
                        continue;
                    };
                    segments.push(ReadSegment {
                        range: overlap,
                        source: match &source {
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
                        },
                    });
                }
            }
        }

        if !unresolved.is_empty() {
            if let Some(resolved) = resolved_regular_file_slices {
                Self::claim_meta_spans_into_segments(
                    resolved.iter(),
                    &mut unresolved,
                    &mut segments,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |payload| ReadSegmentSource::Overlay { payload },
                    &mut claim_buffers,
                );
                Self::claim_meta_spans_into_segments(
                    resolved.iter(),
                    &mut unresolved,
                    &mut segments,
                    requested,
                    ResolvedSliceSource::Base,
                    |payload| ReadSegmentSource::Base { payload },
                    &mut claim_buffers,
                );
            } else {
                let meta = vfs.meta.clone();
                let resolved = run_meta(meta, move |engine: Arc<dyn VfsMeta>| {
                    engine.lookup_regular_file_slices(ino, offset, length)
                })
                .await?;
                Self::claim_meta_spans_into_segments(
                    resolved.iter(),
                    &mut unresolved,
                    &mut segments,
                    requested,
                    ResolvedSliceSource::Overlay,
                    |payload| ReadSegmentSource::Overlay { payload },
                    &mut claim_buffers,
                );
                Self::claim_meta_spans_into_segments(
                    resolved.iter(),
                    &mut unresolved,
                    &mut segments,
                    requested,
                    ResolvedSliceSource::Base,
                    |payload| ReadSegmentSource::Base { payload },
                    &mut claim_buffers,
                );
            }
        }

        for gap in unresolved {
            segments.push(ReadSegment {
                range: gap,
                source: ReadSegmentSource::Zero,
            });
        }
        Ok(segments)
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

    fn claim_meta_spans_into_segments<'b, I, F>(
        resolved: I,
        unresolved: &mut ReadRangeList,
        segments: &mut Vec<ReadSegment>,
        requested: ReadRange,
        source_filter: ResolvedSliceSource,
        source_ctor: F,
        buffers: &mut ReadClaimBuffers,
    ) where
        I: Iterator<Item = &'b ResolvedSliceSpan>,
        F: Fn(PayloadRef) -> ReadSegmentSource + Copy,
    {
        for span in resolved {
            if unresolved.is_empty() {
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
            let Some(clipped) = span_range.intersection(requested) else {
                continue;
            };
            Self::claim_unresolved_ranges(unresolved, clipped, buffers);
            for overlap in buffers.claimed.iter().copied() {
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
                let mut planned_block = block.clone();
                planned_block.offset = block_offset;
                planned_block.length = block_length;
                segments.push(ReadSegment {
                    range: overlap,
                    source: source_ctor(planned_block),
                });
            }
        }
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
                        .or_insert_with(|| payload.clone());
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
        remote_blocks.retain(|object_id, _| !vfs.local_payload_backend.contains(*object_id));
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
            Ok(_) => Ok::<(), VfsError>(()),
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
        for segment in segments {
            let dst_start = segment.range.start().saturating_sub(offset) as usize;
            let dst_end = segment.range.end().saturating_sub(offset) as usize;
            if dst_start >= buffer.len() || dst_end > buffer.len() || dst_start >= dst_end {
                continue;
            }
            let dst = &mut buffer[dst_start..dst_end];
            match &segment.source {
                ReadSegmentSource::Dirty { data, block_offset } => {
                    vfs.copy_payload_into_range(data.as_ref(), *block_offset, dst);
                }
                ReadSegmentSource::PendingOverlay { data, block_offset } => {
                    vfs.copy_payload_into_range(data.as_ref(), u64::from(*block_offset), dst);
                }
                ReadSegmentSource::PendingExtent {
                    object_id,
                    block_offset,
                } => {
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
                        }
                        if !span_resolved {
                            error!(
                                object_id = payload.object_id,
                                file_offset = segment.range.start(),
                                block_offset = payload.offset,
                                requested = dst.len(),
                                read,
                                "data_reader: short local backend read for committed payload object; evicting and refetching"
                            );
                            if let Err(evict_error) =
                                vfs.payload_store_remove_async(payload.object_id).await
                            {
                                warn!(
                                    object_id = payload.object_id,
                                    error = %evict_error,
                                    "data_reader: failed to evict short-read local payload before refetch"
                                );
                            }
                        }
                    }

                    if !span_resolved {
                        match vfs.fetch_block_for_ref_after_local_miss(payload).await {
                            Ok(data) => {
                                vfs.copy_committed_payload_into_range(
                                    data.as_ref(),
                                    payload,
                                    segment.range.start(),
                                    dst,
                                )?;
                            }
                            Err(crate::fetch::FetchError::NotFound { key }) => {
                                warn!(
                                    object_id = payload.object_id,
                                    key,
                                    file_offset = segment.range.start(),
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
                }
                ReadSegmentSource::Zero => dst.fill(0),
            }
            written = written.max(dst_end);
        }
        Ok(written)
    }
}

struct ReadMetadata {
    attr: InodeAttr,
    resolved_slices: Arc<[ResolvedSliceSpan]>,
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
                resolved_slices: Arc::from(Vec::<ResolvedSliceSpan>::new()),
            });
        }
        if let Some(attr) = self.dirty_attr() {
            let resolved_slices = self
                .resolve_slices_for_attr(&attr, offset, buffer_len, true)
                .await?;
            return Ok(ReadMetadata {
                attr,
                resolved_slices,
            });
        }
        if let Some(attr) = self.cached_attr() {
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

        if self
            .vfs
            .has_pending_local_dirty_payloads_for_inode(self.ino)
            || self.vfs.inode_has_dirty_writer_stack(self.ino)
            || !self.vfs.pending_uploads_for_inode(self.ino).is_empty()
        {
            return Some(dirty_attr);
        }
        let dirty_attr_tracks_pending_content = dirty_attr.kind == FileType::RegularFile
            && (dirty_attr.upload_state != MetaUploadState::Ready
                || dirty_attr.content_epoch != dirty_attr.committed_content_epoch);
        if !dirty_attr_tracks_pending_content {
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
                true,
            );
            return Some(remote_attr);
        }

        Some(dirty_attr)
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
        let dirty_attr_cacheable = dirty_attr_present
            && !self
                .vfs
                .has_pending_local_dirty_payloads_for_inode(self.ino)
            && !self.vfs.inode_has_dirty_writer_stack(self.ino);
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
            let mut planned_block = block.clone();
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
        self.enqueue_prefetch_hint(ino, access.stream_id, access.policy, &blocks_needed);
        let data_reader = DataReader::new(
            self,
            ino,
            offset,
            prepared.read_len as u64,
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
        } = batch;
        if self.shutdown_requested() {
            self.finish_prefetch_blocks(ino, &block_indices);
            return;
        }

        let result = async {
            block_indices
                .retain(|object_idx| self.readahead.is_prefetch_relevant(ino, *object_idx));
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
            block_indices.retain(|object_idx| *object_idx <= eof_block);
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
                let data_reader = DataReader::new(
                    self,
                    ino,
                    block_start,
                    block_len,
                    context.pending_content_epoch,
                    None,
                );
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
        DataReader, ReadClaimBuffers, ReadRange, ReadRangeList, ReadSegment, ReadSegmentSource,
    };
    use bytes::{Bytes, BytesMut};
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::Instant;
    use studiofs_cache::local::{
        LocalPayloadBackend, LocalPayloadBackendError, LocalPayloadLifecycleState,
        LocalPayloadObjectStats, LocalPayloadStorageStats,
    };
    use studiofs_meta::schema::{
        CURRENT_FORMAT_VERSION, ChunkId, DEFAULT_BLOCK_SIZE, FormatInfo, MetadataConfig,
        PayloadRefState, ROOT_INO, SliceId, SliceOrder, SliceRange, SliceRecord,
    };
    use studiofs_meta::types::BlockFlags as MetaBlockFlags;
    use studiofs_meta::{MetaFormat, MetaInodes, MetaPayloadObjects, RedbMetaEngine};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn read_range(start: u64, end: u64) -> ReadRange {
        ReadRange::new(start, end).expect("test range is non-empty")
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

        fn mark_uploading(&self, object_id: u64) {
            self.inner.mark_uploading(object_id);
        }

        fn mark_upload_failed(&self, object_id: u64) {
            self.inner.mark_upload_failed(object_id);
        }

        fn mark_uploaded(&self, object_id: u64) {
            self.inner.mark_uploaded(object_id);
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

        fn remove(&self, object_id: u64) {
            self.inner.remove(object_id);
        }

        fn remove_batch(&self, object_ids: &[u64]) {
            self.inner.remove_batch(object_ids);
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
            vfs.local_payload_backend().mark_uploaded(*object_id);
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
                source: ReadSegmentSource::Base {
                    payload: first.clone(),
                },
            },
            ReadSegment {
                range: read_range(16, 24),
                source: ReadSegmentSource::Overlay {
                    payload: second.clone(),
                },
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
    fn resolve_read_metadata_async_reuses_published_read_plan_cache_for_hot_window() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn metadata_invalidation_clears_published_read_plan_cache_without_attr_change() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
                        payload: Some(tail_payload.clone()),
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
                    .contains_key(&ino),
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
                    .contains_key(&ino),
                "local writes must evict cached hole read plans for the same inode"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_reuses_dirty_attr_read_plan_when_local_dirty_is_drained() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn ready_remote_attr_supersedes_matching_local_dirty_read_state() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        });
    }

    #[test]
    fn resolve_read_metadata_async_bypasses_dirty_attr_read_plan_when_local_dirty_exists() {
        let tmp = tempfile::tempdir().expect("tempdir");
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

            let (_, bypassed_slices) = vfs
                .resolve_read_metadata_async(ino, 64 * 1024, 64 * 1024)
                .await
                .expect("resolve while local dirty state is active");

            assert!(
                !Arc::ptr_eq(&cached_slices, &bypassed_slices),
                "active local dirty payloads must bypass the dirty-attr read-plan cache"
            );
        });
    }

    #[test]
    fn resolve_read_metadata_async_bypasses_dirty_attr_read_plan_when_writer_stack_exists() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn committed_local_cache_hit_uses_direct_backend_read_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn data_reader_prefers_local_unpublished_bytes_over_published_base() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn data_reader_zero_fills_sparse_gaps() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn foreground_cold_read_starts_multiple_missing_remote_fetches_before_first_completes() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
        let tmp = tempfile::tempdir().expect("tempdir");
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
    fn sequential_prefetch_warms_existing_local_payloads() {
        let tmp = tempfile::tempdir().expect("tempdir");
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
