use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::events::MetaEvent;

trait SliceNativeBlockFixture {
    fn stage_file_overlay_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>>;
    fn lookup_file_graph_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>>;
    fn lookup_file_graph_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>>;
    fn publish_uploaded_overlay_block(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool>;
    fn mark_overlay_block_uploaded_if_matches(
        &self,
        ino: Ino,
        block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool>;
    fn publish_overlay_graph(&self, ino: Ino, expected_epoch: u64) -> MetaResult<bool>;
}

fn fixture_block_slices(
    block_index: u32,
    block: Option<&BlockRef>,
    state: BlockRefState,
) -> MetaResult<Vec<(ChunkId, Vec<SliceRecord>)>> {
    let block_size = DEFAULT_BLOCK_SIZE;
    let block_start = u64::from(block_index)
        .checked_mul(u64::from(block_size))
        .ok_or_else(|| MetaError::Storage("fixture block offset overflow".to_string()))?;
    let data_len = u64::from(block.map_or(0, |block| block.length));
    let data_end = block_start
        .checked_add(data_len)
        .ok_or_else(|| MetaError::Storage("fixture block data range overflow".to_string()))?;
    let block_end = block_start
        .checked_add(u64::from(block_size))
        .ok_or_else(|| MetaError::Storage("fixture block range overflow".to_string()))?;
    let mut chunks = std::collections::BTreeMap::<ChunkId, Vec<SliceRecord>>::new();
    let mut push = |start: u64, end: u64, source: Option<&BlockRef>| -> MetaResult<()> {
        let mut cursor = start;
        while cursor < end {
            let chunk_id = chunk_id_from_offset(cursor, DEFAULT_CHUNK_SIZE);
            let chunk_start = chunk_start_offset(chunk_id, DEFAULT_CHUNK_SIZE);
            let piece_end = end.min(chunk_start.saturating_add(DEFAULT_CHUNK_SIZE as u64));
            let range = SliceRange::new(
                (cursor - chunk_start) as u32,
                (piece_end - chunk_start) as u32,
            )
            .ok_or_else(|| MetaError::Storage("fixture invalid slice range".to_string()))?;
            let mut slice = if let Some(source) = source {
                let delta = u32::try_from(cursor.saturating_sub(block_start))
                    .map_err(|_| MetaError::Storage("fixture slice delta overflow".to_string()))?;
                SliceRecord::new(
                    SliceId(0),
                    SliceOrder(0),
                    range,
                    BlockRef {
                        object_id: source.object_id,
                        offset: source.offset.saturating_add(delta),
                        length: u32::try_from(piece_end.saturating_sub(cursor)).map_err(|_| {
                            MetaError::Storage("fixture slice length overflow".to_string())
                        })?,
                        flags: source.flags,
                    },
                )
            } else {
                SliceRecord::tombstone(SliceId(0), SliceOrder(0), range)
            };
            slice.block_state = state;
            chunks.entry(chunk_id).or_default().push(slice);
            cursor = piece_end;
        }
        Ok(())
    };
    push(block_start, data_end, block)?;
    if data_end < block_end {
        push(data_end, block_end, None)?;
    }
    Ok(chunks.into_iter().collect())
}

fn fixture_block_from_spans(
    spans: Vec<ResolvedSliceSpan>,
    block_index: u32,
) -> MetaResult<Option<BlockRef>> {
    let block_start = u64::from(block_index) * u64::from(DEFAULT_BLOCK_SIZE);
    let block_end = block_start + u64::from(DEFAULT_BLOCK_SIZE);
    let mut parts = Vec::new();
    for span in spans {
        let Some(block) = span.block else {
            continue;
        };
        let chunk_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE);
        let span_start = chunk_start + u64::from(span.range.start);
        let span_end = chunk_start + u64::from(span.range.end);
        let overlap_start = span_start.max(block_start);
        let overlap_end = span_end.min(block_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let delta = u32::try_from(overlap_start - span_start)
            .map_err(|_| MetaError::Storage("fixture block delta overflow".to_string()))?;
        parts.push((
            overlap_start,
            BlockRef {
                object_id: block.object_id,
                offset: block.offset.saturating_add(delta),
                length: u32::try_from(overlap_end - overlap_start)
                    .map_err(|_| MetaError::Storage("fixture block length overflow".to_string()))?,
                flags: block.flags,
            },
        ));
    }
    parts.sort_by_key(|(offset, _)| *offset);
    let mut merged: Option<BlockRef> = None;
    for (_, part) in parts {
        if let Some(current) = merged.as_mut() {
            if current.object_id != part.object_id
                || current.offset.saturating_add(current.length) != part.offset
            {
                return Err(MetaError::FragmentedBlock {
                    ino: 0,
                    block_index,
                });
            }
            current.length = current.length.saturating_add(part.length);
            current.flags |= part.flags;
        } else {
            merged = Some(part);
        }
    }
    Ok(merged)
}

impl SliceNativeBlockFixture for RedbMetaEngine {
    fn stage_file_overlay_block(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        let old = self
            .lookup_file_graph_block(ino, block_index)
            .ok()
            .flatten();
        self.flush_regular_file_overlay_slices(
            ino,
            fixture_block_slices(block_index, Some(block), BlockRefState::LocalOnly)?.as_slice(),
        )?;
        Ok(old)
    }

    fn lookup_file_graph_block(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        self.lookup_regular_file_slices(
            ino,
            u64::from(block_index) * u64::from(DEFAULT_BLOCK_SIZE),
            u64::from(DEFAULT_BLOCK_SIZE),
        )
        .and_then(|spans| fixture_block_from_spans(spans, block_index))
    }

    fn lookup_file_graph_blocks(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        let attr = self.get_attr(ino)?;
        let blocks = attr.size.div_ceil(u64::from(DEFAULT_BLOCK_SIZE));
        let mut out = Vec::new();
        for idx in 0..blocks {
            let idx = u32::try_from(idx)
                .map_err(|_| MetaError::Storage("fixture block index overflow".to_string()))?;
            if let Some(block) = self.lookup_file_graph_block(ino, idx)? {
                out.push((idx, block));
            }
        }
        Ok(out)
    }

    fn publish_uploaded_overlay_block(
        &self,
        ino: Ino,
        _block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        self.mark_regular_file_overlay_object_uploaded_if_matches(
            ino,
            expected_epoch,
            block.object_id,
        )?;
        self.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
    }

    fn mark_overlay_block_uploaded_if_matches(
        &self,
        ino: Ino,
        _block_index: u32,
        expected_epoch: u64,
        block: &BlockRef,
    ) -> MetaResult<bool> {
        self.mark_regular_file_overlay_object_uploaded_if_matches(
            ino,
            expected_epoch,
            block.object_id,
        )
    }

    fn publish_overlay_graph(&self, ino: Ino, expected_epoch: u64) -> MetaResult<bool> {
        self.publish_regular_file_overlay_to_base_if_matches(ino, expected_epoch)
    }
}

include!("tests/format.rs");
include!("tests/recovery.rs");
include!("tests/replication.rs");
include!("tests/symlink.rs");
include!("tests/xattr.rs");
include!("tests/acl.rs");
include!("tests/snapshots.rs");
include!("tests/inodes.rs");
include!("tests/namespace.rs");
include!("tests/fs_ops.rs");
include!("tests/chunk_state.rs");
