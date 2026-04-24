use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::events::MetaEvent;

trait BlockProjectionFixture {
    fn stage_block_projection(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>>;
    fn lookup_block_projection(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>>;
    fn lookup_block_projections(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>>;
}

fn block_projection_from_spans(
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
                return Err(MetaError::NonContiguousRegularFileGraph {
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

impl BlockProjectionFixture for RedbMetaEngine {
    fn stage_block_projection(
        &self,
        ino: Ino,
        block_index: u32,
        block: &BlockRef,
    ) -> MetaResult<Option<BlockRef>> {
        let old = self
            .lookup_block_projection(ino, block_index)
            .ok()
            .flatten();
        let chunk_slices = Self::chunk_slices_for_block(
            block_index,
            DEFAULT_BLOCK_SIZE,
            Some(block),
            BlockRefState::LocalOnly,
        )?;
        self.flush_regular_file_overlay_slices(ino, chunk_slices.as_slice())?;
        Ok(old)
    }

    fn lookup_block_projection(&self, ino: Ino, block_index: u32) -> MetaResult<Option<BlockRef>> {
        self.lookup_regular_file_slices(
            ino,
            u64::from(block_index) * u64::from(DEFAULT_BLOCK_SIZE),
            u64::from(DEFAULT_BLOCK_SIZE),
        )
        .and_then(|spans| block_projection_from_spans(spans, block_index))
    }

    fn lookup_block_projections(&self, ino: Ino) -> MetaResult<Vec<(u32, BlockRef)>> {
        let attr = self.get_attr(ino)?;
        let blocks = attr.size.div_ceil(u64::from(DEFAULT_BLOCK_SIZE));
        let mut out = Vec::new();
        for idx in 0..blocks {
            let idx = u32::try_from(idx)
                .map_err(|_| MetaError::Storage("fixture block index overflow".to_string()))?;
            if let Some(block) = self.lookup_block_projection(ino, idx)? {
                out.push((idx, block));
            }
        }
        Ok(out)
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
