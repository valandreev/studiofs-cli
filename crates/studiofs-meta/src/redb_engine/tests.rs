use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::events::MetaEvent;

trait BlockSliceFixture {
    fn stage_block_slices(&self, ino: Ino, block_index: u32, block: &BlockRef) -> MetaResult<()>;
}

impl BlockSliceFixture for RedbMetaEngine {
    fn stage_block_slices(&self, ino: Ino, block_index: u32, block: &BlockRef) -> MetaResult<()> {
        let chunk_slices = Self::chunk_slices_for_block(
            block_index,
            DEFAULT_BLOCK_SIZE,
            Some(block),
            BlockRefState::LocalOnly,
        )?;
        self.flush_regular_file_overlay_slices(ino, chunk_slices.as_slice())?;
        Ok(())
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
