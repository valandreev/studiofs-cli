use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::events::MetaEvent;

trait FileGraphFixture {
    fn stage_payload_slice(&self, ino: Ino, block_index: u32, block: &PayloadRef)
    -> MetaResult<()>;
}

impl FileGraphFixture for RedbMetaEngine {
    fn stage_payload_slice(
        &self,
        ino: Ino,
        block_index: u32,
        block: &PayloadRef,
    ) -> MetaResult<()> {
        let chunk_slices = Self::chunk_slices_for_block(
            block_index,
            DEFAULT_BLOCK_SIZE,
            Some(block),
            PayloadRefState::LocalOnly,
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
