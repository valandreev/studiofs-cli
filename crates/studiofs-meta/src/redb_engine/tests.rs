use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::contract_harness::{ContractCoreFixture, ContractCoreOp};
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

#[test]
fn contract_core_harness_runs_ops_and_reopens_with_invariants() {
    let mut fixture =
        ContractCoreFixture::new("redb-contract-harness").expect("create contract fixture");

    let docs = fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: ROOT_INO,
            name: b"docs".to_vec(),
            mode: 0o755,
        })
        .expect("create docs dir")
        .created_attr()
        .expect("dir attr");
    fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: docs.ino,
            name: b"draft.txt".to_vec(),
            mode: 0o644,
        })
        .expect("create draft");
    fixture
        .apply_op(ContractCoreOp::Rename {
            parent: docs.ino,
            name: b"draft.txt".to_vec(),
            new_parent: docs.ino,
            new_name: b"final.txt".to_vec(),
        })
        .expect("rename draft");

    let diagnostics = fixture
        .assert_invariants("after core namespace ops")
        .expect("core invariants");
    assert!(
        diagnostics
            .entries
            .iter()
            .any(|entry| entry.name == b"final.txt"),
        "diagnostics should include the renamed dentry: {diagnostics}"
    );

    fixture.reopen_meta().expect("reopen metadata fixture");
    fixture
        .assert_invariants("after metadata reopen")
        .expect("reopened core invariants");
}

#[test]
fn contract_core_harness_reports_bad_directory_nlink() {
    let fixture =
        ContractCoreFixture::new("redb-contract-bad-dir-nlink").expect("create contract fixture");
    let dir = fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: ROOT_INO,
            name: b"shots".to_vec(),
            mode: 0o755,
        })
        .expect("create dir")
        .created_attr()
        .expect("dir attr");

    let meta = fixture.meta();
    let mut bad_attr = meta.get_attr(dir.ino).expect("load dir attr");
    bad_attr.nlink = 99;
    meta.set_attr(&bad_attr).expect("corrupt dir nlink");

    let err = fixture
        .assert_invariants("bad directory nlink")
        .expect_err("directory nlink mismatch should fail invariants");
    assert!(
        err.to_string()
            .contains("directory inode 2 has nlink=99 but expected 2"),
        "unexpected diagnostics: {err}"
    );
}
