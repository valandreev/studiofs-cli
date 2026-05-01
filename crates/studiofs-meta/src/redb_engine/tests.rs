use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::MetaRecovery;
use crate::contract_harness::{ContractCoreFixture, ContractCoreOp};
use crate::engine::DeferredDeleteRecord;
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
fn contract_core_harness_covers_unlink_rmdir_open_unlink_and_reopen() {
    let mut fixture =
        ContractCoreFixture::new("redb-contract-delete-paths").expect("create contract fixture");

    let work = fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: ROOT_INO,
            name: b"work".to_vec(),
            mode: 0o755,
        })
        .expect("create work dir")
        .created_attr()
        .expect("work dir attr");
    fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: work.ino,
            name: b"remove-me.txt".to_vec(),
            mode: 0o644,
        })
        .expect("create file to unlink");
    fixture
        .apply_op(ContractCoreOp::Unlink {
            parent: work.ino,
            name: b"remove-me.txt".to_vec(),
        })
        .expect("unlink file");

    fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: work.ino,
            name: b"empty".to_vec(),
            mode: 0o755,
        })
        .expect("create empty dir");
    fixture
        .apply_op(ContractCoreOp::Rmdir {
            parent: work.ino,
            name: b"empty".to_vec(),
        })
        .expect("rmdir empty dir");

    let open_unlink_attr = fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: work.ino,
            name: b"open-unlink.txt".to_vec(),
            mode: 0o644,
        })
        .expect("create open-unlink file")
        .created_attr()
        .expect("open-unlink attr");
    fixture
        .apply_op(ContractCoreOp::OpenUnlink {
            parent: work.ino,
            name: b"open-unlink.txt".to_vec(),
        })
        .expect("open-unlink file");

    let diagnostics = fixture
        .assert_invariants("after core delete namespace ops")
        .expect("delete-path invariants");
    assert!(
        diagnostics
            .deferred_deletes
            .iter()
            .any(|record| record.ino == open_unlink_attr.ino && record.parent == work.ino),
        "diagnostics should explain the open-unlinked inode through deferred delete state: {diagnostics}"
    );
    assert!(
        diagnostics.entries.iter().all(|entry| {
            entry.name != b"remove-me.txt"
                && entry.name != b"empty"
                && entry.name != b"open-unlink.txt"
        }),
        "deleted namespace entries should not remain visible: {diagnostics}"
    );

    fixture.reopen_meta().expect("reopen metadata fixture");
    fixture
        .assert_invariants("after delete-path metadata reopen")
        .expect("reopened delete-path invariants");
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

#[test]
fn contract_core_harness_reports_dangling_dentry_with_parent_and_name() {
    let fixture =
        ContractCoreFixture::new("redb-contract-dangling-dentry").expect("create contract fixture");
    let attr = fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: ROOT_INO,
            name: b"dangling.txt".to_vec(),
            mode: 0o644,
        })
        .expect("create file")
        .created_attr()
        .expect("file attr");

    fixture
        .meta()
        .remove_attr(attr.ino)
        .expect("remove attr without removing dentry");

    let err = fixture
        .assert_invariants("dangling dentry")
        .expect_err("dentry pointing at missing inode should fail invariants");
    assert!(
        err.to_string()
            .contains("dentry parent=1 name=dangling.txt points to missing inode 2"),
        "unexpected diagnostics: {err}"
    );
}

#[test]
fn contract_core_harness_reports_directory_parent_mismatch_and_cycle() {
    let fixture =
        ContractCoreFixture::new("redb-contract-dir-cycle").expect("create contract fixture");
    let parent = fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: ROOT_INO,
            name: b"parent".to_vec(),
            mode: 0o755,
        })
        .expect("create parent")
        .created_attr()
        .expect("parent attr");
    let child = fixture
        .apply_op(ContractCoreOp::CreateDir {
            parent: parent.ino,
            name: b"child".to_vec(),
            mode: 0o755,
        })
        .expect("create child")
        .created_attr()
        .expect("child attr");

    let meta = fixture.meta();
    let mut bad_child = meta.get_attr(child.ino).expect("load child attr");
    bad_child.parent = ROOT_INO;
    meta.set_attr(&bad_child).expect("corrupt child parent");
    meta.add_entry(
        child.ino,
        b"self",
        &DirEntryValue {
            ino: child.ino,
            kind: FileType::Directory,
        },
    )
    .expect("create cycle dentry");

    let err = fixture
        .assert_invariants("directory parent mismatch and cycle")
        .expect_err("directory parent mismatch and cycle should fail invariants");
    let diagnostics = err.to_string();
    assert!(
        diagnostics.contains("directory inode 3 records parent 1 but is linked from 2"),
        "unexpected diagnostics: {err}"
    );
    assert!(
        diagnostics.contains("directory inode 3 is linked more than once or creates a cycle"),
        "unexpected diagnostics: {err}"
    );
}

#[test]
fn contract_core_harness_reports_unexplained_zero_link_inode() {
    let fixture = ContractCoreFixture::new("redb-contract-zero-link-orphan")
        .expect("create contract fixture");
    let attr = fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: ROOT_INO,
            name: b"orphan.txt".to_vec(),
            mode: 0o644,
        })
        .expect("create file")
        .created_attr()
        .expect("file attr");

    let meta = fixture.meta();
    meta.remove_entry(ROOT_INO, b"orphan.txt")
        .expect("remove dentry without deferred delete");
    let mut bad_attr = meta.get_attr(attr.ino).expect("load file attr");
    bad_attr.nlink = 0;
    meta.set_attr(&bad_attr).expect("corrupt file nlink");

    let err = fixture
        .assert_invariants("unexplained zero-link inode")
        .expect_err("zero-link inode without deferred delete should fail invariants");
    assert!(
        err.to_string()
            .contains("inode 2 has zero nlink but is not reachable from root and has no deferred delete record"),
        "unexpected diagnostics: {err}"
    );
}

#[test]
fn contract_core_harness_reports_stale_deferred_delete_record() {
    let fixture =
        ContractCoreFixture::new("redb-contract-stale-deferred-delete").expect("create fixture");

    fixture
        .meta()
        .add_deferred_delete(&DeferredDeleteRecord {
            ino: 99,
            parent: ROOT_INO,
            name: b"stale-open-unlink.txt".to_vec(),
            created_at: 123,
        })
        .expect("insert stale deferred delete record");

    let err = fixture
        .assert_invariants("stale deferred delete")
        .expect_err("stale deferred delete should fail invariants");
    assert!(
        err.to_string().contains(
            "deferred delete record ino=99 parent=1 name=stale-open-unlink.txt has no matching inode attr"
        ),
        "unexpected diagnostics: {err}"
    );
}
