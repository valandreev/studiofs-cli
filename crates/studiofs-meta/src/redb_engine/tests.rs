use super::fs_ops::*;
use super::layout::*;
use super::test_support::*;
use super::*;
use crate::MetaRecovery;
use crate::contract_harness::{ContractCoreFixture, ContractCoreOp};
use crate::engine::DeferredDeleteRecord;
use crate::events::MetaEvent;
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn legacy_meta_event_fixture_replays_from_local_event_log() {
    let engine = temp_engine();
    let legacy_bytes = [1_u8, 42_u8];
    let txn = engine
        .begin_write_txn()
        .expect("begin local event fixture transaction");
    {
        let mut events = txn
            .open_table(EVENT_LOG_TABLE)
            .expect("open local event log");
        events
            .insert(1, legacy_bytes.as_slice())
            .expect("insert legacy event bytes");
    }
    {
        let mut counters = txn.open_table(COUNTERS_TABLE).expect("open counters");
        counters
            .insert("nextEventSeq", 2)
            .expect("advance event sequence");
    }
    txn.commit().expect("commit local event fixture");

    let events = engine
        .read_events_range(1, 2)
        .expect("decode legacy event from local event log");
    assert!(matches!(
        events.as_slice(),
        [MetaEvent::RemoveAttr { ino: 42 }]
    ));
}

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
fn sync_local_metadata_executes_immediate_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));
    engine.set_counter("barrier-test", 1).unwrap();

    assert!(engine.relaxed_pending_for_test());
    engine.sync_local_metadata().unwrap();
    assert!(!engine.relaxed_pending_for_test());
    assert_eq!(engine.immediate_barriers_for_test(), 1);
}

#[test]
fn default_engine_uses_one_second_volatile_metadata_durability() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open(dir.path().join("meta.redb")).expect("open metadata");

    let runtime = engine
        .relaxed_durability
        .as_ref()
        .expect("volatile metadata worker is always enabled");
    assert_eq!(runtime.state.interval_ms, 1_000);
}

#[test]
fn flush_metadata_only_executes_due_relaxed_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));
    engine.set_counter("flush-test", 1).unwrap();

    engine.flush_metadata().unwrap();
    assert_eq!(engine.immediate_barriers_for_test(), 0);
    engine.make_relaxed_barrier_due_for_test();
    engine.flush_metadata().unwrap();
    assert_eq!(engine.immediate_barriers_for_test(), 1);
}

#[test]
fn relaxed_durability_state_is_isolated_per_engine() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let first = RedbMetaEngine::open_relaxed_for_test(dir.path().join("first.redb"));
    let second = RedbMetaEngine::open_relaxed_for_test(dir.path().join("second.redb"));
    first.set_counter("first", 1).unwrap();
    second.set_counter("second", 1).unwrap();

    first.sync_local_metadata().unwrap();

    assert!(!first.relaxed_pending_for_test());
    assert!(second.relaxed_pending_for_test());
    assert_eq!(first.immediate_barriers_for_test(), 1);
    assert_eq!(second.immediate_barriers_for_test(), 0);
}

#[test]
fn relaxed_durability_makes_idle_progress_without_another_write() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_with_interval_for_test(
        dir.path().join("meta.redb"),
        Duration::from_millis(20),
    );
    engine.set_counter("idle-progress", 1).unwrap();

    let deadline = Instant::now() + Duration::from_secs(1);
    while engine.immediate_barriers_for_test() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(engine.immediate_barriers_for_test() >= 1);
}

#[test]
fn prepare_for_close_makes_later_relaxed_writes_immediate() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));

    engine
        .sync_local_metadata()
        .expect("initial schema writes must be durable before close preparation");
    engine.prepare_for_close();
    engine.set_counter("after-close-preparation", 1).unwrap();

    assert!(
        !engine.relaxed_pending_for_test(),
        "writes after close preparation must not wait for the relaxed worker"
    );
}

#[test]
fn sync_metadata_does_not_force_a_local_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));

    engine.sync_metadata().unwrap();

    assert_eq!(
        engine.immediate_barriers_for_test(),
        0,
        "remote-style sync_metadata must not force local durability"
    );
}

#[test]
fn relaxed_worker_keeps_the_original_barrier_deadline_after_a_wakeup() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_with_interval_for_test(
        dir.path().join("meta.redb"),
        Duration::from_millis(500),
    );
    engine.set_counter("spurious-wakeup", 1).unwrap();

    let runtime = engine.relaxed_durability.as_ref().expect("relaxed worker");
    let wait_started_deadline = Instant::now() + Duration::from_secs(1);
    while runtime.state.wait_count.load(Ordering::Acquire) == 0
        && Instant::now() < wait_started_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    let initial_wait_count = runtime.state.wait_count.load(Ordering::Acquire);
    assert!(
        initial_wait_count > 0,
        "relaxed worker should enter its timed wait"
    );
    let wake_guard = runtime
        .state
        .wake
        .0
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    drop(wake_guard);

    let elapsed_ms = runtime
        .state
        .started
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    runtime
        .state
        .next_barrier_ms
        .store(elapsed_ms.saturating_add(300), Ordering::Release);
    runtime.state.wake.1.notify_all();

    let wake_deadline = Instant::now() + Duration::from_secs(1);
    while runtime.state.wait_count.load(Ordering::Acquire) <= initial_wait_count
        && Instant::now() < wake_deadline
    {
        std::thread::sleep(Duration::from_millis(10));
    }

    assert!(
        runtime.state.wait_count.load(Ordering::Acquire) > initial_wait_count,
        "relaxed worker should recompute its wait after the wakeup"
    );
    assert!(
        runtime.state.last_wait_timeout_ms.load(Ordering::Acquire) < 500,
        "a wakeup before the deadline must not restart the full interval"
    );
}

#[test]
fn sync_local_metadata_propagates_barrier_failure_and_keeps_pending_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open_relaxed_for_test(dir.path().join("meta.redb"));
    engine.set_counter("barrier-failure", 1).unwrap();
    engine.fail_next_barrier_for_test();

    let error = engine.sync_local_metadata().expect_err("barrier must fail");

    assert!(
        error
            .to_string()
            .contains("simulated metadata barrier failure")
    );
    assert!(engine.relaxed_pending_for_test());
    assert_eq!(engine.immediate_barriers_for_test(), 0);
}

#[test]
fn relaxed_barrier_preserves_write_committed_during_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = Arc::new(RedbMetaEngine::open_relaxed_for_test(
        dir.path().join("meta.redb"),
    ));
    engine.set_counter("before-barrier", 1).unwrap();
    let barrier = Arc::new(Barrier::new(2));
    engine.pause_after_barrier_commit_for_test(Arc::clone(&barrier));

    let sync_engine = Arc::clone(&engine);
    let sync = thread::spawn(move || sync_engine.sync_local_metadata());
    barrier.wait();
    engine.set_counter("during-barrier", 2).unwrap();
    sync.join().unwrap().unwrap();

    assert!(engine.relaxed_pending_for_test());
}

#[test]
fn local_storage_compaction_skips_when_read_transaction_is_active() {
    let engine = temp_engine();
    let _read_txn = engine.begin_read_txn().unwrap();

    assert_eq!(
        engine.compact_local_storage().unwrap(),
        LocalStorageCompactionResult::SkippedBusy
    );
}

#[test]
fn pending_deletions_limited_stops_at_limit() {
    let engine = temp_engine();
    engine.add_pending_deletion(10, 100).unwrap();
    engine.add_pending_deletion(20, 200).unwrap();
    engine.add_pending_deletion(30, 300).unwrap();

    assert_eq!(
        engine.get_pending_deletions_limited(2).unwrap(),
        vec![(10, 100), (20, 200)]
    );
}

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

#[test]
fn contract_core_harness_reports_reachable_pending_deletion_conflict() {
    let fixture = ContractCoreFixture::new("redb-contract-reachable-pending-deletion")
        .expect("create fixture");
    let meta = fixture.meta();
    let file = meta
        .create_file(ROOT_INO, b"reachable-pending.bin", 1000, 1000, 0o644)
        .expect("create file");
    let mut attr = meta.get_attr(file.ino).expect("load file attr");
    attr.size = 4096;
    attr.content_epoch = 1;
    meta.set_attr(&attr).expect("set file attr");
    meta.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(90_001, 4096))
        .expect("stage reachable object");
    meta.add_pending_deletion(90_001, 123)
        .expect("mark reachable object pending deletion");

    let err = fixture
        .assert_invariants("reachable pending deletion conflict")
        .expect_err("reachable pending deletion conflict should fail invariants");
    assert!(
        err.to_string().contains(
            "object_id 90001 is marked for pending deletion (timestamp=123) but is still reachable from metadata"
        ),
        "unexpected diagnostics: {err}"
    );
}

#[test]
fn contract_core_harness_accepts_open_unlink_pending_overlay_cleanup() {
    let fixture = ContractCoreFixture::new("redb-contract-open-unlink-pending-cleanup")
        .expect("create fixture");
    let meta = fixture.meta();

    let file = meta
        .create_file(ROOT_INO, b"open-unlink-overwrite.bin", 1000, 1000, 0o644)
        .expect("create file");
    let mut attr = meta.get_attr(file.ino).expect("load file attr");
    attr.size = 4096;
    attr.content_epoch = 1;
    meta.set_attr(&attr).expect("set published attr");
    meta.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(90_101, 4096))
        .expect("stage published object");
    assert!(
        meta.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 90_101)
            .expect("mark published object uploaded")
    );
    assert!(
        meta.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish base object")
    );

    let mut attr = meta.get_attr(file.ino).expect("reload file attr");
    attr.content_epoch = 2;
    meta.set_attr(&attr).expect("set local overlay attr");
    meta.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(90_102, 4096))
        .expect("stage replacement object");
    assert!(
        meta.mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 2, 90_102)
            .expect("mark replacement object uploaded")
    );

    meta.unlink_open_handle(ROOT_INO, b"open-unlink-overwrite.bin")
        .expect("open-unlink file");

    fixture
        .assert_invariants("open-unlink pending overlay cleanup")
        .expect("open-unlink cleanup state should satisfy contract invariants");
}
