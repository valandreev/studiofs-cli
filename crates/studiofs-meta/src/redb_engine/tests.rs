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

fn invalidate_local_storage_page_stats(engine: &RedbMetaEngine) {
    *engine
        .local_storage_page_stats
        .lock()
        .unwrap_or_else(|error| error.into_inner()) = None;
}

#[test]
fn coherence_journal_directory_rename_and_removal_retain_descendant_paths() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let top = engine
        .create_dir(ROOT_INO, b"old", 1000, 1000, 0o755)
        .unwrap();
    let nested = engine
        .create_dir(top.ino, b"nested", 1000, 1000, 0o755)
        .unwrap();
    let file = engine
        .create_file(nested.ino, b"child", 1000, 1000, 0o644)
        .unwrap();
    let mut initial = Vec::new();
    for (attr, parent, name) in [
        (&top, ROOT_INO, b"old".as_slice()),
        (&nested, top.ino, b"nested".as_slice()),
        (&file, nested.ino, b"child".as_slice()),
    ] {
        initial.push(MetaEvent::SetAttr { attr: attr.clone() });
        initial.push(MetaEvent::AddEntry {
            parent,
            name: name.to_vec(),
            entry: DirEntryValue {
                ino: attr.ino,
                kind: attr.kind,
            },
        });
    }
    engine.apply_replica_events_to_base(&initial).unwrap();
    engine.take_invalidation_batches();
    let rename = MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: b"old".to_vec(),
        new_parent: ROOT_INO,
        new_name: b"new".to_vec(),
        entry: DirEntryValue {
            ino: top.ino,
            kind: FileType::Directory,
        },
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: None,
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&rename))
        .unwrap();
    engine.apply_replica_serving_state(&[rename]).unwrap();
    let renamed: Vec<_> = engine
        .take_invalidation_batches()
        .into_iter()
        .flat_map(|batch| batch.intents)
        .collect();
    assert!(renamed.contains(&InvalidationIntent::Entry {
        parent: nested.ino,
        name: b"child".to_vec(),
        ancestors: Some(vec![b"old".to_vec(), b"nested".to_vec()]),
        ino: Some(file.ino)
    }));
    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveAttr { ino: top.ino }])
        .unwrap();
    let removed: Vec<_> = engine
        .take_invalidation_batches()
        .into_iter()
        .flat_map(|batch| batch.intents)
        .collect();
    assert!(removed.contains(&InvalidationIntent::Entry {
        parent: nested.ino,
        name: b"child".to_vec(),
        ancestors: Some(vec![b"new".to_vec(), b"nested".to_vec()]),
        ino: Some(file.ino)
    }));
}

#[test]
fn coherence_journal_metadata_only_changes_preserve_content_scope() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"attrs", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.mode = 0o600;
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::SetXattr {
                ino,
                name: b"user.test".to_vec(),
                value: b"value".to_vec(),
                flags: 0,
            },
            MetaEvent::SetAclRule {
                ino,
                principal: "other".to_string(),
                access_level: AccessLevel::ReadOnly,
            },
        ])
        .unwrap();
    let intents: HashSet<_> = engine
        .take_invalidation_batches()
        .into_iter()
        .flat_map(|batch| batch.intents)
        .collect();
    assert!(intents.contains(&InvalidationIntent::Attributes { ino }));
    assert!(!intents.contains(&InvalidationIntent::Content { ino }));
}

#[test]
fn coherence_journal_preserves_rename_chain_and_overwritten_inode() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 42,
        kind: FileType::RegularFile,
    };
    let initial = [
        MetaEvent::AddEntry {
            parent: 1,
            name: b"a".to_vec(),
            entry: entry.clone(),
        },
        MetaEvent::AddEntry {
            parent: 2,
            name: b"b".to_vec(),
            entry: DirEntryValue {
                ino: 43,
                kind: FileType::RegularFile,
            },
        },
    ];
    engine.apply_replica_events_to_base(&initial).unwrap();
    engine.apply_replica_serving_state(&initial).unwrap();
    engine.take_invalidation_batches();
    for (parent, name, new_parent, new_name) in [
        (1, b"a".as_slice(), 2, b"b".as_slice()),
        (2, b"b".as_slice(), 2, b"c".as_slice()),
    ] {
        let rename = MetaEvent::RenameEntry {
            parent,
            name: name.to_vec(),
            new_parent,
            new_name: new_name.to_vec(),
            entry: entry.clone(),
            displaced_set_attr: Box::new(None),
            displaced_remove_attr_ino: None,
            moved_dir_attr: Box::new(None),
            old_parent_attr: Box::new(None),
            new_parent_attr: Box::new(None),
        };
        engine
            .apply_replica_events_to_base(std::slice::from_ref(&rename))
            .unwrap();
        engine.apply_replica_serving_state(&[rename]).unwrap();
    }
    let batches = engine.take_invalidation_batches();
    assert!(
        batches
            .windows(2)
            .all(|pair| pair[0].generation < pair[1].generation)
    );
    let intents: HashSet<_> = batches
        .into_iter()
        .flat_map(|batch| batch.intents)
        .collect();
    for (parent, name) in [
        (1, b"a".as_slice()),
        (2, b"b".as_slice()),
        (2, b"c".as_slice()),
    ] {
        assert!(intents.iter().any(|intent| matches!(intent, InvalidationIntent::Entry { parent: actual_parent, name: actual_name, .. } if *actual_parent == parent && actual_name == name)));
        assert!(intents.contains(&InvalidationIntent::Directory { ino: parent }));
    }
    assert!(intents.contains(&InvalidationIntent::Attributes { ino: 43 }));
    assert!(intents.iter().any(|intent| matches!(
        intent,
        InvalidationIntent::Entry {
            parent: 2,
            ancestors: None,
            ..
        }
    )));
    assert!(!intents.contains(&InvalidationIntent::Reset));
}

#[test]
fn coherence_journal_content_epoch_change_does_not_rewrite_local_content() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"content", 1000, 1000, 0o644)
        .unwrap();
    let before = attr.clone();
    attr.content_epoch += 1;
    attr.committed_content_epoch = attr.content_epoch;
    engine
        .apply_replica_events_to_base(&[MetaEvent::SetAttr { attr }])
        .unwrap();
    let batches = engine.take_invalidation_batches();
    assert!(
        batches
            .iter()
            .flat_map(|batch| &batch.intents)
            .any(|intent| *intent == InvalidationIntent::Content { ino: before.ino })
    );
    // Base publication is advisory: the journal must not replace a local serving overlay.
    assert_eq!(
        engine.get_attr(before.ino).unwrap().content_epoch,
        before.content_epoch
    );
}

#[test]
fn coherence_journal_concurrent_writer_generations_follow_commit_order() {
    use crate::coherence::InvalidationIntent;
    let engine = Arc::new(temp_engine());
    let first = engine.begin_write_txn().unwrap();
    first.invalidate(InvalidationIntent::Attributes { ino: 42 });
    let other = Arc::clone(&engine);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let second = other.begin_write_txn().unwrap();
        second.invalidate(InvalidationIntent::Attributes { ino: 43 });
        second.commit().unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    first.commit().unwrap();
    writer.join().unwrap();
    let batches = engine.take_invalidation_batches();
    assert_eq!(batches.len(), 2);
    assert!(batches[0].generation < batches[1].generation);
    assert_eq!(
        batches[0].intents,
        vec![InvalidationIntent::Attributes { ino: 42 }]
    );
    assert_eq!(
        batches[1].intents,
        vec![InvalidationIntent::Attributes { ino: 43 }]
    );
}

#[test]
fn coherence_journal_preserves_removed_aliases_before_drain() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 42,
        kind: FileType::RegularFile,
    };
    engine
        .apply_replica_serving_state(&[
            MetaEvent::AddEntry {
                parent: 1,
                name: b"old".to_vec(),
                entry: entry.clone(),
            },
            MetaEvent::AddEntry {
                parent: 1,
                name: b"alias".to_vec(),
                entry,
            },
        ])
        .unwrap();
    engine.take_invalidation_batches();
    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveAttr { ino: 42 }])
        .unwrap();
    let batches = engine.take_invalidation_batches();
    assert!(!batches.is_empty());
    for name in [b"old".as_slice(), b"alias".as_slice()] {
        assert!(
            batches
                .iter()
                .flat_map(|batch| &batch.intents)
                .any(|intent| *intent
                    == InvalidationIntent::Entry {
                        parent: 1,
                        name: name.to_vec(),
                        ancestors: Some(Vec::new()),
                        ino: Some(42),
                    })
        );
    }
}

#[test]
fn coherence_journal_publishes_only_committed_transactions() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    txn.invalidate(InvalidationIntent::Attributes { ino: 42 });
    assert!(engine.take_invalidation_batches().is_empty());
    txn.abort().unwrap();
    assert!(engine.take_invalidation_batches().is_empty());
    let txn = engine.begin_write_txn().unwrap();
    txn.invalidate(InvalidationIntent::Attributes { ino: 42 });
    txn.commit().unwrap();
    let first = engine.take_invalidation_batches();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].intents,
        vec![InvalidationIntent::Attributes { ino: 42 }]
    );
    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveEntry {
            parent: 1,
            name: b"old".to_vec(),
        }])
        .unwrap();
    assert!(engine.take_invalidation_batches()[0].generation > first[0].generation);
}

#[test]
fn coherence_journal_overflow_signals_reset() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    for ino in 1..=8192 {
        txn.invalidate(InvalidationIntent::Attributes { ino });
    }
    txn.commit().unwrap();
    let batches = engine.take_invalidation_batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].intents, vec![InvalidationIntent::Reset]);
}

#[test]
fn coherence_journal_deduplicates_duplicate_intents_without_reset() {
    use crate::coherence::InvalidationIntent;
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    for _ in 0..8192 {
        txn.invalidate(InvalidationIntent::Attributes { ino: 42 });
    }
    txn.commit().unwrap();

    let batches = engine.take_invalidation_batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].intents,
        vec![InvalidationIntent::Attributes { ino: 42 }]
    );
}

#[test]
fn update_counters_in_txn_persists_an_initial_zero() {
    let engine = temp_engine();
    let txn = engine.begin_write_txn().expect("begin counter transaction");
    RedbMetaEngine::update_counters_in_txn(&txn, &[("pgLastAppliedBatchVersion", Some(0))])
        .expect("persist initial replay counter");
    txn.commit().expect("commit initial replay counter");

    let txn = engine.begin_read_txn().expect("begin counter read");
    let counters = txn.open_table(COUNTERS_TABLE).expect("open counters");
    assert_eq!(
        counters
            .get("pgLastAppliedBatchVersion")
            .expect("read initial replay counter")
            .expect("initial replay counter row")
            .value(),
        0
    );
}

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

#[test]
fn new_local_event_log_rows_use_versioned_wire_envelope() {
    let engine = temp_engine();
    let txn = engine
        .begin_write_txn()
        .expect("begin local event transaction");
    RedbMetaEngine::append_event_in_txn(&txn, &MetaEvent::RemoveAttr { ino: 42 })
        .expect("append local event");
    txn.commit().expect("commit local event");

    let txn = engine.begin_read_txn().expect("begin local event read");
    let events = txn
        .open_table(EVENT_LOG_TABLE)
        .expect("open local event log");
    let row = events
        .get(1)
        .expect("read local event row")
        .expect("local event row");
    assert!(
        row.value().starts_with(b"SFS-EVT"),
        "new event rows must use the versioned envelope"
    );
}

#[test]
fn versioned_event_rows_survive_local_engine_restart() {
    let dir = studiofs_test_support::workspace_target_tempdir("event-restart-");
    let path = dir.path().join("meta.redb");
    let start_seq;
    let end_seq;
    {
        let engine = RedbMetaEngine::open(&path).expect("open local metadata");
        engine.init_root(1000, 1000).expect("initialize root");
        engine
            .set_format_info(&test_format_info("event-restart"))
            .expect("initialize format");
        start_seq = engine
            .get_counter("nextEventSeq")
            .expect("read starting event sequence");
        let txn = engine
            .begin_write_txn()
            .expect("begin local event transaction");
        RedbMetaEngine::append_event_in_txn(&txn, &MetaEvent::RemoveAttr { ino: 42 })
            .expect("append local event");
        txn.commit().expect("commit local event");
        end_seq = engine
            .get_counter("nextEventSeq")
            .expect("read ending event sequence");
        assert_eq!(end_seq, start_seq + 1);
    }

    let reopened = RedbMetaEngine::open(&path).expect("reopen local metadata");
    let events = reopened
        .read_events_range(start_seq, end_seq)
        .expect("read event after restart");

    assert!(matches!(
        events.as_slice(),
        [MetaEvent::RemoveAttr { ino: 42 }]
    ));
}

#[test]
fn local_upload_ledger_survives_reopen_until_remote_ack() {
    let dir = studiofs_test_support::workspace_target_tempdir("upload-ledger-restart-");
    let path = dir.path().join("meta.redb");
    let entry = UploadedFragmentLedgerEntry {
        volume_id: 7,
        object_id: 42,
        fragment_index: 0,
        object_key: "objects/42".to_string(),
        expected_byte_length: 4,
        expected_sha256: [0xA5; 32],
    };

    {
        let engine = RedbMetaEngine::open(&path).expect("open local metadata");
        engine
            .record_local_upload_ledger_begin(std::slice::from_ref(&entry))
            .expect("record local upload begin");
        engine
            .record_local_upload_ledger_confirm(std::slice::from_ref(&entry))
            .expect("record local upload confirmation");
    }

    let reopened = RedbMetaEngine::open(&path).expect("reopen local metadata");
    let pending = reopened
        .pending_local_upload_ledger(16)
        .expect("read local upload ledger");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].entry, entry);
    assert!(pending[0].confirmed);
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
    let path = dir.path().join("meta.redb");
    let engine = RedbMetaEngine::open_relaxed_for_test(&path);
    engine.set_counter("barrier-test", 1).unwrap();

    assert!(engine.relaxed_pending_for_test());
    engine.sync_local_metadata().unwrap();
    assert!(!engine.relaxed_pending_for_test());
    assert_eq!(engine.immediate_barriers_for_test(), 1);
    drop(engine);

    let reopened = RedbMetaEngine::open(&path).expect("reopen metadata database");
    assert_eq!(reopened.get_counter("barrier-test").unwrap(), 1);
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
    engine.prepare_for_close().expect("close redb test engine");
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
fn relaxed_barrier_preserves_pending_for_writes_after_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = Arc::new(RedbMetaEngine::open_relaxed_for_test(
        dir.path().join("meta.redb"),
    ));
    engine.set_counter("before-barrier", 1).unwrap();
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    engine.pause_after_barrier_commit_for_test(Arc::clone(&reached), Arc::clone(&resume));

    let sync_engine = Arc::clone(&engine);
    let sync = thread::spawn(move || sync_engine.sync_local_metadata());
    reached.wait();
    resume.wait();
    sync.join().unwrap().unwrap();
    engine.set_counter("after-barrier", 2).unwrap();

    assert!(engine.relaxed_pending_for_test());
}

#[test]
fn local_storage_diagnostics_does_not_collect_page_stats_during_barrier() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = Arc::new(RedbMetaEngine::open_relaxed_for_test(
        dir.path().join("meta.redb"),
    ));
    let reached = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    engine.pause_after_barrier_commit_for_test(Arc::clone(&reached), Arc::clone(&resume));

    let sync_engine = Arc::clone(&engine);
    let sync = thread::spawn(move || sync_engine.sync_local_metadata());
    reached.wait();
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    resume.wait();
    sync.join().unwrap().unwrap();

    assert!(diagnostics.physical_file_bytes.is_some());
    assert!(diagnostics.allocated_pages.is_none());
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
fn local_storage_compaction_records_active_transaction_skip_reason() {
    let engine = temp_engine();
    let _read_txn = engine.begin_read_txn().unwrap();

    assert_eq!(
        engine.compact_local_storage().unwrap(),
        LocalStorageCompactionResult::SkippedBusy
    );
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");

    assert_eq!(diagnostics.compaction_attempts, 1);
    assert_eq!(diagnostics.compaction_skipped_busy, 1);
    assert_eq!(
        diagnostics.last_compaction_result,
        Some(LocalStorageCompactionResult::SkippedBusy)
    );
    assert_eq!(
        diagnostics.last_compaction_skip_reason,
        Some(LocalStorageCompactionSkipReason::ActiveTransaction)
    );
    assert!(diagnostics.last_compaction_duration_ns.is_some());
}

#[test]
fn local_storage_compaction_records_database_lock_contention_reason() {
    let engine = temp_engine();
    {
        let _database_lock = engine.db.as_ref().unwrap().write().unwrap();
        assert_eq!(
            engine.compact_local_storage().unwrap(),
            LocalStorageCompactionResult::SkippedBusy
        );
    }

    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(diagnostics.compaction_skipped_busy, 1);
    assert_eq!(
        diagnostics.last_compaction_skip_reason,
        Some(LocalStorageCompactionSkipReason::DatabaseLockContended)
    );
}

#[test]
fn local_storage_diagnostics_omits_page_stats_when_writer_lock_is_busy() {
    let engine = temp_engine();
    let _writer_lock = engine.write_txn_lock.lock().unwrap();

    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert!(diagnostics.physical_file_bytes.is_some());
    assert_eq!(diagnostics.allocated_pages, None);
}

#[test]
fn local_storage_diagnostics_reuses_page_stats_when_writer_lock_is_busy() {
    let engine = temp_engine();
    let initial = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert!(initial.allocated_pages.is_some());
    let _writer_lock = engine.write_txn_lock.lock().unwrap();

    let cached = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(cached.allocated_pages, initial.allocated_pages);
}

#[test]
fn local_storage_diagnostics_returns_cached_stats_while_refresh_is_busy() {
    let engine = temp_engine();
    let initial = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    let cached = engine
        .local_storage_page_stats
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .expect("page stats were cached");
    let stale_at = cached
        .sampled_at
        .checked_add(LOCAL_STORAGE_PAGE_STATS_REFRESH_INTERVAL)
        .expect("test timestamp remains representable");
    let _refresh = engine.local_storage_page_stats_refresh.lock().unwrap();

    let stats = engine
        .local_storage_page_stats_best_effort_at(stale_at)
        .expect("the cached page sample is available during refresh");
    assert_eq!(stats.allocated_pages, initial.allocated_pages.unwrap());
}

#[test]
fn local_storage_page_stats_cache_uses_the_supplied_sample_time() {
    let engine = temp_engine();
    let sampled_at = Instant::now()
        .checked_add(LOCAL_STORAGE_PAGE_STATS_REFRESH_INTERVAL)
        .expect("sample time remains representable");

    assert!(
        engine
            .local_storage_page_stats_best_effort_at(sampled_at)
            .is_some()
    );
    let cached = engine
        .local_storage_page_stats
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .expect("page stats were cached");

    assert_eq!(cached.sampled_at, sampled_at);
}

#[test]
fn local_storage_diagnostics_recovers_poisoned_page_stats_cache() {
    let engine = temp_engine();
    let initial = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert!(initial.allocated_pages.is_some());

    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _cache = engine.local_storage_page_stats.lock().unwrap();
        panic!("poison page stats cache for recovery coverage");
    }));
    assert!(poisoned.is_err());

    let recovered = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(recovered.allocated_pages, initial.allocated_pages);
}

#[test]
fn local_storage_compaction_errors_are_counted() {
    let engine = temp_engine();
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _database_lock = engine.db.as_ref().unwrap().write().unwrap();
        panic!("poison database lock for compaction error coverage");
    }));
    assert!(poisoned.is_err());

    assert!(engine.compact_local_storage().is_err());
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(diagnostics.compaction_attempts, 1);
    assert_eq!(diagnostics.compaction_errors, 1);
    assert!(diagnostics.last_compaction_error);
}

#[test]
fn local_storage_diagnostics_report_empty_event_log_span() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open(dir.path().join("metadata.redb")).unwrap();

    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");

    assert_eq!(diagnostics.first_local_event_seq, None);
    assert_eq!(diagnostics.next_local_event_seq, Some(1));
    assert_eq!(diagnostics.retained_local_event_span, Some(0));
}

#[test]
fn local_storage_diagnostics_keeps_exclusive_end_after_retained_event_tail() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open(dir.path().join("metadata.redb")).unwrap();
    let write = engine.begin_write_txn().unwrap();
    {
        let mut events = write.open_table(EVENT_LOG_TABLE).unwrap();
        events.insert(42, b"event".as_slice()).unwrap();
        events.insert(47, b"event".as_slice()).unwrap();
    }
    write.commit().unwrap();

    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");

    assert_eq!(diagnostics.first_local_event_seq, Some(42));
    assert_eq!(diagnostics.next_local_event_seq, Some(48));
    assert_eq!(diagnostics.retained_local_event_span, Some(6));
}

#[test]
fn local_storage_compaction_records_no_work_after_compaction() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let engine = RedbMetaEngine::open(dir.path().join("metadata.redb")).unwrap();

    assert_eq!(
        engine.compact_local_storage().unwrap(),
        LocalStorageCompactionResult::Compacted
    );
    assert_eq!(
        engine.compact_local_storage().unwrap(),
        LocalStorageCompactionResult::NoWork
    );
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");

    assert_eq!(diagnostics.compaction_attempts, 2);
    assert_eq!(diagnostics.compaction_compacted, 1);
    assert_eq!(diagnostics.compaction_no_work, 1);
    assert_eq!(
        diagnostics.last_compaction_result,
        Some(LocalStorageCompactionResult::NoWork)
    );
}

#[test]
fn local_storage_diagnostics_track_delete_and_compaction() {
    const CHURN_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("sfs732_churn");

    let engine = temp_engine();
    let initial = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert!(initial.physical_file_bytes.is_some_and(|bytes| bytes > 0));
    let next_before_event = initial
        .next_local_event_seq
        .expect("next local event sequence is available");

    engine
        .add_pending_deletion(7, 11)
        .expect("append one local event");
    let write = engine.begin_write_txn().unwrap();
    {
        let mut table = write.open_table(CHURN_TABLE).unwrap();
        let payload = vec![b'x'; 8 * 1024];
        for key in 0..128 {
            table.insert(key, payload.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
    invalidate_local_storage_page_stats(&engine);

    let before_delete = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    let first_event = before_delete
        .first_local_event_seq
        .expect("local event log has a retained event");
    let next_event = next_before_event + 1;
    assert_eq!(
        before_delete.first_local_event_seq,
        initial.first_local_event_seq.or(Some(next_before_event))
    );
    assert_eq!(before_delete.next_local_event_seq, Some(next_event));
    assert_eq!(
        before_delete.retained_local_event_span,
        Some(next_event.saturating_sub(first_event))
    );

    let delete = engine.begin_write_txn().unwrap();
    {
        let mut table = delete.open_table(CHURN_TABLE).unwrap();
        for key in 0..128 {
            table.remove(key).unwrap();
        }
    }
    delete.commit().unwrap();
    invalidate_local_storage_page_stats(&engine);

    let after_delete = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert!(
        after_delete.stored_bytes.unwrap() < before_delete.stored_bytes.unwrap(),
        "deleted rows must reduce redb stored bytes"
    );
    assert_ne!(
        after_delete.fragmented_bytes, before_delete.fragmented_bytes,
        "redb fragmentation statistics must reflect the deleted rows"
    );

    assert_eq!(
        engine.compact_local_storage().unwrap(),
        LocalStorageCompactionResult::Compacted
    );
    let after_compaction = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(after_compaction.compaction_attempts, 1);
    assert_eq!(after_compaction.compaction_compacted, 1);
    assert_eq!(
        after_compaction.last_compaction_result,
        Some(LocalStorageCompactionResult::Compacted)
    );
    assert_eq!(
        after_compaction.last_success_bytes_before,
        after_delete.physical_file_bytes
    );
    assert_eq!(
        after_compaction.last_success_bytes_after,
        after_compaction.physical_file_bytes
    );
    assert!(
        after_compaction.physical_file_bytes.unwrap() < after_delete.physical_file_bytes.unwrap(),
        "the controlled compaction must shrink the metadata database"
    );
    assert!(after_compaction.last_compaction_duration_ns.is_some());
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
fn pending_deletion_batch_commits_all_markers_in_one_transaction_with_one_event_each() {
    let engine = temp_engine();
    let markers = [(10, 100), (20, 200), (30, 300)];
    let before = engine.metadata_write_observability();
    let start_seq = engine.get_counter("nextEventSeq").unwrap();

    engine
        .add_pending_deletions(&markers)
        .expect("commit pending deletion batch");

    let after = engine.metadata_write_observability();
    assert_eq!(after.begin_write.calls - before.begin_write.calls, 1);
    assert_eq!(after.commit.calls - before.commit.calls, 1);
    assert_eq!(engine.get_pending_deletions().unwrap(), markers.to_vec());

    let end_seq = engine.get_counter("nextEventSeq").unwrap();
    let events = engine
        .read_events_range(start_seq, end_seq)
        .expect("read pending deletion events");
    assert!(matches!(
        events.as_slice(),
        [
            MetaEvent::AddPendingDeletion {
                object_id: 10,
                timestamp: 100
            },
            MetaEvent::AddPendingDeletion {
                object_id: 20,
                timestamp: 200
            },
            MetaEvent::AddPendingDeletion {
                object_id: 30,
                timestamp: 300
            }
        ]
    ));
}

#[test]
fn pending_deletion_batch_rollback_discards_markers_and_events() {
    let engine = temp_engine();
    let start_seq = engine.get_counter("nextEventSeq").unwrap();
    let txn = engine
        .begin_write_txn()
        .expect("begin pending deletion txn");

    RedbMetaEngine::add_pending_deletions_in_txn(&txn, &[(40, 400), (50, 500)])
        .expect("stage pending deletion batch");
    drop(txn);

    assert!(
        engine.get_pending_deletions().unwrap().is_empty(),
        "dropping the transaction must roll back every pending marker"
    );
    assert_eq!(
        engine.get_counter("nextEventSeq").unwrap(),
        start_seq,
        "dropping the transaction must roll back its event sequence"
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
