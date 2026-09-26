fn clear_event_log_for_test(engine: &RedbMetaEngine) {
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        events.retain(|_, _| false).unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn first_event_seq_returns_none_for_empty_log() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);

    assert_eq!(engine.first_event_seq().unwrap(), None);
}

#[test]
fn first_event_seq_returns_lowest_durable_sequence() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);
    engine.set_counter("nextEventSeq", 1_000_000).unwrap();
    engine
        .create_file(ROOT_INO, b"sparse-event-log.txt", 1000, 1000, 0o644)
        .unwrap();

    assert_eq!(engine.first_event_seq().unwrap(), Some(1_000_000));
}

fn event_log_sequences(engine: &RedbMetaEngine) -> Vec<u64> {
    let txn = engine.begin_read_txn().unwrap();
    let events = txn.open_table(EVENT_LOG_TABLE).unwrap();
    events
        .iter()
        .unwrap()
        .map(|row| row.unwrap().0.value())
        .collect()
}

#[test]
fn local_event_pruning_removes_bounded_prefix_below_exclusive_floor() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        for seq in 1..=6 {
            events.insert(seq, &b"event"[..]).unwrap();
        }
    }
    txn.commit().unwrap();
    engine.set_counter("nextEventSeq", 7).unwrap();

    assert_eq!(engine.prune_local_event_log_before(5, 2).unwrap().rows, 2);
    assert_eq!(event_log_sequences(&engine), vec![3, 4, 5, 6]);
    assert_eq!(engine.prune_local_event_log_before(5, 2).unwrap().rows, 2);
    assert_eq!(event_log_sequences(&engine), vec![5, 6]);
    assert_eq!(engine.prune_local_event_log_before(5, 2).unwrap().rows, 0);
    assert_eq!(engine.get_counter("nextEventSeq").unwrap(), 7);
}

#[test]
fn local_event_pruning_removes_sparse_acknowledged_rows_across_batches() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        for seq in [1, 2, 4, 5] {
            events.insert(seq, &b"event"[..]).unwrap();
        }
    }
    txn.commit().unwrap();
    engine.set_counter("nextEventSeq", 6).unwrap();

    assert_eq!(engine.prune_local_event_log_before(6, 2).unwrap().rows, 2);
    assert_eq!(event_log_sequences(&engine), vec![4, 5]);
    assert_eq!(engine.prune_local_event_log_before(6, 2).unwrap().rows, 2);
    assert!(event_log_sequences(&engine).is_empty());
}

#[test]
fn local_event_pruning_drop_before_commit_preserves_rows_and_floor() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        for seq in 1..=3 {
            events.insert(seq, &b"event"[..]).unwrap();
        }
    }
    txn.commit().unwrap();
    engine.set_counter("nextEventSeq", 4).unwrap();

    let txn = engine.begin_write_txn().unwrap();
    let batch = RedbMetaEngine::prune_local_event_log_batch_in_txn(&txn, 4, 2).unwrap();
    assert_eq!(batch.rows, 2);
    assert_eq!(batch.first_seq, Some(1));
    assert_eq!(batch.last_seq, Some(2));
    drop(txn);

    assert_eq!(event_log_sequences(&engine), vec![1, 2, 3]);
    assert_eq!(
        engine
            .get_counter(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .unwrap(),
        0
    );
}

#[test]
fn local_event_pruning_floor_and_sequence_survive_reopen() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("metadata.redb");
    let engine = RedbMetaEngine::open(&path).unwrap();
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        for seq in [1, 2, 4, 5, 6] {
            events.insert(seq, &b"event"[..]).unwrap();
        }
    }
    txn.commit().unwrap();
    engine.set_counter("nextEventSeq", 7).unwrap();

    let first_batch = engine.prune_local_event_log_before(6, 2).unwrap();
    assert_eq!(first_batch.rows, 2);
    assert_eq!(event_log_sequences(&engine), vec![4, 5, 6]);
    drop(engine);

    let reopened = RedbMetaEngine::open(&path).unwrap();
    assert_eq!(event_log_sequences(&reopened), vec![4, 5, 6]);
    assert_eq!(
        reopened
            .get_counter(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .unwrap(),
        2
    );
    assert_eq!(reopened.get_counter("nextEventSeq").unwrap(), 7);

    let second_batch = reopened.prune_local_event_log_before(6, 2).unwrap();
    assert_eq!(second_batch.rows, 2);
    assert_eq!(event_log_sequences(&reopened), vec![6]);
    assert_eq!(
        reopened
            .get_counter(crate::schema::counters::LOCAL_EVENT_LOG_PRUNED_THROUGH_SEQ)
            .unwrap(),
        5
    );
}

#[test]
fn local_event_pruning_reports_range_payload_estimate_and_duration() {
    let engine = temp_engine();
    clear_event_log_for_test(&engine);
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut events = txn.open_table(EVENT_LOG_TABLE).unwrap();
        events.insert(1, &b"one"[..]).unwrap();
        events.insert(2, &b"two-two"[..]).unwrap();
    }
    txn.commit().unwrap();

    let batch = engine.prune_local_event_log_before(3, 2).unwrap();
    assert_eq!(batch.rows, 2);
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(diagnostics.local_event_log_pruned_through_seq, Some(2));
    assert_eq!(diagnostics.local_event_log_pruned_rows_total, 2);
    assert_eq!(diagnostics.local_event_log_pruned_payload_bytes_total, 10);
    assert_eq!(diagnostics.last_local_event_log_prune_rows, Some(2));
    assert_eq!(diagnostics.last_local_event_log_prune_first_seq, Some(1));
    assert_eq!(diagnostics.last_local_event_log_prune_last_seq, Some(2));
    assert_eq!(diagnostics.last_local_event_log_prune_payload_bytes, Some(10));
    assert!(diagnostics.last_local_event_log_prune_duration_ns.is_some());

    assert_eq!(
        engine.prune_local_event_log_before(3, 2).unwrap().rows,
        0,
        "the follow-up pass should find no eligible rows"
    );
    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available after an idle prune pass");
    assert_eq!(diagnostics.last_local_event_log_prune_rows, Some(0));
    assert_eq!(diagnostics.last_local_event_log_prune_first_seq, Some(1));
    assert_eq!(diagnostics.last_local_event_log_prune_last_seq, Some(2));
}

#[test]
fn local_event_pruning_diagnostics_omit_floor_before_any_pruning() {
    let engine = temp_engine();

    let diagnostics = engine
        .local_storage_diagnostics()
        .unwrap()
        .expect("redb diagnostics are available");
    assert_eq!(diagnostics.local_event_log_pruned_through_seq, None);
}

#[test]
fn local_event_pruning_skips_write_transaction_before_the_first_sequence() {
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = Arc::new(temp_engine());
    let blocked_writer = engine.begin_write_txn().unwrap();
    let worker_engine = Arc::clone(&engine);
    let (sender, receiver) = mpsc::sync_channel(1);
    let worker = thread::spawn(move || {
        sender
            .send(worker_engine.prune_local_event_log_before(1, 10))
            .unwrap();
    });

    let completed_without_writer = receiver.recv_timeout(Duration::from_secs(2));
    let completed_on_time = completed_without_writer.is_ok();
    drop(blocked_writer);
    let result = match completed_without_writer {
        Ok(result) => result,
        Err(_) => receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("prune worker must finish after the held write transaction is released"),
    };
    worker.join().unwrap();

    assert!(
        completed_on_time,
        "floor 1 has no eligible event rows and must not wait for the redb writer"
    );
    assert_eq!(result.unwrap().rows, 0);
}

#[test]
fn test_replica_apply_keeps_namespace_in_base_only() {
    let engine = temp_engine();

    let remote_ino = 555;
    let mut remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 1000, 1000, 0o644);
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"remote.txt".to_vec(),
                entry: DirEntryValue {
                    ino: remote_ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();

    let before_overlay_attrs = engine.local_overlay_attr_inodes().unwrap();
    let before_overlay_dentries = engine.local_overlay_dentries().unwrap();
    engine
        .apply_replica_aux_state(&[
            crate::events::MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"remote.txt".to_vec(),
                entry: DirEntryValue {
                    ino: remote_ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();

    assert_eq!(engine.get_attr(remote_ino).unwrap().ino, remote_ino);
    assert!(engine.lookup(ROOT_INO, b"remote.txt").is_ok());
    assert_eq!(
        engine.local_overlay_attr_inodes().unwrap(),
        before_overlay_attrs
    );
    assert_eq!(
        engine.local_overlay_dentries().unwrap(),
        before_overlay_dentries
    );
}

#[test]
fn test_replica_apply_keeps_symlink_and_xattr_in_base_only() {
    let engine = temp_engine();

    let remote_ino = 777;
    let remote_attr = InodeAttr::new_symlink(remote_ino, ROOT_INO, 0, 0, 7);
    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            crate::events::MetaEvent::SetSymlinkTarget {
                ino: remote_ino,
                target: b"/remote".to_vec(),
            },
            crate::events::MetaEvent::SetXattr {
                ino: remote_ino,
                name: b"user.color".to_vec(),
                value: b"red".to_vec(),
                flags: 0,
            },
        ])
        .unwrap();

    let before_symlink_overlay = engine.local_overlay_symlink_inodes().unwrap();
    let before_xattr_overlay = engine.local_overlay_xattr_keys().unwrap();

    engine
        .apply_replica_aux_state(&[
            crate::events::MetaEvent::SetSymlinkTarget {
                ino: remote_ino,
                target: b"/remote".to_vec(),
            },
            crate::events::MetaEvent::SetXattr {
                ino: remote_ino,
                name: b"user.color".to_vec(),
                value: b"red".to_vec(),
                flags: 0,
            },
        ])
        .unwrap();

    assert_eq!(engine.get_symlink_target(remote_ino).unwrap(), b"/remote");
    assert_eq!(
        engine.get_xattr(remote_ino, b"user.color").unwrap(),
        Some(b"red".to_vec())
    );
    assert_eq!(
        engine.local_overlay_symlink_inodes().unwrap(),
        before_symlink_overlay
    );
    assert_eq!(
        engine.local_overlay_xattr_keys().unwrap(),
        before_xattr_overlay
    );
}

fn replica_rename_event(
    parent: Ino,
    name: &[u8],
    new_parent: Ino,
    new_name: &[u8],
    entry: DirEntryValue,
) -> MetaEvent {
    MetaEvent::RenameEntry {
        parent,
        name: name.to_vec(),
        new_parent,
        new_name: new_name.to_vec(),
        entry,
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: None,
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    }
}

fn assert_only_visible_name(engine: &RedbMetaEngine, expected: &[u8], absent: &[&[u8]]) {
    assert!(
        engine.lookup(ROOT_INO, expected).is_ok(),
        "expected winner name to be visible: {}",
        String::from_utf8_lossy(expected)
    );
    for name in absent {
        assert!(
            engine.lookup(ROOT_INO, name).is_err(),
            "stale name should not be visible: {}",
            String::from_utf8_lossy(name)
        );
    }
}

#[test]
fn test_replica_base_remove_attr_clears_dangling_dentries_for_inode() {
    let engine = temp_engine();
    let ino = 782;
    let entry = DirEntryValue {
        ino,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"base-only.txt".to_vec(),
                entry,
            },
        ])
        .unwrap();

    let dentry_key = encode_dentry_key(ROOT_INO, b"base-only.txt");
    let index_key = encode_dentry_by_inode_key(ino, ROOT_INO, b"base-only.txt");
    {
        let txn = engine.begin_read_txn().unwrap();
        let dentries = txn.open_table(BASE_DENTRIES_TABLE).unwrap();
        let by_inode = txn.open_table(BASE_DENTRIES_BY_INODE_TABLE).unwrap();
        assert!(dentries.get(dentry_key.as_slice()).unwrap().is_some());
        assert!(by_inode.get(index_key.as_slice()).unwrap().is_some());
    }

    engine
        .apply_replica_events_to_base(&[MetaEvent::RemoveAttr { ino }])
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
    let dentries = txn.open_table(BASE_DENTRIES_TABLE).unwrap();
    let by_inode = txn.open_table(BASE_DENTRIES_BY_INODE_TABLE).unwrap();
    assert!(dentries.get(dentry_key.as_slice()).unwrap().is_none());
    assert!(by_inode.get(index_key.as_slice()).unwrap().is_none());
}

#[test]
fn replica_serving_remove_attr_removes_renamed_alias() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 783,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry,
            },
        ])
        .unwrap();

    engine
        .rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"renamed.txt")
        .expect("local rename should be accepted");
    assert!(engine.lookup(ROOT_INO, b"renamed.txt").is_ok());

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
            },
            MetaEvent::RemoveAttr { ino: 783 },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
            },
            MetaEvent::RemoveAttr { ino: 783 },
        ])
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"source.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"renamed.txt").is_err());
}

#[test]
fn replica_serving_rename_uses_base_winner_when_local_loser_moved_to_a() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 778,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();

    engine
        .rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-a.txt")
        .expect("local loser rename should be accepted");

    let pg_winner =
        replica_rename_event(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-b.txt", entry);
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&pg_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&pg_winner))
        .unwrap();

    assert_only_visible_name(
        &engine,
        b"renamed-b.txt",
        &[b"source.txt".as_slice(), b"renamed-a.txt".as_slice()],
    );
}

#[test]
fn replica_serving_rename_uses_base_winner_when_local_loser_moved_to_b() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 779,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();

    engine
        .rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-b.txt")
        .expect("local loser rename should be accepted");

    let pg_winner =
        replica_rename_event(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-a.txt", entry);
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&pg_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&pg_winner))
        .unwrap();

    assert_only_visible_name(
        &engine,
        b"renamed-a.txt",
        &[b"source.txt".as_slice(), b"renamed-b.txt".as_slice()],
    );
}

#[test]
fn replica_serving_rename_does_not_resurrect_locally_unlinked_destination() {
    let source = temp_engine();
    let entry = source
        .create_symlink(ROOT_INO, b"source-link", b"target-v1.txt", 0, 0)
        .unwrap();
    let snapshot = source.export_remote_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine
        .rename_atomic(ROOT_INO, b"source-link", ROOT_INO, b"renamed-link")
        .expect("local rename should be accepted");
    engine
        .set_counter(
            "pgLastFlushedLocalEventSeq",
            engine.get_counter("nextEventSeq").unwrap(),
        )
        .unwrap();
    engine
        .unlink(ROOT_INO, b"renamed-link")
        .expect("local unlink should be accepted");
    engine
        .set_counter(
            "pgLastFlushedLocalEventSeq",
            engine.get_counter("nextEventSeq").unwrap(),
        )
        .unwrap();
    assert!(engine.lookup(ROOT_INO, b"renamed-link").is_err());

    let pg_winner = replica_rename_event(
        ROOT_INO,
        b"source-link",
        ROOT_INO,
        b"renamed-link",
        DirEntryValue {
            ino: entry.ino,
            kind: entry.kind,
        },
    );
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&pg_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&pg_winner))
        .unwrap();

    assert!(
        engine.lookup(ROOT_INO, b"renamed-link").is_err(),
        "a local unlink must hide a promoted rename destination"
    );
    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveEntry {
            parent: ROOT_INO,
            name: b"renamed-link".to_vec(),
        }])
        .unwrap();
    assert!(
        engine.lookup(ROOT_INO, b"renamed-link").is_err(),
        "serving replay of the unlink must not clear its base tombstone"
    );
    engine
        .create_symlink(ROOT_INO, b"renamed-link", b"target-v2.txt", 0, 0)
        .expect("locally unlinked destination must be reusable");
}

#[test]
fn replica_serving_rename_removes_source_even_when_overlay_points_at_other_inode() {
    let engine = temp_engine();
    let source = DirEntryValue {
        ino: 784,
        kind: FileType::RegularFile,
    };
    let target = DirEntryValue {
        ino: 785,
        kind: FileType::RegularFile,
    };
    let local_overlay = DirEntryValue {
        ino: 786,
        kind: FileType::RegularFile,
    };
    let source_attr = InodeAttr::new_file(source.ino, ROOT_INO, 1000, 1000, 0o644);
    let target_attr = InodeAttr::new_file(target.ino, ROOT_INO, 1000, 1000, 0o644);
    let local_attr = InodeAttr::new_file(local_overlay.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: source_attr.clone(),
            },
            MetaEvent::SetAttr {
                attr: target_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"slot-3.txt".to_vec(),
                entry: source.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"slot-3-renamed.txt".to_vec(),
                entry: target.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr: source_attr },
            MetaEvent::SetAttr { attr: target_attr },
            MetaEvent::SetAttr { attr: local_attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"slot-3.txt".to_vec(),
                entry: local_overlay,
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"slot-3-renamed.txt".to_vec(),
                entry: target.clone(),
            },
        ])
        .unwrap();

    let rename_replace = MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: b"slot-3.txt".to_vec(),
        new_parent: ROOT_INO,
        new_name: b"slot-3-renamed.txt".to_vec(),
        entry: source.clone(),
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: Some(target.ino),
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    };

    engine
        .apply_replica_events_to_base(std::slice::from_ref(&rename_replace))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&rename_replace))
        .unwrap();

    assert_only_visible_name(&engine, b"slot-3-renamed.txt", &[b"slot-3.txt".as_slice()]);
    let renamed = engine
        .lookup(ROOT_INO, b"slot-3-renamed.txt")
        .expect("lookup renamed target");
    assert_eq!(renamed.ino, source.ino);
}

#[test]
fn replica_base_rename_uses_pg_order_when_source_already_moved_to_prior_winner() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 781,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();

    let first_winner = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-a.txt",
        entry.clone(),
    );
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&first_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&first_winner))
        .unwrap();

    let pg_later_winner =
        replica_rename_event(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-b.txt", entry);
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&pg_later_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&pg_later_winner))
        .unwrap();

    assert_only_visible_name(
        &engine,
        b"renamed-b.txt",
        &[b"source.txt".as_slice(), b"renamed-a.txt".as_slice()],
    );
}

#[test]
fn replica_serving_rename_preserves_local_hardlink_while_removing_local_loser() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 782,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();

    engine
        .link(entry.ino, ROOT_INO, b"local-link.txt")
        .expect("local hardlink should be accepted");
    engine
        .rename_atomic(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-a.txt")
        .expect("local loser rename should be accepted");

    let pg_winner =
        replica_rename_event(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-b.txt", entry);
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&pg_winner))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&pg_winner))
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"renamed-b.txt").is_ok());
    assert!(engine.lookup(ROOT_INO, b"local-link.txt").is_ok());
    assert!(engine.lookup(ROOT_INO, b"source.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"renamed-a.txt").is_err());
}

#[test]
fn replica_serving_rename_still_applies_when_source_matches() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 780,
        kind: FileType::RegularFile,
    };
    let attr = InodeAttr::new_file(entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();

    let remote_rename =
        replica_rename_event(ROOT_INO, b"source.txt", ROOT_INO, b"renamed-a.txt", entry);
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&remote_rename))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&remote_rename))
        .unwrap();

    assert_only_visible_name(
        &engine,
        b"renamed-a.txt",
        &[b"source.txt".as_slice(), b"renamed-b.txt".as_slice()],
    );
}

#[test]
fn replica_rename_materializes_destination_when_source_was_reused_by_local_inode() {
    let engine = temp_engine();
    let remote_entry = DirEntryValue {
        ino: 790,
        kind: FileType::RegularFile,
    };
    let local_entry = DirEntryValue {
        ino: 791,
        kind: FileType::RegularFile,
    };
    let remote_attr = InodeAttr::new_file(remote_entry.ino, ROOT_INO, 1000, 1000, 0o644);
    let local_attr = InodeAttr::new_file(local_entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: remote_entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: remote_entry.clone(),
            },
        ])
        .unwrap();

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: local_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: local_entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr: local_attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: local_entry,
            },
        ])
        .unwrap();

    let remote_rename = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed.txt",
        remote_entry,
    );
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&remote_rename))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&remote_rename))
        .unwrap();

    assert_eq!(engine.lookup(ROOT_INO, b"source.txt").unwrap().ino, 791);
    assert_eq!(engine.lookup(ROOT_INO, b"renamed.txt").unwrap().ino, 790);
}

#[test]
fn replica_rename_applies_destination_when_reused_source_has_multiple_hardlinks() {
    let engine = temp_engine();
    let remote_entry = DirEntryValue {
        ino: 792,
        kind: FileType::RegularFile,
    };
    let local_entry = DirEntryValue {
        ino: 793,
        kind: FileType::RegularFile,
    };
    let remote_attr = InodeAttr::new_file(remote_entry.ino, ROOT_INO, 1000, 1000, 0o644);
    let local_attr = InodeAttr::new_file(local_entry.ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: remote_entry.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"hardlink-a.txt".to_vec(),
                entry: remote_entry.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"hardlink-b.txt".to_vec(),
                entry: remote_entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: remote_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: remote_entry.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"hardlink-a.txt".to_vec(),
                entry: remote_entry.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"hardlink-b.txt".to_vec(),
                entry: remote_entry.clone(),
            },
        ])
        .unwrap();

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: local_attr.clone(),
            },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: local_entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr: local_attr },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: local_entry,
            },
        ])
        .unwrap();

    let remote_rename = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed.txt",
        remote_entry,
    );
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&remote_rename))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&remote_rename))
        .unwrap();

    assert_eq!(engine.lookup(ROOT_INO, b"source.txt").unwrap().ino, 793);
    assert_eq!(engine.lookup(ROOT_INO, b"renamed.txt").unwrap().ino, 792);
    assert_eq!(engine.lookup(ROOT_INO, b"hardlink-a.txt").unwrap().ino, 792);
    assert_eq!(engine.lookup(ROOT_INO, b"hardlink-b.txt").unwrap().ino, 792);
}

#[test]
fn test_replica_aux_block_events_accept_fragmented_base_graph() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"replica-fragmented-base.bin", 1000, 1000, 0o644)
        .unwrap();

    let mut base_chunk = RegularFileChunkState::default();
    base_chunk.slices.insert(
        SliceId(0),
        SliceRecord {
            slice_id: SliceId(0),
            order: SliceOrder(0),
            range: SliceRange::new(0, 4096).unwrap(),
            payload: Some(PayloadRef::new_partial(71, 4096)),
            payload_state: PayloadRefState::Published,
        },
    );
    base_chunk.slices.insert(
        SliceId(1),
        SliceRecord {
            slice_id: SliceId(1),
            order: SliceOrder(1),
            range: SliceRange::new(4096, 8192).unwrap(),
            payload: Some(PayloadRef::new_partial(72, 4096)),
            payload_state: PayloadRefState::Published,
        },
    );
    base_chunk.next_slice_id = SliceId(2);
    base_chunk.next_slice_order = SliceOrder(2);
    base_chunk.refresh_fragmentation_metrics();
    set_regular_file_chunk_rows(&engine, file.ino, &[], &[(ChunkId(0), base_chunk)]);
    let fragmented_spans = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap();
    assert_eq!(
        fragmented_spans
            .iter()
            .filter(|span| span.payload.is_some())
            .count(),
        2
    );

    let replacement = PayloadRef::new_full(73, DEFAULT_BLOCK_SIZE);
    let replacement_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&replacement),
        PayloadRefState::Published,
    )
    .unwrap();
    engine
        .apply_replica_aux_state(&[crate::events::MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: replacement_slices,
        }])
        .expect("slice-native base replay should update fragmented base graph");
    let replacement_spans = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap();
    assert_eq!(replacement_spans.len(), 1);
    assert_eq!(
        replacement_spans[0]
            .payload
            .as_ref()
            .expect("replacement payload slice")
            .object_id,
        replacement.object_id
    );

    engine
        .apply_replica_aux_state(&[crate::events::MetaEvent::PublishRegularFileBaseSlices {
            ino: file.ino,
            chunk_slices: Vec::new(),
        }])
        .expect("slice-native empty base replay should clear fragmented base graph");
    let cleared_spans = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap();
    assert!(
        cleared_spans.iter().all(|span| span.payload.is_none()),
        "empty base replay should leave no object-backed slices"
    );
}

#[test]
fn replica_serving_stale_ready_attr_restores_newer_base_version() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-stale-ready.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();

    let remote_winner = PayloadRef::new_full(90_003, 227);
    let remote_winner_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        227,
        Some(&remote_winner),
        PayloadRefState::Published,
    )
    .unwrap();
    let mut base_attr = engine.get_attr(file.ino).unwrap();
    base_attr.size = 227;
    base_attr.committed_size = 227;
    base_attr.content_epoch = 3;
    base_attr.committed_content_epoch = 3;
    base_attr.upload_state = UploadState::Ready;
    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::PublishRegularFileBaseSlices {
                ino: file.ino,
                chunk_slices: remote_winner_slices,
            },
            crate::events::MetaEvent::SetAttr {
                attr: base_attr.clone(),
            },
        ])
        .unwrap();

    let mut updating_attr = base_attr.clone();
    updating_attr.size = 446;
    updating_attr.committed_size = 223;
    updating_attr.content_epoch = 2;
    updating_attr.committed_content_epoch = 1;
    updating_attr.upload_state = UploadState::Updating;
    let local_overlay = PayloadRef::new_full(90_004, 446);
    let local_overlay_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        446,
        Some(&local_overlay),
        PayloadRefState::LocalOnly,
    )
    .unwrap();
    engine
        .apply_replica_serving_state(&[
            crate::events::MetaEvent::SetRegularFileOverlaySlices {
                ino: file.ino,
                chunk_slices: local_overlay_slices,
            },
            crate::events::MetaEvent::SetAttr {
                attr: updating_attr.clone(),
            },
        ])
        .unwrap();

    let mut stale_ready_attr = updating_attr;
    stale_ready_attr.committed_size = 223;
    stale_ready_attr.committed_content_epoch = 2;
    stale_ready_attr.upload_state = UploadState::Ready;
    engine
        .apply_replica_serving_state(&[crate::events::MetaEvent::SetAttr {
            attr: stale_ready_attr,
        }])
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, 227)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(object_ids, vec![remote_winner.object_id]);
    assert_eq!(engine.get_attr(file.ino).unwrap(), base_attr);
}

#[test]
fn pending_serving_replay_does_not_restore_equal_ready_state_from_larger_base() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"pending-serving-replay-ready.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();

    let mut ready_attr = engine.get_attr(file.ino).unwrap();
    ready_attr.size = 223;
    ready_attr.committed_size = 223;
    ready_attr.content_epoch = 3;
    ready_attr.committed_content_epoch = 3;
    ready_attr.upload_state = UploadState::Ready;
    engine
        .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
            attr: ready_attr.clone(),
        }])
        .unwrap();
    engine
        .apply_replica_serving_state(&[crate::events::MetaEvent::SetAttr {
            attr: ready_attr.clone(),
        }])
        .unwrap();

    let mut larger_base_attr = ready_attr.clone();
    larger_base_attr.size = 227;
    larger_base_attr.committed_size = 227;
    engine
        .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
            attr: larger_base_attr,
        }])
        .unwrap();

    engine
        .apply_replica_serving_state_for_pending_replay(&[crate::events::MetaEvent::SetAttr {
            attr: ready_attr.clone(),
        }])
        .unwrap();

    assert_eq!(engine.get_attr(file.ino).unwrap(), ready_attr);
}

#[test]
fn replica_serving_base_publish_replaces_stale_overlay_payload() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-base-publish.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    attr.content_epoch = 2;
    attr.committed_content_epoch = 2;
    attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    engine.set_attr(&attr).unwrap();

    let stale_overlay = PayloadRef::new_full(90_001, DEFAULT_BLOCK_SIZE);
    let stale_overlay_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&stale_overlay),
        PayloadRefState::LocalOnly,
    )
    .unwrap();
    engine
        .apply_replica_serving_state(&[MetaEvent::SetRegularFileOverlaySlices {
            ino: file.ino,
            chunk_slices: stale_overlay_slices,
        }])
        .unwrap();

    let remote_winner = PayloadRef::new_full(90_002, DEFAULT_BLOCK_SIZE);
    let remote_winner_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&remote_winner),
        PayloadRefState::Published,
    )
    .unwrap();
    let publish_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: remote_winner_slices,
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&publish_event))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&publish_event))
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![remote_winner.object_id],
        "serving reads must stop shadowing the PG/base payload winner"
    );
}

#[test]
fn replica_serving_base_publish_preserves_unpublished_local_overlay() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-local-overlay.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    attr.content_epoch = 2;
    attr.committed_content_epoch = 1;
    attr.upload_state = UploadState::Updating;
    engine.set_attr(&attr).unwrap();

    let local_overlay = PayloadRef::new_full(91_001, DEFAULT_BLOCK_SIZE);
    let local_overlay_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&local_overlay),
        PayloadRefState::LocalOnly,
    )
    .unwrap();
    engine
        .apply_replica_serving_state(&[MetaEvent::SetRegularFileOverlaySlices {
            ino: file.ino,
            chunk_slices: local_overlay_slices,
        }])
        .unwrap();

    let remote_winner = PayloadRef::new_full(91_002, DEFAULT_BLOCK_SIZE);
    let remote_winner_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&remote_winner),
        PayloadRefState::Published,
    )
    .unwrap();
    let publish_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: remote_winner_slices,
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&publish_event))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&publish_event))
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![local_overlay.object_id],
        "base publish must not discard unpublished local overlay content"
    );
}

#[test]
fn replica_serving_overlay_replay_preserves_uploaded_payload_state() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-stale-overlay.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    attr.content_epoch = 7;
    attr.size = 8192;

    let first = PayloadRef::new_partial(91_101, 4096);
    let second = PayloadRef::new_partial(91_102, 4096);
    engine
        .flush_regular_file_overlay_slices_with_attr(
            attr.ino,
            &[(
                ChunkId(0),
                vec![
                    SliceRecord::new(
                        SliceId::default(),
                        SliceOrder::default(),
                        SliceRange::new(0, 4096).unwrap(),
                        first,
                    ),
                    SliceRecord::new(
                        SliceId::default(),
                        SliceOrder::default(),
                        SliceRange::new(4096, 8192).unwrap(),
                        second,
                    ),
                ],
            )],
            Some(&attr),
        )
        .unwrap();
    mark_overlay_objects_uploaded(&engine, attr.ino, 7, &[first.object_id]);

    let chunk_key = encode_regular_file_chunk_key(attr.ino, ChunkId(0));
    let stale_slices = {
        let txn = engine.begin_read_txn().unwrap();
        let table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let row = table
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("uploaded overlay chunk");
        let mut chunk: RegularFileChunkState = decode(row.value()).unwrap();
        chunk
            .slices
            .values_mut()
            .find(|slice| slice.payload == Some(first))
            .expect("first payload slice")
            .payload_state = PayloadRefState::LocalOnly;
        chunk.slices.values().cloned().collect::<Vec<_>>()
    };

    engine
        .apply_replica_serving_state(&[MetaEvent::SetRegularFileOverlaySlices {
            ino: attr.ino,
            chunk_slices: vec![(ChunkId(0), stale_slices)],
        }])
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
    let table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let row = table
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("replayed overlay chunk");
    let chunk: RegularFileChunkState = decode(row.value()).unwrap();
    assert_eq!(
        chunk
            .slices
            .values()
            .find(|slice| slice.payload == Some(first))
            .expect("first payload after replay")
            .payload_state,
        PayloadRefState::Uploaded,
        "stale serving replay must not roll back an upload completion"
    );
}

#[test]
fn replica_serving_ready_attr_after_publish_replaces_stale_overlay_payload() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-ready-after-publish.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();

    let stale_overlay = PayloadRef::new_full(92_101, DEFAULT_BLOCK_SIZE);
    let stale_overlay_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&stale_overlay),
        PayloadRefState::LocalOnly,
    )
    .unwrap();
    let mut stale_attr = engine.get_attr(file.ino).unwrap();
    stale_attr.size = u64::from(DEFAULT_BLOCK_SIZE) - 11;
    stale_attr.committed_size = stale_attr.size;
    stale_attr.content_epoch = 2;
    stale_attr.committed_content_epoch = 2;
    stale_attr.upload_state = UploadState::Ready;
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetRegularFileOverlaySlices {
                ino: file.ino,
                chunk_slices: stale_overlay_slices,
            },
            MetaEvent::SetAttr {
                attr: stale_attr.clone(),
            },
        ])
        .unwrap();

    let remote_winner = PayloadRef::new_full(92_102, DEFAULT_BLOCK_SIZE);
    let remote_winner_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&remote_winner),
        PayloadRefState::Published,
    )
    .unwrap();
    let publish_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: remote_winner_slices,
    };
    let mut updating_attr = stale_attr.clone();
    updating_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    updating_attr.content_epoch = 3;
    updating_attr.committed_content_epoch = 2;
    updating_attr.committed_size = stale_attr.size;
    updating_attr.upload_state = UploadState::Updating;
    let mut ready_attr = updating_attr.clone();
    ready_attr.committed_content_epoch = 3;
    ready_attr.committed_size = ready_attr.size;
    ready_attr.upload_state = UploadState::Ready;

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr {
                attr: updating_attr.clone(),
            },
            publish_event.clone(),
            MetaEvent::SetAttr {
                attr: ready_attr.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr {
                attr: updating_attr,
            },
            publish_event,
            MetaEvent::SetAttr { attr: ready_attr },
        ])
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![remote_winner.object_id],
        "final Ready attr must stop a stale serving overlay from shadowing the base winner"
    );
}

#[test]
fn reconcile_regular_file_serving_from_base_resets_stale_ready_attr() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"reconcile-serving-attr.bin", 1000, 1000, 0o644)
        .unwrap();

    let mut base_attr = engine.get_attr(file.ino).unwrap();
    base_attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    base_attr.committed_size = u64::from(DEFAULT_BLOCK_SIZE);
    base_attr.content_epoch = 1;
    base_attr.committed_content_epoch = 1;
    base_attr.upload_state = UploadState::Ready;
    let base_payload = PayloadRef::new_full(94_001, DEFAULT_BLOCK_SIZE);
    let base_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&base_payload),
        PayloadRefState::Published,
    )
    .unwrap();
    engine
        .apply_replica_events_to_base(&[
            MetaEvent::PublishRegularFileBaseSlices {
                ino: file.ino,
                chunk_slices: base_slices,
            },
            MetaEvent::SetAttr {
                attr: base_attr.clone(),
            },
        ])
        .unwrap();

    let mut stale_serving_attr = base_attr.clone();
    stale_serving_attr.size = u64::from(DEFAULT_BLOCK_SIZE) + 223;
    stale_serving_attr.committed_size = stale_serving_attr.size;
    engine
        .apply_replica_serving_state(&[MetaEvent::SetAttr {
            attr: stale_serving_attr,
        }])
        .unwrap();
    assert_eq!(
        engine.get_attr(file.ino).unwrap().size,
        u64::from(DEFAULT_BLOCK_SIZE) + 223
    );
    assert!(
        engine
            .local_overlay_attr_inodes()
            .unwrap()
            .contains(&file.ino),
        "stale serving attr should be visible as a local attr overlay before reconciliation"
    );

    assert!(
        engine
            .reconcile_regular_file_serving_from_base(&HashSet::from([file.ino]))
            .unwrap(),
        "reconciliation should rewrite the stale serving mirror"
    );

    let attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(attr.size, u64::from(DEFAULT_BLOCK_SIZE));
    assert_eq!(attr.committed_size, u64::from(DEFAULT_BLOCK_SIZE));
    assert!(
        !engine
            .local_overlay_attr_inodes()
            .unwrap()
            .contains(&file.ino),
        "serving attr should match the base attr after reconciliation"
    );
    assert_eq!(
        engine
            .lookup_regular_file_slices(file.ino, 0, attr.size)
            .unwrap()
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect::<Vec<_>>(),
        vec![base_payload.object_id]
    );
}

#[test]
fn reconcile_regular_file_serving_from_base_removes_stale_namespace_alias() {
    let engine = temp_engine();
    let ino = 95_001;
    let mut attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);
    attr.size = 39;
    attr.committed_size = 39;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 1;
    attr.upload_state = UploadState::Ready;
    let entry = DirEntryValue {
        ino,
        kind: FileType::RegularFile,
    };

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"stale-after-delete.txt".to_vec(),
                entry: entry.clone(),
            },
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            MetaEvent::SetAttr { attr: attr.clone() },
            MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"stale-after-delete.txt".to_vec(),
                entry,
            },
        ])
        .unwrap();
    assert!(engine.lookup(ROOT_INO, b"stale-after-delete.txt").is_ok());

    engine
        .apply_replica_events_to_base(&[
            MetaEvent::RemoveEntry {
                parent: ROOT_INO,
                name: b"stale-after-delete.txt".to_vec(),
            },
            MetaEvent::SetAttr { attr: attr.clone() },
        ])
        .unwrap();

    assert!(
        engine.lookup(ROOT_INO, b"stale-after-delete.txt").is_ok(),
        "setup should reproduce a serving dentry that outlived the base namespace"
    );
    assert!(
        engine
            .reconcile_regular_file_serving_from_base(&HashSet::from([ino]))
            .unwrap(),
        "regular file serving reconciliation should notice the stale namespace alias"
    );
    assert!(
        engine.lookup(ROOT_INO, b"stale-after-delete.txt").is_err(),
        "stale serving dentries must follow the base namespace after local overlay clears"
    );
    assert_eq!(
        engine.get_attr(ino).unwrap().size,
        39,
        "the payload/attr may remain addressable by inode, but the deleted path must not"
    );
}

#[test]
fn protected_base_publish_merges_disjoint_remote_ranges_without_losing_newer_local_ranges() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"protected-base-merge.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let newer_local_tail = PayloadRef::new_partial(93_001, 4096);
    engine
        .stage_payload_slice(file.ino, 3, &newer_local_tail)
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[newer_local_tail.object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );
    let local_object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(local_object_ids, vec![newer_local_tail.object_id]);

    let older_remote_middle = PayloadRef::new_partial(93_002, 4096);
    let older_remote_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: RedbMetaEngine::chunk_slices_for_block(
            2,
            DEFAULT_BLOCK_SIZE,
            Some(&older_remote_middle),
            PayloadRefState::Published,
        )
        .unwrap(),
    };
    engine
        .apply_replica_events_to_base_preserving_protected_base_ranges(
            std::slice::from_ref(&older_remote_event),
            &HashSet::from([file.ino]),
        )
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![older_remote_middle.object_id, newer_local_tail.object_id],
        "an older protected publish should fill disjoint holes without replacing newer local base ranges"
    );
}

#[test]
fn protected_base_publish_does_not_shadow_overlapping_local_range() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"protected-base-overlap.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let newer_local_tail = PayloadRef::new_partial(93_011, 4096);
    engine
        .stage_payload_slice(file.ino, 3, &newer_local_tail)
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[newer_local_tail.object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );

    let stale_remote_tail = PayloadRef::new_partial(93_012, 4096);
    let stale_remote_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: RedbMetaEngine::chunk_slices_for_block(
            3,
            DEFAULT_BLOCK_SIZE,
            Some(&stale_remote_tail),
            PayloadRefState::Published,
        )
        .unwrap(),
    };
    engine
        .apply_replica_events_to_base_preserving_protected_base_ranges(
            std::slice::from_ref(&stale_remote_event),
            &HashSet::from([file.ino]),
        )
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![newer_local_tail.object_id],
        "a protected stale publish must not shadow an overlapping newer local base range"
    );
}

#[test]
fn replica_base_publish_replaces_disjoint_graph_for_unprotected_inode() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"base-merge-disjoint.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let local_tail = PayloadRef::new_partial(93_101, 4096);
    engine
        .stage_payload_slice(file.ino, 3, &local_tail)
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[local_tail.object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );

    let remote_middle = PayloadRef::new_partial(93_102, 4096);
    let remote_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(2 * DEFAULT_BLOCK_SIZE, (2 * DEFAULT_BLOCK_SIZE) + 4096)
                    .unwrap(),
                payload: Some(remote_middle),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&remote_event))
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![remote_middle.object_id],
        "unprotected publish replay should replace the local base graph with the incoming authoritative graph"
    );
}

#[test]
fn replica_aux_base_publish_replaces_disjoint_graph_for_unprotected_inode() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"aux-base-merge-disjoint.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = (3 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096;
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let local_tail = PayloadRef::new_partial(93_201, 4096);
    engine
        .stage_payload_slice(file.ino, 3, &local_tail)
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[local_tail.object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );

    let remote_middle = PayloadRef::new_partial(93_202, 4096);
    let remote_event = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(2 * DEFAULT_BLOCK_SIZE, (2 * DEFAULT_BLOCK_SIZE) + 4096)
                    .unwrap(),
                payload: Some(remote_middle),
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    engine
        .apply_replica_aux_state(std::slice::from_ref(&remote_event))
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, (4 * u64::from(DEFAULT_BLOCK_SIZE)) + 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![remote_middle.object_id],
        "aux replay should replace the local base graph with the incoming authoritative graph"
    );
}

#[test]
fn replica_base_publish_tombstone_clears_stale_unprotected_range() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"base-tombstone-replace.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.committed_content_epoch = 3;
    attr.size = u64::from(DEFAULT_BLOCK_SIZE);
    attr.committed_size = attr.size;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let stale_payload = PayloadRef::new_full(93_301, DEFAULT_BLOCK_SIZE);
    engine
        .stage_payload_slice(file.ino, 0, &stale_payload)
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[stale_payload.object_id]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );

    let remote_hole = MetaEvent::PublishRegularFileBaseSlices {
        ino: file.ino,
        chunk_slices: vec![(
            ChunkId(0),
            vec![SliceRecord {
                slice_id: SliceId(0),
                order: SliceOrder(0),
                range: SliceRange::new(0, DEFAULT_BLOCK_SIZE).unwrap(),
                payload: None,
                payload_state: PayloadRefState::Published,
            }],
        )],
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&remote_hole))
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert!(
        object_ids.is_empty(),
        "unprotected publish tombstones must remove stale base payloads in the same range"
    );
}

#[test]
fn replica_serving_remove_attr_clears_regular_file_overlay_payload() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-remove-attr.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let overlay = PayloadRef::new_full(92_001, DEFAULT_BLOCK_SIZE);
    let overlay_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&overlay),
        PayloadRefState::LocalOnly,
    )
    .unwrap();
    engine
        .apply_replica_serving_state(&[MetaEvent::SetRegularFileOverlaySlices {
            ino: file.ino,
            chunk_slices: overlay_slices,
        }])
        .unwrap();

    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveAttr { ino: file.ino }])
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert!(
        object_ids.is_empty(),
        "remote remove attr must not leave serving overlay payload for the removed inode"
    );
}

#[test]
fn replica_serving_remove_attr_clears_regular_file_base_payload() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"replica-serving-remove-attr-base.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let base_payload = PayloadRef::new_full(92_201, DEFAULT_BLOCK_SIZE);
    let base_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&base_payload),
        PayloadRefState::Published,
    )
    .unwrap();
    engine
        .apply_replica_events_to_base(&[MetaEvent::SetRegularFileOverlaySlices {
            ino: file.ino,
            chunk_slices: base_slices,
        }])
        .unwrap();

    engine
        .apply_replica_serving_state(&[MetaEvent::RemoveAttr { ino: file.ino }])
        .unwrap();

    let object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, u64::from(DEFAULT_BLOCK_SIZE))
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert!(
        object_ids.is_empty(),
        "remote remove attr must not leave base payload graph for the removed inode"
    );
}
