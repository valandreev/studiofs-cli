#[test]
fn test_recover_pending_create_intent_removes_dangling_dentry() {
    let engine = temp_engine();
    let ino = engine.next_inode().unwrap();
    let intent = PendingCreateRecord {
        ino,
        parent: ROOT_INO,
        name: b"ghost-create.txt".to_vec(),
        kind: FileType::RegularFile,
        created_at: 0,
    };
    engine.add_pending_create(&intent).unwrap();
    engine
        .add_entry(
            ROOT_INO,
            b"ghost-create.txt",
            &DirEntryValue {
                ino,
                kind: FileType::RegularFile,
            },
        )
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"ghost-create.txt").is_ok());
    assert!(engine.get_attr(ino).is_err());

    engine.recover_pending_create_intents().unwrap();

    assert!(engine.lookup(ROOT_INO, b"ghost-create.txt").is_err());
    assert!(engine.list_pending_creates().unwrap().is_empty());
}

#[test]
fn test_recover_undo_logs_replays_deferred_delete() {
    let engine = temp_engine();
    let parent = ROOT_INO;
    let name = b"orphaned-open-unlink.txt";

    let attr = engine.create_file(parent, name, 1000, 1000, 0o644).unwrap();
    let unlinked = engine.unlink_open_handle(parent, name).unwrap();
    assert_eq!(unlinked.ino, attr.ino);

    let deferred_before = engine.list_deferred_deletes().unwrap();
    assert!(deferred_before.iter().any(|r| r.ino == attr.ino));
    let undo_before = engine.list_undo_logs().unwrap();
    assert!(
        undo_before
            .iter()
            .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == attr.ino)
    );
    assert!(engine.get_attr(attr.ino).is_ok());

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.contains(&parent));

    assert!(matches!(
        engine.get_attr(attr.ino),
        Err(MetaError::InodeNotFound(_))
    ));
    let deferred_after = engine.list_deferred_deletes().unwrap();
    assert!(!deferred_after.iter().any(|r| r.ino == attr.ino));
    let undo_after = engine.list_undo_logs().unwrap();
    assert!(
        !undo_after
            .iter()
            .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == attr.ino)
    );
}

#[test]
fn test_recover_undo_logs_replays_deferred_delete_with_fragmented_graph() {
    let engine = temp_engine();
    let parent = ROOT_INO;
    let name = b"orphaned-fragmented-open-unlink.txt";

    let attr = engine.create_file(parent, name, 1000, 1000, 0o644).unwrap();
    let mut file_attr = engine.get_attr(attr.ino).unwrap();
    file_attr.size = DEFAULT_BLOCK_SIZE as u64;
    file_attr.content_epoch = 1;
    engine.set_attr(&file_attr).unwrap();

    let midpoint = DEFAULT_BLOCK_SIZE / 2;
    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.slices.insert(
        SliceId(0),
        SliceRecord {
            slice_id: SliceId(0),
            order: SliceOrder(0),
            range: SliceRange::new(0, midpoint).unwrap(),
            block: Some(BlockRef::new_partial(21_001, midpoint)),
            block_state: BlockRefState::LocalOnly,
        },
    );
    overlay_chunk.slices.insert(
        SliceId(1),
        SliceRecord {
            slice_id: SliceId(1),
            order: SliceOrder(1),
            range: SliceRange::new(midpoint, DEFAULT_BLOCK_SIZE).unwrap(),
            block: Some(BlockRef::new_partial(21_002, DEFAULT_BLOCK_SIZE - midpoint)),
            block_state: BlockRefState::LocalOnly,
        },
    );
    overlay_chunk.refresh_fragmentation_metrics();
    set_regular_file_chunk_rows(&engine, attr.ino, &[(ChunkId(0), overlay_chunk)], &[]);

    assert!(matches!(
        engine.lookup_file_graph_blocks(attr.ino),
        Err(MetaError::NonContiguousRegularFileGraph { .. })
    ));
    let unlinked = engine.unlink_open_handle(parent, name).unwrap();
    assert_eq!(unlinked.ino, attr.ino);

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.contains(&parent));
    assert!(matches!(
        engine.get_attr(attr.ino),
        Err(MetaError::InodeNotFound(_))
    ));
    let pending_deletions: std::collections::BTreeSet<_> = engine
        .get_pending_deletions()
        .unwrap()
        .into_iter()
        .map(|(object_id, _)| object_id)
        .collect();
    assert_eq!(
        pending_deletions,
        std::collections::BTreeSet::from([21_001, 21_002])
    );
}

#[test]
fn test_recover_undo_logs_cleans_stale_deferred_undo_record() {
    let engine = temp_engine();

    let stale = UndoLogRecord {
        op_id: 9_999_001,
        kind: UndoLogKind::DeferredDelete,
        phase: UndoLogPhase::Started,
        ino: 424_242,
        parent: ROOT_INO,
        name: b"stale-deferred-undo.txt".to_vec(),
        created_at: 0,
    };
    engine.append_undo_log(&stale).unwrap();
    assert!(
        engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.op_id == stale.op_id)
    );

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.is_empty());
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.op_id == stale.op_id)
    );
}

#[test]
fn test_recover_undo_logs_replays_pending_and_deferred_mixed() {
    let engine = temp_engine();

    let pending_ino = engine.next_inode().unwrap();
    let pending_name = b"mixed-pending-create.txt";
    let intent = PendingCreateRecord {
        ino: pending_ino,
        parent: ROOT_INO,
        name: pending_name.to_vec(),
        kind: FileType::RegularFile,
        created_at: 0,
    };
    engine.add_pending_create(&intent).unwrap();
    engine
        .add_entry(
            ROOT_INO,
            pending_name,
            &DirEntryValue {
                ino: pending_ino,
                kind: FileType::RegularFile,
            },
        )
        .unwrap();
    assert!(engine.lookup(ROOT_INO, pending_name).is_ok());
    assert!(engine.get_attr(pending_ino).is_err());

    let deferred_name = b"mixed-deferred-delete.txt";
    let deferred_attr = engine
        .create_file(ROOT_INO, deferred_name, 1000, 1000, 0o644)
        .unwrap();
    engine.unlink_open_handle(ROOT_INO, deferred_name).unwrap();
    assert!(
        engine
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|r| r.ino == deferred_attr.ino)
    );

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.contains(&ROOT_INO));

    assert!(engine.lookup(ROOT_INO, pending_name).is_err());
    assert!(engine.list_pending_creates().unwrap().is_empty());
    assert!(matches!(
        engine.get_attr(deferred_attr.ino),
        Err(MetaError::InodeNotFound(_))
    ));
    assert!(
        !engine
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|r| r.ino == deferred_attr.ino)
    );
    assert!(engine.list_undo_logs().unwrap().is_empty());
}

#[test]
fn test_recover_undo_logs_replays_deferred_from_replaying_phase() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"replaying-phase.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    engine
        .unlink_open_handle(ROOT_INO, b"replaying-phase.txt")
        .unwrap();
    assert!(
        engine
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|r| r.ino == ino)
    );

    engine
        .remove_undo_logs_for(UndoLogKind::DeferredDelete, ino)
        .unwrap();
    let replaying = UndoLogRecord {
        op_id: 9_999_111,
        kind: UndoLogKind::DeferredDelete,
        phase: UndoLogPhase::Replaying,
        ino,
        parent: ROOT_INO,
        name: b"replaying-phase.txt".to_vec(),
        created_at: 0,
    };
    engine.append_undo_log(&replaying).unwrap();

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.contains(&ROOT_INO));
    assert!(matches!(
        engine.get_attr(ino),
        Err(MetaError::InodeNotFound(_))
    ));
    assert!(
        !engine
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|r| r.ino == ino)
    );
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == ino)
    );
}

#[test]
fn test_recover_undo_logs_replays_pending_from_replaying_phase() {
    let engine = temp_engine();

    let ino = engine.next_inode().unwrap();
    let name = b"pending-replaying-phase.txt";
    let intent = PendingCreateRecord {
        ino,
        parent: ROOT_INO,
        name: name.to_vec(),
        kind: FileType::RegularFile,
        created_at: 0,
    };
    engine.add_pending_create(&intent).unwrap();
    engine
        .add_entry(
            ROOT_INO,
            name,
            &DirEntryValue {
                ino,
                kind: FileType::RegularFile,
            },
        )
        .unwrap();
    assert!(engine.lookup(ROOT_INO, name).is_ok());
    assert!(engine.get_attr(ino).is_err());

    engine
        .remove_undo_logs_for(UndoLogKind::PendingCreate, ino)
        .unwrap();
    let replaying = UndoLogRecord {
        op_id: 9_999_121,
        kind: UndoLogKind::PendingCreate,
        phase: UndoLogPhase::Replaying,
        ino,
        parent: ROOT_INO,
        name: name.to_vec(),
        created_at: 0,
    };
    engine.append_undo_log(&replaying).unwrap();

    let touched = engine.recover_undo_logs().unwrap();
    assert!(touched.contains(&ROOT_INO));
    assert!(engine.lookup(ROOT_INO, name).is_err());
    assert!(engine.list_pending_creates().unwrap().is_empty());
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
    );
}

#[test]
fn test_remove_undo_logs_for_marks_completed_until_cleanup() {
    let engine = temp_engine();

    let ino = engine.next_inode().unwrap();
    let record = UndoLogRecord {
        op_id: 9_999_131,
        kind: UndoLogKind::PendingCreate,
        phase: UndoLogPhase::Started,
        ino,
        parent: ROOT_INO,
        name: b"completed-lifecycle.txt".to_vec(),
        created_at: 0,
    };
    engine.append_undo_log(&record).unwrap();

    engine
        .remove_undo_logs_for(UndoLogKind::PendingCreate, ino)
        .unwrap();
    let logs_after_mark = engine.list_undo_logs().unwrap();
    let completed = logs_after_mark
        .iter()
        .find(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
        .expect("record should remain until cleanup");
    assert_eq!(completed.phase, UndoLogPhase::Completed);

    engine.cleanup_stale_undo_logs().unwrap();
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
    );
}

#[test]
fn test_remove_deferred_delete_performs_post_cleanup_of_completed_undo() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"deferred-completed.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    engine
        .unlink_open_handle(ROOT_INO, b"deferred-completed.txt")
        .unwrap();

    engine.remove_deferred_delete(ino).unwrap();
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == ino)
    );
}

#[test]
fn test_remove_pending_create_performs_post_cleanup_of_completed_undo() {
    let engine = temp_engine();

    let ino = engine.next_inode().unwrap();
    let intent = PendingCreateRecord {
        ino,
        parent: ROOT_INO,
        name: b"pending-completed.txt".to_vec(),
        kind: FileType::RegularFile,
        created_at: 0,
    };
    engine.add_pending_create(&intent).unwrap();

    engine.remove_pending_create(ino).unwrap();
    assert!(
        !engine
            .list_pending_creates()
            .unwrap()
            .iter()
            .any(|r| r.ino == ino)
    );
    assert!(
        !engine
            .list_undo_logs()
            .unwrap()
            .iter()
            .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == ino)
    );
}

#[test]
fn test_cleanup_stale_undo_logs_removes_completed_even_if_live_rows_exist() {
    let engine = temp_engine();

    let pending_ino = engine.next_inode().unwrap();
    let pending_intent = PendingCreateRecord {
        ino: pending_ino,
        parent: ROOT_INO,
        name: b"live-pending-completed-undo.txt".to_vec(),
        kind: FileType::RegularFile,
        created_at: 0,
    };
    engine.add_pending_create(&pending_intent).unwrap();
    engine
        .remove_undo_logs_for(UndoLogKind::PendingCreate, pending_ino)
        .unwrap();

    let deferred_ino = engine.next_inode().unwrap();
    let deferred = DeferredDeleteRecord {
        ino: deferred_ino,
        parent: ROOT_INO,
        name: b"live-deferred-completed-undo.txt".to_vec(),
        created_at: 0,
    };
    engine.add_deferred_delete(&deferred).unwrap();
    engine
        .append_undo_log(&UndoLogRecord {
            op_id: 9_999_201,
            kind: UndoLogKind::DeferredDelete,
            phase: UndoLogPhase::Started,
            ino: deferred_ino,
            parent: ROOT_INO,
            name: deferred.name.clone(),
            created_at: 0,
        })
        .unwrap();
    engine
        .remove_undo_logs_for(UndoLogKind::DeferredDelete, deferred_ino)
        .unwrap();

    let before = engine.list_undo_logs().unwrap();
    assert!(before.iter().any(|r| {
        r.kind == UndoLogKind::PendingCreate
            && r.ino == pending_ino
            && r.phase == UndoLogPhase::Completed
    }));
    assert!(before.iter().any(|r| {
        r.kind == UndoLogKind::DeferredDelete
            && r.ino == deferred_ino
            && r.phase == UndoLogPhase::Completed
    }));
    assert!(
        engine
            .list_pending_creates()
            .unwrap()
            .iter()
            .any(|r| r.ino == pending_ino)
    );
    assert!(
        engine
            .list_deferred_deletes()
            .unwrap()
            .iter()
            .any(|r| r.ino == deferred_ino)
    );

    engine.cleanup_stale_undo_logs().unwrap();
    let after = engine.list_undo_logs().unwrap();
    assert!(
        !after
            .iter()
            .any(|r| r.kind == UndoLogKind::PendingCreate && r.ino == pending_ino)
    );
    assert!(
        !after
            .iter()
            .any(|r| r.kind == UndoLogKind::DeferredDelete && r.ino == deferred_ino)
    );
}

#[test]
fn test_repeated_pending_and_deferred_cycles_do_not_accumulate_undo_logs() {
    let engine = temp_engine();

    for i in 0..16u64 {
        let pending_ino = engine.next_inode().unwrap();
        let pending_intent = PendingCreateRecord {
            ino: pending_ino,
            parent: ROOT_INO,
            name: format!("loop-pending-{i}.txt").into_bytes(),
            kind: FileType::RegularFile,
            created_at: 0,
        };
        engine.add_pending_create(&pending_intent).unwrap();
        engine.remove_pending_create(pending_ino).unwrap();

        let deferred_name = format!("loop-deferred-{i}.txt");
        let attr = engine
            .create_file(ROOT_INO, deferred_name.as_bytes(), 1000, 1000, 0o644)
            .unwrap();
        engine
            .unlink_open_handle(ROOT_INO, deferred_name.as_bytes())
            .unwrap();
        engine.remove_deferred_delete(attr.ino).unwrap();
    }

    assert!(
        engine.list_undo_logs().unwrap().is_empty(),
        "undo log should remain empty after repeated completed-cleanup cycles"
    );
}
