fn mark_empty_file_published(engine: &RedbMetaEngine, ino: Ino) {
    let mut attr = engine.get_attr(ino).unwrap();
    attr.content_epoch = 1;
    attr.committed_content_epoch = 1;
    engine.set_attr(&attr).unwrap();
}

#[test]
fn test_detect_local_overlay_state_for_dentry_only_against_base() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base_name.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(!engine.has_local_overlay_state().unwrap());

    engine.remove_entry(ROOT_INO, b"base_name.txt").unwrap();

    assert!(engine.has_local_overlay_state().unwrap());
}

#[test]
fn entry_bindings_for_inode_reads_base_and_overlay_index_candidates() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let engine = temp_engine();
    engine
        .import_snapshot(&source.export_snapshot().unwrap())
        .unwrap();
    assert_eq!(
        engine.entry_bindings_for_inode(file.ino).unwrap(),
        vec![(ROOT_INO, b"base.txt".to_vec())]
    );

    engine
        .rename(ROOT_INO, b"base.txt", ROOT_INO, b"local.txt")
        .unwrap();
    let bindings = engine.entry_bindings_for_inode(file.ino).unwrap();
    assert!(bindings.contains(&(ROOT_INO, b"local.txt".to_vec())));
    // Any other row is a stale base candidate that lookup must reject.
    for (parent, name) in bindings.iter().filter(|(_, name)| name != b"local.txt") {
        assert!(engine.lookup(*parent, name).is_err());
    }
    assert!(engine.entry_bindings_for_inode(ROOT_INO).unwrap().is_empty());
}

#[test]
fn test_detect_overlay_dentries_against_imported_base() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(engine.local_overlay_dentries().unwrap().is_empty());

    engine
        .create_file(ROOT_INO, b"local-only.txt", 1000, 1000, 0o644)
        .unwrap();
    let local_key = encode_dentry_key(ROOT_INO, b"local-only.txt");
    let overlay = engine.local_overlay_dentries().unwrap();
    assert!(overlay.contains(&local_key));

    engine.unlink(ROOT_INO, b"base.txt").unwrap();
    let base_key = encode_dentry_key(ROOT_INO, b"base.txt");
    let overlay = engine.local_overlay_dentries().unwrap();
    assert!(overlay.contains(&base_key));
}

#[test]
fn test_lookup_and_readdir_fall_back_to_base_when_no_local_overlay() {
    let engine = temp_engine();

    let mut base_attr = InodeAttr::new_file(77, ROOT_INO, 1000, 1000, 0o644);
    base_attr.content_epoch = 1;
    base_attr.committed_content_epoch = 1;
    let base_entry = DirEntryValue {
        ino: 77,
        kind: FileType::RegularFile,
    };
    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::SetAttr {
                attr: base_attr.clone(),
            },
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"base-only.txt".to_vec(),
                entry: base_entry.clone(),
            },
        ])
        .unwrap();

    let looked = engine.lookup(ROOT_INO, b"base-only.txt").unwrap();
    assert_eq!(looked.ino, 77);

    let entries = engine.readdir(ROOT_INO).unwrap();
    assert!(entries.iter().any(|entry| entry.name == b"base-only.txt"));
}

#[test]
fn test_local_overlay_wins_over_base_read_path() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"shared.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.mode = 0o600;
    engine.set_attr(&attr).unwrap();
    engine
        .rename(ROOT_INO, b"shared.txt", ROOT_INO, b"local.txt")
        .unwrap();

    let loaded = engine.get_attr(file.ino).unwrap();
    assert_eq!(loaded.mode, 0o600);
    assert!(engine.lookup(ROOT_INO, b"shared.txt").is_err());
    let renamed = engine.lookup(ROOT_INO, b"local.txt").unwrap();
    assert_eq!(renamed.ino, file.ino);
}

#[test]
fn test_unlink_base_only_entry_creates_local_tombstone() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    let base_key = encode_dentry_key(ROOT_INO, b"remote.txt");

    engine.unlink(ROOT_INO, b"remote.txt").unwrap();

    assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
    assert!(engine.local_overlay_dentries().unwrap().contains(&base_key));
}

#[test]
fn test_rename_base_only_entry_creates_local_overlay_mapping() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    engine
        .rename(ROOT_INO, b"remote.txt", ROOT_INO, b"local.txt")
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
    let renamed = engine.lookup(ROOT_INO, b"local.txt").unwrap();
    assert_eq!(renamed.ino, file.ino);
}

#[test]
fn test_link_base_only_entry_creates_local_overlay_mapping() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    engine.link(file.ino, ROOT_INO, b"local-link.txt").unwrap();

    let original = engine.lookup(ROOT_INO, b"remote.txt").unwrap();
    let link = engine.lookup(ROOT_INO, b"local-link.txt").unwrap();
    assert_eq!(original.ino, file.ino);
    assert_eq!(link.ino, file.ino);
}

#[test]
fn test_add_entry_rejects_name_visible_from_base() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    let result = engine.add_entry(
        ROOT_INO,
        b"remote.txt",
        &DirEntryValue {
            ino: file.ino,
            kind: FileType::RegularFile,
        },
    );
    assert!(matches!(result, Err(MetaError::EntryExists { .. })));
}

#[test]
fn test_rejected_namespace_mutations_record_aborted_write_metrics() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 42,
        kind: FileType::RegularFile,
    };
    engine.add_entry(ROOT_INO, b"existing", &entry).unwrap();

    let before = engine.metadata_write_observability().mutation.calls;
    assert!(matches!(
        engine.add_entry(ROOT_INO, b"existing", &entry),
        Err(MetaError::EntryExists { .. })
    ));
    assert_eq!(
        engine.metadata_write_observability().mutation.calls - before,
        1
    );

    let before = engine.metadata_write_observability().mutation.calls;
    assert_eq!(
        engine
            .remove_entry_if_matches(ROOT_INO, b"missing", entry.ino)
            .unwrap(),
        None
    );
    assert_eq!(
        engine.metadata_write_observability().mutation.calls - before,
        1
    );

    let before = engine.metadata_write_observability().mutation.calls;
    assert_eq!(
        engine
            .remove_entry_if_matches(ROOT_INO, b"existing", entry.ino + 1)
            .unwrap(),
        None
    );
    assert_eq!(
        engine.metadata_write_observability().mutation.calls - before,
        1
    );
}

#[test]
fn test_remove_entry_base_only_creates_local_tombstone() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    let key = encode_dentry_key(ROOT_INO, b"remote.txt");

    let removed = engine.remove_entry(ROOT_INO, b"remote.txt").unwrap();
    assert_eq!(removed.ino, file.ino);
    assert!(engine.lookup(ROOT_INO, b"remote.txt").is_err());
    assert!(engine.local_overlay_dentries().unwrap().contains(&key));
}

#[test]
fn test_write_transaction_attempt_receiver_can_be_dropped() {
    let engine = temp_engine();
    let (sender, receiver) = std::sync::mpsc::channel();
    WRITE_TXN_ATTEMPT.with(|marker| *marker.borrow_mut() = Some(sender));
    drop(receiver);

    let txn = engine.begin_write_txn().unwrap();
    txn.abort().unwrap();
}

#[test]
fn test_add_entry_rechecks_name_after_concurrent_insert() {
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = temp_engine();
    let next_event = engine.get_counter(NEXT_EVENT_SEQ).unwrap();
    let concurrent = DirEntryValue {
        ino: 42,
        kind: FileType::RegularFile,
    };
    let result = thread::scope(|scope| {
        let txn = engine.begin_write_txn().unwrap();
        let worker_engine = &engine;
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        scope.spawn(move || {
            WRITE_TXN_ATTEMPT.with(|sender| *sender.borrow_mut() = Some(attempt_tx));
            result_tx
                .send(worker_engine.add_entry(
                    ROOT_INO,
                    b"concurrent",
                    &DirEntryValue {
                        ino: 43,
                        kind: FileType::RegularFile,
                    },
                ))
                .unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, b"concurrent", &concurrent)
            .unwrap();
        txn.commit().unwrap();
        result_rx.recv_timeout(Duration::from_secs(5)).unwrap()
    });

    assert!(matches!(result, Err(MetaError::EntryExists { .. })), "{result:?}");
    assert_eq!(engine.lookup(ROOT_INO, b"concurrent").unwrap(), concurrent);
    assert_eq!(engine.get_counter(NEXT_EVENT_SEQ).unwrap(), next_event);
}

#[test]
fn test_remove_entry_returns_current_entry_after_concurrent_replacement() {
    use std::sync::mpsc;
    use std::time::Duration;

    let original = DirEntryValue {
        ino: 42,
        kind: FileType::RegularFile,
    };
    for replacement in [
        DirEntryValue {
            ino: 43,
            kind: FileType::RegularFile,
        },
        DirEntryValue {
            ino: 42,
            kind: FileType::Directory,
        },
    ] {
        let engine = temp_engine();
        engine.add_entry(ROOT_INO, b"concurrent", &original).unwrap();
        let next_event = engine.get_counter(NEXT_EVENT_SEQ).unwrap();
        let result = thread::scope(|scope| {
            let txn = engine.begin_write_txn().unwrap();
            let worker_engine = &engine;
            let (attempt_tx, attempt_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            scope.spawn(move || {
                WRITE_TXN_ATTEMPT.with(|sender| *sender.borrow_mut() = Some(attempt_tx));
                result_tx
                    .send(worker_engine.remove_entry(ROOT_INO, b"concurrent"))
                    .unwrap();
            });
            attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, b"concurrent", &replacement)
                .unwrap();
            txn.commit().unwrap();
            result_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        });

        assert_eq!(result.unwrap(), replacement);
        assert!(matches!(engine.lookup(ROOT_INO, b"concurrent"), Err(MetaError::EntryNotFound { .. })));
        assert_eq!(engine.get_counter(NEXT_EVENT_SEQ).unwrap(), next_event + 1);
    }
}

#[test]
fn test_remove_entry_if_matches_preserves_replacement_before_call() {
    let engine = temp_engine();
    let replacement = DirEntryValue { ino: 43, kind: FileType::RegularFile };
    engine.add_entry(ROOT_INO, b"replaced", &replacement).unwrap();
    let next_event = engine.get_counter(NEXT_EVENT_SEQ).unwrap();

    assert!(engine.remove_entry_if_matches(ROOT_INO, b"replaced", 42).unwrap().is_none());
    assert_eq!(engine.lookup(ROOT_INO, b"replaced").unwrap(), replacement);
    assert_eq!(engine.get_counter(NEXT_EVENT_SEQ).unwrap(), next_event);
}

#[test]
fn test_remove_entry_if_matches_preserves_concurrent_replacement() {
    use std::sync::mpsc;
    use std::time::Duration;

    let engine = temp_engine();
    engine.add_entry(ROOT_INO, b"concurrent", &DirEntryValue { ino: 42, kind: FileType::RegularFile }).unwrap();
    let replacement = DirEntryValue { ino: 43, kind: FileType::RegularFile };
    let next_event = engine.get_counter(NEXT_EVENT_SEQ).unwrap();
    let result = thread::scope(|scope| {
        let txn = engine.begin_write_txn().unwrap();
        let worker_engine = &engine;
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        scope.spawn(move || {
            WRITE_TXN_ATTEMPT.with(|sender| *sender.borrow_mut() = Some(attempt_tx));
            result_tx.send(worker_engine.remove_entry_if_matches(ROOT_INO, b"concurrent", 42)).unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, b"concurrent", &replacement).unwrap();
        txn.commit().unwrap();
        result_rx.recv_timeout(Duration::from_secs(5)).unwrap()
    });

    assert!(result.unwrap().is_none());
    assert_eq!(engine.lookup(ROOT_INO, b"concurrent").unwrap(), replacement);
    assert_eq!(engine.get_counter(NEXT_EVENT_SEQ).unwrap(), next_event);
}

#[test]
fn test_remove_entry_if_matches_removes_base_entry_and_skips_absence() {
    let source = temp_engine();
    let file = source.create_file(ROOT_INO, b"remote", 1000, 1000, 0o644).unwrap();
    mark_empty_file_published(&source, file.ino);
    let engine = temp_engine();
    engine.import_snapshot(&source.export_snapshot().unwrap()).unwrap();
    let next_event = engine.get_counter(NEXT_EVENT_SEQ).unwrap();

    let removed = engine.remove_entry_if_matches(ROOT_INO, b"remote", file.ino).unwrap().unwrap();
    assert_eq!(removed.ino, file.ino);
    assert!(engine.local_overlay_dentries().unwrap().contains(&encode_dentry_key(ROOT_INO, b"remote")));
    assert!(engine.remove_entry_if_matches(ROOT_INO, b"remote", file.ino).unwrap().is_none());
    assert_eq!(engine.get_counter(NEXT_EVENT_SEQ).unwrap(), next_event + 1);
}

#[test]
fn test_lookup_merged_in_write_txn_matches_read_lookup() {
    let source = temp_engine();
    let base_only = source
        .create_file(ROOT_INO, b"base-only.txt", 1000, 1000, 0o644)
        .unwrap();
    let shared = source
        .create_file(ROOT_INO, b"shared.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, base_only.ino);
    mark_empty_file_published(&source, shared.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    let local_only = engine
        .create_file(ROOT_INO, b"local-only.txt", 1000, 1000, 0o644)
        .unwrap();
    engine
        .rename(ROOT_INO, b"shared.txt", ROOT_INO, b"local-name.txt")
        .unwrap();

    let txn = engine.begin_write_txn().unwrap();
    let base_only = RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"base-only.txt")
        .unwrap()
        .expect("base entry visible in write txn");
    let local_name = RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"local-name.txt")
        .unwrap()
        .expect("local rename visible in write txn");
    let shared = RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"shared.txt").unwrap();
    let local_only_lookup =
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"local-only.txt")
            .unwrap()
            .expect("local entry visible in write txn");
    txn.abort().unwrap();

    let base_only_read = engine.lookup(ROOT_INO, b"base-only.txt").unwrap();
    assert_eq!(base_only.ino, base_only_read.ino);
    assert_eq!(base_only.kind, base_only_read.kind);
    let local_name_read = engine.lookup(ROOT_INO, b"local-name.txt").unwrap();
    assert_eq!(local_name.ino, local_name_read.ino);
    assert_eq!(local_name.kind, local_name_read.kind);
    assert!(shared.is_none());
    assert!(engine.lookup(ROOT_INO, b"shared.txt").is_err());
    assert_eq!(local_only_lookup.ino, local_only.ino);
}

#[test]
fn test_readdir_merged_in_write_txn_matches_read_readdir() {
    let source = temp_engine();
    let base_a = source
        .create_file(ROOT_INO, b"base-a.txt", 1000, 1000, 0o644)
        .unwrap();
    let base_b = source
        .create_file(ROOT_INO, b"base-b.txt", 1000, 1000, 0o644)
        .unwrap();
    mark_empty_file_published(&source, base_a.ino);
    mark_empty_file_published(&source, base_b.ino);
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine.unlink(ROOT_INO, b"base-a.txt").unwrap();
    engine
        .create_file(ROOT_INO, b"local-c.txt", 1000, 1000, 0o644)
        .unwrap();
    engine
        .rename(ROOT_INO, b"base-b.txt", ROOT_INO, b"local-b.txt")
        .unwrap();

    let mut expected = engine
        .readdir(ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    expected.sort();

    let txn = engine.begin_write_txn().unwrap();
    let mut merged = RedbMetaEngine::readdir_merged_in_write_txn(&txn, ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    txn.abort().unwrap();
    merged.sort();

    assert_eq!(merged, expected);
}

thread_local! {
    pub(super) static VISIBILITY_SETTINGS_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static VISIBILITY_TABLE_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static OVERLAY_DENTRY_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static PARENT_OVERLAY_DENTRY_ROWS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[test]
fn readdir_of_small_directory_does_not_scan_unrelated_overlay_dentries() {
    let engine = temp_engine();
    let dir = engine
        .create_dir(ROOT_INO, b"small", 1000, 1000, 0o755)
        .unwrap();
    let kept = engine
        .create_file(dir.ino, b"kept", 1000, 1000, 0o644)
        .unwrap();
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut current = txn.open_table(DENTRIES_TABLE).unwrap();
        let mut base = txn.open_table(BASE_DENTRIES_TABLE).unwrap();
        let mut tombstones = txn.open_table(DENTRY_TOMBSTONES_TABLE).unwrap();
        let value = encode(&DirEntryValue {
            ino: kept.ino,
            kind: FileType::RegularFile,
        })
        .unwrap();
        // A locally deleted base entry inside `dir` must stay hidden.
        let removed = encode_dentry_key(dir.ino, b"removed");
        base.insert(removed.as_slice(), value.as_slice()).unwrap();
        tombstones.insert(removed.as_slice(), 1).unwrap();
        for parent in 10_000..20_000 {
            current
                .insert(
                    encode_dentry_key(parent, b"unrelated").as_slice(),
                    value.as_slice(),
                )
                .unwrap();
            tombstones
                .insert(encode_dentry_key(parent, b"gone").as_slice(), 1)
                .unwrap();
        }
    }
    txn.commit().unwrap();

    OVERLAY_DENTRY_SCANS.set(0);
    let names = |entries: Vec<DirEntry>| {
        entries
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>()
    };
    assert_eq!(names(engine.readdir(dir.ino).unwrap()), vec![b"kept".to_vec()]);
    assert_eq!(OVERLAY_DENTRY_SCANS.get(), 0, "read readdir scanned the volume");
    // One current row plus one tombstone row belong to `dir`.
    assert_eq!(PARENT_OVERLAY_DENTRY_ROWS.get(), 2);

    let txn = engine.begin_write_txn().unwrap();
    let merged = RedbMetaEngine::readdir_merged_in_write_txn(&txn, dir.ino).unwrap();
    txn.abort().unwrap();
    assert_eq!(names(merged), vec![b"kept".to_vec()]);
    assert_eq!(
        OVERLAY_DENTRY_SCANS.get(),
        0,
        "write readdir scanned the volume"
    );
    assert_eq!(PARENT_OVERLAY_DENTRY_ROWS.get(), 2);
}

#[test]
fn merged_dentry_point_lookups_and_create_batch_do_not_scan_large_volume() {
    let engine = temp_engine();
    let target = engine
        .create_file(ROOT_INO, b"target", 1000, 1000, 0o644)
        .unwrap();
    let txn = engine.begin_write_txn().unwrap();
    {
        let mut table = txn.open_table(DENTRIES_TABLE).unwrap();
        let value = encode(&DirEntryValue {
            ino: target.ino,
            kind: FileType::RegularFile,
        })
        .unwrap();
        for parent in 10_000..20_000 {
            let key = encode_dentry_key(parent, b"unrelated");
            table.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    txn.commit().unwrap();

    // Prove the counter observes the expensive operation before testing its absence.
    OVERLAY_DENTRY_SCANS.set(0);
    assert_eq!(engine.local_overlay_dentries().unwrap().len(), 10_001);
    assert_eq!(OVERLAY_DENTRY_SCANS.get(), 1);
    OVERLAY_DENTRY_SCANS.set(0);
    assert_eq!(engine.lookup(ROOT_INO, b"target").unwrap().ino, target.ino);
    assert_eq!(
        OVERLAY_DENTRY_SCANS.get(),
        0,
        "read lookup scanned the volume"
    );

    let txn = engine.begin_write_txn().unwrap();
    assert_eq!(
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"target")
            .unwrap()
            .unwrap()
            .ino,
        target.ino
    );
    txn.abort().unwrap();
    assert_eq!(
        OVERLAY_DENTRY_SCANS.get(),
        0,
        "write lookup scanned the volume"
    );

    let mutations: Vec<_> = (0..32)
        .map(|index| NamespaceMutation::CreateFile {
            ino: 30_000 + index,
            parent: ROOT_INO,
            name: format!("batch-{index}").into_bytes(),
            uid: 1000,
            gid: 1000,
            mode: 0o644,
        })
        .collect();
    assert_eq!(
        engine.apply_namespace_mutations(&mutations).unwrap().len(),
        32
    );
    assert_eq!(
        OVERLAY_DENTRY_SCANS.get(),
        0,
        "create batch scanned the volume"
    );
    assert_eq!(engine.lookup(ROOT_INO, b"batch-31").unwrap().ino, 30_031);
}

#[test]
fn merged_dentry_point_lookups_preserve_overlay_and_visibility_semantics() {
    // Equal rows must use remote visibility; differing current rows retain local authority.
    for filter in [false, true] {
        for (name, base_ino, current_ino, tombstone, expected) in [
            (b"base-only".as_slice(), Some(77), None, false, Some(77)),
            (b"current-only".as_slice(), None, Some(88), false, Some(88)),
            (b"different".as_slice(), Some(77), Some(88), false, Some(88)),
            (b"equal".as_slice(), Some(77), Some(77), false, Some(77)),
            (b"deleted".as_slice(), Some(77), Some(88), true, None),
            (b"missing".as_slice(), None, None, false, None),
        ] {
            let engine = temp_engine();
            engine
                .set_remote_namespace_visibility_filter(filter)
                .unwrap();
            let txn = engine.begin_write_txn().unwrap();
            let key = encode_dentry_key(ROOT_INO, name);
            for (definition, ino) in [
                (BASE_DENTRIES_TABLE, base_ino),
                (DENTRIES_TABLE, current_ino),
            ] {
                if let Some(ino) = ino {
                    let value = encode(&DirEntryValue {
                        ino,
                        kind: FileType::RegularFile,
                    })
                    .unwrap();
                    txn.open_table(definition)
                        .unwrap()
                        .insert(key.as_slice(), value.as_slice())
                        .unwrap();
                }
            }
            if tombstone {
                txn.open_table(DENTRY_TOMBSTONES_TABLE)
                    .unwrap()
                    .insert(key.as_slice(), 1)
                    .unwrap();
            }
            // No inode row means equal/base-only entries are hidden by the remote filter.
            let expected = if filter && (current_ino == base_ino || current_ino.is_none()) {
                None
            } else {
                expected
            };
            assert_eq!(
                RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, name)
                    .unwrap()
                    .map(|entry| entry.ino),
                expected,
                "write: {name:?}, filter={filter}"
            );
            txn.commit().unwrap();
            let read = engine.lookup(ROOT_INO, name);
            match expected {
                Some(ino) => assert_eq!(read.unwrap().ino, ino),
                None => assert!(matches!(read, Err(MetaError::EntryNotFound { .. }))),
            }
        }
    }
}

#[test]
fn merged_dentry_point_lookups_preserve_case_insensitive_local_authority() {
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    for (base, ino) in [(true, 77), (false, 88)] {
        let entry = DirEntryValue {
            ino,
            kind: FileType::Directory,
        };
        if base {
            RedbMetaEngine::set_base_dentry_in_txn(&txn, ROOT_INO, b"MiXeD", &entry).unwrap();
        } else {
            RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, b"MiXeD", &entry).unwrap();
        }
    }
    // Local replacements retain authority regardless of the requested casing.
    for (name, expected) in [(b"MiXeD".as_slice(), 88), (b"mixed".as_slice(), 88)] {
        assert_eq!(
            RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, name)
                .unwrap()
                .unwrap()
                .ino,
            expected
        );
    }
    txn.commit().unwrap();
    assert_eq!(engine.lookup(ROOT_INO, b"MiXeD").unwrap().ino, 88);
    assert_eq!(engine.lookup(ROOT_INO, b"mixed").unwrap().ino, 88);
}

#[test]
fn merged_dentry_case_insensitive_lookup_respects_tombstones_and_visibility() {
    for (current_ino, tombstone, filter, expected) in [
        (None, true, false, None),
        (Some(88), true, false, None),
        (Some(88), false, true, Some(88)),
        (Some(77), false, true, None),
    ] {
        let engine = temp_engine();
        engine
            .set_remote_namespace_visibility_filter(filter)
            .unwrap();
        let txn = engine.begin_write_txn().unwrap();
        let key = encode_dentry_key(ROOT_INO, b"MiXeD");
        let entry = |ino| DirEntryValue {
            ino,
            kind: FileType::RegularFile,
        };
        RedbMetaEngine::set_base_dentry_in_txn(&txn, ROOT_INO, b"MiXeD", &entry(77)).unwrap();
        if let Some(ino) = current_ino {
            RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, b"MiXeD", &entry(ino)).unwrap();
        }
        if tombstone {
            txn.open_table(DENTRY_TOMBSTONES_TABLE)
                .unwrap()
                .insert(key.as_slice(), 1)
                .unwrap();
        }
        for name in [b"MiXeD".as_slice(), b"mixed".as_slice()] {
            assert_eq!(
                RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, name)
                    .unwrap()
                    .map(|entry| entry.ino),
                expected
            );
        }
        txn.commit().unwrap();
        for name in [b"MiXeD".as_slice(), b"mixed".as_slice()] {
            match expected {
                Some(ino) => assert_eq!(engine.lookup(ROOT_INO, name).unwrap().ino, ino),
                None => assert!(matches!(
                    engine.lookup(ROOT_INO, name),
                    Err(MetaError::EntryNotFound { .. })
                )),
            }
        }
    }
}

#[test]
fn merged_dentry_case_insensitive_current_only_entry_stays_locally_visible() {
    let engine = temp_engine();
    engine.set_remote_namespace_visibility_filter(true).unwrap();
    let attr = engine
        .create_file(ROOT_INO, b"LoCaL", 1000, 1000, 0o644)
        .unwrap();
    let read_txn = engine.begin_read_txn().unwrap();
    let base = read_txn.open_table(BASE_DENTRIES_TABLE).unwrap();
    let current = read_txn.open_table(DENTRIES_TABLE).unwrap();
    let tombstones = read_txn.open_table(DENTRY_TOMBSTONES_TABLE).unwrap();
    let (_, is_overlay) = RedbMetaEngine::lookup_merged_from_opened_tables(
        &base,
        &current,
        &tombstones,
        || {
            Ok(Some((
                read_txn.open_table(DENTRIES_BY_FOLDED_NAME_TABLE).unwrap(),
                read_txn.open_table(BASE_DENTRIES_BY_FOLDED_NAME_TABLE).unwrap(),
            )))
        },
        ROOT_INO,
        b"local",
    )
    .unwrap();
    assert!(
        is_overlay,
        "resolved local name must retain its authority flag"
    );
    assert_eq!(engine.lookup(ROOT_INO, b"local").unwrap().ino, attr.ino);
    let txn = engine.begin_write_txn().unwrap();
    assert_eq!(
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"local")
            .unwrap()
            .unwrap()
            .ino,
        attr.ino
    );
    txn.abort().unwrap();
}

#[test]
fn merged_dentry_local_and_missing_lookups_skip_visibility_tables() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"local", 1000, 1000, 0o644)
        .unwrap();
    engine.set_remote_namespace_visibility_filter(true).unwrap();
    VISIBILITY_TABLE_OPENS.set(0);
    assert_eq!(engine.lookup(ROOT_INO, b"local").unwrap().ino, attr.ino);
    assert!(matches!(
        engine.lookup(ROOT_INO, b"missing"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(
        VISIBILITY_TABLE_OPENS.get(),
        0,
        "read lookup opened unused tables"
    );
    let txn = engine.begin_write_txn().unwrap();
    assert_eq!(
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"local")
            .unwrap()
            .unwrap()
            .ino,
        attr.ino
    );
    assert!(
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"missing")
            .unwrap()
            .is_none()
    );
    txn.abort().unwrap();
    assert_eq!(
        VISIBILITY_TABLE_OPENS.get(),
        0,
        "write lookup opened unused tables"
    );
    // Equal rows need the visibility tables; verify that the counter sees this path.
    let txn = engine.begin_write_txn().unwrap();
    let key = encode_dentry_key(ROOT_INO, b"local");
    let value = encode(&DirEntryValue {
        ino: attr.ino,
        kind: FileType::RegularFile,
    })
    .unwrap();
    txn.open_table(BASE_DENTRIES_TABLE)
        .unwrap()
        .insert(key.as_slice(), value.as_slice())
        .unwrap();
    txn.commit().unwrap();
    VISIBILITY_TABLE_OPENS.set(0);
    assert_eq!(engine.lookup(ROOT_INO, b"local").unwrap().ino, attr.ino);
    assert_eq!(VISIBILITY_TABLE_OPENS.get(), 5);
    let txn = engine.begin_write_txn().unwrap();
    assert_eq!(
        RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, b"local")
            .unwrap()
            .unwrap()
            .ino,
        attr.ino
    );
    txn.abort().unwrap();
    assert_eq!(VISIBILITY_TABLE_OPENS.get(), 10);
}

#[test]
fn merged_dentry_visibility_skips_settings_and_inode_tables_when_unneeded() {
    let engine = temp_engine();
    engine.set_remote_namespace_visibility_filter(true).unwrap();
    let txn = engine.begin_write_txn().unwrap();
    for (name, kind, in_base) in [
        (b"directory".as_slice(), FileType::Directory, true),
        (b"symlink".as_slice(), FileType::Symlink, true),
        (b"local".as_slice(), FileType::RegularFile, false),
        (b"remote".as_slice(), FileType::RegularFile, true),
    ] {
        let key = encode_dentry_key(ROOT_INO, name);
        let value = encode(&DirEntryValue { ino: 77, kind }).unwrap();
        let definition = if in_base {
            BASE_DENTRIES_TABLE
        } else {
            DENTRIES_TABLE
        };
        txn.open_table(definition)
            .unwrap()
            .insert(key.as_slice(), value.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();
    for (name, expected, settings, tables) in [
        (b"directory".as_slice(), Some(77), 0, 0),
        (b"symlink".as_slice(), Some(77), 0, 0),
        (b"local".as_slice(), Some(77), 0, 0),
        (b"missing".as_slice(), None, 0, 0),
        // A base regular file requires the filter and visibility tables (no attr means hidden).
        (b"remote".as_slice(), None, 1, 5),
    ] {
        VISIBILITY_SETTINGS_READS.set(0);
        VISIBILITY_TABLE_OPENS.set(0);
        match expected {
            Some(ino) => assert_eq!(engine.lookup(ROOT_INO, name).unwrap().ino, ino),
            None => assert!(matches!(
                engine.lookup(ROOT_INO, name),
                Err(MetaError::EntryNotFound { .. })
            )),
        }
        assert_eq!(
            VISIBILITY_SETTINGS_READS.get(),
            settings,
            "read settings: {name:?}"
        );
        assert_eq!(
            VISIBILITY_TABLE_OPENS.get(),
            tables,
            "read visibility: {name:?}"
        );
        VISIBILITY_SETTINGS_READS.set(0);
        VISIBILITY_TABLE_OPENS.set(0);
        let txn = engine.begin_write_txn().unwrap();
        assert_eq!(
            RedbMetaEngine::lookup_merged_in_write_txn(&txn, ROOT_INO, name)
                .unwrap()
                .map(|entry| entry.ino),
            expected
        );
        txn.abort().unwrap();
        assert_eq!(
            VISIBILITY_SETTINGS_READS.get(),
            settings,
            "write settings: {name:?}"
        );
        assert_eq!(
            VISIBILITY_TABLE_OPENS.get(),
            tables,
            "write visibility: {name:?}"
        );
    }
}

#[test]
fn case_insensitive_lookup_visits_only_folded_candidates_in_large_directory() {
    let engine = temp_engine();
    let entry = |ino| DirEntryValue {
        ino,
        kind: FileType::Directory,
    };
    let txn = engine.begin_write_txn().unwrap();
    for index in 0..10_000 {
        let name = format!("entry-{index}");
        if index % 2 == 0 {
            RedbMetaEngine::set_base_dentry_in_txn(&txn, ROOT_INO, name.as_bytes(), &entry(77))
                .unwrap();
        } else {
            RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, name.as_bytes(), &entry(77))
                .unwrap();
        }
    }
    for name in ["\u{212a}", "\u{130}"] {
        RedbMetaEngine::set_dentry_in_txn(&txn, ROOT_INO, name.as_bytes(), &entry(78)).unwrap();
    }
    txn.commit().unwrap();

    for (name, expected, rows) in [
        ("missing", None, 0),
        ("ENTRY-9999", Some(77), 1),
        ("ENTRY-9998", Some(77), 1),
        ("отсутствует", None, 0),
        // The Kelvin sign folds to ASCII `k`; a dotted capital I folds to `i` + U+0307.
        ("k", Some(78), 1),
        ("i\u{307}", Some(78), 1),
    ] {
        FOLDED_DENTRY_INDEX_ROWS.set(0);
        CASE_FALLBACK_MERGED_SCANS.set(0);
        match expected {
            Some(ino) => assert_eq!(engine.lookup(ROOT_INO, name.as_bytes()).unwrap().ino, ino),
            None => assert!(matches!(
                engine.lookup(ROOT_INO, name.as_bytes()),
                Err(MetaError::EntryNotFound { .. })
            )),
        }
        assert_eq!(CASE_FALLBACK_MERGED_SCANS.get(), 1, "{name:?} misses its exact key");
        assert_eq!(FOLDED_DENTRY_INDEX_ROWS.get(), rows, "{name:?}");
    }
}

#[test]
fn single_table_case_insensitive_lookup_visits_only_folded_candidates() {
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    for index in 0..8_000 {
        let entry = DirEntryValue {
            ino: 1_000 + index,
            kind: FileType::RegularFile,
        };
        let name = format!("entry-{index}");
        RedbMetaEngine::set_base_dentry_in_txn(&txn, ROOT_INO, name.as_bytes(), &entry).unwrap();
    }
    for (name, expected, rows) in [
        ("missing", None, 0),
        ("ENTRY-7999", Some(8_999), 1),
        ("entry-0", Some(1_000), 0),
    ] {
        FOLDED_DENTRY_INDEX_ROWS.set(0);
        assert_eq!(
            RedbMetaEngine::lookup_in_write_table(
                &txn,
                BASE_DENTRY_TABLES,
                ROOT_INO,
                name.as_bytes()
            )
            .unwrap()
            .map(|entry| entry.ino),
            expected,
            "{name:?}"
        );
        assert_eq!(FOLDED_DENTRY_INDEX_ROWS.get(), rows, "{name:?}");
    }
    txn.abort().unwrap();
}

#[test]
fn read_only_artifact_without_folded_indexes_keeps_case_insensitive_lookup() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let path = dir.path().join("legacy.db");
    let file_ino = {
        let engine = RedbMetaEngine::open(&path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        let file = engine
            .create_file(ROOT_INO, b"MiXeD", 1000, 1000, 0o644)
            .unwrap();
        // Shape the file like an artifact written before the folded indexes existed.
        let txn = engine.begin_write_txn().unwrap();
        for table in [
            DENTRIES_BY_FOLDED_NAME_TABLE,
            BASE_DENTRIES_BY_FOLDED_NAME_TABLE,
        ] {
            assert!(txn.delete_table(table).unwrap());
        }
        txn.open_table(SETTINGS_TABLE)
            .unwrap()
            .insert(DENTRY_BY_INODE_INDEX_VERSION_SETTING, b"1".as_slice())
            .unwrap();
        txn.commit().unwrap();
        engine.prepare_for_close().unwrap();
        file.ino
    };

    let engine = RedbMetaEngine::open_read_only(&path).unwrap();
    assert_eq!(engine.lookup(ROOT_INO, b"MiXeD").unwrap().ino, file_ino);
    assert_eq!(engine.lookup(ROOT_INO, b"mixed").unwrap().ino, file_ino);
    assert!(matches!(
        engine.lookup(ROOT_INO, b"missing"),
        Err(MetaError::EntryNotFound { .. })
    ));
}

#[test]
fn folded_dentry_names_match_case_insensitive_name_equality() {
    let names: [&[u8]; 14] = [
        b"abc",
        b"ABC",
        b"aBc",
        b"abd",
        b"k",
        b"K",
        "\u{212a}".as_bytes(),
        "\u{130}".as_bytes(),
        "i\u{307}".as_bytes(),
        "Привет".as_bytes(),
        "пРИВЕТ".as_bytes(),
        "ΣΑΣ".as_bytes(),
        &[0xff, b'A'],
        &[0xfe, b'a'],
    ];
    for left in names {
        for right in names {
            assert_eq!(
                fold_dentry_name(left) == fold_dentry_name(right),
                RedbMetaEngine::names_equal_case_insensitive(left, right, &mut None),
                "{:?} vs {:?}",
                String::from_utf8_lossy(left),
                String::from_utf8_lossy(right)
            );
        }
    }
}

#[test]
fn rename_in_large_directory_visits_no_unrelated_dentry_rows() {
    let engine = temp_engine();
    let dir = engine
        .create_dir(ROOT_INO, b"big", 1000, 1000, 0o755)
        .unwrap();
    let mut sources = Vec::new();
    for index in 0..20 {
        let name = format!("src-{index}");
        engine
            .create_file(dir.ino, name.as_bytes(), 1000, 1000, 0o644)
            .unwrap();
        sources.push(name);
    }
    // Filler rows use the same index-maintaining helpers as the namespace writes.
    let txn = engine.begin_write_txn().unwrap();
    for index in 0..8_000 {
        let name = format!("filler-{index}");
        let entry = DirEntryValue {
            ino: 1_000_000 + index,
            kind: FileType::RegularFile,
        };
        if index % 2 == 0 {
            RedbMetaEngine::set_base_dentry_in_txn(&txn, dir.ino, name.as_bytes(), &entry)
                .unwrap();
        } else {
            RedbMetaEngine::set_dentry_in_txn(&txn, dir.ino, name.as_bytes(), &entry).unwrap();
        }
    }
    txn.commit().unwrap();

    FOLDED_DENTRY_INDEX_ROWS.set(0);
    CASE_FALLBACK_MERGED_SCANS.set(0);
    for source in &sources {
        let target = source.replace("src", "dst");
        assert!(matches!(
            engine.lookup(dir.ino, target.as_bytes()),
            Err(MetaError::EntryNotFound { .. })
        ));
        engine
            .rename_atomic(
                dir.ino,
                source.as_bytes(),
                dir.ino,
                target.as_bytes(),
                true,
                false,
            )
            .unwrap();
    }
    // Each rename misses its target twice: once in lookup, once in rename_atomic.
    assert_eq!(CASE_FALLBACK_MERGED_SCANS.get(), 2 * sources.len());
    assert_eq!(FOLDED_DENTRY_INDEX_ROWS.get(), 0);

    // The index follows the rename: the new name resolves by case, the old one is gone.
    FOLDED_DENTRY_INDEX_ROWS.set(0);
    assert!(engine.lookup(dir.ino, b"DST-0").is_ok());
    assert!(matches!(
        engine.lookup(dir.ino, b"SRC-0"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert_eq!(FOLDED_DENTRY_INDEX_ROWS.get(), 1);
}

#[test]
fn unicode_query_lowercase_matches_ascii_stored_name() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"K", 1000, 1000, 0o644)
        .unwrap();
    FOLDED_DENTRY_INDEX_ROWS.set(0);
    assert_eq!(
        engine.lookup(ROOT_INO, "K".as_bytes()).unwrap().ino,
        attr.ino
    );
    assert_eq!(FOLDED_DENTRY_INDEX_ROWS.get(), 1);
}
