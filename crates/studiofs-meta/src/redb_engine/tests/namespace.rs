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
    pub(super) static OVERLAY_DENTRY_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
fn merged_dentry_point_lookups_preserve_case_insensitive_base_fallback() {
    let engine = temp_engine();
    let txn = engine.begin_write_txn().unwrap();
    let key = encode_dentry_key(ROOT_INO, b"MiXeD");
    for (definition, ino) in [(BASE_DENTRIES_TABLE, 77), (DENTRIES_TABLE, 88)] {
        let value = encode(&DirEntryValue {
            ino,
            kind: FileType::Directory,
        })
        .unwrap();
        txn.open_table(definition)
            .unwrap()
            .insert(key.as_slice(), value.as_slice())
            .unwrap();
    }
    // The existing fallback searches base first when the requested exact key is absent.
    for (name, expected) in [(b"MiXeD".as_slice(), 88), (b"mixed".as_slice(), 77)] {
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
    assert_eq!(engine.lookup(ROOT_INO, b"mixed").unwrap().ino, 77);
}
