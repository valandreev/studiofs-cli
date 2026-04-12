#[test]
fn test_detect_local_overlay_state_for_dentry_only_against_base() {
    let source = temp_engine();
    source
        .create_file(ROOT_INO, b"base_name.txt", 1000, 1000, 0o644)
        .unwrap();
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
    source
        .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
        .unwrap();
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

    let base_attr = InodeAttr::new_file(77, ROOT_INO, 1000, 1000, 0o644);
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
    source
        .create_file(ROOT_INO, b"remote.txt", 1000, 1000, 0o644)
        .unwrap();
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
    source
        .create_file(ROOT_INO, b"base-only.txt", 1000, 1000, 0o644)
        .unwrap();
    source
        .create_file(ROOT_INO, b"shared.txt", 1000, 1000, 0o644)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    let local_only = engine
        .create_file(ROOT_INO, b"local-only.txt", 1000, 1000, 0o644)
        .unwrap();
    engine
        .rename(ROOT_INO, b"shared.txt", ROOT_INO, b"local-name.txt")
        .unwrap();

    let txn = engine.db.begin_write().unwrap();
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
    source
        .create_file(ROOT_INO, b"base-a.txt", 1000, 1000, 0o644)
        .unwrap();
    source
        .create_file(ROOT_INO, b"base-b.txt", 1000, 1000, 0o644)
        .unwrap();
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

    let txn = engine.db.begin_write().unwrap();
    let mut merged = RedbMetaEngine::readdir_merged_in_write_txn(&txn, ROOT_INO)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    txn.abort().unwrap();
    merged.sort();

    assert_eq!(merged, expected);
}
