#[test]
fn test_detect_overlay_xattr_against_imported_base() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
        .unwrap();
    source
        .set_xattr(file.ino, b"user.color", b"red", 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(engine.local_overlay_xattr_keys().unwrap().is_empty());

    engine
        .set_xattr(file.ino, b"user.color", b"blue", 0)
        .unwrap();

    let overlay = engine.local_overlay_xattr_keys().unwrap();
    assert!(overlay.contains(&encode_xattr_key(file.ino, b"user.color")));
}

#[test]
fn test_xattr_set_get() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    // Set xattr
    engine
        .set_xattr(attr.ino, b"user.color", b"red", 0)
        .unwrap();

    // Get xattr
    let val = engine.get_xattr(attr.ino, b"user.color").unwrap();
    assert_eq!(val, Some(b"red".to_vec()));

    // Non-existent xattr returns None
    let none = engine.get_xattr(attr.ino, b"user.missing").unwrap();
    assert!(none.is_none());
}

#[test]
fn test_resource_fork_xattr_compresses_when_beneficial() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"resource-fork.bin", 0, 0, 0o644)
        .unwrap();

    let value = vec![0x41; 64 * 1024];
    engine
        .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
    let table = txn.open_table(XATTRS_TABLE).unwrap();
    let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
    let stored = table
        .get(key.as_slice())
        .unwrap()
        .expect("stored resource fork xattr")
        .value()
        .to_vec();

    assert!(
        stored.len() < value.len(),
        "expected compressed storage to reduce metadata bytes"
    );
    assert_eq!(
        engine
            .get_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME)
            .unwrap(),
        Some(value)
    );
}

#[test]
fn test_resource_fork_xattr_size_reports_decoded_length() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"resource-fork-size.bin", 0, 0, 0o644)
        .unwrap();

    let value = vec![0x52; 64 * 1024];
    engine
        .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
        .unwrap();
    let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
    let txn = engine.begin_read_txn().unwrap();
    let table = txn.open_table(XATTRS_TABLE).unwrap();
    let stored_len = table
        .get(key.as_slice())
        .unwrap()
        .expect("stored resource fork xattr")
        .value()
        .len();

    assert!(stored_len < value.len());
    assert_eq!(
        engine
            .get_xattr_size(attr.ino, RESOURCE_FORK_XATTR_NAME)
            .unwrap(),
        Some(value.len())
    );
    assert_eq!(
        engine.get_xattr_size(attr.ino, b"user.missing").unwrap(),
        None
    );
}

#[test]
fn test_resource_fork_xattr_keeps_plain_when_not_compressible() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"resource-fork-random.bin", 0, 0, 0o644)
        .unwrap();

    let value: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    engine
        .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
    let table = txn.open_table(XATTRS_TABLE).unwrap();
    let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
    let stored = table
        .get(key.as_slice())
        .unwrap()
        .expect("stored resource fork xattr")
        .value()
        .to_vec();

    assert_eq!(
        stored, value,
        "non-compressible resource fork payload should stay plain"
    );
}

#[test]
fn test_resource_fork_legacy_plain_storage_is_compatible() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"legacy-rfork.bin", 0, 0, 0o644)
        .unwrap();
    let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
    let legacy_plain = b"legacy-plain-resource-fork".to_vec();

    let txn = engine.begin_write_txn().unwrap();
    {
        let mut table = txn.open_table(XATTRS_TABLE).unwrap();
        table
            .insert(key.as_slice(), legacy_plain.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    assert_eq!(
        engine
            .get_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME)
            .unwrap(),
        Some(legacy_plain)
    );
    assert_eq!(
        engine.list_xattrs(attr.ino).unwrap(),
        vec![RESOURCE_FORK_XATTR_NAME.to_vec()]
    );
}

#[test]
fn test_resource_fork_snapshot_exports_plain_value() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"snapshot-rfork.bin", 0, 0, 0o644)
        .unwrap();
    let value = vec![0x33; 8192];
    engine
        .set_xattr(attr.ino, RESOURCE_FORK_XATTR_NAME, value.as_slice(), 0)
        .unwrap();

    let snapshot = engine.export_snapshot().unwrap();
    let key = encode_xattr_key(attr.ino, RESOURCE_FORK_XATTR_NAME);
    let snap_value = snapshot
        .xattrs
        .iter()
        .find_map(|(k, v)| if k == &key { Some(v.clone()) } else { None })
        .expect("snapshot xattr entry");

    assert_eq!(snap_value, value);
}

#[test]
fn test_resource_fork_decode_handles_poisoned_declared_length() {
    let mut framed = Vec::new();
    framed.extend_from_slice(RESOURCE_FORK_ENCODING_MAGIC);
    framed.push(RESOURCE_FORK_ENCODING_VERSION);
    framed.push(RESOURCE_FORK_ENCODING_SCHEME_RLE);
    framed.extend_from_slice(&u32::MAX.to_le_bytes());
    framed.extend_from_slice(&0u32.to_le_bytes());
    framed.push(0); // 1-byte literal run marker, but payload is intentionally incomplete

    let decoded = decode_resource_fork_value_from_storage(framed.as_slice());
    assert_eq!(decoded, framed);
}

#[test]
fn test_xattr_falls_back_to_base_when_no_local_overlay() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
        .unwrap();
    source
        .set_xattr(file.ino, b"user.color", b"red", 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    assert_eq!(
        engine.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"red".to_vec())
    );
    assert_eq!(
        engine.list_xattrs(file.ino).unwrap(),
        vec![b"user.color".to_vec()]
    );
}

#[test]
fn test_local_xattr_overlay_wins_over_base() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
        .unwrap();
    source
        .set_xattr(file.ino, b"user.color", b"red", 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine
        .set_xattr(file.ino, b"user.color", b"blue", 0)
        .unwrap();

    assert_eq!(
        engine.get_xattr(file.ino, b"user.color").unwrap(),
        Some(b"blue".to_vec())
    );
}

#[test]
fn test_remove_base_xattr_creates_local_tombstone() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"base-xattr.txt", 0, 0, 0o644)
        .unwrap();
    source
        .set_xattr(file.ino, b"user.color", b"red", 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine.remove_xattr(file.ino, b"user.color").unwrap();

    assert_eq!(engine.get_xattr(file.ino, b"user.color").unwrap(), None);
    assert!(engine.list_xattrs(file.ino).unwrap().is_empty());
    assert!(
        engine
            .local_overlay_xattr_keys()
            .unwrap()
            .contains(&encode_xattr_key(file.ino, b"user.color"))
    );
}

#[test]
fn test_xattr_overwrite() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    engine.set_xattr(attr.ino, b"user.tag", b"v1", 0).unwrap();
    engine.set_xattr(attr.ino, b"user.tag", b"v2", 0).unwrap();

    let val = engine.get_xattr(attr.ino, b"user.tag").unwrap();
    assert_eq!(val, Some(b"v2".to_vec()));
}

#[test]
fn test_xattr_create_flag() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    // XATTR_CREATE on new key succeeds
    engine.set_xattr(attr.ino, b"user.x", b"a", 1).unwrap();

    // XATTR_CREATE on existing key fails
    let result = engine.set_xattr(attr.ino, b"user.x", b"b", 1);
    assert!(result.is_err());
}

#[test]
fn test_xattr_replace_flag() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    // XATTR_REPLACE on non-existent key fails
    let result = engine.set_xattr(attr.ino, b"user.y", b"a", 2);
    assert!(result.is_err());

    // Set it first, then REPLACE succeeds
    engine.set_xattr(attr.ino, b"user.y", b"a", 0).unwrap();
    engine.set_xattr(attr.ino, b"user.y", b"b", 2).unwrap();
    let val = engine.get_xattr(attr.ino, b"user.y").unwrap();
    assert_eq!(val, Some(b"b".to_vec()));
}

#[test]
fn test_xattr_remove() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    engine.set_xattr(attr.ino, b"user.rm", b"data", 0).unwrap();
    engine.remove_xattr(attr.ino, b"user.rm").unwrap();

    let val = engine.get_xattr(attr.ino, b"user.rm").unwrap();
    assert!(val.is_none());

    // Remove non-existent xattr fails
    let result = engine.remove_xattr(attr.ino, b"user.rm");
    assert!(result.is_err());
}

#[test]
fn test_xattr_list() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    engine.set_xattr(attr.ino, b"user.alpha", b"1", 0).unwrap();
    engine.set_xattr(attr.ino, b"user.beta", b"2", 0).unwrap();
    engine.set_xattr(attr.ino, b"user.gamma", b"3", 0).unwrap();

    let names = engine.list_xattrs(attr.ino).unwrap();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&b"user.alpha".to_vec()));
    assert!(names.contains(&b"user.beta".to_vec()));
    assert!(names.contains(&b"user.gamma".to_vec()));
}

#[test]
fn test_xattr_cascade_on_remove_attr() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"file.txt", 0, 0, 0o644)
        .unwrap();

    engine
        .set_xattr(attr.ino, b"user.keep", b"data", 0)
        .unwrap();

    // Remove the inode — xattrs should be cascade-deleted
    engine.unlink(ROOT_INO, b"file.txt").unwrap();

    // xattr should be gone
    let names = engine.list_xattrs(attr.ino).unwrap();
    assert!(names.is_empty());
}

#[test]
fn test_xattr_isolation_between_inodes() {
    let engine = temp_engine();
    let f1 = engine.create_file(ROOT_INO, b"a.txt", 0, 0, 0o644).unwrap();
    let f2 = engine.create_file(ROOT_INO, b"b.txt", 0, 0, 0o644).unwrap();

    engine
        .set_xattr(f1.ino, b"user.owner", b"alice", 0)
        .unwrap();
    engine.set_xattr(f2.ino, b"user.owner", b"bob", 0).unwrap();

    assert_eq!(
        engine.get_xattr(f1.ino, b"user.owner").unwrap(),
        Some(b"alice".to_vec())
    );
    assert_eq!(
        engine.get_xattr(f2.ino, b"user.owner").unwrap(),
        Some(b"bob".to_vec())
    );

    // list_xattrs should only show each inode's own xattrs
    assert_eq!(engine.list_xattrs(f1.ino).unwrap().len(), 1);
    assert_eq!(engine.list_xattrs(f2.ino).unwrap().len(), 1);
}
