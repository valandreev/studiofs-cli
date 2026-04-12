#[test]
fn test_detect_overlay_symlink_against_imported_base() {
    let source = temp_engine();
    let link = source
        .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(engine.local_overlay_symlink_inodes().unwrap().is_empty());

    engine
        .set_symlink_target(link.ino, b"/local/target")
        .unwrap();

    let overlay = engine.local_overlay_symlink_inodes().unwrap();
    assert!(overlay.contains(&link.ino));
}

#[test]
fn test_create_symlink() {
    let engine = temp_engine();

    let attr = engine
        .create_symlink(ROOT_INO, b"link1", b"/tmp/target", 1000, 1000)
        .unwrap();
    assert_eq!(attr.kind, FileType::Symlink);
    assert_eq!(attr.mode, 0o777);
    assert_eq!(attr.size, 11); // length of "/tmp/target"
    assert_eq!(attr.uid, 1000);
    assert_eq!(attr.gid, 1000);

    // Verify target is stored
    let target = engine.get_symlink_target(attr.ino).unwrap();
    assert_eq!(&target, b"/tmp/target");

    // Verify dentry exists
    let looked_up = engine.lookup(ROOT_INO, b"link1").unwrap();
    assert_eq!(looked_up.ino, attr.ino);
    assert_eq!(looked_up.kind, FileType::Symlink);
}

#[test]
fn test_symlink_falls_back_to_base_when_no_local_overlay() {
    let source = temp_engine();
    let link = source
        .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    let target = engine.get_symlink_target(link.ino).unwrap();
    assert_eq!(target, b"/base/target");
}

#[test]
fn test_local_symlink_overlay_wins_over_base() {
    let source = temp_engine();
    let link = source
        .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine
        .set_symlink_target(link.ino, b"/local/target")
        .unwrap();

    let target = engine.get_symlink_target(link.ino).unwrap();
    assert_eq!(target, b"/local/target");
}

#[test]
fn test_remove_attr_base_symlink_creates_local_tombstone() {
    let source = temp_engine();
    let link = source
        .create_symlink(ROOT_INO, b"base-link", b"/base/target", 0, 0)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    engine.remove_attr(link.ino).unwrap();

    assert!(engine.get_symlink_target(link.ino).is_err());
    assert!(
        engine
            .local_overlay_symlink_inodes()
            .unwrap()
            .contains(&link.ino)
    );
}

#[test]
fn test_symlink_target_stored_and_retrieved() {
    let engine = temp_engine();

    let attr = engine
        .create_symlink(ROOT_INO, b"mylink", b"relative/path/to/file", 0, 0)
        .unwrap();

    let target = engine.get_symlink_target(attr.ino).unwrap();
    assert_eq!(&target, b"relative/path/to/file");
}

#[test]
fn test_symlink_remove_cascades() {
    let engine = temp_engine();

    let attr = engine
        .create_symlink(ROOT_INO, b"cascade_link", b"/target", 0, 0)
        .unwrap();
    let ino = attr.ino;

    // Verify symlink target exists
    assert!(engine.get_symlink_target(ino).is_ok());

    // Remove the dentry and attr
    engine.remove_entry(ROOT_INO, b"cascade_link").unwrap();
    engine.remove_attr(ino).unwrap();

    // Symlink target should be cleaned up by remove_attr
    assert!(engine.get_symlink_target(ino).is_err());
}

#[test]
fn test_symlink_duplicate_name_fails() {
    let engine = temp_engine();

    engine
        .create_file(ROOT_INO, b"existing", 0, 0, 0o644)
        .unwrap();

    let result = engine.create_symlink(ROOT_INO, b"existing", b"/target", 0, 0);
    assert!(result.is_err());
}

// === Hardlink Tests ===
