#[test]
fn test_replica_apply_keeps_namespace_in_base_only() {
    let engine = temp_engine();

    let remote_ino = 555;
    let remote_attr = InodeAttr::new_file(remote_ino, ROOT_INO, 1000, 1000, 0o644);
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
