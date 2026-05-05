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
fn test_replica_base_remove_attr_clears_dangling_dentries_for_inode() {
    let engine = temp_engine();
    let ino = 557;
    let attr = InodeAttr::new_file(ino, ROOT_INO, 1000, 1000, 0o644);

    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::SetAttr { attr },
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"dangling.txt".to_vec(),
                entry: DirEntryValue {
                    ino,
                    kind: FileType::RegularFile,
                },
            },
        ])
        .unwrap();
    assert!(
        engine
            .lookup_base_dentry(ROOT_INO, b"dangling.txt")
            .unwrap()
            .is_some()
    );

    engine
        .apply_replica_events_to_base(&[crate::events::MetaEvent::RemoveAttr { ino }])
        .unwrap();

    assert!(
        engine
            .lookup_base_dentry(ROOT_INO, b"dangling.txt")
            .unwrap()
            .is_none(),
        "RemoveAttr should not leave stale base dentries for the removed inode"
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

fn replica_rename_event(name: &[u8], new_name: &[u8], entry: DirEntryValue) -> crate::events::MetaEvent {
    crate::events::MetaEvent::RenameEntry {
        parent: ROOT_INO,
        name: name.to_vec(),
        new_parent: ROOT_INO,
        new_name: new_name.to_vec(),
        entry,
        displaced_set_attr: Box::new(None),
        displaced_remove_attr_ino: None,
        moved_dir_attr: Box::new(None),
        old_parent_attr: Box::new(None),
        new_parent_attr: Box::new(None),
    }
}

#[test]
fn test_replica_base_rename_relocates_inode_when_source_was_already_moved() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 901,
        kind: FileType::RegularFile,
    };

    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
            replica_rename_event(b"source.txt", b"moved-by-a.txt", entry.clone()),
            replica_rename_event(b"source.txt", b"moved-by-b.txt", entry.clone()),
        ])
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"source.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"moved-by-a.txt").is_err());
    assert_eq!(
        engine.lookup(ROOT_INO, b"moved-by-b.txt").unwrap().ino,
        entry.ino
    );
}

#[test]
fn test_replica_serving_rename_relocates_inode_when_source_was_already_moved() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 902,
        kind: FileType::RegularFile,
    };

    engine
        .apply_replica_serving_state(&[
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
            replica_rename_event(b"source.txt", b"moved-by-a.txt", entry.clone()),
            replica_rename_event(b"source.txt", b"moved-by-b.txt", entry.clone()),
        ])
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"source.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"moved-by-a.txt").is_err());
    assert_eq!(
        engine.lookup(ROOT_INO, b"moved-by-b.txt").unwrap().ino,
        entry.ino
    );
}

#[test]
fn test_reconcile_serving_rename_removes_prior_loser_for_same_inode() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 903,
        kind: FileType::RegularFile,
    };

    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
            replica_rename_event(b"source.txt", b"moved-by-a.txt", entry.clone()),
            replica_rename_event(b"source.txt", b"moved-by-b.txt", entry.clone()),
        ])
        .unwrap();
    engine
        .apply_replica_serving_state(&[
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source.txt".to_vec(),
                entry: entry.clone(),
            },
            replica_rename_event(b"source.txt", b"moved-by-a.txt", entry.clone()),
        ])
        .unwrap();

    engine
        .reconcile_serving_rename_from_base(
            ROOT_INO,
            b"source.txt",
            ROOT_INO,
            b"moved-by-b.txt",
            &entry,
        )
        .unwrap();

    assert!(engine.lookup(ROOT_INO, b"source.txt").is_err());
    assert!(engine.lookup(ROOT_INO, b"moved-by-a.txt").is_err());
    assert_eq!(
        engine.lookup(ROOT_INO, b"moved-by-b.txt").unwrap().ino,
        entry.ino
    );
}

#[test]
fn test_replica_base_inode_dentry_index_tracks_hardlink_ambiguity() {
    let engine = temp_engine();
    let entry = DirEntryValue {
        ino: 904,
        kind: FileType::RegularFile,
    };

    engine
        .apply_replica_events_to_base(&[
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source-a.txt".to_vec(),
                entry: entry.clone(),
            },
            crate::events::MetaEvent::AddEntry {
                parent: ROOT_INO,
                name: b"source-b.txt".to_vec(),
                entry: entry.clone(),
            },
            replica_rename_event(b"missing-source.txt", b"winner.txt", entry.clone()),
        ])
        .unwrap();

    assert_eq!(
        engine
            .base_dentry_keys_for_inode_for_test(entry.ino)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(engine.lookup(ROOT_INO, b"source-a.txt").unwrap().ino, entry.ino);
    assert_eq!(engine.lookup(ROOT_INO, b"source-b.txt").unwrap().ino, entry.ino);
    assert!(
        engine.lookup(ROOT_INO, b"winner.txt").is_err(),
        "ambiguous hardlink relocation must not delete an arbitrary link"
    );
}

#[test]
fn test_serving_inode_dentry_index_tracks_add_link_rename_and_unlink() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"source.txt", 1000, 1000, 0o644)
        .unwrap();

    assert_eq!(
        engine
            .serving_dentry_keys_for_inode_for_test(file.ino)
            .unwrap()
            .len(),
        1
    );

    engine.link(file.ino, ROOT_INO, b"linked.txt").unwrap();
    assert_eq!(
        engine
            .serving_dentry_keys_for_inode_for_test(file.ino)
            .unwrap()
            .len(),
        2
    );

    engine
        .rename(ROOT_INO, b"source.txt", ROOT_INO, b"renamed.txt")
        .unwrap();
    let keys = engine
        .serving_dentry_keys_for_inode_for_test(file.ino)
        .unwrap();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&encode_dentry_key(ROOT_INO, b"renamed.txt")));
    assert!(keys.contains(&encode_dentry_key(ROOT_INO, b"linked.txt")));
    assert!(!keys.contains(&encode_dentry_key(ROOT_INO, b"source.txt")));

    engine.unlink(ROOT_INO, b"linked.txt").unwrap();
    assert_eq!(
        engine
            .serving_dentry_keys_for_inode_for_test(file.ino)
            .unwrap(),
        vec![encode_dentry_key(ROOT_INO, b"renamed.txt")]
    );
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
