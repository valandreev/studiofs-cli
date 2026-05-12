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
        let txn = engine.db.begin_read().unwrap();
        let dentries = txn.open_table(BASE_DENTRIES_TABLE).unwrap();
        let by_inode = txn.open_table(BASE_DENTRIES_BY_INODE_TABLE).unwrap();
        assert!(dentries.get(dentry_key.as_slice()).unwrap().is_some());
        assert!(by_inode.get(index_key.as_slice()).unwrap().is_some());
    }

    engine
        .apply_replica_events_to_base(&[MetaEvent::RemoveAttr { ino }])
        .unwrap();

    let txn = engine.db.begin_read().unwrap();
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

    let pg_winner = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-b.txt",
        entry,
    );
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

    let pg_winner = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-a.txt",
        entry,
    );
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

    let pg_later_winner = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-b.txt",
        entry,
    );
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

    let pg_winner = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-b.txt",
        entry,
    );
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

    let remote_rename = replica_rename_event(
        ROOT_INO,
        b"source.txt",
        ROOT_INO,
        b"renamed-a.txt",
        entry,
    );
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
