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
            block: Some(BlockRef::new_partial(71, 4096)),
            block_state: BlockRefState::Published,
        },
    );
    base_chunk.slices.insert(
        SliceId(1),
        SliceRecord {
            slice_id: SliceId(1),
            order: SliceOrder(1),
            range: SliceRange::new(4096, 8192).unwrap(),
            block: Some(BlockRef::new_partial(72, 4096)),
            block_state: BlockRefState::Published,
        },
    );
    base_chunk.refresh_fragmentation_metrics();
    set_regular_file_chunk_rows(&engine, file.ino, &[], &[(ChunkId(0), base_chunk)]);
    assert!(matches!(
        engine.lookup_block_projection(file.ino, 0),
        Err(MetaError::NonContiguousRegularFileGraph { .. })
    ));

    let replacement = BlockRef::new_full(73, DEFAULT_BLOCK_SIZE);
    let replacement_slices = RedbMetaEngine::chunk_slices_for_block(
        0,
        DEFAULT_BLOCK_SIZE,
        Some(&replacement),
        BlockRefState::Published,
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
            .block
            .as_ref()
            .expect("replacement block slice")
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
        cleared_spans.iter().all(|span| span.block.is_none()),
        "empty base replay should leave no object-backed slices"
    );
}
