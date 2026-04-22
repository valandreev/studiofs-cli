#[test]
fn test_file_block_operations() {
    let engine = temp_engine();

    // Create a file
    let attr = engine
        .create_file(ROOT_INO, b"test_blocks.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    // Initially no blocks
    assert!(engine.get_file_block(ino, 0).unwrap().is_none());
    assert!(engine.get_file_blocks(ino).unwrap().is_empty());

    // Add a block
    let block1 = BlockRef::new_full(1, 4 * 1024 * 1024);
    let old = engine.set_file_block(ino, 0, &block1).unwrap();
    assert!(old.is_none()); // No previous block

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();
    let overlay_chunk = overlay_chunks.get(&ChunkId(0)).unwrap();
    assert_eq!(overlay_chunk.slices.len(), 1);
    assert_eq!(
        overlay_chunk.slices.values().next().unwrap().block_state,
        BlockRefState::LocalOnly
    );

    // Retrieve the block
    let retrieved = engine.get_file_block(ino, 0).unwrap().unwrap();
    assert_eq!(retrieved.object_id, 1);
    assert_eq!(retrieved.length, 4 * 1024 * 1024);
    assert!(!retrieved.is_compressed());
    assert!(!retrieved.is_partial());

    // Add another block
    let block2 = BlockRef::new_partial(2, 1000);
    engine.set_file_block(ino, 1, &block2).unwrap();

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();

    // Get all blocks
    let all_blocks = engine.get_file_blocks(ino).unwrap();
    assert_eq!(all_blocks.len(), 2);
    assert_eq!(all_blocks[0].0, 0); // block index
    assert_eq!(all_blocks[0].1.object_id, 1);
    assert_eq!(all_blocks[1].0, 1); // block index
    assert_eq!(all_blocks[1].1.object_id, 2);
    assert!(all_blocks[1].1.is_partial());
    assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
}

#[test]
fn test_flush_blocks_batch_updates_regular_file_overlay() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"batch_blocks.txt", 1000, 1000, 0o644)
        .unwrap();

    let results = engine
        .flush_blocks_batch(&[
            BatchFlushEntry {
                ino: attr.ino,
                block_index: 0,
                object_id: 11,
                length: 4 * 1024 * 1024,
                flags: BlockFlags::empty(),
            },
            BatchFlushEntry {
                ino: attr.ino,
                block_index: 1,
                object_id: 12,
                length: 1000,
                flags: BlockFlags::PARTIAL_BLOCK,
            },
        ])
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(engine.get_file_blocks(attr.ino).unwrap().len(), 2);

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        attr.ino,
    )
    .unwrap();
    txn.abort().unwrap();

    assert_eq!(overlay_chunks.len(), 1);
    assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
}

#[test]
fn test_block_runtime_paths_use_graph_tables_only() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"graph-runtime-write.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = DEFAULT_BLOCK_SIZE as u64;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let block = BlockRef::new_full(7_001, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(file.ino, 0, &block).unwrap();
    assert!(engine.commit_uploaded_block(file.ino, 0, 1, &block).unwrap());

    let txn = engine.db.begin_read().unwrap();
    let overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    let chunk_key = encode_regular_file_chunk_key(file.ino, ChunkId(0));
    assert!(
        overlay.get(chunk_key.as_slice()).unwrap().is_some(),
        "current local manifest should still be materialized in overlay rows"
    );
    assert!(
        base.get(chunk_key.as_slice()).unwrap().is_some(),
        "runtime writes should be materialized in regular_file_base rows"
    );
}

#[test]
fn test_file_block_overwrite_returns_old() {
    let engine = temp_engine();

    // Create a file
    let attr = engine
        .create_file(ROOT_INO, b"test_overwrite.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    // Add a block
    let block1 = BlockRef::new_full(1, 4 * 1024 * 1024);
    engine.set_file_block(ino, 0, &block1).unwrap();

    // Overwrite with new block
    let block2 = BlockRef::new_full(2, 4 * 1024 * 1024);
    let old = engine.set_file_block(ino, 0, &block2).unwrap();

    // Old block should be returned for GC
    assert!(old.is_some());
    let old_block = old.unwrap();
    assert_eq!(old_block.object_id, 1);

    // Current block should be the new one
    let current = engine.get_file_block(ino, 0).unwrap().unwrap();
    assert_eq!(current.object_id, 2);
}

#[test]
fn test_partial_block_overwrite_hides_old_tail() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"partial-overwrite.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let full = BlockRef::new_full(101, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(ino, 0, &full).unwrap();

    let partial = BlockRef::new_partial(102, 4096);
    engine.set_file_block(ino, 0, &partial).unwrap();

    let current = engine.get_file_block(ino, 0).unwrap().unwrap();
    assert_eq!(current.object_id, 102);
    assert_eq!(current.length, 4096);
    assert!(current.is_partial());

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();

    assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
}

#[test]
fn test_get_file_block_reads_base_graph_rows() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"committed_only.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let block = BlockRef::new_partial(7, 4096);
    let mut remote_attr = engine.get_attr(ino).unwrap();
    remote_attr.size = 4096;
    remote_attr.committed_size = 4096;
    engine
        .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .unwrap();
    engine
        .apply_replica_aux_state(&[crate::events::MetaEvent::CommitFileBlock {
            ino,
            block_index: 0,
            block: block.clone(),
        }])
        .unwrap();

    let current = engine.get_file_block(ino, 0).unwrap().unwrap();
    assert_eq!(current.object_id, 7);

    let all_blocks = engine.get_file_blocks(ino).unwrap();
    assert_eq!(all_blocks.len(), 1);
    assert_eq!(all_blocks[0].0, 0);
    assert_eq!(all_blocks[0].1.object_id, 7);

    let txn = engine.db.begin_read().unwrap();
    let base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(0));
    assert!(
        base.get(chunk_key.as_slice()).unwrap().is_some(),
        "commit replay must materialize graph rows for runtime reads"
    );
}

#[test]
fn test_synthesize_block_ref_rejects_fragmented_block() {
    let spans = vec![
        ResolvedSliceSpan {
            chunk_id: ChunkId(0),
            range: SliceRange::new(0, 4096).unwrap(),
            source: ResolvedSliceSource::Overlay,
            slice_id: Some(SliceId(1)),
            slice_order: Some(SliceOrder(1)),
            block: Some(BlockRef::new_partial(7, 4096)),
        },
        ResolvedSliceSpan {
            chunk_id: ChunkId(0),
            range: SliceRange::new(4096, 8192).unwrap(),
            source: ResolvedSliceSource::Overlay,
            slice_id: Some(SliceId(2)),
            slice_order: Some(SliceOrder(2)),
            block: Some(BlockRef::new_partial(8, 4096)),
        },
    ];

    let err = RedbMetaEngine::synthesize_block_ref_from_resolved_spans(
        ROOT_INO,
        spans.as_slice(),
        0,
        8192,
    )
    .unwrap_err();
    assert!(matches!(
        err,
        MetaError::FragmentedBlock { ino, block_index }
            if ino == ROOT_INO && block_index == 0
    ));
}

#[test]
fn test_local_staged_block_overrides_committed_block() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"overlay_precedence.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let committed = BlockRef::new_partial(7, 4096);
    engine.set_file_block(ino, 0, &committed).unwrap();
    assert!(engine.commit_uploaded_block(ino, 0, 0, &committed).unwrap());

    let txn = engine.db.begin_write().unwrap();
    let base_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_BASE_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();
    let base_chunk = base_chunks.get(&ChunkId(0)).unwrap();
    assert_eq!(base_chunk.slices.len(), 1);
    assert_eq!(
        base_chunk.slices.values().next().unwrap().block_state,
        BlockRefState::Published
    );

    let staged = BlockRef::new_partial(9, 4096);
    engine.set_file_block(ino, 0, &staged).unwrap();

    let current = engine.get_file_block(ino, 0).unwrap().unwrap();
    assert_eq!(current.object_id, 9);

    let all_blocks = engine.get_file_blocks(ino).unwrap();
    assert_eq!(all_blocks.len(), 1);
    assert_eq!(all_blocks[0].0, 0);
    assert_eq!(all_blocks[0].1.object_id, 9);
}

#[test]
fn test_remove_file_blocks() {
    let engine = temp_engine();

    // Create a file
    let attr = engine
        .create_file(ROOT_INO, b"test_remove.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    // Add multiple blocks
    for i in 0..5 {
        let block = BlockRef::new_full(i as u64 + 1, 4 * 1024 * 1024);
        engine.set_file_block(ino, i, &block).unwrap();
    }

    // Verify blocks exist
    assert_eq!(engine.get_file_blocks(ino).unwrap().len(), 5);

    // Remove all blocks
    let removed_ids = engine.remove_file_blocks(ino).unwrap();
    assert_eq!(removed_ids.len(), 5);
    assert_eq!(removed_ids, vec![1, 2, 3, 4, 5]);

    // Verify no blocks remain
    assert!(engine.get_file_blocks(ino).unwrap().is_empty());
    assert!(engine.get_file_block(ino, 0).unwrap().is_none());

    let txn = engine.db.begin_write().unwrap();
    let overlay_empty = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
    )
    .unwrap();
    let base_empty = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
        &txn,
        REGULAR_FILE_BASE_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();
    assert!(!overlay_empty);
    assert!(!base_empty);
}

#[test]
fn test_remove_file_block_updates_regular_file_graph() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"remove-one.txt", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let committed = BlockRef::new_full(31, 4 * 1024 * 1024);
    engine.set_file_block(ino, 0, &committed).unwrap();
    assert!(engine.commit_uploaded_block(ino, 0, 0, &committed).unwrap());

    let staged = BlockRef::new_full(32, 4 * 1024 * 1024);
    engine.set_file_block(ino, 1, &staged).unwrap();

    let removed = engine.remove_file_block(ino, 0).unwrap();
    assert_eq!(removed, Some(31));

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
    )
    .unwrap();
    let base_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_BASE_TABLE,
        ino,
    )
    .unwrap();
    txn.abort().unwrap();

    assert_eq!(overlay_chunks.len(), 1);
    assert_eq!(overlay_chunks.get(&ChunkId(0)).unwrap().slices.len(), 2);
    assert_eq!(base_chunks.get(&ChunkId(0)).unwrap().slices.len(), 1);
    let txn = engine.db.begin_write().unwrap();
    assert_eq!(
        RedbMetaEngine::base_manifest_from_graph_rows_in_txn(&txn, ino)
            .unwrap()
            .len(),
        0
    );
    txn.abort().unwrap();
    assert_eq!(engine.get_file_blocks(ino).unwrap().len(), 1);
    assert_eq!(
        engine.get_file_block(ino, 1).unwrap().unwrap().object_id,
        32
    );
}

#[test]
fn test_commit_uploaded_block_rejects_stale_epoch() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"stale-epoch.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 3;
    engine.set_attr(&attr).unwrap();

    let block = BlockRef::new_partial(30, 4096);
    engine.set_file_block(file.ino, 0, &block).unwrap();

    assert!(
        !engine
            .commit_uploaded_block(file.ino, 0, 2, &block)
            .unwrap()
    );
    assert!(engine.get_pending_deletions().unwrap().is_empty());
}

#[test]
fn test_commit_uploaded_block_rejects_replaced_block() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"stale-block.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    engine.set_attr(&attr).unwrap();

    let old = BlockRef::new_partial(41, 4096);
    engine.set_file_block(file.ino, 0, &old).unwrap();
    let new = BlockRef::new_partial(42, 4096);
    engine.set_file_block(file.ino, 0, &new).unwrap();

    assert!(!engine.commit_uploaded_block(file.ino, 0, 0, &old).unwrap());
    assert!(engine.commit_uploaded_block(file.ino, 0, 0, &new).unwrap());
}

#[test]
fn zeroio_uploaded_overlay_blocks_make_epoch_publish_ready_only_after_full_epoch_upload() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-publish-ready.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 8192;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let block0 = BlockRef::new_partial(1001, 4096);
    let block1 = BlockRef::new_partial(1002, 4096);
    engine.set_file_block(file.ino, 0, &block0).unwrap();
    engine.set_file_block(file.ino, 1, &block1).unwrap();

    assert!(
        engine
            .mark_file_block_uploaded_if_matches(file.ino, 0, 1, &block0)
            .unwrap()
    );
    let partially_uploaded = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(partially_uploaded.state, InodeLifecycleState::LocalOnly);
    assert_eq!(
        partially_uploaded.retention,
        PayloadRetentionClass::PinnedUnpublished
    );

    assert!(
        engine
            .mark_file_block_uploaded_if_matches(file.ino, 1, 1, &block1)
            .unwrap()
    );
    let publish_ready = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(publish_ready.state, InodeLifecycleState::PublishReady);
    assert_eq!(
        publish_ready.retention,
        PayloadRetentionClass::PublishReadyRetained
    );
    assert_eq!(publish_ready.local_visible_size, 8192);
    assert_eq!(publish_ready.published_size, 0);
    assert_eq!(publish_ready.local_content_epoch, 1);
    assert_eq!(publish_ready.published_content_epoch, 0);
}

#[test]
fn zeroio_zero_length_manifest_epoch_becomes_publish_ready_without_payload_upload() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-zero-length-publish-ready.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let block = BlockRef::new_partial(1_111, 4096);
    engine.set_file_block(file.ino, 0, &block).unwrap();
    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

    let mut truncated = engine.get_attr(file.ino).unwrap();
    truncated.size = 0;
    truncated.content_epoch = 2;
    engine.set_attr(&truncated).unwrap();
    engine.remove_file_blocks(file.ino).unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::PublishReady);
    assert_eq!(
        lifecycle.retention,
        PayloadRetentionClass::PublishReadyRetained
    );
    assert!(lifecycle.has_published_base);
    assert_eq!(lifecycle.local_visible_size, 0);
    assert_eq!(lifecycle.published_size, 4096);
    assert_eq!(lifecycle.local_content_epoch, 2);
    assert_eq!(lifecycle.published_content_epoch, 1);
}

#[test]
fn fragmented_overlay_object_upload_marks_matching_slices_uploaded() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"fragmented-overlay-upload.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 8192;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let chunk_id = ChunkId(0);
    let incoming = vec![
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(0, 4096).unwrap(),
            block: Some(BlockRef::new_partial(2_001, 4096)),
            block_state: BlockRefState::LocalOnly,
        },
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).unwrap(),
            block: Some(BlockRef::new_partial(2_002, 4096)),
            block_state: BlockRefState::LocalOnly,
        },
    ];
    engine
        .flush_regular_file_overlay_slices(file.ino, &[(chunk_id, incoming)])
        .unwrap();

    assert!(matches!(
        engine.get_file_block(file.ino, 0),
        Err(MetaError::FragmentedBlock { ino, block_index })
            if ino == file.ino && block_index == 0
    ));

    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 2_001)
            .unwrap()
    );

    let txn = engine.db.begin_write().unwrap();
    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file.ino,
    )
    .unwrap();
    txn.abort().unwrap();
    let chunk = overlay_chunks.get(&chunk_id).unwrap();
    assert_eq!(chunk.slices.len(), 2);
    let uploaded_state = chunk
        .slices
        .values()
        .find(|slice| slice.block.as_ref().is_some_and(|block| block.object_id == 2_001))
        .unwrap()
        .block_state;
    let pending_state = chunk
        .slices
        .values()
        .find(|slice| slice.block.as_ref().is_some_and(|block| block.object_id == 2_002))
        .unwrap()
        .block_state;
    assert_eq!(uploaded_state, BlockRefState::Uploaded);
    assert_eq!(pending_state, BlockRefState::LocalOnly);
    assert_eq!(
        engine.get_inode_lifecycle(file.ino).unwrap().unwrap().state,
        InodeLifecycleState::LocalOnly
    );

    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 2_002)
            .unwrap()
    );
    assert_eq!(
        engine.get_inode_lifecycle(file.ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady
    );
}

#[test]
fn redb_upload_state_transitions_follow_inode_fields() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"upload-state.bin", 1000, 1000, 0o644)
        .unwrap();
    assert_eq!(
        engine.get_attr(file.ino).unwrap().upload_state,
        UploadState::Ready
    );

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(201, 4096))
        .unwrap();
    let uploading = engine.get_attr(file.ino).unwrap();
    assert_eq!(uploading.upload_state, UploadState::Uploading);
    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());
    let ready = engine.get_attr(file.ino).unwrap();
    assert_eq!(ready.upload_state, UploadState::Ready);
    assert_eq!(ready.committed_content_epoch, 1);

    let mut update_attr = ready.clone();
    update_attr.content_epoch = 2;
    engine.set_attr(&update_attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(202, 4096))
        .unwrap();
    let updating = engine.get_attr(file.ino).unwrap();
    assert_eq!(updating.upload_state, UploadState::Updating);
    assert_eq!(updating.committed_content_epoch, 1);
}

#[test]
fn redb_partial_update_does_not_expose_new_blocks_before_publish() {
    let engine = temp_engine();
    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: "partial-update-test".into(),
        uuid: "partial-update-test-uuid".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "partial-update-test".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "partial-update-test/objects/".into(),
        metadata: MetadataConfig::default(),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine
        .set_format_info(&format_info)
        .expect("failed to set format info");

    let file = engine
        .create_file(ROOT_INO, b"partial-update.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let committed = BlockRef::new_partial(301, 4096);
    engine.set_file_block(file.ino, 0, &committed).unwrap();
    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

    let mut update_attr = engine.get_attr(file.ino).unwrap();
    update_attr.content_epoch = 2;
    engine.set_attr(&update_attr).unwrap();
    let staged = BlockRef::new_partial(302, 4096);
    engine.set_file_block(file.ino, 0, &staged).unwrap();

    let local = engine.get_file_block(file.ino, 0).unwrap().unwrap();
    assert_eq!(local.object_id, 302);

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();

    let remote_attr = remote.get_attr(file.ino).unwrap();
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Updating);
    let remote_block = remote.get_file_block(file.ino, 0).unwrap().unwrap();
    assert_eq!(remote_block.object_id, 301);
}

#[test]
fn new_unpublished_file_is_hidden_from_remote_snapshot() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"hidden-until-publish.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let block = BlockRef::new_partial(303, 4096);
    engine.set_file_block(file.ino, 0, &block).unwrap();

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();

    assert!(matches!(
        remote.lookup(ROOT_INO, b"hidden-until-publish.bin"),
        Err(MetaError::EntryNotFound { .. })
    ));
    assert!(matches!(
        remote.get_attr(file.ino),
        Err(MetaError::InodeNotFound(ino)) if ino == file.ino
    ));

    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());
    let published_snapshot = engine.export_remote_snapshot().unwrap();
    let published_remote = temp_engine();
    published_remote
        .import_remote_snapshot(&published_snapshot)
        .unwrap();

    let published_file = published_remote
        .lookup(ROOT_INO, b"hidden-until-publish.bin")
        .unwrap();
    let published_attr = published_remote.get_attr(published_file.ino).unwrap();
    assert_eq!(published_attr.size, 4096);
    assert_eq!(published_attr.committed_size, 4096);
    assert_eq!(published_attr.upload_state, UploadState::Ready);
    let published_block = published_remote
        .get_file_block(published_file.ino, 0)
        .unwrap()
        .unwrap();
    assert_eq!(published_block.object_id, block.object_id);
}

#[test]
fn empty_new_file_is_hidden_from_remote_snapshot() {
    let engine = temp_engine();

    engine
        .create_file(ROOT_INO, b"empty-hidden.txt", 1000, 1000, 0o644)
        .unwrap();

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();

    assert!(matches!(
        remote.lookup(ROOT_INO, b"empty-hidden.txt"),
        Err(MetaError::EntryNotFound { .. })
    ));
}

#[test]
fn published_empty_file_is_visible_from_remote_snapshot() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"empty-visible.txt", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 1;
    attr.committed_content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();

    let remote_file = remote.lookup(ROOT_INO, b"empty-visible.txt").unwrap();
    assert_eq!(remote_file.ino, file.ino);
    let remote_attr = remote.get_attr(remote_file.ino).unwrap();
    assert_eq!(remote_attr.size, 0);
    assert_eq!(remote_attr.committed_size, 0);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);
}

#[test]
fn redb_publish_file_manifest_if_matches_rejects_manifest_changes() {
    let engine = temp_engine();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let file = engine
        .create_file(ROOT_INO, b"publish-race.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = block_size + 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let block0 = BlockRef::new_full(501, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(file.ino, 0, &block0).unwrap();
    let verified_manifest = engine.get_file_blocks(file.ino).unwrap();

    let block1 = BlockRef::new_partial(502, 4096);
    engine.set_file_block(file.ino, 1, &block1).unwrap();

    assert!(
        !engine
            .publish_file_manifest_if_matches(file.ino, 1, &verified_manifest)
            .unwrap()
    );

    let attr_after = engine.get_attr(file.ino).unwrap();
    assert_eq!(attr_after.upload_state, UploadState::Uploading);
    assert_eq!(attr_after.committed_content_epoch, 0);
}

#[test]
fn zeroio_new_file_starts_local_only() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-local-only.bin", 1000, 1000, 0o644)
        .unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.retention, PayloadRetentionClass::PinnedUnpublished);
    assert_eq!(lifecycle.local_visible_size, 0);
    assert_eq!(lifecycle.published_size, 0);
    assert_eq!(lifecycle.local_content_epoch, 0);
    assert_eq!(lifecycle.published_content_epoch, 0);
    assert!(!lifecycle.has_published_base);
}

#[test]
fn zeroio_published_file_can_carry_new_unpublished_epoch() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-overwrite.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let published = BlockRef::new_partial(601, 4096);
    engine.set_file_block(file.ino, 0, &published).unwrap();
    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

    let mut overwrite = engine.get_attr(file.ino).unwrap();
    overwrite.content_epoch = 2;
    engine.set_attr(&overwrite).unwrap();
    let local = BlockRef::new_partial(602, 4096);
    engine.set_file_block(file.ino, 0, &local).unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.retention, PayloadRetentionClass::PinnedUnpublished);
    assert_eq!(lifecycle.local_visible_size, 4096);
    assert_eq!(lifecycle.published_size, 4096);
    assert_eq!(lifecycle.local_content_epoch, 2);
    assert_eq!(lifecycle.published_content_epoch, 1);
    assert!(lifecycle.has_published_base);
}

#[test]
fn zeroio_append_to_unpublished_file_reuses_same_lifecycle() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-append.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(701, 4096))
        .unwrap();

    let mut appended = engine.get_attr(file.ino).unwrap();
    appended.size = 8192;
    engine.set_attr(&appended).unwrap();
    engine
        .set_file_block(file.ino, 1, &BlockRef::new_partial(702, 4096))
        .unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.local_visible_size, 8192);
    assert_eq!(lifecycle.published_size, 0);
    assert_eq!(lifecycle.local_content_epoch, 1);
    assert_eq!(lifecycle.published_content_epoch, 0);
}

#[test]
fn zeroio_truncate_unpublished_file_reuses_same_lifecycle() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-truncate.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 8192;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(801, 4096))
        .unwrap();
    engine
        .set_file_block(file.ino, 1, &BlockRef::new_partial(802, 4096))
        .unwrap();

    let mut truncated = engine.get_attr(file.ino).unwrap();
    truncated.size = 4096;
    engine.set_attr(&truncated).unwrap();
    engine.remove_file_block(file.ino, 1).unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.local_visible_size, 4096);
    assert_eq!(lifecycle.published_size, 0);
    assert_eq!(lifecycle.local_content_epoch, 1);
    assert_eq!(lifecycle.published_content_epoch, 0);
}

#[test]
fn zeroio_unpublished_lifecycle_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("zeroio-restart.redb");

    let file_ino = {
        let engine = RedbMetaEngine::open(&db_path).unwrap();
        engine.init_root(1000, 1000).unwrap();
        engine
            .set_format_info(&test_format_info("zeroio-restart"))
            .unwrap();
        let file = engine
            .create_file(ROOT_INO, b"restart.bin", 1000, 1000, 0o644)
            .unwrap();
        let mut attr = engine.get_attr(file.ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();
        engine
            .set_file_block(file.ino, 0, &BlockRef::new_partial(901, 4096))
            .unwrap();
        file.ino
    };

    let reopened = RedbMetaEngine::open(&db_path).unwrap();
    let lifecycle = reopened.get_inode_lifecycle(file_ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.local_visible_size, 4096);
    assert_eq!(lifecycle.local_content_epoch, 1);
    assert_eq!(lifecycle.published_content_epoch, 0);
}

#[test]
fn zeroio_metadata_only_local_epoch_keeps_published_payload_warm() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"zeroio-metadata-only.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let published = BlockRef::new_partial(902, 4096);
    engine.set_file_block(file.ino, 0, &published).unwrap();
    assert!(engine.publish_file_manifest(file.ino, 1).unwrap());

    let mut truncated = engine.get_attr(file.ino).unwrap();
    truncated.size = 2048;
    truncated.content_epoch = 2;
    engine.set_attr(&truncated).unwrap();

    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::LocalOnly);
    assert_eq!(lifecycle.retention, PayloadRetentionClass::PublishedWarm);
    assert!(lifecycle.has_published_base);
    assert_eq!(lifecycle.local_visible_size, 2048);
    assert_eq!(lifecycle.published_size, 4096);
    assert_eq!(lifecycle.local_content_epoch, 2);
    assert_eq!(lifecycle.published_content_epoch, 1);
}

#[test]
fn test_next_block_id() {
    let engine = temp_engine();

    let id1 = engine.next_block_id().unwrap();
    let id2 = engine.next_block_id().unwrap();
    let id3 = engine.next_block_id().unwrap();

    assert_eq!(id2, id1 + 1);
    assert_eq!(id3, id2 + 1);
}

// === Symlink Tests ===
