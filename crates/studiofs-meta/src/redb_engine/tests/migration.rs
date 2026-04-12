#[test]
fn test_migrate_regular_files_from_v2_to_v3() {
    use crate::schema::DEFAULT_BLOCK_SIZE;

    let engine = temp_engine();

    // Set up v2 format info to simulate a pre-migration database
    let v2_format_info = FormatInfo {
        format_version: 2, // v2 format
        name: "test-vol".into(),
        uuid: "abc-123".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "test-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&v2_format_info).unwrap();

    // Create a file with some blocks in the old format
    let file_attr = engine
        .create_file(ROOT_INO, b"test_migrate.txt", 0, 0, 0o644)
        .unwrap();

    // Simulate old v2 block storage by directly setting FILE_BLOCKS entries
    let block1 = BlockRef::new_full(1001, DEFAULT_BLOCK_SIZE);
    let block2 = BlockRef::new_partial(1002, DEFAULT_BLOCK_SIZE / 2);

    engine.set_file_block(file_attr.ino, 0, &block1).unwrap();
    engine.set_file_block(file_attr.ino, 1, &block2).unwrap();

    // Verify old blocks are stored
    let old_blocks = engine.get_file_blocks(file_attr.ino).unwrap();
    assert_eq!(old_blocks.len(), 2);
    assert_eq!(old_blocks[0].1.object_id, 1001);
    assert_eq!(old_blocks[1].1.object_id, 1002);

    // Run migration
    engine.migrate_regular_files_to_chunk_slice_model().unwrap();

    // Verify legacy blocks are preserved while the chunk/slice tables are backfilled
    let old_blocks_after = engine.get_file_blocks(file_attr.ino).unwrap();
    assert_eq!(old_blocks_after, old_blocks);

    // Verify new chunk/slice data exists
    let txn = engine.db.begin_write().unwrap();
    let has_overlay = RedbMetaEngine::has_regular_file_chunk_rows_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file_attr.ino,
    )
    .unwrap();
    assert!(has_overlay);

    let overlay_chunks = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file_attr.ino,
    )
    .unwrap();

    // Should have one chunk (both blocks fit in DEFAULT_CHUNK_SIZE)
    assert_eq!(overlay_chunks.len(), 1);
    let chunk_state = overlay_chunks.get(&ChunkId(0)).unwrap();
    assert_eq!(chunk_state.slices.len(), 2);

    // Verify slice data
    let slice_ids: Vec<_> = chunk_state.slices.keys().collect();
    let slice1 = &chunk_state.slices[slice_ids[0]];
    let slice2 = &chunk_state.slices[slice_ids[1]];

    // Check that blocks are correctly mapped
    // Note: slice_ids order depends on BTreeMap ordering, not insertion order
    let block1_ref = slice1.block.as_ref().unwrap();
    let block2_ref = slice2.block.as_ref().unwrap();

    assert_eq!(block1_ref.object_id, 1001);
    assert_eq!(block2_ref.object_id, 1002);
    assert!(!block1_ref.is_partial()); // block1 was new_full
    assert!(block2_ref.is_partial()); // block2 was new_partial
}

#[test]
fn test_migrate_multiple_files_with_different_chunks() {
    use crate::schema::DEFAULT_BLOCK_SIZE;

    let engine = temp_engine();

    // Set up v2 format info to simulate a pre-migration database
    let v2_format_info = FormatInfo {
        format_version: 2,
        name: "test-vol".into(),
        uuid: "abc-123".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "test-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&v2_format_info).unwrap();

    // Create two files
    let file1_attr = engine
        .create_file(ROOT_INO, b"file1.txt", 0, 0, 0o644)
        .unwrap();
    let file2_attr = engine
        .create_file(ROOT_INO, b"file2.txt", 0, 0, 0o644)
        .unwrap();

    // Add blocks to file1 (single chunk)
    let block1 = BlockRef::new_full(2001, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(file1_attr.ino, 0, &block1).unwrap();

    // Add blocks to file2 (will span multiple chunks in new model)
    let block2 = BlockRef::new_full(2002, DEFAULT_BLOCK_SIZE);
    let block3 = BlockRef::new_full(2003, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(file2_attr.ino, 0, &block2).unwrap();
    engine.set_file_block(file2_attr.ino, 1, &block3).unwrap();

    // Run migration
    engine.migrate_regular_files_to_chunk_slice_model().unwrap();

    // Verify both files migrated correctly
    let txn = engine.db.begin_write().unwrap();

    // File 1: single chunk
    let file1_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file1_attr.ino,
    )
    .unwrap();
    assert_eq!(file1_overlay.len(), 1);

    // File 2: should also be single chunk since blocks are contiguous
    // Migration backfills the overlay rows without removing legacy blocks.
    let file2_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file2_attr.ino,
    )
    .unwrap();
    assert_eq!(file2_overlay.len(), 1);
    let chunk_state = file2_overlay.get(&ChunkId(0)).unwrap();
    assert_eq!(chunk_state.slices.len(), 2);
}

#[test]
fn test_migration_validation() {
    use crate::schema::DEFAULT_BLOCK_SIZE;

    let engine = temp_engine();

    // Set up v2 format info to simulate a pre-migration database
    let v2_format_info = FormatInfo {
        format_version: 2,
        name: "test-vol".into(),
        uuid: "abc-123".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "test-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&v2_format_info).unwrap();

    // Create a file with blocks
    let file_attr = engine
        .create_file(ROOT_INO, b"validate.txt", 0, 0, 0o644)
        .unwrap();
    let block = BlockRef::new_full(3001, 1024);
    engine.set_file_block(file_attr.ino, 0, &block).unwrap();

    // Run migration
    engine.migrate_regular_files_to_chunk_slice_model().unwrap();

    // Validation should pass
    engine.validate_migration_completed().unwrap();

    // Downgrade the upgrade marker to simulate an incomplete migration.
    let txn = engine.db.begin_write().unwrap();
    {
        let mut migration_status = txn.open_table(MIGRATION_STATUS_TABLE).unwrap();
        migration_status
            .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
            .unwrap();
    }
    txn.commit().unwrap();

    // Validation should now fail
    assert!(engine.validate_migration_completed().is_err());
}

#[test]
fn test_migration_resume_after_interruption() {
    use crate::schema::DEFAULT_BLOCK_SIZE;

    let engine = temp_engine();

    let v2_format_info = FormatInfo {
        format_version: 2,
        name: "test-vol".into(),
        uuid: "abc-123".into(),
        endpoint: "http://localhost:9000".into(),
        bucket: "testbucket".into(),
        access_key: "studiofs".into(),
        secret_key: "studiofs123".into(),
        region: "".into(),
        data_prefix: "test-volume/objects/".into(),
        metadata: MetadataConfig::default(),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    engine.set_format_info(&v2_format_info).unwrap();

    let file1_attr = engine
        .create_file(ROOT_INO, b"resume1.txt", 0, 0, 0o644)
        .unwrap();
    let file2_attr = engine
        .create_file(ROOT_INO, b"resume2.txt", 0, 0, 0o644)
        .unwrap();

    let block1 = BlockRef::new_full(4001, DEFAULT_BLOCK_SIZE);
    let block2 = BlockRef::new_full(4002, DEFAULT_BLOCK_SIZE);
    engine.set_file_block(file1_attr.ino, 0, &block1).unwrap();
    engine.set_file_block(file2_attr.ino, 0, &block2).unwrap();

    let txn = engine.db.begin_write().unwrap();
    {
        let mut migration_status = txn.open_table(MIGRATION_STATUS_TABLE).unwrap();
        migration_status
            .insert(REGULAR_FILE_MIGRATION_NAME, MIGRATION_STATUS_STARTED)
            .unwrap();
    }
    txn.commit().unwrap();

    engine.migrate_regular_files_to_chunk_slice_model().unwrap();

    let file1_blocks = engine.get_file_blocks(file1_attr.ino).unwrap();
    let file2_blocks = engine.get_file_blocks(file2_attr.ino).unwrap();
    assert_eq!(file1_blocks.len(), 1);
    assert_eq!(file2_blocks.len(), 1);
    assert_eq!(file1_blocks[0].1.object_id, 4001);
    assert_eq!(file2_blocks[0].1.object_id, 4002);

    engine.validate_migration_completed().unwrap();

    let txn = engine.db.begin_write().unwrap();
    let file1_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file1_attr.ino,
    )
    .unwrap();
    let file2_overlay = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        file2_attr.ino,
    )
    .unwrap();
    txn.abort().unwrap();

    assert_eq!(file1_overlay.len(), 1);
    assert_eq!(file2_overlay.len(), 1);

    let file1_chunk = file1_overlay.get(&ChunkId(0)).unwrap();
    assert_eq!(file1_chunk.slices.len(), 1);
    let slice = file1_chunk.slices.iter().next().unwrap().1;
    assert_eq!(slice.block.as_ref().unwrap().object_id, 4001);

    let file2_chunk = file2_overlay.get(&ChunkId(0)).unwrap();
    assert_eq!(file2_chunk.slices.len(), 1);
    let slice = file2_chunk.slices.iter().next().unwrap().1;
    assert_eq!(slice.block.as_ref().unwrap().object_id, 4002);
}

#[test]
fn test_convert_blocks_to_chunk_state_rejects_slice_overflow() {
    let block = BlockRef::new_partial(9_999, u32::MAX);
    let err = RedbMetaEngine::convert_blocks_to_chunk_state(&[(1, block)], u32::MAX)
        .expect_err("overflowing slice range must fail");

    assert!(matches!(err, MetaError::Storage(message) if message.contains("slice range overflow")));
}
