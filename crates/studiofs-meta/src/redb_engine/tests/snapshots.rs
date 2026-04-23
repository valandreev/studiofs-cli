#[test]
fn test_snapshot_pins_objects_and_is_listed() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"snap.txt", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = DEFAULT_CHUNK_SIZE as u64;
    engine.set_attr(&attr).unwrap();

    let mut overlay_chunk = RegularFileChunkState::default();
    let mut overlay_slice = SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid overlay slice"),
        BlockRef::new_partial(9_001, DEFAULT_CHUNK_SIZE),
    );
    overlay_slice.block_state = BlockRefState::Uploaded;
    overlay_chunk.insert_slice(overlay_slice);

    let mut base_chunk = RegularFileChunkState::default();
    let mut base_slice = SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid base slice"),
        BlockRef::new_partial(9_002, DEFAULT_CHUNK_SIZE),
    );
    base_slice.block_state = BlockRefState::Published;
    base_chunk.insert_slice(base_slice);

    set_regular_file_chunk_rows(
        &engine,
        file.ino,
        &[(ChunkId(0), overlay_chunk)],
        &[(ChunkId(0), base_chunk)],
    );

    let refs_before = engine.get_referenced_object_ids().unwrap();
    assert!(refs_before.contains(&9_001));
    assert!(refs_before.contains(&9_002));

    let snapshot = engine.create_snapshot(ROOT_INO).unwrap();
    assert_eq!(snapshot.root_ino, ROOT_INO);
    assert_eq!(snapshot.state, SnapshotState::Pending);
    assert_eq!(snapshot.pinned_object_ids, vec![9_001]);
    assert!(!snapshot.snapshot_payload.is_empty());

    let listed = engine.list_snapshots().unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, snapshot.id);
    assert_eq!(
        engine.get_snapshot(&snapshot.id).unwrap().unwrap().id,
        snapshot.id
    );

    engine.unlink(ROOT_INO, b"snap.txt").unwrap();
    let refs = engine.get_referenced_object_ids().unwrap();
    assert!(
        refs.contains(&9_001),
        "snapshot pins should keep object ids referenced"
    );
    assert!(
        !refs.contains(&9_002),
        "shadowed base object should not stay pinned after unlink"
    );
}

#[test]
fn test_snapshot_payload_codec_roundtrips_and_reads_json() {
    let engine = temp_engine();
    let state = engine.export_snapshot().unwrap();

    let binary = RedbMetaEngine::encode_snapshot_payload(&state).unwrap();
    let decoded_binary = RedbMetaEngine::decode_snapshot_payload(&binary).unwrap();
    assert_eq!(decoded_binary.inodes, state.inodes);
    assert_eq!(decoded_binary.dentries, state.dentries);

    let json = serde_json::to_vec(&state).unwrap();
    let decoded_json = RedbMetaEngine::decode_snapshot_payload(&json).unwrap();
    assert_eq!(decoded_json.inodes, state.inodes);
    assert_eq!(decoded_json.dentries, state.dentries);
}

#[test]
fn test_referenced_object_ids_include_committed_blocks() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"refs.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    engine
        .stage_file_overlay_block(ino, 0, &BlockRef::new_partial(21, 4096))
        .unwrap();
    engine
        .publish_uploaded_overlay_block(ino, 0, 0, &BlockRef::new_partial(21, 4096))
        .unwrap();
    engine
        .stage_file_overlay_block(ino, 1, &BlockRef::new_partial(22, 4096))
        .unwrap();

    let refs = engine.get_referenced_object_ids().unwrap();
    assert!(refs.contains(&21));
    assert!(refs.contains(&22));
    assert_eq!(refs.len(), 2);
}

#[test]
fn test_regular_file_graph_tables_roundtrip_and_project_remote_overlay() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"graph.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let mut overlay_slices = BTreeMap::new();
    overlay_slices.insert(
        SliceId(1),
        SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("valid overlay slice"),
            BlockRef::new_partial(301, 4096),
        ),
    );
    let overlay_chunk = RegularFileChunkState {
        next_slice_id: SliceId(2),
        next_slice_order: SliceOrder(2),
        slices: overlay_slices,
        fragmentation: ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 0,
            overlapping_slices: 0,
            fragmented_bytes: 4096,
        },
    };

    let mut overlay_state = RegularFileOverlayState::default();
    overlay_state
        .chunks
        .insert(ChunkId(11), overlay_chunk.clone());

    let mut base_slices = BTreeMap::new();
    let mut base_slice = SliceRecord::new(
        SliceId(3),
        SliceOrder(3),
        SliceRange::new(0, 4096).expect("valid base slice"),
        BlockRef::new_partial(401, 4096),
    );
    base_slice.block_state = BlockRefState::Published;
    base_slices.insert(SliceId(3), base_slice);
    let base_chunk = RegularFileChunkState {
        next_slice_id: SliceId(4),
        next_slice_order: SliceOrder(4),
        slices: base_slices,
        fragmentation: ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 0,
            overlapping_slices: 0,
            fragmented_bytes: 4096,
        },
    };

    let mut base_state = RegularFileBaseState::default();
    base_state.chunks.insert(ChunkId(11), base_chunk.clone());

    let overlay_bytes = encode(&overlay_chunk).expect("encode overlay chunk");
    let base_bytes = encode(&base_chunk).expect("encode base chunk");
    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(11));

    let txn = engine.db.begin_write().unwrap();
    {
        let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        overlay_table
            .insert(chunk_key.as_slice(), overlay_bytes.as_slice())
            .unwrap();

        let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        base_table
            .insert(chunk_key.as_slice(), base_bytes.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    assert!(engine.local_overlay_inodes().unwrap().contains(&ino));

    let snapshot = engine.export_snapshot().unwrap();
    assert_eq!(
        snapshot.regular_file_overlay,
        vec![(
            ino,
            encode(&overlay_state).expect("encode overlay snapshot")
        )]
    );
    assert_eq!(
        snapshot.regular_file_base,
        vec![(ino, encode(&base_state).expect("encode base snapshot"))]
    );
    let imported = temp_engine();
    imported.import_snapshot(&snapshot).unwrap();

    let txn = imported.db.begin_read().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let overlay_row = overlay_table
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("imported overlay row");
    let imported_overlay: RegularFileChunkState =
        decode(overlay_row.value()).expect("decode imported overlay");
    assert_eq!(imported_overlay, overlay_chunk);

    let base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    let base_row = base_table
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("imported base row");
    let imported_base: RegularFileChunkState =
        decode(base_row.value()).expect("decode imported base");
    assert_eq!(imported_base, base_chunk);

    let remote_snapshot = engine.export_remote_snapshot().unwrap();
    assert_eq!(
        remote_snapshot.regular_file_overlay,
        remote_snapshot.regular_file_base
    );
    assert_eq!(
        remote_snapshot.regular_file_base,
        snapshot.regular_file_base
    );
    let used_space = remote_snapshot
        .counters
        .iter()
        .find(|(name, _)| name.as_str() == counters::USED_SPACE)
        .map(|(_, value)| *value)
        .expect("used space counter");
    assert_eq!(used_space, 4096);
}

#[test]
fn test_import_snapshot_rebuilds_pending_compaction_rows() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"snapshot-pending.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = file.ino;

    let mut chunk_slices = BTreeMap::new();
    chunk_slices.insert(
        SliceId(1),
        SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).expect("first slice"),
            BlockRef::new_partial(1001, 4096),
        ),
    );
    chunk_slices.insert(
        SliceId(2),
        SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, 4096).expect("shadowed slice"),
            BlockRef::new_partial(1002, 4096),
        ),
    );
    let mut overlay_state = RegularFileOverlayState::default();
    overlay_state.chunks.insert(
        ChunkId(4),
        RegularFileChunkState {
            next_slice_id: SliceId(3),
            next_slice_order: SliceOrder(3),
            slices: chunk_slices,
            fragmentation: ChunkFragmentationCounters {
                live_slices: 2,
                tombstone_slices: 0,
                overlapping_slices: 1,
                fragmented_bytes: 4096,
            },
        },
    );

    let txn = source.db.begin_write().unwrap();
    {
        let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let key = encode_regular_file_chunk_key(ino, ChunkId(4));
        let bytes = encode(&overlay_state.chunks[&ChunkId(4)]).unwrap();
        overlay.insert(key.as_slice(), bytes.as_slice()).unwrap();
    }
    txn.commit().unwrap();

    let snapshot = source.export_snapshot().unwrap();

    let target = temp_engine();
    set_regular_file_pending_compaction_rows(&target, ino, &[ChunkId(99)]);

    target.import_snapshot(&snapshot).unwrap();

    let txn = target.db.begin_read().unwrap();
    let pending = txn
        .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
        .unwrap();
    assert!(
        pending
            .get(encode_regular_file_chunk_key(ino, ChunkId(99)).as_slice())
            .unwrap()
            .is_none()
    );
    assert!(
        pending
            .get(encode_regular_file_chunk_key(ino, ChunkId(4)).as_slice())
            .unwrap()
            .is_some()
    );
}

#[test]
fn test_import_snapshot_keeps_unpublished_regular_file_visible() {
    let source = temp_engine();
    let file = source
        .create_file(ROOT_INO, b"snapshot-local-only.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = source.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    source.set_attr(&attr).unwrap();
    let block = BlockRef::new_partial(40_001, 4096);
    source.stage_file_overlay_block(file.ino, 0, &block).unwrap();

    let snapshot = source.export_snapshot().unwrap();
    let imported = temp_engine();
    imported.import_snapshot(&snapshot).unwrap();

    let imported_file = imported.lookup(ROOT_INO, b"snapshot-local-only.bin").unwrap();
    assert_eq!(imported_file.ino, file.ino);
    let imported_attr = imported.get_attr(imported_file.ino).unwrap();
    assert_eq!(imported_attr.size, 4096);
    assert_eq!(imported_attr.committed_size, 0);
    assert_eq!(imported_attr.upload_state, UploadState::Uploading);
    let imported_block = imported.lookup_file_graph_block(imported_file.ino, 0).unwrap().unwrap();
    assert_eq!(imported_block.object_id, block.object_id);
}

#[test]
fn test_redb_snapshot_defaults_missing_regular_file_fields() {
    let snapshot_json = r#"{
        "settings": [],
        "counters": [],
        "inodes": [],
        "dentries": [],
        "pending_deletions": [],
        "symlinks": [],
        "xattrs": [],
        "acl_rules": [],
        "event_log": [],
        "snapshots": []
    }"#;
    let snapshot = RedbMetaEngine::decode_snapshot_payload(snapshot_json.as_bytes()).unwrap();
    assert!(snapshot.regular_file_overlay.is_empty());
    assert!(snapshot.regular_file_base.is_empty());
}

#[test]
fn test_regular_file_graph_rows_removed_on_unlink() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"unlink-graph.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let mut chunk = RegularFileChunkState::default();
    chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid slice"),
        BlockRef::new_partial(777, 4096),
    ));
    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(0));
    let chunk_bytes = encode(&chunk).expect("encode chunk state");

    let txn = engine.db.begin_write().unwrap();
    {
        let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        overlay_table
            .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
            .unwrap();

        let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        base_table
            .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    engine.unlink(ROOT_INO, b"unlink-graph.bin").unwrap();

    let txn = engine.db.begin_read().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    assert!(overlay_table.get(chunk_key.as_slice()).unwrap().is_none());
    let base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    assert!(base_table.get(chunk_key.as_slice()).unwrap().is_none());
}
