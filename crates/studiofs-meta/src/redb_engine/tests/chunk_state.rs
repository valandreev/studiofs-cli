#[test]
fn test_regular_file_prefix_scan_is_exact_per_inode() {
    let engine = temp_engine();

    let mut chunk_1 = RegularFileChunkState::default();
    chunk_1.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid slice ino 1"),
        BlockRef::new_partial(3001, 4096),
    ));
    let mut chunk_10 = RegularFileChunkState::default();
    chunk_10.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid slice ino 10"),
        BlockRef::new_partial(3010, 4096),
    ));

    let txn = engine.db.begin_write().unwrap();
    {
        let mut overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        let key_1 = encode_regular_file_chunk_key(1, ChunkId(0));
        let key_10 = encode_regular_file_chunk_key(10, ChunkId(0));
        let bytes_1 = encode(&chunk_1).unwrap();
        let bytes_10 = encode(&chunk_10).unwrap();
        overlay
            .insert(key_1.as_slice(), bytes_1.as_slice())
            .unwrap();
        overlay
            .insert(key_10.as_slice(), bytes_10.as_slice())
            .unwrap();
    }

    let only_1 = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        1,
    )
    .unwrap();
    let only_10 = RedbMetaEngine::regular_file_chunk_states_for_inode_in_write_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        10,
    )
    .unwrap();

    assert_eq!(only_1.len(), 1);
    assert_eq!(
        only_1
            .get(&ChunkId(0))
            .expect("chunk for ino 1")
            .slices
            .len(),
        1
    );
    assert_eq!(only_10.len(), 1);
    assert_eq!(
        only_10
            .get(&ChunkId(0))
            .expect("chunk for ino 10")
            .slices
            .len(),
        1
    );
}

#[test]
fn test_clear_regular_file_graph_clears_pending_compaction_rows() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"clear-graph.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    set_regular_file_pending_compaction_rows(&engine, ino, &[ChunkId(3), ChunkId(7)]);

    let txn = engine.db.begin_write().unwrap();
    RedbMetaEngine::clear_regular_file_graph_in_txn(&txn, ino).unwrap();
    txn.commit().unwrap();

    let txn = engine.db.begin_read().unwrap();
    let pending = txn
        .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
        .unwrap();
    assert!(
        pending
            .get(encode_regular_file_chunk_key(ino, ChunkId(3)).as_slice())
            .unwrap()
            .is_none()
    );
    assert!(
        pending
            .get(encode_regular_file_chunk_key(ino, ChunkId(7)).as_slice())
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_block_replace_clears_pending_compaction_rows() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"block-replace.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    set_regular_file_pending_compaction_rows(&engine, ino, &[ChunkId(11)]);

    let txn = engine.db.begin_write().unwrap();
    RedbMetaEngine::replace_regular_file_chunk_rows_from_blocks_in_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
        &[(0, BlockRef::new_partial(1001, 4096))],
        DEFAULT_BLOCK_SIZE,
        false,
        true,
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.db.begin_read().unwrap();
    let pending = txn
        .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
        .unwrap();
    assert!(
        pending
            .get(encode_regular_file_chunk_key(ino, ChunkId(11)).as_slice())
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_block_projection_to_chunk_state_rejects_slice_overflow() {
    let block = BlockRef::new_partial(9_999, u32::MAX);
    let err = RedbMetaEngine::block_projection_to_chunk_state(&[(1, block)], u32::MAX)
        .expect_err("overflowing slice range must fail");

    assert!(matches!(err, MetaError::Storage(message) if message.contains("slice range overflow")));
}

#[test]
fn test_regular_file_overlay_detection_ignores_counter_only_changes() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"counter-only.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let mut overlay_slice = SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid overlay slice"),
        BlockRef::new_partial(501, 4096),
    );
    overlay_slice.block_state = BlockRefState::Uploading;
    let mut overlay_slices = BTreeMap::new();
    overlay_slices.insert(overlay_slice.slice_id, overlay_slice);
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

    let mut base_chunk = RegularFileChunkState {
        next_slice_id: SliceId(9),
        next_slice_order: SliceOrder(9),
        slices: overlay_chunk.slices.clone(),
        fragmentation: ChunkFragmentationCounters {
            live_slices: 0,
            tombstone_slices: 1,
            overlapping_slices: 2,
            fragmented_bytes: 8192,
        },
    };
    base_chunk
        .slices
        .get_mut(&SliceId(1))
        .expect("base slice")
        .block_state = BlockRefState::Published;

    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(11));
    let overlay_bytes = encode(&overlay_chunk).expect("encode overlay chunk");
    let base_bytes = encode(&base_chunk).expect("encode base chunk");

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

    assert!(!engine.local_overlay_inodes().unwrap().contains(&ino));
}

#[test]
fn test_regular_file_graph_signature_ignores_slice_identity_changes() {
    let mut chunk_a = RegularFileChunkState::default();
    chunk_a.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("slice a"),
        BlockRef::new_partial(501, 4096),
    ));

    let mut chunk_b = RegularFileChunkState::default();
    chunk_b.insert_slice(SliceRecord::new(
        SliceId(9),
        SliceOrder(17),
        SliceRange::new(0, 4096).expect("slice b"),
        BlockRef::new_partial(501, 4096),
    ));

    let mut chunks_a = BTreeMap::new();
    chunks_a.insert(ChunkId(0), chunk_a);
    let mut chunks_b = BTreeMap::new();
    chunks_b.insert(ChunkId(0), chunk_b);

    assert_eq!(
        RedbMetaEngine::regular_file_graph_signature(&chunks_a),
        RedbMetaEngine::regular_file_graph_signature(&chunks_b)
    );
}

#[test]
fn test_regular_file_overlay_flush_allocates_unique_slice_ids_and_orders() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"overlay-flush.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    let chunk_id = ChunkId(11);
    let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

    let mut existing_slices = BTreeMap::new();
    existing_slices.insert(
        SliceId(3),
        SliceRecord::new(
            SliceId(3),
            SliceOrder(8),
            SliceRange::new(0, 4096).expect("valid existing slice"),
            BlockRef::new_partial(601, 4096),
        ),
    );
    let existing_chunk = RegularFileChunkState {
        next_slice_id: SliceId(7),
        next_slice_order: SliceOrder(11),
        slices: existing_slices,
        fragmentation: ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 0,
            overlapping_slices: 0,
            fragmented_bytes: 4096,
        },
    };
    let existing_bytes = encode(&existing_chunk).expect("encode existing chunk");

    let txn = engine.db.begin_write().unwrap();
    {
        let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        overlay_table
            .insert(chunk_key.as_slice(), existing_bytes.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    let incoming = vec![
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(0, 4096).expect("valid replacement slice"),
            block: Some(BlockRef::new_partial(701, 4096)),
            block_state: BlockRefState::LocalOnly,
        },
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).expect("valid appended slice"),
            block: Some(BlockRef::new_partial(702, 4096)),
            block_state: BlockRefState::LocalOnly,
        },
    ];

    engine
        .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
        .unwrap();

    let txn = engine.db.begin_read().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let row = overlay_table
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("flushed overlay row");
    let chunk: RegularFileChunkState = decode(row.value()).expect("decode flushed chunk");

    assert_eq!(chunk.next_slice_id, SliceId(9));
    assert_eq!(chunk.next_slice_order, SliceOrder(13));
    assert_eq!(chunk.slices.len(), 2);

    let slices: Vec<_> = chunk.slices.values().collect();
    assert_eq!(
        slices
            .iter()
            .map(|slice| slice.slice_id)
            .collect::<Vec<_>>(),
        vec![SliceId(7), SliceId(8)]
    );
    assert_eq!(
        slices.iter().map(|slice| slice.order).collect::<Vec<_>>(),
        vec![SliceOrder(11), SliceOrder(12)]
    );
    assert_eq!(
        slices[0].range,
        SliceRange::new(0, 4096).expect("first flushed range")
    );
    assert_eq!(
        slices[1].range,
        SliceRange::new(4096, 8192).expect("second flushed range")
    );
}

#[test]
fn test_regular_file_overlay_flush_emits_final_chunk_state() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"overlay-event.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    let chunk_id = ChunkId(9);
    let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

    let mut existing_slices = BTreeMap::new();
    existing_slices.insert(
        SliceId(3),
        SliceRecord::new(
            SliceId(3),
            SliceOrder(8),
            SliceRange::new(0, 4096).expect("valid existing slice"),
            BlockRef::new_partial(801, 4096),
        ),
    );
    let existing_chunk = RegularFileChunkState {
        next_slice_id: SliceId(7),
        next_slice_order: SliceOrder(11),
        slices: existing_slices,
        fragmentation: ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 0,
            overlapping_slices: 0,
            fragmented_bytes: 4096,
        },
    };
    let existing_bytes = encode(&existing_chunk).expect("encode existing chunk");

    let txn = engine.db.begin_write().unwrap();
    {
        let mut overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
        overlay_table
            .insert(chunk_key.as_slice(), existing_bytes.as_slice())
            .unwrap();
    }
    txn.commit().unwrap();

    let incoming = vec![SliceRecord {
        slice_id: SliceId::default(),
        order: SliceOrder::default(),
        range: SliceRange::new(4096, 8192).expect("incoming range"),
        block: Some(BlockRef::new_partial(802, 4096)),
        block_state: BlockRefState::LocalOnly,
    }];

    engine
        .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
        .unwrap();

    let txn = engine.db.begin_read().unwrap();
    let counters = txn.open_table(COUNTERS_TABLE).unwrap();
    let next_event_seq = counters
        .get("nextEventSeq")
        .unwrap()
        .expect("next event seq")
        .value();
    let events = txn.open_table(EVENT_LOG_TABLE).unwrap();
    let row = events
        .get(next_event_seq - 1)
        .unwrap()
        .expect("flushed event");
    let event: MetaEvent = decode(row.value()).expect("decode flushed event");
    let payload = match event {
        MetaEvent::SetRegularFileOverlaySlices {
            ino: event_ino,
            chunk_slices,
        } => {
            assert_eq!(event_ino, ino);
            chunk_slices
        }
        other => panic!("unexpected event kind: {:?}", other),
    };

    let overlay_row = txn
        .open_table(REGULAR_FILE_OVERLAY_TABLE)
        .unwrap()
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("flushed overlay row");
    let chunk: RegularFileChunkState = decode(overlay_row.value()).expect("decode chunk");

    let expected_slices: Vec<_> = chunk.slices.values().cloned().collect();
    assert_eq!(payload, vec![(chunk_id, expected_slices)]);
}

#[test]
fn test_regular_file_overlay_slice_events_replace_rows_and_delete_empty_payload() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"overlay-replay.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    let chunk_id = ChunkId(17);
    let chunk_key = encode_regular_file_chunk_key(ino, chunk_id);

    let mut initial_slices = BTreeMap::new();
    initial_slices.insert(
        SliceId(1),
        SliceRecord::new(
            SliceId(1),
            SliceOrder(4),
            SliceRange::new(0, 8192).expect("initial slice"),
            BlockRef::new_partial(901, 8192),
        ),
    );
    let initial_chunk = RegularFileChunkState {
        next_slice_id: SliceId(2),
        next_slice_order: SliceOrder(5),
        slices: initial_slices,
        fragmentation: ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 0,
            overlapping_slices: 0,
            fragmented_bytes: 0,
        },
    };
    set_regular_file_chunk_rows(
        &engine,
        ino,
        &[(chunk_id, initial_chunk.clone())],
        &[(chunk_id, initial_chunk)],
    );

    let replacement_event = MetaEvent::SetRegularFileOverlaySlices {
        ino,
        chunk_slices: vec![(
            chunk_id,
            vec![
                SliceRecord::new(
                    SliceId(9),
                    SliceOrder(9),
                    SliceRange::new(0, 4096).expect("replacement data"),
                    BlockRef::new_partial(902, 4096),
                ),
                SliceRecord::tombstone(
                    SliceId(10),
                    SliceOrder(10),
                    SliceRange::new(4096, 8192).expect("replacement tombstone"),
                ),
            ],
        )],
    };

    engine
        .apply_replica_events_to_base(std::slice::from_ref(&replacement_event))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&replacement_event))
        .unwrap();

    {
        let txn = engine.db.begin_read().unwrap();
        let base_row = txn
            .open_table(REGULAR_FILE_BASE_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("replayed base row");
        let base_chunk: RegularFileChunkState = decode(base_row.value()).expect("decode base");
        let overlay_row = txn
            .open_table(REGULAR_FILE_OVERLAY_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .expect("replayed overlay row");
        let overlay_chunk: RegularFileChunkState =
            decode(overlay_row.value()).expect("decode overlay");

        let expected_chunk = RegularFileChunkState {
            next_slice_id: SliceId(11),
            next_slice_order: SliceOrder(11),
            slices: vec![
                SliceRecord::new(
                    SliceId(9),
                    SliceOrder(9),
                    SliceRange::new(0, 4096).expect("expected replacement data"),
                    BlockRef::new_partial(902, 4096),
                ),
                SliceRecord::tombstone(
                    SliceId(10),
                    SliceOrder(10),
                    SliceRange::new(4096, 8192).expect("expected replacement tombstone"),
                ),
            ]
            .into_iter()
            .map(|slice| (slice.slice_id, slice))
            .collect(),
            fragmentation: ChunkFragmentationCounters {
                live_slices: 1,
                tombstone_slices: 1,
                overlapping_slices: 0,
                fragmented_bytes: 0,
            },
        };

        assert_eq!(base_chunk, expected_chunk);
        assert_eq!(overlay_chunk, expected_chunk);
        assert!(
            txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
                .unwrap()
                .get(chunk_key.as_slice())
                .unwrap()
                .is_none()
        );
    }

    let delete_event = MetaEvent::SetRegularFileOverlaySlices {
        ino,
        chunk_slices: vec![(chunk_id, Vec::new())],
    };
    engine
        .apply_replica_events_to_base(std::slice::from_ref(&delete_event))
        .unwrap();
    engine
        .apply_replica_serving_state(std::slice::from_ref(&delete_event))
        .unwrap();

    let txn = engine.db.begin_read().unwrap();
    assert!(
        txn.open_table(REGULAR_FILE_BASE_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .is_none()
    );
    assert!(
        txn.open_table(REGULAR_FILE_OVERLAY_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .is_none()
    );
    assert!(
        txn.open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap()
            .get(chunk_key.as_slice())
            .unwrap()
            .is_none()
    );
}

#[test]
fn test_flush_regular_file_overlay_slices_with_attr_is_atomic() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"overlay-atomic.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 11;
    attr.size = 4096;
    engine.set_attr(&attr).unwrap();

    let incoming = vec![SliceRecord {
        slice_id: SliceId::default(),
        order: SliceOrder::default(),
        range: SliceRange::new(0, 4096).expect("valid range"),
        block: Some(BlockRef::new_partial(200_001, 4096)),
        block_state: BlockRefState::LocalOnly,
    }];

    let mut new_attr = attr.clone();
    new_attr.size = 8192;
    new_attr.content_epoch = 12;
    let committed_attr = engine
        .flush_regular_file_overlay_slices_with_attr(
            ino,
            &[(ChunkId(0), incoming)],
            Some(&new_attr),
        )
        .unwrap()
        .expect("committed attr must be returned when attr is provided");

    // The returned attr should reflect the engine-normalised committed state.
    assert_eq!(committed_attr.size, 8192);
    assert_eq!(committed_attr.content_epoch, 12);
    assert_eq!(committed_attr.upload_state, UploadState::Uploading);

    // Cross-check: the stored attr must match what was returned.
    let persisted_attr = engine.get_attr(ino).unwrap();
    assert_eq!(persisted_attr.size, committed_attr.size);
    assert_eq!(persisted_attr.content_epoch, committed_attr.content_epoch);
    assert_eq!(persisted_attr.upload_state, committed_attr.upload_state);

    let txn = engine.db.begin_read().unwrap();
    assert!(
        RedbMetaEngine::has_regular_file_chunk_rows_in_read_txn(
            &txn,
            REGULAR_FILE_OVERLAY_TABLE,
            ino
        )
        .unwrap()
    );
}

#[test]
fn test_lookup_regular_file_chunk_slices_overlay_precedence_with_tombstone() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"lookup-overlay.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    let mut base_chunk = RegularFileChunkState::default();
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 8192).expect("valid base slice 0"),
        BlockRef::new_partial(901, 8192),
    ));
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(8192, 12288).expect("valid base slice 1"),
        BlockRef::new_partial(902, 4096),
    ));

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(2048, 6144).expect("valid overlay data slice"),
        BlockRef::new_partial(801, 4096),
    ));
    overlay_chunk.insert_slice(SliceRecord::tombstone(
        SliceId(11),
        SliceOrder(2),
        SliceRange::new(7000, 9000).expect("valid tombstone slice"),
    ));

    set_regular_file_chunk_rows(
        &engine,
        ino,
        &[(ChunkId(0), overlay_chunk)],
        &[(ChunkId(0), base_chunk)],
    );

    let spans = engine
        .lookup_regular_file_chunk_slices(
            ino,
            ChunkId(0),
            SliceRange::new(0, 12288).expect("valid query"),
        )
        .unwrap();
    assert_eq!(spans.len(), 5);
    assert_eq!(
        spans[0].range,
        SliceRange {
            start: 0,
            end: 2048
        }
    );
    assert_eq!(spans[0].source, ResolvedSliceSource::Base);
    assert_eq!(spans[0].block.as_ref().unwrap().object_id, 901);

    assert_eq!(
        spans[1].range,
        SliceRange {
            start: 2048,
            end: 6144
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].block.as_ref().unwrap().object_id, 801);

    assert_eq!(
        spans[2].range,
        SliceRange {
            start: 6144,
            end: 7000
        }
    );
    assert_eq!(spans[2].source, ResolvedSliceSource::Base);
    assert_eq!(spans[2].block.as_ref().unwrap().object_id, 901);

    assert_eq!(
        spans[3].range,
        SliceRange {
            start: 7000,
            end: 9000
        }
    );
    assert_eq!(spans[3].source, ResolvedSliceSource::OverlayTombstone);
    assert!(spans[3].block.is_none());

    assert_eq!(
        spans[4].range,
        SliceRange {
            start: 9000,
            end: 12288
        }
    );
    assert_eq!(spans[4].source, ResolvedSliceSource::Base);
    assert_eq!(spans[4].block.as_ref().unwrap().object_id, 902);
}

#[test]
fn test_compact_fragmented_regular_file_chunks_preserves_tombstones() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"compact-overlay.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("shadowed slice"),
        BlockRef::new_partial(1201, 4096),
    ));
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(0, 4096).expect("visible slice"),
        BlockRef::new_partial(1202, 4096),
    ));
    overlay_chunk.insert_slice(SliceRecord::tombstone(
        SliceId(3),
        SliceOrder(3),
        SliceRange::new(4096, 6144).expect("tombstone range"),
    ));

    set_regular_file_chunk_rows(&engine, ino, &[(ChunkId(0), overlay_chunk.clone())], &[]);

    let txn = engine.db.begin_write().unwrap();
    {
        let mut pending = txn
            .open_table(PENDING_REGULAR_FILE_COMPACTION_TABLE)
            .unwrap();
        let key = encode_regular_file_chunk_key(ino, ChunkId(0));
        pending.insert(key.as_slice(), 1).unwrap();
    }
    txn.commit().unwrap();

    let query = SliceRange::new(0, 6144).expect("lookup query");
    let before = engine
        .lookup_regular_file_chunk_slices(ino, ChunkId(0), query)
        .unwrap();
    assert_eq!(before.len(), 2);
    assert_eq!(before[0].source, ResolvedSliceSource::Overlay);
    assert_eq!(before[0].block.as_ref().unwrap().object_id, 1202);
    assert_eq!(before[1].source, ResolvedSliceSource::OverlayTombstone);
    assert!(before[1].block.is_none());

    let compacted = engine.compact_fragmented_regular_file_chunks(1).unwrap();
    assert_eq!(compacted, 1);

    let after = engine
        .lookup_regular_file_chunk_slices(ino, ChunkId(0), query)
        .unwrap();
    assert_eq!(after.len(), before.len());
    for (expected, actual) in before.iter().zip(after.iter()) {
        assert_eq!(actual.range, expected.range);
        assert_eq!(actual.source, expected.source);
        assert_eq!(actual.block, expected.block);
    }

    let txn = engine.db.begin_read().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let key = encode_regular_file_chunk_key(ino, ChunkId(0));
    let row = overlay_table
        .get(key.as_slice())
        .unwrap()
        .expect("compacted overlay chunk");
    let compacted_chunk: RegularFileChunkState =
        decode(row.value()).expect("decode compacted chunk");
    assert_eq!(compacted_chunk.slices.len(), 2);
    assert_eq!(
        compacted_chunk.fragmentation,
        ChunkFragmentationCounters {
            live_slices: 1,
            tombstone_slices: 1,
            overlapping_slices: 0,
            fragmented_bytes: 0,
        }
    );
    assert!(!compacted_chunk.fragmentation.should_compact());
}

#[test]
fn test_lookup_regular_file_slices_cross_chunk_and_ordered_base_resolution() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"lookup-cross-chunk.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    let tail_start = DEFAULT_CHUNK_SIZE - 2048;

    let mut chunk0_base = RegularFileChunkState::default();
    chunk0_base.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(tail_start, DEFAULT_CHUNK_SIZE).expect("valid chunk0 base"),
        BlockRef::new_partial(1101, 2048),
    ));
    let mut chunk0_overlay = RegularFileChunkState::default();
    chunk0_overlay.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(tail_start + 512, tail_start + 1536).expect("valid chunk0 overlay"),
        BlockRef::new_partial(1201, 1024),
    ));

    let mut chunk1_base = RegularFileChunkState::default();
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(3),
        SliceOrder(1),
        SliceRange::new(0, 1024).expect("valid chunk1 base low order"),
        BlockRef::new_partial(1301, 1024),
    ));
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(4),
        SliceOrder(2),
        SliceRange::new(512, 2048).expect("valid chunk1 base high order"),
        BlockRef::new_partial(1401, 1536),
    ));

    set_regular_file_chunk_rows(
        &engine,
        ino,
        &[(ChunkId(0), chunk0_overlay)],
        &[(ChunkId(0), chunk0_base), (ChunkId(1), chunk1_base)],
    );

    let spans = engine
        .lookup_regular_file_slices(ino, DEFAULT_CHUNK_SIZE as u64 - 2048, 4096)
        .unwrap();
    assert_eq!(spans.len(), 5);

    assert_eq!(spans[0].chunk_id, ChunkId(0));
    assert_eq!(
        spans[0].range,
        SliceRange {
            start: tail_start,
            end: tail_start + 512
        }
    );
    assert_eq!(spans[0].source, ResolvedSliceSource::Base);
    assert_eq!(spans[0].block.as_ref().unwrap().object_id, 1101);

    assert_eq!(spans[1].chunk_id, ChunkId(0));
    assert_eq!(
        spans[1].range,
        SliceRange {
            start: tail_start + 512,
            end: tail_start + 1536
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].block.as_ref().unwrap().object_id, 1201);

    assert_eq!(spans[2].chunk_id, ChunkId(0));
    assert_eq!(
        spans[2].range,
        SliceRange {
            start: tail_start + 1536,
            end: DEFAULT_CHUNK_SIZE
        }
    );
    assert_eq!(spans[2].source, ResolvedSliceSource::Base);
    assert_eq!(spans[2].block.as_ref().unwrap().object_id, 1101);

    assert_eq!(spans[3].chunk_id, ChunkId(1));
    assert_eq!(spans[3].range, SliceRange { start: 0, end: 512 });
    assert_eq!(spans[3].source, ResolvedSliceSource::Base);
    assert_eq!(spans[3].block.as_ref().unwrap().object_id, 1301);

    assert_eq!(spans[4].chunk_id, ChunkId(1));
    assert_eq!(
        spans[4].range,
        SliceRange {
            start: 512,
            end: 2048
        }
    );
    assert_eq!(spans[4].source, ResolvedSliceSource::Base);
    assert_eq!(spans[4].block.as_ref().unwrap().object_id, 1401);
}

#[test]
fn test_lookup_regular_file_slices_boundary_spanning_write_is_split_per_chunk() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"lookup-boundary-split.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;

    let tail_start = DEFAULT_CHUNK_SIZE - 1024;

    let mut chunk0_base = RegularFileChunkState::default();
    chunk0_base.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(tail_start, DEFAULT_CHUNK_SIZE).expect("valid chunk0 base"),
        BlockRef::new_partial(2101, 1024),
    ));
    let mut chunk1_base = RegularFileChunkState::default();
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(0, 1024).expect("valid chunk1 base"),
        BlockRef::new_partial(2102, 1024),
    ));

    let mut chunk0_overlay = RegularFileChunkState::default();
    chunk0_overlay.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(tail_start + 512, DEFAULT_CHUNK_SIZE).expect("valid chunk0 overlay"),
        BlockRef::new_partial(2201, 512),
    ));
    let mut chunk1_overlay = RegularFileChunkState::default();
    chunk1_overlay.insert_slice(SliceRecord::new(
        SliceId(11),
        SliceOrder(1),
        SliceRange::new(0, 512).expect("valid chunk1 overlay"),
        BlockRef::new_partial(2202, 512),
    ));

    set_regular_file_chunk_rows(
        &engine,
        ino,
        &[(ChunkId(0), chunk0_overlay), (ChunkId(1), chunk1_overlay)],
        &[(ChunkId(0), chunk0_base), (ChunkId(1), chunk1_base)],
    );

    let spans = engine
        .lookup_regular_file_slices(ino, DEFAULT_CHUNK_SIZE as u64 - 1024, 2048)
        .unwrap();
    assert_eq!(spans.len(), 4);

    assert_eq!(spans[0].chunk_id, ChunkId(0));
    assert_eq!(
        spans[0].range,
        SliceRange {
            start: tail_start,
            end: tail_start + 512
        }
    );
    assert_eq!(spans[0].source, ResolvedSliceSource::Base);
    assert_eq!(spans[0].block.as_ref().unwrap().object_id, 2101);

    assert_eq!(spans[1].chunk_id, ChunkId(0));
    assert_eq!(
        spans[1].range,
        SliceRange {
            start: tail_start + 512,
            end: DEFAULT_CHUNK_SIZE
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].block.as_ref().unwrap().object_id, 2201);

    assert_eq!(spans[2].chunk_id, ChunkId(1));
    assert_eq!(spans[2].range, SliceRange { start: 0, end: 512 });
    assert_eq!(spans[2].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[2].block.as_ref().unwrap().object_id, 2202);

    assert_eq!(spans[3].chunk_id, ChunkId(1));
    assert_eq!(
        spans[3].range,
        SliceRange {
            start: 512,
            end: 1024
        }
    );
    assert_eq!(spans[3].source, ResolvedSliceSource::Base);
    assert_eq!(spans[3].block.as_ref().unwrap().object_id, 2102);
}

#[test]
fn test_publish_regular_file_overlay_to_base_if_matches_promotes_base_rows() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-slice-base.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 7;
    attr.size = chunk_start_offset(ChunkId(2), DEFAULT_CHUNK_SIZE) + 4096;
    engine.set_attr(&attr).unwrap();

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid overlay data"),
        BlockRef::new_partial(1601, 4096),
    ));
    overlay_chunk.insert_slice(SliceRecord::tombstone(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(4096, 8192).expect("valid overlay tombstone"),
    ));
    set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(2), overlay_chunk)], &[]);

    assert!(
        !engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 6)
            .unwrap()
    );
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 7)
            .unwrap()
    );

    let txn = engine.db.begin_read().unwrap();
    let base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    let chunk_key = encode_regular_file_chunk_key(file.ino, ChunkId(2));
    let row = base
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("published base chunk");
    let published: RegularFileChunkState = decode(row.value()).unwrap();
    assert_eq!(published.slices.len(), 2);
    assert_eq!(
        published.slices.get(&SliceId(1)).unwrap().block_state,
        BlockRefState::Published
    );
    assert_eq!(
        published.slices.get(&SliceId(2)).unwrap().block_state,
        BlockRefState::Published
    );
    let overlay = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    assert!(
        overlay.get(chunk_key.as_slice()).unwrap().is_none(),
        "overlay row must be cleared after publish"
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(
        published_attr.committed_size,
        chunk_start_offset(ChunkId(2), DEFAULT_CHUNK_SIZE) + 4096
    );
    assert_eq!(published_attr.committed_content_epoch, 7);
    assert_eq!(published_attr.upload_state, UploadState::Ready);
}

#[test]
fn test_publish_regular_file_overlay_to_base_keeps_sparse_tail_committed_size_below_size() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-sparse-tail.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 5;
    attr.size = 16 * 1024;
    engine.set_attr(&attr).unwrap();

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid sparse data slice"),
        BlockRef::new_partial(2601, 4096),
    ));
    set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(0), overlay_chunk)], &[]);

    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 5)
            .unwrap()
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.size, 16 * 1024);
    assert_eq!(published_attr.committed_size, 4096);
    assert!(published_attr.committed_size < published_attr.size);
}

#[test]
fn test_publish_regular_file_overlay_to_base_accepts_fragmented_block_graph() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-fragmented-slices.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 9;
    attr.size = 8192;
    engine.set_attr(&attr).unwrap();

    let chunk_id = ChunkId(0);
    let incoming = vec![
        SliceRecord::new(
            SliceId(1),
            SliceOrder(1),
            SliceRange::new(0, 4096).unwrap(),
            BlockRef::new_partial(4_201, 4096),
        ),
        SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(4096, 8192).unwrap(),
            BlockRef::new_partial(4_202, 4096),
        ),
    ];
    engine
        .flush_regular_file_overlay_slices(file.ino, &[(chunk_id, incoming)])
        .unwrap();

    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 9)
            .unwrap(),
        "slice-native publish should not require block projection synthesis"
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.upload_state, UploadState::Ready);
    assert_eq!(published_attr.committed_content_epoch, 9);
    assert_eq!(published_attr.committed_size, 8192);
}

#[test]
fn test_compute_committed_size_prefers_regular_file_base_rows() {
    let engine = temp_engine();

    let attr = engine
        .create_file(ROOT_INO, b"committed-size.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;

    let mut chunk = RegularFileChunkState::default();
    chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid committed slice"),
        BlockRef::new_partial(601, 4096),
    ));

    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(1));
    let chunk_bytes = encode(&chunk).expect("encode committed chunk");
    let txn = engine.db.begin_write().unwrap();
    {
        let mut base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
        base_table
            .insert(chunk_key.as_slice(), chunk_bytes.as_slice())
            .unwrap();
        let committed_size = RedbMetaEngine::compute_committed_size_in_txn(&txn, &base_table, ino)
            .expect("compute committed size");
        assert_eq!(
            committed_size,
            chunk_start_offset(ChunkId(1), DEFAULT_CHUNK_SIZE) + 4096
        );
    }
}

#[test]
fn redb_base_graph_publish_sets_committed_head() {
    let engine = temp_engine();
    let block_size = DEFAULT_BLOCK_SIZE as u64;

    let file = engine
        .create_file(ROOT_INO, b"publish-head.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = block_size + 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let block0 = BlockRef::new_full(101, DEFAULT_BLOCK_SIZE);
    let block1 = BlockRef::new_partial(102, 4096);
    engine.stage_block_slices(file.ino, 0, &block0).unwrap();
    engine.stage_block_slices(file.ino, 1, &block1).unwrap();

    assert!(engine.publish_regular_file_overlay_to_base_if_matches(file.ino, 1).unwrap());

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.committed_size, block_size + 4096);
    assert_eq!(published_attr.committed_content_epoch, 1);
    assert_eq!(published_attr.upload_state, UploadState::Ready);

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();
    let remote_attr = remote.get_attr(file.ino).unwrap();
    assert_eq!(remote_attr.size, block_size + 4096);
    assert_eq!(remote_attr.committed_size, block_size + 4096);
    assert_eq!(remote_attr.committed_content_epoch, 1);
    assert_eq!(remote_attr.upload_state, UploadState::Ready);

    let remote_spans = remote
        .lookup_regular_file_slices(file.ino, 0, block_size + 4096)
        .unwrap();
    let remote_blocks: Vec<_> = remote_spans
        .into_iter()
        .map(|span| span.block.expect("published block slice"))
        .collect();
    assert_eq!(remote_blocks, vec![block0, block1]);
}
