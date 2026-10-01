fn partial_payload_ref(object_id: u64, offset: u32, length: u32) -> PayloadRef {
    let mut payload = PayloadRef::new_partial(object_id, length);
    payload.offset = offset;
    payload
}

fn local_payload_slice(object_id: u64) -> SliceRecord {
    SliceRecord::new(
        SliceId::default(),
        SliceOrder::default(),
        SliceRange::new(0, 4096).expect("valid range"),
        PayloadRef::new_partial(object_id, 4096),
    )
}

fn resolved_byte_view(
    spans: &[ResolvedSliceSpan],
    offset: u64,
    length: u64,
    objects: &std::collections::BTreeMap<u64, Vec<u8>>,
) -> Vec<u8> {
    let requested_end = offset
        .checked_add(length)
        .expect("test range end fits in file coordinates");
    let mut view = vec![0; usize::try_from(length).expect("test range fits in memory")];
    for span in spans {
        let file_start = chunk_start_offset(span.chunk_id, DEFAULT_CHUNK_SIZE)
            .checked_add(u64::from(span.range.start))
            .expect("span file offset fits");
        let file_end = file_start
            .checked_add(u64::from(span.range.len()))
            .expect("span end offset fits");
        assert!(
            file_start >= offset,
            "resolved span starts before requested view: span={span:?}, offset={offset}"
        );
        assert!(
            file_end <= requested_end,
            "resolved span ends after requested view: span={span:?}, requested_end={requested_end}"
        );
        let Some(payload) = span.payload.as_ref() else {
            continue;
        };
        assert_eq!(
            payload.length,
            span.range.len(),
            "resolved span range length does not match payload length: span={span:?}"
        );
        let dst_start =
            usize::try_from(file_start - offset).expect("span starts inside requested view");
        let payload_len = usize::try_from(payload.length).expect("payload length fits");
        let dst_end = dst_start
            .checked_add(payload_len)
            .expect("destination span end fits");
        assert!(
            dst_end <= view.len(),
            "resolved span exceeds destination view: span={span:?}, view_len={}",
            view.len()
        );
        let src_start = usize::try_from(payload.offset).expect("payload offset fits");
        let src_end = src_start
            .checked_add(payload_len)
            .expect("source span end fits");
        let object = objects
            .get(&payload.object_id)
            .expect("payload object is available in test map");
        assert!(
            src_end <= object.len(),
            "resolved span exceeds payload object: span={span:?}, object_len={}",
            object.len()
        );
        view[dst_start..dst_end].copy_from_slice(&object[src_start..src_end]);
    }
    view
}

fn mark_overlay_objects_uploaded(
    engine: &RedbMetaEngine,
    ino: Ino,
    expected_epoch: u64,
    object_ids: &[u64],
) {
    for object_id in object_ids {
        assert!(
            engine
                .mark_regular_file_overlay_object_uploaded_if_matches(
                    ino,
                    expected_epoch,
                    *object_id,
                )
                .unwrap(),
            "object {object_id} should still belong to inode {ino} epoch {expected_epoch}"
        );
    }
}

fn remote_payload_object_ids(engine: &RedbMetaEngine, ino: Ino, length: u64) -> Vec<u64> {
    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();
    remote
        .lookup_regular_file_slices(ino, 0, length)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect()
}

#[test]
fn test_regular_file_prefix_scan_is_exact_per_inode() {
    let engine = temp_engine();

    let mut chunk_1 = RegularFileChunkState::default();
    chunk_1.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid slice ino 1"),
        PayloadRef::new_partial(3001, 4096),
    ));
    let mut chunk_10 = RegularFileChunkState::default();
    chunk_10.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid slice ino 10"),
        PayloadRef::new_partial(3010, 4096),
    ));

    let txn = engine.begin_write_txn().unwrap();
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

    let txn = engine.begin_write_txn().unwrap();
    RedbMetaEngine::clear_regular_file_graph_in_txn(&txn, ino).unwrap();
    txn.commit().unwrap();

    let txn = engine.begin_read_txn().unwrap();
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

    let txn = engine.begin_write_txn().unwrap();
    RedbMetaEngine::replace_regular_file_chunk_rows_from_blocks_in_txn(
        &txn,
        REGULAR_FILE_OVERLAY_TABLE,
        ino,
        &[(0, PayloadRef::new_partial(1001, 4096))],
        DEFAULT_BLOCK_SIZE,
        false,
        true,
    )
    .unwrap();
    txn.commit().unwrap();

    let txn = engine.begin_read_txn().unwrap();
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
fn test_logical_payload_refs_to_chunk_state_rejects_slice_overflow() {
    let block = PayloadRef::new_partial(9_999, u32::MAX);
    let err = RedbMetaEngine::logical_payload_refs_to_chunk_state(&[(1, block)], u32::MAX)
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
        PayloadRef::new_partial(501, 4096),
    );
    overlay_slice.payload_state = PayloadRefState::Uploading;
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
        .payload_state = PayloadRefState::Published;

    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(11));
    let overlay_bytes = encode(&overlay_chunk).expect("encode overlay chunk");
    let base_bytes = encode(&base_chunk).expect("encode base chunk");

    let txn = engine.begin_write_txn().unwrap();
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
        PayloadRef::new_partial(501, 4096),
    ));

    let mut chunk_b = RegularFileChunkState::default();
    chunk_b.insert_slice(SliceRecord::new(
        SliceId(9),
        SliceOrder(17),
        SliceRange::new(0, 4096).expect("slice b"),
        PayloadRef::new_partial(501, 4096),
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
            PayloadRef::new_partial(601, 4096),
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

    let txn = engine.begin_write_txn().unwrap();
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
            payload: Some(PayloadRef::new_partial(701, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).expect("valid appended slice"),
            payload: Some(PayloadRef::new_partial(702, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
    ];

    engine
        .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
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
            PayloadRef::new_partial(801, 4096),
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

    let txn = engine.begin_write_txn().unwrap();
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
        payload: Some(PayloadRef::new_partial(802, 4096)),
        payload_state: PayloadRefState::LocalOnly,
    }];

    engine
        .flush_regular_file_overlay_slices(ino, &[(chunk_id, incoming)])
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
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
    let event: MetaEvent = crate::events::decode_event(row.value()).expect("decode flushed event");
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
            PayloadRef::new_partial(901, 8192),
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
                    PayloadRef::new_partial(902, 4096),
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
        let txn = engine.begin_read_txn().unwrap();
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
                    PayloadRef::new_partial(902, 4096),
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

    let txn = engine.begin_read_txn().unwrap();
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
        payload: Some(PayloadRef::new_partial(200_001, 4096)),
        payload_state: PayloadRefState::LocalOnly,
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

    let txn = engine.begin_read_txn().unwrap();
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
fn regular_file_overlay_batch_commits_two_inodes_in_one_transaction() {
    let engine = temp_engine();
    let first = engine
        .create_file(ROOT_INO, b"overlay-batch-first.bin", 1000, 1000, 0o644)
        .unwrap();
    let second = engine
        .create_file(ROOT_INO, b"overlay-batch-second.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut first_attr = first.clone();
    first_attr.size = 4096;
    first_attr.content_epoch = 1;
    let before = engine.metadata_write_observability();

    let results = engine
        .apply_regular_file_overlay_batch(&[
            crate::RegularFileOverlayMutation {
                ino: first.ino,
                chunk_slices: vec![(ChunkId(0), vec![local_payload_slice(200_011)])],
                attr: Some(first_attr),
            },
            crate::RegularFileOverlayMutation {
                ino: second.ino,
                chunk_slices: vec![(ChunkId(0), vec![local_payload_slice(200_012)])],
                attr: None,
            },
        ])
        .unwrap();

    assert_eq!(results[0].as_ref().map(|attr| attr.ino), Some(first.ino));
    assert_eq!(results[1], None);
    assert!(engine.has_local_regular_file_overlay(first.ino).unwrap());
    assert!(engine.has_local_regular_file_overlay(second.ino).unwrap());
    let after = engine.metadata_write_observability();
    assert_eq!(after.begin_write.calls - before.begin_write.calls, 1);
    assert_eq!(after.commit.calls - before.commit.calls, 1);
    assert_eq!(after.batch.batches - before.batch.batches, 1);
    assert_eq!(after.batch.requested_ops - before.batch.requested_ops, 2);
    assert_eq!(after.batch.actual_ops - before.batch.actual_ops, 2);
    assert_eq!(
        after.batch.estimated_slices - before.batch.estimated_slices,
        2
    );
    assert_eq!(
        after.batch.estimated_payload_bytes - before.batch.estimated_payload_bytes,
        8192
    );
}

#[test]
fn regular_file_overlay_batch_rolls_back_on_middle_failure() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"overlay-batch-rollback.bin", 1000, 1000, 0o644)
        .unwrap();
    let overflowing = engine
        .create_file(ROOT_INO, b"overlay-batch-overflow.bin", 1000, 1000, 0o644)
        .unwrap();
    let overflow_key = encode_regular_file_chunk_key(overflowing.ino, ChunkId(0));
    let overflow_state = RegularFileChunkState {
        next_slice_id: SliceId(u64::MAX),
        next_slice_order: SliceOrder(u64::MAX),
        ..Default::default()
    };
    let overflow_bytes = encode(&overflow_state).expect("encode overflow state");
    let txn = engine.begin_write_txn().unwrap();
    txn.open_table(REGULAR_FILE_OVERLAY_TABLE)
        .unwrap()
        .insert(overflow_key.as_slice(), overflow_bytes.as_slice())
        .unwrap();
    txn.commit().unwrap();

    let error = engine
        .apply_regular_file_overlay_batch(&[
            crate::RegularFileOverlayMutation {
                ino: file.ino,
                chunk_slices: vec![(ChunkId(0), vec![local_payload_slice(200_021)])],
                attr: None,
            },
            crate::RegularFileOverlayMutation {
                ino: overflowing.ino,
                chunk_slices: vec![(ChunkId(0), vec![local_payload_slice(200_022)])],
                attr: None,
            },
        ])
        .expect_err("overflowing second mutation must fail the batch");

    assert!(matches!(
        error,
        MetaError::RegularFileOverlayBatchOperation { index: 1, .. }
    ));
    assert!(!engine.has_local_regular_file_overlay(file.ino).unwrap());
    assert_eq!(
        engine.metadata_write_observability().batch.rollback_index,
        Some(1)
    );
    assert_eq!(engine.metadata_write_observability().batch.failures, 1);
}

#[test]
fn mark_overlay_objects_uploaded_batches_matching_objects() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"overlay-upload-batch.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 7;
    attr.size = 8192;

    let slices = vec![
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(0, 4096).expect("valid range"),
            payload: Some(PayloadRef::new_partial(700_001, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).expect("valid range"),
            payload: Some(PayloadRef::new_partial(700_002, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
    ];
    engine
        .flush_regular_file_overlay_slices_with_attr(ino, &[(ChunkId(0), slices)], Some(&attr))
        .unwrap();

    let marked = engine
        .mark_regular_file_overlay_objects_uploaded_if_match(ino, 7, &[700_001, 700_999, 700_002])
        .unwrap();

    assert_eq!(marked, vec![700_001, 700_002]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(ino, 7)
            .unwrap(),
        "batch-marked objects should satisfy publish preconditions"
    );
}

#[test]
fn get_inode_lifecycle_recomputes_stale_local_only_row_from_uploaded_overlay() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"stale-upload-lifecycle.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 7;
    attr.size = 8192;

    let slices = vec![
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(0, 4096).expect("valid range"),
            payload: Some(PayloadRef::new_partial(700_101, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
        SliceRecord {
            slice_id: SliceId::default(),
            order: SliceOrder::default(),
            range: SliceRange::new(4096, 8192).expect("valid range"),
            payload: Some(PayloadRef::new_partial(700_102, 4096)),
            payload_state: PayloadRefState::LocalOnly,
        },
    ];
    engine
        .flush_regular_file_overlay_slices_with_attr(ino, &[(ChunkId(0), slices)], Some(&attr))
        .unwrap();
    engine
        .mark_regular_file_overlay_objects_uploaded_if_match(ino, 7, &[700_101, 700_102])
        .unwrap();

    let txn = engine.begin_write_txn().unwrap();
    let mut stale = engine.get_inode_lifecycle(ino).unwrap().unwrap();
    stale.state = InodeLifecycleState::LocalOnly;
    RedbMetaEngine::put_inode_lifecycle_in_txn(&txn, &stale).unwrap();
    txn.commit().unwrap();

    assert_eq!(
        engine.get_inode_lifecycle(ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady
    );
}

#[test]
fn mark_uploaded_overlay_rebuilds_missing_lifecycle_for_already_uploaded_object() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"overlay-upload-lifecycle.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 7;
    attr.size = 4096;
    attr.committed_size = 0;
    attr.committed_content_epoch = 0;
    attr.upload_state = UploadState::Uploading;

    engine
        .flush_regular_file_overlay_slices_with_attr(
            ino,
            &[(ChunkId(0), vec![local_payload_slice(700_003)])],
            Some(&attr),
        )
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(ino, 7, 700_003)
            .unwrap()
    );

    let txn = engine.begin_write_txn().unwrap();
    RedbMetaEngine::remove_inode_lifecycle_in_txn(&txn, ino).unwrap();
    txn.commit().unwrap();

    assert!(engine.get_inode_lifecycle(ino).unwrap().is_none());
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(ino, 7, 700_003)
            .unwrap()
    );
    assert_eq!(
        engine.get_inode_lifecycle(ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady
    );
}

#[test]
fn regular_file_overlay_reflush_preserves_uploaded_payload_state() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"overlay-uploaded-reflush.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 1;
    attr.size = 4096;

    engine
        .flush_regular_file_overlay_slices_with_attr(
            ino,
            &[(ChunkId(0), vec![local_payload_slice(700_004)])],
            Some(&attr),
        )
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(ino, 1, 700_004)
            .unwrap()
    );
    assert_eq!(
        engine.get_inode_lifecycle(ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady
    );

    engine
        .flush_regular_file_overlay_slices(ino, &[(ChunkId(0), vec![local_payload_slice(700_004)])])
        .unwrap();

    let txn = engine.begin_read_txn().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    let key = encode_regular_file_chunk_key(ino, ChunkId(0));
    let row = overlay_table
        .get(key.as_slice())
        .unwrap()
        .expect("overlay row after reflush");
    let chunk: RegularFileChunkState = decode(row.value()).expect("decode overlay row");
    assert!(
        chunk.slices.values().all(|slice| {
            slice.payload.as_ref().map(|payload| payload.object_id) == Some(700_004)
                && slice.payload_state == PayloadRefState::Uploaded
        }),
        "reflushing the same immutable payload must not downgrade it: {:?}",
        chunk.slices
    );
    assert_eq!(
        engine.get_inode_lifecycle(ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady
    );
}

#[test]
fn concurrent_overlay_upload_acknowledgements_preserve_each_other() {
    let engine = std::sync::Arc::new(temp_engine());
    let mut attr = engine
        .create_file(ROOT_INO, b"concurrent-overlay-acks.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.content_epoch = 1;
    attr.size = 16 * 4096;

    let slices = (0_u32..16)
        .map(|index| {
            SliceRecord::new(
                SliceId::default(),
                SliceOrder::default(),
                SliceRange::new(index * 4096, (index + 1) * 4096).unwrap(),
                PayloadRef::new_partial(710_000 + u64::from(index), 4096),
            )
        })
        .collect();
    engine
        .flush_regular_file_overlay_slices_with_attr(ino, &[(ChunkId(0), slices)], Some(&attr))
        .unwrap();

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(17));
    let handles: Vec<_> = (0_u32..16)
        .map(|index| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                engine
                    .mark_regular_file_overlay_object_uploaded_if_matches(
                        ino,
                        1,
                        710_000 + u64::from(index),
                    )
                    .unwrap();
            })
        })
        .collect();
    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(
        engine.get_inode_lifecycle(ino).unwrap().unwrap().state,
        InodeLifecycleState::PublishReady,
        "parallel upload acknowledgements must not downgrade earlier Uploaded slices"
    );
}

#[test]
fn regular_file_publication_batch_commits_two_inodes_in_one_transaction() {
    let engine = temp_engine();
    let first = engine
        .create_file(ROOT_INO, b"publication-batch-first.bin", 1000, 1000, 0o644)
        .unwrap();
    let second = engine
        .create_file(ROOT_INO, b"publication-batch-second.bin", 1000, 1000, 0o644)
        .unwrap();

    for (ino, object_id) in [(first.ino, 800_001), (second.ino, 800_002)] {
        let mut attr = engine.get_attr(ino).unwrap();
        attr.size = 4096;
        attr.content_epoch = 1;
        engine.set_attr(&attr).unwrap();
        engine
            .stage_payload_slice(attr.ino, 0, &PayloadRef::new_partial(object_id, 4096))
            .unwrap();
        mark_overlay_objects_uploaded(&engine, attr.ino, 1, &[object_id]);
    }

    let before = engine.metadata_write_observability();
    let published = engine
        .publish_regular_file_overlay_to_base_if_matches_batch(&[(first.ino, 1), (second.ino, 1)])
        .unwrap();

    assert_eq!(published, vec![true, true]);
    let after = engine.metadata_write_observability();
    assert_eq!(after.begin_write.calls - before.begin_write.calls, 1);
    assert_eq!(after.commit.calls - before.commit.calls, 1);
}

#[test]
fn test_flush_regular_file_overlay_slices_with_attr_skips_stale_epoch() {
    let engine = temp_engine();
    let mut stale_attr = engine
        .create_file(ROOT_INO, b"overlay-stale-epoch.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = stale_attr.ino;
    stale_attr.size = 4096;
    stale_attr.content_epoch = 1;

    let mut newer_attr = stale_attr.clone();
    newer_attr.size = 0;
    newer_attr.content_epoch = 2;
    engine.set_attr(&newer_attr).unwrap();

    let incoming = vec![SliceRecord {
        slice_id: SliceId::default(),
        order: SliceOrder::default(),
        range: SliceRange::new(0, 4096).expect("valid range"),
        payload: Some(PayloadRef::new_partial(200_002, 4096)),
        payload_state: PayloadRefState::LocalOnly,
    }];
    let before = engine.metadata_write_observability();

    let committed_attr = engine
        .flush_regular_file_overlay_slices_with_attr(
            ino,
            &[(ChunkId(0), incoming)],
            Some(&stale_attr),
        )
        .unwrap();
    assert!(
        committed_attr.is_none(),
        "stale overlay commits must be skipped rather than returning a committed attr"
    );
    let after = engine.metadata_write_observability();
    assert_eq!(after.commit.calls, before.commit.calls);

    let persisted_attr = engine.get_attr(ino).unwrap();
    assert_eq!(persisted_attr.size, 0);
    assert_eq!(persisted_attr.content_epoch, 2);
    let spans = engine.lookup_regular_file_slices(ino, 0, 4096).unwrap();
    assert!(
        spans.iter().all(|span| span.payload.is_none()),
        "stale overlay commits must not install payload slices: {spans:?}"
    );
}

#[test]
fn regular_file_overlay_batch_does_not_regress_ready_attr_same_epoch() {
    let engine = temp_engine();
    let mut published = engine
        .create_file(ROOT_INO, b"overlay-ready-regression.bin", 1000, 1000, 0o644)
        .unwrap();
    published.size = 4096;
    published.committed_size = 4096;
    published.content_epoch = 1;
    published.committed_content_epoch = 1;
    published.upload_state = UploadState::Ready;
    engine.set_attr(&published).unwrap();

    let mut stale = published.clone();
    stale.committed_size = 0;
    stale.committed_content_epoch = 0;
    stale.upload_state = UploadState::Uploading;
    let result = engine
        .apply_regular_file_overlay_batch(&[crate::RegularFileOverlayMutation {
            ino: stale.ino,
            chunk_slices: vec![(ChunkId(0), vec![local_payload_slice(200_003)])],
            attr: Some(stale),
        }])
        .unwrap();

    assert_eq!(result, vec![None]);
    assert_eq!(engine.get_attr(published.ino).unwrap(), published);
    assert!(
        !engine
            .has_local_regular_file_overlay(published.ino)
            .unwrap()
    );
}

#[test]
fn test_local_slice_batch_commits_with_attr_and_resolves_holes() {
    let engine = temp_engine();
    let mut attr = engine
        .create_file(ROOT_INO, b"local-slice-batch.bin", 1000, 1000, 0o644)
        .unwrap();
    let ino = attr.ino;
    attr.size = DEFAULT_CHUNK_SIZE as u64 + 64;
    attr.content_epoch = 2;

    let mut batch = LocalSliceBatch::new(ino);
    batch
        .append_data_range(DEFAULT_CHUNK_SIZE as u64 - 8, 24, 300_001, 4)
        .expect("data range splits by chunk");
    batch
        .append_hole_range(DEFAULT_CHUNK_SIZE as u64 + 24, 16)
        .expect("hole range stays chunk-local");

    let committed_attr = engine
        .flush_regular_file_overlay_slices_with_attr(ino, &batch.into_chunk_slices(), Some(&attr))
        .unwrap()
        .expect("attr committed with slice batch");
    assert_eq!(committed_attr.size, DEFAULT_CHUNK_SIZE as u64 + 64);
    assert_eq!(committed_attr.content_epoch, 2);
    assert_eq!(committed_attr.upload_state, UploadState::Uploading);

    let spans = engine
        .lookup_regular_file_slices(ino, DEFAULT_CHUNK_SIZE as u64 - 8, 72)
        .unwrap();
    assert_eq!(spans.len(), 5);
    assert_eq!(spans[0].chunk_id, ChunkId(0));
    assert_eq!(
        spans[0].range,
        SliceRange::new(DEFAULT_CHUNK_SIZE - 8, DEFAULT_CHUNK_SIZE).unwrap()
    );
    assert_eq!(spans[0].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[0].payload.as_ref().unwrap().object_id, 300_001);
    assert_eq!(spans[0].payload.as_ref().unwrap().offset, 4);

    assert_eq!(spans[1].chunk_id, ChunkId(1));
    assert_eq!(spans[1].range, SliceRange::new(0, 16).unwrap());
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].payload.as_ref().unwrap().offset, 12);

    assert_eq!(spans[2].range, SliceRange::new(16, 24).unwrap());
    assert_eq!(spans[2].source, ResolvedSliceSource::Hole);
    assert_eq!(spans[3].range, SliceRange::new(24, 40).unwrap());
    assert_eq!(spans[3].source, ResolvedSliceSource::OverlayTombstone);
    assert_eq!(spans[4].range, SliceRange::new(40, 64).unwrap());
    assert_eq!(spans[4].source, ResolvedSliceSource::Hole);
}

#[test]
fn test_contract_core_slice_graph_resolves_logical_byte_view_after_reopen() {
    let mut fixture =
        ContractCoreFixture::new("chunk-slice-byte-view").expect("create contract fixture");
    let file = fixture
        .apply_op(ContractCoreOp::CreateFile {
            parent: ROOT_INO,
            name: b"byte-view.bin".to_vec(),
            mode: 0o644,
        })
        .expect("create file")
        .created_attr()
        .expect("created file attr");
    let ino = file.ino;

    let query_offset = DEFAULT_CHUNK_SIZE as u64 - 8;
    let query_len = 48;
    let tail_start = DEFAULT_CHUNK_SIZE - 8;

    let objects = std::collections::BTreeMap::from([
        (101, b"0123456789abcdef".to_vec()),
        (102, b"abcdefghijklmnopqrstuvwxyz012345".to_vec()),
        (103, b"BASE-HIGH-ORDER".to_vec()),
        (201, b"OVERLAY-OBJECT-0123456789".to_vec()),
        (202, b"fragmented-payload".to_vec()),
    ]);

    let mut chunk0_base = RegularFileChunkState::default();
    chunk0_base.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(tail_start, DEFAULT_CHUNK_SIZE).expect("valid chunk0 base"),
        partial_payload_ref(101, 4, 8),
    ));
    let mut chunk0_overlay = RegularFileChunkState::default();
    chunk0_overlay.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(tail_start + 4, DEFAULT_CHUNK_SIZE).expect("valid chunk0 overlay"),
        partial_payload_ref(201, 2, 4),
    ));

    let mut chunk1_base = RegularFileChunkState::default();
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 24).expect("valid chunk1 low-order base"),
        partial_payload_ref(102, 0, 24),
    ));
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(8, 16).expect("valid chunk1 high-order base"),
        partial_payload_ref(103, 4, 8),
    ));

    let mut chunk1_overlay = RegularFileChunkState::default();
    chunk1_overlay.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(4, 12).expect("valid chunk1 overlay data"),
        partial_payload_ref(201, 6, 8),
    ));
    chunk1_overlay.insert_slice(SliceRecord::tombstone(
        SliceId(11),
        SliceOrder(2),
        SliceRange::new(18, 22).expect("valid chunk1 overlay tombstone"),
    ));
    chunk1_overlay.insert_slice(SliceRecord::new(
        SliceId(12),
        SliceOrder(3),
        SliceRange::new(24, 28).expect("valid first fragment"),
        partial_payload_ref(202, 2, 4),
    ));
    chunk1_overlay.insert_slice(SliceRecord::new(
        SliceId(13),
        SliceOrder(4),
        SliceRange::new(28, 32).expect("valid second fragment"),
        partial_payload_ref(202, 6, 4),
    ));

    {
        let engine = fixture.meta();
        let mut attr = engine.get_attr(ino).expect("file attr");
        attr.size = query_offset + query_len;
        attr.content_epoch = 1;
        engine.set_attr(&attr).expect("set file size");
        set_regular_file_chunk_rows(
            &engine,
            ino,
            &[(ChunkId(0), chunk0_overlay), (ChunkId(1), chunk1_overlay)],
            &[(ChunkId(0), chunk0_base), (ChunkId(1), chunk1_base)],
        );
    }

    let mut expected = Vec::new();
    // Chunk 0 tail: base bytes followed by overlay bytes crossing the boundary.
    expected.extend_from_slice(&objects[&101][4..8]);
    expected.extend_from_slice(&objects[&201][2..6]);
    // Chunk 1: overlay shadows base, then later base order wins inside base.
    expected.extend_from_slice(&objects[&102][0..4]);
    expected.extend_from_slice(&objects[&201][6..14]);
    expected.extend_from_slice(&objects[&103][8..12]);
    // Tombstone masks lower-precedence base and resolves as zero-filled bytes.
    expected.extend_from_slice(&objects[&102][16..18]);
    expected.extend_from_slice(&[0; 4]);
    expected.extend_from_slice(&objects[&102][22..24]);
    // Fragmented slices read disjoint ranges from one payload object.
    expected.extend_from_slice(&objects[&202][2..6]);
    expected.extend_from_slice(&objects[&202][6..10]);
    // Sparse tail remains zero-filled.
    expected.extend_from_slice(&[0; 8]);
    assert_eq!(expected.len(), usize::try_from(query_len).unwrap());

    let before_reopen = {
        let engine = fixture.meta();
        engine
            .lookup_regular_file_slices(ino, query_offset, query_len)
            .expect("resolve before reopen")
    };
    assert!(
        before_reopen
            .iter()
            .any(|span| span.source == ResolvedSliceSource::OverlayTombstone),
        "resolved spans must keep overlay tombstones explicit: {before_reopen:?}"
    );
    assert!(
        before_reopen
            .iter()
            .any(|span| span.source == ResolvedSliceSource::Hole),
        "resolved spans must keep sparse holes explicit: {before_reopen:?}"
    );
    assert_eq!(
        resolved_byte_view(&before_reopen, query_offset, query_len, &objects),
        expected
    );

    let reopened = fixture.reopen_meta().expect("reopen metadata fixture");
    let after_reopen = reopened
        .lookup_regular_file_slices(ino, query_offset, query_len)
        .expect("resolve after reopen");

    assert_eq!(after_reopen, before_reopen);
    assert_eq!(
        resolved_byte_view(&after_reopen, query_offset, query_len, &objects),
        expected
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
        PayloadRef::new_partial(901, 8192),
    ));
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(8192, 12288).expect("valid base slice 1"),
        PayloadRef::new_partial(902, 4096),
    ));

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(2048, 6144).expect("valid overlay data slice"),
        PayloadRef::new_partial(801, 4096),
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
    assert_eq!(spans[0].payload.as_ref().unwrap().object_id, 901);

    assert_eq!(
        spans[1].range,
        SliceRange {
            start: 2048,
            end: 6144
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].payload.as_ref().unwrap().object_id, 801);

    assert_eq!(
        spans[2].range,
        SliceRange {
            start: 6144,
            end: 7000
        }
    );
    assert_eq!(spans[2].source, ResolvedSliceSource::Base);
    assert_eq!(spans[2].payload.as_ref().unwrap().object_id, 901);

    assert_eq!(
        spans[3].range,
        SliceRange {
            start: 7000,
            end: 9000
        }
    );
    assert_eq!(spans[3].source, ResolvedSliceSource::OverlayTombstone);
    assert!(spans[3].payload.is_none());

    assert_eq!(
        spans[4].range,
        SliceRange {
            start: 9000,
            end: 12288
        }
    );
    assert_eq!(spans[4].source, ResolvedSliceSource::Base);
    assert_eq!(spans[4].payload.as_ref().unwrap().object_id, 902);
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
        PayloadRef::new_partial(1201, 4096),
    ));
    let mut visible_slice = SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(0, 4096).expect("visible slice"),
        PayloadRef::new_partial(1202, 4096),
    );
    visible_slice.payload_state = PayloadRefState::Uploaded;
    overlay_chunk.insert_slice(visible_slice);
    overlay_chunk.insert_slice(SliceRecord::tombstone(
        SliceId(3),
        SliceOrder(3),
        SliceRange::new(4096, 6144).expect("tombstone range"),
    ));

    set_regular_file_chunk_rows(&engine, ino, &[(ChunkId(0), overlay_chunk.clone())], &[]);
    let mut attr = engine.get_attr(ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let txn = engine.begin_write_txn().unwrap();
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
    assert_eq!(before[0].payload.as_ref().unwrap().object_id, 1202);
    assert_eq!(before[1].source, ResolvedSliceSource::OverlayTombstone);
    assert!(before[1].payload.is_none());

    let compacted = engine.compact_fragmented_regular_file_chunks(1).unwrap();
    assert_eq!(compacted, 1);

    let after = engine
        .lookup_regular_file_chunk_slices(ino, ChunkId(0), query)
        .unwrap();
    assert_eq!(after.len(), before.len());
    for (expected, actual) in before.iter().zip(after.iter()) {
        assert_eq!(actual.range, expected.range);
        assert_eq!(actual.source, expected.source);
        assert_eq!(actual.payload, expected.payload);
    }

    let txn = engine.begin_read_txn().unwrap();
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
    let recovery = engine
        .pending_uploaded_regular_file_recoveries()
        .unwrap()
        .into_iter()
        .find(|record| record.attr.ino == ino)
        .expect("uploaded overlay recovery row");
    assert!(recovery
        .overlay
        .chunks
        .values()
        .flat_map(|chunk| chunk.slices.values())
        .any(|slice| slice.payload.is_some_and(|payload| payload.object_id == 1202)
            && slice.payload_state == PayloadRefState::Uploaded));
}

#[test]
fn test_backfill_pending_regular_file_recovery_preserves_existing_record() {
    let engine = temp_engine();
    let ino = engine
        .create_file(ROOT_INO, b"backfill-recovery.bin", 1000, 1000, 0o644)
        .unwrap()
        .ino;
    let mut slice = SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).unwrap(),
        PayloadRef::new_partial(1203, 4096),
    );
    slice.payload_state = PayloadRefState::Uploaded;
    let mut chunk = RegularFileChunkState::default();
    chunk.insert_slice(slice);
    set_regular_file_chunk_rows(&engine, ino, &[(ChunkId(0), chunk)], &[]);

    let mut attr = engine.get_attr(ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    let txn = engine.begin_write_txn().unwrap();
    txn.open_table(PENDING_REGULAR_FILE_RECOVERY_TABLE)
        .unwrap()
        .remove(ino)
        .unwrap();
    txn.commit().unwrap();

    engine
        .backfill_missing_uploaded_regular_file_recoveries()
        .unwrap();
    let first = engine
        .pending_uploaded_regular_file_recoveries()
        .unwrap()
        .into_iter()
        .find(|record| record.attr.ino == ino)
        .expect("backfilled uploaded recovery row");

    let mut changed_attr = engine.get_attr(ino).unwrap();
    changed_attr.mode = 0o600;
    engine
        .apply_replica_serving_state(&[MetaEvent::SetAttr { attr: changed_attr }])
        .unwrap();
    engine
        .backfill_missing_uploaded_regular_file_recoveries()
        .unwrap();
    let after = engine
        .pending_uploaded_regular_file_recoveries()
        .unwrap()
        .into_iter()
        .find(|record| record.attr.ino == ino)
        .expect("existing uploaded recovery row");

    assert_eq!(first.attr.mode, 0o644);
    assert_eq!(after.attr.mode, 0o600);
    assert_eq!(after.overlay.chunks, first.overlay.chunks);
}

#[test]
fn flush_rejects_invalid_slice_ranges_without_committing() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"invalid-slice-flush.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut changed_attr = attr.clone();
    changed_attr.size = 4096;
    changed_attr.content_epoch += 1;
    let before = engine.begin_read_txn().unwrap();
    let counters = before.open_table(COUNTERS_TABLE).unwrap();
    let next_event_seq = counters.get("nextEventSeq").unwrap().unwrap().value();

    for (start, end) in [(0, 0), (2, 1), (0, DEFAULT_CHUNK_SIZE + 1)] {
        let slices = vec![SliceRecord::new(
            SliceId::default(),
            SliceOrder::default(),
            SliceRange { start, end },
            PayloadRef::new_partial(3001, 1),
        )];
        let error = engine
            .flush_regular_file_overlay_slices_with_attr(
                attr.ino,
                &[
                    (
                        ChunkId(0),
                        vec![SliceRecord::tombstone(
                            SliceId::default(),
                            SliceOrder::default(),
                            SliceRange::new(3, 4).unwrap(),
                        )],
                    ),
                    (ChunkId(1), slices),
                ],
                Some(&changed_attr),
            )
            .expect_err("invalid range must fail before committing metadata");
        assert!(matches!(error, MetaError::Serialization(_)), "{error}");
        assert_eq!(engine.get_attr(attr.ino).unwrap(), attr);
        let after = engine.begin_read_txn().unwrap();
        assert!(
            !RedbMetaEngine::has_regular_file_chunk_rows_in_read_txn(
                &after,
                REGULAR_FILE_OVERLAY_TABLE,
                attr.ino,
            )
            .unwrap()
        );
        assert_eq!(
            after
                .open_table(COUNTERS_TABLE)
                .unwrap()
                .get("nextEventSeq")
                .unwrap()
                .unwrap()
                .value(),
            next_event_seq,
        );
        assert!(
            after
                .open_table(EVENT_LOG_TABLE)
                .unwrap()
                .get(next_event_seq)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn replica_apply_rejects_invalid_slice_ranges_without_committing() {
    let engine = temp_engine();
    let attr = engine
        .create_file(ROOT_INO, b"invalid-slice-apply.bin", 1000, 1000, 0o644)
        .unwrap();
    for (start, end) in [(0, 0), (2, 1), (0, DEFAULT_CHUNK_SIZE + 1)] {
        let event = MetaEvent::PublishRegularFileBaseSlices {
            ino: attr.ino,
            chunk_slices: vec![
                (
                    ChunkId(0),
                    vec![SliceRecord::tombstone(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(3, 4).unwrap(),
                    )],
                ),
                (
                    ChunkId(1),
                    vec![SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange { start, end },
                        PayloadRef::new_partial(3001, 1),
                    )],
                ),
            ],
        };
        let error = engine
            .apply_replica_events_to_base(std::slice::from_ref(&event))
            .expect_err("invalid range must fail before committing replica metadata");
        assert!(matches!(error, MetaError::Serialization(_)), "{error}");
        let txn = engine.begin_read_txn().unwrap();
        assert!(
            !RedbMetaEngine::has_regular_file_chunk_rows_in_read_txn(
                &txn,
                REGULAR_FILE_BASE_TABLE,
                attr.ino,
            )
            .unwrap()
        );
    }
}

#[test]
fn invalid_persisted_slice_ranges_fail_after_redb_reopen() {
    for (start, end, valid) in [
        (0, 1, true),
        (0, 0, false),
        (2, 1, false),
        (0, DEFAULT_CHUNK_SIZE + 1, false),
    ] {
        for table_def in [REGULAR_FILE_OVERLAY_TABLE, REGULAR_FILE_BASE_TABLE] {
            let dir = studiofs_test_support::workspace_target_tempdir("invalid-slice-range-");
            let path = dir.path().join("meta.redb");
            {
                let engine = RedbMetaEngine::open(&path).unwrap();
                engine.init_root(1000, 1000).unwrap();
                engine
                    .set_format_info(&test_format_info("invalid-slice-range"))
                    .unwrap();

                // Historical field order bypasses today's validating range serializer.
                let slices = BTreeMap::from([(
                    SliceId(1),
                    (
                        SliceId(1),
                        SliceOrder(1),
                        (start, end),
                        Some(PayloadRef::new_partial(3001, 1)),
                        PayloadRefState::LocalOnly,
                    ),
                )]);
                let bytes = encode(&(
                    SliceId::default(),
                    SliceOrder::default(),
                    slices,
                    ChunkFragmentationCounters::default(),
                ))
                .unwrap();
                if valid {
                    let mut chunk = RegularFileChunkState::default();
                    chunk.insert_slice(SliceRecord::new(
                        SliceId(1),
                        SliceOrder(1),
                        SliceRange::new(start, end).unwrap(),
                        PayloadRef::new_partial(3001, 1),
                    ));
                    assert_eq!(encode(&chunk).unwrap(), bytes);
                }
                let key = encode_regular_file_chunk_key(42, ChunkId(0));
                let txn = engine.begin_write_txn().unwrap();
                txn.open_table(table_def)
                    .unwrap()
                    .insert(key.as_slice(), bytes.as_slice())
                    .unwrap();
                txn.commit().unwrap();
            }

            let reopened = RedbMetaEngine::open(&path).unwrap();
            let result = reopened.lookup_regular_file_slices(42, 0, 1);
            if valid {
                assert!(
                    result.is_ok(),
                    "valid historical row must remain readable: {result:?}"
                );
                continue;
            }
            let error = result.expect_err("invalid persisted range must fail before read planning");
            assert!(
                matches!(error, MetaError::Serialization(_)),
                "invalid range [{start}, {end}) should be a typed corruption error: {error}"
            );
        }
    }
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
        PayloadRef::new_partial(1101, 2048),
    ));
    let mut chunk0_overlay = RegularFileChunkState::default();
    chunk0_overlay.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(tail_start + 512, tail_start + 1536).expect("valid chunk0 overlay"),
        PayloadRef::new_partial(1201, 1024),
    ));

    let mut chunk1_base = RegularFileChunkState::default();
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(3),
        SliceOrder(1),
        SliceRange::new(0, 1024).expect("valid chunk1 base low order"),
        PayloadRef::new_partial(1301, 1024),
    ));
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(4),
        SliceOrder(2),
        SliceRange::new(512, 2048).expect("valid chunk1 base high order"),
        PayloadRef::new_partial(1401, 1536),
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
    assert_eq!(spans[0].payload.as_ref().unwrap().object_id, 1101);

    assert_eq!(spans[1].chunk_id, ChunkId(0));
    assert_eq!(
        spans[1].range,
        SliceRange {
            start: tail_start + 512,
            end: tail_start + 1536
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].payload.as_ref().unwrap().object_id, 1201);

    assert_eq!(spans[2].chunk_id, ChunkId(0));
    assert_eq!(
        spans[2].range,
        SliceRange {
            start: tail_start + 1536,
            end: DEFAULT_CHUNK_SIZE
        }
    );
    assert_eq!(spans[2].source, ResolvedSliceSource::Base);
    assert_eq!(spans[2].payload.as_ref().unwrap().object_id, 1101);

    assert_eq!(spans[3].chunk_id, ChunkId(1));
    assert_eq!(spans[3].range, SliceRange { start: 0, end: 512 });
    assert_eq!(spans[3].source, ResolvedSliceSource::Base);
    assert_eq!(spans[3].payload.as_ref().unwrap().object_id, 1301);

    assert_eq!(spans[4].chunk_id, ChunkId(1));
    assert_eq!(
        spans[4].range,
        SliceRange {
            start: 512,
            end: 2048
        }
    );
    assert_eq!(spans[4].source, ResolvedSliceSource::Base);
    assert_eq!(spans[4].payload.as_ref().unwrap().object_id, 1401);
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
        PayloadRef::new_partial(2101, 1024),
    ));
    let mut chunk1_base = RegularFileChunkState::default();
    chunk1_base.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(1),
        SliceRange::new(0, 1024).expect("valid chunk1 base"),
        PayloadRef::new_partial(2102, 1024),
    ));

    let mut chunk0_overlay = RegularFileChunkState::default();
    chunk0_overlay.insert_slice(SliceRecord::new(
        SliceId(10),
        SliceOrder(1),
        SliceRange::new(tail_start + 512, DEFAULT_CHUNK_SIZE).expect("valid chunk0 overlay"),
        PayloadRef::new_partial(2201, 512),
    ));
    let mut chunk1_overlay = RegularFileChunkState::default();
    chunk1_overlay.insert_slice(SliceRecord::new(
        SliceId(11),
        SliceOrder(1),
        SliceRange::new(0, 512).expect("valid chunk1 overlay"),
        PayloadRef::new_partial(2202, 512),
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
    assert_eq!(spans[0].payload.as_ref().unwrap().object_id, 2101);

    assert_eq!(spans[1].chunk_id, ChunkId(0));
    assert_eq!(
        spans[1].range,
        SliceRange {
            start: tail_start + 512,
            end: DEFAULT_CHUNK_SIZE
        }
    );
    assert_eq!(spans[1].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[1].payload.as_ref().unwrap().object_id, 2201);

    assert_eq!(spans[2].chunk_id, ChunkId(1));
    assert_eq!(spans[2].range, SliceRange { start: 0, end: 512 });
    assert_eq!(spans[2].source, ResolvedSliceSource::Overlay);
    assert_eq!(spans[2].payload.as_ref().unwrap().object_id, 2202);

    assert_eq!(spans[3].chunk_id, ChunkId(1));
    assert_eq!(
        spans[3].range,
        SliceRange {
            start: 512,
            end: 1024
        }
    );
    assert_eq!(spans[3].source, ResolvedSliceSource::Base);
    assert_eq!(spans[3].payload.as_ref().unwrap().object_id, 2102);
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
        PayloadRef::new_partial(1601, 4096),
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
    mark_overlay_objects_uploaded(&engine, file.ino, 7, &[1601]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 7)
            .unwrap()
    );

    let txn = engine.begin_read_txn().unwrap();
    let base = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    let chunk_key = encode_regular_file_chunk_key(file.ino, ChunkId(2));
    let row = base
        .get(chunk_key.as_slice())
        .unwrap()
        .expect("published base chunk");
    let published: RegularFileChunkState = decode(row.value()).unwrap();
    assert_eq!(published.slices.len(), 2);
    assert!(
        published
            .slices
            .values()
            .all(|slice| slice.payload_state == PayloadRefState::Published)
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
fn publish_regular_file_overlay_recovers_visible_size_from_payload_graph() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-recovers-size.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 1;
    attr.size = 0;
    engine.set_attr(&attr).unwrap();

    let tail_start = u64::from(DEFAULT_CHUNK_SIZE);
    let tail_len = 611_410;
    let mut head_chunk = RegularFileChunkState::default();
    head_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid full block"),
        PayloadRef::new_partial(57_431_701, DEFAULT_CHUNK_SIZE),
    ));
    let mut tail_chunk = RegularFileChunkState::default();
    tail_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, tail_len).expect("valid tail block"),
        PayloadRef::new_partial(57_431_702, tail_len),
    ));
    set_regular_file_chunk_rows(
        &engine,
        file.ino,
        &[(ChunkId(0), head_chunk), (ChunkId(1), tail_chunk)],
        &[],
    );
    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[57_431_701, 57_431_702]);

    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    let expected_size = tail_start + u64::from(tail_len);
    assert_eq!(published_attr.size, expected_size);
    assert_eq!(published_attr.committed_size, expected_size);
    assert_eq!(published_attr.upload_state, UploadState::Ready);
}

#[test]
fn publish_regular_file_overlay_to_base_appends_dentry_only_on_first_visibility() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-dentry-once.bin", 0, 0, 0o644)
        .unwrap();

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(40_101, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[40_101]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

    let second_publish_start = engine.get_counter("nextEventSeq").unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 8192;
    attr.content_epoch = 2;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 1, &PayloadRef::new_partial(40_102, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 2, &[40_102]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .unwrap()
    );
    let second_publish_end = engine.get_counter("nextEventSeq").unwrap();

    let second_publish_events = engine
        .read_events_range(second_publish_start, second_publish_end)
        .unwrap();
    assert!(
        !second_publish_events.iter().any(
            |event| matches!(event, MetaEvent::AddEntry { entry, .. } if entry.ino == file.ino)
        ),
        "republishing an already visible file must not emit a duplicate AddEntry"
    );
}

#[test]
fn test_publish_regular_file_overlay_to_base_preserves_untouched_base_ranges() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-preserve-base.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 3;
    attr.size = 12 * 1024;
    engine.set_attr(&attr).unwrap();

    let mut base_chunk = RegularFileChunkState::default();
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid first base range"),
        PayloadRef::new_partial(3101, 4096),
    ));
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(4096, 8192).expect("valid second base range"),
        PayloadRef::new_partial(3102, 4096),
    ));
    base_chunk.insert_slice(SliceRecord::new(
        SliceId(3),
        SliceOrder(3),
        SliceRange::new(8192, 12 * 1024).expect("valid third base range"),
        PayloadRef::new_partial(3103, 4096),
    ));

    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(4096, 8192).expect("valid replacement range"),
        PayloadRef::new_partial(3202, 4096),
    ));
    set_regular_file_chunk_rows(
        &engine,
        file.ino,
        &[(ChunkId(0), overlay_chunk)],
        &[(ChunkId(0), base_chunk)],
    );

    mark_overlay_objects_uploaded(&engine, file.ino, 3, &[3202]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 3)
            .unwrap()
    );

    let spans = engine
        .lookup_regular_file_slices(file.ino, 0, attr.size)
        .unwrap();
    let object_ids: Vec<_> = spans
        .into_iter()
        .filter_map(|span| span.payload.map(|block| block.object_id))
        .collect();
    assert_eq!(
        object_ids,
        vec![3101, 3202, 3103],
        "publish must resolve overlay over base instead of dropping untouched base ranges"
    );
}

#[test]
fn test_publish_regular_file_overlay_to_base_commits_sparse_tail_logical_size() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"publish-sparse-tail-logical-size.bin",
            1000,
            1000,
            0o644,
        )
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
        PayloadRef::new_partial(2601, 4096),
    ));
    set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(0), overlay_chunk)], &[]);

    mark_overlay_objects_uploaded(&engine, file.ino, 5, &[2601]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 5)
            .unwrap()
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.size, 16 * 1024);
    assert_eq!(
        published_attr.committed_size, published_attr.size,
        "publishing a sparse tail must publish the logical EOF, not only the last payload byte"
    );
    let tail = engine
        .lookup_regular_file_slices(file.ino, 4096, 12 * 1024)
        .unwrap();
    assert!(
        tail.iter().all(|span| span.payload.is_none()),
        "the published sparse tail should remain hole-backed rather than requiring payload bytes"
    );
}

#[test]
fn test_publish_regular_file_overlay_to_base_accepts_fragmented_slice_graph() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"publish-fragmented-slices.bin",
            1000,
            1000,
            0o644,
        )
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
            PayloadRef::new_partial(4_201, 4096),
        ),
        SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(4096, 8192).unwrap(),
            PayloadRef::new_partial(4_202, 4096),
        ),
    ];
    engine
        .flush_regular_file_overlay_slices(file.ino, &[(chunk_id, incoming)])
        .unwrap();

    mark_overlay_objects_uploaded(&engine, file.ino, 9, &[4_201, 4_202]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 9)
            .unwrap(),
        "slice-native publish should not require a contiguous per-block summary"
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.upload_state, UploadState::Ready);
    assert_eq!(published_attr.committed_content_epoch, 9);
    assert_eq!(published_attr.committed_size, 8192);
}

#[test]
fn test_tombstone_only_overlay_is_publish_ready_for_sparse_shrink() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"publish-sparse-shrink.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 1;
    attr.size = 8192;
    engine.set_attr(&attr).unwrap();

    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(7_701, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[7_701]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

    let mut shrink_attr = engine.get_attr(file.ino).unwrap();
    shrink_attr.content_epoch = 2;
    shrink_attr.size = 4096;
    engine
        .flush_regular_file_overlay_slices_with_attr(
            file.ino,
            &[(
                ChunkId(0),
                vec![SliceRecord::tombstone(
                    SliceId(0),
                    SliceOrder(0),
                    SliceRange::new(4096, 8192).expect("valid sparse shrink tombstone"),
                )],
            )],
            Some(&shrink_attr),
        )
        .unwrap();

    let lifecycle = engine
        .get_inode_lifecycle(file.ino)
        .unwrap()
        .expect("sparse shrink lifecycle");
    assert_eq!(lifecycle.state, InodeLifecycleState::PublishReady);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .unwrap()
    );
    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.size, 4096);
    assert_eq!(published_attr.committed_size, 4096);
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
        PayloadRef::new_partial(601, 4096),
    ));

    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(1));
    let chunk_bytes = encode(&chunk).expect("encode committed chunk");
    let txn = engine.begin_write_txn().unwrap();
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

    let block0 = PayloadRef::new_full(101, DEFAULT_BLOCK_SIZE);
    let block1 = PayloadRef::new_partial(102, 4096);
    engine.stage_payload_slice(file.ino, 0, &block0).unwrap();
    engine.stage_payload_slice(file.ino, 1, &block1).unwrap();

    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[101, 102]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

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
        .map(|span| span.payload.expect("published payload slice"))
        .collect();
    assert_eq!(remote_blocks, vec![block0, block1]);
}

#[test]
fn sfs314_publish_requires_all_overlay_payloads_uploaded() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"sfs314-publish-ready-boundary.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 8192;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    let mut ready_slice = SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(0, 4096).expect("valid ready range"),
        PayloadRef::new_partial(31_401, 4096),
    );
    ready_slice.payload_state = PayloadRefState::Uploaded;
    let local_only_slice = SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(4096, 8192).expect("valid local-only range"),
        PayloadRef::new_partial(31_402, 4096),
    );
    let mut overlay_chunk = RegularFileChunkState::default();
    overlay_chunk.insert_slice(ready_slice);
    overlay_chunk.insert_slice(local_only_slice);
    set_regular_file_chunk_rows(&engine, file.ino, &[(ChunkId(0), overlay_chunk)], &[]);

    assert!(
        !engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap(),
        "publish must wait until every referenced overlay payload object is uploaded"
    );

    let remote_snapshot = engine.export_remote_snapshot().unwrap();
    assert!(
        !remote_snapshot
            .inodes
            .iter()
            .any(|(ino, _)| *ino == file.ino),
        "uploaded-but-unpublished payload must not make a new file peer-visible"
    );
    assert_eq!(
        engine.get_attr(file.ino).unwrap().committed_content_epoch,
        0
    );

    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[31_402]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap(),
        "publish should succeed once epoch and payload readiness match"
    );
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 8192),
        vec![31_401, 31_402]
    );
}

#[test]
fn sfs314_uploaded_payload_does_not_replace_peer_visible_base_before_publish() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"sfs314-peer-visibility-boundary.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(31_410, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[31_410]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 4096),
        vec![31_410]
    );

    let mut next_attr = engine.get_attr(file.ino).unwrap();
    next_attr.content_epoch = 2;
    next_attr.upload_state = UploadState::Updating;
    engine.set_attr(&next_attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(31_411, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 2, &[31_411]);

    let local_object_ids: Vec<_> = engine
        .lookup_regular_file_slices(file.ino, 0, 4096)
        .unwrap()
        .into_iter()
        .filter_map(|span| span.payload.map(|payload| payload.object_id))
        .collect();
    assert_eq!(local_object_ids, vec![31_411]);
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 4096),
        vec![31_410],
        "upload completion alone must leave peers on the previous committed version"
    );

    assert!(
        !engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap(),
        "stale publish attempts must not mutate visible base state"
    );
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 4096),
        vec![31_410]
    );

    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .unwrap()
    );
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 4096),
        vec![31_411]
    );
}

#[test]
fn sfs314_partial_publish_preserves_base_tail_and_committed_size() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"sfs314-partial-publish-base-tail.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 12 * 1024;
    attr.committed_size = 12 * 1024;
    attr.content_epoch = 1;
    attr.committed_content_epoch = 1;
    attr.upload_state = UploadState::Ready;
    engine.set_attr(&attr).unwrap();

    let mut base_chunk = RegularFileChunkState::default();
    for (slice_id, start, object_id) in [
        (SliceId(1), 0, 31_420),
        (SliceId(2), 4096, 31_421),
        (SliceId(3), 8192, 31_422),
    ] {
        let mut slice = SliceRecord::new(
            slice_id,
            SliceOrder(slice_id.0),
            SliceRange::new(start, start + 4096).expect("valid base range"),
            PayloadRef::new_partial(object_id, 4096),
        );
        slice.payload_state = PayloadRefState::Published;
        base_chunk.insert_slice(slice);
    }

    let mut overlay_chunk = RegularFileChunkState::default();
    let mut replacement = SliceRecord::new(
        SliceId(1),
        SliceOrder(1),
        SliceRange::new(4096, 8192).expect("valid replacement range"),
        PayloadRef::new_partial(31_423, 4096),
    );
    replacement.payload_state = PayloadRefState::Uploaded;
    overlay_chunk.insert_slice(replacement);
    set_regular_file_chunk_rows(
        &engine,
        file.ino,
        &[(ChunkId(0), overlay_chunk)],
        &[(ChunkId(0), base_chunk)],
    );

    let mut next_attr = engine.get_attr(file.ino).unwrap();
    next_attr.content_epoch = 2;
    next_attr.upload_state = UploadState::Updating;
    engine.set_attr(&next_attr).unwrap();

    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .unwrap()
    );

    let published_attr = engine.get_attr(file.ino).unwrap();
    assert_eq!(published_attr.committed_content_epoch, 2);
    assert_eq!(
        published_attr.committed_size,
        12 * 1024,
        "publishing a partial overlay must keep untouched committed base ranges in the new head"
    );
    assert_eq!(
        remote_payload_object_ids(&engine, file.ino, 12 * 1024),
        vec![31_420, 31_423, 31_422]
    );
}

#[test]
fn sfs314_metadata_only_apply_keeps_attrs_and_file_graph_consistent() {
    let engine = temp_engine();
    let file = engine
        .create_file(
            ROOT_INO,
            b"sfs314-metadata-only-apply.bin",
            1000,
            1000,
            0o644,
        )
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(31_430, 4096))
        .unwrap();
    mark_overlay_objects_uploaded(&engine, file.ino, 1, &[31_430]);
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

    let mut metadata_only_attr = engine.get_attr(file.ino).unwrap();
    metadata_only_attr.mode = 0o600;
    metadata_only_attr.ctime = metadata_only_attr.ctime.saturating_add(1);
    let event = MetaEvent::SetAttr {
        attr: metadata_only_attr.clone(),
    };

    let remote_snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&remote_snapshot).unwrap();
    remote
        .apply_replica_events_to_base(std::slice::from_ref(&event))
        .unwrap();
    remote
        .apply_replica_serving_state(std::slice::from_ref(&event))
        .unwrap();

    let remote_attr = remote.get_attr(file.ino).unwrap();
    assert_eq!(remote_attr.mode, 0o600);
    assert_eq!(remote_attr.size, 4096);
    assert_eq!(remote_attr.committed_size, 4096);
    assert_eq!(
        remote
            .lookup_regular_file_slices(file.ino, 0, 4096)
            .unwrap()
            .into_iter()
            .filter_map(|span| span.payload.map(|payload| payload.object_id))
            .collect::<Vec<_>>(),
        vec![31_430],
        "metadata-only apply must not rewrite or clear the published file graph"
    );
}
