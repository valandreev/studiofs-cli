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
        PayloadRef::new_partial(9_001, DEFAULT_CHUNK_SIZE),
    );
    overlay_slice.payload_state = PayloadRefState::Uploaded;
    overlay_chunk.insert_slice(overlay_slice);

    let mut base_chunk = RegularFileChunkState::default();
    let mut base_slice = SliceRecord::new(
        SliceId(2),
        SliceOrder(2),
        SliceRange::new(0, DEFAULT_CHUNK_SIZE).expect("valid base slice"),
        PayloadRef::new_partial(9_002, DEFAULT_CHUNK_SIZE),
    );
    base_slice.payload_state = PayloadRefState::Published;
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
fn remote_snapshot_hides_root_empty_regular_file_draft() {
    let engine = temp_engine();
    engine
        .create_file(ROOT_INO, b"root-empty-draft.bin", 0, 0, 0o644)
        .unwrap();

    let snapshot = engine.export_remote_snapshot().unwrap();
    let remote = temp_engine();
    remote.import_remote_snapshot(&snapshot).unwrap();

    assert!(
        remote.lookup(ROOT_INO, b"root-empty-draft.bin").is_err(),
        "brand-new empty files at root must stay hidden until publication"
    );
}

#[test]
fn import_remote_snapshot_preserving_local_state_keeps_local_overlay_and_event_log() {
    let local = temp_engine();
    let local_event_start = local.get_counter("nextEventSeq").unwrap();
    let draft = local
        .create_file(ROOT_INO, b"local-draft.bin", 0, 0, 0o644)
        .unwrap();
    local
        .set_xattr(draft.ino, b"user.color", b"local-blue", 0)
        .unwrap();
    local
        .set_acl_rule(draft.ino, "alice", AccessLevel::ReadWrite)
        .unwrap();
    let link = local
        .create_symlink(ROOT_INO, b"local-link", b"/local/target", 0, 0)
        .unwrap();
    local.set_counter("pgLastFlushedLocalEventSeq", local_event_start)
        .unwrap();
    local.set_counter("pgClientSlotPlusOne", 8).unwrap();
    local.set_counter(counters::NEXT_INODE, 50_000).unwrap();
    let local_event_end = local.get_counter("nextEventSeq").unwrap();

    let source = temp_engine();
    let remote_file = source
        .create_file(ROOT_INO, b"remote-file.bin", 0, 0, 0o644)
        .unwrap();
    source
        .set_xattr(remote_file.ino, b"user.color", b"remote-red", 0)
        .unwrap();
    source
        .set_acl_rule(remote_file.ino, "alice", AccessLevel::ReadOnly)
        .unwrap();
    source
        .create_symlink(ROOT_INO, b"remote-link", b"/remote/target", 0, 0)
        .unwrap();
    source.set_counter(counters::NEXT_INODE, 7_000).unwrap();
    source.set_counter("pgLastAppliedBatchVersion", 12).unwrap();
    source.set_counter("pgLastFlushedLocalEventSeq", 99).unwrap();
    source.set_counter("pgClientSlotPlusOne", 99).unwrap();
    let snapshot = source.export_snapshot().unwrap();

    local
        .import_remote_snapshot_preserving_local_state(&snapshot)
        .unwrap();

    let local_draft = local.lookup(ROOT_INO, b"local-draft.bin").unwrap();
    assert_eq!(local_draft.ino, draft.ino);
    assert_eq!(
        local.get_xattr(draft.ino, b"user.color").unwrap(),
        Some(b"local-blue".to_vec())
    );
    assert_eq!(
        local.get_acl_rule(draft.ino, "alice").unwrap(),
        Some(AccessLevel::ReadWrite)
    );
    assert_eq!(local.get_symlink_target(link.ino).unwrap(), b"/local/target");
    assert_eq!(local.get_counter("nextEventSeq").unwrap(), local_event_end);
    assert_eq!(local.get_counter(counters::NEXT_INODE).unwrap(), 50_000);
    assert_eq!(
        local
            .read_events_range(local_event_start, local_event_end)
            .unwrap()
            .len(),
        usize::try_from(local_event_end - local_event_start).unwrap()
    );
    assert_eq!(local.get_counter("pgLastFlushedLocalEventSeq").unwrap(), local_event_start);
    assert_eq!(local.get_counter("pgClientSlotPlusOne").unwrap(), 8);
    assert_eq!(local.get_counter("pgLastAppliedBatchVersion").unwrap(), 12);
}

#[test]
fn import_remote_snapshot_preserving_local_state_keeps_local_deletes_hidden() {
    let source = temp_engine();
    let remote_file = source
        .create_file(ROOT_INO, b"remote-delete-me.bin", 0, 0, 0o644)
        .unwrap();
    mark_empty_file_published(&source, remote_file.ino);
    let snapshot = source.export_snapshot().unwrap();

    let local = temp_engine();
    local.import_snapshot(&snapshot).unwrap();
    local.unlink(ROOT_INO, b"remote-delete-me.bin").unwrap();

    local
        .import_remote_snapshot_preserving_local_state(&snapshot)
        .unwrap();

    assert!(local.lookup(ROOT_INO, b"remote-delete-me.bin").is_err());
    assert!(local.get_attr(remote_file.ino).is_err());
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
        .stage_payload_slice(ino, 0, &PayloadRef::new_partial(21, 4096))
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(ino, 0, 21)
            .unwrap()
    );
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(ino, 0)
            .unwrap()
    );
    engine
        .stage_payload_slice(ino, 1, &PayloadRef::new_partial(22, 4096))
        .unwrap();

    let refs = engine.get_referenced_object_ids().unwrap();
    assert!(refs.contains(&21));
    assert!(refs.contains(&22));
    assert_eq!(refs.len(), 2);
}

fn unix_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

#[test]
fn sfs316_reachability_reports_active_base_overlay_and_snapshot_pins() {
    let fixture = ContractCoreFixture::new("sfs316-reachability").expect("create contract fixture");
    let engine = fixture.meta();

    let file = engine
        .create_file(ROOT_INO, b"reachability.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();

    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(31, 4096))
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 31)
            .unwrap()
    );
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );
    let snapshot = engine.create_snapshot(ROOT_INO).unwrap();
    assert_eq!(snapshot.pinned_object_ids, vec![31]);

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 2;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(32, 4096))
        .unwrap();

    let diagnostics = fixture
        .assert_invariants("sfs316 active graph and snapshot pins")
        .expect("contract invariants");
    let referenced: std::collections::BTreeSet<_> =
        diagnostics.referenced_object_ids.iter().copied().collect();
    assert_eq!(
        referenced,
        std::collections::BTreeSet::from([31, 32]),
        "snapshot-pinned base and active overlay objects should be reachable: {diagnostics}"
    );
    assert!(
        diagnostics
            .object_graphs
            .iter()
            .any(|graph| graph.ino == file.ino
                && graph.object_usage.contains(&(31, 4096))
                && graph.object_usage.contains(&(32, 4096))),
        "object graph diagnostics should name active base and overlay objects: {diagnostics}"
    );
}

#[test]
fn sfs316_publish_queues_displaced_objects_and_reachability_scan_is_idempotent() {
    let fixture =
        ContractCoreFixture::new("sfs316-pending-deletion").expect("create contract fixture");
    let engine = fixture.meta();

    let file = engine
        .create_file(ROOT_INO, b"overwrite.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(41, 4096))
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 41)
            .unwrap()
    );
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap()
    );

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.content_epoch = 2;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(42, 4096))
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 2, 42)
            .unwrap()
    );
    let before_publish = unix_now_secs();
    assert!(
        engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 2)
            .unwrap()
    );
    let after_publish = unix_now_secs();

    let first_refs = engine.get_referenced_object_ids().unwrap();
    let first_pending = engine.get_pending_deletions().unwrap();
    let second_refs = engine.get_referenced_object_ids().unwrap();
    let second_pending = engine.get_pending_deletions().unwrap();

    assert_eq!(
        first_refs, second_refs,
        "reachability scans must be idempotent"
    );
    assert_eq!(
        first_pending, second_pending,
        "pending deletion scans must be idempotent"
    );
    assert!(
        first_refs.contains(&42),
        "replacement object should be reachable after publish"
    );
    assert!(
        !first_refs.contains(&41),
        "displaced object should leave the live graph"
    );
    assert!(
        first_pending
            .iter()
            .any(|(object_id, _timestamp)| *object_id == 41),
        "displaced object should be moved to pending deletion"
    );
    let displaced_timestamp = first_pending
        .iter()
        .find_map(|(object_id, timestamp)| (*object_id == 41).then_some(*timestamp))
        .expect("displaced object pending deletion timestamp");
    assert!(
        (before_publish..=after_publish).contains(&displaced_timestamp),
        "pending deletion timestamp should be recorded when the displaced object is queued"
    );

    let diagnostics = fixture
        .assert_invariants("sfs316 displaced object pending deletion")
        .expect("contract invariants");
    assert!(
        diagnostics
            .pending_deletions
            .iter()
            .any(|(object_id, _timestamp)| *object_id == 41),
        "diagnostics should identify pending deletion object IDs: {diagnostics}"
    );
}

#[test]
fn sfs316_delete_before_publish_tombstone_does_not_publish_deleted_content() {
    let fixture =
        ContractCoreFixture::new("sfs316-delete-before-publish").expect("create contract fixture");
    let engine = fixture.meta();

    let file = engine
        .create_file(ROOT_INO, b"deleted-local.bin", 1000, 1000, 0o644)
        .unwrap();
    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    attr.content_epoch = 1;
    engine.set_attr(&attr).unwrap();
    engine
        .stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(51, 4096))
        .unwrap();
    assert!(
        engine
            .mark_regular_file_overlay_object_uploaded_if_matches(file.ino, 1, 51)
            .unwrap()
    );

    let before_unlink = unix_now_secs();
    engine.unlink(ROOT_INO, b"deleted-local.bin").unwrap();
    let after_unlink = unix_now_secs();

    assert!(
        !engine
            .publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .unwrap(),
        "deleted-before-publish inode must not publish its uploaded overlay"
    );
    let lifecycle = engine.get_inode_lifecycle(file.ino).unwrap().unwrap();
    assert_eq!(lifecycle.state, InodeLifecycleState::DeletedBeforePublish);
    assert_eq!(lifecycle.deferred_cleanup_object_ids, vec![51]);
    assert!(
        !engine.get_referenced_object_ids().unwrap().contains(&51),
        "deleted-before-publish payload should not remain in the live graph"
    );
    let pending_deletions = engine.get_pending_deletions().unwrap();
    let deleted_before_publish_timestamp = pending_deletions
        .iter()
        .find_map(|(object_id, timestamp)| (*object_id == 51).then_some(*timestamp))
        .expect("deleted-before-publish object pending deletion timestamp");
    assert!(
        (before_unlink..=after_unlink).contains(&deleted_before_publish_timestamp),
        "uploaded deleted-before-publish payload should be queued for cleanup"
    );
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
            PayloadRef::new_partial(301, 4096),
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
        PayloadRef::new_partial(401, 4096),
    );
    base_slice.payload_state = PayloadRefState::Published;
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

    let txn = imported.begin_read_txn().unwrap();
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
            PayloadRef::new_partial(1001, 4096),
        ),
    );
    chunk_slices.insert(
        SliceId(2),
        SliceRecord::new(
            SliceId(2),
            SliceOrder(2),
            SliceRange::new(0, 4096).expect("shadowed slice"),
            PayloadRef::new_partial(1002, 4096),
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

    let txn = source.begin_write_txn().unwrap();
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

    let txn = target.begin_read_txn().unwrap();
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
    let block = PayloadRef::new_partial(40_001, 4096);
    source.stage_payload_slice(file.ino, 0, &block).unwrap();

    let snapshot = source.export_snapshot().unwrap();
    let imported = temp_engine();
    imported.import_snapshot(&snapshot).unwrap();

    let imported_file = imported
        .lookup(ROOT_INO, b"snapshot-local-only.bin")
        .unwrap();
    assert_eq!(imported_file.ino, file.ino);
    let imported_attr = imported.get_attr(imported_file.ino).unwrap();
    assert_eq!(imported_attr.size, 4096);
    assert_eq!(imported_attr.committed_size, 0);
    assert_eq!(imported_attr.upload_state, UploadState::Uploading);
    let imported_spans = imported
        .lookup_regular_file_slices(imported_file.ino, 0, imported_attr.size)
        .unwrap();
    assert_eq!(imported_spans.len(), 1);
    assert_eq!(
        imported_spans[0]
            .payload
            .as_ref()
            .expect("imported local-only slice")
            .object_id,
        block.object_id
    );
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
        PayloadRef::new_partial(777, 4096),
    ));
    let chunk_key = encode_regular_file_chunk_key(ino, ChunkId(0));
    let chunk_bytes = encode(&chunk).expect("encode chunk state");

    let txn = engine.begin_write_txn().unwrap();
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

    let txn = engine.begin_read_txn().unwrap();
    let overlay_table = txn.open_table(REGULAR_FILE_OVERLAY_TABLE).unwrap();
    assert!(overlay_table.get(chunk_key.as_slice()).unwrap().is_none());
    let base_table = txn.open_table(REGULAR_FILE_BASE_TABLE).unwrap();
    assert!(base_table.get(chunk_key.as_slice()).unwrap().is_none());
}
