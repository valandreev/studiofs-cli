#[test]
fn test_root_inode_exists() {
    let engine = temp_engine();
    let attr = engine.get_attr(ROOT_INO).unwrap();
    assert_eq!(attr.ino, ROOT_INO);
    assert_eq!(attr.kind, FileType::Directory);
    assert_eq!(attr.mode, 0o755);
}

#[test]
fn test_get_attrs_batch_returns_ordered_attrs() {
    let engine = temp_engine();
    let a = engine
        .create_file(ROOT_INO, b"a.txt", 1000, 1000, 0o644)
        .unwrap();
    let b = engine
        .create_file(ROOT_INO, b"b.txt", 1000, 1000, 0o644)
        .unwrap();

    let attrs = engine.get_attrs_batch(&[b.ino, a.ino]).unwrap();
    assert_eq!(attrs.len(), 2);
    assert_eq!(attrs[0].ino, b.ino);
    assert_eq!(attrs[1].ino, a.ino);
}

#[test]
fn test_get_attrs_batch_skips_missing_inodes() {
    let engine = temp_engine();
    let kept = engine
        .create_file(ROOT_INO, b"kept.txt", 1000, 1000, 0o644)
        .unwrap();
    let removed = engine
        .create_file(ROOT_INO, b"removed.txt", 1000, 1000, 0o644)
        .unwrap();
    engine.unlink(ROOT_INO, b"removed.txt").unwrap();

    let attrs = engine.get_attrs_batch(&[removed.ino, kept.ino]).unwrap();
    assert_eq!(attrs.len(), 1);
    assert_eq!(attrs[0].ino, kept.ino);
}

#[test]
fn test_next_inode_increments() {
    let engine = temp_engine();
    let ino1 = engine.next_inode().unwrap();
    let ino2 = engine.next_inode().unwrap();
    assert_eq!(ino2, ino1 + 1);
}

#[test]
fn test_list_inode_attrs_ignores_sparse_next_inode_range() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"sparse.txt", 1000, 1000, 0o644)
        .unwrap();
    engine.set_counter(counters::NEXT_INODE, 1_000_000).unwrap();

    let attrs = engine.list_inode_attrs().unwrap();
    let inos: std::collections::BTreeSet<_> = attrs.into_iter().map(|attr| attr.ino).collect();

    assert!(inos.contains(&ROOT_INO));
    assert!(inos.contains(&file.ino));
    assert_eq!(inos.len(), 2);
}

#[test]
fn test_list_inode_lifecycles_ignores_sparse_next_inode_range() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"sparse-lifecycle.txt", 1000, 1000, 0o644)
        .unwrap();
    engine.set_counter(counters::NEXT_INODE, 1_000_000).unwrap();

    let lifecycles = engine.list_inode_lifecycles().unwrap();
    let inos: std::collections::BTreeSet<_> =
        lifecycles.into_iter().map(|record| record.ino).collect();

    assert!(inos.contains(&file.ino));
    assert_eq!(inos.len(), 1);
}

// === Architecture Tests ===

#[test]
fn test_detect_local_overlay_state() {
    let engine = temp_engine();

    let file = engine
        .create_file(ROOT_INO, b"draft.bin", 1000, 1000, 0o644)
        .unwrap();
    assert!(!engine.has_local_overlay_state().unwrap());

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.size = 4096;
    engine.set_attr(&attr).unwrap();
    engine
        .set_file_block(file.ino, 0, &BlockRef::new_partial(11, 4096))
        .unwrap();
    assert!(engine.has_local_overlay_state().unwrap());

    assert!(
        engine
            .commit_uploaded_block(file.ino, 0, 0, &BlockRef::new_partial(11, 4096))
            .unwrap()
    );
    assert!(engine.has_local_overlay_state().unwrap());
}

#[test]
fn test_detect_local_overlay_state_for_attr_only_against_base() {
    let source = temp_engine();
    let base_file = source
        .create_file(ROOT_INO, b"base_attr.txt", 1000, 1000, 0o644)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(!engine.has_local_overlay_state().unwrap());

    let mut attr = engine.get_attr(base_file.ino).unwrap();
    attr.mode = 0o600;
    engine.set_attr(&attr).unwrap();

    assert!(engine.has_local_overlay_state().unwrap());
}

#[test]
fn test_detect_overlay_attr_against_imported_base() {
    let source = temp_engine();
    let base_file = source
        .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();
    assert!(engine.local_overlay_attr_inodes().unwrap().is_empty());

    let mut attr = engine.get_attr(base_file.ino).unwrap();
    attr.mode = 0o600;
    engine.set_attr(&attr).unwrap();

    let overlay = engine.local_overlay_attr_inodes().unwrap();
    assert!(overlay.contains(&base_file.ino));
}

#[test]
fn test_set_attr_accepts_fragmented_regular_file_base_graph() {
    let engine = temp_engine();
    let file = engine
        .create_file(ROOT_INO, b"fragmented-base-attr.txt", 1000, 1000, 0o644)
        .unwrap();

    let mut base_chunk = RegularFileChunkState::default();
    base_chunk.slices.insert(
        SliceId(0),
        SliceRecord {
            slice_id: SliceId(0),
            order: SliceOrder(0),
            range: SliceRange::new(0, 4096).unwrap(),
            block: Some(BlockRef::new_partial(41, 4096)),
            block_state: BlockRefState::Published,
        },
    );
    base_chunk.slices.insert(
        SliceId(1),
        SliceRecord {
            slice_id: SliceId(1),
            order: SliceOrder(1),
            range: SliceRange::new(4096, 8192).unwrap(),
            block: Some(BlockRef::new_partial(42, 4096)),
            block_state: BlockRefState::Published,
        },
    );
    base_chunk.refresh_fragmentation_metrics();
    set_regular_file_chunk_rows(&engine, file.ino, &[], &[(ChunkId(0), base_chunk)]);

    let mut attr = engine.get_attr(file.ino).unwrap();
    attr.mode = 0o600;
    engine
        .set_attr(&attr)
        .expect("set_attr should not synthesize a flat manifest for fragmented base rows");

    let loaded = engine.get_attr(file.ino).unwrap();
    assert_eq!(loaded.mode, 0o600);
}

#[test]
fn test_getattr_falls_back_to_base_when_no_local_overlay() {
    let engine = temp_engine();

    let base_attr = InodeAttr::new_file(42, ROOT_INO, 1000, 1000, 0o644);
    engine
        .apply_replica_events_to_base(&[crate::events::MetaEvent::SetAttr {
            attr: base_attr.clone(),
        }])
        .unwrap();

    let loaded = engine.get_attr(42).unwrap();
    assert_eq!(loaded.ino, 42);
    assert_eq!(loaded.mode, base_attr.mode);
}

#[test]
fn test_get_attr_merged_in_write_txn_matches_read_get_attr() {
    let source = temp_engine();
    let base_file = source
        .create_file(ROOT_INO, b"base.txt", 1000, 1000, 0o644)
        .unwrap();
    let local_file = source
        .create_file(ROOT_INO, b"local.txt", 1000, 1000, 0o644)
        .unwrap();
    let snapshot = source.export_snapshot().unwrap();

    let engine = temp_engine();
    engine.import_snapshot(&snapshot).unwrap();

    let mut local_attr = engine.get_attr(local_file.ino).unwrap();
    local_attr.mode = 0o600;
    engine.set_attr(&local_attr).unwrap();

    let txn = engine.db.begin_write().unwrap();
    let base_from_write = RedbMetaEngine::get_attr_merged_in_write_txn(&txn, base_file.ino).unwrap();
    let local_from_write =
        RedbMetaEngine::get_attr_merged_in_write_txn(&txn, local_file.ino).unwrap();
    txn.abort().unwrap();

    let base_from_read = engine.get_attr(base_file.ino).unwrap();
    let local_from_read = engine.get_attr(local_file.ino).unwrap();

    assert_eq!(base_from_write.ino, base_from_read.ino);
    assert_eq!(base_from_write.mode, base_from_read.mode);
    assert_eq!(local_from_write.ino, local_from_read.ino);
    assert_eq!(local_from_write.mode, local_from_read.mode);
}

#[test]
fn test_decr_counter() {
    let engine = temp_engine();
    engine.set_counter("test_space", 1000).unwrap();
    let v = engine.decr_counter("test_space", 300).unwrap();
    assert_eq!(v, 700);
    let v = engine.decr_counter("test_space", 700).unwrap();
    assert_eq!(v, 0);
    // Saturating subtraction — doesn't go below 0
    let v = engine.decr_counter("test_space", 1).unwrap();
    assert_eq!(v, 0);
}

#[test]
fn test_incr_counter_accumulates_from_zero() {
    let engine = temp_engine();

    let first = engine.incr_counter("test_hits", 3).unwrap();
    let second = engine.incr_counter("test_hits", 4).unwrap();

    assert_eq!(first, 3);
    assert_eq!(second, 7);
    assert_eq!(engine.get_counter("test_hits").unwrap(), 7);
}

#[test]
fn test_decr_counter_unset() {
    let engine = temp_engine();
    // Decrementing an unset counter (defaults to 0) saturates at 0
    let v = engine.decr_counter("noexist", 42).unwrap();
    assert_eq!(v, 0);
}
