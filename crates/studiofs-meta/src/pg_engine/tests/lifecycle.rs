use super::*;

#[test]
fn prepare_for_close_does_not_block_forever_on_slow_background_thread() {
    let _join_timeout = set_test_env("STUDIOFS_PG_PREPARE_FOR_CLOSE_JOIN_TIMEOUT_MS", "50");
    let (_dir, engine) = temp_pg_engine_for_unit();
    *engine.poll_handle.lock().expect("poll_handle lock") = Some(
        std::thread::Builder::new()
            .name("pg-test-slow-close".into())
            .spawn(|| {
                std::thread::sleep(Duration::from_secs(5));
            })
            .expect("spawn slow background thread"),
    );

    let started = Instant::now();
    engine.prepare_for_close();

    assert!(
        started.elapsed() < Duration::from_secs(1),
        "prepare_for_close should respect the bounded join timeout for slow threads"
    );
}

#[test]
fn startup_snapshot_is_skipped_when_local_overlay_exists() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local draft");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("local overlay mutation"),
        "local draft namespace should register as a local overlay mutation"
    );
    assert!(
        !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 0)
            .expect("startup snapshot decision"),
        "startup snapshot must not clobber a local overlay after crash/restart"
    );
}

#[test]
fn persisted_pg_version_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );
    assert!(
        !PgMetaEngine::should_pull_startup_snapshot(&pg.local, 42)
            .expect("startup snapshot decision"),
        "persisted PG version should suppress checkpoint bootstrap overwrite"
    );
}

#[test]
fn startup_snapshot_replay_starts_from_snapshot_epoch_not_pg_head() {
    let mut snapshot = RedbSnapshot::default();
    snapshot
        .counters
        .push((LOCAL_PG_VERSION_COUNTER.to_string(), 3));

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 0),
        3,
        "metadata_state.version is the remote head; snapshot import must resume from the checkpoint epoch"
    );
}

#[test]
fn startup_snapshot_replay_caps_snapshot_epoch_to_pg_head() {
    let mut snapshot = RedbSnapshot::default();
    snapshot
        .counters
        .push((LOCAL_PG_VERSION_COUNTER.to_string(), 12));

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 0),
        9,
        "a stale or corrupt checkpoint cursor must not advance past the advertised PG head"
    );
}

#[test]
fn startup_snapshot_replay_uses_previous_epoch_when_snapshot_has_no_cursor() {
    let snapshot = RedbSnapshot::default();

    assert_eq!(
        PgMetaEngine::snapshot_replay_start_version(&snapshot, 9, 2),
        2,
        "legacy checkpoints without a PG cursor must keep the previous local replay floor"
    );
}

#[test]
fn remote_snapshot_does_not_export_local_pg_state() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 7).expect("persist client slot");
    local
        .write_counters(|counters| {
            counters.set(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER, 150)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER, 1_000)?;
            counters.set(LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER, 1_100)?;
            counters.set(LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER, 1_200)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER, 2_000)?;
            counters.set(LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER, 2_100)?;
            counters.set(LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER, 2_200)?;
            Ok(())
        })
        .expect("seed local pg counters");

    let snapshot = local
        .export_remote_snapshot()
        .expect("export remote snapshot");

    for local_counter in [
        LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER,
        LOCAL_PG_CLIENT_SLOT_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER,
        LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER,
        LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER,
        LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER,
    ] {
        assert!(
            snapshot
                .counters
                .iter()
                .all(|(name, _)| name != local_counter),
            "remote discovery snapshots must not export local-only PG counter {local_counter}"
        );
    }
}

#[test]
fn remote_snapshot_import_drops_legacy_local_pg_state() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&path).expect("open local redb");
    local.init_root(0, 0).expect("init root");
    PgMetaEngine::persist_client_slot(&local, 3).expect("persist reserved local slot");

    let mut legacy_snapshot = RedbSnapshot::default();
    legacy_snapshot.counters.extend([
        (LOCAL_PG_VERSION_COUNTER.to_string(), 9),
        (LOCAL_PG_NAMESPACE_VERSION_COUNTER.to_string(), 8),
        (LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER.to_string(), 150),
        (LOCAL_PG_CLIENT_SLOT_COUNTER.to_string(), 8),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_START_COUNTER.to_string(),
            1_000,
        ),
        (
            LEGACY_LOCAL_PG_INODE_ACTIVE_RANGE_END_COUNTER.to_string(),
            1_100,
        ),
        (
            LEGACY_LOCAL_PG_INODE_RESERVED_RANGE_END_COUNTER.to_string(),
            1_200,
        ),
        (
            LOCAL_PG_INODE_CURRENT_WINDOW_START_COUNTER.to_string(),
            2_000,
        ),
        (LOCAL_PG_INODE_CURRENT_WINDOW_END_COUNTER.to_string(), 2_100),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_START_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_NEXT_COUNTER.to_string(),
            2_100,
        ),
        (
            LOCAL_PG_INODE_PREFETCHED_WINDOW_END_COUNTER.to_string(),
            2_200,
        ),
    ]);

    local
        .import_remote_snapshot(&legacy_snapshot)
        .expect("import legacy remote snapshot");

    assert_eq!(
        PgMetaEngine::load_persisted_client_slot(&local).expect("load client slot"),
        None,
        "remote snapshot import must not reuse the checkpoint writer slot"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_FLUSHED_EVENT_SEQ_COUNTER)
            .expect("load flushed cursor"),
        0,
        "remote snapshot import must drop legacy local event-log cursor"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("load applied version"),
        9,
        "remote replay cursor must remain part of the imported data snapshot"
    );
}

#[test]
fn snapshot_reconcile_resets_stale_regular_file_serving_attr() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .create_file(ROOT_INO, b"snapshot-stale-serving.txt", 0, 0, 0o644)
        .expect("create file");
    set_file_state(&pg, file.ino, 227, 1);
    let local_payload = PayloadRef::new_partial(91_001, 227);
    pg.stage_payload_slice(file.ino, 0, &local_payload)
        .expect("stage local payload");
    mark_overlay_objects_uploaded(&pg, file.ino, 1, &[local_payload.object_id]);
    assert!(
        pg.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)
            .expect("publish local payload"),
        "local publish should make the file unprotected"
    );
    let event_end = pg.local.get_counter("nextEventSeq").expect("event end");
    pg.flushed_event_seq.store(event_end, Ordering::SeqCst);

    let mut remote_attr = pg.get_attr(file.ino).expect("load published attr");
    remote_attr.size = 223;
    remote_attr.committed_size = 223;
    remote_attr.content_epoch = 1;
    remote_attr.committed_content_epoch = 1;
    remote_attr.upload_state = UploadState::Ready;
    pg.local
        .apply_replica_events_to_base(&[MetaEvent::SetAttr {
            attr: remote_attr.clone(),
        }])
        .expect("apply remote base attr");

    assert_eq!(
        pg.get_attr(file.ino).expect("load stale serving attr").size,
        227,
        "base-only import can leave a stale serving attr until reconciliation"
    );

    pg.reconcile_imported_snapshot_regular_files()
        .expect("reconcile imported snapshot");

    assert_eq!(
        pg.get_attr(file.ino).expect("load reconciled attr").size,
        remote_attr.size,
        "snapshot reconciliation must reset stale regular-file serving attrs from base"
    );
}

#[test]
fn persisted_pg_flushed_event_seq_roundtrips_in_local_counter() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("initial flushed event seq"),
        0
    );
    pg.persist_flushed_event_seq(19)
        .expect("persist flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 19);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        19
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_flush_advances_checkpoint_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = tempdir().expect("tempdir");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-test");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    create_published_small_file(&pg, ROOT_INO, b"a.txt", 44_010);
    pg.flush_metadata().expect("flush metadata");
    let ok = wait_until(Duration::from_secs(10), || {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            let conn_task = tokio::spawn(async move {
                let _ = connection.await;
            });
            let row = client
                .query_opt(
                    "SELECT version FROM metadata_state WHERE vol=$1",
                    &[&volume.volume_id],
                )
                .await
                .map_err(|e| MetaError::Storage(e.to_string()))?;
            conn_task.abort();
            Ok::<bool, MetaError>(row.map(|r| r.get::<_, i64>(0) >= 1).unwrap_or(false))
        })
        .unwrap_or(false)
    });
    assert!(ok, "expected metadata_state version >= 1");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_snapshot_eventually_visible_and_mountable_on_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = tempdir().expect("tempdir a");
    let dir_b = tempdir().expect("tempdir b");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-snapshot-visibility");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    let file = a
        .create_file(ROOT_INO, b"snap-peer.txt", 0, 0, 0o644)
        .expect("create file on a");
    set_file_state(&a, file.ino, 64, 1);
    a.stage_payload_slice(file.ino, 0, &PayloadRef::new_partial(44_001, 64))
        .expect("set payload slice");

    let created = a.create_snapshot(ROOT_INO).expect("create snapshot");
    a.flush_metadata().expect("flush snapshot");

    let visible = wait_until(Duration::from_secs(12), || {
        let _ = b.sync_metadata();
        b.get_snapshot(&created.id)
            .ok()
            .flatten()
            .map(|s| s.state == SnapshotState::Synced)
            .unwrap_or(false)
    });
    assert!(visible, "snapshot should become visible on peer");

    let peer_snapshot = b
        .get_snapshot(&created.id)
        .expect("snapshot query")
        .expect("snapshot exists on peer");
    assert!(peer_snapshot.pinned_object_ids.contains(&44_001));
    assert!(!peer_snapshot.snapshot_payload.is_empty());

    let mount_dir = tempdir().expect("mount tempdir");
    let mount_db = mount_dir.path().join("snapshot-mount.redb");
    let mount_engine = RedbMetaEngine::open(&mount_db).expect("open mount redb");
    let mut mount_info = info.clone();
    mount_info.metadata.backend = MetadataBackend::RedbLocal;
    mount_info.metadata.postgres = None;
    mount_engine
        .set_format_info(&mount_info)
        .expect("set mount format");
    let snap_state = RedbMetaEngine::decode_snapshot_payload(&peer_snapshot.snapshot_payload)
        .expect("decode payload");
    mount_engine
        .import_snapshot(&snap_state)
        .expect("import payload");
    assert!(mount_engine.lookup(ROOT_INO, b"snap-peer.txt").is_ok());
}
