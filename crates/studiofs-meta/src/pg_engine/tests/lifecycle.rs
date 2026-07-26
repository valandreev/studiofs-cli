use super::*;
use crate::engine::MetaAcl;

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
fn startup_snapshot_preserves_local_overlay_when_local_overlay_exists() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.local
        .create_file(ROOT_INO, b"draft.txt", 0, 0, 0o644)
        .expect("create local draft");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("local overlay mutation"),
        "local draft namespace should register as a local overlay mutation"
    );
    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::PreserveLocalState),
        "startup snapshot must preserve a local overlay after crash/restart"
    );
}

#[test]
fn repair_startup_keeps_recovery_marker_when_remote_sync_fails() {
    let _connect_timeout = set_test_env("STUDIOFS_PG_CONNECT_TIMEOUT_MS", "50");
    let _command_timeout = set_test_env("STUDIOFS_PG_COMMAND_TIMEOUT_MS", "50");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let local = RedbMetaEngine::open(&db).expect("open local metadata");
    local.init_root(0, 0).expect("init root");
    local
        .set_format_info(&format_info(
            "repair-startup-remote-failure",
            "repair-startup-remote-failure-uuid",
            "postgres://127.0.0.1:1/studiofs",
            1,
        ))
        .expect("set unavailable postgres config");
    local
        .set_counter(LOCAL_PG_CLIENT_SLOT_COUNTER, 1)
        .expect("persist client slot");
    local
        .set_counter(LOCAL_PG_VERSION_COUNTER, 73)
        .expect("persist stale replay cursor");
    local
        .set_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER, 71)
        .expect("persist stale namespace replay cursor");
    local
        .set_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER, 1)
        .expect("set repair recovery marker");
    drop(local);

    let reopened = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
        },
    );
    assert!(
        reopened.is_err(),
        "repair startup must fail rather than starting cleanup from stale local state"
    );

    let local = RedbMetaEngine::open(&db).expect("reopen local metadata");
    assert_eq!(
        local
            .get_counter(LOCAL_PG_REPAIR_LOCAL_GRAPH_RECOVERY_COUNTER)
            .expect("read repair recovery marker"),
        1,
        "failed repair startup must leave the recovery marker durable for a later retry"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_VERSION_COUNTER)
            .expect("read replay cursor"),
        0,
        "repair startup must reset stale replay cursor before attempting remote recovery"
    );
    assert_eq!(
        local
            .get_counter(LOCAL_PG_NAMESPACE_VERSION_COUNTER)
            .expect("read namespace replay cursor"),
        0,
        "repair startup must reset stale namespace replay cursor before attempting remote recovery"
    );
}

#[test]
fn startup_snapshot_preserves_local_overlay_when_only_acl_changed() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    let file = pg
        .local
        .create_file(ROOT_INO, b"acl-only.txt", 0, 0, 0o644)
        .expect("create base file");
    let snapshot = pg.local.export_snapshot().expect("export base snapshot");
    pg.local
        .import_snapshot(&snapshot)
        .expect("seed imported base");

    assert!(
        !PgMetaEngine::has_any_local_overlay_mutation(&pg.local)
            .expect("no local overlay mutation before ACL edit")
    );

    pg.local
        .set_acl_rule(file.ino, "alice", AccessLevel::ReadOnly)
        .expect("set local ACL overlay");

    assert!(
        PgMetaEngine::has_any_local_overlay_mutation(&pg.local).expect("ACL overlay mutation"),
        "ACL-only local changes should require preserve-local-state startup import"
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
}

#[test]
fn startup_snapshot_is_skipped_for_existing_local_graph() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");

    assert!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 42)
            .expect("startup snapshot decision")
            .is_none(),
        "an existing local graph must catch up from its own cursor without importing a checkpoint"
    );
}

#[test]
fn startup_snapshot_replaces_acknowledged_local_overlay() {
    let (_dir, pg) = temp_pg_engine_for_unit();
    pg.local
        .create_dir(ROOT_INO, b"acknowledged", 0, 0, 0o755)
        .expect("create local directory");
    let event_end = pg.local.get_counter("nextEventSeq").expect("event end");
    pg.persist_flushed_event_seq(event_end)
        .expect("acknowledge local events");

    assert_eq!(
        PgMetaEngine::startup_snapshot_import_mode(&pg.local, 0)
            .expect("startup snapshot decision"),
        Some(super::StartupSnapshotImportMode::ReplaceLocalState),
        "acknowledged local events must not hide the authoritative remote snapshot"
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
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
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
fn import_remote_snapshot_drops_legacy_local_pg_state() {
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
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
fn local_position_leaf_persistence_is_monotonic() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_local_version(42).expect("persist local version");
    pg.persist_local_version(17)
        .expect("ignore older local version");
    assert_eq!(pg.local_version.load(Ordering::SeqCst), 42);
    assert_eq!(
        PgMetaEngine::load_persisted_local_version(&pg.local).expect("reload local version"),
        42
    );

    pg.persist_namespace_version(23)
        .expect("persist namespace version");
    pg.persist_namespace_version(11)
        .expect("ignore older namespace version");
    assert_eq!(pg.namespace_version.load(Ordering::SeqCst), 23);
    assert_eq!(
        PgMetaEngine::load_persisted_namespace_version(&pg.local)
            .expect("reload namespace version"),
        23
    );

    pg.persist_flushed_event_seq(25)
        .expect("persist flushed event seq");
    pg.persist_flushed_event_seq(19)
        .expect("ignore older flushed event seq");
    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25
    );
}

#[test]
fn replay_position_does_not_regress_flushed_event_seq() {
    let (_dir, pg) = temp_pg_engine_for_unit();

    pg.persist_flushed_event_seq(25)
        .expect("persist newer flushed event seq");
    pg.persist_replay_position(&RemoteReplayApplyOutcome {
        changed: true,
        max_version: 7,
        max_namespace_version: 3,
        flushed_event_seq: Some(19),
    })
    .expect("persist replay position");

    assert_eq!(pg.flushed_event_seq.load(Ordering::SeqCst), 25);
    assert_eq!(
        PgMetaEngine::load_persisted_flushed_event_seq(&pg.local)
            .expect("reload flushed event seq"),
        25,
        "replay position persistence must not move the local flushed cursor backwards"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_flush_advances_checkpoint_version() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
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
fn test_pg_checkpoint_does_not_move_remote_head_backwards() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-checkpoint-monotonic");
    seed_local(&db, &volume.format_info());

    let pg = PgMetaEngine::open(&db).expect("open pg");
    pg.local_version.store(10, Ordering::SeqCst);
    pg.write_checkpoint_locked()
        .expect("write newer checkpoint");
    pg.local_version.store(5, Ordering::SeqCst);
    pg.write_checkpoint_locked()
        .expect("write stale checkpoint");

    let version = run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol=$1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|e| MetaError::Storage(e.to_string()))?;
        conn_task.abort();
        Ok::<i64, MetaError>(row.get(0))
    })
    .expect("query checkpoint version");

    assert_eq!(
        version, 10,
        "stale checkpoint writes must not move the remote metadata head backwards"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_snapshot_eventually_visible_and_mountable_on_peer() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
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

    let mount_dir = studiofs_test_support::workspace_target_tempdir("test-");
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

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_reuses_connection_for_repeated_remote_metadata_queries() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-reuse");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    engine
        .remote_metadata_head_version()
        .expect("first metadata head query");
    engine
        .remote_metadata_head_version()
        .expect("second metadata head query");
    let after = engine.command_pool_stats();

    assert!(
        after.connection_creations - before.connection_creations <= 1,
        "repeated command-path queries should reuse one pooled PostgreSQL connection"
    );
    assert_eq!(
        after.checkouts - before.checkouts,
        2,
        "both metadata head queries should go through the command pool"
    );
    assert_eq!(
        after.discarded - before.discarded,
        0,
        "successful repeated queries should keep the pooled connection healthy"
    );
    assert_eq!(
        after.checkout_waits - before.checkout_waits,
        0,
        "serial repeated queries should not wait for a saturated command pool"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn command_pool_discards_client_after_application_error() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-command-pool-app-error");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open pg engine");

    let before = engine.command_pool_stats();
    let command_pool = engine.command_pool.clone();
    let command_timeout = engine.command_timeout;
    let bad_query = engine.run_async(async move {
        PgMetaEngine::with_postgres_client_timeout(
            command_pool,
            command_timeout,
            "intentional application error",
            move |client| {
                Box::pin(async move {
                    client
                        .batch_execute("SELECT * FROM studiofs_missing_pool_test_table")
                        .await
                        .map_err(|e| MetaError::Storage(PgMetaEngine::format_pg_error(e)))?;
                    Ok(())
                })
            },
        )
        .await
    });
    assert!(bad_query.is_err(), "bad query should fail");

    engine
        .remote_metadata_head_version()
        .expect("metadata head query after application error");
    let after = engine.command_pool_stats();

    assert_eq!(
        after.discarded - before.discarded,
        1,
        "application errors should discard the pooled client to avoid stale transaction state"
    );
    assert_eq!(
        after.connection_creations - before.connection_creations,
        1,
        "the next command should reconnect after the failed client is discarded"
    );
}
