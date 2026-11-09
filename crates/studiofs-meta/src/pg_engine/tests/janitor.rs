use super::*;

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_janitor_lock_is_exclusive() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-lock");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    let a = PgMetaEngine::open(&db_a).expect("open a");
    let b = PgMetaEngine::open(&db_b).expect("open b");

    assert!(
        a.try_acquire_global_janitor_lock()
            .expect("janitor lock acquire on a"),
        "first janitor should acquire the PG lock"
    );
    assert!(
        !b.try_acquire_global_janitor_lock()
            .expect("janitor lock probe on b while a holds it"),
        "janitor should skip when another client already holds the PG lock"
    );
    a.release_global_janitor_lock()
        .expect("janitor lock release on a");

    assert!(
        b.try_acquire_global_janitor_lock()
            .expect("janitor lock acquire on b after release"),
        "janitor should proceed once the PG lock is free again"
    );
    b.release_global_janitor_lock()
        .expect("janitor lock release on b");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn test_pg_uuid_mismatch_blocks_consistency_check() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-uuid-mismatch");
    let info = volume.format_info();
    seed_local(&db_path, &info);

    let engine = PgMetaEngine::open(&db_path).expect("open engine");

    // Initial check should pass
    engine
        .check_consistency()
        .expect("initial UUID check must pass");

    // Simulate disk recreation: change UUID in PG for this volume_id
    volume.update_uuid("recreated-uuid-12345");

    // Next check should fail
    let result = engine.check_consistency();
    assert!(result.is_err());
    match result.unwrap_err() {
        MetaError::UuidMismatch { local, remote } => {
            assert_eq!(local, info.uuid);
            assert_eq!(remote, "recreated-uuid-12345");
        }
        e => panic!("expected UuidMismatch, got {:?}", e),
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn janitor_read_only_session_validates_binding_and_lock() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir_a = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_b = studiofs_test_support::workspace_target_tempdir("test-");
    let dir_c = studiofs_test_support::workspace_target_tempdir("test-");
    let db_a = dir_a.path().join("meta-a.redb");
    let db_b = dir_b.path().join("meta-b.redb");
    let db_c = dir_c.path().join("meta-c.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-read-only");
    let info = volume.format_info();
    seed_local(&db_a, &info);
    seed_local(&db_b, &info);

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0)",
                &[&volume.volume_id],
            )
            .await
            .expect("initialize snapshot reachability");
        client
            .execute(
                "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, 7, 77)",
                &[&volume.volume_id],
            )
            .await
            .expect("insert current object reference");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("initialize janitor metadata fixture");

    let original_remote_format = serde_json::to_vec(&info).expect("encode original binding");
    let update_remote_format = |bytes: Vec<u8>| {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            client
                .execute(
                    "UPDATE metadata_state SET format_info = $1 WHERE vol = $2",
                    &[&bytes, &volume_id],
                )
                .await
                .expect("update remote format binding");
            connection_task.abort();
            Ok::<(), MetaError>(())
        })
        .expect("update remote format binding");
    };
    let update_remote_head = |head: i64| {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            client
                .execute(
                    "UPDATE metadata_state SET version = $1 WHERE vol = $2",
                    &[&head, &volume_id],
                )
                .await
                .expect("update remote metadata head");
            connection_task.abort();
            Ok::<(), MetaError>(())
        })
        .expect("update remote metadata head");
    };
    let set_snapshot_bootstrap = |present: bool| {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            if present {
                client
                    .execute(
                        "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0) ON CONFLICT (vol) DO UPDATE SET initialized_version = 0",
                        &[&volume_id],
                    )
                    .await
                    .expect("restore snapshot reachability bootstrap");
            } else {
                client
                    .execute(
                        "DELETE FROM snapshot_reachability_bootstrap WHERE vol = $1",
                        &[&volume_id],
                    )
                    .await
                    .expect("remove snapshot reachability bootstrap");
            }
            connection_task.abort();
            Ok::<(), MetaError>(())
        })
        .expect("update snapshot reachability bootstrap");
    };
    let assert_unsafe = |db_path: &std::path::Path| {
        let mut session = crate::JanitorMetadataSession::open(db_path)
            .expect("open unsafe-state janitor session");
        assert!(
            session
                .try_acquire_global_janitor_lock()
                .expect("acquire unsafe-state janitor lock")
        );
        let error = session
            .collect_protection()
            .expect_err("incomplete janitor state must fail closed");
        assert!(matches!(
            error,
            crate::JanitorMetadataError::UnsafeToDelete(_)
        ));
        session
            .release_global_janitor_lock()
            .expect("release unsafe-state janitor lock");
    };

    let local_bytes_before = std::fs::read(&db_a).expect("read local metadata before janitor");
    let mut first = crate::JanitorMetadataSession::open(&db_a).expect("open first janitor session");
    let mut second =
        crate::JanitorMetadataSession::open(&db_b).expect("open second janitor session");
    assert!(
        first
            .try_acquire_global_janitor_lock()
            .expect("first janitor lock acquisition")
    );
    assert!(
        !second
            .try_acquire_global_janitor_lock()
            .expect("busy janitor lock probe")
    );
    let protection = first
        .collect_protection()
        .expect("read-only janitor protection");
    assert!(protection.referenced.contains(&77));
    first
        .release_global_janitor_lock()
        .expect("release first janitor lock");
    assert!(
        second
            .try_acquire_global_janitor_lock()
            .expect("second janitor lock after release")
    );
    second
        .collect_protection()
        .expect("second read-only protection snapshot");
    second
        .release_global_janitor_lock()
        .expect("release second janitor lock");
    drop(first);
    drop(second);

    set_snapshot_bootstrap(false);
    assert_unsafe(&db_a);
    set_snapshot_bootstrap(true);

    update_remote_head(1);
    assert_unsafe(&db_a);
    update_remote_head(0);

    update_remote_format(Vec::new());
    assert_unsafe(&db_a);
    update_remote_format(b"not-a-format-info".to_vec());
    assert_unsafe(&db_a);
    update_remote_format(original_remote_format.clone());

    for mismatched_info in [
        {
            let mut info = info.clone();
            info.endpoint = "http://different-storage.invalid".to_owned();
            info
        },
        {
            let mut info = info.clone();
            info.bucket = "different-bucket".to_owned();
            info
        },
        {
            let mut info = info.clone();
            info.data_prefix = "different-prefix/".to_owned();
            info
        },
        {
            let mut info = info.clone();
            info.name = "different-volume-name".to_owned();
            info
        },
    ] {
        update_remote_format(serde_json::to_vec(&mismatched_info).expect("encode mismatch"));
        assert_unsafe(&db_a);
    }
    update_remote_format(original_remote_format);

    let mut empty_prefix_local_info = info.clone();
    empty_prefix_local_info.name = "renamed-local-volume".to_owned();
    empty_prefix_local_info.data_prefix.clear();
    seed_local(&db_c, &empty_prefix_local_info);
    let mut empty_prefix_remote_info = info.clone();
    empty_prefix_remote_info.data_prefix.clear();
    update_remote_format(
        serde_json::to_vec(&empty_prefix_remote_info).expect("encode empty-prefix binding"),
    );
    assert_unsafe(&db_c);
    update_remote_format(serde_json::to_vec(&info).expect("restore original binding"));

    volume.update_uuid("janitor-mismatched-uuid");
    assert_unsafe(&db_a);
    volume.update_uuid(&info.uuid);

    let local_before_unsynced_check = std::fs::read(&db_b).expect("read local metadata");
    let local_writer = RedbMetaEngine::open(&db_b).expect("open local metadata writer");
    let next_event_seq = local_writer
        .get_counter(NEXT_EVENT_SEQ)
        .expect("read local event sequence");
    local_writer
        .set_counter(NEXT_EVENT_SEQ, next_event_seq + 1)
        .expect("mark local event unsynced");
    drop(local_writer);
    let local_before_janitor = std::fs::read(&db_b).expect("read unsynced local metadata");
    assert_unsafe(&db_b);
    let local_after_janitor = std::fs::read(&db_b).expect("read unsynced local metadata after");
    assert_eq!(local_before_janitor, local_after_janitor);
    assert_ne!(local_before_unsynced_check, local_before_janitor);

    let local_bytes_after = std::fs::read(&db_a).expect("read local metadata after janitor");
    assert_eq!(local_bytes_before, local_bytes_after);
}
