use super::*;
use crate::snapshot::{SnapshotOrigin, SnapshotRetention};

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
fn test_pg_janitor_blocks_gc_while_snapshot_is_creating() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-snapshot-creation");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open postgres engine");

    assert!(
        !engine
            .snapshot_gc_is_blocked()
            .expect("read snapshot GC state before capture")
    );
    let snapshot = engine
        .create_snapshot_v2(
            SnapshotOrigin::Manual,
            SnapshotRetention::manual(None).expect("manual infinite retention"),
        )
        .expect("create snapshot");
    assert!(
        engine
            .snapshot_gc_is_blocked()
            .expect("read snapshot GC state during capture")
    );

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "UPDATE snapshot_catalog SET lifecycle = 'ready' WHERE vol = $1 AND id = $2",
                &[&volume.volume_id, &snapshot.snapshot_id.to_string()],
            )
            .await
            .expect("finish snapshot for janitor test");
        connection_task.abort();
        Ok(())
    })
    .expect("mark snapshot ready");

    assert!(
        !engine
            .snapshot_gc_is_blocked()
            .expect("read snapshot GC state after capture")
    );
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
