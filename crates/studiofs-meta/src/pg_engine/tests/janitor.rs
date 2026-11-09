use super::*;
use crate::pg_engine::{PgMetaEngine, PgOpenOptions};
use crate::snapshot::{SnapshotOrigin, SnapshotRetention};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

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
fn pg_janitor_gc_candidate_pages_keyset_through_ten_thousand_rows() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "pg-janitor-gc-candidates");
    let volume_id = volume.volume_id;
    let insert_dsn = dsn.clone();
    run_async(async move {
        let (client, connection) = tokio_postgres::connect(&insert_dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_payload_gc_candidates (vol, object_id, eligible_at) SELECT $1, object_id, clock_timestamp() - interval '1 hour' FROM generate_series(1, 10000) AS object_id",
                &[&volume_id],
            )
            .await
            .expect("insert 10,000 GC candidates");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("seed candidate queue");

    let mut session = crate::pg_engine::janitor::PgJanitorSession::new(dsn, volume_id);
    assert!(
        session
            .try_acquire_global_janitor_lock()
            .expect("acquire lock")
    );
    let mut cursor = None;
    let mut count = 0_usize;
    let mut pages = 0_usize;
    loop {
        let page = session
            .gc_candidate_page(cursor, 64, i64::MAX, true)
            .expect("read bounded candidate page");
        if page.is_empty() {
            break;
        }
        assert!(page.len() <= 64);
        count += page.len();
        pages += 1;
        cursor = page.last().map(|(id, eligible_at)| (*eligible_at, *id));
    }
    assert_eq!(count, 10_000);
    assert_eq!(pages, 157);
    session.release_global_janitor_lock().expect("release lock");
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
fn janitor_session_fences_object_deletion_claims() {
    use crate::snapshot_ports::UploadedObjectDeletionOutcome::{Deleted, Retryable};
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-claims");
    seed_local(&db_path, &volume.format_info());
    let sql = |statement: &'static str| {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let rows = client
                .query(statement, &[&volume_id])
                .await
                .expect("run fixture statement");
            connection_task.abort();
            Ok::<_, MetaError>(rows)
        })
        .expect("run fixture statement")
    };
    let ledger = |object_id: i64| -> Option<(String, Option<String>)> {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let row = client
                .query_opt(
                    "SELECT state, last_error FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
                    &[&volume_id, &object_id],
                )
                .await
                .expect("read ledger row");
            connection_task.abort();
            Ok::<_, MetaError>(row.map(|row| (row.get(0), row.get(1))))
        })
        .expect("read ledger row")
    };
    sql("INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0)");
    sql("INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, 7, 77)");
    sql(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, 78, 'uploading'), ($1, 79, 'uploaded'), ($1, 80, 'published')",
    );
    sql(
        "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, 'claim-ready', 'manual', 0, clock_timestamp(), 'ready')",
    );
    sql(
        "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, 91, 0)",
    );

    let mut session = crate::JanitorMetadataSession::open(&db_path).expect("open janitor session");
    assert!(session.try_acquire_global_janitor_lock().expect("lock"));
    session.validate_protection().expect("validate protection");

    // Referenced, unpublished-upload, and snapshot-pinned objects stay protected
    // without ledger writes, matching batched eligibility.
    for object_id in [77, 78, 79, 91] {
        assert_eq!(
            session.claim_deletion(object_id).expect("claim"),
            None,
            "{object_id}"
        );
    }
    assert_eq!(
        ledger(91),
        None,
        "a protected untracked object keeps no ledger row"
    );
    assert_eq!(ledger(78).expect("uploading row").0, "uploading");
    assert_eq!(ledger(79).expect("uploaded row").0, "uploaded");

    // An untracked object is fenced: publication and upload now observe delete_pending.
    let token = session
        .claim_deletion(90)
        .expect("claim")
        .expect("untracked claim");
    assert_eq!(ledger(90).expect("fence row").0, "delete_pending");
    assert!(matches!(
        session.finish_deletion(90, token + 1, Deleted),
        Err(crate::JanitorMetadataError::StaleClaim)
    ));
    session.finish_deletion(90, token, Deleted).expect("finish");
    assert_eq!(ledger(90), Some(("delete_pending".to_owned(), None)));
    // Late upload replay against this fence is covered by
    // mounted_deletion_claim_fences_untracked_objects_against_late_replay.
    assert!(
        matches!(
            session.finish_deletion(90, token, Deleted),
            Err(crate::JanitorMetadataError::StaleClaim)
        ),
        "a completed claim cannot be finished twice"
    );
    // A duplicate retry after success re-claims and completes idempotently.
    let retry = session
        .claim_deletion(90)
        .expect("claim")
        .expect("re-claim");
    assert!(retry > token);
    session
        .finish_deletion(90, retry, Deleted)
        .expect("finish retry");

    // A retryable outcome keeps the fence with its reason and is re-claimed next
    // cycle; an active lease blocks a second claim, and an expired one is fenced out.
    let first = session
        .claim_deletion(80)
        .expect("claim")
        .expect("claim published, unreferenced 80");
    session
        .finish_deletion(80, first, Retryable)
        .expect("retryable finish");
    let (state, reason) = ledger(80).expect("retry row");
    assert_eq!(state, "delete_pending");
    assert!(reason.is_some());
    let expired = session
        .claim_deletion(80)
        .expect("claim")
        .expect("claim after retry");
    assert_eq!(
        session.claim_deletion(80).expect("claim"),
        None,
        "an active lease protects the object"
    );
    sql(
        "UPDATE snapshot_payload_upload_objects SET reconcile_lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1 AND object_id = 80",
    );
    let current = session
        .claim_deletion(80)
        .expect("claim")
        .expect("claim after expiry");
    assert!(matches!(
        session.finish_deletion(80, expired, Deleted),
        Err(crate::JanitorMetadataError::StaleClaim)
    ));
    session
        .finish_deletion(80, current, Deleted)
        .expect("current finish");
    session.release_global_janitor_lock().expect("release");
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
        client
            .execute(
                "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0) ON CONFLICT (vol) DO UPDATE SET initialized_version = 0",
                &[&volume.volume_id],
            )
            .await
            .expect("initialize snapshot reachability");
        client
            .execute(
                "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, 78, 'uploading'), ($1, 79, 'uploaded'), ($1, 80, 'retry_wait'), ($1, 81, 'delete_pending'), ($1, 82, 'published')",
                &[&volume.volume_id],
            )
            .await
            .expect("insert upload ledger fixtures");
        client
            .execute(
                "UPDATE snapshot_payload_upload_objects SET reconcile_owner = 'janitor-test', reconcile_lease_expires_at = clock_timestamp() + interval '60 seconds' WHERE vol = $1 AND object_id = 82",
                &[&volume.volume_id],
            )
            .await
            .expect("insert active reconciliation lease");
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
            .validate_protection()
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
    first
        .validate_protection()
        .expect("read-only janitor preflight");
    let protection = first
        .check_eligibility(&[77, 78, 79, 80, 81, 82, 83], i64::MAX)
        .expect("bounded janitor protection");
    for object_id in [77, 78, 79, 80, 82] {
        assert_eq!(
            protection.get(&object_id),
            Some(&crate::JanitorEligibility::Protected),
            "object {object_id}"
        );
    }
    for object_id in [81, 83] {
        assert_eq!(
            protection.get(&object_id),
            Some(&crate::JanitorEligibility::Eligible {
                pending_deletion: None
            }),
            "object {object_id}"
        );
    }

    for (snapshot_id, lifecycle, object_id) in [
        ("janitor-creating", "creating", 84),
        ("janitor-ready", "ready", 85),
        ("janitor-deleting", "deleting", 86),
    ] {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to snapshot protection fixture");
            let connection_task = tokio::spawn(connection);
            client
                .execute(
                    "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, $2, 'manual', 0, clock_timestamp(), $3)",
                    &[&volume.volume_id, &snapshot_id, &lifecycle],
                )
                .await
                .expect("insert snapshot catalog row");
            client
                .execute(
                    "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, $2, 0)",
                    &[&volume.volume_id, &i64::try_from(object_id).expect("valid fixture object ID")],
                )
                .await
                .expect("insert snapshot reachability row");
            connection_task.abort();
            Ok::<(), MetaError>(())
        })
        .expect("seed snapshot reachability protection");

        if lifecycle == "deleting" {
            let eligible = first
                .check_eligibility(&[object_id], i64::MAX)
                .expect("unleased deleting snapshot does not pin objects");
            assert_eq!(
                eligible.get(&object_id),
                Some(&crate::JanitorEligibility::Eligible {
                    pending_deletion: None
                })
            );
            run_async(async {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .expect("connect to insert active snapshot lease");
                let connection_task = tokio::spawn(connection);
                client
                    .execute(
                        "INSERT INTO snapshot_mount_leases (vol, snapshot_id, owner, cache_volume_uuid, cache_sha256, lease_expires_at, heartbeat_at) VALUES ($1, $2, 'janitor-test', $3, $4, clock_timestamp() + interval '60 seconds', clock_timestamp())",
                        &[&volume.volume_id, &snapshot_id, &volume.uuid, &vec![0u8; 32]],
                    )
                    .await
                    .expect("insert active snapshot mount lease");
                connection_task.abort();
                Ok::<(), MetaError>(())
            })
            .expect("seed live snapshot mount lease");
        } else {
            let protected = first
                .check_eligibility(&[object_id], i64::MAX)
                .expect("creating or ready snapshot pin query");
            assert_eq!(
                protected.get(&object_id),
                Some(&crate::JanitorEligibility::Protected)
            );
        }

        if lifecycle != "deleting" {
            run_async(async {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .expect("connect to retire snapshot fixture");
                let connection_task = tokio::spawn(connection);
                client
                    .execute(
                        "UPDATE snapshot_catalog SET lifecycle = 'deleted' WHERE vol = $1 AND id = $2",
                        &[&volume.volume_id, &snapshot_id],
                    )
                    .await
                    .expect("retire snapshot fixture");
                connection_task.abort();
                Ok::<(), MetaError>(())
            })
            .expect("retire snapshot fixture");
        }
    }

    let leased = first
        .check_eligibility(&[86], i64::MAX)
        .expect("live mount lease retains deleting snapshot reachability");
    assert_eq!(leased.get(&86), Some(&crate::JanitorEligibility::Protected));
    first
        .release_global_janitor_lock()
        .expect("release first janitor lock");
    assert!(
        second
            .try_acquire_global_janitor_lock()
            .expect("second janitor lock after release")
    );
    second
        .validate_protection()
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

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn snapshot_creation_waits_for_the_standalone_janitor_lock() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let engine_dir = studiofs_test_support::workspace_target_tempdir("snapshot-janitor-engine-");
    let janitor_dir = studiofs_test_support::workspace_target_tempdir("snapshot-janitor-lock-");
    let engine_db = engine_dir.path().join("meta.redb");
    let janitor_db = janitor_dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "snapshot-janitor-lock");
    let info = volume.format_info();
    seed_local(&engine_db, &info);
    seed_local(&janitor_db, &info);
    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to seed snapshot reachability");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0) ON CONFLICT (vol) DO UPDATE SET initialized_version = 0",
                &[&volume.volume_id],
            )
            .await
            .expect("initialize snapshot reachability");
        client
            .execute(
                "INSERT INTO snapshot_object_reachability (vol, object_id, start_version) VALUES ($1, 90, 0)",
                &[&volume.volume_id],
            )
            .await
            .expect("seed snapshot candidate reachability");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("seed snapshot reachability");
    let engine = PgMetaEngine::open_with_options(
        &engine_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open snapshot engine");
    let mut janitor =
        crate::JanitorMetadataSession::open(&janitor_db).expect("open janitor metadata session");
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("acquire janitor lock")
    );

    let snapshot_engine = std::sync::Arc::clone(&engine);
    let volume_id = volume.volume_id;
    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let snapshot_thread = thread::spawn(move || {
        started_tx.send(()).expect("signal snapshot call start");
        let result = snapshot_engine
            .create_snapshot_v2(
                SnapshotOrigin::Manual,
                SnapshotRetention::manual(None).expect("manual retention"),
            )
            .map(|entry| entry.snapshot_id)
            .map_err(|error| error.to_string());
        result_tx.send(result).expect("send snapshot result");
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("snapshot thread started");

    let lock_key = crate::pg_engine::pg_janitor_lock_key(volume_id) as u64;
    let lock_classid = i64::from((lock_key >> 32) as u32);
    let lock_objid = i64::from(lock_key as u32);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut observed_waiter = false;
    while Instant::now() < deadline {
        observed_waiter = run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?;
            let connection_task = tokio::spawn(connection);
            let waiting = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1 AND mode = 'ExclusiveLock' AND NOT granted)",
                    &[&lock_classid, &lock_objid],
                )
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?
                .get(0);
            connection_task.abort();
            Ok(waiting)
        })
        .expect("observe waiting snapshot lock");
        if observed_waiter {
            break;
        }
        thread::yield_now();
    }

    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");
    let result = result_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("snapshot completes after janitor lock release");
    snapshot_thread.join().expect("join snapshot thread");

    assert!(
        observed_waiter,
        "snapshot registration never waited for the janitor lock"
    );
    let snapshot_id = result.expect("snapshot registration succeeds after janitor lock release");
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("reacquire janitor lock after snapshot commit")
    );
    janitor
        .validate_protection()
        .expect("validate metadata after snapshot registration");
    let cli_runtime = tokio::runtime::Runtime::new().expect("build CLI-like runtime");
    let protection = cli_runtime.block_on(async {
        tokio::task::block_in_place(|| janitor.check_eligibility(&[90], i64::MAX))
    });
    assert_eq!(
        protection
            .expect("run real janitor eligibility under CLI runtime")
            .get(&90),
        Some(&crate::JanitorEligibility::Protected),
        "a committed creating snapshot must retain its reachable object"
    );

    // Interleave a real snapshot deletion while the janitor still holds its lock.
    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to insert active snapshot lease");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_mount_leases (vol, snapshot_id, owner, cache_volume_uuid, cache_sha256, lease_expires_at, heartbeat_at) VALUES ($1, $2, 'janitor-test', $3, $4, clock_timestamp() + interval '60 seconds', clock_timestamp())",
                &[&volume_id, &snapshot_id.as_str(), &info.uuid, &vec![0u8; 32]],
            )
            .await
            .expect("insert active snapshot mount lease");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("seed live snapshot mount lease");
    let deleting = engine
        .delete_snapshot_v2(&snapshot_id)
        .expect("snapshot deletion proceeds while the janitor lock is held");
    assert_eq!(
        deleting.lifecycle,
        crate::snapshot::SnapshotLifecycle::Deleting
    );
    let protection = cli_runtime.block_on(async {
        tokio::task::block_in_place(|| janitor.check_eligibility(&[90], i64::MAX))
    });
    assert_eq!(
        protection
            .expect("run janitor eligibility after interleaved deletion")
            .get(&90),
        Some(&crate::JanitorEligibility::Protected),
        "a deleting snapshot with a live mount lease must retain its reachable object"
    );
    drop(cli_runtime);
    janitor
        .release_global_janitor_lock()
        .expect("release reacquired janitor lock");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn concurrent_snapshot_creations_released_by_the_janitor_do_not_deadlock() {
    let Some(dsn) = pg_dsn() else {
        return;
    };

    let engine_dir = studiofs_test_support::workspace_target_tempdir("snapshot-pair-engine-");
    let janitor_dir = studiofs_test_support::workspace_target_tempdir("snapshot-pair-lock-");
    let engine_db = engine_dir.path().join("meta.redb");
    let janitor_db = janitor_dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "snapshot-pair-lock");
    let info = volume.format_info();
    seed_local(&engine_db, &info);
    seed_local(&janitor_db, &info);
    let engine = PgMetaEngine::open_with_options(
        &engine_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open snapshot engine");
    let mut janitor =
        crate::JanitorMetadataSession::open(&janitor_db).expect("open janitor metadata session");
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("acquire janitor lock")
    );

    let (result_tx, result_rx) = mpsc::channel();
    let creators: Vec<_> = (0..2)
        .map(|_| {
            let engine = std::sync::Arc::clone(&engine);
            let result_tx = result_tx.clone();
            thread::spawn(move || {
                let result = engine
                    .create_snapshot_v2(
                        SnapshotOrigin::Manual,
                        SnapshotRetention::manual(None).expect("manual retention"),
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string());
                result_tx.send(result).expect("send snapshot result");
            })
        })
        .collect();

    // Release only after both captures queue on the janitor key, so they contend on release.
    let lock_key = crate::pg_engine::pg_janitor_lock_key(volume.volume_id) as u64;
    let lock_classid = i64::from((lock_key >> 32) as u32);
    let lock_objid = i64::from(lock_key as u32);
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut waiters = 0_i64;
    while Instant::now() < deadline && waiters < 2 {
        waiters = run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?;
            let connection_task = tokio::spawn(connection);
            let waiting: i64 = client
                .query_one(
                    "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2 AND objsubid = 1 AND NOT granted",
                    &[&lock_classid, &lock_objid],
                )
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?
                .get(0);
            connection_task.abort();
            Ok(waiting)
        })
        .expect("observe waiting snapshot captures");
        thread::yield_now();
    }
    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");
    assert_eq!(
        waiters, 2,
        "both snapshot captures must queue on the janitor lock"
    );

    for _ in 0..2 {
        result_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("snapshot capture completes after janitor lock release")
            .expect("concurrent snapshot capture succeeds without deadlock");
    }
    for creator in creators {
        creator.join().expect("join snapshot creator");
    }
}
