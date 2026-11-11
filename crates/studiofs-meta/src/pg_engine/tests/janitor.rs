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
    // without ledger writes, matching batched eligibility; one batch claims only
    // the untracked and the published, unreferenced object.
    let claimed = session
        .claim_deletions(&[77, 78, 79, 91, 90, 80])
        .expect("claim batch");
    let mut claimed_ids: Vec<u64> = claimed.keys().copied().collect();
    claimed_ids.sort_unstable();
    assert_eq!(claimed_ids, [80, 90]);
    assert_eq!(
        ledger(91),
        None,
        "a protected untracked object keeps no ledger row"
    );
    assert_eq!(ledger(78).expect("uploading row").0, "uploading");
    assert_eq!(ledger(79).expect("uploaded row").0, "uploaded");
    assert!(
        session.claim_deletions(&[80]).expect("claim").is_empty(),
        "an active lease protects the object"
    );
    let finish = |session: &mut crate::JanitorMetadataSession, object_id, token, outcome| {
        session
            .settle_deletions(&[crate::JanitorDeletionSettlement::Finished {
                object_id,
                fencing_token: token,
                outcome,
            }])
            .expect("settle")
            .contains(&object_id)
    };

    // An untracked object is fenced: publication and upload now observe delete_pending.
    let token = claimed[&90];
    assert_eq!(ledger(90).expect("fence row").0, "delete_pending");
    assert!(!finish(&mut session, 90, token + 1, Deleted), "stale token");
    assert!(finish(&mut session, 90, token, Deleted));
    assert_eq!(ledger(90), Some(("delete_pending".to_owned(), None)));
    assert_eq!(
        sql("SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = 90 AND acknowledged_at IS NOT NULL").len(),
        1,
        "a confirmed deletion acknowledges its queue row"
    );
    // Late upload replay against this fence is covered by
    // mounted_deletion_claim_fences_untracked_objects_against_late_replay.
    assert!(
        !finish(&mut session, 90, token, Deleted),
        "a completed claim cannot be finished twice"
    );
    // A duplicate retry after success re-claims and completes idempotently.
    let retry = session.claim_deletions(&[90]).expect("claim")[&90];
    assert!(retry > token);
    assert!(finish(&mut session, 90, retry, Deleted), "finish retry");

    // A retryable outcome keeps the fence with its reason and defers the queue row;
    // the row is re-claimed next cycle, and an expired claim is fenced out.
    assert!(finish(&mut session, 80, claimed[&80], Retryable));
    let (state, reason) = ledger(80).expect("retry row");
    assert_eq!(state, "delete_pending");
    assert!(reason.is_some());
    assert_eq!(
        sql("SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = 80 AND acknowledged_at IS NULL AND next_attempt_at > clock_timestamp()").len(),
        1,
        "a retryable outcome defers its queue row"
    );
    let expired = session.claim_deletions(&[80]).expect("claim")[&80];
    sql(
        "UPDATE snapshot_payload_upload_objects SET reconcile_lease_expires_at = clock_timestamp() - interval '1 second' WHERE vol = $1 AND object_id = 80",
    );
    let current = session.claim_deletions(&[80]).expect("claim")[&80];
    assert!(!finish(&mut session, 80, expired, Deleted));
    assert!(finish(&mut session, 80, current, Deleted), "current finish");

    // A released claim (deletes never started) is re-claimable at once and does not
    // defer its queue row; a queue-only deferral touches no ledger row.
    sql(
        "UPDATE snapshot_payload_gc_candidates SET acknowledged_at = NULL, next_attempt_at = NULL WHERE vol = $1 AND object_id = 80",
    );
    sql("INSERT INTO snapshot_payload_gc_candidates (vol, object_id) VALUES ($1, 77)");
    let released = session.claim_deletions(&[80]).expect("claim")[&80];
    let settled = session
        .settle_deletions(&[
            crate::JanitorDeletionSettlement::Released {
                object_id: 80,
                fencing_token: released,
            },
            crate::JanitorDeletionSettlement::Deferred { object_id: 77 },
        ])
        .expect("settle release and deferral");
    assert_eq!(settled, [80].into_iter().collect());
    let (state, reason) = ledger(80).expect("released row");
    assert_eq!(state, "delete_pending");
    assert!(reason.is_some(), "a released object is not deleted yet");
    assert!(
        sql("SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = 80 AND next_attempt_at > clock_timestamp()").is_empty(),
        "a release must not defer the queue row"
    );
    assert_eq!(
        sql("SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = 77 AND next_attempt_at > clock_timestamp()").len(),
        1,
        "a deferral delays the protected queue row"
    );
    assert_eq!(ledger(77), None, "a deferral writes no ledger row");
    assert!(
        session
            .claim_deletions(&[80])
            .expect("claim")
            .contains_key(&80),
        "a released claim is re-claimable at once"
    );
    session.release_global_janitor_lock().expect("release");
}

/// Write, delete and unmount before publication leaves `uploaded` ledger rows that
/// only the owning client can prove abandoned; its CLI Janitor must retire them.
#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn janitor_session_retires_uploads_its_client_abandoned_before_publication() {
    use crate::JanitorEligibility::{Eligible, Protected};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-abandoned-uploads");
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
    let states = || -> Vec<String> {
        sql("SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id IN (79, 81, 82) ORDER BY object_id")
            .iter()
            .map(|row| row.get(0))
            .collect()
    };

    // The mount marks object 79 deleted locally, acknowledges its journal and unmounts.
    {
        let engine = PgMetaEngine::open_with_options(
            &db_path,
            PgOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open postgres engine");
        engine
            .add_pending_deletions(&[(79, 1), (82, 1)])
            .expect("record local deletion markers");
        engine.flush_metadata().expect("acknowledge local journal");
        engine.prepare_for_close().expect("close postgres engine");
    }
    // 79 is this client's abandoned upload; 81 has no local marker (another client's);
    // 82 is this client's upload cancelled mid-PUT, which nothing ever confirms.
    sql(
        "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 0) ON CONFLICT DO NOTHING",
    );
    sql(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, 79, 'uploaded'), ($1, 81, 'uploaded'), ($1, 82, 'uploading')",
    );

    let mut session = crate::JanitorMetadataSession::open(&db_path).expect("open janitor session");
    assert!(session.try_acquire_global_janitor_lock().expect("lock"));
    session.validate_protection().expect("validate protection");
    let eligibility = session
        .check_eligibility(&[79, 81], i64::MAX)
        .expect("eligibility before retirement");
    assert_eq!((eligibility[&79], eligibility[&81]), (Protected, Protected));

    assert_eq!(session.retire_abandoned_uploads(true).expect("dry run"), 2);
    assert_eq!(
        states(),
        ["uploaded", "uploaded", "uploading"],
        "a dry run only counts"
    );

    assert_eq!(session.retire_abandoned_uploads(false).expect("retire"), 2);
    assert_eq!(states(), ["delete_pending", "uploaded", "delete_pending"]);
    let eligibility = session
        .check_eligibility(&[79, 81], i64::MAX)
        .expect("eligibility after retirement");
    assert!(
        matches!(eligibility[&79], Eligible { .. }),
        "the retired upload is deletable in the same cycle: {:?}",
        eligibility[&79]
    );
    assert_eq!(eligibility[&81], Protected);
    assert_eq!(
        sql("SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = 79 AND eligible_at = to_timestamp(1)").len(),
        1,
        "the grace window counts from the local deletion, not from retirement"
    );
    assert_eq!(session.retire_abandoned_uploads(false).expect("repeat"), 0);
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

    // A PostgreSQL head past the local replica is safe: peers may keep publishing.
    update_remote_head(1);
    let mut behind =
        crate::JanitorMetadataSession::open(&db_a).expect("open lagging local replica");
    assert!(behind.try_acquire_global_janitor_lock().unwrap());
    behind
        .validate_protection()
        .expect("a local replica behind PostgreSQL stays valid");
    behind.release_global_janitor_lock().unwrap();
    drop(behind);
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

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_janitor_sweep_checkpoint_restart_scope_dry_run_and_version() {
    use crate::{JanitorSweepCheckpoint, JanitorSweepScope};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let volume = PgTestVolume::new(&dsn, "pg-janitor-sweep");
    let scope = JanitorSweepScope {
        volume_uuid: &volume.uuid,
        endpoint: "endpoint",
        bucket: "bucket",
        data_prefix: "objects/",
        listing_strategy: "start-after-v1",
    };
    let initial = JanitorSweepCheckpoint {
        after_key: None,
        pending_object_id: None,
        pending_key: None,
    };
    let saved = JanitorSweepCheckpoint {
        after_key: Some("objects/002a/000000000000002a/0".into()),
        pending_object_id: Some(42),
        pending_key: Some("objects/002a/000000000000002a/0".into()),
    };
    let open = || {
        let mut session = crate::pg_engine::PgJanitorSession::new(dsn.clone(), volume.volume_id);
        assert!(session.try_acquire_global_janitor_lock().unwrap());
        session
    };
    let mut session = open();
    assert_eq!(
        session.load_sweep_checkpoint(scope, false).unwrap(),
        initial
    );
    session.save_sweep_checkpoint(scope, &saved).unwrap();
    drop(session);
    let mut session = open();
    assert_eq!(session.load_sweep_checkpoint(scope, false).unwrap(), saved);
    for changed in [
        JanitorSweepScope {
            volume_uuid: "other",
            ..scope
        },
        JanitorSweepScope {
            endpoint: "other",
            ..scope
        },
        JanitorSweepScope {
            bucket: "other",
            ..scope
        },
        JanitorSweepScope {
            data_prefix: "other/",
            ..scope
        },
        JanitorSweepScope {
            listing_strategy: "prefix-trie-v1",
            ..scope
        },
    ] {
        assert_eq!(
            session.load_sweep_checkpoint(changed, false).unwrap(),
            initial
        );
        assert_eq!(
            session.load_sweep_checkpoint(scope, false).unwrap(),
            saved,
            "dry-run must not mutate mismatched scope"
        );
        assert_eq!(
            session.load_sweep_checkpoint(changed, true).unwrap(),
            initial
        );
        assert_eq!(
            session.load_sweep_checkpoint(changed, false).unwrap(),
            initial
        );
        session.save_sweep_checkpoint(scope, &saved).unwrap();
    }
    let execute = |sql: &'static str| {
        run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .unwrap();
            let task = tokio::spawn(connection);
            client.execute(sql, &[&volume.volume_id]).await.unwrap();
            task.abort();
            Ok::<_, MetaError>(())
        })
        .unwrap();
    };
    execute(
        "UPDATE snapshot_payload_sweep_checkpoints SET pending_key = 'objects/invalid' WHERE vol = $1",
    );
    assert!(session.load_sweep_checkpoint(scope, false).is_err());
    execute(
        "UPDATE snapshot_payload_sweep_checkpoints SET pending_key = 'objects/002a/000000000000002a/0', schema_version = 2 WHERE vol = $1",
    );
    assert!(session.load_sweep_checkpoint(scope, false).is_err());
    assert!(session.load_sweep_checkpoint(scope, true).is_err());
    assert!(session.save_sweep_checkpoint(scope, &saved).is_err());
}

/// SFS-980: the payload GC schema deploys atomically and repeatably, and the CLI
/// Janitor refuses any missing, interrupted, drifted or unsupported schema.
/// Runs in a private PostgreSQL schema so dropping guards cannot affect other tests.
#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn janitor_refuses_missing_interrupted_drifted_or_unsupported_gc_schema() {
    use crate::pg_engine::schema::{PAYLOAD_GC_SCHEMA_VERSION, studiofs_pg_schema_sql};
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let isolated_dsn = schema.dsn.clone();
    // A volume row is not needed: only the advisory lock and schema probe run.
    let volume_id = i64::from(rand::random::<u32>()) + 1_000_000_000;

    let sql = |statement: String| {
        let dsn = isolated_dsn.clone();
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?;
            let task = tokio::spawn(connection);
            let result = client.batch_execute(&statement).await.map_err(pg_err);
            task.abort();
            Ok::<_, MetaError>(result)
        })
        .expect("connect to isolated schema")
    };
    let query_i64 = |statement: &'static str| {
        let dsn = isolated_dsn.clone();
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?;
            let task = tokio::spawn(connection);
            let value: i64 = client
                .query_one(statement, &[])
                .await
                .map_err(|error| MetaError::Storage(pg_err(error)))?
                .get(0);
            task.abort();
            Ok::<_, MetaError>(value)
        })
        .expect("query isolated schema")
    };
    let verdict = || {
        let mut session = PgJanitorSession::new(isolated_dsn.clone(), volume_id);
        assert!(
            session
                .try_acquire_global_janitor_lock()
                .expect("acquire lock")
        );
        let verdict = session.verify_payload_gc_schema();
        session.release_global_janitor_lock().expect("release lock");
        verdict
    };
    let refused = |reason: &str| match verdict() {
        Err(crate::JanitorMetadataError::SchemaUnavailable(message)) => assert!(
            message.contains(reason),
            "expected refusal containing {reason:?}, got {message:?}"
        ),
        other => panic!("expected schema refusal containing {reason:?}, got {other:?}"),
    };

    {
        // Empty database: nothing deployed.
        refused("SQLSTATE 42P01");

        // Interrupted application: the final marker insert fails, and the
        // single-transaction batch leaves no partial GC table behind.
        sql("CREATE VIEW studiofs_schema_components AS SELECT ''::TEXT AS component, 0 AS version, now() AS applied_at".to_owned())
            .expect("plant a conflicting object");
        assert!(sql(studiofs_pg_schema_sql().to_owned()).is_err());
        assert_eq!(
            query_i64(
                "SELECT count(*) FROM pg_class WHERE relname = 'snapshot_payload_gc_candidates' AND relnamespace = current_schema()::regnamespace"
            ),
            0
        );
        refused("version is not recorded");
        sql("DROP VIEW studiofs_schema_components".to_owned()).expect("remove the conflict");

        // Retry and repeatable application on empty and populated databases.
        sql(studiofs_pg_schema_sql().to_owned()).expect("first application");
        verdict().expect("deployed schema is accepted");
        sql(format!(
            "INSERT INTO volumes (vol, name, uuid, block_size) VALUES ({volume_id}, 'sfs980', 'sfs980', 8192); \
             INSERT INTO snapshot_payload_gc_candidates (vol, object_id) VALUES ({volume_id}, 7); \
             INSERT INTO snapshot_payload_sweep_checkpoints (vol, schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy, after_key) \
                 VALUES ({volume_id}, 1, 'sfs980', 'e', 'b', 'p/', 's', 'p/0007/0000000000000007')"
        ))
        .expect("populate GC state");
        sql(studiofs_pg_schema_sql().to_owned()).expect("second application");
        verdict().expect("re-applied schema is accepted");
        assert_eq!(
            query_i64("SELECT count(*) FROM snapshot_payload_gc_candidates"),
            1
        );
        assert_eq!(
            query_i64(
                "SELECT count(*) FROM snapshot_payload_sweep_checkpoints WHERE after_key IS NOT NULL"
            ),
            1
        );
        assert_eq!(
            query_i64(
                "SELECT count(*)::BIGINT FROM studiofs_schema_components WHERE component = 'payload_gc' AND version = 1"
            ),
            1
        );

        // Drift: a dropped or disabled guard is refused until re-application.
        sql("DROP TRIGGER studiofs_payload_watermark_delete_guard ON snapshot_payload_upload_watermarks".to_owned())
            .expect("drop a guard");
        refused("incomplete");
        sql(studiofs_pg_schema_sql().to_owned()).expect("repair application");
        verdict().expect("repaired schema is accepted");
        sql(
            "ALTER TABLE event_log DISABLE TRIGGER studiofs_snapshot_source_event_prune_guard"
                .to_owned(),
        )
        .expect("disable a guard");
        refused("incomplete");
        sql(studiofs_pg_schema_sql().to_owned()).expect("re-enabling application");
        verdict().expect("re-created guard is accepted");
        // A replica-only trigger does not fire in a normal session.
        sql(
            "ALTER TABLE event_log ENABLE REPLICA TRIGGER studiofs_snapshot_source_event_prune_guard"
                .to_owned(),
        )
        .expect("make a guard replica-only");
        refused("incomplete");
        sql(studiofs_pg_schema_sql().to_owned()).expect("restoring application");
        verdict().expect("restored guard is accepted");

        // A newer schema is unsupported, and re-application never downgrades it.
        sql(format!(
            "UPDATE studiofs_schema_components SET version = {} WHERE component = 'payload_gc'",
            PAYLOAD_GC_SCHEMA_VERSION + 1
        ))
        .expect("simulate a newer schema");
        refused("version is unsupported");
        sql(studiofs_pg_schema_sql().to_owned()).expect("older application");
        refused("version is unsupported");
    }
}

#[test]
fn search_path_dsn_confines_url_and_key_value_dsns() {
    for dsn in [
        "postgres://user@127.0.0.1:5432/db",
        "postgres://user@127.0.0.1:5432/db?sslmode=disable",
        "host=127.0.0.1 port=5432 user=postgres dbname=db sslmode=disable",
    ] {
        let config: tokio_postgres::Config = search_path_dsn(dsn, "sfs_test_x")
            .parse()
            .unwrap_or_else(|error| panic!("{dsn}: {error}"));
        assert_eq!(
            config.get_options(),
            Some("-csearch_path=sfs_test_x"),
            "{dsn}"
        );
        assert_eq!(config.get_dbname(), Some("db"), "{dsn}");
    }
}
