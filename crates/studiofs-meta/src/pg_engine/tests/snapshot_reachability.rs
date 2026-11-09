use super::*;
use crate::events::MetaEvent;
use crate::pg_engine::snapshot_reachability::apply_object_lifetime_changes;
use crate::schema::{
    ChunkId, PayloadRef, PayloadRefState, SliceId, SliceOrder, SliceRange, SliceRecord,
};
use crate::snapshot_ports::{
    UploadedFragmentLedger, UploadedFragmentLedgerEntry, UploadedFragmentReconciliationOutcome,
};
use chrono::Utc;
use std::collections::HashSet;

fn published_slice(object_id: u64) -> SliceRecord {
    SliceRecord {
        slice_id: SliceId(1),
        order: SliceOrder(1),
        range: SliceRange::new(0, 1).expect("valid slice range"),
        payload: Some(PayloadRef::new_full(object_id, 1)),
        payload_state: PayloadRefState::Published,
    }
}

fn publish_event(ino: u64, object_id: u64) -> MetaEvent {
    MetaEvent::PublishRegularFileBaseSlices {
        ino,
        chunk_slices: vec![(ChunkId(0), vec![published_slice(object_id)])],
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn publication_rejects_deleted_payload_object() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-deleted-fence");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, 11, 'deleted')",
                &[&volume.volume_id],
            )
            .await
            .expect("insert deleted upload object");

        let tx = client
            .transaction()
            .await
            .expect("start deleted publication transaction");
        let error = apply_object_lifetime_changes(&tx, volume.volume_id, 1, &[publish_event(7, 11)])
            .await
            .expect_err("publication must reject a deleted payload object");
        assert!(error
            .to_string()
            .contains("after deletion has started"));
        tx.rollback().await.expect("rollback rejected publication");

        // A failed deletion finished by an older mounted Janitor stays fenced, while
        // an upload awaiting reconciliation retry remains publishable.
        client
            .execute(
                "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, last_error) VALUES ($1, 12, 'retry_wait', 'remote deletion retry required'), ($1, 13, 'retry_wait', 'remote proof retry required')",
                &[&volume.volume_id],
            )
            .await
            .expect("insert retry_wait upload objects");
        let tx = client.transaction().await.expect("start legacy retry publication");
        apply_object_lifetime_changes(&tx, volume.volume_id, 1, &[publish_event(7, 12)])
            .await
            .expect_err("publication must reject a legacy deletion retry row");
        tx.rollback().await.expect("rollback rejected legacy retry publication");
        let tx = client.transaction().await.expect("start upload retry publication");
        apply_object_lifetime_changes(&tx, volume.volume_id, 1, &[publish_event(8, 13)])
            .await
            .expect("an upload retry row stays publishable");
        tx.rollback().await.expect("rollback upload retry publication");

        let references = client
            .query(
                "SELECT object_id FROM snapshot_current_references WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .expect("read references after rejected publication");
        assert!(references.is_empty());

        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("deleted publication fence transaction");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn mounted_deletion_claim_fences_untracked_objects_against_late_replay() {
    use crate::snapshot_ports::UploadedObjectDeletionClaim::{Claimed, Protected};
    use crate::snapshot_ports::UploadedObjectDeletionOutcome::{Deleted, Retryable};
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-mounted-claim");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");
    let sql = |statement: &'static str| {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            client
                .execute(statement, &[&volume_id])
                .await
                .expect("run fixture statement");
            connection_task.abort();
            Ok::<_, MetaError>(())
        })
        .expect("run fixture statement");
    };
    let state = |object_id: i64| -> Option<String> {
        let dsn = dsn.clone();
        let volume_id = volume.volume_id;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let row = client
                .query_opt(
                    "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
                    &[&volume_id, &object_id],
                )
                .await
                .expect("read ledger row");
            connection_task.abort();
            Ok::<_, MetaError>(row.map(|row| row.get(0)))
        })
        .expect("read ledger row")
    };
    let replay_upload = |object_id: u64| {
        let entry = UploadedFragmentLedgerEntry {
            volume_id: volume.volume_id,
            object_id,
            fragment_index: 0,
            object_key: format!("objects/{object_id}/0"),
            expected_byte_length: 1,
            expected_sha256: [7; 32],
        };
        engine
            .begin_fragment_uploads(std::slice::from_ref(&entry))
            .expect("late upload registration");
        engine
            .confirm_fragment_uploads(std::slice::from_ref(&entry))
            .expect("late upload confirmation");
        engine.flush_metadata().expect("replay upload ledger");
    };
    let publication_rejected = |object_id: u64| {
        run_async(async {
            let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let tx = client.transaction().await.expect("start publication");
            let result = apply_object_lifetime_changes(
                &tx,
                volume.volume_id,
                1,
                &[publish_event(7, object_id)],
            )
            .await;
            tx.rollback().await.expect("rollback publication");
            connection_task.abort();
            Ok::<_, MetaError>(result.is_err())
        })
        .expect("attempt publication")
    };

    sql(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, 61, 'uploaded'), ($1, 62, 'retry_wait'), ($1, 63, 'deleted')",
    );
    // Uploads awaiting publication are protected, as in batched eligibility.
    for object_id in [61, 62] {
        assert!(matches!(
            engine
                .claim_uploaded_object_deletion(object_id, "mounted-test")
                .expect("claim"),
            Protected
        ));
    }

    // An untracked object is claimed only with a delete_pending fence row.
    let Claimed { fencing_token } = engine
        .claim_uploaded_object_deletion(60, "mounted-test")
        .expect("claim untracked")
    else {
        panic!("an unreferenced untracked object must be claimed under a fence");
    };
    assert_eq!(state(60).as_deref(), Some("delete_pending"));
    engine
        .finish_uploaded_object_deletion(60, "mounted-test", fencing_token, Retryable)
        .expect("retryable finish");
    assert_eq!(state(60).as_deref(), Some("delete_pending"));
    let Claimed { fencing_token } = engine
        .claim_uploaded_object_deletion(60, "mounted-test")
        .expect("re-claim after retry")
    else {
        panic!("a retryable fence row must be re-claimable");
    };
    engine
        .finish_uploaded_object_deletion(60, "mounted-test", fencing_token, Deleted)
        .expect("finish");
    assert_eq!(state(60).as_deref(), Some("delete_pending"));

    // A late ledger replay neither revives the fence nor a legacy 'deleted' row.
    for (object_id, fence) in [(60, "delete_pending"), (63, "deleted")] {
        replay_upload(object_id);
        assert_eq!(state(object_id as i64).as_deref(), Some(fence));
        assert!(
            publication_rejected(object_id),
            "a deleted object id must not be published"
        );
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pruned_deletion_fences_stay_fenced_below_the_payload_watermark() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-fence-prune");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");
    let volume_id = volume.volume_id;
    let sql = |statement: String| {
        let dsn = dsn.clone();
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            client
                .execute(&statement, &[&volume_id])
                .await
                .expect("run fixture statement");
            connection_task.abort();
            Ok::<_, MetaError>(())
        })
        .expect("run fixture statement");
    };
    let state = |object_id: u64| -> Option<String> {
        let dsn = dsn.clone();
        let object_id = object_id as i64;
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let row = client
                .query_opt(
                    "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
                    &[&volume_id, &object_id],
                )
                .await
                .expect("read ledger row");
            connection_task.abort();
            Ok::<_, MetaError>(row.map(|row| row.get(0)))
        })
        .expect("read ledger row")
    };
    let replay_upload = |object_id: u64| {
        let entry = UploadedFragmentLedgerEntry {
            volume_id,
            object_id,
            fragment_index: 0,
            object_key: format!("objects/{object_id}/0"),
            expected_byte_length: 1,
            expected_sha256: [7; 32],
        };
        engine
            .begin_fragment_uploads(std::slice::from_ref(&entry))
            .expect("late upload registration");
        engine.flush_metadata().expect("replay upload ledger");
    };
    let publication_rejected = |object_id: u64| {
        run_async(async {
            let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let tx = client.transaction().await.expect("start publication");
            let result =
                apply_object_lifetime_changes(&tx, volume_id, 1, &[publish_event(7, object_id)])
                    .await;
            tx.rollback().await.expect("rollback publication");
            connection_task.abort();
            Ok::<_, MetaError>(result.is_err())
        })
        .expect("attempt publication")
    };
    // Payload namespace 5: object ID = 5 << 39 | counter.
    let id = |counter: u64| (5_u64 << 39) | counter;
    let (finished, deleted, failed, leased, uploading, referenced, above) =
        (id(10), id(12), id(14), id(16), id(18), id(20), id(30));
    let legacy = 77_u64;
    sql(format!(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, closed_at, last_error, reconcile_owner) VALUES \
         ($1, {finished}, 'delete_pending', now(), NULL, NULL), ($1, {deleted}, 'deleted', now(), NULL, NULL), \
         ($1, {failed}, 'delete_pending', now(), 'retry', NULL), ($1, {leased}, 'delete_pending', now(), NULL, 'janitor'), \
         ($1, {uploading}, 'uploading', NULL, NULL, NULL), ($1, {above}, 'delete_pending', now(), NULL, NULL), \
         ($1, {legacy}, 'delete_pending', now(), NULL, NULL)"
    ));
    sql(format!(
        "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, 8, {referenced})"
    ));

    let mut janitor = crate::pg_engine::PgJanitorSession::new(dsn.clone(), volume_id);
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("janitor lock")
    );
    assert_eq!(
        janitor
            .prune_object_deletion_fences()
            .expect("prune without watermark"),
        0,
        "no fence is pruned before its namespace reports a watermark"
    );
    sql("INSERT INTO snapshot_payload_upload_watermarks (vol, payload_namespace, acked_through) VALUES ($1, 5, 25)".to_owned());
    // Lowering or deleting the watermark would unfence pruned IDs.
    for statement in [
        "UPDATE snapshot_payload_upload_watermarks SET acked_through = 24 WHERE vol = $1",
        "DELETE FROM snapshot_payload_upload_watermarks WHERE vol = $1",
    ] {
        let error = run_async(async {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let result = client.execute(statement, &[&volume_id]).await;
            connection_task.abort();
            Ok::<_, MetaError>(result)
        })
        .expect("attempt watermark regression")
        .expect_err("a watermark must never move backwards");
        assert_eq!(
            error.code(),
            Some(&tokio_postgres::error::SqlState::RESTRICT_VIOLATION),
            "{statement}"
        );
    }
    assert_eq!(janitor.prune_object_deletion_fences().expect("prune"), 2);
    assert_eq!(state(finished), None);
    assert_eq!(state(deleted), None);
    // Unfinished, leased, live, above-watermark, and legacy rows stay.
    assert_eq!(state(failed).as_deref(), Some("delete_pending"));
    assert_eq!(state(leased).as_deref(), Some("delete_pending"));
    assert_eq!(state(uploading).as_deref(), Some("uploading"));
    assert_eq!(state(above).as_deref(), Some("delete_pending"));
    assert_eq!(state(legacy).as_deref(), Some("delete_pending"));

    // After pruning, publishing the deleted ID without a ledger row is rejected.
    assert!(publication_rejected(finished));
    // A late ledger replay re-creates the fence instead of reopening publication.
    replay_upload(finished);
    assert_eq!(state(finished).as_deref(), Some("delete_pending"));
    assert!(publication_rejected(finished));
    assert_eq!(janitor.prune_object_deletion_fences().expect("re-prune"), 1);
    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");

    // Live IDs are unaffected: a currently referenced row-less ID below the
    // watermark and fresh IDs above it still publish and register normally.
    assert!(!publication_rejected(referenced));
    assert!(!publication_rejected(id(40)));
    replay_upload(id(41));
    assert_eq!(state(id(41)).as_deref(), Some("uploading"));

    // The delete guard still lets the volume's own cascade remove its watermark.
    drop(engine);
    sql("DELETE FROM volumes WHERE vol = $1".to_owned());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn published_batch_derives_exact_before_and_after_references() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-interval");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);

        let tx = client.transaction().await.expect("start batch transaction");
        apply_object_lifetime_changes(&tx, volume.volume_id, 1, &[publish_event(7, 11)])
            .await
            .expect("open first object lifetime");
        tx.commit().await.expect("commit first batch");

        let tx = client.transaction().await.expect("start overwrite transaction");
        apply_object_lifetime_changes(&tx, volume.volume_id, 2, &[publish_event(7, 12)])
            .await
            .expect("replace object lifetime");
        tx.commit().await.expect("commit overwrite batch");

        let rows = client
            .query(
                "SELECT object_id, start_version, end_version FROM snapshot_object_reachability WHERE vol = $1 ORDER BY object_id",
                &[&volume.volume_id],
            )
            .await
            .expect("read lifetime intervals");
        let intervals = rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, i64>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, Option<i64>>(2),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(intervals, vec![(11, 1, Some(2)), (12, 2, None)]);

        let refs = client
            .query(
                "SELECT object_id FROM snapshot_current_references WHERE vol = $1 AND ino = 7",
                &[&volume.volume_id],
            )
            .await
            .expect("read current references");
        assert_eq!(refs[0].get::<_, i64>(0), 12);

        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("reachability transaction");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn retry_and_final_reference_are_idempotent() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-retry");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);

        for (batch_version, event) in [
            (1, publish_event(7, 21)),
            (2, publish_event(8, 21)),
            (3, MetaEvent::RemoveRegularFileGraph { ino: 7 }),
            (4, MetaEvent::RemoveRegularFileGraph { ino: 8 }),
        ] {
            let tx = client.transaction().await.expect("start lifetime transaction");
            apply_object_lifetime_changes(&tx, volume.volume_id, batch_version, &[event])
                .await
                .expect("apply lifetime batch");
            tx.commit().await.expect("commit lifetime batch");
        }

        let tx = client.transaction().await.expect("start replay transaction");
        apply_object_lifetime_changes(&tx, volume.volume_id, 4, &[MetaEvent::RemoveRegularFileGraph { ino: 8 }])
            .await
            .expect("replay already applied batch");
        tx.commit().await.expect("commit replay transaction");

        let interval_rows = client
            .query(
                "SELECT start_version, end_version FROM snapshot_object_reachability WHERE vol = $1 AND object_id = 21",
                &[&volume.volume_id],
            )
            .await
            .expect("read shared-object lifetime");
        assert_eq!(interval_rows.len(), 1);
        assert_eq!(interval_rows[0].get::<_, i64>(0), 1);
        assert_eq!(interval_rows[0].get::<_, Option<i64>>(1), Some(4));

        let current_rows = client
            .query(
                "SELECT object_id FROM snapshot_current_references WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .expect("read final current references");
        assert!(current_rows.is_empty());

        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("idempotent reachability transaction");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn bootstrap_defers_while_a_snapshot_is_creating() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-bootstrap-creating");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, 'snapshot-creating', 'manual', 1, $2, 'creating')",
                &[&volume.volume_id, &Utc::now()],
            )
            .await
            .expect("insert creating snapshot");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("insert creating snapshot");

    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");
    engine
        .sync_metadata()
        .expect("metadata sync must remain available while reachability bootstrap is deferred");
    let error = engine
        .get_global_snapshot_pins()
        .expect_err("janitor pins must stay unavailable until bootstrap completes");
    assert!(
        error
            .to_string()
            .contains("snapshot reachability bootstrap incomplete")
    );
    drop(engine);
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn bootstrap_defers_until_local_metadata_catches_up() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-bootstrap-lag");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open lagging postgres engine");

    let publisher_dir = studiofs_test_support::workspace_target_tempdir("test-");
    let publisher_db = publisher_dir.path().join("publisher.redb");
    seed_local(&publisher_db, &volume.format_info());
    let publisher = PgMetaEngine::open_with_options(
        &publisher_db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open publisher postgres engine");
    publisher
        .create_dir(ROOT_INO, b"bootstrap-lag", 0, 0, 0o755)
        .expect("create remote event");
    publisher.flush_metadata().expect("publish remote event");
    drop(publisher);
    drop(publisher_dir);

    engine
        .backfill_current_reference_ledger_if_needed()
        .expect("version mismatch should defer bootstrap without failing");
    assert!(
        !engine
            .snapshot_reachability_bootstrapped
            .load(std::sync::atomic::Ordering::Acquire),
        "bootstrap must stay pending while local metadata trails PostgreSQL"
    );

    engine
        .sync_metadata()
        .expect("metadata sync should catch up the local engine");
    assert!(
        engine
            .snapshot_reachability_bootstrapped
            .load(std::sync::atomic::Ordering::Acquire),
        "bootstrap must complete after local metadata catches up"
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn bootstrap_batches_large_reference_ledger() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-bootstrap-batches");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");

    for object_index in 0..1_025_u64 {
        let name = format!("bootstrap-batch-{object_index}");
        create_published_small_file(&engine, ROOT_INO, name.as_bytes(), 100_000 + object_index);
    }

    engine
        .backfill_current_reference_ledger_if_needed()
        .expect("large reachability bootstrap should complete in batches");
    assert!(
        engine
            .snapshot_reachability_bootstrapped
            .load(std::sync::atomic::Ordering::Acquire),
        "large reachability bootstrap must publish its completion marker"
    );

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let current_reference_count = client
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM snapshot_current_references WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .expect("count bootstrapped current references")
            .get::<_, i64>(0);
        let reachability_count = client
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM snapshot_object_reachability WHERE vol = $1 AND start_version = 0",
                &[&volume.volume_id],
            )
            .await
            .expect("count bootstrapped reachability rows")
            .get::<_, i64>(0);
        connection_task.abort();
        let _ = connection_task.await;
        assert_eq!(current_reference_count, 1_025);
        assert_eq!(reachability_count, 1_025);
        Ok::<(), MetaError>(())
    })
    .expect("verify batched reachability bootstrap");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn snapshot_cut_and_mount_lease_control_object_protection() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-protection");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let tx = client.transaction().await.expect("start reference transaction");
        apply_object_lifetime_changes(&tx, volume.volume_id, 1, &[publish_event(7, 31)])
            .await
            .expect("open protected object lifetime");
        tx.commit().await.expect("commit reference transaction");

        client
            .execute(
                "INSERT INTO snapshot_reachability_bootstrap (vol, initialized_version) VALUES ($1, 1) ON CONFLICT DO NOTHING",
                &[&volume.volume_id],
            )
            .await
            .expect("insert reachability bootstrap marker");
        client
            .execute(
                "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, 'snapshot-protection', 'manual', 1, $2, 'creating')",
                &[&volume.volume_id, &Utc::now()],
            )
            .await
            .expect("insert creating snapshot");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("insert creating snapshot");

    assert_eq!(
        engine
            .get_global_snapshot_pins()
            .expect("load creating snapshot protections"),
        HashSet::from([31])
    );

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to update snapshot");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "UPDATE snapshot_catalog SET lifecycle = 'deleting' WHERE vol = $1 AND id = 'snapshot-protection'",
                &[&volume.volume_id],
            )
            .await
            .expect("move snapshot to deleting");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("update snapshot lifecycle");
    assert!(
        engine
            .get_global_snapshot_pins()
            .expect("load deleting snapshot protections")
            .is_empty()
    );

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to lease update");
        let connection_task = tokio::spawn(connection);
        client
            .execute(
                "INSERT INTO snapshot_mount_leases (vol, snapshot_id, owner, cache_volume_uuid, cache_sha256, lease_expires_at, heartbeat_at) VALUES ($1, 'snapshot-protection', 'test-owner', $2, $3, clock_timestamp() + interval '60 seconds', clock_timestamp())",
                &[&volume.volume_id, &volume.uuid, &vec![0u8; 32]],
            )
            .await
            .expect("insert active mount lease");
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("insert active mount lease");
    assert_eq!(
        engine
            .get_global_snapshot_pins()
            .expect("load leased deleting snapshot protections"),
        HashSet::from([31])
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn uploaded_fragment_reconciliation_is_bounded_and_leased() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-reconcile");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        for object_id in 1_i64..=261 {
            let object_state = if object_id == 261 {
                "uploaded"
            } else {
                "uploading"
            };
            let confirmation_state = if object_id == 1 {
                "pending"
            } else {
                "confirmed"
            };
            client
                .execute(
                    "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, $3)",
                    &[&volume.volume_id, &object_id, &object_state],
                )
                .await
                .expect("insert upload object");
            client
                .execute(
                    "INSERT INTO snapshot_payload_upload_fragments (vol, object_id, fragment_index, object_key, expected_byte_length, expected_sha256, uploaded_at, confirmed_at, confirmation_state) VALUES ($1, $2, 0, $3, 0, $4, clock_timestamp(), clock_timestamp(), $5)",
                    &[
                        &volume.volume_id,
                        &object_id,
                        &format!("objects/{object_id}/0"),
                        &vec![0_u8; 32],
                        &confirmation_state,
                    ],
                )
                .await
                .expect("insert upload fragment");
            if object_id == 1 {
                client
                    .execute(
                        "INSERT INTO snapshot_payload_upload_fragments (vol, object_id, fragment_index, object_key, expected_byte_length, expected_sha256, uploaded_at, confirmed_at, confirmation_state) VALUES ($1, $2, 1, $3, 0, $4, clock_timestamp(), NULL, 'pending')",
                        &[
                            &volume.volume_id,
                            &object_id,
                            &format!("objects/{object_id}/1"),
                            &vec![0_u8; 32],
                        ],
                    )
                    .await
                    .expect("insert second upload fragment");
            }
        }
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("insert upload reconciliation rows");

    let claims = engine
        .claim_uploaded_fragment_reconciliation("test-janitor")
        .expect("claim upload reconciliation page");
    assert_eq!(claims.len(), 256);
    assert!(claims.iter().all(|claim| claim.fencing_token > 0));
    assert_eq!(claims[0].entry.object_id, 1);
    assert_eq!(claims[1].entry.object_id, 1);
    assert_eq!(claims[0].fencing_token, claims[1].fencing_token);
    assert_eq!(claims.first().map(|claim| claim.entry.object_id), Some(1));
    assert_eq!(claims.last().map(|claim| claim.entry.object_id), Some(255));

    engine
        .finish_uploaded_fragment_reconciliation(
            "test-janitor",
            &claims[0],
            UploadedFragmentReconciliationOutcome::RemoteConfirmed,
        )
        .expect("finish first fragment of one object");
    engine
        .finish_uploaded_fragment_reconciliation(
            "test-janitor",
            &claims[1],
            UploadedFragmentReconciliationOutcome::RemoteConfirmed,
        )
        .expect("finish second fragment of one object");

    let tail_claims = engine
        .claim_uploaded_fragment_reconciliation("test-janitor")
        .expect("claim cursor tail");
    assert_eq!(tail_claims.len(), 5);
    assert_eq!(
        tail_claims.first().map(|claim| claim.entry.object_id),
        Some(256)
    );
    assert_eq!(
        tail_claims.last().map(|claim| claim.entry.object_id),
        Some(260)
    );

    let no_claim_after_cursor_wrap = engine
        .claim_uploaded_fragment_reconciliation("test-janitor")
        .expect("respect active reconciliation leases after cursor wrap");
    assert!(no_claim_after_cursor_wrap.is_empty());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn uploaded_fragment_ledger_batches_preserve_recovery_rows() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-upload-batch");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open postgres engine");

    let entries = (0..3)
        .map(|fragment_index| UploadedFragmentLedgerEntry {
            volume_id: volume.volume_id,
            object_id: 41,
            fragment_index,
            object_key: format!("objects/41/{fragment_index}"),
            expected_byte_length: 4 + u64::from(fragment_index),
            expected_sha256: [fragment_index as u8; 32],
        })
        .collect::<Vec<_>>();

    engine
        .begin_fragment_uploads(&entries)
        .expect("begin upload fragment batch");
    engine
        .flush_metadata()
        .expect("replicate upload begin batch");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let object_state = client
            .query_one(
                "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = 41",
                &[&volume.volume_id],
            )
            .await
            .expect("read upload object state")
            .get::<_, String>(0);
        assert_eq!(object_state, "uploading");
        let fragment_count = client
            .query_one(
                "SELECT count(*)::BIGINT FROM snapshot_payload_upload_fragments WHERE vol = $1 AND object_id = 41 AND confirmation_state = 'pending'",
                &[&volume.volume_id],
            )
            .await
            .expect("read pending upload fragments")
            .get::<_, i64>(0);
        assert_eq!(fragment_count, 3);
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("verify upload fragment batch rows");

    engine
        .confirm_fragment_uploads(&entries)
        .expect("confirm upload fragment batch");
    engine
        .flush_metadata()
        .expect("replicate upload confirmation batch");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let object_state = client
            .query_one(
                "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = 41",
                &[&volume.volume_id],
            )
            .await
            .expect("read confirmed upload object state")
            .get::<_, String>(0);
        assert_eq!(object_state, "uploaded");
        let confirmed_count = client
            .query_one(
                "SELECT count(*)::BIGINT FROM snapshot_payload_upload_fragments WHERE vol = $1 AND object_id = 41 AND confirmation_state = 'confirmed'",
                &[&volume.volume_id],
            )
            .await
            .expect("read confirmed upload fragments")
            .get::<_, i64>(0);
        assert_eq!(confirmed_count, 3);
        connection_task.abort();
        Ok::<(), MetaError>(())
    })
    .expect("verify confirmed upload fragment batch rows");
}

fn activate_payload_allocator(engine: &PgMetaEngine, namespace: u32, reserved_through: u64) {
    use crate::allocator_state::{AllocatorRecord, AllocatorStateV1};
    let state = AllocatorStateV1::new(
        AllocatorRecord::new(namespace + 1, 0).expect("inode namespace"),
        AllocatorRecord::new(namespace, reserved_through).expect("payload namespace"),
    )
    .expect("allocator state");
    engine
        .local
        .replace_allocator_state(state)
        .expect("persist allocator state");
    engine
        .bootstrap_allocator_state_local_only()
        .expect("activate allocator state");
}

fn payload_counter(object_id: u64) -> u64 {
    object_id & (crate::allocator_state::COUNTER_LIMIT - 1)
}

#[test]
fn quiescent_payload_watermark_captures_reserved_through_below_later_handouts() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    activate_payload_allocator(&engine, 31, 8_192);

    assert_eq!(
        engine
            .report_quiescent_payload_upload_watermark()
            .expect("capture watermark"),
        Some(8_192)
    );
    assert_eq!(
        engine
            .pending_payload_watermark()
            .expect("pending watermark"),
        Some((31, 8_192))
    );
    // One capture per engine: another caller's payload store was never checked.
    assert_eq!(
        engine
            .report_quiescent_payload_upload_watermark()
            .expect("second capture"),
        None
    );
    // A handout after the capture comes from a fresh reservation at or above it.
    let object_id = engine.allocate_object_ids(1).expect("allocate object id")[0];
    assert!(payload_counter(object_id) >= 8_192);
}

#[test]
fn reserved_unbegun_payload_id_blocks_the_watermark() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    activate_payload_allocator(&engine, 33, 8_192);
    let reserved = engine.allocate_object_ids(1).expect("allocate object id")[0];
    assert_eq!(payload_counter(reserved), 8_192);

    assert_eq!(
        engine
            .report_quiescent_payload_upload_watermark()
            .expect("capture watermark"),
        None,
        "a reserved ID without an upload begin must keep the watermark at or below its counter"
    );
    // Reactivation resets the batch but must not forget the outstanding handout.
    engine
        .bootstrap_allocator_state_local_only()
        .expect("reactivate allocator state");
    assert_eq!(
        engine
            .report_quiescent_payload_upload_watermark()
            .expect("capture watermark after reactivation"),
        None
    );
    assert_eq!(
        engine
            .pending_payload_watermark()
            .expect("pending watermark"),
        None
    );
}

#[test]
fn pending_local_upload_ledger_entry_blocks_the_watermark() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    activate_payload_allocator(&engine, 35, 8_192);
    let entry = UploadedFragmentLedgerEntry {
        volume_id: engine.volume_id,
        object_id: (35_u64 << 39) | 17,
        fragment_index: 0,
        object_key: "objects/pending/0".to_string(),
        expected_byte_length: 1,
        expected_sha256: [7; 32],
    };
    engine
        .begin_fragment_uploads(std::slice::from_ref(&entry))
        .expect("record local upload begin");

    assert_eq!(
        engine
            .report_quiescent_payload_upload_watermark()
            .expect("capture watermark"),
        None,
        "an unreplicated begin below the counter must block the watermark"
    );
    assert_eq!(
        engine
            .pending_payload_watermark()
            .expect("pending watermark"),
        None
    );
}

#[test]
fn unpublished_payload_watermark_stays_pending_without_stalling_ledger_replay() {
    let (_dir, engine) = temp_pg_engine_for_unit();
    activate_payload_allocator(&engine, 37, 4_096);
    engine
        .report_quiescent_payload_upload_watermark()
        .expect("capture watermark");

    // The unit engine's PostgreSQL endpoint is unreachable.
    engine
        .replay_local_upload_ledger()
        .expect("watermark publication failure must not fail ledger replay");
    assert_eq!(
        engine
            .pending_payload_watermark()
            .expect("pending watermark"),
        Some((37, 4_096))
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn reported_payload_watermark_bounds_cli_janitor_fences() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-reachability-watermark-report");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    // Each mount session opens its own engine, which captures at most once.
    let mount = || {
        PgMetaEngine::open_with_options(
            &db_path,
            PgOpenOptions {
                background_poll: false,
                ..Default::default()
            },
        )
        .expect("open postgres engine")
    };
    let volume_id = volume.volume_id;
    let query = |statement: String| -> i64 {
        let dsn = dsn.clone();
        run_async(async move {
            let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .expect("connect to test postgres");
            let connection_task = tokio::spawn(connection);
            let value = client
                .query_one(&statement, &[&volume_id])
                .await
                .expect("run fixture query")
                .get(0);
            connection_task.abort();
            Ok::<_, MetaError>(value)
        })
        .expect("run fixture query")
    };
    let namespace = 7_001_u32;
    let watermark = || {
        query(format!(
            "SELECT acked_through FROM snapshot_payload_upload_watermarks WHERE vol = $1 AND payload_namespace = {namespace}"
        ))
    };
    let mut janitor = crate::pg_engine::PgJanitorSession::new(dsn.clone(), volume_id);
    assert!(
        janitor
            .try_acquire_global_janitor_lock()
            .expect("janitor lock")
    );

    // Sustained deletion across mount sessions: each session leaves finished
    // fences, then the next mount reports from a quiescent bootstrap.
    for round in 0..3_u64 {
        let first = round * 4_096;
        let reserved_through = first + 4_096;
        let engine = mount();
        activate_payload_allocator(&engine, namespace, reserved_through);
        assert_eq!(
            query(format!(
                "WITH fences AS (INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, closed_at) \
                 SELECT $1, ({namespace}::BIGINT << 39) | counter, 'delete_pending', now() \
                 FROM generate_series({first}::BIGINT, {}::BIGINT) AS counter RETURNING 1) \
                 SELECT count(*) FROM fences",
                first + 99
            )),
            100
        );
        assert_eq!(
            engine
                .report_quiescent_payload_upload_watermark()
                .expect("capture watermark"),
            Some(reserved_through)
        );
        engine.flush_metadata().expect("publish watermark");
        assert_eq!(
            watermark(),
            i64::try_from(reserved_through).expect("counter fits")
        );
        assert_eq!(
            engine
                .pending_payload_watermark()
                .expect("pending watermark"),
            None
        );
        assert_eq!(janitor.prune_object_deletion_fences().expect("prune"), 100);
        assert_eq!(
            query("SELECT count(*) FROM snapshot_payload_upload_objects WHERE vol = $1".to_owned()),
            0,
            "fence rows stay bounded once the namespace reports"
        );
    }

    // A stale lower capture never moves the watermark backwards.
    let engine = mount();
    activate_payload_allocator(&engine, namespace, 4_096);
    engine
        .report_quiescent_payload_upload_watermark()
        .expect("capture stale watermark");
    engine.flush_metadata().expect("publish stale watermark");
    assert_eq!(watermark(), 3 * 4_096);
    janitor
        .release_global_janitor_lock()
        .expect("release janitor lock");
}
