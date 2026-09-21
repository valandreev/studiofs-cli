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
