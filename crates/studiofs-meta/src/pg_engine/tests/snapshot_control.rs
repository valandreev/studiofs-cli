use super::*;

use crate::pg_engine::snapshot_control::{
    SnapshotCaptureRequest, allocate_snapshot_id, capture_snapshot_cut,
    capture_snapshot_in_transaction,
};
use crate::snapshot::{
    SnapshotCreationState, SnapshotId, SnapshotLifecycle, SnapshotOrigin, SnapshotRetention,
};
use crate::snapshot_ports::SnapshotPortError;
use chrono::{TimeZone, Utc};

#[test]
fn snapshot_schema_v2_replaces_legacy_snapshot_schema() {
    let ddl = crate::pg_engine::schema::studiofs_pg_schema_sql();

    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_catalog"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_creation_jobs"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_mount_leases"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_policies"));
    assert!(!ddl.contains("snapshot_current_references"));
    assert!(!ddl.contains("snapshot_reachability"));
    assert!(!ddl.contains("snapshot_policy_slots"));
    assert!(!ddl.contains("snapshot_policy_leases"));
    assert!(ddl.contains("byte_offset BIGINT NOT NULL"));
    assert!(!ddl.contains("CREATE TABLE IF NOT EXISTS snapshots"));
    assert!(!ddl.contains("pinned_object_ids"));
    assert!(!ddl.contains("snapshot_payload"));
    assert!(ddl.contains("ADD COLUMN IF NOT EXISTS batch_version BIGINT"));
    assert!(ddl.contains("SET batch_version = 0"));
    assert!(ddl.contains("ALTER COLUMN batch_version SET NOT NULL"));
    assert!(ddl.contains("retention_seconds IS NOT NULL"));
}

fn snapshot_request(volume_id: i64, origin: SnapshotOrigin) -> SnapshotCaptureRequest {
    SnapshotCaptureRequest {
        volume_id,
        origin,
        retention: match origin {
            SnapshotOrigin::Manual => {
                SnapshotRetention::manual(None).expect("infinite manual retention is valid")
            }
            SnapshotOrigin::Automatic => SnapshotRetention::automatic(Duration::from_secs(3600))
                .expect("finite automatic retention is valid"),
        },
    }
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn capture_is_one_transaction_and_one_durable_job() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-control-capture");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let tx = client
            .transaction()
            .await
            .expect("start capture transaction");
        let entry = capture_snapshot_in_transaction(
            &tx,
            &snapshot_request(volume.volume_id, SnapshotOrigin::Manual),
        )
        .await
        .expect("capture snapshot");

        assert_eq!(entry.origin, SnapshotOrigin::Manual);
        assert_eq!(entry.lifecycle, SnapshotLifecycle::Creating);
        assert_eq!(entry.creation_state, SnapshotCreationState::Queued);
        assert_eq!(
            entry.snapshot_id.as_str(),
            format!(
                "{}.{}",
                volume.name,
                entry.cut.captured_at().format("%Y-%m-%dT%H-%M-%S%.3fZ")
            )
        );

        let job_count: i64 = tx
            .query_one(
                "SELECT count(*) FROM snapshot_creation_jobs WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .expect("query durable job")
            .get(0);
        assert_eq!(job_count, 1);
        tx.commit().await.expect("commit capture transaction");
        connection_task.abort();
        Ok(())
    })
    .expect("capture snapshot");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn snapshot_id_collision_suffixes_are_deterministic() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-control-collision");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let captured_at = Utc
            .with_ymd_and_hms(2026, 8, 12, 14, 7, 9)
            .single()
            .expect("valid capture timestamp");
        let base = SnapshotId::from_cut(&volume.name, captured_at).expect("base snapshot ID");
        let second = SnapshotId::with_collision_suffix(&volume.name, captured_at, 2)
            .expect("second snapshot ID");
        let tx = client
            .transaction()
            .await
            .expect("start allocator transaction");
        for snapshot_id in [base.to_string(), second.to_string()] {
            tx.execute(
                "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle, retention_seconds) VALUES ($1, $2, 'manual', 0, $3, 'creating', NULL)",
                &[&volume.volume_id, &snapshot_id, &captured_at],
            )
            .await
            .expect("seed collision catalog row");
        }
        let third = allocate_snapshot_id(
            &tx,
            volume.volume_id,
            &volume.name,
            captured_at,
            &base,
        )
        .await
        .expect("allocate third snapshot ID");
        assert_eq!(third.as_str(), format!("{base}-3"));
        tx.commit().await.expect("commit allocator transaction");
        connection_task.abort();
        Ok(())
    })
    .expect("capture collision sequence");
}

async fn capture_once(dsn: &str, request: SnapshotCaptureRequest) -> String {
    let (mut client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to test postgres");
    let connection_task = tokio::spawn(connection);
    let tx = client
        .transaction()
        .await
        .expect("start capture transaction");
    let entry = capture_snapshot_in_transaction(&tx, &request)
        .await
        .expect("capture snapshot");
    tx.commit().await.expect("commit capture transaction");
    connection_task.abort();
    entry.snapshot_id.to_string()
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn concurrent_create_is_idempotent_and_redacted() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-control-concurrent");
    let request = snapshot_request(volume.volume_id, SnapshotOrigin::Manual);

    let ids = run_async(async {
        let ids = tokio::join!(
            capture_once(&dsn, request.clone()),
            capture_once(&dsn, request),
        );
        Ok(ids)
    })
    .expect("concurrent capture");

    let (first, second) = ids;
    let mut ids = [first, second];
    ids.sort();
    assert_eq!(ids.len(), 2);
    assert!(
        ids.iter()
            .all(|id| id.starts_with(&format!("{}.", volume.name)))
    );

    let error = SnapshotPortError::from_backend(
        "snapshot capture",
        "postgres://snapshot-user:snapshot-password@127.0.0.1:5432/sentinel",
    );
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("snapshot-user"));
    assert!(!rendered.contains("snapshot-password"));
    assert!(!rendered.contains("postgres://"));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn query_helpers_accept_client_and_transaction() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let volume = PgTestVolume::new(&dsn, "snapshot-control-generic-client");

    run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let _ = capture_snapshot_cut(&client, volume.volume_id)
            .await
            .expect("capture cut through client");
        let tx = client.transaction().await.expect("start transaction");
        let _ = capture_snapshot_cut(&tx, volume.volume_id)
            .await
            .expect("capture cut through transaction");
        tx.rollback().await.expect("rollback transaction");
        connection_task.abort();
        Ok(())
    })
    .expect("query snapshot cut");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn error_and_report_redact_sentinel_values() {
    let error = SnapshotPortError::from_backend(
        "snapshot capture",
        "postgres://snapshot-user:snapshot-password@127.0.0.1:5432/sentinel",
    );
    let rendered = format!("{error:?} {error}");
    assert!(!rendered.contains("snapshot-user"));
    assert!(!rendered.contains("snapshot-password"));
    assert!(!rendered.contains("postgres://"));
}
