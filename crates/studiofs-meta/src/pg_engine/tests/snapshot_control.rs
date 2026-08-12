use super::*;

use crate::pg_engine::snapshot_control::{
    SnapshotCaptureRequest, capture_snapshot_cut, capture_snapshot_in_transaction,
};
use crate::snapshot::{
    SnapshotCreationState, SnapshotLifecycle, SnapshotOrigin, SnapshotRetention,
};
use crate::snapshot_ports::SnapshotPortError;

#[test]
fn snapshot_schema_v2_replaces_legacy_snapshot_schema() {
    let ddl = crate::pg_engine::schema::studiofs_pg_schema_sql();

    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_catalog"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_creation_jobs"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_mount_leases"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_policies"));
    assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_reachability"));
    assert!(ddl.contains("byte_offset BIGINT NOT NULL"));
    assert!(!ddl.contains("CREATE TABLE IF NOT EXISTS snapshots"));
    assert!(!ddl.contains("pinned_object_ids"));
    assert!(!ddl.contains("snapshot_payload"));
}

fn snapshot_request(volume_id: i64, origin: SnapshotOrigin) -> SnapshotCaptureRequest {
    SnapshotCaptureRequest {
        volume_id,
        origin,
        retention: SnapshotRetention::manual(None).expect("infinite manual retention is valid"),
        captured_at: chrono::DateTime::parse_from_rfc3339("2026-08-12T14:07:09Z")
            .expect("fixed capture timestamp")
            .with_timezone(&chrono::Utc),
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
            format!("{}.2026-08-12-14-07-09", volume.name)
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

    let ids = run_async(async {
        let (mut client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let mut ids = Vec::new();
        for _ in 0..3 {
            let tx = client
                .transaction()
                .await
                .expect("start capture transaction");
            let entry = capture_snapshot_in_transaction(
                &tx,
                &snapshot_request(volume.volume_id, SnapshotOrigin::Automatic),
            )
            .await
            .expect("capture snapshot");
            ids.push(entry.snapshot_id.to_string());
            tx.commit().await.expect("commit capture transaction");
        }
        connection_task.abort();
        Ok(ids)
    })
    .expect("capture collision sequence");

    let base = format!("{}.2026-08-12-14-07-09", volume.name);
    assert_eq!(
        ids,
        vec![base.clone(), format!("{base}-2"), format!("{base}-3")]
    );
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
    let mut ids = vec![first, second];
    ids.sort();
    let base = format!("{}.2026-08-12-14-07-09", volume.name);
    assert_eq!(ids, vec![base.clone(), format!("{base}-2")]);

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
        let captured_at = chrono::DateTime::parse_from_rfc3339("2026-08-12T14:07:09Z")
            .expect("fixed capture timestamp")
            .with_timezone(&chrono::Utc);
        let _ = capture_snapshot_cut(&client, volume.volume_id, captured_at)
            .await
            .expect("capture cut through client");
        let tx = client.transaction().await.expect("start transaction");
        let _ = capture_snapshot_cut(&tx, volume.volume_id, captured_at)
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
