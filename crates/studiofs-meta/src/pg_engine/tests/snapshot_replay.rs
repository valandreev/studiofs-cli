use super::{PgTestVolume, pg_dsn, run_async, seed_local};
use crate::engine::{MetaFsOps, MetaInodes, MetaNamespace, MetaReplication};
use crate::pg_engine::{PgMetaEngine, PgOpenOptions};
use crate::redb_engine::RedbMetaEngine;
use crate::schema::ROOT_INO;
use crate::snapshot::{SnapshotOrigin, SnapshotRetention};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use tempfile::TempDir;

fn snapshot_replay_tempdir(prefix: &str) -> TempDir {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .map(|path| {
            if path.is_absolute() {
                path
            } else {
                studiofs_test_support::workspace_root().join(path)
            }
        })
        .unwrap_or_else(|| studiofs_test_support::workspace_root().join("target"));
    let replay_root = target_dir.join("snapshot-v2").join("replay");
    std::fs::create_dir_all(&replay_root).expect("create snapshot replay root");
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(replay_root)
        .expect("create snapshot replay tempdir")
}

fn open_pg_fixture(
    dsn: &str,
    prefix: &str,
) -> (TempDir, PgTestVolume, std::sync::Arc<PgMetaEngine>) {
    let dir = snapshot_replay_tempdir(prefix);
    let volume = PgTestVolume::new(dsn, prefix);
    let db_path = dir.path().join("meta.redb");
    seed_local(&db_path, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open postgres metadata engine");
    (dir, volume, engine)
}

fn manual_retention() -> SnapshotRetention {
    SnapshotRetention::manual(None).expect("infinite manual retention is valid")
}

fn push_uncheckpointed_local_batch(engine: &PgMetaEngine) -> i64 {
    let start = engine.flushed_event_seq.load(Ordering::SeqCst);
    let end = engine
        .local
        .get_counter("nextEventSeq")
        .expect("read next local event sequence");
    let events = engine
        .local
        .read_events_range(start, end)
        .expect("read uncheckpointed local events");
    assert!(!events.is_empty(), "test batch must contain events");
    engine
        .push_event_log_batch_locked(&events)
        .expect("publish uncheckpointed event batch")
}

fn source_row(dsn: &str, volume_id: i64, snapshot_id: &str) -> (i64, Vec<u8>, Vec<u8>) {
    run_async(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT checkpoint_version, checkpoint_bytes, checkpoint_sha256 FROM snapshot_source_checkpoints WHERE vol = $1 AND snapshot_id = $2",
                &[&volume_id, &snapshot_id],
            )
            .await
            .expect("read retained source row");
        let result = (row.get(0), row.get(1), row.get(2));
        connection_task.abort();
        Ok(result)
    })
    .expect("read retained source row")
}

fn metadata_versions(dsn: &str, volume_id: i64) -> (i64, i64) {
    run_async(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT version, checkpoint_version FROM metadata_state WHERE vol = $1",
                &[&volume_id],
            )
            .await
            .expect("read metadata versions");
        let result = (row.get(0), row.get(1));
        connection_task.abort();
        Ok(result)
    })
    .expect("read metadata versions")
}

fn remote_payload_metrics(dsn: &str, volume_id: i64) -> (i64, i64, i64) {
    run_async(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let row = client
            .query_one(
                r#"
                SELECT
                    pg_column_size(ms.snapshot)::BIGINT,
                    COALESCE((
                        SELECT SUM(pg_column_size(event_payload))::BIGINT
                        FROM event_log
                        WHERE vol = $1
                    ), 0),
                    (
                        SELECT count(*)::BIGINT
                        FROM event_log
                        WHERE vol = $1
                    )
                FROM metadata_state AS ms
                WHERE ms.vol = $1
                "#,
                &[&volume_id],
            )
            .await
            .expect("read remote payload metrics");
        let result = (row.get(0), row.get(1), row.get(2));
        connection_task.abort();
        Ok(result)
    })
    .expect("read remote payload metrics")
}

fn assert_present(meta: &RedbMetaEngine, name: &[u8]) {
    assert!(meta.lookup(ROOT_INO, name).is_ok(), "expected {:?}", name);
}

fn assert_absent(meta: &RedbMetaEngine, name: &[u8]) {
    assert!(
        meta.lookup(ROOT_INO, name).is_err(),
        "did not expect {:?}",
        name
    );
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn reconstructs_exact_cut_and_excludes_later_events() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let (_dir, volume, engine) = open_pg_fixture(&dsn, "snapshot-replay-exact-");

    engine
        .create_dir(ROOT_INO, b"before-cut", 0, 0, 0o755)
        .expect("create pre-checkpoint directory");
    engine.flush_metadata().expect("flush pre-checkpoint state");
    let (checkpoint_head, checkpoint_version) = metadata_versions(&dsn, volume.volume_id);
    assert_eq!(checkpoint_head, checkpoint_version);

    engine
        .create_dir(ROOT_INO, b"at-cut", 0, 0, 0o755)
        .expect("create event included by cut");
    let included_version = push_uncheckpointed_local_batch(&engine);
    assert_eq!(included_version, checkpoint_version + 1);

    let entry = engine
        .create_snapshot_v2(SnapshotOrigin::Manual, manual_retention())
        .expect("capture snapshot source");
    assert_eq!(entry.cut.cut_version(), included_version);

    engine
        .create_dir(ROOT_INO, b"after-cut", 0, 0, 0o755)
        .expect("create event after cut");
    let later_version = push_uncheckpointed_local_batch(&engine);
    assert_eq!(later_version, included_version + 1);

    let claimant_dir = snapshot_replay_tempdir("snapshot-claimant-");
    let claimant_db = claimant_dir.path().join("meta.redb");
    seed_local(&claimant_db, &volume.format_info());
    let claimant = PgMetaEngine::open_with_options(
        &claimant_db,
        PgOpenOptions {
            background_poll: false,
        },
    )
    .expect("open claimant metadata engine");

    let replay_dir = snapshot_replay_tempdir("exact-cut-");
    let destination = replay_dir.path().join("meta.redb");
    let reconstruction = claimant
        .reconstruct_snapshot_source(&entry.snapshot_id, &destination)
        .expect("claimant reconstructs captured cut");
    let reconstructed = RedbMetaEngine::open(&reconstruction.path).expect("open reconstruction");

    assert_present(&reconstructed, b"before-cut");
    assert_present(&reconstructed, b"at-cut");
    assert_absent(&reconstructed, b"after-cut");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn captured_checkpoint_bytes_survive_serving_checkpoint_replacement() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let (_dir, volume, engine) = open_pg_fixture(&dsn, "snapshot-replay-replacement-");

    engine
        .create_dir(ROOT_INO, b"original-cut", 0, 0, 0o755)
        .expect("create original directory");
    engine.flush_metadata().expect("flush original checkpoint");
    let entry = engine
        .create_snapshot_v2(SnapshotOrigin::Manual, manual_retention())
        .expect("capture snapshot source");
    let source_before = source_row(&dsn, volume.volume_id, entry.snapshot_id.as_str());

    engine
        .create_dir(ROOT_INO, b"newer-serving-state", 0, 0, 0o755)
        .expect("create newer directory");
    engine.flush_metadata().expect("replace serving checkpoint");
    let source_after = source_row(&dsn, volume.volume_id, entry.snapshot_id.as_str());
    let (serving_head, serving_checkpoint) = metadata_versions(&dsn, volume.volume_id);

    assert!(serving_head > entry.cut.cut_version());
    assert!(serving_checkpoint > entry.cut.cut_version());
    assert_eq!(source_before, source_after);

    let replay_dir = snapshot_replay_tempdir("replacement-");
    let destination = replay_dir.path().join("meta.redb");
    let reconstruction = engine
        .reconstruct_snapshot_source(&entry.snapshot_id, &destination)
        .expect("reconstruct from retained source bytes");
    let reconstructed = RedbMetaEngine::open(&reconstruction.path).expect("open reconstruction");

    assert_present(&reconstructed, b"original-cut");
    assert_absent(&reconstructed, b"newer-serving-state");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn many_snapshots_do_not_grow_serving_checkpoint_or_event_payloads() {
    let Some(dsn) = pg_dsn() else {
        return;
    };
    let (_dir, volume, engine) = open_pg_fixture(&dsn, "snapshot-replay-thin-");

    engine
        .create_dir(ROOT_INO, b"stable-serving-state", 0, 0, 0o755)
        .expect("create serving state");
    engine.flush_metadata().expect("flush serving checkpoint");
    let before = remote_payload_metrics(&dsn, volume.volume_id);

    for _ in 0..8 {
        engine
            .create_snapshot_v2(SnapshotOrigin::Manual, manual_retention())
            .expect("capture thin snapshot source");
    }

    let after = remote_payload_metrics(&dsn, volume.volume_id);
    assert_eq!(before, after);

    let snapshot_count = run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect to test postgres");
        let connection_task = tokio::spawn(connection);
        let row = client
            .query_one(
                "SELECT count(*)::BIGINT FROM snapshot_source_checkpoints WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .expect("count retained snapshot sources");
        let count: i64 = row.get(0);
        connection_task.abort();
        Ok(count)
    })
    .expect("count retained snapshot sources");
    assert_eq!(snapshot_count, 8);
}
