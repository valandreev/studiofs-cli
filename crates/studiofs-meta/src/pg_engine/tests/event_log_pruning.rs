use super::{
    DEFAULT_PG_EVENT_LOG_PRUNE_BATCHES, PG_EVENT_LOG_PRUNE_BATCHES_ENV_VAR, PgOpenOptions,
    PgTestVolume, pg_dsn, run_async, seed_local, set_test_env,
};
use crate::pg_engine::PgMetaEngine;
use crate::pg_engine::event_log_pruning::{
    calculate_safe_prune_floor, should_skip_event_log_prune,
};
use crate::pg_engine::janitor::pg_janitor_lock_key;
use crate::pg_engine::sql;
use crate::snapshot::ServingCheckpoint;
use sha2::{Digest, Sha256};

#[test]
fn event_log_prune_skips_without_a_published_checkpoint() {
    assert!(should_skip_event_log_prune(None));
    assert!(should_skip_event_log_prune(Some((4, 4))));
    assert!(!should_skip_event_log_prune(Some((4, 3))));
}

#[test]
fn event_log_prune_floor_uses_the_oldest_retained_source() {
    assert_eq!(
        calculate_safe_prune_floor(10, Some(4), 0).expect("valid prune floor"),
        4
    );
    assert_eq!(
        calculate_safe_prune_floor(10, None, 4).expect("valid prune floor"),
        10
    );
    assert_eq!(
        calculate_safe_prune_floor(10, Some(20), 0).expect("checkpoint bounds floor"),
        10
    );
    assert_eq!(
        calculate_safe_prune_floor(10, Some(0), 0).expect("zero source floor is valid"),
        0
    );
    assert_eq!(
        calculate_safe_prune_floor(10, None, 10).expect("equal floor is stable"),
        10
    );
    assert!(calculate_safe_prune_floor(10, Some(4), 5).is_err());
    assert!(calculate_safe_prune_floor(-1, None, 0).is_err());
    assert!(calculate_safe_prune_floor(10, Some(-1), 0).is_err());
    assert!(calculate_safe_prune_floor(10, None, -1).is_err());
}

#[test]
fn event_log_prune_sql_is_fenced_and_batch_bounded() {
    assert!(sql::SELECT_EVENT_LOG_PRUNE_STATE.contains("FOR UPDATE"));
    assert!(sql::TRY_EVENT_LOG_PRUNE_ADVISORY_LOCK.contains("pg_try_advisory_xact_lock"));
    assert!(sql::SELECT_EVENT_LOG_PRUNE_PROGRESS.contains("checkpoint_version"));
    assert!(!sql::SELECT_EVENT_LOG_PRUNE_PROGRESS.contains("FOR UPDATE"));
    assert!(sql::SELECT_EVENT_LOG_SOURCE_PRUNE_FLOOR.contains("lifecycle <> 'deleted'"));
    assert!(sql::DELETE_EVENT_LOG_PRUNE_BATCH.contains("GROUP BY batch_version"));
    assert!(sql::DELETE_EVENT_LOG_PRUNE_BATCH.contains("LIMIT $3::BIGINT"));
    assert!(sql::DELETE_EVENT_LOG_PRUNE_BATCH.contains("COUNT(*)::BIGINT"));
    assert!(sql::DELETE_EVENT_LOG_PRUNE_BATCH.contains("MAX(batch_version)"));
    assert!(sql::UPDATE_EVENT_LOG_PRUNE_STATE.contains("GREATEST(event_log_prune_floor, $2)"));
}

#[test]
fn event_log_prune_batch_limit_falls_back_for_invalid_environment_values() {
    for invalid in ["abc", "-5", "0"] {
        let _limit = set_test_env(PG_EVENT_LOG_PRUNE_BATCHES_ENV_VAR, invalid);
        assert_eq!(
            PgMetaEngine::env_positive_i64(
                PG_EVENT_LOG_PRUNE_BATCHES_ENV_VAR,
                DEFAULT_PG_EVENT_LOG_PRUNE_BATCHES,
            ),
            DEFAULT_PG_EVENT_LOG_PRUNE_BATCHES,
            "invalid value {invalid} must use the default"
        );
    }
    assert!(PgMetaEngine::validate_event_log_prune_batch_limit(0).is_err());
    assert!(PgMetaEngine::validate_event_log_prune_batch_limit(-1).is_err());
    assert!(PgMetaEngine::validate_event_log_prune_batch_limit(1).is_ok());
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_event_log_prune_deletes_complete_batches_in_bounded_transactions() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let _limit = set_test_env(PG_EVENT_LOG_PRUNE_BATCHES_ENV_VAR, "1");
    let volume = PgTestVolume::new(&dsn, "pg-event-log-prune-bounded");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        for batch_version in 1_i64..=3 {
            for _ in 0..2 {
                client
                    .execute(
                        "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) VALUES ($1, $2, 'test', decode('00', 'hex'), 'prune-test')",
                        &[&volume.volume_id, &batch_version],
                    )
                    .await
                    .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
            }
        }
        client
            .execute(
                "UPDATE metadata_state SET version = 3, checkpoint_version = 3 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let first = engine
            .run_event_log_prune_once()?
            .expect("published metadata state should be prunable");
        assert_eq!(first.safe_floor, 3);
        assert_eq!(first.durable_floor, 1);
        assert_eq!(first.pruned_rows, 2);
        assert_eq!(first.total_pruned_rows, 2);

        let remaining = client
            .query(
                "SELECT batch_version, count(*)::BIGINT FROM event_log WHERE vol = $1 GROUP BY batch_version ORDER BY batch_version",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let remaining = remaining
            .iter()
            .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![(2, 2), (3, 2)]);

        let second = engine
            .run_event_log_prune_once()?
            .expect("published metadata state should be prunable");
        assert_eq!(second.durable_floor, 2);
        assert_eq!(second.pruned_rows, 2);

        let third = engine
            .run_event_log_prune_once()?
            .expect("published metadata state should be prunable");
        assert_eq!(third.durable_floor, 3);
        assert_eq!(third.pruned_rows, 2);
        assert_eq!(third.total_pruned_rows, 6);
        assert!(
            engine
                .run_event_log_prune_once()?
                .is_none(),
            "an already-advanced prune floor must not issue another maintenance update"
        );

        let (floor, rows) = client
            .query_one(
                "SELECT event_log_prune_floor, event_log_pruned_rows FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))
            .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))?;
        assert_eq!((floor, rows), (3, 6));

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("event-log pruning should delete bounded complete batches");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_event_log_prune_defers_when_the_janitor_lock_is_contended() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-event-log-prune-contended");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    run_async(async {
        let (mut client, connection) =
            tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                .await
                .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        let tx = client
            .transaction()
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        tx.execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&pg_janitor_lock_key(volume.volume_id)],
        )
        .await
        .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        assert!(engine.run_event_log_prune_once()?.is_none());

        tx.rollback()
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("contended pruning should defer without waiting for the lock");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_event_log_prune_preserves_retained_snapshot_source_events() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let _limit = set_test_env(PG_EVENT_LOG_PRUNE_BATCHES_ENV_VAR, "64");
    let volume = PgTestVolume::new(&dsn, "pg-event-log-prune-snapshot");
    let dir = studiofs_test_support::workspace_target_tempdir("test-");
    let db = dir.path().join("meta.redb");
    seed_local(&db, &volume.format_info());
    let engine = PgMetaEngine::open_with_options(
        &db,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .expect("open pg engine");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        for batch_version in 1_i64..=5 {
            client
                .execute(
                    "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) VALUES ($1, $2, 'test', decode('00', 'hex'), 'snapshot-prune-test')",
                    &[&volume.volume_id, &batch_version],
                )
                .await
                .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        }
        client
            .execute(
                "UPDATE metadata_state SET version = 5, checkpoint_version = 5 WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let checkpoint = postcard::to_stdvec(&ServingCheckpoint::default())
            .map_err(|error| crate::engine::MetaError::Serialization(error.to_string()))?;
        let checkpoint_sha256 = Sha256::digest(&checkpoint).to_vec();
        let captured_at = chrono::Utc::now();
        client
            .execute(
                "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, 'retained-source', 'manual', 5, $2, 'ready')",
                &[&volume.volume_id, &captured_at],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        client
            .execute(
                "INSERT INTO snapshot_source_checkpoints (vol, snapshot_id, cut_version, checkpoint_version, checkpoint_format_version, checkpoint_schema_version, checkpoint_bytes, checkpoint_sha256, event_start_version, event_end_version, event_row_count) VALUES ($1, 'retained-source', 5, 2, 1, 1, $2, $3, 3, 5, 3)",
                &[&volume.volume_id, &checkpoint, &checkpoint_sha256],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        client
            .execute(
                "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, 'empty-source', 'manual', 5, $2, 'ready')",
                &[&volume.volume_id, &captured_at],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        client
            .execute(
                "INSERT INTO snapshot_source_checkpoints (vol, snapshot_id, cut_version, checkpoint_version, checkpoint_format_version, checkpoint_schema_version, checkpoint_bytes, checkpoint_sha256, event_row_count) VALUES ($1, 'empty-source', 5, 5, 1, 1, $2, $3, 0)",
                &[&volume.volume_id, &checkpoint, &checkpoint_sha256],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let result = engine
            .run_event_log_prune_once()?
            .expect("published metadata state should be prunable");
        assert_eq!(result.safe_floor, 2);
        assert_eq!(result.durable_floor, 2);
        assert_eq!(result.pruned_rows, 2);

        let remaining = client
            .query(
                "SELECT batch_version FROM event_log WHERE vol = $1 ORDER BY batch_version",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let remaining = remaining
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec![3, 4, 5]);

        let (floor, rows) = client
            .query_one(
                "SELECT event_log_prune_floor, event_log_pruned_rows FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))
            .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))?;
        assert_eq!((floor, rows), (2, 2));

        let source_count = client
            .query_one(
                "SELECT count(*)::BIGINT FROM snapshot_source_checkpoints WHERE vol = $1 AND snapshot_id = 'retained-source'",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?
            .get::<_, i64>(0);
        assert_eq!(source_count, 1);

        client
            .execute(
                "DELETE FROM snapshot_source_checkpoints WHERE vol = $1 AND snapshot_id = 'retained-source'",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        client
            .execute(
                "UPDATE snapshot_catalog SET lifecycle = 'deleted' WHERE vol = $1 AND id = 'retained-source'",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let released = engine
            .run_event_log_prune_once()?
            .expect("published metadata state should be prunable after source release");
        assert_eq!(released.safe_floor, 5);
        assert_eq!(released.durable_floor, 5);
        assert_eq!(released.pruned_rows, 3);
        assert_eq!(released.total_pruned_rows, 5);

        let remaining = client
            .query_one(
                "SELECT count(*)::BIGINT FROM event_log WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?
            .get::<_, i64>(0);
        assert_eq!(remaining, 0);

        let (floor, rows) = client
            .query_one(
                "SELECT event_log_prune_floor, event_log_pruned_rows FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))
            .map(|row| (row.get::<_, i64>(0), row.get::<_, i64>(1)))?;
        assert_eq!((floor, rows), (5, 5));

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("event-log pruning must preserve retained snapshot source events");
}
