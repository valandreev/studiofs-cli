use super::{
    DEFAULT_PG_SYNC_MAX_BATCHES_PER_PAGE, DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE,
    PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR,
    PgOpenOptions, PgTestVolume, pg_dsn, run_async, seed_local, set_test_env,
};
use crate::events::{MetaEvent, encode_event};
use crate::pg_engine::PgMetaEngine;
use crate::pg_engine::sql;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

async fn insert_event_rows(
    client: &tokio_postgres::Client,
    volume_id: i64,
    rows: &[(i64, &[u8], &str)],
) -> Result<(), tokio_postgres::Error> {
    for (batch_version, payload, client_id) in rows {
        client
            .execute(
                "INSERT INTO event_log (vol, batch_version, event_type, event_payload, client_id) \
             VALUES ($1, $2, 'test', $3, $4)",
                &[&volume_id, batch_version, payload, client_id],
            )
            .await?;
    }
    Ok(())
}

#[test]
fn foreign_event_page_query_is_bounded_and_exposes_page_metadata() {
    assert!(sql::SELECT_FOREIGN_EVENT_ROWS.contains("LIMIT $4::BIGINT"));
    assert!(sql::SELECT_FOREIGN_EVENT_ROWS.contains("batch_version <= $3"));
    assert!(sql::SELECT_FOREIGN_EVENT_ROWS.contains("p.has_more"));
    assert!(sql::SELECT_FOREIGN_EVENT_ROWS.contains("p.oversized"));
    assert!(sql::SELECT_FOREIGN_EVENT_ROWS.contains("octet_length(e.event_payload)"));
    assert!(
        sql::SELECT_FOREIGN_EVENT_ROWS
            .contains("COALESCE(BOOL_OR(response_bytes > $5::BIGINT), false)")
    );
}

#[test]
fn foreign_event_page_rejects_non_positive_limits_without_postgres() {
    assert!(PgMetaEngine::validate_foreign_event_page_limits(0, 1).is_err());
    assert!(PgMetaEngine::validate_foreign_event_page_limits(-1, 1).is_err());
    assert!(PgMetaEngine::validate_foreign_event_page_limits(1, 0).is_err());
    assert!(PgMetaEngine::validate_foreign_event_page_limits(1, -1).is_err());
}

#[test]
fn pg_sync_page_limits_fall_back_for_invalid_environment_values() {
    let _batches = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, "-1");
    let _bytes = set_test_env(
        PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR,
        "not-an-integer",
    );

    assert_eq!(
        PgMetaEngine::env_positive_i64(
            PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR,
            DEFAULT_PG_SYNC_MAX_BATCHES_PER_PAGE,
        ),
        DEFAULT_PG_SYNC_MAX_BATCHES_PER_PAGE
    );
    assert_eq!(
        PgMetaEngine::env_positive_i64(
            PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR,
            DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE,
        ),
        DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE
    );
}

#[test]
fn pg_sync_page_limits_trim_whitespace_and_reject_zero() {
    let _batches = set_test_env(PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, " 128 ");
    let _bytes = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "0");

    assert_eq!(
        PgMetaEngine::env_positive_i64(
            PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR,
            DEFAULT_PG_SYNC_MAX_BATCHES_PER_PAGE,
        ),
        128
    );
    assert_eq!(
        PgMetaEngine::env_positive_i64(
            PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR,
            DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE,
        ),
        DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE
    );
}

#[test]
fn pull_foreign_events_skips_remote_when_cursor_passes_frozen_target() {
    let (_dir, engine) = super::temp_pg_engine_for_unit();
    engine
        .remote_format_validated
        .store(true, Ordering::Release);
    engine.local_version.store(12, Ordering::Release);
    engine
        .metadata_startup_target_version
        .store(10, Ordering::Release);

    assert!(matches!(engine.pull_foreign_events_locked(None), Ok(false)));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_foreign_event_pages_respect_cursor_target_and_batch_budget() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-event-pages");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        insert_event_rows(
            &client,
            volume.volume_id,
            &[
                (1, b"one-a", "remote-a"),
                (1, b"one-b", "remote-a"),
                (2, b"two-a", "remote-b"),
                (3, b"three-a", "remote-c"),
                (4, b"four-a", "remote-d"),
            ],
        )
        .await
        .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let first =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 0, 3, 2, 1_000)
                .await?;
        assert_eq!(first.first_batch_version, Some(1));
        assert_eq!(first.last_batch_version, Some(2));
        assert_eq!(first.batch_count, 2);
        assert_eq!(first.event_count, 3);
        assert!(first.has_more);
        assert!(!first.oversized);
        assert_eq!(
            first
                .rows
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<Vec<_>>(),
            vec![1, 1, 2]
        );

        let second = PgMetaEngine::query_foreign_event_page(
            &client,
            volume.volume_id,
            first.last_batch_version.expect("first page last batch"),
            3,
            2,
            1_000,
        )
        .await?;
        assert_eq!(second.first_batch_version, Some(3));
        assert_eq!(second.last_batch_version, Some(3));
        assert_eq!(second.batch_count, 1);
        assert_eq!(second.event_count, 1);
        assert!(!second.has_more);
        assert_eq!(second.rows.len(), 1);

        let empty =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 3, 3, 2, 1_000)
                .await?;
        assert!(empty.rows.is_empty());
        assert_eq!(empty.first_batch_version, None);
        assert_eq!(empty.last_batch_version, None);
        assert!(!empty.has_more);

        let past_target =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 4, 3, 2, 1_000)
                .await?;
        assert!(past_target.rows.is_empty());
        assert_eq!(past_target.first_batch_version, None);
        assert!(!past_target.has_more);

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("bounded foreign event pages");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_foreign_event_page_rejects_a_pruned_replay_gap() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-event-gap");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        insert_event_rows(&client, volume.volume_id, &[(51, b"after-gap", "remote")])
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let error = match PgMetaEngine::query_foreign_event_page(
            &client,
            volume.volume_id,
            5,
            60,
            8,
            1_000,
        )
        .await
        {
            Ok(_) => panic!("a page after a pruned range must not be replayed"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::engine::MetaError::ReplayGap {
                local_version: 5,
                first_available_version: 51,
                target_version: 60,
            }
        ));

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("foreign event page must reject a replay gap");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_foreign_event_page_stops_before_byte_budget_without_oversizing() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-event-byte-budget");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        insert_event_rows(
            &client,
            volume.volume_id,
            &[
                (1, b"first", "remote-a"),
                (2, b"second", "remote-b"),
                (3, b"third", "remote-c"),
            ],
        )
        .await
        .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let first_batch =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 0, 3, 1, 1_000_000)
                .await?;
        assert_eq!(first_batch.batch_count, 1);
        assert_eq!(first_batch.event_count, 1);
        assert!(first_batch.response_bytes > 0);

        let page = PgMetaEngine::query_foreign_event_page(
            &client,
            volume.volume_id,
            0,
            3,
            3,
            first_batch.response_bytes,
        )
        .await?;
        assert_eq!(page.first_batch_version, Some(1));
        assert_eq!(page.last_batch_version, Some(1));
        assert_eq!(page.batch_count, 1);
        assert_eq!(page.event_count, 1);
        assert_eq!(page.response_bytes, first_batch.response_bytes);
        assert!(page.has_more);
        assert!(!page.oversized);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.get::<_, i64>(0))
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1])
        );

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("byte-budget foreign event page");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_foreign_event_page_keeps_oversized_first_batch_atomic() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-event-oversized-page");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        insert_event_rows(
            &client,
            volume.volume_id,
            &[
                (1, b"oversized-a", "remote-a"),
                (1, b"oversized-b", "remote-a"),
                (2, b"next", "remote-b"),
            ],
        )
        .await
        .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        let page =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 0, 2, 8, 1).await?;
        assert_eq!(page.first_batch_version, Some(1));
        assert_eq!(page.last_batch_version, Some(1));
        assert_eq!(page.batch_count, 1);
        assert_eq!(page.event_count, 2);
        assert!(page.oversized);
        assert!(page.has_more);

        let versions = page
            .rows
            .iter()
            .map(|row| row.get::<_, i64>(0))
            .collect::<BTreeSet<_>>();
        assert_eq!(versions, BTreeSet::from([1]));

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("oversized foreign event page");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_foreign_event_batch_rejects_mixed_client_identities() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let volume = PgTestVolume::new(&dsn, "pg-foreign-event-mixed-clients");

    run_async(async {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        let event = MetaEvent::RemoveAttr { ino: 42 };
        let payload = encode_event(&event)?;
        for client_id in ["remote-a", "remote-b"] {
            client
                .execute(
                    "INSERT INTO event_log \
                 (vol, batch_version, event_type, event_payload, client_id) \
                 VALUES ($1, 1, $2, $3, $4)",
                    &[&volume.volume_id, &event.kind(), &payload, &client_id],
                )
                .await
                .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        }

        let page =
            PgMetaEngine::query_foreign_event_page(&client, volume.volume_id, 0, 1, 8, 1_000)
                .await?;
        let error = super::RemoteApplyCoordinator::decode_foreign_batch(&page.rows, 0, "local")
            .expect_err("mixed client identities must fail closed");
        assert!(matches!(
            error,
            crate::engine::MetaError::Storage(message)
                if message.contains("mixed client identities")
        ));

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("mixed-client foreign event batch");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_producer_splits_projected_batch_before_assigning_versions() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let _bytes = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "128");
    let volume = PgTestVolume::new(&dsn, "pg-producer-bounded-batch");
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
        let events = (1..=3)
            .map(|ino| MetaEvent::RemoveAttr { ino })
            .collect::<Vec<_>>();
        engine.push_event_log_batch_locked(&events)?;

        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;
        let connection_task = tokio::spawn(connection);
        let rows = client
            .query(
                "SELECT batch_version, COUNT(*)::BIGINT, \
                 SUM(pg_column_size(batch_version) + pg_column_size(seq_id) \
                     + pg_column_size(event_type) + octet_length(event_payload) \
                     + COALESCE(pg_column_size(client_id), 0))::BIGINT \
                 FROM event_log WHERE vol = $1 GROUP BY batch_version ORDER BY batch_version",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?;

        assert_eq!(rows.iter().map(|row| row.get::<_, i64>(1)).sum::<i64>(), 3);
        assert!(rows.len() > 1, "producer must split the projected range");
        for row in rows {
            assert!(
                row.get::<_, i64>(2) <= 128,
                "published batch exceeded replay byte cap"
            );
        }

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("producer must publish bounded event batches");
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn pg_producer_rejects_single_event_over_byte_cap_before_version_bump() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this ignored test");
    let _bytes = set_test_env(PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR, "1");
    let volume = PgTestVolume::new(&dsn, "pg-producer-oversized-event");
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
        let before = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?
            .get::<_, i64>(0);

        let error = engine
            .push_event_log_batch_locked(&[MetaEvent::RemoveAttr { ino: 42 }])
            .expect_err("a single event larger than the replay byte cap must be rejected");
        assert!(matches!(
            error,
            crate::engine::MetaError::EventTooLarge { .. }
        ));

        let after = client
            .query_one(
                "SELECT version FROM metadata_state WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?
            .get::<_, i64>(0);
        assert_eq!(
            after, before,
            "rejected event must not bump metadata version"
        );
        let event_count = client
            .query_one(
                "SELECT COUNT(*)::BIGINT FROM event_log WHERE vol = $1",
                &[&volume.volume_id],
            )
            .await
            .map_err(|error| crate::engine::MetaError::Storage(error.to_string()))?
            .get::<_, i64>(0);
        assert_eq!(event_count, 0, "rejected event must not be inserted");

        connection_task.abort();
        Ok::<(), crate::engine::MetaError>(())
    })
    .expect("oversized event must be rejected before publication");
}
