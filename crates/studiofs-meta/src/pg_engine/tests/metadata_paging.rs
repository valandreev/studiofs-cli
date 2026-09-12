use super::{
    DEFAULT_PG_SYNC_MAX_BATCHES_PER_PAGE, DEFAULT_PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE,
    PG_SYNC_MAX_BATCHES_PER_PAGE_ENV_VAR, PG_SYNC_MAX_RESPONSE_BYTES_PER_PAGE_ENV_VAR,
    PgTestVolume, pg_dsn, run_async, set_test_env,
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
    assert!(PgMetaEngine::validate_foreign_event_page_limits(1, 0).is_err());
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
