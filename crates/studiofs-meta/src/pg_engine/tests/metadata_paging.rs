use super::{PgTestVolume, pg_dsn, run_async};
use crate::pg_engine::PgMetaEngine;
use crate::pg_engine::sql;
use std::collections::BTreeSet;

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
