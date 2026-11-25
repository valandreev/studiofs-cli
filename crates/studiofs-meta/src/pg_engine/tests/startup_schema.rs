//! SFS-1006: capability-scoped startup schema validation against live PostgreSQL.

use super::*;
use crate::snapshot_ports::{SnapshotPolicyStore, SnapshotPortError};
use std::net::{Ipv4Addr, TcpListener};

fn validate(dsn: &str, volume_id: i64, connect_timeout: Duration) -> MetaResult<bool> {
    PgMetaEngine::validate_postgres_schema(dsn, volume_id, connect_timeout, Duration::from_secs(10))
        .map(|outcome| match outcome {
            StartupSchemaValidation::Available => true,
            StartupSchemaValidation::Fallback(error) => {
                assert!(error.is_reachability_error(), "{error}");
                false
            }
        })
}

fn refusal(dsn: &str, volume_id: i64) -> String {
    let error = validate(dsn, volume_id, Duration::from_secs(10))
        .expect_err("schema validation must refuse");
    assert!(
        matches!(error, MetaError::Storage(_)) && !error.is_reachability_error(),
        "a proven schema problem is not a network failure: {error:?}"
    );
    error.to_string()
}

#[test]
fn startup_schema_unreachable_postgres_falls_back_to_local_first() {
    // A bound but non-listening socket is refused on Linux, but Darwin drops
    // the SYN and the connect times out instead. A released port is refused
    // with RST on both.
    let addr = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .and_then(|listener| listener.local_addr())
        .expect("pick a loopback port and release it");

    let outcome = PgMetaEngine::validate_postgres_schema(
        &format!("postgres://{addr}/studiofs"),
        1,
        Duration::from_secs(5),
        Duration::from_secs(1),
    )
    .expect("a refused connection falls back");
    assert!(matches!(
        outcome,
        StartupSchemaValidation::Fallback(MetaError::RemoteUnavailable(_))
    ));
}

#[test]
fn startup_schema_connect_timeout_falls_back_to_local_first() {
    // Keep the listener open without answering the PostgreSQL handshake.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("listen on loopback");
    let addr = listener.local_addr().expect("read the listening address");
    let outcome = PgMetaEngine::validate_postgres_schema(
        &format!("postgres://{addr}/studiofs"),
        1,
        Duration::from_millis(100),
        Duration::from_secs(1),
    )
    .expect("a connect timeout is not a startup failure");
    assert!(matches!(
        outcome,
        StartupSchemaValidation::Fallback(MetaError::Timeout(message))
            if message.contains("validate postgres schema connect timed out")
    ));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_schema_command_timeout_falls_back_to_local_first() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let volume = PgTestVolume::new(&schema.dsn, "sfs1006-command-timeout");
    let dsn = &schema.dsn;
    let vol = volume.volume_id;

    let outcome = run_async(async {
        tokio::time::timeout(Duration::from_secs(20), async {
            let (mut locker, connection) = crate::connect_studiofs_postgres(dsn).await?;
            // Both futures are local to the deadline. Dropping them closes the
            // session and releases its lock if setup or cleanup fails.
            let (_, outcome) = tokio::try_join!(
                async { connection.await.map_err(|e| MetaError::Storage(pg_err(e))) },
                async move {
                    let tx = locker
                        .transaction()
                        .await
                        .map_err(|e| MetaError::Storage(pg_err(e)))?;
                    tx.batch_execute(
                        "LOCK TABLE studiofs_schema_components IN ACCESS EXCLUSIVE MODE",
                    )
                    .await
                    .map_err(|e| MetaError::Storage(pg_err(e)))?;
                    let outcome = PgMetaEngine::validate_postgres_schema(
                        dsn,
                        vol,
                        Duration::from_secs(5),
                        Duration::from_secs(1),
                    );
                    tx.rollback()
                        .await
                        .map_err(|e| MetaError::Storage(pg_err(e)))?;
                    outcome
                }
            )?;
            Ok(outcome)
        })
        .await
        .expect("registry lock setup and cleanup must complete within the test deadline")
    })
    .expect("a command timeout falls back after releasing the registry lock");
    assert!(matches!(
        outcome,
        StartupSchemaValidation::Fallback(MetaError::Timeout(message))
            if message.contains("validate postgres schema timed out")
    ));
    assert_eq!(validate(dsn, vol, Duration::from_secs(5)).ok(), Some(true));
}

#[test]
#[ignore = "requires STUDIOFS_TEST_PG_DSN with local postgres"]
fn startup_schema_distinguishes_additive_optional_absent_and_incompatible_schemas() {
    let dsn = pg_dsn().expect("STUDIOFS_TEST_PG_DSN is required for this explicit test");
    let schema = IsolatedPgSchema::new(&dsn);
    let dsn = schema.dsn.clone();
    let sql = |statement: &str| execute_test_sql(&dsn, statement.to_owned()).expect(statement);

    // A schema with no StudioFS objects at all is absent, not unreachable.
    let message = refusal(&dsn, 1);
    assert!(
        message.contains("schema_component_missing (PostgreSQL core schema)"),
        "{message}"
    );

    let volume = PgTestVolume::new(&dsn, "sfs1006-startup-schema");
    let vol = volume.volume_id;
    assert_eq!(
        validate(&dsn, vol, Duration::from_secs(10)).ok(),
        Some(true)
    );

    for table in ["trash_entry_authority", "trash_publication_receipts"] {
        sql(&format!("ALTER TABLE {table} RENAME TO hidden_{table}"));
        let result = validate(&dsn, vol, Duration::from_secs(10));
        sql(&format!("ALTER TABLE hidden_{table} RENAME TO {table}"));
        let error = result.expect_err("schema validation must refuse missing trash tables");
        assert!(
            matches!(&error, MetaError::Storage(message)
                if message.contains("schema_component_outdated (PostgreSQL core schema")
                    && message.contains(&format!("lacks {table})"))),
            "{error}"
        );
    }

    // Additive successor objects: a newer core, an unknown component, an extra table.
    sql(
        "UPDATE studiofs_schema_components SET version = 99 WHERE component = 'core';
         INSERT INTO studiofs_schema_components (component, version) VALUES ('sfs1006_feature', 1);
         CREATE TABLE sfs1006_feature (vol BIGINT PRIMARY KEY);",
    );
    assert_eq!(
        validate(&dsn, vol, Duration::from_secs(10)).ok(),
        Some(true)
    );

    // A missing snapshot policy table leaves the mount available. The policy
    // port redacts the statement error into a retryable backend error.
    sql("DROP TABLE snapshot_policies");
    assert_eq!(
        validate(&dsn, vol, Duration::from_secs(10)).ok(),
        Some(true)
    );
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
    .expect("mount opens without the optional snapshot policy table");
    let policy = run_async(async { Ok(SnapshotPolicyStore::load_policy(&*engine).await) })
        .expect("run the policy load");
    assert!(
        matches!(policy, Err(SnapshotPortError::Retryable { .. })),
        "the snapshot policy feature is unavailable: {policy:?}"
    );
    drop(engine);

    // Missing required columns refuse startup even though core is newer.
    for column in ["checkpoint_sha256", "write_requirement"] {
        sql(&format!(
            "ALTER TABLE metadata_state RENAME COLUMN {column} TO hidden_{column}"
        ));
        let message = refusal(&dsn, vol);
        sql(&format!(
            "ALTER TABLE metadata_state RENAME COLUMN hidden_{column} TO {column}"
        ));
        assert!(
            message.contains(&format!(
                "schema_component_outdated (PostgreSQL core schema 99 lacks metadata_state.{column})"
            )),
            "{message}"
        );
    }

    // A required component that is older than this binary reads.
    sql("UPDATE studiofs_schema_components SET version = 1 WHERE component = 'payload_gc'");
    let message = refusal(&dsn, vol);
    assert!(
        message.contains("schema_component_outdated (PostgreSQL payload GC schema 1, needs 2)")
            && !message.contains("core schema"),
        "{message}"
    );

    // The revision record itself is gone.
    sql("DELETE FROM studiofs_schema_components");
    let message = refusal(&dsn, vol);
    assert!(
        message.contains("schema_component_missing (PostgreSQL core schema)")
            && message.contains("schema_component_missing (PostgreSQL payload GC schema)"),
        "{message}"
    );
}
