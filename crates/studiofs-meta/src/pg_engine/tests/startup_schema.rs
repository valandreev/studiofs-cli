//! SFS-1006: capability-scoped startup schema validation against live PostgreSQL.

use super::*;
use crate::snapshot_ports::SnapshotPolicyStore;

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
fn startup_schema_unreachable_or_timed_out_postgres_falls_back_to_local_first() {
    assert_eq!(
        validate("postgres://127.0.0.1:1/studiofs", 1, Duration::from_secs(5)).ok(),
        Some(false),
        "refused connection"
    );
    // The zero deadline elapses before any connection can complete.
    let outcome = PgMetaEngine::validate_postgres_schema(
        "postgres://127.0.0.1:1/studiofs",
        1,
        Duration::ZERO,
        Duration::from_secs(1),
    )
    .expect("a connect timeout is not a startup failure");
    assert!(matches!(
        outcome,
        StartupSchemaValidation::Fallback(MetaError::Timeout(_))
    ));
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

    // An optional feature's table is gone: the mount validates and only the
    // feature fails, with a non-retryable error naming the missing table.
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
    // Snapshot ports redact backend failures, so only the failure is observable.
    let policy = run_async(async { Ok(SnapshotPolicyStore::load_policy(&*engine).await) })
        .expect("run the policy load");
    assert!(
        policy.is_err(),
        "the snapshot policy feature is unavailable: {policy:?}"
    );
    drop(engine);

    // A required column is gone: incompatible even though core is newer.
    sql("ALTER TABLE metadata_state DROP COLUMN checkpoint_sha256");
    let message = refusal(&dsn, vol);
    assert!(
        message.contains(
            "schema_component_outdated (PostgreSQL core schema 99 lacks metadata_state.checkpoint_sha256)"
        ),
        "{message}"
    );

    // A required component that is older than this binary reads.
    sql(
        "ALTER TABLE metadata_state ADD COLUMN checkpoint_sha256 BYTEA;
         UPDATE studiofs_schema_components SET version = 1 WHERE component = 'payload_gc'",
    );
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
