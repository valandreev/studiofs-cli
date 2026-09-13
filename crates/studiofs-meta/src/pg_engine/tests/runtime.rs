use super::*;

#[test]
fn pg_pool_size_defaults_to_four() {
    let _env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    assert_eq!(
        PgOpenOptions::default().pg_pool_size,
        DEFAULT_PG_POOL_SIZE,
        "the default per-client PostgreSQL command-pool size should remain four"
    );
    assert_eq!(
        crate::MetaOpenOptions::default().pg_pool_size,
        DEFAULT_PG_POOL_SIZE,
        "the metadata factory default should use the same command-pool size"
    );
    assert_eq!(
        resolve_pg_pool_size(DEFAULT_PG_POOL_SIZE).expect("default pool size should validate"),
        4
    );
}

#[test]
fn pg_pool_size_explicit_value_is_used_without_environment_override() {
    let _env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    assert_eq!(
        resolve_pg_pool_size(7).expect("explicit pool size should validate"),
        7
    );
}

#[test]
fn pg_pool_size_environment_override_takes_precedence() {
    let _env = set_test_env("STUDIOFS_PG_POOL_SIZE", "9");

    assert_eq!(
        resolve_pg_pool_size(2).expect("environment pool size should validate"),
        9
    );
}

#[test]
fn pg_pool_size_rejects_zero() {
    let _env = set_test_env_value("STUDIOFS_PG_POOL_SIZE", None);

    assert!(
        resolve_pg_pool_size(0).is_err(),
        "zero must not be accepted as a command-pool size"
    );
}

#[test]
fn pg_pool_size_rejects_malformed_environment_override() {
    let _env = set_test_env("STUDIOFS_PG_POOL_SIZE", "not-a-number");

    assert!(
        resolve_pg_pool_size(4).is_err(),
        "malformed pool-size overrides must not silently change the connection budget"
    );
}

#[test]
fn pg_pool_size_environment_override_rejects_zero_with_variable_name() {
    let _env = set_test_env("STUDIOFS_PG_POOL_SIZE", "0");

    let error = resolve_pg_pool_size(4).expect_err("zero environment override must be rejected");

    assert!(
        error.to_string().contains("STUDIOFS_PG_POOL_SIZE"),
        "zero override errors should identify the environment variable: {error}"
    );
}

#[test]
fn pg_snapshot_backend_uses_pool_size_override() {
    let _env = set_test_env("STUDIOFS_PG_POOL_SIZE", "9");

    let backend = PgSnapshotBackend::open("postgres://127.0.0.1:1/studiofs", 1)
        .expect("snapshot backend should accept a valid pool-size override");

    assert_eq!(
        backend.command_pool.stats().max_size,
        9,
        "standalone snapshot backend should use the shared pool-size resolver"
    );
}

#[test]
fn checkpoint_freshness_environment_overrides_use_positive_values() {
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "7");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "2048");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "90");

    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, 10),
        7
    );
    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, 100),
        2048
    );
    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, 60),
        90
    );
}

#[test]
fn checkpoint_freshness_environment_overrides_fall_back_when_invalid() {
    let _lag = set_test_env(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, "0");
    let _bytes = set_test_env(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, "not-a-number");
    let _age = set_test_env(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, "-1");

    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_VERSION_LAG_ENV_VAR, 10),
        10
    );
    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_EVENT_BYTES_ENV_VAR, 100),
        100
    );
    assert_eq!(
        PgMetaEngine::env_positive_u64(PG_CHECKPOINT_MAX_AGE_SECS_ENV_VAR, 60),
        60
    );
}
