//! Integration tests for `studiofs destroy`.
//!
//! Run fast-only tests:
//!   cargo test -p studiofs --test destroy_test
//!
//! Run infra-backed tests (RustFS/S3 + PostgreSQL):
//!   cargo test -p studiofs --test destroy_test -- --ignored --nocapture --test-threads=1

use bytes::Bytes;
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::OnceLock;
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, FormatInfo, MetaFormat, MetadataBackend,
    MetadataConfig, PostgresConfig, RedbMetaEngine, ensure_studiofs_database,
};
use uuid::Uuid;

struct TestEnv {
    s3_endpoint: String,
    s3_bucket: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_region: String,
}

struct InfraFixture {
    name: String,
    home_dir: tempfile::TempDir,
    meta_dir: PathBuf,
    marker_key: String,
    pg_dsn: String,
    pg_volume_id: i64,
    pg_uuid: String,
    env: TestEnv,
}

fn workspace_root() -> &'static Path {
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    ROOT.get_or_init(|| {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|dir| dir.parent())
            .unwrap_or(manifest_dir)
            .to_path_buf()
    })
    .as_path()
}

fn workspace_env_path() -> PathBuf {
    workspace_root().join(".env")
}

fn workspace_dotenv_vars() -> &'static HashMap<String, String> {
    static VARS: OnceLock<HashMap<String, String>> = OnceLock::new();
    VARS.get_or_init(|| {
        let mut vars = HashMap::new();
        let env_path = workspace_env_path();
        if let Ok(iter) = dotenvy::from_path_iter(&env_path) {
            for item in iter.flatten() {
                vars.insert(item.0, item.1);
            }
        }
        vars
    })
}

fn env_or_dotenv(key: &str, default: Option<&str>) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .or_else(|| workspace_dotenv_vars().get(key).cloned())
        .or_else(|| default.map(ToString::to_string))
}

fn load_test_env() -> &'static TestEnv {
    static ENV: OnceLock<TestEnv> = OnceLock::new();
    ENV.get_or_init(|| TestEnv {
        s3_endpoint: env_or_dotenv("STUDIOFS_S3_ENDPOINT", Some("http://127.0.0.1:9000"))
            .expect("resolve STUDIOFS_S3_ENDPOINT"),
        s3_bucket: env_or_dotenv("STUDIOFS_S3_BUCKET", Some("studiofs-test"))
            .expect("resolve STUDIOFS_S3_BUCKET"),
        s3_access_key: env_or_dotenv("STUDIOFS_S3_ACCESS_KEY", Some("studiofs"))
            .expect("resolve STUDIOFS_S3_ACCESS_KEY"),
        s3_secret_key: env_or_dotenv("STUDIOFS_S3_SECRET_KEY", Some("studiofs123"))
            .expect("resolve STUDIOFS_S3_SECRET_KEY"),
        s3_region: env_or_dotenv("STUDIOFS_S3_REGION", Some("us-east-1"))
            .expect("resolve STUDIOFS_S3_REGION"),
    })
}

fn random_name(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        Uuid::new_v4().simple()
    )
}

fn home_meta_dir(home: &Path, name: &str) -> PathBuf {
    home.join(".studiofs").join("meta").join(name)
}

fn set_home_env(cmd: &mut Command, home: &Path) {
    cmd.env("HOME", home);
    cmd.env("USERPROFILE", home);
}

fn run_destroy_cmd(home: &Path, name: &str, local_only: bool, confirm_name: &str) -> Output {
    let exe = env!("CARGO_BIN_EXE_studiofs");
    let mut cmd = Command::new(exe);
    cmd.arg("destroy").arg(name);
    if local_only {
        cmd.arg("--local");
    }
    set_home_env(&mut cmd, home);
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn studiofs destroy");
    let mut stdin = child.stdin.take().expect("take stdin");
    write!(stdin, "y\n{confirm_name}\n").expect("write confirmations");
    drop(stdin);
    child.wait_with_output().expect("wait for destroy process")
}

fn output_text(output: &Output) -> String {
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    text
}

fn ensure_pg_schema(dsn: &str) {
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(
                r#"
                CREATE TABLE IF NOT EXISTS volumes (
                    vol BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    uuid TEXT NOT NULL UNIQUE,
                    block_size INTEGER NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                "#,
            )
            .await
            .expect("create volumes table");

        conn_task.abort();
    });
}

fn insert_pg_volume(dsn: &str, name: &str) -> (i64, String) {
    let uuid = Uuid::new_v4().to_string();
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "INSERT INTO volumes (name, uuid, block_size) VALUES ($1, $2, $3) RETURNING vol",
                &[&name, &uuid, &(DEFAULT_BLOCK_SIZE as i32)],
            )
            .await
            .expect("insert volume");
        conn_task.abort();
        let vol: i64 = row.get(0);
        (vol, uuid)
    })
}

fn pg_volume_exists(dsn: &str, volume_id: i64) -> bool {
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM volumes WHERE vol = $1)",
                &[&volume_id],
            )
            .await
            .expect("query volume");
        conn_task.abort();
        row.get::<_, bool>(0)
    })
}

fn make_datastore(env: &TestEnv, name: &str) -> DataStore {
    let cfg = DataStoreConfig {
        endpoint: env.s3_endpoint.clone(),
        bucket: env.s3_bucket.clone(),
        access_key: env.s3_access_key.clone(),
        secret_key: env.s3_secret_key.clone(),
        region: env.s3_region.clone(),
        volume_name: name.to_string(),
    };
    DataStore::new(&cfg).expect("create datastore")
}

fn create_local_metadata(
    meta_dir: &Path,
    name: &str,
    pg_dsn: &str,
    pg_volume_id: i64,
    env: &TestEnv,
    uuid: &str,
) {
    std::fs::create_dir_all(meta_dir).expect("create metadata dir");
    std::fs::create_dir_all(meta_dir.join("cache")).expect("create cache dir");
    std::fs::write(meta_dir.join("cache").join("cache.marker"), b"cache")
        .expect("write cache marker");

    let db_path = meta_dir.join(format!("{name}.db"));
    let meta = RedbMetaEngine::open(&db_path).expect("open redb");
    meta.init_root(0, 0).expect("init root inode");

    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: uuid.to_string(),
        endpoint: env.s3_endpoint.clone(),
        bucket: env.s3_bucket.clone(),
        access_key: env.s3_access_key.clone(),
        secret_key: env.s3_secret_key.clone(),
        region: env.s3_region.clone(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: pg_dsn.to_string(),
                volume_id: pg_volume_id,
            }),
        },
        block_size: DEFAULT_BLOCK_SIZE,
    };
    meta.set_format_info(&format_info).expect("set format info");
}

fn create_local_metadata_with_bad_s3_secret(
    meta_dir: &Path,
    name: &str,
    pg_dsn: &str,
    pg_volume_id: i64,
    env: &TestEnv,
) {
    std::fs::create_dir_all(meta_dir).expect("create metadata dir");
    let db_path = meta_dir.join(format!("{name}.db"));
    let meta = RedbMetaEngine::open(&db_path).expect("open redb");
    meta.init_root(0, 0).expect("init root inode");

    let format_info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: Uuid::new_v4().to_string(),
        endpoint: env.s3_endpoint.clone(),
        bucket: env.s3_bucket.clone(),
        access_key: env.s3_access_key.clone(),
        secret_key: "bad-secret-for-permission-test".to_string(),
        region: env.s3_region.clone(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: pg_dsn.to_string(),
                volume_id: pg_volume_id,
            }),
        },
        block_size: DEFAULT_BLOCK_SIZE,
    };
    meta.set_format_info(&format_info).expect("set format info");
}

fn build_infra_fixture(prefix: &str) -> Option<InfraFixture> {
    let raw_pg = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None)?;
    let env = load_test_env();

    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    let studio_dsn = rt
        .block_on(async { ensure_studiofs_database(&raw_pg).await })
        .ok()?
        .metadata_dsn;

    ensure_pg_schema(&studio_dsn);

    let name = random_name(prefix);
    let home_dir = tempfile::tempdir().expect("create temp home");
    let meta_dir = home_meta_dir(home_dir.path(), &name);

    let (pg_volume_id, pg_uuid) = insert_pg_volume(&studio_dsn, &name);

    let store = make_datastore(env, &name);
    let marker_key = format!("{name}/objects/destroy-marker.txt");
    if rt
        .block_on(store.put_by_key(&marker_key, Bytes::from_static(b"marker")))
        .is_err()
    {
        return None;
    }

    Some(InfraFixture {
        name,
        home_dir,
        meta_dir,
        marker_key,
        pg_dsn: studio_dsn,
        pg_volume_id,
        pg_uuid,
        env: TestEnv {
            s3_endpoint: env.s3_endpoint.clone(),
            s3_bucket: env.s3_bucket.clone(),
            s3_access_key: env.s3_access_key.clone(),
            s3_secret_key: env.s3_secret_key.clone(),
            s3_region: env.s3_region.clone(),
        },
    })
}

#[test]
fn destroy_fails_for_missing_volume_name() {
    let home = tempfile::tempdir().expect("create temp home");
    let volume_name = random_name("destroy-missing");
    let output = run_destroy_cmd(home.path(), &volume_name, false, &volume_name);
    assert!(
        !output.status.success(),
        "destroy should fail for missing metadata DB"
    );
    let text = output_text(&output).to_lowercase();
    assert!(
        text.contains("not found"),
        "output should mention missing volume, got: {text}"
    );
}

#[test]
#[ignore]
fn destroy_fails_cleanly_with_incorrect_s3_permissions() {
    let Some(fixture) = build_infra_fixture("destroy-bad-creds") else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN/S3 env not ready");
        return;
    };

    create_local_metadata_with_bad_s3_secret(
        &fixture.meta_dir,
        &fixture.name,
        &fixture.pg_dsn,
        fixture.pg_volume_id,
        &fixture.env,
    );
    let db_path = fixture.meta_dir.join(format!("{}.db", fixture.name));
    assert!(db_path.exists(), "local DB should exist before destroy");

    let output = run_destroy_cmd(fixture.home_dir.path(), &fixture.name, false, &fixture.name);
    let text = output_text(&output);
    assert!(
        output.status.success(),
        "destroy should exit cleanly (not fail) when S3 credentials are invalid, got: {text}"
    );
    assert!(
        db_path.exists(),
        "local DB should remain after destroy exits early due to consistency check failure"
    );

    let store = make_datastore(&fixture.env, &fixture.name);
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    let marker_exists = rt
        .block_on(store.exists(&fixture.marker_key))
        .expect("check marker key exists");
    assert!(
        marker_exists,
        "remote S3 marker should remain after failed destroy"
    );
    assert!(
        pg_volume_exists(&fixture.pg_dsn, fixture.pg_volume_id),
        "postgres metadata should remain after failed destroy"
    );
}

#[test]
#[ignore]
fn destroy_local_only_removes_local_state_and_keeps_remote() {
    let Some(fixture) = build_infra_fixture("destroy-local-only") else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN/S3 env not ready");
        return;
    };

    create_local_metadata(
        &fixture.meta_dir,
        &fixture.name,
        &fixture.pg_dsn,
        fixture.pg_volume_id,
        &fixture.env,
        &fixture.pg_uuid,
    );
    assert!(
        fixture.meta_dir.exists(),
        "local metadata dir should exist before --local destroy"
    );

    let output = run_destroy_cmd(fixture.home_dir.path(), &fixture.name, true, &fixture.name);
    assert!(
        output.status.success(),
        "--local destroy should succeed, got: {}",
        output_text(&output)
    );
    assert!(
        !fixture.meta_dir.exists(),
        "--local destroy should remove local metadata/cache"
    );

    let store = make_datastore(&fixture.env, &fixture.name);
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    let marker_exists = rt
        .block_on(store.exists(&fixture.marker_key))
        .expect("check marker key exists");
    assert!(
        marker_exists,
        "--local destroy must not delete remote S3 data"
    );
    assert!(
        pg_volume_exists(&fixture.pg_dsn, fixture.pg_volume_id),
        "--local destroy must not delete postgres metadata"
    );
}

#[test]
#[ignore]
fn destroy_full_removes_local_s3_and_postgres_state() {
    let Some(fixture) = build_infra_fixture("destroy-full") else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN/S3 env not ready");
        return;
    };

    create_local_metadata(
        &fixture.meta_dir,
        &fixture.name,
        &fixture.pg_dsn,
        fixture.pg_volume_id,
        &fixture.env,
        &fixture.pg_uuid,
    );

    let output = run_destroy_cmd(fixture.home_dir.path(), &fixture.name, false, &fixture.name);
    assert!(
        output.status.success(),
        "full destroy should succeed, got: {}",
        output_text(&output)
    );
    assert!(
        !fixture.meta_dir.exists(),
        "full destroy should remove local metadata/cache"
    );

    let store = make_datastore(&fixture.env, &fixture.name);
    let rt = tokio::runtime::Runtime::new().expect("build runtime");
    let marker_exists = rt
        .block_on(store.exists(&fixture.marker_key))
        .expect("check marker key exists");
    assert!(
        !marker_exists,
        "full destroy should remove remote S3 data for the volume"
    );
    assert!(
        !pg_volume_exists(&fixture.pg_dsn, fixture.pg_volume_id),
        "full destroy should delete postgres volume metadata"
    );
}
