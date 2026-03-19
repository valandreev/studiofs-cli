//! Integration tests for `studiofs dump` / `studiofs load`.
//!
//! Requires RustFS/S3 + PostgreSQL test env (ignored by default).

use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use studiofs_data::object::{create_object, format_object_key, read_object};
use studiofs_data::{DataStore, DataStoreConfig};
use studiofs_meta::{
    BlockRef, MetaOpenOptions, ROOT_INO, ensure_studiofs_database, open_meta_engine,
    open_meta_engine_with_options,
};
use uuid::Uuid;

struct TestEnv {
    s3_endpoint: String,
    s3_bucket: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_region: String,
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
        if let Ok(iter) = dotenvy::from_path_iter(workspace_env_path()) {
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

fn load_test_env() -> Option<TestEnv> {
    Some(TestEnv {
        s3_endpoint: env_or_dotenv("STUDIOFS_S3_ENDPOINT", Some("http://127.0.0.1:9000"))?,
        s3_bucket: env_or_dotenv("STUDIOFS_S3_BUCKET", Some("studiofs-test"))?,
        s3_access_key: env_or_dotenv("STUDIOFS_S3_ACCESS_KEY", Some("studiofs"))?,
        s3_secret_key: env_or_dotenv("STUDIOFS_S3_SECRET_KEY", Some("studiofs123"))?,
        s3_region: env_or_dotenv("STUDIOFS_S3_REGION", Some("us-east-1"))?,
    })
}

fn random_name(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        Uuid::new_v4().simple()
    )
}

fn set_home_env(cmd: &mut Command, home: &Path) {
    cmd.env("HOME", home);
    cmd.env("USERPROFILE", home);
}

fn run_studiofs(home: &Path, args: &[&str]) -> Output {
    let exe = env!("CARGO_BIN_EXE_studiofs");
    let mut cmd = Command::new(exe);
    cmd.args(args);
    set_home_env(&mut cmd, home);
    cmd.output().expect("run studiofs command")
}

fn assert_ok(output: &Output, context: &str) {
    if output.status.success() {
        return;
    }
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    panic!("{context} failed:\n{text}");
}

fn default_meta_dir(home: &Path, name: &str) -> PathBuf {
    home.join(".studiofs").join("meta").join(name)
}

fn resolved_pg_dsn(raw: &str) -> String {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async { ensure_studiofs_database(raw).await })
        .expect("resolve pg dsn")
        .metadata_dsn
}

fn delete_volume_by_name(pg_dsn: &str, name: &str) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .execute("DELETE FROM volumes WHERE name = $1", &[&name])
            .await
            .expect("delete volume by name");
        conn_task.abort();
    });
}

fn volume_exists(pg_dsn: &str, name: &str) -> bool {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(pg_dsn, tokio_postgres::NoTls)
            .await
            .expect("connect postgres");
        let conn_task = tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM volumes WHERE name = $1)",
                &[&name],
            )
            .await
            .expect("query volume exists");
        conn_task.abort();
        row.get::<_, bool>(0)
    })
}

fn seed_remote_file(home: &Path, disk_name: &str, payload: &[u8]) {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    {
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
            },
        )
        .expect("open meta engine");
        let format_info = meta.get_format_info().expect("get format info");

        let file = meta
            .create_file(ROOT_INO, b"payload.bin", 0, 0, 0o644)
            .expect("create file");
        let object_id = meta.next_block_id().expect("next block id");
        let block = BlockRef::new_partial(object_id, payload.len() as u32);
        meta.set_file_block(file.ino, 0, &block)
            .expect("set file block");
        meta.commit_uploaded_block(file.ino, 0, 0, &block)
            .expect("commit uploaded block");
        meta.publish_file_manifest(file.ino, 0)
            .expect("publish file manifest");

        let mut attr = meta.get_attr(file.ino).expect("get attr");
        attr.size = payload.len() as u64;
        attr.committed_size = payload.len() as u64;
        meta.set_attr(&attr).expect("set attr");

        let object = create_object(payload).expect("encode object");
        let key = format_object_key(&format_info.data_prefix, object_id);
        let store = DataStore::new(&DataStoreConfig::from(&format_info)).expect("datastore");
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        rt.block_on(async { store.put_by_key(&key, Bytes::from(object)).await })
            .expect("put object");

        meta.flush_metadata().expect("flush metadata");
        // The next step shells out to `studiofs dump`, which re-opens this same
        // redb file. Ensure the local probe handle is dropped before that.
        drop(meta);
    }
}

fn verify_payload_from_discovered(home: &Path, disk_name: &str, expected: &[u8]) {
    let db_path = default_meta_dir(home, disk_name).join(format!("{disk_name}.db"));
    {
        let meta = open_meta_engine_with_options(
            &db_path,
            MetaOpenOptions {
                background_poll: false,
            },
        )
        .expect("open discovered meta");
        let format_info = meta.get_format_info().expect("format info");
        let entry = meta
            .lookup(ROOT_INO, b"payload.bin")
            .expect("lookup payload.bin");
        let attr = meta.get_attr(entry.ino).expect("get payload attr");
        assert_eq!(attr.committed_size, expected.len() as u64);

        let block = meta
            .get_file_block(entry.ino, 0)
            .expect("get file block")
            .expect("block 0 exists");
        let key = format_object_key(&format_info.data_prefix, block.object_id);
        let store = DataStore::new(&DataStoreConfig::from(&format_info)).expect("datastore");
        let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
        let raw = rt
            .block_on(async { store.get_by_key(&key).await })
            .expect("get object");
        let decoded = read_object(&raw).expect("decode object");
        assert_eq!(decoded, expected);
        drop(meta);
    }
}

#[test]
#[ignore]
fn dump_load_restores_volume_and_discovery_reads_data() {
    let Some(env) = load_test_env() else {
        eprintln!("skipping: env not ready");
        return;
    };
    let Some(raw_pg) = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None) else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN is not set");
        return;
    };

    let disk_name = random_name("dump-load-a");
    let home_a = tempfile::tempdir().expect("home_a");
    let dump_path = home_a.path().join(format!("{disk_name}.dump.json"));
    let payload = b"studiofs-dump-load-payload-a";

    let init = run_studiofs(
        home_a.path(),
        &[
            "init",
            &disk_name,
            "--endpoint",
            &env.s3_endpoint,
            "--bucket",
            &env.s3_bucket,
            "--access-key",
            &env.s3_access_key,
            "--secret-key",
            &env.s3_secret_key,
            "--region",
            &env.s3_region,
            "--meta",
            &raw_pg,
        ],
    );
    assert_ok(&init, "init");

    seed_remote_file(home_a.path(), &disk_name, payload);

    let dump = run_studiofs(
        home_a.path(),
        &[
            "dump",
            &disk_name,
            "--path-to-dump",
            dump_path.to_str().unwrap(),
        ],
    );
    assert_ok(&dump, "dump");

    let pg_dsn = resolved_pg_dsn(&raw_pg);
    delete_volume_by_name(&pg_dsn, &disk_name);
    assert!(
        !volume_exists(&pg_dsn, &disk_name),
        "volume must be deleted"
    );

    let load = run_studiofs(
        home_a.path(),
        &[
            "load",
            "--path-to-dump",
            dump_path.to_str().unwrap(),
            "--postgresql-adress",
            &raw_pg,
        ],
    );
    assert_ok(&load, "load");
    assert!(
        volume_exists(&pg_dsn, &disk_name),
        "volume must be restored"
    );

    let home_b = tempfile::tempdir().expect("home_b");
    let discover = run_studiofs(home_b.path(), &["discover", &disk_name, "--meta", &raw_pg]);
    assert_ok(&discover, "discover after load");
    verify_payload_from_discovered(home_b.path(), &disk_name, payload);
}

#[test]
#[ignore]
fn dump_load_restores_target_volume_without_breaking_other_volumes() {
    let Some(env) = load_test_env() else {
        eprintln!("skipping: env not ready");
        return;
    };
    let Some(raw_pg) = env_or_dotenv("STUDIOFS_TEST_PG_DSN", None) else {
        eprintln!("skipping: STUDIOFS_TEST_PG_DSN is not set");
        return;
    };

    let target = random_name("dump-load-target");
    let extra1 = random_name("dump-load-extra1");
    let extra2 = random_name("dump-load-extra2");
    let home = tempfile::tempdir().expect("home");
    let dump_path = home.path().join(format!("{target}.dump.json"));
    let payload = b"studiofs-dump-load-payload-b";

    let init_target = run_studiofs(
        home.path(),
        &[
            "init",
            &target,
            "--endpoint",
            &env.s3_endpoint,
            "--bucket",
            &env.s3_bucket,
            "--access-key",
            &env.s3_access_key,
            "--secret-key",
            &env.s3_secret_key,
            "--region",
            &env.s3_region,
            "--meta",
            &raw_pg,
        ],
    );
    assert_ok(&init_target, "init target");
    seed_remote_file(home.path(), &target, payload);

    let dump = run_studiofs(
        home.path(),
        &[
            "dump",
            &target,
            "--path-to-dump",
            dump_path.to_str().unwrap(),
        ],
    );
    assert_ok(&dump, "dump target");

    let pg_dsn = resolved_pg_dsn(&raw_pg);
    delete_volume_by_name(&pg_dsn, &target);

    for disk in [&extra1, &extra2] {
        let out = run_studiofs(
            home.path(),
            &[
                "init",
                disk,
                "--endpoint",
                &env.s3_endpoint,
                "--bucket",
                &env.s3_bucket,
                "--access-key",
                &env.s3_access_key,
                "--secret-key",
                &env.s3_secret_key,
                "--region",
                &env.s3_region,
                "--meta",
                &raw_pg,
            ],
        );
        assert_ok(&out, "init extra");
    }

    let load = run_studiofs(
        home.path(),
        &[
            "load",
            "--path-to-dump",
            dump_path.to_str().unwrap(),
            "--postgresql-adress",
            &raw_pg,
        ],
    );
    assert_ok(&load, "load target");

    let verify_home = tempfile::tempdir().expect("verify home");
    let discover_target = run_studiofs(
        verify_home.path(),
        &["discover", &target, "--meta", &raw_pg],
    );
    assert_ok(&discover_target, "discover restored target");
    verify_payload_from_discovered(verify_home.path(), &target, payload);

    for disk in [&extra1, &extra2] {
        let out = run_studiofs(verify_home.path(), &["discover", disk, "--meta", &raw_pg]);
        assert_ok(&out, "discover extra volume");
        let db_path = default_meta_dir(verify_home.path(), disk).join(format!("{disk}.db"));
        let meta = open_meta_engine(&db_path).expect("open extra meta");
        let root = meta.get_attr(ROOT_INO).expect("extra root inode");
        assert_eq!(root.ino, ROOT_INO);
    }
}
