//! Process-level checks for explicit snapshot volume selection.

use std::fs;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use chrono::NaiveTime;
use sha2::{Digest, Sha256};
use studiofs_meta::snapshot::{SnapshotDailyPolicy, SnapshotOrigin, SnapshotRetention};
use studiofs_meta::snapshot_ports::{SnapshotJobStore, SnapshotPolicyStore};
use studiofs_meta::{
    CURRENT_FORMAT_VERSION, DEFAULT_BLOCK_SIZE, EncryptionConfig, FormatInfo, MetaFormat,
    MetadataBackend, MetadataConfig, PgSnapshotBackend, PostgresConfig, RedbMetaEngine,
    ensure_studiofs_database,
};
use uuid::Uuid;

fn run_snapshot(root: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_studiofs"))
        .arg("snapshot")
        .args(args)
        .env("STUDIOFS_CLIENT_ROOT", root)
        .env("HOME", root)
        .env("USERPROFILE", root)
        .env_remove("STUDIOFS_DAEMON_LOG_DIR")
        .env_remove("STUDIOFS_DAEMON_LOG_STEM")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run studiofs process")
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn configured_test_pg_dsn() -> Option<String> {
    configured_test_setting("STUDIOFS_TEST_PG_DSN")
}

fn configured_test_setting(setting: &str) -> Option<String> {
    std::env::var(setting)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?.parent()?;
            dotenvy::from_path_iter(workspace_root.join(".env"))
                .ok()?
                .find_map(|entry| match entry.ok()? {
                    (key, value) if key == setting => Some(value),
                    _ => None,
                })
                .filter(|value| !value.trim().is_empty())
        })
}

struct PgVolumeCleanup {
    dsn: String,
    names: Vec<String>,
}

impl Drop for PgVolumeCleanup {
    fn drop(&mut self) {
        if let Ok(runtime) = tokio::runtime::Runtime::new() {
            let _ = runtime.block_on(async {
                let (client, connection) =
                    tokio_postgres::connect(&self.dsn, tokio_postgres::NoTls)
                        .await
                        .ok()?;
                let connection_task = tokio::spawn(connection);
                let result = client
                    .execute("DELETE FROM volumes WHERE name = ANY($1)", &[&self.names])
                    .await;
                // Dropping the client lets the connection terminate cleanly before joining it.
                drop(client);
                let _ = connection_task.await;
                result.ok()
            });
        }
    }
}

async fn insert_pg_volume(client: &tokio_postgres::Client, name: &str) -> (i64, String) {
    let uuid = Uuid::new_v4().to_string();
    let row = client
        .query_one(
            "INSERT INTO volumes (name, uuid, block_size) VALUES ($1, $2, $3) RETURNING vol",
            &[&name, &uuid, &(DEFAULT_BLOCK_SIZE as i32)],
        )
        .await
        .expect("insert PostgreSQL volume");
    let volume_id = row.get::<_, i64>(0);
    (volume_id, uuid)
}

async fn seed_pg_metadata_state(
    client: &tokio_postgres::Client,
    volume_id: i64,
    checkpoint: &[u8],
    format_info: &[u8],
) {
    let format_version =
        i16::try_from(CURRENT_FORMAT_VERSION).expect("format version fits PostgreSQL SMALLINT");
    let checksum = Sha256::digest(checkpoint).to_vec();
    client
        .execute(
            r#"
            INSERT INTO metadata_state (
                vol, metadata_format_version, version, checkpoint_version, snapshot,
                checkpoint_sha256, format_info, checkpoint_publication_state, checkpoint_generation
            ) VALUES ($1, $2, 0, 0, $3, $4, $5, 'published', 1)
            "#,
            &[
                &volume_id,
                &format_version,
                &checkpoint,
                &checksum,
                &format_info,
            ],
        )
        .await
        .expect("seed PostgreSQL metadata state");
}

fn create_local_volume_metadata(
    root: &Path,
    name: &str,
    dsn: &str,
    volume_id: i64,
    uuid: &str,
) -> (Vec<u8>, Vec<u8>) {
    let meta_dir = root.join("meta").join(name);
    fs::create_dir_all(&meta_dir).expect("create local metadata directory");
    let db =
        RedbMetaEngine::open(meta_dir.join(format!("{name}.db"))).expect("open local metadata");
    db.init_root(0, 0).expect("initialize local root");
    let info = FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.to_string(),
        uuid: uuid.to_string(),
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "unused-test-bucket".to_string(),
        access_key: "unused-test-access-key".to_string(),
        secret_key: "unused-test-secret-key".to_string(),
        region: "us-east-1".to_string(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig {
            backend: MetadataBackend::Postgres,
            remote: None,
            postgres: Some(PostgresConfig {
                url: dsn.to_string(),
                volume_id,
            }),
        },
        encryption: EncryptionConfig::new([0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH]),
        block_size: DEFAULT_BLOCK_SIZE,
    };
    db.set_format_info(&info)
        .expect("write local volume format info");
    let checkpoint = db
        .export_serving_checkpoint_bytes()
        .expect("export valid serving checkpoint");
    let format_info = serde_json::to_vec(&info).expect("serialize volume format info");
    (checkpoint, format_info)
}

#[test]
#[ignore = "requires the PostgreSQL test service"]
fn upgrade_cli_restarts_after_shared_success_and_preserves_local_journal() {
    exercise_upgrade_retry(false);
}

#[test]
#[ignore = "requires PostgreSQL and RustFS test services"]
fn upgrade_cli_preserves_rustfs_payload_checksum() {
    exercise_upgrade_retry(true);
}

fn exercise_upgrade_retry(with_rustfs: bool) {
    use studiofs_data::store::DataError;
    use studiofs_data::{DataStore, DataStoreConfig};
    use studiofs_meta::{MetaFsOps, MetaInodes};

    struct PayloadCleanup<'a> {
        runtime: &'a tokio::runtime::Runtime,
        store: DataStore,
        object_id: &'a str,
    }

    impl Drop for PayloadCleanup<'_> {
        fn drop(&mut self) {
            if self
                .runtime
                .block_on(self.store.delete_object(self.object_id))
                .is_err()
            {
                eprintln!("could not remove the upgrade test payload");
            }
        }
    }

    let raw_dsn = configured_test_pg_dsn().expect("configured PostgreSQL test service");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dsn = runtime
        .block_on(ensure_studiofs_database(&raw_dsn))
        .unwrap()
        .metadata_dsn;
    let (mut client, connection) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .unwrap();
    let connection_task = runtime.spawn(connection);
    runtime
        .block_on(
            studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema_committed(&mut client),
        )
        .unwrap();
    let name = format!("upgrade-cli-{}", Uuid::new_v4().simple());
    let _cleanup = PgVolumeCleanup {
        dsn: dsn.clone(),
        names: vec![name.clone()],
    };
    let (id, uuid) = runtime.block_on(insert_pg_volume(&client, &name));
    let root = studiofs_test_support::workspace_target_tempdir("upgrade-cli-");
    let (checkpoint, bytes) = create_local_volume_metadata(root.path(), &name, &dsn, id, &uuid);
    runtime.block_on(seed_pg_metadata_state(&client, id, &checkpoint, &bytes));
    let mut legacy: FormatInfo = serde_json::from_slice(&bytes).unwrap();
    let contents = b"StudioFS format adoption payload fixture".as_slice();
    let payload = with_rustfs.then(|| {
        legacy.endpoint = configured_test_setting("STUDIOFS_S3_ENDPOINT").expect("RustFS endpoint");
        legacy.bucket = configured_test_setting("STUDIOFS_S3_BUCKET").expect("RustFS test bucket");
        legacy.access_key =
            configured_test_setting("STUDIOFS_S3_ACCESS_KEY").expect("RustFS access key");
        legacy.secret_key =
            configured_test_setting("STUDIOFS_S3_SECRET_KEY").expect("RustFS secret key");
        if let Some(region) = configured_test_setting("STUDIOFS_S3_REGION") {
            legacy.region = region;
        }
        let store = DataStore::new(&DataStoreConfig::from(&legacy))
            .unwrap_or_else(|_| panic!("open RustFS test bucket"));
        let object_id = "1132";
        runtime
            .block_on(store.put_object(object_id, contents))
            .unwrap_or_else(|error| match error {
                DataError::OpenDal(error) => {
                    panic!("write RustFS fixture failed: {}", error.kind())
                }
                _ => panic!("write RustFS fixture failed"),
            });
        PayloadCleanup {
            runtime: &runtime,
            store,
            object_id,
        }
    });
    legacy.format_version = 5;
    let bytes = serde_json::to_vec(&legacy).unwrap();
    runtime
        .block_on(client.execute(
            "UPDATE metadata_state SET metadata_format_version=5,format_info=$2 WHERE vol=$1",
            &[&id, &bytes],
        ))
        .unwrap();
    let path = root
        .path()
        .join("meta")
        .join(&name)
        .join(format!("{name}.db"));
    let local = RedbMetaEngine::open(&path).unwrap();
    local.set_format_info(&legacy).unwrap();
    let attr = local
        .create_file(1, b"accepted-offline", 0, 0, 0o600)
        .unwrap();
    let journal = local.read_events_range(0, u64::MAX).unwrap();
    let run = || {
        Command::new(env!("CARGO_BIN_EXE_studiofs"))
            .args(["upgrade", &name])
            .env("STUDIOFS_CLIENT_ROOT", root.path())
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    assert!(
        !run().status.success(),
        "an existing redb owner must refuse upgrade"
    );
    assert_eq!(
        runtime
            .block_on(client.query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol=$1",
                &[&id]
            ))
            .unwrap()
            .get::<_, i16>(0),
        5
    );
    // Force server-success/local-completion-failure without a production fault hook.
    let mut mismatched = legacy.clone();
    mismatched.data_prefix = "local-identity-mismatch".into();
    local.set_format_info(&mismatched).unwrap();
    drop(local);
    let failed = run();
    assert!(!failed.status.success());
    let failure = output_text(&failed);
    assert!(!failure.contains(&dsn));
    assert!(!failure.contains(&legacy.secret_key));
    assert!(failure.contains("Shared format is upgraded"));
    assert_eq!(
        runtime
            .block_on(client.query_one(
                "SELECT metadata_format_version FROM metadata_state WHERE vol=$1",
                &[&id]
            ))
            .unwrap()
            .get::<_, i16>(0),
        i16::try_from(CURRENT_FORMAT_VERSION).unwrap()
    );
    assert!(
        RedbMetaEngine::open(&path).is_err(),
        "normal mount opening must remain fenced"
    );
    let local = RedbMetaEngine::open_for_format_upgrade(&path).unwrap();
    assert_eq!(local.format_info_for_upgrade().unwrap().format_version, 5);
    assert_eq!(local.read_events_range(0, u64::MAX).unwrap(), journal);
    local.set_format_info(&legacy).unwrap();
    drop(local);
    for _ in 0..2 {
        let output = run();
        assert!(output.status.success(), "upgrade retry must succeed");
    }
    let local = RedbMetaEngine::open(&path).unwrap();
    assert_eq!(
        local.get_format_info().unwrap().format_version,
        CURRENT_FORMAT_VERSION
    );
    assert_eq!(local.get_attr(attr.ino).unwrap(), attr);
    assert_eq!(local.read_events_range(0, u64::MAX).unwrap(), journal);
    if let Some(payload) = payload {
        let persisted = runtime
            .block_on(payload.store.get_object(payload.object_id))
            .unwrap_or_else(|_| panic!("read existing RustFS object after adoption"));
        assert_eq!(Sha256::digest(persisted.to_vec()), Sha256::digest(contents));
    }
    drop(local);
    drop(client);
    let _ = runtime.block_on(connection_task);
}

#[test]
#[ignore = "requires PostgreSQL test service; isolated trash-days command, no S3"]
fn trash_days_cli_updates_shared_setting_and_client_converges() {
    let raw_dsn = configured_test_pg_dsn().expect("configured PostgreSQL test service");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dsn = runtime
        .block_on(ensure_studiofs_database(&raw_dsn))
        .unwrap()
        .metadata_dsn;
    let (mut client, connection) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .unwrap();
    let connection_task = runtime.spawn(connection);
    runtime
        .block_on(
            studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema_committed(&mut client),
        )
        .unwrap();
    let name = format!("trash-days-cli-{}", Uuid::new_v4().simple());
    let _cleanup = PgVolumeCleanup {
        dsn: dsn.clone(),
        names: vec![name.clone()],
    };
    let (id, uuid) = runtime.block_on(insert_pg_volume(&client, &name));
    let root = studiofs_test_support::workspace_target_tempdir("trash-days-cli-");
    let (checkpoint, bytes) = create_local_volume_metadata(root.path(), &name, &dsn, id, &uuid);
    runtime.block_on(seed_pg_metadata_state(&client, id, &checkpoint, &bytes));
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_studiofs"))
            .arg("trash-days")
            .arg(&name)
            .args(args)
            .env("STUDIOFS_CLIENT_ROOT", root.path())
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    let shared_revision = || {
        let bytes: Option<Vec<u8>> = runtime
            .block_on(client.query_one(
                "SELECT volume_settings FROM metadata_state WHERE vol=$1",
                &[&id],
            ))
            .unwrap()
            .get(0);
        bytes.map(|bytes| {
            serde_json::from_slice::<studiofs_meta::trash::VolumeSettings>(&bytes).unwrap()
        })
    };

    let shown = run(&[]);
    assert!(shown.status.success(), "{}", output_text(&shown));
    assert!(output_text(&shown).contains("trash is disabled (0 days)"));
    for invalid in ["1.5", "4294967296", "-1"] {
        assert!(!run(&[invalid]).status.success(), "{invalid} must fail");
    }
    assert_eq!(shared_revision(), None, "invalid input must not write");

    let enabled = run(&["7"]);
    assert!(enabled.status.success(), "{}", output_text(&enabled));
    assert!(output_text(&enabled).contains("for 7 day(s)"));
    let disabled = run(&["0"]);
    assert!(disabled.status.success(), "{}", output_text(&disabled));
    assert!(
        output_text(&disabled).contains("existing entries still expire after 7 day(s)"),
        "{}",
        output_text(&disabled)
    );
    assert!(!output_text(&disabled).contains(&dsn));
    let shared = shared_revision().unwrap();
    assert_eq!(shared.revision, 2);
    assert!(!shared.trash.enabled());
    assert_eq!(shared.trash.existing_entry_days(), 7);

    // A real client replays the two ordered tag-19 events into its local view.
    let path = root
        .path()
        .join("meta")
        .join(&name)
        .join(format!("{name}.db"));
    let engine = studiofs_meta::open_meta_engine_with_options(
        &path,
        studiofs_meta::MetaOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .unwrap();
    engine.sync_metadata().unwrap();
    let local = engine.retained_store().unwrap().volume_settings().unwrap();
    assert_eq!(local, shared);
    drop(engine);
    drop(client);
    let _ = runtime.block_on(connection_task);
}

#[test]
#[ignore = "requires PostgreSQL test service; isolated compression command, no S3"]
fn compression_cli_updates_shared_setting_handles_cas_conflict_and_converges() {
    use studiofs_meta::trash::{CompressionPolicy, VolumeSettings};

    let raw_dsn = configured_test_pg_dsn().expect("configured PostgreSQL test service");
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dsn = runtime
        .block_on(ensure_studiofs_database(&raw_dsn))
        .unwrap()
        .metadata_dsn;
    let (mut client, connection) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .unwrap();
    let connection_task = runtime.spawn(connection);
    runtime
        .block_on(
            studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema_committed(&mut client),
        )
        .unwrap();
    let name = format!("compression-cli-{}", Uuid::new_v4().simple());
    let _cleanup = PgVolumeCleanup {
        dsn: dsn.clone(),
        names: vec![name.clone()],
    };
    let (id, uuid) = runtime.block_on(insert_pg_volume(&client, &name));
    let root = studiofs_test_support::workspace_target_tempdir("compression-cli-");
    let (checkpoint, bytes) = create_local_volume_metadata(root.path(), &name, &dsn, id, &uuid);
    runtime.block_on(seed_pg_metadata_state(&client, id, &checkpoint, &bytes));
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_studiofs"))
            .arg("compression")
            .arg(&name)
            .args(args)
            .env("STUDIOFS_CLIENT_ROOT", root.path())
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    let shared_settings = |client: &tokio_postgres::Client| {
        let bytes: Option<Vec<u8>> = runtime
            .block_on(client.query_one(
                "SELECT volume_settings FROM metadata_state WHERE vol=$1",
                &[&id],
            ))
            .unwrap()
            .get(0);
        bytes.map(|bytes| serde_json::from_slice::<VolumeSettings>(&bytes).unwrap())
    };

    // Absent volume_settings reads as off.
    let shown = run(&[]);
    assert!(shown.status.success(), "{}", output_text(&shown));
    assert!(output_text(&shown).contains("compression is off."));

    // Invalid policies are rejected without touching PostgreSQL.
    for invalid in ["zstd:0", "zstd:20", "lz4", "zstd:", "zstd:-1"] {
        assert!(!run(&[invalid]).status.success(), "{invalid} must fail");
    }
    assert_eq!(
        shared_settings(&client),
        None,
        "invalid input must not write"
    );

    // `zstd` -> zstd:3, `zstd:9` -> zstd:9, `off` -> off.
    let default_zstd = run(&["zstd"]);
    assert!(
        default_zstd.status.success(),
        "{}",
        output_text(&default_zstd)
    );
    assert!(output_text(&default_zstd).contains("compression is zstd:3."));

    let level9 = run(&["zstd:9"]);
    assert!(level9.status.success(), "{}", output_text(&level9));
    assert!(output_text(&level9).contains("compression is zstd:9."));

    let off = run(&["off"]);
    assert!(off.status.success(), "{}", output_text(&off));
    assert!(output_text(&off).contains("compression is off."));
    assert!(!output_text(&off).contains(&dsn));

    let shared = shared_settings(&client).unwrap();
    assert_eq!(shared.revision, 3);
    assert_eq!(shared.compression, CompressionPolicy::off());

    // Deterministic concurrent CAS conflict: hold the metadata_state row lock while the CLI
    // reads revision 3 and waits on SELECT ... FOR UPDATE, then advance revision to 4 and commit.
    let (observer, observer_conn) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .unwrap();
    let observer_task = runtime.spawn(observer_conn);
    let cli_output = runtime.block_on(async {
        let tx = client.transaction().await.unwrap();
        let holder_pid: i32 = tx
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .unwrap()
            .get(0);
        tx.query_one(
            "SELECT 1 FROM metadata_state WHERE vol=$1 FOR UPDATE",
            &[&id],
        )
        .await
        .unwrap();

        let child = Command::new(env!("CARGO_BIN_EXE_studiofs"))
            .arg("compression")
            .arg(&name)
            .arg("zstd:12")
            .env("STUDIOFS_CLIENT_ROOT", root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        // Wait until the CLI's FOR UPDATE query is blocked on our transaction lock.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let mut blocked = false;
        while std::time::Instant::now() < deadline {
            let waiting: i64 = observer
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity \
                     WHERE $1 = ANY(pg_blocking_pids(pid)) \
                       AND query LIKE '%FOR UPDATE%' \
                       AND query LIKE '%metadata_state%'",
                    &[&holder_pid],
                )
                .await
                .unwrap()
                .get(0);
            if waiting > 0 {
                blocked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            blocked,
            "CLI compression command did not block on metadata_state FOR UPDATE in time"
        );

        let mut concurrent = shared;
        concurrent.revision += 1;
        concurrent.compression = CompressionPolicy::zstd(5).unwrap();
        let concurrent_bytes = serde_json::to_vec(&concurrent).unwrap();
        tx.execute(
            "UPDATE metadata_state SET volume_settings=$2 WHERE vol=$1",
            &[&id, &concurrent_bytes],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        child.wait_with_output().unwrap()
    });
    drop(observer);
    let _ = runtime.block_on(observer_task);

    assert!(
        !cli_output.status.success(),
        "concurrent CAS conflict must fail: {}",
        output_text(&cli_output)
    );
    assert!(
        output_text(&cli_output).contains("volume settings changed concurrently"),
        "{}",
        output_text(&cli_output)
    );
    let after_conflict = shared_settings(&client).unwrap();
    assert_eq!(after_conflict.revision, 4);
    assert_eq!(
        after_conflict.compression,
        CompressionPolicy::zstd(5).unwrap()
    );

    // Apply a final clean CLI update and verify a client's redb view converges via replication.
    let final_set = run(&["zstd:7"]);
    assert!(final_set.status.success(), "{}", output_text(&final_set));
    let expected = shared_settings(&client).unwrap();
    assert_eq!(expected.revision, 5);
    assert_eq!(expected.compression, CompressionPolicy::zstd(7).unwrap());

    let path = root
        .path()
        .join("meta")
        .join(&name)
        .join(format!("{name}.db"));
    let engine = studiofs_meta::open_meta_engine_with_options(
        &path,
        studiofs_meta::MetaOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )
    .unwrap();
    engine.sync_metadata().unwrap();
    let local = engine.retained_store().unwrap().volume_settings().unwrap();
    assert_eq!(local, expected);
    drop(engine);

    let mut future_settings = serde_json::to_value(expected).unwrap();
    future_settings["future_policy"] = serde_json::json!(true);
    let future_bytes = serde_json::to_vec(&future_settings).unwrap();
    runtime
        .block_on(client.execute(
            "UPDATE metadata_state SET volume_settings=$2 WHERE vol=$1",
            &[&id, &future_bytes],
        ))
        .unwrap();
    for args in [&[][..], &["off"][..]] {
        let output = run(args);
        let message = output_text(&output);
        assert!(!output.status.success(), "{message}");
        assert!(
            message.contains("upgrade required: unknown volume settings field or variant"),
            "{message}"
        );
        assert!(
            !message.contains("check PostgreSQL connectivity"),
            "{message}"
        );
    }
    let error = runtime
        .block_on(studiofs_meta::pg_engine::set_shared_compression(
            &mut client,
            id,
            Some(expected.revision),
            CompressionPolicy::off(),
            "compression-cli-fixture",
            0,
        ))
        .unwrap_err();
    assert!(matches!(
        error,
        studiofs_meta::MetaError::UpgradeRequired {
            reason: studiofs_meta::UpgradeRequiredReason::VolumeSettingsUnknown
        }
    ));
    let stored_bytes: Vec<u8> = runtime
        .block_on(client.query_one(
            "SELECT volume_settings FROM metadata_state WHERE vol=$1",
            &[&id],
        ))
        .unwrap()
        .get(0);
    assert_eq!(stored_bytes, future_bytes);

    drop(client);
    let _ = runtime.block_on(connection_task);
}

#[test]
#[ignore = "requires PostgreSQL test service via STUDIOFS_TEST_PG_DSN; does not use S3"]
fn snapshot_cli_scopes_catalog_and_policy_to_the_named_volume() {
    let raw_dsn = configured_test_pg_dsn()
        .expect("STUDIOFS_TEST_PG_DSN must be set or configured in workspace .env");
    let runtime = tokio::runtime::Runtime::new().expect("create test runtime");
    let dsn = runtime
        .block_on(ensure_studiofs_database(&raw_dsn))
        .expect("prepare PostgreSQL test database")
        .metadata_dsn;
    let (mut client, connection) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .expect("connect to PostgreSQL test database");
    let connection_task = runtime.spawn(connection);
    runtime
        .block_on(
            studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema_committed(&mut client),
        )
        .expect("ensure StudioFS PostgreSQL schema");

    let suffix = Uuid::new_v4().simple().to_string();
    let alpha_name = format!("snapshot-cli-a-{suffix}");
    let beta_name = format!("snapshot-cli-b-{suffix}");
    let _cleanup = PgVolumeCleanup {
        dsn: dsn.clone(),
        names: vec![alpha_name.clone(), beta_name.clone()],
    };
    let (alpha_id, alpha_uuid) = runtime.block_on(insert_pg_volume(&client, &alpha_name));
    let (beta_id, beta_uuid) = runtime.block_on(insert_pg_volume(&client, &beta_name));

    let root = studiofs_test_support::workspace_target_tempdir("snapshot-cli-pg-");
    let (alpha_checkpoint, alpha_format_info) =
        create_local_volume_metadata(root.path(), &alpha_name, &dsn, alpha_id, &alpha_uuid);
    let (beta_checkpoint, beta_format_info) =
        create_local_volume_metadata(root.path(), &beta_name, &dsn, beta_id, &beta_uuid);
    runtime.block_on(seed_pg_metadata_state(
        &client,
        alpha_id,
        &alpha_checkpoint,
        &alpha_format_info,
    ));
    runtime.block_on(seed_pg_metadata_state(
        &client,
        beta_id,
        &beta_checkpoint,
        &beta_format_info,
    ));
    drop(client);
    let _ = runtime.block_on(connection_task);
    let alpha =
        PgSnapshotBackend::open(&dsn, alpha_id).expect("open first volume snapshot backend");
    let beta = PgSnapshotBackend::open(&dsn, beta_id).expect("open second volume snapshot backend");

    let alpha_entry = alpha
        .create_snapshot_v2(
            SnapshotOrigin::Manual,
            SnapshotRetention::manual(None).expect("valid manual retention"),
        )
        .expect("register first volume snapshot");
    let beta_entry = beta
        .create_snapshot_v2(
            SnapshotOrigin::Manual,
            SnapshotRetention::manual(None).expect("valid manual retention"),
        )
        .expect("register second volume snapshot");

    let alpha_list = run_snapshot(root.path(), &[alpha_name.as_str(), "list"]);
    assert!(
        alpha_list.status.success(),
        "alpha list failed: {}",
        output_text(&alpha_list)
    );
    let alpha_text = output_text(&alpha_list);
    assert!(alpha_text.contains(alpha_entry.snapshot_id.as_str()));
    assert!(!alpha_text.contains(beta_entry.snapshot_id.as_str()));

    let beta_list = run_snapshot(root.path(), &[beta_name.as_str(), "list"]);
    assert!(
        beta_list.status.success(),
        "beta list failed: {}",
        output_text(&beta_list)
    );
    let beta_text = output_text(&beta_list);
    assert!(beta_text.contains(beta_entry.snapshot_id.as_str()));
    assert!(!beta_text.contains(alpha_entry.snapshot_id.as_str()));

    let policy = SnapshotDailyPolicy::new(
        NaiveTime::from_hms_opt(2, 30, 0).expect("valid daily time"),
        "UTC",
        Duration::from_secs(7 * 24 * 60 * 60),
    )
    .expect("valid daily policy");
    runtime
        .block_on(beta.set_policy(&policy, None))
        .expect("seed second volume policy");

    let alpha_policy_set = run_snapshot(
        root.path(),
        &[
            alpha_name.as_str(),
            "policy",
            "set",
            "--daily-at",
            "03:45",
            "--retention",
            "3d",
        ],
    );
    assert!(
        alpha_policy_set.status.success(),
        "alpha policy set failed: {}",
        output_text(&alpha_policy_set)
    );
    let alpha_policy = run_snapshot(root.path(), &[alpha_name.as_str(), "policy", "show"]);
    let alpha_policy_text = output_text(&alpha_policy);
    assert!(
        alpha_policy.status.success(),
        "alpha policy show failed: {alpha_policy_text}"
    );
    assert!(alpha_policy_text.contains("03:45"));
    let beta_policy = run_snapshot(root.path(), &[beta_name.as_str(), "policy", "show"]);
    let beta_policy_text = output_text(&beta_policy);
    assert!(
        beta_policy.status.success(),
        "beta policy show failed: {beta_policy_text}"
    );
    assert!(beta_policy_text.contains("02:30"));
    assert!(!beta_policy_text.contains("03:45"));

    let alpha_cancel = run_snapshot(root.path(), &[alpha_name.as_str(), "policy", "cancel"]);
    assert!(
        alpha_cancel.status.success(),
        "alpha policy cancel failed: {}",
        output_text(&alpha_cancel)
    );
    let alpha_after_cancel = run_snapshot(root.path(), &[alpha_name.as_str(), "policy", "show"]);
    let alpha_after_cancel_text = output_text(&alpha_after_cancel);
    assert!(alpha_after_cancel.status.success());
    assert!(alpha_after_cancel_text.contains("Daily snapshots: disabled"));
    let beta_after_cancel = run_snapshot(root.path(), &[beta_name.as_str(), "policy", "show"]);
    let beta_after_cancel_text = output_text(&beta_after_cancel);
    assert!(beta_after_cancel.status.success());
    assert!(beta_after_cancel_text.contains("02:30"));

    let alpha_inspect = run_snapshot(
        root.path(),
        &[
            alpha_name.as_str(),
            "inspect",
            alpha_entry.snapshot_id.as_str(),
        ],
    );
    assert!(
        alpha_inspect.status.success(),
        "alpha inspect failed: {}",
        output_text(&alpha_inspect)
    );
    let foreign_inspect = run_snapshot(
        root.path(),
        &[
            alpha_name.as_str(),
            "inspect",
            beta_entry.snapshot_id.as_str(),
        ],
    );
    let foreign_inspect_text = output_text(&foreign_inspect);
    assert!(!foreign_inspect.status.success());
    assert!(
        foreign_inspect_text.contains("snapshot_not_found"),
        "{foreign_inspect_text}"
    );

    let foreign_delete = run_snapshot(
        root.path(),
        &[
            alpha_name.as_str(),
            "delete",
            beta_entry.snapshot_id.as_str(),
        ],
    );
    assert!(!foreign_delete.status.success());
    assert!(
        output_text(&foreign_delete).contains("snapshot job not found"),
        "{}",
        output_text(&foreign_delete)
    );
    let beta_after_delete = runtime
        .block_on(beta.load_catalog(&beta_entry.snapshot_id))
        .expect("load second volume snapshot after foreign delete request");
    assert_eq!(beta_after_delete, beta_entry);
}
