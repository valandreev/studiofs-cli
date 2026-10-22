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

const VALID_SNAPSHOT_COMMANDS: &[&[&str]] = &[
    &["create"],
    &["list"],
    &["inspect", "volume.2026-08-11T20-58-31.482Z"],
    &["delete", "volume.2026-08-11T20-58-31.482Z"],
    &["policy", "set", "--daily-at", "02:30", "--retention", "7d"],
    &["policy", "show"],
    &["policy", "cancel"],
];

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

fn has_snapshot_usage(text: &str) -> bool {
    text.contains("snapshot") && text.contains("<VOLUME_NAME>") && text.contains("<COMMAND>")
}

#[test]
fn snapshot_help_requires_a_volume_name_before_the_command() {
    let root = studiofs_test_support::workspace_target_tempdir("snapshot-cli-help-");
    let output = run_snapshot(root.path(), &["--help"]);
    let text = output_text(&output);

    assert!(output.status.success(), "snapshot --help failed: {text}");
    assert!(
        has_snapshot_usage(&text),
        "help does not show the required volume name: {text}"
    );
}

#[test]
fn snapshot_operations_require_a_volume_name() {
    let root = studiofs_test_support::workspace_target_tempdir("snapshot-cli-no-name-");

    for command in VALID_SNAPSHOT_COMMANDS {
        let output = run_snapshot(root.path(), command);
        let text = output_text(&output);
        assert!(
            !output.status.success(),
            "snapshot accepted a command without a volume name: {command:?}"
        );
        assert!(
            text.contains("usage:") || text.contains("Usage:"),
            "missing-name error did not show command usage: {text}"
        );
    }
}

#[test]
fn snapshot_operations_reject_invalid_names_before_side_effects() {
    let root = studiofs_test_support::workspace_target_tempdir("snapshot-cli-invalid-");

    for command in VALID_SNAPSHOT_COMMANDS {
        let args = std::iter::once("bad:name")
            .chain(command.iter().copied())
            .collect::<Vec<_>>();
        let output = run_snapshot(root.path(), &args);
        let text = output_text(&output);

        assert!(
            !output.status.success(),
            "snapshot accepted an invalid volume name for {command:?}: {text}"
        );
        assert!(
            text.contains("invalid volume name"),
            "snapshot did not report an invalid volume name for {command:?}: {text}"
        );
        assert!(
            !text.contains("could not connect") && !text.contains("Do you want to proceed?"),
            "snapshot performed work before rejecting the invalid name: {text}"
        );
    }
}

#[test]
fn snapshot_operations_report_unknown_named_volumes_without_scanning_other_metadata() {
    for command in VALID_SNAPSHOT_COMMANDS {
        let root = studiofs_test_support::workspace_target_tempdir("snapshot-cli-unknown-");
        let unrelated_db = root
            .path()
            .join("meta")
            .join("unrelated")
            .join("unrelated.db");
        fs::create_dir_all(unrelated_db.parent().expect("metadata directory"))
            .expect("create unrelated metadata directory");
        fs::write(&unrelated_db, b"not a redb database").expect("write unrelated corrupt database");

        let args = std::iter::once("unknown-volume")
            .chain(command.iter().copied())
            .collect::<Vec<_>>();
        let output = run_snapshot(root.path(), &args);
        let text = output_text(&output);

        assert!(
            !output.status.success(),
            "snapshot accepted unknown volume for {command:?}: {text}"
        );
        assert!(
            text.contains("no local metadata found for snapshot volume 'unknown-volume'"),
            "snapshot did not report the explicitly named missing volume for {command:?}: {text}"
        );
        assert!(
            has_snapshot_usage(&text),
            "unknown-volume error omitted the command usage for {command:?}: {text}"
        );
        assert!(
            !text.contains("corrupt or unsupported") && !text.contains("unrelated"),
            "snapshot inspected unrelated metadata for {command:?}: {text}"
        );
    }
}

fn configured_test_pg_dsn() -> Option<String> {
    std::env::var("STUDIOFS_TEST_PG_DSN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?.parent()?;
            dotenvy::from_path_iter(workspace_root.join(".env"))
                .ok()?
                .find_map(|entry| match entry.ok()? {
                    (key, value) if key == "STUDIOFS_TEST_PG_DSN" => Some(value),
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
            let dsn = self.dsn.clone();
            let names = self.names.clone();
            let _ = runtime.block_on(async move {
                let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
                    .await
                    .ok()?;
                let connection_task = tokio::spawn(connection);
                let result = client
                    .execute("DELETE FROM volumes WHERE name = ANY($1)", &[&names])
                    .await;
                connection_task.abort();
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
#[ignore = "requires PostgreSQL test service via STUDIOFS_TEST_PG_DSN; does not use S3"]
fn snapshot_cli_scopes_catalog_and_policy_to_the_named_volume() {
    let raw_dsn = configured_test_pg_dsn()
        .expect("STUDIOFS_TEST_PG_DSN must be set or configured in workspace .env");
    let runtime = tokio::runtime::Runtime::new().expect("create test runtime");
    let dsn = runtime
        .block_on(ensure_studiofs_database(&raw_dsn))
        .expect("prepare PostgreSQL test database")
        .metadata_dsn;
    let (client, connection) = runtime
        .block_on(tokio_postgres::connect(&dsn, tokio_postgres::NoTls))
        .expect("connect to PostgreSQL test database");
    let connection_task = runtime.spawn(connection);
    runtime
        .block_on(studiofs_meta::pg_engine::schema::ensure_studiofs_pg_schema(
            &client,
        ))
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
    connection_task.abort();
}
