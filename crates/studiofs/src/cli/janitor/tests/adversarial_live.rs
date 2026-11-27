//! Live PostgreSQL/S3 acceptance for the standalone janitor CLI.
//!
//! Uses `STUDIOFS_TEST_PG_DSN` and `STUDIOFS_S3_*` from the environment or workspace
//! `.env`. Each test uses a fresh PostgreSQL schema and a unique bucket prefix.

use super::*;
use opendal::Operator;
use opendal::services::S3;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct LiveStack {
    artifact_root: PathBuf,
    postgres: PgTestSchema,
    endpoint: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
    root: PathBuf,
}

impl LiveStack {
    fn start() -> anyhow::Result<Self> {
        let env = |key: &str, default: &str| test_env(key).unwrap_or_else(|| default.to_string());
        let run_id = format!(
            "janitor-live-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let artifact_root = studiofs_test_support::workspace_root()
            .join("target/janitor-live")
            .join(&run_id);
        std::fs::create_dir_all(&artifact_root)?;
        Ok(Self {
            postgres: live_postgres_schema()?,
            endpoint: env("STUDIOFS_S3_ENDPOINT", "http://127.0.0.1:9000"),
            bucket: env("STUDIOFS_S3_BUCKET", "studiofs-test"),
            access_key: env("STUDIOFS_S3_ACCESS_KEY", "studiofs"),
            secret_key: env("STUDIOFS_S3_SECRET_KEY", "studiofs123"),
            region: env("STUDIOFS_S3_REGION", "us-east-1"),
            root: artifact_root.join("client"),
            artifact_root,
        })
    }

    fn writer(&self) -> anyhow::Result<Operator> {
        Ok(Operator::new(
            S3::default()
                .endpoint(&self.endpoint)
                .bucket(&self.bucket)
                .access_key_id(&self.access_key)
                .secret_access_key(&self.secret_key)
                .region(&self.region)
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
        // Fixture writes retry temporary S3 errors, as the janitor store does.
        .layer(opendal::layers::RetryLayer::new())
        .finish())
    }

    fn s3<'a>(&'a self, prefix: &'a str) -> LiveS3Config<'a> {
        LiveS3Config {
            endpoint: &self.endpoint,
            bucket: &self.bucket,
            access_key: &self.access_key,
            secret_key: &self.secret_key,
            region: &self.region,
            prefix,
        }
    }

    fn cli(&self, args: &[String], client_root: &Path) -> anyhow::Result<Output> {
        let label = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let command = self.command(args, client_root)?;
        let stdout = self.artifact_root.join(format!("cli-{label}.stdout"));
        let stderr = self.artifact_root.join(format!("cli-{label}.stderr"));
        // The bounded wait polls a child process; keep the runtime's connection tasks moving.
        tokio::task::block_in_place(|| {
            run_child_bounded(command, Duration::from_secs(90), &stdout, &stderr)
        })
    }

    /// A sanitized CLI command with its client root set to `client_root`.
    fn command(&self, args: &[String], client_root: &Path) -> anyhow::Result<Command> {
        let mut command = Command::new(super::cli_binary()?);
        command.args(args).env_clear();
        for key in [
            "PATH",
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "HOME",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        command
            .env("STUDIOFS_CLIENT_ROOT", client_root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }
}

/// Kills and reaps a CLI child when a test fails before it exits.
struct OwnedCli(std::process::Child);

impl Drop for OwnedCli {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run_child_bounded(
    mut command: Command,
    timeout: Duration,
    stdout: &Path,
    stderr: &Path,
) -> anyhow::Result<Output> {
    command.stdout(std::fs::File::create(stdout)?);
    command.stderr(std::fs::File::create(stderr)?);
    let mut child = OwnedCli(command.spawn()?);
    let started = std::time::Instant::now();
    let timed_out = loop {
        if child.0.try_wait()?.is_some() {
            break false;
        }
        if started.elapsed() >= timeout {
            match child.0.kill() {
                Ok(()) => break true,
                // The child exited between the poll and the kill: a normal exit, not a timeout.
                Err(_) if child.0.try_wait()?.is_some() => break false,
                Err(error) => return Err(error.into()),
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let status = child.0.wait()?;
    let stdout = std::fs::read(stdout)?;
    let stderr = std::fs::read(stderr)?;
    anyhow::ensure!(
        !timed_out,
        "janitor CLI exceeded {timeout:?}; exit status {:?}",
        status.code()
    );
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

/// Every canonical key the CLI may delete for one object.
fn object_keys(prefix: &str, id: u64) -> Vec<String> {
    std::iter::once(studiofs_data::object::format_object_key(prefix, id))
        .chain(studiofs_data::object::format_object_fragment_keys(
            prefix, id,
        ))
        .collect()
}

/// Durable queue, upload, watermark and checkpoint state that dry-run must not change.
async fn durable_janitor_state(
    client: &tokio_postgres::Client,
    volume_id: i64,
) -> anyhow::Result<String> {
    Ok(client.query_one(
        "SELECT concat_ws('|', (SELECT string_agg(row_to_json(row)::text, ',' ORDER BY object_id) FROM snapshot_payload_gc_candidates AS row WHERE vol = $1), (SELECT string_agg(row_to_json(row)::text, ',' ORDER BY object_id) FROM snapshot_payload_upload_objects AS row WHERE vol = $1), (SELECT string_agg(row_to_json(row)::text, ',' ORDER BY payload_namespace) FROM snapshot_payload_upload_watermarks AS row WHERE vol = $1), (SELECT row_to_json(row)::text FROM snapshot_payload_sweep_checkpoints AS row WHERE vol = $1))",
        &[&volume_id],
    ).await?.get(0))
}

fn object_id(offset: u64) -> anyhow::Result<u64> {
    let base = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    Ok((base & ((1_u64 << 50) - 32)) + offset)
}

async fn seed_live_object(stack: &LiveStack, prefix: &str, id: u64) -> anyhow::Result<()> {
    let writer = stack.writer()?;
    let keys = object_keys(prefix, id);
    for key in &keys {
        writer.write(key, b"janitor fixture".to_vec()).await?;
    }
    super::wait_until_s3_keys_are_past(&writer, &keys).await
}

fn parse_events(output: &Output) -> anyhow::Result<Vec<serde_json::Value>> {
    Ok(std::str::from_utf8(&output.stdout)?
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?)
}

fn cli_args(dry_run: bool, max_deletes: u64) -> Vec<String> {
    let mut args = vec![
        "janitor".to_string(),
        "janitor-live".to_string(),
        "--output".to_string(),
        "jsonl".to_string(),
        "--full-scan".to_string(),
        "--skip-newer-minutes".to_string(),
        "0".to_string(),
        "--max-deletes".to_string(),
        max_deletes.to_string(),
    ];
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

fn summary(events: &[serde_json::Value]) -> &serde_json::Value {
    events
        .iter()
        .find(|event| event["event"] == "summary")
        .expect("janitor JSONL summary")
}

async fn connection(
    stack: &LiveStack,
) -> anyhow::Result<(
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
)> {
    let (client, connection) =
        tokio_postgres::connect(stack.postgres.dsn(), tokio_postgres::NoTls).await?;
    Ok((client, tokio::spawn(connection)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local STUDIOFS_TEST_PG_DSN and a live STUDIOFS_S3_* bucket"]
async fn live_cli_maintenance_refusal_preserves_object_and_cursor() -> anyhow::Result<()> {
    use studiofs_meta::compat::ClientCapabilities;
    use studiofs_meta::pg_engine::{read_volume_protocol, update_volume_protocol};
    use studiofs_meta::volume_protocol::FeatureState;

    let stack = LiveStack::start()?;
    let id = object_id(0)?;
    let prefix = format!("sfs1016/{id:016x}/objects/");
    seed_live_object(&stack, &prefix, id).await?;
    let meta_dir = stack.root.join("meta/janitor-live");
    let (volume, _, _) = create_live_postgres_janitor_fixture(
        stack.postgres.dsn(),
        &stack.s3(&prefix),
        &meta_dir,
        id,
    )
    .await?;
    let (mut client, task) = connection(&stack).await?;
    client
        .execute(
            "INSERT INTO snapshot_payload_sweep_checkpoints
         (vol, schema_version, volume_uuid, endpoint, bucket, data_prefix, listing_strategy)
         SELECT vol, 1, uuid, $2, $3, $4, 'start_after' FROM volumes WHERE vol = $1",
            &[&volume, &stack.endpoint, &stack.bucket, &prefix],
        )
        .await?;
    let before = durable_janitor_state(&client, volume).await?;

    let capability = "maintenance.payload_gc.v3";
    let mut actor = ClientCapabilities::current();
    actor.maintain.insert(capability);
    let mut policy = read_volume_protocol(&client, volume).await?;
    for state in [
        FeatureState::Available,
        FeatureState::Compatible,
        FeatureState::Required,
    ] {
        policy
            .requirements
            .features
            .insert(capability.to_owned(), state);
        if state == FeatureState::Required {
            policy.requirements.maintain.insert(capability.to_owned());
        }
        let tx = client.transaction().await?;
        policy = update_volume_protocol(&tx, volume, policy.epoch, &policy, &actor, "test").await?;
        tx.commit().await?;
    }

    let output = stack.cli(&cli_args(false, 1), &stack.root)?;
    assert!(!output.status.success());
    assert!(
        [&output.stderr, &output.stdout]
            .iter()
            .any(|bytes| String::from_utf8_lossy(bytes).contains("maintenance_capability_missing"))
    );
    assert_eq!(durable_janitor_state(&client, volume).await?, before);
    let writer = stack.writer()?;
    for key in object_keys(&prefix, id) {
        assert_eq!(writer.read(&key).await?.to_vec(), b"janitor fixture");
        writer.delete(&key).await?;
    }
    task.abort();
    Ok(())
}

async fn seed_protection_matrix(
    stack: &LiveStack,
    volume_id: i64,
    base: u64,
) -> anyhow::Result<()> {
    let (mut client, task) = connection(stack).await?;
    let current = i64::try_from(base)?;
    client
        .execute(
            "INSERT INTO snapshot_current_references (vol, ino, object_id) VALUES ($1, 1, $2)",
            &[&volume_id, &current],
        )
        .await?;

    for (offset, state) in [(1_i64, "uploading"), (2, "uploaded")] {
        let id = i64::try_from(base)? + offset;
        client.execute(
            "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state) VALUES ($1, $2, $3)",
            &[&volume_id, &id, &state],
        ).await?;
    }

    // Leased rows model a current claimer at the fixture's active epoch.
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT set_config('studiofs.deletion_format', '6', true),
                set_config('studiofs.deletion_volume_format', '7', true),
                set_config('studiofs.maintenance_epoch', publication_epoch::text, true),
                set_config('studiofs.maintenance_capabilities', to_jsonb(ARRAY[$2::text])::text, true)
         FROM metadata_state WHERE vol = $1",
        &[&volume_id, &studiofs_meta::compat::CAP_MAINTENANCE_PAYLOAD_GC_V2],
    )
        .await?;
    for (offset, lease) in [(11_i64, "5 minutes"), (12, "-5 minutes")] {
        tx.execute(
            "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, reconcile_owner, reconcile_lease_expires_at, created_at) VALUES ($1, $2, 'delete_pending', 'fixture-reconciler', clock_timestamp() + $3::text::interval, clock_timestamp() - interval '1 hour')",
            &[&volume_id, &(i64::try_from(base)? + offset), &lease],
        ).await?;
    }
    tx.commit().await?;

    for (offset, lifecycle) in [
        (3_i64, "creating"),
        (4, "ready"),
        (5, "deleting"),
        (6, "deleting"),
    ] {
        let id = i64::try_from(base)? + offset;
        let snapshot_id = format!("janitor-pin-{offset}");
        client.execute(
            "INSERT INTO snapshot_catalog (vol, id, origin, cut_version, captured_at, lifecycle) VALUES ($1, $2, 'manual', $3, clock_timestamp(), $4)",
            &[&volume_id, &snapshot_id, &offset, &lifecycle],
        ).await?;
        client.execute(
            "INSERT INTO snapshot_object_reachability (vol, object_id, start_version, end_version) VALUES ($1, $2, $3, $4)",
            &[&volume_id, &id, &offset, &(offset + 1)],
        ).await?;
        if offset == 5 {
            client.execute(
                "INSERT INTO snapshot_mount_leases (vol, snapshot_id, owner, cache_volume_uuid, cache_sha256, lease_expires_at, heartbeat_at) VALUES ($1, $2, 'fixture', 'fixture', $3, clock_timestamp() + interval '5 minutes', clock_timestamp())",
                &[&volume_id, &snapshot_id, &vec![0_u8; 32]],
            ).await?;
        }
    }

    for offset in [3_i64, 4, 5, 6, 7, 8] {
        let id = i64::try_from(base)? + offset;
        client.execute(
            "INSERT INTO snapshot_payload_gc_candidates (vol, object_id, eligible_at) VALUES ($1, $2, clock_timestamp() - interval '1 hour')",
            &[&volume_id, &id],
        ).await?;
    }

    let legacy = i64::try_from(base)? + 9;
    client.execute(
        "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, created_at, closed_at) VALUES ($1, $2, 'delete_pending', clock_timestamp() - interval '1 hour', clock_timestamp() - interval '1 hour')",
        &[&volume_id, &legacy],
    ).await?;
    task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local STUDIOFS_TEST_PG_DSN and a live STUDIOFS_S3_* bucket"]
async fn live_cli_disposable_adversarial_protection_cleanup_dry_run_and_budget()
-> anyhow::Result<()> {
    let stack = LiveStack::start()?;
    let base = object_id(0)?;
    let prefix = format!("sfsjanitor/{base:016x}/objects/");
    let s3 = stack.s3(&prefix);
    for offset in 0..=12 {
        seed_live_object(&stack, &prefix, base + offset).await?;
    }
    let client_root = stack.root.join("matrix");
    let meta_dir = client_root.join("meta/janitor-live");
    let (volume_id, db_path, _) =
        create_live_postgres_janitor_fixture(stack.postgres.dsn(), &s3, &meta_dir, base).await?;
    let engine = RedbMetaEngine::open(&db_path)?;
    engine.set_counter("pgLastAppliedBatchVersion", 6)?;
    drop(engine);
    let (client, task) = connection(&stack).await?;
    client
        .execute(
            "UPDATE metadata_state SET version = 6 WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    task.abort();
    seed_protection_matrix(&stack, volume_id, base).await?;

    let (before_client, before_task) = connection(&stack).await?;
    let state_before = durable_janitor_state(&before_client, volume_id).await?;

    let dry = stack.cli(&cli_args(true, 1), &client_root)?;
    anyhow::ensure!(
        dry.status.code() == Some(2),
        "dry-run delete cap should return partial status"
    );
    let dry_events = parse_events(&dry)?;
    let dry_cycle = dry_events
        .iter()
        .find(|event| event["event"] == "cycle")
        .expect("cycle event");
    anyhow::ensure!(
        dry_cycle["would_delete"] == 1
            && dry_cycle["delete_attempted"] == 0
            && dry_cycle["delete_confirmed"] == 0,
        "dry-run must respect its cap without issuing deletes"
    );
    let state_after = durable_janitor_state(&before_client, volume_id).await?;
    anyhow::ensure!(
        state_after == state_before,
        "dry-run changed PostgreSQL candidate, upload, watermark, or checkpoint state"
    );
    for offset in 0..=12 {
        let key = studiofs_data::object::format_object_key(&prefix, base + offset);
        assert_eq!(
            stack.writer()?.read(&key).await?.to_vec(),
            b"janitor fixture"
        );
    }
    before_task.abort();

    let capped = stack.cli(&cli_args(false, 1), &client_root)?;
    anyhow::ensure!(
        capped.status.code() == Some(2),
        "delete cap should return partial status"
    );
    let capped_events = parse_events(&capped)?;
    anyhow::ensure!(
        summary(&capped_events)["outcome"] == "partial",
        "capped summary must be partial"
    );
    let capped_cycle = capped_events
        .iter()
        .find(|event| event["event"] == "cycle")
        .expect("capped cycle");
    assert_eq!(capped_cycle["delete_confirmed"], 1);

    let writer = stack.writer()?;
    let mut removed = 0;
    for offset in 0..=12 {
        let key = studiofs_data::object::format_object_key(&prefix, base + offset);
        let remains = writer.exists(&key).await?;
        removed += u64::from(!remains);
        let protected = matches!(offset, 0..=5 | 11);
        if protected {
            anyhow::ensure!(remains, "protected object at offset {offset} was deleted");
            assert_eq!(writer.read(&key).await?.to_vec(), b"janitor fixture");
        }
    }
    assert_eq!(
        removed, 1,
        "real delete budget must remove exactly one payload"
    );

    let completed = stack.cli(&cli_args(false, 10), &client_root)?;
    anyhow::ensure!(
        completed.status.success(),
        "remaining eligible work should complete"
    );
    let completed_events = parse_events(&completed)?;
    anyhow::ensure!(
        summary(&completed_events)["outcome"] == "complete",
        "final sweep should complete"
    );
    for offset in 0..=12 {
        for key in object_keys(&prefix, base + offset) {
            let remains = writer.exists(&key).await?;
            anyhow::ensure!(
                remains == matches!(offset, 0..=5 | 11),
                "unexpected remote state for object offset {offset}"
            );
            if remains {
                assert_eq!(writer.read(&key).await?.to_vec(), b"janitor fixture");
            }
        }
    }
    let (client, task) = connection(&stack).await?;
    let acknowledged: i64 = client.query_one(
        "SELECT count(*) FROM snapshot_payload_gc_candidates WHERE vol = $1 AND acknowledged_at IS NOT NULL",
        &[&volume_id],
    ).await?.get(0);
    assert_eq!(acknowledged, 6, "all eligible objects must be acknowledged");
    let owners: i64 = client.query_one(
        "SELECT count(*) FROM snapshot_payload_upload_objects WHERE vol = $1 AND reconcile_owner IS NOT NULL AND object_id <> $2",
        &[&volume_id, &(i64::try_from(base)? + 11)],
    ).await?.get(0);
    assert_eq!(owners, 0, "finished cleanup retained a claim owner");
    let active_owner: String = client.query_one(
        "SELECT reconcile_owner FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
        &[&volume_id, &(i64::try_from(base)? + 11)],
    ).await?.get(0);
    assert_eq!(active_owner, "fixture-reconciler");
    task.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires local STUDIOFS_TEST_PG_DSN and a live STUDIOFS_S3_* bucket"]
async fn live_cli_disposable_retires_abandoned_upload_but_protects_delayed_publication()
-> anyhow::Result<()> {
    use sha2::{Digest, Sha256};
    use studiofs_meta::engine::{
        MetaFsOps, MetaGc, MetaInodes, MetaPayloadObjects, MetaReplication,
    };
    use studiofs_meta::snapshot_ports::{UploadedFragmentLedger, UploadedFragmentLedgerEntry};
    use studiofs_meta::{PgMetaEngine, PgOpenOptions};

    let stack = LiveStack::start()?;
    let base = object_id(0)?;
    let abandoned = base;
    let waiting_publication = base + 1;
    let prefix = format!("sfsjanitor-abandoned/{base:016x}/objects/");
    let s3 = stack.s3(&prefix);
    seed_live_object(&stack, &prefix, abandoned).await?;
    seed_live_object(&stack, &prefix, waiting_publication).await?;
    let writer_root = stack.root.join("abandoned-upload-writer");
    let janitor_root = stack.root.join("abandoned-upload-janitor");
    let meta_dir = writer_root.join("meta/janitor-live");
    let (volume_id, db_path, _) =
        create_live_postgres_janitor_fixture(stack.postgres.dsn(), &s3, &meta_dir, abandoned)
            .await?;
    let (client, task) = connection(&stack).await?;
    client
        .execute(
            "DELETE FROM snapshot_payload_gc_candidates WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    task.abort();
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )?;

    let payload = b"janitor fixture";
    let digest: [u8; 32] = Sha256::digest(payload).into();
    let entries_for = |id: u64| -> anyhow::Result<Vec<UploadedFragmentLedgerEntry>> {
        Ok(object_keys(&prefix, id)
            .into_iter()
            .enumerate()
            .map(|(fragment_index, object_key)| UploadedFragmentLedgerEntry {
                volume_id,
                object_id: id,
                fragment_index: u32::try_from(fragment_index).expect("two fixture keys"),
                object_key,
                expected_byte_length: u64::try_from(payload.len()).expect("payload length fits"),
                expected_sha256: digest,
            })
            .collect())
    };
    for id in [abandoned, waiting_publication] {
        let entries = entries_for(id)?;
        engine.begin_fragment_uploads(&entries)?;
        engine.confirm_fragment_uploads(&entries)?;
    }
    engine.flush_metadata()?;

    engine.add_pending_deletion(abandoned, 1)?;
    engine.flush_metadata()?;
    let (client, task) = connection(&stack).await?;
    let retired = client
        .query_one(
            "SELECT state, EXISTS (SELECT 1 FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2) FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(abandoned)?],
        )
        .await?;
    anyhow::ensure!(
        retired.get::<_, String>(0) == "delete_pending" && retired.get::<_, bool>(1),
        "settled unreferenced upload was not queued for deletion"
    );
    // Keep the candidate eligible even when the PostgreSQL clock leads the CLI host.
    client
        .execute(
            "UPDATE snapshot_payload_gc_candidates SET eligible_at = to_timestamp(1) WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(abandoned)?],
        )
        .await?;
    task.abort();
    engine.flush_local_metadata()?;
    drop(engine);

    // The janitor client stays caught up while the writer has a local event tail.
    let janitor_db_path = janitor_root.join("meta/janitor-live/janitor-live.db");
    std::fs::create_dir_all(
        janitor_db_path
            .parent()
            .expect("janitor metadata directory"),
    )?;
    std::fs::copy(&db_path, &janitor_db_path)?;
    let engine = PgMetaEngine::open_with_options(
        &db_path,
        PgOpenOptions {
            background_poll: false,
            ..Default::default()
        },
    )?;

    // Keep a real local publication behind the pending deletion marker while the
    // standalone janitor runs against PostgreSQL's older, uploaded-only view.
    engine.add_pending_deletion(waiting_publication, 2)?;
    let file = engine.create_file(1, b"delayed-publication", 0, 0, 0o644)?;
    let mut attr = engine.get_attr(file.ino)?;
    attr.size = u64::try_from(payload.len())?;
    attr.content_epoch = 1;
    engine.set_attr(&attr)?;
    let slice = studiofs_meta::schema::SliceRecord::new(
        studiofs_meta::schema::SliceId(0),
        studiofs_meta::schema::SliceOrder(0),
        studiofs_meta::schema::SliceRange::new(0, u32::try_from(payload.len())?)
            .expect("nonempty fixture payload range"),
        studiofs_meta::schema::PayloadRef::new_partial(
            waiting_publication,
            u32::try_from(payload.len())?,
        ),
    );
    engine.flush_regular_file_overlay_slices(
        file.ino,
        &[(studiofs_meta::schema::ChunkId(0), vec![slice])],
    )?;
    engine.mark_regular_file_overlay_object_uploaded_if_matches(
        file.ino,
        1,
        waiting_publication,
    )?;
    anyhow::ensure!(
        engine.publish_regular_file_overlay_to_base_if_matches(file.ino, 1)?,
        "delayed publication fixture was not publishable"
    );

    let first_sweep = stack.cli(&cli_args(false, 10), &janitor_root)?;
    anyhow::ensure!(
        first_sweep.status.success(),
        "janitor should finish after retiring the abandoned upload"
    );
    let writer = stack.writer()?;
    for key in object_keys(&prefix, abandoned) {
        anyhow::ensure!(
            !writer.exists(&key).await?,
            "abandoned payload key survived: {key}"
        );
    }
    for key in object_keys(&prefix, waiting_publication) {
        anyhow::ensure!(
            writer.exists(&key).await?,
            "delayed publication payload was deleted: {key}"
        );
    }

    let (client, task) = connection(&stack).await?;
    let abandoned_row = client
        .query_one(
            "SELECT uploaded.state, candidate.acknowledged_at IS NOT NULL FROM snapshot_payload_upload_objects AS uploaded JOIN snapshot_payload_gc_candidates AS candidate USING (vol, object_id) WHERE uploaded.vol = $1 AND uploaded.object_id = $2",
            &[&volume_id, &i64::try_from(abandoned)?],
        )
        .await?;
    anyhow::ensure!(
        abandoned_row.get::<_, String>(0) == "delete_pending" && abandoned_row.get::<_, bool>(1),
        "deleted upload must retain its terminal fence and acknowledge its candidate"
    );
    let waiting_state: String = client
        .query_one(
            "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(waiting_publication)?],
        )
        .await?
        .get(0);
    anyhow::ensure!(
        waiting_state == "uploaded",
        "janitor must leave the delayed upload eligible for publication"
    );
    task.abort();

    engine.flush_metadata()?;
    let (client, task) = connection(&stack).await?;
    let published_state: String = client
        .query_one(
            "SELECT state FROM snapshot_payload_upload_objects WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(waiting_publication)?],
        )
        .await?
        .get(0);
    let reference_count: i64 = client
        .query_one(
            "SELECT count(*) FROM snapshot_current_references WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(waiting_publication)?],
        )
        .await?
        .get(0);
    anyhow::ensure!(
        published_state == "published" && reference_count == 1,
        "delayed publication did not commit its object reference"
    );
    task.abort();
    engine.flush_local_metadata()?;
    drop(engine);
    std::fs::copy(&db_path, &janitor_db_path)?;
    let second_sweep = stack.cli(&cli_args(false, 10), &janitor_root)?;
    anyhow::ensure!(
        second_sweep.status.success(),
        "janitor should complete with the published object protected"
    );
    for key in object_keys(&prefix, waiting_publication) {
        anyhow::ensure!(
            writer.exists(&key).await?,
            "published payload key was deleted: {key}"
        );
    }
    Ok(())
}

mod process;
