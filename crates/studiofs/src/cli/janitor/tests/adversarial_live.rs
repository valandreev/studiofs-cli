//! Disposable PostgreSQL/RustFS acceptance for the standalone janitor CLI.

use super::*;
use opendal::Operator;
use opendal::services::S3;
use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use studiofs_e2e::infra::{ComposeConfig, DockerComposeStack, SystemCommandExecutor};

const ACCESS_KEY: &str = "rustfsadmin";
const SECRET_KEY: &str = "rustfsadmin";
const REGION: &str = "us-east-1";

struct DisposableStack {
    compose: DockerComposeStack,
    executor: SystemCommandExecutor,
    artifact_root: PathBuf,
    postgres_url: String,
    endpoint: String,
    bucket: String,
    root: PathBuf,
}

impl DisposableStack {
    async fn start() -> anyhow::Result<Self> {
        let run_id = format!(
            "janitor-adversarial-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let artifact_root = studiofs_test_support::workspace_root()
            .join("target/janitor-adversarial")
            .join(&run_id);
        std::fs::create_dir_all(&artifact_root)?;
        let compose = DockerComposeStack::new(ComposeConfig::for_run(
            &run_id,
            &artifact_root,
            Some(free_port_range()?),
        ))?;
        let executor = SystemCommandExecutor;
        compose.prepare_artifacts()?;
        let mut stack = Self {
            postgres_url: String::new(),
            endpoint: String::new(),
            bucket: format!(
                "sfsjanitor{}",
                SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
            ),
            root: artifact_root.join("client"),
            artifact_root,
            compose,
            executor,
        };
        stack.compose.start(&stack.executor)?;
        let port = |service: &str| {
            stack
                .compose
                .ports()
                .iter()
                .find(|port| port.service == service)
                .map(|port| port.host_port)
                .ok_or_else(|| anyhow::anyhow!("missing Compose port for {service}"))
        };
        stack.postgres_url = format!(
            "host=127.0.0.1 port={} user=studiofs password=studiofs dbname=studiofs",
            port("postgres")?
        );
        stack.endpoint = format!("http://127.0.0.1:{}", port("rustfs")?);
        crate::cli::test::bucket_create(
            &stack.endpoint,
            &stack.bucket,
            ACCESS_KEY,
            SECRET_KEY,
            REGION,
        )
        .await?;
        Ok(stack)
    }

    fn writer(&self) -> anyhow::Result<Operator> {
        Ok(Operator::new(
            S3::default()
                .endpoint(&self.endpoint)
                .bucket(&self.bucket)
                .access_key_id(ACCESS_KEY)
                .secret_access_key(SECRET_KEY)
                .region(REGION)
                .disable_config_load()
                .disable_ec2_metadata(),
        )?
        .finish())
    }

    fn s3<'a>(&'a self, prefix: &'a str) -> LiveS3Config<'a> {
        LiveS3Config {
            endpoint: &self.endpoint,
            bucket: &self.bucket,
            access_key: ACCESS_KEY,
            secret_key: SECRET_KEY,
            region: REGION,
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

    fn command(&self, args: &[String], client_root: &Path) -> anyhow::Result<Command> {
        let mut binary = std::env::current_exe()?;
        binary.pop();
        binary.pop();
        binary.push(if cfg!(windows) {
            "studiofs.exe"
        } else {
            "studiofs"
        });
        anyhow::ensure!(
            binary.is_file(),
            "build the studiofs CLI binary before this ignored test"
        );

        let mut command = Command::new(binary);
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

impl Drop for DisposableStack {
    fn drop(&mut self) {
        let _ = self.compose.collect_logs(&self.executor);
        if let Err(error) = self.compose.teardown(&self.executor) {
            eprintln!(
                "janitor adversarial Compose teardown failed; artifacts retained at {}: {error}",
                self.artifact_root.display()
            );
        }
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
            child.0.kill()?;
            break true;
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

/// Probes a free loopback window so concurrent stacks do not share host ports.
fn free_port_range() -> anyhow::Result<String> {
    const WIDTH: u16 = 16;
    const SLOTS: u32 = 2_500;
    let seed = SystemTime::now().duration_since(UNIX_EPOCH)?.subsec_nanos() ^ std::process::id();
    for attempt in 0..256_u32 {
        let slot = u16::try_from(seed.wrapping_add(attempt.wrapping_mul(7_919)) % SLOTS)?;
        let start = 20_000 + slot * WIDTH;
        if (start..start + WIDTH)
            .all(|port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok())
        {
            return Ok(format!("{start}-{}", start + WIDTH - 1));
        }
    }
    anyhow::bail!("no free loopback port window for the disposable Compose stack")
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

async fn seed_live_object(stack: &DisposableStack, prefix: &str, id: u64) -> anyhow::Result<()> {
    for key in object_keys(prefix, id) {
        stack
            .writer()?
            .write(&key, b"janitor fixture".to_vec())
            .await?;
    }
    Ok(())
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
    stack: &DisposableStack,
) -> anyhow::Result<(
    tokio_postgres::Client,
    tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
)> {
    let (client, connection) =
        tokio_postgres::connect(&stack.postgres_url, tokio_postgres::NoTls).await?;
    Ok((client, tokio::spawn(connection)))
}

async fn seed_protection_matrix(
    stack: &DisposableStack,
    volume_id: i64,
    base: u64,
) -> anyhow::Result<()> {
    let (client, task) = connection(stack).await?;
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

    for (offset, lease) in [(11_i64, "5 minutes"), (12, "-5 minutes")] {
        client.execute(
            "INSERT INTO snapshot_payload_upload_objects (vol, object_id, state, reconcile_owner, reconcile_lease_expires_at, created_at) VALUES ($1, $2, 'delete_pending', 'fixture-reconciler', clock_timestamp() + $3::text::interval, clock_timestamp() - interval '1 hour')",
            &[&volume_id, &(i64::try_from(base)? + offset), &lease],
        ).await?;
    }

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
#[ignore = "requires Docker; creates isolated PostgreSQL and RustFS containers"]
async fn live_cli_disposable_adversarial_protection_cleanup_dry_run_and_budget()
-> anyhow::Result<()> {
    let stack = DisposableStack::start().await?;
    let base = object_id(0)?;
    let prefix = format!("sfsjanitor/{base:016x}/objects/");
    let s3 = stack.s3(&prefix);
    for offset in 0..=12 {
        seed_live_object(&stack, &prefix, base + offset).await?;
    }
    let client_root = stack.root.join("matrix");
    let meta_dir = client_root.join("meta/janitor-live");
    let (volume_id, db_path, _) =
        create_live_postgres_janitor_fixture(&stack.postgres_url, &s3, &meta_dir, base).await?;
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
#[ignore = "requires Docker; creates isolated PostgreSQL and RustFS containers"]
async fn live_cli_disposable_fixture_rejects_stale_local_metadata_and_recovers_after_postgres_restart()
-> anyhow::Result<()> {
    let stack = DisposableStack::start().await?;
    let base = object_id(20)?;
    let prefix = format!("sfsjanitor-stale/{base:016x}/objects/");
    let s3 = stack.s3(&prefix);
    seed_live_object(&stack, &prefix, base).await?;
    let client_root = stack.root.join("stale");
    let meta_dir = client_root.join("meta/janitor-live");
    let (volume_id, _, _) =
        create_live_postgres_janitor_fixture(&stack.postgres_url, &s3, &meta_dir, base).await?;
    let (client, task) = connection(&stack).await?;
    client
        .execute(
            "UPDATE metadata_state SET version = 1 WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    task.abort();

    let stale = stack.cli(&cli_args(false, 10), &client_root)?;
    anyhow::ensure!(
        stale.status.code() == Some(4),
        "stale local metadata must fail closed"
    );
    let keys = object_keys(&prefix, base);
    for key in &keys {
        assert_eq!(
            stack.writer()?.read(key).await?.to_vec(),
            b"janitor fixture"
        );
    }

    stack.compose.stop_service("postgres", &stack.executor)?;
    let unavailable = stack.cli(&cli_args(false, 10), &client_root)?;
    anyhow::ensure!(
        unavailable.status.code() == Some(5),
        "PostgreSQL outage must return backend failure"
    );
    for key in &keys {
        assert_eq!(
            stack.writer()?.read(key).await?.to_vec(),
            b"janitor fixture"
        );
    }
    stack.compose.start_service("postgres", &stack.executor)?;
    let (client, task) = connection(&stack).await?;
    let reachable: i32 = client.query_one("SELECT 1", &[]).await?.get(0);
    anyhow::ensure!(reachable == 1, "PostgreSQL did not recover after restart");
    assert_eq!(
        janitor_ownership(&client, volume_id, base).await?,
        (false, 0),
        "failed runs must leave the candidate pending and unowned"
    );
    client
        .execute(
            "UPDATE metadata_state SET version = 0 WHERE vol = $1",
            &[&volume_id],
        )
        .await?;
    let recovered = stack.cli(&cli_args(false, 10), &client_root)?;
    anyhow::ensure!(
        recovered.status.success(),
        "janitor did not resume after PostgreSQL restart"
    );
    for key in &keys {
        assert!(!stack.writer()?.exists(key).await?);
    }
    assert_eq!(
        janitor_ownership(&client, volume_id, base).await?,
        (true, 0),
        "recovered run must acknowledge the deletion and release its claim"
    );
    task.abort();
    Ok(())
}

/// Whether the object's queue entry is acknowledged, and how many claim owners remain.
async fn janitor_ownership(
    client: &tokio_postgres::Client,
    volume_id: i64,
    object_id: u64,
) -> anyhow::Result<(bool, i64)> {
    let acknowledged: bool = client
        .query_one(
            "SELECT acknowledged_at IS NOT NULL FROM snapshot_payload_gc_candidates WHERE vol = $1 AND object_id = $2",
            &[&volume_id, &i64::try_from(object_id)?],
        )
        .await?
        .get(0);
    let owners: i64 = client
        .query_one(
            "SELECT count(*) FROM snapshot_payload_upload_objects WHERE vol = $1 AND reconcile_owner IS NOT NULL",
            &[&volume_id],
        )
        .await?
        .get(0);
    Ok((acknowledged, owners))
}

mod process;
