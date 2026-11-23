//! Standalone `studiofs janitor` CLI scenarios against the run's Compose PostgreSQL and RustFS.
//!
//! Each scenario initializes its own volume with one `janitor` client, seeds payload
//! objects directly in S3, and drives the CLI as a separate process.

use std::process::{Command, Output, Stdio};

use opendal::Operator;
use opendal::services::S3;

use super::recoverable_trash_janitor::{load_volume_id_and_prefix, with_pg};
use super::snapshot_v2::redact_diagnostics;
use super::*;
use crate::client::ClientControllerConfig;

mod benchmark;
mod restart;

pub(in crate::scenario) use benchmark::JanitorLiveBenchmark;
pub(in crate::scenario) use restart::JanitorPostgresRestart;

const CLIENT: &str = "janitor";
const FIXTURE_BYTES: &[u8] = b"janitor fixture";

/// A freshly initialized volume and the client root the janitor CLI runs from.
struct JanitorVolume {
    config: ClientControllerConfig,
    client_root: PathBuf,
    volume_id: i64,
    prefix: String,
}

impl JanitorVolume {
    /// Runs `studiofs init` for one client and resolves the volume's id and data prefix.
    fn bootstrap(config: ClientControllerConfig) -> anyhow::Result<Self> {
        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        controller.bootstrap_volume(VolumeBootstrapOptions {
            client_names: vec![CLIENT.into()],
            clean_existing_local_metadata: true,
        })?;
        // The janitor refuses a volume whose snapshot reachability bootstrap is incomplete;
        // fsck's PostgreSQL sync completes it without a mount.
        controller.run_cli(
            CLIENT,
            "fsck",
            vec!["fsck".into(), config.volume_name.clone()],
        )?;
        let client_root = controller.clients()[0].client_root.clone();
        let (volume_id, prefix) = load_volume_id_and_prefix(&config)?;
        Ok(Self {
            config,
            client_root,
            volume_id,
            prefix,
        })
    }

    /// `janitor <volume>` with JSONL output and no young-object grace window.
    fn args(&self, max_deletes: u64) -> Vec<String> {
        [
            "janitor",
            self.config.volume_name.as_str(),
            "--output",
            "jsonl",
            "--full-scan",
            "--skip-newer-minutes",
            "0",
            "--max-deletes",
            &max_deletes.to_string(),
        ]
        .map(String::from)
        .to_vec()
    }

    /// A CLI command for `binary` scoped to this volume's janitor client root.
    fn command(&self, binary: &Path, args: &[String]) -> Command {
        let mut command = Command::new(binary);
        command
            .args(args)
            .env("STUDIOFS_CLIENT_ROOT", &self.client_root)
            .env("STUDIOFS_DAEMON_LOG_DIR", self.client_root.join("logs"))
            .env("STUDIOFS_DAEMON_LOG_STEM", "studiofs-janitor-cli")
            .stdin(Stdio::null());
        command
    }

    /// Runs the CLI to completion within `timeout`, keeping redacted output in `dir`.
    fn run(
        &self,
        dir: &Path,
        label: &str,
        args: &[String],
        timeout: Duration,
    ) -> anyhow::Result<Output> {
        let command = self.command(&self.config.studiofs_bin, args);
        let stdout = dir.join(format!("{label}.stdout.log"));
        let stderr = dir.join(format!("{label}.stderr.log"));
        let output = run_child_bounded(command, timeout, &stdout, &stderr, |_| {})?;
        self.redact_logs(&stdout, &stderr)?;
        Ok(output)
    }

    /// Rewrites captured CLI output without DSN or S3 credentials.
    fn redact_logs(&self, stdout: &Path, stderr: &Path) -> io::Result<()> {
        for path in [stdout, stderr] {
            let text = fs::read_to_string(path)?;
            fs::write(path, redact_diagnostics(&self.config, &text))?;
        }
        Ok(())
    }

    /// Queues `object_id` as an already eligible deletion candidate.
    fn queue_candidate(&self, object_id: u64) -> anyhow::Result<()> {
        let (volume_id, object_id) = (self.volume_id, i64::try_from(object_id)?);
        with_pg(
            &self.config,
            "queue janitor candidate",
            |client| async move {
                client
                .execute(
                    "INSERT INTO snapshot_payload_gc_candidates (vol, object_id, eligible_at) VALUES ($1, $2, clock_timestamp() - interval '1 hour')",
                    &[&volume_id, &object_id],
                )
                .await
                .map_err(io::Error::other)?;
                Ok(())
            },
        )?;
        Ok(())
    }
}

/// Kills and reaps a CLI child when a scenario fails before it exits.
struct OwnedCli(std::process::Child);

impl Drop for OwnedCli {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Runs `command` with output in `stdout`/`stderr`, calling `sample` with the child
/// pid on every poll, and kills it once `timeout` elapses.
fn run_child_bounded(
    mut command: Command,
    timeout: Duration,
    stdout: &Path,
    stderr: &Path,
    mut sample: impl FnMut(u32),
) -> anyhow::Result<Output> {
    command.stdout(fs::File::create(stdout)?);
    command.stderr(fs::File::create(stderr)?);
    let mut child = OwnedCli(command.spawn()?);
    let started = Instant::now();
    let status = loop {
        sample(child.0.id());
        if let Some(status) = child.0.try_wait()? {
            break status;
        }
        anyhow::ensure!(
            started.elapsed() < timeout,
            "janitor CLI exceeded {timeout:?}; output at {}",
            stdout.display()
        );
        thread::sleep(Duration::from_millis(50));
    };
    Ok(Output {
        status,
        stdout: fs::read(stdout)?,
        stderr: fs::read(stderr)?,
    })
}

fn s3_writer(config: &ClientControllerConfig) -> anyhow::Result<Operator> {
    Ok(Operator::new(
        S3::default()
            .endpoint(&config.s3.endpoint)
            .bucket(&config.s3.bucket)
            .access_key_id(&config.s3.access_key)
            .secret_access_key(&config.s3.secret_key)
            .region(&config.s3.region)
            .disable_config_load()
            .disable_ec2_metadata(),
    )?
    // Fixture writes retry temporary S3 errors, as the janitor store does.
    .layer(opendal::layers::RetryLayer::new())
    .finish())
}

/// Every canonical key the CLI may delete for one object.
fn object_keys(prefix: &str, id: u64) -> Vec<String> {
    std::iter::once(studiofs_data::object::format_object_key(prefix, id))
        .chain(studiofs_data::object::format_object_fragment_keys(
            prefix, id,
        ))
        .collect()
}

/// A time-derived object id with room for `offset` siblings below 2^50.
fn object_id(offset: u64) -> anyhow::Result<u64> {
    let base = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros())?;
    Ok((base & ((1_u64 << 50) - 32)) + offset)
}

/// Returns once S3 dates every key in the past by this host's clock. With a
/// zero grace window the janitor skips a future-dated object as unknown age,
/// and the S3 server clock may run ahead of this host. HEAD dates have second
/// precision while listings carry milliseconds, hence the 1 s margin.
async fn wait_until_s3_keys_are_past(writer: &Operator, keys: &[String]) -> anyhow::Result<()> {
    let mut newest = UNIX_EPOCH;
    for key in keys {
        let modified = writer
            .stat(key)
            .await?
            .last_modified()
            .ok_or_else(|| anyhow::anyhow!("S3 returned no Last-Modified for {key}"))?;
        newest = newest.max(modified.into());
    }
    let ahead = (newest + Duration::from_secs(1))
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    anyhow::ensure!(
        ahead < Duration::from_secs(10),
        "S3 server clock runs {ahead:?} ahead of this host"
    );
    tokio::time::sleep(ahead).await;
    Ok(())
}

/// Converts a scenario failure into a redacted `ScenarioError`.
fn scenario_error(config: &ClientControllerConfig, error: &anyhow::Error) -> ScenarioError {
    io::Error::other(redact_diagnostics(config, &format!("{error:#}"))).into()
}
