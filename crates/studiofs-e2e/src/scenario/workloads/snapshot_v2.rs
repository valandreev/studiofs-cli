//! Deterministic mounted Snapshot v2 recovery and claimant-takeover scenarios.

use std::ffi::OsStr;
use std::fmt::Write as _;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest as _, Sha256};
use tokio_postgres::NoTls;
use url::Url;

use super::*;
use crate::client::available_snapshot_mount_point;
use crate::client::{
    ClientControllerConfig, cleanup_stale_mount_before_remount, client_spec_for,
    mount_point_is_active,
};
use crate::process_job::{ChildJob, wait_for_child_exit};
use studiofs_meta::schema::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE};

const SNAPSHOT_LIVE_CLIENT_NAMES: &[&str] = &["client-a", "client-b"];
const SNAPSHOT_MOUNT_CLIENT_NAME: &str = "snapshot";
const SNAPSHOT_CLIENT_NAMES: &[&str] = &["client-a", "client-b", "snapshot"];
const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SNAPSHOT_STATE_TIMEOUT: Duration = Duration::from_secs(180);
const SNAPSHOT_MOUNT_READY_TIMEOUT: Duration = Duration::from_secs(45);
const SNAPSHOT_PROCESS_TERMINATION_TIMEOUT: Duration = Duration::from_secs(5);
const SNAPSHOT_UNMOUNT_GRACE: Duration = Duration::from_secs(3);
const SNAPSHOT_TEST_STARTUP_DELAY_MS_ENV: &str = "STUDIOFS_SNAPSHOT_TEST_STARTUP_DELAY_MS";
const SNAPSHOT_TEST_PAUSE_BEFORE_READY_MS_ENV: &str =
    "STUDIOFS_SNAPSHOT_TEST_PAUSE_BEFORE_READY_MS";
const SNAPSHOT_TAKEOVER_STARTUP_DELAY_MS: &str = "120000";
const SNAPSHOT_TAKEOVER_PAUSE_BEFORE_READY_MS: &str = "30000";
const SNAPSHOT_MULTI_CHUNK_BYTES: usize = DEFAULT_CHUNK_SIZE as usize + DEFAULT_BLOCK_SIZE as usize;
const RECOVERY_FILE: &str = "nested/level-two/recover.bin";
const VERSIONED_FILE: &str = "nested/level-two/versioned.bin";
const POST_CUT_FILE: &str = "post-cut.txt";
const SNAPSHOT_STATE_QUERY: &str = r#"
    SELECT c.id, c.origin::text, c.cut_version, c.lifecycle, j.state,
           to_char(c.captured_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
           l.owner,
           to_char(l.lease_expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
           COALESCE(l.fencing_token, 0),
           COALESCE(c.failure_message, j.failure_message),
           a.object_key, a.byte_length, encode(a.sha256, 'hex')
    FROM snapshot_catalog AS c
    JOIN volumes AS v ON v.vol = c.vol
    JOIN snapshot_creation_jobs AS j ON j.vol = c.vol AND j.snapshot_id = c.id
    LEFT JOIN snapshot_creation_leases AS l ON l.vol = c.vol AND l.snapshot_id = c.id
    LEFT JOIN snapshot_artifacts AS a ON a.vol = c.vol AND a.snapshot_id = c.id
    WHERE v.name = $1 AND c.id = $2
    "#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::scenario) enum SnapshotV2Kind {
    ManualRecovery,
    MaterializerTakeover,
}

impl SnapshotV2Kind {
    fn name(self) -> &'static str {
        match self {
            Self::ManualRecovery => SNAPSHOT_MANUAL_RECOVERY,
            Self::MaterializerTakeover => SNAPSHOT_MATERIALIZER_TAKEOVER,
        }
    }

    fn description(self) -> &'static str {
        match self {
            Self::ManualRecovery => "mounted manual Snapshot v2 recovery",
            Self::MaterializerTakeover => "mounted Snapshot v2 materializer claimant takeover",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::scenario) struct SnapshotV2Scenario {
    kind: SnapshotV2Kind,
}

impl SnapshotV2Scenario {
    pub(in crate::scenario) fn new(kind: SnapshotV2Kind) -> Self {
        Self { kind }
    }
}

impl Scenario for SnapshotV2Scenario {
    fn definition(&self) -> ScenarioDefinition {
        ScenarioDefinition {
            name: self.kind.name(),
            description: self.kind.description(),
            requires_studiofs_bin: true,
        }
    }

    fn setup(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        fs::create_dir_all(&context.scenario_dir)?;
        Ok(())
    }

    fn run(&self, context: &ScenarioContext) -> Result<(), ScenarioError> {
        let Some(config) = context.client_controller_config.clone() else {
            return Err(ScenarioError::MissingComposeInfrastructure);
        };

        let mut controller =
            MultiClientController::new(config.clone(), SystemClientProcessExecutor);
        let result = match self.kind {
            SnapshotV2Kind::ManualRecovery => {
                run_manual_recovery(context, &config, &mut controller)
            }
            SnapshotV2Kind::MaterializerTakeover => {
                run_materializer_takeover(context, &config, &mut controller)
            }
        };
        if let Err(error) = &result {
            let _ = fs::write(
                context.scenario_dir.join("failure.json"),
                serde_json::to_vec_pretty(&json!({
                    "scenario": self.kind.name(),
                    "status": "failed",
                    "error": error.to_string(),
                }))?,
            );
        }
        let cleanup = controller.unmount_all();
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(primary), Ok(())) => Err(primary),
            (Ok(()), Err(cleanup)) => Err(ScenarioError::Client(cleanup)),
            (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
                context: self.kind.name().to_string(),
                primary: Box::new(primary),
                cleanup: Box::new(ScenarioError::Client(cleanup)),
            }),
        }
    }

    fn validate(&self, context: &ScenarioContext) -> Result<Vec<String>, ScenarioError> {
        let summary_path = context.scenario_dir.join("snapshot-summary.json");
        let summary: SnapshotV2Summary = serde_json::from_slice(&fs::read(&summary_path)?)?;
        let passed = summary.scenario == self.kind.name()
            && summary.snapshot_id.is_some()
            && summary.snapshot_lifecycle == "ready"
            && summary.cut_version.is_some()
            && summary.origin == "manual"
            && summary.post_cut_absent
            && !summary.original_sha256.is_empty()
            && summary.recovery_size_bytes == Some(SNAPSHOT_MULTI_CHUNK_BYTES as u64)
            && summary.original_sha256 == summary.restored_sha256
            && summary.original_sha256 == summary.peer_sha256
            && summary.read_only_failures.len() == 6
            && summary
                .read_only_failures
                .iter()
                .all(|failure| failure.rejected)
            && match self.kind {
                SnapshotV2Kind::ManualRecovery => summary.recovery_copy_performed,
                SnapshotV2Kind::MaterializerTakeover => {
                    summary.lease_generation_increased && summary.stale_work_rejected
                }
            };
        fs::write(
            context.scenario_dir.join("validation.json"),
            serde_json::to_vec_pretty(&json!({
                "scenario": self.kind.name(),
                "status": if passed { "passed" } else { "failed" },
                "snapshot_id": summary.snapshot_id,
                "snapshot_lifecycle": summary.snapshot_lifecycle,
                "cut_version": summary.cut_version,
                "recovery_size_bytes": summary.recovery_size_bytes,
                "claimant": summary.claimant,
                "lease_generation": summary.lease_generation,
                "last_error": summary.last_error,
            }))?,
        )?;
        if !passed {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "{} validation failed; see {}",
                self.kind.name(),
                context.scenario_dir.join("validation.json").display()
            ))));
        }
        Ok(vec![
            format!(
                "artifacts/scenarios/{}/snapshot-summary.json",
                self.kind.name()
            ),
            format!("artifacts/scenarios/{}/validation.json", self.kind.name()),
        ])
    }

    fn cleanup(&self, _context: &ScenarioContext) -> Result<(), ScenarioError> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// One attempted mutation against a read-only snapshot mount.
pub(in crate::scenario) struct ReadOnlyFailure {
    pub(in crate::scenario) operation: String,
    pub(in crate::scenario) rejected: bool,
    pub(in crate::scenario) error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Validation report emitted by a mounted Snapshot v2 scenario.
pub(in crate::scenario) struct SnapshotV2Summary {
    pub(in crate::scenario) scenario: String,
    pub(in crate::scenario) volume: String,
    pub(in crate::scenario) snapshot_id: Option<String>,
    pub(in crate::scenario) origin: String,
    pub(in crate::scenario) snapshot_lifecycle: String,
    pub(in crate::scenario) cut_version: Option<i64>,
    pub(in crate::scenario) captured_at: Option<String>,
    pub(in crate::scenario) claimant: Option<String>,
    pub(in crate::scenario) lease_generation: i64,
    pub(in crate::scenario) lease_generation_increased: bool,
    pub(in crate::scenario) lease_expires_at: Option<String>,
    pub(in crate::scenario) last_error: Option<String>,
    pub(in crate::scenario) original_sha256: String,
    pub(in crate::scenario) recovery_size_bytes: Option<u64>,
    pub(in crate::scenario) restored_sha256: String,
    pub(in crate::scenario) peer_sha256: String,
    pub(in crate::scenario) post_cut_absent: bool,
    pub(in crate::scenario) recovery_copy_performed: bool,
    pub(in crate::scenario) read_only_failures: Vec<ReadOnlyFailure>,
    pub(in crate::scenario) artifact_key: Option<String>,
    pub(in crate::scenario) artifact_size: Option<i64>,
    pub(in crate::scenario) artifact_sha256: Option<String>,
    pub(in crate::scenario) stale_work_rejected: bool,
    pub(in crate::scenario) state_transitions: Vec<String>,
    pub(in crate::scenario) binary_path: String,
    pub(in crate::scenario) binary_sha256: Option<String>,
}

impl SnapshotV2Summary {
    fn new(kind: SnapshotV2Kind) -> Self {
        Self {
            scenario: kind.name().to_string(),
            volume: String::new(),
            snapshot_id: None,
            origin: String::new(),
            snapshot_lifecycle: "unknown".to_string(),
            cut_version: None,
            captured_at: None,
            claimant: None,
            lease_generation: 0,
            lease_generation_increased: false,
            lease_expires_at: None,
            last_error: None,
            original_sha256: String::new(),
            recovery_size_bytes: None,
            restored_sha256: String::new(),
            peer_sha256: String::new(),
            post_cut_absent: false,
            recovery_copy_performed: false,
            read_only_failures: Vec::new(),
            artifact_key: None,
            artifact_size: None,
            artifact_sha256: None,
            stale_work_rejected: false,
            state_transitions: Vec::new(),
            binary_path: String::new(),
            binary_sha256: None,
        }
    }
}

#[derive(Debug, Clone)]
/// Snapshot catalog and materializer state observed during polling.
pub(in crate::scenario) struct SnapshotState {
    pub(in crate::scenario) id: String,
    pub(in crate::scenario) origin: String,
    pub(in crate::scenario) cut_version: i64,
    pub(in crate::scenario) lifecycle: String,
    pub(in crate::scenario) job_state: String,
    pub(in crate::scenario) captured_at: Option<String>,
    pub(in crate::scenario) claimant: Option<String>,
    pub(in crate::scenario) lease_expires_at: Option<String>,
    pub(in crate::scenario) lease_generation: i64,
    pub(in crate::scenario) last_error: Option<String>,
    pub(in crate::scenario) artifact_key: Option<String>,
    pub(in crate::scenario) artifact_size: Option<i64>,
    pub(in crate::scenario) artifact_sha256: Option<String>,
}

fn run_manual_recovery(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    let mut summary = SnapshotV2Summary::new(SnapshotV2Kind::ManualRecovery);
    initialize_summary(&mut summary, config);
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: SNAPSHOT_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    for client_name in SNAPSHOT_LIVE_CLIENT_NAMES {
        controller.mount(client_name)?;
    }
    let actual_clients = controller.clients().len();
    if actual_clients < 2 {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: 2,
            actual: actual_clients,
        });
    }
    let writer = controller.client_at(0)?;
    let peer = controller.client_at(1)?;
    let snapshot_client = snapshot_mount_client(controller)?;
    let recovery = writer.mount_point.join(RECOVERY_FILE);
    let versioned = writer.mount_point.join(VERSIONED_FILE);
    let post_cut = writer.mount_point.join(POST_CUT_FILE);
    let version_1 = repeated_payload(b"snapshot-v2-version-1\n", SNAPSHOT_MULTI_CHUNK_BYTES);
    let version_2 = repeated_payload(b"snapshot-v2-version-2\n", SNAPSHOT_MULTI_CHUNK_BYTES);
    let version_1_sha256 = bytes_sha256(&version_1);
    summary.original_sha256 = version_1_sha256.clone();
    summary.recovery_size_bytes = Some(version_1.len() as u64);
    write_file_sync(&recovery, &version_1)?;
    write_file_sync(&versioned, &version_1)?;
    wait_for_checksum(
        &peer.mount_point.join(RECOVERY_FILE),
        &version_1_sha256,
        SNAPSHOT_STATE_TIMEOUT,
    )?;

    let output = run_cli_command(
        config,
        writer,
        context,
        "snapshot-create",
        &["snapshot", "create"],
    )?;
    let snapshot_id = parse_snapshot_id(&output).ok_or_else(|| {
        ScenarioError::Io(io::Error::other(
            "snapshot create output did not contain a SnapshotId",
        ))
    })?;
    let state = wait_for_snapshot_state(
        config,
        &config.volume_name,
        &snapshot_id,
        |_| true,
        SNAPSHOT_STATE_TIMEOUT,
    )?;
    summary.snapshot_id = Some(snapshot_id.clone());
    summary.origin = state.origin.clone();
    summary.snapshot_lifecycle = state.lifecycle.clone();
    summary.cut_version = Some(state.cut_version);
    summary.captured_at = state.captured_at.clone();
    summary.claimant = state.claimant.clone();
    summary.lease_generation = state.lease_generation;
    summary.lease_expires_at = state.lease_expires_at.clone();
    summary.last_error = state
        .last_error
        .as_deref()
        .map(|error| redact_diagnostics(config, error));
    summary.artifact_key = state.artifact_key.clone();
    summary.artifact_size = state.artifact_size;
    summary.artifact_sha256 = state.artifact_sha256.clone();
    summary.state_transitions.push(format_state(&state));

    write_file_sync(&versioned, &version_2)?;
    write_file_sync(&post_cut, b"created after snapshot cut\n")?;
    fs::remove_file(&recovery)?;

    let snapshot_mount = snapshot_mount_path(context)?;
    cleanup_stale_mount_before_remount(&snapshot_mount)?;
    let mut snapshot_process = spawn_snapshot_mount(
        config,
        snapshot_client,
        context,
        &snapshot_id,
        &snapshot_mount,
    )?;
    let result = (|| {
        wait_for_snapshot_mount(
            &mut snapshot_process,
            &snapshot_mount,
            SNAPSHOT_MOUNT_READY_TIMEOUT,
        )?;
        let snapshot_recovery = snapshot_mount.join(RECOVERY_FILE);
        let snapshot_versioned = snapshot_mount.join(VERSIONED_FILE);
        let snapshot_recovery_bytes = fs::read(&snapshot_recovery)?;
        if Some(snapshot_recovery_bytes.len() as u64) != summary.recovery_size_bytes {
            return Err(ScenarioError::Io(io::Error::other(
                "snapshot recovery size does not match the multi-chunk oracle",
            )));
        }
        let snapshot_recovery_sha256 = bytes_sha256(&snapshot_recovery_bytes);
        if snapshot_recovery_sha256 != summary.original_sha256 {
            return Err(ScenarioError::Io(io::Error::other(
                "snapshot recovery checksum does not match the pre-cut oracle",
            )));
        }
        if bytes_sha256(&fs::read(&snapshot_versioned)?) != version_1_sha256 {
            return Err(ScenarioError::Io(io::Error::other(
                "snapshot version-1 boundary mismatch",
            )));
        }
        summary.post_cut_absent = !snapshot_mount.join(POST_CUT_FILE).exists();
        summary.read_only_failures =
            snapshot_read_only_failures(&snapshot_mount, &snapshot_versioned, &snapshot_recovery);
        fs::copy(&snapshot_recovery, &recovery)?;
        summary.recovery_copy_performed = true;
        summary.restored_sha256 = bytes_sha256(&fs::read(&recovery)?);
        wait_for_checksum(
            &peer.mount_point.join(RECOVERY_FILE),
            &summary.original_sha256,
            SNAPSHOT_STATE_TIMEOUT,
        )?;
        summary.peer_sha256 = bytes_sha256(&fs::read(peer.mount_point.join(RECOVERY_FILE))?);
        write_summary(context, &summary)
    })();
    finish_snapshot_with_cleanup(
        context,
        config,
        &snapshot_mount,
        &mut snapshot_process,
        result,
    )
}

fn run_materializer_takeover(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    controller: &mut MultiClientController<SystemClientProcessExecutor>,
) -> Result<(), ScenarioError> {
    let mut summary = SnapshotV2Summary::new(SnapshotV2Kind::MaterializerTakeover);
    initialize_summary(&mut summary, config);
    controller.bootstrap_volume(VolumeBootstrapOptions {
        client_names: SNAPSHOT_CLIENT_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect(),
        clean_existing_local_metadata: true,
    })?;
    let actual_clients = controller.clients().len();
    if actual_clients < 2 {
        return Err(ScenarioError::InsufficientMountedClients {
            expected: 2,
            actual: actual_clients,
        });
    }
    controller.set_client_env_overrides(
        SNAPSHOT_CLIENT_NAMES[0],
        [(
            SNAPSHOT_TEST_STARTUP_DELAY_MS_ENV,
            SNAPSHOT_TAKEOVER_STARTUP_DELAY_MS,
        )],
    )?;
    controller.mount(SNAPSHOT_CLIENT_NAMES[0])?;
    let claimant_mount_point = controller.client_at(0)?.mount_point.clone();
    let pre_cut = claimant_mount_point.join(RECOVERY_FILE);
    let post_cut = claimant_mount_point.join(POST_CUT_FILE);
    let version_1 = repeated_payload(
        b"snapshot-v2-takeover-pre-cut\n",
        SNAPSHOT_MULTI_CHUNK_BYTES,
    );
    let version_1_sha256 = bytes_sha256(&version_1);
    summary.original_sha256 = version_1_sha256.clone();
    summary.recovery_size_bytes = Some(version_1.len() as u64);
    write_file_sync(&pre_cut, &version_1)?;

    controller.mount(SNAPSHOT_CLIENT_NAMES[1])?;
    let peer = controller.client_at(1)?;
    wait_for_checksum(
        &peer.mount_point.join(RECOVERY_FILE),
        &version_1_sha256,
        SNAPSHOT_STATE_TIMEOUT,
    )?;

    let claimant_client = controller.client_at(0)?;
    let mut creator = spawn_snapshot_create(config, claimant_client, context)?;
    let expected_claimant = format!("cli-{}", creator.child.id());
    let snapshot_id = match wait_for_snapshot_id(&creator.stdout_path, SNAPSHOT_STATE_TIMEOUT) {
        Ok(snapshot_id) => snapshot_id,
        Err(error) => {
            let _ = kill_snapshot_process(&mut creator.child, &creator.job);
            return Err(error);
        }
    };
    let first = match wait_for_snapshot_state(
        config,
        &config.volume_name,
        &snapshot_id,
        |state| {
            state.lifecycle == "creating"
                && state.claimant.as_deref() == Some(expected_claimant.as_str())
        },
        SNAPSHOT_STATE_TIMEOUT,
    ) {
        Ok(state) => state,
        Err(error) => {
            let _ = kill_snapshot_process(&mut creator.child, &creator.job);
            return Err(error);
        }
    };
    summary.snapshot_id = Some(snapshot_id.clone());
    summary.origin = first.origin.clone();
    summary.cut_version = Some(first.cut_version);
    summary.captured_at = first.captured_at.clone();
    summary.claimant = first.claimant.clone();
    summary.lease_generation = first.lease_generation;
    summary.lease_expires_at = first.lease_expires_at.clone();
    summary.state_transitions.push(format_state(&first));
    write_file_sync(&post_cut, b"created after takeover cut\n")?;
    kill_snapshot_process(&mut creator.child, &creator.job).map_err(|error| {
        ScenarioError::Io(io::Error::other(format!(
            "snapshot creator cleanup failed: {error}"
        )))
    })?;

    let takeover = wait_for_snapshot_state(
        config,
        &config.volume_name,
        &snapshot_id,
        |state| {
            (state.lifecycle == "creating"
                && state.lease_generation > first.lease_generation
                && state.claimant.is_some()
                && state.claimant != first.claimant)
                || (state.lifecycle == "ready" && state.lease_generation > first.lease_generation)
        },
        SNAPSHOT_STATE_TIMEOUT,
    )?;
    summary.lease_generation_increased = true;
    summary.claimant = takeover.claimant.clone();
    summary.lease_generation = takeover.lease_generation;
    summary.lease_expires_at = takeover.lease_expires_at.clone();
    summary.state_transitions.push(format_state(&takeover));

    let final_state = wait_for_snapshot_state(
        config,
        &config.volume_name,
        &snapshot_id,
        |state| state.lifecycle == "ready",
        SNAPSHOT_STATE_TIMEOUT,
    )?;
    summary.snapshot_lifecycle = final_state.lifecycle.clone();
    summary.origin = final_state.origin.clone();
    summary.captured_at = final_state.captured_at.clone();
    summary.lease_generation = final_state.lease_generation.max(first.lease_generation);
    summary.lease_expires_at = final_state.lease_expires_at.clone();
    summary.last_error = final_state
        .last_error
        .as_deref()
        .map(|error| redact_diagnostics(config, error));
    summary.artifact_key = final_state.artifact_key.clone();
    summary.artifact_size = final_state.artifact_size;
    summary.artifact_sha256 = final_state.artifact_sha256.clone();
    summary.stale_work_rejected = final_state.lifecycle == "ready"
        && final_state.id == snapshot_id
        && final_state.artifact_sha256.is_some();
    summary.state_transitions.push(format_state(&final_state));
    let snapshot_mount = snapshot_mount_path(context)?;
    let peer = controller.client_at(1)?;
    let snapshot_client = snapshot_mount_client(controller)?;
    cleanup_stale_mount_before_remount(&snapshot_mount)?;
    let mut snapshot_process = spawn_snapshot_mount(
        config,
        snapshot_client,
        context,
        &snapshot_id,
        &snapshot_mount,
    )?;
    let result = (|| {
        wait_for_snapshot_mount(
            &mut snapshot_process,
            &snapshot_mount,
            SNAPSHOT_MOUNT_READY_TIMEOUT,
        )?;
        let snapshot_recovery_path = snapshot_mount.join(RECOVERY_FILE);
        let snapshot_recovery_bytes = fs::read(&snapshot_recovery_path).map_err(|error| {
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!(
                    "read snapshot recovery '{}': {error}",
                    snapshot_recovery_path.display()
                ),
            ))
        })?;
        if Some(snapshot_recovery_bytes.len() as u64) != summary.recovery_size_bytes {
            return Err(ScenarioError::Io(io::Error::other(
                "snapshot recovery size does not match the multi-chunk oracle",
            )));
        }
        let snapshot_recovery_sha256 = bytes_sha256(&snapshot_recovery_bytes);
        if snapshot_recovery_sha256 != summary.original_sha256 {
            return Err(ScenarioError::Io(io::Error::other(
                "snapshot recovery checksum does not match the pre-cut oracle",
            )));
        }
        summary.restored_sha256 = summary.original_sha256.clone();
        let peer_recovery_path = peer.mount_point.join(RECOVERY_FILE);
        let peer_recovery_bytes = fs::read(&peer_recovery_path).map_err(|error| {
            ScenarioError::Io(io::Error::new(
                error.kind(),
                format!(
                    "read peer recovery '{}': {error}",
                    peer_recovery_path.display()
                ),
            ))
        })?;
        summary.peer_sha256 = bytes_sha256(&peer_recovery_bytes);
        summary.post_cut_absent = !snapshot_mount.join(POST_CUT_FILE).exists();
        let snapshot_recovery = snapshot_mount.join(RECOVERY_FILE);
        summary.read_only_failures =
            snapshot_read_only_failures(&snapshot_mount, &snapshot_recovery, &snapshot_recovery);
        write_summary(context, &summary)
    })();
    finish_snapshot_with_cleanup(
        context,
        config,
        &snapshot_mount,
        &mut snapshot_process,
        result,
    )
}

fn initialize_summary(summary: &mut SnapshotV2Summary, config: &ClientControllerConfig) {
    summary.volume = config.volume_name.clone();
    summary.binary_path = config.studiofs_bin.display().to_string();
    summary.binary_sha256 = fs::read(&config.studiofs_bin)
        .ok()
        .map(|bytes| bytes_sha256(&bytes));
}

fn write_summary(
    context: &ScenarioContext,
    summary: &SnapshotV2Summary,
) -> Result<(), ScenarioError> {
    fs::create_dir_all(&context.scenario_dir)?;
    fs::write(
        context.scenario_dir.join("snapshot-summary.json"),
        serde_json::to_vec_pretty(summary)?,
    )?;
    Ok(())
}

fn finish_snapshot_with_cleanup(
    context: &ScenarioContext,
    config: &ClientControllerConfig,
    mount: &Path,
    process: &mut SnapshotMountProcess,
    result: Result<(), ScenarioError>,
) -> Result<(), ScenarioError> {
    let cleanup = cleanup_snapshot_mount(config, context, mount, process);
    match (result, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => Err(ScenarioError::CleanupAfterFailure {
            context: "snapshot scenario".to_string(),
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

struct SnapshotMountProcess {
    child: Child,
    job: ChildJob,
}

struct SnapshotCreateProcess {
    child: Child,
    job: ChildJob,
    stdout_path: PathBuf,
}

fn spawn_snapshot_create(
    config: &ClientControllerConfig,
    client: &ClientSpec,
    context: &ScenarioContext,
) -> Result<SnapshotCreateProcess, ScenarioError> {
    let stdout_path = context.scenario_dir.join("snapshot-create.stdout.log");
    let stderr_path = context.scenario_dir.join("snapshot-create.stderr.log");
    let stdout = fs::File::create(&stdout_path)?;
    let stderr = fs::File::create(&stderr_path)?;
    let mut command = cli_command(config, client, snapshot_create_args());
    command.env(
        SNAPSHOT_TEST_PAUSE_BEFORE_READY_MS_ENV,
        SNAPSHOT_TAKEOVER_PAUSE_BEFORE_READY_MS,
    );
    command
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let (child, job) = spawn_isolated_command(command)?;
    Ok(SnapshotCreateProcess {
        child,
        job,
        stdout_path,
    })
}

fn spawn_snapshot_mount(
    config: &ClientControllerConfig,
    client: &ClientSpec,
    context: &ScenarioContext,
    snapshot_id: &str,
    mount: &Path,
) -> Result<SnapshotMountProcess, ScenarioError> {
    #[cfg(not(windows))]
    fs::create_dir_all(mount)?;
    let stdout = fs::File::create(context.scenario_dir.join("smount.stdout.log"))?;
    let stderr = fs::File::create(context.scenario_dir.join("smount.stderr.log"))?;
    let args = snapshot_mount_args(&config.volume_name, snapshot_id, mount);
    let mut command = cli_command(config, client, args);
    command.arg("--cache-dir").arg(&client.cache_dir);
    command
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let (child, job) = spawn_isolated_command(command)?;
    Ok(SnapshotMountProcess { child, job })
}

fn spawn_isolated_command(mut command: Command) -> Result<(Child, ChildJob), ScenarioError> {
    ChildJob::prepare_command(&mut command)?;
    let mut child = command.spawn()?;
    let job = match ChildJob::assign_to_child(&child) {
        Ok(job) => job,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(ScenarioError::Io(io::Error::new(
                error.kind(),
                format!("failed to isolate snapshot process: {error}"),
            )));
        }
    };
    Ok((child, job))
}

fn cleanup_snapshot_mount(
    config: &ClientControllerConfig,
    context: &ScenarioContext,
    mount: &Path,
    process: &mut SnapshotMountProcess,
) -> Result<(), ScenarioError> {
    let client = client_spec_for(
        &context.sandbox,
        &config.volume_name,
        "snapshot",
        mount.to_path_buf(),
    );
    let unmount_result = run_cli_command(
        config,
        &client,
        context,
        "snapshot-umount",
        &["umount", &mount.display().to_string()],
    );
    let process_result = match process.child.try_wait() {
        Ok(Some(_)) => Ok(()),
        Ok(None) if unmount_result.is_ok() => {
            match wait_for_child_exit(&mut process.child, SNAPSHOT_UNMOUNT_GRACE) {
                Ok(true) => Ok(()),
                Ok(false) => kill_snapshot_process(&mut process.child, &process.job),
                Err(error) => Err(ScenarioError::Io(error)),
            }
        }
        Ok(None) => kill_snapshot_process(&mut process.child, &process.job),
        Err(error) => Err(ScenarioError::Io(error)),
    };
    match (unmount_result, process_result) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(unmount), Err(process)) => Err(ScenarioError::Io(io::Error::other(format!(
            "snapshot unmount failed: {unmount}; process cleanup failed: {process}"
        )))),
    }
}

fn kill_snapshot_process(process: &mut Child, job: &ChildJob) -> Result<(), ScenarioError> {
    match job.force_kill() {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(ScenarioError::Io(error)),
    }
    match process.try_wait() {
        Ok(Some(_)) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Ok(None) => {}
        Err(error) => return Err(ScenarioError::Io(error)),
    }
    match process.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(ScenarioError::Io(error)),
    }
    match wait_for_child_exit(process, SNAPSHOT_PROCESS_TERMINATION_TIMEOUT) {
        Ok(true) => Ok(()),
        Ok(false) => Err(ScenarioError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "timed out waiting for snapshot process {} to terminate",
                process.id()
            ),
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ScenarioError::Io(error)),
    }
}

fn run_cli_command(
    config: &ClientControllerConfig,
    client: &ClientSpec,
    context: &ScenarioContext,
    action: &str,
    args: &[&str],
) -> Result<String, ScenarioError> {
    let output = cli_command(config, client, args).output()?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    fs::write(
        context.scenario_dir.join(format!("{action}.stdout.log")),
        &output.stdout,
    )?;
    fs::write(
        context.scenario_dir.join(format!("{action}.stderr.log")),
        &output.stderr,
    )?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ScenarioError::Io(io::Error::other(format!(
            "{action} failed with status {}; stderr={}",
            output.status,
            redact_diagnostics(config, &stderr)
        ))));
    }
    Ok(stdout)
}

fn cli_command<I, S>(config: &ClientControllerConfig, client: &ClientSpec, args: I) -> Command
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(&config.studiofs_bin);
    command.args(args);
    command.env("STUDIOFS_CLIENT_ROOT", &client.client_root);
    command.env("STUDIOFS_DAEMON_LOG_DIR", client.client_root.join("logs"));
    command.env("STUDIOFS_DAEMON_LOG_STEM", "studiofs-snapshot");
    if let Some(overrides) = config.client_env_overrides.get(&client.name) {
        for (key, value) in overrides {
            command.env(key, value);
        }
    }
    command
}

pub(in crate::scenario) fn redact_diagnostics(
    config: &ClientControllerConfig,
    text: &str,
) -> String {
    let mut redacted = redact_dsn_diagnostics(&config.meta_dsn, text);
    redact_value(
        &mut redacted,
        &config.s3.access_key,
        "<redacted-access-key>",
    );
    redact_value(
        &mut redacted,
        &config.s3.secret_key,
        "<redacted-secret-key>",
    );
    redact_value(
        &mut redacted,
        &percent_encode(config.s3.access_key.as_bytes()),
        "<redacted-access-key>",
    );
    redact_value(
        &mut redacted,
        &percent_encode(config.s3.secret_key.as_bytes()),
        "<redacted-secret-key>",
    );
    redacted
}

pub(in crate::scenario) fn parse_snapshot_id(stdout: &str) -> Option<String> {
    let marker = "Creating manual snapshot ";
    stdout.lines().find_map(|line| {
        line.strip_prefix(marker)
            .and_then(|rest| rest.strip_suffix("..."))
            .map(str::to_string)
    })
}

pub(in crate::scenario) fn snapshot_create_args() -> [&'static str; 2] {
    ["snapshot", "create"]
}

pub(in crate::scenario) fn snapshot_mount_args(
    volume: &str,
    snapshot_id: &str,
    mount: &Path,
) -> Vec<String> {
    vec![
        "smount".to_string(),
        volume.to_string(),
        snapshot_id.to_string(),
        mount.display().to_string(),
    ]
}

fn wait_for_snapshot_id(path: &Path, timeout: Duration) -> Result<String, ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(id) = parse_snapshot_id(&fs::read_to_string(path).unwrap_or_default()) {
            return Ok(id);
        }
        if Instant::now() >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "snapshot create did not publish a snapshot ID; log={}",
                    path.display()
                ),
            )));
        }
        thread::sleep(SNAPSHOT_POLL_INTERVAL);
    }
}

fn wait_for_snapshot_state<F>(
    config: &ClientControllerConfig,
    volume: &str,
    snapshot_id: &str,
    predicate: F,
    timeout: Duration,
) -> Result<SnapshotState, ScenarioError>
where
    F: Fn(&SnapshotState) -> bool,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(ScenarioError::Io)?;
    let dsn = config.meta_dsn.clone();
    let volume = volume.to_string();
    let snapshot_id = snapshot_id.to_string();
    runtime.block_on(wait_for_snapshot_state_async(
        &dsn,
        &volume,
        &snapshot_id,
        predicate,
        timeout,
    ))
}

struct SnapshotStateConnection {
    client: tokio_postgres::Client,
    connection_task: tokio::task::JoinHandle<()>,
}

impl Drop for SnapshotStateConnection {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

async fn connect_snapshot_state(dsn: &str) -> Result<SnapshotStateConnection, ScenarioError> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await.map_err(|error| {
        ScenarioError::Io(io::Error::other(format!(
            "snapshot state connection failed: {}",
            redact_pg_diagnostics(dsn, &error.to_string())
        )))
    })?;
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(SnapshotStateConnection {
        client,
        connection_task,
    })
}

async fn wait_for_snapshot_state_async<F>(
    dsn: &str,
    volume: &str,
    snapshot_id: &str,
    predicate: F,
    timeout: Duration,
) -> Result<SnapshotState, ScenarioError>
where
    F: Fn(&SnapshotState) -> bool,
{
    let deadline = Instant::now() + timeout;
    let mut connection = None;
    loop {
        if connection.is_none() {
            match connect_snapshot_state(dsn).await {
                Ok(new_connection) => connection = Some(new_connection),
                Err(error) => {
                    let observed = error.to_string();
                    if Instant::now() >= deadline {
                        break Err(ScenarioError::Io(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!(
                                "snapshot state timeout for {snapshot_id}; last observed state: {}",
                                observed
                            ),
                        )));
                    }
                    tokio::time::sleep(SNAPSHOT_POLL_INTERVAL).await;
                    continue;
                }
            }
        }
        let query_result = match connection.as_ref() {
            Some(connection) => {
                query_snapshot_state(&connection.client, dsn, volume, snapshot_id).await
            }
            None => continue,
        };
        let observed = match query_result {
            Ok(Some(state)) if predicate(&state) => break Ok(state),
            Ok(Some(state)) => format_state(&state),
            Ok(None) => "snapshot state row not visible yet".to_string(),
            Err(error) => {
                connection = None;
                error.to_string()
            }
        };
        if Instant::now() >= deadline {
            break Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "snapshot state timeout for {snapshot_id}; last observed state: {}",
                    observed
                ),
            )));
        }
        tokio::time::sleep(SNAPSHOT_POLL_INTERVAL).await;
    }
}

async fn query_snapshot_state(
    client: &tokio_postgres::Client,
    dsn: &str,
    volume: &str,
    snapshot_id: &str,
) -> Result<Option<SnapshotState>, ScenarioError> {
    let row = client
        .query_opt(SNAPSHOT_STATE_QUERY, &[&volume, &snapshot_id])
        .await
        .map_err(|error| {
            ScenarioError::Io(io::Error::other(format!(
                "snapshot state query failed: {}",
                redact_pg_diagnostics(dsn, &error.to_string())
            )))
        })?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(SnapshotState {
        id: row.get(0),
        origin: row.get(1),
        cut_version: row.get(2),
        lifecycle: row.get(3),
        job_state: row.get(4),
        captured_at: row.get(5),
        claimant: row.get(6),
        lease_expires_at: row.get(7),
        lease_generation: row.get(8),
        last_error: row.get(9),
        artifact_key: row.get(10),
        artifact_size: row.get(11),
        artifact_sha256: row.get(12),
    }))
}

pub(in crate::scenario) fn format_state(state: &SnapshotState) -> String {
    format!(
        "id={} origin={} lifecycle={} job_state={} cut_version={} captured_at={} claimant={} lease_generation={} lease_expires_at={} last_error={} artifact_key={} artifact_size={} artifact_sha256={}",
        state.id,
        state.origin,
        state.lifecycle,
        state.job_state,
        state.cut_version,
        state.captured_at.as_deref().unwrap_or("none"),
        state.claimant.as_deref().unwrap_or("none"),
        state.lease_generation,
        state.lease_expires_at.as_deref().unwrap_or("none"),
        if state.last_error.is_some() {
            "present"
        } else {
            "none"
        },
        state.artifact_key.as_deref().unwrap_or("none"),
        state
            .artifact_size
            .map_or_else(|| "none".to_string(), |size| size.to_string()),
        state.artifact_sha256.as_deref().unwrap_or("none")
    )
}

fn redact_pg_diagnostics(dsn: &str, text: &str) -> String {
    redact_dsn_diagnostics(dsn, text)
}

fn redact_dsn_diagnostics(dsn: &str, text: &str) -> String {
    let mut redacted = text.to_string();
    redact_value(&mut redacted, dsn, "<redacted-dsn>");
    if let Ok(url) = Url::parse(dsn) {
        if let Some(password) = url.password() {
            redact_encoded_variants(
                &mut redacted,
                &percent_encode(password.as_bytes()),
                "<redacted-dsn-password>",
            );
        }
        if let Some(raw_password) = raw_url_password(dsn) {
            redact_encoded_variants(&mut redacted, raw_password, "<redacted-dsn-password>");
        }
    } else {
        for token in dsn.split_whitespace() {
            if let Some(password) = token.strip_prefix("password=") {
                redact_encoded_variants(&mut redacted, password, "<redacted-dsn-password>");
            }
        }
    }
    redacted
}

fn raw_url_password(dsn: &str) -> Option<&str> {
    let authority = dsn
        .strip_prefix("postgres://")
        .or_else(|| dsn.strip_prefix("postgresql://"))?;
    let authority = authority.split(['/', '?', '#']).next()?;
    let user_info = authority.rsplit_once('@')?.0;
    user_info.rsplit_once(':').map(|(_, password)| password)
}

fn redact_value(text: &mut String, value: &str, replacement: &str) {
    if !value.is_empty() {
        *text = text.replace(value, replacement);
    }
}

fn redact_encoded_variants(text: &mut String, value: &str, replacement: &str) {
    redact_value(text, value, replacement);
    redact_percent_encoded_matches(text, value, replacement);
    if let Ok(decoded) = percent_decode_str(value).decode_utf8() {
        redact_value(text, decoded.as_ref(), replacement);
    }
}

fn redact_percent_encoded_matches(text: &mut String, value: &str, replacement: &str) {
    if !value.as_bytes().windows(3).any(|window| {
        window[0] == b'%' && window[1].is_ascii_hexdigit() && window[2].is_ascii_hexdigit()
    }) {
        return;
    }
    let text_bytes = text.as_bytes();
    let value_bytes = value.as_bytes();
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < text_bytes.len() {
        let Some(match_end) = match_percent_encoded(text_bytes, value_bytes, start) else {
            start += 1;
            continue;
        };
        let match_start = start;
        if text.is_char_boundary(match_start) && text.is_char_boundary(match_end) {
            ranges.push((match_start, match_end));
        }
        start = match_end;
    }
    for (match_start, match_end) in ranges.into_iter().rev() {
        text.replace_range(match_start..match_end, replacement);
    }
}

fn match_percent_encoded(text: &[u8], value: &[u8], start: usize) -> Option<usize> {
    let mut text_index = start;
    let mut value_index = 0;
    while value_index < value.len() {
        if value_index + 2 < value.len()
            && value[value_index] == b'%'
            && value[value_index + 1].is_ascii_hexdigit()
            && value[value_index + 2].is_ascii_hexdigit()
        {
            let high = hex_value(value[value_index + 1])?;
            let low = hex_value(value[value_index + 2])?;
            let raw_matches = text_index + 3 <= text.len()
                && text[text_index] == b'%'
                && text[text_index + 1].eq_ignore_ascii_case(&value[value_index + 1])
                && text[text_index + 2].eq_ignore_ascii_case(&value[value_index + 2]);
            if raw_matches {
                text_index += 3;
            } else if text.get(text_index).copied() == Some((high << 4) | low) {
                text_index += 1;
            } else {
                return None;
            }
            value_index += 3;
        } else if text.get(text_index).copied() == Some(value[value_index]) {
            text_index += 1;
            value_index += 1;
        } else {
            return None;
        }
    }
    Some(text_index)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn percent_encode(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len() * 3);
    for &byte in value {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(byte as char);
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}

fn snapshot_mount_path(context: &ScenarioContext) -> Result<PathBuf, ScenarioError> {
    available_snapshot_mount_point(&context.scenario_dir).ok_or_else(|| {
        ScenarioError::Io(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "no mount point is available for snapshot mount",
        ))
    })
}

fn wait_for_snapshot_mount(
    process: &mut SnapshotMountProcess,
    path: &Path,
    timeout: Duration,
) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = process.child.try_wait()? {
            return Err(ScenarioError::Io(io::Error::other(format!(
                "snapshot mount exited before readiness with status {status}"
            ))));
        }
        if mount_point_is_active(path)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("snapshot mount did not become ready: {}", path.display()),
            )));
        }
        thread::sleep(SNAPSHOT_POLL_INTERVAL);
    }
}

fn write_file_sync(path: &Path, payload: &[u8]) -> Result<(), ScenarioError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(())
}

fn wait_for_checksum(path: &Path, expected: &str, timeout: Duration) -> Result<(), ScenarioError> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(payload) = fs::read(path)
            && bytes_sha256(&payload) == expected
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(ScenarioError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("checksum did not converge for {}", path.display()),
            )));
        }
        thread::sleep(SNAPSHOT_POLL_INTERVAL);
    }
}

fn snapshot_read_only_failures(
    mount: &Path,
    existing: &Path,
    unlink_target: &Path,
) -> Vec<ReadOnlyFailure> {
    let mut failures = Vec::new();
    let mut record = |operation: &str, result: io::Result<()>| {
        failures.push(ReadOnlyFailure {
            operation: operation.to_string(),
            rejected: result.is_err(),
            error: result.err().map(|error| error.to_string()),
        });
    };
    record(
        "create",
        fs::File::create(mount.join("must-not-exist.txt")).map(|_| ()),
    );
    record("write", fs::write(existing, b"must-not-write\n"));
    record(
        "truncate",
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(existing)
            .map(|_| ()),
    );
    record("rename", fs::rename(existing, mount.join("renamed.txt")));
    record("unlink", fs::remove_file(unlink_target));
    record("mkdir", fs::create_dir(mount.join("must-not-mkdir")));
    failures
}

fn repeated_payload(prefix: &[u8], len: usize) -> Vec<u8> {
    let mut payload = Vec::with_capacity(len);
    while payload.len() < len {
        let take = (len - payload.len()).min(prefix.len());
        payload.extend_from_slice(&prefix[..take]);
    }
    payload
}

fn bytes_sha256(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_summary_round_trips_through_json() {
        let summary = SnapshotV2Summary::new(SnapshotV2Kind::ManualRecovery);
        let encoded = serde_json::to_vec(&summary).expect("serialize summary");
        let decoded: SnapshotV2Summary =
            serde_json::from_slice(&encoded).expect("deserialize summary");

        assert_eq!(decoded, summary);
    }

    #[test]
    fn snapshot_run_requires_compose_infrastructure() {
        let temp = studiofs_test_support::workspace_target_tempdir("snapshot-run-");
        let context = ScenarioContext {
            run_id: "run".to_string(),
            seed: 0,
            sandbox: temp.path().join("sandbox"),
            scenario_dir: temp.path().join("scenario"),
            client_controller_config: None,
        };
        let scenario = SnapshotV2Scenario::new(SnapshotV2Kind::ManualRecovery);

        assert!(matches!(
            scenario.run(&context),
            Err(ScenarioError::MissingComposeInfrastructure)
        ));
    }

    #[test]
    fn snapshot_validation_rejects_an_incomplete_summary() {
        let temp = studiofs_test_support::workspace_target_tempdir("snapshot-validation-");
        let context = ScenarioContext {
            run_id: "validation".to_string(),
            seed: 0,
            sandbox: temp.path().join("sandbox"),
            scenario_dir: temp.path().join("scenario"),
            client_controller_config: None,
        };
        fs::create_dir_all(&context.scenario_dir).expect("create scenario directory");
        let summary = SnapshotV2Summary::new(SnapshotV2Kind::ManualRecovery);
        fs::write(
            context.scenario_dir.join("snapshot-summary.json"),
            serde_json::to_vec(&summary).expect("serialize summary"),
        )
        .expect("write summary");

        let scenario = SnapshotV2Scenario::new(SnapshotV2Kind::ManualRecovery);
        assert!(scenario.validate(&context).is_err());
    }

    #[test]
    fn snapshot_scenario_reserves_a_discovered_client_for_snapshot_mount() {
        assert!(
            SNAPSHOT_CLIENT_NAMES.contains(&"snapshot"),
            "snapshot mount needs a discovered client root separate from live mounts"
        );
    }
}

fn snapshot_mount_client(
    controller: &MultiClientController<SystemClientProcessExecutor>,
) -> Result<&ClientSpec, ScenarioError> {
    controller
        .clients()
        .into_iter()
        .find(|client| client.name == SNAPSHOT_MOUNT_CLIENT_NAME)
        .ok_or_else(|| {
            ScenarioError::Io(io::Error::other(
                "snapshot scenario did not bootstrap its isolated mount client",
            ))
        })
}
