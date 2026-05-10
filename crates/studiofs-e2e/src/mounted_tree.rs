//! Mounted filesystem tree snapshots and convergence validation.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use studiofs_scenario::invariant::{
    ClientLogicalSnapshot, LogicalEntry, LogicalEntryKind, LogicalSnapshot,
    validate_logical_invariants,
};
use thiserror::Error;
use tokio_postgres::NoTls;

use crate::infra::{CommandExecutor, CommandSpec, SystemCommandExecutor};

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const METADATA_DIAGNOSTICS_SCHEMA_VERSION: u32 = 1;
const READ_BUFFER_BYTES: usize = 64 * 1024;
const CLIENT_DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(5);
const REMOTE_METADATA_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ClientMount {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub mount_point: PathBuf,
    /// Field $name.
    pub diagnostics_probe: Option<ClientDiagnosticsProbe>,
    /// Field $name.
    pub remote_metadata_probe: Option<RemoteMetadataProbe>,
}

impl ClientMount {
    /// Function $name.
    pub fn new(client: impl Into<String>, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            client: client.into(),
            mount_point: mount_point.into(),
            diagnostics_probe: None,
            remote_metadata_probe: None,
        }
    }

    /// Function $name.
    pub fn with_diagnostics_probe(
        mut self,
        studiofs_bin: impl Into<PathBuf>,
        client_root: impl Into<PathBuf>,
    ) -> Self {
        self.diagnostics_probe = Some(ClientDiagnosticsProbe {
            studiofs_bin: studiofs_bin.into(),
            client_root: client_root.into(),
            timeout: CLIENT_DIAGNOSTICS_TIMEOUT,
        });
        self
    }

    /// Function $name.
    pub fn with_remote_metadata_probe(
        mut self,
        meta_dsn: impl Into<String>,
        volume_name: impl Into<String>,
    ) -> Self {
        self.remote_metadata_probe = Some(RemoteMetadataProbe {
            meta_dsn: meta_dsn.into(),
            volume_name: volume_name.into(),
            timeout: REMOTE_METADATA_TIMEOUT,
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ClientDiagnosticsProbe {
    /// Field $name.
    pub studiofs_bin: PathBuf,
    /// Field $name.
    pub client_root: PathBuf,
    /// Field $name.
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct RemoteMetadataProbe {
    /// Field $name.
    pub meta_dsn: String,
    /// Current scenario volume name used to scope remote metadata diagnostics.
    pub volume_name: String,
    /// Field $name.
    pub timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct MountedTreeSnapshot {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub mount_point: String,
    /// Field $name.
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct TreeEntry {
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub entry_type: TreeEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub modified_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub symlink_target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum TreeEntryKind {
    /// Variant $name.
    Directory,
    /// Variant $name.
    File,
    /// Variant $name.
    Symlink,
    /// Variant $name.
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct TreeComparison {
    /// Field $name.
    pub converged: bool,
    /// Field $name.
    pub reference_client: Option<String>,
    /// Field $name.
    pub compared_clients: Vec<String>,
    /// Field $name.
    pub differences: Vec<TreeDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct TreeMetadataDiagnostics {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub generated_unix_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub expected_remote: Option<RemoteMetadataDiagnostics>,
    /// Field $name.
    pub clients: Vec<ClientMetadataDiagnostics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Mounted-client quiescence classification captured on convergence timeout.
pub enum ClientQuiescence {
    /// The client still has known metadata or upload work in progress.
    Busy,
    /// All known metadata and upload work counters are zero.
    Quiescent,
    /// The diagnostic probe did not expose enough counters to classify state.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct RemoteMetadataDiagnostics {
    /// Field $name.
    pub status: String,
    /// Volume name used by the remote metadata diagnostic query.
    pub volume_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub max_batch_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub max_seq_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub event_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ClientMetadataDiagnostics {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub mount_point: String,
    /// Field $name.
    pub artifact: String,
    /// Field $name.
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub local_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub namespace_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub flushed_event_seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub pending_metadata_ops: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub inflight_metadata_ops: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub notifications_received: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub listener_reconnects: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub fallback_poll_discoveries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub sync_errors: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub last_successful_sync_unix_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Upload payloads waiting to be stored by this client.
    pub pending_uploads: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Total bytes represented by pending upload payloads.
    pub pending_upload_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Upload payloads currently being transferred by this client.
    pub active_uploads: Option<u64>,
    /// Busy, quiescent, or unknown classification for timeout diagnostics.
    pub quiescence: ClientQuiescence,
    /// Counters or probe failures explaining the quiescence classification.
    pub quiescence_reasons: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct TreeDifference {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub reference_client: Option<String>,
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub actual: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct NamespaceInvariantViolation {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Struct $name.
pub struct ConvergenceOutcome {
    /// Field $name.
    pub attempts: usize,
    /// Field $name.
    pub elapsed_ms: u128,
    /// Field $name.
    pub snapshots: Vec<MountedTreeSnapshot>,
    /// Field $name.
    pub comparison: TreeComparison,
}

#[derive(Debug, Error)]
/// Enum $name.
pub enum MountedTreeError {
    #[error("mounted tree I/O failed at {path}: {source}")]
    /// Variant $name.
    Io {
        /// The path value.
        path: PathBuf,
        /// The source value.
        source: io::Error,
    },
    #[error("mounted tree JSON failed at {path}: {source}")]
    /// Variant $name.
    Json {
        /// Field $name.
        path: PathBuf,
        /// Field $name.
        source: serde_json::Error,
    },
    #[error(
        "mounted filesystem trees did not converge after {attempts} attempts in {elapsed_ms}ms; artifacts written to {artifact_dir}"
    )]
    /// Variant $name.
    ConvergenceTimedOut {
        /// Field $name.
        attempts: usize,
        /// Field $name.
        elapsed_ms: u128,
        /// Field $name.
        artifact_dir: PathBuf,
        /// Field $name.
        comparison: Box<TreeComparison>,
    },
    #[error(
        "mounted filesystem trees could not be snapshotted after {attempts} attempts in {elapsed_ms}ms; last error: {source}"
    )]
    /// Variant $name.
    SnapshotUnavailableTimedOut {
        /// Field $name.
        attempts: usize,
        /// Field $name.
        elapsed_ms: u128,
        /// Field $name.
        artifact_dir: PathBuf,
        /// Field $name.
        source: Box<MountedTreeError>,
    },
    #[error("mounted tree path is not valid UTF-8: {path}")]
    /// Variant $name.
    NonUtf8Path {
        /// The path value.
        path: PathBuf,
    },
}

/// Function $name.
pub fn snapshot_client_tree(
    client: impl Into<String>,
    mount_point: impl AsRef<Path>,
) -> Result<MountedTreeSnapshot, MountedTreeError> {
    let client = client.into();
    let mount_point = mount_point.as_ref();
    let mut entries = Vec::new();
    scan_dir(mount_point, mount_point, &mut entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(MountedTreeSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        client,
        mount_point: path_to_json_string(mount_point)?,
        entries,
    })
}

/// Function $name.
pub fn compare_snapshots(snapshots: &[MountedTreeSnapshot]) -> TreeComparison {
    let compared_clients = snapshots
        .iter()
        .map(|snapshot| snapshot.client.clone())
        .collect::<Vec<_>>();
    let Some(reference) = snapshots.first() else {
        return TreeComparison {
            converged: true,
            reference_client: None,
            compared_clients,
            differences: Vec::new(),
        };
    };

    let reference_entries = entries_by_path(reference);
    let mut differences = Vec::new();
    for snapshot in snapshots.iter().skip(1) {
        let actual_entries = entries_by_path(snapshot);
        for (path, expected) in &reference_entries {
            match actual_entries.get(path) {
                Some(actual) => compare_entry(
                    &reference.client,
                    &snapshot.client,
                    path,
                    expected,
                    actual,
                    &mut differences,
                ),
                None => differences.push(TreeDifference {
                    client: snapshot.client.clone(),
                    reference_client: Some(reference.client.clone()),
                    path: (*path).to_string(),
                    message: "entry is missing".to_string(),
                    expected: Some(entry_contract(expected)),
                    actual: None,
                }),
            }
        }
        for (path, actual) in &actual_entries {
            if !reference_entries.contains_key(path) {
                differences.push(TreeDifference {
                    client: snapshot.client.clone(),
                    reference_client: Some(reference.client.clone()),
                    path: (*path).to_string(),
                    message: "entry is extra".to_string(),
                    expected: None,
                    actual: Some(entry_contract(actual)),
                });
            }
        }
    }

    TreeComparison {
        converged: differences.is_empty(),
        reference_client: Some(reference.client.clone()),
        compared_clients,
        differences,
    }
}

/// Function $name.
pub fn validate_namespace_invariants(
    snapshot: &MountedTreeSnapshot,
    expected_absent_paths: &[&str],
) -> Vec<NamespaceInvariantViolation> {
    let logical = mounted_snapshots_to_logical_snapshot(
        std::slice::from_ref(snapshot),
        expected_absent_paths,
    );
    validate_logical_invariants(&logical)
        .violations
        .into_iter()
        .filter_map(|violation| {
            let message = match violation.invariant.as_str() {
                "canonical_path_ownership" => "duplicate canonical path",
                "no_unexpected_resurrection" => "unexpected resurrection",
                "valid_directory_tree_structure" => "missing parent directory",
                _ => return None,
            };
            Some(NamespaceInvariantViolation {
                client: violation.client.unwrap_or_else(|| snapshot.client.clone()),
                path: violation.path.unwrap_or_default(),
                message: message.to_string(),
            })
        })
        .collect()
}

/// Function $name.
pub fn mounted_snapshots_to_logical_snapshot(
    snapshots: &[MountedTreeSnapshot],
    expected_absent_paths: &[&str],
) -> LogicalSnapshot {
    let mut logical = LogicalSnapshot::new(
        snapshots
            .iter()
            .map(|snapshot| ClientLogicalSnapshot {
                client: snapshot.client.clone(),
                entries: snapshot
                    .entries
                    .iter()
                    .map(mounted_entry_to_logical_entry)
                    .collect(),
            })
            .collect(),
    );
    logical.expected_absent_paths = expected_absent_paths
        .iter()
        .map(|path| (*path).to_string())
        .collect();
    logical
}

/// Function $name.
pub fn write_divergence_artifacts(
    artifact_dir: impl AsRef<Path>,
    snapshots: &[MountedTreeSnapshot],
    comparison: &TreeComparison,
) -> Result<Vec<String>, MountedTreeError> {
    write_divergence_artifacts_with_diagnostics(artifact_dir, snapshots, comparison, None)
}

fn write_divergence_artifacts_with_diagnostics(
    artifact_dir: impl AsRef<Path>,
    snapshots: &[MountedTreeSnapshot],
    comparison: &TreeComparison,
    metadata_diagnostics: Option<&TreeMetadataDiagnostics>,
) -> Result<Vec<String>, MountedTreeError> {
    let artifact_dir = artifact_dir.as_ref();
    create_dir_all(artifact_dir)?;
    let mut artifacts = Vec::with_capacity(snapshots.len() + 2);

    for (index, snapshot) in snapshots.iter().enumerate() {
        let name = format!("tree-client-{}.json", index + 1);
        write_pretty_json(&artifact_dir.join(&name), snapshot)?;
        artifacts.push(name);
    }

    if let Some(metadata_diagnostics) = metadata_diagnostics {
        write_pretty_json(
            &artifact_dir.join("metadata-diagnostics.json"),
            metadata_diagnostics,
        )?;
        artifacts.push("metadata-diagnostics.json".to_string());
    }

    let diff = json!({
        "converged": comparison.converged,
        "reference_client": comparison.reference_client,
        "compared_clients": comparison.compared_clients,
        "differences": comparison.differences,
        "metadata_diagnostics": metadata_diagnostics,
    });
    write_pretty_json(&artifact_dir.join("diff.json"), &diff)?;
    artifacts.push("diff.json".to_string());
    Ok(artifacts)
}

/// Function $name.
pub fn wait_for_convergence(
    mounts: &[ClientMount],
    artifact_dir: impl AsRef<Path>,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, MountedTreeError> {
    let artifact_dir = artifact_dir.as_ref();
    let started = Instant::now();
    let deadline = started + timeout;
    let mut attempts = 0;
    let mut last_divergence = None;
    let mut last_snapshot_error = None;

    loop {
        attempts += 1;
        match snapshot_mounts(mounts) {
            Ok(snapshots) => {
                let comparison = compare_snapshots(&snapshots);
                let elapsed_ms = started.elapsed().as_millis();
                if comparison.converged {
                    return Ok(ConvergenceOutcome {
                        attempts,
                        elapsed_ms,
                        snapshots,
                        comparison,
                    });
                }
                last_divergence = Some((snapshots, comparison));
            }
            Err(error) => {
                last_snapshot_error = Some(error);
            }
        }

        let elapsed_ms = started.elapsed().as_millis();
        if Instant::now() >= deadline {
            let Some((snapshots, comparison)) = last_divergence else {
                let source = last_snapshot_error.unwrap_or(MountedTreeError::ConvergenceTimedOut {
                    attempts,
                    elapsed_ms,
                    artifact_dir: artifact_dir.to_path_buf(),
                    comparison: Box::new(TreeComparison {
                        converged: false,
                        reference_client: None,
                        compared_clients: mounts.iter().map(|mount| mount.client.clone()).collect(),
                        differences: Vec::new(),
                    }),
                });
                return Err(MountedTreeError::SnapshotUnavailableTimedOut {
                    attempts,
                    elapsed_ms,
                    artifact_dir: artifact_dir.to_path_buf(),
                    source: Box::new(source),
                });
            };
            let metadata_diagnostics = collect_tree_metadata_diagnostics(mounts, artifact_dir);
            write_divergence_artifacts_with_diagnostics(
                artifact_dir,
                &snapshots,
                &comparison,
                metadata_diagnostics.as_ref(),
            )?;
            return Err(MountedTreeError::ConvergenceTimedOut {
                attempts,
                elapsed_ms,
                artifact_dir: artifact_dir.to_path_buf(),
                comparison: Box::new(comparison),
            });
        }
        std::thread::sleep(next_sleep(poll_interval, deadline));
    }
}

fn collect_tree_metadata_diagnostics(
    mounts: &[ClientMount],
    artifact_dir: &Path,
) -> Option<TreeMetadataDiagnostics> {
    let has_client_probe = mounts.iter().any(|mount| mount.diagnostics_probe.is_some());
    let remote_probe = mounts
        .iter()
        .find_map(|mount| mount.remote_metadata_probe.as_ref());
    if !has_client_probe && remote_probe.is_none() {
        return None;
    }

    let expected_remote = remote_probe.map(query_remote_metadata);
    let executor = SystemCommandExecutor;
    let clients = mounts
        .iter()
        .enumerate()
        .filter_map(|(index, mount)| {
            collect_client_metadata_diagnostics(&executor, artifact_dir, index, mount)
        })
        .collect();
    Some(TreeMetadataDiagnostics {
        schema_version: METADATA_DIAGNOSTICS_SCHEMA_VERSION,
        generated_unix_ms: now_unix_millis(),
        expected_remote,
        clients,
    })
}

fn collect_client_metadata_diagnostics(
    executor: &dyn CommandExecutor,
    artifact_dir: &Path,
    index: usize,
    mount: &ClientMount,
) -> Option<ClientMetadataDiagnostics> {
    let probe = mount.diagnostics_probe.as_ref()?;
    let artifact = format!("metadata-client-{}.json", index + 1);
    let mount_point = path_to_json_string(&mount.mount_point).unwrap_or_else(|_| {
        mount
            .mount_point
            .to_string_lossy()
            .replace('\\', "/")
            .to_string()
    });
    let command = CommandSpec {
        program: probe.studiofs_bin.to_string_lossy().into_owned(),
        args: vec![
            "internal".to_string(),
            "diagnostics-attach".to_string(),
            mount.mount_point.to_string_lossy().into_owned(),
            "--recent-events-limit".to_string(),
            "8".to_string(),
        ],
        env: vec![(
            "STUDIOFS_CLIENT_ROOT".to_string(),
            probe.client_root.to_string_lossy().into_owned(),
        )],
        cwd: None,
        timeout: Some(probe.timeout),
        stdout_path: None,
        stderr_path: None,
    };

    let output = executor.run(&command);
    let raw = match &output {
        Ok(output) => json!({
            "client": mount.client,
            "mount_point": mount_point,
            "command": {
                "program": &command.program,
                "args": &command.args,
                "env": &command.env,
                "timeout_ms": probe.timeout.as_millis(),
            },
            "exit_code": output.status,
            "stdout_json": serde_json::from_str::<Value>(&output.stdout).ok(),
            "stdout": output.stdout,
            "stderr": output.stderr,
        }),
        Err(error) => json!({
            "client": mount.client,
            "mount_point": mount_point,
            "command": {
                "program": &command.program,
                "args": &command.args,
                "env": &command.env,
                "timeout_ms": probe.timeout.as_millis(),
            },
            "error": error.to_string(),
        }),
    };
    let _ = write_pretty_json(&artifact_dir.join(&artifact), &raw);

    match output {
        Ok(output) => {
            let stdout_json = serde_json::from_str::<Value>(&output.stdout).ok();
            let metadata = stdout_json.as_ref().and_then(|value| {
                value.pointer("/runtime_snapshot/write_path/metadata_invalidation")
            });
            let upload = stdout_json
                .as_ref()
                .and_then(|value| value.pointer("/runtime_snapshot/upload"));
            let write_path = stdout_json
                .as_ref()
                .and_then(|value| value.pointer("/runtime_snapshot/write_path"));
            let pending_metadata_ops = metadata
                .and_then(|value| value.get("pending_metadata_ops"))
                .and_then(Value::as_u64);
            let inflight_metadata_ops = metadata
                .and_then(|value| value.get("inflight_metadata_ops"))
                .and_then(Value::as_u64);
            let pending_uploads = upload
                .and_then(|value| value.get("pending_uploads"))
                .and_then(Value::as_u64)
                .or_else(|| {
                    write_path
                        .and_then(|value| value.get("pending_uploads"))
                        .and_then(Value::as_u64)
                });
            let pending_upload_bytes = upload
                .and_then(|value| value.get("pending_upload_bytes"))
                .and_then(Value::as_u64);
            let active_uploads = upload
                .and_then(|value| value.get("active_uploads"))
                .and_then(Value::as_u64);
            let (quiescence, quiescence_reasons) = classify_client_quiescence(
                output.status,
                pending_metadata_ops,
                inflight_metadata_ops,
                pending_uploads,
                pending_upload_bytes,
                active_uploads,
            );
            Some(ClientMetadataDiagnostics {
                client: mount.client.clone(),
                mount_point,
                artifact,
                status: if output.status == 0 {
                    "collected".to_string()
                } else {
                    "command_failed".to_string()
                },
                exit_code: Some(output.status),
                local_version: metadata
                    .and_then(|value| value.get("metadata_version_token"))
                    .and_then(value_to_string),
                namespace_version: metadata
                    .and_then(|value| value.get("namespace_version_token"))
                    .and_then(value_to_string),
                flushed_event_seq: metadata
                    .and_then(|value| value.get("flushed_event_seq"))
                    .and_then(Value::as_u64),
                pending_metadata_ops,
                inflight_metadata_ops,
                notifications_received: metadata
                    .and_then(|value| value.get("notifications_received"))
                    .and_then(Value::as_u64),
                listener_reconnects: metadata
                    .and_then(|value| value.get("listener_reconnects"))
                    .and_then(Value::as_u64),
                fallback_poll_discoveries: metadata
                    .and_then(|value| value.get("fallback_poll_discoveries"))
                    .and_then(Value::as_u64),
                sync_errors: metadata
                    .and_then(|value| value.get("sync_errors"))
                    .and_then(Value::as_u64),
                last_successful_sync_unix_ms: metadata
                    .and_then(|value| value.get("last_successful_sync_unix_ms"))
                    .and_then(Value::as_i64),
                pending_uploads,
                pending_upload_bytes,
                active_uploads,
                quiescence,
                quiescence_reasons,
                error: (output.status != 0).then_some(truncate_diagnostic_error(&output.stderr)),
            })
        }
        Err(error) => Some(ClientMetadataDiagnostics {
            client: mount.client.clone(),
            mount_point,
            artifact,
            status: "spawn_failed".to_string(),
            exit_code: None,
            local_version: None,
            namespace_version: None,
            flushed_event_seq: None,
            pending_metadata_ops: None,
            inflight_metadata_ops: None,
            notifications_received: None,
            listener_reconnects: None,
            fallback_poll_discoveries: None,
            sync_errors: None,
            last_successful_sync_unix_ms: None,
            pending_uploads: None,
            pending_upload_bytes: None,
            active_uploads: None,
            quiescence: ClientQuiescence::Unknown,
            quiescence_reasons: vec!["diagnostics command could not be started".to_string()],
            error: Some(error.to_string()),
        }),
    }
}

fn classify_client_quiescence(
    exit_code: i32,
    pending_metadata_ops: Option<u64>,
    inflight_metadata_ops: Option<u64>,
    pending_uploads: Option<u64>,
    pending_upload_bytes: Option<u64>,
    active_uploads: Option<u64>,
) -> (ClientQuiescence, Vec<String>) {
    if exit_code != 0 {
        return (
            ClientQuiescence::Unknown,
            vec!["diagnostics command failed".to_string()],
        );
    }

    let mut reasons = Vec::new();
    push_positive_reason(&mut reasons, "pending_metadata_ops", pending_metadata_ops);
    push_positive_reason(&mut reasons, "inflight_metadata_ops", inflight_metadata_ops);
    push_positive_reason(&mut reasons, "pending_uploads", pending_uploads);
    push_positive_reason(&mut reasons, "pending_upload_bytes", pending_upload_bytes);
    push_positive_reason(&mut reasons, "active_uploads", active_uploads);
    if !reasons.is_empty() {
        return (ClientQuiescence::Busy, reasons);
    }

    if [
        pending_metadata_ops,
        inflight_metadata_ops,
        pending_uploads,
        pending_upload_bytes,
        active_uploads,
    ]
    .iter()
    .any(Option::is_none)
    {
        return (
            ClientQuiescence::Unknown,
            vec!["diagnostics missing one or more quiescence counters".to_string()],
        );
    }

    (
        ClientQuiescence::Quiescent,
        vec!["no pending metadata or upload work".to_string()],
    )
}

fn push_positive_reason(reasons: &mut Vec<String>, field: &str, value: Option<u64>) {
    match value {
        Some(value) if value > 0 => reasons.push(format!("{field}={value}")),
        _ => {}
    }
}

fn query_remote_metadata(probe: &RemoteMetadataProbe) -> RemoteMetadataDiagnostics {
    match query_remote_metadata_inner(probe) {
        Ok((max_batch_version, max_seq_id, event_count)) => RemoteMetadataDiagnostics {
            status: "collected".to_string(),
            volume_name: probe.volume_name.clone(),
            max_batch_version: Some(max_batch_version),
            max_seq_id: Some(max_seq_id),
            event_count: Some(event_count),
            error: None,
        },
        Err(error) => RemoteMetadataDiagnostics {
            status: "failed".to_string(),
            volume_name: probe.volume_name.clone(),
            max_batch_version: None,
            max_seq_id: None,
            event_count: None,
            error: Some(error),
        },
    }
}

fn query_remote_metadata_inner(probe: &RemoteMetadataProbe) -> Result<(i64, i64, i64), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(async {
        let connected = tokio::time::timeout(
            probe.timeout,
            tokio_postgres::connect(&probe.meta_dsn, NoTls),
        )
        .await
        .map_err(|_| "remote metadata query timed out while connecting".to_string())?
        .map_err(|error| error.to_string())?;
        let (client, connection) = connected;
        let connection_task = tokio::spawn(connection);
        let row = tokio::time::timeout(
            probe.timeout,
            client.query_one(remote_metadata_query_sql(), &[&probe.volume_name]),
        )
        .await
        .map_err(|_| "remote metadata query timed out".to_string())?
        .map_err(|error| error.to_string())?;
        drop(client);
        let _ = tokio::time::timeout(Duration::from_secs(1), connection_task).await;
        Ok((row.get(0), row.get(1), row.get(2)))
    })
}

fn remote_metadata_query_sql() -> &'static str {
    "SELECT COALESCE(MAX(event_log.batch_version), 0)::BIGINT, \
     COALESCE(MAX(event_log.seq_id), 0)::BIGINT, COUNT(*)::BIGINT \
     FROM public.event_log \
     JOIN public.volumes ON volumes.vol = event_log.vol \
     WHERE volumes.name = $1"
}

fn value_to_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(ToString::to_string)
        .or_else(|| value.as_i64().map(|number| number.to_string()))
        .or_else(|| value.as_u64().map(|number| number.to_string()))
}

fn truncate_diagnostic_error(value: &str) -> String {
    const MAX_ERROR_CHARS: usize = 512;
    if value.chars().count() <= MAX_ERROR_CHARS {
        return value.to_string();
    }
    let mut truncated = value.chars().take(MAX_ERROR_CHARS).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn scan_dir(
    root: &Path,
    current: &Path,
    entries: &mut Vec<TreeEntry>,
) -> Result<(), MountedTreeError> {
    let mut children = read_dir_entries(current)?;
    children.sort_by_key(|left| left.file_name());
    for child in children {
        let path = child.path();
        let metadata = symlink_metadata(&path)?;
        let relative_path = relative_path(root, &path)?;
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: TreeEntryKind::Directory,
                size: None,
                modified_unix_ms: modified_unix_ms(&metadata),
                checksum_sha256: None,
                symlink_target: None,
            });
            scan_dir(root, &path, entries)?;
        } else if file_type.is_file() {
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: TreeEntryKind::File,
                size: Some(metadata.len()),
                modified_unix_ms: modified_unix_ms(&metadata),
                checksum_sha256: Some(file_sha256(&path)?),
                symlink_target: None,
            });
        } else {
            let symlink_target = if file_type.is_symlink() {
                Some(read_link_target(&path)?)
            } else {
                None
            };
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: if file_type.is_symlink() {
                    TreeEntryKind::Symlink
                } else {
                    TreeEntryKind::Other
                },
                size: None,
                modified_unix_ms: modified_unix_ms(&metadata),
                checksum_sha256: None,
                symlink_target,
            });
        }
    }
    Ok(())
}

fn read_dir_entries(path: &Path) -> Result<Vec<fs::DirEntry>, MountedTreeError> {
    let mut children = Vec::new();
    for child in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        children.push(child.map_err(|source| io_error(path, source))?);
    }
    Ok(children)
}

fn entries_by_path(snapshot: &MountedTreeSnapshot) -> BTreeMap<&str, &TreeEntry> {
    snapshot
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect()
}

fn compare_entry(
    reference_client: &str,
    client: &str,
    path: &str,
    expected: &TreeEntry,
    actual: &TreeEntry,
    differences: &mut Vec<TreeDifference>,
) {
    if expected.entry_type != actual.entry_type {
        differences.push(TreeDifference {
            client: client.to_string(),
            reference_client: Some(reference_client.to_string()),
            path: path.to_string(),
            message: "entry type differs".to_string(),
            expected: Some(format!("{:?}", expected.entry_type)),
            actual: Some(format!("{:?}", actual.entry_type)),
        });
        return;
    }

    if expected.entry_type == TreeEntryKind::File && expected.size != actual.size {
        differences.push(TreeDifference {
            client: client.to_string(),
            reference_client: Some(reference_client.to_string()),
            path: path.to_string(),
            message: "file size differs".to_string(),
            expected: expected.size.map(|size| size.to_string()),
            actual: actual.size.map(|size| size.to_string()),
        });
    }

    if expected.entry_type == TreeEntryKind::File
        && expected.checksum_sha256 != actual.checksum_sha256
    {
        differences.push(TreeDifference {
            client: client.to_string(),
            reference_client: Some(reference_client.to_string()),
            path: path.to_string(),
            message: "file checksum differs".to_string(),
            expected: expected.checksum_sha256.clone(),
            actual: actual.checksum_sha256.clone(),
        });
    }

    if expected.entry_type == TreeEntryKind::Symlink
        && expected.symlink_target != actual.symlink_target
    {
        differences.push(TreeDifference {
            client: client.to_string(),
            reference_client: Some(reference_client.to_string()),
            path: path.to_string(),
            message: "symlink target differs".to_string(),
            expected: expected.symlink_target.clone(),
            actual: actual.symlink_target.clone(),
        });
    }
}

fn mounted_entry_to_logical_entry(entry: &TreeEntry) -> LogicalEntry {
    LogicalEntry {
        path: entry.path.clone(),
        kind: match entry.entry_type {
            TreeEntryKind::Directory => LogicalEntryKind::Directory,
            TreeEntryKind::File => LogicalEntryKind::File,
            TreeEntryKind::Symlink => LogicalEntryKind::Symlink,
            TreeEntryKind::Other => LogicalEntryKind::Other,
        },
        logical_id: None,
        object: None,
        checksum_sha256: entry.checksum_sha256.clone(),
        size: entry.size,
        byte_ranges: Vec::new(),
        symlink_target: entry.symlink_target.clone(),
    }
}

fn snapshot_mounts(mounts: &[ClientMount]) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    mounts
        .iter()
        .map(|mount| snapshot_client_tree(&mount.client, &mount.mount_point))
        .collect()
}

fn relative_path(root: &Path, path: &Path) -> Result<String, MountedTreeError> {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .map(str::to_string)
                .ok_or_else(|| MountedTreeError::NonUtf8Path {
                    path: path.to_path_buf(),
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|components| components.join("/"))
}

fn file_sha256(path: &Path) -> Result<String, MountedTreeError> {
    let mut file = File::open(path).map_err(|source| io_error(path, source))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; READ_BUFFER_BYTES];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|source| io_error(path, source))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn read_link_target(path: &Path) -> Result<String, MountedTreeError> {
    let target = fs::read_link(path).map_err(|source| io_error(path, source))?;
    path_to_json_string(&target)
}

fn path_to_json_string(path: &Path) -> Result<String, MountedTreeError> {
    path.as_os_str()
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| MountedTreeError::NonUtf8Path {
            path: path.to_path_buf(),
        })
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to string cannot fail");
    }
    encoded
}

fn modified_unix_ms(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
}

fn entry_contract(entry: &TreeEntry) -> String {
    match entry.entry_type {
        TreeEntryKind::File => format!(
            "file size={} checksum_sha256={}",
            entry
                .size
                .map(|size| size.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            entry.checksum_sha256.as_deref().unwrap_or("unknown")
        ),
        TreeEntryKind::Directory => "directory".to_string(),
        TreeEntryKind::Symlink => format!(
            "symlink target={}",
            entry.symlink_target.as_deref().unwrap_or("unknown")
        ),
        TreeEntryKind::Other => "other".to_string(),
    }
}

fn next_sleep(poll_interval: Duration, deadline: Instant) -> Duration {
    let interval = if poll_interval.is_zero() {
        Duration::from_millis(1)
    } else {
        poll_interval
    };
    deadline
        .checked_duration_since(Instant::now())
        .map(|remaining| remaining.min(interval))
        .unwrap_or(Duration::ZERO)
}

fn create_dir_all(path: &Path) -> Result<(), MountedTreeError> {
    fs::create_dir_all(path).map_err(|source| io_error(path, source))
}

fn symlink_metadata(path: &Path) -> Result<fs::Metadata, MountedTreeError> {
    fs::symlink_metadata(path).map_err(|source| io_error(path, source))
}

fn write_pretty_json<T: Serialize>(path: &Path, value: &T) -> Result<(), MountedTreeError> {
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|source| MountedTreeError::Json {
        path: path.to_path_buf(),
        source,
    })?;
    fs::write(path, bytes).map_err(|source| io_error(path, source))
}

fn io_error(path: &Path, source: io::Error) -> MountedTreeError {
    MountedTreeError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::CommandOutput;

    struct FakeDiagnosticsExecutor {
        output: CommandOutput,
    }

    impl CommandExecutor for FakeDiagnosticsExecutor {
        fn run(&self, _command: &CommandSpec) -> std::io::Result<CommandOutput> {
            Ok(self.output.clone())
        }
    }

    #[test]
    fn client_metadata_diagnostics_extracts_runtime_metadata_fields() {
        let temp = tempfile::tempdir().unwrap();
        let executor = FakeDiagnosticsExecutor {
            output: CommandOutput {
                status: 0,
                stdout: serde_json::json!({
                    "runtime_snapshot": {
                        "write_path": {
                            "metadata_invalidation": {
                                "metadata_version_token": "42",
                                "namespace_version_token": "41",
                                "flushed_event_seq": 17,
                                "pending_metadata_ops": 2,
                                "inflight_metadata_ops": 1,
                                "notifications_received": 3,
                                "listener_reconnects": 4,
                                "fallback_poll_discoveries": 5,
                                "sync_errors": 6,
                                "last_successful_sync_unix_ms": 123456
                            }
                        },
                        "upload": {
                            "pending_uploads": 3,
                            "pending_upload_bytes": 4096,
                            "active_uploads": 1
                        }
                    }
                })
                .to_string(),
                stderr: String::new(),
            },
        };
        let mount = ClientMount::new("client-a", temp.path().join("mount"))
            .with_diagnostics_probe("studiofs", temp.path().join("client-a"));

        let diagnostics =
            collect_client_metadata_diagnostics(&executor, temp.path(), 0, &mount).unwrap();

        let raw = serde_json::from_slice::<Value>(
            &fs::read(temp.path().join("metadata-client-1.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(
            raw.pointer("/command/env/0/0").and_then(Value::as_str),
            Some("STUDIOFS_CLIENT_ROOT")
        );
        assert_eq!(diagnostics.local_version.as_deref(), Some("42"));
        assert_eq!(diagnostics.namespace_version.as_deref(), Some("41"));
        assert_eq!(diagnostics.flushed_event_seq, Some(17));
        assert_eq!(diagnostics.pending_metadata_ops, Some(2));
        assert_eq!(diagnostics.inflight_metadata_ops, Some(1));
        assert_eq!(diagnostics.notifications_received, Some(3));
        assert_eq!(diagnostics.listener_reconnects, Some(4));
        assert_eq!(diagnostics.fallback_poll_discoveries, Some(5));
        assert_eq!(diagnostics.sync_errors, Some(6));
        assert_eq!(diagnostics.last_successful_sync_unix_ms, Some(123456));
        assert_eq!(diagnostics.pending_uploads, Some(3));
        assert_eq!(diagnostics.pending_upload_bytes, Some(4096));
        assert_eq!(diagnostics.active_uploads, Some(1));
        assert_eq!(diagnostics.quiescence, ClientQuiescence::Busy);
        assert!(
            diagnostics
                .quiescence_reasons
                .contains(&"pending_metadata_ops=2".to_string())
        );
        assert!(
            diagnostics
                .quiescence_reasons
                .contains(&"inflight_metadata_ops=1".to_string())
        );
        assert!(
            diagnostics
                .quiescence_reasons
                .contains(&"pending_uploads=3".to_string())
        );
        assert!(
            diagnostics
                .quiescence_reasons
                .contains(&"pending_upload_bytes=4096".to_string())
        );
        assert!(
            diagnostics
                .quiescence_reasons
                .contains(&"active_uploads=1".to_string())
        );
        assert!(temp.path().join("metadata-client-1.json").exists());
    }

    #[test]
    fn client_metadata_diagnostics_marks_zero_backlog_as_quiescent() {
        let temp = tempfile::tempdir().unwrap();
        let executor = FakeDiagnosticsExecutor {
            output: CommandOutput {
                status: 0,
                stdout: serde_json::json!({
                    "runtime_snapshot": {
                        "write_path": {
                            "metadata_invalidation": {
                                "metadata_version_token": "42",
                                "namespace_version_token": "42",
                                "flushed_event_seq": 17,
                                "pending_metadata_ops": 0,
                                "inflight_metadata_ops": 0,
                                "fallback_poll_discoveries": 0,
                                "sync_errors": 0,
                                "last_successful_sync_unix_ms": 123456
                            }
                        },
                        "upload": {
                            "pending_uploads": 0,
                            "pending_upload_bytes": 0,
                            "active_uploads": 0
                        }
                    }
                })
                .to_string(),
                stderr: String::new(),
            },
        };
        let mount = ClientMount::new("client-a", temp.path().join("mount"))
            .with_diagnostics_probe("studiofs", temp.path().join("client-a"));

        let diagnostics =
            collect_client_metadata_diagnostics(&executor, temp.path(), 0, &mount).unwrap();

        assert_eq!(diagnostics.quiescence, ClientQuiescence::Quiescent);
        assert_eq!(
            diagnostics.quiescence_reasons,
            ["no pending metadata or upload work"]
        );
    }

    #[test]
    fn client_metadata_diagnostics_treats_orphaned_upload_bytes_as_busy() {
        let temp = tempfile::tempdir().unwrap();
        let executor = FakeDiagnosticsExecutor {
            output: CommandOutput {
                status: 0,
                stdout: serde_json::json!({
                    "runtime_snapshot": {
                        "write_path": {
                            "metadata_invalidation": {
                                "pending_metadata_ops": 0,
                                "inflight_metadata_ops": 0
                            }
                        },
                        "upload": {
                            "pending_uploads": 0,
                            "pending_upload_bytes": 4096,
                            "active_uploads": 0
                        }
                    }
                })
                .to_string(),
                stderr: String::new(),
            },
        };
        let mount = ClientMount::new("client-a", temp.path().join("mount"))
            .with_diagnostics_probe("studiofs", temp.path().join("client-a"));

        let diagnostics =
            collect_client_metadata_diagnostics(&executor, temp.path(), 0, &mount).unwrap();

        assert_eq!(diagnostics.quiescence, ClientQuiescence::Busy);
        assert_eq!(
            diagnostics.quiescence_reasons,
            ["pending_upload_bytes=4096"]
        );
    }

    #[test]
    fn divergence_diff_embeds_metadata_diagnostics_summary() {
        let temp = tempfile::tempdir().unwrap();
        let snapshot = MountedTreeSnapshot {
            schema_version: SNAPSHOT_SCHEMA_VERSION,
            client: "client-a".to_string(),
            mount_point: "/mnt/client-a".to_string(),
            entries: Vec::new(),
        };
        let comparison = TreeComparison {
            converged: false,
            reference_client: Some("client-a".to_string()),
            compared_clients: vec!["client-a".to_string()],
            differences: Vec::new(),
        };
        let metadata = TreeMetadataDiagnostics {
            schema_version: METADATA_DIAGNOSTICS_SCHEMA_VERSION,
            generated_unix_ms: 1,
            expected_remote: Some(RemoteMetadataDiagnostics {
                status: "collected".to_string(),
                volume_name: "scenario-volume".to_string(),
                max_batch_version: Some(99),
                max_seq_id: Some(100),
                event_count: Some(7),
                error: None,
            }),
            clients: vec![ClientMetadataDiagnostics {
                client: "client-a".to_string(),
                mount_point: "/mnt/client-a".to_string(),
                artifact: "metadata-client-1.json".to_string(),
                status: "collected".to_string(),
                exit_code: Some(0),
                local_version: Some("42".to_string()),
                namespace_version: Some("41".to_string()),
                flushed_event_seq: Some(17),
                pending_metadata_ops: Some(2),
                inflight_metadata_ops: Some(0),
                notifications_received: Some(3),
                listener_reconnects: Some(0),
                fallback_poll_discoveries: Some(5),
                sync_errors: Some(0),
                last_successful_sync_unix_ms: Some(123456),
                pending_uploads: Some(0),
                pending_upload_bytes: Some(0),
                active_uploads: Some(0),
                quiescence: ClientQuiescence::Busy,
                quiescence_reasons: vec!["pending_metadata_ops=2".to_string()],
                error: None,
            }],
        };

        write_divergence_artifacts_with_diagnostics(
            temp.path(),
            &[snapshot],
            &comparison,
            Some(&metadata),
        )
        .unwrap();

        let diff = fs::read_to_string(temp.path().join("diff.json")).unwrap();
        assert!(diff.contains("\"metadata_diagnostics\""));
        assert!(diff.contains("\"volume_name\": \"scenario-volume\""));
        assert!(diff.contains("\"max_batch_version\": 99"));
        assert!(diff.contains("\"quiescence\": \"busy\""));
        assert!(diff.contains("\"pending_metadata_ops=2\""));
        assert!(temp.path().join("metadata-diagnostics.json").exists());
    }

    #[test]
    fn remote_metadata_probe_query_is_scoped_to_volume_name() {
        let mount = ClientMount::new("client-a", "/mnt/client-a").with_remote_metadata_probe(
            "postgres://studiofs@127.0.0.1/studiofs",
            "scenario-volume",
        );
        let probe = mount.remote_metadata_probe.as_ref().unwrap();

        assert_eq!(probe.volume_name, "scenario-volume");
        assert!(
            remote_metadata_query_sql().contains("JOIN public.volumes"),
            "remote diagnostics must join volumes to avoid cross-scenario event counts"
        );
        assert!(
            remote_metadata_query_sql().contains("WHERE volumes.name = $1"),
            "remote diagnostics must filter by the current scenario volume"
        );
    }
}
