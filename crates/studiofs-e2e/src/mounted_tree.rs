//! Mounted filesystem tree snapshots and convergence validation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Snapshot schema version used by serialized tree snapshots.
const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
/// Buffer size in bytes for streaming file reads during checksum generation.
const READ_BUFFER_BYTES: usize = 64 * 1024;
/// Maximum wall time for a single mounted tree snapshot attempt.
// Cold reads include cache/index commits that can wait for the local disk journal.
const SNAPSHOT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
/// Extra time a timed-out attempt gets to finish, separating a stall from a hang in diagnostics.
const SNAPSHOT_TIMEOUT_DIAGNOSTIC_GRACE: Duration = Duration::from_secs(5);
/// Finished snapshot steps at least this slow are listed in timeout diagnostics.
const SNAPSHOT_SLOW_STEP: Duration = Duration::from_millis(100);

/// Metadata for a single mounted client under convergence checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMount {
    /// Logical client name shown in reports.
    pub client: String,
    /// Mount point path that will be scanned for tree snapshots.
    pub mount_point: PathBuf,
}

/// Controls how mounted tree entries are captured.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MountedTreeSnapshotOptions {
    metadata_only_prefixes: Vec<String>,
    file_only_directories: Vec<String>,
    directory_only_directories: Vec<String>,
}

impl MountedTreeSnapshotOptions {
    /// Skips size and checksum reads for regular files at or below `prefix`.
    #[must_use]
    pub fn with_metadata_only_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.metadata_only_prefixes.push(prefix.into());
        self
    }

    /// Treats every direct child of `directory` as a regular file without getattr calls.
    #[must_use]
    pub fn with_file_only_directory(mut self, directory: impl Into<String>) -> Self {
        self.file_only_directories.push(directory.into());
        self
    }

    /// Treats every direct child of `directory` as a directory without metadata calls.
    #[must_use]
    pub fn with_directory_only_directory(mut self, directory: impl Into<String>) -> Self {
        self.directory_only_directories.push(directory.into());
        self
    }

    fn is_metadata_only(&self, path: &str) -> bool {
        self.metadata_only_prefixes.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    }

    fn is_file_only_directory(&self, path: &str) -> bool {
        self.file_only_directories
            .iter()
            .any(|directory| directory == path)
    }

    fn is_directory_only_directory(&self, path: &str) -> bool {
        self.directory_only_directories
            .iter()
            .any(|directory| directory == path)
    }
}

impl ClientMount {
    /// Creates a new client mount descriptor.
    pub fn new(client: impl Into<String>, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            client: client.into(),
            mount_point: mount_point.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Serialized snapshot of a mounted filesystem tree state.
pub struct MountedTreeSnapshot {
    /// Snapshot schema version for backward-compatible deserialization.
    pub schema_version: u32,
    /// Client label associated with this snapshot.
    pub client: String,
    /// Mount point serialized as a JSON string.
    pub mount_point: String,
    /// Canonicalized entries in the tree at snapshot time.
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Single entry in a mounted tree snapshot.
pub struct TreeEntry {
    /// Relative path from the mount root.
    pub path: String,
    /// Kind of filesystem object.
    pub entry_type: TreeEntryKind,
    /// File size in bytes for file entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    /// Last modification time in Unix milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u128>,
    /// SHA-256 checksum in lower hex for file contents.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
    /// Symlink target path for symlink entries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symlink_target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
/// Normalized kind for an entry found during snapshot.
#[serde(rename_all = "snake_case")]
pub enum TreeEntryKind {
    /// Directory entry.
    Directory,
    /// Regular file entry.
    File,
    /// Symbolic link entry.
    Symlink,
    /// Any other non-directory, non-file, non-symlink entry.
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Result of comparing multiple mounted snapshots.
pub struct TreeComparison {
    /// `true` when all provided snapshots are byte-for-byte equivalent.
    pub converged: bool,
    /// Reference client used for comparison, if any snapshots existed.
    pub reference_client: Option<String>,
    /// Clients that were included in this comparison.
    pub compared_clients: Vec<String>,
    /// Differences detected against the reference snapshot.
    pub differences: Vec<TreeDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Difference details for a single compared path.
pub struct TreeDifference {
    /// Client that differs from the reference.
    pub client: String,
    /// Reference client name used for this comparison.
    pub reference_client: Option<String>,
    /// Path where mismatch was detected.
    pub path: String,
    /// Human-readable mismatch reason.
    pub message: String,
    /// Expected serialized representation from reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    /// Actual serialized representation from the compared client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Invariant violation reported while validating a single snapshot.
pub struct NamespaceInvariantViolation {
    /// Client that violates the namespace invariant.
    pub client: String,
    /// Offending path in the snapshot.
    pub path: String,
    /// Description of the namespace violation.
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Convergence attempt summary including final snapshots and comparison result.
pub struct ConvergenceOutcome {
    /// Number of polling iterations performed.
    pub attempts: usize,
    /// Total elapsed time in milliseconds.
    pub elapsed_ms: u128,
    /// Last snapshots collected for each client.
    pub snapshots: Vec<MountedTreeSnapshot>,
    /// Comparison result at completion.
    pub comparison: TreeComparison,
}

#[derive(Debug, Error)]
/// Errors produced by mounted tree snapshotting and convergence checks.
pub enum MountedTreeError {
    /// A filesystem I/O operation failed while accessing a path.
    #[error("mounted tree I/O failed at {path}: {source}")]
    Io {
        /// File-system path involved in the failed I/O operation.
        path: PathBuf,
        /// Underlying I/O error returned by the OS.
        source: io::Error,
    },
    /// JSON serialization or deserialization failed.
    #[error("mounted tree JSON failed at {path}: {source}")]
    Json {
        /// Path to the JSON file involved in the operation.
        path: PathBuf,
        /// Underlying JSON library error.
        source: serde_json::Error,
    },
    /// Convergence polling exceeded timeout and no consistent snapshot was found.
    #[error(
        "mounted filesystem trees did not converge after {attempts} attempts in {elapsed_ms}ms; artifacts written to {artifact_dir}"
    )]
    ConvergenceTimedOut {
        /// Number of polling iterations performed before timing out.
        attempts: usize,
        /// Elapsed time in milliseconds at timeout.
        elapsed_ms: u128,
        /// Directory where convergence artifacts were written.
        artifact_dir: PathBuf,
        /// Final comparison result observed during timeout.
        comparison: Box<TreeComparison>,
    },
    /// Snapshots could not be produced before timeout.
    #[error(
        "mounted filesystem trees could not be snapshotted after {attempts} attempts in {elapsed_ms}ms; last error: {source}"
    )]
    SnapshotUnavailableTimedOut {
        /// Number of snapshot attempts performed before timing out.
        attempts: usize,
        /// Elapsed time in milliseconds at timeout.
        elapsed_ms: u128,
        /// Directory where failure artifacts were written.
        artifact_dir: PathBuf,
        /// Last error that prevented successful snapshotting.
        source: Box<MountedTreeError>,
    },
    /// A single snapshot attempt blocked longer than the per-attempt bound.
    #[error(
        "mounted filesystem tree snapshot attempt timed out after {timeout_ms}ms at {blocked_at}"
    )]
    SnapshotAttemptTimedOut {
        /// Per-attempt timeout in milliseconds.
        timeout_ms: u128,
        /// Client, operation, and path the attempt was on when the bound expired.
        blocked_at: String,
    },
    /// A scanned path was not valid UTF-8.
    #[error("mounted tree path is not valid UTF-8: {path}")]
    NonUtf8Path {
        /// Path that could not be converted to UTF-8 text.
        path: PathBuf,
    },
}

/// Captures a sorted snapshot of a mounted client tree.
pub fn snapshot_client_tree(
    client: impl Into<String>,
    mount_point: impl AsRef<Path>,
) -> Result<MountedTreeSnapshot, MountedTreeError> {
    snapshot_client_tree_with_options(client, mount_point, &MountedTreeSnapshotOptions::default())
}

/// Captures a sorted snapshot using scenario-specific payload read options.
pub fn snapshot_client_tree_with_options(
    client: impl Into<String>,
    mount_point: impl AsRef<Path>,
    options: &MountedTreeSnapshotOptions,
) -> Result<MountedTreeSnapshot, MountedTreeError> {
    snapshot_client_tree_with_probe(client, mount_point, options, &SnapshotProbe::default())
}

fn snapshot_client_tree_with_probe(
    client: impl Into<String>,
    mount_point: impl AsRef<Path>,
    options: &MountedTreeSnapshotOptions,
    probe: &SnapshotProbe,
) -> Result<MountedTreeSnapshot, MountedTreeError> {
    let client = client.into();
    let mount_point = mount_point.as_ref();
    let mut entries = Vec::new();
    probe.enter_client(&client);
    scan_dir(mount_point, mount_point, options, probe, &mut entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(MountedTreeSnapshot {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        client,
        mount_point: path_to_json_string(mount_point)?,
        entries,
    })
}

/// Compares multiple snapshots and returns the full set of differences.
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

/// Validates namespace consistency rules such as unique paths and existing parents.
pub fn validate_namespace_invariants(
    snapshot: &MountedTreeSnapshot,
    expected_absent_paths: &[&str],
) -> Vec<NamespaceInvariantViolation> {
    let mut violations = Vec::new();
    let mut seen = BTreeSet::new();
    let directory_paths = snapshot
        .entries
        .iter()
        .filter(|entry| entry.entry_type == TreeEntryKind::Directory)
        .map(|entry| entry.path.as_str())
        .collect::<BTreeSet<_>>();
    let expected_absent = expected_absent_paths
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    for entry in &snapshot.entries {
        if !seen.insert(entry.path.as_str()) {
            violations.push(NamespaceInvariantViolation {
                client: snapshot.client.clone(),
                path: entry.path.clone(),
                message: "duplicate canonical path".to_string(),
            });
        }

        if expected_absent.contains(entry.path.as_str()) {
            violations.push(NamespaceInvariantViolation {
                client: snapshot.client.clone(),
                path: entry.path.clone(),
                message: "unexpected resurrection".to_string(),
            });
        }

        if let Some(parent) = parent_path(&entry.path)
            && !directory_paths.contains(parent)
        {
            violations.push(NamespaceInvariantViolation {
                client: snapshot.client.clone(),
                path: entry.path.clone(),
                message: "missing parent directory".to_string(),
            });
        }
    }

    violations
}

/// Writes snapshot and diff artifacts for failed convergence and returns file names.
pub fn write_divergence_artifacts(
    artifact_dir: impl AsRef<Path>,
    snapshots: &[MountedTreeSnapshot],
    comparison: &TreeComparison,
) -> Result<Vec<String>, MountedTreeError> {
    let artifact_dir = artifact_dir.as_ref();
    create_dir_all(artifact_dir)?;
    let mut artifacts = Vec::with_capacity(snapshots.len() + 1);

    for (index, snapshot) in snapshots.iter().enumerate() {
        let name = format!("tree-client-{}.json", index + 1);
        write_pretty_json(&artifact_dir.join(&name), snapshot)?;
        artifacts.push(name);
    }

    write_pretty_json(&artifact_dir.join("diff.json"), comparison)?;
    artifacts.push("diff.json".to_string());
    Ok(artifacts)
}

/// Polls mounted snapshots until all clients converge or the timeout is reached.
pub fn wait_for_convergence(
    mounts: &[ClientMount],
    artifact_dir: impl AsRef<Path>,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<ConvergenceOutcome, MountedTreeError> {
    wait_for_convergence_with_options(
        mounts,
        artifact_dir,
        timeout,
        poll_interval,
        SNAPSHOT_ATTEMPT_TIMEOUT,
        &MountedTreeSnapshotOptions::default(),
    )
}

/// Polls mounted snapshots with scenario-specific snapshot and attempt bounds.
pub fn wait_for_convergence_with_options(
    mounts: &[ClientMount],
    artifact_dir: impl AsRef<Path>,
    timeout: Duration,
    poll_interval: Duration,
    snapshot_attempt_timeout: Duration,
    snapshot_options: &MountedTreeSnapshotOptions,
) -> Result<ConvergenceOutcome, MountedTreeError> {
    let artifact_dir = artifact_dir.as_ref();
    let started = Instant::now();
    let deadline = started + timeout;
    let mut attempts = 0;
    let mut last_divergence = None;
    let mut last_snapshot_error = None;

    loop {
        attempts += 1;
        let probe = Arc::new(SnapshotProbe::default());
        match snapshot_mounts_with_timeout(
            mounts,
            snapshot_attempt_timeout,
            snapshot_options,
            &probe,
        ) {
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
                let elapsed_ms = started.elapsed().as_millis();
                if matches!(error, MountedTreeError::SnapshotAttemptTimedOut { .. }) {
                    write_snapshot_timeout_diagnostics(
                        artifact_dir,
                        mounts,
                        &probe,
                        last_snapshot_error.as_ref(),
                    );
                    return Err(MountedTreeError::SnapshotUnavailableTimedOut {
                        attempts,
                        elapsed_ms,
                        artifact_dir: artifact_dir.to_path_buf(),
                        source: Box::new(error),
                    });
                }
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
            write_divergence_artifacts(artifact_dir, &snapshots, &comparison)?;
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

/// Recursively scans `current` and appends normalized tree entries to `entries`.
///
/// `root` is the mount root used to compute relative snapshot paths.
fn scan_dir(
    root: &Path,
    current: &Path,
    options: &MountedTreeSnapshotOptions,
    probe: &SnapshotProbe,
    entries: &mut Vec<TreeEntry>,
) -> Result<(), MountedTreeError> {
    probe.enter("read_dir", current);
    let mut children = read_dir_entries(current)?;
    children.sort_by_key(|left| left.file_name());
    let current_relative_path = relative_path(root, current)?;
    let file_only_directory = options.is_file_only_directory(&current_relative_path);
    let directory_only_children = options.is_directory_only_directory(&current_relative_path);
    for child in children {
        let path = child.path();
        let relative_path = relative_path(root, &path)?;
        if file_only_directory {
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: TreeEntryKind::File,
                size: None,
                modified_unix_ms: None,
                checksum_sha256: None,
                symlink_target: None,
            });
            continue;
        }
        if directory_only_children {
            let file_type = child
                .file_type()
                .map_err(|source| io_error(&path, source))?;
            let entry_type = if file_type.is_dir() {
                TreeEntryKind::Directory
            } else if file_type.is_file() {
                TreeEntryKind::File
            } else if file_type.is_symlink() {
                TreeEntryKind::Symlink
            } else {
                TreeEntryKind::Other
            };
            entries.push(TreeEntry {
                path: relative_path,
                entry_type,
                size: None,
                modified_unix_ms: None,
                checksum_sha256: None,
                symlink_target: None,
            });
            if file_type.is_dir() {
                scan_dir(root, &path, options, probe, entries)?;
            }
            continue;
        }
        if options.is_metadata_only(&relative_path) {
            let file_type = child
                .file_type()
                .map_err(|source| io_error(&path, source))?;
            let entry_type = if file_type.is_dir() {
                TreeEntryKind::Directory
            } else if file_type.is_file() {
                TreeEntryKind::File
            } else if file_type.is_symlink() {
                TreeEntryKind::Symlink
            } else {
                TreeEntryKind::Other
            };
            entries.push(TreeEntry {
                path: relative_path,
                entry_type,
                size: None,
                modified_unix_ms: None,
                checksum_sha256: None,
                symlink_target: None,
            });
            if file_type.is_dir() {
                scan_dir(root, &path, options, probe, entries)?;
            }
            continue;
        }

        probe.enter("lstat", &path);
        let metadata = symlink_metadata(&path)?;
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
            scan_dir(root, &path, options, probe, entries)?;
        } else if file_type.is_file() {
            probe.enter("read_file", &path);
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
                probe.enter("readlink", &path);
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

/// Reads all children of a directory and returns them as a vector.
fn read_dir_entries(path: &Path) -> Result<Vec<fs::DirEntry>, MountedTreeError> {
    let mut children = Vec::new();
    for child in fs::read_dir(path).map_err(|source| io_error(path, source))? {
        children.push(child.map_err(|source| io_error(path, source))?);
    }
    Ok(children)
}

/// Builds a fast lookup map of snapshot entries keyed by relative path.
fn entries_by_path(snapshot: &MountedTreeSnapshot) -> BTreeMap<&str, &TreeEntry> {
    snapshot
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect()
}

/// Returns the parent path segment for a `/`-separated path, if present.
fn parent_path(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(parent, _)| parent)
}

/// Compares two entries from different snapshots and records differences.
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

/// Captures snapshots for every provided mounted client.
fn snapshot_mounts(
    mounts: &[ClientMount],
    options: &MountedTreeSnapshotOptions,
    probe: &SnapshotProbe,
) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    mounts
        .iter()
        .map(|mount| {
            snapshot_client_tree_with_probe(&mount.client, &mount.mount_point, options, probe)
        })
        .collect()
}

fn snapshot_mounts_with_timeout(
    mounts: &[ClientMount],
    timeout: Duration,
    options: &MountedTreeSnapshotOptions,
    probe: &Arc<SnapshotProbe>,
) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    let mounts = mounts.to_vec();
    let options = options.clone();
    run_snapshot_attempt_with_timeout(timeout, probe, move |probe| {
        snapshot_mounts(&mounts, &options, probe)
    })
}

fn run_snapshot_attempt_with_timeout<F>(
    timeout: Duration,
    probe: &Arc<SnapshotProbe>,
    snapshot: F,
) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError>
where
    F: FnOnce(&SnapshotProbe) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError>
        + Send
        + 'static,
{
    let (tx, rx) = mpsc::channel();
    let worker_probe = Arc::clone(probe);
    thread::spawn(move || {
        worker_probe.record_current_task();
        let result = snapshot(&worker_probe);
        worker_probe.finished.store(true, Ordering::Release);
        let _ = tx.send(result);
    });
    rx.recv_timeout(timeout)
        .map_err(|_| MountedTreeError::SnapshotAttemptTimedOut {
            timeout_ms: timeout.as_millis(),
            blocked_at: probe.position(),
        })?
}

/// Where a snapshot attempt currently is, readable while its thread blocks in the kernel.
#[derive(Debug, Default)]
struct SnapshotProbe {
    client: Mutex<String>,
    step: Mutex<SnapshotStep>,
    task_dir: Mutex<Option<PathBuf>>,
    finished: AtomicBool,
}

#[derive(Debug, Default)]
struct SnapshotStep {
    position: String,
    entered: Option<Instant>,
    completed_steps: usize,
    slow_steps: Vec<String>,
}

impl SnapshotStep {
    fn advance(&mut self, position: String) {
        if let Some(entered) = self.entered {
            self.completed_steps += 1;
            let elapsed = entered.elapsed();
            if elapsed >= SNAPSHOT_SLOW_STEP {
                self.slow_steps
                    .push(format!("{}ms {}", elapsed.as_millis(), self.position));
            }
        }
        self.position = position;
        self.entered = Some(Instant::now());
    }
}

impl SnapshotProbe {
    fn enter_client(&self, client: &str) {
        *lock_ignoring_poison(&self.client) = client.to_string();
        lock_ignoring_poison(&self.step).advance(format!("{client} start"));
    }

    fn enter(&self, operation: &str, path: &Path) {
        let position = format!(
            "{} {operation} {}",
            lock_ignoring_poison(&self.client),
            path.display()
        );
        lock_ignoring_poison(&self.step).advance(position);
    }

    fn position(&self) -> String {
        lock_ignoring_poison(&self.step).position.clone()
    }

    fn record_current_task(&self) {
        // Resolves to `<pid>/task/<tid>`; absent outside Linux, where diagnostics stay position-only.
        if let Ok(task) = fs::read_link("/proc/thread-self") {
            *lock_ignoring_poison(&self.task_dir) = Some(Path::new("/proc").join(task));
        }
    }
}

fn lock_ignoring_poison<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Best-effort report for a timed-out attempt; a failure here must not hide the timeout itself.
fn write_snapshot_timeout_diagnostics(
    artifact_dir: &Path,
    mounts: &[ClientMount],
    probe: &SnapshotProbe,
    previous_error: Option<&MountedTreeError>,
) {
    let mut report = String::new();
    if let Some(error) = previous_error {
        let _ = writeln!(report, "previous attempt error: {error}");
    }
    let _ = writeln!(report, "== at timeout ==");
    write_snapshot_state_sample(&mut report, mounts, probe);

    let started = Instant::now();
    while !probe.finished.load(Ordering::Acquire)
        && started.elapsed() < SNAPSHOT_TIMEOUT_DIAGNOSTIC_GRACE
    {
        thread::sleep(Duration::from_millis(20));
    }
    let waited_ms = started.elapsed().as_millis();
    if probe.finished.load(Ordering::Acquire) {
        let _ = writeln!(report, "== attempt finished {waited_ms}ms after timeout ==");
    } else {
        let _ = writeln!(
            report,
            "== attempt still blocked {waited_ms}ms after timeout =="
        );
        write_snapshot_state_sample(&mut report, mounts, probe);
    }

    let path = artifact_dir.join("snapshot-timeout.txt");
    if let Err(error) = create_dir_all(artifact_dir)
        .and_then(|()| fs::write(&path, &report).map_err(|source| io_error(&path, source)))
    {
        eprintln!("studiofs-e2e: failed to write snapshot timeout diagnostics: {error}");
    }
}

/// Reads only `/proc` and `/sys`, which stay responsive while a FUSE request is stuck.
fn write_snapshot_state_sample(report: &mut String, mounts: &[ClientMount], probe: &SnapshotProbe) {
    {
        let step = lock_ignoring_poison(&probe.step);
        let _ = writeln!(
            report,
            "position: {} (for {}ms, after {} completed steps)",
            step.position,
            step.entered
                .map_or(0, |entered| entered.elapsed().as_millis()),
            step.completed_steps
        );
        for slow_step in &step.slow_steps {
            let _ = writeln!(report, "slow step: {slow_step}");
        }
    }
    if let Some(task_dir) = lock_ignoring_poison(&probe.task_dir).as_deref() {
        let _ = writeln!(report, "snapshot thread: {}", proc_task_summary(task_dir));
    }
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").unwrap_or_default();
    for mount in mounts {
        let _ = writeln!(
            report,
            "client {} mount {}: fuse waiting={}",
            mount.client,
            mount.mount_point.display(),
            fuse_waiting_requests(&mountinfo, &mount.mount_point)
                .unwrap_or_else(|| "unknown".to_string())
        );
        for pid in daemon_pids_for_mount(&mount.mount_point) {
            let _ = writeln!(report, "  daemon pid {pid}");
            let tasks = fs::read_dir(format!("/proc/{pid}/task"));
            for task in tasks.into_iter().flatten().flatten() {
                let _ = writeln!(report, "    {}", proc_task_summary(&task.path()));
            }
        }
    }
}

fn proc_task_summary(task_dir: &Path) -> String {
    let read = |name: &str| {
        fs::read_to_string(task_dir.join(name))
            .map(|value| value.trim().to_string())
            .unwrap_or_else(|error| format!("<{error}>"))
    };
    // `stat` is `pid (comm) state ...`, and comm can contain spaces.
    let stat = read("stat");
    let state = stat
        .rsplit_once(") ")
        .and_then(|(_, rest)| rest.split(' ').next())
        .unwrap_or("?");
    format!(
        "tid={} comm={} state={state} wchan={} syscall={} stack={:?}",
        task_dir
            .file_name()
            .map(|tid| tid.to_string_lossy().into_owned())
            .unwrap_or_default(),
        read("comm"),
        read("wchan"),
        read("syscall"),
        read("stack"),
    )
}

/// Pending requests on the mount's FUSE connection, named by the mount's minor device number.
fn fuse_waiting_requests(mountinfo: &str, mount_point: &Path) -> Option<String> {
    let mount_point = mount_point.to_str()?;
    let minor = mountinfo.lines().find_map(|line| {
        let mut fields = line.split(' ');
        let device = fields.nth(2)?;
        (fields.nth(1)? == mount_point).then_some(device.split_once(':')?.1)
    })?;
    fs::read_to_string(format!("/sys/fs/fuse/connections/{minor}/waiting"))
        .ok()
        .map(|waiting| waiting.trim().to_string())
}

/// Finds the mounted client processes by the mount point on their command line.
fn daemon_pids_for_mount(mount_point: &Path) -> Vec<String> {
    let Some(mount_point) = mount_point.to_str() else {
        return Vec::new();
    };
    fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| {
            let pid = entry.file_name().into_string().ok()?;
            pid.bytes()
                .all(|byte| byte.is_ascii_digit())
                .then_some(())?;
            let cmdline = fs::read(entry.path().join("cmdline")).ok()?;
            cmdline
                .split(|byte| *byte == 0)
                .any(|argument| argument == mount_point.as_bytes())
                .then_some(pid)
        })
        .collect()
}

/// Computes a relative path from `root` to `path`, normalized with `/` separators.
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

/// Computes a lower-case SHA-256 checksum for file contents.
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

/// Reads a symlink target path and serializes it for snapshot output.
fn read_link_target(path: &Path) -> Result<String, MountedTreeError> {
    let target = fs::read_link(path).map_err(|source| io_error(path, source))?;
    path_to_json_string(&target)
}

/// Converts a path to UTF-8 string form used in snapshot JSON.
fn path_to_json_string(path: &Path) -> Result<String, MountedTreeError> {
    path.as_os_str()
        .to_str()
        .map(str::to_string)
        .ok_or_else(|| MountedTreeError::NonUtf8Path {
            path: path.to_path_buf(),
        })
}

/// Encodes bytes into a lowercase hexadecimal string.
fn hex_lower(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut encoded, "{byte:02x}").expect("writing to string cannot fail");
    }
    encoded
}

/// Extracts the modified timestamp as milliseconds since Unix epoch.
fn modified_unix_ms(metadata: &fs::Metadata) -> Option<u128> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(SystemTime::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis())
}

/// Builds a compact human-readable contract summary for a snapshot entry.
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

/// Computes the next sleep duration while respecting the convergence deadline.
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

/// Creates the directory and all missing parent components.
fn create_dir_all(path: &Path) -> Result<(), MountedTreeError> {
    fs::create_dir_all(path).map_err(|source| io_error(path, source))
}

/// Reads metadata without following symlinks.
fn symlink_metadata(path: &Path) -> Result<fs::Metadata, MountedTreeError> {
    fs::symlink_metadata(path).map_err(|source| io_error(path, source))
}

/// Serializes `value` as pretty JSON and writes it to `path`.
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

/// Wraps an I/O error with the corresponding path context.
fn io_error(path: &Path, source: io::Error) -> MountedTreeError {
    MountedTreeError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_only_prefix_skips_file_payload_reads_but_preserves_namespace() {
        let temp = studiofs_test_support::workspace_target_tempdir("mounted-tree-options-");
        let root = temp.path();
        let fixture_dir = root.join("fixture-root/fixture-files/shard-00");
        let control_dir = root.join("fixture-root/control");
        fs::create_dir_all(&fixture_dir).unwrap();
        fs::create_dir_all(&control_dir).unwrap();
        fs::write(fixture_dir.join("history-file-000000.bin"), b"fixture").unwrap();
        fs::write(control_dir.join("marker.txt"), b"marker").unwrap();

        let snapshot = snapshot_client_tree_with_options(
            "client-a",
            root,
            &MountedTreeSnapshotOptions::default()
                .with_metadata_only_prefix("fixture-root/fixture-files")
                .with_directory_only_directory("fixture-root/fixture-files")
                .with_file_only_directory("fixture-root/fixture-files/shard-00"),
        )
        .unwrap();
        let entries = entries_by_path(&snapshot);
        let shard = entries["fixture-root/fixture-files/shard-00"];
        assert_eq!(shard.entry_type, TreeEntryKind::Directory);
        assert_eq!(shard.modified_unix_ms, None);
        let fixture = entries["fixture-root/fixture-files/shard-00/history-file-000000.bin"];
        assert_eq!(fixture.entry_type, TreeEntryKind::File);
        assert_eq!(fixture.size, None);
        assert_eq!(fixture.checksum_sha256, None);

        let marker = entries["fixture-root/control/marker.txt"];
        assert_eq!(marker.size, Some(6));
        assert!(marker.checksum_sha256.is_some());
    }

    #[test]
    fn directory_only_option_does_not_recurse_into_non_directories() {
        let temp = studiofs_test_support::workspace_target_tempdir("mounted-tree-directory-only-");
        let root = temp.path();
        let fixture_dir = root.join("fixture-root/fixture-files");
        fs::create_dir_all(&fixture_dir).unwrap();
        fs::write(fixture_dir.join("marker.txt"), b"marker").unwrap();

        let snapshot = snapshot_client_tree_with_options(
            "client-a",
            root,
            &MountedTreeSnapshotOptions::default()
                .with_directory_only_directory("fixture-root/fixture-files"),
        )
        .unwrap();
        let entries = entries_by_path(&snapshot);
        let marker = entries["fixture-root/fixture-files/marker.txt"];
        assert_eq!(marker.entry_type, TreeEntryKind::File);
        assert_eq!(marker.size, None);
        assert_eq!(marker.checksum_sha256, None);
    }

    #[test]
    fn snapshot_budget_allows_cold_cache_commit_beyond_five_seconds() {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let probe = Arc::new(SnapshotProbe::default());
            let result =
                run_snapshot_attempt_with_timeout(SNAPSHOT_ATTEMPT_TIMEOUT, &probe, move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok(Vec::new())
                });
            result_tx.send(result).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        // Model a cold cache commit still in flight after the former deadline.
        let early_result = result_rx.recv_timeout(Duration::from_millis(5200));
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(matches!(early_result, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .is_ok()
        );
    }

    #[test]
    fn snapshot_attempt_timeout_returns_without_waiting_for_blocked_probe() {
        let started = Instant::now();
        let probe = Arc::new(SnapshotProbe::default());
        let err = run_snapshot_attempt_with_timeout(Duration::from_millis(25), &probe, |probe| {
            probe.enter_client("client-b");
            probe.enter("read_file", Path::new("/mnt/client-b/slot.txt"));
            thread::sleep(Duration::from_secs(5));
            Ok(Vec::new())
        })
        .unwrap_err();

        assert!(matches!(
            err,
            MountedTreeError::SnapshotAttemptTimedOut { .. }
        ));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "blocked snapshot attempt must not consume the scenario-child timeout"
        );
    }

    #[test]
    fn snapshot_probe_reports_client_operation_and_path() {
        let probe = SnapshotProbe::default();
        probe.enter_client("client-b");
        probe.enter("read_file", Path::new("/mnt/client-b/slot.txt"));
        assert_eq!(
            probe.position(),
            "client-b read_file /mnt/client-b/slot.txt"
        );
    }

    #[test]
    fn snapshot_probe_keeps_only_slow_finished_steps() {
        let mut step = SnapshotStep::default();
        step.advance("fast".to_string());
        step.advance("slow".to_string());
        step.entered = Some(Instant::now() - SNAPSHOT_SLOW_STEP);
        step.advance("current".to_string());

        assert_eq!(step.completed_steps, 2);
        assert_eq!(step.slow_steps.len(), 1);
        assert!(
            step.slow_steps[0].ends_with("ms slow"),
            "{:?}",
            step.slow_steps
        );
    }

    #[test]
    fn timeout_diagnostics_report_previous_error_and_completion() {
        let temp = studiofs_test_support::workspace_target_tempdir("mounted-tree-timeout-");
        let probe = SnapshotProbe::default();
        probe.enter_client("client-b");
        probe.record_current_task();
        probe.finished.store(true, Ordering::Release);
        let previous = io_error(Path::new("/mnt/gone"), io::ErrorKind::NotFound.into());

        write_snapshot_timeout_diagnostics(
            temp.path(),
            &[ClientMount::new("client-b", temp.path())],
            &probe,
            Some(&previous),
        );

        let report = fs::read_to_string(temp.path().join("snapshot-timeout.txt")).unwrap();
        assert!(report.contains("previous attempt error: mounted tree I/O failed at /mnt/gone"));
        assert!(report.contains("position: client-b start"));
        assert!(report.contains("attempt finished"));
        #[cfg(target_os = "linux")]
        assert!(report.contains("snapshot thread: tid="), "{report}");
    }
}
