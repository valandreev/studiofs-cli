//! Mounted filesystem tree snapshots and convergence validation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
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
const SNAPSHOT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Metadata for a single mounted client under convergence checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMount {
    /// Logical client name shown in reports.
    pub client: String,
    /// Mount point path that will be scanned for tree snapshots.
    pub mount_point: PathBuf,
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
    #[error("mounted filesystem tree snapshot attempt timed out after {timeout_ms}ms")]
    SnapshotAttemptTimedOut {
        /// Per-attempt timeout in milliseconds.
        timeout_ms: u128,
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

        if let Some(parent) = parent_path(&entry.path) {
            if !directory_paths.contains(parent) {
                violations.push(NamespaceInvariantViolation {
                    client: snapshot.client.clone(),
                    path: entry.path.clone(),
                    message: "missing parent directory".to_string(),
                });
            }
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
    let artifact_dir = artifact_dir.as_ref();
    let started = Instant::now();
    let deadline = started + timeout;
    let mut attempts = 0;
    let mut last_divergence = None;
    let mut last_snapshot_error = None;

    loop {
        attempts += 1;
        match snapshot_mounts_with_timeout(mounts, SNAPSHOT_ATTEMPT_TIMEOUT) {
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
fn snapshot_mounts(mounts: &[ClientMount]) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    mounts
        .iter()
        .map(|mount| snapshot_client_tree(&mount.client, &mount.mount_point))
        .collect()
}

fn snapshot_mounts_with_timeout(
    mounts: &[ClientMount],
    timeout: Duration,
) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    let mounts = mounts.to_vec();
    run_snapshot_attempt_with_timeout(timeout, move || snapshot_mounts(&mounts))
}

fn run_snapshot_attempt_with_timeout<F>(
    timeout: Duration,
    snapshot: F,
) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError>
where
    F: FnOnce() -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(snapshot());
    });
    rx.recv_timeout(timeout)
        .map_err(|_| MountedTreeError::SnapshotAttemptTimedOut {
            timeout_ms: timeout.as_millis(),
        })?
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
    fn snapshot_attempt_timeout_returns_without_waiting_for_blocked_probe() {
        let started = Instant::now();
        let err = run_snapshot_attempt_with_timeout(Duration::from_millis(25), || {
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
}
