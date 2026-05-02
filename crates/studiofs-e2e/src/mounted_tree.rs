//! Mounted filesystem tree snapshots and convergence validation.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

const SNAPSHOT_SCHEMA_VERSION: u32 = 1;
const READ_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMount {
    pub client: String,
    pub mount_point: PathBuf,
}

impl ClientMount {
    pub fn new(client: impl Into<String>, mount_point: impl Into<PathBuf>) -> Self {
        Self {
            client: client.into(),
            mount_point: mount_point.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountedTreeSnapshot {
    pub schema_version: u32,
    pub client: String,
    pub mount_point: String,
    pub entries: Vec<TreeEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeEntry {
    pub path: String,
    pub entry_type: TreeEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_unix_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeEntryKind {
    Directory,
    File,
    Symlink,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeComparison {
    pub converged: bool,
    pub reference_client: Option<String>,
    pub compared_clients: Vec<String>,
    pub differences: Vec<TreeDifference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeDifference {
    pub client: String,
    pub reference_client: Option<String>,
    pub path: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConvergenceOutcome {
    pub attempts: usize,
    pub elapsed_ms: u128,
    pub snapshots: Vec<MountedTreeSnapshot>,
    pub comparison: TreeComparison,
}

#[derive(Debug, Error)]
pub enum MountedTreeError {
    #[error("mounted tree I/O failed at {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("mounted tree JSON failed at {path}: {source}")]
    Json {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error(
        "mounted filesystem trees did not converge after {attempts} attempts in {elapsed_ms}ms; artifacts written to {artifact_dir}"
    )]
    ConvergenceTimedOut {
        attempts: usize,
        elapsed_ms: u128,
        artifact_dir: PathBuf,
        comparison: Box<TreeComparison>,
    },
}

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
        mount_point: mount_point.display().to_string(),
        entries,
    })
}

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
                if let Some(error) = last_snapshot_error {
                    return Err(error);
                }
                return Err(MountedTreeError::ConvergenceTimedOut {
                    attempts,
                    elapsed_ms,
                    artifact_dir: artifact_dir.to_path_buf(),
                    comparison: Box::new(compare_snapshots(&[])),
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
        let relative_path = relative_path(root, &path);
        let file_type = metadata.file_type();

        if file_type.is_dir() {
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: TreeEntryKind::Directory,
                size: None,
                modified_unix_ms: modified_unix_ms(&metadata),
                checksum_sha256: None,
            });
            scan_dir(root, &path, entries)?;
        } else if file_type.is_file() {
            entries.push(TreeEntry {
                path: relative_path,
                entry_type: TreeEntryKind::File,
                size: Some(metadata.len()),
                modified_unix_ms: modified_unix_ms(&metadata),
                checksum_sha256: Some(file_sha256(&path)?),
            });
        } else {
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
}

fn snapshot_mounts(mounts: &[ClientMount]) -> Result<Vec<MountedTreeSnapshot>, MountedTreeError> {
    mounts
        .iter()
        .map(|mount| snapshot_client_tree(&mount.client, &mount.mount_point))
        .collect()
}

fn relative_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
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
        TreeEntryKind::Symlink => "symlink".to_string(),
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
