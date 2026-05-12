//! Logical filesystem snapshots and reusable invariant validation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Constant $name.
pub const LOGICAL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct LogicalSnapshot {
    /// Field $name.
    pub schema_version: u32,
    /// Field $name.
    pub clients: Vec<ClientLogicalSnapshot>,
    /// Field $name.
    pub expected_absent_paths: Vec<String>,
    /// Field $name.
    pub durable_writes: Vec<DurableWriteExpectation>,
    /// Field $name.
    pub objects: Vec<ObjectLifecycle>,
    /// Field $name.
    pub allow_hard_links: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub replay_idempotent: Option<bool>,
}

impl LogicalSnapshot {
    /// Function $name.
    pub fn new(clients: Vec<ClientLogicalSnapshot>) -> Self {
        Self {
            schema_version: LOGICAL_SNAPSHOT_SCHEMA_VERSION,
            clients,
            expected_absent_paths: Vec::new(),
            durable_writes: Vec::new(),
            objects: Vec::new(),
            allow_hard_links: false,
            replay_idempotent: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ClientLogicalSnapshot {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub entries: Vec<LogicalEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct LogicalEntry {
    /// Field $name.
    pub path: String,
    /// Field $name.
    pub kind: LogicalEntryKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub logical_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub object: Option<ObjectRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Field $name.
    pub byte_ranges: Vec<LogicalByteRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub symlink_target: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    /// Field $name.
    pub metadata_attributes: BTreeMap<String, String>,
}

impl LogicalEntry {
    /// Function $name.
    pub fn directory(path: impl Into<String>, logical_id: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            kind: LogicalEntryKind::Directory,
            logical_id: Some(logical_id.into()),
            object: None,
            checksum_sha256: None,
            size: None,
            byte_ranges: Vec::new(),
            symlink_target: None,
            metadata_attributes: BTreeMap::new(),
        }
    }

    /// Function $name.
    pub fn file(
        path: impl Into<String>,
        logical_id: impl Into<String>,
        object: Option<ObjectRef>,
        checksum_sha256: Option<String>,
        size: Option<u64>,
    ) -> Self {
        Self {
            path: path.into(),
            kind: LogicalEntryKind::File,
            logical_id: Some(logical_id.into()),
            object,
            checksum_sha256,
            size,
            byte_ranges: Vec::new(),
            symlink_target: None,
            metadata_attributes: BTreeMap::new(),
        }
    }

    /// Function $name.
    pub fn symlink(
        path: impl Into<String>,
        logical_id: impl Into<String>,
        target: impl Into<String>,
    ) -> Self {
        Self {
            path: path.into(),
            kind: LogicalEntryKind::Symlink,
            logical_id: Some(logical_id.into()),
            object: None,
            checksum_sha256: None,
            size: None,
            byte_ranges: Vec::new(),
            symlink_target: Some(target.into()),
            metadata_attributes: BTreeMap::new(),
        }
    }

    fn convergence_key(&self) -> EntryConvergenceKey<'_> {
        EntryConvergenceKey {
            kind: self.kind,
            logical_id: self.logical_id.as_deref(),
            checksum_sha256: self.checksum_sha256.as_deref(),
            size: self.size,
            byte_ranges: self.byte_ranges.as_slice(),
            symlink_target: self.symlink_target.as_deref(),
            metadata_attributes: &self.metadata_attributes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct LogicalByteRange {
    /// Field $name.
    pub offset: u64,
    /// Field $name.
    pub len: u64,
    /// Field $name.
    pub kind: LogicalByteRangeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub checksum_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum LogicalByteRangeKind {
    /// Variant $name.
    Data,
    /// Variant $name.
    Hole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum LogicalEntryKind {
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
pub struct ObjectRef {
    /// Field $name.
    pub object_id: String,
    /// Field $name.
    pub location: ObjectLocation,
}

impl ObjectRef {
    /// Function $name.
    pub fn local(object_id: impl Into<String>) -> Self {
        Self {
            object_id: object_id.into(),
            location: ObjectLocation::Local,
        }
    }

    /// Function $name.
    pub fn remote(object_id: impl Into<String>) -> Self {
        Self {
            object_id: object_id.into(),
            location: ObjectLocation::Remote,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
/// Enum $name.
pub enum ObjectLocation {
    /// Variant $name.
    Local,
    /// Variant $name.
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct ObjectLifecycle {
    /// Field $name.
    pub object_id: String,
    /// Field $name.
    pub present_remote: bool,
    /// Field $name.
    pub protected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct DurableWriteExpectation {
    /// Field $name.
    pub client: String,
    /// Field $name.
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub checksum_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct InvariantReport {
    /// Field $name.
    pub passed: bool,
    /// Field $name.
    pub violations: Vec<InvariantViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Struct $name.
pub struct InvariantViolation {
    /// Field $name.
    pub invariant: String,
    /// Field $name.
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub client: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub logical_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Field $name.
    pub object_id: Option<String>,
}

/// Function $name.
pub fn validate_logical_invariants(snapshot: &LogicalSnapshot) -> InvariantReport {
    let mut violations = Vec::new();
    validate_convergence(snapshot, &mut violations);
    validate_namespace(snapshot, &mut violations);
    validate_sparse_byte_ranges(snapshot, &mut violations);
    validate_durable_writes(snapshot, &mut violations);
    validate_object_reachability(snapshot, &mut violations);
    validate_replay(snapshot, &mut violations);

    InvariantReport {
        passed: violations.is_empty(),
        violations,
    }
}

fn validate_convergence(snapshot: &LogicalSnapshot, violations: &mut Vec<InvariantViolation>) {
    let Some(reference) = snapshot.clients.first() else {
        return;
    };
    let reference_entries = entries_by_path(reference);
    for client in snapshot.clients.iter().skip(1) {
        let actual_entries = entries_by_path(client);
        for (path, expected) in &reference_entries {
            match actual_entries.get(path) {
                Some(actual) if expected.convergence_key() == actual.convergence_key() => {}
                Some(_) => violations.push(violation(
                    "eventual_convergence",
                    "client entry differs from reference",
                    Some(&client.client),
                    Some(path),
                    None,
                    None,
                )),
                None => violations.push(violation(
                    "eventual_convergence",
                    "client is missing reference entry",
                    Some(&client.client),
                    Some(path),
                    None,
                    None,
                )),
            }
        }
        for path in actual_entries.keys() {
            if !reference_entries.contains_key(path) {
                violations.push(violation(
                    "eventual_convergence",
                    "client has extra entry",
                    Some(&client.client),
                    Some(path),
                    None,
                    None,
                ));
            }
        }
    }
}

fn validate_namespace(snapshot: &LogicalSnapshot, violations: &mut Vec<InvariantViolation>) {
    let expected_absent = snapshot
        .expected_absent_paths
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    for client in &snapshot.clients {
        let directory_paths = client
            .entries
            .iter()
            .filter(|entry| entry.kind == LogicalEntryKind::Directory)
            .map(|entry| entry.path.as_str())
            .collect::<BTreeSet<_>>();
        let mut paths = BTreeSet::new();
        let mut paths_by_logical_id: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

        for entry in &client.entries {
            if !paths.insert(entry.path.as_str()) {
                violations.push(violation(
                    "canonical_path_ownership",
                    "duplicate canonical path",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
            }
            if expected_absent.contains(entry.path.as_str()) {
                violations.push(violation(
                    "no_unexpected_resurrection",
                    "path expected absent is present",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
            }
            if parent_path(&entry.path).is_some_and(|parent| !directory_paths.contains(parent)) {
                violations.push(violation(
                    "valid_directory_tree_structure",
                    "entry parent directory is missing",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
            }
            if let Some(logical_id) = entry.logical_id.as_deref() {
                paths_by_logical_id
                    .entry(logical_id)
                    .or_default()
                    .push(entry.path.as_str());
            }
        }

        if !snapshot.allow_hard_links {
            for (logical_id, paths) in paths_by_logical_id {
                if paths.len() > 1 {
                    violations.push(violation(
                        "canonical_path_ownership",
                        "logical id has multiple paths without hard links enabled",
                        Some(&client.client),
                        Some(paths[0]),
                        Some(logical_id),
                        None,
                    ));
                }
            }
        }
    }
}

fn validate_sparse_byte_ranges(
    snapshot: &LogicalSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    for client in &snapshot.clients {
        for entry in &client.entries {
            if entry.byte_ranges.is_empty() {
                continue;
            }
            if entry.kind != LogicalEntryKind::File {
                violations.push(violation(
                    "valid_sparse_byte_ranges",
                    "non-file entry has byte ranges",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
                continue;
            }
            let Some(size) = entry.size else {
                violations.push(violation(
                    "valid_sparse_byte_ranges",
                    "byte-ranged file is missing size",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
                continue;
            };

            let mut cursor = 0;
            for range in &entry.byte_ranges {
                let Some(end) = range.offset.checked_add(range.len) else {
                    violations.push(violation(
                        "valid_sparse_byte_ranges",
                        "byte range overflows",
                        Some(&client.client),
                        Some(&entry.path),
                        entry.logical_id.as_deref(),
                        None,
                    ));
                    continue;
                };
                if range.len == 0 || range.offset != cursor || end > size {
                    violations.push(violation(
                        "valid_sparse_byte_ranges",
                        "byte ranges must be contiguous and inside file size",
                        Some(&client.client),
                        Some(&entry.path),
                        entry.logical_id.as_deref(),
                        None,
                    ));
                    break;
                }
                if range.kind == LogicalByteRangeKind::Data && range.checksum_sha256.is_none() {
                    violations.push(violation(
                        "valid_sparse_byte_ranges",
                        "data range is missing checksum",
                        Some(&client.client),
                        Some(&entry.path),
                        entry.logical_id.as_deref(),
                        None,
                    ));
                    break;
                }
                cursor = end;
            }
            if cursor != size {
                violations.push(violation(
                    "valid_sparse_byte_ranges",
                    "byte ranges do not cover the file size",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    None,
                ));
            }
        }
    }
}

fn validate_durable_writes(snapshot: &LogicalSnapshot, violations: &mut Vec<InvariantViolation>) {
    let clients = snapshot
        .clients
        .iter()
        .map(|client| (client.client.as_str(), entries_by_path(client)))
        .collect::<BTreeMap<_, _>>();

    for expected in &snapshot.durable_writes {
        let Some(entries) = clients.get(expected.client.as_str()) else {
            violations.push(violation(
                "no_lost_locally_durable_writes",
                "durable write client is missing",
                Some(&expected.client),
                Some(&expected.path),
                None,
                None,
            ));
            continue;
        };
        let Some(entry) = entries.get(expected.path.as_str()) else {
            violations.push(violation(
                "no_lost_locally_durable_writes",
                "durable write path is missing",
                Some(&expected.client),
                Some(&expected.path),
                None,
                None,
            ));
            continue;
        };
        if expected
            .checksum_sha256
            .as_ref()
            .is_some_and(|checksum| entry.checksum_sha256.as_ref() != Some(checksum))
            || expected.size.is_some_and(|size| entry.size != Some(size))
        {
            violations.push(violation(
                "no_lost_locally_durable_writes",
                "durable write content differs",
                Some(&expected.client),
                Some(&expected.path),
                entry.logical_id.as_deref(),
                entry
                    .object
                    .as_ref()
                    .map(|object| object.object_id.as_str()),
            ));
        }
    }
}

fn validate_object_reachability(
    snapshot: &LogicalSnapshot,
    violations: &mut Vec<InvariantViolation>,
) {
    let objects = snapshot
        .objects
        .iter()
        .map(|object| (object.object_id.as_str(), object))
        .collect::<BTreeMap<_, _>>();
    for client in &snapshot.clients {
        for entry in &client.entries {
            let Some(object_ref) = &entry.object else {
                continue;
            };
            let lifecycle = objects.get(object_ref.object_id.as_str());
            let reachable = match object_ref.location {
                ObjectLocation::Local => true,
                ObjectLocation::Remote => lifecycle
                    .is_some_and(|lifecycle| lifecycle.present_remote || lifecycle.protected),
            };
            if !reachable {
                violations.push(violation(
                    "valid_object_reachability",
                    "referenced remote object is not present or protected",
                    Some(&client.client),
                    Some(&entry.path),
                    entry.logical_id.as_deref(),
                    Some(&object_ref.object_id),
                ));
            }
        }
    }
}

fn validate_replay(snapshot: &LogicalSnapshot, violations: &mut Vec<InvariantViolation>) {
    if snapshot.replay_idempotent == Some(false) {
        violations.push(violation(
            "idempotent_replication_replay",
            "replay result was not idempotent",
            None,
            None,
            None,
            None,
        ));
    }
}

fn entries_by_path(snapshot: &ClientLogicalSnapshot) -> BTreeMap<&str, &LogicalEntry> {
    snapshot
        .entries
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect()
}

fn parent_path(path: &str) -> Option<&str> {
    path.rsplit_once('/').map(|(parent, _)| parent)
}

fn violation(
    invariant: impl Into<String>,
    message: impl Into<String>,
    client: Option<&str>,
    path: Option<&str>,
    logical_id: Option<&str>,
    object_id: Option<&str>,
) -> InvariantViolation {
    InvariantViolation {
        invariant: invariant.into(),
        message: message.into(),
        client: client.map(str::to_string),
        path: path.map(str::to_string),
        logical_id: logical_id.map(str::to_string),
        object_id: object_id.map(str::to_string),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EntryConvergenceKey<'a> {
    kind: LogicalEntryKind,
    logical_id: Option<&'a str>,
    checksum_sha256: Option<&'a str>,
    size: Option<u64>,
    byte_ranges: &'a [LogicalByteRange],
    symlink_target: Option<&'a str>,
    metadata_attributes: &'a BTreeMap<String, String>,
}
