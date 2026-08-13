//! Shared Contract/Core fixtures for fast local metadata tests.

use crate::engine::DeferredDeleteRecord;
use crate::engine::MetaGc;
use crate::schema::{
    CURRENT_FORMAT_VERSION, ENCRYPTION_KEY_LENGTH, EncryptionConfig, FileType, FormatInfo,
    InodeAttr, InodeLifecycleRecord, InodeLifecycleState, MetadataConfig, ROOT_INO,
    RenameAtomicResult,
};
use crate::{
    MetaError, MetaFormat, MetaFsOps, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaRecovery,
    MetaResult, RedbMetaEngine,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

/// Deterministic filesystem-like metadata operation used by Contract/Core tests.
#[derive(Debug, Clone)]
pub enum ContractCoreOp {
    /// Create a regular file.
    CreateFile {
        /// Parent directory inode.
        parent: u64,
        /// File name bytes.
        name: Vec<u8>,
        /// Unix permission mode.
        mode: u16,
    },
    /// Create a directory.
    CreateDir {
        /// Parent directory inode.
        parent: u64,
        /// Directory name bytes.
        name: Vec<u8>,
        /// Unix permission mode.
        mode: u16,
    },
    /// Create a symbolic link.
    CreateSymlink {
        /// Parent directory inode.
        parent: u64,
        /// Link name bytes.
        name: Vec<u8>,
        /// Link target bytes.
        target: Vec<u8>,
    },
    /// Rename a directory entry.
    Rename {
        /// Source parent inode.
        parent: u64,
        /// Source name bytes.
        name: Vec<u8>,
        /// Destination parent inode.
        new_parent: u64,
        /// Destination name bytes.
        new_name: Vec<u8>,
    },
    /// Unlink a regular file or symlink.
    Unlink {
        /// Parent directory inode.
        parent: u64,
        /// Entry name bytes.
        name: Vec<u8>,
    },
    /// Open-unlink a regular file, removing the dentry while keeping the inode
    /// alive for deferred cleanup.
    OpenUnlink {
        /// Parent directory inode.
        parent: u64,
        /// Entry name bytes.
        name: Vec<u8>,
    },
    /// Remove an empty directory.
    Rmdir {
        /// Parent directory inode.
        parent: u64,
        /// Directory name bytes.
        name: Vec<u8>,
    },
}

/// Result returned after applying a [`ContractCoreOp`].
#[derive(Debug, Clone)]
pub enum ContractCoreOpOutcome {
    /// A filesystem object was created.
    Created(InodeAttr),
    /// A filesystem object was unlinked.
    Unlinked(InodeAttr),
    /// A directory was removed.
    RemovedDirectory,
    /// A dentry was renamed.
    Renamed(RenameAtomicResult),
}

impl ContractCoreOpOutcome {
    /// Return the created inode attributes for create operations.
    pub fn created_attr(&self) -> Option<InodeAttr> {
        match self {
            Self::Created(attr) => Some(attr.clone()),
            Self::Unlinked(_) | Self::RemovedDirectory | Self::Renamed(_) => None,
        }
    }
}

/// Attribute snapshot included in invariant diagnostics.
#[derive(Debug, Clone)]
pub struct ContractAttrSnapshot {
    /// Inode number.
    pub ino: u64,
    /// File type.
    pub kind: FileType,
    /// File size visible locally.
    pub size: u64,
    /// Committed file size visible remotely.
    pub committed_size: u64,
    /// Link count.
    pub nlink: u32,
    /// Parent inode recorded in the attribute.
    pub parent: u64,
    /// Local content epoch.
    pub content_epoch: u64,
    /// Committed content epoch.
    pub committed_content_epoch: u64,
}

impl From<&InodeAttr> for ContractAttrSnapshot {
    fn from(attr: &InodeAttr) -> Self {
        Self {
            ino: attr.ino,
            kind: attr.kind,
            size: attr.size,
            committed_size: attr.committed_size,
            nlink: attr.nlink,
            parent: attr.parent,
            content_epoch: attr.content_epoch,
            committed_content_epoch: attr.committed_content_epoch,
        }
    }
}

/// Directory entry snapshot included in invariant diagnostics.
#[derive(Debug, Clone)]
pub struct ContractDentrySnapshot {
    /// Parent directory inode.
    pub parent: u64,
    /// Entry name bytes.
    pub name: Vec<u8>,
    /// Child inode number.
    pub ino: u64,
    /// File type stored in the dentry.
    pub kind: FileType,
}

/// Regular-file object graph usage snapshot included in invariant diagnostics.
#[derive(Debug, Clone)]
pub struct ContractObjectGraphSnapshot {
    /// Regular-file inode.
    pub ino: u64,
    /// Deduplicated object id and physical byte usage pairs.
    pub object_usage: Vec<(u64, u64)>,
}

/// Regular-file lifecycle snapshot included in invariant diagnostics.
#[derive(Debug, Clone)]
pub struct ContractLifecycleSnapshot {
    /// Regular-file inode.
    pub ino: u64,
    /// Current lifecycle state.
    pub state: InodeLifecycleState,
    /// Locally visible file size for the lifecycle epoch.
    pub local_visible_size: u64,
    /// Published file size for the lifecycle epoch.
    pub published_size: u64,
    /// Locally visible content epoch.
    pub local_content_epoch: u64,
    /// Published content epoch.
    pub published_content_epoch: u64,
    /// Whether this lifecycle has a published base.
    pub has_published_base: bool,
    /// Object ids awaiting deferred cleanup for deleted-before-publish epochs.
    pub deferred_cleanup_object_ids: Vec<u64>,
}

impl From<&InodeLifecycleRecord> for ContractLifecycleSnapshot {
    fn from(record: &InodeLifecycleRecord) -> Self {
        Self {
            ino: record.ino,
            state: record.state,
            local_visible_size: record.local_visible_size,
            published_size: record.published_size,
            local_content_epoch: record.local_content_epoch,
            published_content_epoch: record.published_content_epoch,
            has_published_base: record.has_published_base,
            deferred_cleanup_object_ids: record.deferred_cleanup_object_ids.clone(),
        }
    }
}

/// Deferred delete snapshot included in invariant diagnostics.
#[derive(Debug, Clone)]
pub struct ContractDeferredDeleteSnapshot {
    /// Orphaned inode awaiting deferred cleanup.
    pub ino: u64,
    /// Parent directory from which the dentry was removed.
    pub parent: u64,
    /// Original dentry name removed from the parent.
    pub name: Vec<u8>,
    /// Unix timestamp when the record was created.
    pub created_at: i64,
}

impl From<&DeferredDeleteRecord> for ContractDeferredDeleteSnapshot {
    fn from(record: &DeferredDeleteRecord) -> Self {
        Self {
            ino: record.ino,
            parent: record.parent,
            name: record.name.clone(),
            created_at: record.created_at,
        }
    }
}

/// Collected diagnostics for a Contract/Core invariant check.
#[derive(Debug, Clone, Default)]
pub struct ContractInvariantDiagnostics {
    /// Label supplied by the caller for the checked scenario.
    pub label: String,
    /// All metadata attributes visible through `list_inode_attrs`.
    pub attrs: Vec<ContractAttrSnapshot>,
    /// All reachable dentries discovered from the root directory.
    pub entries: Vec<ContractDentrySnapshot>,
    /// Object graph usage snapshots for regular files.
    pub object_graphs: Vec<ContractObjectGraphSnapshot>,
    /// Full metadata reachability result for payload object ids.
    pub referenced_object_ids: Vec<u64>,
    /// Pending object deletions visible to GC.
    pub pending_deletions: Vec<(u64, i64)>,
    /// Lifecycle rows visible through metadata recovery state.
    pub lifecycles: Vec<ContractLifecycleSnapshot>,
    /// Deferred-delete records that explain open-unlink orphan inodes.
    pub deferred_deletes: Vec<ContractDeferredDeleteSnapshot>,
    /// Human-readable invariant violations.
    pub violations: Vec<String>,
}

impl ContractInvariantDiagnostics {
    fn push_violation(&mut self, violation: impl Into<String>) {
        self.violations.push(violation.into());
    }
}

impl fmt::Display for ContractInvariantDiagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "contract invariant diagnostics: {}", self.label)?;
        if !self.violations.is_empty() {
            writeln!(f, "violations:")?;
            for violation in &self.violations {
                writeln!(f, "  - {violation}")?;
            }
        }

        writeln!(f, "attrs:")?;
        for attr in &self.attrs {
            writeln!(
                f,
                "  - ino={} kind={:?} size={} committed_size={} nlink={} parent={} epoch={}/{}",
                attr.ino,
                attr.kind,
                attr.size,
                attr.committed_size,
                attr.nlink,
                attr.parent,
                attr.content_epoch,
                attr.committed_content_epoch
            )?;
        }

        writeln!(f, "dentries:")?;
        for entry in &self.entries {
            writeln!(
                f,
                "  - parent={} name={} ino={} kind={:?}",
                entry.parent,
                String::from_utf8_lossy(&entry.name),
                entry.ino,
                entry.kind
            )?;
        }

        writeln!(f, "object_graphs:")?;
        for graph in &self.object_graphs {
            writeln!(f, "  - ino={} usage={:?}", graph.ino, graph.object_usage)?;
        }

        writeln!(f, "referenced_object_ids: {:?}", self.referenced_object_ids)?;

        writeln!(f, "pending_deletions:")?;
        for (object_id, timestamp) in &self.pending_deletions {
            writeln!(f, "  - object_id={} timestamp={}", object_id, timestamp)?;
        }

        writeln!(f, "lifecycles:")?;
        for lifecycle in &self.lifecycles {
            writeln!(
                f,
                "  - ino={} state={:?} local_size={} published_size={} epoch={}/{} has_published_base={} deferred_cleanup={:?}",
                lifecycle.ino,
                lifecycle.state,
                lifecycle.local_visible_size,
                lifecycle.published_size,
                lifecycle.local_content_epoch,
                lifecycle.published_content_epoch,
                lifecycle.has_published_base,
                lifecycle.deferred_cleanup_object_ids
            )?;
        }

        writeln!(f, "deferred_deletes:")?;
        for record in &self.deferred_deletes {
            writeln!(
                f,
                "  - ino={} parent={} name={} created_at={}",
                record.ino,
                record.parent,
                String::from_utf8_lossy(&record.name),
                record.created_at
            )?;
        }
        Ok(())
    }
}

/// Error returned when Contract/Core invariants fail.
#[derive(Debug, Clone)]
pub struct ContractInvariantViolation {
    label: String,
    diagnostics: Box<ContractInvariantDiagnostics>,
}

impl ContractInvariantViolation {
    /// Create a violation from collected diagnostics.
    pub fn new(label: impl Into<String>, diagnostics: ContractInvariantDiagnostics) -> Self {
        Self {
            label: label.into(),
            diagnostics: Box::new(diagnostics),
        }
    }

    /// Return the checked scenario label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Return the collected diagnostics.
    pub fn diagnostics(&self) -> &ContractInvariantDiagnostics {
        self.diagnostics.as_ref()
    }
}

impl fmt::Display for ContractInvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "contract invariant violation: {}", self.label)?;
        write!(f, "{}", self.diagnostics)
    }
}

impl Error for ContractInvariantViolation {}

struct TreeInvariantState<'a> {
    diagnostics: &'a mut ContractInvariantDiagnostics,
    attr_by_ino: &'a BTreeMap<u64, InodeAttr>,
    visited_dirs: &'a mut BTreeSet<u64>,
    reachable_inos: &'a mut BTreeSet<u64>,
    dentry_counts: &'a mut BTreeMap<u64, usize>,
    direct_subdir_counts: &'a mut BTreeMap<u64, usize>,
}

/// Temporary local fixture that owns an isolated redb metadata database.
pub struct ContractCoreFixture {
    root: ContractTempDir,
    db_path: PathBuf,
    format_info: FormatInfo,
    uid: u32,
    gid: u32,
    meta: Option<Arc<RedbMetaEngine>>,
}

impl ContractCoreFixture {
    /// Create a fresh local redb metadata fixture with default test format info.
    pub fn new(label: &str) -> MetaResult<Self> {
        let format_info = contract_format_info(label);
        Self::new_with_format_info(label, format_info)
    }

    /// Create a fresh local redb metadata fixture with caller-supplied format info.
    pub fn new_with_format_info(label: &str, format_info: FormatInfo) -> MetaResult<Self> {
        let root = ContractTempDir::new(label)?;
        let db_path = root.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path)?);
        meta.init_root(0, 0)?;
        meta.set_format_info(&format_info)?;

        Ok(Self {
            root,
            db_path,
            format_info,
            uid: 0,
            gid: 0,
            meta: Some(meta),
        })
    }

    /// Return the fixture root directory.
    pub fn root_path(&self) -> &Path {
        self.root.path()
    }

    /// Return the redb database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Return the format info used by the fixture.
    pub fn format_info(&self) -> &FormatInfo {
        &self.format_info
    }

    /// Return a clone of the current redb metadata engine.
    pub fn meta(&self) -> Arc<RedbMetaEngine> {
        self.meta
            .as_ref()
            .expect("contract fixture metadata engine should be open")
            .clone()
    }

    /// Drop the current engine handle and reopen the same local metadata path.
    pub fn reopen_meta(&mut self) -> MetaResult<Arc<RedbMetaEngine>> {
        self.meta.take();
        let meta = Arc::new(RedbMetaEngine::open(&self.db_path)?);
        meta.get_format_info()?;
        self.meta = Some(meta.clone());
        Ok(meta)
    }

    /// Apply one deterministic core metadata operation.
    pub fn apply_op(&self, op: ContractCoreOp) -> MetaResult<ContractCoreOpOutcome> {
        let meta = self.meta();
        match op {
            ContractCoreOp::CreateFile { parent, name, mode } => meta
                .create_file(parent, &name, self.uid, self.gid, mode)
                .map(ContractCoreOpOutcome::Created),
            ContractCoreOp::CreateDir { parent, name, mode } => meta
                .create_dir(parent, &name, self.uid, self.gid, mode)
                .map(ContractCoreOpOutcome::Created),
            ContractCoreOp::CreateSymlink {
                parent,
                name,
                target,
            } => meta
                .create_symlink(parent, &name, &target, self.uid, self.gid)
                .map(ContractCoreOpOutcome::Created),
            ContractCoreOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
            } => meta
                .rename_atomic(parent, &name, new_parent, &new_name)
                .map(ContractCoreOpOutcome::Renamed),
            ContractCoreOp::Unlink { parent, name } => meta
                .unlink(parent, &name)
                .map(ContractCoreOpOutcome::Unlinked),
            ContractCoreOp::OpenUnlink { parent, name } => meta
                .unlink_open_handle(parent, &name)
                .map(ContractCoreOpOutcome::Unlinked),
            ContractCoreOp::Rmdir { parent, name } => {
                meta.rmdir(parent, &name)?;
                Ok(ContractCoreOpOutcome::RemovedDirectory)
            }
        }
    }

    /// Collect and assert common metadata/tree/object-graph invariants.
    pub fn assert_invariants(
        &self,
        label: &str,
    ) -> Result<ContractInvariantDiagnostics, ContractInvariantViolation> {
        let mut diagnostics = ContractInvariantDiagnostics {
            label: label.to_string(),
            ..ContractInvariantDiagnostics::default()
        };

        match self.collect_invariants(&mut diagnostics) {
            Ok(()) if diagnostics.violations.is_empty() => Ok(diagnostics),
            Ok(()) => Err(ContractInvariantViolation::new(label, diagnostics)),
            Err(err) => {
                diagnostics.push_violation(format!("failed to collect invariants: {err}"));
                Err(ContractInvariantViolation::new(label, diagnostics))
            }
        }
    }

    fn collect_invariants(&self, diagnostics: &mut ContractInvariantDiagnostics) -> MetaResult<()> {
        let meta = self.meta();
        let root = match meta.get_attr(ROOT_INO) {
            Ok(attr) => attr,
            Err(err) => {
                diagnostics.push_violation(format!("root inode {ROOT_INO} is missing: {err}"));
                return Ok(());
            }
        };
        if root.kind != FileType::Directory {
            diagnostics.push_violation(format!(
                "root inode {ROOT_INO} has kind {:?}, expected Directory",
                root.kind
            ));
        }

        let attrs = meta.list_inode_attrs()?;
        let attr_by_ino: BTreeMap<u64, InodeAttr> =
            attrs.iter().map(|attr| (attr.ino, attr.clone())).collect();
        diagnostics.attrs = attrs.iter().map(ContractAttrSnapshot::from).collect();
        diagnostics.attrs.sort_by_key(|attr| attr.ino);

        let deferred_deletes = meta.list_deferred_deletes()?;
        let deferred_delete_by_ino: BTreeMap<u64, DeferredDeleteRecord> = deferred_deletes
            .iter()
            .map(|record| (record.ino, record.clone()))
            .collect();
        diagnostics.deferred_deletes = deferred_deletes
            .iter()
            .map(ContractDeferredDeleteSnapshot::from)
            .collect();
        diagnostics
            .deferred_deletes
            .sort_by_key(|record| record.ino);

        let lifecycles = meta.list_inode_lifecycles()?;
        let lifecycle_by_ino: BTreeMap<u64, InodeLifecycleRecord> = lifecycles
            .iter()
            .map(|record| (record.ino, record.clone()))
            .collect();
        diagnostics.lifecycles = lifecycles
            .iter()
            .map(ContractLifecycleSnapshot::from)
            .collect();
        diagnostics.lifecycles.sort_by_key(|record| record.ino);

        let mut object_graph_ref_inos = BTreeMap::<u64, BTreeSet<u64>>::new();
        for attr in attr_by_ino.values() {
            if attr.kind == FileType::RegularFile {
                match meta.get_regular_file_graph_object_usage(attr.ino) {
                    Ok(mut object_usage) => {
                        object_usage.sort_unstable();
                        for (object_id, _bytes) in &object_usage {
                            object_graph_ref_inos
                                .entry(*object_id)
                                .or_default()
                                .insert(attr.ino);
                        }
                        diagnostics.object_graphs.push(ContractObjectGraphSnapshot {
                            ino: attr.ino,
                            object_usage,
                        });
                    }
                    Err(err) => diagnostics.push_violation(format!(
                        "failed to read object graph for inode {}: {err}",
                        attr.ino
                    )),
                }
            }
        }
        diagnostics.object_graphs.sort_by_key(|graph| graph.ino);

        let mut deferred_cleanup_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
        for lifecycle in lifecycle_by_ino.values() {
            let is_open_unlink_cleanup = lifecycle.state
                == InodeLifecycleState::DeletedBeforePublish
                && deferred_delete_by_ino.contains_key(&lifecycle.ino)
                && matches!(
                    attr_by_ino.get(&lifecycle.ino),
                    Some(attr) if attr.kind == FileType::RegularFile && attr.nlink == 0
                );
            if is_open_unlink_cleanup {
                for object_id in &lifecycle.deferred_cleanup_object_ids {
                    deferred_cleanup_object_owners
                        .entry(*object_id)
                        .or_default()
                        .insert(lifecycle.ino);
                }
            }
        }

        let mut referenced_object_ids: Vec<u64> =
            meta.get_referenced_object_ids()?.into_iter().collect();
        referenced_object_ids.sort_unstable();

        let mut pending_deletions = meta.get_pending_deletions()?;
        pending_deletions.sort_unstable_by_key(|(object_id, _timestamp)| *object_id);

        for (object_id, timestamp) in &pending_deletions {
            if referenced_object_ids.binary_search(object_id).is_ok()
                && !Self::pending_deletion_ref_is_deferred_cleanup(
                    *object_id,
                    &object_graph_ref_inos,
                    &deferred_cleanup_object_owners,
                )
            {
                diagnostics.push_violation(format!(
                    "object_id {object_id} is marked for pending deletion (timestamp={timestamp}) but is still reachable from metadata"
                ));
            }
        }

        diagnostics.referenced_object_ids = referenced_object_ids;
        diagnostics.pending_deletions = pending_deletions;

        let mut visited_dirs = BTreeSet::new();
        let mut reachable_inos = BTreeSet::from([ROOT_INO]);
        let mut dentry_counts = BTreeMap::<u64, usize>::new();
        let mut direct_subdir_counts = BTreeMap::<u64, usize>::new();
        let mut tree_state = TreeInvariantState {
            diagnostics,
            attr_by_ino: &attr_by_ino,
            visited_dirs: &mut visited_dirs,
            reachable_inos: &mut reachable_inos,
            dentry_counts: &mut dentry_counts,
            direct_subdir_counts: &mut direct_subdir_counts,
        };
        Self::collect_tree_iterative(&meta, &mut tree_state)?;

        for attr in attr_by_ino.values() {
            if reachable_inos.contains(&attr.ino) {
                if attr.nlink == 0 {
                    diagnostics
                        .push_violation(format!("reachable inode {} has zero nlink", attr.ino));
                }
            } else if attr.nlink > 0 {
                diagnostics.push_violation(format!(
                    "inode {} has nlink={} but is not reachable from root",
                    attr.ino, attr.nlink
                ));
            } else if !deferred_delete_by_ino.contains_key(&attr.ino) {
                diagnostics.push_violation(format!(
                    "inode {} has zero nlink but is not reachable from root and has no deferred delete record",
                    attr.ino
                ));
            }

            if matches!(attr.kind, FileType::RegularFile | FileType::Symlink) {
                let count = dentry_counts.get(&attr.ino).copied().unwrap_or_default();
                if count > 0 && count != attr.nlink as usize {
                    diagnostics.push_violation(format!(
                        "inode {} has nlink={} but {} reachable dentries",
                        attr.ino, attr.nlink, count
                    ));
                }
            }

            if attr.kind == FileType::Directory && reachable_inos.contains(&attr.ino) {
                let expected_nlink = 2usize
                    .saturating_add(direct_subdir_counts.get(&attr.ino).copied().unwrap_or(0));
                if attr.nlink as usize != expected_nlink {
                    diagnostics.push_violation(format!(
                        "directory inode {} has nlink={} but expected {}",
                        attr.ino, attr.nlink, expected_nlink
                    ));
                }
            }
        }

        for lifecycle in lifecycle_by_ino.values() {
            match attr_by_ino.get(&lifecycle.ino) {
                Some(attr) if attr.kind == FileType::RegularFile => {}
                Some(attr) => diagnostics.push_violation(format!(
                    "lifecycle row for inode {} state {:?} points at {:?}, expected RegularFile",
                    lifecycle.ino, lifecycle.state, attr.kind
                )),
                None if lifecycle.state == InodeLifecycleState::DeletedBeforePublish => {}
                None => diagnostics.push_violation(format!(
                    "lifecycle row for inode {} state {:?} has no matching inode attr",
                    lifecycle.ino, lifecycle.state
                )),
            }
        }
        for record in deferred_delete_by_ino.values() {
            match attr_by_ino.get(&record.ino) {
                Some(attr) if attr.kind == FileType::RegularFile && attr.nlink == 0 => {}
                Some(attr) => diagnostics.push_violation(format!(
                    "deferred delete record ino={} parent={} name={} points at {:?} nlink={}, expected zero-link RegularFile",
                    record.ino,
                    record.parent,
                    String::from_utf8_lossy(&record.name),
                    attr.kind,
                    attr.nlink
                )),
                None => diagnostics.push_violation(format!(
                    "deferred delete record ino={} parent={} name={} has no matching inode attr",
                    record.ino,
                    record.parent,
                    String::from_utf8_lossy(&record.name)
                )),
            }
        }
        Ok(())
    }

    fn pending_deletion_ref_is_deferred_cleanup(
        object_id: u64,
        object_graph_ref_inos: &BTreeMap<u64, BTreeSet<u64>>,
        deferred_cleanup_object_owners: &BTreeMap<u64, BTreeSet<u64>>,
    ) -> bool {
        let Some(ref_inos) = object_graph_ref_inos.get(&object_id) else {
            return false;
        };
        let Some(cleanup_owners) = deferred_cleanup_object_owners.get(&object_id) else {
            return false;
        };
        !ref_inos.is_empty() && ref_inos.iter().all(|ino| cleanup_owners.contains(ino))
    }

    fn collect_tree_iterative(
        meta: &Arc<RedbMetaEngine>,
        state: &mut TreeInvariantState<'_>,
    ) -> MetaResult<()> {
        let mut pending_dirs = vec![ROOT_INO];
        let mut queued_dirs = BTreeSet::from([ROOT_INO]);

        while let Some(parent) = pending_dirs.pop() {
            if !state.visited_dirs.insert(parent) {
                state
                    .diagnostics
                    .push_violation(format!("directory cycle detected at inode {parent}"));
                continue;
            }

            let entries = match meta.readdir(parent) {
                Ok(entries) => entries,
                Err(err) => {
                    state
                        .diagnostics
                        .push_violation(format!("failed to read directory inode {parent}: {err}"));
                    continue;
                }
            };

            let mut names = BTreeSet::<Vec<u8>>::new();
            for entry in entries {
                if !names.insert(entry.name.clone()) {
                    state.diagnostics.push_violation(format!(
                        "directory inode {parent} contains duplicate name {}",
                        String::from_utf8_lossy(&entry.name)
                    ));
                }
                state.diagnostics.entries.push(ContractDentrySnapshot {
                    parent,
                    name: entry.name.clone(),
                    ino: entry.ino,
                    kind: entry.kind,
                });
                *state.dentry_counts.entry(entry.ino).or_default() += 1;
                state.reachable_inos.insert(entry.ino);

                let Some(attr) = state.attr_by_ino.get(&entry.ino) else {
                    state.diagnostics.push_violation(format!(
                        "dentry parent={} name={} points to missing inode {}",
                        parent,
                        String::from_utf8_lossy(&entry.name),
                        entry.ino
                    ));
                    continue;
                };
                if attr.kind != entry.kind {
                    state.diagnostics.push_violation(format!(
                        "dentry parent={} name={} kind {:?} disagrees with inode {} kind {:?}",
                        parent,
                        String::from_utf8_lossy(&entry.name),
                        entry.kind,
                        entry.ino,
                        attr.kind
                    ));
                }
                if attr.kind == FileType::Directory {
                    *state.direct_subdir_counts.entry(parent).or_default() += 1;
                    if attr.parent != parent {
                        state.diagnostics.push_violation(format!(
                            "directory inode {} records parent {} but is linked from {}",
                            attr.ino, attr.parent, parent
                        ));
                    }
                    if state.visited_dirs.contains(&entry.ino) || !queued_dirs.insert(entry.ino) {
                        state.diagnostics.push_violation(format!(
                            "directory inode {} is linked more than once or creates a cycle",
                            entry.ino
                        ));
                    } else {
                        pending_dirs.push(entry.ino);
                    }
                }
            }
        }

        Ok(())
    }
}

/// Build default local-only format metadata for Contract/Core fixtures.
pub fn contract_format_info(label: &str) -> FormatInfo {
    let safe = sanitize_label(label);
    let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
    let name = format!("contract-{safe}-{}-{id}", std::process::id());
    FormatInfo {
        format_version: CURRENT_FORMAT_VERSION,
        name: name.clone(),
        uuid: format!("{name}-uuid"),
        endpoint: "http://127.0.0.1:59999".to_string(),
        bucket: "studiofs-contract-test".to_string(),
        access_key: "studiofs".to_string(),
        secret_key: "studiofs123".to_string(),
        region: "us-east-1".to_string(),
        data_prefix: format!("{name}/objects/"),
        metadata: MetadataConfig::default(),
        encryption: EncryptionConfig::new([0xA5; ENCRYPTION_KEY_LENGTH]),
        block_size: crate::schema::DEFAULT_BLOCK_SIZE,
    }
}

#[derive(Debug)]
struct ContractTempDir {
    inner: tempfile::TempDir,
}

impl ContractTempDir {
    fn new(label: &str) -> MetaResult<Self> {
        let safe = sanitize_label(label);
        let prefix = format!("studiofs-contract-core-{safe}-");
        // This harness is part of the normal studiofs-meta library surface, so
        // it cannot depend on the dev-only workspace target tempdir helper.
        let inner = tempfile::Builder::new()
            .prefix(&prefix)
            .tempdir()
            .map_err(|err| {
                MetaError::Storage(format!(
                    "failed to create contract fixture directory for {safe}: {err}"
                ))
            })?;
        Ok(Self { inner })
    }

    fn path(&self) -> &Path {
        self.inner.path()
    }
}

fn sanitize_label(label: &str) -> String {
    let sanitized: String = label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "fixture".to_string()
    } else {
        sanitized.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redb_engine::RedbSnapshot;
    use crate::snapshot::{ServingCheckpoint, SnapshotCut};
    use chrono::TimeZone;

    #[test]
    fn snapshot_v2_public_contract() {
        let captured_at = chrono::Utc
            .with_ymd_and_hms(2026, 8, 13, 10, 11, 12)
            .single()
            .expect("valid captured timestamp");
        let cut = SnapshotCut::new(7, 42, captured_at).expect("valid snapshot cut");
        let cut_json = serde_json::to_value(cut).expect("serialize public snapshot cut");
        let cut_fields = cut_json
            .as_object()
            .expect("snapshot cut is an object")
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            cut_fields,
            BTreeSet::from([
                "captured_at".to_string(),
                "cut_version".to_string(),
                "volume_id".to_string(),
            ])
        );

        let serving_json = serde_json::to_value(ServingCheckpoint::default())
            .expect("serialize serving checkpoint");
        let serving_text = serving_json.to_string();
        for forbidden in [
            "root_ino",
            "snapshot_payload",
            "pinned_object_ids",
            "event_log",
            "counters",
        ] {
            assert!(
                !serving_text.contains(forbidden),
                "serving checkpoint must not contain legacy/local field {forbidden}"
            );
        }

        let local_snapshot_text =
            serde_json::to_string(&RedbSnapshot::default()).expect("serialize local snapshot");
        for forbidden in ["\"snapshots\"", "snapshot_payload", "pinned_object_ids"] {
            assert!(
                !local_snapshot_text.contains(forbidden),
                "local recovery snapshot must not contain removed field {forbidden}"
            );
        }

        let ddl = crate::pg_engine::schema::studiofs_pg_schema_sql();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS snapshot_source_checkpoints"));
        assert!(ddl.contains("checkpoint_version BIGINT"));
        assert!(ddl.contains("event_start_version BIGINT"));
        assert!(ddl.contains("event_end_version BIGINT"));
        assert!(ddl.contains("event_row_count BIGINT"));
        assert!(ddl.contains("studiofs_snapshot_source_event_prune_guard"));
        assert!(ddl.contains("studiofs_snapshot_source_prune_guard"));
        assert!(!ddl.contains("CREATE TABLE IF NOT EXISTS snapshots"));
        assert!(!ddl.contains("snapshot_payload"));
        assert!(!ddl.contains("pinned_object_ids"));
    }
}
