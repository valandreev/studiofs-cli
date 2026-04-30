//! Shared Contract/Core fixtures for fast local metadata tests.

use crate::schema::{
    CURRENT_FORMAT_VERSION, DirEntry, FileType, FormatInfo, InodeAttr, InodeLifecycleState,
    MetadataConfig, ROOT_INO, RenameAtomicResult,
};
use crate::{
    MetaError, MetaFormat, MetaFsOps, MetaInodes, MetaNamespace, MetaPayloadObjects, MetaResult,
    RedbMetaEngine,
};
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
        Ok(())
    }
}

/// Error returned when Contract/Core invariants fail.
#[derive(Debug, Clone)]
pub struct ContractInvariantViolation {
    label: String,
    diagnostics: ContractInvariantDiagnostics,
}

impl ContractInvariantViolation {
    /// Create a violation from collected diagnostics.
    pub fn new(label: impl Into<String>, diagnostics: ContractInvariantDiagnostics) -> Self {
        Self {
            label: label.into(),
            diagnostics,
        }
    }

    /// Return the checked scenario label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// Return the collected diagnostics.
    pub fn diagnostics(&self) -> &ContractInvariantDiagnostics {
        &self.diagnostics
    }
}

impl fmt::Display for ContractInvariantViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "contract invariant violation: {}", self.label)?;
        write!(f, "{}", self.diagnostics)
    }
}

impl Error for ContractInvariantViolation {}

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

        let mut visited_dirs = BTreeSet::new();
        let mut reachable_inos = BTreeSet::from([ROOT_INO]);
        let mut dentry_counts = BTreeMap::<u64, usize>::new();
        self.collect_tree(
            &meta,
            diagnostics,
            ROOT_INO,
            &attr_by_ino,
            &mut visited_dirs,
            &mut reachable_inos,
            &mut dentry_counts,
        )?;

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

            if attr.kind == FileType::RegularFile {
                match meta.get_regular_file_graph_object_usage(attr.ino) {
                    Ok(mut object_usage) => {
                        object_usage.sort_unstable();
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
        for lifecycle in meta.list_inode_lifecycles()? {
            match attr_by_ino.get(&lifecycle.ino) {
                Some(attr) if attr.kind == FileType::RegularFile => {}
                Some(attr) => diagnostics.push_violation(format!(
                    "lifecycle row for inode {} points at {:?}, expected RegularFile",
                    lifecycle.ino, attr.kind
                )),
                None if lifecycle.state == InodeLifecycleState::DeletedBeforePublish => {}
                None => diagnostics.push_violation(format!(
                    "lifecycle row for inode {} has no matching inode attr",
                    lifecycle.ino
                )),
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_tree(
        &self,
        meta: &Arc<RedbMetaEngine>,
        diagnostics: &mut ContractInvariantDiagnostics,
        parent: u64,
        attr_by_ino: &BTreeMap<u64, InodeAttr>,
        visited_dirs: &mut BTreeSet<u64>,
        reachable_inos: &mut BTreeSet<u64>,
        dentry_counts: &mut BTreeMap<u64, usize>,
    ) -> MetaResult<()> {
        if !visited_dirs.insert(parent) {
            diagnostics.push_violation(format!("directory cycle detected at inode {parent}"));
            return Ok(());
        }

        let entries = match meta.readdir(parent) {
            Ok(entries) => entries,
            Err(err) => {
                diagnostics
                    .push_violation(format!("failed to read directory inode {parent}: {err}"));
                return Ok(());
            }
        };

        let mut names = BTreeSet::<Vec<u8>>::new();
        for entry in entries {
            self.record_entry(
                meta,
                diagnostics,
                parent,
                entry,
                attr_by_ino,
                visited_dirs,
                reachable_inos,
                dentry_counts,
                &mut names,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn record_entry(
        &self,
        meta: &Arc<RedbMetaEngine>,
        diagnostics: &mut ContractInvariantDiagnostics,
        parent: u64,
        entry: DirEntry,
        attr_by_ino: &BTreeMap<u64, InodeAttr>,
        visited_dirs: &mut BTreeSet<u64>,
        reachable_inos: &mut BTreeSet<u64>,
        dentry_counts: &mut BTreeMap<u64, usize>,
        names: &mut BTreeSet<Vec<u8>>,
    ) -> MetaResult<()> {
        if !names.insert(entry.name.clone()) {
            diagnostics.push_violation(format!(
                "directory inode {parent} contains duplicate name {}",
                String::from_utf8_lossy(&entry.name)
            ));
        }
        diagnostics.entries.push(ContractDentrySnapshot {
            parent,
            name: entry.name.clone(),
            ino: entry.ino,
            kind: entry.kind,
        });
        *dentry_counts.entry(entry.ino).or_default() += 1;
        reachable_inos.insert(entry.ino);

        let Some(attr) = attr_by_ino.get(&entry.ino) else {
            diagnostics.push_violation(format!(
                "dentry parent={} name={} points to missing inode {}",
                parent,
                String::from_utf8_lossy(&entry.name),
                entry.ino
            ));
            return Ok(());
        };
        if attr.kind != entry.kind {
            diagnostics.push_violation(format!(
                "dentry parent={} name={} kind {:?} disagrees with inode {} kind {:?}",
                parent,
                String::from_utf8_lossy(&entry.name),
                entry.kind,
                entry.ino,
                attr.kind
            ));
        }
        if attr.kind == FileType::Directory {
            if attr.parent != parent {
                diagnostics.push_violation(format!(
                    "directory inode {} records parent {} but is linked from {}",
                    attr.ino, attr.parent, parent
                ));
            }
            self.collect_tree(
                meta,
                diagnostics,
                entry.ino,
                attr_by_ino,
                visited_dirs,
                reachable_inos,
                dentry_counts,
            )?;
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
        block_size: crate::schema::DEFAULT_BLOCK_SIZE,
    }
}

#[derive(Debug)]
struct ContractTempDir {
    path: PathBuf,
}

impl ContractTempDir {
    fn new(label: &str) -> MetaResult<Self> {
        let safe = sanitize_label(label);
        let counter = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir()
            .join("studiofs-contract-core")
            .join(format!("{safe}-{}-{counter}-{now}", std::process::id()));
        std::fs::create_dir_all(&path).map_err(|err| {
            MetaError::Storage(format!(
                "failed to create contract fixture directory {}: {err}",
                path.display()
            ))
        })?;
        Ok(Self { path })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ContractTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
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
