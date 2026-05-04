//! Contract/Core VFS fixtures for fast local tests.

use crate::{OpenFlags, SetattrArgs, StudioVfs, VfsConfigRaw, VfsConfigValidated, VfsError};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use studiofs_data::store::DataStoreConfig;
use studiofs_meta::contract_harness::{
    ContractCoreFixture, ContractInvariantDiagnostics, ContractInvariantViolation,
};
use studiofs_meta::schema::{InodeAttr, ROOT_INO};
use studiofs_meta::{MetaError, RedbMetaEngine};
use studiofs_scenario::{
    OperationOutcome, ScenarioOperation, ScenarioOperationRecord, SimulationSupport,
};
use tokio::runtime::Runtime;

const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(100);

/// Result type for VFS Contract/Core fixture operations.
pub type ContractVfsResult<T> = Result<T, ContractVfsHarnessError>;

/// Errors surfaced by the VFS Contract/Core fixture.
#[derive(Debug, thiserror::Error)]
pub enum ContractVfsHarnessError {
    /// Metadata fixture operation failed.
    #[error("metadata fixture error: {0}")]
    Meta(#[from] MetaError),
    /// VFS operation failed.
    #[error("vfs fixture error: {0}")]
    Vfs(#[from] VfsError),
    /// Invariant check failed.
    #[error("{0}")]
    Invariant(#[from] ContractInvariantViolation),
    /// Tokio runtime setup failed.
    #[error("failed to create contract fixture runtime: {0}")]
    Runtime(#[from] std::io::Error),
    /// Scenario operation replay failed before Contract/Core invariants could run.
    #[error("contract replay failed: {0}")]
    Replay(String),
}

/// Deterministic filesystem-like VFS operation used by Contract/Core tests.
#[derive(Debug, Clone)]
pub enum ContractVfsOp {
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
    Mkdir {
        /// Parent directory inode.
        parent: u64,
        /// Directory name bytes.
        name: Vec<u8>,
        /// Unix permission mode.
        mode: u16,
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

/// Result returned after applying a [`ContractVfsOp`].
#[derive(Debug, Clone)]
pub enum ContractVfsOpOutcome {
    /// A filesystem object was created.
    Created(InodeAttr),
    /// A filesystem object was removed or renamed.
    Completed,
}

/// Summary returned after replaying shared scenario operations into Contract/Core.
#[derive(Debug, Clone, Default)]
pub struct ContractReplayReport {
    /// Records that were applied to the production-backed fixture.
    pub applied: Vec<ContractReplayAppliedRecord>,
    /// Records that were intentionally skipped because they are observational,
    /// unsupported, or outside the non-mounted Contract/Core replay boundary.
    pub skipped: Vec<ContractReplaySkippedRecord>,
}

/// One operation record applied by Contract/Core replay.
#[derive(Debug, Clone)]
pub struct ContractReplayAppliedRecord {
    /// Operation log sequence.
    pub sequence: u64,
    /// Human-readable operation kind.
    pub operation: String,
    /// Operation path, when the operation has one primary path.
    pub path: Option<String>,
}

/// One operation record skipped by Contract/Core replay.
#[derive(Debug, Clone)]
pub struct ContractReplaySkippedRecord {
    /// Operation log sequence.
    pub sequence: u64,
    /// Human-readable operation kind.
    pub operation: String,
    /// Reason the record was not applied to the production fixture.
    pub reason: String,
}

impl ContractVfsOpOutcome {
    /// Return the created inode attributes for create operations.
    pub fn created_attr(&self) -> Option<InodeAttr> {
        match self {
            Self::Created(attr) => Some(attr.clone()),
            Self::Completed => None,
        }
    }
}

/// Local VFS fixture backed by an isolated `RedbMetaEngine` and cache directory.
pub struct ContractVfsFixture {
    core: ContractCoreFixture,
    runtime: Option<Runtime>,
    data_config: DataStoreConfig,
    cache_dir: PathBuf,
    cache_size: u64,
    memory_cache_budget_bytes: u64,
    config: VfsConfigValidated,
    shutdown_timeout: Duration,
    vfs: Option<Arc<StudioVfs>>,
}

impl ContractVfsFixture {
    /// Create a fresh VFS Contract/Core fixture with default VFS config.
    pub fn new(label: &str) -> ContractVfsResult<Self> {
        Self::new_with_config(
            label,
            VfsConfigRaw::default()
                .validate()
                .expect("default VFS config should validate"),
        )
    }

    /// Create a fresh VFS Contract/Core fixture with caller-supplied VFS config.
    pub fn new_with_config(label: &str, config: VfsConfigValidated) -> ContractVfsResult<Self> {
        let core = ContractCoreFixture::new(label)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()?;
        let data_config = DataStoreConfig::from(core.format_info());
        let cache_dir = core.root_path().join("cache");
        let mut fixture = Self {
            core,
            runtime: Some(runtime),
            data_config,
            cache_dir,
            cache_size: 64 * 1024 * 1024,
            memory_cache_budget_bytes: 32 * 1024 * 1024,
            config,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            vfs: None,
        };
        let vfs = fixture.open_vfs()?;
        fixture.vfs = Some(vfs);
        Ok(fixture)
    }

    /// Set the graceful shutdown drain timeout used by reopen and drop cleanup.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Return the graceful shutdown drain timeout used by reopen and drop cleanup.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Return the underlying metadata fixture.
    pub fn core(&self) -> &ContractCoreFixture {
        &self.core
    }

    /// Return the redb metadata path.
    pub fn db_path(&self) -> &Path {
        self.core.db_path()
    }

    /// Return the local cache directory.
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Return the Tokio runtime used by the fixture.
    pub fn runtime(&self) -> &Runtime {
        self.runtime
            .as_ref()
            .expect("contract fixture runtime should be available")
    }

    /// Return a clone of the current VFS handle.
    pub fn vfs(&self) -> Arc<StudioVfs> {
        self.vfs
            .as_ref()
            .expect("contract fixture VFS should be open")
            .clone()
    }

    /// Return a clone of the current redb metadata handle.
    pub fn meta(&self) -> Arc<RedbMetaEngine> {
        self.core.meta()
    }

    /// Drop the current VFS and metadata handles, then reopen the same fixture paths.
    pub fn reopen_vfs(&mut self) -> ContractVfsResult<Arc<StudioVfs>> {
        self.close_vfs();
        self.core.reopen_meta()?;
        let vfs = self.open_vfs()?;
        self.vfs = Some(vfs.clone());
        Ok(vfs)
    }

    /// Start VFS background workers for scenarios that need uploader/replay behavior.
    pub fn start(&self) -> ContractVfsResult<()> {
        let _guard = self.runtime().enter();
        self.vfs().start()?;
        Ok(())
    }

    /// Replay portable shared scenario operations into this production-backed
    /// Contract/Core fixture.
    ///
    /// The adapter intentionally stays inside the non-mounted VFS boundary. It
    /// applies namespace and local file-content operations, observes lightweight
    /// read/stat/readdir operations, and explicitly skips checkpoints, mounted
    /// operations, infrastructure outages, and crash/restart markers that need a
    /// heavier E2E layer.
    pub fn replay_operation_records(
        &mut self,
        records: &[ScenarioOperationRecord],
    ) -> ContractVfsResult<ContractReplayReport> {
        ContractReplaySession::new(self).replay(records)
    }

    /// Apply one deterministic VFS operation.
    pub fn apply_op(&self, op: ContractVfsOp) -> ContractVfsResult<ContractVfsOpOutcome> {
        let vfs = self.vfs();
        match op {
            ContractVfsOp::CreateFile { parent, name, mode } => vfs
                .create(parent, &name, 0, 0, mode)
                .map(ContractVfsOpOutcome::Created)
                .map_err(Into::into),
            ContractVfsOp::Mkdir { parent, name, mode } => vfs
                .mkdir(parent, &name, 0, 0, mode)
                .map(ContractVfsOpOutcome::Created)
                .map_err(Into::into),
            ContractVfsOp::Rename {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                vfs.rename(parent, &name, new_parent, &new_name)?;
                Ok(ContractVfsOpOutcome::Completed)
            }
            ContractVfsOp::Unlink { parent, name } => {
                vfs.unlink(parent, &name)?;
                Ok(ContractVfsOpOutcome::Completed)
            }
            ContractVfsOp::Rmdir { parent, name } => {
                vfs.rmdir(parent, &name)?;
                Ok(ContractVfsOpOutcome::Completed)
            }
        }
    }

    /// Collect and assert common metadata/tree/object-graph invariants.
    pub fn assert_invariants(
        &self,
        label: &str,
    ) -> Result<ContractInvariantDiagnostics, ContractInvariantViolation> {
        self.core.assert_invariants(label)
    }

    fn open_vfs(&self) -> ContractVfsResult<Arc<StudioVfs>> {
        let cache_dir = self.cache_dir.to_string_lossy().into_owned();
        let _guard = self.runtime().enter();
        Ok(StudioVfs::new(
            self.core.meta(),
            &self.data_config,
            &cache_dir,
            self.cache_size,
            self.memory_cache_budget_bytes,
            self.config.clone(),
        )?)
    }

    fn close_vfs(&mut self) {
        if let Some(vfs) = self.vfs.take() {
            let Some(runtime) = self.runtime.as_ref() else {
                return;
            };
            let shutdown_timeout = self.shutdown_timeout;
            if tokio::runtime::Handle::try_current().is_ok() {
                let _ = std::thread::scope(|scope| {
                    scope
                        .spawn(move || runtime.block_on(vfs.shutdown(shutdown_timeout)))
                        .join()
                });
            } else {
                runtime.block_on(vfs.shutdown(shutdown_timeout));
            }
        }
    }
}

impl Drop for ContractVfsFixture {
    fn drop(&mut self) {
        let dropping_inside_runtime = tokio::runtime::Handle::try_current().is_ok();
        self.close_vfs();
        if dropping_inside_runtime && let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

struct ContractReplaySession<'a> {
    fixture: &'a mut ContractVfsFixture,
    dirs: BTreeMap<String, u64>,
    files: BTreeMap<String, u64>,
    file_sizes: BTreeMap<String, u64>,
    report: ContractReplayReport,
}

impl<'a> ContractReplaySession<'a> {
    fn new(fixture: &'a mut ContractVfsFixture) -> Self {
        Self {
            fixture,
            dirs: BTreeMap::from([(String::new(), ROOT_INO)]),
            files: BTreeMap::new(),
            file_sizes: BTreeMap::new(),
            report: ContractReplayReport::default(),
        }
    }

    fn replay(
        mut self,
        records: &[ScenarioOperationRecord],
    ) -> ContractVfsResult<ContractReplayReport> {
        for record in records {
            if !self.record_should_apply(record) {
                continue;
            }
            self.apply_record(record)?;
        }
        Ok(self.report)
    }

    fn record_should_apply(&mut self, record: &ScenarioOperationRecord) -> bool {
        match &record.simulation_support {
            SimulationSupport::Supported => {}
            SimulationSupport::Unsupported { reason } => {
                self.skip(record, format!("operation is unsupported: {reason}"));
                return false;
            }
        }

        match &record.outcome {
            OperationOutcome::Accepted | OperationOutcome::Observed => true,
            OperationOutcome::Planned => {
                self.skip(record, "planned operation is not applied");
                false
            }
            OperationOutcome::Rejected { error } => {
                self.skip(
                    record,
                    format!("rejected operation is not applied: {error}"),
                );
                false
            }
        }
    }

    fn apply_record(&mut self, record: &ScenarioOperationRecord) -> ContractVfsResult<()> {
        match &record.operation {
            ScenarioOperation::CreateDirectory { path } => self.create_dir(record, path),
            ScenarioOperation::CreateFile { path, bytes } => {
                self.create_file(record, path, bytes.unwrap_or_default())
            }
            ScenarioOperation::Write {
                path,
                offset,
                bytes,
                ..
            } => self.write_file(record, path, offset.unwrap_or(0), *bytes),
            ScenarioOperation::Append { path, bytes, .. } => self.append_file(record, path, *bytes),
            ScenarioOperation::Truncate { path, size } => self.truncate_file(record, path, *size),
            ScenarioOperation::Rename { from, to } => self.rename(record, from, to),
            ScenarioOperation::Unlink { path } => self.unlink(record, path),
            ScenarioOperation::Rmdir { path } => self.rmdir(record, path),
            ScenarioOperation::Read { path, bytes, .. } => self.read_file(record, path, *bytes),
            ScenarioOperation::Stat { path } => self.stat(record, path),
            ScenarioOperation::Readdir { path } => self.readdir(record, path),
            ScenarioOperation::Fsync { path } => self.fsync(record, path),
            ScenarioOperation::Flush { path } => {
                if let Some(path) = path {
                    self.fsync(record, path)
                } else {
                    self.skip(record, "flush without a path has no Contract/Core handle");
                    Ok(())
                }
            }
            ScenarioOperation::Close { .. } | ScenarioOperation::Release { .. } => {
                self.skip(
                    record,
                    "handle lifecycle operation requires mounted/E2E replay",
                );
                Ok(())
            }
            ScenarioOperation::ClientRestart | ScenarioOperation::ClientCrash => {
                self.skip(record, "client crash/restart requires mounted/E2E replay");
                Ok(())
            }
            ScenarioOperation::PostgresOutage
            | ScenarioOperation::PostgresRecovery
            | ScenarioOperation::ObjectStoreOutage
            | ScenarioOperation::ObjectStoreRecovery => {
                self.skip(record, "infrastructure fault requires E2E replay");
                Ok(())
            }
            ScenarioOperation::Wait { .. }
            | ScenarioOperation::Barrier { .. }
            | ScenarioOperation::SchedulerCheckpoint { .. } => {
                self.skip(record, "checkpoint/barrier is not a Contract/Core mutation");
                Ok(())
            }
            ScenarioOperation::MountedOnly { .. } => {
                self.skip(record, "mounted-only operation requires E2E replay");
                Ok(())
            }
        }
    }

    fn create_dir(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
    ) -> ContractVfsResult<()> {
        let (parent, name) = self.parent_and_name(path)?;
        let parent_ino = self.dir_ino(&parent)?;
        let attr = self
            .fixture
            .apply_op(ContractVfsOp::Mkdir {
                parent: parent_ino,
                name,
                mode: 0o755,
            })?
            .created_attr()
            .ok_or_else(|| replay_error("mkdir did not return created attributes"))?;
        self.dirs.insert(path.to_string(), attr.ino);
        self.applied(record, path);
        Ok(())
    }

    fn create_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        bytes: u64,
    ) -> ContractVfsResult<()> {
        let (parent, name) = self.parent_and_name(path)?;
        let parent_ino = self.dir_ino(&parent)?;
        let attr = self
            .fixture
            .apply_op(ContractVfsOp::CreateFile {
                parent: parent_ino,
                name,
                mode: 0o644,
            })?
            .created_attr()
            .ok_or_else(|| replay_error("create file did not return created attributes"))?;
        self.files.insert(path.to_string(), attr.ino);
        self.file_sizes.insert(path.to_string(), 0);
        if bytes > 0 {
            self.write_file(record, path, 0, bytes)?;
        } else {
            self.applied(record, path);
        }
        Ok(())
    }

    fn write_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        offset: u64,
        bytes: u64,
    ) -> ContractVfsResult<()> {
        let ino = self.file_ino(path)?;
        let write_len = usize::try_from(bytes)
            .map_err(|_| replay_error(format!("write length {bytes} does not fit usize")))?;
        let payload = deterministic_payload(record.sequence, write_len);
        let vfs = self.fixture.vfs();
        let fh = vfs.open_file(ino, OpenFlags::RDWR, false)?;
        let write_result = self
            .fixture
            .runtime()
            .block_on(vfs.write_fh(fh, ino, offset, &payload));
        let release_result = self.fixture.runtime().block_on(vfs.release_fh(fh));
        write_result?;
        release_result?;
        let new_size = offset.saturating_add(bytes);
        self.file_sizes
            .entry(path.to_string())
            .and_modify(|size| *size = (*size).max(new_size))
            .or_insert(new_size);
        self.applied(record, path);
        Ok(())
    }

    fn append_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        bytes: u64,
    ) -> ContractVfsResult<()> {
        let offset = self.file_sizes.get(path).copied().unwrap_or_default();
        self.write_file(record, path, offset, bytes)
    }

    fn truncate_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        size: u64,
    ) -> ContractVfsResult<()> {
        let ino = self.file_ino(path)?;
        self.fixture
            .vfs()
            .setattr(ino, SetattrArgs::size_only(size))?;
        self.file_sizes.insert(path.to_string(), size);
        self.applied(record, path);
        Ok(())
    }

    fn rename(
        &mut self,
        record: &ScenarioOperationRecord,
        from: &str,
        to: &str,
    ) -> ContractVfsResult<()> {
        let (parent, name) = self.parent_and_name(from)?;
        let (new_parent, new_name) = self.parent_and_name(to)?;
        let parent_ino = self.dir_ino(&parent)?;
        let new_parent_ino = self.dir_ino(&new_parent)?;
        self.fixture.apply_op(ContractVfsOp::Rename {
            parent: parent_ino,
            name,
            new_parent: new_parent_ino,
            new_name,
        })?;
        self.rename_path_state(from, to);
        self.applied(record, to);
        Ok(())
    }

    fn unlink(&mut self, record: &ScenarioOperationRecord, path: &str) -> ContractVfsResult<()> {
        let (parent, name) = self.parent_and_name(path)?;
        let parent_ino = self.dir_ino(&parent)?;
        self.fixture.apply_op(ContractVfsOp::Unlink {
            parent: parent_ino,
            name,
        })?;
        self.files.remove(path);
        self.file_sizes.remove(path);
        self.applied(record, path);
        Ok(())
    }

    fn rmdir(&mut self, record: &ScenarioOperationRecord, path: &str) -> ContractVfsResult<()> {
        let (parent, name) = self.parent_and_name(path)?;
        let parent_ino = self.dir_ino(&parent)?;
        self.fixture.apply_op(ContractVfsOp::Rmdir {
            parent: parent_ino,
            name,
        })?;
        self.dirs.remove(path);
        self.applied(record, path);
        Ok(())
    }

    fn read_file(
        &mut self,
        record: &ScenarioOperationRecord,
        path: &str,
        bytes: Option<u64>,
    ) -> ContractVfsResult<()> {
        let ino = self.file_ino(path)?;
        let read_len = bytes
            .or_else(|| self.file_sizes.get(path).copied())
            .unwrap_or_default();
        let read_len = u32::try_from(read_len)
            .map_err(|_| replay_error(format!("read length for {path} does not fit u32")))?;
        self.fixture
            .runtime()
            .block_on(self.fixture.vfs().read(ino, 0, read_len))?;
        self.applied(record, path);
        Ok(())
    }

    fn stat(&mut self, record: &ScenarioOperationRecord, path: &str) -> ContractVfsResult<()> {
        let ino = self.entry_ino(path)?;
        self.fixture.vfs().getattr(ino)?;
        self.applied(record, path);
        Ok(())
    }

    fn readdir(&mut self, record: &ScenarioOperationRecord, path: &str) -> ContractVfsResult<()> {
        let ino = self.dir_ino(path)?;
        self.fixture.vfs().readdir(ino)?;
        self.applied(record, path);
        Ok(())
    }

    fn fsync(&mut self, record: &ScenarioOperationRecord, path: &str) -> ContractVfsResult<()> {
        let ino = self.file_ino(path)?;
        self.fixture
            .runtime()
            .block_on(self.fixture.vfs().fsync(ino, false))?;
        self.applied(record, path);
        Ok(())
    }

    fn rename_path_state(&mut self, from: &str, to: &str) {
        if let Some(ino) = self.files.remove(from) {
            self.files.insert(to.to_string(), ino);
            if let Some(size) = self.file_sizes.remove(from) {
                self.file_sizes.insert(to.to_string(), size);
            }
            return;
        }
        if let Some(ino) = self.dirs.remove(from) {
            self.dirs.insert(to.to_string(), ino);
            let from_prefix = format!("{from}/");
            let to_prefix = format!("{to}/");
            let dir_paths = self
                .dirs
                .keys()
                .filter(|path| path.starts_with(&from_prefix))
                .cloned()
                .collect::<Vec<_>>();
            for old_path in dir_paths {
                if let Some(child_ino) = self.dirs.remove(&old_path) {
                    let suffix = &old_path[from_prefix.len()..];
                    self.dirs.insert(format!("{to_prefix}{suffix}"), child_ino);
                }
            }
            let file_paths = self
                .files
                .keys()
                .filter(|path| path.starts_with(&from_prefix))
                .cloned()
                .collect::<Vec<_>>();
            for old_path in file_paths {
                if let Some(child_ino) = self.files.remove(&old_path) {
                    let suffix = &old_path[from_prefix.len()..];
                    let new_path = format!("{to_prefix}{suffix}");
                    self.files.insert(new_path.clone(), child_ino);
                    if let Some(size) = self.file_sizes.remove(&old_path) {
                        self.file_sizes.insert(new_path, size);
                    }
                }
            }
        }
    }

    fn parent_and_name(&self, path: &str) -> ContractVfsResult<(String, Vec<u8>)> {
        if path.is_empty() || path.starts_with('/') || path.contains('\\') {
            return Err(replay_error(format!("invalid replay path {path:?}")));
        }
        if path
            .split('/')
            .any(|component| component.is_empty() || component == "..")
        {
            return Err(replay_error(format!("invalid replay path {path:?}")));
        }
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        Ok((parent.to_string(), name.as_bytes().to_vec()))
    }

    fn entry_ino(&self, path: &str) -> ContractVfsResult<u64> {
        self.files
            .get(path)
            .or_else(|| self.dirs.get(path))
            .copied()
            .ok_or_else(|| replay_error(format!("path {path:?} is not known in replay state")))
    }

    fn file_ino(&self, path: &str) -> ContractVfsResult<u64> {
        self.files
            .get(path)
            .copied()
            .ok_or_else(|| replay_error(format!("file path {path:?} is not known in replay state")))
    }

    fn dir_ino(&self, path: &str) -> ContractVfsResult<u64> {
        self.dirs.get(path).copied().ok_or_else(|| {
            replay_error(format!(
                "directory path {path:?} is not known in replay state"
            ))
        })
    }

    fn applied(&mut self, record: &ScenarioOperationRecord, path: &str) {
        self.report.applied.push(ContractReplayAppliedRecord {
            sequence: record.sequence,
            operation: operation_name(&record.operation).to_string(),
            path: Some(path.to_string()),
        });
    }

    fn skip(&mut self, record: &ScenarioOperationRecord, reason: impl Into<String>) {
        self.report.skipped.push(ContractReplaySkippedRecord {
            sequence: record.sequence,
            operation: operation_name(&record.operation).to_string(),
            reason: reason.into(),
        });
    }
}

fn replay_error(message: impl Into<String>) -> ContractVfsHarnessError {
    ContractVfsHarnessError::Replay(message.into())
}

fn deterministic_payload(sequence: u64, len: usize) -> Vec<u8> {
    let byte = (sequence as u8).wrapping_mul(31).wrapping_add(17);
    vec![byte; len]
}

fn operation_name(operation: &ScenarioOperation) -> &'static str {
    match operation {
        ScenarioOperation::CreateFile { .. } => "create_file",
        ScenarioOperation::CreateDirectory { .. } => "create_directory",
        ScenarioOperation::Write { .. } => "write",
        ScenarioOperation::Append { .. } => "append",
        ScenarioOperation::Truncate { .. } => "truncate",
        ScenarioOperation::Rename { .. } => "rename",
        ScenarioOperation::Unlink { .. } => "unlink",
        ScenarioOperation::Rmdir { .. } => "rmdir",
        ScenarioOperation::Read { .. } => "read",
        ScenarioOperation::Stat { .. } => "stat",
        ScenarioOperation::Readdir { .. } => "readdir",
        ScenarioOperation::Flush { .. } => "flush",
        ScenarioOperation::Fsync { .. } => "fsync",
        ScenarioOperation::Close { .. } => "close",
        ScenarioOperation::Release { .. } => "release",
        ScenarioOperation::ClientRestart => "client_restart",
        ScenarioOperation::ClientCrash => "client_crash",
        ScenarioOperation::PostgresOutage => "postgres_outage",
        ScenarioOperation::PostgresRecovery => "postgres_recovery",
        ScenarioOperation::ObjectStoreOutage => "object_store_outage",
        ScenarioOperation::ObjectStoreRecovery => "object_store_recovery",
        ScenarioOperation::Wait { .. } => "wait",
        ScenarioOperation::Barrier { .. } => "barrier",
        ScenarioOperation::SchedulerCheckpoint { .. } => "scheduler_checkpoint",
        ScenarioOperation::MountedOnly { .. } => "mounted_only",
    }
}

/// Root inode constant re-exported for contract harness callers.
pub const CONTRACT_ROOT_INO: u64 = ROOT_INO;
