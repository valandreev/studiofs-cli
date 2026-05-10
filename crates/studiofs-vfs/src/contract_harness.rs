//! Contract/Core VFS fixtures for fast local tests.

use crate::{StudioVfs, VfsConfigRaw, VfsConfigValidated, VfsError};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use studiofs_data::store::DataStoreConfig;
use studiofs_meta::contract_harness::{
    ContractCoreFixture, ContractInvariantDiagnostics, ContractInvariantViolation,
};
use studiofs_meta::schema::{InodeAttr, ROOT_INO};
use studiofs_meta::{MetaError, RedbMetaEngine};
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

/// Root inode constant re-exported for contract harness callers.
pub const CONTRACT_ROOT_INO: u64 = ROOT_INO;
