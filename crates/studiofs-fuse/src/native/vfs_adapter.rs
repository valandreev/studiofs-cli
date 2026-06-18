//! Native Linux FUSE operation adapter backed by StudioFS VFS contracts.

use std::borrow::Cow;
use std::collections::HashMap;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use smallvec::SmallVec;
use studiofs_bridge::posix::{posix_flags_to_open_flags, vfs_err_to_errno};
use studiofs_meta::{DirEntry, FileType, InodeAttr};
use studiofs_vfs::{
    LocalSyncPolicy, OpenFlags, SetattrArgs, StatFs, StudioVfs, VfsFuseAsyncWriteSnapshot,
    VfsNativeBridgeSnapshot, VfsNativeLatencySnapshot,
};
use tokio::runtime::Handle;
use tracing::warn;

use crate::StudioFuseRuntimeOptions;
use crate::fs::pending_write_tracker::{
    PendingWriteAdmissionLimits, PendingWriteCompletion, PendingWriteTracker, PendingWriteTurn,
    WriteRegistrationError,
};
use crate::native::abi::{error_reply, ok_reply};
use crate::native::cache_policy::{
    NativeInodeCacheState, NativeOpenCacheAction, NativeOpenCacheDecision, NativeOpenCacheMetrics,
    NativeOpenCacheMetricsSnapshot, NativeOpenCacheReason, NativeReadAheadDecision,
    NativeReadAheadPolicy, NativeReadAheadState, decide_native_open_cache,
    decide_native_read_ahead,
};
use crate::native::dispatcher::{FuseOperationHandler, OperationBarrierGates, ResourceKey};
use crate::native::protocol::{FuseLocalCacheReadRange, FuseOpcode, FuseReply, FuseRequest};

const FUSE_ENTRY_OUT_PAYLOAD_LEN: usize = 128;
const FUSE_ATTR_PAYLOAD_LEN: usize = 88;
const FUSE_BATCH_FORGET_IN_LEN: usize = 8;
const FUSE_BATCH_FORGET_ONE_LEN: usize = 16;
const FUSE_FLUSH_IN_LEN: usize = 24;
const FUSE_FSYNC_IN_LEN: usize = 16;
const FUSE_FORGET_IN_LEN: usize = 8;
const FUSE_CREATE_IN_LEN: usize = 16;
const FUSE_MKDIR_IN_LEN: usize = 8;
const FUSE_OPEN_IN_LEN: usize = 8;
const FUSE_READ_IN_LEN: usize = 40;
const FUSE_RENAME_IN_LEN: usize = 8;
const FUSE_RELEASE_IN_LEN: usize = 24;
const FUSE_SETATTR_IN_LEN: usize = 88;
const FUSE_WRITE_IN_LEN: usize = 40;
const NATIVE_MAX_READ_SIZE_BYTES: usize = 16 * 1024 * 1024;
const POSIX_STAT_BLOCK_SIZE: u64 = 512;
const FATTR_FH: u32 = 1 << 6;
const FATTR_SIZE: u32 = 1 << 3;
const FATTR_MTIME: u32 = 1 << 5;
const FATTR_MTIME_NOW: u32 = 1 << 8;
const FATTR_LOCKOWNER: u32 = 1 << 9;
const FATTR_CTIME: u32 = 1 << 10;
const SUPPORTED_SETATTR_VALID: u32 =
    FATTR_FH | FATTR_SIZE | FATTR_MTIME | FATTR_MTIME_NOW | FATTR_LOCKOWNER | FATTR_CTIME;
const FOPEN_DIRECT_IO: u32 = 1 << 0;
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

/// Stable local cache file range that a native Linux read transport may use.
pub type NativeLocalCacheReadRange = FuseLocalCacheReadRange;

/// File kind used by the native operation adapter before Linux ABI encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeFileType {
    /// Regular file.
    RegularFile,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

/// Minimal native attribute shape needed to build Linux FUSE replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeAttr {
    /// StudioFS inode number.
    pub ino: u64,
    /// File size in bytes.
    pub size: u64,
    /// File kind.
    pub kind: NativeFileType,
    /// POSIX permission bits.
    pub perm: u16,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// Access time in nanoseconds since Unix epoch.
    pub atime_ns: u64,
    /// Modification time in nanoseconds since Unix epoch.
    pub mtime_ns: u64,
    /// Change time in nanoseconds since Unix epoch.
    pub ctime_ns: u64,
}

/// Directory entry used by the native bridge before FUSE dirent encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeDirEntry {
    /// Child inode number.
    pub ino: u64,
    /// Child file kind.
    pub kind: NativeFileType,
    /// Child name bytes.
    pub name: Vec<u8>,
}

/// Filesystem statistics used by native FUSE `STATFS` replies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeStatFs {
    /// Total number of blocks.
    pub blocks: u64,
    /// Free blocks.
    pub bfree: u64,
    /// Blocks available to unprivileged callers.
    pub bavail: u64,
    /// Total inode count.
    pub files: u64,
    /// Free inode count.
    pub ffree: u64,
    /// Preferred block size.
    pub bsize: u32,
    /// Maximum filename length.
    pub namelen: u32,
    /// Fundamental filesystem block size.
    pub frsize: u32,
}

/// Async write serialization turn for native write executors.
pub struct NativeAsyncWriteTurn {
    inner: PendingWriteTurn,
}

impl NativeAsyncWriteTurn {
    fn new(inner: PendingWriteTurn) -> Self {
        Self { inner }
    }

    /// Wait synchronously until this write may enter the backend.
    pub fn wait_blocking(self) {
        self.inner.wait_blocking();
    }

    /// Wait asynchronously until this write may enter the backend.
    pub async fn wait(self) {
        self.inner.wait().await;
    }
}

/// VFS operations required by the native Linux bridge operation layer.
pub trait NativeVfs {
    /// Resolve `name` under `parent`.
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32>;

    /// Fetch attributes for `ino`.
    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32>;

    /// Open a regular file and return a VFS file handle.
    fn open(&self, ino: u64, flags: i32) -> Result<u64, i32>;

    /// Return page-cache policy state for a regular-file open when available.
    fn open_cache_state(&self, _ino: u64) -> Option<NativeInodeCacheState> {
        None
    }

    /// Release a previously opened file handle.
    fn release(&self, fh: u64) -> Result<(), i32>;

    /// Read bytes from an open file handle.
    fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32>;

    /// Return a stable local cache file range for an optimized read reply, if available.
    fn read_local_cache_range(
        &self,
        _fh: u64,
        _ino: u64,
        _offset: u64,
        _size: u32,
    ) -> Result<Option<NativeLocalCacheReadRange>, i32> {
        Ok(None)
    }

    /// Hint that a local-cache-backed range will likely be read soon.
    fn advise_read_ahead(&self, _fh: u64, _ino: u64, _offset: u64, _len: u64) -> Result<(), i32> {
        Ok(())
    }

    /// Write bytes to an open file handle.
    fn write(&self, fh: u64, ino: u64, offset: u64, data: &[u8], flags: i32) -> Result<u32, i32>;

    /// Record native async write admission and barrier telemetry.
    fn record_async_write_snapshot(&self, _snapshot: NativeAsyncWriteSnapshot) {}

    /// Record native Linux bridge amplification and sparse-read telemetry.
    fn record_native_bridge_snapshot(
        &self,
        _amplification: NativeAmplificationMetricsSnapshot,
        _read: NativeReadMetricsSnapshot,
    ) {
    }

    /// Run a native async write job without blocking the FUSE callback thread.
    fn spawn_native_async_write_job<J>(&self, turn: NativeAsyncWriteTurn, job: J)
    where
        J: FnOnce() + Send + 'static,
    {
        std::thread::spawn(move || {
            turn.wait_blocking();
            job();
        });
    }

    /// Flush dirty state for a file handle or inode.
    fn flush(&self, ino: u64, fh: u64) -> Result<(), i32>;

    /// Synchronize dirty state for an inode.
    fn fsync(&self, ino: u64, datasync: bool) -> Result<(), i32>;

    /// Open a directory stream and return a directory handle.
    fn opendir(&self, ino: u64) -> Result<u64, i32>;

    /// Release a previously opened directory handle.
    fn releasedir(&self, ino: u64, fh: u64) -> Result<(), i32>;

    /// Read directory entries for an opened directory handle.
    fn readdir(&self, ino: u64, fh: u64, offset: i64) -> Result<Arc<[NativeDirEntry]>, i32>;

    /// Create a directory.
    fn mkdir(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<NativeAttr, i32>;

    /// Create and open a regular file.
    fn create(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        flags: i32,
    ) -> Result<(NativeAttr, u64), i32>;

    /// Remove a regular-file name.
    fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32>;

    /// Remove an empty directory name.
    fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32>;

    /// Rename a namespace entry.
    fn rename(&self, parent: u64, name: &[u8], new_parent: u64, new_name: &[u8])
    -> Result<(), i32>;

    /// Truncate or extend an inode, optionally through an open file handle.
    fn truncate(&self, ino: u64, fh: Option<u64>, size: u64) -> Result<NativeAttr, i32>;

    /// Return filesystem statistics.
    fn statfs(&self) -> Result<NativeStatFs, i32>;
}

impl<T: NativeVfs + ?Sized> NativeVfs for Arc<T> {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
        (**self).lookup(parent, name)
    }

    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
        (**self).getattr(ino)
    }

    fn open(&self, ino: u64, flags: i32) -> Result<u64, i32> {
        (**self).open(ino, flags)
    }

    fn open_cache_state(&self, ino: u64) -> Option<NativeInodeCacheState> {
        (**self).open_cache_state(ino)
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        (**self).release(fh)
    }

    fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32> {
        (**self).read(fh, ino, offset, size)
    }

    fn read_local_cache_range(
        &self,
        fh: u64,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> Result<Option<NativeLocalCacheReadRange>, i32> {
        (**self).read_local_cache_range(fh, ino, offset, size)
    }

    fn advise_read_ahead(&self, fh: u64, ino: u64, offset: u64, len: u64) -> Result<(), i32> {
        (**self).advise_read_ahead(fh, ino, offset, len)
    }

    fn write(&self, fh: u64, ino: u64, offset: u64, data: &[u8], flags: i32) -> Result<u32, i32> {
        (**self).write(fh, ino, offset, data, flags)
    }

    fn record_async_write_snapshot(&self, snapshot: NativeAsyncWriteSnapshot) {
        (**self).record_async_write_snapshot(snapshot);
    }

    fn record_native_bridge_snapshot(
        &self,
        amplification: NativeAmplificationMetricsSnapshot,
        read: NativeReadMetricsSnapshot,
    ) {
        (**self).record_native_bridge_snapshot(amplification, read);
    }

    fn spawn_native_async_write_job<J>(&self, turn: NativeAsyncWriteTurn, job: J)
    where
        J: FnOnce() + Send + 'static,
    {
        (**self).spawn_native_async_write_job(turn, job);
    }

    fn flush(&self, ino: u64, fh: u64) -> Result<(), i32> {
        (**self).flush(ino, fh)
    }

    fn fsync(&self, ino: u64, datasync: bool) -> Result<(), i32> {
        (**self).fsync(ino, datasync)
    }

    fn opendir(&self, ino: u64) -> Result<u64, i32> {
        (**self).opendir(ino)
    }

    fn releasedir(&self, ino: u64, fh: u64) -> Result<(), i32> {
        (**self).releasedir(ino, fh)
    }

    fn readdir(&self, ino: u64, fh: u64, offset: i64) -> Result<Arc<[NativeDirEntry]>, i32> {
        (**self).readdir(ino, fh, offset)
    }

    fn mkdir(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<NativeAttr, i32> {
        (**self).mkdir(parent, name, uid, gid, mode)
    }

    fn create(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        flags: i32,
    ) -> Result<(NativeAttr, u64), i32> {
        (**self).create(parent, name, uid, gid, mode, flags)
    }

    fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        (**self).unlink(parent, name)
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        (**self).rmdir(parent, name)
    }

    fn rename(
        &self,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
    ) -> Result<(), i32> {
        (**self).rename(parent, name, new_parent, new_name)
    }

    fn truncate(&self, ino: u64, fh: Option<u64>, size: u64) -> Result<NativeAttr, i32> {
        (**self).truncate(ino, fh, size)
    }

    fn statfs(&self) -> Result<NativeStatFs, i32> {
        (**self).statfs()
    }
}

/// Native operation backend for a real [`StudioVfs`] plus Tokio runtime.
#[derive(Clone)]
pub struct StudioVfsNativeAdapter {
    vfs: Arc<StudioVfs>,
    rt: Handle,
    directory_handles: Arc<NativeDirectoryHandleStore>,
}

impl StudioVfsNativeAdapter {
    /// Build a native VFS backend from shared VFS state and runtime handle.
    pub fn new(vfs: Arc<StudioVfs>, rt: Handle) -> Self {
        Self {
            vfs,
            rt,
            directory_handles: Arc::new(NativeDirectoryHandleStore::default()),
        }
    }
}

impl NativeVfs for StudioVfsNativeAdapter {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
        self.vfs
            .lookup(parent, name)
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
        self.vfs
            .getattr(ino)
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn open(&self, ino: u64, flags: i32) -> Result<u64, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        self.vfs
            .open_file(ino, open_flags, trunc)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn open_cache_state(&self, ino: u64) -> Option<NativeInodeCacheState> {
        let attr = match self.vfs.getattr(ino) {
            Ok(attr) => attr,
            Err(error) => {
                warn!(
                    ino,
                    error = ?error,
                    "native open cache state unavailable after getattr failure"
                );
                return None;
            }
        };
        Some(native_open_cache_state_from_attr(
            attr,
            self.vfs.inode_has_local_dirty_state(ino),
        ))
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        self.rt
            .block_on(self.vfs.release_fh(fh))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32> {
        let size = checked_native_read_size(size)?;
        // StudioVfs currently reads into initialized slices; avoid unsafe uninit buffers here.
        let mut out = vec![0; size];
        let read = self
            .rt
            .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut out))
            .map_err(|error| vfs_err_to_errno(&error))?;
        let read = usize::try_from(read).map_err(|_| libc::EOVERFLOW)?;
        out.truncate(read);
        Ok(out)
    }

    #[cfg(unix)]
    fn read_local_cache_range(
        &self,
        _fh: u64,
        ino: u64,
        offset: u64,
        size: u32,
    ) -> Result<Option<NativeLocalCacheReadRange>, i32> {
        checked_native_read_size(size)?;
        let Some(range) = self
            .rt
            .block_on(self.vfs.local_cache_read_range(ino, offset, size))
            .map_err(|error| vfs_err_to_errno(&error))?
        else {
            return Ok(None);
        };
        let len = u32::try_from(range.payload.len()).map_err(|_| libc::EOVERFLOW)?;
        NativeLocalCacheReadRange::try_dup_with_generation(
            range.payload.file().as_raw_fd(),
            range.payload.file_offset(),
            len,
            range.content_epoch,
            range.invalidation_generation,
        )
        .map(Some)
        .map_err(|error| {
            warn!(
                ino,
                offset,
                size,
                error = ?error,
                "native local cache read range fd duplication failed"
            );
            libc::EIO
        })
    }

    fn advise_read_ahead(&self, _fh: u64, ino: u64, offset: u64, len: u64) -> Result<(), i32> {
        self.vfs
            .enqueue_native_read_ahead(ino, offset, len)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn write(&self, fh: u64, ino: u64, offset: u64, data: &[u8], flags: i32) -> Result<u32, i32> {
        self.rt
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                offset,
                data,
                write_local_sync_policy(flags),
            ))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn record_async_write_snapshot(&self, snapshot: NativeAsyncWriteSnapshot) {
        self.vfs
            .record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
                snapshot_generation: snapshot.snapshot_generation,
                enabled: snapshot.enabled,
                platform_policy: snapshot.platform_policy.clone(),
                max_pending_bytes: snapshot.max_pending_bytes,
                max_pending_writes: snapshot.max_pending_writes,
                per_handle_pending_bytes: snapshot.per_handle_pending_bytes,
                pending_writes: snapshot.pending_writes,
                pending_bytes: snapshot.pending_bytes,
                peak_pending_bytes: snapshot.peak_pending_bytes,
                completed_writes: snapshot.completed_writes,
                failed_writes: snapshot.failed_writes,
                task_failed_writes: snapshot.task_failed_writes,
                admission_waits: snapshot.admission_waits,
                admission_wait_total_ms: snapshot.admission_wait_total_ms,
                barrier_waits: snapshot.barrier_waits,
                barrier_wait_total_ms: snapshot.barrier_wait_total_ms,
                ..Default::default()
            });
    }

    fn record_native_bridge_snapshot(
        &self,
        amplification: NativeAmplificationMetricsSnapshot,
        read: NativeReadMetricsSnapshot,
    ) {
        self.vfs
            .record_native_bridge_snapshot(vfs_native_bridge_snapshot(amplification, read));
    }

    fn spawn_native_async_write_job<J>(&self, turn: NativeAsyncWriteTurn, job: J)
    where
        J: FnOnce() + Send + 'static,
    {
        let rt = self.rt.clone();
        drop(self.rt.spawn(async move {
            turn.wait().await;
            let _ = rt.spawn_blocking(job).await;
        }));
    }

    fn flush(&self, ino: u64, fh: u64) -> Result<(), i32> {
        let result = if fh != 0 {
            self.rt.block_on(self.vfs.flush_fh(fh))
        } else {
            self.rt.block_on(self.vfs.flush(ino))
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    fn fsync(&self, ino: u64, datasync: bool) -> Result<(), i32> {
        self.rt
            .block_on(self.vfs.fsync(ino, datasync))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn opendir(&self, ino: u64) -> Result<u64, i32> {
        let entries = self
            .vfs
            .readdir(ino)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(native_dir_entry)
                    .collect::<Vec<_>>()
                    .into()
            })
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok(self.directory_handles.open(ino, entries))
    }

    fn releasedir(&self, ino: u64, fh: u64) -> Result<(), i32> {
        self.directory_handles.release(ino, fh);
        Ok(())
    }

    fn readdir(&self, ino: u64, fh: u64, _offset: i64) -> Result<Arc<[NativeDirEntry]>, i32> {
        self.directory_handles.entries(ino, fh).ok_or(libc::EINVAL)
    }

    fn mkdir(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
    ) -> Result<NativeAttr, i32> {
        self.vfs
            .mkdir(parent, name, uid, gid, mode)
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn create(
        &self,
        parent: u64,
        name: &[u8],
        uid: u32,
        gid: u32,
        mode: u16,
        flags: i32,
    ) -> Result<(NativeAttr, u64), i32> {
        let attr = self
            .vfs
            .create(parent, name, uid, gid, mode)
            .map_err(|error| vfs_err_to_errno(&error))?;
        let (open_flags, _) = posix_flags_to_open_flags(flags);
        // StudioVfs has no atomic create+open primitive yet; an open failure leaves the file.
        let fh = self
            .vfs
            .open_file(attr.ino, open_flags, false)
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok((native_attr_from_inode_attr(attr), fh))
    }

    fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        self.vfs
            .unlink(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        self.vfs
            .rmdir(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rename(
        &self,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
    ) -> Result<(), i32> {
        self.vfs
            .rename(parent, name, new_parent, new_name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn truncate(&self, ino: u64, fh: Option<u64>, size: u64) -> Result<NativeAttr, i32> {
        self.vfs
            .setattr_with_handle(fh, ino, SetattrArgs::size_only(size))
            .map(native_attr_from_inode_attr)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn statfs(&self) -> Result<NativeStatFs, i32> {
        self.vfs
            .statfs()
            .map(native_statfs)
            .map_err(|error| vfs_err_to_errno(&error))
    }
}

/// Transport-independent Linux native bridge operation handler.
#[derive(Debug)]
pub struct LinuxNativeBridge<V> {
    vfs: V,
    barrier_gates: OperationBarrierGates,
    async_write: Option<NativeAsyncWriteConfig>,
    pending_writes: PendingWriteTracker,
    lookup_counts: NativeLookupCountStore,
    open_cache_metrics: NativeOpenCacheMetrics,
    amplification_metrics: NativeAmplificationMetrics,
    read_metrics: NativeReadMetrics,
    native_bridge_snapshot_started_at: Instant,
    native_bridge_snapshot_last_elapsed_ms: AtomicU64,
    trusted_open_cache_states: NativeTrustedOpenCacheStateStore,
    read_ahead_policy: NativeReadAheadPolicy,
    read_ahead_states: NativeReadAheadStateStore,
}

/// Native Linux async write admission and telemetry profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAsyncWriteConfig {
    /// Maximum bridge-owned pending write bytes.
    pub max_pending_bytes: Option<usize>,
    /// Maximum bridge-owned pending write requests.
    pub max_pending_writes: Option<usize>,
    /// Maximum bridge-owned pending bytes for one file handle.
    pub per_handle_pending_bytes: Option<usize>,
    /// Human-readable profile name surfaced in diagnostics.
    pub platform_policy: Cow<'static, str>,
}

impl NativeAsyncWriteConfig {
    /// Build native async write config from public FUSE runtime options.
    pub fn from_runtime_options(options: &StudioFuseRuntimeOptions) -> Option<Self> {
        if !options.async_write_enabled {
            return None;
        }
        let limits = options
            .async_write_admission_limits
            .map(Into::into)
            .unwrap_or_else(native_async_write_default_limits);
        Some(Self {
            max_pending_bytes: limits.max_pending_bytes,
            max_pending_writes: limits.max_pending_writes,
            per_handle_pending_bytes: limits.per_handle_pending_bytes,
            platform_policy: "linux6-safe".into(),
        })
    }

    fn admission_limits(&self) -> PendingWriteAdmissionLimits {
        PendingWriteAdmissionLimits {
            max_pending_bytes: self.max_pending_bytes,
            max_pending_writes: self.max_pending_writes,
            per_handle_pending_bytes: self.per_handle_pending_bytes,
        }
    }
}

/// Point-in-time native async write admission and barrier telemetry.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeAsyncWriteSnapshot {
    /// Tracker mutation generation for stale-snapshot rejection.
    pub snapshot_generation: u64,
    /// Whether async native writes are enabled.
    pub enabled: bool,
    /// Human-readable profile name.
    pub platform_policy: Cow<'static, str>,
    /// Maximum bridge-owned pending write bytes.
    pub max_pending_bytes: Option<usize>,
    /// Maximum bridge-owned pending write requests.
    pub max_pending_writes: Option<usize>,
    /// Maximum bridge-owned pending bytes for one file handle.
    pub per_handle_pending_bytes: Option<usize>,
    /// Currently pending native write requests.
    pub pending_writes: usize,
    /// Currently pending bridge-owned native write bytes.
    pub pending_bytes: usize,
    /// Peak pending bridge-owned native write bytes.
    pub peak_pending_bytes: usize,
    /// Completed async native writes.
    pub completed_writes: u64,
    /// Async native writes completed with VFS errors.
    pub failed_writes: u64,
    /// Async native write tasks dropped without explicit completion.
    pub task_failed_writes: u64,
    /// Number of admission waits recorded by the tracker.
    pub admission_waits: u64,
    /// Total admission wait time in milliseconds.
    pub admission_wait_total_ms: f64,
    /// Number of barrier waits recorded by the tracker.
    pub barrier_waits: u64,
    /// Total barrier wait time in milliseconds.
    pub barrier_wait_total_ms: f64,
}

impl Default for NativeAsyncWriteSnapshot {
    fn default() -> Self {
        Self {
            snapshot_generation: 0,
            enabled: false,
            platform_policy: Cow::Borrowed(""),
            max_pending_bytes: None,
            max_pending_writes: None,
            per_handle_pending_bytes: None,
            pending_writes: 0,
            pending_bytes: 0,
            peak_pending_bytes: 0,
            completed_writes: 0,
            failed_writes: 0,
            task_failed_writes: 0,
            admission_waits: 0,
            admission_wait_total_ms: 0.0,
            barrier_waits: 0,
            barrier_wait_total_ms: 0.0,
        }
    }
}

#[derive(Debug, Default)]
struct NativeReadMetrics {
    local_cache_hits: AtomicU64,
    local_cache_unavailable_fallbacks: AtomicU64,
    stale_epoch_fallbacks: AtomicU64,
    stale_generation_fallbacks: AtomicU64,
    bytes_fallbacks: AtomicU64,
    bytes_returned: AtomicU64,
    read_ahead_prefetches: AtomicU64,
    read_ahead_suppressed: AtomicU64,
    read_ahead_amplification_bytes: AtomicU64,
}

#[repr(align(64))]
#[derive(Debug, Default)]
struct NativeAmplificationMetrics {
    lookup_ops: AtomicU64,
    getattr_ops: AtomicU64,
    readdir_ops: AtomicU64,
    readdirplus_ops: AtomicU64,
    open_ops: AtomicU64,
    release_ops: AtomicU64,
    read_ops: AtomicU64,
    write_ops: AtomicU64,
    lookup_latency: NativeLatencyMetrics,
    getattr_latency: NativeLatencyMetrics,
    readdir_latency: NativeLatencyMetrics,
    readdirplus_latency: NativeLatencyMetrics,
    open_latency: NativeLatencyMetrics,
    release_latency: NativeLatencyMetrics,
    read_latency: NativeLatencyMetrics,
    write_latency: NativeLatencyMetrics,
}

const NATIVE_BRIDGE_SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const NATIVE_BRIDGE_SNAPSHOT_NEVER_PUBLISHED: u64 = u64::MAX;
const NATIVE_LATENCY_BUCKET_BOUNDS_NS: [u64; 18] = [
    1_000,
    2_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_000_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    u64::MAX,
];

#[repr(align(64))]
#[derive(Debug)]
struct NativeLatencyMetrics {
    samples: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
    buckets: [AtomicU64; NATIVE_LATENCY_BUCKET_BOUNDS_NS.len()],
}

impl Default for NativeLatencyMetrics {
    fn default() -> Self {
        Self {
            samples: AtomicU64::new(0),
            total_ns: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

#[derive(Debug)]
struct NativeReadAheadStateStore {
    shards: [Mutex<NativeReadAheadStateShard>; Self::SHARD_COUNT],
}

type NativeReadAheadHandles = SmallVec<[u64; 4]>;

impl Default for NativeReadAheadStateStore {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(NativeReadAheadStateShard::default())),
        }
    }
}

#[derive(Debug, Default)]
struct NativeReadAheadStateShard {
    states: HashMap<u64, NativeReadAheadState>,
    inode_by_fh: HashMap<u64, u64>,
    fhs_by_inode: HashMap<u64, NativeReadAheadHandles>,
}

impl NativeReadAheadStateStore {
    const SHARD_COUNT: usize = 64;

    fn shard_index(ino: u64) -> usize {
        (ino as usize) % Self::SHARD_COUNT
    }

    fn observe(
        &self,
        policy: &NativeReadAheadPolicy,
        fh: u64,
        ino: u64,
        offset: u64,
        size: u64,
        file_size: u64,
    ) -> NativeReadAheadDecision {
        let mut shard = lock_or_recover(self.shard(ino), "native_read_ahead_state_shard");
        shard.track_handle_inode(fh, ino);
        let state = shard.states.entry(fh).or_default();
        decide_native_read_ahead(policy, state, offset, size, file_size)
    }

    fn invalidate_inodes(&self, inodes: &[u64]) {
        for &ino in inodes {
            let mut shard = lock_or_recover(self.shard(ino), "native_read_ahead_state_shard");
            let Some(fhs) = shard.fhs_by_inode.remove(&ino) else {
                continue;
            };
            for fh in fhs {
                shard.states.remove(&fh);
                shard.inode_by_fh.remove(&fh);
            }
        }
    }

    fn remove_handle(&self, ino: u64, fh: u64) {
        let mut shard = lock_or_recover(self.shard(ino), "native_read_ahead_state_shard");
        shard.remove_handle(fh);
    }

    #[cfg(test)]
    fn contains_handle(&self, ino: u64, fh: u64) -> bool {
        lock_or_recover(self.shard(ino), "native_read_ahead_state_shard")
            .states
            .contains_key(&fh)
    }

    #[cfg(test)]
    fn inode_for_handle(&self, ino: u64, fh: u64) -> Option<u64> {
        lock_or_recover(self.shard(ino), "native_read_ahead_state_shard")
            .inode_by_fh
            .get(&fh)
            .copied()
    }

    fn shard(&self, ino: u64) -> &Mutex<NativeReadAheadStateShard> {
        &self.shards[Self::shard_index(ino)]
    }
}

impl NativeReadAheadStateShard {
    fn track_handle_inode(&mut self, fh: u64, ino: u64) {
        let previous_ino = self.inode_by_fh.insert(fh, ino);
        if previous_ino == Some(ino) {
            return;
        }
        if let Some(previous_ino) = previous_ino {
            self.states.remove(&fh);
            if let Some(fhs) = self.fhs_by_inode.get_mut(&previous_ino) {
                remove_read_ahead_handle(fhs, fh);
                if fhs.is_empty() {
                    self.fhs_by_inode.remove(&previous_ino);
                }
            }
        }
        insert_read_ahead_handle(self.fhs_by_inode.entry(ino).or_default(), fh);
    }

    fn remove_handle(&mut self, fh: u64) {
        self.states.remove(&fh);
        let Some(ino) = self.inode_by_fh.remove(&fh) else {
            return;
        };
        if let Some(fhs) = self.fhs_by_inode.get_mut(&ino) {
            remove_read_ahead_handle(fhs, fh);
            if fhs.is_empty() {
                self.fhs_by_inode.remove(&ino);
            }
        }
    }
}

fn insert_read_ahead_handle(handles: &mut NativeReadAheadHandles, fh: u64) {
    if !handles.contains(&fh) {
        handles.push(fh);
    }
}

fn remove_read_ahead_handle(handles: &mut NativeReadAheadHandles, fh: u64) {
    if let Some(index) = handles.iter().position(|stored_fh| *stored_fh == fh) {
        handles.swap_remove(index);
    }
}

/// Point-in-time latency statistics for one native Linux opcode family.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeLatencyMetricsSnapshot {
    /// Number of sampled operations.
    pub samples: u64,
    /// Total observed latency in nanoseconds.
    pub total_ns: u64,
    /// Maximum observed latency in nanoseconds.
    pub max_ns: u64,
    /// 95th percentile latency in nanoseconds.
    pub p95_ns: u64,
    /// 99th percentile latency in nanoseconds.
    pub p99_ns: u64,
}

/// Snapshot of Linux native metadata and application I/O amplification counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NativeAmplificationMetricsSnapshot {
    /// Total FUSE operations included in amplification telemetry.
    pub fuse_ops: u64,
    /// Metadata probe operations handled by this bridge.
    pub metadata_ops: u64,
    /// Lookup operations handled by this bridge.
    pub lookup_ops: u64,
    /// Getattr operations handled by this bridge.
    pub getattr_ops: u64,
    /// Readdir operations handled by this bridge.
    pub readdir_ops: u64,
    /// Readdirplus operations handled by this bridge.
    pub readdirplus_ops: u64,
    /// File open operations handled by this bridge.
    pub open_ops: u64,
    /// File release operations handled by this bridge.
    pub release_ops: u64,
    /// Application read operations handled by this bridge.
    pub read_ops: u64,
    /// Application write operations handled by this bridge.
    pub write_ops: u64,
    /// Lookup/getattr ratio scaled by 1,000,000 for integer telemetry export.
    pub lookup_getattr_ratio_micros: u64,
    /// Open/release ratio scaled by 1,000,000 for integer telemetry export.
    pub open_release_ratio_micros: u64,
    /// Total FUSE operations per application read/write, scaled by 1,000,000.
    pub fuse_ops_per_read_write_micros: u64,
    /// Metadata operations per file open, scaled by 1,000,000.
    pub metadata_ops_per_open_micros: u64,
    /// Lookup latency statistics.
    pub lookup_latency: NativeLatencyMetricsSnapshot,
    /// Getattr latency statistics.
    pub getattr_latency: NativeLatencyMetricsSnapshot,
    /// Readdir latency statistics.
    pub readdir_latency: NativeLatencyMetricsSnapshot,
    /// Readdirplus latency statistics.
    pub readdirplus_latency: NativeLatencyMetricsSnapshot,
    /// Open latency statistics.
    pub open_latency: NativeLatencyMetricsSnapshot,
    /// Release latency statistics.
    pub release_latency: NativeLatencyMetricsSnapshot,
    /// Read latency statistics.
    pub read_latency: NativeLatencyMetricsSnapshot,
    /// Write latency statistics.
    pub write_latency: NativeLatencyMetricsSnapshot,
    /// Total metadata latency across lookup/getattr/readdir/readdirplus.
    pub metadata_latency_total_ns: u64,
}

impl NativeAmplificationMetrics {
    fn record_lookup(&self, latency_ns: u64) {
        self.lookup_ops.fetch_add(1, Ordering::Relaxed);
        self.lookup_latency.record(latency_ns);
    }

    fn record_getattr(&self, latency_ns: u64) {
        self.getattr_ops.fetch_add(1, Ordering::Relaxed);
        self.getattr_latency.record(latency_ns);
    }

    fn record_readdir(&self, latency_ns: u64) {
        self.readdir_ops.fetch_add(1, Ordering::Relaxed);
        self.readdir_latency.record(latency_ns);
    }

    fn record_readdirplus(&self, latency_ns: u64) {
        self.readdirplus_ops.fetch_add(1, Ordering::Relaxed);
        self.readdirplus_latency.record(latency_ns);
    }

    fn record_open(&self, latency_ns: u64) {
        self.open_ops.fetch_add(1, Ordering::Relaxed);
        self.open_latency.record(latency_ns);
    }

    fn record_release(&self, latency_ns: u64) {
        self.release_ops.fetch_add(1, Ordering::Relaxed);
        self.release_latency.record(latency_ns);
    }

    fn record_read(&self, latency_ns: u64) {
        self.read_ops.fetch_add(1, Ordering::Relaxed);
        self.read_latency.record(latency_ns);
    }

    fn record_write(&self, latency_ns: u64) {
        self.write_ops.fetch_add(1, Ordering::Relaxed);
        self.write_latency.record(latency_ns);
    }

    fn snapshot(&self) -> NativeAmplificationMetricsSnapshot {
        let lookup_ops = self.lookup_ops.load(Ordering::Relaxed);
        let getattr_ops = self.getattr_ops.load(Ordering::Relaxed);
        let readdir_ops = self.readdir_ops.load(Ordering::Relaxed);
        let readdirplus_ops = self.readdirplus_ops.load(Ordering::Relaxed);
        let open_ops = self.open_ops.load(Ordering::Relaxed);
        let release_ops = self.release_ops.load(Ordering::Relaxed);
        let read_ops = self.read_ops.load(Ordering::Relaxed);
        let write_ops = self.write_ops.load(Ordering::Relaxed);
        let metadata_ops = lookup_ops + getattr_ops + readdir_ops + readdirplus_ops;
        let fuse_ops = metadata_ops + open_ops + release_ops + read_ops + write_ops;
        let application_io_ops = read_ops + write_ops;
        let lookup_latency = self.lookup_latency.snapshot();
        let getattr_latency = self.getattr_latency.snapshot();
        let readdir_latency = self.readdir_latency.snapshot();
        let readdirplus_latency = self.readdirplus_latency.snapshot();
        let open_latency = self.open_latency.snapshot();
        let release_latency = self.release_latency.snapshot();
        let read_latency = self.read_latency.snapshot();
        let write_latency = self.write_latency.snapshot();
        NativeAmplificationMetricsSnapshot {
            fuse_ops,
            metadata_ops,
            lookup_ops,
            getattr_ops,
            readdir_ops,
            readdirplus_ops,
            open_ops,
            release_ops,
            read_ops,
            write_ops,
            lookup_getattr_ratio_micros: scaled_ratio_micros(lookup_ops, getattr_ops),
            open_release_ratio_micros: scaled_ratio_micros(open_ops, release_ops),
            fuse_ops_per_read_write_micros: scaled_ratio_micros(fuse_ops, application_io_ops),
            metadata_ops_per_open_micros: scaled_ratio_micros(metadata_ops, open_ops),
            lookup_latency,
            getattr_latency,
            readdir_latency,
            readdirplus_latency,
            open_latency,
            release_latency,
            read_latency,
            write_latency,
            metadata_latency_total_ns: lookup_latency
                .total_ns
                .saturating_add(getattr_latency.total_ns)
                .saturating_add(readdir_latency.total_ns)
                .saturating_add(readdirplus_latency.total_ns),
        }
    }
}

impl NativeLatencyMetrics {
    fn record(&self, latency_ns: u64) {
        let latency_ns = latency_ns.max(1);
        self.samples.fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(latency_ns, Ordering::Relaxed);
        self.max_ns.fetch_max(latency_ns, Ordering::Relaxed);
        self.buckets[native_latency_bucket_index(latency_ns)].fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> NativeLatencyMetricsSnapshot {
        let buckets = self.bucket_counts();
        let samples = self.samples.load(Ordering::Relaxed);
        NativeLatencyMetricsSnapshot {
            samples,
            total_ns: self.total_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
            p95_ns: percentile_from_buckets(&buckets, samples, 95),
            p99_ns: percentile_from_buckets(&buckets, samples, 99),
        }
    }

    fn bucket_counts(&self) -> [u64; NATIVE_LATENCY_BUCKET_BOUNDS_NS.len()] {
        std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed))
    }
}

fn native_latency_bucket_index(latency_ns: u64) -> usize {
    // The bucket table is intentionally tiny; linear search avoids extra branchy
    // machinery and keeps the hot-path choice obvious.
    NATIVE_LATENCY_BUCKET_BOUNDS_NS
        .iter()
        .position(|bound| latency_ns <= *bound)
        .unwrap_or(NATIVE_LATENCY_BUCKET_BOUNDS_NS.len() - 1)
}

fn percentile_from_buckets(
    buckets: &[u64; NATIVE_LATENCY_BUCKET_BOUNDS_NS.len()],
    samples: u64,
    percentile: u64,
) -> u64 {
    if samples == 0 {
        return 0;
    }
    let rank = samples
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1);
    let mut seen = 0_u64;
    for (index, count) in buckets.iter().enumerate() {
        seen = seen.saturating_add(*count);
        if seen > rank {
            return NATIVE_LATENCY_BUCKET_BOUNDS_NS[index];
        }
    }
    NATIVE_LATENCY_BUCKET_BOUNDS_NS[NATIVE_LATENCY_BUCKET_BOUNDS_NS.len() - 1]
}

fn elapsed_latency_ns(started_at: Instant) -> u64 {
    started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn scaled_ratio_micros(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    numerator.saturating_mul(1_000_000) / denominator
}

#[derive(Debug, Clone, Copy)]
enum NativeFuseMetric {
    Lookup,
    Getattr,
    Readdir,
    Readdirplus,
    Open,
    Release,
    Read,
    Write,
}

struct NativeFuseMetricsGuard<'a, V>
where
    V: NativeVfs + Clone + Send + 'static,
{
    bridge: &'a LinuxNativeBridge<V>,
    metric: NativeFuseMetric,
    started_at: Instant,
}

impl<V> Drop for NativeFuseMetricsGuard<'_, V>
where
    V: NativeVfs + Clone + Send + 'static,
{
    fn drop(&mut self) {
        self.bridge
            .record_native_fuse_metric(self.metric, elapsed_latency_ns(self.started_at));
    }
}

/// Snapshot of native Linux read path counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeReadMetricsSnapshot {
    /// Reads served as a stable local-cache range reply.
    pub local_cache_hits: u64,
    /// Reads that had no local-cache range available and used bytes fallback.
    pub local_cache_unavailable_fallbacks: u64,
    /// Reads that rejected a local-cache range because its epoch was stale.
    pub stale_epoch_fallbacks: u64,
    /// Reads that rejected a local-cache range because its invalidation generation was stale.
    pub stale_generation_fallbacks: u64,
    /// Reads served through the safe bytes fallback.
    pub bytes_fallbacks: u64,
    /// Total payload bytes returned from reads by this bridge.
    pub bytes_returned: u64,
    /// Read-ahead decisions that issued a prefetch hint.
    pub read_ahead_prefetches: u64,
    /// Read-ahead decisions that intentionally avoided prefetch.
    pub read_ahead_suppressed: u64,
    /// Bytes requested beyond application reads by read-ahead decisions.
    pub read_ahead_amplification_bytes: u64,
}

impl NativeReadMetrics {
    fn snapshot(&self) -> NativeReadMetricsSnapshot {
        NativeReadMetricsSnapshot {
            local_cache_hits: self.local_cache_hits.load(Ordering::Relaxed),
            local_cache_unavailable_fallbacks: self
                .local_cache_unavailable_fallbacks
                .load(Ordering::Relaxed),
            stale_epoch_fallbacks: self.stale_epoch_fallbacks.load(Ordering::Relaxed),
            stale_generation_fallbacks: self.stale_generation_fallbacks.load(Ordering::Relaxed),
            bytes_fallbacks: self.bytes_fallbacks.load(Ordering::Relaxed),
            bytes_returned: self.bytes_returned.load(Ordering::Relaxed),
            read_ahead_prefetches: self.read_ahead_prefetches.load(Ordering::Relaxed),
            read_ahead_suppressed: self.read_ahead_suppressed.load(Ordering::Relaxed),
            read_ahead_amplification_bytes: self
                .read_ahead_amplification_bytes
                .load(Ordering::Relaxed),
        }
    }
}

impl<V> LinuxNativeBridge<V> {
    /// Build an operation adapter around a VFS backend.
    pub fn new(vfs: V) -> Self {
        Self {
            vfs,
            barrier_gates: OperationBarrierGates::default(),
            async_write: None,
            pending_writes: PendingWriteTracker::default(),
            lookup_counts: NativeLookupCountStore::default(),
            open_cache_metrics: NativeOpenCacheMetrics::default(),
            amplification_metrics: NativeAmplificationMetrics::default(),
            read_metrics: NativeReadMetrics::default(),
            native_bridge_snapshot_started_at: Instant::now(),
            native_bridge_snapshot_last_elapsed_ms: AtomicU64::new(
                NATIVE_BRIDGE_SNAPSHOT_NEVER_PUBLISHED,
            ),
            trusted_open_cache_states: NativeTrustedOpenCacheStateStore::default(),
            read_ahead_policy: NativeReadAheadPolicy {
                max_prefetch_bytes: 1024 * 1024,
                sequential_window_bytes: 128 * 1024,
                min_sequential_reads: 2,
            },
            read_ahead_states: NativeReadAheadStateStore::default(),
        }
    }

    /// Enables native async write admission for this bridge.
    pub fn with_async_write_config(mut self, config: NativeAsyncWriteConfig) -> Self {
        self.async_write = Some(config);
        self
    }

    #[cfg(test)]
    fn with_async_write_for_test(self, config: NativeAsyncWriteConfig) -> Self {
        self.with_async_write_config(config)
    }

    /// Returns the kernel lookup reference count currently tracked for `ino`.
    pub fn kernel_lookup_count(&self, ino: u64) -> u64 {
        self.lookup_counts.count(ino)
    }

    /// Snapshot native Linux open cache policy metrics.
    pub fn open_cache_metrics_snapshot(&self) -> NativeOpenCacheMetricsSnapshot {
        self.open_cache_metrics.snapshot()
    }

    /// Snapshot native Linux metadata and I/O amplification counters.
    pub fn amplification_metrics_snapshot(&self) -> NativeAmplificationMetricsSnapshot {
        self.amplification_metrics.snapshot()
    }

    /// Snapshot native Linux read path metrics.
    pub fn read_metrics_snapshot(&self) -> NativeReadMetricsSnapshot {
        self.read_metrics.snapshot()
    }

    /// Snapshot native Linux async write admission and barrier metrics.
    pub fn async_write_snapshot(&self) -> NativeAsyncWriteSnapshot {
        let Some(config) = &self.async_write else {
            return NativeAsyncWriteSnapshot::default();
        };
        native_async_write_snapshot(&self.pending_writes, config)
    }
}

impl<V> FuseOperationHandler for LinuxNativeBridge<V>
where
    V: NativeVfs + Clone + Send + 'static,
{
    fn handle(&mut self, request: FuseRequest) -> FuseReply {
        match request.opcode {
            FuseOpcode::Lookup => self.handle_lookup(request),
            FuseOpcode::Forget => self.handle_forget(request),
            FuseOpcode::Getattr => self.handle_getattr(request),
            FuseOpcode::Open => self.handle_open(request),
            FuseOpcode::Read => self.handle_read(request),
            FuseOpcode::Write => self.handle_write(request),
            FuseOpcode::Flush => self.handle_flush(request),
            FuseOpcode::Fsync => self.handle_fsync(request),
            FuseOpcode::Release => self.handle_release(request),
            FuseOpcode::Opendir => self.handle_opendir(request),
            FuseOpcode::Releasedir => self.handle_releasedir(request),
            FuseOpcode::Readdir => self.handle_readdir(request),
            FuseOpcode::Readdirplus => self.handle_readdirplus(request),
            FuseOpcode::BatchForget => self.handle_batch_forget(request),
            FuseOpcode::Mkdir => self.handle_mkdir(request),
            FuseOpcode::Create => self.handle_create(request),
            FuseOpcode::Unlink => self.handle_unlink(request),
            FuseOpcode::Rmdir => self.handle_rmdir(request),
            FuseOpcode::Rename => self.handle_rename(request),
            FuseOpcode::Statfs => self.handle_statfs(request),
            FuseOpcode::Setattr => self.handle_setattr(request),
            _ => errno_reply(request.header.unique, libc::ENOSYS),
        }
    }
}

impl<V> LinuxNativeBridge<V>
where
    V: NativeVfs + Clone + Send + 'static,
{
    fn metrics_guard(&self, metric: NativeFuseMetric) -> NativeFuseMetricsGuard<'_, V> {
        NativeFuseMetricsGuard {
            bridge: self,
            metric,
            started_at: Instant::now(),
        }
    }

    fn record_native_fuse_metric(&self, metric: NativeFuseMetric, latency_ns: u64) {
        match metric {
            NativeFuseMetric::Lookup => self.amplification_metrics.record_lookup(latency_ns),
            NativeFuseMetric::Getattr => self.amplification_metrics.record_getattr(latency_ns),
            NativeFuseMetric::Readdir => self.amplification_metrics.record_readdir(latency_ns),
            NativeFuseMetric::Readdirplus => {
                self.amplification_metrics.record_readdirplus(latency_ns);
            }
            NativeFuseMetric::Open => self.amplification_metrics.record_open(latency_ns),
            NativeFuseMetric::Release => self.amplification_metrics.record_release(latency_ns),
            NativeFuseMetric::Read => self.amplification_metrics.record_read(latency_ns),
            NativeFuseMetric::Write => self.amplification_metrics.record_write(latency_ns),
        }
        self.publish_native_bridge_snapshot_if_due();
    }

    fn handle_lookup(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Lookup);
        let name = match lookup_name(&request.payload) {
            Some(name) => name,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.lookup(request.header.nodeid, name) {
            Ok(attr) => {
                self.add_lookup(attr.ino, 1);
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &entry_payload(attr)),
                )
            }
            Err(libc::ENOENT) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, negative_entry_payload()),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_forget(&self, request: FuseRequest) -> FuseReply {
        if let Some(nlookup) = forget_request(&request.payload) {
            self.forget_lookup(request.header.nodeid, nlookup);
        }
        FuseReply::no_reply(request.header.unique)
    }

    fn handle_getattr(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Getattr);
        match self.vfs.getattr(request.header.nodeid) {
            Ok(attr) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &attr_out_payload(attr)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_open(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Open);
        let flags = match open_flags(&request.payload) {
            Some(flags) => flags,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.open(request.header.nodeid, flags) {
            Ok(fh) => {
                let decision = self.open_cache_decision(
                    request.header.nodeid,
                    flags,
                    self.vfs.open_cache_state(request.header.nodeid),
                );
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &open_payload(fh, decision)),
                )
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_release(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Release);
        let fh = match release_fh(&request.payload) {
            Some(fh) => fh,
            None => return invalid_request(request.header.unique),
        };

        if self.async_write.is_some() {
            let pending_result = self
                .pending_writes
                .drain_and_begin_close(request.header.nodeid, fh);
            self.publish_async_write_snapshot();
            if pending_result == Err(libc::EAGAIN) {
                return errno_reply(request.header.unique, libc::EAGAIN);
            }
            let release_result = self.vfs.release(fh);
            self.pending_writes.finish_close(request.header.nodeid, fh);
            self.forget_read_ahead_handle(request.header.nodeid, fh);
            let reply = empty_barrier_reply(request.header.unique, pending_result, release_result);
            return reply;
        }

        self.barrier_gates
            .wait_for_barrier(ResourceKey::Inode(request.header.nodeid));
        let release_result = self.vfs.release(fh);
        self.forget_read_ahead_handle(request.header.nodeid, fh);
        match release_result {
            Ok(()) => {
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_read(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Read);
        let read = match read_request(&request.payload) {
            Some(read) => read,
            None => return invalid_request(request.header.unique),
        };
        if let Err(errno) = checked_native_read_size(read.size) {
            return errno_reply(request.header.unique, errno);
        }
        self.observe_read_ahead(request.header.nodeid, read);

        match self.vfs.read_local_cache_range(
            read.fh,
            request.header.nodeid,
            read.offset,
            read.size,
        ) {
            Ok(Some(range)) => {
                match self.trusted_local_cache_read_range(request.header.nodeid, read.size, range) {
                    LocalCacheReadRangeDecision::Trusted(range) => {
                        let range_len = range.len;
                        match FuseReply::local_cache_range_data(request.header.unique, range) {
                            Ok(reply) => {
                                self.read_metrics
                                    .local_cache_hits
                                    .fetch_add(1, Ordering::Relaxed);
                                self.read_metrics
                                    .bytes_returned
                                    .fetch_add(u64::from(range_len), Ordering::Relaxed);
                                return reply;
                            }
                            Err(error) => {
                                warn!(
                                    ino = request.header.nodeid,
                                    fh = read.fh,
                                    offset = read.offset,
                                    size = read.size,
                                    error = ?error,
                                    "native local cache read reply setup failed; falling back to bytes"
                                );
                                self.read_metrics
                                    .local_cache_unavailable_fallbacks
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    LocalCacheReadRangeDecision::StaleEpoch => {
                        self.read_metrics
                            .stale_epoch_fallbacks
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    LocalCacheReadRangeDecision::StaleGeneration => {
                        self.read_metrics
                            .stale_generation_fallbacks
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    LocalCacheReadRangeDecision::Unsafe => {}
                }
            }
            Ok(None) => {
                self.read_metrics
                    .local_cache_unavailable_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(errno) => {
                return errno_reply(request.header.unique, errno);
            }
        }

        match self
            .vfs
            .read(read.fh, request.header.nodeid, read.offset, read.size)
        {
            Ok(data) => {
                self.read_metrics
                    .bytes_fallbacks
                    .fetch_add(1, Ordering::Relaxed);
                self.read_metrics
                    .bytes_returned
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                FuseReply::segmented_data(request.header.unique, data)
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_write(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Write);
        let write = match write_request(&request.payload) {
            Some(write) => write,
            None => return invalid_request(request.header.unique),
        };

        if let Some(config) = &self.async_write {
            let reply = self.handle_async_write(
                request.header.unique,
                request.header.nodeid,
                write,
                config,
            );
            return reply;
        }

        let _pending_write = self
            .barrier_gates
            .begin_write(ResourceKey::Inode(request.header.nodeid));
        match self.vfs.write(
            write.fh,
            request.header.nodeid,
            write.offset,
            write.data,
            write.flags,
        ) {
            Ok(size) => {
                self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid]);
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &write_payload(size)),
                )
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_async_write(
        &self,
        unique: u64,
        ino: u64,
        write: WriteRequest<'_>,
        config: &NativeAsyncWriteConfig,
    ) -> FuseReply {
        let data_len = write.data.len();
        let registration = match self.pending_writes.register_with_admission(
            ino,
            write.fh,
            data_len,
            config.admission_limits(),
        ) {
            Ok(registration) => registration,
            Err(error) => {
                self.publish_async_write_snapshot();
                return errno_reply(unique, write_registration_error_to_errno(error));
            }
        };
        self.publish_async_write_snapshot();
        self.invalidate_metadata_cache_for_mutation(&[ino]);
        let mut registration = registration;
        let turn = NativeAsyncWriteTurn::new(registration.turn_waiter());
        let data = write.data.to_vec();
        let vfs = self.vfs.clone();
        let pending_writes = self.pending_writes.clone();
        let config = config.clone();
        let fh = write.fh;
        let offset = write.offset;
        let flags = write.flags;
        self.vfs.spawn_native_async_write_job(turn, move || {
            let completion = match vfs.write(fh, ino, offset, &data, flags) {
                Ok(_) => PendingWriteCompletion::Success,
                Err(errno) => PendingWriteCompletion::Error(errno),
            };
            registration.complete(completion);
            drop(registration);
            vfs.record_async_write_snapshot(native_async_write_snapshot(&pending_writes, &config));
        });

        FuseReply::contiguous(unique, ok_reply(unique, &write_payload(data_len as u32)))
    }

    fn handle_flush(&self, request: FuseRequest) -> FuseReply {
        let fh = match flush_fh(&request.payload) {
            Some(fh) => fh,
            None => return invalid_request(request.header.unique),
        };

        if self.async_write.is_some() {
            let pending_result = if fh != 0 {
                self.pending_writes
                    .handle_barrier(request.header.nodeid, fh)
                    .wait()
            } else {
                self.pending_writes
                    .inode_barrier(request.header.nodeid)
                    .wait()
            };
            self.publish_async_write_snapshot();
            let flush_result = self.vfs.flush(request.header.nodeid, fh);
            return empty_barrier_reply(request.header.unique, pending_result, flush_result);
        }

        self.barrier_gates
            .wait_for_barrier(ResourceKey::Inode(request.header.nodeid));
        match self.vfs.flush(request.header.nodeid, fh) {
            Ok(()) => {
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_fsync(&self, request: FuseRequest) -> FuseReply {
        let datasync = match fsync_datasync(&request.payload) {
            Some(datasync) => datasync,
            None => return invalid_request(request.header.unique),
        };

        if self.async_write.is_some() {
            let pending_result = self
                .pending_writes
                .inode_barrier(request.header.nodeid)
                .wait();
            self.publish_async_write_snapshot();
            let fsync_result = self.vfs.fsync(request.header.nodeid, datasync);
            if let Err(errno) = fsync_result {
                warn!(
                    ino = request.header.nodeid,
                    datasync, errno, "native FSYNC failed"
                );
            }
            return empty_barrier_reply(request.header.unique, pending_result, fsync_result);
        }

        self.barrier_gates
            .wait_for_barrier(ResourceKey::Inode(request.header.nodeid));
        match self.vfs.fsync(request.header.nodeid, datasync) {
            Ok(()) => {
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_opendir(&self, request: FuseRequest) -> FuseReply {
        match self.vfs.opendir(request.header.nodeid) {
            Ok(fh) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(
                    request.header.unique,
                    &open_payload(fh, cached_open_decision()),
                ),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_releasedir(&self, request: FuseRequest) -> FuseReply {
        let fh = match release_fh(&request.payload) {
            Some(fh) => fh,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.releasedir(request.header.nodeid, fh) {
            Ok(()) => {
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_readdir(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Readdir);
        let readdir = match read_request(&request.payload) {
            Some(readdir) => readdir,
            None => return invalid_request(request.header.unique),
        };
        let offset = match i64::try_from(readdir.offset) {
            Ok(offset) => offset,
            Err(_) => return invalid_request(request.header.unique),
        };

        match self.vfs.readdir(request.header.nodeid, readdir.fh, offset) {
            Ok(entries) => FuseReply::segmented_data(
                request.header.unique,
                readdir_payload(&entries, offset, readdir.size),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_readdirplus(&self, request: FuseRequest) -> FuseReply {
        let _metrics = self.metrics_guard(NativeFuseMetric::Readdirplus);
        let readdir = match read_request(&request.payload) {
            Some(readdir) => readdir,
            None => return invalid_request(request.header.unique),
        };
        let offset = match i64::try_from(readdir.offset) {
            Ok(offset) => offset,
            Err(_) => return invalid_request(request.header.unique),
        };

        match self.vfs.readdir(request.header.nodeid, readdir.fh, offset) {
            Ok(entries) => {
                let payload =
                    match readdirplus_payload(&self.vfs, &entries, offset, readdir.size, |ino| {
                        self.add_lookup(ino, 1);
                    }) {
                        Ok(payload) => payload,
                        Err(errno) => {
                            return errno_reply(request.header.unique, errno);
                        }
                    };
                FuseReply::segmented_data(request.header.unique, payload)
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_batch_forget(&self, request: FuseRequest) -> FuseReply {
        if let Some(entries) = batch_forget_request(&request.payload) {
            for (ino, nlookup) in entries {
                self.forget_lookup(ino, nlookup);
            }
        }
        FuseReply::no_reply(request.header.unique)
    }

    fn handle_mkdir(&self, request: FuseRequest) -> FuseReply {
        let mkdir = match mkdir_request(&request.payload) {
            Some(mkdir) => mkdir,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.mkdir(
            request.header.nodeid,
            mkdir.name,
            request.header.uid,
            request.header.gid,
            mkdir.mode,
        ) {
            Ok(attr) => {
                self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid, attr.ino]);
                self.add_lookup(attr.ino, 1);
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &entry_payload(attr)),
                )
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_create(&self, request: FuseRequest) -> FuseReply {
        let create = match create_request(&request.payload) {
            Some(create) => create,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.create(
            request.header.nodeid,
            create.name,
            request.header.uid,
            request.header.gid,
            create.mode,
            create.flags,
        ) {
            Ok((attr, fh)) => {
                self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid, attr.ino]);
                self.add_lookup(attr.ino, 1);
                let decision = self.open_cache_decision(
                    attr.ino,
                    create.flags,
                    self.vfs.open_cache_state(attr.ino),
                );
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &create_payload(attr, fh, decision)),
                )
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_unlink(&self, request: FuseRequest) -> FuseReply {
        let name = match lookup_name(&request.payload) {
            Some(name) => name,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.unlink(request.header.nodeid, name) {
            Ok(()) => {
                self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid]);
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_rmdir(&self, request: FuseRequest) -> FuseReply {
        let name = match lookup_name(&request.payload) {
            Some(name) => name,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.rmdir(request.header.nodeid, name) {
            Ok(()) => {
                self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid]);
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_rename(&self, request: FuseRequest) -> FuseReply {
        let rename = match rename_request(&request.payload) {
            Some(rename) => rename,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.rename(
            request.header.nodeid,
            rename.name,
            rename.new_parent,
            rename.new_name,
        ) {
            Ok(()) => {
                self.invalidate_metadata_cache_for_mutation(&[
                    request.header.nodeid,
                    rename.new_parent,
                ]);
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_statfs(&self, request: FuseRequest) -> FuseReply {
        match self.vfs.statfs() {
            Ok(statfs) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &statfs_payload(statfs)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_setattr(&self, request: FuseRequest) -> FuseReply {
        let setattr = match setattr_request(&request.payload) {
            Some(setattr) => setattr,
            None => return invalid_request(request.header.unique),
        };

        let result = match setattr.size {
            Some(size) => {
                if self.async_write.is_some() {
                    let pending_result = self
                        .pending_writes
                        .inode_barrier(request.header.nodeid)
                        .wait();
                    self.publish_async_write_snapshot();
                    match (
                        pending_result,
                        self.vfs.truncate(request.header.nodeid, setattr.fh, size),
                    ) {
                        (Err(errno), _) => return errno_reply(request.header.unique, errno),
                        (Ok(()), result) => result,
                    }
                } else {
                    self.barrier_gates
                        .wait_for_barrier(ResourceKey::Inode(request.header.nodeid));
                    self.vfs.truncate(request.header.nodeid, setattr.fh, size)
                }
            }
            None => self.vfs.getattr(request.header.nodeid),
        };

        match result {
            Ok(attr) => {
                if setattr.size.is_some() {
                    self.invalidate_metadata_cache_for_mutation(&[request.header.nodeid]);
                }
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(request.header.unique, &attr_out_payload(attr)),
                )
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn add_lookup(&self, ino: u64, nlookup: u64) {
        self.lookup_counts.add(ino, nlookup);
    }

    fn forget_lookup(&self, ino: u64, nlookup: u64) {
        if self.lookup_counts.forget(ino, nlookup) {
            self.trusted_open_cache_states.remove(ino);
        }
    }

    fn open_cache_decision(
        &self,
        ino: u64,
        flags: i32,
        cache_state: Option<NativeInodeCacheState>,
    ) -> NativeOpenCacheDecision {
        let Some(mut state) = cache_state else {
            let decision = cached_open_decision();
            self.open_cache_metrics.record(decision);
            return decision;
        };

        self.apply_trusted_open_cache_state(ino, &mut state);
        let (open_flags, _) = posix_flags_to_open_flags(flags);
        let read_only =
            !open_flags.contains(OpenFlags::WRONLY) && !open_flags.contains(OpenFlags::RDWR);
        let decision = decide_native_open_cache(read_only, &state);
        self.open_cache_metrics.record(decision);
        // Keep the read and write phases separate: this trusted snapshot is only
        // a cache hint, so a concurrent stale overwrite can at worst cause a
        // later redundant invalidation rather than stale page reuse.
        if matches!(
            decision.action,
            NativeOpenCacheAction::Cached | NativeOpenCacheAction::InvalidateThenCached
        ) {
            self.remember_trusted_open_cache_state(ino, &state);
        }
        decision
    }

    fn apply_trusted_open_cache_state(&self, ino: u64, state: &mut NativeInodeCacheState) {
        let trusted = self.trusted_open_cache_states.get(ino);
        let Some(trusted) = trusted else {
            return;
        };

        state
            .trusted
            .get_or_insert_with(|| trusted.current_trusted_snapshot());
    }

    fn remember_trusted_open_cache_state(&self, ino: u64, state: &NativeInodeCacheState) {
        self.trusted_open_cache_states.insert(ino, state.clone());
    }

    fn invalidate_metadata_cache_for_mutation(&self, inodes: &[u64]) {
        for &ino in inodes {
            self.trusted_open_cache_states.remove(ino);
        }
        self.read_ahead_states.invalidate_inodes(inodes);
    }

    fn forget_read_ahead_handle(&self, ino: u64, fh: u64) {
        self.read_ahead_states.remove_handle(ino, fh);
    }

    fn trusted_local_cache_read_range(
        &self,
        ino: u64,
        requested_size: u32,
        range: NativeLocalCacheReadRange,
    ) -> LocalCacheReadRangeDecision {
        let Some(state) = self.trusted_open_cache_states.get(ino) else {
            return LocalCacheReadRangeDecision::Unsafe;
        };
        let range_is_current = range.content_epoch == state.content_epoch;
        let range_generation_is_current =
            range.invalidation_generation == state.invalidation_generation;
        let range_is_bounded = range.len <= requested_size;
        if !range_is_current {
            return LocalCacheReadRangeDecision::StaleEpoch;
        }
        if !range_generation_is_current {
            return LocalCacheReadRangeDecision::StaleGeneration;
        }
        if state.local_materialized && !state.writer_active && !state.dirty && range_is_bounded {
            LocalCacheReadRangeDecision::Trusted(range)
        } else {
            LocalCacheReadRangeDecision::Unsafe
        }
    }

    fn observe_read_ahead(&self, ino: u64, read: ReadRequest) {
        let Some(cache_state) = self.trusted_open_cache_states.get(ino) else {
            return;
        };
        if !cache_state.local_materialized || cache_state.writer_active || cache_state.dirty {
            return;
        }

        let decision = self.read_ahead_states.observe(
            &self.read_ahead_policy,
            read.fh,
            ino,
            read.offset,
            u64::from(read.size),
            cache_state.size,
        );
        let Some(prefetch) = decision.prefetch else {
            self.read_metrics
                .read_ahead_suppressed
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.read_metrics
            .read_ahead_prefetches
            .fetch_add(1, Ordering::Relaxed);
        self.read_metrics
            .read_ahead_amplification_bytes
            .fetch_add(decision.read_amplification_bytes, Ordering::Relaxed);
        if let Err(errno) = self
            .vfs
            .advise_read_ahead(read.fh, ino, prefetch.offset, prefetch.len)
        {
            warn!(
                ino,
                fh = read.fh,
                offset = prefetch.offset,
                len = prefetch.len,
                errno,
                "native read-ahead hint failed"
            );
        }
    }

    fn publish_async_write_snapshot(&self) {
        let Some(config) = &self.async_write else {
            return;
        };
        self.vfs
            .record_async_write_snapshot(native_async_write_snapshot(&self.pending_writes, config));
    }

    fn publish_native_bridge_snapshot_if_due(&self) {
        let elapsed_ms = self
            .native_bridge_snapshot_started_at
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        let interval_ms = NATIVE_BRIDGE_SNAPSHOT_MIN_INTERVAL
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;

        let mut last = self
            .native_bridge_snapshot_last_elapsed_ms
            .load(Ordering::Relaxed);
        loop {
            let due = last == NATIVE_BRIDGE_SNAPSHOT_NEVER_PUBLISHED
                || elapsed_ms.saturating_sub(last) >= interval_ms;
            if !due {
                return;
            }
            match self
                .native_bridge_snapshot_last_elapsed_ms
                .compare_exchange_weak(last, elapsed_ms, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    self.publish_native_bridge_snapshot();
                    return;
                }
                Err(next) => last = next,
            }
        }
    }

    fn publish_native_bridge_snapshot(&self) {
        self.vfs.record_native_bridge_snapshot(
            self.amplification_metrics.snapshot(),
            self.read_metrics.snapshot(),
        );
    }
}

#[derive(Debug, Clone)]
enum LocalCacheReadRangeDecision {
    Trusted(NativeLocalCacheReadRange),
    StaleEpoch,
    StaleGeneration,
    Unsafe,
}

fn cached_open_decision() -> NativeOpenCacheDecision {
    NativeOpenCacheDecision {
        action: NativeOpenCacheAction::Cached,
        reason: NativeOpenCacheReason::Cached,
    }
}

fn invalid_request(unique: u64) -> FuseReply {
    errno_reply(unique, libc::EINVAL)
}

fn errno_reply(unique: u64, errno: i32) -> FuseReply {
    FuseReply::contiguous(unique, error_reply(unique, errno).to_vec())
}

fn empty_barrier_reply(
    unique: u64,
    pending_result: Result<(), i32>,
    operation_result: Result<(), i32>,
) -> FuseReply {
    match (pending_result, operation_result) {
        (Err(errno), _) => errno_reply(unique, errno),
        (Ok(()), Err(errno)) => errno_reply(unique, errno),
        (Ok(()), Ok(())) => FuseReply::contiguous(unique, ok_reply(unique, &[])),
    }
}

fn native_async_write_snapshot(
    pending_writes: &PendingWriteTracker,
    config: &NativeAsyncWriteConfig,
) -> NativeAsyncWriteSnapshot {
    let tracker = pending_writes.snapshot();
    NativeAsyncWriteSnapshot {
        snapshot_generation: tracker.mutation_generation,
        enabled: true,
        platform_policy: config.platform_policy.clone(),
        max_pending_bytes: config.max_pending_bytes,
        max_pending_writes: config.max_pending_writes,
        per_handle_pending_bytes: config.per_handle_pending_bytes,
        pending_writes: tracker.pending_writes,
        pending_bytes: tracker.pending_bytes,
        peak_pending_bytes: tracker.peak_pending_bytes,
        completed_writes: tracker.completed_writes,
        failed_writes: tracker.failed_writes,
        task_failed_writes: tracker.task_failed_writes,
        admission_waits: tracker.admission_waits,
        admission_wait_total_ms: tracker.admission_wait_ns as f64 / 1_000_000.0,
        barrier_waits: tracker.barrier_waits,
        barrier_wait_total_ms: tracker.barrier_wait_ns as f64 / 1_000_000.0,
    }
}

fn vfs_native_bridge_snapshot(
    amplification: NativeAmplificationMetricsSnapshot,
    read: NativeReadMetricsSnapshot,
) -> VfsNativeBridgeSnapshot {
    VfsNativeBridgeSnapshot {
        fuse_ops: amplification.fuse_ops,
        metadata_ops: amplification.metadata_ops,
        lookup_ops: amplification.lookup_ops,
        getattr_ops: amplification.getattr_ops,
        readdir_ops: amplification.readdir_ops,
        readdirplus_ops: amplification.readdirplus_ops,
        open_ops: amplification.open_ops,
        release_ops: amplification.release_ops,
        read_ops: amplification.read_ops,
        write_ops: amplification.write_ops,
        lookup_getattr_ratio_micros: amplification.lookup_getattr_ratio_micros,
        open_release_ratio_micros: amplification.open_release_ratio_micros,
        fuse_ops_per_read_write_micros: amplification.fuse_ops_per_read_write_micros,
        metadata_ops_per_open_micros: amplification.metadata_ops_per_open_micros,
        metadata_latency_total_ns: amplification.metadata_latency_total_ns,
        lookup_latency: vfs_native_latency_snapshot(amplification.lookup_latency),
        getattr_latency: vfs_native_latency_snapshot(amplification.getattr_latency),
        readdir_latency: vfs_native_latency_snapshot(amplification.readdir_latency),
        readdirplus_latency: vfs_native_latency_snapshot(amplification.readdirplus_latency),
        open_latency: vfs_native_latency_snapshot(amplification.open_latency),
        release_latency: vfs_native_latency_snapshot(amplification.release_latency),
        read_latency: vfs_native_latency_snapshot(amplification.read_latency),
        write_latency: vfs_native_latency_snapshot(amplification.write_latency),
        local_cache_hits: read.local_cache_hits,
        local_cache_unavailable_fallbacks: read.local_cache_unavailable_fallbacks,
        stale_epoch_fallbacks: read.stale_epoch_fallbacks,
        stale_generation_fallbacks: read.stale_generation_fallbacks,
        bytes_fallbacks: read.bytes_fallbacks,
        bytes_returned: read.bytes_returned,
        read_ahead_prefetches: read.read_ahead_prefetches,
        read_ahead_suppressed: read.read_ahead_suppressed,
        read_ahead_amplification_bytes: read.read_ahead_amplification_bytes,
    }
}

fn vfs_native_latency_snapshot(snapshot: NativeLatencyMetricsSnapshot) -> VfsNativeLatencySnapshot {
    VfsNativeLatencySnapshot {
        samples: snapshot.samples,
        total_ns: snapshot.total_ns,
        max_ns: snapshot.max_ns,
        p95_ns: snapshot.p95_ns,
        p99_ns: snapshot.p99_ns,
    }
}

fn native_async_write_default_limits() -> PendingWriteAdmissionLimits {
    PendingWriteAdmissionLimits {
        max_pending_bytes: Some(128 * 1024 * 1024),
        max_pending_writes: Some(256),
        per_handle_pending_bytes: Some(32 * 1024 * 1024),
    }
}

fn write_registration_error_to_errno(error: WriteRegistrationError) -> i32 {
    match error {
        WriteRegistrationError::HandleClosing => libc::EAGAIN,
        WriteRegistrationError::HandleClosed => libc::EIO,
        WriteRegistrationError::AdmissionLimitExceeded => libc::EAGAIN,
    }
}

fn lookup_name(payload: &[u8]) -> Option<&[u8]> {
    let name = payload.strip_suffix(&[0])?;
    if name.is_empty() || name.contains(&0) {
        return None;
    }
    Some(name)
}

fn open_flags(payload: &[u8]) -> Option<i32> {
    let bytes = payload.get(..FUSE_OPEN_IN_LEN)?;
    Some(read_u32(bytes, 0)? as i32)
}

fn release_fh(payload: &[u8]) -> Option<u64> {
    let bytes = payload.get(..FUSE_RELEASE_IN_LEN)?;
    read_u64(bytes, 0)
}

fn flush_fh(payload: &[u8]) -> Option<u64> {
    let bytes = payload.get(..FUSE_FLUSH_IN_LEN)?;
    read_u64(bytes, 0)
}

fn fsync_datasync(payload: &[u8]) -> Option<bool> {
    let bytes = payload.get(..FUSE_FSYNC_IN_LEN)?;
    Some(read_u32(bytes, 8)? & 1 != 0)
}

fn forget_request(payload: &[u8]) -> Option<u64> {
    let bytes = payload.get(..FUSE_FORGET_IN_LEN)?;
    read_u64(bytes, 0)
}

fn batch_forget_request(payload: &[u8]) -> Option<Vec<(u64, u64)>> {
    let header = payload.get(..FUSE_BATCH_FORGET_IN_LEN)?;
    let count = read_u32(header, 0)? as usize;
    let entries_len = count.checked_mul(FUSE_BATCH_FORGET_ONE_LEN)?;
    let entries_end = FUSE_BATCH_FORGET_IN_LEN.checked_add(entries_len)?;
    let entries = payload.get(FUSE_BATCH_FORGET_IN_LEN..entries_end)?;
    let mut forgets = Vec::with_capacity(count);
    for entry in entries.chunks_exact(FUSE_BATCH_FORGET_ONE_LEN) {
        forgets.push((read_u64(entry, 0)?, read_u64(entry, 8)?));
    }
    Some(forgets)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadRequest {
    fh: u64,
    offset: u64,
    size: u32,
}

fn read_request(payload: &[u8]) -> Option<ReadRequest> {
    let bytes = payload.get(..FUSE_READ_IN_LEN)?;
    Some(ReadRequest {
        fh: read_u64(bytes, 0)?,
        offset: read_u64(bytes, 8)?,
        size: read_u32(bytes, 16)?,
    })
}

fn checked_native_read_size(size: u32) -> Result<usize, i32> {
    let size = size as usize;
    if size > NATIVE_MAX_READ_SIZE_BYTES {
        return Err(libc::ENOMEM);
    }
    Ok(size)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WriteRequest<'a> {
    fh: u64,
    offset: u64,
    flags: i32,
    data: &'a [u8],
}

fn write_request(payload: &[u8]) -> Option<WriteRequest<'_>> {
    let header = payload.get(..FUSE_WRITE_IN_LEN)?;
    let size = read_u32(header, 16)? as usize;
    let data = payload.get(FUSE_WRITE_IN_LEN..FUSE_WRITE_IN_LEN.checked_add(size)?)?;
    Some(WriteRequest {
        fh: read_u64(header, 0)?,
        offset: read_u64(header, 8)?,
        flags: read_u32(header, 32)? as i32,
        data,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MkdirRequest<'a> {
    mode: u16,
    name: &'a [u8],
}

fn mkdir_request(payload: &[u8]) -> Option<MkdirRequest<'_>> {
    let header = payload.get(..FUSE_MKDIR_IN_LEN)?;
    let name = lookup_name(payload.get(FUSE_MKDIR_IN_LEN..)?)?;
    Some(MkdirRequest {
        mode: (read_u32(header, 0)? & 0o7777) as u16,
        name,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CreateRequest<'a> {
    flags: i32,
    mode: u16,
    name: &'a [u8],
}

fn create_request(payload: &[u8]) -> Option<CreateRequest<'_>> {
    let header = payload.get(..FUSE_CREATE_IN_LEN)?;
    let name = lookup_name(payload.get(FUSE_CREATE_IN_LEN..)?)?;
    Some(CreateRequest {
        flags: read_u32(header, 0)? as i32,
        mode: (read_u32(header, 4)? & 0o7777) as u16,
        name,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RenameRequest<'a> {
    new_parent: u64,
    name: &'a [u8],
    new_name: &'a [u8],
}

fn rename_request(payload: &[u8]) -> Option<RenameRequest<'_>> {
    let header = payload.get(..FUSE_RENAME_IN_LEN)?;
    let names = payload.get(FUSE_RENAME_IN_LEN..)?;
    let nul = names.iter().position(|byte| *byte == 0)?;
    let name = names.get(..nul)?;
    let new_name = lookup_name(names.get(nul + 1..)?)?;
    if name.is_empty() {
        return None;
    }
    Some(RenameRequest {
        new_parent: read_u64(header, 0)?,
        name,
        new_name,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SetattrRequest {
    fh: Option<u64>,
    size: Option<u64>,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TruncateRequest {
    fh: Option<u64>,
    size: u64,
}

fn setattr_request(payload: &[u8]) -> Option<SetattrRequest> {
    let bytes = payload.get(..FUSE_SETATTR_IN_LEN)?;
    let valid = read_u32(bytes, 0)?;
    if valid & !SUPPORTED_SETATTR_VALID != 0 {
        return None;
    }
    let fh = if valid & FATTR_FH != 0 {
        Some(read_u64(bytes, 8)?)
    } else {
        None
    };
    let size = if valid & FATTR_SIZE != 0 {
        Some(read_u64(bytes, 16)?)
    } else {
        None
    };
    Some(SetattrRequest { fh, size })
}

#[cfg(test)]
fn truncate_request(payload: &[u8]) -> Option<TruncateRequest> {
    let request = setattr_request(payload)?;
    Some(TruncateRequest {
        fh: request.fh,
        size: request.size?,
    })
}

fn native_attr_from_inode_attr(attr: InodeAttr) -> NativeAttr {
    NativeAttr {
        ino: attr.ino,
        size: attr.size,
        kind: match attr.kind {
            FileType::RegularFile => NativeFileType::RegularFile,
            FileType::Directory => NativeFileType::Directory,
            FileType::Symlink => NativeFileType::Symlink,
        },
        perm: attr.mode & 0o7777,
        uid: attr.uid,
        gid: attr.gid,
        atime_ns: u64::try_from(attr.atime).unwrap_or_default(),
        mtime_ns: u64::try_from(attr.mtime).unwrap_or_default(),
        ctime_ns: u64::try_from(attr.ctime).unwrap_or_default(),
    }
}

fn native_dir_entry(entry: DirEntry) -> NativeDirEntry {
    NativeDirEntry {
        ino: entry.ino,
        kind: match entry.kind {
            FileType::RegularFile => NativeFileType::RegularFile,
            FileType::Directory => NativeFileType::Directory,
            FileType::Symlink => NativeFileType::Symlink,
        },
        name: entry.name,
    }
}

fn entry_payload(attr: NativeAttr) -> Vec<u8> {
    let mut payload = Vec::with_capacity(FUSE_ENTRY_OUT_PAYLOAD_LEN);
    payload.extend_from_slice(&attr.ino.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    append_attr_payload(&mut payload, attr);
    payload
}

fn negative_entry_payload() -> &'static [u8] {
    static PAYLOAD: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();

    PAYLOAD.get_or_init(|| {
        let mut payload = Vec::with_capacity(FUSE_ENTRY_OUT_PAYLOAD_LEN);
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&1_u64.to_le_bytes());
        payload.extend_from_slice(&0_u64.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.extend_from_slice(&0_u32.to_le_bytes());
        payload.resize(FUSE_ENTRY_OUT_PAYLOAD_LEN, 0);
        payload
    })
}

fn attr_out_payload(attr: NativeAttr) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    append_attr_payload(&mut payload, attr);
    payload
}

fn open_payload(fh: u64, decision: NativeOpenCacheDecision) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&fh.to_le_bytes());
    payload.extend_from_slice(&open_reply_flags(decision).to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload
}

fn create_payload(attr: NativeAttr, fh: u64, decision: NativeOpenCacheDecision) -> Vec<u8> {
    let mut payload = entry_payload(attr);
    payload.extend_from_slice(&open_payload(fh, decision));
    payload
}

fn write_payload(size: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&size.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload
}

fn readdir_payload(entries: &[NativeDirEntry], offset: i64, size: u32) -> Vec<u8> {
    let start = usize::try_from(offset).unwrap_or(0);
    let limit = size as usize;
    let mut payload = Vec::new();
    for (index, entry) in entries.iter().enumerate().skip(start) {
        let next_offset = match u64::try_from(index + 1) {
            Ok(next_offset) => next_offset,
            Err(_) => break,
        };
        let encoded_len = fuse_dirent_len(entry.name.len());
        if payload.len().saturating_add(encoded_len) > limit {
            break;
        }
        append_dirent(&mut payload, entry, next_offset);
    }
    payload
}

fn readdirplus_payload<V: NativeVfs>(
    vfs: &V,
    entries: &[NativeDirEntry],
    offset: i64,
    size: u32,
    mut add_lookup: impl FnMut(u64),
) -> Result<Vec<u8>, i32> {
    let start = usize::try_from(offset).unwrap_or(0);
    let limit = size as usize;
    let mut payload = Vec::new();
    let mut pending_lookups = Vec::new();
    for (index, entry) in entries.iter().enumerate().skip(start) {
        let next_offset = match u64::try_from(index + 1) {
            Ok(next_offset) => next_offset,
            Err(_) => break,
        };
        let encoded_len = FUSE_ENTRY_OUT_PAYLOAD_LEN + fuse_dirent_len(entry.name.len());
        if payload.len().saturating_add(encoded_len) > limit {
            break;
        }
        let attr = match vfs.getattr(entry.ino) {
            Ok(attr) => attr,
            Err(errno) if is_stale_dirent_errno(errno) => continue,
            Err(errno) => return Err(errno),
        };
        payload.extend_from_slice(&entry_payload(attr));
        append_dirent(&mut payload, entry, next_offset);
        pending_lookups.push(attr.ino);
    }
    for ino in pending_lookups {
        add_lookup(ino);
    }
    Ok(payload)
}

fn statfs_payload(statfs: NativeStatFs) -> Vec<u8> {
    let mut payload = Vec::with_capacity(80);
    payload.extend_from_slice(&statfs.blocks.to_le_bytes());
    payload.extend_from_slice(&statfs.bfree.to_le_bytes());
    payload.extend_from_slice(&statfs.bavail.to_le_bytes());
    payload.extend_from_slice(&statfs.files.to_le_bytes());
    payload.extend_from_slice(&statfs.ffree.to_le_bytes());
    payload.extend_from_slice(&statfs.bsize.to_le_bytes());
    payload.extend_from_slice(&statfs.namelen.to_le_bytes());
    payload.extend_from_slice(&statfs.frsize.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&[0_u8; 24]);
    payload
}

fn native_statfs(statfs: StatFs) -> NativeStatFs {
    NativeStatFs {
        blocks: statfs.blocks,
        bfree: statfs.bfree,
        bavail: statfs.bavail,
        files: statfs.files,
        ffree: statfs.ffree,
        bsize: statfs.bsize,
        namelen: statfs.namelen,
        frsize: statfs.frsize,
    }
}

fn append_dirent(payload: &mut Vec<u8>, entry: &NativeDirEntry, next_offset: u64) {
    payload.extend_from_slice(&entry.ino.to_le_bytes());
    payload.extend_from_slice(&next_offset.to_le_bytes());
    payload.extend_from_slice(&(entry.name.len() as u32).to_le_bytes());
    payload.extend_from_slice(&dirent_type(entry.kind).to_le_bytes());
    payload.extend_from_slice(&entry.name);
    payload.resize(fuse_dirent_aligned_len(payload.len()), 0);
}

fn fuse_dirent_len(name_len: usize) -> usize {
    fuse_dirent_aligned_len(24 + name_len)
}

fn fuse_dirent_aligned_len(len: usize) -> usize {
    (len + 7) & !7
}

fn dirent_type(kind: NativeFileType) -> u32 {
    match kind {
        NativeFileType::RegularFile => u32::from(libc::DT_REG),
        NativeFileType::Directory => u32::from(libc::DT_DIR),
        NativeFileType::Symlink => u32::from(libc::DT_LNK),
    }
}

fn is_stale_dirent_errno(errno: i32) -> bool {
    matches!(errno, libc::ENOENT | libc::ESTALE)
}

fn write_local_sync_policy(flags: i32) -> LocalSyncPolicy {
    if flags & (libc::O_SYNC | libc::O_DSYNC) != 0 {
        LocalSyncPolicy::WriteThrough
    } else {
        LocalSyncPolicy::Opportunistic
    }
}

fn open_reply_flags(decision: NativeOpenCacheDecision) -> u32 {
    match decision.action {
        NativeOpenCacheAction::KeepCache => FOPEN_KEEP_CACHE,
        NativeOpenCacheAction::Cached | NativeOpenCacheAction::InvalidateThenCached => 0,
        NativeOpenCacheAction::DirectIo => FOPEN_DIRECT_IO,
    }
}

fn native_open_cache_state_from_attr(attr: InodeAttr, dirty: bool) -> NativeInodeCacheState {
    NativeInodeCacheState {
        content_epoch: attr.content_epoch,
        size: attr.size,
        mtime_ns: u64::try_from(attr.mtime).unwrap_or_default(),
        // Native Linux does not yet install a kernel invalidator generation source;
        // content epoch, size, and mtime remain the active production coherency keys.
        invalidation_generation: 0,
        trusted: None,
        writer_active: dirty,
        dirty,
        local_materialized: true,
    }
}

fn append_attr_payload(payload: &mut Vec<u8>, attr: NativeAttr) {
    let (atime_sec, atime_nsec) = split_unix_nanos(attr.atime_ns);
    let (mtime_sec, mtime_nsec) = split_unix_nanos(attr.mtime_ns);
    let (ctime_sec, ctime_nsec) = split_unix_nanos(attr.ctime_ns);
    payload.reserve(FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&attr.ino.to_le_bytes());
    payload.extend_from_slice(&attr.size.to_le_bytes());
    payload.extend_from_slice(&attr.size.div_ceil(POSIX_STAT_BLOCK_SIZE).to_le_bytes());
    payload.extend_from_slice(&atime_sec.to_le_bytes());
    payload.extend_from_slice(&mtime_sec.to_le_bytes());
    payload.extend_from_slice(&ctime_sec.to_le_bytes());
    payload.extend_from_slice(&atime_nsec.to_le_bytes());
    payload.extend_from_slice(&mtime_nsec.to_le_bytes());
    payload.extend_from_slice(&ctime_nsec.to_le_bytes());
    payload.extend_from_slice(&linux_mode(attr).to_le_bytes());
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&attr.uid.to_le_bytes());
    payload.extend_from_slice(&attr.gid.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&4096_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
}

fn split_unix_nanos(nanos: u64) -> (u64, u32) {
    (nanos / 1_000_000_000, (nanos % 1_000_000_000) as u32)
}

fn linux_mode(attr: NativeAttr) -> u32 {
    let file_type = match attr.kind {
        NativeFileType::RegularFile => libc::S_IFREG,
        NativeFileType::Directory => libc::S_IFDIR,
        NativeFileType::Symlink => libc::S_IFLNK,
    };
    file_type | u32::from(attr.perm)
}

fn read_u32(frame: &[u8], offset: usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let value = frame.get(offset..end)?.try_into().ok()?;
    Some(u32::from_le_bytes(value))
}

fn read_u64(frame: &[u8], offset: usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    let value = frame.get(offset..end)?.try_into().ok()?;
    Some(u64::from_le_bytes(value))
}

#[derive(Debug, Default)]
struct NativeDirectoryHandleStore {
    next_fh: AtomicU64,
    handles: Mutex<HashMap<u64, NativeDirectoryHandle>>,
}

#[derive(Debug, Clone)]
struct NativeDirectoryHandle {
    ino: u64,
    entries: Arc<[NativeDirEntry]>,
}

impl NativeDirectoryHandleStore {
    fn open(&self, ino: u64, entries: Arc<[NativeDirEntry]>) -> u64 {
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed) + 1;
        lock_or_recover(&self.handles, "directory_handles")
            .insert(fh, NativeDirectoryHandle { ino, entries });
        fh
    }

    fn entries(&self, ino: u64, fh: u64) -> Option<Arc<[NativeDirEntry]>> {
        let handles = lock_or_recover(&self.handles, "directory_handles");
        handles
            .get(&fh)
            .filter(|handle| handle.ino == ino)
            .map(|handle| Arc::clone(&handle.entries))
    }

    fn release(&self, ino: u64, fh: u64) -> bool {
        let mut handles = lock_or_recover(&self.handles, "directory_handles");
        if handles.get(&fh).is_some_and(|handle| handle.ino == ino) {
            handles.remove(&fh).is_some()
        } else {
            false
        }
    }
}

#[derive(Debug)]
struct NativeTrustedOpenCacheStateStore {
    shards: [RwLock<HashMap<u64, NativeInodeCacheState>>; Self::SHARD_COUNT],
}

impl NativeTrustedOpenCacheStateStore {
    const SHARD_COUNT: usize = 64;

    fn shard_index(ino: u64) -> usize {
        let mut value = ino;
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        (value as usize) & (Self::SHARD_COUNT - 1)
    }

    fn shard(&self, ino: u64) -> &RwLock<HashMap<u64, NativeInodeCacheState>> {
        &self.shards[Self::shard_index(ino)]
    }

    fn get(&self, ino: u64) -> Option<NativeInodeCacheState> {
        read_or_recover(self.shard(ino), "trusted_open_cache_states")
            .get(&ino)
            .cloned()
    }

    fn insert(&self, ino: u64, state: NativeInodeCacheState) {
        write_or_recover(self.shard(ino), "trusted_open_cache_states").insert(ino, state);
    }

    fn remove(&self, ino: u64) {
        write_or_recover(self.shard(ino), "trusted_open_cache_states").remove(&ino);
    }
}

const _: () = assert!(NativeTrustedOpenCacheStateStore::SHARD_COUNT.is_power_of_two());

impl Default for NativeTrustedOpenCacheStateStore {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
struct NativeLookupCountStore {
    shards: [Mutex<HashMap<u64, u64>>; Self::SHARD_COUNT],
}

impl NativeLookupCountStore {
    const SHARD_COUNT: usize = 64;

    fn shard_index(ino: u64) -> usize {
        let mut value = ino;
        value ^= value >> 30;
        value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value ^= value >> 27;
        value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        (value as usize) & (Self::SHARD_COUNT - 1)
    }

    fn shard(&self, ino: u64) -> &Mutex<HashMap<u64, u64>> {
        &self.shards[Self::shard_index(ino)]
    }

    fn add(&self, ino: u64, nlookup: u64) {
        let mut lookup_counts = lock_or_recover(self.shard(ino), "lookup_counts");
        let count = lookup_counts.entry(ino).or_default();
        *count = count.saturating_add(nlookup);
    }

    fn forget(&self, ino: u64, nlookup: u64) -> bool {
        let mut lookup_counts = lock_or_recover(self.shard(ino), "lookup_counts");
        let Some(count) = lookup_counts.get_mut(&ino) else {
            return false;
        };
        if *count <= nlookup {
            lookup_counts.remove(&ino);
            true
        } else {
            *count -= nlookup;
            false
        }
    }

    fn count(&self, ino: u64) -> u64 {
        lock_or_recover(self.shard(ino), "lookup_counts")
            .get(&ino)
            .copied()
            .unwrap_or(0)
    }
}

const _: () = assert!(NativeLookupCountStore::SHARD_COUNT.is_power_of_two());

impl Default for NativeLookupCountStore {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }
}

fn lock_or_recover<'a, T>(mutex: &'a Mutex<T>, name: &str) -> MutexGuard<'a, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(mutex = name, "recovering poisoned native bridge mutex");
            poisoned.into_inner()
        }
    }
}

fn read_or_recover<'a, T>(lock: &'a RwLock<T>, name: &str) -> RwLockReadGuard<'a, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                lock = name,
                "recovering poisoned native bridge rwlock reader"
            );
            poisoned.into_inner()
        }
    }
}

fn write_or_recover<'a, T>(lock: &'a RwLock<T>, name: &str) -> RwLockWriteGuard<'a, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!(
                lock = name,
                "recovering poisoned native bridge rwlock writer"
            );
            poisoned.into_inner()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StudioFuseAsyncWriteAdmissionLimits, StudioFuseRuntimeOptions};
    use std::mem::align_of;

    #[derive(Debug)]
    struct TestNativeVfs {
        lookup_calls: AtomicU64,
        getattr_calls: AtomicU64,
        readdir_calls: AtomicU64,
        read_calls: AtomicU64,
        local_cache_read_calls: AtomicU64,
        advise_read_ahead_calls: AtomicU64,
        last_read_ahead_offset: AtomicU64,
        last_read_ahead_len: AtomicU64,
        local_cache_file: std::fs::File,
        native_bridge_snapshots: Mutex<
            Vec<(
                NativeAmplificationMetricsSnapshot,
                NativeReadMetricsSnapshot,
            )>,
        >,
    }

    impl Default for TestNativeVfs {
        fn default() -> Self {
            Self {
                lookup_calls: AtomicU64::new(0),
                getattr_calls: AtomicU64::new(0),
                readdir_calls: AtomicU64::new(0),
                read_calls: AtomicU64::new(0),
                local_cache_read_calls: AtomicU64::new(0),
                advise_read_ahead_calls: AtomicU64::new(0),
                last_read_ahead_offset: AtomicU64::new(0),
                last_read_ahead_len: AtomicU64::new(0),
                local_cache_file: tempfile::tempfile().expect("create local cache fd"),
                native_bridge_snapshots: Mutex::new(Vec::new()),
            }
        }
    }

    impl NativeVfs for TestNativeVfs {
        fn lookup(&self, _parent: u64, name: &[u8]) -> Result<NativeAttr, i32> {
            self.lookup_calls.fetch_add(1, Ordering::Relaxed);
            if name == b"missing" {
                return Err(libc::ENOENT);
            }
            Ok(test_attr(20))
        }

        fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
            self.getattr_calls.fetch_add(1, Ordering::Relaxed);
            if ino == 21 {
                return Err(libc::EIO);
            }
            Ok(test_attr(ino))
        }

        fn open(&self, _ino: u64, _flags: i32) -> Result<u64, i32> {
            Ok(7)
        }

        fn release(&self, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn read(&self, _fh: u64, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, i32> {
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        fn read_local_cache_range(
            &self,
            _fh: u64,
            _ino: u64,
            _offset: u64,
            _size: u32,
        ) -> Result<Option<NativeLocalCacheReadRange>, i32> {
            self.local_cache_read_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Some(NativeLocalCacheReadRange::borrowed(
                self.local_cache_file.as_raw_fd(),
                4096,
                8192,
                9,
            )))
        }

        fn advise_read_ahead(&self, _fh: u64, _ino: u64, offset: u64, len: u64) -> Result<(), i32> {
            self.advise_read_ahead_calls.fetch_add(1, Ordering::Relaxed);
            self.last_read_ahead_offset.store(offset, Ordering::Relaxed);
            self.last_read_ahead_len.store(len, Ordering::Relaxed);
            Ok(())
        }

        fn record_native_bridge_snapshot(
            &self,
            amplification: NativeAmplificationMetricsSnapshot,
            read: NativeReadMetricsSnapshot,
        ) {
            self.native_bridge_snapshots
                .lock()
                .expect("native bridge snapshots lock should not be poisoned")
                .push((amplification, read));
        }

        fn write(
            &self,
            _fh: u64,
            _ino: u64,
            _offset: u64,
            data: &[u8],
            _flags: i32,
        ) -> Result<u32, i32> {
            Ok(data.len() as u32)
        }

        fn flush(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn fsync(&self, _ino: u64, _datasync: bool) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn opendir(&self, _ino: u64) -> Result<u64, i32> {
            unimplemented!("not used by these tests")
        }

        fn releasedir(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn readdir(&self, _ino: u64, _fh: u64, _offset: i64) -> Result<Arc<[NativeDirEntry]>, i32> {
            self.readdir_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Arc::from([dir_entry(20, b"file")]))
        }

        fn mkdir(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
        ) -> Result<NativeAttr, i32> {
            unimplemented!("not used by these tests")
        }

        fn create(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
            _flags: i32,
        ) -> Result<(NativeAttr, u64), i32> {
            unimplemented!("not used by these tests")
        }

        fn unlink(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn rmdir(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn rename(
            &self,
            _parent: u64,
            _name: &[u8],
            _new_parent: u64,
            _new_name: &[u8],
        ) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn truncate(&self, ino: u64, _fh: Option<u64>, size: u64) -> Result<NativeAttr, i32> {
            let mut attr = test_attr(ino);
            attr.size = size;
            Ok(attr)
        }

        fn statfs(&self) -> Result<NativeStatFs, i32> {
            unimplemented!("not used by these tests")
        }
    }

    fn test_attr(ino: u64) -> NativeAttr {
        NativeAttr {
            ino,
            size: 0,
            kind: NativeFileType::RegularFile,
            perm: 0o644,
            uid: 0,
            gid: 0,
            atime_ns: 0,
            mtime_ns: 0,
            ctime_ns: 0,
        }
    }

    fn dir_entry(ino: u64, name: &[u8]) -> NativeDirEntry {
        NativeDirEntry {
            ino,
            kind: NativeFileType::RegularFile,
            name: name.to_vec(),
        }
    }

    #[test]
    fn amplification_metrics_snapshot_counts_metadata_churn() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let _ = bridge.handle_lookup(lookup_request_for_name(b"file"));
        let _ = bridge.handle_getattr(getattr_request_for_ino(20));
        let _ = bridge.handle_readdir(readdir_request_for_offset_and_size(0, 4096));

        let metrics = bridge.amplification_metrics_snapshot();
        assert_eq!(metrics.metadata_ops, 3);
        assert_eq!(metrics.lookup_ops, 1);
        assert_eq!(metrics.getattr_ops, 1);
        assert_eq!(metrics.readdir_ops, 1);
        assert_eq!(metrics.fuse_ops, 3);
        assert_eq!(metrics.lookup_getattr_ratio_micros, 1_000_000);
        assert_eq!(metrics.fuse_ops_per_read_write_micros, 0);
        assert_eq!(metrics.metadata_ops_per_open_micros, 0);
        assert_eq!(vfs.lookup_calls.load(Ordering::Relaxed), 1);
        assert_eq!(vfs.getattr_calls.load(Ordering::Relaxed), 1);
        assert_eq!(vfs.readdir_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn amplification_metrics_snapshot_reports_named_amplification_ratios() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let _ = bridge.handle_lookup(lookup_request_for_name(b"file"));
        let _ = bridge.handle_getattr(getattr_request_for_ino(20));
        let _ = bridge.handle_open(open_request_for_flags(0));
        let _ = bridge.handle_read(read_request_for_size(8192));
        let _ = bridge.handle_write(write_request_for_data(20, 7, 0, b"abc"));

        let metrics = bridge.amplification_metrics_snapshot();
        assert_eq!(metrics.fuse_ops, 5);
        assert_eq!(metrics.fuse_ops_per_read_write_micros, 2_500_000);
        assert_eq!(metrics.metadata_ops_per_open_micros, 2_000_000);
    }

    #[test]
    fn amplification_metrics_snapshot_records_opcode_latency_percentiles() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let _ = bridge.handle_lookup(lookup_request_for_name(b"file"));
        let _ = bridge.handle_getattr(getattr_request_for_ino(20));
        let _ = bridge.handle_readdir(readdir_request_for_offset_and_size(0, 4096));

        let metrics = bridge.amplification_metrics_snapshot();
        assert_eq!(metrics.lookup_latency.samples, 1);
        assert_eq!(metrics.getattr_latency.samples, 1);
        assert_eq!(metrics.readdir_latency.samples, 1);
        assert!(metrics.lookup_latency.p95_ns >= 1);
        assert!(metrics.getattr_latency.p99_ns >= 1);
        assert!(metrics.metadata_latency_total_ns >= 3);
    }

    #[test]
    fn native_latency_metrics_reports_bucketed_percentiles_without_sample_lock() {
        let metrics = NativeLatencyMetrics::default();

        metrics.record(1_500);
        metrics.record(30_000);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.samples, 2);
        assert_eq!(snapshot.total_ns, 31_500);
        assert_eq!(snapshot.max_ns, 30_000);
        assert_eq!(snapshot.p95_ns, 50_000);
        assert_eq!(snapshot.p99_ns, 50_000);
    }

    #[test]
    fn native_metrics_are_cache_line_aligned_to_reduce_false_sharing() {
        assert_eq!(align_of::<NativeLatencyMetrics>(), 64);
        assert_eq!(align_of::<NativeAmplificationMetrics>(), 64);
    }

    #[test]
    fn lookup_missing_name_returns_negative_entry_with_conservative_ttl() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let reply = bridge.handle_lookup(lookup_request_for_name(b"missing"));
        let bytes = reply.to_vec();

        assert_eq!(reply_errno(&reply), 0);
        assert_eq!(entry_reply_nodeid(&bytes), 0);
        assert_eq!(entry_reply_valid_seconds(&bytes), 1);
    }

    #[test]
    fn handle_lookup_publishes_native_bridge_snapshot_to_backend() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let _ = bridge.handle_lookup(lookup_request_for_name(b"file"));

        let snapshots = vfs
            .native_bridge_snapshots
            .lock()
            .expect("native bridge snapshots lock should not be poisoned");
        let (amplification, read) = snapshots.last().expect("snapshot should be published");
        assert_eq!(amplification.lookup_ops, 1);
        assert_eq!(read.local_cache_hits, 0);
    }

    #[test]
    fn native_bridge_snapshot_publish_is_rate_limited_on_hot_path() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));

        let _ = bridge.handle_lookup(lookup_request_for_name(b"file"));
        let _ = bridge.handle_getattr(getattr_request_for_ino(20));

        let snapshots = vfs
            .native_bridge_snapshots
            .lock()
            .expect("native bridge snapshots lock should not be poisoned");
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].0.lookup_ops, 1);
        assert_eq!(bridge.amplification_metrics_snapshot().getattr_ops, 1);
    }

    #[test]
    fn native_fuse_handlers_use_metrics_guard_for_instrumented_paths() {
        let source = include_str!("vfs_adapter.rs");
        for (handler, metric, manual_record) in [
            ("handle_lookup", "Lookup", ".record_lookup("),
            ("handle_getattr", "Getattr", ".record_getattr("),
            ("handle_open", "Open", ".record_open("),
            ("handle_release", "Release", ".record_release("),
            ("handle_read", "Read", ".record_read("),
            ("handle_write", "Write", ".record_write("),
            ("handle_readdir", "Readdir", ".record_readdir("),
            ("handle_readdirplus", "Readdirplus", ".record_readdirplus("),
        ] {
            let body = source
                .split(&format!("fn {handler}(&self, request: FuseRequest)"))
                .nth(1)
                .and_then(|rest| rest.split("\n    fn ").next())
                .expect("handler body should be present");
            assert!(
                body.contains(&format!("self.metrics_guard(NativeFuseMetric::{metric})")),
                "{handler} should create a metrics guard"
            );
            assert!(
                !body.contains(manual_record),
                "{handler} should not manually record latency on each return path"
            );
            assert!(
                !body.contains("publish_native_bridge_snapshot_if_due"),
                "{handler} should publish through metrics guard drop"
            );
        }
    }

    #[test]
    fn native_open_cache_state_from_attr_maps_epoch_size_mtime_and_dirty_state() {
        let mut attr = InodeAttr::new_file(55, 1, 1000, 1001, 0o644);
        attr.size = 4096;
        attr.mtime = 123_456;
        attr.content_epoch = 9;

        let state = native_open_cache_state_from_attr(attr, true);

        assert_eq!(state.content_epoch, 9);
        assert_eq!(state.size, 4096);
        assert_eq!(state.mtime_ns, 123_456);
        assert!(state.writer_active);
        assert!(state.dirty);
        assert!(state.local_materialized);
    }

    #[test]
    fn native_attr_from_inode_attr_preserves_unix_timestamps() {
        let mut attr = InodeAttr::new_file(55, 1, 1000, 1001, 0o644);
        attr.atime = 1_234_567_890;
        attr.mtime = 2_345_678_901;
        attr.ctime = 3_456_789_012;

        let native_attr = native_attr_from_inode_attr(attr);

        assert_eq!(native_attr.atime_ns, 1_234_567_890);
        assert_eq!(native_attr.mtime_ns, 2_345_678_901);
        assert_eq!(native_attr.ctime_ns, 3_456_789_012);
    }

    #[test]
    fn append_attr_payload_writes_timestamp_seconds_and_nanoseconds() {
        let mut attr = test_attr(55);
        attr.atime_ns = 1_234_567_890;
        attr.mtime_ns = 2_345_678_901;
        attr.ctime_ns = 3_456_789_012;

        let mut payload = Vec::new();
        append_attr_payload(&mut payload, attr);

        assert_eq!(read_u64(&payload, 24), Some(1));
        assert_eq!(read_u64(&payload, 32), Some(2));
        assert_eq!(read_u64(&payload, 40), Some(3));
        assert_eq!(read_u32(&payload, 48), Some(234_567_890));
        assert_eq!(read_u32(&payload, 52), Some(345_678_901));
        assert_eq!(read_u32(&payload, 56), Some(456_789_012));
    }

    #[test]
    fn native_read_ahead_state_store_uses_multiple_shards_for_inodes() {
        assert_eq!(NativeReadAheadStateStore::SHARD_COUNT, 64);
        assert_ne!(
            NativeReadAheadStateStore::shard_index(20),
            NativeReadAheadStateStore::shard_index(21)
        );
    }

    #[test]
    fn native_read_ahead_state_store_invalidates_only_target_inode_shards() {
        let source = include_str!("vfs_adapter.rs");
        let invalidate_inodes = source
            .split("fn invalidate_inodes(&self, inodes: &[u64])")
            .nth(1)
            .and_then(|rest| rest.split("fn remove_handle").next())
            .expect("read-ahead invalidate_inodes body should be present");

        assert!(!invalidate_inodes.contains("for shard in &self.shards"));
    }

    #[test]
    fn native_read_ahead_handles_keep_common_case_inline() {
        let mut handles = NativeReadAheadHandles::new();

        for fh in 1..=4 {
            insert_read_ahead_handle(&mut handles, fh);
        }

        assert!(!handles.spilled());
    }

    #[test]
    fn native_read_ahead_state_shard_reassigns_handle_between_inodes() {
        let mut shard = NativeReadAheadStateShard::default();

        shard.track_handle_inode(7, 20);
        shard.track_handle_inode(7, 21);

        assert!(!shard.fhs_by_inode.contains_key(&20));
        assert_eq!(
            shard.fhs_by_inode.get(&21).map(|fhs| fhs.as_slice()),
            Some(&[7][..])
        );
    }

    #[test]
    fn native_read_ahead_state_shard_reassignment_drops_stale_handle_state() {
        let mut shard = NativeReadAheadStateShard::default();

        shard.track_handle_inode(7, 20);
        shard.states.entry(7).or_default();
        shard.track_handle_inode(7, 21);

        assert!(!shard.states.contains_key(&7));
    }

    #[test]
    fn native_read_ahead_state_shard_repeated_tracking_returns_before_inode_index_update() {
        let source = include_str!("vfs_adapter.rs");
        let track_handle_inode = source
            .split("fn track_handle_inode(&mut self, fh: u64, ino: u64)")
            .nth(1)
            .and_then(|rest| rest.split("fn remove_handle(&mut self, fh: u64)").next())
            .expect("track_handle_inode body should be present");
        let repeated_inode_branch = track_handle_inode
            .split("if previous_ino == Some(ino) {")
            .nth(1)
            .and_then(|rest| rest.split('}').next())
            .expect("repeated inode branch should be present");

        assert!(!repeated_inode_branch.contains("insert_read_ahead_handle"));
    }

    #[test]
    fn negative_entry_payload_reuses_static_payload_buffer() {
        let first = negative_entry_payload();
        let second = negative_entry_payload();

        assert_eq!(first.len(), FUSE_ENTRY_OUT_PAYLOAD_LEN);
        assert_eq!(first.as_ptr(), second.as_ptr());
    }

    #[test]
    fn trusted_open_cache_state_store_shard_index_is_independent() {
        let source = include_str!("vfs_adapter.rs");
        let trusted_store = source
            .split("struct NativeLookupCountStore")
            .next()
            .expect("trusted store should appear before lookup count store");

        assert!(
            !trusted_store.contains("NativeLookupCountStore::shard_index"),
            "trusted open cache state sharding must not depend on lookup count sharding"
        );
    }

    #[test]
    fn trusted_open_cache_state_store_uses_read_write_locks() {
        let source = include_str!("vfs_adapter.rs");
        let trusted_store = source
            .split("struct NativeLookupCountStore")
            .next()
            .expect("trusted store should appear before lookup count store");

        assert!(trusted_store.contains("RwLock<HashMap<u64, NativeInodeCacheState>>"));
        assert!(trusted_store.contains("read_or_recover"));
        assert!(trusted_store.contains("write_or_recover"));
    }

    #[test]
    fn keep_cache_decision_does_not_rewrite_trusted_open_cache_state() {
        let source = include_str!("vfs_adapter.rs");
        let open_cache_decision = source
            .split("fn open_cache_decision")
            .nth(1)
            .and_then(|rest| rest.split("fn apply_trusted_open_cache_state").next())
            .expect("open cache decision body should be present");

        assert!(!open_cache_decision.contains("NativeOpenCacheAction::KeepCache"));
    }

    #[test]
    fn studio_vfs_native_adapter_open_cache_state_logs_getattr_errors() {
        let source = include_str!("vfs_adapter.rs");
        let open_cache_state = source
            .split("impl NativeVfs for StudioVfsNativeAdapter")
            .nth(1)
            .and_then(|rest| rest.split("fn release").next())
            .expect("studio vfs native adapter open_cache_state should be present");

        assert!(!open_cache_state.contains(".ok()?"));
        assert!(open_cache_state.contains("warn!"));
        assert!(open_cache_state.contains("native open cache state unavailable"));
    }

    #[test]
    fn readdirplus_payload_does_not_record_lookups_when_later_getattr_fails() {
        let vfs = TestNativeVfs::default();
        let entries = vec![dir_entry(20, b"ok"), dir_entry(21, b"err")];
        let mut looked_up = Vec::new();

        let result = readdirplus_payload(&vfs, &entries, 0, 4096, |ino| looked_up.push(ino));

        assert_eq!(result, Err(libc::EIO));
        assert!(looked_up.is_empty());
    }

    #[test]
    fn handle_read_rejects_oversized_requests_without_calling_backend() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        let reply = bridge.handle_read(read_request_for_size(16 * 1024 * 1024 + 1));

        assert_eq!(reply_errno(&reply), -libc::ENOMEM);
        assert_eq!(vfs.read_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn handle_read_uses_local_cache_range_when_epoch_is_trusted() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 16 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let reply = bridge.handle_read(read_request_for_size(8192));

        assert_eq!(
            reply.local_cache_range().map(|range| range.fd),
            Some(vfs.local_cache_file.as_raw_fd())
        );
        assert_eq!(bridge.read_metrics_snapshot().local_cache_hits, 1);
        assert_eq!(vfs.local_cache_read_calls.load(Ordering::Relaxed), 1);
        assert_eq!(vfs.read_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn handle_read_triggers_bounded_readahead_for_sequential_trusted_reads() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(vfs.clone());
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 64 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let _ = bridge.handle_read(read_request_for_offset_and_size(0, 8192));
        let _ = bridge.handle_read(read_request_for_offset_and_size(8192, 8192));

        assert_eq!(vfs.advise_read_ahead_calls.load(Ordering::Relaxed), 1);
        assert_eq!(vfs.last_read_ahead_offset.load(Ordering::Relaxed), 16_384);
        assert_eq!(
            vfs.last_read_ahead_len.load(Ordering::Relaxed),
            64 * 1024 - 16_384
        );
    }

    #[test]
    fn mutation_invalidation_keeps_readahead_state_for_unrelated_inode() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(vfs);
        for ino in [20, 21] {
            bridge.remember_trusted_open_cache_state(
                ino,
                &NativeInodeCacheState {
                    content_epoch: 9,
                    size: 64 * 1024,
                    mtime_ns: 100,
                    invalidation_generation: 0,
                    trusted: None,
                    writer_active: false,
                    dirty: false,
                    local_materialized: true,
                },
            );
        }

        let _ = bridge.handle_read(read_request_for_ino_fh_offset_and_size(20, 7, 0, 8192));
        let _ = bridge.handle_read(read_request_for_ino_fh_offset_and_size(21, 8, 0, 8192));

        bridge.invalidate_metadata_cache_for_mutation(&[20]);

        assert!(!bridge.read_ahead_states.contains_handle(20, 7));
        assert!(bridge.read_ahead_states.contains_handle(21, 8));
        assert_eq!(bridge.read_ahead_states.inode_for_handle(20, 7), None);
        assert_eq!(bridge.read_ahead_states.inode_for_handle(21, 8), Some(21));
    }

    #[test]
    fn read_metrics_snapshot_reports_sparse_random_readahead_suppression() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(vfs.clone());
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 512 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let _ = bridge.handle_read(read_request_for_offset_and_size(0, 8192));
        let _ = bridge.handle_read(read_request_for_offset_and_size(256 * 1024, 8192));
        let random_metrics = bridge.read_metrics_snapshot();
        assert_eq!(random_metrics.read_ahead_prefetches, 0);
        assert_eq!(random_metrics.read_ahead_suppressed, 2);

        let _ = bridge.handle_read(read_request_for_offset_and_size(264 * 1024, 8192));
        let sequential_metrics = bridge.read_metrics_snapshot();
        assert_eq!(sequential_metrics.read_ahead_prefetches, 1);
        assert!(sequential_metrics.read_ahead_amplification_bytes > 0);
    }

    #[test]
    fn handle_read_falls_back_to_bytes_when_local_cache_epoch_is_stale() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 10,
                size: 16 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let reply = bridge.handle_read(read_request_for_size(8192));

        assert!(reply.local_cache_range().is_none());
        let metrics = bridge.read_metrics_snapshot();
        assert_eq!(metrics.stale_epoch_fallbacks, 1);
        assert_eq!(metrics.bytes_fallbacks, 1);
        assert_eq!(vfs.local_cache_read_calls.load(Ordering::Relaxed), 1);
        assert_eq!(vfs.read_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_read_falls_back_to_bytes_when_remote_refresh_generation_changed() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 16 * 1024,
                mtime_ns: 100,
                invalidation_generation: 7,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let reply = bridge.handle_read(read_request_for_size(8192));

        assert!(reply.local_cache_range().is_none());
        assert_eq!(bridge.read_metrics_snapshot().stale_generation_fallbacks, 1);
        assert_eq!(bridge.read_metrics_snapshot().bytes_fallbacks, 1);
    }

    #[test]
    fn setattr_truncate_invalidates_trusted_open_cache_state_before_followup_read() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 16 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let _ = bridge.handle_setattr(setattr_size_request(20, 4096));
        let reply = bridge.handle_read(read_request_for_size(8192));

        assert!(reply.local_cache_range().is_none());
        assert_eq!(bridge.read_metrics_snapshot().bytes_fallbacks, 1);
    }

    #[test]
    fn handle_write_overwrite_invalidates_trusted_open_cache_state_before_followup_read() {
        let vfs = Arc::new(TestNativeVfs::default());
        let bridge = LinuxNativeBridge::new(Arc::clone(&vfs));
        bridge.remember_trusted_open_cache_state(
            20,
            &NativeInodeCacheState {
                content_epoch: 9,
                size: 16 * 1024,
                mtime_ns: 100,
                invalidation_generation: 0,
                trusted: None,
                writer_active: false,
                dirty: false,
                local_materialized: true,
            },
        );

        let _ = bridge.handle_write(write_request_for_data(20, 7, 4096, b"overwrite"));
        let reply = bridge.handle_read(read_request_for_size(8192));

        assert!(reply.local_cache_range().is_none());
        assert_eq!(bridge.read_metrics_snapshot().bytes_fallbacks, 1);
    }

    fn read_request_for_size(size: u32) -> FuseRequest {
        read_request_for_offset_and_size(13, size)
    }

    fn read_request_for_offset_and_size(offset: u64, size: u32) -> FuseRequest {
        read_request_for_ino_fh_offset_and_size(20, 7, offset, size)
    }

    fn read_request_for_ino_fh_offset_and_size(
        ino: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseRequest {
        let mut payload = vec![0; FUSE_READ_IN_LEN];
        payload[0..8].copy_from_slice(&fh.to_le_bytes());
        payload[8..16].copy_from_slice(&offset.to_le_bytes());
        payload[16..20].copy_from_slice(&size.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                40 + FUSE_READ_IN_LEN as u32,
                15,
                99,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Read,
            payload,
        }
    }

    fn lookup_request_for_name(name: &[u8]) -> FuseRequest {
        let mut payload = name.to_vec();
        payload.push(0);
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (crate::native::abi::FUSE_IN_HEADER_LEN + payload.len()) as u32,
                1,
                100,
                1,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Lookup,
            payload,
        }
    }

    fn getattr_request_for_ino(ino: u64) -> FuseRequest {
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                crate::native::abi::FUSE_IN_HEADER_LEN as u32,
                3,
                101,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Getattr,
            payload: Vec::new(),
        }
    }

    fn open_request_for_flags(flags: u32) -> FuseRequest {
        let mut payload = vec![0; FUSE_OPEN_IN_LEN];
        payload[0..4].copy_from_slice(&flags.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                40 + FUSE_OPEN_IN_LEN as u32,
                14,
                104,
                20,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Open,
            payload,
        }
    }

    fn readdir_request_for_offset_and_size(offset: u64, size: u32) -> FuseRequest {
        let mut payload = vec![0; FUSE_READ_IN_LEN];
        payload[0..8].copy_from_slice(&7_u64.to_le_bytes());
        payload[8..16].copy_from_slice(&offset.to_le_bytes());
        payload[16..20].copy_from_slice(&size.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                40 + FUSE_READ_IN_LEN as u32,
                28,
                102,
                1,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Readdir,
            payload,
        }
    }

    fn setattr_size_request(ino: u64, size: u64) -> FuseRequest {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        let valid = FATTR_SIZE;
        payload[0..4].copy_from_slice(&valid.to_le_bytes());
        payload[8..16].copy_from_slice(&size.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                40 + FUSE_SETATTR_IN_LEN as u32,
                4,
                103,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Setattr,
            payload,
        }
    }

    #[derive(Debug)]
    struct BlockingWriteVfs {
        write_started: Mutex<Option<std::sync::mpsc::Sender<()>>>,
        release_write: Mutex<std::sync::mpsc::Receiver<()>>,
        write_error: Option<i32>,
        release_error: Option<i32>,
        snapshots: Mutex<Vec<NativeAsyncWriteSnapshot>>,
        flushes: AtomicU64,
        fsyncs: AtomicU64,
        releases: AtomicU64,
        truncates: AtomicU64,
    }

    struct BlockingWriteFixture {
        vfs: Arc<BlockingWriteVfs>,
        bridge: Arc<LinuxNativeBridge<Arc<BlockingWriteVfs>>>,
        write_started: std::sync::mpsc::Receiver<()>,
        release_write: std::sync::mpsc::Sender<()>,
    }

    fn blocking_write_fixture() -> BlockingWriteFixture {
        blocking_write_fixture_with_write_result(None)
    }

    fn blocking_write_fixture_with_write_error(errno: i32) -> BlockingWriteFixture {
        blocking_write_fixture_with_write_result(Some(errno))
    }

    fn blocking_write_fixture_with_write_result(write_error: Option<i32>) -> BlockingWriteFixture {
        blocking_write_fixture_with_results(write_error, None)
    }

    fn blocking_write_fixture_with_release_error(errno: i32) -> BlockingWriteFixture {
        blocking_write_fixture_with_results(None, Some(errno))
    }

    fn blocking_write_fixture_with_results(
        write_error: Option<i32>,
        release_error: Option<i32>,
    ) -> BlockingWriteFixture {
        let (write_started_tx, write_started) = std::sync::mpsc::channel();
        let (release_write, release_write_rx) = std::sync::mpsc::channel();
        let vfs = Arc::new(BlockingWriteVfs {
            write_started: Mutex::new(Some(write_started_tx)),
            release_write: Mutex::new(release_write_rx),
            write_error,
            release_error,
            snapshots: Mutex::new(Vec::new()),
            flushes: AtomicU64::new(0),
            fsyncs: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            truncates: AtomicU64::new(0),
        });
        let bridge = Arc::new(LinuxNativeBridge::new(Arc::clone(&vfs)));

        BlockingWriteFixture {
            vfs,
            bridge,
            write_started,
            release_write,
        }
    }

    impl NativeVfs for BlockingWriteVfs {
        fn lookup(&self, _parent: u64, _name: &[u8]) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn getattr(&self, _ino: u64) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn open(&self, _ino: u64, _flags: i32) -> Result<u64, i32> {
            unimplemented!("not used by this test")
        }

        fn release(&self, _fh: u64) -> Result<(), i32> {
            self.releases.fetch_add(1, Ordering::Relaxed);
            if let Some(errno) = self.release_error {
                return Err(errno);
            }
            Ok(())
        }

        fn read(&self, _fh: u64, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, i32> {
            unimplemented!("not used by this test")
        }

        fn write(
            &self,
            _fh: u64,
            _ino: u64,
            _offset: u64,
            data: &[u8],
            _flags: i32,
        ) -> Result<u32, i32> {
            if let Some(started) = self
                .write_started
                .lock()
                .expect("write_started lock should not be poisoned")
                .take()
            {
                started.send(()).expect("test should observe write start");
            }
            self.release_write
                .lock()
                .expect("release_write lock should not be poisoned")
                .recv()
                .expect("test should release blocked write");
            if let Some(errno) = self.write_error {
                return Err(errno);
            }
            Ok(data.len() as u32)
        }

        fn record_async_write_snapshot(&self, snapshot: NativeAsyncWriteSnapshot) {
            self.snapshots
                .lock()
                .expect("snapshots lock should not be poisoned")
                .push(snapshot);
        }

        fn flush(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn fsync(&self, _ino: u64, _datasync: bool) -> Result<(), i32> {
            self.fsyncs.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn opendir(&self, _ino: u64) -> Result<u64, i32> {
            unimplemented!("not used by this test")
        }

        fn releasedir(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn readdir(&self, _ino: u64, _fh: u64, _offset: i64) -> Result<Arc<[NativeDirEntry]>, i32> {
            unimplemented!("not used by this test")
        }

        fn mkdir(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
        ) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn create(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
            _flags: i32,
        ) -> Result<(NativeAttr, u64), i32> {
            unimplemented!("not used by this test")
        }

        fn unlink(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn rmdir(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn rename(
            &self,
            _parent: u64,
            _name: &[u8],
            _new_parent: u64,
            _new_name: &[u8],
        ) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn truncate(&self, ino: u64, _fh: Option<u64>, size: u64) -> Result<NativeAttr, i32> {
            self.truncates.fetch_add(1, Ordering::Relaxed);
            let mut attr = test_attr(ino);
            attr.size = size;
            Ok(attr)
        }

        fn statfs(&self) -> Result<NativeStatFs, i32> {
            unimplemented!("not used by this test")
        }
    }

    #[test]
    fn handle_flush_waits_for_inflight_write_on_same_inode() {
        let fixture = blocking_write_fixture();
        let writer_bridge = Arc::clone(&fixture.bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("write should enter backend before barrier starts");

        let (flush_done_tx, flush_done_rx) = std::sync::mpsc::channel();
        let flush_bridge = Arc::clone(&fixture.bridge);
        let flusher = std::thread::spawn(move || {
            let reply = flush_bridge.handle_flush(flush_request_for_fh(20, 7));
            flush_done_tx
                .send(reply_errno(&reply))
                .expect("test should observe flush result");
        });

        assert!(
            flush_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "flush must not cross an inflight write on the same inode"
        );

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
        writer.join().expect("write thread should finish");
        assert_eq!(
            flush_done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("flush should finish after write drains"),
            0
        );
        flusher.join().expect("flush thread should finish");
        assert_eq!(fixture.vfs.flushes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_fsync_waits_for_inflight_write_on_same_inode() {
        let fixture = blocking_write_fixture();
        let writer_bridge = Arc::clone(&fixture.bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("write should enter backend before barrier starts");

        let (fsync_done_tx, fsync_done_rx) = std::sync::mpsc::channel();
        let fsync_bridge = Arc::clone(&fixture.bridge);
        let fsyncer = std::thread::spawn(move || {
            let reply = fsync_bridge.handle_fsync(fsync_request_for_datasync(20, true));
            fsync_done_tx
                .send(reply_errno(&reply))
                .expect("test should observe fsync result");
        });

        assert!(
            fsync_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "fsync must not cross an inflight write on the same inode"
        );

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
        writer.join().expect("write thread should finish");
        assert_eq!(
            fsync_done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("fsync should finish after write drains"),
            0
        );
        fsyncer.join().expect("fsync thread should finish");
        assert_eq!(fixture.vfs.fsyncs.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_release_waits_for_inflight_write_on_same_inode() {
        let fixture = blocking_write_fixture();
        let writer_bridge = Arc::clone(&fixture.bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("write should enter backend before barrier starts");

        let (release_done_tx, release_done_rx) = std::sync::mpsc::channel();
        let release_bridge = Arc::clone(&fixture.bridge);
        let releaser = std::thread::spawn(move || {
            let reply = release_bridge.handle_release(release_request_for_fh(20, 7));
            release_done_tx
                .send(reply_errno(&reply))
                .expect("test should observe release result");
        });

        assert!(
            release_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "release must not cross an inflight write on the same inode"
        );

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
        writer.join().expect("write thread should finish");
        assert_eq!(
            release_done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("release should finish after write drains"),
            0
        );
        releaser.join().expect("release thread should finish");
        assert_eq!(fixture.vfs.releases.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_release_short_circuits_eagain_before_releasing_vfs_handle() {
        let source = include_str!("vfs_adapter.rs");
        let body = source
            .split("fn handle_release(&self, request: FuseRequest)")
            .nth(1)
            .and_then(|rest| {
                rest.split("fn handle_read(&self, request: FuseRequest)")
                    .next()
            })
            .expect("handle_release body should be present");
        let eagain_guard = body
            .find("pending_result == Err(libc::EAGAIN)")
            .expect("handle_release should check EAGAIN from drain");
        let vfs_release = body
            .find("self.vfs.release(fh)")
            .expect("handle_release should release VFS handle after successful drain");
        let finish_close = body
            .find("self.pending_writes.finish_close")
            .expect("handle_release should finish close after releasing VFS handle");

        assert!(eagain_guard < vfs_release);
        assert!(eagain_guard < finish_close);
    }

    #[test]
    fn handle_release_forgets_read_ahead_handle_after_release_error() {
        let fixture = blocking_write_fixture_with_release_error(libc::EIO);

        fixture.bridge.read_ahead_states.observe(
            &fixture.bridge.read_ahead_policy,
            7,
            20,
            0,
            8192,
            16 * 1024,
        );
        let reply = fixture.bridge.handle_release(release_request_for_fh(20, 7));

        assert_eq!(reply_errno(&reply), -libc::EIO);
        assert!(!fixture.bridge.read_ahead_states.contains_handle(20, 7));
    }

    #[test]
    fn handle_release_with_async_write_forgets_read_ahead_handle_after_release_error() {
        let fixture = blocking_write_fixture_with_release_error(libc::EIO);
        let bridge = LinuxNativeBridge::new(Arc::clone(&fixture.vfs)).with_async_write_for_test(
            NativeAsyncWriteConfig {
                max_pending_bytes: Some(16),
                max_pending_writes: Some(2),
                per_handle_pending_bytes: Some(8),
                platform_policy: "test-native".into(),
            },
        );

        bridge
            .read_ahead_states
            .observe(&bridge.read_ahead_policy, 7, 20, 0, 8192, 16 * 1024);
        let reply = bridge.handle_release(release_request_for_fh(20, 7));

        assert_eq!(reply_errno(&reply), -libc::EIO);
        assert!(!bridge.read_ahead_states.contains_handle(20, 7));
    }

    #[test]
    fn handle_setattr_truncate_waits_for_inflight_write_on_same_inode() {
        let fixture = blocking_write_fixture();
        let writer_bridge = Arc::clone(&fixture.bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("write should enter backend before truncate starts");

        let (truncate_done_tx, truncate_done_rx) = std::sync::mpsc::channel();
        let truncate_bridge = Arc::clone(&fixture.bridge);
        let truncater = std::thread::spawn(move || {
            let reply = truncate_bridge.handle_setattr(truncate_request_for_size(20, 7, 3));
            truncate_done_tx
                .send(reply_errno(&reply))
                .expect("test should observe truncate result");
        });

        assert!(
            truncate_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "truncate must not cross an inflight write on the same inode"
        );

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
        writer.join().expect("write thread should finish");
        assert_eq!(
            truncate_done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("truncate should finish after write drains"),
            0
        );
        truncater.join().expect("truncate thread should finish");
        assert_eq!(fixture.vfs.truncates.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn handle_write_waits_for_async_admission_when_pending_bytes_are_full() {
        let fixture = blocking_write_fixture();
        let bridge = Arc::new(
            LinuxNativeBridge::new(Arc::clone(&fixture.vfs)).with_async_write_for_test(
                NativeAsyncWriteConfig {
                    max_pending_bytes: Some(1),
                    max_pending_writes: None,
                    per_handle_pending_bytes: None,
                    platform_policy: "test-native".into(),
                },
            ),
        );
        let writer_bridge = Arc::clone(&bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("first async write should enter backend and hold admission bytes");

        let (second_done_tx, second_done_rx) = std::sync::mpsc::channel();
        let second_bridge = Arc::clone(&bridge);
        let second = std::thread::spawn(move || {
            let reply = second_bridge.handle_write(write_request_for_data(20, 8, 7, b"blocked"));
            second_done_tx
                .send(reply_errno(&reply))
                .expect("test should observe second write reply");
        });

        assert!(
            second_done_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "second blocking write must wait for admission instead of returning EAGAIN"
        );

        fixture
            .release_write
            .send(())
            .expect("test should unblock first write");
        writer.join().expect("write thread should finish");
        assert_eq!(
            second_done_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("second write should be admitted after first write completes"),
            0
        );
        fixture
            .release_write
            .send(())
            .expect("test should unblock second write");
        second.join().expect("second write thread should finish");
    }

    #[test]
    fn handle_async_write_uses_shared_executor_instead_of_per_write_threads() {
        let source = include_str!("vfs_adapter.rs");
        let production = source
            .split("#[cfg(test)]")
            .next()
            .expect("production source should precede tests");
        let body = source
            .split("fn handle_async_write(")
            .nth(1)
            .and_then(|rest| rest.split("fn handle_flush(").next())
            .expect("handle_async_write body should be present");

        assert!(!body.contains("std::thread::spawn"));
        assert!(!body.contains("new_current_thread"));
        assert!(!production.contains("NativeAsyncWriteExecutor"));
        assert!(!production.contains("native_async_write_executor"));
        assert!(body.contains("spawn_native_async_write_job"));
    }

    #[test]
    fn studio_vfs_async_write_waits_for_turn_before_entering_blocking_pool() {
        let source = include_str!("vfs_adapter.rs");
        let studio_spawn = source
            .split("impl NativeVfs for StudioVfsNativeAdapter")
            .nth(1)
            .and_then(|rest| rest.split("fn flush(").next())
            .expect("studio native vfs impl should be present");
        let async_write_body = source
            .split("fn handle_async_write(")
            .nth(1)
            .and_then(|rest| rest.split("fn handle_flush(").next())
            .expect("handle_async_write body should be present");
        let wait_pos = studio_spawn
            .find("turn.wait().await")
            .expect("studio adapter should wait for turn asynchronously");
        let spawn_blocking_pos = studio_spawn
            .find("spawn_blocking")
            .expect("studio adapter should run backend write in blocking pool after turn");

        assert!(!async_write_body.contains("wait_blocking"));
        assert!(wait_pos < spawn_blocking_pos);
    }

    #[test]
    fn handle_fsync_logs_failures_through_tracing() {
        let source = include_str!("vfs_adapter.rs");
        let body = source
            .split("fn handle_fsync(")
            .nth(1)
            .and_then(|rest| rest.split("fn handle_opendir(").next())
            .expect("handle_fsync body should be present");

        assert!(!body.contains("eprintln!"));
        assert!(body.contains("warn!"));
    }

    #[test]
    fn async_write_snapshot_reports_native_admission_state() {
        let fixture = blocking_write_fixture();
        let bridge = Arc::new(
            LinuxNativeBridge::new(Arc::clone(&fixture.vfs)).with_async_write_for_test(
                NativeAsyncWriteConfig {
                    max_pending_bytes: Some(16),
                    max_pending_writes: Some(2),
                    per_handle_pending_bytes: Some(8),
                    platform_policy: "test-native".into(),
                },
            ),
        );
        let writer_bridge = Arc::clone(&bridge);

        let writer = std::thread::spawn(move || {
            writer_bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        });
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("async write should enter backend and hold pending bytes");

        let snapshot = bridge.async_write_snapshot();

        assert!(snapshot.enabled);
        assert_eq!(snapshot.platform_policy.as_ref(), "test-native");
        assert_eq!(snapshot.max_pending_bytes, Some(16));
        assert_eq!(snapshot.max_pending_writes, Some(2));
        assert_eq!(snapshot.per_handle_pending_bytes, Some(8));
        assert_eq!(snapshot.pending_writes, 1);
        assert_eq!(snapshot.pending_bytes, b"pending".len());
        assert_eq!(snapshot.peak_pending_bytes, b"pending".len());

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
        writer.join().expect("write thread should finish");
    }

    #[test]
    fn native_async_write_snapshot_reports_completion_before_registration_drop() {
        let pending_writes = PendingWriteTracker::default();
        let config = NativeAsyncWriteConfig {
            max_pending_bytes: Some(16),
            max_pending_writes: Some(2),
            per_handle_pending_bytes: Some(8),
            platform_policy: "test-native".into(),
        };
        let registration = pending_writes
            .register_with_admission(20, 7, 4, config.admission_limits())
            .expect("registration should fit admission limits");

        registration.complete(PendingWriteCompletion::Success);
        let snapshot = native_async_write_snapshot(&pending_writes, &config);

        assert_eq!(snapshot.pending_writes, 0);
        assert_eq!(snapshot.pending_bytes, 0);
        assert_eq!(snapshot.completed_writes, 1);
    }

    #[test]
    fn native_async_write_config_uses_runtime_async_write_profile() {
        let disabled = NativeAsyncWriteConfig::from_runtime_options(&StudioFuseRuntimeOptions {
            async_write_enabled: false,
            async_read_enabled: false,
            async_metadata_enabled: false,
            linux_async_dio_enabled: false,
            linux_parallel_dirops_enabled: false,
            linux_readdirplus_auto_enabled: true,
            async_write_admission_limits: Some(StudioFuseAsyncWriteAdmissionLimits {
                max_pending_bytes: Some(4096),
                max_pending_writes: Some(7),
                per_handle_pending_bytes: Some(1024),
            }),
            async_read_admission_limits: None,
        });
        assert_eq!(disabled, None);

        let enabled = NativeAsyncWriteConfig::from_runtime_options(&StudioFuseRuntimeOptions {
            async_write_enabled: true,
            async_read_enabled: false,
            async_metadata_enabled: false,
            linux_async_dio_enabled: false,
            linux_parallel_dirops_enabled: false,
            linux_readdirplus_auto_enabled: true,
            async_write_admission_limits: Some(StudioFuseAsyncWriteAdmissionLimits {
                max_pending_bytes: Some(4096),
                max_pending_writes: Some(7),
                per_handle_pending_bytes: Some(1024),
            }),
            async_read_admission_limits: None,
        })
        .expect("enabled runtime async write should produce native config");

        assert_eq!(enabled.max_pending_bytes, Some(4096));
        assert_eq!(enabled.max_pending_writes, Some(7));
        assert_eq!(enabled.per_handle_pending_bytes, Some(1024));
        assert_eq!(enabled.platform_policy.as_ref(), "linux6-safe");
    }

    #[test]
    fn native_async_write_default_pending_writes_stays_below_tokio_blocking_pool_limit() {
        let limits = native_async_write_default_limits();

        assert!(limits.max_pending_writes.is_some_and(|limit| limit < 512));
    }

    #[test]
    fn native_async_write_policy_fields_use_cheap_clone_storage() {
        let source = include_str!("vfs_adapter.rs");
        let native_async_types = source
            .split("pub struct NativeAsyncWriteConfig")
            .nth(1)
            .and_then(|rest| rest.split("impl NativeAsyncWriteConfig").next())
            .expect("native async write type definitions should be present");

        assert!(!native_async_types.contains("platform_policy: String"));
        assert!(!native_async_types.contains("platform_policy: Arc<str>"));
        assert!(native_async_types.contains("platform_policy: Cow<'static, str>"));

        let studio_record_snapshot = source
            .split("fn record_async_write_snapshot(&self, snapshot: NativeAsyncWriteSnapshot)")
            .nth(1)
            .and_then(|rest| rest.split("fn spawn_native_async_write_job").next())
            .expect("studio snapshot recording method should be present");
        assert!(!studio_record_snapshot.contains("to_string()"));
    }

    #[test]
    fn handle_write_async_publishes_snapshot_to_backend() {
        let fixture = blocking_write_fixture();
        let bridge = LinuxNativeBridge::new(Arc::clone(&fixture.vfs)).with_async_write_for_test(
            NativeAsyncWriteConfig {
                max_pending_bytes: Some(16),
                max_pending_writes: Some(2),
                per_handle_pending_bytes: Some(8),
                platform_policy: "test-native".into(),
            },
        );

        bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("async write should enter backend and hold pending bytes");

        let snapshots = fixture
            .vfs
            .snapshots
            .lock()
            .expect("snapshots lock should not be poisoned");
        let snapshot = snapshots
            .last()
            .expect("native bridge should publish async write snapshot");

        assert!(snapshot.enabled);
        assert_eq!(snapshot.pending_writes, 1);
        assert_eq!(snapshot.pending_bytes, b"pending".len());
        drop(snapshots);

        fixture
            .release_write
            .send(())
            .expect("test should unblock write");
    }

    #[test]
    fn handle_flush_surfaces_failed_async_write_error() {
        let fixture = blocking_write_fixture_with_write_error(libc::EIO);
        let bridge = LinuxNativeBridge::new(Arc::clone(&fixture.vfs)).with_async_write_for_test(
            NativeAsyncWriteConfig {
                max_pending_bytes: Some(16),
                max_pending_writes: Some(2),
                per_handle_pending_bytes: Some(8),
                platform_policy: "test-native".into(),
            },
        );

        bridge.handle_write(write_request_for_data(20, 7, 0, b"pending"));
        fixture
            .write_started
            .recv_timeout(std::time::Duration::from_millis(250))
            .expect("async write should enter backend");
        fixture
            .release_write
            .send(())
            .expect("test should unblock failed write");

        let reply = bridge.handle_flush(flush_request_for_fh(20, 7));

        assert_eq!(reply_errno(&reply), -libc::EIO);
    }

    #[derive(Debug)]
    struct SequencedWriteVfs {
        started: Mutex<std::sync::mpsc::Sender<u64>>,
        release_first: Mutex<std::sync::mpsc::Receiver<()>>,
        calls: AtomicU64,
    }

    impl NativeVfs for SequencedWriteVfs {
        fn lookup(&self, _parent: u64, _name: &[u8]) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn getattr(&self, _ino: u64) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn open(&self, _ino: u64, _flags: i32) -> Result<u64, i32> {
            unimplemented!("not used by this test")
        }

        fn release(&self, _fh: u64) -> Result<(), i32> {
            Ok(())
        }

        fn read(&self, _fh: u64, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, i32> {
            unimplemented!("not used by this test")
        }

        fn write(
            &self,
            _fh: u64,
            _ino: u64,
            offset: u64,
            data: &[u8],
            _flags: i32,
        ) -> Result<u32, i32> {
            self.started
                .lock()
                .expect("started sender lock should not be poisoned")
                .send(offset)
                .expect("test should observe write start");
            if self.calls.fetch_add(1, Ordering::Relaxed) == 0 {
                self.release_first
                    .lock()
                    .expect("release_first lock should not be poisoned")
                    .recv()
                    .expect("test should release first write");
            }
            Ok(data.len() as u32)
        }

        fn flush(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            Ok(())
        }

        fn fsync(&self, _ino: u64, _datasync: bool) -> Result<(), i32> {
            Ok(())
        }

        fn opendir(&self, _ino: u64) -> Result<u64, i32> {
            unimplemented!("not used by this test")
        }

        fn releasedir(&self, _ino: u64, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn readdir(&self, _ino: u64, _fh: u64, _offset: i64) -> Result<Arc<[NativeDirEntry]>, i32> {
            unimplemented!("not used by this test")
        }

        fn mkdir(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
        ) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn create(
            &self,
            _parent: u64,
            _name: &[u8],
            _uid: u32,
            _gid: u32,
            _mode: u16,
            _flags: i32,
        ) -> Result<(NativeAttr, u64), i32> {
            unimplemented!("not used by this test")
        }

        fn unlink(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn rmdir(&self, _parent: u64, _name: &[u8]) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn rename(
            &self,
            _parent: u64,
            _name: &[u8],
            _new_parent: u64,
            _new_name: &[u8],
        ) -> Result<(), i32> {
            unimplemented!("not used by this test")
        }

        fn truncate(&self, _ino: u64, _fh: Option<u64>, _size: u64) -> Result<NativeAttr, i32> {
            unimplemented!("not used by this test")
        }

        fn statfs(&self) -> Result<NativeStatFs, i32> {
            unimplemented!("not used by this test")
        }
    }

    #[test]
    fn handle_write_async_serializes_writes_for_same_handle() {
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_first_tx, release_first_rx) = std::sync::mpsc::channel();
        let vfs = Arc::new(SequencedWriteVfs {
            started: Mutex::new(started_tx),
            release_first: Mutex::new(release_first_rx),
            calls: AtomicU64::new(0),
        });
        let bridge = Arc::new(
            LinuxNativeBridge::new(Arc::clone(&vfs)).with_async_write_for_test(
                NativeAsyncWriteConfig {
                    max_pending_bytes: Some(1024),
                    max_pending_writes: Some(4),
                    per_handle_pending_bytes: Some(1024),
                    platform_policy: "test-native".into(),
                },
            ),
        );

        bridge.handle_write(write_request_for_data(20, 7, 0, b"first"));
        assert_eq!(
            started_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("first write should enter backend"),
            0
        );
        bridge.handle_write(write_request_for_data(20, 7, 5, b"second"));

        assert!(
            started_rx
                .recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "second write on same handle must wait for the first write turn"
        );

        release_first_tx
            .send(())
            .expect("test should unblock first write");
        assert_eq!(
            started_rx
                .recv_timeout(std::time::Duration::from_millis(250))
                .expect("second write should enter backend after first completes"),
            5
        );
    }

    fn write_request_for_data(ino: u64, fh: u64, offset: u64, data: &[u8]) -> FuseRequest {
        let mut payload = vec![0; FUSE_WRITE_IN_LEN + data.len()];
        payload[0..8].copy_from_slice(&fh.to_le_bytes());
        payload[8..16].copy_from_slice(&offset.to_le_bytes());
        payload[16..20].copy_from_slice(&(data.len() as u32).to_le_bytes());
        payload[FUSE_WRITE_IN_LEN..].copy_from_slice(data);
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (40 + payload.len()) as u32,
                16,
                100,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Write,
            payload,
        }
    }

    fn fsync_request_for_datasync(ino: u64, datasync: bool) -> FuseRequest {
        let mut payload = vec![0; FUSE_FSYNC_IN_LEN];
        payload[8..12].copy_from_slice(&(u32::from(datasync)).to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (40 + FUSE_FSYNC_IN_LEN) as u32,
                20,
                102,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Fsync,
            payload,
        }
    }

    fn release_request_for_fh(ino: u64, fh: u64) -> FuseRequest {
        let mut payload = vec![0; FUSE_RELEASE_IN_LEN];
        payload[0..8].copy_from_slice(&fh.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (40 + FUSE_RELEASE_IN_LEN) as u32,
                18,
                103,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Release,
            payload,
        }
    }

    fn flush_request_for_fh(ino: u64, fh: u64) -> FuseRequest {
        let mut payload = vec![0; FUSE_FLUSH_IN_LEN];
        payload[0..8].copy_from_slice(&fh.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (40 + FUSE_FLUSH_IN_LEN) as u32,
                25,
                101,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Flush,
            payload,
        }
    }

    fn truncate_request_for_size(ino: u64, fh: u64, size: u64) -> FuseRequest {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        payload[0..4].copy_from_slice(&(FATTR_SIZE | FATTR_FH).to_le_bytes());
        payload[8..16].copy_from_slice(&fh.to_le_bytes());
        payload[16..24].copy_from_slice(&size.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                (40 + FUSE_SETATTR_IN_LEN) as u32,
                25,
                104,
                ino,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Setattr,
            payload,
        }
    }

    fn reply_errno(reply: &FuseReply) -> i32 {
        i32::from_le_bytes(reply.to_vec()[4..8].try_into().expect("reply errno"))
    }

    fn entry_reply_nodeid(reply: &[u8]) -> u64 {
        u64::from_le_bytes(
            reply[crate::native::abi::FUSE_OUT_HEADER_LEN
                ..crate::native::abi::FUSE_OUT_HEADER_LEN + 8]
                .try_into()
                .expect("entry nodeid"),
        )
    }

    fn entry_reply_valid_seconds(reply: &[u8]) -> u64 {
        u64::from_le_bytes(
            reply[crate::native::abi::FUSE_OUT_HEADER_LEN + 16
                ..crate::native::abi::FUSE_OUT_HEADER_LEN + 24]
                .try_into()
                .expect("entry valid"),
        )
    }

    #[test]
    fn lookup_count_store_uses_multiple_shards_for_distinct_inodes() {
        assert_eq!(NativeLookupCountStore::SHARD_COUNT, 64);
        assert_ne!(
            NativeLookupCountStore::shard_index(20),
            NativeLookupCountStore::shard_index(21)
        );
    }

    #[test]
    fn lookup_count_store_tracks_counts_across_shards() {
        let counts = NativeLookupCountStore::default();

        counts.add(20, 3);
        counts.add(21, 5);
        counts.forget(20, 1);

        assert_eq!(counts.count(20), 2);
        assert_eq!(counts.count(21), 5);
    }

    #[test]
    fn directory_handle_store_release_keeps_mismatched_inode_handle() {
        let store = NativeDirectoryHandleStore::default();
        let fh = store.open(20, Arc::from([dir_entry(21, b"child")]));

        assert!(!store.release(21, fh));
        assert!(store.entries(20, fh).is_some());
        assert!(store.release(20, fh));
    }

    #[test]
    fn directory_handle_store_entries_rejects_unknown_or_mismatched_handles() {
        let store = NativeDirectoryHandleStore::default();
        let fh = store.open(20, Arc::from([dir_entry(21, b"child")]));

        assert!(store.entries(20, fh + 1).is_none());
        assert!(store.entries(21, fh).is_none());
    }

    #[test]
    fn read_u32_returns_none_when_range_exceeds_frame() {
        assert_eq!(read_u32(&[1, 2, 3], 0), None);
    }

    #[test]
    fn read_u64_returns_none_when_range_exceeds_frame() {
        assert_eq!(read_u64(&[1, 2, 3, 4, 5, 6, 7], 0), None);
    }

    #[test]
    fn truncate_request_accepts_linux_fattr_fh_bit() {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        payload[0..4].copy_from_slice(&(FATTR_SIZE | (1 << 6)).to_le_bytes());
        payload[8..16].copy_from_slice(&77_u64.to_le_bytes());
        payload[16..24].copy_from_slice(&64_u64.to_le_bytes());

        let request = truncate_request(&payload).expect("truncate with fh should parse");

        assert_eq!(
            request,
            TruncateRequest {
                fh: Some(77),
                size: 64
            }
        );
    }

    #[test]
    fn truncate_request_ignores_linux_fattr_lockowner_bit() {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        payload[0..4].copy_from_slice(&(FATTR_SIZE | FATTR_FH | (1 << 9)).to_le_bytes());
        payload[8..16].copy_from_slice(&77_u64.to_le_bytes());
        payload[16..24].copy_from_slice(&64_u64.to_le_bytes());

        let request = truncate_request(&payload).expect("truncate with lock owner should parse");

        assert_eq!(
            request,
            TruncateRequest {
                fh: Some(77),
                size: 64
            }
        );
    }

    #[test]
    fn truncate_request_ignores_writeback_cache_timestamp_bits() {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        payload[0..4].copy_from_slice(
            &(FATTR_SIZE | FATTR_FH | (1 << 5) | (1 << 9) | (1 << 10)).to_le_bytes(),
        );
        payload[8..16].copy_from_slice(&77_u64.to_le_bytes());
        payload[16..24].copy_from_slice(&64_u64.to_le_bytes());

        let request = truncate_request(&payload)
            .expect("truncate with writeback timestamp bits should parse");

        assert_eq!(
            request,
            TruncateRequest {
                fh: Some(77),
                size: 64
            }
        );
    }

    #[test]
    fn setattr_request_accepts_writeback_cache_timestamp_only_update() {
        let mut payload = vec![0; FUSE_SETATTR_IN_LEN];
        payload[0..4].copy_from_slice(&(FATTR_FH | FATTR_MTIME_NOW | FATTR_CTIME).to_le_bytes());
        payload[8..16].copy_from_slice(&77_u64.to_le_bytes());

        let request =
            setattr_request(&payload).expect("writeback-cache timestamp-only setattr should parse");

        assert_eq!(
            request,
            SetattrRequest {
                fh: Some(77),
                size: None
            }
        );
    }
}
