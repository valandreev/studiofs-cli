use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString, OsStr};
use std::mem::{self, MaybeUninit};
use std::ops::{Deref, DerefMut};
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use parking_lot::Mutex as ParkingMutex;
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::{DirEntry, FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{StatFs, StudioVfs};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
#[cfg(test)]
use tokio::task::JoinHandle;
use tracing::debug;

use crate::low_level::{DirectoryEntryBuffer, Entry, OpenReply, Reply, Request, raw};

const TTL_SECONDS: f64 = 1.0;
const MACFUSE_ATTR_BLOCK_SIZE: u32 = 512;
const READDIR_CACHE_TTL: Duration = Duration::from_secs(1);
const READDIR_CACHE_PRUNE_THRESHOLD: usize = 1024;
const READDIR_CACHE_PRUNE_INTERVAL: Duration = Duration::from_secs(1);
const MACFUSE_READ_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS: usize = 32;
const MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY: usize = 1024 * 1024;
// Keep allocator-rounded buffers that grew slightly beyond the reusable read
// cap, while dropping genuinely oversized requests instead of pinning them.
const MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY: usize = MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY * 2;
const MACFUSE_READ_WINDOW_MAX_BYTES: usize = 1024 * 1024;
const MACFUSE_READ_WINDOW_MIN_BYTES: usize = 128 * 1024;

/// macFUSE open-cache flags chosen for one file handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MacFuseOpenCachePolicy {
    /// Whether this handle should bypass kernel page cache.
    pub(crate) direct_io: bool,
    /// Whether macFUSE should keep existing cached pages for this handle.
    pub(crate) keep_cache: bool,
}

/// Operation adapter shared by macFUSE callbacks and fast tests.
pub(crate) struct StudioVfsAdapter {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    readdir_cache: Mutex<HashMap<u64, ReaddirCacheEntry>>,
    readdir_cache_last_pruned: Mutex<Option<Instant>>,
    readdir_cache_ready: Condvar,
    read_admission: Arc<Semaphore>,
    read_queue_byte_limit: u32,
    read_buffers: Arc<ReadBufferPool>,
    read_windows: Arc<ReadWindowStore>,
}

#[derive(Debug, Default)]
struct ReadBufferPool {
    buffers: ParkingMutex<Vec<Vec<u8>>>,
}

impl ReadBufferPool {
    fn acquire(self: Arc<Self>, size: usize) -> PooledReadBuffer {
        let mut buffer = self.buffers.lock().pop().unwrap_or_default();
        // TODO(SFS-489): avoid this per-read zero-fill once the VFS read path accepts
        // MaybeUninit/ReadBuf-style storage instead of initialized `&mut [u8]`.
        buffer.resize(size, 0);
        PooledReadBuffer { pool: self, buffer }
    }

    fn recycle(&self, buffer: Vec<u8>) {
        // `mem::take` during `Drop` leaves a zero-capacity placeholder; recycling
        // it would only occupy a pool slot without avoiding a future allocation.
        if buffer.capacity() == 0 || buffer.capacity() > MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY
        {
            return;
        }
        let mut buffers = self.buffers.lock();
        if buffers.len() < MACFUSE_READ_BUFFER_POOL_MAX_BUFFERS {
            buffers.push(buffer);
        }
    }

    #[cfg(test)]
    fn available_count(&self) -> usize {
        self.buffers.lock().len()
    }

    #[cfg(test)]
    fn last_available_capacity(&self) -> Option<usize> {
        self.buffers.lock().last().map(Vec::capacity)
    }

    #[cfg(test)]
    fn last_available_len(&self) -> Option<usize> {
        self.buffers.lock().last().map(Vec::len)
    }
}

#[derive(Debug, Default)]
struct ReadWindowStore {
    windows: DashMap<u64, ReadWindow>,
    handles_by_inode: DashMap<u64, HashSet<u64>>,
}

impl ReadWindowStore {
    fn try_read(&self, file_handle: u64, ino: u64, offset: u64, out: &mut [u8]) -> Option<usize> {
        if out.is_empty() {
            return None;
        }
        let mut window = self.windows.get_mut(&file_handle)?;
        if window.ino != ino || !window.contains(offset, out.len()) {
            return None;
        }
        let copied = window.copy_into(offset, out);
        window.next_offset = offset.saturating_add(copied as u64);
        Some(copied)
    }

    fn remember(&self, file_handle: u64, ino: u64, offset: u64, next_offset: u64, bytes: Vec<u8>) {
        if bytes.is_empty() {
            return;
        }
        if let Some(previous) = self.windows.insert(
            file_handle,
            ReadWindow {
                ino,
                offset,
                next_offset,
                bytes,
            },
        ) {
            self.remove_inode_handle(previous.ino, file_handle);
        }
        self.handles_by_inode
            .entry(ino)
            .or_default()
            .insert(file_handle);
    }

    fn invalidate_handle(&self, file_handle: u64) {
        if let Some((_, window)) = self.windows.remove(&file_handle) {
            self.remove_inode_handle(window.ino, file_handle);
        }
    }

    fn invalidate_inode(&self, ino: u64) {
        let Some((_, handles)) = self.handles_by_inode.remove(&ino) else {
            return;
        };
        for file_handle in handles {
            if let Some(window) = self.windows.get(&file_handle)
                && window.ino != ino
            {
                continue;
            }
            self.windows.remove(&file_handle);
        }
    }

    fn remove_inode_handle(&self, ino: u64, file_handle: u64) {
        let Some(mut handles) = self.handles_by_inode.get_mut(&ino) else {
            return;
        };
        handles.remove(&file_handle);
        if handles.is_empty() {
            drop(handles);
            self.handles_by_inode
                .remove_if(&ino, |_, handles| handles.is_empty());
        }
    }
}

#[derive(Debug)]
struct ReadWindow {
    ino: u64,
    offset: u64,
    next_offset: u64,
    bytes: Vec<u8>,
}

impl ReadWindow {
    fn contains(&self, offset: u64, len: usize) -> bool {
        if offset != self.next_offset {
            return false;
        }
        let Some(relative) = offset.checked_sub(self.offset) else {
            return false;
        };
        let relative = relative as usize;
        relative
            .checked_add(len)
            .is_some_and(|end| end <= self.bytes.len())
    }

    fn copy_into(&self, offset: u64, out: &mut [u8]) -> usize {
        let relative = (offset - self.offset) as usize;
        let end = relative + out.len();
        out.copy_from_slice(&self.bytes[relative..end]);
        out.len()
    }
}

#[derive(Clone)]
struct ReadExecutionState {
    vfs: Arc<StudioVfs>,
    read_buffers: Arc<ReadBufferPool>,
    read_windows: Arc<ReadWindowStore>,
}

#[derive(Debug)]
struct PooledReadBuffer {
    pool: Arc<ReadBufferPool>,
    buffer: Vec<u8>,
}

impl PooledReadBuffer {
    fn truncate(&mut self, len: usize) {
        self.buffer.truncate(len);
    }
}

impl Deref for PooledReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_slice()
    }
}

impl DerefMut for PooledReadBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut_slice()
    }
}

impl AsRef<[u8]> for PooledReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

impl Drop for PooledReadBuffer {
    fn drop(&mut self) {
        self.pool.recycle(mem::take(&mut self.buffer));
    }
}

#[cfg(test)]
pub(crate) fn max_reusable_read_buffer_capacity_for_test() -> usize {
    MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY
}

#[cfg(test)]
pub(crate) fn max_retained_read_buffer_capacity_for_test() -> usize {
    MACFUSE_READ_BUFFER_POOL_RETAINED_CAPACITY
}

fn clamp_reusable_read_size(size: u32, read_queue_byte_limit: u32) -> u32 {
    size.min(read_queue_byte_limit)
        .min(MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32)
}

fn readahead_window_len(size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    size.saturating_mul(2)
        .clamp(MACFUSE_READ_WINDOW_MIN_BYTES, MACFUSE_READ_WINDOW_MAX_BYTES)
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct QueuedReadForTest {
    runtime_handle: Handle,
    handle: JoinHandle<Result<PooledReadBuffer, i32>>,
}

#[cfg(test)]
impl QueuedReadForTest {
    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub(crate) fn wait(self) -> Result<Vec<u8>, i32> {
        let buffer = self
            .runtime_handle
            .block_on(self.handle)
            .expect("queued macFUSE read worker should not panic")?;
        let bytes = buffer.to_vec();
        drop(buffer);
        Ok(bytes)
    }
}

impl StudioVfsAdapter {
    /// Create a macFUSE adapter over a StudioVfs instance.
    #[cfg(test)]
    pub(crate) fn new(vfs: Arc<StudioVfs>, runtime_handle: Handle) -> Self {
        Self::new_with_read_only(vfs, runtime_handle, false)
    }

    /// Create a macFUSE adapter over a StudioVfs instance with explicit write policy.
    pub(crate) fn new_with_read_only(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
    ) -> Self {
        Self::new_with_read_queue_byte_limit(
            vfs,
            runtime_handle,
            read_only,
            MACFUSE_READ_MAX_PENDING_BYTES,
        )
    }

    fn new_with_read_queue_byte_limit(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        read_queue_byte_limit: usize,
    ) -> Self {
        let read_queue_byte_limit = read_queue_byte_limit
            .max(1)
            .min(u32::MAX as usize)
            .min(Semaphore::MAX_PERMITS) as u32;
        Self {
            vfs,
            runtime_handle,
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            readdir_cache: Mutex::new(HashMap::new()),
            readdir_cache_last_pruned: Mutex::new(None),
            readdir_cache_ready: Condvar::new(),
            read_admission: Arc::new(Semaphore::new(read_queue_byte_limit as usize)),
            read_queue_byte_limit,
            read_buffers: Arc::new(ReadBufferPool::default()),
            read_windows: Arc::new(ReadWindowStore::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_read_queue_byte_limit_for_test(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_queue_byte_limit: usize,
    ) -> Self {
        Self::new_with_read_queue_byte_limit(vfs, runtime_handle, false, read_queue_byte_limit)
    }

    #[cfg(test)]
    pub(crate) fn read_queue_byte_limit_for_test(&self) -> u32 {
        self.read_queue_byte_limit
    }

    #[cfg(test)]
    pub(crate) fn hold_read_capacity_for_test(
        &self,
        bytes: u32,
    ) -> tokio::sync::OwnedSemaphorePermit {
        self.runtime_handle
            .block_on(self.read_admission.clone().acquire_many_owned(bytes))
            .expect("test read capacity should be available")
    }

    #[cfg(test)]
    pub(crate) fn reusable_read_buffer_count_for_test(&self) -> usize {
        self.read_buffers.available_count()
    }

    #[cfg(test)]
    pub(crate) fn reusable_read_buffer_capacity_for_test(&self) -> Option<usize> {
        self.read_buffers.last_available_capacity()
    }

    /// Look up a child inode.
    pub(crate) fn lookup(&self, parent: u64, name: &[u8]) -> Result<InodeAttr, i32> {
        self.vfs
            .lookup(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get inode attributes.
    pub(crate) fn getattr(&self, ino: u64) -> Result<stat, i32> {
        self.vfs
            .getattr(ino)
            .map(|attr| self.attr_to_stat(&attr))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create a directory.
    pub(crate) fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .mkdir(parent, name, self.mount_uid, self.mount_gid, mode & 0o7777)
            .inspect(|_| self.invalidate_readdir_cache(parent))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create a symbolic link.
    pub(crate) fn symlink(
        &self,
        parent: u64,
        name: &[u8],
        target: &[u8],
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .symlink(parent, name, target, self.mount_uid, self.mount_gid)
            .inspect(|_| self.invalidate_readdir_cache(parent))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Create and open a regular file.
    pub(crate) fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: i32,
    ) -> Result<(InodeAttr, OpenReply), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .create(parent, name, self.mount_uid, self.mount_gid, mode & 0o7777)
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.invalidate_readdir_cache(parent);
        let open = self.open(attr.ino, flags)?;
        Ok((attr, open))
    }

    /// Open an existing file.
    pub(crate) fn open(&self, ino: u64, flags: i32) -> Result<OpenReply, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && posix_open_requests_write(flags, trunc) {
            return Err(libc::EROFS);
        }
        self.vfs
            .open_file(ino, open_flags, trunc)
            .map(|fh| {
                let policy = macfuse_open_cache_policy(flags, trunc);
                OpenReply::new(fh)
                    .with_direct_io(policy.direct_io)
                    .with_keep_cache(policy.keep_cache)
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Remove a file entry.
    pub(crate) fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .unlink(parent, name)
            .inspect(|_| self.invalidate_readdir_cache(parent))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Remove an empty directory entry.
    pub(crate) fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rmdir(parent, name)
            .inspect(|_| self.invalidate_readdir_cache(parent))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Rename a namespace entry.
    pub(crate) fn rename(
        &self,
        parent: u64,
        name: &[u8],
        newparent: u64,
        newname: &[u8],
    ) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rename(parent, name, newparent, newname)
            .inspect(|_| {
                self.invalidate_readdir_cache(parent);
                if newparent != parent {
                    self.invalidate_readdir_cache(newparent);
                }
            })
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Read file data through a VFS file handle and record FUSE read diagnostics.
    #[cfg(test)]
    pub(crate) fn read(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size_usize = size as usize;

        // Test-only synchronous reads return an owned Vec, so keep them off the
        // queued-read pool; otherwise this helper would consume pooled buffers.
        let mut out = vec![0u8; size_usize];
        let started = Instant::now();
        let result =
            self.runtime_handle
                .block_on(self.vfs.read_into_fh(file_handle, ino, offset, &mut out));
        match result {
            Ok(written) => {
                self.vfs
                    .record_fuse_read_callback(written as usize, started.elapsed(), false);
                out.truncate(written as usize);
                Ok(out)
            }
            Err(error) => {
                self.vfs
                    .record_fuse_read_callback(size_usize, started.elapsed(), true);
                Err(vfs_err_to_errno(&error))
            }
        }
    }

    async fn perform_read(
        state: ReadExecutionState,
        ino: u64,
        file_handle: u64,
        offset: u64,
        size: u32,
        started: Instant,
    ) -> Result<PooledReadBuffer, i32> {
        let size = clamp_reusable_read_size(size, MACFUSE_READ_BUFFER_POOL_MAX_CAPACITY as u32);
        let size_usize = size as usize;

        let mut out = state.read_buffers.clone().acquire(size_usize);
        if state
            .read_windows
            .try_read(file_handle, ino, offset, &mut out)
            .is_some()
        {
            state
                .vfs
                .record_fuse_read_callback(size_usize, started.elapsed(), false);
            return Ok(out);
        }

        let read_len = readahead_window_len(size_usize);
        if read_len > size_usize {
            out.buffer.resize(read_len, 0);
        }
        let result = state
            .vfs
            .read_into_fh(file_handle, ino, offset, &mut out)
            .await;
        match result {
            Ok(written) => {
                let written = written as usize;
                let reply_len = written.min(size_usize);
                state
                    .vfs
                    .record_fuse_read_callback(reply_len, started.elapsed(), false);
                if written > size_usize {
                    let mut window_bytes = mem::take(&mut out.buffer);
                    window_bytes.truncate(written);
                    let mut reply = state.read_buffers.clone().acquire(reply_len);
                    reply.copy_from_slice(&window_bytes[..reply_len]);
                    state.read_windows.remember(
                        file_handle,
                        ino,
                        offset,
                        offset.saturating_add(reply_len as u64),
                        window_bytes,
                    );
                    return Ok(reply);
                } else {
                    state.read_windows.invalidate_handle(file_handle);
                }
                out.truncate(reply_len);
                Ok(out)
            }
            Err(error) => {
                state
                    .vfs
                    .record_fuse_read_callback(size_usize, started.elapsed(), true);
                state.read_windows.invalidate_handle(file_handle);
                Err(vfs_err_to_errno(&error))
            }
        }
    }

    pub(crate) fn queue_read_reply(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
        request: Request,
    ) {
        let Ok(offset) = u64::try_from(offset) else {
            let _ = Reply::new(request).error(libc::EINVAL);
            return;
        };
        let admission = Arc::clone(&self.read_admission);
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            read_buffers: Arc::clone(&self.read_buffers),
            read_windows: Arc::clone(&self.read_windows),
        };
        let size = clamp_reusable_read_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        // TODO(SFS-489): wire macFUSE INTERRUPT notifications into this task
        // so queued reads can abort before spending VFS/cache I/O on canceled requests.
        self.runtime_handle.spawn(async move {
            let Ok(_permit) = admission.acquire_many_owned(size).await else {
                let _ = Reply::new(request).error(libc::EIO);
                return;
            };
            // Keep the permit until after the reply consumes `bytes`, so queued read
            // memory remains bounded for the whole kernel-reply handoff.
            match Self::perform_read(state, ino, file_handle, offset, size, started).await {
                Ok(bytes) => {
                    let _ = Reply::new(request).read_buffer(&bytes);
                }
                Err(errno) => {
                    let _ = Reply::new(request).error(errno);
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn queue_read_for_test(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<QueuedReadForTest, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let admission = Arc::clone(&self.read_admission);
        let state = ReadExecutionState {
            vfs: Arc::clone(&self.vfs),
            read_buffers: Arc::clone(&self.read_buffers),
            read_windows: Arc::clone(&self.read_windows),
        };
        let size = clamp_reusable_read_size(size, self.read_queue_byte_limit);
        let started = Instant::now();
        Ok(QueuedReadForTest {
            runtime_handle: self.runtime_handle.clone(),
            handle: self.runtime_handle.spawn(async move {
                let _permit = admission
                    .acquire_many_owned(size)
                    .await
                    .map_err(|_| libc::EIO)?;
                Self::perform_read(state, ino, file_handle, offset, size, started).await
            }),
        })
    }

    /// Write file data through a VFS file handle.
    pub(crate) fn write(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        data: &[u8],
    ) -> Result<u32, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let written = self
            .runtime_handle
            .block_on(self.vfs.write_fh(file_handle, ino, offset, data))
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.read_windows.invalidate_inode(ino);
        Ok(written)
    }

    /// Flush file data through a VFS file handle.
    pub(crate) fn flush(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.flush_fh(file_handle))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Synchronize file data for a handle when available, otherwise for an inode.
    pub(crate) fn fsync(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        datasync: bool,
    ) -> Result<(), i32> {
        let result = if let Some(file_handle) = file_handle.filter(|handle| *handle != 0) {
            // Handle-backed sync must preserve pending handle-local write state;
            // StudioVfs does not currently distinguish fdatasync from fsync on
            // this path, so both use the handle flush barrier.
            self.runtime_handle.block_on(self.vfs.flush_fh(file_handle))
        } else {
            self.runtime_handle.block_on(self.vfs.fsync(ino, datasync))
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    /// Change a file length through the VFS setattr path.
    #[cfg(test)]
    pub(crate) fn set_len(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        size: u64,
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .setattr_with_handle(file_handle, ino, SetattrArgs::size_only(size))
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.read_windows.invalidate_inode(ino);
        Ok(attr)
    }

    /// Change file attributes through the VFS setattr path.
    pub(crate) fn setattr(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        args: SetattrArgs,
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let attr = self
            .vfs
            .setattr_with_handle(file_handle, ino, args)
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.read_windows.invalidate_inode(ino);
        Ok(attr)
    }

    /// Release a file handle opened through this adapter.
    pub(crate) fn release(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        let result = self
            .runtime_handle
            .block_on(self.vfs.release_fh(file_handle))
            .map_err(|error| vfs_err_to_errno(&error));
        self.read_windows.invalidate_handle(file_handle);
        result
    }

    /// Read directory entries from VFS.
    pub(crate) fn readdir(
        &self,
        request: &Request,
        ino: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        self.readdir_with_push(ino, offset, size, |buffer, name, attr, next_offset| {
            buffer
                .push(request, name, attr, next_offset)
                .map_err(directory_entry_errno)
        })
    }

    #[cfg(test)]
    pub(crate) fn readdir_for_test(
        &self,
        ino: u64,
        offset: i64,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        self.readdir_with_push(ino, offset, size, |buffer, name, _attr, next_offset| {
            buffer
                .push_for_test(0, name, next_offset)
                .map_err(directory_entry_errno)
        })
    }

    fn readdir_with_push(
        &self,
        ino: u64,
        offset: i64,
        size: usize,
        mut push: impl FnMut(&mut DirectoryEntryBuffer, &OsStr, &stat, off_t) -> Result<(), i32>,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let mut buffer = DirectoryEntryBuffer::new(size);
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        if idx == 0 {
            if !push_or_finish(&mut buffer, OsStr::new("."), &dir_attr, 1, &mut push)? {
                return Ok(buffer);
            }
            idx = 1;
        }
        if idx == 1 {
            if !push_or_finish(&mut buffer, OsStr::new(".."), &dir_attr, 2, &mut push)? {
                return Ok(buffer);
            }
            idx = 2;
        }

        let entries = self.cached_readdir_entries(ino)?;
        for (i, entry) in entries.iter().enumerate().skip(idx.saturating_sub(2)) {
            let entry_idx = i + 2;
            let attr = self.dir_entry_to_stat(entry);
            let next_offset = off_t::try_from(entry_idx + 1).map_err(|_| libc::EOVERFLOW)?;
            if !push_or_finish(
                &mut buffer,
                OsStr::from_bytes(&entry.name),
                &attr,
                next_offset,
                &mut push,
            )? {
                break;
            }
        }
        Ok(buffer)
    }

    /// Read the target for a symlink inode.
    pub(crate) fn readlink(&self, ino: u64) -> Result<Vec<u8>, i32> {
        self.vfs
            .readlink(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get filesystem statistics.
    pub(crate) fn statfs(&self) -> Result<statvfs, i32> {
        self.vfs
            .statfs()
            .map(statfs_to_statvfs)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get a native extended attribute value.
    pub(crate) fn get_xattr(&self, ino: u64, name: &[u8]) -> Result<Option<Vec<u8>>, i32> {
        self.vfs
            .get_xattr(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Get a native extended attribute value size without returning the value bytes.
    pub(crate) fn get_xattr_size(&self, ino: u64, name: &[u8]) -> Result<Option<usize>, i32> {
        self.vfs
            .get_xattr_size(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Set a native extended attribute value.
    pub(crate) fn set_xattr(
        &self,
        ino: u64,
        name: &[u8],
        value: &[u8],
        flags: u32,
    ) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .set_xattr(ino, name, value, flags)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Remove a native extended attribute value.
    pub(crate) fn remove_xattr(&self, ino: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .remove_xattr(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// List native extended attribute names.
    pub(crate) fn list_xattrs(&self, ino: u64) -> Result<Vec<Vec<u8>>, i32> {
        self.vfs
            .list_xattrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn attr_to_stat(&self, attr: &InodeAttr) -> stat {
        bridge_attr_to_stat(&to_bridge_attr(
            attr,
            self.mount_uid,
            self.mount_gid,
            MACFUSE_ATTR_BLOCK_SIZE,
        ))
    }

    fn dir_entry_to_stat(&self, entry: &DirEntry) -> stat {
        let mut out = minimal_stat(entry.ino);
        out.st_ino = entry.ino;
        out.st_mode = match entry.kind {
            FileType::RegularFile => libc::S_IFREG,
            FileType::Directory => libc::S_IFDIR,
            FileType::Symlink => libc::S_IFLNK,
        };
        out
    }

    fn cached_readdir_entries(&self, ino: u64) -> Result<Arc<Vec<DirEntry>>, i32> {
        let mut cache = self.readdir_cache.lock().map_err(|_| libc::EIO)?;
        loop {
            let now = Instant::now();
            match cache.get(&ino) {
                Some(ReaddirCacheEntry::Ready { loaded_at, entries })
                    if now.saturating_duration_since(*loaded_at) <= READDIR_CACHE_TTL =>
                {
                    return Ok(Arc::clone(entries));
                }
                Some(ReaddirCacheEntry::Loading) => {
                    cache = self
                        .readdir_cache_ready
                        .wait(cache)
                        .map_err(|_| libc::EIO)?;
                }
                _ => {
                    if cache.len() >= READDIR_CACHE_PRUNE_THRESHOLD
                        && self.should_prune_readdir_cache(now)?
                    {
                        cache.retain(|_, entry| match entry {
                            ReaddirCacheEntry::Ready { loaded_at, .. } => {
                                now.saturating_duration_since(*loaded_at) <= READDIR_CACHE_TTL
                            }
                            ReaddirCacheEntry::Loading => true,
                        });
                    }
                    cache.insert(ino, ReaddirCacheEntry::Loading);
                    break;
                }
            }
        }
        drop(cache);

        let entries_result = self
            .vfs
            .readdir(ino)
            .map(Arc::new)
            .map_err(|error| vfs_err_to_errno(&error));
        let mut cache = self.readdir_cache.lock().map_err(|_| libc::EIO)?;
        match entries_result {
            Ok(entries) => {
                cache.insert(
                    ino,
                    ReaddirCacheEntry::Ready {
                        loaded_at: Instant::now(),
                        entries: Arc::clone(&entries),
                    },
                );
                self.readdir_cache_ready.notify_all();
                Ok(entries)
            }
            Err(errno) => {
                cache.remove(&ino);
                self.readdir_cache_ready.notify_all();
                Err(errno)
            }
        }
    }

    fn invalidate_readdir_cache(&self, ino: u64) {
        if let Ok(mut cache) = self.readdir_cache.lock() {
            cache.remove(&ino);
        }
    }

    fn should_prune_readdir_cache(&self, now: Instant) -> Result<bool, i32> {
        let mut last_pruned = self
            .readdir_cache_last_pruned
            .lock()
            .map_err(|_| libc::EIO)?;
        if last_pruned
            .is_some_and(|last| now.saturating_duration_since(last) < READDIR_CACHE_PRUNE_INTERVAL)
        {
            return Ok(false);
        }
        *last_pruned = Some(now);
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn readdir_cache_entry_count_for_test(&self) -> usize {
        self.readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned")
            .len()
    }

    #[cfg(test)]
    pub(crate) fn fill_expired_readdir_cache_entries_for_test(&self) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let loaded_at = Instant::now() - READDIR_CACHE_TTL - Duration::from_secs(1);
        for ino in 10_000..(10_000 + READDIR_CACHE_PRUNE_THRESHOLD as u64) {
            cache.insert(
                ino,
                ReaddirCacheEntry::Ready {
                    loaded_at,
                    entries: Arc::new(Vec::new()),
                },
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn fill_fresh_readdir_cache_entries_for_test(&self) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let loaded_at = Instant::now();
        for ino in 20_000..(20_000 + READDIR_CACHE_PRUNE_THRESHOLD as u64) {
            cache.insert(
                ino,
                ReaddirCacheEntry::Ready {
                    loaded_at,
                    entries: Arc::new(Vec::new()),
                },
            );
        }
    }

    #[cfg(test)]
    pub(crate) fn set_ready_readdir_cache_loaded_at_for_test(&self, ino: u64, loaded_at: Instant) {
        let mut cache = self
            .readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned");
        let entries = match cache.remove(&ino) {
            Some(ReaddirCacheEntry::Ready { entries, .. }) => entries,
            _ => Arc::new(Vec::new()),
        };
        cache.insert(ino, ReaddirCacheEntry::Ready { loaded_at, entries });
    }

    #[cfg(test)]
    pub(crate) fn readdir_cache_loaded_at_for_test(&self, ino: u64) -> Option<Instant> {
        self.readdir_cache
            .lock()
            .expect("readdir cache lock should not be poisoned")
            .get(&ino)
            .and_then(|entry| match entry {
                ReaddirCacheEntry::Ready { loaded_at, .. } => Some(*loaded_at),
                ReaddirCacheEntry::Loading => None,
            })
    }
}

enum ReaddirCacheEntry {
    Ready {
        loaded_at: Instant,
        entries: Arc<Vec<DirEntry>>,
    },
    Loading,
}

pub(crate) struct CallbackState {
    adapter: StudioVfsAdapter,
}

impl CallbackState {
    pub(crate) fn new(vfs: Arc<StudioVfs>, runtime_handle: Handle, read_only: bool) -> Self {
        Self {
            adapter: StudioVfsAdapter::new_with_read_only(vfs, runtime_handle, read_only),
        }
    }
}

fn callback_state<'a>(req: *mut raw::FuseReq) -> Option<&'a CallbackState> {
    if req.is_null() {
        return None;
    }
    // SAFETY: `req` is a live macFUSE request supplied by a low-level callback.
    // macFUSE returns the same userdata pointer passed to session creation.
    let userdata = unsafe { raw::fuse_req_userdata(req) }.cast::<CallbackState>();
    if userdata.is_null() {
        None
    } else {
        // SAFETY: `CallbackState` is boxed by `mount_with_request` and kept live
        // until after the session loop exits. Callbacks borrow it immutably only.
        Some(unsafe { &*userdata })
    }
}

fn reply_error(req: *mut raw::FuseReq, errno: i32) {
    if let Some(request) = owned_request(req) {
        let _ = Reply::new(request).error(errno);
    }
}

fn owned_request(req: *mut raw::FuseReq) -> Option<Request> {
    // SAFETY: Each low-level callback receives a live unreplied request pointer.
    unsafe { Request::from_raw(req) }
}

fn directory_entry_errno(error: crate::low_level::DirectoryEntryError) -> i32 {
    match error {
        crate::low_level::DirectoryEntryError::InsufficientCapacity { .. } => libc::ERANGE,
        crate::low_level::DirectoryEntryError::InteriorNul => libc::EINVAL,
    }
}

fn minimal_dir_stat(ino: u64) -> stat {
    let mut out = minimal_stat(ino);
    out.st_mode = libc::S_IFDIR;
    out
}

fn minimal_stat(ino: u64) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: A zeroed `stat` is a valid C value for integer/time fields.
    // readdir only needs inode and mode bits for directory entry packing.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = ino;
    out
}

fn push_or_finish(
    buffer: &mut DirectoryEntryBuffer,
    name: &OsStr,
    attr: &stat,
    next_offset: off_t,
    push: &mut impl FnMut(&mut DirectoryEntryBuffer, &OsStr, &stat, off_t) -> Result<(), i32>,
) -> Result<bool, i32> {
    match push(buffer, name, attr, next_offset) {
        Ok(()) => Ok(true),
        Err(libc::ERANGE) => Ok(false),
        Err(errno) => Err(errno),
    }
}

fn posix_open_requests_write(flags: i32, trunc: bool) -> bool {
    let access_mode = flags & libc::O_ACCMODE;
    trunc
        || access_mode == libc::O_WRONLY
        || access_mode == libc::O_RDWR
        || flags & libc::O_APPEND != 0
}

/// Translate POSIX/macFUSE open flags into the per-handle cache-retention policy.
pub(crate) fn macfuse_open_cache_policy(flags: i32, trunc: bool) -> MacFuseOpenCachePolicy {
    let write_capable = posix_open_requests_write(flags, trunc);
    MacFuseOpenCachePolicy {
        direct_io: false,
        keep_cache: !write_capable,
    }
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` is thread-safe, takes no pointers, and has no Rust-side
    // invariants beyond the platform libc contract.
    unsafe { libc::geteuid() }
}

fn current_gid() -> u32 {
    // SAFETY: `getegid` is thread-safe, takes no pointers, and has no Rust-side
    // invariants beyond the platform libc contract.
    unsafe { libc::getegid() }
}

fn bridge_attr_to_stat(attr: &BridgeFileAttr) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: A zeroed `stat` is a valid C value for integer/time fields; all
    // fields relevant to macFUSE replies are then initialized below.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = attr.ino;
    out.st_mode = mode_bits(attr);
    out.st_nlink = attr.nlink as _;
    out.st_uid = attr.uid;
    out.st_gid = attr.gid;
    out.st_rdev = attr.rdev as _;
    out.st_size = attr.size as _;
    out.st_blocks = attr.blocks as _;
    out.st_blksize = attr.blksize as _;
    assign_stat_time(&mut out, StatTimeField::Access, attr.atime);
    assign_stat_time(&mut out, StatTimeField::Modified, attr.mtime);
    assign_stat_time(&mut out, StatTimeField::Changed, attr.ctime);
    assign_stat_time(&mut out, StatTimeField::Created, attr.crtime);
    out
}

fn mode_bits(attr: &BridgeFileAttr) -> libc::mode_t {
    let kind = match attr.kind {
        BridgeFileType::RegularFile => libc::S_IFREG,
        BridgeFileType::Directory => libc::S_IFDIR,
        BridgeFileType::Symlink => libc::S_IFLNK,
    };
    kind | libc::mode_t::from(attr.perm)
}

enum StatTimeField {
    Access,
    Modified,
    Changed,
    Created,
}

fn assign_stat_time(out: &mut stat, field: StatTimeField, time: std::time::SystemTime) {
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let seconds = duration.as_secs() as _;
    let nanos = duration.subsec_nanos() as _;
    match field {
        StatTimeField::Access => {
            out.st_atime = seconds;
            out.st_atime_nsec = nanos;
        }
        StatTimeField::Modified => {
            out.st_mtime = seconds;
            out.st_mtime_nsec = nanos;
        }
        StatTimeField::Changed => {
            out.st_ctime = seconds;
            out.st_ctime_nsec = nanos;
        }
        StatTimeField::Created => {
            out.st_birthtime = seconds;
            out.st_birthtime_nsec = nanos;
        }
    }
}

fn statfs_to_statvfs(stats: StatFs) -> statvfs {
    let out = MaybeUninit::<statvfs>::zeroed();
    // SAFETY: A zeroed `statvfs` is a valid C value for integer fields; all
    // fields used by macFUSE are then initialized below.
    let mut out = unsafe { out.assume_init() };
    out.f_bsize = stats.bsize as _;
    out.f_frsize = stats.frsize as _;
    out.f_blocks = stats.blocks as _;
    out.f_bfree = stats.bfree as _;
    out.f_bavail = stats.bavail as _;
    out.f_files = stats.files as _;
    out.f_ffree = stats.ffree as _;
    out.f_namemax = stats.namelen as _;
    out
}

/// macFUSE lookup callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_lookup(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for lookup callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.lookup(parent, name) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE getattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_getattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.getattr(ino) {
        Ok(attr) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&attr, TTL_SECONDS);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE mkdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_mkdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for mkdir callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.mkdir(parent, name, mode) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE symlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_symlink(
    req: *mut raw::FuseReq,
    target: *const c_char,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if target.is_null() || name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes NUL-terminated symlink target and entry name strings.
    let target = unsafe { CStr::from_ptr(target) }.to_bytes();
    // SAFETY: macFUSE passes NUL-terminated symlink target and entry name strings.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.symlink(parent, name, target) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).entry(&entry);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE create callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_create(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() || fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for create callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    // SAFETY: macFUSE passes initialized file info for create callbacks.
    let flags = unsafe { (*fi).flags };
    match state.adapter.create(parent, name, mode, flags) {
        Ok((attr, open)) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let entry = Entry::new(attr.ino, 0, stat, TTL_SECONDS, TTL_SECONDS);
                let _ = Reply::new(request).create(&entry, open);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE unlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_unlink(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for unlink callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.unlink(parent, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE rmdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_rmdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a NUL-terminated entry name for rmdir callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    match state.adapter.rmdir(parent, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE rename callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_rename(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    newparent: raw::FuseIno,
    newname: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if name.is_null() || newname.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes NUL-terminated entry names for rename callbacks.
    let name = unsafe { CStr::from_ptr(name) }.to_bytes();
    // SAFETY: macFUSE passes NUL-terminated entry names for rename callbacks.
    let newname = unsafe { CStr::from_ptr(newname) }.to_bytes();
    match state.adapter.rename(parent, name, newparent, newname) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE open callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_open(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for open.
    let flags = unsafe { (*fi).flags };
    match state.adapter.open(ino, flags) {
        Ok(open) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).open(open);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE read callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_read(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let size = match u32::try_from(size) {
        Ok(size) => size,
        Err(_) => {
            reply_error(req, libc::EOVERFLOW);
            return;
        }
    };
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for read.
    let file_handle = unsafe { (*fi).fh };
    let Some(request) = owned_request(req) else {
        debug!(
            ino,
            file_handle,
            offset = off,
            size,
            "macFUSE read request ownership unavailable"
        );
        return;
    };
    state
        .adapter
        .queue_read_reply(ino, file_handle, off, size, request);
}

/// macFUSE write callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_write(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    buf: *const c_char,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let data = if size == 0 {
        &[]
    } else {
        if buf.is_null() {
            reply_error(req, libc::EINVAL);
            return;
        }
        // SAFETY: macFUSE passes a valid buffer of `size` bytes for non-empty
        // writes, and the borrowed slice is used only within this callback.
        unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size) }
    };
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for write.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.write(ino, file_handle, off, data) {
        Ok(count) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).write_count(count as usize);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE flush callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_flush(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for flush.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.flush(ino, file_handle) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE fsync callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_fsync(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    datasync: libc::c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let file_handle = if fi.is_null() {
        None
    } else {
        // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for handle-backed fsync.
        Some(unsafe { (*fi).fh })
    };
    match state.adapter.fsync(file_handle, ino, datasync != 0) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE setattr callback for size-changing truncate/extend operations.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_setattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    attr: *mut stat,
    to_set: libc::c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if attr.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let args = match setattr_args_from_callback(attr, to_set) {
        Ok(args) => args,
        Err(errno) => {
            reply_error(req, errno);
            return;
        }
    };
    if setattr_args_are_empty(&args) {
        reply_error(req, libc::ENOTSUP);
        return;
    }
    let file_handle = if fi.is_null() || !setattr_mask_has_fh(to_set) {
        None
    } else {
        // SAFETY: macFUSE guarantees a valid file handle pointer when the
        // callback mask includes the header-defined file-handle bit.
        Some(unsafe { (*fi).fh })
    };
    match state.adapter.setattr(file_handle, ino, args) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

fn setattr_args_from_callback(attr: *const stat, to_set: libc::c_int) -> Result<SetattrArgs, i32> {
    if setattr_mask_has_unknown(to_set) {
        return Err(libc::ENOTSUP);
    }
    // SAFETY: The caller checked `attr` for null and macFUSE passes an
    // initialized `stat` for setattr callbacks.
    let attr = unsafe { &*attr };
    let mut args = SetattrArgs::default();
    if setattr_mask_has_size(to_set) {
        args.size = Some(u64::try_from(attr.st_size).map_err(|_| libc::EINVAL)?);
    }
    if setattr_mask_has_mode(to_set) {
        args.mode = Some(attr.st_mode & 0o7777);
    }
    if setattr_mask_has_uid(to_set) {
        args.uid = Some(attr.st_uid);
    }
    if setattr_mask_has_gid(to_set) {
        args.gid = Some(attr.st_gid);
    }
    if setattr_mask_has_atime_now(to_set) {
        args.atime = TimeSpec::Now;
    } else if setattr_mask_has_atime(to_set) {
        args.atime = TimeSpec::Set(stat_time_to_nanos(attr.st_atime, attr.st_atime_nsec)?);
    }
    if setattr_mask_has_mtime_now(to_set) {
        args.mtime = TimeSpec::Now;
    } else if setattr_mask_has_mtime(to_set) {
        args.mtime = TimeSpec::Set(stat_time_to_nanos(attr.st_mtime, attr.st_mtime_nsec)?);
    }
    if setattr_mask_has_crtime(to_set) {
        args.crtime = TimeSpec::Set(stat_time_to_nanos(
            attr.st_birthtime,
            attr.st_birthtime_nsec,
        )?);
    }
    Ok(args)
}

fn setattr_args_are_empty(args: &SetattrArgs) -> bool {
    args.size.is_none()
        && args.mode.is_none()
        && args.uid.is_none()
        && args.gid.is_none()
        && args.atime.resolve().is_none()
        && args.mtime.resolve().is_none()
        && args.crtime.resolve().is_none()
        && args.win_attrs.is_none()
}

fn stat_time_to_nanos(seconds: libc::time_t, nanos: libc::c_long) -> Result<i64, i32> {
    if !(0..1_000_000_000).contains(&nanos) {
        return Err(libc::EINVAL);
    }
    let total = i128::from(seconds)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nanos)))
        .ok_or(libc::EOVERFLOW)?;
    i64::try_from(total).map_err(|_| libc::EOVERFLOW)
}

fn setattr_mask_has_size(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined SIZE bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_size_mask() != 0 }
}

fn setattr_mask_has_fh(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim uses the same macFUSE headers and returns either the
    // header-defined FH bit or zero when this FUSE version has no FH bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_fh_mask() != 0 }
}

fn setattr_mask_has_mode(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MODE bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mode_mask() != 0 }
}

fn setattr_mask_has_uid(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined UID bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_uid_mask() != 0 }
}

fn setattr_mask_has_gid(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined GID bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_gid_mask() != 0 }
}

fn setattr_mask_has_atime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined ATIME bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_atime_mask() != 0 }
}

fn setattr_mask_has_mtime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MTIME bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mtime_mask() != 0 }
}

fn setattr_mask_has_atime_now(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined ATIME_NOW bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_atime_now_mask() != 0 }
}

fn setattr_mask_has_mtime_now(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined MTIME_NOW bit.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_mtime_now_mask() != 0 }
}

fn setattr_mask_has_crtime(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim returns the header-defined CRTIME bit, or zero when
    // this FUSE version does not define it.
    unsafe { to_set & crate::low_level::studiofs_macfuse_setattr_crtime_mask() != 0 }
}

fn setattr_mask_has_unknown(to_set: libc::c_int) -> bool {
    to_set & !setattr_supported_mask() != 0
}

fn setattr_supported_mask() -> libc::c_int {
    // SAFETY: Each C shim returns one header-defined bit for this build, or zero
    // for optional bits not present in the active FUSE headers.
    unsafe {
        crate::low_level::studiofs_macfuse_setattr_size_mask()
            | crate::low_level::studiofs_macfuse_setattr_fh_mask()
            | crate::low_level::studiofs_macfuse_setattr_mode_mask()
            | crate::low_level::studiofs_macfuse_setattr_uid_mask()
            | crate::low_level::studiofs_macfuse_setattr_gid_mask()
            | crate::low_level::studiofs_macfuse_setattr_atime_mask()
            | crate::low_level::studiofs_macfuse_setattr_mtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_atime_now_mask()
            | crate::low_level::studiofs_macfuse_setattr_mtime_now_mask()
            | crate::low_level::studiofs_macfuse_setattr_crtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_chgtime_mask()
            | crate::low_level::studiofs_macfuse_setattr_bkuptime_mask()
            | crate::low_level::studiofs_macfuse_setattr_flags_mask()
    }
}

/// macFUSE release callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_release(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: macFUSE passes a valid `fuse_file_info` pointer for release.
    let file_handle = unsafe { (*fi).fh };
    match state.adapter.release(ino, file_handle) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE readdir callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_readdir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Some(request) = owned_request(req) else {
        return;
    };
    match state.adapter.readdir(&request, ino, off, size) {
        Ok(entries) => {
            let _ = Reply::new(request).directory_entries(&entries);
        }
        Err(errno) => {
            let _ = Reply::new(request).error(errno);
        }
    }
}

/// macFUSE readlink callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_readlink(req: *mut raw::FuseReq, ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.readlink(ino) {
        Ok(target) => match CString::new(target) {
            Ok(target) => {
                if let Some(request) = owned_request(req) {
                    let _ = Reply::new(request).readlink(&target);
                }
            }
            Err(_) => reply_error(req, libc::EINVAL),
        },
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE statfs callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_statfs(req: *mut raw::FuseReq, _ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.statfs() {
        Ok(stats) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).statfs(&stats);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE setxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_setxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    value: *const c_char,
    size: size_t,
    flags: libc::c_int,
    position: u32,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(value) = (unsafe { xattr_value_from_callback(req, value, size) }) else {
        return;
    };
    if position != 0 {
        reply_error(req, libc::ENOTSUP);
        return;
    }
    let flags = match u32::try_from(flags) {
        Ok(flags) => flags,
        Err(_) => {
            reply_error(req, libc::EINVAL);
            return;
        }
    };
    match state.adapter.set_xattr(ino, name, value, flags) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE getxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_getxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    size: size_t,
    position: u32,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    let position = position as usize;
    if size == 0 {
        match state.adapter.get_xattr_size(ino, name) {
            Ok(Some(value_size)) => {
                reply_xattr_size(req, xattr_size_from_position(value_size, position))
            }
            Ok(None) => reply_error(req, xattr_not_found_errno()),
            Err(errno) => reply_error(req, errno),
        }
        return;
    }
    match state.adapter.get_xattr(ino, name) {
        Ok(Some(value)) => {
            reply_xattr_value(req, size, xattr_value_from_position(&value, position))
        }
        Ok(None) => reply_error(req, xattr_not_found_errno()),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE listxattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_listxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.list_xattrs(ino) {
        Ok(names) if size == 0 => reply_xattr_size(req, xattr_name_list_len(&names)),
        Ok(names) => reply_xattr_value(req, size, &format_xattr_name_list(&names)),
        Err(errno) => reply_error(req, errno),
    }
}

/// macFUSE removexattr callback for the StudioVfs bridge.
#[unsafe(no_mangle)]
pub extern "C" fn studiofs_macfuse_removexattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    // SAFETY: The returned slice is used only within this callback while the
    // macFUSE-provided request arguments are live.
    let Some(name) = (unsafe { xattr_name_from_callback(req, name) }) else {
        return;
    };
    match state.adapter.remove_xattr(ino, name) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

unsafe fn xattr_name_from_callback<'a>(
    req: *mut raw::FuseReq,
    name: *const c_char,
) -> Option<&'a [u8]> {
    // SAFETY CONTRACT: the returned slice is borrowed from callback-owned
    // macFUSE memory and must not escape the current callback invocation.
    if name.is_null() {
        reply_error(req, libc::EINVAL);
        return None;
    }
    Some(unsafe { CStr::from_ptr(name) }.to_bytes())
}

unsafe fn xattr_value_from_callback<'a>(
    req: *mut raw::FuseReq,
    value: *const c_char,
    size: size_t,
) -> Option<&'a [u8]> {
    // SAFETY CONTRACT: the returned slice is borrowed from callback-owned
    // macFUSE memory and must not escape the current callback invocation.
    if size == 0 {
        return Some(&[]);
    }
    if value.is_null() {
        reply_error(req, libc::EINVAL);
        return None;
    }
    Some(unsafe { std::slice::from_raw_parts(value.cast::<u8>(), size) })
}

fn reply_xattr_size(req: *mut raw::FuseReq, value_len: usize) {
    let Some(request) = owned_request(req) else {
        return;
    };
    let _ = Reply::new(request).xattr_size(value_len);
}

fn xattr_size_from_position(value_len: usize, position: usize) -> usize {
    value_len.saturating_sub(position)
}

fn xattr_value_from_position(value: &[u8], position: usize) -> &[u8] {
    value.get(position..).unwrap_or(&[])
}

fn reply_xattr_value(req: *mut raw::FuseReq, size: size_t, value: &[u8]) {
    let Some(request) = owned_request(req) else {
        return;
    };
    if size < value.len() {
        let _ = Reply::new(request).error(libc::ERANGE);
    } else {
        let _ = Reply::new(request).read_buffer(value);
    }
}

fn format_xattr_name_list(names: &[Vec<u8>]) -> Vec<u8> {
    let total_len = xattr_name_list_len(names);
    let mut out = Vec::with_capacity(total_len);
    for name in names {
        out.extend_from_slice(name);
        out.push(0);
    }
    out
}

fn xattr_name_list_len(names: &[Vec<u8>]) -> usize {
    names.iter().map(|name| name.len() + 1).sum()
}

fn xattr_not_found_errno() -> i32 {
    libc::ENOATTR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_buffer_pool_keeps_recycled_initialized_prefix_len() {
        let pool = Arc::new(ReadBufferPool::default());
        let mut buffer = Arc::clone(&pool).acquire(16);
        buffer.truncate(5);

        drop(buffer);

        assert_eq!(pool.available_count(), 1);
        assert_eq!(pool.last_available_len(), Some(5));
        assert!(
            pool.last_available_capacity()
                .expect("recycled buffer should retain capacity")
                >= 16
        );
    }

    #[test]
    fn readahead_window_len_uses_floor_for_small_reads() {
        assert_eq!(readahead_window_len(0), 0);
        assert_eq!(
            readahead_window_len(4 * 1024),
            MACFUSE_READ_WINDOW_MIN_BYTES
        );
        assert_eq!(
            readahead_window_len(MACFUSE_READ_WINDOW_MAX_BYTES),
            MACFUSE_READ_WINDOW_MAX_BYTES
        );
    }

    #[test]
    fn setattr_mask_accepts_bundled_size_changes_and_gates_file_handle() {
        // SAFETY: The C shim returns the platform header value for this build.
        let size_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_size_mask() };
        // SAFETY: The C shim returns the platform header value for this build.
        let fh_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_fh_mask() };
        // SAFETY: The C shim returns the platform header value for this build.
        let mode_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_mode_mask() };

        assert!(setattr_mask_has_size(size_mask));
        assert!(setattr_mask_has_size(size_mask | mode_mask));
        assert!(!setattr_mask_has_size(0));
        assert!(!setattr_mask_has_fh(size_mask));
        assert_eq!(setattr_mask_has_fh(size_mask | fh_mask), fh_mask != 0);
    }

    #[test]
    fn setattr_args_preserve_bundled_size_mode_and_mtime() {
        // SAFETY: The C shim returns platform header values for this build.
        let mask = unsafe {
            crate::low_level::studiofs_macfuse_setattr_size_mask()
                | crate::low_level::studiofs_macfuse_setattr_mode_mask()
                | crate::low_level::studiofs_macfuse_setattr_mtime_mask()
        };
        let mut attr = minimal_stat(0);
        attr.st_size = 42;
        attr.st_mode = libc::S_IFREG | 0o640;
        attr.st_mtime = 123;
        attr.st_mtime_nsec = 456;

        let args = setattr_args_from_callback(&attr, mask).expect("build setattr args");

        assert_eq!(args.size, Some(42));
        assert_eq!(args.mode, Some(0o640));
        assert!(matches!(args.mtime, TimeSpec::Set(123_000_000_456)));
    }

    #[test]
    fn setattr_supported_mask_accepts_macos_metadata_bits() {
        // SAFETY: The C shim returns platform header values for this build, or
        // zero for optional bits absent from the active FUSE headers.
        let mask = unsafe {
            crate::low_level::studiofs_macfuse_setattr_size_mask()
                | crate::low_level::studiofs_macfuse_setattr_chgtime_mask()
                | crate::low_level::studiofs_macfuse_setattr_bkuptime_mask()
                | crate::low_level::studiofs_macfuse_setattr_flags_mask()
        };

        assert!(!setattr_mask_has_unknown(mask));
    }

    #[test]
    fn xattr_name_list_len_counts_nul_separated_wire_bytes() {
        let names = vec![b"user.alpha".to_vec(), b"com.apple.ResourceFork".to_vec()];

        assert_eq!(
            xattr_name_list_len(&names),
            "user.alpha\0com.apple.ResourceFork\0".len()
        );
        assert_eq!(
            format_xattr_name_list(&names),
            b"user.alpha\0com.apple.ResourceFork\0"
        );
    }

    #[test]
    fn xattr_position_offsets_size_probes_and_value_reads() {
        let value = b"resource-fork-payload";

        assert_eq!(xattr_size_from_position(value.len(), 9), 12);
        assert_eq!(xattr_size_from_position(value.len(), value.len() + 8), 0);
        assert_eq!(xattr_value_from_position(value, 9), b"fork-payload");
        assert_eq!(xattr_value_from_position(value, value.len() + 8), b"");
    }
}
