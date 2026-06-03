use std::collections::HashMap;
use std::ffi::{CStr, CString, OsStr};
use std::mem::MaybeUninit;
use std::os::raw::c_char;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use libc::{off_t, size_t, stat, statvfs};
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::{DirEntry, FileType, InodeAttr, SetattrArgs};
use studiofs_vfs::{StatFs, StudioVfs};
use tokio::runtime::Handle;

use crate::low_level::{DirectoryEntryBuffer, Entry, OpenReply, Reply, Request, raw};

const TTL_SECONDS: f64 = 1.0;
const MACFUSE_ATTR_BLOCK_SIZE: u32 = 512;
const READDIR_CACHE_TTL: Duration = Duration::from_secs(1);
const READDIR_CACHE_PRUNE_THRESHOLD: usize = 1024;
const READDIR_CACHE_PRUNE_INTERVAL: Duration = Duration::from_secs(1);

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
        Self {
            vfs,
            runtime_handle,
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            readdir_cache: Mutex::new(HashMap::new()),
            readdir_cache_last_pruned: Mutex::new(None),
            readdir_cache_ready: Condvar::new(),
        }
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

    /// Open an existing file.
    pub(crate) fn open(&self, ino: u64, flags: i32) -> Result<OpenReply, i32> {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && posix_open_requests_write(flags, trunc) {
            return Err(libc::EROFS);
        }
        self.vfs
            .open_file(ino, open_flags, trunc)
            .map(|fh| OpenReply::new(fh).with_keep_cache(true))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Read file data through a VFS file handle and record FUSE read diagnostics.
    pub(crate) fn read(
        &self,
        ino: u64,
        file_handle: u64,
        offset: i64,
        size: u32,
    ) -> Result<Vec<u8>, i32> {
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size_usize = size as usize;
        if size_usize == 0 {
            return Ok(Vec::new());
        }

        let mut out = vec![0u8; size_usize];
        let started = Instant::now();
        let result =
            self.runtime_handle
                .block_on(self.vfs.read_into_fh(file_handle, ino, offset, &mut out));
        self.vfs
            .record_fuse_read_callback(size_usize, started.elapsed(), result.is_err());
        match result {
            Ok(written) => {
                out.truncate(written as usize);
                Ok(out)
            }
            Err(error) => Err(vfs_err_to_errno(&error)),
        }
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
        self.runtime_handle
            .block_on(self.vfs.write_fh(file_handle, ino, offset, data))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Flush file data through a VFS file handle.
    pub(crate) fn flush(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.flush_fh(file_handle))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Synchronize file data for an inode.
    pub(crate) fn fsync(&self, ino: u64, datasync: bool) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.fsync(ino, datasync))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Change a file length through the VFS setattr path.
    pub(crate) fn set_len(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        size: u64,
    ) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .setattr_with_handle(file_handle, ino, SetattrArgs::size_only(size))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    /// Release a file handle opened through this adapter.
    pub(crate) fn release(&self, _ino: u64, file_handle: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.release_fh(file_handle))
            .map_err(|error| vfs_err_to_errno(&error))
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
    match state.adapter.read(ino, file_handle, off, size) {
        Ok(bytes) => {
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).read_buffer(&bytes);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
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
    let data = match callback_write_buffer(buf, size) {
        Ok(data) => data,
        Err(errno) => {
            reply_error(req, errno);
            return;
        }
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
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.adapter.fsync(ino, datasync != 0) {
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
    if !setattr_mask_is_size_only(to_set) {
        reply_error(req, libc::ENOTSUP);
        return;
    }
    // SAFETY: macFUSE passes an initialized `stat` pointer when setattr runs.
    let size = unsafe { (*attr).st_size };
    let size = match u64::try_from(size) {
        Ok(size) => size,
        Err(_) => {
            reply_error(req, libc::EINVAL);
            return;
        }
    };
    let file_handle = if fi.is_null() {
        None
    } else {
        // SAFETY: Non-null `fi` was supplied by macFUSE for handle-backed setattr.
        Some(unsafe { (*fi).fh })
    };
    match state.adapter.set_len(file_handle, ino, size) {
        Ok(attr) => {
            let stat = state.adapter.attr_to_stat(&attr);
            if let Some(request) = owned_request(req) {
                let _ = Reply::new(request).attr(&stat, TTL_SECONDS);
            }
        }
        Err(errno) => reply_error(req, errno),
    }
}

fn callback_write_buffer<'a>(buf: *const c_char, size: size_t) -> Result<&'a [u8], i32> {
    if size == 0 {
        return Ok(&[]);
    }
    if buf.is_null() {
        return Err(libc::EINVAL);
    }
    // SAFETY: macFUSE passes a valid buffer of `size` bytes for non-empty writes.
    Ok(unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size) })
}

fn setattr_mask_is_size_only(to_set: libc::c_int) -> bool {
    // SAFETY: The C shim uses the same macFUSE headers and compares the callback
    // mask against the header-defined `FUSE_SET_ATTR_SIZE` bit.
    unsafe { crate::low_level::studiofs_macfuse_setattr_to_set_is_size_only(to_set) != 0 }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_write_buffer_accepts_null_only_for_empty_write() {
        let empty = callback_write_buffer(std::ptr::null(), 0).expect("empty null write is valid");
        assert!(empty.is_empty());

        assert_eq!(
            callback_write_buffer(std::ptr::null(), 1).unwrap_err(),
            libc::EINVAL
        );

        let bytes = b"abc";
        let data = callback_write_buffer(bytes.as_ptr().cast::<c_char>(), bytes.len())
            .expect("non-empty write buffer");
        assert_eq!(data, bytes);
    }

    #[test]
    fn setattr_mask_accepts_only_pure_size_change() {
        // SAFETY: The C shim returns the platform header value for this build.
        let size_mask = unsafe { crate::low_level::studiofs_macfuse_setattr_size_mask() };

        assert!(setattr_mask_is_size_only(size_mask));
        assert!(!setattr_mask_is_size_only(0));
        assert!(!setattr_mask_is_size_only(size_mask | (size_mask << 1)));
    }
}
