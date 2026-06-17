//! Native Linux FUSE operation adapter backed by StudioFS VFS contracts.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use studiofs_bridge::posix::{posix_flags_to_open_flags, vfs_err_to_errno};
use studiofs_meta::{DirEntry, FileType, InodeAttr};
use studiofs_vfs::{LocalSyncPolicy, OpenFlags, SetattrArgs, StatFs, StudioVfs};
use tokio::runtime::Handle;
use tracing::warn;

use crate::native::abi::{error_reply, ok_reply};
use crate::native::dispatcher::FuseOperationHandler;
use crate::native::protocol::{FuseOpcode, FuseReply, FuseRequest};

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
const FATTR_FH: u32 = 1 << 0;
const FATTR_SIZE: u32 = 1 << 3;
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

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

/// VFS operations required by the native Linux bridge operation layer.
pub trait NativeVfs {
    /// Resolve `name` under `parent`.
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<NativeAttr, i32>;

    /// Fetch attributes for `ino`.
    fn getattr(&self, ino: u64) -> Result<NativeAttr, i32>;

    /// Open a regular file and return a VFS file handle.
    fn open(&self, ino: u64, flags: i32) -> Result<u64, i32>;

    /// Release a previously opened file handle.
    fn release(&self, fh: u64) -> Result<(), i32>;

    /// Read bytes from an open file handle.
    fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32>;

    /// Write bytes to an open file handle.
    fn write(&self, fh: u64, ino: u64, offset: u64, data: &[u8], flags: i32) -> Result<u32, i32>;

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

    fn release(&self, fh: u64) -> Result<(), i32> {
        (**self).release(fh)
    }

    fn read(&self, fh: u64, ino: u64, offset: u64, size: u32) -> Result<Vec<u8>, i32> {
        (**self).read(fh, ino, offset, size)
    }

    fn write(&self, fh: u64, ino: u64, offset: u64, data: &[u8], flags: i32) -> Result<u32, i32> {
        (**self).write(fh, ino, offset, data, flags)
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
        if let Some(entries) = self.directory_handles.entries(ino, fh) {
            return Ok(entries);
        }
        self.vfs
            .readdir(ino)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(native_dir_entry)
                    .collect::<Vec<_>>()
                    .into()
            })
            .map_err(|error| vfs_err_to_errno(&error))
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
    lookup_counts: NativeLookupCountStore,
}

impl<V> LinuxNativeBridge<V> {
    /// Build an operation adapter around a VFS backend.
    pub fn new(vfs: V) -> Self {
        Self {
            vfs,
            lookup_counts: NativeLookupCountStore::default(),
        }
    }

    /// Returns the kernel lookup reference count currently tracked for `ino`.
    pub fn kernel_lookup_count(&self, ino: u64) -> u64 {
        self.lookup_counts.count(ino)
    }
}

impl<V: NativeVfs> FuseOperationHandler for LinuxNativeBridge<V> {
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

impl<V: NativeVfs> LinuxNativeBridge<V> {
    fn handle_lookup(&self, request: FuseRequest) -> FuseReply {
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
        match self.vfs.getattr(request.header.nodeid) {
            Ok(attr) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &attr_out_payload(attr)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_open(&self, request: FuseRequest) -> FuseReply {
        let flags = match open_flags(&request.payload) {
            Some(flags) => flags,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.open(request.header.nodeid, flags) {
            Ok(fh) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &open_payload(fh, flags)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_release(&self, request: FuseRequest) -> FuseReply {
        let fh = match release_fh(&request.payload) {
            Some(fh) => fh,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.release(fh) {
            Ok(()) => {
                FuseReply::contiguous(request.header.unique, ok_reply(request.header.unique, &[]))
            }
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_read(&self, request: FuseRequest) -> FuseReply {
        let read = match read_request(&request.payload) {
            Some(read) => read,
            None => return invalid_request(request.header.unique),
        };
        if let Err(errno) = checked_native_read_size(read.size) {
            return errno_reply(request.header.unique, errno);
        }

        match self
            .vfs
            .read(read.fh, request.header.nodeid, read.offset, read.size)
        {
            Ok(data) => FuseReply::segmented_data(request.header.unique, data),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_write(&self, request: FuseRequest) -> FuseReply {
        let write = match write_request(&request.payload) {
            Some(write) => write,
            None => return invalid_request(request.header.unique),
        };

        match self.vfs.write(
            write.fh,
            request.header.nodeid,
            write.offset,
            write.data,
            write.flags,
        ) {
            Ok(size) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &write_payload(size)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn handle_flush(&self, request: FuseRequest) -> FuseReply {
        let fh = match flush_fh(&request.payload) {
            Some(fh) => fh,
            None => return invalid_request(request.header.unique),
        };

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
                ok_reply(request.header.unique, &open_payload(fh, 0)),
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
                        Err(errno) => return errno_reply(request.header.unique, errno),
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
                self.add_lookup(attr.ino, 1);
                FuseReply::contiguous(
                    request.header.unique,
                    ok_reply(
                        request.header.unique,
                        &create_payload(attr, fh, create.flags),
                    ),
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
        let truncate = match truncate_request(&request.payload) {
            Some(truncate) => truncate,
            None => return invalid_request(request.header.unique),
        };

        match self
            .vfs
            .truncate(request.header.nodeid, truncate.fh, truncate.size)
        {
            Ok(attr) => FuseReply::contiguous(
                request.header.unique,
                ok_reply(request.header.unique, &attr_out_payload(attr)),
            ),
            Err(errno) => errno_reply(request.header.unique, errno),
        }
    }

    fn add_lookup(&self, ino: u64, nlookup: u64) {
        self.lookup_counts.add(ino, nlookup);
    }

    fn forget_lookup(&self, ino: u64, nlookup: u64) {
        self.lookup_counts.forget(ino, nlookup);
    }
}

fn invalid_request(unique: u64) -> FuseReply {
    errno_reply(unique, libc::EINVAL)
}

fn errno_reply(unique: u64, errno: i32) -> FuseReply {
    FuseReply::contiguous(unique, error_reply(unique, errno).to_vec())
}

fn lookup_name(payload: &[u8]) -> Option<&[u8]> {
    let name = payload.strip_suffix(&[0]).unwrap_or(payload);
    (!name.is_empty()).then_some(name)
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
struct TruncateRequest {
    fh: Option<u64>,
    size: u64,
}

fn truncate_request(payload: &[u8]) -> Option<TruncateRequest> {
    let bytes = payload.get(..FUSE_SETATTR_IN_LEN)?;
    let valid = read_u32(bytes, 0)?;
    if valid & FATTR_SIZE == 0 {
        return None;
    }
    let fh = if valid & FATTR_FH != 0 {
        Some(read_u64(bytes, 8)?)
    } else {
        None
    };
    Some(TruncateRequest {
        fh,
        size: read_u64(bytes, 16)?,
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

fn attr_out_payload(attr: NativeAttr) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16 + FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&1_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    append_attr_payload(&mut payload, attr);
    payload
}

fn open_payload(fh: u64, flags: i32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&fh.to_le_bytes());
    payload.extend_from_slice(&open_reply_flags(flags).to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload
}

fn create_payload(attr: NativeAttr, fh: u64, flags: i32) -> Vec<u8> {
    let mut payload = entry_payload(attr);
    payload.extend_from_slice(&open_payload(fh, flags));
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

fn open_reply_flags(flags: i32) -> u32 {
    let (open_flags, _) = posix_flags_to_open_flags(flags);
    if !open_flags.contains(OpenFlags::WRONLY) && !open_flags.contains(OpenFlags::RDWR) {
        FOPEN_KEEP_CACHE
    } else {
        0
    }
}

fn append_attr_payload(payload: &mut Vec<u8>, attr: NativeAttr) {
    payload.reserve(FUSE_ATTR_PAYLOAD_LEN);
    payload.extend_from_slice(&attr.ino.to_le_bytes());
    payload.extend_from_slice(&attr.size.to_le_bytes());
    payload.extend_from_slice(&attr.size.div_ceil(POSIX_STAT_BLOCK_SIZE).to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u64.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&linux_mode(attr).to_le_bytes());
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(&attr.uid.to_le_bytes());
    payload.extend_from_slice(&attr.gid.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
    payload.extend_from_slice(&4096_u32.to_le_bytes());
    payload.extend_from_slice(&0_u32.to_le_bytes());
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

    fn forget(&self, ino: u64, nlookup: u64) {
        let mut lookup_counts = lock_or_recover(self.shard(ino), "lookup_counts");
        let Some(count) = lookup_counts.get_mut(&ino) else {
            return;
        };
        if *count <= nlookup {
            lookup_counts.remove(&ino);
        } else {
            *count -= nlookup;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct TestNativeVfs {
        read_calls: AtomicU64,
    }

    impl NativeVfs for TestNativeVfs {
        fn lookup(&self, _parent: u64, _name: &[u8]) -> Result<NativeAttr, i32> {
            unimplemented!("not used by these tests")
        }

        fn getattr(&self, ino: u64) -> Result<NativeAttr, i32> {
            if ino == 21 {
                return Err(libc::EIO);
            }
            Ok(test_attr(ino))
        }

        fn open(&self, _ino: u64, _flags: i32) -> Result<u64, i32> {
            unimplemented!("not used by these tests")
        }

        fn release(&self, _fh: u64) -> Result<(), i32> {
            unimplemented!("not used by these tests")
        }

        fn read(&self, _fh: u64, _ino: u64, _offset: u64, _size: u32) -> Result<Vec<u8>, i32> {
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        fn write(
            &self,
            _fh: u64,
            _ino: u64,
            _offset: u64,
            _data: &[u8],
            _flags: i32,
        ) -> Result<u32, i32> {
            unimplemented!("not used by these tests")
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
            unimplemented!("not used by these tests")
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

        fn truncate(&self, _ino: u64, _fh: Option<u64>, _size: u64) -> Result<NativeAttr, i32> {
            unimplemented!("not used by these tests")
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

    fn read_request_for_size(size: u32) -> FuseRequest {
        let mut payload = vec![0; FUSE_READ_IN_LEN];
        payload[0..8].copy_from_slice(&7_u64.to_le_bytes());
        payload[8..16].copy_from_slice(&13_u64.to_le_bytes());
        payload[16..20].copy_from_slice(&size.to_le_bytes());
        FuseRequest {
            header: crate::native::abi::FuseInHeader::new(
                40 + FUSE_READ_IN_LEN as u32,
                15,
                99,
                20,
                0,
                0,
                0,
            ),
            opcode: FuseOpcode::Read,
            payload,
        }
    }

    fn reply_errno(reply: &FuseReply) -> i32 {
        i32::from_le_bytes(reply.to_vec()[4..8].try_into().expect("reply errno"))
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
    fn read_u32_returns_none_when_range_exceeds_frame() {
        assert_eq!(read_u32(&[1, 2, 3], 0), None);
    }

    #[test]
    fn read_u64_returns_none_when_range_exceeds_frame() {
        assert_eq!(read_u64(&[1, 2, 3, 4, 5, 6, 7], 0), None);
    }
}
