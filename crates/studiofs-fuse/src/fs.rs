//! fuser::Filesystem implementation — bridges FUSE kernel calls to StudioVfs.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr,
    Request,
};
use studiofs_meta::FileType;
use studiofs_meta::engine::{XATTR_CREATE_FLAG, XATTR_REPLACE_FLAG};
use studiofs_meta::types::{SetattrArgs, TimeSpec};
use studiofs_vfs::{KernelInvalidator, LockKind, OpenFlags, StudioVfs, VfsError};
use tokio::runtime::Runtime;
use tracing::{debug, error, warn};

const TTL: Duration = Duration::from_secs(1);

fn os_str_bytes(name: &OsStr) -> &[u8] {
    name.as_bytes()
}

fn debug_name(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(target_os = "macos")]
fn merge_xattr_write(existing: Option<Vec<u8>>, position: u32, value: &[u8]) -> Vec<u8> {
    let offset = position as usize;
    let existing_len = existing.as_ref().map_or(0, Vec::len);
    let new_len = existing_len.max(offset.saturating_add(value.len()));
    let mut merged = vec![0; new_len];

    if let Some(existing) = existing {
        merged[..existing.len()].copy_from_slice(&existing);
    }
    merged[offset..offset + value.len()].copy_from_slice(value);
    merged
}

#[cfg(target_os = "macos")]
fn normalize_macos_xattr_flags(flags: i32) -> u32 {
    const MACOS_XATTR_CREATE: i32 = 0x0002;
    const MACOS_XATTR_REPLACE: i32 = 0x0004;
    let mut normalized = 0;
    if flags & MACOS_XATTR_CREATE != 0 {
        normalized |= XATTR_CREATE_FLAG;
    }
    if flags & MACOS_XATTR_REPLACE != 0 {
        normalized |= XATTR_REPLACE_FLAG;
    }
    normalized
}

fn normalize_unix_mode(mode: u32) -> u16 {
    // VFS stores only Unix permission bits; file type is tracked separately.
    (mode & 0o7777) as u16
}

/// Convert POSIX O_* flags to our OpenFlags.
fn posix_flags_to_open_flags(flags: i32) -> (OpenFlags, bool) {
    let access = flags & libc::O_ACCMODE;
    let mut f = match access {
        libc::O_RDONLY => OpenFlags::RDONLY,
        libc::O_WRONLY => OpenFlags::WRONLY,
        libc::O_RDWR => OpenFlags::RDWR,
        _ => OpenFlags::RDONLY,
    };
    if flags & libc::O_APPEND != 0 {
        f = f | OpenFlags::APPEND;
    }
    let trunc = flags & libc::O_TRUNC != 0;
    (f, trunc)
}

// ─── Error Mapping ───────────────────────────────────────────────────────────

/// Map VfsError to the correct libc errno for FUSE replies.
fn vfs_err_to_errno(e: &VfsError) -> i32 {
    use studiofs_meta::MetaError;
    match e {
        VfsError::Meta(me) => match me {
            MetaError::InodeNotFound(_) => libc::ENOENT,
            MetaError::EntryNotFound { .. } => libc::ENOENT,
            MetaError::EntryExists { .. } => libc::EEXIST,
            MetaError::NotADirectory(_) => libc::ENOTDIR,
            MetaError::IsADirectory(_) => libc::EISDIR,
            MetaError::DirNotEmpty(_) => libc::ENOTEMPTY,
            MetaError::NotFormatted => libc::ENODEV,
            _ => libc::EIO,
        },
        VfsError::BlockNotFound(_, _) => libc::EIO,
        VfsError::WouldBlock | VfsError::RemoteContentPending => libc::EAGAIN,
        VfsError::InvalidName(_) => libc::EINVAL,
        VfsError::Io(ioe) => match ioe.kind() {
            std::io::ErrorKind::NotFound => libc::ENOENT,
            std::io::ErrorKind::PermissionDenied => libc::EACCES,
            std::io::ErrorKind::AlreadyExists => libc::EEXIST,
            std::io::ErrorKind::WouldBlock => libc::EAGAIN,
            std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => libc::EINVAL,
            _ => libc::EIO,
        },
        _ => libc::EIO,
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to fuser FileAttr.
/// `mount_uid`/`mount_gid` are used to remap inodes that were created with
/// uid=0/gid=0 (the old default) so that the mounting user owns them.
fn to_fuse_attr(attr: &studiofs_meta::InodeAttr, mount_uid: u32, mount_gid: u32) -> FileAttr {
    let uid = if attr.uid == 0 { mount_uid } else { attr.uid };
    let gid = if attr.gid == 0 { mount_gid } else { attr.gid };
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: (attr.size + 511) / 512,
        atime: UNIX_EPOCH + Duration::from_nanos(attr.atime.max(0) as u64),
        mtime: UNIX_EPOCH + Duration::from_nanos(attr.mtime.max(0) as u64),
        ctime: UNIX_EPOCH + Duration::from_nanos(attr.ctime.max(0) as u64),
        crtime: UNIX_EPOCH + Duration::from_nanos(attr.effective_crtime().max(0) as u64),
        kind: match attr.kind {
            FileType::RegularFile => FuseFileType::RegularFile,
            FileType::Directory => FuseFileType::Directory,
            FileType::Symlink => FuseFileType::Symlink,
        },
        perm: attr.mode & 0o7777,
        nlink: attr.nlink as u32,
        uid,
        gid,
        rdev: 0,
        blksize: 512,
        flags: 0,
    }
}

fn fuse_file_type(kind: FileType) -> FuseFileType {
    match kind {
        FileType::RegularFile => FuseFileType::RegularFile,
        FileType::Directory => FuseFileType::Directory,
        FileType::Symlink => FuseFileType::Symlink,
    }
}

// ─── StudioFuseFs ────────────────────────────────────────────────────────────

pub struct StudioFuseFs {
    vfs: Arc<StudioVfs>,
    rt: Arc<Runtime>,
    /// UID of the user that mounted the filesystem. Inodes owned by uid=0 in
    /// the DB are remapped to this so macOS permission checks don't block writes.
    mount_uid: u32,
    /// GID of the user that mounted the filesystem.
    mount_gid: u32,
}

impl StudioFuseFs {
    pub fn new(vfs: Arc<StudioVfs>, rt: Arc<Runtime>) -> Self {
        let mount_uid = unsafe { libc::getuid() };
        let mount_gid = unsafe { libc::getgid() };
        Self {
            vfs,
            rt,
            mount_uid,
            mount_gid,
        }
    }
}

impl Filesystem for StudioFuseFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.lookup(parent, &name_bytes) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                debug!("lookup({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.vfs.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                debug!("getattr({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.mkdir(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "mkdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("mknod({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => {
                // Open a handle for the newly created file
                let (open_flags, _trunc) = posix_flags_to_open_flags(flags);
                match self.vfs.open_file(attr.ino, open_flags, false) {
                    Ok(fh) => {
                        reply.created(
                            &TTL,
                            &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                            0,
                            fh,
                            0,
                        );
                    }
                    Err(e) => {
                        error!("create open_file({}) -> {e}", attr.ino);
                        reply.error(vfs_err_to_errno(&e));
                    }
                }
            }
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    flags,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "create -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.unlink(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "unlink -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        match self.vfs.rmdir(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "rmdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(link_name).to_vec();
        let tgt = target.as_os_str().as_bytes().to_vec();
        match self.vfs.symlink(parent, &name, &tgt, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("symlink({parent}, {:?}) -> {e}", link_name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.vfs.readlink(ino) {
            Ok(target) => reply.data(&target),
            Err(e) => {
                error!("readlink({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(newname).to_vec();
        match self.vfs.link(ino, newparent, &name) {
            Ok(attr) => {
                let fattr = to_fuse_attr(&attr, self.mount_uid, self.mount_gid);
                reply.entry(&TTL, &fattr, 0);
            }
            Err(e) => {
                error!("link({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let old_name = os_str_bytes(name).to_vec();
        let new_name = os_str_bytes(newname).to_vec();
        match self.vfs.rename(parent, &old_name, newparent, &new_name) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    newparent,
                    flags = _flags,
                    old_name = %debug_name(&old_name),
                    new_name = %debug_name(&new_name),
                    old_name_bytes = ?old_name,
                    new_name_bytes = ?new_name,
                    "rename -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn exchange(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        options: u64,
        reply: ReplyEmpty,
    ) {
        let old_name = os_str_bytes(name).to_vec();
        let new_name = os_str_bytes(newname).to_vec();
        warn!(
            parent,
            newparent,
            options,
            old_name = %debug_name(&old_name),
            new_name = %debug_name(&new_name),
            old_name_bytes = ?old_name,
            new_name_bytes = ?new_name,
            "macOS exchange is not implemented"
        );
        reply.error(libc::ENOSYS);
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        match self.vfs.open_file(ino, open_flags, trunc) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => {
                if matches!(e, VfsError::WouldBlock | VfsError::RemoteContentPending) {
                    debug!("open({ino}) -> {e}");
                } else {
                    error!("open({ino}) -> {e}");
                }
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.rt.block_on(self.vfs.read(ino, offset as u64, size)) {
            Ok(data) => reply.data(&data),
            Err(e) => {
                error!("read({ino}, off={offset}, sz={size}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.rt.block_on(self.vfs.write(ino, offset as u64, data)) {
            Ok(written) => reply.written(written),
            Err(e) => {
                error!("write({ino}, off={offset}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // flush_fh uses the handle to resolve the ino, but if fh==0
        // (old path / no handle), fall back to ino-based flush.
        if fh != 0 {
            match self.rt.block_on(self.vfs.flush_fh(fh)) {
                Ok(()) => reply.ok(),
                Err(e) => {
                    error!("flush_fh({fh}) -> {e}");
                    reply.error(vfs_err_to_errno(&e));
                }
            }
        } else {
            match self.rt.block_on(self.vfs.flush(ino)) {
                Ok(()) => reply.ok(),
                Err(e) => {
                    error!("flush({ino}) -> {e}");
                    reply.error(vfs_err_to_errno(&e));
                }
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.rt.block_on(self.vfs.release_fh(fh)) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("release({ino}, fh={fh}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        match self.rt.block_on(self.vfs.fsync(ino, datasync)) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("fsync({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdir({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        // Offset-based iteration (offset 0 = ".", 1 = "..", 2+ = entries)
        let mut idx = offset as usize;
        if idx == 0 {
            if reply.add(ino, 1, FuseFileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }
        if idx == 1 {
            // Parent (simplified: use same ino)
            if reply.add(ino, 2, FuseFileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        for (i, entry) in entries.iter().enumerate() {
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let ft = fuse_file_type(entry.kind);
            if reply.add(
                entry.ino,
                (entry_idx + 1) as i64,
                ft,
                OsStr::from_bytes(&entry.name),
            ) {
                break;
            }
        }

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdirplus({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        let mut idx = offset as usize;

        // "." entry
        if idx == 0 {
            let dot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 1, ".", &TTL, &dot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 1;
        }

        // ".." entry
        if idx == 1 {
            let dotdot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 2, "..", &TTL, &dotdot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 2;
        }

        // Regular entries with full attributes
        for (i, entry) in entries.iter().enumerate() {
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let fattr = match self.vfs.getattr(entry.ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(_) => continue, // skip entries we can't stat
            };
            if reply.add(
                entry.ino,
                (entry_idx + 1) as i64,
                OsStr::from_bytes(&entry.name),
                &TTL,
                &fattr,
                0,
            ) {
                break;
            }
        }

        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let args = SetattrArgs {
            size,
            mode: mode.map(normalize_unix_mode),
            uid,
            gid,
            atime: match atime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => TimeSpec::Set(
                    st.duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                ),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            mtime: match mtime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => TimeSpec::Set(
                    st.duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                ),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            crtime: match _crtime {
                Some(st) => TimeSpec::Set(
                    st.duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos() as i64)
                        .unwrap_or(0),
                ),
                None => TimeSpec::Omit,
            },
            win_attrs: None,
        };
        match self.vfs.setattr(ino, args) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                error!(
                    ino,
                    mode = ?mode,
                    uid = ?uid,
                    gid = ?gid,
                    size = ?size,
                    atime = ?atime,
                    mtime = ?mtime,
                    crtime = ?_crtime,
                    chgtime = ?_chgtime,
                    bkuptime = ?_bkuptime,
                    flags = ?_flags,
                    "setattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        match self.vfs.statfs() {
            Ok(st) => reply.statfs(
                st.blocks, st.bfree, st.bavail, st.files, st.ffree, st.bsize, st.namelen, st.frsize,
            ),
            Err(e) => {
                error!("statfs -> {e}");
                // Fallback to hardcoded values on error
                reply.statfs(
                    1024 * 1024 * 1024,
                    512 * 1024 * 1024,
                    512 * 1024 * 1024,
                    1_000_000,
                    500_000,
                    4096,
                    255,
                    4096,
                );
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&mut self, _req: &Request<'_>, ino: u64, reply: fuser::ReplyXTimes) {
        match self.vfs.getattr(ino) {
            Ok(attr) => {
                let crtime =
                    UNIX_EPOCH + Duration::from_nanos(attr.effective_crtime().max(0) as u64);
                let bkuptime = UNIX_EPOCH + Duration::from_nanos(attr.mtime.max(0) as u64);
                reply.xtimes(bkuptime, crtime);
            }
            Err(e) => {
                error!(ino, "getxtimes -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    // macOS calls access() to determine if writing is allowed.
    // With DefaultPermissions set the kernel already checked mode bits,
    // so we just say OK here.
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let key = name.as_encoded_bytes();

        match self.vfs.get_xattr(ino, key) {
            Ok(Some(val)) => {
                if size == 0 {
                    reply.size(val.len() as u32);
                } else if val.len() <= size as usize {
                    reply.data(&val);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Ok(None) => {
                #[cfg(target_os = "macos")]
                reply.error(libc::ENOATTR);
                #[cfg(not(target_os = "macos"))]
                reply.error(libc::ENODATA);
            }
            Err(e) => {
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    size,
                    "getxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let key = name.as_encoded_bytes();

        #[cfg(target_os = "macos")]
        let normalized_flags = normalize_macos_xattr_flags(flags);
        #[cfg(not(target_os = "macos"))]
        let normalized_flags = flags as u32;

        #[cfg(target_os = "macos")]
        let value_buf = if _position > 0 {
            match self.vfs.get_xattr(ino, key) {
                Ok(existing) => merge_xattr_write(existing, _position, value),
                Err(e) => {
                    error!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        position = _position,
                        value_len = value.len(),
                        flags,
                        "setxattr preload -> {e}"
                    );
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            }
        } else {
            value.to_vec()
        };

        #[cfg(not(target_os = "macos"))]
        let value_buf = value.to_vec();

        match self.vfs.set_xattr(ino, key, &value_buf, normalized_flags) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    value_len = value.len(),
                    stored_value_len = value_buf.len(),
                    flags,
                    normalized_flags,
                    position = _position,
                    "setxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let key = name.as_encoded_bytes();

        match self.vfs.remove_xattr(ino, key) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    "removexattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        match self.vfs.list_xattrs(ino) {
            Ok(names) => {
                // xattr list format: null-terminated name strings concatenated
                let data: Vec<u8> = names
                    .iter()
                    .flat_map(|n| n.iter().cloned().chain(std::iter::once(0u8)))
                    .collect();
                if size == 0 {
                    reply.size(data.len() as u32);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        // File-level semantics only (byte ranges ignored)
        let op = match typ {
            x if x == libc::F_RDLCK as i32 => LockKind::Shared,
            x if x == libc::F_WRLCK as i32 => LockKind::Exclusive,
            x if x == libc::F_UNLCK as i32 => LockKind::Unlock,
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        match self.rt.block_on(self.vfs.flock(ino, op, lock_owner, sleep)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        // Report no conflicting locks (single-node simplification)
        reply.locked(start, end, libc::F_UNLCK as i32, 0);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        // For cloud FS, preallocation is a no-op — no local disk to reserve.
        // We only extend the file size if FALLOC_FL_KEEP_SIZE is not set.
        if mode == 0 {
            // FALLOC_FL_KEEP_SIZE not set → extend size if needed
            match self.vfs.getattr(ino) {
                Ok(attr) => {
                    let new_size = offset as u64 + length as u64;
                    if new_size > attr.size {
                        let args = SetattrArgs {
                            size: Some(new_size),
                            ..Default::default()
                        };
                        if let Err(e) = self.vfs.setattr(ino, args) {
                            error!("fallocate setattr({ino}) -> {e}");
                            reply.error(vfs_err_to_errno(&e));
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("fallocate getattr({ino}) -> {e}");
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            }
        }
        reply.ok();
    }
}

// ─── Kernel Cache Invalidator ────────────────────────────────────────────────

/// Bridges `fuser::Notifier` to the VFS `KernelInvalidator` trait so that
/// Metadata refreshes can immediately clear the Linux kernel's
/// FUSE dentry and attribute caches.
pub struct FuseKernelInvalidator {
    notifier: fuser::Notifier,
}

impl FuseKernelInvalidator {
    pub fn new(notifier: fuser::Notifier) -> Self {
        Self { notifier }
    }
}

fn should_log_fuse_invalidation_error(err: &std::io::Error) -> bool {
    err.kind() != std::io::ErrorKind::NotFound
}

impl KernelInvalidator for FuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        // offset=0, len=-1 invalidates the entire inode (attributes + all
        // cached page data / directory listing).
        if let Err(e) = self.notifier.inval_inode(ino, 0, -1) {
            // ENOENT is harmless (kernel already evicted the entry).
            if should_log_fuse_invalidation_error(&e) {
                warn!(ino, error = %e, "FUSE notify_inval_inode failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_unix_mode, posix_flags_to_open_flags, should_log_fuse_invalidation_error,
        vfs_err_to_errno,
    };

    #[cfg(target_os = "macos")]
    use super::{merge_xattr_write, normalize_macos_xattr_flags};
    use studiofs_meta::MetaError;
    use studiofs_vfs::{OpenFlags, VfsError};

    #[test]
    fn normalize_unix_mode_strips_file_type_bits() {
        assert_eq!(normalize_unix_mode(0o40755), 0o755);
        assert_eq!(normalize_unix_mode(0o100644), 0o644);
        assert_eq!(normalize_unix_mode(0o100644 | 0o4000), 0o4644);
    }

    #[test]
    fn fuse_invalidation_not_found_is_benign() {
        let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);
        let other = std::io::Error::from(std::io::ErrorKind::PermissionDenied);

        assert!(!should_log_fuse_invalidation_error(&not_found));
        assert!(should_log_fuse_invalidation_error(&other));
    }

    #[test]
    fn vfs_error_maps_to_expected_errno_codes() {
        let not_found = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "missing".into(),
        });
        assert_eq!(vfs_err_to_errno(&not_found), libc::ENOENT);

        let exists = VfsError::Meta(MetaError::EntryExists {
            parent: 1,
            name: "dup".into(),
        });
        assert_eq!(vfs_err_to_errno(&exists), libc::EEXIST);

        let invalid = VfsError::InvalidName("bad".into());
        assert_eq!(vfs_err_to_errno(&invalid), libc::EINVAL);

        let blocked = VfsError::WouldBlock;
        assert_eq!(vfs_err_to_errno(&blocked), libc::EAGAIN);
    }

    #[test]
    fn io_permission_denied_maps_to_eacces() {
        let err = VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(vfs_err_to_errno(&err), libc::EACCES);
    }

    #[test]
    fn posix_open_flags_translate_access_and_truncate_bits() {
        let (rdonly, trunc) = posix_flags_to_open_flags(libc::O_RDONLY);
        assert!(rdonly.contains(OpenFlags::RDONLY));
        assert!(!trunc);

        let (append, trunc) = posix_flags_to_open_flags(libc::O_WRONLY | libc::O_APPEND);
        assert!(append.contains(OpenFlags::WRONLY));
        assert!(append.contains(OpenFlags::APPEND));
        assert!(!trunc);

        let (rdwr, trunc) = posix_flags_to_open_flags(libc::O_RDWR | libc::O_TRUNC);
        assert!(rdwr.contains(OpenFlags::RDWR));
        assert!(trunc);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn merge_xattr_write_overwrites_from_offset() {
        let merged = merge_xattr_write(Some(b"abcdef".to_vec()), 2, b"ZZ");
        assert_eq!(merged, b"abZZef");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn merge_xattr_write_zero_fills_gap() {
        let merged = merge_xattr_write(None, 3, b"xy");
        assert_eq!(merged, vec![0, 0, 0, b'x', b'y']);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn normalize_macos_xattr_flags_maps_create_and_replace_bits() {
        // macOS passes XATTR_CREATE=0x0002 and XATTR_REPLACE=0x0004.
        // StudioVfs expects CREATE=1 and REPLACE=2.
        assert_eq!(normalize_macos_xattr_flags(0), 0);
        assert_eq!(normalize_macos_xattr_flags(0x0002), XATTR_CREATE_FLAG);
        assert_eq!(normalize_macos_xattr_flags(0x0004), XATTR_REPLACE_FLAG);
        assert_eq!(
            normalize_macos_xattr_flags(0x0002 | 0x0004),
            XATTR_CREATE_FLAG | XATTR_REPLACE_FLAG
        );
        assert_eq!(normalize_macos_xattr_flags(0x0010), 0);
    }
}
