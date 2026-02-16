//! fuser::Filesystem implementation — bridges FUSE kernel calls to StudioVfs.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use studiofs_meta::FileType;
use studiofs_vfs::StudioVfs;
use tokio::runtime::Runtime;
use tracing::{debug, error};

const TTL: Duration = Duration::from_secs(1);

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to fuser FileAttr.
fn to_fuse_attr(attr: &studiofs_meta::InodeAttr) -> FileAttr {
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: (attr.size + 511) / 512,
        atime: UNIX_EPOCH + Duration::from_secs(attr.atime_secs as u64),
        mtime: UNIX_EPOCH + Duration::from_secs(attr.mtime_secs as u64),
        ctime: UNIX_EPOCH + Duration::from_secs(attr.ctime_secs as u64),
        crtime: UNIX_EPOCH + Duration::from_secs(attr.ctime_secs as u64),
        kind: match attr.kind {
            FileType::RegularFile => FuseFileType::RegularFile,
            FileType::Directory => FuseFileType::Directory,
            FileType::Symlink => FuseFileType::Symlink,
        },
        perm: attr.mode,
        nlink: attr.nlink as u32,
        uid: attr.uid,
        gid: attr.gid,
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
}

impl StudioFuseFs {
    pub fn new(vfs: Arc<StudioVfs>, rt: Arc<Runtime>) -> Self {
        Self { vfs, rt }
    }
}

impl Filesystem for StudioFuseFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self.vfs.lookup(parent, &name_bytes) {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => {
                debug!("lookup({parent}, {:?}) -> {e}", name);
                reply.error(libc::ENOENT);
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.vfs.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr)),
            Err(e) => {
                debug!("getattr({ino}) -> {e}");
                reply.error(libc::ENOENT);
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
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self
            .vfs
            .mkdir(parent, &name_bytes, req.uid(), req.gid(), mode as u16)
        {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => {
                error!("mkdir({parent}, {:?}) -> {e}", name);
                reply.error(libc::EIO);
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
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self
            .vfs
            .create(parent, &name_bytes, req.uid(), req.gid(), mode as u16)
        {
            Ok(attr) => reply.entry(&TTL, &to_fuse_attr(&attr), 0),
            Err(e) => {
                error!("mknod({parent}, {:?}) -> {e}", name);
                reply.error(libc::EIO);
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
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self
            .vfs
            .create(parent, &name_bytes, req.uid(), req.gid(), mode as u16)
        {
            Ok(attr) => {
                reply.created(&TTL, &to_fuse_attr(&attr), 0, 0, 0);
            }
            Err(e) => {
                error!("create({parent}, {:?}) -> {e}", name);
                reply.error(libc::EIO);
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self.vfs.unlink(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("unlink({parent}, {:?}) -> {e}", name);
                reply.error(libc::EIO);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = name.to_string_lossy().as_bytes().to_vec();
        match self.vfs.rmdir(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("rmdir({parent}, {:?}) -> {e}", name);
                reply.error(libc::EIO);
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
        let old_name = name.to_string_lossy().as_bytes().to_vec();
        let new_name = newname.to_string_lossy().as_bytes().to_vec();
        match self.vfs.rename(parent, &old_name, newparent, &new_name) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("rename -> {e}");
                reply.error(libc::EIO);
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        // We don't track file handles — just approve the open.
        reply.opened(0, 0);
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
                reply.error(libc::EIO);
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
                reply.error(libc::EIO);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        match self.rt.block_on(self.vfs.flush(ino)) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!("flush({ino}) -> {e}");
                reply.error(libc::EIO);
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
                reply.error(libc::EIO);
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
            let name = String::from_utf8_lossy(&entry.name);
            let ft = fuse_file_type(entry.kind);
            if reply.add(entry.ino, (entry_idx + 1) as i64, ft, name.as_ref()) {
                break;
            }
        }

        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        match self.vfs.setattr(ino, size) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr)),
            Err(e) => {
                error!("setattr({ino}) -> {e}");
                reply.error(libc::EIO);
            }
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        // Virtual filesystem stats
        reply.statfs(
            1024 * 1024 * 1024, // blocks (1TB / 1K)
            512 * 1024 * 1024,  // bfree
            512 * 1024 * 1024,  // bavail
            1_000_000,          // files
            500_000,            // ffree
            4096,               // bsize
            255,                // namelen
            4096,               // frsize
        );
    }
}
