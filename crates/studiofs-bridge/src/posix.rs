use std::time::{Duration, SystemTime, UNIX_EPOCH};

use studiofs_meta::FileType;
use studiofs_vfs::{
    BridgeVfsErrorKind, OpenFlags, VfsError, classify_vfs_error, normalize_attr_times,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeFileType {
    RegularFile,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeFileAttr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: BridgeFileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

/// Convert POSIX `O_*` flags into the VFS open contract.
pub fn posix_flags_to_open_flags(flags: i32) -> (OpenFlags, bool) {
    let access = flags & libc::O_ACCMODE;
    let mut open_flags = match access {
        libc::O_RDONLY => OpenFlags::RDONLY,
        libc::O_WRONLY => OpenFlags::WRONLY,
        libc::O_RDWR => OpenFlags::RDWR,
        _ => OpenFlags::RDONLY,
    };
    if flags & libc::O_APPEND != 0 {
        open_flags = open_flags | OpenFlags::APPEND;
    }
    let trunc = flags & libc::O_TRUNC != 0;
    (open_flags, trunc)
}

/// Map `VfsError` to the platform-neutral POSIX errno used by bridge replies.
pub fn vfs_err_to_errno(error: &VfsError) -> i32 {
    match classify_vfs_error(error) {
        BridgeVfsErrorKind::NotFound => libc::ENOENT,
        BridgeVfsErrorKind::AlreadyExists => libc::EEXIST,
        BridgeVfsErrorKind::NotDirectory => libc::ENOTDIR,
        BridgeVfsErrorKind::IsDirectory => libc::EISDIR,
        BridgeVfsErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
        BridgeVfsErrorKind::UnrecognizedVolume => libc::ENODEV,
        BridgeVfsErrorKind::XattrValueTooLarge => libc::E2BIG,
        BridgeVfsErrorKind::InvalidInput => libc::EINVAL,
        BridgeVfsErrorKind::PermissionDenied => libc::EACCES,
        BridgeVfsErrorKind::Retryable => libc::EAGAIN,
        BridgeVfsErrorKind::TimedOut => libc::ETIMEDOUT,
        BridgeVfsErrorKind::InsufficientResources => libc::ENOMEM,
        BridgeVfsErrorKind::DataCorruption
        | BridgeVfsErrorKind::CriticalRuntimeFailure
        | BridgeVfsErrorKind::GenericFailure => libc::EIO,
    }
}

/// Convert metadata attributes into the neutral bridge attribute shape.
///
/// `mount_uid`/`mount_gid` remap inodes created with the legacy root owner so
/// concrete bridges can present ownership as the mounting user.
pub fn to_bridge_attr(
    attr: &studiofs_meta::InodeAttr,
    mount_uid: u32,
    mount_gid: u32,
    blksize: u32,
) -> BridgeFileAttr {
    let times = normalize_attr_times(attr);
    let uid = if attr.uid == 0 { mount_uid } else { attr.uid };
    let gid = if attr.gid == 0 { mount_gid } else { attr.gid };
    BridgeFileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: UNIX_EPOCH + Duration::from_nanos(times.atime_unix_nanos),
        mtime: UNIX_EPOCH + Duration::from_nanos(times.mtime_unix_nanos),
        ctime: UNIX_EPOCH + Duration::from_nanos(times.ctime_unix_nanos),
        crtime: UNIX_EPOCH + Duration::from_nanos(times.creation_time_unix_nanos),
        kind: match attr.kind {
            FileType::RegularFile => BridgeFileType::RegularFile,
            FileType::Directory => BridgeFileType::Directory,
            FileType::Symlink => BridgeFileType::Symlink,
        },
        perm: attr.mode & 0o7777,
        nlink: attr.nlink,
        uid,
        gid,
        rdev: 0,
        blksize,
        flags: 0,
    }
}

#[cfg(test)]
mod tests {
    use studiofs_meta::schema::InodeAttr;
    use studiofs_meta::{FileType, MetaError};
    use studiofs_vfs::{OpenFlags, VfsError};

    use super::{BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno};

    #[test]
    fn vfs_error_maps_to_expected_errno_codes() {
        let invalid = VfsError::InvalidName("bad".into());
        assert_eq!(vfs_err_to_errno(&invalid), libc::EINVAL);

        let exists = VfsError::Meta(MetaError::EntryExists {
            parent: studiofs_meta::ROOT_INO,
            name: "existing".into(),
        });
        assert_eq!(vfs_err_to_errno(&exists), libc::EEXIST);

        let xattr_too_big = VfsError::Meta(MetaError::XattrValueTooLarge {
            actual: 17,
            limit: 16,
        });
        assert_eq!(vfs_err_to_errno(&xattr_too_big), libc::E2BIG);
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

    #[test]
    fn to_bridge_attr_remaps_legacy_root_owner_and_rounds_blocks() {
        let mut attr = InodeAttr::new_file(9, 1, 0, 0, 0o640);
        attr.size = 513;
        attr.kind = FileType::RegularFile;

        let file_attr = to_bridge_attr(&attr, 501, 20, 512);

        assert_eq!(file_attr.uid, 501);
        assert_eq!(file_attr.gid, 20);
        assert_eq!(file_attr.blocks, 2);
        assert_eq!(file_attr.blksize, 512);
        assert_eq!(file_attr.perm, 0o640);
        assert_eq!(file_attr.kind, BridgeFileType::RegularFile);
    }
}
