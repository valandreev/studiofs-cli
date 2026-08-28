//! POSIX conversions that are independent of a concrete filesystem bridge.
//!
//! The helpers in this module translate between StudioFS VFS/meta types and
//! POSIX-shaped bridge data. They intentionally avoid Linux FUSE, macFUSE, and
//! platform-specific Finder/xattr/cache-policy behavior.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use studiofs_meta::FileType;
use studiofs_vfs::{
    BridgeVfsErrorKind, OpenFlags, VfsError, classify_vfs_error, normalize_attr_times,
};

const POSIX_STAT_BLOCK_SIZE: u64 = 512;

/// Platform-neutral file kind used by bridge attribute conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeFileType {
    /// Regular file.
    RegularFile,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

/// Platform-neutral file attributes produced from StudioFS metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeFileAttr {
    /// StudioFS inode number.
    pub ino: u64,
    /// File size in bytes.
    pub size: u64,
    /// Number of 512-byte blocks occupied by the file.
    pub blocks: u64,
    /// Last access time.
    pub atime: SystemTime,
    /// Last modification time.
    pub mtime: SystemTime,
    /// Last metadata change time.
    pub ctime: SystemTime,
    /// Creation time.
    pub crtime: SystemTime,
    /// File kind.
    pub kind: BridgeFileType,
    /// POSIX permission bits.
    pub perm: u16,
    /// Link count.
    pub nlink: u32,
    /// Owner user ID.
    pub uid: u32,
    /// Owner group ID.
    pub gid: u32,
    /// Device ID for special files.
    pub rdev: u32,
    /// Preferred block size for bridge I/O.
    pub blksize: u32,
    /// Bridge-specific attribute flags.
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
        open_flags |= OpenFlags::APPEND;
    }
    if flags & (libc::O_SYNC | libc::O_DSYNC) != 0 {
        open_flags |= OpenFlags::SYNC;
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
        BridgeVfsErrorKind::MetadataPersistenceDegraded => libc::EIO,
        BridgeVfsErrorKind::Retryable => libc::EAGAIN,
        BridgeVfsErrorKind::TimedOut => libc::ETIMEDOUT,
        BridgeVfsErrorKind::RepairRequired => libc::ENOTRECOVERABLE,
        BridgeVfsErrorKind::RemoteUnavailable => libc::EHOSTUNREACH,
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
        blocks: attr.size.div_ceil(POSIX_STAT_BLOCK_SIZE),
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
    fn explicit_metadata_categories_map_to_distinct_errno_codes() {
        let repair = VfsError::Meta(MetaError::RepairRequired("repair".into()));
        let unavailable = VfsError::Meta(MetaError::RemoteUnavailable("offline".into()));
        let timeout = VfsError::Meta(MetaError::Timeout("timeout".into()));

        assert_eq!(vfs_err_to_errno(&repair), libc::ENOTRECOVERABLE);
        assert_eq!(vfs_err_to_errno(&unavailable), libc::EHOSTUNREACH);
        assert_eq!(vfs_err_to_errno(&timeout), libc::ETIMEDOUT);
    }

    #[test]
    fn metadata_persistence_degradation_maps_to_io_errno() {
        let error = VfsError::MetadataPersistenceDegraded;

        assert_eq!(vfs_err_to_errno(&error), libc::EIO);
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
    fn posix_open_flags_translate_sync_bits() {
        let (sync, _trunc) = posix_flags_to_open_flags(libc::O_WRONLY | libc::O_SYNC);
        let (dsync, _trunc) = posix_flags_to_open_flags(libc::O_WRONLY | libc::O_DSYNC);

        assert!(sync.contains(OpenFlags::SYNC));
        assert!(dsync.contains(OpenFlags::SYNC));
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
