use std::io::ErrorKind;

use studiofs_data::store::DataError;
use studiofs_meta::{InodeAttr, MetaError};

use crate::{VfsError, fetch::FetchError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Categories of VFS-facing errors mapped from internal `VfsError` values.
pub enum BridgeVfsErrorKind {
    /// Target object or entry was not found.
    NotFound,
    /// Operation failed because the resource already exists.
    AlreadyExists,
    /// Path component expected a directory but found a non-directory.
    NotDirectory,
    /// Operation expects a non-directory but found a directory.
    IsDirectory,
    /// Directory was expected to be empty but contained entries.
    DirectoryNotEmpty,
    /// The mounted volume does not appear to be recognized/initialized.
    UnrecognizedVolume,
    /// xattr value exceeds the platform or bridge limit.
    XattrValueTooLarge,
    /// Input arguments or values were malformed or invalid.
    InvalidInput,
    /// Access was denied for the requested operation.
    PermissionDenied,
    /// The operation can be retried.
    Retryable,
    /// The operation exceeded a timeout.
    TimedOut,
    /// Insufficient resources are available to satisfy the request.
    InsufficientResources,
    /// Persistent corruption was detected in metadata, data, or payload.
    DataCorruption,
    /// Internal runtime failure interrupted request processing.
    CriticalRuntimeFailure,
    /// General fallback category for unmapped or unexpected failures.
    GenericFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Bridge-facing normalized inode times in Unix nanoseconds.
pub struct BridgeAttrTimes {
    /// Last access time as Unix nanoseconds.
    pub atime_unix_nanos: u64,
    /// Last modification time as Unix nanoseconds.
    pub mtime_unix_nanos: u64,
    /// Last metadata change time as Unix nanoseconds.
    pub ctime_unix_nanos: u64,
    /// Effective creation time as Unix nanoseconds.
    pub creation_time_unix_nanos: u64,
}

/// Maps an internal [`VfsError`] to a bridge error kind.
pub fn classify_vfs_error(error: &VfsError) -> BridgeVfsErrorKind {
    match error {
        VfsError::Meta(meta_error) => classify_meta_error(meta_error),
        VfsError::Io(io_error) => classify_io_error(io_error.kind()),
        VfsError::Join(_) => BridgeVfsErrorKind::CriticalRuntimeFailure,
        VfsError::Object(_) | VfsError::BlockNotFound(_, _) => BridgeVfsErrorKind::DataCorruption,
        VfsError::WouldBlock | VfsError::RemoteContentPending => BridgeVfsErrorKind::Retryable,
        VfsError::InvalidName(_) => BridgeVfsErrorKind::InvalidInput,
        VfsError::Fetch(fetch_error) => classify_fetch_error(fetch_error),
        _ => BridgeVfsErrorKind::GenericFailure,
    }
}

/// Converts filesystem inode timestamps into bridge normalized nanosecond form.
pub fn normalize_attr_times(attr: &InodeAttr) -> BridgeAttrTimes {
    BridgeAttrTimes {
        atime_unix_nanos: clamp_unix_nanos(attr.atime),
        mtime_unix_nanos: clamp_unix_nanos(attr.mtime),
        ctime_unix_nanos: clamp_unix_nanos(attr.ctime),
        creation_time_unix_nanos: clamp_unix_nanos(attr.effective_crtime()),
    }
}

fn classify_meta_error(error: &MetaError) -> BridgeVfsErrorKind {
    match error {
        MetaError::InodeNotFound(_) | MetaError::EntryNotFound { .. } => {
            BridgeVfsErrorKind::NotFound
        }
        MetaError::EntryExists { .. } => BridgeVfsErrorKind::AlreadyExists,
        MetaError::NotADirectory(_) => BridgeVfsErrorKind::NotDirectory,
        MetaError::IsADirectory(_) => BridgeVfsErrorKind::IsDirectory,
        MetaError::DirNotEmpty(_) => BridgeVfsErrorKind::DirectoryNotEmpty,
        MetaError::NotFormatted => BridgeVfsErrorKind::UnrecognizedVolume,
        MetaError::NonContiguousRegularFileGraph { .. } => BridgeVfsErrorKind::DataCorruption,
        MetaError::XattrValueTooLarge { .. } => BridgeVfsErrorKind::XattrValueTooLarge,
        MetaError::AccessDenied { .. } => BridgeVfsErrorKind::PermissionDenied,
        _ => BridgeVfsErrorKind::GenericFailure,
    }
}

fn classify_fetch_error(error: &FetchError) -> BridgeVfsErrorKind {
    match error {
        FetchError::NotFound { .. } => BridgeVfsErrorKind::NotFound,
        FetchError::Exhausted { source, .. } => match source {
            DataError::Crypto(_) | DataError::InvalidCryptoConfig(_) => {
                BridgeVfsErrorKind::DataCorruption
            }
            _ => BridgeVfsErrorKind::TimedOut,
        },
        FetchError::PrefetchSkipped => BridgeVfsErrorKind::Retryable,
        FetchError::CorruptFragment { .. } | FetchError::Decode(_) | FetchError::CacheAccess(_) => {
            BridgeVfsErrorKind::DataCorruption
        }
        FetchError::TransitBudgetTooSmall { .. } => BridgeVfsErrorKind::InsufficientResources,
    }
}

fn classify_io_error(kind: ErrorKind) -> BridgeVfsErrorKind {
    match kind {
        ErrorKind::NotFound => BridgeVfsErrorKind::NotFound,
        ErrorKind::PermissionDenied => BridgeVfsErrorKind::PermissionDenied,
        ErrorKind::AlreadyExists => BridgeVfsErrorKind::AlreadyExists,
        ErrorKind::WouldBlock => BridgeVfsErrorKind::Retryable,
        ErrorKind::TimedOut => BridgeVfsErrorKind::TimedOut,
        ErrorKind::InvalidInput | ErrorKind::InvalidData => BridgeVfsErrorKind::InvalidInput,
        _ => BridgeVfsErrorKind::GenericFailure,
    }
}

fn clamp_unix_nanos(nanos: i64) -> u64 {
    nanos.max(0) as u64
}

#[cfg(test)]
mod tests {
    use std::{
        panic,
        sync::{Mutex, OnceLock},
    };

    use super::{BridgeAttrTimes, BridgeVfsErrorKind, classify_vfs_error, normalize_attr_times};
    use studiofs_data::{crypto::CryptoError, object::ObjectError, store::DataError};
    use studiofs_meta::{AccessLevel, EncryptionContractError, InodeAttr, MetaError};

    fn panic_hook_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_silenced_panic_hook<T>(f: impl FnOnce() -> T) -> T {
        let _lock = panic_hook_lock()
            .lock()
            .expect("panic hook test lock should not be poisoned");
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        let result = panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        panic::set_hook(previous_hook);
        match result {
            Ok(value) => value,
            Err(payload) => panic::resume_unwind(payload),
        }
    }

    #[test]
    fn classify_vfs_error_maps_meta_variants_to_bridge_categories() {
        let not_found = crate::VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "missing".into(),
        });
        assert_eq!(classify_vfs_error(&not_found), BridgeVfsErrorKind::NotFound);

        let exists = crate::VfsError::Meta(MetaError::EntryExists {
            parent: 1,
            name: "dup".into(),
        });
        assert_eq!(
            classify_vfs_error(&exists),
            BridgeVfsErrorKind::AlreadyExists
        );

        let not_dir = crate::VfsError::Meta(MetaError::NotADirectory(7));
        assert_eq!(
            classify_vfs_error(&not_dir),
            BridgeVfsErrorKind::NotDirectory
        );

        let is_dir = crate::VfsError::Meta(MetaError::IsADirectory(7));
        assert_eq!(classify_vfs_error(&is_dir), BridgeVfsErrorKind::IsDirectory);

        let dir_not_empty = crate::VfsError::Meta(MetaError::DirNotEmpty(7));
        assert_eq!(
            classify_vfs_error(&dir_not_empty),
            BridgeVfsErrorKind::DirectoryNotEmpty
        );

        let denied = crate::VfsError::Meta(MetaError::AccessDenied {
            ino: 7,
            principal: "user".into(),
            required: AccessLevel::ReadWrite,
            actual: AccessLevel::ReadOnly,
        });
        assert_eq!(
            classify_vfs_error(&denied),
            BridgeVfsErrorKind::PermissionDenied
        );
    }

    #[test]
    fn classify_vfs_error_maps_fetch_variants_to_bridge_categories() {
        let exhausted = crate::VfsError::Fetch(crate::fetch::FetchError::Exhausted {
            attempts: 3,
            source: DataError::Storage("timeout".into()),
        });
        assert_eq!(classify_vfs_error(&exhausted), BridgeVfsErrorKind::TimedOut);

        let crypto_exhausted = crate::VfsError::Fetch(crate::fetch::FetchError::Exhausted {
            attempts: 1,
            source: DataError::Crypto(CryptoError::AuthenticationFailed),
        });
        assert_eq!(
            classify_vfs_error(&crypto_exhausted),
            BridgeVfsErrorKind::DataCorruption
        );

        let config_exhausted = crate::VfsError::Fetch(crate::fetch::FetchError::Exhausted {
            attempts: 1,
            source: DataError::InvalidCryptoConfig(EncryptionContractError::Disabled),
        });
        assert_eq!(
            classify_vfs_error(&config_exhausted),
            BridgeVfsErrorKind::DataCorruption
        );

        let skipped = crate::VfsError::Fetch(crate::fetch::FetchError::PrefetchSkipped);
        assert_eq!(classify_vfs_error(&skipped), BridgeVfsErrorKind::Retryable);

        let budget = crate::VfsError::Fetch(crate::fetch::FetchError::TransitBudgetTooSmall {
            required_bytes: 8192,
            budget_bytes: 4096,
        });
        assert_eq!(
            classify_vfs_error(&budget),
            BridgeVfsErrorKind::InsufficientResources
        );

        let corrupt = crate::VfsError::Fetch(crate::fetch::FetchError::CorruptFragment {
            object_id: 42,
            reason: "bad layout".into(),
        });
        assert_eq!(
            classify_vfs_error(&corrupt),
            BridgeVfsErrorKind::DataCorruption
        );

        let decode =
            crate::VfsError::Fetch(crate::fetch::FetchError::Decode(ObjectError::InvalidMagic));
        assert_eq!(
            classify_vfs_error(&decode),
            BridgeVfsErrorKind::DataCorruption
        );

        let object = crate::VfsError::Object(ObjectError::InvalidMagic);
        assert_eq!(
            classify_vfs_error(&object),
            BridgeVfsErrorKind::DataCorruption
        );

        let missing_block = crate::VfsError::BlockNotFound(42, 3);
        assert_eq!(
            classify_vfs_error(&missing_block),
            BridgeVfsErrorKind::DataCorruption
        );
    }

    #[test]
    fn classify_vfs_error_maps_io_and_runtime_variants_to_bridge_categories() {
        let permission_denied =
            crate::VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(
            classify_vfs_error(&permission_denied),
            BridgeVfsErrorKind::PermissionDenied
        );

        let timed_out = crate::VfsError::Io(std::io::Error::from(std::io::ErrorKind::TimedOut));
        assert_eq!(classify_vfs_error(&timed_out), BridgeVfsErrorKind::TimedOut);

        let invalid = crate::VfsError::InvalidName("bad".into());
        assert_eq!(
            classify_vfs_error(&invalid),
            BridgeVfsErrorKind::InvalidInput
        );

        let blocked = crate::VfsError::WouldBlock;
        assert_eq!(classify_vfs_error(&blocked), BridgeVfsErrorKind::Retryable);

        let remote_pending = crate::VfsError::RemoteContentPending;
        assert_eq!(
            classify_vfs_error(&remote_pending),
            BridgeVfsErrorKind::Retryable
        );

        let join_err = with_silenced_panic_hook(|| {
            tokio::runtime::Runtime::new()
                .expect("runtime")
                .block_on(async {
                    tokio::spawn(async move { panic!("boom") })
                        .await
                        .expect_err("task should panic")
                })
        });
        let join = crate::VfsError::Join(join_err);
        assert_eq!(
            classify_vfs_error(&join),
            BridgeVfsErrorKind::CriticalRuntimeFailure
        );
    }

    #[test]
    fn normalize_attr_times_clamps_negative_timestamps() {
        let mut attr = InodeAttr::new_file(11, 1, 1000, 1000, 0o644);
        attr.atime = -10;
        attr.mtime = -20;
        attr.ctime = -30;
        attr.crtime = -40;

        assert_eq!(
            normalize_attr_times(&attr),
            BridgeAttrTimes {
                atime_unix_nanos: 0,
                mtime_unix_nanos: 0,
                ctime_unix_nanos: 0,
                creation_time_unix_nanos: 0,
            }
        );
    }

    #[test]
    fn normalize_attr_times_uses_effective_creation_time() {
        let mut attr = InodeAttr::new_file(12, 1, 1000, 1000, 0o644);
        attr.atime = 10;
        attr.mtime = 20;
        attr.ctime = 30;
        attr.crtime = 0;

        assert_eq!(
            normalize_attr_times(&attr),
            BridgeAttrTimes {
                atime_unix_nanos: 10,
                mtime_unix_nanos: 20,
                ctime_unix_nanos: 30,
                creation_time_unix_nanos: 30,
            }
        );
    }
}
