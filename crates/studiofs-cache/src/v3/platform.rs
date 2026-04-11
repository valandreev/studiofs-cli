//! Platform-specific primitives for Cache 3.0 payload files.

use std::fs::File;
use std::io;
use std::path::Path;

/// Open a payload file for read-only access using platform-specific flags.
pub fn open_read_only(path: &Path) -> io::Result<File> {
    open_read_only_impl(path)
}

/// Read up to `buf.len()` bytes from `file` starting at `offset`.
///
/// Returns the number of bytes read, or `0` at EOF.
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let total_offset = u64::try_from(total)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "pread offset overflow"))?;
        let file_offset = offset
            .checked_add(total_offset)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pread offset overflow"))?;
        let n = pread_raw(file, &mut buf[total..], file_offset)?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

/// Read exactly `buf.len()` bytes from `file` at `offset`.
pub fn pread_exact(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let read = pread(file, buf, offset)?;
    if read == buf.len() {
        Ok(())
    } else {
        let read_offset = u64::try_from(read)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "pread length overflow"))?;
        let remaining = buf
            .len()
            .checked_sub(read)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "pread length overflow"))?;
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "pread_exact: EOF at offset {} (wanted {} more bytes)",
                offset + read_offset,
                remaining
            ),
        ))
    }
}

/// Write all bytes to the file.
pub fn write_all(file: &mut File, buf: &[u8]) -> io::Result<()> {
    std::io::Write::write_all(file, buf)?;
    Ok(())
}

/// Flush file payload data to durable storage.
pub fn sync_data(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Atomically rename a file within the same filesystem.
pub fn rename_atomic(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)
}

/// Returns true when a rename failed because source and destination live on
/// different filesystems/devices.
pub fn is_cross_device_link_error(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EXDEV))
    }

    #[cfg(windows)]
    {
        // ERROR_NOT_SAME_DEVICE
        matches!(error.raw_os_error(), Some(17))
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

/// Best-effort sync for directory metadata after rename.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    sync_directory_impl(path)
}

/// Best-effort advisory hint for random reads on a file handle.
pub fn advise_random(file: &File) -> io::Result<()> {
    best_effort(advise_random_impl(file))
}

/// Best-effort advisory hint that a range will likely be read soon.
pub fn advise_will_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(advise_will_need_impl(file, offset, len))
}

#[cfg(unix)]
fn sync_directory_impl(path: &Path) -> io::Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()
}

#[cfg(windows)]
fn sync_directory_impl(path: &Path) -> io::Result<()> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;
    use std::sync::atomic::{AtomicBool, Ordering};

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    static LOGGED_UNSUPPORTED_DIRECTORY_SYNC: AtomicBool = AtomicBool::new(false);

    // FILE_FLAG_BACKUP_SEMANTICS is required to open directory handles on
    // Windows through OpenOptions/File APIs.
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;

    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(error) if is_windows_unsupported_sync_error(&error) => {
            if !LOGGED_UNSUPPORTED_DIRECTORY_SYNC.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    path = %path.display(),
                    error = ?error,
                    "cache-v3 directory sync is unsupported on this Windows filesystem (warning will be logged once)"
                );
            }
            Ok(())
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn open_read_only_impl(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new().read(true).open(path)
}

#[cfg(windows)]
fn open_read_only_impl(path: &Path) -> io::Result<File> {
    use std::fs::OpenOptions;
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_RANDOM_ACCESS: u32 = 0x1000_0000;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_RANDOM_ACCESS)
        .open(path)
}

#[cfg(unix)]
fn pread_raw(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn pread_raw(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

fn best_effort(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error) if is_benign_best_effort_error(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn is_benign_best_effort_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(
            libc::EOPNOTSUPP
                | libc::ENOSYS
                | libc::EINVAL
                | libc::EPERM
                | libc::EACCES
                | libc::ENODEV
        )
    )
}

#[cfg(windows)]
fn is_benign_best_effort_error(error: &io::Error) -> bool {
    is_windows_unsupported_sync_error(error)
}

#[cfg(windows)]
fn is_windows_unsupported_sync_error(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(
            1 // ERROR_INVALID_FUNCTION
                | 5 // ERROR_ACCESS_DENIED
                | 50 // ERROR_NOT_SUPPORTED
                | 87 // ERROR_INVALID_PARAMETER
                | 120 // ERROR_CALL_NOT_IMPLEMENTED
        )
    )
}

#[cfg(target_os = "linux")]
fn advise_random_impl(file: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    // SAFETY: The file descriptor from `as_raw_fd` remains valid for the lifetime of
    // `file`, and POSIX_FADV_RANDOM is advisory only.
    let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn advise_random_impl(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn advise_random_impl(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_will_need_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    // SAFETY: The file descriptor is borrowed from a live `File`, and POSIX_WILLNEED
    // is advisory; arguments are validated against off_t bounds above.
    let rc =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(rc))
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn advise_will_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(windows)]
fn advise_will_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn u64_to_off_t(value: u64) -> io::Result<libc::off_t> {
    let off_t_max = u64::try_from(libc::off_t::MAX)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "off_t range is negative"))?;
    if value > off_t_max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset/length exceeds off_t range: {value}"),
        ));
    }
    value.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("offset/length exceeds off_t range: {value}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn cross_device_detection_matches_exdev() {
        let exdev = io::Error::from_raw_os_error(libc::EXDEV);
        assert!(is_cross_device_link_error(&exdev));

        let not_exdev = io::Error::from_raw_os_error(libc::EINVAL);
        assert!(!is_cross_device_link_error(&not_exdev));
    }

    #[cfg(windows)]
    #[test]
    fn cross_device_detection_matches_windows_error() {
        let exdev = io::Error::from_raw_os_error(17);
        assert!(is_cross_device_link_error(&exdev));

        let other = io::Error::from_raw_os_error(87);
        assert!(!is_cross_device_link_error(&other));
    }

    #[cfg(windows)]
    #[test]
    fn windows_unsupported_directory_sync_errors_are_classified_as_benign() {
        for code in [1, 5, 50, 87, 120] {
            assert!(is_windows_unsupported_sync_error(
                &io::Error::from_raw_os_error(code)
            ));
        }
        assert!(!is_windows_unsupported_sync_error(
            &io::Error::from_raw_os_error(32)
        ));
    }
}
