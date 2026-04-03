//! Platform-specific I/O primitives for the data file.
//!
//! Provides pread/pwrite wrappers that are safe for concurrent access
//! from multiple threads without file-level seeking or locking.

use std::fs::File;
use std::io;
use std::path::Path;

/// Read exactly `buf.len()` bytes from `file` at the given byte `offset`.
///
/// Retries short reads. Returns `UnexpectedEof` if the file ends before
/// the buffer is filled.
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    let mut total = 0;
    while total < buf.len() {
        let n = pread_raw(file, &mut buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "pread: EOF at offset {} (wanted {} more bytes)",
                    offset + total as u64,
                    buf.len() - total,
                ),
            ));
        }
        total += n;
    }
    Ok(())
}

/// Write all of `buf` to `file` at the given byte `offset`.
///
/// Retries short writes.
pub fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let mut total = 0;
    while total < buf.len() {
        let n = pwrite_raw(file, &buf[total..], offset + total as u64)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite: write returned 0 bytes",
            ));
        }
        total += n;
    }
    Ok(())
}

/// Flush file data (not metadata) to stable storage.
pub fn fdatasync(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Mark `file` as sparse so that future [`std::fs::File::set_len`] calls do
/// not trigger synchronous NTFS zero-fill on Windows.
///
/// On Windows/NTFS the OS normally zeroes the gap between the old and new
/// end-of-file whenever `SetEndOfFile` extends the file.  Enabling the sparse
/// attribute tells NTFS to track unwritten regions as logical holes instead,
/// making `set_len` a cheap metadata-only operation; physical clusters are
/// allocated lazily on the first write to each page.
///
/// On Windows this is *mostly* best-effort: common and expected failures
/// (e.g. non-NTFS volumes, older FAT32 volumes, or insufficient permissions)
/// are treated as benign, logged on Windows, and ignored so callers can fall
/// back to the normal zero-fill path. Unexpected I/O errors are still
/// returned to the caller and may cause cache open/create to fail.
pub fn make_sparse(file: &File) -> io::Result<()> {
    match make_sparse_impl(file) {
        Ok(()) => Ok(()),
        Err(error) if is_benign_best_effort_error(&error) => {
            #[cfg(windows)]
            tracing::warn!(
                error = ?error,
                "best-effort sparse-file setup failed; cache growth may zero-fill on Windows"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

pub fn preallocate(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(preallocate_impl(file, offset, len))
}

pub fn punch_hole(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(punch_hole_impl(file, offset, len))
}

pub fn advise_will_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(advise_will_need_impl(file, offset, len))
}

pub fn advise_dont_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(advise_dont_need_impl(file, offset, len))
}

pub fn advise_sequential(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    best_effort(advise_sequential_impl(bytes))
}

/// Returns `(free_bytes, total_bytes)` for the filesystem containing `path`.
#[cfg(target_os = "windows")]
pub fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;

    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            lpDirectoryName: *const u16,
            lpFreeBytesAvailableToCaller: *mut u64,
            lpTotalNumberOfBytes: *mut u64,
            lpTotalNumberOfFreeBytes: *mut u64,
        ) -> i32;
    }

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let mut free_caller: u64 = 0;
    let mut total: u64 = 0;
    let mut free_total: u64 = 0;
    unsafe {
        let ret = GetDiskFreeSpaceExW(wide.as_ptr(), &mut free_caller, &mut total, &mut free_total);
        if ret != 0 {
            Some((free_caller, total))
        } else {
            None
        }
    }
}

/// Returns `(free_bytes, total_bytes)` for the filesystem containing `path`.
#[cfg(not(target_os = "windows"))]
pub fn get_free_disk_space(path: &Path) -> Option<(u64, u64)> {
    use std::ffi::CString;

    let c_path = CString::new(path.to_str()?).ok()?;
    unsafe {
        let mut stat: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c_path.as_ptr(), &mut stat) == 0 {
            let f_blocks: u64 = stat.f_blocks as u64;
            let f_frsize: u64 = stat.f_frsize as u64;
            let f_bavail: u64 = stat.f_bavail as u64;

            let total_bytes: u64 = f_blocks * f_frsize;
            let free_bytes: u64 = f_bavail * f_frsize;
            Some((free_bytes, total_bytes))
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Platform raw I/O
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn pread_raw(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(unix)]
fn pwrite_raw(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.write_at(buf, offset)
}

#[cfg(windows)]
fn pread_raw(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

#[cfg(windows)]
fn pwrite_raw(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_write(buf, offset)
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
    // Keep advisory/storage hints best-effort on Windows too. Filesystems and
    // runtimes commonly report unsupported or denied advisory requests via
    // Win32 errors that should not fail the cache fast path.
    matches!(
        error.raw_os_error(),
        Some(
            1   // ERROR_INVALID_FUNCTION
                | 5   // ERROR_ACCESS_DENIED
                | 50  // ERROR_NOT_SUPPORTED
                | 87  // ERROR_INVALID_PARAMETER
                | 120 // ERROR_CALL_NOT_IMPLEMENTED
        )
    )
}

#[cfg(target_os = "linux")]
fn preallocate_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, offset, len) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Reserve physical disk clusters for the range `[offset, offset + len)` without
/// moving the file's end-of-file pointer or zero-filling.
///
/// Uses `SetFileInformationByHandle` with the `FileAllocationInfo` class (value
/// 5).  This separates the slow cluster-allocation step from the synchronous
/// zero-fill that `set_len` / `SetEndOfFile` would otherwise perform in one
/// atomic operation.  When `make_sparse` has already been called on the same
/// file, this function is a near-no-op because sparse files do not pre-reserve
/// physical clusters.
#[cfg(windows)]
fn preallocate_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    let new_size = offset.checked_add(len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "preallocate: offset + len overflows u64",
        )
    })?;

    let allocation_size = i64::try_from(new_size).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "preallocate: requested size exceeds FILE_ALLOCATION_INFO range",
        )
    })?;

    // FILE_ALLOCATION_INFO: tells NTFS to extend the physical allocation to
    // `new_size` bytes.  Unlike SetEndOfFile this does not change the logical
    // file size and does not zero-fill the extended region.
    #[repr(C)]
    struct FileAllocationInfo {
        allocation_size: i64,
    }
    const FILE_ALLOCATION_INFO_CLASS: u32 = 5;

    unsafe extern "system" {
        fn SetFileInformationByHandle(
            h_file: *mut std::ffi::c_void,
            file_information_class: u32,
            lp_file_information: *mut std::ffi::c_void,
            dw_buffer_size: u32,
        ) -> i32;
    }

    let handle = file.as_raw_handle();
    let mut alloc_info = FileAllocationInfo { allocation_size };

    // SAFETY: `handle` is a valid open file handle valid for the lifetime of
    // `file`.  `alloc_info` is a C-layout struct and we pass its exact size.
    let ret = unsafe {
        SetFileInformationByHandle(
            handle,
            FILE_ALLOCATION_INFO_CLASS,
            std::ptr::addr_of_mut!(alloc_info).cast::<std::ffi::c_void>(),
            std::mem::size_of::<FileAllocationInfo>() as u32,
        )
    };

    if ret == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// macOS and other non-Linux, non-Windows platforms have no equivalent of
/// `fallocate`; treat preallocation as a no-op.
#[cfg(not(any(target_os = "linux", windows)))]
fn preallocate_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn punch_hole_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    let result = unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, len) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn punch_hole_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_will_need_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_will_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_dont_need_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_dont_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn advise_sequential_impl(bytes: &[u8]) -> io::Result<()> {
    let result = unsafe {
        libc::madvise(
            bytes.as_ptr().cast::<libc::c_void>().cast_mut(),
            bytes.len(),
            libc::MADV_SEQUENTIAL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn advise_sequential_impl(_bytes: &[u8]) -> io::Result<()> {
    Ok(())
}

/// Enable the sparse-file attribute on `file` using `FSCTL_SET_SPARSE`.
///
/// Once set, `SetEndOfFile` / `set_len` only updates the EOF pointer without
/// synchronously zeroing the extended range.  Unwritten pages are returned as
/// logical zeros on read.  Physical clusters are only committed when data is
/// actually written to a page, which matches the block-cache write pattern
/// exactly (blocks are always fully written before being made visible).
///
/// The ioctl is idempotent: calling it on a file that is already sparse is
/// safe.  This implementation only performs the ioctl on Windows; on other
/// platforms it compiles to a no-op.
#[cfg(windows)]
fn make_sparse_impl(file: &File) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;

    // 0x000900C4 = FSCTL_SET_SPARSE (CTL_CODE(FILE_DEVICE_FILE_SYSTEM, 49,
    //   METHOD_BUFFERED, FILE_SPECIAL_ACCESS))
    const FSCTL_SET_SPARSE: u32 = 0x000900C4;

    unsafe extern "system" {
        fn DeviceIoControl(
            h_device: *mut std::ffi::c_void,
            dw_io_control_code: u32,
            lp_in_buffer: *mut std::ffi::c_void,
            n_in_buffer_size: u32,
            lp_out_buffer: *mut std::ffi::c_void,
            n_out_buffer_size: u32,
            lp_bytes_returned: *mut u32,
            lp_overlapped: *mut std::ffi::c_void,
        ) -> i32;
    }

    let handle = file.as_raw_handle();
    let mut bytes_returned: u32 = 0;

    // SAFETY: `handle` is a valid open file handle for the lifetime of `file`.
    // The in/out buffer pointers are null and their sizes are zero, which is
    // the documented calling convention for FSCTL_SET_SPARSE.
    let ret = unsafe {
        DeviceIoControl(
            handle,
            FSCTL_SET_SPARSE,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
        )
    };

    if ret == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(windows))]
fn make_sparse_impl(_file: &File) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn u64_to_off_t(value: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "offset exceeds platform off_t range",
        )
    })
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_best_effort_error_list_covers_common_advisory_failures() {
        for code in [1, 5, 50, 87, 120] {
            assert!(is_benign_best_effort_error(&io::Error::from_raw_os_error(
                code
            )));
        }

        assert!(!is_benign_best_effort_error(&io::Error::from_raw_os_error(
            32
        )));
    }

    #[cfg(windows)]
    #[test]
    fn windows_preallocate_rejects_allocation_size_over_i64_max() {
        let dir = tempfile::tempdir().unwrap();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(dir.path().join("blocks.data"))
            .unwrap();

        let error = preallocate(&file, i64::MAX as u64, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
