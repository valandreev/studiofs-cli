//! Platform-specific primitives for `v3` payload files.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

#[cfg(windows)]
const WINDOWS_UNBUFFERED_WRITE_ALIGNMENT: usize = 4096;
#[cfg(windows)]
const WINDOWS_VOLUME_PATH_BUFFER_U16: usize = 32_768;

#[derive(Debug, Default)]
/// Struct $name.
pub struct UnbufferedWriteBuffer {
    #[cfg(any(windows, test))]
    backing: Vec<u8>,
}

impl UnbufferedWriteBuffer {
    /// Function $name.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn backing_capacity(&self) -> usize {
        self.backing.capacity()
    }
}

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

/// Create a new payload file using the platform's normal buffered writer.
pub fn create_new_buffered_write(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .open(path)
}

/// Create a new payload file using platform unbuffered write flags when supported.
pub fn create_new_unbuffered_write(path: &Path) -> io::Result<File> {
    create_new_unbuffered_write_impl(path)
}

/// Return true when this payload can be attempted on the unbuffered write path.
pub fn can_write_unbuffered_payload(buf: &[u8], alignment: usize) -> bool {
    can_write_unbuffered_payload_impl(buf, alignment)
}

/// Return the required unbuffered-write alignment for files under `path`.
pub fn unbuffered_write_alignment_for_path(path: &Path) -> usize {
    unbuffered_write_alignment_for_path_impl(path)
}

/// Write all bytes to an unbuffered payload file.
pub fn write_all_unbuffered(file: &mut File, buf: &[u8]) -> io::Result<()> {
    let mut scratch = UnbufferedWriteBuffer::new();
    write_all_unbuffered_with_buffer(
        file,
        buf,
        &mut scratch,
        default_unbuffered_write_alignment(),
    )
}

/// Write all bytes to an unbuffered payload file, reusing caller-owned scratch.
pub fn write_all_unbuffered_with_buffer(
    file: &mut File,
    buf: &[u8],
    scratch: &mut UnbufferedWriteBuffer,
    alignment: usize,
) -> io::Result<()> {
    write_all_unbuffered_impl(file, buf, scratch, alignment)
}

/// Return true when an I/O error is compatible with falling back to buffered writes.
pub fn is_unbuffered_io_unsupported_error(error: &io::Error) -> bool {
    is_unbuffered_io_unsupported_error_impl(error)
}

/// Flush file payload data to durable storage.
pub fn sync_data(file: &File) -> io::Result<()> {
    file.sync_data()
}

/// Atomically rename a file within the same filesystem.
pub fn rename_atomic(from: &Path, to: &Path) -> io::Result<()> {
    std::fs::rename(from, to)
}

/// Returns `true` when a rename crossed filesystem/device boundaries.
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
fn sync_directory_impl(_path: &Path) -> io::Result<()> {
    // Directory-handle syncs are best-effort only. On Windows they commonly
    // fail with AccessDenied/NotSupported depending on the filesystem, so we
    // intentionally skip them and rely on file-level syncs plus atomic rename.
    Ok(())
}

#[cfg(unix)]
fn open_read_only_impl(path: &Path) -> io::Result<File> {
    std::fs::OpenOptions::new().read(true).open(path)
}

#[cfg(windows)]
fn open_read_only_impl(path: &Path) -> io::Result<File> {
    // Let Windows apply its default cache and readahead policy here.
    // JuiceFS uses plain read-only opens for cache/staging files, and forcing
    // FILE_FLAG_RANDOM_ACCESS made our large sequential read/writeback mix much
    // less cooperative with the OS page cache.
    std::fs::OpenOptions::new().read(true).open(path)
}

#[cfg(not(windows))]
fn create_new_unbuffered_write_impl(path: &Path) -> io::Result<File> {
    create_new_buffered_write(path)
}

#[cfg(windows)]
fn create_new_unbuffered_write_impl(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;

    OpenOptions::new()
        .create_new(true)
        .write(true)
        .read(true)
        .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN)
        .open(path)
}

#[cfg(not(windows))]
fn can_write_unbuffered_payload_impl(_buf: &[u8], _alignment: usize) -> bool {
    false
}

#[cfg(windows)]
fn can_write_unbuffered_payload_impl(buf: &[u8], alignment: usize) -> bool {
    let alignment = normalized_unbuffered_write_alignment(alignment);
    !buf.is_empty() && buf.len().is_multiple_of(alignment)
}

#[cfg(not(windows))]
fn unbuffered_write_alignment_for_path_impl(_path: &Path) -> usize {
    1
}

#[cfg(windows)]
fn unbuffered_write_alignment_for_path_impl(path: &Path) -> usize {
    windows_unbuffered_write_alignment_for_path(path).unwrap_or(WINDOWS_UNBUFFERED_WRITE_ALIGNMENT)
}

#[cfg(not(windows))]
fn default_unbuffered_write_alignment() -> usize {
    1
}

#[cfg(windows)]
fn default_unbuffered_write_alignment() -> usize {
    WINDOWS_UNBUFFERED_WRITE_ALIGNMENT
}

#[cfg(not(windows))]
fn write_all_unbuffered_impl(
    file: &mut File,
    buf: &[u8],
    _scratch: &mut UnbufferedWriteBuffer,
    _alignment: usize,
) -> io::Result<()> {
    write_all(file, buf)
}

#[cfg(windows)]
fn write_all_unbuffered_impl(
    file: &mut File,
    buf: &[u8],
    scratch: &mut UnbufferedWriteBuffer,
    alignment: usize,
) -> io::Result<()> {
    let alignment = normalized_unbuffered_write_alignment(alignment);
    if !can_write_unbuffered_payload(buf, alignment) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unbuffered payload writes must be non-empty and {alignment}-byte aligned"),
        ));
    }

    if is_windows_unbuffered_ptr_aligned(buf.as_ptr() as usize, alignment) {
        return write_all(file, buf);
    }

    let aligned = scratch.aligned_copy(buf, alignment);
    write_all(file, aligned)
}

#[cfg(windows)]
impl UnbufferedWriteBuffer {
    fn aligned_copy(&mut self, buf: &[u8], alignment: usize) -> &[u8] {
        let alignment = normalized_unbuffered_write_alignment(alignment);
        let required_len = buf.len().saturating_add(alignment - 1);
        if self.backing.len() < required_len {
            self.backing.resize(required_len, 0);
        }
        let base = self.backing.as_ptr() as usize;
        let offset = base.next_multiple_of(alignment) - base;
        let aligned = &mut self.backing[offset..offset + buf.len()];
        aligned.copy_from_slice(buf);
        aligned
    }
}

#[cfg(windows)]
fn normalized_unbuffered_write_alignment(alignment: usize) -> usize {
    alignment.max(WINDOWS_UNBUFFERED_WRITE_ALIGNMENT)
}

#[cfg(windows)]
fn is_windows_unbuffered_ptr_aligned(ptr: usize, alignment: usize) -> bool {
    ptr.is_multiple_of(normalized_unbuffered_write_alignment(alignment))
}

#[cfg(windows)]
fn windows_unbuffered_write_alignment_for_path(path: &Path) -> Option<usize> {
    use std::os::windows::ffi::OsStrExt;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetVolumePathNameW"]
        fn get_volume_path_name_w(
            file_name: *const u16,
            volume_path_name: *mut u16,
            buffer_length: u32,
        ) -> i32;

        #[link_name = "GetDiskFreeSpaceW"]
        fn get_disk_free_space_w(
            root_path_name: *const u16,
            sectors_per_cluster: *mut u32,
            bytes_per_sector: *mut u32,
            number_of_free_clusters: *mut u32,
            total_number_of_clusters: *mut u32,
        ) -> i32;
    }

    let file_name = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let mut volume_path = vec![0_u16; WINDOWS_VOLUME_PATH_BUFFER_U16];
    let volume_path_len = u32::try_from(volume_path.len()).ok()?;

    // SAFETY: Both buffers are valid NUL-terminated UTF-16 buffers for the
    // duration of the calls. The output buffer length is passed in WCHAR units.
    let volume_ok = unsafe {
        get_volume_path_name_w(
            file_name.as_ptr(),
            volume_path.as_mut_ptr(),
            volume_path_len,
        )
    };
    if volume_ok == 0 {
        return None;
    }

    let mut sectors_per_cluster = 0_u32;
    let mut bytes_per_sector = 0_u32;
    let mut free_clusters = 0_u32;
    let mut total_clusters = 0_u32;
    // SAFETY: `volume_path` was filled by GetVolumePathNameW and remains a
    // valid NUL-terminated UTF-16 root path. All output pointers are valid.
    let disk_ok = unsafe {
        get_disk_free_space_w(
            volume_path.as_ptr(),
            &mut sectors_per_cluster,
            &mut bytes_per_sector,
            &mut free_clusters,
            &mut total_clusters,
        )
    };
    if disk_ok == 0 || bytes_per_sector == 0 {
        return None;
    }

    usize::try_from(bytes_per_sector)
        .ok()
        .map(normalized_unbuffered_write_alignment)
}

#[cfg(test)]
#[cfg(windows)]
fn copy_for_unbuffered_test<'a>(scratch: &'a mut UnbufferedWriteBuffer, buf: &[u8]) -> &'a [u8] {
    scratch.aligned_copy(buf, default_unbuffered_write_alignment())
}

#[cfg(test)]
#[cfg(not(windows))]
fn copy_for_unbuffered_test<'a>(scratch: &'a mut UnbufferedWriteBuffer, buf: &[u8]) -> &'a [u8] {
    if scratch.backing.len() < buf.len() {
        scratch.backing.resize(buf.len(), 0);
    }
    let aligned = &mut scratch.backing[..buf.len()];
    aligned.copy_from_slice(buf);
    aligned
}

#[cfg(not(windows))]
fn is_unbuffered_io_unsupported_error_impl(_error: &io::Error) -> bool {
    true
}

#[cfg(windows)]
fn is_unbuffered_io_unsupported_error_impl(error: &io::Error) -> bool {
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
    is_windows_sync_unsupported_error(error)
}

#[cfg(windows)]
fn is_windows_sync_unsupported_error(error: &io::Error) -> bool {
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
    let rc = unsafe {
        // SAFETY: The file descriptor is borrowed from a live `File`, and POSIX_WILLNEED
        // is advisory; arguments are validated against off_t bounds above.
        libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED)
    };
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
            assert!(is_windows_sync_unsupported_error(
                &io::Error::from_raw_os_error(code)
            ));
        }
        assert!(!is_windows_sync_unsupported_error(
            &io::Error::from_raw_os_error(32)
        ));
    }

    #[test]
    fn unbuffered_write_buffer_reuses_backing_allocation() {
        let mut scratch = UnbufferedWriteBuffer::new();
        let first = vec![1_u8; 4096];
        let second = vec![2_u8; 4096];

        assert_eq!(copy_for_unbuffered_test(&mut scratch, &first), first);
        let capacity_after_first = scratch.backing_capacity();
        assert!(capacity_after_first >= first.len());

        assert_eq!(copy_for_unbuffered_test(&mut scratch, &second), second);
        assert_eq!(
            scratch.backing_capacity(),
            capacity_after_first,
            "same-sized unbuffered writes should reuse scratch backing storage"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_payload_length_honors_volume_alignment() {
        let four_kib = vec![0_u8; 4096];
        let eight_kib = vec![0_u8; 8192];

        assert!(!can_write_unbuffered_payload(&four_kib, 8192));
        assert!(can_write_unbuffered_payload(&eight_kib, 8192));
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_scratch_aligns_to_volume_alignment() {
        let mut scratch = UnbufferedWriteBuffer::new();
        let data = vec![0x7A_u8; 8192];

        let aligned = scratch.aligned_copy(&data, 8192);

        assert_eq!(aligned, data);
        assert!((aligned.as_ptr() as usize).is_multiple_of(8192));
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_alignment_for_path_is_at_least_default_alignment() {
        let temp = tempfile::tempdir().expect("tempdir");

        let alignment = unbuffered_write_alignment_for_path(temp.path());

        assert!(alignment >= WINDOWS_UNBUFFERED_WRITE_ALIGNMENT);
    }
}
