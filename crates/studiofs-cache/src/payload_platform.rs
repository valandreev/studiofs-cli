//! Platform-specific primitives for `v3` payload files.

#[cfg(windows)]
use parking_lot::Mutex;
#[cfg(windows)]
use std::collections::HashMap;
#[cfg(unix)]
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
#[cfg(windows)]
use std::sync::LazyLock;

/// Filesystem space visible to the current process at `path`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FilesystemSpace {
    pub(crate) available_bytes: u64,
    pub(crate) total_bytes: u64,
}

/// Return available space and physical filesystem capacity (`f_bavail` on Unix, quota-aware on Windows).
pub(crate) fn filesystem_space(path: &Path) -> io::Result<FilesystemSpace> {
    #[cfg(test)]
    FILESYSTEM_SPACE_QUERIES.with(|calls| calls.set(calls.get() + 1));
    filesystem_space_impl(path)
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static FILESYSTEM_SPACE_QUERIES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn checked_filesystem_space(available_bytes: u64, total_bytes: u64) -> io::Result<FilesystemSpace> {
    if total_bytes == 0 || available_bytes > total_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "filesystem returned inconsistent space totals",
        ));
    }
    Ok(FilesystemSpace {
        available_bytes,
        total_bytes,
    })
}

#[cfg(windows)]
fn filesystem_space_impl(path: &Path) -> io::Result<FilesystemSpace> {
    use std::os::windows::ffi::OsStrExt;

    if path.as_os_str().encode_wide().any(|unit| unit == 0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem path contains an interior NUL",
        ));
    }
    // Resolve junctions so the volume query sees the actual target volume root.
    let canonical = path.canonicalize()?;
    let mut canonical_wide: Vec<_> = canonical.as_os_str().encode_wide().collect();
    canonical_wide.push(0);

    #[link(name = "kernel32")]
    #[expect(
        unsafe_code,
        reason = "Declares the Windows volume-root and physical-space queries."
    )]
    unsafe extern "system" {
        #[link_name = "GetVolumePathNameW"]
        fn get_volume_path_name_w(
            file_name: *const u16,
            volume_path_name: *mut u16,
            buffer_length: u32,
        ) -> i32;
        #[link_name = "GetDiskSpaceInformationW"]
        fn get_disk_space_information_w(
            root_path: *const u16,
            disk_space_info: *mut WindowsDiskSpaceInformation,
        ) -> i32;
    }

    let mut volume_path = [0_u16; WINDOWS_VOLUME_PATH_BUFFER_U16];
    let buffer_len = u32::try_from(volume_path.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "volume path buffer too large"))?;
    // SAFETY: `canonical_wide` is NUL-terminated UTF-16 and `volume_path`
    // points to writable storage for `buffer_len` units through the call.
    #[expect(
        unsafe_code,
        reason = "Resolves the volume root for a validated canonical path."
    )]
    let has_root = unsafe {
        get_volume_path_name_w(
            canonical_wide.as_ptr(),
            volume_path.as_mut_ptr(),
            buffer_len,
        )
    };
    if has_root == 0 {
        return Err(io::Error::last_os_error());
    }
    let mut info = std::mem::MaybeUninit::<WindowsDiskSpaceInformation>::uninit();
    // SAFETY: `volume_path` is a NUL-terminated volume root from the
    // successful call above and `info` points to writable struct storage.
    #[expect(
        unsafe_code,
        reason = "Queries physical volume space for a resolved volume root."
    )]
    let status = unsafe { get_disk_space_information_w(volume_path.as_ptr(), info.as_mut_ptr()) };
    if status != 0 {
        return Err(io::Error::other(format!(
            "GetDiskSpaceInformationW failed (HRESULT 0x{status:08X})"
        )));
    }
    // SAFETY: S_OK means the API initialized the output structure.
    #[expect(
        unsafe_code,
        reason = "Reads the disk-space structure initialized by S_OK."
    )]
    let info = unsafe { info.assume_init() };
    windows_disk_space_to_filesystem_space(&info)
}

#[cfg(windows)]
#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct WindowsDiskSpaceInformation {
    actual_total_allocation_units: u64,
    actual_available_allocation_units: u64,
    _actual_pool_unavailable_allocation_units: u64,
    caller_total_allocation_units: u64,
    caller_available_allocation_units: u64,
    _caller_pool_unavailable_allocation_units: u64,
    _used_allocation_units: u64,
    _total_reserved_allocation_units: u64,
    _volume_storage_reserve_allocation_units: u64,
    _available_committed_allocation_units: u64,
    _pool_available_allocation_units: u64,
    sectors_per_allocation_unit: u32,
    bytes_per_sector: u32,
}

#[cfg(windows)]
fn windows_disk_space_to_filesystem_space(
    info: &WindowsDiskSpaceInformation,
) -> io::Result<FilesystemSpace> {
    let unit_bytes =
        u128::from(info.sectors_per_allocation_unit) * u128::from(info.bytes_per_sector);
    let total_bytes = u64::try_from(u128::from(info.actual_total_allocation_units) * unit_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "filesystem size overflow"))?;
    let available_units = info
        .caller_available_allocation_units
        .min(info.actual_available_allocation_units);
    let available_bytes = u64::try_from(u128::from(available_units) * unit_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "available space overflow"))?;
    checked_filesystem_space(available_bytes, total_bytes)
}

#[cfg(unix)]
fn filesystem_space_impl(path: &Path) -> io::Result<FilesystemSpace> {
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "filesystem path contains an interior NUL",
        )
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is NUL-terminated and `stats` points to writable storage
    // for the `statvfs` result for the duration of the call.
    #[expect(
        unsafe_code,
        reason = "Queries filesystem space for a validated Unix path."
    )]
    let result = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success, so it initialized the output structure.
    #[expect(
        unsafe_code,
        reason = "Reads the statvfs structure initialized by a successful call."
    )]
    let stats = unsafe { stats.assume_init() };
    let block_size = u128::from(stats.f_frsize);
    let total_bytes = u64::try_from(u128::from(stats.f_blocks) * block_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "filesystem size overflow"))?;
    let available_bytes = u64::try_from(u128::from(stats.f_bavail) * block_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "available space overflow"))?;
    checked_filesystem_space(available_bytes, total_bytes)
}

#[cfg(not(any(unix, windows)))]
fn filesystem_space_impl(_path: &Path) -> io::Result<FilesystemSpace> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "filesystem space queries are unsupported on this platform",
    ))
}

#[cfg(all(test, any(windows, target_os = "macos")))]
std::thread_local! {
    /// Holds clone-backed reads at the platform boundary for the concurrency regression.
    /// Per-thread storage keeps unrelated payload tests independent.
    pub(crate) static PREAD_TEST_GATE: std::cell::RefCell<Option<(
        std::sync::mpsc::Sender<()>,
        std::sync::mpsc::Receiver<()>,
    )>> = const { std::cell::RefCell::new(None) };
}

/// Blocks the current read at the platform boundary when a test armed the gate.
#[cfg(all(test, any(windows, target_os = "macos")))]
fn wait_pread_test_gate() {
    PREAD_TEST_GATE.with(|gate| {
        if let Some((entered, release)) = gate.borrow_mut().take() {
            entered
                .send(())
                .expect("concurrent-read entry channel must remain open");
            // Allow both 5-second entry waits to finish before the gate expires.
            release
                .recv_timeout(std::time::Duration::from_secs(15))
                .expect("concurrent-read coordinator must release the platform read");
        }
    });
}

#[cfg(windows)]
const WINDOWS_UNBUFFERED_WRITE_ALIGNMENT: usize = MINIMUM_UNBUFFERED_ALIGNMENT;

pub(crate) const MINIMUM_UNBUFFERED_ALIGNMENT: usize = if cfg!(windows) { 4096 } else { 1 };
#[cfg(windows)]
const WINDOWS_VOLUME_PATH_BUFFER_U16: usize = 32_768;
#[cfg(windows)]
const WINDOWS_FINAL_PATH_INITIAL_BUFFER_U16: usize = 256;
#[cfg(windows)]
const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

#[cfg(windows)]
static WINDOWS_VOLUME_ALIGNMENT_CACHE: LazyLock<Mutex<HashMap<WindowsVolumeRoot, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[cfg(windows)]
type WindowsVolumeRoot = [u16; 49];

#[derive(Debug, Default)]
/// Scratch buffer used to build aligned writes for unbuffered payload I/O.
pub struct UnbufferedWriteBuffer {
    #[cfg(any(windows, test))]
    backing: Vec<u8>,
}

impl UnbufferedWriteBuffer {
    /// Create an empty scratch buffer for unbuffered payload writes.
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    fn backing_capacity(&self) -> usize {
        self.backing.capacity()
    }
}

#[derive(Debug, Default)]
/// Scratch buffer used to build aligned reads for unbuffered payload I/O.
pub struct UnbufferedReadBuffer {
    #[cfg(windows)]
    backing: Vec<u8>,
}

impl UnbufferedReadBuffer {
    /// Create an empty scratch buffer for unbuffered payload reads.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Open a payload file for read-only access using platform-specific flags.
pub fn open_read_only(path: &Path) -> io::Result<File> {
    open_read_only_impl(path)
}

/// Open a payload file for read-only access using unbuffered flags when supported.
pub fn open_read_only_unbuffered(path: &Path) -> io::Result<File> {
    open_read_only_unbuffered_impl(path)
}

/// Read up to `buf.len()` bytes from `file` starting at `offset`.
///
/// Returns the number of bytes read, or `0` at EOF.
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    #[cfg(all(test, target_os = "macos"))]
    wait_pread_test_gate();
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

/// Return the required unbuffered-read alignment for files under `path`.
pub fn unbuffered_read_alignment_for_path(path: &Path) -> usize {
    unbuffered_read_alignment_for_path_impl(path)
}

/// Return the required unbuffered-I/O alignment for the volume containing `file`.
pub(crate) fn unbuffered_alignment_for_file(file: &File) -> Option<usize> {
    unbuffered_alignment_for_file_impl(file)
}

/// Return true when this range can be attempted on the unbuffered read path.
pub fn can_read_unbuffered_range(offset: u64, len: usize, alignment: usize) -> bool {
    can_read_unbuffered_range_impl(offset, len, alignment)
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

/// Read up to `buf.len()` bytes from an unbuffered payload file.
pub fn read_unbuffered_with_buffer(
    file: &File,
    buf: &mut [u8],
    offset: u64,
    scratch: &mut UnbufferedReadBuffer,
    alignment: usize,
) -> io::Result<usize> {
    read_unbuffered_impl(file, buf, offset, scratch, alignment)
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

/// Returns `true` when opening files failed due to descriptor exhaustion.
pub fn is_too_many_open_files_error(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
    }

    #[cfg(windows)]
    {
        // ERROR_TOO_MANY_OPEN_FILES
        matches!(error.raw_os_error(), Some(4))
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

/// Returns the current per-process open file descriptor limit when available.
pub fn open_file_descriptor_limit() -> Option<usize> {
    open_file_descriptor_limit_impl()
}

#[cfg(test)]
/// Builds a platform-specific "too many open files" error for cache fallback tests.
pub fn too_many_open_files_error_for_test() -> io::Error {
    #[cfg(unix)]
    {
        io::Error::from_raw_os_error(libc::EMFILE)
    }

    #[cfg(windows)]
    {
        io::Error::from_raw_os_error(4)
    }

    #[cfg(not(any(unix, windows)))]
    {
        io::Error::other("too many open files")
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

#[cfg(unix)]
fn open_file_descriptor_limit_impl() -> Option<usize> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: getrlimit initializes the provided rlimit on success.
    #[expect(
        unsafe_code,
        reason = "getrlimit writes its result to this valid output pointer."
    )]
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: rc == 0 means getrlimit wrote a valid rlimit.
    #[expect(
        unsafe_code,
        reason = "Successful getrlimit initialized the MaybeUninit value."
    )]
    let limit = unsafe { limit.assume_init() };
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return None;
    }
    usize::try_from(limit.rlim_cur).ok()
}

#[cfg(not(unix))]
fn open_file_descriptor_limit_impl() -> Option<usize> {
    None
}

#[cfg(windows)]
fn open_read_only_impl(path: &Path) -> io::Result<File> {
    // Let Windows apply its default cache and readahead policy here.
    // JuiceFS uses plain read-only opens for cache/staging files, and forcing
    // FILE_FLAG_RANDOM_ACCESS made our large sequential read/writeback mix much
    // less cooperative with the OS page cache.
    std::fs::OpenOptions::new().read(true).open(path)
}

#[cfg(target_os = "macos")]
fn open_read_only_unbuffered_impl(path: &Path) -> io::Result<File> {
    use std::os::fd::AsRawFd;

    let file = open_read_only(path)?;
    // SAFETY: `file` owns a live descriptor for the duration of this call.
    #[expect(
        unsafe_code,
        reason = "fcntl applies F_NOCACHE to this live file descriptor."
    )]
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(file)
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn open_read_only_unbuffered_impl(path: &Path) -> io::Result<File> {
    open_read_only(path)
}

#[cfg(windows)]
fn open_read_only_unbuffered_impl(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;

    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_SEQUENTIAL_SCAN)
        .open(path)
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
fn unbuffered_read_alignment_for_path_impl(_path: &Path) -> usize {
    1
}

#[cfg(windows)]
fn unbuffered_read_alignment_for_path_impl(path: &Path) -> usize {
    windows_unbuffered_write_alignment_for_path(path).unwrap_or(WINDOWS_UNBUFFERED_WRITE_ALIGNMENT)
}

#[cfg(not(windows))]
fn unbuffered_alignment_for_file_impl(_file: &File) -> Option<usize> {
    Some(1)
}

#[cfg(windows)]
fn unbuffered_alignment_for_file_impl(file: &File) -> Option<usize> {
    windows_unbuffered_write_alignment_for_file(file)
}

#[cfg(target_os = "macos")]
fn can_read_unbuffered_range_impl(_offset: u64, len: usize, _alignment: usize) -> bool {
    len != 0
}

#[cfg(all(not(windows), not(target_os = "macos")))]
fn can_read_unbuffered_range_impl(_offset: u64, _len: usize, _alignment: usize) -> bool {
    false
}

#[cfg(windows)]
fn can_read_unbuffered_range_impl(offset: u64, len: usize, alignment: usize) -> bool {
    let alignment = normalized_unbuffered_write_alignment(alignment);
    len != 0 && offset.is_multiple_of(alignment as u64) && len.is_multiple_of(alignment)
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

#[cfg(not(windows))]
fn read_unbuffered_impl(
    file: &File,
    buf: &mut [u8],
    offset: u64,
    _scratch: &mut UnbufferedReadBuffer,
    _alignment: usize,
) -> io::Result<usize> {
    pread(file, buf, offset)
}

#[cfg(windows)]
fn read_unbuffered_impl(
    file: &File,
    buf: &mut [u8],
    offset: u64,
    scratch: &mut UnbufferedReadBuffer,
    alignment: usize,
) -> io::Result<usize> {
    let alignment = normalized_unbuffered_write_alignment(alignment);
    if !can_read_unbuffered_range(offset, buf.len(), alignment) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unbuffered payload reads must be non-empty and {alignment}-byte aligned"),
        ));
    }

    #[cfg(test)]
    wait_pread_test_gate();
    if is_windows_unbuffered_ptr_aligned(buf.as_ptr() as usize, alignment) {
        return pread_raw(file, buf, offset);
    }

    let aligned = scratch.aligned_mut(buf.len(), alignment);
    let read = pread_raw(file, aligned, offset)?;
    buf[..read].copy_from_slice(&aligned[..read]);
    Ok(read)
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
impl UnbufferedReadBuffer {
    /// Heap bytes this scratch buffer keeps alive between reads.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.backing.capacity()
    }

    fn aligned_mut(&mut self, len: usize, alignment: usize) -> &mut [u8] {
        let alignment = normalized_unbuffered_write_alignment(alignment);
        let required_len = len.saturating_add(alignment - 1);
        if self.backing.len() < required_len {
            // Exact growth keeps retained capacity at the largest request, without doubling.
            self.backing
                .reserve_exact(required_len - self.backing.len());
            self.backing.resize(required_len, 0);
        }
        let base = self.backing.as_ptr() as usize;
        let offset = base.next_multiple_of(alignment) - base;
        &mut self.backing[offset..offset + len]
    }
}

#[cfg(windows)]
fn normalized_unbuffered_write_alignment(alignment: usize) -> usize {
    alignment.max(WINDOWS_UNBUFFERED_WRITE_ALIGNMENT)
}

#[cfg(windows)]
pub(crate) fn is_windows_unbuffered_ptr_aligned(ptr: usize, alignment: usize) -> bool {
    ptr.is_multiple_of(normalized_unbuffered_write_alignment(alignment))
}

#[cfg(windows)]
fn windows_unbuffered_write_alignment_for_path(path: &Path) -> Option<usize> {
    use std::os::windows::fs::OpenOptionsExt;

    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .ok()?;
    windows_unbuffered_write_alignment_for_file(&directory)
}

#[cfg(windows)]
fn windows_unbuffered_write_alignment_for_file(file: &File) -> Option<usize> {
    use std::os::windows::io::AsRawHandle;

    #[link(name = "kernel32")]
    #[expect(
        unsafe_code,
        reason = "Declares the two Windows volume-query FFI entry points."
    )]
    unsafe extern "system" {
        #[link_name = "GetFinalPathNameByHandleW"]
        fn get_final_path_name_by_handle_w(
            file: *mut std::ffi::c_void,
            file_path: *mut u16,
            buffer_length: u32,
            flags: u32,
        ) -> u32;

        #[link_name = "GetDiskFreeSpaceW"]
        fn get_disk_free_space_w(
            root_path_name: *const u16,
            sectors_per_cluster: *mut u32,
            bytes_per_sector: *mut u32,
            number_of_free_clusters: *mut u32,
            total_number_of_clusters: *mut u32,
        ) -> i32;

    }

    const VOLUME_NAME_GUID: u32 = 0x1;
    let mut final_path = [0_u16; WINDOWS_FINAL_PATH_INITIAL_BUFFER_U16];
    let mut full_path = Vec::new();
    let volume_root = loop {
        let buffer = if full_path.is_empty() {
            &mut final_path[..]
        } else {
            &mut full_path[..]
        };
        // SAFETY: `file` owns a live handle and `buffer` is writable for the
        // checked number of UTF-16 code units, including space for the NUL.
        #[expect(
            unsafe_code,
            reason = "Reads the canonical volume-GUID path from a live file handle."
        )]
        let path_len = unsafe {
            get_final_path_name_by_handle_w(
                file.as_raw_handle(),
                buffer.as_mut_ptr(),
                u32::try_from(buffer.len()).ok()?,
                VOLUME_NAME_GUID,
            )
        };
        let path_len = usize::try_from(path_len).ok()?;
        if path_len == 0 {
            return None;
        }
        if path_len < buffer.len() {
            break windows_volume_guid_root(&buffer[..path_len])?;
        }
        if !full_path.is_empty() || path_len > WINDOWS_VOLUME_PATH_BUFFER_U16 {
            return None;
        }
        full_path.resize(path_len, 0);
    };
    let mut cache = WINDOWS_VOLUME_ALIGNMENT_CACHE.lock();
    cached_windows_volume_alignment(&mut cache, volume_root, |volume_root| {
        let mut volume_root_wide = [0_u16; 50];
        volume_root_wide[..49].copy_from_slice(volume_root);
        let mut sectors_per_cluster = 0_u32;
        let mut bytes_per_sector = 0_u32;
        let mut free_clusters = 0_u32;
        let mut total_clusters = 0_u32;
        // SAFETY: `volume_root_wide` is a NUL-terminated UTF-16 GUID root and
        // each output points to a live `u32` for the duration of the call.
        #[expect(
            unsafe_code,
            reason = "Queries disk geometry with a validated volume root."
        )]
        let disk_ok = unsafe {
            get_disk_free_space_w(
                volume_root_wide.as_ptr(),
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
    })
}

#[cfg(windows)]
fn windows_volume_guid_root(final_path: &[u16]) -> Option<WindowsVolumeRoot> {
    let mut root: WindowsVolumeRoot = final_path.get(..49)?.try_into().ok()?;
    for unit in &mut root {
        *unit = u16::from(u8::try_from(*unit).ok()?.to_ascii_lowercase());
    }
    (root[..11].iter().copied().eq(r"\\?\volume{".encode_utf16())
        && root[47..] == [u16::from(b'}'), u16::from(b'\\')])
    .then_some(root)
}

#[cfg(windows)]
fn cached_windows_volume_alignment(
    cache: &mut HashMap<WindowsVolumeRoot, usize>,
    volume_root: WindowsVolumeRoot,
    query_alignment: impl FnOnce(&WindowsVolumeRoot) -> Option<usize>,
) -> Option<usize> {
    if let Some(alignment) = cache.get(&volume_root) {
        return Some(*alignment);
    }

    let alignment = query_alignment(&volume_root)?;
    cache.insert(volume_root, alignment);
    Some(alignment)
}

#[cfg(test)]
#[cfg(windows)]
fn copy_for_unbuffered_test<'a>(scratch: &'a mut UnbufferedWriteBuffer, buf: &[u8]) -> &'a [u8] {
    scratch.aligned_copy(buf, WINDOWS_UNBUFFERED_WRITE_ALIGNMENT)
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
    #[expect(
        unsafe_code,
        reason = "Applies POSIX_FADV_RANDOM to this live file descriptor."
    )]
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
    #[expect(
        unsafe_code,
        reason = "Applies POSIX_FADV_WILLNEED to checked offsets on a live file."
    )]
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

    #[test]
    fn filesystem_space_reports_sensible_values_for_nested_temp_path() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let nested = temp.path().join("nested").join("filesystem");
        std::fs::create_dir_all(&nested).unwrap();

        let space = filesystem_space(&nested).unwrap();

        assert!(space.total_bytes > 0);
        assert!(space.available_bytes <= space.total_bytes);
    }

    #[test]
    fn filesystem_space_fails_for_invalid_paths() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let missing = temp.path().join("missing").join("nested");

        assert!(filesystem_space(&missing).is_err());
    }

    #[test]
    fn filesystem_space_rejects_inconsistent_totals() {
        assert_eq!(
            checked_filesystem_space(0, 0).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            checked_filesystem_space(2, 1).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_space_rejects_paths_with_interior_nul() {
        use std::os::unix::ffi::OsStrExt;

        let path = Path::new(std::ffi::OsStr::from_bytes(b"/tmp\0invalid"));

        assert_eq!(
            filesystem_space(path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(windows)]
    #[test]
    fn filesystem_space_rejects_paths_with_interior_nul() {
        use std::os::windows::ffi::OsStringExt;

        let os_path = std::ffi::OsString::from_wide(&[u16::from(b'C'), 0, u16::from(b':')]);
        let path = Path::new(&os_path);

        assert_eq!(
            filesystem_space(path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(windows)]
    #[test]
    fn filesystem_space_uses_physical_total_with_quota_available() {
        let info = WindowsDiskSpaceInformation {
            actual_total_allocation_units: 1_000,
            actual_available_allocation_units: 800,
            _actual_pool_unavailable_allocation_units: 0,
            caller_total_allocation_units: 100,
            caller_available_allocation_units: 50,
            _caller_pool_unavailable_allocation_units: 0,
            _used_allocation_units: 200,
            _total_reserved_allocation_units: 0,
            _volume_storage_reserve_allocation_units: 0,
            _available_committed_allocation_units: 0,
            _pool_available_allocation_units: 0,
            sectors_per_allocation_unit: 2,
            bytes_per_sector: 512,
        };

        let space = windows_disk_space_to_filesystem_space(&info).unwrap();

        assert_eq!(space.total_bytes, 1_000 * 1_024);
        assert_eq!(space.available_bytes, 50 * 1_024);
        assert_ne!(
            space.total_bytes,
            info.caller_total_allocation_units * 1_024
        );
    }

    #[cfg(windows)]
    #[test]
    fn filesystem_space_rejects_disk_space_overflow() {
        let info = WindowsDiskSpaceInformation {
            actual_total_allocation_units: u64::MAX,
            actual_available_allocation_units: u64::MAX,
            _actual_pool_unavailable_allocation_units: 0,
            caller_total_allocation_units: u64::MAX,
            caller_available_allocation_units: u64::MAX,
            _caller_pool_unavailable_allocation_units: 0,
            _used_allocation_units: 0,
            _total_reserved_allocation_units: 0,
            _volume_storage_reserve_allocation_units: 0,
            _available_committed_allocation_units: 0,
            _pool_available_allocation_units: 0,
            sectors_per_allocation_unit: 2,
            bytes_per_sector: 512,
        };

        assert_eq!(
            windows_disk_space_to_filesystem_space(&info)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn cross_device_detection_matches_exdev() {
        let exdev = io::Error::from_raw_os_error(libc::EXDEV);
        assert!(is_cross_device_link_error(&exdev));

        let not_exdev = io::Error::from_raw_os_error(libc::EINVAL);
        assert!(!is_cross_device_link_error(&not_exdev));
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_pressure_detection_matches_emfile_and_enfile() {
        let emfile = io::Error::from_raw_os_error(libc::EMFILE);
        let enfile = io::Error::from_raw_os_error(libc::ENFILE);
        let other = io::Error::from_raw_os_error(libc::EINVAL);

        assert!(is_too_many_open_files_error(&emfile));
        assert!(is_too_many_open_files_error(&enfile));
        assert!(!is_too_many_open_files_error(&other));
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
    fn windows_unbuffered_read_scratch_grows_exactly() {
        const MIB: usize = 1024 * 1024;
        let mut scratch = UnbufferedReadBuffer::new();

        scratch.aligned_mut(2 * MIB + 4096, 4096);
        scratch.aligned_mut(4 * MIB, 4096);

        assert_eq!(scratch.retained_bytes(), 4 * MIB + 4095);
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_alignment_for_path_is_at_least_default_alignment() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");

        assert!(windows_unbuffered_write_alignment_for_path(temp.path()).is_some());
        let alignment = unbuffered_write_alignment_for_path(temp.path());

        assert!(alignment >= WINDOWS_UNBUFFERED_WRITE_ALIGNMENT);
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_alignment_for_file_resolves_its_volume() {
        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let path = temp.path().join("payload");
        let file = create_new_unbuffered_write(&path).unwrap();

        assert!(unbuffered_alignment_for_file_impl(&file).is_some());
    }

    #[cfg(windows)]
    #[test]
    fn windows_unbuffered_alignment_for_long_path_resolves_its_volume() {
        use std::os::windows::ffi::OsStrExt;

        let temp = studiofs_test_support::workspace_target_tempdir("test-");
        let directory = temp.path().join("a".repeat(120)).join("b".repeat(120));
        assert!(
            directory.as_os_str().encode_wide().count() > WINDOWS_FINAL_PATH_INITIAL_BUFFER_U16
        );
        std::fs::create_dir_all(&directory).unwrap();
        let file = create_new_unbuffered_write(&directory.join("payload")).unwrap();

        assert!(unbuffered_alignment_for_file_impl(&file).is_some());
    }

    #[cfg(windows)]
    #[test]
    fn windows_volume_alignment_cache_is_keyed_by_volume_and_only_caches_success() {
        let mut cache = HashMap::new();
        let root = |path: &str| {
            windows_volume_guid_root(&path.encode_utf16().collect::<Vec<_>>()).unwrap()
        };
        let volume_a = root(r"\\?\Volume{00000000-0000-0000-0000-000000000001}\");
        let volume_b = root(r"\\?\Volume{00000000-0000-0000-0000-000000000002}\");
        let mut queries = 0;

        let first = cached_windows_volume_alignment(&mut cache, volume_a, |_| {
            queries += 1;
            Some(8192)
        });
        let same_volume = cached_windows_volume_alignment(&mut cache, volume_a, |_| {
            queries += 1;
            Some(4096)
        });
        let other_volume = cached_windows_volume_alignment(&mut cache, volume_b, |_| {
            queries += 1;
            Some(4096)
        });

        assert_eq!(
            (first, same_volume, other_volume),
            (Some(8192), Some(8192), Some(4096))
        );
        assert_eq!(queries, 2, "each volume's geometry should be queried once");

        let failed_volume = root(r"\\?\Volume{00000000-0000-0000-0000-000000000003}\");
        assert_eq!(
            cached_windows_volume_alignment(&mut cache, failed_volume, |_| None),
            None
        );
        assert_eq!(
            cached_windows_volume_alignment(&mut cache, failed_volume, |_| Some(4096)),
            Some(4096),
            "failed geometry lookup must not poison the cache"
        );

        let final_path: Vec<_> = r"\\?\VOLUME{ABCDEF01-2345-6789-ABCD-0123456789AB}\payload\object"
            .encode_utf16()
            .collect();
        assert_eq!(
            windows_volume_guid_root(&final_path),
            Some(root(r"\\?\volume{abcdef01-2345-6789-abcd-0123456789ab}\"))
        );
        let network_path: Vec<_> = r"\\server\share\payload".encode_utf16().collect();
        assert_eq!(windows_volume_guid_root(&network_path), None);
        for invalid in [r"\\?\Volume{abc}", r"\\?\Volume{abc}x", r"\\?\Volume{abc"] {
            assert_eq!(
                windows_volume_guid_root(&invalid.encode_utf16().collect::<Vec<_>>()),
                None
            );
        }
    }
}
