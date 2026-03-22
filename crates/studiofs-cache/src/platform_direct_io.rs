use std::io;
use std::io::{Read, Write};
use std::path::Path;
use tracing::debug;

pub(crate) const DIRECT_IO_ALIGNMENT: usize = 4096;

fn round_up_to_alignment(len: usize, alignment: usize) -> usize {
    if len == 0 {
        return 0;
    }
    len.div_ceil(alignment) * alignment
}

fn is_aligned(ptr: *const u8, alignment: usize) -> bool {
    (ptr as usize).is_multiple_of(alignment)
}

fn make_aligned_buffer(len: usize, alignment: usize) -> (Vec<u8>, usize) {
    if len == 0 {
        return (Vec::new(), 0);
    }
    let raw = vec![0u8; len + alignment - 1];
    let base = raw.as_ptr() as usize;
    let offset = (alignment - (base % alignment)) % alignment;
    (raw, offset)
}

fn read_all(mut std_file: std::fs::File, dst: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0usize;
    while filled < dst.len() {
        match std_file.read(&mut dst[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn read_from_std_file_direct_into(std_file: std::fs::File, dst: &mut [u8]) -> io::Result<usize> {
    if dst.is_empty() {
        return Ok(0);
    }

    let dst_aligned = is_aligned(dst.as_ptr(), DIRECT_IO_ALIGNMENT);
    let len_aligned = dst.len().is_multiple_of(DIRECT_IO_ALIGNMENT);

    if dst_aligned && len_aligned {
        let filled = read_all(std_file, dst)?;
        if filled == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "direct read returned EOF before data",
            ));
        }
        return Ok(filled);
    }

    // O_DIRECT requires aligned user pointers and aligned transfer lengths.
    // When the caller buffer is not suitable, read into a temporary aligned
    // bounce buffer and copy only the requested payload back to `dst`.
    let io_len = round_up_to_alignment(dst.len(), DIRECT_IO_ALIGNMENT);
    let (mut raw, offset) = make_aligned_buffer(io_len, DIRECT_IO_ALIGNMENT);
    let bounce = &mut raw[offset..offset + io_len];
    let filled = read_all(std_file, bounce)?;

    if filled == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "direct read returned EOF before data",
        ));
    }

    let copy_len = dst.len().min(filled);
    dst[..copy_len].copy_from_slice(&bounce[..copy_len]);
    Ok(copy_len)
}

fn write_to_std_file_direct(mut std_file: std::fs::File, data: &[u8]) -> io::Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let data_aligned = is_aligned(data.as_ptr(), DIRECT_IO_ALIGNMENT);
    let len_aligned = data.len().is_multiple_of(DIRECT_IO_ALIGNMENT);

    if data_aligned && len_aligned {
        std_file.write_all(data)?;
        return Ok(());
    }

    let io_len = round_up_to_alignment(data.len(), DIRECT_IO_ALIGNMENT);
    let (mut raw, offset) = make_aligned_buffer(io_len, DIRECT_IO_ALIGNMENT);
    let bounce = &mut raw[offset..offset + io_len];
    bounce[..data.len()].copy_from_slice(data);
    std_file.write_all(bounce)?;

    // Keep observable file size equal to payload length for unaligned tails.
    if io_len != data.len() {
        std_file.set_len(data.len() as u64)?;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::InvalidInput)
        || matches!(
            e.raw_os_error(),
            Some(libc::EINVAL)
                | Some(libc::EOPNOTSUPP)
                | Some(libc::ENOTSUP)
                | Some(libc::EPERM)
                | Some(libc::ENOSYS)
        )
}

#[cfg(target_os = "linux")]
fn open_read_file_direct(path: &Path) -> io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
        .open(path)
}

#[cfg(target_os = "linux")]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let direct = (|| -> io::Result<()> {
        let std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(path)?;
        write_to_std_file_direct(std_file, data)
    })();

    match direct {
        Ok(()) => Ok(()),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "O_DIRECT unavailable for write; falling back to buffered write"
            );
            std::fs::write(path, data)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "windows")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    const ERROR_INVALID_FUNCTION: i32 = 1;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_NOT_SUPPORTED: i32 = 50;
    const ERROR_INVALID_PARAMETER: i32 = 87;
    matches!(e.kind(), io::ErrorKind::InvalidInput)
        || matches!(
            e.raw_os_error(),
            Some(ERROR_INVALID_FUNCTION)
                | Some(ERROR_ACCESS_DENIED)
                | Some(ERROR_NOT_SUPPORTED)
                | Some(ERROR_INVALID_PARAMETER)
        )
}

#[cfg(target_os = "windows")]
fn open_read_file_direct(path: &Path) -> io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_NO_BUFFERING)
        .open(path)
}

#[cfg(target_os = "windows")]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;

    let direct = (|| -> io::Result<()> {
        let std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING)
            .open(path)?;
        write_to_std_file_direct(std_file, data)
    })();

    match direct {
        Ok(()) => Ok(()),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "Windows direct I/O unavailable for write; falling back to buffered write"
            );
            std::fs::write(path, data)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "macos")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::InvalidInput)
        || matches!(
            e.raw_os_error(),
            Some(libc::EINVAL)
                | Some(libc::EOPNOTSUPP)
                | Some(libc::ENOTSUP)
                | Some(libc::EPERM)
                | Some(libc::ENOTTY)
        )
}

#[cfg(target_os = "macos")]
fn open_read_file_direct(path: &Path) -> io::Result<std::fs::File> {
    use std::os::fd::AsRawFd;

    let std_file = std::fs::OpenOptions::new().read(true).open(path)?;
    let fd = std_file.as_raw_fd();
    let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
    if rc == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(std_file)
}

#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
pub(crate) fn open_block_direct_read(path: &Path) -> io::Result<Option<std::fs::File>> {
    match open_read_file_direct(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "direct I/O unavailable for block open; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
pub(crate) fn read_block_direct_from_file(
    std_file: std::fs::File,
    dst: &mut [u8],
) -> io::Result<Option<usize>> {
    match read_from_std_file_direct_into(std_file, dst) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "direct I/O unavailable for block read; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let direct = (|| -> io::Result<()> {
        let std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let fd = std_file.as_raw_fd();
        let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        write_to_std_file_direct(std_file, data)
    })();

    match direct {
        Ok(()) => Ok(()),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "F_NOCACHE unavailable for write; falling back to buffered write");
            std::fs::write(path, data)
        }
        Err(e) => Err(e),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) fn open_block_direct_read(_path: &Path) -> io::Result<Option<std::fs::File>> {
    Ok(None)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) fn read_block_direct_from_file(
    _std_file: std::fs::File,
    _dst: &mut [u8],
) -> io::Result<Option<usize>> {
    Ok(None)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    std::fs::write(path, data)
}
