use std::io;
use std::io::{Read, Write};
use std::path::Path;
use tracing::debug;

use crate::l2_disk::L2IoPriority;

pub(crate) const DIRECT_IO_ALIGNMENT: usize = 4096;
pub(crate) const IO_CHUNK_SIZE_BACKGROUND: usize = 1024 * 1024;

#[cfg(any(target_os = "linux", target_os = "windows"))]
fn is_aligned(ptr: *const (), alignment: usize) -> bool {
    (ptr as usize).is_multiple_of(alignment)
}

fn read_all(
    mut std_file: std::fs::File,
    dst: &mut [u8],
    priority: L2IoPriority,
) -> io::Result<usize> {
    if priority == L2IoPriority::Foreground {
        return std_file.read(dst);
    }

    let mut filled = 0usize;
    // Break into chunks to yield the device queue to other threads/requests.
    // This allows the OS disk scheduler to interleave smaller foreground client
    // reads, preventing massive latency spikes during background S3 uploads.
    while filled < dst.len() {
        let end = (filled + IO_CHUNK_SIZE_BACKGROUND).min(dst.len());
        match std_file.read(&mut dst[filled..end]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
fn read_from_std_file_direct_into(
    std_file: std::fs::File,
    dst: &mut [u8],
    priority: L2IoPriority,
) -> io::Result<usize> {
    if dst.is_empty() {
        return Ok(0);
    }

    let dst_aligned = is_aligned(dst.as_ptr().cast::<()>(), DIRECT_IO_ALIGNMENT);
    let len_aligned = dst.len().is_multiple_of(DIRECT_IO_ALIGNMENT);

    if dst_aligned && len_aligned {
        let filled = read_all(std_file, dst, priority)?;
        if filled == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "direct read returned EOF before data",
            ));
        }
        return Ok(filled);
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "direct read requires aligned buffer and length",
    ))
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
fn write_to_std_file_direct(mut std_file: std::fs::File, data: &[u8]) -> io::Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let data_aligned = is_aligned(data.as_ptr().cast::<()>(), DIRECT_IO_ALIGNMENT);
    let len_aligned = data.len().is_multiple_of(DIRECT_IO_ALIGNMENT);

    if data_aligned && len_aligned {
        std_file.write_all(data)?;
        return Ok(());
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "direct write requires aligned buffer and length",
    ))
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

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub(crate) fn read_block_direct_from_file(
    std_file: std::fs::File,
    dst: &mut [u8],
    priority: L2IoPriority,
) -> io::Result<Option<usize>> {
    match read_from_std_file_direct_into(std_file, dst, priority) {
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
pub(crate) fn read_block_direct_from_file(
    std_file: std::fs::File,
    dst: &mut [u8],
    priority: L2IoPriority,
) -> io::Result<Option<usize>> {
    if dst.is_empty() {
        return Ok(Some(0));
    }
    match read_all(std_file, dst, priority) {
        Ok(0) => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "direct read returned EOF before data",
        )),
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
        let mut std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let fd = std_file.as_raw_fd();
        let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        std_file.write_all(data)
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
    _priority: L2IoPriority,
) -> io::Result<Option<usize>> {
    Ok(None)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    std::fs::write(path, data)
}
