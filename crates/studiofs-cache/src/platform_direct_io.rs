use std::io;
use std::path::Path;

#[cfg(target_os = "linux")]
use std::io::{Read, Write};
#[cfg(target_os = "linux")]
use tracing::debug;

#[cfg(target_os = "linux")]
use crate::l2_disk::L2IoPriority;

#[cfg(target_os = "linux")]
pub(crate) const DIRECT_IO_ALIGNMENT: usize = 4096;
#[cfg(target_os = "linux")]
pub(crate) const IO_CHUNK_SIZE_BACKGROUND: usize = 1024 * 1024;

#[cfg(target_os = "linux")]
fn is_aligned(ptr: *const (), alignment: usize) -> bool {
    (ptr as usize).is_multiple_of(alignment)
}

#[cfg(target_os = "linux")]
fn read_all(
    mut std_file: std::fs::File,
    dst: &mut [u8],
    priority: L2IoPriority,
) -> io::Result<usize> {
    let mut filled = 0usize;
    let chunk_size = match priority {
        L2IoPriority::Foreground => dst.len().max(1),
        L2IoPriority::Background => IO_CHUNK_SIZE_BACKGROUND,
    };
    while filled < dst.len() {
        let end = (filled + chunk_size).min(dst.len());
        match std_file.read(&mut dst[filled..end]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
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
pub(crate) fn open_block_direct_read(path: &Path) -> io::Result<Option<std::fs::File>> {
    match open_read_file_direct(path) {
        Ok(file) => Ok(Some(file)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "O_DIRECT unavailable for block open; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "linux")]
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
                "O_DIRECT unavailable for block read; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
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

#[cfg(not(target_os = "linux"))]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    std::fs::write(path, data)
}
