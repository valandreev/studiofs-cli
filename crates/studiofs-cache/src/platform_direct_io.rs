use bytes::{Bytes, BytesMut};
use std::io;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tracing::debug;

const DIRECT_IO_ALIGNMENT: usize = 4096;

fn aligned_bytes_mut(len: usize, alignment: usize) -> BytesMut {
    // Over-allocate, then trim a prefix so the active slice starts at an
    // alignment-compatible address for direct-I/O buffers.
    let mut raw = BytesMut::with_capacity(len + alignment);
    raw.resize(len + alignment, 0);
    let misalignment = (raw.as_ptr() as usize) % alignment;
    if misalignment != 0 {
        let prefix = alignment - misalignment;
        let _ = raw.split_to(prefix);
    }
    raw.truncate(len);
    raw
}

async fn read_from_std_file_direct(
    std_file: std::fs::File,
    expected_len: usize,
) -> io::Result<Bytes> {
    if expected_len == 0 {
        return Ok(Bytes::new());
    }

    let aligned_len = expected_len.next_multiple_of(DIRECT_IO_ALIGNMENT);
    let mut buf = aligned_bytes_mut(aligned_len, DIRECT_IO_ALIGNMENT);
    let mut file = tokio::fs::File::from_std(std_file);

    let mut filled = 0usize;
    while filled < aligned_len {
        match file.read(&mut buf[filled..]).await {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) => return Err(e),
        }
    }

    if filled == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "direct read returned EOF before data",
        ));
    }

    let actual_len = expected_len.min(filled);
    buf.truncate(actual_len);
    Ok(buf.freeze())
}

#[cfg(target_os = "linux")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EINVAL)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::ENOTSUP)
            | Some(libc::EPERM)
            | Some(libc::ENOSYS)
    )
}

#[cfg(target_os = "linux")]
pub(crate) async fn read_block_direct(path: PathBuf) -> io::Result<Option<Bytes>> {
    use std::os::unix::fs::OpenOptionsExt;

    let opened = tokio::task::spawn_blocking(move || -> io::Result<(std::fs::File, usize)> {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(&path)?;
        let expected_len = std_file.metadata()?.len() as usize;
        Ok((std_file, expected_len))
    })
    .await
    .map_err(io::Error::other)?;

    let (std_file, expected_len) = match opened {
        Ok(v) => v,
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "O_DIRECT unavailable for block; falling back to buffered read");
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    match read_from_std_file_direct(std_file, expected_len).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "O_DIRECT read failed; falling back to buffered read");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "windows")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    // Common Win32 codes for unsupported/invalid unbuffered requests.
    const ERROR_INVALID_FUNCTION: i32 = 1;
    const ERROR_ACCESS_DENIED: i32 = 5;
    const ERROR_NOT_SUPPORTED: i32 = 50;
    const ERROR_INVALID_PARAMETER: i32 = 87;
    matches!(
        e.raw_os_error(),
        Some(ERROR_INVALID_FUNCTION)
            | Some(ERROR_ACCESS_DENIED)
            | Some(ERROR_NOT_SUPPORTED)
            | Some(ERROR_INVALID_PARAMETER)
    )
}

#[cfg(target_os = "windows")]
pub(crate) async fn read_block_direct(path: PathBuf) -> io::Result<Option<Bytes>> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;

    let opened = tokio::task::spawn_blocking(move || -> io::Result<(std::fs::File, usize)> {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
            .open(&path)?;
        let expected_len = std_file.metadata()?.len() as usize;
        Ok((std_file, expected_len))
    })
    .await
    .map_err(io::Error::other)?;

    let (std_file, expected_len) = match opened {
        Ok(v) => v,
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "Windows direct I/O unavailable for block; falling back to buffered read"
            );
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    match read_from_std_file_direct(std_file, expected_len).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "Windows direct I/O read failed; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "macos")]
fn should_fallback_from_direct_io(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EINVAL)
            | Some(libc::EOPNOTSUPP)
            | Some(libc::ENOTSUP)
            | Some(libc::EPERM)
            | Some(libc::ENOTTY)
    )
}

#[cfg(target_os = "macos")]
pub(crate) async fn read_block_direct(path: PathBuf) -> io::Result<Option<Bytes>> {
    use std::os::fd::AsRawFd;

    let opened = tokio::task::spawn_blocking(move || -> io::Result<(std::fs::File, usize)> {
        let std_file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let fd = std_file.as_raw_fd();
        let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        let expected_len = std_file.metadata()?.len() as usize;
        Ok((std_file, expected_len))
    })
    .await
    .map_err(io::Error::other)?;

    let (std_file, expected_len) = match opened {
        Ok(v) => v,
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "F_NOCACHE unavailable for block; falling back to buffered read");
            return Ok(None);
        }
        Err(e) => return Err(e),
    };

    match read_from_std_file_direct(std_file, expected_len).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "F_NOCACHE read failed; falling back to buffered read");
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) async fn read_block_direct(_path: PathBuf) -> io::Result<Option<Bytes>> {
    Ok(None)
}
