use bytes::{Bytes, BytesMut};
use std::io;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use tracing::debug;

const DIRECT_IO_ALIGNMENT: usize = 4096;

fn aligned_uninit_bytes_mut(len: usize, alignment: usize) -> BytesMut {
    // Avoid zero-filling the entire read/write buffer. We only initialize the
    // exact slices that must be written (payload + optional tail padding).
    let mut raw = BytesMut::with_capacity(len + alignment);
    let base = raw.as_ptr() as usize;
    let misalignment = base % alignment;
    let prefix = if misalignment == 0 {
        0
    } else {
        alignment - misalignment
    };

    unsafe {
        raw.set_len(prefix + len);
    }
    if prefix != 0 {
        let _ = raw.split_to(prefix);
    }
    raw
}

fn read_from_std_file_direct(
    mut std_file: std::fs::File,
    expected_len: usize,
) -> io::Result<Bytes> {
    if expected_len == 0 {
        return Ok(Bytes::new());
    }

    if expected_len % DIRECT_IO_ALIGNMENT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file size is not direct-I/O aligned",
        ));
    }

    let mut buf = aligned_uninit_bytes_mut(expected_len, DIRECT_IO_ALIGNMENT);
    let mut filled = 0usize;
    while filled < expected_len {
        match std_file.read(&mut buf[filled..]) {
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

fn write_to_std_file_direct(mut std_file: std::fs::File, data: &[u8]) -> io::Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let aligned_len = data.len().next_multiple_of(DIRECT_IO_ALIGNMENT);
    let mut buf = aligned_uninit_bytes_mut(aligned_len, DIRECT_IO_ALIGNMENT);

    buf[..data.len()].copy_from_slice(data);
    if aligned_len > data.len() {
        buf[data.len()..].fill(0);
    }

    std_file.write_all(&buf)?;

    if aligned_len != data.len() {
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
pub(crate) async fn read_block_direct(
    path: PathBuf,
    expected_len_hint: Option<usize>,
) -> io::Result<Option<Bytes>> {
    use std::os::unix::fs::OpenOptionsExt;

    let outcome = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT | libc::O_CLOEXEC)
            .open(&path)?;
        let expected_len = match expected_len_hint {
            Some(len) => len,
            None => std_file.metadata()?.len() as usize,
        };
        read_from_std_file_direct(std_file, expected_len)
    })
    .await
    .map_err(io::Error::other)?;

    match outcome {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "O_DIRECT unavailable for block; falling back to buffered read");
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
pub(crate) async fn read_block_direct(
    path: PathBuf,
    expected_len_hint: Option<usize>,
) -> io::Result<Option<Bytes>> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;

    let outcome = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
        let std_file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
            .open(&path)?;
        let expected_len = match expected_len_hint {
            Some(len) => len,
            None => std_file.metadata()?.len() as usize,
        };
        read_from_std_file_direct(std_file, expected_len)
    })
    .await
    .map_err(io::Error::other)?;

    match outcome {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(
                error = %e,
                "Windows direct I/O unavailable for block; falling back to buffered read"
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

#[cfg(target_os = "windows")]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_NO_BUFFERING: u32 = 0x2000_0000;
    const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;

    let direct = (|| -> io::Result<()> {
        let std_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED)
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
pub(crate) async fn read_block_direct(
    path: PathBuf,
    expected_len_hint: Option<usize>,
) -> io::Result<Option<Bytes>> {
    use std::os::fd::AsRawFd;

    let outcome = tokio::task::spawn_blocking(move || -> io::Result<Bytes> {
        let std_file = std::fs::OpenOptions::new().read(true).open(&path)?;
        let fd = std_file.as_raw_fd();
        let rc = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
        if rc == -1 {
            return Err(io::Error::last_os_error());
        }
        let expected_len = match expected_len_hint {
            Some(len) => len,
            None => std_file.metadata()?.len() as usize,
        };
        read_from_std_file_direct(std_file, expected_len)
    })
    .await
    .map_err(io::Error::other)?;

    match outcome {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if should_fallback_from_direct_io(&e) => {
            debug!(error = %e, "F_NOCACHE unavailable for block; falling back to buffered read");
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
pub(crate) async fn read_block_direct(
    _path: PathBuf,
    _expected_len_hint: Option<usize>,
) -> io::Result<Option<Bytes>> {
    Ok(None)
}

#[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
pub(crate) fn write_file(path: &Path, data: &[u8]) -> io::Result<()> {
    std::fs::write(path, data)
}
