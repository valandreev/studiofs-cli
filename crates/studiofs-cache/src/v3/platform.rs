//! Platform-specific primitives for Cache 3.0 payload files.

use std::fs::File;
use std::io;
use std::path::Path;

/// Read up to `buf.len()` bytes from `file` starting at `offset`.
///
/// Returns the number of bytes read, or `0` at EOF.
pub fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let n = pread_raw(file, &mut buf[total..], offset + total as u64)?;
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
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "pread_exact: EOF at offset {} (wanted {} more bytes)",
                offset + read as u64,
                buf.len() - read,
            ),
        ))
    }
}

/// Write all bytes to the file.
pub fn write_all(file: &mut File, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let n = std::io::Write::write(file, buf)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "write_all: write returned 0 bytes",
            ));
        }
        buf = &buf[n..];
    }
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

/// Best-effort sync for directory metadata after rename.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    sync_directory_impl(path)
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

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    // FILE_FLAG_BACKUP_SEMANTICS is required to open directory handles on
    // Windows through OpenOptions/File APIs.
    let dir = OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;

    match dir.sync_all() {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(
                    1 // ERROR_INVALID_FUNCTION
                    | 5 // ERROR_ACCESS_DENIED
                    | 50 // ERROR_NOT_SUPPORTED
                    | 87 // ERROR_INVALID_PARAMETER
                    | 120 // ERROR_CALL_NOT_IMPLEMENTED
                )
            ) =>
        {
            tracing::warn!(
                path = %path.display(),
                error = ?error,
                "cache-v3 directory sync is unsupported on this Windows filesystem"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
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
