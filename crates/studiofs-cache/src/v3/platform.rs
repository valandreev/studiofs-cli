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
fn sync_directory_impl(_path: &Path) -> io::Result<()> {
    // Opening a directory for fsync requires native Win32 flags that are not
    // available through stable std APIs. Treat as best-effort on Windows.
    Ok(())
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
