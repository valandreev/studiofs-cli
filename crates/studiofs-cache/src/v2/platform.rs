//! Platform-specific I/O primitives for the data file.
//!
//! Provides pread/pwrite wrappers that are safe for concurrent access
//! from multiple threads without file-level seeking or locking.

use std::fs::File;
use std::io;

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
