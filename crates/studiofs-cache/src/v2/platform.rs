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

pub fn preallocate(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(preallocate_impl(file, offset, len))
}

pub fn punch_hole(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(punch_hole_impl(file, offset, len))
}

pub fn advise_will_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(advise_will_need_impl(file, offset, len))
}

pub fn advise_dont_need(file: &File, offset: u64, len: u64) -> io::Result<()> {
    if len == 0 {
        return Ok(());
    }
    best_effort(advise_dont_need_impl(file, offset, len))
}

pub fn advise_sequential(bytes: &[u8]) -> io::Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    best_effort(advise_sequential_impl(bytes))
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
                | libc::ENOTSUP
                | libc::ENOSYS
                | libc::EINVAL
                | libc::EPERM
                | libc::EACCES
                | libc::ENODEV
        )
    )
}

#[cfg(windows)]
fn is_benign_best_effort_error(_error: &io::Error) -> bool {
    // Windows-specific best-effort advisory primitives are still implemented as
    // safe no-ops. If future implementations start returning native errors,
    // keep the best-effort contract by treating unsupported advisory requests
    // (for example ERROR_INVALID_FUNCTION / ERROR_NOT_SUPPORTED) as benign.
    false
}

#[cfg(target_os = "linux")]
fn preallocate_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, offset, len) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn punch_hole_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    let result = unsafe { libc::fallocate(file.as_raw_fd(), mode, offset, len) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn punch_hole_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_will_need_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_WILLNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_will_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn advise_dont_need_impl(file: &File, offset: u64, len: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let offset = u64_to_off_t(offset)?;
    let len = u64_to_off_t(len)?;
    let result =
        unsafe { libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(result))
    }
}

#[cfg(not(target_os = "linux"))]
fn advise_dont_need_impl(_file: &File, _offset: u64, _len: u64) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn advise_sequential_impl(bytes: &[u8]) -> io::Result<()> {
    let result = unsafe {
        libc::madvise(
            bytes.as_ptr().cast::<libc::c_void>().cast_mut(),
            bytes.len(),
            libc::MADV_SEQUENTIAL,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn advise_sequential_impl(_bytes: &[u8]) -> io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
fn u64_to_off_t(value: u64) -> io::Result<libc::off_t> {
    libc::off_t::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "offset exceeds platform off_t range",
        )
    })
}
