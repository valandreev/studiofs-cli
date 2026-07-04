//! Clean Linux bridge shell for StudioFS.
//!
//! This crate owns the Linux-only `/dev/fuse` boundary and enough shell
//! operations to prove the new path is isolated from the deleted legacy backend.

use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use studiofs_bridge::posix::{BridgeFileAttr, to_bridge_attr, vfs_err_to_errno};
use studiofs_meta::ROOT_INO;
use studiofs_vfs::StudioVfs;

const DEFAULT_BLOCK_SIZE: u32 = 4096;
const FUSE_DEVICE: &str = "/dev/fuse";
const FUSE_MOUNT_TYPE: &str = "fuse";
const FUSE_SOURCE: &str = "studiofs";
const FUSE_IN_HEADER_SIZE: usize = 40;
const FUSE_INIT: u32 = 26;
const FUSE_DESTROY: u32 = 38;
const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_KERNEL_MINOR_VERSION: u32 = 31;
const FUSE_ASYNC_READ: u32 = 1 << 0;
const FUSE_BIG_WRITES: u32 = 1 << 5;
const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;
const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
const FUSE_MAX_PAGES: u32 = 1 << 22;
const STUDIOFS_REQUESTED_CAPS: u32 = FUSE_ASYNC_READ
    | FUSE_BIG_WRITES
    | FUSE_AUTO_INVAL_DATA
    | FUSE_PARALLEL_DIROPS
    | FUSE_MAX_PAGES;
const FUSE_COMPAT_MAX_WRITE: u32 = 128 * 1024;
const FUSE_MAX_PAGES_DEFAULT: u16 = 256;
const FUSE_REQUEST_BUFFER_SIZE: usize = 1024 * 1024 + 4096;
const FUSE_POLL_TIMEOUT: Duration = Duration::from_secs(30);

/// Linux bridge launch request owned by the CLI boundary.
pub struct LinuxMountRequest {
    /// Shared VFS instance used by the Linux bridge boundary.
    pub vfs: Arc<StudioVfs>,
    /// Directory where the Linux FUSE filesystem should be mounted.
    pub mount_point: PathBuf,
}

/// Minimal Linux mount shell for lifecycle and root namespace smoke checks.
pub struct LinuxMountShell {
    request: LinuxMountRequest,
}

/// Directory entry shape returned by the Linux shell boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinuxDirEntry {
    /// StudioFS inode number.
    pub ino: u64,
    /// Raw POSIX entry name bytes.
    pub name: Vec<u8>,
}

/// Mount `/dev/fuse`, negotiate the kernel session, then hold the single-fd shell.
pub fn mount_with_request(request: LinuxMountRequest) -> io::Result<()> {
    let mut device = open_fuse_device()?;
    mount_fuse_device(&request.mount_point, device.as_raw_fd())?;
    let negotiated = read_and_reply_init(&mut device)?;
    apply_read_ahead_kb_best_effort(&request.mount_point, negotiated.out.max_readahead / 1024);
    run_single_fd_shell(device)
}

impl LinuxMountShell {
    /// Create an uninitialized Linux shell from a mount request.
    pub fn new(request: LinuxMountRequest) -> Self {
        Self { request }
    }

    /// Accept the Linux init lifecycle callback.
    pub fn init(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return bridge attributes for an inode.
    pub fn getattr(&self, ino: u64) -> io::Result<BridgeFileAttr> {
        self.request
            .vfs
            .getattr(ino)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Resolve a child name under a parent inode.
    pub fn lookup(&self, parent: u64, name: &[u8]) -> io::Result<BridgeFileAttr> {
        if parent == ROOT_INO && matches!(name, b"." | b"") {
            return self.getattr(ROOT_INO);
        }
        self.request
            .vfs
            .lookup(parent, name)
            .map(|attr| to_bridge_attr(&attr, 0, 0, DEFAULT_BLOCK_SIZE))
            .map_err(io_error_from_vfs)
    }

    /// Read directory entries for an inode.
    pub fn readdir(&self, ino: u64) -> io::Result<Vec<LinuxDirEntry>> {
        self.request
            .vfs
            .readdir(ino)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| LinuxDirEntry {
                        ino: entry.ino,
                        name: entry.name,
                    })
                    .collect()
            })
            .map_err(io_error_from_vfs)
    }

    /// Accept the Linux destroy lifecycle callback.
    pub fn destroy(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// Return `ENOSYS` for shell operations not implemented yet.
    pub fn unsupported() -> io::Result<()> {
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

fn io_error_from_vfs(error: studiofs_vfs::VfsError) -> io::Error {
    io::Error::from_raw_os_error(vfs_err_to_errno(&error))
}

fn open_fuse_device() -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(FUSE_DEVICE)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("open {FUSE_DEVICE} failed: {error}; check FUSE device permissions"),
            )
        })
}

fn mount_fuse_device(mount_point: &Path, fd: RawFd) -> io::Result<()> {
    let mode = std::fs::metadata(mount_point)
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "read mount point {} metadata failed: {error}",
                    mount_point.display()
                ),
            )
        })?
        .permissions()
        .mode()
        & 0o7777;
    let options = format!(
        "fd={fd},rootmode={mode:o},user_id={},group_id={},default_permissions,fsname=studiofs,subtype=studiofs",
        current_uid(),
        current_gid()
    );
    let source = cstring(FUSE_SOURCE)?;
    let target = cstring_path(mount_point)?;
    let fstype = cstring(FUSE_MOUNT_TYPE)?;
    let data = cstring(&options)?;

    // SAFETY: all pointers are valid NUL-terminated C strings for the duration
    // of the call, and `data` points to the mount option string expected by the
    // Linux `mount(2)` FUSE filesystem type.
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            (libc::MS_NODEV | libc::MS_NOSUID) as libc::c_ulong,
            data.as_ptr().cast(),
        )
    };
    if rc == 0 {
        return Ok(());
    }

    let error = io::Error::last_os_error();
    let hint = match error.raw_os_error() {
        Some(libc::EACCES | libc::EPERM) => {
            "; FUSE mount requires host permission for /dev/fuse and mount(2)"
        }
        _ => "",
    };
    Err(io::Error::new(
        error.kind(),
        format!(
            "mount {FUSE_DEVICE} at {} failed: {error}{hint}",
            mount_point.display()
        ),
    ))
}

fn current_uid() -> libc::uid_t {
    // SAFETY: getuid has no preconditions.
    unsafe { libc::getuid() }
}

fn current_gid() -> libc::gid_t {
    // SAFETY: getgid has no preconditions.
    unsafe { libc::getgid() }
}

fn cstring(value: &str) -> io::Result<CString> {
    CString::new(value)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string contains NUL byte"))
}

fn cstring_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("path {} contains NUL byte", path.display()),
        )
    })
}

fn read_and_reply_init(device: &mut File) -> io::Result<NegotiatedInit> {
    let mut buffer = vec![0_u8; FUSE_REQUEST_BUFFER_SIZE];
    let len = read_fuse_message(device, &mut buffer)?;
    let header = parse_in_header(&buffer[..len])?;
    if header.opcode != FUSE_INIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "expected FUSE_INIT as first request, got opcode {}",
                header.opcode
            ),
        ));
    }
    let init = parse_init_in(&buffer[FUSE_IN_HEADER_SIZE..len])?;
    let negotiated = negotiate_init(&init, page_size()?)?;
    write_init_reply(device, header.unique, &negotiated.out)?;
    tracing::info!(
        requested_caps = format_args!("0x{:08x}", negotiated.requested_caps),
        accepted_caps = format_args!("0x{:08x}", negotiated.accepted_caps),
        max_write = negotiated.out.max_write,
        max_pages = negotiated.out.max_pages,
        max_readahead = negotiated.out.max_readahead,
        "linux fuse init negotiated"
    );
    Ok(negotiated)
}

fn run_single_fd_shell(mut device: File) -> io::Result<()> {
    // ponytail: single-fd ENOSYS loop; SFS-584 replaces this with real receive/work/reply loops.
    let mut buffer = vec![0_u8; FUSE_REQUEST_BUFFER_SIZE];
    loop {
        let len = read_fuse_message(&mut device, &mut buffer)?;
        let header = parse_in_header(&buffer[..len])?;
        if header.opcode == FUSE_DESTROY {
            return Ok(());
        }
        write_error_reply(&mut device, header.unique, libc::ENOSYS)?;
    }
}

fn read_fuse_message(device: &mut File, buffer: &mut [u8]) -> io::Result<usize> {
    loop {
        match device.read(buffer) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "FUSE device closed while waiting for request",
                ));
            }
            Ok(len) => return Ok(len),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd(device.as_raw_fd(), libc::POLLIN)?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_all_fuse(device: &mut File, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match device.write(bytes) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "FUSE device accepted zero reply bytes",
                ));
            }
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                poll_fd(device.as_raw_fd(), libc::POLLOUT)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn poll_fd(fd: RawFd, events: libc::c_short) -> io::Result<()> {
    let mut pollfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    let timeout_ms = i32::try_from(FUSE_POLL_TIMEOUT.as_millis()).unwrap_or(i32::MAX);
    // SAFETY: `pollfd` points to one initialized poll descriptor.
    let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
    match rc {
        0 => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "timed out waiting for FUSE device readiness",
        )),
        n if n > 0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

fn page_size() -> io::Result<u32> {
    // SAFETY: sysconf has no preconditions for `_SC_PAGESIZE`.
    let value = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    u32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(io::Error::last_os_error)
}

fn negotiate_init(init: &FuseInitIn, page_size: u32) -> io::Result<NegotiatedInit> {
    if init.major != FUSE_KERNEL_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported FUSE ABI major version {}", init.major),
        ));
    }
    let requested_caps = STUDIOFS_REQUESTED_CAPS;
    let mut accepted = init.flags & STUDIOFS_REQUESTED_CAPS;
    let max_pages = if init.flags & FUSE_MAX_PAGES == 0 {
        accepted &= !FUSE_MAX_PAGES;
        0
    } else {
        FUSE_MAX_PAGES_DEFAULT
    };
    let max_write = if max_pages == 0 {
        FUSE_COMPAT_MAX_WRITE
    } else {
        page_size.saturating_mul(u32::from(max_pages))
    };
    let out = FuseInitOut {
        major: FUSE_KERNEL_VERSION,
        minor: FUSE_KERNEL_MINOR_VERSION.min(init.minor),
        max_readahead: init.max_readahead,
        flags: accepted,
        max_background: 0,
        congestion_threshold: 0,
        max_write,
        time_gran: 1,
        max_pages,
        map_alignment: 0,
        flags2: 0,
        max_stack_depth: 0,
        request_timeout: 0,
        unused: [0; 11],
    };
    Ok(NegotiatedInit {
        out,
        requested_caps,
        accepted_caps: accepted,
    })
}

fn write_init_reply(device: &mut File, unique: u64, init: &FuseInitOut) -> io::Result<()> {
    let header = FuseOutHeader {
        len: (mem::size_of::<FuseOutHeader>() + mem::size_of::<FuseInitOut>()) as u32,
        error: 0,
        unique,
    };
    write_all_fuse(device, struct_bytes(&header))?;
    write_all_fuse(device, struct_bytes(init))
}

fn write_error_reply(device: &mut File, unique: u64, errno: i32) -> io::Result<()> {
    let header = FuseOutHeader {
        len: mem::size_of::<FuseOutHeader>() as u32,
        error: -errno,
        unique,
    };
    write_all_fuse(device, struct_bytes(&header))
}

fn struct_bytes<T>(value: &T) -> &[u8] {
    // SAFETY: `value` is a valid reference and the returned byte slice is
    // bounded to its exact in-memory size for immediate kernel writes.
    unsafe { std::slice::from_raw_parts(std::ptr::from_ref(value).cast(), mem::size_of::<T>()) }
}

fn parse_in_header(bytes: &[u8]) -> io::Result<FuseInHeader> {
    if bytes.len() < FUSE_IN_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE request header",
        ));
    }
    let len = read_u32(bytes, 0);
    if usize::try_from(len).map_or(true, |len| len > bytes.len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "FUSE request length exceeds received bytes",
        ));
    }
    Ok(FuseInHeader {
        opcode: read_u32(bytes, 4),
        unique: read_u64(bytes, 8),
    })
}

fn parse_init_in(bytes: &[u8]) -> io::Result<FuseInitIn> {
    if bytes.len() < 16 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE_INIT request",
        ));
    }
    Ok(FuseInitIn {
        major: read_u32(bytes, 0),
        minor: read_u32(bytes, 4),
        max_readahead: read_u32(bytes, 8),
        flags: read_u32(bytes, 12),
        flags2: if bytes.len() >= 20 {
            read_u32(bytes, 16)
        } else {
            0
        },
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    let mut value = [0_u8; 4];
    value.copy_from_slice(&bytes[offset..offset + 4]);
    u32::from_ne_bytes(value)
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_ne_bytes(value)
}

fn apply_read_ahead_kb_best_effort(mount_point: &Path, kb: u32) {
    if kb == 0 {
        return;
    }
    match mountinfo_bdi_path(mount_point) {
        Ok(Some(path)) => {
            if let Err(error) = std::fs::write(&path, kb.to_string()) {
                tracing::warn!(
                    path = %mount_point.display(),
                    bdi = %path.display(),
                    %error,
                    "linux fuse BDI read_ahead_kb tuning skipped"
                );
            }
        }
        Ok(None) => tracing::warn!(
            path = %mount_point.display(),
            "linux fuse BDI read_ahead_kb tuning skipped: mountinfo entry not found"
        ),
        Err(error) => tracing::warn!(
            path = %mount_point.display(),
            %error,
            "linux fuse BDI read_ahead_kb tuning skipped: mountinfo unreadable"
        ),
    }
}

fn mountinfo_bdi_path(mount_point: &Path) -> io::Result<Option<PathBuf>> {
    std::fs::read_to_string("/proc/self/mountinfo")
        .map(|mountinfo| mountinfo_bdi_path_from(&mountinfo, mount_point))
}

fn mountinfo_bdi_path_from(mountinfo: &str, mount_point: &Path) -> Option<PathBuf> {
    let target = mount_point.to_string_lossy();
    for line in mountinfo.lines() {
        let mut fields = line.split_whitespace();
        let Some(majmin) = fields.nth(2) else {
            continue;
        };
        let Some(mounted_at) = fields.nth(1) else {
            continue;
        };
        if unescape_mountinfo_field(mounted_at) == target {
            return Some(PathBuf::from(format!(
                "/sys/class/bdi/{majmin}/read_ahead_kb"
            )));
        }
    }
    None
}

fn unescape_mountinfo_field(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

#[derive(Debug)]
struct NegotiatedInit {
    out: FuseInitOut,
    requested_caps: u32,
    accepted_caps: u32,
}

#[derive(Debug, Clone, Copy)]
struct FuseInHeader {
    opcode: u32,
    unique: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FuseOutHeader {
    len: u32,
    error: i32,
    unique: u64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FuseInitIn {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
    flags2: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct FuseInitOut {
    major: u32,
    minor: u32,
    max_readahead: u32,
    flags: u32,
    max_background: u16,
    congestion_threshold: u16,
    max_write: u32,
    time_gran: u32,
    max_pages: u16,
    map_alignment: u16,
    flags2: u32,
    max_stack_depth: u32,
    request_timeout: u16,
    unused: [u16; 11],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negotiate_init_uses_kernel_readahead_and_supported_pages() {
        let init = FuseInitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead: 768 * 1024,
            flags: FUSE_MAX_PAGES | FUSE_ASYNC_READ,
            flags2: 0,
        };

        let negotiated = negotiate_init(&init, 4096).expect("negotiate init");

        assert_eq!(negotiated.out.max_readahead, 768 * 1024);
        assert_eq!(negotiated.out.max_pages, 256);
        assert_eq!(negotiated.out.max_write, 1024 * 1024);
        assert_ne!(negotiated.accepted_caps & FUSE_MAX_PAGES, 0);
    }

    #[test]
    fn negotiate_init_omits_pages_when_kernel_does_not_offer_it() {
        let init = FuseInitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_KERNEL_MINOR_VERSION,
            max_readahead: 256 * 1024,
            flags: FUSE_ASYNC_READ,
            flags2: 0,
        };

        let negotiated = negotiate_init(&init, 4096).expect("negotiate init");

        assert_eq!(negotiated.out.max_readahead, 256 * 1024);
        assert_eq!(negotiated.out.max_pages, 0);
        assert_eq!(negotiated.out.max_write, 128 * 1024);
        assert_eq!(negotiated.accepted_caps & FUSE_MAX_PAGES, 0);
    }

    #[test]
    fn mountinfo_bdi_path_from_returns_read_ahead_path() {
        let mountinfo = "126 32 0:114 / /mnt/studiofs rw,relatime shared:98 - fuse studiofs rw,user_id=1000,group_id=1000\n";

        assert_eq!(
            mountinfo_bdi_path_from(mountinfo, std::path::Path::new("/mnt/studiofs")),
            Some(std::path::PathBuf::from(
                "/sys/class/bdi/0:114/read_ahead_kb"
            ))
        );
    }
}
