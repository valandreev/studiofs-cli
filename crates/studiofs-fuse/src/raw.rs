use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use studiofs_bridge::posix::{posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{LocalSyncPolicy, StudioVfs};
use tokio::runtime::Handle;
use tracing::{debug, info, warn};

const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_MINOR_VERSION: u32 = 38;
const FUSE_ROOT_ID: u64 = 1;

const OP_LOOKUP: u32 = 1;
const OP_FORGET: u32 = 2;
const OP_GETATTR: u32 = 3;
const OP_SETATTR: u32 = 4;
const OP_OPEN: u32 = 14;
const OP_READ: u32 = 15;
const OP_WRITE: u32 = 16;
const OP_STATFS: u32 = 17;
const OP_RELEASE: u32 = 18;
const OP_FSYNC: u32 = 20;
const OP_SETXATTR: u32 = 21;
const OP_GETXATTR: u32 = 22;
const OP_LISTXATTR: u32 = 23;
const OP_REMOVEXATTR: u32 = 24;
const OP_FLUSH: u32 = 25;
const OP_INIT: u32 = 26;
const OP_OPENDIR: u32 = 27;
const OP_READDIR: u32 = 28;
const OP_RELEASEDIR: u32 = 29;
const OP_ACCESS: u32 = 34;
const OP_CREATE: u32 = 35;
const OP_DESTROY: u32 = 38;
const OP_IOCTL: u32 = 39;
const OP_READDIRPLUS: u32 = 44;

const FATTR_MODE: u32 = 1 << 0;
const FATTR_UID: u32 = 1 << 1;
const FATTR_GID: u32 = 1 << 2;
const FATTR_SIZE: u32 = 1 << 3;
const FATTR_ATIME: u32 = 1 << 4;
const FATTR_MTIME: u32 = 1 << 5;
const FATTR_FH: u32 = 1 << 6;
const FATTR_ATIME_NOW: u32 = 1 << 7;
const FATTR_MTIME_NOW: u32 = 1 << 8;

pub const FUSE_ASYNC_READ: u32 = 1 << 0;
pub const FUSE_FILE_OPS: u32 = 1 << 2;
pub const FUSE_BIG_WRITES: u32 = 1 << 5;
pub const FUSE_AUTO_INVAL_DATA: u32 = 1 << 12;
pub const FUSE_READDIRPLUS: u32 = 1 << 13;
pub const FUSE_READDIRPLUS_AUTO: u32 = 1 << 14;
pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
pub const FUSE_MAX_PAGES: u32 = 1 << 22;
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25;

const WANTED_CAPS: [u32; 7] = [
    FUSE_ASYNC_READ,
    FUSE_BIG_WRITES,
    FUSE_MAX_PAGES,
    FUSE_PARALLEL_DIROPS,
    FUSE_READDIRPLUS,
    FUSE_FILE_OPS,
    FUSE_EXPLICIT_INVAL_DATA,
];

/// Mount request for the Linux raw `/dev/fuse` backend.
pub struct RawFuseMountRequest {
    /// Filesystem mount point path.
    pub mount_point: String,
    /// Volume name used in the kernel mount options.
    pub volume_name: String,
    /// Whether the mounted filesystem should be read-only.
    pub read_only: bool,
    /// Linux INIT profile.
    pub profile: LinuxRawFuseProfile,
    /// Shared VFS instance exposed through raw FUSE callbacks.
    pub vfs: Option<Arc<StudioVfs>>,
    /// Tokio runtime used for async VFS read/write/barrier operations.
    pub runtime_handle: Option<Handle>,
}

impl RawFuseMountRequest {
    /// Create a raw FUSE mount request with JuiceFS-like Linux defaults.
    pub fn new(mount_point: impl Into<String>, volume_name: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            profile: LinuxRawFuseProfile::juicefs_like(),
            vfs: None,
            runtime_handle: None,
        }
    }

    pub fn with_vfs(mut self, vfs: Arc<StudioVfs>, runtime_handle: Handle) -> Self {
        self.vfs = Some(vfs);
        self.runtime_handle = Some(runtime_handle);
        self
    }
}

/// One Linux FUSE profile used for mount options and INIT negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxRawFuseProfile {
    pub max_write: u32,
    pub max_read: u32,
    pub max_pages: u16,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_readahead: u32,
}

impl LinuxRawFuseProfile {
    pub fn juicefs_like() -> Self {
        let max_write = 1024 * 1024;
        Self {
            max_write,
            max_read: max_write,
            max_pages: (max_write / 4096) as u16,
            max_background: 50,
            congestion_threshold: 37,
            max_readahead: 1024 * 1024,
        }
    }

    pub fn negotiate_init(self, input: InitIn) -> NegotiatedInit {
        let mut flags = 0;
        let mut degraded = Vec::new();
        for cap in WANTED_CAPS {
            if input.flags & cap != 0 {
                flags |= cap;
            } else {
                degraded.push(cap);
            }
        }
        if flags & FUSE_READDIRPLUS != 0 {
            flags |= FUSE_READDIRPLUS_AUTO;
        }
        if flags & FUSE_EXPLICIT_INVAL_DATA == 0 && input.flags & FUSE_AUTO_INVAL_DATA != 0 {
            flags |= FUSE_AUTO_INVAL_DATA;
        }

        NegotiatedInit {
            out: InitOut {
                major: FUSE_KERNEL_VERSION,
                minor: FUSE_MINOR_VERSION.min(input.minor),
                max_readahead: self.max_readahead.min(input.max_readahead),
                flags,
                max_background: self.max_background,
                congestion_threshold: self.congestion_threshold,
                max_write: self.max_write,
                time_gran: 1,
                max_pages: if flags & FUSE_MAX_PAGES != 0 {
                    self.max_pages
                } else {
                    0
                },
                padding: 0,
                unused: [0; 8],
            },
            degraded,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedInit {
    pub out: InitOut,
    pub degraded: Vec<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawDispatchClass {
    Metadata,
    Stream,
}

impl RawDispatchClass {
    fn for_opcode(opcode: u32) -> Self {
        match opcode {
            OP_READ | OP_WRITE | OP_FLUSH | OP_FSYNC | OP_RELEASE => Self::Stream,
            OP_LOOKUP | OP_GETATTR | OP_SETATTR | OP_STATFS | OP_OPENDIR | OP_READDIR
            | OP_RELEASEDIR | OP_ACCESS | OP_CREATE | OP_SETXATTR | OP_GETXATTR | OP_LISTXATTR
            | OP_REMOVEXATTR | OP_IOCTL | OP_READDIRPLUS | OP_OPEN => Self::Metadata,
            _ => Self::Metadata,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RawDispatchSnapshot {
    inflight_total: usize,
    inflight_stream: usize,
    request_count: u64,
    reply_bytes: u64,
    read_latency_count: u64,
    read_latency_nanos: u64,
    write_latency_count: u64,
    write_latency_nanos: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawDispatchAdmissionError {
    QueueFull,
    StreamQueueFull,
}

#[derive(Debug, Clone)]
struct RawDispatchAccounting {
    inner: Arc<RawDispatchAccountingInner>,
}

#[derive(Debug)]
struct RawDispatchAccountingInner {
    max_inflight: usize,
    max_stream_inflight: usize,
    inflight_total: AtomicUsize,
    inflight_stream: AtomicUsize,
    request_count: AtomicU64,
    reply_bytes: AtomicU64,
    read_latency_count: AtomicU64,
    read_latency_nanos: AtomicU64,
    write_latency_count: AtomicU64,
    write_latency_nanos: AtomicU64,
}

impl RawDispatchAccounting {
    fn new(max_inflight: usize, max_stream_inflight: usize) -> Self {
        Self {
            inner: Arc::new(RawDispatchAccountingInner {
                max_inflight: max_inflight.max(1),
                max_stream_inflight: max_stream_inflight.max(1),
                inflight_total: AtomicUsize::new(0),
                inflight_stream: AtomicUsize::new(0),
                request_count: AtomicU64::new(0),
                reply_bytes: AtomicU64::new(0),
                read_latency_count: AtomicU64::new(0),
                read_latency_nanos: AtomicU64::new(0),
                write_latency_count: AtomicU64::new(0),
                write_latency_nanos: AtomicU64::new(0),
            }),
        }
    }

    fn try_admit(
        &self,
        class: RawDispatchClass,
    ) -> Result<RawDispatchAdmissionGuard, RawDispatchAdmissionError> {
        increment_bounded(&self.inner.inflight_total, self.inner.max_inflight)
            .map_err(|()| RawDispatchAdmissionError::QueueFull)?;
        if class == RawDispatchClass::Stream
            && increment_bounded(&self.inner.inflight_stream, self.inner.max_stream_inflight)
                .is_err()
        {
            self.inner.inflight_total.fetch_sub(1, Ordering::AcqRel);
            return Err(RawDispatchAdmissionError::StreamQueueFull);
        }
        Ok(RawDispatchAdmissionGuard {
            accounting: self.clone(),
            class,
        })
    }

    fn record_request(&self) {
        self.inner.request_count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_reply_bytes(&self, bytes: usize) {
        self.inner
            .reply_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_latency(&self, opcode: u32, latency: Duration) {
        let nanos = latency.as_nanos().min(u128::from(u64::MAX)) as u64;
        match opcode {
            OP_READ => {
                self.inner
                    .read_latency_count
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .read_latency_nanos
                    .fetch_add(nanos, Ordering::Relaxed);
            }
            OP_WRITE => {
                self.inner
                    .write_latency_count
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .write_latency_nanos
                    .fetch_add(nanos, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn snapshot(&self) -> RawDispatchSnapshot {
        RawDispatchSnapshot {
            inflight_total: self.inner.inflight_total.load(Ordering::Acquire),
            inflight_stream: self.inner.inflight_stream.load(Ordering::Acquire),
            request_count: self.inner.request_count.load(Ordering::Acquire),
            reply_bytes: self.inner.reply_bytes.load(Ordering::Acquire),
            read_latency_count: self.inner.read_latency_count.load(Ordering::Acquire),
            read_latency_nanos: self.inner.read_latency_nanos.load(Ordering::Acquire),
            write_latency_count: self.inner.write_latency_count.load(Ordering::Acquire),
            write_latency_nanos: self.inner.write_latency_nanos.load(Ordering::Acquire),
        }
    }
}

#[derive(Debug)]
struct RawDispatchAdmissionGuard {
    accounting: RawDispatchAccounting,
    class: RawDispatchClass,
}

impl Drop for RawDispatchAdmissionGuard {
    fn drop(&mut self) {
        self.accounting
            .inner
            .inflight_total
            .fetch_sub(1, Ordering::AcqRel);
        if self.class == RawDispatchClass::Stream {
            self.accounting
                .inner
                .inflight_stream
                .fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn increment_bounded(counter: &AtomicUsize, max: usize) -> Result<(), ()> {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current >= max {
            return Err(());
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return Ok(()),
            Err(next) => current = next,
        }
    }
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InHeader {
    len: u32,
    opcode: u32,
    unique: u64,
    nodeid: u64,
    uid: u32,
    gid: u32,
    pid: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct InitIn {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitOut {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub padding: u16,
    pub unused: [u32; 8],
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct OpenIn {
    flags: u32,
    open_flags: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ReadIn {
    fh: u64,
    offset: u64,
    size: u32,
    read_flags: u32,
    lock_owner: u64,
    flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct WriteIn {
    fh: u64,
    offset: u64,
    size: u32,
    write_flags: u32,
    lock_owner: u64,
    flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FlushIn {
    fh: u64,
    unused: u32,
    padding: u32,
    lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct FsyncIn {
    fh: u64,
    fsync_flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ReleaseIn {
    fh: u64,
    flags: u32,
    release_flags: u32,
    lock_owner: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SetattrIn {
    valid: u32,
    padding: u32,
    fh: u64,
    size: u64,
    lock_owner: u64,
    atime: u64,
    mtime: u64,
    ctime: u64,
    atimensec: u32,
    mtimensec: u32,
    ctimensec: u32,
    mode: u32,
    unused4: u32,
    uid: u32,
    gid: u32,
    unused5: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct SetxattrIn {
    size: u32,
    flags: u32,
    setxattr_flags: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct GetxattrIn {
    size: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct IoctlIn {
    fh: u64,
    flags: u32,
    cmd: u32,
    arg: u64,
    in_size: u32,
    out_size: u32,
}

/// Mount and serve a minimal Linux raw FUSE session.
pub fn mount_raw_with_request(request: RawFuseMountRequest) -> io::Result<()> {
    let RawFuseMountRequest {
        mount_point,
        volume_name,
        read_only,
        profile,
        vfs,
        runtime_handle,
    } = request;

    let mount_point = PathBuf::from(mount_point);
    let dev_fuse = mount_dev_fuse(&mount_point, &volume_name, read_only, profile)?;
    info!(mount_point = %mount_point.display(), "mounted StudioFS raw FUSE session");
    let _guard = RawMountGuard(&mount_point);
    serve_minimal_session(dev_fuse.as_raw_fd(), profile, vfs, runtime_handle)
}

fn mount_dev_fuse(
    mount_point: &Path,
    volume_name: &str,
    read_only: bool,
    profile: LinuxRawFuseProfile,
) -> io::Result<File> {
    let dev_fuse = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")?;
    match mount_dev_fuse_direct(
        dev_fuse.as_raw_fd(),
        mount_point,
        volume_name,
        read_only,
        profile,
    ) {
        Ok(()) => Ok(dev_fuse),
        Err(err) if err.raw_os_error() == Some(libc::EPERM) => {
            call_fusermount(mount_point, volume_name, read_only, profile)
        }
        Err(err) => Err(err),
    }
}

fn mount_dev_fuse_direct(
    fd: RawFd,
    mount_point: &Path,
    volume_name: &str,
    read_only: bool,
    profile: LinuxRawFuseProfile,
) -> io::Result<()> {
    let mount_point_c = cstring_path(mount_point)?;
    let source = CString::new(if volume_name.is_empty() {
        "studiofs"
    } else {
        volume_name
    })
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "volume name contains NUL"))?;
    let fstype = c"fuse.studiofs";
    let mode = std::fs::metadata(mount_point)?.mode();
    let data = CString::new(format!(
        "fd={fd},rootmode={:o},user_id={},group_id={},max_read={}",
        mode,
        // SAFETY: `geteuid` has no preconditions and does not dereference pointers.
        unsafe { libc::geteuid() },
        // SAFETY: `getegid` has no preconditions and does not dereference pointers.
        unsafe { libc::getegid() },
        profile.max_read
    ))
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mount data contains NUL"))?;
    let mut flags = libc::MS_NOSUID | libc::MS_NODEV;
    if read_only {
        flags |= libc::MS_RDONLY;
    }

    // SAFETY: all pointers are valid NUL-terminated C strings for this call,
    // and `data` stays alive until `mount(2)` returns.
    let rc = unsafe {
        libc::mount(
            source.as_ptr(),
            mount_point_c.as_ptr(),
            fstype.as_ptr(),
            flags,
            data.as_ptr().cast(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn call_fusermount(
    mount_point: &Path,
    volume_name: &str,
    read_only: bool,
    profile: LinuxRawFuseProfile,
) -> io::Result<File> {
    let (local, remote) = unix_seqpacket_socketpair()?;
    let bin = fusermount_binary()?;
    let options = fusermount_options(volume_name, read_only, profile);
    let remote_fd = remote.as_raw_fd();
    let mut command = Command::new(bin);
    command.arg(mount_point);
    command.arg("-o").arg(options);
    command.env("_FUSE_COMMFD", "3");
    // SAFETY: `pre_exec` runs after fork and before exec; `dup2`/`fcntl` are
    // async-signal-safe and only prepare the fusermount communication fd.
    unsafe {
        command.pre_exec(move || {
            if remote_fd == 3 {
                let flags = libc::fcntl(3, libc::F_GETFD);
                if flags < 0 || libc::fcntl(3, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(io::Error::last_os_error());
                }
            } else if libc::dup2(remote_fd, 3) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let status = command.status()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "{bin} exited with status {status}"
        )));
    }
    recv_fuse_fd(&local)
}

fn unix_seqpacket_socketpair() -> io::Result<(File, File)> {
    let mut fds = [-1; 2];
    // SAFETY: `fds` points to two writable fd slots for `socketpair`.
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, fds.as_mut_ptr()) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `socketpair` returned two owned file descriptors.
    let left = unsafe { File::from_raw_fd(fds[0]) };
    // SAFETY: `socketpair` returned two owned file descriptors.
    let right = unsafe { File::from_raw_fd(fds[1]) };
    Ok((left, right))
}

fn fusermount_options(volume_name: &str, read_only: bool, profile: LinuxRawFuseProfile) -> String {
    let fsname = if volume_name.is_empty() {
        "studiofs"
    } else {
        volume_name
    };
    let mut options = format!(
        "fsname={fsname},subtype=studiofs,max_read={}",
        profile.max_read
    );
    if read_only {
        options.push_str(",ro");
    }
    options
}

fn fusermount_binary() -> io::Result<&'static str> {
    if binary_in_path("fusermount3") || Path::new("/bin/fusermount3").exists() {
        Ok("fusermount3")
    } else if binary_in_path("fusermount") || Path::new("/bin/fusermount").exists() {
        Ok("fusermount")
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            "fusermount3 or fusermount not found",
        ))
    }
}

fn binary_in_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
}

fn recv_fuse_fd(socket: &File) -> io::Result<File> {
    let mut byte = 0_u8;
    let mut iov = libc::iovec {
        iov_base: (&mut byte as *mut u8).cast(),
        iov_len: 1,
    };
    let mut control = [0_usize; 8];
    let mut msg = libc::msghdr {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: &mut iov,
        msg_iovlen: 1,
        msg_control: control.as_mut_ptr().cast(),
        msg_controllen: size_of::<[usize; 8]>(),
        msg_flags: 0,
    };
    // SAFETY: `msg` points to live iovec/control buffers for this `recvmsg`.
    let rc = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut msg, 0) };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `recvmsg` initialized the control buffer; CMSG helpers only walk
    // within `msg_controllen`.
    let cmsg = unsafe { libc::CMSG_FIRSTHDR(&msg) };
    if cmsg.is_null() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fusermount did not return a FUSE fd",
        ));
    }
    // SAFETY: `cmsg` is non-null and points inside the control buffer.
    let is_rights =
        unsafe { (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS };
    if !is_rights {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fusermount returned unexpected control message",
        ));
    }
    // SAFETY: SCM_RIGHTS payload starts with at least one received file
    // descriptor when fusermount succeeds.
    let fd = unsafe { *libc::CMSG_DATA(cmsg).cast::<RawFd>() };
    // SAFETY: ownership of this fd was transferred through SCM_RIGHTS.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn serve_minimal_session(
    fd: RawFd,
    profile: LinuxRawFuseProfile,
    vfs: Option<Arc<StudioVfs>>,
    runtime_handle: Option<Handle>,
) -> io::Result<()> {
    let mut buf = vec![0_u8; profile.max_write as usize + 4096];
    let ids = SessionIds {
        // SAFETY: `geteuid` has no preconditions and does not dereference pointers.
        uid: unsafe { libc::geteuid() },
        // SAFETY: `getegid` has no preconditions and does not dereference pointers.
        gid: unsafe { libc::getegid() },
    };
    let accounting = RawDispatchAccounting::new(
        profile.max_background as usize,
        profile.congestion_threshold as usize,
    );
    let context = RawRequestContext {
        vfs,
        runtime_handle,
        ids,
    };
    let mut workers = Vec::new();
    let session_result = loop {
        let len = match read_fd(fd, &mut buf) {
            Ok(len) => len,
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENODEV | libc::EIO | libc::ENOENT)
                ) =>
            {
                break Ok(());
            }
            Err(err) => break Err(err),
        };
        if len == 0 {
            break Ok(());
        }
        if len < size_of::<InHeader>() {
            break Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short FUSE request header",
            ));
        }
        let header = read_struct::<InHeader>(&buf[..size_of::<InHeader>()])?;
        accounting.record_request();
        if header.opcode == OP_INIT {
            handle_init(
                fd,
                header,
                &buf[size_of::<InHeader>()..len],
                profile,
                Some(&accounting),
            )?;
            continue;
        }
        let payload = buf[size_of::<InHeader>()..len].to_vec();
        dispatch_minimal_request(
            fd,
            header,
            payload,
            context.clone(),
            &accounting,
            &mut workers,
        )?;
        reap_finished_workers(&mut workers)?;
    };
    join_workers(workers)?;
    session_result
}

fn dispatch_minimal_request(
    fd: RawFd,
    header: InHeader,
    payload: Vec<u8>,
    context: RawRequestContext,
    accounting: &RawDispatchAccounting,
    workers: &mut Vec<JoinHandle<io::Result<()>>>,
) -> io::Result<()> {
    match accounting.try_admit(RawDispatchClass::for_opcode(header.opcode)) {
        Ok(guard) => {
            let accounting = accounting.clone();
            workers.push(std::thread::spawn(move || {
                let _guard = guard;
                let started = Instant::now();
                let result = handle_minimal_request(fd, header, &payload, &context, &accounting);
                accounting.record_latency(header.opcode, started.elapsed());
                result
            }));
            Ok(())
        }
        Err(err) => {
            let snapshot = accounting.snapshot();
            warn!(
                opcode = header.opcode,
                unique = header.unique,
                inflight_total = snapshot.inflight_total,
                inflight_stream = snapshot.inflight_stream,
                ?err,
                "raw FUSE request rejected by bounded dispatcher"
            );
            reply_error(fd, header.unique, libc::EAGAIN, Some(accounting))
        }
    }
}

fn join_workers(workers: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    for worker in workers {
        match worker.join() {
            Ok(result) => result?,
            Err(_) => return Err(io::Error::other("raw FUSE worker thread panicked")),
        }
    }
    Ok(())
}

fn reap_finished_workers(workers: &mut Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    let mut index = 0;
    while index < workers.len() {
        if workers[index].is_finished() {
            let worker = workers.swap_remove(index);
            match worker.join() {
                Ok(result) => result?,
                Err(_) => return Err(io::Error::other("raw FUSE worker thread panicked")),
            }
        } else {
            index += 1;
        }
    }
    Ok(())
}

fn handle_minimal_request(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    match header.opcode {
        OP_GETATTR => reply_getattr(fd, header.unique, header.nodeid, context, Some(accounting)),
        OP_STATFS => reply_statfs(fd, header.unique, Some(accounting)),
        OP_OPEN => raw_open(fd, header, payload, context, accounting),
        OP_SETATTR => raw_setattr(fd, header, payload, context, accounting),
        OP_READ => raw_read(fd, header, payload, context, accounting),
        OP_WRITE => raw_write(fd, header, payload, context, accounting),
        OP_FLUSH => raw_flush(fd, header, payload, context, accounting),
        OP_FSYNC => raw_fsync(fd, header, payload, context, accounting),
        OP_RELEASE => raw_release(fd, header, payload, context, accounting),
        OP_SETXATTR => raw_setxattr(fd, header, payload, context, accounting),
        OP_GETXATTR => raw_getxattr(fd, header, payload, context, accounting),
        OP_LISTXATTR => raw_listxattr(fd, header, payload, context, accounting),
        OP_REMOVEXATTR => raw_removexattr(fd, header, payload, context, accounting),
        OP_IOCTL => raw_ioctl(fd, header, payload, context, accounting),
        OP_OPENDIR => reply_open(fd, header.unique, 1, 0, Some(accounting)),
        OP_READDIR | OP_READDIRPLUS => raw_readdir(fd, header, payload, context, accounting),
        OP_ACCESS | OP_RELEASEDIR => reply_empty(fd, header.unique, Some(accounting)),
        OP_LOOKUP => raw_lookup(fd, header, payload, context, accounting),
        OP_FORGET | OP_DESTROY => Ok(()),
        opcode => {
            debug!(
                opcode,
                "raw FUSE minimal backend does not implement request"
            );
            reply_error(fd, header.unique, libc::ENOSYS, Some(accounting))
        }
    }
}

fn raw_setattr(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<SetattrIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<SetattrIn>(&payload[..size_of::<SetattrIn>()])?;
    let args = setattr_args_from_raw(input);
    let fh = if input.valid & FATTR_FH != 0 {
        Some(input.fh)
    } else {
        None
    };
    match vfs.setattr_with_handle(fh, header.nodeid, args) {
        Ok(attr) => reply_attr(fd, header.unique, &attr, context, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_lookup(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOENT, Some(accounting));
    };
    let name = payload.strip_suffix(&[0]).unwrap_or(payload);
    match vfs.lookup(header.nodeid, name) {
        Ok(attr) => reply_entry(fd, header.unique, &attr, context, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_open(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_open(fd, header.unique, 1, 0, Some(accounting));
    };
    if payload.len() < size_of::<OpenIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<OpenIn>(&payload[..size_of::<OpenIn>()])?;
    let Ok(flags) = i32::try_from(input.flags) else {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    };
    let (open_flags, trunc) = posix_flags_to_open_flags(flags);
    match vfs.open_file(header.nodeid, open_flags, trunc) {
        Ok(fh) => reply_open(fd, header.unique, fh, 0, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_read(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    let Some(rt) = &context.runtime_handle else {
        return reply_error(fd, header.unique, libc::EIO, Some(accounting));
    };
    if payload.len() < size_of::<ReadIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<ReadIn>(&payload[..size_of::<ReadIn>()])?;
    let mut out = vec![0; input.size as usize];
    match rt.block_on(vfs.read_into_fh(input.fh, header.nodeid, input.offset, &mut out)) {
        Ok(read) => {
            out.truncate(read as usize);
            reply_data(fd, header.unique, &out, Some(accounting))
        }
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_write(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    let Some(rt) = &context.runtime_handle else {
        return reply_error(fd, header.unique, libc::EIO, Some(accounting));
    };
    if payload.len() < size_of::<WriteIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<WriteIn>(&payload[..size_of::<WriteIn>()])?;
    let data = &payload[size_of::<WriteIn>()..];
    let write_len = (input.size as usize).min(data.len());
    match rt.block_on(vfs.write_fh_with_deferred_materialize(
        input.fh,
        header.nodeid,
        input.offset,
        &data[..write_len],
        LocalSyncPolicy::Opportunistic,
    )) {
        Ok(written) => reply_write(fd, header.unique, written, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_flush(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_empty(fd, header.unique, Some(accounting));
    };
    let Some(rt) = &context.runtime_handle else {
        return reply_error(fd, header.unique, libc::EIO, Some(accounting));
    };
    if payload.len() < size_of::<FlushIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<FlushIn>(&payload[..size_of::<FlushIn>()])?;
    match rt.block_on(vfs.flush_fh(input.fh)) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_fsync(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_empty(fd, header.unique, Some(accounting));
    };
    let Some(rt) = &context.runtime_handle else {
        return reply_error(fd, header.unique, libc::EIO, Some(accounting));
    };
    if payload.len() < size_of::<FsyncIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<FsyncIn>(&payload[..size_of::<FsyncIn>()])?;
    let datasync = input.fsync_flags & 1 != 0;
    let result = if input.fh != 0 {
        rt.block_on(vfs.flush_fh(input.fh))
    } else {
        rt.block_on(vfs.fsync(header.nodeid, datasync))
    };
    match result {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_release(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_empty(fd, header.unique, Some(accounting));
    };
    let Some(rt) = &context.runtime_handle else {
        return reply_error(fd, header.unique, libc::EIO, Some(accounting));
    };
    if payload.len() < size_of::<ReleaseIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<ReleaseIn>(&payload[..size_of::<ReleaseIn>()])?;
    match rt.block_on(vfs.release_fh(input.fh)) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_readdir(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_empty(fd, header.unique, Some(accounting));
    };
    if payload.len() < size_of::<ReadIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<ReadIn>(&payload[..size_of::<ReadIn>()])?;
    let mut out = Vec::with_capacity(input.size as usize);
    if input.offset == 0 {
        push_dirent(
            &mut out,
            FUSE_ROOT_ID,
            1,
            libc::DT_DIR,
            b".",
            input.size as usize,
        );
        push_dirent(
            &mut out,
            FUSE_ROOT_ID,
            2,
            libc::DT_DIR,
            b"..",
            input.size as usize,
        );
    }
    let entries = match vfs.readdir_with_attrs(header.nodeid) {
        Ok(entries) => entries,
        Err(err) => {
            return reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting));
        }
    };
    for (index, entry_with_attr) in entries
        .iter()
        .enumerate()
        .skip(input.offset.saturating_sub(2) as usize)
    {
        let entry = &entry_with_attr.entry;
        let Ok(offset) = u64::try_from(index + 3) else {
            return reply_error(fd, header.unique, libc::EOVERFLOW, Some(accounting));
        };
        let dtype = match entry.kind {
            FileType::RegularFile => libc::DT_REG,
            FileType::Directory => libc::DT_DIR,
            FileType::Symlink => libc::DT_LNK,
        };
        if !push_dirent(
            &mut out,
            entry.ino,
            offset,
            dtype,
            &entry.name,
            input.size as usize,
        ) {
            break;
        }
    }
    reply_data(fd, header.unique, &out, Some(accounting))
}

fn raw_setxattr(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<SetxattrIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<SetxattrIn>(&payload[..size_of::<SetxattrIn>()])?;
    let rest = &payload[size_of::<SetxattrIn>()..];
    let Some(name_end) = rest.iter().position(|byte| *byte == 0) else {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    };
    let name = &rest[..name_end];
    let value = &rest[name_end + 1..];
    if value.len() < input.size as usize {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    match vfs.set_xattr(
        header.nodeid,
        name,
        &value[..input.size as usize],
        input.flags,
    ) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, raw_xattr_errno(&err), Some(accounting)),
    }
}

fn raw_getxattr(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<GetxattrIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<GetxattrIn>(&payload[..size_of::<GetxattrIn>()])?;
    let name = payload[size_of::<GetxattrIn>()..]
        .strip_suffix(&[0])
        .unwrap_or(&payload[size_of::<GetxattrIn>()..]);
    match vfs.get_xattr(header.nodeid, name) {
        Ok(Some(value)) => reply_xattr_value(fd, header.unique, &value, input.size, accounting),
        Ok(None) => reply_error(fd, header.unique, libc::ENODATA, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, raw_xattr_errno(&err), Some(accounting)),
    }
}

fn raw_listxattr(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<GetxattrIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<GetxattrIn>(&payload[..size_of::<GetxattrIn>()])?;
    match vfs.list_xattrs(header.nodeid) {
        Ok(names) => {
            let data = encode_xattr_name_list(&names);
            reply_xattr_value(fd, header.unique, &data, input.size, accounting)
        }
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_removexattr(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    let name = payload.strip_suffix(&[0]).unwrap_or(payload);
    match vfs.remove_xattr(header.nodeid, name) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, raw_xattr_errno(&err), Some(accounting)),
    }
}

fn raw_ioctl(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    _context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if payload.len() < size_of::<IoctlIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<IoctlIn>(&payload[..size_of::<IoctlIn>()])?;
    let data = &payload[size_of::<IoctlIn>()..];
    match raw_fs_ioc_getflags_reply_data(input.cmd, data, input.out_size) {
        Ok(Some(out)) => reply_ioctl(fd, header.unique, 0, &out, Some(accounting)),
        Ok(None) => reply_error(fd, header.unique, libc::ENOTTY, Some(accounting)),
        Err(errno) => reply_error(fd, header.unique, errno, Some(accounting)),
    }
}

fn handle_init(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    profile: LinuxRawFuseProfile,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    if payload.len() < size_of::<InitIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, accounting);
    }
    let input = read_struct::<InitIn>(&payload[..size_of::<InitIn>()])?;
    if input.major != FUSE_KERNEL_VERSION {
        return reply_error(fd, header.unique, libc::EPROTO, accounting);
    }
    let negotiated = profile.negotiate_init(input);
    for cap in negotiated.degraded {
        warn!(
            cap,
            "raw FUSE kernel did not advertise requested capability; degrading"
        );
    }
    reply_struct(fd, header.unique, &negotiated.out, accounting)
}

#[derive(Clone, Copy)]
struct SessionIds {
    uid: libc::uid_t,
    gid: libc::gid_t,
}

#[derive(Clone)]
struct RawRequestContext {
    vfs: Option<Arc<StudioVfs>>,
    runtime_handle: Option<Handle>,
    ids: SessionIds,
}

fn reply_getattr(
    fd: RawFd,
    unique: u64,
    ino: u64,
    context: &RawRequestContext,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Attr {
        ino: u64,
        size: u64,
        blocks: u64,
        atime: u64,
        mtime: u64,
        ctime: u64,
        atimensec: u32,
        mtimensec: u32,
        ctimensec: u32,
        mode: u32,
        nlink: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
        blksize: u32,
        padding: u32,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct AttrOut {
        attr_valid: u64,
        attr_valid_nsec: u32,
        dummy: u32,
        attr: Attr,
    }
    let attr = if let Some(vfs) = &context.vfs {
        match vfs.getattr(ino) {
            Ok(attr) => {
                let bridge = to_bridge_attr(&attr, context.ids.uid, context.ids.gid, 4096);
                AttrOut {
                    attr_valid: 1,
                    attr_valid_nsec: 0,
                    dummy: 0,
                    attr: Attr {
                        ino: bridge.ino,
                        size: bridge.size,
                        blocks: bridge.blocks,
                        atime: system_time_secs(bridge.atime),
                        mtime: system_time_secs(bridge.mtime),
                        ctime: system_time_secs(bridge.ctime),
                        atimensec: system_time_nsecs(bridge.atime),
                        mtimensec: system_time_nsecs(bridge.mtime),
                        ctimensec: system_time_nsecs(bridge.ctime),
                        mode: raw_mode(&attr),
                        nlink: bridge.nlink,
                        uid: bridge.uid,
                        gid: bridge.gid,
                        rdev: bridge.rdev,
                        blksize: bridge.blksize,
                        padding: 0,
                    },
                }
            }
            Err(err) => return reply_error(fd, unique, vfs_err_to_errno(&err), accounting),
        }
    } else {
        AttrOut {
            attr_valid: 1,
            attr_valid_nsec: 0,
            dummy: 0,
            attr: Attr {
                ino: FUSE_ROOT_ID,
                size: 0,
                blocks: 0,
                atime: 0,
                mtime: 0,
                ctime: 0,
                atimensec: 0,
                mtimensec: 0,
                ctimensec: 0,
                mode: libc::S_IFDIR | 0o755,
                nlink: 2,
                uid: context.ids.uid,
                gid: context.ids.gid,
                rdev: 0,
                blksize: 4096,
                padding: 0,
            },
        }
    };
    // SAFETY: `AttrOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for AttrOut {}
    reply_struct(fd, unique, &attr, accounting)
}

fn reply_entry(
    fd: RawFd,
    unique: u64,
    inode_attr: &InodeAttr,
    context: &RawRequestContext,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct EntryOut {
        nodeid: u64,
        generation: u64,
        entry_valid: u64,
        attr_valid: u64,
        entry_valid_nsec: u32,
        attr_valid_nsec: u32,
        attr: EntryAttr,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct EntryAttr {
        ino: u64,
        size: u64,
        blocks: u64,
        atime: u64,
        mtime: u64,
        ctime: u64,
        atimensec: u32,
        mtimensec: u32,
        ctimensec: u32,
        mode: u32,
        nlink: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
        blksize: u32,
        padding: u32,
    }
    let bridge = to_bridge_attr(inode_attr, context.ids.uid, context.ids.gid, 4096);
    let out = EntryOut {
        nodeid: inode_attr.ino,
        generation: 0,
        entry_valid: 1,
        attr_valid: 1,
        entry_valid_nsec: 0,
        attr_valid_nsec: 0,
        attr: EntryAttr {
            ino: bridge.ino,
            size: bridge.size,
            blocks: bridge.blocks,
            atime: system_time_secs(bridge.atime),
            mtime: system_time_secs(bridge.mtime),
            ctime: system_time_secs(bridge.ctime),
            atimensec: system_time_nsecs(bridge.atime),
            mtimensec: system_time_nsecs(bridge.mtime),
            ctimensec: system_time_nsecs(bridge.ctime),
            mode: raw_mode(inode_attr),
            nlink: bridge.nlink,
            uid: bridge.uid,
            gid: bridge.gid,
            rdev: bridge.rdev,
            blksize: bridge.blksize,
            padding: 0,
        },
    };
    // SAFETY: `EntryOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for EntryOut {}
    reply_struct(fd, unique, &out, accounting)
}

fn reply_attr(
    fd: RawFd,
    unique: u64,
    inode_attr: &InodeAttr,
    context: &RawRequestContext,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Attr {
        ino: u64,
        size: u64,
        blocks: u64,
        atime: u64,
        mtime: u64,
        ctime: u64,
        atimensec: u32,
        mtimensec: u32,
        ctimensec: u32,
        mode: u32,
        nlink: u32,
        uid: u32,
        gid: u32,
        rdev: u32,
        blksize: u32,
        padding: u32,
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct AttrOut {
        attr_valid: u64,
        attr_valid_nsec: u32,
        dummy: u32,
        attr: Attr,
    }
    let bridge = to_bridge_attr(inode_attr, context.ids.uid, context.ids.gid, 4096);
    let out = AttrOut {
        attr_valid: 1,
        attr_valid_nsec: 0,
        dummy: 0,
        attr: Attr {
            ino: bridge.ino,
            size: bridge.size,
            blocks: bridge.blocks,
            atime: system_time_secs(bridge.atime),
            mtime: system_time_secs(bridge.mtime),
            ctime: system_time_secs(bridge.ctime),
            atimensec: system_time_nsecs(bridge.atime),
            mtimensec: system_time_nsecs(bridge.mtime),
            ctimensec: system_time_nsecs(bridge.ctime),
            mode: raw_mode(inode_attr),
            nlink: bridge.nlink,
            uid: bridge.uid,
            gid: bridge.gid,
            rdev: bridge.rdev,
            blksize: bridge.blksize,
            padding: 0,
        },
    };
    // SAFETY: `AttrOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for AttrOut {}
    reply_struct(fd, unique, &out, accounting)
}

fn reply_statfs(
    fd: RawFd,
    unique: u64,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Kstatfs {
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
        padding: u32,
        spare: [u32; 6],
    }
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct StatfsOut {
        st: Kstatfs,
    }
    // SAFETY: `StatfsOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for StatfsOut {}
    reply_struct(
        fd,
        unique,
        &StatfsOut {
            st: Kstatfs {
                blocks: 1,
                bfree: 1,
                bavail: 1,
                files: 1,
                ffree: 1,
                bsize: 4096,
                namelen: 255,
                frsize: 4096,
                padding: 0,
                spare: [0; 6],
            },
        },
        accounting,
    )
}

fn reply_open(
    fd: RawFd,
    unique: u64,
    fh: u64,
    open_flags: u32,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct OpenOut {
        fh: u64,
        open_flags: u32,
        padding: u32,
    }
    // SAFETY: `OpenOut` is `repr(C)`, integer-only, and has explicit padding.
    unsafe impl FuseBytes for OpenOut {}
    reply_struct(
        fd,
        unique,
        &OpenOut {
            fh,
            open_flags,
            padding: 0,
        },
        accounting,
    )
}

fn reply_data(
    fd: RawFd,
    unique: u64,
    data: &[u8],
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    let len = (size_of::<OutHeader>() + data.len()) as u32;
    let header = OutHeader {
        len,
        error: 0,
        unique,
    };
    write_fuse_packet_vectored_fd(fd, as_bytes(&header), data)?;
    if let Some(accounting) = accounting {
        accounting.record_reply_bytes(len as usize);
    }
    Ok(())
}

fn reply_write(
    fd: RawFd,
    unique: u64,
    written: u32,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct WriteOut {
        size: u32,
        padding: u32,
    }
    // SAFETY: `WriteOut` is `repr(C)`, integer-only, and has explicit padding.
    unsafe impl FuseBytes for WriteOut {}
    reply_struct(
        fd,
        unique,
        &WriteOut {
            size: written,
            padding: 0,
        },
        accounting,
    )
}

fn reply_xattr_value(
    fd: RawFd,
    unique: u64,
    data: &[u8],
    size: u32,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if size == 0 {
        let Ok(data_len) = u32::try_from(data.len()) else {
            return reply_error(fd, unique, libc::EOVERFLOW, Some(accounting));
        };
        return reply_xattr_size(fd, unique, data_len, Some(accounting));
    }
    if data.len() <= size as usize {
        reply_data(fd, unique, data, Some(accounting))
    } else {
        reply_error(fd, unique, libc::ERANGE, Some(accounting))
    }
}

fn reply_xattr_size(
    fd: RawFd,
    unique: u64,
    size: u32,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct GetxattrOut {
        size: u32,
        padding: u32,
    }
    // SAFETY: `GetxattrOut` is `repr(C)`, integer-only, and has explicit padding.
    unsafe impl FuseBytes for GetxattrOut {}
    reply_struct(fd, unique, &GetxattrOut { size, padding: 0 }, accounting)
}

fn reply_ioctl(
    fd: RawFd,
    unique: u64,
    result: i32,
    data: &[u8],
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct IoctlOut {
        result: i32,
        flags: u32,
        in_iovs: u32,
        out_iovs: u32,
    }
    // SAFETY: `IoctlOut` is `repr(C)`, integer-only, and has explicit fields.
    unsafe impl FuseBytes for IoctlOut {}
    let out = IoctlOut {
        result,
        flags: 0,
        in_iovs: 0,
        out_iovs: 0,
    };
    let header_len = size_of::<OutHeader>() + size_of::<IoctlOut>() + data.len();
    let header = OutHeader {
        len: header_len as u32,
        error: 0,
        unique,
    };
    let mut body = Vec::with_capacity(size_of::<IoctlOut>() + data.len());
    body.extend_from_slice(as_bytes(&out));
    body.extend_from_slice(data);
    write_fuse_packet_vectored_fd(fd, as_bytes(&header), &body)?;
    if let Some(accounting) = accounting {
        accounting.record_reply_bytes(header_len);
    }
    Ok(())
}

fn push_dirent(
    out: &mut Vec<u8>,
    ino: u64,
    offset: u64,
    dtype: u8,
    name: &[u8],
    limit: usize,
) -> bool {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Dirent {
        ino: u64,
        off: u64,
        namelen: u32,
        dtype: u32,
    }
    let Ok(namelen) = u32::try_from(name.len()) else {
        return false;
    };
    let entry_len = align_8(size_of::<Dirent>() + name.len());
    if out.len().saturating_add(entry_len) > limit {
        return false;
    }
    let dirent = Dirent {
        ino,
        off: offset,
        namelen,
        dtype: u32::from(dtype),
    };
    // SAFETY: `Dirent` is `repr(C)`, integer-only, and has no uninitialized padding.
    unsafe impl FuseBytes for Dirent {}
    out.extend_from_slice(as_bytes(&dirent));
    out.extend_from_slice(name);
    out.resize(
        out.len() + (entry_len - size_of::<Dirent>() - name.len()),
        0,
    );
    true
}

fn align_8(value: usize) -> usize {
    (value + 7) & !7
}

fn reply_empty(
    fd: RawFd,
    unique: u64,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    write_reply_header(fd, unique, 0, size_of::<OutHeader>() as u32, accounting)
}

fn reply_error(
    fd: RawFd,
    unique: u64,
    errno: i32,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    write_reply_header(
        fd,
        unique,
        -errno,
        size_of::<OutHeader>() as u32,
        accounting,
    )
}

fn reply_struct<T: FuseBytes>(
    fd: RawFd,
    unique: u64,
    value: &T,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    let bytes = as_bytes(value);
    let len = (size_of::<OutHeader>() + bytes.len()) as u32;
    let header = OutHeader {
        len,
        error: 0,
        unique,
    };
    write_fuse_packet_vectored_fd(fd, as_bytes(&header), bytes)?;
    if let Some(accounting) = accounting {
        accounting.record_reply_bytes(len as usize);
    }
    Ok(())
}

#[repr(C)]
#[derive(Clone, Copy)]
struct OutHeader {
    len: u32,
    error: i32,
    unique: u64,
}

fn write_reply_header(
    fd: RawFd,
    unique: u64,
    error: i32,
    len: u32,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    write_fuse_packet_fd(fd, as_bytes(&OutHeader { len, error, unique }))?;
    if let Some(accounting) = accounting {
        accounting.record_reply_bytes(len as usize);
    }
    Ok(())
}

fn read_fd(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        // SAFETY: `buf` is a valid writable byte slice for the provided length.
        let rc = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
        if rc >= 0 {
            return Ok(rc as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn write_fuse_packet_fd(fd: RawFd, bytes: &[u8]) -> io::Result<()> {
    loop {
        // SAFETY: `bytes` is a valid readable byte slice for the provided length.
        let rc = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if rc >= 0 {
            return check_fuse_packet_write(bytes.len(), rc as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn write_fuse_packet_vectored_fd(fd: RawFd, first: &[u8], second: &[u8]) -> io::Result<()> {
    let iov = [
        libc::iovec {
            iov_base: first.as_ptr().cast_mut().cast(),
            iov_len: first.len(),
        },
        libc::iovec {
            iov_base: second.as_ptr().cast_mut().cast(),
            iov_len: second.len(),
        },
    ];
    let expected = first.len() + second.len();
    loop {
        // SAFETY: both iovec entries point to readable byte slices that stay
        // alive for the duration of this single `writev(2)` call.
        let rc = unsafe { libc::writev(fd, iov.as_ptr(), iov.len() as libc::c_int) };
        if rc >= 0 {
            return check_fuse_packet_write(expected, rc as usize);
        }
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::Interrupted {
            return Err(err);
        }
    }
}

fn check_fuse_packet_write(expected: usize, written: usize) -> io::Result<()> {
    if written == expected {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::WriteZero,
            "short FUSE packet write",
        ))
    }
}

fn raw_mode(attr: &InodeAttr) -> u32 {
    let kind = match attr.kind {
        FileType::RegularFile => libc::S_IFREG,
        FileType::Directory => libc::S_IFDIR,
        FileType::Symlink => libc::S_IFLNK,
    };
    kind | u32::from(attr.mode & 0o7777)
}

fn setattr_args_from_raw(input: SetattrIn) -> SetattrArgs {
    SetattrArgs {
        size: (input.valid & FATTR_SIZE != 0).then_some(input.size),
        mode: (input.valid & FATTR_MODE != 0).then_some((input.mode & 0o7777) as u16),
        uid: (input.valid & FATTR_UID != 0).then_some(input.uid),
        gid: (input.valid & FATTR_GID != 0).then_some(input.gid),
        atime: raw_time_spec(
            input.valid,
            FATTR_ATIME,
            FATTR_ATIME_NOW,
            input.atime,
            input.atimensec,
        ),
        mtime: raw_time_spec(
            input.valid,
            FATTR_MTIME,
            FATTR_MTIME_NOW,
            input.mtime,
            input.mtimensec,
        ),
        crtime: TimeSpec::Omit,
        win_attrs: None,
    }
}

fn raw_time_spec(valid: u32, field: u32, now_field: u32, secs: u64, nsecs: u32) -> TimeSpec {
    if valid & field == 0 {
        TimeSpec::Omit
    } else if valid & now_field != 0 {
        TimeSpec::Now
    } else {
        let nanos = (u128::from(secs) * 1_000_000_000).saturating_add(u128::from(nsecs));
        TimeSpec::Set(nanos.min(i64::MAX as u128) as i64)
    }
}

fn encode_xattr_name_list(names: &[Vec<u8>]) -> Vec<u8> {
    names
        .iter()
        .flat_map(|name| name.iter().copied().chain(std::iter::once(0)))
        .collect()
}

fn raw_xattr_errno(err: &studiofs_vfs::VfsError) -> i32 {
    if matches!(
        err,
        studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    ) {
        libc::ENODATA
    } else {
        vfs_err_to_errno(err)
    }
}

fn raw_fs_ioc_getflags_reply_data(
    cmd: u32,
    in_data: &[u8],
    out_size: u32,
) -> Result<Option<Vec<u8>>, i32> {
    let is_getflags = cmd == libc::FS_IOC_GETFLAGS as u32 || cmd == libc::FS_IOC32_GETFLAGS as u32;
    if !is_getflags {
        return Ok(None);
    }
    if !in_data.is_empty() || out_size < u32::BITS / 8 {
        return Err(libc::EINVAL);
    }
    Ok(Some(vec![0; out_size as usize]))
}

fn system_time_secs(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn system_time_nsecs(time: SystemTime) -> u32 {
    time.duration_since(UNIX_EPOCH)
        .map(|duration| duration.subsec_nanos())
        .unwrap_or(0)
}

fn cstring_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

struct RawMountGuard<'a>(&'a Path);

impl Drop for RawMountGuard<'_> {
    fn drop(&mut self) {
        let _ = unmount_raw(self.0);
    }
}

fn unmount_raw(mount_point: &Path) -> io::Result<()> {
    let mount_point_c = cstring_path(mount_point)?;
    // SAFETY: `mount_point` is a valid NUL-terminated C string.
    let rc = unsafe { libc::umount2(mount_point_c.as_ptr(), libc::MNT_DETACH) };
    if rc == 0 {
        Ok(())
    } else {
        let err = io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(libc::EINVAL | libc::ENOENT)) {
            Err(err)
        } else {
            unmount_raw_with_fusermount(mount_point)
        }
    }
}

fn unmount_raw_with_fusermount(mount_point: &Path) -> io::Result<()> {
    let bin = fusermount_binary()?;
    let status = Command::new(bin)
        .arg("-u")
        .arg(mount_point)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{bin} -u exited with status {status}"
        )))
    }
}

fn read_struct<T: FuseBytes>(bytes: &[u8]) -> io::Result<T> {
    if bytes.len() < size_of::<T>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "short FUSE struct",
        ));
    }
    // SAFETY: callers pass at least `size_of::<T>()` bytes; unaligned reads are
    // accepted because FUSE payload alignment is not trusted at this boundary.
    Ok(unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast()) })
}

fn as_bytes<T: FuseBytes>(value: &T) -> &[u8] {
    // SAFETY: `value` is valid for `size_of::<T>()` bytes and the returned slice
    // is tied to `value`'s lifetime.
    unsafe { std::slice::from_raw_parts((value as *const T).cast(), size_of::<T>()) }
}

unsafe trait FuseBytes: Copy {}

// SAFETY: these Linux FUSE wire structs are `repr(C)`, integer-only, and any
// protocol padding is represented as initialized fields.
unsafe impl FuseBytes for InHeader {}
unsafe impl FuseBytes for InitIn {}
unsafe impl FuseBytes for InitOut {}
unsafe impl FuseBytes for OpenIn {}
unsafe impl FuseBytes for ReadIn {}
unsafe impl FuseBytes for WriteIn {}
unsafe impl FuseBytes for FlushIn {}
unsafe impl FuseBytes for FsyncIn {}
unsafe impl FuseBytes for ReleaseIn {}
unsafe impl FuseBytes for SetattrIn {}
unsafe impl FuseBytes for SetxattrIn {}
unsafe impl FuseBytes for GetxattrIn {}
unsafe impl FuseBytes for IoctlIn {}
unsafe impl FuseBytes for OutHeader {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_dispatch_classifies_stream_heavy_operations() {
        for opcode in [OP_READ, OP_WRITE, OP_FLUSH, OP_FSYNC, OP_RELEASE] {
            assert_eq!(
                RawDispatchClass::for_opcode(opcode),
                RawDispatchClass::Stream
            );
        }
    }

    #[test]
    fn raw_dispatch_classifies_metadata_operations() {
        for opcode in [OP_LOOKUP, OP_GETATTR, OP_READDIR, OP_SETATTR, OP_GETXATTR] {
            assert_eq!(
                RawDispatchClass::for_opcode(opcode),
                RawDispatchClass::Metadata
            );
        }
    }

    #[test]
    fn raw_dispatch_accounting_limits_stream_heavy_work_without_blocking_metadata() {
        let accounting = RawDispatchAccounting::new(8, 1);

        let stream = accounting.try_admit(RawDispatchClass::Stream).unwrap();
        assert_eq!(accounting.snapshot().inflight_stream, 1);
        assert_eq!(
            accounting.try_admit(RawDispatchClass::Stream).unwrap_err(),
            RawDispatchAdmissionError::StreamQueueFull
        );

        let metadata = accounting.try_admit(RawDispatchClass::Metadata).unwrap();
        assert_eq!(accounting.snapshot().inflight_total, 2);

        drop(stream);
        assert_eq!(accounting.snapshot().inflight_stream, 0);
        drop(metadata);
        assert_eq!(accounting.snapshot().inflight_total, 0);
    }

    #[test]
    fn raw_dispatch_accounting_records_request_reply_and_io_latency_counters() {
        let accounting = RawDispatchAccounting::new(8, 2);

        accounting.record_request();
        accounting.record_reply_bytes(24);
        accounting.record_reply_bytes(8);
        accounting.record_latency(OP_READ, Duration::from_nanos(7));
        accounting.record_latency(OP_WRITE, Duration::from_nanos(11));
        accounting.record_latency(OP_LOOKUP, Duration::from_nanos(13));

        let snapshot = accounting.snapshot();
        assert_eq!(snapshot.request_count, 1);
        assert_eq!(snapshot.reply_bytes, 32);
        assert_eq!(snapshot.read_latency_count, 1);
        assert_eq!(snapshot.read_latency_nanos, 7);
        assert_eq!(snapshot.write_latency_count, 1);
        assert_eq!(snapshot.write_latency_nanos, 11);
    }

    #[test]
    fn raw_fusermount_options_match_direct_mount_identity() {
        let options = fusermount_options("studio", true, LinuxRawFuseProfile::juicefs_like());

        assert_eq!(
            options,
            "fsname=studio,subtype=studiofs,max_read=1048576,ro"
        );
    }

    #[test]
    fn raw_xattr_name_list_uses_null_terminated_names() {
        let data = encode_xattr_name_list(&[b"user.one".to_vec(), b"user.two".to_vec()]);

        assert_eq!(data, b"user.one\0user.two\0");
    }

    #[test]
    fn raw_ioctl_getflags_reply_matches_requested_output_size() {
        let data = raw_fs_ioc_getflags_reply_data(libc::FS_IOC_GETFLAGS as u32, &[], 8)
            .expect("getflags should be valid")
            .expect("getflags should return data");

        assert_eq!(data, vec![0; 8]);
    }

    #[test]
    fn raw_ioctl_getflags_rejects_input_payload() {
        assert_eq!(
            raw_fs_ioc_getflags_reply_data(libc::FS_IOC_GETFLAGS as u32, b"x", 4).unwrap_err(),
            libc::EINVAL
        );
    }
}
