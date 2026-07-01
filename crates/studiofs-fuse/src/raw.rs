use std::ffi::CString;
use std::fs::OpenOptions;
use std::io;
use std::mem::{self, size_of};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_MINOR_VERSION: u32 = 38;
const FUSE_ROOT_ID: u64 = 1;

const OP_LOOKUP: u32 = 1;
const OP_FORGET: u32 = 2;
const OP_GETATTR: u32 = 3;
const OP_STATFS: u32 = 17;
const OP_INIT: u32 = 26;
const OP_OPENDIR: u32 = 27;
const OP_READDIR: u32 = 28;
const OP_RELEASEDIR: u32 = 29;
const OP_ACCESS: u32 = 34;
const OP_DESTROY: u32 = 38;
const OP_READDIRPLUS: u32 = 44;

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
}

impl RawFuseMountRequest {
    /// Create a raw FUSE mount request with JuiceFS-like Linux defaults.
    pub fn new(mount_point: impl Into<String>, volume_name: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            profile: LinuxRawFuseProfile::juicefs_like(),
        }
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

/// Mount and serve a minimal Linux raw FUSE session.
pub fn mount_raw_with_request(request: RawFuseMountRequest) -> io::Result<()> {
    let RawFuseMountRequest {
        mount_point,
        volume_name,
        read_only,
        profile,
    } = request;

    let mount_point = PathBuf::from(mount_point);
    let dev_fuse = OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/fuse")?;
    mount_dev_fuse(
        dev_fuse.as_raw_fd(),
        &mount_point,
        &volume_name,
        read_only,
        profile,
    )?;
    info!(mount_point = %mount_point.display(), "mounted StudioFS raw FUSE session");
    let result = serve_minimal_session(dev_fuse.as_raw_fd(), profile);
    let _ = unmount_raw(&mount_point);
    result
}

fn mount_dev_fuse(
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
    let fstype = CString::new("fuse.studiofs").expect("static string has no NUL");
    let st = stat_path(mount_point)?;
    let data = CString::new(format!(
        "fd={fd},rootmode={:o},user_id={},group_id={},max_read={}",
        st.st_mode & libc::S_IFMT,
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

fn serve_minimal_session(fd: RawFd, profile: LinuxRawFuseProfile) -> io::Result<()> {
    let mut buf = vec![0_u8; profile.max_write as usize + 4096];
    let ids = SessionIds {
        // SAFETY: `geteuid` has no preconditions and does not dereference pointers.
        uid: unsafe { libc::geteuid() },
        // SAFETY: `getegid` has no preconditions and does not dereference pointers.
        gid: unsafe { libc::getegid() },
    };
    loop {
        let len = match read_fd(fd, &mut buf) {
            Ok(len) => len,
            Err(err)
                if matches!(
                    err.raw_os_error(),
                    Some(libc::ENODEV | libc::EIO | libc::ENOENT)
                ) =>
            {
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if len == 0 {
            return Ok(());
        }
        if len < size_of::<InHeader>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "short FUSE request header",
            ));
        }
        let header = read_struct::<InHeader>(&buf[..size_of::<InHeader>()])?;
        match header.opcode {
            OP_INIT => handle_init(fd, header, &buf[size_of::<InHeader>()..len], profile)?,
            OP_GETATTR => reply_getattr(fd, header.unique, ids)?,
            OP_STATFS => reply_statfs(fd, header.unique)?,
            OP_OPENDIR => reply_open(fd, header.unique)?,
            OP_READDIR | OP_READDIRPLUS => reply_empty(fd, header.unique)?,
            OP_ACCESS | OP_RELEASEDIR => reply_empty(fd, header.unique)?,
            OP_LOOKUP => reply_error(fd, header.unique, libc::ENOENT)?,
            OP_FORGET | OP_DESTROY => {}
            opcode => {
                debug!(
                    opcode,
                    "raw FUSE minimal backend does not implement request"
                );
                reply_error(fd, header.unique, libc::ENOSYS)?;
            }
        }
    }
}

fn handle_init(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    profile: LinuxRawFuseProfile,
) -> io::Result<()> {
    if payload.len() < size_of::<InitIn>() {
        return reply_error(fd, header.unique, libc::EINVAL);
    }
    let input = read_struct::<InitIn>(&payload[..size_of::<InitIn>()])?;
    if input.major != FUSE_KERNEL_VERSION {
        return reply_error(fd, header.unique, libc::EPROTO);
    }
    let negotiated = profile.negotiate_init(input);
    for cap in negotiated.degraded {
        warn!(
            cap,
            "raw FUSE kernel did not advertise requested capability; degrading"
        );
    }
    reply_struct(fd, header.unique, &negotiated.out)
}

#[derive(Clone, Copy)]
struct SessionIds {
    uid: libc::uid_t,
    gid: libc::gid_t,
}

fn reply_getattr(fd: RawFd, unique: u64, ids: SessionIds) -> io::Result<()> {
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
    let attr = AttrOut {
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
            uid: ids.uid,
            gid: ids.gid,
            rdev: 0,
            blksize: 4096,
            padding: 0,
        },
    };
    // SAFETY: `AttrOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for AttrOut {}
    reply_struct(fd, unique, &attr)
}

fn reply_statfs(fd: RawFd, unique: u64) -> io::Result<()> {
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
    )
}

fn reply_open(fd: RawFd, unique: u64) -> io::Result<()> {
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
            fh: 1,
            open_flags: 0,
            padding: 0,
        },
    )
}

fn reply_empty(fd: RawFd, unique: u64) -> io::Result<()> {
    write_reply_header(fd, unique, 0, size_of::<OutHeader>() as u32)
}

fn reply_error(fd: RawFd, unique: u64, errno: i32) -> io::Result<()> {
    write_reply_header(fd, unique, -errno, size_of::<OutHeader>() as u32)
}

fn reply_struct<T: FuseBytes>(fd: RawFd, unique: u64, value: &T) -> io::Result<()> {
    let bytes = as_bytes(value);
    let len = (size_of::<OutHeader>() + bytes.len()) as u32;
    let header = OutHeader {
        len,
        error: 0,
        unique,
    };
    let mut reply = Vec::with_capacity(len as usize);
    reply.extend_from_slice(as_bytes(&header));
    reply.extend_from_slice(bytes);
    write_all_fd(fd, &reply)
}

#[repr(C)]
#[derive(Clone, Copy)]
struct OutHeader {
    len: u32,
    error: i32,
    unique: u64,
}

fn write_reply_header(fd: RawFd, unique: u64, error: i32, len: u32) -> io::Result<()> {
    write_all_fd(fd, as_bytes(&OutHeader { len, error, unique }))
}

fn read_fd(fd: RawFd, buf: &mut [u8]) -> io::Result<usize> {
    // SAFETY: `buf` is a valid writable byte slice for the provided length.
    let rc = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), buf.len()) };
    if rc >= 0 {
        Ok(rc as usize)
    } else {
        Err(io::Error::last_os_error())
    }
}

fn write_all_fd(fd: RawFd, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        // SAFETY: `bytes` is a valid readable byte slice for the provided length.
        let rc = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        bytes = &bytes[rc as usize..];
    }
    Ok(())
}

fn stat_path(path: &Path) -> io::Result<libc::stat> {
    let path = cstring_path(path)?;
    let mut st = mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: `path` is a valid NUL-terminated C string and `st` points to
    // writable storage for `stat(2)`.
    let rc = unsafe { libc::stat(path.as_ptr(), st.as_mut_ptr()) };
    if rc == 0 {
        // SAFETY: successful `stat(2)` initialized `st`.
        Ok(unsafe { st.assume_init() })
    } else {
        Err(io::Error::last_os_error())
    }
}

fn cstring_path(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

fn unmount_raw(mount_point: &Path) -> io::Result<()> {
    let mount_point = cstring_path(mount_point)?;
    // SAFETY: `mount_point` is a valid NUL-terminated C string.
    let rc = unsafe { libc::umount2(mount_point.as_ptr(), libc::MNT_DETACH) };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
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
unsafe impl FuseBytes for OutHeader {}
