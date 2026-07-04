use std::cell::RefCell;
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io;
use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{SignalHandling, signal_handlers_for_policy};
use bytes::Bytes;
use dashmap::DashMap;
use studiofs_bridge::posix::{posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{
    DirEntryWithAttr, KernelInvalidator, LocalSyncPolicy, OpenFlags, StudioVfs,
    VfsFuseAsyncWriteSnapshot,
};
use tokio::runtime::Handle;
use tracing::{debug, info, warn};

thread_local! {
    static RAW_READ_BUFFER: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

const FUSE_KERNEL_VERSION: u32 = 7;
const FUSE_MINOR_VERSION: u32 = 38;
const FUSE_ROOT_ID: u64 = 1;
const READDIR_REPLY_INITIAL_CAPACITY: usize = 4096;
const JFS_LIKE_BUFFER_BUDGET_BYTES: u64 = 800 * 1024 * 1024;
const JFS_LIKE_READ_BUFFER_BUDGET_BYTES: u64 = JFS_LIKE_BUFFER_BUDGET_BYTES / 2;
const JFS_LIKE_WRITE_BUFFER_BUDGET_BYTES: u64 = JFS_LIKE_BUFFER_BUDGET_BYTES / 2;
const JFS_LIKE_MAX_BACKGROUND: u16 = 200;
const JFS_LIKE_CONGESTION_THRESHOLD: u16 = 150;
const JFS_LIKE_PREFETCH_CONCURRENCY: u16 = 1;
const JFS_LIKE_UPLOAD_CONCURRENCY: u16 = 20;
const JFS_LIKE_DOWNLOAD_CONCURRENCY: u16 = 200;
const JFS_LIKE_CACHE_SIZE_POLICY: &str = "operator-cache-budget";
const RAW_SIGNAL_POLL_INTERVAL: Duration = Duration::from_millis(100);
const RAW_INVALIDATION_QUEUE_CAPACITY: usize = 4096;

const OP_LOOKUP: u32 = 1;
const OP_FORGET: u32 = 2;
const OP_GETATTR: u32 = 3;
const OP_SETATTR: u32 = 4;
const OP_READLINK: u32 = 5;
const OP_SYMLINK: u32 = 6;
const OP_MKNOD: u32 = 8;
const OP_MKDIR: u32 = 9;
const OP_UNLINK: u32 = 10;
const OP_RMDIR: u32 = 11;
const OP_RENAME: u32 = 12;
const OP_LINK: u32 = 13;
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

const FUSE_FSYNC_FDATASYNC: u32 = 1;
const FUSE_NOTIFY_INVAL_INODE: i32 = 2;
const FOPEN_KEEP_CACHE: u32 = 1 << 1;
const LINUX_DEFAULT_IO_SIZE: u32 = 4096;
const LINUX_REGULAR_FILE_IO_SIZE: u32 = 4 * 1024 * 1024;

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
pub const FUSE_WRITEBACK_CACHE: u32 = 1 << 16;
pub const FUSE_PARALLEL_DIROPS: u32 = 1 << 18;
pub const FUSE_MAX_PAGES: u32 = 1 << 22;
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 1 << 25;

const WANTED_CAPS: [u32; 8] = [
    FUSE_ASYNC_READ,
    FUSE_BIG_WRITES,
    FUSE_MAX_PAGES,
    FUSE_PARALLEL_DIROPS,
    FUSE_READDIRPLUS,
    FUSE_READDIRPLUS_AUTO,
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
    /// Process signal handling policy for foreground CLI mounts.
    pub signal_handling: SignalHandling,
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
            signal_handling: SignalHandling::default(),
        }
    }

    /// Attach the VFS instance and Tokio runtime used by raw FUSE callbacks.
    pub fn with_vfs(mut self, vfs: Arc<StudioVfs>, runtime_handle: Handle) -> Self {
        self.vfs = Some(vfs);
        self.runtime_handle = Some(runtime_handle);
        self
    }

    /// Set process signal ownership policy.
    pub fn with_signal_handling(mut self, signal_handling: SignalHandling) -> Self {
        self.signal_handling = signal_handling;
        self
    }
}

fn raw_shutdown_requested() -> bool {
    crate::SHUTDOWN_REQUESTED.load(Ordering::Acquire)
}

fn raw_shutdown_never_requested() -> bool {
    false
}

/// One Linux FUSE profile used for mount options and INIT negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LinuxRawFuseProfile {
    /// Requested FUSE INIT `max_write`.
    pub max_write: u32,
    /// Requested mount `max_read`.
    pub max_read: u32,
    /// Requested FUSE INIT max pages for kernels that advertise `FUSE_MAX_PAGES`.
    pub max_pages: u16,
    /// Requested FUSE INIT background queue depth.
    pub max_background: u16,
    /// Requested FUSE INIT congestion threshold.
    pub congestion_threshold: u16,
    /// Requested FUSE INIT kernel readahead.
    pub max_readahead: u32,
    /// Read-side userspace buffer budget represented by this profile.
    pub read_buffer_budget_bytes: u64,
    /// Write-side userspace buffer budget represented by this profile.
    pub write_buffer_budget_bytes: u64,
    /// Number of read-ahead fetches to run in parallel.
    pub prefetch_concurrency: u16,
    /// Number of object uploads to run in parallel.
    pub upload_concurrency: u16,
    /// Number of object downloads to run in parallel.
    pub download_concurrency: u16,
    /// Cache sizing source for this profile.
    pub cache_size_policy: &'static str,
    /// Whether to request kernel writeback cache when the kernel advertises it.
    pub kernel_writeback_cache: bool,
}

impl LinuxRawFuseProfile {
    pub fn label(&self) -> &'static str {
        "raw-juicefs-like"
    }

    pub fn juicefs_like() -> Self {
        let max_write = 1024 * 1024;
        Self {
            max_write,
            max_read: max_write,
            max_pages: (max_write / 4096) as u16,
            max_background: JFS_LIKE_MAX_BACKGROUND,
            congestion_threshold: JFS_LIKE_CONGESTION_THRESHOLD,
            max_readahead: 1024 * 1024,
            read_buffer_budget_bytes: JFS_LIKE_READ_BUFFER_BUDGET_BYTES,
            write_buffer_budget_bytes: JFS_LIKE_WRITE_BUFFER_BUDGET_BYTES,
            prefetch_concurrency: JFS_LIKE_PREFETCH_CONCURRENCY,
            upload_concurrency: JFS_LIKE_UPLOAD_CONCURRENCY,
            download_concurrency: JFS_LIKE_DOWNLOAD_CONCURRENCY,
            cache_size_policy: JFS_LIKE_CACHE_SIZE_POLICY,
            kernel_writeback_cache: true,
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
        if flags & FUSE_EXPLICIT_INVAL_DATA == 0 && input.flags & FUSE_AUTO_INVAL_DATA != 0 {
            flags |= FUSE_AUTO_INVAL_DATA;
        }
        if self.kernel_writeback_cache && input.flags & FUSE_WRITEBACK_CACHE != 0 {
            flags |= FUSE_WRITEBACK_CACHE;
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
            OP_LOOKUP | OP_GETATTR | OP_SETATTR | OP_READLINK | OP_SYMLINK | OP_MKNOD
            | OP_MKDIR | OP_UNLINK | OP_RMDIR | OP_RENAME | OP_LINK | OP_STATFS | OP_OPENDIR
            | OP_READDIR | OP_RELEASEDIR | OP_ACCESS | OP_CREATE | OP_SETXATTR | OP_GETXATTR
            | OP_LISTXATTR | OP_REMOVEXATTR | OP_IOCTL | OP_READDIRPLUS | OP_OPEN => Self::Metadata,
            _ => Self::Metadata,
        }
    }
}

#[cfg(test)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawCreateEntryKind {
    File,
    Directory,
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
    idle_lock: Mutex<()>,
    idle_changed: Condvar,
    request_count: AtomicU64,
    reply_bytes: AtomicU64,
    read_latency_count: AtomicU64,
    read_latency_nanos: AtomicU64,
    write_latency_count: AtomicU64,
    write_latency_nanos: AtomicU64,
}

#[derive(Debug)]
struct RawBufferSpilloverAccounting {
    max_inflight: usize,
    inflight: AtomicUsize,
}

impl RawBufferSpilloverAccounting {
    fn new(max_inflight: usize) -> Self {
        Self {
            max_inflight: max_inflight.max(1),
            inflight: AtomicUsize::new(0),
        }
    }

    fn try_acquire(self: &Arc<Self>) -> Option<RawBufferSpilloverGuard> {
        increment_bounded(&self.inflight, self.max_inflight).ok()?;
        Some(RawBufferSpilloverGuard {
            accounting: Arc::clone(self),
        })
    }
}

#[derive(Debug)]
struct RawBufferSpilloverGuard {
    accounting: Arc<RawBufferSpilloverAccounting>,
}

impl Drop for RawBufferSpilloverGuard {
    fn drop(&mut self) {
        self.accounting.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RawDispatchAccounting {
    fn new(max_inflight: usize, max_stream_inflight: usize) -> Self {
        Self {
            inner: Arc::new(RawDispatchAccountingInner {
                max_inflight: max_inflight.max(1),
                max_stream_inflight: max_stream_inflight.max(1),
                inflight_total: AtomicUsize::new(0),
                inflight_stream: AtomicUsize::new(0),
                idle_lock: Mutex::new(()),
                idle_changed: Condvar::new(),
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

    fn admit_blocking(&self, class: RawDispatchClass) -> RawDispatchAdmissionGuard {
        let mut wait = self
            .inner
            .idle_lock
            .lock()
            .expect("raw dispatch lock poisoned");
        loop {
            match self.try_admit(class) {
                Ok(guard) => return guard,
                Err(
                    RawDispatchAdmissionError::QueueFull
                    | RawDispatchAdmissionError::StreamQueueFull,
                ) => {
                    wait = self
                        .inner
                        .idle_changed
                        .wait(wait)
                        .expect("raw dispatch lock poisoned");
                }
            }
        }
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

    #[cfg(test)]
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

    fn wait_for_idle(&self) {
        let mut wait = self
            .inner
            .idle_lock
            .lock()
            .expect("raw dispatch lock poisoned");
        while self.inner.inflight_total.load(Ordering::Acquire) != 0 {
            wait = self
                .inner
                .idle_changed
                .wait(wait)
                .expect("raw dispatch lock poisoned");
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
        self.accounting.inner.idle_changed.notify_all();
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
struct MknodIn {
    mode: u32,
    rdev: u32,
    umask: u32,
    padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MkdirIn {
    mode: u32,
    umask: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RenameIn {
    newdir: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct LinkIn {
    oldnodeid: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct CreateIn {
    flags: u32,
    mode: u32,
    umask: u32,
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
        signal_handling,
    } = request;

    let mount_point = PathBuf::from(mount_point);
    let dev_fuse = mount_dev_fuse(&mount_point, &volume_name, read_only, profile)?;
    info!(
        mount_point = %mount_point.display(),
        max_write = profile.max_write,
        max_read = profile.max_read,
        max_readahead = profile.max_readahead,
        max_background = profile.max_background,
        congestion_threshold = profile.congestion_threshold,
        read_buffer_budget_bytes = profile.read_buffer_budget_bytes,
        write_buffer_budget_bytes = profile.write_buffer_budget_bytes,
        prefetch_concurrency = profile.prefetch_concurrency,
        upload_concurrency = profile.upload_concurrency,
        download_concurrency = profile.download_concurrency,
        cache_size_policy = profile.cache_size_policy,
        kernel_writeback_cache = profile.kernel_writeback_cache,
        "mounted StudioFS raw FUSE session"
    );
    if let Some(vfs) = vfs.as_ref() {
        publish_raw_profile_diagnostics(vfs, &profile);
    }
    crate::SHUTDOWN_REQUESTED.store(false, Ordering::Release);
    let _signal_handlers = signal_handlers_for_policy(signal_handling)?;
    let shutdown = RawSessionShutdown::for_signal_handling(signal_handling);
    let _mount_guard = RawMountGuard(&mount_point);
    let _kernel_invalidator = match vfs.as_ref() {
        Some(vfs) => Some(RawKernelInvalidatorRegistration::new(
            Arc::clone(vfs),
            dev_fuse.as_raw_fd(),
        )?),
        None => None,
    };
    serve_minimal_session(dev_fuse.as_raw_fd(), profile, vfs, runtime_handle, shutdown)
}

fn publish_raw_profile_diagnostics(vfs: &StudioVfs, profile: &LinuxRawFuseProfile) {
    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        enabled: true,
        linux_fuse_profile: profile.label().into(),
        fuse_max_write_bytes: profile.max_write,
        fuse_max_readahead_bytes: profile.max_readahead,
        fuse_max_background: Some(profile.max_background),
        fuse_congestion_threshold: Some(profile.congestion_threshold),
        read_buffer_budget_bytes: Some(profile.read_buffer_budget_bytes),
        write_buffer_budget_bytes: Some(profile.write_buffer_budget_bytes),
        prefetch_concurrency: Some(profile.prefetch_concurrency),
        upload_concurrency: Some(profile.upload_concurrency),
        download_concurrency: Some(profile.download_concurrency),
        kernel_writeback_cache: Some(profile.kernel_writeback_cache),
        ..Default::default()
    });
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
    let rc = unsafe {
        libc::socketpair(
            libc::AF_UNIX,
            libc::SOCK_SEQPACKET | libc::SOCK_CLOEXEC,
            0,
            fds.as_mut_ptr(),
        )
    };
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
    let rc = unsafe { libc::recvmsg(socket.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
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
    let min_cmsg_len = unsafe { libc::CMSG_LEN(size_of::<RawFd>() as libc::c_uint) } as usize;
    if unsafe { (*cmsg).cmsg_len } < min_cmsg_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "fusermount returned truncated control message",
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
    shutdown: RawSessionShutdown,
) -> io::Result<()> {
    let buffer_size = profile.max_write as usize + 4096;
    let buffer_count = usize::from(profile.max_background.max(1));
    let (buffer_recycler, recycled_buffers) = mpsc::sync_channel(buffer_count);
    let spillover_accounting = Arc::new(RawBufferSpilloverAccounting::new(buffer_count));
    for _ in 0..buffer_count {
        buffer_recycler
            .send(vec![0_u8; buffer_size])
            .map_err(|_| io::Error::other("raw FUSE buffer pool closed"))?;
    }
    let mut buf = recv_request_buffer(&recycled_buffers, buffer_size, &spillover_accounting)?;
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
        max_write: profile.max_write as usize,
        ids,
        next_open_dir_fh: Arc::new(AtomicU64::new(1)),
        open_dir_handles: Arc::new(DashMap::new()),
        write_sync_policies: Arc::new(DashMap::new()),
    };
    let session_result = loop {
        let len = match read_next_fuse_request(fd, buf.as_mut_slice(), shutdown) {
            Ok(Some(len)) => len,
            Ok(None) => break Ok(()),
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
        let header = read_struct::<InHeader>(&buf.as_slice()[..size_of::<InHeader>()])?;
        accounting.record_request();
        if header.opcode == OP_INIT {
            handle_init(
                fd,
                header,
                &buf.as_slice()[size_of::<InHeader>()..len],
                profile,
                Some(&accounting),
            )?;
            continue;
        }
        let request_buf = std::mem::replace(
            &mut buf,
            recv_request_buffer(&recycled_buffers, buffer_size, &spillover_accounting)?,
        );
        dispatch_minimal_request(
            fd,
            header,
            request_buf,
            len,
            context.clone(),
            &accounting,
            buffer_recycler.clone(),
        )?;
    };
    accounting.wait_for_idle();
    session_result
}

struct RawRequestBuffer {
    buffer: Vec<u8>,
    _spillover: Option<RawBufferSpilloverGuard>,
}

impl RawRequestBuffer {
    fn pooled(buffer: Vec<u8>) -> Self {
        Self {
            buffer,
            _spillover: None,
        }
    }

    fn spillover(buffer_size: usize, guard: RawBufferSpilloverGuard) -> Self {
        Self {
            buffer: vec![0_u8; buffer_size],
            _spillover: Some(guard),
        }
    }

    fn as_slice(&self) -> &[u8] {
        &self.buffer
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buffer
    }

    fn into_parts(self) -> (Vec<u8>, Option<RawBufferSpilloverGuard>) {
        (self.buffer, self._spillover)
    }
}

fn recv_request_buffer(
    receiver: &Receiver<Vec<u8>>,
    buffer_size: usize,
    spillover_accounting: &Arc<RawBufferSpilloverAccounting>,
) -> io::Result<RawRequestBuffer> {
    match receiver.try_recv() {
        Ok(buffer) => Ok(RawRequestBuffer::pooled(buffer)),
        Err(TryRecvError::Empty) => match spillover_accounting.try_acquire() {
            Some(guard) => {
                debug!(
                    buffer_size,
                    "raw FUSE request buffer pool empty; allocating spillover buffer"
                );
                Ok(RawRequestBuffer::spillover(buffer_size, guard))
            }
            None => receiver
                .recv()
                .map(RawRequestBuffer::pooled)
                .map_err(|_| io::Error::other("raw FUSE buffer pool closed")),
        },
        Err(TryRecvError::Disconnected) => Err(io::Error::other("raw FUSE buffer pool closed")),
    }
}

struct RecycledRequestBuffer {
    buffer: Option<Vec<u8>>,
    _spillover: Option<RawBufferSpilloverGuard>,
    recycler: SyncSender<Vec<u8>>,
}

impl RecycledRequestBuffer {
    fn new(buffer: RawRequestBuffer, recycler: SyncSender<Vec<u8>>) -> Self {
        let (buffer, spillover) = buffer.into_parts();
        Self {
            buffer: Some(buffer),
            _spillover: spillover,
            recycler,
        }
    }

    fn payload(&self, request_len: usize) -> &[u8] {
        &self.buffer.as_ref().expect("request buffer present")[size_of::<InHeader>()..request_len]
    }
}

impl Drop for RecycledRequestBuffer {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if self.recycler.try_send(buffer).is_err() {
            debug!("raw FUSE request buffer recycler is full or closed; dropping request buffer");
        }
    }
}

struct RecyclableRequestPayload {
    buffer: Option<Vec<u8>>,
    _spillover: Option<RawBufferSpilloverGuard>,
    payload_start: usize,
    payload_end: usize,
    recycler: SyncSender<Vec<u8>>,
}

impl RecyclableRequestPayload {
    fn new(
        buffer: RawRequestBuffer,
        request_len: usize,
        recycler: SyncSender<Vec<u8>>,
    ) -> io::Result<Self> {
        let (buffer, spillover) = buffer.into_parts();
        let payload_start = size_of::<InHeader>();
        if request_len < payload_start || request_len > buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid raw FUSE request length",
            ));
        }
        Ok(Self {
            buffer: Some(buffer),
            _spillover: spillover,
            payload_start,
            payload_end: request_len,
            recycler,
        })
    }
}

impl AsRef<[u8]> for RecyclableRequestPayload {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_ref().expect("request buffer present")[self.payload_start..self.payload_end]
    }
}

impl Drop for RecyclableRequestPayload {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        if self.recycler.try_send(buffer).is_err() {
            debug!("raw FUSE request buffer recycler is full or closed; dropping request buffer");
        }
    }
}

fn owned_request_payload(
    buffer: RawRequestBuffer,
    request_len: usize,
    recycler: SyncSender<Vec<u8>>,
) -> io::Result<Bytes> {
    RecyclableRequestPayload::new(buffer, request_len, recycler).map(Bytes::from_owner)
}

fn linux_should_use_owned_write_for_block(write_len: usize, block_size: usize) -> bool {
    block_size > 0 && write_len >= block_size
}

fn linux_should_use_owned_write(context: &RawRequestContext, write_len: usize) -> bool {
    linux_should_use_owned_write_for_block(write_len, context.max_write)
}

struct RawRequestJob {
    fd: RawFd,
    header: InHeader,
    request_buf: RawRequestBuffer,
    request_len: usize,
    context: RawRequestContext,
    accounting: RawDispatchAccounting,
    buffer_recycler: SyncSender<Vec<u8>>,
}

fn dispatch_minimal_request(
    fd: RawFd,
    header: InHeader,
    request_buf: RawRequestBuffer,
    request_len: usize,
    context: RawRequestContext,
    accounting: &RawDispatchAccounting,
    buffer_recycler: SyncSender<Vec<u8>>,
) -> io::Result<()> {
    let accounting = accounting.clone();
    let job = RawRequestJob {
        fd,
        header,
        request_buf,
        request_len,
        context,
        accounting,
        buffer_recycler,
    };
    if let (OP_WRITE, Some(runtime_handle)) = (header.opcode, job.context.runtime_handle.clone()) {
        dispatch_write_async(job, runtime_handle);
        return Ok(());
    }

    let RawRequestJob {
        fd,
        header,
        request_buf,
        request_len,
        context,
        accounting,
        buffer_recycler,
    } = job;
    let request_buf = RecycledRequestBuffer::new(request_buf, buffer_recycler);
    if let Some(runtime_handle) = context.runtime_handle.clone() {
        runtime_handle.spawn_blocking(move || {
            let _guard = accounting.admit_blocking(RawDispatchClass::for_opcode(header.opcode));
            let started = Instant::now();
            let payload = request_buf.payload(request_len);
            if let Err(err) = handle_minimal_request(fd, header, payload, &context, &accounting) {
                warn!(
                    opcode = header.opcode,
                    unique = header.unique,
                    error = %err,
                    "raw FUSE worker failed to reply"
                );
            }
            accounting.record_latency(header.opcode, started.elapsed());
        });
    } else {
        let _guard = accounting.admit_blocking(RawDispatchClass::for_opcode(header.opcode));
        let started = Instant::now();
        let payload = request_buf.payload(request_len);
        let result = handle_minimal_request(fd, header, payload, &context, &accounting);
        accounting.record_latency(header.opcode, started.elapsed());
        result?;
    }
    Ok(())
}

fn dispatch_write_async(job: RawRequestJob, runtime_handle: Handle) {
    let RawRequestJob {
        fd,
        header,
        request_buf,
        request_len,
        context,
        accounting,
        buffer_recycler,
    } = job;
    let guard = accounting.admit_blocking(RawDispatchClass::Stream);
    runtime_handle.spawn(async move {
        let _guard = guard;
        let started = Instant::now();
        let payload = match owned_request_payload(request_buf, request_len, buffer_recycler) {
            Ok(payload) => payload,
            Err(err) => {
                warn!(
                    opcode = header.opcode,
                    unique = header.unique,
                    error = %err,
                    "raw FUSE async write received invalid request buffer"
                );
                let _ = reply_error(fd, header.unique, libc::EINVAL, Some(&accounting));
                accounting.record_latency(header.opcode, started.elapsed());
                return;
            }
        };
        let reply_result = async {
            let Some(vfs) = &context.vfs else {
                return reply_error(fd, header.unique, libc::ENOSYS, Some(&accounting));
            };
            if payload.len() < size_of::<WriteIn>() {
                return reply_error(fd, header.unique, libc::EINVAL, Some(&accounting));
            }
            let input = read_struct::<WriteIn>(&payload[..size_of::<WriteIn>()])?;
            let data = payload.slice(size_of::<WriteIn>()..);
            let write_len = (input.size as usize).min(data.len());
            let local_sync_policy = linux_write_sync_policy(&context, vfs, input.fh);
            let result = if linux_should_use_owned_write(&context, write_len) {
                vfs.write_fh_owned_with_deferred_materialize(
                    input.fh,
                    header.nodeid,
                    input.offset,
                    data.slice(..write_len),
                    local_sync_policy,
                )
                .await
            } else {
                vfs.write_fh_with_deferred_materialize(
                    input.fh,
                    header.nodeid,
                    input.offset,
                    &data[..write_len],
                    local_sync_policy,
                )
                .await
            };
            match result {
                Ok(written) => reply_write(fd, header.unique, written, Some(&accounting)),
                Err(err) => {
                    reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(&accounting))
                }
            }
        }
        .await;
        if let Err(err) = reply_result {
            warn!(
                opcode = header.opcode,
                unique = header.unique,
                error = %err,
                "raw FUSE async write failed to reply"
            );
        }
        accounting.record_latency(header.opcode, started.elapsed());
    });
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
        OP_STATFS => reply_statfs(fd, header.unique, context, Some(accounting)),
        OP_OPEN => raw_open(fd, header, payload, context, accounting),
        OP_CREATE => raw_create(fd, header, payload, context, accounting),
        OP_MKNOD => raw_mknod(fd, header, payload, context, accounting),
        OP_MKDIR => raw_mkdir(fd, header, payload, context, accounting),
        OP_UNLINK => raw_unlink(fd, header, payload, context, accounting),
        OP_RMDIR => raw_rmdir(fd, header, payload, context, accounting),
        OP_RENAME => raw_rename(fd, header, payload, context, accounting),
        OP_SYMLINK => raw_symlink(fd, header, payload, context, accounting),
        OP_READLINK => raw_readlink(fd, header, context, accounting),
        OP_LINK => raw_link(fd, header, payload, context, accounting),
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
        OP_OPENDIR => raw_opendir(fd, header, context, accounting),
        OP_READDIR => raw_readdir(fd, header, payload, context, accounting),
        OP_READDIRPLUS => raw_readdirplus(fd, header, payload, context, accounting),
        OP_RELEASEDIR => raw_releasedir(fd, header, payload, context, accounting),
        OP_ACCESS => reply_empty(fd, header.unique, Some(accounting)),
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
        Ok(fh) => {
            context
                .write_sync_policies
                .insert(fh, linux_write_sync_policy_for_open_flags(open_flags));
            reply_open(
                fd,
                header.unique,
                fh,
                linux_open_flags_for_posix(flags, trunc),
                Some(accounting),
            )
        }
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_create(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if payload.len() < size_of::<CreateIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<CreateIn>(&payload[..size_of::<CreateIn>()])?;
    let name = nul_stripped(&payload[size_of::<CreateIn>()..]);
    let Some(attr) = raw_create_parent_entry(
        fd,
        header,
        context,
        accounting,
        name,
        input.mode,
        RawCreateEntryKind::File,
    )?
    else {
        return Ok(());
    };
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    let Ok(flags) = i32::try_from(input.flags) else {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    };
    let (open_flags, trunc) = posix_flags_to_open_flags(flags);
    match vfs.open_file(attr.ino, open_flags, trunc) {
        Ok(fh) => {
            context
                .write_sync_policies
                .insert(fh, linux_write_sync_policy_for_open_flags(open_flags));
            reply_create(
                fd,
                header.unique,
                &attr,
                fh,
                linux_open_flags_for_posix(flags, trunc),
                context,
                Some(accounting),
            )
        }
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_mknod(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if payload.len() < size_of::<MknodIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<MknodIn>(&payload[..size_of::<MknodIn>()])?;
    let name = nul_stripped(&payload[size_of::<MknodIn>()..]);
    if let Some(attr) = raw_create_parent_entry(
        fd,
        header,
        context,
        accounting,
        name,
        input.mode,
        RawCreateEntryKind::File,
    )? {
        reply_entry(fd, header.unique, &attr, context, Some(accounting))
    } else {
        Ok(())
    }
}

fn raw_mkdir(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if payload.len() < size_of::<MkdirIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<MkdirIn>(&payload[..size_of::<MkdirIn>()])?;
    let name = nul_stripped(&payload[size_of::<MkdirIn>()..]);
    if let Some(attr) = raw_create_parent_entry(
        fd,
        header,
        context,
        accounting,
        name,
        input.mode,
        RawCreateEntryKind::Directory,
    )? {
        reply_entry(fd, header.unique, &attr, context, Some(accounting))
    } else {
        Ok(())
    }
}

fn raw_create_parent_entry(
    fd: RawFd,
    header: InHeader,
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
    name: &[u8],
    mode: u32,
    kind: RawCreateEntryKind,
) -> io::Result<Option<InodeAttr>> {
    let Some(vfs) = &context.vfs else {
        reply_error(fd, header.unique, libc::ENOSYS, Some(accounting))?;
        return Ok(None);
    };
    let mode = normalize_unix_mode(mode);
    let result = match kind {
        RawCreateEntryKind::File => {
            vfs.create_deferred_metadata_flush(header.nodeid, name, header.uid, header.gid, mode)
        }
        RawCreateEntryKind::Directory => {
            vfs.mkdir(header.nodeid, name, header.uid, header.gid, mode)
        }
    };
    match result {
        Ok(attr) => Ok(Some(attr)),
        Err(err) => {
            reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting))?;
            Ok(None)
        }
    }
}

fn raw_unlink(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    match vfs.unlink(header.nodeid, nul_stripped(payload)) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_rmdir(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    match vfs.rmdir(header.nodeid, nul_stripped(payload)) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_rename(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<RenameIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<RenameIn>(&payload[..size_of::<RenameIn>()])?;
    let Some((name, new_name)) = split_two_nul_strings(&payload[size_of::<RenameIn>()..]) else {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    };
    match vfs.rename(header.nodeid, name, input.newdir, new_name) {
        Ok(()) => reply_empty(fd, header.unique, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_symlink(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    let Some((name, target)) = split_two_nul_strings(payload) else {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    };
    match vfs.symlink(header.nodeid, name, target, header.uid, header.gid) {
        Ok(attr) => reply_entry(fd, header.unique, &attr, context, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_readlink(
    fd: RawFd,
    header: InHeader,
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    match vfs.readlink(header.nodeid) {
        Ok(target) => reply_data(fd, header.unique, &target, Some(accounting)),
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_link(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_error(fd, header.unique, libc::ENOSYS, Some(accounting));
    };
    if payload.len() < size_of::<LinkIn>() {
        return reply_error(fd, header.unique, libc::EINVAL, Some(accounting));
    }
    let input = read_struct::<LinkIn>(&payload[..size_of::<LinkIn>()])?;
    let name = nul_stripped(&payload[size_of::<LinkIn>()..]);
    match vfs.link(input.oldnodeid, header.nodeid, name) {
        Ok(attr) => reply_entry(fd, header.unique, &attr, context, Some(accounting)),
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
    RAW_READ_BUFFER.with(|buffer| {
        let mut out = buffer.borrow_mut();
        let size = input.size as usize;
        if out.len() < size {
            out.resize(size, 0);
        }
        match rt.block_on(vfs.read_into_fh(input.fh, header.nodeid, input.offset, &mut out[..size]))
        {
            Ok(read) => reply_data(fd, header.unique, &out[..read as usize], Some(accounting)),
            Err(err) => {
                out.clear();
                reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting))
            }
        }
    })
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
    let local_sync_policy = linux_write_sync_policy(context, vfs, input.fh);
    match rt.block_on(vfs.write_fh_with_deferred_materialize(
        input.fh,
        header.nodeid,
        input.offset,
        &data[..write_len],
        local_sync_policy,
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
    match rt.block_on(vfs.flush_fuse_writeback_with_outcome(header.nodeid, input.fh)) {
        Ok(_) => reply_empty(fd, header.unique, Some(accounting)),
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
    let is_datasync = (input.fsync_flags & FUSE_FSYNC_FDATASYNC) != 0;
    let result = if input.fh != 0 && vfs.get_open_file(input.fh).is_some() {
        // flush_fh persists local dirty file data and attrs, so it is strong
        // enough for both fsync and fdatasync without waiting for publication.
        rt.block_on(vfs.flush_fh(input.fh))
    } else {
        rt.block_on(vfs.fsync(header.nodeid, is_datasync))
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
    context.write_sync_policies.remove(&input.fh);
    // The deferred helper owns final-handle errors by logging them out of band;
    // raw FUSE release must not park the dispatch loop on the local barrier.
    vfs.release_fh_with_deferred_last_handle_barrier(input.fh, rt);
    reply_empty(fd, header.unique, Some(accounting))
}

fn raw_opendir(
    fd: RawFd,
    header: InHeader,
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    let Some(vfs) = &context.vfs else {
        return reply_open(fd, header.unique, 1, 0, Some(accounting));
    };
    match raw_load_open_dir_state(vfs, header.nodeid) {
        Ok(state) => {
            let fh = context.next_open_dir_fh.fetch_add(1, Ordering::Relaxed);
            context.open_dir_handles.insert(fh, state);
            reply_open(fd, header.unique, fh, 0, Some(accounting))
        }
        Err(err) => reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting)),
    }
}

fn raw_releasedir(
    fd: RawFd,
    header: InHeader,
    payload: &[u8],
    context: &RawRequestContext,
    accounting: &RawDispatchAccounting,
) -> io::Result<()> {
    if payload.len() >= size_of::<ReleaseIn>() {
        let input = read_struct::<ReleaseIn>(&payload[..size_of::<ReleaseIn>()])?;
        context.open_dir_handles.remove(&input.fh);
    }
    reply_empty(fd, header.unique, Some(accounting))
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
    let mut out = Vec::with_capacity((input.size as usize).min(READDIR_REPLY_INITIAL_CAPACITY));
    if input.offset == 0
        && !push_dirent(
            &mut out,
            FUSE_ROOT_ID,
            1,
            libc::DT_DIR,
            b".",
            input.size as usize,
        )
    {
        return reply_data(fd, header.unique, &out, Some(accounting));
    }
    if input.offset <= 1
        && !push_dirent(
            &mut out,
            FUSE_ROOT_ID,
            2,
            libc::DT_DIR,
            b"..",
            input.size as usize,
        )
    {
        return reply_data(fd, header.unique, &out, Some(accounting));
    }
    let state = match raw_open_dir_state(context, vfs, header.nodeid, input.fh) {
        Ok(state) => state,
        Err(errno) => {
            return reply_error(fd, header.unique, errno, Some(accounting));
        }
    };
    for (index, entry_with_attr) in state
        .entries
        .iter()
        .enumerate()
        .skip(readdir_entry_skip_offset(input.offset))
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

fn raw_readdirplus(
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
    let mut out = Vec::with_capacity((input.size as usize).min(READDIR_REPLY_INITIAL_CAPACITY));
    if input.offset <= 1 {
        let current_attr = match vfs.getattr(header.nodeid) {
            Ok(attr) => attr,
            Err(err) => {
                return reply_error(fd, header.unique, vfs_err_to_errno(&err), Some(accounting));
            }
        };
        let parent_attr = if current_attr.parent == 0 || current_attr.parent == current_attr.ino {
            current_attr.clone()
        } else {
            match vfs.getattr(current_attr.parent) {
                Ok(attr) => attr,
                Err(err) => {
                    return reply_error(
                        fd,
                        header.unique,
                        vfs_err_to_errno(&err),
                        Some(accounting),
                    );
                }
            }
        };
        if !push_readdirplus_dot_entries(
            &mut out,
            &current_attr,
            &parent_attr,
            context,
            input.offset,
            input.size as usize,
        ) {
            return reply_data(fd, header.unique, &out, Some(accounting));
        }
    }
    let state = match raw_open_dir_state(context, vfs, header.nodeid, input.fh) {
        Ok(state) => state,
        Err(errno) => {
            return reply_error(fd, header.unique, errno, Some(accounting));
        }
    };
    for (index, entry_with_attr) in state
        .entries
        .iter()
        .enumerate()
        .skip(readdir_entry_skip_offset(input.offset))
    {
        let entry = &entry_with_attr.entry;
        let Some(attr) = &entry_with_attr.attr else {
            continue;
        };
        let Ok(offset) = u64::try_from(index + 3) else {
            return reply_error(fd, header.unique, libc::EOVERFLOW, Some(accounting));
        };
        if !push_direntplus(
            &mut out,
            attr,
            context,
            offset,
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
    max_write: usize,
    ids: SessionIds,
    next_open_dir_fh: Arc<AtomicU64>,
    open_dir_handles: Arc<DashMap<u64, RawOpenDirState>>,
    write_sync_policies: Arc<DashMap<u64, LocalSyncPolicy>>,
}

#[derive(Clone)]
struct RawOpenDirState {
    entries: Arc<Vec<DirEntryWithAttr>>,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawEntryOut {
    nodeid: u64,
    generation: u64,
    entry_valid: u64,
    attr_valid: u64,
    entry_valid_nsec: u32,
    attr_valid_nsec: u32,
    attr: RawEntryAttr,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawEntryAttr {
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
struct RawOpenOut {
    fh: u64,
    open_flags: u32,
    backing_id: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct RawDirent {
    ino: u64,
    off: u64,
    namelen: u32,
    dtype: u32,
}

fn linux_open_flags_for_posix(flags: i32, trunc: bool) -> u32 {
    let access = flags & libc::O_ACCMODE;
    let read_only = access == libc::O_RDONLY;
    let append = flags & libc::O_APPEND != 0;
    if read_only && !append && !trunc {
        FOPEN_KEEP_CACHE
    } else {
        0
    }
}

fn linux_attr_blksize(attr: &InodeAttr) -> u32 {
    match attr.kind {
        FileType::RegularFile => LINUX_REGULAR_FILE_IO_SIZE,
        FileType::Directory | FileType::Symlink => LINUX_DEFAULT_IO_SIZE,
    }
}

fn linux_write_sync_policy_for_open_flags(flags: OpenFlags) -> LocalSyncPolicy {
    if flags.contains(OpenFlags::SYNC) {
        LocalSyncPolicy::WriteThrough
    } else {
        LocalSyncPolicy::Opportunistic
    }
}

fn linux_write_sync_policy(
    context: &RawRequestContext,
    vfs: &StudioVfs,
    fh: u64,
) -> LocalSyncPolicy {
    if let Some(policy) = context.write_sync_policies.get(&fh) {
        return *policy;
    }
    if vfs
        .get_open_file(fh)
        .is_some_and(|open_file| open_file.flags.contains(OpenFlags::SYNC))
    {
        LocalSyncPolicy::WriteThrough
    } else {
        LocalSyncPolicy::Opportunistic
    }
}

fn raw_entry_out(inode_attr: &InodeAttr, context: &RawRequestContext) -> RawEntryOut {
    let bridge = to_bridge_attr(
        inode_attr,
        context.ids.uid,
        context.ids.gid,
        linux_attr_blksize(inode_attr),
    );
    RawEntryOut {
        nodeid: inode_attr.ino,
        generation: 0,
        entry_valid: 1,
        attr_valid: 1,
        entry_valid_nsec: 0,
        attr_valid_nsec: 0,
        attr: RawEntryAttr {
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
    }
}

fn raw_load_open_dir_state(
    vfs: &StudioVfs,
    ino: u64,
) -> Result<RawOpenDirState, studiofs_vfs::VfsError> {
    // ponytail: full-dir snapshot is enough for current mounted workloads; move to
    // paginated VFS readdir if large-directory memory pressure shows up.
    Ok(RawOpenDirState {
        entries: Arc::new(vfs.readdir_with_attrs(ino)?),
    })
}

fn raw_open_dir_state(
    context: &RawRequestContext,
    vfs: &StudioVfs,
    ino: u64,
    fh: u64,
) -> Result<RawOpenDirState, i32> {
    if let Some(state) = raw_open_dir_handle_state(context, ino, fh)? {
        return Ok(state);
    }
    raw_load_open_dir_state(vfs, ino).map_err(|err| vfs_err_to_errno(&err))
}

fn raw_open_dir_handle_state(
    context: &RawRequestContext,
    ino: u64,
    fh: u64,
) -> Result<Option<RawOpenDirState>, i32> {
    if let Some(state) = context.open_dir_handles.get(&fh) {
        return Ok(Some(state.value().clone()));
    }
    if fh != 0 {
        warn!(ino, fh, "raw readdir with unknown directory fh");
        return Err(libc::EBADF);
    }
    Ok(None)
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
                let bridge = to_bridge_attr(
                    &attr,
                    context.ids.uid,
                    context.ids.gid,
                    linux_attr_blksize(&attr),
                );
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
    let out = raw_entry_out(inode_attr, context);
    reply_struct(fd, unique, &out, accounting)
}

fn reply_create(
    fd: RawFd,
    unique: u64,
    inode_attr: &InodeAttr,
    fh: u64,
    open_flags: u32,
    context: &RawRequestContext,
    accounting: Option<&RawDispatchAccounting>,
) -> io::Result<()> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct CreateOut {
        entry: RawEntryOut,
        open: RawOpenOut,
    }
    let out = CreateOut {
        entry: raw_entry_out(inode_attr, context),
        open: RawOpenOut {
            fh,
            open_flags,
            backing_id: 0,
        },
    };
    // SAFETY: `CreateOut` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding/layout fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for CreateOut {}
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
    let bridge = to_bridge_attr(
        inode_attr,
        context.ids.uid,
        context.ids.gid,
        linux_attr_blksize(inode_attr),
    );
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
    context: &RawRequestContext,
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
    let st = match &context.vfs {
        Some(vfs) => match vfs.statfs() {
            Ok(st) => Kstatfs {
                blocks: st.blocks,
                bfree: st.bfree,
                bavail: st.bavail,
                files: st.files,
                ffree: st.ffree,
                bsize: st.bsize,
                namelen: st.namelen,
                frsize: st.frsize,
                padding: 0,
                spare: [0; 6],
            },
            Err(err) => return reply_error(fd, unique, vfs_err_to_errno(&err), accounting),
        },
        None => Kstatfs {
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
    };
    reply_struct(fd, unique, &StatfsOut { st }, accounting)
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
        backing_id: i32,
    }
    // SAFETY: `OpenOut` is `repr(C)`, integer-only, and has explicit padding.
    unsafe impl FuseBytes for OpenOut {}
    reply_struct(
        fd,
        unique,
        &OpenOut {
            fh,
            open_flags,
            backing_id: 0,
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
    let Ok(namelen) = u32::try_from(name.len()) else {
        return false;
    };
    let entry_len = align_8(size_of::<RawDirent>() + name.len());
    if out.len().saturating_add(entry_len) > limit {
        return false;
    }
    let dirent = RawDirent {
        ino,
        off: offset,
        namelen,
        dtype: u32::from(dtype),
    };
    out.extend_from_slice(as_bytes(&dirent));
    out.extend_from_slice(name);
    out.resize(
        out.len() + (entry_len - size_of::<RawDirent>() - name.len()),
        0,
    );
    true
}

fn push_direntplus(
    out: &mut Vec<u8>,
    attr: &InodeAttr,
    context: &RawRequestContext,
    offset: u64,
    name: &[u8],
    limit: usize,
) -> bool {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct DirentPlus {
        entry_out: RawEntryOut,
        dirent: RawDirent,
    }
    const _: () =
        assert!(size_of::<DirentPlus>() == size_of::<RawEntryOut>() + size_of::<RawDirent>());
    let Ok(namelen) = u32::try_from(name.len()) else {
        return false;
    };
    let entry_len = align_8(size_of::<DirentPlus>() + name.len());
    if out.len().saturating_add(entry_len) > limit {
        return false;
    }
    let dtype = match attr.kind {
        FileType::RegularFile => libc::DT_REG,
        FileType::Directory => libc::DT_DIR,
        FileType::Symlink => libc::DT_LNK,
    };
    let entry = DirentPlus {
        entry_out: raw_entry_out(attr, context),
        dirent: RawDirent {
            ino: attr.ino,
            off: offset,
            namelen,
            dtype: u32::from(dtype),
        },
    };
    // SAFETY: `DirentPlus` and nested structs are `repr(C)`, integer-only, and
    // include explicit padding/layout fields for the Linux FUSE wire layout.
    unsafe impl FuseBytes for DirentPlus {}
    out.extend_from_slice(as_bytes(&entry));
    out.extend_from_slice(name);
    out.resize(
        out.len() + (entry_len - size_of::<DirentPlus>() - name.len()),
        0,
    );
    true
}

fn push_readdirplus_dot_entries(
    out: &mut Vec<u8>,
    current_attr: &InodeAttr,
    parent_attr: &InodeAttr,
    context: &RawRequestContext,
    offset: u64,
    limit: usize,
) -> bool {
    if offset == 0 && !push_direntplus(out, current_attr, context, 1, b".", limit) {
        return false;
    }
    if offset <= 1 && !push_direntplus(out, parent_attr, context, 2, b"..", limit) {
        return false;
    }
    true
}

fn readdir_entry_skip_offset(offset: u64) -> usize {
    offset.saturating_sub(2) as usize
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

#[repr(C)]
#[derive(Clone, Copy)]
struct NotifyInvalInodeOut {
    ino: u64,
    off: i64,
    len: i64,
}

fn fuse_notify_header(notify_code: i32, payload_len: usize) -> OutHeader {
    OutHeader {
        len: (size_of::<OutHeader>() + payload_len) as u32,
        error: notify_code,
        unique: 0,
    }
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

#[derive(Clone, Copy)]
struct RawSessionShutdown {
    poll_interval: Option<Duration>,
    requested: fn() -> bool,
}

impl RawSessionShutdown {
    fn for_signal_handling(signal_handling: SignalHandling) -> Self {
        match signal_handling {
            SignalHandling::Disabled => Self {
                poll_interval: None,
                requested: raw_shutdown_never_requested,
            },
            SignalHandling::InstallForCli => Self {
                poll_interval: Some(RAW_SIGNAL_POLL_INTERVAL),
                requested: raw_shutdown_requested,
            },
        }
    }

    fn is_requested(self) -> bool {
        (self.requested)()
    }
}

fn read_next_fuse_request(
    fd: RawFd,
    buf: &mut [u8],
    shutdown: RawSessionShutdown,
) -> io::Result<Option<usize>> {
    let Some(poll_interval) = shutdown.poll_interval else {
        return read_fd(fd, buf).map(Some);
    };

    loop {
        if shutdown.is_requested() {
            return Ok(None);
        }
        if !poll_fd_for_read(fd, poll_interval)? {
            continue;
        }
        return read_fd(fd, buf).map(Some);
    }
}

fn poll_fd_for_read(fd: RawFd, timeout: Duration) -> io::Result<bool> {
    let timeout_ms = timeout.as_millis().min(libc::c_int::MAX as u128) as libc::c_int;
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: `pollfd` points to one valid poll descriptor.
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if rc > 0 {
            return Ok(pollfd.revents != 0);
        }
        if rc == 0 {
            return Ok(false);
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

fn normalize_unix_mode(mode: u32) -> u16 {
    u16::try_from(mode & 0o7777).unwrap_or(0)
}

fn nul_stripped(bytes: &[u8]) -> &[u8] {
    bytes.strip_suffix(&[0]).unwrap_or(bytes)
}

fn split_two_nul_strings(bytes: &[u8]) -> Option<(&[u8], &[u8])> {
    let first_end = bytes.iter().position(|byte| *byte == 0)?;
    let rest = &bytes[first_end + 1..];
    Some((&bytes[..first_end], nul_stripped(rest)))
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
    let total_len = names.iter().map(|name| name.len() + 1).sum();
    let mut data = Vec::with_capacity(total_len);
    for name in names {
        data.extend_from_slice(name);
        data.push(0);
    }
    data
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
    if !in_data.is_empty() || !(u32::BITS / 8..=8).contains(&out_size) {
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

struct RawKernelInvalidatorRegistration {
    vfs: Arc<StudioVfs>,
}

impl RawKernelInvalidatorRegistration {
    fn new(vfs: Arc<StudioVfs>, fd: RawFd) -> io::Result<Self> {
        let invalidator = Arc::new(RawFuseKernelInvalidator::new(fd)?);
        vfs.set_kernel_invalidator(invalidator);
        Ok(Self { vfs })
    }
}

impl Drop for RawKernelInvalidatorRegistration {
    fn drop(&mut self) {
        self.vfs.clear_kernel_invalidator();
    }
}

struct RawFuseKernelInvalidator {
    requests: SyncSender<RawInvalidationRequest>,
}

impl RawFuseKernelInvalidator {
    fn new(fd: RawFd) -> io::Result<Self> {
        // SAFETY: `F_DUPFD_CLOEXEC` duplicates the fd and marks it close-on-exec
        // so the worker copy cannot leak into child processes.
        let fd = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` was freshly returned by `fcntl` above and is not owned
        // anywhere else.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let (requests, receiver) = mpsc::sync_channel(RAW_INVALIDATION_QUEUE_CAPACITY);
        thread::Builder::new()
            .name("studiofs-raw-fuse-invalidator".into())
            .spawn(move || run_raw_invalidation_worker(fd, receiver))?;
        Ok(Self { requests })
    }

    fn enqueue_inval_inode(&self, ino: u64, off: i64, len: i64, description: &'static str) {
        let out = NotifyInvalInodeOut { ino, off, len };
        match self
            .requests
            .try_send(RawInvalidationRequest::InvalInode { out, description })
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                warn!(
                    ino,
                    description, "raw FUSE inode invalidation queue is full; dropping notification"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                warn!(
                    ino,
                    description, "raw FUSE inode invalidation worker is unavailable"
                );
            }
        }
    }
}

impl KernelInvalidator for RawFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.enqueue_inval_inode(ino, 0, -1, "data");
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.enqueue_inval_inode(ino, -1, 0, "attributes");
    }
}

enum RawInvalidationRequest {
    InvalInode {
        out: NotifyInvalInodeOut,
        description: &'static str,
    },
}

fn run_raw_invalidation_worker(fd: OwnedFd, receiver: Receiver<RawInvalidationRequest>) {
    while let Ok(request) = receiver.recv() {
        match request {
            RawInvalidationRequest::InvalInode { out, description } => {
                if let Err(error) = notify_inval_inode_fd(fd.as_raw_fd(), out) {
                    match raw_invalidation_error_action(&error) {
                        RawInvalidationErrorAction::StopWorker => break,
                        RawInvalidationErrorAction::Ignore => {}
                        RawInvalidationErrorAction::Warn => {
                            warn!(
                                ino = out.ino,
                                off = out.off,
                                len = out.len,
                                error = ?error,
                                description,
                                "raw FUSE inode invalidation failed"
                            );
                        }
                    }
                }
            }
        }
    }
}

fn notify_inval_inode_fd(fd: RawFd, out: NotifyInvalInodeOut) -> io::Result<()> {
    let header = fuse_notify_header(FUSE_NOTIFY_INVAL_INODE, size_of::<NotifyInvalInodeOut>());
    write_fuse_packet_vectored_fd(fd, as_bytes(&header), as_bytes(&out))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RawInvalidationErrorAction {
    StopWorker,
    Ignore,
    Warn,
}

fn raw_invalidation_error_action(error: &io::Error) -> RawInvalidationErrorAction {
    match error.raw_os_error() {
        Some(libc::ENODEV | libc::ENOTCONN | libc::EPIPE | libc::EBADF | libc::ESHUTDOWN) => {
            RawInvalidationErrorAction::StopWorker
        }
        Some(libc::ENOENT) => RawInvalidationErrorAction::Ignore,
        _ => RawInvalidationErrorAction::Warn,
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

/// Marker for plain-old-data FUSE wire structs that can be viewed as bytes.
///
/// # Safety
///
/// Implementors must be `repr(C)`, contain only initialized byte-safe fields,
/// and have no padding whose contents can be observed as uninitialized memory.
unsafe trait FuseBytes: Copy {}

// SAFETY: these Linux FUSE wire structs are `repr(C)`, integer-only, and any
// protocol padding is represented as initialized fields.
unsafe impl FuseBytes for InHeader {}
unsafe impl FuseBytes for InitIn {}
unsafe impl FuseBytes for InitOut {}
unsafe impl FuseBytes for RawEntryOut {}
unsafe impl FuseBytes for RawOpenOut {}
unsafe impl FuseBytes for RawDirent {}
unsafe impl FuseBytes for NotifyInvalInodeOut {}
unsafe impl FuseBytes for OpenIn {}
unsafe impl FuseBytes for MknodIn {}
unsafe impl FuseBytes for MkdirIn {}
unsafe impl FuseBytes for RenameIn {}
unsafe impl FuseBytes for LinkIn {}
unsafe impl FuseBytes for CreateIn {}
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
    fn raw_notify_inval_inode_header_uses_kernel_notify_opcode() {
        let header = fuse_notify_header(FUSE_NOTIFY_INVAL_INODE, size_of::<NotifyInvalInodeOut>());

        assert_eq!(
            header.len as usize,
            size_of::<OutHeader>() + size_of::<NotifyInvalInodeOut>()
        );
        assert_eq!(header.error, FUSE_NOTIFY_INVAL_INODE);
        assert_eq!(header.unique, 0);
    }

    #[test]
    fn raw_notify_inval_inode_payload_supports_full_and_attr_only_invalidations() {
        let full = NotifyInvalInodeOut {
            ino: 42,
            off: 0,
            len: -1,
        };
        let attrs_only = NotifyInvalInodeOut {
            ino: 42,
            off: -1,
            len: 0,
        };

        assert_eq!(full.ino, attrs_only.ino);
        assert_eq!(full.off, 0);
        assert_eq!(attrs_only.off, -1);
        assert_eq!(full.len, -1);
        assert_eq!(attrs_only.len, 0);
    }

    #[test]
    fn raw_invalidation_error_action_stops_only_on_dead_fuse_session() {
        for errno in [
            libc::ENODEV,
            libc::ENOTCONN,
            libc::EPIPE,
            libc::EBADF,
            libc::ESHUTDOWN,
        ] {
            assert_eq!(
                raw_invalidation_error_action(&io::Error::from_raw_os_error(errno)),
                RawInvalidationErrorAction::StopWorker
            );
        }
        assert_eq!(
            raw_invalidation_error_action(&io::Error::from_raw_os_error(libc::ENOENT)),
            RawInvalidationErrorAction::Ignore
        );
        assert_eq!(
            raw_invalidation_error_action(&io::Error::from_raw_os_error(libc::ENOBUFS)),
            RawInvalidationErrorAction::Warn
        );
    }

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
    fn raw_juicefs_like_profile_centralizes_buffer_cache_and_concurrency_defaults() {
        let profile = LinuxRawFuseProfile::juicefs_like();

        assert_eq!(profile.label(), "raw-juicefs-like");
        assert_eq!(profile.read_buffer_budget_bytes, 400 * 1024 * 1024);
        assert_eq!(profile.write_buffer_budget_bytes, 400 * 1024 * 1024);
        assert_eq!(profile.max_background, 200);
        assert_eq!(profile.congestion_threshold, 150);
        assert_eq!(profile.prefetch_concurrency, 1);
        assert_eq!(profile.upload_concurrency, 20);
        assert_eq!(profile.download_concurrency, 200);
        assert_eq!(profile.cache_size_policy, "operator-cache-budget");
        assert!(profile.kernel_writeback_cache);
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
    fn raw_init_advertises_readdirplus_when_kernel_supports_it() {
        let negotiated = LinuxRawFuseProfile::juicefs_like().negotiate_init(InitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_MINOR_VERSION,
            max_readahead: 1024 * 1024,
            flags: u32::MAX,
        });

        assert_eq!(negotiated.out.flags & FUSE_READDIRPLUS, FUSE_READDIRPLUS);
        assert_eq!(
            negotiated.out.flags & FUSE_READDIRPLUS_AUTO,
            FUSE_READDIRPLUS_AUTO
        );
    }

    #[test]
    fn raw_init_advertises_writeback_cache_for_juicefs_like_profile_when_kernel_supports_it() {
        let input = InitIn {
            major: FUSE_KERNEL_VERSION,
            minor: FUSE_MINOR_VERSION,
            max_readahead: 1024 * 1024,
            flags: FUSE_ASYNC_READ | FUSE_WRITEBACK_CACHE,
        };

        let enabled = LinuxRawFuseProfile::juicefs_like().negotiate_init(input);
        let disabled = LinuxRawFuseProfile {
            kernel_writeback_cache: false,
            ..LinuxRawFuseProfile::juicefs_like()
        }
        .negotiate_init(input);

        assert_eq!(
            enabled.out.flags & FUSE_WRITEBACK_CACHE,
            FUSE_WRITEBACK_CACHE
        );
        assert_eq!(disabled.out.flags & FUSE_WRITEBACK_CACHE, 0);
    }

    #[test]
    fn raw_readdir_cookie_one_preserves_first_real_entry() {
        assert_eq!(readdir_entry_skip_offset(0), 0);
        assert_eq!(readdir_entry_skip_offset(1), 0);
        assert_eq!(readdir_entry_skip_offset(2), 0);
        assert_eq!(readdir_entry_skip_offset(3), 1);
    }

    #[test]
    fn raw_readdirplus_dot_entries_use_standard_offsets() {
        let context = RawRequestContext {
            vfs: None,
            runtime_handle: None,
            max_write: LinuxRawFuseProfile::juicefs_like().max_write as usize,
            ids: SessionIds {
                uid: 1000,
                gid: 1000,
            },
            next_open_dir_fh: Arc::new(AtomicU64::new(1)),
            open_dir_handles: Arc::new(DashMap::new()),
            write_sync_policies: Arc::new(DashMap::new()),
        };
        let current = InodeAttr::new_dir(7, FUSE_ROOT_ID, 1000, 1000, 0o755);
        let parent = InodeAttr::new_dir(FUSE_ROOT_ID, 0, 1000, 1000, 0o755);
        let mut out = Vec::new();

        assert!(push_readdirplus_dot_entries(
            &mut out, &current, &parent, &context, 0, 4096
        ));

        let dirent_offset = size_of::<RawEntryOut>();
        let first = read_struct::<RawDirent>(&out[dirent_offset..]).unwrap();
        let first_name = dirent_offset + size_of::<RawDirent>();
        let second_offset = align_8(size_of::<RawEntryOut>() + size_of::<RawDirent>() + 1);
        let second = read_struct::<RawDirent>(&out[second_offset + dirent_offset..]).unwrap();
        let second_name = second_offset + dirent_offset + size_of::<RawDirent>();

        assert_eq!(
            (first.ino, first.off, &out[first_name..first_name + 1]),
            (7, 1, &b"."[..])
        );
        assert_eq!(
            (second.ino, second.off, &out[second_name..second_name + 2]),
            (FUSE_ROOT_ID, 2, &b".."[..])
        );
    }

    #[test]
    fn raw_read_only_open_keeps_kernel_page_cache() {
        let open_flags = linux_open_flags_for_posix(libc::O_RDONLY, false);

        assert_eq!(open_flags & FOPEN_KEEP_CACHE, FOPEN_KEEP_CACHE);
    }

    #[test]
    fn raw_read_only_truncate_open_does_not_keep_kernel_page_cache() {
        let open_flags = linux_open_flags_for_posix(libc::O_RDONLY | libc::O_TRUNC, true);

        assert_eq!(open_flags & FOPEN_KEEP_CACHE, 0);
    }

    #[test]
    fn raw_write_capable_open_does_not_keep_kernel_page_cache() {
        let open_flags = linux_open_flags_for_posix(libc::O_RDWR, false);

        assert_eq!(open_flags & FOPEN_KEEP_CACHE, 0);
    }

    #[test]
    fn raw_sync_open_flags_use_write_through_policy() {
        assert_eq!(
            linux_write_sync_policy_for_open_flags(OpenFlags::SYNC),
            LocalSyncPolicy::WriteThrough
        );
        assert_eq!(
            linux_write_sync_policy_for_open_flags(OpenFlags::empty()),
            LocalSyncPolicy::Opportunistic
        );
    }

    #[test]
    fn raw_regular_file_attrs_report_four_mib_preferred_io_size() {
        let context = RawRequestContext {
            vfs: None,
            runtime_handle: None,
            max_write: LinuxRawFuseProfile::juicefs_like().max_write as usize,
            ids: SessionIds {
                uid: 1000,
                gid: 1000,
            },
            next_open_dir_fh: Arc::new(AtomicU64::new(1)),
            open_dir_handles: Arc::new(DashMap::new()),
            write_sync_policies: Arc::new(DashMap::new()),
        };
        let attr = InodeAttr::new_file(9, FUSE_ROOT_ID, 1000, 1000, 0o644);

        let out = raw_entry_out(&attr, &context);

        assert_eq!(out.attr.blksize, 4 * 1024 * 1024);
    }

    #[test]
    fn raw_open_dir_handle_state_rejects_unknown_nonzero_fh() {
        let context = RawRequestContext {
            vfs: None,
            runtime_handle: None,
            max_write: LinuxRawFuseProfile::juicefs_like().max_write as usize,
            ids: SessionIds { uid: 0, gid: 0 },
            next_open_dir_fh: Arc::new(AtomicU64::new(1)),
            open_dir_handles: Arc::new(DashMap::new()),
            write_sync_policies: Arc::new(DashMap::new()),
        };

        assert!(matches!(
            raw_open_dir_handle_state(&context, FUSE_ROOT_ID, 42),
            Err(errno) if errno == libc::EBADF
        ));
    }

    #[test]
    fn raw_request_buffer_returns_to_pool_on_drop() {
        let (sender, receiver) = mpsc::sync_channel(1);

        drop(RecycledRequestBuffer::new(
            RawRequestBuffer::pooled(vec![7, 8, 9]),
            sender,
        ));

        assert_eq!(receiver.try_recv().unwrap(), vec![7, 8, 9]);
    }

    #[test]
    fn raw_recv_request_buffer_bounds_spillover_when_pool_is_empty() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let spillover_accounting = Arc::new(RawBufferSpilloverAccounting::new(1));
        sender.send(vec![1, 2, 3]).unwrap();

        let pooled = recv_request_buffer(&receiver, 8, &spillover_accounting).unwrap();
        assert_eq!(pooled.as_slice(), &[1, 2, 3]);

        let spillover = recv_request_buffer(&receiver, 8, &spillover_accounting).unwrap();
        assert_eq!(spillover.as_slice(), vec![0; 8]);

        let sender_for_blocked_recv = sender.clone();
        let join = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            sender_for_blocked_recv.send(vec![4, 5, 6]).unwrap();
        });

        let blocked = recv_request_buffer(&receiver, 8, &spillover_accounting).unwrap();
        assert_eq!(blocked.as_slice(), &[4, 5, 6]);
        join.join().unwrap();

        drop(sender);
        drop(spillover);
        assert!(recv_request_buffer(&receiver, 8, &spillover_accounting).is_err());
    }

    #[test]
    fn raw_owned_request_payload_returns_buffer_after_last_bytes_drop() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let mut buffer = vec![0; size_of::<InHeader>() + 4];
        buffer[size_of::<InHeader>()..].copy_from_slice(b"data");

        let payload = owned_request_payload(
            RawRequestBuffer::pooled(buffer),
            size_of::<InHeader>() + 4,
            sender,
        )
        .expect("payload bytes");
        let view = payload.slice(1..3);

        assert_eq!(&payload[..], b"data");
        assert_eq!(&view[..], b"at");
        assert!(receiver.try_recv().is_err());

        drop(payload);
        assert!(receiver.try_recv().is_err());

        drop(view);
        let recycled = receiver.try_recv().expect("request buffer recycled");
        assert_eq!(&recycled[size_of::<InHeader>()..], b"data");
    }

    #[test]
    fn raw_owned_write_is_reserved_for_full_fuse_writes() {
        assert!(!linux_should_use_owned_write_for_block(0, 0));
        assert!(!linux_should_use_owned_write_for_block(
            4 * 1024,
            1024 * 1024
        ));
        assert!(!linux_should_use_owned_write_for_block(
            1024 * 1024 - 1,
            1024 * 1024
        ));
        assert!(linux_should_use_owned_write_for_block(
            1024 * 1024,
            1024 * 1024
        ));
    }

    #[test]
    fn raw_read_loop_returns_cleanly_when_cli_shutdown_is_requested() {
        let shutdown = RawSessionShutdown {
            poll_interval: Some(Duration::ZERO),
            requested: || true,
        };
        let mut buf = [];

        let result = read_next_fuse_request(-1, &mut buf, shutdown).unwrap();

        assert_eq!(result, None);
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

    #[test]
    fn raw_ioctl_getflags_rejects_oversized_output() {
        assert_eq!(
            raw_fs_ioc_getflags_reply_data(libc::FS_IOC_GETFLAGS as u32, &[], 16).unwrap_err(),
            libc::EINVAL
        );
    }
}
