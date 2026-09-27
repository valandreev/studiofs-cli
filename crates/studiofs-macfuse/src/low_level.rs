//! Safe wrappers around the macFUSE low-level C API.
//!
//! This module is the only place in `studiofs-macfuse` that declares raw
//! low-level macFUSE FFI. Public types copy callback-owned values before they
//! can cross async boundaries and keep request/reply ownership explicit.

use std::collections::{HashMap, VecDeque};
use std::ffi::{CString, OsStr};
use std::io;
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_double, c_int, c_uint, c_ulong, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use parking_lot::{Condvar, Mutex as ParkingMutex};
use studiofs_vfs::{KernelInvalidator, StudioVfs, VfsFuseInvalidationSnapshot};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const DEFAULT_FUSE_MAX_WRITE_BYTES: u32 = 16 * 1024 * 1024;
const DEFAULT_FUSE_MAX_READAHEAD_BYTES: u32 = 16 * 1024 * 1024;
pub(crate) const MACOS_FUSE_IOSIZE_BYTES: u32 = 4 * 1024 * 1024;
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
// Bound unique invalidation targets per session; producers apply backpressure
// instead of dropping invalidations when this finite metadata budget is exhausted.
const MACFUSE_INVALIDATION_QUEUE_CAPACITY: usize = 8192;
static NEXT_INTERRUPT_KEY: AtomicUsize = AtomicUsize::new(1);
// Interrupt callbacks can race with request reply/drop after macFUSE has
// copied callback data out of its session lock. Keep callback data as an opaque
// key and shard independent requests so their registry operations can proceed
// concurrently.
type InterruptRegistry = DashMap<usize, CancellationToken, foldhash::fast::RandomState>;
static INTERRUPT_REGISTRY: OnceLock<InterruptRegistry> = OnceLock::new();

fn interrupt_registry() -> &'static InterruptRegistry {
    INTERRUPT_REGISTRY.get_or_init(|| {
        // Keys are locally generated, so the fast integer hasher is sufficient.
        DashMap::with_hasher(foldhash::fast::RandomState::default())
    })
}

fn insert_interrupt_token(key: usize, token: CancellationToken) {
    interrupt_registry().insert(key, token);
}

fn remove_interrupt_token(key: usize) {
    interrupt_registry().remove(&key);
}

fn next_interrupt_key() -> usize {
    next_interrupt_key_from(&NEXT_INTERRUPT_KEY)
}

fn next_interrupt_key_from(next_key: &AtomicUsize) -> usize {
    loop {
        let key = next_key.fetch_add(1, Ordering::Relaxed);
        if key != 0 {
            return key;
        }
    }
}

pub(crate) mod raw {
    use super::*;

    /// Low-level FUSE inode number.
    pub type FuseIno = u64;

    /// Opaque low-level FUSE request handle.
    pub enum FuseReq {}

    /// Opaque low-level FUSE session handle.
    pub enum FuseSession {}

    /// Opaque low-level FUSE channel handle.
    pub enum FuseChan {}

    /// Opaque low-level FUSE connection information handle.
    pub enum FuseConnInfo {}

    /// Opaque low-level FUSE argument vector.
    #[repr(C)]
    pub struct FuseArgs {
        /// Argument count.
        pub argc: c_int,
        /// Argument vector.
        pub argv: *mut *mut c_char,
        /// Whether libfuse owns `argv`.
        pub allocated: c_int,
    }

    /// macFUSE low-level entry parameters.
    #[repr(C)]
    pub struct FuseEntryParam {
        /// Unique inode number.
        pub ino: FuseIno,
        /// Generation number for this entry.
        pub generation: c_ulong,
        /// Inode attributes.
        pub attr: stat,
        /// Attribute validity timeout in seconds.
        pub attr_timeout: c_double,
        /// Name validity timeout in seconds.
        pub entry_timeout: c_double,
    }

    /// macFUSE low-level file-open information.
    #[repr(C)]
    #[derive(Clone, Copy)]
    pub struct FuseFileInfo {
        /// Open flags.
        pub flags: c_int,
        /// Legacy file handle slot.
        pub fh_old: c_ulong,
        /// Writepage marker.
        pub writepage: c_int,
        bitfield: c_int,
        /// File handle.
        pub fh: u64,
        /// Lock owner.
        pub lock_owner: u64,
    }

    /// Darwin-specific setattr extension payload used by macFUSE.
    #[repr(C)]
    pub struct FuseSetattrX {
        pub valid: i32,
        pub mode: mode_t,
        pub uid: libc::uid_t,
        pub gid: libc::gid_t,
        pub size: off_t,
        pub acctime: libc::timespec,
        pub modtime: libc::timespec,
        pub crtime: libc::timespec,
        pub chgtime: libc::timespec,
        pub bkuptime: libc::timespec,
        pub flags: u32,
    }

    impl FuseFileInfo {
        /// Construct test-only callback data with the fields used by wrappers.
        #[cfg(test)]
        pub fn for_test(flags: c_int, fh: u64, direct_io: bool, keep_cache: bool) -> Self {
            Self::from_fields(flags, fh, direct_io, keep_cache)
        }

        /// Mutate test-only cache flag bits.
        #[cfg(test)]
        pub fn set_cache_flags_for_test(&mut self, direct_io: bool, keep_cache: bool) {
            *self = Self::from_fields(self.flags, self.fh, direct_io, keep_cache);
        }

        pub(super) fn direct_io(self) -> bool {
            // SAFETY: `self` is an initialized C-compatible `fuse_file_info`.
            unsafe { super::studiofs_macfuse_file_info_direct_io(&self) != 0 }
        }

        pub(super) fn keep_cache(self) -> bool {
            // SAFETY: `self` is an initialized C-compatible `fuse_file_info`.
            unsafe { super::studiofs_macfuse_file_info_keep_cache(&self) != 0 }
        }

        pub(super) fn from_open_reply(reply: OpenReply) -> Self {
            Self::from_fields(0, reply.file_handle, reply.direct_io, reply.keep_cache)
        }

        fn from_fields(flags: c_int, file_handle: u64, direct_io: bool, keep_cache: bool) -> Self {
            let mut info = MaybeUninit::<Self>::uninit();
            // SAFETY: The C shim initializes the entire `fuse_file_info`,
            // including implementation-defined bitfields, using the same
            // headers/compiler as libfuse.
            unsafe {
                super::studiofs_macfuse_file_info_init(
                    info.as_mut_ptr(),
                    flags,
                    file_handle,
                    c_int::from(direct_io),
                    c_int::from(keep_cache),
                );
                info.assume_init()
            }
        }
    }

    /// macFUSE low-level callback table.
    #[repr(C)]
    pub struct FuseLowLevelOps {
        _private: [u8; 0],
    }

    #[link(name = "fuse")]
    unsafe extern "C" {
        /// Reply with a POSIX errno.
        pub fn fuse_reply_err(req: *mut FuseReq, err: c_int) -> c_int;
        /// Reply with an entry.
        pub fn fuse_reply_entry(req: *mut FuseReq, entry: *const FuseEntryParam) -> c_int;
        /// Reply with attributes.
        pub fn fuse_reply_attr(req: *mut FuseReq, attr: *const stat, timeout: c_double) -> c_int;
        /// Reply with open parameters.
        pub fn fuse_reply_open(req: *mut FuseReq, fi: *const FuseFileInfo) -> c_int;
        /// Reply with a newly created entry and its open parameters.
        pub fn fuse_reply_create(
            req: *mut FuseReq,
            entry: *const FuseEntryParam,
            fi: *const FuseFileInfo,
        ) -> c_int;
        /// Reply with a read or directory buffer.
        pub fn fuse_reply_buf(req: *mut FuseReq, buf: *const c_char, size: size_t) -> c_int;
        /// Reply with a symlink target.
        pub fn fuse_reply_readlink(req: *mut FuseReq, link: *const c_char) -> c_int;
        /// Reply with the number of written bytes.
        pub fn fuse_reply_write(req: *mut FuseReq, count: size_t) -> c_int;
        /// Reply with filesystem statistics.
        pub fn fuse_reply_statfs(req: *mut FuseReq, stat: *const statvfs) -> c_int;
        /// Reply with the required xattr buffer size.
        pub fn fuse_reply_xattr(req: *mut FuseReq, count: size_t) -> c_int;
        /// Return session user data associated with a request.
        pub fn fuse_req_userdata(req: *mut FuseReq) -> *mut c_void;
        /// Register or unregister a request interruption callback.
        pub fn fuse_req_interrupt_func(
            req: *mut FuseReq,
            func: Option<unsafe extern "C" fn(*mut FuseReq, *mut c_void)>,
            data: *mut c_void,
        );
        /// Append or size a directory entry.
        pub fn fuse_add_direntry(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            stbuf: *const stat,
            off: off_t,
        ) -> size_t;
        /// Create a low-level session from callback operations.
        pub fn fuse_lowlevel_new(
            args: *mut FuseArgs,
            op: *const FuseLowLevelOps,
            op_size: size_t,
            userdata: *mut c_void,
        ) -> *mut FuseSession;
        /// Mount a FUSE channel at a mount point.
        pub fn fuse_mount(mountpoint: *const c_char, args: *mut FuseArgs) -> *mut FuseChan;
        /// Attach a channel to a session.
        pub fn fuse_session_add_chan(session: *mut FuseSession, channel: *mut FuseChan);
        /// Remove a channel from its session.
        pub fn fuse_session_remove_chan(channel: *mut FuseChan);
        /// Destroy a low-level session and its attached channel.
        pub fn fuse_session_destroy(session: *mut FuseSession);
        /// Ask a session loop to exit.
        pub fn fuse_session_exit(session: *mut FuseSession);
        /// Run a low-level session loop on the current thread.
        pub fn fuse_session_loop(session: *mut FuseSession) -> c_int;
        /// Notify the kernel to invalidate inode cache state.
        pub fn fuse_lowlevel_notify_inval_inode(
            channel: *mut FuseChan,
            ino: FuseIno,
            off: off_t,
            len: off_t,
        ) -> c_int;
        /// Notify the kernel to invalidate a cached directory entry.
        pub fn fuse_lowlevel_notify_inval_entry(
            channel: *mut FuseChan,
            parent: FuseIno,
            name: *const c_char,
            namelen: size_t,
        ) -> c_int;
        /// Unmount a FUSE mount point and release its channel.
        pub fn fuse_unmount(mountpoint: *const c_char, channel: *mut FuseChan);
        /// Free arguments allocated by libfuse option parsing.
        pub fn fuse_opt_free_args(args: *mut FuseArgs);
    }
}

unsafe extern "C" {
    fn studiofs_macfuse_empty_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_macfuse_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_macfuse_read_only_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_macfuse_low_level_ops_size() -> size_t;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_xattr_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_read_only_xattr_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_init_callback(ops: *const raw::FuseLowLevelOps) -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_forget_callback(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_rmdir_callback(ops: *const raw::FuseLowLevelOps)
    -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_mknod_callback(ops: *const raw::FuseLowLevelOps)
    -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_link_callback(ops: *const raw::FuseLowLevelOps) -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_directory_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    fn studiofs_macfuse_conn_info_configure(
        conn: *mut raw::FuseConnInfo,
        max_write: c_uint,
        max_readahead: c_uint,
    );
    fn studiofs_macfuse_file_info_init(
        info: *mut raw::FuseFileInfo,
        flags: c_int,
        file_handle: u64,
        direct_io: c_int,
        keep_cache: c_int,
    );
    fn studiofs_macfuse_file_info_direct_io(info: *const raw::FuseFileInfo) -> c_int;
    fn studiofs_macfuse_file_info_keep_cache(info: *const raw::FuseFileInfo) -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_size_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_fh_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_mode_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_uid_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_gid_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_atime_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_mtime_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_atime_now_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_mtime_now_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_crtime_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_chgtime_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_bkuptime_mask() -> c_int;
    pub(crate) fn studiofs_macfuse_setattr_flags_mask() -> c_int;
}

/// Configure macFUSE connection-level I/O sizing for the low-level bridge.
///
/// # Safety
///
/// `conn` must be a valid macFUSE-provided `fuse_conn_info` pointer for the
/// duration of this callback, or null. macFUSE owns the pointed-to storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_macfuse_init(
    _userdata: *mut c_void,
    conn: *mut raw::FuseConnInfo,
) {
    // SAFETY: macFUSE passes a live `fuse_conn_info` pointer for this callback;
    // the C shim writes only documented connection size fields.
    unsafe {
        studiofs_macfuse_conn_info_configure(
            conn,
            DEFAULT_FUSE_MAX_WRITE_BYTES,
            DEFAULT_FUSE_MAX_READAHEAD_BYTES,
        );
    }
}

/// Low-level callback operation table provided by macFUSE headers.
#[derive(Debug, Clone, Copy)]
pub struct LowLevelOperations {
    raw: *const raw::FuseLowLevelOps,
    size: size_t,
}

impl LowLevelOperations {
    /// Empty operation table for lifecycle smoke tests.
    pub fn empty() -> Self {
        // SAFETY: The C shim returns a pointer to a static table and its exact
        // header-derived size.
        unsafe {
            Self {
                raw: studiofs_macfuse_empty_low_level_ops(),
                size: studiofs_macfuse_low_level_ops_size(),
            }
        }
    }

    /// Read-only StudioVfs callback operation table.
    pub fn read_only_studio_vfs() -> Self {
        // SAFETY: The C shim returns a pointer to a static table and its exact
        // header-derived size. Callback function pointers are Rust `extern "C"`
        // functions with signatures matching the same macFUSE header.
        unsafe {
            Self {
                raw: studiofs_macfuse_read_only_low_level_ops(),
                size: studiofs_macfuse_low_level_ops_size(),
            }
        }
    }

    /// StudioVfs callback operation table.
    pub fn studio_vfs() -> Self {
        // SAFETY: The C shim returns a pointer to a static table and its exact
        // header-derived size. Callback function pointers are Rust `extern "C"`
        // functions with signatures matching the same macFUSE header.
        unsafe {
            Self {
                raw: studiofs_macfuse_low_level_ops(),
                size: studiofs_macfuse_low_level_ops_size(),
            }
        }
    }

    pub(crate) fn as_ptr(self) -> *const raw::FuseLowLevelOps {
        self.raw
    }

    /// Header-derived callback table size.
    pub fn size(self) -> size_t {
        self.size
    }

    #[cfg(test)]
    pub(crate) fn has_xattr_callbacks(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe {
            studiofs_macfuse_low_level_ops_has_xattr_callbacks(self.raw) != 0
                || studiofs_macfuse_low_level_ops_has_read_only_xattr_callbacks(self.raw) != 0
        }
    }

    #[cfg(test)]
    pub(crate) fn has_init_callback(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_init_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_forget_callback(self) -> bool {
        // SAFETY: The C shim only inspects a callback pointer in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_forget_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_rmdir_callback(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_rmdir_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_mknod_callback(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_mknod_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_link_callback(self) -> bool {
        // SAFETY: The C shim only inspects a callback pointer in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_link_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_directory_callbacks(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_directory_callbacks(self.raw) != 0 }
    }
}

/// Owned copy of callback-scoped `fuse_file_info` data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileInfoSnapshot {
    flags: c_int,
    file_handle: u64,
    direct_io: bool,
    keep_cache: bool,
}

/// Owned mount/session configuration for low-level macFUSE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    mount_point: PathBuf,
    volume_name: String,
    read_only: bool,
    run_in_foreground: bool,
}

impl SessionConfig {
    /// Build a low-level session configuration.
    pub fn new(mount_point: impl Into<PathBuf>, volume_name: impl Into<String>) -> Self {
        Self {
            mount_point: mount_point.into(),
            volume_name: volume_name.into(),
            read_only: false,
            run_in_foreground: false,
        }
    }

    /// Set whether the filesystem is mounted read-only.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Set whether the session stays in the foreground.
    pub fn with_run_in_foreground(mut self, run_in_foreground: bool) -> Self {
        self.run_in_foreground = run_in_foreground;
        self
    }

    /// Mount point for this session.
    pub fn mount_point(&self) -> &Path {
        &self.mount_point
    }

    /// Requested volume name retained for higher-level macOS integration.
    pub fn volume_name(&self) -> &str {
        &self.volume_name
    }

    /// Whether the eventual session runner should stay in the foreground.
    pub fn run_in_foreground(&self) -> bool {
        self.run_in_foreground
    }

    /// Build owned argv strings for libfuse.
    pub fn argv_strings(&self) -> Vec<String> {
        let args = vec!["studiofs-macfuse".to_owned()];
        let _ = self.read_only;
        // `run_in_foreground` is runner policy. The macFUSE 2.x low-level
        // session constructor rejects `-f`. macFUSE 5.2.0 also rejects `ro`
        // for low-level sessions, so read-only behavior is enforced by the
        // StudioFS callback adapter rather than a kernel mount option.
        args
    }

    /// Build owned argv strings for the macFUSE mount channel.
    pub fn mount_argv_strings(&self) -> Vec<String> {
        vec![
            "studiofs-macfuse".to_owned(),
            format!("-ovolname={}", self.volume_name),
            "-olocal".to_owned(),
            "-onovncache".to_owned(),
            "-oauto_cache".to_owned(),
            format!("-oiosize={MACOS_FUSE_IOSIZE_BYTES}"),
            format!("-omax_read={MACOS_FUSE_IOSIZE_BYTES}"),
            "-odefer_permissions".to_owned(),
            "-osparse".to_owned(),
        ]
    }

    fn mount_point_cstring(&self) -> io::Result<CString> {
        CString::new(self.mount_point.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "macFUSE mount point contains an interior NUL byte",
            )
        })
    }
}

struct OwnedFuseArgs {
    _strings: Vec<CString>,
    _argv: Vec<*mut c_char>,
    raw: raw::FuseArgs,
}

impl OwnedFuseArgs {
    fn new(argv_strings: Vec<String>) -> io::Result<Self> {
        let strings = argv_strings
            .into_iter()
            .map(|arg| {
                CString::new(arg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "macFUSE option contains an interior NUL byte",
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut argv = strings
            .iter()
            .map(|arg| arg.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        let raw = raw::FuseArgs {
            argc: argv.len().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many macFUSE arguments")
            })?,
            argv: argv.as_mut_ptr(),
            allocated: 0,
        };
        Ok(Self {
            _strings: strings,
            _argv: argv,
            raw,
        })
    }

    fn as_mut_ptr(&mut self) -> *mut raw::FuseArgs {
        &mut self.raw
    }
}

impl Drop for OwnedFuseArgs {
    fn drop(&mut self) {
        // SAFETY: `self.raw` is the same `fuse_args` structure passed to
        // libfuse. If libfuse allocated a replacement argv during option
        // parsing, `fuse_opt_free_args` releases it; otherwise it is a no-op for
        // our stack-owned Rust argv (`allocated == 0`).
        unsafe { raw::fuse_opt_free_args(&mut self.raw) };
    }
}

#[cfg(test)]
static TEST_SESSION_EXIT_CALLS: AtomicUsize = AtomicUsize::new(0);

struct SessionControl {
    raw: ParkingMutex<Option<NonNull<raw::FuseSession>>>,
    invalidation_state: Arc<SessionInvalidationState>,
    invalidation_queue: Arc<SessionInvalidationQueue>,
    exit: unsafe extern "C" fn(*mut raw::FuseSession),
}

// SAFETY: The raw session is accessed only while holding `raw`; `close` takes
// the same lock before Session destroys the pointer. macFUSE supports exit
// signaling from the watcher thread.
unsafe impl Send for SessionControl {}
unsafe impl Sync for SessionControl {}

impl SessionControl {
    fn new(
        raw: NonNull<raw::FuseSession>,
        invalidation_state: Arc<SessionInvalidationState>,
        invalidation_queue: Arc<SessionInvalidationQueue>,
        exit: unsafe extern "C" fn(*mut raw::FuseSession),
    ) -> Self {
        Self {
            raw: ParkingMutex::new(Some(raw)),
            invalidation_state,
            invalidation_queue,
            exit,
        }
    }

    #[cfg(test)]
    fn for_test() -> Self {
        unsafe extern "C" fn record_exit(_: *mut raw::FuseSession) {
            TEST_SESSION_EXIT_CALLS.fetch_add(1, Ordering::Relaxed);
        }

        Self::new(
            NonNull::dangling(),
            Arc::new(SessionInvalidationState::new()),
            SessionInvalidationQueue::new(),
            record_exit,
        )
    }

    fn with_live_raw<T>(&self, operation: impl FnOnce(*mut raw::FuseSession) -> T) -> Option<T> {
        let raw = self.raw.lock();
        Some(operation((*raw)?.as_ptr()))
    }

    fn request_exit(&self) -> bool {
        self.shutdown_invalidation();
        self.with_live_raw(|raw| {
            // SAFETY: The raw pointer is protected by the control lock, which
            // Session::drop acquires before destroying the session.
            unsafe { (self.exit)(raw) };
        })
        .is_some()
    }

    fn shutdown_invalidation(&self) -> bool {
        prepare_session_shutdown(&self.invalidation_state, &self.invalidation_queue)
    }

    fn close(&self) {
        self.raw.lock().take();
    }
}

/// Mounted low-level macFUSE session.
pub struct Session {
    raw: NonNull<raw::FuseSession>,
    channel: NonNull<raw::FuseChan>,
    mount_point: CString,
    control: Arc<SessionControl>,
}

// SAFETY: macFUSE session handles support cross-thread exit signaling, and this
// wrapper only exposes shared access for `request_exit`. Teardown is serialized
// with external exit requests by SessionControl.
unsafe impl Send for Session {}
// SAFETY: See the `Send` impl above; shared references use SessionControl's
// lifetime gate for the intended cross-thread wakeup path.
unsafe impl Sync for Session {}

impl Session {
    /// Create and mount a low-level macFUSE session.
    ///
    /// # Safety
    ///
    /// `ops` must point to a valid `fuse_lowlevel_ops` table for the duration
    /// of this call. libfuse copies the table during session creation, but
    /// callback function pointers stored in the table must remain valid for the
    /// returned session lifetime.
    /// `userdata` must remain valid until the session is destroyed, unless the
    /// callback table intentionally handles a null user-data pointer.
    pub unsafe fn mount_with_ops(
        config: &SessionConfig,
        ops: LowLevelOperations,
        userdata: *mut c_void,
    ) -> io::Result<Self> {
        let mut session_args = OwnedFuseArgs::new(config.argv_strings())?;
        let mut mount_args = OwnedFuseArgs::new(config.mount_argv_strings())?;
        let mount_point = config.mount_point_cstring()?;
        // SAFETY: `args`, `ops`, and `userdata` satisfy the caller contract for
        // `fuse_lowlevel_new`; `args` and callback table live through the call.
        let session = NonNull::new(unsafe {
            raw::fuse_lowlevel_new(
                session_args.as_mut_ptr(),
                ops.as_ptr(),
                ops.size(),
                userdata,
            )
        })
        .ok_or_else(|| io::Error::other("macFUSE failed to create low-level session"))?;
        // SAFETY: `mount_point` and `args` are valid for this call.
        let channel =
            NonNull::new(unsafe { raw::fuse_mount(mount_point.as_ptr(), mount_args.as_mut_ptr()) });
        match channel {
            Some(channel) => {
                // SAFETY: Both pointers were returned by macFUSE and are live.
                unsafe { raw::fuse_session_add_chan(session.as_ptr(), channel.as_ptr()) };
                let invalidation_state = Arc::new(SessionInvalidationState::new());
                let invalidation_queue =
                    match SessionInvalidationQueue::start(channel, Arc::clone(&invalidation_state))
                    {
                        Ok(queue) => queue,
                        Err(error) => {
                            // SAFETY: The channel was just attached to this live session and
                            // no Session value was returned to the caller.
                            unsafe { raw::fuse_session_remove_chan(channel.as_ptr()) };
                            // SAFETY: `channel` is still the live channel for this mount path.
                            unsafe { raw::fuse_unmount(mount_point.as_ptr(), channel.as_ptr()) };
                            // SAFETY: `session` is still owned by this error path.
                            unsafe { raw::fuse_session_destroy(session.as_ptr()) };
                            return Err(error);
                        }
                    };
                Ok(Self {
                    raw: session,
                    channel,
                    mount_point,
                    control: Arc::new(SessionControl::new(
                        session,
                        invalidation_state,
                        invalidation_queue,
                        raw::fuse_session_exit,
                    )),
                })
            }
            None => {
                // SAFETY: `session` is live and has not been returned.
                unsafe { raw::fuse_session_destroy(session.as_ptr()) };
                Err(io::Error::other(
                    "macFUSE failed to mount low-level channel",
                ))
            }
        }
    }

    /// Request session loop shutdown.
    pub fn request_exit(&self) {
        self.control.request_exit();
    }

    /// Build a thread-safe handle that can request session loop shutdown.
    pub fn exit_handle(&self) -> SessionExitHandle {
        SessionExitHandle {
            control: Arc::clone(&self.control),
        }
    }

    /// Build a VFS kernel invalidator for this mounted session.
    pub fn kernel_invalidator(&self) -> Arc<dyn KernelInvalidator> {
        Arc::new(MacFuseKernelInvalidator {
            queue: Arc::clone(&self.control.invalidation_queue),
        })
    }

    pub(crate) fn attach_invalidation_metrics(&self, vfs: &Arc<StudioVfs>) {
        self.control.invalidation_queue.attach_metrics(vfs);
    }

    /// Run the low-level session loop on the current thread.
    pub fn run(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live session owned by this wrapper for the
        // duration of the blocking loop call.
        let result = unsafe { raw::fuse_session_loop(self.raw.as_ptr()) };
        // An external unmount returns from the loop without going through
        // SessionControl::request_exit, so stop queued kernel sends here too.
        self.control.shutdown_invalidation();
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(result.abs()))
        }
    }
}

/// Thread-safe handle used to request low-level session shutdown.
pub struct SessionExitHandle {
    control: Arc<SessionControl>,
}

impl SessionExitHandle {
    /// Ask the session loop to exit.
    ///
    /// Returns `false` when the session has already started closing.
    pub fn request_exit(&self) -> bool {
        self.control.request_exit()
    }
}

fn prepare_session_shutdown(
    invalidation_state: &SessionInvalidationState,
    invalidation_queue: &SessionInvalidationQueue,
) -> bool {
    invalidation_state.begin_shutdown();
    invalidation_queue.close();
    invalidation_state.wait_for_idle(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct InodeInvalidation {
    ino: u64,
    data: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct EntryInvalidation {
    parent: u64,
    name: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum InvalidationKey {
    Inode(u64),
    Entry { parent: u64, name: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SessionInvalidation {
    Inode(InodeInvalidation),
    Entry(EntryInvalidation),
}

struct SessionInvalidationQueue {
    state: ParkingMutex<SessionInvalidationQueueState>,
    wake: Condvar,
    worker: ParkingMutex<Option<std::thread::JoinHandle<()>>>,
    metrics_vfs: OnceLock<Weak<StudioVfs>>,
}

struct SessionInvalidationQueueState {
    closed: bool,
    pending: VecDeque<InvalidationKey>,
    kinds: HashMap<InvalidationKey, bool>,
    queue_peak: usize,
    coalesced: u64,
    snapshot_generation: u64,
    #[cfg(test)]
    waiting_producers: usize,
}

#[derive(Clone, Copy)]
struct SessionChannel(NonNull<raw::FuseChan>);

// SAFETY: The channel is owned by the mounted macFUSE session and is only used
// by the dedicated invalidation worker while SessionInvalidationState guards
// the FFI call against Session::drop.
unsafe impl Send for SessionChannel {}
unsafe impl Sync for SessionChannel {}

impl SessionInvalidationQueue {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: ParkingMutex::new(SessionInvalidationQueueState {
                closed: false,
                pending: VecDeque::new(),
                kinds: HashMap::new(),
                queue_peak: 0,
                coalesced: 0,
                snapshot_generation: 0,
                #[cfg(test)]
                waiting_producers: 0,
            }),
            wake: Condvar::new(),
            worker: ParkingMutex::new(None),
            metrics_vfs: OnceLock::new(),
        })
    }

    fn attach_metrics(&self, vfs: &Arc<StudioVfs>) {
        let _ = self.metrics_vfs.set(Arc::downgrade(vfs));
        let snapshot = {
            let mut state = self.state.lock();
            Self::next_metrics_snapshot(&mut state)
        };
        self.record_metrics(snapshot);
    }

    fn start(
        channel: NonNull<raw::FuseChan>,
        invalidation_state: Arc<SessionInvalidationState>,
    ) -> io::Result<Arc<Self>> {
        let queue = Self::new();
        let worker_queue = Arc::clone(&queue);
        let channel = SessionChannel(channel);
        let worker = std::thread::Builder::new()
            .name("studiofs-macfuse-invalidation".to_string())
            .spawn(move || {
                while let Some(invalidation) = worker_queue.next() {
                    match invalidation {
                        SessionInvalidation::Inode(invalidation) => {
                            let (offset, len) = invalidation_range(invalidation.data);
                            notify_inode_range(
                                channel,
                                &invalidation_state,
                                invalidation.ino,
                                offset,
                                len,
                            );
                        }
                        SessionInvalidation::Entry(invalidation) => notify_entry(
                            channel,
                            &invalidation_state,
                            invalidation.parent,
                            &invalidation.name,
                        ),
                    }
                }
            })?;
        *queue.worker.lock() = Some(worker);
        Ok(queue)
    }

    fn enqueue(&self, ino: u64, data: bool) {
        self.enqueue_key(InvalidationKey::Inode(ino), data);
    }

    fn enqueue_entry(&self, parent: u64, name: &[u8]) {
        self.enqueue_key(
            InvalidationKey::Entry {
                parent,
                name: name.to_vec(),
            },
            false,
        );
    }

    fn enqueue_key(&self, key: InvalidationKey, data: bool) {
        let mut state = self.state.lock();
        loop {
            if state.closed {
                return;
            }
            if let Some(kind) = state.kinds.get_mut(&key) {
                *kind |= data;
                state.coalesced = state.coalesced.saturating_add(1);
                break;
            }
            if state.pending.len() < MACFUSE_INVALIDATION_QUEUE_CAPACITY {
                state.kinds.insert(key.clone(), data);
                state.pending.push_back(key.clone());
                state.queue_peak = state.queue_peak.max(state.pending.len());
                self.wake.notify_one();
                break;
            }
            // Preserve every invalidation while bounding memory: producers
            // wait for the dedicated worker to make queue capacity available.
            #[cfg(test)]
            {
                state.waiting_producers = state.waiting_producers.saturating_add(1);
                self.wake.notify_all();
            }
            self.wake.wait(&mut state);
            #[cfg(test)]
            {
                state.waiting_producers = state.waiting_producers.saturating_sub(1);
            }
        }
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
    }

    fn next(&self) -> Option<SessionInvalidation> {
        let mut state = self.state.lock();
        loop {
            if let Some(key) = state.pending.pop_front() {
                let data = state.kinds.remove(&key).unwrap_or(false);
                self.wake.notify_one();
                let snapshot = Self::next_metrics_snapshot(&mut state);
                drop(state);
                self.record_metrics(snapshot);
                return Some(match key {
                    InvalidationKey::Inode(ino) => {
                        SessionInvalidation::Inode(InodeInvalidation { ino, data })
                    }
                    InvalidationKey::Entry { parent, name } => {
                        SessionInvalidation::Entry(EntryInvalidation { parent, name })
                    }
                });
            }
            if state.closed {
                return None;
            }
            self.wake.wait(&mut state);
        }
    }

    #[cfg(test)]
    fn wait_until_closed(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        while !state.closed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            if self.wake.wait_for(&mut state, remaining).timed_out() {
                return state.closed;
            }
        }
        true
    }

    #[cfg(test)]
    fn wait_until_producer_blocked(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();
        while state.waiting_producers == 0 {
            if self.wake.wait_until(&mut state, deadline).timed_out() {
                return state.waiting_producers > 0;
            }
        }
        true
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.pending.clear();
        state.kinds.clear();
        self.wake.notify_all();
        let snapshot = Self::next_metrics_snapshot(&mut state);
        drop(state);
        self.record_metrics(snapshot);
    }

    pub(crate) fn reset_metrics(&self) {
        let mut state = self.state.lock();
        state.queue_peak = 0;
        state.coalesced = 0;
    }

    #[cfg(test)]
    fn metrics_snapshot(&self) -> VfsFuseInvalidationSnapshot {
        let state = self.state.lock();
        Self::metrics_snapshot_from_state(&state)
    }

    fn metrics_snapshot_from_state(
        state: &SessionInvalidationQueueState,
    ) -> VfsFuseInvalidationSnapshot {
        VfsFuseInvalidationSnapshot {
            snapshot_generation: state.snapshot_generation,
            queue_depth: state.pending.len(),
            queue_peak: state.queue_peak,
            coalesced: state.coalesced,
            ..VfsFuseInvalidationSnapshot::default()
        }
    }

    fn next_metrics_snapshot(
        state: &mut SessionInvalidationQueueState,
    ) -> VfsFuseInvalidationSnapshot {
        state.snapshot_generation = state.snapshot_generation.saturating_add(1);
        Self::metrics_snapshot_from_state(state)
    }

    fn record_metrics(&self, snapshot: VfsFuseInvalidationSnapshot) {
        if let Some(metrics_vfs) = self.metrics_vfs.get()
            && let Some(vfs) = metrics_vfs.upgrade()
        {
            vfs.record_fuse_invalidation_snapshot(snapshot);
        }
    }

    fn join_worker(&self) {
        let worker = self.worker.lock().take();
        if let Some(worker) = worker
            && worker.join().is_err()
        {
            warn!("macFUSE invalidation worker panicked during shutdown");
        }
    }
}

struct MacFuseKernelInvalidator {
    queue: Arc<SessionInvalidationQueue>,
}

#[derive(Debug)]
struct SessionInvalidationState {
    status: ParkingMutex<SessionInvalidationStatus>,
    idle: Condvar,
}

#[derive(Debug)]
struct SessionInvalidationStatus {
    closed: bool,
    active: usize,
    shutdown_deadline: Option<Instant>,
}

struct SessionInvalidationGuard<'a> {
    state: &'a SessionInvalidationState,
}

impl SessionInvalidationState {
    fn new() -> Self {
        Self {
            status: ParkingMutex::new(SessionInvalidationStatus {
                closed: false,
                active: 0,
                shutdown_deadline: None,
            }),
            idle: Condvar::new(),
        }
    }

    fn begin_invalidation(&self) -> Option<SessionInvalidationGuard<'_>> {
        let mut status = self.status.lock();
        if status.closed {
            return None;
        }
        status.active += 1;
        Some(SessionInvalidationGuard { state: self })
    }

    fn begin_shutdown(&self) {
        self.status.lock().closed = true;
    }

    fn wait_for_idle(&self, timeout: Duration) -> bool {
        let mut status = self.status.lock();
        let deadline = if let Some(deadline) = status.shutdown_deadline {
            deadline
        } else {
            let now = Instant::now();
            let deadline = now.checked_add(timeout).unwrap_or(now);
            status.shutdown_deadline = Some(deadline);
            deadline
        };
        while status.active > 0 {
            if self.idle.wait_until(&mut status, deadline).timed_out() {
                return status.active == 0;
            }
        }
        true
    }
}

impl Drop for SessionInvalidationGuard<'_> {
    fn drop(&mut self) {
        let mut status = self.state.status.lock();
        status.active -= 1;
        if status.active == 0 {
            self.state.idle.notify_all();
        }
    }
}

impl KernelInvalidator for MacFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.queue.enqueue(ino, true);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.queue.enqueue(ino, false);
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) {
        self.queue.enqueue_entry(parent, name);
    }

    fn reset_diagnostics(&self) {
        self.queue.reset_metrics();
    }
}

fn invalidation_range(data: bool) -> (off_t, off_t) {
    if data { (0, 0) } else { (-1, 0) }
}

fn notify_inode_range(
    channel: SessionChannel,
    state: &SessionInvalidationState,
    ino: u64,
    offset: off_t,
    len: off_t,
) {
    let Some(_active) = state.begin_invalidation() else {
        debug!(
            ino,
            offset, len, "macFUSE inode invalidation skipped after session shutdown"
        );
        return;
    };
    // SAFETY: `channel` comes from a mounted macFUSE session. The counted
    // invalidation guard prevents `Session::drop` from destroying the raw
    // handles while this FFI call may still use the channel.
    let result =
        unsafe { raw::fuse_lowlevel_notify_inval_inode(channel.0.as_ptr(), ino, offset, len) };
    if result != 0 && result != -libc::ENOENT {
        debug!(
            ino,
            offset, len, result, "macFUSE inode invalidation failed"
        );
    }
}

fn notify_entry(
    channel: SessionChannel,
    state: &SessionInvalidationState,
    parent: u64,
    name: &[u8],
) {
    let Ok(name) = CString::new(name) else {
        debug!(
            parent,
            name_len = name.len(),
            "macFUSE entry invalidation skipped for invalid name"
        );
        return;
    };
    let Some(_active) = state.begin_invalidation() else {
        debug!(
            parent,
            name_len = name.as_bytes().len(),
            "macFUSE entry invalidation skipped after session shutdown"
        );
        return;
    };
    // SAFETY: `channel` comes from a mounted macFUSE session. The counted
    // invalidation guard prevents `Session::drop` from destroying the raw
    // channel while this FFI call may still use it.
    let result = unsafe {
        raw::fuse_lowlevel_notify_inval_entry(
            channel.0.as_ptr(),
            parent,
            name.as_ptr(),
            name.as_bytes().len(),
        )
    };
    if result != 0 && result != -libc::ENOENT {
        debug!(
            parent,
            name_len = name.as_bytes().len(),
            result,
            "macFUSE entry invalidation failed"
        );
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.control.close();
        if !self.control.shutdown_invalidation() {
            warn!(
                "macFUSE session shutdown timed out waiting for active kernel invalidation; \
                 leaking raw session handles to avoid use-after-free"
            );
            return;
        }
        self.control.invalidation_queue.join_worker();
        // SAFETY: `self.channel` was attached to `self.raw`; removing it before
        // unmount avoids leaving the session with a stale channel pointer.
        unsafe { raw::fuse_session_remove_chan(self.channel.as_ptr()) };
        // SAFETY: `self.mount_point` is the same NUL-terminated path passed to
        // `fuse_mount`, and `self.channel` is still the live channel for it.
        unsafe { raw::fuse_unmount(self.mount_point.as_ptr(), self.channel.as_ptr()) };
        // SAFETY: `self.raw` is owned by this wrapper and destroyed exactly once
        // after its channel has been detached and unmounted.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

impl FileInfoSnapshot {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "called by low-level callbacks added after this boundary"
        )
    )]
    pub(crate) fn copy_from_callback(info: &raw::FuseFileInfo) -> Self {
        let copied = *info;
        Self {
            flags: copied.flags,
            file_handle: copied.fh,
            direct_io: copied.direct_io(),
            keep_cache: copied.keep_cache(),
        }
    }

    /// Open flags from the callback.
    pub fn flags(self) -> c_int {
        self.flags
    }

    /// File handle carried by the callback.
    pub fn file_handle(self) -> u64 {
        self.file_handle
    }

    /// Whether direct I/O was requested.
    pub fn direct_io(self) -> bool {
        self.direct_io
    }

    /// Whether kernel cache should be kept.
    pub fn keep_cache(self) -> bool {
        self.keep_cache
    }
}

/// Open reply parameters owned by Rust.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenReply {
    file_handle: u64,
    direct_io: bool,
    keep_cache: bool,
}

impl OpenReply {
    /// Create an open reply.
    pub fn new(file_handle: u64) -> Self {
        Self {
            file_handle,
            direct_io: false,
            keep_cache: false,
        }
    }

    /// Set whether direct I/O should be requested.
    pub fn with_direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }

    /// Set whether the kernel should keep cached data.
    pub fn with_keep_cache(mut self, keep_cache: bool) -> Self {
        self.keep_cache = keep_cache;
        self
    }

    /// File handle returned to macFUSE.
    pub fn file_handle(self) -> u64 {
        self.file_handle
    }

    /// Whether direct I/O should be requested.
    pub fn direct_io(self) -> bool {
        self.direct_io
    }

    /// Whether kernel cache should be kept.
    pub fn keep_cache(self) -> bool {
        self.keep_cache
    }
}

/// Owned low-level request token.
pub struct Request {
    raw: Option<NonNull<raw::FuseReq>>,
    interrupt_key: Option<usize>,
}

// SAFETY: macFUSE low-level requests may be handed off to worker threads and
// replied from there. `Request` still enforces single-reply ownership through
// `Option::take` and its fallback `Drop` reply.
unsafe impl Send for Request {}

impl Request {
    /// Wrap a callback-provided request pointer.
    ///
    /// # Safety
    ///
    /// `raw` must be a live macFUSE request pointer for a callback that has not
    /// been replied to. The returned wrapper must be consumed by exactly one
    /// reply method.
    pub unsafe fn from_raw(raw: *mut raw::FuseReq) -> Option<Self> {
        NonNull::new(raw).map(|raw| Self {
            raw: Some(raw),
            interrupt_key: None,
        })
    }

    pub(crate) fn register_interrupt_token(&mut self, token: CancellationToken) {
        let Some(raw) = self.raw else {
            return;
        };
        self.clear_interrupt_token();
        let key = next_interrupt_key();
        insert_interrupt_token(key, token);
        self.interrupt_key = Some(key);
        // SAFETY: `raw` is a live low-level request. The callback data is an
        // opaque integer key, never a dereferenced pointer.
        unsafe {
            raw::fuse_req_interrupt_func(
                raw.as_ptr(),
                Some(request_interrupted),
                key as *mut c_void,
            );
        }
    }

    fn clear_interrupt_token(&mut self) {
        let Some(key) = self.interrupt_key.take() else {
            return;
        };
        if let Some(raw) = self.raw {
            // SAFETY: `raw` is still owned by this request; unregister before
            // freeing callback data so macFUSE cannot call into dangling state.
            unsafe {
                raw::fuse_req_interrupt_func(raw.as_ptr(), None, std::ptr::null_mut());
            }
        }
        remove_interrupt_token(key);
    }

    fn as_ptr(&self) -> *mut raw::FuseReq {
        match self.raw {
            Some(raw) => raw.as_ptr(),
            None => unreachable!("type-state invariant: macFUSE request was already replied"),
        }
    }

    fn take_raw(&mut self) -> NonNull<raw::FuseReq> {
        self.clear_interrupt_token();
        match self.raw.take() {
            Some(raw) => raw,
            None => unreachable!("type-state invariant: macFUSE request was already replied"),
        }
    }
}

impl Drop for Request {
    fn drop(&mut self) {
        self.clear_interrupt_token();
        if let Some(raw) = self.raw.take() {
            // SAFETY: `Request` owns a live, unreplied callback request by
            // construction. Dropping without a reply must fail the request so
            // the kernel is not left waiting indefinitely.
            unsafe { raw::fuse_reply_err(raw.as_ptr(), libc::EIO) };
        }
    }
}

unsafe extern "C" fn request_interrupted(_req: *mut raw::FuseReq, data: *mut c_void) {
    if data.is_null() {
        return;
    }
    let key = data as usize;
    // An unwind crossing this non-unwinding C ABI boundary aborts the process.
    if !catch_interrupt_callback(|| {
        let token = interrupt_registry()
            .get(&key)
            .map(|entry| entry.value().clone());
        if let Some(token) = token {
            token.cancel();
        }
    }) {
        warn!("macFUSE interrupt callback panicked; request cancellation may be incomplete");
    }
}

fn catch_interrupt_callback(callback: impl FnOnce() + std::panic::UnwindSafe) -> bool {
    std::panic::catch_unwind(callback).is_ok()
}

/// Low-level reply sender that consumes request ownership.
pub struct Reply {
    request: Request,
}

impl Reply {
    /// Create a reply sender for an owned request token.
    pub fn new(request: Request) -> Self {
        Self { request }
    }

    /// Reply with a POSIX errno.
    pub fn error(mut self, errno: c_int) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `Request` guarantees a live, unreplied low-level request and
        // this method consumes it, so macFUSE sees at most one reply.
        reply_result(unsafe { raw::fuse_reply_err(request.as_ptr(), errno) })
    }

    /// Reply with an entry.
    pub fn entry(mut self, entry: &Entry) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `entry.raw` points to initialized C-compatible data for the
        // duration of the call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_entry(request.as_ptr(), &entry.raw) })
    }

    /// Reply with attributes.
    pub fn attr(mut self, attr: &stat, timeout: f64) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `attr` is initialized and borrowed for the duration of the
        // call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_attr(request.as_ptr(), attr, timeout) })
    }

    /// Reply with open parameters.
    pub fn open(mut self, open: OpenReply) -> io::Result<()> {
        let request = self.request.take_raw();
        let file_info = raw::FuseFileInfo::from_open_reply(open);
        // SAFETY: `file_info` is initialized and borrowed for the duration of
        // the call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_open(request.as_ptr(), &file_info) })
    }

    /// Reply with a newly created entry and open parameters.
    pub fn create(mut self, entry: &Entry, open: OpenReply) -> io::Result<()> {
        let request = self.request.take_raw();
        let file_info = raw::FuseFileInfo::from_open_reply(open);
        // SAFETY: `entry` and `file_info` are initialized and borrowed for the
        // duration of the call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_create(request.as_ptr(), &entry.raw, &file_info) })
    }

    /// Reply with a read buffer.
    pub fn read_buffer(mut self, bytes: &[u8]) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `bytes` is valid for the duration of the call and macFUSE
        // copies/sends it synchronously; `self` consumes the single reply.
        reply_result(unsafe {
            raw::fuse_reply_buf(
                request.as_ptr(),
                bytes.as_ptr().cast::<c_char>(),
                bytes.len(),
            )
        })
    }

    /// Reply with a symbolic link target.
    pub fn readlink(mut self, target: &std::ffi::CStr) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `target` is NUL-terminated and valid for the duration of the
        // call; macFUSE copies/sends it synchronously and `self` is consumed.
        reply_result(unsafe { raw::fuse_reply_readlink(request.as_ptr(), target.as_ptr()) })
    }

    /// Reply with a write byte count.
    pub fn write_count(mut self, count: usize) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `self` owns a live, unreplied request and is consumed here.
        reply_result(unsafe { raw::fuse_reply_write(request.as_ptr(), count) })
    }

    /// Reply with owned directory entries.
    pub fn directory_entries(self, entries: &DirectoryEntryBuffer) -> io::Result<()> {
        self.read_buffer(entries.as_slice())
    }

    /// Reply with filesystem statistics.
    pub fn statfs(mut self, stat: &statvfs) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `stat` is initialized and borrowed for the duration of the
        // call; `self` consumes the single request reply.
        reply_result(unsafe { raw::fuse_reply_statfs(request.as_ptr(), stat) })
    }

    /// Reply with the required xattr value/list buffer size.
    pub fn xattr_size(mut self, count: usize) -> io::Result<()> {
        let request = self.request.take_raw();
        // SAFETY: `self` owns a live, unreplied request and is consumed here.
        reply_result(unsafe { raw::fuse_reply_xattr(request.as_ptr(), count) })
    }
}

/// Owned low-level entry reply data.
pub struct Entry {
    raw: raw::FuseEntryParam,
}

impl Entry {
    /// Create entry reply data.
    pub fn new(
        ino: u64,
        generation: c_ulong,
        attr: stat,
        attr_timeout: f64,
        entry_timeout: f64,
    ) -> Self {
        Self {
            raw: raw::FuseEntryParam {
                ino,
                generation,
                attr,
                attr_timeout,
                entry_timeout,
            },
        }
    }
}

/// Owned readdir reply buffer.
pub struct DirectoryEntryBuffer {
    words: Vec<MaybeUninit<u64>>,
    len: usize,
    scratch_name: Vec<u8>,
    capacity: usize,
}

impl DirectoryEntryBuffer {
    /// Create a directory entry buffer with a fixed maximum size.
    pub fn new(capacity: usize) -> Self {
        Self {
            words: Vec::with_capacity(capacity.div_ceil(std::mem::size_of::<u64>())),
            len: 0,
            scratch_name: Vec::with_capacity(256),
            capacity,
        }
    }

    /// Current buffer length.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Borrow the initialized directory entry bytes.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.len` bytes from `words.as_ptr()` have been initialized
        // by `push`, `push_for_test`, or are zero bytes for an empty buffer.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }

    /// Append an entry using macFUSE's layout helper.
    pub fn push(
        &mut self,
        request: &Request,
        name: &OsStr,
        attr: &stat,
        next_offset: off_t,
    ) -> Result<(), DirectoryEntryError> {
        let name_bytes = name.as_bytes();
        if name_bytes.contains(&0) {
            return Err(DirectoryEntryError::InteriorNul);
        }
        self.scratch_name.clear();
        self.scratch_name.extend_from_slice(name_bytes);
        self.scratch_name.push(0);
        let available = self.capacity.saturating_sub(self.len);
        // SAFETY: Passing a null buffer with zero size asks macFUSE for the
        // required entry size. `scratch_name` and `attr` remain valid for the
        // call.
        let required = unsafe {
            raw::fuse_add_direntry(
                request.as_ptr(),
                std::ptr::null_mut(),
                0,
                self.scratch_name.as_ptr().cast::<c_char>(),
                attr,
                next_offset,
            )
        };
        if required > available {
            return Err(DirectoryEntryError::InsufficientCapacity {
                required,
                available,
            });
        }

        let start = self.len;
        let new_len = start + required;
        let required_words = new_len.div_ceil(std::mem::size_of::<u64>());
        debug_assert!(required_words <= self.words.capacity());
        // SAFETY: the capacity check above ensures enough spare capacity for
        // `required` bytes in the 8-byte-aligned word buffer.
        // `fuse_add_direntry` writes at most the provided buffer length, after
        // which `set_len` marks the initialized bytes.
        unsafe {
            raw::fuse_add_direntry(
                request.as_ptr(),
                self.words
                    .as_mut_ptr()
                    .cast::<u8>()
                    .add(start)
                    .cast::<c_char>(),
                required,
                self.scratch_name.as_ptr().cast::<c_char>(),
                attr,
                next_offset,
            );
            self.words.set_len(required_words);
            self.len = new_len;
        }
        Ok(())
    }

    /// Test-only append that preserves owned names without a live request.
    ///
    /// This does not produce a valid `fuse_dirent` binary layout. It exists only
    /// to exercise owned-buffer capacity checks and name retention without a
    /// live macFUSE request handle.
    #[cfg(test)]
    pub fn push_for_test(
        &mut self,
        ino: u64,
        name: &OsStr,
        next_offset: off_t,
    ) -> Result<(), DirectoryEntryError> {
        let name = CString::new(name.as_bytes()).map_err(|_| DirectoryEntryError::InteriorNul)?;
        let bytes = name.as_bytes_with_nul();
        let required = std::mem::size_of::<u64>() + std::mem::size_of::<off_t>() + bytes.len();
        let available = self.capacity.saturating_sub(self.len);
        if required > available {
            return Err(DirectoryEntryError::InsufficientCapacity {
                required,
                available,
            });
        }
        self.extend_for_test(&ino.to_ne_bytes());
        self.extend_for_test(&next_offset.to_ne_bytes());
        self.extend_for_test(bytes);
        Ok(())
    }

    #[cfg(test)]
    fn extend_for_test(&mut self, bytes: &[u8]) {
        let start = self.len;
        let new_len = start + bytes.len();
        let required_words = new_len.div_ceil(std::mem::size_of::<u64>());
        if required_words > self.words.len() {
            self.words.resize_with(required_words, MaybeUninit::uninit);
        }
        // SAFETY: The word buffer has enough 8-byte-aligned storage and the
        // copied range is initialized immediately.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.words.as_mut_ptr().cast::<u8>().add(start),
                bytes.len(),
            );
        }
        self.len = new_len;
    }
}

/// Directory-entry buffer construction error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DirectoryEntryError {
    /// Entry name contains an interior NUL byte.
    #[error("directory entry name contains an interior NUL byte")]
    InteriorNul,
    /// Entry does not fit in the remaining reply buffer.
    #[error("directory entry requires {required} bytes, but only {available} remain")]
    InsufficientCapacity {
        /// Required entry bytes.
        required: usize,
        /// Remaining buffer bytes.
        available: usize,
    },
}

fn reply_result(result: c_int) -> io::Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn session_exit_handle_repeated_request_is_forwarded_while_session_is_live() {
        TEST_SESSION_EXIT_CALLS.store(0, Ordering::Relaxed);
        let control = Arc::new(SessionControl::for_test());
        let handle = SessionExitHandle {
            control: Arc::clone(&control),
        };

        assert!(handle.request_exit());
        assert!(handle.request_exit());
        assert_eq!(TEST_SESSION_EXIT_CALLS.load(Ordering::Relaxed), 2);

        control.close();
        assert!(!handle.request_exit());
    }

    #[test]
    fn session_exit_handle_repeated_request_after_session_drop_is_a_noop() {
        let control = Arc::new(SessionControl::for_test());
        let handle = SessionExitHandle {
            control: Arc::clone(&control),
        };
        control.close();

        assert!(!handle.request_exit());
        assert!(!handle.request_exit());
    }

    #[test]
    fn session_control_close_waits_for_an_in_flight_request() {
        let control = Arc::new(SessionControl::for_test());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let request_control = Arc::clone(&control);
        let request = std::thread::spawn(move || {
            request_control.with_live_raw(|_| {
                entered_tx.send(()).expect("signal request entry");
                release_rx.recv().expect("wait for request release");
            });
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("request should enter the raw-session gate");
        let close_control = Arc::clone(&control);
        let (closed_tx, closed_rx) = mpsc::channel();
        let close = std::thread::spawn(move || {
            close_control.close();
            closed_tx.send(()).expect("signal session close");
        });

        assert!(
            closed_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "session close must wait for an in-flight raw-session request"
        );
        release_tx.send(()).expect("release raw-session request");

        request.join().expect("request thread should exit");
        close.join().expect("close thread should exit");
        assert!(control.with_live_raw(|_| ()).is_none());
    }

    #[test]
    fn session_control_close_does_not_wait_for_shutdown_preparation() {
        let control = Arc::new(SessionControl::for_test());
        let active = control
            .invalidation_state
            .begin_invalidation()
            .expect("invalidation should start while session is live");
        let request_control = Arc::clone(&control);
        let request = std::thread::spawn(move || request_control.request_exit());

        assert!(
            control
                .invalidation_queue
                .wait_until_closed(Duration::from_secs(1)),
            "exit request should start shutdown preparation"
        );
        let close_control = Arc::clone(&control);
        let (closed_tx, closed_rx) = mpsc::channel();
        let close = std::thread::spawn(move || {
            close_control.close();
            closed_tx.send(()).expect("signal session close");
        });

        closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("session close must not wait for shutdown preparation");
        drop(active);

        assert!(
            !request.join().expect("exit request should finish"),
            "a request superseded by session close must not reach macFUSE"
        );
        close.join().expect("close thread should exit");
    }

    #[test]
    fn session_invalidation_state_closes_future_invalidations() {
        let state = SessionInvalidationState::new();
        assert!(state.begin_invalidation().is_some());

        state.begin_shutdown();

        assert!(
            state.begin_invalidation().is_none(),
            "closed sessions must not allow later kernel invalidation FFI calls"
        );
    }

    #[test]
    fn session_invalidation_state_shutdown_does_not_wait_for_active_invalidation() {
        let state = Arc::new(SessionInvalidationState::new());
        let active = state
            .begin_invalidation()
            .expect("invalidation should start while session is active");
        let concurrent_active = state
            .begin_invalidation()
            .expect("concurrent invalidations should share the active session gate");
        let (closed_tx, closed_rx) = mpsc::channel();
        let shutdown_state = Arc::clone(&state);

        let shutdown = std::thread::spawn(move || {
            let started = Instant::now();
            shutdown_state.begin_shutdown();
            closed_tx
                .send(started.elapsed())
                .expect("signal shutdown close");
        });

        let elapsed = closed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown should close without waiting for active invalidation guards");
        assert!(
            elapsed < Duration::from_millis(50),
            "shutdown should not block behind active invalidation guards"
        );
        assert!(
            state.begin_invalidation().is_none(),
            "closed sessions must reject invalidations even while older guards are alive"
        );

        drop(active);
        drop(concurrent_active);
        shutdown.join().expect("shutdown thread should exit");
    }

    #[test]
    fn session_invalidation_state_wait_for_idle_waits_for_active_guard_to_drop() {
        let state = Arc::new(SessionInvalidationState::new());
        let active = state
            .begin_invalidation()
            .expect("invalidation should start while session is active");
        let waiter_state = Arc::clone(&state);
        let (result_tx, result_rx) = mpsc::channel();

        let waiter = std::thread::spawn(move || {
            let waited = waiter_state.wait_for_idle(Duration::from_secs(1));
            result_tx.send(waited).expect("signal waiter result");
        });

        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "wait_for_idle should remain blocked while an invalidation guard is active"
        );
        drop(active);

        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("wait_for_idle should finish after active guard drops"),
            "wait_for_idle should succeed once active invalidation guards drop"
        );
        waiter.join().expect("waiter thread should exit");
    }

    #[test]
    fn session_invalidation_state_wait_for_idle_times_out_while_guard_is_active() {
        let state = SessionInvalidationState::new();
        let active = state
            .begin_invalidation()
            .expect("invalidation should start while session is active");
        let started = Instant::now();

        assert!(
            !state.wait_for_idle(Duration::from_millis(20)),
            "wait_for_idle should time out while an invalidation guard is active"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(20),
            "wait_for_idle should not return before the configured timeout"
        );

        drop(active);
    }

    #[test]
    fn prepare_session_shutdown_closes_invalidation_before_waiting_for_idle() {
        let state = Arc::new(SessionInvalidationState::new());
        let queue = SessionInvalidationQueue::new();
        let active = state
            .begin_invalidation()
            .expect("invalidation should start while session is active");
        let waiter_state = Arc::clone(&state);
        let waiter_queue = Arc::clone(&queue);

        let waiter =
            std::thread::spawn(move || prepare_session_shutdown(&waiter_state, &waiter_queue));

        assert!(
            queue.wait_until_closed(Duration::from_secs(1)),
            "shutdown helper must close the invalidation queue promptly"
        );
        assert!(
            queue.next().is_none(),
            "shutdown must close and clear queued invalidations"
        );
        assert!(
            state.begin_invalidation().is_none(),
            "shutdown must close new invalidations before waiting for active work"
        );
        drop(active);

        assert!(
            waiter.join().expect("shutdown waiter should not panic"),
            "shutdown should complete after the active invalidation guard drops"
        );
    }

    #[test]
    fn session_loop_shutdown_closes_invalidation_before_reply_drain() {
        let control = SessionControl::for_test();
        control.invalidation_queue.enqueue(7, true);

        control.shutdown_invalidation();

        assert!(
            control.invalidation_queue.next().is_none(),
            "session-loop shutdown must discard queued invalidations"
        );
        assert!(
            control.invalidation_state.begin_invalidation().is_none(),
            "session-loop shutdown must reject new invalidations"
        );
    }

    #[test]
    fn session_invalidation_state_reuses_shutdown_wait_deadline() {
        let state = SessionInvalidationState::new();
        let active = state
            .begin_invalidation()
            .expect("invalidation should start while session is active");
        state.begin_shutdown();

        assert!(!state.wait_for_idle(Duration::from_millis(20)));

        let started = Instant::now();
        assert!(!state.wait_for_idle(Duration::from_secs(1)));
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "a repeated shutdown wait must not extend the original deadline"
        );

        drop(active);
    }

    #[test]
    fn session_invalidation_queue_coalesces_inode_requests() {
        let queue = SessionInvalidationQueue::new();

        queue.enqueue(7, false);
        queue.enqueue(7, false);
        queue.enqueue(8, false);
        queue.enqueue(7, true);

        assert_eq!(
            queue.next(),
            Some(SessionInvalidation::Inode(InodeInvalidation {
                ino: 7,
                data: true,
            }))
        );
        assert_eq!(
            queue.next(),
            Some(SessionInvalidation::Inode(InodeInvalidation {
                ino: 8,
                data: false,
            }))
        );
    }

    #[test]
    fn session_invalidation_queue_coalesces_entry_requests() {
        let queue = SessionInvalidationQueue::new();

        queue.enqueue_entry(7, b"recovery.bin");
        queue.enqueue_entry(7, b"recovery.bin");

        assert_eq!(
            queue.next(),
            Some(SessionInvalidation::Entry(EntryInvalidation {
                parent: 7,
                name: b"recovery.bin".to_vec(),
            }))
        );
        assert_eq!(queue.metrics_snapshot().coalesced, 1);
    }

    #[test]
    fn macfuse_data_invalidation_uses_zero_length_for_the_whole_cache() {
        assert_eq!(invalidation_range(true), (0, 0));
        assert_eq!(invalidation_range(false), (-1, 0));
    }

    #[test]
    fn session_invalidation_queue_reports_depth_peak_and_coalescing() {
        let queue = SessionInvalidationQueue::new();

        queue.enqueue(7, false);
        queue.enqueue(7, true);
        queue.enqueue(8, false);

        let metrics = queue.metrics_snapshot();
        assert_eq!(metrics.queue_depth, 2);
        assert_eq!(metrics.queue_peak, 2);
        assert_eq!(metrics.coalesced, 1);
    }

    #[test]
    fn session_invalidation_queue_resets_peak_and_coalescing_metrics() {
        let queue = SessionInvalidationQueue::new();
        let invalidator = MacFuseKernelInvalidator {
            queue: Arc::clone(&queue),
        };

        invalidator.inval_inode_attrs(7);
        invalidator.inval_inode(7);
        invalidator.inval_inode_attrs(8);

        invalidator.reset_diagnostics();
        invalidator.inval_inode_attrs(9);

        let metrics = queue.metrics_snapshot();
        assert_eq!(metrics.queue_depth, 3);
        assert_eq!(metrics.queue_peak, 3);
        assert_eq!(metrics.coalesced, 0);
    }

    #[test]
    fn session_invalidation_queue_backpressures_distinct_inodes_at_capacity() {
        let queue = SessionInvalidationQueue::new();
        for ino in 0..MACFUSE_INVALIDATION_QUEUE_CAPACITY as u64 {
            queue.enqueue(ino, false);
        }
        {
            let state = queue.state.lock();
            assert_eq!(state.pending.len(), MACFUSE_INVALIDATION_QUEUE_CAPACITY);
            assert_eq!(state.kinds.len(), MACFUSE_INVALIDATION_QUEUE_CAPACITY);
        }

        let producer_queue = Arc::clone(&queue);
        let (finished_tx, finished_rx) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            producer_queue.enqueue(MACFUSE_INVALIDATION_QUEUE_CAPACITY as u64, false);
            finished_tx
                .send(())
                .expect("signal blocked producer finish");
        });
        assert!(
            queue.wait_until_producer_blocked(Duration::from_secs(1)),
            "producer should enter the full-queue wait"
        );
        assert!(
            finished_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "a full queue must apply producer backpressure"
        );

        assert_eq!(
            queue.next(),
            Some(SessionInvalidation::Inode(InodeInvalidation {
                ino: 0,
                data: false,
            }))
        );
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("producer should continue after queue capacity is freed");
        producer.join().expect("blocked producer should exit");
    }

    #[test]
    fn session_invalidation_queue_closes_without_leaving_pending_requests() {
        let queue = SessionInvalidationQueue::new();
        queue.enqueue(7, true);

        queue.close();

        assert_eq!(queue.next(), None);
    }

    fn invoke_interrupt_callback(key: usize) {
        // SAFETY: The callback treats `data` as an opaque integer key and does
        // not dereference either pointer.
        unsafe { request_interrupted(std::ptr::null_mut(), key as *mut c_void) };
    }

    #[test]
    fn interrupt_callback_boundary_contains_unwind() {
        assert!(!catch_interrupt_callback(|| {
            std::panic::resume_unwind(Box::new("simulated callback panic"));
        }));
    }

    #[test]
    fn interrupt_registry_callback_cancels_registered_token() {
        let key = next_interrupt_key();
        let token = CancellationToken::new();
        insert_interrupt_token(key, token.clone());

        invoke_interrupt_callback(key);

        assert!(token.is_cancelled());
        remove_interrupt_token(key);
    }

    #[test]
    fn interrupt_registry_removal_makes_later_callback_a_noop() {
        let key = next_interrupt_key();
        let token = CancellationToken::new();
        insert_interrupt_token(key, token.clone());
        remove_interrupt_token(key);

        invoke_interrupt_callback(key);

        assert!(!token.is_cancelled());
    }

    #[test]
    fn next_interrupt_key_skips_zero_after_wrap() {
        let next_key = AtomicUsize::new(usize::MAX);

        assert_eq!(next_interrupt_key_from(&next_key), usize::MAX);
        assert_eq!(next_interrupt_key_from(&next_key), 1);
    }

    #[test]
    fn interrupt_registry_callback_and_removal_are_race_safe() {
        for _ in 0..64 {
            let key = next_interrupt_key();
            let token = CancellationToken::new();
            insert_interrupt_token(key, token);
            let start = Arc::new(std::sync::Barrier::new(3));

            let callback_start = Arc::clone(&start);
            let callback = std::thread::spawn(move || {
                callback_start.wait();
                invoke_interrupt_callback(key);
            });

            let removal_start = Arc::clone(&start);
            let removal = std::thread::spawn(move || {
                removal_start.wait();
                remove_interrupt_token(key);
            });

            start.wait();
            callback.join().expect("interrupt callback should complete");
            removal.join().expect("registry removal should complete");
            assert!(
                interrupt_registry().get(&key).is_none(),
                "interrupt registry retained key {key} after concurrent removal"
            );
        }
    }

    fn measure_interrupt_registry_sample<const WITH_CALLBACK: bool>(
        workers: usize,
        requests_per_worker: usize,
    ) -> Duration {
        let ready_barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let start_barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let done_barrier = Arc::new(std::sync::Barrier::new(workers + 1));
        let mut threads = Vec::with_capacity(workers);
        for _ in 0..workers {
            let ready_barrier = Arc::clone(&ready_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let done_barrier = Arc::clone(&done_barrier);
            threads.push(std::thread::spawn(move || {
                ready_barrier.wait();
                start_barrier.wait();
                for _ in 0..requests_per_worker {
                    let key = next_interrupt_key();
                    let token = CancellationToken::new();
                    insert_interrupt_token(key, token.clone());
                    if WITH_CALLBACK {
                        invoke_interrupt_callback(key);
                        std::hint::black_box(token.is_cancelled());
                    }
                    remove_interrupt_token(key);
                }
                done_barrier.wait();
            }));
        }

        ready_barrier.wait();
        let started = Instant::now();
        start_barrier.wait();
        done_barrier.wait();
        let elapsed = started.elapsed();
        for thread in threads {
            thread.join().expect("benchmark worker should complete");
        }
        elapsed
    }

    #[test]
    #[ignore = "manual release-mode registry contention measurement"]
    fn interrupt_registry_contention_benchmark() {
        const REQUESTS_PER_WORKER: usize = 25_000;
        const SAMPLES: usize = 7;

        for workers in [1, 2, 4, 8] {
            let _ = measure_interrupt_registry_sample::<false>(workers, REQUESTS_PER_WORKER);
            let _ = measure_interrupt_registry_sample::<true>(workers, REQUESTS_PER_WORKER);
            let mut elapsed_samples = [Vec::with_capacity(SAMPLES), Vec::with_capacity(SAMPLES)];

            for sample in 0..SAMPLES {
                let workloads = if sample % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                };
                for with_callback in workloads {
                    let elapsed = if with_callback {
                        measure_interrupt_registry_sample::<true>(workers, REQUESTS_PER_WORKER)
                    } else {
                        measure_interrupt_registry_sample::<false>(workers, REQUESTS_PER_WORKER)
                    };
                    elapsed_samples[usize::from(with_callback)].push(elapsed);
                }
            }

            for (workload, mut elapsed_samples) in ["register-remove", "register-callback-remove"]
                .into_iter()
                .zip(elapsed_samples)
            {
                elapsed_samples.sort_unstable();
                let median = elapsed_samples[SAMPLES / 2];
                let requests = workers * REQUESTS_PER_WORKER;
                println!(
                    "workload={workload} workers={workers} median_requests_per_second={:.0} median_elapsed_ms={:.3}",
                    requests as f64 / median.as_secs_f64(),
                    median.as_secs_f64() * 1_000.0,
                );
            }
        }
    }
}
