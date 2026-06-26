//! Safe wrappers around the macFUSE low-level C API.
//!
//! This module is the only place in `studiofs-macfuse` that declares raw
//! low-level macFUSE FFI. Public types copy callback-owned values before they
//! can cross async boundaries and keep request/reply ownership explicit.

use std::collections::HashMap;
use std::ffi::{CString, OsStr};
use std::io;
use std::mem::MaybeUninit;
use std::os::raw::{c_char, c_double, c_int, c_uint, c_ulong, c_void};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use libc::{mode_t, off_t, size_t, stat, statvfs};
use parking_lot::{Condvar, Mutex as ParkingMutex};
use studiofs_vfs::KernelInvalidator;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

const DEFAULT_FUSE_MAX_WRITE_BYTES: u32 = 16 * 1024 * 1024;
const DEFAULT_FUSE_MAX_READAHEAD_BYTES: u32 = 16 * 1024 * 1024;
pub(crate) const MACOS_FUSE_IOSIZE_BYTES: u32 = 4 * 1024 * 1024;
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(10);
static NEXT_INTERRUPT_KEY: AtomicUsize = AtomicUsize::new(1);
// Interrupt callbacks can race with request reply/drop after macFUSE has
// copied callback data out of its session lock. This registry keeps callback
// data as an opaque key instead of a Rust pointer; the mutex serializes cancel
// lookup/removal at the cost of lower throughput under heavy interrupt load.
static INTERRUPT_REGISTRY: OnceLock<ParkingMutex<HashMap<usize, CancellationToken>>> =
    OnceLock::new();

fn interrupt_registry() -> &'static ParkingMutex<HashMap<usize, CancellationToken>> {
    INTERRUPT_REGISTRY.get_or_init(|| ParkingMutex::new(HashMap::new()))
}

fn next_interrupt_key() -> usize {
    NEXT_INTERRUPT_KEY.fetch_add(1, Ordering::Relaxed)
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
    fn studiofs_macfuse_low_level_ops_has_rmdir_callback(ops: *const raw::FuseLowLevelOps)
    -> c_int;
    #[cfg(test)]
    fn studiofs_macfuse_low_level_ops_has_mknod_callback(ops: *const raw::FuseLowLevelOps)
    -> c_int;
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
    pub(crate) fn has_rmdir_callback(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_rmdir_callback(self.raw) != 0 }
    }

    #[cfg(test)]
    pub(crate) fn has_mknod_callback(self) -> bool {
        // SAFETY: The C shim only inspects callback pointers in a static table.
        unsafe { studiofs_macfuse_low_level_ops_has_mknod_callback(self.raw) != 0 }
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
            "-odirect_io".to_owned(),
            "-onolocalcaches".to_owned(),
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

/// Mounted low-level macFUSE session.
pub struct Session {
    raw: NonNull<raw::FuseSession>,
    channel: NonNull<raw::FuseChan>,
    mount_point: CString,
    invalidation_state: Arc<SessionInvalidationState>,
}

// SAFETY: macFUSE session handles support cross-thread exit signaling, and this
// wrapper only exposes shared access for `request_exit`. Teardown still requires
// owning `Session`, so `Drop` cannot race active shared borrows in safe Rust.
unsafe impl Send for Session {}
// SAFETY: See the `Send` impl above; shared references expose only
// `fuse_session_exit`, which is the intended cross-thread wakeup path.
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
                Ok(Self {
                    raw: session,
                    channel,
                    mount_point,
                    invalidation_state: Arc::new(SessionInvalidationState::new()),
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
        // SAFETY: `self.raw` is owned by this wrapper and remains live until Drop.
        unsafe { raw::fuse_session_exit(self.raw.as_ptr()) };
    }

    /// Build a thread-safe handle that can request session loop shutdown.
    pub fn exit_handle(&self) -> SessionExitHandle {
        SessionExitHandle { raw: self.raw }
    }

    /// Build a VFS kernel invalidator for this mounted session.
    pub fn kernel_invalidator(&self) -> Arc<dyn KernelInvalidator> {
        Arc::new(MacFuseKernelInvalidator {
            channel: self.channel,
            state: Arc::clone(&self.invalidation_state),
        })
    }

    /// Run the low-level session loop on the current thread.
    pub fn run(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live session owned by this wrapper for the
        // duration of the blocking loop call.
        let result = unsafe { raw::fuse_session_loop(self.raw.as_ptr()) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::from_raw_os_error(result.abs()))
        }
    }
}

/// Thread-safe handle used to request low-level session shutdown.
#[derive(Clone, Copy, Debug)]
pub struct SessionExitHandle {
    raw: NonNull<raw::FuseSession>,
}

// SAFETY: `SessionExitHandle` is created only from a live `Session` and is used
// by watcher threads that are joined before the session is dropped. The handle
// exposes only `fuse_session_exit`, macFUSE's cross-thread shutdown request.
unsafe impl Send for SessionExitHandle {}
// SAFETY: Shared access cannot mutate Rust-owned state; see `Send` invariant.
unsafe impl Sync for SessionExitHandle {}

impl SessionExitHandle {
    /// Ask the session loop to exit.
    pub fn request_exit(self) {
        // SAFETY: the handle is created from a live `Session` and used only by
        // watchers that are joined before that session is dropped.
        unsafe { raw::fuse_session_exit(self.raw.as_ptr()) };
    }
}

struct MacFuseKernelInvalidator {
    channel: NonNull<raw::FuseChan>,
    state: Arc<SessionInvalidationState>,
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
        let now = Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
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

// SAFETY: Invalidators may be cloned into VFS worker tasks, but every macFUSE
// notify call must hold a counted `SessionInvalidationGuard`. `Session::drop`
// closes the gate to new invalidations, waits for active guards only up to a
// bounded timeout, and leaks the raw macFUSE handles instead of destroying them
// while an FFI call may still be using the channel.
unsafe impl Send for MacFuseKernelInvalidator {}
// SAFETY: Concurrent invalidation calls are counted against session teardown by
// `state`; each call otherwise goes through macFUSE's channel API.
unsafe impl Sync for MacFuseKernelInvalidator {}

impl KernelInvalidator for MacFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.inval_inode_range(ino, 0, -1);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.inval_inode_range(ino, -1, 0);
    }
}

impl MacFuseKernelInvalidator {
    fn inval_inode_range(&self, ino: u64, offset: off_t, len: off_t) {
        let Some(_active) = self.state.begin_invalidation() else {
            debug!(
                ino,
                offset, len, "macFUSE inode invalidation skipped after session shutdown"
            );
            return;
        };
        // SAFETY: `channel` comes from a mounted macFUSE session. The counted
        // invalidation guard prevents `Session::drop` from destroying the raw
        // handles while this FFI call may still use the channel.
        let result = unsafe {
            raw::fuse_lowlevel_notify_inval_inode(self.channel.as_ptr(), ino, offset, len)
        };
        if result != 0 && result != -libc::ENOENT {
            debug!(
                ino,
                offset, len, result, "macFUSE inode invalidation failed"
            );
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.invalidation_state.begin_shutdown();
        if !self
            .invalidation_state
            .wait_for_idle(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT)
        {
            warn!(
                "macFUSE session shutdown timed out waiting for active kernel invalidation; \
                 leaking raw session handles to avoid use-after-free"
            );
            return;
        }
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
        interrupt_registry().lock().insert(key, token);
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
        interrupt_registry().lock().remove(&key);
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
    if let Some(token) = interrupt_registry().lock().get(&key) {
        token.cancel();
    }
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
}
