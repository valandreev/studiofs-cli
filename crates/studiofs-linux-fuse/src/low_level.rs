use super::*;
use std::ffi::{CStr, OsStr};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use libc::{mode_t, off_t, size_t, stat, statvfs};
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{
    DirEntryWithAttr, KernelInvalidator, LocalSyncPolicy, OpenFlags, StatFs, StudioVfs,
};
use tokio::runtime::Handle;
use tracing::{debug, warn};

const TTL_SECONDS: f64 = 1.0;
const LINUX_FUSE_BLOCK_SIZE: u32 = 4096;
const LINUX_FUSE_MAX_READ_SIZE: usize = 4 * 1024 * 1024;
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const FUSE_SET_ATTR_MODE: c_int = 1 << 0;
const FUSE_SET_ATTR_UID: c_int = 1 << 1;
const FUSE_SET_ATTR_GID: c_int = 1 << 2;
const FUSE_SET_ATTR_SIZE: c_int = 1 << 3;
const FUSE_SET_ATTR_ATIME: c_int = 1 << 4;
const FUSE_SET_ATTR_MTIME: c_int = 1 << 5;
const FUSE_SET_ATTR_ATIME_NOW: c_int = 1 << 7;
const FUSE_SET_ATTR_MTIME_NOW: c_int = 1 << 8;

pub(crate) mod raw {
    use super::*;

    pub(crate) type FuseIno = u64;

    pub(crate) enum FuseReq {}
    pub(crate) enum FuseSession {}

    #[repr(C)]
    pub(crate) struct FuseArgs {
        pub(crate) argc: c_int,
        pub(crate) argv: *mut *mut c_char,
        pub(crate) allocated: c_int,
    }

    #[repr(C)]
    pub(crate) struct FuseLowLevelOps {
        _private: [u8; 0],
    }

    #[repr(C)]
    pub(crate) struct FuseEntryParam {
        pub(crate) ino: FuseIno,
        pub(crate) generation: u64,
        pub(crate) attr: stat,
        pub(crate) attr_timeout: f64,
        pub(crate) entry_timeout: f64,
    }

    #[repr(C)]
    pub(crate) struct FuseFileInfo {
        _private: [u8; 0],
    }

    #[link(name = "fuse3")]
    unsafe extern "C" {
        pub(crate) fn fuse_reply_err(req: *mut FuseReq, err: c_int) -> c_int;
        pub(crate) fn fuse_reply_entry(req: *mut FuseReq, entry: *const FuseEntryParam) -> c_int;
        pub(crate) fn fuse_reply_attr(req: *mut FuseReq, attr: *const stat, timeout: f64) -> c_int;
        pub(crate) fn fuse_reply_open(req: *mut FuseReq, fi: *const FuseFileInfo) -> c_int;
        pub(crate) fn fuse_reply_create(
            req: *mut FuseReq,
            entry: *const FuseEntryParam,
            fi: *const FuseFileInfo,
        ) -> c_int;
        pub(crate) fn fuse_reply_buf(req: *mut FuseReq, buf: *const c_char, size: size_t) -> c_int;
        pub(crate) fn fuse_reply_xattr(req: *mut FuseReq, count: size_t) -> c_int;
        pub(crate) fn fuse_reply_write(req: *mut FuseReq, count: size_t) -> c_int;
        pub(crate) fn fuse_reply_statfs(req: *mut FuseReq, stat: *const statvfs) -> c_int;
        pub(crate) fn fuse_req_userdata(req: *mut FuseReq) -> *mut c_void;
        pub(crate) fn fuse_add_direntry(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            stbuf: *const stat,
            off: off_t,
        ) -> size_t;
        pub(crate) fn fuse_opt_free_args(args: *mut FuseArgs);
        pub(crate) fn fuse_session_mount(
            session: *mut FuseSession,
            mountpoint: *const c_char,
        ) -> c_int;
        #[link_name = "fuse_session_loop_mt_31"]
        pub(crate) fn fuse_session_loop_mt(session: *mut FuseSession, clone_fd: c_int) -> c_int;
        pub(crate) fn fuse_session_unmount(session: *mut FuseSession);
        pub(crate) fn fuse_session_destroy(session: *mut FuseSession);
        pub(crate) fn fuse_set_signal_handlers(session: *mut FuseSession) -> c_int;
        pub(crate) fn fuse_remove_signal_handlers(session: *mut FuseSession);
    }
}

unsafe extern "C" {
    #[cfg(test)]
    fn studiofs_linux_fuse_empty_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_linux_fuse_low_level_ops() -> *const raw::FuseLowLevelOps;
    fn studiofs_linux_fuse_low_level_ops_size() -> libc::size_t;
    fn studiofs_linux_fuse_session_new(
        args: *mut raw::FuseArgs,
        ops: *const raw::FuseLowLevelOps,
        ops_size: libc::size_t,
        userdata: *mut c_void,
    ) -> *mut raw::FuseSession;
    fn studiofs_linux_fuse_notify_inval_inode(
        session: *mut raw::FuseSession,
        ino: u64,
        offset: off_t,
        len: off_t,
    ) -> c_int;
    fn studiofs_linux_fuse_dir_file_info_init(info: *mut raw::FuseFileInfo, fh: u64);
    fn studiofs_linux_fuse_file_info_init(info: *mut raw::FuseFileInfo, fh: u64);
    fn studiofs_linux_fuse_file_info_fh(info: *const raw::FuseFileInfo) -> u64;
    fn studiofs_linux_fuse_file_info_flags(info: *const raw::FuseFileInfo) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_namespace_mutation_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_low_level_ops_has_posix_compat_callbacks(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_sets_bounded_background(
        userdata: *mut c_void,
        max_background: u32,
        congestion_threshold: u32,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_direct_io_after_init() -> c_int;
}

pub(crate) struct SessionState {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    connection_config: LinuxFuseConnectionConfig,
}

const _: fn() = assert_send_sync_static::<SessionState>;

impl SessionState {
    pub(crate) fn new(
        vfs: Arc<StudioVfs>,
        runtime_handle: Handle,
        read_only: bool,
        connection_config: LinuxFuseConnectionConfig,
    ) -> Self {
        Self {
            vfs,
            runtime_handle,
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            connection_config,
        }
    }
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_max_background(
    userdata: *mut c_void,
) -> u32 {
    callback_state_from_userdata(userdata)
        .map(|state| state.connection_config.max_background)
        .unwrap_or_else(|| LinuxFuseConnectionConfig::default().max_background)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_congestion_threshold(
    userdata: *mut c_void,
) -> u32 {
    callback_state_from_userdata(userdata)
        .map(|state| state.connection_config.congestion_threshold)
        .unwrap_or_else(|| LinuxFuseConnectionConfig::default().congestion_threshold)
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions.
    unsafe { libc::geteuid() }
}

fn current_gid() -> u32 {
    // SAFETY: `getegid` has no preconditions.
    unsafe { libc::getegid() }
}

fn argv_strings(volume_name: &str) -> Vec<String> {
    vec![
        "studiofs-linux-fuse".to_owned(),
        "-o".to_owned(),
        format!("fsname={volume_name}"),
        "-o".to_owned(),
        "subtype=studiofs".to_owned(),
        "-o".to_owned(),
        "auto_unmount".to_owned(),
    ]
}

fn path_to_cstring(path: &std::path::Path) -> io::Result<CString> {
    use std::os::unix::ffi::OsStrExt;

    CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Linux FUSE mount point contains an interior NUL byte",
        )
    })
}

struct OwnedFuseArgs {
    _strings: Vec<CString>,
    _argv: Vec<*mut c_char>,
    raw: raw::FuseArgs,
}

impl OwnedFuseArgs {
    fn new(args: Vec<String>) -> io::Result<Self> {
        let strings = args
            .into_iter()
            .map(|arg| {
                CString::new(arg).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Linux FUSE argument contains an interior NUL byte",
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut argv = strings
            .iter()
            .map(|arg| arg.as_ptr().cast_mut())
            .collect::<Vec<_>>();
        let argc = argv.len().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "too many Linux FUSE arguments")
        })?;
        argv.push(std::ptr::null_mut());
        let raw = raw::FuseArgs {
            argc,
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
        // SAFETY: libfuse accepts a stack-owned fuse_args with allocated == 0;
        // this frees only libfuse-owned replacements if option parsing made any.
        unsafe { raw::fuse_opt_free_args(&mut self.raw) };
    }
}

pub(crate) struct Session {
    raw: NonNull<raw::FuseSession>,
    state: Option<Box<SessionState>>,
    invalidation_state: Arc<SessionInvalidationState>,
    signal_handlers_installed: bool,
}

impl Session {
    pub(crate) fn mount(
        state: SessionState,
        mount_point: impl AsRef<std::path::Path>,
        volume_name: impl AsRef<str>,
    ) -> io::Result<Self> {
        let mount_point = path_to_cstring(mount_point.as_ref())?;
        let mut args = OwnedFuseArgs::new(argv_strings(volume_name.as_ref()))?;
        let mut state = Box::new(state);
        let userdata = (&mut *state as *mut SessionState).cast::<c_void>();
        // SAFETY: the C shim expands libfuse's versioned session constructor.
        // The ops table is static, and userdata points to state owned by Session.
        let raw = NonNull::new(unsafe {
            studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                studiofs_linux_fuse_low_level_ops(),
                studiofs_linux_fuse_low_level_ops_size(),
                userdata,
            )
        });
        let Some(raw) = raw else {
            return Err(io::Error::other(
                "libfuse3 failed to create low-level session",
            ));
        };
        // SAFETY: `raw` is a live libfuse session and `mount_point` is a valid
        // NUL-terminated path for this call.
        let mount_result = unsafe { raw::fuse_session_mount(raw.as_ptr(), mount_point.as_ptr()) };
        if mount_result == 0 {
            let invalidation_state = Arc::new(SessionInvalidationState::new());
            let session = Self {
                raw,
                state: Some(state),
                invalidation_state,
                signal_handlers_installed: false,
            };
            if let Some(state) = &session.state {
                state
                    .vfs
                    .set_kernel_invalidator(session.kernel_invalidator());
            }
            Ok(session)
        } else {
            // SAFETY: `raw` has not been returned and must be destroyed here.
            unsafe { raw::fuse_session_destroy(raw.as_ptr()) };
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn install_signal_handlers(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live libfuse session.
        let result = unsafe { raw::fuse_set_signal_handlers(self.raw.as_ptr()) };
        if result == 0 {
            self.signal_handlers_installed = true;
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(test)]
    fn mount_empty_for_test(
        mount_point: impl AsRef<std::path::Path>,
        volume_name: impl AsRef<str>,
    ) -> io::Result<Self> {
        let mount_point = path_to_cstring(mount_point.as_ref())?;
        let mut args = OwnedFuseArgs::new(argv_strings(volume_name.as_ref()))?;
        // SAFETY: the C shim returns a static empty ops table; userdata is null
        // because no callbacks are installed in this lifecycle-only smoke.
        let raw = NonNull::new(unsafe {
            studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                studiofs_linux_fuse_empty_low_level_ops(),
                studiofs_linux_fuse_low_level_ops_size(),
                std::ptr::null_mut(),
            )
        })
        .ok_or_else(|| io::Error::other("libfuse3 failed to create low-level session"))?;
        // SAFETY: `raw` is a live libfuse session and `mount_point` is valid.
        let mount_result = unsafe { raw::fuse_session_mount(raw.as_ptr(), mount_point.as_ptr()) };
        if mount_result == 0 {
            Ok(Self {
                raw,
                state: None,
                invalidation_state: Arc::new(SessionInvalidationState::new()),
                signal_handlers_installed: false,
            })
        } else {
            // SAFETY: `raw` has not been returned and must be destroyed here.
            unsafe { raw::fuse_session_destroy(raw.as_ptr()) };
            Err(io::Error::last_os_error())
        }
    }

    pub(crate) fn run(&mut self) -> io::Result<()> {
        // SAFETY: `self.raw` is a live libfuse session owned by this wrapper.
        // `clone_fd = 0` keeps libfuse transport ownership on the mounted session
        // while the bounded kernel background queue applies callback backpressure.
        let result = unsafe { raw::fuse_session_loop_mt(self.raw.as_ptr(), 0) };
        if result == 0
            || result == libc::SIGINT
            || result == libc::SIGTERM
            || result == libc::SIGHUP
        {
            Ok(())
        } else if result < 0 {
            Err(io::Error::from_raw_os_error(-result))
        } else {
            Err(io::Error::from_raw_os_error(result))
        }
    }

    pub(crate) fn kernel_invalidator(&self) -> Arc<dyn KernelInvalidator> {
        Arc::new(LinuxFuseKernelInvalidator {
            session: self.raw,
            state: Arc::clone(&self.invalidation_state),
        })
    }
}

#[derive(Debug)]
struct LinuxFuseKernelInvalidator {
    session: NonNull<raw::FuseSession>,
    state: Arc<SessionInvalidationState>,
}

#[derive(Debug)]
struct SessionInvalidationState {
    status: Mutex<SessionInvalidationStatus>,
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
            status: Mutex::new(SessionInvalidationStatus {
                closed: false,
                active: 0,
            }),
            idle: Condvar::new(),
        }
    }

    fn lock_status(&self) -> MutexGuard<'_, SessionInvalidationStatus> {
        // A panic while holding this gate should not prevent shutdown from
        // closing the gate or observing active invalidations.
        self.status.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn begin_invalidation(&self) -> Option<SessionInvalidationGuard<'_>> {
        let mut status = self.lock_status();
        if status.closed {
            return None;
        }
        status.active += 1;
        Some(SessionInvalidationGuard { state: self })
    }

    fn begin_shutdown(&self) {
        self.lock_status().closed = true;
    }

    fn wait_for_idle(&self, timeout: Duration) -> bool {
        let status = self.lock_status();
        let result = self
            .idle
            .wait_timeout_while(status, timeout, |status| status.active > 0)
            .unwrap_or_else(|err| err.into_inner());
        result.0.active == 0
    }
}

impl Drop for SessionInvalidationGuard<'_> {
    fn drop(&mut self) {
        let should_notify = {
            let mut status = self.state.lock_status();
            status.active -= 1;
            status.active == 0
        };
        if should_notify {
            self.state.idle.notify_all();
        }
    }
}

// SAFETY: invalidation calls are gated against `Session::drop`, and the raw
// pointer is only used for libfuse notification calls while the session is live.
unsafe impl Send for LinuxFuseKernelInvalidator {}
// SAFETY: shared access only issues libfuse notification calls under the same gate.
unsafe impl Sync for LinuxFuseKernelInvalidator {}

impl KernelInvalidator for LinuxFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.inval_inode_range(ino, 0, 0);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.inval_inode_range(ino, -1, 0);
    }
}

impl LinuxFuseKernelInvalidator {
    fn inval_inode_range(&self, ino: u64, offset: off_t, len: off_t) {
        let Some(_active) = self.state.begin_invalidation() else {
            debug!(
                ino,
                offset, len, "Linux FUSE inode invalidation skipped after session shutdown"
            );
            return;
        };
        // SAFETY: `session` comes from a mounted libfuse3 session and the
        // invalidation guard prevents teardown from destroying it during the call.
        let result = unsafe {
            studiofs_linux_fuse_notify_inval_inode(self.session.as_ptr(), ino, offset, len)
        };
        if result != 0 && result != -libc::ENOENT {
            warn!(
                ino,
                offset, len, result, "Linux FUSE inode invalidation failed"
            );
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_destroy(userdata: *mut c_void) {
    let _ = userdata;
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_lookup(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.lookup(parent, name)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_getattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    _fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.getattr(ino) {
        Ok(attr) => {
            // SAFETY: `attr` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_attr(req, &attr, TTL_SECONDS) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_setattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    attr: *mut stat,
    to_set: c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if state.read_only {
        reply_error(req, libc::EROFS);
        return;
    }
    let args = match setattr_args(attr.cast_const(), to_set) {
        Ok(args) => args,
        Err(errno) => {
            reply_error(req, errno);
            return;
        }
    };
    match state.setattr(state.vfs_file_handle(fi), ino, args) {
        Ok(attr) => {
            let attr = state.attr_to_stat(&attr);
            // SAFETY: `attr` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_attr(req, &attr, TTL_SECONDS) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_create(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if require_file_info(fi).is_err() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let result = with_name_bytes(name, |name| state.create(parent, name, mode, fi))
        .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok((attr, fh)) => reply_create(req, state, fi, &attr, fh),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_mkdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.mkdir(parent, name, mode)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_mknod(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    mode: mode_t,
    _rdev: libc::dev_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.mknod(parent, name, mode)).unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_symlink(
    req: *mut raw::FuseReq,
    link: *const c_char,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result = with_name_bytes(link, |target| {
        with_name_bytes(name, |name| state.symlink(parent, name, target))
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_unlink(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match with_name_bytes(name, |name| state.unlink(parent, name)).unwrap_or(Err(libc::EINVAL)) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_rmdir(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match with_name_bytes(name, |name| state.rmdir(parent, name)).unwrap_or(Err(libc::EINVAL)) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_rename(
    req: *mut raw::FuseReq,
    parent: raw::FuseIno,
    name: *const c_char,
    newparent: raw::FuseIno,
    newname: *const c_char,
    flags: c_uint,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if flags != 0 {
        reply_error(req, libc::EINVAL);
        return;
    }
    let result = with_name_bytes(name, |name| {
        with_name_bytes(newname, |newname| {
            state.rename(parent, name, newparent, newname)
        })
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_link(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    newparent: raw::FuseIno,
    newname: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result = with_name_bytes(newname, |newname| state.link(ino, newparent, newname))
        .unwrap_or(Err(libc::EINVAL));
    reply_entry_result(req, state, result);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_open(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if require_file_info(fi).is_err() {
        reply_error(req, libc::EINVAL);
        return;
    }
    match state.open(ino, fi) {
        Ok(fh) => reply_file_open(req, fi, fh),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_read(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.read(ino, size, off, fi) {
        Ok(buffer) => {
            // SAFETY: `buffer` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, buffer.as_ptr().cast::<c_char>(), buffer.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_readlink(req: *mut raw::FuseReq, ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.readlink(ino) {
        Ok(target) => {
            // Linux readlink replies are byte buffers without a trailing NUL.
            // SAFETY: `target` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, target.as_ptr().cast::<c_char>(), target.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_write(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    buf: *const c_char,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.write(ino, buf, size, off, fi) {
        Ok(written) => {
            // SAFETY: `req` is live and this consumes the callback reply.
            unsafe { raw::fuse_reply_write(req, written as size_t) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_flush(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match file_handle(fi)
        .ok_or(libc::EBADF)
        .and_then(|fh| state.flush(fh))
    {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_fsync(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    datasync: c_int,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.fsync(file_handle(fi), ino, datasync != 0) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_release(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match file_handle(fi)
        .ok_or(libc::EBADF)
        .and_then(|fh| state.release(fh))
    {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_opendir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.getattr_attr(ino) {
        Ok(attr) if attr.kind == FileType::Directory => match state.open_dir(ino) {
            Ok(handle) => reply_dir_open(req, fi, handle),
            Err(errno) => reply_error(req, errno),
        },
        Ok(_) => reply_error(req, libc::ENOTDIR),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_readdir(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
    off: off_t,
    fi: *mut raw::FuseFileInfo,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Some(handle) = dir_handle(fi) else {
        reply_error(req, libc::EBADF);
        return;
    };
    match state.readdir(req, ino, handle, off, size) {
        Ok(buffer) => {
            let bytes = buffer.as_slice();
            // SAFETY: `bytes` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, bytes.as_ptr().cast::<c_char>(), bytes.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_releasedir(
    req: *mut raw::FuseReq,
    _ino: raw::FuseIno,
    fi: *mut raw::FuseFileInfo,
) {
    if let Some(handle) = dir_handle(fi) {
        // SAFETY: `opendir` created this pointer with `Box::into_raw`, and
        // libfuse calls `releasedir` once for each successful directory open.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
    }
    reply_error(req, 0);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_statfs(req: *mut raw::FuseReq, _ino: raw::FuseIno) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.statfs() {
        Ok(stat) => {
            // SAFETY: `stat` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_statfs(req, &stat) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_access(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    mask: c_int,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.access(ino, mask) {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_setxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    value: *const c_char,
    size: size_t,
    flags: c_int,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let Ok(flags) = u32::try_from(flags) else {
        reply_error(req, libc::EINVAL);
        return;
    };
    let result = with_name_bytes(name, |name| {
        with_xattr_value(value, size, |value| {
            state.set_xattr(ino, name, value, flags)
        })
    })
    .flatten()
    .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_getxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    if size == 0 {
        let result = with_name_bytes(name, |name| state.get_xattr_size(ino, name))
            .unwrap_or(Err(libc::EINVAL));
        match result {
            Ok(Some(value_size)) => {
                // SAFETY: `req` is live and this consumes the callback reply.
                unsafe { raw::fuse_reply_xattr(req, value_size) };
            }
            Ok(None) => reply_error(req, libc::ENODATA),
            Err(errno) => reply_error(req, errno),
        }
        return;
    }

    let result =
        with_name_bytes(name, |name| state.get_xattr(ino, name)).unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(Some(value)) => reply_xattr_value(req, size, &value),
        Ok(None) => reply_error(req, libc::ENODATA),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_listxattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    size: size_t,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    match state.list_xattrs(ino) {
        Ok(names) if size == 0 => {
            let value_size = names.iter().map(|name| name.len() + 1).sum();
            // SAFETY: `req` is live and this consumes the callback reply.
            unsafe { raw::fuse_reply_xattr(req, value_size) };
        }
        Ok(names) => reply_xattr_value(req, size, &format_xattr_name_list(&names)),
        Err(errno) => reply_error(req, errno),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_removexattr(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    name: *const c_char,
) {
    let Some(state) = callback_state(req) else {
        reply_error(req, libc::EIO);
        return;
    };
    let result =
        with_name_bytes(name, |name| state.remove_xattr(ino, name)).unwrap_or(Err(libc::EINVAL));
    match result {
        Ok(()) => reply_error(req, 0),
        Err(errno) => reply_error(req, errno),
    }
}

fn callback_state(req: *mut raw::FuseReq) -> Option<&'static SessionState> {
    if req.is_null() {
        return None;
    }
    // SAFETY: libfuse returns the userdata pointer supplied at session creation.
    let userdata = unsafe { raw::fuse_req_userdata(req) };
    callback_state_from_userdata(userdata)
}

fn callback_state_from_userdata(userdata: *mut c_void) -> Option<&'static SessionState> {
    if userdata.is_null() {
        None
    } else {
        // SAFETY: userdata lives until libfuse calls the destroy callback.
        Some(unsafe { &*userdata.cast::<SessionState>() })
    }
}

fn with_name_bytes<T>(name: *const c_char, f: impl FnOnce(&[u8]) -> T) -> Option<T> {
    if name.is_null() {
        return None;
    }
    // SAFETY: libfuse passes callback names as NUL-terminated strings valid
    // for the duration of the callback.
    Some(f(unsafe { CStr::from_ptr(name) }.to_bytes()))
}

fn with_xattr_value<T>(value: *const c_char, size: usize, f: impl FnOnce(&[u8]) -> T) -> Option<T> {
    if size == 0 {
        return Some(f(&[]));
    }
    if value.is_null() {
        return None;
    }
    // SAFETY: libfuse passes `value` as a request-owned byte slice valid for
    // the duration of the callback.
    Some(f(unsafe {
        std::slice::from_raw_parts(value.cast::<u8>(), size)
    }))
}

impl SessionState {
    fn lookup(&self, parent: u64, name: &[u8]) -> Result<InodeAttr, i32> {
        self.vfs
            .lookup(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr_attr(&self, ino: u64) -> Result<InodeAttr, i32> {
        self.vfs
            .getattr(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn getattr(&self, ino: u64) -> Result<stat, i32> {
        self.getattr_attr(ino).map(|attr| self.attr_to_stat(&attr))
    }

    fn setattr(
        &self,
        file_handle: Option<u64>,
        ino: u64,
        args: SetattrArgs,
    ) -> Result<InodeAttr, i32> {
        self.vfs
            .setattr_with_handle(file_handle, ino, args)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn create(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<(InodeAttr, u64), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let attr = self
            .vfs
            .create(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                (mode & 0o7777) as u16,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        let (open_flags, trunc) = posix_flags_to_open_flags(file_info_flags(fi));
        let fh = self
            .vfs
            .open_created_file_with_known_attr(&attr, open_flags, trunc)
            .map_err(|error| vfs_err_to_errno(&error))?;
        Ok((attr, fh))
    }

    fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .mkdir(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                (mode & 0o7777) as u16,
            )
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn mknod(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        self.vfs
            .create(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                (mode & 0o7777) as u16,
            )
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn symlink(&self, parent: u64, name: &[u8], target: &[u8]) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .symlink(parent, name, target, self.mount_uid, self.mount_gid)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn unlink(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .unlink(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rmdir(&self, parent: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rmdir(parent, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn rename(&self, parent: u64, name: &[u8], newparent: u64, newname: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .rename(parent, name, newparent, newname)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn link(&self, ino: u64, newparent: u64, newname: &[u8]) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .link(ino, newparent, newname)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn open(&self, ino: u64, fi: *const raw::FuseFileInfo) -> Result<u64, i32> {
        let attr = self.getattr_attr(ino)?;
        match attr.kind {
            FileType::RegularFile => {}
            FileType::Directory => return Err(libc::EISDIR),
            FileType::Symlink => return Err(libc::EINVAL),
        }
        let (open_flags, trunc) = posix_flags_to_open_flags(file_info_flags(fi));
        if self.read_only
            && (trunc
                || open_flags.contains(OpenFlags::WRONLY)
                || open_flags.contains(OpenFlags::RDWR))
        {
            return Err(libc::EROFS);
        }
        self.vfs
            .open_file_with_known_attr(&attr, open_flags, trunc)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn read(
        &self,
        ino: u64,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<Vec<u8>, i32> {
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size = clamp_read_size(size);
        let mut buffer = vec![0; size];
        let read = self
            .runtime_handle
            .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut buffer))
            .map_err(|error| vfs_err_to_errno(&error))?;
        buffer.truncate(read as usize);
        Ok(buffer)
    }

    fn readlink(&self, ino: u64) -> Result<Vec<u8>, i32> {
        self.vfs
            .readlink(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn write(
        &self,
        ino: u64,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<u32, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        if buf.is_null() && size > 0 {
            return Err(libc::EINVAL);
        }
        let data = if size == 0 {
            &[]
        } else {
            // SAFETY: libfuse passes `buf` as a request-owned byte slice valid for
            // the duration of the callback.
            unsafe { std::slice::from_raw_parts(buf.cast::<u8>(), size) }
        };
        let local_sync_policy = self.local_sync_policy_for_handle(fh);
        self.runtime_handle
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                offset,
                data,
                local_sync_policy,
            ))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn local_sync_policy_for_handle(&self, fh: u64) -> LocalSyncPolicy {
        self.vfs
            .get_open_file(fh)
            .filter(|open_file| open_file.flags.contains(OpenFlags::SYNC))
            .map(|_| LocalSyncPolicy::WriteThrough)
            .unwrap_or(LocalSyncPolicy::Opportunistic)
    }

    fn flush(&self, fh: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.flush_fh(fh))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn fsync(&self, file_handle: Option<u64>, ino: u64, datasync: bool) -> Result<(), i32> {
        let result = if let Some(fh) = file_handle {
            // Handle-backed sync must preserve pending handle-local write state;
            // StudioVfs does not currently distinguish fdatasync from fsync on
            // this path, so both use the handle flush barrier.
            self.runtime_handle.block_on(self.vfs.flush_fh(fh))
        } else {
            self.runtime_handle.block_on(self.vfs.fsync(ino, datasync))
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        self.runtime_handle
            .block_on(self.vfs.release_fh(fh))
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn vfs_file_handle(&self, fi: *const raw::FuseFileInfo) -> Option<u64> {
        file_handle(fi).filter(|fh| self.vfs.get_open_file(*fh).is_some())
    }

    fn open_dir(&self, ino: u64) -> Result<NonNull<DirectoryHandle>, i32> {
        // SFS-604 snapshots each directory handle eagerly so Linux readdir offsets
        // stay stable across chunked kernel reads. Replace with VFS cursors if
        // huge-directory memory use shows up in workload evidence.
        let entries = self
            .vfs
            .readdir_with_attrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))?;
        let raw = Box::into_raw(Box::new(DirectoryHandle { entries }));
        NonNull::new(raw).ok_or(libc::ENOMEM)
    }

    fn readdir(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        handle: NonNull<DirectoryHandle>,
        offset: off_t,
        size: usize,
    ) -> Result<DirectoryEntryBuffer, i32> {
        let mut buffer = DirectoryEntryBuffer::new(size);
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        if idx == 0 {
            if !buffer.push(req, OsStr::new("."), &dir_attr, 1)? {
                return Ok(buffer);
            }
            idx = 1;
        }
        if idx == 1 {
            if !buffer.push(req, OsStr::new(".."), &dir_attr, 2)? {
                return Ok(buffer);
            }
            idx = 2;
        }

        // SAFETY: `handle` comes from `opendir` and remains owned by the
        // matching directory handle until `releasedir`.
        let entries = unsafe { &handle.as_ref().entries };
        for (i, entry) in entries.iter().enumerate().skip(idx.saturating_sub(2)) {
            let entry_idx = i + 2;
            let attr = entry.attr.as_ref().map_or_else(
                || minimal_entry_stat(entry.entry.ino, entry.entry.kind),
                |attr| self.attr_to_stat(attr),
            );
            let next_offset = off_t::try_from(entry_idx + 1).map_err(|_| libc::EOVERFLOW)?;
            if !buffer.push(
                req,
                OsStr::from_bytes(&entry.entry.name),
                &attr,
                next_offset,
            )? {
                break;
            }
        }
        Ok(buffer)
    }

    fn statfs(&self) -> Result<statvfs, i32> {
        self.vfs
            .statfs()
            .map(statfs_to_statvfs)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn access(&self, ino: u64, mask: c_int) -> Result<(), i32> {
        let attr = self.getattr_attr(ino)?;
        if mask == libc::F_OK {
            return Ok(());
        }
        if self.read_only && mask & libc::W_OK != 0 {
            return Err(libc::EROFS);
        }
        let mode = attr.mode;
        let readable = mode & 0o444 != 0;
        let writable = mode & 0o222 != 0;
        let executable = mode & 0o111 != 0 || attr.kind == FileType::Directory;
        if (mask & libc::R_OK == 0 || readable)
            && (mask & libc::W_OK == 0 || writable)
            && (mask & libc::X_OK == 0 || executable)
        {
            Ok(())
        } else {
            Err(libc::EACCES)
        }
    }

    fn attr_to_stat(&self, attr: &InodeAttr) -> stat {
        bridge_attr_to_stat(&to_bridge_attr(
            attr,
            self.mount_uid,
            self.mount_gid,
            LINUX_FUSE_BLOCK_SIZE,
        ))
    }

    fn get_xattr(&self, ino: u64, name: &[u8]) -> Result<Option<Vec<u8>>, i32> {
        self.vfs
            .get_xattr(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn get_xattr_size(&self, ino: u64, name: &[u8]) -> Result<Option<usize>, i32> {
        self.vfs
            .get_xattr_size(ino, name)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn set_xattr(&self, ino: u64, name: &[u8], value: &[u8], flags: u32) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .set_xattr(ino, name, value, flags)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    fn remove_xattr(&self, ino: u64, name: &[u8]) -> Result<(), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.vfs
            .remove_xattr(ino, name)
            .map_err(|error| xattr_mutation_err_to_errno(&error))
    }

    fn list_xattrs(&self, ino: u64) -> Result<Vec<Vec<u8>>, i32> {
        self.vfs
            .list_xattrs(ino)
            .map_err(|error| vfs_err_to_errno(&error))
    }
}

struct DirectoryHandle {
    entries: Vec<DirEntryWithAttr>,
}

fn dir_handle(fi: *const raw::FuseFileInfo) -> Option<NonNull<DirectoryHandle>> {
    if fi.is_null() {
        return None;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `fh` field using the system header layout.
    let fh = unsafe { studiofs_linux_fuse_file_info_fh(fi) };
    NonNull::new(fh as *mut DirectoryHandle)
}

fn file_handle(fi: *const raw::FuseFileInfo) -> Option<u64> {
    if fi.is_null() {
        return None;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `fh` field using the system header layout.
    let fh = unsafe { studiofs_linux_fuse_file_info_fh(fi) };
    (fh != 0).then_some(fh)
}

fn file_info_flags(fi: *const raw::FuseFileInfo) -> c_int {
    if fi.is_null() {
        return 0;
    }
    // SAFETY: libfuse passes a live `fuse_file_info`; the C shim reads only
    // the `flags` field using the system header layout.
    unsafe { studiofs_linux_fuse_file_info_flags(fi) }
}

fn bridge_attr_to_stat(attr: &BridgeFileAttr) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: zeroed `stat` is valid; fields used by libfuse are initialized.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = attr.ino as _;
    out.st_size = attr.size as _;
    out.st_blocks = attr.blocks as _;
    out.st_blksize = attr.blksize as _;
    out.st_nlink = attr.nlink as _;
    out.st_uid = attr.uid as _;
    out.st_gid = attr.gid as _;
    out.st_rdev = attr.rdev as _;
    out.st_mode = mode_t::from(attr.perm) | file_type_mode(attr.kind);
    let (seconds, nanos) = system_time_parts(attr.atime);
    out.st_atime = seconds;
    out.st_atime_nsec = nanos;
    let (seconds, nanos) = system_time_parts(attr.mtime);
    out.st_mtime = seconds;
    out.st_mtime_nsec = nanos;
    let (seconds, nanos) = system_time_parts(attr.ctime);
    out.st_ctime = seconds;
    out.st_ctime_nsec = nanos;
    out
}

fn minimal_dir_stat(ino: u64) -> stat {
    minimal_entry_stat(ino, FileType::Directory)
}

fn minimal_entry_stat(ino: u64, kind: FileType) -> stat {
    let out = MaybeUninit::<stat>::zeroed();
    // SAFETY: zeroed `stat` is valid; minimal direntry fields are initialized.
    let mut out = unsafe { out.assume_init() };
    out.st_ino = ino as _;
    out.st_mode = file_type_mode(match kind {
        FileType::RegularFile => BridgeFileType::RegularFile,
        FileType::Directory => BridgeFileType::Directory,
        FileType::Symlink => BridgeFileType::Symlink,
    });
    out
}

fn file_type_mode(kind: BridgeFileType) -> mode_t {
    match kind {
        BridgeFileType::RegularFile => libc::S_IFREG,
        BridgeFileType::Directory => libc::S_IFDIR,
        BridgeFileType::Symlink => libc::S_IFLNK,
    }
}

fn system_time_parts(time: std::time::SystemTime) -> (libc::time_t, libc::c_long) {
    // StudioFS metadata normally stores Unix-epoch times; clamp pre-epoch values
    // to zero rather than reporting negative timestamps through this first Linux bridge slice.
    let duration = time
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (duration.as_secs() as _, duration.subsec_nanos() as _)
}

fn statfs_to_statvfs(stats: StatFs) -> statvfs {
    let out = MaybeUninit::<statvfs>::zeroed();
    // SAFETY: zeroed `statvfs` is valid; fields used by libfuse are initialized.
    let mut out = unsafe { out.assume_init() };
    out.f_bsize = stats.bsize as _;
    out.f_frsize = stats.frsize as _;
    out.f_blocks = stats.blocks as _;
    out.f_bfree = stats.bfree as _;
    out.f_bavail = stats.bavail as _;
    out.f_files = stats.files as _;
    out.f_ffree = stats.ffree as _;
    out.f_namemax = stats.namelen as _;
    out
}

fn setattr_args(attr: *const stat, to_set: c_int) -> Result<SetattrArgs, i32> {
    if attr.is_null() {
        return Err(libc::EINVAL);
    }
    let mut args = SetattrArgs::default();
    // SAFETY: libfuse passes a live `stat` pointer for setattr callbacks.
    let attr = unsafe { &*attr };
    if to_set & FUSE_SET_ATTR_SIZE != 0 {
        args.size = Some(u64::try_from(attr.st_size).map_err(|_| libc::EINVAL)?);
    }
    if to_set & FUSE_SET_ATTR_MODE != 0 {
        args.mode = Some((attr.st_mode & 0o7777) as u16);
    }
    if to_set & FUSE_SET_ATTR_UID != 0 {
        args.uid = Some(attr.st_uid);
    }
    if to_set & FUSE_SET_ATTR_GID != 0 {
        args.gid = Some(attr.st_gid);
    }
    if to_set & FUSE_SET_ATTR_ATIME_NOW != 0 {
        args.atime = TimeSpec::Now;
    } else if to_set & FUSE_SET_ATTR_ATIME != 0 {
        args.atime = TimeSpec::Set(timespec_nanos(attr.st_atime, attr.st_atime_nsec)?);
    }
    if to_set & FUSE_SET_ATTR_MTIME_NOW != 0 {
        args.mtime = TimeSpec::Now;
    } else if to_set & FUSE_SET_ATTR_MTIME != 0 {
        args.mtime = TimeSpec::Set(timespec_nanos(attr.st_mtime, attr.st_mtime_nsec)?);
    }
    Ok(args)
}

fn timespec_nanos(sec: libc::time_t, nsec: libc::c_long) -> Result<i64, i32> {
    let nanos = i128::from(sec)
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(i128::from(nsec)))
        .ok_or(libc::EOVERFLOW)?;
    i64::try_from(nanos).map_err(|_| libc::EOVERFLOW)
}

fn require_file_info(fi: *const raw::FuseFileInfo) -> Result<(), i32> {
    if fi.is_null() {
        Err(libc::EINVAL)
    } else {
        Ok(())
    }
}

fn clamp_read_size(size: usize) -> usize {
    size.min(LINUX_FUSE_MAX_READ_SIZE)
}

fn reply_entry_result(
    req: *mut raw::FuseReq,
    state: &SessionState,
    result: Result<InodeAttr, i32>,
) {
    match result {
        Ok(attr) => {
            let stat = state.attr_to_stat(&attr);
            let entry = raw::FuseEntryParam {
                ino: attr.ino,
                generation: 1,
                attr: stat,
                attr_timeout: TTL_SECONDS,
                entry_timeout: TTL_SECONDS,
            };
            // SAFETY: `entry` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_entry(req, &entry) };
        }
        Err(errno) => reply_error(req, errno),
    }
}

fn reply_dir_open(
    req: *mut raw::FuseReq,
    fi: *mut raw::FuseFileInfo,
    handle: NonNull<DirectoryHandle>,
) {
    if fi.is_null() {
        // SAFETY: ownership has not been handed to libfuse because `fi` is invalid.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
    // using the exact system header layout.
    unsafe {
        studiofs_linux_fuse_dir_file_info_init(fi, handle.as_ptr() as u64);
        raw::fuse_reply_open(req, fi);
    }
}

fn reply_file_open(req: *mut raw::FuseReq, fi: *mut raw::FuseFileInfo, fh: u64) {
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
    // using the exact system header layout.
    unsafe {
        studiofs_linux_fuse_file_info_init(fi, fh);
        raw::fuse_reply_open(req, fi);
    }
}

fn reply_create(
    req: *mut raw::FuseReq,
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    attr: &InodeAttr,
    fh: u64,
) {
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return;
    }
    let stat = state.attr_to_stat(attr);
    let entry = raw::FuseEntryParam {
        ino: attr.ino,
        generation: 1,
        attr: stat,
        attr_timeout: TTL_SECONDS,
        entry_timeout: TTL_SECONDS,
    };
    // SAFETY: `entry` and `fi` are initialized and valid for this synchronous reply.
    unsafe {
        studiofs_linux_fuse_file_info_init(fi, fh);
        raw::fuse_reply_create(req, &entry, fi);
    }
}

fn reply_error(req: *mut raw::FuseReq, errno: c_int) {
    // SAFETY: libfuse accepts errno 0 as a successful empty reply for callbacks
    // whose contract is `fuse_reply_err`.
    unsafe { raw::fuse_reply_err(req, errno) };
}

fn reply_xattr_value(req: *mut raw::FuseReq, size: usize, value: &[u8]) {
    debug_assert!(size > 0);
    if size < value.len() {
        reply_error(req, libc::ERANGE);
    } else {
        // SAFETY: `value` is valid for this synchronous reply.
        unsafe { raw::fuse_reply_buf(req, value.as_ptr().cast::<c_char>(), value.len()) };
    }
}

fn format_xattr_name_list(names: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(names.iter().map(|name| name.len() + 1).sum());
    for name in names {
        out.extend_from_slice(name);
        out.push(0);
    }
    out
}

fn xattr_mutation_err_to_errno(err: &studiofs_vfs::VfsError) -> i32 {
    if matches!(
        err,
        studiofs_vfs::VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    ) {
        libc::ENODATA
    } else {
        vfs_err_to_errno(err)
    }
}

struct DirectoryEntryBuffer {
    words: Vec<MaybeUninit<u64>>,
    len: usize,
    scratch_name: Vec<u8>,
    capacity: usize,
}

impl DirectoryEntryBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            words: Vec::with_capacity(capacity.div_ceil(std::mem::size_of::<u64>())),
            len: 0,
            scratch_name: Vec::with_capacity(256),
            capacity,
        }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.len` bytes from `words.as_ptr()` were initialized by push.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.len) }
    }

    fn push(
        &mut self,
        req: *mut raw::FuseReq,
        name: &OsStr,
        attr: &stat,
        next_offset: off_t,
    ) -> Result<bool, i32> {
        let name_bytes = name.as_bytes();
        if name_bytes.contains(&0) {
            return Err(libc::EINVAL);
        }
        self.scratch_name.clear();
        self.scratch_name.extend_from_slice(name_bytes);
        self.scratch_name.push(0);
        let available = self.capacity.saturating_sub(self.len);
        // SAFETY: null buffer asks libfuse for required size; pointers stay valid.
        let required = unsafe {
            raw::fuse_add_direntry(
                req,
                std::ptr::null_mut(),
                0,
                self.scratch_name.as_ptr().cast::<c_char>(),
                attr,
                next_offset,
            )
        };
        if required > available {
            return Ok(false);
        }

        let start = self.len;
        let new_len = start + required;
        let required_words = new_len.div_ceil(std::mem::size_of::<u64>());
        // SAFETY: capacity check above ensures `required` bytes of spare room.
        unsafe {
            raw::fuse_add_direntry(
                req,
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
        Ok(true)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(state) = &self.state {
            state.vfs.clear_kernel_invalidator();
        }
        self.invalidation_state.begin_shutdown();
        let safe_to_destroy = self
            .invalidation_state
            .wait_for_idle(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT);
        if !safe_to_destroy {
            warn!(
                "Linux FUSE session shutdown timed out waiting for active kernel invalidation; \
                 unmounting and leaking raw session handles to avoid use-after-free"
            );
        }
        if self.signal_handlers_installed {
            // SAFETY: handlers were installed for this live session.
            unsafe { raw::fuse_remove_signal_handlers(self.raw.as_ptr()) };
        }
        // SAFETY: libfuse tolerates unmount after regular kernel disconnects;
        // it is needed after signal-triggered exits to avoid a stuck mount.
        unsafe { raw::fuse_session_unmount(self.raw.as_ptr()) };
        if !safe_to_destroy {
            // An active notification may still hold this session pointer.
            return;
        }
        // SAFETY: `self.raw` is owned by this wrapper and destroyed once.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    struct UnmountGuard(Option<std::path::PathBuf>);

    impl UnmountGuard {
        fn new(path: std::path::PathBuf) -> Self {
            Self(Some(path))
        }

        fn disarm(mut self) {
            self.0 = None;
        }
    }

    impl Drop for UnmountGuard {
        fn drop(&mut self) {
            if let Some(path) = &self.0 {
                let _ = Command::new("fusermount3").arg("-u").arg(path).status();
            }
        }
    }

    fn mknod_regular_file(path: &std::path::Path) -> std::io::Result<()> {
        let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "test path contains an interior NUL byte",
            )
        })?;
        // SAFETY: `path` is NUL-terminated and valid for this syscall.
        let result = unsafe { libc::mknod(path.as_ptr(), libc::S_IFREG | 0o644, 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn path_cstring(path: &std::path::Path) -> std::io::Result<std::ffi::CString> {
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "test path contains an interior NUL byte",
            )
        })
    }

    fn set_xattr(path: &std::path::Path, name: &[u8], value: &[u8]) -> std::io::Result<()> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated; value points to `value.len()` bytes.
        let result = unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn get_xattr(path: &std::path::Path, name: &[u8]) -> std::io::Result<Vec<u8>> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated; null value asks for size.
        let size = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
        if size < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut value = vec![0; size as usize];
        // SAFETY: value has `size` bytes of writable capacity.
        let read = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        if read < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            value.truncate(read as usize);
            Ok(value)
        }
    }

    fn list_xattrs(path: &std::path::Path) -> std::io::Result<Vec<Vec<u8>>> {
        let path = path_cstring(path)?;
        // SAFETY: path is NUL-terminated; null list asks for size.
        let size = unsafe { libc::listxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
        if size < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut names = vec![0; size as usize];
        // SAFETY: names has `size` bytes of writable capacity.
        let read =
            unsafe { libc::listxattr(path.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
        if read < 0 {
            return Err(std::io::Error::last_os_error());
        }
        names.truncate(read as usize);
        Ok(names
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| name.to_vec())
            .collect())
    }

    fn remove_xattr(path: &std::path::Path, name: &[u8]) -> std::io::Result<()> {
        let path = path_cstring(path)?;
        let name = std::ffi::CString::new(name).expect("test xattr name has no NUL");
        // SAFETY: path and name are NUL-terminated.
        let result = unsafe { libc::removexattr(path.as_ptr(), name.as_ptr()) };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    fn default_connection_config() -> super::LinuxFuseConnectionConfig {
        super::LinuxFuseConnectionConfig::default()
    }

    #[test]
    fn argv_uses_system_libfuse_mount_options() {
        let args = super::argv_strings("demo");

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
        assert!(!args.iter().any(|arg| arg.contains("max_background")));
        assert!(!args.iter().any(|arg| arg.contains("congestion_threshold")));
    }

    #[test]
    fn owned_fuse_args_are_null_terminated_without_counting_sentinel() {
        let args = super::OwnedFuseArgs::new(super::argv_strings("demo")).expect("build args");

        assert_eq!(args.raw.argc as usize, args._strings.len());
        assert!(args._argv.last().is_some_and(|arg| arg.is_null()));
    }

    #[test]
    fn low_level_ops_include_metadata_and_directory_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_metadata_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn init_callback_sets_bounded_kernel_background_queue() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-default-bounds")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let config = default_connection_config();
        // SAFETY: the C shim initializes a stack-local fuse_conn_info.
        let has_bounds = unsafe {
            super::studiofs_linux_fuse_init_sets_bounded_background(
                (&mut state as *mut super::SessionState).cast(),
                config.max_background,
                config.congestion_threshold,
            )
        };

        assert_eq!(has_bounds, 1);
    }

    #[test]
    fn init_callback_uses_request_background_queue_config() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-custom-bounds")
                .expect("create VFS fixture");
        let config = super::LinuxFuseConnectionConfig {
            max_background: 32,
            congestion_threshold: 24,
        };
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            config,
        );
        // SAFETY: the C shim initializes a stack-local fuse_conn_info.
        let has_bounds = unsafe {
            super::studiofs_linux_fuse_init_sets_bounded_background(
                (&mut state as *mut super::SessionState).cast(),
                config.max_background,
                config.congestion_threshold,
            )
        };

        assert_eq!(has_bounds, 1);
    }

    #[test]
    fn low_level_ops_include_regular_file_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_regular_file_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn low_level_ops_include_namespace_mutation_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_namespace_mutation_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn low_level_ops_include_posix_compat_callbacks() {
        // SAFETY: the C shim only inspects callback pointers in a static table.
        let has_callbacks = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_posix_compat_callbacks(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callbacks, 1);
    }

    #[test]
    fn regular_file_info_defaults_to_direct_io() {
        // SAFETY: the C shim initializes and inspects a stack-local fuse_file_info.
        let direct_io = unsafe { super::studiofs_linux_fuse_file_info_direct_io_after_init() };

        assert_eq!(direct_io, 1);
    }

    #[test]
    fn invalidation_state_wait_for_idle_returns_true_when_idle() {
        let state = super::SessionInvalidationState::new();

        assert!(state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_wait_for_idle_wakes_when_last_guard_drops() {
        let state = Arc::new(super::SessionInvalidationState::new());
        let guard = state.begin_invalidation().expect("begin invalidation");
        let waiter_state = Arc::clone(&state);
        let (observed_tx, observed_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let status = waiter_state.lock_status();
            let result = waiter_state
                .idle
                .wait_timeout_while(status, Duration::from_secs(1), |status| {
                    if status.active > 0 {
                        let _ = observed_tx.send(());
                    }
                    status.active > 0
                })
                .unwrap_or_else(|err| err.into_inner());
            result_tx
                .send(result.0.active == 0)
                .expect("send idle result");
        });

        observed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("waiter should observe active invalidation");
        drop(guard);
        assert!(
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("idle result")
        );
        waiter.join().expect("join idle waiter");
    }

    #[test]
    fn invalidation_state_wait_for_idle_returns_false_on_timeout() {
        let state = super::SessionInvalidationState::new();
        let _guard = state.begin_invalidation().expect("begin invalidation");

        assert!(!state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_rejects_new_invalidations_after_shutdown() {
        let state = super::SessionInvalidationState::new();
        state.begin_shutdown();

        assert!(state.begin_invalidation().is_none());
    }

    #[test]
    fn require_file_info_rejects_null_pointer() {
        assert_eq!(
            super::require_file_info(std::ptr::null()),
            Err(libc::EINVAL)
        );
    }

    #[test]
    fn clamp_read_size_caps_oversized_request() {
        assert_eq!(
            super::clamp_read_size(super::LINUX_FUSE_MAX_READ_SIZE + 1),
            super::LINUX_FUSE_MAX_READ_SIZE
        );
    }

    #[test]
    fn sync_open_handle_uses_write_through_policy() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-sync-policy")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"sync.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file_with_known_attr(
                &attr,
                studiofs_vfs::OpenFlags::WRONLY | studiofs_vfs::OpenFlags::SYNC,
                false,
            )
            .expect("open sync file");
        let state = super::SessionState::new(
            vfs,
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert_eq!(
            state.local_sync_policy_for_handle(fh),
            studiofs_vfs::LocalSyncPolicy::WriteThrough
        );
    }

    #[test]
    fn read_only_write_rejects_before_using_file_info() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-write")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );

        assert_eq!(
            state.write(1, std::ptr::null(), 0, 0, std::ptr::null()),
            Err(libc::EROFS)
        );
    }

    #[test]
    fn read_only_namespace_mutations_reject_before_touching_vfs() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-mutate")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );

        assert_eq!(state.mkdir(1, b"dir", 0o755).unwrap_err(), libc::EROFS);
        assert_eq!(
            state.mknod(1, b"file", libc::S_IFREG | 0o644).unwrap_err(),
            libc::EROFS
        );
        assert_eq!(state.unlink(1, b"file"), Err(libc::EROFS));
        assert_eq!(state.rmdir(1, b"dir"), Err(libc::EROFS));
        assert_eq!(state.rename(1, b"old", 1, b"new"), Err(libc::EROFS));
        assert_eq!(
            state.symlink(1, b"link", b"target").unwrap_err(),
            libc::EROFS
        );
        assert_eq!(state.link(2, 1, b"hard").unwrap_err(), libc::EROFS);
        assert_eq!(
            state.set_xattr(1, b"user.key", b"value", 0),
            Err(libc::EROFS)
        );
        assert_eq!(state.remove_xattr(1, b"user.key"), Err(libc::EROFS));
    }

    #[test]
    fn mknod_reports_special_files_as_unsupported() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-mknod-special")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert_eq!(
            state.mknod(1, b"fifo", libc::S_IFIFO | 0o644).unwrap_err(),
            libc::ENOTSUP
        );
    }

    #[test]
    fn setattr_args_translate_linux_mask() {
        let attr = std::mem::MaybeUninit::<libc::stat>::zeroed();
        // SAFETY: zeroed `stat` is valid for this test and fields are set below.
        let mut attr = unsafe { attr.assume_init() };
        attr.st_size = 42;
        attr.st_mode = libc::S_IFREG | 0o640;
        attr.st_uid = 1000;
        attr.st_gid = 1001;
        attr.st_mtime = 7;
        attr.st_mtime_nsec = 9;

        let args = super::setattr_args(
            &attr,
            super::FUSE_SET_ATTR_SIZE
                | super::FUSE_SET_ATTR_MODE
                | super::FUSE_SET_ATTR_UID
                | super::FUSE_SET_ATTR_GID
                | super::FUSE_SET_ATTR_MTIME,
        )
        .expect("setattr args");

        assert_eq!(args.size, Some(42));
        assert_eq!(args.mode, Some(0o640));
        assert_eq!(args.uid, Some(1000));
        assert_eq!(args.gid, Some(1001));
        assert_eq!(args.mtime.resolve(), Some(7_000_000_009));
    }

    #[test]
    fn xattr_name_list_is_nul_separated() {
        assert_eq!(
            super::format_xattr_name_list(&[b"user.one".to_vec(), b"user.two".to_vec()]),
            b"user.one\0user.two\0"
        );
    }

    #[test]
    fn xattr_mutation_errno_preserves_missing_inode() {
        assert_eq!(
            super::xattr_mutation_err_to_errno(&studiofs_vfs::VfsError::Meta(
                studiofs_meta::MetaError::EntryNotFound {
                    parent: 7,
                    name: "user.missing".to_owned(),
                },
            )),
            libc::ENODATA
        );
        assert_eq!(
            super::xattr_mutation_err_to_errno(&studiofs_vfs::VfsError::Meta(
                studiofs_meta::MetaError::InodeNotFound(7),
            )),
            libc::ENOENT
        );
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_smoke_mounts_and_unmounts_cleanly() {
        let mount_point =
            std::env::temp_dir().join(format!("studiofs-linux-fuse-smoke-{}", std::process::id()));
        std::fs::create_dir_all(&mount_point).expect("create smoke mount point");

        let mut session = super::Session::mount_empty_for_test(&mount_point, "studiofs-smoke")
            .expect("mount libfuse3 session");
        let mount_point_for_unmount = mount_point.clone();
        let unmount = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            Command::new("fusermount3")
                .arg("-u")
                .arg(&mount_point_for_unmount)
                .status()
                .expect("run fusermount3 -u")
        });

        session
            .install_signal_handlers()
            .expect("install libfuse3 signal handlers");
        session.run().expect("run libfuse3 session");
        assert!(unmount.join().expect("join unmount thread").success());
        drop(session);
        std::fs::remove_dir_all(&mount_point).expect("remove smoke mount point");
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_regular_file_io_is_visible_across_reopen() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-regular-io")
                .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let mount_point = std::env::temp_dir().join(format!(
            "studiofs-linux-fuse-regular-io-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&mount_point).expect("create smoke mount point");

        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let mut session =
            super::Session::mount(state, &mount_point, "studiofs-regular-io").expect("mount");
        let mount_point_for_worker = mount_point.clone();
        let worker = std::thread::spawn(move || -> std::io::Result<std::process::ExitStatus> {
            let guard = UnmountGuard::new(mount_point_for_worker.clone());
            let path = mount_point_for_worker.join("regular.txt");
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            file.write_all(b"hello local-first")?;
            file.flush()?;
            file.seek(SeekFrom::Start(0))?;
            let mut same_handle = String::new();
            file.read_to_string(&mut same_handle)?;
            assert_eq!(same_handle, "hello local-first");
            file.sync_all()?;
            drop(file);

            assert_eq!(std::fs::read_to_string(&path)?, "hello local-first");
            let file = std::fs::OpenOptions::new().write(true).open(&path)?;
            file.set_len(5)?;
            file.sync_data()?;
            drop(file);
            assert_eq!(std::fs::read_to_string(&path)?, "hello");

            let status = Command::new("fusermount3")
                .arg("-u")
                .arg(&mount_point_for_worker)
                .status();
            guard.disarm();
            status
        });

        session.run().expect("run libfuse3 session");
        let unmount_status = worker
            .join()
            .expect("mounted I/O worker should not panic")
            .expect("mounted I/O worker should complete");
        assert!(unmount_status.success());
        drop(session);
        drop(fixture);
        std::fs::remove_dir_all(&mount_point).expect("remove smoke mount point");
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_namespace_mutations_are_visible_through_readdir() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-namespace-mutate")
                .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let mount_point = std::env::temp_dir().join(format!(
            "studiofs-linux-fuse-namespace-mutate-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&mount_point).expect("create smoke mount point");

        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let mut session =
            super::Session::mount(state, &mount_point, "studiofs-namespace-mutate").expect("mount");
        let mount_point_for_worker = mount_point.clone();
        let worker = std::thread::spawn(move || -> std::io::Result<std::process::ExitStatus> {
            let guard = UnmountGuard::new(mount_point_for_worker.clone());
            let dir = mount_point_for_worker.join("dir");
            let file = dir.join("file.txt");
            let hardlink = dir.join("hardlink.txt");
            let symlink = dir.join("symlink.txt");
            let mknod_file = dir.join("mknod.txt");
            let renamed = dir.join("renamed.txt");
            let xattr_name = b"user.studiofs.test";
            std::fs::create_dir(&dir)?;
            mknod_regular_file(&mknod_file)?;
            assert!(mknod_file.is_file());
            std::fs::remove_file(&mknod_file)?;
            std::fs::write(&file, b"namespace")?;
            std::fs::rename(&file, &renamed)?;
            std::fs::hard_link(&renamed, &hardlink)?;
            assert_eq!(std::fs::read_to_string(&hardlink)?, "namespace");
            assert!(std::fs::metadata(&renamed)?.nlink() >= 2);
            std::os::unix::fs::symlink("renamed.txt", &symlink)?;
            assert_eq!(
                std::fs::read_link(&symlink)?,
                std::path::PathBuf::from("renamed.txt")
            );
            set_xattr(&renamed, xattr_name, b"value")?;
            assert_eq!(get_xattr(&renamed, xattr_name)?, b"value");
            assert!(list_xattrs(&renamed)?.iter().any(|name| name == xattr_name));
            remove_xattr(&renamed, xattr_name)?;
            assert_eq!(
                get_xattr(&renamed, xattr_name)
                    .expect_err("removed xattr should be missing")
                    .raw_os_error(),
                Some(libc::ENODATA)
            );
            let mut names = std::fs::read_dir(&dir)?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect::<std::io::Result<Vec<_>>>()?;
            names.sort();
            assert_eq!(
                names,
                vec![
                    std::ffi::OsString::from("hardlink.txt"),
                    std::ffi::OsString::from("renamed.txt"),
                    std::ffi::OsString::from("symlink.txt")
                ]
            );
            std::fs::remove_file(&symlink)?;
            std::fs::remove_file(&hardlink)?;
            std::fs::remove_file(&renamed)?;
            std::fs::remove_dir(&dir)?;

            let status = Command::new("fusermount3")
                .arg("-u")
                .arg(&mount_point_for_worker)
                .status();
            guard.disarm();
            status
        });

        session.run().expect("run libfuse3 session");
        let unmount_status = worker
            .join()
            .expect("mounted namespace worker should not panic")
            .expect("mounted namespace worker should complete");
        assert!(unmount_status.success());
        drop(session);
        drop(fixture);
        std::fs::remove_dir_all(&mount_point).expect("remove smoke mount point");
    }
}
