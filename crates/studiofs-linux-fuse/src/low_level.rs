use super::*;
use std::ffi::{CStr, OsStr};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, normalize_close_barrier_result, posix_flags_to_open_flags,
    to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::{
    DirEntryWithAttr, KernelInvalidator, LocalSyncPolicy, OpenFlags, StatFs, StudioVfs,
    VfsFuseAsyncWriteSnapshot,
};
use tokio::runtime::Handle;
use tracing::{debug, warn};

use crate::async_writes::{
    LinuxFuseAsyncWriteCompletion, LinuxFuseAsyncWriteStats, LinuxFuseAsyncWrites,
    LinuxFuseRequestTicket, LinuxFuseWriteReservation,
};

const TTL_SECONDS: f64 = 1.0;
const LINUX_FUSE_BLOCK_SIZE: u32 = 4096;
const LINUX_FUSE_MAX_READ_SIZE: usize = 4 * 1024 * 1024;
const LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES: usize = 256 * 1024 * 1024;
const LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES: usize = 1024;
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
    fn studiofs_linux_fuse_session_loop_mt(
        session: *mut raw::FuseSession,
        max_threads: c_uint,
        max_idle_threads: c_uint,
    ) -> c_int;
    fn studiofs_linux_fuse_notify_inval_inode(
        session: *mut raw::FuseSession,
        ino: u64,
        offset: off_t,
        len: off_t,
    ) -> c_int;
    fn studiofs_linux_fuse_dir_file_info_init(info: *mut raw::FuseFileInfo, fh: u64);
    fn studiofs_linux_fuse_file_info_init(
        info: *mut raw::FuseFileInfo,
        fh: u64,
        direct_io: c_int,
        keep_cache: c_int,
        parallel_direct_writes: c_int,
    );
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
    fn studiofs_linux_fuse_loop_config_uses_loop_config_api() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_async_dio_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_writeback_cache_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_direct_io_after_init() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_flags_after_init(
        direct_io: c_int,
        keep_cache: c_int,
        parallel_direct_writes: c_int,
    ) -> c_int;
}

pub(crate) struct SessionState {
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    async_writes: Arc<LinuxFuseAsyncWrites>,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    connection_config: LinuxFuseConnectionConfig,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
}

const _: fn() = assert_send_sync_static::<SessionState>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinuxFuseCachePolicy {
    Direct,
    Kernel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinuxFuseReleaseMode {
    Synchronous,
    Deferred,
}

fn linux_fuse_release_mode(
    local_sync_policy: LocalSyncPolicy,
    has_local_dirty_state: bool,
) -> LinuxFuseReleaseMode {
    if local_sync_policy.is_opportunistic() && !has_local_dirty_state {
        LinuxFuseReleaseMode::Deferred
    } else {
        LinuxFuseReleaseMode::Synchronous
    }
}

fn merge_async_and_local_barrier_results(
    async_result: Result<(), i32>,
    local_result: Result<(), i32>,
) -> Result<(), i32> {
    match async_result {
        Err(errno) => Err(errno),
        Ok(()) => local_result,
    }
}

struct LinuxFuseHandleLedgerGuard {
    async_writes: Arc<LinuxFuseAsyncWrites>,
    fh: u64,
}

impl LinuxFuseHandleLedgerGuard {
    fn new(async_writes: Arc<LinuxFuseAsyncWrites>, fh: u64) -> Self {
        Self { async_writes, fh }
    }
}

impl Drop for LinuxFuseHandleLedgerGuard {
    fn drop(&mut self) {
        self.async_writes.forget_handle(self.fh);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinuxFuseOpenCachePolicy {
    direct_io: bool,
    keep_cache: bool,
    parallel_direct_writes: bool,
}

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
            async_writes: Arc::new(LinuxFuseAsyncWrites::new(
                LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES,
                LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES,
            )),
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            connection_config,
            metric_config: LinuxFuseAsyncWriteMetricConfig {
                cache_policy: linux_fuse_cache_policy_from_env(),
                connection_config,
            },
        }
    }
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[cfg(test)]
type FuseReplyWriteFn = unsafe extern "C" fn(*mut raw::FuseReq, size_t) -> c_int;
#[cfg(test)]
type FuseReplyErrorFn = unsafe extern "C" fn(*mut raw::FuseReq, c_int) -> c_int;

struct FuseReqPtr {
    raw: *mut raw::FuseReq,
    #[cfg(test)]
    reply_write_fn: FuseReplyWriteFn,
    #[cfg(test)]
    reply_error_fn: FuseReplyErrorFn,
}

// SAFETY: libfuse low-level requests may be replied asynchronously exactly once.
// Async write transfers that reply obligation to the spawned task.
unsafe impl Send for FuseReqPtr {}

impl FuseReqPtr {
    fn new(raw: *mut raw::FuseReq) -> Self {
        Self {
            raw,
            #[cfg(test)]
            reply_write_fn: raw::fuse_reply_write,
            #[cfg(test)]
            reply_error_fn: raw::fuse_reply_err,
        }
    }

    #[cfg(test)]
    fn for_test(
        raw: *mut raw::FuseReq,
        reply_write_fn: FuseReplyWriteFn,
        reply_error_fn: FuseReplyErrorFn,
    ) -> Self {
        Self {
            raw,
            reply_write_fn,
            reply_error_fn,
        }
    }

    fn reply_write(self, written: size_t) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        #[cfg(test)]
        unsafe {
            (self.reply_write_fn)(self.raw, written)
        }
        #[cfg(not(test))]
        unsafe {
            raw::fuse_reply_write(self.raw, written)
        }
    }

    fn reply_error(self, errno: c_int) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        #[cfg(test)]
        unsafe {
            (self.reply_error_fn)(self.raw, errno)
        }
        #[cfg(not(test))]
        unsafe {
            raw::fuse_reply_err(self.raw, errno)
        }
    }
}

#[derive(Clone, Copy)]
struct LinuxFuseAsyncWriteMetricConfig {
    cache_policy: LinuxFuseCachePolicy,
    connection_config: LinuxFuseConnectionConfig,
}

struct FuseWriteTaskGuard {
    request: Option<FuseReqPtr>,
    request_ticket: Option<LinuxFuseRequestTicket>,
    reservation: Option<LinuxFuseWriteReservation>,
    writes: Arc<LinuxFuseAsyncWrites>,
    vfs: Arc<StudioVfs>,
    started: Instant,
    requested_bytes: usize,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
}

enum FuseWriteTaskResult {
    AdmissionRejected(i32),
    Completed(Result<u32, i32>),
    Cancelled,
}

impl FuseWriteTaskGuard {
    fn attach_reservation(&mut self, reservation: LinuxFuseWriteReservation) {
        self.reservation = Some(reservation);
    }

    fn finish(mut self, result: FuseWriteTaskResult) {
        let request = self.request.take().expect("request guard owns one reply");
        let (reply_status, errored, completion) = match result {
            FuseWriteTaskResult::AdmissionRejected(errno) => (
                request.reply_error(errno),
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            ),
            FuseWriteTaskResult::Completed(Ok(written)) => {
                let status = request.reply_write(written as size_t);
                let completion = if status == 0 {
                    LinuxFuseAsyncWriteCompletion::Success
                } else {
                    LinuxFuseAsyncWriteCompletion::Failed(libc::EIO)
                };
                (status, status != 0, completion)
            }
            FuseWriteTaskResult::Completed(Err(errno)) => (
                request.reply_error(errno),
                true,
                LinuxFuseAsyncWriteCompletion::Failed(errno),
            ),
            FuseWriteTaskResult::Cancelled => (
                request.reply_error(libc::EIO),
                true,
                LinuxFuseAsyncWriteCompletion::Cancelled,
            ),
        };
        if reply_status != 0 {
            warn!(reply_status, "libfuse rejected async write reply");
        }
        self.vfs
            .record_fuse_write_callback(self.requested_bytes, self.started.elapsed(), errored);
        drop(self.reservation.take());
        self.request_ticket
            .take()
            .expect("request sequence completes once")
            .complete(completion);
        record_linux_async_write_snapshot(&self.vfs, &self.writes, self.metric_config);
    }
}

impl Drop for FuseWriteTaskGuard {
    fn drop(&mut self) {
        let Some(request) = self.request.take() else {
            return;
        };
        let reply_status = request.reply_error(libc::EIO);
        if reply_status != 0 {
            warn!(reply_status, "libfuse rejected async write reply");
        }
        self.vfs
            .record_fuse_write_callback(self.requested_bytes, self.started.elapsed(), true);
        drop(self.reservation.take());
        drop(self.request_ticket.take());
        record_linux_async_write_snapshot(&self.vfs, &self.writes, self.metric_config);
    }
}

fn record_linux_async_write_snapshot(
    vfs: &StudioVfs,
    writes: &LinuxFuseAsyncWrites,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
) {
    record_linux_async_write_stats(vfs, writes.stats(), metric_config);
}

fn record_linux_async_write_stats(
    vfs: &StudioVfs,
    stats: LinuxFuseAsyncWriteStats,
    metric_config: LinuxFuseAsyncWriteMetricConfig,
) {
    let (profile, kernel_writeback_cache) = match metric_config.cache_policy {
        LinuxFuseCachePolicy::Direct => ("direct", false),
        LinuxFuseCachePolicy::Kernel => ("kernel", true),
    };
    vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
        snapshot_generation: stats.snapshot_generation,
        enabled: linux_fuse_async_write_enabled(),
        linux_fuse_profile: profile.into(),
        fuse_max_background: u16::try_from(metric_config.connection_config.max_background).ok(),
        fuse_congestion_threshold: u16::try_from(
            metric_config.connection_config.congestion_threshold,
        )
        .ok(),
        kernel_writeback_cache: Some(kernel_writeback_cache),
        max_pending_bytes: Some(LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_BYTES),
        max_pending_writes: Some(LINUX_FUSE_ASYNC_WRITE_MAX_PENDING_WRITES),
        active_requests: stats.active_requests,
        admission_waiters: stats.admission_waiters,
        pending_handles: stats.pending_handles,
        pending_writes: stats.pending_writes,
        pending_bytes: stats.pending_bytes,
        peak_pending_bytes: stats.peak_pending_bytes,
        max_handle_pending_writes: stats.max_handle_pending_writes,
        peak_handle_pending_writes: stats.peak_handle_pending_writes,
        completed_writes: stats.completed_writes,
        failed_writes: stats.failed_writes,
        task_failed_writes: stats.task_failed_writes,
        cancelled_writes: stats.cancelled_writes,
        admission_waits: stats.admission_waits,
        admission_wait_total_ms: stats.admission_wait_total_ms,
        barrier_waits: stats.barrier_waits,
        barrier_wait_total_ms: stats.barrier_wait_total_ms,
        ..VfsFuseAsyncWriteSnapshot::default()
    });
}

struct LinuxFuseWriteRequest {
    fh: u64,
    offset: u64,
    local_sync_policy: LocalSyncPolicy,
}

fn linux_fuse_loop_thread_count(max_background: u32) -> c_uint {
    let parallelism = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);
    linux_fuse_loop_thread_count_for_parallelism(parallelism, max_background)
}

fn linux_fuse_loop_thread_count_for_parallelism(parallelism: usize, max_background: u32) -> c_uint {
    u32::try_from(parallelism)
        .unwrap_or(u32::MAX)
        // FUSE callbacks can block on local disk and remote object I/O, so keep
        // a small surplus over CPU count without exceeding kernel queue bounds.
        .saturating_mul(2)
        .clamp(1, max_background.max(1))
}

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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_async_dio(
    _userdata: *mut c_void,
) -> c_int {
    if linux_fuse_env_flag("STUDIOFS_FUSE_ASYNC_DIO") {
        1
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_writeback_cache(
    _userdata: *mut c_void,
) -> c_int {
    if linux_fuse_cache_policy_from_env() == LinuxFuseCachePolicy::Kernel {
        1
    } else {
        0
    }
}

fn current_uid() -> u32 {
    // SAFETY: `geteuid` has no preconditions.
    unsafe { libc::geteuid() }
}

fn current_gid() -> u32 {
    // SAFETY: `getegid` has no preconditions.
    unsafe { libc::getegid() }
}

fn linux_fuse_cache_policy_from_env() -> LinuxFuseCachePolicy {
    // ponytail: internal benchmark knob; promote only after studiofs-bench proves it.
    std::env::var("STUDIOFS_LINUX_FUSE_CACHE_POLICY")
        .ok()
        .and_then(|value| linux_fuse_cache_policy_from_str(&value))
        .unwrap_or(LinuxFuseCachePolicy::Direct)
}

fn linux_fuse_env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .is_some_and(|value| linux_fuse_env_flag_value(&value))
}

fn linux_fuse_env_flag_value(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn linux_fuse_async_write_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        linux_fuse_async_write_enabled_from_env(
            std::env::var("STUDIOFS_FUSE_ASYNC_WRITE").ok().as_deref(),
            std::env::var("STUDIOFS_FUSE_ASYNC_WRITE_DISABLED")
                .ok()
                .as_deref(),
        )
    })
}

fn linux_fuse_async_write_enabled_from_env(
    async_write: Option<&str>,
    disabled: Option<&str>,
) -> bool {
    if disabled.is_some_and(linux_fuse_env_flag_value) {
        return false;
    }
    async_write.is_none_or(linux_fuse_env_flag_value)
}

fn linux_fuse_metadata_admission_enabled() -> bool {
    true
}

fn linux_fuse_setattr_args_are_attr_only(args: &SetattrArgs) -> bool {
    args.size.is_none()
        && (args.mode.is_some()
            || args.uid.is_some()
            || args.gid.is_some()
            || !matches!(args.atime, TimeSpec::Omit)
            || !matches!(args.mtime, TimeSpec::Omit)
            || !matches!(args.crtime, TimeSpec::Omit)
            || args.win_attrs.is_some())
}

fn linux_fuse_cache_policy_from_str(value: &str) -> Option<LinuxFuseCachePolicy> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "direct" | "direct_io" | "dio" | "0" | "false" | "no" | "off" => {
            Some(LinuxFuseCachePolicy::Direct)
        }
        "kernel" | "cached" | "page_cache" | "1" | "true" | "yes" | "on" => {
            Some(LinuxFuseCachePolicy::Kernel)
        }
        _ => None,
    }
}

fn argv_strings(volume_name: &str) -> Vec<String> {
    argv_strings_for_cache_policy(volume_name, linux_fuse_cache_policy_from_env())
}

fn argv_strings_for_cache_policy(
    volume_name: &str,
    _cache_policy: LinuxFuseCachePolicy,
) -> Vec<String> {
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
        let max_background = self
            .state
            .as_ref()
            .map(|state| state.connection_config.max_background)
            .unwrap_or_else(|| LinuxFuseConnectionConfig::default().max_background);
        let loop_threads = linux_fuse_loop_thread_count(max_background);
        // SAFETY: `self.raw` is a live libfuse session owned by this wrapper.
        // Keep idle workers equal to the cap so bursty filesystem I/O does not
        // pay thread create/destroy overhead between request spikes.
        let result = unsafe {
            studiofs_linux_fuse_session_loop_mt(self.raw.as_ptr(), loop_threads, loop_threads)
        };
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
    let started = Instant::now();
    match state.read(ino, size, off, fi) {
        Ok(buffer) => {
            state
                .vfs
                .record_fuse_read_callback(buffer.len(), started.elapsed(), false);
            // SAFETY: `buffer` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, buffer.as_ptr().cast::<c_char>(), buffer.len()) };
        }
        Err(errno) => {
            state
                .vfs
                .record_fuse_read_callback(size, started.elapsed(), true);
            reply_error(req, errno);
        }
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
    let started = Instant::now();
    match state.queue_async_write(req, ino, buf, size, off, fi, started) {
        Ok(true) => return,
        Ok(false) => {}
        Err(errno) => {
            state
                .vfs
                .record_fuse_write_callback(size, started.elapsed(), true);
            reply_error(req, errno);
            return;
        }
    }
    match state.write(ino, buf, size, off, fi) {
        Ok(written) => {
            state
                .vfs
                .record_fuse_write_callback(size, started.elapsed(), false);
            // SAFETY: `req` is live and this consumes the callback reply.
            unsafe { raw::fuse_reply_write(req, written as size_t) };
        }
        Err(errno) => {
            state
                .vfs
                .record_fuse_write_callback(size, started.elapsed(), true);
            reply_error(req, errno);
        }
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
        if linux_fuse_metadata_admission_enabled() && linux_fuse_setattr_args_are_attr_only(&args) {
            return self
                .vfs
                .setattr_attr_only_deferred_metadata(ino, args, &self.runtime_handle)
                .map_err(|error| vfs_err_to_errno(&error));
        }
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
        require_file_info(fi)?;
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        let attr = if linux_fuse_metadata_admission_enabled() {
            self.vfs.create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .create(parent, name, self.mount_uid, self.mount_gid, mode)
        }
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
        let mode = (mode & 0o7777) as u16;
        let result = if linux_fuse_metadata_admission_enabled() {
            self.vfs.mkdir_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .mkdir(parent, name, self.mount_uid, self.mount_gid, mode)
        };
        result.map_err(|error| vfs_err_to_errno(&error))
    }

    fn mknod(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        let result = if linux_fuse_metadata_admission_enabled() {
            self.vfs.create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
        } else {
            self.vfs
                .create(parent, name, self.mount_uid, self.mount_gid, mode)
        };
        result.map_err(|error| vfs_err_to_errno(&error))
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
        let request = self.prepare_write_metadata(buf, size, offset, fi)?;
        with_write_data_slice(buf, size, |data| {
            self.runtime_handle
                .block_on(self.vfs.write_fh_with_deferred_materialize(
                    request.fh,
                    ino,
                    request.offset,
                    data,
                    request.local_sync_policy,
                ))
        })?
        .map_err(|error| vfs_err_to_errno(&error))
    }

    fn queue_async_write(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
        started: Instant,
    ) -> Result<bool, i32> {
        if !linux_fuse_async_write_enabled() {
            return Ok(false);
        }
        let request = self.prepare_write_metadata(buf, size, offset, fi)?;
        if !request.local_sync_policy.is_opportunistic() {
            return Ok(false);
        }

        let fh = request.fh;
        let write_offset = request.offset;
        let writes = Arc::clone(&self.async_writes);
        let vfs = self.vfs.clone();
        let metric_config = self.metric_config;
        let request_ticket = self.start_live_async_write_request(fh)?;
        let mut guard = FuseWriteTaskGuard {
            request: Some(FuseReqPtr::new(req)),
            request_ticket: Some(request_ticket),
            reservation: None,
            writes: Arc::clone(&writes),
            vfs: Arc::clone(&vfs),
            started,
            requested_bytes: size,
            metric_config,
        };
        record_linux_async_write_snapshot(&vfs, &writes, metric_config);

        let admitted = match with_write_data_slice(buf, size, |data| {
            writes
                .admit(data.len(), |waiting_stats| {
                    record_linux_async_write_stats(&vfs, waiting_stats, metric_config);
                })
                .map(|reservation| {
                    guard.attach_reservation(reservation);
                    record_linux_async_write_snapshot(&vfs, &writes, metric_config);
                    Bytes::copy_from_slice(data)
                })
        }) {
            Ok(admitted) => admitted,
            Err(errno) => {
                guard.finish(FuseWriteTaskResult::AdmissionRejected(errno));
                return Ok(true);
            }
        };
        let data = match admitted {
            Ok(data) => data,
            Err(errno) => {
                guard.finish(FuseWriteTaskResult::AdmissionRejected(errno));
                return Ok(true);
            }
        };
        let cancellation = writes.task_cancellation_token();
        self.runtime_handle.spawn(async move {
            let result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => FuseWriteTaskResult::Cancelled,
                result = vfs.write_fh_owned_with_deferred_materialize(
                    fh,
                    ino,
                    write_offset,
                    data,
                    LocalSyncPolicy::Opportunistic,
                ) => FuseWriteTaskResult::Completed(
                    result.map_err(|error| vfs_err_to_errno(&error)),
                ),
            };
            guard.finish(result);
        });
        Ok(true)
    }

    fn start_live_async_write_request(&self, fh: u64) -> Result<LinuxFuseRequestTicket, i32> {
        let request_ticket = self.async_writes.start_request(fh)?;
        if self.vfs.get_open_file(fh).is_some() {
            return Ok(request_ticket);
        }

        request_ticket.complete(LinuxFuseAsyncWriteCompletion::Failed(libc::EBADF));
        self.async_writes.forget_handle(fh);
        record_linux_async_write_snapshot(&self.vfs, &self.async_writes, self.metric_config);
        Err(libc::EBADF)
    }

    fn prepare_write_metadata(
        &self,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<LinuxFuseWriteRequest, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        if buf.is_null() && size > 0 {
            return Err(libc::EINVAL);
        }
        let local_sync_policy = self.local_sync_policy_for_handle(fh);
        Ok(LinuxFuseWriteRequest {
            fh,
            offset,
            local_sync_policy,
        })
    }

    fn local_sync_policy_for_handle(&self, fh: u64) -> LocalSyncPolicy {
        self.vfs
            .get_open_file(fh)
            .filter(|open_file| open_file.flags.contains(OpenFlags::SYNC))
            .map(|_| LocalSyncPolicy::WriteThrough)
            .unwrap_or(LocalSyncPolicy::Opportunistic)
    }

    fn flush(&self, fh: u64) -> Result<(), i32> {
        let watermark = self.async_writes.watermark(fh);
        let async_result = self.async_writes.wait_through(watermark);
        let local_result = normalize_close_barrier_result(
            self.runtime_handle
                .block_on(self.vfs.flush_fh(fh))
                .map_err(|error| vfs_err_to_errno(&error)),
        );
        merge_async_and_local_barrier_results(async_result, local_result)
    }

    fn fsync(&self, file_handle: Option<u64>, _ino: u64, _datasync: bool) -> Result<(), i32> {
        let fh = file_handle
            .filter(|fh| self.vfs.get_open_file(*fh).is_some())
            .ok_or(libc::EBADF)?;
        let watermark = self.async_writes.watermark(fh);
        let async_result = self.async_writes.wait_through(watermark);
        // StudioVfs does not currently distinguish fdatasync from fsync on
        // this handle-backed path, so both use the strict handle flush barrier.
        let local_result = self
            .runtime_handle
            .block_on(self.vfs.flush_fh(fh))
            .map_err(|error| vfs_err_to_errno(&error));
        merge_async_and_local_barrier_results(async_result, local_result)
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        let open_file = self.vfs.get_open_file(fh).ok_or(libc::EBADF)?;
        let local_sync_policy = if open_file.flags.contains(OpenFlags::SYNC) {
            LocalSyncPolicy::WriteThrough
        } else {
            LocalSyncPolicy::Opportunistic
        };
        let watermark = self.async_writes.close_handle(fh);
        let _forget_guard = LinuxFuseHandleLedgerGuard::new(Arc::clone(&self.async_writes), fh);
        let async_result = self.async_writes.wait_through(watermark);
        let mode = linux_fuse_release_mode(
            local_sync_policy,
            self.vfs.inode_has_local_dirty_state(open_file.ino),
        );
        let local_result = match mode {
            LinuxFuseReleaseMode::Synchronous => self
                .runtime_handle
                .block_on(self.vfs.release_fh_with_outcome(fh))
                .map(|_| ())
                .map_err(|error| vfs_err_to_errno(&error)),
            LinuxFuseReleaseMode::Deferred => {
                self.vfs
                    .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
                Ok(())
            }
        };
        merge_async_and_local_barrier_results(
            async_result,
            normalize_close_barrier_result(local_result),
        )
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

fn with_write_data_slice<T>(
    buf: *const c_char,
    size: usize,
    f: impl FnOnce(&[u8]) -> T,
) -> Result<T, i32> {
    if size == 0 {
        return Ok(f(&[]));
    }
    if buf.is_null() {
        return Err(libc::EINVAL);
    }
    // SAFETY: libfuse passes `buf` as a request-owned byte slice valid for the
    // duration of this write callback; the closure cannot return a borrowed
    // reference to it because the slice has no named lifetime.
    Ok(f(unsafe {
        std::slice::from_raw_parts(buf.cast::<u8>(), size)
    }))
}

fn linux_fuse_open_cache_policy(flags: c_int) -> LinuxFuseOpenCachePolicy {
    linux_fuse_open_cache_policy_for(flags, linux_fuse_cache_policy_from_env())
}

fn linux_fuse_open_cache_policy_for(
    flags: c_int,
    cache_policy: LinuxFuseCachePolicy,
) -> LinuxFuseOpenCachePolicy {
    if cache_policy == LinuxFuseCachePolicy::Direct {
        return LinuxFuseOpenCachePolicy {
            direct_io: true,
            keep_cache: false,
            parallel_direct_writes: linux_fuse_env_flag("STUDIOFS_FUSE_ASYNC_DIO"),
        };
    }

    let (open_flags, trunc) = posix_flags_to_open_flags(flags);
    let write_capable = trunc
        || open_flags.contains(OpenFlags::WRONLY)
        || open_flags.contains(OpenFlags::RDWR)
        || flags & libc::O_APPEND != 0;
    LinuxFuseOpenCachePolicy {
        direct_io: false,
        keep_cache: !write_capable,
        parallel_direct_writes: false,
    }
}

fn fuse_flag(value: bool) -> c_int {
    if value { 1 } else { 0 }
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
        let policy = linux_fuse_open_cache_policy(file_info_flags(fi));
        studiofs_linux_fuse_file_info_init(
            fi,
            fh,
            fuse_flag(policy.direct_io),
            fuse_flag(policy.keep_cache),
            fuse_flag(policy.parallel_direct_writes),
        );
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
        let policy = linux_fuse_open_cache_policy(file_info_flags(fi));
        studiofs_linux_fuse_file_info_init(
            fi,
            fh,
            fuse_flag(policy.direct_io),
            fuse_flag(policy.keep_cache),
            fuse_flag(policy.parallel_direct_writes),
        );
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
    use std::time::{Duration, Instant};

    use studiofs_vfs::LocalSyncPolicy;

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

    fn write_via_linux_session(state: &super::SessionState, ino: u64, fh: u64, payload: &[u8]) {
        let mut file_info_storage = [0_u64; 32];
        let file_info = file_info_storage
            .as_mut_ptr()
            .cast::<super::raw::FuseFileInfo>();
        // SAFETY: the aligned scratch storage outlives this synchronous callback
        // and is large enough for the system libfuse `fuse_file_info` value.
        unsafe {
            super::studiofs_linux_fuse_file_info_init(file_info, fh, 1, 0, 0);
        }

        assert_eq!(
            state.write(ino, payload.as_ptr().cast(), payload.len(), 0, file_info),
            Ok(payload.len() as u32),
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    enum FakeFuseReply {
        Write(usize),
        Error(i32),
    }

    struct FakeFuseReplyState {
        status: libc::c_int,
        replies: Vec<FakeFuseReply>,
        observations: Vec<(u64, usize, usize)>,
        writes: Arc<super::LinuxFuseAsyncWrites>,
        vfs: Arc<studiofs_vfs::StudioVfs>,
    }

    impl FakeFuseReplyState {
        fn new(
            status: libc::c_int,
            writes: Arc<super::LinuxFuseAsyncWrites>,
            vfs: Arc<studiofs_vfs::StudioVfs>,
        ) -> Self {
            Self {
                status,
                replies: Vec::new(),
                observations: Vec::new(),
                writes,
                vfs,
            }
        }

        fn record(&mut self, reply: FakeFuseReply) -> libc::c_int {
            let callbacks = self
                .vfs
                .write_path_observability_snapshot()
                .fuse_callbacks
                .write
                .calls;
            let stats = self.writes.stats();
            self.replies.push(reply);
            self.observations
                .push((callbacks, stats.pending_writes, stats.active_requests));
            self.status
        }
    }

    unsafe extern "C" fn fake_fuse_reply_write(
        req: *mut super::raw::FuseReq,
        written: libc::size_t,
    ) -> libc::c_int {
        // SAFETY: guard tests pass a live `FakeFuseReplyState` as the request pointer.
        let state = unsafe { &mut *req.cast::<FakeFuseReplyState>() };
        state.record(FakeFuseReply::Write(written))
    }

    unsafe extern "C" fn fake_fuse_reply_error(
        req: *mut super::raw::FuseReq,
        errno: libc::c_int,
    ) -> libc::c_int {
        // SAFETY: guard tests pass a live `FakeFuseReplyState` as the request pointer.
        let state = unsafe { &mut *req.cast::<FakeFuseReplyState>() };
        state.record(FakeFuseReply::Error(errno))
    }

    fn make_async_write_guard(
        reply: &mut FakeFuseReplyState,
        writes: &Arc<super::LinuxFuseAsyncWrites>,
        vfs: &Arc<studiofs_vfs::StudioVfs>,
    ) -> super::FuseWriteTaskGuard {
        let request_ticket = writes.start_request(41).expect("start request");
        let reservation = writes.admit(8, |_| {}).expect("admit write");
        let request = super::FuseReqPtr::for_test(
            (reply as *mut FakeFuseReplyState).cast(),
            fake_fuse_reply_write,
            fake_fuse_reply_error,
        );
        super::FuseWriteTaskGuard {
            request: Some(request),
            request_ticket: Some(request_ticket),
            reservation: Some(reservation),
            writes: Arc::clone(writes),
            vfs: Arc::clone(vfs),
            started: Instant::now(),
            requested_bytes: 8,
            metric_config: super::LinuxFuseAsyncWriteMetricConfig {
                cache_policy: super::LinuxFuseCachePolicy::Direct,
                connection_config: default_connection_config(),
            },
        }
    }

    #[test]
    fn async_write_guard_successful_write_replies_before_releasing_owned_state() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("async-write-guard-success")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
        assert_eq!(reply.observations, [(0, 1, 1)]);
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 0));
        let stats = writes.stats();
        assert_eq!(
            (
                stats.pending_writes,
                stats.active_requests,
                stats.completed_writes,
                stats.failed_writes,
                stats.task_failed_writes,
            ),
            (0, 0, 1, 0, 0)
        );
    }

    #[test]
    fn async_write_guard_rejected_success_reply_records_eio() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-rejected-success",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(-1, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);
        let watermark = writes.watermark(41);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
        assert_eq!(writes.wait_through(watermark), Err(libc::EIO));
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 1));
        assert_eq!(writes.stats().failed_writes, 1);
    }

    #[test]
    fn async_write_guard_rejected_error_reply_retains_original_errno() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-rejected-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(-1, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);
        let watermark = writes.watermark(41);

        guard.finish(super::FuseWriteTaskResult::Completed(Err(libc::ENOSPC)));

        assert_eq!(reply.replies, [FakeFuseReply::Error(libc::ENOSPC)]);
        assert_eq!(writes.wait_through(watermark), Err(libc::ENOSPC));
        assert_eq!(writes.stats().failed_writes, 1);
    }

    #[test]
    fn async_write_guard_unfinished_drop_replies_eio_once() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-unfinished-drop",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        drop(guard);

        assert_eq!(reply.replies, [FakeFuseReply::Error(libc::EIO)]);
        assert_eq!(reply.observations, [(0, 1, 1)]);
        let callback = vfs.write_path_observability_snapshot().fuse_callbacks.write;
        assert_eq!((callback.calls, callback.errors), (1, 1));
        let stats = writes.stats();
        assert_eq!(
            (
                stats.pending_writes,
                stats.active_requests,
                stats.failed_writes,
                stats.task_failed_writes,
            ),
            (0, 0, 0, 1)
        );
    }

    #[test]
    fn async_write_guard_drop_after_finish_does_not_reply_twice() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "async-write-guard-finished-drop",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));
        let mut reply = FakeFuseReplyState::new(0, Arc::clone(&writes), Arc::clone(&vfs));
        let guard = make_async_write_guard(&mut reply, &writes, &vfs);

        guard.finish(super::FuseWriteTaskResult::Completed(Ok(8)));

        assert_eq!(reply.replies, [FakeFuseReply::Write(8)]);
    }

    #[test]
    fn argv_uses_system_libfuse_mount_options() {
        let args = super::argv_strings("demo");

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
        assert!(!args.iter().any(|arg| arg == "writeback_cache"));
        assert!(!args.iter().any(|arg| arg.contains("max_background")));
        assert!(!args.iter().any(|arg| arg.contains("congestion_threshold")));
    }

    #[test]
    fn argv_kernel_cache_policy_keeps_mount_options_portable() {
        let args =
            super::argv_strings_for_cache_policy("demo", super::LinuxFuseCachePolicy::Kernel);

        assert!(!args.iter().any(|arg| arg == "writeback_cache"));
    }

    #[test]
    fn conn_feature_helper_requests_writeback_cache_when_supported() {
        let writeback_cache_cap = 1_u64 << 16;
        // SAFETY: the C shim mutates and inspects a stack-local fuse_conn_info.
        let enabled = unsafe {
            super::studiofs_linux_fuse_conn_wants_writeback_cache_after_set(writeback_cache_cap)
        };

        assert_eq!(enabled, 1);
    }

    #[test]
    fn conn_feature_helper_requests_async_dio_when_supported() {
        let async_dio_cap = 1_u64 << 15;
        // SAFETY: the C shim mutates and inspects a stack-local fuse_conn_info.
        let enabled =
            unsafe { super::studiofs_linux_fuse_conn_wants_async_dio_after_set(async_dio_cap) };

        assert_eq!(enabled, 1);
    }

    #[test]
    fn async_write_reservation_releases_pending_bytes_on_drop() {
        let writes = Arc::new(super::LinuxFuseAsyncWrites::new(16, 4));

        let reservation = writes.admit(8, |_| {}).expect("admit write");
        let pending = writes.stats();
        assert_eq!((pending.pending_writes, pending.pending_bytes), (1, 8));

        drop(reservation);
        let released = writes.stats();
        assert_eq!((released.pending_writes, released.pending_bytes), (0, 0));
    }

    #[test]
    fn async_write_registration_after_release_returns_ebadf_without_orphan_ledger() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-async-write-release-race",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"release-race.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload = b"race";
        let mut file_info_storage = [0_u64; 32];
        let file_info = file_info_storage
            .as_mut_ptr()
            .cast::<super::raw::FuseFileInfo>();
        // SAFETY: the aligned scratch storage outlives this synchronous test
        // and is large enough for the system libfuse `fuse_file_info` value.
        unsafe {
            super::studiofs_linux_fuse_file_info_init(file_info, fh, 1, 0, 0);
        }
        let prepared = state
            .prepare_write_metadata(payload.as_ptr().cast(), payload.len(), 0, file_info)
            .expect("prepare async write");

        state.release(fh).expect("release file before registration");
        let result = state.start_live_async_write_request(prepared.fh);
        let stats = state.async_writes.stats();
        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;

        assert!(matches!(result, Err(errno) if errno == libc::EBADF));
        assert_eq!(
            (
                stats.active_requests,
                stats.pending_handles,
                stats.pending_writes,
                stats.pending_bytes,
                stats.failed_writes,
                snapshot.active_requests,
                snapshot.pending_handles,
                snapshot.pending_writes,
                snapshot.pending_bytes,
                snapshot.failed_writes,
                vfs.get_open_file(fh).is_none(),
            ),
            (0, 0, 0, 0, 1, 0, 0, 0, 0, 1, true),
        );
    }

    #[test]
    fn async_write_defaults_to_enabled_with_explicit_disable() {
        assert!(super::linux_fuse_env_flag_value("1"));
        assert!(super::linux_fuse_env_flag_value("true"));
        assert!(super::linux_fuse_env_flag_value("yes"));
        assert!(super::linux_fuse_env_flag_value("on"));
        assert!(!super::linux_fuse_env_flag_value("0"));
        assert!(!super::linux_fuse_env_flag_value("false"));
        assert!(super::linux_fuse_async_write_enabled_from_env(None, None));
        assert!(super::linux_fuse_async_write_enabled_from_env(
            Some("1"),
            None
        ));
        assert!(!super::linux_fuse_async_write_enabled_from_env(
            Some("0"),
            None
        ));
        assert!(!super::linux_fuse_async_write_enabled_from_env(
            Some("1"),
            Some("1")
        ));
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
    fn multithread_loop_uses_fuse_loop_config_api() {
        // SAFETY: the C shim returns its compile-time libfuse API selection.
        let uses_loop_config_api =
            unsafe { super::studiofs_linux_fuse_loop_config_uses_loop_config_api() };

        assert_eq!(uses_loop_config_api, 1);
    }

    #[test]
    fn loop_thread_count_scales_with_parallelism_and_background_queue() {
        assert_eq!(
            super::linux_fuse_loop_thread_count_for_parallelism(4, 16),
            8
        );
    }

    #[test]
    fn loop_thread_count_caps_at_background_queue() {
        assert_eq!(
            super::linux_fuse_loop_thread_count_for_parallelism(64, 16),
            16
        );
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
    fn file_info_init_sets_requested_cache_flags() {
        // SAFETY: the C shim initializes and inspects a stack-local fuse_file_info.
        let flags = unsafe { super::studiofs_linux_fuse_file_info_flags_after_init(0, 1, 1) };

        assert_eq!(flags, 6);
    }

    #[test]
    fn kernel_cache_policy_keeps_read_only_open_cache() {
        let policy = super::linux_fuse_open_cache_policy_for(
            libc::O_RDONLY,
            super::LinuxFuseCachePolicy::Kernel,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: true,
                parallel_direct_writes: false,
            }
        );
    }

    #[test]
    fn kernel_cache_policy_drops_write_capable_open_cache() {
        let policy = super::linux_fuse_open_cache_policy_for(
            libc::O_RDWR,
            super::LinuxFuseCachePolicy::Kernel,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: false,
                parallel_direct_writes: false,
            }
        );
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
    fn dirty_opportunistic_release_is_synchronous() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, true),
            super::LinuxFuseReleaseMode::Synchronous,
        );
    }

    #[test]
    fn clean_opportunistic_release_is_deferred() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::Opportunistic, false),
            super::LinuxFuseReleaseMode::Deferred,
        );
    }

    #[test]
    fn write_through_release_is_synchronous() {
        assert_eq!(
            super::linux_fuse_release_mode(LocalSyncPolicy::WriteThrough, false),
            super::LinuxFuseReleaseMode::Synchronous,
        );
    }

    #[test]
    fn merge_async_and_local_barrier_results_uses_computed_local_error() {
        let async_result = Ok(());
        let local_result = Err(libc::EIO);

        assert_eq!(
            super::merge_async_and_local_barrier_results(async_result, local_result),
            Err(libc::EIO),
        );
    }

    #[test]
    fn merge_async_and_local_barrier_results_prefers_async_error_when_both_fail() {
        let async_result = Err(libc::ENOSPC);
        let local_result = Err(libc::EIO);

        assert_eq!(
            super::merge_async_and_local_barrier_results(async_result, local_result),
            Err(libc::ENOSPC),
        );
    }

    #[test]
    fn flush_runs_vfs_barrier_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-flush-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"flush-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"local bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.flush(fh);

        assert_eq!(
            (result, vfs.inode_has_local_dirty_state(attr.ino)),
            (Err(libc::ENOSPC), false),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn handle_backed_fsync_runs_vfs_barrier_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-fsync-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"fsync-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"fsync local bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.fsync(Some(fh), attr.ino, true);

        assert_eq!(
            (result, vfs.inode_has_local_dirty_state(attr.ino)),
            (Err(libc::ENOSPC), false),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn release_closes_vfs_handle_after_async_error() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-release-after-async-error",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"release-error.dat", 0, 0, 0o644)
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let result = state.release(fh);

        assert_eq!(
            (
                result,
                vfs.get_open_file(fh).is_none(),
                vfs.file_session_stats().open_handles,
            ),
            (Err(libc::ENOSPC), true, 0),
        );
    }

    #[test]
    fn linux_flush_is_a_strict_local_durability_barrier() {
        let mut fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-strict-flush")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(studiofs_meta::ROOT_INO, b"strict-flush.dat", 0, 0, 0o644)
            .expect("create file");
        let ino = attr.ino;
        let fh = vfs
            .open_file(ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload: &[u8] = b"strict local durability";
        write_via_linux_session(&state, ino, fh, payload);
        assert!(vfs.inode_has_local_dirty_state(ino));

        state.flush(fh).expect("flush file");

        assert!(!vfs.inode_has_local_dirty_state(ino));
        state.release(fh).expect("release file");
        drop(state);
        drop(vfs);

        let reopened = fixture.reopen_vfs().expect("reopen VFS fixture");
        let read_fh = reopened
            .open_file(ino, studiofs_vfs::OpenFlags::RDONLY, false)
            .expect("open reopened file");
        let mut readback = vec![0; payload.len()];
        let read = fixture
            .runtime()
            .block_on(reopened.read_into_fh(read_fh, ino, 0, &mut readback))
            .expect("read reopened file");
        fixture
            .runtime()
            .block_on(reopened.release_fh(read_fh))
            .expect("release reopened file");

        assert_eq!(
            (read as usize, readback.as_slice()),
            (payload.len(), payload)
        );
    }

    #[test]
    fn linux_flush_after_async_error_still_materializes_prior_local_state() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-flush-error-materializes",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"flush-error-bytes.dat",
                0,
                0,
                0o644,
            )
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let payload: &[u8] = b"bytes before async error";
        write_via_linux_session(&state, attr.ino, fh, payload);
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let flush_result = state.flush(fh);
        let mut readback = vec![0; payload.len()];
        let read = fixture
            .runtime()
            .block_on(vfs.read_into_fh(fh, attr.ino, 0, &mut readback))
            .expect("read flushed file");

        assert_eq!(
            (
                flush_result,
                vfs.inode_has_local_dirty_state(attr.ino),
                read as usize,
                readback.as_slice(),
            ),
            (Err(libc::ENOSPC), false, payload.len(), payload),
        );
        state.release(fh).expect("release file");
    }

    #[test]
    fn linux_release_after_async_error_removes_handle_and_lifecycle() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-release-error-lifecycle",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let attr = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"release-error-lifecycle.dat",
                0,
                0,
                0o644,
            )
            .expect("create file");
        let fh = vfs
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        write_via_linux_session(&state, attr.ino, fh, b"dirty release bytes");
        let failed = state
            .async_writes
            .start_request(fh)
            .expect("start failed async write");
        failed.complete(super::LinuxFuseAsyncWriteCompletion::Failed(libc::ENOSPC));

        let release_result = state.release(fh);
        let sessions = vfs.file_session_stats();

        assert_eq!(
            (
                release_result,
                vfs.get_open_file(fh).is_none(),
                sessions.open_handles,
                sessions.active_sessions,
                vfs.inode_has_local_dirty_state(attr.ino),
            ),
            (Err(libc::ENOSPC), true, 0, 0, false),
        );
    }

    #[test]
    fn linux_barrier_does_not_wait_for_another_inode() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-inode-scoped-barrier",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let target = vfs
            .create(studiofs_meta::ROOT_INO, b"barrier-target.dat", 0, 0, 0o644)
            .expect("create target file");
        let unrelated = vfs
            .create(
                studiofs_meta::ROOT_INO,
                b"barrier-unrelated.dat",
                0,
                0,
                0o644,
            )
            .expect("create unrelated file");
        let target_fh = vfs
            .open_file(target.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open target file");
        let unrelated_fh = vfs
            .open_file(unrelated.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open unrelated file");
        let state = Arc::new(super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        ));
        write_via_linux_session(&state, target.ino, target_fh, b"target bytes");
        let unrelated_write = state
            .async_writes
            .start_request(unrelated_fh)
            .expect("start unrelated async write");
        let (result_tx, result_rx) = mpsc::channel();
        let barrier_state = Arc::clone(&state);
        let barrier = thread::spawn(move || {
            let _ = result_tx.send(barrier_state.flush(target_fh));
        });

        let result = result_rx.recv_timeout(Duration::from_millis(750));
        unrelated_write.complete(super::LinuxFuseAsyncWriteCompletion::Success);
        barrier.join().expect("join target barrier");
        state.release(target_fh).expect("release target file");
        state.release(unrelated_fh).expect("release unrelated file");

        assert_eq!(
            result.expect("target barrier waited for unrelated inode"),
            Ok(()),
        );
    }

    #[test]
    fn regular_file_fsync_without_handle_returns_ebadf_without_mount_global_wait() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-fsync-without-handle",
        )
        .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let target = vfs
            .create(studiofs_meta::ROOT_INO, b"target.dat", 0, 0, 0o644)
            .expect("create target file");
        let unrelated = vfs
            .create(studiofs_meta::ROOT_INO, b"unrelated.dat", 0, 0, 0o644)
            .expect("create unrelated file");
        let unrelated_fh = vfs
            .open_file(unrelated.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open unrelated file");
        let state = Arc::new(super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        ));
        let pending = state
            .async_writes
            .start_request(unrelated_fh)
            .expect("start unrelated async write");
        let (result_tx, result_rx) = mpsc::channel();
        let fsync_state = Arc::clone(&state);
        let fsync = thread::spawn(move || {
            let _ = result_tx.send(fsync_state.fsync(None, target.ino, false));
        });

        let result = result_rx.recv_timeout(Duration::from_millis(250));
        pending.complete(super::LinuxFuseAsyncWriteCompletion::Success);
        fsync.join().expect("join fsync callback");
        fixture
            .runtime()
            .block_on(vfs.release_fh(unrelated_fh))
            .expect("release unrelated handle");

        assert_eq!(
            result.expect("handle-less fsync waited for unrelated async write"),
            Err(libc::EBADF),
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
    fn create_rejects_null_file_info_before_touching_vfs() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-create-null-fi")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );

        assert!(matches!(
            state.create(1, b"null-fi.txt", libc::S_IFREG | 0o644, std::ptr::null()),
            Err(errno) if errno == libc::EINVAL
        ));
        assert!(fixture.vfs().lookup(1, b"null-fi.txt").is_err());
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
    fn setattr_args_attr_only_classifier_accepts_metadata_updates_without_size() {
        for args in [
            studiofs_meta::SetattrArgs {
                mode: Some(0o644),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                uid: Some(1000),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                gid: Some(1001),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                mtime: studiofs_meta::TimeSpec::Now,
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                win_attrs: Some(0),
                ..Default::default()
            },
        ] {
            assert!(super::linux_fuse_setattr_args_are_attr_only(&args));
        }
    }

    #[test]
    fn setattr_args_attr_only_classifier_rejects_empty_and_size_updates() {
        for args in [
            studiofs_meta::SetattrArgs::default(),
            studiofs_meta::SetattrArgs {
                size: Some(42),
                ..Default::default()
            },
            studiofs_meta::SetattrArgs {
                size: Some(42),
                mode: Some(0o644),
                ..Default::default()
            },
        ] {
            assert!(!super::linux_fuse_setattr_args_are_attr_only(&args));
        }
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
