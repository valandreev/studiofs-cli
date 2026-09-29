use super::*;
use std::collections::HashMap;
use std::ffi::{CStr, OsStr};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use crate::async_writes::{
    AsyncWriteLedger, AsyncWriteMetricsSnapshot, AsyncWriteTicket, BarrierOutcome,
    DEFAULT_MAX_PENDING_BYTES, DEFAULT_MAX_PENDING_WRITES,
};
use crate::cache_policy::{
    LinuxFuseCachePolicy, OpenCacheDecision, ReadAheadIntent, SyncReadBufferPool,
    SystemMemorySampler, decide_open_cache,
};
use crate::invalidation::{
    FuseInodeLiveness, FuseInvalidationGeometry, FuseInvalidationKind,
    FuseInvalidationMetricsSnapshot, FuseInvalidationQueue, FuseInvalidationReceiptOutcome,
    FuseInvalidationShutdownOutcome, FuseInvalidationWorker, FuseLookupReservation,
    FuseOpenHandleGuard,
};
use bytes::Bytes;
use libc::{mode_t, off_t, size_t, stat, statvfs};
use studiofs_bridge::fuse_open_reply::FuseOpenReplySerializer;
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr, vfs_err_to_errno,
};
use studiofs_meta::{FileType, InodeAttr, SetattrArgs, TimeSpec};
use studiofs_vfs::vfs::OsCacheParticipation;
use studiofs_vfs::{
    DirEntryWithAttr, KernelInvalidator, LocalSyncPolicy, OpenFlags, ReadAheadOwner, StatFs,
    StudioVfs, VfsFuseAsyncWriteSnapshot, VfsFuseConnectionSnapshot, VfsFuseInvalidationSnapshot,
};
use tokio::runtime::Handle;
use tracing::{debug, warn};

const TTL_SECONDS: f64 = 1.0;
const LINUX_FUSE_BLOCK_SIZE: u32 = 4096;
const LINUX_FUSE_MAX_READ_SIZE: usize = 4 * 1024 * 1024;
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const NAMESPACE_ATTR_INVALIDATION_TIMEOUT: Duration = Duration::from_secs(2);
const ASYNC_WRITE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
// Raw invalidation, lookup/open liveness, and generation retirement are wired,
// so unset Linux mounts can safely use the requested adaptive default.
const LINUX_FUSE_ADAPTIVE_DEFAULT_READY: bool = true;
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
        pub(crate) fn fuse_reply_none(req: *mut FuseReq);
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
    #[cfg(test)]
    fn studiofs_linux_fuse_dir_file_info_cache_readdir_after_init() -> c_int;
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
    fn studiofs_linux_fuse_low_level_ops_has_forget_callback(
        ops: *const raw::FuseLowLevelOps,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_sets_bounded_background(
        userdata: *mut c_void,
        max_background: u32,
        congestion_threshold: u32,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_effective_max_read(offered: u32) -> u32;
    #[cfg(test)]
    fn studiofs_linux_fuse_init_effective_max_readahead(offered: u32) -> u32;
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
    async_writes: Arc<AsyncWriteLedger>,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    connection_config: LinuxFuseConnectionConfig,
    cache_policy: LinuxFuseCachePolicy,
    available_memory: Mutex<SystemMemorySampler>,
    read_buffers: SyncReadBufferPool,
    open_reply_serializer: FuseOpenReplySerializer,
    inode_liveness: FuseInodeLiveness,
    open_liveness: Mutex<HashMap<u64, FuseOpenHandleGuard>>,
    invalidation_queue: FuseInvalidationQueue,
}

const _: fn() = assert_send_sync_static::<SessionState>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinuxFuseOpenCachePolicy {
    direct_io: bool,
    keep_cache: bool,
    parallel_direct_writes: bool,
    read_ahead_owner: ReadAheadOwner,
    memory_bypassed: bool,
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
            async_writes: Arc::new(AsyncWriteLedger::with_limits(
                DEFAULT_MAX_PENDING_BYTES,
                DEFAULT_MAX_PENDING_WRITES,
            )),
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            connection_config,
            cache_policy: linux_fuse_cache_policy_from_env(),
            available_memory: Mutex::new(SystemMemorySampler::system()),
            read_buffers: SyncReadBufferPool::new(),
            open_reply_serializer: FuseOpenReplySerializer::new(),
            inode_liveness: FuseInodeLiveness::default(),
            open_liveness: Mutex::new(HashMap::new()),
            invalidation_queue: FuseInvalidationQueue::default(),
        }
    }
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

#[derive(Clone, Copy, Debug)]
struct FuseReqPtr(*mut raw::FuseReq);

// SAFETY: libfuse low-level requests may be replied asynchronously exactly once.
// Async write transfers that reply obligation to the spawned task.
unsafe impl Send for FuseReqPtr {}

impl FuseReqPtr {
    fn reply_write(&mut self, written: size_t) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        unsafe { raw::fuse_reply_write(self.0, written) }
    }

    fn reply_error(&mut self, errno: c_int) -> c_int {
        // SAFETY: ownership of the single FUSE reply was transferred with this pointer.
        unsafe { raw::fuse_reply_err(self.0, errno) }
    }
}

trait FuseWriteReply: Send {
    fn reply_write(&mut self, written: size_t) -> c_int;
    fn reply_error(&mut self, errno: c_int) -> c_int;
}

impl FuseWriteReply for FuseReqPtr {
    fn reply_write(&mut self, written: size_t) -> c_int {
        Self::reply_write(self, written)
    }

    fn reply_error(&mut self, errno: c_int) -> c_int {
        Self::reply_error(self, errno)
    }
}

struct FuseWriteTaskGuard<R: FuseWriteReply> {
    reply: Option<R>,
    ticket: Option<AsyncWriteTicket>,
    metrics: Option<FuseWriteCallbackMetrics>,
}

struct FuseWriteCallbackMetrics {
    vfs: Arc<StudioVfs>,
    requested_bytes: usize,
    started: Instant,
}

#[derive(Clone, Copy)]
struct FuseWriteCallbackContext {
    req: *mut raw::FuseReq,
    started: Instant,
}

impl<R: FuseWriteReply> FuseWriteTaskGuard<R> {
    #[cfg(test)]
    fn new(reply: R, ticket: AsyncWriteTicket) -> Self {
        Self {
            reply: Some(reply),
            ticket: Some(ticket),
            metrics: None,
        }
    }

    fn with_metrics(
        reply: R,
        ticket: AsyncWriteTicket,
        vfs: Arc<StudioVfs>,
        requested_bytes: usize,
        started: Instant,
    ) -> Self {
        Self {
            reply: Some(reply),
            ticket: Some(ticket),
            metrics: Some(FuseWriteCallbackMetrics {
                vfs,
                requested_bytes,
                started,
            }),
        }
    }

    fn complete(mut self, result: Result<u32, i32>) -> c_int {
        let mut reply = self
            .reply
            .take()
            .expect("write task reply must be owned until completion");
        let ticket = self
            .ticket
            .take()
            .expect("write task ticket must be owned until completion");
        let reply_result = match result {
            Ok(written) => reply.reply_write(written as size_t),
            Err(errno) => reply.reply_error(errno),
        };
        if reply_result != 0 {
            debug!(
                result = reply_result,
                "Linux FUSE async write reply was rejected"
            );
        }
        if let Some(metrics) = self.metrics.take() {
            metrics.vfs.record_fuse_write_callback(
                metrics.requested_bytes,
                metrics.started.elapsed(),
                result.is_err(),
            );
        }
        ticket.complete(result.map(|_| ()));
        reply_result
    }
}

impl<R: FuseWriteReply> Drop for FuseWriteTaskGuard<R> {
    fn drop(&mut self) {
        let Some(mut reply) = self.reply.take() else {
            return;
        };
        let reply_result = reply.reply_error(libc::EIO);
        if reply_result != 0 {
            debug!(
                result = reply_result,
                "Linux FUSE cancelled async write EIO reply was rejected"
            );
        }
        if let Some(metrics) = self.metrics.take() {
            metrics.vfs.record_fuse_write_callback(
                metrics.requested_bytes,
                metrics.started.elapsed(),
                true,
            );
        }
        if let Some(ticket) = self.ticket.take() {
            ticket.complete(Err(libc::EIO));
        }
    }
}

fn finish_async_barrier_with<T>(
    outcome: BarrierOutcome,
    vfs_barrier: impl FnOnce() -> Result<T, i32>,
) -> Result<T, i32> {
    let async_errno = match outcome {
        BarrierOutcome::Complete(errno) => errno,
        BarrierOutcome::TimedOut => Some(libc::EIO),
    };
    let vfs_result = vfs_barrier();
    async_errno.map_or(vfs_result, Err)
}

struct ForgetClosedHandle<'a> {
    ledger: &'a AsyncWriteLedger,
    fh: u64,
}

impl Drop for ForgetClosedHandle<'_> {
    fn drop(&mut self) {
        let forgotten = self.ledger.forget_handle(self.fh);
        debug_assert!(
            forgotten,
            "closed handle barrier must consume all pending writes and errors"
        );
    }
}

struct LinuxFuseWriteRequest {
    fh: u64,
    offset: u64,
    local_sync_policy: LocalSyncPolicy,
}

#[derive(Clone, Copy, Debug)]
struct LinuxFusePreparedOpen {
    fh: u64,
    cache: LinuxFuseOpenCachePolicy,
    disposition: studiofs_vfs::vfs::OsCacheDisposition,
    reason: studiofs_vfs::vfs::OsCacheReason,
}

#[derive(Debug)]
struct LinuxFuseAllocatedOpen {
    ino: u64,
    fh: u64,
    flags: c_int,
}

struct LinuxFuseAllocatedCreate {
    open: LinuxFuseAllocatedOpen,
    lookup: FuseLookupReservation,
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
    userdata: *mut c_void,
) -> c_int {
    let cache_policy = callback_state_from_userdata(userdata)
        .map(|state| state.cache_policy)
        .unwrap_or_else(linux_fuse_cache_policy_from_env);
    if cache_policy == LinuxFuseCachePolicy::Kernel {
        1
    } else {
        0
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_record_connection(
    userdata: *mut c_void,
    requested_max_read: u32,
    requested_max_readahead: u32,
    effective_max_read: u32,
    effective_max_readahead: u32,
    effective_max_write: u32,
    effective_max_background: u32,
    effective_congestion_threshold: u32,
    capable_features: u64,
    enabled_features: u64,
) {
    let Some(state) = callback_state_from_userdata(userdata) else {
        return;
    };
    let cache_policy = match state.cache_policy {
        LinuxFuseCachePolicy::Direct => "direct",
        LinuxFuseCachePolicy::Adaptive => "adaptive",
        LinuxFuseCachePolicy::Kernel => "kernel",
    };
    state
        .vfs
        .record_fuse_connection_snapshot(VfsFuseConnectionSnapshot {
            snapshot_generation: 1,
            cache_policy: cache_policy.into(),
            requested_max_read_bytes: requested_max_read,
            requested_max_readahead_bytes: requested_max_readahead,
            effective_max_read_bytes: effective_max_read,
            effective_max_readahead_bytes: effective_max_readahead,
            effective_max_write_bytes: effective_max_write,
            effective_max_background: Some(effective_max_background),
            effective_congestion_threshold: Some(effective_congestion_threshold),
            capable_features,
            enabled_features,
        });
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
    let value = std::env::var("STUDIOFS_LINUX_FUSE_CACHE_POLICY").ok();
    let selection = LinuxFuseCachePolicy::parse(value.as_deref());
    if selection.warning_required {
        warn!(
            value = value.as_deref().unwrap_or_default(),
            "unknown STUDIOFS_LINUX_FUSE_CACHE_POLICY; using adaptive policy"
        );
    }
    linux_fuse_effective_cache_policy_for_value(value.as_deref(), LINUX_FUSE_ADAPTIVE_DEFAULT_READY)
}

fn linux_fuse_effective_cache_policy_for_value(
    value: Option<&str>,
    adaptive_default_ready: bool,
) -> LinuxFuseCachePolicy {
    let selection = LinuxFuseCachePolicy::parse(value);
    if !adaptive_default_ready && value.is_none() {
        LinuxFuseCachePolicy::Direct
    } else {
        selection.policy
    }
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
        "-o".to_owned(),
        format!("max_read={LINUX_FUSE_MAX_READ_SIZE}"),
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
    invalidation_worker: Option<FuseInvalidationWorker>,
    invalidation_metrics_generation: Arc<AtomicU64>,
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
            let invalidation_queue = state.invalidation_queue.clone();
            let notify_target = SessionInvalidationTarget {
                session: raw,
                state: Arc::clone(&invalidation_state),
            };
            let invalidation_metrics_generation = Arc::new(AtomicU64::new(0));
            let metrics_vfs = Arc::clone(&state.vfs);
            let worker_metrics_generation = Arc::clone(&invalidation_metrics_generation);
            let invalidation_worker = match FuseInvalidationWorker::spawn_with_observer(
                invalidation_queue.clone(),
                state.inode_liveness.clone(),
                move |ino, geometry| notify_target.notify(ino, geometry),
                move |metrics| {
                    record_invalidation_metrics(&metrics_vfs, &worker_metrics_generation, metrics);
                },
            ) {
                Ok(worker) => worker,
                Err(error) => {
                    // SAFETY: the session is mounted but no callback worker
                    // retained its pointer because thread creation failed.
                    unsafe {
                        raw::fuse_session_unmount(raw.as_ptr());
                        raw::fuse_session_destroy(raw.as_ptr());
                    }
                    return Err(error);
                }
            };
            state
                .vfs
                .set_kernel_invalidator(Arc::new(LinuxFuseKernelInvalidator {
                    queue: invalidation_queue,
                }));
            let session = Self {
                raw,
                state: Some(state),
                invalidation_state,
                invalidation_worker: Some(invalidation_worker),
                invalidation_metrics_generation,
                signal_handlers_installed: false,
            };
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
        // SAFETY: the C shim returns a static ops table. Userdata is null for
        // this lifecycle-only smoke; init supports null userdata and every
        // request callback rejects a missing SessionState before dereferencing.
        let raw = NonNull::new(unsafe {
            studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                studiofs_linux_fuse_low_level_ops(),
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
                invalidation_worker: None,
                invalidation_metrics_generation: Arc::new(AtomicU64::new(0)),
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
}

struct LinuxFuseKernelInvalidator {
    queue: FuseInvalidationQueue,
}

struct SessionInvalidationTarget {
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

// SAFETY: the target is moved into the sole invalidation worker. Its session
// pointer is used only while `SessionInvalidationState` admits a notification.
unsafe impl Send for SessionInvalidationTarget {}

impl KernelInvalidator for LinuxFuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.enqueue(ino, FuseInvalidationKind::Data);
    }

    fn inval_inode_attrs(&self, ino: u64) {
        self.enqueue(ino, FuseInvalidationKind::Attributes);
    }
}

impl LinuxFuseKernelInvalidator {
    fn enqueue(&self, ino: u64, kind: FuseInvalidationKind) {
        if self.queue.push(ino, kind).is_err() {
            debug!(
                ino,
                ?kind,
                "Linux FUSE invalidation skipped after queue shutdown"
            );
        }
    }
}

impl SessionInvalidationTarget {
    fn notify(&self, ino: u64, geometry: FuseInvalidationGeometry) -> Result<(), ()> {
        let Some(_active) = self.state.begin_invalidation() else {
            debug!(
                ino,
                offset = geometry.offset,
                len = geometry.length,
                "Linux FUSE inode invalidation skipped after session shutdown"
            );
            return Err(());
        };
        // SAFETY: `session` comes from a mounted libfuse3 session and the
        // invalidation guard prevents teardown from destroying it during the call.
        let result = unsafe {
            studiofs_linux_fuse_notify_inval_inode(
                self.session.as_ptr(),
                ino,
                geometry.offset,
                geometry.length,
            )
        };
        if result != 0 && result != -libc::ENOENT {
            warn!(
                ino,
                offset = geometry.offset,
                len = geometry.length,
                result,
                "Linux FUSE inode invalidation failed"
            );
            Err(())
        } else {
            Ok(())
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_destroy(userdata: *mut c_void) {
    let _ = userdata;
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_forget(
    req: *mut raw::FuseReq,
    ino: raw::FuseIno,
    nlookup: u64,
) {
    forget_request_with(callback_state(req), ino, nlookup, || {
        // SAFETY: FORGET requests must receive exactly one non-error reply.
        unsafe { raw::fuse_reply_none(req) };
    });
}

fn forget_request_with(
    state: Option<&SessionState>,
    ino: u64,
    nlookup: u64,
    reply_none: impl FnOnce(),
) {
    if let Some(state) = state {
        state.inode_liveness.forget(ino, nlookup);
    }
    reply_none();
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
        Ok(allocated) => {
            let result = state.run_create_reply_transaction(allocated, |attr, prepared, lookup| {
                reply_create(req, state, fi, attr, prepared, lookup)
            });
            if let Err(errno) = result {
                reply_error(req, errno);
            }
        }
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
    let lookup = state.inode_liveness.reserve_lookup(ino);
    let result = with_name_bytes(newname, |newname| state.link(ino, newparent, newname))
        .unwrap_or(Err(libc::EINVAL));
    match result {
        Ok((attr, cache_safe)) => {
            let attr_timeout = if cache_safe { TTL_SECONDS } else { 0.0 };
            reply_reserved_entry_with_timeout(state, &attr, lookup, attr_timeout, |entry| {
                // SAFETY: `entry` is initialized and valid for this synchronous reply.
                unsafe { raw::fuse_reply_entry(req, entry) }
            });
        }
        Err(errno) => reply_error(req, errno),
    }
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
    match state.run_open_reply_transaction(
        ino,
        || state.open(ino, fi),
        |prepared| reply_file_open(req, fi, prepared),
    ) {
        Ok(_) => {}
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
            let read = buffer.len();
            state
                .vfs
                .record_fuse_read_callback(read, started.elapsed(), false);
            state.reply_with_pooled_read_buffer(buffer, |buffer| {
                // SAFETY: `buffer` remains owned by the pool helper until this
                // synchronous reply returns.
                unsafe { raw::fuse_reply_buf(req, buffer.as_ptr().cast::<c_char>(), buffer.len()) }
            });
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
    match state.queue_async_write(
        FuseWriteCallbackContext { req, started },
        ino,
        buf,
        size,
        off,
        fi,
    ) {
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
    let fh = file_handle(fi);
    let result = fh.ok_or(libc::EBADF).and_then(|fh| state.release(fh));
    finish_regular_release_with(state, fh, result, |errno| reply_error(req, errno));
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
            Ok(handle) => {
                reply_dir_open(req, fi, handle);
            }
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
    let handle = dir_handle(fi).map(|handle| {
        // SAFETY: `opendir` created this pointer with `Box::into_raw`, and
        // libfuse calls `releasedir` once for each successful directory open.
        unsafe { Box::from_raw(handle.as_ptr()) }
    });
    finish_dir_release_with(handle, || reply_error(req, 0));
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
        if linux_fuse_setattr_args_are_attr_only(&args) {
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
    ) -> Result<LinuxFuseAllocatedCreate, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        require_file_info(fi)?;
        self.create_with_flags(parent, name, mode, file_info_flags(fi))
    }

    fn create_with_flags(
        &self,
        parent: u64,
        name: &[u8],
        mode: mode_t,
        flags: c_int,
    ) -> Result<LinuxFuseAllocatedCreate, i32> {
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        let attr = self
            .vfs
            .create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        // Reserve the kernel lookup before opening the writer handle. Any
        // invalidation from writer admission onward is now marked for a
        // post-reply DATA notification.
        let lookup = self.inode_liveness.reserve_lookup(attr.ino);
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        let fh = self
            .vfs
            .open_created_file_with_known_attr_and_read_ahead_owner(
                &attr,
                open_flags,
                trunc,
                ReadAheadOwner::Kernel,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.install_open_liveness(attr.ino, fh);
        Ok(LinuxFuseAllocatedCreate {
            open: LinuxFuseAllocatedOpen {
                ino: attr.ino,
                fh,
                flags,
            },
            lookup,
        })
    }

    fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<InodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let mode = (mode & 0o7777) as u16;
        let result = self.vfs.mkdir_with_deferred_metadata_admission(
            parent,
            name,
            self.mount_uid,
            self.mount_gid,
            mode,
            &self.runtime_handle,
        );
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
        let result = self.vfs.create_with_deferred_metadata_admission(
            parent,
            name,
            self.mount_uid,
            self.mount_gid,
            mode,
            &self.runtime_handle,
        );
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

    fn link(&self, ino: u64, newparent: u64, newname: &[u8]) -> Result<(InodeAttr, bool), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let watermark = self.async_writes.capture_inode_watermark(ino);
        self.async_writes.wait_inode_completion(watermark);
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        let attr = self
            .vfs
            .link(ino, newparent, newname)
            .map_err(|error| vfs_err_to_errno(&error))?;
        let cache_safe = match self
            .invalidation_queue
            .push_with_receipt(ino, FuseInvalidationKind::Attributes)
        {
            Ok(receipt) => match receipt.wait_timeout(NAMESPACE_ATTR_INVALIDATION_TIMEOUT) {
                Some(
                    FuseInvalidationReceiptOutcome::Notified
                    | FuseInvalidationReceiptOutcome::SkippedUnobserved,
                ) => true,
                Some(FuseInvalidationReceiptOutcome::NotifyFailed) => {
                    warn!(
                        ino,
                        "hardlink attribute invalidation failed; replying with zero attr TTL"
                    );
                    false
                }
                None => {
                    warn!(
                        ino,
                        "timed out waiting for hardlink attribute invalidation; replying with zero attr TTL"
                    );
                    false
                }
            },
            Err(_) => {
                warn!(
                    ino,
                    "hardlink invalidation queue is closed; replying with zero attr TTL"
                );
                false
            }
        };
        Ok((attr, cache_safe))
    }

    fn open(&self, ino: u64, fi: *const raw::FuseFileInfo) -> Result<LinuxFuseAllocatedOpen, i32> {
        self.open_with_flags(ino, file_info_flags(fi))
    }

    fn open_with_flags(&self, ino: u64, flags: c_int) -> Result<LinuxFuseAllocatedOpen, i32> {
        let attr = self.getattr_attr(ino)?;
        match attr.kind {
            FileType::RegularFile => {}
            FileType::Directory => return Err(libc::EISDIR),
            FileType::Symlink => return Err(libc::EINVAL),
        }
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && (trunc || open_flags.allows_write()) {
            return Err(libc::EROFS);
        }
        let fh = self
            .vfs
            .open_file_with_known_attr_and_read_ahead_owner(
                &attr,
                open_flags,
                trunc,
                ReadAheadOwner::Kernel,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        // Establish kernel liveness before generation preparation or any
        // operation that can publish an invalidation for this handle.
        self.install_open_liveness(ino, fh);
        Ok(LinuxFuseAllocatedOpen { ino, fh, flags })
    }

    fn prepare_file_open(&self, attr: &InodeAttr, flags: c_int, fh: u64) -> LinuxFusePreparedOpen {
        let ino = attr.ino;
        let participation = if self.cache_policy == LinuxFuseCachePolicy::Direct
            || self.vfs.is_runtime_stats_file_ino(ino)
        {
            OsCacheParticipation::Disabled
        } else {
            OsCacheParticipation::Enabled
        };
        let generation = self
            .vfs
            .prepare_os_cache_open_with_attr(fh, participation, attr);
        let available_memory = if self.cache_policy == LinuxFuseCachePolicy::Adaptive {
            self.available_memory
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .available_memory()
        } else {
            None
        };
        let cache = if self.vfs.is_runtime_stats_file_ino(ino) {
            LinuxFuseOpenCachePolicy {
                direct_io: true,
                keep_cache: false,
                parallel_direct_writes: false,
                read_ahead_owner: ReadAheadOwner::Vfs,
                memory_bypassed: false,
            }
        } else {
            linux_fuse_open_cache_policy_for_decision(
                flags,
                self.cache_policy,
                generation,
                attr.size,
                available_memory,
                linux_fuse_env_flag("STUDIOFS_FUSE_ASYNC_DIO"),
            )
        };
        let owner_was_set = self.vfs.set_read_ahead_owner(fh, cache.read_ahead_owner);
        debug_assert!(owner_was_set, "newly-opened VFS handle must remain live");
        LinuxFusePreparedOpen {
            fh,
            cache,
            disposition: generation.disposition(),
            reason: generation.reason(),
        }
    }

    fn prepare_allocated_open(
        &self,
        allocated: LinuxFuseAllocatedOpen,
    ) -> Result<LinuxFusePreparedOpen, i32> {
        // Local metadata is cached; this refresh closes the result→reservation
        // race without introducing remote I/O.
        let attr = self.getattr_attr(allocated.ino)?;
        Ok(self.prepare_file_open(&attr, allocated.flags, allocated.fh))
    }

    fn run_open_reply_transaction(
        &self,
        ino: u64,
        allocate: impl FnOnce() -> Result<LinuxFuseAllocatedOpen, i32>,
        reply: impl FnOnce(LinuxFusePreparedOpen) -> c_int,
    ) -> Result<c_int, i32> {
        let _reply_guard = self.open_reply_serializer.lock(ino);
        let allocated = allocate()?;
        debug_assert_eq!(allocated.ino, ino);
        let fh = allocated.fh;
        let prepared = match self.prepare_allocated_open(allocated) {
            Ok(prepared) => prepared,
            Err(errno) => {
                self.reject_prepared_open(ino, fh);
                return Err(errno);
            }
        };
        let reply_result = reply(prepared);
        if reply_result != 0 {
            self.reject_prepared_open(ino, prepared.fh);
        } else {
            self.record_accepted_open_cache(prepared);
        }
        Ok(reply_result)
    }

    fn run_create_reply_transaction(
        &self,
        allocated: LinuxFuseAllocatedCreate,
        reply: impl FnOnce(&InodeAttr, LinuxFusePreparedOpen, FuseLookupReservation) -> c_int,
    ) -> Result<c_int, i32> {
        let ino = allocated.open.ino;
        let _reply_guard = self.open_reply_serializer.lock(ino);
        let attr = match self.getattr_attr(ino) {
            Ok(attr) => attr,
            Err(errno) => {
                self.reject_prepared_open(ino, allocated.open.fh);
                return Err(errno);
            }
        };
        let prepared = self.prepare_file_open(&attr, allocated.open.flags, allocated.open.fh);
        let reply_result = reply(&attr, prepared, allocated.lookup);
        if reply_result != 0 {
            self.reject_prepared_open(ino, prepared.fh);
        } else {
            self.record_accepted_open_cache(prepared);
        }
        Ok(reply_result)
    }

    fn record_accepted_open_cache(&self, prepared: LinuxFusePreparedOpen) {
        self.vfs.record_linux_fuse_open_cache_decision(
            prepared.cache.direct_io,
            prepared.disposition,
            prepared.reason,
            prepared.cache.memory_bypassed,
        );
    }

    fn reject_prepared_open(&self, ino: u64, fh: u64) {
        debug!(ino, fh, "Linux FUSE rolls back rejected open reply");
        self.remove_open_liveness(fh);
        self.vfs.abort_os_cache_open(fh);
        let _ = self
            .vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
    }

    fn install_open_liveness(&self, ino: u64, fh: u64) {
        let guard = self.inode_liveness.open_handle(ino);
        let replaced = self
            .open_liveness
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(fh, guard);
        debug_assert!(replaced.is_none(), "VFS file handles must be unique");
    }

    fn remove_open_liveness(&self, fh: u64) {
        self.open_liveness
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&fh);
    }

    fn enqueue_follow_up_invalidation(&self, ino: u64) {
        if self
            .invalidation_queue
            .push(ino, FuseInvalidationKind::Data)
            .is_err()
        {
            debug!(
                ino,
                "Linux FUSE follow-up invalidation skipped after shutdown"
            );
        }
    }

    fn reply_with_pooled_read_buffer(
        &self,
        buffer: Vec<u8>,
        reply: impl FnOnce(&[u8]) -> c_int,
    ) -> c_int {
        let result = reply(&buffer);
        self.read_buffers.put(buffer);
        result
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
        let mut buffer = self.read_buffers.take(size);
        let read =
            match self
                .runtime_handle
                .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut buffer))
            {
                Ok(read) => read,
                Err(error) => {
                    self.read_buffers.put(buffer);
                    return Err(vfs_err_to_errno(&error));
                }
            };
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
        callback: FuseWriteCallbackContext,
        ino: u64,
        buf: *const c_char,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<bool, i32> {
        if !linux_fuse_async_write_enabled() {
            return Ok(false);
        }
        let request = self.prepare_write_metadata(buf, size, offset, fi)?;
        if !request.local_sync_policy.is_opportunistic() {
            return Ok(false);
        }

        // Register before capacity admission so a concurrently captured
        // inode/handle watermark cannot miss this callback.
        let mut ticket = self
            .async_writes
            .start_request(ino, request.fh)
            .map_err(|error| error.errno())?;
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        if let Err(error) = ticket.reserve_capacity(size) {
            let errno = error.errno();
            ticket.complete(Err(errno));
            record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
            return Err(errno);
        }
        // The request buffer remains owned by libfuse while this callback is
        // blocked on admission. Copy only after bounded capacity is reserved.
        let data = with_write_data_slice(buf, size, Bytes::copy_from_slice)?;
        let fh = request.fh;
        let write_offset = request.offset;
        let vfs = self.vfs.clone();
        let metrics_vfs = Arc::clone(&self.vfs);
        let metrics_ledger = Arc::clone(&self.async_writes);
        let cache_policy = self.cache_policy;
        let sequence = ticket.sequence();
        let task_guard = FuseWriteTaskGuard::with_metrics(
            FuseReqPtr(callback.req),
            ticket,
            Arc::clone(&self.vfs),
            size,
            callback.started,
        );
        let write_ledger = Arc::clone(&self.async_writes);
        let task = self.runtime_handle.spawn(async move {
            if !write_ledger.mark_task_non_cancellable(sequence) {
                record_async_write_metrics(&metrics_vfs, &metrics_ledger, cache_policy);
                return;
            }
            let result = vfs
                .write_fh_owned_with_deferred_materialize(
                    fh,
                    ino,
                    write_offset,
                    data,
                    LocalSyncPolicy::Opportunistic,
                )
                .await
                .map_err(|error| vfs_err_to_errno(&error));
            let reply_result = task_guard.complete(result);
            if reply_result != 0 {
                debug!(
                    ino,
                    fh,
                    result = reply_result,
                    "Linux FUSE async write reply was rejected"
                );
            }
            record_async_write_metrics(&metrics_vfs, &metrics_ledger, cache_policy);
        });
        self.async_writes.track_task(sequence, task.abort_handle());
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        Ok(true)
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
        let ino = self
            .vfs
            .get_open_file(fh)
            .map(|open_file| open_file.ino)
            .ok_or(libc::EBADF)?;
        let watermark = self.async_writes.capture_inode_watermark(ino);
        let outcome = self.async_writes.wait_inode(watermark);
        let result = finish_async_barrier_with(outcome, || {
            if self.local_sync_policy_for_handle(fh).is_opportunistic() {
                return self
                    .runtime_handle
                    .block_on(self.vfs.flush_volatile_writeback_with_outcome(ino, fh))
                    .map(|_| ())
                    .map_err(|error| vfs_err_to_errno(&error));
            }
            self.runtime_handle
                .block_on(self.vfs.flush_fh(fh))
                .map_err(|error| vfs_err_to_errno(&error))
        });
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        result
    }

    fn fsync(&self, file_handle: Option<u64>, ino: u64, datasync: bool) -> Result<(), i32> {
        let watermark = self.async_writes.capture_inode_watermark(ino);
        let outcome = self.async_writes.wait_inode(watermark);
        let result = finish_async_barrier_with(outcome, || {
            let result = if let Some(fh) = file_handle {
                // StudioVfs does not currently distinguish fdatasync from fsync on
                // this path; both drain the handle and execute the strict metadata barrier.
                self.runtime_handle.block_on(self.vfs.fsync_fh(fh))
            } else {
                self.runtime_handle.block_on(self.vfs.fsync(ino, datasync))
            };
            result.map_err(|error| vfs_err_to_errno(&error))
        });
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        result
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        let local_sync_policy = self.local_sync_policy_for_handle(fh);
        let watermark = self.async_writes.close_handle(fh);
        let _forget = ForgetClosedHandle {
            ledger: &self.async_writes,
            fh,
        };
        let outcome = self.async_writes.wait_handle(watermark);
        let result = finish_async_barrier_with(outcome, || {
            if local_sync_policy.is_opportunistic() {
                self.vfs
                    .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
                return Ok(());
            }
            self.runtime_handle
                .block_on(self.vfs.release_fh(fh))
                .map_err(|error| vfs_err_to_errno(&error))
        });
        record_async_write_metrics(&self.vfs, &self.async_writes, self.cache_policy);
        result
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
        let liveness = self.inode_liveness.open_handle(ino);
        let raw = Box::into_raw(Box::new(DirectoryHandle {
            entries,
            _liveness: liveness,
        }));
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
    _liveness: FuseOpenHandleGuard,
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

#[cfg(test)]
fn linux_fuse_open_cache_policy_for(
    flags: c_int,
    cache_policy: LinuxFuseCachePolicy,
) -> LinuxFuseOpenCachePolicy {
    let (open_flags, trunc) = posix_flags_to_open_flags(flags);
    let write_capable = trunc || open_flags.allows_write();
    let generation = if write_capable {
        OpenCacheDecision {
            disposition: studiofs_vfs::vfs::OsCacheDisposition::Replace,
            reason: studiofs_vfs::vfs::OsCacheReason::WriteCapable,
        }
    } else {
        OpenCacheDecision {
            disposition: studiofs_vfs::vfs::OsCacheDisposition::Reuse,
            reason: studiofs_vfs::vfs::OsCacheReason::StableGeneration,
        }
    };
    linux_fuse_open_cache_policy_for_decision(flags, cache_policy, generation, 0, None, false)
}

fn linux_fuse_open_cache_policy_for_decision(
    flags: c_int,
    cache_policy: LinuxFuseCachePolicy,
    generation: impl Into<OpenCacheDecision>,
    file_size: u64,
    available_memory: Option<u64>,
    async_direct_io: bool,
) -> LinuxFuseOpenCachePolicy {
    let (open_flags, trunc) = posix_flags_to_open_flags(flags);
    let write_capable = trunc || open_flags.allows_write();
    let decision = decide_open_cache(
        cache_policy,
        write_capable,
        generation,
        file_size,
        available_memory,
    );
    LinuxFuseOpenCachePolicy {
        direct_io: decision.direct_io,
        keep_cache: decision.keep_cache,
        parallel_direct_writes: decision.direct_io && async_direct_io,
        read_ahead_owner: match decision.readahead {
            ReadAheadIntent::Vfs => ReadAheadOwner::Vfs,
            ReadAheadIntent::Kernel => ReadAheadOwner::Kernel,
        },
        memory_bypassed: decision.memory_bypassed,
    }
}

fn fuse_flag(value: bool) -> c_int {
    if value { 1 } else { 0 }
}

fn reply_entry_result(
    req: *mut raw::FuseReq,
    state: &SessionState,
    result: Result<InodeAttr, i32>,
) -> c_int {
    match result {
        Ok(attr) => match reply_entry_with(state, attr.ino, |entry| {
            // SAFETY: `entry` is initialized and valid for this synchronous reply.
            unsafe { raw::fuse_reply_entry(req, entry) }
        }) {
            Ok(result) => result,
            Err(errno) => {
                reply_error(req, errno);
                0
            }
        },
        Err(errno) => {
            reply_error(req, errno);
            0
        }
    }
}

fn reply_entry_with(
    state: &SessionState,
    ino: u64,
    reply: impl FnOnce(*const raw::FuseEntryParam) -> c_int,
) -> Result<c_int, i32> {
    let reservation = state.inode_liveness.reserve_lookup(ino);
    // Metadata reads are local/cached. Refreshing after reservation guarantees
    // an invalidation either precedes this snapshot or marks the pending reply.
    let attr = state.getattr_attr(ino)?;
    Ok(reply_reserved_entry_with(state, &attr, reservation, reply))
}

fn reply_reserved_entry_with(
    state: &SessionState,
    attr: &InodeAttr,
    reservation: FuseLookupReservation,
    reply: impl FnOnce(*const raw::FuseEntryParam) -> c_int,
) -> c_int {
    reply_reserved_entry_with_timeout(state, attr, reservation, TTL_SECONDS, reply)
}

fn reply_reserved_entry_with_timeout(
    state: &SessionState,
    attr: &InodeAttr,
    reservation: FuseLookupReservation,
    attr_timeout: f64,
    reply: impl FnOnce(*const raw::FuseEntryParam) -> c_int,
) -> c_int {
    let stat = state.attr_to_stat(attr);
    let entry = raw::FuseEntryParam {
        ino: attr.ino,
        generation: 1,
        attr: stat,
        attr_timeout,
        entry_timeout: TTL_SECONDS,
    };
    let began = reservation.begin_reply();
    debug_assert!(began, "fresh lookup reservation must begin exactly once");
    let result = reply(&entry);
    if result == 0 {
        if reservation
            .commit()
            .is_some_and(|outcome| outcome.follow_up_invalidation_required())
        {
            state.enqueue_follow_up_invalidation(attr.ino);
        }
    } else {
        reservation.rollback();
    }
    result
}

fn reply_dir_open(
    req: *mut raw::FuseReq,
    fi: *mut raw::FuseFileInfo,
    handle: NonNull<DirectoryHandle>,
) -> c_int {
    if fi.is_null() {
        // SAFETY: ownership has not been handed to libfuse because `fi` is invalid.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
        reply_error(req, libc::EINVAL);
        return -libc::EINVAL;
    }
    // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
    // using the exact system header layout.
    let result = unsafe {
        studiofs_linux_fuse_dir_file_info_init(fi, handle.as_ptr() as u64);
        raw::fuse_reply_open(req, fi)
    };
    finish_dir_open_reply(handle, result)
}

fn finish_dir_open_reply(handle: NonNull<DirectoryHandle>, result: c_int) -> c_int {
    if result != 0 {
        // The kernel rejected the open, so ownership never left this callback.
        unsafe { drop(Box::from_raw(handle.as_ptr())) };
    }
    result
}

fn finish_dir_release_with(handle: Option<Box<DirectoryHandle>>, reply: impl FnOnce()) {
    reply();
    // Keep the inode live until the kernel has consumed the release reply.
    drop(handle);
}

fn finish_regular_release_with(
    state: &SessionState,
    fh: Option<u64>,
    result: Result<(), c_int>,
    reply: impl FnOnce(c_int),
) {
    reply(result.err().unwrap_or(0));
    if let Some(fh) = fh {
        state.remove_open_liveness(fh);
    }
}

fn reply_file_open(
    req: *mut raw::FuseReq,
    fi: *mut raw::FuseFileInfo,
    prepared: LinuxFusePreparedOpen,
) -> c_int {
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return -libc::EINVAL;
    }
    // SAFETY: libfuse owns `fi` for this callback; the C shim writes fields
    // using the exact system header layout.
    unsafe {
        studiofs_linux_fuse_file_info_init(
            fi,
            prepared.fh,
            fuse_flag(prepared.cache.direct_io),
            fuse_flag(prepared.cache.keep_cache),
            fuse_flag(prepared.cache.parallel_direct_writes),
        );
        raw::fuse_reply_open(req, fi)
    }
}

fn reply_create(
    req: *mut raw::FuseReq,
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    attr: &InodeAttr,
    prepared: LinuxFusePreparedOpen,
    lookup: FuseLookupReservation,
) -> c_int {
    if fi.is_null() {
        reply_error(req, libc::EINVAL);
        return -libc::EINVAL;
    }
    reply_create_with(state, fi, attr, prepared, lookup, |entry, fi| {
        // SAFETY: `entry` and `fi` are initialized and valid for this synchronous reply.
        unsafe { raw::fuse_reply_create(req, entry, fi) }
    })
}

fn reply_create_with(
    state: &SessionState,
    fi: *mut raw::FuseFileInfo,
    attr: &InodeAttr,
    prepared: LinuxFusePreparedOpen,
    lookup: FuseLookupReservation,
    reply: impl FnOnce(*const raw::FuseEntryParam, *const raw::FuseFileInfo) -> c_int,
) -> c_int {
    // SAFETY: libfuse owns `fi` for this callback; the C shim uses the system layout.
    unsafe {
        studiofs_linux_fuse_file_info_init(
            fi,
            prepared.fh,
            fuse_flag(prepared.cache.direct_io),
            fuse_flag(prepared.cache.keep_cache),
            fuse_flag(prepared.cache.parallel_direct_writes),
        );
    }
    reply_reserved_entry_with(state, attr, lookup, |entry| reply(entry, fi))
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
        let writes_drained = self.state.as_ref().is_none_or(|state| {
            let drained = shutdown_async_writes(&state.async_writes);
            if !drained {
                state.async_writes.record_shutdown_timeout();
            }
            record_async_write_metrics(&state.vfs, &state.async_writes, state.cache_policy);
            drained
        });
        let worker_outcome = shutdown_invalidation_with(
            || {
                if let Some(state) = &self.state {
                    state.vfs.clear_kernel_invalidator();
                }
            },
            || {
                self.invalidation_worker
                    .as_ref()
                    .map(|worker| worker.shutdown(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT))
                    .unwrap_or(FuseInvalidationShutdownOutcome::Drained)
            },
            || self.invalidation_state.begin_shutdown(),
        );
        let invalidation_metrics = self
            .invalidation_worker
            .as_ref()
            .map(FuseInvalidationWorker::metrics);
        if let (Some(state), Some(metrics)) = (&self.state, invalidation_metrics) {
            record_invalidation_metrics(&state.vfs, &self.invalidation_metrics_generation, metrics);
        }
        if !writes_drained {
            warn!(
                "Linux FUSE session shutdown timed out cancelling async writes; \
                 retaining raw session and callback state to avoid dangling request pointers"
            );
        }
        let worker_timed_out = worker_outcome == FuseInvalidationShutdownOutcome::TimedOut;
        if worker_timed_out {
            warn!(
                ?invalidation_metrics,
                "Linux FUSE session shutdown timed out draining kernel invalidations; \
                 unmounting and retaining worker, raw session and callback state \
                 to avoid use-after-free"
            );
        }
        if worker_outcome == FuseInvalidationShutdownOutcome::WorkerPanicked {
            warn!(
                ?invalidation_metrics,
                "Linux FUSE invalidation worker panicked after releasing its session pointer; \
                 continuing safe session teardown"
            );
        }
        if !worker_timed_out {
            drop(self.invalidation_worker.take());
        }
        let invalidations_idle = !worker_timed_out
            && self
                .invalidation_state
                .wait_for_idle(INVALIDATION_SHUTDOWN_IDLE_TIMEOUT);
        if !invalidations_idle && !worker_timed_out {
            warn!(
                "Linux FUSE session shutdown timed out waiting for active kernel invalidation; \
                 unmounting and retaining raw session and callback state \
                 to avoid use-after-free"
            );
        }
        self.detach_raw_session();
        if !writes_drained || !invalidations_idle {
            if let Some(worker) = self.invalidation_worker.take() {
                std::mem::forget(worker);
            }
            if let Some(state) = self.state.take() {
                std::mem::forget(state);
            }
            return;
        }
        // SAFETY: `self.raw` is owned by this wrapper and destroyed once.
        unsafe { raw::fuse_session_destroy(self.raw.as_ptr()) };
    }
}

impl Session {
    fn detach_raw_session(&mut self) {
        let signal_handlers_installed =
            std::mem::replace(&mut self.signal_handlers_installed, false);
        let raw = self.raw;
        detach_raw_session_with(
            signal_handlers_installed,
            || {
                // SAFETY: handlers were installed for this retained live session.
                unsafe { raw::fuse_remove_signal_handlers(raw.as_ptr()) };
            },
            || {
                // SAFETY: libfuse tolerates unmount after regular kernel disconnects.
                unsafe { raw::fuse_session_unmount(raw.as_ptr()) };
            },
        );
    }
}

fn shutdown_async_writes(ledger: &AsyncWriteLedger) -> bool {
    shutdown_async_writes_with_timeout(ledger, ASYNC_WRITE_SHUTDOWN_TIMEOUT)
}

fn shutdown_async_writes_with_timeout(ledger: &AsyncWriteLedger, timeout: Duration) -> bool {
    ledger.shutdown();
    if matches!(ledger.wait_all(timeout), BarrierOutcome::Complete(_)) {
        return true;
    }

    // Only tasks that have not entered the VFS mutation future remain
    // cancellable. Their guard converts cancellation into exactly one EIO
    // reply and a sticky ledger error; post-transition writes keep draining.
    ledger.abort_pending_tasks();
    matches!(ledger.wait_all(timeout), BarrierOutcome::Complete(_))
}

fn record_invalidation_metrics(
    vfs: &StudioVfs,
    generation: &AtomicU64,
    metrics: FuseInvalidationMetricsSnapshot,
) {
    let snapshot_generation = generation.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    vfs.record_fuse_invalidation_snapshot(VfsFuseInvalidationSnapshot {
        snapshot_generation,
        queue_depth: metrics.queue_depth,
        queue_peak: metrics.queue_peak,
        coalesced: metrics.coalesced,
        skipped_unobserved: metrics.skipped_unobserved,
        notify_failures: metrics.notify_failures,
        notify_latency_count: metrics.notify_latency_count,
        notify_latency_total_ns: metrics.notify_latency_total_nanos,
        notify_latency_max_ns: metrics.notify_latency_max_nanos,
        shutdown_timeouts: metrics.shutdown_timeouts,
        worker_panics: metrics.worker_panics,
        notification_in_flight: metrics.notification_in_flight,
    });
}

fn record_async_write_metrics(
    vfs: &StudioVfs,
    ledger: &AsyncWriteLedger,
    cache_policy: LinuxFuseCachePolicy,
) {
    let snapshot =
        async_write_metrics_snapshot(ledger, cache_policy, linux_fuse_async_write_enabled());
    vfs.record_fuse_async_write_snapshot(snapshot);
}

fn async_write_metrics_snapshot(
    ledger: &AsyncWriteLedger,
    cache_policy: LinuxFuseCachePolicy,
    enabled: bool,
) -> VfsFuseAsyncWriteSnapshot {
    let AsyncWriteMetricsSnapshot {
        snapshot_generation,
        pending_writes,
        pending_bytes,
        pending_handles,
        pending_writes_per_inode,
        peak_pending_bytes,
        completed_writes,
        failed_writes,
        task_failed_writes,
        canceled_writes,
        shutdown_timeouts,
        admission_waits,
        admission_wait_total_nanos,
        barrier_waits,
        inode_barrier_waits,
        handle_barrier_waits,
        barrier_wait_total_nanos,
    } = ledger.metrics_snapshot();
    let profile = match cache_policy {
        LinuxFuseCachePolicy::Direct => "direct",
        LinuxFuseCachePolicy::Adaptive => "adaptive",
        LinuxFuseCachePolicy::Kernel => "kernel",
    };
    VfsFuseAsyncWriteSnapshot {
        snapshot_generation,
        enabled,
        linux_fuse_profile: profile.into(),
        kernel_writeback_cache: Some(cache_policy == LinuxFuseCachePolicy::Kernel),
        max_pending_bytes: enabled.then_some(DEFAULT_MAX_PENDING_BYTES),
        max_pending_writes: enabled.then_some(DEFAULT_MAX_PENDING_WRITES),
        pending_writes,
        pending_bytes,
        pending_handles,
        pending_writes_per_inode,
        peak_pending_bytes,
        completed_writes,
        failed_writes,
        task_failed_writes,
        canceled_writes,
        shutdown_timeouts,
        admission_waits,
        admission_wait_total_ms: admission_wait_total_nanos as f64 / 1_000_000.0,
        barrier_waits,
        inode_barrier_waits,
        handle_barrier_waits,
        barrier_wait_total_ms: barrier_wait_total_nanos as f64 / 1_000_000.0,
        ..VfsFuseAsyncWriteSnapshot::default()
    }
}

fn shutdown_invalidation_with(
    clear_vfs_invalidator: impl FnOnce(),
    drain_worker: impl FnOnce() -> FuseInvalidationShutdownOutcome,
    close_session_gate: impl FnOnce(),
) -> FuseInvalidationShutdownOutcome {
    clear_vfs_invalidator();
    let outcome = drain_worker();
    close_session_gate();
    outcome
}

fn detach_raw_session_with(
    signal_handlers_installed: bool,
    remove_signal_handlers: impl FnOnce(),
    unmount: impl FnOnce(),
) {
    if signal_handlers_installed {
        remove_signal_handlers();
    }
    unmount();
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};
    use studiofs_meta::MetaInodes;

    struct UnmountGuard(Option<std::path::PathBuf>);

    struct FakeWriteReply {
        replies: Arc<Mutex<Vec<Result<u32, i32>>>>,
        result: libc::c_int,
    }

    impl super::FuseWriteReply for FakeWriteReply {
        fn reply_write(&mut self, written: libc::size_t) -> libc::c_int {
            self.replies
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(Ok(written as u32));
            self.result
        }

        fn reply_error(&mut self, errno: libc::c_int) -> libc::c_int {
            self.replies
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(Err(errno));
            self.result
        }
    }

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
    fn directory_replies_do_not_enable_kernel_readdir_cache() {
        // Remote namespace changes are delivered through inode invalidation;
        // keeping a kernel-side readdir snapshot would retain stale dirents.
        let cache_readdir =
            unsafe { super::studiofs_linux_fuse_dir_file_info_cache_readdir_after_init() };
        assert_eq!(
            cache_readdir, 0,
            "Linux directory replies must force fresh readdir callbacks after peer namespace changes"
        );
    }

    #[test]
    fn argv_uses_system_libfuse_mount_options() {
        let args = super::argv_strings("demo");

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
        assert_eq!(
            args.iter()
                .filter(|arg| arg.as_str() == "max_read=4194304")
                .count(),
            1
        );
        assert!(!args.iter().any(|arg| arg.contains("max_readahead")));
        assert!(!args.iter().any(|arg| arg == "writeback_cache"));
        assert!(!args.iter().any(|arg| arg.contains("max_background")));
        assert!(!args.iter().any(|arg| arg.contains("congestion_threshold")));
    }

    #[test]
    fn generated_argv_is_accepted_by_low_level_session_parser() {
        let mut args =
            super::OwnedFuseArgs::new(super::argv_strings("demo")).expect("build FUSE arguments");
        // SAFETY: the C shim returns a static empty ops table and this test
        // destroys the resulting session before the stack-owned arguments drop.
        let session = unsafe {
            super::studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                super::studiofs_linux_fuse_empty_low_level_ops(),
                super::studiofs_linux_fuse_low_level_ops_size(),
                std::ptr::null_mut(),
            )
        };

        assert!(
            !session.is_null(),
            "generated mount options must be accepted by the low-level libfuse parser"
        );
        // SAFETY: the non-null session is uniquely owned by this test.
        unsafe { super::raw::fuse_session_destroy(session) };
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
    fn connection_read_geometry_preserves_smaller_offer_and_caps_larger_offer() {
        const KIB_128: u32 = 128 * 1024;
        const MIB_8: u32 = 8 * 1024 * 1024;
        const LIMIT: u32 = 4 * 1024 * 1024;

        // SAFETY: both C helpers initialize a stack-local fuse_conn_info.
        unsafe {
            assert_eq!(
                super::studiofs_linux_fuse_init_effective_max_read(KIB_128),
                KIB_128
            );
            assert_eq!(
                super::studiofs_linux_fuse_init_effective_max_read(MIB_8),
                LIMIT
            );
            assert_eq!(
                super::studiofs_linux_fuse_init_effective_max_readahead(KIB_128),
                KIB_128
            );
            assert_eq!(
                super::studiofs_linux_fuse_init_effective_max_readahead(MIB_8),
                LIMIT
            );
        }
    }

    #[test]
    fn connection_read_geometry_handles_zero_offer_without_userdata() {
        const LIMIT: u32 = 4 * 1024 * 1024;

        // SAFETY: the helpers intentionally exercise init with null userdata.
        unsafe {
            assert_eq!(super::studiofs_linux_fuse_init_effective_max_read(0), LIMIT);
            assert_eq!(
                super::studiofs_linux_fuse_init_effective_max_readahead(0),
                0
            );
        }
    }

    #[test]
    fn negotiated_connection_geometry_is_recorded_in_vfs_stats() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-geometry-stats")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;

        // SAFETY: userdata points to `state` for the duration of the call.
        unsafe {
            super::studiofs_linux_fuse_record_connection(
                (&mut state as *mut super::SessionState).cast(),
                4 * 1024 * 1024,
                4 * 1024 * 1024,
                128 * 1024,
                256 * 1024,
                1024 * 1024,
                128,
                96,
                0x55,
                0x11,
            );
        }

        let snapshot = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(snapshot.fuse_connection.cache_policy, "adaptive");
        assert_eq!(
            snapshot.fuse_connection.effective_max_read_bytes,
            128 * 1024
        );
        assert_eq!(
            snapshot.fuse_connection.effective_max_readahead_bytes,
            256 * 1024
        );
        assert_eq!(snapshot.fuse_connection.effective_max_background, Some(128));
        assert_eq!(
            snapshot.fuse_connection.effective_congestion_threshold,
            Some(96)
        );
        assert_eq!(snapshot.fuse_connection.capable_features, 0x55);
        assert_eq!(snapshot.fuse_connection.enabled_features, 0x11);
    }

    #[test]
    fn async_write_ledger_metrics_are_published_to_vfs_stats() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-stats")
                .expect("create VFS fixture");
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");

        super::record_async_write_metrics(
            &fixture.vfs(),
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        let pending = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(pending.fuse_async_write.pending_writes, 1);
        assert_eq!(pending.fuse_async_write.pending_handles, 1);
        assert_eq!(
            pending.fuse_async_write.pending_writes_per_inode.get(&10),
            Some(&1)
        );

        ticket.complete(Err(libc::ENOSPC));
        super::record_async_write_metrics(
            &fixture.vfs(),
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        let completed = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(completed.fuse_async_write.pending_writes, 0);
        assert_eq!(completed.fuse_async_write.completed_writes, 1);
        assert_eq!(completed.fuse_async_write.failed_writes, 1);
    }

    #[test]
    fn disabled_async_write_metrics_report_the_effective_setting() {
        let ledger = crate::async_writes::AsyncWriteLedger::with_limits(16, 4);

        let snapshot = super::async_write_metrics_snapshot(
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
            false,
        );

        assert!(!snapshot.enabled);
        assert_eq!(snapshot.max_pending_bytes, None);
        assert_eq!(snapshot.max_pending_writes, None);
    }

    #[test]
    fn dropped_async_write_task_replies_once_and_records_sticky_eio() {
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        let replies = Arc::new(Mutex::new(Vec::new()));
        let guard = super::FuseWriteTaskGuard::new(
            FakeWriteReply {
                replies: Arc::clone(&replies),
                result: 0,
            },
            ticket,
        );

        drop(guard);

        assert_eq!(
            *replies.lock().unwrap_or_else(|error| error.into_inner()),
            [Err(libc::EIO)]
        );
        assert_eq!(
            ledger.wait_inode(watermark),
            crate::async_writes::BarrierOutcome::Complete(Some(libc::EIO))
        );
    }

    #[test]
    fn completed_async_write_captures_native_reply_result_without_sticky_error() {
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");
        let watermark = ledger.capture_inode_watermark(10);
        let replies = Arc::new(Mutex::new(Vec::new()));
        let guard = super::FuseWriteTaskGuard::new(
            FakeWriteReply {
                replies: Arc::clone(&replies),
                result: -libc::ENOENT,
            },
            ticket,
        );

        assert_eq!(guard.complete(Ok(8)), -libc::ENOENT);
        assert_eq!(
            *replies.lock().unwrap_or_else(|error| error.into_inner()),
            [Ok(8)]
        );
        assert_eq!(
            ledger.wait_inode(watermark),
            crate::async_writes::BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn async_write_callback_metrics_are_recorded_only_when_reply_completes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-async-write-callback-stats",
        )
        .expect("create VFS fixture");
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");
        let guard = super::FuseWriteTaskGuard::with_metrics(
            FakeWriteReply {
                replies: Arc::new(Mutex::new(Vec::new())),
                result: 0,
            },
            ticket,
            fixture.vfs(),
            8,
            Instant::now(),
        );

        assert_eq!(
            fixture
                .vfs()
                .write_path_observability_snapshot()
                .fuse_callbacks
                .write
                .calls,
            0,
            "enqueue must not count as callback completion"
        );
        guard.complete(Err(libc::ENOSPC));
        let stats = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(stats.fuse_callbacks.write.calls, 1);
        assert_eq!(stats.fuse_callbacks.write.bytes, 8);
        assert_eq!(stats.fuse_callbacks.write.errors, 1);
    }

    #[test]
    fn async_error_has_precedence_but_vfs_barrier_still_runs() {
        let vfs_calls = AtomicUsize::new(0);

        let result = super::finish_async_barrier_with(
            crate::async_writes::BarrierOutcome::Complete(Some(libc::ENOSPC)),
            || {
                vfs_calls.fetch_add(1, Ordering::Relaxed);
                Err::<(), _>(libc::EROFS)
            },
        );

        assert_eq!(result, Err(libc::ENOSPC));
        assert_eq!(vfs_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn async_write_shutdown_aborts_opportunistic_task_and_drains_reply_guard() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("create test runtime");
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");
        let sequence = ticket.sequence();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let guard = super::FuseWriteTaskGuard::new(
            FakeWriteReply {
                replies: Arc::clone(&replies),
                result: 0,
            },
            ticket,
        );
        let task = runtime.spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        assert!(ledger.track_task(sequence, task.abort_handle()));

        assert!(super::shutdown_async_writes_with_timeout(
            &ledger,
            Duration::from_millis(100)
        ));
        let error = runtime
            .block_on(task)
            .expect_err("shutdown should abort pending write task");
        assert!(error.is_cancelled());
        assert_eq!(
            *replies.lock().unwrap_or_else(|error| error.into_inner()),
            [Err(libc::EIO)]
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
    fn low_level_ops_include_forget_callback() {
        // SAFETY: the C shim only inspects the callback pointer in a static table.
        let has_callback = unsafe {
            super::studiofs_linux_fuse_low_level_ops_has_forget_callback(
                super::studiofs_linux_fuse_low_level_ops(),
            )
        };

        assert_eq!(has_callback, 1);
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
                read_ahead_owner: studiofs_vfs::ReadAheadOwner::Kernel,
                memory_bypassed: false,
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
                read_ahead_owner: studiofs_vfs::ReadAheadOwner::Kernel,
                memory_bypassed: false,
            }
        );
    }

    #[test]
    fn runtime_cache_policy_keeps_unset_default_direct_until_adaptive_is_ready() {
        assert_eq!(
            super::linux_fuse_effective_cache_policy_for_value(None, false),
            crate::cache_policy::LinuxFuseCachePolicy::Direct
        );
    }

    #[test]
    fn runtime_cache_policy_honors_explicit_adaptive_before_default_switch() {
        assert_eq!(
            super::linux_fuse_effective_cache_policy_for_value(Some("adaptive"), false),
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive
        );
    }

    #[test]
    fn runtime_cache_policy_uses_adaptive_after_readiness_gate() {
        assert_eq!(
            super::linux_fuse_effective_cache_policy_for_value(None, true),
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive
        );
    }

    #[test]
    fn adaptive_stable_read_maps_to_kernel_readahead_and_keep_cache() {
        let policy = super::linux_fuse_open_cache_policy_for_decision(
            libc::O_RDONLY,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
            crate::cache_policy::OpenCacheDecision {
                disposition: studiofs_vfs::vfs::OsCacheDisposition::Reuse,
                reason: studiofs_vfs::vfs::OsCacheReason::StableGeneration,
            },
            4096,
            Some(8192),
            false,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: true,
                parallel_direct_writes: false,
                read_ahead_owner: studiofs_vfs::ReadAheadOwner::Kernel,
                memory_bypassed: false,
            }
        );
    }

    #[test]
    fn adaptive_memory_bypass_maps_to_direct_vfs_readahead() {
        let policy = super::linux_fuse_open_cache_policy_for_decision(
            libc::O_RDONLY,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
            crate::cache_policy::OpenCacheDecision {
                disposition: studiofs_vfs::vfs::OsCacheDisposition::Replace,
                reason: studiofs_vfs::vfs::OsCacheReason::FirstObservation,
            },
            8193,
            Some(8192),
            true,
        );

        assert_eq!(
            policy,
            super::LinuxFuseOpenCachePolicy {
                direct_io: true,
                keep_cache: false,
                parallel_direct_writes: true,
                read_ahead_owner: studiofs_vfs::ReadAheadOwner::Vfs,
                memory_bypassed: true,
            }
        );
    }

    #[test]
    fn direct_linux_decision_restores_vfs_owner_after_prepared_open() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-direct-owner")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        let attr = fixture
            .vfs()
            .create(1, b"direct-owner.txt", 1000, 1000, 0o644)
            .expect("create file");
        let fh = fixture
            .vfs()
            .open_file_with_read_ahead_owner(
                attr.ino,
                studiofs_vfs::OpenFlags::RDONLY,
                false,
                studiofs_vfs::ReadAheadOwner::Kernel,
            )
            .expect("open with provisional kernel owner");

        let prepared = state.prepare_file_open(&attr, libc::O_RDONLY, fh);

        assert!(prepared.cache.direct_io);
        assert_eq!(
            fixture.vfs().read_ahead_owner(fh),
            Some(studiofs_vfs::ReadAheadOwner::Vfs)
        );
    }

    #[test]
    fn rejected_open_transaction_aborts_generation_and_retires_handle() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-rejected-open")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;
        let attr = fixture
            .vfs()
            .create(1, b"rejected-open.txt", 1000, 1000, 0o644)
            .expect("create file");
        let warm_fh = fixture
            .vfs()
            .open_file_with_read_ahead_owner(
                attr.ino,
                studiofs_vfs::OpenFlags::RDONLY,
                false,
                studiofs_vfs::ReadAheadOwner::Kernel,
            )
            .expect("open initial generation");
        let warm = state.prepare_file_open(&attr, libc::O_RDONLY, warm_fh);
        assert!(
            !warm.cache.keep_cache,
            "first generation must replace pages"
        );
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(warm_fh))
            .expect("release initial generation");

        let fh = fixture
            .vfs()
            .open_file_with_read_ahead_owner(
                attr.ino,
                studiofs_vfs::OpenFlags::RDONLY,
                false,
                studiofs_vfs::ReadAheadOwner::Kernel,
            )
            .expect("open with provisional owner");

        let reply_result = state
            .run_open_reply_transaction(
                attr.ino,
                || {
                    state.install_open_liveness(attr.ino, fh);
                    Ok(super::LinuxFuseAllocatedOpen {
                        ino: attr.ino,
                        fh,
                        flags: libc::O_RDONLY,
                    })
                },
                |prepared| {
                    assert!(
                        prepared.cache.keep_cache,
                        "rejected transaction must start from a reusable generation"
                    );
                    -libc::ENOENT
                },
            )
            .expect("prepare transaction");

        assert_eq!(reply_result, -libc::ENOENT);
        assert!(fixture.vfs().get_open_file(fh).is_none());
        assert_eq!(
            fixture
                .vfs()
                .write_path_observability_snapshot()
                .linux_fuse_open_cache
                .total,
            0,
            "a rejected native FUSE reply must not count as an open"
        );

        let next_fh = fixture
            .vfs()
            .open_file_with_read_ahead_owner(
                attr.ino,
                studiofs_vfs::OpenFlags::RDONLY,
                false,
                studiofs_vfs::ReadAheadOwner::Kernel,
            )
            .expect("reopen after rejected reply");
        let next = state.prepare_file_open(&attr, libc::O_RDONLY, next_fh);
        assert!(
            !next.cache.keep_cache,
            "rejected reply must not leave a reusable generation snapshot"
        );
    }

    #[test]
    fn accepted_open_transaction_records_cache_decision_once() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-accepted-open")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        let attr = fixture
            .vfs()
            .create(1, b"accepted-open.txt", 1000, 1000, 0o644)
            .expect("create file");

        let result = state
            .run_open_reply_transaction(
                attr.ino,
                || state.open_with_flags(attr.ino, libc::O_RDONLY),
                |_| 0,
            )
            .expect("accepted reply transaction");

        assert_eq!(result, 0);
        let cache = fixture
            .vfs()
            .write_path_observability_snapshot()
            .linux_fuse_open_cache;
        assert_eq!(cache.total, 1);
        assert_eq!(cache.direct_opens, 1);
        assert_eq!(cache.disabled, 1);
    }

    #[test]
    fn open_reply_cache_generation_uses_refreshed_attributes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-refreshed-open-generation",
        )
        .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;
        let attr = fixture
            .vfs()
            .create(1, b"refreshed-open-generation.txt", 1000, 1000, 0o644)
            .expect("create file");

        let warm_fh = fixture
            .vfs()
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDONLY, false)
            .expect("open initial generation");
        let warm = state.prepare_file_open(&attr, libc::O_RDONLY, warm_fh);
        assert!(!warm.cache.keep_cache, "first observation replaces pages");
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(warm_fh))
            .expect("release initial generation");

        let allocated = state
            .open_with_flags(attr.ino, libc::O_RDONLY)
            .expect("allocate open from old attributes");
        let mut refreshed = fixture.meta().get_attr(attr.ino).expect("refresh attr");
        refreshed.size = 4096;
        refreshed.committed_size = 4096;
        refreshed.content_epoch = refreshed.content_epoch.saturating_add(1);
        refreshed.committed_content_epoch = refreshed.content_epoch;
        fixture
            .meta()
            .set_attr(&refreshed)
            .expect("publish refreshed attributes");
        thread::sleep(Duration::from_millis(1_100));

        state
            .run_open_reply_transaction(
                attr.ino,
                || Ok(allocated),
                |prepared| {
                    assert_eq!(
                        (prepared.cache.keep_cache, prepared.reason),
                        (false, studiofs_vfs::vfs::OsCacheReason::GenerationChanged),
                        "the generation must match the refreshed attributes used for the reply"
                    );
                    -libc::ENOENT
                },
            )
            .expect("prepare refreshed open reply");
    }

    #[test]
    fn read_reply_buffer_returns_to_pool_only_after_reply_closure() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-reply-pool")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let mut buffer = state.read_buffers.take(4096);
        buffer[0] = 0x5a;
        let allocation = buffer.as_ptr();
        let spare = vec![0_u8; 4096];
        assert_ne!(spare.as_ptr(), allocation);
        state.read_buffers.put(spare);

        let reply_result = state.reply_with_pooled_read_buffer(buffer, |bytes| {
            assert_eq!(bytes[0], 0x5a);
            0
        });
        let reused = state.read_buffers.take(4096);

        assert_eq!(reply_result, 0);
        assert_eq!(reused.as_ptr(), allocation);
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
    fn invalidation_shutdown_clears_then_drains_then_closes_gate() {
        let events = RefCell::new(Vec::new());

        let outcome = super::shutdown_invalidation_with(
            || events.borrow_mut().push("clear"),
            || {
                events.borrow_mut().push("drain");
                crate::invalidation::FuseInvalidationShutdownOutcome::Drained
            },
            || events.borrow_mut().push("gate"),
        );

        assert_eq!(
            outcome,
            crate::invalidation::FuseInvalidationShutdownOutcome::Drained
        );
        assert_eq!(*events.borrow(), ["clear", "drain", "gate"]);
    }

    #[test]
    fn invalidation_shutdown_timeout_closes_gate_before_retention() {
        let events = RefCell::new(Vec::new());
        let state = super::SessionInvalidationState::new();

        let outcome = super::shutdown_invalidation_with(
            || events.borrow_mut().push("clear"),
            || {
                events.borrow_mut().push("drain");
                crate::invalidation::FuseInvalidationShutdownOutcome::TimedOut
            },
            || {
                events.borrow_mut().push("gate");
                state.begin_shutdown();
            },
        );

        assert_eq!(
            outcome,
            crate::invalidation::FuseInvalidationShutdownOutcome::TimedOut
        );
        assert_eq!(*events.borrow(), ["clear", "drain", "gate"]);
        assert!(
            state.begin_invalidation().is_none(),
            "a retained worker must not start another raw notification"
        );
    }

    #[test]
    fn retained_raw_session_removes_handlers_before_unmount() {
        let events = RefCell::new(Vec::new());

        super::detach_raw_session_with(
            true,
            || events.borrow_mut().push("signals"),
            || events.borrow_mut().push("unmount"),
        );

        assert_eq!(*events.borrow(), ["signals", "unmount"]);
    }

    #[test]
    fn raw_session_without_handlers_still_unmounts() {
        let events = RefCell::new(Vec::new());

        super::detach_raw_session_with(
            false,
            || events.borrow_mut().push("signals"),
            || events.borrow_mut().push("unmount"),
        );

        assert_eq!(*events.borrow(), ["unmount"]);
    }

    #[test]
    fn forget_request_updates_liveness_and_replies_exactly_once() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-forget")
            .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let reservation = state.inode_liveness.reserve_lookup(42);
        assert!(reservation.begin_reply());
        assert!(reservation.commit().is_some());
        let replies = AtomicUsize::new(0);

        super::forget_request_with(Some(&state), 42, 1, || {
            replies.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(replies.load(Ordering::Relaxed), 1);
        assert!(!state.inode_liveness.is_live(42));
    }

    #[test]
    fn forget_request_without_userdata_still_replies_exactly_once() {
        let replies = AtomicUsize::new(0);

        super::forget_request_with(None, 42, 1, || {
            replies.fetch_add(1, Ordering::Relaxed);
        });

        assert_eq!(replies.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn entry_reply_commits_only_after_native_success() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-entry-reply")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let accepted = fixture
            .vfs()
            .create(1, b"accepted", 0, 0, 0o644)
            .expect("create accepted inode");
        let rejected = fixture
            .vfs()
            .create(1, b"rejected", 0, 0, 0o644)
            .expect("create rejected inode");

        assert_eq!(super::reply_entry_with(&state, accepted.ino, |_| 0), Ok(0));
        assert!(state.inode_liveness.is_live(accepted.ino));
        assert_eq!(
            super::reply_entry_with(&state, rejected.ino, |_| -libc::ENOENT),
            Ok(-libc::ENOENT)
        );
        assert!(!state.inode_liveness.is_live(rejected.ino));
    }

    #[test]
    fn entry_reply_refreshes_attr_after_creating_lookup_reservation() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-entry-refresh")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let stale = fixture
            .vfs()
            .create(1, b"fresh-attr", 0, 0, 0o644)
            .expect("create inode");
        fixture
            .vfs()
            .setattr(
                stale.ino,
                studiofs_meta::SetattrArgs {
                    size: Some(99),
                    ..Default::default()
                },
            )
            .expect("update size before reply reservation");

        let result = super::reply_entry_with(&state, stale.ino, |entry| {
            // SAFETY: the reply helper keeps the entry live through this closure.
            let entry = unsafe { &*entry };
            assert_eq!(entry.attr.st_size, 99);
            0
        });

        assert_eq!(result, Ok(0));
    }

    #[test]
    fn entry_reply_race_enqueues_data_follow_up_after_commit() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-entry-race")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .create(1, b"raced", 0, 0, 0o644)
            .expect("create raced inode");

        assert_eq!(
            super::reply_entry_with(&state, attr.ino, |_| {
                let admission = state.inode_liveness.admit_invalidation(attr.ino, || {});
                assert_eq!(admission.deferred_reply_count(), 1);
                0
            }),
            Ok(0)
        );

        assert_eq!(
            state.invalidation_queue.pop(),
            Some((attr.ino, crate::invalidation::FuseInvalidationKind::Data))
        );
    }

    #[test]
    fn open_liveness_is_installed_before_reply_and_removed_on_rejection() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-open-liveness")
                .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        let attr = fixture
            .vfs()
            .create(1, b"open", 0, 0, 0o644)
            .expect("create inode");
        let fh = fixture
            .vfs()
            .open_file_with_known_attr(&attr, studiofs_vfs::OpenFlags::RDONLY, false)
            .expect("open VFS handle");

        let result = state
            .run_open_reply_transaction(
                attr.ino,
                || {
                    state.install_open_liveness(attr.ino, fh);
                    Ok(super::LinuxFuseAllocatedOpen {
                        ino: attr.ino,
                        fh,
                        flags: libc::O_RDONLY,
                    })
                },
                |_| {
                    assert!(state.inode_liveness.is_live(attr.ino));
                    -libc::ENOENT
                },
            )
            .expect("prepare open");

        assert_eq!(result, -libc::ENOENT);
        assert!(!state.inode_liveness.is_live(attr.ino));
        assert!(fixture.vfs().get_open_file(fh).is_none());
    }

    #[test]
    fn allocated_open_is_live_before_cache_preparation() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-open-gap")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .create(1, b"open-gap", 0, 0, 0o644)
            .expect("create inode");
        let allocated = state
            .open_with_flags(attr.ino, libc::O_RDONLY)
            .expect("allocate open");

        let admission = state.inode_liveness.admit_invalidation(attr.ino, || {});
        assert!(admission.notified_now());
        assert_eq!(admission.deferred_reply_count(), 0);

        let result = state
            .run_open_reply_transaction(attr.ino, || Ok(allocated), |_| -libc::ENOENT)
            .expect("prepare and reject open");
        assert_eq!(result, -libc::ENOENT);
        assert!(!state.inode_liveness.is_live(attr.ino));
    }

    #[test]
    fn create_reservation_precedes_writer_open_and_defers_follow_up() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-create-gap")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let allocated = state
            .create_with_flags(1, b"create-gap", libc::S_IFREG | 0o644, libc::O_WRONLY)
            .expect("allocate created writer");
        let ino = allocated.open.ino;
        let fh = allocated.open.fh;

        let admission = state.inode_liveness.admit_invalidation(ino, || {});
        assert!(admission.notified_now(), "open guard is already installed");
        assert_eq!(
            admission.deferred_reply_count(),
            1,
            "create lookup reservation predates writer open"
        );

        let result = state
            .run_create_reply_transaction(allocated, |attr, _prepared, lookup| {
                super::reply_reserved_entry_with(&state, attr, lookup, |_| 0)
            })
            .expect("complete create reply");
        assert_eq!(result, 0);
        assert_eq!(
            state.invalidation_queue.pop(),
            Some((ino, crate::invalidation::FuseInvalidationKind::Data))
        );

        state.remove_open_liveness(fh);
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(fh))
            .expect("release created writer");
        state.inode_liveness.forget(ino, 1);
    }

    #[test]
    fn rejected_directory_reply_reclaims_liveness_guard() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-dir-liveness")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .mkdir(1, b"dir", 0, 0, 0o755)
            .expect("create directory");
        let handle = state.open_dir(attr.ino).expect("open directory");
        assert!(state.inode_liveness.is_live(attr.ino));

        assert_eq!(
            super::finish_dir_open_reply(handle, -libc::ENOENT),
            -libc::ENOENT
        );
        assert!(!state.inode_liveness.is_live(attr.ino));
    }

    #[test]
    fn regular_release_keeps_inode_live_through_reply_on_error() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-release-liveness")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.install_open_liveness(42, 7);

        super::finish_regular_release_with(&state, Some(7), Err(libc::EIO), |errno| {
            assert_eq!(errno, libc::EIO);
            assert!(state.inode_liveness.is_live(42));
        });

        assert!(!state.inode_liveness.is_live(42));
    }

    #[test]
    fn directory_release_keeps_inode_live_through_reply() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-dir-release")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .mkdir(1, b"dir", 0, 0, 0o755)
            .expect("create directory");
        let handle = state.open_dir(attr.ino).expect("open directory");
        // SAFETY: this test owns the pointer returned by `open_dir`.
        let handle = unsafe { Box::from_raw(handle.as_ptr()) };

        super::finish_dir_release_with(Some(handle), || {
            assert!(state.inode_liveness.is_live(attr.ino));
        });

        assert!(!state.inode_liveness.is_live(attr.ino));
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
    fn committed_hardlink_succeeds_with_conservative_cache_when_queue_is_closed() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-link-closed-queue")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let source = fixture
            .vfs()
            .create(1, b"source.txt", 0, 0, 0o644)
            .expect("create source");
        state.invalidation_queue.close();

        let (linked, cache_safe) = state
            .link(source.ino, 1, b"alias.txt")
            .expect("committed link must still report success");

        assert_eq!(linked.ino, source.ino);
        assert!(!cache_safe);
        assert_eq!(
            fixture.vfs().lookup(1, b"alias.txt").expect("alias").ino,
            source.ino
        );
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
        fixture
            .runtime()
            .block_on(fixture.vfs().shutdown(Duration::ZERO))
            .expect("mounted VFS shutdown should succeed");
        fixture
            .runtime()
            .block_on(fixture.vfs().shutdown(Duration::ZERO))
            .expect("repeated mounted VFS shutdown should succeed");
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
            assert_eq!(std::fs::metadata(&hardlink)?.len(), 9);
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
