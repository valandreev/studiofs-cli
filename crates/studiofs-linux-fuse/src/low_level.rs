use super::*;
use std::collections::HashMap;
use std::ffi::{CStr, OsStr};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use crate::async_writes::{
    AsyncWriteLedger, AsyncWriteMetricsSnapshot, AsyncWriteTicket, BarrierOutcome,
    DEFAULT_MAX_PENDING_BYTES, DEFAULT_MAX_PENDING_WRITES,
};
use crate::cache_policy::{
    CachePolicySelection, LinuxFuseCachePolicy, OpenCacheDecision, ReadAheadIntent, SyncReadBuffer,
    SyncReadBufferPool, SystemMemorySampler, decide_open_cache,
};
use crate::directory_cache::{DirectorySnapshot, DirectorySnapshotCache, linux_observation};
use crate::invalidation::{
    FuseInodeLiveness, FuseInvalidationGeometry, FuseInvalidationKind,
    FuseInvalidationMetricsSnapshot, FuseInvalidationQueue, FuseInvalidationReceiptOutcome,
    FuseInvalidationShutdownOutcome, FuseInvalidationWorker, FuseLookupReservation,
    FuseOpenHandleGuard, LIVENESS_SHARDS,
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
    DirectoryEnumerationObservation, KernelInvalidator, LocalSyncPolicy, OpenFlags, ReadAheadOwner,
    StatFs, StudioVfs, VfsFuseAsyncWriteSnapshot, VfsFuseConnectionSnapshot,
    VfsFuseInvalidationSnapshot, read_runtime_stats_buffer,
};
use tokio::runtime::Handle;
use tracing::{debug, warn};

const TTL_SECONDS: f64 = 1.0;
const LINUX_FUSE_BLOCK_SIZE: u32 = 4096;
const LINUX_FUSE_MAX_READ_SIZE: usize = 4 * 1024 * 1024;
const INVALIDATION_SHUTDOWN_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
const NAMESPACE_ATTR_INVALIDATION_TIMEOUT: Duration = Duration::from_secs(2);
const ASYNC_WRITE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Ledger-tracked writes up to this size run on the callback thread.
const INLINE_WRITE_MAX_BYTES: usize = 64 * 1024;
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

    #[cfg(not(test))]
    pub(crate) use libfuse_add_direntry as fuse_add_direntry;

    // Per-thread so parallel libtest threads don't share counts; tests must call `push` on their own thread.
    #[cfg(test)]
    thread_local! {
        pub(crate) static DIRENTRY_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// # Safety
    /// `name` must be a readable NUL-terminated string and `stbuf` a valid stat.
    /// A non-null `buf` must be aligned for a FUSE dirent and writable for `bufsize`
    /// bytes without aliasing the inputs. Null requests are supported: libfuse ignores `req`.
    #[cfg(test)]
    pub(crate) unsafe fn fuse_add_direntry(
        req: *mut FuseReq,
        buf: *mut c_char,
        bufsize: size_t,
        name: *const c_char,
        stbuf: *const stat,
        off: off_t,
    ) -> size_t {
        DIRENTRY_CALLS.with(|calls| calls.set(calls.get() + 1));
        // SAFETY: the caller supplies the same pointers and capacity as the libfuse API.
        unsafe { libfuse_add_direntry(req, buf, bufsize, name, stbuf, off) }
    }

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
        #[link_name = "fuse_add_direntry"]
        pub(crate) fn libfuse_add_direntry(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            stbuf: *const stat,
            off: off_t,
        ) -> size_t;
        pub(crate) fn fuse_add_direntry_plus(
            req: *mut FuseReq,
            buf: *mut c_char,
            bufsize: size_t,
            name: *const c_char,
            entry: *const FuseEntryParam,
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
    fn studiofs_linux_fuse_wrap_exit_signal(sig: c_int) -> c_int;
    fn studiofs_linux_fuse_unwrap_exit_signal(sig: c_int);
    #[cfg(test)]
    fn studiofs_linux_fuse_enter_loop_thread();
    #[cfg(test)]
    fn studiofs_linux_fuse_leave_loop_thread();
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
    fn studiofs_linux_fuse_loop_has_worker_limit() -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_async_dio_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_conn_wants_writeback_cache_after_set(capable_ext: u64) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_direct_io_after_init() -> c_int;
    /// Returns null on allocation failure; free a nonnull result once with `libc::free`.
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_new_for_test(fh: u64) -> *mut raw::FuseFileInfo;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_flags_after_init(
        direct_io: c_int,
        keep_cache: c_int,
        parallel_direct_writes: c_int,
    ) -> c_int;
    #[cfg(test)]
    fn studiofs_linux_fuse_file_info_has_parallel_direct_writes() -> c_int;
}

pub(crate) struct SessionState {
    async_write_source: u64,
    vfs: Arc<StudioVfs>,
    runtime_handle: Handle,
    async_writes: Arc<AsyncWriteLedger>,
    read_only: bool,
    mount_uid: u32,
    mount_gid: u32,
    connection_config: LinuxFuseConnectionConfig,
    cache_policy: LinuxFuseCachePolicy,
    async_direct_io: bool,
    available_memory: Option<SystemMemorySampler>,
    read_buffers: SyncReadBufferPool,
    directory_buffers: DirectoryEntryBufferPool,
    directory_snapshots: DirectorySnapshotCache,
    open_reply_serializer: FuseOpenReplySerializer,
    inode_liveness: FuseInodeLiveness,
    open_liveness: [Mutex<HashMap<u64, FuseOpenHandleGuard>>; LIVENESS_SHARDS],
    invalidation_queue: FuseInvalidationQueue,
    /// Runtime stats text captured at open, keyed by VFS file handle.
    runtime_stats_snapshots: Mutex<HashMap<u64, Arc<[u8]>>>,
    #[cfg(test)]
    getattr_calls: AtomicU64,
    #[cfg(test)]
    lookup_calls: Arc<AtomicU64>,
    /// Inline writes hold a callback worker; at most this many run at once.
    inline_write_limit: usize,
    inline_writes: AtomicUsize,
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
        let async_writes = Arc::new(AsyncWriteLedger::with_limits(
            DEFAULT_MAX_PENDING_BYTES,
            DEFAULT_MAX_PENDING_WRITES,
        ));
        let cache_policy = linux_fuse_cache_policy_from_env();
        let async_write_source = register_async_write_metrics(&vfs, &async_writes, cache_policy);
        Self {
            async_write_source,
            vfs,
            runtime_handle,
            async_writes,
            read_only,
            mount_uid: current_uid(),
            mount_gid: current_gid(),
            connection_config,
            cache_policy,
            async_direct_io: linux_fuse_env_flag("STUDIOFS_FUSE_ASYNC_DIO"),
            available_memory: (cache_policy == LinuxFuseCachePolicy::Adaptive)
                .then(SystemMemorySampler::system),
            read_buffers: SyncReadBufferPool::new(),
            directory_buffers: DirectoryEntryBufferPool::default(),
            directory_snapshots: DirectorySnapshotCache::default(),
            open_reply_serializer: FuseOpenReplySerializer::new(),
            inode_liveness: FuseInodeLiveness::default(),
            open_liveness: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            invalidation_queue: FuseInvalidationQueue::default(),
            runtime_stats_snapshots: Mutex::new(HashMap::new()),
            #[cfg(test)]
            getattr_calls: AtomicU64::new(0),
            #[cfg(test)]
            lookup_calls: Arc::new(AtomicU64::new(0)),
            // Leave half the callback workers for metadata and reads while
            // inline writes wait on VFS backpressure.
            inline_write_limit: usize::try_from(
                connection_config.worker_thread_count(has_worker_limit()) / 2,
            )
            .unwrap_or(usize::MAX)
            .max(1),
            inline_writes: AtomicUsize::new(0),
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

/// The write reply is consumed only when the request is accepted for async handling.
struct FuseWriteCallbackContext<R> {
    reply: R,
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
        BarrierOutcome::TimedOut => return Err(libc::EIO),
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
    observed: ObservedInodeAttr,
    fh: u64,
    flags: c_int,
}

#[derive(Debug)]
struct ObservedInodeAttr {
    attr: InodeAttr,
    observation: studiofs_vfs::AttributeObservation,
    expires_at: Option<Instant>,
}

struct LinuxFuseAllocatedCreate {
    open: LinuxFuseAllocatedOpen,
    lookup: FuseLookupReservation,
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

/// Query whether the session requests asynchronous direct I/O.
///
/// # Safety
///
/// `userdata` must be null or point to a valid, initialized `SessionState`
/// that remains alive and is not mutated for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_async_dio(
    userdata: *mut c_void,
) -> c_int {
    fuse_flag(callback_state_from_userdata(userdata).is_some_and(|state| state.async_direct_io))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn studiofs_linux_fuse_connection_wants_writeback_cache(
    userdata: *mut c_void,
) -> c_int {
    // Without a session there is no chosen policy; never request writeback.
    fuse_flag(
        callback_state_from_userdata(userdata)
            .is_some_and(|state| state.cache_policy.wants_writeback_cache()),
    )
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
    tracing::info!(
        requested_max_background = state.connection_config.max_background,
        effective_max_background,
        effective_congestion_threshold,
        worker_threads = state
            .connection_config
            .worker_thread_count(has_worker_limit()),
        "Linux FUSE connection initialized"
    );
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

pub(crate) fn has_worker_limit() -> bool {
    // SAFETY: the C shim reports its compile-time loop API selection.
    unsafe { studiofs_linux_fuse_loop_has_worker_limit() != 0 }
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
    let selection = linux_fuse_cache_policy_selection_for_value(
        value.as_deref(),
        LINUX_FUSE_ADAPTIVE_DEFAULT_READY,
    );
    if selection.warning_required {
        warn!(
            value = value.as_deref().unwrap_or_default(),
            "unknown STUDIOFS_LINUX_FUSE_CACHE_POLICY; using adaptive policy"
        );
    }
    selection.policy
}

fn linux_fuse_cache_policy_selection_for_value(
    value: Option<&str>,
    adaptive_default_ready: bool,
) -> CachePolicySelection {
    let mut selection = LinuxFuseCachePolicy::parse(value);
    if !adaptive_default_ready && value.is_none() {
        selection.policy = LinuxFuseCachePolicy::Direct;
    }
    selection
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

fn argv_strings(volume_name: &str, read_only: bool) -> Vec<String> {
    let mut args = vec![
        "studiofs-linux-fuse".to_owned(),
        "-o".to_owned(),
        format!("fsname={volume_name}"),
        "-o".to_owned(),
        "subtype=studiofs".to_owned(),
        "-o".to_owned(),
        "auto_unmount".to_owned(),
        "-o".to_owned(),
        "default_permissions".to_owned(),
        "-o".to_owned(),
        format!("max_read={LINUX_FUSE_MAX_READ_SIZE}"),
    ];
    if read_only {
        args.extend(["-o".to_owned(), "ro".to_owned()]);
    }
    args
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
        let root_prime_path = mount_point.as_ref().to_path_buf();
        let mount_point = path_to_cstring(mount_point.as_ref())?;
        let mut args = OwnedFuseArgs::new(argv_strings(volume_name.as_ref(), state.read_only))?;
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
            spawn_root_attr_primer(root_prime_path, Arc::clone(&invalidation_state));
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
            for signal in FUSE_EXIT_SIGNALS {
                // SAFETY: wrapping only replaces the plain handler libfuse just
                // installed; the wrapper chains to it.
                unsafe { studiofs_linux_fuse_wrap_exit_signal(signal) };
            }
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
        let mut args = OwnedFuseArgs::new(argv_strings(volume_name.as_ref(), false))?;
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
        let connection_config = self
            .state
            .as_ref()
            .map(|state| state.connection_config)
            .unwrap_or_default();
        let hard_worker_limit = has_worker_limit();
        let loop_threads = connection_config.worker_thread_count(hard_worker_limit);
        tracing::info!(
            requested_worker_threads = ?connection_config.worker_threads,
            worker_threads = loop_threads,
            max_background = connection_config.max_background,
            hard_worker_limit,
            "Linux FUSE callback worker policy"
        );
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

/// Signals whose libfuse handler exits the session loop.
///
/// The handler only sets a flag that the loop thread sees when the signal
/// interrupts its own wait, so the C shim forwards these signals to that thread.
const FUSE_EXIT_SIGNALS: [c_int; 3] = [libc::SIGHUP, libc::SIGINT, libc::SIGTERM];

/// Hold kernel invalidations until the kernel has applied real root attributes.
///
/// fusermount3 creates the root inode without permission bits. If an inode
/// invalidation lands while its first GETATTR reply is in flight, the kernel
/// drops that reply and `default_permissions` denies the access with EACCES.
fn spawn_root_attr_primer(mount_point: std::path::PathBuf, state: Arc<SessionInvalidationState>) {
    let primer_state = Arc::clone(&state);
    let spawned = std::thread::Builder::new()
        .name("studiofs-fuse-root-prime".to_owned())
        .spawn(move || {
            // Blocks until the session loop answers INIT and GETATTR(root), or
            // fails once the session is unmounted.
            if let Err(error) = std::fs::metadata(&mount_point) {
                debug!(%error, "Linux FUSE root attribute priming failed");
            }
            primer_state.mark_root_primed();
        });
    if let Err(error) = spawned {
        warn!(%error, "Linux FUSE root attribute priming thread failed to start");
        state.mark_root_primed();
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
    root_primed: bool,
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
                root_primed: false,
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
        let mut status = self
            .idle
            .wait_while(self.lock_status(), |status| {
                !status.root_primed && !status.closed
            })
            .unwrap_or_else(|err| err.into_inner());
        if status.closed {
            return None;
        }
        status.active += 1;
        Some(SessionInvalidationGuard { state: self })
    }

    fn begin_shutdown(&self) {
        self.lock_status().closed = true;
        self.idle.notify_all();
    }

    fn mark_root_primed(&self) {
        self.lock_status().root_primed = true;
        self.idle.notify_all();
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
            let read = buffer.as_ref().len();
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
        FuseWriteCallbackContext {
            reply: FuseReqPtr(req),
            started,
        },
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
        Ok(attr) if attr.kind == FileType::Directory => match state.open_dir(ino, attr.parent) {
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
    let mut buffer = state.directory_buffers.take(size);
    match state.readdir(req, ino, handle, off, &mut buffer) {
        Ok(()) => {
            let bytes = buffer.as_slice();
            // SAFETY: `bytes` is valid for this synchronous reply.
            unsafe { raw::fuse_reply_buf(req, bytes.as_ptr().cast::<c_char>(), bytes.len()) };
        }
        Err(errno) => reply_error(req, errno),
    }
    state.directory_buffers.put(buffer);
}

#[unsafe(no_mangle)]
pub extern "C" fn studiofs_linux_fuse_readdirplus(
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
    let mut buffer = state.directory_buffers.take(size);
    match state.readdirplus(req, ino, handle, off, &mut buffer) {
        Ok(reservations) => {
            let bytes = buffer.as_slice();
            reply_reserved_entries_with(state, reservations, || {
                // SAFETY: `bytes` is valid for this synchronous reply.
                unsafe { raw::fuse_reply_buf(req, bytes.as_ptr().cast::<c_char>(), bytes.len()) }
            });
        }
        Err(errno) => reply_error(req, errno),
    }
    state.directory_buffers.put(buffer);
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
    fn observe_attr(
        &self,
        read: impl FnOnce() -> Result<InodeAttr, i32>,
    ) -> Result<ObservedInodeAttr, i32> {
        let observation = self.vfs.observe_attributes();
        read().map(|attr| {
            let expires_at = self.vfs.attribute_cache_deadline(&attr);
            ObservedInodeAttr {
                attr,
                observation,
                expires_at,
            }
        })
    }

    fn current_attr(&self, observed: ObservedInodeAttr) -> Result<InodeAttr, i32> {
        if observed
            .expires_at
            .is_some_and(|deadline| Instant::now() < deadline)
            && self
                .vfs
                .can_reuse_attributes(&observed.attr, observed.observation)
                .map_err(|error| vfs_err_to_errno(&error))?
        {
            Ok(observed.attr)
        } else {
            self.getattr_attr(observed.attr.ino)
        }
    }

    fn lookup(&self, parent: u64, name: &[u8]) -> Result<ObservedInodeAttr, i32> {
        #[cfg(test)]
        self.lookup_calls.fetch_add(1, Ordering::Relaxed);
        self.observe_attr(|| {
            self.vfs
                .lookup(parent, name)
                .map_err(|error| vfs_err_to_errno(&error))
        })
    }

    fn getattr_attr(&self, ino: u64) -> Result<InodeAttr, i32> {
        #[cfg(test)]
        self.getattr_calls.fetch_add(1, Ordering::Relaxed);
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
        let observed = self.observe_attr(|| {
            self.vfs
                .create_with_deferred_metadata_admission(
                    parent,
                    name,
                    self.mount_uid,
                    self.mount_gid,
                    mode,
                    &self.runtime_handle,
                )
                .map_err(|error| vfs_err_to_errno(&error))
        })?;
        let attr = &observed.attr;
        // Reserve the kernel lookup before opening the writer handle. Any
        // invalidation from writer admission onward is now marked for a
        // post-reply DATA notification.
        let lookup = self.inode_liveness.reserve_lookup(attr.ino);
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        let fh = self
            .vfs
            .open_created_file_with_known_attr_and_read_ahead_owner(
                attr,
                open_flags,
                trunc,
                ReadAheadOwner::Kernel,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        self.install_open_liveness(attr.ino, fh);
        Ok(LinuxFuseAllocatedCreate {
            open: LinuxFuseAllocatedOpen {
                observed,
                fh,
                flags,
            },
            lookup,
        })
    }

    fn mkdir(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<ObservedInodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let mode = (mode & 0o7777) as u16;
        self.observe_attr(|| {
            let result = self.vfs.mkdir_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            );
            result.map_err(|error| vfs_err_to_errno(&error))
        })
    }

    fn mknod(&self, parent: u64, name: &[u8], mode: mode_t) -> Result<ObservedInodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let file_type = mode & libc::S_IFMT as mode_t;
        if file_type != 0 && file_type != libc::S_IFREG as mode_t {
            return Err(libc::ENOTSUP);
        }
        let mode = (mode & 0o7777) as u16;
        self.observe_attr(|| {
            let result = self.vfs.create_with_deferred_metadata_admission(
                parent,
                name,
                self.mount_uid,
                self.mount_gid,
                mode,
                &self.runtime_handle,
            );
            result.map_err(|error| vfs_err_to_errno(&error))
        })
    }

    fn symlink(&self, parent: u64, name: &[u8], target: &[u8]) -> Result<ObservedInodeAttr, i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        self.observe_attr(|| {
            self.vfs
                .symlink(parent, name, target, self.mount_uid, self.mount_gid)
                .map_err(|error| vfs_err_to_errno(&error))
        })
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
            .rename(parent, name, newparent, newname, true)
            .map_err(|error| vfs_err_to_errno(&error))
    }

    fn link(&self, ino: u64, newparent: u64, newname: &[u8]) -> Result<(InodeAttr, bool), i32> {
        if self.read_only {
            return Err(libc::EROFS);
        }
        let watermark = self.async_writes.capture_inode_watermark(ino);
        let outcome = self.async_writes.wait_inode_completion(watermark);
        let attr = finish_async_barrier_with(outcome, || {
            self.vfs
                .link(ino, newparent, newname)
                .map_err(|error| vfs_err_to_errno(&error))
        })?;
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
        let observed = self.observe_attr(|| self.getattr_attr(ino))?;
        let attr = &observed.attr;
        match attr.kind {
            FileType::RegularFile => {}
            FileType::Directory => return Err(libc::EISDIR),
            FileType::Symlink => return Err(libc::EINVAL),
        }
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.read_only && (trunc || open_flags.write_access_requested()) {
            return Err(libc::EROFS);
        }
        let runtime_stats = self.vfs.is_runtime_stats_file_ino(ino);
        if runtime_stats && (trunc || open_flags.write_access_requested()) {
            return Err(libc::EACCES);
        }
        let fh = self
            .vfs
            .open_file_with_known_attr_and_read_ahead_owner(
                attr,
                open_flags,
                trunc,
                ReadAheadOwner::Kernel,
            )
            .map_err(|error| vfs_err_to_errno(&error))?;
        // Establish kernel liveness before generation preparation or any
        // operation that can publish an invalidation for this handle.
        self.install_open_liveness(ino, fh);
        if runtime_stats {
            // Each open reads one consistent snapshot, matching WinFSP and macFUSE.
            let snapshot = Arc::from(self.vfs.runtime_stats_file_text().into_bytes());
            self.runtime_stats_snapshots_guard().insert(fh, snapshot);
        }
        Ok(LinuxFuseAllocatedOpen {
            observed,
            fh,
            flags,
        })
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
        let available_memory = self
            .available_memory
            .as_ref()
            .and_then(SystemMemorySampler::available_memory);
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
                self.async_direct_io,
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

    fn allocated_open_attr(
        &self,
        observed: ObservedInodeAttr,
        flags: c_int,
    ) -> Result<InodeAttr, i32> {
        // O_TRUNC changes the snapshot used to allocate the handle itself.
        if flags & libc::O_TRUNC != 0 {
            self.getattr_attr(observed.attr.ino)
        } else {
            self.current_attr(observed)
        }
    }

    fn prepare_allocated_open(
        &self,
        allocated: LinuxFuseAllocatedOpen,
    ) -> Result<LinuxFusePreparedOpen, i32> {
        let attr = self.allocated_open_attr(allocated.observed, allocated.flags)?;
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
        debug_assert_eq!(allocated.observed.attr.ino, ino);
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
        let ino = allocated.open.observed.attr.ino;
        let _reply_guard = self.open_reply_serializer.lock(ino);
        let attr = match self.allocated_open_attr(allocated.open.observed, allocated.open.flags) {
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
        self.runtime_stats_snapshots_guard().remove(&fh);
        self.remove_open_liveness(fh);
        self.vfs.abort_os_cache_open(fh);
        let _ = self
            .vfs
            .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
    }

    fn install_open_liveness(&self, ino: u64, fh: u64) {
        let guard = self.inode_liveness.open_handle(ino);
        let replaced = self
            .open_liveness_shard(fh)
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(fh, guard);
        debug_assert!(replaced.is_none(), "VFS file handles must be unique");
    }

    fn open_liveness_shard(&self, fh: u64) -> &Mutex<HashMap<u64, FuseOpenHandleGuard>> {
        &self.open_liveness[usize::from(fh.to_le_bytes()[0]) % LIVENESS_SHARDS]
    }

    fn remove_open_liveness(&self, fh: u64) -> Option<FuseOpenHandleGuard> {
        self.open_liveness_shard(fh)
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&fh)
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
        buffer: SyncReadBuffer,
        reply: impl FnOnce(&[u8]) -> c_int,
    ) -> c_int {
        let result = reply(buffer.as_ref());
        self.read_buffers.put(buffer);
        result
    }

    fn read(
        &self,
        ino: u64,
        size: usize,
        offset: off_t,
        fi: *const raw::FuseFileInfo,
    ) -> Result<SyncReadBuffer, i32> {
        let fh = file_handle(fi).ok_or(libc::EBADF)?;
        let offset = u64::try_from(offset).map_err(|_| libc::EINVAL)?;
        let size = clamp_read_size(size);
        if self.vfs.is_runtime_stats_file_ino(ino) {
            return self.read_runtime_stats(fh, offset, size);
        }
        let mut buffer = self.read_buffers.take(size);
        let read = match self.runtime_handle.block_on(self.vfs.read_into_fh(
            fh,
            ino,
            offset,
            buffer.as_mut(),
        )) {
            Ok(read) => read,
            Err(error) => {
                self.read_buffers.put(buffer);
                return Err(vfs_err_to_errno(&error));
            }
        };
        buffer.truncate(read as usize);
        Ok(buffer)
    }

    fn read_runtime_stats(&self, fh: u64, offset: u64, size: usize) -> Result<SyncReadBuffer, i32> {
        let snapshot = self
            .runtime_stats_snapshots_guard()
            .get(&fh)
            .cloned()
            .ok_or(libc::EBADF)?;
        let mut buffer = self.read_buffers.take(size);
        let read = read_runtime_stats_buffer(&snapshot, offset, buffer.as_mut());
        buffer.truncate(read as usize);
        Ok(buffer)
    }

    fn runtime_stats_snapshots_guard(&self) -> MutexGuard<'_, HashMap<u64, Arc<[u8]>>> {
        self.runtime_stats_snapshots
            .lock()
            .unwrap_or_else(|error| error.into_inner())
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

    fn queue_async_write<R: FuseWriteReply + 'static>(
        &self,
        callback: FuseWriteCallbackContext<R>,
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
        if let Err(error) = ticket.reserve_capacity(size) {
            let errno = error.errno();
            ticket.complete(Err(errno));
            return Err(errno);
        }
        let fh = request.fh;
        let write_offset = request.offset;
        let sequence = ticket.sequence();
        if size <= INLINE_WRITE_MAX_BYTES
            && let Some(_inline) =
                InlineWriteSlot::acquire(&self.inline_writes, self.inline_write_limit)
        {
            let task_guard = FuseWriteTaskGuard::with_metrics(
                callback.reply,
                ticket,
                Arc::clone(&self.vfs),
                size,
                callback.started,
            );
            // The kernel waits for this reply either way, so for a small write a
            // runtime hand-off only adds a copy, a spawn, and a cross-thread reply.
            // The ticket keeps barrier, shutdown, and capacity accounting unchanged.
            if !self.async_writes.mark_task_non_cancellable(sequence) {
                // Shutdown cancelled the request; dropping the guard replies EIO.
                return Ok(true);
            }
            let result = with_write_data_slice(buf, size, |data| {
                catch_inline_write_panic(|| {
                    self.runtime_handle
                        .block_on(self.vfs.write_fh_with_deferred_materialize(
                            fh,
                            ino,
                            write_offset,
                            data,
                            LocalSyncPolicy::Opportunistic,
                        ))
                        .map_err(|error| vfs_err_to_errno(&error))
                })
            })
            .and_then(|result| result);
            let reply_result = task_guard.complete(result);
            if reply_result != 0 {
                debug!(
                    ino,
                    fh,
                    result = reply_result,
                    "Linux FUSE inline write reply was rejected"
                );
            }
            return Ok(true);
        }
        // The request buffer remains owned by libfuse while this callback is
        // blocked on admission. Copy only after bounded capacity is reserved.
        let data = with_write_data_slice(buf, size, Bytes::copy_from_slice)?;
        let vfs = self.vfs.clone();
        let task_guard = FuseWriteTaskGuard::with_metrics(
            callback.reply,
            ticket,
            Arc::clone(&self.vfs),
            size,
            callback.started,
        );
        let write_ledger = Arc::clone(&self.async_writes);
        let task = self.runtime_handle.spawn(async move {
            if !write_ledger.mark_task_non_cancellable(sequence) {
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
        });
        self.async_writes.track_task(sequence, task.abort_handle());
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
        finish_async_barrier_with(outcome, || {
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
        })
    }

    fn fsync(&self, file_handle: Option<u64>, ino: u64, datasync: bool) -> Result<(), i32> {
        let watermark = self.async_writes.capture_inode_watermark(ino);
        let outcome = self.async_writes.wait_inode(watermark);
        finish_async_barrier_with(outcome, || {
            let result = if let Some(fh) = file_handle {
                // StudioVfs does not currently distinguish fdatasync from fsync on
                // this path; both drain the handle and execute the strict metadata barrier.
                self.runtime_handle.block_on(self.vfs.fsync_fh(fh))
            } else {
                self.runtime_handle.block_on(self.vfs.fsync(ino, datasync))
            };
            result.map_err(|error| vfs_err_to_errno(&error))
        })
    }

    fn release(&self, fh: u64) -> Result<(), i32> {
        self.runtime_stats_snapshots_guard().remove(&fh);
        let local_sync_policy = self.local_sync_policy_for_handle(fh);
        let watermark = self.async_writes.close_handle(fh);
        let outcome = self.async_writes.wait_handle(watermark);
        let _forget = matches!(outcome, BarrierOutcome::Complete(_)).then(|| ForgetClosedHandle {
            ledger: &self.async_writes,
            fh,
        });
        finish_async_barrier_with(outcome, || {
            if local_sync_policy.is_opportunistic() {
                self.vfs
                    .release_fh_with_deferred_last_handle_barrier(fh, &self.runtime_handle);
                return Ok(());
            }
            self.runtime_handle
                .block_on(self.vfs.release_fh(fh))
                .map_err(|error| vfs_err_to_errno(&error))
        })
    }

    fn vfs_file_handle(&self, fi: *const raw::FuseFileInfo) -> Option<u64> {
        file_handle(fi).filter(|fh| self.vfs.get_open_file(*fh).is_some())
    }

    /// `parent_ino` is captured at open, matching macFUSE; the root reports itself.
    fn open_dir(&self, ino: u64, parent_ino: u64) -> Result<NonNull<DirectoryHandle>, i32> {
        let liveness = self.inode_liveness.open_handle(ino);
        let raw = Box::into_raw(Box::new(DirectoryHandle {
            parent_ino: if parent_ino == 0 { ino } else { parent_ino },
            entries: Mutex::new(None),
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
        buffer: &mut DirectoryEntryBuffer,
    ) -> Result<(), i32> {
        self.read_directory(req, ino, handle, offset, buffer, None)
    }

    /// Returns one lookup reservation per entry that carries a kernel entry.
    fn readdirplus(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        handle: NonNull<DirectoryHandle>,
        offset: off_t,
        buffer: &mut DirectoryEntryBuffer,
    ) -> Result<Vec<(u64, FuseLookupReservation)>, i32> {
        let mut reservations = Vec::new();
        self.read_directory(req, ino, handle, offset, buffer, Some(&mut reservations))?;
        Ok(reservations)
    }

    /// `plus` selects `readdirplus` encoding and collects its lookup reservations.
    fn read_directory(
        &self,
        req: *mut raw::FuseReq,
        ino: u64,
        handle: NonNull<DirectoryHandle>,
        offset: off_t,
        buffer: &mut DirectoryEntryBuffer,
        mut plus: Option<&mut Vec<(u64, FuseLookupReservation)>>,
    ) -> Result<(), i32> {
        let mut idx = usize::try_from(offset).map_err(|_| libc::EINVAL)?;
        let dir_attr = minimal_dir_stat(ino);
        // SAFETY: `handle` comes from `opendir` and remains owned by the
        // matching directory handle until `releasedir`.
        let handle = unsafe { handle.as_ref() };
        let entries = &handle.entries;
        let encoding = if plus.is_some() {
            DirectoryEntryEncoding::Plus
        } else {
            DirectoryEntryEncoding::Plain
        };
        if idx == 0 {
            // Like WinFSP's marker-less read, a rewind starts a fresh enumeration.
            *entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            if !buffer.push_name_only(req, encoding, OsStr::new("."), &dir_attr, 1)? {
                return Ok(());
            }
            idx = 1;
        }
        if idx == 1 {
            if !buffer.push_name_only(
                req,
                encoding,
                OsStr::new(".."),
                &minimal_dir_stat(handle.parent_ino),
                2,
            )? {
                return Ok(());
            }
            idx = 2;
        }

        let minimum_entry_size = encoding.minimum_entry_size(req, &dir_attr);
        if buffer.capacity.saturating_sub(buffer.len) < minimum_entry_size {
            return Ok(());
        }
        let mut captured = entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Capture once per enumeration; namespace changes must not shift existing offsets.
        let (captured, fresh) = match captured.as_ref() {
            Some(captured) => (captured, false),
            None => (&*captured.insert(self.directory_snapshot(ino)?), true),
        };
        // Offsets stay on the captured snapshot, but kernel dentries must not:
        // after a namespace change, a later chunk sends names only. A fresh
        // capture already compared observations before and after its build.
        let reply_entries = plus.is_some()
            && if fresh {
                captured.observation.is_some()
            } else {
                self.directory_snapshot_is_current(ino, captured)
            };
        for (i, entry) in captured
            .entries
            .iter()
            .enumerate()
            .skip(idx.saturating_sub(2))
        {
            let entry_idx = i + 2;
            let attr = minimal_entry_stat(entry.ino, entry.kind);
            let next_offset = off_t::try_from(entry_idx + 1).map_err(|_| libc::EOVERFLOW)?;
            let name = OsStr::from_bytes(&entry.name);
            let pushed = match plus.as_deref_mut() {
                Some(reservations) if reply_entries => {
                    let reservation = self.inode_liveness.reserve_lookup(entry.ino);
                    // Read after reservation: later invalidations mark the pending reply.
                    match self.getattr_attr(entry.ino) {
                        Ok(entry_attr) => {
                            let param = entry_param(self.attr_to_stat(&entry_attr), TTL_SECONDS);
                            let pushed = buffer.push_plus(req, name, &param, next_offset)?;
                            if pushed {
                                reservations.push((entry.ino, reservation));
                            }
                            pushed
                        }
                        Err(_) => buffer.push_name_only(req, encoding, name, &attr, next_offset)?,
                    }
                }
                _ => buffer.push_name_only(req, encoding, name, &attr, next_offset)?,
            };
            if !pushed {
                break;
            }
        }
        Ok(())
    }

    fn directory_snapshot_is_current(&self, ino: u64, captured: &CapturedDirectory) -> bool {
        captured.observation.is_some_and(|observation| {
            self.vfs
                .directory_enumeration_observation(ino)
                .is_ok_and(|current| linux_observation(current) == linux_observation(observation))
        })
    }

    /// Reuses a session-cached snapshot while the directory observation is unchanged.
    // shortcut: an uncached capture still holds O(N) names per handle, as WinFSP's
    // DirBuffer does; use metadata cursors if large directories need bounded memory.
    fn directory_snapshot(&self, ino: u64) -> Result<CapturedDirectory, i32> {
        let observe = || {
            self.vfs
                .directory_enumeration_observation(ino)
                .map_err(|error| vfs_err_to_errno(&error))
        };
        let observation = observe()?;
        if let Some(entries) = self.directory_snapshots.lookup(ino, observation) {
            return Ok(CapturedDirectory {
                entries,
                observation: Some(observation),
            });
        }
        let entries = DirectorySnapshot::from(
            self.vfs
                .readdir(ino)
                .map_err(|error| vfs_err_to_errno(&error))?,
        );
        // Never cache a build that crossed an invalidation.
        let unchanged = linux_observation(observe()?) == linux_observation(observation);
        if unchanged {
            self.directory_snapshots
                .insert(ino, observation, Arc::clone(&entries));
        }
        Ok(CapturedDirectory {
            entries,
            observation: unchanged.then_some(observation),
        })
    }

    fn statfs(&self) -> Result<statvfs, i32> {
        self.vfs
            .statfs()
            .map(statfs_to_statvfs)
            .map_err(|error| vfs_err_to_errno(&error))
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
    parent_ino: u64,
    entries: Mutex<Option<CapturedDirectory>>,
    _liveness: FuseOpenHandleGuard,
}

/// One handle's enumeration and the observation it was built under.
struct CapturedDirectory {
    entries: DirectorySnapshot,
    /// `None` when the build crossed a namespace change.
    observation: Option<DirectoryEnumerationObservation>,
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

/// Counts one inline write against the session limit until dropped.
struct InlineWriteSlot<'a>(&'a AtomicUsize);

impl<'a> InlineWriteSlot<'a> {
    fn acquire(in_flight: &'a AtomicUsize, limit: usize) -> Option<Self> {
        in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(in_flight))
    }
}

impl Drop for InlineWriteSlot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// An unwind must not cross the `extern "C"` write callback; the async path
/// confines the same panic to its task, so the inline path replies EIO.
fn catch_inline_write_panic(write: impl FnOnce() -> Result<u32, i32>) -> Result<u32, i32> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(write)).unwrap_or_else(|_| {
        warn!("Linux FUSE inline write panicked");
        Err(libc::EIO)
    })
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
    result: Result<ObservedInodeAttr, i32>,
) -> c_int {
    match result {
        Ok(observed) => match reply_entry_with(state, observed, |entry| {
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
    observed: ObservedInodeAttr,
    reply: impl FnOnce(*const raw::FuseEntryParam) -> c_int,
) -> Result<c_int, i32> {
    let reservation = state.inode_liveness.reserve_lookup(observed.attr.ino);
    // Check after reservation: later invalidations mark the pending reply.
    let attr = state.current_attr(observed)?;
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
    let entry = entry_param(state.attr_to_stat(attr), attr_timeout);
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

fn entry_param(attr: stat, attr_timeout: f64) -> raw::FuseEntryParam {
    raw::FuseEntryParam {
        ino: attr.st_ino,
        generation: 1,
        attr,
        attr_timeout,
        entry_timeout: TTL_SECONDS,
    }
}

/// Commits every `readdirplus` entry the kernel accepted, or none of them.
fn reply_reserved_entries_with(
    state: &SessionState,
    reservations: Vec<(u64, FuseLookupReservation)>,
    reply: impl FnOnce() -> c_int,
) -> c_int {
    for (_, reservation) in &reservations {
        let began = reservation.begin_reply();
        debug_assert!(began, "fresh lookup reservation must begin exactly once");
    }
    let result = reply();
    if result == 0 {
        for (ino, reservation) in reservations {
            if reservation
                .commit()
                .is_some_and(|outcome| outcome.follow_up_invalidation_required())
            {
                state.enqueue_follow_up_invalidation(ino);
            }
        }
    }
    // A rejected reply drops the reservations, rolling each one back.
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
        let guard = state.remove_open_liveness(fh);
        if state.vfs.get_open_file(fh).is_some() {
            let vfs = Arc::clone(&state.vfs);
            let runtime = state.runtime_handle.clone();
            state.async_writes.defer_release(fh, move || {
                vfs.release_fh_with_deferred_last_handle_barrier(fh, &runtime);
                drop(guard);
            });
        }
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

// Bound idle reply storage independently of how many directories remain open.
const MAX_DIRECTORY_BUFFERS: usize = 16;
const MAX_DIRECTORY_BUFFER_CAPACITY: usize = 128 * 1024;
const MAX_DIRECTORY_SCRATCH_CAPACITY: usize = 4096;

#[derive(Default)]
struct DirectoryEntryBufferPool {
    buffers: Mutex<Vec<DirectoryEntryBuffer>>,
}

impl DirectoryEntryBufferPool {
    fn take(&self, capacity: usize) -> DirectoryEntryBuffer {
        let buffer = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop();
        let mut buffer = buffer.unwrap_or_else(|| DirectoryEntryBuffer::new(capacity));
        buffer
            .words
            .reserve_exact(capacity.div_ceil(std::mem::size_of::<u64>()));
        buffer.scratch_name.clear();
        buffer.len = 0;
        buffer.capacity = capacity;
        buffer
    }

    fn put(&self, buffer: DirectoryEntryBuffer) {
        if buffer.words.capacity() > MAX_DIRECTORY_BUFFER_CAPACITY / std::mem::size_of::<u64>()
            || buffer.scratch_name.capacity() > MAX_DIRECTORY_SCRATCH_CAPACITY
        {
            return;
        }
        let mut buffers = self
            .buffers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if buffers.len() < MAX_DIRECTORY_BUFFERS {
            buffers.push(buffer);
        }
    }
}

#[derive(Clone, Copy)]
enum DirectoryEntryEncoding {
    Plain,
    Plus,
}

impl DirectoryEntryEncoding {
    /// Queries libfuse for the size of a one-byte-name entry; no ABI is hardcoded.
    fn minimum_entry_size(self, req: *mut raw::FuseReq, attr: &stat) -> size_t {
        // SAFETY: a null buffer queries size only; name and attr are valid, req is ignored.
        unsafe {
            match self {
                Self::Plain => {
                    raw::fuse_add_direntry(req, std::ptr::null_mut(), 0, c".".as_ptr(), attr, 0)
                }
                Self::Plus => raw::fuse_add_direntry_plus(
                    req,
                    std::ptr::null_mut(),
                    0,
                    c".".as_ptr(),
                    &name_only_entry_param(attr),
                    0,
                ),
            }
        }
    }
}

/// The kernel links no dentry and takes no lookup reference for inode zero.
fn name_only_entry_param(attr: &stat) -> raw::FuseEntryParam {
    raw::FuseEntryParam {
        ino: 0,
        ..entry_param(*attr, 0.0)
    }
}

struct DirectoryEntryBuffer {
    // Keep Vec length zero; libfuse initializes bytes tracked by `len` in spare storage.
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
        // SAFETY: libfuse's `fuse_add_direntry` meets the `push_with` contract;
        // attr is valid and libfuse ignores req.
        unsafe {
            self.push_with(name, |buf, available, name| {
                raw::fuse_add_direntry(req, buf, available, name, attr, next_offset)
            })
        }
    }

    /// Encodes a `readdirplus` entry; a zero `entry.ino` creates no kernel dentry.
    fn push_plus(
        &mut self,
        req: *mut raw::FuseReq,
        name: &OsStr,
        entry: &raw::FuseEntryParam,
        next_offset: off_t,
    ) -> Result<bool, i32> {
        // SAFETY: libfuse's `fuse_add_direntry_plus` meets the `push_with`
        // contract; entry is valid and libfuse ignores req.
        unsafe {
            self.push_with(name, |buf, available, name| {
                raw::fuse_add_direntry_plus(req, buf, available, name, entry, next_offset)
            })
        }
    }

    /// Encodes a dirent that never transfers a kernel lookup reference.
    fn push_name_only(
        &mut self,
        req: *mut raw::FuseReq,
        encoding: DirectoryEntryEncoding,
        name: &OsStr,
        attr: &stat,
        next_offset: off_t,
    ) -> Result<bool, i32> {
        match encoding {
            DirectoryEntryEncoding::Plain => self.push(req, name, attr, next_offset),
            DirectoryEntryEncoding::Plus => {
                self.push_plus(req, name, &name_only_entry_param(attr), next_offset)
            }
        }
    }

    /// `add` receives an aligned buffer writable for `available` bytes and a
    /// NUL-terminated name.
    ///
    /// # Safety
    /// Like libfuse, `add` must return the entry's required size and, when that
    /// size fits in `available`, initialize exactly that many bytes.
    unsafe fn push_with(
        &mut self,
        name: &OsStr,
        add: impl FnOnce(*mut c_char, size_t, *const c_char) -> size_t,
    ) -> Result<bool, i32> {
        let name_bytes = name.as_bytes();
        if name_bytes.contains(&0) {
            return Err(libc::EINVAL);
        }
        let available = self.capacity.saturating_sub(self.len);
        if available == 0 {
            return Ok(false);
        }
        self.scratch_name.clear();
        self.scratch_name.extend_from_slice(name_bytes);
        self.scratch_name.push(0);
        let start = self.len;
        // SAFETY: words has `available` writable bytes at `start`, aligned for libfuse.
        // Libfuse writes only if the entry fits, initializing the entry and padding.
        let buf = unsafe {
            self.words
                .as_mut_ptr()
                .cast::<u8>()
                .add(start)
                .cast::<c_char>()
        };
        let required = add(buf, available, self.scratch_name.as_ptr().cast::<c_char>());
        if required > available {
            return Ok(false);
        }

        self.len = start + required;
        Ok(true)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(sampler) = self
            .state
            .as_ref()
            .and_then(|state| state.available_memory.as_ref())
        {
            // Callback state can be retained after timeout, so stop its worker independently.
            sampler.shutdown();
        }
        let writes_drained = self.state.as_ref().is_none_or(|state| {
            let drained = shutdown_async_writes(&state.async_writes);
            if !drained {
                state.async_writes.record_shutdown_timeout();
            }
            state.vfs.detach_fuse_async_write_snapshot_source(
                state.async_write_source,
                async_write_metrics_snapshot(
                    &state.async_writes,
                    state.cache_policy,
                    linux_fuse_async_write_enabled(),
                ),
            );
            drained
        });
        // Draining must not wait behind a root primer that never got served.
        self.invalidation_state.mark_root_primed();
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
                for signal in FUSE_EXIT_SIGNALS {
                    // SAFETY: restores libfuse's own handler so its removal
                    // recognizes it; a no-op for signals that were not wrapped.
                    unsafe { studiofs_linux_fuse_unwrap_exit_signal(signal) };
                }
                // SAFETY: handlers were installed for this retained live session.
                unsafe { raw::fuse_remove_signal_handlers(raw.as_ptr()) };
                // libfuse restores SIG_DFL, but `studiofs umount` sends SIGTERM after
                // the lazy detach; dying here skips VFS/redb shutdown and forces repair.
                // SIGINT keeps its default so a second Ctrl+C can still force-quit.
                // SAFETY: setting a disposition to SIG_IGN is async-signal-safe.
                unsafe {
                    libc::signal(libc::SIGTERM, libc::SIG_IGN);
                    libc::signal(libc::SIGHUP, libc::SIG_IGN);
                }
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

fn register_async_write_metrics(
    vfs: &StudioVfs,
    ledger: &Arc<AsyncWriteLedger>,
    cache_policy: LinuxFuseCachePolicy,
) -> u64 {
    let ledger = Arc::downgrade(ledger);
    let enabled = linux_fuse_async_write_enabled();
    vfs.set_fuse_async_write_snapshot_source(move || {
        ledger
            .upgrade()
            .map(|ledger| async_write_metrics_snapshot(&ledger, cache_policy, enabled))
    })
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
        barrier_timeouts,
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
        barrier_timeouts,
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

    static EXIT_SIGNAL_TEST_HITS: AtomicUsize = AtomicUsize::new(0);

    extern "C" fn record_exit_signal_for_test(_signal: libc::c_int) {
        EXIT_SIGNAL_TEST_HITS.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn exit_signal_on_another_thread_wakes_the_session_loop_thread() {
        // SIGUSR2 stands in for SIGINT so the test never touches the harness's
        // own interrupt handling; the shim wraps any plain handler the same way.
        let signal = libc::SIGUSR2;
        // SAFETY: plain sigaction calls on a zeroed, then kernel-filled struct.
        let previous = unsafe {
            let mut previous: libc::sigaction = std::mem::zeroed();
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = record_exit_signal_for_test as *const () as libc::sighandler_t;
            libc::sigemptyset(&mut action.sa_mask);
            assert_eq!(libc::sigaction(signal, &action, &mut previous), 0);
            previous
        };
        // SAFETY: the current handler is the plain test handler installed above.
        assert_eq!(
            unsafe { super::studiofs_linux_fuse_wrap_exit_signal(signal) },
            1
        );

        let (ready_tx, ready_rx) = mpsc::channel();
        let (woke_tx, woke_rx) = mpsc::channel();
        let loop_thread = thread::spawn(move || {
            // Model the libfuse loop: block the signal, then wait with
            // sigsuspend until the exit flag is set, without a lost wakeup.
            // SAFETY: per-thread signal mask manipulation on local sets.
            unsafe {
                let mut blocked: libc::sigset_t = std::mem::zeroed();
                let mut waiting: libc::sigset_t = std::mem::zeroed();
                libc::sigemptyset(&mut blocked);
                libc::sigaddset(&mut blocked, signal);
                libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, &mut waiting);
                libc::sigdelset(&mut waiting, signal);
                super::studiofs_linux_fuse_enter_loop_thread();
                ready_tx.send(()).expect("test receiver remains alive");
                while EXIT_SIGNAL_TEST_HITS.load(Ordering::SeqCst) == 0 {
                    libc::sigsuspend(&waiting);
                }
                super::studiofs_linux_fuse_leave_loop_thread();
            }
            let _ = woke_tx.send(());
        });
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("loop thread registers itself");

        // Deliver the signal to this thread, as the kernel does when it picks
        // the main thread for a process-directed Ctrl+C.
        // SAFETY: raise targets the calling thread, which leaves it unblocked.
        assert_eq!(unsafe { libc::raise(signal) }, 0);
        let woke = woke_rx.recv_timeout(Duration::from_secs(5));

        // SAFETY: restores the wrapped handler, then the original disposition.
        let restored_handler = unsafe {
            super::studiofs_linux_fuse_unwrap_exit_signal(signal);
            let mut current: libc::sigaction = std::mem::zeroed();
            libc::sigaction(signal, std::ptr::null(), &mut current);
            libc::sigaction(signal, &previous, std::ptr::null_mut());
            current.sa_sigaction
        };
        woke.expect("an exit signal on another thread must wake the loop thread");
        loop_thread.join().expect("loop thread must not panic");
        assert!(EXIT_SIGNAL_TEST_HITS.load(Ordering::SeqCst) >= 1);
        assert_eq!(
            restored_handler, record_exit_signal_for_test as *const () as libc::sighandler_t,
            "unwrap must reinstall the original handler so libfuse can remove it"
        );
    }

    fn decode_directory_reply(buffer: &super::DirectoryEntryBuffer) -> Vec<(Vec<u8>, u64, u32)> {
        let mut bytes = buffer.as_slice();
        let mut entries = Vec::new();
        while !bytes.is_empty() {
            let offset = u64::from_ne_bytes(bytes[8..16].try_into().unwrap());
            let name_len = u32::from_ne_bytes(bytes[16..20].try_into().unwrap()) as usize;
            let kind = u32::from_ne_bytes(bytes[20..24].try_into().unwrap());
            entries.push((bytes[24..24 + name_len].to_vec(), offset, kind));
            bytes = &bytes[(24 + name_len).next_multiple_of(8)..];
        }
        entries
    }

    #[test]
    fn directory_offsets_stay_stable_across_chunks_and_namespace_mutations() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-offsets")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let dir = fixture.vfs().mkdir(1, b"dir", 0, 0, 0o755).unwrap();
        for name in [b"a", b"b", b"c", b"d"] {
            fixture.vfs().create(dir.ino, name, 0, 0, 0o644).unwrap();
        }
        let raw_handle = state.open_dir(dir.ino, 1).unwrap();
        // SAFETY: this test owns the handle and keeps the box alive through every read.
        let handle = unsafe { Box::from_raw(raw_handle.as_ptr()) };
        fixture.vfs().mkdir(dir.ino, b"e", 0, 0, 0o755).unwrap();
        let read = |offset, size| {
            let mut buffer = state.directory_buffers.take(size);
            state
                .readdir(
                    std::ptr::null_mut(),
                    dir.ino,
                    raw_handle,
                    offset,
                    &mut buffer,
                )
                .unwrap();
            let result = decode_directory_reply(&buffer);
            state.directory_buffers.put(buffer);
            result
        };
        let first = read(0, 96);
        assert_eq!(
            first,
            vec![
                (b".".to_vec(), 1, u32::from(libc::DT_DIR)),
                (b"..".to_vec(), 2, u32::from(libc::DT_DIR)),
                (b"a".to_vec(), 3, u32::from(libc::DT_REG)),
            ]
        );

        std::thread::scope(|scope| {
            let vfs = fixture.vfs();
            scope
                .spawn(move || {
                    vfs.unlink(dir.ino, b"b").unwrap();
                    vfs.rename(dir.ino, b"c", dir.ino, b"z", false).unwrap();
                    vfs.create(dir.ino, b"aa", 0, 0, 0o644).unwrap();
                })
                .join()
                .unwrap();
        });
        assert_eq!(
            read(3, 64),
            vec![
                (b"b".to_vec(), 4, u32::from(libc::DT_REG)),
                (b"c".to_vec(), 5, u32::from(libc::DT_REG)),
            ]
        );
        assert_eq!(
            read(5, 64),
            vec![
                (b"d".to_vec(), 6, u32::from(libc::DT_REG)),
                (b"e".to_vec(), 7, u32::from(libc::DT_DIR)),
            ]
        );
        assert!(read(7, 64).is_empty());
        assert_eq!(
            read(3, 32),
            vec![(b"b".to_vec(), 4, u32::from(libc::DT_REG))],
            "seeking back within an enumeration must replay the original offsets"
        );
        assert!(read(i64::MAX, 64).is_empty());
        assert_eq!(read(0, 96), first);
        assert_eq!(
            read(3, 32),
            vec![(b"aa".to_vec(), 4, u32::from(libc::DT_REG))],
            "rewinding to offset 0 must start a fresh enumeration, as on WinFSP"
        );

        let fresh = state.open_dir(dir.ino, 1).unwrap();
        let mut buffer = state.directory_buffers.take(4096);
        state
            .readdir(std::ptr::null_mut(), dir.ino, fresh, 2, &mut buffer)
            .unwrap();
        // SAFETY: this test owns this second handle, with no further reads pending.
        let _fresh = unsafe { Box::from_raw(fresh.as_ptr()) };
        assert_eq!(
            decode_directory_reply(&buffer)
                .into_iter()
                .map(|e| e.0)
                .collect::<Vec<_>>(),
            vec![
                b"a".to_vec(),
                b"aa".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
                b"z".to_vec()
            ]
        );
        drop(handle);
    }

    #[test]
    fn directory_snapshot_is_shared_until_namespace_changes() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-shared")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let dir = fixture.vfs().mkdir(1, b"dir", 0, 0, 0o755).unwrap();
        fixture.vfs().create(dir.ino, b"a", 0, 0, 0o644).unwrap();
        let capture = || {
            let raw = state.open_dir(dir.ino, 1).unwrap();
            let mut buffer = state.directory_buffers.take(4096);
            state
                .readdir(std::ptr::null_mut(), dir.ino, raw, 2, &mut buffer)
                .unwrap();
            state.directory_buffers.put(buffer);
            // SAFETY: this test exclusively owns the handle returned by open_dir.
            let handle = unsafe { Box::from_raw(raw.as_ptr()) };
            handle
                .entries
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .entries
                .clone()
        };
        let first = capture();
        let second = capture();
        assert!(
            std::sync::Arc::ptr_eq(&first, &second),
            "an unchanged directory reuses the session snapshot"
        );
        fixture
            .vfs()
            .setattr(
                first[0].ino,
                studiofs_meta::SetattrArgs {
                    mtime: studiofs_meta::TimeSpec::Now,
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            std::sync::Arc::ptr_eq(&first, &capture()),
            "attribute-only invalidation keeps Linux dirents valid"
        );
        fixture.vfs().create(dir.ino, b"b", 0, 0, 0o644).unwrap();
        let third = capture();
        assert!(!std::sync::Arc::ptr_eq(&first, &third));
        assert_eq!(
            third
                .iter()
                .map(|entry| entry.name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"a".as_slice(), b"b".as_slice()]
        );
        assert_eq!(first.len(), 1, "an existing snapshot stays immutable");
        assert!(std::sync::Arc::ptr_eq(&third, &capture()));
    }

    #[test]
    fn directory_empty_and_invalid_reads_do_not_capture_snapshot() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-empty")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        fixture.vfs().create(1, b"child", 0, 0, 0o644).unwrap();
        let raw_handle = state.open_dir(1, 1).unwrap();
        // SAFETY: this test keeps its handle alive until all reads complete.
        let handle = unsafe { Box::from_raw(raw_handle.as_ptr()) };
        for (offset, size, expected) in [
            (0, 0, Ok(())),
            (2, 0, Ok(())),
            (0, 31, Ok(())),
            (2, 31, Ok(())),
            (-1, 4096, Err(libc::EINVAL)),
        ] {
            let mut buffer = state.directory_buffers.take(size);
            assert_eq!(
                state.readdir(std::ptr::null_mut(), 1, raw_handle, offset, &mut buffer),
                expected
            );
            assert!(buffer.as_slice().is_empty());
            state.directory_buffers.put(buffer);
            assert!(handle.entries.lock().unwrap().is_none());
        }
        for (offset, sizes, names) in [
            (0, 64..96, vec![b".".to_vec(), b"..".to_vec()]),
            (1, 32..64, vec![b"..".to_vec()]),
        ] {
            for size in sizes {
                let mut buffer = state.directory_buffers.take(size);
                state
                    .readdir(std::ptr::null_mut(), 1, raw_handle, offset, &mut buffer)
                    .unwrap();
                assert_eq!(
                    decode_directory_reply(&buffer)
                        .into_iter()
                        .map(|entry| entry.0)
                        .collect::<Vec<_>>(),
                    names
                );
                state.directory_buffers.put(buffer);
                assert!(
                    handle.entries.lock().unwrap().is_none(),
                    "offset {offset}, size {size} must not capture children"
                );
            }
        }
    }

    /// Decodes `(nodeid, dirent ino, offset, name)` from a `readdirplus` reply.
    fn decode_directory_plus_reply(
        buffer: &super::DirectoryEntryBuffer,
    ) -> Vec<(u64, u64, u64, Vec<u8>)> {
        // fuse_entry_out is 128 bytes; fuse_dirent follows it.
        let mut bytes = buffer.as_slice();
        let mut entries = Vec::new();
        while !bytes.is_empty() {
            let word = |at: usize| u64::from_ne_bytes(bytes[at..at + 8].try_into().unwrap());
            let name_len = u32::from_ne_bytes(bytes[144..148].try_into().unwrap()) as usize;
            entries.push((
                word(0),
                word(128),
                word(136),
                bytes[152..152 + name_len].to_vec(),
            ));
            bytes = &bytes[(152 + name_len).next_multiple_of(8)..];
        }
        entries
    }

    #[test]
    fn readdirplus_replies_entries_and_reserves_one_lookup_per_child() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dirplus")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let dir = fixture.vfs().mkdir(1, b"dir", 0, 0, 0o755).unwrap();
        let a = fixture.vfs().create(dir.ino, b"a", 0, 0, 0o644).unwrap();
        let b = fixture.vfs().mkdir(dir.ino, b"b", 0, 0, 0o755).unwrap();
        let raw_handle = state.open_dir(dir.ino, 1).unwrap();
        // SAFETY: this test owns the handle and keeps the box alive through every read.
        let handle = unsafe { Box::from_raw(raw_handle.as_ptr()) };
        let mut buffer = state.directory_buffers.take(4096);
        let reservations = state
            .readdirplus(std::ptr::null_mut(), dir.ino, raw_handle, 0, &mut buffer)
            .unwrap();
        assert_eq!(
            decode_directory_plus_reply(&buffer),
            vec![
                (0, dir.ino, 1, b".".to_vec()),
                (0, 1, 2, b"..".to_vec()),
                (a.ino, a.ino, 3, b"a".to_vec()),
                (b.ino, b.ino, 4, b"b".to_vec()),
            ],
            "dot entries must not transfer lookup references"
        );
        assert_eq!(
            reservations.iter().map(|(ino, _)| *ino).collect::<Vec<_>>(),
            vec![a.ino, b.ino]
        );
        assert_eq!(
            super::reply_reserved_entries_with(&state, reservations, || 0),
            0
        );
        assert!(state.inode_liveness.is_live(a.ino));
        assert!(state.inode_liveness.is_live(b.ino));
        state.directory_buffers.put(buffer);
        drop(handle);
    }

    #[test]
    fn readdirplus_rejected_reply_rolls_back_every_reservation() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dirplus-reject")
                .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let a = fixture.vfs().create(1, b"a", 0, 0, 0o644).unwrap();
        let raw_handle = state.open_dir(1, 1).unwrap();
        // SAFETY: this test owns the handle and keeps the box alive through the read.
        let handle = unsafe { Box::from_raw(raw_handle.as_ptr()) };
        let mut buffer = state.directory_buffers.take(4096);
        let reservations = state
            .readdirplus(std::ptr::null_mut(), 1, raw_handle, 2, &mut buffer)
            .unwrap();
        assert_eq!(reservations.len(), 1);
        assert_eq!(
            super::reply_reserved_entries_with(&state, reservations, || -libc::ENOENT),
            -libc::ENOENT
        );
        assert!(
            !state.inode_liveness.is_live(a.ino),
            "a rejected reply must not leave a lookup reference"
        );
        drop(handle);
    }

    #[test]
    fn readdirplus_after_namespace_change_replies_names_without_entries() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dirplus-stale")
                .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let dir = fixture.vfs().mkdir(1, b"dir", 0, 0, 0o755).unwrap();
        let a = fixture.vfs().create(dir.ino, b"a", 0, 0, 0o644).unwrap();
        let b = fixture.vfs().create(dir.ino, b"b", 0, 0, 0o644).unwrap();
        let raw_handle = state.open_dir(dir.ino, 1).unwrap();
        // SAFETY: this test owns the handle and keeps the box alive through every read.
        let handle = unsafe { Box::from_raw(raw_handle.as_ptr()) };
        let read = |offset| {
            let mut buffer = state.directory_buffers.take(4096);
            let reservations = state
                .readdirplus(
                    std::ptr::null_mut(),
                    dir.ino,
                    raw_handle,
                    offset,
                    &mut buffer,
                )
                .unwrap();
            let entries = decode_directory_plus_reply(&buffer);
            state.directory_buffers.put(buffer);
            (entries, reservations.len())
        };
        // A plain readdir captures the snapshot before the rename.
        let mut buffer = state.directory_buffers.take(4096);
        state
            .readdir(std::ptr::null_mut(), dir.ino, raw_handle, 0, &mut buffer)
            .unwrap();
        state.directory_buffers.put(buffer);
        fixture
            .vfs()
            .rename(dir.ino, b"b", dir.ino, b"renamed", false)
            .unwrap();
        assert_eq!(
            read(3),
            (vec![(0, b.ino, 4, b"b".to_vec())], 0),
            "a stale snapshot name must not become a kernel dentry"
        );
        let (fresh, reserved) = read(0);
        assert_eq!(reserved, 2, "a rewind captures a current snapshot");
        assert_eq!(
            fresh[2..],
            [
                (a.ino, a.ino, 3, b"a".to_vec()),
                (b.ino, b.ino, 4, b"renamed".to_vec()),
            ]
        );
        drop(handle);
    }

    #[test]
    fn directory_dot_dot_reports_parent_inode() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-dotdot")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let parent = fixture.vfs().mkdir(1, b"parent", 0, 0, 0o755).unwrap();
        let child = fixture
            .vfs()
            .mkdir(parent.ino, b"child", 0, 0, 0o755)
            .unwrap();
        for (ino, expected_parent) in [(child.ino, parent.ino), (1, 1)] {
            // Same attr source as the opendir callback.
            let attr = state.getattr_attr(ino).unwrap();
            let raw_handle = state.open_dir(ino, attr.parent).unwrap();
            let dot_inodes = |offset| {
                let mut buffer = state.directory_buffers.take(4096);
                state
                    .readdir(std::ptr::null_mut(), ino, raw_handle, offset, &mut buffer)
                    .unwrap();
                let mut bytes = buffer.as_slice();
                let mut inodes = Vec::new();
                while !bytes.is_empty() {
                    let name_len = u32::from_ne_bytes(bytes[16..20].try_into().unwrap()) as usize;
                    let name = &bytes[24..24 + name_len];
                    if name == b"." || name == b".." {
                        inodes.push((
                            name.to_vec(),
                            u64::from_ne_bytes(bytes[..8].try_into().unwrap()),
                        ));
                    }
                    bytes = &bytes[(24 + name_len).next_multiple_of(8)..];
                }
                state.directory_buffers.put(buffer);
                inodes
            };
            assert_eq!(
                dot_inodes(0),
                vec![(b".".to_vec(), ino), (b"..".to_vec(), expected_parent)]
            );
            assert_eq!(dot_inodes(1), vec![(b"..".to_vec(), expected_parent)]);
            // SAFETY: this test exclusively owns the handle returned by open_dir.
            drop(unsafe { Box::from_raw(raw_handle.as_ptr()) });
        }
    }

    #[test]
    fn directory_buffer_pool_reuses_storage_without_stale_bytes() {
        let pool = super::DirectoryEntryBufferPool::default();
        let mut buffer = pool.take(4096);
        assert!(
            buffer
                .push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::new("old"),
                    &super::minimal_dir_stat(1),
                    1
                )
                .unwrap()
        );
        let words = buffer.words.as_ptr();
        let scratch = buffer.scratch_name.as_ptr();
        pool.put(buffer);
        let mut buffer = pool.take(32);
        assert_eq!(buffer.words.as_ptr(), words);
        assert_eq!(buffer.scratch_name.as_ptr(), scratch);
        assert!(buffer.as_slice().is_empty());
        assert!(buffer.scratch_name.is_empty());
        assert!(
            buffer
                .push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::new("x"),
                    &super::minimal_dir_stat(1),
                    9
                )
                .unwrap()
        );
        assert_eq!(
            decode_directory_reply(&buffer),
            vec![(b"x".to_vec(), 9, u32::from(libc::DT_DIR))]
        );
        assert!(
            !buffer
                .push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::new("y"),
                    &super::minimal_dir_stat(1),
                    10
                )
                .unwrap()
        );
        pool.put(buffer);
        let mut buffer = pool.take(31);
        assert!(
            !buffer
                .push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::new("x"),
                    &super::minimal_dir_stat(1),
                    9
                )
                .unwrap()
        );
        assert!(
            buffer.as_slice().is_empty(),
            "allocated capacity must not override requested size"
        );
        pool.put(buffer);
        let mut buffer = pool.take(8192);
        assert!(buffer.words.capacity() * 8 >= 8192);
        assert_eq!(
            buffer.push(
                std::ptr::null_mut(),
                std::ffi::OsStr::from_bytes(b"bad\0name"),
                &super::minimal_dir_stat(1),
                1
            ),
            Err(libc::EINVAL)
        );
        let words = buffer.words.as_ptr();
        pool.put(buffer);
        assert_eq!(
            pool.take(4096).words.as_ptr(),
            words,
            "errors must not prevent reuse"
        );
    }

    #[test]
    fn directory_buffer_pool_reuses_storage_grown_to_retention_limit() {
        let pool = super::DirectoryEntryBufferPool::default();
        pool.put(pool.take(96 * 1024));
        let buffer = pool.take(super::MAX_DIRECTORY_BUFFER_CAPACITY);
        assert!(buffer.words.capacity() * 8 >= super::MAX_DIRECTORY_BUFFER_CAPACITY);
        let words = buffer.words.as_ptr();
        pool.put(buffer);
        assert_eq!(pool.buffers.lock().unwrap().len(), 1);
        assert_eq!(
            pool.take(super::MAX_DIRECTORY_BUFFER_CAPACITY)
                .words
                .as_ptr(),
            words,
            "storage grown to the limit must be recycled"
        );
    }

    #[test]
    fn directory_buffer_pool_bounds_count_and_retained_capacities() {
        let pool = super::DirectoryEntryBufferPool::default();
        let buffers = (0..super::MAX_DIRECTORY_BUFFERS + 3)
            .map(|_| pool.take(4096))
            .collect::<Vec<_>>();
        for buffer in buffers {
            pool.put(buffer);
        }
        assert_eq!(
            pool.buffers.lock().unwrap().len(),
            super::MAX_DIRECTORY_BUFFERS
        );
        let _held = (0..super::MAX_DIRECTORY_BUFFERS)
            .map(|_| pool.take(4096))
            .collect::<Vec<_>>();
        pool.put(pool.take(super::MAX_DIRECTORY_BUFFER_CAPACITY + 1));
        assert!(
            pool.buffers.lock().unwrap().is_empty(),
            "oversized reply storage must be discarded"
        );
        let mut buffer = pool.take(64);
        let name = vec![b'x'; super::MAX_DIRECTORY_SCRATCH_CAPACITY + 1];
        assert!(
            !buffer
                .push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::from_bytes(&name),
                    &super::minimal_dir_stat(1),
                    1
                )
                .unwrap()
        );
        pool.put(buffer);
        assert!(
            pool.buffers.lock().unwrap().is_empty(),
            "oversized scratch storage must be discarded"
        );
    }

    #[test]
    fn directory_open_does_not_materialize_unused_entries() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-lazy")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        fixture.vfs().create(1, b"child", 0, 0, 0o644).unwrap();
        let handle = state.open_dir(1, 1).unwrap();
        // SAFETY: this test exclusively owns the handle returned by open_dir.
        let handle = unsafe { Box::from_raw(handle.as_ptr()) };
        assert!(
            handle.entries.lock().unwrap().is_none(),
            "an unused handle must not enumerate"
        );
    }

    #[test]
    fn directory_large_snapshot_retains_only_dirent_fields() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-dir-compact")
            .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let dir = fixture.vfs().mkdir(1, b"large", 0, 0, 0o755).unwrap();
        for index in 0..1024 {
            fixture
                .vfs()
                .create(dir.ino, format!("file-{index:04}").as_bytes(), 0, 0, 0o644)
                .unwrap();
        }
        let handle = state.open_dir(dir.ino, 1).unwrap();
        let mut reply = state.directory_buffers.take(4096);
        state
            .readdir(std::ptr::null_mut(), dir.ino, handle, 2, &mut reply)
            .unwrap();
        assert!(!reply.as_slice().is_empty());
        // SAFETY: this test exclusively owns the handle returned by open_dir.
        let handle = unsafe { Box::from_raw(handle.as_ptr()) };
        let snapshot = handle.entries.lock().unwrap();
        let entries = &snapshot.as_ref().unwrap().entries[..];
        assert_eq!(entries.len(), 1024);
        let names: usize = entries.iter().map(|entry| entry.name.capacity()).sum();
        let retained = crate::directory_cache::snapshot_byte_size(entries);
        let previous =
            entries.len() * std::mem::size_of::<studiofs_vfs::DirEntryWithAttr>() + names;
        eprintln!("directory retained bytes: {retained}; old snapshot bytes: {previous}");
        assert!(
            retained * 3 < previous,
            "retain less than one third of the old snapshot"
        );
    }

    #[test]
    fn directory_entry_buffer_encodes_once_per_entry() {
        let mut buffer = super::DirectoryEntryBuffer::new(4096);
        let attr = super::minimal_dir_stat(42);
        super::raw::DIRENTRY_CALLS.with(|calls| calls.set(0));
        for (index, name) in [".", "..", "file"].into_iter().enumerate() {
            assert!(
                buffer
                    .push(
                        std::ptr::null_mut(),
                        std::ffi::OsStr::new(name),
                        &attr,
                        (index + 1) as libc::off_t
                    )
                    .unwrap()
            );
        }
        super::raw::DIRENTRY_CALLS.with(|calls| {
            assert_eq!(
                calls.get(),
                3,
                "each entry must cross the libfuse boundary once"
            );
        });
    }

    #[test]
    fn directory_entry_buffer_matches_libfuse_at_capacity_boundaries() {
        let attr = super::minimal_dir_stat(42);
        for name_len in [
            0, 1, 2, 7, 8, 9, 15, 16, 17, 63, 64, 65, 254, 255, 256, 4096,
        ] {
            let name = vec![0xff; name_len];
            let c_name = std::ffi::CString::new(name.clone()).unwrap();
            // SAFETY: libfuse ignores req; a null buffer queries size without writing.
            let required = unsafe {
                super::raw::libfuse_add_direntry(
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    0,
                    c_name.as_ptr(),
                    &attr,
                    7,
                )
            };
            let mut expected = vec![0u64; required.div_ceil(8)];
            // SAFETY: expected is aligned and has at least required writable bytes.
            unsafe {
                super::raw::libfuse_add_direntry(
                    std::ptr::null_mut(),
                    expected.as_mut_ptr().cast(),
                    required,
                    c_name.as_ptr(),
                    &attr,
                    7,
                );
            }
            // SAFETY: libfuse initialized required bytes above.
            let expected =
                unsafe { std::slice::from_raw_parts(expected.as_ptr().cast::<u8>(), required) };
            for capacity in [0, required - 1, required, required + 1, required * 2] {
                let mut buffer = super::DirectoryEntryBuffer::new(capacity);
                super::raw::DIRENTRY_CALLS.with(|calls| calls.set(0));
                let accepted = buffer
                    .push(
                        std::ptr::null_mut(),
                        std::ffi::OsStr::from_bytes(&name),
                        &attr,
                        7,
                    )
                    .unwrap();
                super::raw::DIRENTRY_CALLS.with(|calls| {
                    assert_eq!(calls.get(), usize::from(capacity != 0));
                });
                if capacity == 0 {
                    assert!(buffer.scratch_name.is_empty());
                }
                assert_eq!(
                    accepted,
                    capacity >= required,
                    "name length {name_len}, capacity {capacity}"
                );
                assert_eq!(buffer.as_slice(), if accepted { expected } else { &[] });
                if accepted {
                    let before = buffer.as_slice().to_vec();
                    super::raw::DIRENTRY_CALLS.with(|calls| calls.set(0));
                    let accepted_again = buffer
                        .push(
                            std::ptr::null_mut(),
                            std::ffi::OsStr::from_bytes(&name),
                            &attr,
                            7,
                        )
                        .unwrap();
                    super::raw::DIRENTRY_CALLS.with(|calls| {
                        assert_eq!(calls.get(), usize::from(capacity != required));
                    });
                    assert_eq!(accepted_again, capacity >= required * 2);
                    if accepted_again {
                        assert_eq!(buffer.as_slice(), [before.as_slice(), expected].concat());
                    } else {
                        assert_eq!(
                            buffer.as_slice(),
                            before,
                            "a rejected entry must preserve prior entries"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn directory_entry_buffer_rejects_nul_without_calling_libfuse() {
        let attr = super::minimal_dir_stat(42);
        super::raw::DIRENTRY_CALLS.with(|calls| calls.set(0));
        for capacity in [0, 4096] {
            let mut buffer = super::DirectoryEntryBuffer::new(capacity);
            assert_eq!(
                buffer.push(
                    std::ptr::null_mut(),
                    std::ffi::OsStr::from_bytes(b"bad\0name"),
                    &attr,
                    1
                ),
                Err(libc::EINVAL)
            );
            assert!(buffer.as_slice().is_empty());
        }
        super::raw::DIRENTRY_CALLS.with(|calls| assert_eq!(calls.get(), 0));
    }

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
        let args = super::argv_strings("demo", false);

        assert!(args.iter().any(|arg| arg == "studiofs-linux-fuse"));
        assert!(args.iter().any(|arg| arg == "fsname=demo"));
        assert!(args.iter().any(|arg| arg == "subtype=studiofs"));
        assert!(args.iter().any(|arg| arg == "auto_unmount"));
        assert!(args.iter().any(|arg| arg == "default_permissions"));
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
    fn read_only_mount_arguments_enable_native_read_only_policy() {
        assert!(
            super::argv_strings("demo", true)
                .iter()
                .any(|arg| arg == "ro")
        );
        assert!(
            !super::argv_strings("demo", false)
                .iter()
                .any(|arg| arg == "ro")
        );
    }

    #[test]
    fn generated_argv_is_accepted_by_low_level_session_parser() {
        let mut args = super::OwnedFuseArgs::new(super::argv_strings("demo", false))
            .expect("build FUSE arguments");
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
    fn async_write_ledger_metrics_are_collected_on_demand() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-stats")
                .expect("create VFS fixture");
        let ledger = Arc::new(
            crate::async_writes::AsyncWriteLedger::with_limits(16, 4)
                .with_barrier_timeout(Duration::ZERO),
        );
        let ticket = ledger.admit(10, 100, 8).expect("write should admit");
        assert_eq!(
            ledger.wait_inode(ledger.capture_inode_watermark(10)),
            crate::async_writes::BarrierOutcome::TimedOut
        );

        super::register_async_write_metrics(
            &fixture.vfs(),
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        let pending = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(pending.fuse_async_write.pending_writes, 1);
        assert_eq!(pending.fuse_async_write.barrier_timeouts, 1);
        assert_eq!(pending.fuse_async_write.pending_handles, 1);
        assert_eq!(
            pending.fuse_async_write.pending_writes_per_inode.get(&10),
            Some(&1)
        );

        ticket.complete(Err(libc::ENOSPC));
        let completed = fixture.vfs().write_path_observability_snapshot();
        assert_eq!(completed.fuse_async_write.pending_writes, 0);
        assert_eq!(completed.fuse_async_write.completed_writes, 1);
        assert_eq!(completed.fuse_async_write.failed_writes, 1);
    }

    #[test]
    fn async_write_metrics_reset_samples_unread_counters_and_preserves_pending_gauges() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-reset")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        super::register_async_write_metrics(
            &vfs,
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        ledger.admit(10, 100, 4).unwrap().complete(Err(libc::EIO));
        let ticket = ledger.admit(20, 200, 8).unwrap();

        vfs.reset_diagnostics_observability();
        let reset = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(reset.completed_writes, 0);
        assert_eq!(reset.failed_writes, 0);
        assert_eq!(reset.pending_writes, 1);
        assert_eq!(reset.pending_bytes, 8);
        assert_eq!(reset.pending_handles, 1);
        assert_eq!(reset.pending_writes_per_inode.get(&20), Some(&1));

        ticket.complete(Err(libc::ENOSPC));
        let completed = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(completed.completed_writes, 1);
        assert_eq!(completed.failed_writes, 1);
        assert!(completed.pending_writes_per_inode.is_empty());
        vfs.reset_diagnostics_observability();
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .fuse_async_write
                .completed_writes,
            0
        );
    }

    #[test]
    fn async_write_metrics_source_does_not_retain_ledger_and_keeps_final_snapshot() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-final-stats")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let weak = Arc::downgrade(&ledger);
        super::register_async_write_metrics(
            &vfs,
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        ledger.admit(10, 100, 8).unwrap().complete(Ok(()));
        vfs.record_fuse_async_write_snapshot(super::async_write_metrics_snapshot(
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
            true,
        ));
        drop(ledger);

        assert!(weak.upgrade().is_none());
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .fuse_async_write
                .completed_writes,
            1
        );
    }

    #[test]
    fn stale_async_write_source_cannot_detach_or_poison_a_newer_one() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-stale")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;
        let old = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        let old_id = super::register_async_write_metrics(&vfs, &old, policy);
        for _ in 0..3 {
            old.admit(10, 100, 8).unwrap().complete(Ok(()));
        }
        vfs.reset_diagnostics_observability();
        old.admit(10, 100, 8).unwrap().complete(Ok(()));

        let new = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        super::register_async_write_metrics(&vfs, &new, policy);
        new.admit(20, 200, 8).unwrap().complete(Ok(()));
        // The old session tears down after the new one registered.
        vfs.detach_fuse_async_write_snapshot_source(
            old_id,
            super::async_write_metrics_snapshot(&old, policy, true),
        );

        new.admit(20, 200, 8).unwrap().complete(Ok(()));
        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(
            snapshot.completed_writes, 2,
            "new ledger stays live and unbiased"
        );
    }

    #[test]
    fn timed_out_session_teardown_stops_sampler_before_retaining_state() {
        struct SampleOwner(std::sync::mpsc::Sender<()>);
        impl Drop for SampleOwner {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-retained-memory-sampler",
        )
        .expect("create fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let (exited_tx, exited_rx) = std::sync::mpsc::channel();
        let owner = SampleOwner(exited_tx);
        state.available_memory = Some(crate::cache_policy::SystemMemorySampler::start(move || {
            let _ = &owner;
            Some(4096)
        }));
        let pending = state.async_writes.admit(10, 100, 8).unwrap();
        let mut args = super::OwnedFuseArgs::new(super::argv_strings("demo", false)).unwrap();
        // SAFETY: the empty ops table has no callbacks; this test owns the raw session.
        let raw = std::ptr::NonNull::new(unsafe {
            super::studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                super::studiofs_linux_fuse_empty_low_level_ops(),
                super::studiofs_linux_fuse_low_level_ops_size(),
                std::ptr::null_mut(),
            )
        })
        .unwrap();
        let state = Box::into_raw(Box::new(state));
        drop(super::Session {
            raw,
            // SAFETY: transfer our unique allocation into Session until retention.
            state: Some(unsafe { Box::from_raw(state) }),
            invalidation_state: Arc::new(super::SessionInvalidationState::new()),
            invalidation_worker: None,
            invalidation_metrics_generation: Arc::new(super::AtomicU64::new(0)),
            signal_handlers_installed: false,
        });
        let stopped = exited_rx.recv_timeout(Duration::from_secs(2));
        pending.complete(Ok(()));
        // SAFETY: the pending write forced retention; it is now complete, and
        // the empty callback table and absent invalidation worker retain no pointers.
        unsafe {
            super::raw::fuse_session_destroy(raw.as_ptr());
            drop(Box::from_raw(state));
        }
        assert!(
            stopped.is_ok(),
            "retaining state must not retain the sampler thread"
        );
    }

    #[test]
    fn session_drop_detaches_async_write_metrics_and_keeps_final_snapshot() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-write-detach")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let state = super::SessionState::new(
            Arc::clone(&vfs),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let ledger = Arc::clone(&state.async_writes);
        ledger.admit(10, 100, 8).unwrap().complete(Ok(()));
        let mut args = super::OwnedFuseArgs::new(super::argv_strings("demo", false))
            .expect("build FUSE arguments");
        // SAFETY: the empty ops table has no callbacks; Session owns and destroys the raw session.
        let raw = std::ptr::NonNull::new(unsafe {
            super::studiofs_linux_fuse_session_new(
                args.as_mut_ptr(),
                super::studiofs_linux_fuse_empty_low_level_ops(),
                super::studiofs_linux_fuse_low_level_ops_size(),
                std::ptr::null_mut(),
            )
        })
        .expect("create unmounted FUSE session");
        drop(super::Session {
            raw,
            state: Some(Box::new(state)),
            invalidation_state: Arc::new(super::SessionInvalidationState::new()),
            invalidation_worker: None,
            invalidation_metrics_generation: Arc::new(super::AtomicU64::new(0)),
            signal_handlers_installed: false,
        });

        ledger.record_shutdown_timeout();
        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(snapshot.completed_writes, 1, "retain final counters");
        assert_eq!(
            snapshot.shutdown_timeouts, 0,
            "stop sampling the detached ledger"
        );
        vfs.reset_diagnostics_observability();
        assert_eq!(
            vfs.write_path_observability_snapshot()
                .fuse_async_write
                .shutdown_timeouts,
            0,
            "reset must use the retained final snapshot"
        );
    }

    #[test]
    fn async_write_metrics_reset_rejects_shutdown_snapshot_sampled_before_reset() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-reset-shutdown")
                .expect("create VFS fixture");
        let vfs = fixture.vfs();
        let ledger = Arc::new(crate::async_writes::AsyncWriteLedger::with_limits(16, 4));
        super::register_async_write_metrics(
            &vfs,
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
        );
        ledger.admit(10, 100, 8).unwrap().complete(Err(libc::EIO));
        let shutdown = super::async_write_metrics_snapshot(
            &ledger,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive,
            true,
        );
        vfs.reset_diagnostics_observability();
        vfs.record_fuse_async_write_snapshot(shutdown);
        drop(ledger);

        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(snapshot.completed_writes, 0);
        assert_eq!(snapshot.failed_writes, 0);
    }

    #[test]
    fn async_write_barriers_do_not_collect_diagnostic_snapshots() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-write-no-snapshots",
        )
        .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = state.vfs.create(1, b"write", 0, 0, 0o644).unwrap();
        let open = state.open_with_flags(attr.ino, libc::O_WRONLY).unwrap();
        state
            .async_writes
            .admit(attr.ino, open.fh, 8)
            .unwrap()
            .complete(Ok(()));
        state.flush(open.fh).unwrap();
        state.fsync(Some(open.fh), attr.ino, false).unwrap();
        state.release(open.fh).unwrap();

        let snapshot = state.async_writes.metrics_snapshot();
        assert_eq!(snapshot.snapshot_generation, 1);
        assert_eq!(snapshot.completed_writes, 1);
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
    fn kernel_barriers_recover_worker_from_incomplete_async_write() {
        for operation in ["link", "flush", "fsync", "release"] {
            let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(operation)
                .expect("create VFS fixture");
            let mut state = super::SessionState::new(
                fixture.vfs(),
                fixture.runtime().handle().clone(),
                false,
                default_connection_config(),
            );
            state.async_writes = Arc::new(
                crate::async_writes::AsyncWriteLedger::with_limits(
                    super::DEFAULT_MAX_PENDING_BYTES,
                    super::DEFAULT_MAX_PENDING_WRITES,
                )
                .with_barrier_timeout(Duration::from_millis(10)),
            );
            let attr = state
                .vfs
                .create(1, b"wedged-write", 0, 0, 0o644)
                .expect("create file");
            let open = state
                .open_with_flags(attr.ino, libc::O_WRONLY)
                .expect("open file");
            let state = Arc::new(state);
            let ticket = state
                .async_writes
                .admit(attr.ino, open.fh, 4)
                .expect("admit write");
            assert!(
                state
                    .async_writes
                    .mark_task_non_cancellable(ticket.sequence())
            );
            let (sender, receiver) = std::sync::mpsc::channel();
            let worker_state = Arc::clone(&state);
            let worker = std::thread::spawn(move || {
                let result = match operation {
                    "flush" => worker_state.flush(open.fh),
                    "fsync" => worker_state.fsync(Some(open.fh), attr.ino, false),
                    "release" => worker_state.release(open.fh),
                    "link" => worker_state.link(attr.ino, 1, b"hardlink").map(|_| ()),
                    _ => unreachable!(),
                };
                sender.send(result).expect("send barrier result");
            });
            let result = receiver.recv_timeout(Duration::from_secs(1));
            let pending = state.async_writes.metrics_snapshot();
            // Always unblock and join even when the regression fails.
            ticket.complete(Ok(()));
            worker.join().expect("barrier worker should not panic");
            assert_eq!(
                result,
                Ok(Err(libc::EIO)),
                "{operation} must recover its worker"
            );
            assert_eq!(pending.pending_writes, 1, "timeout must retain the write");
            assert_eq!(
                pending.pending_bytes, 4,
                "timeout must retain its reservation"
            );
            assert_eq!(state.async_writes.metrics_snapshot().pending_writes, 0);
            assert!(matches!(state.lookup(1, b"hardlink"), Err(libc::ENOENT)));
            state
                .link(attr.ino, 1, b"hardlink")
                .expect("late completion restores link");
            assert!(
                state.vfs.get_open_file(open.fh).is_some(),
                "timeout must retain the VFS handle"
            );
            assert_eq!(
                state.flush(open.fh),
                Ok(()),
                "late success restores barriers"
            );
            if operation != "release" {
                state
                    .async_writes
                    .start_request(attr.ino, open.fh)
                    .expect("timeout does not stop new writes")
                    .complete(Ok(()));
            }
            let release = state.release(open.fh);
            super::finish_regular_release_with(&state, Some(open.fh), release, |_| {});
            assert!(state.vfs.get_open_file(open.fh).is_none());
            assert!(
                matches!(
                    state.async_writes.wait_all(Duration::ZERO),
                    crate::async_writes::BarrierOutcome::Complete(None)
                ),
                "shutdown must still reconcile late completion"
            );
        }
    }

    #[test]
    fn timeout_preserves_release_liveness_until_late_write_reclaims_handle() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("timeout-release-liveness")
                .unwrap();
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.async_writes = Arc::new(
            crate::async_writes::AsyncWriteLedger::with_limits(
                super::DEFAULT_MAX_PENDING_BYTES,
                super::DEFAULT_MAX_PENDING_WRITES,
            )
            .with_barrier_timeout(Duration::ZERO),
        );
        let attr = state.vfs.create(1, b"pending", 0, 0, 0o644).unwrap();
        let open = state.open_with_flags(attr.ino, libc::O_WRONLY).unwrap();
        let ticket = state.async_writes.admit(attr.ino, open.fh, 4).unwrap();
        state.unlink(1, b"pending").expect("unlink open file");
        let release = state.release(open.fh);
        assert_eq!(release, Err(libc::EIO));
        super::finish_regular_release_with(&state, Some(open.fh), release, |errno| {
            assert_eq!(errno, libc::EIO);
            assert!(state.inode_liveness.is_live(attr.ino));
        });
        assert!(state.vfs.get_open_file(open.fh).is_some());
        assert!(
            state.inode_liveness.is_live(attr.ino),
            "pending release must retain its pin after reply"
        );
        let policy = state.local_sync_policy_for_handle(open.fh);
        state
            .runtime_handle
            .block_on(
                state
                    .vfs
                    .write_fh_with_deferred_materialize(open.fh, attr.ino, 0, b"late", policy),
            )
            .expect("late write must retain access to the unlinked inode");
        let mut bytes = [0; 4];
        assert_eq!(
            state
                .runtime_handle
                .block_on(state.vfs.read_into_fh(open.fh, attr.ino, 0, &mut bytes))
                .unwrap(),
            4
        );
        assert_eq!(&bytes, b"late");
        ticket.complete(Ok(()));
        assert!(
            state.vfs.get_open_file(open.fh).is_none(),
            "late completion must close the VFS handle"
        );
        assert!(!state.inode_liveness.is_live(attr.ino));
        assert!(state.async_writes.forget_handle(open.fh));
        assert_eq!(
            state.async_writes.wait_all(Duration::ZERO),
            crate::async_writes::BarrierOutcome::Complete(None)
        );
    }

    #[test]
    fn timeout_keeps_unrelated_readonly_callbacks_and_link_usable() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("timeout-unrelated-callbacks")
                .unwrap();
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.async_writes = Arc::new(
            crate::async_writes::AsyncWriteLedger::with_limits(
                super::DEFAULT_MAX_PENDING_BYTES,
                super::DEFAULT_MAX_PENDING_WRITES,
            )
            .with_barrier_timeout(Duration::ZERO),
        );
        let wedged = state.vfs.create(1, b"wedged", 0, 0, 0o644).unwrap();
        let writer = state.open_with_flags(wedged.ino, libc::O_WRONLY).unwrap();
        let ticket = state.async_writes.admit(wedged.ino, writer.fh, 4).unwrap();
        assert_eq!(state.flush(writer.fh), Err(libc::EIO));
        let idle = state.vfs.create(1, b"idle", 0, 0, 0o644).unwrap();
        let reader = state.open_with_flags(idle.ino, libc::O_RDONLY).unwrap();
        assert_eq!(state.flush(reader.fh), Ok(()));
        assert_eq!(state.fsync(Some(reader.fh), idle.ino, false), Ok(()));
        state
            .link(idle.ino, 1, b"idle-link")
            .expect("unrelated link succeeds");
        let release = state.release(reader.fh);
        assert_eq!(release, Ok(()));
        super::finish_regular_release_with(&state, Some(reader.fh), release, |_| {});
        assert!(state.vfs.get_open_file(reader.fh).is_none());
        assert!(!state.inode_liveness.is_live(idle.ino));
        ticket.complete(Ok(()));
        let release = state.release(writer.fh);
        assert_eq!(release, Ok(()));
        super::finish_regular_release_with(&state, Some(writer.fh), release, |_| {});
    }

    #[test]
    fn timed_out_async_barrier_does_not_enter_vfs() {
        let result = super::finish_async_barrier_with::<()>(
            crate::async_writes::BarrierOutcome::TimedOut,
            || panic!("a timed out write must not enter a VFS barrier"),
        );
        assert_eq!(result, Err(libc::EIO));
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

    type ThreadReplies = Arc<Mutex<Vec<(std::thread::ThreadId, Result<u32, i32>)>>>;

    struct ThreadRecordingWriteReply {
        replies: ThreadReplies,
    }

    impl super::FuseWriteReply for ThreadRecordingWriteReply {
        fn reply_write(&mut self, written: libc::size_t) -> libc::c_int {
            self.replies
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((thread::current().id(), Ok(written as u32)));
            0
        }

        fn reply_error(&mut self, errno: libc::c_int) -> libc::c_int {
            self.replies
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((thread::current().id(), Err(errno)));
            0
        }
    }

    #[test]
    fn inline_write_panic_replies_eio_instead_of_unwinding_into_libfuse() {
        assert_eq!(
            super::catch_inline_write_panic(|| panic!("simulated VFS write panic")),
            Err(libc::EIO)
        );
        assert_eq!(super::catch_inline_write_panic(|| Ok(7)), Ok(7));
    }

    #[test]
    fn inline_write_slots_are_bounded_and_released_on_drop() {
        let in_flight = std::sync::atomic::AtomicUsize::new(0);
        let first = super::InlineWriteSlot::acquire(&in_flight, 2).expect("first slot");
        let second = super::InlineWriteSlot::acquire(&in_flight, 2).expect("second slot");
        assert!(super::InlineWriteSlot::acquire(&in_flight, 2).is_none());
        drop(first);
        let third = super::InlineWriteSlot::acquire(&in_flight, 2).expect("released slot");
        drop((second, third));
        assert_eq!(in_flight.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn small_async_writes_run_inline_and_large_writes_keep_the_runtime_hand_off() {
        if !super::linux_fuse_async_write_enabled() {
            eprintln!("skipped: async writes are disabled by the environment");
            return;
        }
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-inline-write")
                .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .create(1, b"inline.bin", 1000, 1000, 0o644)
            .expect("create file");
        let fh = fixture
            .vfs()
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDWR, false)
            .expect("open file");
        // SAFETY: the C shim allocates a zeroed file info that this test frees below.
        let fi = unsafe { super::studiofs_linux_fuse_file_info_new_for_test(fh) };
        let replies = Arc::new(Mutex::new(Vec::new()));
        let write = |data: &[u8], offset: libc::off_t| {
            state.queue_async_write(
                super::FuseWriteCallbackContext {
                    reply: ThreadRecordingWriteReply {
                        replies: Arc::clone(&replies),
                    },
                    started: Instant::now(),
                },
                attr.ino,
                data.as_ptr().cast(),
                data.len(),
                offset,
                fi,
            )
        };
        let small = vec![1_u8; super::INLINE_WRITE_MAX_BYTES];
        assert_eq!(write(&small, 0), Ok(true));
        assert_eq!(
            *replies.lock().unwrap(),
            [(thread::current().id(), Ok(small.len() as u32))],
            "a small write must reply before the callback returns"
        );
        assert_eq!(state.async_writes.metrics_snapshot().pending_writes, 0);

        let large = vec![2_u8; super::INLINE_WRITE_MAX_BYTES + 1];
        assert_eq!(write(&large, small.len() as libc::off_t), Ok(true));
        assert_eq!(
            state.async_writes.wait_all(Duration::from_secs(10)),
            crate::async_writes::BarrierOutcome::Complete(None)
        );
        let replies = replies.lock().unwrap().clone();
        assert_eq!(replies.len(), 2);
        assert_ne!(
            replies[1].0,
            thread::current().id(),
            "a large write keeps the runtime hand-off"
        );
        assert_eq!(replies[1].1, Ok(large.len() as u32));

        // With every inline slot taken, a small write falls back to the hand-off.
        let held = (0..state.inline_write_limit)
            .map(|_| {
                super::InlineWriteSlot::acquire(&state.inline_writes, state.inline_write_limit)
                    .expect("free inline slot")
            })
            .collect::<Vec<_>>();
        let replies_before = replies.len();
        let replies = Arc::new(Mutex::new(Vec::new()));
        assert_eq!(
            state.queue_async_write(
                super::FuseWriteCallbackContext {
                    reply: ThreadRecordingWriteReply {
                        replies: Arc::clone(&replies),
                    },
                    started: Instant::now(),
                },
                attr.ino,
                small.as_ptr().cast(),
                small.len(),
                0,
                fi,
            ),
            Ok(true)
        );
        assert_eq!(
            state.async_writes.wait_all(Duration::from_secs(10)),
            crate::async_writes::BarrierOutcome::Complete(None)
        );
        drop(held);
        assert_eq!(replies_before, 2);
        let fallback = replies.lock().unwrap().clone();
        assert_eq!(fallback.len(), 1);
        assert_ne!(
            fallback[0].0,
            thread::current().id(),
            "a write beyond the inline limit must not hold the callback worker"
        );
        // SAFETY: allocated by the C shim with calloc and no longer used.
        unsafe { libc::free(fi.cast()) };
        let mut read_back = vec![0_u8; small.len() + large.len()];
        let read = fixture
            .runtime()
            .block_on(fixture.vfs().read_into_fh(fh, attr.ino, 0, &mut read_back))
            .expect("read back");
        assert_eq!(read as usize, read_back.len());
        assert!(read_back[..small.len()].iter().all(|byte| *byte == 1));
        assert!(read_back[small.len()..].iter().all(|byte| *byte == 2));
        fixture
            .runtime()
            .block_on(fixture.vfs().shutdown(Duration::ZERO))
            .expect("VFS shutdown");
    }

    #[test]
    fn async_dio_session_reads_configuration_at_initialization() {
        const CHILD_EXPECTED: &str = "STUDIOFS_TEST_ASYNC_DIO_EXPECTED";
        let Ok(expected) = std::env::var(CHILD_EXPECTED) else {
            for (value, expected) in [
                (None, false),
                (Some("true"), true),
                (Some("false"), false),
                (Some("invalid"), false),
            ] {
                let mut child = Command::new(std::env::current_exe().expect("test executable"));
                child
                    .args([
                        "--exact",
                        "low_level::tests::async_dio_session_reads_configuration_at_initialization",
                    ])
                    .env(CHILD_EXPECTED, if expected { "1" } else { "0" })
                    .env_remove("STUDIOFS_FUSE_ASYNC_DIO");
                if let Some(value) = value {
                    child.env("STUDIOFS_FUSE_ASYNC_DIO", value);
                }
                let output = child.output().expect("run isolated configuration test");
                assert!(
                    output.status.success(),
                    "async-DIO value {value:?}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
                assert!(
                    String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed;"),
                    "child must execute exactly one configuration test: {}",
                    String::from_utf8_lossy(&output.stdout),
                );
            }
            return;
        };
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-async-dio")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        assert_eq!(state.async_direct_io, expected == "1");
    }

    #[test]
    fn async_dio_session_configuration_controls_connection_and_opens() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-async-dio-opens")
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
            .create(1, b"async-dio.txt", 1000, 1000, 0o644)
            .expect("create file");
        let fh = fixture
            .vfs()
            .open_file(attr.ino, studiofs_vfs::OpenFlags::RDONLY, false)
            .expect("open file");
        for enabled in [false, true] {
            // Explicit session configuration must win over the process environment.
            state.async_direct_io = enabled;
            let userdata = std::ptr::from_mut(&mut state).cast();
            // SAFETY: userdata points to the live session for this synchronous call.
            let requested =
                unsafe { super::studiofs_linux_fuse_connection_wants_async_dio(userdata) };
            assert_eq!(requested, i32::from(enabled));
            // Repeated preparation must keep using the stored session decision.
            for _ in 0..2 {
                let prepared = state.prepare_file_open(&attr, libc::O_RDONLY, fh);
                assert!(prepared.cache.direct_io);
                assert_eq!(prepared.cache.parallel_direct_writes, enabled);
            }
        }
    }

    #[test]
    fn linux_fuse_env_flags_accept_only_true_values() {
        for (value, expected) in [
            ("1", true),
            ("true", true),
            ("yes", true),
            ("on", true),
            (" ON ", true),
            ("0", false),
            ("false", false),
            ("off", false),
            ("", false),
            ("invalid", false),
        ] {
            assert_eq!(
                super::linux_fuse_env_flag_value(value),
                expected,
                "{value:?}"
            );
        }
    }

    #[test]
    fn async_write_defaults_to_enabled_with_explicit_disable() {
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
        let args =
            super::OwnedFuseArgs::new(super::argv_strings("demo", false)).expect("build args");

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
    fn loop_thread_count_caps_default_at_small_queue() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 16,
            congestion_threshold: 12,
            ..Default::default()
        };
        assert_eq!(config.worker_thread_count(true), 16);
    }

    #[test]
    fn loop_thread_count_caps_at_background_queue() {
        let config = super::LinuxFuseConnectionConfig {
            max_background: 16,
            congestion_threshold: 12,
            worker_threads: Some(64),
        };
        assert_eq!(config.worker_thread_count(true), 16);
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
            worker_threads: None,
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
        // SAFETY: the C shim only reports the libfuse headers it was compiled against.
        let has_parallel_direct_writes =
            unsafe { super::studiofs_linux_fuse_file_info_has_parallel_direct_writes() } != 0;

        // libfuse < 3.15 (Ubuntu 24.04 ships 3.14) has no parallel_direct_writes bit.
        let expected = if has_parallel_direct_writes { 6 } else { 2 };
        assert_eq!(flags, expected);
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
            super::linux_fuse_cache_policy_selection_for_value(None, false).policy,
            crate::cache_policy::LinuxFuseCachePolicy::Direct
        );
    }

    #[test]
    fn runtime_cache_policy_honors_explicit_adaptive_before_default_switch() {
        assert_eq!(
            super::linux_fuse_cache_policy_selection_for_value(Some("adaptive"), false).policy,
            crate::cache_policy::LinuxFuseCachePolicy::Adaptive
        );
    }

    #[test]
    fn runtime_cache_policy_uses_adaptive_after_readiness_gate() {
        assert_eq!(
            super::linux_fuse_cache_policy_selection_for_value(None, true).policy,
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

    /// Start a sampler reporting ample memory and wait for its first snapshot.
    fn published_memory_sampler() -> crate::cache_policy::SystemMemorySampler {
        let sampler = crate::cache_policy::SystemMemorySampler::start(|| Some(1 << 40));
        let deadline = Instant::now() + Duration::from_secs(5);
        while sampler.available_memory().is_none() {
            assert!(Instant::now() < deadline, "memory sampler never published");
            thread::yield_now();
        }
        sampler
    }

    #[test]
    fn adaptive_local_dirty_reader_stays_direct_with_active_writer_and_caches_after_release() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-local-dirty-page-cache",
        )
        .expect("create VFS fixture");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;
        state.available_memory = Some(published_memory_sampler());
        let attr = fixture
            .vfs()
            .create(1, b"local-dirty-page-cache.txt", 1000, 1000, 0o644)
            .expect("create file");
        let open = |flags| {
            let mut prepared = None;
            let result = state
                .run_open_reply_transaction(
                    attr.ino,
                    || state.open_with_flags(attr.ino, flags),
                    |open| {
                        prepared = Some(open);
                        0
                    },
                )
                .expect("open transaction");
            assert_eq!(result, 0);
            prepared.expect("accepted open")
        };

        let writer = open(libc::O_RDWR);
        assert!(writer.cache.direct_io);
        fixture
            .runtime()
            .block_on(
                fixture
                    .vfs()
                    .write_fh(writer.fh, attr.ino, 0, b"written locally"),
            )
            .expect("local write");

        let concurrent = open(libc::O_RDONLY);
        assert_eq!(
            concurrent.reason,
            studiofs_vfs::vfs::OsCacheReason::ActiveWriter
        );
        assert!(
            concurrent.cache.direct_io,
            "a reader beside an open writer must not cache pages"
        );
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(concurrent.fh))
            .expect("release concurrent reader");
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(writer.fh))
            .expect("release writer");

        // The fixture is not started, so the upload stays pending.
        let reader = open(libc::O_RDONLY);
        assert_eq!(reader.reason, studiofs_vfs::vfs::OsCacheReason::LocalDirty);
        assert_eq!(
            reader.cache,
            super::LinuxFuseOpenCachePolicy {
                direct_io: false,
                keep_cache: false,
                parallel_direct_writes: false,
                read_ahead_owner: studiofs_vfs::ReadAheadOwner::Kernel,
                memory_bypassed: false,
            },
            "a closed writer's bytes may use kernel readahead, but the open must purge older pages"
        );
        fixture
            .runtime()
            .block_on(fixture.vfs().release_fh(reader.fh))
            .expect("release reader");
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
                        observed: state
                            .observe_attr(|| Ok(attr.clone()))
                            .expect("observe attrs"),
                        fh,
                        flags: libc::O_RDONLY,
                    })
                },
                |prepared| {
                    assert_eq!(
                        (prepared.disposition, prepared.reason),
                        (
                            studiofs_vfs::vfs::OsCacheDisposition::Reuse,
                            studiofs_vfs::vfs::OsCacheReason::StableGeneration
                        ),
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
        assert_ne!(
            next.reason,
            studiofs_vfs::vfs::OsCacheReason::StableGeneration,
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
    fn session_read_reuses_dense_buffer_for_holes_short_reads_and_eof() {
        struct FileInfo(*mut super::raw::FuseFileInfo);
        impl Drop for FileInfo {
            fn drop(&mut self) {
                // SAFETY: the C test helper allocated this pointer with calloc.
                unsafe { libc::free(self.0.cast()) };
            }
        }

        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new(
            "linux-fuse-pooled-sparse-read",
        )
        .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let attr = state.vfs.create(1, b"sparse", 0, 0, 0o644).unwrap();
        let open = state.open_with_flags(attr.ino, libc::O_RDWR).unwrap();
        fixture.runtime().block_on(async {
            state
                .vfs
                .write_fh(open.fh, attr.ino, 0, b"abcdefgh")
                .await
                .unwrap();
            state
                .vfs
                .write_fh(open.fh, attr.ino, 16, b"end")
                .await
                .unwrap();
        });
        // SAFETY: the helper allocates and initializes the native header's layout.
        let fi = FileInfo(unsafe { super::studiofs_linux_fuse_file_info_new_for_test(open.fh) });
        assert!(!fi.0.is_null(), "allocate native file info");

        let dense = state.read(attr.ino, 8, 0, fi.0).unwrap();
        let allocation = dense.as_ref().as_ptr();
        state.reply_with_pooled_read_buffer(dense, |bytes| {
            assert_eq!(bytes, b"abcdefgh");
            0
        });
        for (offset, expected) in [
            (8, &b"\0\0\0\0\0\0\0\0"[..]),
            (16, &b"end"[..]),
            (19, &b""[..]),
        ] {
            let buffer = state.read(attr.ino, 8, offset, fi.0).unwrap();
            assert_eq!(
                buffer.as_ref().as_ptr(),
                allocation,
                "reuse the dense read buffer"
            );
            state.reply_with_pooled_read_buffer(buffer, |bytes| {
                assert_eq!(bytes, expected);
                0
            });
        }
        state.release(open.fh).unwrap();
    }

    #[test]
    fn runtime_stats_open_serves_captured_snapshot_until_release() {
        struct FileInfo(*mut super::raw::FuseFileInfo);
        impl Drop for FileInfo {
            fn drop(&mut self) {
                // SAFETY: the C test helper allocated this pointer with calloc.
                unsafe { libc::free(self.0.cast()) };
            }
        }

        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-runtime-stats")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let ino = state.vfs.runtime_stats_file_attr().ino;
        for flags in [libc::O_WRONLY, libc::O_RDWR, libc::O_RDONLY | libc::O_TRUNC] {
            assert_eq!(
                state.open_with_flags(ino, flags).err(),
                Some(libc::EACCES),
                "the stats surface is read-only for flags {flags:#x}"
            );
        }

        let open = state.open_with_flags(ino, libc::O_RDONLY).unwrap();
        // SAFETY: the helper allocates and initializes the native header's layout.
        let fi = FileInfo(unsafe { super::studiofs_linux_fuse_file_info_new_for_test(open.fh) });
        assert!(!fi.0.is_null(), "allocate native file info");

        let mut text = Vec::new();
        loop {
            let offset = libc::off_t::try_from(text.len()).unwrap();
            let chunk = state.read(ino, 512, offset, fi.0).unwrap();
            let len = chunk.as_ref().len();
            text.extend_from_slice(chunk.as_ref());
            state.read_buffers.put(chunk);
            if len == 0 {
                break;
            }
        }
        let text = String::from_utf8(text).expect("stats text is UTF-8");
        assert!(
            text.starts_with("# studiofs_runtime_stats"),
            "unexpected stats text: {text:?}"
        );

        // Later reads at any offset come from the same captured snapshot.
        let middle = text.len() / 2;
        let tail = state
            .read(
                ino,
                text.len(),
                libc::off_t::try_from(middle).unwrap(),
                fi.0,
            )
            .unwrap();
        assert_eq!(tail.as_ref(), &text.as_bytes()[middle..]);
        state.read_buffers.put(tail);
        let past_eof = state
            .read(
                ino,
                64,
                libc::off_t::try_from(text.len() + 4096).unwrap(),
                fi.0,
            )
            .unwrap();
        assert!(past_eof.as_ref().is_empty(), "reads past EOF are empty");
        state.read_buffers.put(past_eof);

        state.release(open.fh).unwrap();
        assert!(
            state.runtime_stats_snapshots.lock().unwrap().is_empty(),
            "release drops the captured snapshot"
        );
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
        buffer.as_mut()[0] = 0x5a;
        let allocation = buffer.as_ref().as_ptr();
        let spare = state.read_buffers.take(4096);
        assert_ne!(spare.as_ref().as_ptr(), allocation);
        state.read_buffers.put(spare);

        let reply_result = state.reply_with_pooled_read_buffer(buffer, |bytes| {
            assert_eq!(bytes[0], 0x5a);
            -libc::ENOENT
        });
        let reused = state.read_buffers.take(4096);

        assert_eq!(reply_result, -libc::ENOENT);
        assert_eq!(reused.as_ref().as_ptr(), allocation);
    }

    #[test]
    fn invalidation_state_wait_for_idle_returns_true_when_idle() {
        let state = super::SessionInvalidationState::new();

        assert!(state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_wait_for_idle_wakes_when_last_guard_drops() {
        let state = Arc::new(super::SessionInvalidationState::new());
        state.mark_root_primed();
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
        state.mark_root_primed();
        let _guard = state.begin_invalidation().expect("begin invalidation");

        assert!(!state.wait_for_idle(Duration::from_millis(1)));
    }

    #[test]
    fn invalidation_state_holds_invalidations_until_root_is_primed() {
        let state = Arc::new(super::SessionInvalidationState::new());
        let waiter_state = Arc::clone(&state);
        let (admitted_tx, admitted_rx) = mpsc::channel();
        let waiter = thread::spawn(move || {
            let admitted = waiter_state.begin_invalidation().is_some();
            admitted_tx.send(admitted).expect("send admission");
        });

        assert_eq!(
            admitted_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "root invalidation must wait for the first root GETATTR"
        );
        state.mark_root_primed();
        assert!(
            admitted_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("admission after priming")
        );
        waiter.join().expect("join gated invalidation");
    }

    #[test]
    fn invalidation_state_shutdown_releases_unprimed_invalidation() {
        let state = Arc::new(super::SessionInvalidationState::new());
        let waiter_state = Arc::clone(&state);
        let waiter = thread::spawn(move || waiter_state.begin_invalidation().is_none());

        state.begin_shutdown();
        assert!(waiter.join().expect("join gated invalidation"));
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

    fn install_reply_invalidator(state: &super::SessionState) {
        state
            .vfs
            .set_kernel_invalidator(Arc::new(super::LinuxFuseKernelInvalidator {
                queue: state.invalidation_queue.clone(),
            }));
    }

    fn reply_fixture(
        name: &str,
    ) -> (
        studiofs_vfs::contract_harness::ContractVfsFixture,
        super::SessionState,
    ) {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new(name).expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        (fixture, state)
    }

    #[test]
    fn reply_getattr_counter_tracks_worker_threads() {
        let (fixture, state) = reply_fixture("getattr-worker-counter");
        let attr = fixture
            .vfs()
            .create(1, b"file", 0, 0, 0o644)
            .expect("create inode");
        let before = state.getattr_calls.load(Ordering::Relaxed);
        thread::scope(|scope| {
            scope
                .spawn(|| state.getattr_attr(attr.ino).expect("read on worker"))
                .join()
                .expect("join worker");
        });
        assert_eq!(state.getattr_calls.load(Ordering::Relaxed), before + 1);
        let other_state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        assert_eq!(other_state.getattr_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn stable_reply_entry_avoids_second_getattr() {
        let (fixture, state) = reply_fixture("stable-entry");
        fixture
            .vfs()
            .create(1, b"stable", 0, 0, 0o644)
            .expect("create inode");
        install_reply_invalidator(&state);
        let observed = state.lookup(1, b"stable").expect("lookup inode");
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(super::reply_entry_with(&state, observed, |_| 0), Ok(0));
        let after = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            after, before,
            "stable entry must reuse the lookup attributes"
        );
    }

    #[test]
    fn stable_reply_open_avoids_second_getattr() {
        let (fixture, mut state) = reply_fixture("stable-open");
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        let attr = fixture
            .vfs()
            .create(1, b"stable", 0, 0, 0o644)
            .expect("create inode");
        install_reply_invalidator(&state);
        let allocated = state
            .open_with_flags(attr.ino, libc::O_RDONLY)
            .expect("allocate open");
        let fh = allocated.fh;
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            state.run_open_reply_transaction(attr.ino, || Ok(allocated), |_| 0),
            Ok(0)
        );
        let after = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            after, before,
            "stable open must reuse the allocated attributes"
        );
        state.reject_prepared_open(attr.ino, fh);
    }

    #[test]
    fn stable_reply_create_avoids_second_getattr() {
        let (fixture, mut state) = reply_fixture("stable-create");
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        install_reply_invalidator(&state);
        // Isolate reply preparation from fresh-create incarnation retirement.
        let attr = fixture
            .vfs()
            .create(1, b"stable", 0, 0, 0o644)
            .expect("create inode");
        let ino = attr.ino;
        let open = state
            .open_with_flags(ino, libc::O_RDONLY)
            .expect("allocate handle");
        let fh = open.fh;
        let allocated = super::LinuxFuseAllocatedCreate {
            open,
            lookup: state.inode_liveness.reserve_lookup(ino),
        };
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            state.run_create_reply_transaction(allocated, |attr, _, lookup| {
                super::reply_reserved_entry_with(&state, attr, lookup, |_| 0)
            }),
            Ok(0)
        );
        let after = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            after, before,
            "stable create must reuse the allocated attributes"
        );
        state.reject_prepared_open(ino, fh);
        state.inode_liveness.forget(ino, 1);
    }

    #[test]
    fn create_reply_refreshes_invalidation_from_handle_allocation() {
        let (_fixture, mut state) = reply_fixture("create-own-invalidation");
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        let allocated = state
            .create_with_flags(1, b"new", libc::S_IFREG | 0o644, libc::O_RDONLY)
            .expect("allocate create");
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            state.run_create_reply_transaction(allocated, |_, _, _| -libc::ENOENT),
            Ok(-libc::ENOENT)
        );
        assert_eq!(
            state.getattr_calls.load(Ordering::Relaxed),
            before + 1,
            "allocation retired the original snapshot and must force refresh"
        );
    }

    #[test]
    fn entry_reply_preserves_source_cache_deadline_and_refreshes_expired_snapshot() {
        let (fixture, state) = reply_fixture("entry-expiry");
        fixture
            .vfs()
            .create(1, b"cached", 0, 0, 0o644)
            .expect("create inode");
        let mut observed = state.lookup(1, b"cached").expect("observe cached inode");
        assert!(observed.expires_at.is_some());
        assert_eq!(
            observed.expires_at,
            state.vfs.attribute_cache_deadline(&observed.attr)
        );
        let mut different = observed.attr.clone();
        different.size += 1;
        assert_eq!(state.vfs.attribute_cache_deadline(&different), None);
        observed.expires_at = Some(Instant::now());
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(super::reply_entry_with(&state, observed, |_| 0), Ok(0));
        assert_eq!(state.getattr_calls.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn entry_reply_refreshes_uncached_snapshot() {
        let (fixture, state) = reply_fixture("entry-uncached");
        let attr = fixture
            .vfs()
            .create(1, b"uncached", 0, 0, 0o644)
            .expect("create inode");
        let observed = state
            .observe_attr(|| Ok(attr))
            .expect("observe uncached attributes");
        assert_eq!(observed.expires_at, None);
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(super::reply_entry_with(&state, observed, |_| 0), Ok(0));
        assert_eq!(state.getattr_calls.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn entry_reply_refreshes_invalidation_during_attribute_observation() {
        let (fixture, state) = reply_fixture("entry-read-race");
        let attr = fixture
            .vfs()
            .create(1, b"raced", 0, 0, 0o644)
            .expect("create inode");
        install_reply_invalidator(&state);
        let observed = state
            .observe_attr(|| {
                let stale = state.getattr_attr(attr.ino)?;
                fixture
                    .vfs()
                    .setattr(attr.ino, studiofs_meta::SetattrArgs::size_only(73))
                    .expect("change size during observation");
                Ok(stale)
            })
            .expect("observe raced attributes");
        assert_eq!(
            super::reply_entry_with(&state, observed, |entry| {
                // SAFETY: the entry stays live for the synchronous reply closure.
                assert_eq!(unsafe { (*entry).attr.st_size }, 73);
                0
            }),
            Ok(0)
        );
    }

    #[test]
    fn entry_reply_failed_refresh_releases_reservation() {
        let (fixture, state) = reply_fixture("entry-refresh-error");
        let attr = fixture
            .vfs()
            .create(1, b"removed", 0, 0, 0o644)
            .expect("create inode");
        install_reply_invalidator(&state);
        let observed = state.lookup(1, b"removed").expect("lookup before unlink");
        fixture.vfs().unlink(1, b"removed").expect("unlink inode");
        assert_eq!(
            super::reply_entry_with(&state, observed, |_| panic!("must not reply stale entry")),
            Err(libc::ENOENT)
        );
        assert!(!state.inode_liveness.is_live(attr.ino));
        assert_eq!(
            state
                .inode_liveness
                .admit_invalidation(attr.ino, || {})
                .deferred_reply_count(),
            0
        );
    }

    #[test]
    fn allocated_open_refreshes_local_change_before_native_invalidation() {
        let (fixture, state) = reply_fixture("open-refresh-race");
        let attr = fixture
            .vfs()
            .create(1, b"raced", 0, 0, 0o644)
            .expect("create inode");
        install_reply_invalidator(&state);
        let allocated = state
            .open_with_flags(attr.ino, libc::O_RDONLY)
            .expect("allocate open");
        fixture
            .vfs()
            .setattr(attr.ino, studiofs_meta::SetattrArgs::size_only(91))
            .expect("change size after handle pinning");
        let fh = allocated.fh;
        let current = state
            .allocated_open_attr(allocated.observed, allocated.flags)
            .expect("refresh queued change");
        assert_eq!(current.size, 91);
        state.reject_prepared_open(attr.ino, fh);
        assert!(!state.inode_liveness.is_live(attr.ino));
    }

    #[test]
    fn allocated_open_truncation_refreshes_its_own_size_change() {
        let (fixture, state) = reply_fixture("open-trunc-refresh");
        let attr = fixture
            .vfs()
            .create(1, b"truncated", 0, 0, 0o644)
            .expect("create inode");
        fixture
            .vfs()
            .setattr(attr.ino, studiofs_meta::SetattrArgs::size_only(57))
            .expect("set initial size");
        let allocated = state
            .open_with_flags(attr.ino, libc::O_WRONLY | libc::O_TRUNC)
            .expect("allocate truncating open");
        assert_eq!(
            allocated.observed.attr.size, 57,
            "allocation observed pre-truncate size"
        );
        let fh = allocated.fh;
        let current = state
            .allocated_open_attr(allocated.observed, allocated.flags)
            .expect("refresh truncated size");
        assert_eq!(current.size, 0);
        state.reject_prepared_open(attr.ino, fh);
    }

    #[test]
    fn create_reply_refreshes_after_reservation_and_keeps_follow_up() {
        let (fixture, mut state) = reply_fixture("create-refresh-race");
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Direct;
        // Same reusable allocation as `stable_reply_create_avoids_second_getattr`,
        // so only the post-reservation change can force the refresh.
        let ino = fixture
            .vfs()
            .create(1, b"raced", 0, 0, 0o644)
            .expect("create inode")
            .ino;
        let open = state
            .open_with_flags(ino, libc::O_RDONLY)
            .expect("allocate handle");
        let fh = open.fh;
        let allocated = super::LinuxFuseAllocatedCreate {
            open,
            lookup: state.inode_liveness.reserve_lookup(ino),
        };
        fixture
            .vfs()
            .setattr(ino, studiofs_meta::SetattrArgs::size_only(83))
            .expect("change size after lookup reservation");
        let admission = state.inode_liveness.admit_invalidation(ino, || {});
        assert_eq!(admission.deferred_reply_count(), 1);
        let before = state.getattr_calls.load(Ordering::Relaxed);
        assert_eq!(
            state.run_create_reply_transaction(allocated, |attr, _, reservation| {
                assert_eq!(attr.size, 83);
                super::reply_reserved_entry_with(&state, attr, reservation, |_| 0)
            }),
            Ok(0)
        );
        assert_eq!(
            state.getattr_calls.load(Ordering::Relaxed),
            before + 1,
            "post-reservation change must force an attribute refresh"
        );
        assert_eq!(
            state.invalidation_queue.pop(),
            Some((ino, crate::invalidation::FuseInvalidationKind::Data))
        );
        state.reject_prepared_open(ino, fh);
        state.inode_liveness.forget(ino, 1);
    }

    #[test]
    fn entry_reply_rechecks_access_after_attribute_observation() {
        let (fixture, state) = reply_fixture("entry-access-race");
        let attr = fixture
            .vfs()
            .create(1, b"restricted", 0, 0, 0o644)
            .expect("create inode");
        let observed = state
            .lookup(1, b"restricted")
            .expect("lookup before access change");
        fixture
            .vfs()
            .set_acl_rule(attr.ino, "*", studiofs_meta::AccessLevel::NoAccess)
            .expect("revoke access without changing attributes");
        assert_eq!(
            super::reply_entry_with(&state, observed, |_| panic!("must not bypass access")),
            Err(libc::ENOENT)
        );
        assert!(!state.inode_liveness.is_live(attr.ino));
    }

    #[test]
    fn entry_reply_refreshes_synthetic_attributes_without_cache_retirement() {
        let (fixture, state) = reply_fixture("entry-synthetic");
        let observed = state
            .mknod(1, b"._synthetic", libc::S_IFREG | 0o644)
            .expect("create synthetic inode");
        fixture
            .vfs()
            .setattr(observed.attr.ino, studiofs_meta::SetattrArgs::size_only(61))
            .expect("change synthetic attributes");
        assert_eq!(
            super::reply_entry_with(&state, observed, |entry| {
                // SAFETY: the entry stays live for the synchronous reply closure.
                assert_eq!(unsafe { (*entry).attr.st_size }, 61);
                0
            }),
            Ok(0)
        );
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

        assert_eq!(
            super::reply_entry_with(
                &state,
                state.observe_attr(|| Ok(accepted.clone())).unwrap(),
                |_| 0
            ),
            Ok(0)
        );
        assert!(state.inode_liveness.is_live(accepted.ino));
        assert_eq!(
            super::reply_entry_with(
                &state,
                state.observe_attr(|| Ok(rejected.clone())).unwrap(),
                |_| -libc::ENOENT
            ),
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
        install_reply_invalidator(&state);
        let observed = state.lookup(1, b"fresh-attr").expect("observe stale attrs");
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

        let result = super::reply_entry_with(&state, observed, |entry| {
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
            super::reply_entry_with(
                &state,
                state.observe_attr(|| Ok(attr.clone())).unwrap(),
                |_| {
                    let admission = state.inode_liveness.admit_invalidation(attr.ino, || {});
                    assert_eq!(admission.deferred_reply_count(), 1);
                    0
                }
            ),
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
                        observed: state
                            .observe_attr(|| Ok(attr.clone()))
                            .expect("observe attrs"),
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
        let ino = allocated.open.observed.attr.ino;
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
        let handle = state
            .open_dir(attr.ino, attr.parent)
            .expect("open directory");
        assert!(state.inode_liveness.is_live(attr.ino));

        assert_eq!(
            super::finish_dir_open_reply(handle, -libc::ENOENT),
            -libc::ENOENT
        );
        assert!(!state.inode_liveness.is_live(attr.ino));
    }

    #[test]
    fn open_handles_on_another_segment_do_not_wait_for_busy_handle() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-open-segments")
                .expect("create fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.install_open_liveness(42, 7);
        state.install_open_liveness(43, 8);
        let busy = state.open_liveness_shard(7).lock().unwrap();
        let result = std::thread::scope(|scope| {
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let state = &state;
            let worker = scope.spawn(move || {
                drop(state.remove_open_liveness(8));
                done_tx.send(()).unwrap();
            });
            let result = done_rx.recv_timeout(Duration::from_secs(2));
            drop(busy);
            worker.join().unwrap();
            result
        });
        assert!(result.is_ok(), "an unrelated handle must make progress");
        assert!(state.inode_liveness.is_live(42));
        assert!(!state.inode_liveness.is_live(43));
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
        let handle = state
            .open_dir(attr.ino, attr.parent)
            .expect("open directory");
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
    fn read_only_mount_allows_read_only_append_open() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-append")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .create(1, b"read-only-append.txt", 1000, 1000, 0o644)
            .expect("create file");

        let open = state
            .open_with_flags(attr.ino, libc::O_RDONLY | libc::O_APPEND)
            .expect("O_APPEND does not make a read-only open writable");
        let release = state.release(open.fh);
        super::finish_regular_release_with(&state, Some(open.fh), release, |_| {});
    }

    #[test]
    fn read_only_mount_rejects_write_and_truncate_open_modes() {
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-read-only-open")
                .expect("create VFS fixture");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            true,
            default_connection_config(),
        );
        let attr = fixture
            .vfs()
            .create(1, b"read-only-open.txt", 1000, 1000, 0o644)
            .expect("create file");

        for flags in [libc::O_WRONLY, libc::O_RDWR, libc::O_RDONLY | libc::O_TRUNC] {
            assert_eq!(
                state.open_with_flags(attr.ino, flags).unwrap_err(),
                libc::EROFS,
                "read-only mount must reject open flags {flags:#x}"
            );
        }
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

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_stat_after_readdir_needs_no_lookup() {
        let fixture = studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-dirplus")
            .expect("create VFS fixture");
        fixture.start().expect("start VFS fixture");
        let dir = fixture.vfs().mkdir(1, b"dir", 0, 0, 0o755).unwrap();
        for index in 0..64 {
            fixture
                .vfs()
                .create(dir.ino, format!("file-{index:02}").as_bytes(), 0, 0, 0o644)
                .unwrap();
        }
        let mount_point = std::env::temp_dir().join(format!(
            "studiofs-linux-fuse-dirplus-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&mount_point).expect("create mount point");
        let state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        let mut session =
            super::Session::mount(state, &mount_point, "studiofs-dirplus").expect("mount");
        let lookup_calls = Arc::clone(
            &session
                .state
                .as_deref()
                .expect("mounted state")
                .lookup_calls,
        );
        let mount_point_for_worker = mount_point.clone();
        let worker = std::thread::spawn(
            move || -> std::io::Result<(u64, std::process::ExitStatus)> {
                let guard = UnmountGuard::new(mount_point_for_worker.clone());
                let dir = mount_point_for_worker.join("dir");
                assert!(std::fs::metadata(&dir)?.is_dir());
                let before = lookup_calls.load(Ordering::Relaxed);
                let mut entries = 0;
                for entry in std::fs::read_dir(&dir)? {
                    assert!(std::fs::symlink_metadata(entry?.path())?.is_file());
                    entries += 1;
                }
                assert_eq!(entries, 64);
                let lookups = lookup_calls.load(Ordering::Relaxed) - before;
                let status = Command::new("fusermount3")
                    .arg("-u")
                    .arg(&mount_point_for_worker)
                    .status();
                guard.disarm();
                Ok((lookups, status?))
            },
        );

        session.run().expect("run libfuse3 session");
        let (lookups, unmount_status) = worker
            .join()
            .expect("mounted readdirplus worker should not panic")
            .expect("mounted readdirplus worker should complete");
        assert!(unmount_status.success());
        assert_eq!(lookups, 0, "readdirplus must prime every child dentry");
        drop(session);
        drop(fixture);
        std::fs::remove_dir_all(&mount_point).expect("remove mount point");
    }

    #[test]
    #[ignore = "requires /dev/fuse and fusermount3 on a Linux host"]
    fn mounted_local_dirty_reads_use_kernel_readahead_and_purge_on_reopen() {
        const FILE_LEN: usize = 1024 * 1024;
        const READ_LEN: usize = 4096;
        let fixture =
            studiofs_vfs::contract_harness::ContractVfsFixture::new("linux-fuse-dirty-readahead")
                .expect("create VFS fixture");
        // Leave the fixture unstarted so uploads stay pending and reads are LocalDirty.
        let mount_point = std::env::temp_dir().join(format!(
            "studiofs-linux-fuse-dirty-readahead-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&mount_point).expect("create mount point");
        let mut state = super::SessionState::new(
            fixture.vfs(),
            fixture.runtime().handle().clone(),
            false,
            default_connection_config(),
        );
        state.cache_policy = crate::cache_policy::LinuxFuseCachePolicy::Adaptive;
        state.available_memory = Some(published_memory_sampler());
        let mut session =
            super::Session::mount(state, &mount_point, "studiofs-dirty-readahead").expect("mount");
        let vfs = fixture.vfs();
        let mount_point_for_worker = mount_point.clone();
        let worker = std::thread::spawn(
            move || -> std::io::Result<(u64, std::process::ExitStatus)> {
                let guard = UnmountGuard::new(mount_point_for_worker.clone());
                let path = mount_point_for_worker.join("dirty.bin");
                let read_all = |file: &mut std::fs::File| -> std::io::Result<Vec<u8>> {
                    file.seek(SeekFrom::Start(0))?;
                    let mut bytes = Vec::with_capacity(FILE_LEN);
                    let mut chunk = [0_u8; READ_LEN];
                    loop {
                        let read = file.read(&mut chunk)?;
                        if read == 0 {
                            return Ok(bytes);
                        }
                        bytes.extend_from_slice(&chunk[..read]);
                    }
                };
                std::fs::write(&path, vec![b'a'; FILE_LEN])?;

                // The writer is closed but its bytes are still local; this
                // reader keeps its page-cache handle open across the rewrite.
                // FUSE RELEASE is asynchronous, so an open can still observe
                // the closing writer and stay direct.
                let local_dirty_kernel_opens = || {
                    let cache = vfs
                        .write_path_observability_snapshot()
                        .linux_fuse_open_cache;
                    (cache.local_dirty, cache.kernel_opens)
                };
                let deadline = Instant::now() + Duration::from_secs(10);
                let mut held = loop {
                    let (dirty_before, kernel_before) = local_dirty_kernel_opens();
                    let file = std::fs::File::open(&path)?;
                    let (dirty_after, kernel_after) = local_dirty_kernel_opens();
                    if dirty_after > dirty_before && kernel_after > kernel_before {
                        break file;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "locally dirty read-only open never used the kernel page cache"
                    );
                    drop(file);
                    thread::yield_now();
                };
                let before = vfs.write_path_observability_snapshot().vfs_read_path.calls;
                assert!(read_all(&mut held)?.iter().all(|byte| *byte == b'a'));
                let read_calls =
                    vfs.write_path_observability_snapshot().vfs_read_path.calls - before;

                let mut writer = std::fs::OpenOptions::new().write(true).open(&path)?;
                writer.write_all(&vec![b'b'; FILE_LEN])?;
                drop(writer);
                let mut reopened = std::fs::File::open(&path)?;
                assert!(read_all(&mut reopened)?.iter().all(|byte| *byte == b'b'));
                // The reopen replied without keep_cache, which purged the
                // inode mapping shared with the still-open handle.
                assert!(read_all(&mut held)?.iter().all(|byte| *byte == b'b'));
                drop((held, reopened));

                let status = Command::new("fusermount3")
                    .arg("-u")
                    .arg(&mount_point_for_worker)
                    .status();
                guard.disarm();
                Ok((read_calls, status?))
            },
        );

        session.run().expect("run libfuse3 session");
        let (read_calls, unmount_status) = worker
            .join()
            .expect("mounted readahead worker should not panic")
            .expect("mounted readahead worker should complete");
        assert!(unmount_status.success());
        assert!(
            read_calls < (FILE_LEN / READ_LEN / 4) as u64,
            "kernel readahead must coalesce 4 KiB reads; saw {read_calls} VFS reads"
        );
        drop(session);
        drop(fixture);
        std::fs::remove_dir_all(&mount_point).expect("remove mount point");
    }
}
