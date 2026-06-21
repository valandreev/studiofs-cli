//! fuser::Filesystem implementation — bridges FUSE kernel calls to StudioVfs.

mod handle_drain;
mod pending_write_tracker;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType as FuseFileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, ReplyXattr,
    Request,
};
use handle_drain::FuseHandleDrainFacade;
use pending_write_tracker::{
    PendingWriteAdmissionLimits, PendingWriteCompletion, PendingWriteRegistration,
    PendingWriteTracker, WriteRegistrationError,
};
use studiofs_bridge::posix::{
    BridgeFileAttr, BridgeFileType, posix_flags_to_open_flags, to_bridge_attr,
    vfs_err_to_errno as bridge_vfs_err_to_errno,
};
use studiofs_meta::types::{SetattrArgs, TimeSpec};
use studiofs_meta::{FileType, ROOT_INO};
use studiofs_vfs::is_hidden_platform_root_metadata_name;
use studiofs_vfs::{
    BridgeVfsErrorKind, KernelInvalidator, LocalSyncPolicy, LockKind, OpenFlags, StudioVfs,
    VfsError, VfsFuseAsyncWriteSnapshot, classify_vfs_error,
};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

const TTL: Duration = Duration::from_secs(1);
const DEFAULT_FUSE_MAX_WRITE_BYTES: u32 = 16 * 1024 * 1024;
const DEFAULT_FUSE_MAX_READAHEAD_BYTES: u32 = 16 * 1024 * 1024;
const STUDIOFS_FUSE_MAX_BACKGROUND_ENV: &str = "STUDIOFS_FUSE_MAX_BACKGROUND";
const STUDIOFS_FUSE_CONGESTION_THRESHOLD_ENV: &str = "STUDIOFS_FUSE_CONGESTION_THRESHOLD";
const STUDIOFS_FUSE_ASYNC_WRITE_ENV: &str = "STUDIOFS_FUSE_ASYNC_WRITE";
const STUDIOFS_FUSE_ASYNC_MAX_PENDING_BYTES_ENV: &str = "STUDIOFS_FUSE_ASYNC_MAX_PENDING_BYTES";
const STUDIOFS_FUSE_ASYNC_MAX_PENDING_WRITES_ENV: &str = "STUDIOFS_FUSE_ASYNC_MAX_PENDING_WRITES";
const STUDIOFS_FUSE_ASYNC_PER_HANDLE_PENDING_BYTES_ENV: &str =
    "STUDIOFS_FUSE_ASYNC_PER_HANDLE_PENDING_BYTES";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FuseBackgroundQueuePolicy {
    max_background: Option<u16>,
    congestion_threshold: Option<u16>,
}

impl FuseBackgroundQueuePolicy {
    fn platform_default() -> Self {
        {
            Self {
                max_background: None,
                congestion_threshold: None,
            }
        }
    }

    fn from_env() -> Self {
        let max_background = std::env::var(STUDIOFS_FUSE_MAX_BACKGROUND_ENV).ok();
        let congestion_threshold = std::env::var(STUDIOFS_FUSE_CONGESTION_THRESHOLD_ENV).ok();

        Self::from_env_values(max_background.as_deref(), congestion_threshold.as_deref())
    }

    fn from_env_values(max_background: Option<&str>, congestion_threshold: Option<&str>) -> Self {
        let defaults = Self::platform_default();
        let max_background = parse_bridge_optional_u16_env_value(
            STUDIOFS_FUSE_MAX_BACKGROUND_ENV,
            max_background,
            defaults.max_background,
        );
        let mut congestion_threshold = parse_bridge_optional_u16_env_value(
            STUDIOFS_FUSE_CONGESTION_THRESHOLD_ENV,
            congestion_threshold,
            defaults.congestion_threshold,
        );

        if let (Some(max_bg), Some(threshold)) = (max_background, congestion_threshold)
            && threshold > max_bg
        {
            warn!(
                configured = threshold,
                max_background = max_bg,
                "configured FUSE congestion_threshold exceeds max_background; clamping"
            );
            congestion_threshold = Some(max_bg);
        }

        Self {
            max_background,
            congestion_threshold,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FuseAsyncWriteAdmissionPolicy {
    limits: PendingWriteAdmissionLimits,
}

impl FuseAsyncWriteAdmissionPolicy {
    fn platform_default() -> Self {
        {
            Self {
                limits: PendingWriteAdmissionLimits {
                    max_pending_bytes: Some(128 * 1024 * 1024),
                    max_pending_writes: Some(1024),
                    per_handle_pending_bytes: Some(32 * 1024 * 1024),
                },
            }
        }
    }

    fn from_env() -> Self {
        Self::from_env_values(
            std::env::var(STUDIOFS_FUSE_ASYNC_MAX_PENDING_BYTES_ENV)
                .ok()
                .as_deref(),
            std::env::var(STUDIOFS_FUSE_ASYNC_MAX_PENDING_WRITES_ENV)
                .ok()
                .as_deref(),
            std::env::var(STUDIOFS_FUSE_ASYNC_PER_HANDLE_PENDING_BYTES_ENV)
                .ok()
                .as_deref(),
        )
    }

    fn from_env_values(
        max_pending_bytes: Option<&str>,
        max_pending_writes: Option<&str>,
        per_handle_pending_bytes: Option<&str>,
    ) -> Self {
        let defaults = Self::platform_default().limits;
        Self {
            limits: PendingWriteAdmissionLimits {
                max_pending_bytes: parse_bridge_optional_usize_env_value(
                    STUDIOFS_FUSE_ASYNC_MAX_PENDING_BYTES_ENV,
                    max_pending_bytes,
                    defaults.max_pending_bytes,
                ),
                max_pending_writes: parse_bridge_optional_usize_env_value(
                    STUDIOFS_FUSE_ASYNC_MAX_PENDING_WRITES_ENV,
                    max_pending_writes,
                    defaults.max_pending_writes,
                ),
                per_handle_pending_bytes: parse_bridge_optional_usize_env_value(
                    STUDIOFS_FUSE_ASYNC_PER_HANDLE_PENDING_BYTES_ENV,
                    per_handle_pending_bytes,
                    defaults.per_handle_pending_bytes,
                ),
            },
        }
    }
}

fn fuse_attr_blksize() -> u32 {
    512
}

fn fuse_open_reply_flags(open_flags: OpenFlags) -> u32 {
    if !open_flags.contains(OpenFlags::WRONLY) && !open_flags.contains(OpenFlags::RDWR) {
        fuser::consts::FOPEN_KEEP_CACHE
    } else {
        0
    }
}

fn fuse_write_local_sync_policy(flags: i32) -> LocalSyncPolicy {
    if flags & (libc::O_SYNC | libc::O_DSYNC) != 0 {
        return LocalSyncPolicy::WriteThrough;
    }
    LocalSyncPolicy::Opportunistic
}

fn parse_bridge_u32_env(key: &str, default: u32) -> u32 {
    match std::env::var(key) {
        Ok(raw) => {
            let trimmed = raw.trim();
            match trimmed.parse::<u32>() {
                Ok(value) if value > 0 => value,
                Ok(_) => {
                    warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        "bridge tuning override must be > 0; using default"
                    );
                    default
                }
                Err(error) => {
                    warn!(
                        env_key = key,
                        env_value = trimmed,
                        fallback_value = default,
                        error = %error,
                        "invalid bridge tuning override; using default"
                    );
                    default
                }
            }
        }
        Err(_) => default,
    }
}

fn parse_bridge_optional_u16_env_value(
    key: &str,
    raw: Option<&str>,
    default: Option<u16>,
) -> Option<u16> {
    let Some(raw) = raw else {
        return default;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.parse::<u16>() {
        Ok(value) if value > 0 => Some(value),
        Ok(_) => None,
        Err(error) => {
            warn!(
                env_key = key,
                env_value = trimmed,
                fallback_value = ?default,
                error = %error,
                "invalid bridge tuning override; using default"
            );
            default
        }
    }
}

fn parse_bridge_optional_usize_env_value(
    key: &str,
    raw: Option<&str>,
    default: Option<usize>,
) -> Option<usize> {
    let Some(raw) = raw else {
        return default;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.parse::<usize>() {
        Ok(value) if value > 0 => Some(value),
        Ok(_) => {
            // Empty string explicitly disables admission caps; numeric zero falls
            // back to the safe platform default so hard caps are not removed by
            // accident.
            warn!(
                env_key = key,
                env_value = trimmed,
                fallback_value = ?default,
                "bridge tuning override must be > 0; using default"
            );
            default
        }
        Err(error) => {
            warn!(
                env_key = key,
                env_value = trimmed,
                fallback_value = ?default,
                error = %error,
                "invalid bridge tuning override; using default"
            );
            default
        }
    }
}

fn parse_bridge_bool_env_value(raw: &str) -> bool {
    let trimmed = raw.trim();
    trimmed == "1"
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("on")
}

fn should_request_fuse_async_dio_from_env_value(raw: &str) -> bool {
    parse_bridge_bool_env_value(raw)
}

fn should_request_fuse_async_dio_from_env() -> bool {
    std::env::var("STUDIOFS_FUSE_ASYNC_DIO")
        .is_ok_and(|raw| should_request_fuse_async_dio_from_env_value(&raw))
}

fn should_enable_async_fuse_write_from_env_value(raw: &str) -> bool {
    parse_bridge_bool_env_value(raw)
}

fn resolve_async_fuse_write_enabled(raw: Option<&str>) -> bool {
    raw.is_some_and(should_enable_async_fuse_write_from_env_value)
}

fn should_enable_async_fuse_write_from_env() -> bool {
    let raw = std::env::var(STUDIOFS_FUSE_ASYNC_WRITE_ENV).ok();
    resolve_async_fuse_write_enabled(raw.as_deref())
}

trait FuseWriteReply {
    fn written(self, size: u32);
    fn error(self, errno: i32);
}

trait FuseEmptyReply {
    fn ok(self);
    fn error(self, errno: i32);
}

struct FuseWriteReplyGuard<R: FuseWriteReply> {
    reply: Option<R>,
}

struct FuseEmptyReplyGuard<R: FuseEmptyReply> {
    reply: Option<R>,
}

impl<R: FuseWriteReply> FuseWriteReplyGuard<R> {
    fn new(reply: R) -> Self {
        Self { reply: Some(reply) }
    }

    fn written(mut self, size: u32) {
        if let Some(reply) = self.reply.take() {
            reply.written(size);
        }
    }

    fn error(mut self, errno: i32) {
        if let Some(reply) = self.reply.take() {
            reply.error(errno);
        }
    }
}

impl<R: FuseWriteReply> Drop for FuseWriteReplyGuard<R> {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.take() {
            reply.error(libc::EIO);
        }
    }
}

impl FuseWriteReply for ReplyWrite {
    fn written(self, size: u32) {
        self.written(size);
    }

    fn error(self, errno: i32) {
        self.error(errno);
    }
}

impl<R: FuseEmptyReply> FuseEmptyReplyGuard<R> {
    fn new(reply: R) -> Self {
        Self { reply: Some(reply) }
    }

    fn ok(mut self) {
        if let Some(reply) = self.reply.take() {
            reply.ok();
        }
    }

    fn error(mut self, errno: i32) {
        if let Some(reply) = self.reply.take() {
            reply.error(errno);
        }
    }
}

impl<R: FuseEmptyReply> Drop for FuseEmptyReplyGuard<R> {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.take() {
            reply.error(libc::EIO);
        }
    }
}

impl FuseEmptyReply for ReplyEmpty {
    fn ok(self) {
        self.ok();
    }

    fn error(self, errno: i32) {
        self.error(errno);
    }
}

struct PendingFuseCloseGuard {
    pending_writes: PendingWriteTracker,
    ino: u64,
    fh: u64,
    armed: bool,
}

impl PendingFuseCloseGuard {
    fn new(pending_writes: PendingWriteTracker, ino: u64, fh: u64) -> Self {
        Self {
            pending_writes,
            ino,
            fh,
            armed: true,
        }
    }

    fn finish(&mut self) {
        if self.armed {
            self.pending_writes.finish_close(self.ino, self.fh);
            self.armed = false;
        }
    }
}

impl Drop for PendingFuseCloseGuard {
    fn drop(&mut self) {
        self.finish();
    }
}

fn complete_fuse_write_reply<R>(
    reply: FuseWriteReplyGuard<R>,
    result: Result<u32, VfsError>,
    registration: &PendingWriteRegistration,
    ino: u64,
    offset: u64,
) where
    R: FuseWriteReply,
{
    match result {
        Ok(written) => {
            trace!(
                ino,
                offset,
                sequence = registration.sequence(),
                written,
                "async FUSE write completed"
            );
            reply.written(written);
            registration.complete(PendingWriteCompletion::Success);
        }
        Err(e) => {
            error!("write({ino}, off={offset}) -> {e}");
            let errno = vfs_err_to_errno(&e);
            trace!(
                ino,
                offset,
                sequence = registration.sequence(),
                errno,
                "async FUSE write failed"
            );
            reply.error(errno);
            registration.complete(PendingWriteCompletion::Error(errno));
        }
    }
}

fn complete_fuse_empty_reply<R>(
    reply: FuseEmptyReplyGuard<R>,
    pending_result: Result<(), i32>,
    result: Result<(), VfsError>,
    op: &str,
    ino: u64,
    fh: u64,
) where
    R: FuseEmptyReply,
{
    match (pending_result, result) {
        (Err(errno), Err(e)) => {
            error!(
                "{op}({ino}, fh={fh}) observed pending write error: {errno}; local {op} error: {e}"
            );
            reply.error(errno);
        }
        (Err(errno), _) => {
            error!("{op}({ino}, fh={fh}) observed pending write error: {errno}");
            reply.error(errno);
        }
        (Ok(()), Ok(())) => reply.ok(),
        (Ok(()), Err(e)) => {
            error!("{op}({ino}, fh={fh}) -> {e}");
            reply.error(vfs_err_to_errno(&e));
        }
    }
}

fn write_registration_error_to_errno(error: WriteRegistrationError) -> i32 {
    match error {
        WriteRegistrationError::HandleClosing => libc::EAGAIN,
        WriteRegistrationError::HandleClosed => libc::EIO,
        WriteRegistrationError::AdmissionLimitExceeded => libc::EAGAIN,
    }
}

fn os_str_bytes(name: &OsStr) -> &[u8] {
    name.as_bytes()
}

fn debug_name(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn should_hide_linux_root_metadata_entry(parent: u64, name: &[u8]) -> bool {
    parent == ROOT_INO && is_hidden_platform_root_metadata_name(name)
}

fn heal_hidden_linux_root_metadata_entry(vfs: &StudioVfs, parent: u64, name: &[u8]) {
    if !should_hide_linux_root_metadata_entry(parent, name) {
        return;
    }

    if let Err(error) = vfs.heal_dangling_entry_if_inode_missing(parent, name) {
        debug!(
            parent,
            name = %debug_name(name),
            "failed to heal hidden Linux root metadata entry: {error}"
        );
    }
}

fn is_missing_xattr_error(err: &VfsError) -> bool {
    matches!(
        err,
        VfsError::Meta(studiofs_meta::MetaError::EntryNotFound { .. })
    )
}

fn missing_xattr_errno() -> i32 {
    libc::ENODATA
}

fn note_critical_vfs_error(e: &VfsError) {
    if matches!(
        classify_vfs_error(e),
        BridgeVfsErrorKind::CriticalRuntimeFailure
    ) && let VfsError::Join(join_err) = e
    {
        error!(
            error = %join_err,
            "critical runtime failure (JoinError), requesting emergency unmount"
        );
        super::SHUTDOWN_REQUESTED.store(true, std::sync::atomic::Ordering::Release);
    }
}

fn normalize_unix_mode(mode: u32) -> u16 {
    // VFS stores only Unix permission bits; file type is tracked separately.
    u16::try_from(mode & 0o7777).unwrap_or(0)
}

fn system_time_to_timespec(time: SystemTime) -> TimeSpec {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or(i64::MAX);
    TimeSpec::Set(secs)
}

// ─── Error Mapping ───────────────────────────────────────────────────────────

/// Map VfsError to the correct libc errno for FUSE replies.
fn vfs_err_to_errno(e: &VfsError) -> i32 {
    note_critical_vfs_error(e);
    bridge_vfs_err_to_errno(e)
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Convert InodeAttr to fuser FileAttr.
/// `mount_uid`/`mount_gid` are used to remap inodes that were created with
/// uid=0/gid=0 (the old default) so that the mounting user owns them.
fn to_fuse_attr(attr: &studiofs_meta::InodeAttr, mount_uid: u32, mount_gid: u32) -> FileAttr {
    bridge_attr_to_fuse_attr(to_bridge_attr(
        attr,
        mount_uid,
        mount_gid,
        fuse_attr_blksize(),
    ))
}

fn bridge_attr_to_fuse_attr(attr: BridgeFileAttr) -> FileAttr {
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: attr.crtime,
        kind: match attr.kind {
            BridgeFileType::RegularFile => FuseFileType::RegularFile,
            BridgeFileType::Directory => FuseFileType::Directory,
            BridgeFileType::Symlink => FuseFileType::Symlink,
        },
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: attr.blksize,
        flags: attr.flags,
    }
}

fn to_u64_offset(offset: i64) -> Result<u64, libc::c_int> {
    u64::try_from(offset).map_err(|_| libc::EINVAL)
}

fn clamp_offset_to_usize(offset: i64) -> Result<usize, libc::c_int> {
    usize::try_from(offset).map_err(|_| libc::EINVAL)
}

fn fuse_file_type(kind: FileType) -> FuseFileType {
    match kind {
        FileType::RegularFile => FuseFileType::RegularFile,
        FileType::Directory => FuseFileType::Directory,
        FileType::Symlink => FuseFileType::Symlink,
    }
}

// ─── StudioFuseFs ────────────────────────────────────────────────────────────

/// FUSE filesystem adapter that forwards kernel callbacks to `StudioVfs`.
pub struct StudioFuseFs {
    vfs: Arc<StudioVfs>,
    rt: Handle,
    pending_writes: PendingWriteTracker,
    async_write_enabled: bool,
    linux_async_dio_enabled: bool,
    async_write_admission_policy: FuseAsyncWriteAdmissionPolicy,
    /// UID of the user that mounted the filesystem. Inodes owned by uid=0 in
    /// the DB are remapped to this so macOS permission checks don't block writes.
    mount_uid: u32,
    /// GID of the user that mounted the filesystem.
    mount_gid: u32,
    async_write_diagnostics_stop: Arc<AtomicBool>,
    async_write_diagnostics_task: Option<JoinHandle<()>>,
    next_runtime_stats_fh: u64,
    runtime_stats_handles: HashMap<u64, Vec<u8>>,
}

/// Runtime options for constructing a FUSE adapter without mutating process-wide
/// environment during tests or embedded use.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StudioFuseRuntimeOptions {
    /// Enable the async FUSE write-reply path.
    pub async_write_enabled: bool,
    /// Request Linux `FUSE_ASYNC_DIO` during FUSE init when supported.
    /// Ignored on non-Linux platforms.
    pub linux_async_dio_enabled: bool,
    /// Optional custom limits for async write admission control. When omitted,
    /// explicit-runtime construction uses the safe platform defaults.
    pub async_write_admission_limits: Option<StudioFuseAsyncWriteAdmissionLimits>,
}

/// Public async write admission limit overrides for `StudioFuseRuntimeOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StudioFuseAsyncWriteAdmissionLimits {
    /// Maximum async write bytes admitted across the mounted FUSE adapter.
    pub max_pending_bytes: Option<usize>,
    /// Maximum async write requests admitted across the mounted FUSE adapter.
    pub max_pending_writes: Option<usize>,
    /// Maximum async write bytes admitted for a single file handle.
    pub per_handle_pending_bytes: Option<usize>,
}

impl From<StudioFuseAsyncWriteAdmissionLimits> for PendingWriteAdmissionLimits {
    fn from(limits: StudioFuseAsyncWriteAdmissionLimits) -> Self {
        Self {
            max_pending_bytes: limits.max_pending_bytes,
            max_pending_writes: limits.max_pending_writes,
            per_handle_pending_bytes: limits.per_handle_pending_bytes,
        }
    }
}

impl From<PendingWriteAdmissionLimits> for StudioFuseAsyncWriteAdmissionLimits {
    fn from(limits: PendingWriteAdmissionLimits) -> Self {
        Self {
            max_pending_bytes: limits.max_pending_bytes,
            max_pending_writes: limits.max_pending_writes,
            per_handle_pending_bytes: limits.per_handle_pending_bytes,
        }
    }
}

impl StudioFuseRuntimeOptions {
    /// Read FUSE runtime options from the current process environment.
    pub fn from_env() -> Self {
        Self {
            async_write_enabled: should_enable_async_fuse_write_from_env(),
            linux_async_dio_enabled: should_request_fuse_async_dio_from_env(),
            async_write_admission_limits: Some(
                FuseAsyncWriteAdmissionPolicy::from_env().limits.into(),
            ),
        }
    }
}

impl StudioFuseFs {
    /// Create a FUSE adapter backed by a shared VFS and Tokio runtime handle.
    pub fn new(vfs: Arc<StudioVfs>, rt: Handle) -> Self {
        Self::new_with_runtime_options(vfs, rt, StudioFuseRuntimeOptions::from_env())
    }

    /// Create a FUSE adapter with explicit runtime options.
    pub fn new_with_runtime_options(
        vfs: Arc<StudioVfs>,
        rt: Handle,
        options: StudioFuseRuntimeOptions,
    ) -> Self {
        // SAFETY: getuid/getgid read process-wide credential values from libc and are
        // side-effect-free.
        let mount_uid = unsafe { libc::getuid() };
        // SAFETY: same safety rationale as getuid.
        let mount_gid = unsafe { libc::getgid() };
        let async_write_admission_limits = options
            .async_write_admission_limits
            .map(PendingWriteAdmissionLimits::from)
            .unwrap_or_else(|| FuseAsyncWriteAdmissionPolicy::platform_default().limits);
        let mut fs = Self {
            vfs,
            rt,
            pending_writes: PendingWriteTracker::default(),
            async_write_enabled: options.async_write_enabled,
            linux_async_dio_enabled: options.linux_async_dio_enabled,
            async_write_admission_policy: FuseAsyncWriteAdmissionPolicy {
                limits: async_write_admission_limits,
            },
            mount_uid,
            mount_gid,
            async_write_diagnostics_stop: Arc::new(AtomicBool::new(false)),
            async_write_diagnostics_task: None,
            next_runtime_stats_fh: u64::MAX / 2,
            runtime_stats_handles: HashMap::new(),
        };
        fs.publish_async_write_diagnostics();
        if fs.async_write_enabled {
            fs.async_write_diagnostics_task = Some(fs.spawn_async_write_diagnostics_publisher());
        }
        debug!(
            async_write_enabled = fs.async_write_enabled,
            platform_policy = fs.async_write_platform_policy_label(),
            max_pending_bytes = ?fs.async_write_admission_policy.limits.max_pending_bytes,
            max_pending_writes = ?fs.async_write_admission_policy.limits.max_pending_writes,
            per_handle_pending_bytes = ?fs.async_write_admission_policy.limits.per_handle_pending_bytes,
            "async FUSE write policy selected"
        );
        fs
    }

    fn handle_drain(&self) -> FuseHandleDrainFacade<'_> {
        FuseHandleDrainFacade::new(self)
    }

    fn wait_pending_writes_for_handle(&self, ino: u64, fh: u64) -> Result<(), i32> {
        if !self.async_write_enabled {
            return Ok(());
        }
        let result = if fh != 0 {
            self.pending_writes.handle_barrier(ino, fh).wait()
        } else {
            self.pending_writes.inode_barrier(ino).wait()
        };
        self.publish_async_write_diagnostics();
        result
    }

    fn wait_pending_writes_for_fsync(&self, ino: u64, _fh: u64) -> Result<(), i32> {
        if !self.async_write_enabled {
            return Ok(());
        }
        let result = self.pending_writes.inode_barrier(ino).wait();
        self.publish_async_write_diagnostics();
        result
    }

    fn async_write_platform_policy_label(&self) -> &'static str {
        "linux"
    }

    fn open_runtime_stats_handle(&mut self) -> u64 {
        let fh = self.next_runtime_stats_fh;
        self.next_runtime_stats_fh = self.next_runtime_stats_fh.wrapping_add(1);
        self.runtime_stats_handles
            .insert(fh, self.vfs.runtime_stats_file_text().into_bytes());
        fh
    }

    fn publish_async_write_diagnostics(&self) {
        Self::publish_async_write_diagnostics_snapshot(
            &self.vfs,
            self.async_write_enabled,
            self.async_write_platform_policy_label(),
            self.async_write_admission_policy.limits,
            &self.pending_writes,
        );
    }

    fn spawn_async_write_diagnostics_publisher(&self) -> JoinHandle<()> {
        let vfs = Arc::clone(&self.vfs);
        let pending_writes = self.pending_writes.clone();
        let enabled = self.async_write_enabled;
        let platform_policy = self.async_write_platform_policy_label();
        let limits = self.async_write_admission_policy.limits;
        let stop = Arc::clone(&self.async_write_diagnostics_stop);
        self.rt.spawn(async move {
            let mut interval = tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_millis(500),
                Duration::from_millis(500),
            );
            loop {
                interval.tick().await;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                StudioFuseFs::publish_async_write_diagnostics_snapshot(
                    &vfs,
                    enabled,
                    platform_policy,
                    limits,
                    &pending_writes,
                );
            }
        })
    }

    fn publish_async_write_diagnostics_snapshot(
        vfs: &StudioVfs,
        enabled: bool,
        platform_policy: &'static str,
        limits: PendingWriteAdmissionLimits,
        pending_writes: &PendingWriteTracker,
    ) {
        let tracker = pending_writes.snapshot();
        vfs.record_fuse_async_write_snapshot(VfsFuseAsyncWriteSnapshot {
            snapshot_generation: tracker.mutation_generation,
            enabled,
            platform_policy: platform_policy.into(),
            max_pending_bytes: limits.max_pending_bytes,
            max_pending_writes: limits.max_pending_writes,
            per_handle_pending_bytes: limits.per_handle_pending_bytes,
            pending_writes: tracker.pending_writes,
            pending_bytes: tracker.pending_bytes,
            peak_pending_bytes: tracker.peak_pending_bytes,
            completed_writes: tracker.completed_writes,
            failed_writes: tracker.failed_writes,
            task_failed_writes: tracker.task_failed_writes,
            admission_waits: tracker.admission_waits,
            admission_wait_total_ms: tracker.admission_wait_ns as f64 / 1_000_000.0,
            barrier_waits: tracker.barrier_waits,
            barrier_wait_total_ms: tracker.barrier_wait_ns as f64 / 1_000_000.0,
        });
    }
}

impl Drop for StudioFuseFs {
    fn drop(&mut self) {
        self.async_write_diagnostics_stop
            .store(true, Ordering::Release);
        if let Some(task) = &self.async_write_diagnostics_task {
            task.abort();
        }
    }
}

impl Filesystem for StudioFuseFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        let requested_max_write = parse_bridge_u32_env(
            "STUDIOFS_FUSE_MAX_WRITE_BYTES",
            DEFAULT_FUSE_MAX_WRITE_BYTES,
        );
        if let Err(max_supported) = config.set_max_write(requested_max_write) {
            warn!(
                requested = requested_max_write,
                max_supported, "requested FUSE max_write exceeds kernel-supported limit; clamping"
            );
            if config.set_max_write(max_supported).is_err() {
                return Err(libc::EINVAL);
            }
        }

        let requested_readahead = parse_bridge_u32_env(
            "STUDIOFS_FUSE_MAX_READAHEAD_BYTES",
            DEFAULT_FUSE_MAX_READAHEAD_BYTES,
        );
        if let Err(max_supported) = config.set_max_readahead(requested_readahead) {
            warn!(
                requested = requested_readahead,
                max_supported,
                "requested FUSE max_readahead exceeds kernel-supported limit; clamping"
            );
            if config.set_max_readahead(max_supported).is_err() {
                return Err(libc::EINVAL);
            }
        }

        let background_queue_policy = FuseBackgroundQueuePolicy::from_env();
        if let Some(max_background) = background_queue_policy.max_background
            && let Err(supported_limit) = config.set_max_background(max_background)
        {
            warn!(
                requested = max_background,
                supported_limit,
                "requested FUSE max_background is outside kernel-supported limits; clamping"
            );
            if config.set_max_background(supported_limit).is_err() {
                return Err(libc::EINVAL);
            }
        }
        if let Some(congestion_threshold) = background_queue_policy.congestion_threshold
            && let Err(supported_limit) = config.set_congestion_threshold(congestion_threshold)
        {
            warn!(
                requested = congestion_threshold,
                supported_limit,
                "requested FUSE congestion_threshold is outside kernel-supported limits; clamping"
            );
            if config.set_congestion_threshold(supported_limit).is_err() {
                return Err(libc::EINVAL);
            }
        }

        debug!(
            max_write = requested_max_write,
            max_readahead = requested_readahead,
            max_background = ?background_queue_policy.max_background,
            congestion_threshold = ?background_queue_policy.congestion_threshold,
            "configured FUSE bridge I/O policy"
        );

        if self.linux_async_dio_enabled
            && let Err(unsupported) = config.add_capabilities(fuser::consts::FUSE_ASYNC_DIO)
        {
            warn!(
                ?unsupported,
                "Linux FUSE kernel did not advertise FUSE_ASYNC_DIO; continuing without async direct I/O"
            );
        }

        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_bytes = os_str_bytes(name);
        if should_hide_linux_root_metadata_entry(parent, name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        let attr_result = self.vfs.lookup(parent, name_bytes);

        match attr_result {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                debug!("lookup({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        if self.vfs.is_runtime_stats_file_ino(ino) {
            let attr = self.vfs.runtime_stats_file_attr();
            reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid));
            return;
        }
        match self.vfs.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                debug!("getattr({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mkdir(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.mkdir(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "mkdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn mknod(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("mknod({parent}, {:?}) -> {e}", name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn create(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.create(
            parent,
            &name_bytes,
            req.uid(),
            req.gid(),
            normalize_unix_mode(mode),
        ) {
            Ok(attr) => {
                // Open a handle for the newly created file
                let (open_flags, _trunc) = posix_flags_to_open_flags(flags);
                match self.vfs.open_file(attr.ino, open_flags, false) {
                    Ok(fh) => {
                        reply.created(
                            &TTL,
                            &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                            0,
                            fh,
                            fuse_open_reply_flags(open_flags),
                        );
                    }
                    Err(e) => {
                        error!("create open_file({}) -> {e}", attr.ino);
                        reply.error(vfs_err_to_errno(&e));
                    }
                }
            }
            Err(e) => {
                error!(
                    parent,
                    uid = req.uid(),
                    gid = req.gid(),
                    mode,
                    umask = _umask,
                    flags,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "create -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.unlink(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "unlink -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_bytes = os_str_bytes(name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name_bytes) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name_bytes);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.rmdir(parent, &name_bytes) {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    name = %debug_name(&name_bytes),
                    name_bytes = ?name_bytes,
                    "rmdir -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn symlink(
        &mut self,
        req: &Request<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(link_name).to_vec();
        if should_hide_linux_root_metadata_entry(parent, &name) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, &name);
            reply.error(libc::ENOENT);
            return;
        }
        let tgt = target.as_os_str().as_bytes().to_vec();
        match self.vfs.symlink(parent, &name, &tgt, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(
                &TTL,
                &to_fuse_attr(&attr, self.mount_uid, self.mount_gid),
                0,
            ),
            Err(e) => {
                error!("symlink({parent}, {:?}) -> {e}", link_name);
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.vfs.readlink(ino) {
            Ok(target) => reply.data(&target),
            Err(e) => {
                error!("readlink({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: ReplyEntry,
    ) {
        let name = os_str_bytes(newname).to_vec();
        if should_hide_linux_root_metadata_entry(newparent, &name) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, newparent, &name);
            reply.error(libc::ENOENT);
            return;
        }
        match self.vfs.link(ino, newparent, &name) {
            Ok(attr) => {
                let fattr = to_fuse_attr(&attr, self.mount_uid, self.mount_gid);
                reply.entry(&TTL, &fattr, 0);
            }
            Err(e) => {
                error!("link({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let requested_old_name = os_str_bytes(name);
        let requested_new_name = os_str_bytes(newname);
        if should_hide_linux_root_metadata_entry(parent, requested_old_name) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, parent, requested_old_name);
            reply.error(libc::ENOENT);
            return;
        }
        if should_hide_linux_root_metadata_entry(newparent, requested_new_name) {
            heal_hidden_linux_root_metadata_entry(&self.vfs, newparent, requested_new_name);
            reply.error(libc::ENOENT);
            return;
        }

        match self
            .vfs
            .rename(parent, requested_old_name, newparent, requested_new_name)
        {
            Ok(()) => reply.ok(),
            Err(e) => {
                error!(
                    parent,
                    newparent,
                    flags = _flags,
                    requested_old_name = %debug_name(requested_old_name),
                    requested_new_name = %debug_name(requested_new_name),
                    requested_old_name_bytes = ?requested_old_name,
                    requested_new_name_bytes = ?requested_new_name,
                    "rename -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let (open_flags, trunc) = posix_flags_to_open_flags(flags);
        if self.vfs.is_runtime_stats_file_ino(ino) {
            if open_flags.contains(studiofs_vfs::OpenFlags::WRONLY)
                || open_flags.contains(studiofs_vfs::OpenFlags::RDWR)
                || trunc
            {
                reply.error(libc::EROFS);
                return;
            }
            let fh = self.open_runtime_stats_handle();
            reply.opened(fh, fuser::consts::FOPEN_DIRECT_IO);
            return;
        }
        match self.vfs.open_file(ino, open_flags, trunc) {
            Ok(fh) => {
                let reply_flags = fuse_open_reply_flags(open_flags);
                debug!(
                    ino,
                    fh,
                    posix_flags = flags,
                    ?open_flags,
                    trunc,
                    reply_flags,
                    direct_io = reply_flags & fuser::consts::FOPEN_DIRECT_IO != 0,
                    keep_cache = reply_flags & fuser::consts::FOPEN_KEEP_CACHE != 0,
                    "selected FUSE open cache policy"
                );
                reply.opened(fh, reply_flags);
            }
            Err(e) => {
                if matches!(e, VfsError::WouldBlock | VfsError::RemoteContentPending) {
                    debug!("open({ino}) -> {e}");
                } else {
                    error!("open({ino}) -> {e}");
                }
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let offset = match to_u64_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if self.vfs.is_runtime_stats_file_ino(ino) {
            let Some(bytes) = self.runtime_stats_handles.get(&fh) else {
                reply.error(libc::EBADF);
                return;
            };
            let Ok(start) = usize::try_from(offset) else {
                reply.data(&[]);
                return;
            };
            if start >= bytes.len() {
                reply.data(&[]);
                return;
            }
            let end = start.saturating_add(size as usize).min(bytes.len());
            reply.data(&bytes[start..end]);
            return;
        }
        let clamped_size = match self.vfs.getattr(ino) {
            Ok(attr) => attr.size.saturating_sub(offset).min(u64::from(size)),
            Err(e) => {
                error!("read getattr({ino}, off={offset}, sz={size}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };
        let clamped_size = match usize::try_from(clamped_size) {
            Ok(clamped_size) => clamped_size,
            Err(_) => {
                reply.error(libc::EOVERFLOW);
                return;
            }
        };
        if clamped_size == 0 {
            reply.data(&[]);
            return;
        }

        // fuser's ReplyData API accepts an immutable reply slice instead of
        // exposing a mutable kernel buffer, so FUSE needs this boundary buffer.
        let mut out = vec![0u8; clamped_size];
        let callback_started = Instant::now();
        let result = self
            .rt
            .block_on(self.vfs.read_into_fh(fh, ino, offset, &mut out));
        self.vfs.record_fuse_read_callback(
            clamped_size,
            callback_started.elapsed(),
            result.is_err(),
        );
        match result {
            Ok(n) => {
                let n = match usize::try_from(n) {
                    Ok(n) => n,
                    Err(_) => {
                        reply.error(libc::EOVERFLOW);
                        return;
                    }
                };
                reply.data(&out[..n]);
            }
            Err(e) => {
                error!("read({ino}, off={offset}, sz={size}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let offset = match to_u64_offset(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        let callback_started = Instant::now();
        if self.async_write_enabled {
            // TODO(SFS-465): Async writes must own the FUSE callback buffer.
            // If this path graduates from experimental, replace per-write Vec
            // allocation with a reusable buffer pool to reduce allocator churn.
            let data_len = data.len();
            let registration = match self.pending_writes.register_with_admission(
                ino,
                fh,
                data_len,
                self.async_write_admission_policy.limits,
            ) {
                Ok(registration) => registration,
                Err(e) => {
                    debug!(
                        ino,
                        fh,
                        offset,
                        bytes = data_len,
                        error = ?e,
                        "async FUSE write admission rejected"
                    );
                    reply.error(write_registration_error_to_errno(e));
                    return;
                }
            };
            trace!(
                ino,
                fh,
                offset,
                bytes = data_len,
                sequence = registration.sequence(),
                "async FUSE write scheduled"
            );
            let data = data.to_vec();
            let vfs = Arc::clone(&self.vfs);
            let local_sync_policy = fuse_write_local_sync_policy(flags);
            let reply = FuseWriteReplyGuard::new(reply);
            self.rt.spawn(async move {
                let mut registration = registration;
                let turn = registration.turn_waiter();
                turn.wait().await;
                let result = vfs
                    .write_fh_with_deferred_materialize(fh, ino, offset, &data, local_sync_policy)
                    .await;
                vfs.record_fuse_write_callback(
                    data_len,
                    callback_started.elapsed(),
                    result.is_err(),
                );
                complete_fuse_write_reply(reply, result, &registration, ino, offset);
            });
            return;
        }

        let result = self
            .rt
            .block_on(self.vfs.write_fh_with_deferred_materialize(
                fh,
                ino,
                offset,
                data,
                fuse_write_local_sync_policy(flags),
            ));
        self.vfs.record_fuse_write_callback(
            data.len(),
            callback_started.elapsed(),
            result.is_err(),
        );
        match result {
            Ok(written) => reply.written(written),
            Err(e) => {
                error!("write({ino}, off={offset}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let started = Instant::now();
        debug!(ino, fh, "flush started");
        if self.async_write_enabled {
            let barrier = if fh != 0 {
                self.pending_writes.handle_barrier(ino, fh)
            } else {
                self.pending_writes.inode_barrier(ino)
            };
            let vfs = Arc::clone(&self.vfs);
            let pending_writes = self.pending_writes.clone();
            let admission_limits = self.async_write_admission_policy.limits;
            let platform_policy = self.async_write_platform_policy_label();
            let reply = FuseEmptyReplyGuard::new(reply);
            self.rt.spawn(async move {
                let pending_result = barrier.wait_async().await;
                debug!(
                    ino,
                    fh,
                    pending_error = ?pending_result.as_ref().err(),
                    "async FUSE flush barrier wait completed"
                );
                StudioFuseFs::publish_async_write_diagnostics_snapshot(
                    &vfs,
                    true,
                    platform_policy,
                    admission_limits,
                    &pending_writes,
                );
                let result = if fh != 0 {
                    vfs.flush_fh(fh).await
                } else {
                    vfs.flush(ino).await
                };
                if pending_result.is_ok() && result.is_ok() {
                    debug!(
                        ino,
                        fh,
                        elapsed_ms = started.elapsed().as_millis(),
                        "flush completed"
                    );
                }
                complete_fuse_empty_reply(reply, pending_result, result, "flush", ino, fh);
            });
            return;
        }
        let pending_result = self.wait_pending_writes_for_handle(ino, fh);
        let result = self.handle_drain().flush(ino, fh);
        match (pending_result, result) {
            (Err(errno), Err(e)) => {
                error!(
                    "flush({ino}, fh={fh}) observed pending write error: {errno}; local flush error: {e}"
                );
                reply.error(errno);
            }
            (Err(errno), _) => {
                error!("flush({ino}, fh={fh}) observed pending write error: {errno}");
                reply.error(errno);
            }
            (Ok(()), Ok(())) => {
                debug!(
                    ino,
                    fh,
                    elapsed_ms = started.elapsed().as_millis(),
                    "flush completed"
                );
                reply.ok();
            }
            (Ok(()), Err(e)) => {
                if fh != 0 {
                    error!("flush_fh({fh}) -> {e}");
                } else {
                    error!("flush({ino}) -> {e}");
                }
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let started = Instant::now();
        debug!(ino, fh, "release started");
        if self.vfs.is_runtime_stats_file_ino(ino) {
            self.runtime_stats_handles.remove(&fh);
            reply.ok();
            return;
        }
        if self.async_write_enabled {
            let barrier = match self.pending_writes.begin_close(ino, fh) {
                Ok(barrier) => barrier,
                Err(e) => {
                    reply.error(write_registration_error_to_errno(e));
                    return;
                }
            };
            let vfs = Arc::clone(&self.vfs);
            let pending_writes = self.pending_writes.clone();
            let admission_limits = self.async_write_admission_policy.limits;
            let platform_policy = self.async_write_platform_policy_label();
            let mut close_guard = PendingFuseCloseGuard::new(self.pending_writes.clone(), ino, fh);
            let reply = FuseEmptyReplyGuard::new(reply);
            self.rt.spawn(async move {
                let pending_result = barrier.wait_async().await;
                debug!(
                    ino,
                    fh,
                    pending_error = ?pending_result.as_ref().err(),
                    "async FUSE release barrier wait completed"
                );
                StudioFuseFs::publish_async_write_diagnostics_snapshot(
                    &vfs,
                    true,
                    platform_policy,
                    admission_limits,
                    &pending_writes,
                );
                let release_result = vfs.release_fh(fh).await;
                close_guard.finish();
                if pending_result.is_ok() && release_result.is_ok() {
                    debug!(
                        ino,
                        fh,
                        elapsed_ms = started.elapsed().as_millis(),
                        "release completed"
                    );
                }
                complete_fuse_empty_reply(
                    reply,
                    pending_result,
                    release_result,
                    "release",
                    ino,
                    fh,
                );
            });
            return;
        }
        let pending_result = if self.async_write_enabled {
            match self.pending_writes.begin_close(ino, fh) {
                Ok(barrier) => barrier.wait(),
                Err(e) => Err(write_registration_error_to_errno(e)),
            }
        } else {
            Ok(())
        };
        let release_result = self.handle_drain().release(ino, fh);
        if self.async_write_enabled {
            self.pending_writes.finish_close(ino, fh);
        }
        match (pending_result, release_result) {
            (Err(errno), Err(e)) => {
                error!(
                    "release({ino}, fh={fh}) observed pending write error: {errno}; local release error: {e}"
                );
                reply.error(errno);
            }
            (Ok(()), Ok(())) => {
                debug!(
                    ino,
                    fh,
                    elapsed_ms = started.elapsed().as_millis(),
                    "release completed"
                );
                reply.ok();
            }
            (Err(errno), Ok(())) => {
                error!("release({ino}, fh={fh}) observed pending write error: {errno}");
                reply.error(errno);
            }
            (_, Err(e)) => {
                error!("release({ino}, fh={fh}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        let started = Instant::now();
        debug!(ino, fh, datasync, "fsync started");
        if self.async_write_enabled {
            let barrier = self.pending_writes.inode_barrier(ino);
            let vfs = Arc::clone(&self.vfs);
            let pending_writes = self.pending_writes.clone();
            let admission_limits = self.async_write_admission_policy.limits;
            let platform_policy = self.async_write_platform_policy_label();
            let reply = FuseEmptyReplyGuard::new(reply);
            self.rt.spawn(async move {
                let pending_result = barrier.wait_async().await;
                debug!(
                    ino,
                    fh,
                    datasync,
                    pending_error = ?pending_result.as_ref().err(),
                    "async FUSE fsync barrier wait completed"
                );
                StudioFuseFs::publish_async_write_diagnostics_snapshot(
                    &vfs,
                    true,
                    platform_policy,
                    admission_limits,
                    &pending_writes,
                );
                let result = if fh != 0 {
                    vfs.flush_fh(fh).await
                } else {
                    vfs.fsync(ino, datasync).await
                };
                if pending_result.is_ok() && result.is_ok() {
                    debug!(
                        ino,
                        fh,
                        datasync,
                        elapsed_ms = started.elapsed().as_millis(),
                        "fsync completed"
                    );
                }
                complete_fuse_empty_reply(reply, pending_result, result, "fsync", ino, fh);
            });
            return;
        }
        let pending_result = self.wait_pending_writes_for_fsync(ino, fh);
        let result = if fh != 0 {
            // Handle-backed FUSE fsync follows the same local-durability
            // contract as flush_fh: it persists dirty state locally, waits for
            // any deferred release barrier, and leaves asynchronous upload
            // sticky errors isolated from this callback. Inode-only fsync
            // below is the explicit sticky-error reporting path used when no
            // file handle is available.
            self.rt.block_on(self.vfs.flush_fh(fh))
        } else {
            self.rt.block_on(self.vfs.fsync(ino, datasync))
        };
        match (pending_result, result) {
            (Err(errno), Err(e)) => {
                error!(
                    "fsync({ino}, fh={fh}) observed pending write error: {errno}; local fsync error: {e}"
                );
                reply.error(errno);
            }
            (Err(errno), _) => {
                error!("fsync({ino}, fh={fh}) observed pending write error: {errno}");
                reply.error(errno);
            }
            (Ok(()), Ok(())) => {
                debug!(
                    ino,
                    fh,
                    datasync,
                    elapsed_ms = started.elapsed().as_millis(),
                    "fsync completed"
                );
                reply.ok();
            }
            (Ok(()), Err(e)) => {
                error!("fsync({ino}, fh={fh}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdir({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        // Offset-based iteration (offset 0 = ".", 1 = "..", 2+ = entries)
        let mut idx = match clamp_offset_to_usize(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };
        if idx == 0 {
            if reply.add(ino, 1, FuseFileType::Directory, ".") {
                reply.ok();
                return;
            }
            idx = 1;
        }
        if idx == 1 {
            // Parent (simplified: use same ino)
            if reply.add(ino, 2, FuseFileType::Directory, "..") {
                reply.ok();
                return;
            }
            idx = 2;
        }

        for (i, entry) in entries.iter().enumerate() {
            if should_hide_linux_root_metadata_entry(ino, &entry.name) {
                heal_hidden_linux_root_metadata_entry(&self.vfs, ino, &entry.name);
                continue;
            }
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let ft = fuse_file_type(entry.kind);
            let entry_name = &entry.name;
            let next_offset = match i64::try_from(entry_idx + 1) {
                Ok(next_offset) => next_offset,
                Err(_) => {
                    reply.error(libc::EOVERFLOW);
                    return;
                }
            };
            if reply.add(entry.ino, next_offset, ft, OsStr::from_bytes(entry_name)) {
                break;
            }
        }

        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let entries = match self.vfs.readdir(ino) {
            Ok(e) => e,
            Err(e) => {
                error!("readdirplus({ino}) -> {e}");
                reply.error(vfs_err_to_errno(&e));
                return;
            }
        };

        let mut idx = match clamp_offset_to_usize(offset) {
            Ok(offset) => offset,
            Err(errno) => {
                reply.error(errno);
                return;
            }
        };

        // "." entry
        if idx == 0 {
            let dot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 1, ".", &TTL, &dot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 1;
        }

        // ".." entry
        if idx == 1 {
            let dotdot_attr = match self.vfs.getattr(ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(e) => {
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            };
            if reply.add(ino, 2, "..", &TTL, &dotdot_attr, 0) {
                reply.ok();
                return;
            }
            idx = 2;
        }

        // Regular entries with full attributes
        for (i, entry) in entries.iter().enumerate() {
            if should_hide_linux_root_metadata_entry(ino, &entry.name) {
                heal_hidden_linux_root_metadata_entry(&self.vfs, ino, &entry.name);
                continue;
            }
            let entry_idx = i + 2;
            if entry_idx < idx {
                continue;
            }
            let next_offset = match i64::try_from(entry_idx + 1) {
                Ok(next_offset) => next_offset,
                Err(_) => {
                    reply.error(libc::EOVERFLOW);
                    return;
                }
            };
            let fattr = match self.vfs.getattr(entry.ino) {
                Ok(a) => to_fuse_attr(&a, self.mount_uid, self.mount_gid),
                Err(_) => continue, // skip entries we can't stat
            };
            let entry_name = &entry.name;
            if reply.add(
                entry.ino,
                next_offset,
                OsStr::from_bytes(entry_name),
                &TTL,
                &fattr,
                0,
            ) {
                break;
            }
        }

        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let args = SetattrArgs {
            size,
            mode: mode.map(normalize_unix_mode),
            uid,
            gid,
            atime: match atime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => system_time_to_timespec(st),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            mtime: match mtime {
                Some(fuser::TimeOrNow::SpecificTime(st)) => system_time_to_timespec(st),
                Some(fuser::TimeOrNow::Now) => TimeSpec::Now,
                None => TimeSpec::Omit,
            },
            crtime: match _crtime {
                Some(st) => system_time_to_timespec(st),
                None => TimeSpec::Omit,
            },
            win_attrs: None,
        };
        match self.vfs.setattr_with_handle(fh, ino, args) {
            Ok(attr) => reply.attr(&TTL, &to_fuse_attr(&attr, self.mount_uid, self.mount_gid)),
            Err(e) => {
                error!(
                    ino,
                    mode = ?mode,
                    uid = ?uid,
                    gid = ?gid,
                    size = ?size,
                    atime = ?atime,
                    mtime = ?mtime,
                    crtime = ?_crtime,
                    chgtime = ?_chgtime,
                    bkuptime = ?_bkuptime,
                    flags = ?_flags,
                    "setattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        match self.vfs.statfs() {
            Ok(st) => reply.statfs(
                st.blocks, st.bfree, st.bavail, st.files, st.ffree, st.bsize, st.namelen, st.frsize,
            ),
            Err(e) => {
                error!("statfs -> {e}");
                // Fallback to hardcoded values on error
                reply.statfs(
                    1024 * 1024 * 1024,
                    512 * 1024 * 1024,
                    512 * 1024 * 1024,
                    1_000_000,
                    500_000,
                    4096,
                    255,
                    4096,
                );
            }
        }
    }

    // With DefaultPermissions set the kernel already checked mode bits.
    fn access(&mut self, _req: &Request<'_>, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        let key = name.as_encoded_bytes();

        match self.vfs.get_xattr(ino, key) {
            Ok(Some(val)) => {
                if size == 0 {
                    let data_len = match u32::try_from(val.len()) {
                        Ok(data_len) => data_len,
                        Err(_) => {
                            reply.error(libc::EOVERFLOW);
                            return;
                        }
                    };
                    reply.size(data_len);
                } else if val.len() <= size as usize {
                    reply.data(&val);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Ok(None) => {
                reply.error(missing_xattr_errno());
            }
            Err(e) => {
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    size,
                    "getxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let key = name.as_encoded_bytes();

        let normalized_flags = match u32::try_from(flags) {
            Ok(normalized_flags) => normalized_flags,
            Err(_) => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let value_buf = value.to_vec();

        match self.vfs.set_xattr(ino, key, &value_buf, normalized_flags) {
            Ok(()) => reply.ok(),
            Err(e) => {
                if is_missing_xattr_error(&e) {
                    debug!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        flags,
                        normalized_flags,
                        position = _position,
                        "setxattr missing xattr"
                    );
                    reply.error(missing_xattr_errno());
                    return;
                }
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    value_len = value.len(),
                    stored_value_len = value_buf.len(),
                    flags,
                    normalized_flags,
                    position = _position,
                    "setxattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn removexattr(&mut self, _req: &Request<'_>, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        let key = name.as_encoded_bytes();

        match self.vfs.remove_xattr(ino, key) {
            Ok(()) => reply.ok(),
            Err(e) => {
                if is_missing_xattr_error(&e) {
                    debug!(
                        ino,
                        key = %debug_name(key),
                        key_bytes = ?key,
                        "removexattr missing xattr"
                    );
                    reply.error(missing_xattr_errno());
                    return;
                }
                error!(
                    ino,
                    key = %debug_name(key),
                    key_bytes = ?key,
                    "removexattr -> {e}"
                );
                reply.error(vfs_err_to_errno(&e));
            }
        }
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        match self.vfs.list_xattrs(ino) {
            Ok(names) => {
                // xattr list format: null-terminated name strings concatenated
                let data: Vec<u8> = names
                    .iter()
                    .flat_map(|n| n.iter().cloned().chain(std::iter::once(0u8)))
                    .collect();
                if size == 0 {
                    let data_len = match u32::try_from(data.len()) {
                        Ok(data_len) => data_len,
                        Err(_) => {
                            reply.error(libc::EOVERFLOW);
                            return;
                        }
                    };
                    reply.size(data_len);
                } else if data.len() <= size as usize {
                    reply.data(&data);
                } else {
                    reply.error(libc::ERANGE);
                }
            }
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    fn fsyncdir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    #[allow(clippy::unnecessary_cast)]
    fn setlk(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        lock_owner: u64,
        _start: u64,
        _end: u64,
        typ: i32,
        _pid: u32,
        sleep: bool,
        reply: ReplyEmpty,
    ) {
        // File-level semantics only (byte ranges ignored)
        let op = if typ == libc::F_RDLCK as i32 {
            LockKind::Shared
        } else if typ == libc::F_WRLCK as i32 {
            LockKind::Exclusive
        } else if typ == libc::F_UNLCK as i32 {
            LockKind::Unlock
        } else {
            reply.error(libc::EINVAL);
            return;
        };
        match self.rt.block_on(self.vfs.flock(ino, op, lock_owner, sleep)) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(vfs_err_to_errno(&e)),
        }
    }

    #[allow(clippy::unnecessary_cast)]
    fn getlk(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _lock_owner: u64,
        start: u64,
        end: u64,
        _typ: i32,
        _pid: u32,
        reply: fuser::ReplyLock,
    ) {
        // Report no conflicting locks (single-node simplification)
        reply.locked(start, end, libc::F_UNLCK as i32, 0);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        length: i64,
        mode: i32,
        reply: ReplyEmpty,
    ) {
        // For cloud FS, preallocation is a no-op — no local disk to reserve.
        // We only extend the file size if FALLOC_FL_KEEP_SIZE is not set.
        if mode == 0 {
            // FALLOC_FL_KEEP_SIZE not set → extend size if needed
            match self.vfs.getattr(ino) {
                Ok(attr) => {
                    let offset = match to_u64_offset(offset) {
                        Ok(offset) => offset,
                        Err(errno) => {
                            reply.error(errno);
                            return;
                        }
                    };
                    let length = match u64::try_from(length) {
                        Ok(length) => length,
                        Err(_) => {
                            reply.error(libc::EINVAL);
                            return;
                        }
                    };
                    let new_size = match offset.checked_add(length) {
                        Some(new_size) => new_size,
                        None => {
                            reply.error(libc::EINVAL);
                            return;
                        }
                    };
                    if new_size > attr.size {
                        let args = SetattrArgs {
                            size: Some(new_size),
                            ..Default::default()
                        };
                        if let Err(e) = self.vfs.setattr_with_handle(Some(fh), ino, args) {
                            error!("fallocate setattr({ino}) -> {e}");
                            reply.error(vfs_err_to_errno(&e));
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("fallocate getattr({ino}) -> {e}");
                    reply.error(vfs_err_to_errno(&e));
                    return;
                }
            }
        }
        reply.ok();
    }
}

// ─── Kernel Cache Invalidator ────────────────────────────────────────────────

/// Bridges `fuser::Notifier` to the VFS `KernelInvalidator` trait so that
/// Metadata refreshes can immediately clear the Linux kernel's
/// FUSE dentry and attribute caches.
pub struct FuseKernelInvalidator {
    notifier: fuser::Notifier,
}

impl FuseKernelInvalidator {
    /// Create a kernel invalidator from a live FUSE notifier.
    pub fn new(notifier: fuser::Notifier) -> Self {
        Self { notifier }
    }
}

fn should_log_invalidation_error(err: &std::io::Error) -> bool {
    !matches!(
        err.kind(),
        std::io::ErrorKind::NotFound | std::io::ErrorKind::NotConnected
    )
}

impl KernelInvalidator for FuseKernelInvalidator {
    fn inval_inode(&self, ino: u64) {
        self.inval_inode_range(ino, 0, -1, "data");
    }

    fn inval_inode_attrs(&self, ino: u64) {
        // fuser uses a negative offset as attribute-only invalidation.
        self.inval_inode_range(ino, -1, 0, "attrs");
    }
}

impl FuseKernelInvalidator {
    fn inval_inode_range(&self, ino: u64, offset: i64, len: i64, scope: &'static str) {
        if let Err(e) = self.notifier.inval_inode(ino, offset, len) {
            // ENOENT is harmless (kernel already evicted the entry).
            if should_log_invalidation_error(&e) {
                warn!(ino, offset, len, scope, error = %e, "FUSE notify_inval_inode failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fuse_open_reply_flags;
    use super::{
        DEFAULT_FUSE_MAX_READAHEAD_BYTES, DEFAULT_FUSE_MAX_WRITE_BYTES,
        FuseAsyncWriteAdmissionPolicy, FuseBackgroundQueuePolicy, FuseEmptyReplyGuard,
        FuseWriteReplyGuard, PendingFuseCloseGuard, StudioFuseFs, TTL, complete_fuse_write_reply,
        fuse_write_local_sync_policy, is_missing_xattr_error, missing_xattr_errno,
        normalize_unix_mode, posix_flags_to_open_flags, resolve_async_fuse_write_enabled,
        should_enable_async_fuse_write_from_env_value, should_log_invalidation_error,
        should_request_fuse_async_dio_from_env_value, to_fuse_attr, vfs_err_to_errno,
    };
    use crate::fs::pending_write_tracker::{PendingWriteAdmissionLimits, PendingWriteTracker};
    use std::cell::Cell;

    use std::sync::Arc;
    use studiofs_data::store::DataStoreConfig;
    use studiofs_meta::InodeAttr;
    use studiofs_meta::MetaError;
    use studiofs_meta::schema::{CURRENT_FORMAT_VERSION, FormatInfo, MetadataConfig};
    use studiofs_meta::{MetaFormat, RedbMetaEngine};
    use studiofs_vfs::{LocalSyncPolicy, OpenFlags, StudioVfs, VfsConfig, VfsError};
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    fn test_format_info(volume_suffix: &str) -> FormatInfo {
        let volume_name = format!("fuse-barrier-{volume_suffix}-{}", std::process::id());
        FormatInfo {
            format_version: CURRENT_FORMAT_VERSION,
            name: volume_name.clone(),
            uuid: format!("fuse-barrier-uuid-{volume_suffix}"),
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "studiofs-test".into(),
            access_key: "test".into(),
            secret_key: "test".into(),
            region: "us-east-1".into(),
            data_prefix: format!("{volume_name}/objects/"),
            metadata: MetadataConfig::default(),
            encryption: studiofs_meta::EncryptionConfig::new(
                [0xA5; studiofs_meta::ENCRYPTION_KEY_LENGTH],
            ),
            block_size: 4 * 1024 * 1024,
        }
    }

    pub(super) fn make_test_fuse_fs(
        volume_suffix: &str,
    ) -> (TempDir, Arc<Runtime>, Arc<StudioVfs>, StudioFuseFs) {
        let tmp = studiofs_test_support::workspace_target_tempdir("test-");
        let db_path = tmp.path().join("meta.redb");
        let meta = Arc::new(RedbMetaEngine::open(&db_path).expect("open RedbMetaEngine"));
        meta.init_root(0, 0).expect("init root");

        let format_info = test_format_info(volume_suffix);
        meta.set_format_info(&format_info).expect("set format info");
        let data_config = DataStoreConfig::from(&format_info);

        let cache_dir = tmp.path().join("cache");
        let rt = Arc::new(Runtime::new().expect("runtime"));
        let _guard = rt.enter();
        let vfs = StudioVfs::new(
            meta,
            &data_config,
            cache_dir.to_str().expect("cache dir str"),
            64 * 1024 * 1024,
            32 * 1024 * 1024,
            VfsConfig::default()
                .try_into()
                .expect("default VFS config should validate"),
        )
        .expect("create vfs");
        vfs.start().expect("start vfs background workers");
        let fs = StudioFuseFs::new(vfs.clone(), rt.handle().clone());
        (tmp, rt, vfs, fs)
    }

    #[test]
    fn runtime_options_override_async_write_admission_limits() {
        let (_tmp, rt, vfs, _env_fs) = make_test_fuse_fs("runtime-options-admission");
        let fs = StudioFuseFs::new_with_runtime_options(
            vfs,
            rt.handle().clone(),
            super::StudioFuseRuntimeOptions {
                async_write_enabled: true,
                linux_async_dio_enabled: true,
                async_write_admission_limits: Some(super::StudioFuseAsyncWriteAdmissionLimits {
                    max_pending_bytes: Some(4096),
                    max_pending_writes: Some(7),
                    per_handle_pending_bytes: Some(1024),
                }),
            },
        );

        assert!(fs.async_write_enabled);
        assert_eq!(
            fs.async_write_admission_policy.limits,
            PendingWriteAdmissionLimits {
                max_pending_bytes: Some(4096),
                max_pending_writes: Some(7),
                per_handle_pending_bytes: Some(1024),
            }
        );
    }

    #[test]
    fn async_write_diagnostics_publish_policy_and_tracker_state() {
        let (_tmp, rt, vfs, _env_fs) = make_test_fuse_fs("async-write-diagnostics");
        let fs = StudioFuseFs::new_with_runtime_options(
            vfs.clone(),
            rt.handle().clone(),
            super::StudioFuseRuntimeOptions {
                async_write_enabled: true,
                linux_async_dio_enabled: false,
                async_write_admission_limits: Some(super::StudioFuseAsyncWriteAdmissionLimits {
                    max_pending_bytes: Some(4096),
                    max_pending_writes: Some(7),
                    per_handle_pending_bytes: Some(1024),
                }),
            },
        );
        let first = fs
            .pending_writes
            .register_with_admission(42, 7, 512, fs.async_write_admission_policy.limits)
            .expect("register pending async write");
        let second = fs
            .pending_writes
            .register_with_admission(42, 8, 256, fs.async_write_admission_policy.limits)
            .expect("register second async write");
        first.complete(super::PendingWriteCompletion::Success);

        fs.publish_async_write_diagnostics();
        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;

        assert!(snapshot.enabled);
        assert_eq!(
            snapshot.platform_policy,
            fs.async_write_platform_policy_label()
        );
        assert_eq!(snapshot.max_pending_bytes, Some(4096));
        assert_eq!(snapshot.max_pending_writes, Some(7));
        assert_eq!(snapshot.per_handle_pending_bytes, Some(1024));
        assert_eq!(snapshot.pending_writes, 1);
        assert_eq!(snapshot.pending_bytes, 256);
        assert_eq!(snapshot.peak_pending_bytes, 768);
        assert_eq!(snapshot.completed_writes, 1);
        assert_eq!(snapshot.failed_writes, 0);
        assert_eq!(snapshot.task_failed_writes, 0);

        second.complete(super::PendingWriteCompletion::Error(libc::EIO));
    }

    #[test]
    fn disabled_async_write_diagnostics_publish_once_without_periodic_task() {
        let (_tmp, rt, vfs, env_fs) =
            make_test_fuse_fs("disabled-async-write-diagnostics-publisher");
        drop(env_fs);
        let _fs = StudioFuseFs::new_with_runtime_options(
            vfs.clone(),
            rt.handle().clone(),
            super::StudioFuseRuntimeOptions {
                async_write_enabled: false,
                linux_async_dio_enabled: false,
                async_write_admission_limits: None,
            },
        );

        vfs.record_fuse_async_write_snapshot(studiofs_vfs::VfsFuseAsyncWriteSnapshot {
            snapshot_generation: 0,
            enabled: true,
            platform_policy: "sentinel".into(),
            ..Default::default()
        });
        rt.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        });

        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert!(snapshot.enabled);
        assert_eq!(snapshot.platform_policy, "sentinel");
    }

    #[test]
    fn normalize_unix_mode_strips_file_type_bits() {
        assert_eq!(normalize_unix_mode(0o40755), 0o755);
        assert_eq!(normalize_unix_mode(0o100644), 0o644);
        assert_eq!(normalize_unix_mode(0o100644 | 0o4000), 0o4644);
    }

    #[test]
    fn default_fuse_max_readahead_matches_platform_expectation() {
        assert_eq!(DEFAULT_FUSE_MAX_READAHEAD_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn default_fuse_max_write_matches_platform_expectation() {
        assert_eq!(DEFAULT_FUSE_MAX_WRITE_BYTES, 16 * 1024 * 1024);
    }

    #[test]
    fn default_fuse_background_queue_policy_matches_platform_expectation() {
        let policy = FuseBackgroundQueuePolicy::platform_default();

        assert_eq!(
            policy,
            FuseBackgroundQueuePolicy {
                max_background: None,
                congestion_threshold: None,
            }
        );
    }

    #[test]
    fn fuse_background_queue_policy_env_overrides_defaults() {
        let policy = FuseBackgroundQueuePolicy::from_env_values(Some("64"), Some("48"));

        assert_eq!(
            policy,
            FuseBackgroundQueuePolicy {
                max_background: Some(64),
                congestion_threshold: Some(48),
            }
        );
    }

    #[test]
    fn fuse_background_queue_policy_env_invalid_values_fall_back_to_defaults() {
        let policy = FuseBackgroundQueuePolicy::from_env_values(Some("zero"), Some("nope"));
        let expected = FuseBackgroundQueuePolicy::platform_default();

        assert_eq!(policy, expected);
    }

    #[test]
    fn fuse_background_queue_policy_empty_env_values_disable_overrides() {
        let policy = FuseBackgroundQueuePolicy::from_env_values(Some(" "), Some(""));

        assert_eq!(
            policy,
            FuseBackgroundQueuePolicy {
                max_background: None,
                congestion_threshold: None,
            }
        );
    }

    #[test]
    fn fuse_background_queue_policy_zero_env_values_disable_overrides() {
        let policy = FuseBackgroundQueuePolicy::from_env_values(Some("0"), Some("0"));

        assert_eq!(
            policy,
            FuseBackgroundQueuePolicy {
                max_background: None,
                congestion_threshold: None,
            }
        );
    }

    #[test]
    fn fuse_background_queue_policy_clamps_congestion_to_max_background() {
        let policy = FuseBackgroundQueuePolicy::from_env_values(Some("32"), Some("64"));

        assert_eq!(
            policy,
            FuseBackgroundQueuePolicy {
                max_background: Some(32),
                congestion_threshold: Some(32),
            }
        );
    }

    #[test]
    fn fuse_kernel_cache_ttl_matches_platform_policy() {
        assert_eq!(TTL, std::time::Duration::from_secs(1));
    }

    #[test]
    fn fuse_open_reply_flags_match_platform_cache_policy() {
        {
            assert_eq!(
                fuse_open_reply_flags(OpenFlags::RDONLY),
                fuser::consts::FOPEN_KEEP_CACHE
            );
            assert_eq!(
                fuse_open_reply_flags(OpenFlags::RDONLY | OpenFlags::APPEND),
                fuser::consts::FOPEN_KEEP_CACHE
            );
            assert_eq!(fuse_open_reply_flags(OpenFlags::WRONLY), 0);
            assert_eq!(fuse_open_reply_flags(OpenFlags::RDWR), 0);
        }
    }

    #[test]
    fn fuse_write_sync_flags_select_write_through_policy() {
        assert_eq!(
            fuse_write_local_sync_policy(libc::O_SYNC),
            LocalSyncPolicy::WriteThrough
        );
        assert_eq!(
            fuse_write_local_sync_policy(libc::O_DSYNC),
            LocalSyncPolicy::WriteThrough
        );
        assert_eq!(
            fuse_write_local_sync_policy(0),
            LocalSyncPolicy::Opportunistic
        );
    }

    #[test]
    fn fuse_async_dio_env_is_opt_in_only() {
        for false_value in ["0", "false", "off", ""] {
            assert!(!should_request_fuse_async_dio_from_env_value(false_value));
        }

        for true_value in ["1", "true", "True", "TRUE", "yes", "Yes", "on", "On"] {
            assert!(should_request_fuse_async_dio_from_env_value(true_value));
        }
    }

    #[test]
    fn fuse_async_write_env_values_parse_as_boolean_overrides() {
        for false_value in ["0", "false", "off", ""] {
            assert!(!should_enable_async_fuse_write_from_env_value(false_value));
        }

        for true_value in ["1", "true", "True", "TRUE", "yes", "Yes", "on", "On"] {
            assert!(should_enable_async_fuse_write_from_env_value(true_value));
        }
    }

    #[test]
    fn async_fuse_write_default_profile_is_disabled_when_env_unset() {
        assert!(!resolve_async_fuse_write_enabled(None));
    }

    #[test]
    fn async_fuse_write_env_overrides_default_profile() {
        for false_value in ["0", "false", "off", ""] {
            assert!(!resolve_async_fuse_write_enabled(Some(false_value)));
        }

        for true_value in ["1", "true", "True", "TRUE", "yes", "Yes", "on", "On"] {
            assert!(resolve_async_fuse_write_enabled(Some(true_value)));
        }
    }

    #[test]
    fn fuse_async_write_admission_policy_env_overrides_defaults() {
        let policy =
            FuseAsyncWriteAdmissionPolicy::from_env_values(Some("4096"), Some("9"), Some("1024"));

        assert_eq!(
            policy.limits,
            PendingWriteAdmissionLimits {
                max_pending_bytes: Some(4096),
                max_pending_writes: Some(9),
                per_handle_pending_bytes: Some(1024),
            }
        );
    }

    #[test]
    fn fuse_async_write_admission_policy_empty_env_values_disable_limits() {
        let policy = FuseAsyncWriteAdmissionPolicy::from_env_values(Some(""), Some(" "), Some(""));

        assert_eq!(
            policy.limits,
            PendingWriteAdmissionLimits {
                max_pending_bytes: None,
                max_pending_writes: None,
                per_handle_pending_bytes: None,
            }
        );
    }

    #[test]
    fn fuse_async_write_admission_policy_zero_env_values_fall_back_to_defaults() {
        let policy =
            FuseAsyncWriteAdmissionPolicy::from_env_values(Some("0"), Some("0"), Some("0"));
        let expected = FuseAsyncWriteAdmissionPolicy::platform_default();

        assert_eq!(policy, expected);
    }

    #[derive(Debug, Default)]
    struct RecordingWriteReply {
        written: Vec<u32>,
        errors: Vec<i32>,
    }

    impl RecordingWriteReply {
        fn assert_written_once(&self, size: u32) {
            assert_eq!(self.written, [size]);
            assert!(self.errors.is_empty());
        }
    }

    impl super::FuseWriteReply for &mut RecordingWriteReply {
        fn written(self, size: u32) {
            self.written.push(size);
        }

        fn error(self, errno: i32) {
            self.errors.push(errno);
        }
    }

    #[derive(Debug, Default)]
    struct RecordingEmptyReply {
        oks: usize,
        errors: Vec<i32>,
    }

    impl super::FuseEmptyReply for &mut RecordingEmptyReply {
        fn ok(self) {
            self.oks += 1;
        }

        fn error(self, errno: i32) {
            self.errors.push(errno);
        }
    }

    #[test]
    fn empty_reply_guard_errors_on_drop_without_completion() {
        let mut reply = RecordingEmptyReply::default();

        drop(FuseEmptyReplyGuard::new(&mut reply));

        assert_eq!(reply.oks, 0);
        assert_eq!(reply.errors, [libc::EIO]);
    }

    struct BarrierCheckingWriteReply<'a> {
        barrier: &'a super::pending_write_tracker::PendingWriteBarrier,
        saw_barrier_pending_on_reply: &'a Cell<bool>,
    }

    impl super::FuseWriteReply for BarrierCheckingWriteReply<'_> {
        fn written(self, _size: u32) {
            self.saw_barrier_pending_on_reply
                .set(!self.barrier.is_ready());
        }

        fn error(self, _errno: i32) {
            self.saw_barrier_pending_on_reply
                .set(!self.barrier.is_ready());
        }
    }

    #[test]
    fn async_write_completion_replies_once_and_marks_success_complete() {
        let tracker = PendingWriteTracker::default();
        let registration = tracker.register(42, 7).expect("register write");
        let barrier = tracker.handle_barrier(42, 7);
        let mut reply = RecordingWriteReply::default();

        complete_fuse_write_reply(
            FuseWriteReplyGuard::new(&mut reply),
            Ok(5),
            &registration,
            42,
            0,
        );

        reply.assert_written_once(5);
        assert!(barrier.is_ready());
        assert_eq!(barrier.wait(), Ok(()));
    }

    #[test]
    fn async_write_completion_replies_once_and_marks_error_complete() {
        let tracker = PendingWriteTracker::default();
        let registration = tracker.register(42, 7).expect("register write");
        let barrier = tracker.handle_barrier(42, 7);
        let mut reply = RecordingWriteReply::default();

        complete_fuse_write_reply(
            FuseWriteReplyGuard::new(&mut reply),
            Err(VfsError::WouldBlock),
            &registration,
            42,
            0,
        );

        assert!(reply.written.is_empty());
        assert_eq!(reply.errors, [libc::EAGAIN]);
        assert!(barrier.is_ready());
        assert_eq!(barrier.wait(), Err(libc::EAGAIN));
    }

    #[test]
    fn async_write_completion_replies_before_unblocking_barriers() {
        let tracker = PendingWriteTracker::default();
        let registration = tracker.register(42, 7).expect("register write");
        let barrier = tracker.handle_barrier(42, 7);
        let saw_barrier_pending_on_reply = Cell::new(false);
        let reply = BarrierCheckingWriteReply {
            barrier: &barrier,
            saw_barrier_pending_on_reply: &saw_barrier_pending_on_reply,
        };

        complete_fuse_write_reply(FuseWriteReplyGuard::new(reply), Ok(5), &registration, 42, 0);

        assert!(
            saw_barrier_pending_on_reply.get(),
            "write reply must be sent before the pending write barrier is unblocked"
        );
        assert!(barrier.is_ready());
    }

    #[test]
    fn async_write_handle_barrier_surfaces_pending_error() {
        let (_tmp, _rt, _vfs, mut fs) = make_test_fuse_fs("async-write-handle-barrier");
        fs.async_write_enabled = true;
        let pending = fs
            .pending_writes
            .register(42, 7)
            .expect("register pending write");
        pending.complete(super::PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(fs.wait_pending_writes_for_handle(42, 7), Err(libc::EIO));
    }

    #[test]
    fn async_write_handle_barrier_wait_publishes_wait_totals() {
        let (_tmp, _rt, vfs, mut fs) = make_test_fuse_fs("async-handle-barrier-wait-metrics");
        fs.async_write_enabled = true;
        let pending = fs
            .pending_writes
            .register(42, 7)
            .expect("register pending write");
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            started_tx.send(()).expect("send waiter started");
            fs.wait_pending_writes_for_handle(42, 7)
        });

        started_rx.recv().expect("waiter should start");
        std::thread::sleep(std::time::Duration::from_millis(25));
        pending.complete(super::PendingWriteCompletion::Success);
        assert_eq!(waiter.join().expect("waiter should finish"), Ok(()));

        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(snapshot.barrier_waits, 1);
        assert!(snapshot.barrier_wait_total_ms > 0.0);
    }

    #[test]
    fn periodic_async_write_diagnostics_publish_task_failed_registration() {
        let (_tmp, rt, vfs, _env_fs) = make_test_fuse_fs("async-write-periodic-diagnostics");
        let fs = StudioFuseFs::new_with_runtime_options(
            vfs.clone(),
            rt.handle().clone(),
            super::StudioFuseRuntimeOptions {
                async_write_enabled: true,
                linux_async_dio_enabled: false,
                async_write_admission_limits: None,
            },
        );
        let pending = fs
            .pending_writes
            .register(42, 7)
            .expect("register pending write");

        drop(pending);
        rt.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        });

        let snapshot = vfs.write_path_observability_snapshot().fuse_async_write;
        assert_eq!(snapshot.pending_writes, 0);
        assert_eq!(snapshot.task_failed_writes, 1);
    }

    #[test]
    fn async_write_handle_barrier_uses_inode_target_without_handle() {
        let (_tmp, _rt, _vfs, mut fs) = make_test_fuse_fs("async-write-inode-flush-barrier");
        fs.async_write_enabled = true;
        let pending = fs
            .pending_writes
            .register(42, 7)
            .expect("register pending write");
        pending.complete(super::PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(fs.wait_pending_writes_for_handle(42, 0), Err(libc::EIO));
    }

    #[test]
    fn async_write_fsync_inode_barrier_waits_across_handles() {
        let (_tmp, _rt, _vfs, mut fs) = make_test_fuse_fs("async-write-inode-barrier");
        fs.async_write_enabled = true;
        let first = fs
            .pending_writes
            .register(42, 7)
            .expect("register first pending write");
        let second = fs
            .pending_writes
            .register(42, 8)
            .expect("register second pending write");
        first.complete(super::PendingWriteCompletion::Success);
        second.complete(super::PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(fs.wait_pending_writes_for_fsync(42, 0), Err(libc::EIO));
    }

    #[test]
    fn async_write_fsync_handle_barrier_waits_across_handles() {
        let (_tmp, _rt, _vfs, mut fs) = make_test_fuse_fs("async-write-handle-fsync-barrier");
        fs.async_write_enabled = true;
        let first = fs
            .pending_writes
            .register(42, 7)
            .expect("register first pending write");
        let second = fs
            .pending_writes
            .register(42, 8)
            .expect("register second pending write");
        first.complete(super::PendingWriteCompletion::Success);
        second.complete(super::PendingWriteCompletion::Error(libc::EIO));

        assert_eq!(fs.wait_pending_writes_for_fsync(42, 7), Err(libc::EIO));
    }

    #[test]
    fn pending_fuse_close_guard_finishes_close_on_drop() {
        let tracker = PendingWriteTracker::default();
        let barrier = tracker.begin_close(42, 7).expect("begin close");
        assert_eq!(barrier.wait(), Ok(()));

        drop(PendingFuseCloseGuard::new(tracker.clone(), 42, 7));

        let reopened = tracker
            .register(42, 7)
            .expect("dropped close guard should finish close");
        assert_eq!(reopened.sequence(), 1);
    }

    #[test]
    fn fuse_invalidation_not_found_is_benign() {
        let not_found = std::io::Error::from(std::io::ErrorKind::NotFound);
        let not_connected = std::io::Error::from(std::io::ErrorKind::NotConnected);
        let other = std::io::Error::from(std::io::ErrorKind::PermissionDenied);

        assert!(!should_log_invalidation_error(&not_found));
        assert!(!should_log_invalidation_error(&not_connected));
        assert!(should_log_invalidation_error(&other));
    }

    #[test]
    fn vfs_error_maps_to_expected_errno_codes() {
        let not_found = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "missing".into(),
        });
        assert_eq!(vfs_err_to_errno(&not_found), libc::ENOENT);

        let exists = VfsError::Meta(MetaError::EntryExists {
            parent: 1,
            name: "dup".into(),
        });
        assert_eq!(vfs_err_to_errno(&exists), libc::EEXIST);

        let invalid = VfsError::InvalidName("bad".into());
        assert_eq!(vfs_err_to_errno(&invalid), libc::EINVAL);

        let blocked = VfsError::WouldBlock;
        assert_eq!(vfs_err_to_errno(&blocked), libc::EAGAIN);

        let xattr_too_big = VfsError::Meta(MetaError::XattrValueTooLarge {
            actual: 17,
            limit: 16,
        });
        assert_eq!(vfs_err_to_errno(&xattr_too_big), libc::E2BIG);

        let fragmented = VfsError::Meta(MetaError::NonContiguousRegularFileGraph {
            ino: 7,
            block_index: 3,
        });
        assert_eq!(vfs_err_to_errno(&fragmented), libc::EIO);

        let exhausted = VfsError::Fetch(studiofs_vfs::fetch::FetchError::Exhausted {
            attempts: 3,
            source: studiofs_data::store::DataError::Storage("timeout".into()),
        });
        assert_eq!(vfs_err_to_errno(&exhausted), libc::ETIMEDOUT);
    }

    #[test]
    fn io_permission_denied_maps_to_eacces() {
        let err = VfsError::Io(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert_eq!(vfs_err_to_errno(&err), libc::EACCES);
    }

    #[test]
    fn to_fuse_attr_remaps_legacy_root_owner_and_rounds_blocks() {
        let mut attr = InodeAttr::new_file(9, 1, 0, 0, 0o640);
        attr.size = 513;

        let file_attr = to_fuse_attr(&attr, 501, 20);

        assert_eq!(file_attr.uid, 501);
        assert_eq!(file_attr.gid, 20);
        assert_eq!(file_attr.blocks, 2);
        assert_eq!(file_attr.perm, 0o640);
    }

    #[test]
    fn posix_open_flags_translate_access_and_truncate_bits() {
        let (rdonly, trunc) = posix_flags_to_open_flags(libc::O_RDONLY);
        assert!(rdonly.contains(OpenFlags::RDONLY));
        assert!(!trunc);

        let (append, trunc) = posix_flags_to_open_flags(libc::O_WRONLY | libc::O_APPEND);
        assert!(append.contains(OpenFlags::WRONLY));
        assert!(append.contains(OpenFlags::APPEND));
        assert!(!trunc);

        let (rdwr, trunc) = posix_flags_to_open_flags(libc::O_RDWR | libc::O_TRUNC);
        assert!(rdwr.contains(OpenFlags::RDWR));
        assert!(trunc);
    }

    #[test]
    fn detects_missing_xattr_errors() {
        let missing = VfsError::Meta(MetaError::EntryNotFound {
            parent: 1,
            name: "user.test".into(),
        });
        assert!(is_missing_xattr_error(&missing));

        let other = VfsError::Meta(MetaError::Storage("boom".into()));
        assert!(!is_missing_xattr_error(&other));
    }

    #[test]
    fn missing_xattr_errno_matches_platform() {
        assert_eq!(missing_xattr_errno(), libc::ENODATA);
    }

    #[test]
    fn hidden_linux_root_metadata_entry_matches_trashes_only() {
        assert!(super::should_hide_linux_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b".Trashes"
        ));
        assert!(!super::should_hide_linux_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b".Trash"
        ));
        assert!(!super::should_hide_linux_root_metadata_entry(
            studiofs_meta::ROOT_INO + 1,
            b".Trashes"
        ));
        assert!(!super::should_hide_linux_root_metadata_entry(
            studiofs_meta::ROOT_INO,
            b"visible"
        ));
    }
}
